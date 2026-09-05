"""Read-only desired-source resolution and fleet revision health.

The operator declares source intent in the fleet config.  A moving ref is
resolved once to an immutable full Git object ID; runtime inspection then
compares that ID with every Orcest-managed execution surface without changing
Git state, containers, Redis keys, or VMs.
"""

from __future__ import annotations

import json
import os
import re
import selectors
import shlex
import signal
import subprocess
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

from orcest.revision import normalize_revision, revision_is_attested

if TYPE_CHECKING:
    from orcest.fleet.config import FleetConfig, SourceConfig

_FULL_SHA_RE = re.compile(r"^(?:[0-9a-fA-F]{40}|[0-9a-fA-F]{64})$")
_REF_RE = re.compile(r"^refs/[A-Za-z0-9][A-Za-z0-9._/-]{0,254}$")
_OWNER_REPO_RE = re.compile(r"^[A-Za-z0-9._-]+/[A-Za-z0-9._-]+$")
_SCP_REPOSITORY_RE = re.compile(r"^git@[A-Za-z0-9.-]+:[A-Za-z0-9._/-]+(?:\.git)?$")
_RUNTIME_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_RESOLVE_TIMEOUT_SECONDS = 15.0
_SSH_TIMEOUT_SECONDS = 20.0
_MAX_COMMAND_OUTPUT = 64 * 1024
_MAX_REF_OUTPUT = 4096
_PROCESS_TERM_GRACE_SECONDS = 1.0


@dataclass(frozen=True)
class DesiredRevision:
    """One bounded resolution of the configured desired source."""

    configured: bool
    repository: str | None
    ref: str | None
    revision: str | None
    status: str

    @property
    def resolved(self) -> bool:
        return self.revision is not None and self.status == "resolved"


@dataclass(frozen=True)
class RuntimeRevision:
    """Revision observed on one runtime surface."""

    runtime_class: str
    runtime_id: str
    revision: str | None
    status: str

    def as_dict(self) -> dict[str, str | None]:
        return {
            "class": self.runtime_class,
            "id": self.runtime_id,
            "revision": self.revision,
            "status": self.status,
        }


@dataclass(frozen=True)
class SourceHealthReport:
    """Machine-readable whole-fleet source revision health."""

    desired: DesiredRevision
    healthy: bool
    runtimes: tuple[RuntimeRevision, ...]
    diagnostics: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "healthy": self.healthy,
            "desired": {
                "configured": self.desired.configured,
                "repository": self.desired.repository,
                "ref": self.desired.ref,
                "revision": self.desired.revision,
                "status": self.desired.status,
            },
            "runtimes": [runtime.as_dict() for runtime in self.runtimes],
            "diagnostics": list(self.diagnostics),
        }


def _safe_repository(repository: str) -> tuple[str, str] | None:
    """Return ``(display, git_argument)`` for a non-secret repository value."""
    if not repository or len(repository) > 512 or repository.startswith("-"):
        return None
    if any(ord(char) < 32 or char.isspace() for char in repository):
        return None
    if _OWNER_REPO_RE.fullmatch(repository):
        return repository, f"https://github.com/{repository}.git"
    if _SCP_REPOSITORY_RE.fullmatch(repository):
        return repository, repository

    try:
        parsed = urlsplit(repository)
        hostname = parsed.hostname
        # Accessing `.port` performs numeric/range validation. Without it an
        # arbitrary token in the port position would be accepted and echoed.
        _port = parsed.port
    except ValueError:
        return None
    if parsed.scheme not in {"https", "ssh"}:
        return None
    if not hostname or parsed.query or parsed.fragment:
        return None
    # A password, token, or arbitrary username in an URL is not valid
    # non-secret fleet policy.  SSH URLs may use the conventional `git` user.
    if parsed.password is not None:
        return None
    if parsed.username is not None and not (parsed.scheme == "ssh" and parsed.username == "git"):
        return None
    if not parsed.path or not re.fullmatch(r"/[A-Za-z0-9._/-]+", parsed.path):
        return None
    return repository, repository


def _valid_ref(ref: str) -> bool:
    """Conservatively validate a fully-qualified ref without running Git."""
    return bool(
        _REF_RE.fullmatch(ref)
        and ".." not in ref
        and "//" not in ref
        and not ref.endswith(("/", "."))
        and "/." not in ref
        and "@{" not in ref
    )


def _terminate_process(process: subprocess.Popen[bytes]) -> None:
    """Terminate and reap one isolated subprocess group."""
    if process.poll() is not None:
        process.wait()
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        process.wait(timeout=_PROCESS_TERM_GRACE_SECONDS)
        return
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait()


@dataclass(frozen=True)
class _CommandResult:
    status: str
    output: bytes


def _run_bounded(
    argv: list[str],
    *,
    timeout: float,
    max_output: int,
) -> _CommandResult:
    """Run an argv with bounded time/output and no captured diagnostics."""
    try:
        process = subprocess.Popen(
            argv,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
    except OSError:
        return _CommandResult("failed", b"")

    assert process.stdout is not None
    output = bytearray()
    deadline = time.monotonic() + timeout
    eof = False
    with selectors.DefaultSelector() as selector:
        selector.register(process.stdout, selectors.EVENT_READ)
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                _terminate_process(process)
                return _CommandResult("timeout", b"")
            for key, _events in selector.select(timeout=min(remaining, 0.05)):
                chunk = os.read(key.fd, min(65536, max_output + 1 - len(output)))
                if not chunk:
                    selector.unregister(process.stdout)
                    eof = True
                    break
                output.extend(chunk)
                if len(output) > max_output:
                    _terminate_process(process)
                    return _CommandResult("oversized response", b"")
            returncode = process.poll()
            if returncode is not None and eof:
                return _CommandResult("ok" if returncode == 0 else "failed", bytes(output))


def resolve_desired_revision(source: SourceConfig) -> DesiredRevision:
    """Resolve source policy once, returning only fixed secret-safe errors."""
    repository = source.repository.strip()
    ref = source.ref.strip()
    sha = source.sha.strip()
    configured = bool(repository or ref or sha)
    if not configured:
        return DesiredRevision(False, None, None, None, "desired revision unconfigured")

    safe_repository = _safe_repository(repository) if repository else None
    repository_display = safe_repository[0] if safe_repository is not None else None

    if sha:
        # Immutable intent is SHA-only. Accepting a repository beside the SHA
        # would imply that the commit was verified against that repository,
        # even though immutable mode intentionally performs no network lookup.
        if repository or ref or not _FULL_SHA_RE.fullmatch(sha):
            return DesiredRevision(True, repository_display, None, None, "invalid configuration")
        return DesiredRevision(True, repository_display, None, sha.lower(), "resolved")

    if safe_repository is None or not _valid_ref(ref):
        return DesiredRevision(True, repository_display, None, None, "invalid configuration")

    result = _run_bounded(
        ["git", "ls-remote", "--exit-code", safe_repository[1], ref, f"{ref}^{{}}"],
        timeout=_RESOLVE_TIMEOUT_SECONDS,
        max_output=_MAX_REF_OUTPUT,
    )
    if result.status != "ok":
        return DesiredRevision(True, repository_display, ref, None, result.status)
    try:
        text = result.output.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        return DesiredRevision(True, repository_display, ref, None, "malformed response")

    exact: str | None = None
    peeled: str | None = None
    for line in text.splitlines():
        parts = line.split("\t")
        if len(parts) != 2 or not _FULL_SHA_RE.fullmatch(parts[0]):
            return DesiredRevision(True, repository_display, ref, None, "malformed response")
        revision, name = parts[0].lower(), parts[1]
        if name == ref:
            if exact is not None and exact != revision:
                return DesiredRevision(True, repository_display, ref, None, "malformed response")
            exact = revision
        elif name == f"{ref}^{{}}":
            if peeled is not None and peeled != revision:
                return DesiredRevision(True, repository_display, ref, None, "malformed response")
            peeled = revision
        else:
            return DesiredRevision(True, repository_display, ref, None, "malformed response")
    resolved_revision = peeled or exact
    if resolved_revision is None:
        return DesiredRevision(True, repository_display, ref, None, "missing ref")
    return DesiredRevision(True, repository_display, ref, resolved_revision, "resolved")


def _safe_runtime_id(value: object, fallback: str) -> str:
    candidate = str(value)
    return candidate if _RUNTIME_ID_RE.fullmatch(candidate) else fallback


def _read_ssh(ssh_target: str, remote_command: str) -> _CommandResult:
    from orcest.fleet.cli import _SSH_OPTS

    return _run_bounded(
        ["ssh", *_SSH_OPTS, ssh_target, remote_command],
        timeout=_SSH_TIMEOUT_SECONDS,
        max_output=_MAX_COMMAND_OUTPUT,
    )


def _parse_runtime_revision(result: _CommandResult) -> str | None:
    if result.status != "ok":
        return None
    try:
        lines = [
            line.strip() for line in result.output.decode("utf-8").splitlines() if line.strip()
        ]
    except UnicodeDecodeError:
        return None
    if len(lines) != 1:
        return None
    revision = normalize_revision(lines[0])
    return revision if revision_is_attested(revision) else None


def _container_revision(ssh_target: str, compose_project: str, service: str) -> str | None:
    """Read one running Compose service's self-reported revision."""
    project = shlex.quote(compose_project)
    service_name = shlex.quote(service)
    command = (
        "cid=$(docker ps --filter "
        f"label=com.docker.compose.project={project} --filter "
        f"label=com.docker.compose.service={service_name} --format '{{{{.ID}}}}'); "
        "test \"$(printf '%s\\n' \"$cid\" | sed '/^$/d' | wc -l)\" -eq 1 && "
        'docker exec "$cid" orcest revision --short'
    )
    return _parse_runtime_revision(_read_ssh(ssh_target, command))


def _redis_read(ssh_target: str, command: str) -> _CommandResult:
    from orcest.fleet.orchestrator import _REDIS_CLI_PREFIX

    return _read_ssh(ssh_target, f"{_REDIS_CLI_PREFIX} --raw {command}")


def _active_template(
    ssh_target: str,
    fallback_vmid: int,
    fallback_revision: str,
) -> tuple[str, str | None]:
    pointer = _redis_read(ssh_target, "GET orcest:pool:current_template_vmid")
    if pointer.status != "ok":
        return "active-template", None
    try:
        raw_pointer = pointer.output.decode("ascii", errors="strict").strip()
    except UnicodeDecodeError:
        return "active-template", None
    if raw_pointer:
        try:
            vmid = int(raw_pointer)
        except ValueError:
            return "active-template", None
        if vmid <= 0:
            return "active-template", None
    elif fallback_vmid > 0:
        vmid = fallback_vmid
    else:
        return "active-template", None

    runtime_id = f"vm-{vmid}"
    revision_result = _redis_read(
        ssh_target,
        f"GET orcest:pool:template_revision:{shlex.quote(str(vmid))}",
    )
    if revision_result.status != "ok":
        return runtime_id, None
    try:
        raw_revision = revision_result.output.decode("ascii", errors="strict").strip()
    except UnicodeDecodeError:
        return runtime_id, None
    revision = _parse_runtime_revision(revision_result)
    # Only a successful nil/empty metadata read is a legacy record eligible
    # for config fallback. Nonempty malformed/dirty evidence stays unknown.
    if not raw_revision and vmid == fallback_vmid:
        normalized = normalize_revision(fallback_revision)
        if revision_is_attested(normalized):
            revision = normalized
    return runtime_id, revision


def _worker_revisions(ssh_target: str) -> list[tuple[str, str | None]] | None:
    scan = _redis_read(ssh_target, "--scan --pattern 'orcest:workers:heartbeat:*'")
    if scan.status != "ok":
        return None
    try:
        keys = [line.strip() for line in scan.output.decode("utf-8").splitlines() if line.strip()]
    except UnicodeDecodeError:
        return None
    # Bound both response size and remote argv length.  One MGET keeps the
    # whole worker snapshot to two bounded SSH operations rather than one SSH
    # timeout per worker.
    if len(keys) > 256:
        return None

    if not keys:
        return []
    values = _redis_read(
        ssh_target,
        "MGET " + " ".join(shlex.quote(key) for key in sorted(keys)),
    )
    if values.status != "ok":
        return None
    try:
        payload_lines = values.output.decode("utf-8").splitlines()
    except UnicodeDecodeError:
        return None
    if len(payload_lines) != len(keys):
        return None

    workers: list[tuple[str, str | None]] = []
    for index, (key, raw_payload) in enumerate(zip(sorted(keys), payload_lines, strict=True)):
        prefix = "orcest:workers:heartbeat:"
        if not key.startswith(prefix):
            return None
        worker_id = _safe_runtime_id(key.removeprefix(prefix), f"invalid-worker-{index + 1}")
        try:
            payload = json.loads(raw_payload)
        except json.JSONDecodeError:
            workers.append((worker_id, None))
            continue
        raw_revision = payload.get("revision") if isinstance(payload, dict) else None
        normalized = normalize_revision(raw_revision) if isinstance(raw_revision, str) else None
        workers.append((worker_id, normalized if revision_is_attested(normalized) else None))
    return workers


def _runtime_status(revision: str | None, desired: str | None) -> str:
    if revision is None:
        return "unknown"
    if desired is None:
        return "unverified"
    return "current" if revision == desired else "stale"


def collect_source_health(
    config: FleetConfig,
    *,
    desired: DesiredRevision | None = None,
) -> SourceHealthReport:
    """Collect a side-effect-free source health snapshot for all runtimes."""
    resolution = desired or resolve_desired_revision(config.source)
    if not resolution.configured:
        return SourceHealthReport(
            desired=resolution,
            healthy=False,
            runtimes=(),
            diagnostics=("desired revision unconfigured",),
        )
    wanted = resolution.revision
    runtimes: list[RuntimeRevision] = []

    if not config.orchestrator.host:
        for project in config.projects:
            runtimes.append(RuntimeRevision("project-orchestrator", project.name, None, "unknown"))
        runtimes.append(RuntimeRevision("pool-manager", "pool-manager", None, "unknown"))
        if config.pool.size > 0 or config.pool.template_vm_id or config.pool.template_vmid_range:
            runtimes.append(RuntimeRevision("active-template", "active-template", None, "unknown"))
        if config.pool.size > 0:
            runtimes.append(RuntimeRevision("worker", "no-live-workers", None, "unknown"))
    else:
        ssh_target = config.ssh_target()
        for index, project in enumerate(config.projects):
            runtime_id = _safe_runtime_id(project.name, f"invalid-project-{index + 1}")
            revision = _container_revision(
                ssh_target,
                f"orcest-{project.name}",
                "orchestrator",
            )
            runtimes.append(
                RuntimeRevision(
                    "project-orchestrator",
                    runtime_id,
                    revision,
                    _runtime_status(revision, wanted),
                )
            )

        pool_revision = _container_revision(ssh_target, "orcest-pool", "pool-manager")
        runtimes.append(
            RuntimeRevision(
                "pool-manager",
                "pool-manager",
                pool_revision,
                _runtime_status(pool_revision, wanted),
            )
        )

        if config.pool.size > 0 or config.pool.template_vm_id or config.pool.template_vmid_range:
            template_id, template_revision = _active_template(
                ssh_target,
                config.pool.template_vm_id,
                config.pool.template_revision,
            )
            runtimes.append(
                RuntimeRevision(
                    "active-template",
                    template_id,
                    template_revision,
                    _runtime_status(template_revision, wanted),
                )
            )

        workers = _worker_revisions(ssh_target)
        if workers is None:
            runtimes.append(RuntimeRevision("worker", "inspection-unavailable", None, "unknown"))
        elif not workers and config.pool.size > 0:
            runtimes.append(RuntimeRevision("worker", "no-live-workers", None, "unknown"))
        elif workers:
            for worker_id, revision in workers:
                runtimes.append(
                    RuntimeRevision(
                        "worker",
                        worker_id,
                        revision,
                        _runtime_status(revision, wanted),
                    )
                )

    diagnostics: list[str] = []
    if not resolution.resolved:
        diagnostics.append(resolution.status)
    for runtime in runtimes:
        if runtime.status == "stale":
            diagnostics.append(
                f"{runtime.runtime_class} {runtime.runtime_id}: "
                f"expected {wanted}, observed {runtime.revision}"
            )
        elif runtime.status in {"unknown", "unverified"}:
            diagnostics.append(
                f"{runtime.runtime_class} {runtime.runtime_id}: revision {runtime.status}"
            )

    healthy = resolution.resolved and all(runtime.status == "current" for runtime in runtimes)
    return SourceHealthReport(
        desired=resolution,
        healthy=healthy,
        runtimes=tuple(runtimes),
        diagnostics=tuple(diagnostic[:240] for diagnostic in diagnostics),
    )
