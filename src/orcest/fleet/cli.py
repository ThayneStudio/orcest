"""Fleet management CLI commands.

Provides ``orcest fleet`` subcommands for managing the fleet of
orchestrator stacks and disposable worker VMs via Terraform and
Docker Compose, driven by a single config file.
"""

from __future__ import annotations

import fcntl
import functools
import re
import shlex
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Iterator, ParamSpec, TextIO, TypeVar

import click
from rich.console import Console
from rich.table import Table

from orcest.fleet.config import DEFAULT_CONFIG_PATH

if TYPE_CHECKING:
    from orcest.fleet.config import FleetConfig, ProjectEntry
    from orcest.fleet.proxmox_api import ProxmoxClient

_REPO_RE = re.compile(r"^[a-zA-Z0-9._-]+/[a-zA-Z0-9._-]+$")

_DEFAULT_CLOUD_IMAGE_URL = (
    "https://cloud-images.ubuntu.com/noble/current/noble-server-cloudimg-amd64.img"
)
_DRAIN_QUIESCE_SECONDS = 5.25
_COORDINATED_BACKEND_CHANGE_META_KEY = "orcest_coordinated_backend_change"
_DEFER_PROJECT_START_META_KEY = "orcest_defer_project_start"
_CANDIDATE_WORKER_WAIT_SECONDS = 900
_FLEET_OPERATION_LOCK_PATH = "/run/lock/orcest-fleet-operation.lock"
_fleet_operation_lock_depth = 0
_fleet_operation_lock_handle: TextIO | None = None
_P = ParamSpec("_P")
_R = TypeVar("_R")


class _OwnedTemplateVmCreationError(RuntimeError):
    """Template creation failed after this invocation created the VM ID."""


@contextmanager
def _fleet_operation_lock() -> Iterator[None]:
    """Serialize fleet mutations on the Proxmox operator host.

    The runbook requires every mutating fleet command to execute on the same
    Proxmox host. ``flock`` is process-held, automatically releases on a crash,
    and remains held across nested ``deploy`` -> ``stop/update/rebake/start``
    invocations in this process.
    """
    global _fleet_operation_lock_depth, _fleet_operation_lock_handle
    if _fleet_operation_lock_depth:
        _fleet_operation_lock_depth += 1
        try:
            yield
        finally:
            _fleet_operation_lock_depth -= 1
        return

    handle = open(_FLEET_OPERATION_LOCK_PATH, "a", encoding="utf-8")
    try:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise click.ClickException(
                "another Orcest fleet mutation is already running on this Proxmox host"
            ) from exc
        _fleet_operation_lock_handle = handle
        _fleet_operation_lock_depth = 1
        try:
            yield
        finally:
            _fleet_operation_lock_depth = 0
            _fleet_operation_lock_handle = None
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    finally:
        handle.close()


def _serialized_fleet_operation(function: Callable[_P, _R]) -> Callable[_P, _R]:
    """Wrap a mutating Click callback in the shared fleet operation lock."""

    @functools.wraps(function)
    def wrapped(*args: _P.args, **kwargs: _P.kwargs) -> _R:
        with _fleet_operation_lock():
            return function(*args, **kwargs)

    setattr(wrapped, "_orcest_serialized_fleet_operation", True)
    return wrapped


def _wait_for_worker_drain_quiescence() -> None:
    """Wait until workers blocked in XREADGROUP have observed drain leases."""
    time.sleep(_DRAIN_QUIESCE_SECONDS)


def _is_proxmox_template(vm_info: dict) -> bool:
    """Return True if Proxmox reports the VM as a converted template.

    Proxmox returns the flag as an integer or a boolean depending on
    transport, so a bare truthiness test would read the string ``"0"`` as
    True. Mirrors ``PoolManager._is_proxmox_template``; destructive paths must
    not parse this more loosely than the pool manager does.
    """
    flag = vm_info.get("template", 0)
    try:
        return int(flag) == 1
    except (TypeError, ValueError):
        return bool(flag)


def _drain_leases_still_held(ssh_target: str, worker_ids: list[str]) -> list[str]:
    """Return the worker IDs still present in ``orcest:pool:draining``.

    A stale drain lease permanently fences a surviving worker: it exits 75 on
    every loop until the systemd restart budget is exhausted. ``clean_pool_redis``
    verifies the marker for destroyed VMs; workers that were deliberately left
    alive need the same proof that their lease really went away.

    Raises ``RuntimeError`` if the membership cannot be read at all.
    """
    from orcest.fleet.orchestrator import _REDIS_CLI_PREFIX, _require_redis_cli_success, _ssh

    if not worker_ids:
        return []
    query = " && ".join(
        f"{_REDIS_CLI_PREFIX} --raw SISMEMBER orcest:pool:draining {shlex.quote(worker_id)}"
        for worker_id in worker_ids
    )
    result = _ssh(ssh_target, query)
    _require_redis_cli_success(result, "Failed to verify worker drain leases were cleared")
    states = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    if len(states) != len(worker_ids):
        raise RuntimeError(
            "Failed to verify worker drain leases were cleared: expected "
            f"{len(worker_ids)} SISMEMBER results, got {len(states)}"
        )
    return [
        worker_id
        for worker_id, state in zip(worker_ids, states, strict=True)
        if state.strip() != "0"
    ]


def _write_project_files_from_config(
    cfg: FleetConfig,
    ssh_target: str,
    project: ProjectEntry,
    redis_password: str,
) -> None:
    """Regenerate and upload one project's orchestrator config from fleet config."""
    from orcest.fleet.orchestrator import (
        generate_env_file,
        generate_orchestrator_config,
        write_project_files,
    )

    mismatches = cfg.provider_stream_mismatches().get(project.name, [])
    if mismatches:
        providers = ", ".join(mismatches)
        backends = ", ".join(sorted(cfg.pool.worker_backends())) or "none"
        raise ValueError(
            f"project {project.name!r} configures provider stream(s) {providers}, "
            f"but the managed pool schedules only these backends: {backends}; "
            "add matching worker profiles or remove those providers"
        )

    org = cfg.resolve_org(project)
    if cfg.monitor_ingest_url and not cfg.monitor_write_token:
        raise ValueError("monitor_ingest_url is configured but monitor_write_token is empty")
    env_content = generate_env_file(
        github_token=org.github_token,
        key_prefix=project.name,
        project_name=project.name,
        claude_tokens=org.claude_oauth_tokens,
        provider_credentials=getattr(org, "provider_credentials", None),
        trace_archive_host_path=cfg.trace_archive_host_path,
        redis_password=redis_password,
        monitor_write_token=cfg.monitor_write_token,
    )
    config_yaml = generate_orchestrator_config(
        repo=project.repo,
        key_prefix=project.name,
        extra_providers=list((getattr(org, "provider_credentials", None) or {}).keys()),
        default_runner=cfg.pool.default_task_backend(),
        trace_archive_enabled=bool(cfg.trace_archive_host_path),
        monitor_ingest_url=cfg.monitor_ingest_url,
    )
    write_project_files(ssh_target, project.name, env_content, config_yaml)


def _next_free_vmid() -> int | None:
    """Query Proxmox for the next available VM ID, or return None."""
    import json

    try:
        result = subprocess.run(
            ["pvesh", "get", "/cluster/nextid", "--output-format", "json"],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            return int(json.loads(result.stdout))
    except (FileNotFoundError, ValueError, json.JSONDecodeError):
        pass
    return None


def _humanize_bytes(n: float) -> str:
    """Format bytes as a human-readable string (e.g. '1.7 TiB')."""
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(n) < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PiB"


def _prompt_storage(
    px: ProxmoxClient,
    content_type: str,
    purpose: str,
    console: Console,
    default: str | None = None,
) -> str:
    """Interactively select a Proxmox storage pool, honouring *default*.

    Queries available storage filtered by *content_type* (e.g. ``"images"``).
    When *default* is supplied and matches an available storage, returns it
    without prompting — this is the non-interactive path used by systemd
    timers (``orcest-rebake-template.timer``) where there is no TTY.

    Falls back to a Rich table + prompt only when *default* is unset or no
    longer matches available storage; useful for first-time setup.

    Args:
        px: Proxmox API client.
        content_type: Required content type (``"images"``, ``"snippets"``, etc.).
        purpose: Human description shown in the prompt (e.g. ``"template VM disk"``).
        console: Rich console for output.
        default: Pre-selected storage name. Skips the prompt when present and valid.

    Returns:
        The chosen storage name.
    """
    storages = px.list_storage(content_type=content_type)
    if not storages:
        console.print(f"[red]No storage found supporting '{content_type}' content.[/red]")
        raise SystemExit(1)

    if len(storages) == 1:
        name = storages[0]["storage"]
        console.print(f"  Storage for {purpose}: [green]{name}[/green] (only option)")
        return name

    # Non-interactive shortcut: use the configured default when it matches
    # one of the actually-available storages. Skipping the prompt is what
    # lets the rebake timer run without a TTY.
    if default and any(s["storage"] == default for s in storages):
        console.print(f"  Storage for {purpose}: [green]{default}[/green] (from config)")
        return default

    # Build table
    table = Table(title=f"Available storage ({purpose})")
    table.add_column("#", style="bold", width=3)
    table.add_column("Name")
    table.add_column("Type")
    table.add_column("Free")

    default_idx = 0
    for i, s in enumerate(storages):
        if default and s["storage"] == default:
            default_idx = i
        table.add_row(
            str(i + 1),
            s["storage"],
            s.get("type", "?"),
            _humanize_bytes(s.get("avail", 0)),
        )

    console.print(table)
    choice = click.prompt(
        f"  Select storage for {purpose}",
        default=default_idx + 1,
        type=click.IntRange(1, len(storages)),
    )
    selected = storages[choice - 1]["storage"]
    return selected


def _find_snippet_storage(px: ProxmoxClient, console: Console) -> str:
    """Auto-detect a storage pool that supports cloud-init snippets.

    Returns the first enabled storage with ``snippets`` content type.
    """
    storages = px.list_storage(content_type="snippets")
    if not storages:
        console.print("[red]No storage found supporting 'snippets' content.[/red]")
        console.print("  Cloud-init requires a storage with snippets enabled (usually 'local').")
        raise SystemExit(1)
    name = storages[0]["storage"]
    console.print(f"  Snippet storage (cloud-init): [green]{name}[/green]")
    return name


def _get_vm_ip(vm_id: int, console: Console, timeout: int = 300) -> str | None:
    """Wait for a VM to get an IPv4 address via the QEMU guest agent.

    Polls ``qm guest cmd network-get-interfaces`` until a non-loopback
    IPv4 address appears or the timeout expires.  The guest agent must
    be installed and running — there is no fallback.
    """
    import json

    deadline = time.monotonic() + timeout
    console.print(f"  Waiting for VM {vm_id} to get an IP...", end=" ")

    while time.monotonic() < deadline:
        result = subprocess.run(
            ["qm", "guest", "cmd", str(vm_id), "network-get-interfaces"],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            try:
                interfaces = json.loads(result.stdout)
                for iface in interfaces:
                    if iface.get("name") == "lo":
                        continue
                    for addr in iface.get("ip-addresses", []):
                        if addr.get("ip-address-type") == "ipv4":
                            ip = addr["ip-address"]
                            console.print(f"[green]{ip}[/green]")
                            return ip
            except (json.JSONDecodeError, KeyError):
                pass

        time.sleep(5)

    console.print("[yellow]timed out[/yellow]")
    return None


def _validate_project_name(name: str) -> None:
    """Validate project name, exit on failure."""
    from orcest.fleet.config import require_valid_project_name

    try:
        require_valid_project_name(name)
    except ValueError as exc:
        click.echo(f"Error: {exc}", err=True)
        sys.exit(1)


def _validate_repo(repo: str) -> None:
    """Validate repo format (owner/repo)."""
    if not _REPO_RE.match(repo):
        click.echo(
            f"Error: Invalid repo format {repo!r}: expected 'owner/repo' with "
            "alphanumeric/dot/hyphen/underscore characters.",
            err=True,
        )
        sys.exit(1)


def _repo_to_project_name(repo: str) -> str:
    """Derive project name from repo (e.g. 'ThayneStudio/my-project' -> 'my-project')."""
    return repo.rsplit("/", 1)[-1]


_SSH_OPTS = [
    "-o",
    "ConnectTimeout=5",
    "-o",
    "StrictHostKeyChecking=no",
    "-o",
    "UserKnownHostsFile=/dev/null",
    "-o",
    "BatchMode=yes",
    "-o",
    "LogLevel=ERROR",
]


def _run_on_orchestrator(
    cfg: FleetConfig,
    cmd: list[str],
    input_data: str | None = None,
    timeout: int = 30,
) -> subprocess.CompletedProcess[str]:
    """Run an orcest CLI command on the orchestrator in a throwaway Docker container.

    SSHes to the orchestrator VM and executes ``docker run --rm -i orcest <cmd>``
    in an ephemeral container. Useful for self-test commands that need tools
    only available inside the orchestrator image (e.g. ``gh``).

    Args:
        cfg: Fleet config (must have orchestrator host set).
        cmd: Command arguments to run inside the container
            (e.g. ``["orcest", "check", "github-token"]``).
        input_data: Optional data to pipe to the container's stdin.
        timeout: Maximum seconds to wait for the command to complete.

    Returns:
        The completed process result.
    """
    import shlex

    ssh_target = cfg.ssh_target()
    quoted = " ".join(shlex.quote(c) for c in cmd)
    docker_cmd = f"docker run --rm -i --entrypoint '' orcest {quoted}"
    return subprocess.run(
        ["ssh", *_SSH_OPTS, ssh_target, docker_cmd],
        input=input_data,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def _create_proxmox_client(cfg: FleetConfig) -> ProxmoxClient:
    """Create a ProxmoxClient from fleet config."""
    from orcest.fleet.proxmox_api import ProxmoxClient

    return ProxmoxClient(
        endpoint=cfg.proxmox.endpoint,
        token_id=cfg.proxmox.api_token_id,
        token_secret=cfg.proxmox.api_token_secret,
        node=cfg.proxmox.node,
        verify_ssl=cfg.proxmox.verify_ssl,
    )


def _wait_for_cloud_init(
    host: str,
    user: str,
    console: Console,
    timeout: int = 600,
) -> bool:
    """Wait for cloud-init to truly finish on a remote host.

    Cloud-init writes ``/var/lib/cloud/instance/boot-finished`` only when
    ``cloud-final.service`` exits — that's the real "done" signal. Polling
    for the file is robust to ``cloud-init status --wait`` returning early
    on recoverable errors (e.g. a single ``write_files`` OSError) while
    cloud-final is still running tooling installs.

    Returns True if boot-finished appears within *timeout*, False otherwise.
    """
    ssh_target = f"{user}@{host}"
    console.print(f"  Waiting for cloud-init to finish on {host}...", end=" ")
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            result = subprocess.run(
                [
                    "ssh",
                    *_SSH_OPTS,
                    ssh_target,
                    "test -f /var/lib/cloud/instance/boot-finished",
                ],
                capture_output=True,
                text=True,
                timeout=15,
            )
        except subprocess.TimeoutExpired:
            time.sleep(5)
            continue
        if result.returncode == 0:
            # Cloud-final exited. The pointer swap in `rebake` is irreversible
            # from the pool manager's point of view, so a DEFINITIVE non-`done`
            # status (error/degraded) must HARD-FAIL the bake rather than warn
            # and proceed -- otherwise a failed provider-CLI install (runcmd has
            # no cross-entry `set -e`) would silently ship a broken template.
            #
            # But distinguish a definitive bad status from a TRANSIENT read
            # failure (a single flaky SSH read / timeout, or empty output):
            # one flaky read must not abort an otherwise-good bake. Retry the
            # status read a few times; only a real `status: <something>` line
            # decides the outcome. If every read is transiently unreadable we
            # still fail closed (never accept a template we couldn't verify).
            return _read_cloud_init_status(ssh_target, console)
        time.sleep(5)
    console.print("[red]timed out[/red]")
    return False


# Maximum transient-read retries for `cloud-init status` after boot-finished.
_CLOUD_INIT_STATUS_READ_ATTEMPTS = 5


def _read_cloud_init_status(ssh_target: str, console: Console) -> bool:
    """Read ``cloud-init status`` after boot-finished and decide pass/fail.

    Returns True only on a definitive ``status: done``. A definitive non-done
    status (e.g. ``status: error`` / ``degraded``) returns False immediately
    -- the retry budget is reserved for TRANSIENT read failures (SSH timeout or
    empty/unreadable output), so a single flaky read can't abort a good bake.
    If every attempt is transiently unreadable, fails closed (returns False).
    """
    for attempt in range(_CLOUD_INIT_STATUS_READ_ATTEMPTS):
        try:
            status = subprocess.run(
                ["ssh", *_SSH_OPTS, ssh_target, "cloud-init status"],
                capture_output=True,
                text=True,
                timeout=15,
            )
            output = (status.stdout or "").strip()
        except subprocess.TimeoutExpired:
            output = ""  # transient: SSH read timed out

        if "status: done" in output:
            console.print("[green]ok[/green]")
            return True
        if "status:" in output:
            # Definitive non-done status (error/degraded): hard-fail now, do
            # not burn the retry budget on a genuinely-bad outcome.
            console.print("[red]failed[/red]")
            console.print(f"    cloud-init: {output}")
            return False
        # Transient: no readable status line. Retry a few times before deciding.
        if attempt < _CLOUD_INIT_STATUS_READ_ATTEMPTS - 1:
            time.sleep(5)

    console.print("[red]failed[/red]")
    console.print(
        "    cloud-init: status unreadable after"
        f" {_CLOUD_INIT_STATUS_READ_ATTEMPTS} attempts (failing closed)"
    )
    return False


def _ssh_run(host: str, user: str, cmd: str, timeout: int = 60) -> subprocess.CompletedProcess[str]:
    """Run a command over SSH and return the result.

    Raises:
        subprocess.TimeoutExpired: if the command does not complete within ``timeout`` seconds.
    """
    ssh_target = f"{user}@{host}"
    return subprocess.run(
        ["ssh", *_SSH_OPTS, ssh_target, cmd],
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def _scp_to_vm(
    host: str,
    user: str,
    local_path: str,
    remote_path: str,
    timeout: int = 120,
) -> subprocess.CompletedProcess[str]:
    """Copy a local file to a VM over SCP."""
    ssh_target = f"{user}@{host}"
    return subprocess.run(
        ["scp", *_SSH_OPTS, local_path, f"{ssh_target}:{remote_path}"],
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def _install_source_on_worker_template(host: str, user: str, console: Console) -> bool:
    """Install the active Orcest source into the worker template venv."""
    from orcest.fleet.orchestrator import create_source_tarball

    tarball_path = create_source_tarball()
    remote_tarball = "/tmp/orcest-source.tar.gz"
    try:
        copy = _scp_to_vm(host, user, tarball_path, remote_tarball)
        if copy.returncode != 0:
            console.print(f"[red]failed[/red]: {copy.stderr.strip()}")
            return False

        install_cmd = (
            "set -e; "
            "sudo rm -rf /tmp/orcest-template-source; "
            "sudo mkdir -p /tmp/orcest-template-source; "
            f"sudo tar xzf {shlex.quote(remote_tarball)} -C /tmp/orcest-template-source; "
            "sudo chown -R orcest:orcest /tmp/orcest-template-source; "
            "sudo -u orcest -H /opt/orcest/venv/bin/python -m pip install"
            " -q --no-cache-dir -r /tmp/orcest-template-source/requirements.lock; "
            "sudo -u orcest -H /opt/orcest/venv/bin/python -m pip install"
            " -q --no-cache-dir --no-deps /tmp/orcest-template-source; "
            "revision=$(/opt/orcest/venv/bin/orcest revision --short); "
            'case "$revision" in unknown|*-dirty) exit 42;; esac; '
            "printf '%s\\n' \"$revision\" | sudo tee /etc/orcest/source-revision >/dev/null; "
            "sudo chmod 0644 /etc/orcest/source-revision; "
            f"sudo rm -rf {shlex.quote(remote_tarball)} /tmp/orcest-template-source"
        )
        result = _ssh_run(host, user, install_cmd, timeout=300)
        if result.returncode != 0:
            console.print(f"[red]failed[/red]: {result.stderr.strip()}")
            return False
        console.print("[green]ok[/green]")
        return True
    except subprocess.TimeoutExpired:
        console.print("[red]timed out[/red]")
        return False
    finally:
        try:
            Path(tarball_path).unlink()
        except OSError:
            pass


def _verify_provider_clis(host: str, user: str, console: Console) -> bool:
    """Smoke-check every required provider CLI on the worker template.

    Runs *after* ``cloud-init status: done`` and *before* the irreversible
    convert-to-template / pool-pointer swap. cloud-init's runcmd has no
    cross-entry ``set -e``, so a failed provider-CLI install in the middle of
    the list still lets cloud-final report ``done`` -- a half-baked template
    could otherwise ship missing ``claude`` / ``grok`` / ``codex`` and the
    pointer would flip to it, breaking every task routed to that provider.

    Probes each binary in :data:`cloud_init.REQUIRED_PROVIDER_BINARIES` as the
    non-root ``orcest`` worker user (the runtime user) so it also catches an
    exec-permission regression (e.g. the grok binary symlinked under root-only
    ``/root``), not merely root visibility.

    Returns True only if every binary resolves and executes. Grok and Codex
    must also report the exact versions their output parsers were validated
    against. Fails closed: any missing binary, non-zero probe, version
    mismatch, or transient SSH read failure returns False so the caller aborts
    and cleans up.
    """
    from orcest.fleet.cloud_init import (
        _CODEX_VERSION,
        _GROK_VERSION,
        REQUIRED_PROVIDER_BINARIES,
    )

    console.print("  Verifying provider CLIs on template...", end=" ")
    failed: list[str] = []
    pinned_versions = {"codex": _CODEX_VERSION, "grok": _GROK_VERSION}
    for binary in REQUIRED_PROVIDER_BINARIES:
        # Resolve and execute on PATH as the orcest user's login shell (-l) so
        # PATH and permissions match the systemd worker runtime.
        inner_probe = f"command -v {binary} >/dev/null && {binary} --version"
        probe = f"sudo -u orcest -H bash -lc {shlex.quote(inner_probe)}"
        try:
            result = _ssh_run(host, user, probe, timeout=30)
        except subprocess.TimeoutExpired:
            console.print("[red]failed[/red]")
            console.print(f"    provider-CLI check timed out probing '{binary}' (failing closed)")
            return False
        output = "\n".join((result.stdout or "", result.stderr or ""))
        if result.returncode != 0 or not output.strip():
            failed.append(f"{binary}:missing-or-not-executable")
            continue
        expected_version = pinned_versions.get(binary)
        if expected_version is not None:
            first_line = next((line.strip() for line in output.splitlines() if line.strip()), "")
            installed_match = re.search(
                r"(?<![0-9A-Za-z.])v?(\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?)"
                r"(?![0-9A-Za-z.])",
                first_line,
            )
            installed_version = installed_match.group(1) if installed_match else None
            if installed_version != expected_version:
                failed.append(f"{binary}:version-mismatch")

    if failed:
        console.print("[red]failed[/red]")
        console.print(
            f"    provider CLI verification failed for: {', '.join(failed)}.\n"
            "    A provider-CLI install failed mid cloud-init (runcmd has no"
            " cross-entry `set -e`).\n"
            "    Refusing to convert this half-baked VM to a template."
        )
        return False

    console.print("[green]ok[/green]")
    return True


def _wait_for_ssh(host: str, user: str, console: Console, timeout: int = 300) -> bool:
    """Poll until SSH connects or timeout expires. Returns True on success."""
    ssh_target = f"{user}@{host}"
    deadline = time.monotonic() + timeout
    console.print(f"  Waiting for SSH on {host}...", end=" ")
    while time.monotonic() < deadline:
        result = subprocess.run(
            ["ssh", *_SSH_OPTS, ssh_target, "true"],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            console.print("[green]ok[/green]")
            return True
        time.sleep(5)
    console.print("[yellow]timed out[/yellow]")
    return False


def _ensure_orchestrator_ssh(
    ssh_target: str,
    proxmox_ip: str,
    console: Console,
) -> None:
    """Ensure the orchestrator VM can SSH to the Proxmox host.

    The pool manager runs on the orchestrator and needs SSH access to the
    Proxmox host to write cloud-init snippets and run ``qm`` commands.

    Idempotent: skips if SSH already works.
    """
    # Quick check: does SSH already work?
    verify = subprocess.run(
        [
            "ssh",
            *_SSH_OPTS,
            ssh_target,
            f"ssh -o StrictHostKeyChecking=no -o BatchMode=yes"
            f" -o ConnectTimeout=3 root@{proxmox_ip} true",
        ],
        capture_output=True,
        text=True,
        timeout=15,
    )
    if verify.returncode == 0:
        console.print("  Orchestrator SSH to Proxmox... [green]ok[/green]")
        return

    console.print("  Setting up orchestrator SSH to Proxmox...", end=" ")

    # Generate key if missing
    subprocess.run(
        [
            "ssh",
            *_SSH_OPTS,
            ssh_target,
            "test -f ~/.ssh/id_ed25519 || ssh-keygen -t ed25519 -f ~/.ssh/id_ed25519 -N ''",
        ],
        capture_output=True,
        text=True,
        timeout=15,
    )

    # Read public key
    result = subprocess.run(
        ["ssh", *_SSH_OPTS, ssh_target, "cat ~/.ssh/id_ed25519.pub"],
        capture_output=True,
        text=True,
        timeout=10,
    )
    if result.returncode != 0 or not result.stdout.strip():
        console.print("[red]failed (could not read key)[/red]")
        return
    pub_key = result.stdout.strip()

    # Add to Proxmox authorized_keys (local — this command runs on the Proxmox host)
    auth_keys = Path("/root/.ssh/authorized_keys")
    if auth_keys.exists() and pub_key in auth_keys.read_text():
        pass  # already present
    else:
        auth_keys.parent.mkdir(parents=True, exist_ok=True)
        with auth_keys.open("a") as f:
            f.write(f"\n{pub_key}\n")

    # Verify
    verify = subprocess.run(
        [
            "ssh",
            *_SSH_OPTS,
            ssh_target,
            f"ssh -o StrictHostKeyChecking=no -o BatchMode=yes"
            f" -o ConnectTimeout=5 root@{proxmox_ip} hostname",
        ],
        capture_output=True,
        text=True,
        timeout=15,
    )
    if verify.returncode == 0:
        console.print("[green]ok[/green]")
    else:
        console.print("[yellow]failed (SSH verify failed)[/yellow]")
        console.print(f"    {verify.stderr.strip()}")


@click.group()
def fleet() -> None:
    """Manage the orcest fleet: orchestrators, workers, and VMs."""


@fleet.command("add-org")
@click.argument("org_name")
@click.option(
    "--github-token",
    required=True,
    help="GitHub PAT (classic: repo+workflow scopes; "
    "fine-grained: contents, issues, pull-requests, actions R/W).",
)
@click.option(
    "--claude-token",
    required=True,
    multiple=True,
    help="Claude OAuth token(s). Repeat for round-robin pool.",
)
@click.option(
    "--config",
    default=str(DEFAULT_CONFIG_PATH),
    help="Fleet config path.",
    show_default=True,
)
@_serialized_fleet_operation
def add_org(org_name: str, github_token: str, claude_token: tuple[str, ...], config: str) -> None:
    """Register a GitHub organization with its credentials.

    ORG_NAME is the GitHub org or user (e.g. 'ThayneStudio').

    \b
    GitHub token requirements:
      Classic PAT (ghp_): repo + workflow scopes
      Fine-grained PAT:   contents, issues, pull-requests, actions (R/W)
                          metadata (read)
    """
    from orcest.fleet.config import OrgEntry, load_config, save_config

    console = Console()
    cfg = load_config(config)

    existing = cfg.orgs.get(org_name)
    if existing is not None:
        console.print(f"[yellow]Org '{org_name}' already exists, updating credentials.[/yellow]")
        if existing.provider_credentials:
            preserved = ", ".join(sorted(existing.provider_credentials))
            console.print(f"  Preserving hand-configured provider_credentials: {preserved}")

    # Validate the GitHub token via the orchestrator's Docker image (which has gh installed)
    console.print("  Validating GitHub token...", end=" ")
    if not cfg.orchestrator.host:
        console.print("[yellow]skipped (orchestrator not set up yet)[/yellow]")
    else:
        try:
            result = _run_on_orchestrator(
                cfg,
                ["orcest", "check", "github-token"],
                input_data=github_token + "\n",
            )
            if result.returncode != 0:
                console.print("[red]failed[/red]")
                output = (result.stderr or result.stdout or "").strip()
                if output:
                    console.print(f"    {output}")
                console.print("[yellow]Warning: token validation failed, saving anyway.[/yellow]")
            else:
                console.print("[green]ok[/green]")
        except (OSError, subprocess.SubprocessError) as exc:
            console.print(f"[yellow]skipped ({exc})[/yellow]")

    # Update in place rather than replacing the entry: fields this command does
    # not manage (notably `provider_credentials`, which is hand-edited into the
    # fleet config) must survive a routine GitHub PAT rotation.
    if existing is not None:
        existing.github_token = github_token
        existing.claude_oauth_tokens = list(claude_token)
    else:
        cfg.orgs[org_name] = OrgEntry(
            github_token=github_token,
            claude_oauth_tokens=list(claude_token),
        )
    save_config(cfg, config)
    token_count = len(claude_token)
    pool_note = f" ({token_count} Claude tokens)" if token_count > 1 else ""
    console.print(f"\n[bold]Org '{org_name}' registered{pool_note}.[/bold]")


@fleet.command("create-orchestrator")
@click.option("--vm-id", type=int, default=None, help="Proxmox VM ID for the orchestrator.")
@click.option(
    "--storage",
    default=None,
    help="Proxmox storage for VM disk (skip interactive prompt).",
)
@click.option(
    "--config",
    default=str(DEFAULT_CONFIG_PATH),
    help="Fleet config path.",
    show_default=True,
)
@_serialized_fleet_operation
def create_orchestrator(vm_id: int | None, storage: str | None, config: str) -> None:
    """Create the orchestrator VM via Terraform and deploy the Docker stack."""
    from orcest.fleet.config import load_config, save_config

    console = Console()
    cfg = load_config(config)

    # Select storage for orchestrator VM disk
    if storage is None and cfg.proxmox.api_token_id and cfg.proxmox.api_token_secret:
        px = _create_proxmox_client(cfg)
        storage = _prompt_storage(
            px,
            "images",
            "orchestrator VM disk",
            console,
            default=cfg.proxmox.storage,
        )
    if storage:
        cfg.proxmox.storage = storage

    # Prompt for VM ID
    if vm_id is None:
        default_id = _next_free_vmid() or cfg.orchestrator.vm_id
        vm_id = click.prompt("  VM ID for orchestrator", default=default_id, type=int)
    cfg.orchestrator.vm_id = vm_id

    console.print(f"\n[bold]Creating orchestrator VM (ID {vm_id})[/bold]\n")

    # Step 1: Generate and write tfvars
    console.print("  Generating Terraform variables...", end=" ")
    try:
        from orcest.fleet.provisioner import generate_tfvars, write_tfvars

        tfvars = generate_tfvars(cfg)
        write_tfvars(tfvars)
        console.print("[green]ok[/green]")
    except Exception as exc:
        console.print(f"[red]failed[/red]: {exc}")
        sys.exit(1)

    # Step 2: Apply Terraform
    console.print("  Applying Terraform (this may take a few minutes)...")
    try:
        from orcest.fleet.provisioner import apply

        apply()
        console.print("  Terraform apply [green]ok[/green]")
    except Exception as exc:
        console.print(f"  Terraform apply [red]failed[/red]: {exc}")
        sys.exit(1)

    # Step 3: Get orchestrator IP (via guest agent or ARP)
    orch_ip = _get_vm_ip(vm_id, console)
    if not orch_ip:
        console.print("  [yellow]Could not determine IP. VM may still be booting.[/yellow]")
        console.print("  Saving config. Re-run after VM is ready.")
        save_config(cfg, config)
        sys.exit(1)

    # Step 4: Wait for SSH
    if not _wait_for_ssh(orch_ip, cfg.orchestrator.user, console):
        console.print("[yellow]SSH not available yet. VM may still be booting.[/yellow]")
        console.print("  Saving config with the IP and exiting. Re-run after VM is ready.")
        cfg.orchestrator.host = orch_ip
        save_config(cfg, config)
        sys.exit(1)

    # Step 5: Wait for cloud-init to finish (installs Docker, etc.)
    ssh_target = f"{cfg.orchestrator.user}@{orch_ip}"
    if not _wait_for_cloud_init(orch_ip, cfg.orchestrator.user, console):
        console.print("[red]Cloud-init timed out. Saving config with partial state.[/red]")
        cfg.orchestrator.host = orch_ip
        save_config(cfg, config)
        sys.exit(1)

    # Step 5b: Set up SSH from orchestrator to Proxmox host
    # (needed by pool manager to write cloud-init snippets)
    from urllib.parse import urlparse

    proxmox_ip = urlparse(cfg.proxmox.endpoint).hostname or "127.0.0.1"
    _ensure_orchestrator_ssh(ssh_target, proxmox_ip, console)

    # Step 6: Upload source and build Docker image
    try:
        from orcest.fleet.orchestrator import build_image, upload_source

        console.print("  Uploading orcest source...")
        upload_source(ssh_target)
        console.print("  Upload [green]ok[/green]")

        console.print("  Building Docker image (this may take a minute)...")
        build_image(ssh_target)
        console.print("  Docker build [green]ok[/green]")
    except Exception as exc:
        console.print(f"  [red]failed[/red]: {exc}")
        console.print("  Saving config with partial state.")
        cfg.orchestrator.host = orch_ip
        save_config(cfg, config)
        sys.exit(1)

    # Step 7: Mint Redis password, then start shared Redis stack
    try:
        from orcest.fleet.orchestrator import ensure_redis_password, ensure_redis_stack

        # C1: mint BEFORE the stack starts so --requirepass gets a real value
        # (an empty ORCEST_REDIS_PASSWORD makes Redis consume the next flag as
        # its password -- a FATAL boot / total outage).
        console.print("  Minting Redis password...")
        ensure_redis_password(ssh_target)
        console.print("  Starting shared Redis stack...")
        ensure_redis_stack(ssh_target)
        console.print("  Redis stack [green]ok[/green]")
    except Exception as exc:
        console.print(f"  Redis stack [red]failed[/red]: {exc}")
        console.print("  Saving config with partial state.")
        cfg.orchestrator.host = orch_ip
        save_config(cfg, config)
        sys.exit(1)

    # Step 8: Update config with orchestrator host (before uploading to remote)
    cfg.orchestrator.host = orch_ip
    save_config(cfg, config)

    # Step 9: Start pool manager (if a legacy template or template range and
    # Proxmox credentials are configured).
    if (
        (cfg.pool.template_vm_id or cfg.pool.template_range() is not None)
        and cfg.proxmox.api_token_id
        and cfg.proxmox.api_token_secret
    ):
        if cfg.proxmox.is_localhost():
            console.print("  [yellow]Skipping pool manager: proxmox.endpoint is localhost[/yellow]")
            console.print(
                "  The pool manager runs on the orchestrator VM and needs the"
                " Proxmox host's real IP."
            )
            console.print("  Fix with: orcest init  (or edit /etc/orcest/config.yaml)")
        else:
            try:
                from orcest.fleet.orchestrator import ensure_pool_manager, upload_fleet_config

                console.print("  Uploading fleet config and starting pool manager...")
                upload_fleet_config(ssh_target, config)
                ensure_pool_manager(ssh_target)
                console.print("  Pool manager [green]ok[/green]")
            except Exception as exc:
                console.print(f"  Pool manager [yellow]failed: {exc}[/yellow]")
                console.print("  (Pool manager can be started later with 'orcest fleet update')")

    console.print(f"\n[bold]Orchestrator created at {orch_ip}.[/bold]")
    console.print("\n  Next steps:")
    console.print("  1. Create template:  orcest fleet create-template")
    console.print("  2. Set pool size:    orcest fleet set-pool-size <N>")
    console.print(
        "  3. Register an org:  orcest fleet add-org <org> --github-token ... --claude-token ..."
    )
    console.print("  4. Onboard a repo:   orcest fleet onboard <owner/repo>")


@fleet.command()
@click.argument("repo")
@click.option("--name", default=None, help="Project name (default: derived from repo).")
@click.option(
    "--config",
    default=str(DEFAULT_CONFIG_PATH),
    help="Fleet config path.",
    show_default=True,
)
@_serialized_fleet_operation
def onboard(repo: str, name: str | None, config: str) -> None:
    """Onboard a new repo: register project and deploy orchestrator stack.

    REPO is in "owner/repo" format (e.g. ThayneStudio/my-project).
    Workers are managed by the pool manager, not per-project.
    Requires the orchestrator VM to be created first (fleet create-orchestrator).
    """
    from orcest.fleet.config import ProjectEntry, load_config, save_config

    console = Console()
    cfg = load_config(config)
    project_name = name or _repo_to_project_name(repo)

    # Validate inputs
    _validate_repo(repo)
    _validate_project_name(project_name)

    console.print(f"\n[bold]Onboarding {repo} as '{project_name}'[/bold]\n")

    # Validate orchestrator is set up
    if not cfg.orchestrator.host:
        console.print(
            "[red]Orchestrator host not set in fleet config.[/red]\n"
            "  Run 'orcest fleet create-orchestrator' first."
        )
        sys.exit(1)

    # Resolve org credentials
    org_name = repo.split("/")[0] if "/" in repo else ""
    org = cfg.orgs.get(org_name)
    if not org:
        console.print(
            f"[red]Org '{org_name}' not found in fleet config.[/red]\n"
            f"  Run 'orcest fleet add-org {org_name} --github-token ... --claude-token ...' first."
        )
        sys.exit(1)

    # Check for duplicate
    if cfg.get_project(project_name):
        console.print(f"[red]Project '{project_name}' already exists in fleet config.[/red]")
        sys.exit(1)

    # A newly onboarded project must publish to the backend already consumed
    # by the deployed pool. Merely editing the local fleet config does not
    # coordinate a worker-template transition.
    _validate_backend_transition(
        cfg,
        config,
        console,
        allow_backend_change=False,
    )

    # Add project to config
    project = ProjectEntry(
        name=project_name,
        repo=repo,
    )
    cfg.projects.append(project)

    try:
        _validate_provider_stream_routing(cfg, console)
    except SystemExit:
        cfg.projects = [p for p in cfg.projects if p.name != project_name]
        raise

    console.print(f"  Project: {project_name}")
    console.print(f"  Repo: {repo}")

    # Step 1: Write project files to orchestrator
    ssh_target = cfg.ssh_target()
    console.print("\n  Deploying orchestrator stack...")
    try:
        from orcest.fleet.orchestrator import ensure_redis_password

        # C1: mint/persist the Redis AUTH password BEFORE generating the .env
        # (so it carries ORCEST_REDIS_PASSWORD) and BEFORE ensure_redis_stack
        # (so Redis never boots with an empty requirepass). Idempotent: reuses an
        # existing password, so re-onboarding never rotates it.
        redis_password = ensure_redis_password(ssh_target)

        _write_project_files_from_config(cfg, ssh_target, project, redis_password)
        console.print("  Project files written [green]ok[/green]")
    except Exception as exc:
        console.print(f"  Writing project files [red]failed[/red]: {exc}")
        cfg.projects = [p for p in cfg.projects if p.name != project_name]
        sys.exit(1)

    # Step 2: Ensure shared Redis stack is running, then deploy project stack
    try:
        from orcest.fleet.orchestrator import (
            deploy_stack,
            ensure_redis_stack,
            image_exists,
        )

        ensure_redis_stack(ssh_target)

        if not image_exists(ssh_target):
            from orcest.fleet.orchestrator import build_image

            console.print("  Docker image not found, building...")
            build_image(ssh_target)
            console.print("  Docker build [green]ok[/green]")

        deploy_stack(ssh_target, project_name)
        console.print("  Stack deployed [green]ok[/green]")
    except Exception as exc:
        console.print(f"  Deploy stack [red]failed[/red]: {exc}")
        cfg.projects = [p for p in cfg.projects if p.name != project_name]
        sys.exit(1)

    save_config(cfg, config)
    console.print(f"\n[bold]Project '{project_name}' onboarded.[/bold]")


@fleet.command()
@click.argument("project_name")
@click.option(
    "--config",
    default=str(DEFAULT_CONFIG_PATH),
    help="Fleet config path.",
    show_default=True,
)
@click.option("--yes", is_flag=True, help="Skip confirmation prompt.")
@_serialized_fleet_operation
def destroy(project_name: str, config: str, yes: bool) -> None:
    """Destroy a project: remove orchestrator stack and deregister.

    Tears down the Docker Compose stack on the orchestrator and removes
    the project from config. Workers are managed by the pool manager.
    """
    from orcest.fleet.config import load_config, save_config

    console = Console()
    cfg = load_config(config)

    project = cfg.get_project(project_name)
    if not project:
        console.print(f"[red]Project '{project_name}' not found.[/red]")
        sys.exit(1)

    if not yes:
        click.confirm(
            f"Destroy project '{project_name}'?",
            abort=True,
        )

    console.print(f"\n[bold]Destroying project '{project_name}'[/bold]")

    # Teardown orchestrator stack
    if cfg.orchestrator.host:
        ssh_target = cfg.ssh_target()
        console.print("  Tearing down orchestrator stack...", end=" ")
        try:
            from orcest.fleet.orchestrator import teardown_stack

            teardown_stack(ssh_target, project_name)
            console.print("[green]ok[/green]")
        except Exception as exc:
            console.print(f"[yellow]failed: {exc}[/yellow]")

    # Remove project from config
    cfg.projects = [p for p in cfg.projects if p.name != project_name]
    save_config(cfg, config)
    console.print(f"\n[bold]Project '{project_name}' destroyed.[/bold]")


@fleet.command()
@click.option(
    "--config",
    default=str(DEFAULT_CONFIG_PATH),
    help="Fleet config path.",
    show_default=True,
)
@click.option(
    "--skip-pool-manager",
    is_flag=True,
    hidden=True,
    help="Do not start/update the pool manager during this update.",
)
@click.pass_context
@_serialized_fleet_operation
def update(ctx: click.Context, config: str, skip_pool_manager: bool) -> None:
    """Update the fleet: rebuild Docker image and restart stacks.

    Uploads fresh source to the orchestrator, rebuilds the Docker image,
    and restarts all project stacks. Worker VMs are managed by the pool
    manager and will pick up changes on next clone cycle.
    """
    from orcest.fleet.config import load_config

    console = Console()
    cfg = load_config(config)

    if not cfg.orchestrator.host:
        console.print("[red]Orchestrator host not set in fleet config.[/red]")
        sys.exit(1)

    ssh_target = cfg.ssh_target()

    _validate_provider_stream_routing(cfg, console)
    _validate_deploy_source_revision(console)

    # Project configs and workers must change backend as one operation. The
    # authorization bit is set only by this process's coordinated deploy path;
    # there is deliberately no user-callable update flag that can bypass it.
    _validate_backend_transition(
        cfg,
        config,
        console,
        allow_backend_change=bool(ctx.meta.get(_COORDINATED_BACKEND_CHANGE_META_KEY, False)),
    )

    console.print("\n[bold]Updating fleet[/bold]\n")

    # Step 1: Upload source and rebuild Docker image
    try:
        from orcest.fleet.orchestrator import build_image, upload_source

        console.print("  Uploading fresh source...")
        upload_source(ssh_target)
        console.print("  Upload [green]ok[/green]")

        console.print("  Rebuilding Docker image...")
        build_image(ssh_target)
        console.print("  Docker build [green]ok[/green]")
    except Exception as exc:
        console.print(f"  [red]failed[/red]: {exc}")
        sys.exit(1)

    failures: list[str] = []

    redis_password: str | None = None

    # Step 2: Update shared Redis stack
    console.print("  Updating shared Redis stack...", end=" ")
    try:
        from orcest.fleet.orchestrator import ensure_redis_password, ensure_redis_stack

        # C1: ensure the password exists (idempotent; reuses an existing one) so
        # the --env-file'd stack restarts with --requirepass populated rather
        # than empty.
        redis_password = ensure_redis_password(ssh_target)
        ensure_redis_stack(ssh_target)
        console.print("[green]ok[/green]")
    except Exception as exc:
        console.print(f"[red]failed: {exc}[/red]")
        failures.append(f"shared Redis stack: {exc}")

    # Step 3: Regenerate project files and restart all project stacks before
    # workers are allowed to pull from the possibly-renamed task streams.
    from orcest.fleet.orchestrator import restart_stack

    for project in cfg.projects:
        if redis_password is None:
            console.print(
                f"  Updating project files for '{project.name}'... "
                "[yellow]skipped: Redis password unavailable[/yellow]"
            )
            failures.append(f"project files {project.name}: Redis password unavailable")
            continue

        console.print(f"  Updating project files for '{project.name}'...", end=" ")
        try:
            _write_project_files_from_config(cfg, ssh_target, project, redis_password)
            console.print("[green]ok[/green]")
        except Exception as exc:
            console.print(f"[red]failed: {exc}[/red]")
            failures.append(f"project files {project.name}: {exc}")
            continue

        if ctx.meta.get(_DEFER_PROJECT_START_META_KEY, False):
            console.print(
                f"  Starting stack for '{project.name}'... "
                "[yellow]deferred until candidate workers attest[/yellow]"
            )
            continue

        console.print(f"  Restarting stack for '{project.name}'...", end=" ")
        try:
            restart_stack(ssh_target, project.name)
            console.print("[green]ok[/green]")
        except Exception as exc:
            console.print(f"[red]failed: {exc}[/red]")
            failures.append(f"project stack {project.name}: {exc}")

    if failures:
        console.print(f"\n[bold red]Fleet update FAILED ({len(failures)} step(s)):[/bold red]")
        for f in failures:
            console.print(f"  - {f}")
        sys.exit(1)

    # Step 4: Update pool manager only after project stacks are publishing with
    # regenerated configs. Deploy defers this to its final start step.
    if skip_pool_manager:
        console.print("  Skipping pool manager update until final start step")
    elif (
        (cfg.pool.template_vm_id or cfg.pool.template_range() is not None)
        and cfg.proxmox.api_token_id
        and cfg.proxmox.api_token_secret
    ):
        if cfg.proxmox.is_localhost():
            console.print("  [yellow]Skipping pool manager: proxmox.endpoint is localhost[/yellow]")
            console.print(
                "  The pool manager runs on the orchestrator VM and needs the"
                " Proxmox host's real IP."
            )
            console.print("  Fix with: orcest init  (or edit /etc/orcest/config.yaml)")
        else:
            console.print("  Updating pool manager...", end=" ")
            try:
                from orcest.fleet.orchestrator import ensure_pool_manager, upload_fleet_config

                upload_fleet_config(ssh_target, config)
                ensure_pool_manager(ssh_target)
                console.print("[green]ok[/green]")
            except Exception as exc:
                console.print(f"[red]failed: {exc}[/red]")
                failures.append(f"pool manager: {exc}")

    if failures:
        console.print(f"\n[bold red]Fleet update FAILED ({len(failures)} step(s)):[/bold red]")
        for f in failures:
            console.print(f"  - {f}")
        sys.exit(1)

    console.print("\n[bold]Fleet update complete.[/bold]")


@fleet.command()
@click.option(
    "--config",
    default=str(DEFAULT_CONFIG_PATH),
    help="Fleet config path.",
    show_default=True,
)
def status(config: str) -> None:
    """Show fleet status: orchestrator, projects, and workers."""
    from orcest.fleet.config import load_config, validate_project_name

    console = Console()
    cfg = load_config(config)

    # Orchestrator status
    orch_table = Table(title="Orchestrator")
    orch_table.add_column("Property", style="cyan")
    orch_table.add_column("Value", style="white")

    orch_table.add_row("Host", cfg.orchestrator.host or "[dim]not set[/dim]")
    orch_table.add_row("User", cfg.orchestrator.user)
    orch_table.add_row("VM ID", str(cfg.orchestrator.vm_id))

    # Try SSH ping to check status
    if cfg.orchestrator.host:
        ssh_target = cfg.ssh_target()
        result = subprocess.run(
            ["ssh", *_SSH_OPTS, ssh_target, "true"],
            capture_output=True,
            text=True,
        )
        ssh_status = (
            "[green]reachable[/green]" if result.returncode == 0 else "[red]unreachable[/red]"
        )
        orch_table.add_row("SSH Status", ssh_status)

    console.print(orch_table)

    # Orgs
    if cfg.orgs:
        org_table = Table(title="Registered Orgs")
        org_table.add_column("Org", style="cyan")
        org_table.add_column("GitHub Token", style="green")
        org_table.add_column("Claude Token", style="yellow")

        for org_name, org_entry in cfg.orgs.items():
            gh = f"{org_entry.github_token[:8]}..." if org_entry.github_token else "[dim]none[/dim]"
            cl = (
                f"{org_entry.claude_oauth_token[:8]}..."
                if org_entry.claude_oauth_token
                else "[dim]none[/dim]"
            )
            org_table.add_row(org_name, gh, cl)

        console.print(org_table)

    # Projects
    if not cfg.projects:
        console.print("\n[dim]No projects in fleet config.[/dim]")
        return

    proj_table = Table(title="Projects")
    proj_table.add_column("Project", style="cyan")
    proj_table.add_column("Repo", style="white")
    proj_table.add_column("Stack Status", style="magenta")

    for project in cfg.projects:
        if not validate_project_name(project.name):
            # Skip projects with invalid names rather than aborting the
            # entire status display.  This can happen if the config file
            # was hand-edited with an invalid name.
            proj_table.add_row(
                project.name,
                project.repo,
                "[red]invalid name[/red]",
            )
            continue
        stack_status = "[dim]unknown[/dim]"
        if cfg.orchestrator.host:
            ssh_target = cfg.ssh_target()
            result = subprocess.run(
                [
                    "ssh",
                    *_SSH_OPTS,
                    ssh_target,
                    f"cd /opt/orcest && docker compose"
                    f" -p orcest-{project.name}"
                    f" ps --format json 2>/dev/null",
                ],
                capture_output=True,
                text=True,
            )
            if result.returncode == 0 and result.stdout.strip():
                stack_status = "[green]running[/green]"
            elif result.returncode == 0:
                stack_status = "[yellow]stopped[/yellow]"
            else:
                stack_status = "[red]error[/red]"

        proj_table.add_row(
            project.name,
            project.repo,
            stack_status,
        )

    console.print(proj_table)

    # Pool info
    pool_table = Table(title="Worker Pool")
    pool_table.add_column("Property", style="cyan")
    pool_table.add_column("Value", style="white")
    pool_table.add_row("Target Size", str(cfg.pool.size))
    tmpl_id = str(cfg.pool.template_vm_id) if cfg.pool.template_vm_id else "[dim]not set[/dim]"
    pool_table.add_row("Template VM ID", tmpl_id)
    pool_table.add_row("Storage", cfg.pool.storage)
    pool_table.add_row("Worker Memory", f"{cfg.pool.worker_memory} MB")
    pool_table.add_row("Worker Cores", str(cfg.pool.worker_cores))
    console.print(pool_table)


def _resolve_image_checksum(image_url: str, cfg: FleetConfig, console: Console) -> str:
    """Return a VERIFIED sha256 hex digest for *image_url* (M5-infra).

    Fail-closed image integrity for the template cloud image, mirroring
    ``provision/create-vm.sh`` (GPG-verify ``SHA256SUMS`` then ``sha256sum -c``)
    so the Proxmox node can verify the bytes it downloads as root.

    Resolution order:

    1. If ``pool.expected_image_sha256`` is set (64 hex chars), use it directly
       -- the offline / air-gapped pin. No network, no GPG fetch.
    2. Otherwise fetch the image's published ``SHA256SUMS`` + ``SHA256SUMS.gpg``
       (same directory as the image), import + GPG-verify against
       ``pool.expected_image_gpg_key`` (the ``VALIDSIG ... <fpr>`` line), and
       extract the digest for the pinned image filename.

    Raises:
        RuntimeError: on any verification failure (bad signature, missing
            filename in SHA256SUMS, fetch failure). The caller aborts the bake
            rather than download the image unverified.
    """
    from urllib.parse import urlparse

    # 1) Pinned digest short-circuit (offline / air-gapped).
    pinned = (cfg.pool.expected_image_sha256 or "").strip().lower()
    if pinned:
        if not re.fullmatch(r"[0-9a-f]{64}", pinned):
            raise RuntimeError(f"pool.expected_image_sha256 must be 64 hex chars, got {pinned!r}")
        console.print("  Image checksum: [green]pinned (config)[/green]")
        return pinned

    # 2) Fetch + GPG-verify the published SHA256SUMS.
    gpg_key = (cfg.pool.expected_image_gpg_key or "").strip()
    if not gpg_key:
        raise RuntimeError(
            "Image integrity is unverifiable: neither pool.expected_image_sha256"
            " nor pool.expected_image_gpg_key is set. Refusing to download the"
            " cloud image unverified."
        )

    parsed = urlparse(image_url)
    image_filename = parsed.path.rsplit("/", 1)[-1]
    base_url = image_url.rsplit("/", 1)[0]
    sums_url = f"{base_url}/SHA256SUMS"
    sig_url = f"{base_url}/SHA256SUMS.gpg"

    console.print("  Verifying image checksum (GPG)...", end=" ")
    workdir = Path(tempfile.mkdtemp(prefix="orcest-img-verify-"))
    sums_path = workdir / "SHA256SUMS"
    sig_path = workdir / "SHA256SUMS.gpg"
    gnupg_home = workdir / "gnupg"
    try:
        gnupg_home.mkdir(mode=0o700, exist_ok=True)

        def _run(cmd: list[str], *, what: str) -> subprocess.CompletedProcess[str]:
            try:
                return subprocess.run(cmd, capture_output=True, text=True, timeout=120)
            except (OSError, subprocess.SubprocessError) as exc:
                raise RuntimeError(f"image checksum: {what} failed: {exc}") from exc

        # Fetch SHA256SUMS and its detached signature (no redirects).
        r = _run(
            ["curl", "-fsSL", "--max-redirs", "0", "-o", str(sums_path), sums_url],
            what="fetch SHA256SUMS",
        )
        if r.returncode != 0 or not sums_path.exists():
            raise RuntimeError(f"image checksum: could not fetch {sums_url}: {r.stderr.strip()}")
        r = _run(
            ["curl", "-fsSL", "--max-redirs", "0", "-o", str(sig_path), sig_url],
            what="fetch SHA256SUMS.gpg",
        )
        if r.returncode != 0 or not sig_path.exists():
            raise RuntimeError(f"image checksum: could not fetch {sig_url}: {r.stderr.strip()}")

        # Import the expected signing key into the throwaway keyring, then
        # GPG-verify the signature. Anchor on the primary-key fingerprint in
        # the VALIDSIG status line (Ubuntu may sign with a subkey).
        gpg_base = ["gpg", "--homedir", str(gnupg_home), "--batch"]
        _run(
            [*gpg_base, "--keyserver", "hkps://keyserver.ubuntu.com", "--recv-keys", gpg_key],
            what="import signing key",
        )
        verify = _run(
            [*gpg_base, "--status-fd", "1", "--verify", str(sig_path), str(sums_path)],
            what="gpg --verify",
        )
        validsig = any(
            line.startswith("[GNUPG:] VALIDSIG") and line.rstrip().endswith(gpg_key)
            for line in (verify.stdout or "").splitlines()
        )
        if verify.returncode != 0 or not validsig:
            raise RuntimeError(
                "image checksum: GPG signature verification failed or was signed by"
                f" an unexpected key (expected {gpg_key})."
            )

        # Extract the digest for the pinned image filename. Lines look like
        # "<sha256> *noble-server-cloudimg-amd64.img" (or two-space separator).
        digest = ""
        for line in sums_path.read_text().splitlines():
            parts = line.split()
            if len(parts) == 2:
                sha, name = parts
                if name.lstrip("*") == image_filename:
                    digest = sha.strip().lower()
                    break
        if not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise RuntimeError(
                f"image checksum: no sha256 for {image_filename!r} found in the"
                " GPG-verified SHA256SUMS."
            )
        console.print("[green]ok[/green]")
        return digest
    except RuntimeError:
        console.print("[red]failed[/red]")
        raise
    finally:
        import shutil

        shutil.rmtree(workdir, ignore_errors=True)


def _create_vm_from_cloud_image(
    px: ProxmoxClient,
    cfg: FleetConfig,
    vm_id: int,
    image_url: str,
    console: Console,
    *,
    storage: str | None = None,
    snippet_storage: str = "local",
) -> None:
    """Download a cloud image and create a VM with it as the boot disk.

    Uses the Proxmox ``download-url`` API to fetch the image, then creates
    a VM with ``import-from`` to use the downloaded image as the boot disk.
    Disk is resized to ``cfg.pool.worker_disk_size``.

    Args:
        storage: Proxmox storage for the VM boot disk. Falls back to
            ``cfg.pool.storage``.
        snippet_storage: Proxmox storage for cloud-init drive.

    Raises on any failure — caller is responsible for destroying the VM.
    """
    from urllib.parse import urlparse

    parsed = urlparse(image_url)
    if parsed.scheme not in ("https", "http"):
        raise ValueError(f"Invalid image URL scheme: {parsed.scheme!r} (expected http or https)")

    if storage is None:
        storage = cfg.pool.storage
    # Derive filename from the URL path and sanitize it
    raw_filename = image_url.rsplit("/", 1)[-1].split("?")[0] or "cloud-image.img"
    filename = re.sub(r"[^a-zA-Z0-9._-]", "_", raw_filename)
    if not filename or filename.startswith("."):
        filename = "cloud-image.img"

    # Step 0: Resolve a VERIFIED sha256 for the image (fail-closed). Either a
    # GPG-verified entry from the image's published SHA256SUMS, or a pinned
    # pool.expected_image_sha256. Raises (aborting the bake) if the digest
    # cannot be resolved/verified -- we never download the cloud image (run as
    # root on the Proxmox node) without integrity verification.
    image_sha256 = _resolve_image_checksum(image_url, cfg, console)

    # Step 1: Download cloud image to Proxmox local storage (skip if already
    # present). The node verifies the downloaded bytes against image_sha256.
    download_storage = "local"
    console.print("  Downloading cloud image...", end=" ")
    try:
        px.download_image(
            image_url,
            filename,
            storage=download_storage,
            checksum=image_sha256,
            checksum_algorithm="sha256",
        )
        console.print("[green]ok[/green]")
    except RuntimeError as exc:
        if "already exists" in str(exc) or "override existing" in str(exc):
            console.print("[yellow]already cached[/yellow]")
        else:
            raise

    # Step 2: Create VM (without disk — import-from requires root which
    # API tokens don't have, so we import the disk via qm CLI in step 3)
    console.print("  Creating VM...", end=" ")
    px.create_vm(
        vm_id=vm_id,
        name="orcest-worker-template",
        memory=cfg.pool.worker_memory,
        cores=cfg.pool.worker_cores,
        cpu="host",
        scsihw="virtio-scsi-pci",
        ide2=f"{storage}:cloudinit",
        net0="virtio,bridge=vmbr0",
        ipconfig0="ip=dhcp",
        serial0="socket",
        vga="serial0",
        agent="1",
    )
    console.print("[green]ok[/green]")

    try:
        # Step 3: Import cloud image as boot disk via qm CLI (runs as root
        # on the Proxmox host, bypassing API token filesystem path restrictions)
        image_path = f"/var/lib/vz/template/iso/{filename}"
        console.print("  Importing boot disk...", end=" ")
        result = subprocess.run(
            [
                "qm",
                "set",
                str(vm_id),
                "--scsi0",
                f"{storage}:0,import-from={image_path},discard=on,ssd=1",
                "--boot",
                "order=scsi0",
            ],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"qm set failed: {(result.stderr or result.stdout).strip()}")
        console.print("[green]ok[/green]")

        # Step 4: Resize disk to configured worker size
        console.print(f"  Resizing disk to {cfg.pool.worker_disk_size}G...", end=" ")
        px.resize_disk(vm_id, "scsi0", f"{cfg.pool.worker_disk_size}G")
        console.print("[green]ok[/green]")
    except Exception as exc:
        # ``create_vm`` returned successfully, so the operation lock plus the
        # previously-free VMID proves this invocation owns cleanup. A failure
        # before that point is deliberately left untouched because an API
        # timeout/collision cannot prove ownership.
        raise _OwnedTemplateVmCreationError(str(exc)) from exc


@fleet.command("create-template")
@click.option("--vm-id", type=int, default=None, help="VM ID for the new template.")
@click.option(
    "--image-url",
    default=_DEFAULT_CLOUD_IMAGE_URL,
    help="Cloud image URL to download.",
    show_default=True,
)
@click.option(
    "--storage",
    default=None,
    help="Proxmox storage for VM disk (skip interactive prompt).",
)
@click.option(
    "--config",
    default=str(DEFAULT_CONFIG_PATH),
    help="Fleet config path.",
    show_default=True,
)
@_serialized_fleet_operation
def create_template(vm_id: int | None, image_url: str, storage: str | None, config: str) -> None:
    """Create a worker VM template for the warm pool.

    Downloads a cloud image, creates a VM, installs worker tools
    via cloud-init, then converts to a template for fast linked cloning.
    """
    from orcest.fleet.config import load_config, save_config

    console = Console()
    cfg = load_config(config)

    if not cfg.proxmox.api_token_id or not cfg.proxmox.api_token_secret:
        console.print("[red]Proxmox API credentials not configured.[/red]")
        console.print("  Set proxmox.api_token_id and proxmox.api_token_secret in fleet config.")
        sys.exit(1)

    px = _create_proxmox_client(cfg)

    # Select storage for VM disk
    if storage is None:
        storage = _prompt_storage(
            px,
            "images",
            "template VM disk",
            console,
            default=cfg.pool.storage,
        )
    cfg.pool.storage = storage

    # Auto-detect snippet storage for cloud-init
    snippet_storage = _find_snippet_storage(px, console)
    cfg.pool.snippet_storage = snippet_storage

    # Prompt for template VM ID
    if vm_id is None:
        default_id = _next_free_vmid()
        vm_id = click.prompt("  VM ID for new template", default=default_id, type=int)

    # Check if VM already exists and offer to replace
    existing_vms = {int(v["vmid"]) for v in px.list_vms() if "vmid" in v}
    if vm_id in existing_vms:
        if not click.confirm(f"  VM {vm_id} already exists. Destroy and replace it?"):
            console.print("Aborted.")
            sys.exit(0)
        console.print(f"  Destroying existing VM {vm_id}...", end=" ")
        try:
            px.stop_vm(vm_id)
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                if px.get_vm_status(vm_id) == "stopped":
                    break
                time.sleep(1)
        except Exception:
            pass  # May already be stopped or a template
        px.destroy_vm(vm_id)
        console.print("[green]ok[/green]")

    _create_template_at_vmid(
        px,
        cfg,
        vm_id,
        image_url=image_url,
        storage=storage,
        snippet_storage=snippet_storage,
        console=console,
    )

    # Save template_vm_id and prompt for worker VM ID range.
    # The worker range must stay disjoint from the template range, otherwise
    # load_config() raises on every subsequent fleet command and the
    # pool-manager container refuses to start. Offer a valid default and never
    # persist a value that would not load back.
    cfg.pool.template_vm_id = vm_id
    template_rng = cfg.pool.template_range()
    default_start = template_rng[1] + 1 if template_rng is not None else vm_id + 1
    previous_start = cfg.pool.vm_id_start
    vm_id_start = default_start
    for attempt in range(3):
        vm_id_start = click.prompt(
            "  Worker VM ID range starts at",
            default=default_start,
            type=int,
        )
        cfg.pool.vm_id_start = vm_id_start
        try:
            cfg.pool.validate_vmid_ranges()
        except ValueError as exc:
            cfg.pool.vm_id_start = previous_start
            console.print(f"  [red]Invalid pool configuration: {exc}[/red]")
            if attempt == 2:
                console.print(
                    f"  [red]Not saving vm_id_start; template VM {vm_id} was still"
                    " created.[/red]\n"
                    "  Rerun with a valid worker VMID range, or set pool.vm_id_start"
                    " in the fleet config."
                )
                sys.exit(1)
            continue
        break
    save_config(cfg, config)

    console.print(f"\n[bold]Worker template created (VM {vm_id}).[/bold]")
    console.print(f"  Saved template_vm_id={vm_id}, vm_id_start={vm_id_start} to fleet config.")


def _create_template_at_vmid(
    px: ProxmoxClient,
    cfg: FleetConfig,
    vm_id: int,
    *,
    image_url: str,
    storage: str,
    snippet_storage: str,
    console: Console,
) -> None:
    """Bake a worker template at *vm_id*: download image, provision, convert.

    Shared by the original ``create-template`` command (which manages the
    single-VMID config) and the new ``rebake`` command (which allocates a
    fresh VMID from the template range and atomically swaps the Redis
    pointer on success).

    Calls :func:`sys.exit(1)` on any failure after best-effort cleanup of
    the half-built VM.  Caller is responsible for any post-success bookkeeping
    (config save, Redis pointer swap).
    """
    from orcest.fleet.cloud_init import render_template_userdata

    console.print(f"\n[bold]Creating worker template (VM {vm_id})[/bold]\n")

    # Step 1: Create VM from cloud image
    try:
        _create_vm_from_cloud_image(
            px,
            cfg,
            vm_id,
            image_url,
            console,
            storage=storage,
            snippet_storage=snippet_storage,
        )
    except Exception as exc:
        console.print(f"  [red]failed[/red]: {exc}")
        if isinstance(exc, _OwnedTemplateVmCreationError):
            # Best-effort cleanup only when VM creation ownership is proven.
            try:
                px.destroy_vm(vm_id)
            except Exception:
                pass
        sys.exit(1)

    # Steps 2-9 can all fail; on any failure we destroy the VM
    # to avoid leaving orphaned resources.
    def _cleanup_vm() -> None:
        console.print("  Cleaning up: destroying VM...")
        try:
            # Stop the VM first -- Proxmox refuses to delete running VMs.
            # Best-effort; the VM may already be stopped or never started.
            try:
                px.stop_vm(vm_id)
                # Brief wait for it to actually stop before destroying
                stop_deadline = time.monotonic() + 15
                while time.monotonic() < stop_deadline:
                    if px.get_vm_status(vm_id) == "stopped":
                        break
                    time.sleep(1)
            except Exception:
                pass  # VM may already be stopped or never started
            px.destroy_vm(vm_id)
        except Exception:
            console.print("  [yellow]Warning: cleanup failed; VM may need manual removal.[/yellow]")

    # Step 2: Configure cloud-init userdata
    console.print("  Configuring cloud-init...", end=" ")
    try:
        userdata = render_template_userdata(
            ssh_public_key=cfg.orchestrator.ssh_key,
        )
        _set_vm_cloud_init(px, vm_id, userdata, snippet_storage=snippet_storage)
        console.print("[green]ok[/green]")
    except Exception as exc:
        console.print(f"[red]failed[/red]: {exc}")
        _cleanup_vm()
        sys.exit(1)

    # Step 3: Start the VM
    console.print("  Starting VM...", end=" ")
    try:
        px.start_vm(vm_id)
        console.print("[green]ok[/green]")
    except Exception as exc:
        console.print(f"[red]failed[/red]: {exc}")
        _cleanup_vm()
        sys.exit(1)

    # Step 4: Wait for IP (uses ARP fallback so we don't need to wait
    # for cloud-init to install qemu-guest-agent first)
    vm_ip = _get_vm_ip(vm_id, console, timeout=600)
    if not vm_ip:
        console.print("  Could not get VM IP. Template creation aborted.")
        _cleanup_vm()
        sys.exit(1)

    # Step 5: Wait for SSH
    if not _wait_for_ssh(vm_ip, cfg.orchestrator.user, console):
        console.print("[red]SSH not available. Template creation aborted.[/red]")
        _cleanup_vm()
        sys.exit(1)

    # Step 6: Wait for cloud-init to finish
    if not _wait_for_cloud_init(vm_ip, cfg.orchestrator.user, console):
        console.print("[red]Cloud-init timed out. Template creation aborted.[/red]")
        _cleanup_vm()
        sys.exit(1)

    # Step 6b: Smoke-check the baked provider CLIs are actually on PATH for the
    # orcest worker user BEFORE the irreversible convert-to-template / pointer
    # swap. cloud-init reporting `done` does not prove every install in runcmd
    # succeeded (no cross-entry `set -e`), so a failed provider-CLI install
    # could otherwise flip the pool pointer to a broken template.
    if not _verify_provider_clis(vm_ip, cfg.orchestrator.user, console):
        console.print("[red]Provider-CLI smoke-check failed. Template creation aborted.[/red]")
        _cleanup_vm()
        sys.exit(1)

    # Step 6c: overwrite the bootstrap install with the same source tree used
    # for orchestrator deploys. Pool clones do not fetch GitHub at boot; they
    # inherit this verified template install.
    console.print("  Installing current orcest source into template...", end=" ")
    if not _install_source_on_worker_template(vm_ip, cfg.orchestrator.user, console):
        console.print("[red]Source install failed. Template creation aborted.[/red]")
        _cleanup_vm()
        sys.exit(1)

    # Step 7: Clean cloud-init state so clones run fresh cloud-init
    # (clones get per-VM cloud-init userdata from the pool manager)
    console.print("  Cleaning cloud-init state...", end=" ")
    try:
        result = _ssh_run(vm_ip, cfg.orchestrator.user, "sudo rm -rf /var/lib/cloud/*")
    except subprocess.TimeoutExpired:
        console.print("[red]timed out[/red]")
        _cleanup_vm()
        sys.exit(1)
    if result.returncode != 0:
        console.print(f"[red]failed[/red]: {result.stderr.strip()}")
        _cleanup_vm()
        sys.exit(1)
    console.print("[green]ok[/green]")

    # Step 7b: Prepare template for unique clone identities.
    # Clear machine-id so each linked clone gets a unique one on first boot.
    console.print("  Preparing template for cloning...", end=" ")
    try:
        result = _ssh_run(
            vm_ip,
            cfg.orchestrator.user,
            "sudo truncate -s 0 /etc/machine-id && sudo rm -f /var/lib/dbus/machine-id",
        )
    except subprocess.TimeoutExpired:
        console.print("[red]timed out[/red]")
        _cleanup_vm()
        sys.exit(1)
    if result.returncode != 0:
        console.print(f"[red]failed[/red]: {result.stderr.strip()}")
        _cleanup_vm()
        sys.exit(1)
    console.print("[green]ok[/green]")

    # Step 8: Flush filesystem and gracefully shut down the VM.
    console.print("  Syncing filesystem...", end=" ")
    try:
        result = _ssh_run(vm_ip, cfg.orchestrator.user, "sudo sync", timeout=120)
    except subprocess.TimeoutExpired:
        console.print("[red]timed out[/red]")
        _cleanup_vm()
        sys.exit(1)
    if result.returncode != 0:
        console.print(f"[red]failed[/red]: {result.stderr.strip()}")
        _cleanup_vm()
        sys.exit(1)
    console.print("[green]ok[/green]")

    console.print("  Shutting down VM...", end=" ")
    try:
        px.shutdown_vm(vm_id, timeout=60)
        if px.get_vm_status(vm_id) != "stopped":
            console.print("[red]VM did not stop[/red]")
            _cleanup_vm()
            sys.exit(1)
        console.print("[green]ok[/green]")
    except Exception as exc:
        console.print(f"[red]failed[/red]: {exc}")
        _cleanup_vm()
        sys.exit(1)

    # Step 9: Convert to template
    console.print("  Converting to template...", end=" ")
    try:
        px.convert_to_template(vm_id)
        console.print("[green]ok[/green]")
    except Exception as exc:
        console.print(f"[red]failed[/red]: {exc}")
        _cleanup_vm()
        sys.exit(1)


def _set_vm_cloud_init(
    px: ProxmoxClient,
    vm_id: int,
    userdata: str,
    snippet_storage: str = "local",
) -> None:
    """Set cloud-init user-data on a VM.

    Writes the snippet directly to the Proxmox host filesystem and
    configures ``cicustom`` via ``qm set``. This avoids the snippet
    upload API which can fail with certain API token configurations.

    Args:
        snippet_storage: Proxmox storage name for snippets (default ``"local"``).
    """
    from pathlib import Path

    snippet_name = f"orcest-template-{vm_id}-user.yaml"
    snippets_dir = Path("/var/lib/vz/snippets")
    snippets_dir.mkdir(parents=True, exist_ok=True)
    (snippets_dir / snippet_name).write_text(userdata)
    result = subprocess.run(
        ["qm", "set", str(vm_id), "--cicustom", f"user={snippet_storage}:snippets/{snippet_name}"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"qm set --cicustom failed: {(result.stderr or result.stdout).strip()}")


@fleet.command("pool-status")
@click.option(
    "--config",
    default=str(DEFAULT_CONFIG_PATH),
    help="Fleet config path.",
    show_default=True,
)
def pool_status(config: str) -> None:
    """Show worker pool status: template info, idle/active VMs."""
    from orcest.fleet.config import load_config
    from orcest.fleet.orchestrator import get_current_template_vmid

    console = Console()
    cfg = load_config(config)

    active_template: int | None = None
    template_source = "config"
    if cfg.orchestrator.host:
        try:
            redis_vmid = get_current_template_vmid(cfg.ssh_target())
        except Exception as exc:
            console.print(f"[yellow]Warning: could not read Redis template pointer:[/yellow] {exc}")
            redis_vmid = None
        if redis_vmid is not None:
            active_template = redis_vmid
            template_source = "Redis pointer"
    if active_template is None and cfg.pool.template_vm_id:
        active_template = cfg.pool.template_vm_id

    if active_template:
        tmpl_display = f"{active_template} [dim]({template_source})[/dim]"
    else:
        tmpl_display = "[dim]not set[/dim]"

    # Pool configuration table
    pool_table = Table(title="Worker Pool Configuration")
    pool_table.add_column("Property", style="cyan")
    pool_table.add_column("Value", style="white")

    pool_table.add_row("Target Size", str(cfg.pool.size))
    pool_table.add_row("Template VM ID", tmpl_display)
    pool_table.add_row("Storage", cfg.pool.storage)
    pool_table.add_row("Worker Memory", f"{cfg.pool.worker_memory} MB")
    pool_table.add_row("Worker Cores", str(cfg.pool.worker_cores))
    pool_table.add_row("Worker Disk Size", f"{cfg.pool.worker_disk_size} GB")
    pool_table.add_row("Max Task Duration", f"{cfg.pool.max_task_duration}s")
    console.print(pool_table)

    if not active_template:
        console.print(
            "\n[yellow]No template configured.[/yellow]\n"
            "  Run 'orcest fleet create-template' first."
        )
        return

    # Check template status via Proxmox API
    if not cfg.proxmox.api_token_id or not cfg.proxmox.api_token_secret:
        console.print(
            "\n[yellow]Proxmox API credentials not configured -- cannot query VMs.[/yellow]"
        )
        return

    px = _create_proxmox_client(cfg)

    # Check if template exists
    console.print(f"\n  Checking template VM {active_template}...", end=" ")
    try:
        tpl_status = px.get_vm_status(active_template)
        console.print(f"[green]{tpl_status}[/green]")
    except Exception as exc:
        console.print(f"[red]not found[/red]: {exc}")
        return

    # List worker VMs (VMs named orcest-worker-*)
    console.print("\n  Scanning for worker VMs...")
    try:
        worker_vms = [
            vm for vm in px.list_vms(name_prefix="orcest-worker-") if not _is_proxmox_template(vm)
        ]
    except Exception as exc:
        console.print(f"  [red]Failed to list VMs[/red]: {exc}")
        return

    if not worker_vms:
        console.print("  [dim]No worker VMs found.[/dim]")
        return

    vm_table = Table(title="Worker VMs")
    vm_table.add_column("VM ID", style="cyan")
    vm_table.add_column("Name", style="white")
    vm_table.add_column("Status", style="magenta")
    vm_table.add_column("CPU", style="yellow")
    vm_table.add_column("Memory", style="yellow")

    running = 0
    stopped = 0
    for vm in sorted(worker_vms, key=lambda v: v.get("vmid", 0)):
        vm_status = vm.get("status", "unknown")
        if vm_status == "running":
            running += 1
            status_str = "[green]running[/green]"
        elif vm_status == "stopped":
            stopped += 1
            status_str = "[dim]stopped[/dim]"
        else:
            status_str = f"[yellow]{vm_status}[/yellow]"

        mem_mb = vm.get("maxmem", 0) // (1024 * 1024)
        vm_table.add_row(
            str(vm.get("vmid", "?")),
            vm.get("name", "?"),
            status_str,
            str(vm.get("cpus", "?")),
            f"{mem_mb} MB",
        )

    console.print(vm_table)
    console.print(f"\n  Total: {len(worker_vms)} VMs ({running} running, {stopped} stopped)")
    console.print(f"  Target pool size: {cfg.pool.size}")


def _allocate_template_vmid(
    px: ProxmoxClient,
    cfg: FleetConfig,
    *,
    skip: int | None = None,
) -> int:
    """Pick the next free VMID in ``pool.template_vmid_range``.

    Scans Proxmox for VMIDs already in use within the range and returns the
    lowest free one. *skip* is excluded from candidates (used to avoid the
    currently-active template even if it has been destroyed but still
    appears in Proxmox).

    Raises :class:`SystemExit(1)` if no range is configured or the range
    is exhausted.
    """
    console = Console()
    rng = cfg.pool.template_range()
    if rng is None:
        console.print(
            "[red]pool.template_vmid_range is not configured.[/red]\n"
            "  Add e.g. 'template_vmid_range: [9000, 9009]' to /etc/orcest/config.yaml,"
            " or use 'orcest fleet create-template' for the legacy single-VMID flow."
        )
        sys.exit(1)
    start, end = rng

    in_use: set[int] = set()
    try:
        for vm in px.list_vms():
            vmid = vm.get("vmid")
            if vmid is not None:
                in_use.add(int(vmid))
    except Exception as exc:
        console.print(f"[red]Failed to list VMs from Proxmox:[/red] {exc}")
        sys.exit(1)

    for candidate in range(start, end + 1):
        if candidate in in_use or candidate == skip:
            continue
        return candidate

    console.print(
        f"[red]Template VMID range exhausted: all of {start}-{end} are in use.[/red]\n"
        "  Run 'orcest fleet gc-templates' to garbage-collect old templates,"
        " then retry."
    )
    sys.exit(1)


@fleet.command("rebake")
@click.option(
    "--image-url",
    default=_DEFAULT_CLOUD_IMAGE_URL,
    help="Cloud image URL to download.",
    show_default=True,
)
@click.option(
    "--storage",
    default=None,
    help="Proxmox storage for VM disk (default: pool.storage from config).",
)
@click.option(
    "--config",
    default=str(DEFAULT_CONFIG_PATH),
    help="Fleet config path.",
    show_default=True,
)
@_serialized_fleet_operation
def rebake(image_url: str, storage: str | None, config: str) -> None:
    """Bake a new worker template and atomically swap the active pointer.

    Allocates the next free VMID from ``pool.template_vmid_range``, builds
    a fresh template there, then sets the Redis pointer
    ``orcest:pool:current_template_vmid`` so the pool manager picks it up
    on its next reconciliation cycle (~10s).

    Never touches the previous template. Old templates with live linked
    clones stay alive until their clones churn out, then can be cleaned
    up with ``orcest fleet gc-templates``.

    On any failure the half-built VM is destroyed and the pointer is left
    untouched, so the active template is unchanged.
    """
    from orcest.fleet.config import load_config
    from orcest.fleet.orchestrator import _REDIS_CLI_PREFIX, set_current_template_vmid

    console = Console()
    cfg = load_config(config)

    if not cfg.proxmox.api_token_id or not cfg.proxmox.api_token_secret:
        console.print("[red]Proxmox API credentials not configured.[/red]")
        sys.exit(1)

    if not cfg.orchestrator.host:
        console.print("[red]Orchestrator host not set — cannot reach Redis to swap pointer.[/red]")
        sys.exit(1)

    px = _create_proxmox_client(cfg)

    if storage is None:
        storage = cfg.pool.storage or _prompt_storage(
            px, "images", "template VM disk", console, default=cfg.pool.storage
        )
    snippet_storage = cfg.pool.snippet_storage or _find_snippet_storage(px, console)

    new_vmid = _allocate_template_vmid(px, cfg, skip=cfg.pool.template_vm_id or None)
    rng = cfg.pool.template_range()
    assert rng is not None  # _allocate_template_vmid would have exited
    console.print(f"\n[bold]Rebaking template at VM {new_vmid}[/bold] (range {rng[0]}-{rng[1]})\n")

    _create_template_at_vmid(
        px,
        cfg,
        new_vmid,
        image_url=image_url,
        storage=storage,
        snippet_storage=snippet_storage,
        console=console,
    )

    # Atomic swap: pool manager picks up the new VMID on its next cycle.
    console.print("\n  Swapping active template pointer...", end=" ")
    try:
        set_current_template_vmid(cfg.ssh_target(), new_vmid)
        console.print("[green]ok[/green]")
    except Exception as exc:
        console.print(f"[red]failed[/red]: {exc}")
        redis_set_cmd = f"{_REDIS_CLI_PREFIX} SET orcest:pool:current_template_vmid {new_vmid}"
        console.print(
            "  [yellow]New template VM "
            f"{new_vmid}[/yellow] was built successfully but the pointer swap failed.\n"
            "  Set it manually with:"
            f" ssh {cfg.ssh_target()} {shlex.quote(redis_set_cmd)}"
        )
        sys.exit(1)

    console.print(
        f"\n[bold]Rebake complete.[/bold] Active template is now VM {new_vmid}.\n"
        "  New clones will use this template. Existing pool VMs are not replaced until\n"
        "  they are drained/stopped or the pool needs additional capacity."
    )


@fleet.command("destroy-template")
@click.argument("vm_id", type=int)
@click.option(
    "--yes",
    is_flag=True,
    help="Skip confirmation prompt.",
)
@click.option(
    "--config",
    default=str(DEFAULT_CONFIG_PATH),
    help="Fleet config path.",
    show_default=True,
)
@_serialized_fleet_operation
def destroy_template(vm_id: int, yes: bool, config: str) -> None:
    """Destroy a worker template VM.

    Refuses if VM_ID is the currently-active template (would leave the pool
    without one) or if any linked clones still reference it (Proxmox would
    reject the destroy anyway, but a clear pre-flight error is friendlier).
    """
    from orcest.fleet.config import load_config
    from orcest.fleet.orchestrator import get_current_template_vmid

    console = Console()
    cfg = load_config(config)

    if not cfg.proxmox.api_token_id or not cfg.proxmox.api_token_secret:
        console.print("[red]Proxmox API credentials not configured.[/red]")
        sys.exit(1)

    px = _create_proxmox_client(cfg)

    # Refuse to destroy the active template (Redis pointer or single-VMID config).
    active: int | None = None
    if cfg.orchestrator.host:
        try:
            active = get_current_template_vmid(cfg.ssh_target())
        except Exception as exc:
            console.print(f"[yellow]Warning: could not read active pointer:[/yellow] {exc}")
    if active is None and cfg.pool.template_vm_id:
        active = cfg.pool.template_vm_id
    if active is None:
        console.print(
            "[red]Could not determine the active template VMID[/red] "
            "(Redis pointer unreadable and no pool.template_vm_id fallback).\n"
            f"  Refusing to destroy VM {vm_id} in case it is the live template.\n"
            "  Verify the pointer or set pool.template_vm_id, then retry."
        )
        sys.exit(1)
    if active == vm_id:
        console.print(
            f"[red]Refusing to destroy VM {vm_id}: it is the currently-active template.[/red]\n"
            "  Run 'orcest fleet rebake' first to swap to a new template, then retry."
        )
        sys.exit(1)

    # Pre-flight: check for linked clones referencing this template.
    try:
        all_vms = px.list_vms()
    except Exception as exc:
        console.print(f"[red]Failed to list VMs:[/red] {exc}")
        sys.exit(1)
    clones = [
        int(v.get("vmid", 0))
        for v in all_vms
        if v.get("name", "").startswith("orcest-worker-")
        and not _is_proxmox_template(v)
        and int(v.get("vmid", 0)) != vm_id
    ]
    if clones:
        sample = sorted(clones)[:5]
        more = "..." if len(clones) > 5 else ""
        console.print(
            f"[red]Refusing to destroy VM {vm_id}: {len(clones)} linked clone(s) still"
            f" reference it (vmids: {sample}{more}).[/red]\n"
            "  Wait for the clones to churn out, or run 'orcest fleet stop --drain-active'"
            " to drain them."
        )
        sys.exit(1)

    if not yes:
        click.confirm(f"Destroy template VM {vm_id}?", abort=True)

    console.print(f"  Destroying template VM {vm_id}...", end=" ")
    try:
        try:
            px.stop_vm(vm_id)
        except Exception:
            pass  # templates are usually already stopped
        px.destroy_vm(vm_id)
        console.print("[green]ok[/green]")
    except Exception as exc:
        console.print(f"[red]failed[/red]: {exc}")
        sys.exit(1)


@fleet.command("gc-templates")
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show which templates would be destroyed without destroying them.",
)
@click.option(
    "--yes",
    is_flag=True,
    help="Skip confirmation prompt (required for non-interactive automation).",
)
@click.option(
    "--config",
    default=str(DEFAULT_CONFIG_PATH),
    help="Fleet config path.",
    show_default=True,
)
@_serialized_fleet_operation
def gc_templates(dry_run: bool, yes: bool, config: str) -> None:
    """Destroy old templates in the range that have no live linked clones.

    Only genuine orcest templates are candidates: a VM inside
    ``pool.template_vmid_range`` must be flagged as a Proxmox template *and*
    be named ``orcest-worker-*``. Anything else that happens to occupy a VMID
    in the range belongs to somebody else and is reported, never destroyed.

    Skips the currently-active template and attempts to destroy each remaining
    candidate. Templates with live linked clones will fail to destroy (Proxmox
    enforces this) and are left in place.
    """
    from orcest.fleet.config import load_config
    from orcest.fleet.orchestrator import get_current_template_vmid

    console = Console()
    cfg = load_config(config)

    rng = cfg.pool.template_range()
    if rng is None:
        console.print("[red]pool.template_vmid_range is not configured — nothing to GC.[/red]")
        sys.exit(1)
    start, end = rng

    if not cfg.proxmox.api_token_id or not cfg.proxmox.api_token_secret:
        console.print("[red]Proxmox API credentials not configured.[/red]")
        sys.exit(1)

    px = _create_proxmox_client(cfg)

    active: int | None = None
    if cfg.orchestrator.host:
        try:
            active = get_current_template_vmid(cfg.ssh_target())
        except Exception:
            pass
    if active is None and cfg.pool.template_vm_id:
        active = cfg.pool.template_vm_id

    if active is None:
        console.print(
            "[red]Could not determine the active template VMID[/red] "
            "(Redis pointer unreadable and no pool.template_vm_id fallback).\n"
            "  Refusing to garbage-collect — every in-range template would be a "
            "candidate, including the live one.\n"
            "  Verify the pointer first:"
            f" ssh {cfg.ssh_target()} 'sudo docker exec orcest-redis-redis-1"
            " redis-cli GET orcest:pool:current_template_vmid'"
        )
        sys.exit(1)

    try:
        all_vms = px.list_vms()
    except Exception as exc:
        console.print(f"[red]Failed to list VMs:[/red] {exc}")
        sys.exit(1)
    # A VMID inside the template range proves nothing on its own: anybody can
    # create an unrelated VM there. Ownership proof here is the orcest name
    # prefix, NOT the Proxmox template flag. `validate_vmid_ranges()` guarantees
    # the worker and template ranges are disjoint, so an in-range
    # `orcest-worker-*` VM can never be a live pool worker -- and requiring the
    # template flag would permanently strand half-baked templates, which are
    # created as `orcest-worker-template` and only converted at the very last
    # step of the bake. Those are exactly what GC must reclaim: each one
    # consumes a slot in a 10-wide range, and once the range fills, `rebake`
    # tells the operator to run this command.
    in_range_vmids: list[int] = []
    unconverted: list[str] = []
    foreign: list[str] = []
    for v in all_vms:
        if "vmid" not in v:
            continue
        vmid = int(v["vmid"])
        if not (start <= vmid <= end):
            continue
        name = str(v.get("name") or "")
        if not name.startswith("orcest-worker-"):
            foreign.append(f"{vmid} ({name or 'unnamed'})")
            continue
        if not _is_proxmox_template(v):
            unconverted.append(f"{vmid} ({name})")
        in_range_vmids.append(vmid)
    in_range_vmids.sort()

    if foreign:
        console.print(
            f"  [yellow]Excluded {len(foreign)} VM(s) inside range"
            f" {start}-{end} not owned by orcest[/yellow]: {', '.join(sorted(foreign))}"
        )
        console.print("    These are not orcest worker templates and will never be destroyed.")
    if unconverted:
        console.print(
            f"  [yellow]{len(unconverted)} in-range orcest VM(s) are not converted"
            f" templates[/yellow]: {', '.join(sorted(unconverted))}"
        )
        console.print("    Treating as half-baked template residue eligible for reclamation.")

    candidates = [vmid for vmid in in_range_vmids if vmid != active]
    if not candidates:
        console.print(
            f"  No GC candidates in range {start}-{end}"
            + (f" (active template is VM {active})" if active else "")
            + "."
        )
        return

    console.print(f"  Found {len(candidates)} GC candidate(s) in range {start}-{end}: {candidates}")

    if not dry_run and not yes:
        # Non-interactive callers (the weekly rebake timer) must pass --yes;
        # click aborts on EOF, so an unattended run fails closed rather than
        # destroying templates without an operator ever seeing the list.
        click.confirm(
            f"Destroy {len(candidates)} template VM(s) {candidates}?",
            abort=True,
        )
    destroyed: list[int] = []
    skipped: list[int] = []
    for vmid in candidates:
        if dry_run:
            console.print(f"  [dry-run] would destroy VM {vmid}")
            continue
        # Re-read the active pointer right before each destroy: a concurrent
        # `rebake` may have swapped orcest:pool:current_template_vmid to a VMID
        # we classified as a candidate at start-up. Fail safe -- never destroy
        # the currently-active template even if the pointer moved mid-run.
        if cfg.orchestrator.host:
            try:
                current = get_current_template_vmid(cfg.ssh_target())
            except Exception:
                # Pointer unreadable now (was readable at start): fail closed,
                # skip the rest rather than risk destroying a live template.
                console.print(
                    "  [yellow]Active pointer became unreadable; aborting further"
                    " destroys to avoid racing a rebake.[/yellow]"
                )
                break
            if current is not None and current == vmid:
                console.print(
                    f"  [yellow]skipped[/yellow]: VM {vmid} became the active"
                    " template (concurrent rebake)"
                )
                skipped.append(vmid)
                continue
        console.print(f"  Destroying VM {vmid}...", end=" ")
        try:
            try:
                px.stop_vm(vmid)
            except Exception:
                pass
            px.destroy_vm(vmid)
            console.print("[green]ok[/green]")
            destroyed.append(vmid)
        except Exception as exc:
            # Most common reason: linked clones still reference this template.
            console.print(f"[yellow]skipped[/yellow]: {exc}")
            skipped.append(vmid)

    if not dry_run:
        console.print(
            f"\n  Destroyed {len(destroyed)} template(s)"
            + (f", skipped {len(skipped)} (live clones)" if skipped else "")
            + "."
        )


@fleet.command("set-pool-size")
@click.argument("size", type=int)
@click.option("--vm-id-start", type=int, default=None, help="First VM ID for worker clones.")
@click.option(
    "--config",
    default=str(DEFAULT_CONFIG_PATH),
    help="Fleet config path.",
    show_default=True,
)
@_serialized_fleet_operation
def set_pool_size(size: int, vm_id_start: int | None, config: str) -> None:
    """Set the target warm pool size."""
    from orcest.fleet.config import load_config, save_config

    console = Console()

    if size < 0:
        console.print("[red]Pool size must be non-negative.[/red]")
        sys.exit(1)

    cfg = load_config(config)
    old_size = cfg.pool.size
    cfg.pool.size = size
    if vm_id_start is not None:
        cfg.pool.vm_id_start = vm_id_start
        console.print(f"Worker VM ID start: {vm_id_start}")
    try:
        cfg.pool.validate_vmid_ranges()
    except ValueError as exc:
        console.print(f"[red]Invalid pool configuration: {exc}[/red]")
        sys.exit(1)
    save_config(cfg, config)

    console.print(f"Pool size updated: {old_size} -> {size}")


@fleet.command()
@click.option(
    "--drain-active",
    is_flag=True,
    help="Also destroy active workers (interrupts running tasks).",
)
@click.option(
    "--yes",
    is_flag=True,
    help="Skip confirmation prompt.",
)
@click.option(
    "--config",
    default=str(DEFAULT_CONFIG_PATH),
    help="Fleet config path.",
    show_default=True,
)
@_serialized_fleet_operation
def stop(drain_active: bool, yes: bool, config: str) -> None:
    """Stop the pool manager and destroy idle worker VMs."""
    if drain_active and not yes:
        click.confirm(
            "This will destroy active workers and interrupt running tasks. Continue?",
            abort=True,
        )

    from orcest.fleet.config import load_config
    from orcest.fleet.orchestrator import (
        clean_pool_redis,
        get_current_template_vmid,
        get_deployed_pool_vmid_range,
        get_pool_redis_members,
        get_workers_with_pending_tasks,
        set_workers_draining,
        stop_pool_manager,
    )

    console = Console()
    cfg = load_config(config)

    if not cfg.orchestrator.host:
        console.print("[red]Orchestrator host not set.[/red]")
        console.print("  Run: orcest fleet create-orchestrator")
        sys.exit(1)
    if drain_active and (not cfg.proxmox.api_token_id or not cfg.proxmox.api_token_secret):
        console.print(
            "[red]Cannot drain worker VMs: Proxmox API credentials are not configured.[/red]"
        )
        console.print("  No fleet state was changed; configure credentials and retry.")
        raise SystemExit(1)

    ssh_target = cfg.ssh_target()

    # The remote deployed config is the last authoritative ownership boundary
    # for destructive VM operations. A locally edited range must never retarget
    # `stop` at a different set of VMIDs. An absent remote config is allowed for
    # first-deployment cleanup, but read/parse failures and mismatches fail before
    # the pool manager or any VM is touched.
    try:
        deployed_vmid_range = get_deployed_pool_vmid_range(ssh_target)
    except Exception as exc:
        console.print(f"[red]Could not verify deployed worker VMID range:[/red] {exc}")
        console.print("  No fleet state was changed.")
        raise SystemExit(1) from exc
    desired_vmid_range = (cfg.pool.vm_id_start, cfg.pool.vm_id_end)
    if deployed_vmid_range is not None and deployed_vmid_range != desired_vmid_range:
        console.print(
            "[red]Refusing to stop workers with a local VMID range that differs "
            f"from the deployed fleet ({desired_vmid_range[0]}-{desired_vmid_range[1]} vs "
            f"{deployed_vmid_range[0]}-{deployed_vmid_range[1]}).[/red]"
        )
        console.print("  No fleet state was changed; use the deployed configuration to drain.")
        raise SystemExit(1)

    # Step 1: Stop pool manager
    console.print("  Stopping pool manager...", end=" ")
    try:
        stop_pool_manager(ssh_target)
        console.print("[green]ok[/green]")
    except RuntimeError as exc:
        console.print(f"[red]failed[/red]: {exc}")
        console.print(
            "  Refusing to destroy worker VMs while the pool manager may still be running."
        )
        sys.exit(1)

    # Step 2: Read Redis state
    console.print("  Reading pool state...", end=" ")
    idle_ids: set[str]
    active_ids: dict[str, str]
    try:
        idle_ids, active_ids = get_pool_redis_members(ssh_target)
    except Exception as exc:
        if not drain_active:
            console.print(f"[red]failed[/red]: {exc}")
            console.print(
                "  Refusing to destroy worker VMs without Redis pool state; "
                "rerun with --drain-active only if interrupting active workers is intended."
            )
            sys.exit(1)
        console.print(f"[yellow]warning[/yellow]: {exc}")
        console.print("  Continuing because --drain-active was requested.")
        idle_ids, active_ids = set(), {}

    busy_consumers: set[str]
    try:
        busy_consumers = get_workers_with_pending_tasks(ssh_target)
        console.print(
            f"[green]ok[/green] ({len(idle_ids)} idle, {len(active_ids)} active,"
            f" {len(busy_consumers)} pending)"
        )
    except Exception as exc:
        if not drain_active:
            console.print(f"[red]failed[/red]: {exc}")
            console.print(
                "  Refusing to destroy worker VMs without Redis pending-consumer state; "
                "rerun with --drain-active only if interrupting active workers is intended."
            )
            sys.exit(1)
        busy_consumers = set()
        console.print(f"[yellow]warning[/yellow]: {exc}")
        console.print("  Continuing because --drain-active was requested.")

    # Step 3: Destroy worker VMs
    if not cfg.proxmox.api_token_id or not cfg.proxmox.api_token_secret:
        if drain_active:
            console.print(
                "[red]Cannot drain worker VMs: Proxmox API credentials are not configured.[/red]"
            )
            console.print("  No worker VM was destroyed; configure credentials and retry.")
            raise SystemExit(1)
        console.print(
            "[yellow]Proxmox API credentials not configured — skipping VM destruction.[/yellow]"
        )
        console.print("  VMs must be destroyed manually or via Proxmox UI.")
        return

    px = _create_proxmox_client(cfg)
    worker_vms = px.list_vms(name_prefix="orcest-worker-")
    protected_template_vmids = set()
    if cfg.pool.template_vm_id:
        protected_template_vmids.add(cfg.pool.template_vm_id)
    template_range = cfg.pool.template_range()
    if template_range:
        protected_template_vmids.update(range(template_range[0], template_range[1] + 1))
        try:
            active_template = get_current_template_vmid(ssh_target)
            if active_template:
                protected_template_vmids.add(active_template)
        except Exception as exc:
            console.print(
                f"[yellow]warning[/yellow]: could not read active template pointer: {exc}"
            )

    worker_vms = [
        v
        for v in worker_vms
        if not _is_proxmox_template(v) and int(v.get("vmid", 0)) not in protected_template_vmids
    ]

    # Destructive lifecycle operations must never rely on a name prefix alone.
    # A finite or open-ended worker VMID range is the authority that proves the
    # VM belongs to this pool.
    safe_worker_vms: list[dict] = []
    range_skipped: list[str] = []
    for vm in worker_vms:
        vm_id = int(vm.get("vmid", 0))
        if cfg.pool.contains_worker_vmid(vm_id):
            safe_worker_vms.append(vm)
        else:
            range_skipped.append(str(vm_id))
            console.print(f"  Refusing to destroy VM {vm_id}: outside configured worker VMID range")
    worker_vms = safe_worker_vms

    draining_worker_ids: list[str] = []
    if not drain_active:
        for vm in worker_vms:
            vm_id = int(vm["vmid"])
            vm_id_str = str(vm_id)
            worker_id = f"orcest-worker-{vm_id}"
            if (
                vm_id_str in active_ids
                or worker_id in busy_consumers
                or vm_id_str in busy_consumers
            ):
                continue
            draining_worker_ids.append(worker_id)
        if draining_worker_ids:
            try:
                set_workers_draining(ssh_target, draining_worker_ids, draining=True)
                _wait_for_worker_drain_quiescence()
            except Exception as exc:
                console.print(f"[red]failed to establish worker drain leases[/red]: {exc}")
                sys.exit(1)

    destroyed: list[str] = []
    skipped: list[str] = list(range_skipped)
    destroy_failures: list[str] = []
    recovery_failures: list[str] = []
    drain_lease_failures: list[str] = []

    def restart_stopped_worker(vm_id: int, reason: str) -> bool:
        """Re-establish worker recovery after an aborted destruction commit."""
        try:
            px.start_vm(vm_id)
        except Exception as exc:
            console.print(f"[red]{reason}; failed to restart VM {vm_id}: {exc}[/red]")
            recovery_failures.append(str(vm_id))
            return False
        console.print(f"[yellow]{reason}; restarted VM for task recovery[/yellow]")
        return True

    try:
        for vm in worker_vms:
            vm_id = int(vm["vmid"])
            vm_id_str = str(vm_id)
            worker_id = f"orcest-worker-{vm_id}"
            is_idle = vm_id_str in idle_ids
            is_active = vm_id_str in active_ids
            has_pending = worker_id in busy_consumers or vm_id_str in busy_consumers

            if not drain_active and not is_active and not has_pending:
                # The drain lease prevents new reads; this final fresh PEL read
                # catches a claim that won immediately before the lease landed.
                try:
                    fresh_busy = get_workers_with_pending_tasks(ssh_target)
                except Exception as exc:
                    console.print(f"  Leaving VM {vm_id}: final pending-task check failed: {exc}")
                    skipped.append(vm_id_str)
                    continue
                has_pending = worker_id in fresh_busy or vm_id_str in fresh_busy

            if has_pending and not drain_active:
                console.print(f"  Leaving VM {vm_id} ({vm.get('name', '')}) with pending task")
                skipped.append(vm_id_str)
                continue

            if is_active and not drain_active:
                console.print(f"  Leaving active VM {vm_id} ({vm.get('name', '')})")
                skipped.append(vm_id_str)
                continue

            label = (
                "active"
                if is_active
                else ("busy" if has_pending else ("idle" if is_idle else "orphan"))
            )
            console.print(f"  Destroying {label} VM {vm_id}...", end=" ")
            try:
                stopped = False
                try:
                    px.stop_vm(vm_id)
                    deadline = time.monotonic() + 15
                    while time.monotonic() < deadline:
                        if px.get_vm_status(vm_id) == "stopped":
                            stopped = True
                            break
                        time.sleep(1)
                except Exception:
                    try:
                        stopped = px.get_vm_status(vm_id) == "stopped"
                    except Exception:
                        stopped = False
                if not stopped:
                    console.print("[yellow]failed to confirm stopped; leaving VM intact[/yellow]")
                    destroy_failures.append(vm_id_str)
                    continue
                if not drain_active:
                    try:
                        post_stop_busy = get_workers_with_pending_tasks(ssh_target)
                    except Exception as exc:
                        restart_stopped_worker(
                            vm_id,
                            f"post-stop pending-task check failed: {exc}",
                        )
                        skipped.append(vm_id_str)
                        continue
                    if worker_id in post_stop_busy or vm_id_str in post_stop_busy:
                        restart_stopped_worker(vm_id, "late task claim detected")
                        skipped.append(vm_id_str)
                        continue
                px.destroy_vm(vm_id)
                console.print("[green]ok[/green]")
                destroyed.append(vm_id_str)
            except Exception as exc:
                console.print(f"[yellow]failed[/yellow]: {exc}")
                destroy_failures.append(vm_id_str)
                if not drain_active and stopped:
                    restart_stopped_worker(vm_id, "destruction failed")
    finally:
        if draining_worker_ids:
            # A lease that outlives this command fences the worker forever, so
            # a clear failure is a command failure -- never a warning.
            #
            # Only SURVIVING workers can be fenced, though. A destroyed VM has
            # no process left to fence, and Step 4's clean_pool_redis SREMs and
            # verifies its marker immediately after this block. Reporting those
            # as failures would abort the command -- and, through `deploy`,
            # abort the whole rollout after the orchestrators and pool manager
            # are already stopped -- over leases that are provably gone moments
            # later, with a printed SREM remediation that is a no-op.
            surviving_worker_ids = [
                worker_id
                for worker_id in draining_worker_ids
                if worker_id.removeprefix("orcest-worker-") not in destroyed
            ]
            try:
                set_workers_draining(ssh_target, draining_worker_ids, draining=False)
            except Exception as exc:
                console.print(f"  [red]failed to clear drain leases[/red]: {exc}")
                drain_lease_failures = list(surviving_worker_ids)
            else:
                if not surviving_worker_ids:
                    drain_lease_failures = []
                else:
                    try:
                        drain_lease_failures = _drain_leases_still_held(
                            ssh_target, surviving_worker_ids
                        )
                    except Exception as exc:
                        console.print(
                            f"  [red]failed to verify drain leases were cleared[/red]: {exc}"
                        )
                        drain_lease_failures = list(surviving_worker_ids)
                    else:
                        if drain_lease_failures:
                            console.print(
                                "  [red]drain leases are still held after clearing them[/red]"
                            )

    # Step 4: Clean Redis
    pool_cleanup_failed = False
    if destroyed:
        console.print("  Cleaning Redis state...", end=" ")
        try:
            clean_pool_redis(ssh_target, destroyed)
            console.print("[green]ok[/green]")
        except Exception as exc:
            pool_cleanup_failed = True
            console.print(f"[red]failed[/red]: {exc}")

    if destroyed:
        console.print("  Pending task markers left intact for stream recovery or TTL expiry.")

    console.print(f"\n  Destroyed {len(destroyed)} VMs", end="")
    if skipped:
        console.print(f", intentionally left {len(skipped)} busy or protected", end="")
    if destroy_failures:
        console.print(f", failed to destroy {len(destroy_failures)}", end="")
    console.print(".")
    if drain_lease_failures:
        srem_cmd = "<redis-cli> SREM orcest:pool:draining " + " ".join(
            shlex.quote(worker_id) for worker_id in drain_lease_failures
        )
        console.print(
            "[red]Stop incomplete: worker drain leases were not cleared for "
            f"{', '.join(drain_lease_failures)}.[/red]\n"
            "  Every surviving worker holding a stale lease exits 75 on each loop "
            "until systemd marks it failed, while still counting toward pool size.\n"
            f"  Clear them manually on {ssh_target}: {srem_cmd}"
        )
        raise SystemExit(1)
    if drain_active and (skipped or destroy_failures):
        console.print("[red]Drain incomplete: one or more worker VMs were not destroyed.[/red]")
        raise SystemExit(1)
    if recovery_failures:
        console.print(
            "[red]Stop incomplete: one or more stopped workers could not be "
            "restarted for pending-task recovery.[/red]"
        )
        raise SystemExit(1)
    if destroy_failures:
        console.print(
            "[red]Stop incomplete: one or more eligible worker VMs could not be destroyed.[/red]"
        )
        raise SystemExit(1)
    if pool_cleanup_failed:
        console.print(
            "[red]Stop incomplete: worker VMs were destroyed, but durable Redis "
            "done/drain markers were not verified as cleared. Refusing to report success.[/red]"
        )
        raise SystemExit(1)


@fleet.command()
@click.option(
    "--config",
    default=str(DEFAULT_CONFIG_PATH),
    help="Fleet config path.",
    show_default=True,
)
@click.pass_context
@_serialized_fleet_operation
def start(ctx: click.Context, config: str) -> None:
    """Start the pool manager.

    Uploads the current fleet config and starts the pool manager, which
    will begin cloning worker VMs to reach the target pool size.
    """
    from orcest.fleet.config import load_config
    from orcest.fleet.orchestrator import (
        ensure_pool_manager,
        ensure_redis_password,
        upload_fleet_config,
    )

    console = Console()
    cfg = load_config(config)

    if not cfg.orchestrator.host:
        console.print("[red]Orchestrator host not set.[/red]")
        console.print("  Run: orcest fleet create-orchestrator")
        sys.exit(1)

    if not cfg.pool.template_vm_id and cfg.pool.template_range() is None:
        console.print("[red]No worker template configured.[/red]")
        console.print("  Run: orcest fleet create-template or configure pool.template_vmid_range")
        sys.exit(1)

    if not cfg.proxmox.api_token_id or not cfg.proxmox.api_token_secret:
        console.print("[red]Proxmox API credentials not configured.[/red]")
        console.print("  Run: orcest init")
        sys.exit(1)

    if cfg.proxmox.is_localhost():
        console.print(
            "[red]Proxmox endpoint is localhost — unreachable from orchestrator VM.[/red]"
        )
        console.print("  Run: orcest init")
        sys.exit(1)

    ssh_target = cfg.ssh_target()

    _validate_provider_stream_routing(cfg, console)

    # Direct `fleet start` may restart an unchanged pool, but it may not turn
    # an edited local backend into a partial transition. Coordinated deploy
    # supplies an in-process authorization only after draining old workers and
    # regenerating every project publisher config.
    _validate_backend_transition(
        cfg,
        config,
        console,
        allow_backend_change=bool(ctx.meta.get(_COORDINATED_BACKEND_CHANGE_META_KEY, False)),
    )

    # Ensure orchestrator can SSH to Proxmox host (for cloud-init snippets)
    from urllib.parse import urlparse

    proxmox_ip = urlparse(cfg.proxmox.endpoint).hostname or "127.0.0.1"
    _ensure_orchestrator_ssh(ssh_target, proxmox_ip, console)

    console.print("  Uploading fleet config...", end=" ")
    try:
        upload_fleet_config(ssh_target, config)
        console.print("[green]ok[/green]")
    except Exception as exc:
        console.print(f"[red]failed[/red]: {exc}")
        sys.exit(1)

    # C1: the pool-manager stack --env-file's REDIS_ENV_PATH so the manager can
    # AUTH to Redis and forward the password to worker clones; ensure it exists
    # first (idempotent; the redis stack normally minted it already).
    try:
        ensure_redis_password(ssh_target)
    except Exception as exc:
        console.print(f"  [yellow]Could not ensure Redis password: {exc}[/yellow]")

    console.print("  Starting pool manager...", end=" ")
    try:
        ensure_pool_manager(ssh_target)
        console.print("[green]ok[/green]")
    except Exception as exc:
        console.print(f"[red]failed[/red]: {exc}")
        sys.exit(1)

    console.print(f"\n  Pool manager started (target size: {cfg.pool.size}).")


@fleet.command()
@click.option(
    "--rebuild-template",
    is_flag=True,
    help="Also rebake the worker template VM and atomically swap the active pointer.",
)
@click.option(
    "--drain-active",
    is_flag=True,
    help="Destroy active workers (interrupts running tasks).",
)
@click.option(
    "--keep-orchestrators-paused",
    is_flag=True,
    help="Attest candidate workers but leave project orchestrators stopped.",
)
@click.option(
    "--config",
    default=str(DEFAULT_CONFIG_PATH),
    help="Fleet config path.",
    show_default=True,
)
@click.pass_context
@_serialized_fleet_operation
def deploy(
    ctx: click.Context,
    rebuild_template: bool,
    drain_active: bool,
    keep_orchestrators_paused: bool,
    config: str,
) -> None:
    """Full deploy: rebuild images, restart fleet, and optionally rebake workers.

    Runs the full deployment sequence in order:

    \b
      1. Pause orchestrators so no new tasks are published
      2. Stop fleet (stop pool manager, destroy workers, clean pool Redis)
      3. Update source, image, and project files while orchestrators stay stopped
      4. Rebake template and swap active pointer (only with --rebuild-template)
      5. Start and attest the exact candidate worker layout
      6. Resume orchestrators, unless --keep-orchestrators-paused was requested
    """
    console = Console()

    _preflight_deploy_config(
        config,
        console,
        rebuild_template=rebuild_template,
        drain_active=drain_active,
    )
    _preflight_backend_transition(
        config,
        console,
        allow_backend_change=rebuild_template and drain_active,
    )

    total = 6 if rebuild_template else 5
    step = 0
    coordinated_backend_change = rebuild_template and drain_active
    if coordinated_backend_change:
        ctx.meta[_COORDINATED_BACKEND_CHANGE_META_KEY] = True
    ctx.meta[_DEFER_PROJECT_START_META_KEY] = True
    try:
        from orcest.fleet.config import load_config
        from orcest.fleet.orchestrator import restart_stack, stop_stack

        cfg = load_config(config)
        ssh_target = cfg.ssh_target()

        # Step 1: Fence publishers before creating a workerless interval.
        step += 1
        console.print(f"\n[bold]Step {step}/{total}: Pausing orchestrators[/bold]\n")
        paused_projects: list[str] = []
        try:
            for project in cfg.projects:
                console.print(f"  Stopping stack for '{project.name}'...", end=" ")
                stop_stack(ssh_target, project.name)
                paused_projects.append(project.name)
                console.print("[green]ok[/green]")
        except Exception as exc:
            console.print(f"[red]failed[/red]: {exc}")
            if keep_orchestrators_paused:
                console.print(
                    "  [yellow]Already-paused stacks remain stopped by operator request.[/yellow]"
                )
            else:
                for project_name in paused_projects:
                    try:
                        restart_stack(ssh_target, project_name)
                    except Exception:
                        console.print(
                            f"  [red]Could not resume already-paused stack '{project_name}'.[/red]"
                        )
            raise SystemExit(1) from exc

        # Step 2: Stop fleet.
        step += 1
        console.print(f"\n[bold]Step {step}/{total}: Stopping fleet[/bold]\n")
        ctx.invoke(stop, drain_active=drain_active, yes=True, config=config)

        # Step 3: Update without resuming project publishers.
        step += 1
        console.print(f"\n[bold]Step {step}/{total}: Updating orchestrator[/bold]\n")
        ctx.invoke(
            update,
            config=config,
            skip_pool_manager=True,
        )

        # Step 4: Rebake template (optional).
        if rebuild_template:
            step += 1
            console.print(f"\n[bold]Step {step}/{total}: Rebaking template[/bold]\n")
            ctx.invoke(rebake, config=config)

        # Step 5: Start and attest workers.
        step += 1
        console.print(f"\n[bold]Step {step}/{total}: Starting fleet[/bold]\n")
        ctx.invoke(start, config=config)
        from orcest.fleet.orchestrator import _resolve_deploy_revision

        try:
            _wait_for_candidate_workers(
                cfg,
                console,
                expected_revision=_resolve_deploy_revision() if rebuild_template else None,
            )
        except (Exception, SystemExit):
            # Attestation failed after Step 1 stopped every project publisher.
            # Leaving the stacks fenced would keep the whole fleet down until
            # an operator notices, so resume them before propagating failure.
            if keep_orchestrators_paused:
                console.print(
                    "  [yellow]Paused stacks remain stopped by operator request.[/yellow]"
                )
            else:
                resume_failures: list[str] = []
                for project_name in paused_projects:
                    console.print(f"  Resuming stack for '{project_name}'...", end=" ")
                    try:
                        restart_stack(ssh_target, project_name)
                        console.print("[green]ok[/green]")
                    except Exception as resume_exc:
                        console.print(f"[red]failed[/red]: {resume_exc}")
                        resume_failures.append(project_name)
                if resume_failures:
                    console.print(
                        "[red]Could not resume paused orchestrator stacks: "
                        f"{', '.join(resume_failures)}.[/red]\n"
                        f"  Restart them manually with: orcest fleet update --config {config}"
                    )
            raise

        # Step 6: Resume publishers only after workers attest.
        step += 1
        console.print(f"\n[bold]Step {step}/{total}: Resuming orchestrators[/bold]\n")
        failures: list[str] = []
        for project in cfg.projects:
            if keep_orchestrators_paused:
                console.print(
                    f"  Keeping stack for '{project.name}' "
                    "[yellow]paused by operator request[/yellow]"
                )
                continue
            console.print(f"  Starting stack for '{project.name}'...", end=" ")
            try:
                restart_stack(ssh_target, project.name)
                console.print("[green]ok[/green]")
            except Exception as exc:
                console.print(f"[red]failed[/red]: {exc}")
                failures.append(project.name)
        if failures:
            console.print(
                "[red]Candidate workers are healthy, but one or more orchestrators "
                "remained stopped.[/red]"
            )
            raise SystemExit(1)
    finally:
        ctx.meta.pop(_COORDINATED_BACKEND_CHANGE_META_KEY, None)
        ctx.meta.pop(_DEFER_PROJECT_START_META_KEY, None)

    console.print("\n[bold green]Deploy complete.[/bold green]")


def _wait_for_candidate_workers(
    cfg: FleetConfig,
    console: Console,
    *,
    expected_revision: str | None,
) -> None:
    """Wait for the exact VMID/backend/revision layout before resuming intake."""
    from orcest.fleet.orchestrator import get_pool_redis_members, get_worker_heartbeats
    from orcest.revision import revision_is_attested

    expected_layout = {
        f"orcest-worker-{cfg.pool.vm_id_start + index}": profile.backend
        for index, profile in enumerate(cfg.pool.scheduled_worker_profiles())
    }
    expected_vmids = {
        cfg.pool.vm_id_start + index
        for index, _profile in enumerate(cfg.pool.scheduled_worker_profiles())
    }
    deadline = time.monotonic() + _CANDIDATE_WORKER_WAIT_SECONDS
    last_report = 0.0
    while time.monotonic() < deadline:
        heartbeats = get_worker_heartbeats(cfg.ssh_target())
        revisions = {revision for _, revision in heartbeats.values()}
        stale_worker_ids: set[str] = set()
        if expected_revision is None:
            required_layout = expected_layout
            revisions_match = (not expected_layout and not revisions) or (
                len(revisions) == 1
                and all(revision_is_attested(revision) for revision in revisions)
            )
            observed_layout = {
                worker_id: backend for worker_id, (backend, _revision) in heartbeats.items()
            }
        else:
            # `fleet stop` without --drain-active deliberately spares busy
            # workers. Survivors keep heartbeating the previous template
            # revision until the pool manager retires them at end of task,
            # and their slots cannot be recloned while their VMs are alive,
            # so they are excused from the candidate layout instead of
            # blocking it. At least one expected slot must still be served
            # by the new revision so the new template is actually attested.
            stale_worker_ids = {
                worker_id
                for worker_id, (_backend, revision) in heartbeats.items()
                if revision != expected_revision
            }
            required_layout = {
                worker_id: backend
                for worker_id, backend in expected_layout.items()
                if worker_id not in stale_worker_ids
            }
            revisions_match = not expected_layout or bool(required_layout)
            observed_layout = {
                worker_id: backend
                for worker_id, (backend, revision) in heartbeats.items()
                if revision == expected_revision
            }
        stale_vmids: set[int] = set()
        for worker_id in stale_worker_ids:
            try:
                stale_vmids.add(int(worker_id.rsplit("-", 1)[-1]))
            except ValueError:
                continue
        idle_ids, active_ids = get_pool_redis_members(cfg.ssh_target())
        pool_members_valid = True
        try:
            idle_vmids = {int(vm_id) for vm_id in idle_ids}
            active_vmids = {int(vm_id) for vm_id in active_ids}
        except (TypeError, ValueError):
            idle_vmids = set()
            active_vmids = set()
            pool_members_valid = False
        if idle_vmids & active_vmids:
            pool_members_valid = False
        tracked_vmids = idle_vmids | active_vmids
        pool_total = len(idle_ids) + len(active_ids)
        # Slots held by spared old-generation workers are excused on both
        # sides: they may legitimately remain tracked (still finishing a
        # task) and can never carry the new-generation VMID layout.
        pool_layout_matches = pool_members_valid and (
            tracked_vmids - stale_vmids == expected_vmids - stale_vmids
        )
        if observed_layout == required_layout and revisions_match and pool_layout_matches:
            if stale_worker_ids:
                console.print(
                    f"  Ignoring {len(stale_worker_ids)} surviving old-revision worker(s) "
                    f"pending retirement: {', '.join(sorted(stale_worker_ids))}."
                )
            revision_label = expected_revision or (next(iter(revisions)) if revisions else "none")
            console.print(
                f"  Candidate worker layout attested [green]ok[/green] "
                f"({pool_total}/{cfg.pool.size}, revision {revision_label[:12]})."
            )
            return
        now = time.monotonic()
        if now - last_report >= 30:
            console.print(
                "  Waiting for candidate workers "
                f"({len(observed_layout)}/{len(expected_layout)} heartbeats, "
                f"{pool_total}/{cfg.pool.size} tracked)..."
            )
            last_report = now
        time.sleep(5)
    console.print(
        "[red]Candidate workers did not attest before timeout; orchestrators remain stopped.[/red]"
    )
    raise SystemExit(1)


def _preflight_backend_transition(
    config: str,
    console: Console,
    *,
    allow_backend_change: bool,
) -> None:
    """Reject backend changes before deploy stops any running services."""
    from orcest.fleet.config import load_config

    cfg = load_config(config)
    if not cfg.orchestrator.host:
        return
    _validate_backend_transition(
        cfg,
        config,
        console,
        allow_backend_change=allow_backend_change,
    )


def _validate_provider_stream_routing(cfg: FleetConfig, console: Console) -> None:
    """Fail before mutation when the managed profiles cannot claim tasks."""
    try:
        mismatches = cfg.provider_stream_mismatches()
    except KeyError as exc:
        console.print(f"[red]Could not validate provider stream routing:[/red] {exc}")
        raise SystemExit(1) from exc
    if not mismatches:
        return

    backends = ", ".join(sorted(cfg.pool.worker_backends())) or "none"
    console.print(
        "[red]Fleet provider routing is unsafe: the managed worker pool schedules "
        f"only these backends: {backends}.[/red]"
    )
    for project, providers in sorted(mismatches.items()):
        console.print(f"  - {project}: unconsumed provider stream(s): {', '.join(providers)}")
    console.print(
        "  Add matching pool.worker_profiles entries, or remove the mismatched "
        "providers before deploy."
    )
    raise SystemExit(1)


def _validate_deploy_source_revision(console: Console) -> None:
    """Reject unattested source before any remote update mutation."""
    from orcest.fleet.orchestrator import _resolve_deploy_revision
    from orcest.revision import revision_is_attested

    deploy_revision = _resolve_deploy_revision()
    if revision_is_attested(deploy_revision):
        return
    console.print(
        "[red]Deployment source revision is "
        f"{deploy_revision!r}; commit every source file before updating.[/red]"
    )
    raise SystemExit(1)


def _validate_backend_transition(
    cfg: FleetConfig,
    config: str,
    console: Console,
    *,
    allow_backend_change: bool,
) -> None:
    """Fail closed when local and deployed worker backends differ.

    ``allow_backend_change`` is supplied only by the coordinated deploy call
    chain. Public ``update``, ``start``, and ``onboard`` entry points always
    pass ``False``.
    """
    from orcest.fleet.orchestrator import (
        get_deployed_pool_backend,
        get_deployed_pool_vmid_range,
    )

    if not cfg.orchestrator.host:
        return
    try:
        deployed_backend = get_deployed_pool_backend(cfg.ssh_target())
        deployed_vmid_range = get_deployed_pool_vmid_range(cfg.ssh_target())
    except Exception as exc:
        console.print(f"[red]Could not verify deployed worker layout:[/red] {exc}")
        raise SystemExit(1) from exc
    desired_vmid_range = (cfg.pool.vm_id_start, cfg.pool.vm_id_end)
    if deployed_vmid_range is not None and deployed_vmid_range != desired_vmid_range:
        console.print(
            "[red]Refusing to change the worker VMID range while a deployed fleet exists "
            f"({deployed_vmid_range[0]}-{deployed_vmid_range[1]} -> "
            f"{desired_vmid_range[0]}-{desired_vmid_range[1]}).[/red]"
        )
        console.print(
            "  Drain the fleet using its deployed configuration before changing "
            "pool.vm_id_start or pool.vm_id_end."
        )
        raise SystemExit(1)
    desired_backend = cfg.pool.worker_layout_signature()
    if (
        deployed_backend is not None
        and deployed_backend != desired_backend
        and not allow_backend_change
    ):
        console.print(
            "[red]Refusing an uncoordinated worker backend change or layout change "
            f"({deployed_backend} -> {desired_backend}).[/red]"
        )
        console.print(
            f"  Run: orcest fleet deploy --rebuild-template --drain-active --config {config}"
        )
        raise SystemExit(1)
    if (
        deployed_backend is not None
        and deployed_backend != desired_backend
        and cfg.pool.vm_id_start <= 0
    ):
        console.print(
            "[red]Backend transition requires a configured pool.vm_id_start so "
            "all old workers can be drained safely.[/red]"
        )
        raise SystemExit(1)


def _preflight_deploy_config(
    config: str,
    console: Console,
    *,
    rebuild_template: bool,
    drain_active: bool,
) -> None:
    """Validate every static deploy prerequisite before stopping the fleet."""
    from orcest.fleet.config import load_config
    from orcest.fleet.orchestrator import _resolve_deploy_revision
    from orcest.revision import revision_is_attested

    try:
        cfg = load_config(config)
        rng = cfg.pool.template_range()
    except ValueError as exc:
        console.print(f"[red]Invalid pool.template_vmid_range:[/red] {exc}")
        raise SystemExit(1) from exc

    problems: list[str] = []
    if not cfg.orchestrator.host:
        problems.append("orchestrator.host is not configured")
    if not cfg.proxmox.api_token_id or not cfg.proxmox.api_token_secret:
        problems.append("Proxmox API credentials are not configured")
    if cfg.proxmox.is_localhost():
        problems.append("Proxmox endpoint is localhost and is unreachable from the pool manager")
    if not cfg.pool.template_vm_id and rng is None:
        problems.append("no worker template or template VMID range is configured")
    if cfg.pool.vm_id_start <= 0:
        problems.append("pool.vm_id_start is required to allocate workers")
    if rebuild_template and rng is None:
        problems.append("pool.template_vmid_range is required by --rebuild-template")

    deploy_revision = _resolve_deploy_revision()
    if not revision_is_attested(deploy_revision):
        problems.append(
            f"deployment source revision is {deploy_revision!r}; commit every source file "
            "before deploying"
        )

    try:
        routing_mismatches = cfg.provider_stream_mismatches()
    except KeyError as exc:
        problems.append(f"provider stream routing could not be validated: {exc}")
    else:
        backends = ", ".join(sorted(cfg.pool.worker_backends())) or "none"
        if len(cfg.pool.worker_backends()) == 1:
            routing_summary = f"workers consume only tasks:{backends}"
        else:
            routing_summary = f"managed worker backends are {backends}"
        for project, providers in sorted(routing_mismatches.items()):
            if len(cfg.pool.worker_backends()) == 1:
                problems.append(
                    f"project {project}: unconsumed provider stream(s) {', '.join(providers)}"
                )
                problems.append(routing_summary)
            else:
                problems.append(
                    f"project {project} publishes to unconsumed provider stream(s) "
                    f"{', '.join(providers)}; {routing_summary}"
                )

    if problems:
        console.print("[red]Deploy preflight failed before any services were stopped:[/red]")
        for problem in problems:
            console.print(f"  - {problem}")
        raise SystemExit(1)


def _upgrade_cli(console: Console) -> None:
    """Deprecated: deploys use the currently installed CLI/source tree."""
    console.print(
        "  [yellow]CLI self-upgrade is disabled; install the desired source before deploy.[/yellow]"
    )
