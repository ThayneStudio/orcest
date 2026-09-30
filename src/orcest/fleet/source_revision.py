"""Detect fleet source-revision drift against a declared desired Git ref/SHA.

The desired revision is resolved once, bounded, and secret-safe (see
:func:`resolve_desired_revision`); every runtime surface (project
orchestrators, pool manager, active worker template, live worker heartbeats)
is then compared against that single resolved SHA with a pure, read-only
comparison (see :func:`evaluate_source_revision`). Neither function performs
any deployment mutation.
"""

from __future__ import annotations

import os
import re
import selectors
import signal
import subprocess
import time
from collections.abc import Sequence
from dataclasses import dataclass
from urllib.parse import urlsplit

from orcest.revision import normalize_revision, revision_is_attested

RESOLUTION_TIMEOUT_SECONDS = 10.0

# `git ls-remote` output for a single ref is one short line; anything wildly
# larger indicates a malformed or hostile response, not a real ref.
_MAX_LS_REMOTE_BYTES = 4096
_FULL_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_MAX_DIAGNOSTIC_REVISION_CHARS = 64
_REF_RE = re.compile(r"^refs/[A-Za-z0-9][A-Za-z0-9._/-]{0,254}$")
_OWNER_REPO_RE = re.compile(r"^[A-Za-z0-9._-]+/[A-Za-z0-9._-]+$")
_SCP_REPOSITORY_RE = re.compile(r"^git@[A-Za-z0-9.-]+:[A-Za-z0-9._/-]+(?:\.git)?$")
_PROCESS_TERM_GRACE_SECONDS = 1.0

# Fixed-vocabulary error classification: never echo raw stderr (it can quote
# back the repository URL) -- only ever return one of these bounded strings.
_ERROR_CATEGORIES: tuple[tuple[str, str], ...] = (
    ("could not resolve host", "desired ref repository unreachable"),
    ("could not read", "desired ref authentication failed"),
    ("authentication", "desired ref authentication failed"),
    ("permission denied", "desired ref authentication failed"),
    ("not found", "desired ref not found"),
    ("timed out", "desired ref resolution timed out"),
)


@dataclass(frozen=True)
class DesiredRevision:
    """A resolved (or failed-to-resolve) desired source revision.

    ``sha`` is ``None`` whenever resolution did not produce an exact,
    well-formed commit hash -- callers must treat that as unknown/unhealthy,
    never as current.
    """

    repo: str
    ref: str
    sha: str | None
    error: str | None = None

    @property
    def resolved(self) -> bool:
        return self.sha is not None


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
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        process.wait(timeout=_PROCESS_TERM_GRACE_SECONDS)
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait()


def _run_ls_remote(repo: str, ref: str, timeout: float) -> subprocess.CompletedProcess[str]:
    """Run the bounded, read-only remote ref lookup.

    ``GIT_TERMINAL_PROMPT=0`` and a null ``GIT_ASKPASS`` stop git from
    blocking on an interactive credential prompt when auth fails, so a bad
    credential fails fast instead of hanging until *timeout*.
    """
    env = dict(os.environ)
    env["GIT_TERMINAL_PROMPT"] = "0"
    env["GIT_ASKPASS"] = "true"
    argv = ["git", "ls-remote", "--exit-code", repo, ref, f"{ref}^{{}}"]
    process = subprocess.Popen(
        argv,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
        env=env,
    )
    assert process.stdout is not None and process.stderr is not None
    buffers = {"stdout": bytearray(), "stderr": bytearray()}
    deadline = time.monotonic() + timeout
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ, "stdout")
            selector.register(process.stderr, selectors.EVENT_READ, "stderr")
            while selector.get_map() or process.poll() is None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise subprocess.TimeoutExpired(argv, timeout)
                for key, _events in selector.select(timeout=min(remaining, 0.05)):
                    buffer = buffers[key.data]
                    chunk = os.read(key.fd, _MAX_LS_REMOTE_BYTES + 1 - len(buffer))
                    if not chunk:
                        selector.unregister(key.fileobj)
                        continue
                    buffer.extend(chunk)
                    if len(buffer) > _MAX_LS_REMOTE_BYTES:
                        # Return fixed over-limit data; never retain unbounded diagnostics.
                        return subprocess.CompletedProcess(
                            argv, 1, stdout="x" * (_MAX_LS_REMOTE_BYTES + 1), stderr=""
                        )
            return subprocess.CompletedProcess(
                argv,
                process.wait(),
                stdout=buffers["stdout"].decode("utf-8", errors="replace"),
                stderr=buffers["stderr"].decode("utf-8", errors="replace"),
            )
    finally:
        _terminate_process(process)
        process.stdout.close()
        process.stderr.close()


def _classify_git_failure(stderr: str) -> str:
    lowered = stderr.lower()
    for needle, label in _ERROR_CATEGORIES:
        if needle in lowered:
            return label
    return "desired ref resolution failed"


def resolve_desired_revision(
    desired: object,
    *,
    timeout: float = RESOLUTION_TIMEOUT_SECONDS,
) -> DesiredRevision:
    """Resolve *desired* (a ``DesiredSourceConfig``) to one exact full SHA.

    Never raises. A moving ``ref`` is resolved through a single bounded
    ``git ls-remote`` call; an immutable ``sha`` is validated and returned
    without any network call. Timeout, missing ref, authentication failure,
    or a malformed/oversized response all resolve to ``sha=None`` with a
    bounded, secret-safe *error* -- never silently "current".
    """
    repo = str(getattr(desired, "repo", "") or "").strip()
    ref = str(getattr(desired, "ref", "") or "").strip()
    sha = str(getattr(desired, "sha", "") or "").strip().lower()
    safe_repository = _safe_repository(repo)
    safe_ref = ref if _valid_ref(ref) else ""
    display_repo = safe_repository[0] if safe_repository else ""

    if not repo or not (ref or sha):
        return DesiredRevision(
            repo=display_repo, ref=safe_ref, sha=None, error="desired revision unconfigured"
        )
    if safe_repository is None or (not sha and not safe_ref):
        return DesiredRevision(
            repo=display_repo, ref=safe_ref, sha=None, error="invalid desired source configuration"
        )
    repo = display_repo
    ref = safe_ref

    if sha:
        normalized = normalize_revision(sha)
        if normalized is None or not _FULL_SHA_RE.fullmatch(normalized):
            return DesiredRevision(
                repo=repo,
                ref="",
                sha=None,
                error="desired sha is not a full 40-character commit hash",
            )
        return DesiredRevision(repo=repo, ref="", sha=normalized, error=None)

    try:
        result = _run_ls_remote(safe_repository[1], ref, timeout)
    except subprocess.TimeoutExpired:
        return DesiredRevision(
            repo=repo, ref=ref, sha=None, error="desired ref resolution timed out"
        )
    except OSError:
        return DesiredRevision(
            repo=repo, ref=ref, sha=None, error="desired ref resolution failed to start"
        )

    stdout = result.stdout or ""
    stderr = result.stderr or ""
    if len(stdout) > _MAX_LS_REMOTE_BYTES or len(stderr) > _MAX_LS_REMOTE_BYTES:
        return DesiredRevision(
            repo=repo, ref=ref, sha=None, error="desired ref response was oversized"
        )

    if result.returncode == 2:
        # `git ls-remote --exit-code` exits 2 specifically for "no matching ref".
        return DesiredRevision(repo=repo, ref=ref, sha=None, error="desired ref not found")
    if result.returncode != 0:
        return DesiredRevision(repo=repo, ref=ref, sha=None, error=_classify_git_failure(stderr))

    revisions: dict[str, str] = {}
    for line in stdout.splitlines():
        parts = line.split("\t")
        if (
            len(parts) != 2
            or not _FULL_SHA_RE.fullmatch(parts[0].lower())
            or parts[1] not in {ref, f"{ref}^{{}}"}
            or (parts[1] in revisions and revisions[parts[1]] != parts[0].lower())
        ):
            return DesiredRevision(
                repo=repo, ref=ref, sha=None, error="desired ref response was malformed"
            )
        revisions[parts[1]] = parts[0].lower()
    candidate = revisions.get(f"{ref}^{{}}") or revisions.get(ref)
    if candidate is None:
        return DesiredRevision(
            repo=repo, ref=ref, sha=None, error="desired ref response was malformed"
        )
    return DesiredRevision(repo=repo, ref=ref, sha=candidate, error=None)


@dataclass(frozen=True)
class RuntimeRevision:
    """A single runtime surface's observed source revision.

    ``degraded`` marks a surface that is expected to lag briefly by policy
    (a busy old-generation worker kept alive for its drain grace) -- it is
    still reported and still counted as a mismatch, just labeled distinctly
    from an unexplained stale surface.
    """

    surface: str
    revision: str | None
    degraded: bool = False


@dataclass(frozen=True)
class SourceRevisionReport:
    """The result of comparing every runtime surface against the desired SHA."""

    desired: DesiredRevision
    surfaces: tuple[RuntimeRevision, ...]
    mismatches: tuple[str, ...]
    healthy: bool


def _bounded(value: str | None) -> str:
    if not value:
        return "none"
    return value[:_MAX_DIAGNOSTIC_REVISION_CHARS]


def evaluate_source_revision(
    desired: DesiredRevision,
    surfaces: Sequence[RuntimeRevision],
) -> SourceRevisionReport:
    """Compare every runtime surface's revision against the desired SHA.

    Pure and read-only: no I/O, no mutation. A surface counts as a mismatch
    when it is absent, unattested (dirty/unknown), or disagrees with the
    desired SHA. A ``degraded`` surface is labeled separately in the
    diagnostic text but is never excluded from ``mismatches`` -- an
    intentional rolling deploy must stay visibly non-green until every
    surface is current.
    """
    mismatches: list[str] = []
    if not desired.resolved:
        mismatches.append(desired.error or "desired revision unconfigured")

    for surface in surfaces:
        revision = surface.revision
        if revision is None:
            mismatches.append(f"{surface.surface}: no revision reported")
            continue
        if not revision_is_attested(revision):
            mismatches.append(f"{surface.surface}: unattested revision {_bounded(revision)}")
            continue
        if desired.resolved and revision != desired.sha:
            label = "degraded" if surface.degraded else "stale"
            mismatches.append(
                f"{surface.surface}: {label} revision {_bounded(revision)} "
                f"!= desired {_bounded(desired.sha)}"
            )

    healthy = desired.resolved and not mismatches
    return SourceRevisionReport(
        desired=desired,
        surfaces=tuple(surfaces),
        mismatches=tuple(mismatches),
        healthy=healthy,
    )
