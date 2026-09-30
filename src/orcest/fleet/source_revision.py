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
import subprocess
from collections.abc import Sequence
from dataclasses import dataclass

from orcest.revision import normalize_revision, revision_is_attested

RESOLUTION_TIMEOUT_SECONDS = 10.0

# `git ls-remote` output for a single ref is one short line; anything wildly
# larger indicates a malformed or hostile response, not a real ref.
_MAX_LS_REMOTE_BYTES = 4096
_FULL_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_MAX_DIAGNOSTIC_REVISION_CHARS = 64

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


def _run_ls_remote(repo: str, ref: str, timeout: float) -> subprocess.CompletedProcess[str]:
    """Run the bounded, read-only remote ref lookup.

    ``GIT_TERMINAL_PROMPT=0`` and a null ``GIT_ASKPASS`` stop git from
    blocking on an interactive credential prompt when auth fails, so a bad
    credential fails fast instead of hanging until *timeout*.
    """
    env = dict(os.environ)
    env["GIT_TERMINAL_PROMPT"] = "0"
    env["GIT_ASKPASS"] = "true"
    return subprocess.run(
        ["git", "ls-remote", "--exit-code", repo, ref],
        capture_output=True,
        text=True,
        timeout=timeout,
        env=env,
    )


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

    if not repo or not (ref or sha):
        return DesiredRevision(repo=repo, ref=ref, sha=None, error="desired revision unconfigured")

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
        result = _run_ls_remote(repo, ref, timeout)
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

    first_line = stdout.strip().splitlines()[0] if stdout.strip() else ""
    parts = first_line.split()
    candidate = parts[0].strip().lower() if parts else ""
    if len(parts) < 2 or not _FULL_SHA_RE.fullmatch(candidate):
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
