"""Runtime source-revision attestation for release verification."""

from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path

_REVISION_RE = re.compile(r"^[0-9a-f]{7,64}(?:-dirty)?$")
UNKNOWN_REVISION = "unknown"
SYSTEM_REVISION_FILE = Path("/etc/orcest/source-revision")


def normalize_revision(value: object) -> str | None:
    """Return a normalized attested revision, or ``None`` when invalid."""
    revision = str(value or "").strip().lower()
    return revision if _REVISION_RE.fullmatch(revision) else None


def _generated_revision() -> str | None:
    try:
        from orcest._build_revision import BUILD_REVISION
    except ImportError:
        return None
    return normalize_revision(BUILD_REVISION)


def _system_revision(path: Path = SYSTEM_REVISION_FILE) -> str | None:
    try:
        return normalize_revision(path.read_text(encoding="utf-8"))
    except OSError:
        return None


def _checkout_revision() -> str | None:
    """Resolve a development checkout revision without claiming clean state."""
    for candidate in Path(__file__).resolve().parents:
        if not (candidate / ".git").exists():
            continue
        try:
            head = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=candidate,
                capture_output=True,
                text=True,
                timeout=5,
                check=True,
            ).stdout.strip()
            status = subprocess.run(
                ["git", "status", "--porcelain"],
                cwd=candidate,
                capture_output=True,
                text=True,
                timeout=5,
                check=True,
            ).stdout.strip()
        except (OSError, subprocess.SubprocessError):
            return None
        normalized = normalize_revision(head)
        if normalized is None:
            return None
        return f"{normalized}-dirty" if status else normalized
    return None


def get_build_revision() -> str:
    """Return the strongest available revision attestation.

    Production images set ``ORCEST_BUILD_REVISION`` and release-built source
    archives contain ``orcest._build_revision``. Host installations may use
    the root-owned system revision file. A source checkout takes precedence
    over potentially stale generated or installed-host markers and explicitly
    marks modified checkouts as dirty.
    """
    environment = normalize_revision(os.environ.get("ORCEST_BUILD_REVISION"))
    return (
        environment
        or _checkout_revision()
        or _generated_revision()
        or _system_revision()
        or UNKNOWN_REVISION
    )


def revision_is_attested(revision: str | None = None) -> bool:
    """Return whether *revision* identifies an exact, clean source commit."""
    value = revision or get_build_revision()
    normalized = normalize_revision(value)
    return normalized is not None and not normalized.endswith("-dirty")
