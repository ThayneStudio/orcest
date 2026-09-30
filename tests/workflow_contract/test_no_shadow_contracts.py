"""Repository-wide gate: no shadow protocol constants, no non-domain-separated digests.

Issue #668 acceptance criterion: "A repository-wide test prevents feature code
from defining shadow protocol constants or non-domain-separated digests."

Every ``orcest.<name>/<n>`` protocol-version literal and every ``sha256``
content digest computation MUST live in
``src/orcest/workflow_contract/v1/protocol_registry.py`` and
``src/orcest/workflow_contract/v1/digest.py`` respectively. Any other file in
``src/`` that hard-codes one of these is a shadow contract: a second,
un-registered source of truth that will silently drift from the wiki.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"
CONTRACT_PACKAGE_ROOT = SRC_ROOT / "orcest" / "workflow_contract"
DIGEST_MODULE = CONTRACT_PACKAGE_ROOT / "v1" / "digest.py"

# Files that predate the Workflow-Control v1 contract registry and hash
# content unrelated to it (session-token comparison, credential redaction,
# PR/output dedup fingerprints). They are grandfathered in exactly as they
# are today; any *other* file (including new files added later) must use
# orcest.workflow_contract.v1.digest instead of calling hashlib.sha256
# itself. This keeps the gate repository-wide for all new/changed code
# without rewriting unrelated legacy call sites as a side effect of #668.
LEGACY_SHA256_FILES = {
    SRC_ROOT / "orcest" / "monitor" / "auth.py",
    SRC_ROOT / "orcest" / "monitor" / "ingest_app.py",
    SRC_ROOT / "orcest" / "orchestrator" / "issue_publication.py",
    SRC_ROOT / "orcest" / "orchestrator" / "pr_ops.py",
    SRC_ROOT / "orcest" / "shared" / "credential_handoff.py",
    SRC_ROOT / "orcest" / "shared" / "providers.py",
    SRC_ROOT / "orcest" / "worker" / "loop.py",
    SRC_ROOT / "orcest" / "worker" / "repetition.py",
}

PROTOCOL_LITERAL_RE = re.compile(r"orcest\.[a-z][a-z0-9-]*/[0-9]+")
SHA256_CALL_RE = re.compile(r"hashlib\.sha256\s*\(|hashlib\.new\(\s*[\"']sha256[\"']")


def _python_files() -> list[Path]:
    return sorted(p for p in SRC_ROOT.rglob("*.py") if "__pycache__" not in p.parts)


def test_no_shadow_protocol_version_literals() -> None:
    """No file outside the contract package may hard-code an orcest.*/N literal.

    Files inside the contract package itself are exempt: that package is the
    sanctioned single source of truth, and its docstrings legitimately quote
    the very literals they document.
    """
    violations: list[str] = []
    for path in _python_files():
        if CONTRACT_PACKAGE_ROOT in path.parents:
            continue
        text = path.read_text(encoding="utf-8")
        for lineno, line in enumerate(text.splitlines(), start=1):
            for match in PROTOCOL_LITERAL_RE.finditer(line):
                violations.append(f"{path.relative_to(REPO_ROOT)}:{lineno}: {match.group(0)!r}")
    assert not violations, (
        "Found protocol-version literal(s) defined outside "
        "orcest.workflow_contract. Register the literal in "
        "orcest.workflow_contract.v1.protocol_registry and import it from "
        "there instead of hard-coding it:\n" + "\n".join(violations)
    )


def test_no_shadow_sha256_digests() -> None:
    """No file but the v1 digest module (or a grandfathered legacy file) may hash sha256."""
    violations: list[str] = []
    for path in _python_files():
        if path == DIGEST_MODULE or path in LEGACY_SHA256_FILES:
            continue
        text = path.read_text(encoding="utf-8")
        for lineno, line in enumerate(text.splitlines(), start=1):
            if SHA256_CALL_RE.search(line):
                violations.append(f"{path.relative_to(REPO_ROOT)}:{lineno}: {line.strip()!r}")
    assert not violations, (
        "Found a raw hashlib.sha256 call outside the v1 digest module. Every "
        "content digest MUST be domain-separated and produced by "
        "orcest.workflow_contract.v1.digest instead of an ad hoc "
        "hashlib.sha256 call:\n" + "\n".join(violations)
    )


@pytest.mark.parametrize("path", [DIGEST_MODULE, *sorted(LEGACY_SHA256_FILES)])
def test_allowlisted_files_exist(path: Path) -> None:
    assert path.is_file(), f"allowlisted contract file is missing: {path}"
