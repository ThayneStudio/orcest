"""Tests for orcest.workflow_contract.v1.project_bundle_compile (git-pinned compilation)."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

from orcest.workflow_contract.v1.project_bundle import BundleValidationError
from orcest.workflow_contract.v1.project_bundle_compile import compile_bundle
from orcest.workflow_contract.v1.project_bundle_source import (
    GitBundleSource,
    GitSourceError,
    resolve_commit,
)

_PROJECT_YAML = """\
apiVersion: orcest.dev/v1
kind: Project
spec:
  workflow: .orcest/workflows/implementation.yaml
"""

_WORKFLOW_YAML = """\
apiVersion: orcest.dev/v1
kind: Workflow
metadata:
  name: implementation
spec:
  implementation:
    profile: codex-default
    prompt: .orcest/prompts/implement.md
  verification:
    commands:
      - id: unit
        argv: [make, test]
    repair:
      profile: codex-default
      prompt: .orcest/prompts/repair.md
  review:
    slots:
      - id: correctness
        profile: claude-review
        prompt: .orcest/prompts/review-correctness.md
      - id: security
        profile: codex-review
        prompt: .orcest/prompts/review-security.md
    adjudicator:
      profile: claude-review
      prompt: .orcest/prompts/adjudicate.md
"""

_PROMPTS = {
    ".orcest/prompts/implement.md": "# Implement\n",
    ".orcest/prompts/repair.md": "# Repair\n",
    ".orcest/prompts/review-correctness.md": "# Review correctness\n",
    ".orcest/prompts/review-security.md": "# Review security\n",
    ".orcest/prompts/adjudicate.md": "# Adjudicate\n",
}


def _git(repo: Path, *args: str) -> subprocess.CompletedProcess:
    env = {
        **os.environ,
        "GIT_AUTHOR_NAME": "t",
        "GIT_AUTHOR_EMAIL": "t@example.com",
        "GIT_COMMITTER_NAME": "t",
        "GIT_COMMITTER_EMAIL": "t@example.com",
    }
    result = subprocess.run(
        ["git", "-C", str(repo), *args], capture_output=True, env=env, timeout=30
    )
    assert result.returncode == 0, result.stderr.decode()
    return result


def _init_repo(repo: Path, files: dict[str, str]) -> str:
    repo.mkdir(parents=True, exist_ok=True)
    _git(repo, "init", "-q")
    for relpath, content in files.items():
        path = repo / relpath
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        _git(repo, "add", relpath)
    _git(repo, "commit", "-q", "-m", "bundle")
    return _git(repo, "rev-parse", "HEAD").stdout.decode().strip()


def _valid_bundle_files() -> dict[str, str]:
    return {
        ".orcest/project.yaml": _PROJECT_YAML,
        ".orcest/workflows/implementation.yaml": _WORKFLOW_YAML,
        **_PROMPTS,
    }


def _open_source(repo: Path, commit_oid: str) -> GitBundleSource:
    return GitBundleSource(str(repo), resolve_commit(str(repo), commit_oid))


def test_compile_valid_bundle(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    commit = _init_repo(repo, _valid_bundle_files())
    compiled = compile_bundle(_open_source(repo, commit))
    assert compiled.workflow_hash.startswith("sha256:")
    assert set(compiled.files) == {
        ".orcest/project.yaml",
        ".orcest/workflows/implementation.yaml",
        *_PROMPTS,
    }
    assert compiled.files[".orcest/project.yaml"].media_kind == "CONFIG_JSON"
    assert compiled.files[".orcest/prompts/implement.md"].media_kind == "PROMPT_UTF8"


def test_compile_deterministic_across_clones(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    commit = _init_repo(repo, _valid_bundle_files())
    clone = tmp_path / "clone"
    _git(tmp_path, "clone", "-q", str(repo), str(clone))

    hash_a = compile_bundle(_open_source(repo, commit)).workflow_hash
    hash_b = compile_bundle(_open_source(clone, commit)).workflow_hash
    assert hash_a == hash_b


def test_compile_hash_changes_when_prompt_bytes_change(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    commit_a = _init_repo(repo, _valid_bundle_files())
    hash_a = compile_bundle(_open_source(repo, commit_a)).workflow_hash

    files = _valid_bundle_files()
    files[".orcest/prompts/implement.md"] = "# Implement (edited)\n"
    (repo / ".orcest/prompts/implement.md").write_text(files[".orcest/prompts/implement.md"])
    _git(repo, "add", ".orcest/prompts/implement.md")
    _git(repo, "commit", "-q", "-m", "edit prompt")
    commit_b = _git(repo, "rev-parse", "HEAD").stdout.decode().strip()

    hash_b = compile_bundle(_open_source(repo, commit_b)).workflow_hash
    assert hash_a != hash_b


def test_compile_symlink_rejected(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q")
    files = _valid_bundle_files()
    for relpath, content in files.items():
        if relpath == ".orcest/prompts/implement.md":
            continue
        path = repo / relpath
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        _git(repo, "add", relpath)
    symlink_path = repo / ".orcest/prompts/implement.md"
    symlink_path.parent.mkdir(parents=True, exist_ok=True)
    os.symlink("/etc/passwd", symlink_path)
    _git(repo, "add", ".orcest/prompts/implement.md")
    _git(repo, "commit", "-q", "-m", "symlink")
    commit = _git(repo, "rev-parse", "HEAD").stdout.decode().strip()

    with pytest.raises(BundleValidationError) as excinfo:
        compile_bundle(_open_source(repo, commit))
    assert any(d.code == "SYMLINK_REJECTED" for d in excinfo.value.diagnostics)


def test_compile_case_collision_rejected(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    files = _valid_bundle_files()
    files[".orcest/prompts/Implement.md"] = "# Duplicate-case file\n"
    commit = _init_repo(repo, files)

    with pytest.raises(BundleValidationError) as excinfo:
        compile_bundle(_open_source(repo, commit))
    assert any(d.code == "CASE_COLLISION" for d in excinfo.value.diagnostics)


def test_compile_missing_workflow_file_rejected(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    files = {".orcest/project.yaml": _PROJECT_YAML}
    commit = _init_repo(repo, files)

    with pytest.raises(BundleValidationError) as excinfo:
        compile_bundle(_open_source(repo, commit))
    assert any(d.code == "FILE_NOT_FOUND" for d in excinfo.value.diagnostics)


def test_compile_prompt_secret_rejected(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    files = _valid_bundle_files()
    files[".orcest/prompts/implement.md"] = "token: ghp_abcdefghijklmnopqrstuvwxyz012345\n"
    commit = _init_repo(repo, files)

    with pytest.raises(BundleValidationError) as excinfo:
        compile_bundle(_open_source(repo, commit))
    assert any(d.code == "SECRET_VALUE_REJECTED" for d in excinfo.value.diagnostics)


def test_compile_submodule_rejected(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q")
    files = _valid_bundle_files()
    for relpath, content in files.items():
        if relpath == ".orcest/prompts/implement.md":
            continue
        path = repo / relpath
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        _git(repo, "add", relpath)
    # Fabricate a gitlink (commit) tree entry without a real submodule checkout.
    _git(
        repo,
        "update-index",
        "--add",
        "--cacheinfo",
        "160000",
        "1111111111111111111111111111111111111111",
        ".orcest/prompts/implement.md",
    )
    _git(repo, "commit", "-q", "-m", "submodule")
    commit = _git(repo, "rev-parse", "HEAD").stdout.decode().strip()

    with pytest.raises(BundleValidationError) as excinfo:
        compile_bundle(_open_source(repo, commit))
    assert any(d.code == "SUBMODULE_REFERENCE_REJECTED" for d in excinfo.value.diagnostics)


def test_resolve_commit_unknown_revision(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    _init_repo(repo, _valid_bundle_files())
    with pytest.raises(GitSourceError):
        resolve_commit(str(repo), "not-a-real-revision")
