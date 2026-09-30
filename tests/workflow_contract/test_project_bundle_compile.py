"""Tests for orcest.workflow_contract.v1.project_bundle_compile (git-pinned compilation)."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

from orcest.workflow_contract.v1 import project_bundle_source as source_mod
from orcest.workflow_contract.v1.project_bundle import BundleValidationError
from orcest.workflow_contract.v1.project_bundle_compile import compile_bundle
from orcest.workflow_contract.v1.project_bundle_source import (
    GitBundleSource,
    GitSourceError,
    resolve_commit,
)
from orcest.workflow_contract.v1.project_bundle_yaml import MAX_DOCUMENT_BYTES

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


def _blob_oid(source: GitBundleSource, path: str) -> str:
    entry = source.ls_tree_entry(path)
    assert entry is not None
    return entry.oid


def _spy_run_git(
    monkeypatch: pytest.MonkeyPatch,
    *,
    size_stdout_by_oid: dict[str, bytes] | None = None,
    size_returncode_by_oid: dict[str, int] | None = None,
) -> list[list[str]]:
    """Record every ``_run_git`` argv; optionally stub ``cat-file -s`` per oid."""
    calls: list[list[str]] = []
    original = source_mod._run_git

    def _spy(repo_root: str, args: list[str]) -> subprocess.CompletedProcess:
        calls.append(list(args))
        if len(args) >= 3 and args[0] == "cat-file" and args[1] == "-s":
            oid = args[2]
            if size_returncode_by_oid is not None and oid in size_returncode_by_oid:
                return subprocess.CompletedProcess(
                    args=["git", "-C", repo_root, *args],
                    returncode=size_returncode_by_oid[oid],
                    stdout=b"",
                    stderr=b"cat-file: stub failure",
                )
            if size_stdout_by_oid is not None and oid in size_stdout_by_oid:
                return subprocess.CompletedProcess(
                    args=["git", "-C", repo_root, *args],
                    returncode=0,
                    stdout=size_stdout_by_oid[oid],
                    stderr=b"",
                )
        return original(repo_root, args)

    monkeypatch.setattr(source_mod, "_run_git", _spy)
    return calls


def _cat_file_called_for(calls: list[list[str]], flag: str, oid: str) -> bool:
    return any(len(args) >= 3 and args[:3] == ["cat-file", flag, oid] for args in calls)


def test_read_regular_blob_oversized_skips_content_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = tmp_path / "repo"
    content = "hello\n"
    commit = _init_repo(repo, {"blob.txt": content})
    source = _open_source(repo, commit)
    oid = _blob_oid(source, "blob.txt")
    calls = _spy_run_git(monkeypatch)

    with pytest.raises(GitSourceError) as excinfo:
        source.read_regular_blob("blob.txt", max_bytes=len(content) - 1)
    assert excinfo.value.code == "DOCUMENT_TOO_LARGE"
    assert _cat_file_called_for(calls, "-s", oid)
    assert not _cat_file_called_for(calls, "-p", oid)


def test_read_regular_blob_at_limit_is_readable(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    content = "hello\n"
    commit = _init_repo(repo, {"blob.txt": content})
    source = _open_source(repo, commit)
    assert source.read_regular_blob("blob.txt", max_bytes=len(content)) == content.encode()


def test_read_regular_blob_size_query_failure_skips_content_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = tmp_path / "repo"
    commit = _init_repo(repo, {"blob.txt": "hello\n"})
    source = _open_source(repo, commit)
    oid = _blob_oid(source, "blob.txt")
    calls = _spy_run_git(monkeypatch, size_returncode_by_oid={oid: 128})

    with pytest.raises(GitSourceError) as excinfo:
        source.read_regular_blob("blob.txt", max_bytes=MAX_DOCUMENT_BYTES)
    assert excinfo.value.code == "GIT_CAT_FILE_FAILED"
    assert "blob.txt" in str(excinfo.value)
    assert len(str(excinfo.value)) < 500
    assert _cat_file_called_for(calls, "-s", oid)
    assert not _cat_file_called_for(calls, "-p", oid)


@pytest.mark.parametrize(
    "stdout",
    [
        b"",
        b"nope\n",
        b"-1\n",
        b"1.5\n",
        b"1 2\n",
        b"+10\n",
        b" \n",
        b"x" * 65,
    ],
    ids=[
        "empty",
        "non-numeric",
        "negative",
        "float",
        "two-tokens",
        "leading-plus",
        "whitespace",
        "oversized-stdout",
    ],
)
def test_read_regular_blob_malformed_size_skips_content_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, stdout: bytes
) -> None:
    repo = tmp_path / "repo"
    commit = _init_repo(repo, {"blob.txt": "hello\n"})
    source = _open_source(repo, commit)
    oid = _blob_oid(source, "blob.txt")
    calls = _spy_run_git(monkeypatch, size_stdout_by_oid={oid: stdout})

    with pytest.raises(GitSourceError) as excinfo:
        source.read_regular_blob("blob.txt", max_bytes=MAX_DOCUMENT_BYTES)
    assert excinfo.value.code == "GIT_CAT_FILE_FAILED"
    assert len(str(excinfo.value)) < 500
    if stdout.strip():
        assert stdout.strip() not in str(excinfo.value).encode()
    assert _cat_file_called_for(calls, "-s", oid)
    assert not _cat_file_called_for(calls, "-p", oid)


def test_compile_oversized_project_skips_content_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = tmp_path / "repo"
    commit = _init_repo(repo, _valid_bundle_files())
    source = _open_source(repo, commit)
    oid = _blob_oid(source, ".orcest/project.yaml")
    calls = _spy_run_git(
        monkeypatch, size_stdout_by_oid={oid: f"{MAX_DOCUMENT_BYTES + 1}\n".encode()}
    )

    with pytest.raises(BundleValidationError) as excinfo:
        compile_bundle(source)
    assert any(d.code == "DOCUMENT_TOO_LARGE" for d in excinfo.value.diagnostics)
    assert _cat_file_called_for(calls, "-s", oid)
    assert not _cat_file_called_for(calls, "-p", oid)


def test_compile_oversized_workflow_skips_content_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = tmp_path / "repo"
    commit = _init_repo(repo, _valid_bundle_files())
    source = _open_source(repo, commit)
    oid = _blob_oid(source, ".orcest/workflows/implementation.yaml")
    calls = _spy_run_git(
        monkeypatch, size_stdout_by_oid={oid: f"{MAX_DOCUMENT_BYTES + 1}\n".encode()}
    )

    with pytest.raises(BundleValidationError) as excinfo:
        compile_bundle(source)
    assert any(d.code == "DOCUMENT_TOO_LARGE" for d in excinfo.value.diagnostics)
    assert _cat_file_called_for(calls, "-s", oid)
    assert not _cat_file_called_for(calls, "-p", oid)


def test_compile_oversized_prompt_skips_content_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = tmp_path / "repo"
    commit = _init_repo(repo, _valid_bundle_files())
    source = _open_source(repo, commit)
    oid = _blob_oid(source, ".orcest/prompts/implement.md")
    calls = _spy_run_git(
        monkeypatch, size_stdout_by_oid={oid: f"{MAX_DOCUMENT_BYTES + 1}\n".encode()}
    )

    with pytest.raises(BundleValidationError) as excinfo:
        compile_bundle(source)
    assert any(d.code == "DOCUMENT_TOO_LARGE" for d in excinfo.value.diagnostics)
    assert _cat_file_called_for(calls, "-s", oid)
    assert not _cat_file_called_for(calls, "-p", oid)


def test_compile_blob_exactly_at_document_limit_is_readable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = tmp_path / "repo"
    commit = _init_repo(repo, _valid_bundle_files())
    source = _open_source(repo, commit)
    oids = {
        _blob_oid(source, path)
        for path in (
            ".orcest/project.yaml",
            ".orcest/workflows/implementation.yaml",
            *_PROMPTS,
        )
    }
    calls = _spy_run_git(
        monkeypatch,
        size_stdout_by_oid={oid: f"{MAX_DOCUMENT_BYTES}\n".encode() for oid in oids},
    )

    compiled = compile_bundle(source)
    assert compiled.workflow_hash.startswith("sha256:")
    for oid in oids:
        assert _cat_file_called_for(calls, "-s", oid)
        assert _cat_file_called_for(calls, "-p", oid)
