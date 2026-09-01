"""Git-backed, pinned-commit source access for repository-owned ``.orcest`` bundles.

``docs/wiki/repository-configuration.md`` requires that:

- the controller "resolves the registration-owned trusted base ref once and
  reads every bundle blob by that immutable commit" (never a PR head, never
  the working tree);
- "All referenced files MUST be regular Git blobs at the same trusted commit
  ... Absolute paths, `..`, symlinks, submodule entries, case-colliding
  paths, YAML includes, and network references are invalid."

This module is the local-CLI analogue of that server-side commit pin: every
byte this compiler reads comes from ``git cat-file``/``git ls-tree`` against
one resolved commit, never from the working tree, so an uncommitted or
PR-branch edit to ``.orcest`` cannot influence a `lint`/`explain`/`simulate`
run unless the caller explicitly points ``--revision`` at it.
"""

from __future__ import annotations

import subprocess
from dataclasses import dataclass

from orcest.workflow_contract.v1.identity import CommitId, ObjectFormat

__all__ = [
    "GitSourceError",
    "TreeEntry",
    "resolve_default_branch_revision",
    "resolve_commit",
    "GitBundleSource",
]

_GIT_TIMEOUT_SECONDS = 30
# ``git cat-file -s`` prints a decimal byte count. 64 bytes of stdout is far
# more than a well-formed size needs; anything larger is treated as malformed
# so the resulting GitSourceError stays bounded.
_MAX_CAT_FILE_SIZE_STDOUT_BYTES = 64


class GitSourceError(ValueError):
    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


def _run_git(repo_root: str, args: list[str]) -> subprocess.CompletedProcess:
    try:
        return subprocess.run(
            ["git", "-C", repo_root, *args],
            capture_output=True,
            timeout=_GIT_TIMEOUT_SECONDS,
        )
    except FileNotFoundError as exc:
        raise GitSourceError("GIT_NOT_FOUND", "the 'git' executable was not found on PATH") from exc
    except subprocess.TimeoutExpired as exc:
        raise GitSourceError("GIT_TIMEOUT", f"git {' '.join(args)} timed out") from exc


def resolve_default_branch_revision(repo_root: str) -> str:
    """Resolve the repository's trusted default-branch ref, e.g. ``refs/remotes/origin/main``.

    Fails closed (:class:`GitSourceError`) rather than silently falling back
    to the current working-tree ``HEAD`` -- on a PR branch checkout, ``HEAD``
    is exactly the untrusted PR-head commit this compiler must not treat as
    authoritative. Callers that cannot resolve an ``origin`` remote must pass
    ``--revision`` explicitly.
    """
    result = _run_git(repo_root, ["symbolic-ref", "--quiet", "refs/remotes/origin/HEAD"])
    if result.returncode == 0:
        ref = result.stdout.decode("utf-8", errors="replace").strip()
        if ref:
            return ref
    raise GitSourceError(
        "DEFAULT_BRANCH_UNRESOLVED",
        "could not resolve the repository's trusted default branch (no "
        "refs/remotes/origin/HEAD); pass --revision explicitly rather than "
        "trusting the current checkout",
    )


def resolve_commit(repo_root: str, revision: str) -> CommitId:
    result = _run_git(repo_root, ["rev-parse", "--verify", f"{revision}^{{commit}}"])
    if result.returncode != 0:
        raise GitSourceError(
            "REVISION_UNRESOLVED",
            f"revision {revision!r} did not resolve to a commit in this repository",
        )
    oid = result.stdout.decode("ascii", errors="replace").strip()
    return CommitId(object_format=ObjectFormat.SHA1, oid=oid)


@dataclass(frozen=True, slots=True)
class TreeEntry:
    path: str
    mode: str
    object_type: str
    oid: str


_ALLOWED_BLOB_MODES = frozenset({"100644", "100755"})


class GitBundleSource:
    """Read repository blobs at exactly one pinned, immutable commit."""

    def __init__(self, repo_root: str, commit: CommitId):
        self.repo_root = repo_root
        self.commit = commit

    def ls_tree_entry(self, path: str) -> TreeEntry | None:
        result = _run_git(self.repo_root, ["ls-tree", "-z", self.commit.oid, "--", path])
        if result.returncode != 0:
            raise GitSourceError("GIT_LS_TREE_FAILED", f"git ls-tree failed for {path!r}")
        raw = result.stdout.decode("utf-8", errors="replace")
        for record in raw.split("\x00"):
            if not record:
                continue
            meta, _, entry_path = record.partition("\t")
            if entry_path != path:
                continue
            mode, object_type, oid = meta.split(" ")
            return TreeEntry(path=entry_path, mode=mode, object_type=object_type, oid=oid)
        return None

    def list_tree_recursive(self, prefix: str) -> list[TreeEntry]:
        result = _run_git(self.repo_root, ["ls-tree", "-r", "-z", self.commit.oid, "--", prefix])
        if result.returncode != 0:
            raise GitSourceError("GIT_LS_TREE_FAILED", f"git ls-tree -r failed for {prefix!r}")
        raw = result.stdout.decode("utf-8", errors="replace")
        entries: list[TreeEntry] = []
        for record in raw.split("\x00"):
            if not record:
                continue
            meta, _, entry_path = record.partition("\t")
            mode, object_type, oid = meta.split(" ")
            entries.append(TreeEntry(path=entry_path, mode=mode, object_type=object_type, oid=oid))
        return entries

    def read_regular_blob(self, path: str, *, max_bytes: int) -> bytes:
        """Read ``path`` at the pinned commit; rejects a symlink, submodule, or other non-blob
        mode.

        Queries ``git cat-file -s`` first and refuses to materialize the blob
        when its size exceeds ``max_bytes``. A size-query failure or malformed
        size output raises :class:`GitSourceError` without falling through to
        ``git cat-file -p``.
        """
        entry = self.ls_tree_entry(path)
        if entry is None:
            raise GitSourceError("FILE_NOT_FOUND", f"{path} does not exist at {self.commit.oid}")
        if entry.object_type == "commit":
            raise GitSourceError(
                "SUBMODULE_REFERENCE_REJECTED", f"{path} is a submodule (gitlink) entry, not a file"
            )
        if entry.object_type != "blob":
            raise GitSourceError(
                "NOT_A_REGULAR_FILE", f"{path} is a {entry.object_type}, not a regular file"
            )
        if entry.mode not in _ALLOWED_BLOB_MODES:
            raise GitSourceError(
                "SYMLINK_REJECTED" if entry.mode == "120000" else "IRREGULAR_FILE_MODE_REJECTED",
                f"{path} has git mode {entry.mode!r}; only regular files "
                f"({sorted(_ALLOWED_BLOB_MODES)}) are allowed",
            )
        size = _blob_byte_size(self.repo_root, entry.oid, path=path)
        if size > max_bytes:
            raise GitSourceError(
                "DOCUMENT_TOO_LARGE",
                f"{path} exceeds the {max_bytes}-byte per-file limit",
            )
        result = _run_git(self.repo_root, ["cat-file", "-p", entry.oid])
        if result.returncode != 0:
            raise GitSourceError("GIT_CAT_FILE_FAILED", f"git cat-file failed to read {path}")
        return result.stdout


def _blob_byte_size(repo_root: str, oid: str, *, path: str) -> int:
    """Return the byte size of ``oid`` via ``git cat-file -s``, failing closed."""
    result = _run_git(repo_root, ["cat-file", "-s", oid])
    if result.returncode != 0:
        raise GitSourceError("GIT_CAT_FILE_FAILED", f"git cat-file -s failed to size {path}")
    if len(result.stdout) > _MAX_CAT_FILE_SIZE_STDOUT_BYTES:
        raise GitSourceError(
            "GIT_CAT_FILE_FAILED",
            f"git cat-file -s returned a malformed size for {path}",
        )
    text = result.stdout.decode("ascii", errors="replace").strip()
    if not text.isdigit():
        raise GitSourceError(
            "GIT_CAT_FILE_FAILED",
            f"git cat-file -s returned a malformed size for {path}",
        )
    return int(text)
