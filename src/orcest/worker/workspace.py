"""Repository workspace management for worker task execution.

Handles cloning, branch checkout, git credential setup, and cleanup.
Each task gets its own temporary directory under base_dir to prevent
state leakage between tasks.
"""

import logging
import re
import shutil
import subprocess
import tempfile
from pathlib import Path

logger = logging.getLogger(__name__)

# Timeout for git clone operations (seconds). Prevents the worker from
# hanging indefinitely on network issues.
_CLONE_TIMEOUT_SECONDS = 300
_FETCH_TIMEOUT_SECONDS = 120
_OWNER_RE = re.compile(r"^[A-Za-z0-9._-]+$")
_REPO_RE = re.compile(r"^[A-Za-z0-9._-]+/[A-Za-z0-9._-]+$")
_REF_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]{0,243}$")


def _assert_same_repo_expected_ref(repo: str, expected_owner: str, expected_ref: str) -> None:
    """Reject an owner or ref that is not the same-repository expected head."""
    if not _REPO_RE.fullmatch(repo):
        raise WorkspaceError(f"refusing to resume unexpected repository {repo!r}")
    owner, _sep, _name = repo.partition("/")
    if expected_owner != owner or not _OWNER_RE.fullmatch(expected_owner):
        raise WorkspaceError(f"refusing to resume unexpected owner {expected_owner!r}")
    if not _is_safe_git_ref(expected_ref):
        raise WorkspaceError(f"refusing to resume unexpected ref {expected_ref!r}")


def _is_safe_git_ref(ref: str) -> bool:
    if not ref or not _REF_RE.fullmatch(ref):
        return False
    if ".." in ref or "//" in ref or "@{" in ref:
        return False
    if ref.endswith(".lock") or ref.endswith("/") or ref.endswith("."):
        return False
    if ref.startswith("/") or ref.startswith("-"):
        return False
    if any(ord(char) < 32 for char in ref):
        return False
    return True


def _missing_remote_ref(stderr: str) -> bool:
    text = stderr.lower()
    if "couldn't find remote ref" in text:
        return True
    return "remote ref" in text and "not found" in text


def _transient_fetch_error(stderr: str) -> bool:
    text = stderr.lower()
    return any(
        token in text
        for token in (
            "timed out",
            "timeout",
            "temporarily unavailable",
            "connection reset",
            "could not resolve host",
            "unable to access",
            "ssl",
        )
    )


def _git_config(repo_dir: Path, key: str, value: str) -> None:
    """Set a git config value in the given repo. Non-fatal on failure."""
    try:
        subprocess.run(
            ["git", "-C", str(repo_dir), "config", key, value],
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
        logger.warning("Failed to set git config %s in %s", key, repo_dir)


class WorkspaceError(Exception):
    """Raised when workspace setup or cleanup fails.

    Unlike a raw subprocess.CalledProcessError, this exception is
    guaranteed not to contain secrets (tokens) in its message.

    ``transient=True`` indicates a failure that is safe to retry automatically
    (e.g. a clone timeout due to a transient network issue).  Permanent
    failures such as bad credentials or a deleted branch leave the flag False
    so the orchestrator can label the PR for human review.
    """

    def __init__(self, message: str, transient: bool = False) -> None:
        super().__init__(message)
        self.transient = transient


class Workspace:
    """Manages a temporary repo clone for task execution.

    Each call to ``setup()`` creates a fresh temp directory under *base_dir*.
    ``cleanup()`` removes it.  The class is reusable across tasks -- call
    ``cleanup()`` then ``setup()`` again for the next task.
    """

    def __init__(self, base_dir: str):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        # _temp_dir is the top-level temp directory created by mkdtemp.
        # _work_dir is the repo checkout inside it (_temp_dir / "repo").
        # We track both so that cleanup always removes the right directory,
        # even if setup() fails partway through.
        self._temp_dir: Path | None = None
        self._work_dir: Path | None = None

    @property
    def path(self) -> Path:
        if self._work_dir is None:
            raise RuntimeError("Workspace not initialized. Call setup() first.")
        return self._work_dir

    def setup(
        self,
        repo: str,
        branch: str | None,
        token: str,
    ) -> Path:
        """Clone the repo and configure the workspace.

        Args:
            repo: "owner/repo" format
            branch: branch to checkout (None = default branch)
            token: GitHub PAT for clone auth and gh CLI

        Returns:
            Path to the cloned repo directory.

        Raises:
            WorkspaceError: if the clone or post-clone configuration fails.
                The error message is safe to log (no embedded secrets).
        """
        # If a previous setup left state (e.g. caller forgot cleanup), clean
        # it up first so we don't leak temp directories.
        if self._temp_dir is not None:
            self.cleanup()

        # Create unique temp directory under base_dir
        self._temp_dir = Path(tempfile.mkdtemp(dir=self.base_dir))
        repo_dir = self._temp_dir / "repo"

        clone_url = f"https://x-access-token:{token}@github.com/{repo}.git"

        cmd: list[str] = [
            "git",
            "clone",
        ]
        if branch:
            cmd.extend(["--branch", branch])

        cmd.extend([clone_url, str(repo_dir)])

        try:
            subprocess.run(
                cmd,
                check=True,
                capture_output=True,
                text=True,
                timeout=_CLONE_TIMEOUT_SECONDS,
            )
        except subprocess.TimeoutExpired:
            raise WorkspaceError(
                f"git clone timed out after {_CLONE_TIMEOUT_SECONDS}s "
                f"for {repo}" + (f" branch {branch}" if branch else ""),
                transient=True,
            )
        except subprocess.CalledProcessError as exc:
            # Sanitise stderr/stdout so the token doesn't leak into logs.
            # Git usually prints the URL in error messages.
            safe_stderr = exc.stderr.replace(token, "***") if exc.stderr else ""
            raise WorkspaceError(
                f"git clone failed (exit {exc.returncode}) for {repo}"
                + (f" branch {branch}" if branch else "")
                + (f": {safe_stderr.strip()}" if safe_stderr.strip() else "")
            ) from None  # suppress chained exc that contains the token in cmd

        # Strip the token out of .git/config.  git clone stores the full
        # remote URL including embedded credentials in [remote "origin"].
        try:
            subprocess.run(
                [
                    "git",
                    "-C",
                    str(repo_dir),
                    "remote",
                    "set-url",
                    "origin",
                    f"https://github.com/{repo}.git",
                ],
                check=True,
                capture_output=True,
                text=True,
                timeout=30,
            )
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            # Non-fatal: the token will be cleaned up with the directory.
            # Log at warning level without including the exception (which
            # may contain the URL in its cmd attribute).
            logger.warning(
                "Failed to strip credentials from .git/config for %s "
                "(will be cleaned up with workspace directory)",
                repo,
            )

        # Configure a credential helper so git push works via the
        # GITHUB_TOKEN env var (which the runner forwards to Claude).
        # This avoids storing the token in plaintext in .git/config.
        _git_config(
            repo_dir,
            "credential.helper",
            "!f() { echo username=x-access-token; echo password=$GITHUB_TOKEN; }; f",
        )

        # Set git identity so Claude's commits have a valid author.
        _git_config(repo_dir, "user.name", "orcest-bot")
        _git_config(repo_dir, "user.email", "orcest-bot@users.noreply.github.com")

        self._work_dir = repo_dir
        return self._work_dir

    def current_head_sha(self) -> str:
        """Return the checked-out HEAD SHA for the current workspace."""
        repo_dir = self.path
        try:
            result = subprocess.run(
                ["git", "-C", str(repo_dir), "rev-parse", "HEAD"],
                check=True,
                capture_output=True,
                text=True,
                timeout=30,
            )
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
            raise WorkspaceError("failed to read workspace HEAD SHA") from exc
        return result.stdout.strip()

    def resume_expected_ref(self, repo: str, expected_owner: str, expected_ref: str) -> bool:
        """Fetch and check out *expected_ref* from a default-branch workspace.

        Only the authoritative same-repository owner/ref is resumed. An
        unexpected owner or ref is rejected without touching git. A missing
        remote ref returns False and leaves the default-branch checkout in
        place.

        Returns:
            True when the expected ref was fetched and checked out.
        """
        repo_dir = self.path
        _assert_same_repo_expected_ref(repo, expected_owner, expected_ref)
        fetch_spec = f"refs/heads/{expected_ref}:refs/remotes/origin/{expected_ref}"
        try:
            subprocess.run(
                [
                    "git",
                    "-C",
                    str(repo_dir),
                    "fetch",
                    "--no-tags",
                    "origin",
                    fetch_spec,
                ],
                check=True,
                capture_output=True,
                text=True,
                timeout=_FETCH_TIMEOUT_SECONDS,
            )
        except subprocess.TimeoutExpired:
            raise WorkspaceError(
                f"git fetch timed out after {_FETCH_TIMEOUT_SECONDS}s "
                f"for {repo} ref {expected_ref}",
                transient=True,
            ) from None
        except subprocess.CalledProcessError as exc:
            stderr = exc.stderr or ""
            if _missing_remote_ref(stderr):
                logger.info(
                    "Expected ref %s does not exist on origin for %s; staying on default branch",
                    expected_ref,
                    repo,
                )
                return False
            raise WorkspaceError(
                f"git fetch failed (exit {exc.returncode}) for {repo} ref {expected_ref}"
                + (f": {stderr.strip()}" if stderr.strip() else ""),
                transient=_transient_fetch_error(stderr),
            ) from None
        try:
            subprocess.run(
                [
                    "git",
                    "-C",
                    str(repo_dir),
                    "checkout",
                    "-B",
                    expected_ref,
                    f"origin/{expected_ref}",
                ],
                check=True,
                capture_output=True,
                text=True,
                timeout=30,
            )
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
            detail = ""
            if isinstance(exc, subprocess.CalledProcessError) and exc.stderr:
                detail = f": {exc.stderr.strip()}"
            raise WorkspaceError(
                f"git checkout failed for {repo} ref {expected_ref}{detail}"
            ) from None
        return True

    def cleanup(self) -> None:
        """Remove the workspace directory.

        Safe to call multiple times, before setup(), or after a failed setup().
        """
        temp = self._temp_dir
        self._temp_dir = None
        self._work_dir = None

        if temp is not None:
            shutil.rmtree(
                temp,
                onexc=lambda func, path, exc: logger.warning(
                    "Failed to remove workspace path %s via %s: %s", path, func.__name__, exc
                ),
            )
