"""Repository workspace management for worker task execution.

Handles cloning, branch checkout, git credential setup, and cleanup.
Each task gets its own temporary directory under base_dir to prevent
state leakage between tasks.
"""

import logging
import shutil
import subprocess
import tempfile
from pathlib import Path

logger = logging.getLogger(__name__)

# Timeout for git clone operations (seconds). Prevents the worker from
# hanging indefinitely on network issues.
_CLONE_TIMEOUT_SECONDS = 300


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
    """


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
        base_branch: str | None = None,
    ) -> Path:
        """Clone the repo and configure the workspace.

        Args:
            repo: "owner/repo" format
            branch: branch to checkout (None = default branch)
            token: GitHub PAT for clone auth and gh CLI
            base_branch: if set, rebase onto this branch after clone

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
                f"for {repo}" + (f" branch {branch}" if branch else "")
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

        # Rebase onto the base branch so the worker operates on up-to-date
        # code (e.g. picks up workflow changes, avoids stale conflicts).
        if base_branch and branch:
            self._rebase_onto(repo_dir, base_branch)

        self._work_dir = repo_dir
        return self._work_dir

    def _rebase_onto(self, repo_dir: Path, base_branch: str) -> None:
        """Fetch and rebase the current branch onto the base branch."""
        try:
            subprocess.run(
                ["git", "-C", str(repo_dir), "fetch", "origin", base_branch],
                check=True,
                capture_output=True,
                text=True,
                timeout=120,
            )
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
            safe_msg = f"git fetch origin {base_branch} failed"
            if isinstance(exc, subprocess.CalledProcessError) and exc.stderr:
                safe_msg += f": {exc.stderr.strip()}"
            raise WorkspaceError(safe_msg) from None

        result = subprocess.run(
            ["git", "-C", str(repo_dir), "rebase", f"origin/{base_branch}"],
            capture_output=True,
            text=True,
            timeout=120,
        )
        if result.returncode != 0:
            # Abort the failed rebase to leave the repo in a clean state
            subprocess.run(
                ["git", "-C", str(repo_dir), "rebase", "--abort"],
                capture_output=True,
                timeout=30,
            )
            raise WorkspaceError(
                f"rebase onto origin/{base_branch} failed with conflicts"
            )

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
