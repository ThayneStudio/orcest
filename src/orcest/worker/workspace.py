"""Repository workspace management for worker task execution.

Handles shallow cloning, branch checkout, and cleanup. Each task gets its
own temporary directory under base_dir to prevent state leakage between tasks.
"""

import shutil
import subprocess
import tempfile
from pathlib import Path


class Workspace:
    """Manages a temporary repo clone for task execution."""

    def __init__(self, base_dir: str):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self._work_dir: Path | None = None

    @property
    def path(self) -> Path:
        if self._work_dir is None:
            raise RuntimeError("Workspace not initialized. Call setup() first.")
        return self._work_dir

    def setup(self, repo: str, branch: str | None, token: str) -> Path:
        """Clone the repo and configure the workspace.

        Args:
            repo: "owner/repo" format
            branch: branch to checkout (None = default branch)
            token: GitHub PAT for clone auth and gh CLI

        Returns:
            Path to the cloned repo directory.
        """
        # Create unique temp directory under base_dir
        self._work_dir = Path(tempfile.mkdtemp(dir=self.base_dir))

        clone_url = f"https://x-access-token:{token}@github.com/{repo}.git"

        # Shallow clone for speed
        cmd: list[str] = [
            "git",
            "clone",
            "--depth",
            "1",
            "--single-branch",
        ]
        if branch:
            cmd.extend(["--branch", branch])

        cmd.extend([clone_url, str(self._work_dir / "repo")])

        subprocess.run(cmd, check=True, capture_output=True, text=True)
        self._work_dir = self._work_dir / "repo"

        # Configure gh CLI auth for this workspace
        # gh uses GITHUB_TOKEN env var, which we'll pass to Claude subprocess
        return self._work_dir

    def cleanup(self) -> None:
        """Remove the workspace directory."""
        if self._work_dir and self._work_dir.exists():
            # _work_dir points to .../tmpXXXX/repo, so remove the parent
            # temp directory to clean up everything
            shutil.rmtree(self._work_dir.parent, ignore_errors=True)
            self._work_dir = None
