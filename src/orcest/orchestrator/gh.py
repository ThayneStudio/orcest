"""GitHub CLI wrapper for orchestrator operations.

All GitHub interaction goes through the `gh` CLI. No direct API calls.
Every function takes `token` as a parameter rather than reading it from
the environment, making testing straightforward and keeping the dependency
explicit.
"""

import json
import logging
import os
import re
import subprocess
import tempfile

logger = logging.getLogger(__name__)

_REPO_RE = re.compile(r"^[A-Za-z0-9._-]+/[A-Za-z0-9._-]+$")


class GhCliError(Exception):
    """Raised when a gh CLI operation fails."""

    def __init__(self, message: str, stderr: str = "", returncode: int | None = None):
        super().__init__(message)
        self.stderr = stderr
        self.returncode = returncode


class GhNotInstalledError(GhCliError):
    """Raised when the gh CLI binary is not found on PATH."""


def _validate_repo(repo: str) -> None:
    """Validate that repo matches the expected 'owner/repo' format.

    Raises ValueError if the format is invalid.
    """
    if not _REPO_RE.match(repo):
        raise ValueError(
            f"Invalid repo format: {repo!r}. Expected 'owner/repo' with "
            "alphanumeric characters, hyphens, underscores, and dots only."
        )


def _run_gh(args: list[str], token: str) -> str:
    """Execute a gh CLI command and return stdout.

    Sets both GITHUB_TOKEN and GH_TOKEN for compatibility across
    gh CLI versions.

    Raises:
        GhNotInstalledError: If the gh CLI binary is not on PATH.
        GhCliError: On non-zero exit, with stderr included in the message.
    """
    env = os.environ.copy()
    env["GITHUB_TOKEN"] = token
    env["GH_TOKEN"] = token  # gh CLI also checks GH_TOKEN

    try:
        result = subprocess.run(
            ["gh", *args],
            capture_output=True,
            text=True,
            env=env,
            check=True,
        )
    except FileNotFoundError:
        raise GhNotInstalledError(
            "gh CLI not found on PATH. Install it from https://cli.github.com/"
        ) from None
    except subprocess.CalledProcessError as exc:
        raise GhCliError(
            f"gh command failed (exit {exc.returncode}): {exc.stderr.strip()}",
            stderr=exc.stderr,
            returncode=exc.returncode,
        ) from exc

    return result.stdout.strip()


def _run_gh_bytes(args: list[str], token: str) -> bytes:
    """Execute a gh CLI command and return raw stdout bytes.

    Same as _run_gh but does not decode output. Useful for endpoints
    that return binary data (e.g., log zip files).

    Raises:
        GhNotInstalledError: If the gh CLI binary is not on PATH.
        GhCliError: On non-zero exit, with stderr included in the message.
    """
    env = os.environ.copy()
    env["GITHUB_TOKEN"] = token
    env["GH_TOKEN"] = token

    try:
        result = subprocess.run(
            ["gh", *args],
            capture_output=True,
            env=env,
            check=True,
        )
    except FileNotFoundError:
        raise GhNotInstalledError(
            "gh CLI not found on PATH. Install it from https://cli.github.com/"
        ) from None
    except subprocess.CalledProcessError as exc:
        stderr_text = exc.stderr.decode("utf-8", errors="replace") if exc.stderr else ""
        raise GhCliError(
            f"gh command failed (exit {exc.returncode}): {stderr_text.strip()}",
            stderr=stderr_text,
            returncode=exc.returncode,
        ) from exc

    return result.stdout


def list_open_prs(repo: str, token: str) -> list[dict]:
    """List all open PRs, sorted oldest first.

    Returns list of dicts with keys: number, title, headRefName,
    author, createdAt, labels, reviewDecision.
    """
    _validate_repo(repo)
    output = _run_gh([
        "pr", "list",
        "--repo", repo,
        "--state", "open",
        "--json", "number,title,headRefName,author,createdAt,"
                  "labels,reviewDecision",
        "--limit", "100",
    ], token)
    return json.loads(output) if output else []


def get_pr(repo: str, number: int, token: str) -> dict:
    """Get detailed PR info."""
    _validate_repo(repo)
    output = _run_gh([
        "pr", "view", str(number),
        "--repo", repo,
        "--json", "number,title,body,headRefName,baseRefName,state,"
                  "author,labels,reviewDecision,reviews,"
                  "statusCheckRollup,commits,additions,deletions",
    ], token)
    if not output:
        raise GhCliError(f"gh pr view returned empty output for PR #{number}")
    return json.loads(output)


def get_ci_status(repo: str, pr_number: int, token: str) -> list[dict]:
    """Get CI check runs for a PR.

    Returns list of dicts with: name, status, conclusion, detailsUrl.
    """
    pr = get_pr(repo, pr_number, token)
    checks = pr.get("statusCheckRollup") or []
    return checks


def get_pr_diff(repo: str, number: int, token: str) -> str:
    """Get the diff for a PR."""
    _validate_repo(repo)
    return _run_gh([
        "pr", "diff", str(number),
        "--repo", repo,
    ], token)


def get_check_run_logs(
    repo: str, run_id: int, token: str
) -> bytes:
    """Get logs for a specific check run.

    Uses gh api to fetch the logs. The GitHub API returns a zip file,
    so this returns raw bytes that the caller must unzip.
    """
    _validate_repo(repo)
    return _run_gh_bytes([
        "api",
        f"repos/{repo}/actions/runs/{run_id}/logs",
    ], token)


def add_label(repo: str, number: int, label: str, token: str) -> None:
    """Add a label to a PR/issue."""
    _validate_repo(repo)
    _run_gh([
        "pr", "edit", str(number),
        "--repo", repo,
        "--add-label", label,
    ], token)


def remove_label(repo: str, number: int, label: str, token: str) -> None:
    """Remove a label from a PR/issue. Silently succeeds if not present."""
    _validate_repo(repo)
    try:
        _run_gh([
            "pr", "edit", str(number),
            "--repo", repo,
            "--remove-label", label,
        ], token)
    except GhCliError as exc:
        # gh pr edit --remove-label exits non-zero when the label isn't
        # present. We swallow that specific case but re-raise others.
        if "not found" in (exc.stderr or "").lower():
            logger.debug(
                "remove_label: label %r not on PR #%d, ignoring",
                label, number,
            )
        else:
            raise


def post_comment(repo: str, number: int, body: str, token: str) -> None:
    """Post a comment on a PR/issue.

    Uses --body-file with a temp file to avoid argument length limits
    and to prevent any interpretation of special characters in the body.
    """
    _validate_repo(repo)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".md", delete=True) as f:
        f.write(body)
        f.flush()
        _run_gh([
            "pr", "comment", str(number),
            "--repo", repo,
            "--body-file", f.name,
        ], token)


def get_review_comments(repo: str, number: int, token: str) -> list[dict]:
    """Get review comments on a PR."""
    _validate_repo(repo)
    output = _run_gh([
        "api",
        f"repos/{repo}/pulls/{number}/reviews",
    ], token)
    return json.loads(output) if output else []
