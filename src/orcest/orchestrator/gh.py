"""GitHub CLI wrapper for orchestrator operations.

All GitHub interaction goes through the `gh` CLI. No direct API calls.
Every function takes `token` as a parameter rather than reading it from
the environment, making testing straightforward and keeping the dependency
explicit.
"""

import json
import os
import subprocess


def _run_gh(args: list[str], token: str) -> str:
    """Execute a gh CLI command and return stdout.

    Sets both GITHUB_TOKEN and GH_TOKEN for compatibility across
    gh CLI versions.

    Raises:
        subprocess.CalledProcessError: On non-zero exit.
    """
    env = os.environ.copy()
    env["GITHUB_TOKEN"] = token
    env["GH_TOKEN"] = token  # gh CLI also checks GH_TOKEN

    result = subprocess.run(
        ["gh"] + args,
        capture_output=True,
        text=True,
        env=env,
        check=True,
    )
    return result.stdout.strip()


def list_open_prs(repo: str, token: str) -> list[dict]:
    """List all open PRs, sorted oldest first.

    Returns list of dicts with keys: number, title, headRefName,
    author, createdAt, labels, reviewDecision.
    """
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
    output = _run_gh([
        "pr", "view", str(number),
        "--repo", repo,
        "--json", "number,title,body,headRefName,baseRefName,state,"
                  "author,labels,reviewDecision,reviews,"
                  "statusCheckRollup,commits,additions,deletions",
    ], token)
    return json.loads(output)


def get_ci_status(repo: str, pr_number: int, token: str) -> list[dict]:
    """Get CI check runs for a PR.

    Returns list of dicts with: name, status, conclusion, detailsUrl.
    """
    pr = get_pr(repo, pr_number, token)
    checks = pr.get("statusCheckRollup", [])
    return checks


def get_pr_diff(repo: str, number: int, token: str) -> str:
    """Get the diff for a PR."""
    return _run_gh([
        "pr", "diff", str(number),
        "--repo", repo,
    ], token)


def get_check_run_logs(
    repo: str, run_id: int, token: str
) -> str:
    """Get logs for a specific check run.

    Uses gh api to fetch the logs URL, then downloads.
    """
    output = _run_gh([
        "api",
        f"repos/{repo}/actions/runs/{run_id}/logs",
    ], token)
    return output


def add_label(repo: str, number: int, label: str, token: str) -> None:
    """Add a label to a PR/issue."""
    _run_gh([
        "pr", "edit", str(number),
        "--repo", repo,
        "--add-label", label,
    ], token)


def remove_label(repo: str, number: int, label: str, token: str) -> None:
    """Remove a label from a PR/issue. Silently succeeds if not present."""
    try:
        _run_gh([
            "pr", "edit", str(number),
            "--repo", repo,
            "--remove-label", label,
        ], token)
    except subprocess.CalledProcessError:
        pass  # Label wasn't present


def post_comment(repo: str, number: int, body: str, token: str) -> None:
    """Post a comment on a PR/issue."""
    _run_gh([
        "pr", "comment", str(number),
        "--repo", repo,
        "--body", body,
    ], token)


def get_review_comments(repo: str, number: int, token: str) -> list[dict]:
    """Get review comments on a PR."""
    output = _run_gh([
        "api",
        f"repos/{repo}/pulls/{number}/reviews",
    ], token)
    return json.loads(output) if output else []
