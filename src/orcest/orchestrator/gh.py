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
import time
from typing import Any

logger = logging.getLogger(__name__)

_REPO_RE = re.compile(r"^[A-Za-z0-9._-]+/[A-Za-z0-9._-]+$")

_MAX_PAGES = 50  # safety cap; each page fetches up to 100 threads (50 × 100 = 5 000 total)


class GhCliError(Exception):
    """Raised when a gh CLI operation fails."""

    def __init__(self, message: str, stderr: str = "", returncode: int | None = None):
        super().__init__(message)
        self.stderr = stderr
        self.returncode = returncode


class GhRateLimitError(GhCliError):
    """Raised when the gh CLI is rate-limited by GitHub (HTTP 429)."""

    def __init__(
        self,
        message: str,
        stderr: str = "",
        returncode: int | None = None,
        retry_after: int | None = None,
    ):
        super().__init__(message, stderr=stderr, returncode=returncode)
        self.retry_after = retry_after


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


_GH_TIMEOUT_SECONDS = 120

# Exponential backoff delays (seconds) between rate-limit retries.
# The number of entries equals the number of retries attempted before giving up.
_RATE_LIMIT_BACKOFF_SECONDS: tuple[int, ...] = (30, 60, 120)

_RATE_LIMIT_RE = re.compile(r"rate.?limit", re.IGNORECASE)
_RATE_LIMIT_429_RE = re.compile(r"HTTP\s+429\b")
_RETRY_AFTER_RE = re.compile(r"retry.?after[:\s]+(\d+)", re.IGNORECASE)

# Maximum seconds to honour a server-supplied retry-after header; guards
# against misbehaving or adversarial responses with extreme values.
_MAX_RETRY_AFTER_SECONDS = 300


def _is_rate_limited(stderr: str) -> bool:
    """Return True if stderr indicates a GitHub rate-limit response."""
    return bool(_RATE_LIMIT_RE.search(stderr)) or bool(_RATE_LIMIT_429_RE.search(stderr))


def _extract_retry_after(stderr: str) -> int | None:
    """Extract retry-after duration in seconds from gh CLI stderr, if present."""
    m = _RETRY_AFTER_RE.search(stderr)
    return int(m.group(1)) if m else None


def _run_gh(args: list[str], token: str) -> str:
    """Execute a gh CLI command and return stdout.

    Sets both GITHUB_TOKEN and GH_TOKEN for compatibility across
    gh CLI versions.

    Retries up to len(_RATE_LIMIT_BACKOFF_SECONDS) times with exponential
    backoff when GitHub rate-limits the request.

    Raises:
        GhNotInstalledError: If the gh CLI binary is not on PATH.
        GhRateLimitError: When rate-limited and all retries are exhausted.
        GhCliError: On non-zero exit for any other error.
    """
    env = os.environ.copy()
    env["GITHUB_TOKEN"] = token
    env["GH_TOKEN"] = token  # gh CLI also checks GH_TOKEN

    for attempt in range(len(_RATE_LIMIT_BACKOFF_SECONDS) + 1):
        try:
            result = subprocess.run(
                ["gh", *args],
                capture_output=True,
                text=True,
                env=env,
                check=True,
                timeout=_GH_TIMEOUT_SECONDS,
            )
            return result.stdout.strip()
        except FileNotFoundError:
            raise GhNotInstalledError(
                "gh CLI not found on PATH. Install it from https://cli.github.com/"
            ) from None
        except subprocess.TimeoutExpired as exc:
            # Truncate args to avoid dumping entire GraphQL queries into logs
            brief = " ".join(args[:4])
            if len(args) > 4:
                brief += " ..."
            raise GhCliError(
                f"gh command timed out after {exc.timeout}s: gh {brief}",
            ) from exc
        except subprocess.CalledProcessError as exc:
            if _is_rate_limited(exc.stderr):
                retry_after_raw = _extract_retry_after(exc.stderr)
                retry_after = (
                    min(retry_after_raw, _MAX_RETRY_AFTER_SECONDS)
                    if retry_after_raw is not None
                    else None
                )
                if attempt < len(_RATE_LIMIT_BACKOFF_SECONDS):
                    wait = (
                        retry_after
                        if retry_after is not None
                        else _RATE_LIMIT_BACKOFF_SECONDS[attempt]
                    )
                    logger.warning(
                        "GitHub rate limit hit; retrying in %ds (attempt %d/%d)",
                        wait,
                        attempt + 1,
                        len(_RATE_LIMIT_BACKOFF_SECONDS),
                    )
                    time.sleep(wait)
                    continue
                raise GhRateLimitError(
                    f"gh command rate-limited (exit {exc.returncode}): {exc.stderr.strip()}",
                    stderr=exc.stderr,
                    returncode=exc.returncode,
                    retry_after=retry_after,
                ) from exc
            raise GhCliError(
                f"gh command failed (exit {exc.returncode}): {exc.stderr.strip()}",
                stderr=exc.stderr,
                returncode=exc.returncode,
            ) from exc

    # Unreachable: the loop always returns or raises before this point.
    raise AssertionError("unreachable")  # pragma: no cover


def list_open_prs(repo: str, token: str, limit: int = 100) -> list[dict]:
    """List all open PRs, sorted oldest first.

    Returns list of dicts with keys: number, title, headRefName,
    headRefOid, isDraft, createdAt, labels, reviewDecision,
    mergeable.

    Args:
        repo: Repository in 'owner/repo' format.
        token: GitHub token.
        limit: Maximum number of PRs to fetch. Defaults to 100.
    """
    _validate_repo(repo)
    output = _run_gh(
        [
            "pr",
            "list",
            "--repo",
            repo,
            "--state",
            "open",
            "--json",
            "number,title,headRefName,baseRefName,headRefOid,isDraft,createdAt,labels,reviewDecision,mergeable",
            "--limit",
            str(limit),
        ],
        token,
    )
    if not output:
        return []
    try:
        return json.loads(output)
    except json.JSONDecodeError as e:
        raise GhCliError(f"Failed to parse gh output as JSON: {e}") from e


def get_pr(repo: str, number: int, token: str) -> dict:
    """Get detailed PR info."""
    _validate_repo(repo)
    output = _run_gh(
        [
            "pr",
            "view",
            str(number),
            "--repo",
            repo,
            "--json",
            "number,title,body,headRefName,baseRefName,state,"
            "labels,reviewDecision,reviews,"
            "statusCheckRollup,commits,additions,deletions",
        ],
        token,
    )
    if not output:
        raise GhCliError(f"gh pr view returned empty output for PR #{number}")
    try:
        return json.loads(output)
    except json.JSONDecodeError as e:
        raise GhCliError(f"Failed to parse gh output as JSON: {e}") from e


def get_ci_status(repo: str, pr_number: int, token: str) -> list[dict]:
    """Get CI check runs for a PR.

    Returns list of dicts with: name, status, conclusion, detailsUrl.
    """
    _validate_repo(repo)
    output = _run_gh(
        [
            "pr",
            "view",
            str(pr_number),
            "--repo",
            repo,
            "--json",
            "statusCheckRollup",
        ],
        token,
    )
    if not output:
        return []
    try:
        data = json.loads(output)
    except json.JSONDecodeError as e:
        raise GhCliError(f"Failed to parse gh output as JSON: {e}") from e
    return data.get("statusCheckRollup") or []


def get_pr_diff(repo: str, number: int, token: str) -> str:
    """Get the diff for a PR."""
    _validate_repo(repo)
    return _run_gh(
        [
            "pr",
            "diff",
            str(number),
            "--repo",
            repo,
        ],
        token,
    )


def cancel_workflow(repo: str, run_id: int, token: str) -> None:
    """Cancel a GitHub Actions workflow run.

    Used before re-triggering a stale in-progress run, since ``gh run rerun``
    requires the run to be in a completed state.
    """
    _validate_repo(repo)
    _run_gh(
        [
            "run",
            "cancel",
            str(run_id),
            "--repo",
            repo,
        ],
        token,
    )


def rerun_workflow(repo: str, run_id: int, token: str, failed_only: bool = False) -> None:
    """Re-run a GitHub Actions workflow run.

    Used to re-trigger claude-review when it completed without submitting
    a formal review, and to re-trigger transient CI failures.

    Args:
        repo: Repository in 'owner/repo' format.
        run_id: The workflow run ID to re-run.
        token: GitHub token.
        failed_only: If True, only re-run failed jobs (``--failed`` flag).
            Defaults to False (re-runs all jobs).
    """
    _validate_repo(repo)
    args = ["run", "rerun", str(run_id), "--repo", repo]
    if failed_only:
        args.append("--failed")
    _run_gh(args, token)


def get_failed_run_logs(repo: str, run_id: int, token: str) -> str:
    """Get failed step logs for a GitHub Actions workflow run.

    Uses ``gh run view --log-failed`` which returns plain text output
    of only the failed steps.  This avoids downloading and unzipping
    the full log archive.

    Returns empty string on any failure -- log fetching should never
    block task creation.
    """
    _validate_repo(repo)
    try:
        return _run_gh(
            [
                "run",
                "view",
                str(run_id),
                "--repo",
                repo,
                "--log-failed",
            ],
            token,
        )
    except GhCliError:
        logger.warning(
            "Failed to fetch failed-step logs for run %d in %s",
            run_id,
            repo,
            exc_info=True,
        )
        return ""


def add_label(repo: str, number: int, label: str, token: str) -> None:
    """Add a label to a PR/issue via REST API.

    Uses the REST API instead of ``gh pr edit --add-label`` because the
    GraphQL mutation behind ``gh edit`` requires ``read:org`` scope on
    classic PATs.
    """
    _validate_repo(repo)
    _run_gh(
        ["api", f"repos/{repo}/issues/{number}/labels", "-f", f"labels[]={label}"],
        token,
    )


def remove_label(repo: str, number: int, label: str, token: str) -> None:
    """Remove a label from a PR/issue via REST API. Silently succeeds if not present."""
    _validate_repo(repo)
    try:
        _run_gh(
            ["api", f"repos/{repo}/issues/{number}/labels/{label}", "-X", "DELETE"],
            token,
        )
    except GhCliError as exc:
        if "not found" in (exc.stderr or "").lower():
            logger.debug(
                "remove_label: label %r not on #%d, ignoring",
                label,
                number,
            )
        else:
            raise


def post_comment(repo: str, number: int, body: str, token: str) -> None:
    """Post a comment on a PR or issue.

    Uses --body-file with a temp file to avoid argument length limits
    and to prevent any interpretation of special characters in the body.
    """
    _validate_repo(repo)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".md", delete=True) as f:
        f.write(body)
        f.flush()
        _run_gh(
            [
                "pr",
                "comment",
                str(number),
                "--repo",
                repo,
                "--body-file",
                f.name,
            ],
            token,
        )


_VALID_MERGE_METHODS = {"squash", "merge", "rebase"}


def merge_pr(
    repo: str,
    number: int,
    token: str,
    method: str = "squash",
    delete_branch: bool = True,
) -> None:
    """Merge a PR. Raises GhCliError on failure.

    Args:
        repo: Repository in 'owner/repo' format.
        number: PR number to merge.
        token: GitHub token.
        method: Merge method — one of 'squash', 'merge', or 'rebase'.
        delete_branch: Whether to delete the head branch after merging.
            Defaults to True. Set to False if branch protection rules
            prevent deletion or if you prefer to keep branches post-merge.
    """
    if method not in _VALID_MERGE_METHODS:
        raise ValueError(
            f"Invalid merge method: {method!r}. Must be one of {sorted(_VALID_MERGE_METHODS)}."
        )
    _validate_repo(repo)
    args = [
        "pr",
        "merge",
        str(number),
        "--repo",
        repo,
        f"--{method}",
    ]
    if delete_branch:
        args.append("--delete-branch")
    _run_gh(args, token)


def get_unresolved_review_threads(repo: str, number: int, token: str) -> list[dict]:
    """Get unresolved review threads on a PR.

    Uses the GitHub GraphQL API to fetch review threads and filters
    to only those that are not yet resolved.

    Returns list of dicts with keys: id, path, line, comments.
    Each comment dict has keys: author, body.
    """
    _validate_repo(repo)
    owner, name = repo.split("/", 1)

    query = """
query($owner: String!, $repo: String!, $number: Int!, $after: String) {
  repository(owner: $owner, name: $repo) {
    pullRequest(number: $number) {
      reviewThreads(first: 100, after: $after) {
        pageInfo { hasNextPage endCursor }
        nodes {
          id
          path
          line
          isResolved
          comments(first: 10) {
            pageInfo { hasNextPage }
            nodes {
              author { login }
              body
            }
          }
        }
      }
    }
  }
}
"""

    all_thread_nodes: list[dict] = []
    cursor: str | None = None
    page_count = 0
    review_threads: dict[str, Any] = {}

    while page_count < _MAX_PAGES:
        page_count += 1
        args = [
            "api",
            "graphql",
            "-f",
            f"owner={owner}",
            "-f",
            f"repo={name}",
            "-F",
            f"number={number}",
            "-f",
            f"query={query}",
        ]
        if cursor is not None:
            args.extend(["-f", f"after={cursor}"])

        output = _run_gh(args, token)

        if not output:
            raise GhCliError(f"GraphQL query returned empty response for PR #{number} in {repo}")

        try:
            data = json.loads(output)
        except json.JSONDecodeError as e:
            raise GhCliError(f"Failed to parse gh output as JSON: {e}") from e

        # GraphQL can return HTTP 200 with errors in the body. Raise so
        # callers don't mistake a failed query for "no threads" (which
        # could trigger an incorrect auto-merge).
        if "errors" in data:
            msgs = [e.get("message", str(e)) for e in data["errors"]]
            raise GhCliError(f"GraphQL errors fetching review threads: {'; '.join(msgs)}")

        repo_data = (data.get("data") or {}).get("repository")
        if not repo_data:
            raise GhCliError(
                f"GraphQL returned null repository for {repo!r} — "
                "check that the repo exists and the token has access"
            )
        pr_node = repo_data.get("pullRequest")
        if not pr_node:
            raise GhCliError(f"GraphQL returned null pullRequest for PR #{number} in {repo}")
        review_threads = pr_node.get("reviewThreads") or {}
        all_thread_nodes.extend(review_threads.get("nodes") or [])

        page_info = review_threads.get("pageInfo") or {}
        if page_info.get("hasNextPage"):
            if cursor is None:
                logger.warning(
                    "PR #%d in %s has more than 100 review threads; fetching additional pages",
                    number,
                    repo,
                )
            cursor = page_info.get("endCursor")
            if not cursor:
                # Safety guard: hasNextPage is True but no cursor returned.
                logger.warning(
                    "PR #%d in %s: hasNextPage is True but endCursor is missing; "
                    "stopping pagination with %d threads fetched so far",
                    number,
                    repo,
                    len(all_thread_nodes),
                )
                break
        else:
            break
    else:
        # Loop exhausted _MAX_PAGES without a natural break.
        page_info = review_threads.get("pageInfo") or {}
        if page_info.get("hasNextPage"):
            logger.warning(
                "PR #%d in %s: reached MAX_PAGES (%d) pagination limit; "
                "some review threads may have been truncated (%d fetched so far)",
                number,
                repo,
                _MAX_PAGES,
                len(all_thread_nodes),
            )

    results = []
    for thread in all_thread_nodes:
        if thread.get("isResolved"):
            continue
        comments_data = thread.get("comments") or {}
        comment_page = comments_data.get("pageInfo") or {}
        if comment_page.get("hasNextPage"):
            thread_id = thread.get("id", "<unknown>")
            logger.warning(
                "Review thread %s has more than 10 comments; later comments were not fetched",
                thread_id,
            )
        comments = []
        for comment in comments_data.get("nodes") or []:
            author_info = comment.get("author") or {}
            comments.append(
                {
                    "author": author_info.get("login", ""),
                    "body": comment.get("body", ""),
                }
            )
        results.append(
            {
                "id": thread.get("id", ""),
                "path": thread.get("path", ""),
                "line": thread.get("line"),
                "comments": comments,
            }
        )

    return results


def list_labeled_issues(repo: str, label: str, token: str) -> list[dict]:
    """List open issues with a specific label.

    Returns list of dicts with keys: number, title, body, labels.
    """
    _validate_repo(repo)
    output = _run_gh(
        [
            "issue",
            "list",
            "--repo",
            repo,
            "--label",
            label,
            "--state",
            "open",
            "--json",
            "number,title,body,labels",
            "--limit",
            "100",
        ],
        token,
    )
    if not output:
        return []
    try:
        return json.loads(output)
    except json.JSONDecodeError as e:
        raise GhCliError(f"Failed to parse gh output as JSON: {e}") from e


def get_issue(repo: str, number: int, token: str) -> dict:
    """Get detailed issue info."""
    _validate_repo(repo)
    output = _run_gh(
        [
            "issue",
            "view",
            str(number),
            "--repo",
            repo,
            "--json",
            "number,title,body,labels,assignees",
        ],
        token,
    )
    if not output:
        raise GhCliError(f"gh issue view returned empty output for issue #{number}")
    try:
        return json.loads(output)
    except json.JSONDecodeError as e:
        raise GhCliError(f"Failed to parse gh output as JSON: {e}") from e


def add_issue_label(repo: str, number: int, label: str, token: str) -> None:
    """Add a label to an issue via REST API."""
    _validate_repo(repo)
    _run_gh(
        ["api", f"repos/{repo}/issues/{number}/labels", "-f", f"labels[]={label}"],
        token,
    )


def remove_issue_label(repo: str, number: int, label: str, token: str) -> None:
    """Remove a label from an issue via REST API. Silently succeeds if not present."""
    _validate_repo(repo)
    try:
        _run_gh(
            ["api", f"repos/{repo}/issues/{number}/labels/{label}", "-X", "DELETE"],
            token,
        )
    except GhCliError as exc:
        if "not found" in (exc.stderr or "").lower():
            logger.debug(
                "remove_issue_label: label %r not on issue #%d, ignoring",
                label,
                number,
            )
        else:
            raise


def post_issue_comment(repo: str, number: int, body: str, token: str) -> None:
    """Post a comment on an issue.

    Uses --body-file with a temp file to avoid argument length limits.
    """
    _validate_repo(repo)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".md", delete=True) as f:
        f.write(body)
        f.flush()
        _run_gh(
            [
                "issue",
                "comment",
                str(number),
                "--repo",
                repo,
                "--body-file",
                f.name,
            ],
            token,
        )


def create_issue(
    repo: str,
    title: str,
    body: str,
    token: str,
    labels: list[str] | None = None,
) -> int:
    """Create a new GitHub issue and return its number.

    Args:
        repo: Repository in 'owner/repo' format.
        title: Issue title.
        body: Issue body (Markdown).
        token: GitHub token.
        labels: Optional list of label names to apply.

    Returns:
        The newly created issue number.
    """
    _validate_repo(repo)
    args = [
        "issue",
        "create",
        "--repo",
        repo,
        "--title",
        title,
        "--body",
        body,
    ]
    for label in labels or []:
        args.extend(["--label", label])
    output = _run_gh(args, token)
    # gh issue create returns the issue URL; extract the issue number from it.
    # Use the first line that looks like an issue URL — gh may emit trailing
    # warnings or deprecation notices after the URL.
    for line in output.strip().splitlines():
        line = line.strip()
        if "/issues/" in line:
            try:
                return int(line.rstrip("/").rsplit("/", 1)[-1])
            except (ValueError, IndexError):
                continue
    raise GhCliError(f"Could not parse issue number from gh output: {output.strip()!r}")


def resolve_review_thread(thread_id: str, token: str) -> None:
    """Resolve a review thread on a PR.

    Uses the GitHub GraphQL API resolveReviewThread mutation.
    The thread_id should be the GraphQL node ID from
    get_unresolved_review_threads.
    """
    if not isinstance(thread_id, str) or not thread_id:
        raise ValueError(f"thread_id must be a non-empty string, got {thread_id!r}")
    mutation = """
mutation($threadId: ID!) {
  resolveReviewThread(input: {threadId: $threadId}) {
    thread {
      id
      isResolved
    }
  }
}
"""

    output = _run_gh(
        [
            "api",
            "graphql",
            "-f",
            f"threadId={thread_id}",
            "-f",
            f"query={mutation}",
        ],
        token,
    )

    if not output:
        raise GhCliError(f"GraphQL mutation returned empty response for thread {thread_id!r}")

    try:
        data = json.loads(output)
    except json.JSONDecodeError as e:
        raise GhCliError(f"Failed to parse gh output as JSON: {e}") from e
    if "errors" in data:
        msgs = [e.get("message", str(e)) for e in data["errors"]]
        raise GhCliError(f"GraphQL errors resolving review thread: {'; '.join(msgs)}")
