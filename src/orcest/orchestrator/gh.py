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

_MAX_PAGES = 50  # safety cap; each page fetches up to 100 threads (50 × 100 = 5 000 total)


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


_GH_TIMEOUT_SECONDS = 120


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
            timeout=_GH_TIMEOUT_SECONDS,
        )
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
        raise GhCliError(
            f"gh command failed (exit {exc.returncode}): {exc.stderr.strip()}",
            stderr=exc.stderr,
            returncode=exc.returncode,
        ) from exc

    return result.stdout.strip()


def list_open_prs(repo: str, token: str, limit: int = 100) -> list[dict]:
    """List all open PRs, sorted oldest first.

    Returns list of dicts with keys: number, title, headRefName,
    headRefOid, isDraft, author, createdAt, labels, reviewDecision,
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
            "number,title,headRefName,baseRefName,headRefOid,isDraft,author,createdAt,labels,reviewDecision,mergeable",
            "--limit",
            str(limit),
        ],
        token,
    )
    return json.loads(output) if output else []


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
            "author,labels,reviewDecision,reviews,"
            "statusCheckRollup,commits,additions,deletions",
        ],
        token,
    )
    if not output:
        raise GhCliError(f"gh pr view returned empty output for PR #{number}")
    return json.loads(output)


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
    data = json.loads(output)
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


def rerun_workflow(repo: str, run_id: int, token: str) -> None:
    """Re-run a GitHub Actions workflow run.

    Used to re-trigger claude-review when it completed without submitting
    a formal review.
    """
    _validate_repo(repo)
    _run_gh(
        [
            "run",
            "rerun",
            str(run_id),
            "--repo",
            repo,
        ],
        token,
    )


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
    except Exception:
        logger.warning(
            "Failed to fetch failed-step logs for run %d in %s",
            run_id,
            repo,
            exc_info=True,
        )
        return ""


def add_label(repo: str, number: int, label: str, token: str) -> None:
    """Add a label to a PR/issue."""
    _validate_repo(repo)
    _run_gh(
        [
            "pr",
            "edit",
            str(number),
            "--repo",
            repo,
            "--add-label",
            label,
        ],
        token,
    )


def remove_label(repo: str, number: int, label: str, token: str) -> None:
    """Remove a label from a PR/issue. Silently succeeds if not present."""
    _validate_repo(repo)
    try:
        _run_gh(
            [
                "pr",
                "edit",
                str(number),
                "--repo",
                repo,
                "--remove-label",
                label,
            ],
            token,
        )
    except GhCliError as exc:
        # gh pr edit --remove-label exits non-zero when the label isn't
        # present. We swallow that specific case but re-raise others.
        if "not found" in (exc.stderr or "").lower():
            logger.debug(
                "remove_label: label %r not on PR #%d, ignoring",
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
              body
              author {
                login
              }
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

        data = json.loads(output)

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
        page_info = review_threads.get("pageInfo") or {}  # type: ignore[possibly-undefined]
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


def get_pr_review_comments(repo: str, number: int, token: str) -> list[dict]:
    """Get inline review comments for a PR.

    Uses the /pulls/{number}/comments REST endpoint to fetch line-specific
    review comments left by reviewers. Unlike get_unresolved_review_threads,
    this returns all inline comments (not just unresolved ones) and uses the
    REST API rather than GraphQL.

    Fetches all pages of results using --paginate.

    Returns list of dicts with keys: path, line, author, body.
    """
    _validate_repo(repo)
    output = _run_gh(
        [
            "api",
            "--method",
            "GET",
            "--paginate",
            f"repos/{repo}/pulls/{number}/comments",
            "-F",
            "per_page=100",  # fetch 100 per page for efficiency
        ],
        token,
    )
    if not output:
        return []
    # --paginate concatenates one JSON array per page: [page1][page2]...
    # Parse all pages and flatten into a single list.
    comments: list[dict] = []
    decoder = json.JSONDecoder()
    idx = 0
    text = output.strip()
    while idx < len(text):
        page, end_idx = decoder.raw_decode(text, idx)
        comments.extend(page)
        idx = end_idx
        while idx < len(text) and text[idx].isspace():
            idx += 1
    results = []
    for comment in comments:
        line = comment.get("line")
        results.append(
            {
                "path": comment.get("path", ""),
                "line": line if line is not None else comment.get("original_line"),
                "author": (comment.get("user") or {}).get("login", ""),
                "body": comment.get("body", ""),
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
    return json.loads(output) if output else []


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
    return json.loads(output)


def add_issue_label(repo: str, number: int, label: str, token: str) -> None:
    """Add a label to an issue."""
    _validate_repo(repo)
    _run_gh(
        [
            "issue",
            "edit",
            str(number),
            "--repo",
            repo,
            "--add-label",
            label,
        ],
        token,
    )


def remove_issue_label(repo: str, number: int, label: str, token: str) -> None:
    """Remove a label from an issue. Silently succeeds if not present."""
    _validate_repo(repo)
    try:
        _run_gh(
            [
                "issue",
                "edit",
                str(number),
                "--repo",
                repo,
                "--remove-label",
                label,
            ],
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

    data = json.loads(output)
    if "errors" in data:
        msgs = [e.get("message", str(e)) for e in data["errors"]]
        raise GhCliError(f"GraphQL errors resolving review thread: {'; '.join(msgs)}")
