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
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

_REPO_RE = re.compile(r"^[A-Za-z0-9._-]+/[A-Za-z0-9._-]+$")
_LABEL_ALREADY_ABSENT_RE = re.compile(
    r"(?:gh:\s*)?label(?:\s+(?:'[^'\r\n]+'|\"[^\"\r\n]+\"))?\s+"
    r"(?:does not exist|not found)(?:\s+\(http\s+404\))?",
    re.IGNORECASE,
)

_MAX_PAGES = 50  # safety cap; each page fetches up to 100 threads (50 × 100 = 5 000 total)


class GhCliError(Exception):
    """Raised when a gh CLI operation fails."""

    def __init__(self, message: str, stderr: str = "", returncode: int | None = None):
        super().__init__(message)
        self.stderr = stderr
        self.returncode = returncode


class GhStaleSnapshotError(GhCliError):
    """GitHub data no longer matches the PR snapshot being evaluated."""


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


def _label_already_absent(stderr: str | None) -> bool:
    """True when `stderr` means a label-removal target was already gone.

    Covers gh's current REST error for a missing label
    (``gh: Label does not exist (HTTP 404)``) and narrowly anchored older
    phrasings that name the label (``label 'x' not found``). A response with
    an HTTP status is idempotent only when that status is 404. Additional
    lines or context are rejected so permission, authentication, repository,
    and server failures cannot be hidden by incidental label wording.
    """
    return _LABEL_ALREADY_ABSENT_RE.fullmatch((stderr or "").strip()) is not None


@dataclass(frozen=True)
class PRReviewSnapshot:
    """Strictly validated review evidence for one PR head."""

    head_sha: str
    state: str
    is_draft: bool
    labels: tuple[str, ...]
    review_decision: str
    has_current_head_approval: bool


def _validate_repo(repo: str) -> None:
    """Validate that repo matches the expected 'owner/repo' format.

    Raises ValueError if the format is invalid.
    """
    if not _REPO_RE.match(repo):
        raise ValueError(
            f"Invalid repo format: {repo!r}. Expected 'owner/repo' with "
            "alphanumeric characters, hyphens, underscores, and dots only."
        )


def _require_nonempty_sha(value: str | None, *, field: str = "head SHA") -> str:
    if not isinstance(value, str) or not value:
        raise GhCliError(f"Missing or empty {field}")
    return value


def _parse_json_object(output: str, *, context: str) -> dict[str, Any]:
    if not output:
        raise GhCliError(f"{context} returned empty response")
    try:
        data = json.loads(output)
    except json.JSONDecodeError as e:
        raise GhCliError(f"Failed to parse gh output as JSON: {e}") from e
    if not isinstance(data, dict):
        raise GhCliError(f"{context} returned non-object JSON")
    if "errors" in data:
        errors = data["errors"]
        if isinstance(errors, list):
            msgs = [e.get("message", str(e)) if isinstance(e, dict) else str(e) for e in errors]
        else:
            msgs = [str(errors)]
        raise GhCliError(f"{context} returned GraphQL errors: {'; '.join(msgs)}")
    return data


def _require_object(value: Any, *, field: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise GhCliError(f"Malformed GitHub response: {field} must be an object")
    return value


def _require_list(value: Any, *, field: str) -> list[Any]:
    if not isinstance(value, list):
        raise GhCliError(f"Malformed GitHub response: {field} must be a list")
    return value


def _require_bool(value: Any, *, field: str) -> bool:
    if not isinstance(value, bool):
        raise GhCliError(f"Malformed GitHub response: {field} must be a boolean")
    return value


def _require_str(value: Any, *, field: str, allow_empty: bool = True) -> str:
    if not isinstance(value, str) or (not allow_empty and not value):
        raise GhCliError(f"Malformed GitHub response: {field} must be a string")
    return value


def _pull_request_from_graphql(data: dict[str, Any], *, repo: str, number: int) -> dict[str, Any]:
    root_data = data.get("data")
    if root_data is None:
        raise GhCliError(
            f"GraphQL returned null repository for {repo!r} -- "
            "check that the repo exists and the token has access"
        )
    root = _require_object(root_data, field="data")
    repo_data = root.get("repository")
    if repo_data is None:
        raise GhCliError(
            f"GraphQL returned null repository for {repo!r} -- "
            "check that the repo exists and the token has access"
        )
    repository = _require_object(repo_data, field="data.repository")
    pr_node = repository.get("pullRequest")
    if pr_node is None:
        raise GhCliError(f"GraphQL returned null pullRequest for PR #{number} in {repo}")
    return _require_object(pr_node, field="data.repository.pullRequest")


def _require_connection(
    value: Any, *, field: str
) -> tuple[dict[str, Any], list[Any], dict[str, Any]]:
    connection = _require_object(value, field=field)
    page_info = _require_object(connection.get("pageInfo"), field=f"{field}.pageInfo")
    nodes = _require_list(connection.get("nodes"), field=f"{field}.nodes")
    _require_bool(page_info.get("hasNextPage"), field=f"{field}.pageInfo.hasNextPage")
    return connection, nodes, page_info


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
    """List open PRs, sorted oldest first.

    Returns list of dicts with keys: number, title, headRefName,
    baseRefName, headRefOid, isDraft, createdAt, labels, reviewDecision,
    mergeable, mergeStateStatus.

    Args:
        repo: Repository in 'owner/repo' format.
        token: GitHub token.
        limit: Maximum number of PRs to fetch. Defaults to 100. Values above
            100 are fetched with GraphQL cursor pagination.
    """
    _validate_repo(repo)
    if limit < 1:
        return []

    owner, name = repo.split("/", 1)
    query = """
query($owner: String!, $repo: String!, $first: Int!, $after: String) {
  repository(owner: $owner, name: $repo) {
    pullRequests(
      states: OPEN
      first: $first
      after: $after
      orderBy: {field: CREATED_AT, direction: ASC}
    ) {
      pageInfo { hasNextPage endCursor }
      nodes {
        number
        title
        headRefName
        baseRefName
        headRefOid
        isDraft
        createdAt
        labels(first: 100) {
          nodes {
            id
            name
            description
            color
          }
        }
        reviewDecision
        mergeable
        mergeStateStatus
      }
    }
  }
}
"""

    prs: list[dict] = []
    cursor: str | None = None
    page_count = 0
    pull_requests: dict[str, Any] = {}

    while len(prs) < limit and page_count < _MAX_PAGES:
        page_count += 1
        page_size = min(100, limit - len(prs))
        args = [
            "api",
            "graphql",
            "-f",
            f"owner={owner}",
            "-f",
            f"repo={name}",
            "-F",
            f"first={page_size}",
            "-f",
            f"query={query}",
        ]
        if cursor is not None:
            args.extend(["-f", f"after={cursor}"])

        output = _run_gh(args, token)
        if not output:
            return prs
        try:
            data = json.loads(output)
        except json.JSONDecodeError as e:
            raise GhCliError(f"Failed to parse gh output as JSON: {e}") from e
        if isinstance(data, list):
            return data

        if "errors" in data:
            msgs = [e.get("message", str(e)) for e in data["errors"]]
            raise GhCliError(f"GraphQL errors fetching open PRs: {'; '.join(msgs)}")

        repo_data = (data.get("data") or {}).get("repository")
        if not repo_data:
            raise GhCliError(
                f"GraphQL returned null repository for {repo!r} — "
                "check that the repo exists and the token has access"
            )
        pull_requests = repo_data.get("pullRequests") or {}
        for pr in pull_requests.get("nodes") or []:
            labels = (pr.get("labels") or {}).get("nodes") or []
            normalized = dict(pr)
            normalized["labels"] = labels
            prs.append(normalized)

        page_info = pull_requests.get("pageInfo") or {}
        if page_info.get("hasNextPage") and len(prs) < limit:
            cursor = page_info.get("endCursor")
            if not cursor:
                logger.warning(
                    "Open PR pagination for %s had hasNextPage=True but no endCursor; "
                    "stopping with %d PRs fetched",
                    repo,
                    len(prs),
                )
                break
        else:
            break
    else:
        page_info = pull_requests.get("pageInfo") or {}
        if page_info.get("hasNextPage") and len(prs) < limit:
            logger.warning(
                "Open PR listing for %s reached MAX_PAGES (%d); "
                "some PRs may have been truncated (%d fetched so far)",
                repo,
                _MAX_PAGES,
                len(prs),
            )

    return prs


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
            "number,title,body,headRefName,headRefOid,baseRefName,state,"
            "labels,reviewDecision,reviews,"
            "statusCheckRollup,commits,additions,deletions,"
            "mergeable,mergeStateStatus",
        ],
        token,
    )
    if not output:
        raise GhCliError(f"gh pr view returned empty output for PR #{number}")
    try:
        return json.loads(output)
    except json.JSONDecodeError as e:
        raise GhCliError(f"Failed to parse gh output as JSON: {e}") from e


def get_ci_status(
    repo: str,
    pr_number: int,
    token: str,
    *,
    expected_head_sha: str | None = None,
) -> list[dict]:
    """Get CI check runs for a PR.

    Returns list of dicts with: name, status, conclusion, detailsUrl.
    """
    _validate_repo(repo)
    json_fields = "statusCheckRollup"
    if expected_head_sha is not None:
        _require_nonempty_sha(expected_head_sha, field="expected_head_sha")
        json_fields = "headRefOid,statusCheckRollup"

    output = _run_gh(
        [
            "pr",
            "view",
            str(pr_number),
            "--repo",
            repo,
            "--json",
            json_fields,
        ],
        token,
    )
    if not output and expected_head_sha is None:
        return []
    data = _parse_json_object(output, context=f"gh pr view CI status for PR #{pr_number}")
    if expected_head_sha is not None:
        head_sha = _require_nonempty_sha(data.get("headRefOid"), field="headRefOid")
        if head_sha != expected_head_sha:
            raise GhStaleSnapshotError(
                f"PR #{pr_number} head changed while fetching CI status "
                f"(expected {expected_head_sha}, got {head_sha})"
            )
    checks = data.get("statusCheckRollup") or []
    if not isinstance(checks, list):
        raise GhCliError("Malformed GitHub response: statusCheckRollup must be a list")
    return checks


def get_review_snapshot(
    repo: str,
    number: int,
    token: str,
    *,
    expected_head_sha: str,
) -> PRReviewSnapshot:
    """Fetch current-head review evidence with strict pagination validation."""
    _validate_repo(repo)
    expected_head_sha = _require_nonempty_sha(expected_head_sha, field="expected_head_sha")
    owner, name = repo.split("/", 1)

    query = """
query($owner: String!, $repo: String!, $number: Int!, $after: String) {
  repository(owner: $owner, name: $repo) {
    pullRequest(number: $number) {
      headRefOid
      state
      isDraft
      reviewDecision
      labels(first: 100) {
        pageInfo { hasNextPage }
        nodes { name }
      }
      latestOpinionatedReviews(first: 100, after: $after, writersOnly: true) {
        pageInfo { hasNextPage endCursor }
        nodes {
          state
          author { login }
          authorCanPushToRepository
          commit { oid }
        }
      }
    }
  }
}
"""

    cursor: str | None = None
    page_count = 0
    baseline: tuple[str, bool, tuple[str, ...], str] | None = None

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
        data = _parse_json_object(output, context=f"review snapshot for PR #{number}")
        pr_node = _pull_request_from_graphql(data, repo=repo, number=number)
        head_sha = _require_nonempty_sha(pr_node.get("headRefOid"), field="headRefOid")
        if head_sha != expected_head_sha:
            raise GhStaleSnapshotError(
                f"PR #{number} head changed while fetching review snapshot "
                f"(expected {expected_head_sha}, got {head_sha})"
            )
        state = _require_str(pr_node.get("state"), field="pullRequest.state", allow_empty=False)
        is_draft = _require_bool(pr_node.get("isDraft"), field="pullRequest.isDraft")
        if "reviewDecision" not in pr_node:
            raise GhCliError("Malformed GitHub response: pullRequest.reviewDecision is missing")
        review_decision_raw = pr_node.get("reviewDecision")
        if review_decision_raw is None:
            review_decision = ""
        else:
            review_decision = _require_str(review_decision_raw, field="pullRequest.reviewDecision")

        _labels_conn, label_nodes, label_page = _require_connection(
            pr_node.get("labels"), field="pullRequest.labels"
        )
        if label_page.get("hasNextPage"):
            raise GhCliError(f"PR #{number} in {repo} has more than 100 labels")
        labels: list[str] = []
        for label in label_nodes:
            label_node = _require_object(label, field="pullRequest.labels.nodes[]")
            labels.append(_require_str(label_node.get("name"), field="label.name"))
        label_tuple = tuple(labels)

        current = (state, is_draft, label_tuple, review_decision)
        if baseline is None:
            baseline = current
        elif current != baseline:
            raise GhStaleSnapshotError(
                f"PR #{number} review snapshot metadata changed during pagination"
            )

        _reviews_conn, review_nodes, page_info = _require_connection(
            pr_node.get("latestOpinionatedReviews"),
            field="pullRequest.latestOpinionatedReviews",
        )
        for review in review_nodes:
            review_node = _require_object(
                review, field="pullRequest.latestOpinionatedReviews.nodes[]"
            )
            review_state = _require_str(review_node.get("state"), field="review.state")
            author_can_push = _require_bool(
                review_node.get("authorCanPushToRepository"),
                field="review.authorCanPushToRepository",
            )
            if "commit" not in review_node:
                raise GhCliError("Malformed GitHub response: review.commit is missing")
            commit = review_node.get("commit")
            commit_oid = ""
            if commit is not None:
                commit_obj = _require_object(commit, field="review.commit")
                commit_oid = _require_str(commit_obj.get("oid"), field="review.commit.oid")
            if review_state == "APPROVED" and author_can_push and commit_oid == expected_head_sha:
                assert baseline is not None
                return PRReviewSnapshot(
                    head_sha=head_sha,
                    state=baseline[0],
                    is_draft=baseline[1],
                    labels=baseline[2],
                    review_decision=baseline[3],
                    has_current_head_approval=True,
                )

        if page_info.get("hasNextPage"):
            cursor_value = page_info.get("endCursor")
            if not isinstance(cursor_value, str) or not cursor_value:
                raise GhCliError(f"PR #{number} review snapshot pagination missing endCursor")
            cursor = cursor_value
            continue

        assert baseline is not None
        return PRReviewSnapshot(
            head_sha=head_sha,
            state=baseline[0],
            is_draft=baseline[1],
            labels=baseline[2],
            review_decision=baseline[3],
            has_current_head_approval=False,
        )

    raise GhCliError(
        f"PR #{number} review snapshot reached MAX_PAGES ({_MAX_PAGES}) before completion"
    )


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


def update_branch(
    repo: str,
    number: int,
    token: str,
    expected_head_sha: str | None = None,
) -> bool:
    """Update a PR's branch from its base via the GitHub REST API.

    Equivalent to clicking the "Update branch" button in the PR UI: GitHub
    merges the base branch into the PR branch with a default merge commit.
    Returns True once GitHub accepts the request (the actual merge runs async
    on GitHub's side; the next poll cycle will see the new head SHA).

    Used for PRs that branch protection holds back because the head is not
    up to date with base (mergeStateStatus is BEHIND, or BLOCKED when an
    "up to date" rule is the blocker). When the branch is already current,
    GitHub returns 422 "no new commits on the base branch" — that's a
    no-op signal, not a failure, so False is returned. Conflicting PRs need
    a worker rebase and go through ENQUEUE_REBASE instead.
    """
    _validate_repo(repo)
    args = [
        "api",
        f"repos/{repo}/pulls/{number}/update-branch",
        "-X",
        "PUT",
    ]
    if expected_head_sha:
        args.extend(["-f", f"expected_head_sha={expected_head_sha}"])
    try:
        _run_gh(args, token)
        return True
    except GhCliError as exc:
        if "no new commits on the base branch" in (exc.stderr or "").lower():
            logger.debug(
                "update_branch: PR #%d already up to date with base, no-op",
                number,
            )
            return False
        raise


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
        if _label_already_absent(exc.stderr):
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


def has_issue_comment_marker(repo: str, number: int, marker: str, token: str) -> bool:
    """Return whether any PR/issue conversation comment contains *marker*.

    Pull-request conversation comments use the issues comments REST endpoint,
    so this provides one idempotency check for both resource types. ``--paginate``
    prevents an older result comment from falling off the first page.
    """
    _validate_repo(repo)
    output = _run_gh(
        [
            "api",
            f"repos/{repo}/issues/{number}/comments",
            "--paginate",
            "--jq",
            ".[].body",
        ],
        token,
    )
    return marker in output


_VALID_MERGE_METHODS = {"squash", "merge", "rebase"}


def merge_pr(
    repo: str,
    number: int,
    token: str,
    method: str = "squash",
    delete_branch: bool = True,
    head_sha: str | None = None,
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
        head_sha: Expected PR head SHA. GitHub rejects the merge if the branch
            head changed after discovery.
    """
    if method not in _VALID_MERGE_METHODS:
        raise ValueError(
            f"Invalid merge method: {method!r}. Must be one of {sorted(_VALID_MERGE_METHODS)}."
        )
    _validate_repo(repo)
    head_sha = _require_nonempty_sha(head_sha, field="head_sha")
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
    args.extend(["--match-head-commit", head_sha])
    _run_gh(args, token)


def get_unresolved_review_threads(
    repo: str,
    number: int,
    token: str,
    *,
    expected_head_sha: str | None = None,
) -> list[dict]:
    """Get unresolved review threads on a PR.

    Uses the GitHub GraphQL API to fetch review threads and filters
    to only those that are not yet resolved.

    Returns list of dicts with keys: id, path, line, comments.
    Each comment dict has keys: id, author, body, createdAt, updatedAt.
    """
    _validate_repo(repo)
    if expected_head_sha is not None:
        expected_head_sha = _require_nonempty_sha(expected_head_sha, field="expected_head_sha")
    owner, name = repo.split("/", 1)

    query = """
query($owner: String!, $repo: String!, $number: Int!, $after: String) {
  repository(owner: $owner, name: $repo) {
    pullRequest(number: $number) {
      headRefOid
      reviewThreads(first: 100, after: $after) {
        pageInfo { hasNextPage endCursor }
        nodes {
          id
          path
          line
          isResolved
          comments(last: 100) {
            pageInfo { hasPreviousPage }
            nodes {
              id
              author { login }
              body
              createdAt
              updatedAt
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
        data = _parse_json_object(output, context=f"review threads for PR #{number}")
        pr_node = _pull_request_from_graphql(data, repo=repo, number=number)
        if expected_head_sha is not None:
            head_sha = _require_nonempty_sha(pr_node.get("headRefOid"), field="headRefOid")
            if head_sha != expected_head_sha:
                raise GhStaleSnapshotError(
                    f"PR #{number} head changed while fetching review threads "
                    f"(expected {expected_head_sha}, got {head_sha})"
                )
        review_threads, nodes, page_info = _require_connection(
            pr_node.get("reviewThreads"), field="pullRequest.reviewThreads"
        )
        for node in nodes:
            thread_node = _require_object(node, field="pullRequest.reviewThreads.nodes[]")
            _require_bool(thread_node.get("isResolved"), field="reviewThread.isResolved")
            all_thread_nodes.append(thread_node)

        if page_info.get("hasNextPage"):
            if cursor is None:
                logger.warning(
                    "PR #%d in %s has more than 100 review threads; fetching additional pages",
                    number,
                    repo,
                )
            cursor = page_info.get("endCursor")
            if not isinstance(cursor, str) or not cursor:
                raise GhCliError(f"PR #{number} review thread pagination missing endCursor")
        else:
            break
    else:
        # Loop exhausted _MAX_PAGES without a natural break.
        page_info = review_threads.get("pageInfo") or {}
        if page_info.get("hasNextPage"):
            raise GhCliError(
                f"PR #{number} review threads reached MAX_PAGES ({_MAX_PAGES}) before completion"
            )

    results = []
    for thread in all_thread_nodes:
        if thread.get("isResolved"):
            continue
        raw_comments_data = thread.get("comments")
        if raw_comments_data is None:
            comments_data: dict[str, Any] = {}
        elif isinstance(raw_comments_data, dict):
            comments_data = raw_comments_data
        else:
            raise GhCliError("Malformed GitHub response: reviewThread.comments must be an object")
        comment_page = comments_data.get("pageInfo") or {}
        if comment_page and not isinstance(comment_page, dict):
            raise GhCliError(
                "Malformed GitHub response: reviewThread.comments.pageInfo must be an object"
            )
        if comment_page.get("hasPreviousPage"):
            thread_id = thread.get("id", "<unknown>")
            logger.warning(
                "Review thread %s has more than 100 comments; oldest comments were not fetched",
                thread_id,
            )
        comments = []
        comment_nodes = comments_data.get("nodes") or []
        if not isinstance(comment_nodes, list):
            raise GhCliError(
                "Malformed GitHub response: reviewThread.comments.nodes must be a list"
            )
        for comment in comment_nodes:
            if not isinstance(comment, dict):
                raise GhCliError(
                    "Malformed GitHub response: reviewThread.comments.nodes[] must be an object"
                )
            author_info = comment.get("author") or {}
            if not isinstance(author_info, dict):
                raise GhCliError("Malformed GitHub response: comment.author must be an object")
            comments.append(
                {
                    "id": comment.get("id", ""),
                    "author": author_info.get("login", ""),
                    "body": comment.get("body", ""),
                    "createdAt": comment.get("createdAt", ""),
                    "updatedAt": comment.get("updatedAt", ""),
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


def list_labeled_issues(repo: str, label: str, token: str, limit: int = 1000) -> list[dict]:
    """List open issues with a specific label, oldest first.

    Returns list of dicts with keys: number, title, body, labels, blocked_by.
    `blocked_by` holds the issue's native GitHub dependencies as
    {number, state, repo} dicts (state is GraphQL-cased: "OPEN"/"CLOSED").
    Paginated via GraphQL cursor (mirrors list_open_prs) so repos with more
    than 100 labeled issues do not silently drop the oldest ones. Capped at
    _MAX_PAGES * 100 issues.
    """
    _validate_repo(repo)
    if limit < 1:
        return []
    owner, name = repo.split("/", 1)
    query = """
query($owner: String!, $repo: String!, $label: String!, $first: Int!, $after: String) {
  repository(owner: $owner, name: $repo) {
    issues(
      states: OPEN
      first: $first
      after: $after
      filterBy: {labels: [$label]}
      orderBy: {field: CREATED_AT, direction: ASC}
    ) {
      pageInfo { hasNextPage endCursor }
      nodes {
        number
        title
        body
        labels(first: 100) { nodes { name color description } }
        blockedBy(first: 50) {
          nodes { number state repository { nameWithOwner } }
        }
      }
    }
  }
}
"""
    issues: list[dict] = []
    cursor: str | None = None
    page_count = 0
    issues_conn: dict[str, Any] = {}
    while len(issues) < limit and page_count < _MAX_PAGES:
        page_count += 1
        page_size = min(100, limit - len(issues))
        args = [
            "api",
            "graphql",
            "-f",
            f"owner={owner}",
            "-f",
            f"repo={name}",
            "-f",
            f"label={label}",
            "-F",
            f"first={page_size}",
            "-f",
            f"query={query}",
        ]
        if cursor is not None:
            args.extend(["-f", f"after={cursor}"])
        output = _run_gh(args, token)
        if not output:
            return issues
        try:
            data = json.loads(output)
        except json.JSONDecodeError as e:
            raise GhCliError(f"Failed to parse gh output as JSON: {e}") from e
        if isinstance(data, list):
            return data
        if "errors" in data:
            msgs = [e.get("message", str(e)) for e in data["errors"]]
            raise GhCliError(f"GraphQL errors fetching labeled issues: {'; '.join(msgs)}")
        repo_data = (data.get("data") or {}).get("repository")
        if not repo_data:
            raise GhCliError(
                f"GraphQL returned null repository for {repo!r} -- "
                "check that the repo exists and the token has access"
            )
        issues_conn = repo_data.get("issues") or {}
        for node in issues_conn.get("nodes") or []:
            labels = (node.get("labels") or {}).get("nodes") or []
            normalized = dict(node)
            normalized["labels"] = labels
            # GitHub caps native dependencies at 50 per relationship type,
            # so blockedBy(first: 50) is always the complete set.
            normalized["blocked_by"] = [
                {
                    "number": blocker["number"],
                    "state": blocker.get("state"),
                    "repo": (blocker.get("repository") or {}).get("nameWithOwner"),
                }
                for blocker in (node.get("blockedBy") or {}).get("nodes") or []
                if blocker.get("number") is not None
            ]
            normalized.pop("blockedBy", None)
            issues.append(normalized)
        page_info = issues_conn.get("pageInfo") or {}
        if page_info.get("hasNextPage") and len(issues) < limit:
            cursor = page_info.get("endCursor")
            if not cursor:
                logger.warning(
                    "Labeled-issue pagination for %s had hasNextPage=True but no endCursor; "
                    "stopping with %d issues fetched",
                    repo,
                    len(issues),
                )
                break
        else:
            break
    else:
        page_info = issues_conn.get("pageInfo") or {}
        if page_info.get("hasNextPage") and len(issues) < limit:
            logger.warning(
                "Labeled-issue listing for %s reached MAX_PAGES (%d); some issues may "
                "have been truncated (%d fetched so far)",
                repo,
                _MAX_PAGES,
                len(issues),
            )
    return issues


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


def get_issue_state(repo: str, number: int, token: str) -> str:
    """Return "open" / "closed" / "missing" for a single issue.

    Lighter than `get_issue` — only the `state` field is requested.
    "missing" is returned when gh reports the issue cannot be found,
    so callers can treat deleted / wrong-number references as non-blocking.
    """
    _validate_repo(repo)
    try:
        output = _run_gh(
            [
                "issue",
                "view",
                str(number),
                "--repo",
                repo,
                "--json",
                "state",
            ],
            token,
        )
    except GhCliError as exc:
        stderr_lc = (exc.stderr or "").lower()
        if "could not resolve" in stderr_lc or "not found" in stderr_lc:
            # The number may belong to a PR, not an Issue (shared number space).
            # `gh issue view` errors on a PR number; fall back to a PR lookup so
            # `blocked by #PR` / `after #PR merges` correctly defers the dependent.
            return _pr_state_for_blocker(repo, number, token)
        raise
    if not output:
        return "missing"
    try:
        data = json.loads(output)
    except json.JSONDecodeError as e:
        raise GhCliError(f"Failed to parse gh output as JSON: {e}") from e
    state = (data.get("state") or "").lower()
    if state in ("open", "closed"):
        return state
    return "missing"


def _pr_state_for_blocker(repo: str, number: int, token: str) -> str:
    """Resolve a blocker number that is not an issue as a PR. Returns
    'open' (OPEN), 'closed' (CLOSED/MERGED), or 'missing' (genuinely absent).
    """
    try:
        output = _run_gh(
            ["pr", "view", str(number), "--repo", repo, "--json", "state"],
            token,
        )
    except GhCliError as exc:
        stderr_lc = (exc.stderr or "").lower()
        if "could not resolve" in stderr_lc or "not found" in stderr_lc:
            return "missing"
        raise
    if not output:
        return "missing"
    try:
        data = json.loads(output)
    except json.JSONDecodeError as e:
        raise GhCliError(f"Failed to parse gh output as JSON: {e}") from e
    state = (data.get("state") or "").lower()
    if state == "open":
        return "open"
    if state in ("closed", "merged"):
        return "closed"
    return "missing"


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
        if _label_already_absent(exc.stderr):
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
