"""PR discovery and state management.

Discovers open PRs, applies a filter cascade
(labels -> drafts -> locks -> attempts -> CI -> reviews),
and returns a list of PRState objects with recommended actions. The orchestrator
main loop acts on these recommendations.
"""

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import cast

from orcest.orchestrator import gh
from orcest.shared.config import LabelConfig
from orcest.shared.coordination import make_pending_task_key, make_pr_lock_key
from orcest.shared.redis_client import RedisClient

logger = logging.getLogger(__name__)

# Terminal CheckRun conclusions that indicate CI is not green.
# "neutral" and "skipped" are excluded as non-blocking outcomes.
_FAILURE_CONCLUSIONS = frozenset(
    {
        "FAILURE",
        "CANCELLED",
        "TIMED_OUT",
        "ACTION_REQUIRED",
        "STALE",
        "STARTUP_FAILURE",
    }
)


class PRAction(str, Enum):
    """What the orchestrator should do with a PR."""

    MERGE = "merge"  # Ready to merge (CI green + approved + no unresolved threads)
    ENQUEUE_FIX = "enqueue_fix"  # CI failing or review feedback
    ENQUEUE_FOLLOWUP = "enqueue_followup"  # Approved but unresolved threads — triage into issues
    ENQUEUE_REBASE = "enqueue_rebase"  # PR has merge conflicts; worker should rebase
    SKIP_LOCKED = "skip_locked"  # Another worker already on it
    SKIP_LABELED = "skip_labeled"  # Terminal label (blocked/needs-human)
    SKIP_ACTIVE = "skip_active"  # Previously attempted, awaiting external change (new commits)
    SKIP_GREEN = "skip_green"  # CI passing, nothing to do
    SKIP_DRAFT = "skip_draft"  # Draft PR, ignore
    SKIP_PENDING = "skip_pending"  # CI checks still running
    SKIP_QUEUED = "skip_queued"  # Task already pending in queue
    SKIP_MAX_ATTEMPTS = "skip_max_attempts"  # Exhausted per-SHA retry budget
    SKIP_MAX_TOTAL_ATTEMPTS = "skip_max_total_attempts"  # Exhausted cross-SHA retry budget
    SKIP_NO_CHECKS = "skip_no_checks"  # No CI checks configured or triggered
    RETRIGGER_REVIEW = "retrigger_review"  # claude-review passed but no formal review submitted
    RETRIGGER_STALE_CHECKS = "retrigger_stale_checks"  # Pending checks stuck; re-trigger
    SKIP_USAGE_COOLDOWN = "skip_usage_cooldown"  # USAGE_EXHAUSTED cooldown active; retry later


@dataclass
class PRState:
    """Analyzed state of a PR."""

    number: int
    title: str
    branch: str
    head_sha: str
    action: PRAction
    ci_failures: list[dict]  # Failed check runs
    review_threads: list[dict]  # Actionable review comments
    labels: list[str]
    base_branch: str = "main"  # Target branch (from baseRefName)
    review_run_id: int | None = None  # GitHub Actions run ID for re-triggering review
    stale_run_ids: list[int] = field(default_factory=list)  # Run IDs of stale pending checks


def _make_attempts_key(pr_number: int) -> str:
    """Redis key for tracking task attempt count per PR."""
    return f"pr:{pr_number}:attempts"


def get_attempt_count(redis: RedisClient, pr_number: int, head_sha: str) -> int:
    """Get the current attempt count for a PR.

    If the stored head SHA differs from the current one (new commits pushed),
    the counter is reset to 0.
    """
    key = _make_attempts_key(pr_number)
    data: dict[str, str] = cast(dict[str, str], redis.client.hgetall(key))
    if not data:
        return 0
    stored_sha = data.get("head_sha", "")
    if stored_sha != head_sha:
        # New commits pushed — reset counter.
        # TOCTOU note: the hgetall → delete sequence is not atomic. A second
        # concurrent caller could observe the same stale SHA and also call
        # delete, resulting in a double-delete (benign). This is intentional:
        # the system is single-orchestrator by design, so the race cannot
        # occur in practice. A Lua script would provide atomicity if
        # multi-instance support is ever added.
        redis.client.delete(key)
        return 0
    try:
        return int(data.get("count", 0))
    except (ValueError, TypeError):
        return 0


def increment_attempts(redis: RedisClient, pr_number: int, head_sha: str) -> int:
    """Increment and return the attempt count for a PR.

    If the stored head SHA differs from ``head_sha`` (new commits were
    pushed), the counter is reset to 1 instead of blindly incrementing
    from the stale value.

    Sets a 7-day TTL on the key so closed/merged PR counters don't
    leak memory indefinitely.
    """
    key = _make_attempts_key(pr_number)

    # Check for SHA mismatch *before* incrementing so the counter
    # resets correctly even if get_attempt_count was never called.
    # TOCTOU note: the hget → delete → pipeline sequence is not atomic. Two
    # concurrent callers could both observe a stale SHA, both call delete, and
    # then both hincrby — producing an incorrect retry count. This is
    # intentional: the system is single-orchestrator by design, so the race
    # cannot occur in practice. A Lua script would provide atomicity if
    # multi-instance support is ever added.
    stored_sha = redis.client.hget(key, "head_sha")
    if stored_sha is not None and stored_sha != head_sha:
        redis.client.delete(key)

    pipe = redis.client.pipeline(transaction=True)
    pipe.hincrby(key, "count", 1)
    pipe.hset(key, "head_sha", head_sha)
    pipe.expire(key, 7 * 24 * 3600)  # 7-day TTL
    results = pipe.execute()
    return results[0]  # new count


def clear_attempts(redis: RedisClient, pr_number: int) -> None:
    """Clear the attempt counter for a PR (e.g. on successful completion)."""
    redis.client.delete(_make_attempts_key(pr_number))


def _make_total_attempts_key(pr_number: int) -> str:
    """Redis key for tracking total attempts across all SHAs."""
    return f"pr:{pr_number}:total_attempts"


def get_total_attempt_count(redis: RedisClient, pr_number: int) -> int:
    """Get the total attempt count for a PR (across all SHAs)."""
    val: str | None = cast(str | None, redis.client.get(_make_total_attempts_key(pr_number)))
    if val is None:
        return 0
    try:
        return int(val)
    except (ValueError, TypeError):
        return 0


def increment_total_attempts(redis: RedisClient, pr_number: int) -> int:
    """Increment the total attempt count for a PR. Returns the new count.

    Uses INCR + EXPIRE so the counter auto-cleans after 30 days.
    """
    key = _make_total_attempts_key(pr_number)
    pipe = redis.client.pipeline(transaction=True)
    pipe.incr(key)
    pipe.expire(key, 30 * 24 * 3600)  # 30-day TTL
    results = pipe.execute()
    return results[0]


def clear_total_attempts(redis: RedisClient, pr_number: int) -> None:
    """Clear the total attempt counter for a PR (on successful completion)."""
    redis.client.delete(_make_total_attempts_key(pr_number))


def _make_exhausted_notified_key(pr_number: int) -> str:
    """Redis key tracking whether we've already notified humans of total-attempt exhaustion."""
    return f"pr:{pr_number}:exhausted_notified"


def get_exhausted_notified(redis: RedisClient, pr_number: int) -> bool:
    """Return True if we have already posted the exhausted-budget notification for this PR."""
    return bool(redis.client.exists(_make_exhausted_notified_key(pr_number)))


def set_exhausted_notified(redis: RedisClient, pr_number: int) -> None:
    """Record that the exhausted-budget notification was posted for this PR.

    Uses a 30-day TTL to match the total_attempts counter lifetime.
    """
    redis.client.set(_make_exhausted_notified_key(pr_number), "1", ex=30 * 24 * 3600)


def clear_exhausted_notified(redis: RedisClient, pr_number: int) -> None:
    """Clear the exhausted-budget notification flag (e.g. when human approves a retry)."""
    redis.client.delete(_make_exhausted_notified_key(pr_number))


def _make_review_retrigger_key(pr_number: int) -> str:
    """Redis key for tracking review re-trigger attempts per PR."""
    return f"pr:{pr_number}:review_retrigger"


def get_review_retrigger_sha(redis: RedisClient, pr_number: int) -> str | None:
    """Get the SHA that was already re-triggered for review, or None."""
    val: str | None = cast(str | None, redis.client.get(_make_review_retrigger_key(pr_number)))
    return val


def set_review_retrigger_sha(redis: RedisClient, pr_number: int, head_sha: str) -> None:
    """Record that we re-triggered review for this SHA. Expires in 7 days."""
    redis.client.set(_make_review_retrigger_key(pr_number), head_sha, ex=7 * 24 * 3600)


def clear_review_retrigger(redis: RedisClient, pr_number: int) -> None:
    """Clear the review re-trigger marker for a PR."""
    redis.client.delete(_make_review_retrigger_key(pr_number))


def _make_stale_retrigger_key(pr_number: int) -> str:
    """Redis key for tracking stale-check re-trigger per PR."""
    return f"pr:{pr_number}:stale_retrigger"


def get_stale_retrigger_sha(redis: RedisClient, pr_number: int) -> str | None:
    """Get the SHA for which stale checks were already re-triggered, or None."""
    val: str | None = cast(str | None, redis.client.get(_make_stale_retrigger_key(pr_number)))
    return val


def set_stale_retrigger_sha(redis: RedisClient, pr_number: int, head_sha: str, ex: int) -> None:
    """Record that we re-triggered stale checks for this SHA. Expires after ``ex`` seconds."""
    redis.client.set(_make_stale_retrigger_key(pr_number), head_sha, ex=ex)


def _make_usage_cooldown_key(pr_number: int) -> str:
    """Redis key for the USAGE_EXHAUSTED cooldown marker."""
    return f"pr:{pr_number}:usage_cooldown"


def set_usage_exhausted_cooldown(
    redis: RedisClient, pr_number: int, ttl_seconds: int = 1800
) -> None:
    """Set a cooldown marker so the PR is not immediately re-enqueued after USAGE_EXHAUSTED.

    The key expires after ``ttl_seconds`` (default 30 minutes), at which point
    the next poll cycle will pick the PR up again.
    """
    redis.client.set(_make_usage_cooldown_key(pr_number), "1", ex=ttl_seconds)


def has_usage_exhausted_cooldown(redis: RedisClient, pr_number: int) -> bool:
    """Return True if a USAGE_EXHAUSTED cooldown is still active for this PR."""
    return bool(redis.client.exists(_make_usage_cooldown_key(pr_number)))


def _make_transient_attempts_key(pr_number: int) -> str:
    """Redis key for tracking transient CI retry count per PR."""
    return f"pr:{pr_number}:transient_attempts"


def get_transient_attempt_count(redis: RedisClient, pr_number: int, head_sha: str) -> int:
    """Get the transient CI retry count for a PR.

    Resets to 0 when the head SHA changes (new commits pushed), so the
    transient budget is per-SHA just like the main attempt counter.
    """
    key = _make_transient_attempts_key(pr_number)
    data: dict[str, str] = cast(dict[str, str], redis.client.hgetall(key))
    if not data:
        return 0
    stored_sha = data.get("head_sha", "")
    if stored_sha != head_sha:
        return 0
    try:
        return int(data.get("count", 0))
    except (ValueError, TypeError):
        return 0


def increment_transient_attempts(redis: RedisClient, pr_number: int, head_sha: str) -> int:
    """Increment and return the transient CI retry count for a PR.

    Resets to 1 if the stored head SHA differs from head_sha (new commits).
    Sets a 7-day TTL on the key so closed/merged PR counters don't leak.
    """
    key = _make_transient_attempts_key(pr_number)
    # The hget + conditional delete are intentionally outside the pipeline/transaction:
    # the orchestrator is a single instance, so there is no concurrent writer that
    # could race between this delete and the pipeline execute below.  Moving the
    # delete inside the pipeline would require a Lua script to make the
    # read-then-conditional-delete atomic; that complexity isn't warranted here.
    stored_sha = redis.client.hget(key, "head_sha")
    if stored_sha is not None and stored_sha != head_sha:
        redis.client.delete(key)
    # pipeline(transaction=True) maps to a Redis MULTI/EXEC block: all three
    # commands execute atomically — either all succeed or none do.  There is no
    # risk of the counter being incremented while head_sha is left stale.
    pipe = redis.client.pipeline(transaction=True)
    pipe.hincrby(key, "count", 1)
    pipe.hset(key, "head_sha", head_sha)
    pipe.expire(key, 7 * 24 * 3600)  # 7-day TTL
    results = pipe.execute()
    return results[0]  # new count


def _parse_iso_timestamp(ts: str | None) -> datetime | None:
    """Parse an ISO 8601 timestamp string into a timezone-aware datetime.

    Returns None if the input is absent or cannot be parsed.
    """
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


def _check_stale_pending(ci_pending: list[dict], timeout_seconds: int) -> tuple[bool, list[int]]:
    """Determine whether all pending checks have exceeded the staleness timeout.

    Returns ``(all_stale, run_ids)`` where:
    - ``all_stale`` is True only when every pending check has been in a
      pending state for longer than ``timeout_seconds``.
    - ``run_ids`` lists the GitHub Actions workflow run IDs extracted from
      ``detailsUrl`` for re-triggering (may be empty if checks lack that URL,
      e.g. StatusContext checks).

    A check without a parseable ``startedAt``/``createdAt`` timestamp is
    treated as non-stale (conservative: avoids spurious re-triggers).
    """
    if not ci_pending:
        return False, []

    now = datetime.now(timezone.utc)

    for check in ci_pending:
        ts_str = check.get("startedAt") or check.get("createdAt")
        started_at = _parse_iso_timestamp(ts_str)
        if started_at is None:
            return False, []
        if (now - started_at).total_seconds() < timeout_seconds:
            return False, []

    # All pending checks have exceeded the timeout — collect their run IDs
    run_ids: list[int] = []
    for check in ci_pending:
        details_url = check.get("detailsUrl") or ""
        match = re.search(r"/actions/runs/(\d+)", details_url)
        if match:
            run_id = int(match.group(1))
            if run_id not in run_ids:
                run_ids.append(run_id)
    return True, run_ids


def _get_claude_review_run_id(checks: list[dict]) -> int | None:
    """Extract the GitHub Actions run ID for a successful claude-review check.

    Returns None if no claude-review check exists, it didn't succeed, or any
    claude-review run is currently in progress (to avoid acting on a stale
    completed result while a new run is pending after a re-trigger).
    """
    claude_review_checks = [c for c in checks if c.get("name") == "claude-review"]
    if not claude_review_checks:
        return None

    # If any run is still in progress, a re-triggered run may be pending —
    # don't act on a stale completed result.
    for check in claude_review_checks:
        if (check.get("status") or "").upper() != "COMPLETED":
            return None

    for check in claude_review_checks:
        if (check.get("conclusion") or "").upper() == "SUCCESS":
            details_url = check.get("detailsUrl", "")
            # URL format: https://github.com/.../actions/runs/{run_id}/job/{job_id}
            match = re.search(r"/actions/runs/(\d+)", details_url)
            if match:
                return int(match.group(1))
    return None


def discover_actionable_prs(
    repo: str,
    token: str,
    redis: RedisClient,
    label_config: LabelConfig,
    max_attempts: int = 3,
    max_total_attempts: int = 10,
    stale_pending_timeout_seconds: int = 7200,
) -> list[PRState]:
    """Discover PRs that need action.

    Filter cascade (ordered by cost, cheapest first):
    1. Skip draft PRs (single boolean field, cheapest check)
    2. Skip PRs with terminal orcest labels (blocked/needs-human)
    3. Skip PRs with active Redis locks (worker in progress)
    4. Skip PRs with a pending task already queued
    5. Skip PRs that exceeded total cross-SHA attempt limit
    6. Skip PRs that have been attempted but haven't changed (attempt count > 0)
    7. Route PRs with merge conflicts (mergeable == CONFLICTING) to ENQUEUE_REBASE
    8. Fetch CI status; skip if checks are still pending or absent
    9. Route by CI + review state: failures -> fix, changes requested -> fix,
       approved + unresolved threads -> followup, approved + clean -> merge
    """
    prs = gh.list_open_prs(repo, token)
    results: list[PRState] = []

    terminal_labels = {
        label_config.blocked,
        label_config.needs_human,
    }

    for pr_data in prs:
        number: int = pr_data["number"]
        title: str = pr_data["title"]
        branch: str = pr_data["headRefName"]
        base_branch: str = pr_data.get("baseRefName", "main")
        head_sha: str = pr_data.get("headRefOid", "")
        pr_labels: list[str] = [lbl.get("name", "") for lbl in (pr_data.get("labels") or [])]

        # Skip draft PRs -- cheapest check, single boolean field
        if pr_data.get("isDraft"):
            results.append(
                PRState(
                    number=number,
                    title=title,
                    branch=branch,
                    head_sha=head_sha,
                    action=PRAction.SKIP_DRAFT,
                    ci_failures=[],
                    review_threads=[],
                    labels=pr_labels,
                    base_branch=base_branch,
                )
            )
            continue

        # Skip if terminal orcest label present (blocked/needs-human)
        if any(label in terminal_labels for label in pr_labels):
            results.append(
                PRState(
                    number=number,
                    title=title,
                    branch=branch,
                    head_sha=head_sha,
                    action=PRAction.SKIP_LABELED,
                    ci_failures=[],
                    review_threads=[],
                    labels=pr_labels,
                    base_branch=base_branch,
                )
            )
            continue

        # Skip if locked in Redis
        lock_key = make_pr_lock_key(repo, number)
        if redis.client.exists(lock_key):
            results.append(
                PRState(
                    number=number,
                    title=title,
                    branch=branch,
                    head_sha=head_sha,
                    action=PRAction.SKIP_LOCKED,
                    ci_failures=[],
                    review_threads=[],
                    labels=pr_labels,
                    base_branch=base_branch,
                )
            )
            continue

        # Skip if a task for this PR is already pending in the queue
        pending_key = make_pending_task_key(repo, "pr", number)
        if redis.client.exists(pending_key):
            results.append(
                PRState(
                    number=number,
                    title=title,
                    branch=branch,
                    head_sha=head_sha,
                    action=PRAction.SKIP_QUEUED,
                    ci_failures=[],
                    review_threads=[],
                    labels=pr_labels,
                    base_branch=base_branch,
                )
            )
            continue

        # Skip if total cross-SHA attempt limit exceeded (circuit breaker).
        # Check this before the usage cooldown so the circuit-breaker state is
        # visible immediately rather than being masked for 30 minutes.
        #
        # Recovery path: if the exhausted_notified flag is set and the
        # needs-human label is gone, reset the counters and fall through to
        # normal processing. Any actor (human or automation) that removes the
        # label is treated as approval — the code cannot distinguish between
        # deliberate human intent and automated label removal.
        # Note: the label is *not* re-checked here because needs_human is in
        # terminal_labels — any PR still carrying it is caught by SKIP_LABELED
        # above and never reaches this block.
        total_attempts = get_total_attempt_count(redis, number)
        if total_attempts >= max_total_attempts:
            if get_exhausted_notified(redis, number):
                # exhausted_notified is set and needs-human label is absent (inferred
                # via SKIP_LABELED invariant above); treat as retry signal and reset.
                clear_total_attempts(redis, number)
                clear_exhausted_notified(redis, number)
                logger.info(
                    "PR #%d: exhausted_notified set and needs-human label absent"
                    " (inferred via SKIP_LABELED invariant); resetting attempt counters for retry",
                    number,
                )
                # Fall through to normal processing (Redis counters now reset).
            else:
                results.append(
                    PRState(
                        number=number,
                        title=title,
                        branch=branch,
                        head_sha=head_sha,
                        action=PRAction.SKIP_MAX_TOTAL_ATTEMPTS,
                        ci_failures=[],
                        review_threads=[],
                        labels=pr_labels,
                        base_branch=base_branch,
                    )
                )
                continue

        # Skip if a USAGE_EXHAUSTED cooldown is still active (waiting for
        # API capacity to recover before re-enqueuing).
        # Note: the cooldown is keyed to PR number, not head SHA, so new commits
        # pushed during the cooldown window are still blocked for up to 30 minutes.
        # This is intentional — USAGE_EXHAUSTED is account-level, so new commits
        # don't help. If new commits should bypass the cooldown (e.g. urgent
        # hotfixes), a SHA comparison would be needed here.
        if has_usage_exhausted_cooldown(redis, number):
            results.append(
                PRState(
                    number=number,
                    title=title,
                    branch=branch,
                    head_sha=head_sha,
                    action=PRAction.SKIP_USAGE_COOLDOWN,
                    ci_failures=[],
                    review_threads=[],
                    labels=pr_labels,
                    base_branch=base_branch,
                )
            )
            continue

        # Skip if previously attempted on this SHA (awaiting new commits)
        # or max attempts reached.
        attempt_count = get_attempt_count(redis, number, head_sha)
        if attempt_count >= max_attempts:
            results.append(
                PRState(
                    number=number,
                    title=title,
                    branch=branch,
                    head_sha=head_sha,
                    action=PRAction.SKIP_MAX_ATTEMPTS,
                    ci_failures=[],
                    review_threads=[],
                    labels=pr_labels,
                    base_branch=base_branch,
                )
            )
            continue
        if attempt_count > 0:
            results.append(
                PRState(
                    number=number,
                    title=title,
                    branch=branch,
                    head_sha=head_sha,
                    action=PRAction.SKIP_ACTIVE,
                    ci_failures=[],
                    review_threads=[],
                    labels=pr_labels,
                    base_branch=base_branch,
                )
            )
            continue

        # Route conflicting PRs to rebase before the expensive CI fetch.
        # mergeable is fetched as part of list_open_prs (no extra API call).
        # UNKNOWN means GitHub hasn't computed mergeability yet — ignore it.
        if pr_data.get("mergeable") == "CONFLICTING":
            logger.info(
                "PR #%d has merge conflicts (mergeable=CONFLICTING), enqueuing rebase",
                number,
            )
            results.append(
                PRState(
                    number=number,
                    title=title,
                    branch=branch,
                    head_sha=head_sha,
                    action=PRAction.ENQUEUE_REBASE,
                    ci_failures=[],
                    review_threads=[],
                    labels=pr_labels,
                    base_branch=base_branch,
                )
            )
            continue

        # Check CI status -- wrapped in try/except so a single PR's
        # failure does not crash discovery for all other PRs.
        try:
            checks = gh.get_ci_status(repo, number, token)
        except Exception:
            logger.warning(
                "Failed to fetch CI status for PR #%d, skipping",
                number,
                exc_info=True,
            )
            continue

        # No CI checks at all — distinct from green (all checks passed).
        # This can happen when CI is not configured, or when mergeability
        # is UNKNOWN and GitHub did not trigger CI on the branch.
        # Note: PRs in repos with no CI configured will never be merged by orcest.
        if not checks:
            logger.debug("PR #%d has no CI checks, skipping", number)
            results.append(
                PRState(
                    number=number,
                    title=title,
                    branch=branch,
                    head_sha=head_sha,
                    action=PRAction.SKIP_NO_CHECKS,
                    ci_failures=[],
                    review_threads=[],
                    labels=pr_labels,
                    base_branch=base_branch,
                )
            )
            continue

        ci_failures = [
            c
            for c in checks
            if (c.get("conclusion") or "").upper() in _FAILURE_CONCLUSIONS
            or (not c.get("conclusion") and (c.get("state") or "").upper() in ("FAILURE", "ERROR"))
        ]
        # A check is pending if it hasn't reached a terminal state.
        # statusCheckRollup can include both CheckRun objects (which have
        # "conclusion") and StatusContext objects (which have "state").
        # - CheckRun: pending when "conclusion" is absent/empty (still running)
        # - StatusContext: pending when "state" is absent, empty, or "PENDING"
        ci_pending = [
            c
            for c in checks
            if not c.get("conclusion")
            # "" matches both absent StatusContext state and absent CheckRun state (no state field)
            and (c.get("state") or "").upper() in ("", "PENDING", "EXPECTED")
        ]

        if ci_pending and not ci_failures:
            # Only skip as pending if no checks have failed yet.
            # If there are already failures, enqueue a fix immediately
            # rather than waiting for other checks to finish.
            all_stale, stale_run_ids = _check_stale_pending(
                ci_pending, stale_pending_timeout_seconds
            )
            if all_stale:
                # All pending checks have exceeded the staleness timeout.
                # Re-trigger what we can; if no run IDs are extractable
                # (e.g. StatusContext checks), the loop will add needs-human.
                logger.warning(
                    "PR #%d has %d stale pending check(s) (>%ds), "
                    "escalating for re-trigger (run_ids=%s)",
                    number,
                    len(ci_pending),
                    stale_pending_timeout_seconds,
                    stale_run_ids,
                )
                results.append(
                    PRState(
                        number=number,
                        title=title,
                        branch=branch,
                        head_sha=head_sha,
                        action=PRAction.RETRIGGER_STALE_CHECKS,
                        ci_failures=[],
                        review_threads=[],
                        labels=pr_labels,
                        base_branch=base_branch,
                        stale_run_ids=stale_run_ids,
                    )
                )
            else:
                logger.debug(
                    "PR #%d has %d check(s) still pending, skipping",
                    number,
                    len(ci_pending),
                )
                results.append(
                    PRState(
                        number=number,
                        title=title,
                        branch=branch,
                        head_sha=head_sha,
                        action=PRAction.SKIP_PENDING,
                        ci_failures=[],
                        review_threads=[],
                        labels=pr_labels,
                        base_branch=base_branch,
                    )
                )
            continue

        # Check review state
        review_decision = pr_data.get("reviewDecision", "")

        if ci_failures:
            # CI failing — enqueue fix (priority over review state)
            results.append(
                PRState(
                    number=number,
                    title=title,
                    branch=branch,
                    head_sha=head_sha,
                    action=PRAction.ENQUEUE_FIX,
                    ci_failures=ci_failures,
                    review_threads=[],
                    labels=pr_labels,
                    base_branch=base_branch,
                )
            )
        elif review_decision == "CHANGES_REQUESTED":
            # CI green but reviewer requested changes — enqueue fix
            # Fetch unresolved review threads for worker prompt context
            try:
                threads = gh.get_unresolved_review_threads(repo, number, token)
            except Exception:
                logger.warning(
                    "Failed to fetch review threads for PR #%d, enqueuing without thread details",
                    number,
                    exc_info=True,
                )
                threads = []

            results.append(
                PRState(
                    number=number,
                    title=title,
                    branch=branch,
                    head_sha=head_sha,
                    action=PRAction.ENQUEUE_FIX,
                    ci_failures=[],
                    review_threads=threads,
                    labels=pr_labels,
                    base_branch=base_branch,
                )
            )
        elif review_decision == "APPROVED":
            # CI green + approved — check for unresolved threads
            try:
                threads = gh.get_unresolved_review_threads(repo, number, token)
            except Exception:
                # Cannot verify thread state — do NOT merge. Fall through
                # to SKIP_GREEN so the PR stays visible and gets retried
                # on the next poll cycle.
                logger.warning(
                    "Failed to fetch review threads for PR #%d, "
                    "skipping merge until threads can be verified",
                    number,
                    exc_info=True,
                )
                results.append(
                    PRState(
                        number=number,
                        title=title,
                        branch=branch,
                        head_sha=head_sha,
                        action=PRAction.SKIP_GREEN,
                        ci_failures=[],
                        review_threads=[],
                        labels=pr_labels,
                        base_branch=base_branch,
                    )
                )
                continue

            if threads:
                # Approved but unresolved threads — triage into issues
                logger.info(
                    "PR #%d is approved but has %d unresolved thread(s), enqueuing followup triage",
                    number,
                    len(threads),
                )
                results.append(
                    PRState(
                        number=number,
                        title=title,
                        branch=branch,
                        head_sha=head_sha,
                        action=PRAction.ENQUEUE_FOLLOWUP,
                        ci_failures=[],
                        review_threads=threads,
                        labels=pr_labels,
                        base_branch=base_branch,
                    )
                )
            else:
                # All clear — merge
                results.append(
                    PRState(
                        number=number,
                        title=title,
                        branch=branch,
                        head_sha=head_sha,
                        action=PRAction.MERGE,
                        ci_failures=[],
                        review_threads=[],
                        labels=pr_labels,
                        base_branch=base_branch,
                    )
                )
        else:
            # CI green, no formal review decision — check for unresolved
            # review threads (e.g. from automated code review comments that
            # use COMMENTED state rather than CHANGES_REQUESTED).
            try:
                threads = gh.get_unresolved_review_threads(repo, number, token)
            except Exception:
                logger.warning(
                    "Failed to fetch review threads for PR #%d, skipping",
                    number,
                    exc_info=True,
                )
                threads = []

            if threads:
                logger.info(
                    "PR #%d is CI green with %d unresolved review thread(s), enqueuing fix",
                    number,
                    len(threads),
                )
                results.append(
                    PRState(
                        number=number,
                        title=title,
                        branch=branch,
                        head_sha=head_sha,
                        action=PRAction.ENQUEUE_FIX,
                        ci_failures=[],
                        review_threads=threads,
                        labels=pr_labels,
                        base_branch=base_branch,
                    )
                )
            else:
                # CI green, no review threads, no formal review decision.
                # Check if claude-review passed but didn't submit a formal
                # review — if so, re-trigger once per SHA.
                review_run_id = _get_claude_review_run_id(checks)
                retrigger_sha = get_review_retrigger_sha(redis, number)

                if review_run_id is not None and retrigger_sha != head_sha:
                    # claude-review passed but no formal review — re-trigger
                    logger.info(
                        "PR #%d: claude-review passed but no formal review, will re-trigger run %d",
                        number,
                        review_run_id,
                    )
                    results.append(
                        PRState(
                            number=number,
                            title=title,
                            branch=branch,
                            head_sha=head_sha,
                            action=PRAction.RETRIGGER_REVIEW,
                            ci_failures=[],
                            review_threads=[],
                            labels=pr_labels,
                            base_branch=base_branch,
                            review_run_id=review_run_id,
                        )
                    )
                elif review_run_id is not None and retrigger_sha == head_sha:
                    # Already re-triggered for this SHA, still no review — escalate
                    logger.warning(
                        "PR #%d: claude-review re-trigger exhausted (SHA %s), escalating",
                        number,
                        head_sha[:8],
                    )
                    results.append(
                        PRState(
                            number=number,
                            title=title,
                            branch=branch,
                            head_sha=head_sha,
                            action=PRAction.SKIP_MAX_ATTEMPTS,
                            ci_failures=[],
                            review_threads=[],
                            labels=pr_labels,
                            base_branch=base_branch,
                        )
                    )
                else:
                    # No claude-review check found — normal SKIP_GREEN
                    results.append(
                        PRState(
                            number=number,
                            title=title,
                            branch=branch,
                            head_sha=head_sha,
                            action=PRAction.SKIP_GREEN,
                            ci_failures=[],
                            review_threads=[],
                            labels=pr_labels,
                            base_branch=base_branch,
                        )
                    )

    return results
