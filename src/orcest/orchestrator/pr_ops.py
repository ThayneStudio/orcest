"""PR discovery and state management.

Discovers open PRs, applies a filter cascade
(labels -> drafts -> locks -> attempts -> CI -> reviews),
and returns a list of PRState objects with recommended actions. The orchestrator
main loop acts on these recommendations.
"""

import hashlib
import logging
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum

import redis as redis_py

from orcest.orchestrator import gh
from orcest.shared.config import LabelConfig
from orcest.shared.coordination import (
    clear_backoff,
    clear_pending_task_if_matches,
    clear_transient_failure_count,
    get_backoff_head_sha,
    get_backoff_step,
    get_pending_task_metadata,
    make_pending_task_key,
    make_pr_lock_key,
)
from orcest.shared.redis_client import RedisClient
from orcest.workflow_contract.v1.publication import is_legacy_marker_reserved

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
    # Out-of-date with base, no conflicts — orchestrator updates via GitHub API
    UPDATE_BRANCH = "update_branch"
    SKIP_LOCKED = "skip_locked"  # Another worker already on it
    SKIP_LABELED = "skip_labeled"  # Terminal needs-human label
    SKIP_ACTIVE = "skip_active"  # Historical value; attempts alone no longer suppress work
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
    SKIP_BACKOFF = "skip_backoff"  # Transient failure backoff is active
    SKIP_V1_OWNED = "skip_v1_owned"  # Reserved for the workflow-control v1 engine
    # The v1 ownership snapshot could not be read, so fail closed without
    # pretending that an ownership association was actually observed.
    SKIP_V1_LOOKUP_UNAVAILABLE = "skip_v1_lookup_unavailable"


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


@dataclass(frozen=True)
class CIClassification:
    """Normalized CI state used by discovery and action-time merge validation."""

    failures: list[dict]
    pending: list[dict]

    @property
    def terminal_success(self) -> bool:
        return not self.failures and not self.pending


def classify_ci_checks(checks: list[dict]) -> CIClassification:
    """Classify GitHub statusCheckRollup nodes conservatively."""
    ci_failures = [
        c
        for c in checks
        if (c.get("conclusion") or "").upper() in _FAILURE_CONCLUSIONS
        or (not c.get("conclusion") and (c.get("state") or "").upper() in ("FAILURE", "ERROR"))
    ]
    ci_pending = [
        c
        for c in checks
        if not c.get("conclusion") and (c.get("state") or "").upper() in ("", "PENDING", "EXPECTED")
    ]
    return CIClassification(failures=ci_failures, pending=ci_pending)


def _make_attempts_key(repo: str, pr_number: int) -> str:
    """Redis key for tracking task attempt count per PR."""
    return f"pr:{repo}:{pr_number}:attempts"


def get_attempt_count(redis: RedisClient, repo: str, pr_number: int, head_sha: str) -> int:
    """Get the current attempt count for a PR.

    If the stored head SHA differs from the current one (new commits pushed),
    the counter is reset to 0.
    """
    key = _make_attempts_key(repo, pr_number)
    data: dict[str, str] = redis.hgetall(key)
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
        redis.delete(key)
        return 0
    try:
        return int(data.get("count", 0))
    except (ValueError, TypeError):
        return 0


def increment_attempts(redis: RedisClient, repo: str, pr_number: int, head_sha: str) -> int:
    """Increment and return the attempt count for a PR.

    If the stored head SHA differs from ``head_sha`` (new commits were
    pushed), the counter is reset to 1 instead of blindly incrementing
    from the stale value.

    Sets a 7-day TTL on the key so closed/merged PR counters don't
    leak memory indefinitely.
    """
    key = _make_attempts_key(repo, pr_number)

    # Check for SHA mismatch *before* incrementing so the counter
    # resets correctly even if get_attempt_count was never called.
    # TOCTOU note: the hget → delete → pipeline sequence is not atomic. Two
    # concurrent callers could both observe a stale SHA, both call delete, and
    # then both hincrby — producing an incorrect retry count. This is
    # intentional: the system is single-orchestrator by design, so the race
    # cannot occur in practice. A Lua script would provide atomicity if
    # multi-instance support is ever added.
    stored_sha = redis.hget(key, "head_sha")
    if stored_sha is not None and stored_sha != head_sha:
        redis.delete(key)

    pipe = redis.pipeline(transaction=True)
    pipe.hincrby(key, "count", 1)
    pipe.hset(key, "head_sha", head_sha)
    pipe.expire(key, 7 * 24 * 3600)  # 7-day TTL
    results = pipe.execute()
    return results[0]  # new count


def clear_attempts(redis: RedisClient, repo: str, pr_number: int) -> None:
    """Clear the attempt counter for a PR (e.g. on successful completion)."""
    redis.delete(_make_attempts_key(repo, pr_number))


def clear_attempts_if_head_sha(
    redis: RedisClient, repo: str, pr_number: int, head_sha: str
) -> bool:
    """Clear PR attempt counter only when it still belongs to ``head_sha``."""
    if not head_sha:
        return False
    key = _make_attempts_key(repo, pr_number)
    fq_key = redis._prefixed(key)
    while True:
        pipe = redis.client.pipeline()
        try:
            pipe.watch(fq_key)
            if pipe.hget(fq_key, "head_sha") != head_sha:
                pipe.unwatch()
                return False
            pipe.multi()
            pipe.delete(fq_key)
            pipe.execute()
            return True
        except redis_py.WatchError:
            continue
        finally:
            pipe.reset()


def _make_total_attempts_key(repo: str, pr_number: int) -> str:
    """Redis key for tracking total attempts across all SHAs."""
    return f"pr:{repo}:{pr_number}:total_attempts"


def get_total_attempt_count(redis: RedisClient, repo: str, pr_number: int) -> int:
    """Get the total attempt count for a PR (across all SHAs)."""
    val: str | None = redis.get(_make_total_attempts_key(repo, pr_number))
    if val is None:
        return 0
    try:
        return int(val)
    except (ValueError, TypeError):
        return 0


def increment_total_attempts(redis: RedisClient, repo: str, pr_number: int) -> int:
    """Increment the total attempt count for a PR. Returns the new count.

    Counts only terminal (non-transient, non-usage-exhausted) task failures
    across SHAs, not every task publication. Healthy review-fix churn does
    not bump this counter. Uses INCR + EXPIRE so the counter auto-cleans
    after 30 days.
    """
    key = _make_total_attempts_key(repo, pr_number)
    pipe = redis.pipeline(transaction=True)
    pipe.incr(key)
    pipe.expire(key, 30 * 24 * 3600)  # 30-day TTL
    results = pipe.execute()
    return results[0]


def clear_total_attempts(redis: RedisClient, repo: str, pr_number: int) -> None:
    """Clear the total attempt counter for a PR (on successful completion)."""
    redis.delete(_make_total_attempts_key(repo, pr_number))


def _make_exhausted_notified_key(repo: str, pr_number: int) -> str:
    """Redis key tracking whether we've already notified humans of total-attempt exhaustion."""
    return f"pr:{repo}:{pr_number}:exhausted_notified"


def get_exhausted_notified(redis: RedisClient, repo: str, pr_number: int) -> bool:
    """Return True if we have already posted the exhausted-budget notification for this PR."""
    return bool(redis.exists(_make_exhausted_notified_key(repo, pr_number)))


def set_exhausted_notified(redis: RedisClient, repo: str, pr_number: int) -> None:
    """Record that the exhausted-budget notification was posted for this PR.

    Uses a 30-day TTL to match the total_attempts counter lifetime.
    """
    redis.set_ex(_make_exhausted_notified_key(repo, pr_number), "1", 30 * 24 * 3600)


def clear_exhausted_notified(redis: RedisClient, repo: str, pr_number: int) -> None:
    """Clear the exhausted-budget notification flag (e.g. when human approves a retry)."""
    redis.delete(_make_exhausted_notified_key(repo, pr_number))


def _make_review_retrigger_key(repo: str, pr_number: int) -> str:
    """Redis key for tracking review re-trigger attempts per PR."""
    return f"pr:{repo}:{pr_number}:review_retrigger"


def get_review_retrigger_sha(redis: RedisClient, repo: str, pr_number: int) -> str | None:
    """Get the SHA that was already re-triggered for review, or None."""
    val: str | None = redis.get(_make_review_retrigger_key(repo, pr_number))
    return val


def set_review_retrigger_sha(redis: RedisClient, repo: str, pr_number: int, head_sha: str) -> None:
    """Record that we re-triggered review for this SHA. Expires in 7 days."""
    redis.set_ex(_make_review_retrigger_key(repo, pr_number), head_sha, 7 * 24 * 3600)


def clear_review_retrigger(redis: RedisClient, repo: str, pr_number: int) -> None:
    """Clear the review re-trigger marker for a PR."""
    redis.delete(_make_review_retrigger_key(repo, pr_number))


def _make_stale_retrigger_key(repo: str, pr_number: int) -> str:
    """Redis key for tracking stale-check re-trigger per PR."""
    return f"pr:{repo}:{pr_number}:stale_retrigger"


def get_stale_retrigger_sha(redis: RedisClient, repo: str, pr_number: int) -> str | None:
    """Get the SHA for which stale checks were already re-triggered, or None."""
    val: str | None = redis.get(_make_stale_retrigger_key(repo, pr_number))
    return val


def set_stale_retrigger_sha(
    redis: RedisClient, repo: str, pr_number: int, head_sha: str, ex: int
) -> None:
    """Record that we re-triggered stale checks for this SHA. Expires after ``ex`` seconds."""
    redis.set_ex(_make_stale_retrigger_key(repo, pr_number), head_sha, ex)


def _make_self_cancelled_stale_runs_key(repo: str, pr_number: int, head_sha: str) -> str:
    """Redis set of GitHub Actions run IDs cancelled by stale-check self-healing."""
    return f"pr:{repo}:{pr_number}:{head_sha}:self_cancelled_stale_runs"


def record_self_cancelled_stale_runs(
    redis: RedisClient,
    repo: str,
    pr_number: int,
    head_sha: str,
    run_ids: list[int],
    ttl_seconds: int,
) -> None:
    """Record stale workflow runs cancelled by orcest so their CANCELLED checks are ignored."""
    if not run_ids:
        return
    key = _make_self_cancelled_stale_runs_key(repo, pr_number, head_sha)
    redis.sadd(key, *(str(run_id) for run_id in run_ids))
    redis.expire(key, ttl_seconds)


def _extract_actions_run_id(check: dict) -> int | None:
    """Extract a GitHub Actions run ID from a check details URL."""
    details_url = check.get("detailsUrl") or ""
    match = re.search(r"/actions/runs/(\d+)", details_url)
    if not match:
        return None
    return int(match.group(1))


def _is_self_cancelled_stale_failure(
    redis: RedisClient,
    repo: str,
    pr_number: int,
    head_sha: str,
    check: dict,
) -> bool:
    """Return True when this CANCELLED check was created by stale-check self-healing."""
    if (check.get("conclusion") or "").upper() != "CANCELLED":
        return False
    run_id = _extract_actions_run_id(check)
    if run_id is None:
        return False
    key = _make_self_cancelled_stale_runs_key(repo, pr_number, head_sha)
    return str(run_id) in redis.smembers(key)


def _make_pending_check_first_seen_key(
    repo: str, pr_number: int, head_sha: str, check: dict
) -> str:
    """Redis key for timestamp-less pending check first-observed time."""
    # State (EXPECTED vs PENDING) is intentionally excluded from the identity:
    # the same check transitioning EXPECTED→PENDING must keep its first-seen
    # timestamp, otherwise stale-pending escalation never fires.
    identity = "\0".join(
        str(check.get(field) or "") for field in ("name", "detailsUrl", "targetUrl")
    )
    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:24]
    return f"pr:{repo}:{pr_number}:{head_sha}:pending_check_first_seen:{digest}"


def _fill_pending_first_seen_timestamps(
    redis: RedisClient,
    repo: str,
    pr_number: int,
    head_sha: str,
    ci_pending: list[dict],
    ttl_seconds: int,
) -> list[dict]:
    """Attach first-seen timestamps to pending checks that GitHub reports without one."""
    now = datetime.now(timezone.utc).isoformat()
    enriched: list[dict] = []

    for check in ci_pending:
        if check.get("startedAt") or check.get("createdAt"):
            enriched.append(check)
            continue

        key = _make_pending_check_first_seen_key(repo, pr_number, head_sha, check)
        first_seen = redis.get(key)
        if first_seen is None:
            first_seen = now
            redis.set_ex(key, first_seen, ttl_seconds)

        enriched_check = dict(check)
        enriched_check["createdAt"] = first_seen
        enriched.append(enriched_check)

    return enriched


def _make_usage_cooldown_key(repo: str, pr_number: int) -> str:
    """Redis key for the USAGE_EXHAUSTED cooldown marker."""
    return f"pr:{repo}:{pr_number}:usage_cooldown"


def set_usage_exhausted_cooldown(
    redis: RedisClient, repo: str, pr_number: int, ttl_seconds: int = 1800
) -> None:
    """Set a cooldown marker so the PR is not immediately re-enqueued after USAGE_EXHAUSTED.

    The key expires after ``ttl_seconds`` (default 30 minutes), at which point
    the next poll cycle will pick the PR up again.
    """
    redis.set_ex(_make_usage_cooldown_key(repo, pr_number), "1", ttl_seconds)


def has_usage_exhausted_cooldown(redis: RedisClient, repo: str, pr_number: int) -> bool:
    """Return True if a USAGE_EXHAUSTED cooldown is still active for this PR."""
    return bool(redis.exists(_make_usage_cooldown_key(repo, pr_number)))


def _make_transient_attempts_key(repo: str, pr_number: int) -> str:
    """Redis key for tracking transient CI retry count per PR."""
    return f"pr:{repo}:{pr_number}:transient_attempts"


def get_transient_attempt_count(
    redis: RedisClient, repo: str, pr_number: int, head_sha: str
) -> int:
    """Get the transient CI retry count for a PR.

    Resets to 0 when the head SHA changes (new commits pushed), so the
    transient budget is per-SHA just like the main attempt counter.
    """
    key = _make_transient_attempts_key(repo, pr_number)
    data: dict[str, str] = redis.hgetall(key)
    if not data:
        return 0
    stored_sha = data.get("head_sha", "")
    if stored_sha != head_sha:
        return 0
    try:
        return int(data.get("count", 0))
    except (ValueError, TypeError):
        return 0


def increment_transient_attempts(
    redis: RedisClient, repo: str, pr_number: int, head_sha: str
) -> int:
    """Increment and return the transient CI retry count for a PR.

    Resets to 1 if the stored head SHA differs from head_sha (new commits).
    Sets a 7-day TTL on the key so closed/merged PR counters don't leak.
    """
    key = _make_transient_attempts_key(repo, pr_number)
    # The hget + conditional delete are intentionally outside the pipeline/transaction:
    # the orchestrator is a single instance, so there is no concurrent writer that
    # could race between this delete and the pipeline execute below.  Moving the
    # delete inside the pipeline would require a Lua script to make the
    # read-then-conditional-delete atomic; that complexity isn't warranted here.
    stored_sha = redis.hget(key, "head_sha")
    if stored_sha is not None and stored_sha != head_sha:
        redis.delete(key)
    # pipeline(transaction=True) maps to a Redis MULTI/EXEC block: all three
    # commands execute atomically — either all succeed or none do.  There is no
    # risk of the counter being incremented while head_sha is left stale.
    pipe = redis.pipeline(transaction=True)
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
        run_id = _extract_actions_run_id(check)
        if run_id is not None and run_id not in run_ids:
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
    max_total_attempts: int = 50,
    stale_pending_timeout_seconds: int = 7200,
    legacy_exclusion_predicate: Callable[..., bool] | None = None,
    legacy_exclusion_unavailable: bool = False,
) -> list[PRState]:
    """Discover PRs that need action.

    Filter cascade (ordered by cost, cheapest first):
    1. Exclude PRs reserved for the workflow-control v1 engine.
    2. Skip draft PRs (single boolean field, cheapest check)
    3. Skip PRs with the terminal `orcest:needs-human` label
    4. Skip PRs with active Redis locks (worker in progress)
    5. Skip PRs with a pending task already queued
    6. Skip PRs that exceeded total cross-SHA attempt limit
    7. Route PRs with merge conflicts to ENQUEUE_REBASE.
    8. Skip PRs whose retry budget is exhausted for the current SHA.
    9. Fetch CI status; skip if checks are still pending or absent.
    10. Route by CI + review state: failures -> fix, changes requested -> fix,
       approved + unresolved threads -> followup, approved + clean -> merge
    """
    prs = gh.list_open_prs(repo, token)
    results: list[PRState] = []

    for pr_data in prs:
        number: int = pr_data["number"]
        title: str = pr_data["title"]
        branch: str = pr_data["headRefName"]
        base_branch: str = pr_data.get("baseRefName", "main")
        head_sha: str = pr_data.get("headRefOid", "")
        pr_labels: list[str] = [lbl.get("name", "") for lbl in (pr_data.get("labels") or [])]

        marker_reserved = is_legacy_marker_reserved(pr_data.get("body") or "")
        association_reserved = False
        lookup_unavailable = legacy_exclusion_unavailable
        if not lookup_unavailable and legacy_exclusion_predicate is not None:
            try:
                association_reserved = legacy_exclusion_predicate(
                    change_request_external_id=str(number),
                    deterministic_ref=f"refs/heads/{branch}",
                )
            except Exception:
                # A failed ownership read is not evidence that the v1 engine
                # owns the object. Fail closed, but preserve the distinction
                # so health/metrics can report the outage accurately.
                logger.error(
                    "PR #%d: workflow-control v1 ownership lookup failed; excluding",
                    number,
                    exc_info=True,
                )
                lookup_unavailable = True
        if marker_reserved or association_reserved:
            results.append(
                PRState(
                    number=number,
                    title=title,
                    branch=branch,
                    head_sha=head_sha,
                    action=PRAction.SKIP_V1_OWNED,
                    ci_failures=[],
                    review_threads=[],
                    labels=pr_labels,
                    base_branch=base_branch,
                )
            )
            continue
        if lookup_unavailable:
            results.append(
                PRState(
                    number=number,
                    title=title,
                    branch=branch,
                    head_sha=head_sha,
                    action=PRAction.SKIP_V1_LOOKUP_UNAVAILABLE,
                    ci_failures=[],
                    review_threads=[],
                    labels=pr_labels,
                    base_branch=base_branch,
                )
            )
            continue

        if not head_sha:
            logger.warning(
                "PR #%d: missing head SHA in open-PR snapshot; retrying next poll",
                number,
            )
            results.append(
                PRState(
                    number=number,
                    title=title,
                    branch=branch,
                    head_sha="",
                    action=PRAction.SKIP_PENDING,
                    ci_failures=[],
                    review_threads=[],
                    labels=pr_labels,
                    base_branch=base_branch,
                )
            )
            continue

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

        # Skip if human intervention is required.
        if label_config.needs_human in pr_labels:
            # TTL cliff prevention: refresh exhausted_notified on every SKIP_LABELED cycle
            # while the needs-human label is present and the flag is set. Without this,
            # the 30-day TTL can expire before the operator removes the label, causing the
            # recovery branch to silently miss and the circuit breaker to re-fire instead.
            has_notified = get_exhausted_notified(redis, repo, number)
            if label_config.needs_human in pr_labels and has_notified:
                set_exhausted_notified(redis, repo, number)
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
        if redis.exists(lock_key):
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

        # Skip if a task for this PR is already pending in the queue and
        # still belongs to the current GitHub snapshot. Pending markers from
        # older SHAs are disposable coordination state.
        pending_key = make_pending_task_key(repo, "pr", number)
        pending_metadata = get_pending_task_metadata(redis, repo, "pr", number)
        if pending_metadata is not None and not pending_metadata.snapshot_head_sha:
            logger.info(
                "PR #%d: clearing legacy pending task %s without snapshot metadata",
                number,
                pending_metadata.task_id,
            )
            clear_pending_task_if_matches(redis, repo, "pr", number, pending_metadata.task_id)
        elif pending_metadata is not None and pending_metadata.snapshot_head_sha != head_sha:
            logger.info(
                "PR #%d: clearing pending task %s from old SHA %s (current %s)",
                number,
                pending_metadata.task_id,
                pending_metadata.snapshot_head_sha,
                head_sha,
            )
            clear_pending_task_if_matches(redis, repo, "pr", number, pending_metadata.task_id)
        elif pending_metadata is not None or redis.exists(pending_key):
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

        # Clear legacy attempt-derived backoff state when a new SHA is pushed.
        # SHA-aware backoff below handles transient failures whose attempts
        # hash was intentionally cleared.
        stored_sha_data = redis.hgetall(_make_attempts_key(repo, number))
        if stored_sha_data and stored_sha_data.get("head_sha", "") != head_sha:
            clear_backoff(redis, repo, number)

        backoff_step = get_backoff_step(redis, repo, number)
        if backoff_step is not None:
            backoff_head_sha = get_backoff_head_sha(redis, repo, number)
            if backoff_head_sha and backoff_head_sha != head_sha:
                logger.info(
                    "PR #%d: clearing transient backoff from old SHA %s (current %s)",
                    number,
                    backoff_head_sha,
                    head_sha,
                )
                clear_backoff(redis, repo, number)
                clear_transient_failure_count(redis, repo, number)
            else:
                results.append(
                    PRState(
                        number=number,
                        title=title,
                        branch=branch,
                        head_sha=head_sha,
                        action=PRAction.SKIP_BACKOFF,
                        ci_failures=[],
                        review_threads=[],
                        labels=pr_labels,
                        base_branch=base_branch,
                    )
                )
                continue

        total_attempts = get_total_attempt_count(redis, repo, number)
        if total_attempts >= max_total_attempts:
            logger.warning(
                "PR #%d: total attempts (%d) >= limit (%d), stopping",
                number,
                total_attempts,
                max_total_attempts,
            )
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
        if has_usage_exhausted_cooldown(redis, repo, number):
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

        # Attempts are retry budget, not proof of active work. Active work is
        # represented by the pending marker or lock above.
        attempt_count = get_attempt_count(redis, repo, number, head_sha)
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
        # Route conflicting PRs to rebase. SKIP_MAX_ATTEMPTS above still
        # applies to prevent infinite rebase loops.
        mergeable = str(pr_data.get("mergeable") or "").upper()
        merge_state = str(pr_data.get("mergeStateStatus") or "").upper()
        if mergeable == "CONFLICTING" or merge_state == "DIRTY":
            logger.info(
                "PR #%d has merge conflicts (mergeable=%s, mergeStateStatus=%s), enqueuing rebase",
                number,
                mergeable,
                merge_state,
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

        # Do not update a PR branch just because GitHub reports BEHIND. A
        # BEHIND PR can still be mergeable, and updating it creates a base
        # merge commit that retriggers CI. If branch protection truly requires
        # an up-to-date branch, the merge path handles GitHub's rejection.

        # Check CI status -- wrapped in try/except so a single PR's
        # failure does not crash discovery for all other PRs.
        try:
            checks = gh.get_ci_status(repo, number, token, expected_head_sha=head_sha)
        except gh.GhStaleSnapshotError:
            logger.info(
                "PR #%d: CI snapshot head changed during discovery; retrying next poll",
                number,
                exc_info=True,
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

        ci_classification = classify_ci_checks(checks)
        ci_failures = ci_classification.failures
        suppressed_self_cancelled_failures = [
            c
            for c in ci_failures
            if _is_self_cancelled_stale_failure(redis, repo, number, head_sha, c)
        ]
        if suppressed_self_cancelled_failures:
            ci_failures = [
                c
                for c in ci_failures
                if not _is_self_cancelled_stale_failure(redis, repo, number, head_sha, c)
            ]
            logger.info(
                "PR #%d: suppressing %d CANCELLED check(s) from stale-check self-cancellation",
                number,
                len(suppressed_self_cancelled_failures),
            )
        ci_pending = ci_classification.pending

        if suppressed_self_cancelled_failures and not ci_failures and not ci_pending:
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

        if ci_pending and not ci_failures:
            # Only skip as pending if no checks have failed yet.
            # If there are already failures, enqueue a fix immediately
            # rather than waiting for other checks to finish.
            ci_pending = _fill_pending_first_seen_timestamps(
                redis,
                repo,
                number,
                head_sha,
                ci_pending,
                ttl_seconds=max(stale_pending_timeout_seconds * 2, 24 * 3600),
            )
            all_stale, stale_run_ids = _check_stale_pending(
                ci_pending, stale_pending_timeout_seconds
            )
            if all_stale:
                # All pending checks have exceeded the staleness timeout.
                # Re-trigger what we can; if no run IDs are extractable
                # (e.g. StatusContext checks), the loop just logs and moves on.
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
            threads: list | None = None
            try:
                threads = gh.get_unresolved_review_threads(
                    repo, number, token, expected_head_sha=head_sha
                )
            except gh.GhStaleSnapshotError:
                logger.info(
                    "PR #%d: review-thread snapshot head changed during discovery; "
                    "retrying next poll",
                    number,
                    exc_info=True,
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
            except Exception:
                logger.warning(
                    "Failed to fetch review threads for PR #%d with CHANGES_REQUESTED, "
                    "skipping until review context can be fetched",
                    number,
                    exc_info=True,
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

            if threads is not None and not threads:
                # All review threads resolved but reviewDecision is stale
                # (GitHub doesn't clear CHANGES_REQUESTED when threads are
                # resolved — only a new approving review clears it).
                # Re-trigger claude-review so it can submit a fresh APPROVED
                # or raise new objections.
                review_run_id = _get_claude_review_run_id(checks)
                retrigger_sha = get_review_retrigger_sha(redis, repo, number)

                if review_run_id is not None and retrigger_sha != head_sha:
                    logger.info(
                        "PR #%d has CHANGES_REQUESTED but all threads resolved, "
                        "re-triggering claude-review (run %d)",
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
                else:
                    # Already re-triggered for this SHA or no claude-review
                    # run found — nothing actionable, skip.
                    logger.info(
                        "PR #%d has CHANGES_REQUESTED but all threads resolved, skipping",
                        number,
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
            else:
                results.append(
                    PRState(
                        number=number,
                        title=title,
                        branch=branch,
                        head_sha=head_sha,
                        action=PRAction.ENQUEUE_FIX,
                        ci_failures=[],
                        review_threads=threads or [],
                        labels=pr_labels,
                        base_branch=base_branch,
                    )
                )
        else:
            if review_decision == "APPROVED":
                try:
                    review_snapshot = gh.get_review_snapshot(
                        repo, number, token, expected_head_sha=head_sha
                    )
                except gh.GhStaleSnapshotError:
                    logger.info(
                        "PR #%d: review snapshot head changed during discovery; retrying next poll",
                        number,
                        exc_info=True,
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
                except Exception:
                    logger.warning(
                        "PR #%d: review evidence could not be verified; retrying next poll",
                        number,
                        exc_info=True,
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
                if review_snapshot.review_decision != review_decision:
                    logger.info(
                        "PR #%d: aggregate review decision changed during discovery "
                        "(listed=%s snapshot=%s); retrying next poll",
                        number,
                        review_decision,
                        review_snapshot.review_decision,
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
                if not review_snapshot.has_current_head_approval:
                    logger.info(
                        "PR #%d: aggregate approval ignored because it targets an older "
                        "head than %s; using no-formal-verdict path",
                        number,
                        head_sha,
                    )
                    review_decision = ""

            if review_decision == "APPROVED":
                # CI green + approved — check for unresolved threads
                try:
                    threads = gh.get_unresolved_review_threads(
                        repo, number, token, expected_head_sha=head_sha
                    )
                except gh.GhStaleSnapshotError:
                    logger.info(
                        "PR #%d: review-thread snapshot head changed during discovery; "
                        "retrying next poll",
                        number,
                        exc_info=True,
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
                except Exception:
                    # Cannot verify thread state — do NOT merge.
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
                            action=PRAction.SKIP_PENDING,
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
                        "PR #%d is approved but has %d unresolved thread(s), "
                        "enqueuing followup triage",
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
                    if pr_data.get("mergeable") == "UNKNOWN":
                        logger.debug(
                            "PR #%d is approved and green but mergeability is UNKNOWN, "
                            "skipping merge",
                            number,
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
                    threads = gh.get_unresolved_review_threads(
                        repo, number, token, expected_head_sha=head_sha
                    )
                except gh.GhStaleSnapshotError:
                    logger.info(
                        "PR #%d: review-thread snapshot head changed during discovery; "
                        "retrying next poll",
                        number,
                        exc_info=True,
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
                except Exception:
                    logger.warning(
                        "Failed to fetch review threads for PR #%d, skipping",
                        number,
                        exc_info=True,
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
                    retrigger_sha = get_review_retrigger_sha(redis, repo, number)

                    if review_run_id is not None and retrigger_sha != head_sha:
                        # claude-review passed but no formal review — re-trigger
                        logger.info(
                            "PR #%d: claude-review passed but no formal review, "
                            "will re-trigger run %d",
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
                        # Already re-triggered for this SHA, still no review. Route
                        # to SKIP_MAX_ATTEMPTS, which backs off and retries later
                        # (it no longer escalates to a human).
                        logger.warning(
                            "PR #%d: claude-review re-trigger exhausted (SHA %s), backing off",
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
                        # No actionable claude-review run (absent, not SUCCESS, or missing run URL)
                        # — normal SKIP_GREEN
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
