"""PR discovery and state management.

Discovers open PRs, applies a filter cascade (labels -> locks -> CI -> reviews),
and returns a list of PRState objects with recommended actions. The orchestrator
main loop acts on these recommendations.
"""

import logging
from dataclasses import dataclass
from enum import Enum

from orcest.orchestrator import gh
from orcest.shared.config import LabelConfig
from orcest.shared.coordination import make_pr_lock_key
from orcest.shared.redis_client import RedisClient

logger = logging.getLogger(__name__)

# Terminal CheckRun conclusions that indicate CI is not green.
# "neutral" and "skipped" are excluded as non-blocking outcomes.
_FAILURE_CONCLUSIONS = frozenset({
    "FAILURE", "CANCELLED", "TIMED_OUT", "ACTION_REQUIRED", "STALE",
    "STARTUP_FAILURE",
})


class PRAction(str, Enum):
    """What the orchestrator should do with a PR."""
    MERGE = "merge"                  # Ready to merge (CI green + approved + no unresolved threads)
    ENQUEUE_FIX = "enqueue_fix"      # CI failing or review feedback
    ENQUEUE_FOLLOWUP = "enqueue_followup"  # Approved but unresolved threads — triage into issues
    SKIP_LOCKED = "skip_locked"      # Another worker already on it
    SKIP_LABELED = "skip_labeled"    # Already queued/in-progress
    SKIP_GREEN = "skip_green"        # CI passing, nothing to do
    SKIP_DRAFT = "skip_draft"        # Draft PR, ignore
    SKIP_PENDING = "skip_pending"    # CI checks still running
    SKIP_MAX_ATTEMPTS = "skip_max_attempts"  # Exhausted retry budget


@dataclass
class PRState:
    """Analyzed state of a PR."""
    number: int
    title: str
    branch: str
    head_sha: str
    action: PRAction
    ci_failures: list[dict]          # Failed check runs
    review_threads: list[dict]       # Actionable review comments
    labels: list[str]


def _make_attempts_key(pr_number: int) -> str:
    """Redis key for tracking task attempt count per PR."""
    return f"pr:{pr_number}:attempts"


def get_attempt_count(redis: RedisClient, pr_number: int, head_sha: str) -> int:
    """Get the current attempt count for a PR.

    If the stored head SHA differs from the current one (new commits pushed),
    the counter is reset to 0.
    """
    key = _make_attempts_key(pr_number)
    data = redis.client.hgetall(key)
    if not data:
        return 0
    stored_sha = data.get("head_sha", "")
    if stored_sha != head_sha:
        # New commits pushed — reset counter
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


def discover_actionable_prs(
    repo: str,
    token: str,
    redis: RedisClient,
    label_config: LabelConfig,
    max_attempts: int = 3,
) -> list[PRState]:
    """Discover PRs that need action.

    Filter cascade (ordered by cost, cheapest first):
    1. Skip PRs with orcest labels (already being handled)
    2. Skip draft PRs
    3. Skip PRs with active Redis locks (worker in progress)
    4. Fetch CI status; skip if checks are still pending
    5. Route by CI + review state: failures -> fix, changes requested -> fix,
       approved + unresolved threads -> followup, approved + clean -> merge
    6. Within each enqueue branch, check attempt budget before enqueuing
    """
    prs = gh.list_open_prs(repo, token)
    results: list[PRState] = []

    orcest_labels = {
        label_config.queued,
        label_config.in_progress,
        label_config.blocked,
        label_config.needs_human,
    }

    for pr_data in prs:
        number: int = pr_data["number"]
        title: str = pr_data["title"]
        branch: str = pr_data["headRefName"]
        head_sha: str = pr_data.get("headRefOid", "")
        pr_labels: list[str] = [
            lbl.get("name", "") for lbl in (pr_data.get("labels") or [])
        ]

        # Skip if already labeled by orcest
        if any(label in orcest_labels for label in pr_labels):
            results.append(PRState(
                number=number, title=title, branch=branch,
                head_sha=head_sha, action=PRAction.SKIP_LABELED,
                ci_failures=[], review_threads=[], labels=pr_labels,
            ))
            continue

        # Skip draft PRs
        if pr_data.get("isDraft"):
            results.append(PRState(
                number=number, title=title, branch=branch,
                head_sha=head_sha, action=PRAction.SKIP_DRAFT,
                ci_failures=[], review_threads=[], labels=pr_labels,
            ))
            continue

        # Skip if locked in Redis
        lock_key = make_pr_lock_key(number)
        if redis.client.exists(lock_key):
            results.append(PRState(
                number=number, title=title, branch=branch,
                head_sha=head_sha, action=PRAction.SKIP_LOCKED,
                ci_failures=[], review_threads=[], labels=pr_labels,
            ))
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

        ci_failures = [
            c for c in checks
            if (c.get("conclusion") or "").upper() in _FAILURE_CONCLUSIONS
            or (
                not c.get("conclusion")
                and (c.get("state") or "").upper() in ("FAILURE", "ERROR")
            )
        ]
        # A check is pending if it hasn't reached a terminal state.
        # statusCheckRollup can include both CheckRun objects (which have
        # "conclusion") and StatusContext objects (which have "state").
        # - CheckRun: pending when "conclusion" is absent/empty (still running)
        # - StatusContext: pending when "state" is absent, empty, or "PENDING"
        ci_pending = [
            c for c in checks
            if not c.get("conclusion")
            and (c.get("state") or "").upper() in ("", "PENDING", "EXPECTED")
        ]

        if ci_pending and not ci_failures:
            # Only skip as pending if no checks have failed yet.
            # If there are already failures, enqueue a fix immediately
            # rather than waiting for other checks to finish.
            logger.debug(
                "PR #%d has %d check(s) still pending, skipping",
                number, len(ci_pending),
            )
            results.append(PRState(
                number=number, title=title, branch=branch,
                head_sha=head_sha, action=PRAction.SKIP_PENDING,
                ci_failures=[], review_threads=[], labels=pr_labels,
            ))
            continue

        # Check review state
        review_decision = pr_data.get("reviewDecision", "")

        if ci_failures:
            # CI failing — enqueue fix (priority over review state)
            attempts = get_attempt_count(redis, number, head_sha)
            if attempts >= max_attempts:
                logger.warning(
                    "PR #%d has reached %d attempts (max %d), skipping",
                    number, attempts, max_attempts,
                )
                results.append(PRState(
                    number=number, title=title, branch=branch,
                    head_sha=head_sha,
                    action=PRAction.SKIP_MAX_ATTEMPTS,
                    ci_failures=ci_failures, review_threads=[],
                    labels=pr_labels,
                ))
                continue

            results.append(PRState(
                number=number, title=title, branch=branch,
                head_sha=head_sha, action=PRAction.ENQUEUE_FIX,
                ci_failures=ci_failures, review_threads=[],
                labels=pr_labels,
            ))
        elif review_decision == "CHANGES_REQUESTED":
            # CI green but reviewer requested changes — enqueue fix
            attempts = get_attempt_count(redis, number, head_sha)
            if attempts >= max_attempts:
                logger.warning(
                    "PR #%d has reached %d attempts (max %d), skipping",
                    number, attempts, max_attempts,
                )
                results.append(PRState(
                    number=number, title=title, branch=branch,
                    head_sha=head_sha,
                    action=PRAction.SKIP_MAX_ATTEMPTS,
                    ci_failures=[], review_threads=[],
                    labels=pr_labels,
                ))
                continue

            # Fetch unresolved review threads for worker prompt context
            try:
                threads = gh.get_unresolved_review_threads(
                    repo, number, token
                )
            except Exception:
                logger.warning(
                    "Failed to fetch review threads for PR #%d, "
                    "enqueuing without thread details",
                    number, exc_info=True,
                )
                threads = []

            results.append(PRState(
                number=number, title=title, branch=branch,
                head_sha=head_sha, action=PRAction.ENQUEUE_FIX,
                ci_failures=[], review_threads=threads,
                labels=pr_labels,
            ))
        elif review_decision == "APPROVED":
            # CI green + approved — check for unresolved threads
            try:
                threads = gh.get_unresolved_review_threads(
                    repo, number, token
                )
            except Exception:
                # Cannot verify thread state — do NOT merge. Fall through
                # to SKIP_GREEN so the PR stays visible and gets retried
                # on the next poll cycle.
                logger.warning(
                    "Failed to fetch review threads for PR #%d, "
                    "skipping merge until threads can be verified",
                    number, exc_info=True,
                )
                results.append(PRState(
                    number=number, title=title, branch=branch,
                    head_sha=head_sha, action=PRAction.SKIP_GREEN,
                    ci_failures=[], review_threads=[], labels=pr_labels,
                ))
                continue

            if threads:
                # Approved but unresolved threads — triage into issues
                attempts = get_attempt_count(redis, number, head_sha)
                if attempts >= max_attempts:
                    logger.warning(
                        "PR #%d has reached %d attempts (max %d), skipping",
                        number, attempts, max_attempts,
                    )
                    results.append(PRState(
                        number=number, title=title, branch=branch,
                        head_sha=head_sha,
                        action=PRAction.SKIP_MAX_ATTEMPTS,
                        ci_failures=[], review_threads=[],
                        labels=pr_labels,
                    ))
                    continue

                logger.info(
                    "PR #%d is approved but has %d unresolved thread(s), "
                    "enqueuing followup triage",
                    number, len(threads),
                )
                results.append(PRState(
                    number=number, title=title, branch=branch,
                    head_sha=head_sha,
                    action=PRAction.ENQUEUE_FOLLOWUP,
                    ci_failures=[], review_threads=threads,
                    labels=pr_labels,
                ))
            else:
                # All clear — merge
                results.append(PRState(
                    number=number, title=title, branch=branch,
                    head_sha=head_sha, action=PRAction.MERGE,
                    ci_failures=[], review_threads=[],
                    labels=pr_labels,
                ))
        else:
            # CI green, no actionable review state
            results.append(PRState(
                number=number, title=title, branch=branch,
                head_sha=head_sha, action=PRAction.SKIP_GREEN,
                ci_failures=[], review_threads=[], labels=pr_labels,
            ))

    return results
