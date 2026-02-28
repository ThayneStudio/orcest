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


class PRAction(str, Enum):
    """What the orchestrator should do with a PR."""
    ENQUEUE_FIX = "enqueue_fix"      # CI failing or review feedback
    SKIP_LOCKED = "skip_locked"      # Another worker already on it
    SKIP_LABELED = "skip_labeled"    # Already queued/in-progress
    SKIP_GREEN = "skip_green"        # CI passing, nothing to do
    SKIP_DRAFT = "skip_draft"        # Draft PR, ignore
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
    review_comments: list[dict]      # Actionable review comments
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
    """Increment and return the attempt count for a PR."""
    key = _make_attempts_key(pr_number)
    pipe = redis.client.pipeline()
    pipe.hincrby(key, "count", 1)
    pipe.hset(key, "head_sha", head_sha)
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
    2. Skip PRs with active Redis locks (worker in progress)
    3. Skip PRs that have exhausted their retry budget
    4. Identify PRs with CI failures or review feedback
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
        pr_labels: list[str] = [lbl["name"] for lbl in pr_data.get("labels", [])]

        # Skip if already labeled by orcest
        if any(label in orcest_labels for label in pr_labels):
            results.append(PRState(
                number=number, title=title, branch=branch,
                head_sha=head_sha, action=PRAction.SKIP_LABELED,
                ci_failures=[], review_comments=[], labels=pr_labels,
            ))
            continue

        # Skip if locked in Redis
        lock_key = make_pr_lock_key(number)
        if redis.client.exists(lock_key):
            results.append(PRState(
                number=number, title=title, branch=branch,
                head_sha=head_sha, action=PRAction.SKIP_LOCKED,
                ci_failures=[], review_comments=[], labels=pr_labels,
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
            if (c.get("conclusion") or "").upper() == "FAILURE"
        ]

        # Check review state
        review_decision = pr_data.get("reviewDecision", "")
        has_actionable_reviews = review_decision == "CHANGES_REQUESTED"

        if ci_failures or has_actionable_reviews:
            # Check attempt budget before enqueuing
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
                    ci_failures=ci_failures, review_comments=[],
                    labels=pr_labels,
                ))
                continue

            results.append(PRState(
                number=number, title=title, branch=branch,
                head_sha=head_sha, action=PRAction.ENQUEUE_FIX,
                ci_failures=ci_failures,
                review_comments=[],  # Populated by task_publisher
                labels=pr_labels,
            ))
        else:
            results.append(PRState(
                number=number, title=title, branch=branch,
                head_sha=head_sha, action=PRAction.SKIP_GREEN,
                ci_failures=[], review_comments=[], labels=pr_labels,
            ))

    return results
