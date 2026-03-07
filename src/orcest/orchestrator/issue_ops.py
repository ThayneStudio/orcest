"""Issue discovery and state management.

Discovers open issues labeled `orcest:ready`, applies a filter cascade
(labels -> locks -> attempts), and returns a list of IssueState objects
with recommended actions.
"""

import logging
from dataclasses import dataclass
from enum import Enum
from typing import cast

from orcest.orchestrator import gh
from orcest.shared.config import LabelConfig
from orcest.shared.coordination import make_issue_lock_key, make_pending_task_key
from orcest.shared.redis_client import RedisClient

logger = logging.getLogger(__name__)

# Sentinel value for issue attempt tracking (issues don't have a head_sha).
_ISSUE_SHA_SENTINEL = "issue"


class IssueAction(str, Enum):
    """What the orchestrator should do with an issue."""

    ENQUEUE_IMPLEMENT = "enqueue_implement"
    SKIP_LOCKED = "skip_locked"
    SKIP_LABELED = "skip_labeled"  # Terminal label (blocked/needs-human)
    SKIP_QUEUED = "skip_queued"  # Task already pending in queue
    SKIP_ACTIVE = "skip_active"  # Task in flight (attempts > 0, no terminal label)
    SKIP_MAX_ATTEMPTS = "skip_max_attempts"


@dataclass
class IssueState:
    """Analyzed state of an issue."""

    number: int
    title: str
    body: str
    action: IssueAction
    labels: list[str]


def _make_attempts_key(issue_number: int) -> str:
    """Redis key for tracking task attempt count per issue."""
    return f"issue:{issue_number}:attempts"


def get_attempt_count(redis: RedisClient, issue_number: int) -> int:
    """Get the current attempt count for an issue."""
    key = _make_attempts_key(issue_number)
    data: dict[str, str] = cast(dict[str, str], redis.client.hgetall(key))
    if not data:
        return 0
    try:
        return int(data.get("count", 0))
    except (ValueError, TypeError):
        return 0


def increment_attempts(redis: RedisClient, issue_number: int) -> int:
    """Increment and return the attempt count for an issue.

    Sets a 7-day TTL so closed issue counters don't leak memory.
    """
    key = _make_attempts_key(issue_number)
    pipe = redis.client.pipeline(transaction=True)
    pipe.hincrby(key, "count", 1)
    pipe.hset(key, "head_sha", _ISSUE_SHA_SENTINEL)
    pipe.expire(key, 7 * 24 * 3600)
    results = pipe.execute()
    return results[0]


def clear_attempts(redis: RedisClient, issue_number: int) -> None:
    """Clear the attempt counter for an issue."""
    redis.client.delete(_make_attempts_key(issue_number))


def discover_actionable_issues(
    repo: str,
    token: str,
    redis: RedisClient,
    label_config: LabelConfig,
    max_attempts: int = 3,
) -> list[IssueState]:
    """Discover issues labeled `orcest:ready` that need implementation.

    Filter cascade:
    1. Fetch issues with the `orcest:ready` label
    2. Skip if terminal orcest label present (blocked/needs-human)
    3. Skip if Redis lock exists (worker in progress)
    4. Skip if max attempts reached
    5. Skip if task already in flight (attempts > 0)
    6. Everything else -> ENQUEUE_IMPLEMENT
    """
    issues = gh.list_labeled_issues(repo, label_config.ready, token)
    results: list[IssueState] = []

    terminal_labels = {
        label_config.blocked,
        label_config.needs_human,
    }

    for issue_data in issues:
        number: int = issue_data["number"]
        title: str = issue_data["title"]
        body: str = issue_data.get("body") or ""
        issue_labels: list[str] = [
            name for lbl in (issue_data.get("labels") or []) if (name := lbl.get("name"))
        ]

        # Skip if terminal orcest label present (blocked/needs-human)
        if any(label in terminal_labels for label in issue_labels):
            results.append(
                IssueState(
                    number=number,
                    title=title,
                    body=body,
                    action=IssueAction.SKIP_LABELED,
                    labels=issue_labels,
                )
            )
            continue

        # Skip if locked in Redis
        lock_key = make_issue_lock_key(repo, number)
        if redis.client.exists(lock_key):
            results.append(
                IssueState(
                    number=number,
                    title=title,
                    body=body,
                    action=IssueAction.SKIP_LOCKED,
                    labels=issue_labels,
                )
            )
            continue

        # Skip if a task for this issue is already pending in the queue
        pending_key = make_pending_task_key(repo, "issue", number)
        if redis.client.exists(pending_key):
            results.append(
                IssueState(
                    number=number,
                    title=title,
                    body=body,
                    action=IssueAction.SKIP_QUEUED,
                    labels=issue_labels,
                )
            )
            continue

        # Skip if task already in flight or max attempts reached
        attempt_count = get_attempt_count(redis, number)
        if attempt_count >= max_attempts:
            logger.warning(
                "Issue #%d has reached %d attempts (max %d), skipping",
                number,
                attempt_count,
                max_attempts,
            )
            results.append(
                IssueState(
                    number=number,
                    title=title,
                    body=body,
                    action=IssueAction.SKIP_MAX_ATTEMPTS,
                    labels=issue_labels,
                )
            )
            continue
        if attempt_count > 0:
            results.append(
                IssueState(
                    number=number,
                    title=title,
                    body=body,
                    action=IssueAction.SKIP_ACTIVE,
                    labels=issue_labels,
                )
            )
            continue

        # Ready for implementation
        results.append(
            IssueState(
                number=number,
                title=title,
                body=body,
                action=IssueAction.ENQUEUE_IMPLEMENT,
                labels=issue_labels,
            )
        )

    return results
