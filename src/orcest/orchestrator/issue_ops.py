"""Issue discovery and state management.

Discovers open issues labeled `orcest:ready`, applies a filter cascade
(labels -> locks -> attempts), and returns a list of IssueState objects
with recommended actions.
"""

import logging
from dataclasses import dataclass, field
from enum import Enum

from orcest.orchestrator import gh
from orcest.orchestrator.issue_deps import (
    fetch_blocker_states,
    native_open_blockers,
    open_blockers,
    parse_blocker_refs,
)
from orcest.shared.config import IssueDeliveryVerifierConfig, LabelConfig
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
    SKIP_USAGE_COOLDOWN = "skip_usage_cooldown"
    SKIP_DELIVERY_COOLDOWN = "skip_delivery_cooldown"
    SKIP_VERIFYING = "skip_verifying"  # Durable delivery verification is in flight
    SKIP_DEPENDENCY = "skip_dependency"  # One or more prerequisite issues still open


@dataclass
class IssueState:
    """Analyzed state of an issue."""

    number: int
    title: str
    body: str
    action: IssueAction
    labels: list[str]
    # Display refs of still-open blockers: "#N" same-repo, "owner/repo#N"
    # for cross-repo native dependencies.
    open_blockers: list[str] = field(default_factory=list)


def _make_attempts_key(repo: str, issue_number: int) -> str:
    """Redis key for tracking task attempt count per issue."""
    return f"issue:{repo}:{issue_number}:attempts"


def get_attempt_count(redis: RedisClient, repo: str, issue_number: int) -> int:
    """Get the current attempt count for an issue."""
    key = _make_attempts_key(repo, issue_number)
    data: dict[str, str] = redis.hgetall(key)
    if not data:
        return 0
    try:
        return int(data.get("count", 0))
    except (ValueError, TypeError):
        return 0


def increment_attempts(redis: RedisClient, repo: str, issue_number: int) -> int:
    """Increment and return the attempt count for an issue.

    Sets a 7-day TTL so closed issue counters don't leak memory.
    """
    key = _make_attempts_key(repo, issue_number)
    pipe = redis.pipeline(transaction=True)
    pipe.hincrby(key, "count", 1)
    pipe.hset(key, "head_sha", _ISSUE_SHA_SENTINEL)
    pipe.expire(key, 7 * 24 * 3600)
    results = pipe.execute()
    return results[0]


def clear_attempts(redis: RedisClient, repo: str, issue_number: int) -> None:
    """Clear the attempt counter for an issue."""
    redis.delete(_make_attempts_key(repo, issue_number))


def _make_usage_cooldown_key(repo: str, issue_number: int) -> str:
    """Redis key for the USAGE_EXHAUSTED cooldown marker."""
    return f"issue:{repo}:{issue_number}:usage_cooldown"


def set_usage_exhausted_cooldown(
    redis: RedisClient, repo: str, issue_number: int, ttl_seconds: int = 1800
) -> None:
    """Set a cooldown marker so the issue is not immediately re-enqueued."""
    redis.set_ex(_make_usage_cooldown_key(repo, issue_number), "1", ttl_seconds)


def has_usage_exhausted_cooldown(redis: RedisClient, repo: str, issue_number: int) -> bool:
    """Return True if a USAGE_EXHAUSTED cooldown is still active for this issue."""
    return bool(redis.exists(_make_usage_cooldown_key(repo, issue_number)))


def discover_actionable_issues(
    repo: str,
    token: str,
    redis: RedisClient,
    label_config: LabelConfig,
    max_attempts: int = 3,
    issue_delivery_verifier: IssueDeliveryVerifierConfig | None = None,
) -> list[IssueState]:
    """Discover issues labeled `orcest:ready` that need implementation.

    Filter cascade:
    1. Fetch issues with the `orcest:ready` label
    2. Skip if terminal orcest label present (blocked/needs-human)
    3. Skip if Redis lock exists (worker in progress)
    4. Skip if usage-exhausted cooldown is active
    5. Skip if a nonterminal delivery-verification job holds the dispatch barrier
       (logs a warning if issue_delivery_verifier.enabled is false, since the
       barrier is then no longer being processed or cleared)
    6. Skip if ineffective-delivery cooldown is active
    7. Skip if max attempts reached (issue attempt hash or INEFFECTIVE
       delivery-verification generations, whichever is larger)
    8. Skip if task already in flight (attempts > 0 with a pending marker)
    9. Clear orphaned attempts (attempts > 0 without a pending marker and
       without INEFFECTIVE delivery history for this issue)
    10. Skip if task already pending in the queue
    11. Skip if any GitHub-native blocked-by dependency is still open
    12. Skip if any body-declared blocker issue is still open
    13. Everything else -> ENQUEUE_IMPLEMENT
    """
    from orcest.orchestrator.issue_delivery import (
        count_ineffective_delivery_generations,
        has_delivery_retry_cooldown,
        has_issue_dispatch_barrier,
    )

    verifier_config = issue_delivery_verifier or IssueDeliveryVerifierConfig()
    issues = gh.list_labeled_issues(repo, label_config.ready, token)
    results: list[IssueState] = []

    terminal_labels = {
        label_config.blocked,
        label_config.needs_human,
    }

    # Cache of blocker issue states for the duration of this discovery cycle.
    # If 10 dependent issues all reference #5, we hit gh once.
    blocker_state_cache: dict[int, str] = {}

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
        if redis.exists(lock_key):
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

        pending_key = make_pending_task_key(repo, "issue", number)

        if has_usage_exhausted_cooldown(redis, repo, number):
            results.append(
                IssueState(
                    number=number,
                    title=title,
                    body=body,
                    action=IssueAction.SKIP_USAGE_COOLDOWN,
                    labels=issue_labels,
                )
            )
            continue

        if has_issue_dispatch_barrier(redis, repo, number):
            if not verifier_config.enabled:
                logger.warning(
                    "Issue #%d in %s has an in-flight delivery-verification dispatch "
                    "barrier but issue_delivery_verifier.enabled is false, so it is not "
                    "being processed or cleared; discovery stays blocked until the "
                    "barrier is drained. See "
                    "docs/operations/issue-delivery-verification-rollback.md.",
                    number,
                    repo,
                )
            results.append(
                IssueState(
                    number=number,
                    title=title,
                    body=body,
                    action=IssueAction.SKIP_VERIFYING,
                    labels=issue_labels,
                )
            )
            continue

        if has_delivery_retry_cooldown(redis, repo, number):
            results.append(
                IssueState(
                    number=number,
                    title=title,
                    body=body,
                    action=IssueAction.SKIP_DELIVERY_COOLDOWN,
                    labels=issue_labels,
                )
            )
            continue

        # Attempts are retry budget, not proof of active work. If Redis has
        # attempts but no pending marker, that is usually a crash-orphan and
        # we clear it so GitHub-source-of-truth work cannot get stuck.
        # INEFFECTIVE delivery verification is not an orphan: admission
        # clears the pending marker (and workers delete the attempts hash)
        # long before the saga resolves, so those generations must keep
        # counting toward max_attempts.
        ineffective_generations = count_ineffective_delivery_generations(
            redis, repo, number, limit=max_attempts
        )
        attempt_count = get_attempt_count(redis, repo, number)
        if attempt_count > 0 and not redis.exists(pending_key) and ineffective_generations == 0:
            logger.info(
                "Issue #%d has %d attempt(s) but no pending task marker; "
                "clearing orphaned attempts",
                number,
                attempt_count,
            )
            clear_attempts(redis, repo, number)
            attempt_count = 0

        # Skip if task already in flight or max attempts reached
        spent_attempts = max(attempt_count, ineffective_generations)
        if spent_attempts >= max_attempts:
            logger.warning(
                "Issue #%d has reached %d attempts (max %d), skipping",
                number,
                spent_attempts,
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
        if attempt_count > 0 and redis.exists(pending_key):
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

        # Skip if a task for this issue is already pending in the queue
        if redis.exists(pending_key):
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

        # Skip if any GitHub-native blocked-by relationship is still open.
        # Blocker states arrived inline with the issue listing, so this
        # costs nothing and runs before body-declared refs (which cost gh
        # API calls to resolve).
        native_open = native_open_blockers(issue_data, repo)
        if native_open:
            results.append(
                IssueState(
                    number=number,
                    title=title,
                    body=body,
                    action=IssueAction.SKIP_DEPENDENCY,
                    labels=issue_labels,
                    open_blockers=native_open,
                )
            )
            continue

        # Skip if any body-declared blocker issue is still open.
        # Position matters: blocker resolution costs gh API calls, so it
        # runs after all the cheap Redis skips and the free native check.
        blocker_refs = parse_blocker_refs(body)
        if blocker_refs:
            # Native data already told us the state of any same-repo blocker
            # it listed -- seed the cache so those refs cost no gh call.
            # The cache is keyed by bare same-repo issue numbers, so only
            # blockers explicitly attributed to this repo may seed it; a
            # cross-repo or unattributed blocker sharing a number with a
            # same-repo body ref must fall through to the gh lookup.
            for blocker in issue_data.get("blocked_by") or []:
                state = blocker.get("state")
                if blocker.get("repo") == repo and isinstance(state, str):
                    blocker_state_cache.setdefault(
                        blocker["number"],
                        "closed" if state.upper() == "CLOSED" else "open",
                    )
            states = fetch_blocker_states(repo, blocker_refs, token, blocker_state_cache)
            still_open = open_blockers(blocker_refs, states)
            if still_open:
                results.append(
                    IssueState(
                        number=number,
                        title=title,
                        body=body,
                        action=IssueAction.SKIP_DEPENDENCY,
                        labels=issue_labels,
                        open_blockers=[f"#{n}" for n in still_open],
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
