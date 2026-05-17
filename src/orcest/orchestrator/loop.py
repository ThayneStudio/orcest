"""Orchestrator main loop.

Polls GitHub for actionable PRs, enqueues fix tasks to Redis, and consumes
results from workers. Uses graceful shutdown on SIGTERM/SIGINT with
interruptible sleep (1-second chunks) for responsive termination.
"""

import json
import logging
import math
import re
import signal
import sys
import time

from orcest.orchestrator import gh
from orcest.orchestrator.deployment import DeploymentError, run_deployment
from orcest.orchestrator.gh import GhRateLimitError
from orcest.orchestrator.issue_ops import (
    IssueAction,
    clear_attempts as clear_issue_attempts,
    discover_actionable_issues,
    set_usage_exhausted_cooldown as set_issue_usage_exhausted_cooldown,
)
from orcest.orchestrator.pr_ops import (
    PRAction,
    PRState,
    clear_attempts,
    clear_attempts_if_head_sha,
    clear_review_retrigger,
    clear_total_attempts,
    discover_actionable_prs,
    get_stale_retrigger_sha,
    increment_total_attempts,
    record_self_cancelled_stale_runs,
    set_review_retrigger_sha,
    set_stale_retrigger_sha,
    set_usage_exhausted_cooldown,
)
from orcest.orchestrator.task_publisher import (
    publish_fix_task,
    publish_followup_task,
    publish_issue_task,
    publish_rebase_task,
    rerun_all_transient_ci,
)
from orcest.orchestrator.provider_pool import ProviderPool
from orcest.orchestrator.usage_check import get_token_reset_time
from orcest.shared.config import LabelConfig, OrchestratorConfig, ProjectConfig
from orcest.shared.coordination import (
    clear_backoff,
    clear_pending_task_if_matches,
    clear_transient_failure_count,
    compute_pending_task_ttl,
    get_pending_task,
    increment_transient_failure_count,
    set_backoff_cooldown,
)
from orcest.shared.logging import setup_logging
from orcest.shared.models import (
    CONSUMER_GROUP,
    TRANSIENT_SUMMARY_PREFIX,
    ResultStatus,
    TaskResult,
)
from orcest.shared.redis_client import RedisClient

RESULTS_STREAM = "results"
RESULTS_GROUP = "orchestrator"

# Observability counter incremented when _select_claude_token returns None
# (all tokens in the project's pool are cooling).  TTL keeps it self-cleaning
# so operators can SCAN the keyspace without finding stale leftovers.
_TOKEN_EXHAUSTED_SKIP_KEY = "tokens:exhausted_skip"
_TOKEN_EXHAUSTED_SKIP_TTL_SECONDS = 24 * 3600  # 24 hours
_USAGE_EXHAUSTED_RESULT_KEY = "tokens:usage_exhausted_result"
_USAGE_EXHAUSTED_RESULT_TTL_SECONDS = 24 * 3600  # 24 hours
_USAGE_EXHAUSTED_COOLDOWN_SECONDS = 1800
_USAGE_EXHAUSTED_PROCESSED_TTL_SECONDS = 24 * 3600
_TRANSIENT_FAILURE_PROCESSED_TTL_SECONDS = 24 * 3600
_REVIEW_RERUN_FAILURE_COOLDOWN_SECONDS = 15 * 60
_REVIEW_RERUN_FAILURE_TTL_SECONDS = 7 * 24 * 3600
_MAX_REVIEW_RERUN_FAILURES = 3
_FAILURE_CONCLUSIONS = frozenset(
    {"FAILURE", "CANCELLED", "TIMED_OUT", "ACTION_REQUIRED", "STALE", "STARTUP_FAILURE"}
)

# Patterns matching Go HTTP / network errors surfaced by the `gh` CLI.
# Used to distinguish transient network failures from permanent merge errors.
_MERGE_NETWORK_PATTERNS: list[re.Pattern[str]] = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"timed?\s*out",
        r"ETIMEDOUT",
        r"connection reset",
        r"ECONNRESET",
        r"ECONNREFUSED",
        r"dial tcp",
        r"TLS handshake",
        r"socket hang up",
        r"no such host",
        r"i/o timeout",
        r"network is unreachable",
        r"HTTP 50[234]",
    ]
]


def _make_usage_exhausted_processed_key(task_id: str) -> str:
    """Redis key tracking USAGE_EXHAUSTED accounting already done for a task."""
    return f"result:{task_id}:usage_exhausted_processed"


def _make_transient_failure_processed_key(task_id: str) -> str:
    """Redis key tracking transient-failure accounting already done for a task."""
    return f"result:{task_id}:transient_failure_processed"


def _usage_exhausted_cooldown_ttl_seconds(result: TaskResult) -> int:
    """Return persisted usage cooldown TTL, preferring result reset timestamp."""
    if result.rate_limit_resets_at:
        ttl = math.ceil(result.rate_limit_resets_at - time.time())
        if ttl > 0:
            return ttl
    return _USAGE_EXHAUSTED_COOLDOWN_SECONDS


def _failed_check_names(checks: list[dict]) -> set[str]:
    names: set[str] = set()
    for check in checks:
        conclusion = (check.get("conclusion") or "").upper()
        state = (check.get("state") or "").upper()
        failed = conclusion in _FAILURE_CONCLUSIONS or (
            not conclusion and state in ("FAILURE", "ERROR")
        )
        if failed:
            name = (
                check.get("detailsUrl")
                or check.get("details_url")
                or check.get("name")
                or check.get("context")
                or check.get("workflowName")
            )
            if name:
                names.add(str(name))
    return names


def _failed_check_fingerprints(checks: list[dict]) -> set[str]:
    fingerprints: set[str] = set()
    for check in checks:
        conclusion = (check.get("conclusion") or "").upper()
        state = (check.get("state") or "").upper()
        failed = conclusion in _FAILURE_CONCLUSIONS or (
            not conclusion and state in ("FAILURE", "ERROR")
        )
        if not failed:
            continue
        payload = {
            "name": str(check.get("name") or ""),
            "context": str(check.get("context") or ""),
            "workflow_name": str(check.get("workflowName") or ""),
            "details_url": str(check.get("detailsUrl") or check.get("details_url") or ""),
            "target_url": str(check.get("targetUrl") or check.get("target_url") or ""),
        }
        if any(payload.values()):
            fingerprints.add(json.dumps(payload, sort_keys=True, separators=(",", ":")))
    return fingerprints


def _snapshot_failed_checks_still_failing(checks: list[dict], snapshot: list[str]) -> bool:
    snapshot_set = set(snapshot)
    if not snapshot_set:
        return True
    current_fingerprints = _failed_check_fingerprints(checks)
    if current_fingerprints & snapshot_set:
        return True
    return bool(_failed_check_names(checks) & snapshot_set)


def _review_thread_ids(threads: list[dict]) -> set[str]:
    ids: set[str] = set()
    for thread in threads:
        thread_id = thread.get("id") or thread.get("node_id")
        if thread_id:
            ids.add(str(thread_id))
    return ids


def _review_thread_fingerprints(threads: list[dict]) -> set[str]:
    fingerprints: set[str] = set()
    for thread in threads:
        comments = []
        for comment in thread.get("comments") or []:
            comments.append(
                {
                    "id": str(comment.get("id") or comment.get("node_id") or ""),
                    "author": str(comment.get("author") or ""),
                    "body": str(comment.get("body") or ""),
                    "created_at": str(comment.get("createdAt") or comment.get("created_at") or ""),
                    "updated_at": str(comment.get("updatedAt") or comment.get("updated_at") or ""),
                }
            )
        payload = {
            "id": str(thread.get("id") or thread.get("node_id") or ""),
            "path": str(thread.get("path") or ""),
            "line": str(thread.get("line") or ""),
            "comments": comments,
        }
        fingerprints.add(json.dumps(payload, sort_keys=True, separators=(",", ":")))
    return fingerprints


def _is_pr_result_stale(project: ProjectConfig, result: TaskResult, logger: logging.Logger) -> bool:
    """Cheaply reject PR results that no longer match the current GitHub snapshot."""
    if result.resource_type != "pr":
        return False
    if not result.snapshot_head_sha:
        logger.info(
            "Dropping snapshot-less legacy PR result for PR #%d",
            result.resource_id,
        )
        return True

    try:
        pr_data = gh.get_pr(project.repo, result.resource_id, project.token)
    except Exception:
        logger.warning(
            "Failed to validate result snapshot for PR #%d; dropping result side-effects",
            result.resource_id,
            exc_info=True,
        )
        return True

    current_sha = str(pr_data.get("headRefOid") or "")
    if not current_sha:
        logger.info(
            "Dropping result for PR #%d: GitHub response did not include headRefOid",
            result.resource_id,
        )
        return True
    if current_sha and current_sha != result.snapshot_head_sha:
        logger.info(
            "Dropping stale result for PR #%d: result SHA %s, current SHA %s",
            result.resource_id,
            result.snapshot_head_sha,
            current_sha,
        )
        return True

    if result.decision_reason == "ci_failure" and result.snapshot_failed_checks:
        checks = pr_data.get("statusCheckRollup") or gh.get_ci_status(
            project.repo, result.resource_id, project.token
        )
        if not _snapshot_failed_checks_still_failing(checks, result.snapshot_failed_checks):
            logger.info(
                "Dropping stale CI result for PR #%d: captured failed checks are no longer failing",
                result.resource_id,
            )
            return True

    if result.decision_reason in ("changes_requested", "followup_threads") and (
        result.snapshot_review_thread_ids
    ):
        try:
            threads = gh.get_unresolved_review_threads(
                project.repo, result.resource_id, project.token
            )
        except Exception:
            logger.warning(
                "Failed to validate review-thread snapshot for PR #%d; "
                "dropping result side-effects",
                result.resource_id,
                exc_info=True,
            )
            return True
        if result.snapshot_review_thread_fingerprints:
            if not set(result.snapshot_review_thread_fingerprints).issubset(
                _review_thread_fingerprints(threads)
            ):
                logger.info(
                    "Dropping stale review result for PR #%d: captured thread content changed",
                    result.resource_id,
                )
                return True
        elif not set(result.snapshot_review_thread_ids).issubset(_review_thread_ids(threads)):
            logger.info(
                "Dropping stale review result for PR #%d: captured threads are resolved",
                result.resource_id,
            )
            return True

    if result.decision_reason == "merge_conflict_rebase":
        mergeable = str(pr_data.get("mergeable") or "").upper()
        merge_state = str(pr_data.get("mergeStateStatus") or "").upper()
        if mergeable != "CONFLICTING" and merge_state != "DIRTY":
            logger.info(
                "Dropping stale rebase result for PR #%d: conflict no longer applies",
                result.resource_id,
            )
            return True

    if result.decision_reason == "proactive_rebase":
        mergeable = str(pr_data.get("mergeable") or "").upper()
        if mergeable == "CONFLICTING":
            logger.info(
                "Dropping stale proactive rebase result for PR #%d: PR is now conflicting",
                result.resource_id,
            )
            return True

    return False


def _mark_usage_exhausted_token(
    result: TaskResult, token_pool: ProviderPool | None, logger: logging.Logger
) -> None:
    if token_pool is None:
        return
    exhausted_token = token_pool.get_task_token(result.task_id)
    cooldown_until = None
    if result.rate_limit_resets_at:
        from datetime import datetime, timezone

        cooldown_until = datetime.fromtimestamp(result.rate_limit_resets_at, tz=timezone.utc)
        logger.info("Rate limit resets at %s (from stream-json)", cooldown_until.isoformat())
    elif exhausted_token:
        try:
            cooldown_until = get_token_reset_time(exhausted_token)
        except Exception as e:
            logger.warning("Failed to query token reset time: %s", e)
    token_pool.mark_exhausted(result.task_id, cooldown_until=cooldown_until)


# Maximum number of merge retries for transient network errors before
# falling through to the backoff-and-retry path.
_MAX_MERGE_RETRIES = 5


def _is_network_error(msg: str) -> bool:
    """Return True if *msg* matches any known network error pattern."""
    return any(pat.search(msg) for pat in _MERGE_NETWORK_PATTERNS)


def _is_required_checks_expected_error(msg: str) -> bool:
    """Return True when branch protection says required checks need rerunning."""
    lower = msg.lower()
    return (
        "required status check" in lower
        and "expected" in lower
        and (
            "repository rule violations" in lower
            or "branch protection" in lower
            or "rule violations" in lower
        )
    )


def _is_merge_conflict_error(msg: str) -> bool:
    """Fallback classifier for gh errors when PR merge status cannot be refreshed."""
    lower = msg.lower()
    return (
        "merge conflict" in lower
        or "merge conflicts" in lower
        or "is not mergeable" in lower
        or "cannot be cleanly created" in lower
        or "cannot automatically merge" in lower
    )


def _merge_status_indicates_conflict(pr_data: dict) -> bool | None:
    """Return GitHub's conflict decision, or None when the status is inconclusive."""
    mergeable = pr_data.get("mergeable")
    merge_state_status = pr_data.get("mergeStateStatus")
    if mergeable == "CONFLICTING" or merge_state_status == "DIRTY":
        return True
    if mergeable == "MERGEABLE" and merge_state_status in {
        "BEHIND",
        "BLOCKED",
        "CLEAN",
        "DRAFT",
        "HAS_HOOKS",
        "UNSTABLE",
    }:
        return False
    return None


def _merge_status_summary(pr_data: dict) -> str:
    """Render the GitHub merge status fields for logs."""
    return (
        f"mergeable={pr_data.get('mergeable')!r} "
        f"mergeStateStatus={pr_data.get('mergeStateStatus')!r}"
    )


def _merge_failure_indicates_conflict(
    *, repo: str, pr_number: int, token: str, err_msg: str, logger: logging.Logger
) -> bool:
    """Prefer GitHub merge status; fall back to gh error text if status is unknown."""
    try:
        refreshed_pr = gh.get_pr(repo, pr_number, token)
        status_conflict = _merge_status_indicates_conflict(refreshed_pr)
        if status_conflict is not None:
            return status_conflict
        logger.warning(
            "PR #%d: GitHub merge status was inconclusive after merge failure "
            "(%s), falling back to gh error text",
            pr_number,
            _merge_status_summary(refreshed_pr),
        )
    except Exception as status_err:
        logger.warning(
            "PR #%d: failed to refresh GitHub merge status after merge failure, "
            "falling back to gh error text: %s",
            pr_number,
            status_err,
            exc_info=True,
        )
    return _is_merge_conflict_error(err_msg)


def _is_stale_head_error(msg: str) -> bool:
    """Return True when GitHub rejected an action because the PR head changed."""
    lower = msg.lower()
    return (
        ("match-head-commit" in lower)
        or ("expected_head_sha" in lower)
        or ("expected head sha" in lower)
        or ("head sha" in lower and ("mismatch" in lower or "modified" in lower))
        or ("head commit" in lower and ("changed" in lower or "mismatch" in lower))
        or ("head branch was modified" in lower)
    )


def _exception_message_with_stderr(exc: Exception) -> str:
    """Return exception text plus gh stderr when available."""
    stderr = getattr(exc, "stderr", "")
    if stderr:
        return f"{exc}\n{stderr}"
    return str(exc)


def _make_merge_retries_key(repo: str, pr_number: int) -> str:
    """Redis key for tracking merge retry count due to network errors."""
    return f"pr:{repo}:{pr_number}:merge_retries"


def _increment_merge_retries(redis: RedisClient, repo: str, pr_number: int) -> int:
    """Increment and return the merge retry count. TTL 1 hour."""
    key = _make_merge_retries_key(repo, pr_number)
    pipe = redis.pipeline(transaction=True)
    pipe.incr(key)
    pipe.expire(key, 3600)  # 1-hour TTL
    results = pipe.execute()
    return results[0]


def _make_review_rerun_failure_key(repo: str, pr_number: int, head_sha: str) -> str:
    """Redis key for failed claude-review rerun attempts on a PR SHA."""
    return f"pr:{repo}:{pr_number}:review_rerun_failures:{head_sha}"


def _make_review_rerun_failure_cooldown_key(repo: str, pr_number: int, head_sha: str) -> str:
    """Redis key suppressing claude-review reruns after a failed rerun call."""
    return f"pr:{repo}:{pr_number}:review_rerun_failure_cooldown:{head_sha}"


def _record_review_rerun_failure(
    redis: RedisClient, repo: str, pr_number: int, head_sha: str
) -> int:
    """Increment failed rerun attempts for this PR SHA and set a cooldown."""
    key = _make_review_rerun_failure_key(repo, pr_number, head_sha)
    count = redis.incr(key)
    redis.expire(key, _REVIEW_RERUN_FAILURE_TTL_SECONDS)
    redis.set_ex(
        _make_review_rerun_failure_cooldown_key(repo, pr_number, head_sha),
        "1",
        _REVIEW_RERUN_FAILURE_COOLDOWN_SECONDS,
    )
    return count


def _clear_review_rerun_failures(
    redis: RedisClient, repo: str, pr_number: int, head_sha: str
) -> None:
    """Clear loop-side failed rerun state after a successful rerun."""
    redis.delete(
        _make_review_rerun_failure_key(repo, pr_number, head_sha),
        _make_review_rerun_failure_cooldown_key(repo, pr_number, head_sha),
    )


def _review_rerun_failure_cooldown_active(
    redis: RedisClient, repo: str, pr_number: int, head_sha: str
) -> bool:
    """Return True when a failed claude-review rerun is still cooling down."""
    return redis.exists(_make_review_rerun_failure_cooldown_key(repo, pr_number, head_sha))


def _back_off_pr_retries(
    *,
    repo: str,
    pr_state: PRState,
    logger: logging.Logger,
    redis: RedisClient,
    reason: str,
    step: int,
) -> None:
    """Lengthen a PR's retry cadence instead of escalating it to a human.

    Orcest never abandons a PR or applies the needs-human label on budget
    exhaustion -- a "hard" PR is the worker's job, not a human's. The per-SHA
    attempt counter is cleared and a backoff cooldown is set so the PR is
    retried automatically once the cooldown expires. ``step`` selects the
    cooldown length (see coordination.get_backoff_cooldown_seconds); a larger
    step is used the deeper into the budget we are, so a repeatedly failing PR
    is retried less often -- never zero.
    """
    try:
        set_backoff_cooldown(redis, repo, pr_state.number, step, head_sha=pr_state.head_sha)
        clear_attempts(redis, repo, pr_state.number)
    except Exception as e:
        logger.error(
            "PR #%d: failed to set retry backoff: %s",
            pr_state.number,
            e,
            exc_info=True,
        )
    logger.info(
        "PR #%d: %s -- backing off retries (step %d), not escalating to a human",
        pr_state.number,
        reason,
        step,
    )


def run_orchestrator(config: OrchestratorConfig) -> None:
    """Main orchestrator entry point. Polls GitHub in a loop."""
    logger = setup_logging("orchestrator", "main")
    redis = RedisClient(config.redis)

    # Shared task Redis client — all projects publish tasks to this prefix
    # so workers only need to read from one stream.
    task_redis = RedisClient.from_client(redis.client, key_prefix=config.task_key_prefix)

    # Verify Redis connection
    if not redis.health_check():
        logger.error("Cannot connect to Redis. Exiting.")
        sys.exit(1)

    # Build per-project Redis clients once; reuse across all poll cycles
    project_clients = _build_project_clients(config, redis)

    # Ensure consumer groups for shared task streams (so workers don't race)
    for stream in (
        f"tasks:{config.default_runner}",
        f"tasks:issue:{config.default_runner}",
    ):
        task_redis.ensure_consumer_group(stream, CONSUMER_GROUP)

    # Ensure consumer group for results stream (per-project)
    for _, project_redis in project_clients:
        project_redis.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    # Create per-project provider pools (generalized from legacy TokenPool).
    # For the claude_tokens migration path we synthesize lean ProviderEntry objects
    # (rich execution fields left None) via from_claude_tokens per Task 3 / boundary rule.
    token_pools: dict[str, ProviderPool] = {}
    for project in config.projects:
        tokens = project.claude_tokens
        if tokens:
            token_pools[project.key_prefix] = ProviderPool.from_claude_tokens(tokens)
            if len(tokens) > 1:
                logger.info(
                    "Project %s: provider pool with %d Claude entries (via legacy claude_tokens)",
                    project.repo,
                    len(tokens),
                )

    # Graceful shutdown
    shutdown = False

    def handle_signal(signum: int, frame: object) -> None:
        nonlocal shutdown
        logger.info("Received signal %d, shutting down gracefully...", signum)
        shutdown = True

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    pending_task_ttl = compute_pending_task_ttl(config.runner)

    repos = ", ".join(p.repo for p in config.projects) if config.projects else "(none)"
    logger.info(
        "Orchestrator started. Projects: %s, poll interval: %ds",
        repos,
        config.polling.interval,
    )

    while not shutdown:
        try:
            _poll_cycle(
                config,
                redis,
                task_redis,
                token_pools,
                logger,
                pending_task_ttl,
                project_clients,
            )
        except Exception as e:
            logger.error("Poll cycle failed: %s", e, exc_info=True)
            # Continue after error -- don't crash the loop

        # Wait for next cycle (interruptible in 1-second chunks)
        for _ in range(config.polling.interval):
            if shutdown:
                break
            time.sleep(1)

    logger.info("Orchestrator shut down cleanly.")


def _build_project_clients(
    config: OrchestratorConfig,
    redis: RedisClient,
) -> list[tuple[ProjectConfig, RedisClient]]:
    """Build per-project Redis clients from config."""
    return [
        (project, RedisClient.from_client(redis.client, key_prefix=project.key_prefix))
        for project in config.projects
    ]


def _poll_cycle(
    config: OrchestratorConfig,
    redis: RedisClient,
    task_redis: RedisClient,
    token_pools: dict[str, ProviderPool],
    logger: logging.Logger,
    pending_task_ttl: int,
    project_clients: list[tuple[ProjectConfig, RedisClient]] | None = None,
) -> None:
    """Single orchestrator poll cycle across all configured projects."""
    if project_clients is None:
        project_clients = _build_project_clients(config, redis)

    # Step 1: Consume results per project
    for project, project_redis in project_clients:
        try:
            pool = token_pools.get(project.key_prefix)
            _consume_results_for_project(
                project,
                project_redis,
                config.labels,
                logger,
                token_pool=pool,
                max_transient_failures=config.max_transient_failures,
            )
        except Exception:
            logger.error("Failed to consume results for %s", project.repo, exc_info=True)

    # Do not trim task/result streams by consumer-group last-delivered-id.
    # That ID can include delivered but unACKed PEL entries; trimming them would
    # erase the only recoverable task/result body and leave Redis coordination
    # state as the only memory of work.

    # Step 2: Poll each project
    total_enqueued = 0
    total_merged = 0
    total_prs = 0
    total_issues = 0
    for project, project_redis in project_clients:
        try:
            pool = token_pools.get(project.key_prefix)
            enqueued, merged, prs_checked, issues_checked = _poll_project(
                project,
                project_redis,
                task_redis,
                config,
                logger,
                pending_task_ttl,
                token_pool=pool,
            )
            total_enqueued += enqueued
            total_merged += merged
            total_prs += prs_checked
            total_issues += issues_checked
        except Exception:
            logger.error("Failed to poll %s", project.repo, exc_info=True)

    logger.info(
        "Poll cycle complete. %d tasks enqueued, %d merged, %d PRs checked, %d issues checked.",
        total_enqueued,
        total_merged,
        total_prs,
        total_issues,
    )


def _poll_project(
    project: ProjectConfig,
    project_redis: RedisClient,
    task_redis: RedisClient,
    config: OrchestratorConfig,
    logger: logging.Logger,
    pending_task_ttl: int,
    token_pool: ProviderPool | None = None,
) -> tuple[int, int, int, int]:
    """Poll a single project for actionable PRs and issues.

    Args:
        project_redis: Per-project Redis client (for pending markers, attempt counters, etc.).
        task_redis: Shared Redis client (for publishing tasks to the common stream).
        token_pool: Optional provider pool (generalized; still called token_pool in this
            transitional function for minimal diff). Uses lean ProviderEntry surface only.

    Returns (enqueued, merged, prs_checked, issues_checked).
    """
    logger = logger.getChild(project.repo)
    repo = project.repo
    token = project.token
    key_prefix = project.key_prefix

    def _select_claude_token() -> str | None:
        """Pick the next Claude token from the pool (round-robin).

        Returns None if all tokens are exhausted (caller should skip enqueue).
        Falls back to project.claude_token if no pool is configured.

        On exhaustion, increments a per-project Redis counter so operators can
        distinguish "quiet because no PRs need work" from "quiet because all
        Claude tokens are cooling."
        """
        if token_pool is None:
            return project.claude_token
        token = token_pool.next_token()
        if token is None:
            try:
                count = project_redis.incr(_TOKEN_EXHAUSTED_SKIP_KEY)
                if count == 1:
                    project_redis.expire(
                        _TOKEN_EXHAUSTED_SKIP_KEY, _TOKEN_EXHAUSTED_SKIP_TTL_SECONDS
                    )
            except Exception:
                # Observability is best-effort; never break the poll cycle.
                logger.debug("Failed to increment token-exhaustion counter", exc_info=True)
        return token

    def _register_task(task_id: str, claude_token: str) -> None:
        """Record which token was used for a task (for exhaustion tracking)."""
        if token_pool is not None:
            token_pool.register_task(task_id, claude_token)

    labels = config.labels

    # Discover PRs needing action
    pr_states = discover_actionable_prs(
        repo=repo,
        token=token,
        redis=project_redis,
        label_config=labels,
        max_attempts=config.max_attempts,
        max_total_attempts=config.max_total_attempts,
        stale_pending_timeout_seconds=config.stale_pending_timeout_seconds,
    )

    # Sort: merges first (quick wins), then fixes/followups oldest-first
    # (lowest PR number = longest waiting). Skips don't matter but sort
    # them last so actionable items are processed first.
    _ACTION_PRIORITY = {
        PRAction.MERGE: 0,
        PRAction.UPDATE_BRANCH: 0,
        PRAction.ENQUEUE_FIX: 1,
        PRAction.ENQUEUE_FOLLOWUP: 1,
        PRAction.ENQUEUE_REBASE: 1,
    }
    pr_states.sort(key=lambda ps: (_ACTION_PRIORITY.get(ps.action, 9), ps.number))

    # Pre-compute unclaimed issue tasks for gating issue discovery. Tasks that
    # are claimed and in flight don't block — they're consuming worker capacity,
    # not buffer space. Only undelivered (lag) entries justify deferral.
    issue_tasks_stream = f"tasks:issue:{config.default_runner}"
    unclaimed_issue_tasks = task_redis.stream_unread_count(issue_tasks_stream, CONSUMER_GROUP)

    enqueued = 0
    merged = 0
    for pr_state in pr_states:
        if pr_state.action == PRAction.MERGE:
            logger.info("PR #%d (%s): merging", pr_state.number, pr_state.title)
            try:
                gh.merge_pr(
                    repo,
                    pr_state.number,
                    token,
                    delete_branch=config.delete_branch_on_merge,
                    head_sha=pr_state.head_sha,
                )
                merged += 1
            except Exception as e:
                err_msg = _exception_message_with_stderr(e)
                logger.error(
                    f"Failed to merge PR #{pr_state.number}: {err_msg}",
                    exc_info=True,
                )
                # If the error looks like a merge conflict, enqueue a
                # rebase task so a worker can resolve it automatically.
                is_conflict = _merge_failure_indicates_conflict(
                    repo=repo,
                    pr_number=pr_state.number,
                    token=token,
                    err_msg=err_msg,
                    logger=logger,
                )
                if is_conflict:
                    logger.info(
                        f"PR #{pr_state.number}: merge conflict detected, enqueueing rebase task"
                    )
                    ct = _select_claude_token()
                    if ct is None:
                        logger.warning(
                            "All Claude tokens exhausted, skipping rebase for PR #%d",
                            pr_state.number,
                        )
                        continue
                    else:
                        try:
                            task = publish_rebase_task(
                                pr_state=pr_state,
                                repo=repo,
                                token=token,
                                redis=project_redis,
                                default_runner=config.default_runner,
                                merge_error=err_msg[:200],
                                pending_task_ttl=pending_task_ttl,
                                logger=logger,
                                claude_token=ct,
                                key_prefix=key_prefix,
                                task_redis=task_redis,
                            )
                            if task is not None:
                                _register_task(task.id, ct)
                                enqueued += 1
                        except Exception as rebase_err:
                            logger.error(
                                f"Failed to enqueue rebase task for PR #{pr_state.number}: "
                                f"{rebase_err}",
                                exc_info=True,
                            )
                            # Fall through to the backoff-and-retry path
                            is_conflict = False

                if not is_conflict:
                    if _is_required_checks_expected_error(err_msg):
                        logger.info(
                            "PR #%d: merge blocked because required status checks are "
                            "expected; updating branch so checks can rerun",
                            pr_state.number,
                        )
                        try:
                            if gh.update_branch(
                                repo,
                                pr_state.number,
                                token,
                                expected_head_sha=pr_state.head_sha,
                            ):
                                continue
                        except Exception as update_err:
                            if _is_stale_head_error(_exception_message_with_stderr(update_err)):
                                logger.info(
                                    "PR #%d: update-branch rejected stale discovered "
                                    "head SHA %s after merge rejection; retrying next poll",
                                    pr_state.number,
                                    pr_state.head_sha,
                                )
                                continue
                            logger.warning(
                                "PR #%d: update-branch after merge rejection failed: %s",
                                pr_state.number,
                                update_err,
                                exc_info=True,
                            )

                    if _is_stale_head_error(err_msg):
                        logger.info(
                            "PR #%d: merge rejected stale discovered head SHA %s; "
                            "retrying next poll",
                            pr_state.number,
                            pr_state.head_sha,
                        )
                        continue

                    # Check for transient network errors — retry silently
                    # unless we've exceeded the merge retry budget.
                    # GhRateLimitError is excluded: _run_gh() already
                    # exhausted its own retry budget for rate limits.
                    if not isinstance(e, GhRateLimitError) and _is_network_error(err_msg):
                        retry_count = _increment_merge_retries(project_redis, repo, pr_state.number)
                        if retry_count <= _MAX_MERGE_RETRIES:
                            logger.warning(
                                "PR #%d: transient network error during merge "
                                "(attempt %d/%d), will retry on next poll cycle: %s",
                                pr_state.number,
                                retry_count,
                                _MAX_MERGE_RETRIES,
                                err_msg[:200],
                            )
                            continue  # skip needs-human label

                    # A non-conflict, non-stale, non-network merge failure
                    # (branch protection, a missing required check, etc.) is
                    # not a human-decision blocker -- it usually clears on its
                    # own once CI/branch state settles. Back off and let the
                    # next poll re-evaluate; never escalate to needs-human.
                    logger.warning(
                        "PR #%d: merge failed (%s) -- backing off, will retry",
                        pr_state.number,
                        err_msg[:200],
                    )
                    _back_off_pr_retries(
                        repo=repo,
                        pr_state=pr_state,
                        logger=logger,
                        redis=project_redis,
                        reason="merge failed",
                        step=1,
                    )
            else:
                # Clean up state on successful merge
                try:
                    clear_review_retrigger(project_redis, repo, pr_state.number)
                except Exception:
                    logger.debug(
                        "cleanup failed: clear_review_retrigger for PR #%d",
                        pr_state.number,
                        exc_info=True,
                    )  # Best-effort cleanup; key has TTL anyway
                try:
                    clear_total_attempts(project_redis, repo, pr_state.number)
                except Exception:
                    logger.debug(
                        "cleanup failed: clear_total_attempts for PR #%d",
                        pr_state.number,
                        exc_info=True,
                    )  # Best-effort cleanup; key has TTL anyway
                try:
                    clear_backoff(project_redis, repo, pr_state.number)
                except Exception:
                    logger.debug(
                        "cleanup failed: clear_backoff for PR #%d",
                        pr_state.number,
                        exc_info=True,
                    )  # Best-effort cleanup; key has TTL anyway
                try:
                    gh.post_comment(
                        repo,
                        pr_state.number,
                        "**orcest** merged this PR.",
                        token,
                    )
                except Exception as comment_err:
                    logger.warning(
                        "Merged PR #%d but failed to post comment: %s",
                        pr_state.number,
                        comment_err,
                        exc_info=True,
                    )
                # Run deployment if configured (run_deployment is a no-op when disabled)
                try:
                    if run_deployment(config.deployment, pr_state.number, logger):
                        logger.info("PR #%d: deployment succeeded", pr_state.number)
                except DeploymentError as deploy_err:
                    err_msg = str(deploy_err)
                    logger.error("PR #%d: deployment failed: %s", pr_state.number, err_msg)
                    try:
                        issue_number = gh.create_issue(
                            repo,
                            f"Deployment failed after merge of PR #{pr_state.number}",
                            f"**orcest** deployment failed after merging "
                            f"PR #{pr_state.number} ({pr_state.title}).\n\n"
                            f"Error: {err_msg[:500]}",
                            token,
                            labels=["orcest:needs-human"],
                        )
                        logger.info(
                            "PR #%d: created deployment failure issue #%d",
                            pr_state.number,
                            issue_number,
                        )
                    except Exception as issue_err:
                        logger.error(
                            "PR #%d: failed to create deployment failure issue: %s",
                            pr_state.number,
                            issue_err,
                            exc_info=True,
                        )
                # After successful merge, rebase other open PRs onto updated master.
                # If multiple PRs are merged in the same poll cycle, this loop runs
                # once per merged PR; publish_rebase_task calls set_pending_task (SET
                # NX EX), which silently deduplicates redundant enqueue attempts.
                logger.info(
                    "PR #%d merged; checking for SKIP_GREEN PRs to proactively rebase",
                    pr_state.number,
                )
                for other_pr in pr_states:
                    if other_pr.number == pr_state.number:
                        continue  # skip the one we just merged
                    if other_pr.action != PRAction.SKIP_GREEN:
                        continue  # only proactively rebase green PRs
                    ct = _select_claude_token()
                    if ct is None:
                        logger.warning(
                            "All Claude tokens exhausted, skipping proactive rebase for PR #%d",
                            other_pr.number,
                        )
                        continue
                    try:
                        task = publish_rebase_task(
                            pr_state=other_pr,
                            repo=repo,
                            token=token,
                            redis=project_redis,
                            default_runner=config.default_runner,
                            merge_error="",
                            pending_task_ttl=pending_task_ttl,
                            logger=logger,
                            claude_token=ct,
                            key_prefix=key_prefix,
                            proactive=True,
                            task_redis=task_redis,
                        )
                        if task is not None:
                            _register_task(task.id, ct)
                    except Exception:
                        logger.warning(
                            "Failed to enqueue rebase for PR #%d",
                            other_pr.number,
                            exc_info=True,
                        )
        elif pr_state.action == PRAction.ENQUEUE_FIX:
            logger.info("PR #%d (%s): enqueueing fix task", pr_state.number, pr_state.title)
            try:
                if rerun_all_transient_ci(
                    pr_state=pr_state,
                    repo=repo,
                    token=token,
                    redis=project_redis,
                    logger=logger,
                ):
                    continue
            except Exception as e:
                logger.warning(
                    "Failed to classify/re-trigger transient CI for PR #%d before "
                    "token selection: %s",
                    pr_state.number,
                    e,
                    exc_info=True,
                )
            ct = _select_claude_token()
            if ct is None:
                logger.warning(
                    "All Claude tokens exhausted, skipping fix task for PR #%d",
                    pr_state.number,
                )
            else:
                try:
                    result = publish_fix_task(
                        pr_state=pr_state,
                        repo=repo,
                        token=token,
                        redis=project_redis,
                        default_runner=config.default_runner,
                        pending_task_ttl=pending_task_ttl,
                        logger=logger,
                        claude_token=ct,
                        key_prefix=key_prefix,
                        task_redis=task_redis,
                        skip_transient_rerun=True,
                    )
                    if result is not None:
                        _register_task(result.id, ct)
                        enqueued += 1
                except Exception as e:
                    logger.error(
                        "Failed to publish fix task for PR #%d: %s",
                        pr_state.number,
                        e,
                        exc_info=True,
                    )
        elif pr_state.action == PRAction.ENQUEUE_FOLLOWUP:
            logger.info("PR #%d (%s): enqueueing followup triage", pr_state.number, pr_state.title)
            ct = _select_claude_token()
            if ct is None:
                logger.warning(
                    "All Claude tokens exhausted, skipping followup task for PR #%d",
                    pr_state.number,
                )
            else:
                try:
                    task = publish_followup_task(
                        pr_state=pr_state,
                        repo=repo,
                        token=token,
                        redis=project_redis,
                        default_runner=config.default_runner,
                        pending_task_ttl=pending_task_ttl,
                        logger=logger,
                        claude_token=ct,
                        key_prefix=key_prefix,
                        task_redis=task_redis,
                    )
                    if task is not None:
                        _register_task(task.id, ct)
                        enqueued += 1
                except Exception as e:
                    logger.error(
                        "Failed to publish followup task for PR #%d: %s",
                        pr_state.number,
                        e,
                        exc_info=True,
                    )
        elif pr_state.action == PRAction.ENQUEUE_REBASE:
            logger.info(
                "PR #%d (%s): merge conflicts detected, enqueueing rebase task",
                pr_state.number,
                pr_state.title,
            )
            ct = _select_claude_token()
            if ct is None:
                logger.warning(
                    "All Claude tokens exhausted, skipping rebase task for PR #%d",
                    pr_state.number,
                )
            else:
                try:
                    task = publish_rebase_task(
                        pr_state=pr_state,
                        repo=repo,
                        token=token,
                        redis=project_redis,
                        default_runner=config.default_runner,
                        pending_task_ttl=pending_task_ttl,
                        logger=logger,
                        claude_token=ct,
                        key_prefix=key_prefix,
                        task_redis=task_redis,
                    )
                    if task is not None:
                        _register_task(task.id, ct)
                        enqueued += 1
                except Exception as e:
                    logger.error(
                        "Failed to publish rebase task for PR #%d: %s",
                        pr_state.number,
                        e,
                        exc_info=True,
                    )
        elif pr_state.action == PRAction.UPDATE_BRANCH:
            logger.info(
                "PR #%d (%s): out-of-date with base, calling update-branch",
                pr_state.number,
                pr_state.title,
            )
            try:
                gh.update_branch(
                    repo,
                    pr_state.number,
                    token,
                    expected_head_sha=pr_state.head_sha,
                )
            except Exception as e:
                if _is_stale_head_error(_exception_message_with_stderr(e)):
                    logger.info(
                        "PR #%d: update-branch rejected stale discovered head SHA %s; "
                        "retrying next poll",
                        pr_state.number,
                        pr_state.head_sha,
                    )
                    continue
                logger.warning(
                    "PR #%d: update-branch failed: %s",
                    pr_state.number,
                    e,
                    exc_info=True,
                )
        elif pr_state.action == PRAction.SKIP_GREEN:
            logger.debug("PR #%d: CI green, skipping", pr_state.number)
        elif pr_state.action == PRAction.RETRIGGER_REVIEW:
            if pr_state.review_run_id is None:
                logger.error(
                    "PR #%d: RETRIGGER_REVIEW action but review_run_id is None, skipping",
                    pr_state.number,
                )
            elif _review_rerun_failure_cooldown_active(
                project_redis, repo, pr_state.number, pr_state.head_sha
            ):
                logger.info(
                    "PR #%d: claude-review rerun failure cooldown active, skipping",
                    pr_state.number,
                )
            else:
                run_id = pr_state.review_run_id
                logger.info(
                    "PR #%d: claude-review passed but no formal review, re-triggering run %d",
                    pr_state.number,
                    run_id,
                )
                try:
                    gh.rerun_workflow(
                        repo,
                        run_id,
                        token,
                    )
                    set_review_retrigger_sha(
                        project_redis, repo, pr_state.number, pr_state.head_sha
                    )
                    _clear_review_rerun_failures(
                        project_redis, repo, pr_state.number, pr_state.head_sha
                    )
                except Exception as e:
                    failure_count = _record_review_rerun_failure(
                        project_redis, repo, pr_state.number, pr_state.head_sha
                    )
                    logger.error(
                        "Failed to re-trigger review for PR #%d (failure %d/%d; cooldown %ds): %s",
                        pr_state.number,
                        failure_count,
                        _MAX_REVIEW_RERUN_FAILURES,
                        _REVIEW_RERUN_FAILURE_COOLDOWN_SECONDS,
                        e,
                        exc_info=True,
                    )
                    if failure_count >= _MAX_REVIEW_RERUN_FAILURES:
                        # Repeated failures to re-trigger claude-review are not
                        # a human-decision blocker. Back off and keep retrying;
                        # never escalate to needs-human.
                        logger.warning(
                            "PR #%d: claude-review re-trigger failed %d times -- "
                            "backing off, will retry",
                            pr_state.number,
                            failure_count,
                        )
                        _back_off_pr_retries(
                            repo=repo,
                            pr_state=pr_state,
                            logger=logger,
                            redis=project_redis,
                            reason="claude-review re-trigger repeatedly failed",
                            step=2,
                        )
        elif pr_state.action == PRAction.RETRIGGER_STALE_CHECKS:
            run_ids = pr_state.stale_run_ids
            # Cooldown guard: skip if we already acted on this SHA
            stale_sha = get_stale_retrigger_sha(project_redis, repo, pr_state.number)
            if stale_sha == pr_state.head_sha:
                logger.debug(
                    "PR #%d: stale checks already handled for SHA %s, skipping",
                    pr_state.number,
                    pr_state.head_sha,
                )
            elif not run_ids:
                # Stale pending checks found but no re-triggerable run IDs
                # (e.g. StatusContext checks). Nothing orcest can act on and no
                # human decision is required -- the checks may still settle.
                # Log and move on; never escalate to needs-human.
                logger.warning(
                    "PR #%d: stale pending checks with no re-triggerable run IDs; "
                    "leaving for the check provider to settle",
                    pr_state.number,
                )
                set_stale_retrigger_sha(
                    project_redis,
                    repo,
                    pr_state.number,
                    pr_state.head_sha,
                    ex=config.stale_pending_timeout_seconds,
                )
            else:
                logger.warning(
                    "PR #%d: stale pending check(s) (>%ds); re-triggering %d run(s) %s",
                    pr_state.number,
                    config.stale_pending_timeout_seconds,
                    len(run_ids),
                    run_ids,
                )
                any_cancel_succeeded = False
                cancelled_count = 0
                cancelled_run_ids: list[int] = []
                for run_id in run_ids:
                    try:
                        gh.cancel_workflow(
                            repo,
                            run_id,
                            token,
                        )
                        any_cancel_succeeded = True
                        cancelled_count += 1
                        cancelled_run_ids.append(run_id)
                        logger.info(
                            "PR #%d: cancelled stale workflow run %d",
                            pr_state.number,
                            run_id,
                        )
                    except Exception as e:
                        logger.warning(
                            "Failed to cancel stale run %d for PR #%d: %s",
                            run_id,
                            pr_state.number,
                            e,
                            exc_info=True,
                        )
                    # Best-effort immediate rerun; gh run rerun requires the
                    # run to be in a completed state, so this will usually fail
                    # while the cancel is still propagating.  If it does fail,
                    # the cancelled run will appear as a CI failure on the next
                    # poll cycle and be handled by the normal fix flow.
                    try:
                        gh.rerun_workflow(
                            repo,
                            run_id,
                            token,
                        )
                        logger.info(
                            "PR #%d: re-triggered stale workflow run %d",
                            pr_state.number,
                            run_id,
                        )
                    except Exception as e:
                        logger.debug(
                            "Could not immediately re-trigger run %d for PR #%d "
                            "(cancel may still be propagating): %s",
                            run_id,
                            pr_state.number,
                            e,
                        )
                # Always set cooldown after attempting — prevents a busy retry
                # loop if the run can't be cancelled or immediately rerun.
                set_stale_retrigger_sha(
                    project_redis,
                    repo,
                    pr_state.number,
                    pr_state.head_sha,
                    ex=config.stale_pending_timeout_seconds,
                )
                if any_cancel_succeeded:
                    record_self_cancelled_stale_runs(
                        project_redis,
                        repo,
                        pr_state.number,
                        pr_state.head_sha,
                        cancelled_run_ids,
                        ttl_seconds=max(config.stale_pending_timeout_seconds * 2, 24 * 3600),
                    )
                    try:
                        gh.post_comment(
                            repo,
                            pr_state.number,
                            f"**orcest** detected CI checks stuck in pending state for"
                            f" more than {config.stale_pending_timeout_seconds // 60}m."
                            f" Cancelled {cancelled_count} of {len(run_ids)} run(s) to self-heal."
                            f" CI will restart once the cancellation propagates.",
                            token,
                        )
                    except Exception as e:
                        logger.error(
                            "Failed to post stale-check comment on PR #%d: %s",
                            pr_state.number,
                            e,
                            exc_info=True,
                        )
        elif pr_state.action == PRAction.SKIP_LOCKED:
            logger.debug("PR #%d: locked, skipping", pr_state.number)
        elif pr_state.action == PRAction.SKIP_MAX_ATTEMPTS:
            _back_off_pr_retries(
                repo=repo,
                pr_state=pr_state,
                logger=logger,
                redis=project_redis,
                reason=f"per-SHA retry budget reached ({config.max_attempts} attempts)",
                step=1,
            )
        elif pr_state.action == PRAction.SKIP_MAX_TOTAL_ATTEMPTS:
            _back_off_pr_retries(
                repo=repo,
                pr_state=pr_state,
                logger=logger,
                redis=project_redis,
                reason=(f"total retry budget reached ({config.max_total_attempts} attempts)"),
                step=7,
            )
            # The total-attempt counter only paces the retry cadence. Reset it
            # now so that, once the long backoff cooldown expires, discovery no
            # longer returns SKIP_MAX_TOTAL_ATTEMPTS and work resumes.
            try:
                clear_total_attempts(project_redis, repo, pr_state.number)
            except Exception as e:
                logger.error(
                    "PR #%d: failed to reset total-attempt counter: %s",
                    pr_state.number,
                    e,
                    exc_info=True,
                )
        elif pr_state.action == PRAction.SKIP_BACKOFF:
            logger.info("PR #%d: in backoff cooldown, skipping", pr_state.number)
        elif pr_state.action == PRAction.SKIP_DRAFT:
            logger.debug("PR #%d: draft, skipping", pr_state.number)
        elif pr_state.action == PRAction.SKIP_PENDING:
            logger.debug(f"PR #{pr_state.number}: CI pending, skipping")
        elif pr_state.action == PRAction.SKIP_QUEUED:
            logger.debug(f"PR #{pr_state.number}: task already queued, skipping")
        elif pr_state.action == PRAction.SKIP_ACTIVE:
            logger.debug(f"PR #{pr_state.number}: task in flight, skipping")
        elif pr_state.action == PRAction.SKIP_LABELED:
            logger.debug(f"PR #{pr_state.number}: terminal label, skipping")
        elif pr_state.action == PRAction.SKIP_NO_CHECKS:
            logger.debug(f"PR #{pr_state.number}: no CI checks, skipping")
        elif pr_state.action == PRAction.SKIP_USAGE_COOLDOWN:
            logger.debug("PR #%d: usage-exhausted cooldown active, skipping", pr_state.number)
        else:
            logger.warning(
                "PR #%d: unhandled action %r, skipping", pr_state.number, pr_state.action
            )

    # Discover issues needing implementation. The only backpressure here is
    # the count of unclaimed issue tasks already on the stream — adding more
    # while a backlog of undelivered tasks exists just bloats the queue.
    # Tasks that workers have already claimed don't block: they're consuming
    # capacity, freeing other idle workers to take new work. PR-fix tasks
    # ride a separate stream and share worker capacity organically.
    issue_states: list = []
    if unclaimed_issue_tasks > 0:
        logger.info(
            f"Issue task queue has {unclaimed_issue_tasks} unclaimed entries, "
            f"deferring issue discovery until queue drains"
        )
    else:
        try:
            issue_states = discover_actionable_issues(
                repo=repo,
                token=token,
                redis=project_redis,
                label_config=labels,
                max_attempts=config.max_attempts,
            )
        except Exception as e:
            logger.error(f"Issue discovery failed: {e}", exc_info=True)

    # Act on issues
    for issue_state in issue_states:
        if issue_state.action == IssueAction.ENQUEUE_IMPLEMENT:
            logger.info(
                f"Issue #{issue_state.number} ({issue_state.title}): enqueueing implementation task"
            )
            ct = _select_claude_token()
            if ct is None:
                logger.warning(
                    "All Claude tokens exhausted, skipping issue task for issue #%d",
                    issue_state.number,
                )
            else:
                try:
                    task = publish_issue_task(
                        issue_state=issue_state,
                        repo=repo,
                        token=token,
                        redis=project_redis,
                        default_runner=config.default_runner,
                        pending_task_ttl=pending_task_ttl,
                        logger=logger,
                        claude_token=ct,
                        key_prefix=key_prefix,
                        task_redis=task_redis,
                    )
                    if task is not None:
                        _register_task(task.id, ct)
                        enqueued += 1
                except Exception as e:
                    logger.error(
                        f"Failed to publish issue task for issue #{issue_state.number}: {e}",
                        exc_info=True,
                    )
        elif issue_state.action == IssueAction.SKIP_MAX_ATTEMPTS:
            # Budget exhaustion is not a human-decision blocker. Orcest does
            # not label the issue needs-human; it simply stops actively
            # retrying until the issue changes (a new comment/edit resets the
            # counter). A human can still pick it up, but orcest is not
            # asserting that one is required.
            logger.info(
                "Issue #%d: per-issue retry budget reached (%d attempts), "
                "pausing retries -- not escalating to a human",
                issue_state.number,
                config.max_attempts,
            )
        elif issue_state.action == IssueAction.SKIP_QUEUED:
            logger.debug(f"Issue #{issue_state.number}: task already queued, skipping")
        elif issue_state.action == IssueAction.SKIP_LOCKED:
            logger.debug(f"Issue #{issue_state.number}: locked, skipping")
        elif issue_state.action == IssueAction.SKIP_ACTIVE:
            logger.debug(f"Issue #{issue_state.number}: task in flight, skipping")
        elif issue_state.action == IssueAction.SKIP_USAGE_COOLDOWN:
            logger.debug(
                "Issue #%d: usage-exhausted cooldown active, skipping",
                issue_state.number,
            )
        elif issue_state.action == IssueAction.SKIP_LABELED:
            logger.debug(f"Issue #{issue_state.number}: terminal label, skipping")
        else:
            logger.warning(
                f"Issue #{issue_state.number}: unhandled action {issue_state.action!r}, skipping"
            )

    return enqueued, merged, len(pr_states), len(issue_states)


def _consume_results_for_project(
    project: ProjectConfig,
    redis: RedisClient,
    labels: LabelConfig,
    logger: logging.Logger,
    token_pool: ProviderPool | None = None,
    max_transient_failures: int = 5,
) -> None:
    """Consume any pending results from workers for a single project.

    Non-blocking: reads all available results without waiting.

    First drains pending entries (delivered but not ACKed — can happen if
    the orchestrator was restarted mid-cycle), then reads new entries.
    This prevents orphaned labels from results that were read but never
    processed after a restart.
    """
    logger = logger.getChild(project.repo)
    # Phase 1: Drain pending (unACKed) entries from previous runs
    while True:
        entries = redis.xreadgroup(
            group=RESULTS_GROUP,
            consumer="orchestrator-main",
            stream=RESULTS_STREAM,
            count=10,
            block_ms=None,
            pending=True,
        )
        if not entries:
            break
        for entry_id, fields in entries:
            try:
                result = TaskResult.from_dict(fields)
                _handle_result(
                    project,
                    labels,
                    redis,
                    result,
                    logger,
                    token_pool=token_pool,
                    max_transient_failures=max_transient_failures,
                )
                logger.info(f"Recovered pending result {entry_id}")
            except Exception as e:
                logger.error(
                    f"Failed to process pending result {entry_id}: {e}",
                    exc_info=True,
                )
            try:
                redis.xack(RESULTS_STREAM, RESULTS_GROUP, entry_id)
            except Exception as ack_err:
                logger.error(
                    f"Failed to ACK pending result {entry_id}: {ack_err}",
                    exc_info=True,
                )

    # Phase 2: Read new entries
    while True:
        entries = redis.xreadgroup(
            group=RESULTS_GROUP,
            consumer="orchestrator-main",
            stream=RESULTS_STREAM,
            count=10,
            block_ms=None,
        )

        if not entries:
            break

        for entry_id, fields in entries:
            try:
                result = TaskResult.from_dict(fields)
                _handle_result(
                    project,
                    labels,
                    redis,
                    result,
                    logger,
                    token_pool=token_pool,
                    max_transient_failures=max_transient_failures,
                )
            except Exception as e:
                logger.error(
                    "Failed to process result entry %s: %s",
                    entry_id,
                    e,
                    exc_info=True,
                )
            # Always ACK to prevent infinite reprocessing of
            # malformed or unhandleable entries.
            try:
                redis.xack(RESULTS_STREAM, RESULTS_GROUP, entry_id)
            except Exception as ack_err:
                logger.error(
                    "Failed to ACK result entry %s: %s",
                    entry_id,
                    ack_err,
                    exc_info=True,
                )


def _handle_result(
    project: ProjectConfig,
    labels: LabelConfig,
    redis: RedisClient,
    result: TaskResult,
    logger: logging.Logger,
    token_pool: ProviderPool | None = None,
    max_transient_failures: int = 5,
) -> None:
    """Process a single task result.

    Posts a comment on the resource (PR or issue) with the result summary
    and manages labels:
    - completed: clears issue attempt counter; PR attempts remain until the SHA changes
    - failed: retried automatically; needs-human is added ONLY when the worker
      explicitly reported a human-decision blocker (result.needs_human)
    - blocked: adds blocked label
    - usage_exhausted: no label changes; resource resumes after cooldown
    """
    logger.info(
        "Result for task %s: %s (worker: %s, %ss)",
        result.task_id,
        result.status.value,
        result.worker_id,
        result.duration_seconds,
    )

    repo = project.repo
    token = project.token
    resource_id = result.resource_id
    is_issue = result.resource_type == "issue"
    resource_label = "issue" if is_issue else "PR"
    resource_type = result.resource_type or ("issue" if is_issue else "pr")

    # Guard against stale task IDs. GitHub snapshot validation below is the
    # authoritative staleness check for PR tasks; the pending marker is only
    # coordination state that prevents an old result from affecting a newer
    # in-flight task.
    try:
        current_task_id = get_pending_task(redis, repo, resource_type, resource_id)
        if current_task_id is not None and current_task_id != result.task_id:
            logger.warning(
                "Stale result for %s #%d: result task_id=%s but active task_id=%s; "
                "skipping label/comment side-effects",
                resource_label,
                resource_id,
                result.task_id,
                current_task_id,
            )
            if result.status == ResultStatus.USAGE_EXHAUSTED:
                _mark_usage_exhausted_token(result, token_pool, logger)
            if token_pool is not None:
                token_pool.task_completed(result.task_id)
            return
    except Exception as e:
        logger.error(
            "Failed to check pending task ID for %s #%d: %s; proceeding with result processing",
            resource_label,
            resource_id,
            e,
            exc_info=True,
        )

    if result.status == ResultStatus.STALE or _is_pr_result_stale(project, result, logger):
        try:
            clear_pending_task_if_matches(redis, repo, resource_type, resource_id, result.task_id)
        except Exception:
            logger.warning(
                "Failed to clear stale pending task marker for %s #%d",
                resource_label,
                resource_id,
                exc_info=True,
            )
        if not is_issue:
            try:
                clear_attempts_if_head_sha(redis, repo, resource_id, result.snapshot_head_sha)
            except Exception:
                logger.warning(
                    "Failed to clear attempt reservation for stale PR result #%d",
                    resource_id,
                    exc_info=True,
                )
        if result.status == ResultStatus.USAGE_EXHAUSTED:
            _mark_usage_exhausted_token(result, token_pool, logger)
        if token_pool is not None:
            token_pool.task_completed(result.task_id)
        return

    # Select the right GitHub functions based on resource type
    _add_label = gh.add_issue_label if is_issue else gh.add_label
    _post_comment = gh.post_issue_comment if is_issue else gh.post_comment

    # Clear the pending-task marker so the orchestrator can enqueue again
    # if needed. This applies to ALL result statuses — the task is no longer
    # pending regardless of whether it succeeded or failed.
    try:
        clear_pending_task_if_matches(redis, repo, resource_type, resource_id, result.task_id)
    except Exception as e:
        logger.error(
            f"Failed to clear pending task marker for {resource_label} #{resource_id}: {e}",
            exc_info=True,
        )

    # Completed issue tasks are resolved by removing the ready label below, so
    # their attempt counter can be cleared. Completed PR tasks are intentionally
    # different: a worker can report success without changing PR head or resolving
    # the failing condition. Leaving the per-SHA guard in place prevents unlimited
    # same-SHA retries; a real pushed commit resets the guard during discovery.
    # Do NOT clear total_attempts here — that cross-SHA circuit breaker should
    # only be reset when the PR is truly resolved (merged), not on intermediate
    # task successes.
    if result.status == ResultStatus.COMPLETED:
        if is_issue:
            try:
                clear_issue_attempts(redis, repo, resource_id)
            except Exception as e:
                logger.error(
                    f"Failed to clear attempt counter for {resource_label} #{resource_id}: {e}",
                    exc_info=True,
                )
        else:
            try:
                clear_backoff(redis, repo, resource_id)
                clear_transient_failure_count(redis, repo, resource_id)
            except Exception as e:
                logger.error(
                    f"Failed to clear transient backoff state for PR #{resource_id}: {e}",
                    exc_info=True,
                )

        # Remove orcest:ready label from completed issues so they are not
        # re-discovered on the next poll cycle.
        if is_issue:
            try:
                gh.remove_issue_label(repo, resource_id, labels.ready, token)
            except Exception as e:
                logger.error(
                    f"Failed to remove ready label from issue #{resource_id}: {e}",
                    exc_info=True,
                )
    elif result.status == ResultStatus.USAGE_EXHAUSTED:
        usage_accounting_processed = False
        try:
            usage_accounting_processed = redis.set_nx_ex(
                _make_usage_exhausted_processed_key(result.task_id),
                "1",
                _USAGE_EXHAUSTED_PROCESSED_TTL_SECONDS,
            )
            if usage_accounting_processed:
                count = redis.incr(_USAGE_EXHAUSTED_RESULT_KEY)
                if count == 1:
                    redis.expire(
                        _USAGE_EXHAUSTED_RESULT_KEY,
                        _USAGE_EXHAUSTED_RESULT_TTL_SECONDS,
                    )
        except Exception:
            logger.debug("Failed to increment usage-exhausted result counter", exc_info=True)

        # Mark the exhausted token in the pool so it's skipped in future rounds.
        # Use the resets_at timestamp from the stream-json rate_limit_event if
        # available; fall back to querying the usage endpoint; final fallback 30 min.
        _mark_usage_exhausted_token(result, token_pool, logger)
        usage_cooldown_ttl = _usage_exhausted_cooldown_ttl_seconds(result)
        # PR-specific cooldown: clear per-SHA attempts so PR can be re-enqueued
        # after the cooldown expires.  Issues don't have per-SHA counters.
        if not is_issue:
            cooldown_set = False
            try:
                set_usage_exhausted_cooldown(
                    redis,
                    repo,
                    resource_id,
                    ttl_seconds=usage_cooldown_ttl,
                )
                cooldown_set = True
            except Exception as e:
                logger.error(
                    f"Failed to set usage-exhausted cooldown for PR #{resource_id}: {e}",
                    exc_info=True,
                )
            if cooldown_set:
                try:
                    clear_attempts(redis, repo, resource_id)
                    logger.info(
                        "PR #%d: cleared per-SHA attempt counter after USAGE_EXHAUSTED",
                        resource_id,
                    )
                except Exception as e:
                    logger.error(
                        f"Failed to clear per-SHA attempt counter for PR #{resource_id} "
                        f"after USAGE_EXHAUSTED: {e}",
                        exc_info=True,
                    )
        else:
            try:
                set_issue_usage_exhausted_cooldown(
                    redis,
                    repo,
                    resource_id,
                    ttl_seconds=usage_cooldown_ttl,
                )
            except Exception as e:
                logger.error(
                    f"Failed to set usage-exhausted cooldown for issue #{resource_id}: {e}",
                    exc_info=True,
                )
            try:
                clear_issue_attempts(redis, repo, resource_id)
                logger.info(
                    "Issue #%d: cleared attempt counter after USAGE_EXHAUSTED",
                    resource_id,
                )
            except Exception as e:
                logger.error(
                    f"Failed to clear issue attempt counter for issue #{resource_id} "
                    f"after USAGE_EXHAUSTED: {e}",
                    exc_info=True,
                )

    # Transient failures (clone timeout, provider overload, worker restart)
    # should be retried automatically after backoff without burning the
    # cross-SHA PR attempt budget.
    is_transient = result.status == ResultStatus.FAILED and result.summary.startswith(
        TRANSIENT_SUMMARY_PREFIX
    )

    if is_transient:
        transient_accounting_processed = False
        try:
            transient_accounting_processed = redis.set_nx_ex(
                _make_transient_failure_processed_key(result.task_id),
                "1",
                _TRANSIENT_FAILURE_PROCESSED_TTL_SECONDS,
            )
        except Exception:
            logger.debug("Failed to mark transient failure as processed", exc_info=True)

        try:
            if is_issue:
                clear_issue_attempts(redis, repo, resource_id)
            else:
                head_sha = result.snapshot_head_sha
                try:
                    attempt_data: dict[str, str] = redis.hgetall(
                        f"pr:{repo}:{resource_id}:attempts"
                    )
                    attempt_head_sha = attempt_data.get("head_sha", "")
                    if attempt_head_sha:
                        head_sha = attempt_head_sha
                except Exception:
                    logger.debug(
                        "Failed to read attempt SHA before transient cleanup for PR #%d",
                        resource_id,
                        exc_info=True,
                    )
                if transient_accounting_processed:
                    # Each transient failure lengthens the backoff cooldown
                    # (get_backoff_cooldown_seconds clamps the step, so it
                    # plateaus rather than growing without bound). Orcest never
                    # escalates a transient failure to a human -- it just keeps
                    # retrying at the capped cadence.
                    transient_count = increment_transient_failure_count(redis, repo, resource_id)
                    set_backoff_cooldown(
                        redis,
                        repo,
                        resource_id,
                        transient_count - 1,
                        head_sha=head_sha,
                    )
                    clear_attempts(redis, repo, resource_id)
                else:
                    clear_attempts(redis, repo, resource_id)
        except Exception as e:
            logger.error(
                f"Failed to clear attempts for transient failure on "
                f"{resource_label} #{resource_id}: {e}",
                exc_info=True,
            )

    # Manage labels based on result status.
    #
    # needs-human is applied for exactly one reason: the worker's agent
    # explicitly reported a genuine human-decision blocker (result.needs_human).
    # Orcest never infers needs-human from a failure or a budget count -- an
    # ordinary fix-attempt failure is just retried on the next eligible cycle.
    labeled = False
    needs_human = result.status == ResultStatus.FAILED and result.needs_human
    if result.status == ResultStatus.FAILED and not is_transient and not is_issue:
        # The total-attempt counter only paces the retry cadence (see the
        # SKIP_MAX_TOTAL_ATTEMPTS handler); it never escalates.
        try:
            increment_total_attempts(redis, repo, resource_id)
        except Exception as e:
            logger.error(
                f"Failed to increment total-attempt counter for PR #{resource_id}: {e}",
                exc_info=True,
            )
    if needs_human:
        try:
            _add_label(repo, resource_id, labels.needs_human, token)
            labeled = True
        except Exception as e:
            logger.error(
                f"Failed to add needs-human label on {resource_label} #{resource_id}: {e}",
                exc_info=True,
            )
    elif result.status == ResultStatus.BLOCKED:
        try:
            _add_label(repo, resource_id, labels.blocked, token)
            labeled = True
        except Exception as e:
            logger.error(
                f"Failed to add blocked label on {resource_label} #{resource_id}: {e}",
                exc_info=True,
            )

    # Only post comments for non-success statuses (failures, blocked, etc.)
    # Success is silent to avoid comment noise on PRs/issues.
    if result.status != ResultStatus.COMPLETED:
        safe_summary = result.summary[:500] if result.summary else ""

        if result.status == ResultStatus.FAILED and not needs_human:
            # An ordinary (transient or otherwise) fix-attempt failure is
            # retried automatically -- stay silent to avoid accumulating
            # comment noise across attempts.
            if token_pool is not None:
                token_pool.task_completed(result.task_id)
            return
        elif result.status == ResultStatus.FAILED:
            # needs_human is True: the worker's agent explicitly asked for a
            # human decision. Surface its reason so the human knows what to do.
            reason = result.needs_human_reason or "(no reason given)"
            label_note = (
                f"Labeled `{labels.needs_human}`."
                if labeled
                else f"Failed to add `{labels.needs_human}` label — please add it manually."
            )
            body = (
                f"**orcest** needs a human decision on this PR.\n\n"
                f"The worker reported: {reason}\n\n"
                f"{label_note}"
            )
        elif result.status == ResultStatus.BLOCKED:
            label_note = (
                f"Labeling as `{labels.blocked}` — waiting for external input."
                if labeled
                else f"Failed to add `{labels.blocked}` label — please triage manually."
            )
            body = (
                f"**orcest** task `{result.task_id}` is blocked "
                f"({result.duration_seconds}s, "
                f"worker: {result.worker_id}).\n\n"
                f"Summary: {safe_summary}\n\n"
                f"{label_note}"
            )
        elif result.status == ResultStatus.USAGE_EXHAUSTED:
            branch_note = (
                f"Work saved on branch `{result.branch}`. " if result.branch else "Work saved. "
            )
            body = (
                f"**orcest** task `{result.task_id}` paused "
                f"(usage limit reached, "
                f"worker: {result.worker_id}).\n\n"
                f"{branch_note}"
                f"Will resume when capacity is available."
            )
        else:
            body = (
                f"**orcest** task `{result.task_id}`: "
                f"{result.status.value} "
                f"({result.duration_seconds}s, "
                f"worker: {result.worker_id}).\n\n"
                f"Summary: {safe_summary}"
            )

        try:
            _post_comment(repo, resource_id, body, token)
        except Exception as e:
            logger.error(
                f"Failed to post comment on {resource_label} #{resource_id}: {e}",
                exc_info=True,
            )

        logger.info("Result comment: %s...", body[:100])

    # Clean up token pool tracking. mark_exhausted already pops from _task_tokens
    # for exhausted results, so task_completed is a no-op in that case (safe to call).
    if token_pool is not None:
        token_pool.task_completed(result.task_id)
