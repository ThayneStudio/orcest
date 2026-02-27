"""Orchestrator main loop.

Polls GitHub for actionable PRs, enqueues fix tasks to Redis, and consumes
results from workers. Uses graceful shutdown on SIGTERM/SIGINT with
interruptible sleep (1-second chunks) for responsive termination.
"""

import logging
import signal
import sys
import time

from orcest.orchestrator import gh
from orcest.orchestrator.pr_ops import PRAction, discover_actionable_prs
from orcest.orchestrator.task_publisher import publish_fix_task
from orcest.shared.config import OrchestratorConfig
from orcest.shared.logging import setup_logging
from orcest.shared.models import ResultStatus, TaskResult
from orcest.shared.redis_client import RedisClient

RESULTS_STREAM = "results"
RESULTS_GROUP = "orchestrator"


def run_orchestrator(config: OrchestratorConfig) -> None:
    """Main orchestrator entry point. Polls GitHub in a loop."""
    logger = setup_logging("orchestrator", "main")
    redis = RedisClient(config.redis)

    # Verify Redis connection
    if not redis.health_check():
        logger.error("Cannot connect to Redis. Exiting.")
        sys.exit(1)

    # Ensure consumer group for results stream
    redis.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    # Graceful shutdown
    shutdown = False

    def handle_signal(signum: int, frame: object) -> None:
        nonlocal shutdown
        logger.info(
            f"Received signal {signum}, shutting down gracefully..."
        )
        shutdown = True

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    logger.info(
        f"Orchestrator started. Repo: {config.github.repo}, "
        f"poll interval: {config.polling.interval}s"
    )

    while not shutdown:
        try:
            _poll_cycle(config, redis, logger)
        except Exception as e:
            logger.error(f"Poll cycle failed: {e}", exc_info=True)
            # Continue after error -- don't crash the loop

        # Wait for next cycle (interruptible in 1-second chunks)
        for _ in range(config.polling.interval):
            if shutdown:
                break
            time.sleep(1)

    logger.info("Orchestrator shut down cleanly.")


def _poll_cycle(
    config: OrchestratorConfig,
    redis: RedisClient,
    logger: logging.Logger,
) -> None:
    """Single orchestrator poll cycle."""

    # Step 1: Consume results from workers
    _consume_results(config, redis, logger)

    # Step 2: Discover PRs needing action
    pr_states = discover_actionable_prs(
        repo=config.github.repo,
        token=config.github.token,
        redis=redis,
        label_config=config.labels,
    )

    # Step 3: Enqueue tasks for actionable PRs
    for pr_state in pr_states:
        if pr_state.action == PRAction.ENQUEUE_FIX:
            logger.info(
                f"PR #{pr_state.number} ({pr_state.title}): "
                f"enqueueing fix task"
            )
            publish_fix_task(
                pr_state=pr_state,
                repo=config.github.repo,
                token=config.github.token,
                redis=redis,
                label_config=config.labels,
                logger=logger,
            )
        elif pr_state.action == PRAction.SKIP_GREEN:
            logger.debug(
                f"PR #{pr_state.number}: CI green, skipping"
            )
        elif pr_state.action == PRAction.SKIP_LOCKED:
            logger.debug(
                f"PR #{pr_state.number}: locked, skipping"
            )
        elif pr_state.action == PRAction.SKIP_LABELED:
            logger.debug(
                f"PR #{pr_state.number}: already labeled, skipping"
            )

    logger.info(
        f"Poll cycle complete. "
        f"{sum(1 for p in pr_states if p.action == PRAction.ENQUEUE_FIX)} "
        f"tasks enqueued, {len(pr_states)} PRs checked."
    )


def _consume_results(
    config: OrchestratorConfig,
    redis: RedisClient,
    logger: logging.Logger,
) -> None:
    """Consume any pending results from workers.

    Non-blocking: reads all available results without waiting.
    Uses block_ms=None for immediate return when no results are pending.
    """
    while True:
        entries = redis.xreadgroup(
            group=RESULTS_GROUP,
            consumer="orchestrator-main",
            stream=RESULTS_STREAM,
            count=10,
            block_ms=None,  # Non-blocking: return immediately
        )

        if not entries:
            break

        for entry_id, fields in entries:
            result = TaskResult.from_dict(fields)
            _handle_result(config, redis, result, logger)
            redis.xack(RESULTS_STREAM, RESULTS_GROUP, entry_id)


def _handle_result(
    config: OrchestratorConfig,
    redis: RedisClient,
    result: TaskResult,
    logger: logging.Logger,
) -> None:
    """Process a single task result.

    Posts a comment on the PR with the result summary and manages labels:
    - completed: removes queued/in-progress labels
    - failed: removes queued/in-progress labels, adds needs-human
    - usage_exhausted: keeps in-progress label (will resume)
    """
    logger.info(
        f"Result for task {result.task_id}: {result.status.value} "
        f"(worker: {result.worker_id}, {result.duration_seconds}s)"
    )

    repo = config.github.repo
    token = config.github.token
    labels = config.labels
    pr_number = result.resource_id

    # Format result comment for the PR
    if result.status == ResultStatus.COMPLETED:
        body = (
            f"**orcest** task `{result.task_id}` completed "
            f"({result.duration_seconds}s, "
            f"worker: {result.worker_id}).\n\n"
            f"Summary: {result.summary}"
        )
    elif result.status == ResultStatus.FAILED:
        body = (
            f"**orcest** task `{result.task_id}` failed "
            f"({result.duration_seconds}s, "
            f"worker: {result.worker_id}).\n\n"
            f"Summary: {result.summary}\n\n"
            f"Labeling as `orcest:needs-human` for manual review."
        )
    elif result.status == ResultStatus.USAGE_EXHAUSTED:
        body = (
            f"**orcest** task `{result.task_id}` paused "
            f"(Claude usage limit reached, "
            f"worker: {result.worker_id}).\n\n"
            f"Work saved on branch `{result.branch}`. "
            f"Will resume when capacity is available."
        )
    else:
        body = (
            f"**orcest** task `{result.task_id}`: "
            f"{result.status.value}"
        )

    # Post comment on the PR
    try:
        gh.post_comment(repo, pr_number, body, token)
    except Exception as e:
        logger.error(
            f"Failed to post comment on PR #{pr_number}: {e}"
        )

    # Manage labels based on result status.
    # Label lifecycle (from spec section 5):
    #   completed      -> remove queued + in-progress
    #   failed         -> remove queued + in-progress, add needs-human
    #   usage_exhausted -> keep in-progress (will resume when capacity available)
    try:
        if result.status == ResultStatus.USAGE_EXHAUSTED:
            # Keep in-progress label so PR is not re-enqueued.
            # Swap queued -> in-progress if still queued.
            gh.remove_label(repo, pr_number, labels.queued, token)
            gh.add_label(repo, pr_number, labels.in_progress, token)
        else:
            gh.remove_label(repo, pr_number, labels.queued, token)
            gh.remove_label(repo, pr_number, labels.in_progress, token)

            if result.status == ResultStatus.FAILED:
                gh.add_label(repo, pr_number, labels.needs_human, token)
    except Exception as e:
        logger.error(
            f"Failed to manage labels on PR #{pr_number}: {e}"
        )

    logger.info(f"Result comment: {body[:100]}...")
