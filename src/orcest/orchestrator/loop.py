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
from orcest.orchestrator.pr_ops import PRAction, clear_attempts, discover_actionable_prs
from orcest.orchestrator.task_publisher import publish_fix_task, publish_followup_task
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
        max_attempts=config.max_attempts,
    )

    # Step 3: Act on PRs
    enqueued = 0
    merged = 0
    for pr_state in pr_states:
        if pr_state.action == PRAction.MERGE:
            logger.info(
                f"PR #{pr_state.number} ({pr_state.title}): merging"
            )
            try:
                gh.merge_pr(
                    config.github.repo, pr_state.number,
                    config.github.token,
                )
                merged += 1
            except Exception as e:
                logger.error(
                    f"Failed to merge PR #{pr_state.number}: {e}",
                    exc_info=True,
                )
                labeled = False
                try:
                    gh.add_label(
                        config.github.repo, pr_state.number,
                        config.labels.needs_human, config.github.token,
                    )
                    labeled = True
                except Exception as label_err:
                    logger.error(
                        f"Failed to label PR #{pr_state.number} after "
                        f"merge failure: {label_err}",
                        exc_info=True,
                    )
                try:
                    # Truncate error message to avoid leaking verbose
                    # subprocess stderr (which could contain internal details)
                    # into a public PR comment.
                    safe_err = str(e)[:200]
                    label_note = (
                        f"Labeling as `{config.labels.needs_human}` for manual review."
                        if labeled
                        else
                        f"Failed to add `{config.labels.needs_human}` "
                        f"label — please triage manually."
                    )
                    gh.post_comment(
                        config.github.repo, pr_state.number,
                        f"**orcest** failed to merge this PR: {safe_err}\n\n"
                        f"{label_note}",
                        config.github.token,
                    )
                except Exception as comment_err:
                    logger.error(
                        f"Failed to comment on PR #{pr_state.number} "
                        f"after merge failure: {comment_err}",
                        exc_info=True,
                    )
            else:
                try:
                    gh.post_comment(
                        config.github.repo, pr_state.number,
                        "**orcest** merged this PR.",
                        config.github.token,
                    )
                except Exception as comment_err:
                    logger.warning(
                        f"Merged PR #{pr_state.number} but failed to "
                        f"post comment: {comment_err}",
                        exc_info=True,
                    )
        elif pr_state.action == PRAction.ENQUEUE_FIX:
            logger.info(
                f"PR #{pr_state.number} ({pr_state.title}): "
                f"enqueueing fix task"
            )
            try:
                publish_fix_task(
                    pr_state=pr_state,
                    repo=config.github.repo,
                    token=config.github.token,
                    redis=redis,
                    label_config=config.labels,
                    default_runner=config.default_runner,
                    logger=logger,
                )
                enqueued += 1
            except Exception as e:
                logger.error(
                    f"Failed to publish fix task for PR "
                    f"#{pr_state.number}: {e}",
                    exc_info=True,
                )
        elif pr_state.action == PRAction.ENQUEUE_FOLLOWUP:
            logger.info(
                f"PR #{pr_state.number} ({pr_state.title}): "
                f"enqueueing followup triage"
            )
            try:
                publish_followup_task(
                    pr_state=pr_state,
                    repo=config.github.repo,
                    token=config.github.token,
                    redis=redis,
                    label_config=config.labels,
                    default_runner=config.default_runner,
                    logger=logger,
                )
                enqueued += 1
            except Exception as e:
                logger.error(
                    f"Failed to publish followup task for PR "
                    f"#{pr_state.number}: {e}",
                    exc_info=True,
                )
        elif pr_state.action == PRAction.SKIP_GREEN:
            logger.debug(
                f"PR #{pr_state.number}: CI green, skipping"
            )
        elif pr_state.action == PRAction.SKIP_LOCKED:
            logger.debug(
                f"PR #{pr_state.number}: locked, skipping"
            )
        elif pr_state.action == PRAction.SKIP_MAX_ATTEMPTS:
            logger.warning(
                f"PR #{pr_state.number}: max attempts reached, "
                f"adding needs-human label"
            )
            labeled = False
            try:
                gh.add_label(
                    config.github.repo, pr_state.number,
                    config.labels.needs_human, config.github.token,
                )
                labeled = True
            except Exception as e:
                logger.error(
                    f"Failed to label PR #{pr_state.number} as "
                    f"needs-human: {e}",
                    exc_info=True,
                )
            try:
                label_note = (
                    f"Labeling as `{config.labels.needs_human}` for "
                    f"manual review."
                    if labeled
                    else
                    f"Failed to add `{config.labels.needs_human}` "
                    f"label — please triage manually."
                )
                gh.post_comment(
                    config.github.repo, pr_state.number,
                    f"**orcest** has exhausted its retry budget "
                    f"({config.max_attempts} attempts) for this PR. "
                    f"{label_note}\n\nPush a new commit to reset "
                    f"the counter and allow orcest to try again.",
                    config.github.token,
                )
            except Exception as e:
                logger.error(
                    f"Failed to comment on PR #{pr_state.number} "
                    f"about max attempts: {e}",
                    exc_info=True,
                )
        elif pr_state.action == PRAction.SKIP_DRAFT:
            logger.debug(
                f"PR #{pr_state.number}: draft, skipping"
            )
        elif pr_state.action == PRAction.SKIP_PENDING:
            logger.debug(
                f"PR #{pr_state.number}: CI pending, skipping"
            )
        elif pr_state.action == PRAction.SKIP_LABELED:
            logger.debug(
                f"PR #{pr_state.number}: already labeled, skipping"
            )
        else:
            logger.warning(
                f"PR #{pr_state.number}: unhandled action "
                f"{pr_state.action!r}, skipping"
            )

    logger.info(
        f"Poll cycle complete. "
        f"{enqueued} tasks enqueued, {merged} merged, "
        f"{len(pr_states)} PRs checked."
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
            try:
                result = TaskResult.from_dict(fields)
                _handle_result(config, redis, result, logger)
            except Exception as e:
                logger.error(
                    f"Failed to process result entry {entry_id}: {e}",
                    exc_info=True,
                )
            # Always ACK to prevent infinite reprocessing of
            # malformed or unhandleable entries.
            try:
                redis.xack(RESULTS_STREAM, RESULTS_GROUP, entry_id)
            except Exception as ack_err:
                logger.error(
                    f"Failed to ACK result entry {entry_id}: {ack_err}",
                    exc_info=True,
                )


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

    # Clear attempt counter on success so future failures start fresh.
    # Wrapped in try/except so a Redis hiccup does not prevent label
    # management and comment posting below.
    if result.status == ResultStatus.COMPLETED:
        try:
            clear_attempts(redis, pr_number)
        except Exception as e:
            logger.error(
                f"Failed to clear attempt counter for PR #{pr_number}: {e}",
                exc_info=True,
            )

    # Manage labels based on result status.
    # Label lifecycle (from spec section 5):
    #   completed      -> remove queued + in-progress
    #   failed         -> remove queued + in-progress, add needs-human
    #   usage_exhausted -> keep in-progress (will resume when capacity available)
    #
    # Each label operation is independent so that a failure in one
    # (e.g. transient GitHub API error) does not prevent the others.
    labeled = False
    if result.status == ResultStatus.USAGE_EXHAUSTED:
        # Keep in-progress label so PR is not re-enqueued.
        # Swap queued -> in-progress if still queued.
        try:
            gh.remove_label(repo, pr_number, labels.queued, token)
        except Exception as e:
            logger.error(
                f"Failed to remove queued label on PR #{pr_number}: {e}",
                exc_info=True,
            )
        try:
            gh.add_label(repo, pr_number, labels.in_progress, token)
        except Exception as e:
            logger.error(
                f"Failed to add in-progress label on PR #{pr_number}: {e}",
                exc_info=True,
            )
    else:
        try:
            gh.remove_label(repo, pr_number, labels.queued, token)
        except Exception as e:
            logger.error(
                f"Failed to remove queued label on PR #{pr_number}: {e}",
                exc_info=True,
            )
        try:
            gh.remove_label(repo, pr_number, labels.in_progress, token)
        except Exception as e:
            logger.error(
                f"Failed to remove in-progress label on PR "
                f"#{pr_number}: {e}",
                exc_info=True,
            )

        if result.status == ResultStatus.FAILED:
            try:
                gh.add_label(
                    repo, pr_number, labels.needs_human, token,
                )
                labeled = True
            except Exception as e:
                logger.error(
                    f"Failed to add needs-human label on PR "
                    f"#{pr_number}: {e}",
                    exc_info=True,
                )
        elif result.status == ResultStatus.BLOCKED:
            try:
                gh.add_label(
                    repo, pr_number, labels.blocked, token,
                )
            except Exception as e:
                logger.error(
                    f"Failed to add blocked label on PR "
                    f"#{pr_number}: {e}",
                    exc_info=True,
                )

    # Format result comment for the PR (after label management so
    # the comment accurately reflects whether labeling succeeded).
    # Truncate summary to avoid posting excessively long PR comments
    # (e.g. if a worker exception stringifies into a large traceback).
    safe_summary = result.summary[:500] if result.summary else ""

    if result.status == ResultStatus.COMPLETED:
        body = (
            f"**orcest** task `{result.task_id}` completed "
            f"({result.duration_seconds}s, "
            f"worker: {result.worker_id}).\n\n"
            f"Summary: {safe_summary}"
        )
    elif result.status == ResultStatus.FAILED:
        label_note = (
            f"Labeling as `{labels.needs_human}` for manual review."
            if labeled
            else
            f"Failed to add `{labels.needs_human}` "
            f"label — please triage manually."
        )
        body = (
            f"**orcest** task `{result.task_id}` failed "
            f"({result.duration_seconds}s, "
            f"worker: {result.worker_id}).\n\n"
            f"Summary: {safe_summary}\n\n"
            f"{label_note}"
        )
    elif result.status == ResultStatus.USAGE_EXHAUSTED:
        branch_note = (
            f"Work saved on branch `{result.branch}`. "
            if result.branch
            else "Work saved. "
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

    # Post comment on the PR
    try:
        gh.post_comment(repo, pr_number, body, token)
    except Exception as e:
        logger.error(
            f"Failed to post comment on PR #{pr_number}: {e}",
            exc_info=True,
        )

    logger.info(f"Result comment: {body[:100]}...")
