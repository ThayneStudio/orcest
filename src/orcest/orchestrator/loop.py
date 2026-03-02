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
from orcest.orchestrator.issue_ops import IssueAction, discover_actionable_issues
from orcest.orchestrator.issue_ops import clear_attempts as clear_issue_attempts
from orcest.orchestrator.pr_ops import PRAction, clear_attempts, discover_actionable_prs
from orcest.orchestrator.task_publisher import (
    publish_fix_task,
    publish_followup_task,
    publish_issue_task,
)
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
        logger.info("Received signal %d, shutting down gracefully...", signum)
        shutdown = True

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    logger.info(
        "Orchestrator started. Repo: %s, poll interval: %ds",
        config.github.repo,
        config.polling.interval,
    )

    while not shutdown:
        try:
            _poll_cycle(config, redis, logger)
        except Exception as e:
            logger.error("Poll cycle failed: %s", e, exc_info=True)
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
            logger.info("PR #%d (%s): merging", pr_state.number, pr_state.title)
            try:
                gh.merge_pr(
                    config.github.repo,
                    pr_state.number,
                    config.github.token,
                    delete_branch=config.delete_branch_on_merge,
                )
                merged += 1
            except Exception as e:
                logger.error(
                    "Failed to merge PR #%d: %s",
                    pr_state.number,
                    e,
                    exc_info=True,
                )
                labeled = False
                try:
                    gh.add_label(
                        config.github.repo,
                        pr_state.number,
                        config.labels.needs_human,
                        config.github.token,
                    )
                    labeled = True
                except Exception as label_err:
                    logger.error(
                        "Failed to label PR #%d after merge failure: %s",
                        pr_state.number,
                        label_err,
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
                        else f"Failed to add `{config.labels.needs_human}` "
                        f"label — please triage manually."
                    )
                    gh.post_comment(
                        config.github.repo,
                        pr_state.number,
                        f"**orcest** failed to merge this PR: {safe_err}\n\n{label_note}",
                        config.github.token,
                    )
                except Exception as comment_err:
                    logger.error(
                        "Failed to comment on PR #%d after merge failure: %s",
                        pr_state.number,
                        comment_err,
                        exc_info=True,
                    )
            else:
                try:
                    gh.post_comment(
                        config.github.repo,
                        pr_state.number,
                        "**orcest** merged this PR.",
                        config.github.token,
                    )
                except Exception as comment_err:
                    logger.warning(
                        "Merged PR #%d but failed to post comment: %s",
                        pr_state.number,
                        comment_err,
                        exc_info=True,
                    )
        elif pr_state.action == PRAction.ENQUEUE_FIX:
            logger.info("PR #%d (%s): enqueueing fix task", pr_state.number, pr_state.title)
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
                    "Failed to publish fix task for PR #%d: %s",
                    pr_state.number,
                    e,
                    exc_info=True,
                )
        elif pr_state.action == PRAction.ENQUEUE_FOLLOWUP:
            logger.info("PR #%d (%s): enqueueing followup triage", pr_state.number, pr_state.title)
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
                    "Failed to publish followup task for PR #%d: %s",
                    pr_state.number,
                    e,
                    exc_info=True,
                )
        elif pr_state.action == PRAction.SKIP_GREEN:
            logger.debug("PR #%d: CI green, skipping", pr_state.number)
        elif pr_state.action == PRAction.SKIP_LOCKED:
            logger.debug("PR #%d: locked, skipping", pr_state.number)
        elif pr_state.action == PRAction.SKIP_MAX_ATTEMPTS:
            logger.warning(
                "PR #%d: max attempts reached, adding needs-human label", pr_state.number
            )
            labeled = False
            try:
                gh.add_label(
                    config.github.repo,
                    pr_state.number,
                    config.labels.needs_human,
                    config.github.token,
                )
                labeled = True
            except Exception as e:
                logger.error(
                    "Failed to label PR #%d as needs-human: %s",
                    pr_state.number,
                    e,
                    exc_info=True,
                )
            try:
                label_note = (
                    f"Labeling as `{config.labels.needs_human}` for manual review."
                    if labeled
                    else f"Failed to add `{config.labels.needs_human}` "
                    f"label — please triage manually."
                )
                gh.post_comment(
                    config.github.repo,
                    pr_state.number,
                    f"**orcest** has exhausted its retry budget "
                    f"({config.max_attempts} attempts) for this PR. "
                    f"{label_note}\n\nPush a new commit to reset "
                    f"the counter and allow orcest to try again.",
                    config.github.token,
                )
            except Exception as e:
                logger.error(
                    "Failed to comment on PR #%d about max attempts: %s",
                    pr_state.number,
                    e,
                    exc_info=True,
                )
        elif pr_state.action == PRAction.SKIP_DRAFT:
            logger.debug("PR #%d: draft, skipping", pr_state.number)
        elif pr_state.action == PRAction.SKIP_PENDING:
            logger.debug("PR #%d: CI pending, skipping", pr_state.number)
        elif pr_state.action == PRAction.SKIP_LABELED:
            logger.debug("PR #%d: already labeled, skipping", pr_state.number)
        else:
            logger.warning(
                "PR #%d: unhandled action %r, skipping", pr_state.number, pr_state.action
            )

    # Step 4: Discover issues needing implementation
    try:
        issue_states = discover_actionable_issues(
            repo=config.github.repo,
            token=config.github.token,
            redis=redis,
            label_config=config.labels,
            max_attempts=config.max_attempts,
        )
    except Exception as e:
        logger.error("Issue discovery failed: %s", e, exc_info=True)
        issue_states = []

    # Step 5: Act on issues
    for issue_state in issue_states:
        if issue_state.action == IssueAction.ENQUEUE_IMPLEMENT:
            logger.info(
                "Issue #%d (%s): enqueueing implementation task",
                issue_state.number,
                issue_state.title,
            )
            try:
                publish_issue_task(
                    issue_state=issue_state,
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
                    "Failed to publish issue task for issue #%d: %s",
                    issue_state.number,
                    e,
                    exc_info=True,
                )
        elif issue_state.action == IssueAction.SKIP_MAX_ATTEMPTS:
            logger.warning(
                "Issue #%d: max attempts reached, adding needs-human label",
                issue_state.number,
            )
            try:
                gh.add_issue_label(
                    config.github.repo,
                    issue_state.number,
                    config.labels.needs_human,
                    config.github.token,
                )
            except Exception as e:
                logger.error(
                    "Failed to label issue #%d as needs-human: %s",
                    issue_state.number,
                    e,
                    exc_info=True,
                )
            try:
                gh.post_issue_comment(
                    config.github.repo,
                    issue_state.number,
                    f"**orcest** has exhausted its retry budget "
                    f"({config.max_attempts} attempts) for this issue. "
                    f"Labeling as `{config.labels.needs_human}` for manual review.",
                    config.github.token,
                )
            except Exception as e:
                logger.error(
                    "Failed to comment on issue #%d about max attempts: %s",
                    issue_state.number,
                    e,
                    exc_info=True,
                )
        elif issue_state.action == IssueAction.SKIP_LOCKED:
            logger.debug("Issue #%d: locked, skipping", issue_state.number)
        elif issue_state.action == IssueAction.SKIP_LABELED:
            logger.debug("Issue #%d: already labeled, skipping", issue_state.number)

    logger.info(
        "Poll cycle complete. %d tasks enqueued, %d merged, %d PRs checked, %d issues checked.",
        enqueued,
        merged,
        len(pr_states),
        len(issue_states),
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
    config: OrchestratorConfig,
    redis: RedisClient,
    result: TaskResult,
    logger: logging.Logger,
) -> None:
    """Process a single task result.

    Posts a comment on the resource (PR or issue) with the result summary
    and manages labels:
    - completed: removes queued/in-progress labels
    - failed: removes queued/in-progress labels, adds needs-human
    - usage_exhausted: keeps in-progress label (will resume)
    """
    logger.info(
        "Result for task %s: %s (worker: %s, %ss)",
        result.task_id,
        result.status.value,
        result.worker_id,
        result.duration_seconds,
    )

    repo = config.github.repo
    token = config.github.token
    labels = config.labels
    resource_id = result.resource_id
    is_issue = result.resource_type == "issue"
    resource_label = "issue" if is_issue else "PR"

    # Select the right GitHub functions based on resource type
    _add_label = gh.add_issue_label if is_issue else gh.add_label
    _remove_label = gh.remove_issue_label if is_issue else gh.remove_label
    _post_comment = gh.post_issue_comment if is_issue else gh.post_comment

    # Clear attempt counter on success so future failures start fresh.
    if result.status == ResultStatus.COMPLETED:
        try:
            if is_issue:
                clear_issue_attempts(redis, resource_id)
            else:
                clear_attempts(redis, resource_id)
        except Exception as e:
            logger.error(
                "Failed to clear attempt counter for %s #%s: %s",
                resource_label,
                resource_id,
                e,
                exc_info=True,
            )

    # Manage labels based on result status.
    labeled = False
    if result.status == ResultStatus.USAGE_EXHAUSTED:
        try:
            _remove_label(repo, resource_id, labels.queued, token)
        except Exception as e:
            logger.error(
                "Failed to remove queued label on %s #%s: %s",
                resource_label,
                resource_id,
                e,
                exc_info=True,
            )
        try:
            _add_label(repo, resource_id, labels.in_progress, token)
        except Exception as e:
            logger.error(
                "Failed to add in-progress label on %s #%s: %s",
                resource_label,
                resource_id,
                e,
                exc_info=True,
            )
    else:
        try:
            _remove_label(repo, resource_id, labels.queued, token)
        except Exception as e:
            logger.error(
                "Failed to remove queued label on %s #%s: %s",
                resource_label,
                resource_id,
                e,
                exc_info=True,
            )
        try:
            _remove_label(repo, resource_id, labels.in_progress, token)
        except Exception as e:
            logger.error(
                "Failed to remove in-progress label on %s #%s: %s",
                resource_label,
                resource_id,
                e,
                exc_info=True,
            )

        if result.status == ResultStatus.FAILED:
            try:
                _add_label(repo, resource_id, labels.needs_human, token)
                labeled = True
            except Exception as e:
                logger.error(
                    "Failed to add needs-human label on %s #%s: %s",
                    resource_label,
                    resource_id,
                    e,
                    exc_info=True,
                )
        elif result.status == ResultStatus.BLOCKED:
            try:
                _add_label(repo, resource_id, labels.blocked, token)
            except Exception as e:
                logger.error(
                    "Failed to add blocked label on %s #%s: %s",
                    resource_label,
                    resource_id,
                    e,
                    exc_info=True,
                )

    # Format result comment
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
            else f"Failed to add `{labels.needs_human}` label — please triage manually."
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

    # Post comment on the resource
    try:
        _post_comment(repo, resource_id, body, token)
    except Exception as e:
        logger.error(
            "Failed to post comment on %s #%s: %s",
            resource_label,
            resource_id,
            e,
            exc_info=True,
        )

    logger.info("Result comment: %s...", body[:100])
