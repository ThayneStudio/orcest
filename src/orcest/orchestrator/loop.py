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
    publish_rebase_task,
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
        logger.info(f"Received signal {signum}, shutting down gracefully...")
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
    # Sort: merges first (quick wins), then fixes/followups oldest-first
    # (lowest PR number = longest waiting). Skips don't matter but sort
    # them last so actionable items are processed first.
    _ACTION_PRIORITY = {
        PRAction.MERGE: 0,
        PRAction.ENQUEUE_FIX: 1,
        PRAction.ENQUEUE_FOLLOWUP: 1,
    }
    pr_states.sort(key=lambda ps: (_ACTION_PRIORITY.get(ps.action, 9), ps.number))

    enqueued = 0
    merged = 0
    for pr_state in pr_states:
        if pr_state.action == PRAction.MERGE:
            logger.info(f"PR #{pr_state.number} ({pr_state.title}): merging")
            try:
                gh.merge_pr(
                    config.github.repo,
                    pr_state.number,
                    config.github.token,
                )
                merged += 1
            except Exception as e:
                err_msg = str(e)
                logger.error(
                    f"Failed to merge PR #{pr_state.number}: {err_msg}",
                    exc_info=True,
                )
                # If the error looks like a merge conflict, enqueue a
                # rebase task so a worker can resolve it automatically.
                is_conflict = (
                    "is not mergeable" in err_msg
                    or "cannot be cleanly created" in err_msg
                )
                if is_conflict:
                    logger.info(
                        f"PR #{pr_state.number}: merge conflict detected, "
                        f"enqueueing rebase task"
                    )
                    try:
                        publish_rebase_task(
                            pr_state=pr_state,
                            repo=config.github.repo,
                            token=config.github.token,
                            redis=redis,
                            label_config=config.labels,
                            default_runner=config.default_runner,
                            merge_error=err_msg[:200],
                            logger=logger,
                        )
                        enqueued += 1
                    except Exception as rebase_err:
                        logger.error(
                            f"Failed to enqueue rebase task for PR #{pr_state.number}: "
                            f"{rebase_err}",
                            exc_info=True,
                        )
                        # Fall through to needs-human labeling
                        is_conflict = False

                if not is_conflict:
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
                            f"Failed to label PR #{pr_state.number} after merge failure: "
                            f"{label_err}",
                            exc_info=True,
                        )
                    try:
                        safe_err = err_msg[:200]
                        label_note = (
                            f"Labeling as `{config.labels.needs_human}` for manual review."
                            if labeled
                            else f"Failed to add `{config.labels.needs_human}` "
                            f"label — please triage manually."
                        )
                        gh.post_comment(
                            config.github.repo,
                            pr_state.number,
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
                        config.github.repo,
                        pr_state.number,
                        "**orcest** merged this PR.",
                        config.github.token,
                    )
                except Exception as comment_err:
                    logger.warning(
                        f"Merged PR #{pr_state.number} but failed to post comment: {comment_err}",
                        exc_info=True,
                    )
        elif pr_state.action == PRAction.ENQUEUE_FIX:
            logger.info(f"PR #{pr_state.number} ({pr_state.title}): enqueueing fix task")
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
                    f"Failed to publish fix task for PR #{pr_state.number}: {e}",
                    exc_info=True,
                )
        elif pr_state.action == PRAction.ENQUEUE_FOLLOWUP:
            logger.info(f"PR #{pr_state.number} ({pr_state.title}): enqueueing followup triage")
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
                    f"Failed to publish followup task for PR #{pr_state.number}: {e}",
                    exc_info=True,
                )
        elif pr_state.action == PRAction.SKIP_GREEN:
            logger.debug(f"PR #{pr_state.number}: CI green, skipping")
        elif pr_state.action == PRAction.SKIP_LOCKED:
            logger.debug(f"PR #{pr_state.number}: locked, skipping")
        elif pr_state.action == PRAction.SKIP_MAX_ATTEMPTS:
            logger.warning(f"PR #{pr_state.number}: max attempts reached, adding needs-human label")
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
                    f"Failed to label PR #{pr_state.number} as needs-human: {e}",
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
                    f"Failed to comment on PR #{pr_state.number} about max attempts: {e}",
                    exc_info=True,
                )
        elif pr_state.action == PRAction.SKIP_DRAFT:
            logger.debug(f"PR #{pr_state.number}: draft, skipping")
        elif pr_state.action == PRAction.SKIP_PENDING:
            logger.debug(f"PR #{pr_state.number}: CI pending, skipping")
        elif pr_state.action == PRAction.SKIP_LABELED:
            logger.debug(f"PR #{pr_state.number}: already labeled, skipping")
        else:
            logger.warning(f"PR #{pr_state.number}: unhandled action {pr_state.action!r}, skipping")

    # Step 4: Discover issues needing implementation
    # Prioritize existing PRs over new issue work using two checks:
    #
    # (a) GitHub state: PRs being actively worked on or just enqueued.
    #     SKIP_LABELED with needs-human/blocked labels does NOT count —
    #     those PRs are explicitly parked and shouldn't block issue work.
    _active_labels = {config.labels.queued, config.labels.in_progress}
    pr_work_pending = any(
        pr_state.action
        in (PRAction.ENQUEUE_FIX, PRAction.ENQUEUE_FOLLOWUP, PRAction.SKIP_LOCKED)
        or (
            pr_state.action == PRAction.SKIP_LABELED
            and any(lbl in _active_labels for lbl in pr_state.labels)
        )
        for pr_state in pr_states
    )
    # (b) Queue depth: tasks already waiting in Redis (from previous cycles)
    tasks_stream = f"tasks:{config.default_runner}"
    queue_depth = redis.stream_queue_depth(tasks_stream, "workers")

    issue_states: list = []
    if pr_work_pending:
        logger.info(
            "PRs need attention, deferring issue discovery until PR backlog clears"
        )
    elif queue_depth > 0:
        logger.info(
            f"Task queue has {queue_depth} pending entries, "
            f"deferring issue discovery until queue drains"
        )
    else:
        try:
            issue_states = discover_actionable_issues(
                repo=config.github.repo,
                token=config.github.token,
                redis=redis,
                label_config=config.labels,
                max_attempts=config.max_attempts,
            )
        except Exception as e:
            logger.error(f"Issue discovery failed: {e}", exc_info=True)

    # Step 5: Act on issues
    for issue_state in issue_states:
        if issue_state.action == IssueAction.ENQUEUE_IMPLEMENT:
            logger.info(
                f"Issue #{issue_state.number} ({issue_state.title}): "
                f"enqueueing implementation task"
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
                    f"Failed to publish issue task for issue #{issue_state.number}: {e}",
                    exc_info=True,
                )
        elif issue_state.action == IssueAction.SKIP_MAX_ATTEMPTS:
            logger.warning(
                f"Issue #{issue_state.number}: max attempts reached, "
                f"adding needs-human label"
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
                    f"Failed to label issue #{issue_state.number} as needs-human: {e}",
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
                    f"Failed to comment on issue #{issue_state.number} "
                    f"about max attempts: {e}",
                    exc_info=True,
                )
        elif issue_state.action == IssueAction.SKIP_LOCKED:
            logger.debug(f"Issue #{issue_state.number}: locked, skipping")
        elif issue_state.action == IssueAction.SKIP_LABELED:
            logger.debug(f"Issue #{issue_state.number}: already labeled, skipping")

    logger.info(
        f"Poll cycle complete. "
        f"{enqueued} tasks enqueued, {merged} merged, "
        f"{len(pr_states)} PRs checked, "
        f"{len(issue_states)} issues checked."
    )


def _consume_results(
    config: OrchestratorConfig,
    redis: RedisClient,
    logger: logging.Logger,
) -> None:
    """Consume any pending results from workers.

    Non-blocking: reads all available results without waiting.

    First drains pending entries (delivered but not ACKed — can happen if
    the orchestrator was restarted mid-cycle), then reads new entries.
    This prevents orphaned labels from results that were read but never
    processed after a restart.
    """
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
                _handle_result(config, redis, result, logger)
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

    Posts a comment on the resource (PR or issue) with the result summary
    and manages labels:
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
                f"Failed to clear attempt counter for {resource_label} #{resource_id}: {e}",
                exc_info=True,
            )

    # Manage labels based on result status.
    labeled = False
    if result.status == ResultStatus.USAGE_EXHAUSTED:
        try:
            _remove_label(repo, resource_id, labels.queued, token)
        except Exception as e:
            logger.error(
                f"Failed to remove queued label on {resource_label} #{resource_id}: {e}",
                exc_info=True,
            )
        try:
            _add_label(repo, resource_id, labels.in_progress, token)
        except Exception as e:
            logger.error(
                f"Failed to add in-progress label on {resource_label} #{resource_id}: {e}",
                exc_info=True,
            )
    else:
        try:
            _remove_label(repo, resource_id, labels.queued, token)
        except Exception as e:
            logger.error(
                f"Failed to remove queued label on {resource_label} #{resource_id}: {e}",
                exc_info=True,
            )
        try:
            _remove_label(repo, resource_id, labels.in_progress, token)
        except Exception as e:
            logger.error(
                f"Failed to remove in-progress label on {resource_label} #{resource_id}: {e}",
                exc_info=True,
            )

        if result.status == ResultStatus.FAILED:
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
            except Exception as e:
                logger.error(
                    f"Failed to add blocked label on {resource_label} #{resource_id}: {e}",
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
            f"Failed to post comment on {resource_label} #{resource_id}: {e}",
            exc_info=True,
        )

    logger.info(f"Result comment: {body[:100]}...")
