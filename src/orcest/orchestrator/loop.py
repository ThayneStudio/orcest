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
from orcest.orchestrator.deployment import DeploymentError, run_deployment
from orcest.orchestrator.issue_ops import IssueAction, discover_actionable_issues
from orcest.orchestrator.issue_ops import clear_attempts as clear_issue_attempts
from orcest.orchestrator.pr_ops import (
    PRAction,
    clear_attempts,
    clear_review_retrigger,
    clear_total_attempts,
    discover_actionable_prs,
    set_exhausted_notified,
    set_review_retrigger_sha,
)
from orcest.orchestrator.task_publisher import (
    publish_fix_task,
    publish_followup_task,
    publish_issue_task,
    publish_rebase_task,
)
from orcest.shared.config import OrchestratorConfig
from orcest.shared.coordination import clear_pending_task
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
        max_total_attempts=config.max_total_attempts,
        stale_pending_timeout_seconds=config.stale_pending_timeout_seconds,
    )

    # Step 3: Act on PRs
    # Sort: merges first (quick wins), then fixes/followups oldest-first
    # (lowest PR number = longest waiting). Skips don't matter but sort
    # them last so actionable items are processed first.
    _ACTION_PRIORITY = {
        PRAction.MERGE: 0,
        PRAction.ENQUEUE_FIX: 1,
        PRAction.ENQUEUE_FOLLOWUP: 1,
        PRAction.ENQUEUE_REBASE: 1,
    }
    pr_states.sort(key=lambda ps: (_ACTION_PRIORITY.get(ps.action, 9), ps.number))

    # Pre-compute issue queue depth for gating issue discovery.
    issue_tasks_stream = f"tasks:issue:{config.default_runner}"
    issue_queue_depth = redis.stream_queue_depth(issue_tasks_stream, "workers")

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
                err_msg = str(e)
                logger.error(
                    f"Failed to merge PR #{pr_state.number}: {err_msg}",
                    exc_info=True,
                )
                # If the error looks like a merge conflict, enqueue a
                # rebase task so a worker can resolve it automatically.
                is_conflict = (
                    "is not mergeable" in err_msg or "cannot be cleanly created" in err_msg
                )
                if is_conflict:
                    logger.info(
                        f"PR #{pr_state.number}: merge conflict detected, enqueueing rebase task"
                    )
                    try:
                        publish_rebase_task(
                            pr_state=pr_state,
                            repo=config.github.repo,
                            token=config.github.token,
                            redis=redis,
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
                            f"**orcest** failed to merge this PR: {safe_err}\n\n{label_note}",
                            config.github.token,
                        )
                    except Exception as comment_err:
                        logger.error(
                            f"Failed to comment on PR #{pr_state.number} "
                            f"after merge failure: {comment_err}",
                            exc_info=True,
                        )
            else:
                # Clean up state on successful merge
                try:
                    clear_review_retrigger(redis, pr_state.number)
                except Exception:
                    pass  # Best-effort cleanup; key has TTL anyway
                try:
                    clear_total_attempts(redis, pr_state.number)
                except Exception:
                    pass  # Best-effort cleanup; key has TTL anyway
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
                # Run deployment if configured (run_deployment is a no-op when disabled)
                try:
                    if run_deployment(config.deployment, pr_state.number, logger):
                        logger.info("PR #%d: deployment succeeded", pr_state.number)
                except DeploymentError as deploy_err:
                    err_msg = str(deploy_err)
                    logger.error("PR #%d: deployment failed: %s", pr_state.number, err_msg)
                    try:
                        issue_number = gh.create_issue(
                            config.github.repo,
                            f"Deployment failed after merge of PR #{pr_state.number}",
                            f"**orcest** deployment failed after merging "
                            f"PR #{pr_state.number} ({pr_state.title}).\n\n"
                            f"Error: {err_msg[:500]}",
                            config.github.token,
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
        elif pr_state.action == PRAction.ENQUEUE_FIX:
            logger.info("PR #%d (%s): enqueueing fix task", pr_state.number, pr_state.title)
            try:
                publish_fix_task(
                    pr_state=pr_state,
                    repo=config.github.repo,
                    token=config.github.token,
                    redis=redis,
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
        elif pr_state.action == PRAction.ENQUEUE_REBASE:
            logger.info(
                "PR #%d (%s): merge conflicts detected, enqueueing rebase task",
                pr_state.number,
                pr_state.title,
            )
            try:
                publish_rebase_task(
                    pr_state=pr_state,
                    repo=config.github.repo,
                    token=config.github.token,
                    redis=redis,
                    default_runner=config.default_runner,
                    logger=logger,
                )
                enqueued += 1
            except Exception as e:
                logger.error(
                    "Failed to publish rebase task for PR #%d: %s",
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
            else:
                run_id = pr_state.review_run_id
                logger.info(
                    "PR #%d: claude-review passed but no formal review, re-triggering run %d",
                    pr_state.number,
                    run_id,
                )
                try:
                    gh.rerun_workflow(
                        config.github.repo,
                        run_id,
                        config.github.token,
                    )
                    set_review_retrigger_sha(redis, pr_state.number, pr_state.head_sha)
                except Exception as e:
                    logger.error(
                        "Failed to re-trigger review for PR #%d: %s",
                        pr_state.number,
                        e,
                        exc_info=True,
                    )
        elif pr_state.action == PRAction.RETRIGGER_STALE_CHECKS:
            run_ids = pr_state.stale_run_ids
            if not run_ids:
                # Stale pending checks found but no re-triggerable run IDs
                # (e.g. StatusContext checks). Escalate to needs-human.
                logger.warning(
                    "PR #%d: stale pending checks with no re-triggerable run IDs; "
                    "adding needs-human label",
                    pr_state.number,
                )
                try:
                    gh.add_label(
                        config.github.repo,
                        pr_state.number,
                        config.labels.needs_human,
                        config.github.token,
                    )
                except Exception as e:
                    logger.error(
                        "Failed to add needs-human label to PR #%d: %s",
                        pr_state.number,
                        e,
                        exc_info=True,
                    )
                try:
                    gh.post_comment(
                        config.github.repo,
                        pr_state.number,
                        f"**orcest** detected stale CI checks that have been pending for "
                        f"more than {config.stale_pending_timeout_seconds // 3600}h but "
                        f"could not re-trigger them automatically. "
                        f"Please investigate the stuck checks manually.",
                        config.github.token,
                    )
                except Exception as e:
                    logger.error(
                        "Failed to comment on PR #%d about stale checks: %s",
                        pr_state.number,
                        e,
                        exc_info=True,
                    )
            else:
                logger.warning(
                    "PR #%d: %d pending check(s) stuck (>%ds), re-triggering run(s) %s",
                    pr_state.number,
                    len(pr_state.stale_run_ids),
                    config.stale_pending_timeout_seconds,
                    run_ids,
                )
                for run_id in run_ids:
                    try:
                        gh.rerun_workflow(
                            config.github.repo,
                            run_id,
                            config.github.token,
                        )
                        logger.info(
                            "PR #%d: re-triggered stale workflow run %d",
                            pr_state.number,
                            run_id,
                        )
                    except Exception as e:
                        logger.error(
                            "Failed to re-trigger stale run %d for PR #%d: %s",
                            run_id,
                            pr_state.number,
                            e,
                            exc_info=True,
                        )
                try:
                    gh.post_comment(
                        config.github.repo,
                        pr_state.number,
                        f"**orcest** detected CI checks stuck in pending state for more than "
                        f"{config.stale_pending_timeout_seconds // 3600}h. "
                        f"Re-triggering {len(run_ids)} workflow run(s) to self-heal.",
                        config.github.token,
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
        elif pr_state.action == PRAction.SKIP_MAX_TOTAL_ATTEMPTS:
            logger.warning(
                "PR #%d: total attempt limit reached (%d), adding needs-human label",
                pr_state.number,
                config.max_total_attempts,
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
            if labeled:
                try:
                    set_exhausted_notified(redis, pr_state.number)
                except Exception as e:
                    logger.error(
                        "Failed to set exhausted_notified flag for PR #%d: %s",
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
                    f"**orcest** has exhausted its total retry budget "
                    f"({config.max_total_attempts} attempts across all commits) "
                    f"for this PR. {label_note}\n\n"
                    f"Remove the `{config.labels.needs_human}` label to allow "
                    f"orcest to try again.",
                    config.github.token,
                )
            except Exception as e:
                logger.error(
                    "Failed to comment on PR #%d about max total attempts: %s",
                    pr_state.number,
                    e,
                    exc_info=True,
                )
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
        else:
            logger.warning(
                "PR #%d: unhandled action %r, skipping", pr_state.number, pr_state.action
            )

    # Step 4: Discover issues needing implementation
    # Prioritize existing PRs over new issue work. PRs with terminal
    # labels (needs-human/blocked) are parked and don't block issue work.
    pr_work_pending = any(
        pr_state.action
        in (
            PRAction.ENQUEUE_FIX,
            PRAction.ENQUEUE_FOLLOWUP,
            PRAction.ENQUEUE_REBASE,
            PRAction.SKIP_LOCKED,
            PRAction.SKIP_ACTIVE,
            PRAction.SKIP_QUEUED,
        )
        for pr_state in pr_states
    )

    issue_states: list = []
    if pr_work_pending:
        logger.info("PRs need attention, deferring issue discovery until PR backlog clears")
    elif issue_queue_depth > 0:
        logger.info(
            f"Issue task queue has {issue_queue_depth} pending entries, "
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
                f"Issue #{issue_state.number} ({issue_state.title}): enqueueing implementation task"
            )
            try:
                publish_issue_task(
                    issue_state=issue_state,
                    repo=config.github.repo,
                    token=config.github.token,
                    redis=redis,
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
                f"Issue #{issue_state.number}: max attempts reached, adding needs-human label"
            )
            labeled = False
            try:
                gh.add_issue_label(
                    config.github.repo,
                    issue_state.number,
                    config.labels.needs_human,
                    config.github.token,
                )
                labeled = True
            except Exception as e:
                logger.error(
                    f"Failed to label issue #{issue_state.number} as needs-human: {e}",
                    exc_info=True,
                )
            try:
                label_note = (
                    f"Labeling as `{config.labels.needs_human}` for manual review."
                    if labeled
                    else f"Failed to add `{config.labels.needs_human}` "
                    f"label — please triage manually."
                )
                gh.post_issue_comment(
                    config.github.repo,
                    issue_state.number,
                    f"**orcest** has exhausted its retry budget "
                    f"({config.max_attempts} attempts) for this issue. "
                    f"{label_note}",
                    config.github.token,
                )
            except Exception as e:
                logger.error(
                    f"Failed to comment on issue #{issue_state.number} about max attempts: {e}",
                    exc_info=True,
                )
        elif issue_state.action == IssueAction.SKIP_QUEUED:
            logger.debug(f"Issue #{issue_state.number}: task already queued, skipping")
        elif issue_state.action == IssueAction.SKIP_LOCKED:
            logger.debug(f"Issue #{issue_state.number}: locked, skipping")
        elif issue_state.action == IssueAction.SKIP_ACTIVE:
            logger.debug(f"Issue #{issue_state.number}: task in flight, skipping")
        elif issue_state.action == IssueAction.SKIP_LABELED:
            logger.debug(f"Issue #{issue_state.number}: terminal label, skipping")
        else:
            logger.warning(
                f"Issue #{issue_state.number}: unhandled action {issue_state.action!r}, skipping"
            )

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
    - completed: clears attempt counter
    - failed: adds needs-human label
    - blocked: adds blocked label
    - usage_exhausted: no label changes (task stays parked as SKIP_ACTIVE)
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
    resource_type = result.resource_type or ("issue" if is_issue else "pr")

    # Select the right GitHub functions based on resource type
    _add_label = gh.add_issue_label if is_issue else gh.add_label
    _post_comment = gh.post_issue_comment if is_issue else gh.post_comment

    # Clear the pending-task marker so the orchestrator can enqueue again
    # if needed. This applies to ALL result statuses — the task is no longer
    # pending regardless of whether it succeeded or failed.
    try:
        clear_pending_task(redis, repo, resource_type, resource_id)
    except Exception as e:
        logger.error(
            f"Failed to clear pending task marker for {resource_label} #{resource_id}: {e}",
            exc_info=True,
        )

    # Clear per-SHA attempt counter on success so future failures on a new
    # SHA start fresh. Do NOT clear total_attempts here — that cross-SHA
    # circuit breaker should only be reset when the PR is truly resolved
    # (merged), not on intermediate task successes.
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

    # Manage labels based on result status.
    # Only terminal statuses (FAILED, BLOCKED) add labels.
    # USAGE_EXHAUSTED does nothing — task stays parked via attempt counter.
    labeled = False
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

        if result.status == ResultStatus.FAILED:
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
