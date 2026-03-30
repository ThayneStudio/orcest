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
    get_stale_retrigger_sha,
    set_review_retrigger_sha,
    set_stale_retrigger_sha,
    set_usage_exhausted_cooldown,
)
from orcest.orchestrator.task_publisher import (
    publish_fix_task,
    publish_followup_task,
    publish_issue_task,
    publish_rebase_task,
)
from orcest.orchestrator.token_pool import TokenPool
from orcest.orchestrator.usage_check import get_token_reset_time
from orcest.shared.config import LabelConfig, OrchestratorConfig, ProjectConfig
from orcest.shared.coordination import (
    clear_backoff,
    clear_pending_task,
    compute_pending_task_ttl,
    get_pending_task,
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

    # Ensure consumer groups for shared task streams (so workers don't race)
    for stream in (
        f"tasks:{config.default_runner}",
        f"tasks:issue:{config.default_runner}",
    ):
        task_redis.ensure_consumer_group(stream, CONSUMER_GROUP)

    # Ensure consumer group for results stream (per-project)
    for project in config.projects:
        project_redis = RedisClient.from_client(redis.client, key_prefix=project.key_prefix)
        project_redis.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    # Create per-project token pools for round-robin Claude token distribution
    token_pools: dict[str, TokenPool] = {}
    for project in config.projects:
        tokens = project.claude_tokens
        if tokens:
            token_pools[project.key_prefix] = TokenPool(tokens)
            if len(tokens) > 1:
                logger.info(
                    "Project %s: token pool with %d Claude tokens",
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
            _poll_cycle(config, redis, task_redis, token_pools, logger, pending_task_ttl)
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
    task_redis: RedisClient,
    token_pools: dict[str, TokenPool],
    logger: logging.Logger,
    pending_task_ttl: int,
) -> None:
    """Single orchestrator poll cycle across all configured projects."""
    # Build per-project Redis clients once and reuse for results/state
    project_clients = [
        (project, RedisClient.from_client(redis.client, key_prefix=project.key_prefix))
        for project in config.projects
    ]

    # Step 1: Consume results per project
    for project, project_redis in project_clients:
        try:
            pool = token_pools.get(project.key_prefix)
            _consume_results_for_project(
                project, project_redis, config.labels, logger, token_pool=pool
            )
        except Exception:
            logger.error("Failed to consume results for %s", project.repo, exc_info=True)

    # Step 1b: Trim processed entries from streams to prevent unbounded growth.
    # Task streams are shared (single namespace), trim once per cycle.
    for stream in (
        f"tasks:{config.default_runner}",
        f"tasks:issue:{config.default_runner}",
    ):
        try:
            for g in task_redis.xinfo_groups(stream):
                last_id = g.get("last-delivered-id")
                if last_id and last_id != "0-0":
                    task_redis.xtrim_minid(stream, last_id)
        except Exception:
            pass  # Stream may not exist yet
    # Results streams are per-project, trim each one.
    for _project, project_redis in project_clients:
        try:
            for g in project_redis.xinfo_groups(RESULTS_STREAM):
                last_id = g.get("last-delivered-id")
                if last_id and last_id != "0-0":
                    project_redis.xtrim_minid(RESULTS_STREAM, last_id)
        except Exception:
            pass  # Stream may not exist yet

    # Step 2: Poll each project
    total_enqueued = 0
    total_merged = 0
    total_prs = 0
    total_issues = 0
    for project, project_redis in project_clients:
        try:
            pool = token_pools.get(project.key_prefix)
            enqueued, merged, prs_checked, issues_checked = _poll_project(
                project, project_redis, task_redis, config, logger, pending_task_ttl,
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
    token_pool: TokenPool | None = None,
) -> tuple[int, int, int, int]:
    """Poll a single project for actionable PRs and issues.

    Args:
        project_redis: Per-project Redis client (for pending markers, attempt counters, etc.).
        task_redis: Shared Redis client (for publishing tasks to the common stream).
        token_pool: Optional token pool for round-robin Claude token selection.

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
        """
        if token_pool is None:
            return project.claude_token
        return token_pool.next_token()

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
        PRAction.ENQUEUE_FIX: 1,
        PRAction.ENQUEUE_FOLLOWUP: 1,
        PRAction.ENQUEUE_REBASE: 1,
    }
    pr_states.sort(key=lambda ps: (_ACTION_PRIORITY.get(ps.action, 9), ps.number))

    # Pre-compute issue queue depth for gating issue discovery.
    issue_tasks_stream = f"tasks:issue:{config.default_runner}"
    issue_queue_depth = task_redis.stream_queue_depth(issue_tasks_stream, CONSUMER_GROUP)

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
                    ct = _select_claude_token()
                    if ct is None:
                        logger.warning(
                            "All Claude tokens exhausted, skipping rebase for PR #%d",
                            pr_state.number,
                        )
                        is_conflict = False
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
                            _register_task(task.id, ct)
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
                            repo,
                            pr_state.number,
                            labels.needs_human,
                            token,
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
                            f"Labeling as `{labels.needs_human}` for manual review."
                            if labeled
                            else f"Failed to add `{labels.needs_human}` "
                            f"label — please triage manually."
                        )
                        gh.post_comment(
                            repo,
                            pr_state.number,
                            f"**orcest** failed to merge this PR: {safe_err}\n\n{label_note}",
                            token,
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
                        _register_task(task.id, ct)
                    except Exception:
                        logger.warning(
                            "Failed to enqueue rebase for PR #%d",
                            other_pr.number,
                            exc_info=True,
                        )
        elif pr_state.action == PRAction.ENQUEUE_FIX:
            logger.info("PR #%d (%s): enqueueing fix task", pr_state.number, pr_state.title)
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
                    try:
                        clear_pending_task(project_redis, repo, "pr", pr_state.number)
                    except Exception as clear_err:
                        logger.error(
                            "Failed to clear pending task marker for PR #%d: %s",
                            pr_state.number,
                            clear_err,
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
                    _register_task(task.id, ct)
                    enqueued += 1
                except Exception as e:
                    logger.error(
                        "Failed to publish followup task for PR #%d: %s",
                        pr_state.number,
                        e,
                        exc_info=True,
                    )
                    try:
                        clear_pending_task(project_redis, repo, "pr", pr_state.number)
                    except Exception as clear_err:
                        logger.error(
                            "Failed to clear pending task marker for PR #%d: %s",
                            pr_state.number,
                            clear_err,
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
                    _register_task(task.id, ct)
                    enqueued += 1
                except Exception as e:
                    logger.error(
                        "Failed to publish rebase task for PR #%d: %s",
                        pr_state.number,
                        e,
                        exc_info=True,
                    )
                    try:
                        clear_pending_task(project_redis, repo, "pr", pr_state.number)
                    except Exception as clear_err:
                        logger.error(
                            "Failed to clear pending task marker for PR #%d: %s",
                            pr_state.number,
                            clear_err,
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
                        repo,
                        run_id,
                        token,
                    )
                    set_review_retrigger_sha(
                        project_redis, repo, pr_state.number, pr_state.head_sha
                    )
                except Exception as e:
                    logger.error(
                        "Failed to re-trigger review for PR #%d: %s",
                        pr_state.number,
                        e,
                        exc_info=True,
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
                # (e.g. StatusContext checks). Escalate to needs-human.
                logger.warning(
                    "PR #%d: stale pending checks with no re-triggerable run IDs; "
                    "adding needs-human label",
                    pr_state.number,
                )
                try:
                    gh.add_label(
                        repo,
                        pr_state.number,
                        labels.needs_human,
                        token,
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
                        repo,
                        pr_state.number,
                        f"**orcest** detected stale CI checks that have been pending for "
                        f"more than {config.stale_pending_timeout_seconds // 60}m but "
                        f"could not re-trigger them automatically. "
                        f"Please investigate the stuck checks manually.",
                        token,
                    )
                except Exception as e:
                    logger.error(
                        "Failed to comment on PR #%d about stale checks: %s",
                        pr_state.number,
                        e,
                        exc_info=True,
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
                for run_id in run_ids:
                    try:
                        gh.cancel_workflow(
                            repo,
                            run_id,
                            token,
                        )
                        any_cancel_succeeded = True
                        cancelled_count += 1
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
            logger.warning(
                "PR #%d: max attempts reached, adding needs-human label", pr_state.number
            )
            labeled = False
            try:
                gh.add_label(
                    repo,
                    pr_state.number,
                    labels.needs_human,
                    token,
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
                    f"Labeling as `{labels.needs_human}` for manual review."
                    if labeled
                    else f"Failed to add `{labels.needs_human}` label — please triage manually."
                )
                gh.post_comment(
                    repo,
                    pr_state.number,
                    f"**orcest** has exhausted its retry budget "
                    f"({config.max_attempts} attempts) for this PR. "
                    f"{label_note}\n\nPush a new commit to reset "
                    f"the counter and allow orcest to try again.",
                    token,
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

    # Discover issues needing implementation
    # Prioritize existing PRs over new issue work. PRs with terminal
    # labels (needs-human/blocked) are parked and don't block issue work.
    pr_work_pending = any(
        pr_state.action
        in (
            PRAction.ENQUEUE_FIX,
            PRAction.ENQUEUE_FOLLOWUP,
            PRAction.ENQUEUE_REBASE,
            PRAction.SKIP_LOCKED,
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
                    _register_task(task.id, ct)
                    enqueued += 1
                except Exception as e:
                    logger.error(
                        f"Failed to publish issue task for issue #{issue_state.number}: {e}",
                        exc_info=True,
                    )
                    try:
                        clear_pending_task(project_redis, repo, "issue", issue_state.number)
                    except Exception as clear_err:
                        logger.error(
                            f"Failed to clear pending task marker for issue #{issue_state.number}: "
                            f"{clear_err}",
                            exc_info=True,
                        )
        elif issue_state.action == IssueAction.SKIP_MAX_ATTEMPTS:
            logger.warning(
                f"Issue #{issue_state.number}: max attempts reached, adding needs-human label"
            )
            labeled = False
            try:
                gh.add_issue_label(
                    repo,
                    issue_state.number,
                    labels.needs_human,
                    token,
                )
                labeled = True
            except Exception as e:
                logger.error(
                    f"Failed to label issue #{issue_state.number} as needs-human: {e}",
                    exc_info=True,
                )
            try:
                label_note = (
                    f"Labeling as `{labels.needs_human}` for manual review."
                    if labeled
                    else f"Failed to add `{labels.needs_human}` label — please triage manually."
                )
                gh.post_issue_comment(
                    repo,
                    issue_state.number,
                    f"**orcest** has exhausted its retry budget "
                    f"({config.max_attempts} attempts) for this issue. "
                    f"{label_note}",
                    token,
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

    return enqueued, merged, len(pr_states), len(issue_states)


def _consume_results_for_project(
    project: ProjectConfig,
    redis: RedisClient,
    labels: LabelConfig,
    logger: logging.Logger,
    token_pool: TokenPool | None = None,
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
                _handle_result(project, labels, redis, result, logger, token_pool=token_pool)
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
                _handle_result(project, labels, redis, result, logger, token_pool=token_pool)
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
    token_pool: TokenPool | None = None,
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

    repo = project.repo
    token = project.token
    resource_id = result.resource_id
    is_issue = result.resource_type == "issue"
    resource_label = "issue" if is_issue else "PR"
    resource_type = result.resource_type or ("issue" if is_issue else "pr")

    # Guard against stale/duplicate task IDs.
    # When result publishing fails in a worker, the pending-task marker is cleared
    # so the orchestrator can re-enqueue. If the old task entry stays unACKed in the
    # Redis PEL and a drain later publishes a FAILED result for it, the orchestrator
    # may have already enqueued a newer task. In that case the pending-task marker
    # holds the *new* task's ID, and applying label/comment side-effects for the old
    # task would be incorrect. Skip processing entirely for stale results.
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
                clear_issue_attempts(redis, repo, resource_id)
            else:
                clear_attempts(redis, repo, resource_id)
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
    elif result.status == ResultStatus.USAGE_EXHAUSTED:
        # Mark the exhausted token in the pool so it's skipped in future rounds.
        # Query the usage endpoint for the precise reset time; fall back to 30 min.
        if token_pool is not None:
            exhausted_token = token_pool.get_task_token(result.task_id)
            cooldown_until = None
            if exhausted_token:
                try:
                    cooldown_until = get_token_reset_time(exhausted_token)
                except Exception as e:
                    logger.warning("Failed to query token reset time: %s", e)
            token_pool.mark_exhausted(result.task_id, cooldown_until=cooldown_until)
        # PR-specific cooldown: clear per-SHA attempts so PR can be re-enqueued
        # after the cooldown expires.  Issues don't have per-SHA counters.
        if not is_issue:
            cooldown_set = False
            try:
                set_usage_exhausted_cooldown(redis, repo, resource_id)
                cooldown_set = True
            except Exception as e:
                logger.error(
                    f"Failed to set usage-exhausted cooldown for PR #{resource_id}: {e}",
                    exc_info=True,
                )
            if cooldown_set:
                try:
                    clear_attempts(redis, repo, resource_id)
                except Exception as e:
                    logger.error(
                        f"Failed to clear per-SHA attempt counter for PR #{resource_id} "
                        f"after USAGE_EXHAUSTED: {e}",
                        exc_info=True,
                    )

    # Transient failures (clone timeout, worker restart) should be retried
    # automatically — don't label needs-human or burn attempt slots.
    is_transient = result.status == ResultStatus.FAILED and result.summary.startswith(
        TRANSIENT_SUMMARY_PREFIX
    )

    if is_transient:
        # Clear per-SHA attempts so the PR will be re-enqueued on the next
        # poll cycle. Total_attempts is left incremented as a circuit-breaker
        # against persistent infra failures (fibonacci backoff kicks in).
        try:
            if is_issue:
                clear_issue_attempts(redis, repo, resource_id)
            else:
                clear_attempts(redis, repo, resource_id)
        except Exception as e:
            logger.error(
                f"Failed to clear attempts for transient failure on "
                f"{resource_label} #{resource_id}: {e}",
                exc_info=True,
            )

    # Manage labels based on result status.
    # Only terminal statuses (FAILED, BLOCKED) add labels.
    # USAGE_EXHAUSTED adds no labels — the PR will resume via the cooldown mechanism.
    # Transient failures skip labeling — they will be retried automatically.
    labeled = False
    if result.status == ResultStatus.FAILED and not is_transient:
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

        if result.status == ResultStatus.FAILED and is_transient:
            # Transient failures are retried silently — no comment to avoid
            # accumulating noise if infrastructure is degraded across many attempts.
            if token_pool is not None:
                token_pool.task_completed(result.task_id)
            return
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
