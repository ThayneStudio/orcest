"""Task creation and enqueueing for the orchestrator.

Renders prompts from PR context (diffs, CI failures, review threads) and
publishes tasks to the Redis stream. Also handles GitHub visibility by
adding labels and posting comments when tasks are queued.
"""

import logging
import re

from orcest.orchestrator import gh
from orcest.orchestrator.ci_triage import CIFailureType, classify_ci_failure
from orcest.orchestrator.pr_ops import PRState, increment_attempts
from orcest.shared.config import LabelConfig
from orcest.shared.models import Task, TaskType
from orcest.shared.redis_client import RedisClient

_RUN_ID_RE = re.compile(r"https://github\.com/[^/]+/[^/]+/actions/runs/(\d+)")

# Per-check log excerpt limit (errors are at the end of logs)
_PER_CHECK_LOG_LIMIT = 5000
# Total log budget across all checks in a single prompt
_TOTAL_LOG_BUDGET = 15000


def _extract_run_id(details_url: str) -> int | None:
    """Extract the workflow run ID from a GitHub Actions detailsUrl.

    Expected format:
        https://github.com/{owner}/{repo}/actions/runs/{run_id}/job/{job_id}

    Returns None if the URL doesn't match (e.g. third-party CI).
    """
    m = _RUN_ID_RE.search(details_url)
    return int(m.group(1)) if m else None


def _publish_and_notify(
    task: Task,
    pr_state: PRState,
    repo: str,
    token: str,
    redis: RedisClient,
    label_config: LabelConfig,
    default_runner: str,
    logger: logging.Logger | None = None,
) -> None:
    """Publish a task to Redis and update GitHub visibility.

    Shared by publish_fix_task and publish_followup_task to avoid
    duplicating the publish -> label -> comment -> log sequence.

    The task is published to Redis first. If labeling or commenting
    fails afterwards, the error is logged but the task is not lost.
    """
    task_type = task.type

    # Publish to backend-specific stream
    tasks_stream = f"tasks:{default_runner}"
    redis.xadd(tasks_stream, task.to_dict())

    # Track attempt count for re-enqueue throttling.
    # Wrapped in try/except because the task is already in Redis -- a
    # failed increment is harmless (next cycle may re-enqueue, but the
    # max-attempts check in discover_actionable_prs prevents runaway loops).
    _log = logger or logging.getLogger(__name__)
    try:
        increment_attempts(redis, pr_state.number, pr_state.head_sha)
    except Exception:
        _log.error(
            f"Failed to increment attempt counter for PR #{pr_state.number} "
            f"(task {task.id} already published to Redis)",
            exc_info=True,
        )

    # Update GitHub visibility -- wrapped in try/except because the task
    # is already published to Redis. If labeling fails, the next poll cycle
    # won't see SKIP_LABELED, but that is safer than losing the task.
    _label_ok = True
    _comment_ok = True
    try:
        gh.add_label(repo, pr_state.number, label_config.queued, token)
    except Exception:
        _label_ok = False
        _log.error(
            f"Failed to add queued label to PR #{pr_state.number} "
            f"(task {task.id} already published to Redis)",
            exc_info=True,
        )
    try:
        gh.post_comment(
            repo,
            pr_state.number,
            f"**orcest** queued task `{task.id}` ({task_type.value}) for this PR.",
            token,
        )
    except Exception:
        _comment_ok = False
        _log.error(
            f"Failed to post comment on PR #{pr_state.number} "
            f"(task {task.id} already published to Redis)",
            exc_info=True,
        )

    if _label_ok and _comment_ok:
        _log.info(f"Published {task_type.value} task {task.id} for PR #{pr_state.number}")
    else:
        _log.warning(
            f"Published {task_type.value} task {task.id} "
            f"for PR #{pr_state.number} (GitHub visibility partially failed: "
            f"label={'ok' if _label_ok else 'FAILED'}, "
            f"comment={'ok' if _comment_ok else 'FAILED'})"
        )


def publish_fix_task(
    pr_state: PRState,
    repo: str,
    token: str,
    redis: RedisClient,
    label_config: LabelConfig,
    default_runner: str,
    logger: logging.Logger | None = None,
) -> Task:
    """Create and publish a fix task for a PR.

    Steps:
    1. Gather context (diff, CI logs, review threads)
    2. Classify CI failures
    3. Render prompt
    4. Publish to Redis stream
    5. Add label and post comment on PR
    """
    # Gather context
    diff = gh.get_pr_diff(repo, pr_state.number, token)

    # Fetch CI logs and classify failures
    failure_summaries: list[dict] = []
    ci_logs: dict[str, str] = {}  # check_name -> log_text
    run_logs_cache: dict[int, str] = {}  # run_id -> log_text (dedup)
    task_type = TaskType.FIX_PR

    for check in pr_state.ci_failures:
        details_url = check.get("detailsUrl", "")
        check_name = check.get("name", "unknown")
        log_text = ""

        # Try to fetch logs from GitHub Actions run
        run_id = _extract_run_id(details_url)
        if run_id is not None:
            if run_id not in run_logs_cache:
                try:
                    run_logs_cache[run_id] = gh.get_failed_run_logs(repo, run_id, token)
                except Exception:
                    # Never let log fetching break task creation
                    run_logs_cache[run_id] = ""
            log_text = run_logs_cache[run_id]

        ci_logs[check_name] = log_text

        classification = classify_ci_failure(check_name, log_text)
        failure_summaries.append(
            {
                "name": check_name,
                "classification": classification.value,
                "details_url": details_url,
            }
        )

        if classification == CIFailureType.CODE:
            task_type = TaskType.FIX_CI

    # Use review threads from pr_state (already populated by discover_actionable_prs)
    review_threads = pr_state.review_threads if not pr_state.ci_failures else []

    # Render prompt
    prompt = _render_fix_prompt(
        pr_number=pr_state.number,
        pr_title=pr_state.title,
        branch=pr_state.branch,
        diff=diff,
        ci_failures=failure_summaries,
        review_threads=review_threads,
        ci_logs=ci_logs,
    )

    # Create task
    task = Task.create(
        task_type=task_type,
        repo=repo,
        token=token,
        resource_type="pr",
        resource_id=pr_state.number,
        prompt=prompt,
        branch=pr_state.branch,
    )

    _publish_and_notify(
        task=task,
        pr_state=pr_state,
        repo=repo,
        token=token,
        redis=redis,
        label_config=label_config,
        default_runner=default_runner,
        logger=logger,
    )

    return task


def publish_followup_task(
    pr_state: PRState,
    repo: str,
    token: str,
    redis: RedisClient,
    label_config: LabelConfig,
    default_runner: str,
    logger: logging.Logger | None = None,
) -> Task:
    """Create and publish a triage-followups task for a PR.

    This is used when a PR is approved and CI is green, but there are
    unresolved review threads that need to be triaged into GitHub issues
    before merging.

    Steps:
    1. Render followup prompt from review threads
    2. Publish to Redis stream
    3. Add label and post comment on PR
    """
    task_type = TaskType.TRIAGE_FOLLOWUPS

    if not pr_state.review_threads:
        raise ValueError(
            f"publish_followup_task called for PR #{pr_state.number} "
            f"but pr_state.review_threads is empty -- nothing to triage"
        )

    # Render prompt
    prompt = _render_followup_prompt(
        pr_number=pr_state.number,
        pr_title=pr_state.title,
        branch=pr_state.branch,
        review_threads=pr_state.review_threads,
        repo=repo,
    )

    # Create task
    task = Task.create(
        task_type=task_type,
        repo=repo,
        token=token,
        resource_type="pr",
        resource_id=pr_state.number,
        prompt=prompt,
        branch=pr_state.branch,
    )

    _publish_and_notify(
        task=task,
        pr_state=pr_state,
        repo=repo,
        token=token,
        redis=redis,
        label_config=label_config,
        default_runner=default_runner,
        logger=logger,
    )

    return task


def _render_review_threads(threads: list[dict]) -> list[str]:
    """Render review thread sections for inclusion in prompts.

    Returns a list of formatted lines describing each thread's
    file path, line number, and reviewer comments.
    """
    lines: list[str] = []
    for thread in threads:
        path = thread.get("path") or "unknown"
        line_no = thread.get("line")
        lines.append(f"### `{path}` line {line_no if line_no is not None else '?'}")
        for comment in thread.get("comments") or []:
            author = comment.get("author") or "unknown"
            body = comment.get("body") or ""
            lines.append(f"**{author}**: {body}")
        lines.append("")
    return lines


def _render_followup_prompt(
    pr_number: int,
    pr_title: str,
    branch: str,
    review_threads: list[dict],
    repo: str,
) -> str:
    """Render a prompt for triaging unresolved review threads into issues.

    This prompt instructs the worker to create GitHub issues for follow-up
    work identified in review threads, without making code changes.
    """
    sections: list[str] = [
        f"# Triage follow-up items from PR #{pr_number}: {pr_title}",
        "",
        f"You are on branch `{branch}`.",
        "",
        "This PR is approved and CI is green. However, the reviewer left "
        "comments that need to be triaged into GitHub issues before merging.",
        "",
    ]

    # review_threads is guaranteed non-empty by publish_followup_task
    sections.append("## Unresolved Review Threads")
    sections.append("")
    sections.extend(_render_review_threads(review_threads))

    sections.extend(
        [
            "## Instructions",
            "",
            "1. Read each unresolved review thread above.",
            f"2. Create GitHub issues for follow-up work "
            f'(`gh issue create --repo {repo} --title "..." --body "..."`) '
            f"-- group related items into single issues where appropriate.",
            "3. Reply to each thread with a comment linking to the created issue.",
            "4. Resolve each thread after creating the issue.",
            "5. Do NOT make code changes -- this is triage only.",
            "6. Do NOT call `gh pr review --approve` or `--request-changes` "
            "-- you are not authorized to change review status.",
        ]
    )

    return "\n".join(sections)


def _render_fix_prompt(
    pr_number: int,
    pr_title: str,
    branch: str,
    diff: str,
    ci_failures: list[dict],
    review_threads: list[dict],
    ci_logs: dict[str, str] | None = None,
) -> str:
    """Render the prompt that Claude will receive.

    Uses simple string formatting (no Jinja2 dependency).
    Diff is truncated to 10,000 characters to keep prompt size manageable.
    CI log excerpts are included when available, with each log capped at
    the last ~5000 characters (errors are at the end) and total log
    content capped at ~15000 characters.
    """
    sections: list[str] = [
        f"# Fix PR #{pr_number}: {pr_title}",
        "",
        f"You are on branch `{branch}`.",
        "Your task is to fix the issues described below, commit your "
        "changes, and push to this branch.",
        "",
    ]

    if ci_failures:
        sections.append("## CI Failures")
        sections.append("")
        log_budget = _TOTAL_LOG_BUDGET
        for failure in ci_failures:
            sections.append(f"- **{failure['name']}** ({failure['classification']})")
            if failure.get("details_url"):
                sections.append(f"  Details: {failure['details_url']}")
            # Include log excerpt if available
            if ci_logs:
                log_text = ci_logs.get(failure["name"], "")
                if log_text and log_budget > 0:
                    max_len = min(_PER_CHECK_LOG_LIMIT, log_budget)
                    excerpt = log_text[-max_len:]
                    if len(log_text) > max_len:
                        excerpt = f"... (truncated, showing last {max_len} chars)\n" + excerpt
                    sections.append("")
                    sections.append(f"  **Log output for {failure['name']}:**")
                    sections.append("")
                    sections.append("  ```")
                    sections.append(excerpt)
                    sections.append("  ```")
                    log_budget -= min(len(log_text), max_len)
        sections.append("")
        sections.append(
            "Fix the CI failures listed above. Read the error "
            "messages carefully and make targeted fixes."
        )
        sections.append("")

    ci_is_green = not ci_failures

    if review_threads:
        sections.append("## Review Feedback")
        sections.append("")
        sections.extend(_render_review_threads(review_threads))
        sections.append("Address all review feedback above.")
        if ci_is_green:
            sections.append("After fixing each item, resolve the corresponding review thread.")
        sections.append("")

    truncated = len(diff) > 10000
    sections.extend(
        [
            "## Current Diff (against base branch)",
            "",
            "```diff",
            diff[:10000],
            "```",
        ]
    )
    if truncated:
        sections.append(
            f"*Note: diff truncated from {len(diff)} to 10,000 characters. "
            f"Review the full files in the repository for complete context.*"
        )
    sections.extend(
        [
            "",
            "## Instructions",
            "",
            "1. Read the CI failure details and/or review feedback carefully.",
            "2. Make the minimal changes needed to fix the issues.",
            "3. Run the project's linter/tests to verify your fix.",
            "4. Commit your changes with a descriptive message.",
            "5. Push to the branch.",
            "6. Do NOT call `gh pr review --approve` or `--request-changes` "
            "-- you are not authorized to change review status.",
            "",
            "Push to the existing branch. Do not create new PRs.",
        ]
    )

    return "\n".join(sections)
