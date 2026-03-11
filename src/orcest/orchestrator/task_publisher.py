"""Task creation and enqueueing for the orchestrator.

Renders prompts from PR context (diffs, CI failures, review threads) and
publishes tasks to the Redis stream. Also handles GitHub visibility by
posting comments when tasks are queued.
"""

import logging
import re

from orcest.orchestrator import gh
from orcest.orchestrator.ci_triage import CIFailureType, classify_ci_failure
from orcest.orchestrator.issue_ops import IssueState
from orcest.orchestrator.issue_ops import increment_attempts as increment_issue_attempts
from orcest.orchestrator.pr_ops import (
    PRState,
    increment_attempts,
    increment_total_attempts,
    increment_transient_attempts,
)
from orcest.shared.coordination import set_pending_task
from orcest.shared.models import Task, TaskType
from orcest.shared.redis_client import RedisClient

_RUN_ID_RE = re.compile(r"https://github\.com/[^/]+/[^/]+/actions/runs/(\d+)")

# Per-check log excerpt limit (errors are at the end of logs)
_PER_CHECK_LOG_LIMIT = 20000
# Total log budget across all checks in a single prompt
_TOTAL_LOG_BUDGET = 50000

# Tasks stream cap: 10 000 entries.
#
# SEMANTIC NOTE: tasks:* streams are work queues consumed via XREADGROUP,
# unlike output/log streams which are append-only observation channels.
# This distinction matters for trimming:
#
#   - Output/log streams: trimming old entries is safe because consumers only
#     need recent data for display; losing old log lines is acceptable.
#   - Task streams: trimming *undelivered* entries causes silent task loss —
#     the entry is removed from Redis before any worker reads it, so the work
#     is silently dropped.
#
# With approximate MAXLEN, Redis may trim the oldest entries as soon as the
# stream exceeds ~maxlen entries.  If the backlog of undelivered tasks grows
# beyond that threshold (i.e. workers are seriously behind), the oldest tasks
# can be lost before a worker ever reads them.
#
# SAFETY NET: the orchestrator's GitHub re-poll loop is the mitigation.
# On the next poll cycle the orchestrator will re-discover actionable
# PRs/issues and re-enqueue any tasks that were trimmed, so work is not
# permanently lost — only delayed by one poll interval.
# CAVEAT: each trimmed (and therefore un-executed) task still consumes an
# attempt counter slot.  If trimming happens on every cycle until
# max_attempts is exhausted, the item will be permanently silenced.
# At 10 000 MAXLEN this is extraordinarily unlikely, but not impossible.
#
# MAXLEN RATIONALE: 10 000 was chosen to make silent loss practically
# impossible under realistic conditions.  At typical throughput the queue
# depth stays well below 100.  A backlog of 10 000 undelivered tasks would
# require workers to be catastrophically behind; at that point the system is
# already degraded for other reasons.  10 000 entries add only ~5-10 MB of
# Redis memory (stream entries are compact), which is negligible.
_TASKS_STREAM_MAXLEN = 10_000

# Regex matching lines that signal the start of an important log section
# (stack traces, test failures, assertion errors, etc.)
_LOG_ERROR_RE = re.compile(
    r"(Traceback \(most recent call last\)"
    r"|^ERROR[:\s]"
    r"|^FAILED[:\s]"
    r"|^FAIL[:\s]"
    r"|AssertionError"
    r"|Tests run:.*Failures:"
    r"|\bFAILURES\b)",
    re.MULTILINE,
)


def _extract_relevant_log_sections(log_text: str, max_len: int) -> str:
    """Extract the most relevant sections from a CI log.

    If an important error/failure marker appears *before* the tail window,
    up to half the budget is reserved for context around that early error so
    the cause (not just the symptom) is visible.  Otherwise falls back to a
    plain tail excerpt, which works well for most CI logs where errors appear
    at the end.
    """
    if len(log_text) <= max_len:
        return log_text

    tail_start = len(log_text) - max_len
    first_error = _LOG_ERROR_RE.search(log_text)

    if first_error and first_error.start() < tail_start:
        # Important section exists before the tail — capture context around it
        ctx_start = max(0, first_error.start() - 200)
        ctx_end = min(len(log_text), first_error.start() + max_len // 4)
        early_section = log_text[ctx_start:ctx_end]

        separator = "\n... (middle of log omitted) ...\n"
        early_budget = min(len(early_section), max_len // 2)
        tail_budget = max(0, max_len - early_budget - len(separator))
        return (
            early_section[:early_budget] + separator + log_text[-tail_budget:]
            if tail_budget > 0
            else early_section[:early_budget]
        )

    # Default: return the tail (errors are usually at the end of CI logs)
    return log_text[-max_len:]


# Max number of transient-CI re-triggers before falling back to a Claude fix task.
# Transient retries do not consume the main per-SHA attempt budget.
_MAX_TRANSIENT_RETRIES = 3


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
    default_runner: str,
    logger: logging.Logger | None = None,
) -> None:
    """Publish a task to Redis and update GitHub visibility.

    Shared by publish_fix_task and publish_followup_task to avoid
    duplicating the increment -> publish -> comment -> log sequence.

    The attempt counter is incremented before publishing to Redis so that a
    crash between the two operations still counts the attempt, preventing
    unbounded retries.  If commenting fails afterwards, the error is logged
    but the task is not lost.
    """
    task_type = task.type
    _log = logger or logging.getLogger(__name__)

    # Claim the pending-task slot atomically (SET NX EX). If another task
    # is already pending for this PR, skip publish to avoid duplicates.
    if not set_pending_task(redis, task.repo, "pr", pr_state.number, task.id):
        _log.info(f"Pending task already exists for PR #{pr_state.number}, skipping publish")
        return

    # Increment attempt count BEFORE publishing to Redis to eliminate the
    # check-then-act race: if the orchestrator crashes between xadd and
    # increment, the attempt would never be counted, allowing unbounded
    # retries.  An increment without a subsequent xadd is safe -- the
    # max-attempts guard in discover_actionable_prs prevents runaway loops.
    try:
        increment_attempts(redis, pr_state.number, pr_state.head_sha)
        increment_total_attempts(redis, pr_state.number)
    except Exception:
        _log.error(
            f"Failed to increment attempt counter for PR #{pr_state.number} "
            f"before publishing task {task.id} to Redis; skipping publish to "
            f"avoid an un-counted attempt — will retry next poll cycle",
            exc_info=True,
        )
        return

    # Publish to backend-specific stream
    tasks_stream = f"tasks:{default_runner}"
    redis.xadd_capped(tasks_stream, task.to_dict(), maxlen=_TASKS_STREAM_MAXLEN)

    _log.info(f"Published {task_type.value} task {task.id} for PR #{pr_state.number}")


def publish_fix_task(
    pr_state: PRState,
    repo: str,
    token: str,
    redis: RedisClient,
    default_runner: str,
    logger: logging.Logger | None = None,
) -> Task | None:
    """Create and publish a fix task for a PR.

    Steps:
    1. Gather context (diff, CI logs, review threads)
    2. Classify CI failures
    3. Render prompt
    4. Publish to Redis stream
    5. Post comment on PR

    Returns:
        The published Task, or None if all CI failures were classified as
        transient and CI was re-triggered directly (no Claude task enqueued,
        main attempt budget not consumed).
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

    # If every CI failure is transient, re-trigger the runs directly instead of
    # asking Claude to "fix" something that isn't a code problem.  A separate
    # transient counter (per SHA) tracks how many times we've done this so we
    # don't spin forever: after _MAX_TRANSIENT_RETRIES we fall back to the
    # normal fix-task path and let Claude investigate.
    _log = logger or logging.getLogger(__name__)
    if failure_summaries and all(
        s["classification"] == CIFailureType.TRANSIENT.value for s in failure_summaries
    ):
        transient_count = increment_transient_attempts(redis, pr_state.number, pr_state.head_sha)
        if transient_count <= _MAX_TRANSIENT_RETRIES:
            _log.info(
                "PR #%d: all CI failures are transient (retry %d/%d), re-triggering CI",
                pr_state.number,
                transient_count,
                _MAX_TRANSIENT_RETRIES,
            )
            retriggered: set[int] = set()
            for check in pr_state.ci_failures:
                run_id = _extract_run_id(check.get("detailsUrl", ""))
                if run_id is not None and run_id not in retriggered:
                    try:
                        gh.rerun_workflow(repo, run_id, token, failed_only=True)
                        retriggered.add(run_id)
                    except Exception:
                        _log.warning(
                            "PR #%d: failed to re-trigger run %d",
                            pr_state.number,
                            run_id,
                            exc_info=True,
                        )
            if not retriggered:
                _log.warning(
                    "PR #%d: transient path triggered but no runs were re-triggered (retry %d/%d); "
                    "falling back to fix task",
                    pr_state.number,
                    transient_count,
                    _MAX_TRANSIENT_RETRIES,
                )
                # Fall through to enqueue a Claude fix task since we couldn't retrigger anything.
            else:
                # No Claude task needed — return without consuming the main attempt budget.
                return None
        else:
            _log.warning(
                "PR #%d: transient retry budget exhausted (%d/%d), falling back to fix task",
                pr_state.number,
                transient_count,
                _MAX_TRANSIENT_RETRIES,
            )
            # Fall through to enqueue a Claude fix task below.

    # Use review threads from pr_state (populated by discover_actionable_prs for
    # CHANGES_REQUESTED). For CI failures, fetch inline review comments from the
    # REST API since discover_actionable_prs does not populate review_threads there.
    if pr_state.ci_failures:
        try:
            raw_inline = gh.get_pr_review_comments(repo, pr_state.number, token)
            review_threads = _group_inline_comments(raw_inline)
        except Exception as exc:
            _log.warning(
                "Failed to fetch inline review comments for PR #%s; proceeding without them: %s",
                pr_state.number,
                exc,
            )
            review_threads = []
    else:
        review_threads = pr_state.review_threads

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
        base_branch=pr_state.base_branch,
    )

    _publish_and_notify(
        task=task,
        pr_state=pr_state,
        repo=repo,
        token=token,
        redis=redis,
        default_runner=default_runner,
        logger=logger,
    )

    return task


def publish_followup_task(
    pr_state: PRState,
    repo: str,
    token: str,
    redis: RedisClient,
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
    3. Post comment on PR
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
        base_branch=pr_state.base_branch,
    )

    _publish_and_notify(
        task=task,
        pr_state=pr_state,
        repo=repo,
        token=token,
        redis=redis,
        default_runner=default_runner,
        logger=logger,
    )

    return task


def publish_rebase_task(
    pr_state: PRState,
    repo: str,
    token: str,
    redis: RedisClient,
    default_runner: str,
    merge_error: str = "",
    logger: logging.Logger | None = None,
) -> Task:
    """Create and publish a rebase task for a PR with merge conflicts.

    Enqueued when ``gh pr merge`` fails due to merge conflicts. The worker
    will rebase the branch onto the base branch and resolve any conflicts,
    then push. The orchestrator will attempt to merge again on the next cycle.
    """
    prompt = _render_rebase_prompt(
        pr_number=pr_state.number,
        pr_title=pr_state.title,
        branch=pr_state.branch,
        repo=repo,
        merge_error=merge_error,
        base_branch=pr_state.base_branch,
    )

    task = Task.create(
        task_type=TaskType.REBASE_PR,
        repo=repo,
        token=token,
        resource_type="pr",
        resource_id=pr_state.number,
        prompt=prompt,
        branch=pr_state.branch,
        base_branch=pr_state.base_branch,
    )

    _publish_and_notify(
        task=task,
        pr_state=pr_state,
        repo=repo,
        token=token,
        redis=redis,
        default_runner=default_runner,
        logger=logger,
    )

    return task


def publish_issue_task(
    issue_state: IssueState,
    repo: str,
    token: str,
    redis: RedisClient,
    default_runner: str,
    logger: logging.Logger | None = None,
) -> Task:
    """Create and publish an implementation task for a GitHub issue.

    Steps:
    1. Render prompt from issue title and body
    2. Publish to Redis stream
    3. Post comment on issue
    """
    prompt = _render_issue_prompt(
        issue_number=issue_state.number,
        issue_title=issue_state.title,
        issue_body=issue_state.body,
        repo=repo,
    )

    task = Task.create(
        task_type=TaskType.IMPLEMENT_ISSUE,
        repo=repo,
        token=token,
        resource_type="issue",
        resource_id=issue_state.number,
        prompt=prompt,
        branch=None,
    )

    _publish_issue_and_notify(
        task=task,
        issue_state=issue_state,
        repo=repo,
        token=token,
        redis=redis,
        default_runner=default_runner,
        logger=logger,
    )

    return task


def _publish_issue_and_notify(
    task: Task,
    issue_state: IssueState,
    repo: str,
    token: str,
    redis: RedisClient,
    default_runner: str,
    logger: logging.Logger | None = None,
) -> None:
    """Publish a task to Redis and update GitHub visibility on the issue."""
    task_type = task.type
    _log = logger or logging.getLogger(__name__)

    # Claim the pending-task slot atomically (SET NX EX).
    if not set_pending_task(redis, task.repo, "issue", issue_state.number, task.id):
        _log.info(f"Pending task already exists for issue #{issue_state.number}, skipping publish")
        return

    # Increment attempt count BEFORE publishing to Redis (same rationale as
    # _publish_and_notify: prevents unbounded retries on orchestrator crash).
    try:
        increment_issue_attempts(redis, issue_state.number)
    except Exception:
        _log.error(
            f"Failed to increment attempt counter for issue #{issue_state.number} "
            f"before publishing task {task.id} to Redis; skipping publish to "
            f"avoid an un-counted attempt — will retry next poll cycle",
            exc_info=True,
        )
        return

    # Publish to issue-specific stream (lower priority than PR tasks)
    tasks_stream = f"tasks:issue:{default_runner}"
    redis.xadd_capped(tasks_stream, task.to_dict(), maxlen=_TASKS_STREAM_MAXLEN)

    _log.info(f"Published {task_type.value} task {task.id} for issue #{issue_state.number}")


def _slugify(text: str, max_len: int = 40) -> str:
    """Convert text to a branch-name-safe slug."""
    slug = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")
    return slug[:max_len].rstrip("-")


def _group_inline_comments(comments: list[dict]) -> list[dict]:
    """Group flat inline review comments by (path, line) into thread-like dicts.

    Input dicts have keys: path, line, author, body (as returned by
    gh.get_pr_review_comments). Output dicts match the format expected by
    _render_review_threads: {path, line, comments: [{author, body}]}.
    """
    groups: dict[tuple, list[dict]] = {}
    for c in comments:
        key = (c.get("path", ""), c.get("line"))
        if key not in groups:
            groups[key] = []
        groups[key].append({"author": c.get("author", ""), "body": c.get("body", "")})
    return [
        {"path": path, "line": line, "comments": thread_comments}
        for (path, line), thread_comments in groups.items()
    ]


def _render_issue_prompt(
    issue_number: int,
    issue_title: str,
    issue_body: str,
    repo: str,
) -> str:
    """Render the prompt for implementing a GitHub issue."""
    branch_name = f"issue-{issue_number}-{_slugify(issue_title)}"

    sections: list[str] = [
        f"# Implement Issue #{issue_number}: {issue_title}",
        "",
        "You are on the default branch.",
        "",
        "## Issue Description",
        "",
        issue_body or "(No description provided.)",
        "",
        "## Instructions",
        "",
        "1. Read the issue description carefully.",
        f"2. Create a new branch: `git checkout -b {branch_name}`",
        "3. Read the repo's CLAUDE.md (if it exists) for project conventions.",
        "4. Implement the requested changes.",
        "5. Run the project's linter and tests to verify your changes.",
        "6. Commit your changes with a descriptive message referencing the issue.",
        f"7. Push the branch: `git push -u origin {branch_name}`",
        f'8. Open a PR: `gh pr create --repo {repo} --title "{issue_title}" '
        f'--body "Closes #{issue_number}" --head {branch_name}`',
        "",
        "Important:",
        "- Make minimal, focused changes.",
        "- Do NOT close the issue directly -- the PR will close it on merge.",
        "- Do NOT call `gh pr review` -- you are not authorized to change review status.",
    ]

    return "\n".join(sections)


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
            "2. For each thread, determine if it is actionable (requests a change "
            "or raises a genuine concern) or non-actionable (purely positive "
            "feedback, acknowledgment, or praise).",
            "3. For non-actionable threads, resolve them directly without creating issues.",
            f"4. For actionable threads, create GitHub issues for follow-up work "
            f'(`gh issue create --repo {repo} --title "..." --body "..."`) '
            f"-- group related items into single issues where appropriate.",
            "5. Reply to each actionable thread with a comment linking to the created issue, "
            "then resolve the thread.",
            "6. Do NOT make code changes -- this is triage only.",
            "7. Do NOT call `gh pr review --approve` or `--request-changes` "
            "-- you are not authorized to change review status.",
        ]
    )

    return "\n".join(sections)


def _render_rebase_prompt(
    pr_number: int,
    pr_title: str,
    branch: str,
    repo: str,
    merge_error: str = "",
    base_branch: str = "main",
) -> str:
    """Render a prompt for rebasing a PR branch to resolve merge conflicts."""
    sections: list[str] = [
        f"# Rebase PR #{pr_number}: {pr_title}",
        "",
        f"You are on branch `{branch}`.",
        "This PR has merge conflicts that prevent it from being merged.",
        "",
    ]

    if merge_error:
        sections.append("## Merge Error")
        sections.append("")
        sections.append(f"```\n{merge_error[:500]}\n```")
        sections.append("")

    sections.extend(
        [
            "## Instructions",
            "",
            f"1. Fetch the latest base branch (`{base_branch}`):",
            "   ```",
            f"   git fetch origin {base_branch}",
            "   ```",
            f"2. Rebase your branch onto `{base_branch}`:",
            "   ```",
            f"   git rebase origin/{base_branch}",
            "   ```",
            "3. If there are merge conflicts:",
            "   - Read the conflicting files to understand both sides",
            "   - Resolve each conflict by keeping the intent of both changes",
            "   - Stage resolved files with `git add`",
            "   - Continue the rebase with `git rebase --continue`",
            "4. After the rebase is complete, force-push to update the PR:",
            "   ```",
            "   git push --force-with-lease",
            "   ```",
            "5. Verify the branch is clean and the rebase succeeded.",
            "",
            "Do NOT create new commits \u2014 only rebase existing ones.",
            "Do NOT squash commits during the rebase.",
            "Push to the existing branch. Do not create new PRs.",
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
    ~20,000 characters (prioritising error sections and tail) and total log
    content capped at ~50,000 characters.
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
                    excerpt = _extract_relevant_log_sections(log_text, max_len)
                    if len(log_text) > max_len:
                        excerpt = (
                            f"... (truncated, showing {len(excerpt)} of {len(log_text)} chars)\n"
                            + excerpt
                        )
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

    if review_threads:
        sections.append("## Review Feedback")
        sections.append("")
        sections.extend(_render_review_threads(review_threads))
        sections.append("Address the review feedback above. For each thread:")
        sections.append("- If it requests a code change, make the fix and resolve the thread.")
        sections.append(
            "- If it is purely positive feedback or has no actionable request, "
            "resolve the thread without making changes."
        )
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
            f"Run `git diff HEAD` in the workspace to view the complete diff.*"
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
