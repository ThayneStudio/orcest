"""Task creation and enqueueing for the orchestrator.

Renders prompts from PR context (diffs, CI failures, review comments) and
publishes tasks to the Redis stream. Also handles GitHub visibility by
adding labels and posting comments when tasks are queued.
"""

import logging

from orcest.orchestrator import gh
from orcest.orchestrator.ci_triage import CIFailureType, classify_ci_failure
from orcest.orchestrator.pr_ops import PRState, increment_attempts
from orcest.shared.config import LabelConfig
from orcest.shared.models import Task, TaskType
from orcest.shared.redis_client import RedisClient


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
    1. Gather context (diff, CI logs, review comments)
    2. Classify CI failures
    3. Render prompt
    4. Publish to Redis stream
    5. Add label and post comment on PR
    """
    # Gather context
    diff = gh.get_pr_diff(repo, pr_state.number, token)

    # Classify CI failures
    failure_summaries: list[dict] = []
    task_type = TaskType.FIX_PR

    for check in pr_state.ci_failures:
        classification = classify_ci_failure(
            check.get("name", ""),
            "",  # logs -- Phase 1 may not fetch full logs
        )
        failure_summaries.append({
            "name": check.get("name", "unknown"),
            "classification": classification.value,
            "details_url": check.get("detailsUrl", ""),
        })

        if classification == CIFailureType.CODE:
            task_type = TaskType.FIX_CI

    # Get review comments if review-driven
    review_summary = ""
    if not pr_state.ci_failures:
        reviews = gh.get_review_comments(repo, pr_state.number, token)
        review_summary = _format_reviews(reviews)

    # Render prompt
    prompt = _render_fix_prompt(
        pr_number=pr_state.number,
        pr_title=pr_state.title,
        branch=pr_state.branch,
        diff=diff,
        ci_failures=failure_summaries,
        review_summary=review_summary,
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

    # Publish to backend-specific stream
    tasks_stream = f"tasks:{default_runner}"
    redis.xadd(tasks_stream, task.to_dict())

    # Track attempt count for re-enqueue throttling
    attempt_num = increment_attempts(redis, pr_state.number, pr_state.head_sha)

    # Update GitHub visibility
    gh.add_label(repo, pr_state.number, label_config.queued, token)
    gh.post_comment(
        repo, pr_state.number,
        f"**orcest** queued task `{task.id}` ({task_type.value}) "
        f"for this PR.",
        token,
    )

    if logger:
        logger.info(
            f"Published {task_type.value} task {task.id} "
            f"for PR #{pr_state.number}"
        )

    return task


def _render_fix_prompt(
    pr_number: int,
    pr_title: str,
    branch: str,
    diff: str,
    ci_failures: list[dict],
    review_summary: str,
) -> str:
    """Render the prompt that Claude will receive.

    Uses simple string formatting (no Jinja2 dependency).
    Diff is truncated to 10,000 characters to keep prompt size manageable.
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
        for f in ci_failures:
            sections.append(
                f"- **{f['name']}** ({f['classification']})"
            )
            if f.get("details_url"):
                sections.append(f"  Details: {f['details_url']}")
        sections.append("")
        sections.append(
            "Fix the CI failures listed above. Read the error "
            "messages carefully and make targeted fixes."
        )
        sections.append("")

    if review_summary:
        sections.append("## Review Feedback")
        sections.append("")
        sections.append(review_summary)
        sections.append("")
        sections.append("Address all review feedback above.")
        sections.append("")

    truncated = len(diff) > 10000
    sections.extend([
        "## Current Diff (against base branch)",
        "",
        "```diff",
        diff[:10000],
        "```",
    ])
    if truncated:
        sections.append(
            f"*Note: diff truncated from {len(diff)} to 10,000 characters. "
            f"Review the full files in the repository for complete context.*"
        )
    sections.extend([
        "",
        "## Instructions",
        "",
        "1. Read the CI failure details and/or review feedback carefully.",
        "2. Make the minimal changes needed to fix the issues.",
        "3. Run the project's linter/tests to verify your fix.",
        "4. Commit your changes with a descriptive message.",
        "5. Push to the branch.",
        "",
        "Do NOT create new PRs. Push to the existing branch.",
    ])

    return "\n".join(sections)


def _format_reviews(reviews: list[dict]) -> str:
    """Format review comments into a readable summary.

    Only includes CHANGES_REQUESTED reviews with non-empty bodies.
    """
    if not reviews:
        return ""

    lines: list[str] = []
    for review in reviews:
        state = review.get("state", "")
        body = review.get("body", "").strip()
        author = review.get("user", {}).get("login", "unknown")

        if state == "CHANGES_REQUESTED" and body:
            lines.append(f"**{author}** requested changes:")
            lines.append(body)
            lines.append("")

    return "\n".join(lines)
