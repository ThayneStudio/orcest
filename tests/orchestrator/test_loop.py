"""Tests for orcest.orchestrator.loop internals (_poll_cycle, _consume_results).

Uses mocker (pytest-mock) to patch high-level functions called by the loop,
and fake_redis_client for result stream operations.
"""

import logging

import pytest

from orcest.orchestrator.loop import (
    RESULTS_GROUP,
    RESULTS_STREAM,
    _consume_results,
    _poll_cycle,
)
from orcest.orchestrator.pr_ops import PRAction, PRState
from orcest.shared.models import ResultStatus, TaskResult


def _make_pr_state(
    number: int = 42,
    action: PRAction = PRAction.ENQUEUE_FIX,
    ci_failures: list[dict] | None = None,
) -> PRState:
    """Build a minimal PRState for loop tests."""
    if ci_failures is None:
        ci_failures = [{"name": "ruff", "conclusion": "failure"}]
    return PRState(
        number=number,
        title=f"PR #{number}",
        branch=f"fix/{number}",
        head_sha="abc123",
        action=action,
        ci_failures=ci_failures,
        review_threads=[],
        labels=[],
    )


def _make_task_result(
    status: ResultStatus = ResultStatus.COMPLETED,
    pr_number: int = 42,
    task_id: str = "task-abc-123",
    worker_id: str = "worker-1",
    branch: str = "fix/widget",
    summary: str = "Fixed the lint errors",
    duration: int = 120,
) -> TaskResult:
    """Build a TaskResult for result-handling tests."""
    return TaskResult(
        task_id=task_id,
        worker_id=worker_id,
        status=status,
        branch=branch,
        summary=summary,
        duration_seconds=duration,
        resource_type="pr",
        resource_id=pr_number,
    )


# ---------------------------------------------------------------------------
# _poll_cycle tests
# ---------------------------------------------------------------------------


def test_poll_cycle_enqueues_tasks(mocker, fake_redis_client, orchestrator_config, gh_mock):
    """_poll_cycle calls publish_fix_task for PRs with ENQUEUE_FIX action."""
    pr_state = _make_pr_state(number=10, action=PRAction.ENQUEUE_FIX)

    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        return_value=[pr_state],
    )
    mock_publish = mocker.patch(
        "orcest.orchestrator.loop.publish_fix_task",
    )
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    # _consume_results reads from Redis -- ensure consumer group exists
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    logger = logging.getLogger("test")
    _poll_cycle(orchestrator_config, fake_redis_client, logger)

    mock_publish.assert_called_once()
    assert mock_publish.call_args.kwargs["pr_state"] is pr_state


def test_poll_cycle_skips_non_actionable(mocker, fake_redis_client, orchestrator_config, gh_mock):
    """_poll_cycle does NOT call publish_fix_task for SKIP_GREEN PRs."""
    pr_state = _make_pr_state(number=20, action=PRAction.SKIP_GREEN, ci_failures=[])

    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        return_value=[pr_state],
    )
    mock_publish = mocker.patch(
        "orcest.orchestrator.loop.publish_fix_task",
    )
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    logger = logging.getLogger("test")
    _poll_cycle(orchestrator_config, fake_redis_client, logger)

    mock_publish.assert_not_called()


def test_poll_cycle_enqueues_followup(mocker, fake_redis_client, orchestrator_config, gh_mock):
    """_poll_cycle calls publish_followup_task for PRs with ENQUEUE_FOLLOWUP action."""
    pr_state = PRState(
        number=30,
        title="PR #30",
        branch="feat/30",
        head_sha="abc123",
        action=PRAction.ENQUEUE_FOLLOWUP,
        ci_failures=[],
        review_threads=[{"id": "t1", "path": "a.py", "line": 1, "comments": []}],
        labels=[],
    )

    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        return_value=[pr_state],
    )
    mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mock_followup = mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    logger = logging.getLogger("test")
    _poll_cycle(orchestrator_config, fake_redis_client, logger)

    mock_followup.assert_called_once()
    assert mock_followup.call_args.kwargs["pr_state"] is pr_state


def test_poll_cycle_merges_pr(mocker, fake_redis_client, orchestrator_config, gh_mock):
    """_poll_cycle calls gh.merge_pr for PRs with MERGE action."""
    pr_state = PRState(
        number=40,
        title="PR #40",
        branch="feat/40",
        head_sha="abc123",
        action=PRAction.MERGE,
        ci_failures=[],
        review_threads=[],
        labels=[],
    )

    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        return_value=[pr_state],
    )
    mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    logger = logging.getLogger("test")
    _poll_cycle(orchestrator_config, fake_redis_client, logger)

    gh_mock.merge_pr.assert_called_once_with(
        orchestrator_config.github.repo, 40, orchestrator_config.github.token,
    )
    # On successful merge, should post a confirmation comment
    gh_mock.post_comment.assert_called_once()
    comment_body = gh_mock.post_comment.call_args[0][2]
    assert "merged" in comment_body


def test_poll_cycle_merge_failure_labels_needs_human(
    mocker, fake_redis_client, orchestrator_config, gh_mock,
):
    """When merge fails, the PR is labeled needs-human and a comment is posted."""
    pr_state = PRState(
        number=41,
        title="PR #41",
        branch="feat/41",
        head_sha="abc123",
        action=PRAction.MERGE,
        ci_failures=[],
        review_threads=[],
        labels=[],
    )

    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        return_value=[pr_state],
    )
    mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    gh_mock.merge_pr.side_effect = RuntimeError("merge conflict")
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    logger = logging.getLogger("test")
    _poll_cycle(orchestrator_config, fake_redis_client, logger)

    # Should label needs-human after merge failure
    gh_mock.add_label.assert_called_once_with(
        orchestrator_config.github.repo, 41,
        orchestrator_config.labels.needs_human,
        orchestrator_config.github.token,
    )
    # Should post a comment about the failure
    gh_mock.post_comment.assert_called_once()
    comment_body = gh_mock.post_comment.call_args[0][2]
    assert "failed to merge" in comment_body
    assert "merge conflict" in comment_body


def test_poll_cycle_skip_max_attempts_labels_and_comments(
    mocker, fake_redis_client, orchestrator_config, gh_mock,
):
    """SKIP_MAX_ATTEMPTS adds needs-human label and posts an explanatory comment."""
    pr_state = _make_pr_state(number=50, action=PRAction.SKIP_MAX_ATTEMPTS)

    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        return_value=[pr_state],
    )
    mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    logger = logging.getLogger("test")
    _poll_cycle(orchestrator_config, fake_redis_client, logger)

    # Should add needs-human label
    gh_mock.add_label.assert_called_once_with(
        orchestrator_config.github.repo, 50,
        orchestrator_config.labels.needs_human,
        orchestrator_config.github.token,
    )
    # Should post a comment about exhausted retry budget
    gh_mock.post_comment.assert_called_once()
    comment_body = gh_mock.post_comment.call_args[0][2]
    assert "exhausted" in comment_body
    assert str(orchestrator_config.max_attempts) in comment_body


def test_poll_cycle_exception_handled(mocker, fake_redis_client, orchestrator_config, gh_mock):
    """When discover_actionable_prs raises, _poll_cycle propagates the exception.

    The crash protection lives in run_orchestrator's while-loop (try/except),
    so _poll_cycle itself should let the exception bubble up.
    """
    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        side_effect=RuntimeError("GitHub is down"),
    )
    mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    logger = logging.getLogger("test")

    with pytest.raises(RuntimeError, match="GitHub is down"):
        _poll_cycle(orchestrator_config, fake_redis_client, logger)


# ---------------------------------------------------------------------------
# _consume_results tests
# ---------------------------------------------------------------------------


def test_consume_results_completed(fake_redis_client, orchestrator_config, gh_mock):
    """A COMPLETED result removes queued and in-progress labels."""
    # Set up consumer group and add a result to the stream
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    result = _make_task_result(status=ResultStatus.COMPLETED, pr_number=42)
    fake_redis_client.xadd(RESULTS_STREAM, result.to_dict())

    logger = logging.getLogger("test")
    _consume_results(orchestrator_config, fake_redis_client, logger)

    labels = orchestrator_config.labels
    # Should post a comment about completion
    gh_mock.post_comment.assert_called_once()
    comment_body = gh_mock.post_comment.call_args[0][2]
    assert "completed" in comment_body

    # Should remove both queued and in-progress labels
    remove_calls = gh_mock.remove_label.call_args_list
    removed_labels = {call[0][2] for call in remove_calls}
    assert labels.queued in removed_labels
    assert labels.in_progress in removed_labels

    # Should NOT add needs-human label
    add_calls = gh_mock.add_label.call_args_list
    added_labels = {call[0][2] for call in add_calls}
    assert labels.needs_human not in added_labels


def test_consume_results_failed(fake_redis_client, orchestrator_config, gh_mock):
    """A FAILED result removes queued/in-progress and adds needs-human."""
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    result = _make_task_result(status=ResultStatus.FAILED, pr_number=55)
    fake_redis_client.xadd(RESULTS_STREAM, result.to_dict())

    logger = logging.getLogger("test")
    _consume_results(orchestrator_config, fake_redis_client, logger)

    labels = orchestrator_config.labels

    # Should post a comment mentioning failure
    gh_mock.post_comment.assert_called_once()
    comment_body = gh_mock.post_comment.call_args[0][2]
    assert "failed" in comment_body
    assert labels.needs_human in comment_body

    # Should remove queued and in-progress
    remove_calls = gh_mock.remove_label.call_args_list
    removed_labels = {call[0][2] for call in remove_calls}
    assert labels.queued in removed_labels
    assert labels.in_progress in removed_labels

    # Should add needs-human
    add_calls = gh_mock.add_label.call_args_list
    added_labels = {call[0][2] for call in add_calls}
    assert labels.needs_human in added_labels


def test_consume_results_usage_exhausted(fake_redis_client, orchestrator_config, gh_mock):
    """A USAGE_EXHAUSTED result removes queued and keeps in-progress."""
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    result = _make_task_result(
        status=ResultStatus.USAGE_EXHAUSTED, pr_number=60, branch="fix/widget",
    )
    fake_redis_client.xadd(RESULTS_STREAM, result.to_dict())

    logger = logging.getLogger("test")
    _consume_results(orchestrator_config, fake_redis_client, logger)

    labels = orchestrator_config.labels

    # Should post a comment mentioning paused
    gh_mock.post_comment.assert_called_once()
    comment_body = gh_mock.post_comment.call_args[0][2]
    assert "paused" in comment_body
    assert "fix/widget" in comment_body

    # Should remove queued label
    remove_calls = gh_mock.remove_label.call_args_list
    removed_labels = {call[0][2] for call in remove_calls}
    assert labels.queued in removed_labels
    # Should NOT remove in-progress
    assert labels.in_progress not in removed_labels

    # Should add in-progress label (swap queued -> in-progress)
    add_calls = gh_mock.add_label.call_args_list
    added_labels = {call[0][2] for call in add_calls}
    assert labels.in_progress in added_labels
    # Should NOT add needs-human
    assert labels.needs_human not in added_labels


def test_consume_results_usage_exhausted_no_branch(
    fake_redis_client, orchestrator_config, gh_mock,
):
    """A USAGE_EXHAUSTED result with no branch uses generic 'Work saved.' note."""
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    result = _make_task_result(
        status=ResultStatus.USAGE_EXHAUSTED, pr_number=61, branch="",
    )
    fake_redis_client.xadd(RESULTS_STREAM, result.to_dict())

    logger = logging.getLogger("test")
    _consume_results(orchestrator_config, fake_redis_client, logger)

    comment_body = gh_mock.post_comment.call_args[0][2]
    assert "Work saved." in comment_body
    # Should not reference a branch name
    assert "branch `" not in comment_body


def test_consume_results_malformed_entry_is_acked(fake_redis_client, orchestrator_config, gh_mock):
    """A malformed result entry is ACKed to prevent infinite reprocessing."""
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    # Add a malformed entry (missing required fields)
    fake_redis_client.xadd(RESULTS_STREAM, {"garbage": "data"})

    logger = logging.getLogger("test")
    # Should not raise -- error is logged and entry is ACKed
    _consume_results(orchestrator_config, fake_redis_client, logger)

    # No GitHub operations should have been attempted
    gh_mock.post_comment.assert_not_called()
    gh_mock.add_label.assert_not_called()
    gh_mock.remove_label.assert_not_called()

    # The entry should be ACKed (verify by reading again -- nothing pending)
    entries = fake_redis_client.xreadgroup(
        group=RESULTS_GROUP,
        consumer="orchestrator-main",
        stream=RESULTS_STREAM,
        count=10,
        block_ms=None,
    )
    assert entries == []


def test_consume_results_empty(fake_redis_client, orchestrator_config, gh_mock):
    """When no results are pending, _consume_results returns without error."""
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    logger = logging.getLogger("test")
    # Should not raise
    _consume_results(orchestrator_config, fake_redis_client, logger)

    gh_mock.post_comment.assert_not_called()
    gh_mock.add_label.assert_not_called()
    gh_mock.remove_label.assert_not_called()


def test_consume_results_xack_failure_continues(
    mocker, fake_redis_client, orchestrator_config, gh_mock,
):
    """When redis.xack raises, processing continues (entry was handled, just not acked)."""
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    result = _make_task_result(status=ResultStatus.COMPLETED, pr_number=70)
    fake_redis_client.xadd(RESULTS_STREAM, result.to_dict())

    # Make xack raise an exception
    mocker.patch.object(
        fake_redis_client, "xack", side_effect=RuntimeError("ACK failed"),
    )

    logger = logging.getLogger("test")
    # Should not raise -- xack failure is caught and logged
    _consume_results(orchestrator_config, fake_redis_client, logger)

    # The result was still processed: comment was posted
    gh_mock.post_comment.assert_called_once()
    comment_body = gh_mock.post_comment.call_args[0][2]
    assert "completed" in comment_body


def test_consume_results_blocked_status_posts_comment(
    fake_redis_client, orchestrator_config, gh_mock,
):
    """A result with BLOCKED status still posts a comment (falls to else branch)."""
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    result = _make_task_result(status=ResultStatus.BLOCKED, pr_number=71)
    fake_redis_client.xadd(RESULTS_STREAM, result.to_dict())

    logger = logging.getLogger("test")
    _consume_results(orchestrator_config, fake_redis_client, logger)

    # Should post a comment with the fallback format (includes summary/duration/worker)
    gh_mock.post_comment.assert_called_once()
    comment_body = gh_mock.post_comment.call_args[0][2]
    assert result.task_id in comment_body
    assert "blocked" in comment_body
    assert result.summary in comment_body
    assert result.worker_id in comment_body

    # BLOCKED falls into the else branch for label management:
    # removes queued and in-progress, adds blocked label, does NOT add needs-human
    remove_calls = gh_mock.remove_label.call_args_list
    removed_labels = {call[0][2] for call in remove_calls}
    assert orchestrator_config.labels.queued in removed_labels
    assert orchestrator_config.labels.in_progress in removed_labels

    add_calls = gh_mock.add_label.call_args_list
    added_labels = {call[0][2] for call in add_calls}
    assert orchestrator_config.labels.blocked in added_labels
    assert orchestrator_config.labels.needs_human not in added_labels


# ---------------------------------------------------------------------------
# Additional _poll_cycle tests
# ---------------------------------------------------------------------------


def test_poll_cycle_merge_comment_failure_logged(
    mocker, fake_redis_client, orchestrator_config, gh_mock,
):
    """Merge succeeds but post_comment raises -- merge still happened, comment just failed."""
    pr_state = PRState(
        number=80,
        title="PR #80",
        branch="feat/80",
        head_sha="abc123",
        action=PRAction.MERGE,
        ci_failures=[],
        review_threads=[],
        labels=[],
    )

    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        return_value=[pr_state],
    )
    mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    # merge succeeds, but post_comment fails
    gh_mock.post_comment.side_effect = RuntimeError("GitHub API down")

    logger = logging.getLogger("test")
    # Should not raise -- comment failure is caught and logged
    _poll_cycle(orchestrator_config, fake_redis_client, logger)

    # merge_pr was still called successfully
    gh_mock.merge_pr.assert_called_once_with(
        orchestrator_config.github.repo, 80, orchestrator_config.github.token,
    )
    # post_comment was attempted (and failed)
    gh_mock.post_comment.assert_called_once()


def test_poll_cycle_merge_fail_label_fail(
    mocker, fake_redis_client, orchestrator_config, gh_mock,
):
    """Merge fails AND add_label('orcest:needs-human') raises -- labeled=False in comment."""
    pr_state = PRState(
        number=81,
        title="PR #81",
        branch="feat/81",
        head_sha="abc123",
        action=PRAction.MERGE,
        ci_failures=[],
        review_threads=[],
        labels=[],
    )

    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        return_value=[pr_state],
    )
    mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    # merge fails, then add_label also fails
    gh_mock.merge_pr.side_effect = RuntimeError("merge conflict")
    gh_mock.add_label.side_effect = RuntimeError("label API down")

    logger = logging.getLogger("test")
    _poll_cycle(orchestrator_config, fake_redis_client, logger)

    # Comment should say "Failed to add" since labeling failed
    gh_mock.post_comment.assert_called_once()
    comment_body = gh_mock.post_comment.call_args[0][2]
    assert "Failed to add" in comment_body
    assert "please triage manually" in comment_body


def test_poll_cycle_enqueue_fix_publish_failure(
    mocker, fake_redis_client, orchestrator_config, gh_mock,
):
    """publish_fix_task raises -- exception is logged, loop continues (no crash)."""
    pr_state = _make_pr_state(number=82, action=PRAction.ENQUEUE_FIX)

    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        return_value=[pr_state],
    )
    mock_publish = mocker.patch(
        "orcest.orchestrator.loop.publish_fix_task",
        side_effect=RuntimeError("Redis down"),
    )
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    logger = logging.getLogger("test")
    # Should not raise -- publish failure is caught inside _poll_cycle
    _poll_cycle(orchestrator_config, fake_redis_client, logger)

    mock_publish.assert_called_once()


def test_poll_cycle_enqueue_followup_publish_failure(
    mocker, fake_redis_client, orchestrator_config, gh_mock,
):
    """publish_followup_task raises -- exception is logged, loop continues."""
    pr_state = PRState(
        number=83,
        title="PR #83",
        branch="feat/83",
        head_sha="abc123",
        action=PRAction.ENQUEUE_FOLLOWUP,
        ci_failures=[],
        review_threads=[{"id": "t1", "path": "a.py", "line": 1, "comments": []}],
        labels=[],
    )

    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        return_value=[pr_state],
    )
    mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mock_followup = mocker.patch(
        "orcest.orchestrator.loop.publish_followup_task",
        side_effect=RuntimeError("Redis down"),
    )
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    logger = logging.getLogger("test")
    # Should not raise -- publish failure is caught inside _poll_cycle
    _poll_cycle(orchestrator_config, fake_redis_client, logger)

    mock_followup.assert_called_once()


def test_poll_cycle_skip_max_attempts_label_failure(
    mocker, fake_redis_client, orchestrator_config, gh_mock,
):
    """add_label raises during SKIP_MAX_ATTEMPTS -- labeled=False variant of the comment."""
    pr_state = _make_pr_state(number=84, action=PRAction.SKIP_MAX_ATTEMPTS)

    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        return_value=[pr_state],
    )
    mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    gh_mock.add_label.side_effect = RuntimeError("label API down")

    logger = logging.getLogger("test")
    _poll_cycle(orchestrator_config, fake_redis_client, logger)

    # add_label was attempted
    gh_mock.add_label.assert_called_once()

    # Comment should have the "Failed to add" variant
    gh_mock.post_comment.assert_called_once()
    comment_body = gh_mock.post_comment.call_args[0][2]
    assert "Failed to add" in comment_body
    assert "please triage manually" in comment_body
    assert "exhausted" in comment_body


def test_poll_cycle_skip_draft_action(
    mocker, fake_redis_client, orchestrator_config, gh_mock,
):
    """Explicit test for SKIP_DRAFT action -- PR is not published/enqueued."""
    pr_state = _make_pr_state(number=85, action=PRAction.SKIP_DRAFT, ci_failures=[])

    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        return_value=[pr_state],
    )
    mock_publish = mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mock_followup = mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    logger = logging.getLogger("test")
    _poll_cycle(orchestrator_config, fake_redis_client, logger)

    # No task should be published and no merge attempted
    mock_publish.assert_not_called()
    mock_followup.assert_not_called()
    gh_mock.merge_pr.assert_not_called()
    gh_mock.add_label.assert_not_called()
    gh_mock.post_comment.assert_not_called()


def test_poll_cycle_skip_pending_action(
    mocker, fake_redis_client, orchestrator_config, gh_mock,
):
    """Explicit test for SKIP_PENDING action -- PR is not published/enqueued."""
    pr_state = _make_pr_state(number=86, action=PRAction.SKIP_PENDING, ci_failures=[])

    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        return_value=[pr_state],
    )
    mock_publish = mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mock_followup = mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    logger = logging.getLogger("test")
    _poll_cycle(orchestrator_config, fake_redis_client, logger)

    # No task should be published and no merge attempted
    mock_publish.assert_not_called()
    mock_followup.assert_not_called()
    gh_mock.merge_pr.assert_not_called()
    gh_mock.add_label.assert_not_called()
    gh_mock.post_comment.assert_not_called()


# ---------------------------------------------------------------------------
# Additional _handle_result tests (via _consume_results)
# ---------------------------------------------------------------------------


def test_handle_result_failed_label_failure(
    fake_redis_client, orchestrator_config, gh_mock,
):
    """FAILED result where add_label raises -- comment says 'Failed to add'."""
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    result = _make_task_result(status=ResultStatus.FAILED, pr_number=90)
    fake_redis_client.xadd(RESULTS_STREAM, result.to_dict())

    gh_mock.add_label.side_effect = RuntimeError("label API down")

    logger = logging.getLogger("test")
    _consume_results(orchestrator_config, fake_redis_client, logger)

    # Should post a comment with the "Failed to add ... label" variant
    gh_mock.post_comment.assert_called_once()
    comment_body = gh_mock.post_comment.call_args[0][2]
    assert "failed" in comment_body
    assert "Failed to add" in comment_body
    assert "please triage manually" in comment_body


def test_handle_result_post_comment_failure(
    mocker, fake_redis_client, orchestrator_config, gh_mock,
):
    """When post_comment raises in _handle_result, it should be logged (no crash)."""
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    result = _make_task_result(status=ResultStatus.COMPLETED, pr_number=91)
    fake_redis_client.xadd(RESULTS_STREAM, result.to_dict())

    gh_mock.post_comment.side_effect = RuntimeError("GitHub API down")

    logger = logging.getLogger("test")
    # Should not raise -- post_comment failure is caught and logged
    _consume_results(orchestrator_config, fake_redis_client, logger)

    # post_comment was attempted
    gh_mock.post_comment.assert_called_once()

    # Label operations still proceeded (remove queued + in-progress)
    remove_calls = gh_mock.remove_label.call_args_list
    removed_labels = {call[0][2] for call in remove_calls}
    assert orchestrator_config.labels.queued in removed_labels
    assert orchestrator_config.labels.in_progress in removed_labels
