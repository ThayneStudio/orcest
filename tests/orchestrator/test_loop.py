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
) -> PRState:
    """Build a minimal PRState for loop tests."""
    return PRState(
        number=number,
        title=f"PR #{number}",
        branch=f"fix/{number}",
        head_sha="abc123",
        action=action,
        ci_failures=[{"name": "ruff", "conclusion": "failure"}],
        review_comments=[],
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
    # _consume_results reads from Redis -- ensure consumer group exists
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    logger = logging.getLogger("test")
    _poll_cycle(orchestrator_config, fake_redis_client, logger)

    mock_publish.assert_called_once()
    assert mock_publish.call_args.kwargs["pr_state"] is pr_state


def test_poll_cycle_skips_non_actionable(mocker, fake_redis_client, orchestrator_config, gh_mock):
    """_poll_cycle does NOT call publish_fix_task for SKIP_GREEN PRs."""
    pr_state = _make_pr_state(number=20, action=PRAction.SKIP_GREEN)

    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        return_value=[pr_state],
    )
    mock_publish = mocker.patch(
        "orcest.orchestrator.loop.publish_fix_task",
    )
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    logger = logging.getLogger("test")
    _poll_cycle(orchestrator_config, fake_redis_client, logger)

    mock_publish.assert_not_called()


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


def test_consume_results_empty(fake_redis_client, orchestrator_config, gh_mock):
    """When no results are pending, _consume_results returns without error."""
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    logger = logging.getLogger("test")
    # Should not raise
    _consume_results(orchestrator_config, fake_redis_client, logger)

    gh_mock.post_comment.assert_not_called()
    gh_mock.add_label.assert_not_called()
    gh_mock.remove_label.assert_not_called()
