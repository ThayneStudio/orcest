"""Tests for orcest.orchestrator.task_publisher.publish_fix_task().

Verifies task creation, Redis stream publishing, GitHub label/comment
side effects, and prompt diff truncation.
"""

from orcest.orchestrator.pr_ops import PRAction, PRState
from orcest.orchestrator.task_publisher import publish_fix_task
from orcest.shared.models import Task, TaskType


def _make_pr_state(
    number: int = 42,
    title: str = "Fix the widget",
    branch: str = "fix/widget",
    ci_failures: list[dict] | None = None,
) -> PRState:
    """Build a PRState suitable for publish_fix_task."""
    return PRState(
        number=number,
        title=title,
        branch=branch,
        head_sha="abc123",
        action=PRAction.ENQUEUE_FIX,
        ci_failures=ci_failures or [],
        review_comments=[],
        labels=[],
    )


def _setup_gh_defaults(gh_mock):
    """Set sensible default return values for gh mock functions."""
    gh_mock.get_pr_diff.return_value = "diff --git a/foo.py b/foo.py\n+pass"
    gh_mock.get_review_comments.return_value = []
    gh_mock.add_label.return_value = None
    gh_mock.post_comment.return_value = None


def test_publish_creates_task(gh_mock, fake_redis_client, label_config):
    """publish_fix_task returns a Task with the correct type, repo, and resource_id."""
    _setup_gh_defaults(gh_mock)
    pr_state = _make_pr_state(number=42)

    task = publish_fix_task(
        pr_state=pr_state,
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
        default_runner="claude",
    )

    assert isinstance(task, Task)
    assert task.repo == "test-org/test-repo"
    assert task.resource_id == 42
    assert task.resource_type == "pr"
    assert task.branch == "fix/widget"
    # No CI failures -> FIX_PR (review-driven path)
    assert task.type == TaskType.FIX_PR


def test_publish_adds_to_stream(gh_mock, fake_redis_client, label_config):
    """After publishing, the Redis 'tasks' stream contains an entry."""
    _setup_gh_defaults(gh_mock)
    pr_state = _make_pr_state(number=7)

    task = publish_fix_task(
        pr_state=pr_state,
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
        default_runner="claude",
    )

    # Read all entries from the tasks stream
    default_runner = "claude"
    entries = fake_redis_client.client.xrange(f"tasks:{default_runner}")
    assert len(entries) >= 1

    # The last entry should match our task
    _entry_id, fields = entries[-1]
    assert fields["id"] == task.id
    assert fields["repo"] == "test-org/test-repo"
    assert fields["resource_id"] == "7"


def test_publish_adds_label(gh_mock, fake_redis_client, label_config):
    """publish_fix_task calls gh.add_label with the 'queued' label."""
    _setup_gh_defaults(gh_mock)
    pr_state = _make_pr_state(number=15)

    publish_fix_task(
        pr_state=pr_state,
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
        default_runner="claude",
    )

    gh_mock.add_label.assert_called_once_with(
        "test-org/test-repo",
        15,
        label_config.queued,
        "fake-token",
    )


def test_publish_posts_comment(gh_mock, fake_redis_client, label_config):
    """publish_fix_task calls gh.post_comment on the PR."""
    _setup_gh_defaults(gh_mock)
    pr_state = _make_pr_state(number=20)

    task = publish_fix_task(
        pr_state=pr_state,
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
        default_runner="claude",
    )

    gh_mock.post_comment.assert_called_once()
    call_args = gh_mock.post_comment.call_args
    assert call_args[0][0] == "test-org/test-repo"
    assert call_args[0][1] == 20
    # Comment body should mention the task ID
    assert task.id in call_args[0][2]


def test_prompt_truncates_long_diff(gh_mock, fake_redis_client, label_config):
    """A diff longer than 10,000 characters is truncated in the prompt."""
    long_diff = "x" * 15000
    gh_mock.get_pr_diff.return_value = long_diff
    gh_mock.get_review_comments.return_value = []
    gh_mock.add_label.return_value = None
    gh_mock.post_comment.return_value = None

    pr_state = _make_pr_state(number=33)

    task = publish_fix_task(
        pr_state=pr_state,
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
        default_runner="claude",
    )

    # The full 15,000-char diff should NOT appear in the prompt
    assert long_diff not in task.prompt
    # But the first 10,000 chars should be present
    assert "x" * 10000 in task.prompt
    # And the truncation notice should appear
    assert "truncated from 15000 to 10,000" in task.prompt
