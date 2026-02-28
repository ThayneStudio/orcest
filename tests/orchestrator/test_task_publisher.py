"""Tests for orcest.orchestrator.task_publisher.publish_fix_task().

Verifies task creation, Redis stream publishing, GitHub label/comment
side effects, prompt diff truncation, and CI log fetching/rendering.
"""

from orcest.orchestrator.gh import GhCliError
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


# --- CI log fetching tests ---


def _make_ci_failures_with_urls(
    run_ids: list[int | None],
    names: list[str] | None = None,
) -> list[dict]:
    """Build a ci_failures list with GitHub Actions detailsUrl values.

    Pass None as a run_id to simulate a third-party CI URL.
    """
    failures = []
    for i, run_id in enumerate(run_ids):
        name = (names[i] if names else None) or f"check-{i}"
        if run_id is not None:
            url = (
                f"https://github.com/org/repo/actions/runs/{run_id}"
                f"/job/{9000 + i}"
            )
        else:
            url = "https://circleci.com/gh/org/repo/12345"
        failures.append({
            "name": name,
            "conclusion": "FAILURE",
            "detailsUrl": url,
        })
    return failures


def test_publish_fetches_ci_logs_from_details_url(
    gh_mock, fake_redis_client, label_config,
):
    """When a check has a detailsUrl with a run_id, logs are fetched and
    included in the prompt."""
    _setup_gh_defaults(gh_mock)
    sample_logs = "Step 4/5\nERROR: pytest FAILED test_foo.py::test_bar"
    gh_mock.get_failed_run_logs.return_value = sample_logs

    ci_failures = _make_ci_failures_with_urls(
        run_ids=[77001], names=["tests"]
    )
    pr_state = _make_pr_state(number=50, ci_failures=ci_failures)

    task = publish_fix_task(
        pr_state=pr_state,
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
        default_runner="claude",
    )

    # Verify get_failed_run_logs was called with the correct run ID
    gh_mock.get_failed_run_logs.assert_called_once_with(
        "test-org/test-repo", 77001, "fake-token"
    )
    # Log content should appear in the prompt
    assert "pytest FAILED test_foo.py::test_bar" in task.prompt
    # Check that the log section header is present
    assert "Log output for tests" in task.prompt


def test_publish_deduplicates_run_log_fetches(
    gh_mock, fake_redis_client, label_config,
):
    """Two checks sharing the same run_id only trigger one log fetch."""
    _setup_gh_defaults(gh_mock)
    gh_mock.get_failed_run_logs.return_value = "some log output"

    # Two checks, same run_id
    ci_failures = _make_ci_failures_with_urls(
        run_ids=[55555, 55555], names=["lint", "typecheck"]
    )
    pr_state = _make_pr_state(number=51, ci_failures=ci_failures)

    publish_fix_task(
        pr_state=pr_state,
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
        default_runner="claude",
    )

    # Should only fetch once despite two checks
    gh_mock.get_failed_run_logs.assert_called_once_with(
        "test-org/test-repo", 55555, "fake-token"
    )


def test_publish_graceful_on_log_fetch_failure(
    gh_mock, fake_redis_client, label_config,
):
    """If log fetching raises an exception, task creation still succeeds."""
    _setup_gh_defaults(gh_mock)
    gh_mock.get_failed_run_logs.side_effect = GhCliError(
        "gh command failed (exit 1): not found", stderr="not found"
    )

    ci_failures = _make_ci_failures_with_urls(
        run_ids=[99999], names=["tests"]
    )
    pr_state = _make_pr_state(number=52, ci_failures=ci_failures)

    task = publish_fix_task(
        pr_state=pr_state,
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
        default_runner="claude",
    )

    # Task should still be created despite log fetch failure
    assert isinstance(task, Task)
    assert task.resource_id == 52
    # The check name should still appear in the prompt
    assert "tests" in task.prompt


def test_publish_handles_non_github_actions_url(
    gh_mock, fake_redis_client, label_config,
):
    """Third-party CI URLs (no run_id) don't trigger log fetching."""
    _setup_gh_defaults(gh_mock)
    gh_mock.get_failed_run_logs.return_value = ""

    # run_id=None produces a non-GitHub-Actions URL
    ci_failures = _make_ci_failures_with_urls(
        run_ids=[None], names=["circleci/build"]
    )
    pr_state = _make_pr_state(number=53, ci_failures=ci_failures)

    task = publish_fix_task(
        pr_state=pr_state,
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
        default_runner="claude",
    )

    # get_failed_run_logs should NOT be called for non-GitHub-Actions URLs
    gh_mock.get_failed_run_logs.assert_not_called()
    # Task should still be created
    assert isinstance(task, Task)
    assert "circleci/build" in task.prompt


def test_prompt_truncates_long_ci_logs(
    gh_mock, fake_redis_client, label_config,
):
    """CI logs longer than 5000 chars are truncated to the last 5000."""
    _setup_gh_defaults(gh_mock)
    # Create a log that's well over the per-check limit
    long_log = "x" * 3000 + "REAL_ERROR_AT_END" + "y" * 3000
    gh_mock.get_failed_run_logs.return_value = long_log

    ci_failures = _make_ci_failures_with_urls(
        run_ids=[88888], names=["tests"]
    )
    pr_state = _make_pr_state(number=54, ci_failures=ci_failures)

    task = publish_fix_task(
        pr_state=pr_state,
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
        default_runner="claude",
    )

    # The full log (6017 chars) should NOT appear verbatim
    assert long_log not in task.prompt
    # The end of the log (where errors are) should be present
    assert "REAL_ERROR_AT_END" in task.prompt
    assert "y" * 3000 in task.prompt
    # Truncation indicator should appear
    assert "truncated" in task.prompt
