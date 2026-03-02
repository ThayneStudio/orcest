"""Tests for orcest.orchestrator.task_publisher.

Verifies task creation, Redis stream publishing, GitHub label/comment
side effects, prompt diff truncation, CI log fetching/rendering, and
review thread prompt rendering for both fix and followup tasks.
"""

import pytest

from orcest.orchestrator.gh import GhCliError
from orcest.orchestrator.pr_ops import PRAction, PRState
from orcest.orchestrator.task_publisher import publish_fix_task, publish_followup_task
from orcest.shared.models import Task, TaskType


def _make_pr_state(
    number: int = 42,
    title: str = "Fix the widget",
    branch: str = "fix/widget",
    ci_failures: list[dict] | None = None,
    review_threads: list[dict] | None = None,
) -> PRState:
    """Build a PRState suitable for publish_fix_task."""
    return PRState(
        number=number,
        title=title,
        branch=branch,
        head_sha="abc123",
        action=PRAction.ENQUEUE_FIX,
        ci_failures=ci_failures or [],
        review_threads=review_threads or [],
        labels=[],
    )


def _setup_gh_defaults(gh_mock):
    """Set sensible default return values for gh mock functions."""
    gh_mock.get_pr_diff.return_value = "diff --git a/foo.py b/foo.py\n+pass"
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
            url = f"https://github.com/org/repo/actions/runs/{run_id}/job/{9000 + i}"
        else:
            url = "https://circleci.com/gh/org/repo/12345"
        failures.append(
            {
                "name": name,
                "conclusion": "FAILURE",
                "detailsUrl": url,
            }
        )
    return failures


def test_publish_fetches_ci_logs_from_details_url(
    gh_mock,
    fake_redis_client,
    label_config,
):
    """When a check has a detailsUrl with a run_id, logs are fetched and
    included in the prompt."""
    _setup_gh_defaults(gh_mock)
    sample_logs = "Step 4/5\nERROR: pytest FAILED test_foo.py::test_bar"
    gh_mock.get_failed_run_logs.return_value = sample_logs

    ci_failures = _make_ci_failures_with_urls(run_ids=[77001], names=["tests"])
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
    gh_mock.get_failed_run_logs.assert_called_once_with("test-org/test-repo", 77001, "fake-token")
    # Log content should appear in the prompt
    assert "pytest FAILED test_foo.py::test_bar" in task.prompt
    # Check that the log section header is present
    assert "Log output for tests" in task.prompt


def test_publish_deduplicates_run_log_fetches(
    gh_mock,
    fake_redis_client,
    label_config,
):
    """Two checks sharing the same run_id only trigger one log fetch."""
    _setup_gh_defaults(gh_mock)
    gh_mock.get_failed_run_logs.return_value = "some log output"

    # Two checks, same run_id
    ci_failures = _make_ci_failures_with_urls(run_ids=[55555, 55555], names=["lint", "typecheck"])
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
    gh_mock.get_failed_run_logs.assert_called_once_with("test-org/test-repo", 55555, "fake-token")


def test_publish_graceful_on_log_fetch_failure(
    gh_mock,
    fake_redis_client,
    label_config,
):
    """If log fetching raises an exception, task creation still succeeds."""
    _setup_gh_defaults(gh_mock)
    gh_mock.get_failed_run_logs.side_effect = GhCliError(
        "gh command failed (exit 1): not found", stderr="not found"
    )

    ci_failures = _make_ci_failures_with_urls(run_ids=[99999], names=["tests"])
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
    gh_mock,
    fake_redis_client,
    label_config,
):
    """Third-party CI URLs (no run_id) don't trigger log fetching."""
    _setup_gh_defaults(gh_mock)
    gh_mock.get_failed_run_logs.return_value = ""

    # run_id=None produces a non-GitHub-Actions URL
    ci_failures = _make_ci_failures_with_urls(run_ids=[None], names=["circleci/build"])
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
    gh_mock,
    fake_redis_client,
    label_config,
):
    """CI logs longer than 5000 chars are truncated to the last 5000."""
    _setup_gh_defaults(gh_mock)
    # Create a log that's well over the per-check limit
    long_log = "x" * 3000 + "REAL_ERROR_AT_END" + "y" * 3000
    gh_mock.get_failed_run_logs.return_value = long_log

    ci_failures = _make_ci_failures_with_urls(run_ids=[88888], names=["tests"])
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


# --- Review thread prompt tests ---


def _make_sample_threads() -> list[dict]:
    """Build sample review thread data for prompt tests."""
    return [
        {
            "id": "PRRT_abc",
            "path": "src/handler.py",
            "line": 55,
            "comments": [
                {"author": "alice", "body": "This function is too long, split it up."},
                {"author": "bob", "body": "Agreed, especially the parsing logic."},
            ],
        },
        {
            "id": "PRRT_def",
            "path": "tests/test_handler.py",
            "line": 12,
            "comments": [
                {"author": "alice", "body": "Missing edge case test for empty input."},
            ],
        },
    ]


def test_publish_review_fix_includes_thread_details(
    gh_mock,
    fake_redis_client,
    label_config,
):
    """Review fix prompt includes file path, line number, author, and body
    from each review thread."""
    _setup_gh_defaults(gh_mock)
    threads = _make_sample_threads()
    pr_state = _make_pr_state(
        number=200,
        ci_failures=[],
        review_threads=threads,
    )

    task = publish_fix_task(
        pr_state=pr_state,
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
        default_runner="claude",
    )

    # Check thread details appear in the prompt
    assert "src/handler.py" in task.prompt
    assert "55" in task.prompt
    assert "alice" in task.prompt
    assert "This function is too long, split it up." in task.prompt
    assert "bob" in task.prompt
    assert "Agreed, especially the parsing logic." in task.prompt
    assert "tests/test_handler.py" in task.prompt
    assert "12" in task.prompt
    assert "Missing edge case test for empty input." in task.prompt


def test_publish_review_fix_resolve_instructions(
    gh_mock,
    fake_redis_client,
    label_config,
):
    """Review fix prompt includes thread resolution instruction and
    prohibits calling gh pr review."""
    _setup_gh_defaults(gh_mock)
    threads = _make_sample_threads()
    pr_state = _make_pr_state(
        number=201,
        ci_failures=[],
        review_threads=threads,
    )

    task = publish_fix_task(
        pr_state=pr_state,
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
        default_runner="claude",
    )

    assert "resolve" in task.prompt.lower()
    assert "Do NOT call `gh pr review" in task.prompt


def test_publish_followup_triage_prompt(
    gh_mock,
    fake_redis_client,
    label_config,
):
    """Followup triage prompt contains triage instructions, gh issue create,
    prohibition on code changes, and thread details."""
    _setup_gh_defaults(gh_mock)
    threads = _make_sample_threads()
    pr_state = PRState(
        number=202,
        title="Add feature Y",
        branch="feat/y",
        head_sha="def456",
        action=PRAction.ENQUEUE_FOLLOWUP,
        ci_failures=[],
        review_threads=threads,
        labels=[],
    )

    task = publish_followup_task(
        pr_state=pr_state,
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
        default_runner="claude",
    )

    assert isinstance(task, Task)
    assert task.type == TaskType.TRIAGE_FOLLOWUPS
    # Prompt should mention triage
    assert "Triage" in task.prompt or "triage" in task.prompt
    # Prompt should include gh issue create instruction
    assert "gh issue create" in task.prompt
    # Prompt should prohibit code changes
    assert "Do NOT make code changes" in task.prompt
    # Prompt should include thread details
    assert "src/handler.py" in task.prompt
    assert "55" in task.prompt
    assert "alice" in task.prompt
    assert "This function is too long, split it up." in task.prompt
    assert "tests/test_handler.py" in task.prompt


def test_publish_ci_fix_no_thread_details(
    gh_mock,
    fake_redis_client,
    label_config,
):
    """CI fix prompt does NOT contain review thread sections."""
    _setup_gh_defaults(gh_mock)
    gh_mock.get_failed_run_logs.return_value = ""
    ci_failures = [
        {
            "name": "pytest",
            "conclusion": "FAILURE",
            "detailsUrl": "https://circleci.com/gh/org/repo/999",
        },
    ]
    pr_state = _make_pr_state(number=203, ci_failures=ci_failures)

    task = publish_fix_task(
        pr_state=pr_state,
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
        default_runner="claude",
    )

    # Should not contain review thread sections
    assert "Review Feedback" not in task.prompt
    assert "review thread" not in task.prompt.lower()
    # But should contain CI failure info
    assert "pytest" in task.prompt


def test_ci_failures_suppress_review_threads_in_prompt(
    gh_mock,
    fake_redis_client,
    label_config,
):
    """When both CI failures and review threads are present, the prompt
    should contain CI failure details but NOT review thread sections.
    CI failures take priority; review threads are omitted to keep the
    worker focused on fixing CI."""
    _setup_gh_defaults(gh_mock)
    gh_mock.get_failed_run_logs.return_value = ""
    ci_failures = [
        {
            "name": "pytest",
            "conclusion": "FAILURE",
            "detailsUrl": "https://circleci.com/gh/org/repo/999",
        },
    ]
    threads = _make_sample_threads()
    pr_state = _make_pr_state(
        number=204,
        ci_failures=ci_failures,
        review_threads=threads,
    )

    task = publish_fix_task(
        pr_state=pr_state,
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
        default_runner="claude",
    )

    # CI failure info should be present
    assert "pytest" in task.prompt
    assert "CI Failures" in task.prompt
    # Review thread sections should NOT be present
    assert "Review Feedback" not in task.prompt
    assert "src/handler.py" not in task.prompt
    assert "This function is too long" not in task.prompt


# --- Resilience: GitHub label/comment failure after Redis publish ---


def test_publish_fix_task_survives_label_failure(
    gh_mock,
    fake_redis_client,
    label_config,
):
    """publish_fix_task returns a task even when add_label raises, because
    the task is already in Redis and must not be lost."""
    _setup_gh_defaults(gh_mock)
    gh_mock.add_label.side_effect = RuntimeError("GitHub 500")
    pr_state = _make_pr_state(number=300)

    task = publish_fix_task(
        pr_state=pr_state,
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
        default_runner="claude",
    )

    assert isinstance(task, Task)
    assert task.resource_id == 300
    # Task should be in Redis despite label failure
    entries = fake_redis_client.client.xrange("tasks:claude")
    assert any(f["id"] == task.id for _, f in entries)


def test_publish_fix_task_survives_comment_failure(
    gh_mock,
    fake_redis_client,
    label_config,
):
    """publish_fix_task returns a task even when post_comment raises."""
    _setup_gh_defaults(gh_mock)
    gh_mock.post_comment.side_effect = RuntimeError("GitHub 500")
    pr_state = _make_pr_state(number=301)

    task = publish_fix_task(
        pr_state=pr_state,
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
        default_runner="claude",
    )

    assert isinstance(task, Task)
    assert task.resource_id == 301
    # Label should still have been added (it's called before comment)
    gh_mock.add_label.assert_called_once()


def test_publish_followup_task_survives_label_failure(
    gh_mock,
    fake_redis_client,
    label_config,
):
    """publish_followup_task returns a task even when add_label raises."""
    _setup_gh_defaults(gh_mock)
    gh_mock.add_label.side_effect = RuntimeError("GitHub 500")
    pr_state = PRState(
        number=302,
        title="Followup test",
        branch="feat/test",
        head_sha="abc123",
        action=PRAction.ENQUEUE_FOLLOWUP,
        ci_failures=[],
        review_threads=_make_sample_threads(),
        labels=[],
    )

    task = publish_followup_task(
        pr_state=pr_state,
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
        default_runner="claude",
    )

    assert isinstance(task, Task)
    assert task.type == TaskType.TRIAGE_FOLLOWUPS


def test_publish_followup_task_survives_comment_failure(
    gh_mock,
    fake_redis_client,
    label_config,
):
    """publish_followup_task returns a task even when post_comment raises."""
    _setup_gh_defaults(gh_mock)
    gh_mock.post_comment.side_effect = RuntimeError("GitHub 500")
    pr_state = PRState(
        number=303,
        title="Followup test",
        branch="feat/test",
        head_sha="abc123",
        action=PRAction.ENQUEUE_FOLLOWUP,
        ci_failures=[],
        review_threads=_make_sample_threads(),
        labels=[],
    )

    task = publish_followup_task(
        pr_state=pr_state,
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
        default_runner="claude",
    )

    assert isinstance(task, Task)
    assert task.type == TaskType.TRIAGE_FOLLOWUPS
    # Label should still have been added (it's called before comment)
    gh_mock.add_label.assert_called_once()


# --- _render_review_threads edge cases ---


def test_review_fix_prompt_handles_missing_thread_fields(
    gh_mock,
    fake_redis_client,
    label_config,
):
    """Review threads with None line numbers and missing fields render safely
    when published through publish_fix_task."""
    _setup_gh_defaults(gh_mock)
    threads = [
        {
            "id": "PRRT_edge",
            "path": None,  # missing path
            "line": None,  # missing line number
            "comments": [
                {"author": None, "body": "Some feedback"},
                {"body": "No author key at all"},  # missing author key
            ],
        },
    ]
    pr_state = _make_pr_state(
        number=400,
        ci_failures=[],
        review_threads=threads,
    )

    task = publish_fix_task(
        pr_state=pr_state,
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
        default_runner="claude",
    )

    # Should render without crashing; unknown/? should appear for missing values
    assert "unknown" in task.prompt  # path fallback and/or author fallback
    assert "?" in task.prompt  # line number fallback
    assert "Some feedback" in task.prompt


def test_publish_followup_task_rejects_empty_threads(
    gh_mock,
    fake_redis_client,
    label_config,
):
    """publish_followup_task raises ValueError when review_threads is empty,
    because there is nothing to triage."""
    _setup_gh_defaults(gh_mock)
    pr_state = PRState(
        number=500,
        title="Empty followup",
        branch="feat/empty",
        head_sha="abc123",
        action=PRAction.ENQUEUE_FOLLOWUP,
        ci_failures=[],
        review_threads=[],
        labels=[],
    )

    with pytest.raises(ValueError, match="review_threads is empty"):
        publish_followup_task(
            pr_state=pr_state,
            repo="test-org/test-repo",
            token="fake-token",
            redis=fake_redis_client,
            label_config=label_config,
            default_runner="claude",
        )


# --- Additional coverage ---


def test_publish_fix_task_sets_fix_ci_type(
    gh_mock,
    fake_redis_client,
    label_config,
):
    """When CI failure is classified as CIFailureType.CODE, the task type
    should be TaskType.FIX_CI."""
    _setup_gh_defaults(gh_mock)
    # Return log text containing a code-error pattern that triggers CODE classification
    gh_mock.get_failed_run_logs.return_value = (
        "Collecting tests...\nERROR: SyntaxError: invalid syntax\n"
    )

    ci_failures = _make_ci_failures_with_urls(run_ids=[12345], names=["tests"])
    pr_state = _make_pr_state(number=600, ci_failures=ci_failures)

    task = publish_fix_task(
        pr_state=pr_state,
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
        default_runner="claude",
    )

    assert task.type == TaskType.FIX_CI


def test_extract_run_id_direct():
    """Direct tests for _extract_run_id: valid URL, non-matching URL, and
    empty string."""
    from orcest.orchestrator.task_publisher import _extract_run_id

    # Valid GitHub Actions URL extracts the integer run ID
    assert _extract_run_id("https://github.com/org/repo/actions/runs/123456/job/789") == 123456

    # Non-matching URL returns None
    assert _extract_run_id("https://circleci.com/gh/org/repo/12345") is None

    # Empty string returns None
    assert _extract_run_id("") is None


def test_publish_fix_task_diff_fetch_failure(
    gh_mock,
    fake_redis_client,
    label_config,
):
    """When gh.get_pr_diff raises GhCliError, the exception propagates
    (publish_fix_task does not catch it)."""
    _setup_gh_defaults(gh_mock)
    gh_mock.get_pr_diff.side_effect = GhCliError(
        "gh command failed (exit 1): not found", stderr="not found"
    )
    pr_state = _make_pr_state(number=601)

    with pytest.raises(GhCliError):
        publish_fix_task(
            pr_state=pr_state,
            repo="test-org/test-repo",
            token="fake-token",
            redis=fake_redis_client,
            label_config=label_config,
            default_runner="claude",
        )


def test_publish_fix_task_log_budget_exhaustion(
    gh_mock,
    fake_redis_client,
    label_config,
):
    """Multiple CI checks that collectively exhaust the 15k total budget.
    The last check should get truncated or empty log in the prompt.

    Logs are intentionally larger than _PER_CHECK_LOG_LIMIT (5000) so that
    the truncation-indicator path is exercised and the budget accounting
    (which subtracts the *capped* length, not the indicator-inflated
    excerpt length) is validated.
    """
    _setup_gh_defaults(gh_mock)
    # Each check has 6000 chars of logs (> 5000 per-check limit).
    # Per-check cap is 5000, so each consumes 5000 of the 15000 budget.
    # With 4 checks that is 20k capped total but only 15k budget, so the
    # 4th check should have no log excerpt.
    log_a = "A" * 6000
    log_b = "B" * 6000
    log_c = "C" * 6000
    log_d = "D" * 6000

    # Each check has its own unique run_id
    ci_failures = _make_ci_failures_with_urls(
        run_ids=[1001, 1002, 1003, 1004],
        names=["check-a", "check-b", "check-c", "check-d"],
    )
    pr_state = _make_pr_state(number=602, ci_failures=ci_failures)

    # Map each run_id to its log text
    def mock_get_logs(repo, run_id, token):
        return {1001: log_a, 1002: log_b, 1003: log_c, 1004: log_d}[run_id]

    gh_mock.get_failed_run_logs.side_effect = mock_get_logs

    task = publish_fix_task(
        pr_state=pr_state,
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
        default_runner="claude",
    )

    # First three checks consume the full 15k budget (5k each, capped from 6k)
    assert "A" * 100 in task.prompt  # check-a log present
    assert "B" * 100 in task.prompt  # check-b log present
    assert "C" * 100 in task.prompt  # check-c log present
    # Truncation indicator should appear (logs were > per-check limit)
    assert "truncated" in task.prompt
    # Fourth check should NOT have its log in the prompt (budget exhausted)
    assert "D" * 100 not in task.prompt


def test_publish_and_notify_both_label_and_comment_fail(
    gh_mock,
    fake_redis_client,
    label_config,
    caplog,
):
    """Both add_label and post_comment raise simultaneously. The warning
    log should indicate both failed."""
    import logging

    _setup_gh_defaults(gh_mock)
    gh_mock.add_label.side_effect = RuntimeError("label fail")
    gh_mock.post_comment.side_effect = RuntimeError("comment fail")
    pr_state = _make_pr_state(number=603)

    with caplog.at_level(logging.WARNING):
        task = publish_fix_task(
            pr_state=pr_state,
            repo="test-org/test-repo",
            token="fake-token",
            redis=fake_redis_client,
            label_config=label_config,
            default_runner="claude",
        )

    # Task is still published to Redis despite both failures
    assert isinstance(task, Task)
    entries = fake_redis_client.client.xrange("tasks:claude")
    assert any(f["id"] == task.id for _, f in entries)

    # The warning log should show both label and comment as FAILED
    warning_msgs = [r.message for r in caplog.records if r.levelno == logging.WARNING]
    assert any("label=FAILED" in m and "comment=FAILED" in m for m in warning_msgs)


def test_publish_and_notify_xadd_failure(
    gh_mock,
    fake_redis_client,
    label_config,
):
    """When redis.xadd raises (Redis down after task construction), the
    exception should propagate."""
    _setup_gh_defaults(gh_mock)
    pr_state = _make_pr_state(number=604)

    # Sabotage xadd to simulate Redis failure
    original_xadd = fake_redis_client.xadd

    def broken_xadd(stream, fields):
        raise ConnectionError("Redis connection lost")

    fake_redis_client.xadd = broken_xadd

    try:
        with pytest.raises(ConnectionError, match="Redis connection lost"):
            publish_fix_task(
                pr_state=pr_state,
                repo="test-org/test-repo",
                token="fake-token",
                redis=fake_redis_client,
                label_config=label_config,
                default_runner="claude",
            )
    finally:
        fake_redis_client.xadd = original_xadd


def test_render_review_threads_missing_body():
    """A review thread comment with body=None should use empty string."""
    from orcest.orchestrator.task_publisher import _render_review_threads

    threads = [
        {
            "path": "src/app.py",
            "line": 10,
            "comments": [
                {"author": "reviewer", "body": None},
            ],
        },
    ]

    lines = _render_review_threads(threads)
    rendered = "\n".join(lines)

    # Should not crash and should contain the author
    assert "reviewer" in rendered
    # The body=None should have been replaced with empty string,
    # so no "None" literal should appear after the author
    assert "**reviewer**: " in rendered
    assert "None" not in rendered
