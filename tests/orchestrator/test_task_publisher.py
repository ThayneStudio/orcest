"""Tests for orcest.orchestrator.task_publisher.

Verifies task creation, Redis stream publishing, GitHub comment
side effects, prompt diff truncation, CI log fetching/rendering, and
review thread prompt rendering for both fix and followup tasks.
"""

import logging

import pytest

from orcest.orchestrator.gh import GhCliError
from orcest.orchestrator.issue_ops import IssueAction, IssueState
from orcest.orchestrator.pr_ops import PRAction, PRState
from orcest.orchestrator.task_publisher import (
    _render_rebase_prompt,
    publish_fix_task,
    publish_followup_task,
    publish_issue_task,
)
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
    gh_mock.get_pr_review_comments.return_value = []
    gh_mock.get_unresolved_review_threads.return_value = []
    gh_mock.post_comment.return_value = None


def test_publish_creates_task(gh_mock, fake_redis_client):
    """publish_fix_task returns a Task with the correct type, repo, and resource_id."""
    _setup_gh_defaults(gh_mock)
    pr_state = _make_pr_state(number=42)

    task = publish_fix_task(
        pr_state=pr_state,
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        default_runner="claude",
    )

    assert isinstance(task, Task)
    assert task.repo == "test-org/test-repo"
    assert task.resource_id == 42
    assert task.resource_type == "pr"
    assert task.branch == "fix/widget"
    # No CI failures -> FIX_PR (review-driven path)
    assert task.type == TaskType.FIX_PR


def test_publish_adds_to_stream(gh_mock, fake_redis_client):
    """After publishing, the Redis 'tasks' stream contains an entry."""
    _setup_gh_defaults(gh_mock)
    pr_state = _make_pr_state(number=7)

    task = publish_fix_task(
        pr_state=pr_state,
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        default_runner="claude",
    )

    # Read all entries from the tasks stream
    default_runner = "claude"
    stream = fake_redis_client._prefixed(f"tasks:{default_runner}")
    entries = fake_redis_client.client.xrange(stream)
    assert len(entries) >= 1

    # The last entry should match our task
    _entry_id, fields = entries[-1]
    assert fields["id"] == task.id
    assert fields["repo"] == "test-org/test-repo"
    assert fields["resource_id"] == "7"


def test_publish_does_not_add_label(gh_mock, fake_redis_client):
    """publish_fix_task does not call gh.add_label (labels removed)."""
    _setup_gh_defaults(gh_mock)
    pr_state = _make_pr_state(number=15)

    publish_fix_task(
        pr_state=pr_state,
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        default_runner="claude",
    )

    gh_mock.add_label.assert_not_called()


def test_publish_does_not_post_comment(gh_mock, fake_redis_client):
    """publish_fix_task does not post a comment (queued comments are noise)."""
    _setup_gh_defaults(gh_mock)
    pr_state = _make_pr_state(number=20)

    publish_fix_task(
        pr_state=pr_state,
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        default_runner="claude",
    )

    gh_mock.post_comment.assert_not_called()


def test_prompt_truncates_long_diff(gh_mock, fake_redis_client):
    """A diff longer than 10,000 characters is truncated in the prompt."""
    long_diff = "x" * 15000
    _setup_gh_defaults(gh_mock)
    gh_mock.get_pr_diff.return_value = long_diff

    pr_state = _make_pr_state(number=33)

    task = publish_fix_task(
        pr_state=pr_state,
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
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
        default_runner="claude",
    )

    # Should only fetch once despite two checks
    gh_mock.get_failed_run_logs.assert_called_once_with("test-org/test-repo", 55555, "fake-token")


def test_publish_graceful_on_log_fetch_failure(
    gh_mock,
    fake_redis_client,
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
):
    """CI logs longer than 20,000 chars are truncated to the last 20,000."""
    _setup_gh_defaults(gh_mock)
    # Create a log that's well over the per-check limit
    long_log = "x" * 12000 + "REAL_ERROR_AT_END" + "y" * 12000
    gh_mock.get_failed_run_logs.return_value = long_log

    ci_failures = _make_ci_failures_with_urls(run_ids=[88888], names=["tests"])
    pr_state = _make_pr_state(number=54, ci_failures=ci_failures)

    task = publish_fix_task(
        pr_state=pr_state,
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        default_runner="claude",
    )

    # The full log (24,017 chars) should NOT appear verbatim
    assert long_log not in task.prompt
    # The end of the log (where errors are) should be present
    assert "REAL_ERROR_AT_END" in task.prompt
    assert "y" * 3000 in task.prompt
    # Truncation indicator should appear
    assert "truncated" in task.prompt


def test_prompt_early_error_path_captures_marker_before_tail(
    gh_mock,
    fake_redis_client,
):
    """Error markers before the tail window are included via the early-error path.

    With the error at position 0 and a total log of 25,009 chars, the plain
    tail slice (last 20,000 chars) starts at position 5,009 and would miss
    the "FAILURES" marker entirely.  The early-error branch must capture it.
    """
    _setup_gh_defaults(gh_mock)
    # "FAILURES\n" at position 0 (word boundary after S), followed by filler,
    # then more filler to push the total length well past the 20,000 per-check
    # limit.  Matches the \bFAILURES\b pattern used in _LOG_ERROR_RE.
    long_log = "FAILURES\n" + "x" * 5000 + "y" * 20000  # 25,009 chars total
    gh_mock.get_failed_run_logs.return_value = long_log

    ci_failures = _make_ci_failures_with_urls(run_ids=[88889], names=["tests"])
    pr_state = _make_pr_state(number=55, ci_failures=ci_failures)

    task = publish_fix_task(
        pr_state=pr_state,
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        default_runner="claude",
    )

    # The early error marker must appear even though it sits before the tail window
    assert "FAILURES" in task.prompt
    # The mid-log separator signals that the early-error branch was taken
    assert "middle of log omitted" in task.prompt
    # The tail portion should also be present
    assert "y" * 1000 in task.prompt
    # The full log should not appear verbatim (it was truncated)
    assert long_log not in task.prompt


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
        default_runner="claude",
    )

    # Check thread details appear in the prompt (including thread IDs)
    assert "src/handler.py" in task.prompt
    assert "55" in task.prompt
    assert "PRRT_abc" in task.prompt
    assert "alice" in task.prompt
    assert "This function is too long, split it up." in task.prompt
    assert "bob" in task.prompt
    assert "Agreed, especially the parsing logic." in task.prompt
    assert "tests/test_handler.py" in task.prompt
    assert "12" in task.prompt
    assert "PRRT_def" in task.prompt
    assert "Missing edge case test for empty input." in task.prompt


def test_publish_review_fix_resolve_instructions(
    gh_mock,
    fake_redis_client,
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
        default_runner="claude",
    )

    assert "resolve" in task.prompt.lower()
    assert "gh api graphql" in task.prompt
    assert "resolveReviewThread" in task.prompt
    assert "Do NOT call `gh pr review" in task.prompt


def test_publish_followup_triage_prompt(
    gh_mock,
    fake_redis_client,
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


def test_publish_fix_task_survives_comment_failure(
    gh_mock,
    fake_redis_client,
):
    """publish_fix_task returns a task even when post_comment raises, because
    the task is already in Redis and must not be lost."""
    _setup_gh_defaults(gh_mock)
    gh_mock.post_comment.side_effect = RuntimeError("GitHub 500")
    pr_state = _make_pr_state(number=300)

    task = publish_fix_task(
        pr_state=pr_state,
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        default_runner="claude",
    )

    assert isinstance(task, Task)
    assert task.resource_id == 300
    # Task should be in Redis despite comment failure
    entries = fake_redis_client.client.xrange(fake_redis_client._prefixed("tasks:claude"))
    assert any(f["id"] == task.id for _, f in entries)
    # No labels are added by publish
    gh_mock.add_label.assert_not_called()


def test_publish_followup_task_survives_comment_failure(
    gh_mock,
    fake_redis_client,
):
    """publish_followup_task returns a task even when post_comment raises,
    and no labels are added."""
    _setup_gh_defaults(gh_mock)
    gh_mock.post_comment.side_effect = RuntimeError("GitHub 500")
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
        default_runner="claude",
    )

    assert isinstance(task, Task)
    assert task.type == TaskType.TRIAGE_FOLLOWUPS
    # No labels are added by publish
    gh_mock.add_label.assert_not_called()


# --- _render_review_threads edge cases ---


def test_review_fix_prompt_handles_missing_thread_fields(
    gh_mock,
    fake_redis_client,
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
        default_runner="claude",
    )

    # Should render without crashing; unknown/? should appear for missing values
    assert "unknown" in task.prompt  # path fallback and/or author fallback
    assert "?" in task.prompt  # line number fallback
    assert "Some feedback" in task.prompt


def test_publish_followup_task_rejects_empty_threads(
    gh_mock,
    fake_redis_client,
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
            default_runner="claude",
        )


# --- Additional coverage ---


def test_publish_fix_task_sets_fix_ci_type(
    gh_mock,
    fake_redis_client,
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
            default_runner="claude",
        )


def test_publish_fix_task_log_budget_exhaustion(
    gh_mock,
    fake_redis_client,
):
    """Multiple CI checks that collectively exhaust the 50k total budget.
    The last check should get truncated or empty log in the prompt.

    Logs are intentionally larger than _PER_CHECK_LOG_LIMIT (20000) so that
    the truncation-indicator path is exercised and the budget accounting
    (which subtracts the *capped* length, not the indicator-inflated
    excerpt length) is validated.
    """
    _setup_gh_defaults(gh_mock)
    # Each check has 22000 chars of logs (> 20000 per-check limit).
    # Per-check cap is 20000, so A and B each consume 20000 of the 50000 budget.
    # C gets the remaining 10000 (min of 20000 and 10000).
    # With budget exhausted, the 4th check should have no log excerpt.
    log_a = "A" * 22000
    log_b = "B" * 22000
    log_c = "C" * 22000
    log_d = "D" * 22000

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
        default_runner="claude",
    )

    # First three checks consume the full 50k budget (A:20k, B:20k, C:10k, capped from 22k)
    assert "A" * 100 in task.prompt  # check-a log present
    assert "B" * 100 in task.prompt  # check-b log present
    assert "C" * 100 in task.prompt  # check-c log present
    # Truncation indicator should appear (logs were > per-check limit)
    assert "truncated" in task.prompt
    # Fourth check should NOT have its log in the prompt (budget exhausted)
    assert "D" * 100 not in task.prompt


def test_publish_does_not_comment_even_if_gh_would_fail(
    gh_mock,
    fake_redis_client,
):
    """publish_fix_task never calls post_comment, so gh errors don't matter."""
    _setup_gh_defaults(gh_mock)
    gh_mock.post_comment.side_effect = RuntimeError("comment fail")
    pr_state = _make_pr_state(number=603)

    task = publish_fix_task(
        pr_state=pr_state,
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        default_runner="claude",
    )

    # Task is still published to Redis
    assert isinstance(task, Task)
    entries = fake_redis_client.client.xrange(fake_redis_client._prefixed("tasks:claude"))
    assert any(f["id"] == task.id for _, f in entries)
    # No comment was attempted
    gh_mock.post_comment.assert_not_called()


def test_publish_and_notify_xadd_failure(
    gh_mock,
    fake_redis_client,
):
    """When redis.xadd_capped raises (Redis down after task construction), the
    exception should propagate."""
    _setup_gh_defaults(gh_mock)
    pr_state = _make_pr_state(number=604)

    # Patch xadd_capped (not xadd): task_publisher calls redis.xadd_capped()
    # directly, and xadd_capped calls self._client.xadd internally, bypassing
    # the RedisClient.xadd wrapper. Patching the lower-level xadd would have
    # no effect on the publisher's code path.
    original_xadd_capped = fake_redis_client.xadd_capped

    def broken_xadd_capped(stream, fields, **kwargs):
        raise ConnectionError("Redis connection lost")

    fake_redis_client.xadd_capped = broken_xadd_capped

    try:
        with pytest.raises(ConnectionError, match="Redis connection lost"):
            publish_fix_task(
                pr_state=pr_state,
                repo="test-org/test-repo",
                token="fake-token",
                redis=fake_redis_client,
                default_runner="claude",
            )
    finally:
        fake_redis_client.xadd_capped = original_xadd_capped


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


# --- Inline review comment capture tests ---


def test_publish_ci_fix_includes_inline_review_comments(
    gh_mock,
    fake_redis_client,
):
    """When CI failures are present, unresolved review threads are fetched
    via GraphQL and included in the prompt."""
    _setup_gh_defaults(gh_mock)
    gh_mock.get_failed_run_logs.return_value = ""
    gh_mock.get_unresolved_review_threads.return_value = [
        {
            "id": "PRRT_foo42",
            "path": "src/foo.py",
            "line": 42,
            "comments": [
                {"author": "reviewer", "body": "fix this variable on line 42"},
            ],
        },
    ]
    ci_failures = [
        {
            "name": "pytest",
            "conclusion": "FAILURE",
            "detailsUrl": "https://circleci.com/gh/org/repo/999",
        },
    ]
    pr_state = _make_pr_state(number=700, ci_failures=ci_failures)

    task = publish_fix_task(
        pr_state=pr_state,
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        default_runner="claude",
    )

    # get_unresolved_review_threads should be called for CI fix tasks
    gh_mock.get_unresolved_review_threads.assert_called_once_with(
        "test-org/test-repo", 700, "fake-token"
    )
    # Review thread content should appear in the prompt
    assert "src/foo.py" in task.prompt
    assert "42" in task.prompt
    assert "fix this variable on line 42" in task.prompt
    assert "reviewer" in task.prompt


def test_publish_ci_fix_multiple_review_threads(
    gh_mock,
    fake_redis_client,
):
    """Multiple review threads from GraphQL are included in the prompt."""
    _setup_gh_defaults(gh_mock)
    gh_mock.get_failed_run_logs.return_value = ""
    gh_mock.get_unresolved_review_threads.return_value = [
        {
            "id": "PRRT_t1",
            "path": "src/bar.py",
            "line": 10,
            "comments": [
                {"author": "alice", "body": "First comment on this line"},
                {"author": "bob", "body": "Second comment on this line"},
            ],
        },
        {
            "id": "PRRT_t2",
            "path": "src/bar.py",
            "line": 20,
            "comments": [
                {"author": "alice", "body": "Comment on a different line"},
            ],
        },
    ]
    ci_failures = [
        {
            "name": "lint",
            "conclusion": "FAILURE",
            "detailsUrl": "https://circleci.com/gh/org/repo/111",
        },
    ]
    pr_state = _make_pr_state(number=701, ci_failures=ci_failures)

    task = publish_fix_task(
        pr_state=pr_state,
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        default_runner="claude",
    )

    # All comment bodies should appear in the prompt
    assert "First comment on this line" in task.prompt
    assert "Second comment on this line" in task.prompt
    assert "Comment on a different line" in task.prompt


def test_publish_ci_fix_graceful_on_review_thread_fetch_failure(
    gh_mock,
    fake_redis_client,
):
    """If get_unresolved_review_threads raises, task creation still succeeds
    and the prompt still includes CI failure info."""
    _setup_gh_defaults(gh_mock)
    gh_mock.get_failed_run_logs.return_value = ""
    gh_mock.get_unresolved_review_threads.side_effect = GhCliError(
        "gh command failed (exit 1): not found", stderr="not found"
    )
    ci_failures = [
        {
            "name": "pytest",
            "conclusion": "FAILURE",
            "detailsUrl": "https://circleci.com/gh/org/repo/222",
        },
    ]
    pr_state = _make_pr_state(number=702, ci_failures=ci_failures)

    task = publish_fix_task(
        pr_state=pr_state,
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        default_runner="claude",
    )

    assert isinstance(task, Task)
    # CI failure info should still appear despite inline comment fetch failure
    assert "pytest" in task.prompt


def test_publish_review_fix_uses_pr_state_threads(
    gh_mock,
    fake_redis_client,
):
    """When there are no CI failures, pr_state.review_threads is used directly.
    No additional GitHub API calls are made for review threads."""
    _setup_gh_defaults(gh_mock)
    threads = _make_sample_threads()
    pr_state = _make_pr_state(
        number=703,
        ci_failures=[],
        review_threads=threads,
    )

    task = publish_fix_task(
        pr_state=pr_state,
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        default_runner="claude",
    )

    # Should not call get_unresolved_review_threads — use pr_state threads instead
    gh_mock.get_unresolved_review_threads.assert_not_called()
    # Existing review thread details should still appear
    assert "src/handler.py" in task.prompt
    assert isinstance(task, Task)


def test_group_inline_comments_direct():
    """Direct tests for _group_inline_comments: groups by (path, line)."""
    from orcest.orchestrator.task_publisher import _group_inline_comments

    comments = [
        {"path": "a.py", "line": 1, "author": "u1", "body": "first"},
        {"path": "a.py", "line": 1, "author": "u2", "body": "second"},
        {"path": "b.py", "line": 5, "author": "u1", "body": "third"},
    ]
    threads = _group_inline_comments(comments)

    # Should produce two thread groups
    assert len(threads) == 2

    # Find the a.py group
    a_thread = next(t for t in threads if t["path"] == "a.py")
    assert a_thread["line"] == 1
    assert len(a_thread["comments"]) == 2
    assert {"author": "u1", "body": "first"} in a_thread["comments"]
    assert {"author": "u2", "body": "second"} in a_thread["comments"]

    # Find the b.py group
    b_thread = next(t for t in threads if t["path"] == "b.py")
    assert b_thread["line"] == 5
    assert b_thread["comments"] == [{"author": "u1", "body": "third"}]


def test_group_inline_comments_empty():
    """_group_inline_comments returns empty list for empty input."""
    from orcest.orchestrator.task_publisher import _group_inline_comments

    assert _group_inline_comments([]) == []


# --- increment_attempts failure: Option A early-return behavior ---


def test_publish_and_notify_skips_xadd_on_increment_failure(
    gh_mock,
    fake_redis_client,
    mocker,
    caplog,
):
    """When increment_attempts raises, _publish_and_notify returns early and
    the task is NOT published to Redis (Option A: skip publish on counter failure)."""
    _setup_gh_defaults(gh_mock)
    mocker.patch(
        "orcest.orchestrator.task_publisher.increment_attempts",
        side_effect=ConnectionError("Redis down"),
    )
    pr_state = _make_pr_state(number=800)

    with caplog.at_level(logging.ERROR):
        task = publish_fix_task(
            pr_state=pr_state,
            repo="test-org/test-repo",
            token="fake-token",
            redis=fake_redis_client,
            default_runner="claude",
        )

    # Task should NOT be published to Redis
    entries = fake_redis_client.client.xrange(fake_redis_client._prefixed("tasks:claude"))
    assert not any(f["id"] == task.id for _, f in entries)

    # Pending marker should be cleared so the PR can be retried immediately
    pending_key = fake_redis_client._prefixed("pending:pr:test-org/test-repo:800")
    assert fake_redis_client.client.get(pending_key) is None

    # Error should be logged with skip rationale
    error_msgs = [r.message for r in caplog.records if r.levelno == logging.ERROR]
    assert any("Failed to increment attempt counter" in m for m in error_msgs)
    assert any("skipping publish" in m for m in error_msgs)


def test_publish_issue_and_notify_skips_xadd_on_increment_failure(
    gh_mock,
    fake_redis_client,
    mocker,
    caplog,
):
    """When increment_issue_attempts raises, _publish_issue_and_notify returns
    early and the task is NOT published to Redis (Option A)."""
    _setup_gh_defaults(gh_mock)
    mocker.patch(
        "orcest.orchestrator.task_publisher.increment_issue_attempts",
        side_effect=ConnectionError("Redis down"),
    )
    issue_state = IssueState(
        number=801,
        title="Test issue",
        body="Test issue body",
        action=IssueAction.ENQUEUE_IMPLEMENT,
        labels=[],
    )

    with caplog.at_level(logging.ERROR):
        task = publish_issue_task(
            issue_state=issue_state,
            repo="test-org/test-repo",
            token="fake-token",
            redis=fake_redis_client,
            default_runner="claude",
        )

    # Task should NOT be published to Redis
    entries = fake_redis_client.client.xrange(fake_redis_client._prefixed("tasks:issue:claude"))
    assert not any(f["id"] == task.id for _, f in entries)

    # Pending marker should be cleared so the issue can be retried immediately
    pending_key = fake_redis_client._prefixed("pending:issue:test-org/test-repo:801")
    assert fake_redis_client.client.get(pending_key) is None

    # Error should be logged with skip rationale
    error_msgs = [r.message for r in caplog.records if r.levelno == logging.ERROR]
    assert any("Failed to increment attempt counter" in m for m in error_msgs)
    assert any("skipping publish" in m for m in error_msgs)


# --- Rebase prompt tests ---


def test_rebase_prompt_uses_base_branch():
    """_render_rebase_prompt uses the base_branch parameter instead of hardcoding master."""
    prompt = _render_rebase_prompt(
        pr_number=1,
        pr_title="Test PR",
        branch="fix/thing",
        repo="test-org/test-repo",
        base_branch="develop",
    )
    assert "git fetch origin develop" in prompt
    assert "git rebase origin/develop" in prompt
    assert "master" not in prompt


def test_rebase_prompt_defaults_to_main():
    """_render_rebase_prompt defaults to main when base_branch is not specified."""
    prompt = _render_rebase_prompt(
        pr_number=1,
        pr_title="Test PR",
        branch="fix/thing",
        repo="test-org/test-repo",
    )
    assert "git fetch origin main" in prompt
    assert "git rebase origin/main" in prompt


# --- Transient CI failure handling tests ---


def _make_transient_ci_failures(run_ids: list[int]) -> list[dict]:
    """Build ci_failures where each check has a transient-matching name and a GitHub Actions URL."""
    return [
        {
            "name": f"timeout-check-{i}",
            "conclusion": "TIMED_OUT",
            "detailsUrl": f"https://github.com/org/repo/actions/runs/{run_id}/job/{9000 + i}",
        }
        for i, run_id in enumerate(run_ids)
    ]


def test_all_transient_failures_retrigger_ci_not_enqueue(gh_mock, fake_redis_client):
    """When all CI failures are transient, publish_fix_task re-triggers CI
    and returns None instead of enqueueing a Claude task."""
    _setup_gh_defaults(gh_mock)
    # Name contains 'timeout' → TRANSIENT classification
    gh_mock.get_failed_run_logs.return_value = "connection reset by peer"

    ci_failures = _make_transient_ci_failures([42001])
    pr_state = _make_pr_state(number=900, ci_failures=ci_failures)

    result = publish_fix_task(
        pr_state=pr_state,
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        default_runner="claude",
    )

    # Should return None (no Claude task)
    assert result is None
    # Should have called rerun_workflow for the run
    gh_mock.rerun_workflow.assert_called_once_with(
        "test-org/test-repo", 42001, "fake-token", failed_only=True
    )
    # Nothing should be in the tasks stream
    entries = fake_redis_client.client.xrange(fake_redis_client._prefixed("tasks:claude"))
    assert len(entries) == 0


def test_all_transient_failures_does_not_increment_main_attempts(gh_mock, fake_redis_client):
    """All-transient path does not increment the main per-SHA attempt counter."""
    from orcest.orchestrator.pr_ops import get_attempt_count

    _setup_gh_defaults(gh_mock)
    gh_mock.get_failed_run_logs.return_value = "timeout"

    ci_failures = _make_transient_ci_failures([42002])
    pr_state = _make_pr_state(number=901, ci_failures=ci_failures)

    publish_fix_task(
        pr_state=pr_state,
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        default_runner="claude",
    )

    # Main attempt counter must still be 0
    assert get_attempt_count(fake_redis_client, "test-org/test-repo", 901, "abc123") == 0


def test_transient_retries_counted_separately(gh_mock, fake_redis_client):
    """Each call to publish_fix_task for all-transient failures increments
    the transient counter, not the main attempt counter."""
    from orcest.orchestrator.pr_ops import get_transient_attempt_count

    _setup_gh_defaults(gh_mock)
    gh_mock.get_failed_run_logs.return_value = "ETIMEDOUT"

    ci_failures = _make_transient_ci_failures([42003])
    pr_state = _make_pr_state(number=902, ci_failures=ci_failures)

    for expected_count in (1, 2, 3):
        publish_fix_task(
            pr_state=pr_state,
            repo="test-org/test-repo",
            token="fake-token",
            redis=fake_redis_client,
            default_runner="claude",
        )
        count = get_transient_attempt_count(fake_redis_client, "test-org/test-repo", 902, "abc123")
        assert count == expected_count


def test_transient_budget_exhausted_falls_back_to_fix_task(gh_mock, fake_redis_client):
    """After _MAX_TRANSIENT_RETRIES transient re-triggers, publish_fix_task
    falls back to enqueuing a Claude fix task (returning a Task, not None)."""
    from orcest.orchestrator.task_publisher import _MAX_TRANSIENT_RETRIES

    _setup_gh_defaults(gh_mock)
    gh_mock.get_failed_run_logs.return_value = "ETIMEDOUT"

    ci_failures = _make_transient_ci_failures([42004])
    pr_state = _make_pr_state(number=903, ci_failures=ci_failures)

    # Exhaust the transient budget
    for _ in range(_MAX_TRANSIENT_RETRIES):
        publish_fix_task(
            pr_state=pr_state,
            repo="test-org/test-repo",
            token="fake-token",
            redis=fake_redis_client,
            default_runner="claude",
        )

    # Reset tasks stream so we can check the fallback enqueue
    fake_redis_client.delete("tasks:claude")

    result = publish_fix_task(
        pr_state=pr_state,
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        default_runner="claude",
    )

    # Now should return a Task (fallback to Claude)
    assert isinstance(result, Task)
    # And the task should be in the stream
    entries = fake_redis_client.client.xrange(fake_redis_client._prefixed("tasks:claude"))
    assert len(entries) == 1


def test_mixed_failures_enqueue_fix_task(gh_mock, fake_redis_client):
    """When failures include both TRANSIENT and CODE, publish_fix_task
    enqueues a Claude fix task (not a transient re-trigger)."""
    _setup_gh_defaults(gh_mock)

    def mock_get_logs(repo, run_id, token):
        # run 10001 → transient log, run 10002 → code error log
        return {
            10001: "connection reset by peer",
            10002: "FAILED test_foo.py::test_bar (AssertionError)",
        }.get(run_id, "")

    gh_mock.get_failed_run_logs.side_effect = mock_get_logs

    ci_failures = [
        {
            "name": "flaky-network",
            "conclusion": "FAILURE",
            "detailsUrl": "https://github.com/org/repo/actions/runs/10001/job/1",
        },
        {
            "name": "tests",
            "conclusion": "FAILURE",
            "detailsUrl": "https://github.com/org/repo/actions/runs/10002/job/2",
        },
    ]
    pr_state = _make_pr_state(number=904, ci_failures=ci_failures)

    result = publish_fix_task(
        pr_state=pr_state,
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        default_runner="claude",
    )

    # Mixed failures → Claude task, not a transient re-trigger
    assert isinstance(result, Task)
    # rerun_workflow should NOT be called for mixed failures
    gh_mock.rerun_workflow.assert_not_called()
    # Task should be in the stream
    entries = fake_redis_client.client.xrange(fake_redis_client._prefixed("tasks:claude"))
    assert len(entries) == 1


def test_all_transient_deduplicates_run_ids(gh_mock, fake_redis_client):
    """Two transient checks with the same run_id only trigger one rerun call."""
    _setup_gh_defaults(gh_mock)
    gh_mock.get_failed_run_logs.return_value = "timeout"

    # Two checks sharing run_id 77777
    ci_failures = [
        {
            "name": "timeout-job-a",
            "conclusion": "TIMED_OUT",
            "detailsUrl": "https://github.com/org/repo/actions/runs/77777/job/1",
        },
        {
            "name": "timeout-job-b",
            "conclusion": "TIMED_OUT",
            "detailsUrl": "https://github.com/org/repo/actions/runs/77777/job/2",
        },
    ]
    pr_state = _make_pr_state(number=905, ci_failures=ci_failures)

    publish_fix_task(
        pr_state=pr_state,
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        default_runner="claude",
    )

    # rerun_workflow called exactly once despite two checks
    gh_mock.rerun_workflow.assert_called_once()


def test_all_transient_graceful_on_rerun_failure(gh_mock, fake_redis_client):
    """If all rerun_workflow calls raise, publish_fix_task falls back to a Claude
    fix task immediately (the transient retry is still counted)."""
    from orcest.orchestrator.gh import GhCliError
    from orcest.orchestrator.pr_ops import get_transient_attempt_count

    _setup_gh_defaults(gh_mock)
    gh_mock.get_failed_run_logs.return_value = "timeout"
    gh_mock.rerun_workflow.side_effect = GhCliError("gh run rerun failed")

    ci_failures = _make_transient_ci_failures([99001])
    pr_state = _make_pr_state(number=906, ci_failures=ci_failures)

    result = publish_fix_task(
        pr_state=pr_state,
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        default_runner="claude",
    )

    # Falls back to Claude fix task since no runs were re-triggered
    assert isinstance(result, Task)
    # Transient counter was still incremented
    assert get_transient_attempt_count(fake_redis_client, "test-org/test-repo", 906, "abc123") == 1
    # Task enqueued in stream
    assert len(fake_redis_client.client.xrange(fake_redis_client._prefixed("tasks:claude"))) == 1


def test_no_ci_failures_not_transient_path(gh_mock, fake_redis_client):
    """With no CI failures (review-only fix), the transient path is not triggered."""
    _setup_gh_defaults(gh_mock)
    threads = _make_sample_threads()
    pr_state = _make_pr_state(number=907, ci_failures=[], review_threads=threads)

    result = publish_fix_task(
        pr_state=pr_state,
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        default_runner="claude",
    )

    # Review-only fix: Claude task created normally
    assert isinstance(result, Task)
    gh_mock.rerun_workflow.assert_not_called()
