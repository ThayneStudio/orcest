"""Tests for orcest.orchestrator.issue_ops.discover_actionable_issues().

Exercises the filter cascade: labels -> locks -> pending tasks -> attempts.
Each test uses the issue_gh_mock fixture (list_labeled_issues mocked) and
fake_redis_client (fakeredis-backed RedisClient).
"""

import pytest

from orcest.orchestrator.issue_ops import (
    IssueAction,
    clear_attempts,
    discover_actionable_issues,
    get_attempt_count,
    increment_attempts,
)
from orcest.shared.coordination import make_issue_lock_key, make_pending_task_key

REPO = "test-org/test-repo"
TOKEN = "fake-token"


@pytest.fixture
def issue_gh_mock(mocker):
    """Patch list_labeled_issues in orcest.orchestrator.gh."""
    return mocker.patch("orcest.orchestrator.gh.list_labeled_issues")


def _make_issue_data(
    number: int = 1,
    title: str = "Implement feature X",
    body: str = "Some description",
    labels: list[dict] | None = None,
) -> dict:
    """Build an issue dict matching the shape returned by gh.list_labeled_issues."""
    return {
        "number": number,
        "title": title,
        "body": body,
        "labels": labels or [],
    }


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_happy_path_enqueue_implement(issue_gh_mock, fake_redis_client, label_config):
    """An issue with orcest:ready and no blockers is ENQUEUE_IMPLEMENT."""
    issue_gh_mock.return_value = [
        _make_issue_data(number=1, labels=[{"name": "orcest:ready"}]),
    ]

    results = discover_actionable_issues(
        repo=REPO,
        token=TOKEN,
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == IssueAction.ENQUEUE_IMPLEMENT
    assert results[0].number == 1
    issue_gh_mock.assert_called_once_with(REPO, label_config.ready, TOKEN)


def test_empty_issue_list(issue_gh_mock, fake_redis_client, label_config):
    """When there are no labeled issues, the result list is empty."""
    issue_gh_mock.return_value = []

    results = discover_actionable_issues(
        repo=REPO,
        token=TOKEN,
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert results == []


def test_multiple_actionable_issues(issue_gh_mock, fake_redis_client, label_config):
    """Multiple clean issues are all returned as ENQUEUE_IMPLEMENT."""
    issue_gh_mock.return_value = [
        _make_issue_data(number=10),
        _make_issue_data(number=20),
        _make_issue_data(number=30),
    ]

    results = discover_actionable_issues(
        repo=REPO,
        token=TOKEN,
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 3
    assert all(r.action == IssueAction.ENQUEUE_IMPLEMENT for r in results)
    assert [r.number for r in results] == [10, 20, 30]


# ---------------------------------------------------------------------------
# Terminal labels
# ---------------------------------------------------------------------------


def test_skip_blocked_label(issue_gh_mock, fake_redis_client, label_config):
    """An issue with orcest:blocked is classified as SKIP_LABELED."""
    issue_gh_mock.return_value = [
        _make_issue_data(
            number=5,
            labels=[{"name": "orcest:ready"}, {"name": "orcest:blocked"}],
        ),
    ]

    results = discover_actionable_issues(
        repo=REPO,
        token=TOKEN,
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == IssueAction.SKIP_LABELED
    assert results[0].number == 5


def test_skip_needs_human_label(issue_gh_mock, fake_redis_client, label_config):
    """An issue with orcest:needs-human is classified as SKIP_LABELED."""
    issue_gh_mock.return_value = [
        _make_issue_data(
            number=6,
            labels=[{"name": "orcest:ready"}, {"name": "orcest:needs-human"}],
        ),
    ]

    results = discover_actionable_issues(
        repo=REPO,
        token=TOKEN,
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == IssueAction.SKIP_LABELED


# ---------------------------------------------------------------------------
# Lock checks
# ---------------------------------------------------------------------------


def test_skip_locked_issue(issue_gh_mock, fake_redis_client, label_config):
    """An issue with a Redis lock present is SKIP_LOCKED."""
    issue_gh_mock.return_value = [
        _make_issue_data(number=42, labels=[]),
    ]
    # Simulate a worker holding the lock
    fake_redis_client.client.set(make_issue_lock_key(REPO, 42), "worker-1")

    results = discover_actionable_issues(
        repo=REPO,
        token=TOKEN,
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == IssueAction.SKIP_LOCKED
    assert results[0].number == 42


# ---------------------------------------------------------------------------
# Pending task checks
# ---------------------------------------------------------------------------


def test_skip_issue_with_pending_task(issue_gh_mock, fake_redis_client, label_config):
    """An issue that already has a pending task in the queue is SKIP_QUEUED."""
    issue_gh_mock.return_value = [
        _make_issue_data(number=99, labels=[]),
    ]
    pending_key = make_pending_task_key(REPO, "issue", 99)
    fake_redis_client.client.set(pending_key, "task-abc-123")

    results = discover_actionable_issues(
        repo=REPO,
        token=TOKEN,
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == IssueAction.SKIP_QUEUED
    assert results[0].number == 99


# ---------------------------------------------------------------------------
# Attempt budget
# ---------------------------------------------------------------------------


def test_skip_active_issue(issue_gh_mock, fake_redis_client, label_config):
    """An issue with attempts > 0 (but below max) is SKIP_ACTIVE."""
    issue_gh_mock.return_value = [
        _make_issue_data(number=7, labels=[]),
    ]
    increment_attempts(fake_redis_client, 7)  # count = 1

    results = discover_actionable_issues(
        repo=REPO,
        token=TOKEN,
        redis=fake_redis_client,
        label_config=label_config,
        max_attempts=3,
    )

    assert len(results) == 1
    assert results[0].action == IssueAction.SKIP_ACTIVE
    assert results[0].number == 7


def test_skip_max_attempts_reached(issue_gh_mock, fake_redis_client, label_config):
    """An issue that has exhausted its attempt budget is SKIP_MAX_ATTEMPTS."""
    issue_gh_mock.return_value = [
        _make_issue_data(number=8, labels=[]),
    ]
    for _ in range(3):
        increment_attempts(fake_redis_client, 8)  # count = 3 (== max_attempts)

    results = discover_actionable_issues(
        repo=REPO,
        token=TOKEN,
        redis=fake_redis_client,
        label_config=label_config,
        max_attempts=3,
    )

    assert len(results) == 1
    assert results[0].action == IssueAction.SKIP_MAX_ATTEMPTS
    assert results[0].number == 8


def test_skip_max_attempts_exceeded(issue_gh_mock, fake_redis_client, label_config):
    """An issue with attempts > max is also SKIP_MAX_ATTEMPTS."""
    issue_gh_mock.return_value = [
        _make_issue_data(number=9, labels=[]),
    ]
    for _ in range(5):
        increment_attempts(fake_redis_client, 9)

    results = discover_actionable_issues(
        repo=REPO,
        token=TOKEN,
        redis=fake_redis_client,
        label_config=label_config,
        max_attempts=3,
    )

    assert len(results) == 1
    assert results[0].action == IssueAction.SKIP_MAX_ATTEMPTS


# ---------------------------------------------------------------------------
# Filter cascade ordering
# ---------------------------------------------------------------------------


def test_terminal_label_checked_before_lock(issue_gh_mock, fake_redis_client, label_config):
    """Terminal label check happens before lock check (SKIP_LABELED, not SKIP_LOCKED)."""
    issue_gh_mock.return_value = [
        _make_issue_data(
            number=11,
            labels=[{"name": "orcest:blocked"}],
        ),
    ]
    # Also set a lock — the label check should short-circuit first
    fake_redis_client.client.set(make_issue_lock_key(REPO, 11), "worker-2")

    results = discover_actionable_issues(
        repo=REPO,
        token=TOKEN,
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert results[0].action == IssueAction.SKIP_LABELED


def test_lock_checked_before_pending(issue_gh_mock, fake_redis_client, label_config):
    """Lock check happens before pending-task check (SKIP_LOCKED, not SKIP_QUEUED)."""
    issue_gh_mock.return_value = [
        _make_issue_data(number=12, labels=[]),
    ]
    fake_redis_client.client.set(make_issue_lock_key(REPO, 12), "worker-3")
    pending_key = make_pending_task_key(REPO, "issue", 12)
    fake_redis_client.client.set(pending_key, "task-xyz")

    results = discover_actionable_issues(
        repo=REPO,
        token=TOKEN,
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert results[0].action == IssueAction.SKIP_LOCKED


# ---------------------------------------------------------------------------
# IssueState fields
# ---------------------------------------------------------------------------


def test_issue_state_fields_populated(issue_gh_mock, fake_redis_client, label_config):
    """IssueState is populated with number, title, body, and labels."""
    issue_gh_mock.return_value = [
        _make_issue_data(
            number=50,
            title="Add dark mode",
            body="Users want dark mode",
            labels=[{"name": "orcest:ready"}, {"name": "enhancement"}],
        ),
    ]

    results = discover_actionable_issues(
        repo=REPO,
        token=TOKEN,
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    state = results[0]
    assert state.number == 50
    assert state.title == "Add dark mode"
    assert state.body == "Users want dark mode"
    assert "orcest:ready" in state.labels
    assert "enhancement" in state.labels
    assert state.action == IssueAction.ENQUEUE_IMPLEMENT


def test_issue_body_none_defaults_to_empty_string(
    issue_gh_mock, fake_redis_client, label_config
):
    """Issues with null body are handled gracefully (body defaults to '')."""
    issue_gh_mock.return_value = [
        {
            "number": 51,
            "title": "No body issue",
            "body": None,
            "labels": [],
        },
    ]

    results = discover_actionable_issues(
        repo=REPO,
        token=TOKEN,
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].body == ""


# ---------------------------------------------------------------------------
# Attempt helper functions
# ---------------------------------------------------------------------------


def test_get_attempt_count_zero_when_missing(fake_redis_client):
    """get_attempt_count returns 0 for an issue with no recorded attempts."""
    assert get_attempt_count(fake_redis_client, 999) == 0


def test_increment_and_get_attempt_count(fake_redis_client):
    """increment_attempts increments the count; get_attempt_count reflects it."""
    assert increment_attempts(fake_redis_client, 100) == 1
    assert increment_attempts(fake_redis_client, 100) == 2
    assert get_attempt_count(fake_redis_client, 100) == 2


def test_clear_attempts(fake_redis_client):
    """clear_attempts resets the counter to 0."""
    increment_attempts(fake_redis_client, 200)
    increment_attempts(fake_redis_client, 200)
    assert get_attempt_count(fake_redis_client, 200) == 2

    clear_attempts(fake_redis_client, 200)
    assert get_attempt_count(fake_redis_client, 200) == 0
