"""Tests for orcest.orchestrator.pr_ops.discover_actionable_prs().

Exercises the filter cascade: labels -> locks -> CI status -> reviews.
Each test uses the gh_mock fixture (all gh.* functions mocked) and
fake_redis_client (fakeredis-backed RedisClient).
"""

from orcest.orchestrator.pr_ops import PRAction, discover_actionable_prs
from orcest.shared.coordination import make_pr_lock_key


def _make_pr_data(
    number: int = 42,
    title: str = "Fix the widget",
    branch: str = "fix/widget",
    labels: list[dict] | None = None,
    review_decision: str = "",
) -> dict:
    """Build a PR dict matching the shape returned by gh.list_open_prs."""
    return {
        "number": number,
        "title": title,
        "headRefName": branch,
        "labels": labels or [],
        "reviewDecision": review_decision,
    }


def test_skip_labeled_pr(gh_mock, fake_redis_client, label_config):
    """A PR carrying an orcest label is classified as SKIP_LABELED."""
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(
            number=10,
            labels=[{"name": "orcest:queued"}],
        ),
    ]

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == PRAction.SKIP_LABELED
    assert results[0].number == 10
    # get_ci_status should never be called -- label check short-circuits
    gh_mock.get_ci_status.assert_not_called()


def test_skip_locked_pr(gh_mock, fake_redis_client, label_config):
    """A PR with no orcest labels but a Redis lock is SKIP_LOCKED."""
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=42, labels=[]),
    ]
    # Set the lock that discover_actionable_prs checks via redis.client.exists
    fake_redis_client.client.set(make_pr_lock_key(42), "worker-7")

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == PRAction.SKIP_LOCKED
    # CI should not be fetched for a locked PR
    gh_mock.get_ci_status.assert_not_called()


def test_enqueue_ci_failure(gh_mock, fake_redis_client, label_config):
    """A clean PR with failing CI checks gets ENQUEUE_FIX with ci_failures."""
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=99, labels=[]),
    ]
    gh_mock.get_ci_status.return_value = [
        {"name": "ruff", "conclusion": "failure", "detailsUrl": "https://ci/1"},
        {"name": "pytest", "conclusion": "success"},
    ]

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    pr = results[0]
    assert pr.action == PRAction.ENQUEUE_FIX
    assert pr.number == 99
    assert len(pr.ci_failures) == 1
    assert pr.ci_failures[0]["name"] == "ruff"
    assert pr.ci_failures[0]["conclusion"] == "failure"


def test_skip_green_pr(gh_mock, fake_redis_client, label_config):
    """A PR with all-green CI and no CHANGES_REQUESTED review is SKIP_GREEN."""
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=7, labels=[], review_decision="APPROVED"),
    ]
    gh_mock.get_ci_status.return_value = [
        {"name": "ruff", "conclusion": "success"},
        {"name": "pytest", "conclusion": "success"},
    ]

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == PRAction.SKIP_GREEN
    assert results[0].ci_failures == []


def test_ci_fetch_error_skips_pr(gh_mock, fake_redis_client, label_config):
    """If get_ci_status raises, the PR is excluded (not a crash)."""
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=55, labels=[]),
    ]
    gh_mock.get_ci_status.side_effect = RuntimeError("API 500")

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    # PR should be silently skipped -- not present in results at all
    assert len(results) == 0


def test_empty_pr_list(gh_mock, fake_redis_client, label_config):
    """When there are no open PRs, the result list is empty."""
    gh_mock.list_open_prs.return_value = []

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert results == []
    gh_mock.get_ci_status.assert_not_called()
