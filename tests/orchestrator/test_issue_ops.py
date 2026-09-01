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
    set_usage_exhausted_cooldown,
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
    blocked_by: list[dict] | None = None,
) -> dict:
    """Build an issue dict matching the shape returned by gh.list_labeled_issues."""
    return {
        "number": number,
        "title": title,
        "body": body,
        "labels": labels or [],
        "blocked_by": blocked_by or [],
    }


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_happy_path_enqueue_implement(issue_gh_mock, fake_redis_client, label_config):
    """An issue with orcest:ready and no blockers is ENQUEUE_IMPLEMENT."""
    issue_gh_mock.return_value = [
        _make_issue_data(number=1, labels=[{"name": label_config.ready}]),
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


def test_skip_when_delivery_barrier_present(issue_gh_mock, fake_redis_client, label_config):
    """A nonterminal verification job blocks rediscovery independent of pending TTL."""
    from orcest.orchestrator.issue_publication import make_issue_dispatch_barrier_key

    issue_number = 3
    issue_gh_mock.return_value = [
        _make_issue_data(number=issue_number, labels=[{"name": label_config.ready}]),
    ]
    fake_redis_client.set_value(make_issue_dispatch_barrier_key(REPO, issue_number), "1|1")

    results = discover_actionable_issues(
        repo=REPO,
        token=TOKEN,
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == IssueAction.SKIP_VERIFYING


def test_skip_when_delivery_barrier_present_and_verifier_disabled(
    issue_gh_mock, fake_redis_client, label_config, caplog
):
    """A dispatch barrier still blocks discovery when the verifier is disabled,
    but a warning is logged so the stuck state is not silent."""
    from orcest.orchestrator.issue_publication import make_issue_dispatch_barrier_key
    from orcest.shared.config import IssueDeliveryVerifierConfig

    issue_number = 4
    issue_gh_mock.return_value = [
        _make_issue_data(number=issue_number, labels=[{"name": label_config.ready}]),
    ]
    fake_redis_client.set_value(make_issue_dispatch_barrier_key(REPO, issue_number), "1|1")

    with caplog.at_level("WARNING"):
        results = discover_actionable_issues(
            repo=REPO,
            token=TOKEN,
            redis=fake_redis_client,
            label_config=label_config,
            issue_delivery_verifier=IssueDeliveryVerifierConfig(enabled=False),
        )

    assert len(results) == 1
    assert results[0].action == IssueAction.SKIP_VERIFYING
    assert any(
        "issue_delivery_verifier.enabled is false" in record.message for record in caplog.records
    )


def test_skip_usage_cooldown_when_active(issue_gh_mock, fake_redis_client, label_config):
    """An issue with an active USAGE_EXHAUSTED cooldown is not re-enqueued."""
    issue_number = 2
    issue_gh_mock.return_value = [
        _make_issue_data(number=issue_number, labels=[{"name": label_config.ready}]),
    ]
    set_usage_exhausted_cooldown(fake_redis_client, REPO, issue_number, ttl_seconds=300)

    results = discover_actionable_issues(
        repo=REPO,
        token=TOKEN,
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == IssueAction.SKIP_USAGE_COOLDOWN


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
# Terminal label
# ---------------------------------------------------------------------------


def test_skip_needs_human_label(issue_gh_mock, fake_redis_client, label_config):
    """An issue with orcest:needs-human is classified as SKIP_LABELED."""
    issue_gh_mock.return_value = [
        _make_issue_data(
            number=6,
            labels=[{"name": label_config.ready}, {"name": label_config.needs_human}],
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
    fake_redis_client.set_ex(make_issue_lock_key(REPO, 42), "worker-1", 86400)

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
    fake_redis_client.set_ex(pending_key, "task-abc-123", 86400)

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


def test_skip_active_issue_with_live_pending_task(issue_gh_mock, fake_redis_client, label_config):
    """An issue with attempts > 0 and a live pending task is SKIP_ACTIVE."""
    issue_gh_mock.return_value = [
        _make_issue_data(number=7, labels=[]),
    ]
    increment_attempts(fake_redis_client, REPO, 7)  # count = 1
    pending_key = make_pending_task_key(REPO, "issue", 7)
    fake_redis_client.set_ex(pending_key, "task-active", 86400)

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


def test_orphaned_active_issue_clears_attempts_and_enqueues(
    issue_gh_mock, fake_redis_client, label_config
):
    """An issue with attempts > 0 but no pending task is retried normally."""
    issue_gh_mock.return_value = [
        _make_issue_data(number=14, labels=[]),
    ]
    increment_attempts(fake_redis_client, REPO, 14)  # count = 1

    results = discover_actionable_issues(
        repo=REPO,
        token=TOKEN,
        redis=fake_redis_client,
        label_config=label_config,
        max_attempts=3,
    )

    assert len(results) == 1
    assert results[0].action == IssueAction.ENQUEUE_IMPLEMENT
    assert results[0].number == 14
    assert get_attempt_count(fake_redis_client, REPO, 14) == 0


def _seed_ineffective_generations(redis, issue_number: int, generations: int) -> None:
    """Record durable INEFFECTIVE retry history without a live pending marker."""
    from orcest.orchestrator.issue_publication import (
        make_issue_generation_key,
        make_issue_retry_record_key,
    )

    redis.set_value(make_issue_generation_key(REPO, issue_number), str(generations))
    for gen in range(1, generations + 1):
        redis.hset_mapping(
            make_issue_retry_record_key(REPO, issue_number, gen),
            {
                "reason": "ineffective_delivery",
                "generation": str(gen),
                "task_id": f"task-{gen}",
                "cooldown_until": "0",
                "created_at": "0",
            },
        )


def test_ineffective_history_preserves_attempt_budget(
    issue_gh_mock, fake_redis_client, label_config
):
    """INEFFECTIVE history is not an orphaned counter; keep the attempt budget."""
    issue_gh_mock.return_value = [
        _make_issue_data(number=16, labels=[]),
    ]
    increment_attempts(fake_redis_client, REPO, 16)
    increment_attempts(fake_redis_client, REPO, 16)
    _seed_ineffective_generations(fake_redis_client, 16, generations=1)

    results = discover_actionable_issues(
        repo=REPO,
        token=TOKEN,
        redis=fake_redis_client,
        label_config=label_config,
        max_attempts=3,
    )

    assert len(results) == 1
    assert results[0].action == IssueAction.ENQUEUE_IMPLEMENT
    assert get_attempt_count(fake_redis_client, REPO, 16) == 2


def test_prior_generation_ineffective_history_preserves_attempts(
    issue_gh_mock, fake_redis_client, label_config
):
    """Retry records on older generations still prevent an attempt-budget reset."""
    from orcest.orchestrator.issue_publication import make_issue_generation_key

    issue_gh_mock.return_value = [
        _make_issue_data(number=17, labels=[]),
    ]
    increment_attempts(fake_redis_client, REPO, 17)
    _seed_ineffective_generations(fake_redis_client, 17, generations=1)
    # A later reservation that crashed after incrementing generation.
    fake_redis_client.set_value(make_issue_generation_key(REPO, 17), "2")

    results = discover_actionable_issues(
        repo=REPO,
        token=TOKEN,
        redis=fake_redis_client,
        label_config=label_config,
        max_attempts=3,
    )

    assert results[0].action == IssueAction.ENQUEUE_IMPLEMENT
    assert get_attempt_count(fake_redis_client, REPO, 17) == 1


def test_ineffective_generations_exhaust_max_attempts_without_attempt_hash(
    issue_gh_mock, fake_redis_client, label_config
):
    """Worker/admission clearing the attempts hash must not refresh max_attempts."""
    issue_gh_mock.return_value = [
        _make_issue_data(number=18, labels=[]),
    ]
    _seed_ineffective_generations(fake_redis_client, 18, generations=3)

    results = discover_actionable_issues(
        repo=REPO,
        token=TOKEN,
        redis=fake_redis_client,
        label_config=label_config,
        max_attempts=3,
    )

    assert len(results) == 1
    assert results[0].action == IssueAction.SKIP_MAX_ATTEMPTS
    assert get_attempt_count(fake_redis_client, REPO, 18) == 0


def test_issue_without_attempts_or_pending_task_still_enqueues(
    issue_gh_mock, fake_redis_client, label_config
):
    """An issue with attempts == 0 and no pending task keeps the normal path."""
    issue_gh_mock.return_value = [
        _make_issue_data(number=15, labels=[]),
    ]

    results = discover_actionable_issues(
        repo=REPO,
        token=TOKEN,
        redis=fake_redis_client,
        label_config=label_config,
        max_attempts=3,
    )

    assert len(results) == 1
    assert results[0].action == IssueAction.ENQUEUE_IMPLEMENT
    assert results[0].number == 15
    assert get_attempt_count(fake_redis_client, REPO, 15) == 0


def test_skip_max_attempts_reached(issue_gh_mock, fake_redis_client, label_config):
    """An issue that has exhausted its attempt budget is SKIP_MAX_ATTEMPTS."""
    issue_gh_mock.return_value = [
        _make_issue_data(number=8, labels=[]),
    ]
    for _ in range(3):
        increment_attempts(fake_redis_client, REPO, 8)  # count = 3 (== max_attempts)
    fake_redis_client.set_ex(make_pending_task_key(REPO, "issue", 8), "task-8", 3600)

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
        increment_attempts(fake_redis_client, REPO, 9)
    fake_redis_client.set_ex(make_pending_task_key(REPO, "issue", 9), "task-9", 3600)

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
            labels=[{"name": label_config.needs_human}],
        ),
    ]
    # Also set a lock — the label check should short-circuit first
    fake_redis_client.set_ex(make_issue_lock_key(REPO, 11), "worker-2", 86400)

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
    fake_redis_client.set_ex(make_issue_lock_key(REPO, 12), "worker-3", 86400)
    pending_key = make_pending_task_key(REPO, "issue", 12)
    fake_redis_client.set_ex(pending_key, "task-xyz", 86400)

    results = discover_actionable_issues(
        repo=REPO,
        token=TOKEN,
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert results[0].action == IssueAction.SKIP_LOCKED


def test_attempts_checked_before_pending(issue_gh_mock, fake_redis_client, label_config):
    """An active attempt with a live pending task is SKIP_ACTIVE, not SKIP_QUEUED."""
    issue_gh_mock.return_value = [
        _make_issue_data(number=13, labels=[]),
    ]
    pending_key = make_pending_task_key(REPO, "issue", 13)
    fake_redis_client.set_ex(pending_key, "task-pending", 86400)
    increment_attempts(fake_redis_client, REPO, 13)  # count = 1 — would cause SKIP_ACTIVE

    results = discover_actionable_issues(
        repo=REPO,
        token=TOKEN,
        redis=fake_redis_client,
        label_config=label_config,
        max_attempts=3,
    )

    assert results[0].action == IssueAction.SKIP_ACTIVE


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
            labels=[{"name": label_config.ready}, {"name": "enhancement"}],
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
    assert label_config.ready in state.labels
    assert "enhancement" in state.labels
    assert state.action == IssueAction.ENQUEUE_IMPLEMENT


def test_issue_body_none_defaults_to_empty_string(issue_gh_mock, fake_redis_client, label_config):
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
# Dependency resolution
# ---------------------------------------------------------------------------


@pytest.fixture
def issue_state_mock(mocker):
    """Patch gh.get_issue_state (called via issue_deps)."""
    return mocker.patch("orcest.orchestrator.gh.get_issue_state")


def test_skip_dependency_when_blocker_is_open(
    issue_gh_mock, issue_state_mock, fake_redis_client, label_config
):
    issue_gh_mock.return_value = [
        _make_issue_data(number=200, body="Blocked by #101", labels=[]),
    ]
    issue_state_mock.return_value = "open"

    results = discover_actionable_issues(
        repo=REPO,
        token=TOKEN,
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == IssueAction.SKIP_DEPENDENCY
    assert results[0].open_blockers == ["#101"]
    issue_state_mock.assert_called_once_with(REPO, 101, TOKEN)


def test_enqueue_when_blocker_is_closed(
    issue_gh_mock, issue_state_mock, fake_redis_client, label_config
):
    issue_gh_mock.return_value = [
        _make_issue_data(number=201, body="Blocked by #50", labels=[]),
    ]
    issue_state_mock.return_value = "closed"

    results = discover_actionable_issues(
        repo=REPO,
        token=TOKEN,
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == IssueAction.ENQUEUE_IMPLEMENT


def test_enqueue_when_blocker_is_missing(
    issue_gh_mock, issue_state_mock, fake_redis_client, label_config
):
    """A deleted or wrong-number blocker reference should not defer the issue."""
    issue_gh_mock.return_value = [
        _make_issue_data(number=205, body="Blocked by #99999", labels=[]),
    ]
    issue_state_mock.return_value = "missing"

    results = discover_actionable_issues(
        repo=REPO,
        token=TOKEN,
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == IssueAction.ENQUEUE_IMPLEMENT


def test_skip_dependency_when_blocker_lookup_fails_transiently(
    issue_gh_mock, issue_state_mock, fake_redis_client, label_config
):
    """Transient gh failures must fail-safe to deferral, not enqueue."""
    import orcest.orchestrator.gh as gh_module

    issue_gh_mock.return_value = [
        _make_issue_data(number=206, body="Blocked by #100", labels=[]),
    ]
    issue_state_mock.side_effect = gh_module.GhRateLimitError("rate limited")

    results = discover_actionable_issues(
        repo=REPO,
        token=TOKEN,
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == IssueAction.SKIP_DEPENDENCY
    assert results[0].open_blockers == ["#100"]


def test_closes_directive_does_not_block(
    issue_gh_mock, issue_state_mock, fake_redis_client, label_config
):
    """`Closes #N` is an output, not a blocker — issue must still enqueue."""
    issue_gh_mock.return_value = [
        _make_issue_data(number=202, body="Closes #999", labels=[]),
    ]

    results = discover_actionable_issues(
        repo=REPO,
        token=TOKEN,
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == IssueAction.ENQUEUE_IMPLEMENT
    issue_state_mock.assert_not_called()


def test_shared_blocker_cache_dedupes_gh_calls(
    issue_gh_mock, issue_state_mock, fake_redis_client, label_config
):
    """Two dependents on the same blocker only cost one gh lookup."""
    issue_gh_mock.return_value = [
        _make_issue_data(number=210, body="Blocked by #1", labels=[]),
        _make_issue_data(number=211, body="Depends on #1", labels=[]),
    ]
    issue_state_mock.return_value = "open"

    results = discover_actionable_issues(
        repo=REPO,
        token=TOKEN,
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 2
    assert all(r.action == IssueAction.SKIP_DEPENDENCY for r in results)
    assert issue_state_mock.call_count == 1


def test_mixed_blockers_open_and_closed(
    issue_gh_mock, issue_state_mock, fake_redis_client, label_config
):
    issue_gh_mock.return_value = [
        _make_issue_data(number=220, body="Blocked by #1 and depends on #2", labels=[]),
    ]
    issue_state_mock.side_effect = lambda repo, number, token: "closed" if number == 1 else "open"

    results = discover_actionable_issues(
        repo=REPO,
        token=TOKEN,
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == IssueAction.SKIP_DEPENDENCY
    assert results[0].open_blockers == ["#2"]


def test_dependency_check_runs_after_cheap_filters(
    issue_gh_mock, issue_state_mock, fake_redis_client, label_config
):
    """A locked issue must not incur a gh.get_issue_state call."""
    issue_gh_mock.return_value = [
        _make_issue_data(number=230, body="Blocked by #1", labels=[]),
    ]
    fake_redis_client.set_ex(make_issue_lock_key(REPO, 230), "worker-1", 86400)

    results = discover_actionable_issues(
        repo=REPO,
        token=TOKEN,
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert results[0].action == IssueAction.SKIP_LOCKED
    issue_state_mock.assert_not_called()


# ---------------------------------------------------------------------------
# GitHub-native issue dependencies (blocked_by from list_labeled_issues)
# ---------------------------------------------------------------------------


def test_skip_dependency_when_native_blocker_is_open(
    issue_gh_mock, issue_state_mock, fake_redis_client, label_config
):
    """A native blocked-by relationship defers the issue with zero extra gh calls."""
    issue_gh_mock.return_value = [
        _make_issue_data(
            number=300,
            blocked_by=[{"number": 3, "state": "OPEN", "repo": REPO}],
        ),
    ]

    results = discover_actionable_issues(
        repo=REPO,
        token=TOKEN,
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == IssueAction.SKIP_DEPENDENCY
    assert results[0].open_blockers == ["#3"]
    issue_state_mock.assert_not_called()


def test_enqueue_when_native_blocker_is_closed(
    issue_gh_mock, issue_state_mock, fake_redis_client, label_config
):
    issue_gh_mock.return_value = [
        _make_issue_data(
            number=301,
            blocked_by=[{"number": 3, "state": "CLOSED", "repo": REPO}],
        ),
    ]

    results = discover_actionable_issues(
        repo=REPO,
        token=TOKEN,
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == IssueAction.ENQUEUE_IMPLEMENT
    issue_state_mock.assert_not_called()


def test_skip_dependency_when_cross_repo_native_blocker_is_open(
    issue_gh_mock, issue_state_mock, fake_redis_client, label_config
):
    issue_gh_mock.return_value = [
        _make_issue_data(
            number=302,
            blocked_by=[{"number": 8, "state": "OPEN", "repo": "other-org/other-repo"}],
        ),
    ]

    results = discover_actionable_issues(
        repo=REPO,
        token=TOKEN,
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert results[0].action == IssueAction.SKIP_DEPENDENCY
    assert results[0].open_blockers == ["other-org/other-repo#8"]


def test_native_open_blocker_short_circuits_body_resolution(
    issue_gh_mock, issue_state_mock, fake_redis_client, label_config
):
    """When a native blocker already defers the issue, body refs are not
    resolved -- no gh calls are spent on an issue we know is deferred."""
    issue_gh_mock.return_value = [
        _make_issue_data(
            number=303,
            body="Blocked by #77",
            blocked_by=[{"number": 3, "state": "OPEN", "repo": REPO}],
        ),
    ]

    results = discover_actionable_issues(
        repo=REPO,
        token=TOKEN,
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert results[0].action == IssueAction.SKIP_DEPENDENCY
    assert results[0].open_blockers == ["#3"]
    issue_state_mock.assert_not_called()


def test_native_blocker_state_seeds_body_resolution_cache(
    issue_gh_mock, issue_state_mock, fake_redis_client, label_config
):
    """A body ref whose state is already known from native data costs no gh call
    and does not defer when that blocker is closed."""
    issue_gh_mock.return_value = [
        _make_issue_data(
            number=304,
            body="Blocked by #9",
            blocked_by=[{"number": 9, "state": "CLOSED", "repo": REPO}],
        ),
    ]

    results = discover_actionable_issues(
        repo=REPO,
        token=TOKEN,
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert results[0].action == IssueAction.ENQUEUE_IMPLEMENT
    issue_state_mock.assert_not_called()


def test_repo_less_native_blocker_does_not_seed_cache(
    issue_gh_mock, issue_state_mock, fake_redis_client, label_config
):
    """A native blocker with no repo attribution must not be treated as
    same-repo when seeding the body-resolution cache. Here the closed
    repo-less blocker shares issue number 12 with an open same-repo body
    ref -- the issue must defer via a real gh lookup, not enqueue off a
    poisoned cache entry."""
    issue_gh_mock.return_value = [
        _make_issue_data(
            number=307,
            body="Blocked by #12",
            blocked_by=[{"number": 12, "state": "CLOSED", "repo": None}],
        ),
    ]
    issue_state_mock.return_value = "open"

    results = discover_actionable_issues(
        repo=REPO,
        token=TOKEN,
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert results[0].action == IssueAction.SKIP_DEPENDENCY
    assert results[0].open_blockers == ["#12"]
    issue_state_mock.assert_called_once_with(REPO, 12, TOKEN)


def test_cross_repo_closed_native_blocker_does_not_seed_cache(
    issue_gh_mock, issue_state_mock, fake_redis_client, label_config
):
    """The cache is keyed by bare same-repo issue numbers, so a closed
    cross-repo native blocker whose number collides with an open same-repo
    body ref must not satisfy that ref."""
    issue_gh_mock.return_value = [
        _make_issue_data(
            number=308,
            body="Blocked by #9",
            blocked_by=[{"number": 9, "state": "CLOSED", "repo": "other-org/other-repo"}],
        ),
    ]
    issue_state_mock.return_value = "open"

    results = discover_actionable_issues(
        repo=REPO,
        token=TOKEN,
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert results[0].action == IssueAction.SKIP_DEPENDENCY
    assert results[0].open_blockers == ["#9"]
    issue_state_mock.assert_called_once_with(REPO, 9, TOKEN)


def test_native_and_body_same_blocker_reported_once(
    issue_gh_mock, issue_state_mock, fake_redis_client, label_config
):
    """A blocker declared both natively and in the body appears once."""
    issue_gh_mock.return_value = [
        _make_issue_data(
            number=305,
            body="Blocked by #5",
            blocked_by=[{"number": 5, "state": "OPEN", "repo": REPO}],
        ),
    ]

    results = discover_actionable_issues(
        repo=REPO,
        token=TOKEN,
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert results[0].action == IssueAction.SKIP_DEPENDENCY
    assert results[0].open_blockers == ["#5"]
    issue_state_mock.assert_not_called()


def test_issue_without_blocked_by_key_still_works(
    issue_gh_mock, issue_state_mock, fake_redis_client, label_config
):
    """Issue dicts lacking the blocked_by key (e.g. from an older caller)
    behave as having no native dependencies."""
    issue = _make_issue_data(number=306)
    del issue["blocked_by"]
    issue_gh_mock.return_value = [issue]

    results = discover_actionable_issues(
        repo=REPO,
        token=TOKEN,
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert results[0].action == IssueAction.ENQUEUE_IMPLEMENT


# ---------------------------------------------------------------------------
# Attempt helper functions
# ---------------------------------------------------------------------------


def test_get_attempt_count_zero_when_missing(fake_redis_client):
    """get_attempt_count returns 0 for an issue with no recorded attempts."""
    assert get_attempt_count(fake_redis_client, REPO, 999) == 0


def test_increment_and_get_attempt_count(fake_redis_client):
    """increment_attempts increments the count; get_attempt_count reflects it."""
    assert increment_attempts(fake_redis_client, REPO, 100) == 1
    assert increment_attempts(fake_redis_client, REPO, 100) == 2
    assert get_attempt_count(fake_redis_client, REPO, 100) == 2


def test_clear_attempts(fake_redis_client):
    """clear_attempts resets the counter to 0."""
    increment_attempts(fake_redis_client, REPO, 200)
    increment_attempts(fake_redis_client, REPO, 200)
    assert get_attempt_count(fake_redis_client, REPO, 200) == 2

    clear_attempts(fake_redis_client, REPO, 200)
    assert get_attempt_count(fake_redis_client, REPO, 200) == 0
