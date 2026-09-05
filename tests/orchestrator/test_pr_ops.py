"""Tests for orcest.orchestrator.pr_ops.discover_actionable_prs().

Exercises the filter cascade: labels -> drafts -> locks -> attempts -> CI status -> reviews.
Each test uses the gh_mock fixture (all gh.* functions mocked) and
fake_redis_client (fakeredis-backed RedisClient).
"""

from datetime import datetime, timedelta, timezone

from orcest.orchestrator.gh import GhCliError, GhStaleSnapshotError, PRReviewSnapshot
from orcest.orchestrator.pr_ops import (
    PRAction,
    _check_stale_pending,
    _get_claude_review_run_id,
    _make_pending_check_first_seen_key,
    clear_attempts,
    clear_exhausted_notified,
    clear_review_retrigger,
    clear_total_attempts,
    discover_actionable_prs,
    get_attempt_count,
    get_exhausted_notified,
    get_review_retrigger_sha,
    get_total_attempt_count,
    get_transient_attempt_count,
    increment_attempts,
    increment_total_attempts,
    increment_transient_attempts,
    record_self_cancelled_stale_runs,
    set_exhausted_notified,
    set_review_retrigger_sha,
    set_usage_exhausted_cooldown,
)
from orcest.shared.coordination import (
    get_backoff_step,
    get_pending_task,
    get_transient_failure_count,
    increment_transient_failure_count,
    make_pending_task_key,
    make_pr_lock_key,
    set_backoff_cooldown,
    set_pending_task,
)
from orcest.workflow_contract.v1.publication import render_run_marker

REPO = "test-org/test-repo"


def _make_pr_data(
    number: int = 42,
    title: str = "Fix the widget",
    branch: str = "fix/widget",
    labels: list[dict] | None = None,
    review_decision: str = "",
    head_sha: str = "abc123",
    is_draft: bool = False,
    mergeable: str = "MERGEABLE",
    base_branch: str = "main",
    merge_state_status: str = "CLEAN",
    body: str = "",
) -> dict:
    """Build a PR dict matching the shape returned by gh.list_open_prs."""
    return {
        "number": number,
        "title": title,
        "headRefName": branch,
        "baseRefName": base_branch,
        "headRefOid": head_sha,
        "isDraft": is_draft,
        "labels": labels or [],
        "reviewDecision": review_decision,
        "mergeable": mergeable,
        "mergeStateStatus": merge_state_status,
        "body": body,
    }


def test_skip_pr_reserved_by_v1_run_marker(gh_mock, fake_redis_client, label_config):
    marker = render_run_marker(
        run_id="11111111-1111-4111-8111-111111111111",
        publication_id="22222222-2222-4222-8222-222222222222",
    )
    gh_mock.list_open_prs.return_value = [_make_pr_data(number=10, body=marker)]

    results = discover_actionable_prs(
        repo=REPO,
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert results[0].action == PRAction.SKIP_V1_OWNED
    gh_mock.get_ci_status.assert_not_called()


def test_skip_pr_reserved_by_v1_association(gh_mock, fake_redis_client, label_config):
    gh_mock.list_open_prs.return_value = [_make_pr_data(number=10, branch="orcest/run/owned")]
    seen: list[dict[str, str | None]] = []

    def is_v1_owned(**identities: str | None) -> bool:
        seen.append(identities)
        return True

    results = discover_actionable_prs(
        repo=REPO,
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
        legacy_exclusion_predicate=is_v1_owned,
    )

    assert seen == [
        {
            "change_request_external_id": "10",
            "deterministic_ref": "refs/heads/orcest/run/owned",
        }
    ]
    assert results[0].action == PRAction.SKIP_V1_OWNED
    gh_mock.get_ci_status.assert_not_called()


def test_v1_association_lookup_failure_excludes_for_current_poll(
    gh_mock, fake_redis_client, label_config
):
    gh_mock.list_open_prs.return_value = [_make_pr_data(number=10)]

    def unavailable(**_identities: str | None) -> bool:
        raise OSError("workflow store unavailable")

    results = discover_actionable_prs(
        repo=REPO,
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
        legacy_exclusion_predicate=unavailable,
    )

    assert results[0].action == PRAction.SKIP_V1_LOOKUP_UNAVAILABLE
    gh_mock.get_ci_status.assert_not_called()


def test_skip_labeled_pr(gh_mock, fake_redis_client, label_config):
    """A PR carrying a terminal orcest label is classified as SKIP_LABELED."""
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(
            number=10,
            labels=[{"name": "orcest:needs-human"}],
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
    # Set the lock that discover_actionable_prs checks via redis.exists
    fake_redis_client.set_ex(make_pr_lock_key("test-org/test-repo", 42), "worker-7", 86400)

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


def test_skip_draft_pr(gh_mock, fake_redis_client, label_config):
    """A draft PR is classified as SKIP_DRAFT, CI not fetched."""
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=15, labels=[], is_draft=True),
    ]

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == PRAction.SKIP_DRAFT
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
    assert pr.review_threads == []


def test_skip_green_pr(gh_mock, fake_redis_client, label_config):
    """A PR with all-green CI, no review, and no unresolved threads is SKIP_GREEN."""
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=7, labels=[], review_decision=""),
    ]
    gh_mock.get_ci_status.return_value = [
        {"name": "ruff", "conclusion": "success"},
        {"name": "pytest", "conclusion": "success"},
    ]
    gh_mock.get_unresolved_review_threads.return_value = []

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


# ---------------------------------------------------------------------------
# Review thread / follow-up tests
# ---------------------------------------------------------------------------


def test_enqueue_review_feedback(gh_mock, fake_redis_client, label_config):
    """CHANGES_REQUESTED + CI green -> ENQUEUE_FIX with review_threads populated."""
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=60, labels=[], review_decision="CHANGES_REQUESTED"),
    ]
    gh_mock.get_ci_status.return_value = [
        {"name": "tests", "conclusion": "success"},
    ]
    threads = [
        {
            "id": "PRRT_1",
            "path": "src/foo.py",
            "line": 42,
            "comments": [{"author": "reviewer1", "body": "Fix this"}],
        },
    ]
    gh_mock.get_unresolved_review_threads.return_value = threads

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    pr = results[0]
    assert pr.action == PRAction.ENQUEUE_FIX
    assert pr.number == 60
    gh_mock.get_unresolved_review_threads.assert_called_once_with(
        "test-org/test-repo",
        60,
        "fake-token",
        expected_head_sha="abc123",
    )
    assert pr.review_threads == threads


def test_merge_when_approved_no_threads(gh_mock, fake_redis_client, label_config):
    """APPROVED + CI green + no unresolved threads -> MERGE."""
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=70, labels=[], review_decision="APPROVED"),
    ]
    gh_mock.get_ci_status.return_value = [
        {"name": "tests", "conclusion": "success"},
    ]
    gh_mock.get_unresolved_review_threads.return_value = []

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == PRAction.MERGE


def test_followup_when_approved_with_threads(gh_mock, fake_redis_client, label_config):
    """APPROVED + CI green + unresolved threads -> ENQUEUE_FOLLOWUP with threads."""
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=80, labels=[], review_decision="APPROVED"),
    ]
    gh_mock.get_ci_status.return_value = [
        {"name": "tests", "conclusion": "success"},
    ]
    threads = [
        {
            "id": "PRRT_10",
            "path": "lib/bar.py",
            "line": 99,
            "comments": [{"author": "lead", "body": "Consider refactoring"}],
        },
    ]
    gh_mock.get_unresolved_review_threads.return_value = threads

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    pr = results[0]
    assert pr.action == PRAction.ENQUEUE_FOLLOWUP
    assert pr.review_threads == threads


def test_skip_green_no_review_no_threads(gh_mock, fake_redis_client, label_config):
    """Empty reviewDecision + CI green + no unresolved threads -> SKIP_GREEN."""
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=90, labels=[], review_decision=""),
    ]
    gh_mock.get_ci_status.return_value = [
        {"name": "lint", "conclusion": "success"},
    ]
    gh_mock.get_unresolved_review_threads.return_value = []

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == PRAction.SKIP_GREEN
    gh_mock.get_unresolved_review_threads.assert_called_once()


def test_enqueue_fix_for_unresolved_review_threads(gh_mock, fake_redis_client, label_config):
    """Empty reviewDecision + CI green + unresolved threads -> ENQUEUE_FIX."""
    threads = [
        {
            "id": "thread-1",
            "path": "src/foo.py",
            "line": 10,
            "comments": [{"author": "reviewer", "body": "Fix this"}],
        },
    ]
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=91, labels=[], review_decision=""),
    ]
    gh_mock.get_ci_status.return_value = [
        {"name": "lint", "conclusion": "success"},
    ]
    gh_mock.get_unresolved_review_threads.return_value = threads

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == PRAction.ENQUEUE_FIX
    assert results[0].review_threads == threads
    assert results[0].ci_failures == []


def test_ci_failure_skips_review_check(gh_mock, fake_redis_client, label_config):
    """CI failing + CHANGES_REQUESTED -> ENQUEUE_FIX with ci_failures, threads NOT fetched."""
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=100, labels=[], review_decision="CHANGES_REQUESTED"),
    ]
    gh_mock.get_ci_status.return_value = [
        {"name": "tests", "conclusion": "failure", "detailsUrl": "https://ci/1"},
        {"name": "lint", "conclusion": "success"},
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
    assert len(pr.ci_failures) == 1
    assert pr.ci_failures[0]["name"] == "tests"
    gh_mock.get_unresolved_review_threads.assert_not_called()


def test_review_feedback_respects_max_attempts(gh_mock, fake_redis_client, label_config):
    """Max attempts exhausted -> SKIP_MAX_ATTEMPTS (before CI/review check)."""
    pr_number = 110
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=pr_number, labels=[], review_decision="CHANGES_REQUESTED"),
    ]

    # Seed Redis with attempts at the max (3), matching the default head_sha.
    for _ in range(3):
        increment_attempts(fake_redis_client, REPO, pr_number, head_sha="abc123")

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == PRAction.SKIP_MAX_ATTEMPTS
    # Max-attempts check fires early, before CI/review analysis
    gh_mock.get_ci_status.assert_not_called()
    gh_mock.get_unresolved_review_threads.assert_not_called()


# ---------------------------------------------------------------------------
# Pending CI / StatusContext tests
# ---------------------------------------------------------------------------


def test_skip_pending_status_context(gh_mock, fake_redis_client, label_config):
    """A StatusContext with state='PENDING' should be treated as pending."""
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=120, labels=[]),
    ]
    gh_mock.get_ci_status.return_value = [
        {"name": "ci/external", "state": "PENDING"},
    ]

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == PRAction.SKIP_PENDING


def test_failure_plus_pending_enqueues_fix(gh_mock, fake_redis_client, label_config):
    """If one check failed and another is pending, enqueue fix immediately."""
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=130, labels=[]),
    ]
    gh_mock.get_ci_status.return_value = [
        {"name": "tests", "conclusion": "failure", "detailsUrl": "https://ci/1"},
        {"name": "deploy", "state": "PENDING"},
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
    assert len(pr.ci_failures) == 1
    assert pr.ci_failures[0]["name"] == "tests"


def test_error_state_counted_as_failure(gh_mock, fake_redis_client, label_config):
    """A StatusContext with state='ERROR' should be treated as a CI failure."""
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=140, labels=[]),
    ]
    gh_mock.get_ci_status.return_value = [
        {"name": "ci/external", "state": "ERROR"},
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
    assert len(pr.ci_failures) == 1
    assert pr.ci_failures[0]["name"] == "ci/external"


def test_cancelled_conclusion_counted_as_failure(gh_mock, fake_redis_client, label_config):
    """A CheckRun with conclusion='cancelled' should be treated as a CI failure."""
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=141, labels=[]),
    ]
    gh_mock.get_ci_status.return_value = [
        {"name": "deploy", "conclusion": "cancelled"},
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
    assert len(pr.ci_failures) == 1
    assert pr.ci_failures[0]["name"] == "deploy"


def test_self_cancelled_stale_run_is_suppressed(gh_mock, fake_redis_client, label_config):
    """A CANCELLED check from stale self-healing does not enqueue a Claude fix."""
    pr_number = 143
    head_sha = "sha-self-cancelled"
    run_id = 12345
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=pr_number, labels=[], head_sha=head_sha),
    ]
    gh_mock.get_ci_status.return_value = [
        {
            "name": "build",
            "conclusion": "cancelled",
            "detailsUrl": f"https://github.com/org/repo/actions/runs/{run_id}/job/1",
        },
    ]
    record_self_cancelled_stale_runs(
        fake_redis_client,
        REPO,
        pr_number,
        head_sha,
        [run_id],
        ttl_seconds=86400,
    )

    results = discover_actionable_prs(
        repo=REPO,
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == PRAction.SKIP_PENDING
    assert results[0].ci_failures == []


def test_unrelated_cancelled_run_still_enqueues_fix(gh_mock, fake_redis_client, label_config):
    """Only the run IDs cancelled by stale self-healing are suppressed."""
    pr_number = 144
    head_sha = "sha-other-cancelled"
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=pr_number, labels=[], head_sha=head_sha),
    ]
    gh_mock.get_ci_status.return_value = [
        {
            "name": "build",
            "conclusion": "cancelled",
            "detailsUrl": "https://github.com/org/repo/actions/runs/99999/job/1",
        },
    ]
    record_self_cancelled_stale_runs(
        fake_redis_client,
        REPO,
        pr_number,
        head_sha,
        [12345],
        ttl_seconds=86400,
    )

    results = discover_actionable_prs(
        repo=REPO,
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == PRAction.ENQUEUE_FIX
    assert results[0].ci_failures == gh_mock.get_ci_status.return_value


def test_timed_out_conclusion_counted_as_failure(gh_mock, fake_redis_client, label_config):
    """A CheckRun with conclusion='timed_out' should be treated as a CI failure."""
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=142, labels=[]),
    ]
    gh_mock.get_ci_status.return_value = [
        {"name": "integration-tests", "conclusion": "timed_out"},
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
    assert len(pr.ci_failures) == 1
    assert pr.ci_failures[0]["name"] == "integration-tests"


def test_startup_failure_conclusion_counted_as_failure(gh_mock, fake_redis_client, label_config):
    """A CheckRun with conclusion='startup_failure' should be treated as a CI failure."""
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=143, labels=[]),
    ]
    gh_mock.get_ci_status.return_value = [
        {"name": "build", "conclusion": "startup_failure"},
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
    assert len(pr.ci_failures) == 1
    assert pr.ci_failures[0]["name"] == "build"


# ---------------------------------------------------------------------------
# Thread fetch failure tests
# ---------------------------------------------------------------------------


def test_approved_thread_fetch_failure_skips_merge(gh_mock, fake_redis_client, label_config):
    """APPROVED + CI green + thread fetch raises -> SKIP_GREEN (safe: don't merge)."""
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=150, labels=[], review_decision="APPROVED"),
    ]
    gh_mock.get_ci_status.return_value = [
        {"name": "tests", "conclusion": "success"},
    ]
    gh_mock.get_unresolved_review_threads.side_effect = RuntimeError("GraphQL 502")

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == PRAction.SKIP_PENDING
    assert results[0].number == 150


def test_changes_requested_all_threads_resolved_retriggers_review(
    gh_mock,
    fake_redis_client,
    label_config,
):
    """CHANGES_REQUESTED + CI green + threads resolved + claude-review -> RETRIGGER_REVIEW."""
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(
            number=155, labels=[], review_decision="CHANGES_REQUESTED", head_sha="abc999"
        ),
    ]
    gh_mock.get_ci_status.return_value = [
        {"name": "tests", "conclusion": "success"},
        {
            "name": "claude-review",
            "conclusion": "SUCCESS",
            "status": "COMPLETED",
            "detailsUrl": "https://github.com/o/r/actions/runs/99999/job/1",
        },
    ]
    gh_mock.get_unresolved_review_threads.return_value = []

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    pr = results[0]
    assert pr.action == PRAction.RETRIGGER_REVIEW
    assert pr.review_run_id == 99999


def test_changes_requested_all_threads_resolved_no_claude_review_skips(
    gh_mock,
    fake_redis_client,
    label_config,
):
    """CHANGES_REQUESTED + CI green + all threads resolved + no claude-review -> SKIP_GREEN."""
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=156, labels=[], review_decision="CHANGES_REQUESTED"),
    ]
    gh_mock.get_ci_status.return_value = [
        {"name": "tests", "conclusion": "success"},
    ]
    gh_mock.get_unresolved_review_threads.return_value = []

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    pr = results[0]
    assert pr.action == PRAction.SKIP_GREEN
    assert pr.review_threads == []


def test_changes_requested_thread_fetch_failure_skips_retryably(
    gh_mock,
    fake_redis_client,
    label_config,
):
    """CHANGES_REQUESTED + CI green + thread fetch raises -> retryable skip."""
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=160, labels=[], review_decision="CHANGES_REQUESTED"),
    ]
    gh_mock.get_ci_status.return_value = [
        {"name": "tests", "conclusion": "success"},
    ]
    gh_mock.get_unresolved_review_threads.side_effect = RuntimeError("API 500")

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    pr = results[0]
    assert pr.action == PRAction.SKIP_PENDING
    assert pr.review_threads == []


# ---------------------------------------------------------------------------
# Max attempts on other paths
# ---------------------------------------------------------------------------


def test_ci_failure_respects_max_attempts(gh_mock, fake_redis_client, label_config):
    """Max attempts exhausted -> SKIP_MAX_ATTEMPTS (before CI check)."""
    pr_number = 170
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=pr_number, labels=[]),
    ]

    for _ in range(3):
        increment_attempts(fake_redis_client, REPO, pr_number, head_sha="abc123")

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == PRAction.SKIP_MAX_ATTEMPTS
    # Max-attempts check fires early, before CI analysis
    gh_mock.get_ci_status.assert_not_called()


def test_followup_respects_max_attempts(gh_mock, fake_redis_client, label_config):
    """Max attempts exhausted -> SKIP_MAX_ATTEMPTS (before review check)."""
    pr_number = 180
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=pr_number, labels=[], review_decision="APPROVED"),
    ]

    for _ in range(3):
        increment_attempts(fake_redis_client, REPO, pr_number, head_sha="abc123")

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == PRAction.SKIP_MAX_ATTEMPTS
    # Max-attempts check fires early, before CI/review analysis
    gh_mock.get_ci_status.assert_not_called()
    gh_mock.get_unresolved_review_threads.assert_not_called()


# ---------------------------------------------------------------------------
# CheckRun pending (no conclusion, no state) tests
# ---------------------------------------------------------------------------


def test_skip_pending_checkrun_no_conclusion(gh_mock, fake_redis_client, label_config):
    """A CheckRun with no conclusion (in-progress) should be treated as pending.

    CheckRuns that are still running have no "conclusion" field.
    The ci_pending filter catches these via the absent conclusion
    (and the absent/empty "state" field, which defaults to "").
    """
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=190, labels=[]),
    ]
    gh_mock.get_ci_status.return_value = [
        {"name": "build"},
    ]

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == PRAction.SKIP_PENDING


# ---------------------------------------------------------------------------
# Attempt tracking unit tests
# ---------------------------------------------------------------------------


def test_get_attempt_count_sha_mismatch_resets(fake_redis_client):
    """When head SHA changes (new commits pushed), the attempt counter resets to 0."""
    pr_number = 200
    # Seed attempts with old SHA
    increment_attempts(fake_redis_client, REPO, pr_number, head_sha="abc123")
    increment_attempts(fake_redis_client, REPO, pr_number, head_sha="abc123")
    assert get_attempt_count(fake_redis_client, REPO, pr_number, "abc123") == 2

    # Query with a different SHA — counter should reset
    count = get_attempt_count(fake_redis_client, REPO, pr_number, "def456")
    assert count == 0

    # The key should have been deleted, so a fresh increment starts at 1
    assert increment_attempts(fake_redis_client, REPO, pr_number, "def456") == 1


def test_get_attempt_count_missing_key_returns_zero(fake_redis_client):
    """When no attempts have been recorded for a PR, get_attempt_count returns 0."""
    count = get_attempt_count(fake_redis_client, REPO, 999, "anysha")
    assert count == 0


def test_get_attempt_count_corrupt_count_returns_zero(fake_redis_client):
    """Non-integer 'count' value in the Redis hash returns 0 (ValueError caught)."""
    pr_number = 210
    key = f"pr:{REPO}:{pr_number}:attempts"
    fake_redis_client.hset(key, "count", "not-a-number")
    fake_redis_client.hset(key, "head_sha", "abc123")

    count = get_attempt_count(fake_redis_client, REPO, pr_number, "abc123")
    assert count == 0


def test_increment_attempts_sets_ttl(fake_redis_client):
    """increment_attempts sets a 7-day TTL on the attempts key."""
    pr_number = 220
    increment_attempts(fake_redis_client, REPO, pr_number, head_sha="sha1")

    key = f"pr:{REPO}:{pr_number}:attempts"
    ttl = fake_redis_client.ttl(key)
    expected_ttl = 7 * 24 * 3600
    # TTL should be set and close to 7 days (allow a small margin for execution time)
    assert ttl > 0
    assert ttl <= expected_ttl


def test_increment_attempts_stores_sha(fake_redis_client):
    """increment_attempts stores the head_sha in the hash."""
    pr_number = 230
    increment_attempts(fake_redis_client, REPO, pr_number, head_sha="deadbeef")

    key = f"pr:{REPO}:{pr_number}:attempts"
    stored_sha = fake_redis_client.hget(key, "head_sha")
    assert stored_sha == "deadbeef"


def test_increment_attempts_returns_new_count(fake_redis_client):
    """increment_attempts returns the new (incremented) count each time."""
    pr_number = 240
    assert increment_attempts(fake_redis_client, REPO, pr_number, "sha1") == 1
    assert increment_attempts(fake_redis_client, REPO, pr_number, "sha1") == 2
    assert increment_attempts(fake_redis_client, REPO, pr_number, "sha1") == 3


def test_increment_attempts_resets_on_sha_change(fake_redis_client):
    """increment_attempts resets the counter when the head SHA changes."""
    pr_number = 245
    assert increment_attempts(fake_redis_client, REPO, pr_number, "old_sha") == 1
    assert increment_attempts(fake_redis_client, REPO, pr_number, "old_sha") == 2

    # SHA changes — counter should reset to 1, not increment to 3
    assert increment_attempts(fake_redis_client, REPO, pr_number, "new_sha") == 1

    # Subsequent increments with the new SHA continue from 1
    assert increment_attempts(fake_redis_client, REPO, pr_number, "new_sha") == 2


def test_clear_attempts_deletes_key(fake_redis_client):
    """clear_attempts removes the attempts key from Redis entirely."""
    pr_number = 250
    increment_attempts(fake_redis_client, REPO, pr_number, head_sha="sha1")
    assert get_attempt_count(fake_redis_client, REPO, pr_number, "sha1") == 1

    clear_attempts(fake_redis_client, REPO, pr_number)

    # Key should be gone — get_attempt_count returns 0
    assert get_attempt_count(fake_redis_client, REPO, pr_number, "sha1") == 0
    # Verify key is actually deleted in Redis
    key = f"pr:{REPO}:{pr_number}:attempts"
    assert not fake_redis_client.exists(key)


# ---------------------------------------------------------------------------
# Additional discover_actionable_prs tests
# ---------------------------------------------------------------------------


def test_discover_skips_needs_human_label(gh_mock, fake_redis_client, label_config):
    """PRs that need human intervention are skipped as SKIP_LABELED."""
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=303, labels=[{"name": "orcest:needs-human"}]),
    ]

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    for pr in results:
        assert pr.action == PRAction.SKIP_LABELED
    # CI should never be fetched for any of these
    gh_mock.get_ci_status.assert_not_called()


def test_discover_attempt_counter_without_active_work_does_not_skip(
    gh_mock, fake_redis_client, label_config
):
    """Attempts are retry budget, not active-work proof."""
    pr_number = 304
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=pr_number, labels=[], head_sha="sha1"),
    ]

    # Seed attempt counter so it's > 0
    increment_attempts(fake_redis_client, REPO, pr_number, head_sha="sha1")
    gh_mock.get_ci_status.return_value = [
        {"name": "tests", "conclusion": "failure"},
    ]

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == PRAction.ENQUEUE_FIX
    assert results[0].number == pr_number


def test_discover_multiple_prs(gh_mock, fake_redis_client, label_config):
    """Multiple PRs in a single list get independently classified."""
    gh_mock.list_open_prs.return_value = [
        # PR 310: CI failure -> ENQUEUE_FIX
        _make_pr_data(number=310, labels=[]),
        # PR 311: CI green, no review, no threads -> SKIP_GREEN
        _make_pr_data(number=311, labels=[], review_decision=""),
    ]

    def ci_status_side_effect(repo, pr_number, token, **kwargs):
        if pr_number == 310:
            return [{"name": "tests", "conclusion": "failure", "detailsUrl": "x"}]
        return [{"name": "tests", "conclusion": "success"}]

    gh_mock.get_ci_status.side_effect = ci_status_side_effect
    gh_mock.get_unresolved_review_threads.return_value = []

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 2
    by_number = {pr.number: pr for pr in results}
    assert by_number[310].action == PRAction.ENQUEUE_FIX
    assert len(by_number[310].ci_failures) == 1
    assert by_number[311].action == PRAction.SKIP_GREEN


def test_discover_review_required_skips_green(gh_mock, fake_redis_client, label_config):
    """REVIEW_REQUIRED reviewDecision (not APPROVED, not CHANGES_REQUESTED) -> SKIP_GREEN."""
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=320, labels=[], review_decision="REVIEW_REQUIRED"),
    ]
    gh_mock.get_ci_status.return_value = [
        {"name": "tests", "conclusion": "success"},
    ]
    gh_mock.get_unresolved_review_threads.return_value = []

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == PRAction.SKIP_GREEN
    assert results[0].number == 320


# ---------------------------------------------------------------------------
# Merge conflict detection tests
# ---------------------------------------------------------------------------


def test_conflicting_pr_enqueues_rebase(gh_mock, fake_redis_client, label_config):
    """A PR with mergeable=CONFLICTING is routed to ENQUEUE_REBASE, CI not fetched."""
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=400, labels=[], mergeable="CONFLICTING"),
    ]

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    pr = results[0]
    assert pr.action == PRAction.ENQUEUE_REBASE
    assert pr.number == 400
    assert pr.ci_failures == []
    assert pr.review_threads == []
    # CI should not be fetched — merge conflict detected before that step
    gh_mock.get_ci_status.assert_not_called()


def test_base_branch_propagated_to_pr_state(gh_mock, fake_redis_client, label_config):
    """base_branch is extracted from baseRefName and set on PRState."""
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=500, labels=[], mergeable="CONFLICTING", base_branch="develop"),
    ]

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].base_branch == "develop"


def test_behind_pr_falls_through_to_ci_without_update_branch(
    gh_mock, fake_redis_client, label_config
):
    """BEHIND alone must not update the PR branch and churn CI."""
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(
            number=410,
            labels=[],
            mergeable="MERGEABLE",
            merge_state_status="BEHIND",
        ),
    ]
    gh_mock.get_ci_status.return_value = [
        {"name": "tests", "conclusion": "success"},
    ]
    gh_mock.get_unresolved_review_threads.return_value = []

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == PRAction.SKIP_GREEN
    assert results[0].number == 410
    gh_mock.get_ci_status.assert_called_once()


def test_blocked_pr_with_failing_ci_routes_to_fix(gh_mock, fake_redis_client, label_config):
    """BLOCKED usually means branch protection is blocked by checks/reviews.

    It must not short-circuit to UPDATE_BRANCH before failed CI can be queued.
    """
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(
            number=413,
            labels=[],
            mergeable="MERGEABLE",
            merge_state_status="BLOCKED",
        ),
    ]
    gh_mock.get_ci_status.return_value = [
        {"name": "Node Quality", "conclusion": "failure"},
    ]

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == PRAction.ENQUEUE_FIX
    assert results[0].ci_failures == [{"name": "Node Quality", "conclusion": "failure"}]


def test_conflicting_pr_takes_priority_over_behind(gh_mock, fake_redis_client, label_config):
    """If a PR is both CONFLICTING and BEHIND, conflict resolution wins —
    a worker rebase is needed; orchestrator-side update-branch would fail."""
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(
            number=411,
            labels=[],
            mergeable="CONFLICTING",
            merge_state_status="BEHIND",
        ),
    ]

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == PRAction.ENQUEUE_REBASE


def test_clean_pr_does_not_trigger_update_branch(gh_mock, fake_redis_client, label_config):
    """mergeStateStatus=CLEAN (or absent) must not route to UPDATE_BRANCH."""
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(
            number=412,
            labels=[],
            mergeable="MERGEABLE",
            merge_state_status="CLEAN",
        ),
    ]
    gh_mock.get_ci_status.return_value = [{"name": "tests", "conclusion": "success"}]
    gh_mock.get_unresolved_review_threads.return_value = []

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action != PRAction.UPDATE_BRANCH


def test_unstable_pr_does_not_trigger_update_branch(gh_mock, fake_redis_client, label_config):
    """mergeStateStatus=UNSTABLE means non-required CI is failing but the PR is
    otherwise mergeable. Don't fire UPDATE_BRANCH — being merely UNSTABLE doesn't
    indicate behind-base; the normal flow handles it."""
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(
            number=414,
            labels=[],
            mergeable="MERGEABLE",
            merge_state_status="UNSTABLE",
        ),
    ]
    gh_mock.get_ci_status.return_value = [{"name": "tests", "conclusion": "success"}]
    gh_mock.get_unresolved_review_threads.return_value = []

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action != PRAction.UPDATE_BRANCH


def test_unknown_mergeable_falls_through_to_ci_and_blocks_merge(
    gh_mock, fake_redis_client, label_config
):
    """A PR with mergeable=UNKNOWN continues normally to the CI check."""
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=401, labels=[], mergeable="UNKNOWN", review_decision="APPROVED"),
    ]
    gh_mock.get_ci_status.return_value = [
        {"name": "tests", "conclusion": "success"},
    ]
    gh_mock.get_unresolved_review_threads.return_value = []

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == PRAction.SKIP_PENDING
    gh_mock.get_ci_status.assert_called_once()


def test_unknown_mergeable_ci_failure_still_enqueues_fix(gh_mock, fake_redis_client, label_config):
    """mergeable=UNKNOWN does not mask CI failure classification."""
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=403, labels=[], mergeable="UNKNOWN", review_decision="APPROVED"),
    ]
    gh_mock.get_ci_status.return_value = [
        {"name": "tests", "conclusion": "failure", "detailsUrl": "https://ci/1"},
    ]

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == PRAction.ENQUEUE_FIX
    gh_mock.get_unresolved_review_threads.assert_not_called()


def test_unknown_mergeable_unresolved_threads_still_enqueue_fix(
    gh_mock, fake_redis_client, label_config
):
    """mergeable=UNKNOWN does not mask review-thread fix classification."""
    threads = [
        {
            "id": "PRRT_401",
            "path": "src/foo.py",
            "line": 12,
            "comments": [{"author": "reviewer", "body": "Please adjust this"}],
        },
    ]
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=404, labels=[], mergeable="UNKNOWN", review_decision=""),
    ]
    gh_mock.get_ci_status.return_value = [
        {"name": "tests", "conclusion": "success"},
    ]
    gh_mock.get_unresolved_review_threads.return_value = threads

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == PRAction.ENQUEUE_FIX
    assert results[0].review_threads == threads


def test_mergeable_pr_falls_through_to_ci(gh_mock, fake_redis_client, label_config):
    """A PR with mergeable=MERGEABLE continues normally to the CI check."""
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=402, labels=[], mergeable="MERGEABLE"),
    ]
    gh_mock.get_ci_status.return_value = [
        {"name": "tests", "conclusion": "failure", "detailsUrl": "https://ci/1"},
    ]

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == PRAction.ENQUEUE_FIX
    gh_mock.get_ci_status.assert_called_once()


def test_conflicting_pr_enqueues_rebase_despite_attempt_counter(
    gh_mock, fake_redis_client, label_config
):
    """A conflicting PR with attempt_count > 0 routes to ENQUEUE_REBASE, not SKIP_ACTIVE.

    Conflicts can't resolve without a rebase, so SKIP_ACTIVE would deadlock.
    """
    pr_number = 403
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=pr_number, labels=[], mergeable="CONFLICTING", head_sha="sha1"),
    ]
    increment_attempts(fake_redis_client, REPO, pr_number, head_sha="sha1")

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == PRAction.ENQUEUE_REBASE
    gh_mock.get_ci_status.assert_not_called()


# ---------------------------------------------------------------------------
# SKIP_NO_CHECKS tests
# ---------------------------------------------------------------------------


def test_skip_no_checks_pr(gh_mock, fake_redis_client, label_config):
    """A PR with zero CI checks is classified as SKIP_NO_CHECKS (not SKIP_GREEN)."""
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=410, labels=[]),
    ]
    gh_mock.get_ci_status.return_value = []

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == PRAction.SKIP_NO_CHECKS
    assert results[0].number == 410


# ---------------------------------------------------------------------------
# Pending task dedup tests
# ---------------------------------------------------------------------------


def test_skip_queued_when_pending_task_exists(gh_mock, fake_redis_client, label_config):
    """A PR with a pending task marker is classified as SKIP_QUEUED."""
    pr_number = 600
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=pr_number, labels=[], head_sha="abc123"),
    ]
    set_pending_task(
        fake_redis_client,
        "test-org/test-repo",
        "pr",
        pr_number,
        "task-xyz",
        snapshot_head_sha="abc123",
        decision_reason="ci_failure",
        ttl=86400,
    )

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == PRAction.SKIP_QUEUED


def test_pending_task_old_sha_is_cleared_and_current_snapshot_enqueues(
    gh_mock, fake_redis_client, label_config
):
    pr_number = 601
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=pr_number, labels=[], head_sha="sha-new"),
    ]
    gh_mock.get_ci_status.return_value = [
        {"name": "tests", "conclusion": "FAILURE", "status": "COMPLETED"}
    ]

    set_pending_task(
        fake_redis_client,
        REPO,
        "pr",
        pr_number,
        "task-old",
        snapshot_head_sha="sha-old",
        decision_reason="ci_failure",
        ttl=3600,
    )

    results = discover_actionable_prs(
        repo=REPO,
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == PRAction.ENQUEUE_FIX
    assert get_pending_task(fake_redis_client, REPO, "pr", pr_number) is None


def test_legacy_pending_task_without_snapshot_is_cleared_and_requeued(
    gh_mock, fake_redis_client, label_config
):
    pr_number = 602
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=pr_number, labels=[], head_sha="sha-current"),
    ]
    gh_mock.get_ci_status.return_value = [
        {"name": "tests", "conclusion": "FAILURE", "status": "COMPLETED"}
    ]
    fake_redis_client.set_ex(
        make_pending_task_key(REPO, "pr", pr_number),
        "legacy-task",
        ttl=3600,
    )

    results = discover_actionable_prs(
        repo=REPO,
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == PRAction.ENQUEUE_FIX
    assert get_pending_task(fake_redis_client, REPO, "pr", pr_number) is None


def test_no_skip_queued_without_pending_marker(gh_mock, fake_redis_client, label_config):
    """A PR without a pending marker proceeds to CI check as normal."""
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=601, labels=[]),
    ]
    gh_mock.get_ci_status.return_value = [
        {"name": "tests", "conclusion": "success"},
    ]
    gh_mock.get_unresolved_review_threads.return_value = []

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == PRAction.SKIP_GREEN
    gh_mock.get_ci_status.assert_called_once()


def test_skip_usage_cooldown_when_active(gh_mock, fake_redis_client, label_config):
    """A PR with an active USAGE_EXHAUSTED cooldown is classified as SKIP_USAGE_COOLDOWN."""
    pr_number = 602
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=pr_number, labels=[]),
    ]
    # Set a cooldown marker (short TTL is fine for tests)
    set_usage_exhausted_cooldown(fake_redis_client, REPO, pr_number, ttl_seconds=300)

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == PRAction.SKIP_USAGE_COOLDOWN
    # CI should not be fetched during cooldown
    gh_mock.get_ci_status.assert_not_called()


def test_no_skip_usage_cooldown_when_not_set(gh_mock, fake_redis_client, label_config):
    """A PR with no active cooldown proceeds normally (equivalent to an expired cooldown)."""
    pr_number = 603
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=pr_number, labels=[]),
    ]
    gh_mock.get_ci_status.return_value = [
        {"name": "tests", "conclusion": "failure"},
    ]

    # No cooldown set — PR should proceed to CI evaluation
    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == PRAction.ENQUEUE_FIX
    gh_mock.get_ci_status.assert_called_once()


# ---------------------------------------------------------------------------
# Total attempt counter (circuit breaker) tests
# ---------------------------------------------------------------------------


def test_total_attempts_at_limit_hard_stops(gh_mock, fake_redis_client, label_config):
    """A PR at max_total_attempts is classified as SKIP_MAX_TOTAL_ATTEMPTS."""
    pr_number = 700
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=pr_number, labels=[]),
    ]
    for _ in range(10):
        increment_total_attempts(fake_redis_client, REPO, pr_number)

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
        max_total_attempts=10,
    )

    assert len(results) == 1
    assert results[0].action == PRAction.SKIP_MAX_TOTAL_ATTEMPTS
    gh_mock.get_ci_status.assert_not_called()


def test_total_attempts_survives_sha_change(fake_redis_client):
    """Total attempt counter is NOT reset when the per-SHA counter resets."""
    pr_number = 710
    # Increment both counters
    increment_attempts(fake_redis_client, REPO, pr_number, "sha1")
    increment_total_attempts(fake_redis_client, REPO, pr_number)

    # SHA changes — per-SHA counter resets, total does not
    assert get_attempt_count(fake_redis_client, REPO, pr_number, "sha2") == 0
    assert get_total_attempt_count(fake_redis_client, REPO, pr_number) == 1


def test_clear_total_attempts(fake_redis_client):
    """clear_total_attempts removes the total attempts key."""
    pr_number = 720
    increment_total_attempts(fake_redis_client, REPO, pr_number)
    increment_total_attempts(fake_redis_client, REPO, pr_number)
    assert get_total_attempt_count(fake_redis_client, REPO, pr_number) == 2

    clear_total_attempts(fake_redis_client, REPO, pr_number)
    assert get_total_attempt_count(fake_redis_client, REPO, pr_number) == 0


def test_total_attempts_below_limit_proceeds(gh_mock, fake_redis_client, label_config):
    """A PR with total attempts below the limit proceeds normally."""
    pr_number = 730
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=pr_number, labels=[]),
    ]
    gh_mock.get_ci_status.return_value = [
        {"name": "tests", "conclusion": "failure", "detailsUrl": "x"},
    ]
    # Set total attempts below limit
    for _ in range(5):
        increment_total_attempts(fake_redis_client, REPO, pr_number)

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
        max_total_attempts=10,
    )

    assert len(results) == 1
    assert results[0].action == PRAction.ENQUEUE_FIX


def test_backoff_cooldown_skips_before_enqueue(gh_mock, fake_redis_client, label_config):
    """A PR in transient-failure backoff is not enqueued until cooldown expires."""
    pr_number = 735
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=pr_number, labels=[]),
    ]
    set_backoff_cooldown(fake_redis_client, REPO, pr_number, step=2)

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == PRAction.SKIP_BACKOFF
    gh_mock.get_ci_status.assert_not_called()


def test_expired_transient_backoff_enqueues_retry(gh_mock, fake_redis_client, label_config):
    """A transient-exhausted PR is picked up again after the cooldown expires."""
    pr_number = 738
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=pr_number, labels=[]),
    ]
    gh_mock.get_ci_status.return_value = [
        {"name": "tests", "conclusion": "failure", "detailsUrl": "x"},
    ]
    set_backoff_cooldown(fake_redis_client, REPO, pr_number, step=2)
    fake_redis_client.expire(f"backoff:pr:{REPO}:{pr_number}", 0)

    results = discover_actionable_prs(
        repo=REPO,
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == PRAction.ENQUEUE_FIX
    gh_mock.get_ci_status.assert_called_once()


def test_backoff_cooldown_same_sha_skips_before_enqueue(gh_mock, fake_redis_client, label_config):
    """A SHA-aware transient backoff still skips for the same head SHA."""
    pr_number = 736
    head_sha = "sha-a"
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=pr_number, labels=[], head_sha=head_sha),
    ]
    set_backoff_cooldown(fake_redis_client, REPO, pr_number, step=2, head_sha=head_sha)

    results = discover_actionable_prs(
        repo=REPO,
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == PRAction.SKIP_BACKOFF
    gh_mock.get_ci_status.assert_not_called()


def test_backoff_cooldown_new_sha_clears_backoff_and_transient_count(
    gh_mock, fake_redis_client, label_config
):
    """A new commit bypasses stale transient backoff even after attempts were cleared."""
    pr_number = 737
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=pr_number, labels=[], head_sha="sha-b"),
    ]
    gh_mock.get_ci_status.return_value = [
        {"name": "tests", "conclusion": "failure", "detailsUrl": "x"},
    ]
    set_backoff_cooldown(fake_redis_client, REPO, pr_number, step=2, head_sha="sha-a")
    increment_transient_failure_count(fake_redis_client, REPO, pr_number)

    results = discover_actionable_prs(
        repo=REPO,
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == PRAction.ENQUEUE_FIX
    assert get_backoff_step(fake_redis_client, REPO, pr_number) is None
    assert get_transient_failure_count(fake_redis_client, REPO, pr_number) == 0


def test_total_attempts_hard_stop_preserves_counter(gh_mock, fake_redis_client, label_config):
    """Hard stop at max_total_attempts preserves the counter (no reset)."""
    pr_number = 740
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=pr_number, labels=[]),
    ]
    for _ in range(10):
        increment_total_attempts(fake_redis_client, REPO, pr_number)

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
        max_total_attempts=10,
    )

    assert len(results) == 1
    assert results[0].action == PRAction.SKIP_MAX_TOTAL_ATTEMPTS
    assert get_total_attempt_count(fake_redis_client, REPO, pr_number) == 10


def test_total_attempts_no_recovery_when_needs_human_label_still_present(
    gh_mock, fake_redis_client, label_config
):
    """exhausted_notified=True but needs-human label still present → PR is skipped.

    SKIP_LABELED fires first (needs_human is in terminal_labels), so the PR
    never reaches the circuit-breaker block.  This test documents that invariant:
    counters are not cleared, and the result is SKIP_LABELED, not
    SKIP_MAX_TOTAL_ATTEMPTS.
    """
    pr_number = 751
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=pr_number, labels=[{"name": label_config.needs_human}]),
    ]
    for _ in range(10):
        increment_total_attempts(fake_redis_client, REPO, pr_number)
    set_exhausted_notified(fake_redis_client, REPO, pr_number)

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
        max_total_attempts=10,
    )

    assert len(results) == 1
    assert results[0].action == PRAction.SKIP_LABELED
    # Counters must NOT be cleared — no human approval yet.
    assert get_total_attempt_count(fake_redis_client, REPO, pr_number) == 10
    assert get_exhausted_notified(fake_redis_client, REPO, pr_number)


def test_exhausted_notified_helpers(fake_redis_client):
    """set/get/clear_exhausted_notified operate correctly."""
    pr_number = 760
    assert not get_exhausted_notified(fake_redis_client, REPO, pr_number)

    set_exhausted_notified(fake_redis_client, REPO, pr_number)
    assert get_exhausted_notified(fake_redis_client, REPO, pr_number)

    clear_exhausted_notified(fake_redis_client, REPO, pr_number)
    assert not get_exhausted_notified(fake_redis_client, REPO, pr_number)


def test_skip_labeled_needs_human_refreshes_exhausted_notified_ttl(
    gh_mock, fake_redis_client, label_config
):
    """SKIP_LABELED with needs-human present and exhausted_notified set refreshes the flag TTL.

    This prevents the TTL cliff: without the refresh, the 30-day flag TTL can
    expire while the needs-human label is still present (because SKIP_LABELED
    fires before the circuit breaker, never triggering a refresh). When the
    human eventually removes the label, the recovery branch would silently miss
    and the circuit breaker would re-fire instead.
    """
    pr_number = 761
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=pr_number, labels=[{"name": label_config.needs_human}]),
    ]
    for _ in range(10):
        increment_total_attempts(fake_redis_client, REPO, pr_number)
    set_exhausted_notified(fake_redis_client, REPO, pr_number)

    # Manually shorten the TTL to simulate approaching expiry.
    key = f"pr:{REPO}:{pr_number}:exhausted_notified"
    fake_redis_client.expire(key, 60)  # 60 seconds remaining
    ttl_before = fake_redis_client.ttl(key)
    assert ttl_before == 60

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
        max_total_attempts=10,
    )

    assert len(results) == 1
    assert results[0].action == PRAction.SKIP_LABELED
    # Flag must still be set and TTL must have been reset to the full 30-day window.
    assert get_exhausted_notified(fake_redis_client, REPO, pr_number)
    ttl_after = fake_redis_client.ttl(key)
    assert ttl_after > 24 * 3600  # reset to ~30-day window, not just any increase


def test_max_total_attempts_hard_stop_ignores_exhausted_notified(
    gh_mock, fake_redis_client, label_config
):
    """Hard stop at max_total_attempts fires regardless of exhausted_notified flag."""
    pr_number = 763
    head_sha = "abc123"
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=pr_number, labels=[], head_sha=head_sha),
    ]

    for _ in range(10):
        increment_total_attempts(fake_redis_client, REPO, pr_number)
    set_exhausted_notified(fake_redis_client, REPO, pr_number)

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
        max_total_attempts=10,
    )

    assert len(results) == 1
    assert results[0].action == PRAction.SKIP_MAX_TOTAL_ATTEMPTS
    gh_mock.get_ci_status.assert_not_called()


# ---------------------------------------------------------------------------
# Review re-trigger tests
# ---------------------------------------------------------------------------


def _make_claude_review_check(
    run_id: int = 12345,
    conclusion: str = "SUCCESS",
) -> dict:
    """Build a claude-review check dict matching statusCheckRollup shape."""
    return {
        "name": "claude-review",
        "conclusion": conclusion,
        "detailsUrl": f"https://github.com/org/repo/actions/runs/{run_id}/job/999",
        "status": "COMPLETED",
    }


def test_retrigger_review_when_claude_review_passed_no_formal_review(
    gh_mock, fake_redis_client, label_config
):
    """claude-review SUCCESS + empty reviewDecision + no retrigger yet → RETRIGGER_REVIEW."""
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=800, labels=[], review_decision="", head_sha="abc123"),
    ]
    gh_mock.get_ci_status.return_value = [
        {"name": "lint", "conclusion": "success"},
        _make_claude_review_check(run_id=55555),
    ]
    gh_mock.get_unresolved_review_threads.return_value = []

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    pr = results[0]
    assert pr.action == PRAction.RETRIGGER_REVIEW
    assert pr.review_run_id == 55555
    assert pr.number == 800


def test_pr330_shape_stale_aggregate_approval_retriggers_review(
    gh_mock, fake_redis_client, label_config
):
    """Aggregate APPROVED with only old formal approval never emits MERGE."""
    head_sha = "a5292b40"
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=330, labels=[], review_decision="APPROVED", head_sha=head_sha),
    ]
    gh_mock.get_ci_status.return_value = [
        {"name": "lint", "conclusion": "success"},
        _make_claude_review_check(run_id=330330),
    ]
    gh_mock.get_review_snapshot.return_value = PRReviewSnapshot(
        head_sha=head_sha,
        state="OPEN",
        is_draft=False,
        labels=(),
        review_decision="APPROVED",
        has_current_head_approval=False,
    )
    gh_mock.get_unresolved_review_threads.return_value = []

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == PRAction.RETRIGGER_REVIEW
    assert results[0].review_run_id == 330330
    gh_mock.get_review_snapshot.assert_called_once_with(
        "test-org/test-repo", 330, "fake-token", expected_head_sha=head_sha
    )


def test_approved_snapshot_fetch_error_never_merges(gh_mock, fake_redis_client, label_config):
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=331, labels=[], review_decision="APPROVED", head_sha="abc123"),
    ]
    gh_mock.get_ci_status.return_value = [{"name": "tests", "conclusion": "success"}]
    gh_mock.get_review_snapshot.side_effect = GhCliError("malformed review evidence")

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == PRAction.SKIP_PENDING
    gh_mock.get_unresolved_review_threads.assert_not_called()


def test_ci_head_mismatch_cannot_select_action(gh_mock, fake_redis_client, label_config):
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=332, labels=[], review_decision="APPROVED", head_sha="abc123"),
    ]
    gh_mock.get_ci_status.side_effect = GhStaleSnapshotError("head changed")

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == PRAction.SKIP_PENDING
    gh_mock.get_review_snapshot.assert_not_called()


def test_retrigger_review_escalates_after_retrigger_exhausted(
    gh_mock, fake_redis_client, label_config
):
    """claude-review SUCCESS + already re-triggered for this SHA → SKIP_MAX_ATTEMPTS."""
    pr_number = 801
    head_sha = "def456"
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=pr_number, labels=[], review_decision="", head_sha=head_sha),
    ]
    gh_mock.get_ci_status.return_value = [
        {"name": "lint", "conclusion": "success"},
        _make_claude_review_check(run_id=66666),
    ]
    gh_mock.get_unresolved_review_threads.return_value = []

    # Mark that we already re-triggered for this SHA
    set_review_retrigger_sha(fake_redis_client, REPO, pr_number, head_sha)

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == PRAction.SKIP_MAX_ATTEMPTS
    assert results[0].number == pr_number


def test_retrigger_review_allowed_after_new_sha(gh_mock, fake_redis_client, label_config):
    """Re-trigger allowed when SHA changes (old retrigger was for a different SHA)."""
    pr_number = 802
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=pr_number, labels=[], review_decision="", head_sha="new_sha"),
    ]
    gh_mock.get_ci_status.return_value = [
        {"name": "lint", "conclusion": "success"},
        _make_claude_review_check(run_id=77777),
    ]
    gh_mock.get_unresolved_review_threads.return_value = []

    # Old retrigger was for a different SHA
    set_review_retrigger_sha(fake_redis_client, REPO, pr_number, "old_sha")

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == PRAction.RETRIGGER_REVIEW
    assert results[0].review_run_id == 77777


def test_skip_green_when_no_claude_review_check(gh_mock, fake_redis_client, label_config):
    """No claude-review check in CI → normal SKIP_GREEN (no retrigger)."""
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=803, labels=[], review_decision=""),
    ]
    gh_mock.get_ci_status.return_value = [
        {"name": "lint", "conclusion": "success"},
        {"name": "tests", "conclusion": "success"},
    ]
    gh_mock.get_unresolved_review_threads.return_value = []

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == PRAction.SKIP_GREEN


def test_skip_green_when_claude_review_not_success(gh_mock, fake_redis_client, label_config):
    """claude-review exists but conclusion is neutral → normal SKIP_GREEN."""
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=804, labels=[], review_decision=""),
    ]
    gh_mock.get_ci_status.return_value = [
        {"name": "lint", "conclusion": "success"},
        {
            "name": "claude-review",
            "conclusion": "neutral",
            "status": "COMPLETED",
            "detailsUrl": "https://github.com/org/repo/actions/runs/88888/job/999",
        },
    ]
    gh_mock.get_unresolved_review_threads.return_value = []

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
    )

    assert len(results) == 1
    assert results[0].action == PRAction.SKIP_GREEN


# ---------------------------------------------------------------------------
# _get_claude_review_run_id unit tests
# ---------------------------------------------------------------------------


def test_get_claude_review_run_id_success():
    """Extracts run ID from a successful claude-review check."""
    checks = [
        {"name": "lint", "conclusion": "success", "status": "COMPLETED"},
        {
            "name": "claude-review",
            "conclusion": "SUCCESS",
            "status": "COMPLETED",
            "detailsUrl": "https://github.com/org/repo/actions/runs/12345/job/67890",
        },
    ]
    assert _get_claude_review_run_id(checks) == 12345


def test_get_claude_review_run_id_no_match():
    """Returns None when no claude-review check exists."""
    checks = [
        {"name": "lint", "conclusion": "success"},
        {"name": "tests", "conclusion": "success"},
    ]
    assert _get_claude_review_run_id(checks) is None


def test_get_claude_review_run_id_not_success():
    """Returns None when claude-review exists but conclusion is not SUCCESS."""
    checks = [
        {
            "name": "claude-review",
            "conclusion": "failure",
            "status": "COMPLETED",
            "detailsUrl": "https://github.com/org/repo/actions/runs/12345/job/67890",
        },
    ]
    assert _get_claude_review_run_id(checks) is None


def test_get_claude_review_run_id_no_details_url():
    """Returns None when claude-review has no detailsUrl."""
    checks = [
        {"name": "claude-review", "conclusion": "SUCCESS", "status": "COMPLETED"},
    ]
    assert _get_claude_review_run_id(checks) is None


def test_get_claude_review_run_id_in_progress_returns_none():
    """Returns None when a claude-review run is in progress (new run pending after re-trigger)."""
    checks = [
        {
            "name": "claude-review",
            "conclusion": "SUCCESS",
            "status": "COMPLETED",
            "detailsUrl": "https://github.com/org/repo/actions/runs/12345/job/67890",
        },
        {
            "name": "claude-review",
            "conclusion": None,
            "status": "IN_PROGRESS",
            "detailsUrl": "https://github.com/org/repo/actions/runs/99999/job/11111",
        },
    ]
    assert _get_claude_review_run_id(checks) is None


def test_get_claude_review_run_id_queued_returns_none():
    """Returns None when a claude-review run is queued (new run pending after re-trigger)."""
    checks = [
        {
            "name": "claude-review",
            "conclusion": None,
            "status": "QUEUED",
            "detailsUrl": "https://github.com/org/repo/actions/runs/99999/job/11111",
        },
    ]
    assert _get_claude_review_run_id(checks) is None


# ---------------------------------------------------------------------------
# Review retrigger Redis tracking tests
# ---------------------------------------------------------------------------


def test_review_retrigger_sha_roundtrip(fake_redis_client):
    """set/get/clear review retrigger SHA works correctly."""
    pr_number = 900
    assert get_review_retrigger_sha(fake_redis_client, REPO, pr_number) is None

    set_review_retrigger_sha(fake_redis_client, REPO, pr_number, "abc123")
    assert get_review_retrigger_sha(fake_redis_client, REPO, pr_number) == "abc123"

    clear_review_retrigger(fake_redis_client, REPO, pr_number)
    assert get_review_retrigger_sha(fake_redis_client, REPO, pr_number) is None


def test_review_retrigger_sha_has_ttl(fake_redis_client):
    """set_review_retrigger_sha sets a 7-day TTL."""
    pr_number = 901
    set_review_retrigger_sha(fake_redis_client, REPO, pr_number, "sha1")

    key = f"pr:{REPO}:{pr_number}:review_retrigger"
    ttl = fake_redis_client.ttl(key)
    expected_ttl = 7 * 24 * 3600
    assert 0 < ttl <= expected_ttl


# ---------------------------------------------------------------------------
# _check_stale_pending unit tests
# ---------------------------------------------------------------------------


def _stale_ts(hours_ago: float = 3.0) -> str:
    """Return an ISO 8601 timestamp that is `hours_ago` hours in the past."""
    ts = datetime.now(timezone.utc) - timedelta(hours=hours_ago)
    return ts.isoformat()


def _fresh_ts(minutes_ago: float = 30.0) -> str:
    """Return an ISO 8601 timestamp that is `minutes_ago` minutes in the past."""
    ts = datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)
    return ts.isoformat()


def test_check_stale_pending_all_stale_with_run_ids():
    """All pending checks are stale and have detailsUrl — returns (True, [run_ids])."""
    checks = [
        {
            "name": "build",
            "startedAt": _stale_ts(3),
            "detailsUrl": "https://github.com/org/repo/actions/runs/11111/job/1",
        },
        {
            "name": "test",
            "startedAt": _stale_ts(4),
            "detailsUrl": "https://github.com/org/repo/actions/runs/22222/job/2",
        },
    ]
    all_stale, run_ids = _check_stale_pending(checks, timeout_seconds=7200)
    assert all_stale is True
    assert sorted(run_ids) == [11111, 22222]


def test_check_stale_pending_one_check_fresh():
    """One fresh check prevents re-triggering — returns (False, [])."""
    checks = [
        {
            "name": "build",
            "startedAt": _stale_ts(3),
            "detailsUrl": "https://github.com/org/repo/actions/runs/11111/job/1",
        },
        {
            "name": "deploy",
            "startedAt": _fresh_ts(30),
            "detailsUrl": "https://github.com/org/repo/actions/runs/33333/job/3",
        },
    ]
    all_stale, run_ids = _check_stale_pending(checks, timeout_seconds=7200)
    assert all_stale is False
    assert run_ids == []


def test_check_stale_pending_no_timestamp_is_conservative():
    """A check without a timestamp is treated as non-stale — returns (False, [])."""
    checks = [
        {
            "name": "build",
            "detailsUrl": "https://github.com/org/repo/actions/runs/11111/job/1",
        },
    ]
    all_stale, run_ids = _check_stale_pending(checks, timeout_seconds=7200)
    assert all_stale is False
    assert run_ids == []


def test_check_stale_pending_stale_but_no_run_id():
    """Stale checks with no extractable run ID return (True, [])."""
    checks = [
        {
            "name": "ci/external",
            "state": "PENDING",
            "createdAt": _stale_ts(3),
            # No detailsUrl — StatusContext check
        },
    ]
    all_stale, run_ids = _check_stale_pending(checks, timeout_seconds=7200)
    assert all_stale is True
    assert run_ids == []


def test_check_stale_pending_deduplicates_run_ids():
    """Multiple jobs from the same workflow run produce a single run ID."""
    checks = [
        {
            "name": "job-1",
            "startedAt": _stale_ts(3),
            "detailsUrl": "https://github.com/org/repo/actions/runs/99999/job/1",
        },
        {
            "name": "job-2",
            "startedAt": _stale_ts(3),
            "detailsUrl": "https://github.com/org/repo/actions/runs/99999/job/2",
        },
    ]
    all_stale, run_ids = _check_stale_pending(checks, timeout_seconds=7200)
    assert all_stale is True
    assert run_ids == [99999]


def test_check_stale_pending_uses_created_at_for_status_context():
    """StatusContext checks use createdAt as their age timestamp."""
    checks = [
        {
            "name": "ci/external",
            "state": "PENDING",
            "createdAt": _stale_ts(3),
            "targetUrl": "https://example.com/build/123",
        },
    ]
    all_stale, run_ids = _check_stale_pending(checks, timeout_seconds=7200)
    assert all_stale is True
    assert run_ids == []  # No GitHub Actions run ID in targetUrl


# ---------------------------------------------------------------------------
# discover_actionable_prs — stale pending check tests
# ---------------------------------------------------------------------------


def test_retrigger_stale_checks_when_all_pending_stale(gh_mock, fake_redis_client, label_config):
    """All pending checks exceed the timeout → RETRIGGER_STALE_CHECKS with run IDs."""
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=1000, labels=[]),
    ]
    gh_mock.get_ci_status.return_value = [
        {
            "name": "build",
            "startedAt": _stale_ts(3),
            "detailsUrl": "https://github.com/org/repo/actions/runs/55555/job/1",
        },
    ]

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
        stale_pending_timeout_seconds=7200,
    )

    assert len(results) == 1
    pr = results[0]
    assert pr.action == PRAction.RETRIGGER_STALE_CHECKS
    assert pr.number == 1000
    assert pr.stale_run_ids == [55555]


def test_skip_pending_when_checks_are_fresh(gh_mock, fake_redis_client, label_config):
    """Pending checks within timeout window → SKIP_PENDING (not re-triggered)."""
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=1001, labels=[]),
    ]
    gh_mock.get_ci_status.return_value = [
        {
            "name": "build",
            "startedAt": _fresh_ts(30),
            "detailsUrl": "https://github.com/org/repo/actions/runs/66666/job/1",
        },
    ]

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
        stale_pending_timeout_seconds=7200,
    )

    assert len(results) == 1
    assert results[0].action == PRAction.SKIP_PENDING


def test_skip_pending_when_no_timestamp_first_seen_is_fresh(
    gh_mock, fake_redis_client, label_config
):
    """Timestamp-less pending check is first observed as fresh."""
    check = {
        "name": "build",
        # No startedAt or createdAt
    }
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=1002, labels=[]),
    ]
    gh_mock.get_ci_status.return_value = [check]

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
        stale_pending_timeout_seconds=7200,
    )

    assert len(results) == 1
    assert results[0].action == PRAction.SKIP_PENDING
    key = _make_pending_check_first_seen_key(REPO, 1002, "abc123", check)
    assert fake_redis_client.get(key) is not None


def test_retrigger_stale_checks_when_no_timestamp_first_seen_is_stale(
    gh_mock, fake_redis_client, label_config
):
    """Timestamp-less pending check uses Redis first-seen time to become stale."""
    check = {
        "name": "build",
        "detailsUrl": "https://github.com/org/repo/actions/runs/12345/job/1",
        # No startedAt or createdAt
    }
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=1005, labels=[], head_sha="abc123"),
    ]
    gh_mock.get_ci_status.return_value = [check]
    key = _make_pending_check_first_seen_key(REPO, 1005, "abc123", check)
    fake_redis_client.set_ex(key, _stale_ts(3), 86400)

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
        stale_pending_timeout_seconds=7200,
    )

    assert len(results) == 1
    pr = results[0]
    assert pr.action == PRAction.RETRIGGER_STALE_CHECKS
    assert pr.stale_run_ids == [12345]


def test_expected_pending_check_without_timestamp_first_seen_is_fresh(
    gh_mock, fake_redis_client, label_config
):
    """EXPECTED StatusContext without timestamp starts as SKIP_PENDING."""
    check = {
        "name": "ci/external",
        "state": "EXPECTED",
        # No startedAt or createdAt
    }
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=1006, labels=[], head_sha="def456"),
    ]
    gh_mock.get_ci_status.return_value = [check]

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
        stale_pending_timeout_seconds=7200,
    )

    assert len(results) == 1
    assert results[0].action == PRAction.SKIP_PENDING


def test_expected_pending_check_without_timestamp_first_seen_is_stale(
    gh_mock, fake_redis_client, label_config
):
    """EXPECTED StatusContext without timestamp eventually leaves SKIP_PENDING."""
    check = {
        "name": "ci/external",
        "state": "EXPECTED",
        # No startedAt or createdAt
    }
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=1007, labels=[], head_sha="ghi789"),
    ]
    gh_mock.get_ci_status.return_value = [check]
    key = _make_pending_check_first_seen_key(REPO, 1007, "ghi789", check)
    fake_redis_client.set_ex(key, _stale_ts(3), 86400)

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
        stale_pending_timeout_seconds=7200,
    )

    assert len(results) == 1
    pr = results[0]
    assert pr.action == PRAction.RETRIGGER_STALE_CHECKS
    assert pr.stale_run_ids == []


def test_retrigger_stale_checks_no_run_ids(gh_mock, fake_redis_client, label_config):
    """Stale StatusContext checks (no GitHub Actions URL) → RETRIGGER_STALE_CHECKS, empty list."""
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=1003, labels=[]),
    ]
    gh_mock.get_ci_status.return_value = [
        {
            "name": "ci/jenkins",
            "state": "PENDING",
            "createdAt": _stale_ts(3),
            # No GitHub Actions detailsUrl
        },
    ]

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
        stale_pending_timeout_seconds=7200,
    )

    assert len(results) == 1
    pr = results[0]
    assert pr.action == PRAction.RETRIGGER_STALE_CHECKS
    assert pr.stale_run_ids == []


def test_skip_pending_when_one_check_fresh_one_stale(gh_mock, fake_redis_client, label_config):
    """Mix of fresh and stale pending checks → SKIP_PENDING (not all stale)."""
    gh_mock.list_open_prs.return_value = [
        _make_pr_data(number=1004, labels=[]),
    ]
    gh_mock.get_ci_status.return_value = [
        {
            "name": "build",
            "startedAt": _stale_ts(3),
            "detailsUrl": "https://github.com/org/repo/actions/runs/77777/job/1",
        },
        {
            "name": "deploy",
            "startedAt": _fresh_ts(30),
            "detailsUrl": "https://github.com/org/repo/actions/runs/88888/job/2",
        },
    ]

    results = discover_actionable_prs(
        repo="test-org/test-repo",
        token="fake-token",
        redis=fake_redis_client,
        label_config=label_config,
        stale_pending_timeout_seconds=7200,
    )

    assert len(results) == 1
    assert results[0].action == PRAction.SKIP_PENDING


# --- Transient attempt counter tests ---


def test_transient_attempt_count_starts_at_zero(fake_redis_client):
    """get_transient_attempt_count returns 0 for an unknown PR."""
    assert get_transient_attempt_count(fake_redis_client, REPO, 1000, "abc") == 0


def test_increment_transient_attempts_increments(fake_redis_client):
    """increment_transient_attempts increments and returns the new count."""
    pr_number = 1001
    sha = "sha-aaa"
    assert increment_transient_attempts(fake_redis_client, REPO, pr_number, sha) == 1
    assert increment_transient_attempts(fake_redis_client, REPO, pr_number, sha) == 2
    assert increment_transient_attempts(fake_redis_client, REPO, pr_number, sha) == 3
    assert get_transient_attempt_count(fake_redis_client, REPO, pr_number, sha) == 3


def test_transient_attempts_reset_on_new_sha(fake_redis_client):
    """Transient counter resets to 1 when head SHA changes (new commits)."""
    pr_number = 1002
    sha_v1 = "sha-111"
    sha_v2 = "sha-222"

    increment_transient_attempts(fake_redis_client, REPO, pr_number, sha_v1)
    increment_transient_attempts(fake_redis_client, REPO, pr_number, sha_v1)
    assert get_transient_attempt_count(fake_redis_client, REPO, pr_number, sha_v1) == 2

    # New SHA: counter resets
    new_count = increment_transient_attempts(fake_redis_client, REPO, pr_number, sha_v2)
    assert new_count == 1
    assert get_transient_attempt_count(fake_redis_client, REPO, pr_number, sha_v2) == 1
    # Old SHA now returns 0 (key was reset)
    assert get_transient_attempt_count(fake_redis_client, REPO, pr_number, sha_v1) == 0


def test_transient_attempts_has_ttl(fake_redis_client):
    """increment_transient_attempts sets a 7-day TTL on the key."""
    pr_number = 1003
    increment_transient_attempts(fake_redis_client, REPO, pr_number, "sha-ttl")

    key = f"pr:{REPO}:{pr_number}:transient_attempts"
    ttl = fake_redis_client.ttl(key)
    expected_ttl = 7 * 24 * 3600
    assert 0 < ttl <= expected_ttl


def test_transient_attempts_independent_of_main_attempts(fake_redis_client):
    """Transient counter does not affect the main per-SHA attempt counter."""
    pr_number = 1004
    sha = "sha-ind"

    increment_transient_attempts(fake_redis_client, REPO, pr_number, sha)
    increment_transient_attempts(fake_redis_client, REPO, pr_number, sha)

    # Main attempt counter should still be 0
    assert get_attempt_count(fake_redis_client, REPO, pr_number, sha) == 0
