"""Tests for orcest.orchestrator.loop internals (_poll_cycle, _consume_results).

Uses mocker (pytest-mock) to patch high-level functions called by the loop,
and fake_redis_client for result stream operations.
"""

import json
import logging
import time

import pytest

from orcest.orchestrator.issue_ops import (
    get_attempt_count as get_issue_attempt_count,
    has_usage_exhausted_cooldown as has_issue_usage_exhausted_cooldown,
    increment_attempts as increment_issue_attempts,
)
from orcest.orchestrator.loop import (
    _MAX_REVIEW_RERUN_FAILURES,
    _SHARED_CREDENTIAL_OVERRIDES_KEY,
    _TASK_PROVIDER_ACCOUNT_PREFIX,
    _TASK_PROVIDER_ACCOUNTS_KEY,
    _USAGE_EXHAUSTED_RESULT_KEY,
    RESULTS_GROUP,
    RESULTS_STREAM,
    _consume_results_for_project,
    _handle_result,
    _is_merge_conflict_error,
    _is_required_checks_expected_error,
    _is_stale_head_error,
    _make_review_rerun_failure_cooldown_key,
    _mark_usage_exhausted_token,
    _merge_status_indicates_conflict,
    _poll_cycle,
    _poll_project,
    _RetryableResultError,
)
from orcest.orchestrator.pr_ops import (
    PRAction,
    PRState,
    get_attempt_count,
    get_stale_retrigger_sha,
    get_total_attempt_count,
    has_usage_exhausted_cooldown,
    increment_attempts,
    increment_total_attempts,
    set_stale_retrigger_sha,
)
from orcest.shared.config import OrchestratorConfig, ProjectConfig
from orcest.shared.coordination import (
    clear_backoff,
    clear_pending_task_if_matches,
    get_backoff_head_sha,
    get_backoff_step,
    get_pending_task,
    get_transient_failure_count,
    increment_transient_failure_count,
    set_pending_task,
)
from orcest.shared.models import CONSUMER_GROUP, ResultStatus, TaskResult
from orcest.shared.redis_client import RedisClient


def _consume_results(config: OrchestratorConfig, redis, logger):
    """Compat wrapper: calls _consume_results_for_project with the first project."""
    project = config.projects[0]
    _consume_results_for_project(
        project,
        redis,
        config.labels,
        logger,
        max_transient_failures=config.max_transient_failures,
    )


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
    resource_type: str = "pr",
    resource_id: int | None = None,
    rate_limit_resets_at: int = 0,
    snapshot_head_sha: str | None = None,
    needs_human: bool = False,
    needs_human_reason: str = "",
) -> TaskResult:
    """Build a TaskResult for result-handling tests.

    If ``resource_id`` is ``None`` (whether omitted or explicitly passed) and
    ``resource_type`` is ``"pr"``, the ``pr_number`` default is used.  For any
    other ``resource_type`` an explicit ``resource_id`` must be supplied;
    passing ``None`` raises ``ValueError`` to prevent silently constructing a
    TaskResult with a mismatched id.
    """
    if resource_id is None and resource_type != "pr":
        raise ValueError(f"resource_id must be provided when resource_type={resource_type!r}")
    return TaskResult(
        task_id=task_id,
        worker_id=worker_id,
        status=status,
        branch=branch,
        summary=summary,
        duration_seconds=duration,
        resource_type=resource_type,
        resource_id=resource_id if resource_id is not None else pr_number,
        rate_limit_resets_at=rate_limit_resets_at,
        snapshot_head_sha=(
            snapshot_head_sha
            if snapshot_head_sha is not None
            else ("abc123" if resource_type == "pr" else "")
        ),
        needs_human=needs_human,
        needs_human_reason=needs_human_reason,
    )


# ---------------------------------------------------------------------------
# _poll_cycle tests
# ---------------------------------------------------------------------------


def test_is_merge_conflict_error_matches_gh_merge_commit_failure():
    """The fallback gh CLI text classifier matches non-clean merge commits."""
    msg = (
        "gh command failed (exit 1): X Pull request #2638 is not mergeable: "
        "the merge commit cannot be cleanly created.\n"
        "To have the pull request merged after all the requirements have been met,"
    )

    assert _is_merge_conflict_error(msg)


def test_merge_status_indicates_conflict_for_github_states():
    """GitHub's structured merge states drive conflict routing."""
    assert _merge_status_indicates_conflict(
        {"mergeable": "CONFLICTING", "mergeStateStatus": "BLOCKED"}
    )
    assert _merge_status_indicates_conflict({"mergeable": "MERGEABLE", "mergeStateStatus": "DIRTY"})
    assert (
        _merge_status_indicates_conflict({"mergeable": "MERGEABLE", "mergeStateStatus": "CLEAN"})
        is False
    )
    assert (
        _merge_status_indicates_conflict({"mergeable": "UNKNOWN", "mergeStateStatus": "UNKNOWN"})
        is None
    )


@pytest.fixture(autouse=True)
def _mock_issue_discovery(mocker):
    """Mock discover_actionable_issues for all _poll_cycle tests to avoid real gh calls."""
    mocker.patch("orcest.orchestrator.loop.discover_actionable_issues", return_value=[])


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
    _poll_cycle(orchestrator_config, fake_redis_client, fake_redis_client, {}, logger, 3600)

    mock_publish.assert_called_once()
    assert mock_publish.call_args.kwargs["pr_state"] is pr_state


def test_poll_cycle_routes_legacy_claude_credentials_to_default_clauder(
    mocker,
    fake_redis_client,
    orchestrator_config,
    gh_mock,
):
    """Legacy Claude credentials should publish to clauder when that is the project default."""
    from orcest.orchestrator.provider_pool import ProviderPool
    from orcest.shared.providers import ProviderEntry

    pr_state = _make_pr_state(number=11, action=PRAction.ENQUEUE_FIX)
    orchestrator_config.default_runner = "clauder"
    entry = ProviderEntry(
        provider="claude",
        credential="legacy-claude-token",
        source="legacy_claude_tokens",
    )
    pool = ProviderPool([entry])

    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        return_value=[pr_state],
    )
    mock_publish = mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    project_key = orchestrator_config.projects[0].key_prefix
    logger = logging.getLogger("test")
    _poll_cycle(
        orchestrator_config,
        fake_redis_client,
        fake_redis_client,
        {project_key: pool},
        logger,
        3600,
    )

    mock_publish.assert_called_once()
    assert mock_publish.call_args.kwargs["provider"] == "clauder"
    assert mock_publish.call_args.kwargs["credential"] == "legacy-claude-token"
    assert mock_publish.call_args.kwargs["claude_token"] == "legacy-claude-token"


def test_poll_cycle_routes_provider_pool_legacy_helper_to_default_clauder(
    mocker,
    fake_redis_client,
    orchestrator_config,
    gh_mock,
):
    """ProviderPool.from_claude_tokens must keep legacy tokens on the configured default."""
    from orcest.orchestrator.provider_pool import ProviderPool

    pr_state = _make_pr_state(number=14, action=PRAction.ENQUEUE_FIX)
    orchestrator_config.default_runner = "clauder"
    pool = ProviderPool.from_claude_tokens(["legacy-helper-token"])

    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        return_value=[pr_state],
    )
    mock_publish = mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    project_key = orchestrator_config.projects[0].key_prefix
    logger = logging.getLogger("test")
    _poll_cycle(
        orchestrator_config,
        fake_redis_client,
        fake_redis_client,
        {project_key: pool},
        logger,
        3600,
    )

    mock_publish.assert_called_once()
    assert mock_publish.call_args.kwargs["provider"] == "clauder"
    assert mock_publish.call_args.kwargs["credential"] == "legacy-helper-token"
    assert mock_publish.call_args.kwargs["claude_token"] == "legacy-helper-token"


def test_poll_project_routes_single_legacy_token_fallback_to_default_clauder(
    mocker,
    fake_redis_client,
    orchestrator_config,
    gh_mock,
):
    """The no-pool single-token fallback must use the configured Claude runner."""
    pr_state = _make_pr_state(number=15, action=PRAction.ENQUEUE_FIX)
    orchestrator_config.default_runner = "clauder"
    project = orchestrator_config.projects[0]
    project.providers = []
    project.claude_tokens = ["legacy-single-token"]

    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        return_value=[pr_state],
    )
    # The issue-discovery gate reads per-stream counts; patching the aggregate
    # helper would no longer disable it.
    mocker.patch("orcest.orchestrator.loop._unclaimed_task_counts", return_value={})
    mock_publish = mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")

    _poll_project(
        project,
        fake_redis_client,
        fake_redis_client,
        orchestrator_config,
        logging.getLogger("test"),
        3600,
        token_pool=None,
    )

    mock_publish.assert_called_once()
    assert mock_publish.call_args.kwargs["provider"] == "clauder"
    assert mock_publish.call_args.kwargs["credential"] == "legacy-single-token"
    assert mock_publish.call_args.kwargs["claude_token"] == "legacy-single-token"


def test_poll_cycle_routes_explicit_claude_to_default_clauder(
    mocker,
    fake_redis_client,
    orchestrator_config,
    gh_mock,
):
    """An explicit YAML `provider: claude` entry follows the clauder default.

    Worker fleets baked around the default runner consume `tasks:clauder`
    only; publishing explicit claude credentials to `tasks:claude` would
    strand those tasks forever with a green preflight.
    """
    from orcest.orchestrator.provider_pool import ProviderPool
    from orcest.shared.providers import ProviderEntry

    pr_state = _make_pr_state(number=13, action=PRAction.ENQUEUE_FIX)
    orchestrator_config.default_runner = "clauder"
    entry = ProviderEntry(provider="claude", credential="explicit-claude-token")
    pool = ProviderPool([entry])

    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        return_value=[pr_state],
    )
    mock_publish = mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    project_key = orchestrator_config.projects[0].key_prefix
    logger = logging.getLogger("test")
    _poll_cycle(
        orchestrator_config,
        fake_redis_client,
        fake_redis_client,
        {project_key: pool},
        logger,
        3600,
    )

    mock_publish.assert_called_once()
    assert mock_publish.call_args.kwargs["provider"] == "clauder"
    assert mock_publish.call_args.kwargs["credential"] == "explicit-claude-token"
    assert mock_publish.call_args.kwargs["claude_token"] == "explicit-claude-token"


def test_poll_cycle_preserves_explicit_clauder_when_default_runner_is_legacy_claude(
    mocker,
    fake_redis_client,
    orchestrator_config,
    gh_mock,
):
    """An explicit clauder provider must not be downgraded to tasks:claude."""
    from orcest.orchestrator.provider_pool import ProviderPool
    from orcest.shared.providers import ProviderEntry

    pr_state = _make_pr_state(number=12, action=PRAction.ENQUEUE_FIX)
    orchestrator_config.default_runner = "claude"
    entry = ProviderEntry(provider="clauder", credential="interactive-claude-token")
    pool = ProviderPool([entry])

    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        return_value=[pr_state],
    )
    mock_publish = mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    project_key = orchestrator_config.projects[0].key_prefix
    logger = logging.getLogger("test")
    _poll_cycle(
        orchestrator_config,
        fake_redis_client,
        fake_redis_client,
        {project_key: pool},
        logger,
        3600,
    )

    mock_publish.assert_called_once()
    assert mock_publish.call_args.kwargs["provider"] == "clauder"
    assert mock_publish.call_args.kwargs["credential"] == "interactive-claude-token"
    assert mock_publish.call_args.kwargs["claude_token"] == "interactive-claude-token"


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
    _poll_cycle(orchestrator_config, fake_redis_client, fake_redis_client, {}, logger, 3600)

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
    _poll_cycle(orchestrator_config, fake_redis_client, fake_redis_client, {}, logger, 3600)

    mock_followup.assert_called_once()
    assert mock_followup.call_args.kwargs["pr_state"] is pr_state


def test_poll_cycle_calls_update_branch_for_behind_pr(
    mocker, fake_redis_client, orchestrator_config, gh_mock
):
    """_poll_cycle calls gh.update_branch for PRs with UPDATE_BRANCH action — no
    worker, no Claude token; the orchestrator handles it directly."""
    pr_state = PRState(
        number=70,
        title="PR #70",
        branch="feat/70",
        head_sha="abc123",
        action=PRAction.UPDATE_BRANCH,
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
    mock_publish_rebase = mocker.patch("orcest.orchestrator.loop.publish_rebase_task")
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    logger = logging.getLogger("test")
    _poll_cycle(orchestrator_config, fake_redis_client, fake_redis_client, {}, logger, 3600)

    gh_mock.update_branch.assert_called_once_with(
        orchestrator_config.github.repo,
        70,
        orchestrator_config.github.token,
        expected_head_sha="abc123",
    )
    # No worker task should be published — update-branch is orchestrator-side.
    mock_publish_rebase.assert_not_called()
    gh_mock.merge_pr.assert_not_called()


def test_poll_cycle_update_branch_stale_head_skips_needs_human(
    mocker, fake_redis_client, orchestrator_config, gh_mock
):
    """A stale discovered head during update-branch is retried next poll without escalation."""
    pr_state = PRState(
        number=71,
        title="PR #71",
        branch="feat/71",
        head_sha="abc123",
        action=PRAction.UPDATE_BRANCH,
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
    gh_mock.update_branch.side_effect = RuntimeError(
        "Head branch was modified. Review and try again."
    )
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    logger = logging.getLogger("test")
    _poll_cycle(orchestrator_config, fake_redis_client, fake_redis_client, {}, logger, 3600)

    gh_mock.update_branch.assert_called_once_with(
        orchestrator_config.github.repo,
        71,
        orchestrator_config.github.token,
        expected_head_sha="abc123",
    )
    gh_mock.add_label.assert_not_called()
    gh_mock.post_comment.assert_not_called()


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
    # Pre-populate total_attempts so we can verify it is cleared on merge
    repo = orchestrator_config.github.repo
    increment_total_attempts(fake_redis_client, repo, 40)
    assert get_total_attempt_count(fake_redis_client, repo, 40) == 1

    logger = logging.getLogger("test")
    _poll_cycle(orchestrator_config, fake_redis_client, fake_redis_client, {}, logger, 3600)

    gh_mock.merge_pr.assert_called_once_with(
        orchestrator_config.github.repo,
        40,
        orchestrator_config.github.token,
        delete_branch=orchestrator_config.delete_branch_on_merge,
        head_sha="abc123",
    )
    # On successful merge, should post a confirmation comment
    gh_mock.post_comment.assert_called_once()
    comment_body = gh_mock.post_comment.call_args[0][2]
    assert "merged" in comment_body
    # Critical invariant: total_attempts must be cleared when PR is merged
    assert get_total_attempt_count(fake_redis_client, repo, 40) == 0


def test_poll_cycle_merge_conflict_enqueues_rebase(
    mocker,
    fake_redis_client,
    orchestrator_config,
    gh_mock,
):
    """When merge fails due to conflicts, a worker rebase task is enqueued."""
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
    mock_publish_rebase = mocker.patch("orcest.orchestrator.loop.publish_rebase_task")
    mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    gh_mock.merge_pr.side_effect = RuntimeError("merge conflict")
    gh_mock.get_pr.return_value = {"mergeable": "CONFLICTING", "mergeStateStatus": "DIRTY"}
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    logger = logging.getLogger("test")
    _poll_cycle(orchestrator_config, fake_redis_client, fake_redis_client, {}, logger, 3600)

    mock_publish_rebase.assert_called_once()
    gh_mock.add_label.assert_not_called()
    gh_mock.post_comment.assert_not_called()


def test_poll_cycle_non_conflict_merge_failure_backs_off(
    mocker,
    fake_redis_client,
    orchestrator_config,
    gh_mock,
):
    """A non-conflict merge failure backs off and retries -- it is never
    escalated to needs-human."""
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
    gh_mock.merge_pr.side_effect = RuntimeError("repository rule violation")
    gh_mock.get_pr.return_value = {"mergeable": "MERGEABLE", "mergeStateStatus": "CLEAN"}
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    logger = logging.getLogger("test")
    _poll_cycle(orchestrator_config, fake_redis_client, fake_redis_client, {}, logger, 3600)

    gh_mock.add_label.assert_not_called()
    gh_mock.post_comment.assert_not_called()
    # A backoff cooldown was set so the merge is retried later.
    assert get_backoff_step(fake_redis_client, orchestrator_config.projects[0].repo, 41) is not None


def test_poll_cycle_merge_stale_head_skips_needs_human(
    mocker,
    fake_redis_client,
    orchestrator_config,
    gh_mock,
):
    """A stale discovered head during merge is retried next poll without escalation."""
    pr_state = PRState(
        number=43,
        title="PR #43",
        branch="feat/43",
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
    gh_mock.merge_pr.side_effect = RuntimeError(
        "Head commit changed since discovery; match-head-commit rejected the merge"
    )
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    logger = logging.getLogger("test")
    _poll_cycle(orchestrator_config, fake_redis_client, fake_redis_client, {}, logger, 3600)

    gh_mock.merge_pr.assert_called_once_with(
        orchestrator_config.github.repo,
        43,
        orchestrator_config.github.token,
        delete_branch=orchestrator_config.delete_branch_on_merge,
        head_sha="abc123",
    )
    gh_mock.add_label.assert_not_called()
    gh_mock.post_comment.assert_not_called()


def test_poll_cycle_merge_conflict_token_exhausted_skips_needs_human(
    mocker,
    fake_redis_client,
    orchestrator_config,
    gh_mock,
):
    """Merge conflicts skipped for Claude capacity do not get generic needs-human handling."""
    from datetime import datetime, timedelta, timezone

    from orcest.orchestrator.provider_pool import ProviderPool
    from orcest.shared.providers import ProviderEntry

    pr_state = PRState(
        number=42,
        title="PR #42",
        branch="feat/42",
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
    mock_publish_rebase = mocker.patch("orcest.orchestrator.loop.publish_rebase_task")
    mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    gh_mock.merge_pr.side_effect = RuntimeError("Pull request is not mergeable")
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    # Use ProviderPool + public API (register+mark) for hardened exhaustion simulation.
    entry = ProviderEntry(provider="claude", credential="sk-test-token")
    pool = ProviderPool([entry])
    pool.register_task("t-exh", entry)
    pool.mark_exhausted("t-exh", resets_at=datetime.now(timezone.utc) + timedelta(minutes=30))
    project_key = orchestrator_config.projects[0].key_prefix

    logger = logging.getLogger("test")
    _poll_cycle(
        orchestrator_config,
        fake_redis_client,
        fake_redis_client,
        {project_key: pool},
        logger,
        3600,
    )

    mock_publish_rebase.assert_not_called()
    gh_mock.add_label.assert_not_called()
    gh_mock.post_comment.assert_not_called()


def test_poll_cycle_provider_pool_wiring_registers_and_rolls_back_on_publish_none(
    mocker,
    fake_redis_client,
    orchestrator_config,
    gh_mock,
):
    """Supplies real non-exhausted ProviderPool; verifies register_task called and
    task_completed invoked when a publish_* returns None (dedup/no-enqueue path).
    Covers the happy wiring path (most prior tests used empty {} legacy token_pools).
    """
    from orcest.orchestrator.provider_pool import ProviderPool
    from orcest.shared.providers import ProviderEntry

    pr_state = _make_pr_state(number=99, action=PRAction.ENQUEUE_FIX)

    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        return_value=[pr_state],
    )
    mock_publish_fix = mocker.patch(
        "orcest.orchestrator.loop.publish_fix_task",
        return_value=None,  # triggers the rollback path in _try_publish
    )
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    mocker.patch("orcest.orchestrator.loop.publish_rebase_task")
    mocker.patch("orcest.orchestrator.loop.publish_issue_task")
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    entry = ProviderEntry(provider="claude", credential="tok-wiring-test-42")
    pool = ProviderPool([entry])
    reg_spy = mocker.spy(pool, "register_task")
    comp_spy = mocker.spy(pool, "task_completed")

    project_key = orchestrator_config.projects[0].key_prefix
    logger = logging.getLogger("test")
    _poll_cycle(
        orchestrator_config,
        fake_redis_client,
        fake_redis_client,
        {project_key: pool},
        logger,
        3600,
    )

    # register_task called exactly once with generated task_id + the entry
    assert reg_spy.call_count == 1
    reg_call = reg_spy.call_args[0]
    assert len(reg_call) == 2  # (task_id, entry) for bound method spy
    registered_entry = reg_call[1]
    assert registered_entry.provider == "claude"
    assert registered_entry.credential == "tok-wiring-test-42"

    # since publish returned None, _try_publish performed rollback
    assert comp_spy.call_count == 1

    mock_publish_fix.assert_called_once()
    assert mock_publish_fix.call_args.kwargs["pr_state"] is pr_state


def test_poll_cycle_skip_max_attempts_backs_off(
    mocker,
    fake_redis_client,
    orchestrator_config,
    gh_mock,
):
    """SKIP_MAX_ATTEMPTS backs off and retries -- it never labels needs-human."""
    pr_state = _make_pr_state(number=50, action=PRAction.SKIP_MAX_ATTEMPTS)

    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        return_value=[pr_state],
    )
    mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    logger = logging.getLogger("test")
    _poll_cycle(orchestrator_config, fake_redis_client, fake_redis_client, {}, logger, 3600)

    gh_mock.add_label.assert_not_called()
    gh_mock.post_comment.assert_not_called()
    assert get_backoff_step(fake_redis_client, orchestrator_config.projects[0].repo, 50) is not None


def test_poll_cycle_skip_max_total_attempts_backs_off_and_resets(
    mocker,
    fake_redis_client,
    orchestrator_config,
    gh_mock,
):
    """SKIP_MAX_TOTAL_ATTEMPTS backs off, resets the total counter so work
    resumes, and never labels needs-human."""
    repo = orchestrator_config.projects[0].repo
    pr_state = _make_pr_state(number=53, action=PRAction.SKIP_MAX_TOTAL_ATTEMPTS)
    for _ in range(orchestrator_config.max_total_attempts):
        increment_total_attempts(fake_redis_client, repo, 53)

    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        return_value=[pr_state],
    )
    mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    logger = logging.getLogger("test")
    _poll_cycle(orchestrator_config, fake_redis_client, fake_redis_client, {}, logger, 3600)

    gh_mock.add_label.assert_not_called()
    gh_mock.post_comment.assert_not_called()
    assert get_backoff_step(fake_redis_client, repo, 53) is not None
    # The total-attempt counter is reset so the PR resumes after the cooldown.
    assert get_total_attempt_count(fake_redis_client, repo, 53) == 0


def test_poll_cycle_skip_backoff_no_label_no_comment(
    mocker,
    fake_redis_client,
    orchestrator_config,
    gh_mock,
):
    """SKIP_BACKOFF simply logs — no labels added, no comments posted."""
    pr_state = _make_pr_state(number=51, action=PRAction.SKIP_BACKOFF)

    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        return_value=[pr_state],
    )
    mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    logger = logging.getLogger("test")
    _poll_cycle(orchestrator_config, fake_redis_client, fake_redis_client, {}, logger, 3600)

    # No labels should be added
    gh_mock.add_label.assert_not_called()
    # No comments should be posted
    gh_mock.post_comment.assert_not_called()


def test_poll_cycle_increments_token_exhausted_skip_counter(
    mocker,
    fake_redis_client,
    orchestrator_config,
    gh_mock,
):
    """When all entries in the ProviderPool are cooling, _select_provider_entry increments
    the per-project Redis counter (legacy key name) so operators can detect the situation."""
    from datetime import datetime, timedelta, timezone

    from orcest.orchestrator.loop import _PROVIDER_EXHAUSTED_SKIP_KEY
    from orcest.orchestrator.provider_pool import ProviderPool
    from orcest.shared.providers import ProviderEntry

    pr_state = _make_pr_state(number=77, action=PRAction.ENQUEUE_FIX)

    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        return_value=[pr_state],
    )
    publish_mock = mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    # Pool with one entry, marked exhausted via public API so next_entry() returns None.
    entry = ProviderEntry(provider="claude", credential="sk-test-token")
    pool = ProviderPool([entry])
    pool.register_task("t-exh", entry)
    pool.mark_exhausted("t-exh", resets_at=datetime.now(timezone.utc) + timedelta(minutes=30))
    project_key = orchestrator_config.projects[0].key_prefix

    logger = logging.getLogger("test")
    _poll_cycle(
        orchestrator_config,
        fake_redis_client,
        fake_redis_client,
        {project_key: pool},
        logger,
        3600,
    )

    # Counter incremented and TTL set, no task published.
    publish_mock.assert_not_called()
    raw_count = fake_redis_client.get(_PROVIDER_EXHAUSTED_SKIP_KEY)
    assert raw_count == "1"
    assert fake_redis_client.ttl(_PROVIDER_EXHAUSTED_SKIP_KEY) > 0

    # Task 8 per-provider counter also incremented for "claude"
    per_prov_count = fake_redis_client.get("providers:claude:exhausted_skip")
    assert per_prov_count == "1"
    assert fake_redis_client.ttl("providers:claude:exhausted_skip") > 0


def test_poll_cycle_token_exhausted_still_reruns_transient_ci(
    mocker,
    fake_redis_client,
    orchestrator_config,
    gh_mock,
):
    """All-transient CI reruns happen before Claude token selection."""
    from datetime import datetime, timedelta, timezone

    from orcest.orchestrator.loop import _PROVIDER_EXHAUSTED_SKIP_KEY
    from orcest.orchestrator.provider_pool import ProviderPool
    from orcest.shared.providers import ProviderEntry

    ci_failures = [
        {
            "name": "timeout-check",
            "conclusion": "TIMED_OUT",
            "detailsUrl": "https://github.com/org/repo/actions/runs/42007/job/1",
        }
    ]
    pr_state = _make_pr_state(number=78, action=PRAction.ENQUEUE_FIX, ci_failures=ci_failures)

    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        return_value=[pr_state],
    )
    publish_mock = mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    gh_mock.get_failed_run_logs.return_value = "connection reset by peer"
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    # ProviderPool via public API
    entry = ProviderEntry(provider="claude", credential="sk-test-token")
    pool = ProviderPool([entry])
    pool.register_task("t-exh", entry)
    pool.mark_exhausted("t-exh", resets_at=datetime.now(timezone.utc) + timedelta(minutes=30))
    project_key = orchestrator_config.projects[0].key_prefix

    logger = logging.getLogger("test")
    _poll_cycle(
        orchestrator_config,
        fake_redis_client,
        fake_redis_client,
        {project_key: pool},
        logger,
        3600,
    )

    gh_mock.rerun_workflow.assert_called_once_with(
        orchestrator_config.github.repo,
        42007,
        orchestrator_config.github.token,
        failed_only=True,
    )
    publish_mock.assert_not_called()
    assert fake_redis_client.get(_PROVIDER_EXHAUSTED_SKIP_KEY) is None


def test_poll_cycle_token_exhausted_code_fix_still_requires_claude_token(
    mocker,
    fake_redis_client,
    orchestrator_config,
    gh_mock,
):
    """Code CI failures do not publish worker tasks when all providers are exhausted."""
    from datetime import datetime, timedelta, timezone

    from orcest.orchestrator.loop import _PROVIDER_EXHAUSTED_SKIP_KEY
    from orcest.orchestrator.provider_pool import ProviderPool
    from orcest.shared.providers import ProviderEntry

    ci_failures = [
        {
            "name": "tests",
            "conclusion": "FAILURE",
            "detailsUrl": "https://github.com/org/repo/actions/runs/42008/job/1",
        }
    ]
    pr_state = _make_pr_state(number=79, action=PRAction.ENQUEUE_FIX, ci_failures=ci_failures)

    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        return_value=[pr_state],
    )
    publish_mock = mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    gh_mock.get_failed_run_logs.return_value = "FAILED test_widget.py::test_case"
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    # ProviderPool via public API
    entry = ProviderEntry(provider="claude", credential="sk-test-token")
    pool = ProviderPool([entry])
    pool.register_task("t-exh", entry)
    pool.mark_exhausted("t-exh", resets_at=datetime.now(timezone.utc) + timedelta(minutes=30))
    project_key = orchestrator_config.projects[0].key_prefix

    logger = logging.getLogger("test")
    _poll_cycle(
        orchestrator_config,
        fake_redis_client,
        fake_redis_client,
        {project_key: pool},
        logger,
        3600,
    )

    gh_mock.rerun_workflow.assert_not_called()
    publish_mock.assert_not_called()
    assert fake_redis_client.get(_PROVIDER_EXHAUSTED_SKIP_KEY) == "1"


def test_poll_cycle_exception_handled(mocker, fake_redis_client, orchestrator_config, gh_mock):
    """When discover_actionable_prs raises, _poll_cycle catches it per-project and continues.

    Per-project error isolation ensures one project's failure doesn't crash others.
    """
    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        side_effect=RuntimeError("GitHub is down"),
    )
    mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    logger = logging.getLogger("test")

    # Should not raise — per-project error isolation catches and logs
    _poll_cycle(orchestrator_config, fake_redis_client, fake_redis_client, {}, logger, 3600)


def test_poll_cycle_trims_acked_task_entries(
    mocker, fake_redis_client, orchestrator_config, gh_mock
):
    """M1-conc: a poll cycle reclaims delivered+ACKed entries from the task
    streams (so credentials don't live forever) but keeps un-ACKed entries."""
    mocker.patch("orcest.orchestrator.loop.discover_actionable_prs", return_value=[])
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    # default_runner defaults to "claude".
    stream = f"tasks:{orchestrator_config.default_runner}"
    fake_redis_client.ensure_consumer_group(stream, CONSUMER_GROUP)
    acked = fake_redis_client.xadd(stream, {"token": "ghp_acked"})
    unacked = fake_redis_client.xadd(stream, {"token": "ghp_unacked"})
    fake_redis_client.xreadgroup(
        group=CONSUMER_GROUP, consumer="c1", stream=stream, count=10, block_ms=None
    )
    fake_redis_client.xack(stream, CONSUMER_GROUP, acked)

    logger = logging.getLogger("test")
    # task_redis is the same fake client as the project redis in these tests.
    _poll_cycle(orchestrator_config, fake_redis_client, fake_redis_client, {}, logger, 3600)

    remaining = [eid for eid, _ in fake_redis_client.xrevrange(stream, count=10)]
    assert acked not in remaining  # ACKed credential entry reclaimed
    assert unacked in remaining  # in-flight work preserved


def test_poll_cycle_trims_provider_specific_task_entries(
    mocker, fake_redis_client, orchestrator_config, gh_mock
):
    """Provider-specific task streams are reclaimed like the legacy default stream."""
    from orcest.shared.providers import ProviderEntry

    mocker.patch("orcest.orchestrator.loop.discover_actionable_prs", return_value=[])
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)
    orchestrator_config.projects[0].providers = [
        ProviderEntry(provider="grok", credential='{"refresh_token":"grok-refresh"}')
    ]

    stream = "tasks:grok"
    fake_redis_client.ensure_consumer_group(stream, CONSUMER_GROUP)
    acked = fake_redis_client.xadd(stream, {"token": "ghp_acked", "credential": "grok-old"})
    unacked = fake_redis_client.xadd(stream, {"token": "ghp_unacked", "credential": "grok-live"})
    fake_redis_client.xreadgroup(
        group=CONSUMER_GROUP, consumer="grok-worker", stream=stream, count=10, block_ms=None
    )
    fake_redis_client.xack(stream, CONSUMER_GROUP, acked)

    logger = logging.getLogger("test")
    _poll_cycle(orchestrator_config, fake_redis_client, fake_redis_client, {}, logger, 3600)

    remaining = [eid for eid, _ in fake_redis_client.xrevrange(stream, count=10)]
    assert acked not in remaining
    assert unacked in remaining


def test_poll_project_issue_gate_counts_provider_specific_issue_stream(
    mocker, fake_redis_client, gh_mock
):
    """Unread provider issue streams defer issue discovery when not on the fairness slot."""
    from orcest.shared.config import LabelConfig
    from orcest.shared.providers import ProviderEntry

    project = ProjectConfig(
        repo="acme/widgets",
        token="ghp-project",
        claude_tokens=[],
        key_prefix="widgets",
        providers=[ProviderEntry(provider="grok", credential='{"refresh_token":"grok-refresh"}')],
    )
    config = OrchestratorConfig(labels=LabelConfig(), projects=[project])
    project_redis = RedisClient.from_client(fake_redis_client.client, key_prefix=project.key_prefix)

    mocker.patch("orcest.orchestrator.loop.discover_actionable_prs", return_value=[])
    issue_discovery = mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_issues",
        return_value=[],
    )
    counted_streams: list[str] = []

    def unread_count(stream: str, group: str) -> int:
        counted_streams.append(stream)
        assert group == CONSUMER_GROUP
        return 1 if stream == "tasks:issue:grok" else 0

    mocker.patch.object(fake_redis_client, "stream_unread_count", side_effect=unread_count)

    _poll_project(
        project,
        project_redis,
        fake_redis_client,
        config,
        logging.getLogger("test"),
        3600,
        token_pool=None,
        force_issue_discovery=False,
    )

    issue_discovery.assert_not_called()
    assert "tasks:issue:grok" in counted_streams


def test_poll_project_issue_gate_ignores_other_providers_dead_stream(
    mocker, fake_redis_client, gh_mock
):
    """One backed-up provider stream must not gate discovery for a healthy one.

    Regression: the gate summed unclaimed entries across every configured
    provider issue stream. Provider streams are isolated -- only a worker whose
    backend matches consumes one -- so a provider with no running workers
    accumulates undelivered entries forever, and the sum stayed above zero
    permanently. That deferred issue discovery for every healthy provider on
    every cycle except the rotating fairness slot, collapsing throughput to
    1/N and defeating the isolation per-provider streams exist to give.
    """
    from orcest.shared.config import LabelConfig
    from orcest.shared.providers import ProviderEntry

    project = ProjectConfig(
        repo="acme/widgets",
        token="ghp-project",
        claude_tokens=[],
        key_prefix="widgets",
        providers=[
            ProviderEntry(provider="clauder", credential="clauder-token"),
            ProviderEntry(provider="grok", credential='{"refresh_token":"grok-refresh"}'),
        ],
    )
    config = OrchestratorConfig(labels=LabelConfig(), projects=[project])
    project_redis = RedisClient.from_client(fake_redis_client.client, key_prefix=project.key_prefix)

    mocker.patch("orcest.orchestrator.loop.discover_actionable_prs", return_value=[])
    issue_discovery = mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_issues",
        return_value=[],
    )

    # grok has no workers, so its stream never drains; clauder is healthy.
    def unread_count(stream: str, group: str) -> int:
        return 7 if stream == "tasks:issue:grok" else 0

    mocker.patch.object(fake_redis_client, "stream_unread_count", side_effect=unread_count)

    _poll_project(
        project,
        project_redis,
        fake_redis_client,
        config,
        logging.getLogger("test"),
        3600,
        token_pool=None,
        force_issue_discovery=False,
    )

    issue_discovery.assert_called_once()


# ---------------------------------------------------------------------------
# _consume_results tests
# ---------------------------------------------------------------------------


def test_consume_results_completed_pr_attempt_guard_is_retry_budget_not_active_work(
    fake_redis_client, orchestrator_config, gh_mock
):
    """A COMPLETED PR result leaves attempt budget but does not imply active work."""
    from orcest.orchestrator import pr_ops

    # Set up consumer group and add a result to the stream
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    repo = orchestrator_config.github.repo
    pr_number = 42
    head_sha = "abc123"
    increment_attempts(fake_redis_client, repo, pr_number, head_sha)
    assert get_attempt_count(fake_redis_client, repo, pr_number, head_sha) == 1

    result = _make_task_result(status=ResultStatus.COMPLETED, pr_number=pr_number)
    fake_redis_client.xadd(RESULTS_STREAM, result.to_dict())

    logger = logging.getLogger("test")
    _consume_results(orchestrator_config, fake_redis_client, logger)

    # Success is silent — no comment posted
    gh_mock.post_comment.assert_not_called()

    # No label operations
    gh_mock.remove_label.assert_not_called()
    gh_mock.add_label.assert_not_called()

    assert get_attempt_count(fake_redis_client, repo, pr_number, head_sha) == 1

    gh_mock.list_open_prs.return_value = [
        {
            "number": pr_number,
            "title": "PR #42",
            "headRefName": "fix/42",
            "headRefOid": head_sha,
            "baseRefName": "main",
            "labels": [],
            "isDraft": False,
            "isLocked": False,
            "mergeable": "MERGEABLE",
            "reviewDecision": "",
        }
    ]
    gh_mock.get_ci_status.return_value = [
        {"name": "tests", "conclusion": "failure"},
    ]

    results = pr_ops.discover_actionable_prs(
        repo=repo,
        token=orchestrator_config.github.token,
        redis=fake_redis_client,
        label_config=orchestrator_config.labels,
    )

    assert len(results) == 1
    assert results[0].action == PRAction.ENQUEUE_FIX


def test_consume_results_completed_issue_clears_attempts(
    fake_redis_client, orchestrator_config, gh_mock
):
    """Completed issue tasks keep the existing issue behavior."""
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    repo = orchestrator_config.github.repo
    issue_number = 43
    increment_issue_attempts(fake_redis_client, repo, issue_number)

    result = _make_task_result(
        status=ResultStatus.COMPLETED,
        resource_type="issue",
        resource_id=issue_number,
        branch="",
    )
    fake_redis_client.xadd(RESULTS_STREAM, result.to_dict())

    logger = logging.getLogger("test")
    _consume_results(orchestrator_config, fake_redis_client, logger)

    assert get_issue_attempt_count(fake_redis_client, repo, issue_number) == 0
    gh_mock.remove_issue_label.assert_called_once()


def test_consume_results_completed_does_not_clear_total_attempts(
    fake_redis_client, orchestrator_config, gh_mock
):
    """A COMPLETED result must NOT clear total_attempts.

    total_attempts is the cross-SHA circuit-breaker counter and should only be
    reset when the PR is truly resolved (merged), not on intermediate successes.
    Regression test for the bug fixed in PR #331 (issue #335).
    """
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    # Pre-populate the cross-SHA counter to simulate prior attempts
    repo = orchestrator_config.github.repo
    increment_total_attempts(fake_redis_client, repo, 42)
    increment_total_attempts(fake_redis_client, repo, 42)
    assert get_total_attempt_count(fake_redis_client, repo, 42) == 2

    result = _make_task_result(status=ResultStatus.COMPLETED, pr_number=42)
    fake_redis_client.xadd(RESULTS_STREAM, result.to_dict())

    logger = logging.getLogger("test")
    _consume_results(orchestrator_config, fake_redis_client, logger)

    # total_attempts must still be non-zero after an intermediate task success
    assert get_total_attempt_count(fake_redis_client, repo, 42) == 2


def test_consume_results_failed(fake_redis_client, orchestrator_config, gh_mock):
    """An ordinary FAILED result is retried silently -- no label, no comment."""
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    result = _make_task_result(status=ResultStatus.FAILED, pr_number=55)
    fake_redis_client.xadd(RESULTS_STREAM, result.to_dict())

    logger = logging.getLogger("test")
    _consume_results(orchestrator_config, fake_redis_client, logger)

    # An ordinary fix-attempt failure is never escalated and never commented on.
    gh_mock.add_label.assert_not_called()
    gh_mock.post_comment.assert_not_called()
    gh_mock.remove_label.assert_not_called()


def test_consume_results_failed_with_needs_human_signal(
    fake_redis_client, orchestrator_config, gh_mock
):
    """A FAILED result carrying the worker's NEEDS_HUMAN signal -- and only
    that -- applies the needs-human label and comments the worker's reason."""
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    result = _make_task_result(
        status=ResultStatus.FAILED,
        pr_number=55,
        summary="Investigated; a product decision is required.",
        needs_human=True,
        needs_human_reason="product owner must choose the canonical role value",
    )
    fake_redis_client.xadd(RESULTS_STREAM, result.to_dict())

    logger = logging.getLogger("test")
    _consume_results(orchestrator_config, fake_redis_client, logger)

    labels = orchestrator_config.labels
    gh_mock.add_label.assert_called_once_with(
        orchestrator_config.github.repo,
        55,
        labels.needs_human,
        orchestrator_config.github.token,
    )
    gh_mock.post_comment.assert_called_once()
    comment_body = gh_mock.post_comment.call_args[0][2]
    assert "product owner must choose the canonical role value" in comment_body


def test_consume_results_transient_failure_no_needs_human(
    fake_redis_client, orchestrator_config, gh_mock
):
    """A transient FAILED result does NOT add needs-human label."""
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    result = _make_task_result(
        status=ResultStatus.FAILED,
        pr_number=55,
        summary="[transient] Worker exception: git clone timed out after 300s",
    )
    fake_redis_client.xadd(RESULTS_STREAM, result.to_dict())

    logger = logging.getLogger("test")
    _consume_results(orchestrator_config, fake_redis_client, logger)

    # Should NOT add needs-human label
    gh_mock.add_label.assert_not_called()

    # Transient failures are retried silently — no comment should be posted
    gh_mock.post_comment.assert_not_called()


def test_consume_results_transient_failure_clears_attempts(
    fake_redis_client, orchestrator_config, gh_mock
):
    """A transient FAILED result clears per-SHA attempts for silent retry."""
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    pr_number = 56
    head_sha = "abc123"
    repo = orchestrator_config.github.repo

    # Simulate a prior attempt on this SHA
    increment_attempts(fake_redis_client, repo, pr_number, head_sha)
    assert get_attempt_count(fake_redis_client, repo, pr_number, head_sha) == 1

    result = _make_task_result(
        status=ResultStatus.FAILED,
        pr_number=pr_number,
        summary="[transient] Worker restarted mid-execution; task was not completed.",
    )
    fake_redis_client.xadd(RESULTS_STREAM, result.to_dict())

    logger = logging.getLogger("test")
    _consume_results(orchestrator_config, fake_redis_client, logger)

    # Per-SHA attempts should be cleared
    assert get_attempt_count(fake_redis_client, repo, pr_number, head_sha) == 0
    # Transient infrastructure/provider failures must never bump total_attempts.
    assert get_total_attempt_count(fake_redis_client, repo, pr_number) == 0
    assert get_backoff_step(fake_redis_client, repo, pr_number) is None


def test_consume_results_duplicate_transient_does_not_clear_new_pr_attempt(
    fake_redis_client, orchestrator_config, gh_mock
):
    """A duplicate transient result must not erase a later task's PR attempt."""
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    pr_number = 57
    head_sha = "abc123"
    repo = orchestrator_config.github.repo
    result = _make_task_result(
        status=ResultStatus.FAILED,
        pr_number=pr_number,
        task_id="task-transient-dup",
        summary="[transient] Worker restarted mid-execution; task was not completed.",
    )

    increment_attempts(fake_redis_client, repo, pr_number, head_sha)
    fake_redis_client.xadd(RESULTS_STREAM, result.to_dict())
    logger = logging.getLogger("test")
    _consume_results(orchestrator_config, fake_redis_client, logger)
    assert get_attempt_count(fake_redis_client, repo, pr_number, head_sha) == 0

    increment_attempts(fake_redis_client, repo, pr_number, head_sha)
    fake_redis_client.xadd(RESULTS_STREAM, result.to_dict())
    _consume_results(orchestrator_config, fake_redis_client, logger)

    assert get_attempt_count(fake_redis_client, repo, pr_number, head_sha) == 1
    assert get_transient_failure_count(fake_redis_client, repo, pr_number) == 1
    gh_mock.add_label.assert_not_called()
    gh_mock.post_comment.assert_not_called()


def test_consume_results_transient_failure_at_budget_still_retries(
    fake_redis_client, orchestrator_config, gh_mock
):
    """The configured transient budget is inclusive: count 5 does not back off."""
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)
    orchestrator_config.max_transient_failures = 5

    pr_number = 58
    head_sha = "abc123"
    repo = orchestrator_config.github.repo
    for _ in range(4):
        increment_transient_failure_count(fake_redis_client, repo, pr_number)
    increment_attempts(fake_redis_client, repo, pr_number, head_sha)

    result = _make_task_result(
        status=ResultStatus.FAILED,
        pr_number=pr_number,
        summary="[transient] Timed out after 1800s",
    )
    fake_redis_client.xadd(RESULTS_STREAM, result.to_dict())

    logger = logging.getLogger("test")
    _consume_results(orchestrator_config, fake_redis_client, logger)

    assert get_transient_failure_count(fake_redis_client, repo, pr_number) == 5
    assert get_backoff_step(fake_redis_client, repo, pr_number) is None
    assert get_attempt_count(fake_redis_client, repo, pr_number, head_sha) == 0
    gh_mock.add_label.assert_not_called()
    gh_mock.post_comment.assert_not_called()


def test_consume_results_transient_failure_over_budget_keeps_retrying(
    fake_redis_client, orchestrator_config, gh_mock
):
    """Past the transient budget, orcest keeps retrying at the capped backoff
    cadence -- even an attached needs-human flag cannot make a transient
    failure terminal."""
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)
    orchestrator_config.max_transient_failures = 5

    pr_number = 59
    head_sha = "abc123"
    repo = orchestrator_config.github.repo

    for _ in range(5):
        increment_transient_failure_count(fake_redis_client, repo, pr_number)
    increment_attempts(fake_redis_client, repo, pr_number, head_sha)
    clear_backoff(fake_redis_client, repo, pr_number)

    result = _make_task_result(
        status=ResultStatus.FAILED,
        pr_number=pr_number,
        summary="[transient] Timed out after 5400s",
        needs_human=True,
        needs_human_reason="ambiguous requirement",
    )
    fake_redis_client.xadd(RESULTS_STREAM, result.to_dict())

    logger = logging.getLogger("test")
    _consume_results(orchestrator_config, fake_redis_client, logger)

    assert get_transient_failure_count(fake_redis_client, repo, pr_number) == 6
    # The first failure past the silent budget starts the backoff cadence.
    assert get_backoff_step(fake_redis_client, repo, pr_number) == 0
    # Per-SHA attempts are cleared so the PR retries after the cooldown.
    assert get_attempt_count(fake_redis_client, repo, pr_number, head_sha) == 0
    gh_mock.add_label.assert_not_called()
    gh_mock.post_comment.assert_not_called()


def test_consume_results_transient_failure_clears_issue_attempts(
    fake_redis_client, orchestrator_config, gh_mock
):
    """A transient FAILED result for an issue clears the counter via clear_issue_attempts."""
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    issue_number = 57
    repo = orchestrator_config.github.repo

    # Simulate a prior attempt on the issue
    increment_issue_attempts(fake_redis_client, repo, issue_number)
    assert get_issue_attempt_count(fake_redis_client, repo, issue_number) == 1

    result = _make_task_result(
        status=ResultStatus.FAILED,
        resource_type="issue",
        resource_id=issue_number,
        task_id="task-issue-001",
        branch="",
        summary="[transient] Worker restarted mid-execution; task was not completed.",
        duration=10,
    )
    fake_redis_client.xadd(RESULTS_STREAM, result.to_dict())

    logger = logging.getLogger("test")
    _consume_results(orchestrator_config, fake_redis_client, logger)

    # Issue attempts should be cleared (not PR attempts)
    assert get_issue_attempt_count(fake_redis_client, repo, issue_number) == 0

    # Transient failures are silent — no label or comment
    gh_mock.add_issue_label.assert_not_called()
    gh_mock.remove_issue_label.assert_not_called()
    gh_mock.post_issue_comment.assert_not_called()


def test_consume_results_duplicate_transient_does_not_clear_new_issue_attempt(
    fake_redis_client, orchestrator_config, gh_mock
):
    """A duplicate transient result must not erase a later issue attempt."""
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    issue_number = 62
    repo = orchestrator_config.github.repo
    result = _make_task_result(
        status=ResultStatus.FAILED,
        resource_type="issue",
        resource_id=issue_number,
        task_id="task-issue-transient-dup",
        branch="",
        summary="[transient] Worker restarted mid-execution; task was not completed.",
        duration=10,
    )

    increment_issue_attempts(fake_redis_client, repo, issue_number)
    fake_redis_client.xadd(RESULTS_STREAM, result.to_dict())
    logger = logging.getLogger("test")
    _consume_results(orchestrator_config, fake_redis_client, logger)
    assert get_issue_attempt_count(fake_redis_client, repo, issue_number) == 0

    increment_issue_attempts(fake_redis_client, repo, issue_number)
    fake_redis_client.xadd(RESULTS_STREAM, result.to_dict())
    _consume_results(orchestrator_config, fake_redis_client, logger)

    assert get_issue_attempt_count(fake_redis_client, repo, issue_number) == 1
    gh_mock.add_issue_label.assert_not_called()
    gh_mock.remove_issue_label.assert_not_called()
    gh_mock.post_issue_comment.assert_not_called()


def test_consume_results_permanent_failure_bumps_total_but_does_not_label(
    fake_redis_client, orchestrator_config, gh_mock
):
    """A non-transient FAILED result bumps total_attempts (which only paces the
    retry cadence) but never adds needs-human."""
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    repo = orchestrator_config.github.repo
    assert get_total_attempt_count(fake_redis_client, repo, 57) == 0

    result = _make_task_result(
        status=ResultStatus.FAILED,
        pr_number=57,
        summary="Claude crashed with exit code 1",
    )
    fake_redis_client.xadd(RESULTS_STREAM, result.to_dict())

    logger = logging.getLogger("test")
    _consume_results(orchestrator_config, fake_redis_client, logger)

    # No escalation -- the failure is just retried.
    gh_mock.add_label.assert_not_called()
    gh_mock.post_comment.assert_not_called()
    # Non-transient failures still bump the cross-SHA counter (paces backoff).
    assert get_total_attempt_count(fake_redis_client, repo, 57) == 1


def test_consume_results_completed_does_not_bump_total_attempts(
    fake_redis_client, orchestrator_config, gh_mock
):
    """Healthy fix-cycles must not bump total_attempts.

    Each review-fix cycle that completes successfully (worker pushed a commit,
    review bot may still ask for changes) must keep the cross-SHA failure
    counter at zero. Otherwise a PR going through many healthy cycles would
    trip the 50-attempt circuit breaker even though nothing is wrong.
    """
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    repo = orchestrator_config.github.repo
    pr_number = 58
    assert get_total_attempt_count(fake_redis_client, repo, pr_number) == 0

    for _ in range(3):
        result = _make_task_result(status=ResultStatus.COMPLETED, pr_number=pr_number)
        fake_redis_client.xadd(RESULTS_STREAM, result.to_dict())

    logger = logging.getLogger("test")
    _consume_results(orchestrator_config, fake_redis_client, logger)

    assert get_total_attempt_count(fake_redis_client, repo, pr_number) == 0


def test_consume_results_usage_exhausted(fake_redis_client, orchestrator_config, gh_mock):
    """USAGE_EXHAUSTED clears per-SHA attempts and sets cooldown."""
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    pr_number = 60
    head_sha = "deadbeef"

    # Simulate the pre-publish per-SHA increment for this task.
    repo = orchestrator_config.github.repo
    increment_attempts(fake_redis_client, repo, pr_number, head_sha)
    assert get_attempt_count(fake_redis_client, repo, pr_number, head_sha) == 1

    result = _make_task_result(
        status=ResultStatus.USAGE_EXHAUSTED,
        pr_number=pr_number,
        branch="fix/widget",
    )
    fake_redis_client.xadd(RESULTS_STREAM, result.to_dict())

    logger = logging.getLogger("test")
    _consume_results(orchestrator_config, fake_redis_client, logger)

    # Should post a comment mentioning paused
    gh_mock.post_comment.assert_called_once()
    comment_body = gh_mock.post_comment.call_args[0][2]
    assert "paused" in comment_body
    assert "fix/widget" in comment_body

    # No label operations for USAGE_EXHAUSTED
    gh_mock.remove_label.assert_not_called()
    gh_mock.add_label.assert_not_called()

    # Per-SHA attempt counter must be cleared so PR can be re-enqueued after cooldown
    assert get_attempt_count(fake_redis_client, repo, pr_number, head_sha) == 0
    # Rate limits never bump the cross-SHA failure counter (USAGE_EXHAUSTED isn't
    # a terminal failure), so it stays at 0.
    assert get_total_attempt_count(fake_redis_client, repo, pr_number) == 0

    assert fake_redis_client.get(_USAGE_EXHAUSTED_RESULT_KEY) == "1"

    # Cooldown marker must be set
    assert has_usage_exhausted_cooldown(fake_redis_client, repo, pr_number)
    assert 1700 <= fake_redis_client.ttl(f"pr:{repo}:{pr_number}:usage_cooldown") <= 1800


def test_consume_results_usage_exhausted_pr_cooldown_ttl_uses_reset_time(
    fake_redis_client, orchestrator_config, gh_mock
):
    """PR usage cooldown TTL is derived from rate_limit_resets_at when present."""
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    pr_number = 64
    repo = orchestrator_config.github.repo
    resets_at = int(time.time()) + 120

    result = _make_task_result(
        status=ResultStatus.USAGE_EXHAUSTED,
        pr_number=pr_number,
        rate_limit_resets_at=resets_at,
    )
    fake_redis_client.xadd(RESULTS_STREAM, result.to_dict())

    logger = logging.getLogger("test")
    _consume_results(orchestrator_config, fake_redis_client, logger)

    ttl = fake_redis_client.ttl(f"pr:{repo}:{pr_number}:usage_cooldown")
    assert 1 <= ttl <= 120


def test_mark_usage_exhausted_queries_reset_time_for_clauder(mocker):
    from datetime import datetime, timezone

    from orcest.orchestrator.provider_pool import ProviderPool
    from orcest.shared.providers import ProviderEntry

    reset_at = datetime(2026, 6, 26, 12, 0, tzinfo=timezone.utc)
    get_reset = mocker.patch("orcest.orchestrator.loop.get_token_reset_time", return_value=reset_at)
    entry = ProviderEntry(provider="clauder", credential="claude-oauth")
    pool = ProviderPool([entry])
    pool.register_task("task-clauder-exhausted", entry)

    _mark_usage_exhausted_token(
        _make_task_result(
            status=ResultStatus.USAGE_EXHAUSTED,
            task_id="task-clauder-exhausted",
        ),
        pool,
        logging.getLogger("test"),
    )

    get_reset.assert_called_once_with("claude-oauth")


def test_consume_results_usage_exhausted_no_branch(
    fake_redis_client,
    orchestrator_config,
    gh_mock,
):
    """A USAGE_EXHAUSTED result with no branch uses generic 'Work saved.' note."""
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    result = _make_task_result(
        status=ResultStatus.USAGE_EXHAUSTED,
        pr_number=61,
        branch="",
    )
    fake_redis_client.xadd(RESULTS_STREAM, result.to_dict())

    logger = logging.getLogger("test")
    _consume_results(orchestrator_config, fake_redis_client, logger)

    comment_body = gh_mock.post_comment.call_args[0][2]
    assert "Work saved." in comment_body
    # Should not reference a branch name
    assert "branch `" not in comment_body


def test_consume_results_usage_exhausted_issue_clears_attempts(
    fake_redis_client, orchestrator_config, gh_mock
):
    """An issue USAGE_EXHAUSTED result clears attempts and sets a cooldown."""
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    issue_number = 62
    repo = orchestrator_config.github.repo

    # Simulate a prior attempt so the counter is non-zero
    increment_issue_attempts(fake_redis_client, repo, issue_number)
    assert get_issue_attempt_count(fake_redis_client, repo, issue_number) == 1

    result = _make_task_result(
        status=ResultStatus.USAGE_EXHAUSTED,
        resource_type="issue",
        resource_id=issue_number,
        task_id="task-issue-ue-001",
        branch="",
    )
    fake_redis_client.xadd(RESULTS_STREAM, result.to_dict())

    logger = logging.getLogger("test")
    _consume_results(orchestrator_config, fake_redis_client, logger)

    # Attempt counter is cleared, but cooldown prevents immediate rediscovery.
    assert get_issue_attempt_count(fake_redis_client, repo, issue_number) == 0
    assert has_issue_usage_exhausted_cooldown(fake_redis_client, repo, issue_number)
    assert 1700 <= fake_redis_client.ttl(f"issue:{repo}:{issue_number}:usage_cooldown") <= 1800
    assert fake_redis_client.get(_USAGE_EXHAUSTED_RESULT_KEY) == "1"

    # A "paused" comment must be posted to the issue
    gh_mock.post_issue_comment.assert_called_once()
    comment_body = gh_mock.post_issue_comment.call_args[0][2]
    assert "paused" in comment_body

    # No label operations for USAGE_EXHAUSTED on issues
    gh_mock.add_issue_label.assert_not_called()
    gh_mock.remove_issue_label.assert_not_called()


def test_consume_results_usage_exhausted_issue_sets_cooldown_from_reset_time(
    fake_redis_client, orchestrator_config, gh_mock
):
    """Issue USAGE_EXHAUSTED persists a cooldown using rate_limit_resets_at."""
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    issue_number = 65
    repo = orchestrator_config.github.repo
    resets_at = int(time.time()) + 120

    increment_issue_attempts(fake_redis_client, repo, issue_number)
    result = _make_task_result(
        status=ResultStatus.USAGE_EXHAUSTED,
        resource_type="issue",
        resource_id=issue_number,
        task_id="task-issue-ue-cooldown",
        rate_limit_resets_at=resets_at,
    )
    fake_redis_client.xadd(RESULTS_STREAM, result.to_dict())

    logger = logging.getLogger("test")
    _consume_results(orchestrator_config, fake_redis_client, logger)

    assert get_issue_attempt_count(fake_redis_client, repo, issue_number) == 0
    assert has_issue_usage_exhausted_cooldown(fake_redis_client, repo, issue_number)
    ttl = fake_redis_client.ttl(f"issue:{repo}:{issue_number}:usage_cooldown")
    assert 1 <= ttl <= 120


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
    mocker,
    fake_redis_client,
    orchestrator_config,
    gh_mock,
):
    """When redis.xack raises, processing continues (entry was handled, just not acked)."""
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    result = _make_task_result(status=ResultStatus.COMPLETED, pr_number=70)
    fake_redis_client.xadd(RESULTS_STREAM, result.to_dict())

    # Make xack raise an exception
    mocker.patch.object(
        fake_redis_client,
        "xack",
        side_effect=RuntimeError("ACK failed"),
    )

    logger = logging.getLogger("test")
    # Should not raise -- xack failure is caught and logged
    _consume_results(orchestrator_config, fake_redis_client, logger)

    # The result was still processed (completed = no comment posted)
    gh_mock.post_comment.assert_not_called()


def test_consume_results_blocked_status_posts_comment(
    fake_redis_client,
    orchestrator_config,
    gh_mock,
):
    """A result with BLOCKED status posts a comment and adds blocked label."""
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

    # No label removals
    gh_mock.remove_label.assert_not_called()

    # Should add blocked label, NOT needs-human
    gh_mock.add_label.assert_called_once_with(
        orchestrator_config.github.repo,
        71,
        orchestrator_config.labels.blocked,
        orchestrator_config.github.token,
    )


# ---------------------------------------------------------------------------
# Additional _poll_cycle tests
# ---------------------------------------------------------------------------


def test_poll_cycle_merge_comment_failure_logged(
    mocker,
    fake_redis_client,
    orchestrator_config,
    gh_mock,
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
    _poll_cycle(orchestrator_config, fake_redis_client, fake_redis_client, {}, logger, 3600)

    # merge_pr was still called successfully
    gh_mock.merge_pr.assert_called_once_with(
        orchestrator_config.github.repo,
        80,
        orchestrator_config.github.token,
        delete_branch=orchestrator_config.delete_branch_on_merge,
        head_sha="abc123",
    )
    # post_comment was attempted (and failed)
    gh_mock.post_comment.assert_called_once()


def test_poll_cycle_merge_fail_does_not_label(
    mocker,
    fake_redis_client,
    orchestrator_config,
    gh_mock,
):
    """A non-conflict merge failure never labels or comments -- it backs off."""
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

    gh_mock.merge_pr.side_effect = RuntimeError("repository rule violation")

    logger = logging.getLogger("test")
    _poll_cycle(orchestrator_config, fake_redis_client, fake_redis_client, {}, logger, 3600)

    gh_mock.add_label.assert_not_called()
    gh_mock.post_comment.assert_not_called()
    assert get_backoff_step(fake_redis_client, orchestrator_config.projects[0].repo, 81) is not None


def test_poll_cycle_enqueue_fix_publish_failure(
    mocker,
    fake_redis_client,
    orchestrator_config,
    gh_mock,
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
    _poll_cycle(orchestrator_config, fake_redis_client, fake_redis_client, {}, logger, 3600)

    mock_publish.assert_called_once()


def test_poll_cycle_enqueue_followup_publish_failure(
    mocker,
    fake_redis_client,
    orchestrator_config,
    gh_mock,
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
    _poll_cycle(orchestrator_config, fake_redis_client, fake_redis_client, {}, logger, 3600)

    mock_followup.assert_called_once()


def test_poll_cycle_skip_max_attempts_does_not_touch_github(
    mocker,
    fake_redis_client,
    orchestrator_config,
    gh_mock,
):
    """SKIP_MAX_ATTEMPTS makes no GitHub calls -- it only sets a local backoff."""
    pr_state = _make_pr_state(number=84, action=PRAction.SKIP_MAX_ATTEMPTS)

    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        return_value=[pr_state],
    )
    mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    logger = logging.getLogger("test")
    _poll_cycle(orchestrator_config, fake_redis_client, fake_redis_client, {}, logger, 3600)

    gh_mock.add_label.assert_not_called()
    gh_mock.post_comment.assert_not_called()
    assert get_backoff_step(fake_redis_client, orchestrator_config.projects[0].repo, 84) is not None


def test_poll_cycle_skip_draft_action(
    mocker,
    fake_redis_client,
    orchestrator_config,
    gh_mock,
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
    _poll_cycle(orchestrator_config, fake_redis_client, fake_redis_client, {}, logger, 3600)

    # No task should be published and no merge attempted
    mock_publish.assert_not_called()
    mock_followup.assert_not_called()
    gh_mock.merge_pr.assert_not_called()
    gh_mock.add_label.assert_not_called()
    gh_mock.post_comment.assert_not_called()


def test_poll_cycle_skip_pending_action(
    mocker,
    fake_redis_client,
    orchestrator_config,
    gh_mock,
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
    _poll_cycle(orchestrator_config, fake_redis_client, fake_redis_client, {}, logger, 3600)

    # No task should be published and no merge attempted
    mock_publish.assert_not_called()
    mock_followup.assert_not_called()
    gh_mock.merge_pr.assert_not_called()
    gh_mock.add_label.assert_not_called()
    gh_mock.post_comment.assert_not_called()


# ---------------------------------------------------------------------------
# Additional _handle_result tests (via _consume_results)
# ---------------------------------------------------------------------------


def test_handle_result_needs_human_label_failure(
    fake_redis_client,
    orchestrator_config,
    gh_mock,
    mocker,
):
    """A failed required label leaves the result pending and preserves its lock."""
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    result = _make_task_result(
        status=ResultStatus.FAILED,
        pr_number=90,
        needs_human=True,
        needs_human_reason="ambiguous requirement",
    )
    set_pending_task(
        fake_redis_client,
        orchestrator_config.github.repo,
        "pr",
        90,
        result.task_id,
    )
    entry_id = fake_redis_client.xadd(RESULTS_STREAM, result.to_dict())

    gh_mock.add_label.side_effect = [RuntimeError("label API down"), None]

    logger = logging.getLogger("test")
    _consume_results(orchestrator_config, fake_redis_client, logger)

    pending = fake_redis_client.client.xpending(
        fake_redis_client._prefixed(RESULTS_STREAM),
        RESULTS_GROUP,
    )
    assert pending["pending"] == 1
    assert get_pending_task(fake_redis_client, orchestrator_config.github.repo, "pr", 90) == (
        result.task_id
    )
    gh_mock.post_comment.assert_not_called()

    mocker.patch.object(
        fake_redis_client,
        "xreadgroup",
        side_effect=[[(entry_id, result.to_dict())], [], []],
    )
    _consume_results(orchestrator_config, fake_redis_client, logger)

    pending = fake_redis_client.client.xpending(
        fake_redis_client._prefixed(RESULTS_STREAM),
        RESULTS_GROUP,
    )
    assert pending["pending"] == 0
    assert get_pending_task(fake_redis_client, orchestrator_config.github.repo, "pr", 90) is None
    assert gh_mock.add_label.call_count == 2
    gh_mock.post_comment.assert_called_once()


def test_handle_result_post_comment_failure(
    fake_redis_client,
    orchestrator_config,
    gh_mock,
    mocker,
):
    """An ambiguous comment failure retries without posting a duplicate."""
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    result = _make_task_result(
        status=ResultStatus.FAILED,
        pr_number=91,
        needs_human=True,
        needs_human_reason="needs a product decision",
    )
    set_pending_task(
        fake_redis_client,
        orchestrator_config.github.repo,
        "pr",
        91,
        result.task_id,
    )
    entry_id = fake_redis_client.xadd(RESULTS_STREAM, result.to_dict())

    gh_mock.post_comment.side_effect = RuntimeError("GitHub API down")
    # The first query precedes a post that reaches GitHub but loses its response.
    # On retry the deterministic hidden marker proves the comment already exists.
    gh_mock.has_issue_comment_marker.side_effect = [False, True]

    logger = logging.getLogger("test")
    _consume_results(orchestrator_config, fake_redis_client, logger)

    pending = fake_redis_client.client.xpending(
        fake_redis_client._prefixed(RESULTS_STREAM),
        RESULTS_GROUP,
    )
    assert pending["pending"] == 1
    assert get_pending_task(fake_redis_client, orchestrator_config.github.repo, "pr", 91) == (
        result.task_id
    )

    mocker.patch.object(
        fake_redis_client,
        "xreadgroup",
        side_effect=[[(entry_id, result.to_dict())], [], []],
    )
    _consume_results(orchestrator_config, fake_redis_client, logger)

    pending = fake_redis_client.client.xpending(
        fake_redis_client._prefixed(RESULTS_STREAM),
        RESULTS_GROUP,
    )
    assert pending["pending"] == 0
    assert get_pending_task(fake_redis_client, orchestrator_config.github.repo, "pr", 91) is None
    assert gh_mock.add_label.call_count == 2
    gh_mock.post_comment.assert_called_once()


def test_completed_issue_label_failure_stays_pending_then_retries(
    fake_redis_client,
    orchestrator_config,
    gh_mock,
    mocker,
):
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)
    repo = orchestrator_config.github.repo
    issue_number = 92
    result = _make_task_result(
        status=ResultStatus.COMPLETED,
        resource_type="issue",
        resource_id=issue_number,
        task_id="completed-issue-label-retry",
        branch="",
    )
    increment_issue_attempts(fake_redis_client, repo, issue_number)
    set_pending_task(fake_redis_client, repo, "issue", issue_number, result.task_id)
    entry_id = fake_redis_client.xadd(RESULTS_STREAM, result.to_dict())
    gh_mock.remove_issue_label.side_effect = [RuntimeError("GitHub down"), None]

    _consume_results(orchestrator_config, fake_redis_client, logging.getLogger("test"))

    assert get_pending_task(fake_redis_client, repo, "issue", issue_number) == result.task_id
    assert get_issue_attempt_count(fake_redis_client, repo, issue_number) == 0
    assert (
        fake_redis_client.client.xpending(
            fake_redis_client._prefixed(RESULTS_STREAM), RESULTS_GROUP
        )["pending"]
        == 1
    )

    mocker.patch.object(
        fake_redis_client,
        "xreadgroup",
        side_effect=[[(entry_id, result.to_dict())], [], []],
    )
    _consume_results(orchestrator_config, fake_redis_client, logging.getLogger("test"))

    assert get_pending_task(fake_redis_client, repo, "issue", issue_number) is None
    assert gh_mock.remove_issue_label.call_count == 2
    assert (
        fake_redis_client.client.xpending(
            fake_redis_client._prefixed(RESULTS_STREAM), RESULTS_GROUP
        )["pending"]
        == 0
    )


def test_pending_marker_clear_failure_retries_without_duplicate_github_effects(
    fake_redis_client,
    orchestrator_config,
    gh_mock,
    mocker,
):
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)
    repo = orchestrator_config.github.repo
    pr_number = 93
    result = _make_task_result(
        status=ResultStatus.BLOCKED,
        pr_number=pr_number,
        task_id="pending-clear-retry",
    )
    set_pending_task(fake_redis_client, repo, "pr", pr_number, result.task_id)
    entry_id = fake_redis_client.xadd(RESULTS_STREAM, result.to_dict())
    real_clear = clear_pending_task_if_matches
    clear_calls = 0

    def flaky_clear(*args, **kwargs):
        nonlocal clear_calls
        clear_calls += 1
        if clear_calls == 1:
            raise ConnectionError("Redis unavailable")
        return real_clear(*args, **kwargs)

    mocker.patch(
        "orcest.orchestrator.loop.clear_pending_task_if_matches",
        side_effect=flaky_clear,
    )

    _consume_results(orchestrator_config, fake_redis_client, logging.getLogger("test"))

    assert get_pending_task(fake_redis_client, repo, "pr", pr_number) == result.task_id
    assert (
        fake_redis_client.client.xpending(
            fake_redis_client._prefixed(RESULTS_STREAM), RESULTS_GROUP
        )["pending"]
        == 1
    )
    gh_mock.add_label.assert_called_once()
    gh_mock.post_comment.assert_called_once()

    mocker.patch.object(
        fake_redis_client,
        "xreadgroup",
        side_effect=[[(entry_id, result.to_dict())], [], []],
    )
    _consume_results(orchestrator_config, fake_redis_client, logging.getLogger("test"))

    assert get_pending_task(fake_redis_client, repo, "pr", pr_number) is None
    assert (
        fake_redis_client.client.xpending(
            fake_redis_client._prefixed(RESULTS_STREAM), RESULTS_GROUP
        )["pending"]
        == 0
    )
    # The durable side-effect checkpoint bypasses already-completed GitHub work.
    gh_mock.add_label.assert_called_once()
    gh_mock.post_comment.assert_called_once()


def test_blocked_label_failure_stays_pending_then_retries(
    fake_redis_client,
    orchestrator_config,
    gh_mock,
    mocker,
):
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)
    repo = orchestrator_config.github.repo
    pr_number = 94
    result = _make_task_result(
        status=ResultStatus.BLOCKED,
        pr_number=pr_number,
        task_id="blocked-label-retry",
    )
    set_pending_task(fake_redis_client, repo, "pr", pr_number, result.task_id)
    entry_id = fake_redis_client.xadd(RESULTS_STREAM, result.to_dict())
    gh_mock.add_label.side_effect = [RuntimeError("GitHub down"), None]

    _consume_results(orchestrator_config, fake_redis_client, logging.getLogger("test"))

    assert get_pending_task(fake_redis_client, repo, "pr", pr_number) == result.task_id
    assert (
        fake_redis_client.client.xpending(
            fake_redis_client._prefixed(RESULTS_STREAM), RESULTS_GROUP
        )["pending"]
        == 1
    )
    gh_mock.post_comment.assert_not_called()

    mocker.patch.object(
        fake_redis_client,
        "xreadgroup",
        side_effect=[[(entry_id, result.to_dict())], [], []],
    )
    _consume_results(orchestrator_config, fake_redis_client, logging.getLogger("test"))

    assert get_pending_task(fake_redis_client, repo, "pr", pr_number) is None
    assert gh_mock.add_label.call_count == 2
    gh_mock.post_comment.assert_called_once()


def test_usage_attempt_cleanup_failure_stays_pending_then_retries(
    fake_redis_client,
    orchestrator_config,
    gh_mock,
    mocker,
):
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)
    repo = orchestrator_config.github.repo
    pr_number = 95
    result = _make_task_result(
        status=ResultStatus.USAGE_EXHAUSTED,
        pr_number=pr_number,
        task_id="usage-cleanup-retry",
    )
    set_pending_task(fake_redis_client, repo, "pr", pr_number, result.task_id)
    entry_id = fake_redis_client.xadd(RESULTS_STREAM, result.to_dict())
    clear_mock = mocker.patch(
        "orcest.orchestrator.loop.clear_attempts",
        side_effect=[ConnectionError("Redis down"), None],
    )

    _consume_results(orchestrator_config, fake_redis_client, logging.getLogger("test"))

    assert has_usage_exhausted_cooldown(fake_redis_client, repo, pr_number)
    assert get_pending_task(fake_redis_client, repo, "pr", pr_number) == result.task_id
    assert (
        fake_redis_client.client.xpending(
            fake_redis_client._prefixed(RESULTS_STREAM), RESULTS_GROUP
        )["pending"]
        == 1
    )
    gh_mock.post_comment.assert_not_called()

    mocker.patch.object(
        fake_redis_client,
        "xreadgroup",
        side_effect=[[(entry_id, result.to_dict())], [], []],
    )
    _consume_results(orchestrator_config, fake_redis_client, logging.getLogger("test"))

    assert get_pending_task(fake_redis_client, repo, "pr", pr_number) is None
    assert clear_mock.call_count == 2
    gh_mock.post_comment.assert_called_once()


# ---------------------------------------------------------------------------
# RETRIGGER_REVIEW tests
# ---------------------------------------------------------------------------


def test_poll_cycle_retrigger_review(mocker, fake_redis_client, orchestrator_config, gh_mock):
    """_poll_cycle calls gh.rerun_workflow and records retrigger SHA for RETRIGGER_REVIEW."""
    pr_state = PRState(
        number=500,
        title="PR #500",
        branch="fix/500",
        head_sha="sha999",
        action=PRAction.RETRIGGER_REVIEW,
        ci_failures=[],
        review_threads=[],
        labels=[],
        review_run_id=12345,
    )

    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        return_value=[pr_state],
    )
    mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    logger = logging.getLogger("test")
    _poll_cycle(orchestrator_config, fake_redis_client, fake_redis_client, {}, logger, 3600)

    gh_mock.rerun_workflow.assert_called_once_with(
        orchestrator_config.github.repo,
        12345,
        orchestrator_config.github.token,
    )

    # Verify the retrigger SHA was recorded in Redis
    from orcest.orchestrator.pr_ops import get_review_retrigger_sha

    repo = orchestrator_config.github.repo
    assert get_review_retrigger_sha(fake_redis_client, repo, 500) == "sha999"


def test_poll_cycle_retrigger_review_failure_logged(
    mocker, fake_redis_client, orchestrator_config, gh_mock
):
    """If rerun_workflow raises, the error is logged but the loop doesn't crash."""
    pr_state = PRState(
        number=501,
        title="PR #501",
        branch="fix/501",
        head_sha="sha000",
        action=PRAction.RETRIGGER_REVIEW,
        ci_failures=[],
        review_threads=[],
        labels=[],
        review_run_id=99999,
    )

    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        return_value=[pr_state],
    )
    mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    gh_mock.rerun_workflow.side_effect = RuntimeError("GitHub API error")

    logger = logging.getLogger("test")
    # Should not raise
    _poll_cycle(orchestrator_config, fake_redis_client, fake_redis_client, {}, logger, 3600)

    gh_mock.rerun_workflow.assert_called_once()

    # Retrigger SHA should NOT be recorded on failure
    from orcest.orchestrator.pr_ops import get_review_retrigger_sha

    assert get_review_retrigger_sha(fake_redis_client, orchestrator_config.github.repo, 501) is None


def test_poll_cycle_retrigger_review_failure_cooldown_skips_next_poll(
    mocker, fake_redis_client, orchestrator_config, gh_mock
):
    """A failed claude-review rerun sets a cooldown instead of retrying every poll."""
    pr_state = PRState(
        number=502,
        title="PR #502",
        branch="fix/502",
        head_sha="sha-cooldown",
        action=PRAction.RETRIGGER_REVIEW,
        ci_failures=[],
        review_threads=[],
        labels=[],
        review_run_id=88888,
    )

    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        return_value=[pr_state],
    )
    mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)
    gh_mock.rerun_workflow.side_effect = RuntimeError("GitHub API error")

    logger = logging.getLogger("test")
    _poll_cycle(orchestrator_config, fake_redis_client, fake_redis_client, {}, logger, 3600)
    _poll_cycle(orchestrator_config, fake_redis_client, fake_redis_client, {}, logger, 3600)

    gh_mock.rerun_workflow.assert_called_once()
    gh_mock.add_label.assert_not_called()
    gh_mock.post_comment.assert_not_called()


def test_poll_cycle_retrigger_review_failures_back_off_after_limit(
    mocker, fake_redis_client, orchestrator_config, gh_mock
):
    """Repeated failed claude-review reruns back off and retry -- they never
    add needs-human."""
    pr_state = PRState(
        number=503,
        title="PR #503",
        branch="fix/503",
        head_sha="sha-escalate",
        action=PRAction.RETRIGGER_REVIEW,
        ci_failures=[],
        review_threads=[],
        labels=[],
        review_run_id=77777,
    )

    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        return_value=[pr_state],
    )
    mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)
    gh_mock.rerun_workflow.side_effect = RuntimeError("GitHub API error")

    repo = orchestrator_config.github.repo
    cooldown_key = _make_review_rerun_failure_cooldown_key(repo, pr_state.number, pr_state.head_sha)
    logger = logging.getLogger("test")
    for _ in range(_MAX_REVIEW_RERUN_FAILURES):
        fake_redis_client.delete(cooldown_key)
        _poll_cycle(orchestrator_config, fake_redis_client, fake_redis_client, {}, logger, 3600)

    assert gh_mock.rerun_workflow.call_count == _MAX_REVIEW_RERUN_FAILURES
    gh_mock.add_label.assert_not_called()
    assert get_backoff_step(fake_redis_client, repo, 503) is not None


# ---------------------------------------------------------------------------
# Proactive rebase after merge tests
# ---------------------------------------------------------------------------


def test_poll_cycle_proactive_rebase_skip_green_sibling(
    mocker, fake_redis_client, orchestrator_config, gh_mock
):
    """After a successful merge, SKIP_GREEN siblings get publish_rebase_task(proactive=True)."""
    merged_pr = PRState(
        number=10,
        title="PR #10",
        branch="feat/10",
        head_sha="sha10",
        action=PRAction.MERGE,
        ci_failures=[],
        review_threads=[],
        labels=[],
    )
    green_pr = PRState(
        number=20,
        title="PR #20",
        branch="feat/20",
        head_sha="sha20",
        action=PRAction.SKIP_GREEN,
        ci_failures=[],
        review_threads=[],
        labels=[],
    )

    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        return_value=[merged_pr, green_pr],
    )
    mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    mock_rebase = mocker.patch("orcest.orchestrator.loop.publish_rebase_task")
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    logger = logging.getLogger("test")
    _poll_cycle(orchestrator_config, fake_redis_client, fake_redis_client, {}, logger, 3600)

    mock_rebase.assert_called_once()
    assert mock_rebase.call_args.kwargs["pr_state"] is green_pr
    assert mock_rebase.call_args.kwargs["proactive"] is True
    assert mock_rebase.call_args.kwargs["merge_error"] == ""


def test_poll_cycle_proactive_rebase_skips_non_green_siblings(
    mocker, fake_redis_client, orchestrator_config, gh_mock
):
    """Non-SKIP_GREEN siblings are not proactively rebased after a merge."""
    merged_pr = PRState(
        number=10,
        title="PR #10",
        branch="feat/10",
        head_sha="sha10",
        action=PRAction.MERGE,
        ci_failures=[],
        review_threads=[],
        labels=[],
    )
    non_green_actions = [
        PRAction.SKIP_ACTIVE,
        PRAction.SKIP_DRAFT,
        PRAction.ENQUEUE_FIX,
        PRAction.ENQUEUE_FOLLOWUP,
    ]
    siblings = [
        PRState(
            number=100 + i,
            title=f"PR #{100 + i}",
            branch=f"feat/{100 + i}",
            head_sha=f"sha{100 + i}",
            action=action,
            ci_failures=[],
            review_threads=[],
            labels=[],
        )
        for i, action in enumerate(non_green_actions)
    ]

    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        return_value=[merged_pr, *siblings],
    )
    mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    mock_rebase = mocker.patch("orcest.orchestrator.loop.publish_rebase_task")
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    logger = logging.getLogger("test")
    _poll_cycle(orchestrator_config, fake_redis_client, fake_redis_client, {}, logger, 3600)

    mock_rebase.assert_not_called()


def test_poll_cycle_proactive_rebase_skips_merged_pr_itself(
    mocker, fake_redis_client, orchestrator_config, gh_mock
):
    """The merged PR itself is never passed to publish_rebase_task."""
    merged_pr = PRState(
        number=10,
        title="PR #10",
        branch="feat/10",
        head_sha="sha10",
        # SKIP_GREEN action on the same PR that is being merged won't happen in practice,
        # but verify the self-skip guard works regardless.
        action=PRAction.MERGE,
        ci_failures=[],
        review_threads=[],
        labels=[],
    )

    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        return_value=[merged_pr],
    )
    mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    mock_rebase = mocker.patch("orcest.orchestrator.loop.publish_rebase_task")
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    logger = logging.getLogger("test")
    _poll_cycle(orchestrator_config, fake_redis_client, fake_redis_client, {}, logger, 3600)

    mock_rebase.assert_not_called()


def test_poll_cycle_proactive_rebase_exception_does_not_propagate(
    mocker, fake_redis_client, orchestrator_config, gh_mock
):
    """An exception from publish_rebase_task during proactive rebase is caught."""
    merged_pr = PRState(
        number=10,
        title="PR #10",
        branch="feat/10",
        head_sha="sha10",
        action=PRAction.MERGE,
        ci_failures=[],
        review_threads=[],
        labels=[],
    )
    green_pr = PRState(
        number=20,
        title="PR #20",
        branch="feat/20",
        head_sha="sha20",
        action=PRAction.SKIP_GREEN,
        ci_failures=[],
        review_threads=[],
        labels=[],
    )

    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        return_value=[merged_pr, green_pr],
    )
    mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    mocker.patch(
        "orcest.orchestrator.loop.publish_rebase_task",
        side_effect=RuntimeError("Redis connection lost"),
    )
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    logger = logging.getLogger("test")
    # Must not raise
    _poll_cycle(orchestrator_config, fake_redis_client, fake_redis_client, {}, logger, 3600)


# ---------------------------------------------------------------------------
# Stale task ID guard tests
# ---------------------------------------------------------------------------


def test_handle_result_stale_task_id_skips_side_effects(
    fake_redis_client,
    orchestrator_config,
    gh_mock,
):
    """A FAILED result for an old task ID is silently dropped when a newer task is active.

    Scenario: result publishing failed for old_task, so the pending-task marker was
    cleared to allow re-enqueueing. The orchestrator enqueued new_task. When the worker
    drains its PEL and publishes a FAILED result for old_task, _handle_result must not
    add labels or post comments because the resource is already being handled by new_task.
    """
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    repo = orchestrator_config.github.repo
    pr_number = 200
    old_task_id = "old-task-aaa"
    new_task_id = "new-task-bbb"

    # Simulate the orchestrator having enqueued a newer task after the old one failed
    set_pending_task(fake_redis_client, repo, "pr", pr_number, new_task_id, ttl=3600)

    # The drained FAILED result carries the old task's ID
    stale_result = TaskResult(
        task_id=old_task_id,
        worker_id="worker-1",
        status=ResultStatus.FAILED,
        branch="fix/stale",
        summary="Something went wrong",
        duration_seconds=30,
        resource_type="pr",
        resource_id=pr_number,
    )
    fake_redis_client.xadd(RESULTS_STREAM, stale_result.to_dict())

    logger = logging.getLogger("test")
    _consume_results(orchestrator_config, fake_redis_client, logger)

    # Label and comment side-effects must be skipped for stale results
    gh_mock.add_label.assert_not_called()
    gh_mock.post_comment.assert_not_called()

    # The pending-task marker for new_task must remain intact
    assert get_pending_task(fake_redis_client, repo, "pr", pr_number) == new_task_id


def test_handle_result_none_pending_applies_side_effects(
    fake_redis_client,
    orchestrator_config,
    gh_mock,
):
    """A FAILED result with no pending-task marker still applies label/comment side-effects.

    Scenario: the pending-task marker was already cleared (e.g. on a previous delivery)
    so get_pending_task returns None. Because there is no active replacement task in
    flight, the result side-effects (e.g. needs-human label) should still apply.
    """
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    pr_number = 201
    task_id = "task-no-pending"

    # No pending-task marker is set — get_pending_task will return None.
    # Use a needs-human result so there is an observable side-effect.
    result = TaskResult(
        task_id=task_id,
        worker_id="worker-1",
        status=ResultStatus.FAILED,
        branch="fix/none-window",
        summary="Something went wrong",
        duration_seconds=15,
        resource_type="pr",
        resource_id=pr_number,
        snapshot_head_sha="abc123",
        needs_human=True,
        needs_human_reason="a human decision is required",
    )
    fake_redis_client.xadd(RESULTS_STREAM, result.to_dict())

    logger = logging.getLogger("test")
    _consume_results(orchestrator_config, fake_redis_client, logger)

    # Side-effects must apply: needs-human label and a comment should be posted
    gh_mock.add_label.assert_called_once()
    gh_mock.post_comment.assert_called_once()


def test_handle_result_stale_pr_snapshot_skips_side_effects(
    fake_redis_client,
    orchestrator_config,
    gh_mock,
):
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    pr_number = 202
    result = TaskResult(
        task_id="task-old-sha",
        worker_id="worker-1",
        status=ResultStatus.FAILED,
        branch="fix/stale-sha",
        summary="Something went wrong",
        duration_seconds=15,
        resource_type="pr",
        resource_id=pr_number,
        snapshot_head_sha="sha-old",
        decision_reason="ci_failure",
        snapshot_failed_checks=["tests"],
    )
    gh_mock.get_pr.return_value = {
        "headRefOid": "sha-new",
        "statusCheckRollup": [{"name": "tests", "conclusion": "FAILURE"}],
    }
    fake_redis_client.xadd(RESULTS_STREAM, result.to_dict())

    logger = logging.getLogger("test")
    _consume_results(orchestrator_config, fake_redis_client, logger)

    gh_mock.add_label.assert_not_called()
    gh_mock.post_comment.assert_not_called()


def test_handle_result_stale_pr_snapshot_clears_attempt_reservation(
    fake_redis_client,
    orchestrator_config,
    gh_mock,
):
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    repo = orchestrator_config.github.repo
    pr_number = 203
    increment_attempts(fake_redis_client, repo, pr_number, "sha-old")
    result = TaskResult(
        task_id="task-stale-attempt",
        worker_id="worker-1",
        status=ResultStatus.STALE,
        branch="fix/stale",
        summary="stale",
        duration_seconds=1,
        resource_type="pr",
        resource_id=pr_number,
        snapshot_head_sha="sha-old",
        decision_reason="ci_failure",
        snapshot_failed_checks=["tests"],
    )
    fake_redis_client.xadd(RESULTS_STREAM, result.to_dict())

    logger = logging.getLogger("test")
    _consume_results(orchestrator_config, fake_redis_client, logger)

    assert get_attempt_count(fake_redis_client, repo, pr_number, "sha-old") == 0
    gh_mock.add_label.assert_not_called()
    gh_mock.post_comment.assert_not_called()


def test_handle_result_stale_pr_snapshot_does_not_clear_new_sha_attempts(
    fake_redis_client,
    orchestrator_config,
    gh_mock,
):
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    repo = orchestrator_config.github.repo
    pr_number = 204
    increment_attempts(fake_redis_client, repo, pr_number, "sha-new")
    result = TaskResult(
        task_id="task-stale-old-attempt",
        worker_id="worker-1",
        status=ResultStatus.STALE,
        branch="fix/stale",
        summary="stale",
        duration_seconds=1,
        resource_type="pr",
        resource_id=pr_number,
        snapshot_head_sha="sha-old",
        decision_reason="ci_failure",
        snapshot_failed_checks=["tests"],
    )
    fake_redis_client.xadd(RESULTS_STREAM, result.to_dict())

    logger = logging.getLogger("test")
    _consume_results(orchestrator_config, fake_redis_client, logger)

    assert get_attempt_count(fake_redis_client, repo, pr_number, "sha-new") == 1
    gh_mock.add_label.assert_not_called()
    gh_mock.post_comment.assert_not_called()


def test_handle_result_uses_ci_status_fallback_for_snapshot_validation(
    fake_redis_client,
    orchestrator_config,
    gh_mock,
):
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    pr_number = 205
    result = TaskResult(
        task_id="task-current",
        worker_id="worker-1",
        status=ResultStatus.FAILED,
        branch="fix/current",
        summary="failed",
        duration_seconds=1,
        resource_type="pr",
        resource_id=pr_number,
        snapshot_head_sha="sha-current",
        decision_reason="ci_failure",
        snapshot_failed_checks=["tests"],
        needs_human=True,
        needs_human_reason="a human decision is required",
    )
    gh_mock.get_pr.return_value = {"headRefOid": "sha-current"}
    gh_mock.get_ci_status.return_value = [{"name": "tests", "conclusion": "failure"}]
    fake_redis_client.xadd(RESULTS_STREAM, result.to_dict())

    logger = logging.getLogger("test")
    _consume_results(orchestrator_config, fake_redis_client, logger)

    gh_mock.add_label.assert_called_once()
    gh_mock.post_comment.assert_called_once()


def test_handle_result_validation_failure_leaves_result_pending(
    fake_redis_client,
    orchestrator_config,
    gh_mock,
):
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    result = TaskResult(
        task_id="task-unvalidated",
        worker_id="worker-1",
        status=ResultStatus.FAILED,
        branch="fix/unvalidated",
        summary="failed",
        duration_seconds=1,
        resource_type="pr",
        resource_id=206,
        snapshot_head_sha="sha-current",
        decision_reason="ci_failure",
        snapshot_failed_checks=["tests"],
    )
    gh_mock.get_pr.side_effect = RuntimeError("github unavailable")
    fake_redis_client.xadd(RESULTS_STREAM, result.to_dict())

    logger = logging.getLogger("test")
    _consume_results(orchestrator_config, fake_redis_client, logger)

    gh_mock.add_label.assert_not_called()
    gh_mock.post_comment.assert_not_called()
    assert (
        fake_redis_client.client.xpending(
            fake_redis_client._prefixed(RESULTS_STREAM), RESULTS_GROUP
        )["pending"]
        == 1
    )


def test_handle_result_snapshotless_pr_result_drops_side_effects(
    fake_redis_client,
    orchestrator_config,
    gh_mock,
):
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)
    result = TaskResult(
        task_id="legacy-task",
        worker_id="worker-1",
        status=ResultStatus.FAILED,
        branch="fix/legacy",
        summary="failed",
        duration_seconds=1,
        resource_type="pr",
        resource_id=207,
    )
    fake_redis_client.xadd(RESULTS_STREAM, result.to_dict())

    logger = logging.getLogger("test")
    _consume_results(orchestrator_config, fake_redis_client, logger)

    gh_mock.add_label.assert_not_called()
    gh_mock.post_comment.assert_not_called()


def test_handle_result_review_thread_body_change_drops_side_effects(
    fake_redis_client,
    orchestrator_config,
    gh_mock,
):
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)
    old_fingerprint = (
        '{"comments":[{"author":"alice","body":"old feedback",'
        '"created_at":"","id":"","updated_at":""}],'
        '"id":"thread-1","line":"10","path":"app.py"}'
    )
    result = TaskResult(
        task_id="task-review-stale",
        worker_id="worker-1",
        status=ResultStatus.FAILED,
        branch="fix/review",
        summary="failed",
        duration_seconds=1,
        resource_type="pr",
        resource_id=208,
        snapshot_head_sha="sha-current",
        decision_reason="changes_requested",
        snapshot_review_thread_ids=["thread-1"],
        snapshot_review_thread_fingerprints=[old_fingerprint],
    )
    gh_mock.get_pr.return_value = {"headRefOid": "sha-current"}
    gh_mock.get_unresolved_review_threads.return_value = [
        {
            "id": "thread-1",
            "path": "app.py",
            "line": 10,
            "comments": [{"author": "alice", "body": "new feedback"}],
        }
    ]
    fake_redis_client.xadd(RESULTS_STREAM, result.to_dict())

    logger = logging.getLogger("test")
    _consume_results(orchestrator_config, fake_redis_client, logger)

    gh_mock.add_label.assert_not_called()
    gh_mock.post_comment.assert_not_called()


def test_handle_result_rebase_conflict_resolved_drops_side_effects(
    fake_redis_client,
    orchestrator_config,
    gh_mock,
):
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    result = TaskResult(
        task_id="task-rebase-stale",
        worker_id="worker-1",
        status=ResultStatus.FAILED,
        branch="fix/rebase",
        summary="failed",
        duration_seconds=1,
        resource_type="pr",
        resource_id=209,
        snapshot_head_sha="sha-current",
        decision_reason="merge_conflict_rebase",
    )
    gh_mock.get_pr.return_value = {
        "headRefOid": "sha-current",
        "mergeable": "MERGEABLE",
        "mergeStateStatus": "CLEAN",
    }
    fake_redis_client.xadd(RESULTS_STREAM, result.to_dict())

    logger = logging.getLogger("test")
    _consume_results(orchestrator_config, fake_redis_client, logger)

    gh_mock.add_label.assert_not_called()
    gh_mock.post_comment.assert_not_called()


def test_transient_result_backoff_records_result_snapshot_sha_without_attempt_hash(
    fake_redis_client,
    orchestrator_config,
    gh_mock,
):
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)
    orchestrator_config.max_transient_failures = 0

    repo = orchestrator_config.github.repo
    pr_number = 210
    result = _make_task_result(
        status=ResultStatus.FAILED,
        pr_number=pr_number,
        summary="[transient] worker restarted",
        snapshot_head_sha="sha-current",
    )
    gh_mock.get_pr.return_value = {"headRefOid": "sha-current"}
    fake_redis_client.xadd(RESULTS_STREAM, result.to_dict())

    logger = logging.getLogger("test")
    _consume_results(orchestrator_config, fake_redis_client, logger)

    assert get_backoff_step(fake_redis_client, repo, pr_number) == 0
    assert get_backoff_head_sha(fake_redis_client, repo, pr_number) == "sha-current"


def test_stale_pending_usage_exhausted_marks_token_before_return(
    fake_redis_client,
    orchestrator_config,
    gh_mock,
    mocker,
):
    from orcest.orchestrator.provider_pool import ProviderPool
    from orcest.shared.providers import ProviderEntry

    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    project = orchestrator_config.projects[0]
    pr_number = 211
    set_pending_task(
        fake_redis_client,
        project.repo,
        "pr",
        pr_number,
        "new-task",
        snapshot_head_sha="sha-current",
        decision_reason="ci_failure",
    )
    result = _make_task_result(
        status=ResultStatus.USAGE_EXHAUSTED,
        pr_number=pr_number,
        task_id="old-task",
        snapshot_head_sha="sha-current",
    )
    fake_redis_client.xadd(RESULTS_STREAM, result.to_dict())
    mocker.patch("orcest.orchestrator.loop.get_token_reset_time", return_value=None)
    entry = ProviderEntry(provider="claude", credential="exhausted-token")
    token_pool = ProviderPool([entry])
    token_pool.register_task("old-task", entry)
    mark_spy = mocker.spy(token_pool, "mark_exhausted")
    completed_spy = mocker.spy(token_pool, "task_completed")

    _consume_results_for_project(
        project,
        fake_redis_client,
        orchestrator_config.labels,
        logging.getLogger("test"),
        token_pool=token_pool,
    )

    mark_spy.assert_called_once()
    completed_spy.assert_called_once_with("old-task")
    gh_mock.post_comment.assert_not_called()
    assert token_pool.available_count == 0


def test_structured_ci_result_stales_when_target_url_changes(
    fake_redis_client,
    orchestrator_config,
    gh_mock,
):
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    snapshot = json.dumps(
        {
            "context": "build",
            "details_url": "",
            "name": "",
            "target_url": "https://ci.example/build/old",
            "workflow_name": "",
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    result = _make_task_result(
        status=ResultStatus.FAILED,
        pr_number=212,
        snapshot_head_sha="sha-current",
    )
    result.decision_reason = "ci_failure"
    result.snapshot_failed_checks = [snapshot]
    gh_mock.get_pr.return_value = {
        "headRefOid": "sha-current",
        "statusCheckRollup": [
            {
                "context": "build",
                "state": "FAILURE",
                "targetUrl": "https://ci.example/build/new",
            }
        ],
    }
    fake_redis_client.xadd(RESULTS_STREAM, result.to_dict())

    logger = logging.getLogger("test")
    _consume_results(orchestrator_config, fake_redis_client, logger)

    gh_mock.add_label.assert_not_called()
    gh_mock.post_comment.assert_not_called()


# ---------------------------------------------------------------------------
# RETRIGGER_STALE_CHECKS handler tests
# ---------------------------------------------------------------------------


def _make_stale_pr_state(
    number: int = 99,
    head_sha: str = "stale111",
    stale_run_ids: list[int] | None = None,
) -> PRState:
    """Build a PRState with RETRIGGER_STALE_CHECKS action."""
    return PRState(
        number=number,
        title=f"PR #{number}",
        branch=f"fix/{number}",
        head_sha=head_sha,
        action=PRAction.RETRIGGER_STALE_CHECKS,
        ci_failures=[],
        review_threads=[],
        labels=[],
        stale_run_ids=stale_run_ids if stale_run_ids is not None else [],
    )


def test_poll_cycle_retrigger_stale_checks_cooldown_skips(
    mocker,
    fake_redis_client,
    orchestrator_config,
    gh_mock,
):
    """When stale_sha matches head_sha, the handler is skipped (cooldown guard)."""
    pr_state = _make_stale_pr_state(number=99, head_sha="stale111", stale_run_ids=[1001])

    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        return_value=[pr_state],
    )
    mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    # Pre-set the cooldown for this SHA
    set_stale_retrigger_sha(
        fake_redis_client,
        orchestrator_config.github.repo,
        pr_state.number,
        pr_state.head_sha,
        ex=orchestrator_config.stale_pending_timeout_seconds,
    )

    logger = logging.getLogger("test")
    _poll_cycle(orchestrator_config, fake_redis_client, fake_redis_client, {}, logger, 3600)

    # No GitHub operations — cooldown suppresses all action
    gh_mock.cancel_workflow.assert_not_called()
    gh_mock.rerun_workflow.assert_not_called()
    gh_mock.add_label.assert_not_called()
    gh_mock.post_comment.assert_not_called()


def test_poll_cycle_retrigger_stale_checks_no_run_ids_does_not_label(
    mocker,
    fake_redis_client,
    orchestrator_config,
    gh_mock,
):
    """With no re-triggerable run IDs, orcest logs and moves on -- it never
    labels needs-human."""
    pr_state = _make_stale_pr_state(number=100, head_sha="stale222", stale_run_ids=[])

    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        return_value=[pr_state],
    )
    mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    logger = logging.getLogger("test")
    _poll_cycle(orchestrator_config, fake_redis_client, fake_redis_client, {}, logger, 3600)

    gh_mock.add_label.assert_not_called()
    gh_mock.post_comment.assert_not_called()

    # No cancel/rerun operations
    gh_mock.cancel_workflow.assert_not_called()
    gh_mock.rerun_workflow.assert_not_called()

    # Cooldown SHA must still be recorded to prevent re-handling every cycle
    recorded_sha = get_stale_retrigger_sha(fake_redis_client, orchestrator_config.github.repo, 100)
    assert recorded_sha == "stale222"


def test_poll_cycle_retrigger_stale_checks_cancels_and_reruns(
    mocker,
    fake_redis_client,
    orchestrator_config,
    gh_mock,
):
    """With run IDs present, cancels and re-triggers each workflow run."""
    pr_state = _make_stale_pr_state(number=101, head_sha="stale333", stale_run_ids=[2001, 2002])

    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        return_value=[pr_state],
    )
    mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    logger = logging.getLogger("test")
    _poll_cycle(orchestrator_config, fake_redis_client, fake_redis_client, {}, logger, 3600)

    # cancel_workflow called for each run ID
    assert gh_mock.cancel_workflow.call_count == 2
    cancelled_run_ids = {call.args[1] for call in gh_mock.cancel_workflow.call_args_list}
    assert cancelled_run_ids == {2001, 2002}

    # rerun_workflow called for each run ID (best-effort)
    assert gh_mock.rerun_workflow.call_count == 2
    rerun_run_ids = {call.args[1] for call in gh_mock.rerun_workflow.call_args_list}
    assert rerun_run_ids == {2001, 2002}

    # A comment should be posted since cancellations succeeded
    gh_mock.post_comment.assert_called_once()
    comment_body = gh_mock.post_comment.call_args[0][2]
    assert "Cancelled 2 of 2" in comment_body

    # Cooldown SHA must be recorded
    recorded_sha = get_stale_retrigger_sha(fake_redis_client, orchestrator_config.github.repo, 101)
    assert recorded_sha == "stale333"


def test_stale_self_cancelled_failure_suppressed_on_next_discovery(
    mocker,
    fake_redis_client,
    orchestrator_config,
    gh_mock,
):
    """A stale run cancelled by the loop is not routed to a fix on the next discovery."""
    from orcest.orchestrator import pr_ops

    run_id = 2101
    pr_state = _make_stale_pr_state(number=103, head_sha="stale555", stale_run_ids=[run_id])

    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        return_value=[pr_state],
    )
    mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    logger = logging.getLogger("test")
    _poll_cycle(orchestrator_config, fake_redis_client, fake_redis_client, {}, logger, 3600)

    gh_mock.list_open_prs.return_value = [
        {
            "number": pr_state.number,
            "title": pr_state.title,
            "headRefName": pr_state.branch,
            "headRefOid": pr_state.head_sha,
            "baseRefName": "main",
            "labels": [],
            "isDraft": False,
            "isLocked": False,
            "mergeable": "MERGEABLE",
            "reviewDecision": "",
        }
    ]
    gh_mock.get_ci_status.return_value = [
        {
            "name": "build",
            "conclusion": "cancelled",
            "detailsUrl": f"https://github.com/org/repo/actions/runs/{run_id}/job/1",
        }
    ]

    results = pr_ops.discover_actionable_prs(
        repo=orchestrator_config.github.repo,
        token=orchestrator_config.github.token,
        redis=fake_redis_client,
        label_config=orchestrator_config.labels,
    )

    assert len(results) == 1
    assert results[0].action == PRAction.SKIP_PENDING
    assert results[0].ci_failures == []


def test_poll_cycle_retrigger_stale_checks_sets_sha_even_if_cancel_fails(
    mocker,
    fake_redis_client,
    orchestrator_config,
    gh_mock,
):
    """Cooldown SHA is recorded even when all cancel_workflow calls raise."""
    pr_state = _make_stale_pr_state(number=102, head_sha="stale444", stale_run_ids=[3001])

    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        return_value=[pr_state],
    )
    mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)
    gh_mock.cancel_workflow.side_effect = RuntimeError("GitHub error")

    logger = logging.getLogger("test")
    _poll_cycle(orchestrator_config, fake_redis_client, fake_redis_client, {}, logger, 3600)

    # Cancel was attempted
    gh_mock.cancel_workflow.assert_called_once()

    # Cooldown SHA must still be set to prevent a retry busy-loop
    recorded_sha = get_stale_retrigger_sha(fake_redis_client, orchestrator_config.github.repo, 102)
    assert recorded_sha == "stale444"

    # rerun is still attempted best-effort even when cancel raises
    gh_mock.rerun_workflow.assert_called_once()

    # No success comment since cancel failed
    gh_mock.post_comment.assert_not_called()


# ---------------------------------------------------------------------------
# Multi-project tests
# ---------------------------------------------------------------------------


def _make_multi_project_config(label_config=None):
    """Build an OrchestratorConfig with two ProjectConfig entries."""
    from orcest.shared.config import LabelConfig

    if label_config is None:
        label_config = LabelConfig()
    return OrchestratorConfig(
        labels=label_config,
        projects=[
            ProjectConfig(
                repo="acme/frontend",
                token="token-frontend",
                claude_tokens=["claude-frontend"],
                key_prefix="frontend",
            ),
            ProjectConfig(
                repo="acme/backend",
                token="token-backend",
                claude_tokens=["claude-backend"],
                key_prefix="backend",
            ),
        ],
    )


def test_poll_cycle_multi_project_polls_each_project(mocker, fake_redis_client, gh_mock):
    """_poll_cycle calls discover_actionable_prs once per project with the correct repo/token."""
    config = _make_multi_project_config()

    mock_discover_prs = mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        return_value=[],
    )
    # Override the autouse fixture's mock for discover_actionable_issues
    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_issues",
        return_value=[],
    )
    mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")

    # Ensure consumer groups exist for both prefixes
    for project in config.projects:
        project_redis = fake_redis_client.__class__.from_client(
            fake_redis_client.client, key_prefix=project.key_prefix
        )
        project_redis.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    logger = logging.getLogger("test")
    _poll_cycle(config, fake_redis_client, fake_redis_client, {}, logger, 3600)

    # discover_actionable_prs should be called once per project
    assert mock_discover_prs.call_count == 2

    # Extract the repo and token from each call
    calls = mock_discover_prs.call_args_list
    call_repos = {c.kwargs["repo"] for c in calls}
    call_tokens = {c.kwargs["token"] for c in calls}

    assert call_repos == {"acme/frontend", "acme/backend"}
    assert call_tokens == {"token-frontend", "token-backend"}


def test_poll_cycle_multi_project_error_isolation(mocker, fake_redis_client, gh_mock):
    """An exception in one project does not prevent the other from being polled."""
    config = _make_multi_project_config()

    call_repos: list[str] = []

    def discover_side_effect(*, repo, token, **kwargs):
        call_repos.append(repo)
        if repo == "acme/frontend":
            raise RuntimeError("GitHub is down for frontend")
        return []

    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        side_effect=discover_side_effect,
    )
    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_issues",
        return_value=[],
    )
    mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")

    # Ensure consumer groups exist for both prefixes
    for project in config.projects:
        project_redis = fake_redis_client.__class__.from_client(
            fake_redis_client.client, key_prefix=project.key_prefix
        )
        project_redis.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    logger = logging.getLogger("test")

    # Should not raise -- per-project error isolation catches and logs
    _poll_cycle(config, fake_redis_client, fake_redis_client, {}, logger, 3600)

    # Both projects were attempted despite the first one raising
    assert "acme/frontend" in call_repos
    assert "acme/backend" in call_repos


def test_consume_results_multi_project_isolates_streams(fake_redis_client, gh_mock):
    """Results written to project A's stream are not visible to project B."""
    from orcest.shared.config import LabelConfig
    from orcest.shared.redis_client import RedisClient

    label_config = LabelConfig()

    project_a = ProjectConfig(
        repo="acme/frontend",
        token="token-a",
        claude_tokens=["claude-a"],
        key_prefix="proj_a",
    )
    project_b = ProjectConfig(
        repo="acme/backend",
        token="token-b",
        claude_tokens=["claude-b"],
        key_prefix="proj_b",
    )

    # Create per-project Redis clients sharing the same underlying connection
    # but with different key prefixes (mirrors what _poll_cycle does)
    redis_a = RedisClient.from_client(fake_redis_client.client, key_prefix="proj_a")
    redis_b = RedisClient.from_client(fake_redis_client.client, key_prefix="proj_b")

    # Ensure consumer groups for both
    redis_a.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)
    redis_b.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    # Write a result to project A's results stream
    result_a = TaskResult(
        task_id="task-a-001",
        worker_id="worker-1",
        status=ResultStatus.COMPLETED,
        branch="fix/a",
        summary="Fixed frontend tests",
        duration_seconds=60,
        resource_type="pr",
        resource_id=10,
    )
    redis_a.xadd(RESULTS_STREAM, result_a.to_dict())

    logger = logging.getLogger("test")

    # Consume results for project A -- should process the result
    _consume_results_for_project(project_a, redis_a, label_config, logger)

    # Verify it was consumed (read again -- nothing pending)
    entries_a = redis_a.xreadgroup(
        group=RESULTS_GROUP,
        consumer="orchestrator-main",
        stream=RESULTS_STREAM,
        count=10,
        block_ms=None,
    )
    assert entries_a == []

    # Consume results for project B -- should NOT see project A's result
    _consume_results_for_project(project_b, redis_b, label_config, logger)

    # Verify project B's stream is empty (no results were ever written to it)
    entries_b = redis_b.xreadgroup(
        group=RESULTS_GROUP,
        consumer="orchestrator-main",
        stream=RESULTS_STREAM,
        count=10,
        block_ms=None,
    )
    assert entries_b == []

    # No GitHub operations should have been triggered for project B
    # (gh_mock tracks all calls across both projects; only project A had a result
    # and COMPLETED results are silent -- no comments or labels)
    gh_mock.post_comment.assert_not_called()
    gh_mock.add_label.assert_not_called()


# ---------------------------------------------------------------------------
# Merge network-error retry tests
# ---------------------------------------------------------------------------


def _make_merge_pr_state(number: int = 42) -> PRState:
    """Build a minimal PRState with MERGE action for merge tests."""
    return PRState(
        number=number,
        title=f"PR #{number}",
        branch=f"feat/{number}",
        head_sha="abc123",
        action=PRAction.MERGE,
        ci_failures=[],
        review_threads=[],
        labels=[],
    )


def test_merge_network_error_skips_needs_human(
    mocker,
    fake_redis_client,
    orchestrator_config,
    gh_mock,
):
    """A transient network error during merge skips the needs-human label."""
    pr_state = _make_merge_pr_state(number=300)

    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        return_value=[pr_state],
    )
    mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    gh_mock.merge_pr.side_effect = RuntimeError("TLS handshake timeout")
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    logger = logging.getLogger("test")
    _poll_cycle(orchestrator_config, fake_redis_client, fake_redis_client, {}, logger, 3600)

    # Should NOT label needs-human for transient network error
    gh_mock.add_label.assert_not_called()
    # Should NOT post a comment
    gh_mock.post_comment.assert_not_called()


def test_merge_github_gateway_timeout_skips_needs_human(
    mocker,
    fake_redis_client,
    orchestrator_config,
    gh_mock,
):
    """A GitHub API 504 during merge skips the needs-human label."""
    pr_state = _make_merge_pr_state(number=305)

    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        return_value=[pr_state],
    )
    mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    gh_mock.merge_pr.side_effect = RuntimeError(
        "gh command failed (exit 1): HTTP 504: We couldn't respond to your "
        "request in time. Sorry about that. Please try resubmitting your request "
        "and contact us if the problem persists. (https://api.github.com/graphql)"
    )
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    logger = logging.getLogger("test")
    _poll_cycle(orchestrator_config, fake_redis_client, fake_redis_client, {}, logger, 3600)

    gh_mock.add_label.assert_not_called()
    gh_mock.post_comment.assert_not_called()


def test_merge_network_error_retry_exhaustion_backs_off(
    mocker,
    fake_redis_client,
    orchestrator_config,
    gh_mock,
):
    """After exceeding merge retry budget, network error falls through to needs-human."""
    from orcest.orchestrator.loop import _increment_merge_retries

    pr_state = _make_merge_pr_state(number=301)
    repo = orchestrator_config.github.repo

    # Pre-populate 5 retries (the max) so the next one should fall through
    for _ in range(5):
        _increment_merge_retries(fake_redis_client, repo, 301)

    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        return_value=[pr_state],
    )
    mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    gh_mock.merge_pr.side_effect = RuntimeError("TLS handshake timeout")
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    logger = logging.getLogger("test")
    _poll_cycle(orchestrator_config, fake_redis_client, fake_redis_client, {}, logger, 3600)

    # Past the merge retry budget orcest backs off and retries -- never labels.
    gh_mock.add_label.assert_not_called()
    gh_mock.post_comment.assert_not_called()
    assert get_backoff_step(fake_redis_client, repo, 301) is not None


def test_merge_conflict_not_classified_as_network_error(
    mocker,
    fake_redis_client,
    orchestrator_config,
    gh_mock,
):
    """A merge conflict is NOT classified as a network error (existing behavior preserved)."""
    pr_state = _make_merge_pr_state(number=302)

    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        return_value=[pr_state],
    )
    mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    mocker.patch("orcest.orchestrator.loop.publish_rebase_task")
    # Merge conflict error triggers the rebase path, not network retry
    gh_mock.merge_pr.side_effect = RuntimeError(
        "is not mergeable: the merge commit cannot be cleanly created"
    )
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    logger = logging.getLogger("test")
    _poll_cycle(orchestrator_config, fake_redis_client, fake_redis_client, {}, logger, 3600)

    # Merge conflict should trigger rebase, not needs-human or network retry
    # (the conflict handler attempts publish_rebase_task, not add_label)
    # Verify needs-human was NOT labeled (rebase path was taken)
    # Note: publish_rebase_task is mocked, so it won't actually enqueue
    gh_mock.add_label.assert_not_called()


def test_merge_gh_rate_limit_error_not_classified_as_network_error(
    mocker,
    fake_redis_client,
    orchestrator_config,
    gh_mock,
):
    """GhRateLimitError bypasses the network-retry path and backs off -- it is
    never escalated to needs-human."""
    from orcest.orchestrator.gh import GhRateLimitError

    pr_state = _make_merge_pr_state(number=303)

    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        return_value=[pr_state],
    )
    mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    gh_mock.merge_pr.side_effect = GhRateLimitError("timed out waiting for rate limit reset")
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    logger = logging.getLogger("test")
    _poll_cycle(orchestrator_config, fake_redis_client, fake_redis_client, {}, logger, 3600)

    gh_mock.add_label.assert_not_called()
    assert (
        get_backoff_step(fake_redis_client, orchestrator_config.projects[0].repo, 303) is not None
    )


def test_non_network_merge_error_backs_off(
    mocker,
    fake_redis_client,
    orchestrator_config,
    gh_mock,
):
    """A non-network, non-conflict merge error backs off and retries -- it is
    never escalated to needs-human."""
    pr_state = _make_merge_pr_state(number=304)

    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        return_value=[pr_state],
    )
    mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    gh_mock.merge_pr.side_effect = RuntimeError("branch protection rule violation")
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    logger = logging.getLogger("test")
    _poll_cycle(orchestrator_config, fake_redis_client, fake_redis_client, {}, logger, 3600)

    gh_mock.add_label.assert_not_called()
    gh_mock.post_comment.assert_not_called()
    assert (
        get_backoff_step(fake_redis_client, orchestrator_config.projects[0].repo, 304) is not None
    )


def test_required_checks_expected_merge_error_updates_branch(
    mocker,
    fake_redis_client,
    orchestrator_config,
    gh_mock,
):
    """Branch protection saying checks are expected updates the branch instead of escalating."""
    pr_state = _make_merge_pr_state(number=305)

    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        return_value=[pr_state],
    )
    mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    gh_mock.merge_pr.side_effect = RuntimeError(
        "Repository rule violations found: 11 of 11 required status checks are expected"
    )
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    logger = logging.getLogger("test")
    _poll_cycle(orchestrator_config, fake_redis_client, fake_redis_client, {}, logger, 3600)

    gh_mock.update_branch.assert_called_once_with(
        orchestrator_config.github.repo,
        305,
        orchestrator_config.github.token,
        expected_head_sha=pr_state.head_sha,
    )
    gh_mock.add_label.assert_not_called()
    gh_mock.post_comment.assert_not_called()


def test_required_status_check_expected_singular_matches_recovery_predicate():
    """GitHub may use singular 'required status check' for one required check."""
    assert _is_required_checks_expected_error(
        'Repository rule violations found: Required status check "test" is expected.'
    )


def test_required_checks_expected_update_branch_noop_backs_off(
    mocker,
    fake_redis_client,
    orchestrator_config,
    gh_mock,
):
    """If update-branch is a no-op, the merge failure backs off -- it never
    labels needs-human."""
    pr_state = _make_merge_pr_state(number=306)

    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        return_value=[pr_state],
    )
    mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    gh_mock.merge_pr.side_effect = RuntimeError(
        "Repository rule violations found: required status check is expected"
    )
    gh_mock.update_branch.return_value = False
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    logger = logging.getLogger("test")
    _poll_cycle(orchestrator_config, fake_redis_client, fake_redis_client, {}, logger, 3600)

    gh_mock.update_branch.assert_called_once_with(
        orchestrator_config.github.repo,
        306,
        orchestrator_config.github.token,
        expected_head_sha=pr_state.head_sha,
    )
    gh_mock.add_label.assert_not_called()
    gh_mock.post_comment.assert_not_called()


def test_required_checks_expected_update_branch_failure_backs_off(
    mocker,
    fake_redis_client,
    orchestrator_config,
    gh_mock,
):
    """If the recovery update-branch call fails, the merge failure backs off --
    it never labels needs-human."""
    pr_state = _make_merge_pr_state(number=307)

    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        return_value=[pr_state],
    )
    mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    gh_mock.merge_pr.side_effect = RuntimeError(
        "Repository rule violations found: 11 of 11 required status checks are expected"
    )
    gh_mock.update_branch.side_effect = RuntimeError("update branch failed")
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    logger = logging.getLogger("test")
    _poll_cycle(orchestrator_config, fake_redis_client, fake_redis_client, {}, logger, 3600)

    gh_mock.update_branch.assert_called_once()
    gh_mock.add_label.assert_not_called()
    assert (
        get_backoff_step(fake_redis_client, orchestrator_config.projects[0].repo, 307) is not None
    )


def test_required_checks_expected_update_branch_stale_head_skips_needs_human(
    mocker,
    fake_redis_client,
    orchestrator_config,
    gh_mock,
):
    """A stale-head update-branch recovery failure is stale discovery, not terminal."""
    pr_state = _make_merge_pr_state(number=308)

    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        return_value=[pr_state],
    )
    mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    gh_mock.merge_pr.side_effect = RuntimeError(
        "Repository rule violations found: required status check is expected"
    )
    gh_mock.update_branch.side_effect = RuntimeError(
        "expected_head_sha does not match the pull request head sha"
    )
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    logger = logging.getLogger("test")
    _poll_cycle(orchestrator_config, fake_redis_client, fake_redis_client, {}, logger, 3600)

    gh_mock.update_branch.assert_called_once_with(
        orchestrator_config.github.repo,
        308,
        orchestrator_config.github.token,
        expected_head_sha=pr_state.head_sha,
    )
    gh_mock.add_label.assert_not_called()
    gh_mock.post_comment.assert_not_called()


def test_stale_head_error_predicate_matches_known_messages():
    """Known stale-head messages should take the non-terminal retry path."""
    assert _is_stale_head_error("match-head-commit rejected the merge")
    assert _is_stale_head_error("expected_head_sha does not match the current head sha")
    assert _is_stale_head_error("Head branch was modified. Review and try again.")


def test_full_multi_provider_flow_exhaust_one_continue_on_other(
    mocker, fake_redis_client, orchestrator_config, gh_mock
):
    """End-to-end multi-provider task flow verification (Task 9).

    - Mixed project ProviderPool (claude + grok entries)
    - Exhaust all grok entries (simulating rate limit on one provider)
    - _poll_cycle must continue selecting from the remaining provider (claude)
    - Verify register/publish wiring receives the correct lean entry
    - Per-provider exhausted_skip counters are incremented only for the
      exhausted provider (grok), not for claude
    - Round-robin semantics preserved for the surviving provider
    - Covers PR fix + (via discovery) potential issue paths in spirit

    Respects the lean Provider Registration & Invocation Boundary: only
    provider/credential/model/identity() ever cross from pool to publish.
    """
    from datetime import datetime, timedelta, timezone

    from orcest.orchestrator.loop import _PROVIDER_EXHAUSTED_SKIP_KEY
    from orcest.orchestrator.provider_pool import ProviderPool
    from orcest.shared.providers import ProviderEntry

    pr_state = _make_pr_state(number=900, action=PRAction.ENQUEUE_FIX)

    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        return_value=[pr_state],
    )
    publish_spy = mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    mocker.patch("orcest.orchestrator.loop.publish_rebase_task")
    mocker.patch("orcest.orchestrator.loop.publish_issue_task")
    fake_redis_client.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    # Mixed pool: 1 claude + 2 grok (so we can exhaust "the grok provider")
    c = ProviderEntry(provider="claude", credential="claude-flow-001")
    g1 = ProviderEntry(provider="grok", credential="grok-flow-gg1", model="grok-3")
    g2 = ProviderEntry(provider="grok", credential="grok-flow-gg2", model="grok-3")
    pool = ProviderPool([c, g1, g2])

    # Exhaust the grok provider entries (all of them) via public API
    for i, ge in enumerate([g1, g2]):
        tid = f"g-exh-{i}"
        pool.register_task(tid, ge)
        pool.mark_exhausted(tid, resets_at=datetime.now(timezone.utc) + timedelta(hours=1))

    # Only claude should remain available (grok fully benched independently)
    assert pool.available_count == 1

    project_key = orchestrator_config.projects[0].key_prefix

    logger = logging.getLogger("test.multi-flow")
    _poll_cycle(
        orchestrator_config,
        fake_redis_client,
        fake_redis_client,
        {project_key: pool},
        logger,
        3600,
    )

    # Publish succeeded using the surviving provider (claude) via the lean
    # provider/credential/model surface passed to publish_fix_task.
    # This proves "exhaust one provider, the system continues on the other".
    publish_spy.assert_called_once()
    assert publish_spy.call_args.kwargs["provider_account"] == c.account_key()

    # To also cover the full-exhaustion + per-provider counter path for mixed,
    # exhaust the remaining claude and run another cycle: now selection returns
    # None and per-provider exhausted_skip counters are bumped for every
    # provider name present in the pool (grok + claude).
    # (existing single-provider tests already cover the counter logic in depth)
    last_claude = pool.next_entry()
    if last_claude:
        pool.register_task("last-c", last_claude)
        pool.mark_exhausted("last-c", resets_at=datetime.now(timezone.utc) + timedelta(hours=1))

    # second poll: fully exhausted mixed pool
    publish_spy.reset_mock()
    _poll_cycle(
        orchestrator_config,
        fake_redis_client,
        fake_redis_client,
        {project_key: pool},
        logger,
        3600,
    )
    publish_spy.assert_not_called()
    # skip counter should now be >0
    raw2 = fake_redis_client.get(_PROVIDER_EXHAUSTED_SKIP_KEY)
    assert raw2 is not None and int(raw2) >= 1


# ---------------------------------------------------------------------------
# Credential write-back persistence (rotated OAuth blobs survive restart)
# ---------------------------------------------------------------------------


def test_credential_override_persist_and_load_round_trip(fake_redis_client):
    """A rotated blob persisted to shared Redis is restored into a fresh pool
    at startup (simulating an orchestrator restart) — not reverting to the
    stale config blob."""
    import logging

    from orcest.orchestrator.loop import (
        _SHARED_CREDENTIAL_OVERRIDES_KEY,
        _load_credential_overrides,
        _persist_credential_override,
    )
    from orcest.orchestrator.provider_pool import ProviderPool
    from orcest.shared.providers import ProviderEntry

    logger = logging.getLogger("test")
    entry = ProviderEntry(provider="grok", credential="config-blob")
    account = entry.account_key()
    shared_redis = RedisClient.from_client(fake_redis_client.client, key_prefix="shared")

    # Worker reported a rotated blob; orchestrator persisted it.
    _persist_credential_override(
        fake_redis_client,
        account,
        "rotated-blob",
        123.0,
        logger,
        shared_redis=shared_redis,
    )
    assert shared_redis.hgetall(_SHARED_CREDENTIAL_OVERRIDES_KEY)

    # Fresh pool from the SAME config (restart) starts with the config blob...
    pool = ProviderPool([entry])
    assert pool.effective_credential(entry) == "config-blob"
    # ...then load from Redis restores the rotated blob.
    _load_credential_overrides(fake_redis_client, pool, logger, shared_redis=shared_redis)
    assert pool.effective_credential(entry) == "rotated-blob"


def test_shared_credential_override_rejects_out_of_order_rotation(fake_redis_client):
    from orcest.orchestrator.loop import (
        _SHARED_CREDENTIAL_OVERRIDES_KEY,
        _persist_credential_override,
    )
    from orcest.shared.providers import ProviderEntry

    entry = ProviderEntry(provider="codex", credential="config-blob")
    shared = RedisClient.from_client(fake_redis_client.client, key_prefix="shared")
    logger = logging.getLogger("test")

    assert (
        _persist_credential_override(
            fake_redis_client,
            entry.account_key(),
            "newer",
            200.0,
            logger,
            shared_redis=shared,
        )
        is True
    )
    assert (
        _persist_credential_override(
            fake_redis_client,
            entry.account_key(),
            "older",
            100.0,
            logger,
            shared_redis=shared,
        )
        is False
    )
    stored = json.loads(shared.hget(_SHARED_CREDENTIAL_OVERRIDES_KEY, entry.account_key()) or "{}")
    assert stored == {"blob": "newer", "minted_at": 200.0}


def test_credential_override_loads_legacy_project_identity(fake_redis_client):
    import logging

    from orcest.orchestrator.loop import _CREDENTIAL_OVERRIDES_KEY, _load_credential_overrides
    from orcest.orchestrator.provider_pool import ProviderPool
    from orcest.shared.providers import ProviderEntry

    entry = ProviderEntry(provider="grok", credential="config-blob")
    fake_redis_client.hset(
        _CREDENTIAL_OVERRIDES_KEY,
        entry.identity(),
        json.dumps({"blob": "legacy-rotated-blob", "minted_at": 123.0}),
    )
    pool = ProviderPool([entry])
    _load_credential_overrides(fake_redis_client, pool, logging.getLogger("test"))
    assert pool.effective_credential(entry) == "legacy-rotated-blob"


def test_credential_override_load_ignores_corrupt_entries(fake_redis_client):
    import logging

    from orcest.orchestrator.loop import _CREDENTIAL_OVERRIDES_KEY, _load_credential_overrides
    from orcest.orchestrator.provider_pool import ProviderPool
    from orcest.shared.providers import ProviderEntry

    entry = ProviderEntry(provider="grok", credential="config-blob")
    fake_redis_client.hset(_CREDENTIAL_OVERRIDES_KEY, entry.identity(), "not-json{")
    pool = ProviderPool([entry])
    _load_credential_overrides(fake_redis_client, pool, logging.getLogger("test"))
    # Corrupt entry skipped; falls back to the config blob (no crash).
    assert pool.effective_credential(entry) == "config-blob"


def test_credential_override_load_ignores_grok_blob_without_refresh_token(fake_redis_client):
    import logging

    from orcest.orchestrator.loop import _CREDENTIAL_OVERRIDES_KEY, _load_credential_overrides
    from orcest.orchestrator.provider_pool import ProviderPool
    from orcest.shared.providers import ProviderEntry

    entry = ProviderEntry(provider="grok", credential="config-blob")
    fake_redis_client.hset(
        _CREDENTIAL_OVERRIDES_KEY,
        entry.account_key(),
        json.dumps({"blob": json.dumps({"key": "access-token-only"}), "minted_at": 123.0}),
    )
    pool = ProviderPool([entry])
    _load_credential_overrides(fake_redis_client, pool, logging.getLogger("test"))
    assert pool.effective_credential(entry) == "config-blob"


def test_credential_update_uses_persisted_task_account_after_restart(fake_redis_client, mocker):
    import logging

    from orcest.orchestrator.provider_pool import ProviderPool
    from orcest.shared.config import LabelConfig, ProjectConfig
    from orcest.shared.providers import ProviderEntry

    entry = ProviderEntry(provider="grok", credential="config-blob")
    pool = ProviderPool([entry])
    shared_redis = RedisClient.from_client(fake_redis_client.client, key_prefix="shared")
    task_id = "task-after-restart"
    rotated = '{"access_token":"new-access","refresh_token":"new-refresh"}'
    fake_redis_client.hset(_TASK_PROVIDER_ACCOUNTS_KEY, task_id, entry.account_key())

    result = _make_task_result(
        status=ResultStatus.COMPLETED,
        task_id=task_id,
        pr_number=77,
    )
    result.credential_update = rotated
    mocker.patch(
        "orcest.orchestrator.loop.gh.get_pr",
        return_value={"headRefOid": result.snapshot_head_sha, "statusCheckRollup": []},
    )

    _handle_result(
        ProjectConfig(
            repo="owner/testrepo",
            token="fake-token",
            claude_tokens=[],
            key_prefix="test",
            providers=[entry],
        ),
        LabelConfig(),
        fake_redis_client,
        result,
        logging.getLogger("test"),
        token_pool=pool,
        shared_credential_redis=shared_redis,
    )

    assert pool.effective_credential(entry) == rotated
    assert shared_redis.hgetall(_SHARED_CREDENTIAL_OVERRIDES_KEY)
    assert fake_redis_client.hget(_TASK_PROVIDER_ACCOUNTS_KEY, task_id) is None


def test_pre_upgrade_retained_task_backfills_account_before_result(
    fake_redis_client,
    gh_mock,
):
    """A task published by 6fb2edd has no account field or Redis mapping.

    Startup reconstructs its non-secret account from the retained task entry,
    ignores unrelated ``tasks:*`` non-stream keys, and a fresh pool can then
    persist a rotated credential from an old-worker result.
    """
    from orcest.orchestrator.loop import _backfill_retained_task_provider_accounts
    from orcest.orchestrator.provider_pool import ProviderPool
    from orcest.shared.config import LabelConfig, ProjectConfig
    from orcest.shared.models import Task, TaskType
    from orcest.shared.providers import ProviderEntry

    original = '{"access_token":"old","refresh_token":"old-refresh"}'
    rotated = '{"access_token":"new","refresh_token":"new-refresh"}'
    entry = ProviderEntry(provider="grok", credential=original)
    project = ProjectConfig(
        repo="owner/testrepo",
        token="fake-token",
        claude_tokens=[],
        key_prefix="test",
        providers=[entry],
    )
    task_id = "pre-upgrade-in-flight-task"
    legacy_task = Task.create(
        task_type=TaskType.FIX_PR,
        repo=project.repo,
        token=project.token,
        resource_type="pr",
        resource_id=82,
        prompt="fix it",
        branch="fix/82",
        key_prefix=project.key_prefix,
        provider="grok",
        credential=original,
        task_id=task_id,
    )
    task_redis = RedisClient.from_client(fake_redis_client.client, key_prefix="shared")
    legacy_fields = legacy_task.to_dict()
    legacy_fields.pop("provider_account")
    task_redis.xadd("tasks:grok", legacy_fields)
    task_redis.set_value("tasks:metadata", "dashboard-smoke-key")
    mapping_key = f"{_TASK_PROVIDER_ACCOUNT_PREFIX}{task_id}"
    assert fake_redis_client.get(mapping_key) is None
    assert fake_redis_client.hget(_TASK_PROVIDER_ACCOUNTS_KEY, task_id) is None

    pool = ProviderPool([entry])
    count = _backfill_retained_task_provider_accounts(
        task_redis,
        ["tasks:grok", "tasks:issue:grok"],
        [(project, fake_redis_client)],
        {project.key_prefix: pool},
        ttl_seconds=300,
        logger=logging.getLogger("test"),
    )

    assert count == 1
    assert fake_redis_client.get(mapping_key) == entry.account_key()
    assert original not in (fake_redis_client.get(mapping_key) or "")

    result = _make_task_result(
        status=ResultStatus.COMPLETED,
        task_id=task_id,
        pr_number=82,
    )
    result.credential_update = rotated
    assert result.provider_account == ""  # old-worker wire payload

    _handle_result(
        project,
        LabelConfig(),
        fake_redis_client,
        result,
        logging.getLogger("test"),
        token_pool=pool,
        shared_credential_redis=task_redis,
    )

    stored = json.loads(
        task_redis.hget(_SHARED_CREDENTIAL_OVERRIDES_KEY, entry.account_key()) or "{}"
    )
    assert stored["blob"] == rotated
    assert pool.effective_credential(entry) == rotated


def test_pre_upgrade_backfill_matches_current_rotation_to_original_account(
    fake_redis_client,
):
    from orcest.orchestrator.loop import _backfill_retained_task_provider_accounts
    from orcest.orchestrator.provider_pool import ProviderPool
    from orcest.shared.models import Task, TaskType
    from orcest.shared.providers import ProviderEntry

    original = '{"access_token":"original","refresh_token":"original-refresh"}'
    current = '{"access_token":"current","refresh_token":"current-refresh"}'
    entry = ProviderEntry(provider="grok", credential=original)
    project = ProjectConfig(
        repo="owner/testrepo",
        token="fake-token",
        claude_tokens=[],
        key_prefix="test",
        providers=[entry],
    )
    pool = ProviderPool([entry])
    pool.seed_credential_override(entry.account_key(), current, minted_at=100.0)
    task = Task.create(
        task_type=TaskType.FIX_PR,
        repo=project.repo,
        token=project.token,
        resource_type="pr",
        resource_id=83,
        prompt="fix it",
        key_prefix=project.key_prefix,
        provider="grok",
        credential=current,
        task_id="legacy-task-after-prior-rotation",
    )
    task_redis = RedisClient.from_client(fake_redis_client.client, key_prefix="shared")
    legacy_fields = task.to_dict()
    legacy_fields.pop("provider_account")
    task_redis.xadd("tasks:grok", legacy_fields)

    count = _backfill_retained_task_provider_accounts(
        task_redis,
        ["tasks:grok"],
        [(project, fake_redis_client)],
        {project.key_prefix: pool},
        ttl_seconds=300,
        logger=logging.getLogger("test"),
    )

    assert count == 1
    assert fake_redis_client.get(f"{_TASK_PROVIDER_ACCOUNT_PREFIX}{task.id}") == entry.account_key()
    assert entry.account_key() != ProviderEntry("grok", current).account_key()


def test_credential_update_without_validated_account_remains_retryable(
    fake_redis_client,
):
    from orcest.orchestrator.provider_pool import ProviderPool
    from orcest.shared.config import LabelConfig
    from orcest.shared.providers import ProviderEntry

    entry = ProviderEntry(
        provider="grok",
        credential='{"access_token":"old","refresh_token":"old-refresh"}',
    )
    result = _make_task_result(
        status=ResultStatus.COMPLETED,
        task_id="unmapped-pre-upgrade-task",
        pr_number=84,
    )
    result.credential_update = '{"access_token":"new","refresh_token":"new-refresh"}'

    with pytest.raises(_RetryableResultError, match="no unambiguous"):
        _handle_result(
            ProjectConfig(
                repo="owner/testrepo",
                token="fake-token",
                claude_tokens=[],
                key_prefix="test",
                providers=[entry],
            ),
            LabelConfig(),
            fake_redis_client,
            result,
            logging.getLogger("test"),
            token_pool=ProviderPool([entry]),
            shared_credential_redis=RedisClient.from_client(
                fake_redis_client.client,
                key_prefix="shared",
            ),
        )


def test_credential_update_without_validated_account_stops_retrying(
    fake_redis_client,
    mocker,
):
    """Retries are bounded so an unresolvable account cannot wedge the entry.

    Regression: this raised `_RetryableResultError` unconditionally. The
    resolution inputs are static, so an operator re-auth (or a task published
    under a prior config) never becomes resolvable -- the results PEL entry was
    deferred on every cycle forever, blocking the label/comment/pending-marker
    side effects, leaking the task->account mapping, and re-burning GitHub API
    calls each poll.
    """
    from orcest.orchestrator.loop import _MAX_UNRESOLVED_CREDENTIAL_ACCOUNT_ATTEMPTS
    from orcest.orchestrator.provider_pool import ProviderPool
    from orcest.shared.config import LabelConfig
    from orcest.shared.providers import ProviderEntry

    entry = ProviderEntry(
        provider="grok",
        credential='{"access_token":"old","refresh_token":"old-refresh"}',
    )
    result = _make_task_result(
        status=ResultStatus.COMPLETED,
        task_id="unmapped-pre-upgrade-task",
        pr_number=84,
    )
    result.credential_update = '{"access_token":"new","refresh_token":"new-refresh"}'
    result.provider_account = "grok:deadbeef"
    mocker.patch(
        "orcest.orchestrator.loop.gh.get_pr",
        return_value={"headRefOid": result.snapshot_head_sha, "statusCheckRollup": []},
    )

    def handle():
        _handle_result(
            ProjectConfig(
                repo="owner/testrepo",
                token="fake-token",
                claude_tokens=[],
                key_prefix="test",
                providers=[entry],
            ),
            LabelConfig(),
            fake_redis_client,
            result,
            logging.getLogger("test"),
            token_pool=ProviderPool([entry]),
        )

    for _ in range(_MAX_UNRESOLVED_CREDENTIAL_ACCOUNT_ATTEMPTS - 1):
        with pytest.raises(_RetryableResultError, match="no unambiguous"):
            handle()

    # The bound is reached: the result is processed instead of deferred again.
    handle()
    assert fake_redis_client.get("providers:grok:credential_update_unresolved_account") == str(
        _MAX_UNRESOLVED_CREDENTIAL_ACCOUNT_ATTEMPTS
    )


def test_permanent_comment_failure_stops_wedging_the_result(fake_redis_client, gh_mock):
    """A permanently failing GitHub side effect must not defer the entry forever.

    Regression: any `_post_comment` failure raised `_RetryableResultError`
    unconditionally. A locked conversation or a revoked `issues:write` scope
    never succeeds, so the results PEL entry was deferred on every poll cycle
    forever -- the side-effect checkpoint was never committed, the
    task->account mapping leaked, and each cycle re-ran the preceding GitHub
    reads. Network errors must still retry without consuming the budget.
    """
    from orcest.orchestrator.loop import (
        _MAX_GITHUB_SIDE_EFFECT_FAILURES,
        _make_result_side_effects_processed_key,
    )
    from orcest.shared.config import LabelConfig

    result = _make_task_result(
        status=ResultStatus.FAILED,
        task_id="locked-conversation-task",
        pr_number=91,
        summary="Investigated; a product decision is required.",
        needs_human=True,
        needs_human_reason="conversation is locked",
    )
    gh_mock.get_pr.return_value = {
        "headRefOid": result.snapshot_head_sha,
        "statusCheckRollup": [],
    }
    gh_mock.has_issue_comment_marker.return_value = False
    gh_mock.post_comment.side_effect = RuntimeError(
        "HTTP 403: Unable to create comment (conversation is locked)"
    )

    def handle():
        _handle_result(
            ProjectConfig(
                repo="owner/testrepo",
                token="fake-token",
                claude_tokens=[],
                key_prefix="test",
            ),
            LabelConfig(),
            fake_redis_client,
            result,
            logging.getLogger("test"),
        )

    for _ in range(_MAX_GITHUB_SIDE_EFFECT_FAILURES - 1):
        with pytest.raises(_RetryableResultError, match="failed to post comment"):
            handle()

    # Budget exhausted: the result commits instead of deferring forever.
    handle()
    assert fake_redis_client.get(_make_result_side_effects_processed_key(result.task_id)) == "1"


def test_network_comment_failure_never_consumes_the_budget(fake_redis_client, gh_mock):
    """Transient network errors stay retryable indefinitely."""
    from orcest.orchestrator.loop import _MAX_GITHUB_SIDE_EFFECT_FAILURES
    from orcest.shared.config import LabelConfig

    result = _make_task_result(
        status=ResultStatus.FAILED,
        task_id="flaky-network-task",
        pr_number=92,
        summary="Investigated; a product decision is required.",
        needs_human=True,
        needs_human_reason="transient network",
    )
    gh_mock.get_pr.return_value = {
        "headRefOid": result.snapshot_head_sha,
        "statusCheckRollup": [],
    }
    gh_mock.has_issue_comment_marker.return_value = False
    gh_mock.post_comment.side_effect = RuntimeError("dial tcp 140.82.121.6:443: i/o timeout")

    for _ in range(_MAX_GITHUB_SIDE_EFFECT_FAILURES + 2):
        with pytest.raises(_RetryableResultError, match="failed to post comment"):
            _handle_result(
                ProjectConfig(
                    repo="owner/testrepo",
                    token="fake-token",
                    claude_tokens=[],
                    key_prefix="test",
                ),
                LabelConfig(),
                fake_redis_client,
                result,
                logging.getLogger("test"),
            )

    assert fake_redis_client.get(f"result:{result.task_id}:github_side_effect_failures") is None


def test_discarded_stale_credential_refresh_is_counted(fake_redis_client, mocker):
    """Dropping an already-performed rotation must leave a health signal.

    Two workers can hold the same OAuth account concurrently (ProviderPool
    makes no reservation), and ordering is decided at publish time, so a
    genuinely newer rotation can lose to an older one. That is invisible in an
    info log; the counter is the only warning before the stored blob is found
    to hold a consumed refresh token.
    """
    from orcest.orchestrator.provider_pool import ProviderPool
    from orcest.shared.config import LabelConfig
    from orcest.shared.providers import ProviderEntry

    entry = ProviderEntry(provider="grok", credential="config-blob")
    pool = ProviderPool([entry])
    task_id = "task-stale-rotation"
    fake_redis_client.hset(_TASK_PROVIDER_ACCOUNTS_KEY, task_id, entry.account_key())
    # A newer rotation is already stored -- in the shared hash, which is where
    # the ordering comparison actually happens -- so this result's update loses.
    newer_minted_at = 1_900_000_000_000_000.0
    shared_redis = RedisClient.from_client(fake_redis_client.client, key_prefix="shared")
    shared_redis.hset_json_if_newer(
        "providers:credential_overrides",
        entry.account_key(),
        json.dumps({"blob": "winning-blob", "minted_at": newer_minted_at}),
        newer_minted_at,
    )
    pool.seed_credential_override(entry.account_key(), "winning-blob", minted_at=newer_minted_at)

    result = _make_task_result(
        status=ResultStatus.COMPLETED,
        task_id=task_id,
        pr_number=93,
    )
    result.credential_update = '{"access_token":"loser","refresh_token":"loser-refresh"}'
    result.credential_update_minted_at = 1_700_000_000_000_000.0
    mocker.patch(
        "orcest.orchestrator.loop.gh.get_pr",
        return_value={"headRefOid": result.snapshot_head_sha, "statusCheckRollup": []},
    )

    _handle_result(
        ProjectConfig(
            repo="owner/testrepo",
            token="fake-token",
            claude_tokens=[],
            key_prefix="test",
            providers=[entry],
        ),
        LabelConfig(),
        fake_redis_client,
        result,
        logging.getLogger("test"),
        token_pool=pool,
        shared_credential_redis=shared_redis,
    )

    counter_key = "providers:grok:credential_refresh_discarded_stale"
    assert fake_redis_client.get(counter_key) == "1"
    assert fake_redis_client.ttl(counter_key) > 0
    assert pool.effective_credential(entry) == "winning-blob"


def test_task_provider_account_uses_atomic_per_task_ttl(fake_redis_client):
    from orcest.orchestrator.loop import (
        _clear_task_provider_account,
        _load_task_provider_account,
        _persist_task_provider_account,
    )
    from orcest.shared.providers import ProviderEntry

    entry = ProviderEntry(provider="grok", credential="config-blob")
    logger = logging.getLogger("test")
    task_id = "task-with-own-ttl"

    _persist_task_provider_account(
        fake_redis_client,
        task_id,
        entry,
        ttl_seconds=90,
        logger=logger,
    )

    key = f"{_TASK_PROVIDER_ACCOUNT_PREFIX}{task_id}"
    assert fake_redis_client.get(key) == entry.account_key()
    assert 0 < fake_redis_client.ttl(key) <= 90
    assert fake_redis_client.hget(_TASK_PROVIDER_ACCOUNTS_KEY, task_id) is None
    assert _load_task_provider_account(fake_redis_client, task_id, logger) == entry.account_key()

    _clear_task_provider_account(fake_redis_client, task_id, logger)
    assert fake_redis_client.get(key) is None


def test_shared_credential_write_failure_retries_after_local_apply(
    fake_redis_client,
    mocker,
):
    from unittest.mock import MagicMock

    from orcest.orchestrator.provider_pool import ProviderPool
    from orcest.shared.config import LabelConfig, ProjectConfig
    from orcest.shared.providers import ProviderEntry

    entry = ProviderEntry(provider="grok", credential="config-blob")
    pool = ProviderPool([entry])
    task_id = "task-retry-shared-rotation"
    pool.register_task(task_id, entry)
    fake_redis_client.set_ex(
        f"{_TASK_PROVIDER_ACCOUNT_PREFIX}{task_id}",
        entry.account_key(),
        ttl=300,
    )
    rotated = '{"access_token":"new","refresh_token":"rotated"}'
    result = _make_task_result(
        status=ResultStatus.COMPLETED,
        task_id=task_id,
        pr_number=81,
    )
    result.credential_update = rotated
    result.credential_update_minted_at = 1_800_000_000_000_001.0
    mocker.patch(
        "orcest.orchestrator.loop.gh.get_pr",
        return_value={"headRefOid": result.snapshot_head_sha, "statusCheckRollup": []},
    )
    shared = MagicMock()
    shared.hset_json_if_newer.side_effect = [ConnectionError("down"), True]
    shared.hget.return_value = json.dumps(
        {
            "blob": rotated,
            "minted_at": result.credential_update_minted_at,
        }
    )
    project = ProjectConfig(
        repo="owner/testrepo",
        token="fake-token",
        claude_tokens=[],
        key_prefix="test",
        providers=[entry],
    )

    with pytest.raises(_RetryableResultError):
        _handle_result(
            project,
            LabelConfig(),
            fake_redis_client,
            result,
            logging.getLogger("test"),
            token_pool=pool,
            shared_credential_redis=shared,
        )

    assert pool.effective_credential(entry) == rotated
    # Retry sees a local duplicate, but still retries the canonical write.
    _handle_result(
        project,
        LabelConfig(),
        fake_redis_client,
        result,
        logging.getLogger("test"),
        token_pool=pool,
        shared_credential_redis=shared,
    )
    assert shared.hset_json_if_newer.call_count == 2


def test_retryable_result_is_left_pending_until_next_consume(
    orchestrator_config,
    mocker,
):
    from unittest.mock import MagicMock

    redis = MagicMock()
    fields = _make_task_result(status=ResultStatus.COMPLETED).to_dict()
    # First call: empty pending phase, then one new result, then no more new.
    redis.xreadgroup.side_effect = [[], [("1-0", fields)], []]
    attempts = 0

    def handle_result(*args, **kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise _RetryableResultError("shared Redis down")

    mocker.patch(
        "orcest.orchestrator.loop._handle_result",
        side_effect=handle_result,
    )

    _consume_results(orchestrator_config, redis, logging.getLogger("test"))

    redis.xack.assert_not_called()
    # Next call: the same entry is returned from the pending phase and now
    # succeeds, so it crosses the ACK boundary.
    redis.xreadgroup.side_effect = [[("1-0", fields)], [], []]
    _consume_results(orchestrator_config, redis, logging.getLogger("test"))
    redis.xack.assert_called_once_with(RESULTS_STREAM, RESULTS_GROUP, "1-0")


def test_retryable_pending_results_do_not_starve_entries_beyond_batch(
    orchestrator_config,
    mocker,
):
    """A full batch of poison results cannot hide later PEL entries."""
    from unittest.mock import MagicMock

    redis = MagicMock()
    poison_entries = [
        (
            f"{index}-0",
            _make_task_result(
                status=ResultStatus.COMPLETED,
                task_id=f"poison-{index}",
            ).to_dict(),
        )
        for index in range(1, 13)
    ]
    healthy_entries = [
        (
            f"{index}-0",
            _make_task_result(
                status=ResultStatus.COMPLETED,
                task_id=f"healthy-{index}",
            ).to_dict(),
        )
        for index in range(13, 15)
    ]
    redis.xreadgroup.side_effect = [
        poison_entries[:10],
        poison_entries[10:] + healthy_entries,
        [],
        [],
    ]

    handled: list[str] = []

    def handle_result(project, labels, redis_client, result, logger, **kwargs):
        handled.append(result.task_id)
        if result.task_id.startswith("poison-"):
            raise _RetryableResultError("permanent GitHub failure")

    mocker.patch(
        "orcest.orchestrator.loop._handle_result",
        side_effect=handle_result,
    )

    _consume_results(orchestrator_config, redis, logging.getLogger("test"))

    assert handled == [
        *(f"poison-{index}" for index in range(1, 13)),
        "healthy-13",
        "healthy-14",
    ]
    redis.xack.assert_any_call(RESULTS_STREAM, RESULTS_GROUP, "13-0")
    redis.xack.assert_any_call(RESULTS_STREAM, RESULTS_GROUP, "14-0")
    assert redis.xack.call_count == 2
    pending_calls = [
        call for call in redis.xreadgroup.call_args_list if call.kwargs.get("pending") is True
    ]
    assert [call.kwargs["pending_start_id"] for call in pending_calls] == [
        "0",
        "10-0",
        "14-0",
    ]


def test_permanent_pending_failure_retries_without_blocking_later_work(
    orchestrator_config,
    mocker,
):
    """Each poll retries poison PEL entries while healthy old and new work progresses."""
    from unittest.mock import MagicMock

    redis = MagicMock()
    poison = (
        "1-0",
        _make_task_result(
            status=ResultStatus.COMPLETED,
            task_id="permanent-poison",
        ).to_dict(),
    )
    following = (
        "2-0",
        _make_task_result(
            status=ResultStatus.COMPLETED,
            task_id="following-pending",
        ).to_dict(),
    )
    new_entry = (
        "3-0",
        _make_task_result(
            status=ResultStatus.COMPLETED,
            task_id="following-new",
        ).to_dict(),
    )
    attempts: dict[str, int] = {}

    def handle_result(project, labels, redis_client, result, logger, **kwargs):
        attempts[result.task_id] = attempts.get(result.task_id, 0) + 1
        if result.task_id == "permanent-poison":
            raise _RetryableResultError("still unavailable")

    mocker.patch(
        "orcest.orchestrator.loop._handle_result",
        side_effect=handle_result,
    )

    redis.xreadgroup.side_effect = [[poison, following], [], []]
    _consume_results(orchestrator_config, redis, logging.getLogger("test"))

    redis.xreadgroup.side_effect = [[poison], [], [new_entry], []]
    _consume_results(orchestrator_config, redis, logging.getLogger("test"))

    assert attempts == {
        "permanent-poison": 2,
        "following-pending": 1,
        "following-new": 1,
    }
    acked_ids = [call.args[2] for call in redis.xack.call_args_list]
    assert acked_ids == ["2-0", "3-0"]


def test_rejected_grok_credential_update_increments_provider_health_counter(
    fake_redis_client,
    mocker,
):
    import logging

    from orcest.orchestrator.provider_pool import ProviderPool
    from orcest.shared.config import LabelConfig, ProjectConfig
    from orcest.shared.providers import ProviderEntry

    entry = ProviderEntry(provider="grok", credential="config-blob")
    pool = ProviderPool([entry])
    task_id = "task-rejected-refresh"
    fake_redis_client.hset(_TASK_PROVIDER_ACCOUNTS_KEY, task_id, entry.account_key())

    result = _make_task_result(
        status=ResultStatus.COMPLETED,
        task_id=task_id,
        pr_number=79,
    )
    result.credential_update = '{"access_token":"new-access","expires_at":123}'
    mocker.patch(
        "orcest.orchestrator.loop.gh.get_pr",
        return_value={"headRefOid": result.snapshot_head_sha, "statusCheckRollup": []},
    )

    _handle_result(
        ProjectConfig(
            repo="owner/testrepo",
            token="fake-token",
            claude_tokens=[],
            key_prefix="test",
            providers=[entry],
        ),
        LabelConfig(),
        fake_redis_client,
        result,
        logging.getLogger("test"),
        token_pool=pool,
    )

    counter_key = "providers:grok:credential_refresh_failures"
    assert pool.effective_credential(entry) == "config-blob"
    assert fake_redis_client.get(counter_key) == "1"
    assert fake_redis_client.ttl(counter_key) > 0
    assert fake_redis_client.hget(_TASK_PROVIDER_ACCOUNTS_KEY, task_id) is None


def test_rejected_codex_credential_update_increments_provider_health_counter(
    fake_redis_client,
    mocker,
):
    """Codex blobs must be judged by the same refresh-token rule as Grok.

    Regression: the orchestrator kept a private grok-only copy of the
    usability predicate, so a Codex account that lost its refresh token was
    judged "usable", the counter never incremented, and the silent failure
    the counter exists to surface produced no signal at all.
    """
    import logging

    from orcest.orchestrator.provider_pool import ProviderPool
    from orcest.shared.config import LabelConfig, ProjectConfig
    from orcest.shared.providers import ProviderEntry

    entry = ProviderEntry(provider="codex", credential="config-blob")
    pool = ProviderPool([entry])
    task_id = "task-rejected-codex-refresh"
    fake_redis_client.hset(_TASK_PROVIDER_ACCOUNTS_KEY, task_id, entry.account_key())

    result = _make_task_result(
        status=ResultStatus.COMPLETED,
        task_id=task_id,
        pr_number=81,
    )
    # Rotated blob that lost its refresh token -- unusable for the next task.
    result.credential_update = '{"access_token":"new-access","expires_at":123}'
    mocker.patch(
        "orcest.orchestrator.loop.gh.get_pr",
        return_value={"headRefOid": result.snapshot_head_sha, "statusCheckRollup": []},
    )

    _handle_result(
        ProjectConfig(
            repo="owner/testrepo",
            token="fake-token",
            claude_tokens=[],
            key_prefix="test",
            providers=[entry],
        ),
        LabelConfig(),
        fake_redis_client,
        result,
        logging.getLogger("test"),
        token_pool=pool,
    )

    counter_key = "providers:codex:credential_refresh_failures"
    assert pool.effective_credential(entry) == "config-blob"
    assert fake_redis_client.get(counter_key) == "1"
    assert fake_redis_client.ttl(counter_key) > 0


def test_legacy_credential_update_is_minted_in_microseconds(fake_redis_client, mocker):
    """A legacy result carries no minted_at, so the orchestrator synthesizes one.

    Regression: it synthesized `time.time()` (seconds, ~1.7e9) while workers
    mint from `next_monotonic_version` (microseconds, ~1.7e15). Both land in
    one comparison domain, so once any new-worker value was stored, every
    legacy rotation compared as older and was discarded forever -- pinning the
    account to a blob whose refresh token had already been consumed.
    """
    import logging

    from orcest.orchestrator.provider_pool import ProviderPool
    from orcest.shared.config import LabelConfig, ProjectConfig
    from orcest.shared.providers import ProviderEntry

    entry = ProviderEntry(provider="grok", credential="config-blob")
    pool = ProviderPool([entry])
    task_id = "task-legacy-mint"
    fake_redis_client.hset(_TASK_PROVIDER_ACCOUNTS_KEY, task_id, entry.account_key())

    # A new-style worker already stored a rotation stamped by the Redis clock
    # (microseconds). Dated in the past so a value minted "now" must beat it.
    newer_style_minted_at = 1_700_000_000_000_000.0
    pool.seed_credential_override(
        entry.account_key(), "blob-from-new-worker", minted_at=newer_style_minted_at
    )

    rotated = '{"access_token":"legacy","refresh_token":"legacy-refresh"}'
    result = _make_task_result(
        status=ResultStatus.COMPLETED,
        task_id=task_id,
        pr_number=82,
    )
    result.credential_update = rotated
    result.credential_update_minted_at = 0.0  # legacy worker: field absent
    mocker.patch(
        "orcest.orchestrator.loop.gh.get_pr",
        return_value={"headRefOid": result.snapshot_head_sha, "statusCheckRollup": []},
    )

    _handle_result(
        ProjectConfig(
            repo="owner/testrepo",
            token="fake-token",
            claude_tokens=[],
            key_prefix="test",
            providers=[entry],
        ),
        LabelConfig(),
        fake_redis_client,
        result,
        logging.getLogger("test"),
        token_pool=pool,
    )

    # The legacy rotation happened later in real time, so it must win.
    assert pool.effective_credential(entry) == rotated


def test_usage_exhausted_uses_persisted_task_account_after_restart(fake_redis_client, gh_mock):
    import logging

    from orcest.orchestrator.provider_pool import ProviderPool
    from orcest.shared.config import LabelConfig, ProjectConfig
    from orcest.shared.providers import ProviderEntry

    entry = ProviderEntry(provider="grok", credential="config-blob")
    pool = ProviderPool([entry])
    task_id = "task-usage-after-restart"
    fake_redis_client.hset(_TASK_PROVIDER_ACCOUNTS_KEY, task_id, entry.account_key())

    result = _make_task_result(
        status=ResultStatus.USAGE_EXHAUSTED,
        task_id=task_id,
        pr_number=78,
        rate_limit_resets_at=int(time.time()) + 600,
    )

    _handle_result(
        ProjectConfig(
            repo="owner/testrepo",
            token="fake-token",
            claude_tokens=[],
            key_prefix="test",
            providers=[entry],
        ),
        LabelConfig(),
        fake_redis_client,
        result,
        logging.getLogger("test"),
        token_pool=pool,
    )

    assert pool.available_count == 0
    assert pool.next_entry() is None
    assert fake_redis_client.hget(_TASK_PROVIDER_ACCOUNTS_KEY, task_id) is None


# ---------------------------------------------------------------------------
# M4-logic: no-credential project must not publish credential-less tasks
# ---------------------------------------------------------------------------


def test_poll_cycle_no_credentials_does_not_publish_credentialless_task(
    mocker,
    fake_redis_client,
    gh_mock,
    caplog,
):
    """A project with empty providers AND empty claude_tokens must NOT publish a
    task with credential='' (which fails on every worker). It should skip and
    log a clear operator error instead."""
    from orcest.shared.config import LabelConfig

    # Project with no credentials at all and no pool entry.
    config = OrchestratorConfig(
        labels=LabelConfig(),
        projects=[
            ProjectConfig(
                repo="acme/uncredentialed",
                token="gh-token",
                claude_tokens=[],  # no claude token
                providers=[],  # no providers
                key_prefix="uncred",
            ),
        ],
    )

    pr_state = _make_pr_state(number=10, action=PRAction.ENQUEUE_FIX)
    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        return_value=[pr_state],
    )
    mock_publish_fix = mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    mocker.patch("orcest.orchestrator.loop.publish_rebase_task")
    mocker.patch("orcest.orchestrator.loop.publish_issue_task")

    project_redis = fake_redis_client.__class__.from_client(
        fake_redis_client.client, key_prefix="uncred"
    )
    project_redis.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    logger = logging.getLogger("test")
    with caplog.at_level(logging.ERROR):
        # token_pools={} -> no pool for this project -> token_pool is None path.
        _poll_cycle(config, fake_redis_client, fake_redis_client, {}, logger, 3600)

    # No credential-less task may be published.
    mock_publish_fix.assert_not_called()

    # A clear operator error naming the project must be logged.
    error_msgs = [r.message for r in caplog.records if r.levelno == logging.ERROR]
    assert any(
        "acme/uncredentialed" in m and "no" in m.lower() and "credential" in m.lower()
        for m in error_msgs
    ), f"expected a no-credential operator error, got: {error_msgs}"


def test_poll_cycle_configured_providers_without_pool_do_not_fallback_to_empty_claude(
    mocker,
    fake_redis_client,
    gh_mock,
    caplog,
):
    """If provider pool construction rejected every configured provider, the
    project must skip publishing instead of falling back to credential=''.
    """
    from orcest.shared.config import LabelConfig
    from orcest.shared.providers import ProviderEntry

    config = OrchestratorConfig(
        labels=LabelConfig(),
        projects=[
            ProjectConfig(
                repo="acme/bad-grok",
                token="gh-token",
                claude_tokens=[],
                providers=[ProviderEntry("grok", '{"access_token":"access-only"}')],
                key_prefix="badgrok",
            ),
        ],
    )

    pr_state = _make_pr_state(number=10, action=PRAction.ENQUEUE_FIX)
    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_prs",
        return_value=[pr_state],
    )
    mock_publish_fix = mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    mocker.patch("orcest.orchestrator.loop.publish_rebase_task")
    mocker.patch("orcest.orchestrator.loop.publish_issue_task")

    logger = logging.getLogger("test")
    with caplog.at_level(logging.ERROR):
        _poll_cycle(config, fake_redis_client, fake_redis_client, {}, logger, 3600)

    mock_publish_fix.assert_not_called()
    error_msgs = [r.message for r in caplog.records if r.levelno == logging.ERROR]
    assert any("no usable provider pool" in m for m in error_msgs)


# ---------------------------------------------------------------------------
# M5-logic: shared issue stream must not starve other projects' discovery
# ---------------------------------------------------------------------------


def test_busy_project_does_not_permanently_starve_other_project_issue_discovery(
    mocker,
    fake_redis_client,
    gh_mock,
):
    """With a backlog on the SHARED issue stream, issue discovery must still
    rotate to every project across cycles -- one busy project cannot gate all
    others indefinitely.

    Fails against current code, where every project sees unclaimed_issue_tasks>0
    on the shared stream and defers discovery, so project B is never polled for
    issues.
    """
    from orcest.shared.config import LabelConfig

    config = OrchestratorConfig(
        labels=LabelConfig(),
        projects=[
            ProjectConfig(
                repo="acme/busy",
                token="t-busy",
                claude_tokens=["c-busy"],
                key_prefix="busy",
            ),
            ProjectConfig(
                repo="acme/quiet",
                token="t-quiet",
                claude_tokens=["c-quiet"],
                key_prefix="quiet",
            ),
        ],
    )

    mocker.patch("orcest.orchestrator.loop.discover_actionable_prs", return_value=[])
    mocker.patch("orcest.orchestrator.loop.publish_fix_task")
    mocker.patch("orcest.orchestrator.loop.publish_followup_task")
    mocker.patch("orcest.orchestrator.loop.publish_issue_task")

    # Record which repos got issue discovery (override the autouse mock so we can spy).
    discovered_repos: list[str] = []

    def fake_discover_issues(*, repo, **kwargs):
        discovered_repos.append(repo)
        return []

    mocker.patch(
        "orcest.orchestrator.loop.discover_actionable_issues",
        side_effect=fake_discover_issues,
    )

    # Pre-load a backlog on the SHARED issue stream so the gate trips.
    fake_redis_client.ensure_consumer_group("tasks:issue:claude", "workers")
    fake_redis_client.xadd("tasks:issue:claude", {"a": "1"})
    fake_redis_client.xadd("tasks:issue:claude", {"a": "2"})
    assert fake_redis_client.stream_unread_count("tasks:issue:claude", "workers") > 0

    for project in config.projects:
        pr = fake_redis_client.__class__.from_client(
            fake_redis_client.client, key_prefix=project.key_prefix
        )
        pr.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    logger = logging.getLogger("test")
    # Run as many cycles as there are projects: with rotation, every project
    # gets at least one issue-discovery turn even though the shared queue stays
    # non-empty the whole time.
    for _ in range(len(config.projects)):
        _poll_cycle(config, fake_redis_client, fake_redis_client, {}, logger, 3600)

    assert "acme/busy" in discovered_repos
    assert "acme/quiet" in discovered_repos, (
        "quiet project was starved of issue discovery by the shared-stream gate"
    )


def test_distributed_issue_priority_is_shared_across_project_processes(
    mocker,
    fake_redis_client,
):
    from orcest.orchestrator.loop import _issue_discovery_priority

    busy = ProjectConfig(repo="acme/busy", token="t", claude_tokens=[], key_prefix="busy")
    quiet = ProjectConfig(repo="acme/quiet", token="t", claude_tokens=[], key_prefix="quiet")
    logger = logging.getLogger("test")
    mocker.patch("orcest.orchestrator.loop.time.time", return_value=120.0)

    _issue_discovery_priority(fake_redis_client, [busy], 60, logger)
    _issue_discovery_priority(fake_redis_client, [quiet], 60, logger)
    owner_from_busy = _issue_discovery_priority(fake_redis_client, [busy], 60, logger)
    owner_from_quiet = _issue_discovery_priority(fake_redis_client, [quiet], 60, logger)

    assert owner_from_busy == owner_from_quiet
    assert owner_from_busy in {"busy", "quiet"}


def test_distributed_issue_priority_advances_after_lease_expiry(
    mocker,
    fake_redis_client,
):
    from orcest.orchestrator.loop import (
        _ISSUE_DISCOVERY_TURN_KEY,
        _issue_discovery_priority,
    )

    busy = ProjectConfig(repo="acme/busy", token="t", claude_tokens=[], key_prefix="busy")
    quiet = ProjectConfig(repo="acme/quiet", token="t", claude_tokens=[], key_prefix="quiet")
    logger = logging.getLogger("test")
    mocker.patch("orcest.orchestrator.loop.time.time", return_value=120.0)

    _issue_discovery_priority(fake_redis_client, [busy], 60, logger)
    _issue_discovery_priority(fake_redis_client, [quiet], 60, logger)
    first = _issue_discovery_priority(fake_redis_client, [busy], 60, logger)
    fake_redis_client.delete(_ISSUE_DISCOVERY_TURN_KEY)
    second_from_busy = _issue_discovery_priority(fake_redis_client, [busy], 60, logger)
    second_from_quiet = _issue_discovery_priority(fake_redis_client, [quiet], 60, logger)

    assert first == "busy"
    assert second_from_busy == second_from_quiet == "quiet"


def test_fast_caller_does_not_prune_slow_projects_heartbeat(
    mocker,
    fake_redis_client,
):
    """Staleness is judged by each heartbeat's OWN declared interval.

    A 5s-interval caller must not prune a sibling orchestrator's heartbeat
    that declares a 60s interval and is only 30s old; doing so would exclude
    the slow project from the issue-discovery rotation entirely.
    """
    from orcest.orchestrator.loop import (
        _ISSUE_DISCOVERY_PROJECTS_KEY,
        _issue_discovery_priority,
    )

    fast = ProjectConfig(repo="acme/fast", token="t", claude_tokens=[], key_prefix="fast")
    logger = logging.getLogger("test")
    mocker.patch("orcest.orchestrator.loop.time.time", return_value=120.0)

    # A slow (60s-interval) sibling wrote its heartbeat 30 seconds ago.
    fake_redis_client.hset(_ISSUE_DISCOVERY_PROJECTS_KEY, "slow", "90.0|60")

    _issue_discovery_priority(fake_redis_client, [fast], 5, logger)

    heartbeats = fake_redis_client.hgetall(_ISSUE_DISCOVERY_PROJECTS_KEY)
    assert "slow" in heartbeats, "fast caller pruned a live slow-interval heartbeat"
    assert "fast" in heartbeats


def test_fast_caller_spares_legacy_heartbeat_during_rolling_upgrade(
    mocker,
    fake_redis_client,
):
    """Timestamp-only heartbeats (old writers) get a generous staleness floor."""
    from orcest.orchestrator.loop import (
        _ISSUE_DISCOVERY_PROJECTS_KEY,
        _issue_discovery_priority,
    )

    fast = ProjectConfig(repo="acme/fast", token="t", claude_tokens=[], key_prefix="fast")
    logger = logging.getLogger("test")
    mocker.patch("orcest.orchestrator.loop.time.time", return_value=120.0)

    # Pre-upgrade writer format: bare timestamp, 30 seconds old.
    fake_redis_client.hset(_ISSUE_DISCOVERY_PROJECTS_KEY, "legacy", "90.0")
    # Genuinely dead entries are still pruned.
    fake_redis_client.hset(_ISSUE_DISCOVERY_PROJECTS_KEY, "dead", "-300.0|5")

    _issue_discovery_priority(fake_redis_client, [fast], 5, logger)

    heartbeats = fake_redis_client.hgetall(_ISSUE_DISCOVERY_PROJECTS_KEY)
    assert "legacy" in heartbeats, "fast caller pruned a legacy-format heartbeat"
    assert "dead" not in heartbeats


# ---------------------------------------------------------------------------
# Bootstrap degradation under Redis noeviction OOM (#611)
# ---------------------------------------------------------------------------


def _oom_error():
    import redis as redis_py

    oom_cls = getattr(redis_py.exceptions, "OutOfMemoryError", redis_py.ResponseError)
    return oom_cls("OOM command not allowed when used memory > maxmemory")


def test_ensure_consumer_group_or_defer_existing_group_no_write(mocker):
    """An existing group is confirmed without issuing a write, even under OOM."""
    from unittest.mock import MagicMock

    from orcest.orchestrator.loop import _ensure_consumer_group_or_defer

    redis_client = MagicMock()
    redis_client.ensure_consumer_group.return_value = None  # read-first, no OOM raised

    result = _ensure_consumer_group_or_defer(
        redis_client, "tasks:claude", "orcest-workers", logging.getLogger("test")
    )

    assert result is True
    redis_client.ensure_consumer_group.assert_called_once_with("tasks:claude", "orcest-workers")


def test_ensure_consumer_group_or_defer_missing_group_oom_defers():
    """A missing group's creation OOM defers instead of raising."""
    from unittest.mock import MagicMock

    from orcest.orchestrator.loop import _ensure_consumer_group_or_defer

    redis_client = MagicMock()
    redis_client.ensure_consumer_group.side_effect = _oom_error()

    result = _ensure_consumer_group_or_defer(
        redis_client, "tasks:claude", "orcest-workers", logging.getLogger("test")
    )

    assert result is False


def test_ensure_consumer_group_or_defer_non_oom_raises():
    """Wrong-type/ACL/protocol failures remain fatal, not deferred."""
    from unittest.mock import MagicMock

    import redis as redis_py

    from orcest.orchestrator.loop import _ensure_consumer_group_or_defer

    redis_client = MagicMock()
    redis_client.ensure_consumer_group.side_effect = redis_py.ResponseError(
        "WRONGTYPE Operation against a key holding the wrong kind of value"
    )

    with pytest.raises(redis_py.ResponseError, match="WRONGTYPE"):
        _ensure_consumer_group_or_defer(
            redis_client, "tasks:claude", "orcest-workers", logging.getLogger("test")
        )


def test_retry_pending_consumer_groups_recovers_after_oom():
    """A deferred group is retried and drops out of the pending list on success."""
    from unittest.mock import MagicMock

    from orcest.orchestrator.loop import _retry_pending_consumer_groups

    redis_client = MagicMock()
    redis_client.ensure_consumer_group.side_effect = _oom_error()
    pending = [(redis_client, "tasks:claude", "orcest-workers")]

    still_pending = _retry_pending_consumer_groups(pending, logging.getLogger("test"))
    assert still_pending == pending

    redis_client.ensure_consumer_group.side_effect = None
    redis_client.ensure_consumer_group.return_value = None
    still_pending = _retry_pending_consumer_groups(still_pending, logging.getLogger("test"))
    assert still_pending == []


def test_retry_pending_provider_account_backfill_defers_on_oom(mocker):
    """Classified Redis OOM during backfill defers rather than raising."""
    from unittest.mock import MagicMock

    from orcest.orchestrator.loop import _retry_pending_provider_account_backfill

    mocker.patch(
        "orcest.orchestrator.loop._backfill_retained_task_provider_accounts",
        side_effect=_oom_error(),
    )

    completed = _retry_pending_provider_account_backfill(
        MagicMock(), [], [], {}, 300, logging.getLogger("test")
    )

    assert completed is False


def test_retry_pending_provider_account_backfill_succeeds(mocker):
    """A completed backfill (including a no-op pass) reports done."""
    from unittest.mock import MagicMock

    from orcest.orchestrator.loop import _retry_pending_provider_account_backfill

    mocker.patch(
        "orcest.orchestrator.loop._backfill_retained_task_provider_accounts",
        return_value=0,
    )

    completed = _retry_pending_provider_account_backfill(
        MagicMock(), [], [], {}, 300, logging.getLogger("test")
    )

    assert completed is True


def test_retry_pending_provider_account_backfill_non_oom_raises(mocker):
    """A non-OOM backfill failure propagates instead of being deferred."""
    from unittest.mock import MagicMock

    from orcest.orchestrator.loop import _retry_pending_provider_account_backfill

    mocker.patch(
        "orcest.orchestrator.loop._backfill_retained_task_provider_accounts",
        side_effect=ValueError("malformed retained task"),
    )

    with pytest.raises(ValueError, match="malformed retained task"):
        _retry_pending_provider_account_backfill(
            MagicMock(), [], [], {}, 300, logging.getLogger("test")
        )


def test_backfill_provider_account_oom_propagates_for_deferral(fake_redis_client, mocker):
    """A Redis OOM while persisting a backfilled mapping propagates raw.

    This is what lets ``_retry_pending_provider_account_backfill`` classify it
    and defer the whole pass instead of the backfill loop silently swallowing
    the failure (as the live task-registration path does) and only ever
    surfacing a generic "no mapping written" symptom.
    """
    from orcest.orchestrator.loop import _backfill_retained_task_provider_accounts
    from orcest.orchestrator.provider_pool import ProviderPool
    from orcest.shared.models import Task, TaskType
    from orcest.shared.providers import ProviderEntry

    entry = ProviderEntry(provider="grok", credential="config-blob")
    project = ProjectConfig(
        repo="owner/testrepo",
        token="fake-token",
        claude_tokens=[],
        key_prefix="test",
        providers=[entry],
    )
    task = Task.create(
        task_type=TaskType.FIX_PR,
        repo=project.repo,
        token=project.token,
        resource_type="pr",
        resource_id=91,
        prompt="fix it",
        key_prefix=project.key_prefix,
        provider="grok",
        credential="config-blob",
        task_id="oom-during-backfill",
    )
    task_redis = RedisClient.from_client(fake_redis_client.client, key_prefix="shared")
    legacy_fields = task.to_dict()
    legacy_fields.pop("provider_account")
    task_redis.xadd("tasks:grok", legacy_fields)

    mocker.patch.object(fake_redis_client._client, "set", side_effect=_oom_error())

    with pytest.raises(type(_oom_error())):
        _backfill_retained_task_provider_accounts(
            task_redis,
            ["tasks:grok"],
            [(project, fake_redis_client)],
            {project.key_prefix: ProviderPool([entry])},
            ttl_seconds=300,
            logger=logging.getLogger("test"),
        )
