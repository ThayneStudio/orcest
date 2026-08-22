"""Unit tests for the worker main loop and task execution."""

from __future__ import annotations

import json
import logging
import signal
import threading
from pathlib import Path
from unittest.mock import MagicMock, call as mock_call, patch

import pytest

from orcest.shared.config import RedisConfig, RunnerConfig, WorkerConfig
from orcest.shared.credential_handoff import CREDENTIAL_CHECKPOINT_TTL_SECONDS
from orcest.shared.models import (
    REDACTED_FIELDS,
    TRANSIENT_SUMMARY_PREFIX,
    ResultStatus,
    Task,
    TaskResult,
    TaskType,
)
from orcest.shared.output_streams import (
    OUTPUT_BUDGET_PROJECTS as _OUTPUT_BUDGET_PROJECTS,
    OUTPUT_BUDGET_WORKERS as _OUTPUT_BUDGET_WORKERS,
    OUTPUT_STREAM_BUDGET_BYTES as _OUTPUT_STREAM_BUDGET_BYTES,
    OUTPUT_STREAM_MAX_ENTRY_BYTES as _OUTPUT_STREAM_MAX_ENTRY_BYTES,
    OUTPUT_STREAM_MAXLEN as _OUTPUT_STREAM_MAXLEN,
    OUTPUT_STREAM_TTL_SECONDS as _OUTPUT_STREAM_TTL_SECONDS,
    iter_capped_output_fields,
    output_streams_worst_case_bytes,
)
from orcest.worker.loop import (
    _CREDENTIAL_DIAGNOSTIC_HANDOFF_PREFIX,
    _HANDOFF_FINGERPRINT_FIELD,
    _HANDOFF_MARKER_TTL_SECONDS,
    _RESULT_PUBLISH_BACKOFF,
    _RESULT_PUBLISH_RETRIES,
    _STREAM_MAXLEN,
    CONSUMER_GROUP,
    DEAD_LETTER_STREAM,
    HEARTBEAT_INTERVAL,
    LOCK_TTL,
    MAX_DELIVERY_COUNT,
    RESULTS_STREAM,
    CoordinationIdentity,
    ResultHandoff,
    ResultPublishOutcome,
    _check_gh_credentials,
    _cleanup_coordination_once,
    _credential_checkpoint_key,
    _dead_letter_task,
    _drain_pending_tasks_raw,
    _execute_task,
    _handoff_marker_key,
    _handoff_result_until_terminal,
    _make_abort_event,
    _publish_result_with_retry,
    _publish_task_output,
    _runner_for_task,
    _signal_ephemeral_done,
    _stream_handoff_state,
    _task_result,
    _wait_for_redis,
    run_worker,
)
from orcest.worker.runner import PROVIDER_REGISTRY, ProviderRecipe, RunnerResult
from orcest.worker.workspace import WorkspaceError

# ---------------------------------------------------------------------------
# Helpers / fixtures local to this module
# ---------------------------------------------------------------------------


@pytest.fixture
def local_worker_config(tmp_path):
    """WorkerConfig with short timeouts for fast tests."""
    return WorkerConfig(
        redis=RedisConfig(host="localhost", port=6379, db=0),
        worker_id="test-worker-1",
        workspace_dir=str(tmp_path / "workspaces"),
        runner=RunnerConfig(timeout=10, max_retries=1, retry_backoff=0),
    )


@pytest.fixture
def sample_task():
    """A minimal Task for testing."""
    return Task.create(
        task_type=TaskType.FIX_PR,
        repo="owner/repo",
        token="test-token-loop",
        resource_type="pr",
        resource_id=42,
        prompt="Fix the failing CI checks",
        branch="fix-ci",
        snapshot_head_sha="abc123",
    )


@pytest.fixture
def mock_workspace():
    """A mock Workspace whose setup() returns a tmp path without cloning."""
    ws = MagicMock()
    ws.setup.return_value = Path("/tmp/fake-workspace/repo")
    ws.current_head_sha.return_value = "abc123"
    ws.cleanup.return_value = None
    return ws


@pytest.fixture(autouse=True)
def _mock_worker_pr_snapshot(mocker):
    """Default PR snapshot lookup for tests that exercise normal PR execution."""
    return mocker.patch(
        "orcest.worker.loop.gh.get_pr",
        return_value={"headRefOid": "abc123", "statusCheckRollup": []},
    )


def _success_runner_result() -> RunnerResult:
    return RunnerResult(success=True, summary="All checks fixed")


def _failure_runner_result() -> RunnerResult:
    return RunnerResult(success=False, summary="Could not resolve merge conflict")


# ---------------------------------------------------------------------------
# Tests for _make_abort_event
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestMakeAbortEvent:
    """Tests for the _make_abort_event combined-event helper."""

    def test_fires_when_first_event_set(self):
        """Combined event fires when the first input event fires."""
        e1, e2 = threading.Event(), threading.Event()
        combined = _make_abort_event(e1, e2)
        assert not combined.is_set()
        e1.set()
        assert combined.wait(timeout=1), "combined should fire when e1 fires"

    def test_fires_when_second_event_set(self):
        """Combined event fires when the second input event fires."""
        e1, e2 = threading.Event(), threading.Event()
        combined = _make_abort_event(e1, e2)
        assert not combined.is_set()
        e2.set()
        assert combined.wait(timeout=1), "combined should fire when e2 fires"

    def test_already_set_short_circuits(self):
        """Combined event is immediately set when any input is already set."""
        e1, e2 = threading.Event(), threading.Event()
        e1.set()
        combined = _make_abort_event(e1, e2)
        assert combined.is_set()

    def test_shutdown_event_wakes_abort_sleep(self):
        """SIGTERM (shutdown_event) wakes the abort event used in retry-backoff sleeps.

        This is the regression test for issue #148: after PR #98 changed
        abort_event from shutdown_event to lock_lost, SIGTERM no longer
        interrupted retry-backoff sleeps.  _make_abort_event restores that.
        """
        shutdown_event = threading.Event()
        lock_lost = threading.Event()
        abort = _make_abort_event(lock_lost, shutdown_event)

        assert not abort.is_set()
        shutdown_event.set()
        assert abort.wait(timeout=1), "abort event must wake when shutdown_event fires"


@pytest.mark.unit
def test_wait_for_redis_aborts_during_backoff():
    """External shutdown interrupts the startup Redis retry budget."""
    redis = MagicMock()
    abort = threading.Event()

    def fail_and_stop():
        abort.set()
        return False

    redis.health_check.side_effect = fail_and_stop

    assert _wait_for_redis(redis, logging.getLogger("test.redis-wait"), abort) is False
    redis.health_check.assert_called_once()


# ---------------------------------------------------------------------------
# Tests for _execute_task (the single-iteration helper)
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestExecuteTask:
    """Tests for the _execute_task internal helper."""

    def test_worker_processes_task(self, local_worker_config, sample_task, mock_workspace):
        """_execute_task returns a COMPLETED TaskResult on runner success."""
        mock_runner = MagicMock()
        mock_runner.run.return_value = _success_runner_result()

        mock_redis = MagicMock()
        mock_redis.xadd_capped.return_value = "1-0"

        result = _execute_task(
            sample_task,
            local_worker_config,
            mock_runner,
            mock_workspace,
            mock_redis,
            logging.getLogger("test"),
        )

        assert isinstance(result, TaskResult)
        assert result.status == ResultStatus.COMPLETED
        assert result.task_id == sample_task.id
        assert result.worker_id == local_worker_config.worker_id
        assert result.summary == "All checks fixed"

        # Workspace lifecycle
        mock_workspace.setup.assert_called_once_with(
            sample_task.repo, sample_task.branch, sample_task.token
        )
        mock_workspace.cleanup.assert_called_once()

    def test_worker_handles_runner_failure(self, local_worker_config, sample_task, mock_workspace):
        """_execute_task returns a FAILED TaskResult when the runner fails."""
        mock_runner = MagicMock()
        mock_runner.run.return_value = _failure_runner_result()

        mock_redis = MagicMock()
        mock_redis.xadd_capped.return_value = "1-0"

        result = _execute_task(
            sample_task,
            local_worker_config,
            mock_runner,
            mock_workspace,
            mock_redis,
            logging.getLogger("test"),
        )

        assert result.status == ResultStatus.FAILED
        assert result.task_id == sample_task.id
        assert "merge conflict" in result.summary.lower()
        assert not result.summary.startswith("[transient] ")

    def test_pr_task_stale_when_head_sha_changed(self, local_worker_config, mock_workspace, mocker):
        task = Task.create(
            task_type=TaskType.FIX_CI,
            repo="owner/repo",
            token="test-token-loop",
            resource_type="pr",
            resource_id=42,
            prompt="Fix CI",
            branch="fix-ci",
            snapshot_head_sha="sha-old",
            decision_reason="ci_failure",
            snapshot_failed_checks=["tests"],
        )
        mocker.patch(
            "orcest.worker.loop.gh.get_pr",
            return_value={"headRefOid": "sha-new", "statusCheckRollup": []},
        )
        mock_runner = MagicMock()
        mock_redis = MagicMock()
        mock_redis.xadd_capped.return_value = "1-0"

        result = _execute_task(
            task,
            local_worker_config,
            mock_runner,
            mock_workspace,
            mock_redis,
            logging.getLogger("test"),
        )

        assert result.status == ResultStatus.STALE
        assert result.snapshot_head_sha == "sha-old"
        mock_runner.run.assert_not_called()
        mock_workspace.setup.assert_not_called()

    def test_snapshotless_pr_task_is_stale(self, local_worker_config, mock_workspace):
        task = Task.create(
            task_type=TaskType.FIX_CI,
            repo="owner/repo",
            token="test-token-loop",
            resource_type="pr",
            resource_id=42,
            prompt="Fix CI",
            branch="fix-ci",
        )
        mock_runner = MagicMock()
        mock_redis = MagicMock()
        mock_redis.xadd_capped.return_value = "1-0"

        result = _execute_task(
            task,
            local_worker_config,
            mock_runner,
            mock_workspace,
            mock_redis,
            logging.getLogger("test"),
        )

        assert result.status == ResultStatus.STALE
        mock_runner.run.assert_not_called()
        mock_workspace.setup.assert_not_called()

    def test_pr_ci_task_stale_when_failed_check_now_green(
        self, local_worker_config, mock_workspace, mocker
    ):
        task = Task.create(
            task_type=TaskType.FIX_CI,
            repo="owner/repo",
            token="test-token-loop",
            resource_type="pr",
            resource_id=42,
            prompt="Fix CI",
            branch="fix-ci",
            snapshot_head_sha="sha-same",
            decision_reason="ci_failure",
            snapshot_failed_checks=["tests"],
        )
        mocker.patch(
            "orcest.worker.loop.gh.get_pr",
            return_value={
                "headRefOid": "sha-same",
                "statusCheckRollup": [{"name": "tests", "conclusion": "SUCCESS"}],
            },
        )
        mock_runner = MagicMock()
        mock_redis = MagicMock()
        mock_redis.xadd_capped.return_value = "1-0"

        result = _execute_task(
            task,
            local_worker_config,
            mock_runner,
            mock_workspace,
            mock_redis,
            logging.getLogger("test"),
        )

        assert result.status == ResultStatus.STALE
        mock_runner.run.assert_not_called()

    def test_pr_ci_task_stale_when_same_check_name_has_different_run(
        self, local_worker_config, mock_workspace, mocker
    ):
        task = Task.create(
            task_type=TaskType.FIX_CI,
            repo="owner/repo",
            token="test-token-loop",
            resource_type="pr",
            resource_id=42,
            prompt="Fix CI",
            branch="fix-ci",
            snapshot_head_sha="sha-same",
            decision_reason="ci_failure",
            snapshot_failed_checks=["https://github.com/o/r/actions/runs/1"],
        )
        mocker.patch(
            "orcest.worker.loop.gh.get_pr",
            return_value={
                "headRefOid": "sha-same",
                "statusCheckRollup": [
                    {
                        "name": "tests",
                        "conclusion": "FAILURE",
                        "detailsUrl": "https://github.com/o/r/actions/runs/2",
                    }
                ],
            },
        )
        mock_runner = MagicMock()
        mock_redis = MagicMock()
        mock_redis.xadd_capped.return_value = "1-0"

        result = _execute_task(
            task,
            local_worker_config,
            mock_runner,
            mock_workspace,
            mock_redis,
            logging.getLogger("test"),
        )

        assert result.status == ResultStatus.STALE
        mock_runner.run.assert_not_called()

    def test_pr_ci_task_stale_when_same_context_has_different_target_url(
        self, local_worker_config, mock_workspace, mocker
    ):
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
        task = Task.create(
            task_type=TaskType.FIX_CI,
            repo="owner/repo",
            token="test-token-loop",
            resource_type="pr",
            resource_id=42,
            prompt="Fix CI",
            branch="fix-ci",
            snapshot_head_sha="sha-same",
            decision_reason="ci_failure",
            snapshot_failed_checks=[snapshot],
        )
        mocker.patch(
            "orcest.worker.loop.gh.get_pr",
            return_value={
                "headRefOid": "sha-same",
                "statusCheckRollup": [
                    {
                        "context": "build",
                        "state": "FAILURE",
                        "targetUrl": "https://ci.example/build/new",
                    }
                ],
            },
        )
        mock_runner = MagicMock()
        mock_redis = MagicMock()
        mock_redis.xadd_capped.return_value = "1-0"

        result = _execute_task(
            task,
            local_worker_config,
            mock_runner,
            mock_workspace,
            mock_redis,
            logging.getLogger("test"),
        )

        assert result.status == ResultStatus.STALE
        mock_runner.run.assert_not_called()

    def test_review_task_stale_when_thread_body_changes(
        self, local_worker_config, mock_workspace, mocker
    ):
        old_fingerprint = (
            '{"comments":[{"author":"alice","body":"old feedback",'
            '"created_at":"","id":"","updated_at":""}],'
            '"id":"thread-1","line":"10","path":"app.py"}'
        )
        task = Task.create(
            task_type=TaskType.FIX_PR,
            repo="owner/repo",
            token="test-token-loop",
            resource_type="pr",
            resource_id=42,
            prompt="Fix review",
            branch="fix-review",
            snapshot_head_sha="sha-same",
            decision_reason="changes_requested",
            snapshot_review_thread_ids=["thread-1"],
            snapshot_review_thread_fingerprints=[old_fingerprint],
        )
        mocker.patch(
            "orcest.worker.loop.gh.get_pr",
            return_value={"headRefOid": "sha-same"},
        )
        mocker.patch(
            "orcest.worker.loop.gh.get_unresolved_review_threads",
            return_value=[
                {
                    "id": "thread-1",
                    "path": "app.py",
                    "line": 10,
                    "comments": [{"author": "alice", "body": "new feedback"}],
                }
            ],
        )
        mock_runner = MagicMock()
        mock_redis = MagicMock()
        mock_redis.xadd_capped.return_value = "1-0"

        result = _execute_task(
            task,
            local_worker_config,
            mock_runner,
            mock_workspace,
            mock_redis,
            logging.getLogger("test"),
        )

        assert result.status == ResultStatus.STALE
        mock_runner.run.assert_not_called()

    def test_merge_conflict_rebase_task_stale_when_conflict_resolved(
        self, local_worker_config, mock_workspace, mocker
    ):
        task = Task.create(
            task_type=TaskType.REBASE_PR,
            repo="owner/repo",
            token="test-token-loop",
            resource_type="pr",
            resource_id=42,
            prompt="Rebase",
            branch="fix-conflict",
            snapshot_head_sha="sha-same",
            decision_reason="merge_conflict_rebase",
        )
        mocker.patch(
            "orcest.worker.loop.gh.get_pr",
            return_value={
                "headRefOid": "sha-same",
                "mergeable": "MERGEABLE",
                "mergeStateStatus": "CLEAN",
            },
        )
        mock_runner = MagicMock()
        mock_redis = MagicMock()
        mock_redis.xadd_capped.return_value = "1-0"

        result = _execute_task(
            task,
            local_worker_config,
            mock_runner,
            mock_workspace,
            mock_redis,
            logging.getLogger("test"),
        )

        assert result.status == ResultStatus.STALE
        mock_runner.run.assert_not_called()

    def test_pr_task_stale_when_cloned_head_differs(
        self, local_worker_config, mock_workspace, mocker
    ):
        task = Task.create(
            task_type=TaskType.FIX_CI,
            repo="owner/repo",
            token="test-token-loop",
            resource_type="pr",
            resource_id=42,
            prompt="Fix CI",
            branch="fix-ci",
            snapshot_head_sha="sha-expected",
            decision_reason="ci_failure",
            snapshot_failed_checks=["tests"],
        )
        mocker.patch(
            "orcest.worker.loop.gh.get_pr",
            return_value={
                "headRefOid": "sha-expected",
                "statusCheckRollup": [{"name": "tests", "conclusion": "FAILURE"}],
            },
        )
        mock_workspace.current_head_sha.return_value = "sha-other"
        mock_runner = MagicMock()
        mock_redis = MagicMock()
        mock_redis.xadd_capped.return_value = "1-0"

        result = _execute_task(
            task,
            local_worker_config,
            mock_runner,
            mock_workspace,
            mock_redis,
            logging.getLogger("test"),
        )

        assert result.status == ResultStatus.STALE
        mock_runner.run.assert_not_called()

    def test_worker_handles_usage_exhaustion(
        self, local_worker_config, sample_task, mock_workspace
    ):
        """_execute_task returns USAGE_EXHAUSTED when the runner reports limits."""
        mock_runner = MagicMock()
        mock_runner.run.return_value = RunnerResult(
            success=False, summary="limit reached", usage_exhausted=True
        )

        mock_redis = MagicMock()
        mock_redis.xadd_capped.return_value = "1-0"

        result = _execute_task(
            sample_task,
            local_worker_config,
            mock_runner,
            mock_workspace,
            mock_redis,
            logging.getLogger("test"),
        )

        assert result.status == ResultStatus.USAGE_EXHAUSTED

    def test_runner_timeout_produces_transient_result(
        self, local_worker_config, sample_task, mock_workspace
    ):
        """Runner timeout (success=False) produces a task result with [transient] prefix."""
        mock_runner = MagicMock()
        mock_runner.run.return_value = RunnerResult(
            success=False, summary="Timed out after 600s", transient=True
        )

        mock_redis = MagicMock()
        mock_redis.xadd_capped.return_value = "1-0"

        result = _execute_task(
            sample_task,
            local_worker_config,
            mock_runner,
            mock_workspace,
            mock_redis,
            logging.getLogger("test"),
        )

        assert result.status == ResultStatus.FAILED
        assert result.summary.startswith("[transient] ")
        assert "Timed out" in result.summary

    def test_runner_crash_produces_transient_result(
        self, local_worker_config, sample_task, mock_workspace
    ):
        """Runner crash (success=False, all retries exhausted) produces transient result."""
        mock_runner = MagicMock()
        mock_runner.run.return_value = RunnerResult(
            success=False, summary="Failed after 3 attempts", transient=True
        )

        mock_redis = MagicMock()
        mock_redis.xadd_capped.return_value = "1-0"

        result = _execute_task(
            sample_task,
            local_worker_config,
            mock_runner,
            mock_workspace,
            mock_redis,
            logging.getLogger("test"),
        )

        assert result.status == ResultStatus.FAILED
        assert result.summary.startswith("[transient] ")
        assert "Failed after 3 attempts" in result.summary

    def test_usage_exhausted_not_marked_transient(
        self, local_worker_config, sample_task, mock_workspace
    ):
        """usage_exhausted=True is NOT marked transient (already separate status)."""
        mock_runner = MagicMock()
        mock_runner.run.return_value = RunnerResult(
            success=False, summary="limit reached", usage_exhausted=True
        )

        mock_redis = MagicMock()
        mock_redis.xadd_capped.return_value = "1-0"

        result = _execute_task(
            sample_task,
            local_worker_config,
            mock_runner,
            mock_workspace,
            mock_redis,
            logging.getLogger("test"),
        )

        assert result.status == ResultStatus.USAGE_EXHAUSTED
        assert not result.summary.startswith("[transient] ")
        assert result.summary == "limit reached"

    def test_workspace_exception_returns_failed(
        self, local_worker_config, sample_task, mock_workspace
    ):
        """If workspace.setup() raises, the result is FAILED and cleanup runs."""
        mock_workspace.setup.side_effect = RuntimeError("clone failed")
        mock_runner = MagicMock()

        mock_redis = MagicMock()
        mock_redis.xadd_capped.return_value = "1-0"

        result = _execute_task(
            sample_task,
            local_worker_config,
            mock_runner,
            mock_workspace,
            mock_redis,
            logging.getLogger("test"),
        )

        assert result.status == ResultStatus.FAILED
        assert "clone failed" in result.summary
        mock_workspace.cleanup.assert_called_once()

    def test_workspace_error_produces_transient_summary(
        self, local_worker_config, sample_task, mock_workspace
    ):
        """WorkspaceError (clone timeout, network) produces [transient] summary prefix."""
        mock_workspace.setup.side_effect = WorkspaceError(
            "git clone timed out after 300s for owner/repo", transient=True
        )
        mock_runner = MagicMock()
        mock_redis = MagicMock()
        mock_redis.xadd_capped.return_value = "1-0"

        result = _execute_task(
            sample_task,
            local_worker_config,
            mock_runner,
            mock_workspace,
            mock_redis,
            logging.getLogger("test"),
        )

        assert result.status == ResultStatus.FAILED
        assert result.summary.startswith("[transient]")
        assert "timed out" in result.summary

    def test_workspace_error_without_timeout_is_not_transient(
        self, local_worker_config, sample_task, mock_workspace
    ):
        """WorkspaceError for auth/credential failures is NOT treated as transient."""
        mock_workspace.setup.side_effect = WorkspaceError(
            "git clone failed: remote: Repository not found (exit code 128)"
        )
        mock_runner = MagicMock()
        mock_redis = MagicMock()
        mock_redis.xadd_capped.return_value = "1-0"

        result = _execute_task(
            sample_task,
            local_worker_config,
            mock_runner,
            mock_workspace,
            mock_redis,
            logging.getLogger("test"),
        )

        assert result.status == ResultStatus.FAILED
        assert not result.summary.startswith("[transient]")
        assert "exit code 128" in result.summary

    def test_non_workspace_error_produces_normal_summary(
        self, local_worker_config, sample_task, mock_workspace
    ):
        """Non-WorkspaceError exceptions produce summaries without [transient] prefix."""
        mock_workspace.setup.side_effect = RuntimeError("unexpected error")
        mock_runner = MagicMock()
        mock_redis = MagicMock()
        mock_redis.xadd_capped.return_value = "1-0"

        result = _execute_task(
            sample_task,
            local_worker_config,
            mock_runner,
            mock_workspace,
            mock_redis,
            logging.getLogger("test"),
        )

        assert result.status == ResultStatus.FAILED
        assert not result.summary.startswith("[transient]")
        assert "unexpected error" in result.summary

    def test_output_callback_publishes_to_redis(
        self, local_worker_config, sample_task, mock_workspace
    ):
        """on_output callback publishes each line to output:{worker_id}."""
        mock_runner = MagicMock()

        # Configure the mock runner to invoke on_output during run(),
        # which mirrors how real runners (ClaudeRunner, NoopRunner) behave.
        def run_with_output(**kwargs):
            on_output = kwargs.get("on_output")
            if on_output:
                on_output('{"role": "assistant"}\n')
            return _success_runner_result()

        mock_runner.run.side_effect = run_with_output

        mock_redis = MagicMock()
        mock_redis.xadd_capped.return_value = "1-0"

        result = _execute_task(
            sample_task,
            local_worker_config,
            mock_runner,
            mock_workspace,
            mock_redis,
            logging.getLogger("test"),
        )

        assert result.status == ResultStatus.COMPLETED

        # Verify the callback published the line to Redis during execution
        stream = f"output:{local_worker_config.worker_id}"
        mock_redis.xadd_capped.assert_any_call(
            stream,
            {"line": '{"role": "assistant"}\n', "task_id": sample_task.id},
            maxlen=_OUTPUT_STREAM_MAXLEN,
            approximate=False,
        )
        mock_redis.expire.assert_any_call(stream, _OUTPUT_STREAM_TTL_SECONDS)

    def test_stderr_callback_publishes_task_id_to_redis(
        self, local_worker_config, sample_task, mock_workspace
    ):
        """on_stderr callback tags stderr lines with task_id."""
        mock_runner = MagicMock()

        def run_with_stderr(**kwargs):
            on_stderr = kwargs.get("on_stderr")
            if on_stderr:
                on_stderr("warning\n")
            return _success_runner_result()

        mock_runner.run.side_effect = run_with_stderr

        mock_redis = MagicMock()
        mock_redis.xadd_capped.return_value = "1-0"

        result = _execute_task(
            sample_task,
            local_worker_config,
            mock_runner,
            mock_workspace,
            mock_redis,
            logging.getLogger("test"),
        )

        assert result.status == ResultStatus.COMPLETED
        mock_redis.xadd_capped.assert_any_call(
            f"output:{local_worker_config.worker_id}",
            {"line": "warning\n", "stream": "stderr", "task_id": sample_task.id},
            maxlen=_OUTPUT_STREAM_MAXLEN,
            approximate=False,
        )
        mock_redis.expire.assert_any_call(
            f"output:{local_worker_config.worker_id}", _OUTPUT_STREAM_TTL_SECONDS
        )

    def test_task_start_end_markers(self, local_worker_config, sample_task, mock_workspace):
        """task_start and task_end markers are published to Redis."""
        mock_runner = MagicMock()
        mock_runner.run.return_value = _success_runner_result()

        mock_redis = MagicMock()
        mock_redis.xadd_capped.return_value = "1-0"

        result = _execute_task(
            sample_task,
            local_worker_config,
            mock_runner,
            mock_workspace,
            mock_redis,
            logging.getLogger("test"),
        )

        assert result.status == ResultStatus.COMPLETED

        stream = f"output:{local_worker_config.worker_id}"
        # Filter to the output stream only: xadd_capped is also used to spool
        # task lifecycle events (net.orcest.task.*) onto a separate "events"
        # stream on the same mock, which would otherwise interleave here.
        calls = [c for c in mock_redis.xadd_capped.call_args_list if c[0][0] == stream]

        # First call should be task_start marker
        first_call_args = calls[0][0]
        assert first_call_args[0] == stream
        assert first_call_args[1]["type"] == "task_start"
        assert first_call_args[1]["task_id"] == sample_task.id

        # Last call should be task_end marker
        last_call_args = calls[-1][0]
        assert last_call_args[0] == stream
        assert last_call_args[1]["type"] == "task_end"
        assert last_call_args[1]["task_id"] == sample_task.id
        assert last_call_args[1]["status"] == "completed"

    def test_project_prefixed_task_output_uses_project_stream(
        self, local_worker_config, mock_workspace
    ):
        """Project-scoped tasks publish output under task.key_prefix so the
        dashboard can use the same prefix it gets from locks and results."""
        task = Task.create(
            task_type=TaskType.FIX_PR,
            repo="owner/repo",
            token="test-token-loop",
            resource_type="pr",
            resource_id=42,
            prompt="Fix the failing CI checks",
            branch="fix-ci",
            snapshot_head_sha="abc123",
            key_prefix="projectA",
        )
        mock_runner = MagicMock()

        def run_with_output(**kwargs):
            kwargs["on_output"]("line one\n")
            kwargs["on_stderr"]("warning\n")
            return _success_runner_result()

        mock_runner.run.side_effect = run_with_output
        mock_redis = MagicMock()
        mock_redis.xadd_capped_raw.return_value = "1-0"

        result = _execute_task(
            task,
            local_worker_config,
            mock_runner,
            mock_workspace,
            mock_redis,
            logging.getLogger("test"),
        )

        assert result.status == ResultStatus.COMPLETED
        stream = f"projectA:output:{local_worker_config.worker_id}"
        mock_redis.xadd_capped_raw.assert_any_call(
            stream,
            {
                "type": "task_start",
                "task_id": task.id,
                "resource": "pr #42",
                "repo": task.repo,
                "resource_type": "pr",
                "resource_id": "42",
                "provider": local_worker_config.runner.type,
                "worker_id": local_worker_config.worker_id,
                "branch": "fix-ci",
            },
            maxlen=_OUTPUT_STREAM_MAXLEN,
            approximate=False,
        )
        mock_redis.xadd_capped_raw.assert_any_call(
            stream,
            {"line": "line one\n", "task_id": task.id},
            maxlen=_OUTPUT_STREAM_MAXLEN,
            approximate=False,
        )
        mock_redis.xadd_capped_raw.assert_any_call(
            stream,
            {"line": "warning\n", "stream": "stderr", "task_id": task.id},
            maxlen=_OUTPUT_STREAM_MAXLEN,
            approximate=False,
        )
        mock_redis.xadd_capped_raw.assert_any_call(
            stream,
            {
                "type": "task_end",
                "task_id": task.id,
                "status": "completed",
                "worker_id": local_worker_config.worker_id,
            },
            maxlen=_OUTPUT_STREAM_MAXLEN,
            approximate=False,
        )
        mock_redis.expire_raw.assert_any_call(stream, _OUTPUT_STREAM_TTL_SECONDS)
        output_calls = [
            call
            for call in mock_redis.xadd_capped.call_args_list
            if call[0] and call[0][0] == f"output:{local_worker_config.worker_id}"
        ]
        assert output_calls == []

    def test_worker_runner_exception_returns_failed(
        self, local_worker_config, sample_task, mock_workspace
    ):
        """When runner.run() raises an exception, _execute_task catches it
        and returns a FAILED TaskResult with the exception message."""
        mock_runner = MagicMock()
        mock_runner.run.side_effect = RuntimeError("crash")

        mock_redis = MagicMock()
        mock_redis.xadd_capped.return_value = "1-0"

        result = _execute_task(
            sample_task,
            local_worker_config,
            mock_runner,
            mock_workspace,
            mock_redis,
            logging.getLogger("test"),
        )

        assert result.status == ResultStatus.FAILED
        assert result.task_id == sample_task.id
        assert "crash" in result.summary
        # Workspace cleanup should still run via the finally block
        mock_workspace.cleanup.assert_called_once()

    def test_worker_on_output_redis_error_rate_limited_logging(
        self, local_worker_config, sample_task, mock_workspace, caplog
    ):
        """When redis.xadd_capped raises inside on_output, errors are logged
        at powers of ten (1, 10, 100, …) so operators see ongoing degradation
        without flooding the log."""
        mock_runner = MagicMock()

        # Configure the runner to invoke on_output multiple times (3 errors)
        def run_with_output(**kwargs):
            on_output = kwargs.get("on_output")
            if on_output:
                on_output("line 1\n")
                on_output("line 2\n")
                on_output("line 3\n")
            return _success_runner_result()

        mock_runner.run.side_effect = run_with_output

        mock_redis = MagicMock()

        # task_start marker succeeds, then all output lines fail
        def xadd_capped_side_effect(stream, data, **kwargs):
            if "line" in data:
                raise ConnectionError("Redis down")
            return "1-0"

        mock_redis.xadd_capped.side_effect = xadd_capped_side_effect

        with caplog.at_level(logging.WARNING):
            result = _execute_task(
                sample_task,
                local_worker_config,
                mock_runner,
                mock_workspace,
                mock_redis,
                logging.getLogger("test"),
            )

        assert result.status == ResultStatus.COMPLETED

        # 3 errors: only error #1 is a power of ten, so exactly one warning.
        output_warnings = [
            r for r in caplog.records if "Failed to publish output line" in r.message
        ]
        assert len(output_warnings) == 1
        assert "error #1" in output_warnings[0].message

    def test_worker_on_output_redis_error_logs_at_powers_of_ten(
        self, local_worker_config, sample_task, mock_workspace, caplog
    ):
        """Errors are logged again at #10, #100, etc. to surface ongoing degradation."""
        mock_runner = MagicMock()

        # Configure the runner to invoke on_output 10 times
        def run_with_output(**kwargs):
            on_output = kwargs.get("on_output")
            if on_output:
                for i in range(10):
                    on_output(f"line {i}\n")
            return _success_runner_result()

        mock_runner.run.side_effect = run_with_output

        mock_redis = MagicMock()

        def xadd_capped_side_effect(stream, data, **kwargs):
            if "line" in data:
                raise ConnectionError("Redis down")
            return "1-0"

        mock_redis.xadd_capped.side_effect = xadd_capped_side_effect

        with caplog.at_level(logging.WARNING):
            result = _execute_task(
                sample_task,
                local_worker_config,
                mock_runner,
                mock_workspace,
                mock_redis,
                logging.getLogger("test"),
            )

        assert result.status == ResultStatus.COMPLETED

        # 10 errors: #1 and #10 are powers of ten, so two warnings.
        output_warnings = [
            r for r in caplog.records if "Failed to publish output line" in r.message
        ]
        assert len(output_warnings) == 2
        assert "error #1" in output_warnings[0].message
        assert "error #10" in output_warnings[1].message

    def test_abort_event_passed_to_runner(self, local_worker_config, sample_task, mock_workspace):
        """_execute_task passes abort_event to runner.run()."""
        mock_runner = MagicMock()
        mock_runner.run.return_value = _success_runner_result()

        mock_redis = MagicMock()
        mock_redis.xadd_capped.return_value = "1-0"

        abort_event = threading.Event()

        _execute_task(
            sample_task,
            local_worker_config,
            mock_runner,
            mock_workspace,
            mock_redis,
            logging.getLogger("test"),
            abort_event=abort_event,
        )

        call_kwargs = mock_runner.run.call_args[1]
        assert call_kwargs.get("abort_event") is abort_event

    def test_lock_lost_returns_failed(self, local_worker_config, sample_task, mock_workspace):
        """When abort_event is pre-set, runner returns failure and result is FAILED."""
        mock_runner = MagicMock()
        mock_runner.run.return_value = _failure_runner_result()

        mock_redis = MagicMock()
        mock_redis.xadd_capped.return_value = "1-0"

        abort_event = threading.Event()
        abort_event.set()  # Simulate lock already lost

        result = _execute_task(
            sample_task,
            local_worker_config,
            mock_runner,
            mock_workspace,
            mock_redis,
            logging.getLogger("test"),
            abort_event=abort_event,
        )

        assert result.status == ResultStatus.FAILED

    def test_worker_task_start_publish_failure_continues(
        self, local_worker_config, sample_task, mock_workspace
    ):
        """When redis.xadd_capped raises for the task_start marker,
        the task still executes normally."""
        mock_runner = MagicMock()
        mock_runner.run.return_value = _success_runner_result()

        mock_redis = MagicMock()

        # Fail on task_start marker, succeed on everything else
        first_call = [True]

        def xadd_capped_side_effect(stream, data, **kwargs):
            if first_call[0] and data.get("type") == "task_start":
                first_call[0] = False
                raise ConnectionError("Redis unavailable")
            return "1-0"

        mock_redis.xadd_capped.side_effect = xadd_capped_side_effect

        result = _execute_task(
            sample_task,
            local_worker_config,
            mock_runner,
            mock_workspace,
            mock_redis,
            logging.getLogger("test"),
        )

        # Task should complete successfully despite task_start failure
        assert result.status == ResultStatus.COMPLETED
        assert result.task_id == sample_task.id
        assert result.summary == "All checks fixed"

        # Runner should still have been invoked
        mock_runner.run.assert_called_once()

    def test_rebase_pr_workspace_setup(self, local_worker_config, mock_workspace):
        """REBASE_PR tasks call workspace.setup without base_branch — Claude
        handles the rebase itself (including conflict resolution)."""
        task = Task.create(
            task_type=TaskType.REBASE_PR,
            repo="owner/repo",
            token="tok",
            resource_type="pr",
            resource_id=1,
            prompt="rebase",
            branch="feature",
            base_branch="main",
            snapshot_head_sha="abc123",
        )
        mock_runner = MagicMock()
        mock_runner.run.return_value = _success_runner_result()
        mock_redis = MagicMock()
        mock_redis.xadd_capped.return_value = "1-0"
        mock_redis.xadd_capped_raw.return_value = "1-0"

        _execute_task(
            task,
            local_worker_config,
            mock_runner,
            mock_workspace,
            mock_redis,
            logging.getLogger("test"),
        )

        mock_workspace.setup.assert_called_once_with(
            task.repo,
            task.branch,
            task.token,
        )


# ---------------------------------------------------------------------------
# Output-stream memory budget (#585)
# ---------------------------------------------------------------------------


def test_output_stream_budget_holds_for_worst_case_fanout():
    """Worst-case project × worker fan-out at max entry size must fit the budget.

    Fails if `_OUTPUT_STREAM_MAXLEN` (or the per-entry cap) silently drifts
    back toward the old 20000-entry estimate that filled the 1 GB Redis.
    """
    worst = output_streams_worst_case_bytes(_OUTPUT_BUDGET_PROJECTS, _OUTPUT_BUDGET_WORKERS)
    assert worst <= _OUTPUT_STREAM_BUDGET_BYTES
    # Live fleet: 4 projects × pool.size 4.
    assert output_streams_worst_case_bytes(4, 4) <= _OUTPUT_STREAM_BUDGET_BYTES
    # Incident shape: 4 projects × 6 workers = 24 *:output:* keys.
    assert output_streams_worst_case_bytes(4, 6) <= _OUTPUT_STREAM_BUDGET_BYTES
    redis_ceiling = 1024 * 1024 * 1024
    # Output streams must leave at least half of the 1 GiB noeviction ceiling
    # for :results, :events, task streams, locks, and AOF.
    assert _OUTPUT_STREAM_BUDGET_BYTES <= redis_ceiling // 2
    old_style = (
        _OUTPUT_BUDGET_PROJECTS
        * _OUTPUT_BUDGET_WORKERS
        * 20000
        * _OUTPUT_STREAM_MAX_ENTRY_BYTES
        * 2
    )
    assert old_style > _OUTPUT_STREAM_BUDGET_BYTES


def test_output_stream_budget_rejects_non_positive_fanout():
    with pytest.raises(ValueError, match="positive"):
        output_streams_worst_case_bytes(0, 4)
    with pytest.raises(ValueError, match="positive"):
        output_streams_worst_case_bytes(4, 0)


def test_capped_output_fields_chunks_line_to_max_entry_bytes():
    """A huge stream-json line is split so MAXLEN is a real byte bound."""
    task_id = "t" * 36
    line = "x" * (_OUTPUT_STREAM_MAX_ENTRY_BYTES + 1000)
    chunks = iter_capped_output_fields({"line": line, "task_id": task_id})
    assert len(chunks) > 1
    assert "".join(c["line"] for c in chunks) == line
    for chunk in chunks:
        assert sum(len(v.encode("utf-8")) for v in chunk.values()) <= _OUTPUT_STREAM_MAX_ENTRY_BYTES
        assert chunk["task_id"] == task_id
        assert int(chunk["parts"]) == len(chunks)


def test_capped_output_fields_leaves_short_lines_and_markers_unchanged():
    short = {"line": "hello\n", "task_id": "abc"}
    assert iter_capped_output_fields(short) == [short]
    marker = {"type": "task_start", "task_id": "abc"}
    assert iter_capped_output_fields(marker)[0] is marker


def test_capped_output_fields_preserves_utf8_across_chunk_boundary():
    """A cut inside a multi-byte code point must not drop or replace characters."""
    line = "é" * (_OUTPUT_STREAM_MAX_ENTRY_BYTES)
    chunks = iter_capped_output_fields({"line": line, "task_id": "x"})
    assert "".join(c["line"] for c in chunks) == line
    for chunk in chunks:
        chunk["line"].encode("utf-8")  # must be valid UTF-8
        assert sum(len(v.encode("utf-8")) for v in chunk.values()) <= _OUTPUT_STREAM_MAX_ENTRY_BYTES


def test_publish_task_output_sets_ttl_and_trims_exactly(fake_redis_client):
    """Output streams expire and are capped by exact MAXLEN, not the results cap."""
    stream = "output:worker-1"
    huge = "x" * (_OUTPUT_STREAM_MAX_ENTRY_BYTES * 2)
    _publish_task_output(fake_redis_client, stream, False, {"line": huge, "task_id": "t1"})
    ttl = fake_redis_client.ttl(stream)
    assert 0 < ttl <= _OUTPUT_STREAM_TTL_SECONDS
    entries = fake_redis_client.xread_after(stream, "0-0", count=20)
    assert len(entries) > 1
    assert "".join(payload["line"] for _, payload in entries) == huge
    for _, payload in entries:
        total = sum(len(v.encode("utf-8")) for v in payload.values())
        assert total <= _OUTPUT_STREAM_MAX_ENTRY_BYTES

    for i in range(_OUTPUT_STREAM_MAXLEN + 25):
        _publish_task_output(
            fake_redis_client, stream, False, {"line": f"line-{i}", "task_id": "t1"}
        )
    assert fake_redis_client.xlen(stream) == _OUTPUT_STREAM_MAXLEN
    assert 0 < fake_redis_client.ttl(stream) <= _OUTPUT_STREAM_TTL_SECONDS


def test_publish_task_output_raw_sets_ttl_on_fully_qualified_key(fake_redis_client):
    stream = "projectA:output:worker-1"
    _publish_task_output(fake_redis_client, stream, True, {"line": "hello", "task_id": "t1"})
    ttl = fake_redis_client.client.ttl(stream)
    assert 0 < ttl <= _OUTPUT_STREAM_TTL_SECONDS
    # Raw writes must not land under the client's default prefix.
    assert fake_redis_client.xlen("output:worker-1") == 0


# ---------------------------------------------------------------------------
# Tests for run_worker (the full loop)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_pool_managed_done_handoff_retries_until_redis_accepts(worker_config):
    worker_config.ephemeral = True
    worker_config.pool_managed = True
    redis = MagicMock()
    redis.set_value.side_effect = [ConnectionError("down"), None]
    event = MagicMock()
    event.wait.return_value = False

    assert _signal_ephemeral_done(redis, worker_config, logging.getLogger("test"), event) is True
    assert redis.set_value.call_count == 2
    redis.set_ex.assert_not_called()


def _pool_done_calls(redis: MagicMock) -> list:
    """Return SETEX calls for the lifecycle marker, excluding liveness writes."""
    return [
        redis_call
        for redis_call in redis.set_ex.call_args_list
        if redis_call.args and str(redis_call.args[0]).startswith("pool:done:")
    ]


class TestRunWorker:
    """Integration-level tests for the run_worker main loop.

    These tests mock Redis, Workspace, and the runner to verify the
    loop's orchestration logic: stream reading, locking, result
    publishing, and ACK handling.
    """

    def _build_mock_redis(self):
        """Create a mock RedisClient pre-configured for a single task run.

        Returns the mock_redis instance for assertion inspection.
        """
        mock_redis = MagicMock()
        mock_redis.health_check.return_value = True
        mock_redis.ensure_consumer_group.return_value = None
        mock_redis.ensure_consumer_group_raw.return_value = None
        mock_redis.xack.return_value = 1
        mock_redis.xack_raw.return_value = 1

        # Capture published results via xadd / xadd_capped / xadd_capped_raw
        mock_redis.xadd.return_value = "1-0"
        mock_redis.xadd_capped.return_value = "1-0"
        mock_redis.xadd_capped_raw.return_value = "1-0"

        # Default delivery count below threshold so existing tests proceed normally
        mock_redis.xpending_count.return_value = 1
        mock_redis.xpending_count_raw.return_value = 1

        # For RedisLock -- it accesses redis.client.register_script
        mock_script = MagicMock(return_value=1)
        mock_redis.client.register_script.return_value = mock_script
        # lock.acquire calls redis.client.set(..., nx=True, ex=...)
        mock_redis.client.set.return_value = True
        # RedisLock uses _prefixed() to namespace lock keys
        mock_redis._prefixed = lambda key: f"test:{key}"

        # Raw methods for multi-project support
        mock_redis.delete_raw.return_value = 1
        mock_redis.set_nx_ex_raw.return_value = True
        mock_redis.get_raw.return_value = None

        return mock_redis

    def _setup_run_worker(self, mocker, worker_config, mock_redis, *, heartbeat_mock=None):
        """Patch all external dependencies of run_worker.

        Returns a dict of relevant mocks for assertions.

        Pass ``heartbeat_mock`` to supply an explicit mock for the
        ``orcest.worker.loop.Heartbeat`` class.  When omitted a plain
        ``MagicMock()`` is used so the real thread is never spawned.
        """
        # Patch RedisClient constructor to return our mock
        mocker.patch("orcest.worker.loop.RedisClient", return_value=mock_redis)

        # Patch setup_logging to return a plain logger
        mocker.patch(
            "orcest.worker.loop.setup_logging",
            return_value=logging.getLogger("test.run_worker"),
        )

        # Patch Workspace to avoid real filesystem
        mock_ws = MagicMock()
        mock_ws.setup.return_value = Path("/tmp/fake-workspace/repo")
        mock_ws.current_head_sha.return_value = "abc123"
        mock_ws.cleanup.return_value = None
        mocker.patch("orcest.worker.loop.Workspace", return_value=mock_ws)

        # Patch signal.signal to capture handlers instead of registering
        # real signal handlers (which interfere with pytest).
        signal_handlers = {}

        def fake_signal(signum, handler):
            signal_handlers[signum] = handler

        mocker.patch("orcest.worker.loop.signal.signal", side_effect=fake_signal)

        # Patch create_runner to return a mock runner
        mock_runner = MagicMock()
        mocker.patch("orcest.worker.loop.create_runner", return_value=mock_runner)

        # The early multi-provider dispatch (Task 6) consults the local
        # PROVIDER_REGISTRY + shutil.which() to decide whether the worker
        # image can execute the task's provider.  CI runners do not have the
        # `claude` (or `grok`) binary installed, so without this patch every
        # run_worker test would early-reject and never reach the runner.
        # Default to "supported" here; individual tests that exercise the
        # unsupported path can override.
        unsupported_reason = mocker.patch(
            "orcest.worker.loop.get_unsupported_reason", return_value=None
        )

        # Patch Heartbeat to avoid spawning real daemon threads in unit tests.
        # Use the caller-supplied mock when provided so the dependency is explicit.
        if heartbeat_mock is None:
            heartbeat_mock = MagicMock()
        mocker.patch("orcest.worker.loop.Heartbeat", heartbeat_mock)

        return {
            "workspace": mock_ws,
            "runner": mock_runner,
            "signal_handlers": signal_handlers,
            "unsupported_reason": unsupported_reason,
        }

    def _configure_one_iteration(self, mock_redis, task, signal_handlers):
        """Configure xreadgroup to return one task, then trigger shutdown.

        The first call returns the task on the PR stream; subsequent calls
        trigger SIGTERM.
        """
        task_fields = task.to_dict()
        normal_call_count = 0

        # Drain phase: no pending tasks
        mock_redis.xreadgroup_multi.return_value = []

        def xreadgroup_side_effect(group, consumer, stream, count=1, block_ms=5000, pending=False):
            nonlocal normal_call_count
            normal_call_count += 1
            if normal_call_count == 1:
                return [("entry-1", task_fields)]
            # On subsequent calls, trigger SIGTERM handler to exit loop
            handler = signal_handlers.get(signal.SIGTERM)
            if handler:
                handler(signal.SIGTERM, None)
            return []

        mock_redis.xreadgroup.side_effect = xreadgroup_side_effect

    def test_worker_processes_task(self, mocker, worker_config, sample_task):
        """run_worker reads a task from the stream, executes it, and publishes."""
        mock_redis = self._build_mock_redis()
        mocks = self._setup_run_worker(mocker, worker_config, mock_redis)
        mocks["runner"].run.return_value = _success_runner_result()
        self._configure_one_iteration(mock_redis, sample_task, mocks["signal_handlers"])

        run_worker(worker_config)

        # Verify runner was called
        mocks["runner"].run.assert_called_once()
        # Verify result was published to the results stream
        results_calls = [
            c for c in mock_redis.xadd_capped.call_args_list if c[0][0] == RESULTS_STREAM
        ]
        assert len(results_calls) == 1
        result_fields = results_calls[0][0][1]
        assert result_fields["status"] == ResultStatus.COMPLETED.value
        assert result_fields["task_id"] == sample_task.id

    def test_worker_rejects_task_from_wrong_provider_stream(
        self, mocker, worker_config, sample_task
    ):
        """A dedicated backend never executes a cross-provider payload.

        The rejection is TRANSIENT: a stream mismatch happens during upgrade
        skew (worker template rebaked before orchestrator containers, or
        tasks queued on the legacy shared stream), so it must not burn the
        PR/issue attempt budget permanently."""
        sample_task.provider = "grok"
        mock_redis = self._build_mock_redis()
        mocks = self._setup_run_worker(mocker, worker_config, mock_redis)
        self._configure_one_iteration(mock_redis, sample_task, mocks["signal_handlers"])

        run_worker(worker_config)

        mocks["runner"].run.assert_not_called()
        results_calls = [
            c for c in mock_redis.xadd_capped.call_args_list if c[0][0] == RESULTS_STREAM
        ]
        assert len(results_calls) == 1
        result_fields = results_calls[0][0][1]
        assert result_fields["status"] == ResultStatus.FAILED.value
        assert result_fields["summary"].startswith(TRANSIENT_SUMMARY_PREFIX)
        assert "does not match" in result_fields["summary"]

    def test_worker_processes_issue_task_from_fallback_stream(
        self, mocker, worker_config, sample_task
    ):
        """When the PR stream is empty, the worker falls through to the issue
        stream and processes the task found there.  The ACK targets the issue
        stream, not the PR stream."""
        mock_redis = self._build_mock_redis()
        mocks = self._setup_run_worker(mocker, worker_config, mock_redis)
        mocks["runner"].run.return_value = _success_runner_result()

        task_fields = sample_task.to_dict()
        normal_call_count = 0

        # Drain phase: no pending tasks
        mock_redis.xreadgroup_multi.return_value = []

        def xreadgroup_side_effect(group, consumer, stream, count=1, block_ms=5000, pending=False):
            nonlocal normal_call_count
            normal_call_count += 1
            # PR stream is checked first (non-blocking) -- return empty
            if normal_call_count == 1:
                return []  # PR stream empty
            # Issue stream is checked second (blocking) -- return task
            if normal_call_count == 2:
                return [("entry-1", task_fields)]
            # Trigger shutdown on subsequent calls
            handler = mocks["signal_handlers"].get(signal.SIGTERM)
            if handler:
                handler(signal.SIGTERM, None)
            return []

        mock_redis.xreadgroup.side_effect = xreadgroup_side_effect

        run_worker(worker_config)

        # Runner was called
        mocks["runner"].run.assert_called_once()
        # ACK must target the issue stream (fully-qualified name)
        issue_fq_stream = f"{worker_config.redis.key_prefix}:tasks:issue:{worker_config.backend}"
        mock_redis.xack_raw.assert_any_call(issue_fq_stream, CONSUMER_GROUP, "entry-1")

    def test_worker_acquires_lock(self, mocker, worker_config, sample_task):
        """run_worker acquires a Redis lock keyed by the task's resource_id."""
        mock_redis = self._build_mock_redis()
        mocks = self._setup_run_worker(mocker, worker_config, mock_redis)
        mocks["runner"].run.return_value = _success_runner_result()
        self._configure_one_iteration(mock_redis, sample_task, mocks["signal_handlers"])

        run_worker(worker_config)

        # The lock is acquired via redis.client.set with NX
        set_call = next(
            call for call in mock_redis.client.set.call_args_list if call.kwargs.get("nx") is True
        )
        lock_key = set_call[0][0]
        assert lock_key == f"test:lock:pr:{sample_task.repo}:{sample_task.resource_id}"
        assert set_call[1]["nx"] is True
        assert set_call[1]["ex"] == LOCK_TTL

    def test_unconfirmed_lock_release_forces_stale_result(self, mocker, worker_config, sample_task):
        """A false owner-checked release is proof that publication authority was lost."""
        mock_redis = self._build_mock_redis()
        mocks = self._setup_run_worker(mocker, worker_config, mock_redis)
        mocks["runner"].run.return_value = _success_runner_result()
        mock_redis.client.register_script.return_value.return_value = 0
        self._configure_one_iteration(mock_redis, sample_task, mocks["signal_handlers"])

        run_worker(worker_config)

        result_call = next(
            call for call in mock_redis.xadd_capped.call_args_list if call.args[0] == RESULTS_STREAM
        )
        result = TaskResult.from_dict(result_call.args[1])
        assert result.status is ResultStatus.STALE
        assert "lost the Redis lock" in result.summary

    def test_stale_result_preserves_rotated_credential(self, mocker, worker_config, sample_task):
        """Losing the lock drops the task outcome, never a completed OAuth rotation.

        Regression: the STALE replacement result carried `rate_limit_resets_at`
        forward but not `credential_update`. For Path B providers the CLI has
        already consumed the old refresh token server-side by this point, so
        dropping the new blob strands the provider account on a dead credential
        until an operator re-authenticates by hand.
        """
        mock_redis = self._build_mock_redis()
        mocks = self._setup_run_worker(mocker, worker_config, mock_redis)
        rotated = '{"access_token":"new","refresh_token":"new-refresh"}'
        runner_result = _success_runner_result()
        runner_result.credential_update = rotated
        runner_result.credential_update_minted_at = 1_700_000_000_000_000.0
        mocks["runner"].run.return_value = runner_result
        # Force the unconfirmed-release path that rewrites the result as STALE.
        mock_redis.client.register_script.return_value.return_value = 0
        self._configure_one_iteration(mock_redis, sample_task, mocks["signal_handlers"])

        # Inspect the result handed to publication: a credential-bearing result
        # goes through the private handoff path, whose Lua is not emulated here.
        handoff = mocker.patch("orcest.worker.loop._handoff_result_until_terminal")
        handoff.return_value.terminal = True
        handoff.return_value.publish_outcome = ResultPublishOutcome.PUBLISHED

        run_worker(worker_config)

        published = handoff.call_args.args[1]
        assert published.status is ResultStatus.STALE
        assert "lost the Redis lock" in published.summary
        assert published.credential_update == rotated
        assert published.credential_update_minted_at == 1_700_000_000_000_000.0

    def test_worker_skips_locked_task(self, mocker, worker_config, sample_task):
        """When the lock is already held, the runner is NOT called and the
        task is ACKed only after a transient result is durable.
        """
        mock_redis = self._build_mock_redis()
        mocks = self._setup_run_worker(mocker, worker_config, mock_redis)
        cleanup = mocker.patch("orcest.worker.loop._cleanup_coordination_once", return_value=True)

        # Simulate lock already held: set returns None (NX fails)
        mock_redis.client.set.return_value = None

        self._configure_one_iteration(mock_redis, sample_task, mocks["signal_handlers"])

        run_worker(worker_config)

        # runner should NOT have been called
        mocks["runner"].run.assert_not_called()
        # The task must still be ACKed (to avoid redelivery)
        expected_fq_stream = f"{worker_config.redis.key_prefix}:tasks:{worker_config.backend}"
        mock_redis.xack_raw.assert_called_once_with(expected_fq_stream, CONSUMER_GROUP, "entry-1")
        # The orchestrator consumes the result and owns coordination cleanup.
        cleanup.assert_not_called()
        result_calls = [
            call for call in mock_redis.xadd_capped.call_args_list if call.args[0] == RESULTS_STREAM
        ]
        assert len(result_calls) == 1
        result = TaskResult.from_dict(result_calls[0].args[1])
        assert result.status is ResultStatus.FAILED
        assert result.summary.startswith(TRANSIENT_SUMMARY_PREFIX)

    def test_worker_publishes_result(self, mocker, worker_config, sample_task):
        """A completed task produces a TaskResult with COMPLETED on the
        results stream.
        """
        mock_redis = self._build_mock_redis()
        mocks = self._setup_run_worker(mocker, worker_config, mock_redis)
        mocks["runner"].run.return_value = _success_runner_result()
        self._configure_one_iteration(mock_redis, sample_task, mocks["signal_handlers"])

        run_worker(worker_config)

        # Verify the published result
        results_calls = [
            c for c in mock_redis.xadd_capped.call_args_list if c[0][0] == RESULTS_STREAM
        ]
        assert len(results_calls) == 1
        result_dict = results_calls[0][0][1]
        parsed = TaskResult.from_dict(result_dict)
        assert parsed.status == ResultStatus.COMPLETED
        assert parsed.task_id == sample_task.id
        assert parsed.worker_id == worker_config.worker_id
        assert parsed.repo == sample_task.repo
        assert parsed.resource_id == sample_task.resource_id

    def test_worker_leaves_pending_marker_after_success(self, mocker, worker_config, sample_task):
        """After a successful result publish, orchestrator result handling owns pending cleanup."""
        mock_redis = self._build_mock_redis()
        mocks = self._setup_run_worker(mocker, worker_config, mock_redis)
        mocks["runner"].run.return_value = _success_runner_result()
        cleanup = mocker.patch("orcest.worker.loop._cleanup_coordination_once", return_value=True)
        self._configure_one_iteration(mock_redis, sample_task, mocks["signal_handlers"])

        run_worker(worker_config)

        cleanup.assert_not_called()

    def test_worker_handles_runner_failure(self, mocker, worker_config, sample_task):
        """When the runner returns success=False, the result has FAILED status."""
        mock_redis = self._build_mock_redis()
        mocks = self._setup_run_worker(mocker, worker_config, mock_redis)
        mocks["runner"].run.return_value = _failure_runner_result()
        self._configure_one_iteration(mock_redis, sample_task, mocks["signal_handlers"])

        run_worker(worker_config)

        results_calls = [
            c for c in mock_redis.xadd_capped.call_args_list if c[0][0] == RESULTS_STREAM
        ]
        assert len(results_calls) == 1
        result_dict = results_calls[0][0][1]
        parsed = TaskResult.from_dict(result_dict)
        assert parsed.status == ResultStatus.FAILED
        assert "merge conflict" in parsed.summary.lower()

    def test_worker_health_check_failure_exits(self, mocker, worker_config):
        """When redis.health_check() always returns False, run_worker exhausts
        the startup retry budget and calls sys.exit(1)."""
        mock_redis = self._build_mock_redis()
        # Override health_check to return False on every attempt
        mock_redis.health_check.return_value = False
        self._setup_run_worker(mocker, worker_config, mock_redis)
        # Make abortable backoffs immediate while recording their durations.
        wait_patch = mocker.patch("orcest.worker.loop.threading.Event.wait", return_value=False)

        with pytest.raises(SystemExit) as exc_info:
            run_worker(worker_config)

        assert exc_info.value.code == 1
        # Should never attempt to read from the stream
        mock_redis.xreadgroup.assert_not_called()
        # Confirm the retry loop actually ran (10 attempts, 9 waits in between).
        from orcest.worker.loop import _STARTUP_PING_BACKOFF, _STARTUP_PING_RETRIES

        assert mock_redis.health_check.call_count == _STARTUP_PING_RETRIES
        assert wait_patch.call_count == _STARTUP_PING_RETRIES - 1
        # Backoff sequence is what the helper documents.
        assert [c.kwargs["timeout"] for c in wait_patch.call_args_list] == list(
            _STARTUP_PING_BACKOFF
        )

    def test_worker_health_check_recovers_after_transient_failure(
        self, mocker, worker_config, sample_task
    ):
        """If health_check fails a few times then succeeds, the worker proceeds
        normally instead of exiting — this is the brief-Redis-restart scenario."""
        mock_redis = self._build_mock_redis()
        mocks = self._setup_run_worker(mocker, worker_config, mock_redis)
        # Fail 3 times then succeed.
        mock_redis.health_check.side_effect = [False, False, False, True]
        mocker.patch("orcest.worker.loop.threading.Event.wait", return_value=False)
        # Ensure the loop exits after one iteration so the test terminates.
        mocks["runner"].run.return_value = _success_runner_result()
        self._configure_one_iteration(mock_redis, sample_task, mocks["signal_handlers"])

        # Should NOT raise SystemExit
        run_worker(worker_config)

        assert mock_redis.health_check.call_count == 4

    def test_worker_dead_lettered_result_acks_and_clears_source(
        self, mocker, worker_config, sample_task
    ):
        """A durable full-result DLQ payload becomes the recovery source."""
        mock_redis = self._build_mock_redis()
        mocks = self._setup_run_worker(mocker, worker_config, mock_redis)
        mocks["runner"].run.return_value = _success_runner_result()
        cleanup = mocker.patch("orcest.worker.loop._cleanup_coordination_once", return_value=True)
        self._configure_one_iteration(mock_redis, sample_task, mocks["signal_handlers"])
        mocker.patch("orcest.worker.loop.time.sleep")  # avoid real sleeps during retries

        # Make the results-stream publish fail (all retries)
        def _xadd_capped_side_effect(stream, data, **kwargs):
            if stream == RESULTS_STREAM:
                raise ConnectionError("Redis unavailable")
            return "1-0"

        mock_redis.xadd_capped.side_effect = _xadd_capped_side_effect

        run_worker(worker_config)

        mock_redis.xack_raw.assert_called_once_with(
            "orcest:tasks:claude", CONSUMER_GROUP, "entry-1"
        )
        dead_letter_calls = [
            call
            for call in mock_redis.xadd_capped.call_args_list
            if call.args[0] == DEAD_LETTER_STREAM
        ]
        assert len(dead_letter_calls) == 1
        cleanup.assert_called_once()

    def test_worker_malformed_task_acks_and_continues(self, mocker, worker_config):
        """When a stream entry cannot be deserialized, the worker ACKs it
        (to prevent infinite redelivery) and continues to the next entry."""
        mock_redis = self._build_mock_redis()
        mocks = self._setup_run_worker(mocker, worker_config, mock_redis)

        # Return a malformed entry (missing required fields), then trigger shutdown
        normal_call_count = 0

        # Drain phase: no pending tasks
        mock_redis.xreadgroup_multi.return_value = []

        def xreadgroup_side_effect(group, consumer, stream, count=1, block_ms=5000, pending=False):
            nonlocal normal_call_count
            normal_call_count += 1
            if normal_call_count == 1:
                return [("entry-bad", {"garbage": "data"})]
            handler = mocks["signal_handlers"].get(signal.SIGTERM)
            if handler:
                handler(signal.SIGTERM, None)
            return []

        mock_redis.xreadgroup.side_effect = xreadgroup_side_effect

        run_worker(worker_config)

        # Runner should NOT have been called (task was unparseable)
        mocks["runner"].run.assert_not_called()
        # The malformed entry must still be ACKed (fully-qualified stream name)
        expected_fq_stream = f"{worker_config.redis.key_prefix}:tasks:{worker_config.backend}"
        mock_redis.xack_raw.assert_called_once_with(expected_fq_stream, CONSUMER_GROUP, "entry-bad")

    def test_worker_drains_pending_on_startup(self, mocker, worker_config, sample_task):
        """On startup, pending (unACKed) tasks from a previous lifecycle are
        drained: a FAILED result is published, the entry is ACKed, and the
        pending marker remains for the orchestrator result consumer."""
        mock_redis = self._build_mock_redis()
        mocks = self._setup_run_worker(mocker, worker_config, mock_redis)
        cleanup = mocker.patch("orcest.worker.loop._cleanup_coordination_once")

        # Pending marker still points at this task so the dedup guard (M4-conc)
        # lets the recovery result through (genuine restart mid-execution).
        from orcest.shared.coordination import PendingTaskMetadata

        marker = PendingTaskMetadata(task_id=sample_task.id).to_json()
        mock_redis.get.return_value = marker
        mock_redis.get_raw.return_value = marker

        task_fields = sample_task.to_dict()
        drain_call_count = 0

        def xreadgroup_multi_side_effect(**kwargs):
            nonlocal drain_call_count
            streams = kwargs.get("streams", {})
            if any(v == "0" for v in streams.values()):
                drain_call_count += 1
                if drain_call_count == 1:
                    first_stream = next(iter(streams))
                    return [(first_stream, "pending-1", task_fields)]
                return []
            return []

        mock_redis.xreadgroup_multi.side_effect = xreadgroup_multi_side_effect

        # Normal reading: trigger shutdown immediately
        def xreadgroup_side_effect(group, consumer, stream, count=1, block_ms=5000, pending=False):
            handler = mocks["signal_handlers"].get(signal.SIGTERM)
            if handler:
                handler(signal.SIGTERM, None)
            return []

        mock_redis.xreadgroup.side_effect = xreadgroup_side_effect

        run_worker(worker_config)

        # Runner should NOT have been called (pending tasks are not re-executed)
        mocks["runner"].run.assert_not_called()
        # A FAILED result should have been published for the pending task
        drain_results_calls = [
            c for c in mock_redis.xadd_capped.call_args_list if c[0][0] == RESULTS_STREAM
        ]
        assert len(drain_results_calls) == 1
        stream, result_dict = drain_results_calls[0][0][:2]
        assert stream == RESULTS_STREAM
        parsed = TaskResult.from_dict(result_dict)
        assert parsed.status == ResultStatus.FAILED
        assert parsed.task_id == sample_task.id
        assert "restarted" in parsed.summary.lower()
        # The pending entry must be ACKed (raw, fully-qualified)
        pr_fq_stream = f"{worker_config.redis.key_prefix}:tasks:{worker_config.backend}"
        mock_redis.xack_raw.assert_any_call(pr_fq_stream, CONSUMER_GROUP, "pending-1")
        # A primary result's consumer owns coordination cleanup.
        cleanup.assert_not_called()

    def test_ephemeral_worker_exits_after_startup_recovery(
        self, mocker, worker_config, sample_task
    ):
        """Startup recovery uses the combined stop/signal event for pool handoff."""
        worker_config.ephemeral = True
        mock_redis = self._build_mock_redis()
        mocks = self._setup_run_worker(mocker, worker_config, mock_redis)
        terminal_abort = threading.Event()
        mocker.patch("orcest.worker.loop._make_abort_event", return_value=terminal_abort)
        signal_done = mocker.patch("orcest.worker.loop._signal_ephemeral_done", return_value=True)
        mock_redis.get.return_value = sample_task.id
        mock_redis.xreadgroup_multi.side_effect = [
            [("orcest:tasks:claude", "pending-1", sample_task.to_dict())],
            [],
            [],
        ]

        run_worker(worker_config, threading.Event())

        mocks["runner"].run.assert_not_called()
        mock_redis.xreadgroup.assert_not_called()
        signal_done.assert_called_once()
        assert signal_done.call_args.args[:2] == (mock_redis, worker_config)
        assert signal_done.call_args.args[3] is terminal_abort

    def test_ephemeral_malformed_task_dead_letters_before_done(self, mocker, worker_config):
        """Malformed one-shot work gets a redacted durable handoff before VM teardown."""
        worker_config.ephemeral = True
        mock_redis = self._build_mock_redis()
        mocks = self._setup_run_worker(mocker, worker_config, mock_redis)
        fields = {
            "id": "bad-task",
            "repo": "owner/repo",
            "resource_type": "issue",
            "resource_id": "7",
            "token": "github-secret",
            "claude_token": "claude-secret",
            "credential": "provider-secret",
        }
        mock_redis.xreadgroup_multi.return_value = []
        mock_redis.xreadgroup.return_value = [("entry-bad", fields)]

        run_worker(worker_config)

        mocks["runner"].run.assert_not_called()
        dlq = next(
            call.args[1]
            for call in mock_redis.xadd_capped.call_args_list
            if call.args[0] == DEAD_LETTER_STREAM
        )
        assert all(dlq[field] == "[REDACTED]" for field in REDACTED_FIELDS)
        mock_redis.xack_raw.assert_called_once_with(
            "orcest:tasks:claude", CONSUMER_GROUP, "entry-bad"
        )
        assert len(_pool_done_calls(mock_redis)) == 1

    def test_worker_drain_preserves_pending_entry_on_publish_failure(
        self, mocker, worker_config, sample_task
    ):
        """A failed recovery publish leaves the PEL entry and markers intact."""
        mock_redis = self._build_mock_redis()
        mocks = self._setup_run_worker(mocker, worker_config, mock_redis)
        cleanup = mocker.patch("orcest.worker.loop._cleanup_coordination_once")

        # Pending marker still points at this task so the dedup guard (M4-conc)
        # lets the recovery publish be attempted (which then fails below),
        # exercising the publish-failure cleanup path this test targets.
        from orcest.shared.coordination import PendingTaskMetadata

        marker = PendingTaskMetadata(task_id=sample_task.id).to_json()
        mock_redis.get.return_value = marker
        mock_redis.get_raw.return_value = marker

        # Make the results-stream publish fail during drain
        mock_redis.xadd_capped.side_effect = ConnectionError("Redis unavailable")

        task_fields = sample_task.to_dict()
        drain_call_count = 0

        def xreadgroup_multi_side_effect(**kwargs):
            nonlocal drain_call_count
            streams = kwargs.get("streams", {})
            if any(v == "0" for v in streams.values()):
                drain_call_count += 1
                if drain_call_count == 1:
                    first_stream = next(iter(streams))
                    return [(first_stream, "pending-1", task_fields)]
                return []
            return []

        mock_redis.xreadgroup_multi.side_effect = xreadgroup_multi_side_effect

        # Normal reading: trigger shutdown immediately
        def xreadgroup_side_effect(group, consumer, stream, count=1, block_ms=5000, pending=False):
            handler = mocks["signal_handlers"].get(signal.SIGTERM)
            if handler:
                handler(signal.SIGTERM, None)
            return []

        mock_redis.xreadgroup.side_effect = xreadgroup_side_effect

        with pytest.raises(SystemExit) as exc_info:
            run_worker(worker_config)

        # Runner should NOT have been called (pending tasks are not re-executed)
        assert exc_info.value.code == 1
        mocks["runner"].run.assert_not_called()
        mock_redis.xack_raw.assert_not_called()
        cleanup.assert_not_called()
        # The failed PEL entry is not reread in a tight loop, and the worker
        # exits before claiming any new work.
        assert drain_call_count == 1

    def test_worker_drain_lease_exits_with_restartable_status(self, mocker, worker_config):
        mock_redis = self._build_mock_redis()
        mocks = self._setup_run_worker(mocker, worker_config, mock_redis)
        mock_redis.xreadgroup_multi.return_value = []
        mock_redis.sismember.return_value = True

        with pytest.raises(SystemExit) as exc_info:
            run_worker(worker_config)

        assert exc_info.value.code == 75
        mocks["runner"].run.assert_not_called()
        mock_redis.xreadgroup.assert_not_called()

    def test_abort_event_fires_on_sigterm(self, mocker, worker_config, sample_task):
        """The abort_event passed to _execute_task is set when SIGTERM fires,
        so that retry-backoff sleeps are interrupted promptly on shutdown."""
        mock_redis = self._build_mock_redis()
        mocks = self._setup_run_worker(mocker, worker_config, mock_redis)

        captured_abort_event: list[threading.Event | None] = [None]

        def fake_execute_task(*args, abort_event=None, **kwargs):
            captured_abort_event[0] = abort_event
            # Simulate SIGTERM arriving while the task is running
            handler = mocks["signal_handlers"].get(signal.SIGTERM)
            if handler:
                handler(signal.SIGTERM, None)
            # Assert here, while the task is still "running" — the finally block
            # hasn't fired yet, so only the SIGTERM → shutdown_event path can have
            # set abort_event.  This catches regressions where SIGTERM no longer
            # propagates to the abort_event.
            assert abort_event is not None
            assert abort_event.wait(timeout=1.0), (
                "abort_event not set after SIGTERM; "
                "SIGTERM would not interrupt retry-backoff sleeps"
            )
            task = args[0]
            return TaskResult(
                task_id=task.id,
                worker_id=worker_config.worker_id,
                status=ResultStatus.COMPLETED,
                resource_type=task.resource_type,
                resource_id=task.resource_id,
                branch=task.branch,
                summary="ok",
                duration_seconds=0,
            )

        mocker.patch("orcest.worker.loop._execute_task", side_effect=fake_execute_task)
        # _configure_one_iteration sets up xreadgroup to return the task on the first call.
        # SIGTERM is fired inside fake_execute_task, so the second-call shutdown path
        # configured by _configure_one_iteration is never reached.
        self._configure_one_iteration(mock_redis, sample_task, mocks["signal_handlers"])

        run_worker(worker_config)

        assert captured_abort_event[0] is not None, "abort_event was not passed to _execute_task"

    def test_lock_ttl_equals_3x_heartbeat_interval(self):
        """LOCK_TTL must equal 3 × HEARTBEAT_INTERVAL so that a crashed worker's
        orphaned lock expires within ~180 s instead of ~92 minutes.

        Regression test for issue #206.
        """
        assert LOCK_TTL == 180, (
            f"LOCK_TTL ({LOCK_TTL}s) must equal 180 s (3 × HEARTBEAT_INTERVAL) "
            f"to bound the crash orphaned-lock window"
        )

    def test_heartbeat_uses_explicit_interval_not_lock_ttl(
        self, mocker, worker_config, sample_task
    ):
        """Heartbeat must be started with HEARTBEAT_INTERVAL, not lock.ttl / 3.

        Regression test for issue #121: after PR #83 raised the lock TTL to
        ~5540 s, the default heartbeat interval (ttl/3 ~= 1847 s) caused
        crashed workers to hold stale locks for up to ~92 minutes.  The fix
        passes an explicit HEARTBEAT_INTERVAL so refresh cadence is decoupled
        from TTL size.
        """
        mock_redis = self._build_mock_redis()
        mock_heartbeat_cls = MagicMock()
        mocks = self._setup_run_worker(
            mocker, worker_config, mock_redis, heartbeat_mock=mock_heartbeat_cls
        )
        mocks["runner"].run.return_value = _success_runner_result()
        self._configure_one_iteration(mock_redis, sample_task, mocks["signal_handlers"])

        run_worker(worker_config)

        mock_heartbeat_cls.assert_called_once()
        _, kwargs = mock_heartbeat_cls.call_args
        assert "interval" in kwargs, "Heartbeat must receive an explicit interval kwarg"
        assert kwargs["interval"] == HEARTBEAT_INTERVAL

    def test_worker_base_exception_releases_lock_and_stops_heartbeat(
        self, mocker, worker_config, sample_task
    ):
        """When _execute_task raises a BaseException (e.g. KeyboardInterrupt),
        heartbeat.stop() and lock.release() are called before the exception
        propagates out of run_worker."""
        mock_redis = self._build_mock_redis()

        # Capture the mocked heartbeat and lock so we can assert on them.
        mock_heartbeat = MagicMock()
        mock_lock = MagicMock()
        mock_lock.acquire.return_value = True

        mocks = self._setup_run_worker(mocker, worker_config, mock_redis)
        mocker.patch("orcest.worker.loop.Heartbeat", return_value=mock_heartbeat)
        mocker.patch("orcest.worker.loop.RedisLock", return_value=mock_lock)
        mocker.patch(
            "orcest.worker.loop._execute_task",
            side_effect=KeyboardInterrupt(),
        )

        self._configure_one_iteration(mock_redis, sample_task, mocks["signal_handlers"])

        with pytest.raises(KeyboardInterrupt):
            run_worker(worker_config)

        # Both cleanup methods must be invoked before the exception propagates.
        mock_heartbeat.stop.assert_called_once()
        mock_lock.release.assert_called_once()

    def test_worker_dead_letters_task_exceeding_max_delivery_count(
        self, mocker, worker_config, sample_task
    ):
        """When a task's delivery count exceeds MAX_DELIVERY_COUNT, the worker
        routes it to DEAD_LETTER_STREAM, ACKs the original entry, and does NOT
        invoke the runner."""
        mock_redis = self._build_mock_redis()
        mocks = self._setup_run_worker(mocker, worker_config, mock_redis)

        # Simulate delivery count above the threshold
        mock_redis.xpending_count_raw.return_value = MAX_DELIVERY_COUNT + 1

        self._configure_one_iteration(mock_redis, sample_task, mocks["signal_handlers"])

        run_worker(worker_config)

        # Runner must NOT have been called
        mocks["runner"].run.assert_not_called()

        # Dead-letter stream must have received the task
        dl_calls = [
            c for c in mock_redis.xadd_capped.call_args_list if c[0][0] == DEAD_LETTER_STREAM
        ]
        assert len(dl_calls) == 1, "expected exactly one dead-letter entry"
        dl_fields = dl_calls[0][0][1]
        assert dl_fields["id"] == sample_task.id
        assert "dead_letter_reason" in dl_fields
        assert "original_entry_id" in dl_fields

        # Explicit redaction assertion at DL write site for worker integration test
        assert dl_fields.get("token") == "[REDACTED]"
        assert "test-token-loop" not in str(dl_fields)

        # The original entry must be ACKed so the main stream doesn't stall
        expected_fq_stream = f"{worker_config.redis.key_prefix}:tasks:{worker_config.backend}"
        mock_redis.xack_raw.assert_any_call(expected_fq_stream, CONSUMER_GROUP, "entry-1")

    def test_worker_dead_letters_task_at_max_delivery_count(
        self, mocker, worker_config, sample_task
    ):
        """When delivery count equals MAX_DELIVERY_COUNT the task is dead-lettered."""
        mock_redis = self._build_mock_redis()
        mocks = self._setup_run_worker(mocker, worker_config, mock_redis)

        # Simulate delivery count exactly at the threshold
        mock_redis.xpending_count_raw.return_value = MAX_DELIVERY_COUNT

        self._configure_one_iteration(mock_redis, sample_task, mocks["signal_handlers"])

        run_worker(worker_config)

        # Runner must NOT have been called
        mocks["runner"].run.assert_not_called()

        # Dead-letter stream must have received the task
        dl_calls = [
            c for c in mock_redis.xadd_capped.call_args_list if c[0][0] == DEAD_LETTER_STREAM
        ]
        assert len(dl_calls) == 1, "expected exactly one dead-letter entry"

    def test_worker_dead_letter_clears_pending_marker(self, mocker, worker_config, sample_task):
        """When a task is dead-lettered, the pending-task marker is cleared so
        the orchestrator can re-enqueue work for the resource immediately
        rather than waiting ~95 min for marker TTL expiry."""
        mock_redis = self._build_mock_redis()
        mocks = self._setup_run_worker(mocker, worker_config, mock_redis)
        cleanup = mocker.patch("orcest.worker.loop._cleanup_coordination_once", return_value=True)

        # Delivery count above threshold triggers dead-letter path
        mock_redis.xpending_count_raw.return_value = MAX_DELIVERY_COUNT + 1

        self._configure_one_iteration(mock_redis, sample_task, mocks["signal_handlers"])

        run_worker(worker_config)

        # Runner must NOT have been called (task was dead-lettered)
        mocks["runner"].run.assert_not_called()

        cleanup.assert_called_once()

    def test_worker_processes_task_below_max_delivery_count(
        self, mocker, worker_config, sample_task
    ):
        """When delivery count is below MAX_DELIVERY_COUNT the task is
        processed normally."""
        mock_redis = self._build_mock_redis()
        mocks = self._setup_run_worker(mocker, worker_config, mock_redis)
        mocks["runner"].run.return_value = _success_runner_result()

        # Delivery count is one below the threshold — should still execute
        mock_redis.xpending_count_raw.return_value = MAX_DELIVERY_COUNT - 1

        self._configure_one_iteration(mock_redis, sample_task, mocks["signal_handlers"])

        run_worker(worker_config)

        # Runner must have been called
        mocks["runner"].run.assert_called_once()

        # No dead-letter entry should have been published
        dl_calls = [
            c for c in mock_redis.xadd_capped.call_args_list if c[0][0] == DEAD_LETTER_STREAM
        ]
        assert len(dl_calls) == 0

    def test_ephemeral_worker_exits_after_one_task(self, mocker, worker_config, sample_task):
        """When ephemeral=True, the worker processes one task and exits without
        needing a SIGTERM signal."""
        worker_config.ephemeral = True
        mock_redis = self._build_mock_redis()
        mocks = self._setup_run_worker(mocker, worker_config, mock_redis)
        mocks["runner"].run.return_value = _success_runner_result()

        task_fields = sample_task.to_dict()
        normal_call_count = 0

        # Drain phase: no pending tasks
        mock_redis.xreadgroup_multi.return_value = []

        def xreadgroup_side_effect(group, consumer, stream, count=1, block_ms=5000, pending=False):
            nonlocal normal_call_count
            normal_call_count += 1
            if normal_call_count == 1:
                return [("entry-1", task_fields)]
            # Should never reach here in ephemeral mode
            return []

        mock_redis.xreadgroup.side_effect = xreadgroup_side_effect

        run_worker(worker_config)

        # Runner was called exactly once
        mocks["runner"].run.assert_called_once()
        # Result was published
        results_calls = [
            c for c in mock_redis.xadd_capped.call_args_list if c[0][0] == RESULTS_STREAM
        ]
        assert len(results_calls) == 1
        # pool:done key was set in Redis
        assert _pool_done_calls(mock_redis) == [
            mock_call(f"pool:done:{worker_config.worker_id}", "1", ttl=300)
        ]
        # Only one task read from the stream (no second xreadgroup for normal tasks)
        assert normal_call_count == 1

    def test_ephemeral_worker_sets_pool_done_key(self, mocker, worker_config, sample_task):
        """Ephemeral worker sets pool:done:{worker_id} with TTL 300 on exit."""
        worker_config.ephemeral = True
        mock_redis = self._build_mock_redis()
        mocks = self._setup_run_worker(mocker, worker_config, mock_redis)
        mocks["runner"].run.return_value = _success_runner_result()

        task_fields = sample_task.to_dict()

        mock_redis.xreadgroup_multi.return_value = []
        mock_redis.xreadgroup.return_value = [("entry-1", task_fields)]

        run_worker(worker_config)

        assert _pool_done_calls(mock_redis) == [
            mock_call(f"pool:done:{worker_config.worker_id}", "1", ttl=300)
        ]

    def test_ephemeral_worker_survives_pool_done_key_failure(
        self, mocker, worker_config, sample_task, caplog
    ):
        """When set_ex for pool:done fails, the worker still exits gracefully."""
        worker_config.ephemeral = True
        mock_redis = self._build_mock_redis()
        mocks = self._setup_run_worker(mocker, worker_config, mock_redis)
        mocks["runner"].run.return_value = _success_runner_result()

        def fail_pool_done(key, value, **kwargs):
            if str(key).startswith("pool:done:"):
                raise ConnectionError("Redis unavailable")

        mock_redis.set_ex.side_effect = fail_pool_done

        task_fields = sample_task.to_dict()

        mock_redis.xreadgroup_multi.return_value = []
        mock_redis.xreadgroup.return_value = [("entry-1", task_fields)]

        with caplog.at_level(logging.WARNING):
            run_worker(worker_config)

        # Worker still exited (runner was called once, no hang)
        mocks["runner"].run.assert_called_once()

    def test_standalone_ephemeral_worker_retries_result_before_exit(
        self, mocker, worker_config, sample_task
    ):
        """Standalone --once stays alive until its result handoff is durable."""
        worker_config.ephemeral = True
        mock_redis = self._build_mock_redis()
        mocks = self._setup_run_worker(mocker, worker_config, mock_redis)
        mocks["runner"].run.return_value = _success_runner_result()
        publish = mocker.patch(
            "orcest.worker.loop._publish_result_with_retry",
            side_effect=[ResultPublishOutcome.LOST, ResultPublishOutcome.PUBLISHED],
        )
        mocker.patch("orcest.worker.loop._EPHEMERAL_RESULT_RETRY_SECONDS", 0)
        cleanup = mocker.patch("orcest.worker.loop._cleanup_coordination_once")

        task_fields = sample_task.to_dict()

        mock_redis.xreadgroup_multi.return_value = []
        mock_redis.xreadgroup.return_value = [("entry-1", task_fields)]

        run_worker(worker_config)

        # Worker still exited (runner was called once, no hang)
        mocks["runner"].run.assert_called_once()
        # pool:done key was still set despite publish failure
        assert len(_pool_done_calls(mock_redis)) == 1
        assert publish.call_count == 2
        mock_redis.xack_raw.assert_called_once_with(
            "orcest:tasks:claude", CONSUMER_GROUP, "entry-1"
        )
        cleanup.assert_not_called()

    def test_ephemeral_dead_lettered_result_acks_clears_and_signals_done_once(
        self, mocker, worker_config, sample_task
    ):
        """A durable DLQ handoff is terminal and must not be written repeatedly."""
        worker_config.ephemeral = True
        mock_redis = self._build_mock_redis()
        mocks = self._setup_run_worker(mocker, worker_config, mock_redis)
        mocks["runner"].run.return_value = _success_runner_result()
        mocker.patch("orcest.worker.loop._RESULT_PUBLISH_BACKOFF", (0, 0))
        cleanup = mocker.patch("orcest.worker.loop._cleanup_coordination_once", return_value=True)

        def publish_or_dead_letter(stream, data, **kwargs):
            if stream == RESULTS_STREAM:
                raise ConnectionError("results stream unavailable")
            return "1-0"

        mock_redis.xadd_capped.side_effect = publish_or_dead_letter
        mock_redis.xreadgroup_multi.return_value = []
        mock_redis.xreadgroup.return_value = [("entry-1", sample_task.to_dict())]

        run_worker(worker_config)

        result_calls = [
            call for call in mock_redis.xadd_capped.call_args_list if call.args[0] == RESULTS_STREAM
        ]
        dead_letter_calls = [
            call
            for call in mock_redis.xadd_capped.call_args_list
            if call.args[0] == DEAD_LETTER_STREAM
        ]
        assert len(result_calls) == _RESULT_PUBLISH_RETRIES
        assert len(dead_letter_calls) == 1
        mock_redis.xack_raw.assert_called_once_with(
            "orcest:tasks:claude", CONSUMER_GROUP, "entry-1"
        )
        assert _pool_done_calls(mock_redis) == [
            mock_call(f"pool:done:{worker_config.worker_id}", "1", ttl=300)
        ]
        cleanup.assert_called_once()
        assert cleanup.call_args.args[0] is mock_redis
        assert cleanup.call_args.args[1].task_id == sample_task.id

    def test_ephemeral_total_publish_loss_keeps_task_pending_and_does_not_signal_done(
        self, mocker, worker_config, sample_task
    ):
        """Shutdown after total loss retains the PEL entry and coordination state."""
        worker_config.ephemeral = True
        mock_redis = self._build_mock_redis()
        mocks = self._setup_run_worker(mocker, worker_config, mock_redis)
        mocks["runner"].run.return_value = _success_runner_result()
        cleanup = mocker.patch("orcest.worker.loop._cleanup_coordination_once")

        def lose_result(*args, **kwargs):
            mocks["signal_handlers"][signal.SIGTERM](signal.SIGTERM, None)
            return ResultPublishOutcome.LOST

        publish = mocker.patch(
            "orcest.worker.loop._publish_result_with_retry",
            side_effect=lose_result,
        )
        mock_redis.xreadgroup_multi.return_value = []
        mock_redis.xreadgroup.return_value = [("entry-1", sample_task.to_dict())]

        run_worker(worker_config)

        publish.assert_called_once()
        mock_redis.xack_raw.assert_not_called()
        assert _pool_done_calls(mock_redis) == []
        mock_redis.set_value.assert_not_called()
        cleanup.assert_not_called()

    def test_non_ephemeral_publish_loss_retries_without_rerunning_task(
        self, mocker, worker_config, sample_task
    ):
        """A live reusable worker must not strand its own PEL entry on LOST."""
        mock_redis = self._build_mock_redis()
        mocks = self._setup_run_worker(mocker, worker_config, mock_redis)
        mocks["runner"].run.return_value = _success_runner_result()
        publish = mocker.patch(
            "orcest.worker.loop._publish_result_with_retry",
            side_effect=[ResultPublishOutcome.LOST, ResultPublishOutcome.PUBLISHED],
        )
        mocker.patch("orcest.worker.loop._EPHEMERAL_RESULT_RETRY_SECONDS", 0)
        self._configure_one_iteration(mock_redis, sample_task, mocks["signal_handlers"])

        run_worker(worker_config)

        mocks["runner"].run.assert_called_once()
        assert publish.call_count == 2
        mock_redis.xack_raw.assert_called_once_with(
            "orcest:tasks:claude", CONSUMER_GROUP, "entry-1"
        )

    def test_ack_retry_does_not_republish_result(self, mocker, worker_config, sample_task):
        """Once publication is durable, only XACK is retried."""
        worker_config.ephemeral = True
        mock_redis = self._build_mock_redis()
        mocks = self._setup_run_worker(mocker, worker_config, mock_redis)
        mocks["runner"].run.return_value = _success_runner_result()
        publish = mocker.patch(
            "orcest.worker.loop._publish_result_with_retry",
            return_value=ResultPublishOutcome.PUBLISHED,
        )
        mocker.patch("orcest.worker.loop._SOURCE_ACK_RETRY_BASE_SECONDS", 0)
        mock_redis.xack_raw.side_effect = [ConnectionError("ack unavailable"), 1]
        mock_redis.xreadgroup_multi.return_value = []
        mock_redis.xreadgroup.return_value = [("entry-1", sample_task.to_dict())]

        run_worker(worker_config)

        publish.assert_called_once()
        assert mock_redis.xack_raw.call_count == 2
        assert len(_pool_done_calls(mock_redis)) == 1
        mock_redis.client.delete.assert_any_call(
            _handoff_marker_key(
                "test:results",
                "orcest:tasks:claude",
                "entry-1",
                sample_task.id,
            )
        )

    def test_ack_failure_after_cleanup_does_not_signal_pool_done(
        self, mocker, worker_config, sample_task
    ):
        """Durable DLQ plus cleanup is not terminal while its source remains pending."""
        worker_config.ephemeral = True
        mock_redis = self._build_mock_redis()
        mocks = self._setup_run_worker(mocker, worker_config, mock_redis)
        mocks["runner"].run.return_value = _success_runner_result()
        mocker.patch(
            "orcest.worker.loop._publish_result_with_retry",
            return_value=ResultPublishOutcome.DEAD_LETTERED,
        )
        cleanup = mocker.patch("orcest.worker.loop._cleanup_coordination_once", return_value=True)

        def fail_ack(*args, **kwargs):
            mocks["signal_handlers"][signal.SIGTERM](signal.SIGTERM, None)
            raise ConnectionError("ack unavailable")

        mock_redis.xack_raw.side_effect = fail_ack
        mock_redis.xreadgroup_multi.return_value = []
        mock_redis.xreadgroup.return_value = [("entry-1", sample_task.to_dict())]

        run_worker(worker_config)

        cleanup.assert_called_once()
        assert _pool_done_calls(mock_redis) == []
        mock_redis.set_value.assert_not_called()

    def test_cleanup_failure_does_not_ack_or_signal_pool_done(
        self, mocker, worker_config, sample_task
    ):
        """DLQ cleanup is part of the terminal boundary and precedes XACK."""
        worker_config.ephemeral = True
        mock_redis = self._build_mock_redis()
        mocks = self._setup_run_worker(mocker, worker_config, mock_redis)
        mocks["runner"].run.return_value = _success_runner_result()
        mocker.patch(
            "orcest.worker.loop._publish_result_with_retry",
            return_value=ResultPublishOutcome.DEAD_LETTERED,
        )
        stop_event = threading.Event()

        def fail_cleanup(*args, **kwargs):
            stop_event.set()
            raise ConnectionError("cleanup unavailable")

        cleanup = mocker.patch(
            "orcest.worker.loop._cleanup_coordination_once",
            side_effect=fail_cleanup,
        )
        mock_redis.xreadgroup_multi.return_value = []
        mock_redis.xreadgroup.return_value = [("entry-1", sample_task.to_dict())]

        run_worker(worker_config, stop_event)

        cleanup.assert_called_once()
        mock_redis.xack_raw.assert_not_called()
        assert _pool_done_calls(mock_redis) == []
        mock_redis.set_value.assert_not_called()

    def test_preset_stop_event_aborts_redis_wait_before_health_check(self, mocker, worker_config):
        """A preset external stop exits cleanly before pinging or reading work."""
        mock_redis = self._build_mock_redis()
        mocks = self._setup_run_worker(mocker, worker_config, mock_redis)
        stop_event = threading.Event()
        stop_event.set()

        run_worker(worker_config, stop_event)

        mock_redis.health_check.assert_not_called()
        mock_redis.xreadgroup_multi.assert_not_called()
        mock_redis.xreadgroup.assert_not_called()
        mocks["runner"].run.assert_not_called()
        assert set(mocks["signal_handlers"]) == {signal.SIGTERM, signal.SIGINT}

    def test_ephemeral_unsupported_task_signals_done_after_durable_handoff(
        self, mocker, worker_config, sample_task
    ):
        """A durable early rejection is a terminal ephemeral-worker outcome."""
        worker_config.ephemeral = True
        mock_redis = self._build_mock_redis()
        mocks = self._setup_run_worker(mocker, worker_config, mock_redis)
        mocks["unsupported_reason"].return_value = "provider CLI missing"
        reject = mocker.patch(
            "orcest.worker.loop._early_reject_unsupported_provider",
            return_value=ResultHandoff(ResultPublishOutcome.DEAD_LETTERED, source_acked=True),
        )
        mock_redis.xreadgroup_multi.return_value = []
        mock_redis.xreadgroup.return_value = [("entry-1", sample_task.to_dict())]

        run_worker(worker_config)

        reject.assert_called_once()
        mocks["runner"].run.assert_not_called()
        assert _pool_done_calls(mock_redis) == [
            mock_call(f"pool:done:{worker_config.worker_id}", "1", ttl=300)
        ]
        assert mock_redis.xreadgroup.call_count == 1

    def test_ephemeral_unsupported_task_retries_until_handoff_is_durable(
        self, mocker, worker_config, sample_task
    ):
        """Total Redis loss retains the worker until a later handoff succeeds."""
        worker_config.ephemeral = True
        mock_redis = self._build_mock_redis()
        mocks = self._setup_run_worker(mocker, worker_config, mock_redis)
        mocks["unsupported_reason"].return_value = "provider CLI missing"
        reject = mocker.patch(
            "orcest.worker.loop._early_reject_unsupported_provider",
            return_value=ResultHandoff(ResultPublishOutcome.PUBLISHED, source_acked=True),
        )
        mocker.patch("orcest.worker.loop._EPHEMERAL_RESULT_RETRY_SECONDS", 0)
        mock_redis.xreadgroup_multi.return_value = []
        mock_redis.xreadgroup.return_value = [("entry-1", sample_task.to_dict())]

        run_worker(worker_config)

        reject.assert_called_once()
        mocks["runner"].run.assert_not_called()
        assert len(_pool_done_calls(mock_redis)) == 1
        assert mock_redis.xreadgroup.call_count == 1

    def test_ephemeral_worker_exits_on_runner_failure(self, mocker, worker_config, sample_task):
        """Ephemeral worker exits and sets pool:done even when the runner fails."""
        worker_config.ephemeral = True
        mock_redis = self._build_mock_redis()
        mocks = self._setup_run_worker(mocker, worker_config, mock_redis)
        mocks["runner"].run.return_value = _failure_runner_result()

        task_fields = sample_task.to_dict()

        mock_redis.xreadgroup_multi.return_value = []
        mock_redis.xreadgroup.return_value = [("entry-1", task_fields)]

        run_worker(worker_config)

        # Runner was called exactly once
        mocks["runner"].run.assert_called_once()
        # Result was published with FAILED status
        results_calls = [
            c for c in mock_redis.xadd_capped.call_args_list if c[0][0] == RESULTS_STREAM
        ]
        assert len(results_calls) == 1
        result_fields = results_calls[0][0][1]
        assert result_fields["status"] == ResultStatus.FAILED.value
        # pool:done key was set despite task failure
        assert _pool_done_calls(mock_redis) == [
            mock_call(f"pool:done:{worker_config.worker_id}", "1", ttl=300)
        ]

    def test_non_ephemeral_worker_continues_looping(self, mocker, worker_config, sample_task):
        """Default (non-ephemeral) worker does NOT exit after one task and
        does NOT set pool:done key."""
        assert not worker_config.ephemeral  # sanity: default is False
        mock_redis = self._build_mock_redis()
        mocks = self._setup_run_worker(mocker, worker_config, mock_redis)
        mocks["runner"].run.return_value = _success_runner_result()
        self._configure_one_iteration(mock_redis, sample_task, mocks["signal_handlers"])

        run_worker(worker_config)

        # Runner was called (task processed)
        mocks["runner"].run.assert_called_once()
        # pool:done key must NOT have been set
        assert _pool_done_calls(mock_redis) == []

    def test_ephemeral_worker_exits_after_dead_lettered_task(
        self, mocker, worker_config, sample_task
    ):
        """When an ephemeral worker receives a task that gets dead-lettered
        (delivery count >= MAX_DELIVERY_COUNT), the worker sets pool:done
        and exits cleanly instead of looping indefinitely on an empty queue.
        """
        worker_config.ephemeral = True
        mock_redis = self._build_mock_redis()
        mocks = self._setup_run_worker(mocker, worker_config, mock_redis)

        # Delivery count above threshold triggers dead-letter path
        mock_redis.xpending_count_raw.return_value = MAX_DELIVERY_COUNT + 1

        task_fields = sample_task.to_dict()
        normal_call_count = 0

        # Drain phase: no pending tasks
        mock_redis.xreadgroup_multi.return_value = []

        def xreadgroup_side_effect(group, consumer, stream, count=1, block_ms=5000, pending=False):
            nonlocal normal_call_count
            normal_call_count += 1
            if normal_call_count == 1:
                return [("entry-1", task_fields)]
            # Should never reach here -- ephemeral mode exits after dead-letter
            return []

        mock_redis.xreadgroup.side_effect = xreadgroup_side_effect

        run_worker(worker_config)

        # Runner must NOT have been called (task was dead-lettered)
        mocks["runner"].run.assert_not_called()
        # Dead-letter stream received the task
        dl_calls = [
            c for c in mock_redis.xadd_capped.call_args_list if c[0][0] == DEAD_LETTER_STREAM
        ]
        assert len(dl_calls) == 1
        # pool:done key was set (ephemeral exit)
        assert _pool_done_calls(mock_redis) == [
            mock_call(f"pool:done:{worker_config.worker_id}", "1", ttl=300)
        ]
        # Worker exited after one task (no second xreadgroup for normal tasks)
        assert normal_call_count == 1

    def test_ephemeral_dead_letter_failure_does_not_signal_done(
        self, mocker, worker_config, sample_task
    ):
        """The pool VM remains recoverable until DLQ plus source ACK are terminal."""
        worker_config.ephemeral = True
        mock_redis = self._build_mock_redis()
        mocks = self._setup_run_worker(mocker, worker_config, mock_redis)
        mock_redis.xpending_count_raw.return_value = MAX_DELIVERY_COUNT
        mocker.patch("orcest.worker.loop._dead_letter_task", return_value=False)
        mock_redis.xreadgroup_multi.return_value = []
        mock_redis.xreadgroup.return_value = [("entry-1", sample_task.to_dict())]

        run_worker(worker_config)

        mocks["runner"].run.assert_not_called()
        assert _pool_done_calls(mock_redis) == []
        mock_redis.set_value.assert_not_called()

    @pytest.mark.skip(
        reason="Dead-letter now happens before lock acquisition; needs redesign per issue #398"
    )
    def test_ephemeral_worker_releases_lock_before_shutdown_on_dead_letter(
        self, mocker, worker_config, sample_task
    ):
        """The lock must be released before shutdown=True is set when an
        ephemeral worker dead-letters a task.  Without this the lock lingers
        for LOCK_TTL seconds, blocking other workers from claiming the same
        resource."""
        worker_config.ephemeral = True
        mock_redis = self._build_mock_redis()
        self._setup_run_worker(mocker, worker_config, mock_redis)

        # Delivery count above threshold triggers dead-letter path
        mock_redis.xpending_count_raw.return_value = MAX_DELIVERY_COUNT + 1

        release_called_before_set_ex: list[bool] = []
        set_ex_called: list[bool] = [False]

        def make_mock_lock(redis, key, *, ttl, owner):
            mock_lock = MagicMock()
            mock_lock.acquire.return_value = True

            def release_side_effect():
                # Record whether set_ex (pool:done) has NOT yet been called
                release_called_before_set_ex.append(not set_ex_called[0])

            mock_lock.release.side_effect = release_side_effect
            return mock_lock

        mocker.patch("orcest.worker.loop.RedisLock", side_effect=make_mock_lock)

        def set_ex_side_effect(key, value, **kwargs):
            if "pool:done" in key:
                set_ex_called[0] = True

        mock_redis.set_ex.side_effect = set_ex_side_effect

        task_fields = sample_task.to_dict()

        normal_call_count = [0]

        mock_redis.xreadgroup_multi.return_value = []

        def xreadgroup_side_effect(group, consumer, stream, count=1, block_ms=5000, pending=False):
            normal_call_count[0] += 1
            if normal_call_count[0] == 1:
                return [("entry-1", task_fields)]
            return []

        mock_redis.xreadgroup.side_effect = xreadgroup_side_effect

        run_worker(worker_config)

        # lock.release() must have been called exactly once (in dead-letter path)
        assert len(release_called_before_set_ex) == 1
        # release() must precede the pool:done set_ex call (i.e. before shutdown)
        assert release_called_before_set_ex[0], (
            "lock.release() was called AFTER pool:done set_ex; "
            "it must be called BEFORE shutdown is set"
        )

    def test_non_ephemeral_worker_continues_after_dead_lettered_task(
        self, mocker, worker_config, sample_task
    ):
        """Non-ephemeral workers loop back to read more tasks after dead-lettering."""
        assert not worker_config.ephemeral
        mock_redis = self._build_mock_redis()
        mocks = self._setup_run_worker(mocker, worker_config, mock_redis)

        # Delivery count above threshold triggers dead-letter path
        mock_redis.xpending_count_raw.return_value = MAX_DELIVERY_COUNT + 1

        task_fields = sample_task.to_dict()
        normal_call_count = 0

        # Drain phase: no pending tasks
        mock_redis.xreadgroup_multi.return_value = []

        def xreadgroup_side_effect(group, consumer, stream, count=1, block_ms=5000, pending=False):
            nonlocal normal_call_count
            normal_call_count += 1
            if normal_call_count == 1:
                return [("entry-1", task_fields)]
            # After dead-lettering, trigger SIGTERM to exit
            handler = mocks["signal_handlers"].get(signal.SIGTERM)
            if handler:
                handler(signal.SIGTERM, None)
            return []

        mock_redis.xreadgroup.side_effect = xreadgroup_side_effect

        run_worker(worker_config)

        # Runner must NOT have been called (task was dead-lettered)
        mocks["runner"].run.assert_not_called()
        # pool:done was NOT set (not ephemeral)
        assert _pool_done_calls(mock_redis) == []
        # Worker looped back after dead-lettering (second xreadgroup call happened)
        assert normal_call_count >= 2


# ---------------------------------------------------------------------------
# Tests for _dead_letter_task helper
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDeadLetterTask:
    """Tests for the _dead_letter_task helper."""

    def test_publishes_to_dead_letter_stream_and_acks(self, local_worker_config, sample_task):
        """_dead_letter_task writes to DEAD_LETTER_STREAM and ACKs the entry."""
        mock_redis = MagicMock()
        mock_redis.xadd_capped.return_value = "1-0"
        mock_redis.xack_raw.return_value = 1

        _dead_letter_task(
            mock_redis,
            "tasks:claude",
            "entry-42",
            sample_task,
            5,
            logging.getLogger("test"),
        )

        mock_redis.xadd_capped.assert_called_once()
        stream, fields, *_ = mock_redis.xadd_capped.call_args[0]
        assert stream == DEAD_LETTER_STREAM
        assert fields["id"] == sample_task.id
        assert "dead_letter_reason" in fields
        assert fields["original_entry_id"] == "entry-42"
        assert fields["tasks_stream"] == "tasks:claude"
        assert fields["delivery_count"] == "5"

        # Explicit redaction assertion at the DL write site (code quality review)
        assert fields.get("token") == "[REDACTED]", "secret token must be redacted in DL"
        assert fields.get("credential") == "[REDACTED]"
        assert fields.get("claude_token") == "[REDACTED]"
        # sample secret must not appear
        assert "test-token-loop" not in str(fields)

        mock_redis.xack_raw.assert_called_once_with("tasks:claude", CONSUMER_GROUP, "entry-42")

    def test_dead_letter_redaction_regression_both_paths(self, local_worker_config):
        """Regression test (Task 8): Task created with real secrets produces DL entries
        containing only "[REDACTED]" for REDACTED_FIELDS in both _dead_letter_task and
        result-publish exhaustion paths. No raw secret ever reaches the DL stream.
        """
        from orcest.shared.models import REDACTED_FIELDS, ResultStatus, Task, TaskResult, TaskType
        from orcest.worker.loop import _dead_letter_task, _publish_result_with_retry

        secret = "sk-regression-real-secret-XYZ123-should-never-appear"
        task = Task.create(
            task_type=TaskType.FIX_PR,
            repo="owner/regrepo",
            token=secret,
            claude_token=secret,
            credential=secret,
            provider="claude",
            model=None,
            resource_type="pr",
            resource_id=99,
            prompt="test prompt for redaction",
            branch="main",
            base_branch="main",
            key_prefix="",
        )
        mock_redis = MagicMock()

        # Path 1: _dead_letter_task
        _dead_letter_task(mock_redis, "tasks:claude", "e-1", task, 10, logging.getLogger("test"))
        assert mock_redis.xadd_capped.called
        dl1 = mock_redis.xadd_capped.call_args[0][1]
        for f in REDACTED_FIELDS:
            assert dl1.get(f) == "[REDACTED]", f"redact {f} path1"
        assert secret not in str(dl1)

        # Path 2: result publish exhaustion (force all result xadds to fail to reach DL)
        mock_redis.reset_mock()
        result = TaskResult(
            task_id=task.id,
            worker_id="w1",
            status=ResultStatus.FAILED,
            branch="main",
            summary="boom",
            duration_seconds=1,
            resource_type="pr",
            resource_id=99,
        )

        def fail_results_only(stream, data, **kw):
            if stream == RESULTS_STREAM:
                raise RuntimeError("simulated result publish fail")
            # allow DL xadd
            return "0-0"

        mock_redis.xadd_capped.side_effect = fail_results_only
        _publish_result_with_retry(
            mock_redis, result, task, logging.getLogger("test"), "tasks:claude", "e-2"
        )
        # find the DL call
        dl2 = None
        for call in mock_redis.xadd_capped.call_args_list:
            if call[0][0] == DEAD_LETTER_STREAM:
                dl2 = call[0][1]
                break
        assert dl2 is not None, "DL path2 exercised"
        assert dl2["repo"] == task.repo
        for f in REDACTED_FIELDS:
            assert dl2.get(f) == "[REDACTED]", f"redact {f} path2"
        assert secret not in str(dl2)

    def test_publish_failure_preserves_source(self, local_worker_config, sample_task):
        """Shutdown during DLQ failure leaves the source recoverable."""
        mock_redis = MagicMock()
        mock_redis.xadd_capped.side_effect = ConnectionError("Redis unavailable")
        mock_redis.xack_raw.return_value = 1
        abort = threading.Event()
        abort.set()

        terminal = _dead_letter_task(
            mock_redis,
            "tasks:claude",
            "entry-99",
            sample_task,
            3,
            logging.getLogger("test"),
            abort_event=abort,
        )

        assert terminal is False
        mock_redis.xack_raw.assert_not_called()

    def test_clears_pending_task_marker(self, local_worker_config, sample_task):
        """_dead_letter_task clears the pending-task marker so the orchestrator
        can re-enqueue work for the resource immediately."""
        mock_redis = MagicMock()
        mock_redis.xadd_capped.return_value = "1-0"
        mock_redis.xack_raw.return_value = 1

        with patch("orcest.worker.loop._cleanup_coordination_once", return_value=True) as cleanup:
            _dead_letter_task(
                mock_redis,
                "tasks:claude",
                "entry-42",
                sample_task,
                5,
                logging.getLogger("test"),
            )

            cleanup.assert_called_once()

    def test_preserves_pending_marker_when_publish_fails(self, local_worker_config, sample_task):
        """Coordination cannot be cleared before DLQ and ACK are terminal."""
        mock_redis = MagicMock()
        mock_redis.xadd_capped.side_effect = ConnectionError("Redis unavailable")
        mock_redis.xack_raw.side_effect = ConnectionError("Redis unavailable")
        abort = threading.Event()
        abort.set()

        with patch("orcest.worker.loop._cleanup_coordination_once") as cleanup:
            _dead_letter_task(
                mock_redis,
                "tasks:claude",
                "entry-99",
                sample_task,
                3,
                logging.getLogger("test"),
                abort_event=abort,
            )

            cleanup.assert_not_called()

    def test_ack_retry_does_not_duplicate_dead_letter(self, mocker, sample_task):
        """A DLQ row is written once even when its following XACK is transiently down."""
        mock_redis = MagicMock()
        mock_redis.xadd_capped.return_value = "1-0"
        mock_redis.xack_raw.side_effect = [ConnectionError("ack unavailable"), 1]
        mocker.patch("orcest.worker.loop._SOURCE_ACK_RETRY_BASE_SECONDS", 0)
        cleanup = mocker.patch("orcest.worker.loop._cleanup_coordination_once", return_value=True)

        terminal = _dead_letter_task(
            mock_redis,
            "tasks:claude",
            "entry-99",
            sample_task,
            3,
            logging.getLogger("test"),
        )

        assert terminal is True
        mock_redis.xadd_capped.assert_called_once()
        assert mock_redis.xack_raw.call_count == 2
        cleanup.assert_called_once()
        mock_redis.client.delete.assert_called_with(
            _handoff_marker_key(
                DEAD_LETTER_STREAM,
                "tasks:claude",
                "entry-99",
                sample_task.id,
            )
        )

    @pytest.mark.parametrize("raw_pending", [None, '{"task_id":"newer-task"}'])
    def test_absent_or_newer_pending_owner_is_safe_noop(self, sample_task, raw_pending):
        """A stale terminal task cannot erase a newer task's coordination state."""
        mock_redis = MagicMock()
        pipe = mock_redis.client.pipeline.return_value
        pipe.get.return_value = raw_pending

        terminal = _cleanup_coordination_once(
            mock_redis,
            CoordinationIdentity(
                task_id=sample_task.id,
                repo=sample_task.repo,
                resource_type=sample_task.resource_type,
                resource_id=sample_task.resource_id,
                snapshot_head_sha=sample_task.snapshot_head_sha,
            ),
        )

        assert terminal is True
        pipe.multi.assert_not_called()
        pipe.delete.assert_not_called()
        pipe.execute.assert_not_called()


@pytest.mark.unit
def test_atomic_cleanup_deletes_owned_pending_and_attempt_state(fake_redis_client, sample_task):
    """Owned terminal coordination is committed in one Redis transaction."""
    from orcest.shared.coordination import PendingTaskMetadata

    pending_key = f"pending:pr:{sample_task.repo}:{sample_task.resource_id}"
    attempts_key = f"pr:{sample_task.repo}:{sample_task.resource_id}:attempts"
    fake_redis_client.set_value(
        pending_key,
        PendingTaskMetadata(task_id=sample_task.id).to_json(),
    )
    fake_redis_client.hset(attempts_key, "head_sha", sample_task.snapshot_head_sha)
    fake_redis_client.hset(attempts_key, "count", "1")

    terminal = _cleanup_coordination_once(
        fake_redis_client,
        CoordinationIdentity(
            task_id=sample_task.id,
            repo=sample_task.repo,
            resource_type=sample_task.resource_type,
            resource_id=sample_task.resource_id,
            snapshot_head_sha=sample_task.snapshot_head_sha,
        ),
    )

    assert terminal is True
    assert fake_redis_client.get(pending_key) is None
    assert fake_redis_client.hgetall(attempts_key) == {}


@pytest.mark.unit
def test_handoff_marker_is_task_scoped_and_uses_exact_lookup():
    """A reused source entry ID cannot dedupe a different task."""
    mock_redis = MagicMock()
    target_stream = "orcest:results"
    tasks_stream = "orcest:tasks:claude"
    marker_a = _handoff_marker_key(target_stream, tasks_stream, "1-0", "task-a")

    def get_marker(key):
        return "2-0|fingerprint-a" if key == marker_a else None

    mock_redis.client.get.side_effect = get_marker
    mock_redis.client.xrange.return_value = [("2-0", {_HANDOFF_FINGERPRINT_FIELD: "fingerprint-a"})]
    logger = logging.getLogger("test.handoff")

    assert (
        _stream_handoff_state(
            mock_redis,
            target_stream,
            tasks_stream,
            "1-0",
            "task-a",
            logger,
        )
        is True
    )
    assert (
        _stream_handoff_state(
            mock_redis,
            target_stream,
            tasks_stream,
            "1-0",
            "task-b",
            logger,
        )
        is False
    )
    mock_redis.client.xrange.assert_called_once_with(
        target_stream,
        min="2-0",
        max="2-0",
        count=1,
    )
    mock_redis.client.xrevrange.assert_not_called()


# ---------------------------------------------------------------------------
# Tests for _publish_result_with_retry
# ---------------------------------------------------------------------------


def test_task_result_copies_stable_provider_account_from_task(local_worker_config):
    task = Task.create(
        task_type=TaskType.FIX_PR,
        repo="owner/repo",
        token="github-token",
        resource_type="pr",
        resource_id=42,
        prompt="fix it",
        provider="grok",
        credential='{"access_token":"rotated","refresh_token":"rotated-refresh"}',
        provider_account="grok:originalhash",
    )

    result = _task_result(
        task,
        local_worker_config,
        ResultStatus.COMPLETED,
        branch=None,
        summary="done",
        duration_seconds=1,
    )

    assert result.provider_account == "grok:originalhash"


@pytest.mark.unit
class TestPublishResultWithRetry:
    """Tests for the _publish_result_with_retry helper."""

    def _make_result(self, task: "Task") -> "TaskResult":
        return TaskResult(
            task_id=task.id,
            worker_id="test-worker",
            status=ResultStatus.COMPLETED,
            resource_type=task.resource_type,
            resource_id=task.resource_id,
            repo=task.repo,
            branch=task.branch,
            summary="done",
            duration_seconds=1,
        )

    def test_redacted_credential_update_dead_letter_is_not_terminal(self, sample_task):
        """A redacted diagnostic cannot replace a credential-rotation result."""
        result = self._make_result(sample_task)
        result.credential_update = '{"key":"super-secret-refresh-token"}'

        dead_letter_payloads = []

        def xadd_capped(stream, data, **kwargs):
            if stream == DEAD_LETTER_STREAM:
                dead_letter_payloads.append(data)
            return "1-0"

        def xadd_capped_raw(fq, data, **kwargs):
            raise ConnectionError("results stream down")

        mock_redis = MagicMock()
        mock_redis.xadd_capped.side_effect = xadd_capped
        mock_redis.xadd_capped_raw.side_effect = xadd_capped_raw
        abort_event = MagicMock(spec=threading.Event)
        abort_event.wait.side_effect = lambda timeout: False

        outcome = _publish_result_with_retry(
            mock_redis,
            result,
            sample_task,
            logging.getLogger("test"),
            "tasks:claude",
            "1-1",
            abort_event=abort_event,
        )

        assert outcome is ResultPublishOutcome.LOST
        assert outcome.durable is False
        # The diagnostic write happened and is safe, but callers must retain
        # and retry the source because it cannot replay the rotated credential.
        all_payloads = dead_letter_payloads or [
            c.args[1] for c in mock_redis.xadd_capped.call_args_list if "dead" in str(c).lower()
        ]
        assert all_payloads, "expected a dead-letter write after all retries failed"
        for payload in all_payloads:
            assert "super-secret-refresh-token" not in str(payload)
            if "credential_update" in payload:
                assert payload["credential_update"] == "[REDACTED]"

    def test_succeeds_on_first_attempt(self, sample_task):
        """Returns PUBLISHED and calls xadd_capped once on immediate success."""
        mock_redis = MagicMock()
        mock_redis.xadd_capped.return_value = "1-0"
        result = self._make_result(sample_task)

        outcome = _publish_result_with_retry(
            mock_redis, result, sample_task, logging.getLogger("test"), "tasks:claude", "1-1"
        )

        assert outcome is ResultPublishOutcome.PUBLISHED
        mock_redis.xadd_capped.assert_called_once()
        stream, published = mock_redis.xadd_capped.call_args.args
        assert stream == RESULTS_STREAM
        assert published.items() >= result.to_dict().items()
        assert mock_redis.xadd_capped.call_args.kwargs == {"maxlen": _STREAM_MAXLEN}

    def test_successful_publish_backfills_missing_repo(self, sample_task):
        """Results published by the helper carry task.repo even for partial callers."""
        mock_redis = MagicMock()
        mock_redis.xadd_capped.return_value = "1-0"
        result = self._make_result(sample_task)
        result.repo = ""

        outcome = _publish_result_with_retry(
            mock_redis, result, sample_task, logging.getLogger("test"), "tasks:claude", "1-1"
        )

        assert outcome is ResultPublishOutcome.PUBLISHED
        published = mock_redis.xadd_capped.call_args[0][1]
        assert published["repo"] == sample_task.repo

    def test_credential_update_gets_shared_monotonic_version(self, sample_task):
        mock_redis = MagicMock()
        mock_redis.client.xpending_range.return_value = []
        mock_redis.next_monotonic_version.return_value = 1_800_000_000_000_001.0
        mock_redis.xadd_capped.side_effect = [ConnectionError("blip"), "1-0"]
        result = self._make_result(sample_task)
        result.credential_update = '{"refresh_token":"rotated"}'
        abort = MagicMock(spec=threading.Event)
        abort.wait.return_value = False

        outcome = _publish_result_with_retry(
            mock_redis,
            result,
            sample_task,
            logging.getLogger("test"),
            "tasks:claude",
            "1-1",
            abort_event=abort,
        )

        assert outcome is ResultPublishOutcome.PUBLISHED
        mock_redis.next_monotonic_version.assert_called_once()
        published = mock_redis.xadd_capped_raw.call_args.args[1]
        assert float(published["credential_update_minted_at"]) == 1_800_000_000_000_001.0
        checkpoint_key = _credential_checkpoint_key(
            RESULTS_STREAM,
            "tasks:claude",
            "1-1",
            sample_task.id,
        )
        checkpoint_set_index = next(
            index
            for index, call in enumerate(mock_redis.client.method_calls)
            if call[0] == "set" and call.args[0] == checkpoint_key
        )
        assert checkpoint_set_index >= 0
        assert any(
            call[0] == "delete" and call.args[0] == checkpoint_key
            for call in mock_redis.client.method_calls
        )
        mock_redis.xack_raw.assert_called_once_with("tasks:claude", CONSUMER_GROUP, "1-1")

    def test_retries_and_succeeds_on_second_attempt(self, sample_task):
        """Returns PUBLISHED when the first attempt fails and the second succeeds."""
        call_count = [0]

        def xadd_capped(stream, data, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1 and stream == RESULTS_STREAM:
                raise ConnectionError("blip")
            return "1-0"

        mock_redis = MagicMock()
        mock_redis.xadd_capped.side_effect = xadd_capped
        waited: list[float] = []
        abort_event = MagicMock(spec=threading.Event)
        abort_event.wait.side_effect = lambda timeout: waited.append(timeout) or False
        result = self._make_result(sample_task)

        outcome = _publish_result_with_retry(
            mock_redis,
            result,
            sample_task,
            logging.getLogger("test"),
            "tasks:claude",
            "1-1",
            abort_event=abort_event,
        )

        assert outcome is ResultPublishOutcome.PUBLISHED
        assert call_count[0] == 2
        # Should have waited once before the second attempt
        assert waited == [_RESULT_PUBLISH_BACKOFF[0]]

    def test_retries_and_succeeds_on_third_attempt(self, sample_task):
        """Returns PUBLISHED when the first two attempts fail and the third succeeds."""
        call_count = [0]

        def xadd_capped(stream, data, **kwargs):
            call_count[0] += 1
            if call_count[0] < 3 and stream == RESULTS_STREAM:
                raise ConnectionError("blip")
            return "1-0"

        mock_redis = MagicMock()
        mock_redis.xadd_capped.side_effect = xadd_capped
        waited: list[float] = []
        abort_event = MagicMock(spec=threading.Event)
        abort_event.wait.side_effect = lambda timeout: waited.append(timeout) or False
        result = self._make_result(sample_task)

        outcome = _publish_result_with_retry(
            mock_redis,
            result,
            sample_task,
            logging.getLogger("test"),
            "tasks:claude",
            "1-1",
            abort_event=abort_event,
        )

        assert outcome is ResultPublishOutcome.PUBLISHED
        assert call_count[0] == 3
        assert waited == [_RESULT_PUBLISH_BACKOFF[0], _RESULT_PUBLISH_BACKOFF[1]]

    def test_all_retries_fail_writes_dead_letter(self, sample_task):
        """Returns DEAD_LETTERED when the fallback recovery write succeeds."""
        mock_redis = MagicMock()

        def xadd_capped(stream, data, **kwargs):
            if stream == RESULTS_STREAM:
                raise ConnectionError("Redis down")
            return "1-0"

        mock_redis.xadd_capped.side_effect = xadd_capped
        abort_event = MagicMock(spec=threading.Event)
        abort_event.wait.return_value = False  # not aborted; simulate normal timeout
        result = self._make_result(sample_task)

        outcome = _publish_result_with_retry(
            mock_redis,
            result,
            sample_task,
            logging.getLogger("test"),
            "tasks:claude",
            "entry-42",
            abort_event=abort_event,
        )

        assert outcome is ResultPublishOutcome.DEAD_LETTERED
        assert outcome.durable is True
        # Should have attempted RESULTS_STREAM exactly _RESULT_PUBLISH_RETRIES times
        results_calls = [
            c for c in mock_redis.xadd_capped.call_args_list if c[0][0] == RESULTS_STREAM
        ]
        assert len(results_calls) == _RESULT_PUBLISH_RETRIES
        # Dead-letter stream must have been written exactly once
        dl_calls = [
            c for c in mock_redis.xadd_capped.call_args_list if c[0][0] == DEAD_LETTER_STREAM
        ]
        assert len(dl_calls) == 1
        dl_fields = dl_calls[0][0][1]
        assert dl_fields["task_id"] == sample_task.id
        assert "dead_letter_reason" in dl_fields
        assert dl_fields["tasks_stream"] == "tasks:claude"
        assert dl_fields["original_entry_id"] == "entry-42"

        # Explicit redaction assertion at the result-DL write site (code quality review)
        assert dl_fields.get("token") == "[REDACTED]", (
            "github token must be redacted even in result DL path"
        )
        assert "test-token-loop" not in str(dl_fields)

    def test_all_retries_fail_dead_letter_also_fails_returns_lost(self, sample_task, caplog):
        """Returns LOST when both the primary and dead-letter writes fail."""
        mock_redis = MagicMock()
        mock_redis.xadd_capped.side_effect = ConnectionError("Redis down")
        abort_event = MagicMock(spec=threading.Event)
        abort_event.wait.return_value = False  # not aborted; simulate normal timeout
        result = self._make_result(sample_task)

        with caplog.at_level(logging.ERROR):
            outcome = _publish_result_with_retry(
                mock_redis,
                result,
                sample_task,
                logging.getLogger("test"),
                "tasks:claude",
                "1-1",
                abort_event=abort_event,
            )

        assert outcome is ResultPublishOutcome.LOST
        assert outcome.durable is False
        assert any("permanently lost" in r.message for r in caplog.records)

    def test_abort_during_backoff_returns_aborted_immediately(self, sample_task):
        """Returns ABORTED immediately when abort_event fires during backoff."""
        call_count = [0]

        def xadd_capped(stream, data, **kwargs):
            call_count[0] += 1
            raise ConnectionError("Redis down")

        mock_redis = MagicMock()
        mock_redis.xadd_capped.side_effect = xadd_capped
        abort_event = MagicMock(spec=threading.Event)
        # First attempt fails; abort fires during backoff before second attempt
        abort_event.wait.return_value = True
        result = self._make_result(sample_task)

        outcome = _publish_result_with_retry(
            mock_redis,
            result,
            sample_task,
            logging.getLogger("test"),
            "tasks:claude",
            "1-1",
            abort_event=abort_event,
        )

        assert outcome is ResultPublishOutcome.ABORTED
        assert outcome.durable is False
        # Only one attempt was made before abort short-circuited the loop
        assert call_count[0] == 1
        abort_event.wait.assert_called_once_with(timeout=_RESULT_PUBLISH_BACKOFF[0])


# ---------------------------------------------------------------------------
# Tests for _check_gh_credentials
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestCheckGhCredentials:
    """Unit tests for the startup OAuth-token detector."""

    def _make_hosts_yml(self, tmp_path: Path, token: str) -> Path:
        hosts_file = tmp_path / ".config" / "gh" / "hosts.yml"
        hosts_file.parent.mkdir(parents=True)
        hosts_file.write_text(f"github.com:\n  oauth_token: {token}\n  user: testuser\n")
        return hosts_file

    def _run(self, tmp_path: Path, logger: logging.Logger) -> None:
        """Call _check_gh_credentials with Path.home() pointing to tmp_path."""
        with patch("pathlib.Path.home", return_value=tmp_path):
            _check_gh_credentials(logger)

    def test_no_warning_for_classic_pat(self, tmp_path, caplog, monkeypatch):
        """Classic PAT (ghp_) must not trigger a warning."""
        monkeypatch.delenv("GH_TOKEN", raising=False)
        monkeypatch.delenv("GITHUB_TOKEN", raising=False)
        self._make_hosts_yml(tmp_path, "ghp_abc123")
        logger = logging.getLogger("test.creds")
        with caplog.at_level(logging.WARNING, logger="test.creds"):
            self._run(tmp_path, logger)
        assert not caplog.records, f"Unexpected warning: {caplog.text}"

    def test_no_warning_for_fine_grained_pat(self, tmp_path, caplog, monkeypatch):
        """Fine-grained PAT (github_pat_) must not trigger a warning."""
        monkeypatch.delenv("GH_TOKEN", raising=False)
        monkeypatch.delenv("GITHUB_TOKEN", raising=False)
        self._make_hosts_yml(tmp_path, "github_pat_abc123")
        logger = logging.getLogger("test.creds")
        with caplog.at_level(logging.WARNING, logger="test.creds"):
            self._run(tmp_path, logger)
        assert not caplog.records

    def test_warning_for_oauth_token_gho(self, tmp_path, caplog, monkeypatch):
        """OAuth app token (gho_) must trigger a warning."""
        monkeypatch.delenv("GH_TOKEN", raising=False)
        monkeypatch.delenv("GITHUB_TOKEN", raising=False)
        self._make_hosts_yml(tmp_path, "gho_abc123")
        logger = logging.getLogger("test.creds")
        with caplog.at_level(logging.WARNING, logger="test.creds"):
            self._run(tmp_path, logger)
        assert any("OAuth" in r.message for r in caplog.records)

    def test_warning_for_oauth_token_ghu(self, tmp_path, caplog, monkeypatch):
        """User-to-server OAuth token (ghu_) must trigger a warning."""
        monkeypatch.delenv("GH_TOKEN", raising=False)
        monkeypatch.delenv("GITHUB_TOKEN", raising=False)
        self._make_hosts_yml(tmp_path, "ghu_xyz789")
        logger = logging.getLogger("test.creds")
        with caplog.at_level(logging.WARNING, logger="test.creds"):
            self._run(tmp_path, logger)
        assert any("OAuth" in r.message for r in caplog.records)

    def test_no_warning_when_gh_token_env_set(self, tmp_path, caplog, monkeypatch):
        """When GH_TOKEN env var is set, skip the file check entirely."""
        monkeypatch.setenv("GH_TOKEN", "ghp_env_token")
        # Even if hosts.yml has an OAuth token, no warning should fire.
        self._make_hosts_yml(tmp_path, "gho_should_be_ignored")
        logger = logging.getLogger("test.creds")
        with caplog.at_level(logging.WARNING, logger="test.creds"):
            self._run(tmp_path, logger)
        assert not caplog.records

    def test_no_warning_when_github_token_env_set(self, tmp_path, caplog, monkeypatch):
        """When GITHUB_TOKEN env var is set, skip the file check entirely."""
        monkeypatch.setenv("GITHUB_TOKEN", "ghp_env_token")
        self._make_hosts_yml(tmp_path, "gho_should_be_ignored")
        logger = logging.getLogger("test.creds")
        with caplog.at_level(logging.WARNING, logger="test.creds"):
            self._run(tmp_path, logger)
        assert not caplog.records

    def test_no_warning_when_hosts_file_missing(self, tmp_path, caplog, monkeypatch):
        """If hosts.yml does not exist, no warning should be emitted."""
        monkeypatch.delenv("GH_TOKEN", raising=False)
        monkeypatch.delenv("GITHUB_TOKEN", raising=False)
        logger = logging.getLogger("test.creds")
        with caplog.at_level(logging.WARNING, logger="test.creds"):
            self._run(tmp_path, logger)
        assert not caplog.records

    def test_corrupt_hosts_yml_logs_warning(self, tmp_path, caplog, monkeypatch):
        """When hosts.yml contains invalid YAML, a warning is logged but no crash."""
        monkeypatch.delenv("GH_TOKEN", raising=False)
        monkeypatch.delenv("GITHUB_TOKEN", raising=False)
        hosts_file = tmp_path / ".config" / "gh" / "hosts.yml"
        hosts_file.parent.mkdir(parents=True)
        hosts_file.write_text("[invalid yaml {{{{")
        logger = logging.getLogger("test.creds")
        with caplog.at_level(logging.WARNING, logger="test.creds"):
            self._run(tmp_path, logger)
        assert any("Could not read" in r.message for r in caplog.records)

    def test_non_dict_hosts_yml_no_crash(self, tmp_path, caplog, monkeypatch):
        """When hosts.yml parses to a non-dict (e.g. a list), no crash occurs."""
        monkeypatch.delenv("GH_TOKEN", raising=False)
        monkeypatch.delenv("GITHUB_TOKEN", raising=False)
        hosts_file = tmp_path / ".config" / "gh" / "hosts.yml"
        hosts_file.parent.mkdir(parents=True)
        hosts_file.write_text("- item1\n- item2\n")
        logger = logging.getLogger("test.creds")
        with caplog.at_level(logging.WARNING, logger="test.creds"):
            self._run(tmp_path, logger)
        # Should not crash, and no OAuth warning should be emitted
        assert not any("OAuth" in r.message for r in caplog.records)

    def test_non_dict_host_entry_skipped(self, tmp_path, caplog, monkeypatch):
        """When a host entry is a non-dict (e.g. a string), it is skipped."""
        monkeypatch.delenv("GH_TOKEN", raising=False)
        monkeypatch.delenv("GITHUB_TOKEN", raising=False)
        hosts_file = tmp_path / ".config" / "gh" / "hosts.yml"
        hosts_file.parent.mkdir(parents=True)
        hosts_file.write_text("github.com: just-a-string\n")
        logger = logging.getLogger("test.creds")
        with caplog.at_level(logging.WARNING, logger="test.creds"):
            self._run(tmp_path, logger)
        assert not any("OAuth" in r.message for r in caplog.records)

    def test_non_string_token_skipped(self, tmp_path, caplog, monkeypatch):
        """When oauth_token is not a string (e.g. an integer or null), it is skipped."""
        monkeypatch.delenv("GH_TOKEN", raising=False)
        monkeypatch.delenv("GITHUB_TOKEN", raising=False)
        hosts_file = tmp_path / ".config" / "gh" / "hosts.yml"
        hosts_file.parent.mkdir(parents=True)
        hosts_file.write_text("github.com:\n  oauth_token: 12345\n  user: testuser\n")
        logger = logging.getLogger("test.creds")
        with caplog.at_level(logging.WARNING, logger="test.creds"):
            self._run(tmp_path, logger)
        assert not any("OAuth" in r.message for r in caplog.records)

    def test_null_token_skipped(self, tmp_path, caplog, monkeypatch):
        """When oauth_token is null, it is skipped without error."""
        monkeypatch.delenv("GH_TOKEN", raising=False)
        monkeypatch.delenv("GITHUB_TOKEN", raising=False)
        hosts_file = tmp_path / ".config" / "gh" / "hosts.yml"
        hosts_file.parent.mkdir(parents=True)
        hosts_file.write_text("github.com:\n  oauth_token: null\n  user: testuser\n")
        logger = logging.getLogger("test.creds")
        with caplog.at_level(logging.WARNING, logger="test.creds"):
            self._run(tmp_path, logger)
        assert not any("OAuth" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# Tests for multi-project stream routing
# ---------------------------------------------------------------------------


def test_drain_pending_tasks_preserves_snapshot_metadata(local_worker_config):
    task = Task.create(
        task_type=TaskType.FIX_CI,
        repo="owner/repo",
        token="tok",
        resource_type="pr",
        resource_id=42,
        prompt="fix it",
        branch="feature",
        snapshot_head_sha="sha-old",
        decision_reason="ci_failure",
        snapshot_failed_checks=["tests"],
    )
    mock_redis = MagicMock()
    mock_redis.xreadgroup_multi.side_effect = [
        [("tasks", "1-0", task.to_dict())],
        [],
    ]
    # Pending marker still points at this task so the dedup guard (M4-conc)
    # lets the recovery result through (genuine restart, not a replayed dupe).
    from orcest.shared.coordination import PendingTaskMetadata

    marker = PendingTaskMetadata(task_id=task.id).to_json()
    mock_redis.get.return_value = marker
    mock_redis.get_raw.return_value = marker

    _drain_pending_tasks_raw(
        mock_redis,
        "tasks",
        local_worker_config,
        logging.getLogger("test"),
    )

    fields = mock_redis.xadd_capped.call_args.args[1]
    result = TaskResult.from_dict(fields)
    assert result.snapshot_head_sha == "sha-old"
    assert result.decision_reason == "ci_failure"
    assert result.snapshot_failed_checks == ["tests"]


@pytest.mark.unit
def test_drain_missing_marker_publishes_recovery_result(local_worker_config):
    """A missing marker alone cannot prove a PEL task completed successfully."""
    task = Task.create(
        task_type=TaskType.IMPLEMENT_ISSUE,
        repo="owner/repo",
        token="tok",
        resource_type="issue",
        resource_id=7,
        prompt="implement it",
        branch="impl-7",
    )
    mock_redis = MagicMock()
    # One pending entry on the drain pass, then empty.
    mock_redis.xreadgroup_multi.side_effect = [
        [("tasks:issue:claude", "1-0", task.to_dict())],
        [],
    ]
    # Pending marker absent could mean expiry/loss while the PEL still owns work.
    mock_redis.get.return_value = None
    mock_redis.get_raw.return_value = None
    mock_redis.xadd_capped.return_value = "1-0"
    mock_redis.xack_raw.return_value = 1

    _drain_pending_tasks_raw(
        mock_redis,
        "tasks:issue:claude",
        local_worker_config,
        logging.getLogger("test"),
    )

    # Recovery result is the commit point before the PEL entry is ACKed.
    results_calls = [c for c in mock_redis.xadd_capped.call_args_list if c[0][0] == RESULTS_STREAM]
    assert len(results_calls) == 1
    assert TaskResult.from_dict(results_calls[0][0][1]).task_id == task.id
    mock_redis.xack_raw.assert_any_call("tasks:issue:claude", CONSUMER_GROUP, "1-0")


@pytest.mark.unit
def test_drain_ack_failure_stops_without_cleanup_or_duplicate_recovery(mocker, local_worker_config):
    """Restart recovery remains idempotent when its first XACK fails."""
    task = Task.create(
        task_type=TaskType.FIX_PR,
        repo="owner/repo",
        token="tok",
        resource_type="pr",
        resource_id=42,
        prompt="fix it",
        snapshot_head_sha="sha",
    )
    mock_redis = MagicMock()
    mock_redis.xreadgroup_multi.return_value = [("tasks:claude", "1-0", task.to_dict())]
    mock_redis.get.return_value = task.id
    mock_redis.xadd_capped.return_value = "2-0"
    mock_redis.xack_raw.side_effect = ConnectionError("ack unavailable")
    cleanup = mocker.patch("orcest.worker.loop._cleanup_coordination_once")

    complete, drained = _drain_pending_tasks_raw(
        mock_redis, "tasks:claude", local_worker_config, logging.getLogger("test")
    )

    assert (complete, drained) == (False, 1)
    assert mock_redis.xadd_capped.call_count == 1
    cleanup.assert_not_called()

    # On restart the retained result proves publication already committed, so
    # only XACK is retried and no second recovery result is appended.
    published_fields = mock_redis.xadd_capped.call_args.args[1]
    marker_key, marker_value = mock_redis.client.set.call_args.args[:2]
    mock_redis.reset_mock()
    mock_redis.xreadgroup_multi.side_effect = [
        [("tasks:claude", "1-0", task.to_dict())],
        [],
    ]
    mock_redis.get.return_value = task.id
    mock_redis.client.get.side_effect = lambda key: marker_value if key == marker_key else None
    mock_redis.client.xrange.return_value = [(b"2-0", published_fields)]
    mock_redis.xack_raw.side_effect = None
    mock_redis.xack_raw.return_value = 1
    complete, drained = _drain_pending_tasks_raw(
        mock_redis, "tasks:claude", local_worker_config, logging.getLogger("test")
    )
    assert (complete, drained) == (True, 1)
    mock_redis.xadd_capped.assert_not_called()
    mock_redis.client.xrange.assert_called()


@pytest.mark.unit
def test_version_failure_restart_republishes_exact_private_credential_checkpoint(
    mocker, local_worker_config, sample_task, caplog
):
    """An unversioned secret survives restart, is durably versioned, and republishes."""
    tasks_stream = "tasks:claude"
    entry_id = "1-0"
    rotated_secret = '{"refresh_token":"rotated-secret"}'
    minted_at = 1_800_000_000_000_123.0
    result = _task_result(
        sample_task,
        local_worker_config,
        ResultStatus.COMPLETED,
        sample_task.branch,
        "done",
        1,
        credential_update=rotated_secret,
        credential_update_minted_at=0.0,
    )
    mock_redis = MagicMock()
    private_store: dict[str, str] = {}
    expirations: dict[str, int] = {}

    def store_value(key, value, *, ex=None, **kwargs):
        private_store[key] = value
        if ex is not None:
            expirations[key] = ex
        return True

    def delete_value(key):
        private_store.pop(key, None)
        return 1

    mock_redis.client.set.side_effect = store_value
    mock_redis.client.get.side_effect = private_store.get
    mock_redis.client.delete.side_effect = delete_value

    def diagnostic_succeeds(stream, data, **kwargs):
        assert stream == DEAD_LETTER_STREAM
        return "9-0"

    mock_redis.xadd_capped.side_effect = diagnostic_succeeds
    mock_redis.next_monotonic_version.side_effect = ConnectionError("version unavailable")
    abort = MagicMock(spec=threading.Event)
    abort.wait.return_value = False
    logger = logging.getLogger("test.credential-restart")

    with caplog.at_level(logging.WARNING):
        outcome = _publish_result_with_retry(
            mock_redis,
            result,
            sample_task,
            logger,
            tasks_stream,
            entry_id,
            abort_event=abort,
        )

    assert outcome is ResultPublishOutcome.LOST
    checkpoint_key = _credential_checkpoint_key(
        RESULTS_STREAM,
        tasks_stream,
        entry_id,
        sample_task.id,
    )
    diagnostic_marker_key = _handoff_marker_key(
        DEAD_LETTER_STREAM,
        tasks_stream,
        entry_id,
        f"{_CREDENTIAL_DIAGNOSTIC_HANDOFF_PREFIX}{sample_task.id}",
    )
    assert checkpoint_key in private_store
    assert diagnostic_marker_key in private_store
    # The checkpoint survives the restart, but on a bounded backstop rather
    # than forever: it holds a plaintext rotated OAuth blob, and a checkpoint
    # whose PEL entry is discarded (e.g. by XGROUP DELCONSUMER) can never reach
    # the terminal path that deletes it. The TTL is far longer than any task.
    assert expirations[checkpoint_key] == CREDENTIAL_CHECKPOINT_TTL_SECONDS
    assert expirations[diagnostic_marker_key] == _HANDOFF_MARKER_TTL_SECONDS
    public_dlq = next(
        call.args[1]
        for call in mock_redis.xadd_capped.call_args_list
        if call.args[0] == DEAD_LETTER_STREAM
    )
    assert public_dlq["credential_update"] == "[REDACTED]"
    assert rotated_secret not in str(public_dlq)
    assert all(rotated_secret not in record.getMessage() for record in caplog.records)

    # Simulate time beyond the diagnostic marker TTL. The private checkpoint
    # remains persistent while the source is still pending.
    private_store.pop(diagnostic_marker_key)

    mock_redis.reset_mock()
    mock_redis.xreadgroup_multi.side_effect = [
        [(tasks_stream, entry_id, sample_task.to_dict())],
        [],
    ]
    mock_redis.xadd_capped.side_effect = None
    mock_redis.xadd_capped.return_value = "10-0"
    mock_redis.xadd_capped_raw.return_value = "10-0"
    mock_redis.next_monotonic_version.side_effect = None
    mock_redis.next_monotonic_version.return_value = minted_at
    mock_redis.xack_raw.return_value = 1
    mock_redis.client.xpending_range.return_value = []
    cleanup = mocker.patch("orcest.worker.loop._cleanup_coordination_once")

    complete, drained = _drain_pending_tasks_raw(
        mock_redis,
        tasks_stream,
        local_worker_config,
        logger,
    )

    assert (complete, drained) == (True, 1)
    recovery_calls = [
        call for call in mock_redis.xadd_capped_raw.call_args_list if call.args[0] == RESULTS_STREAM
    ]
    assert len(recovery_calls) == 1
    recovered_fields = recovery_calls[0].args[1]
    recovered = TaskResult.from_dict(recovered_fields)
    assert recovered.status is ResultStatus.COMPLETED
    assert recovered.credential_update == rotated_secret
    assert recovered.credential_update_minted_at == minted_at
    mock_redis.xack_raw.assert_called_once_with(tasks_stream, CONSUMER_GROUP, entry_id)
    cleanup.assert_not_called()
    assert checkpoint_key not in private_store
    assert diagnostic_marker_key not in private_store
    assert all(rotated_secret not in record.getMessage() for record in caplog.records)


@pytest.mark.unit
def test_primary_credential_result_survives_xack_failure_and_restart(
    mocker, local_worker_config, sample_task
):
    """A durable primary never loses its checkpoint before confirmed source XACK."""
    tasks_stream = "tasks:claude"
    entry_id = "1-0"
    result = _task_result(
        sample_task,
        local_worker_config,
        ResultStatus.COMPLETED,
        sample_task.branch,
        "done",
        1,
        credential_update='{"refresh_token":"rotated-secret"}',
        credential_update_minted_at=123.0,
    )
    mock_redis = MagicMock()
    private_store: dict[str, str] = {}

    def store_value(key, value, **kwargs):
        private_store[key] = value
        return True

    def delete_value(key):
        private_store.pop(key, None)
        return 1

    mock_redis.client.set.side_effect = store_value
    mock_redis.client.get.side_effect = private_store.get
    mock_redis.client.delete.side_effect = delete_value
    mock_redis.xadd_capped_raw.return_value = "2-0"
    abort = threading.Event()

    def fail_ack(*args, **kwargs):
        abort.set()
        raise ConnectionError("ack unavailable")

    mock_redis.xack_raw.side_effect = fail_ack
    handoff = _handoff_result_until_terminal(
        mock_redis,
        result,
        sample_task,
        logging.getLogger("test.credential-ack"),
        tasks_stream,
        entry_id,
        abort_event=abort,
    )

    checkpoint_key = _credential_checkpoint_key(
        RESULTS_STREAM,
        tasks_stream,
        entry_id,
        sample_task.id,
    )
    assert handoff.publish_outcome is ResultPublishOutcome.ABORTED
    assert handoff.source_acked is False
    assert checkpoint_key in private_store
    terminal_marker_key = _handoff_marker_key(
        RESULTS_STREAM, tasks_stream, entry_id, sample_task.id
    )
    marker_set = next(
        call for call in mock_redis.client.set.call_args_list if call.args[0] == terminal_marker_key
    )
    assert marker_set.kwargs == {}, "incomplete credential receipts must not expire"
    first_result_fields = mock_redis.xadd_capped_raw.call_args.args[1]

    mock_redis.reset_mock()
    mock_redis.xreadgroup_multi.side_effect = [
        [(tasks_stream, entry_id, sample_task.to_dict())],
        [],
    ]
    mock_redis.xack_raw.side_effect = None
    mock_redis.client.xpending_range.return_value = []
    cleanup = mocker.patch("orcest.worker.loop._cleanup_coordination_once")

    mock_redis.client.xrange.return_value = [("2-0", first_result_fields)]

    complete, drained = _drain_pending_tasks_raw(
        mock_redis,
        tasks_stream,
        local_worker_config,
        logging.getLogger("test.credential-ack"),
    )

    assert (complete, drained) == (True, 1)
    mock_redis.xadd_capped_raw.assert_not_called()
    cleanup.assert_not_called()
    assert checkpoint_key not in private_store


@pytest.mark.unit
def test_live_v2_checkpoint_missing_intent_blocks_without_public_churn(
    sample_task,
):
    """Blocked private state never retries, publishes diagnostics, or ACKs."""
    from orcest.shared.credential_handoff import (
        credential_intent_key,
        store_credential_checkpoint,
    )

    tasks_stream = "tasks:claude"
    entry_id = "1-0"
    result = TaskResult(
        task_id=sample_task.id,
        worker_id="worker",
        status=ResultStatus.COMPLETED,
        branch=sample_task.branch,
        summary="done",
        duration_seconds=1,
        resource_type=sample_task.resource_type,
        resource_id=sample_task.resource_id,
        repo=sample_task.repo,
        credential_update='{"refresh_token":"rotated-secret"}',
        credential_update_minted_at=123,
    )
    mock_redis = MagicMock()
    private_store: dict[str, str] = {}

    def get_value(key):
        return private_store.get(key)

    def set_value(key, value, *, nx=False, **_kwargs):
        if nx and key in private_store:
            return None
        private_store[key] = value
        return True

    mock_redis.client.get.side_effect = get_value
    mock_redis.client.set.side_effect = set_value
    checkpoint = store_credential_checkpoint(
        mock_redis,
        RESULTS_STREAM,
        tasks_stream,
        entry_id,
        sample_task.id,
        result.to_dict(),
    )
    private_store.pop(credential_intent_key(RESULTS_STREAM, tasks_stream, entry_id, sample_task.id))
    mock_redis.reset_mock()
    mock_redis.client.get.side_effect = get_value
    mock_redis.client.set.side_effect = set_value

    outcome = _publish_result_with_retry(
        mock_redis,
        result,
        sample_task,
        logging.getLogger("test.credential-missing-intent"),
        tasks_stream,
        entry_id,
    )

    assert outcome is ResultPublishOutcome.BLOCKED
    assert private_store[checkpoint.key] == checkpoint.serialized
    mock_redis.xadd_capped.assert_not_called()
    mock_redis.xadd_capped_raw.assert_not_called()
    mock_redis.xack_raw.assert_not_called()
    mock_redis.client.eval.assert_not_called()


@pytest.mark.unit
@pytest.mark.parametrize("checkpoint_state", ["missing", "corrupt"])
def test_drain_fails_closed_on_persistent_intent_after_diagnostic_expiry(
    mocker, local_worker_config, sample_task, checkpoint_state
):
    """Intent keeps missing/corrupt private state blocked after markers expire."""
    from orcest.shared.credential_handoff import credential_intent_key

    tasks_stream = "tasks:claude"
    entry_id = "1-0"
    checkpoint_key = _credential_checkpoint_key(
        RESULTS_STREAM,
        tasks_stream,
        entry_id,
        sample_task.id,
    )
    intent_key = credential_intent_key(
        RESULTS_STREAM,
        tasks_stream,
        entry_id,
        sample_task.id,
    )
    # The public diagnostic and bounded handoff marker have both expired.
    private_store = {intent_key: "1"}
    if checkpoint_state == "corrupt":
        private_store[checkpoint_key] = "not-json"

    mock_redis = MagicMock()
    mock_redis.client.get.side_effect = private_store.get
    mock_redis.xreadgroup_multi.return_value = [(tasks_stream, entry_id, sample_task.to_dict())]
    cleanup = mocker.patch("orcest.worker.loop._cleanup_coordination_once")

    complete, drained = _drain_pending_tasks_raw(
        mock_redis,
        tasks_stream,
        local_worker_config,
        logging.getLogger("test.credential-fail-closed"),
    )

    assert (complete, drained) == (False, 1)
    mock_redis.xadd_capped.assert_not_called()
    mock_redis.xadd_capped_raw.assert_not_called()
    mock_redis.xack_raw.assert_not_called()
    cleanup.assert_not_called()


@pytest.mark.unit
def test_drain_malformed_entry_dead_letters_redacted_payload(local_worker_config):
    """Malformed startup entries are durable and secret-free before ACK."""
    fields = {
        "id": "bad-task",
        "repo": "owner/repo",
        "resource_type": "issue",
        "resource_id": "7",
        "key_prefix": "",
        "token": "github-secret",
        "claude_token": "claude-secret",
        "credential": "provider-secret",
        "api_key": "unknown-secret",
        "password": "unknown-password",
        "authorization": "Bearer unknown-secret",
    }
    mock_redis = MagicMock()
    mock_redis.xreadgroup_multi.side_effect = [
        [("tasks:issue:claude", "1-0", fields)],
        [],
    ]
    mock_redis.xadd_capped.return_value = "2-0"
    mock_redis.xack_raw.return_value = 1

    complete, drained = _drain_pending_tasks_raw(
        mock_redis,
        "tasks:issue:claude",
        local_worker_config,
        logging.getLogger("test"),
    )

    assert (complete, drained) == (True, 1)
    dlq = mock_redis.xadd_capped.call_args.args[1]
    assert all(dlq[field] == "[REDACTED]" for field in REDACTED_FIELDS)
    assert "api_key" not in dlq
    assert "password" not in dlq
    assert "authorization" not in dlq
    mock_redis.xack_raw.assert_called_once_with("tasks:issue:claude", CONSUMER_GROUP, "1-0")


@pytest.mark.unit
def test_drain_malformed_entry_empty_id_uses_malformed_fallback(local_worker_config):
    """A present-but-empty ``id`` must derive the same ``malformed:<entry>``
    handoff identity the pool-manager reaper uses (``fields.get("id") or
    ...``), so a worker dying between publish and ACK cannot make the reaper
    duplicate the dead-letter row under a different marker key."""
    fields = {
        "id": "",
        "repo": "owner/repo",
        "resource_type": "issue",
        "resource_id": "7",
    }
    mock_redis = MagicMock()
    mock_redis.xreadgroup_multi.side_effect = [
        [("tasks:issue:claude", "1-0", fields)],
        [],
    ]
    mock_redis.xadd_capped.return_value = "2-0"
    mock_redis.xack_raw.return_value = 1

    complete, drained = _drain_pending_tasks_raw(
        mock_redis,
        "tasks:issue:claude",
        local_worker_config,
        logging.getLogger("test"),
    )

    assert (complete, drained) == (True, 1)
    # The reaper (pool_manager) derives the identity with `or`, so an empty
    # id falls back to malformed:<entry_id>. The worker must match exactly.
    expected_key = _handoff_marker_key(
        DEAD_LETTER_STREAM, "tasks:issue:claude", "1-0", "malformed:1-0"
    )
    rejected_key = _handoff_marker_key(DEAD_LETTER_STREAM, "tasks:issue:claude", "1-0", "")
    marker_keys = [c.args[0] for c in mock_redis.client.set.call_args_list]
    assert expected_key in marker_keys
    assert rejected_key not in marker_keys


@pytest.mark.unit
def test_drain_superseded_duplicate_does_not_clear_newer_issue_reservation(local_worker_config):
    """Case B for M4-conc: a drained stale duplicate whose pending marker points
    at a NEWER in-flight task must NOT wipe the newer task's issue attempts
    counter.

    A stale cleanup on the suppressed path would clobber the reservation of
    the newer task the marker now points at.

    Here a stale duplicate (older task id) is drained while the pending marker
    points at a NEWER task. The drain must suppress the recovery result, ACK the
    orphaned PEL entry, clear the (CAS-guarded, so safely no-op) pending marker,
    and leave the newer task's attempts counter untouched — i.e. it must NOT
    issue any delete against the issue attempts key.
    """
    from orcest.shared.coordination import PendingTaskMetadata

    stale_task = Task.create(
        task_type=TaskType.IMPLEMENT_ISSUE,
        repo="owner/repo",
        token="tok",
        resource_type="issue",
        resource_id=7,
        prompt="implement it",
        branch="impl-7",
    )
    newer_task_id = "newer-task-id-9999"
    assert newer_task_id != stale_task.id

    mock_redis = MagicMock()
    # One stale-duplicate entry on the drain pass, then empty.
    mock_redis.xreadgroup_multi.side_effect = [
        [("tasks:issue:claude", "1-0", stale_task.to_dict())],
        [],
    ]
    # Pending marker points at the NEWER task => original result was already
    # processed and the orchestrator re-enqueued newer work.
    newer_marker = PendingTaskMetadata(task_id=newer_task_id).to_json()
    mock_redis.get.return_value = newer_marker
    mock_redis.get_raw.return_value = newer_marker
    mock_redis.xadd_capped.return_value = "1-0"
    mock_redis.xack_raw.return_value = 1

    _drain_pending_tasks_raw(
        mock_redis,
        "tasks:issue:claude",
        local_worker_config,
        logging.getLogger("test"),
    )

    # No recovery result published (the marker no longer matches the stale task).
    results_calls = [c for c in mock_redis.xadd_capped.call_args_list if c[0][0] == RESULTS_STREAM]
    assert results_calls == [], "drain published a recovery result for a superseded stale duplicate"
    # The orphaned PEL entry must still be ACKed so it is not redelivered.
    mock_redis.xack_raw.assert_any_call("tasks:issue:claude", CONSUMER_GROUP, "1-0")
    # CRITICAL: the suppressed stale duplicate must NOT delete the NEWER task's
    # issue attempts counter. For a no-prefix issue task the unconditional clear
    # would call redis.delete("issue:owner/repo:7:attempts").
    attempts_key = "issue:owner/repo:7:attempts"
    delete_calls = [c.args for c in mock_redis.delete.call_args_list]
    delete_raw_calls = [c.args for c in mock_redis.delete_raw.call_args_list]
    assert (attempts_key,) not in delete_calls, (
        "drain wiped the NEWER in-flight task's issue attempts counter "
        f"(delete calls: {delete_calls})"
    )
    assert all(attempts_key not in args for args in delete_raw_calls), (
        "drain wiped the NEWER in-flight task's issue attempts counter via "
        f"delete_raw (calls: {delete_raw_calls})"
    )


@pytest.mark.unit
def test_drain_publishes_recovery_result_when_marker_still_matches(local_worker_config):
    """Companion to the dedup test: when the pending marker STILL points at the
    drained task (genuine restart mid-execution, original result never sent),
    the drain path MUST still publish the [transient] FAILED recovery result so
    the orchestrator can unblock the resource. Pins that the guard does not
    over-suppress.
    """
    from orcest.shared.coordination import PendingTaskMetadata

    task = Task.create(
        task_type=TaskType.IMPLEMENT_ISSUE,
        repo="owner/repo",
        token="tok",
        resource_type="issue",
        resource_id=7,
        prompt="implement it",
        branch="impl-7",
    )
    mock_redis = MagicMock()
    mock_redis.xreadgroup_multi.side_effect = [
        [("tasks:issue:claude", "1-0", task.to_dict())],
        [],
    ]
    # Marker still points at THIS task => original result never reached orchestrator.
    mock_redis.get.return_value = PendingTaskMetadata(task_id=task.id).to_json()
    mock_redis.get_raw.return_value = PendingTaskMetadata(task_id=task.id).to_json()
    mock_redis.xadd_capped.return_value = "1-0"
    mock_redis.xack_raw.return_value = 1

    _drain_pending_tasks_raw(
        mock_redis,
        "tasks:issue:claude",
        local_worker_config,
        logging.getLogger("test"),
    )

    results_calls = [c for c in mock_redis.xadd_capped.call_args_list if c[0][0] == RESULTS_STREAM]
    assert len(results_calls) == 1, "genuine restart recovery result must still be published"
    parsed = TaskResult.from_dict(results_calls[0][0][1])
    assert parsed.status == ResultStatus.FAILED
    assert parsed.task_id == task.id
    assert parsed.summary.startswith("[transient] ")


@pytest.mark.unit
class TestMultiProjectRouting:
    """Tests for multi-project key_prefix routing in result publication."""

    def test_result_published_to_correct_project_stream(self, monkeypatch):
        """When a Task carries a key_prefix, _publish_result_with_retry must
        publish to the project-specific results stream via xadd_capped_raw
        (e.g. 'projectA:results'), NOT the worker's default prefix stream."""
        task = Task.create(
            task_type=TaskType.FIX_PR,
            repo="owner/repo",
            token="tok",
            resource_type="pr",
            resource_id=42,
            prompt="fix it",
            branch="feature",
            key_prefix="projectA",
        )
        result = TaskResult(
            task_id=task.id,
            worker_id="test-worker",
            status=ResultStatus.COMPLETED,
            resource_type=task.resource_type,
            resource_id=task.resource_id,
            branch=task.branch,
            summary="done",
            duration_seconds=1,
        )
        mock_redis = MagicMock()
        mock_redis.xadd_capped_raw.return_value = "1-0"

        outcome = _publish_result_with_retry(
            mock_redis,
            result,
            task,
            logging.getLogger("test"),
            "projectA:tasks:claude",
            "entry-1",
        )

        assert outcome is ResultPublishOutcome.PUBLISHED
        # Must publish to the project-namespaced results stream via raw
        expected_fields = {**result.to_dict(), "repo": task.repo}
        mock_redis.xadd_capped_raw.assert_called_once()
        stream, published = mock_redis.xadd_capped_raw.call_args.args
        assert stream == "projectA:results"
        assert published.items() >= expected_fields.items()
        assert mock_redis.xadd_capped_raw.call_args.kwargs == {"maxlen": _STREAM_MAXLEN}
        # Must NOT use the default-prefix xadd_capped for the results stream
        results_calls = [
            c for c in mock_redis.xadd_capped.call_args_list if c[0][0] == RESULTS_STREAM
        ]
        assert len(results_calls) == 0

    def test_lock_uses_task_key_prefix_when_set(self):
        """When a Task carries a key_prefix, the worker must acquire the lock
        under the task's key_prefix (fully-qualified), not the worker's default
        prefix, so the orchestrator's per-project RedisClient sees it."""
        task = Task.create(
            task_type=TaskType.FIX_PR,
            repo="owner/repo",
            token="tok",
            resource_type="pr",
            resource_id=42,
            prompt="fix it",
            branch="feature",
            key_prefix="projectA",
        )
        mock_redis = MagicMock()
        mock_redis.client.register_script.return_value = MagicMock(return_value=1)
        # Lock acquire succeeds
        mock_redis.client.set.return_value = True
        mock_redis._prefixed = lambda key: f"orcest:{key}"

        from orcest.shared.coordination import RedisLock, make_pr_lock_key

        lock_key = make_pr_lock_key(task.repo, task.resource_id)
        fq_lock_key = f"{task.key_prefix}:{lock_key}"
        lock = RedisLock(mock_redis, fq_lock_key, ttl=LOCK_TTL, owner="w1", raw_key=True)

        assert lock.key == "projectA:lock:pr:owner/repo:42"
        assert lock.acquire() is True
        # Verify the SET call used the fully-qualified key
        set_call = mock_redis.client.set.call_args
        assert set_call[0][0] == "projectA:lock:pr:owner/repo:42"

    def test_lock_uses_default_prefix_when_key_prefix_empty(self):
        """When a Task has an empty key_prefix, the worker must acquire the lock
        using the worker's default auto-prefix (backward compatibility)."""
        task = Task.create(
            task_type=TaskType.FIX_PR,
            repo="owner/repo",
            token="tok",
            resource_type="pr",
            resource_id=42,
            prompt="fix it",
            branch="feature",
            key_prefix="",
        )
        mock_redis = MagicMock()
        mock_redis.client.register_script.return_value = MagicMock(return_value=1)
        mock_redis.client.set.return_value = True
        mock_redis._prefixed = lambda key: f"orcest:{key}"

        from orcest.shared.coordination import RedisLock, make_pr_lock_key

        lock_key = make_pr_lock_key(task.repo, task.resource_id)
        lock = RedisLock(mock_redis, lock_key, ttl=LOCK_TTL, owner="w1")

        # Should use the auto-prefixed key
        assert lock.key == "orcest:lock:pr:owner/repo:42"

    def test_issue_lock_uses_task_key_prefix(self):
        """Issue locks also use the task's key_prefix for correct routing."""
        task = Task.create(
            task_type=TaskType.IMPLEMENT_ISSUE,
            repo="owner/repo",
            token="tok",
            resource_type="issue",
            resource_id=7,
            prompt="implement it",
            key_prefix="projectB",
        )
        mock_redis = MagicMock()
        mock_redis.client.register_script.return_value = MagicMock(return_value=1)
        mock_redis.client.set.return_value = True
        mock_redis._prefixed = lambda key: f"orcest:{key}"

        from orcest.shared.coordination import RedisLock, make_issue_lock_key

        lock_key = make_issue_lock_key(task.repo, task.resource_id)
        fq_lock_key = f"{task.key_prefix}:{lock_key}"
        lock = RedisLock(mock_redis, fq_lock_key, ttl=LOCK_TTL, owner="w1", raw_key=True)

        assert lock.key == "projectB:lock:issue:owner/repo:7"


# ---------------------------------------------------------------------------
# Task 6: early graceful reject for unsupported providers (old image + new provider)
# ---------------------------------------------------------------------------


def test_early_reject_unsupported_provider_publishes_clean_failed(local_worker_config, sample_task):
    """Directly exercising the reject helper: produces non-transient FAILED
    whose summary contains the required 'rebake worker image' guidance,
    publishes the result and acks the entry. The pending marker remains for
    the orchestrator's result consumer to clear after processing the result.
    The real path (after from_dict in receive loop) guarantees we never
    acquire locks or invoke the runner for unknown providers.
    """
    from orcest.worker.loop import _early_reject_unsupported_provider

    mock_redis = MagicMock()
    # _publish_result_with_retry will call xadd_capped (or _raw for prefixed)
    mock_redis.xadd_capped.return_value = "1-0"
    mock_redis.xadd_capped_raw.return_value = "1-0"
    mock_redis._prefixed.side_effect = lambda key: key
    pipe = MagicMock()
    mock_redis.client.pipeline.return_value = pipe
    pipe.get.return_value = None
    pipe.hget.return_value = sample_task.snapshot_head_sha
    logger = logging.getLogger("test.reject")

    # Exercise
    _early_reject_unsupported_provider(
        sample_task,
        "grok",
        local_worker_config,
        mock_redis,
        logger,
        "tasks:claude",  # current_stream (fq or not doesn't matter for mock)
        "0-0",
    )

    # Result must have been published to results stream with FAILED + rebake text
    # AND must be permanent (no transient prefix / flag) so the orchestrator
    # surfaces it instead of retrying indefinitely.
    from orcest.shared.models import TRANSIENT_SUMMARY_PREFIX

    published = False
    for meth in (mock_redis.xadd_capped, mock_redis.xadd_capped_raw):
        for call in getattr(meth, "call_args_list", []):
            args = call[0]
            if len(args) >= 2 and isinstance(args[1], dict):
                d = args[1]
                if (
                    d.get("status") == "failed"
                    and "rebake" in d.get("summary", "").lower()
                    and "grok" in d.get("summary", "").lower()
                ):
                    # Lock in PERMANENT failure: summary must not carry the
                    # transient wire-protocol prefix, and any explicit
                    # serialized transient flag (now or future) must be falsy.
                    assert not d.get("summary", "").startswith(TRANSIENT_SUMMARY_PREFIX), (
                        "early reject must be permanent, not transient"
                    )
                    assert d.get("transient") in (
                        None,
                        "",
                        "0",
                        "false",
                        "False",
                    ), "early reject must not set transient flag"
                    published = True
    assert published, "Expected FAILED result with rebake text"

    # Must ACK so the entry is removed from the PEL
    mock_redis.xack_raw.assert_called_once()

    # Primary result processing owns coordination cleanup; the worker must not
    # prematurely unlock/release attempts before the orchestrator consumes it.
    mock_redis.client.pipeline.assert_not_called()


def test_early_reject_dead_letter_handoff_acks_and_clears_coordination(
    mocker, local_worker_config, sample_task
):
    """A durable reject DLQ payload replaces the source task as recovery state."""
    from orcest.worker.loop import _early_reject_unsupported_provider

    mock_redis = MagicMock()
    mocker.patch(
        "orcest.worker.loop._publish_result_with_retry",
        return_value=ResultPublishOutcome.DEAD_LETTERED,
    )
    cleanup = mocker.patch("orcest.worker.loop._cleanup_coordination_once", return_value=True)

    handoff = _early_reject_unsupported_provider(
        sample_task,
        "grok",
        local_worker_config,
        mock_redis,
        logging.getLogger("test.reject"),
        "tasks:claude",
        "0-0",
    )

    assert handoff.publish_outcome is ResultPublishOutcome.DEAD_LETTERED
    assert handoff.terminal is True
    mock_redis.xack_raw.assert_called_once_with("tasks:claude", CONSUMER_GROUP, "0-0")
    cleanup.assert_called_once()
    assert cleanup.call_args.args[0] is mock_redis
    assert cleanup.call_args.args[1].task_id == sample_task.id


@pytest.mark.parametrize(
    "outcome",
    [ResultPublishOutcome.LOST, ResultPublishOutcome.ABORTED],
)
def test_early_reject_without_durable_handoff_preserves_source_and_coordination(
    mocker, local_worker_config, sample_task, outcome
):
    """LOST/ABORTED rejects retain every recovery mechanism for a later retry."""
    from orcest.worker.loop import _early_reject_unsupported_provider

    mock_redis = MagicMock()
    mocker.patch("orcest.worker.loop._publish_result_with_retry", return_value=outcome)
    cleanup = mocker.patch("orcest.worker.loop._cleanup_coordination_once")
    abort = threading.Event()
    abort.set()

    handoff = _early_reject_unsupported_provider(
        sample_task,
        "grok",
        local_worker_config,
        mock_redis,
        logging.getLogger("test.reject"),
        "tasks:claude",
        "0-0",
        abort_event=abort,
    )

    assert handoff.publish_outcome is outcome
    assert handoff.terminal is False
    mock_redis.xack_raw.assert_not_called()
    cleanup.assert_not_called()


# ---------------------------------------------------------------------------
# Tests for _runner_for_task (PR 1: per-task runner dispatch)
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestRunnerForTask:
    """Per-task dispatch picks the right Runner instance based on PROVIDER_REGISTRY."""

    def test_matching_provider_uses_fallback(self, local_worker_config, sample_task):
        """task.provider == config.runner.type → reuse the pre-created fallback.

        This is the common path (claude worker, claude task) and the contract
        tests rely on: the runner the worker was initialized with is what
        actually runs the task. No per-task re-instantiation.
        """
        fallback = MagicMock()
        assert sample_task.provider == "claude"
        assert local_worker_config.runner.type == "claude"

        runner = _runner_for_task(sample_task, local_worker_config, fallback)

        assert runner is fallback

    def test_different_provider_instantiates_from_registry(self, local_worker_config, sample_task):
        """task.provider != config.runner.type → fresh instance from the registry.

        Demonstrates the new per-task dispatch surface: a worker configured
        with one provider can serve a task for another, provided the
        registry has a Runner registered for that provider.
        """
        # Configure the worker as "noop" so the task's "claude" provider
        # diverges from config.runner.type and triggers registry lookup.
        from orcest.shared.config import RedisConfig, RunnerConfig, WorkerConfig

        config = WorkerConfig(
            worker_id="test-worker",
            backend="claude",
            redis=RedisConfig(host="localhost", port=6379),
            workspace_dir="/tmp/x",
            runner=RunnerConfig(
                type="noop",
                timeout=10,
                max_retries=1,
                retry_backoff=0,
                model="some-test-model",
            ),
        )
        fallback = MagicMock()  # represents the noop runner; should NOT be used

        runner = _runner_for_task(sample_task, config, fallback)

        # Registry's runner_cls for "claude" is ClaudeRunner — that's what we get.
        from orcest.worker.claude_runner import ClaudeRunner

        assert isinstance(runner, ClaudeRunner)
        assert runner is not fallback
        # RunnerConfig.model flows through to the freshly-instantiated runner.
        assert runner.model == "some-test-model"

    def test_claude_provider_dispatch_honors_interactive_mode(self, sample_task, tmp_path):
        """A Claude task reached through provider dispatch keeps interactive mode."""
        from orcest.worker.claude_interactive_runner import ClaudeInteractiveRunner

        config = WorkerConfig(
            worker_id="test-worker",
            backend="claude",
            redis=RedisConfig(host="localhost", port=6379),
            workspace_dir=str(tmp_path / "workspaces"),
            runner=RunnerConfig(
                type="noop",
                timeout=10,
                max_retries=1,
                retry_backoff=0,
                model="some-test-model",
                extra={"mode": "interactive"},
            ),
        )
        fallback = MagicMock()

        runner = _runner_for_task(sample_task, config, fallback)

        assert isinstance(runner, ClaudeInteractiveRunner)
        assert runner is not fallback
        assert runner.model == "some-test-model"

    def test_clauder_provider_dispatch_honors_interactive_mode(self, sample_task, tmp_path):
        """A clauder task uses the PTY Claude runner for isolated interactive pools."""
        from orcest.worker.claude_interactive_runner import ClaudeInteractiveRunner

        sample_task.provider = "clauder"
        config = WorkerConfig(
            worker_id="test-worker",
            backend="clauder",
            redis=RedisConfig(host="localhost", port=6379),
            workspace_dir=str(tmp_path / "workspaces"),
            runner=RunnerConfig(
                type="claude",
                timeout=10,
                max_retries=1,
                retry_backoff=0,
                model="some-test-model",
                extra={"mode": "interactive"},
            ),
        )
        fallback = MagicMock()

        runner = _runner_for_task(sample_task, config, fallback)

        assert isinstance(runner, ClaudeInteractiveRunner)
        assert runner is not fallback
        assert runner.model == "some-test-model"

    def test_codex_provider_dispatches_to_codex_runner(
        self, local_worker_config, sample_task, monkeypatch
    ):
        """Codex registration regression: once ``codex`` is in
        PROVIDER_REGISTRY, a task with ``provider="codex"`` must instantiate
        CodexRunner — never fall back to ClaudeRunner, which would 401
        against codex auth (Claude OAuth header vs CODEX_API_KEY /
        ~/.codex/auth.json). The previous version of this test asserted the
        opposite ("codex not in registry, falls back"); flipped here when
        the registration landed."""
        from orcest.worker.codex_runner import CodexRunner

        assert "codex" in PROVIDER_REGISTRY
        monkeypatch.setattr(sample_task, "provider", "codex")
        fallback = MagicMock()

        runner = _runner_for_task(sample_task, local_worker_config, fallback)

        assert isinstance(runner, CodexRunner)
        assert runner is not fallback

    def test_unknown_provider_falls_back(self, local_worker_config, sample_task, monkeypatch):
        """Provider absent from the registry → use the fallback.

        The main loop's early-reject normally catches unknown providers, but
        if dispatch is reached anyway (e.g. via a direct unit test path) the
        fallback is preserved — never instantiates something arbitrary.
        """
        monkeypatch.setattr(sample_task, "provider", "neverregistered")
        fallback = MagicMock()

        runner = _runner_for_task(sample_task, local_worker_config, fallback)

        assert runner is fallback

    def test_registry_entry_without_runner_cls_falls_back(
        self, local_worker_config, sample_task, monkeypatch
    ):
        """A ProviderRecipe with runner_cls=None falls back.

        Backwards-compat path: any recipe lacking a runner_cls keeps the
        worker's configured runner instead of crashing on None().
        """
        monkeypatch.setattr(sample_task, "provider", "no-runner-cls")
        monkeypatch.setitem(
            PROVIDER_REGISTRY,
            "no-runner-cls",
            ProviderRecipe(binary="fake", env_var="FAKE_TOKEN", runner_cls=None),
        )
        fallback = MagicMock()

        runner = _runner_for_task(sample_task, local_worker_config, fallback)

        assert runner is fallback


@pytest.mark.unit
def test_execute_task_passes_per_task_model_override(
    local_worker_config, sample_task, mock_workspace, monkeypatch
):
    """When task.model is set, it overrides config.runner.model in runner.run().

    Verifies the model plumbing added in PR 1: a per-task model is forwarded
    as a kwarg to the runner, so different tasks against the same provider
    can use different models on the same worker.
    """
    monkeypatch.setattr(sample_task, "model", "claude-3-opus-test")

    mock_runner = MagicMock()
    mock_runner.run.return_value = _success_runner_result()
    mock_redis = MagicMock()
    mock_redis.xadd_capped.return_value = "1-0"

    _execute_task(
        sample_task,
        local_worker_config,
        mock_runner,
        mock_workspace,
        mock_redis,
        logging.getLogger("test"),
    )

    mock_runner.run.assert_called_once()
    _, kwargs = mock_runner.run.call_args
    assert kwargs["model"] == "claude-3-opus-test"


@pytest.mark.unit
def test_execute_task_falls_back_to_config_model_when_task_unset(
    local_worker_config, sample_task, mock_workspace
):
    """When task.model is empty, config.runner.model is used.

    Complements the override test: the worker-wide default still applies
    when the task carries no per-task override (the default case for
    pre-multi-runner publishers).
    """
    from orcest.shared.config import RunnerConfig, WorkerConfig

    config = WorkerConfig(
        worker_id=local_worker_config.worker_id,
        backend=local_worker_config.backend,
        redis=local_worker_config.redis,
        workspace_dir=local_worker_config.workspace_dir,
        runner=RunnerConfig(
            timeout=10,
            max_retries=1,
            retry_backoff=0,
            model="worker-default-model",
        ),
    )
    assert not sample_task.model  # baseline

    mock_runner = MagicMock()
    mock_runner.run.return_value = _success_runner_result()
    mock_redis = MagicMock()
    mock_redis.xadd_capped.return_value = "1-0"

    _execute_task(
        sample_task,
        config,
        mock_runner,
        mock_workspace,
        mock_redis,
        logging.getLogger("test"),
    )

    mock_runner.run.assert_called_once()
    _, kwargs = mock_runner.run.call_args
    assert kwargs["model"] == "worker-default-model"


@pytest.mark.unit
def test_execute_task_model_empty_string_falls_back(
    local_worker_config, sample_task, mock_workspace, monkeypatch
):
    """Empty-string task.model (not None) still falls back to config.runner.model.

    Pins the specific `task.model or config.runner.model` expression — a
    naive refactor to `task.model if task.model is not None else …` would
    silently regress this case (empty string is a real value Task may carry).
    """
    from orcest.shared.config import RunnerConfig, WorkerConfig

    config = WorkerConfig(
        worker_id=local_worker_config.worker_id,
        backend=local_worker_config.backend,
        redis=local_worker_config.redis,
        workspace_dir=local_worker_config.workspace_dir,
        runner=RunnerConfig(
            timeout=10,
            max_retries=1,
            retry_backoff=0,
            model="worker-default-model",
        ),
    )
    monkeypatch.setattr(sample_task, "model", "")

    mock_runner = MagicMock()
    mock_runner.run.return_value = _success_runner_result()
    mock_redis = MagicMock()
    mock_redis.xadd_capped.return_value = "1-0"

    _execute_task(
        sample_task,
        config,
        mock_runner,
        mock_workspace,
        mock_redis,
        logging.getLogger("test"),
    )

    mock_runner.run.assert_called_once()
    _, kwargs = mock_runner.run.call_args
    assert kwargs["model"] == "worker-default-model"


@pytest.mark.unit
def test_execute_task_model_override_when_config_default_empty(
    local_worker_config, sample_task, monkeypatch, mock_workspace
):
    """task.model wins even when config.runner.model is empty.

    Symmetric to the override test: per-task model is honored regardless of
    whether the worker has a default.
    """
    monkeypatch.setattr(sample_task, "model", "claude-3-opus-test")
    # local_worker_config has RunnerConfig(..., model="") by default — pin it.
    assert local_worker_config.runner.model == ""

    mock_runner = MagicMock()
    mock_runner.run.return_value = _success_runner_result()
    mock_redis = MagicMock()
    mock_redis.xadd_capped.return_value = "1-0"

    _execute_task(
        sample_task,
        local_worker_config,
        mock_runner,
        mock_workspace,
        mock_redis,
        logging.getLogger("test"),
    )

    mock_runner.run.assert_called_once()
    _, kwargs = mock_runner.run.call_args
    assert kwargs["model"] == "claude-3-opus-test"
