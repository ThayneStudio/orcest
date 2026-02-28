"""Unit tests for the worker main loop and task execution."""

from __future__ import annotations

import logging
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from orcest.shared.config import ClaudeConfig, RedisConfig, WorkerConfig
from orcest.shared.models import ResultStatus, Task, TaskResult, TaskType
from orcest.worker.claude_runner import ClaudeResult
from orcest.worker.loop import (
    CONSUMER_GROUP,
    RESULTS_STREAM,
    TASKS_STREAM,
    _execute_task,
    run_worker,
)

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
        claude=ClaudeConfig(timeout=10, max_retries=1, retry_backoff=0),
    )


@pytest.fixture
def sample_task():
    """A minimal Task for testing."""
    return Task.create(
        task_type=TaskType.FIX_PR,
        repo="owner/repo",
        token="ghp_fake_token",
        resource_type="pr",
        resource_id=42,
        prompt="Fix the failing CI checks",
        branch="fix-ci",
    )


@pytest.fixture
def mock_workspace(mocker):
    """A mock Workspace whose setup() returns a tmp path without cloning."""
    ws = MagicMock()
    ws.setup.return_value = Path("/tmp/fake-workspace/repo")
    ws.cleanup.return_value = None
    return ws


def _success_claude_result() -> ClaudeResult:
    return ClaudeResult(
        success=True,
        summary="All checks fixed",
        duration_seconds=5,
        raw_output="{}",
    )


def _failure_claude_result() -> ClaudeResult:
    return ClaudeResult(
        success=False,
        summary="Could not resolve merge conflict",
        duration_seconds=3,
        raw_output="{}",
    )


# ---------------------------------------------------------------------------
# Tests for _execute_task (the single-iteration helper)
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestExecuteTask:
    """Tests for the _execute_task internal helper."""

    def test_worker_processes_task(self, mocker, local_worker_config, sample_task, mock_workspace):
        """_execute_task returns a COMPLETED TaskResult on Claude success."""
        mocker.patch(
            "orcest.worker.loop.run_claude",
            return_value=_success_claude_result(),
        )

        result = _execute_task(
            sample_task, local_worker_config, mock_workspace, logging.getLogger("test")
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

    def test_worker_handles_claude_failure(
        self, mocker, local_worker_config, sample_task, mock_workspace
    ):
        """_execute_task returns a FAILED TaskResult when Claude fails."""
        mocker.patch(
            "orcest.worker.loop.run_claude",
            return_value=_failure_claude_result(),
        )

        result = _execute_task(
            sample_task, local_worker_config, mock_workspace, logging.getLogger("test")
        )

        assert result.status == ResultStatus.FAILED
        assert result.task_id == sample_task.id
        assert "merge conflict" in result.summary.lower()

    def test_worker_handles_usage_exhaustion(
        self, mocker, local_worker_config, sample_task, mock_workspace
    ):
        """_execute_task returns USAGE_EXHAUSTED when Claude reports limits."""
        exhausted = ClaudeResult(
            success=False,
            summary="Claude usage limit reached",
            duration_seconds=1,
            raw_output="",
        )
        mocker.patch("orcest.worker.loop.run_claude", return_value=exhausted)

        result = _execute_task(
            sample_task, local_worker_config, mock_workspace, logging.getLogger("test")
        )

        assert result.status == ResultStatus.USAGE_EXHAUSTED

    def test_workspace_exception_returns_failed(
        self, mocker, local_worker_config, sample_task, mock_workspace
    ):
        """If workspace.setup() raises, the result is FAILED and cleanup runs."""
        mock_workspace.setup.side_effect = RuntimeError("clone failed")

        result = _execute_task(
            sample_task, local_worker_config, mock_workspace, logging.getLogger("test")
        )

        assert result.status == ResultStatus.FAILED
        assert "clone failed" in result.summary
        mock_workspace.cleanup.assert_called_once()


# ---------------------------------------------------------------------------
# Tests for run_worker (the full loop)
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestRunWorker:
    """Integration-level tests for the run_worker main loop.

    These tests mock Redis, Workspace, and run_claude to verify the
    loop's orchestration logic: stream reading, locking, result
    publishing, and ACK handling.
    """

    def _build_mock_redis(self, mocker, task: Task):
        """Create a mock RedisClient that serves one task then triggers shutdown.

        Returns (mock_redis, captured_signal_handlers) so the test can
        inspect calls and the signal handler dict.
        """
        mock_redis = MagicMock()
        mock_redis.health_check.return_value = True
        mock_redis.ensure_consumer_group.return_value = None
        mock_redis.xack.return_value = 1

        # Capture published results via xadd
        mock_redis.xadd.return_value = "1-0"

        # For RedisLock -- it accesses redis.client.register_script
        mock_script = MagicMock(return_value=1)
        mock_redis.client.register_script.return_value = mock_script
        # lock.acquire calls redis.client.set(..., nx=True, ex=...)
        mock_redis.client.set.return_value = True

        return mock_redis

    def _setup_run_worker(self, mocker, worker_config, task, mock_redis):
        """Patch all external dependencies of run_worker.

        Returns a dict of relevant mocks for assertions.
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
        mock_ws.cleanup.return_value = None
        mocker.patch("orcest.worker.loop.Workspace", return_value=mock_ws)

        # Patch signal.signal to capture handlers instead of registering
        # real signal handlers (which interfere with pytest).
        signal_handlers = {}

        def fake_signal(signum, handler):
            signal_handlers[signum] = handler

        mocker.patch("orcest.worker.loop.signal.signal", side_effect=fake_signal)

        # Patch run_claude
        mock_run_claude = mocker.patch("orcest.worker.loop.run_claude")

        return {
            "workspace": mock_ws,
            "run_claude": mock_run_claude,
            "signal_handlers": signal_handlers,
        }

    def _configure_one_iteration(self, mock_redis, task, signal_handlers):
        """Configure xreadgroup to return one task, then trigger shutdown."""
        task_fields = task.to_dict()
        call_count = 0

        def xreadgroup_side_effect(**kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return [("entry-1", task_fields)]
            # On subsequent calls, trigger SIGTERM handler to exit loop
            import signal as sig

            handler = signal_handlers.get(sig.SIGTERM)
            if handler:
                handler(sig.SIGTERM, None)
            return []

        mock_redis.xreadgroup.side_effect = xreadgroup_side_effect

    def test_worker_processes_task(self, mocker, worker_config, sample_task):
        """run_worker reads a task from the stream, executes it, and publishes."""
        mock_redis = self._build_mock_redis(mocker, sample_task)
        mocks = self._setup_run_worker(mocker, worker_config, sample_task, mock_redis)
        mocks["run_claude"].return_value = _success_claude_result()
        self._configure_one_iteration(mock_redis, sample_task, mocks["signal_handlers"])

        run_worker(worker_config)

        # Verify run_claude was called
        mocks["run_claude"].assert_called_once()
        # Verify result was published to the results stream
        mock_redis.xadd.assert_called_once()
        call_args = mock_redis.xadd.call_args
        assert call_args[0][0] == RESULTS_STREAM
        result_fields = call_args[0][1]
        assert result_fields["status"] == ResultStatus.COMPLETED.value
        assert result_fields["task_id"] == sample_task.id

    def test_worker_acquires_lock(self, mocker, worker_config, sample_task):
        """run_worker acquires a Redis lock keyed by the task's resource_id."""
        mock_redis = self._build_mock_redis(mocker, sample_task)
        mocks = self._setup_run_worker(mocker, worker_config, sample_task, mock_redis)
        mocks["run_claude"].return_value = _success_claude_result()
        self._configure_one_iteration(mock_redis, sample_task, mocks["signal_handlers"])

        run_worker(worker_config)

        # The lock is acquired via redis.client.set with NX
        mock_redis.client.set.assert_called_once()
        set_call = mock_redis.client.set.call_args
        lock_key = set_call[0][0]
        assert lock_key == f"lock:pr:{sample_task.resource_id}"
        assert set_call[1]["nx"] is True
        assert set_call[1]["ex"] == worker_config.claude.timeout + 60

    def test_worker_skips_locked_task(self, mocker, worker_config, sample_task):
        """When the lock is already held, run_claude is NOT called and the
        task is ACKed so it is not redelivered.
        """
        mock_redis = self._build_mock_redis(mocker, sample_task)
        mocks = self._setup_run_worker(mocker, worker_config, sample_task, mock_redis)

        # Simulate lock already held: set returns None (NX fails)
        mock_redis.client.set.return_value = None

        self._configure_one_iteration(mock_redis, sample_task, mocks["signal_handlers"])

        run_worker(worker_config)

        # run_claude should NOT have been called
        mocks["run_claude"].assert_not_called()
        # The task must still be ACKed (to avoid redelivery)
        mock_redis.xack.assert_called_once_with(TASKS_STREAM, CONSUMER_GROUP, "entry-1")
        # No result should be published
        mock_redis.xadd.assert_not_called()

    def test_worker_publishes_result(self, mocker, worker_config, sample_task):
        """A completed task produces a TaskResult with COMPLETED on the
        results stream.
        """
        mock_redis = self._build_mock_redis(mocker, sample_task)
        mocks = self._setup_run_worker(mocker, worker_config, sample_task, mock_redis)
        mocks["run_claude"].return_value = _success_claude_result()
        self._configure_one_iteration(mock_redis, sample_task, mocks["signal_handlers"])

        run_worker(worker_config)

        # Verify the published result
        mock_redis.xadd.assert_called_once()
        stream, result_dict = mock_redis.xadd.call_args[0]
        assert stream == RESULTS_STREAM
        parsed = TaskResult.from_dict(result_dict)
        assert parsed.status == ResultStatus.COMPLETED
        assert parsed.task_id == sample_task.id
        assert parsed.worker_id == worker_config.worker_id
        assert parsed.resource_id == sample_task.resource_id

    def test_worker_handles_claude_failure(self, mocker, worker_config, sample_task):
        """When run_claude returns success=False, the result has FAILED status."""
        mock_redis = self._build_mock_redis(mocker, sample_task)
        mocks = self._setup_run_worker(mocker, worker_config, sample_task, mock_redis)
        mocks["run_claude"].return_value = _failure_claude_result()
        self._configure_one_iteration(mock_redis, sample_task, mocks["signal_handlers"])

        run_worker(worker_config)

        mock_redis.xadd.assert_called_once()
        stream, result_dict = mock_redis.xadd.call_args[0]
        assert stream == RESULTS_STREAM
        parsed = TaskResult.from_dict(result_dict)
        assert parsed.status == ResultStatus.FAILED
        assert "merge conflict" in parsed.summary.lower()
