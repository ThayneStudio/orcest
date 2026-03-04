"""Unit tests for the worker main loop and task execution."""

from __future__ import annotations

import logging
import threading
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from orcest.shared.config import RedisConfig, RunnerConfig, WorkerConfig
from orcest.shared.models import ResultStatus, Task, TaskResult, TaskType
from orcest.worker.loop import (
    _STREAM_MAXLEN,
    CONSUMER_GROUP,
    HEARTBEAT_INTERVAL,
    LOCK_TTL,
    RESULTS_STREAM,
    _execute_task,
    _make_abort_event,
    run_worker,
)
from orcest.worker.runner import RunnerResult

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
    )


@pytest.fixture
def mock_workspace():
    """A mock Workspace whose setup() returns a tmp path without cloning."""
    ws = MagicMock()
    ws.setup.return_value = Path("/tmp/fake-workspace/repo")
    ws.cleanup.return_value = None
    return ws


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
            stream, {"line": '{"role": "assistant"}\n'}, maxlen=_STREAM_MAXLEN
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
        calls = mock_redis.xadd_capped.call_args_list

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

    def test_worker_on_output_redis_error_logs_once(
        self, local_worker_config, sample_task, mock_workspace, caplog
    ):
        """When redis.xadd_capped raises inside on_output, the first error
        is logged as a warning but subsequent errors are suppressed."""
        mock_runner = MagicMock()

        # Configure the runner to invoke on_output multiple times
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

        # Count how many "Failed to publish output line" warnings were logged.
        # The first error should be logged; subsequent ones should be suppressed.
        output_warnings = [
            r for r in caplog.records if "Failed to publish output line" in r.message
        ]
        assert len(output_warnings) == 1

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


# ---------------------------------------------------------------------------
# Tests for run_worker (the full loop)
# ---------------------------------------------------------------------------


@pytest.mark.unit
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
        mock_redis.xack.return_value = 1

        # Capture published results via xadd / xadd_capped
        mock_redis.xadd.return_value = "1-0"
        mock_redis.xadd_capped.return_value = "1-0"

        # For RedisLock -- it accesses redis.client.register_script
        mock_script = MagicMock(return_value=1)
        mock_redis.client.register_script.return_value = mock_script
        # lock.acquire calls redis.client.set(..., nx=True, ex=...)
        mock_redis.client.set.return_value = True

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

        # Patch Heartbeat to avoid spawning real daemon threads in unit tests.
        # Use the caller-supplied mock when provided so the dependency is explicit.
        if heartbeat_mock is None:
            heartbeat_mock = MagicMock()
        mocker.patch("orcest.worker.loop.Heartbeat", heartbeat_mock)

        return {
            "workspace": mock_ws,
            "runner": mock_runner,
            "signal_handlers": signal_handlers,
        }

    def _configure_one_iteration(self, mock_redis, task, signal_handlers):
        """Configure xreadgroup to return one task, then trigger shutdown.

        Pending drain calls (pending=True) always return empty so the
        drain completes immediately.  The first non-pending call returns
        the task; subsequent calls trigger SIGTERM.
        """
        task_fields = task.to_dict()
        normal_call_count = 0

        def xreadgroup_side_effect(**kwargs):
            nonlocal normal_call_count
            # Pending drain phase — return empty so it finishes quickly
            if kwargs.get("pending", False):
                return []
            normal_call_count += 1
            if normal_call_count == 1:
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

    def test_worker_acquires_lock(self, mocker, worker_config, sample_task):
        """run_worker acquires a Redis lock keyed by the task's resource_id."""
        mock_redis = self._build_mock_redis()
        mocks = self._setup_run_worker(mocker, worker_config, mock_redis)
        mocks["runner"].run.return_value = _success_runner_result()
        self._configure_one_iteration(mock_redis, sample_task, mocks["signal_handlers"])

        run_worker(worker_config)

        # The lock is acquired via redis.client.set with NX
        mock_redis.client.set.assert_called_once()
        set_call = mock_redis.client.set.call_args
        lock_key = set_call[0][0]
        assert lock_key == f"lock:pr:{sample_task.repo}:{sample_task.resource_id}"
        assert set_call[1]["nx"] is True
        assert set_call[1]["ex"] == LOCK_TTL

    def test_worker_skips_locked_task(self, mocker, worker_config, sample_task):
        """When the lock is already held, the runner is NOT called and the
        task is ACKed so it is not redelivered.
        """
        mock_redis = self._build_mock_redis()
        mocks = self._setup_run_worker(mocker, worker_config, mock_redis)

        # Simulate lock already held: set returns None (NX fails)
        mock_redis.client.set.return_value = None

        self._configure_one_iteration(mock_redis, sample_task, mocks["signal_handlers"])

        run_worker(worker_config)

        # runner should NOT have been called
        mocks["runner"].run.assert_not_called()
        # The task must still be ACKed (to avoid redelivery)
        expected_stream = f"tasks:{worker_config.backend}"
        mock_redis.xack.assert_called_once_with(expected_stream, CONSUMER_GROUP, "entry-1")
        # No result should be published
        mock_redis.xadd.assert_not_called()

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
        assert parsed.resource_id == sample_task.resource_id

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
        """When redis.health_check() returns False, run_worker calls sys.exit(1)."""
        mock_redis = self._build_mock_redis()
        # Override health_check to return False
        mock_redis.health_check.return_value = False
        self._setup_run_worker(mocker, worker_config, mock_redis)

        with pytest.raises(SystemExit) as exc_info:
            run_worker(worker_config)

        assert exc_info.value.code == 1
        # Should never attempt to read from the stream
        mock_redis.xreadgroup.assert_not_called()

    def test_worker_result_publish_failure_does_not_ack(self, mocker, worker_config, sample_task):
        """When redis.xadd_capped for the results stream raises, xack must NOT be called.
        The message stays in XPENDING so it can be re-delivered and the result
        is not silently lost."""
        mock_redis = self._build_mock_redis()
        mocks = self._setup_run_worker(mocker, worker_config, mock_redis)
        mocks["runner"].run.return_value = _success_runner_result()
        self._configure_one_iteration(mock_redis, sample_task, mocks["signal_handlers"])

        # Make the results-stream publish fail
        def _xadd_capped_side_effect(stream, data, **kwargs):
            if stream == RESULTS_STREAM:
                raise ConnectionError("Redis unavailable")
            return "1-0"

        mock_redis.xadd_capped.side_effect = _xadd_capped_side_effect

        run_worker(worker_config)

        # xack must NOT be called — leave the message in XPENDING for re-delivery
        mock_redis.xack.assert_not_called()

    def test_worker_malformed_task_acks_and_continues(self, mocker, worker_config):
        """When a stream entry cannot be deserialized, the worker ACKs it
        (to prevent infinite redelivery) and continues to the next entry."""
        mock_redis = self._build_mock_redis()
        mocks = self._setup_run_worker(mocker, worker_config, mock_redis)

        # Return a malformed entry (missing required fields), then trigger shutdown
        normal_call_count = 0

        def xreadgroup_side_effect(**kwargs):
            nonlocal normal_call_count
            if kwargs.get("pending", False):
                return []
            normal_call_count += 1
            if normal_call_count == 1:
                return [("entry-bad", {"garbage": "data"})]
            import signal as sig

            handler = mocks["signal_handlers"].get(sig.SIGTERM)
            if handler:
                handler(sig.SIGTERM, None)
            return []

        mock_redis.xreadgroup.side_effect = xreadgroup_side_effect

        run_worker(worker_config)

        # Runner should NOT have been called (task was unparseable)
        mocks["runner"].run.assert_not_called()
        # The malformed entry must still be ACKed
        expected_stream = f"tasks:{worker_config.backend}"
        mock_redis.xack.assert_called_once_with(expected_stream, CONSUMER_GROUP, "entry-bad")

    def test_worker_drains_pending_on_startup(self, mocker, worker_config, sample_task):
        """On startup, pending (unACKed) tasks from a previous lifecycle are
        drained: a FAILED result is published and the entry is ACKed."""
        mock_redis = self._build_mock_redis()
        mocks = self._setup_run_worker(mocker, worker_config, mock_redis)

        task_fields = sample_task.to_dict()
        pending_calls = 0

        def xreadgroup_side_effect(**kwargs):
            nonlocal pending_calls
            if kwargs.get("pending", False):
                pending_calls += 1
                if pending_calls == 1:
                    return [("pending-1", task_fields)]
                return []
            # No new tasks — trigger shutdown immediately
            import signal as sig

            handler = mocks["signal_handlers"].get(sig.SIGTERM)
            if handler:
                handler(sig.SIGTERM, None)
            return []

        mock_redis.xreadgroup.side_effect = xreadgroup_side_effect

        run_worker(worker_config)

        # Runner should NOT have been called (pending tasks are not re-executed)
        mocks["runner"].run.assert_not_called()
        # A FAILED result should have been published for the pending task
        mock_redis.xadd.assert_called_once()
        stream, result_dict = mock_redis.xadd.call_args[0]
        assert stream == RESULTS_STREAM
        parsed = TaskResult.from_dict(result_dict)
        assert parsed.status == ResultStatus.FAILED
        assert parsed.task_id == sample_task.id
        assert "restarted" in parsed.summary.lower()
        # The pending entry must be ACKed
        expected_stream = f"tasks:{worker_config.backend}"
        mock_redis.xack.assert_any_call(expected_stream, CONSUMER_GROUP, "pending-1")

    def test_lock_ttl_proportional_to_heartbeat_interval(self):
        """LOCK_TTL must equal 3 × HEARTBEAT_INTERVAL to bound the crash-orphaned-lock window.

        Regression test for issue #206: before this fix the lock TTL was
        derived from runner timeouts (~5540 s ≈ 92 min), meaning a crashed
        worker held its Redis lock for up to 92 min.  Setting LOCK_TTL =
        3 × HEARTBEAT_INTERVAL (180 s) bounds that window to ~3 min.
        """
        assert LOCK_TTL == 3 * HEARTBEAT_INTERVAL, (
            f"LOCK_TTL ({LOCK_TTL}s) must be 3 × HEARTBEAT_INTERVAL ({HEARTBEAT_INTERVAL}s) "
            "to bound the crash-orphaned-lock window (issue #206)"
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
