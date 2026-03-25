"""Unit tests for the worker main loop and task execution."""

from __future__ import annotations

import logging
import signal
import threading
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from orcest.shared.config import RedisConfig, RunnerConfig, WorkerConfig
from orcest.shared.models import ResultStatus, Task, TaskResult, TaskType
from orcest.worker.loop import (
    _RESULT_PUBLISH_BACKOFF,
    _RESULT_PUBLISH_RETRIES,
    _STREAM_MAXLEN,
    CONSUMER_GROUP,
    DEAD_LETTER_STREAM,
    HEARTBEAT_INTERVAL,
    LOCK_TTL,
    MAX_DELIVERY_COUNT,
    RESULTS_STREAM,
    _check_gh_credentials,
    _dead_letter_task,
    _execute_task,
    _make_abort_event,
    _publish_result_with_retry,
    run_worker,
)
from orcest.worker.runner import RunnerResult
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
            sample_task.repo, sample_task.branch, sample_task.token, sample_task.base_branch
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

    def test_rebase_pr_skips_auto_rebase(self, local_worker_config, mock_workspace):
        """REBASE_PR tasks pass base_branch=None to workspace.setup so the
        workspace doesn't auto-rebase — Claude handles the rebase itself."""
        task = Task.create(
            task_type=TaskType.REBASE_PR,
            repo="owner/repo",
            token="tok",
            resource_type="pr",
            resource_id=1,
            prompt="rebase",
            branch="feature",
            base_branch="main",
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
            None,  # base_branch suppressed
        )


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
        """Configure xreadgroup_multi to return one task, then trigger shutdown.

        The first call returns the task on the PR stream; subsequent calls
        trigger SIGTERM.
        """
        task_fields = task.to_dict()
        normal_call_count = 0

        def xreadgroup_multi_side_effect(**kwargs):
            nonlocal normal_call_count
            streams = kwargs.get("streams", {})
            # Drain phase: streams have "0" as entry ID
            if any(v == "0" for v in streams.values()):
                return []
            normal_call_count += 1
            if normal_call_count == 1:
                # Return task on the first PR stream
                first_stream = next(iter(streams))
                return [(first_stream, "entry-1", task_fields)]
            # On subsequent calls, trigger SIGTERM handler to exit loop
            handler = signal_handlers.get(signal.SIGTERM)
            if handler:
                handler(signal.SIGTERM, None)
            return []

        mock_redis.xreadgroup_multi.side_effect = xreadgroup_multi_side_effect

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

        def xreadgroup_multi_side_effect(**kwargs):
            nonlocal normal_call_count
            streams = kwargs.get("streams", {})
            if any(v == "0" for v in streams.values()):
                return []
            normal_call_count += 1
            # PR stream is checked first (non-blocking) -- return empty
            if normal_call_count == 1:
                return []  # PR stream empty
            # Issue stream is checked second (blocking) -- return task
            if normal_call_count == 2:
                first_stream = next(iter(streams))
                return [(first_stream, "entry-1", task_fields)]
            # Trigger shutdown on subsequent calls
            handler = mocks["signal_handlers"].get(signal.SIGTERM)
            if handler:
                handler(signal.SIGTERM, None)
            return []

        mock_redis.xreadgroup_multi.side_effect = xreadgroup_multi_side_effect

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
        mock_redis.client.set.assert_called_once()
        set_call = mock_redis.client.set.call_args
        lock_key = set_call[0][0]
        assert lock_key == f"test:lock:pr:{sample_task.repo}:{sample_task.resource_id}"
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
        expected_fq_stream = f"{worker_config.redis.key_prefix}:tasks:{worker_config.backend}"
        mock_redis.xack_raw.assert_called_once_with(expected_fq_stream, CONSUMER_GROUP, "entry-1")
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

    def test_worker_clears_pending_marker_after_success(self, mocker, worker_config, sample_task):
        """After a successful task, the pending-task marker is cleared so the
        orchestrator can re-enqueue if needed."""
        mock_redis = self._build_mock_redis()
        mocks = self._setup_run_worker(mocker, worker_config, mock_redis)
        mocks["runner"].run.return_value = _success_runner_result()
        mock_clear = mocker.patch("orcest.worker.loop._clear_pending_task_for_task")
        self._configure_one_iteration(mock_redis, sample_task, mocks["signal_handlers"])

        run_worker(worker_config)

        mock_clear.assert_called_once()

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
        """When all result-stream publish retries raise, xack_raw must NOT be called.
        The message stays in XPENDING so it can be re-delivered and the result
        is not silently lost (a dead-letter entry is written instead)."""
        mock_redis = self._build_mock_redis()
        mocks = self._setup_run_worker(mocker, worker_config, mock_redis)
        mocks["runner"].run.return_value = _success_runner_result()
        self._configure_one_iteration(mock_redis, sample_task, mocks["signal_handlers"])
        mocker.patch("orcest.worker.loop.time.sleep")  # avoid real sleeps during retries

        # Make the results-stream publish fail (all retries)
        def _xadd_capped_side_effect(stream, data, **kwargs):
            if stream == RESULTS_STREAM:
                raise ConnectionError("Redis unavailable")
            return "1-0"

        mock_redis.xadd_capped.side_effect = _xadd_capped_side_effect

        run_worker(worker_config)

        # xack_raw must NOT be called — leave the message in XPENDING for re-delivery
        mock_redis.xack_raw.assert_not_called()

    def test_worker_malformed_task_acks_and_continues(self, mocker, worker_config):
        """When a stream entry cannot be deserialized, the worker ACKs it
        (to prevent infinite redelivery) and continues to the next entry."""
        mock_redis = self._build_mock_redis()
        mocks = self._setup_run_worker(mocker, worker_config, mock_redis)

        # Return a malformed entry (missing required fields), then trigger shutdown
        normal_call_count = 0

        def xreadgroup_multi_side_effect(**kwargs):
            nonlocal normal_call_count
            streams = kwargs.get("streams", {})
            if any(v == "0" for v in streams.values()):
                return []
            normal_call_count += 1
            if normal_call_count == 1:
                first_stream = next(iter(streams))
                return [(first_stream, "entry-bad", {"garbage": "data"})]
            handler = mocks["signal_handlers"].get(signal.SIGTERM)
            if handler:
                handler(signal.SIGTERM, None)
            return []

        mock_redis.xreadgroup_multi.side_effect = xreadgroup_multi_side_effect

        run_worker(worker_config)

        # Runner should NOT have been called (task was unparseable)
        mocks["runner"].run.assert_not_called()
        # The malformed entry must still be ACKed (fully-qualified stream name)
        expected_fq_stream = f"{worker_config.redis.key_prefix}:tasks:{worker_config.backend}"
        mock_redis.xack_raw.assert_called_once_with(expected_fq_stream, CONSUMER_GROUP, "entry-bad")

    def test_worker_drains_pending_on_startup(self, mocker, worker_config, sample_task):
        """On startup, pending (unACKed) tasks from a previous lifecycle are
        drained: a FAILED result is published, the entry is ACKed, and the
        pending-task marker is cleared so the orchestrator can re-enqueue."""
        mock_redis = self._build_mock_redis()
        mocks = self._setup_run_worker(mocker, worker_config, mock_redis)
        mock_clear = mocker.patch("orcest.worker.loop._clear_pending_task_for_task")

        task_fields = sample_task.to_dict()
        drain_call_count = 0

        def xreadgroup_multi_side_effect(**kwargs):
            nonlocal drain_call_count
            streams = kwargs.get("streams", {})
            # Drain phase: streams have "0" as entry ID
            if any(v == "0" for v in streams.values()):
                drain_call_count += 1
                if drain_call_count == 1:
                    first_stream = next(iter(streams))
                    return [(first_stream, "pending-1", task_fields)]
                return []
            # No new tasks — trigger shutdown immediately
            handler = mocks["signal_handlers"].get(signal.SIGTERM)
            if handler:
                handler(signal.SIGTERM, None)
            return []

        mock_redis.xreadgroup_multi.side_effect = xreadgroup_multi_side_effect

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
        # The pending-task marker must be cleared so the orchestrator can re-enqueue
        mock_clear.assert_called()

    def test_worker_drain_clears_pending_marker_even_on_publish_failure(
        self, mocker, worker_config, sample_task
    ):
        """When the recovery result publish fails during drain, the pending-task
        marker must still be cleared so the orchestrator can re-enqueue the task
        instead of waiting for the marker's TTL to expire (~95 min)."""
        mock_redis = self._build_mock_redis()
        mocks = self._setup_run_worker(mocker, worker_config, mock_redis)
        mock_clear = mocker.patch("orcest.worker.loop._clear_pending_task_for_task")

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
            # No new tasks -- trigger shutdown immediately
            handler = mocks["signal_handlers"].get(signal.SIGTERM)
            if handler:
                handler(signal.SIGTERM, None)
            return []

        mock_redis.xreadgroup_multi.side_effect = xreadgroup_multi_side_effect

        run_worker(worker_config)

        # Runner should NOT have been called (pending tasks are not re-executed)
        mocks["runner"].run.assert_not_called()
        # The pending-task marker must still be cleared despite publish failure
        mock_clear.assert_called()

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
        mock_clear = mocker.patch("orcest.worker.loop._clear_pending_task_for_task")

        # Delivery count above threshold triggers dead-letter path
        mock_redis.xpending_count_raw.return_value = MAX_DELIVERY_COUNT + 1

        self._configure_one_iteration(mock_redis, sample_task, mocks["signal_handlers"])

        run_worker(worker_config)

        # Runner must NOT have been called (task was dead-lettered)
        mocks["runner"].run.assert_not_called()

        # Pending-task marker must be cleared
        mock_clear.assert_called_once()

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

        def xreadgroup_multi_side_effect(**kwargs):
            nonlocal normal_call_count
            streams = kwargs.get("streams", {})
            if any(v == "0" for v in streams.values()):
                return []
            normal_call_count += 1
            if normal_call_count == 1:
                first_stream = next(iter(streams))
                return [(first_stream, "entry-1", task_fields)]
            # Should never reach here in ephemeral mode
            return []

        mock_redis.xreadgroup_multi.side_effect = xreadgroup_multi_side_effect

        run_worker(worker_config)

        # Runner was called exactly once
        mocks["runner"].run.assert_called_once()
        # Result was published
        results_calls = [
            c for c in mock_redis.xadd_capped.call_args_list if c[0][0] == RESULTS_STREAM
        ]
        assert len(results_calls) == 1
        # pool:done key was set in Redis
        mock_redis.set_ex.assert_called_once_with(
            f"pool:done:{worker_config.worker_id}", "1", ttl=300
        )
        # Only one task read from the stream (no second xreadgroup for normal tasks)
        assert normal_call_count == 1

    def test_ephemeral_worker_sets_pool_done_key(self, mocker, worker_config, sample_task):
        """Ephemeral worker sets pool:done:{worker_id} with TTL 300 on exit."""
        worker_config.ephemeral = True
        mock_redis = self._build_mock_redis()
        mocks = self._setup_run_worker(mocker, worker_config, mock_redis)
        mocks["runner"].run.return_value = _success_runner_result()

        task_fields = sample_task.to_dict()

        def xreadgroup_multi_side_effect(**kwargs):
            streams = kwargs.get("streams", {})
            if any(v == "0" for v in streams.values()):
                return []
            first_stream = next(iter(streams))
            return [(first_stream, "entry-1", task_fields)]

        mock_redis.xreadgroup_multi.side_effect = xreadgroup_multi_side_effect

        run_worker(worker_config)

        mock_redis.set_ex.assert_called_once_with(
            f"pool:done:{worker_config.worker_id}", "1", ttl=300
        )

    def test_ephemeral_worker_survives_pool_done_key_failure(
        self, mocker, worker_config, sample_task, caplog
    ):
        """When set_ex for pool:done fails, the worker still exits gracefully."""
        worker_config.ephemeral = True
        mock_redis = self._build_mock_redis()
        mocks = self._setup_run_worker(mocker, worker_config, mock_redis)
        mocks["runner"].run.return_value = _success_runner_result()
        mock_redis.set_ex.side_effect = ConnectionError("Redis unavailable")

        task_fields = sample_task.to_dict()

        def xreadgroup_multi_side_effect(**kwargs):
            streams = kwargs.get("streams", {})
            if any(v == "0" for v in streams.values()):
                return []
            first_stream = next(iter(streams))
            return [(first_stream, "entry-1", task_fields)]

        mock_redis.xreadgroup_multi.side_effect = xreadgroup_multi_side_effect

        with caplog.at_level(logging.WARNING):
            run_worker(worker_config)

        # Worker still exited (runner was called once, no hang)
        mocks["runner"].run.assert_called_once()

    def test_ephemeral_worker_exits_even_on_publish_failure(
        self, mocker, worker_config, sample_task
    ):
        """Ephemeral worker still exits and sets pool:done even when result publish fails.

        When the result cannot be published, the ephemeral exit path ACKs the
        entry and clears the pending-task marker to prevent an orphaned PEL
        entry and a stale marker blocking re-enqueue (the VM will be destroyed
        so no future drain will ever claim it).
        """
        worker_config.ephemeral = True
        mock_redis = self._build_mock_redis()
        mocks = self._setup_run_worker(mocker, worker_config, mock_redis)
        mocks["runner"].run.return_value = _success_runner_result()
        # Make result publish fail (xadd_capped raises on every attempt)
        mock_redis.xadd_capped.side_effect = ConnectionError("Redis unavailable")
        mocker.patch("orcest.worker.loop.time.sleep")  # avoid real sleeps during retries
        mock_clear = mocker.patch("orcest.worker.loop._clear_pending_task_for_task")

        task_fields = sample_task.to_dict()
        expected_fq_stream = f"{worker_config.redis.key_prefix}:tasks:{worker_config.backend}"

        def xreadgroup_multi_side_effect(**kwargs):
            streams = kwargs.get("streams", {})
            if any(v == "0" for v in streams.values()):
                return []
            first_stream = next(iter(streams))
            return [(first_stream, "entry-1", task_fields)]

        mock_redis.xreadgroup_multi.side_effect = xreadgroup_multi_side_effect

        run_worker(worker_config)

        # Worker still exited (runner was called once, no hang)
        mocks["runner"].run.assert_called_once()
        # pool:done key was still set despite publish failure
        mock_redis.set_ex.assert_called_once()
        assert "pool:done:" in mock_redis.set_ex.call_args[0][0]
        # Entry was ACKed on the ephemeral exit path to prevent orphaned PEL
        mock_redis.xack_raw.assert_called_once_with(expected_fq_stream, CONSUMER_GROUP, "entry-1")
        # Pending-task marker was cleared so orchestrator can re-enqueue
        mock_clear.assert_called()

    def test_ephemeral_worker_exits_on_runner_failure(self, mocker, worker_config, sample_task):
        """Ephemeral worker exits and sets pool:done even when the runner fails."""
        worker_config.ephemeral = True
        mock_redis = self._build_mock_redis()
        mocks = self._setup_run_worker(mocker, worker_config, mock_redis)
        mocks["runner"].run.return_value = _failure_runner_result()

        task_fields = sample_task.to_dict()

        def xreadgroup_multi_side_effect(**kwargs):
            streams = kwargs.get("streams", {})
            if any(v == "0" for v in streams.values()):
                return []
            first_stream = next(iter(streams))
            return [(first_stream, "entry-1", task_fields)]

        mock_redis.xreadgroup_multi.side_effect = xreadgroup_multi_side_effect

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
        mock_redis.set_ex.assert_called_once_with(
            f"pool:done:{worker_config.worker_id}", "1", ttl=300
        )

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
        mock_redis.set_ex.assert_not_called()

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

        def xreadgroup_multi_side_effect(**kwargs):
            nonlocal normal_call_count
            streams = kwargs.get("streams", {})
            if any(v == "0" for v in streams.values()):
                return []
            normal_call_count += 1
            if normal_call_count == 1:
                first_stream = next(iter(streams))
                return [(first_stream, "entry-1", task_fields)]
            # Should never reach here -- ephemeral mode exits after dead-letter
            return []

        mock_redis.xreadgroup_multi.side_effect = xreadgroup_multi_side_effect

        run_worker(worker_config)

        # Runner must NOT have been called (task was dead-lettered)
        mocks["runner"].run.assert_not_called()
        # Dead-letter stream received the task
        dl_calls = [
            c for c in mock_redis.xadd_capped.call_args_list if c[0][0] == DEAD_LETTER_STREAM
        ]
        assert len(dl_calls) == 1
        # pool:done key was set (ephemeral exit)
        mock_redis.set_ex.assert_called_once_with(
            f"pool:done:{worker_config.worker_id}", "1", ttl=300
        )
        # Worker exited after one task (no second xreadgroup for normal tasks)
        assert normal_call_count == 1

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
        fq_stream = f"{worker_config.key_prefixes[0]}:tasks:issue:{worker_config.backend}"

        normal_call_count = [0]

        def xreadgroup_multi_side_effect(**kwargs):
            streams = kwargs.get("streams", {})
            # Drain phase: streams have "0" as entry ID
            if any(v == "0" for v in streams.values()):
                return []
            normal_call_count[0] += 1
            if normal_call_count[0] == 1:
                return [(fq_stream, "entry-1", task_fields)]
            return []

        mock_redis.xreadgroup_multi.side_effect = xreadgroup_multi_side_effect

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

        def xreadgroup_multi_side_effect(**kwargs):
            nonlocal normal_call_count
            streams = kwargs.get("streams", {})
            if any(v == "0" for v in streams.values()):
                return []
            normal_call_count += 1
            if normal_call_count == 1:
                first_stream = next(iter(streams))
                return [(first_stream, "entry-1", task_fields)]
            # After dead-lettering, trigger SIGTERM to exit
            handler = mocks["signal_handlers"].get(signal.SIGTERM)
            if handler:
                handler(signal.SIGTERM, None)
            return []

        mock_redis.xreadgroup_multi.side_effect = xreadgroup_multi_side_effect

        run_worker(worker_config)

        # Runner must NOT have been called (task was dead-lettered)
        mocks["runner"].run.assert_not_called()
        # pool:done was NOT set (not ephemeral)
        mock_redis.set_ex.assert_not_called()
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

        mock_redis.xack_raw.assert_called_once_with("tasks:claude", CONSUMER_GROUP, "entry-42")

    def test_acks_even_when_dead_letter_publish_fails(self, local_worker_config, sample_task):
        """_dead_letter_task ACKs the original entry even if publishing to the
        dead-letter stream raises an exception."""
        mock_redis = MagicMock()
        mock_redis.xadd_capped.side_effect = ConnectionError("Redis unavailable")
        mock_redis.xack_raw.return_value = 1

        _dead_letter_task(
            mock_redis,
            "tasks:claude",
            "entry-99",
            sample_task,
            3,
            logging.getLogger("test"),
        )

        # xack_raw must still be called despite the publish failure
        mock_redis.xack_raw.assert_called_once_with("tasks:claude", CONSUMER_GROUP, "entry-99")

    def test_clears_pending_task_marker(self, local_worker_config, sample_task):
        """_dead_letter_task clears the pending-task marker so the orchestrator
        can re-enqueue work for the resource immediately."""
        mock_redis = MagicMock()
        mock_redis.xadd_capped.return_value = "1-0"
        mock_redis.xack_raw.return_value = 1

        with patch("orcest.worker.loop._clear_pending_task_for_task") as mock_clear:
            _dead_letter_task(
                mock_redis,
                "tasks:claude",
                "entry-42",
                sample_task,
                5,
                logging.getLogger("test"),
            )

            mock_clear.assert_called_once()

    def test_clears_pending_marker_even_when_publish_and_ack_fail(
        self, local_worker_config, sample_task
    ):
        """Pending-task marker is cleared even if both the dead-letter publish
        and ACK fail, so the orchestrator is not blocked for ~95 min."""
        mock_redis = MagicMock()
        mock_redis.xadd_capped.side_effect = ConnectionError("Redis unavailable")
        mock_redis.xack_raw.side_effect = ConnectionError("Redis unavailable")

        with patch("orcest.worker.loop._clear_pending_task_for_task") as mock_clear:
            _dead_letter_task(
                mock_redis,
                "tasks:claude",
                "entry-99",
                sample_task,
                3,
                logging.getLogger("test"),
            )

            mock_clear.assert_called_once()


# ---------------------------------------------------------------------------
# Tests for _publish_result_with_retry
# ---------------------------------------------------------------------------


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
            branch=task.branch,
            summary="done",
            duration_seconds=1,
        )

    def test_succeeds_on_first_attempt(self, sample_task):
        """Returns True and calls xadd_capped once when the first attempt succeeds."""
        mock_redis = MagicMock()
        mock_redis.xadd_capped.return_value = "1-0"
        result = self._make_result(sample_task)

        ok = _publish_result_with_retry(
            mock_redis, result, sample_task, logging.getLogger("test"), "tasks:claude", "1-1"
        )

        assert ok is True
        mock_redis.xadd_capped.assert_called_once_with(
            RESULTS_STREAM, result.to_dict(), maxlen=_STREAM_MAXLEN
        )

    def test_retries_and_succeeds_on_second_attempt(self, sample_task):
        """Returns True when the first attempt fails and the second succeeds."""
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
        abort_event.wait.side_effect = lambda timeout: waited.append(timeout)
        result = self._make_result(sample_task)

        ok = _publish_result_with_retry(
            mock_redis,
            result,
            sample_task,
            logging.getLogger("test"),
            "tasks:claude",
            "1-1",
            abort_event=abort_event,
        )

        assert ok is True
        assert call_count[0] == 2
        # Should have waited once before the second attempt
        assert waited == [_RESULT_PUBLISH_BACKOFF[0]]

    def test_retries_and_succeeds_on_third_attempt(self, sample_task):
        """Returns True when the first two attempts fail and the third succeeds."""
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
        abort_event.wait.side_effect = lambda timeout: waited.append(timeout)
        result = self._make_result(sample_task)

        ok = _publish_result_with_retry(
            mock_redis,
            result,
            sample_task,
            logging.getLogger("test"),
            "tasks:claude",
            "1-1",
            abort_event=abort_event,
        )

        assert ok is True
        assert call_count[0] == 3
        assert waited == [_RESULT_PUBLISH_BACKOFF[0], _RESULT_PUBLISH_BACKOFF[1]]

    def test_all_retries_fail_writes_dead_letter(self, sample_task, monkeypatch):
        """Returns False and writes to DEAD_LETTER_STREAM when all retries fail."""
        mock_redis = MagicMock()

        def xadd_capped(stream, data, **kwargs):
            if stream == RESULTS_STREAM:
                raise ConnectionError("Redis down")
            return "1-0"

        mock_redis.xadd_capped.side_effect = xadd_capped
        abort_event = MagicMock(spec=threading.Event)
        result = self._make_result(sample_task)

        ok = _publish_result_with_retry(
            mock_redis,
            result,
            sample_task,
            logging.getLogger("test"),
            "tasks:claude",
            "entry-42",
            abort_event=abort_event,
        )

        assert ok is False
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

    def test_all_retries_fail_dead_letter_also_fails_returns_false(
        self, sample_task, monkeypatch, caplog
    ):
        """Returns False even when the dead-letter write itself raises."""
        mock_redis = MagicMock()
        mock_redis.xadd_capped.side_effect = ConnectionError("Redis down")
        monkeypatch.setattr("orcest.worker.loop.time.sleep", lambda _: None)
        result = self._make_result(sample_task)

        with caplog.at_level(logging.ERROR):
            ok = _publish_result_with_retry(
                mock_redis,
                result,
                sample_task,
                logging.getLogger("test"),
                "tasks:claude",
                "1-1",
            )

        assert ok is False
        assert any("permanently lost" in r.message for r in caplog.records)


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
