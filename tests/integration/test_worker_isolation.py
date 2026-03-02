"""Integration test: real worker loop with NoopRunner proves no overlap.

Uses the actual run_worker code with NoopRunner (runner.type="noop")
and backend="claude" to exercise the real lock -> heartbeat -> execute ->
release -> ack lifecycle.

Two test approaches are used:
1. _execute_task with real NoopRunner and real Redis locks (reliable, direct)
2. run_worker in threads with mocked signal/logging/workspace (full lifecycle)
"""

from __future__ import annotations

import logging
import threading
import time
import unittest.mock
from pathlib import Path

import pytest

from orcest.shared.config import RedisConfig, RunnerConfig, WorkerConfig
from orcest.shared.coordination import RedisLock, make_pr_lock_key
from orcest.shared.models import Task, TaskType
from orcest.shared.redis_client import RedisClient
from orcest.worker.heartbeat import Heartbeat
from orcest.worker.loop import CONSUMER_GROUP, RESULTS_STREAM, _execute_task, run_worker
from orcest.worker.noop_runner import NoopRunner
from orcest.worker.workspace import Workspace


@pytest.mark.integration
class TestWorkerIsolation:
    """Prove workers using the real loop can't overlap on the same PR."""

    # ------------------------------------------------------------------
    # Approach 1: Direct _execute_task with real locks and real NoopRunner
    # ------------------------------------------------------------------

    def test_execute_task_no_overlap_same_pr(
        self,
        real_redis_client: RedisClient,
    ) -> None:
        """Multiple threads running _execute_task for the same PR are
        serialized by the Redis lock — no concurrent execution detected.

        This exercises the real NoopRunner, real RedisLock, real Heartbeat,
        and the real _execute_task function from the worker loop.
        """
        redis = real_redis_client

        # Concurrency tracking
        active_count = {"value": 0, "max": 0}
        count_lock = threading.Lock()

        # Create a NoopRunner with instrumented sleep to track concurrency
        noop_duration = 0.15
        runner = NoopRunner(duration=noop_duration)
        original_run = runner.run

        def instrumented_run(prompt, work_dir, token, timeout, logger=None, on_output=None):
            """Wrap NoopRunner.run to track concurrent executions."""
            with count_lock:
                active_count["value"] += 1
                active_count["max"] = max(active_count["max"], active_count["value"])
            try:
                return original_run(
                    prompt=prompt,
                    work_dir=work_dir,
                    token=token,
                    timeout=timeout,
                    logger=logger,
                    on_output=on_output,
                )
            finally:
                with count_lock:
                    active_count["value"] -= 1

        runner.run = instrumented_run  # type: ignore[assignment]

        config = WorkerConfig(
            worker_id="isolation-test",
            workspace_dir="/tmp/orcest-test-isolation",
            runner=RunnerConfig(type="noop", timeout=10, extra={"duration": "0.15"}),
        )
        test_logger = logging.getLogger("test.isolation.execute")

        num_tasks = 10
        num_threads = 5
        pr_number = 42  # All tasks target the same PR

        tasks = [
            Task.create(
                task_type=TaskType.FIX_CI,
                repo="owner/testrepo",
                token="fake",
                resource_type="pr",
                resource_id=pr_number,
                prompt=f"Task {i}",
                branch="fix-branch",
            )
            for i in range(num_tasks)
        ]

        results = []
        results_lock = threading.Lock()
        errors = []

        def worker_fn(worker_id: str, task: Task) -> None:
            """Acquire lock, run _execute_task, release lock."""
            # Each thread gets its own mock workspace to avoid thread-safety
            # issues with MagicMock (its internal call tracking is not
            # thread-safe).
            mock_workspace = unittest.mock.MagicMock(spec=Workspace)
            mock_workspace.setup.return_value = Path("/tmp/fake-workspace")

            lock_key = make_pr_lock_key(task.resource_id)
            lock = RedisLock(redis, lock_key, ttl=30, owner=worker_id)

            if not lock.acquire():
                # Another worker holds the lock — skip (expected behavior)
                return

            heartbeat = Heartbeat(lock, interval=5)
            heartbeat.start()
            try:
                result = _execute_task(task, config, runner, mock_workspace, redis, test_logger)
                with results_lock:
                    results.append((worker_id, task.id, result))
            except Exception as e:
                errors.append(f"{worker_id}: {e}")
            finally:
                heartbeat.stop()
                lock.release()

        # Run threads: each thread picks a task and tries to lock + execute
        barrier = threading.Barrier(num_threads)

        def contending_worker(worker_id: str, assigned_tasks: list[Task]) -> None:
            """Worker that processes its assigned tasks sequentially."""
            try:
                barrier.wait(timeout=10)
            except threading.BrokenBarrierError:
                errors.append(f"{worker_id}: barrier broken (thread start synchronization failed)")
                return
            for task in assigned_tasks:
                worker_fn(worker_id, task)

        # Distribute tasks round-robin to workers
        worker_tasks: dict[str, list[Task]] = {f"w-{i}": [] for i in range(num_threads)}
        for i, task in enumerate(tasks):
            worker_id = f"w-{i % num_threads}"
            worker_tasks[worker_id].append(task)

        threads = [
            threading.Thread(
                target=contending_worker,
                args=(wid, wtasks),
                name=wid,
            )
            for wid, wtasks in worker_tasks.items()
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        hung = [t.name for t in threads if t.is_alive()]
        assert not hung, f"Threads still alive after join timeout: {hung}"

        # Assertions
        assert errors == [], f"Worker errors: {errors}"
        assert active_count["max"] <= 1, (
            f"Concurrent executions on same PR detected! max={active_count['max']}, "
            f"expected at most 1"
        )
        # At least some tasks should have been processed
        assert len(results) >= 1, "No tasks were processed"

    def test_execute_task_different_prs_can_overlap(
        self,
        real_redis_client: RedisClient,
    ) -> None:
        """Tasks for different PRs CAN execute concurrently.

        This is the complement of the isolation test — different PRs should
        NOT block each other.
        """
        redis = real_redis_client

        active_count = {"value": 0, "max": 0}
        count_lock = threading.Lock()

        noop_duration = 0.3
        runner = NoopRunner(duration=noop_duration)
        original_run = runner.run

        def instrumented_run(prompt, work_dir, token, timeout, logger=None, on_output=None):
            with count_lock:
                active_count["value"] += 1
                active_count["max"] = max(active_count["max"], active_count["value"])
            try:
                return original_run(
                    prompt=prompt,
                    work_dir=work_dir,
                    token=token,
                    timeout=timeout,
                    logger=logger,
                    on_output=on_output,
                )
            finally:
                with count_lock:
                    active_count["value"] -= 1

        runner.run = instrumented_run  # type: ignore[assignment]

        config = WorkerConfig(
            worker_id="isolation-test",
            workspace_dir="/tmp/orcest-test-isolation",
            runner=RunnerConfig(type="noop", timeout=10, extra={"duration": "0.3"}),
        )
        test_logger = logging.getLogger("test.isolation.overlap")

        num_threads = 3
        errors = []
        barrier = threading.Barrier(num_threads)

        def worker_fn(worker_id: str, pr_number: int) -> None:
            # Each thread gets its own mock workspace to avoid thread-safety
            # issues with MagicMock (its internal call tracking is not
            # thread-safe).
            mock_workspace = unittest.mock.MagicMock(spec=Workspace)
            mock_workspace.setup.return_value = Path("/tmp/fake-workspace")

            task = Task.create(
                task_type=TaskType.FIX_CI,
                repo="owner/testrepo",
                token="fake",
                resource_type="pr",
                resource_id=pr_number,  # Different PR per worker!
                prompt=f"Task for PR {pr_number}",
                branch=f"fix-pr-{pr_number}",
            )
            lock_key = make_pr_lock_key(task.resource_id)
            lock = RedisLock(redis, lock_key, ttl=30, owner=worker_id)

            if not lock.acquire():
                errors.append(f"{worker_id}: couldn't acquire lock for PR {pr_number}")
                return

            heartbeat = Heartbeat(lock, interval=5)
            heartbeat.start()
            try:
                barrier.wait(timeout=5)  # Synchronize start
                _execute_task(task, config, runner, mock_workspace, redis, test_logger)
            except Exception as e:
                errors.append(f"{worker_id}: {e}")
            finally:
                heartbeat.stop()
                lock.release()

        threads = [
            threading.Thread(target=worker_fn, args=(f"w-{i}", i + 1), name=f"w-{i}")
            for i in range(num_threads)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        hung = [t.name for t in threads if t.is_alive()]
        assert not hung, f"Threads still alive after join timeout: {hung}"

        assert errors == [], f"Worker errors: {errors}"
        # With different PRs, workers SHOULD be able to run concurrently
        assert active_count["max"] >= 2, (
            f"Expected concurrent execution on different PRs, but max={active_count['max']}"
        )

    # ------------------------------------------------------------------
    # Approach 2: Full run_worker with real Redis, NoopRunner, mocked I/O
    # ------------------------------------------------------------------

    def test_run_worker_no_concurrent_execution_same_pr(
        self,
        real_redis_client: RedisClient,
        make_real_redis_client,
    ) -> None:
        """3 real workers via run_worker, 5 tasks for same PR — no overlap.

        Uses the actual run_worker function from loop.py with mocked
        Workspace, setup_logging, and signal.signal (which can't be set
        from non-main threads).
        """
        redis = real_redis_client
        tasks_stream = "tasks:claude"
        results_stream = RESULTS_STREAM

        redis.ensure_consumer_group(tasks_stream, CONSUMER_GROUP)
        redis.ensure_consumer_group(results_stream, "orchestrator")

        # Publish 5 tasks all targeting the same PR
        for i in range(5):
            task = Task.create(
                task_type=TaskType.FIX_CI,
                repo="owner/testrepo",
                token="fake",
                resource_type="pr",
                resource_id=1,  # Same PR!
                prompt=f"Task {i}",
                branch="fix-branch",
            )
            redis.xadd(tasks_stream, task.to_dict())

        # Track concurrency via instrumented sleep in noop_runner
        active_count = {"value": 0, "max": 0}
        count_lock = threading.Lock()
        original_sleep = time.sleep

        def instrumented_sleep(duration):
            """Track concurrent executions during noop sleep."""
            with count_lock:
                active_count["value"] += 1
                active_count["max"] = max(active_count["max"], active_count["value"])
            try:
                original_sleep(duration)
            finally:
                with count_lock:
                    active_count["value"] -= 1

        # Build worker configs
        num_workers = 3
        configs = []
        for i in range(num_workers):
            rc = make_real_redis_client()
            conn_kwargs = rc.client.connection_pool.connection_kwargs
            parsed = {
                "host": conn_kwargs.get("host", "localhost"),
                "port": conn_kwargs.get("port", 6379),
                "db": conn_kwargs.get("db", 15),
                "password": conn_kwargs.get("password"),
            }
            cfg = WorkerConfig(
                redis=RedisConfig(
                    host=parsed["host"],
                    port=parsed["port"],
                    db=parsed["db"],
                    password=parsed["password"],
                ),
                worker_id=f"test-worker-{i}",
                workspace_dir="/tmp/orcest-test-isolation",
                backend="claude",
                runner=RunnerConfig(
                    type="noop",
                    timeout=10,
                    extra={"duration": "0.1"},
                ),
            )
            configs.append(cfg)

        # Patchers for things that don't work in threads or need mocking.
        # Use side_effect (not return_value) so each thread gets its own
        # MagicMock instance -- MagicMock's internal call tracking is not
        # thread-safe.
        workspace_patcher = unittest.mock.patch("orcest.worker.loop.Workspace")
        mock_ws_cls = workspace_patcher.start()

        def _make_mock_ws(*args, **kwargs):
            ws = unittest.mock.MagicMock(spec=Workspace)
            ws.setup.return_value = Path("/tmp/fake-workspace")
            return ws

        mock_ws_cls.side_effect = _make_mock_ws

        logging_patcher = unittest.mock.patch("orcest.worker.loop.setup_logging")
        mock_logging = logging_patcher.start()
        mock_logging.return_value = logging.getLogger("test.isolation.runworker")

        signal_patcher = unittest.mock.patch("orcest.worker.loop.signal.signal")
        signal_patcher.start()

        noop_sleep_patcher = unittest.mock.patch(
            "orcest.worker.noop_runner.time.sleep",
            side_effect=instrumented_sleep,
        )
        noop_sleep_patcher.start()

        try:
            # Run workers in threads
            errors: list[str] = []

            def run_with_client(cfg: WorkerConfig) -> None:
                try:
                    run_worker(cfg)
                except SystemExit:
                    # run_worker calls sys.exit(1) on Redis health check
                    # failure. SystemExit is a BaseException, not Exception,
                    # so catch it explicitly to avoid silent thread death.
                    errors.append(f"{cfg.worker_id}: sys.exit called (Redis health check failed?)")
                except Exception as e:
                    errors.append(f"{cfg.worker_id}: {e}")

            threads = []
            for cfg in configs:
                t = threading.Thread(
                    target=run_with_client, args=(cfg,), daemon=True, name=cfg.worker_id,
                )
                threads.append(t)

            for t in threads:
                t.start()

            # Wait for tasks to be consumed. Workers that can't acquire the
            # lock will skip (ACK without result), so we just need enough
            # time for all 5 tasks to be read and either processed or skipped.
            # With 0.1s noop sleep, this is very fast.
            #
            # IMPORTANT: use original_sleep for the test's own waits.
            # The noop_sleep_patcher patches time.sleep as imported in the
            # noop_runner module; using original_sleep here avoids any
            # ambiguity about which sleep is being called.
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                results_count = redis.client.xlen(results_stream)
                if results_count >= 1:
                    # At least one task processed; give others time to skip.
                    original_sleep(1)
                    break
                original_sleep(0.2)
        finally:
            # Cleanup patchers
            workspace_patcher.stop()
            logging_patcher.stop()
            signal_patcher.stop()
            noop_sleep_patcher.stop()

        # Workers that can't acquire the PR lock correctly skip the task
        # (ACK without processing). This IS the locking mechanism working:
        # only one worker processes a task for a given PR at a time, and
        # redundant tasks for the same PR are discarded.
        results_count = redis.client.xlen(results_stream)
        assert results_count >= 1, (
            f"Expected at least 1 result, got {results_count}"
        )
        assert errors == [], f"Worker errors: {errors}"
        assert active_count["max"] <= 1, (
            f"Concurrent executions detected! max={active_count['max']}"
        )

    # ------------------------------------------------------------------
    # Approach 2b: run_worker skips locked tasks (ACK without processing)
    # ------------------------------------------------------------------

    def test_run_worker_skips_locked_tasks(
        self,
        real_redis_client: RedisClient,
        make_real_redis_client,
    ) -> None:
        """When a PR lock is already held, run_worker ACKs the task
        without executing it (no duplicate work, no stall).
        """
        redis = real_redis_client
        tasks_stream = "tasks:claude"
        results_stream = RESULTS_STREAM

        redis.ensure_consumer_group(tasks_stream, CONSUMER_GROUP)
        redis.ensure_consumer_group(results_stream, "orchestrator")

        pr_number = 77

        # Pre-acquire the lock for this PR (simulating another worker)
        lock_key = make_pr_lock_key(pr_number)
        blocker_lock = RedisLock(redis, lock_key, ttl=60, owner="blocker-worker")
        assert blocker_lock.acquire() is True

        # Publish a task for the locked PR
        task = Task.create(
            task_type=TaskType.FIX_CI,
            repo="owner/testrepo",
            token="fake",
            resource_type="pr",
            resource_id=pr_number,
            prompt="This should be skipped",
            branch="fix-branch",
        )
        redis.xadd(tasks_stream, task.to_dict())

        # Also publish a task for a different PR (should succeed)
        task2 = Task.create(
            task_type=TaskType.FIX_CI,
            repo="owner/testrepo",
            token="fake",
            resource_type="pr",
            resource_id=pr_number + 1,  # Different PR
            prompt="This should succeed",
            branch="fix-branch-2",
        )
        redis.xadd(tasks_stream, task2.to_dict())

        # Build config for a single worker
        rc = make_real_redis_client()
        conn_kwargs = rc.client.connection_pool.connection_kwargs
        parsed = {
            "host": conn_kwargs.get("host", "localhost"),
            "port": conn_kwargs.get("port", 6379),
            "db": conn_kwargs.get("db", 15),
            "password": conn_kwargs.get("password"),
        }
        cfg = WorkerConfig(
            redis=RedisConfig(
                host=parsed["host"],
                port=parsed["port"],
                db=parsed["db"],
                password=parsed["password"],
            ),
            worker_id="skip-test-worker",
            workspace_dir="/tmp/orcest-test-isolation",
            backend="claude",
            runner=RunnerConfig(
                type="noop",
                timeout=10,
                extra={"duration": "0.01"},
            ),
        )

        workspace_patcher = unittest.mock.patch("orcest.worker.loop.Workspace")
        mock_ws_cls = workspace_patcher.start()

        def _make_mock_ws(*args, **kwargs):
            ws = unittest.mock.MagicMock(spec=Workspace)
            ws.setup.return_value = Path("/tmp/fake-workspace")
            return ws

        mock_ws_cls.side_effect = _make_mock_ws

        logging_patcher = unittest.mock.patch("orcest.worker.loop.setup_logging")
        mock_logging = logging_patcher.start()
        mock_logging.return_value = logging.getLogger("test.isolation.skip")

        signal_patcher = unittest.mock.patch("orcest.worker.loop.signal.signal")
        signal_patcher.start()

        try:
            errors: list[str] = []

            def run_worker_thread(config: WorkerConfig) -> None:
                try:
                    run_worker(config)
                except SystemExit:
                    errors.append("sys.exit called (Redis health check failed?)")
                except Exception as e:
                    errors.append(str(e))

            t = threading.Thread(
                target=run_worker_thread, args=(cfg,), daemon=True, name="skip-test-worker",
            )
            t.start()

            # Wait for the unlocked task to produce a result
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                results_count = redis.client.xlen(results_stream)
                if results_count >= 1:
                    break
                time.sleep(0.2)

            # Allow time for the worker to also process (skip) the locked task
            time.sleep(1)
        finally:
            workspace_patcher.stop()
            logging_patcher.stop()
            signal_patcher.stop()

            # Release the blocker lock
            blocker_lock.release()

        # The results stream should have exactly 1 result (the unlocked task)
        results_count = redis.client.xlen(results_stream)
        assert results_count == 1, (
            f"Expected exactly 1 result (locked task skipped), got {results_count}"
        )

        # Both tasks should be ACKed (no pending entries)
        pending = redis.client.xpending(tasks_stream, CONSUMER_GROUP)
        assert pending["pending"] == 0, (
            f"Expected 0 pending tasks (both ACKed), got {pending['pending']}"
        )

        assert errors == [], f"Worker errors: {errors}"
