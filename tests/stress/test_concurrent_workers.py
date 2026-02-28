"""Stress tests for concurrent worker task processing.

Each test publishes tasks to a Redis stream, starts N worker threads
that compete to consume them, and verifies correct exactly-once
delivery, result publication, and distributed locking behaviour.

Requires a running Redis instance on localhost:6379.
"""

from __future__ import annotations

import threading
import time

import pytest

from orcest.shared.models import Task, TaskResult, TaskType
from orcest.shared.redis_client import RedisClient
from tests.stress.conftest import simulate_worker


@pytest.mark.stress
class TestConcurrentWorkers:
    """Concurrent worker scenarios against a live Redis instance."""

    # ------------------------------------------------------------------
    # Shared helper
    # ------------------------------------------------------------------

    def _run_scenario(
        self,
        real_redis_client: RedisClient,
        make_real_redis_client,
        num_workers: int,
        num_tasks: int,
        work_duration: float = 0.01,
        resource_id_fn=None,
    ) -> dict[str, str]:
        """Publish tasks, start worker threads, wait, verify.

        Parameters
        ----------
        real_redis_client:
            Pre-configured RedisClient (flushed per-test by the
            fixture).
        make_real_redis_client:
            Factory that creates additional RedisClient instances for
            worker threads.
        num_workers:
            How many worker threads to start.
        num_tasks:
            How many tasks to publish.
        work_duration:
            Seconds each simulated task takes.
        resource_id_fn:
            Optional callable ``(index: int) -> int`` to control the
            ``resource_id`` of each task.  Defaults to ``i + 1``
            (unique PR per task).

        Returns
        -------
        dict mapping task_id -> worker_id for every processed task.
        """
        redis = real_redis_client
        redis.ensure_consumer_group("tasks", "workers")
        redis.ensure_consumer_group("results", "orchestrator")

        if resource_id_fn is None:
            resource_id_fn = lambda i: i + 1  # noqa: E731

        # -- Publish tasks ------------------------------------------------
        tasks: list[Task] = []
        for i in range(num_tasks):
            task = Task.create(
                task_type=TaskType.FIX_CI,
                repo="owner/testrepo",
                token="fake",
                resource_type="pr",
                resource_id=resource_id_fn(i),
                prompt=f"Task {i}",
                branch=f"fix-{i}",
            )
            redis.xadd("tasks", task.to_dict())
            tasks.append(task)

        # -- Shared state -------------------------------------------------
        tasks_processed: dict[str, str] = {}
        dict_lock = threading.Lock()
        errors: list[str] = []
        shutdown = threading.Event()

        # -- Start worker threads -----------------------------------------
        threads: list[threading.Thread] = []
        for w in range(num_workers):
            wc = make_real_redis_client()
            t = threading.Thread(
                target=simulate_worker,
                args=(
                    f"worker-{w}",
                    wc,
                    tasks_processed,
                    dict_lock,
                    errors,
                    shutdown,
                    work_duration,
                ),
            )
            threads.append(t)

        for t in threads:
            t.start()

        # -- Wait for completion ------------------------------------------
        deadline = time.monotonic() + 30
        while len(tasks_processed) < num_tasks and time.monotonic() < deadline:
            time.sleep(0.1)

        shutdown.set()
        for t in threads:
            t.join(timeout=5)

        # -- Assertions ---------------------------------------------------
        assert errors == [], f"Errors: {errors}"
        assert len(tasks_processed) == num_tasks, f"{len(tasks_processed)}/{num_tasks} processed"

        results_count = redis.client.xlen("results")
        assert results_count == num_tasks, f"{results_count}/{num_tasks} results"

        if num_workers > 1 and num_tasks > num_workers:
            workers_used = set(tasks_processed.values())
            assert len(workers_used) >= 2, f"Only {len(workers_used)} workers got tasks"

        return tasks_processed

    # ------------------------------------------------------------------
    # Tests
    # ------------------------------------------------------------------

    def test_single_worker(
        self,
        real_redis_client: RedisClient,
        make_real_redis_client,
    ) -> None:
        """One worker processes 20 tasks sequentially."""
        processed = self._run_scenario(
            real_redis_client,
            make_real_redis_client,
            num_workers=1,
            num_tasks=20,
        )
        workers_used = set(processed.values())
        assert workers_used == {"worker-0"}

    def test_five_workers(
        self,
        real_redis_client: RedisClient,
        make_real_redis_client,
    ) -> None:
        """Five workers share 50 tasks -- all exactly once."""
        self._run_scenario(
            real_redis_client,
            make_real_redis_client,
            num_workers=5,
            num_tasks=50,
        )

    @pytest.mark.timeout(60)
    def test_fifty_workers(
        self,
        real_redis_client: RedisClient,
        make_real_redis_client,
    ) -> None:
        """50 workers share 200 tasks."""
        self._run_scenario(
            real_redis_client,
            make_real_redis_client,
            num_workers=50,
            num_tasks=200,
        )

    def test_more_workers_than_tasks(
        self,
        real_redis_client: RedisClient,
        make_real_redis_client,
    ) -> None:
        """50 workers, only 10 tasks -- idle workers must not error."""
        self._run_scenario(
            real_redis_client,
            make_real_redis_client,
            num_workers=50,
            num_tasks=10,
        )

    @pytest.mark.timeout(120)
    def test_hundred_workers(
        self,
        real_redis_client: RedisClient,
        make_real_redis_client,
    ) -> None:
        """100 workers share 500 tasks."""
        self._run_scenario(
            real_redis_client,
            make_real_redis_client,
            num_workers=100,
            num_tasks=500,
        )

    def test_all_results_arrive(
        self,
        real_redis_client: RedisClient,
        make_real_redis_client,
    ) -> None:
        """After processing, every task_id appears in the results stream."""
        processed = self._run_scenario(
            real_redis_client,
            make_real_redis_client,
            num_workers=20,
            num_tasks=100,
        )

        # Drain the results stream and collect task_ids.
        raw_entries = real_redis_client.client.xrange("results", "-", "+")
        result_task_ids = {
            TaskResult.from_dict(fields).task_id for _entry_id, fields in raw_entries
        }
        expected_task_ids = set(processed.keys())
        assert result_task_ids == expected_task_ids

    def test_locking_prevents_duplicate_pr_work(
        self,
        real_redis_client: RedisClient,
        make_real_redis_client,
    ) -> None:
        """Tasks sharing a PR resource_id are serialised by the lock.

        10 workers compete for 30 tasks mapped to only 3 unique PRs.
        Some tasks will be skipped because another worker already holds
        the lock for that PR.  We verify:

        * No task_id appears twice in the processed dict.
        * Total processed may be fewer than 30 (locked tasks skipped).
        * No errors from the workers.
        """
        redis = real_redis_client
        redis.ensure_consumer_group("tasks", "workers")
        redis.ensure_consumer_group("results", "orchestrator")

        num_workers = 10
        num_tasks = 30

        tasks: list[Task] = []
        for i in range(num_tasks):
            task = Task.create(
                task_type=TaskType.FIX_CI,
                repo="owner/testrepo",
                token="fake",
                resource_type="pr",
                resource_id=(i % 3) + 1,
                prompt=f"Task {i}",
                branch=f"fix-{i}",
            )
            redis.xadd("tasks", task.to_dict())
            tasks.append(task)

        tasks_processed: dict[str, str] = {}
        dict_lock = threading.Lock()
        errors: list[str] = []
        shutdown = threading.Event()

        threads: list[threading.Thread] = []
        for w in range(num_workers):
            wc = make_real_redis_client()
            t = threading.Thread(
                target=simulate_worker,
                args=(
                    f"worker-{w}",
                    wc,
                    tasks_processed,
                    dict_lock,
                    errors,
                    shutdown,
                    0.05,  # longer work to increase lock contention
                ),
            )
            threads.append(t)

        for t in threads:
            t.start()

        # Wait long enough for all tasks to be consumed from the
        # stream (either processed or skipped).
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            pending = redis.client.xpending("tasks", "workers")
            # When pending reaches 0, all tasks have been acked.
            if pending.get("pending", 0) == 0:  # type: ignore[union-attr]
                break
            time.sleep(0.1)

        shutdown.set()
        for t in threads:
            t.join(timeout=5)

        # -- Assertions ---------------------------------------------------
        assert errors == [], f"Errors: {errors}"

        # Some tasks were processed (at least one per PR).
        assert len(tasks_processed) >= 3, (
            f"Expected at least 3 processed, got {len(tasks_processed)}"
        )

        # Results stream matches processed count.
        results_count = redis.client.xlen("results")
        assert results_count == len(tasks_processed), (
            f"results stream has {results_count} entries, "
            f"but {len(tasks_processed)} tasks were processed"
        )
