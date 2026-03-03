"""Stress test utility helpers."""

import threading
import time

import redis

from orcest.shared.coordination import RedisLock, make_pr_lock_key
from orcest.shared.models import ResultStatus, Task, TaskResult
from orcest.shared.redis_client import RedisClient
from orcest.worker.heartbeat import Heartbeat


def simulate_worker(
    worker_id: str,
    redis_client: RedisClient,
    tasks_processed: dict,
    dict_lock: threading.Lock,
    errors: list,
    shutdown_event: threading.Event,
    work_duration: float = 0.01,
):
    """Simulate a worker: consume tasks, acquire locks, do no-op work."""
    consumer_group = "workers"
    tasks_stream = "tasks"
    results_stream = "results"

    redis_client.ensure_consumer_group(tasks_stream, consumer_group)

    while not shutdown_event.is_set():
        try:
            entries = redis_client.xreadgroup(
                group=consumer_group,
                consumer=worker_id,
                stream=tasks_stream,
                count=1,
                block_ms=500,
            )
        except redis.exceptions.ConnectionError:
            continue

        if not entries:
            continue

        entry_id, fields = entries[0]
        task = Task.from_dict(fields)

        lock_key = make_pr_lock_key(task.repo, task.resource_id)
        pr_lock = RedisLock(redis_client, lock_key, ttl=60, owner=worker_id)

        if not pr_lock.acquire():
            redis_client.xack(tasks_stream, consumer_group, entry_id)
            continue

        heartbeat = Heartbeat(pr_lock, interval=pr_lock.ttl / 3)
        heartbeat.start()

        try:
            time.sleep(work_duration)

            with dict_lock:
                if task.id in tasks_processed:
                    errors.append(
                        f"DUPLICATE: task {task.id} processed by both "
                        f"{tasks_processed[task.id]} and {worker_id}"
                    )
                tasks_processed[task.id] = worker_id

            result = TaskResult(
                task_id=task.id,
                worker_id=worker_id,
                status=ResultStatus.COMPLETED,
                resource_type=task.resource_type,
                resource_id=task.resource_id,
                branch=task.branch,
                summary="no-op",
                duration_seconds=0,
            )
            try:
                redis_client.xadd_capped(results_stream, result.to_dict(), maxlen=2000)
            except Exception as exc:
                errors.append(f"Worker {worker_id} failed to publish result: {exc}")
                continue
        finally:
            heartbeat.stop()
            redis_client.xack(tasks_stream, consumer_group, entry_id)
            pr_lock.release()
