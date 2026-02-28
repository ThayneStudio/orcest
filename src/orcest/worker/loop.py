"""Worker main loop: block on Redis stream, acquire lock, run Claude, publish result.

The central worker loop reads tasks from the Redis stream via XREADGROUP,
acquires a distributed lock per PR, runs Claude to produce fixes, and
publishes results back to a results stream for the orchestrator.
"""

import logging
import signal
import sys
import time

from orcest.shared.config import WorkerConfig
from orcest.shared.coordination import RedisLock, make_pr_lock_key
from orcest.shared.logging import setup_logging
from orcest.shared.models import ResultStatus, Task, TaskResult
from orcest.shared.redis_client import RedisClient
from orcest.worker.heartbeat import Heartbeat
from orcest.worker.runner import Runner, RunnerResult, create_runner
from orcest.worker.workspace import Workspace

RESULTS_STREAM = "results"
CONSUMER_GROUP = "workers"


def run_worker(config: WorkerConfig) -> None:
    """Main worker entry point. Blocks indefinitely."""
    logger = setup_logging("worker", config.worker_id)
    redis = RedisClient(config.redis)
    runner = create_runner(config.runner)
    tasks_stream = f"tasks:{config.backend}"

    # Verify Redis connection
    if not redis.health_check():
        logger.error("Cannot connect to Redis. Exiting.")
        sys.exit(1)

    # Ensure consumer group exists
    redis.ensure_consumer_group(tasks_stream, CONSUMER_GROUP)

    # Graceful shutdown
    shutdown = False

    def handle_signal(signum: int, frame: object) -> None:
        nonlocal shutdown
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        shutdown = True

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    workspace = Workspace(config.workspace_dir)

    logger.info(
        f"Worker {config.worker_id} started (backend={config.backend}, "
        f"runner={config.runner.type}). Waiting for tasks..."
    )

    while not shutdown:
        # Block waiting for tasks (5 second timeout to check shutdown flag)
        entries = redis.xreadgroup(
            group=CONSUMER_GROUP,
            consumer=config.worker_id,
            stream=tasks_stream,
            count=1,
            block_ms=5000,
        )

        if not entries:
            continue  # Timeout, loop back to check shutdown

        entry_id, fields = entries[0]
        task = Task.from_dict(fields)

        logger.info(
            f"Received task {task.id}: {task.type.value} "
            f"for {task.resource_type} #{task.resource_id}"
        )

        # Try to acquire lock
        lock_key = make_pr_lock_key(task.resource_id)
        lock = RedisLock(
            redis,
            lock_key,
            ttl=config.runner.timeout + 60,
            owner=config.worker_id,
        )

        if not lock.acquire():
            logger.warning(
                f"Lock {lock_key} already held, skipping task {task.id}"
            )
            # ACK the message so it's not redelivered to us
            # (another worker has the lock and presumably the same task)
            redis.xack(tasks_stream, CONSUMER_GROUP, entry_id)
            continue

        logger.info(f"Acquired lock {lock_key}")

        # Start heartbeat
        heartbeat = Heartbeat(lock, logger=logger)
        heartbeat.start()

        try:
            result = _execute_task(task, config, runner, workspace, logger)
        except BaseException:
            # KeyboardInterrupt, SystemExit, or any other BaseException
            # that _execute_task's except Exception doesn't catch.
            # Ensure heartbeat and lock are cleaned up before re-raising.
            heartbeat.stop()
            lock.release()
            logger.warning(f"Released lock {lock_key} after unexpected interruption")
            raise
        else:
            # Normal path: stop heartbeat and release lock
            heartbeat.stop()
            lock.release()
            logger.info(f"Released lock {lock_key}")

        # Publish result and ACK (only reached on normal execution)
        try:
            redis.xadd(RESULTS_STREAM, result.to_dict())
            logger.info(
                f"Published result for task {task.id}: {result.status.value}"
            )
        except Exception:
            logger.error(
                f"Failed to publish result for task {task.id}", exc_info=True
            )
            # Continue to ACK -- the task can be retried via XPENDING if needed

        redis.xack(tasks_stream, CONSUMER_GROUP, entry_id)

    logger.info("Worker shut down cleanly.")


def _execute_task(
    task: Task,
    config: WorkerConfig,
    runner: Runner,
    workspace: Workspace,
    logger: logging.Logger,
) -> TaskResult:
    """Execute a single task: clone, run runner, return result."""
    start = time.monotonic()

    try:
        # Setup workspace
        logger.info(f"Cloning {task.repo} (branch: {task.branch or 'default'})")
        work_dir = workspace.setup(task.repo, task.branch, task.token)

        # Run the configured backend
        runner_result: RunnerResult = runner.run(
            prompt=task.prompt,
            work_dir=work_dir,
            token=task.token,
            timeout=config.runner.timeout,
            logger=logger,
        )

        duration = int(time.monotonic() - start)

        if runner_result.success:
            status = ResultStatus.COMPLETED
        elif runner_result.usage_exhausted:
            status = ResultStatus.USAGE_EXHAUSTED
        else:
            status = ResultStatus.FAILED

        return TaskResult(
            task_id=task.id,
            worker_id=config.worker_id,
            status=status,
            resource_type=task.resource_type,
            resource_id=task.resource_id,
            branch=task.branch,
            summary=runner_result.summary,
            duration_seconds=duration,
        )

    except Exception as e:
        duration = int(time.monotonic() - start)
        logger.error(f"Task execution failed: {e}", exc_info=True)
        return TaskResult(
            task_id=task.id,
            worker_id=config.worker_id,
            status=ResultStatus.FAILED,
            resource_type=task.resource_type,
            resource_id=task.resource_id,
            branch=task.branch,
            summary=f"Worker exception: {e}",
            duration_seconds=duration,
        )

    finally:
        workspace.cleanup()
