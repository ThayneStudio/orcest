"""Worker main loop: block on Redis streams, acquire lock, run Claude, publish result.

The central worker loop reads tasks from Redis streams via XREADGROUP
(PR tasks with priority, then issue tasks), acquires a distributed lock
per resource, runs Claude, and publishes results back to a results stream
for the orchestrator.
"""

import logging
import signal
import sys
import threading
import time

from orcest.shared.config import WorkerConfig
from orcest.shared.coordination import RedisLock, make_issue_lock_key, make_pr_lock_key
from orcest.shared.logging import setup_logging
from orcest.shared.models import ResultStatus, Task, TaskResult
from orcest.shared.redis_client import RedisClient
from orcest.worker.heartbeat import Heartbeat
from orcest.worker.runner import Runner, RunnerResult, create_runner
from orcest.worker.workspace import Workspace

RESULTS_STREAM = "results"
CONSUMER_GROUP = "workers"
HEARTBEAT_INTERVAL = 60  # seconds; independent of lock TTL to bound orphaned-lock window
_OUTPUT_STREAM_MAXLEN = 2000


def _make_abort_event(*events: threading.Event) -> threading.Event:
    """Return an Event that is set when any of the given events fires.

    Used to combine ``lock_lost`` and ``shutdown_event`` so that either a
    lost heartbeat lock *or* a SIGTERM will interrupt retry-backoff sleeps
    inside ``run_claude``.  Background daemon threads watch each input event
    and set the combined event when any one of them fires.
    """
    combined = threading.Event()
    # Short-circuit if any event is already set.
    for ev in events:
        if ev.is_set():
            combined.set()
            return combined

    def _watch(ev: threading.Event) -> None:
        ev.wait()
        combined.set()

    for ev in events:
        threading.Thread(target=_watch, args=(ev,), daemon=True).start()
    return combined


def run_worker(config: WorkerConfig, stop_event: threading.Event | None = None) -> None:
    """Main worker entry point. Blocks indefinitely.

    Args:
        config: Worker configuration.
        stop_event: Optional event to signal graceful shutdown from outside
            (e.g. from a test harness). When set, the worker exits its loop
            after the current iteration completes.
    """
    logger = setup_logging("worker", config.worker_id)
    redis = RedisClient(config.redis)
    runner = create_runner(config.runner)
    pr_tasks_stream = f"tasks:{config.backend}"
    issue_tasks_stream = f"tasks:issue:{config.backend}"

    # Verify Redis connection
    if not redis.health_check():
        logger.error("Cannot connect to Redis. Exiting.")
        sys.exit(1)

    # Ensure consumer groups exist on both streams
    redis.ensure_consumer_group(pr_tasks_stream, CONSUMER_GROUP)
    redis.ensure_consumer_group(issue_tasks_stream, CONSUMER_GROUP)

    # Drain pending tasks from previous worker lifecycle.
    # If this worker was killed mid-execution, the task was delivered but
    # never ACKed.  Publish a FAILED result so the orchestrator cleans up
    # labels, then ACK to avoid re-processing stale work.
    _drain_pending_tasks(redis, pr_tasks_stream, config, logger)
    _drain_pending_tasks(redis, issue_tasks_stream, config, logger)

    # Graceful shutdown
    shutdown = False
    shutdown_event = threading.Event()

    def handle_signal(signum: int, frame: object) -> None:
        nonlocal shutdown
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        shutdown = True
        shutdown_event.set()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    workspace = Workspace(config.workspace_dir)

    logger.info(
        f"Worker {config.worker_id} started (backend={config.backend}, "
        f"runner={config.runner.type}). Waiting for tasks..."
    )

    while not shutdown and (stop_event is None or not stop_event.is_set()):
        # PR tasks have priority — non-blocking check first
        entries = redis.xreadgroup(
            group=CONSUMER_GROUP,
            consumer=config.worker_id,
            stream=pr_tasks_stream,
            count=1,
            block_ms=None,
        )
        current_stream = pr_tasks_stream
        if not entries:
            if shutdown:
                break
            # No PR work — block on issue stream (5s timeout to recheck PRs)
            entries = redis.xreadgroup(
                group=CONSUMER_GROUP,
                consumer=config.worker_id,
                stream=issue_tasks_stream,
                count=1,
                block_ms=5000,
            )
            current_stream = issue_tasks_stream

        if not entries:
            continue  # Timeout, loop back to check shutdown

        entry_id, fields = entries[0]
        try:
            task = Task.from_dict(fields)
        except (KeyError, ValueError) as e:
            logger.error(
                f"Malformed task entry {entry_id}: {e}; ACKing to skip",
                exc_info=True,
            )
            try:
                redis.xack(current_stream, CONSUMER_GROUP, entry_id)
            except Exception:
                logger.error(
                    f"Failed to ACK malformed entry {entry_id}",
                    exc_info=True,
                )
            continue

        logger.info(
            f"Received task {task.id}: {task.type.value} "
            f"for {task.resource_type} #{task.resource_id}"
        )

        # Try to acquire lock (use resource-type-aware key)
        if task.resource_type == "issue":
            lock_key = make_issue_lock_key(task.repo, task.resource_id)
        else:
            lock_key = make_pr_lock_key(task.repo, task.resource_id)
        ttl = (
            config.runner.timeout * config.runner.max_retries
            + config.runner.retry_backoff * (config.runner.max_retries - 1)
            + 120  # 2-minute safety buffer
        )
        lock = RedisLock(
            redis,
            lock_key,
            ttl=ttl,
            owner=config.worker_id,
        )

        if not lock.acquire():
            logger.warning(f"Lock {lock_key} already held, skipping task {task.id}")
            # ACK the message so it's not redelivered to us
            # (another worker has the lock and presumably the same task)
            try:
                redis.xack(current_stream, CONSUMER_GROUP, entry_id)
            except Exception:
                logger.error(f"Failed to ACK skipped task {task.id}", exc_info=True)
            continue

        logger.info(f"Acquired lock {lock_key}")

        # Start heartbeat; signal lock_lost if the lock cannot be refreshed.
        # Use an explicit interval independent of lock.ttl so the orphaned-lock
        # window stays bounded even when the TTL grows large (see issue #121).
        lock_lost = threading.Event()
        heartbeat = Heartbeat(
            lock,
            interval=HEARTBEAT_INTERVAL,
            logger=logger,
            on_lock_lost=lock_lost.set,
        )
        heartbeat.start()

        # Combine lock_lost and shutdown_event so that either a lost lock *or*
        # a SIGTERM immediately wakes retry-backoff sleeps inside run_claude.
        # Before PR #98 the abort_event was shutdown_event directly; after that
        # refactor it became lock_lost alone, losing the SIGTERM fast-exit path.
        abort_event = _make_abort_event(lock_lost, shutdown_event)
        try:
            result = _execute_task(
                task,
                config,
                runner,
                workspace,
                redis,
                logger,
                abort_event=abort_event,
            )
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
            # safe no-op if lock already expired — release() verifies owner token via Lua
            lock.release()
            if lock_lost.is_set():
                logger.warning(f"Lock {lock_key} was lost during task execution; task aborted")
            else:
                logger.info(f"Released lock {lock_key}")

        # Publish result, then ACK only if publish succeeded.
        # If publish fails, the result is lost — there is no XPENDING
        # reconciliation for RESULTS_STREAM. The task entry stays unACKed so
        # _drain_pending_tasks will publish a FAILED result on the next restart.
        try:
            redis.xadd_capped(RESULTS_STREAM, result.to_dict(), maxlen=_OUTPUT_STREAM_MAXLEN)
            logger.info(f"Published result for task {task.id}: {result.status.value}")
        except Exception:
            logger.error(
                f"Failed to publish result for task {task.id}; result lost — "
                "task remains pending for _drain_pending_tasks on next restart",
                exc_info=True,
            )
            continue

        try:
            redis.xack(current_stream, CONSUMER_GROUP, entry_id)
        except Exception:
            logger.error(
                f"Failed to ACK task {task.id} (will be redelivered)",
                exc_info=True,
            )

    logger.info("Worker shut down cleanly.")


def _drain_pending_tasks(
    redis: RedisClient,
    tasks_stream: str,
    config: WorkerConfig,
    logger: logging.Logger,
) -> None:
    """Drain pending (unACKed) tasks left over from a previous worker lifecycle.

    When a worker is killed mid-execution (e.g. systemd restart), the task
    it was processing remains delivered-but-unACKed in the consumer group.
    On restart, XREADGROUP with ``">"`` skips these entries, so they'd be
    stuck forever — and the orchestrator never receives a result, leaving
    labels orphaned.

    This function reads all pending entries (ID ``"0"``), publishes a FAILED
    result for each so the orchestrator can clean up, and ACKs them.
    """
    drained = 0
    while True:
        entries = redis.xreadgroup(
            group=CONSUMER_GROUP,
            consumer=config.worker_id,
            stream=tasks_stream,
            count=10,
            block_ms=None,
            pending=True,
        )
        if not entries:
            break
        for entry_id, fields in entries:
            drained += 1
            try:
                task = Task.from_dict(fields)
                logger.warning(
                    f"Recovering pending task {task.id} ({task.type.value} "
                    f"for {task.resource_type} #{task.resource_id}) — "
                    f"publishing FAILED result"
                )
                result = TaskResult(
                    task_id=task.id,
                    worker_id=config.worker_id,
                    status=ResultStatus.FAILED,
                    resource_type=task.resource_type,
                    resource_id=task.resource_id,
                    branch=task.branch,
                    summary="Worker restarted mid-execution; task was not completed.",
                    duration_seconds=0,
                )
                try:
                    redis.xadd(RESULTS_STREAM, result.to_dict())
                except Exception:
                    logger.error(
                        f"Failed to publish recovery result for task {task.id}",
                        exc_info=True,
                    )
            except (KeyError, ValueError) as e:
                logger.error(
                    f"Malformed pending entry {entry_id}: {e}; ACKing to discard",
                    exc_info=True,
                )
            try:
                redis.xack(tasks_stream, CONSUMER_GROUP, entry_id)
            except Exception:
                logger.error(
                    f"Failed to ACK pending entry {entry_id}",
                    exc_info=True,
                )
    if drained:
        logger.info(f"Drained {drained} pending task(s) from previous lifecycle")


def _execute_task(
    task: Task,
    config: WorkerConfig,
    runner: Runner,
    workspace: Workspace,
    redis: RedisClient,
    logger: logging.Logger,
    abort_event: threading.Event | None = None,
) -> TaskResult:
    """Execute a single task: clone, run runner, stream output, return result."""
    start = time.monotonic()
    output_stream = f"output:{config.worker_id}"

    try:
        # Publish task start marker (non-critical; don't fail the task)
        try:
            redis.xadd_capped(
                output_stream,
                {
                    "type": "task_start",
                    "task_id": task.id,
                    "resource": f"{task.resource_type} #{task.resource_id}",
                },
                maxlen=_OUTPUT_STREAM_MAXLEN,
            )
        except Exception:
            logger.warning("Failed to publish task_start marker to Redis", exc_info=True)

        # Setup workspace
        logger.info(f"Cloning {task.repo} (branch: {task.branch or 'default'})")
        work_dir = workspace.setup(task.repo, task.branch, task.token)

        output_errors = 0

        def on_output(line: str) -> None:
            nonlocal output_errors
            try:
                redis.xadd_capped(output_stream, {"line": line}, maxlen=_OUTPUT_STREAM_MAXLEN)
            except Exception:
                # Non-critical: don't kill the task over a streaming failure.
                # Log the first occurrence so operators know Redis output
                # streaming is degraded.
                output_errors += 1
                if output_errors == 1:
                    logger.warning(
                        "Failed to publish output line to Redis (further errors suppressed)",
                        exc_info=True,
                    )

        # Run the configured backend
        runner_result: RunnerResult = runner.run(
            prompt=task.prompt,
            work_dir=work_dir,
            token=task.token,
            timeout=config.runner.timeout,
            logger=logger,
            on_output=on_output,
            abort_event=abort_event,
        )

        duration = int(time.monotonic() - start)

        if runner_result.success:
            status = ResultStatus.COMPLETED
        elif runner_result.usage_exhausted:
            status = ResultStatus.USAGE_EXHAUSTED
        else:
            status = ResultStatus.FAILED

        try:
            redis.xadd_capped(
                output_stream,
                {
                    "type": "task_end",
                    "task_id": task.id,
                    "status": status.value,
                },
                maxlen=_OUTPUT_STREAM_MAXLEN,
            )
        except Exception:
            logger.warning("Failed to publish task_end marker to Redis", exc_info=True)

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

        try:
            redis.xadd_capped(
                output_stream,
                {
                    "type": "task_end",
                    "task_id": task.id,
                    "status": ResultStatus.FAILED.value,
                },
                maxlen=_OUTPUT_STREAM_MAXLEN,
            )
        except Exception:
            logger.warning("Failed to publish task_end marker to Redis", exc_info=True)

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
        try:
            workspace.cleanup()
        except Exception:
            logger.warning("Workspace cleanup failed", exc_info=True)
