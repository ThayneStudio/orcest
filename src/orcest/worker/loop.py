"""Worker main loop: block on Redis streams, acquire lock, run Claude, publish result.

The central worker loop reads tasks from Redis streams via XREADGROUP
(PR tasks with priority, then issue tasks), acquires a distributed lock
per resource, runs Claude, and publishes results back to a results stream
for the orchestrator.
"""

import logging
import os
import signal
import sys
import threading
import time
from pathlib import Path

import yaml

from orcest.shared.config import WorkerConfig
from orcest.shared.coordination import (
    RedisLock,
    clear_pending_task,
    make_issue_lock_key,
    make_pr_lock_key,
)
from orcest.shared.logging import setup_logging
from orcest.shared.models import DEAD_LETTER_STREAM, ResultStatus, Task, TaskResult, TaskType
from orcest.shared.redis_client import RedisClient
from orcest.worker.heartbeat import Heartbeat
from orcest.worker.runner import Runner, RunnerResult, create_runner
from orcest.worker.workspace import Workspace

RESULTS_STREAM = "results"
CONSUMER_GROUP = "workers"
HEARTBEAT_INTERVAL = 60  # seconds; heartbeat refresh cadence
LOCK_TTL = 3 * HEARTBEAT_INTERVAL  # 180 s — crash orphaned-lock expires within 3 × heartbeat
MAX_DELIVERY_COUNT = 3  # Dead-letter at or after N deliveries; task runs at most N-1 times
_STREAM_MAXLEN = 2000
_RESULT_PUBLISH_RETRIES = 3  # Max attempts to publish a result
_RESULT_PUBLISH_BACKOFF = (1, 2)  # Seconds to sleep before each retry (before attempt 2, 3)
assert len(_RESULT_PUBLISH_BACKOFF) == _RESULT_PUBLISH_RETRIES - 1, (
    "_RESULT_PUBLISH_BACKOFF must have exactly _RESULT_PUBLISH_RETRIES - 1 entries"
)


def _check_gh_credentials(logger: logging.Logger) -> None:
    """Warn if gh is configured with an OAuth token that may attempt refresh writes.

    Under ``ProtectHome=read-only`` (PR #92), gh cannot write an updated token
    back to ``~/.config/gh/hosts.yml``.  OAuth app tokens (prefix ``gho_`` or
    ``ghu_``) are subject to expiry and refresh; fine-grained PATs
    (``github_pat_``) and classic PATs (``ghp_``) are not.

    If the ``GH_TOKEN`` / ``GITHUB_TOKEN`` environment variable is set, gh uses
    that value directly and never writes to ``hosts.yml``, so no check is needed.
    """
    if os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN"):
        # Token supplied via env var — gh won't refresh / write hosts.yml.
        return

    hosts_file = Path.home() / ".config" / "gh" / "hosts.yml"
    if not hosts_file.exists():
        return

    try:
        data = yaml.safe_load(hosts_file.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("Could not read gh credentials file %s: %s", hosts_file, exc, exc_info=True)
        return

    if not isinstance(data, dict):
        return

    # OAuth token prefixes that gh may attempt to refresh by writing hosts.yml.
    _OAUTH_PREFIXES = ("gho_", "ghu_")

    for host, host_cfg in data.items():
        if not isinstance(host_cfg, dict):
            continue
        token = host_cfg.get("oauth_token")
        if not isinstance(token, str):
            continue
        if token.startswith(_OAUTH_PREFIXES):
            logger.warning(
                "gh credential for %r appears to be an OAuth app token "
                "(prefix %r).  Under ProtectHome=read-only, gh cannot "
                "refresh this token by writing to ~/.config/gh/hosts.yml, which "
                "will cause intermittent authentication failures.  "
                "Replace it with a fine-grained PAT (github_pat_…) or classic PAT "
                "(ghp_…) that does not require refresh, or set the GH_TOKEN "
                "environment variable in /opt/orcest/.env.",
                host,
                token[:4],
            )


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
        while not combined.is_set():
            if ev.wait(timeout=0.05):
                combined.set()
                return

    for ev in events:
        threading.Thread(target=_watch, args=(ev,), daemon=True).start()
    return combined


def _clear_pending_task_for_task(redis: RedisClient, task: Task) -> None:
    """Clear the pending-task marker using the task's key_prefix for correct routing.

    When the task carries a key_prefix, use it to ensure the marker is cleared
    in the correct project namespace. Falls back to the redis client's default
    prefix if the task has no key_prefix set.
    """
    if task.key_prefix:
        # Build the fully-qualified pending key directly
        fq_key = f"{task.key_prefix}:pending:{task.resource_type}:{task.repo}:{task.resource_id}"
        redis.delete_raw(fq_key)
    else:
        clear_pending_task(redis, task.repo, task.resource_type, task.resource_id)


def _build_stream_names(
    key_prefixes: list[str], backend: str
) -> tuple[list[str], list[str]]:
    """Build fully-qualified stream names for multi-project reading.

    Returns (pr_streams, issue_streams) where each stream name is
    fully qualified (e.g. ``"myproject:tasks:claude"``).
    """
    pr_streams: list[str] = []
    issue_streams: list[str] = []
    seen_pr: set[str] = set()
    seen_issue: set[str] = set()
    for prefix in key_prefixes:
        fq_prefix = prefix + ":"
        pr_name = f"{fq_prefix}tasks:{backend}"
        issue_name = f"{fq_prefix}tasks:issue:{backend}"
        if pr_name not in seen_pr:
            seen_pr.add(pr_name)
            pr_streams.append(pr_name)
        if issue_name not in seen_issue:
            seen_issue.add(issue_name)
            issue_streams.append(issue_name)
    return pr_streams, issue_streams


def run_worker(config: WorkerConfig, stop_event: threading.Event | None = None) -> None:
    """Main worker entry point. Blocks indefinitely.

    Args:
        config: Worker configuration.
        stop_event: Optional event to signal graceful shutdown from outside
            (e.g. from a test harness). When set, the worker exits its loop
            after the current iteration completes.
    """
    logger = setup_logging("worker", config.worker_id)
    _check_gh_credentials(logger)
    redis = RedisClient(config.redis)
    runner = create_runner(config.runner)

    # Build stream names from key_prefixes for multi-project support
    key_prefixes = config.key_prefixes or [config.redis.key_prefix]
    pr_fq_streams, issue_fq_streams = _build_stream_names(key_prefixes, config.backend)

    # Legacy single-prefix stream names (used for backward-compatible drain)
    pr_tasks_stream = f"tasks:{config.backend}"
    issue_tasks_stream = f"tasks:issue:{config.backend}"

    # Verify Redis connection
    if not redis.health_check():
        logger.error("Cannot connect to Redis. Exiting.")
        sys.exit(1)

    # Ensure consumer groups exist on all streams
    for fq_stream in pr_fq_streams + issue_fq_streams:
        redis.ensure_consumer_group_raw(fq_stream, CONSUMER_GROUP)

    # Drain pending tasks from previous worker lifecycle.
    for fq_stream in pr_fq_streams + issue_fq_streams:
        _drain_pending_tasks_raw(redis, fq_stream, config, logger)

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
        f"runner={config.runner.type}, prefixes={key_prefixes}). Waiting for tasks..."
    )

    while not shutdown and (stop_event is None or not stop_event.is_set()):
        # PR tasks have priority — non-blocking check first
        pr_stream_dict = {s: ">" for s in pr_fq_streams}
        multi_entries = redis.xreadgroup_multi(
            streams=pr_stream_dict,
            group=CONSUMER_GROUP,
            consumer=config.worker_id,
            count=1,
            block=None,
        )
        current_stream: str | None = None
        entry_id: str | None = None
        fields: dict[str, str] | None = None

        if multi_entries:
            current_stream, entry_id, fields = multi_entries[0]
        else:
            if shutdown:
                break
            # No PR work — block on all issue streams (5s timeout to recheck PRs)
            issue_stream_dict = {s: ">" for s in issue_fq_streams}
            multi_entries = redis.xreadgroup_multi(
                streams=issue_stream_dict,
                group=CONSUMER_GROUP,
                consumer=config.worker_id,
                count=1,
                block=5000,
            )
            if multi_entries:
                current_stream, entry_id, fields = multi_entries[0]

        if not current_stream or entry_id is None or fields is None:
            continue  # Timeout, loop back to check shutdown
        try:
            task = Task.from_dict(fields)
        except (KeyError, ValueError) as e:
            logger.error(
                f"Malformed task entry {entry_id}: {e}; ACKing to skip",
                exc_info=True,
            )
            try:
                redis.xack_raw(current_stream, CONSUMER_GROUP, entry_id)
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

        # Dead-letter guard: if this entry has been delivered too many times
        # (result-publish failures leaving it unACKed), route it to the
        # dead-letter stream instead of running Claude again.
        delivery_count = redis.xpending_count_raw(current_stream, CONSUMER_GROUP, entry_id)
        if delivery_count >= MAX_DELIVERY_COUNT:
            _dead_letter_task(redis, current_stream, entry_id, task, delivery_count, logger)
            if config.ephemeral:
                try:
                    redis.set_ex(f"pool:done:{config.worker_id}", "1", ttl=300)
                except Exception:
                    logger.warning("Failed to set pool:done key", exc_info=True)
                logger.info("Ephemeral mode: dead-lettered task, shutting down.")
                shutdown = True
                shutdown_event.set()
            continue


        # Try to acquire lock (use resource-type-aware key)
        if task.resource_type == "issue":
            lock_key = make_issue_lock_key(task.repo, task.resource_id)
        else:
            lock_key = make_pr_lock_key(task.repo, task.resource_id)
        lock = RedisLock(
            redis,
            lock_key,
            ttl=LOCK_TTL,
            owner=config.worker_id,
        )

        if not lock.acquire():
            logger.warning(f"Lock {lock_key} already held, skipping task {task.id}")
            # ACK the message so it's not redelivered to us
            # (another worker has the lock and presumably the same task)
            try:
                redis.xack_raw(current_stream, CONSUMER_GROUP, entry_id)
            except Exception:
                logger.error(f"Failed to ACK skipped task {task.id}", exc_info=True)
            continue

        logger.info(f"Acquired lock {lock_key}")

        # Dead-letter guard: if this entry has been delivered too many times
        # (result-publish failures leaving it unACKed), route it to the
        # dead-letter stream instead of running Claude again.
        delivery_count = redis.xpending_count(current_stream, CONSUMER_GROUP, entry_id)
        if delivery_count >= MAX_DELIVERY_COUNT:
            lock.release()
            _dead_letter_task(redis, current_stream, entry_id, task, delivery_count, logger)
            if config.ephemeral:
                # Ephemeral workers must exit after encountering any task,
                # including dead-lettered ones.  Without this the worker
                # would loop indefinitely on an empty queue until the pool
                # manager's SIGTERM timeout fires, wasting a VM slot.
                try:
                    redis.set_ex(f"pool:done:{config.worker_id}", "1", ttl=300)
                except Exception:
                    logger.warning("Failed to set pool:done key", exc_info=True)
                logger.info("Ephemeral mode: dead-lettered task, shutting down.")
                shutdown = True
                shutdown_event.set()
            continue

        # Start heartbeat; signal lock_lost if the lock cannot be refreshed.
        # LOCK_TTL = 3 * HEARTBEAT_INTERVAL so the lock survives up to 2 missed
        # refreshes; a crashed worker's lock expires within LOCK_TTL (≈ 180 s).
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
        finally:
            # Terminate abort_event watch threads so they don't accumulate
            # across tasks.  Setting lock_lost is idempotent when it was
            # already set by the heartbeat callback.
            lock_lost.set()

        # Publish result with retry + dead-letter fallback; ACK only on success.
        published = _publish_result_with_retry(
            redis,
            result,
            task,
            logger,
            current_stream,
            entry_id,
        )

        if published:
            logger.info(f"Published result for task {task.id}: {result.status.value}")

            try:
                redis.xack_raw(current_stream, CONSUMER_GROUP, entry_id)
            except Exception:
                logger.error(
                    f"Failed to ACK task {task.id} (will be redelivered)",
                    exc_info=True,
                )

            # Clear pending-task marker promptly so the orchestrator can
            # re-enqueue if needed (belt-and-suspenders with the orchestrator's
            # clear in _handle_result).
            try:
                resource_type = task.resource_type
                _clear_pending_task_for_task(redis, task)
            except Exception:
                logger.warning(
                    "Failed to clear pending task marker for "
                    f"{task.resource_type} #{task.resource_id}",
                    exc_info=True,
                )

        # Ephemeral mode: signal pool manager and exit after one task.
        # Exit regardless of publish success — the VM will be destroyed.
        if config.ephemeral:
            # In ephemeral mode, if result publish failed the entry is still
            # unACKed.  ACK it now to prevent a permanently orphaned PEL entry
            # (the VM will be destroyed, so no future drain will ever claim it).
            if not published:
                try:
                    redis.xack_raw(current_stream, CONSUMER_GROUP, entry_id)
                except Exception:
                    logger.error(
                        f"Failed to ACK task {task.id} on ephemeral exit "
                        "(PEL entry will be orphaned)",
                        exc_info=True,
                    )
                # Clear pending-task marker so the orchestrator can re-enqueue.
                # When publish succeeded this was already done above; when it
                # failed the marker would otherwise linger until TTL expiry
                # (~95 min) since the VM is about to be destroyed.
                try:
                    _clear_pending_task_for_task(redis, task)
                except Exception:
                    logger.warning(
                        "Failed to clear pending task marker for "
                        f"{task.resource_type} #{task.resource_id} on ephemeral exit",
                        exc_info=True,
                    )
            try:
                redis.set_ex(f"pool:done:{config.worker_id}", "1", ttl=300)
            except Exception:
                logger.warning("Failed to set pool:done key", exc_info=True)
            logger.info("Ephemeral mode: task complete, shutting down.")
            shutdown = True
            shutdown_event.set()  # Must mirror handle_signal; abort_event watches this
        elif not published:
            try:
                clear_pending_task(redis, task.repo, task.resource_type, task.resource_id)
            except Exception:
                logger.warning(
                    "Failed to clear pending task marker for "
                    f"{task.resource_type} #{task.resource_id} after publish failure",
                    exc_info=True,
                )
            continue

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
            task: Task | None = None
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
                    redis.xadd_capped(RESULTS_STREAM, result.to_dict(), maxlen=_STREAM_MAXLEN)
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
            # Clear the pending-task marker so the orchestrator can
            # re-enqueue promptly.  Without this, the marker lingers
            # until TTL expiry (~95 min with defaults) — especially
            # problematic when the result publish above also failed,
            # since the orchestrator never learns the task ended.
            if task is not None:
                try:
                    clear_pending_task(redis, task.repo, task.resource_type, task.resource_id)
                except Exception:
                    logger.warning(
                        "Failed to clear pending task marker for "
                        f"{task.resource_type} #{task.resource_id} during drain",
                        exc_info=True,
                    )
    if drained:
        logger.info(f"Drained {drained} pending task(s) from previous lifecycle")


def _drain_pending_tasks_raw(
    redis: RedisClient,
    fq_stream: str,
    config: WorkerConfig,
    logger: logging.Logger,
) -> None:
    """Drain pending tasks from a fully-qualified stream name.

    Same logic as _drain_pending_tasks but uses raw (un-prefixed) Redis
    operations for multi-project stream support.
    """
    drained = 0
    while True:
        result = redis.xreadgroup_multi(
            streams={fq_stream: "0"},
            group=CONSUMER_GROUP,
            consumer=config.worker_id,
            count=10,
            block=None,
        )
        if not result:
            break
        for stream_name, entry_id, fields in result:
            drained += 1
            task: Task | None = None
            try:
                task = Task.from_dict(fields)
                logger.warning(
                    f"Recovering pending task {task.id} ({task.type.value} "
                    f"for {task.resource_type} #{task.resource_id}) — "
                    f"publishing FAILED result"
                )
                task_result = TaskResult(
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
                    # Publish result to the correct project's results stream
                    if task.key_prefix:
                        fq_results = f"{task.key_prefix}:{RESULTS_STREAM}"
                        redis.xadd_capped_raw(fq_results, task_result.to_dict(), maxlen=_STREAM_MAXLEN)
                    else:
                        redis.xadd_capped(RESULTS_STREAM, task_result.to_dict(), maxlen=_STREAM_MAXLEN)
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
                redis.xack_raw(fq_stream, CONSUMER_GROUP, entry_id)
            except Exception:
                logger.error(
                    f"Failed to ACK pending entry {entry_id}",
                    exc_info=True,
                )
            if task is not None:
                try:
                    _clear_pending_task_for_task(redis, task)
                except Exception:
                    logger.warning(
                        "Failed to clear pending task marker for "
                        f"{task.resource_type} #{task.resource_id} during drain",
                        exc_info=True,
                    )
    if drained:
        logger.info(f"Drained {drained} pending task(s) from {fq_stream}")


def _dead_letter_task(
    redis: RedisClient,
    tasks_stream: str,
    entry_id: str,
    task: Task,
    delivery_count: int,
    logger: logging.Logger,
) -> None:
    """Route a task that has exceeded MAX_DELIVERY_COUNT to the dead-letter stream.

    Publishes the task payload to DEAD_LETTER_STREAM with metadata explaining
    why it was dead-lettered, then ACKs the original entry so the main stream
    does not stall.  ACK happens even if the dead-letter publish fails so the
    worker can make progress.

    Also clears the pending-task marker so the orchestrator can re-enqueue
    work for this resource immediately rather than waiting for marker TTL
    expiry (~95 min).

    At-least-once delivery caveat: if ``xadd_capped`` succeeds but the
    subsequent ``xack`` fails, the entry remains in the PEL.  The next time
    it is reclaimed its delivery count will still exceed MAX_DELIVERY_COUNT,
    causing ``_dead_letter_task`` to fire again and produce a duplicate entry
    in DEAD_LETTER_STREAM.  Consumers of that stream must therefore
    de-duplicate on ``original_entry_id``.
    """
    try:
        dl_fields = {
            **task.to_dict(),
            "dead_letter_reason": f"Exceeded max delivery count ({MAX_DELIVERY_COUNT})",
            "tasks_stream": tasks_stream,
            "original_entry_id": entry_id,
            "delivery_count": str(delivery_count),
        }
        redis.xadd_capped(DEAD_LETTER_STREAM, dl_fields, maxlen=_STREAM_MAXLEN)
        logger.error(
            f"Task {task.id} ({task.type.value} for {task.resource_type} "
            f"#{task.resource_id}) exceeded max delivery count "
            f"({MAX_DELIVERY_COUNT}); routed to {DEAD_LETTER_STREAM!r}"
        )
    except Exception:
        logger.error(
            f"Failed to publish dead-letter entry for task {task.id}; ACKing anyway",
            exc_info=True,
        )
    try:
        redis.xack_raw(tasks_stream, CONSUMER_GROUP, entry_id)
    except Exception:
        logger.error(
            f"Failed to ACK dead-lettered task {task.id} (entry {entry_id})",
            exc_info=True,
        )
    # Clear the pending-task marker so the orchestrator can re-enqueue
    # promptly.  Dead-lettered tasks never produce a result on RESULTS_STREAM,
    # so the orchestrator's normal _handle_result path never fires; without
    # this the marker lingers until TTL expiry (~95 min with defaults).
    try:
        _clear_pending_task_for_task(redis, task)
    except Exception:
        logger.warning(
            "Failed to clear pending task marker for "
            f"{task.resource_type} #{task.resource_id} during dead-letter",
            exc_info=True,
        )


def _publish_result_with_retry(
    redis: RedisClient,
    result: TaskResult,
    task: Task,
    logger: logging.Logger,
    tasks_stream: str,
    entry_id: str,
) -> bool:
    """Publish a task result to RESULTS_STREAM with exponential backoff retry.

    Attempts up to _RESULT_PUBLISH_RETRIES times, sleeping _RESULT_PUBLISH_BACKOFF
    seconds between consecutive attempts.  If all attempts fail, writes the result
    and full task context to DEAD_LETTER_STREAM for manual recovery.  The dead-letter
    entry includes ``tasks_stream`` and ``original_entry_id`` so that
    ``orcest dead-letters --replay`` can re-enqueue it.

    Returns True if the result was successfully published to RESULTS_STREAM,
    False otherwise (dead-letter write may or may not have succeeded).
    """
    last_exc: Exception | None = None
    for attempt in range(_RESULT_PUBLISH_RETRIES):
        if attempt > 0:
            time.sleep(_RESULT_PUBLISH_BACKOFF[attempt - 1])
        try:
            # Publish to the correct project's results stream
            if task.key_prefix:
                fq_results = f"{task.key_prefix}:{RESULTS_STREAM}"
                redis.xadd_capped_raw(fq_results, result.to_dict(), maxlen=_STREAM_MAXLEN)
            else:
                redis.xadd_capped(RESULTS_STREAM, result.to_dict(), maxlen=_STREAM_MAXLEN)
            return True
        except Exception as exc:
            last_exc = exc
            logger.warning(
                f"Result publish attempt {attempt + 1}/{_RESULT_PUBLISH_RETRIES} "
                f"failed for task {result.task_id}: {exc}"
            )

    # All retries exhausted — send to dead-letter stream for manual recovery.
    logger.error(
        f"All {_RESULT_PUBLISH_RETRIES} result publish attempts failed for task "
        f"{result.task_id}; writing to {DEAD_LETTER_STREAM!r}",
        exc_info=last_exc,
    )
    try:
        dl_fields = {
            **task.to_dict(),
            **result.to_dict(),
            "dead_letter_reason": (
                f"Result publish failed after {_RESULT_PUBLISH_RETRIES} attempts"
            ),
            "tasks_stream": tasks_stream,
            "original_entry_id": entry_id,
        }
        redis.xadd_capped(DEAD_LETTER_STREAM, dl_fields, maxlen=_STREAM_MAXLEN)
        logger.error(
            f"Result for task {result.task_id} written to dead-letter stream "
            f"{DEAD_LETTER_STREAM!r} for manual recovery"
        )
    except Exception:
        logger.error(
            f"Failed to write result for task {result.task_id} to dead-letter stream; "
            "result is permanently lost",
            exc_info=True,
        )
    return False


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
                maxlen=_STREAM_MAXLEN,
            )
        except Exception:
            logger.warning("Failed to publish task_start marker to Redis", exc_info=True)

        # Setup workspace
        logger.info(f"Cloning {task.repo} (branch: {task.branch or 'default'})")
        # For REBASE_PR tasks, skip the automatic rebase so Claude can resolve
        # conflicts itself — the task prompt instructs Claude to rebase.
        setup_base_branch = None if task.type == TaskType.REBASE_PR else task.base_branch
        work_dir = workspace.setup(task.repo, task.branch, task.token, setup_base_branch)

        output_errors = 0

        def on_output(line: str) -> None:
            nonlocal output_errors
            try:
                redis.xadd_capped(output_stream, {"line": line}, maxlen=_STREAM_MAXLEN)
            except Exception:
                # Non-critical: don't kill the task over a streaming failure.
                # Log at error #1, #10, #100, … (powers of ten) so operators
                # see ongoing degradation without flooding the log.
                output_errors += 1
                n = output_errors
                while n % 10 == 0:
                    n //= 10
                if n == 1:
                    logger.warning(
                        f"Failed to publish output line to Redis (error #{output_errors})",
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
            claude_token=task.claude_token,
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
                maxlen=_STREAM_MAXLEN,
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
                maxlen=_STREAM_MAXLEN,
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
