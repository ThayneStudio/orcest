"""Worker main loop: block on Redis streams, acquire lock, run Claude, publish result.

The central worker loop reads tasks from Redis streams via XREADGROUP
(PR tasks with priority, then issue tasks), acquires a distributed lock
per resource, runs Claude, and publishes results back to a results stream
for the orchestrator.
"""

import json
import logging
import os
import signal
import sys
import threading
import time
from pathlib import Path

import redis as redis_py
import yaml

from orcest.orchestrator import gh
from orcest.orchestrator.pr_ops import clear_attempts_if_head_sha
from orcest.shared.config import WorkerConfig
from orcest.shared.coordination import (
    RedisLock,
    clear_pending_task_if_matches,
    make_issue_lock_key,
    make_pr_lock_key,
    parse_pending_task_metadata,
)
from orcest.shared.logging import setup_logging
from orcest.shared.models import (
    CONSUMER_GROUP,
    DEAD_LETTER_STREAM,
    TRANSIENT_SUMMARY_PREFIX,
    ResultStatus,
    Task,
    TaskResult,
)
from orcest.shared.redis_client import RedisClient
from orcest.worker.heartbeat import Heartbeat
from orcest.worker.runner import (
    PROVIDER_REGISTRY,
    Runner,
    RunnerResult,
    create_runner,
    get_unsupported_reason,
)
from orcest.worker.workspace import Workspace, WorkspaceError

RESULTS_STREAM = "results"
HEARTBEAT_INTERVAL = 60  # seconds; heartbeat refresh cadence
LOCK_TTL = 3 * HEARTBEAT_INTERVAL  # 180 s — crash orphaned-lock expires within 3 × heartbeat
MAX_DELIVERY_COUNT = 3  # Dead-letter at or after N deliveries; task runs at most N-1 times
_STREAM_MAXLEN = 2000
_RESULT_PUBLISH_RETRIES = 3  # Max attempts to publish a result
_RESULT_PUBLISH_BACKOFF = (1, 2)  # Seconds to sleep before each retry (before attempt 2, 3)
if len(_RESULT_PUBLISH_BACKOFF) != _RESULT_PUBLISH_RETRIES - 1:
    raise ValueError(
        "_RESULT_PUBLISH_BACKOFF must have exactly _RESULT_PUBLISH_RETRIES - 1 entries"
    )

# Startup Redis-connect retry budget.  Tuned so a brief Redis container restart
# during ``orcest fleet update`` doesn't kill the worker process (which would
# trip systemd's StartLimit and require manual reset-failed).  The
# (1, 2, 4, 8, 10, 10, 10, 10, 10) backoff sums to ~65 s across 10 attempts.
_STARTUP_PING_RETRIES = 10
_STARTUP_PING_BACKOFF = (1, 2, 4, 8, 10, 10, 10, 10, 10)
if len(_STARTUP_PING_BACKOFF) != _STARTUP_PING_RETRIES - 1:
    raise ValueError("_STARTUP_PING_BACKOFF must have exactly _STARTUP_PING_RETRIES - 1 entries")

_FAILURE_CONCLUSIONS = frozenset(
    {"FAILURE", "CANCELLED", "TIMED_OUT", "ACTION_REQUIRED", "STALE", "STARTUP_FAILURE"}
)
_CI_DECISIONS = frozenset({"ci_failure"})
_REVIEW_DECISIONS = frozenset({"changes_requested", "followup_threads"})


def _runner_for_task(task: Task, config: WorkerConfig, fallback: Runner) -> Runner:
    """Return the Runner instance to execute ``task``.

    Dispatch policy:
      - When ``task.provider`` matches the worker's configured runner type
        (the common case — a Claude worker receiving a Claude task), use
        the pre-instantiated fallback. Avoids per-task instantiation and
        preserves the contract that tests inject a runner via the fallback.
      - When the provider differs and is registered in PROVIDER_REGISTRY
        with a ``runner_cls``, instantiate that class fresh.
      - Otherwise (unknown provider, or registered without a ``runner_cls``),
        fall back. The early-reject in the main loop already filters
        genuinely unknown providers before dispatch.
    """
    if task.provider == config.runner.type:
        return fallback
    recipe = PROVIDER_REGISTRY.get(task.provider)
    if recipe is None or recipe.runner_cls is None:
        return fallback
    return recipe.runner_cls(
        max_retries=config.runner.max_retries,
        retry_backoff=config.runner.retry_backoff,
        model=config.runner.model,
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


def _wait_for_redis(redis: RedisClient, logger: logging.Logger) -> bool:
    """Ping Redis with exponential backoff so a brief outage doesn't kill the worker.

    During ``orcest fleet update`` the Redis container restarts briefly.  Without
    this loop a single failed ``health_check()`` makes the worker exit(1), and
    after ~5 quick failures systemd's StartLimit trips and the unit must be
    reset manually on every worker VM.  Retrying for ~60 s in-process keeps
    the worker patient enough to ride out a normal deploy, while still falling
    through to exit(1) for a genuinely down Redis (so systemd's outer retry
    logic, hardened in cloud_init.py, can take over).

    Returns True if Redis became reachable, False if all attempts failed.
    Logs each attempt at INFO so operators can tell the worker is patient,
    not stuck.
    """
    for attempt in range(_STARTUP_PING_RETRIES):
        if redis.health_check():
            if attempt > 0:
                logger.info("Redis reachable after %d attempt(s)", attempt + 1)
            return True
        if attempt < _STARTUP_PING_RETRIES - 1:
            sleep_s = _STARTUP_PING_BACKOFF[attempt]
            logger.info(
                "Redis ping failed (attempt %d/%d); retrying in %ds",
                attempt + 1,
                _STARTUP_PING_RETRIES,
                sleep_s,
            )
            time.sleep(sleep_s)
    return False


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
        _clear_raw_pending_task_if_matches(redis, fq_key, task.id)
    else:
        clear_pending_task_if_matches(
            redis, task.repo, task.resource_type, task.resource_id, task.id
        )


def _clear_raw_pending_task_if_matches(redis: RedisClient, fq_key: str, task_id: str) -> bool:
    while True:
        pipe = redis.client.pipeline()
        try:
            pipe.watch(fq_key)
            raw = pipe.get(fq_key)
            raw_str = str(raw) if raw is not None else None
            metadata = parse_pending_task_metadata(raw_str)
            if metadata is None or metadata.task_id != task_id:
                pipe.unwatch()
                return False
            pipe.multi()
            pipe.delete(fq_key)
            pipe.execute()
            return True
        except redis_py.WatchError:
            continue
        finally:
            pipe.reset()


def _clear_task_attempt_reservation(redis: RedisClient, task: Task) -> None:
    """Clear the attempt reservation when no result can reach the orchestrator."""
    if task.resource_type == "issue":
        key = f"issue:{task.repo}:{task.resource_id}:attempts"
        if task.key_prefix:
            redis.delete_raw(f"{task.key_prefix}:{key}")
        else:
            redis.delete(key)
        return

    if task.key_prefix:
        project_redis = RedisClient.from_client(redis.client, key_prefix=task.key_prefix)
    else:
        project_redis = redis
    clear_attempts_if_head_sha(
        project_redis,
        task.repo,
        task.resource_id,
        task.snapshot_head_sha or "",
    )


def _early_reject_unsupported_provider(
    task: Task,
    provider: str,
    config: WorkerConfig,
    redis: RedisClient,
    logger: logging.Logger,
    current_stream: str,
    entry_id: str,
) -> None:
    """Publish a clean permanent FAILED for an unknown/missing provider and ACK.

    This is the early graceful reject path (before lock, heartbeat, clone, or
    any runner work). The orchestrator will see a non-transient failure with
    an actionable summary telling operators to rebake the worker image.
    Credentials are never logged or leaked because we use the redacted path
    only for DL (if publish fails) and TaskResult itself contains no secrets.
    """
    summary = f"Rebake worker image to include {provider} CLI"
    result = _task_result(
        task,
        config,
        ResultStatus.FAILED,
        task.branch,
        summary,
        0,
    )
    _publish_result_with_retry(
        redis,
        result,
        task,
        logger,
        current_stream,
        entry_id,
    )
    try:
        redis.xack_raw(current_stream, CONSUMER_GROUP, entry_id)
    except Exception:
        logger.error(
            f"Failed to ACK unsupported-provider task {task.id} (entry {entry_id})",
            exc_info=True,
        )
    try:
        _clear_pending_task_for_task(redis, task)
        _clear_task_attempt_reservation(redis, task)
    except Exception:
        logger.warning(
            "Failed to clear pending-task/attempt markers for unsupported "
            f"provider task {task.resource_type} #{task.resource_id}",
            exc_info=True,
        )
    logger.info(f"Early graceful reject for task {task.id} (provider={provider})")


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

    # Shared task stream names (all projects publish to the same streams)
    pr_stream = f"tasks:{config.backend}"
    issue_stream = f"tasks:issue:{config.backend}"
    # Fully-qualified names for raw Redis operations
    if config.redis.key_prefix:
        pr_fq = f"{config.redis.key_prefix}:{pr_stream}"
        issue_fq = f"{config.redis.key_prefix}:{issue_stream}"
    else:
        pr_fq = pr_stream
        issue_fq = issue_stream

    # Verify Redis connection with retry budget so a brief Redis restart
    # (e.g. during ``orcest fleet update``) doesn't kill the worker.
    if not _wait_for_redis(redis, logger):
        logger.error(
            "Redis unreachable after %d retries (~%ds); exiting for systemd to handle",
            _STARTUP_PING_RETRIES,
            sum(_STARTUP_PING_BACKOFF),
        )
        sys.exit(1)

    # Ensure consumer groups exist on shared streams
    redis.ensure_consumer_group(pr_stream, CONSUMER_GROUP)
    redis.ensure_consumer_group(issue_stream, CONSUMER_GROUP)

    # Drain pending tasks from previous worker lifecycle.
    _drain_pending_tasks_raw(redis, pr_fq, config, logger)
    _drain_pending_tasks_raw(redis, issue_fq, config, logger)

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
        f"runner={config.runner.type}, streams={pr_fq},{issue_fq}). Waiting for tasks..."
    )

    while not shutdown and (stop_event is None or not stop_event.is_set()):
        # PR tasks have priority — non-blocking check first
        pr_entries = redis.xreadgroup(
            group=CONSUMER_GROUP,
            consumer=config.worker_id,
            stream=pr_stream,
            count=1,
            block_ms=None,
        )
        current_stream: str | None = None
        entry_id: str | None = None
        fields: dict[str, str] | None = None

        if pr_entries:
            entry_id, fields = pr_entries[0]
            current_stream = pr_fq
        else:
            if shutdown:
                break
            # No PR work — block on issue stream (5s timeout to recheck PRs)
            issue_entries = redis.xreadgroup(
                group=CONSUMER_GROUP,
                consumer=config.worker_id,
                stream=issue_stream,
                count=1,
                block_ms=5000,
            )
            if issue_entries:
                entry_id, fields = issue_entries[0]
                current_stream = issue_fq

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

        # === Early multi-provider dispatch (Task 6) ===
        # Performed *immediately* after parse, before *any* lock acquisition,
        # delivery-count DL logic, heartbeat, workspace clone, or runner work.
        # This guarantees old worker images fail cleanly (non-transient FAILED)
        # when they encounter a task for a provider they do not have baked in.
        # The registry + binary check are strictly local to the worker image.
        unsupported = get_unsupported_reason(task.provider)
        if unsupported:
            logger.warning(
                "Task %s for provider=%s is unsupported on this worker image (%s); "
                "early graceful reject (permanent FAILED, no runner reached)",
                task.id,
                task.provider,
                unsupported,
            )
            _early_reject_unsupported_provider(
                task, task.provider, config, redis, logger, current_stream, entry_id
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

        # Try to acquire lock (use resource-type-aware key).
        # When the task carries a key_prefix (multi-project mode), construct
        # the fully-qualified lock key so the worker writes the lock under the
        # same prefix that the orchestrator checks.  Without this, the worker
        # would write e.g. "orcest:lock:pr:…" while the orchestrator looks for
        # "myproject:lock:pr:…" — different Redis keys.
        if task.resource_type == "issue":
            lock_key = make_issue_lock_key(task.repo, task.resource_id)
        else:
            lock_key = make_pr_lock_key(task.repo, task.resource_id)

        if task.key_prefix:
            fq_lock_key = f"{task.key_prefix}:{lock_key}"
            lock = RedisLock(
                redis,
                fq_lock_key,
                ttl=LOCK_TTL,
                owner=config.worker_id,
                raw_key=True,
            )
        else:
            lock = RedisLock(
                redis,
                lock_key,
                ttl=LOCK_TTL,
                owner=config.worker_id,
            )

        if not lock.acquire():
            logger.warning(f"Lock {lock.key} already held, skipping task {task.id}")
            # The stream entry is being discarded, so clear only this task's
            # coordination markers. Otherwise a matching pending marker can
            # strand the PR/issue until TTL even though the task was ACKed.
            try:
                _clear_pending_task_for_task(redis, task)
                _clear_task_attempt_reservation(redis, task)
            except Exception:
                logger.warning(
                    "Failed to clear skipped task coordination state for "
                    f"{task.resource_type} #{task.resource_id}",
                    exc_info=True,
                )
            try:
                redis.xack_raw(current_stream, CONSUMER_GROUP, entry_id)
            except Exception:
                logger.error(f"Failed to ACK skipped task {task.id}", exc_info=True)
            continue

        logger.info(f"Acquired lock {lock.key}")

        # Dead-letter guard: if this entry has been delivered too many times
        # (result-publish failures leaving it unACKed), route it to the
        # dead-letter stream instead of running Claude again.
        delivery_count = redis.xpending_count_raw(current_stream, CONSUMER_GROUP, entry_id)
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
            logger.warning(f"Released lock {lock.key} after unexpected interruption")
            raise
        else:
            # Normal path: stop heartbeat and release lock
            heartbeat.stop()
            # safe no-op if lock already expired — release() verifies owner token via Lua
            lock.release()
            if lock_lost.is_set():
                logger.warning(f"Lock {lock.key} was lost during task execution; task aborted")
                result = _task_result(
                    task,
                    config,
                    ResultStatus.STALE,
                    task.branch,
                    "Worker lost the Redis lock before publishing; dropping task result.",
                    result.duration_seconds,
                    rate_limit_resets_at=result.rate_limit_resets_at,
                )
            else:
                logger.info(f"Released lock {lock.key}")
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
            abort_event=shutdown_event,
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
                    _clear_task_attempt_reservation(redis, task)
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
                _clear_pending_task_for_task(redis, task)
                _clear_task_attempt_reservation(redis, task)
            except Exception:
                logger.warning(
                    "Failed to clear pending task marker for "
                    f"{task.resource_type} #{task.resource_id} after publish failure",
                    exc_info=True,
                )
            continue

    logger.info("Worker shut down cleanly.")


def _drain_pending_tasks_raw(
    redis: RedisClient,
    fq_stream: str,
    config: WorkerConfig,
    logger: logging.Logger,
) -> None:
    """Drain pending tasks from a fully-qualified stream name.

    Uses raw (un-prefixed) Redis operations since the stream name is
    already fully qualified (e.g. ``orcest:tasks:claude``).
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
            recovery_result_published = False
            try:
                task = Task.from_dict(fields)
                logger.warning(
                    f"Recovering pending task {task.id} ({task.type.value} "
                    f"for {task.resource_type} #{task.resource_id}) — "
                    f"publishing FAILED result"
                )
                task_result = _task_result(
                    task,
                    config,
                    ResultStatus.FAILED,
                    task.branch,
                    (
                        f"{TRANSIENT_SUMMARY_PREFIX}"
                        "Worker restarted mid-execution; task was not completed."
                    ),
                    0,
                )
                try:
                    # Publish result to the correct project's results stream
                    if task.key_prefix:
                        fq_results = f"{task.key_prefix}:{RESULTS_STREAM}"
                        redis.xadd_capped_raw(
                            fq_results, task_result.to_dict(), maxlen=_STREAM_MAXLEN
                        )
                    else:
                        redis.xadd_capped(
                            RESULTS_STREAM, task_result.to_dict(), maxlen=_STREAM_MAXLEN
                        )
                    recovery_result_published = True
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
                    if not recovery_result_published:
                        _clear_task_attempt_reservation(redis, task)
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
        # Use safe projection so credentials never land in the dead-letter stream
        # (systematic redaction layer, Task 2 / security review).
        dl_fields = {
            **task.to_safe_dict(),
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
        _clear_task_attempt_reservation(redis, task)
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
    abort_event: threading.Event | None = None,
) -> bool:
    """Publish a task result to RESULTS_STREAM with exponential backoff retry.

    Attempts up to _RESULT_PUBLISH_RETRIES times, sleeping _RESULT_PUBLISH_BACKOFF
    seconds between consecutive attempts.  If all attempts fail, writes the result
    and full task context to DEAD_LETTER_STREAM for manual recovery.  The dead-letter
    entry includes ``tasks_stream`` and ``original_entry_id`` so that
    ``orcest dead-letters --replay`` can re-enqueue it.

    Returns True if the result was successfully published to RESULTS_STREAM,
    False if all attempts fail (dead-letter write may or may not have succeeded)
    or if ``abort_event`` is set during a backoff wait (no dead-letter written).
    """
    last_exc: Exception | None = None
    # Callers that omit abort_event get a fresh, never-set Event, which means
    # _abort.wait() will block for the full real backoff duration on each retry.
    _abort = abort_event if abort_event is not None else threading.Event()
    for attempt in range(_RESULT_PUBLISH_RETRIES):
        if attempt > 0:
            if _abort.wait(timeout=_RESULT_PUBLISH_BACKOFF[attempt - 1]):
                return False
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
        # Use safe projection so credentials never land in the dead-letter stream
        # (systematic redaction layer, Task 2 / security review). Result dicts
        # contain no secrets, only the task part is redacted.
        dl_fields = {
            **task.to_safe_dict(),
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


def _task_result(
    task: Task,
    config: WorkerConfig,
    status: ResultStatus,
    branch: str | None,
    summary: str,
    duration_seconds: int,
    rate_limit_resets_at: int = 0,
    needs_human: bool = False,
    needs_human_reason: str = "",
    credential_update: str = "",
) -> TaskResult:
    return TaskResult(
        task_id=task.id,
        worker_id=config.worker_id,
        status=status,
        resource_type=task.resource_type,
        resource_id=task.resource_id,
        branch=branch,
        summary=summary,
        duration_seconds=duration_seconds,
        rate_limit_resets_at=rate_limit_resets_at,
        snapshot_head_sha=task.snapshot_head_sha,
        decision_reason=task.decision_reason,
        snapshot_failed_checks=task.snapshot_failed_checks,
        snapshot_review_thread_ids=task.snapshot_review_thread_ids,
        snapshot_review_thread_fingerprints=task.snapshot_review_thread_fingerprints,
        needs_human=needs_human,
        needs_human_reason=needs_human_reason,
        credential_update=credential_update,
    )


def _failed_check_names(checks: list[dict]) -> set[str]:
    names: set[str] = set()
    for check in checks:
        conclusion = (check.get("conclusion") or "").upper()
        state = (check.get("state") or "").upper()
        failed = conclusion in _FAILURE_CONCLUSIONS or (
            not conclusion and state in ("FAILURE", "ERROR")
        )
        if failed:
            name = (
                check.get("detailsUrl")
                or check.get("details_url")
                or check.get("name")
                or check.get("context")
                or check.get("workflowName")
            )
            if name:
                names.add(str(name))
    return names


def _failed_check_fingerprints(checks: list[dict]) -> set[str]:
    fingerprints: set[str] = set()
    for check in checks:
        conclusion = (check.get("conclusion") or "").upper()
        state = (check.get("state") or "").upper()
        failed = conclusion in _FAILURE_CONCLUSIONS or (
            not conclusion and state in ("FAILURE", "ERROR")
        )
        if not failed:
            continue
        payload = {
            "name": str(check.get("name") or ""),
            "context": str(check.get("context") or ""),
            "workflow_name": str(check.get("workflowName") or ""),
            "details_url": str(check.get("detailsUrl") or check.get("details_url") or ""),
            "target_url": str(check.get("targetUrl") or check.get("target_url") or ""),
        }
        if any(payload.values()):
            fingerprints.add(json.dumps(payload, sort_keys=True, separators=(",", ":")))
    return fingerprints


def _snapshot_failed_checks_still_failing(checks: list[dict], snapshot: list[str]) -> bool:
    snapshot_set = set(snapshot)
    if not snapshot_set:
        return True
    current_fingerprints = _failed_check_fingerprints(checks)
    if current_fingerprints & snapshot_set:
        return True
    return bool(_failed_check_names(checks) & snapshot_set)


def _review_thread_ids(threads: list[dict]) -> set[str]:
    ids: set[str] = set()
    for thread in threads:
        thread_id = thread.get("id") or thread.get("node_id")
        if thread_id:
            ids.add(str(thread_id))
    return ids


def _review_thread_fingerprints(threads: list[dict]) -> set[str]:
    fingerprints: set[str] = set()
    for thread in threads:
        comments = []
        for comment in thread.get("comments") or []:
            comments.append(
                {
                    "id": str(comment.get("id") or comment.get("node_id") or ""),
                    "author": str(comment.get("author") or ""),
                    "body": str(comment.get("body") or ""),
                    "created_at": str(comment.get("createdAt") or comment.get("created_at") or ""),
                    "updated_at": str(comment.get("updatedAt") or comment.get("updated_at") or ""),
                }
            )
        payload = {
            "id": str(thread.get("id") or thread.get("node_id") or ""),
            "path": str(thread.get("path") or ""),
            "line": str(thread.get("line") or ""),
            "comments": comments,
        }
        fingerprints.add(json.dumps(payload, sort_keys=True, separators=(",", ":")))
    return fingerprints


def _validate_pr_task_snapshot(task: Task, logger: logging.Logger) -> tuple[bool, str]:
    """Return (is_stale, reason) for cheap PR snapshot checks."""
    if task.resource_type != "pr":
        return False, ""
    if not task.snapshot_head_sha:
        return True, "PR task has no snapshot head SHA; dropping stale legacy task."

    pr_data = gh.get_pr(task.repo, task.resource_id, task.token)
    current_sha = str(pr_data.get("headRefOid") or "")
    if not current_sha:
        return True, "GitHub PR response did not include headRefOid; dropping stale task."
    if current_sha and current_sha != task.snapshot_head_sha:
        return True, (
            f"PR head changed from {task.snapshot_head_sha} to {current_sha}; dropping stale task."
        )

    if task.decision_reason in _CI_DECISIONS and task.snapshot_failed_checks:
        checks = pr_data.get("statusCheckRollup") or gh.get_ci_status(
            task.repo, task.resource_id, task.token
        )
        if not _snapshot_failed_checks_still_failing(checks, task.snapshot_failed_checks):
            return True, "Captured CI failures are no longer failing; dropping stale task."

    if task.decision_reason in _REVIEW_DECISIONS and task.snapshot_review_thread_ids:
        threads = gh.get_unresolved_review_threads(task.repo, task.resource_id, task.token)
        if task.snapshot_review_thread_fingerprints:
            if not set(task.snapshot_review_thread_fingerprints).issubset(
                _review_thread_fingerprints(threads)
            ):
                return True, "Captured review thread content changed; dropping stale task."
        elif not set(task.snapshot_review_thread_ids).issubset(_review_thread_ids(threads)):
            return True, "Captured review threads are no longer unresolved; dropping stale task."

    if task.decision_reason == "merge_conflict_rebase":
        mergeable = str(pr_data.get("mergeable") or "").upper()
        merge_state = str(pr_data.get("mergeStateStatus") or "").upper()
        if mergeable == "UNKNOWN" and merge_state == "UNKNOWN":
            raise RuntimeError("GitHub mergeability is UNKNOWN; retry snapshot validation later.")
        if mergeable != "CONFLICTING" and merge_state != "DIRTY":
            return True, "PR no longer has merge conflicts; dropping stale rebase task."

    if task.decision_reason == "proactive_rebase":
        mergeable = str(pr_data.get("mergeable") or "").upper()
        if mergeable == "CONFLICTING":
            return True, "PR became conflicting; dropping stale proactive rebase task."

    logger.debug("PR snapshot validation passed for task %s", task.id)
    return False, ""


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

    def publish_task_end(status: ResultStatus) -> None:
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

        try:
            is_stale, stale_reason = _validate_pr_task_snapshot(task, logger)
        except Exception as exc:
            duration = int(time.monotonic() - start)
            publish_task_end(ResultStatus.FAILED)
            return _task_result(
                task,
                config,
                ResultStatus.FAILED,
                task.branch,
                f"{TRANSIENT_SUMMARY_PREFIX}GitHub snapshot validation failed: {exc}",
                duration,
            )
        if is_stale:
            duration = int(time.monotonic() - start)
            publish_task_end(ResultStatus.STALE)
            return _task_result(
                task,
                config,
                ResultStatus.STALE,
                task.branch,
                stale_reason,
                duration,
            )

        logger.info(f"Cloning {task.repo} (branch: {task.branch or 'default'})")
        work_dir = workspace.setup(task.repo, task.branch, task.token)
        if task.resource_type == "pr" and task.snapshot_head_sha:
            workspace_head_sha = workspace.current_head_sha()
            if workspace_head_sha != task.snapshot_head_sha:
                duration = int(time.monotonic() - start)
                publish_task_end(ResultStatus.STALE)
                return _task_result(
                    task,
                    config,
                    ResultStatus.STALE,
                    task.branch,
                    (
                        f"Workspace HEAD {workspace_head_sha} did not match task snapshot "
                        f"{task.snapshot_head_sha}; dropping stale task."
                    ),
                    duration,
                )

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

        def on_stderr(line: str) -> None:
            # Stderr is published to the same output stream tagged ``stream=stderr``
            # so postmortems can reconstruct what the runner emitted before a crash.
            # Failures here are silent: streaming visibility is non-critical and we
            # never want a flaky Redis to break task execution.
            try:
                redis.xadd_capped(
                    output_stream,
                    {"line": line, "stream": "stderr"},
                    maxlen=_STREAM_MAXLEN,
                )
            except Exception:
                pass

        # Per-task runner dispatch: select the Runner class baked into the
        # registry entry for ``task.provider``. Falls back to the worker's
        # configured default runner (so the ``noop`` path used in tests, and
        # any legacy single-runner deployment, still works). Per-task model
        # from ``task.model`` overrides the worker-wide default.
        per_task_runner = _runner_for_task(task, config, fallback=runner)
        effective_model = task.model or config.runner.model
        runner_result: RunnerResult = per_task_runner.run(
            prompt=task.prompt,
            work_dir=work_dir,
            token=task.token,
            timeout=config.runner.timeout,
            logger=logger,
            on_output=on_output,
            on_stderr=on_stderr,
            abort_event=abort_event,
            claude_token=task.claude_token,
            provider=task.provider,
            credential=task.credential,
            model=effective_model,
        )

        duration = int(time.monotonic() - start)

        if runner_result.needs_human:
            # A worker-reported human-decision blocker is never a success: the
            # PR was not resolved. Force FAILED (even if the CLI exited 0) so
            # the orchestrator surfaces the signal instead of silently
            # treating the task as completed.
            status = ResultStatus.FAILED
        elif runner_result.success:
            status = ResultStatus.COMPLETED
        elif runner_result.usage_exhausted:
            status = ResultStatus.USAGE_EXHAUSTED
        else:
            status = ResultStatus.FAILED

        # Only failures explicitly classified as transient should be retried
        # silently. Normal task failures need human-visible handling instead
        # of being recycled until the retry budget is exhausted.
        summary = runner_result.summary
        if (
            status == ResultStatus.FAILED
            and runner_result.transient
            and not summary.startswith(TRANSIENT_SUMMARY_PREFIX)
        ):
            summary = f"{TRANSIENT_SUMMARY_PREFIX}{summary}"

        publish_task_end(status)

        return _task_result(
            task,
            config,
            status,
            task.branch,
            summary,
            duration,
            rate_limit_resets_at=runner_result.rate_limit_resets_at,
            needs_human=runner_result.needs_human,
            needs_human_reason=runner_result.needs_human_reason,
            credential_update=runner_result.credential_update or "",
        )

    except Exception as e:
        duration = int(time.monotonic() - start)
        logger.error(f"Task execution failed: {e}", exc_info=True)

        publish_task_end(ResultStatus.FAILED)

        # Infrastructure failures (clone timeout, network) are transient —
        # the orchestrator will retry without burning an attempt slot.
        is_transient = isinstance(e, WorkspaceError) and e.transient
        prefix = TRANSIENT_SUMMARY_PREFIX if is_transient else ""

        return _task_result(
            task,
            config,
            ResultStatus.FAILED,
            task.branch,
            f"{prefix}Worker exception: {e}",
            duration,
        )

    finally:
        try:
            workspace.cleanup()
        except Exception:
            logger.warning("Workspace cleanup failed", exc_info=True)
