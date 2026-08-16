"""Worker main loop: block on Redis streams, acquire lock, run Claude, publish result.

The central worker loop reads tasks from Redis streams via XREADGROUP
(PR tasks with priority, then issue tasks), acquires a distributed lock
per resource, runs Claude, and publishes results back to a results stream
for the orchestrator.
"""

import hashlib
import json
import logging
import os
import signal
import sys
import threading
import time
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import cast

import redis as redis_py
import yaml

from orcest.orchestrator import gh
from orcest.revision import get_build_revision
from orcest.shared.config import WorkerConfig
from orcest.shared.coordination import (
    RedisLock,
    make_issue_lock_key,
    make_pending_task_key,
    make_pr_lock_key,
    parse_pending_task_metadata,
)
from orcest.shared.credential_handoff import (
    CREDENTIAL_DIAGNOSTIC_HANDOFF_PREFIX as _CREDENTIAL_DIAGNOSTIC_HANDOFF_PREFIX,
    CredentialCheckpointStatus,
    CredentialRecoveryOutcome,
    CredentialTerminalOutcome,
    credential_checkpoint_key as _shared_credential_checkpoint_key,
    load_credential_checkpoint as _load_private_credential_checkpoint,
    recover_credential_checkpoint as _recover_private_credential_checkpoint,
    safe_dead_letter_fields as _shared_safe_dead_letter_fields,
    store_credential_checkpoint as _store_private_credential_checkpoint,
    terminal_credential_handoff_once as _terminal_credential_handoff_once,
    version_credential_checkpoint as _version_credential_checkpoint,
)
from orcest.shared.logging import setup_logging
from orcest.shared.models import (
    CONSUMER_GROUP,
    DEAD_LETTER_STREAM,
    TRANSIENT_SUMMARY_PREFIX,
    ResultStatus,
    Task,
    TaskResult,
    is_claude_provider,
    task_stream_name,
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
WORKER_LIVENESS_TTL = 150  # Covers two 60s task-heartbeat refresh intervals plus jitter.
MAX_DELIVERY_COUNT = 3  # Dead-letter at or after N deliveries; task runs at most N-1 times
_STREAM_MAXLEN = 20000  # bumped from 2000 for archiver-hiccup headroom (~50MB across 4 workers)
_RESULT_PUBLISH_RETRIES = 3  # Max attempts to publish a result
_RESULT_PUBLISH_BACKOFF = (1, 2)  # Seconds to sleep before each retry (before attempt 2, 3)
_EPHEMERAL_RESULT_RETRY_SECONDS = 5


def _refresh_worker_liveness(
    redis: RedisClient, config: WorkerConfig, logger: logging.Logger
) -> None:
    """Publish an expiring, non-secret worker process/backend/revision heartbeat."""
    payload = json.dumps(
        {"backend": config.backend, "revision": get_build_revision()},
        sort_keys=True,
        separators=(",", ":"),
    )
    try:
        redis.set_ex(
            f"workers:heartbeat:{config.worker_id}",
            payload,
            ttl=WORKER_LIVENESS_TTL,
        )
    except redis_py.RedisError:
        logger.warning("Failed to publish worker liveness heartbeat", exc_info=True)


_SOURCE_ACK_RETRY_BASE_SECONDS = 1
_HANDOFF_MARKER_TTL_SECONDS = 30 * 24 * 3600
_HANDOFF_FINGERPRINT_FIELD = "orcest_handoff_fingerprint"
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
_POOL_DRAINING_KEY = "pool:draining"
_DRAIN_RESTART_EXIT_CODE = 75
_CI_DECISIONS = frozenset({"ci_failure"})
_REVIEW_DECISIONS = frozenset({"changes_requested", "followup_threads"})

_ATOMIC_XADD_MARKER_LUA = r"""
local marker = redis.call('GET', KEYS[2])
if marker then
  local split = string.find(marker, '|', 1, true)
  if split then
    local entry_id = string.sub(marker, 1, split - 1)
    local fingerprint = string.sub(marker, split + 1)
    if fingerprint == ARGV[3] then
      local rows = redis.call('XRANGE', KEYS[1], entry_id, entry_id, 'COUNT', 1)
      if #rows == 1 then
        local row = rows[1][2]
        for i = 1, #row, 2 do
          if row[i] == ARGV[4] and row[i + 1] == fingerprint then
            return {0, entry_id}
          end
        end
      end
    end
  end
end

local xargs = {}
for i = 5, #ARGV do
  table.insert(xargs, ARGV[i])
end
local entry_id = redis.call('XADD', KEYS[1], 'MAXLEN', '~', ARGV[1], '*', unpack(xargs))
redis.call('SET', KEYS[2], entry_id .. '|' .. ARGV[3], 'EX', ARGV[2])
return {1, entry_id}
"""


class ResultPublishOutcome(Enum):
    """Durability boundary reached while handing off a worker result."""

    PUBLISHED = "published"
    DEAD_LETTERED = "dead_lettered"
    LOST = "lost"
    ABORTED = "aborted"
    BLOCKED = "blocked"

    @property
    def durable(self) -> bool:
        """Whether Redis now holds a recoverable copy of the result."""
        return self in {self.PUBLISHED, self.DEAD_LETTERED}


@dataclass(frozen=True)
class ResultHandoff:
    """Result publication plus the source-stream acknowledgement boundary."""

    publish_outcome: ResultPublishOutcome
    source_acked: bool

    @property
    def terminal(self) -> bool:
        """Whether the result is durable and the source PEL entry is gone."""
        return self.publish_outcome.durable and self.source_acked


@dataclass(frozen=True)
class CoordinationIdentity:
    task_id: str
    repo: str
    resource_type: str
    resource_id: int
    key_prefix: str = ""
    snapshot_head_sha: str = ""


def _runner_for_task(task: Task, config: WorkerConfig, fallback: Runner) -> Runner:
    """Return the Runner instance to execute ``task``.

    Dispatch policy:
      - When ``task.provider`` matches the worker's configured runner type
        (the common case — a Claude worker receiving a Claude task), use
        the pre-instantiated fallback. Avoids per-task instantiation and
        preserves the contract that tests inject a runner via the fallback.
      - When the provider differs and is registered in PROVIDER_REGISTRY
        with a ``runner_cls``, instantiate that class fresh. Claude keeps the
        worker's explicit ``runner.extra.mode=interactive`` selection in this
        branch so provider dispatch cannot accidentally fall back to ``-p``.
      - Otherwise (unknown provider, or registered without a ``runner_cls``),
        fall back. The early-reject in the main loop already filters
        genuinely unknown providers before dispatch.
    """
    if task.provider == config.runner.type:
        return fallback
    if is_claude_provider(task.provider) and config.runner.extra.get("mode") == "interactive":
        from orcest.worker.claude_interactive_runner import ClaudeInteractiveRunner

        return ClaudeInteractiveRunner(
            max_retries=config.runner.max_retries,
            retry_backoff=config.runner.retry_backoff,
            model=config.runner.model,
        )
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


def _wait_for_redis(
    redis: RedisClient,
    logger: logging.Logger,
    abort_event: threading.Event | None = None,
) -> bool:
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
    abort = abort_event if abort_event is not None else threading.Event()
    for attempt in range(_STARTUP_PING_RETRIES):
        if abort.is_set():
            return False
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
            if abort.wait(timeout=sleep_s):
                return False
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


def _task_coordination_identity(task: Task) -> CoordinationIdentity:
    return CoordinationIdentity(
        task_id=task.id,
        repo=task.repo,
        resource_type=task.resource_type,
        resource_id=task.resource_id,
        key_prefix=task.key_prefix,
        snapshot_head_sha=task.snapshot_head_sha,
    )


def _cleanup_coordination_once(
    redis: RedisClient,
    identity: CoordinationIdentity,
) -> bool:
    """Atomically clear owned pending+attempt state; mismatch is a safe no-op."""
    pending_base = make_pending_task_key(
        identity.repo,
        identity.resource_type,
        identity.resource_id,
    )
    attempts_base = (
        f"issue:{identity.repo}:{identity.resource_id}:attempts"
        if identity.resource_type == "issue"
        else f"pr:{identity.repo}:{identity.resource_id}:attempts"
    )
    if identity.key_prefix:
        pending_key = f"{identity.key_prefix}:{pending_base}"
        attempts_key = f"{identity.key_prefix}:{attempts_base}"
    else:
        pending_key = redis._prefixed(pending_base)
        attempts_key = redis._prefixed(attempts_base)

    pipe = redis.client.pipeline()
    try:
        pipe.watch(pending_key, attempts_key)
        raw_pending = pipe.get(pending_key)
        if isinstance(raw_pending, bytes):
            raw_pending = raw_pending.decode("utf-8")
        metadata = parse_pending_task_metadata(
            raw_pending if isinstance(raw_pending, str) else None
        )
        if metadata is None or metadata.task_id != identity.task_id:
            pipe.unwatch()
            return True

        clear_attempts = identity.resource_type == "issue"
        if identity.resource_type == "pr" and identity.snapshot_head_sha:
            raw_head_sha = pipe.hget(attempts_key, "head_sha")
            if isinstance(raw_head_sha, bytes):
                raw_head_sha = raw_head_sha.decode("utf-8")
            clear_attempts = raw_head_sha == identity.snapshot_head_sha

        pipe.multi()
        pipe.delete(pending_key)
        if clear_attempts:
            pipe.delete(attempts_key)
        pipe.execute()
        return True
    except redis_py.WatchError:
        return False
    finally:
        pipe.reset()


def _cleanup_coordination_until_terminal(
    redis: RedisClient,
    identity: CoordinationIdentity,
    logger: logging.Logger,
    abort_event: threading.Event,
) -> bool:
    attempt = 0
    while True:
        try:
            if _cleanup_coordination_once(redis, identity):
                return True
        except Exception:
            logger.warning(
                "Failed atomic coordination cleanup for task %s",
                identity.task_id,
                exc_info=True,
            )
        attempt += 1
        if abort_event.wait(
            timeout=min(30, _SOURCE_ACK_RETRY_BASE_SECONDS * 2 ** min(attempt - 1, 5))
        ):
            return False


def _ack_source_entry_until_confirmed(
    redis: RedisClient,
    tasks_stream: str,
    entry_id: str,
    task_id: str,
    logger: logging.Logger,
    abort_event: threading.Event,
) -> bool:
    """Retry XACK until Redis confirms the command or shutdown is requested."""
    attempt = 0
    while True:
        try:
            # A successful command with a zero count is also terminal: it means
            # an earlier ambiguous XACK already removed the entry from this PEL.
            redis.xack_raw(tasks_stream, CONSUMER_GROUP, entry_id)
            return True
        except Exception:
            attempt += 1
            logger.error(
                "Failed to ACK task %s entry %s (attempt %d); retrying",
                task_id,
                entry_id,
                attempt,
                exc_info=True,
            )
            if abort_event.wait(
                timeout=min(30, _SOURCE_ACK_RETRY_BASE_SECONDS * 2 ** min(attempt - 1, 5))
            ):
                return False


def _stream_handoff_identity(
    target_stream: str,
    tasks_stream: str,
    entry_id: str,
    task_id: str,
) -> str:
    identity = "\0".join((target_stream, tasks_stream, entry_id, task_id))
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def _fully_qualified_stream(redis: RedisClient, logical_stream: str) -> str:
    value = redis._prefixed(logical_stream)
    return value if isinstance(value, str) else logical_stream


def _handoff_marker_key(
    target_stream: str,
    tasks_stream: str,
    entry_id: str,
    task_id: str,
) -> str:
    identity = _stream_handoff_identity(target_stream, tasks_stream, entry_id, task_id)
    return f"{target_stream}:handoff:{identity}"


def _credential_checkpoint_key(
    target_stream: str,
    tasks_stream: str,
    entry_id: str,
    task_id: str,
) -> str:
    return _shared_credential_checkpoint_key(target_stream, tasks_stream, entry_id, task_id)


def _handoff_payload_fingerprint(fields: dict[str, str]) -> str:
    payload = json.dumps(fields, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _store_credential_checkpoint(
    redis: RedisClient,
    target_stream: str,
    tasks_stream: str,
    entry_id: str,
    task_id: str,
    result_fields: dict[str, str],
) -> None:
    """Persist a private replayable result; never copy this payload to diagnostics."""
    _store_private_credential_checkpoint(
        redis, target_stream, tasks_stream, entry_id, task_id, result_fields
    )


def _load_credential_checkpoint(
    redis: RedisClient,
    target_stream: str,
    tasks_stream: str,
    entry_id: str,
    task: Task,
    logger: logging.Logger,
) -> tuple[CredentialCheckpointStatus, dict[str, str] | None]:
    status, checkpoint = _load_private_credential_checkpoint(
        redis, target_stream, tasks_stream, entry_id, task, logger
    )
    return status, checkpoint.result_fields if checkpoint is not None else None


def _handoff_marker_present(
    redis: RedisClient,
    target_stream: str,
    tasks_stream: str,
    entry_id: str,
    handoff_id: str,
    logger: logging.Logger,
) -> bool | None:
    try:
        marker = redis.client.get(
            _handoff_marker_key(
                target_stream,
                tasks_stream,
                entry_id,
                handoff_id,
            )
        )
        return isinstance(marker, (str, bytes))
    except Exception:
        logger.warning(
            "Failed to inspect credential diagnostic marker for task %s",
            handoff_id,
            exc_info=True,
        )
        return None


def _delete_handoff_marker_best_effort(
    redis: RedisClient,
    target_stream: str,
    tasks_stream: str,
    entry_id: str,
    handoff_id: str,
    logger: logging.Logger,
) -> None:
    try:
        redis.client.delete(
            _handoff_marker_key(
                target_stream,
                tasks_stream,
                entry_id,
                handoff_id,
            )
        )
    except Exception:
        logger.warning(
            "Failed to delete completed handoff marker for task %s; TTL cleanup remains",
            handoff_id,
            exc_info=True,
        )


def _atomic_xadd_capped_with_marker(
    redis: RedisClient,
    target_stream: str,
    tasks_stream: str,
    entry_id: str,
    task_id: str,
    fields: dict[str, str],
    *,
    raw_target: bool,
    logical_target: str,
) -> str:
    """Atomically append a stream row and its bounded idempotency marker."""
    fingerprint = _handoff_payload_fingerprint(fields)
    marked_fields = {**fields, _HANDOFF_FINGERPRINT_FIELD: fingerprint}
    marker_key = _handoff_marker_key(target_stream, tasks_stream, entry_id, task_id)
    flattened_fields = [item for pair in marked_fields.items() for item in pair]
    response = cast(
        object,
        redis.client.eval(
            _ATOMIC_XADD_MARKER_LUA,
            2,
            target_stream,
            marker_key,
            str(_STREAM_MAXLEN),
            str(_HANDOFF_MARKER_TTL_SECONDS),
            fingerprint,
            _HANDOFF_FINGERPRINT_FIELD,
            *flattened_fields,
        ),
    )
    if isinstance(response, (list, tuple)) and len(response) == 2:
        raw_entry_id = response[1]
        if isinstance(raw_entry_id, bytes):
            return raw_entry_id.decode("utf-8")
        return str(raw_entry_id)

    # Unit-test doubles do not execute Lua. Preserve their observable wrapper
    # calls while production Redis always takes the atomic path above.
    if raw_target:
        published_id = redis.xadd_capped_raw(target_stream, marked_fields, maxlen=_STREAM_MAXLEN)
    else:
        published_id = redis.xadd_capped(logical_target, marked_fields, maxlen=_STREAM_MAXLEN)
    redis.client.set(
        marker_key,
        f"{published_id}|{fingerprint}",
        ex=_HANDOFF_MARKER_TTL_SECONDS,
    )
    return published_id


def _stream_handoff_state(
    redis: RedisClient,
    target_stream: str,
    tasks_stream: str,
    entry_id: str,
    task_id: str,
    logger: logging.Logger,
) -> bool | None:
    """Verify an O(1) companion marker against its exact retained stream row."""
    marker_key = _handoff_marker_key(target_stream, tasks_stream, entry_id, task_id)
    try:
        raw_marker = redis.client.get(marker_key)
    except Exception:
        logger.warning("Failed to inspect handoff marker for task %s", task_id, exc_info=True)
        return None
    if isinstance(raw_marker, bytes):
        raw_marker = raw_marker.decode("utf-8")
    if not isinstance(raw_marker, str) or "|" not in raw_marker:
        return False
    stream_entry_id, fingerprint = raw_marker.split("|", 1)
    try:
        rows = cast(
            object,
            redis.client.xrange(
                target_stream,
                min=stream_entry_id,
                max=stream_entry_id,
                count=1,
            ),
        )
    except Exception:
        logger.warning("Failed to verify handoff stream row for task %s", task_id, exc_info=True)
        return None
    if not isinstance(rows, list) or len(rows) != 1:
        return False
    row_id, row_fields = rows[0]
    if isinstance(row_id, bytes):
        row_id = row_id.decode("utf-8")
    if row_id != stream_entry_id or not isinstance(row_fields, dict):
        return False
    row_fingerprint = row_fields.get(_HANDOFF_FINGERPRINT_FIELD)
    if isinstance(row_fingerprint, bytes):
        row_fingerprint = row_fingerprint.decode("utf-8")
    return row_fingerprint == fingerprint


def _restore_credential_checkpoint(
    redis: RedisClient,
    task: Task,
    target_stream: str,
    tasks_stream: str,
    entry_id: str,
    logger: logging.Logger,
) -> CredentialRecoveryOutcome:
    """Restore an exact rotated-credential result before generic startup recovery."""
    return _recover_private_credential_checkpoint(
        redis,
        task,
        target_stream,
        tasks_stream,
        entry_id,
        _fully_qualified_stream(redis, DEAD_LETTER_STREAM),
        logger,
        maxlen=_STREAM_MAXLEN,
    )


def _handoff_dead_letter_fields_until_terminal(
    redis: RedisClient,
    tasks_stream: str,
    entry_id: str,
    fields: dict[str, str],
    task_id: str,
    logger: logging.Logger,
    abort_event: threading.Event | None = None,
    *,
    retry: bool = True,
    coordination: CoordinationIdentity | None = None,
) -> bool:
    """Persist safe DLQ fields and ACK their source without duplicate writes."""
    abort = abort_event if abort_event is not None else threading.Event()
    target_stream = _fully_qualified_stream(redis, DEAD_LETTER_STREAM)
    while True:
        existing = _stream_handoff_state(
            redis, target_stream, tasks_stream, entry_id, task_id, logger
        )
        if existing is True:
            break
        if existing is None:
            if not retry or abort.wait(timeout=_EPHEMERAL_RESULT_RETRY_SECONDS):
                return False
            continue
        try:
            _atomic_xadd_capped_with_marker(
                redis,
                target_stream,
                tasks_stream,
                entry_id,
                task_id,
                fields,
                raw_target=False,
                logical_target=DEAD_LETTER_STREAM,
            )
            break
        except Exception:
            logger.error(
                "Failed to publish dead-letter entry for task %s; retaining source and retrying",
                task_id,
                exc_info=True,
            )
            if not retry or abort.wait(timeout=_EPHEMERAL_RESULT_RETRY_SECONDS):
                return False

    if coordination is not None:
        if retry:
            cleanup_terminal = _cleanup_coordination_until_terminal(
                redis, coordination, logger, abort
            )
        else:
            try:
                cleanup_terminal = _cleanup_coordination_once(redis, coordination)
            except Exception:
                logger.warning(
                    "Startup coordination cleanup failed for task %s",
                    coordination.task_id,
                    exc_info=True,
                )
                cleanup_terminal = False
        if not cleanup_terminal:
            return False

    if retry:
        source_acked = _ack_source_entry_until_confirmed(
            redis,
            tasks_stream,
            entry_id,
            task_id,
            logger,
            abort,
        )
    else:
        try:
            redis.xack_raw(tasks_stream, CONSUMER_GROUP, entry_id)
            source_acked = True
        except Exception:
            logger.error(
                "Failed to ACK startup-recovery entry %s; preserving coordination",
                entry_id,
                exc_info=True,
            )
            source_acked = False
    if source_acked:
        _delete_handoff_marker_best_effort(
            redis,
            target_stream,
            tasks_stream,
            entry_id,
            task_id,
            logger,
        )
    return source_acked


def _safe_raw_dead_letter_fields(
    fields: dict[str, str],
    tasks_stream: str,
    entry_id: str,
    reason: str,
) -> dict[str, str]:
    """Build a diagnostic payload without deserializing or persisting secrets."""
    return _shared_safe_dead_letter_fields(fields, tasks_stream, entry_id, reason)


def _malformed_coordination_identity(
    fields: dict[str, str],
    logger: logging.Logger,
) -> CoordinationIdentity | None:
    """Extract only the validated identity needed for atomic coordination cleanup."""
    task_id = fields.get("id", "")
    repo = fields.get("repo", "")
    resource_type = fields.get("resource_type", "")
    resource_id_raw = fields.get("resource_id", "")
    if not task_id or not repo or resource_type not in {"pr", "issue"}:
        logger.warning("Malformed task lacks a safe coordination identity; marker will expire")
        return None
    try:
        resource_id = int(resource_id_raw)
    except (TypeError, ValueError):
        logger.warning("Malformed task has an invalid resource ID; marker will expire")
        return None
    return CoordinationIdentity(
        task_id=task_id,
        repo=repo,
        resource_type=resource_type,
        resource_id=resource_id,
        key_prefix=fields.get("key_prefix", ""),
        snapshot_head_sha=fields.get("snapshot_head_sha", ""),
    )


def _handoff_result_until_terminal(
    redis: RedisClient,
    result: TaskResult,
    task: Task,
    logger: logging.Logger,
    tasks_stream: str,
    entry_id: str,
    abort_event: threading.Event | None = None,
) -> ResultHandoff:
    """Publish a result, then remove its source entry without rerunning work.

    Publication and XACK are separate Redis commands. Once publication is
    durable, only XACK is retried so an ACK outage cannot duplicate results or
    dead letters. Shutdown leaves the source PEL entry and coordination state
    intact for startup/pool recovery.
    """
    abort = abort_event if abort_event is not None else threading.Event()
    publish_outcome = _publish_result_with_retry(
        redis,
        result,
        task,
        logger,
        tasks_stream,
        entry_id,
        abort_event=abort,
    )
    if publish_outcome is ResultPublishOutcome.BLOCKED:
        return ResultHandoff(publish_outcome, source_acked=False)
    while not publish_outcome.durable:
        logger.error(
            "Result publish failed for task %s; retaining its source entry and retrying",
            task.id,
        )
        if abort.wait(timeout=_EPHEMERAL_RESULT_RETRY_SECONDS):
            return ResultHandoff(publish_outcome, source_acked=False)
        publish_outcome = _publish_result_with_retry(
            redis,
            result,
            task,
            logger,
            tasks_stream,
            entry_id,
            abort_event=abort,
        )
        if publish_outcome is ResultPublishOutcome.BLOCKED:
            return ResultHandoff(publish_outcome, source_acked=False)

    if publish_outcome is ResultPublishOutcome.DEAD_LETTERED:
        if not _cleanup_coordination_until_terminal(
            redis,
            _task_coordination_identity(task),
            logger,
            abort,
        ):
            return ResultHandoff(publish_outcome, source_acked=False)

    result_target_stream = (
        f"{task.key_prefix}:{RESULTS_STREAM}"
        if task.key_prefix
        else _fully_qualified_stream(redis, RESULTS_STREAM)
    )
    if publish_outcome is ResultPublishOutcome.PUBLISHED and result.credential_update:
        # Credential publication is already terminal: its shared Lua script
        # appends the exact result, ACKs this source, and deletes the private
        # checkpoint in one idempotent operation.
        source_acked = True
    else:
        source_acked = _ack_source_entry_until_confirmed(
            redis,
            tasks_stream,
            entry_id,
            task.id,
            logger,
            abort,
        )
    if source_acked:
        if not (publish_outcome is ResultPublishOutcome.PUBLISHED and result.credential_update):
            target_stream = (
                result_target_stream
                if publish_outcome is ResultPublishOutcome.PUBLISHED
                else _fully_qualified_stream(redis, DEAD_LETTER_STREAM)
            )
            _delete_handoff_marker_best_effort(
                redis,
                target_stream,
                tasks_stream,
                entry_id,
                task.id,
                logger,
            )
        if result.credential_update:
            _delete_handoff_marker_best_effort(
                redis,
                _fully_qualified_stream(redis, DEAD_LETTER_STREAM),
                tasks_stream,
                entry_id,
                f"{_CREDENTIAL_DIAGNOSTIC_HANDOFF_PREFIX}{task.id}",
                logger,
            )
    return ResultHandoff(publish_outcome, source_acked)


def _early_reject_unsupported_provider(
    task: Task,
    provider: str,
    config: WorkerConfig,
    redis: RedisClient,
    logger: logging.Logger,
    current_stream: str,
    entry_id: str,
    abort_event: threading.Event | None = None,
    summary: str | None = None,
) -> ResultHandoff:
    """Publish a clean permanent FAILED for an unknown/missing provider.

    This is the early graceful reject path (before lock, heartbeat, clone, or
    any runner work). The orchestrator will see a non-transient failure with
    an actionable summary telling operators to rebake the worker image.
    Credentials are never logged or leaked because we use the redacted path
    only for DL (if publish fails) and TaskResult itself contains no secrets.
    The source task is ACKed only after either the result or its full recovery
    payload is durable in Redis. Primary-result coordination remains for the
    orchestrator consumer; DLQ coordination is cleared here.
    """
    failure_summary = summary or f"Rebake worker image to include {provider} CLI"
    result = _task_result(
        task,
        config,
        ResultStatus.FAILED,
        task.branch,
        failure_summary,
        0,
    )
    handoff = _handoff_result_until_terminal(
        redis,
        result,
        task,
        logger,
        current_stream,
        entry_id,
        abort_event=abort_event,
    )
    if not handoff.terminal:
        logger.error(
            "Unsupported-provider result for task %s has no terminal handoff; "
            "leaving the source entry and coordination state pending",
            task.id,
        )
    logger.info(f"Early graceful reject for task {task.id} (provider={provider})")
    return handoff


def _signal_ephemeral_done(
    redis: RedisClient,
    config: WorkerConfig,
    logger: logging.Logger,
    shutdown_event: threading.Event,
) -> bool:
    """Signal pool completion, retrying durably for pool-managed clones."""
    attempt = 0
    while True:
        try:
            key = f"pool:done:{config.worker_id}"
            if config.pool_managed:
                # The pool manager is the acknowledgement boundary: it
                # deletes this durable handoff only after the VM is destroyed.
                redis.set_value(key, "1")
            else:
                # Standalone ephemeral workers have no pool manager to clean
                # an unbounded marker, so retain the legacy diagnostic TTL.
                redis.set_ex(key, "1", ttl=300)
            return True
        except Exception:
            attempt += 1
            if not config.pool_managed:
                logger.warning("Failed to set pool:done key", exc_info=True)
                return False
            logger.warning(
                "Failed to hand task completion to pool manager (attempt %d); retrying",
                attempt,
                exc_info=True,
            )
            if shutdown_event.wait(timeout=min(30, 2 ** min(attempt - 1, 5))):
                return False


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
    pr_stream = task_stream_name(config.backend)
    issue_stream = task_stream_name(config.backend, issue=True)
    # Fully-qualified names for raw Redis operations
    if config.redis.key_prefix:
        pr_fq = f"{config.redis.key_prefix}:{pr_stream}"
        issue_fq = f"{config.redis.key_prefix}:{issue_stream}"
    else:
        pr_fq = pr_stream
        issue_fq = issue_stream

    # Install shutdown handling before any Redis retries so SIGTERM and an
    # external stop request can interrupt startup ping backoff as well as
    # recovery/result handoffs.
    shutdown = False
    shutdown_event = threading.Event()

    def handle_signal(signum: int, frame: object) -> None:
        nonlocal shutdown
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        shutdown = True
        shutdown_event.set()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)
    terminal_abort_event = (
        _make_abort_event(shutdown_event, stop_event) if stop_event is not None else shutdown_event
    )

    # Verify Redis connection with retry budget so a brief Redis restart
    # (e.g. during ``orcest fleet update``) doesn't kill the worker.
    if not _wait_for_redis(redis, logger, terminal_abort_event):
        if terminal_abort_event.is_set():
            logger.info("Redis startup wait stopped by shutdown request")
            return
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
    pr_drain_complete, pr_drained = _drain_pending_tasks_raw(
        redis, pr_fq, config, logger, abort_event=terminal_abort_event
    )
    if not pr_drain_complete:
        if terminal_abort_event.is_set():
            logger.info("Startup recovery stopped by shutdown request")
            return
        logger.error("Pending PR-task recovery is not durable; exiting for a backed-off retry")
        raise SystemExit(1)
    issue_drain_complete, issue_drained = _drain_pending_tasks_raw(
        redis, issue_fq, config, logger, abort_event=terminal_abort_event
    )
    if not issue_drain_complete:
        if terminal_abort_event.is_set():
            logger.info("Startup recovery stopped by shutdown request")
            return
        logger.error("Pending issue-task recovery is not durable; exiting for a backed-off retry")
        raise SystemExit(1)

    if config.ephemeral and pr_drained + issue_drained > 0:
        _signal_ephemeral_done(redis, config, logger, terminal_abort_event)
        logger.info("Ephemeral mode: recovered prior pending work, shutting down.")
        return

    workspace = Workspace(config.workspace_dir)
    runner_mode = config.runner.extra.get("mode", "default")

    logger.info(
        f"Worker {config.worker_id} started (backend={config.backend}, "
        f"runner={config.runner.type}, runner_mode={runner_mode}, "
        f"streams={pr_fq},{issue_fq}). Waiting for tasks..."
    )

    while not shutdown and (stop_event is None or not stop_event.is_set()):
        _refresh_worker_liveness(redis, config, logger)
        try:
            if redis.sismember(_POOL_DRAINING_KEY, config.worker_id) is True:
                logger.info(
                    "Worker %s is draining; exiting with restartable status before claiming work",
                    config.worker_id,
                )
                # The template unit uses Restart=on-failure. A non-zero drain
                # exit keeps the worker recoverable if the pool manager aborts
                # after quiescing it; on a successful drain the VM is stopped
                # before systemd's RestartSec elapses.
                raise SystemExit(_DRAIN_RESTART_EXIT_CODE)
        except Exception:
            # Redis reads immediately below remain authoritative. A transient
            # drain-key failure should not crash a healthy worker.
            logger.debug("Failed to check worker drain state", exc_info=True)
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
        except (KeyError, ValueError):
            logger.error(
                "Malformed task entry %s; routing redacted diagnostics to dead-letter recovery",
                entry_id,
            )
            malformed_terminal = _handoff_dead_letter_fields_until_terminal(
                redis,
                current_stream,
                entry_id,
                _safe_raw_dead_letter_fields(
                    fields,
                    current_stream,
                    entry_id,
                    "Malformed task payload",
                ),
                fields.get("id", f"malformed:{entry_id}"),
                logger,
                terminal_abort_event,
                coordination=_malformed_coordination_identity(fields, logger),
            )
            if config.ephemeral:
                if malformed_terminal:
                    _signal_ephemeral_done(redis, config, logger, terminal_abort_event)
                logger.info("Ephemeral mode: malformed task handled, shutting down.")
                shutdown = True
                shutdown_event.set()
            continue

        # === Early multi-provider dispatch (Task 6) ===
        # Performed *immediately* after parse, before *any* lock acquisition,
        # delivery-count DL logic, heartbeat, workspace clone, or runner work.
        # This guarantees old worker images fail cleanly (non-transient FAILED)
        # when they encounter a task for a provider they do not have baked in.
        # The registry + binary check are strictly local to the worker image.
        routing_mismatch = task.provider != config.backend
        unsupported = None if routing_mismatch else get_unsupported_reason(task.provider)
        if routing_mismatch or unsupported:
            logger.warning(
                "Task %s for provider=%s cannot run on backend=%s (%s); "
                "early graceful reject (permanent FAILED, no runner reached)",
                task.id,
                task.provider,
                config.backend,
                "provider/backend stream mismatch" if routing_mismatch else unsupported,
            )
            handoff = _early_reject_unsupported_provider(
                task,
                task.provider,
                config,
                redis,
                logger,
                current_stream,
                entry_id,
                abort_event=terminal_abort_event,
                summary=(
                    "Task provider does not match the dedicated worker backend"
                    if routing_mismatch
                    else None
                ),
            )

            if config.ephemeral:
                if handoff.terminal:
                    _signal_ephemeral_done(redis, config, logger, terminal_abort_event)
                else:
                    logger.error(
                        "Ephemeral unsupported-provider task %s remains unACKed "
                        "after shutdown; leaving it for recovery",
                        task.id,
                    )
                logger.info("Ephemeral mode: unsupported task handled, shutting down.")
                shutdown = True
                shutdown_event.set()
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
            dead_letter_terminal = _dead_letter_task(
                redis,
                current_stream,
                entry_id,
                task,
                delivery_count,
                logger,
                abort_event=terminal_abort_event,
            )
            if config.ephemeral:
                if dead_letter_terminal:
                    _signal_ephemeral_done(redis, config, logger, terminal_abort_event)
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
            # Discarding an unrecorded entry would silently lose work. Publish
            # a transient result so the orchestrator owns retry/coordination,
            # then ACK through the same terminal boundary as executed tasks.
            skipped_result = _task_result(
                task,
                config,
                ResultStatus.FAILED,
                task.branch,
                (
                    f"{TRANSIENT_SUMMARY_PREFIX}Another worker held the resource lock; "
                    "this task was not executed."
                ),
                0,
            )
            handoff = _handoff_result_until_terminal(
                redis,
                skipped_result,
                task,
                logger,
                current_stream,
                entry_id,
                abort_event=terminal_abort_event,
            )
            if config.ephemeral:
                if handoff.terminal:
                    _signal_ephemeral_done(redis, config, logger, terminal_abort_event)
                logger.info("Ephemeral mode: lock-contention task handled, shutting down.")
                shutdown = True
                shutdown_event.set()
            continue

        logger.info(f"Acquired lock {lock.key}")

        # Dead-letter guard: if this entry has been delivered too many times
        # (result-publish failures leaving it unACKed), route it to the
        # dead-letter stream instead of running Claude again.
        delivery_count = redis.xpending_count_raw(current_stream, CONSUMER_GROUP, entry_id)
        if delivery_count >= MAX_DELIVERY_COUNT:
            lock.release()
            dead_letter_terminal = _dead_letter_task(
                redis,
                current_stream,
                entry_id,
                task,
                delivery_count,
                logger,
                abort_event=terminal_abort_event,
            )
            if config.ephemeral:
                # Ephemeral workers must exit after encountering any task,
                # including dead-lettered ones.  Without this the worker
                # would loop indefinitely on an empty queue until the pool
                # manager's SIGTERM timeout fires, wasting a VM slot.
                if dead_letter_terminal:
                    _signal_ephemeral_done(redis, config, logger, terminal_abort_event)
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
            on_refreshed=lambda: _refresh_worker_liveness(redis, config, logger),
        )
        heartbeat.start()

        # Combine lock_lost and shutdown_event so that either a lost lock *or*
        # a SIGTERM immediately wakes retry-backoff sleeps inside run_claude.
        # Before PR #98 the abort_event was shutdown_event directly; after that
        # refactor it became lock_lost alone, losing the SIGTERM fast-exit path.
        abort_event = _make_abort_event(lock_lost, terminal_abort_event)
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
            lock_released = lock.release()
            if not lock_released:
                lock_lost.set()
                logger.warning(
                    "Could not confirm release ownership for lock %s; treating result as stale",
                    lock.key,
                )
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

        # Retain every worker until publication and source acknowledgement are
        # both terminal. Publication is never repeated merely because XACK
        # failed; shutdown preserves the PEL entry for recovery.
        handoff = _handoff_result_until_terminal(
            redis,
            result,
            task,
            logger,
            current_stream,
            entry_id,
            abort_event=terminal_abort_event,
        )
        publish_outcome = handoff.publish_outcome

        if handoff.terminal:
            if publish_outcome is ResultPublishOutcome.PUBLISHED:
                logger.info(f"Published result for task {task.id}: {result.status.value}")
            else:
                logger.error(
                    "Result for task %s was durably dead-lettered for manual recovery",
                    task.id,
                )

        # Ephemeral mode: signal pool manager and exit after one task.
        # Exit after one task. If neither result nor recovery payload became
        # durable, leave the stream entry unACKed and coordination markers
        # intact so the pool manager can publish a recovery result before
        # destroying the VM.
        if config.ephemeral:
            if not handoff.terminal:
                if config.pool_managed:
                    logger.error(
                        "Pool-managed task %s is still unACKed after shutdown; "
                        "leaving VM for reaper recovery",
                        task.id,
                    )
                else:
                    logger.error(
                        "Standalone ephemeral task %s remains unACKed after shutdown; "
                        "restart with the same worker id to resume recovery",
                        task.id,
                    )
            if handoff.terminal:
                _signal_ephemeral_done(redis, config, logger, terminal_abort_event)
            logger.info("Ephemeral mode: task complete, shutting down.")
            shutdown = True
            shutdown_event.set()  # Must mirror handle_signal; abort_event watches this
        elif not handoff.terminal:
            continue

    try:
        redis.delete(f"workers:heartbeat:{config.worker_id}")
    except redis_py.RedisError:
        logger.debug("Failed to clear worker liveness heartbeat", exc_info=True)
    logger.info("Worker shut down cleanly.")


def _pending_task_id_for_task(redis: RedisClient, task: Task) -> str | None:
    """Return the task_id currently recorded in the pending marker for ``task``.

    Routed by ``task.key_prefix`` so multi-project markers are read from the
    correct namespace. Returns None when
    no marker is present. Absence alone is not proof of completion because the
    marker can expire or be lost while the stream entry remains pending.
    """
    base_key = make_pending_task_key(task.repo, task.resource_type, task.resource_id)
    if task.key_prefix:
        raw = redis.get_raw(f"{task.key_prefix}:{base_key}")
    else:
        raw = redis.get(base_key)
    metadata = parse_pending_task_metadata(raw)
    return metadata.task_id if metadata is not None else None


def _drain_pending_tasks_raw(
    redis: RedisClient,
    fq_stream: str,
    config: WorkerConfig,
    logger: logging.Logger,
    abort_event: threading.Event | None = None,
) -> tuple[bool, int]:
    """Drain pending tasks from a fully-qualified stream name.

    Uses raw (un-prefixed) Redis operations since the stream name is
    already fully qualified (e.g. ``orcest:tasks:claude``).
    """
    abort = abort_event if abort_event is not None else threading.Event()
    drained = 0
    while True:
        if abort.is_set():
            return False, drained
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
            if abort.is_set():
                return False, drained
            drained += 1
            task: Task | None = None
            recovery_result_published = False
            recovery_dead_lettered = False
            should_publish_recovery = False
            source_acked = False
            try:
                task = Task.from_dict(fields)
                logger.warning(
                    f"Recovering pending task {task.id} ({task.type.value} "
                    f"for {task.resource_type} #{task.resource_id})"
                )
                result_stream = (
                    f"{task.key_prefix}:{RESULTS_STREAM}"
                    if task.key_prefix
                    else _fully_qualified_stream(redis, RESULTS_STREAM)
                )
                credential_recovery = _restore_credential_checkpoint(
                    redis,
                    task,
                    result_stream,
                    fq_stream,
                    entry_id,
                    logger,
                )
                if credential_recovery is CredentialRecoveryOutcome.BLOCKED:
                    return False, drained
                if credential_recovery is CredentialRecoveryOutcome.RECOVERED:
                    _delete_handoff_marker_best_effort(
                        redis,
                        _fully_qualified_stream(redis, DEAD_LETTER_STREAM),
                        fq_stream,
                        entry_id,
                        f"{_CREDENTIAL_DIAGNOSTIC_HANDOFF_PREFIX}{task.id}",
                        logger,
                    )
                    continue
                # Dedup/staleness guard (M4-conc): only publish a recovery
                # FAILED unless the pending marker points at a DIFFERENT task.
                # A missing marker is ambiguous (it can expire or be lost while
                # work remains in the PEL), so fail safe by publishing recovery.
                # A newer task id is authoritative and suppresses this stale
                # replay. Marker read errors likewise publish recovery.
                #
                should_publish_recovery = True
                try:
                    current_pending_id = _pending_task_id_for_task(redis, task)
                    if current_pending_id is not None and current_pending_id != task.id:
                        should_publish_recovery = False
                        logger.info(
                            "Skipping duplicate recovery result for %s #%d: "
                            "pending marker no longer matches task %s (now %r); "
                            "original result already processed",
                            task.resource_type,
                            task.resource_id,
                            task.id,
                            current_pending_id,
                        )
                except Exception:
                    logger.warning(
                        "Failed to read pending marker for %s #%d during drain; "
                        "publishing recovery result",
                        task.resource_type,
                        task.resource_id,
                        exc_info=True,
                    )
                if should_publish_recovery:
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
                    recovery_dead_lettered_state = _stream_handoff_state(
                        redis,
                        _fully_qualified_stream(redis, DEAD_LETTER_STREAM),
                        fq_stream,
                        entry_id,
                        task.id,
                        logger,
                    )
                    if recovery_dead_lettered_state is None:
                        return False, drained
                    recovery_dead_lettered = recovery_dead_lettered_state
                    already_published = _stream_handoff_state(
                        redis,
                        result_stream,
                        fq_stream,
                        entry_id,
                        task.id,
                        logger,
                    )
                    if already_published is None:
                        return False, drained
                    if already_published or recovery_dead_lettered:
                        recovery_result_published = True
                    else:
                        try:
                            # Publish result to the correct project's results stream
                            if task.key_prefix:
                                fq_results = f"{task.key_prefix}:{RESULTS_STREAM}"
                                _atomic_xadd_capped_with_marker(
                                    redis,
                                    fq_results,
                                    fq_stream,
                                    entry_id,
                                    task.id,
                                    task_result.to_dict(),
                                    raw_target=True,
                                    logical_target=RESULTS_STREAM,
                                )
                            else:
                                _atomic_xadd_capped_with_marker(
                                    redis,
                                    _fully_qualified_stream(redis, RESULTS_STREAM),
                                    fq_stream,
                                    entry_id,
                                    task.id,
                                    task_result.to_dict(),
                                    raw_target=False,
                                    logical_target=RESULTS_STREAM,
                                )
                            recovery_result_published = True
                        except Exception:
                            logger.error(
                                f"Failed to publish recovery result for task {task.id}",
                                exc_info=True,
                            )
            except (KeyError, ValueError) as e:
                logger.error(
                    f"Malformed pending entry {entry_id}: {e}; dead-lettering safely",
                    exc_info=True,
                )
                source_acked = _handoff_dead_letter_fields_until_terminal(
                    redis,
                    fq_stream,
                    entry_id,
                    _safe_raw_dead_letter_fields(
                        fields,
                        fq_stream,
                        entry_id,
                        f"Malformed pending task payload: {e}",
                    ),
                    fields.get("id", f"malformed:{entry_id}"),
                    logger,
                    abort,
                    retry=False,
                    coordination=_malformed_coordination_identity(fields, logger),
                )
                if not source_acked:
                    return False, drained
            # Do not ACK a valid task until its recovery result is durable (or
            # a different pending task proves this entry stale). Otherwise a
            # Redis outage can erase the only recoverable handoff.
            should_ack = task is None or recovery_result_published or not should_publish_recovery
            if task is not None and recovery_dead_lettered:
                try:
                    if not _cleanup_coordination_once(redis, _task_coordination_identity(task)):
                        return False, drained
                except Exception:
                    logger.warning(
                        "Startup DLQ coordination cleanup failed for task %s",
                        task.id,
                        exc_info=True,
                    )
                    return False, drained
            if should_ack and not source_acked:
                try:
                    redis.xack_raw(fq_stream, CONSUMER_GROUP, entry_id)
                    source_acked = True
                except Exception:
                    logger.error(
                        f"Failed to ACK pending entry {entry_id}",
                        exc_info=True,
                    )
                    return False, drained
            if source_acked and task is not None:
                if recovery_result_published:
                    _delete_handoff_marker_best_effort(
                        redis,
                        result_stream,
                        fq_stream,
                        entry_id,
                        task.id,
                        logger,
                    )
                if recovery_dead_lettered:
                    _delete_handoff_marker_best_effort(
                        redis,
                        _fully_qualified_stream(redis, DEAD_LETTER_STREAM),
                        fq_stream,
                        entry_id,
                        task.id,
                        logger,
                    )
            if task is not None and should_publish_recovery and not recovery_result_published:
                # Preserve the PEL entry, but stop this drain pass. Reading ID
                # 0 again would return the same entry immediately and hammer a
                # degraded/wrong-type results stream in a tight loop.
                logger.warning(
                    "Stopping pending-task drain after recovery publication failed for %s; "
                    "the PEL entry will be retried on the next worker restart",
                    task.id,
                )
                return False, drained
    if drained:
        logger.info(f"Drained {drained} pending task(s) from {fq_stream}")
    return True, drained


def _dead_letter_task(
    redis: RedisClient,
    tasks_stream: str,
    entry_id: str,
    task: Task,
    delivery_count: int,
    logger: logging.Logger,
    abort_event: threading.Event | None = None,
) -> bool:
    """Route a task that has exceeded MAX_DELIVERY_COUNT to the dead-letter stream.

    The dead-letter write is the durability boundary. Once it succeeds, only
    XACK is retried, preventing an ACK outage from creating repeated DLQ rows.
    Coordination is released only after ACK succeeds and this task's pending
    marker is atomically cleared. Returns whether the complete handoff is
    terminal; shutdown preserves the source PEL entry on any incomplete step.
    """
    dl_fields = {
        **task.to_safe_dict(),
        "dead_letter_reason": f"Exceeded max delivery count ({MAX_DELIVERY_COUNT})",
        "tasks_stream": tasks_stream,
        "original_entry_id": entry_id,
        "delivery_count": str(delivery_count),
    }
    terminal = _handoff_dead_letter_fields_until_terminal(
        redis,
        tasks_stream,
        entry_id,
        dl_fields,
        task.id,
        logger,
        abort_event,
        coordination=_task_coordination_identity(task),
    )
    if not terminal:
        return False
    logger.error(
        f"Task {task.id} ({task.type.value} for {task.resource_type} "
        f"#{task.resource_id}) exceeded max delivery count "
        f"({MAX_DELIVERY_COUNT}); routed to {DEAD_LETTER_STREAM!r}"
    )
    return True


def _publish_result_with_retry(
    redis: RedisClient,
    result: TaskResult,
    task: Task,
    logger: logging.Logger,
    tasks_stream: str,
    entry_id: str,
    abort_event: threading.Event | None = None,
) -> ResultPublishOutcome:
    """Publish a task result to RESULTS_STREAM with exponential backoff retry.

    Attempts up to _RESULT_PUBLISH_RETRIES times, sleeping _RESULT_PUBLISH_BACKOFF
    seconds between consecutive attempts.  If all attempts fail, writes the result
    and full task context to DEAD_LETTER_STREAM for manual recovery.  The dead-letter
    entry includes ``tasks_stream`` and ``original_entry_id`` so that
    ``orcest dead-letters --replay`` can re-enqueue it.

    Returns a distinct outcome for a primary result publish, a durable
    dead-letter fallback, total publish loss, or an aborted backoff. Callers
    may ACK the source task only when ``outcome.durable`` is true.
    """
    last_exc: Exception | None = None
    result_target_stream = (
        f"{task.key_prefix}:{RESULTS_STREAM}"
        if task.key_prefix
        else _fully_qualified_stream(redis, RESULTS_STREAM)
    )
    result_fields: dict[str, str] | None = None
    credential_checkpoint = None
    # Callers that omit abort_event get a fresh, never-set Event, which means
    # _abort.wait() will block for the full real backoff duration on each retry.
    _abort = abort_event if abort_event is not None else threading.Event()
    for attempt in range(_RESULT_PUBLISH_RETRIES):
        if attempt > 0:
            if _abort.wait(timeout=_RESULT_PUBLISH_BACKOFF[attempt - 1]):
                return ResultPublishOutcome.ABORTED
        try:
            result_fields = result.to_dict()
            if not result_fields.get("repo"):
                result_fields["repo"] = task.repo
            if result.credential_update:
                if credential_checkpoint is None:
                    checkpoint_status, credential_checkpoint = _load_private_credential_checkpoint(
                        redis,
                        result_target_stream,
                        tasks_stream,
                        entry_id,
                        task,
                        logger,
                    )
                    if checkpoint_status is CredentialCheckpointStatus.BLOCKED:
                        logger.error(
                            "Private credential state is blocked for task %s; "
                            "refusing retries or public diagnostics",
                            task.id,
                        )
                        return ResultPublishOutcome.BLOCKED
                    if checkpoint_status is CredentialCheckpointStatus.ABSENT:
                        credential_checkpoint = _store_private_credential_checkpoint(
                            redis,
                            result_target_stream,
                            tasks_stream,
                            entry_id,
                            task.id,
                            result_fields,
                        )
                assert credential_checkpoint is not None
                credential_checkpoint = _version_credential_checkpoint(
                    redis,
                    credential_checkpoint,
                    result_target_stream,
                    tasks_stream,
                    entry_id,
                    task.id,
                )
                result_fields = credential_checkpoint.result_fields
                result.credential_update_minted_at = TaskResult.from_dict(
                    result_fields
                ).credential_update_minted_at
                terminal_outcome = _terminal_credential_handoff_once(
                    redis,
                    credential_checkpoint,
                    result_target_stream,
                    tasks_stream,
                    entry_id,
                    task.id,
                    maxlen=_STREAM_MAXLEN,
                )
                if terminal_outcome is not CredentialTerminalOutcome.COMPLETE:
                    logger.error(
                        "Credential terminal handoff is blocked for task %s (%s); "
                        "refusing to recreate or republish private state",
                        task.id,
                        terminal_outcome.value,
                    )
                    return ResultPublishOutcome.BLOCKED
            else:
                # Ordinary results retain the bounded publish marker followed
                # by a separately retried source ACK.
                _atomic_xadd_capped_with_marker(
                    redis,
                    result_target_stream,
                    tasks_stream,
                    entry_id,
                    task.id,
                    result_fields,
                    raw_target=bool(task.key_prefix),
                    logical_target=RESULTS_STREAM,
                )
            return ResultPublishOutcome.PUBLISHED
        except Exception as exc:
            last_exc = exc
            if (
                result.credential_update
                and result_fields is not None
                and credential_checkpoint is None
            ):
                try:
                    status, _checkpoint = _load_private_credential_checkpoint(
                        redis,
                        result_target_stream,
                        tasks_stream,
                        entry_id,
                        task,
                        logger,
                    )
                    if status is CredentialCheckpointStatus.ABSENT:
                        _store_credential_checkpoint(
                            redis,
                            result_target_stream,
                            tasks_stream,
                            entry_id,
                            task.id,
                            result_fields,
                        )
                except Exception:
                    logger.warning(
                        "Failed to persist private credential recovery checkpoint for task %s",
                        task.id,
                        exc_info=True,
                    )
            logger.warning(
                f"Result publish attempt {attempt + 1}/{_RESULT_PUBLISH_RETRIES} "
                f"failed for task {result.task_id}: {exc}"
            )

    if result.credential_update:
        if result_fields is None:
            logger.error(
                "Credential-update result for task %s could not be checkpointed; "
                "retaining source without public recovery",
                task.id,
            )
            return ResultPublishOutcome.LOST
    # All retries exhausted — send to dead-letter stream for manual recovery.
    logger.error(
        f"All {_RESULT_PUBLISH_RETRIES} result publish attempts failed for task "
        f"{result.task_id}; writing to {DEAD_LETTER_STREAM!r}",
        exc_info=last_exc,
    )
    dead_letter_stream = _fully_qualified_stream(redis, DEAD_LETTER_STREAM)
    # A redacted diagnostic cannot replay a rotated credential. Keep its
    # idempotency marker separate from the terminal DLQ identity inspected by
    # startup recovery, so a restart still publishes a primary recovery result
    # before ACKing the source task.
    dead_letter_handoff_id = (
        f"{_CREDENTIAL_DIAGNOSTIC_HANDOFF_PREFIX}{task.id}" if result.credential_update else task.id
    )
    existing_dead_letter = _stream_handoff_state(
        redis,
        dead_letter_stream,
        tasks_stream,
        entry_id,
        dead_letter_handoff_id,
        logger,
    )
    if existing_dead_letter is True:
        return (
            ResultPublishOutcome.LOST
            if result.credential_update
            else ResultPublishOutcome.DEAD_LETTERED
        )
    if existing_dead_letter is None:
        return ResultPublishOutcome.LOST
    try:
        # Use safe projections so credentials never land in the dead-letter
        # stream (a persistent, human-inspected recovery stream). The task
        # carries the provider credential; the result may carry a rotated
        # OAuth blob (credential_update) — both are redacted here.
        result_fields = result.to_safe_dict()
        if not result_fields.get("repo"):
            result_fields["repo"] = task.repo
        dl_fields = {
            **task.to_safe_dict(),
            **result_fields,
            "dead_letter_reason": (
                f"Result publish failed after {_RESULT_PUBLISH_RETRIES} attempts"
            ),
            "tasks_stream": tasks_stream,
            "original_entry_id": entry_id,
        }
        _atomic_xadd_capped_with_marker(
            redis,
            dead_letter_stream,
            tasks_stream,
            entry_id,
            dead_letter_handoff_id,
            dl_fields,
            raw_target=False,
            logical_target=DEAD_LETTER_STREAM,
        )
        logger.error(
            f"Result for task {result.task_id} written to dead-letter stream "
            f"{DEAD_LETTER_STREAM!r} for manual recovery"
        )
        if result.credential_update:
            logger.error(
                "Task %s carries a credential rotation; redacted diagnostics are not a "
                "terminal handoff, so the source remains pending",
                result.task_id,
            )
            return ResultPublishOutcome.LOST
        return ResultPublishOutcome.DEAD_LETTERED
    except Exception:
        logger.error(
            f"Failed to write result for task {result.task_id} to dead-letter stream; "
            "result is permanently lost",
            exc_info=True,
        )
    return ResultPublishOutcome.LOST


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
    credential_update_minted_at: float = 0.0,
) -> TaskResult:
    return TaskResult(
        task_id=task.id,
        worker_id=config.worker_id,
        status=status,
        resource_type=task.resource_type,
        resource_id=task.resource_id,
        repo=task.repo,
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
        credential_update_minted_at=credential_update_minted_at,
        provider_account=task.provider_account,
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


def _task_output_stream(task: Task, config: WorkerConfig) -> tuple[str, bool]:
    stream = f"output:{config.worker_id}"
    if task.key_prefix:
        return f"{task.key_prefix}:{stream}", True
    return stream, False


def _publish_task_output(
    redis: RedisClient,
    stream: str,
    raw_stream: bool,
    fields: dict[str, str],
) -> None:
    if raw_stream:
        redis.xadd_capped_raw(stream, fields, maxlen=_STREAM_MAXLEN)
    else:
        redis.xadd_capped(stream, fields, maxlen=_STREAM_MAXLEN)


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
    output_stream, output_stream_is_raw = _task_output_stream(task, config)

    def publish_task_end(status: ResultStatus) -> None:
        try:
            _publish_task_output(
                redis,
                output_stream,
                output_stream_is_raw,
                {
                    "type": "task_end",
                    "task_id": task.id,
                    "status": status.value,
                    "worker_id": config.worker_id,
                },
            )
        except Exception:
            logger.warning("Failed to publish task_end marker to Redis", exc_info=True)

    try:
        # Publish task start marker (non-critical; don't fail the task).
        # Extra fields (repo, resource_type, resource_id, provider, worker_id)
        # let the trace archiver materialize a .meta.json sidecar without
        # cross-referencing the results stream.
        try:
            _publish_task_output(
                redis,
                output_stream,
                output_stream_is_raw,
                {
                    "type": "task_start",
                    "task_id": task.id,
                    "resource": f"{task.resource_type} #{task.resource_id}",
                    "repo": task.repo,
                    "resource_type": task.resource_type,
                    "resource_id": str(task.resource_id),
                    "provider": task.provider or config.runner.type,
                    "worker_id": config.worker_id,
                    "branch": task.branch or "",
                },
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
                _publish_task_output(
                    redis,
                    output_stream,
                    output_stream_is_raw,
                    {"line": line, "task_id": task.id},
                )
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
                _publish_task_output(
                    redis,
                    output_stream,
                    output_stream_is_raw,
                    {"line": line, "stream": "stderr", "task_id": task.id},
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
            credential_update_minted_at=runner_result.credential_update_minted_at,
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
