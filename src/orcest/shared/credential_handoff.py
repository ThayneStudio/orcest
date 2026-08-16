"""Private, crash-safe terminal handoff for rotated provider credentials.

Credential updates cannot use the public dead-letter stream because that would
expose a reusable secret.  This module is deliberately shared by workers and
the fleet reaper: both must prefer the private checkpoint over generic failure
recovery and finish the same Redis-side handoff.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
from dataclasses import dataclass
from enum import Enum
from typing import cast

from orcest.shared.models import CONSUMER_GROUP, REDACTED_FIELDS, Task, TaskResult
from orcest.shared.redis_client import RedisClient

CREDENTIAL_UPDATE_VERSION_KEY = "providers:credential_update_version"
CREDENTIAL_DIAGNOSTIC_HANDOFF_PREFIX = "credential-update-diagnostic:"
HANDOFF_FINGERPRINT_FIELD = "orcest_handoff_fingerprint"
HANDOFF_MARKER_TTL_SECONDS = 30 * 24 * 3600
# The private checkpoint holds a plaintext rotated OAuth blob. The terminal
# handoff deletes it as soon as the result is durable, so this TTL is only a
# backstop for checkpoints that can never reach that path -- most concretely,
# `XGROUP DELCONSUMER` (the documented operator fix for a stalled queue)
# discards the consumer's PEL entries, leaving the checkpoint unreachable and,
# previously, immortal.
#
# It must stay strictly longer than HANDOFF_MARKER_TTL_SECONDS: the reaper is
# required to finish a checkpoint-only handoff after the bounded public
# diagnostic/receipt markers have already expired, so the private checkpoint
# has to outlive them. Being far longer than any task can run
# (pool.max_task_duration is hours) it can never expire under live work.
CREDENTIAL_CHECKPOINT_TTL_SECONDS = 90 * 24 * 3600
if CREDENTIAL_CHECKPOINT_TTL_SECONDS <= HANDOFF_MARKER_TTL_SECONDS:
    raise ValueError("credential checkpoints must outlive the public handoff markers")
_CREDENTIAL_INTENT_VALUE = "1"


class CredentialCheckpointStatus(Enum):
    ABSENT = "absent"
    VALID = "valid"
    BLOCKED = "blocked"


class CredentialRecoveryOutcome(Enum):
    ABSENT = "absent"
    RECOVERED = "recovered"
    BLOCKED = "blocked"


class CredentialTerminalOutcome(Enum):
    COMPLETE = "complete"
    ABSENT = "absent"
    MISMATCH = "mismatch"


@dataclass(frozen=True)
class CredentialCheckpoint:
    """Validated private checkpoint plus its exact serialized Redis value."""

    key: str
    serialized: str
    result_fields: dict[str, str]


# Redis scripts do not roll back commands preceding a runtime error. The
# persistent ``inflight`` receipt therefore precedes XACK and remains alongside
# checkpoint + intent if XACK errors. Only after XPENDING proves the source is
# gone does the script replace it with a bounded ``terminal`` proof and delete
# the private state. A caller recovering a lost EVAL response accepts an absent
# checkpoint only when that proof verifies both the exact result row and ACK
# state.
ATOMIC_CREDENTIAL_TERMINAL_LUA = r"""
local function verified_receipt()
  local marker = redis.call('GET', KEYS[2])
  if not marker then
    return '', ''
  end
  local first = string.find(marker, '|', 1, true)
  if not first then
    return '', ''
  end
  local second = string.find(marker, '|', first + 1, true)
  local state = 'inflight'
  local candidate_id = string.sub(marker, 1, first - 1)
  local candidate_fingerprint = string.sub(marker, first + 1)
  if second then
    state = string.sub(marker, 1, first - 1)
    candidate_id = string.sub(marker, first + 1, second - 1)
    candidate_fingerprint = string.sub(marker, second + 1)
  end
  if candidate_fingerprint ~= ARGV[7] then
    return '', ''
  end
  local rows = redis.call('XRANGE', KEYS[1], candidate_id, candidate_id, 'COUNT', 1)
  if #rows ~= 1 then
    return '', ''
  end
  local row = rows[1][2]
  if #row ~= (#ARGV - 8) then
    return '', ''
  end
  for expected = 9, #ARGV, 2 do
    local matched = false
    for actual = 1, #row, 2 do
      if row[actual] == ARGV[expected] and row[actual + 1] == ARGV[expected + 1] then
        matched = true
        break
      end
    end
    if not matched then
      return '', ''
    end
  end
  return state, candidate_id
end

local function pending_state()
  local pending = redis.pcall(
    'XPENDING', KEYS[3], ARGV[2], ARGV[3], ARGV[3], 1
  )
  if type(pending) == 'table' and pending.err then
    return -1
  end
  if #pending == 0 then
    return 0
  end
  return 1
end

local checkpoint = redis.call('GET', KEYS[4])
if not checkpoint then
  local state, entry_id = verified_receipt()
  if state == 'terminal' and pending_state() == 0 then
    redis.call('DEL', KEYS[5])
    return {3, entry_id, 0}
  end
  return {2, '', 0}
end
if checkpoint ~= ARGV[6] then
  return {-2, '', 0}
end
if redis.call('GET', KEYS[5]) ~= ARGV[5] then
  return {-3, '', 0}
end

local receipt_state, entry_id = verified_receipt()

local created = 0
if entry_id == '' then
  local xargs = {}
  for i = 9, #ARGV do
    table.insert(xargs, ARGV[i])
  end
  entry_id = redis.call('XADD', KEYS[1], 'MAXLEN', '~', ARGV[1], '*', unpack(xargs))
  redis.call('SET', KEYS[2], 'inflight|' .. entry_id .. '|' .. ARGV[7])
  created = 1
end

local acknowledged = redis.call('XACK', KEYS[3], ARGV[2], ARGV[3])
if pending_state() ~= 0 then
  return {-4, entry_id, acknowledged}
end
redis.call(
  'SET', KEYS[2], 'terminal|' .. entry_id .. '|' .. ARGV[7],
  'EX', ARGV[8]
)
redis.call('DEL', KEYS[4])
redis.call('DEL', KEYS[5])
return {created, entry_id, acknowledged}
"""

_CREATE_CREDENTIAL_CHECKPOINT_LUA = r"""
if redis.call('EXISTS', KEYS[2]) == 1 then
  return -1
end
local checkpoint_created = redis.call('SET', KEYS[1], ARGV[1], 'NX', 'EX', ARGV[3])
if not checkpoint_created then
  return 0
end
local intent_created = redis.call('SET', KEYS[2], ARGV[2], 'NX', 'EX', ARGV[3])
if not intent_created then
  redis.call('DEL', KEYS[1])
  return -1
end
return 1
"""

_CAS_CREDENTIAL_CHECKPOINT_LUA = r"""
local current = redis.call('GET', KEYS[1])
if not current then
  return 0
end
if current ~= ARGV[1] then
  return -1
end
if redis.call('GET', KEYS[2]) ~= ARGV[3] then
  return -2
end
redis.call('SET', KEYS[1], ARGV[2], 'KEEPTTL')
return 1
"""

_MIGRATE_V1_INTENT_LUA = r"""
local current = redis.call('GET', KEYS[1])
if not current then
  return 0
end
if current ~= ARGV[1] then
  return -1
end
local intent = redis.call('GET', KEYS[2])
if intent then
  if intent ~= ARGV[2] then
    return -2
  end
else
  local created = redis.call('SET', KEYS[2], ARGV[2], 'NX', 'EX', ARGV[3])
  if not created then
    return -2
  end
end
redis.call('EXPIRE', KEYS[1], ARGV[3])
redis.call('EXPIRE', KEYS[2], ARGV[3])
return 1
"""

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
if tonumber(ARGV[2]) > 0 then
  redis.call('SET', KEYS[2], entry_id .. '|' .. ARGV[3], 'EX', ARGV[2])
else
  redis.call('SET', KEYS[2], entry_id .. '|' .. ARGV[3])
end
return {1, entry_id}
"""


def stream_handoff_identity(
    target_stream: str,
    tasks_stream: str,
    entry_id: str,
    task_id: str,
) -> str:
    identity = "\0".join((target_stream, tasks_stream, entry_id, task_id))
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def handoff_marker_key(
    target_stream: str,
    tasks_stream: str,
    entry_id: str,
    task_id: str,
) -> str:
    identity = stream_handoff_identity(target_stream, tasks_stream, entry_id, task_id)
    return f"{target_stream}:handoff:{identity}"


def credential_checkpoint_key(
    target_stream: str,
    tasks_stream: str,
    entry_id: str,
    task_id: str,
) -> str:
    identity = stream_handoff_identity(target_stream, tasks_stream, entry_id, task_id)
    return f"{target_stream}:private-credential-recovery:{identity}"


def credential_intent_key(
    target_stream: str,
    tasks_stream: str,
    entry_id: str,
    task_id: str,
) -> str:
    identity = stream_handoff_identity(target_stream, tasks_stream, entry_id, task_id)
    return f"{target_stream}:credential-recovery-intent:{identity}"


def handoff_payload_fingerprint(fields: dict[str, str]) -> str:
    payload = json.dumps(fields, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _set_nx_succeeded(response: object) -> bool:
    """Redis SET NX succeeds with True/OK and collides with None/False."""
    if response is None or response is False:
        return False
    if isinstance(response, bytes):
        return response == b"OK"
    if isinstance(response, str):
        return response == "OK"
    return bool(response)


def source_entry_pending_state(
    redis: RedisClient,
    tasks_stream: str,
    entry_id: str,
) -> bool | None:
    """Return exact PEL membership, or ``None`` when Redis cannot prove it."""
    try:
        pending = redis.client.xpending_range(
            tasks_stream,
            CONSUMER_GROUP,
            min=entry_id,
            max=entry_id,
            count=1,
        )
    except Exception:
        return None
    if not isinstance(pending, list):
        return None
    return bool(pending)


def safe_dead_letter_fields(
    fields: dict[str, str],
    tasks_stream: str,
    entry_id: str,
    reason: str,
) -> dict[str, str]:
    """Strictly project an untrusted task payload into a secret-free DLQ row."""
    allowlist = {
        "id",
        "type",
        "repo",
        "resource_type",
        "resource_id",
        "provider",
        "model",
        "branch",
        "base_branch",
        "key_prefix",
        "created_at",
        "snapshot_head_sha",
        "decision_reason",
        "provider_account",
    }
    safe = {name: value for name, value in fields.items() if name in allowlist}
    for name in REDACTED_FIELDS:
        if name in fields:
            safe[name] = "[REDACTED]"
    safe.update(
        {
            "dead_letter_reason": reason,
            "tasks_stream": tasks_stream,
            "original_entry_id": entry_id,
        }
    )
    return safe


def publish_handoff_once(
    redis: RedisClient,
    target_stream: str,
    tasks_stream: str,
    entry_id: str,
    handoff_id: str,
    fields: dict[str, str],
    *,
    maxlen: int,
    marker_ttl_seconds: int | None = HANDOFF_MARKER_TTL_SECONDS,
) -> str:
    """Idempotently append a bounded stream handoff using an exact-row receipt."""
    fingerprint = handoff_payload_fingerprint(fields)
    marked = {**fields, HANDOFF_FINGERPRINT_FIELD: fingerprint}
    marker_key = handoff_marker_key(target_stream, tasks_stream, entry_id, handoff_id)
    flattened = [item for pair in marked.items() for item in pair]
    response = cast(
        object,
        redis.client.eval(
            _ATOMIC_XADD_MARKER_LUA,
            2,
            target_stream,
            marker_key,
            str(maxlen),
            str(marker_ttl_seconds or 0),
            fingerprint,
            HANDOFF_FINGERPRINT_FIELD,
            *flattened,
        ),
    )
    if isinstance(response, (list, tuple)) and len(response) == 2:
        result_id = response[1]
        return result_id.decode("utf-8") if isinstance(result_id, bytes) else str(result_id)
    result_id = redis.xadd_capped_raw(target_stream, marked, maxlen=maxlen)
    if marker_ttl_seconds is None:
        redis.client.set(marker_key, f"{result_id}|{fingerprint}")
    else:
        redis.client.set(
            marker_key,
            f"{result_id}|{fingerprint}",
            ex=marker_ttl_seconds,
        )
    return result_id


def _checkpoint_serialized(
    target_stream: str,
    tasks_stream: str,
    entry_id: str,
    task_id: str,
    result_fields: dict[str, str],
) -> str:
    payload = {
        "version": 2,
        "target_stream": target_stream,
        "tasks_stream": tasks_stream,
        "entry_id": entry_id,
        "task_id": task_id,
        "result_fingerprint": handoff_payload_fingerprint(result_fields),
        "result": result_fields,
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def store_credential_checkpoint(
    redis: RedisClient,
    target_stream: str,
    tasks_stream: str,
    entry_id: str,
    task_id: str,
    result_fields: dict[str, str],
) -> CredentialCheckpoint:
    """Create private checkpoint + nonsecret intent exactly once.

    Existing state is authoritative. Callers that race this creation must load
    and validate it instead of overwriting it.
    """
    key = credential_checkpoint_key(target_stream, tasks_stream, entry_id, task_id)
    intent_key = credential_intent_key(target_stream, tasks_stream, entry_id, task_id)
    serialized = _checkpoint_serialized(
        target_stream, tasks_stream, entry_id, task_id, result_fields
    )
    response = cast(
        object,
        redis.client.eval(
            _CREATE_CREDENTIAL_CHECKPOINT_LUA,
            2,
            key,
            intent_key,
            serialized,
            _CREDENTIAL_INTENT_VALUE,
            str(CREDENTIAL_CHECKPOINT_TTL_SECONDS),
        ),
    )
    if isinstance(response, int):
        if response != 1:
            raise RuntimeError("private credential checkpoint already exists or is incomplete")
    else:
        # Unit-test doubles cannot execute Lua. Real Redis always uses the
        # atomic script above.
        raw_checkpoint = redis.client.get(key)
        raw_intent = redis.client.get(intent_key)
        checkpoint_absent = not isinstance(raw_checkpoint, (str, bytes))
        intent_absent = not isinstance(raw_intent, (str, bytes))
        if not checkpoint_absent or not intent_absent:
            raise RuntimeError("private credential checkpoint already exists or is incomplete")
        created = redis.client.set(key, serialized, nx=True, ex=CREDENTIAL_CHECKPOINT_TTL_SECONDS)
        if not _set_nx_succeeded(created):
            raise RuntimeError("private credential checkpoint creation raced")
        intent_created = redis.client.set(
            intent_key,
            _CREDENTIAL_INTENT_VALUE,
            nx=True,
            ex=CREDENTIAL_CHECKPOINT_TTL_SECONDS,
        )
        if not _set_nx_succeeded(intent_created):
            # The fallback is not transactional. Remove only the exact value
            # this actor created; never delete a concurrent replacement.
            current = redis.client.get(key)
            if isinstance(current, bytes):
                current = current.decode("utf-8")
            if current == serialized:
                redis.client.delete(key)
            raise RuntimeError("private credential intent creation raced")
    return CredentialCheckpoint(key, serialized, result_fields)


def _migrate_v1_intent_once(
    redis: RedisClient,
    checkpoint_key: str,
    intent_key: str,
    serialized: str,
) -> bool:
    """Add a v1 intent only while the exact validated checkpoint remains."""
    response = cast(
        object,
        redis.client.eval(
            _MIGRATE_V1_INTENT_LUA,
            2,
            checkpoint_key,
            intent_key,
            serialized,
            _CREDENTIAL_INTENT_VALUE,
            str(CREDENTIAL_CHECKPOINT_TTL_SECONDS),
        ),
    )
    if isinstance(response, int):
        return response == 1
    current = redis.client.get(checkpoint_key)
    if isinstance(current, bytes):
        current = current.decode("utf-8")
    if current != serialized:
        return False
    current_intent = redis.client.get(intent_key)
    if isinstance(current_intent, bytes):
        current_intent = current_intent.decode("utf-8")
    if isinstance(current_intent, str):
        if current_intent != _CREDENTIAL_INTENT_VALUE:
            return False
    elif not _set_nx_succeeded(
        redis.client.set(
            intent_key,
            _CREDENTIAL_INTENT_VALUE,
            nx=True,
            ex=CREDENTIAL_CHECKPOINT_TTL_SECONDS,
        )
    ):
        return False
    current = redis.client.get(checkpoint_key)
    if isinstance(current, bytes):
        current = current.decode("utf-8")
    if current != serialized:
        return False
    expire = getattr(redis.client, "expire", None)
    if callable(expire):
        expire(checkpoint_key, CREDENTIAL_CHECKPOINT_TTL_SECONDS)
        expire(intent_key, CREDENTIAL_CHECKPOINT_TTL_SECONDS)
    return True


def load_credential_checkpoint(
    redis: RedisClient,
    target_stream: str,
    tasks_stream: str,
    entry_id: str,
    task: Task,
    logger: logging.Logger,
) -> tuple[CredentialCheckpointStatus, CredentialCheckpoint | None]:
    """Load and strictly validate a task-scoped private checkpoint."""
    key = credential_checkpoint_key(target_stream, tasks_stream, entry_id, task.id)
    intent_key = credential_intent_key(target_stream, tasks_stream, entry_id, task.id)
    try:
        raw = redis.client.get(key)
        raw_intent = redis.client.get(intent_key)
    except Exception:
        logger.warning(
            "Failed to read private credential recovery checkpoint for task %s",
            task.id,
            exc_info=True,
        )
        return CredentialCheckpointStatus.BLOCKED, None
    intent_present = isinstance(raw_intent, (str, bytes))
    if raw is None or not isinstance(raw, (str, bytes)):
        if intent_present:
            logger.error(
                "Private credential intent exists without a valid checkpoint for task %s; "
                "retaining the source entry",
                task.id,
            )
            return CredentialCheckpointStatus.BLOCKED, None
        return CredentialCheckpointStatus.ABSENT, None
    try:
        serialized = raw.decode("utf-8") if isinstance(raw, bytes) else raw
        payload = json.loads(serialized)
        if not isinstance(payload, dict):
            raise ValueError("checkpoint is not an object")
        if payload.get("version") not in {1, 2}:
            raise ValueError("checkpoint version is unsupported")
        expected_identity = {
            "target_stream": target_stream,
            "tasks_stream": tasks_stream,
            "entry_id": entry_id,
            "task_id": task.id,
        }
        if any(payload.get(name) != value for name, value in expected_identity.items()):
            raise ValueError("checkpoint identity mismatch")
        result_fields = payload.get("result")
        if not isinstance(result_fields, dict) or not all(
            isinstance(name, str) and isinstance(value, str)
            for name, value in result_fields.items()
        ):
            raise ValueError("checkpoint result is malformed")
        if payload.get("result_fingerprint") != handoff_payload_fingerprint(result_fields):
            raise ValueError("checkpoint fingerprint mismatch")
        result = TaskResult.from_dict(result_fields)
        if (
            result.task_id != task.id
            or result.repo != task.repo
            or result.resource_type != task.resource_type
            or result.resource_id != task.resource_id
            or not result.credential_update
            or not math.isfinite(result.credential_update_minted_at)
            or result.credential_update_minted_at < 0
        ):
            raise ValueError("checkpoint result identity mismatch")
    except (KeyError, TypeError, ValueError, json.JSONDecodeError):
        logger.error(
            "Private credential recovery checkpoint is invalid for task %s; "
            "retaining the source entry",
            task.id,
        )
        return CredentialCheckpointStatus.BLOCKED, None
    checkpoint_version = int(payload["version"])
    if checkpoint_version == 1:
        try:
            migrated = _migrate_v1_intent_once(
                redis,
                key,
                intent_key,
                serialized,
            )
        except Exception:
            migrated = False
            logger.warning(
                "Failed to migrate private v1 credential intent for task %s",
                task.id,
                exc_info=True,
            )
        if not migrated:
            logger.error(
                "Private v1 credential checkpoint changed during intent migration for task %s; "
                "retaining the source entry",
                task.id,
            )
            return CredentialCheckpointStatus.BLOCKED, None
    elif intent_present:
        if isinstance(raw_intent, bytes):
            raw_intent = raw_intent.decode("utf-8")
        if raw_intent != _CREDENTIAL_INTENT_VALUE:
            logger.error(
                "Private credential intent is invalid for task %s; retaining the source entry",
                task.id,
            )
            return CredentialCheckpointStatus.BLOCKED, None
    else:
        logger.error(
            "Private credential checkpoint lacks its durable intent for task %s; "
            "retaining the source entry",
            task.id,
        )
        return CredentialCheckpointStatus.BLOCKED, None
    return (
        CredentialCheckpointStatus.VALID,
        CredentialCheckpoint(key, serialized, result_fields),
    )


def version_credential_checkpoint(
    redis: RedisClient,
    checkpoint: CredentialCheckpoint,
    target_stream: str,
    tasks_stream: str,
    entry_id: str,
    task_id: str,
) -> CredentialCheckpoint:
    """Allocate and persist the trusted version if the checkpoint lacks one."""
    result = TaskResult.from_dict(checkpoint.result_fields)
    if result.credential_update_minted_at > 0:
        return checkpoint
    minted_at = float(redis.next_monotonic_version(CREDENTIAL_UPDATE_VERSION_KEY))
    if not math.isfinite(minted_at) or minted_at <= 0:
        raise ValueError("credential version is not positive and finite")
    fields = {**checkpoint.result_fields, "credential_update_minted_at": str(minted_at)}
    serialized = _checkpoint_serialized(target_stream, tasks_stream, entry_id, task_id, fields)
    intent_key = credential_intent_key(target_stream, tasks_stream, entry_id, task_id)
    response = cast(
        object,
        redis.client.eval(
            _CAS_CREDENTIAL_CHECKPOINT_LUA,
            2,
            checkpoint.key,
            intent_key,
            checkpoint.serialized,
            serialized,
            _CREDENTIAL_INTENT_VALUE,
        ),
    )
    if isinstance(response, int):
        if response != 1:
            raise RuntimeError("private credential checkpoint changed during versioning")
    else:
        raw_checkpoint = redis.client.get(checkpoint.key)
        if isinstance(raw_checkpoint, bytes):
            raw_checkpoint = raw_checkpoint.decode("utf-8")
        raw_intent = redis.client.get(intent_key)
        if isinstance(raw_intent, bytes):
            raw_intent = raw_intent.decode("utf-8")
        # A non-string MagicMock means the unit double does not model storage;
        # production always takes the CAS script above.
        if isinstance(raw_checkpoint, str) and raw_checkpoint != checkpoint.serialized:
            raise RuntimeError("private credential checkpoint changed during versioning")
        if raw_checkpoint is None or (isinstance(raw_intent, str) and raw_intent != "1"):
            raise RuntimeError("private credential checkpoint changed during versioning")
        redis.client.set(checkpoint.key, serialized, keepttl=True)
    return CredentialCheckpoint(checkpoint.key, serialized, fields)


def _mock_marker_matches(
    redis: RedisClient,
    target_stream: str,
    marker_key: str,
    fingerprint: str,
    expected_fields: dict[str, str],
) -> tuple[str, str] | None:
    """Best-effort idempotency for unit doubles that cannot execute Lua."""
    marker = redis.client.get(marker_key)
    if isinstance(marker, bytes):
        marker = marker.decode("utf-8")
    if not isinstance(marker, str) or "|" not in marker:
        return None
    parts = marker.split("|", 2)
    if len(parts) == 2:
        state = "inflight"
        result_id, marker_fingerprint = parts
    else:
        state, result_id, marker_fingerprint = parts
    if marker_fingerprint != fingerprint:
        return None
    rows = redis.client.xrange(target_stream, min=result_id, max=result_id, count=1)
    if not isinstance(rows, list) or len(rows) != 1:
        return None
    fields = rows[0][1]
    if not isinstance(fields, dict) or fields != expected_fields:
        return None
    return state, result_id


def terminal_credential_handoff_once(
    redis: RedisClient,
    checkpoint: CredentialCheckpoint,
    target_stream: str,
    tasks_stream: str,
    entry_id: str,
    task_id: str,
    *,
    maxlen: int,
) -> CredentialTerminalOutcome:
    """Publish the exact secret result, ACK its source, and erase recovery state.

    Missing/replaced private state never implies success. A lost-response retry
    is complete only when Redis verifies the terminal receipt and ACK state.
    """
    fingerprint = handoff_payload_fingerprint(checkpoint.result_fields)
    marked_fields = {
        **checkpoint.result_fields,
        HANDOFF_FINGERPRINT_FIELD: fingerprint,
    }
    marker_key = handoff_marker_key(target_stream, tasks_stream, entry_id, task_id)
    intent_key = credential_intent_key(target_stream, tasks_stream, entry_id, task_id)
    flattened = [item for pair in marked_fields.items() for item in pair]
    response = cast(
        object,
        redis.client.eval(
            ATOMIC_CREDENTIAL_TERMINAL_LUA,
            5,
            target_stream,
            marker_key,
            tasks_stream,
            checkpoint.key,
            intent_key,
            str(maxlen),
            CONSUMER_GROUP,
            entry_id,
            HANDOFF_FINGERPRINT_FIELD,
            _CREDENTIAL_INTENT_VALUE,
            checkpoint.serialized,
            fingerprint,
            str(HANDOFF_MARKER_TTL_SECONDS),
            *flattened,
        ),
    )
    if isinstance(response, (list, tuple)) and len(response) == 3:
        code = int(response[0])
        if code in {0, 1, 3}:
            return CredentialTerminalOutcome.COMPLETE
        if code == 2:
            return CredentialTerminalOutcome.ABSENT
        return CredentialTerminalOutcome.MISMATCH

    # MagicMock/faker unit doubles do not execute EVAL.  Keep the same safe
    # ordering; real Redis always follows the single-script path above.
    raw_checkpoint = redis.client.get(checkpoint.key)
    if raw_checkpoint is None:
        receipt = _mock_marker_matches(redis, target_stream, marker_key, fingerprint, marked_fields)
        pending = source_entry_pending_state(redis, tasks_stream, entry_id)
        if receipt is not None and receipt[0] == "terminal" and pending is False:
            redis.client.delete(intent_key)
            return CredentialTerminalOutcome.COMPLETE
        return CredentialTerminalOutcome.ABSENT
    if isinstance(raw_checkpoint, bytes):
        raw_checkpoint = raw_checkpoint.decode("utf-8")
    if isinstance(raw_checkpoint, str) and raw_checkpoint != checkpoint.serialized:
        return CredentialTerminalOutcome.MISMATCH
    raw_intent = redis.client.get(intent_key)
    if isinstance(raw_intent, bytes):
        raw_intent = raw_intent.decode("utf-8")
    if raw_intent is None or (isinstance(raw_intent, str) and raw_intent != "1"):
        return CredentialTerminalOutcome.MISMATCH
    receipt = _mock_marker_matches(redis, target_stream, marker_key, fingerprint, marked_fields)
    if receipt is None:
        result_id = redis.xadd_capped_raw(target_stream, marked_fields, maxlen=maxlen)
        redis.client.set(marker_key, f"inflight|{result_id}|{fingerprint}")
    else:
        _receipt_state, result_id = receipt
    redis.xack_raw(tasks_stream, CONSUMER_GROUP, entry_id)
    if source_entry_pending_state(redis, tasks_stream, entry_id) is not False:
        return CredentialTerminalOutcome.MISMATCH
    redis.client.set(
        marker_key,
        f"terminal|{result_id}|{fingerprint}",
        ex=HANDOFF_MARKER_TTL_SECONDS,
    )
    redis.client.delete(checkpoint.key)
    redis.client.delete(intent_key)
    return CredentialTerminalOutcome.COMPLETE


def recover_credential_checkpoint(
    redis: RedisClient,
    task: Task,
    target_stream: str,
    tasks_stream: str,
    entry_id: str,
    diagnostic_stream: str,
    logger: logging.Logger,
    *,
    maxlen: int,
) -> CredentialRecoveryOutcome:
    """Finish a private checkpoint before any generic pending-task recovery."""
    status, checkpoint = load_credential_checkpoint(
        redis, target_stream, tasks_stream, entry_id, task, logger
    )
    if status is CredentialCheckpointStatus.BLOCKED:
        return CredentialRecoveryOutcome.BLOCKED
    if status is CredentialCheckpointStatus.ABSENT:
        diagnostic_id = f"{CREDENTIAL_DIAGNOSTIC_HANDOFF_PREFIX}{task.id}"
        try:
            diagnostic = redis.client.get(
                handoff_marker_key(diagnostic_stream, tasks_stream, entry_id, diagnostic_id)
            )
        except Exception:
            logger.warning(
                "Failed to inspect credential diagnostic marker for task %s",
                task.id,
                exc_info=True,
            )
            return CredentialRecoveryOutcome.BLOCKED
        if not isinstance(diagnostic, (str, bytes)):
            return CredentialRecoveryOutcome.ABSENT
        logger.error(
            "Credential diagnostic recovery state is incomplete for task %s; "
            "retaining the source entry",
            task.id,
        )
        return CredentialRecoveryOutcome.BLOCKED

    assert checkpoint is not None
    try:
        checkpoint = version_credential_checkpoint(
            redis, checkpoint, target_stream, tasks_stream, entry_id, task.id
        )
        terminal = terminal_credential_handoff_once(
            redis,
            checkpoint,
            target_stream,
            tasks_stream,
            entry_id,
            task.id,
            maxlen=maxlen,
        )
        if terminal is not CredentialTerminalOutcome.COMPLETE:
            raise RuntimeError("credential checkpoint changed during terminal handoff")
    except Exception:
        logger.error(
            "Failed terminal private credential recovery for task %s; retaining source",
            task.id,
            exc_info=True,
        )
        return CredentialRecoveryOutcome.BLOCKED
    logger.warning("Recovered private credential checkpoint for task %s", task.id)
    return CredentialRecoveryOutcome.RECOVERED
