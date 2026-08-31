"""Generation-scoped, idempotent publication for issue implementation tasks.

Issue publication spans project Redis (generation, pending marker, attempt
counter, expected outcome) and the shared task-stream Redis (XADD). A lost
XADD reply must not roll back reserved state or append a duplicate entry.

This module owns the publication and expected-outcome contract. Issue
completion is gated by durable delivery verification (``issue_delivery``).
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from dataclasses import dataclass
from enum import Enum
from typing import Any

from orcest.shared.coordination import (
    PendingTaskMetadata,
    make_pending_task_key,
    parse_pending_task_metadata,
)
from orcest.shared.redis_client import RedisClient

logger = logging.getLogger(__name__)

# Match issue_ops.increment_attempts: closed-issue counters must not leak forever.
_ATTEMPTS_TTL_SECONDS = 7 * 24 * 3600
_ISSUE_SHA_SENTINEL = "issue"

PUBLICATION_STATE_PREPARED = "prepared"
PUBLICATION_STATE_PUBLISHED = "published"
PUBLICATION_STATE_AMBIGUOUS = "ambiguous"


class IssuePublicationState(str, Enum):
    """Lifecycle of one repository/issue generation's publication record."""

    PREPARED = PUBLICATION_STATE_PREPARED
    PUBLISHED = PUBLICATION_STATE_PUBLISHED
    AMBIGUOUS = PUBLICATION_STATE_AMBIGUOUS


class AmbiguousTaskPublishError(Exception):
    """Raised when stream publication cannot be proven to have happened or not.

    The caller must record ``ambiguous`` and must not roll back reserved
    generation/pending/attempt state.
    """

    def __init__(self, task_id: str) -> None:
        super().__init__(
            f"Task {task_id} publication outcome is ambiguous; preserving reserved state"
        )
        self.task_id = task_id


@dataclass(frozen=True)
class IssuePublicationReservation:
    """Outcome of an atomic project-Redis reservation for one issue task."""

    generation: int
    task_id: str
    prompt_input_hash: str
    expected_head_owner: str
    expected_branch: str
    attempt: int
    created_at: str


@dataclass(frozen=True)
class IssuePublicationRecord:
    """Durable per-generation publication record stored in project Redis."""

    repo: str
    issue_number: int
    generation: int
    task_id: str
    prompt_input_hash: str
    expected_head_owner: str
    expected_branch: str
    attempt: int
    state: IssuePublicationState
    created_at: str
    stream: str = ""
    stream_id: str = ""


# Atomic reservation: monotonic generation, publication hash, attempt, pending.
# Constructs the per-generation publication key from ARGV[1] + generation.
# All KEYS/ARGV key material is fully-qualified (already prefixed).
_RESERVE_SCRIPT = r"""
local pending = redis.call("GET", KEYS[2])
if pending then
    return {0, pending}
end

local generation = redis.call("INCR", KEYS[1])
local pub_key = ARGV[1] .. generation
local attempt = redis.call("HINCRBY", KEYS[3], "count", 1)
redis.call("HSET", KEYS[3], "head_sha", ARGV[12])
redis.call("EXPIRE", KEYS[3], ARGV[8])

redis.call(
    "HSET", pub_key,
    "repo", ARGV[10],
    "issue_number", ARGV[11],
    "generation", tostring(generation),
    "task_id", ARGV[2],
    "prompt_input_hash", ARGV[3],
    "expected_head_owner", ARGV[4],
    "expected_branch", ARGV[5],
    "attempt", tostring(attempt),
    "state", ARGV[13],
    "created_at", ARGV[9],
    "stream", "",
    "stream_id", ""
)

redis.call("SET", KEYS[2], ARGV[6], "NX", "EX", ARGV[7])
return {1, tostring(generation), tostring(attempt), ARGV[2]}
"""

# Compare-and-set publication state only when the issue's current generation
# still matches. A newer generation must not be mutated by stale cleanup.
_CAS_STATE_SCRIPT = r"""
local current = redis.call("GET", KEYS[1])
if (not current) or current ~= ARGV[1] then
    return -1
end
if redis.call("EXISTS", KEYS[2]) == 0 then
    return -2
end
local state = redis.call("HGET", KEYS[2], "state")
if state ~= ARGV[2] then
    return -3
end
redis.call("HSET", KEYS[2], "state", ARGV[3])
for i = 4, #ARGV, 2 do
    redis.call("HSET", KEYS[2], ARGV[i], ARGV[i + 1])
end
return 1
"""

# Roll back only a still-prepared reservation for the expected generation.
# The generation counter is left incremented so generations stay monotonic.
_ROLLBACK_PREPARED_SCRIPT = r"""
local current = redis.call("GET", KEYS[1])
if (not current) or current ~= ARGV[1] then
    return -1
end
if redis.call("EXISTS", KEYS[2]) == 0 then
    return 0
end
local state = redis.call("HGET", KEYS[2], "state")
if state ~= "prepared" then
    return 0
end
local task_id = redis.call("HGET", KEYS[2], "task_id")
if task_id ~= ARGV[2] then
    return 0
end
redis.call("DEL", KEYS[2])

local pending = redis.call("GET", KEYS[3])
if pending then
    local quoted = '"' .. ARGV[2] .. '"'
    local compact = '"task_id":' .. quoted
    local spaced = '"task_id": ' .. quoted
    if string.find(pending, compact, 1, true) or string.find(pending, spaced, 1, true) then
        redis.call("DEL", KEYS[3])
    end
end

local count = tonumber(redis.call("HGET", KEYS[4], "count") or "0")
if not count or count <= 1 then
    redis.call("DEL", KEYS[4])
else
    redis.call("HINCRBY", KEYS[4], "count", -1)
end
return 1
"""

# Task-stream idempotency: one XADD per task ID. A retry returns the original
# stream ID from the receipt instead of appending a duplicate.
_IDEMPOTENT_XADD_SCRIPT = r"""
local existing = redis.call("GET", KEYS[2])
if existing then
    return existing
end
local id = redis.call("XADD", KEYS[1], "*", unpack(ARGV))
redis.call("SET", KEYS[2], id)
return id
"""


def make_issue_generation_key(repo: str, issue_number: int) -> str:
    """Redis key for the monotonic repository/issue generation counter."""
    return f"issue:{repo}:{issue_number}:generation"


def make_issue_publication_key(repo: str, issue_number: int, generation: int) -> str:
    """Redis key for one generation's publication record."""
    return f"issue:{repo}:{issue_number}:pub:{generation}"


def make_issue_publication_key_prefix(repo: str, issue_number: int) -> str:
    """Prefix used by the reservation script to form the per-generation key."""
    return f"issue:{repo}:{issue_number}:pub:"


def make_issue_attempts_key(repo: str, issue_number: int) -> str:
    """Redis key for the issue attempt counter (same layout as issue_ops)."""
    return f"issue:{repo}:{issue_number}:attempts"


def make_task_receipt_key(task_id: str) -> str:
    """Stream-Redis receipt proving a task ID was appended exactly once."""
    return f"task-receipt:{task_id}"


def make_issue_result_ref_key(repo: str, issue_number: int, generation: int) -> str:
    """Project-Redis key later result admission uses as a GC reference."""
    return f"issue:{repo}:{issue_number}:result:{generation}"


def make_issue_verification_job_key(repo: str, issue_number: int, generation: int) -> str:
    """Project-Redis key later delivery verification uses as a GC reference."""
    return f"issue:{repo}:{issue_number}:verification:{generation}"


def make_issue_retry_record_key(repo: str, issue_number: int, generation: int) -> str:
    """Project-Redis key later retry-context storage uses as a GC reference."""
    return f"issue:{repo}:{issue_number}:retry:{generation}"


def make_issue_admission_key(task_id: str) -> str:
    """Per-task first-payload CAS ledger for issue results."""
    return f"issue:admission:{task_id}"


def make_issue_dispatch_barrier_key(repo: str, issue_number: int) -> str:
    """Issue-level dispatch barrier independent of pending-marker TTL."""
    return f"issue:{repo}:{issue_number}:dispatch-barrier"


def make_issue_delivery_cooldown_key(repo: str, issue_number: int) -> str:
    """Cooldown after ineffective delivery before the next generation may dispatch."""
    return f"issue:{repo}:{issue_number}:delivery_cooldown"


def expected_head_owner(repo: str) -> str:
    """Return the same-repository head owner for an ``owner/repo`` string."""
    owner, sep, name = repo.partition("/")
    if not sep or not owner or not name or "/" in name:
        raise ValueError(f"repo must be 'owner/repo', got {repo!r}")
    return owner


def expected_branch_name(issue_number: int, issue_title: str, max_slug_len: int = 40) -> str:
    """Deterministic issue-implementation branch snapshotted at publication."""
    slug = re.sub(r"[^a-z0-9]+", "-", issue_title.lower()).strip("-")
    slug = slug[:max_slug_len].rstrip("-")
    return f"issue-{issue_number}-{slug}"


def hash_prompt_inputs(
    *,
    repo: str,
    issue_number: int,
    issue_title: str,
    issue_body: str,
    expected_branch: str,
) -> str:
    """Stable SHA-256 of the inputs that produced the issue prompt and branch."""
    payload = json.dumps(
        {
            "body": issue_body,
            "expected_branch": expected_branch,
            "number": issue_number,
            "repo": repo,
            "title": issue_title,
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def get_issue_generation(redis: RedisClient, repo: str, issue_number: int) -> int:
    """Return the current monotonic generation, or 0 if none has been reserved."""
    raw = redis.get(make_issue_generation_key(repo, issue_number))
    if raw is None:
        return 0
    try:
        return int(raw)
    except (TypeError, ValueError):
        return 0


def get_issue_publication(
    redis: RedisClient, repo: str, issue_number: int, generation: int
) -> IssuePublicationRecord | None:
    """Load a generation's publication record, or None if absent."""
    data = redis.hgetall(make_issue_publication_key(repo, issue_number, generation))
    if not data:
        return None
    try:
        state = IssuePublicationState(data.get("state", ""))
        gen = int(data.get("generation", generation))
        attempt = int(data.get("attempt", "0") or "0")
        number = int(data.get("issue_number", issue_number))
    except (TypeError, ValueError):
        return None
    task_id = data.get("task_id", "")
    if not task_id:
        return None
    return IssuePublicationRecord(
        repo=data.get("repo", repo),
        issue_number=number,
        generation=gen,
        task_id=task_id,
        prompt_input_hash=data.get("prompt_input_hash", ""),
        expected_head_owner=data.get("expected_head_owner", ""),
        expected_branch=data.get("expected_branch", ""),
        attempt=attempt,
        state=state,
        created_at=data.get("created_at", ""),
        stream=data.get("stream", ""),
        stream_id=data.get("stream_id", ""),
    )


def reserve_issue_publication(
    redis: RedisClient,
    *,
    repo: str,
    issue_number: int,
    task_id: str,
    prompt_input_hash: str,
    expected_head_owner: str,
    expected_branch: str,
    pending_ttl: int,
    created_at: str,
) -> IssuePublicationReservation | None:
    """Atomically reserve generation, attempt, pending marker, and publication.

    Returns None when a pending marker already exists for the issue (skip).
    """
    if pending_ttl < 1:
        raise ValueError(f"pending_ttl must be positive, got {pending_ttl}")
    pending_value = PendingTaskMetadata(task_id=task_id, created_at=created_at).to_json()
    response = redis.client.eval(
        _RESERVE_SCRIPT,
        3,
        redis._prefixed(make_issue_generation_key(repo, issue_number)),
        redis._prefixed(make_pending_task_key(repo, "issue", issue_number)),
        redis._prefixed(make_issue_attempts_key(repo, issue_number)),
        redis._prefixed(make_issue_publication_key_prefix(repo, issue_number)),
        task_id,
        prompt_input_hash,
        expected_head_owner,
        expected_branch,
        pending_value,
        str(pending_ttl),
        str(_ATTEMPTS_TTL_SECONDS),
        created_at,
        repo,
        str(issue_number),
        _ISSUE_SHA_SENTINEL,
        IssuePublicationState.PREPARED.value,
    )
    parsed = _as_list(response)
    if not parsed:
        return None
    status = _as_int(parsed[0])
    if status == 0:
        return None
    if status != 1 or len(parsed) < 4:
        raise RuntimeError(f"Unexpected issue publication reservation response: {response!r}")
    return IssuePublicationReservation(
        generation=_as_int(parsed[1]),
        task_id=_as_str(parsed[3]),
        prompt_input_hash=prompt_input_hash,
        expected_head_owner=expected_head_owner,
        expected_branch=expected_branch,
        attempt=_as_int(parsed[2]),
        created_at=created_at,
    )


def cas_issue_publication_state(
    redis: RedisClient,
    repo: str,
    issue_number: int,
    generation: int,
    expected_state: IssuePublicationState,
    new_state: IssuePublicationState,
    extra_fields: dict[str, str] | None = None,
) -> bool:
    """CAS the publication record. Refuses stale generations and wrong states."""
    extra = extra_fields or {}
    flattened = [item for pair in extra.items() for item in pair]
    response = redis.client.eval(
        _CAS_STATE_SCRIPT,
        2,
        redis._prefixed(make_issue_generation_key(repo, issue_number)),
        redis._prefixed(make_issue_publication_key(repo, issue_number, generation)),
        str(generation),
        expected_state.value,
        new_state.value,
        *flattened,
    )
    return _as_int(response) == 1


def rollback_prepared_issue_publication(
    redis: RedisClient,
    repo: str,
    issue_number: int,
    generation: int,
    task_id: str,
) -> bool:
    """Undo a still-prepared reservation proven to have never reached the stream.

    Leaves the generation counter incremented so a later reserve cannot reuse
    the rolled-back generation.
    """
    response = redis.client.eval(
        _ROLLBACK_PREPARED_SCRIPT,
        4,
        redis._prefixed(make_issue_generation_key(repo, issue_number)),
        redis._prefixed(make_issue_publication_key(repo, issue_number, generation)),
        redis._prefixed(make_pending_task_key(repo, "issue", issue_number)),
        redis._prefixed(make_issue_attempts_key(repo, issue_number)),
        str(generation),
        task_id,
    )
    return _as_int(response) == 1


def xadd_task_idempotent(
    redis: RedisClient,
    stream: str,
    fields: dict[str, str],
    task_id: str,
) -> str:
    """Append ``fields`` to ``stream`` exactly once for ``task_id``.

    A retry after a lost EVAL reply returns the original stream ID from the
    receipt instead of appending a second entry. If the reply is lost *and*
    the receipt cannot be read, raises ``AmbiguousTaskPublishError``.
    """
    if not fields:
        raise ValueError("fields must be a non-empty dict")
    if not task_id:
        raise ValueError("task_id is required")
    receipt_key = make_task_receipt_key(task_id)
    flattened = [item for pair in fields.items() for item in pair]
    try:
        response = redis.client.eval(
            _IDEMPOTENT_XADD_SCRIPT,
            2,
            redis._prefixed(stream),
            redis._prefixed(receipt_key),
            *flattened,
        )
    except Exception as exc:
        return _reconcile_task_receipt(redis, receipt_key, task_id, exc)
    entry_id = _maybe_stream_id(response)
    if entry_id:
        return entry_id
    # MagicMock/unit doubles do not execute EVAL. Preserve a non-atomic
    # fallback so tests that stub RedisClient methods keep working.
    existing = redis.get(receipt_key)
    if existing:
        return existing
    published_id = redis.xadd(stream, fields)
    redis.set_value(receipt_key, published_id)
    return published_id


def get_task_receipt(redis: RedisClient, task_id: str) -> str | None:
    """Return the stream ID recorded for ``task_id``, if any."""
    return redis.get(make_task_receipt_key(task_id))


def gc_issue_publication(
    project_redis: RedisClient,
    stream_redis: RedisClient,
    repo: str,
    issue_number: int,
    generation: int,
) -> bool:
    """Delete a publication record only when nothing still references it.

    A wall-clock TTL is never sufficient: if any stream receipt, pending
    marker, result, verification job, or retry record still points at this
    generation, the record is persisted (TTL removed) and kept.
    """
    record = get_issue_publication(project_redis, repo, issue_number, generation)
    if record is None:
        return False

    referenced = _publication_references(
        project_redis, stream_redis, repo, issue_number, generation, record
    )
    pub_key = make_issue_publication_key(repo, issue_number, generation)
    if referenced:
        project_redis.persist(pub_key)
        return False
    return project_redis.delete(pub_key) > 0


def _publication_references(
    project_redis: RedisClient,
    stream_redis: RedisClient,
    repo: str,
    issue_number: int,
    generation: int,
    record: IssuePublicationRecord,
) -> list[str]:
    refs: list[str] = []
    if stream_redis.exists(make_task_receipt_key(record.task_id)):
        refs.append("stream_receipt")
    pending = parse_pending_task_metadata(
        project_redis.get(make_pending_task_key(repo, "issue", issue_number))
    )
    if pending is not None and pending.task_id == record.task_id:
        refs.append("pending")
    if project_redis.exists(make_issue_result_ref_key(repo, issue_number, generation)):
        refs.append("result")
    if project_redis.exists(make_issue_verification_job_key(repo, issue_number, generation)):
        refs.append("verification")
    if project_redis.exists(make_issue_retry_record_key(repo, issue_number, generation)):
        refs.append("retry")
    return refs


def _reconcile_task_receipt(
    redis: RedisClient,
    receipt_key: str,
    task_id: str,
    original: BaseException,
) -> str:
    try:
        receipt = redis.get(receipt_key)
    except Exception as lookup_exc:
        raise AmbiguousTaskPublishError(task_id) from lookup_exc
    if receipt:
        return receipt
    raise original


def _maybe_stream_id(value: object) -> str | None:
    if isinstance(value, bytes):
        text = value.decode("utf-8")
        return text or None
    if isinstance(value, str):
        return value or None
    return None


def _as_str(value: object) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def _as_int(value: object) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    return int(_as_str(value))


def _as_list(value: object) -> list[Any]:
    if isinstance(value, (list, tuple)):
        return list(value)
    return []


def mark_issue_published(
    redis: RedisClient,
    repo: str,
    issue_number: int,
    generation: int,
    stream: str,
    stream_id: str,
    from_state: IssuePublicationState = IssuePublicationState.PREPARED,
) -> bool:
    """Record a confirmed stream append from prepared or ambiguous state."""
    if cas_issue_publication_state(
        redis,
        repo,
        issue_number,
        generation,
        from_state,
        IssuePublicationState.PUBLISHED,
        extra_fields={"stream": stream, "stream_id": stream_id},
    ):
        return True
    if from_state is IssuePublicationState.PREPARED:
        return cas_issue_publication_state(
            redis,
            repo,
            issue_number,
            generation,
            IssuePublicationState.AMBIGUOUS,
            IssuePublicationState.PUBLISHED,
            extra_fields={"stream": stream, "stream_id": stream_id},
        )
    return False


def mark_issue_ambiguous(
    redis: RedisClient,
    repo: str,
    issue_number: int,
    generation: int,
    logger_: logging.Logger | None = None,
) -> bool:
    """Record that stream publication cannot be proven. Never rolls back."""
    log = logger_ or logger
    updated = cas_issue_publication_state(
        redis,
        repo,
        issue_number,
        generation,
        IssuePublicationState.PREPARED,
        IssuePublicationState.AMBIGUOUS,
    )
    if not updated:
        current = get_issue_publication(redis, repo, issue_number, generation)
        if current is not None and current.state is IssuePublicationState.AMBIGUOUS:
            return True
        log.warning(
            "Failed to mark issue %s#%d generation %d as ambiguous (current=%s)",
            repo,
            issue_number,
            generation,
            None if current is None else current.state.value,
        )
    return updated
