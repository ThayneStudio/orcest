"""Durable issue-result admission and delivery verification.

GitHub live state is the authority for issue completion. A provider zero-exit
does not remove ``orcest:ready`` until a verification job observes an open,
non-draft pull request that canonically closes the exact issue.

Control-plane keys for nonterminal jobs, copied expected outcomes, dispatch
barriers, and the admission ledger have no wall-clock TTL. Cleanup is
reference-aware and will not collect active or still-referenced state.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable

from orcest.orchestrator import gh
from orcest.orchestrator.github_delivery_verifier import (
    DeliveryErrorKind,
    DeliveryFailureReason,
    HandoffObservation,
    observe_issue_handoff,
)
from orcest.orchestrator.issue_ops import clear_attempts as clear_issue_attempts
from orcest.orchestrator.issue_publication import (
    gc_issue_publication,
    get_issue_generation,
    get_issue_publication,
    make_issue_admission_key,
    make_issue_delivery_cooldown_key,
    make_issue_dispatch_barrier_key,
    make_issue_result_ref_key,
    make_issue_retry_record_key,
    make_issue_verification_job_key,
)
from orcest.shared.config import IssueDeliveryVerifierConfig, LabelConfig
from orcest.shared.events import EventPublisher, make_event
from orcest.shared.models import ResultStatus, TaskResult
from orcest.shared.redis_client import RedisClient
from orcest.workflow_contract.v1.digest import generic_domain_digest

logger = logging.getLogger(__name__)

DUE_INDEX_KEY = "issue:verification:due"
ACTIVE_JOBS_KEY = "issue:verification:active"
UNVERIFIABLE_INDEX_KEY = "issue:verification:unverifiable"
QUARANTINE_KEY = "issue:result:quarantine"
METRICS_KEY = "issue:delivery:metrics"
GC_DUE_INDEX_KEY = "issue:delivery:gc:due"

ROUTE_COMPLETED_VERIFY = "completed_verify"
ROUTE_NONSUCCESS = "nonsuccess"

_QUARANTINE_MAXLEN = 1000
_PAYLOAD_SUMMARY_LIMIT = 200
_METRICS_TTL_SECONDS = 30 * 24 * 3600
_GENERATION_WALK_CAP = 32

NowFn = Callable[[], float]


class VerificationState(str, Enum):
    PENDING = "pending"
    VERIFIED = "verified"
    INEFFECTIVE = "ineffective"
    UNVERIFIABLE = "unverifiable"


class SagaPhase(str, Enum):
    ADMITTED = "admitted"
    TERMINAL_PERSISTED = "terminal_persisted"
    LABEL_MUTATED = "label_mutated"
    RETRY_RECORDED = "retry_recorded"
    CHECKPOINTED = "checkpointed"
    GENERATION_CLEANED = "generation_cleaned"
    BARRIER_REMOVED = "barrier_removed"


class AdmissionKind(str, Enum):
    ADMITTED = "admitted"
    REPLAY = "replay"
    CONFLICT = "conflict"


TERMINAL_VERIFICATION_STATES = frozenset(
    {VerificationState.VERIFIED, VerificationState.INEFFECTIVE}
)
NONTERMINAL_VERIFICATION_STATES = frozenset(
    {VerificationState.PENDING, VerificationState.UNVERIFIABLE}
)

_UNVERIFIABLE_KINDS = frozenset(
    {
        DeliveryErrorKind.AUTHENTICATION,
        DeliveryErrorKind.PERMISSION,
        DeliveryErrorKind.SCHEMA,
        DeliveryErrorKind.COMPLETENESS,
        DeliveryErrorKind.NOT_FOUND,
    }
)
_RETRY_KINDS = frozenset({DeliveryErrorKind.TRANSPORT, DeliveryErrorKind.RATE_LIMIT})


_ADMIT_SCRIPT = r"""
local existing = redis.call("EXISTS", KEYS[1])
if existing == 1 then
    local fp = redis.call("HGET", KEYS[1], "fingerprint")
    local status = redis.call("HGET", KEYS[1], "status")
    local route = redis.call("HGET", KEYS[1], "route")
    if fp == ARGV[1] then
        return {0, "replay", status, route, fp}
    end
    return {0, "conflict", status, route, fp}
end
redis.call(
    "HSET", KEYS[1],
    "fingerprint", ARGV[1],
    "status", ARGV[2],
    "route", ARGV[3],
    "payload", ARGV[4],
    "task_id", ARGV[5],
    "generation", ARGV[6],
    "admitted_at", ARGV[7],
    "repo", ARGV[8],
    "issue_number", ARGV[9]
)
redis.call("PERSIST", KEYS[1])
if KEYS[2] ~= false and KEYS[2] ~= nil then
    redis.call("HSET", KEYS[2], "task_id", ARGV[5], "admission_key", ARGV[10])
    redis.call("PERSIST", KEYS[2])
end
return {1, "admitted", ARGV[2], ARGV[3], ARGV[1]}
"""

_ADMIT_JOB_SCRIPT = r"""
local existing = redis.call("EXISTS", KEYS[1])
if existing == 1 then
    local existing_task = redis.call("HGET", KEYS[1], "task_id")
    local existing_gen = redis.call("HGET", KEYS[1], "generation")
    if existing_task ~= ARGV[3] or existing_gen ~= ARGV[4] then
        return {-1, "mismatch"}
    end
    local state = redis.call("HGET", KEYS[1], "state")
    if state == "pending" then
        redis.call("ZADD", KEYS[2], ARGV[2], ARGV[1])
    end
    redis.call("SET", KEYS[3], ARGV[1])
    redis.call("SADD", KEYS[4], ARGV[1])
    redis.call("PERSIST", KEYS[1])
    redis.call("PERSIST", KEYS[3])
    return {0, "existing", state}
end
for i = 5, #ARGV, 2 do
    redis.call("HSET", KEYS[1], ARGV[i], ARGV[i + 1])
end
redis.call("ZADD", KEYS[2], ARGV[2], ARGV[1])
redis.call("SET", KEYS[3], ARGV[1])
redis.call("SADD", KEYS[4], ARGV[1])
redis.call("PERSIST", KEYS[1])
redis.call("PERSIST", KEYS[3])
return {1, "created", "pending"}
"""

_CAS_JOB_SCRIPT = r"""
if redis.call("EXISTS", KEYS[1]) == 0 then
    return -2
end
local state = redis.call("HGET", KEYS[1], "state")
local phase = redis.call("HGET", KEYS[1], "saga_phase")
if state ~= ARGV[1] then
    return 0
end
if phase ~= ARGV[3] then
    return 0
end
if state == "verified" and ARGV[2] ~= "verified" then
    return -1
end
redis.call("HSET", KEYS[1], "state", ARGV[2], "saga_phase", ARGV[4])
for i = 5, #ARGV, 2 do
    redis.call("HSET", KEYS[1], ARGV[i], ARGV[i + 1])
end
redis.call("PERSIST", KEYS[1])
return 1
"""

_BARRIER_REMOVAL_SCRIPT = r"""
if redis.call("EXISTS", KEYS[1]) == 0 then
    return -2
end
local state = redis.call("HGET", KEYS[1], "state")
local phase = redis.call("HGET", KEYS[1], "saga_phase")
if state ~= ARGV[1] then
    return 0
end
if phase ~= ARGV[3] then
    return 0
end
if state == "verified" and ARGV[2] ~= "verified" then
    return -1
end
redis.call("HSET", KEYS[1], "state", ARGV[2], "saga_phase", ARGV[4])
for i = 7, #ARGV, 2 do
    redis.call("HSET", KEYS[1], ARGV[i], ARGV[i + 1])
end
redis.call("PERSIST", KEYS[1])
redis.call("DEL", KEYS[2])
redis.call("SREM", KEYS[3], ARGV[5])
redis.call("ZREM", KEYS[4], ARGV[5])
redis.call("ZADD", KEYS[5], ARGV[6], ARGV[5])
return 1
"""


def now_seconds() -> float:
    """Wall-clock seconds; tests patch this to advance time."""
    return time.time()


@dataclass(frozen=True)
class IssueResultAdmission:
    task_id: str
    repo: str
    issue_number: int
    generation: int
    status: str
    route: str
    fingerprint: str
    payload: str
    admitted_at: str


@dataclass(frozen=True)
class VerificationJob:
    repo: str
    issue_number: int
    generation: int
    task_id: str
    state: VerificationState
    saga_phase: SagaPhase
    expected_head_owner: str
    expected_branch: str
    pre_schema: bool
    created_at: float
    due_at: float
    grace_deadline: float
    attempt_count: int
    claimed_head_oid: str
    claimed_branch: str
    selected_pr_number: str = ""
    reason: str = ""
    error_kind: str = ""
    echo_mismatch: bool = False
    ambiguous: bool = False
    event_flags: str = ""


@dataclass(frozen=True)
class AdmissionDecision:
    kind: AdmissionKind
    status: str
    route: str
    fingerprint: str
    existing_fingerprint: str = ""
    generation: int = 0
    pre_schema: bool = False
    expected_head_owner: str = ""
    expected_branch: str = ""


def job_member(repo: str, issue_number: int, generation: int) -> str:
    return f"{repo}|{issue_number}|{generation}"


def parse_job_member(member: str) -> tuple[str, int, int] | None:
    parts = member.split("|")
    if len(parts) < 3:
        return None
    generation_s, issue_s = parts[-1], parts[-2]
    repo = "|".join(parts[:-2])
    try:
        return repo, int(issue_s), int(generation_s)
    except (TypeError, ValueError):
        return None


def result_fingerprint(result: TaskResult) -> str:
    payload = {
        "branch": result.branch or "",
        "needs_human": bool(result.needs_human),
        "resource_id": result.resource_id,
        "resource_type": result.resource_type,
        "snapshot_head_sha": result.snapshot_head_sha,
        "status": result.status.value,
        "summary": (result.summary or "")[:500],
    }
    return generic_domain_digest("orcest.issue-result.fingerprint", payload)


def _bounded_payload(result: TaskResult) -> str:
    payload = {
        "branch": (result.branch or "")[:120],
        "needs_human": bool(result.needs_human),
        "snapshot_head_sha": result.snapshot_head_sha,
        "status": result.status.value,
        "summary": (result.summary or "")[:_PAYLOAD_SUMMARY_LIMIT],
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


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


def _as_float(value: str | None, default: float = 0.0) -> float:
    if not value:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def has_issue_dispatch_barrier(redis: RedisClient, repo: str, issue_number: int) -> bool:
    """Return True when a verification job still blocks issue dispatch.

    Nonterminal jobs (PENDING, UNVERIFIABLE) and unfinished terminal sagas keep
    the barrier. UNVERIFIABLE never clears it.
    """
    if redis.exists(make_issue_dispatch_barrier_key(repo, issue_number)):
        return True
    generation = get_issue_generation(redis, repo, issue_number)
    if generation < 1:
        return False
    job = get_verification_job(redis, repo, issue_number, generation)
    if job is None:
        return False
    if job.state is VerificationState.UNVERIFIABLE:
        return True
    return job.saga_phase is not SagaPhase.BARRIER_REMOVED


def has_delivery_retry_cooldown(
    redis: RedisClient, repo: str, issue_number: int, now: NowFn = now_seconds
) -> bool:
    if redis.exists(make_issue_delivery_cooldown_key(repo, issue_number)):
        return True
    generation = get_issue_generation(redis, repo, issue_number)
    if generation < 1:
        return False
    data = redis.hgetall(make_issue_retry_record_key(repo, issue_number, generation))
    cooldown_until = _as_float(data.get("cooldown_until"))
    return cooldown_until > now()


def get_admission(redis: RedisClient, task_id: str) -> IssueResultAdmission | None:
    data = redis.hgetall(make_issue_admission_key(task_id))
    if not data or not data.get("fingerprint"):
        return None
    try:
        issue_number = int(data.get("issue_number", "0") or "0")
        generation = int(data.get("generation", "0") or "0")
    except (TypeError, ValueError):
        return None
    return IssueResultAdmission(
        task_id=data.get("task_id", task_id),
        repo=data.get("repo", ""),
        issue_number=issue_number,
        generation=generation,
        status=data.get("status", ""),
        route=data.get("route", ""),
        fingerprint=data.get("fingerprint", ""),
        payload=data.get("payload", ""),
        admitted_at=data.get("admitted_at", ""),
    )


def get_verification_job(
    redis: RedisClient, repo: str, issue_number: int, generation: int
) -> VerificationJob | None:
    data = redis.hgetall(make_issue_verification_job_key(repo, issue_number, generation))
    if not data:
        return None
    try:
        state = VerificationState(data.get("state", ""))
        phase = SagaPhase(data.get("saga_phase", ""))
        gen = int(data.get("generation", generation))
        number = int(data.get("issue_number", issue_number))
        attempt_count = int(data.get("attempt_count", "0") or "0")
    except (TypeError, ValueError):
        return None
    task_id = data.get("task_id", "")
    if not task_id:
        return None
    flags = ",".join(
        sorted(field[6:] for field in data if field.startswith("event:") and data[field] == "1")
    )
    return VerificationJob(
        repo=data.get("repo", repo),
        issue_number=number,
        generation=gen,
        task_id=task_id,
        state=state,
        saga_phase=phase,
        expected_head_owner=data.get("expected_head_owner", ""),
        expected_branch=data.get("expected_branch", ""),
        pre_schema=data.get("pre_schema", "0") == "1",
        created_at=_as_float(data.get("created_at")),
        due_at=_as_float(data.get("due_at")),
        grace_deadline=_as_float(data.get("grace_deadline")),
        attempt_count=attempt_count,
        claimed_head_oid=data.get("claimed_head_oid", ""),
        claimed_branch=data.get("claimed_branch", ""),
        selected_pr_number=data.get("selected_pr_number", ""),
        reason=data.get("reason", ""),
        error_kind=data.get("error_kind", ""),
        echo_mismatch=data.get("echo_mismatch", "0") == "1",
        ambiguous=data.get("ambiguous", "0") == "1",
        event_flags=flags,
    )


def publication_for_task(
    redis: RedisClient, repo: str, issue_number: int, task_id: str
) -> tuple[int, Any]:
    """Return (generation, publication record or None) for *task_id*."""
    current = get_issue_generation(redis, repo, issue_number)
    if current < 1:
        return 0, None
    start = current
    stop = max(1, current - _GENERATION_WALK_CAP + 1)
    for gen in range(start, stop - 1, -1):
        record = get_issue_publication(redis, repo, issue_number, gen)
        if record is not None and record.task_id == task_id:
            return gen, record
    return 0, None


def admit_issue_result(
    redis: RedisClient,
    repo: str,
    result: TaskResult,
    *,
    now: NowFn = now_seconds,
    logger_: logging.Logger | None = None,
) -> AdmissionDecision:
    """First-payload CAS admit of an issue result. No status-specific side effects."""
    log = logger_ or logger
    fingerprint = result_fingerprint(result)
    generation, publication = publication_for_task(redis, repo, result.resource_id, result.task_id)
    pre_schema = publication is None
    expected_head_owner = "" if publication is None else publication.expected_head_owner
    expected_branch = "" if publication is None else publication.expected_branch
    route = ROUTE_COMPLETED_VERIFY if result.status is ResultStatus.COMPLETED else ROUTE_NONSUCCESS
    keys = [redis._prefixed(make_issue_admission_key(result.task_id))]
    num_keys = 1
    generation_ref = ""
    if generation > 0:
        keys.append(
            redis._prefixed(make_issue_result_ref_key(repo, result.resource_id, generation))
        )
        num_keys = 2
        generation_ref = make_issue_admission_key(result.task_id)
    response = redis.client.eval(
        _ADMIT_SCRIPT,
        num_keys,
        *keys,
        fingerprint,
        result.status.value,
        route,
        _bounded_payload(result),
        result.task_id,
        str(generation),
        f"{now():.3f}",
        repo,
        str(result.resource_id),
        generation_ref,
    )
    parsed = _as_list(response)
    kind = AdmissionKind(_as_str(parsed[1])) if len(parsed) > 1 else AdmissionKind.ADMITTED
    status = _as_str(parsed[2]) if len(parsed) > 2 else result.status.value
    recorded_route = _as_str(parsed[3]) if len(parsed) > 3 else route
    recorded_fp = _as_str(parsed[4]) if len(parsed) > 4 else fingerprint
    decision = AdmissionDecision(
        kind=kind,
        status=status,
        route=recorded_route,
        fingerprint=fingerprint if kind is not AdmissionKind.CONFLICT else recorded_fp,
        existing_fingerprint=recorded_fp,
        generation=generation,
        pre_schema=pre_schema,
        expected_head_owner=expected_head_owner,
        expected_branch=expected_branch,
    )
    if kind is AdmissionKind.ADMITTED:
        _emit_event(
            redis,
            "net.orcest.issue.result.admitted",
            result.task_id,
            repo,
            result.resource_id,
            {
                "route": recorded_route,
                "status": status,
                "generation": generation,
                "pre_schema": pre_schema,
            },
            event_key=f"admit:{fingerprint}",
        )
        _incr_metric(redis, "admitted")
        log.info(
            "Admitted issue result for #%d task %s status=%s route=%s",
            result.resource_id,
            result.task_id,
            status,
            recorded_route,
        )
    return decision


def admit_completed_verification_job(
    redis: RedisClient,
    repo: str,
    result: TaskResult,
    decision: AdmissionDecision,
    config: IssueDeliveryVerifierConfig,
    *,
    now: NowFn = now_seconds,
    logger_: logging.Logger | None = None,
) -> bool:
    """Atomically create or validate the verification job and due-index entry."""
    log = logger_ or logger
    generation = decision.generation
    if generation < 1:
        generation = 0
    created = now()
    due_at = created
    grace_deadline = created + max(0, config.grace_seconds)
    member = job_member(repo, result.resource_id, generation)
    job_key = make_issue_verification_job_key(repo, result.resource_id, generation)
    fields = {
        "repo": repo,
        "issue_number": str(result.resource_id),
        "generation": str(generation),
        "task_id": result.task_id,
        "state": VerificationState.PENDING.value,
        "saga_phase": SagaPhase.ADMITTED.value,
        "expected_head_owner": decision.expected_head_owner,
        "expected_branch": decision.expected_branch,
        "pre_schema": "1" if decision.pre_schema else "0",
        "created_at": f"{created:.3f}",
        "due_at": f"{due_at:.3f}",
        "grace_deadline": f"{grace_deadline:.3f}",
        "attempt_count": "0",
        "claimed_head_oid": result.snapshot_head_sha,
        "claimed_branch": result.branch or "",
        "admission_fingerprint": decision.fingerprint,
    }
    flattened: list[str] = []
    for key, value in fields.items():
        flattened.extend((key, value))
    response = redis.client.eval(
        _ADMIT_JOB_SCRIPT,
        4,
        redis._prefixed(job_key),
        redis._prefixed(DUE_INDEX_KEY),
        redis._prefixed(make_issue_dispatch_barrier_key(repo, result.resource_id)),
        redis._prefixed(ACTIVE_JOBS_KEY),
        member,
        str(due_at),
        result.task_id,
        str(generation),
        *flattened,
    )
    parsed = _as_list(response)
    status = _as_int(parsed[0]) if parsed else 0
    if status < 0:
        logger.warning(
            "Verification job for %s#%d generation %d belongs to a different task; "
            "leaving the existing job unchanged",
            repo,
            result.resource_id,
            generation,
        )
        return False
    created_job = status == 1
    if created_job:
        _flag_event(redis, job_key, "admitted")
        _emit_event(
            redis,
            "net.orcest.issue.delivery.phase",
            result.task_id,
            repo,
            result.resource_id,
            {
                "phase": SagaPhase.ADMITTED.value,
                "state": VerificationState.PENDING.value,
                "generation": generation,
            },
            event_key=f"job-admitted:{generation}",
        )
        _incr_metric(redis, "pending")
        log.info(
            "Admitted verification job for issue #%d generation %d task %s",
            result.resource_id,
            generation,
            result.task_id,
        )
    return created_job


def apply_admission_conflict(
    redis: RedisClient,
    repo: str,
    result: TaskResult,
    decision: AdmissionDecision,
    *,
    now: NowFn = now_seconds,
    logger_: logging.Logger | None = None,
) -> None:
    """Freeze a nonterminal job or quarantine a post-terminal conflicting payload."""
    log = logger_ or logger
    generation = decision.generation
    job = (
        get_verification_job(redis, repo, result.resource_id, generation)
        if generation >= 0
        else None
    )
    if job is not None and job.state in NONTERMINAL_VERIFICATION_STATES:
        if job.state is VerificationState.PENDING:
            _transition_job(
                redis,
                job,
                VerificationState.PENDING,
                VerificationState.UNVERIFIABLE,
                job.saga_phase,
                job.saga_phase,
                extra={
                    "reason": "conflicting_result_payload",
                    "error_kind": DeliveryErrorKind.SCHEMA.value,
                },
            )
            redis.zrem(DUE_INDEX_KEY, job_member(repo, job.issue_number, job.generation))
            redis.sadd(UNVERIFIABLE_INDEX_KEY, job_member(repo, job.issue_number, job.generation))
            _incr_metric(redis, "unverifiable")
            _alert_unverifiable(redis, job, "conflicting_result_payload")
        log.warning(
            "Conflicting issue result for task %s froze verification as UNVERIFIABLE",
            result.task_id,
        )
        return
    _quarantine_conflict(redis, result, decision, now=now)
    log.warning(
        "Quarantined conflicting issue result for task %s without changing terminal state",
        result.task_id,
    )


def _quarantine_conflict(
    redis: RedisClient,
    result: TaskResult,
    decision: AdmissionDecision,
    *,
    now: NowFn,
) -> None:
    record = json.dumps(
        {
            "task_id": result.task_id,
            "status": result.status.value,
            "fingerprint": result_fingerprint(result),
            "existing_fingerprint": decision.existing_fingerprint,
            "existing_status": decision.status,
            "existing_route": decision.route,
            "generation": decision.generation,
            "at": f"{now():.3f}",
        },
        separators=(",", ":"),
        sort_keys=True,
    )
    redis.lpush(QUARANTINE_KEY, record)
    redis.ltrim(QUARANTINE_KEY, 0, _QUARANTINE_MAXLEN - 1)
    _incr_metric(redis, "quarantined")
    _emit_event(
        redis,
        "net.orcest.issue.delivery.alert",
        result.task_id,
        result.repo or "",
        result.resource_id,
        {"alert": "quarantined_conflict", "generation": decision.generation},
        event_key=f"quarantine:{result_fingerprint(result)}",
    )


def reconcile_verification_due_index(redis: RedisClient, now: NowFn = now_seconds) -> int:
    """Ensure PENDING jobs have due-index membership; drop stale terminal members."""
    repaired = 0
    members = redis.smembers(ACTIVE_JOBS_KEY)
    current = now()
    for member in members:
        parsed = parse_job_member(member)
        if parsed is None:
            redis.srem(ACTIVE_JOBS_KEY, member)
            redis.zrem(DUE_INDEX_KEY, member)
            continue
        repo, issue_number, generation = parsed
        job = get_verification_job(redis, repo, issue_number, generation)
        if job is None:
            redis.srem(ACTIVE_JOBS_KEY, member)
            redis.zrem(DUE_INDEX_KEY, member)
            redis.srem(UNVERIFIABLE_INDEX_KEY, member)
            continue
        if job.state is VerificationState.PENDING:
            score = redis.zscore(DUE_INDEX_KEY, member)
            if score is None:
                redis.zadd(DUE_INDEX_KEY, {member: job.due_at or current})
                repaired += 1
            redis.persist(make_issue_verification_job_key(repo, issue_number, generation))
            redis.persist(make_issue_dispatch_barrier_key(repo, issue_number))
            redis.persist(make_issue_admission_key(job.task_id))
        elif job.state is VerificationState.UNVERIFIABLE:
            redis.zrem(DUE_INDEX_KEY, member)
            redis.sadd(UNVERIFIABLE_INDEX_KEY, member)
            redis.persist(make_issue_verification_job_key(repo, issue_number, generation))
            redis.persist(make_issue_dispatch_barrier_key(repo, issue_number))
            redis.persist(make_issue_admission_key(job.task_id))
        else:
            redis.zrem(DUE_INDEX_KEY, member)
            if job.saga_phase is SagaPhase.BARRIER_REMOVED:
                redis.srem(ACTIVE_JOBS_KEY, member)
    stale_due = redis.zrangebyscore(DUE_INDEX_KEY, "-inf", "+inf")
    for member in stale_due:
        parsed = parse_job_member(member)
        if parsed is None:
            redis.zrem(DUE_INDEX_KEY, member)
            continue
        repo, issue_number, generation = parsed
        job = get_verification_job(redis, repo, issue_number, generation)
        if job is None or job.state is not VerificationState.PENDING:
            redis.zrem(DUE_INDEX_KEY, member)
            repaired += 1
    _set_metric(redis, "due", redis.zcard(DUE_INDEX_KEY))
    _set_metric(redis, "pending_gauge", _count_state(redis, VerificationState.PENDING))
    _set_metric(redis, "unverifiable_gauge", redis.scard(UNVERIFIABLE_INDEX_KEY))
    return repaired


def process_due_verification_jobs(
    redis: RedisClient,
    repo: str,
    token: str,
    labels: LabelConfig,
    config: IssueDeliveryVerifierConfig,
    *,
    stream_redis: RedisClient | None = None,
    now: NowFn = now_seconds,
    logger_: logging.Logger | None = None,
    observe: Callable[..., HandoffObservation] | None = None,
) -> int:
    """Run due PENDING jobs for *repo* and resume incomplete sagas."""
    log = logger_ or logger
    observe_fn = observe or observe_issue_handoff
    processed = 0
    current = now()
    due_members = redis.zrangebyscore(
        DUE_INDEX_KEY, "-inf", current, start=0, num=config.scheduler_batch_size
    )
    for member in due_members:
        parsed = parse_job_member(member)
        if parsed is None:
            redis.zrem(DUE_INDEX_KEY, member)
            continue
        job_repo, issue_number, generation = parsed
        if job_repo != repo:
            continue
        job = get_verification_job(redis, job_repo, issue_number, generation)
        if job is None:
            redis.zrem(DUE_INDEX_KEY, member)
            continue
        if job.state is VerificationState.PENDING:
            _observe_and_transition(
                redis,
                job,
                token,
                labels,
                config,
                stream_redis=stream_redis,
                now=now,
                logger_=log,
                observe=observe_fn,
            )
            processed += 1
        elif job.state in TERMINAL_VERIFICATION_STATES:
            _resume_saga(redis, job, token, labels, config, stream_redis=stream_redis, logger_=log)
            processed += 1
    active = redis.smembers(ACTIVE_JOBS_KEY)
    for member in active:
        parsed = parse_job_member(member)
        if parsed is None or parsed[0] != repo:
            continue
        job = get_verification_job(redis, parsed[0], parsed[1], parsed[2])
        if job is None:
            continue
        if (
            job.state in TERMINAL_VERIFICATION_STATES
            and job.saga_phase is not SagaPhase.BARRIER_REMOVED
        ):
            _resume_saga(redis, job, token, labels, config, stream_redis=stream_redis, logger_=log)
            processed += 1
    emit_delivery_alerts(redis, config, now=now, logger_=log)
    return processed


def _observe_and_transition(
    redis: RedisClient,
    job: VerificationJob,
    token: str,
    labels: LabelConfig,
    config: IssueDeliveryVerifierConfig,
    *,
    stream_redis: RedisClient | None,
    now: NowFn,
    logger_: logging.Logger,
    observe: Callable[..., HandoffObservation],
) -> None:
    current = now()
    if not job.pre_schema and not job.expected_branch:
        _mark_unverifiable(
            redis,
            job,
            {
                "reason": "missing_expected_outcome",
                "error_kind": DeliveryErrorKind.SCHEMA.value,
            },
            "copied expected outcome was missing or corrupt",
        )
        return
    try:
        observation = observe(
            job.repo,
            job.issue_number,
            job.expected_branch,
            token,
            expected_head_owner=job.expected_head_owner,
            claimed_head_oid=job.claimed_head_oid,
            claimed_branch=job.claimed_branch,
            page_cap=config.page_cap,
            pre_schema=job.pre_schema,
        )
    except Exception as exc:
        observation = HandoffObservation(
            verified=False,
            error_kind=DeliveryErrorKind.TRANSPORT,
            reason=DeliveryFailureReason.GH_TRANSPORT_ERROR,
            repo=job.repo,
            issue_number=job.issue_number,
            default_branch="",
            default_branch_oid="",
            expected_head_ref=job.expected_branch,
            claimed_head_oid=job.claimed_head_oid,
            live_head_oid="",
            complete=False,
            message=f"unexpected observer failure: {exc}",
        )
    extra = {
        "reason": observation.reason.value,
        "error_kind": observation.error_kind.value,
        "echo_mismatch": "1" if observation.echo_mismatch else "0",
        "ambiguous": "1" if observation.ambiguous else "0",
        "selected_pr_number": (
            "" if observation.selected_pr is None else str(observation.selected_pr.number)
        ),
        "live_head_oid": observation.live_head_oid,
        "message": observation.message[:500],
    }
    if observation.echo_mismatch:
        _incr_metric_once(redis, job, "echo_mismatch_metric", "echo_mismatch")
        _emit_job_event(
            redis,
            job,
            "echo_mismatch",
            {"alert": "echo_mismatch", "diagnostic": True},
        )
    if observation.ambiguous:
        _incr_metric_once(redis, job, "ambiguity", "ambiguity")
    if observation.verified:
        if _transition_job(
            redis,
            job,
            VerificationState.PENDING,
            VerificationState.VERIFIED,
            SagaPhase.ADMITTED,
            SagaPhase.TERMINAL_PERSISTED,
            extra=extra,
        ):
            _incr_metric(redis, "verified")
            latency_ms = int(max(0.0, current - job.created_at) * 1000)
            _set_metric(redis, "last_latency_ms", latency_ms)
            _incr_metric(redis, "latency_samples")
            _emit_job_event(
                redis,
                job,
                "verified",
                {
                    "phase": SagaPhase.TERMINAL_PERSISTED.value,
                    "state": VerificationState.VERIFIED.value,
                    "selected_pr_number": extra["selected_pr_number"],
                    "latency_ms": latency_ms,
                },
            )
            redis.zrem(DUE_INDEX_KEY, job_member(job.repo, job.issue_number, job.generation))
        refreshed = get_verification_job(redis, job.repo, job.issue_number, job.generation)
        if refreshed is not None:
            _resume_saga(
                redis, refreshed, token, labels, config, stream_redis=stream_redis, logger_=logger_
            )
        return

    if observation.error_kind in _RETRY_KINDS or not observation.complete:
        if observation.error_kind in _UNVERIFIABLE_KINDS:
            _mark_unverifiable(redis, job, extra, observation.message)
            return
        attempt = job.attempt_count + 1
        delay = min(
            config.backoff_max_seconds,
            max(1, config.backoff_initial_seconds) * (2 ** max(0, attempt - 1)),
        )
        due_at = current + delay
        _transition_job(
            redis,
            job,
            VerificationState.PENDING,
            VerificationState.PENDING,
            job.saga_phase,
            job.saga_phase,
            extra={**extra, "attempt_count": str(attempt), "due_at": f"{due_at:.3f}"},
        )
        redis.zadd(DUE_INDEX_KEY, {job_member(job.repo, job.issue_number, job.generation): due_at})
        logger_.info(
            "Issue #%d verification backoff %ss (%s/%s)",
            job.issue_number,
            delay,
            observation.error_kind.value,
            observation.reason.value,
        )
        return

    if observation.error_kind in _UNVERIFIABLE_KINDS:
        _mark_unverifiable(redis, job, extra, observation.message)
        return

    if job.pre_schema:
        _mark_unverifiable(redis, job, extra, "pre-schema handoff could not be proven")
        return

    if current < job.grace_deadline:
        due_at = min(job.grace_deadline, current + max(1, config.backoff_initial_seconds))
        _transition_job(
            redis,
            job,
            VerificationState.PENDING,
            VerificationState.PENDING,
            job.saga_phase,
            job.saga_phase,
            extra={**extra, "due_at": f"{due_at:.3f}"},
        )
        redis.zadd(DUE_INDEX_KEY, {job_member(job.repo, job.issue_number, job.generation): due_at})
        return

    if _transition_job(
        redis,
        job,
        VerificationState.PENDING,
        VerificationState.INEFFECTIVE,
        SagaPhase.ADMITTED,
        SagaPhase.TERMINAL_PERSISTED,
        extra=extra,
    ):
        _incr_metric(redis, "ineffective")
        _emit_job_event(
            redis,
            job,
            "ineffective",
            {
                "phase": SagaPhase.TERMINAL_PERSISTED.value,
                "state": VerificationState.INEFFECTIVE.value,
                "reason": observation.reason.value,
            },
        )
        redis.zrem(DUE_INDEX_KEY, job_member(job.repo, job.issue_number, job.generation))
    refreshed = get_verification_job(redis, job.repo, job.issue_number, job.generation)
    if refreshed is not None:
        _resume_saga(
            redis, refreshed, token, labels, config, stream_redis=stream_redis, logger_=logger_
        )


def _mark_unverifiable(
    redis: RedisClient, job: VerificationJob, extra: dict[str, str], message: str
) -> None:
    if job.state is VerificationState.UNVERIFIABLE:
        return
    if _transition_job(
        redis,
        job,
        job.state,
        VerificationState.UNVERIFIABLE,
        job.saga_phase,
        job.saga_phase,
        extra=extra,
    ):
        _incr_metric(redis, "unverifiable")
        _alert_unverifiable(redis, job, message)
    redis.zrem(DUE_INDEX_KEY, job_member(job.repo, job.issue_number, job.generation))
    redis.sadd(UNVERIFIABLE_INDEX_KEY, job_member(job.repo, job.issue_number, job.generation))
    redis.persist(make_issue_verification_job_key(job.repo, job.issue_number, job.generation))
    redis.persist(make_issue_dispatch_barrier_key(job.repo, job.issue_number))
    redis.persist(make_issue_admission_key(job.task_id))


def _resume_saga(
    redis: RedisClient,
    job: VerificationJob,
    token: str,
    labels: LabelConfig,
    config: IssueDeliveryVerifierConfig,
    *,
    stream_redis: RedisClient | None,
    logger_: logging.Logger,
) -> None:
    if job.state is VerificationState.VERIFIED:
        _advance_verified_saga(redis, job, token, labels, stream_redis, logger_)
    elif job.state is VerificationState.INEFFECTIVE:
        _advance_ineffective_saga(redis, job, config, stream_redis, logger_)


def _advance_verified_saga(
    redis: RedisClient,
    job: VerificationJob,
    token: str,
    labels: LabelConfig,
    stream_redis: RedisClient | None,
    logger_: logging.Logger,
) -> None:
    current = get_verification_job(redis, job.repo, job.issue_number, job.generation)
    if current is None or current.state is not VerificationState.VERIFIED:
        return
    job = current
    if job.saga_phase is SagaPhase.ADMITTED:
        _transition_job(
            redis,
            job,
            VerificationState.VERIFIED,
            VerificationState.VERIFIED,
            SagaPhase.ADMITTED,
            SagaPhase.TERMINAL_PERSISTED,
        )
        job = get_verification_job(redis, job.repo, job.issue_number, job.generation) or job
    if job.saga_phase is SagaPhase.TERMINAL_PERSISTED:
        try:
            gh.remove_issue_label(job.repo, job.issue_number, labels.ready, token)
        except Exception:
            logger_.warning(
                "Failed to remove %s from issue #%d after VERIFIED; will retry",
                labels.ready,
                job.issue_number,
                exc_info=True,
            )
            return
        if _transition_job(
            redis,
            job,
            VerificationState.VERIFIED,
            VerificationState.VERIFIED,
            SagaPhase.TERMINAL_PERSISTED,
            SagaPhase.LABEL_MUTATED,
        ):
            _emit_job_event(
                redis,
                job,
                "label_mutated",
                {"phase": SagaPhase.LABEL_MUTATED.value, "state": "verified"},
            )
        job = get_verification_job(redis, job.repo, job.issue_number, job.generation) or job
    if job.saga_phase is SagaPhase.LABEL_MUTATED:
        try:
            clear_issue_attempts(redis, job.repo, job.issue_number)
        except Exception:
            logger_.warning(
                "Failed to clear attempts after VERIFIED for issue #%d",
                job.issue_number,
                exc_info=True,
            )
            return
        if _transition_job(
            redis,
            job,
            VerificationState.VERIFIED,
            VerificationState.VERIFIED,
            SagaPhase.LABEL_MUTATED,
            SagaPhase.CHECKPOINTED,
        ):
            _emit_job_event(
                redis,
                job,
                "checkpointed",
                {"phase": SagaPhase.CHECKPOINTED.value, "state": "verified"},
            )
        job = get_verification_job(redis, job.repo, job.issue_number, job.generation) or job
    if job.saga_phase is SagaPhase.CHECKPOINTED:
        _generation_cleanup(redis, job, stream_redis)
        if _transition_job(
            redis,
            job,
            VerificationState.VERIFIED,
            VerificationState.VERIFIED,
            SagaPhase.CHECKPOINTED,
            SagaPhase.GENERATION_CLEANED,
        ):
            _emit_job_event(
                redis,
                job,
                "generation_cleaned",
                {"phase": SagaPhase.GENERATION_CLEANED.value, "state": "verified"},
            )
        job = get_verification_job(redis, job.repo, job.issue_number, job.generation) or job
    if job.saga_phase is SagaPhase.GENERATION_CLEANED:
        if _remove_barrier(
            redis,
            job,
            VerificationState.VERIFIED,
            VerificationState.VERIFIED,
            SagaPhase.GENERATION_CLEANED,
            SagaPhase.BARRIER_REMOVED,
        ):
            _emit_job_event(
                redis,
                job,
                "barrier_removed",
                {"phase": SagaPhase.BARRIER_REMOVED.value, "state": "verified"},
            )


def _advance_ineffective_saga(
    redis: RedisClient,
    job: VerificationJob,
    config: IssueDeliveryVerifierConfig,
    stream_redis: RedisClient | None,
    logger_: logging.Logger,
) -> None:
    current = get_verification_job(redis, job.repo, job.issue_number, job.generation)
    if current is None or current.state is not VerificationState.INEFFECTIVE:
        return
    job = current
    if job.saga_phase is SagaPhase.ADMITTED:
        _transition_job(
            redis,
            job,
            VerificationState.INEFFECTIVE,
            VerificationState.INEFFECTIVE,
            SagaPhase.ADMITTED,
            SagaPhase.TERMINAL_PERSISTED,
        )
        job = get_verification_job(redis, job.repo, job.issue_number, job.generation) or job
    if job.saga_phase is SagaPhase.TERMINAL_PERSISTED:
        cooldown = max(0, config.ineffective_cooldown_seconds)
        cooldown_until = now_seconds() + cooldown
        retry_key = make_issue_retry_record_key(job.repo, job.issue_number, job.generation)
        redis.hset_mapping(
            retry_key,
            {
                "reason": job.reason or "ineffective_delivery",
                "generation": str(job.generation),
                "task_id": job.task_id,
                "cooldown_until": f"{cooldown_until:.3f}",
                "created_at": f"{now_seconds():.3f}",
            },
        )
        redis.persist(retry_key)
        if cooldown > 0:
            redis.set_ex(
                make_issue_delivery_cooldown_key(job.repo, job.issue_number),
                "1",
                cooldown,
            )
        if _transition_job(
            redis,
            job,
            VerificationState.INEFFECTIVE,
            VerificationState.INEFFECTIVE,
            SagaPhase.TERMINAL_PERSISTED,
            SagaPhase.RETRY_RECORDED,
        ):
            _emit_job_event(
                redis,
                job,
                "retry_recorded",
                {"phase": SagaPhase.RETRY_RECORDED.value, "state": "ineffective"},
            )
        job = get_verification_job(redis, job.repo, job.issue_number, job.generation) or job
    if job.saga_phase is SagaPhase.RETRY_RECORDED:
        if _transition_job(
            redis,
            job,
            VerificationState.INEFFECTIVE,
            VerificationState.INEFFECTIVE,
            SagaPhase.RETRY_RECORDED,
            SagaPhase.CHECKPOINTED,
        ):
            _emit_job_event(
                redis,
                job,
                "checkpointed",
                {"phase": SagaPhase.CHECKPOINTED.value, "state": "ineffective"},
            )
        job = get_verification_job(redis, job.repo, job.issue_number, job.generation) or job
    if job.saga_phase is SagaPhase.CHECKPOINTED:
        _generation_cleanup(redis, job, stream_redis)
        if _transition_job(
            redis,
            job,
            VerificationState.INEFFECTIVE,
            VerificationState.INEFFECTIVE,
            SagaPhase.CHECKPOINTED,
            SagaPhase.GENERATION_CLEANED,
        ):
            _emit_job_event(
                redis,
                job,
                "generation_cleaned",
                {"phase": SagaPhase.GENERATION_CLEANED.value, "state": "ineffective"},
            )
        job = get_verification_job(redis, job.repo, job.issue_number, job.generation) or job
    if job.saga_phase is SagaPhase.GENERATION_CLEANED:
        if _remove_barrier(
            redis,
            job,
            VerificationState.INEFFECTIVE,
            VerificationState.INEFFECTIVE,
            SagaPhase.GENERATION_CLEANED,
            SagaPhase.BARRIER_REMOVED,
        ):
            _emit_job_event(
                redis,
                job,
                "barrier_removed",
                {"phase": SagaPhase.BARRIER_REMOVED.value, "state": "ineffective"},
            )


def _generation_cleanup(
    redis: RedisClient, job: VerificationJob, stream_redis: RedisClient | None
) -> None:
    current = get_issue_generation(redis, job.repo, job.issue_number)
    if current != job.generation:
        return
    if stream_redis is None:
        return
    try:
        gc_issue_publication(redis, stream_redis, job.repo, job.issue_number, job.generation)
    except Exception:
        logger.debug(
            "Publication GC skipped for %s#%d gen %d",
            job.repo,
            job.issue_number,
            job.generation,
        )


def _remove_barrier(
    redis: RedisClient,
    job: VerificationJob,
    expected_state: VerificationState,
    new_state: VerificationState,
    expected_phase: SagaPhase,
    new_phase: SagaPhase,
    extra: dict[str, str] | None = None,
) -> bool:
    member = job_member(job.repo, job.issue_number, job.generation)
    flattened: list[str] = []
    for key, value in (extra or {}).items():
        flattened.extend((key, value))
    response = redis.client.eval(
        _BARRIER_REMOVAL_SCRIPT,
        5,
        redis._prefixed(
            make_issue_verification_job_key(job.repo, job.issue_number, job.generation)
        ),
        redis._prefixed(make_issue_dispatch_barrier_key(job.repo, job.issue_number)),
        redis._prefixed(ACTIVE_JOBS_KEY),
        redis._prefixed(DUE_INDEX_KEY),
        redis._prefixed(GC_DUE_INDEX_KEY),
        expected_state.value,
        new_state.value,
        expected_phase.value,
        new_phase.value,
        member,
        f"{now_seconds():.3f}",
        *flattened,
    )
    return _as_int(response) == 1


def _transition_job(
    redis: RedisClient,
    job: VerificationJob,
    expected_state: VerificationState,
    new_state: VerificationState,
    expected_phase: SagaPhase,
    new_phase: SagaPhase,
    extra: dict[str, str] | None = None,
) -> bool:
    flattened: list[str] = []
    for key, value in (extra or {}).items():
        flattened.extend((key, value))
    response = redis.client.eval(
        _CAS_JOB_SCRIPT,
        1,
        redis._prefixed(
            make_issue_verification_job_key(job.repo, job.issue_number, job.generation)
        ),
        expected_state.value,
        new_state.value,
        expected_phase.value,
        new_phase.value,
        *flattened,
    )
    return _as_int(response) == 1


def gc_issue_delivery_state(
    redis: RedisClient, repo: str, issue_number: int, generation: int
) -> bool:
    """Collect delivery state only when it is terminal and unreferenced.

    The dispatch barrier is atomically removed by ``_remove_barrier`` while
    transitioning into ``BARRIER_REMOVED``, so only the job hash, the admission
    ledger entry, and the result-ref key -- all ``PERSIST``ed, no TTL -- remain
    to collect here.
    """
    job = get_verification_job(redis, repo, issue_number, generation)
    job_key = make_issue_verification_job_key(repo, issue_number, generation)
    barrier_key = make_issue_dispatch_barrier_key(repo, issue_number)
    result_key = make_issue_result_ref_key(repo, issue_number, generation)
    if job is None:
        return False
    if (
        job.state in NONTERMINAL_VERIFICATION_STATES
        or job.saga_phase is not SagaPhase.BARRIER_REMOVED
    ):
        redis.persist(job_key)
        redis.persist(barrier_key)
        redis.persist(make_issue_admission_key(job.task_id))
        redis.persist(result_key)
        return False
    redis.delete(job_key, result_key, make_issue_admission_key(job.task_id))
    return True


def process_due_delivery_state_gc(
    redis: RedisClient,
    repo: str,
    *,
    now: NowFn = now_seconds,
    batch_size: int = 200,
) -> int:
    """Collect delivery state for generations queued by ``_remove_barrier``.

    ``_remove_barrier`` atomically queues a job's member into
    ``GC_DUE_INDEX_KEY`` as its saga reaches ``BARRIER_REMOVED``, independent of
    when this is called -- so state stays readable for as long as collection
    keeps failing, and callers can defer sweeping without racing saga
    completion.
    """
    current = now()
    due_members = redis.zrangebyscore(GC_DUE_INDEX_KEY, "-inf", current, start=0, num=batch_size)
    collected = 0
    for member in due_members:
        parsed = parse_job_member(member)
        if parsed is None:
            redis.zrem(GC_DUE_INDEX_KEY, member)
            continue
        job_repo, issue_number, generation = parsed
        if job_repo != repo:
            continue
        if gc_issue_delivery_state(redis, job_repo, issue_number, generation):
            collected += 1
            redis.zrem(GC_DUE_INDEX_KEY, member)
    return collected


def delivery_state_blocks_old_orchestrator_rollback(redis: RedisClient) -> list[str]:
    """Return human-readable reasons an old-orchestrator rollback is unsafe."""
    reasons: list[str] = []
    due = redis.zcard(DUE_INDEX_KEY)
    if due:
        reasons.append(f"{due} due verification job(s)")
    active = redis.scard(ACTIVE_JOBS_KEY)
    if active:
        reasons.append(f"{active} active verification job(s)")
    unverifiable = redis.scard(UNVERIFIABLE_INDEX_KEY)
    if unverifiable:
        reasons.append(f"{unverifiable} UNVERIFIABLE job(s)")
    quarantined = redis.llen(QUARANTINE_KEY)
    if quarantined:
        reasons.append(f"{quarantined} quarantined conflicting result(s)")
    gc_due = redis.zcard(GC_DUE_INDEX_KEY)
    if gc_due:
        reasons.append(f"{gc_due} delivery GC job(s)")
    return reasons


def emit_delivery_alerts(
    redis: RedisClient,
    config: IssueDeliveryVerifierConfig,
    *,
    now: NowFn = now_seconds,
    logger_: logging.Logger | None = None,
) -> None:
    log = logger_ or logger
    current = now()
    oldest = _oldest_pending_age(redis, current)
    _set_metric(redis, "oldest_pending_age", int(oldest))
    if oldest >= config.oldest_pending_alert_seconds:
        log.warning("Oldest PENDING issue verification job is %ss old", int(oldest))
        _incr_metric(redis, "oldest_pending_alerts")
        _emit_event(
            redis,
            "net.orcest.issue.delivery.alert",
            "issue-delivery",
            "",
            0,
            {"alert": "oldest_pending", "age_seconds": int(oldest)},
            event_key=(
                f"oldest-pending:{int(current // max(1, config.oldest_pending_alert_seconds))}"
            ),
        )
    unverifiable = redis.scard(UNVERIFIABLE_INDEX_KEY)
    if unverifiable:
        log.warning("%d UNVERIFIABLE issue verification job(s) need operator action", unverifiable)


def _oldest_pending_age(redis: RedisClient, current: float) -> float:
    oldest = 0.0
    for member in redis.smembers(ACTIVE_JOBS_KEY):
        parsed = parse_job_member(member)
        if parsed is None:
            continue
        job = get_verification_job(redis, parsed[0], parsed[1], parsed[2])
        if job is None or job.state is not VerificationState.PENDING:
            continue
        oldest = max(oldest, current - job.created_at)
    return oldest


def _count_state(redis: RedisClient, state: VerificationState) -> int:
    count = 0
    for member in redis.smembers(ACTIVE_JOBS_KEY):
        parsed = parse_job_member(member)
        if parsed is None:
            continue
        job = get_verification_job(redis, parsed[0], parsed[1], parsed[2])
        if job is not None and job.state is state:
            count += 1
    return count


def _incr_metric(redis: RedisClient, field: str) -> None:
    try:
        key = METRICS_KEY
        count = redis.client.hincrby(redis._prefixed(key), field, 1)
        if count == 1:
            redis.expire(key, _METRICS_TTL_SECONDS)
    except Exception:
        logger.debug("Failed to increment issue delivery metric %s", field, exc_info=True)


def _set_metric(redis: RedisClient, field: str, value: int) -> None:
    try:
        redis.hset(METRICS_KEY, field, str(value))
        redis.expire(METRICS_KEY, _METRICS_TTL_SECONDS)
    except Exception:
        logger.debug("Failed to set issue delivery metric %s", field, exc_info=True)


def _incr_metric_once(redis: RedisClient, job: VerificationJob, flag: str, field: str) -> None:
    job_key = make_issue_verification_job_key(job.repo, job.issue_number, job.generation)
    if _flag_event(redis, job_key, flag):
        _incr_metric(redis, field)


def _flag_event(redis: RedisClient, job_key: str, name: str) -> bool:
    field = f"event:{name}"
    try:
        return redis.client.hsetnx(redis._prefixed(job_key), field, "1") == 1
    except Exception:
        return False


def _emit_job_event(
    redis: RedisClient, job: VerificationJob, name: str, data: dict[str, Any]
) -> None:
    job_key = make_issue_verification_job_key(job.repo, job.issue_number, job.generation)
    if not _flag_event(redis, job_key, name):
        return
    payload = {"generation": job.generation, **data}
    _emit_event(
        redis,
        "net.orcest.issue.delivery.phase",
        job.task_id,
        job.repo,
        job.issue_number,
        payload,
        event_key=f"{name}:{job.generation}",
    )


def _alert_unverifiable(redis: RedisClient, job: VerificationJob, message: str) -> None:
    job_key = make_issue_verification_job_key(job.repo, job.issue_number, job.generation)
    if not _flag_event(redis, job_key, "alert_unverifiable"):
        return
    _emit_event(
        redis,
        "net.orcest.issue.delivery.alert",
        job.task_id,
        job.repo,
        job.issue_number,
        {
            "alert": "unverifiable",
            "generation": job.generation,
            "message": message[:300],
        },
        event_key=f"unverifiable:{job.generation}",
    )
    logger.warning(
        "Issue #%d generation %d is UNVERIFIABLE and operator-blocked: %s",
        job.issue_number,
        job.generation,
        message[:300],
    )


def _emit_event(
    redis: RedisClient,
    event_type: str,
    task_id: str,
    repo: str,
    resource_id: int,
    data: dict[str, Any],
    *,
    event_key: str,
) -> None:
    try:
        event_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"orcest:{event_type}:{task_id}:{event_key}"))
        envelope = make_event(
            event_type,
            source_project=redis.key_prefix or "orcest",
            task_id=task_id,
            repo=repo,
            resource_type="issue",
            resource_id=resource_id,
            attempt=int(data.get("generation", 0) or 0),
            data=data,
            event_id=event_id,
        )
        EventPublisher(redis).publish(envelope)
    except Exception:
        logger.debug("Failed to emit %s for task %s", event_type, task_id, exc_info=True)


def get_delivery_metrics(redis: RedisClient) -> dict[str, str]:
    return redis.hgetall(METRICS_KEY)


def list_quarantined_conflicts(redis: RedisClient, limit: int = 20) -> list[dict[str, Any]]:
    raw = redis.lrange(QUARANTINE_KEY, 0, max(0, limit - 1))
    items: list[dict[str, Any]] = []
    for entry in raw:
        try:
            parsed = json.loads(entry)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            items.append(parsed)
    return items
