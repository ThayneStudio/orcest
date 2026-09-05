"""Controller restart recovery orchestration (persistence-and-recovery.md
"Controller restart recovery" / "Redis reconstruction").

An ordinary controller-only restart does not drain workers and must not
infer success or failure from the controller's own absence. This module
composes the store's already-durable, individually crash-resumable
primitives -- Timer Fact / Attempt Terminal Fact deadline sweeps, Redis
epoch reconstruction, offer republication, and each subsystem's own
``PENDING``-operation resumption -- into the one ordered pass a freshly
started controller runs before it reopens ordinary dispatch. Nothing here
invents a synthetic Fact, Result, or Terminal outcome: every step either
replays an existing durable row through its normal reducer/store path or
reports a still-``PENDING``/outstanding identity for its owning subsystem
to resume.
"""

from __future__ import annotations

from dataclasses import dataclass

from orcest.shared.redis_client import RedisClient
from orcest.workflow_store.store import RunStore
from orcest.workflow_store.v1.offer_projection import (
    dispatch_pending_offers,
    reconstruct_open_offers,
)
from orcest.workflow_store.v1.secret_provision import reconcile_pending_secret_provision_operation
from orcest.workflow_store.v1.secrets import SecretStore

__all__ = [
    "DeadlineSweepResult",
    "StartupRecoveryReport",
    "run_startup_recovery",
    "sweep_due_deadlines",
]

_STARTUP_SOURCE_KIND = "STARTUP_RECONCILIATION"


@dataclass(frozen=True, slots=True)
class DeadlineSweepResult:
    woken_wait_condition_ids: tuple[str, ...]
    expired_claim_attempt_ids: tuple[str, ...]
    expired_execution_attempt_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class StartupRecoveryReport:
    redis_epoch: int
    deadlines: DeadlineSweepResult
    republished_offers: int
    dispatched_offers: int
    resumed_secret_provision_operation_ids: tuple[str, ...]
    pending_storage_restoration_operation_ids: tuple[str, ...]
    pending_outbox_ids_by_source_kind: dict[str, tuple[str, ...]]


def sweep_due_deadlines(run_store: RunStore, *, now_ms: int | None = None) -> DeadlineSweepResult:
    """One full pass over every currently due Wait timer and Attempt
    claim/execution deadline (persistence-and-recovery.md "Controller
    restart recovery" steps 7 and 10). Each due deadline is materialized
    through the exact same durable Timer Fact / Attempt Terminal Fact
    reducer path normal operation uses -- never a synthesized shortcut --
    so a caller MUST run this before consulting any rebuilt timer or health
    cache.
    """
    woken = run_store.wake_due_wait_timers(source_kind=_STARTUP_SOURCE_KIND, now_ms=now_ms)

    expired_claims: list[str] = []
    for attempt_id in run_store.list_due_attempt_claim_deadlines(now_ms=now_ms):
        run_store.expire_attempt_claim_deadline(
            attempt_id=attempt_id, source_kind=_STARTUP_SOURCE_KIND, now_ms=now_ms
        )
        expired_claims.append(attempt_id)

    expired_executions: list[str] = []
    for attempt_id in run_store.list_due_attempt_execution_deadlines(now_ms=now_ms):
        run_store.expire_attempt_execution_deadline(
            attempt_id=attempt_id, source_kind=_STARTUP_SOURCE_KIND, now_ms=now_ms
        )
        expired_executions.append(attempt_id)

    return DeadlineSweepResult(
        woken_wait_condition_ids=tuple(woken),
        expired_claim_attempt_ids=tuple(expired_claims),
        expired_execution_attempt_ids=tuple(expired_executions),
    )


def run_startup_recovery(
    run_store: RunStore,
    redis: RedisClient,
    secret_store: SecretStore,
    *,
    now_ms: int | None = None,
    outbox_report_limit: int = 1000,
) -> StartupRecoveryReport:
    """Run the full controller-restart recovery sequence and return a report.

    Order matches persistence-and-recovery.md "Controller restart recovery"
    and "Redis reconstruction": materialize due deadlines first (so
    reconstruction never republishes an Attempt that this same pass just
    expired), then advance the Redis epoch and rebuild every current,
    unexpired ``OFFERED`` Attempt's offer under that new epoch -- an
    unexpired ``CLAIMED`` Attempt is never republished as schedulable work
    -- then resume ordinary dispatch of anything still durably ``PENDING``.
    Each subsystem's own ``PENDING`` operation (Secret Provision, Storage
    Restoration, and the generic multi-kind Outbox) is resumed or reported
    without this module inventing subsystem-specific delivery logic that
    belongs to that subsystem's own leaf.
    """
    deadlines = sweep_due_deadlines(run_store, now_ms=now_ms)

    redis_epoch = run_store.advance_redis_epoch()
    republished = reconstruct_open_offers(run_store, redis, redis_epoch=redis_epoch)
    dispatched = dispatch_pending_offers(run_store, redis, redis_epoch=redis_epoch)

    resumed_secret_provisions: list[str] = []
    for op in run_store.list_pending_secret_provision_operations():
        reconcile_pending_secret_provision_operation(
            run_store, secret_store, op.secret_provision_operation_id
        )
        resumed_secret_provisions.append(op.secret_provision_operation_id)

    pending_storage_restoration = [
        op.operation_id for op in run_store.list_pending_storage_restoration_operations()
    ]

    pending_outbox_by_kind: dict[str, list[str]] = {}
    for row in run_store.list_pending_outbox_rows(limit=outbox_report_limit):
        pending_outbox_by_kind.setdefault(row.source_kind, []).append(row.outbox_id)

    return StartupRecoveryReport(
        redis_epoch=redis_epoch,
        deadlines=deadlines,
        republished_offers=republished,
        dispatched_offers=dispatched,
        resumed_secret_provision_operation_ids=tuple(resumed_secret_provisions),
        pending_storage_restoration_operation_ids=tuple(pending_storage_restoration),
        pending_outbox_ids_by_source_kind={
            kind: tuple(ids) for kind, ids in pending_outbox_by_kind.items()
        },
    )
