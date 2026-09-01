"""Capacity Report ledger + derived Health Observations (issue #680)."""

from __future__ import annotations

import time
import uuid
from pathlib import Path

import pytest

from orcest.workflow_contract.v1.digest import request_digest
from orcest.workflow_store.store import (
    CapacityReportEntryInput,
    CasMismatchError,
    IdempotencyConflictError,
    RunStore,
)

pytestmark = pytest.mark.unit

POOL_MANAGER_ID = "pool-manager-1"


def _uid() -> str:
    return str(uuid.uuid4())


def _now_ms() -> int:
    return int(time.time() * 1000)


@pytest.fixture
def store(tmp_path: Path) -> RunStore:
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        yield store


def _session_entry(
    *,
    worker_session_id: str,
    available_slots: int = 1,
    capacity_pool_id: str = "default",
    worker_profile: str = "codex",
) -> CapacityReportEntryInput:
    state = "SESSION_READY" if available_slots > 0 else "SESSION_STOPPED"
    return CapacityReportEntryInput(
        scope_kind="WORKER_SESSION",
        scope_id=worker_session_id,
        capacity_pool_id=capacity_pool_id,
        worker_profile=worker_profile,
        available_slots=available_slots,
        session_evidence={
            "worker_id": "orcest-worker-1",
            "worker_session_id": worker_session_id,
            "state": state,
        },
    )


def _submit(
    store: RunStore,
    *,
    capacity_report_id: str | None = None,
    report_id: str | None = None,
    idempotency_key: str | None = None,
    report_sequence: int = 1,
    entries=None,
    expires_at_ms: int | None = None,
    configured_max_ttl_ms: int = 300_000,
    observed_at_ms: int | None = None,
):
    now = _now_ms()
    return store.submit_capacity_report(
        capacity_report_id=capacity_report_id or _uid(),
        pool_manager_id=POOL_MANAGER_ID,
        report_id=report_id or _uid(),
        idempotency_key=idempotency_key or _uid(),
        report_sequence=report_sequence,
        observed_at_ms=observed_at_ms if observed_at_ms is not None else now,
        expires_at_ms=expires_at_ms if expires_at_ms is not None else now + 60_000,
        configured_max_ttl_ms=configured_max_ttl_ms,
        entries=entries if entries is not None else [_session_entry(worker_session_id=_uid())],
        authenticated_principal_id="pool-manager-principal",
        authorization_context_digest="sha256:" + "1" * 64,
    )


def test_submit_capacity_report_creates_health_observation_and_response(store: RunStore) -> None:
    session_id = _uid()
    result = _submit(store, entries=[_session_entry(worker_session_id=session_id)])

    assert result.replayed is False
    assert len(result.health_observations) == 1
    observation = result.health_observations[0]
    assert observation.scope_kind == "WORKER_SESSION"
    assert observation.scope_id == session_id
    assert observation.kind == "AVAILABLE"
    assert observation.source_kind == "CAPACITY_REPORT"
    assert observation.source_id == result.capacity_report_id
    assert observation.health_sequence == 1
    assert result.woken_wait_condition_ids == ()

    fetched = store.get_latest_health_observation("WORKER_SESSION", session_id)
    assert fetched is not None
    assert fetched.health_observation_id == observation.health_observation_id


def test_unavailable_slots_derive_unavailable_kind(store: RunStore) -> None:
    session_id = _uid()
    result = _submit(
        store, entries=[_session_entry(worker_session_id=session_id, available_slots=0)]
    )
    assert result.health_observations[0].kind == "UNAVAILABLE"


def test_replaying_same_report_id_and_body_returns_stored_response(store: RunStore) -> None:
    report_id = _uid()
    idempotency_key = _uid()
    session_id = _uid()
    entries = [_session_entry(worker_session_id=session_id)]

    first = _submit(
        store,
        report_id=report_id,
        idempotency_key=idempotency_key,
        entries=entries,
    )
    assert first.replayed is False

    second = store.submit_capacity_report(
        capacity_report_id=_uid(),
        pool_manager_id=POOL_MANAGER_ID,
        report_id=report_id,
        idempotency_key=idempotency_key,
        report_sequence=1,
        observed_at_ms=first.accepted_at_ms,
        expires_at_ms=first.accepted_at_ms + 60_000,
        configured_max_ttl_ms=300_000,
        entries=entries,
        authenticated_principal_id="pool-manager-principal",
        authorization_context_digest="sha256:" + "1" * 64,
    )
    assert second.replayed is True
    assert second.capacity_report_id == first.capacity_report_id
    assert second.health_observations == first.health_observations
    assert store.conn.execute("SELECT COUNT(*) FROM capacity_reports").fetchone()[0] == 1
    assert store.conn.execute("SELECT COUNT(*) FROM health_observations").fetchone()[0] == 1


def test_reusing_report_id_with_different_body_conflicts(store: RunStore) -> None:
    report_id = _uid()
    _submit(store, report_id=report_id, entries=[_session_entry(worker_session_id=_uid())])
    with pytest.raises(IdempotencyConflictError):
        _submit(store, report_id=report_id, entries=[_session_entry(worker_session_id=_uid())])


def test_reusing_idempotency_key_with_different_body_conflicts(store: RunStore) -> None:
    idempotency_key = _uid()
    _submit(
        store, idempotency_key=idempotency_key, entries=[_session_entry(worker_session_id=_uid())]
    )
    with pytest.raises(IdempotencyConflictError):
        _submit(
            store,
            idempotency_key=idempotency_key,
            entries=[_session_entry(worker_session_id=_uid())],
        )


def test_out_of_order_report_sequence_is_rejected_without_ledger_row(store: RunStore) -> None:
    _submit(store, report_sequence=5)
    with pytest.raises(CasMismatchError):
        _submit(store, report_sequence=5)
    with pytest.raises(CasMismatchError):
        _submit(store, report_sequence=1)
    assert store.conn.execute("SELECT COUNT(*) FROM capacity_reports").fetchone()[0] == 1


def test_gap_in_report_sequence_is_accepted(store: RunStore) -> None:
    _submit(store, report_sequence=1)
    result = _submit(store, report_sequence=42)
    assert result.report_sequence == 42


def test_expires_at_ms_outside_ttl_bound_is_rejected(store: RunStore) -> None:
    now = _now_ms()
    with pytest.raises(ValueError):
        _submit(store, expires_at_ms=now + 10_000_000, configured_max_ttl_ms=1_000)
    assert store.conn.execute("SELECT COUNT(*) FROM capacity_reports").fetchone()[0] == 0


def test_duplicate_scope_in_entries_is_rejected(store: RunStore) -> None:
    session_id = _uid()
    with pytest.raises(ValueError):
        _submit(
            store,
            entries=[
                _session_entry(worker_session_id=session_id),
                _session_entry(worker_session_id=session_id),
            ],
        )


def test_entries_out_of_canonical_order_is_rejected(store: RunStore) -> None:
    pool_entry = CapacityReportEntryInput(
        scope_kind="CAPACITY_POOL",
        scope_id="default",
        capacity_pool_id="default",
        available_slots=4,
    )
    session_entry = _session_entry(worker_session_id=_uid())
    with pytest.raises(ValueError):
        _submit(store, entries=[pool_entry, session_entry])


def test_multiple_entries_get_ordered_health_observations(store: RunStore) -> None:
    session_id = _uid()
    entries = [
        _session_entry(worker_session_id=session_id),
        CapacityReportEntryInput(
            scope_kind="WORKER_PROFILE",
            scope_id="codex",
            capacity_pool_id="default",
            worker_profile="codex",
            available_slots=2,
        ),
        CapacityReportEntryInput(
            scope_kind="CAPACITY_POOL",
            scope_id="default",
            capacity_pool_id="default",
            available_slots=4,
        ),
    ]
    result = _submit(store, entries=entries)
    assert [obs.scope_kind for obs in result.health_observations] == [
        "WORKER_SESSION",
        "WORKER_PROFILE",
        "CAPACITY_POOL",
    ]


def test_wakes_waiting_capacity_run_and_returns_wait_condition_id(store: RunStore) -> None:
    run_id = _uid()
    wait_condition_id = _uid()
    with store.transaction():
        store.create_run(
            run_id=run_id,
            project_id="project-a",
            work_item_key="work-1",
            state="WAITING",
            specification_generation=1,
        )
        payload = {"wait_condition_id": wait_condition_id, "wait_reason": "CAPACITY"}
        store.put_revisioned_object(
            object_kind="run_pointers",
            object_id=run_id,
            expected_revision=0,
            payload_digest=request_digest(payload),
            payload=payload,
        )

    result = _submit(store, entries=[_session_entry(worker_session_id=_uid())])
    assert result.woken_wait_condition_ids == (wait_condition_id,)

    run_row = store.conn.execute("SELECT state FROM runs WHERE run_id = ?", (run_id,)).fetchone()
    assert run_row["state"] == "RECOVERING"


def test_unavailable_observation_does_not_wake_waiting_capacity_run(store: RunStore) -> None:
    run_id = _uid()
    with store.transaction():
        store.create_run(
            run_id=run_id,
            project_id="project-a",
            work_item_key="work-1",
            state="WAITING",
            specification_generation=1,
        )
        payload = {"wait_condition_id": "wait-1", "wait_reason": "CAPACITY"}
        store.put_revisioned_object(
            object_kind="run_pointers",
            object_id=run_id,
            expected_revision=0,
            payload_digest=request_digest(payload),
            payload=payload,
        )

    result = _submit(store, entries=[_session_entry(worker_session_id=_uid(), available_slots=0)])
    assert result.woken_wait_condition_ids == ()
    run_row = store.conn.execute("SELECT state FROM runs WHERE run_id = ?", (run_id,)).fetchone()
    assert run_row["state"] == "WAITING"
