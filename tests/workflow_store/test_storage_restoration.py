"""Storage Restoration Operations/Facts and affected-Run fanout (issue #695).

persistence-and-recovery.md "Per-object storage restoration": a missing or
corrupt live Candidate/Secret/Workflow Blob object suspends only the affected
Runs (via the existing Health Probe Fact path); a Storage Restoration Fact is
the sole authority that resumes them, and it never repeats a restoration for
the same exact object/source identity.
"""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest

from orcest.workflow_contract.v1.digest import request_digest
from orcest.workflow_store.store import (
    CasMismatchError,
    IdempotencyConflictError,
    RunStore,
)

pytestmark = pytest.mark.unit


def _uid() -> str:
    return str(uuid.uuid4())


def _digest(label: str) -> str:
    return request_digest({"test": label})


@pytest.fixture
def store(tmp_path: Path) -> RunStore:
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        yield store


def _run(store: RunStore, run_id: str, *, state: str = "BUILDING") -> None:
    with store.transaction():
        store.create_run(
            run_id=run_id,
            project_id="project-a",
            work_item_key=run_id,
            state=state,
            specification_generation=1,
        )
        payload = {
            "next_transition_sequence": 1,
            "policy_hash": "sha256:" + "0" * 64,
            "generation_installed": True,
            "initial_plan_absent": False,
        }
        store.put_revisioned_object(
            object_kind="run_pointers",
            object_id=run_id,
            expected_revision=0,
            payload_digest=request_digest(payload),
            payload=payload,
        )


def _suspend_run_on_missing_candidate(store: RunStore, *, run_id: str, bundle_digest: str) -> None:
    """Drive ``run_id`` into ``WAITING``/``STORAGE_RECOVERY`` the same way a
    real ``STORAGE_OBJECT_INTEGRITY`` probe would (test_health_probes.py)."""
    request_id = _uid()
    fact_id = _uid()
    store.create_health_probe_request(
        health_probe_request_id=request_id,
        probe_kind="STORAGE_OBJECT_INTEGRITY",
        scope_kind="STORAGE",
        scope_id=bundle_digest,
        request_identity=f"candidate:{bundle_digest}",
        subject_bindings={"object_kind": "CANDIDATE_ARTIFACT", "object_id": bundle_digest},
        implementation_digest=_digest("impl"),
        input_digest=_digest("input"),
        evidence_digest=_digest("evidence"),
    )
    store.complete_health_probe_request(
        health_probe_request_id=request_id,
        health_probe_fact_id=fact_id,
        outcome="UNAVAILABLE",
        integrity_failure_code="MISSING",
        evidence={"stat": "ENOENT"},
        affected_run_ids=[run_id],
    )


# -- Controller State / Redis epoch -----------------------------------------


def test_controller_state_singleton_bootstraps_at_epoch_zero(store: RunStore) -> None:
    state = store.get_controller_state()
    assert state.controller_id == "ORCEST_V1"
    assert state.redis_epoch == 0
    assert state.schema_version > 0
    assert state.compatibility_version > 0


def test_advance_redis_epoch_is_monotonic(store: RunStore) -> None:
    first = store.advance_redis_epoch()
    second = store.advance_redis_epoch()
    assert first == 1
    assert second == 2
    assert store.get_controller_state().redis_epoch == 2


# -- Storage Restoration Operations ------------------------------------------


def test_begin_storage_restoration_operation_replay_returns_same_projection(
    store: RunStore,
) -> None:
    operation_id = _uid()
    digest = "sha256:" + "a" * 64
    kwargs = dict(
        operation_id=operation_id,
        object_kind="CANDIDATE_ARTIFACT",
        object_id=digest,
        expected_byte_length=10,
        media_kind=None,
        authenticated_principal_id="controller-storage-reconciler",
        authorization_context_digest=_digest("auth"),
        staged_object_key="incoming/" + _uid(),
    )
    first = store.begin_storage_restoration_operation(**kwargs)
    replay = store.begin_storage_restoration_operation(**kwargs)

    assert first.state == "PENDING"
    assert replay == first
    assert (
        store.conn.execute("SELECT COUNT(*) FROM storage_restoration_operations").fetchone()[0] == 1
    )


def test_begin_storage_restoration_operation_rejects_conflicting_replay(store: RunStore) -> None:
    operation_id = _uid()
    digest = "sha256:" + "b" * 64
    store.begin_storage_restoration_operation(
        operation_id=operation_id,
        object_kind="CANDIDATE_ARTIFACT",
        object_id=digest,
        expected_byte_length=10,
        media_kind=None,
        authenticated_principal_id="controller-storage-reconciler",
        authorization_context_digest=_digest("auth"),
        staged_object_key="incoming/" + _uid(),
    )

    with pytest.raises(IdempotencyConflictError):
        store.begin_storage_restoration_operation(
            operation_id=operation_id,
            object_kind="CANDIDATE_ARTIFACT",
            object_id=digest,
            expected_byte_length=999,
            media_kind=None,
            authenticated_principal_id="controller-storage-reconciler",
            authorization_context_digest=_digest("auth"),
            staged_object_key="incoming/" + _uid(),
        )


def test_complete_storage_restoration_operation_is_idempotent_and_cas_fenced(
    store: RunStore,
) -> None:
    operation_id = _uid()
    digest = "sha256:" + "c" * 64
    store.begin_storage_restoration_operation(
        operation_id=operation_id,
        object_kind="CANDIDATE_ARTIFACT",
        object_id=digest,
        expected_byte_length=10,
        media_kind=None,
        authenticated_principal_id="controller-storage-reconciler",
        authorization_context_digest=_digest("auth"),
        staged_object_key="incoming/" + _uid(),
    )
    fact = store.create_storage_restoration_fact(
        storage_restoration_fact_id=_uid(),
        object_kind="CANDIDATE_ARTIFACT",
        object_id=digest,
        source_kind="AUTHENTICATED_STORAGE_OPERATION",
        source_id=operation_id,
        matched_digest=digest,
    )
    fact_id = fact.storage_restoration_fact_id

    first = store.complete_storage_restoration_operation(
        operation_id=operation_id, storage_restoration_fact_id=fact_id
    )
    replay = store.complete_storage_restoration_operation(
        operation_id=operation_id, storage_restoration_fact_id=fact_id
    )

    assert first.state == "RESTORED"
    assert first.storage_restoration_fact_id == fact_id
    assert replay == first

    with pytest.raises(CasMismatchError):
        store.complete_storage_restoration_operation(
            operation_id=operation_id, storage_restoration_fact_id=_uid()
        )


def test_fail_storage_restoration_operation_is_idempotent_and_cas_fenced(store: RunStore) -> None:
    operation_id = _uid()
    digest = "sha256:" + "d" * 64
    store.begin_storage_restoration_operation(
        operation_id=operation_id,
        object_kind="CANDIDATE_ARTIFACT",
        object_id=digest,
        expected_byte_length=10,
        media_kind=None,
        authenticated_principal_id="controller-storage-reconciler",
        authorization_context_digest=_digest("auth"),
        staged_object_key="incoming/" + _uid(),
    )

    first = store.fail_storage_restoration_operation(
        operation_id=operation_id, rejection_code="STAGED_OBJECT_INVALID"
    )
    replay = store.fail_storage_restoration_operation(
        operation_id=operation_id, rejection_code="STAGED_OBJECT_INVALID"
    )

    assert first.state == "REJECTED"
    assert first.rejection_code == "STAGED_OBJECT_INVALID"
    assert replay == first

    with pytest.raises(CasMismatchError):
        store.fail_storage_restoration_operation(
            operation_id=operation_id, rejection_code="INTEGRITY_CONFLICT"
        )


def test_list_pending_storage_restoration_operations_excludes_terminal(store: RunStore) -> None:
    pending_id = _uid()
    restored_id = _uid()
    digest = "sha256:" + "e" * 64
    for op_id in (pending_id, restored_id):
        store.begin_storage_restoration_operation(
            operation_id=op_id,
            object_kind="CANDIDATE_ARTIFACT",
            object_id=digest,
            expected_byte_length=10,
            media_kind=None,
            authenticated_principal_id="controller-storage-reconciler",
            authorization_context_digest=_digest("auth"),
            staged_object_key="incoming/" + _uid(),
        )
    fact = store.create_storage_restoration_fact(
        storage_restoration_fact_id=_uid(),
        object_kind="CANDIDATE_ARTIFACT",
        object_id=digest,
        source_kind="AUTHENTICATED_STORAGE_OPERATION",
        source_id=restored_id,
        matched_digest=digest,
    )
    store.complete_storage_restoration_operation(
        operation_id=restored_id, storage_restoration_fact_id=fact.storage_restoration_fact_id
    )

    pending = store.list_pending_storage_restoration_operations()

    assert [op.operation_id for op in pending] == [pending_id]


# -- Storage Restoration Facts and affected-Run fanout -----------------------


def test_storage_restoration_fact_wakes_only_frozen_member_and_replays_once(
    store: RunStore,
) -> None:
    affected = _uid()
    unrelated = _uid()
    _run(store, affected)
    _run(store, unrelated)
    bundle_digest = "sha256:" + "1" * 64
    _suspend_run_on_missing_candidate(store, run_id=affected, bundle_digest=bundle_digest)

    affected_before = store.conn.execute(
        "SELECT state FROM runs WHERE run_id = ?", (affected,)
    ).fetchone()
    assert affected_before["state"] == "WAITING"

    fact_id = _uid()
    first = store.create_storage_restoration_fact(
        storage_restoration_fact_id=fact_id,
        object_kind="CANDIDATE_ARTIFACT",
        object_id=bundle_digest,
        source_kind="BACKUP_RESTORE",
        source_id=_uid(),
        matched_digest=bundle_digest,
    )
    replay = store.create_storage_restoration_fact(
        storage_restoration_fact_id=fact_id,
        object_kind="CANDIDATE_ARTIFACT",
        object_id=bundle_digest,
        source_kind="BACKUP_RESTORE",
        source_id=first.source_id,
        matched_digest=bundle_digest,
    )

    assert first.affected_run_ids == (affected,)
    assert first.fanout_completed_at_ms is not None
    assert replay.storage_restoration_fact_id == first.storage_restoration_fact_id
    assert replay.affected_run_ids == first.affected_run_ids
    assert store.conn.execute("SELECT COUNT(*) FROM storage_restoration_facts").fetchone()[0] == 1

    affected_row = store.conn.execute(
        "SELECT state, wait_condition_id FROM runs WHERE run_id = ?", (affected,)
    ).fetchone()
    unrelated_row = store.conn.execute(
        "SELECT state, wait_condition_id FROM runs WHERE run_id = ?", (unrelated,)
    ).fetchone()
    assert affected_row["state"] == "RECOVERING"
    assert affected_row["wait_condition_id"] is None
    assert unrelated_row["state"] == "BUILDING"

    observation = store.conn.execute(
        "SELECT kind, source_kind, source_id FROM health_observations WHERE source_id = ?",
        (fact_id,),
    ).fetchone()
    assert observation["kind"] == "RECOVERED"
    assert observation["source_kind"] == "STORAGE_RESTORATION"


def test_storage_restoration_fact_rejects_reused_id_with_different_digest(
    store: RunStore,
) -> None:
    bundle_digest = "sha256:" + "2" * 64
    source_id = _uid()
    fact_id = _uid()
    store.create_storage_restoration_fact(
        storage_restoration_fact_id=fact_id,
        object_kind="CANDIDATE_ARTIFACT",
        object_id=bundle_digest,
        source_kind="BACKUP_RESTORE",
        source_id=source_id,
        matched_digest=bundle_digest,
    )

    with pytest.raises(IdempotencyConflictError):
        store.create_storage_restoration_fact(
            storage_restoration_fact_id=fact_id,
            object_kind="CANDIDATE_ARTIFACT",
            object_id=bundle_digest,
            source_kind="BACKUP_RESTORE",
            source_id=source_id,
            matched_digest="sha256:" + "3" * 64,
        )


def test_storage_restoration_fact_with_no_frozen_runs_completes_empty_fanout(
    store: RunStore,
) -> None:
    fact = store.create_storage_restoration_fact(
        storage_restoration_fact_id=_uid(),
        object_kind="WORKFLOW_BLOB",
        object_id="sha256:" + "4" * 64,
        source_kind="AUTHENTICATED_STORAGE_OPERATION",
        source_id=_uid(),
        matched_digest="sha256:" + "4" * 64,
    )

    assert fact.affected_run_ids == ()
    assert fact.fanout_completed_at_ms is not None
