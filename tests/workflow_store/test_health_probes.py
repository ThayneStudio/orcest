"""Request-first Health Probe Request/Fact fanout."""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest

from orcest.workflow_contract.v1.digest import request_digest
from orcest.workflow_store.store import IdempotencyConflictError, RunStore

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


def _integrity_probe_request(
    store: RunStore, *, request_id: str, digest: str, request_identity: str | None = None
) -> dict[str, object]:
    args: dict[str, object] = dict(
        health_probe_request_id=request_id,
        probe_kind="STORAGE_OBJECT_INTEGRITY",
        scope_kind="STORAGE",
        scope_id=digest,
        request_identity=request_identity or f"candidate:{digest}",
        subject_bindings={"object_kind": "CANDIDATE_ARTIFACT", "object_id": digest},
        implementation_digest=_digest("impl"),
        input_digest=_digest("input"),
        evidence_digest=_digest("evidence"),
    )
    store.create_health_probe_request(**args)
    return args


def test_health_probe_request_is_persisted_with_outbox_before_probe_io(store: RunStore) -> None:
    request_id = _uid()
    outbox_id = _uid()
    record = store.create_health_probe_request(
        health_probe_request_id=request_id,
        outbox_id=outbox_id,
        probe_kind="STORAGE_OBJECT_INTEGRITY",
        scope_kind="STORAGE",
        scope_id="sha256:" + "1" * 64,
        request_identity="storage:sha256:" + "1" * 64,
        subject_bindings={
            "object_kind": "CANDIDATE_ARTIFACT",
            "object_id": "sha256:" + "1" * 64,
        },
        implementation_digest=_digest("impl"),
        input_digest=_digest("input"),
        evidence_digest=_digest("evidence"),
    )

    assert record.state == "PENDING"
    outbox = store.conn.execute("SELECT * FROM outbox WHERE outbox_id = ?", (outbox_id,)).fetchone()
    assert outbox is not None
    assert outbox["source_kind"] == "HEALTH_PROBE_REQUEST"
    assert outbox["source_id"] == request_id


def test_health_probe_unavailable_suspends_only_frozen_members_and_replays_once(
    store: RunStore,
) -> None:
    affected = _uid()
    unrelated = _uid()
    _run(store, affected)
    _run(store, unrelated)
    request_id = _uid()
    fact_id = _uid()
    bundle_digest = "sha256:" + "2" * 64
    _integrity_probe_request(store, request_id=request_id, digest=bundle_digest)

    first = store.complete_health_probe_request(
        health_probe_request_id=request_id,
        health_probe_fact_id=fact_id,
        outcome="UNAVAILABLE",
        integrity_failure_code="MISSING",
        evidence={"stat": "ENOENT"},
        affected_run_ids=[affected],
    )
    replay = store.complete_health_probe_request(
        health_probe_request_id=request_id,
        health_probe_fact_id=fact_id,
        outcome="UNAVAILABLE",
        integrity_failure_code="MISSING",
        evidence={"stat": "ENOENT"},
        affected_run_ids=[affected],
    )

    assert first.replayed is False
    assert replay.replayed is True
    assert first.fact.affected_run_ids == (affected,)
    assert replay.fact.affected_run_ids == (affected,)
    assert store.conn.execute("SELECT COUNT(*) FROM health_probe_facts").fetchone()[0] == 1
    assert store.conn.execute("SELECT COUNT(*) FROM health_observations").fetchone()[0] == 1
    assert (
        store.conn.execute(
            "SELECT COUNT(*) FROM transitions WHERE trigger_kind = 'HEALTH_OBSERVATION'"
        ).fetchone()[0]
        == 1
    )
    assert store.conn.execute("SELECT COUNT(*) FROM recovery_evidence").fetchone()[0] == 1

    affected_row = store.conn.execute(
        "SELECT state, wait_condition_id FROM runs WHERE run_id = ?", (affected,)
    ).fetchone()
    unrelated_row = store.conn.execute(
        "SELECT state, wait_condition_id FROM runs WHERE run_id = ?", (unrelated,)
    ).fetchone()
    assert affected_row["state"] == "WAITING"
    assert affected_row["wait_condition_id"] is not None
    assert unrelated_row["state"] == "BUILDING"
    assert unrelated_row["wait_condition_id"] is None
    wait = store.get_wait_condition(affected_row["wait_condition_id"])
    assert wait is not None
    assert wait.reason == "STORAGE_RECOVERY"
    assert wait.wake_identity is not None
    assert wait.wake_identity["object_id"] == bundle_digest


def test_available_integrity_probe_resumes_recovering_run(store: RunStore) -> None:
    run_id = _uid()
    _run(store, run_id, state="RECOVERING")
    with store.transaction():
        payload = {
            "next_transition_sequence": 1,
            "policy_hash": "sha256:" + "0" * 64,
            "generation_installed": True,
            "initial_plan_absent": False,
            "recovery_origin_state": "BUILDING",
        }
        store.put_revisioned_object(
            object_kind="run_pointers",
            object_id=run_id,
            expected_revision=1,
            payload_digest=request_digest(payload),
            payload=payload,
        )
    request_id = _uid()
    fact_id = _uid()
    bundle_digest = "sha256:" + "4" * 64
    _integrity_probe_request(store, request_id=request_id, digest=bundle_digest)

    result = store.complete_health_probe_request(
        health_probe_request_id=request_id,
        health_probe_fact_id=fact_id,
        outcome="AVAILABLE",
        evidence={"verified": True},
        affected_run_ids=[run_id],
    )

    assert result.recovery_evidence_ids
    run_row = store.conn.execute("SELECT state FROM runs WHERE run_id = ?", (run_id,)).fetchone()
    assert run_row["state"] == "BUILDING"
    assert store.get_current_wait_condition(run_id) is None


def test_available_integrity_recheck_wakes_exact_storage_wait(store: RunStore) -> None:
    run_id = _uid()
    _run(store, run_id)
    bundle_digest = "sha256:" + "6" * 64
    down_request_id = _uid()
    _integrity_probe_request(store, request_id=down_request_id, digest=bundle_digest)
    store.complete_health_probe_request(
        health_probe_request_id=down_request_id,
        health_probe_fact_id=_uid(),
        outcome="UNAVAILABLE",
        integrity_failure_code="MISSING",
        evidence={"stat": "ENOENT"},
        affected_run_ids=[run_id],
    )
    wait = store.get_current_wait_condition(run_id)
    assert wait is not None

    up_request_id = _uid()
    _integrity_probe_request(
        store,
        request_id=up_request_id,
        digest=bundle_digest,
        request_identity=f"candidate:{bundle_digest}:recheck",
    )
    result = store.complete_health_probe_request(
        health_probe_request_id=up_request_id,
        health_probe_fact_id=_uid(),
        outcome="AVAILABLE",
        evidence={"verified": True},
        affected_run_ids=[run_id],
    )

    assert result.recovery_evidence_ids
    run = store.get_run(run_id)
    assert run is not None
    assert run.state == "BUILDING"
    assert store.get_current_wait_condition(run_id) is None


def test_unrelated_available_integrity_probe_cannot_wake_storage_wait(store: RunStore) -> None:
    run_id = _uid()
    _run(store, run_id)
    missing_digest = "sha256:" + "7" * 64
    down_request_id = _uid()
    _integrity_probe_request(store, request_id=down_request_id, digest=missing_digest)
    store.complete_health_probe_request(
        health_probe_request_id=down_request_id,
        health_probe_fact_id=_uid(),
        outcome="UNAVAILABLE",
        integrity_failure_code="MISSING",
        evidence={"stat": "ENOENT"},
        affected_run_ids=[run_id],
    )
    wait = store.get_current_wait_condition(run_id)
    assert wait is not None

    other_digest = "sha256:" + "8" * 64
    up_request_id = _uid()
    _integrity_probe_request(store, request_id=up_request_id, digest=other_digest)
    store.complete_health_probe_request(
        health_probe_request_id=up_request_id,
        health_probe_fact_id=_uid(),
        outcome="AVAILABLE",
        evidence={"verified": True},
        affected_run_ids=[run_id],
    )

    run = store.get_run(run_id)
    assert run is not None
    assert run.state == "WAITING"
    assert store.get_current_wait_condition(run_id) == wait


def test_provider_probe_suspends_and_available_recheck_resumes_exact_member(
    store: RunStore,
) -> None:
    affected = _uid()
    unrelated = _uid()
    _run(store, affected)
    _run(store, unrelated)
    provider_scope = "codex:account-1"
    down_request_id = _uid()
    down_fact_id = _uid()
    store.create_health_probe_request(
        health_probe_request_id=down_request_id,
        probe_kind="PROVIDER_ACCOUNT_STATUS",
        scope_kind="PROVIDER_ACCOUNT",
        scope_id=provider_scope,
        request_identity=f"provider:{provider_scope}:down",
        subject_bindings={"provider": "codex", "account": "account-1"},
        implementation_digest=_digest("provider-impl"),
        input_digest=_digest("provider-input"),
        evidence_digest=_digest("provider-evidence"),
    )
    store.complete_health_probe_request(
        health_probe_request_id=down_request_id,
        health_probe_fact_id=down_fact_id,
        outcome="UNAVAILABLE",
        evidence={"status": "unreachable"},
        affected_run_ids=[affected],
    )

    wait = store.get_current_wait_condition(affected)
    assert wait is not None
    assert wait.reason == "CAPACITY"
    assert wait.wake_identity == {"scope_kind": "PROVIDER_ACCOUNT", "scope_id": provider_scope}
    unrelated_run = store.get_run(unrelated)
    assert unrelated_run is not None
    assert unrelated_run.state == "BUILDING"

    up_request_id = _uid()
    up_fact_id = _uid()
    store.create_health_probe_request(
        health_probe_request_id=up_request_id,
        probe_kind="PROVIDER_ACCOUNT_STATUS",
        scope_kind="PROVIDER_ACCOUNT",
        scope_id=provider_scope,
        request_identity=f"provider:{provider_scope}:up",
        subject_bindings={"provider": "codex", "account": "account-1"},
        implementation_digest=_digest("provider-impl"),
        input_digest=_digest("provider-input"),
        evidence_digest=_digest("provider-evidence-up"),
    )
    store.complete_health_probe_request(
        health_probe_request_id=up_request_id,
        health_probe_fact_id=up_fact_id,
        outcome="AVAILABLE",
        evidence={"status": "ok"},
        affected_run_ids=[affected],
    )

    affected_run = store.get_run(affected)
    assert affected_run is not None
    assert affected_run.state == "BUILDING"
    assert store.get_current_wait_condition(affected) is None


def test_health_probe_request_identity_conflict_is_rejected(store: RunStore) -> None:
    request_id = _uid()
    store.create_health_probe_request(
        health_probe_request_id=request_id,
        probe_kind="PROVIDER_ACCOUNT_STATUS",
        scope_kind="PROVIDER_ACCOUNT",
        scope_id="codex:acct",
        request_identity="provider:codex:acct",
        subject_bindings={"provider": "codex"},
        implementation_digest=_digest("impl"),
        input_digest=_digest("input"),
        evidence_digest=_digest("evidence"),
    )

    with pytest.raises(IdempotencyConflictError):
        store.create_health_probe_request(
            health_probe_request_id=request_id,
            probe_kind="PROVIDER_ACCOUNT_STATUS",
            scope_kind="PROVIDER_ACCOUNT",
            scope_id="codex:acct",
            request_identity="provider:codex:acct",
            subject_bindings={"provider": "codex", "changed": True},
            implementation_digest=_digest("impl"),
            input_digest=_digest("input"),
            evidence_digest=_digest("evidence"),
        )


def test_health_probe_matrix_is_closed(store: RunStore) -> None:
    with pytest.raises(ValueError):
        store.create_health_probe_request(
            health_probe_request_id=_uid(),
            probe_kind="SECRET_VERSION_INTEGRITY",
            scope_kind="STORAGE",
            scope_id="sha256:" + "3" * 64,
            request_identity="bad",
            subject_bindings={},
            implementation_digest=_digest("impl"),
            input_digest=_digest("input"),
            evidence_digest=_digest("evidence"),
        )


def test_non_integrity_probe_rejects_integrity_failure_code(store: RunStore) -> None:
    request_id = _uid()
    store.create_health_probe_request(
        health_probe_request_id=request_id,
        probe_kind="PROVIDER_ACCOUNT_STATUS",
        scope_kind="PROVIDER_ACCOUNT",
        scope_id="codex:acct",
        request_identity="provider:codex:acct:down",
        subject_bindings={"provider": "codex", "account": "acct"},
        implementation_digest=_digest("impl"),
        input_digest=_digest("input"),
        evidence_digest=_digest("evidence"),
    )

    with pytest.raises(ValueError):
        store.complete_health_probe_request(
            health_probe_request_id=request_id,
            health_probe_fact_id=_uid(),
            outcome="UNAVAILABLE",
            integrity_failure_code="MISSING",
            evidence={"status": "down"},
            affected_run_ids=[],
        )


def test_integrity_probe_rejects_wrong_failure_code_for_kind(store: RunStore) -> None:
    request_id = _uid()
    digest = "sha256:" + "5" * 64
    _integrity_probe_request(store, request_id=request_id, digest=digest)

    with pytest.raises(ValueError):
        store.complete_health_probe_request(
            health_probe_request_id=request_id,
            health_probe_fact_id=_uid(),
            outcome="UNAVAILABLE",
            integrity_failure_code="KEYED_ATTESTATION_MISMATCH",
            evidence={"stat": "bad"},
            affected_run_ids=[],
        )
