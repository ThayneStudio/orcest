"""Cumulative Budget Report ledger and restartable per-Run wake fanout
(issue #680)."""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path

import pytest

from orcest.workflow_contract.v1.digest import request_digest
from orcest.workflow_contract.v1.protocol import validate_envelope
from orcest.workflow_store.store import (
    CasMismatchError,
    IdempotencyConflictError,
    RunStore,
)

pytestmark = pytest.mark.unit

PROJECT_ID = "66666666-6666-4666-8666-666666666666"
ACCOUNTING_SCOPE_ID = "default"


def _uid() -> str:
    return str(uuid.uuid4())


def _now_ms() -> int:
    return int(time.time() * 1000)


@pytest.fixture
def store(tmp_path: Path) -> RunStore:
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        yield store


def _waiting_run(
    store: RunStore,
    run_id: str,
    *,
    project_id: str = PROJECT_ID,
    accounting_scope_id: str = ACCOUNTING_SCOPE_ID,
    minimum_source_sequence: int = 1,
) -> str:
    wait_condition_id = _uid()
    with store.transaction():
        store.create_run(
            run_id=run_id,
            project_id=project_id,
            work_item_key=f"work-{run_id}",
            state="WAITING",
            specification_generation=1,
        )
        payload = {"wait_condition_id": wait_condition_id, "wait_reason": "BUDGET"}
        store.put_revisioned_object(
            object_kind="run_pointers",
            object_id=run_id,
            expected_revision=0,
            payload_digest=request_digest(payload),
            payload=payload,
        )
        store.create_wait_condition(
            wait_condition_id=wait_condition_id,
            run_id=run_id,
            reason="BUDGET",
            resume_state="BUILDING",
            specification_generation=1,
            policy_hash="sha256:" + "0" * 64,
            created_from_kind="RECOVERY_EVIDENCE",
            created_from_id=_uid(),
            created_transition_sequence=1,
            not_before_ms=_now_ms() + 3_600_000,
            wake_kind="BUDGET_WINDOW",
            wake_identity={
                "project_id": project_id,
                "accounting_scope_id": accounting_scope_id,
                "budget_policy_ref": "default",
                "budget_reset_window_ref": "default",
                "budget_report_id": _uid(),
                "window_id": "window-0",
                "reset_at_ms": _now_ms(),
                "minimum_source_sequence": minimum_source_sequence,
            },
        )
    return wait_condition_id


def _submit(store: RunStore, **overrides):
    now = _now_ms()
    kwargs = dict(
        budget_report_id=_uid(),
        project_id=PROJECT_ID,
        accounting_scope_id=ACCOUNTING_SCOPE_ID,
        budget_policy_ref="default",
        budget_reset_window_ref="default",
        window_id="window-1",
        window_start_ms=now - 1_000,
        reset_at_ms=now + 3_600_000,
        source_sequence=1,
        source_revision="rev-1",
        limit_microunits=1_000_000,
        consumed_microunits=0,
        authenticated_principal_id="budget-accounting-service",
        authorization_context_digest="sha256:" + "4" * 64,
        max_budget_report_age_ms=600_000,
    )
    kwargs.update(overrides)
    return store.submit_budget_report(**kwargs)


def test_submit_budget_report_derives_available(store: RunStore) -> None:
    result = _submit(store, limit_microunits=100, consumed_microunits=10)
    assert result.availability == "AVAILABLE"
    assert result.replayed is False
    body = json.loads(result.response_json)
    validate_envelope(body)
    assert body["replayed"] is False


def test_submit_budget_report_derives_exhausted(store: RunStore) -> None:
    result = _submit(store, limit_microunits=100, consumed_microunits=100)
    assert result.availability == "EXHAUSTED"


def test_replaying_same_budget_report_id_returns_stored_response(store: RunStore) -> None:
    budget_report_id = _uid()
    now = _now_ms()
    fixed = dict(window_start_ms=now - 1_000, reset_at_ms=now + 3_600_000)
    first = _submit(store, budget_report_id=budget_report_id, **fixed)
    second = _submit(store, budget_report_id=budget_report_id, **fixed)
    assert second.replayed is True
    replay_body = json.loads(second.response_json)
    validate_envelope(replay_body)
    assert replay_body["replayed"] is True
    assert second.response_digest == first.response_digest
    assert store.conn.execute("SELECT COUNT(*) FROM budget_reports").fetchone()[0] == 1


def test_replaying_same_body_with_different_principal_conflicts(store: RunStore) -> None:
    budget_report_id = _uid()
    now = _now_ms()
    fixed = dict(window_start_ms=now - 1_000, reset_at_ms=now + 3_600_000)
    _submit(store, budget_report_id=budget_report_id, **fixed)

    with pytest.raises(IdempotencyConflictError):
        _submit(
            store,
            budget_report_id=budget_report_id,
            authenticated_principal_id="other-budget-accounting-service",
            **fixed,
        )


def test_reusing_budget_report_id_with_different_body_conflicts(store: RunStore) -> None:
    budget_report_id = _uid()
    _submit(store, budget_report_id=budget_report_id, consumed_microunits=1)
    with pytest.raises(IdempotencyConflictError):
        _submit(store, budget_report_id=budget_report_id, consumed_microunits=2)


def test_reusing_source_revision_under_different_id_conflicts(store: RunStore) -> None:
    _submit(store, source_revision="rev-shared", source_sequence=1)
    with pytest.raises(IdempotencyConflictError):
        _submit(store, source_revision="rev-shared", source_sequence=2)


def test_out_of_order_source_sequence_is_rejected_without_ledger_row(store: RunStore) -> None:
    _submit(store, source_sequence=5, source_revision="rev-5")
    with pytest.raises(CasMismatchError):
        _submit(store, source_sequence=5, source_revision="rev-5-again")
    with pytest.raises(CasMismatchError):
        _submit(store, source_sequence=1, source_revision="rev-1")
    assert store.conn.execute("SELECT COUNT(*) FROM budget_reports").fetchone()[0] == 1


def test_expiry_is_bounded_by_reset_and_max_age(store: RunStore) -> None:
    result = _submit(
        store,
        reset_at_ms=_now_ms() + 10_000_000,
        max_budget_report_age_ms=1_000,
    )
    row = store.conn.execute(
        "SELECT accepted_at_ms, expires_at_ms FROM budget_reports WHERE budget_report_id = ?",
        (result.budget_report_id,),
    ).fetchone()
    assert row["expires_at_ms"] == row["accepted_at_ms"] + 1_000


def test_available_report_freezes_and_wakes_same_project_waiting_budget_runs(
    store: RunStore,
) -> None:
    wait_condition_id = _waiting_run(store, _uid())
    other_project_run = _uid()
    _waiting_run(store, other_project_run, project_id="other-project")

    result = _submit(store, limit_microunits=100, consumed_microunits=0)
    assert result.availability == "AVAILABLE"

    members = store.conn.execute(
        "SELECT run_id FROM budget_report_runs WHERE budget_report_id = ? ORDER BY member_ordinal",
        (result.budget_report_id,),
    ).fetchall()
    assert len(members) == 1

    other_run_row = store.conn.execute(
        "SELECT state FROM runs WHERE run_id = ?", (other_project_run,)
    ).fetchone()
    assert other_run_row["state"] == "WAITING"

    fanout_row = store.conn.execute(
        "SELECT next_member_ordinal, fanout_completed_at_ms FROM budget_reports "
        "WHERE budget_report_id = ?",
        (result.budget_report_id,),
    ).fetchone()
    assert fanout_row["next_member_ordinal"] == 1
    assert fanout_row["fanout_completed_at_ms"] is not None
    _ = wait_condition_id


def test_exhausted_report_freezes_empty_membership(store: RunStore) -> None:
    _waiting_run(store, _uid())
    result = _submit(store, limit_microunits=100, consumed_microunits=100)
    assert result.availability == "EXHAUSTED"
    members = store.conn.execute(
        "SELECT COUNT(*) FROM budget_report_runs WHERE budget_report_id = ?",
        (result.budget_report_id,),
    ).fetchone()[0]
    assert members == 0


def test_fanout_wakes_each_member_run_exactly_once(store: RunStore) -> None:
    run_a = _uid()
    run_b = _uid()
    _waiting_run(store, run_a)
    _waiting_run(store, run_b)

    result = _submit(store, limit_microunits=100, consumed_microunits=0)

    for run_id in (run_a, run_b):
        run_row = store.conn.execute(
            "SELECT state FROM runs WHERE run_id = ?", (run_id,)
        ).fetchone()
        assert run_row["state"] == "RECOVERING"

    transition_count = store.conn.execute(
        "SELECT COUNT(*) FROM transitions WHERE trigger_kind = 'BUDGET_REPORT' AND trigger_id = ?",
        (result.budget_report_id,),
    ).fetchone()[0]
    assert transition_count == 2


def test_run_budget_report_fanout_is_idempotent_and_resumable(store: RunStore) -> None:
    """Acceptance fans out eagerly in the same transaction; a later
    reconciliation-sweep call to the public, restartable entry point must be
    a safe no-op that neither re-applies a member Transition nor disturbs the
    already-recorded fanout completion."""
    run_a = _uid()
    run_b = _uid()
    _waiting_run(store, run_a)
    _waiting_run(store, run_b)

    result = _submit(store, limit_microunits=100, consumed_microunits=0)
    fanout_before = store.conn.execute(
        "SELECT next_member_ordinal, fanout_completed_at_ms FROM budget_reports "
        "WHERE budget_report_id = ?",
        (result.budget_report_id,),
    ).fetchone()
    assert fanout_before["next_member_ordinal"] == 2
    assert fanout_before["fanout_completed_at_ms"] is not None

    # Simulate a resumable partial state: only member 0's cursor advance is
    # visible, as if the process crashed after committing that Transition but
    # before the outer commit recorded the rest.
    with store.transaction():
        store.conn.execute(
            "UPDATE budget_reports SET next_member_ordinal = 1, fanout_completed_at_ms = NULL "
            "WHERE budget_report_id = ?",
            (result.budget_report_id,),
        )

    store.run_budget_report_fanout(result.budget_report_id)

    for run_id in (run_a, run_b):
        run_row = store.conn.execute(
            "SELECT state FROM runs WHERE run_id = ?", (run_id,)
        ).fetchone()
        assert run_row["state"] == "RECOVERING"

    transition_count = store.conn.execute(
        "SELECT COUNT(*) FROM transitions WHERE trigger_kind = 'BUDGET_REPORT' AND trigger_id = ?",
        (result.budget_report_id,),
    ).fetchone()[0]
    assert transition_count == 2

    fanout_after = store.conn.execute(
        "SELECT next_member_ordinal, fanout_completed_at_ms FROM budget_reports "
        "WHERE budget_report_id = ?",
        (result.budget_report_id,),
    ).fetchone()
    assert fanout_after["next_member_ordinal"] == 2
    assert fanout_after["fanout_completed_at_ms"] is not None

    # Calling it again once genuinely complete is a pure no-op.
    store.run_budget_report_fanout(result.budget_report_id)
    transition_count_again = store.conn.execute(
        "SELECT COUNT(*) FROM transitions WHERE trigger_kind = 'BUDGET_REPORT' AND trigger_id = ?",
        (result.budget_report_id,),
    ).fetchone()[0]
    assert transition_count_again == 2


def test_get_latest_budget_report_ignores_expired_reports(store: RunStore) -> None:
    result = _submit(store, max_budget_report_age_ms=1)
    latest = store.get_latest_budget_report(
        PROJECT_ID, ACCOUNTING_SCOPE_ID, now_ms=result.accepted_at_ms + 1_000_000
    )
    assert latest is None


def test_get_latest_budget_report_returns_greatest_sequence(store: RunStore) -> None:
    _submit(store, source_sequence=1, source_revision="rev-1")
    second = _submit(store, source_sequence=2, source_revision="rev-2")
    latest = store.get_latest_budget_report(PROJECT_ID, ACCOUNTING_SCOPE_ID)
    assert latest is not None
    assert latest.budget_report_id == second.budget_report_id
