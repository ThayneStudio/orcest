"""Recovery Evidence persistence for Workflow-Control v1."""

from __future__ import annotations

from pathlib import Path

import pytest

from orcest.workflow_contract.v1.digest import request_digest
from orcest.workflow_store import HealthObservationRecord, IdempotencyConflictError, RunStore

pytestmark = pytest.mark.unit

RUN_ID = "11111111-1111-4111-8111-111111111111"
EVIDENCE_ID = "22222222-2222-4222-8222-222222222222"


def _create_run(store: RunStore) -> None:
    with store.transaction():
        store.create_run(
            run_id=RUN_ID,
            project_id="project-a",
            work_item_key="work-1",
            state="RECOVERING",
            specification_generation=1,
        )


def _health(store: RunStore, scope_id: str, sequence_bias: int) -> HealthObservationRecord:
    with store.transaction():
        for index in range(sequence_bias):
            record = store._insert_health_observation(
                scope_kind="PROVIDER_ACCOUNT",
                scope_id=scope_id,
                kind="AVAILABLE",
                source_kind="CAPACITY_REPORT",
                source_id=f"{scope_id}/report-{index}",
                subject_bindings={"scope": scope_id, "index": index},
                observed_revision=index,
                effective_at_ms=1000 + index,
                expires_at_ms=100_000,
            )
    return record


def test_recovery_evidence_freezes_ordered_health_membership(tmp_path: Path) -> None:
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        _create_run(store)
        later_a = _health(store, "provider-a/account-1", 3)
        earlier_b = _health(store, "provider-b/account-2", 1)
        fingerprint = request_digest({"category": "PROVIDER_RATE_LIMIT", "scope": "attempt-1"})
        first = store.create_recovery_evidence(
            recovery_evidence_id=EVIDENCE_ID,
            run_id=RUN_ID,
            source_kind="ATTEMPT_RESULT",
            source_id="attempt-1",
            category="PROVIDER_RATE_LIMIT",
            failure_fingerprint=fingerprint,
            strategy_index=4,
            selected_tactic="REPLACE_CAPACITY",
            attempt_count=1,
            repair_cycle_count=0,
            diagnosis_count=0,
            rescue_epoch=0,
            selected_fallback="provider-b/account-2",
            health_observations=(later_a, earlier_b),
            specification_generation=1,
        )
        replay = store.create_recovery_evidence(
            recovery_evidence_id=EVIDENCE_ID,
            run_id=RUN_ID,
            source_kind="ATTEMPT_RESULT",
            source_id="attempt-1",
            category="PROVIDER_RATE_LIMIT",
            failure_fingerprint=fingerprint,
            strategy_index=4,
            selected_tactic="REPLACE_CAPACITY",
            attempt_count=1,
            repair_cycle_count=0,
            diagnosis_count=0,
            rescue_epoch=0,
            selected_fallback="provider-b/account-2",
            health_observations=(earlier_b, later_a),
            specification_generation=1,
        )
        assert first.recovery_sequence == 1
        assert replay == first
        assert first.health_observation_ids == (
            later_a.health_observation_id,
            earlier_b.health_observation_id,
        )
        pointer = store.conn.execute(
            "SELECT current_recovery_evidence_id FROM runs WHERE run_id = ?", (RUN_ID,)
        ).fetchone()
        assert pointer[0] == EVIDENCE_ID


def test_recovery_evidence_source_reuse_with_different_tactic_conflicts(
    tmp_path: Path,
) -> None:
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        _create_run(store)
        fingerprint = request_digest({"category": "BUDGET", "scope": "run"})
        store.create_recovery_evidence(
            recovery_evidence_id=EVIDENCE_ID,
            run_id=RUN_ID,
            source_kind="BUDGET_REPORT",
            source_id="budget-1",
            category="BUDGET",
            failure_fingerprint=fingerprint,
            strategy_index=10,
            selected_tactic="WAIT_BUDGET",
            attempt_count=0,
            repair_cycle_count=0,
            diagnosis_count=0,
            rescue_epoch=0,
        )
        with pytest.raises(IdempotencyConflictError):
            store.create_recovery_evidence(
                recovery_evidence_id=EVIDENCE_ID,
                run_id=RUN_ID,
                source_kind="BUDGET_REPORT",
                source_id="budget-1",
                category="BUDGET",
                failure_fingerprint=fingerprint,
                strategy_index=3,
                selected_tactic="RETRY_EXECUTION",
                attempt_count=1,
                repair_cycle_count=0,
                diagnosis_count=0,
                rescue_epoch=0,
            )
