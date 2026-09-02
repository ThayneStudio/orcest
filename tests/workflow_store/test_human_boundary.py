"""Durable Human Boundary / Human Resolution persistence (issue #689)."""

from __future__ import annotations

import time
import uuid
from pathlib import Path

import pytest

from orcest.workflow_store import IdempotencyConflictError, RunStore, RunStoreError

pytestmark = pytest.mark.unit

MISSING_AUTHORITY_CONSEQUENCES = {
    "AUTHORITY_GRANTED": "Resumes the paused publish with the granted authority."
}


def _uid() -> str:
    return str(uuid.uuid4())


def _now_ms() -> int:
    return int(time.time() * 1000)


@pytest.fixture
def store(tmp_path: Path) -> RunStore:
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        yield store


def _create_recovering_run(
    store: RunStore,
    run_id: str,
    *,
    project_id: str = "project-a",
    recovery_origin_state: str = "BUILDING",
) -> None:
    from orcest.workflow_contract.v1.digest import request_digest

    with store.transaction():
        store.create_run(
            run_id=run_id,
            project_id=project_id,
            work_item_key=f"work-{run_id}",
            state="RECOVERING",
            specification_generation=1,
        )
        payload = {"recovery_origin_state": recovery_origin_state}
        store.put_revisioned_object(
            object_kind="run_pointers",
            object_id=run_id,
            expected_revision=0,
            payload_digest=request_digest(payload),
            payload=payload,
        )


def _enter_missing_authority_boundary(store: RunStore, run_id: str) -> str:
    """Drive a RECOVERING run into NEEDS_HUMAN/MISSING_AUTHORITY and return
    the created Human Boundary id."""
    outcome = store.submit_recovery_evidence(
        recovery_evidence_id=_uid(),
        run_id=run_id,
        source_kind="CONTROLLER_OPERATION",
        source_id=_uid(),
        facts={"outcome": "FAILED", "failure_category": "POLICY"},
        exhausted_autonomous=True,
        human_boundary_reason="MISSING_AUTHORITY",
        human_boundary_minimum_request=(
            "Need admin authorization to force-push the protected branch."
        ),
        human_boundary_choice_consequences=MISSING_AUTHORITY_CONSEQUENCES,
        accepted_at_ms=_now_ms(),
    )
    assert outcome.selected_tactic == "ENTER_HUMAN_BOUNDARY"
    assert outcome.human_boundary is not None
    return outcome.human_boundary.human_boundary_id


# -- create_human_boundary ---------------------------------------------------


def test_create_human_boundary_basic_shape(store: RunStore) -> None:
    run_id = _uid()
    _create_recovering_run(store, run_id)

    boundary = store.create_human_boundary(
        human_boundary_id=_uid(),
        run_id=run_id,
        reason="MISSING_AUTHORITY",
        resume_state="BUILDING",
        minimum_request="Need authority to proceed.",
        created_from_kind="RECOVERY_EVIDENCE",
        created_from_id=_uid(),
        created_transition_sequence=1,
        choice_consequences=MISSING_AUTHORITY_CONSEQUENCES,
    )

    assert boundary.reason == "MISSING_AUTHORITY"
    assert boundary.required_resolution_kinds == ("AUTHORITY_GRANTED",)
    assert len(boundary.choices) == 1
    assert boundary.choices[0].resolution_kind == "AUTHORITY_GRANTED"

    run_row = store.conn.execute(
        "SELECT human_boundary_id FROM runs WHERE run_id = ?", (run_id,)
    ).fetchone()
    assert run_row["human_boundary_id"] == boundary.human_boundary_id


def test_create_human_boundary_is_idempotent_by_created_from_identity(store: RunStore) -> None:
    run_id = _uid()
    _create_recovering_run(store, run_id)
    created_from_id = _uid()
    kwargs = dict(
        run_id=run_id,
        reason="MISSING_AUTHORITY",
        resume_state="BUILDING",
        minimum_request="Need authority to proceed.",
        created_from_kind="RECOVERY_EVIDENCE",
        created_from_id=created_from_id,
        created_transition_sequence=1,
        choice_consequences=MISSING_AUTHORITY_CONSEQUENCES,
    )

    first = store.create_human_boundary(human_boundary_id=_uid(), **kwargs)
    second = store.create_human_boundary(human_boundary_id=first.human_boundary_id, **kwargs)

    assert second.human_boundary_id == first.human_boundary_id
    count = store.conn.execute("SELECT COUNT(*) AS n FROM human_boundaries").fetchone()["n"]
    assert count == 1


def test_create_human_boundary_rejects_reused_identity_with_different_content(
    store: RunStore,
) -> None:
    run_id = _uid()
    _create_recovering_run(store, run_id)
    created_from_id = _uid()
    store.create_human_boundary(
        human_boundary_id=_uid(),
        run_id=run_id,
        reason="MISSING_AUTHORITY",
        resume_state="BUILDING",
        minimum_request="Need authority to proceed.",
        created_from_kind="RECOVERY_EVIDENCE",
        created_from_id=created_from_id,
        created_transition_sequence=1,
        choice_consequences=MISSING_AUTHORITY_CONSEQUENCES,
    )

    with pytest.raises(IdempotencyConflictError):
        store.create_human_boundary(
            human_boundary_id=_uid(),
            run_id=run_id,
            reason="MISSING_AUTHORITY",
            resume_state="BUILDING",
            minimum_request="A completely different request.",
            created_from_kind="RECOVERY_EVIDENCE",
            created_from_id=created_from_id,
            created_transition_sequence=1,
            choice_consequences=MISSING_AUTHORITY_CONSEQUENCES,
        )


def test_create_human_boundary_rejects_second_active_boundary_for_run(store: RunStore) -> None:
    run_id = _uid()
    _create_recovering_run(store, run_id)
    store.create_human_boundary(
        human_boundary_id=_uid(),
        run_id=run_id,
        reason="MISSING_AUTHORITY",
        resume_state="BUILDING",
        minimum_request="Need authority to proceed.",
        created_from_kind="RECOVERY_EVIDENCE",
        created_from_id=_uid(),
        created_transition_sequence=1,
        choice_consequences=MISSING_AUTHORITY_CONSEQUENCES,
    )

    with pytest.raises(ValueError, match="already has a current human boundary"):
        store.create_human_boundary(
            human_boundary_id=_uid(),
            run_id=run_id,
            reason="SECURITY_POLICY_BOUNDARY",
            resume_state="BUILDING",
            minimum_request="Need security sign-off.",
            created_from_kind="RECOVERY_EVIDENCE",
            created_from_id=_uid(),
            created_transition_sequence=2,
            choice_consequences={
                "SECURITY_ACTION_AUTHORIZED": "Authorizes the classified operation."
            },
        )


@pytest.mark.parametrize(
    ("reason", "created_from_kind"),
    [
        ("PUBLICATION_OWNERSHIP_CONFLICT", "RECOVERY_EVIDENCE"),
        ("MISSING_AUTHORITY", "RECONCILIATION_FACT"),
    ],
)
def test_create_human_boundary_rejects_wrong_created_from_kind_pairing(
    store: RunStore, reason: str, created_from_kind: str
) -> None:
    run_id = _uid()
    _create_recovering_run(store, run_id)
    kwargs = dict(
        human_boundary_id=_uid(),
        run_id=run_id,
        reason=reason,
        resume_state="BUILDING",
        minimum_request="Need a decision.",
        created_from_kind=created_from_kind,
        created_from_id=_uid(),
        created_transition_sequence=1,
    )
    if reason == "PUBLICATION_OWNERSHIP_CONFLICT":
        kwargs.update(
            ownership_project_id="project-1",
            ownership_deterministic_ref="refs/heads/main",
            ownership_change_request_external_id="123",
            ownership_run_marker="orcest-v1:abc",
        )
    with pytest.raises(ValueError, match="sole direct Reconciliation Fact"):
        store.create_human_boundary(**kwargs)


def test_create_human_boundary_ownership_conflict_has_one_fixed_choice(store: RunStore) -> None:
    run_id = _uid()
    _create_recovering_run(store, run_id)

    boundary = store.create_human_boundary(
        human_boundary_id=_uid(),
        run_id=run_id,
        reason="PUBLICATION_OWNERSHIP_CONFLICT",
        resume_state="PR_MONITORING",
        minimum_request="Resolve the ownership conflict.",
        created_from_kind="RECONCILIATION_FACT",
        created_from_id=_uid(),
        created_transition_sequence=1,
        ownership_project_id="project-1",
        ownership_deterministic_ref="refs/heads/main",
        ownership_change_request_external_id="123",
        ownership_run_marker="orcest-v1:abc",
    )

    assert boundary.required_resolution_kinds == ("PUBLICATION_OWNERSHIP_RESOLVED",)
    assert len(boundary.choices) == 1
    assert boundary.choices[0].resolution_kind == "PUBLICATION_OWNERSHIP_RESOLVED"


def test_create_human_boundary_rejects_choice_consequences_mismatch(store: RunStore) -> None:
    run_id = _uid()
    _create_recovering_run(store, run_id)
    with pytest.raises(ValueError, match="choice_consequences must supply exactly"):
        store.create_human_boundary(
            human_boundary_id=_uid(),
            run_id=run_id,
            reason="MISSING_AUTHORITY",
            resume_state="BUILDING",
            minimum_request="Need authority to proceed.",
            created_from_kind="RECOVERY_EVIDENCE",
            created_from_id=_uid(),
            created_transition_sequence=1,
            choice_consequences={"SECURITY_ACTION_AUTHORIZED": "wrong kind"},
        )


def test_create_human_boundary_rejects_blank_minimum_request(store: RunStore) -> None:
    run_id = _uid()
    _create_recovering_run(store, run_id)
    with pytest.raises(ValueError, match="must not be blank"):
        store.create_human_boundary(
            human_boundary_id=_uid(),
            run_id=run_id,
            reason="MISSING_AUTHORITY",
            resume_state="BUILDING",
            minimum_request="   ",
            created_from_kind="RECOVERY_EVIDENCE",
            created_from_id=_uid(),
            created_transition_sequence=1,
            choice_consequences=MISSING_AUTHORITY_CONSEQUENCES,
        )


# -- submit_recovery_evidence: ENTER_HUMAN_BOUNDARY --------------------------


def test_submit_recovery_evidence_enters_human_boundary(store: RunStore) -> None:
    run_id = _uid()
    _create_recovering_run(store, run_id, recovery_origin_state="BUILDING")

    boundary_id = _enter_missing_authority_boundary(store, run_id)

    run_row = store.conn.execute(
        "SELECT state, human_boundary_id FROM runs WHERE run_id = ?", (run_id,)
    ).fetchone()
    assert run_row["state"] == "NEEDS_HUMAN"
    assert run_row["human_boundary_id"] == boundary_id

    boundary = store.get_current_human_boundary(run_id)
    assert boundary is not None
    assert boundary.reason == "MISSING_AUTHORITY"
    assert boundary.resume_state == "BUILDING"


def test_submit_recovery_evidence_enter_human_boundary_requires_minimum_request(
    store: RunStore,
) -> None:
    run_id = _uid()
    _create_recovering_run(store, run_id)
    with pytest.raises(ValueError, match="requires human_boundary_minimum_request"):
        store.submit_recovery_evidence(
            recovery_evidence_id=_uid(),
            run_id=run_id,
            source_kind="CONTROLLER_OPERATION",
            source_id=_uid(),
            facts={"outcome": "FAILED", "failure_category": "POLICY"},
            exhausted_autonomous=True,
            human_boundary_reason="MISSING_AUTHORITY",
            accepted_at_ms=_now_ms(),
        )


def test_submit_recovery_evidence_enter_human_boundary_rejects_reason_outside_category(
    store: RunStore,
) -> None:
    run_id = _uid()
    _create_recovering_run(store, run_id)
    with pytest.raises(ValueError, match="not allowlisted for category"):
        store.submit_recovery_evidence(
            recovery_evidence_id=_uid(),
            run_id=run_id,
            source_kind="CONTROLLER_OPERATION",
            source_id=_uid(),
            facts={"outcome": "FAILED", "failure_category": "POLICY"},
            exhausted_autonomous=True,
            human_boundary_reason="INTEGRITY_FAILURE",
            human_boundary_minimum_request="Need a decision.",
            human_boundary_choice_consequences={"INTEGRITY_RESTORED": "restored"},
            accepted_at_ms=_now_ms(),
        )


# -- submit_human_resolution --------------------------------------------------


def test_submit_human_resolution_authority_granted_resumes_run(store: RunStore) -> None:
    run_id = _uid()
    _create_recovering_run(store, run_id, recovery_origin_state="BUILDING")
    boundary_id = _enter_missing_authority_boundary(store, run_id)

    outcome = store.submit_human_resolution(
        human_resolution_id=_uid(),
        human_boundary_id=boundary_id,
        run_id=run_id,
        source_kind="MANAGEMENT_COMMAND",
        source_id=_uid(),
        authenticated_principal_id="ops-lead",
        resolution_kind="AUTHORITY_GRANTED",
        resolution={"granted_authority": "force-push", "scope": "run-scoped"},
    )

    assert outcome.applied.reduction.next_state == "RECOVERING"
    run_row = store.conn.execute(
        "SELECT state, human_boundary_id FROM runs WHERE run_id = ?", (run_id,)
    ).fetchone()
    assert run_row["state"] == "RECOVERING"
    assert run_row["human_boundary_id"] is None


def test_submit_human_resolution_rejects_wrong_kind_for_reason(store: RunStore) -> None:
    run_id = _uid()
    _create_recovering_run(store, run_id)
    boundary_id = _enter_missing_authority_boundary(store, run_id)

    with pytest.raises(ValueError, match="is not permitted by boundary reason"):
        store.submit_human_resolution(
            human_resolution_id=_uid(),
            human_boundary_id=boundary_id,
            run_id=run_id,
            source_kind="MANAGEMENT_COMMAND",
            source_id=_uid(),
            authenticated_principal_id="ops-lead",
            resolution_kind="SECURITY_ACTION_AUTHORIZED",
            resolution={"authorized_action": "force-push"},
        )


def test_submit_human_resolution_rejects_incomplete_schema(store: RunStore) -> None:
    run_id = _uid()
    _create_recovering_run(store, run_id)
    boundary_id = _enter_missing_authority_boundary(store, run_id)

    with pytest.raises(ValueError, match="requires exactly"):
        store.submit_human_resolution(
            human_resolution_id=_uid(),
            human_boundary_id=boundary_id,
            run_id=run_id,
            source_kind="MANAGEMENT_COMMAND",
            source_id=_uid(),
            authenticated_principal_id="ops-lead",
            resolution_kind="AUTHORITY_GRANTED",
            resolution={"granted_authority": "force-push"},
        )


def test_submit_human_resolution_replay_is_idempotent(store: RunStore) -> None:
    run_id = _uid()
    _create_recovering_run(store, run_id)
    boundary_id = _enter_missing_authority_boundary(store, run_id)
    command_id = _uid()
    kwargs = dict(
        human_resolution_id=_uid(),
        human_boundary_id=boundary_id,
        run_id=run_id,
        source_kind="MANAGEMENT_COMMAND",
        source_id=command_id,
        authenticated_principal_id="ops-lead",
        resolution_kind="AUTHORITY_GRANTED",
        resolution={"granted_authority": "force-push", "scope": "run-scoped"},
    )

    first = store.submit_human_resolution(**kwargs)
    second = store.submit_human_resolution(**kwargs)

    assert second.human_resolution.human_resolution_id == first.human_resolution.human_resolution_id
    assert second.applied.replayed is True
    count = store.conn.execute("SELECT COUNT(*) AS n FROM human_resolutions").fetchone()["n"]
    assert count == 1


def test_submit_human_resolution_conflicting_replay_rejected(store: RunStore) -> None:
    run_id = _uid()
    _create_recovering_run(store, run_id)
    boundary_id = _enter_missing_authority_boundary(store, run_id)
    command_id = _uid()
    store.submit_human_resolution(
        human_resolution_id=_uid(),
        human_boundary_id=boundary_id,
        run_id=run_id,
        source_kind="MANAGEMENT_COMMAND",
        source_id=command_id,
        authenticated_principal_id="ops-lead",
        resolution_kind="AUTHORITY_GRANTED",
        resolution={"granted_authority": "force-push", "scope": "run-scoped"},
    )

    with pytest.raises(IdempotencyConflictError):
        store.submit_human_resolution(
            human_resolution_id=_uid(),
            human_boundary_id=boundary_id,
            run_id=run_id,
            source_kind="MANAGEMENT_COMMAND",
            source_id=command_id,
            authenticated_principal_id="ops-lead",
            resolution_kind="AUTHORITY_GRANTED",
            resolution={"granted_authority": "something-else", "scope": "run-scoped"},
        )


def test_submit_human_resolution_rejects_stale_boundary(store: RunStore) -> None:
    run_id = _uid()
    _create_recovering_run(store, run_id)
    boundary_id = _enter_missing_authority_boundary(store, run_id)
    store.submit_human_resolution(
        human_resolution_id=_uid(),
        human_boundary_id=boundary_id,
        run_id=run_id,
        source_kind="MANAGEMENT_COMMAND",
        source_id=_uid(),
        authenticated_principal_id="ops-lead",
        resolution_kind="AUTHORITY_GRANTED",
        resolution={"granted_authority": "force-push", "scope": "run-scoped"},
    )

    with pytest.raises(RunStoreError, match="already resolved or superseded"):
        store.submit_human_resolution(
            human_resolution_id=_uid(),
            human_boundary_id=boundary_id,
            run_id=run_id,
            source_kind="MANAGEMENT_COMMAND",
            source_id=_uid(),
            authenticated_principal_id="ops-lead",
            resolution_kind="AUTHORITY_GRANTED",
            resolution={"granted_authority": "force-push-again", "scope": "run-scoped"},
        )


def test_submit_human_resolution_management_command_cannot_synthesize_specification_amended(
    store: RunStore,
) -> None:
    run_id = _uid()
    _create_recovering_run(store, run_id)
    boundary_id = store.create_human_boundary(
        human_boundary_id=_uid(),
        run_id=run_id,
        reason="SPECIFICATION_CONFLICT",
        resume_state="PLANNING",
        minimum_request="Resolve the conflicting requirements.",
        created_from_kind="RECOVERY_EVIDENCE",
        created_from_id=_uid(),
        created_transition_sequence=1,
        choice_consequences={"SPECIFICATION_AMENDED": "Amends the specification."},
    ).human_boundary_id

    with pytest.raises(ValueError, match="cannot be sourced from"):
        store.submit_human_resolution(
            human_resolution_id=_uid(),
            human_boundary_id=boundary_id,
            run_id=run_id,
            source_kind="MANAGEMENT_COMMAND",
            source_id=_uid(),
            authenticated_principal_id="ops-lead",
            resolution_kind="SPECIFICATION_AMENDED",
            resolution={},
        )


def test_submit_human_resolution_secret_version_requires_verifier_principal(
    store: RunStore,
) -> None:
    run_id = _uid()
    _create_recovering_run(store, run_id)
    boundary_id = store.create_human_boundary(
        human_boundary_id=_uid(),
        run_id=run_id,
        reason="REQUIRED_SECRET_OR_PERMISSION",
        resume_state="BUILDING",
        minimum_request="Need a rotated secret.",
        created_from_kind="RECOVERY_EVIDENCE",
        created_from_id=_uid(),
        created_transition_sequence=1,
        choice_consequences={"SECRET_OR_PERMISSION_PROVIDED": "Provides the secret."},
    ).human_boundary_id

    with pytest.raises(ValueError, match="Secret-Store verifier"):
        store.submit_human_resolution(
            human_resolution_id=_uid(),
            human_boundary_id=boundary_id,
            run_id=run_id,
            source_kind="SECRET_VERSION",
            source_id=f"{_uid()}:1",
            authenticated_principal_id="some-worker",
            resolution_kind="SECRET_OR_PERMISSION_PROVIDED",
            resolution={
                "secret_version_key": "placeholder",
                "creation_receipt_id": "placeholder",
                "integrity_attestation_id": "placeholder",
            },
        )


def _seed_secret_version(store: RunStore, secret_id: str, version: int, receipt_id: str) -> None:
    now = _now_ms()
    with store.transaction():
        store.conn.execute(
            "INSERT INTO secret_current_versions(secret_id, purpose, owner_scope_kind, "
            "owner_scope_id, provider_account_ref, current_version, last_operation_id, "
            "created_at_ms, updated_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (secret_id, "FORGE_API", "PROJECT", "project-a", None, version, "op-1", now, now),
        )
        store.conn.execute(
            "INSERT INTO secret_versions(secret_id, version, creation_receipt_id, "
            "storage_path, affected_run_ids_digest, created_at_ms) VALUES (?, ?, ?, ?, ?, ?)",
            (secret_id, version, receipt_id, "secrets/path", "sha256:" + "0" * 64, now),
        )


def test_submit_human_resolution_secret_version_success(store: RunStore) -> None:
    run_id = _uid()
    _create_recovering_run(store, run_id, recovery_origin_state="BUILDING")
    boundary_id = store.create_human_boundary(
        human_boundary_id=_uid(),
        run_id=run_id,
        reason="REQUIRED_SECRET_OR_PERMISSION",
        resume_state="BUILDING",
        minimum_request="Need a rotated secret.",
        created_from_kind="RECOVERY_EVIDENCE",
        created_from_id=_uid(),
        created_transition_sequence=1,
        choice_consequences={"SECRET_OR_PERMISSION_PROVIDED": "Provides the secret."},
    ).human_boundary_id
    secret_id = _uid()
    _seed_secret_version(store, secret_id, 1, "receipt-1")
    source_id = f"{secret_id}:1"

    outcome = store.submit_human_resolution(
        human_resolution_id=_uid(),
        human_boundary_id=boundary_id,
        run_id=run_id,
        source_kind="SECRET_VERSION",
        source_id=source_id,
        authenticated_principal_id="controller-secret-store-verifier",
        resolution_kind="SECRET_OR_PERMISSION_PROVIDED",
        resolution={
            "secret_version_key": source_id,
            "creation_receipt_id": "receipt-1",
            "integrity_attestation_id": "attestation-1",
        },
    )

    assert outcome.applied.reduction.next_state == "RECOVERING"


def test_submit_human_resolution_ownership_resolved_bindings_must_match(store: RunStore) -> None:
    run_id = _uid()
    _create_recovering_run(store, run_id)
    boundary_id = store.create_human_boundary(
        human_boundary_id=_uid(),
        run_id=run_id,
        reason="PUBLICATION_OWNERSHIP_CONFLICT",
        resume_state="PR_MONITORING",
        minimum_request="Resolve the ownership conflict.",
        created_from_kind="RECONCILIATION_FACT",
        created_from_id=_uid(),
        created_transition_sequence=1,
        ownership_project_id="project-1",
        ownership_deterministic_ref="refs/heads/main",
        ownership_change_request_external_id="123",
        ownership_run_marker="orcest-v1:abc",
    ).human_boundary_id

    with pytest.raises(ValueError, match="must equal the current boundary"):
        store.submit_human_resolution(
            human_resolution_id=_uid(),
            human_boundary_id=boundary_id,
            run_id=run_id,
            source_kind="MANAGEMENT_COMMAND",
            source_id=_uid(),
            authenticated_principal_id="ops-lead",
            resolution_kind="PUBLICATION_OWNERSHIP_RESOLVED",
            resolution={
                "selected_engine": "ORCEST_V1",
                "project_id": "wrong-project",
                "deterministic_ref": "refs/heads/main",
                "change_request_external_id": "123",
                "run_marker": "orcest-v1:abc",
                "publication_id": "11111111-1111-4111-8111-111111111111",
                "effect_generation": 1,
            },
        )
