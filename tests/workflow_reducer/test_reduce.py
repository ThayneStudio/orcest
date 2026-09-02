"""Named reducer branches beyond the default contract fixtures."""

from __future__ import annotations

import pytest

from orcest.workflow_reducer.contract import default_view
from orcest.workflow_reducer.reduce import reduce
from orcest.workflow_reducer.types import IllegalTransitionError, ReductionKind, Trigger

pytestmark = pytest.mark.unit


def test_review_receipts_in_either_order_reach_aggregating() -> None:
    first = default_view(
        "REVIEWING",
        "ATTEMPT_RESULT",
        panel_complete=False,
        filling_review_slots=("slot-a",),
    )
    r1 = reduce(
        first,
        Trigger(
            kind="ATTEMPT_RESULT",
            trigger_id="attempt-a",
            facts={"outcome": "SUCCEEDED", "activity_kind": "REVIEW", "fills_slot": True},
        ),
    )
    assert r1.next_state == "REVIEWING"
    second = default_view(
        "REVIEWING",
        "ATTEMPT_RESULT",
        panel_complete=True,
        filling_review_slots=("slot-a", "slot-b"),
        unfilled_review_slots=(),
    )
    r2 = reduce(
        second,
        Trigger(
            kind="ATTEMPT_RESULT",
            trigger_id="attempt-b",
            facts={
                "outcome": "SUCCEEDED",
                "activity_kind": "REVIEW",
                "fills_slot": True,
                "panel_complete": True,
            },
        ),
    )
    assert r2.next_state == "AGGREGATING"
    swapped = reduce(
        second,
        Trigger(
            kind="ATTEMPT_RESULT",
            trigger_id="attempt-a",
            facts={
                "outcome": "SUCCEEDED",
                "activity_kind": "REVIEW",
                "fills_slot": True,
                "panel_complete": True,
            },
        ),
    )
    assert swapped.next_state == r2.next_state
    assert swapped.reason_code == r2.reason_code


def test_verify_fail_below_threshold_remediates() -> None:
    view = default_view("VERIFYING", "ATTEMPT_RESULT")
    reduction = reduce(
        view,
        Trigger(
            kind="ATTEMPT_RESULT",
            trigger_id="attempt-v",
            facts={
                "outcome": "SUCCEEDED",
                "activity_kind": "VERIFY",
                "verification_outcome": "FAIL",
            },
        ),
    )
    assert reduction.kind is ReductionKind.ADVANCE
    assert reduction.next_state == "REMEDIATING"
    assert any(activity.kind == "REMEDIATE" for activity in reduction.planned_activities)


def test_candidate_producing_success_without_candidate_enters_recovery() -> None:
    view = default_view("BUILDING", "ATTEMPT_RESULT")
    reduction = reduce(
        view,
        Trigger(
            kind="ATTEMPT_RESULT",
            trigger_id="attempt-build",
            facts={"outcome": "SUCCEEDED", "activity_kind": "BUILD"},
        ),
    )
    assert reduction.next_state == "RECOVERING"
    assert reduction.reason_code == "MISSING_CANDIDATE"
    assert reduction.pending_continuation is not None
    assert reduction.pending_continuation.kind == "RECOVERY_EVIDENCE"


def test_plan_success_requires_validated_structured_output_protocol() -> None:
    view = default_view("PLANNING", "ATTEMPT_RESULT")
    reduction = reduce(
        view,
        Trigger(
            kind="ATTEMPT_RESULT",
            trigger_id="attempt-plan",
            facts={"outcome": "SUCCEEDED", "activity_kind": "PLAN"},
        ),
    )
    assert reduction.next_state == "RECOVERING"
    assert reduction.reason_code == "INVALID_STRUCTURED_OUTPUT"


def test_policy_only_replan_reselects_prior_candidate_after_identity_revalidation() -> None:
    view = default_view(
        "REPLANNING",
        "ATTEMPT_RESULT",
        policy_replan_candidate_id="66666666-6666-4666-8666-666666666666",
    )
    reduction = reduce(
        view,
        Trigger(
            kind="ATTEMPT_RESULT",
            trigger_id="attempt-replan",
            facts={
                "outcome": "SUCCEEDED",
                "activity_kind": "REPLAN",
                "structured_output_protocol": "orcest.plan/1",
                "policy_identity_holds": True,
            },
        ),
    )
    assert reduction.next_state == "VERIFYING"
    assert reduction.reason_code == "POLICY_REPLAN_RESELECT"
    assert reduction.pointer_updates["current_candidate_id"] == (
        "66666666-6666-4666-8666-666666666666"
    )
    assert any(activity.kind == "VERIFY" for activity in reduction.planned_activities)


def test_cancellation_suppresses_semantic_work() -> None:
    view = default_view(
        "PLANNING",
        "ATTEMPT_RESULT",
        cancellation_source_kind="MANAGEMENT_COMMAND",
        cancellation_source_id="cmd-1",
    )
    reduction = reduce(
        view,
        Trigger(
            kind="ATTEMPT_RESULT",
            trigger_id="attempt-1",
            facts={"outcome": "SUCCEEDED", "activity_kind": "PLAN"},
        ),
    )
    assert reduction.kind is ReductionKind.SUPERSEDED
    assert reduction.next_state == "PLANNING"
    assert reduction.reason_code == "CANCELLATION_PRECEDENCE"
    assert reduction.planned_activities == ()


def test_pending_snapshot_wins_internal_arbitration() -> None:
    view = default_view(
        "PLANNING",
        "INTERNAL",
        pending_snapshot_id="22222222-2222-2222-2222-222222222222",
        pending_internal_sequence=1,
    )
    reduction = reduce(view, Trigger(kind="INTERNAL", trigger_id="1", facts={}))
    assert reduction.reason_code == "PENDING_SNAPSHOT_PRECEDENCE"
    assert reduction.pending_continuation is not None
    assert reduction.pending_continuation.kind == "SPEC_SUPERSEDE"


def test_generation_install_does_not_plan_work() -> None:
    view = default_view("ADMITTED", "SPEC_SUPERSEDE")
    reduction = reduce(
        view,
        Trigger(
            kind="SPEC_SUPERSEDE",
            trigger_id="22222222-2222-2222-2222-222222222222",
            facts={"install": True},
        ),
    )
    assert reduction.next_state == "ADMITTED"
    assert reduction.specification_generation == 1
    assert reduction.planned_activities == ()
    assert reduction.pending_continuation is not None
    assert reduction.pending_continuation.kind == "INTERNAL"


def test_head_advanced_forge_observation_advances_to_pr_remediating() -> None:
    view = default_view("PR_MONITORING", "FORGE_OBSERVATION")
    reduction = reduce(
        view,
        Trigger(
            kind="FORGE_OBSERVATION",
            trigger_id="obs-head-1",
            facts={"kind": "CHANGE_REQUEST_HEAD", "head_advanced": True},
        ),
    )
    assert reduction.kind is ReductionKind.ADVANCE
    assert reduction.next_state == "PR_REMEDIATING"
    assert reduction.reason_code == "HEAD_ADVANCED"
    assert reduction.same_state is False
    assert reduction.emits_semantic_work is True
    assert any(activity.kind == "IMPORT" for activity in reduction.planned_activities)
    assert reduction.consume_forge_observation_ids == ("obs-head-1",)


def test_stale_health_fanout_does_not_change_state() -> None:
    view = default_view("BUILDING", "HEALTH_OBSERVATION")
    reduction = reduce(
        view,
        Trigger(kind="HEALTH_OBSERVATION", trigger_id="health-1", facts={"kind": "AVAILABLE"}),
    )
    assert reduction.kind is ReductionKind.STALE
    assert reduction.next_state == "BUILDING"


def test_recovery_evidence_rejects_tactic_that_does_not_match_evidence() -> None:
    view = default_view("RECOVERING", "RECOVERY_EVIDENCE")
    with pytest.raises(IllegalTransitionError, match="does not match deterministic selection"):
        reduce(
            view,
            Trigger(
                kind="RECOVERY_EVIDENCE",
                trigger_id="re-1",
                facts={
                    "source_kind": "BUDGET_REPORT",
                    "source_id": "budget-1",
                    "category": "BUDGET",
                    "selected_tactic": "RETRY_EXECUTION",
                },
            ),
        )


def test_recovery_evidence_applies_deterministic_wait_tactic() -> None:
    view = default_view("RECOVERING", "RECOVERY_EVIDENCE")
    reduction = reduce(
        view,
        Trigger(
            kind="RECOVERY_EVIDENCE",
            trigger_id="re-budget",
            facts={
                "source_kind": "BUDGET_REPORT",
                "source_id": "budget-1",
                "category": "BUDGET",
                "selected_tactic": "WAIT_BUDGET",
            },
        ),
    )
    assert reduction.next_state == "WAITING"
    assert reduction.reason_code == "WAIT_BUDGET"
    assert reduction.pointer_updates["current_recovery_evidence_id"] == "re-budget"


def test_recovery_evidence_external_wait_reason_comes_from_category() -> None:
    view = default_view("RECOVERING", "RECOVERY_EVIDENCE")
    reduction = reduce(
        view,
        Trigger(
            kind="RECOVERY_EVIDENCE",
            trigger_id="re-secret",
            facts={
                "source_kind": "SECRET_VERSION",
                "source_id": "secret:1",
                "category": "CREDENTIAL",
                "selected_tactic": "WAIT_EXTERNAL",
            },
        ),
    )
    assert reduction.next_state == "WAITING"
    assert reduction.pointer_updates["wait_reason"] == "SECRET_RECOVERY"


@pytest.mark.parametrize(
    ("facts", "expected_state", "expected_reason"),
    [
        (
            {
                "source_kind": "FORGE_REQUEST_FAILURE",
                "source_id": "forge-request-1",
                "category": "FORGE_TRANSIENT",
                "accepted_at_ms": 1_700_000_000_000,
                "selected_tactic": "WAIT_BACKOFF",
            },
            "WAITING",
            "WAIT_BACKOFF",
        ),
        (
            {
                "source_kind": "TIMER_FACT",
                "source_id": "timer-1",
                "category": "CAPACITY",
                "resumed_wait_condition_id": "wait-1",
                "selected_tactic": "WAIT_EVIDENCE",
            },
            "WAITING",
            "WAIT_EVIDENCE",
        ),
        (
            {
                "source_kind": "HEALTH_OBSERVATION",
                "source_id": "health-1",
                "category": "CAPACITY",
                "bounded_evidence": {"staff_panel": True},
                "selected_tactic": "STAFF_PANEL",
            },
            "REVIEWING",
            "STAFF_PANEL",
        ),
        (
            {
                "source_kind": "FORGE_OBSERVATION",
                "source_id": "forge-1",
                "category": "BASE_CONFLICT",
                "bounded_evidence": {"external_head_importable": True},
                "selected_tactic": "IMPORT_EXTERNAL_HEAD",
            },
            "PR_REMEDIATING",
            "IMPORT_EXTERNAL_HEAD",
        ),
        (
            {
                "source_kind": "FORGE_OBSERVATION",
                "source_id": "forge-2",
                "category": "BASE_CONFLICT",
                "bounded_evidence": {"reconstruct_foreign_head": True},
                "selected_tactic": "RECONSTRUCT_FOREIGN_HEAD",
            },
            "REMEDIATING",
            "RECONSTRUCT_FOREIGN_HEAD",
        ),
        (
            {
                "source_kind": "FORGE_OBSERVATION",
                "source_id": "forge-3",
                "category": "REVIEW_DISAGREEMENT",
                "bounded_evidence": {"reconcile_publication": True},
                "selected_tactic": "RECONCILE",
            },
            "PLANNING",
            "RECONCILE",
        ),
        (
            {
                "source_kind": "RECONCILIATION_FACT",
                "source_id": "reconcile-1",
                "category": "FORGE_TRANSIENT",
                "bounded_evidence": {"effect_absent": True},
                "selected_tactic": "REDELIVER",
            },
            "PLANNING",
            "REDELIVER",
        ),
        (
            {
                "source_kind": "CONTROLLER_OPERATION",
                "source_id": "cop-1",
                "category": "POLICY",
                "exhausted_autonomous": True,
                "human_boundary_reason": "MISSING_AUTHORITY",
                "selected_tactic": "ENTER_HUMAN_BOUNDARY",
            },
            "NEEDS_HUMAN",
            "ENTER_HUMAN_BOUNDARY",
        ),
    ],
)
def test_recovery_evidence_selected_specialized_tactics_reach_handlers(
    facts: dict[str, object], expected_state: str, expected_reason: str
) -> None:
    view = default_view("RECOVERING", "RECOVERY_EVIDENCE")
    reduction = reduce(
        view,
        Trigger(kind="RECOVERY_EVIDENCE", trigger_id="re-specialized", facts=facts),
    )
    assert reduction.next_state == expected_state
    assert reduction.reason_code == expected_reason


def test_recovery_evidence_enter_human_boundary_records_reason_pointer() -> None:
    view = default_view("RECOVERING", "RECOVERY_EVIDENCE")
    boundary_id = "44444444-4444-4444-4444-444444444444"
    reduction = reduce(
        view,
        Trigger(
            kind="RECOVERY_EVIDENCE",
            trigger_id="re-boundary",
            facts={
                "source_kind": "CONTROLLER_OPERATION",
                "source_id": "cop-1",
                "category": "POLICY",
                "exhausted_autonomous": True,
                "human_boundary_reason": "SECURITY_POLICY_BOUNDARY",
                "selected_tactic": "ENTER_HUMAN_BOUNDARY",
                "pending_human_boundary_id": boundary_id,
            },
        ),
    )
    assert reduction.next_state == "NEEDS_HUMAN"
    assert reduction.pointer_updates["human_boundary_reason"] == "SECURITY_POLICY_BOUNDARY"
    assert reduction.pointer_updates["human_boundary_id"] == boundary_id
    assert reduction.pointer_updates["current_recovery_evidence_id"] == "re-boundary"


def test_reconciliation_ownership_conflict_records_human_boundary_pointer() -> None:
    view = default_view("PUBLISHING", "RECONCILIATION_FACT")
    boundary_id = "44444444-4444-4444-4444-444444444444"
    reduction = reduce(
        view,
        Trigger(
            kind="RECONCILIATION_FACT",
            trigger_id="reconcile-ownership",
            facts={
                "kind": "OWNERSHIP_CONFLICT",
                "pending_human_boundary_id": boundary_id,
            },
        ),
    )
    assert reduction.next_state == "NEEDS_HUMAN"
    assert reduction.pointer_updates["human_boundary_reason"] == "PUBLICATION_OWNERSHIP_CONFLICT"
    assert reduction.pointer_updates["human_boundary_id"] == boundary_id


def test_resolve_human_boundary_rejects_mismatched_boundary_id() -> None:
    view = default_view("NEEDS_HUMAN", "MANAGEMENT_COMMAND")
    reduction = reduce(
        view,
        Trigger(
            kind="MANAGEMENT_COMMAND",
            trigger_id="cmd-1",
            facts={"kind": "RESOLVE_HUMAN_BOUNDARY", "human_boundary_id": "not-the-current-one"},
        ),
    )
    assert reduction.kind == ReductionKind.STALE
    assert reduction.next_state == "NEEDS_HUMAN"


def test_resolve_human_boundary_rejects_missing_boundary_id() -> None:
    view = default_view("NEEDS_HUMAN", "MANAGEMENT_COMMAND")
    reduction = reduce(
        view,
        Trigger(
            kind="MANAGEMENT_COMMAND",
            trigger_id="cmd-1",
            facts={"kind": "RESOLVE_HUMAN_BOUNDARY"},
        ),
    )
    assert reduction.kind == ReductionKind.STALE


def test_resolve_human_boundary_clears_reason_pointer_on_success() -> None:
    view = default_view("NEEDS_HUMAN", "MANAGEMENT_COMMAND")
    reduction = reduce(
        view,
        Trigger(
            kind="MANAGEMENT_COMMAND",
            trigger_id="cmd-1",
            facts={
                "kind": "RESOLVE_HUMAN_BOUNDARY",
                "human_boundary_id": view.human_boundary_id,
            },
        ),
    )
    assert reduction.kind == ReductionKind.ADVANCE
    assert reduction.next_state == "RECOVERING"
    assert reduction.pointer_updates["human_boundary_id"] is None
    assert reduction.pointer_updates["human_boundary_reason"] is None


def test_secret_version_satisfies_boundary_requires_matching_boundary_id() -> None:
    view = default_view("NEEDS_HUMAN", "SECRET_VERSION")
    reduction = reduce(
        view,
        Trigger(
            kind="SECRET_VERSION",
            trigger_id="secret:1",
            facts={"satisfies_boundary": True, "human_boundary_id": "some-other-boundary"},
        ),
    )
    assert reduction.kind == ReductionKind.STALE


def test_storage_restoration_matches_object_requires_matching_boundary_id() -> None:
    view = default_view("NEEDS_HUMAN", "STORAGE_RESTORATION")
    reduction = reduce(
        view,
        Trigger(
            kind="STORAGE_RESTORATION",
            trigger_id="srf-1",
            facts={"matches_object": True, "human_boundary_id": "some-other-boundary"},
        ),
    )
    assert reduction.kind == ReductionKind.STALE
