"""Recovery Evidence tactic selection for Workflow-Control v1."""

from __future__ import annotations

import pytest

from orcest.workflow_reducer.recovery import (
    HealthObservationRef,
    RecoveryEvidenceInput,
    RecoveryLimits,
    classify_recovery_category,
    select_recovery_decision,
)

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    ("source_kind", "facts", "category", "tactic"),
    [
        ("ATTEMPT_RESULT", {"failure_class": "INFRASTRUCTURE"}, "WORKER_LOST", "RETRY_EXECUTION"),
        (
            "ATTEMPT_RESULT",
            {"failure_class": "PROVIDER_RATE_LIMIT"},
            "PROVIDER_RATE_LIMIT",
            "WAIT_RATE_LIMIT",
        ),
        (
            "ATTEMPT_RESULT",
            {"failure_class": "INVALID_AGENT_OUTPUT"},
            "INVALID_RESULT",
            "REPAIR_SCHEMA",
        ),
        ("ATTEMPT_RESULT", {"verification_outcome": "FAIL"}, "VERIFICATION_FAILURE", "ADJUDICATE"),
        ("ATTEMPT_TERMINAL", {"kind": "EXECUTION_DEADLINE"}, "TIMEOUT", "RETRY_EXECUTION"),
        ("CONTROLLER_OPERATION", {"failure_category": "STORAGE"}, "STORAGE", "WAIT_EXTERNAL"),
        ("FORGE_REQUEST_FAILURE", {"failure_kind": "TIMEOUT"}, "FORGE_TRANSIENT", "WAIT_EXTERNAL"),
        (
            "HEALTH_OBSERVATION",
            {"kind": "RATE_LIMITED"},
            "PROVIDER_RATE_LIMIT",
            "WAIT_RATE_LIMIT",
        ),
        ("HEALTH_OBSERVATION", {"kind": "UNAVAILABLE"}, "CAPACITY", "WAIT_CAPACITY"),
        ("FORGE_OBSERVATION", {"kind": "DEPENDENCY_STATE"}, "EXTERNAL_DEPENDENCY", "WAIT_EXTERNAL"),
        ("BUDGET_REPORT", {"availability": "EXHAUSTED"}, "BUDGET", "WAIT_BUDGET"),
        ("RECONCILIATION_FACT", {"kind": "OWNERSHIP_CONFLICT"}, "POLICY", "DIAGNOSE"),
        ("RECOVERY_EVIDENCE", {}, "REPEATED_NON_PROGRESS", "DIAGNOSE"),
        ("POLICY_UPDATE", {}, "POLICY", "DIAGNOSE"),
        (
            "STORAGE_RESTORATION",
            {"matches_object": False},
            "INTEGRITY_SUSPECTED",
            "PROBE_INTEGRITY",
        ),
        ("SECRET_VERSION", {}, "CREDENTIAL", "WAIT_EXTERNAL"),
        ("MANAGEMENT_COMMAND", {}, "POLICY", "DIAGNOSE"),
        ("TIMER_FACT", {"scope_kind": "BUDGET_REPORT_EXPIRY"}, "BUDGET", "WAIT_BUDGET"),
        ("INTERNAL", {}, "EXTERNAL_DEPENDENCY", "WAIT_EXTERNAL"),
    ],
)
def test_accepted_failure_sources_map_to_one_tactic(
    source_kind: str, facts: dict[str, object], category: str, tactic: str
) -> None:
    assert classify_recovery_category(source_kind, facts) == category
    decision = select_recovery_decision(
        RecoveryEvidenceInput(source_kind=source_kind, source_id="source-1", category=category)
    )
    assert decision.selected_tactic == tactic


def test_source_category_mismatch_fails_closed() -> None:
    with pytest.raises(ValueError, match="cannot source recovery category"):
        select_recovery_decision(
            RecoveryEvidenceInput(
                source_kind="BUDGET_REPORT",
                source_id="budget-1",
                category="WORKER_LOST",
            )
        )


@pytest.mark.parametrize(
    ("source_kind", "facts"),
    [
        ("FORGE_REQUEST_FAILURE", {"failure_kind": "AUTHORIZATION"}),
        ("HEALTH_OBSERVATION", {"kind": "RECOVERED"}),
        ("TIMER_FACT", {"scope_kind": "UNKNOWN"}),
    ],
)
def test_unmapped_failure_facts_fail_closed(source_kind: str, facts: dict[str, object]) -> None:
    with pytest.raises(ValueError, match="facts do not map"):
        classify_recovery_category(source_kind, facts)


def test_health_membership_order_cannot_change_decision_or_digest() -> None:
    evidence = RecoveryEvidenceInput(
        source_kind="ATTEMPT_RESULT",
        source_id="attempt-1",
        category="PROVIDER_RATE_LIMIT",
        fallback_order=("provider-b/account-2",),
    )
    health = (
        HealthObservationRef("health-b", "PROVIDER_ACCOUNT", "provider-b/account-2", 2),
        HealthObservationRef("health-a", "PROVIDER_ACCOUNT", "provider-a/account-1", 3),
    )
    left = select_recovery_decision(evidence, health_observations=health)
    right = select_recovery_decision(evidence, health_observations=tuple(reversed(health)))
    assert left.selected_tactic == right.selected_tactic == "REPLACE_CAPACITY"
    assert left.selected_fallback == right.selected_fallback == "provider-b/account-2"
    assert left.ordered_health_observation_ids == right.ordered_health_observation_ids
    assert left.health_observation_ids_digest == right.health_observation_ids_digest


def test_repeated_non_progress_reaches_diagnosis_then_replan_boundary() -> None:
    limits = RecoveryLimits(max_diagnoses_before_replan=2)
    diagnose = select_recovery_decision(
        RecoveryEvidenceInput(
            source_kind="RECOVERY_EVIDENCE",
            source_id="recovery-1",
            category="REPEATED_NON_PROGRESS",
            prior_diagnosis_count=1,
        ),
        limits=limits,
    )
    replan = select_recovery_decision(
        RecoveryEvidenceInput(
            source_kind="RECOVERY_EVIDENCE",
            source_id="recovery-2",
            category="REPEATED_NON_PROGRESS",
            prior_diagnosis_count=2,
        ),
        limits=limits,
    )
    assert diagnose.selected_tactic == "DIAGNOSE"
    assert diagnose.diagnosis_count == 2
    assert replan.selected_tactic == "REPLAN"


def test_repeated_repair_fingerprint_reaches_diagnosis_boundary() -> None:
    decision = select_recovery_decision(
        RecoveryEvidenceInput(
            source_kind="ATTEMPT_RESULT",
            source_id="verify-1",
            category="VERIFICATION_FAILURE",
            prior_repair_cycle_count=3,
        ),
        limits=RecoveryLimits(max_repair_cycles_before_diagnosis=4),
    )
    assert decision.selected_tactic == "DIAGNOSE"
    assert decision.repair_cycle_count == 4
