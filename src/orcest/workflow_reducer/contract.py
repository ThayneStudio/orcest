"""Closed (state, trigger) contract generated from the v1 enum registry.

Every legal pair has exactly one default reduction. Every unlisted pair fails
closed. Additional named branches are extra fixtures, never a second default.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import Any

from orcest.workflow_contract.v1 import enums
from orcest.workflow_contract.v1.protocol_registry import DIAGNOSIS_PROTOCOL, PLAN_PROTOCOL
from orcest.workflow_reducer.graph import (
    ACTIVE_STATES,
    ALL_RUN_STATES,
    ALL_TRIGGER_KINDS,
    CONTROLLER_ACTIVITY_STATES,
    WORKER_ACTIVITY_STATES,
)
from orcest.workflow_reducer.types import ReductionKind, RunView, Trigger

__all__ = [
    "ContractCase",
    "default_view",
    "is_legal_pair",
    "iter_contract_cases",
    "iter_illegal_pairs",
    "legal_from_states",
    "legal_pairs",
]


def legal_from_states(trigger_kind: str) -> frozenset[str | None]:
    """States (or ``None`` for no Run) that may reduce ``trigger_kind``."""
    mapping: dict[str, frozenset[str | None]] = {
        "ADMIT": frozenset({None}),
        "INTERNAL": frozenset(ACTIVE_STATES | {"MERGED"}),
        "ATTEMPT_RESULT": frozenset(WORKER_ACTIVITY_STATES),
        "ATTEMPT_TERMINAL": frozenset(ALL_RUN_STATES),
        "CONTROLLER_OPERATION": frozenset(CONTROLLER_ACTIVITY_STATES),
        "FORGE_REQUEST_FAILURE": frozenset(ACTIVE_STATES),
        "FORGE_OBSERVATION": frozenset(ACTIVE_STATES | {"MERGED"}),
        "HEALTH_OBSERVATION": frozenset(ALL_RUN_STATES),
        "BUDGET_REPORT": frozenset(ALL_RUN_STATES),
        "MANAGEMENT_COMMAND": frozenset(ACTIVE_STATES),
        "POLICY_UPDATE": frozenset(ACTIVE_STATES),
        "PUBLICATION_CHECKPOINT": frozenset({"PUBLISHING"}),
        "RECONCILIATION_FACT": frozenset({"RECOVERING", "PUBLISHING", "PR_MONITORING"}),
        "RECOVERY_EVIDENCE": frozenset({"RECOVERING"}),
        "SECRET_VERSION": frozenset(ALL_RUN_STATES),
        "SPEC_SUPERSEDE": frozenset(ACTIVE_STATES),
        "STORAGE_RESTORATION": frozenset(ALL_RUN_STATES),
        "TIMER_FACT": frozenset(ALL_RUN_STATES),
    }
    try:
        return mapping[trigger_kind]
    except KeyError as exc:
        raise KeyError(f"unknown trigger kind {trigger_kind!r}") from exc


def legal_pairs() -> frozenset[tuple[str | None, str]]:
    pairs: set[tuple[str | None, str]] = set()
    for trigger in ALL_TRIGGER_KINDS:
        for state in legal_from_states(trigger):
            pairs.add((state, trigger))
    return frozenset(pairs)


def is_legal_pair(from_state: str | None, trigger_kind: str) -> bool:
    if trigger_kind not in ALL_TRIGGER_KINDS:
        return False
    return from_state in legal_from_states(trigger_kind)


@dataclass(frozen=True, slots=True)
class ContractCase:
    """One default (or extra) expected reduction for a legal pair."""

    case_id: str
    from_state: str | None
    trigger_kind: str
    expected_kind: ReductionKind
    expected_state: str
    expected_plan: str | None
    reason_code: str
    trigger_id: str
    facts: Mapping[str, Any]
    view_overrides: Mapping[str, Any] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.view_overrides is None:
            object.__setattr__(self, "view_overrides", {})


def default_view(state: str | None, trigger_kind: str, **overrides: Any) -> RunView:
    """Canonical durable view for the default fixture of ``(state, trigger)``."""
    kwargs: dict[str, Any] = {
        "run_id": None if state is None else "11111111-1111-1111-1111-111111111111",
        "project_id": "project-a",
        "work_item_key": "work-1",
        "state": state,
        "specification_generation": 0 if state in {None, "ADMITTED"} else 1,
        "generation_installed": state not in {None, "ADMITTED"},
        "initial_plan_absent": state in {None, "ADMITTED"},
        "safe_boundary": True,
        "offer_permitted": True,
        "next_transition_sequence": 2 if state is not None else 1,
        "pending_internal_sequence": 1 if trigger_kind == "INTERNAL" else None,
    }
    if trigger_kind == "SPEC_SUPERSEDE":
        kwargs.update(pending_snapshot_id="22222222-2222-2222-2222-222222222222")
        if state == "ADMITTED":
            kwargs.update(generation_installed=False, specification_generation=0)
    if state == "ADMITTED" and trigger_kind == "INTERNAL":
        kwargs.update(
            pending_snapshot_id=None,
            generation_installed=True,
            specification_generation=1,
            current_snapshot_id="22222222-2222-2222-2222-222222222222",
            initial_plan_absent=True,
        )
    if state == "RECOVERING":
        kwargs["recovery_origin_state"] = "PLANNING"
        kwargs["recovery_tactic"] = "RETRY_EXECUTION"
    if state == "WAITING":
        kwargs["wait_condition_id"] = "33333333-3333-3333-3333-333333333333"
        kwargs["wait_reason"] = "CAPACITY"
        kwargs["recovery_origin_state"] = "PLANNING"
    if state == "NEEDS_HUMAN":
        kwargs["human_boundary_id"] = "44444444-4444-4444-4444-444444444444"
        kwargs["human_boundary_reason"] = "MISSING_AUTHORITY"
        kwargs["recovery_origin_state"] = "PLANNING"
    if state in {"PUBLISHING", "PR_MONITORING", "PR_REMEDIATING", "MERGED"}:
        kwargs["publication_id"] = "55555555-5555-5555-5555-555555555555"
        kwargs["publication_state"] = (
            "ACTIVE" if state in {"PR_MONITORING", "PR_REMEDIATING", "MERGED"} else "PLANNED"
        )
    if state == "MERGED":
        kwargs["terminal_duplicate_cleanup_active"] = True
        kwargs["pending_internal_sequence"] = 1 if trigger_kind == "INTERNAL" else None
    if state in {"REVIEWING", "ADJUDICATING"}:
        kwargs["unfilled_review_slots"] = ("default",)
        kwargs["panel_complete"] = trigger_kind == "ATTEMPT_RESULT"
        if trigger_kind == "ATTEMPT_RESULT":
            kwargs["unfilled_review_slots"] = ()
            kwargs["filling_review_slots"] = ("slot-a", "slot-b")
    if state == "APPROVED" and trigger_kind == "INTERNAL":
        kwargs["current_candidate_id"] = "66666666-6666-6666-6666-666666666666"
        kwargs["publication_id"] = None
    if state == "REPLANNING":
        kwargs["current_snapshot_id"] = "22222222-2222-2222-2222-222222222222"
    kwargs.update(overrides)
    return RunView(**kwargs)


def _default_facts(state: str | None, trigger: str) -> tuple[str, dict[str, Any]]:
    """Return (trigger_id, facts) for the default fixture of a legal pair."""
    if trigger == "ADMIT":
        return (
            "obs-work-1",
            {
                "snapshot_id": "22222222-2222-2222-2222-222222222222",
                "base_observation_id": "obs-base-1",
                "project_id": "project-a",
                "work_item_key": "work-1",
            },
        )
    if trigger == "SPEC_SUPERSEDE":
        return ("22222222-2222-2222-2222-222222222222", {"install": True})
    if trigger == "INTERNAL":
        return ("1", {})
    if trigger == "ATTEMPT_RESULT":
        kind = {
            "PLANNING": "PLAN",
            "BUILDING": "BUILD",
            "VERIFYING": "VERIFY",
            "REVIEWING": "REVIEW",
            "REMEDIATING": "REMEDIATE",
            "DIAGNOSING": "DIAGNOSE",
            "REPLANNING": "REPLAN",
            "ADJUDICATING": "ADJUDICATE",
            "PR_REMEDIATING": "PR_REMEDIATE",
            "RECOVERING": "PLAN",
        }[state or ""]
        facts: dict[str, Any] = {
            "outcome": "SUCCEEDED",
            "activity_kind": kind,
            "attempt_state": "CLAIMED",
        }
        if state == "VERIFYING":
            facts["verification_outcome"] = "PASS"
            facts["staffable"] = True
        if state == "REVIEWING":
            facts["fills_slot"] = True
            facts["panel_complete"] = True
        if state == "ADJUDICATING":
            facts["disposition"] = "SUSTAIN"
            facts["below_diagnosis_threshold"] = True
        if state == "DIAGNOSING":
            facts["plan_assessment"] = "VIABLE"
            facts["below_replan_threshold"] = True
            facts["structured_output_protocol"] = DIAGNOSIS_PROTOCOL
        if state in {"PLANNING", "REPLANNING"}:
            facts["structured_output_protocol"] = PLAN_PROTOCOL
        if state in {"BUILDING", "REMEDIATING", "PR_REMEDIATING"}:
            facts["candidate_id"] = "66666666-6666-4666-8666-666666666666"
        if state == "RECOVERING":
            facts["outcome"] = "FAILED_RETRYABLE"
        return ("attempt-1", facts)
    if trigger == "ATTEMPT_TERMINAL":
        if state in WORKER_ACTIVITY_STATES:
            return (
                "term-1",
                {
                    "kind": "EXECUTION_DEADLINE",
                    "attempt_state": "CLAIMED",
                    "already_terminal": False,
                },
            )
        return ("term-1", {"kind": "RESULT_AFTER_TERMINAL", "already_terminal": True})
    if trigger == "CONTROLLER_OPERATION":
        if state in {"REMEDIATING", "PR_REMEDIATING"}:
            return ("cop-1", {"outcome": "SUCCEEDED", "activity_kind": "IMPORT"})
        return ("cop-1", {"outcome": "FAILED", "failure_category": "SOURCE_READ"})
    if trigger == "FORGE_REQUEST_FAILURE":
        return ("frf-1", {"failure_kind": "TIMEOUT", "request_pending": True})
    if trigger == "FORGE_OBSERVATION":
        facts = {"kind": "WORK_ITEM_SNAPSHOT", "supersession_key_changed": False}
        if state == "MERGED":
            facts = {"kind": "CHANGE_REQUEST_CLOSED", "cleanup_action": "CLOSE"}
        if state in {"PUBLISHING", "PR_MONITORING"}:
            facts = {"kind": "CHANGE_REQUEST_HEAD", "same_state": True}
        return ("obs-1", facts)
    if trigger == "HEALTH_OBSERVATION":
        return ("health-1", {"kind": "AVAILABLE", "wakes_wait": state == "WAITING"})
    if trigger == "BUDGET_REPORT":
        return ("budget-1", {"availability": "AVAILABLE", "wakes_wait": state == "WAITING"})
    if trigger == "MANAGEMENT_COMMAND":
        return ("cmd-1", {"kind": "CANCEL"})
    if trigger == "POLICY_UPDATE":
        return ("pol-1", {"supersession_key_changed": True})
    if trigger == "PUBLICATION_CHECKPOINT":
        return ("ckpt-1", {"status": "AMBIGUOUS"})
    if trigger == "RECONCILIATION_FACT":
        return ("recon-1", {"kind": "EFFECT_PRESENT"})
    if trigger == "RECOVERY_EVIDENCE":
        return (
            "re-1",
            {
                "source_kind": "ATTEMPT_RESULT",
                "source_id": "attempt-1",
                "category": "WORKER_LOST",
                "selected_tactic": "RETRY_EXECUTION",
                "next_eligible_at_ms": None,
            },
        )
    if trigger == "SECRET_VERSION":
        return ("secret:1", {"satisfies_boundary": state == "NEEDS_HUMAN"})
    if trigger == "STORAGE_RESTORATION":
        return (
            "srf-1",
            {"matches_object": state == "NEEDS_HUMAN", "stale_member": state != "NEEDS_HUMAN"},
        )
    if trigger == "TIMER_FACT":
        return (
            "timer-1",
            {
                "scope_kind": "WAIT_CONDITION_NOT_BEFORE"
                if state == "WAITING"
                else "HEALTH_OBSERVATION_EXPIRY",
                "wait_current": state == "WAITING",
            },
        )
    raise AssertionError(f"no default facts for {trigger}")


def _default_expected(
    state: str | None, trigger: str
) -> tuple[ReductionKind, str, str | None, str]:
    """Return (kind, next_state, plan_kind, reason_code) for the default fixture."""
    if trigger == "ADMIT":
        return ReductionKind.ADVANCE, "ADMITTED", None, "ADMIT"
    if trigger == "SPEC_SUPERSEDE":
        if state == "ADMITTED":
            return ReductionKind.ADVANCE, "ADMITTED", None, "INSTALL_GENERATION"
        return ReductionKind.ADVANCE, "REPLANNING", "REPLAN", "SPEC_SUPERSEDE"
    if trigger == "INTERNAL":
        if state == "ADMITTED":
            return ReductionKind.ADVANCE, "PLANNING", "PLAN", "INITIAL_PLAN"
        if state == "AGGREGATING":
            return ReductionKind.ADVANCE, "APPROVED", None, "CONSENSUS_APPROVED"
        if state == "APPROVED":
            return ReductionKind.ADVANCE, "PUBLISHING", "PUBLISH", "INITIAL_PUBLISH"
        if state == "MERGED":
            return ReductionKind.SAME_STATE_AUDIT, "MERGED", None, "CLEANUP_CONTINUATION"
        if state in {"REVIEWING", "ADJUDICATING"}:
            return ReductionKind.SAME_STATE_AUDIT, state, None, "STAFFING_OR_STATE"
        return ReductionKind.SAME_STATE_AUDIT, state or "ADMITTED", None, "INTERNAL_CONTINUATION"
    if trigger == "ATTEMPT_RESULT":
        success = {
            "PLANNING": (ReductionKind.ADVANCE, "BUILDING", "BUILD", "PLAN_ACCEPTED"),
            "BUILDING": (ReductionKind.ADVANCE, "VERIFYING", "VERIFY", "CANDIDATE_SELECTED"),
            "VERIFYING": (ReductionKind.ADVANCE, "REVIEWING", None, "VERIFY_PASS"),
            "REVIEWING": (ReductionKind.ADVANCE, "AGGREGATING", None, "PANEL_COMPLETE"),
            "REMEDIATING": (ReductionKind.ADVANCE, "VERIFYING", "VERIFY", "CANDIDATE_SELECTED"),
            "DIAGNOSING": (ReductionKind.ADVANCE, "RECOVERING", None, "DIAGNOSIS_ACCEPTED"),
            "REPLANNING": (ReductionKind.ADVANCE, "BUILDING", "BUILD", "REPLAN_ACCEPTED"),
            "ADJUDICATING": (
                ReductionKind.ADVANCE,
                "REMEDIATING",
                "REMEDIATE",
                "ADJUDICATION_SUSTAIN",
            ),
            "PR_REMEDIATING": (ReductionKind.ADVANCE, "VERIFYING", "VERIFY", "CANDIDATE_SELECTED"),
            "RECOVERING": (ReductionKind.ADVANCE, "RECOVERING", None, "RECOVERABLE_FAILURE"),
        }
        return success[state or ""]
    if trigger == "ATTEMPT_TERMINAL":
        if state in WORKER_ACTIVITY_STATES:
            return ReductionKind.ADVANCE, "RECOVERING", None, "ATTEMPT_TERMINAL"
        return ReductionKind.SAME_STATE_AUDIT, state or "ADMITTED", None, "ALREADY_TERMINAL_AUDIT"
    if trigger == "CONTROLLER_OPERATION":
        if state in {"PR_REMEDIATING", "REMEDIATING"}:
            return ReductionKind.ADVANCE, "VERIFYING", "VERIFY", "IMPORT_ACCEPTED"
        return ReductionKind.ADVANCE, "RECOVERING", None, "CONTROLLER_OPERATION_FAILED"
    if trigger == "FORGE_REQUEST_FAILURE":
        return ReductionKind.ADVANCE, "RECOVERING", None, "FORGE_TRANSIENT"
    if trigger == "FORGE_OBSERVATION":
        if state == "MERGED":
            return ReductionKind.SAME_STATE_AUDIT, "MERGED", None, "CLEANUP_OBSERVATION"
        return ReductionKind.SAME_STATE_AUDIT, state or "ADMITTED", None, "NO_OP_AUDIT"
    if trigger == "HEALTH_OBSERVATION":
        if state == "WAITING":
            return ReductionKind.ADVANCE, "RECOVERING", None, "WAIT_WAKE"
        return ReductionKind.STALE, state or "ADMITTED", None, "STALE_FANOUT"
    if trigger == "BUDGET_REPORT":
        if state == "WAITING":
            return ReductionKind.ADVANCE, "RECOVERING", None, "WAIT_WAKE"
        return ReductionKind.STALE, state or "ADMITTED", None, "STALE_FANOUT"
    if trigger == "MANAGEMENT_COMMAND":
        if state in {"PR_MONITORING", "PR_REMEDIATING"}:
            return ReductionKind.ADVANCE, "PR_MONITORING", "CLOSE_PUBLICATION", "CANCEL_RECONCILE"
        return ReductionKind.ADVANCE, "CANCELLED", None, "CANCEL"
    if trigger == "POLICY_UPDATE":
        return ReductionKind.SAME_STATE_AUDIT, state or "ADMITTED", None, "SNAPSHOT_CAPTURE"
    if trigger == "PUBLICATION_CHECKPOINT":
        return ReductionKind.SAME_STATE_AUDIT, "PUBLISHING", None, "AMBIGUOUS_CHECKPOINT"
    if trigger == "RECONCILIATION_FACT":
        if state == "RECOVERING":
            return ReductionKind.ADVANCE, "PLANNING", None, "EFFECT_PRESENT"
        return ReductionKind.SAME_STATE_AUDIT, state or "ADMITTED", None, "RECONCILE"
    if trigger == "RECOVERY_EVIDENCE":
        return ReductionKind.ADVANCE, "PLANNING", None, "RETRY_EXECUTION"
    if trigger == "SECRET_VERSION":
        if state == "NEEDS_HUMAN":
            return ReductionKind.ADVANCE, "RECOVERING", None, "HUMAN_RESOLUTION"
        return ReductionKind.STALE, state or "ADMITTED", None, "STALE_FANOUT"
    if trigger == "STORAGE_RESTORATION":
        if state == "NEEDS_HUMAN":
            return ReductionKind.ADVANCE, "RECOVERING", None, "HUMAN_RESOLUTION"
        return ReductionKind.STALE, state or "ADMITTED", None, "STALE_FANOUT"
    if trigger == "TIMER_FACT":
        if state == "WAITING":
            return ReductionKind.ADVANCE, "RECOVERING", None, "WAIT_WAKE"
        return ReductionKind.STALE, state or "ADMITTED", None, "STALE_FANOUT"
    raise AssertionError(f"no default expected for {state} {trigger}")


def iter_contract_cases() -> Iterator[ContractCase]:
    """Yield the one default case for every legal (state, trigger) pair."""
    missing_triggers = ALL_TRIGGER_KINDS - {member.value for member in enums.TransitionTrigger}
    if missing_triggers:
        raise RuntimeError(f"contract missing triggers {sorted(missing_triggers)!r}")
    for state, trigger in sorted(legal_pairs(), key=lambda item: (item[0] or "", item[1])):
        trigger_id, facts = _default_facts(state, trigger)
        kind, next_state, plan, reason = _default_expected(state, trigger)
        yield ContractCase(
            case_id=f"{state or 'NONE'}.{trigger}.default",
            from_state=state,
            trigger_kind=trigger,
            expected_kind=kind,
            expected_state=next_state,
            expected_plan=plan,
            reason_code=reason,
            trigger_id=trigger_id,
            facts=facts,
        )


def iter_illegal_pairs() -> Iterator[tuple[str | None, str]]:
    """Yield every unlisted (state, trigger) pair, including no-Run rows."""
    states: tuple[str | None, ...] = (None, *sorted(ALL_RUN_STATES))
    for state in states:
        for trigger in sorted(ALL_TRIGGER_KINDS):
            if not is_legal_pair(state, trigger):
                yield state, trigger


def trigger_for_case(case: ContractCase) -> Trigger:
    return Trigger(kind=case.trigger_kind, trigger_id=case.trigger_id, facts=case.facts)


def view_for_case(case: ContractCase) -> RunView:
    return default_view(case.from_state, case.trigger_kind, **dict(case.view_overrides))
