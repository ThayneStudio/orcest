"""Closed Run state graph derived from the v1 enum registry."""

from __future__ import annotations

from orcest.workflow_contract.v1 import enums

__all__ = [
    "ACTIVITY_OWNING_STATES",
    "WORKER_ACTIVITY_STATES",
    "ACTIVE_STATES",
    "ALL_RUN_STATES",
    "ALL_TRIGGER_KINDS",
    "CONTROLLER_ACTIVITY_STATES",
    "PANEL_STATES",
    "TERMINAL_STATES",
    "activity_execution_class",
    "is_terminal_state",
]


ALL_RUN_STATES = frozenset(member.value for member in enums.RunState)
ALL_TRIGGER_KINDS = frozenset(member.value for member in enums.TransitionTrigger)
TERMINAL_STATES = frozenset(member.value for member in enums.RUN_TERMINAL_STATES)
ACTIVE_STATES = ALL_RUN_STATES - TERMINAL_STATES
PANEL_STATES = frozenset({"REVIEWING", "ADJUDICATING"})
WORKER_ACTIVITY_STATES = frozenset(
    {
        "PLANNING",
        "BUILDING",
        "VERIFYING",
        "REVIEWING",
        "REMEDIATING",
        "DIAGNOSING",
        "REPLANNING",
        "ADJUDICATING",
        "PR_REMEDIATING",
        "RECOVERING",
    }
)
ACTIVITY_OWNING_STATES = WORKER_ACTIVITY_STATES | frozenset({"PUBLISHING", "PR_MONITORING"})
CONTROLLER_ACTIVITY_STATES = frozenset(
    {
        "REMEDIATING",
        "PUBLISHING",
        "PR_MONITORING",
        "PR_REMEDIATING",
        "RECOVERING",
    }
)


def is_terminal_state(state: str | None) -> bool:
    return state in TERMINAL_STATES


def activity_execution_class(kind: str) -> str:
    parsed = enums.parse_enum("activity.kind", kind)
    if parsed in enums.CONTROLLER_ACTIVITY_KINDS:
        return enums.ActivityExecutionClass.CONTROLLER.value
    return enums.ActivityExecutionClass.WORKER.value
