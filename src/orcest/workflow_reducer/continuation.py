"""Internal continuation arbitration (workflow-lifecycle.md)."""

from __future__ import annotations

from enum import Enum

from orcest.workflow_reducer.types import RunView

__all__ = ["ContinuationWinner", "arbitrate_internal_continuation"]


class ContinuationWinner(str, Enum):
    CANCELLATION = "CANCELLATION"
    SPEC_SUPERSEDE = "SPEC_SUPERSEDE"
    DEPENDENCY = "DEPENDENCY"
    STAFFING = "STAFFING"
    STATE_SPECIFIC = "STATE_SPECIFIC"


def arbitrate_internal_continuation(view: RunView, entering_sequence: int) -> ContinuationWinner:
    """Return the single INTERNAL branch owned by ``entering_sequence``.

    Precedence is closed: cancellation, pending Snapshot, pending dependency,
    panel staffing, then the one state-specific continuation. Recovery work is
    never an INTERNAL continuation.
    """
    if view.cancellation_pending:
        return ContinuationWinner.CANCELLATION
    if view.pending_snapshot_id is not None:
        return ContinuationWinner.SPEC_SUPERSEDE
    if view.pending_dependency_observation_id is not None and view.safe_boundary:
        return ContinuationWinner.DEPENDENCY
    if (
        view.latest_staffing_recheck_transition_sequence == entering_sequence
        and not view.claimed_unfilled_peer
    ):
        return ContinuationWinner.STAFFING
    return ContinuationWinner.STATE_SPECIFIC
