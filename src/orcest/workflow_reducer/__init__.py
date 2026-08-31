"""Pure Workflow-Control v1 reducer and transition ledger."""

from orcest.workflow_reducer.continuation import (
    ContinuationWinner,
    arbitrate_internal_continuation,
)
from orcest.workflow_reducer.contract import (
    ContractCase,
    is_legal_pair,
    iter_contract_cases,
    iter_illegal_pairs,
    legal_pairs,
)
from orcest.workflow_reducer.ledger import apply, load_view
from orcest.workflow_reducer.reduce import reduce
from orcest.workflow_reducer.types import (
    PRIOR_STATE_NONE,
    AppliedReduction,
    IllegalTransitionError,
    Reduction,
    ReductionKind,
    RunView,
    Trigger,
)

__all__ = [
    "PRIOR_STATE_NONE",
    "AppliedReduction",
    "ContinuationWinner",
    "ContractCase",
    "IllegalTransitionError",
    "Reduction",
    "ReductionKind",
    "RunView",
    "Trigger",
    "apply",
    "arbitrate_internal_continuation",
    "is_legal_pair",
    "iter_contract_cases",
    "iter_illegal_pairs",
    "legal_pairs",
    "load_view",
    "reduce",
]
