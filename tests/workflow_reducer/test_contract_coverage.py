"""Exhaustive (state, trigger) coverage generated from the v1 enum registry."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from orcest.workflow_contract.v1 import enums
from orcest.workflow_reducer.contract import (
    default_view,
    is_legal_pair,
    iter_contract_cases,
    iter_illegal_pairs,
    legal_pairs,
    trigger_for_case,
    view_for_case,
)
from orcest.workflow_reducer.reduce import reduce
from orcest.workflow_reducer.types import IllegalTransitionError

pytestmark = pytest.mark.unit

_REDUCER_ROOT = Path(__file__).resolve().parents[2] / "src" / "orcest" / "workflow_reducer"
_FORBIDDEN_MODULES = frozenset(
    {"redis", "socket", "http.client", "httpx", "requests", "urllib.request", "urllib3"}
)


def test_contract_cases_cover_every_legal_pair_exactly_once() -> None:
    cases = list(iter_contract_cases())
    pairs = [(case.from_state, case.trigger_kind) for case in cases]
    assert len(pairs) == len(set(pairs))
    assert set(pairs) == legal_pairs()


def test_legal_pairs_are_closed_over_the_enum_registry() -> None:
    triggers = {member.value for member in enums.TransitionTrigger}
    states = {member.value for member in enums.RunState}
    seen_triggers = {trigger for _, trigger in legal_pairs()}
    assert seen_triggers == triggers
    for state, trigger in legal_pairs():
        if state is not None:
            assert state in states


@pytest.mark.parametrize("case", list(iter_contract_cases()), ids=lambda case: case.case_id)
def test_every_legal_pair_has_one_expected_reduction(case: object) -> None:
    from orcest.workflow_reducer.contract import ContractCase

    assert isinstance(case, ContractCase)
    view = view_for_case(case)
    trigger = trigger_for_case(case)
    reduction = reduce(view, trigger)
    assert reduction.kind is case.expected_kind
    assert reduction.next_state == case.expected_state
    assert reduction.reason_code == case.reason_code
    planned = tuple(activity.kind for activity in reduction.planned_activities)
    if case.expected_plan is not None:
        assert case.expected_plan in planned


_ILLEGAL_PAIRS = list(iter_illegal_pairs())


@pytest.mark.parametrize(
    "pair",
    _ILLEGAL_PAIRS,
    ids=[f"{state or 'NONE'}.{trigger}" for state, trigger in _ILLEGAL_PAIRS],
)
def test_every_unlisted_pair_fails_closed(pair: tuple[str | None, str]) -> None:
    from_state, trigger_kind = pair
    view = default_view(from_state, trigger_kind)
    from orcest.workflow_reducer.types import Trigger

    trigger = Trigger(kind=trigger_kind, trigger_id="unlisted-1", facts={})
    with pytest.raises(IllegalTransitionError, match="fails closed|unknown trigger|no matching"):
        reduce(view, trigger)


def test_unknown_trigger_kind_fails_closed() -> None:
    view = default_view("PLANNING", "INTERNAL")
    from orcest.workflow_reducer.types import Trigger

    trigger = Trigger(kind="NOT_A_TRIGGER", trigger_id="x", facts={})
    with pytest.raises(IllegalTransitionError):
        reduce(view, trigger)


def test_reducer_package_has_no_network_or_redis_imports() -> None:
    found: list[str] = []
    for path in _REDUCER_ROOT.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            names: list[str] = []
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                names = [node.module]
            for name in names:
                root = name.split(".")[0]
                if name in _FORBIDDEN_MODULES or root in _FORBIDDEN_MODULES:
                    found.append(f"{path.name}:{name}")
    assert found == []


def test_is_legal_pair_matches_generated_set() -> None:
    assert is_legal_pair(None, "ADMIT")
    assert not is_legal_pair("PLANNING", "ADMIT")
    assert not is_legal_pair(None, "INTERNAL")
    assert is_legal_pair("PLANNING", "ATTEMPT_RESULT")
    assert not is_legal_pair("ADMITTED", "ATTEMPT_RESULT")
    assert not is_legal_pair("CLOSED", "FORGE_OBSERVATION")
    assert is_legal_pair("MERGED", "FORGE_OBSERVATION")
