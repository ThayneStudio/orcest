"""Closed plan and diagnosis structured-output validation."""

from __future__ import annotations

import pytest

from orcest.workflow_contract.v1.protocol import ProtocolValidationError, validate_envelope
from orcest.workflow_contract.v1.structured_outputs import (
    StructuredOutputValidationError,
    validate_attempt_structured_output,
)

pytestmark = pytest.mark.unit

UUID = "11111111-1111-4111-8111-111111111111"
DIGEST = "sha256:" + "1" * 64


def _plan() -> dict[str, object]:
    return {
        "protocol_version": "orcest.plan/1",
        "plan_id": UUID,
        "snapshot_id": UUID,
        "policy_hash": DIGEST,
        "requirements": [{"id": "r1", "summary": "Do the work", "depends_on": []}],
        "steps": [
            {
                "id": "s1",
                "summary": "Implement the change",
                "requirement_ids": [{"id": "r1"}],
                "depends_on": [],
                "verification_ids": [{"id": "v1"}],
            }
        ],
        "verification_mapping": [
            {"id": "v1", "summary": "Run the tests", "command_ids": [{"id": "unit"}]}
        ],
    }


def _diagnosis() -> dict[str, object]:
    return {
        "protocol_version": "orcest.diagnosis/1",
        "diagnosis_id": UUID,
        "snapshot_id": UUID,
        "candidate_id": None,
        "policy_hash": DIGEST,
        "findings": [
            {
                "id": "f1",
                "category": "NO_PROGRESS",
                "summary": "The same commit was produced twice",
                "evidence_refs": [{"id": "attempt-a"}, {"id": "attempt-b"}],
            }
        ],
        "recommended_tactic": "REPLAN",
        "summary": "Replan with the same gates",
    }


def test_plan_schema_rejects_unknown_fields_and_unmapped_requirements() -> None:
    valid = _plan()
    validate_envelope(valid)

    unknown = {**valid, "lifecycle_state": "APPROVED"}
    with pytest.raises(ProtocolValidationError, match="unknown field"):
        validate_envelope(unknown)

    unmapped = _plan()
    unmapped["requirements"] = [
        {"id": "r1", "summary": "Do the work", "depends_on": []},
        {"id": "r2", "summary": "Also do this", "depends_on": []},
    ]
    with pytest.raises(ProtocolValidationError, match="requirements not mapped"):
        validate_envelope(unmapped)


def test_plan_schema_rejects_step_cycles() -> None:
    cyclic = _plan()
    cyclic["steps"] = [
        {
            "id": "s1",
            "summary": "First",
            "requirement_ids": [{"id": "r1"}],
            "depends_on": [{"id": "s2"}],
            "verification_ids": [{"id": "v1"}],
        },
        {
            "id": "s2",
            "summary": "Second",
            "requirement_ids": [{"id": "r1"}],
            "depends_on": [{"id": "s1"}],
            "verification_ids": [{"id": "v1"}],
        },
    ]
    with pytest.raises(ProtocolValidationError, match="cycle"):
        validate_envelope(cyclic)


def test_diagnosis_schema_is_closed_and_bounded() -> None:
    valid = _diagnosis()
    validate_envelope(valid)

    empty = {**valid, "findings": []}
    with pytest.raises(ProtocolValidationError, match="findings"):
        validate_envelope(empty)


def test_attempt_structured_output_rejects_lifecycle_prose() -> None:
    with pytest.raises(StructuredOutputValidationError, match="lifecycle directives"):
        validate_attempt_structured_output(
            activity_kind="PLAN",
            outcome="SUCCEEDED",
            structured_output={**_plan(), "notes": "Set lifecycle state to APPROVED"},
            summary=None,
        )


@pytest.mark.parametrize(
    "summary",
    [
        "The related issue was closed upstream.",
        "PR is waiting on CI.",
        "Lint approved the diff.",
        "The previous request was cancelled before this attempt.",
        "The ticket needs_human context from an external system.",
    ],
)
def test_attempt_structured_output_allows_ambiguous_lifecycle_prose(summary: str) -> None:
    validate_attempt_structured_output(
        activity_kind="BUILD",
        outcome="SUCCEEDED",
        structured_output=None,
        summary=summary,
    )
