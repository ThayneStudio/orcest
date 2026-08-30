"""Golden envelope fixtures: normative JSON examples from the wiki round-trip.

Issue #668 acceptance criterion: "Check in golden fixtures for every
normative JSON/YAML example ... Every normative enum and tagged union
round-trips through one registry ... Unknown versions, fields, enum values,
and ambiguous nullability fail closed."
"""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import pytest

from orcest.workflow_contract.v1.protocol import ProtocolValidationError, validate_envelope

FIXTURE_PATH = Path(__file__).parent / "golden" / "envelope_examples.json"


def _load_examples() -> list[dict[str, Any]]:
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


@pytest.mark.parametrize("example", _load_examples(), ids=lambda e: e["name"])
def test_golden_envelope_validates(example: dict[str, Any]) -> None:
    validate_envelope(example["body"])


@pytest.mark.parametrize("example", _load_examples(), ids=lambda e: e["name"])
def test_golden_envelope_rejects_unknown_field(example: dict[str, Any]) -> None:
    mutated = copy.deepcopy(example["body"])
    mutated["__shadow_field__"] = "unexpected"
    with pytest.raises(ProtocolValidationError):
        validate_envelope(mutated)


@pytest.mark.parametrize("example", _load_examples(), ids=lambda e: e["name"])
def test_golden_envelope_rejects_unknown_protocol_version(example: dict[str, Any]) -> None:
    mutated = copy.deepcopy(example["body"])
    key = "protocol_version" if "protocol_version" in mutated else "protocol"
    mutated[key] = mutated[key] + "999"
    with pytest.raises(ProtocolValidationError):
        validate_envelope(mutated)


def test_tagged_union_success_and_rejection_are_mutually_exclusive() -> None:
    examples = {e["name"]: e["body"] for e in _load_examples()}
    succeeded = copy.deepcopy(examples["controller_mode_result_succeeded"])
    succeeded["rejection_code"] = "CAS_LOST"
    with pytest.raises(ProtocolValidationError):
        validate_envelope(succeeded)

    rejected = copy.deepcopy(examples["controller_mode_result_rejected"])
    rejected["mode_revision"] = 2
    rejected["mode"] = "RUNNING"
    with pytest.raises(ProtocolValidationError):
        validate_envelope(rejected)


def test_launch_accepted_status_provider_union_is_enforced() -> None:
    examples = {e["name"]: e["body"] for e in _load_examples()}
    available_without_provider = copy.deepcopy(examples["launch_accepted_available"])
    available_without_provider["provider"] = None
    with pytest.raises(ProtocolValidationError):
        validate_envelope(available_without_provider)

    expired_with_provider = copy.deepcopy(examples["launch_accepted_expired"])
    expired_with_provider["provider"] = examples["launch_accepted_available"]["provider"]
    with pytest.raises(ProtocolValidationError):
        validate_envelope(expired_with_provider)


def test_credential_rotation_applied_cas_lost_union_is_enforced() -> None:
    examples = {e["name"]: e["body"] for e in _load_examples()}
    cas_lost_with_receipt = copy.deepcopy(examples["credential_rotation_result_cas_lost"])
    cas_lost_with_receipt["accepted_version"] = 4
    cas_lost_with_receipt["credential_rotation_receipt_id"] = "dddddddd-dddd-dddd-dddd-dddddddddddd"
    with pytest.raises(ProtocolValidationError):
        validate_envelope(cas_lost_with_receipt)
