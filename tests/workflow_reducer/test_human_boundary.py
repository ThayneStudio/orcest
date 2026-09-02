"""Closed Human Boundary / Human Resolution schema mappings (issue #689)."""

from __future__ import annotations

import pytest

from orcest.workflow_reducer.human_boundary import (
    CATEGORY_HUMAN_BOUNDARY_REASONS,
    HUMAN_BOUNDARY_RESOLUTION_KINDS,
    HUMAN_RESOLUTION_SOURCE_KINDS,
    required_resolution_kinds,
    resolution_source_kinds,
    validate_resolution_payload,
)

pytestmark = pytest.mark.unit


def test_every_boundary_reason_has_a_non_empty_resolution_mapping() -> None:
    from orcest.workflow_contract.v1 import enums

    for reason in enums.HumanBoundaryReason:
        assert HUMAN_BOUNDARY_RESOLUTION_KINDS[reason.value]
        assert required_resolution_kinds(reason.value) == tuple(
            sorted(HUMAN_BOUNDARY_RESOLUTION_KINDS[reason.value])
        )


def test_every_resolution_kind_has_a_non_empty_source_mapping() -> None:
    from orcest.workflow_contract.v1 import enums

    for kind in enums.HumanResolutionKind:
        assert HUMAN_RESOLUTION_SOURCE_KINDS[kind.value]
        assert resolution_source_kinds(kind.value) == HUMAN_RESOLUTION_SOURCE_KINDS[kind.value]


def test_management_command_cannot_synthesize_specification_amended() -> None:
    assert "MANAGEMENT_COMMAND" not in resolution_source_kinds("SPECIFICATION_AMENDED")
    assert resolution_source_kinds("SPECIFICATION_AMENDED") == {"FORGE_OBSERVATION"}


def test_management_command_cannot_synthesize_integrity_restored() -> None:
    assert "MANAGEMENT_COMMAND" not in resolution_source_kinds("INTEGRITY_RESTORED")


def test_publication_ownership_conflict_is_not_recovery_evidence_reachable() -> None:
    assert "PUBLICATION_OWNERSHIP_CONFLICT" not in CATEGORY_HUMAN_BOUNDARY_REASONS.get(
        "INTEGRITY_SUSPECTED", frozenset()
    )
    assert "PUBLICATION_OWNERSHIP_CONFLICT" not in CATEGORY_HUMAN_BOUNDARY_REASONS.get(
        "POLICY", frozenset()
    )


def test_validate_resolution_payload_accepts_exact_closed_schema() -> None:
    validate_resolution_payload(
        "AUTHORITY_GRANTED", {"granted_authority": "force-push", "scope": "run-scoped"}
    )


@pytest.mark.parametrize(
    "resolution",
    [
        {},
        {"granted_authority": "force-push"},
        {"granted_authority": "force-push", "scope": "run-scoped", "extra": "field"},
    ],
)
def test_validate_resolution_payload_rejects_wrong_key_set(resolution: dict) -> None:
    with pytest.raises(ValueError, match="requires exactly"):
        validate_resolution_payload("AUTHORITY_GRANTED", resolution)


def test_validate_resolution_payload_rejects_blank_value() -> None:
    with pytest.raises(ValueError, match="non-empty string"):
        validate_resolution_payload("AUTHORITY_GRANTED", {"granted_authority": "  ", "scope": "x"})


def test_validate_resolution_payload_secret_schema_has_no_raw_secret_field() -> None:
    validate_resolution_payload(
        "SECRET_OR_PERMISSION_PROVIDED",
        {
            "secret_version_key": "11111111-1111-4111-8111-111111111111:1",
            "creation_receipt_id": "receipt-1",
            "integrity_attestation_id": "attestation-1",
        },
    )
    with pytest.raises(ValueError, match="requires exactly"):
        validate_resolution_payload(
            "SECRET_OR_PERMISSION_PROVIDED",
            {
                "secret_version_key": "11111111-1111-4111-8111-111111111111:1",
                "creation_receipt_id": "receipt-1",
                "raw_secret_value": "hunter2",
            },
        )


def test_validate_resolution_payload_rejects_non_orcest_v1_engine() -> None:
    with pytest.raises(ValueError, match="no other v1 value"):
        validate_resolution_payload(
            "PUBLICATION_OWNERSHIP_RESOLVED",
            {
                "selected_engine": "LEGACY",
                "project_id": "project-1",
                "deterministic_ref": "refs/heads/main",
                "change_request_external_id": "123",
                "run_marker": "orcest-v1:abc",
                "publication_id": "11111111-1111-4111-8111-111111111111",
                "effect_generation": 1,
            },
        )


def test_validate_resolution_payload_requires_positive_effect_generation() -> None:
    with pytest.raises(ValueError, match="positive integer"):
        validate_resolution_payload(
            "PUBLICATION_OWNERSHIP_RESOLVED",
            {
                "selected_engine": "ORCEST_V1",
                "project_id": "project-1",
                "deterministic_ref": "refs/heads/main",
                "change_request_external_id": "123",
                "run_marker": "orcest-v1:abc",
                "publication_id": "11111111-1111-4111-8111-111111111111",
                "effect_generation": 0,
            },
        )
