"""Closed Workflow-Control v1 Human Boundary / Human Resolution schemas.

The allowlisted reason set, the reason->resolution and resolution->source
mappings, and the reason-specific resolution payload schemas are all
code-owned (domain-model.md "Human Boundary" / "Human Resolution",
workflow-lifecycle.md "Exceptional human boundary"). Kept separate from
``recovery.py`` because these mappings govern the Human Boundary/Resolution
objects themselves, not recovery-tactic selection; both the reducer and the
durable store validate against them so a boundary or resolution can never be
widened by an implicit default.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from orcest.workflow_contract.v1 import enums

__all__ = [
    "CATEGORY_HUMAN_BOUNDARY_REASONS",
    "HUMAN_BOUNDARY_RESOLUTION_KINDS",
    "HUMAN_RESOLUTION_SOURCE_KINDS",
    "MAX_BOUNDED_ENTRIES",
    "MAX_PROSE_LENGTH",
    "OWNERSHIP_CHOICE_ID",
    "SECRET_STORE_VERIFIER_PRINCIPAL_ID",
    "required_resolution_kinds",
    "resolution_source_kinds",
    "validate_resolution_payload",
]

MAX_PROSE_LENGTH = 2048
MAX_BOUNDED_ENTRIES = 128

# Closed Human Boundary reason -> allowed Human Resolution kind(s)
# (workflow-lifecycle.md "Resumption" table).
HUMAN_BOUNDARY_RESOLUTION_KINDS: Mapping[str, frozenset[str]] = {
    "MISSING_AUTHORITY": frozenset({"AUTHORITY_GRANTED"}),
    "REQUIRED_SECRET_OR_PERMISSION": frozenset({"SECRET_OR_PERMISSION_PROVIDED"}),
    "IRREVERSIBLE_DECISION": frozenset({"IRREVERSIBLE_ACTION_AUTHORIZED"}),
    "SPECIFICATION_CONFLICT": frozenset({"SPECIFICATION_AMENDED"}),
    "SECURITY_POLICY_BOUNDARY": frozenset({"SECURITY_ACTION_AUTHORIZED"}),
    "INTEGRITY_FAILURE": frozenset({"INTEGRITY_RESTORED"}),
    "UNSATISFIABLE_REQUIREMENTS": frozenset(
        {"SPECIFICATION_AMENDED", "ENVIRONMENT_CAPABILITY_PROVIDED"}
    ),
    "PUBLICATION_OWNERSHIP_CONFLICT": frozenset({"PUBLICATION_OWNERSHIP_RESOLVED"}),
}

# Closed Human Resolution kind -> allowed source kind(s) (same table's
# "Required source and behavior" column). A Management Command can never
# synthesize SPECIFICATION_AMENDED or INTEGRITY_RESTORED, and only a
# Secret-Store-verifier-authenticated SECRET_VERSION or an authenticated
# Management Command may satisfy SECRET_OR_PERMISSION_PROVIDED.
HUMAN_RESOLUTION_SOURCE_KINDS: Mapping[str, frozenset[str]] = {
    "AUTHORITY_GRANTED": frozenset({"MANAGEMENT_COMMAND"}),
    "SECRET_OR_PERMISSION_PROVIDED": frozenset({"MANAGEMENT_COMMAND", "SECRET_VERSION"}),
    "IRREVERSIBLE_ACTION_AUTHORIZED": frozenset({"MANAGEMENT_COMMAND"}),
    "SPECIFICATION_AMENDED": frozenset({"FORGE_OBSERVATION"}),
    "SECURITY_ACTION_AUTHORIZED": frozenset({"MANAGEMENT_COMMAND"}),
    "INTEGRITY_RESTORED": frozenset({"STORAGE_RESTORATION"}),
    "ENVIRONMENT_CAPABILITY_PROVIDED": frozenset({"MANAGEMENT_COMMAND"}),
    "PUBLICATION_OWNERSHIP_RESOLVED": frozenset({"MANAGEMENT_COMMAND"}),
}

# A Recovery Evidence ``ENTER_HUMAN_BOUNDARY`` tactic can only source the
# reasons that survive autonomous diagnosis; PUBLICATION_OWNERSHIP_CONFLICT
# has the sole direct Reconciliation Fact path and can never be entered from
# Recovery Evidence (domain-model.md "Human Boundary").
CATEGORY_HUMAN_BOUNDARY_REASONS: Mapping[str, frozenset[str]] = {
    "INTEGRITY_SUSPECTED": frozenset({"INTEGRITY_FAILURE"}),
    "POLICY": frozenset(
        {
            "MISSING_AUTHORITY",
            "REQUIRED_SECRET_OR_PERMISSION",
            "IRREVERSIBLE_DECISION",
            "SPECIFICATION_CONFLICT",
            "SECURITY_POLICY_BOUNDARY",
            "UNSATISFIABLE_REQUIREMENTS",
        }
    ),
}

OWNERSHIP_CHOICE_ID = "continue-orcest-ownership"

# The registered controller Secret-Store verifier/reconciler service
# principal that alone may author an automatic SECRET_OR_PERMISSION_PROVIDED
# Resolution sourced from a SECRET_VERSION -- never the worker, the original
# provisioning operator, or a synthetic user.
SECRET_STORE_VERIFIER_PRINCIPAL_ID = "controller-secret-store-verifier"

# Closed, bounded required-key schema per resolution kind. Every key's value
# must be a non-empty bounded string (or, for effect_generation, a positive
# integer); no key may carry a raw secret value, only a Secret Reference or
# version identity.
_RESOLUTION_SCHEMAS: Mapping[str, frozenset[str]] = {
    "AUTHORITY_GRANTED": frozenset({"granted_authority", "scope"}),
    "SECRET_OR_PERMISSION_PROVIDED": frozenset(
        {"secret_version_key", "creation_receipt_id", "integrity_attestation_id"}
    ),
    "IRREVERSIBLE_ACTION_AUTHORIZED": frozenset({"choice_id", "action_scope"}),
    "SECURITY_ACTION_AUTHORIZED": frozenset({"authorized_action"}),
    "INTEGRITY_RESTORED": frozenset({"storage_restoration_fact_id"}),
    "ENVIRONMENT_CAPABILITY_PROVIDED": frozenset({"capability"}),
    "PUBLICATION_OWNERSHIP_RESOLVED": frozenset(
        {
            "selected_engine",
            "project_id",
            "deterministic_ref",
            "change_request_external_id",
            "run_marker",
            "publication_id",
            "effect_generation",
        }
    ),
}


def required_resolution_kinds(reason: str) -> tuple[str, ...]:
    """The canonically sorted, non-empty, code-owned resolution kinds this
    boundary reason accepts. Never caller-supplied."""
    enums.parse_enum("human_boundary.reason", reason)
    return tuple(sorted(HUMAN_BOUNDARY_RESOLUTION_KINDS[reason]))


def resolution_source_kinds(resolution_kind: str) -> frozenset[str]:
    enums.parse_enum("human_resolution.resolution_kind", resolution_kind)
    return HUMAN_RESOLUTION_SOURCE_KINDS[resolution_kind]


def validate_resolution_payload(resolution_kind: str, resolution: Mapping[str, Any]) -> None:
    """Reject a resolution payload that is not this kind's exact closed schema.

    A repository file, prompt, issue comment, or generic "continue" text can
    never satisfy a Human Resolution (workflow-lifecycle.md "Resumption");
    this enforces that only the exact reason-bound field set and non-empty
    bounded values are ever accepted. The closed per-kind key sets below
    never include a raw secret value field -- only a Secret Reference or
    version identity -- so no separately-named field can smuggle one in.
    """
    enums.parse_enum("human_resolution.resolution_kind", resolution_kind)
    required_keys = _RESOLUTION_SCHEMAS[resolution_kind]
    actual_keys = set(resolution.keys())
    if actual_keys != required_keys:
        raise ValueError(
            f"{resolution_kind} resolution requires exactly {sorted(required_keys)}, "
            f"got {sorted(actual_keys)}"
        )
    for key, value in resolution.items():
        if key == "effect_generation":
            if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
                raise ValueError("effect_generation must be a positive integer")
            continue
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"resolution field {key!r} must be a non-empty string")
        if len(value) > MAX_PROSE_LENGTH:
            raise ValueError(f"resolution field {key!r} exceeds {MAX_PROSE_LENGTH} scalars")
    if (
        resolution_kind == "PUBLICATION_OWNERSHIP_RESOLVED"
        and resolution["selected_engine"] != "ORCEST_V1"
    ):
        raise ValueError("PUBLICATION_OWNERSHIP_RESOLVED.selected_engine has no other v1 value")
