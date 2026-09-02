"""Closed structured-output validation for Workflow-Control v1 worker Results."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from orcest.workflow_contract.v1.protocol import ProtocolValidationError, validate_envelope
from orcest.workflow_contract.v1.protocol_registry import DIAGNOSIS_PROTOCOL, PLAN_PROTOCOL

__all__ = [
    "STRUCTURED_OUTPUT_ACTIVITY_PROTOCOLS",
    "StructuredOutputValidationError",
    "structured_output_protocol",
    "validate_attempt_structured_output",
]


class StructuredOutputValidationError(ValueError):
    """Raised when a worker's model-authored structured output is not typed data."""


STRUCTURED_OUTPUT_ACTIVITY_PROTOCOLS: Mapping[str, str] = {
    "PLAN": PLAN_PROTOCOL,
    "REPLAN": PLAN_PROTOCOL,
    "DIAGNOSE": DIAGNOSIS_PROTOCOL,
}

_PROSE_FIELDS = frozenset({"summary", "notes", "rationale", "description"})
_LIFECYCLE_WORDS = frozenset(
    {
        "ADMITTED",
        "PLANNING",
        "BUILDING",
        "VERIFYING",
        "REVIEWING",
        "AGGREGATING",
        "REMEDIATING",
        "DIAGNOSING",
        "REPLANNING",
        "ADJUDICATING",
        "APPROVED",
        "PUBLISHING",
        "PR_MONITORING",
        "PR_REMEDIATING",
        "RECOVERING",
        "WAITING",
        "NEEDS_HUMAN",
        "MERGED",
        "CLOSED",
        "CANCELLED",
    }
)
_DIRECTIVE_WORDS = frozenset(
    {
        "STATE",
        "TRANSITION",
        "LIFECYCLE",
        "APPROVE",
        "APPROVED",
        "MERGE",
        "PUBLISH",
        "CANCEL",
        "CANCELLED",
        "CLOSE",
        "CLOSED",
        "NEEDS_HUMAN",
        "WAIT",
        "WAITING",
    }
)


def structured_output_protocol(activity_kind: str) -> str | None:
    """Return the closed structured-output protocol required by ``activity_kind``."""
    return STRUCTURED_OUTPUT_ACTIVITY_PROTOCOLS.get(activity_kind)


def validate_attempt_structured_output(
    *,
    activity_kind: str,
    outcome: str,
    structured_output: Any | None,
    candidate_upload_id: str | None,
    receipt: Any | None,
    summary: str | None,
) -> None:
    """Validate the model/result payload before it can mutate lifecycle state.

    PLAN, REPLAN, and DIAGNOSE success require closed typed envelopes. Candidate
    and receipt Activities use controller-owned artifacts/receipts. Prose is
    retained only as non-authoritative summary text and may not carry lifecycle
    directives.
    """
    _reject_lifecycle_directive_prose(summary)
    expected_protocol = structured_output_protocol(activity_kind)
    if outcome != "SUCCEEDED":
        if structured_output is not None:
            raise StructuredOutputValidationError(
                "non-SUCCEEDED Attempt Results must not carry structured_output"
            )
        return
    if expected_protocol is not None:
        if structured_output is None:
            raise StructuredOutputValidationError(
                f"{activity_kind} success requires {expected_protocol} structured_output"
            )
        try:
            validated = validate_envelope(structured_output)
        except ProtocolValidationError as exc:
            raise StructuredOutputValidationError(str(exc)) from exc
        protocol = validated.get("protocol_version", validated.get("protocol"))
        if protocol != expected_protocol:
            raise StructuredOutputValidationError(
                f"{activity_kind} success requires {expected_protocol}, got {protocol!r}"
            )
        _reject_lifecycle_directives(structured_output)
        return
    if activity_kind in {"BUILD", "REMEDIATE", "REBASE", "PR_REMEDIATE"}:
        if structured_output is not None:
            raise StructuredOutputValidationError(
                f"{activity_kind} success must not carry model lifecycle structured_output"
            )
        return
    if activity_kind in {"VERIFY", "REVIEW", "ADJUDICATE"}:
        if structured_output is not None:
            raise StructuredOutputValidationError(
                f"{activity_kind} success must not carry model lifecycle structured_output"
            )
        return
    if structured_output is not None:
        raise StructuredOutputValidationError(
            f"{activity_kind} does not accept model structured_output in v1"
        )


def _reject_lifecycle_directives(value: Any) -> None:
    for path, text in _iter_strings(value):
        if path[-1:] and str(path[-1]) in _PROSE_FIELDS:
            _reject_lifecycle_directive_prose(text, field_path=".".join(str(part) for part in path))


def _iter_strings(
    value: Any, *, path: tuple[str | int, ...] = ()
) -> Iterable[tuple[tuple[str | int, ...], str]]:
    if isinstance(value, str):
        yield path, value
    elif isinstance(value, Mapping):
        for key, item in value.items():
            yield from _iter_strings(item, path=(*path, str(key)))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            yield from _iter_strings(item, path=(*path, index))


def _reject_lifecycle_directive_prose(text: str | None, *, field_path: str = "summary") -> None:
    if not text:
        return
    tokens = {
        token.strip(".,:;!?()[]{}<>\"'`").upper()
        for token in text.replace("/", " ").replace("-", "_").split()
    }
    if tokens & _LIFECYCLE_WORDS and tokens & _DIRECTIVE_WORDS:
        raise StructuredOutputValidationError(
            f"{field_path} appears to contain lifecycle directives"
        )
