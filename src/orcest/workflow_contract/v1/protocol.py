"""Strict versioned request/response validators for Workflow-Control v1.

This module is the one registry through which every normative
``protocol``/``protocol_version`` envelope (e.g. ``orcest.attempt-claim/1``,
``orcest.controller-mode-operation/1``) round-trips. It fails closed on:

- an unregistered/unknown protocol literal,
- an unknown top-level or nested field,
- an unknown enum value,
- a null where the field is required non-null, or a missing value where the
  field is nullable-but-required-present versus genuinely optional.

Feature code (SQLite migrations, HTTP handlers, Redis envelopes, forge
adapters) MUST call :func:`validate_envelope` rather than hand-rolling field
presence/enum checks, and MUST NOT define its own ``orcest.<name>/<n>``
literal outside :mod:`orcest.workflow_contract.v1.protocol_registry` --
``tests/workflow_contract/test_no_shadow_contracts.py`` enforces this.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

__all__ = [
    "ProtocolValidationError",
    "Field",
    "Schema",
    "EnvelopeSchema",
    "register_envelope",
    "known_protocol_literals",
    "get_envelope_schema",
    "validate_object",
    "validate_envelope",
]


class ProtocolValidationError(ValueError):
    """Raised when an envelope fails strict v1 validation. Always fail closed."""


@dataclass(frozen=True, slots=True)
class Field:
    """One field's shape within a :class:`Schema`.

    ``required`` and ``nullable`` are independent: a field may be required
    but nullable (must be present, JSON ``null`` allowed), or optional and
    non-nullable (may be omitted, but if present must not be ``null``).
    Ambiguous nullability -- an implicit "maybe absent, maybe null, treat the
    same" -- is exactly what this module must reject; callers must pick one.
    """

    required: bool = True
    nullable: bool = False
    enum: frozenset[str] | None = None
    schema: "Schema | None" = None
    item_schema: "Schema | None" = None
    validator: Callable[[Any], None] | None = None

    def __post_init__(self) -> None:
        if self.schema is not None and self.item_schema is not None:
            raise ValueError("a Field cannot have both schema and item_schema")


@dataclass(frozen=True, slots=True)
class Schema:
    """A closed set of allowed fields. Any field not listed here is rejected.

    ``object_validator``, when given, runs only after every individual field
    has passed its own check; it is where a tagged union's cross-field
    invariant lives (e.g. "``mode_revision`` is present if and only if
    ``status == SUCCEEDED``"), since a single :class:`Field` cannot express a
    constraint that depends on a sibling field's value.
    """

    fields: Mapping[str, Field] = field(default_factory=dict)
    object_validator: Callable[[Mapping[str, Any]], None] | None = None


@dataclass(frozen=True, slots=True)
class EnvelopeSchema:
    """A registered protocol envelope: its literal, discriminant field name, and shape."""

    literal: str
    schema: Schema
    protocol_field: str = "protocol_version"


_REGISTRY: dict[str, EnvelopeSchema] = {}


def register_envelope(
    literal: str,
    fields: Mapping[str, Field],
    *,
    protocol_field: str = "protocol_version",
    object_validator: Callable[[Mapping[str, Any]], None] | None = None,
) -> EnvelopeSchema:
    """Register one closed protocol envelope schema under ``literal``.

    The discriminant field itself is injected into the schema automatically
    (as a required, non-nullable field pinned to exactly ``literal``), so
    callers only need to describe the payload fields.
    """
    if literal in _REGISTRY:
        raise RuntimeError(f"protocol literal already registered: {literal!r}")
    if protocol_field in fields:
        raise ValueError(
            f"{literal}: fields must not redeclare the discriminant field {protocol_field!r}"
        )
    full_fields = dict(fields)
    full_fields[protocol_field] = Field(required=True, nullable=False, enum=frozenset({literal}))
    envelope = EnvelopeSchema(
        literal=literal,
        schema=Schema(fields=full_fields, object_validator=object_validator),
        protocol_field=protocol_field,
    )
    _REGISTRY[literal] = envelope
    return envelope


def known_protocol_literals() -> frozenset[str]:
    return frozenset(_REGISTRY)


def get_envelope_schema(literal: str) -> EnvelopeSchema:
    try:
        return _REGISTRY[literal]
    except KeyError as exc:
        raise ProtocolValidationError(f"unknown protocol version {literal!r}") from exc


def validate_object(value: Any, schema: Schema, *, path: str) -> None:
    """Validate ``value`` (a JSON object) against ``schema``, raising on the first violation."""
    if not isinstance(value, Mapping):
        raise ProtocolValidationError(f"{path}: expected a JSON object, got {type(value).__name__}")
    unknown = set(value) - set(schema.fields)
    if unknown:
        raise ProtocolValidationError(f"{path}: unknown field(s) {sorted(unknown)!r}")
    for name, spec in schema.fields.items():
        present = name in value
        if not present:
            if spec.required:
                raise ProtocolValidationError(f"{path}.{name}: missing required field")
            continue
        item = value[name]
        if item is None:
            if not spec.nullable:
                raise ProtocolValidationError(f"{path}.{name}: must not be null")
            continue
        if spec.enum is not None and item not in spec.enum:
            raise ProtocolValidationError(f"{path}.{name}: unknown enum value {item!r}")
        if spec.schema is not None:
            validate_object(item, spec.schema, path=f"{path}.{name}")
        if spec.item_schema is not None:
            if not isinstance(item, Sequence) or isinstance(item, (str, bytes)):
                raise ProtocolValidationError(f"{path}.{name}: expected an array")
            for index, element in enumerate(item):
                validate_object(element, spec.item_schema, path=f"{path}.{name}[{index}]")
        if spec.validator is not None:
            spec.validator(item)
    if schema.object_validator is not None:
        try:
            schema.object_validator(value)
        except ProtocolValidationError:
            raise
        except ValueError as exc:
            raise ProtocolValidationError(f"{path}: {exc}") from exc


def validate_envelope(value: Any) -> Mapping[str, Any]:
    """Validate a top-level protocol envelope, dispatching on its own literal.

    ``value`` must carry either ``protocol_version`` or ``protocol`` naming a
    registered literal (both discriminant field names are used across the
    v1 wire contracts). Returns ``value`` unchanged on success.
    """
    if not isinstance(value, Mapping):
        raise ProtocolValidationError("envelope must be a JSON object")
    if "protocol_version" in value:
        literal = value["protocol_version"]
    elif "protocol" in value:
        literal = value["protocol"]
    else:
        raise ProtocolValidationError("envelope is missing protocol/protocol_version")
    if not isinstance(literal, str):
        raise ProtocolValidationError(
            f"protocol/protocol_version must be a string, got {literal!r}"
        )
    envelope = get_envelope_schema(literal)
    validate_object(value, envelope.schema, path=literal)
    return value
