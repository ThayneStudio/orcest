import pytest

from orcest.workflow_contract.v1.protocol import (
    Field,
    ProtocolValidationError,
    Schema,
    known_protocol_literals,
    register_envelope,
    validate_envelope,
    validate_object,
)


def test_known_protocol_literals_includes_registered_ones() -> None:
    literals = known_protocol_literals()
    assert "orcest.error/1" in literals
    assert "orcest.controller-mode-result/1" in literals


def test_validate_envelope_requires_protocol_field() -> None:
    with pytest.raises(ProtocolValidationError):
        validate_envelope({"not_a_protocol_field": 1})


def test_validate_envelope_rejects_non_object() -> None:
    with pytest.raises(ProtocolValidationError):
        validate_envelope("not-an-object")  # type: ignore[arg-type]


def test_nested_schema_rejects_unknown_nested_field() -> None:
    register_envelope(
        "orcest.__test-nested/1",
        {"inner": Field(schema=Schema(fields={"a": Field(required=True)}))},
    )
    try:
        validate_envelope({"protocol_version": "orcest.__test-nested/1", "inner": {"a": 1, "b": 2}})
        raise AssertionError("expected ProtocolValidationError")
    except ProtocolValidationError:
        pass


def test_item_schema_validates_each_array_element() -> None:
    register_envelope(
        "orcest.__test-items/1",
        {"items": Field(item_schema=Schema(fields={"a": Field(required=True)}))},
    )
    validate_envelope({"protocol_version": "orcest.__test-items/1", "items": [{"a": 1}, {"a": 2}]})
    with pytest.raises(ProtocolValidationError):
        validate_envelope(
            {"protocol_version": "orcest.__test-items/1", "items": [{"a": 1}, {"b": 2}]}
        )


def test_required_non_nullable_field_rejects_null() -> None:
    schema = Schema(fields={"x": Field(required=True, nullable=False)})
    with pytest.raises(ProtocolValidationError):
        validate_object({"x": None}, schema, path="test")


def test_optional_field_may_be_omitted_but_not_null_unless_nullable() -> None:
    schema = Schema(fields={"x": Field(required=False, nullable=False)})
    validate_object({}, schema, path="test")
    with pytest.raises(ProtocolValidationError):
        validate_object({"x": None}, schema, path="test")


def test_enum_field_rejects_unknown_value() -> None:
    schema = Schema(fields={"x": Field(enum=frozenset({"A", "B"}))})
    validate_object({"x": "A"}, schema, path="test")
    with pytest.raises(ProtocolValidationError):
        validate_object({"x": "C"}, schema, path="test")


def test_duplicate_protocol_registration_raises() -> None:
    register_envelope("orcest.__test-dup/1", {})
    with pytest.raises(RuntimeError):
        register_envelope("orcest.__test-dup/1", {})


def test_field_cannot_declare_both_schema_and_item_schema() -> None:
    with pytest.raises(ValueError):
        Field(schema=Schema(fields={}), item_schema=Schema(fields={}))


def _error_envelope(code: str) -> dict[str, object]:
    return {"protocol": "orcest.error/1", "code": code, "retryable": True}


def test_error_envelope_accepts_attempt_stale() -> None:
    validate_envelope(_error_envelope("ATTEMPT_STALE"))


def test_error_envelope_accepts_cas_lost_extension() -> None:
    validate_envelope(_error_envelope("CAS_LOST"))


def test_error_envelope_rejects_unknown_code() -> None:
    with pytest.raises(ProtocolValidationError):
        validate_envelope(_error_envelope("NOT_A_REAL_CODE"))
