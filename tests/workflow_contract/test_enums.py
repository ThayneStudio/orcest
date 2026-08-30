import pytest

from orcest.workflow_contract.v1 import enums


def test_registry_is_nonempty_and_names_are_dotted() -> None:
    names = enums.registered_enum_names()
    assert len(names) > 50
    assert all("." in name for name in names)


def test_parse_enum_round_trips() -> None:
    value = enums.parse_enum("run.state", "MERGED")
    assert value.value == "MERGED"
    assert value is enums.RunState.MERGED


def test_parse_enum_fails_closed_on_unknown_value() -> None:
    with pytest.raises(enums.UnknownEnumValueError):
        enums.parse_enum("run.state", "NOT_A_REAL_STATE")


def test_get_enum_fails_closed_on_unknown_name() -> None:
    with pytest.raises(enums.UnknownEnumNameError):
        enums.get_enum("not.a.real.enum")


def test_run_terminal_states_closed_set() -> None:
    assert enums.RUN_TERMINAL_STATES == {
        enums.RunState.MERGED,
        enums.RunState.CLOSED,
        enums.RunState.CANCELLED,
    }
    assert enums.RunState.PLANNING not in enums.RUN_TERMINAL_STATES


def test_activity_kind_partition_is_total_and_disjoint() -> None:
    all_kinds = set(enums.ActivityKind)
    worker = enums.WORKER_ACTIVITY_KINDS
    controller = enums.CONTROLLER_ACTIVITY_KINDS
    assert worker | controller == all_kinds
    assert worker & controller == set()


def test_failure_class_is_closed_to_fourteen_values() -> None:
    assert len(list(enums.FailureClass)) == 14


def test_result_request_disposition_is_closed_to_five_values() -> None:
    assert {member.value for member in enums.ResultRequestDisposition} == {
        "ACCEPTED",
        "UPLOAD_EXPIRED",
        "STALE_ATTEMPT",
        "EXPIRED_CURRENT",
        "ALREADY_TERMINAL",
    }


def test_recovery_tactic_has_no_lifecycle_ordering_only_vocabulary() -> None:
    # The registry stores the closed vocabulary only; it must not expose any
    # notion of "next" tactic or legality -- that is lifecycle logic owned
    # elsewhere.
    assert not hasattr(enums.RecoveryTactic, "next")
    assert "RETRY_EXECUTION" in {member.value for member in enums.RecoveryTactic}


def test_duplicate_registration_is_rejected() -> None:
    with pytest.raises(RuntimeError):
        enums._register_enum("run.state", "RunStateAgain", ["X"])


def test_str_enum_values_serialize_as_plain_strings() -> None:
    # Every v1 enum member IS its own wire value (str mixin), so json.dumps
    # of the member behaves like the plain string per the wiki's
    # "Enum values are the uppercase ASCII values written in this document."
    import json

    assert json.dumps(enums.RunState.MERGED.value) == '"MERGED"'
