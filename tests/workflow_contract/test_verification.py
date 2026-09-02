"""Deterministic Verification Profile materialization and receipt admission."""

from __future__ import annotations

import pytest

from orcest.workflow_contract.v1.digest import content_digest
from orcest.workflow_contract.v1.verification import (
    VerificationReceiptRejectedError,
    command_invocation_digest,
    materialize_verification_profile,
    recompute_verification_outcome,
    validate_verification_receipt,
    verification_profile_from_effective_policy,
    verification_profile_hash,
)

pytestmark = pytest.mark.unit

CANDIDATE_ID = "11111111-1111-4111-8111-111111111111"
COMMIT = {"object_format": "sha1", "oid": "a" * 40}
RAW_COMMANDS = [
    {"id": "unit", "argv": ["make", "test-unit"], "timeoutSeconds": 600},
    {"id": "lint", "argv": ["make", "lint-check"], "cwd": ".", "timeoutSeconds": 300},
]


def _digest() -> str:
    return content_digest(b"stand-in")


def _profile():
    commands = materialize_verification_profile(RAW_COMMANDS)
    return commands, verification_profile_hash(commands)


def _check(command: dict, *, termination: str = "EXITED", exit_code: int | None = 0) -> dict:
    check: dict[str, object] = {
        "command_id": command["id"],
        "invocation_digest": command_invocation_digest(command),
        "termination": termination,
        "stdout_digest": _digest(),
        "stderr_digest": _digest(),
        "evidence": [],
    }
    if exit_code is not None:
        check["exit_code"] = exit_code
    return check


def _receipt(checks: list[dict], *, outcome: str, profile_hash: str, error=None) -> dict:
    return {
        "protocol": "orcest.verification-receipt/1",
        "candidate": {"candidate_id": CANDIDATE_ID, "commit": COMMIT},
        "profile_id": "default",
        "profile_hash": profile_hash,
        "checks": checks,
        "outcome": outcome,
        "error": error,
    }


def _validate(receipt: dict, commands: list[dict], profile_hash: str) -> str:
    return validate_verification_receipt(
        receipt,
        expected_candidate_id=CANDIDATE_ID,
        expected_commit=COMMIT,
        expected_profile_id="default",
        expected_profile_hash=profile_hash,
        expected_commands=commands,
    )


# --- materialization ---------------------------------------------------------


def test_materialize_preserves_declared_order_and_normalizes_defaults():
    commands = materialize_verification_profile(RAW_COMMANDS)
    assert [c["id"] for c in commands] == ["unit", "lint"]
    assert commands[0]["cwd"] == "."
    assert commands[0]["environment"] == {}


def test_materialize_is_deterministic():
    commands_a = materialize_verification_profile(RAW_COMMANDS)
    commands_b = materialize_verification_profile(RAW_COMMANDS)
    assert verification_profile_hash(commands_a) == verification_profile_hash(commands_b)


def test_materialize_rejects_empty_commands():
    with pytest.raises(VerificationReceiptRejectedError):
        materialize_verification_profile([])


def test_materialize_rejects_duplicate_ids():
    with pytest.raises(VerificationReceiptRejectedError):
        materialize_verification_profile([RAW_COMMANDS[0], RAW_COMMANDS[0]])


def test_verification_profile_from_effective_policy_round_trips():
    profile_id, commands, profile_hash = verification_profile_from_effective_policy(
        {"verification": {"profile": "default", "commands": RAW_COMMANDS}}
    )
    assert profile_id == "default"
    assert commands == materialize_verification_profile(RAW_COMMANDS)
    assert profile_hash == verification_profile_hash(commands)


def test_verification_profile_from_effective_policy_rejects_missing_verification():
    with pytest.raises(VerificationReceiptRejectedError):
        verification_profile_from_effective_policy({})


def test_verification_profile_from_effective_policy_rejects_non_default_profile():
    with pytest.raises(VerificationReceiptRejectedError):
        verification_profile_from_effective_policy(
            {"verification": {"profile": "other", "commands": RAW_COMMANDS}}
        )


# --- outcome recomputation ----------------------------------------------------


def test_recompute_outcome_pass_when_every_command_exits_zero():
    commands, _ = _profile()
    checks = [_check(c) for c in commands]
    assert recompute_verification_outcome(checks) == "PASS"


def test_recompute_outcome_fail_on_ordinary_nonzero_exit_with_later_not_run():
    commands, _ = _profile()
    checks = [
        _check(commands[0], exit_code=1),
        _check(commands[1], termination="NOT_RUN", exit_code=None),
    ]
    assert recompute_verification_outcome(checks) == "FAIL"


def test_recompute_outcome_fail_on_signal():
    commands, _ = _profile()
    checks = [
        _check(commands[0], termination="SIGNALED", exit_code=None),
        _check(commands[1], termination="NOT_RUN", exit_code=None),
    ]
    assert recompute_verification_outcome(checks) == "FAIL"


def test_recompute_outcome_error_on_timeout():
    commands, _ = _profile()
    checks = [_check(commands[0], termination="TIMED_OUT", exit_code=None), _check(commands[1])]
    assert recompute_verification_outcome(checks) == "ERROR"


def test_recompute_outcome_error_on_unexplained_not_run():
    commands, _ = _profile()
    checks = [
        _check(commands[0]),
        _check(commands[1], termination="NOT_RUN", exit_code=None),
    ]
    # first command passed (exit 0), so the second one being NOT_RUN is unexplained
    assert recompute_verification_outcome(checks) == "ERROR"


# --- full receipt admission ---------------------------------------------------


def test_validate_verification_receipt_admits_pass():
    commands, profile_hash = _profile()
    checks = [_check(c) for c in commands]
    receipt = _receipt(checks, outcome="PASS", profile_hash=profile_hash)
    assert _validate(receipt, commands, profile_hash) == "PASS"


def test_validate_verification_receipt_admits_fail():
    commands, profile_hash = _profile()
    checks = [
        _check(commands[0], exit_code=1),
        _check(commands[1], termination="NOT_RUN", exit_code=None),
    ]
    receipt = _receipt(checks, outcome="FAIL", profile_hash=profile_hash)
    assert _validate(receipt, commands, profile_hash) == "FAIL"


def test_validate_verification_receipt_admits_error_with_error_body():
    commands, profile_hash = _profile()
    checks = [_check(commands[0], termination="TIMED_OUT", exit_code=None), _check(commands[1])]
    receipt = _receipt(
        checks,
        outcome="ERROR",
        profile_hash=profile_hash,
        error={"code": "TIMEOUT", "command_id": commands[0]["id"], "evidence": []},
    )
    assert _validate(receipt, commands, profile_hash) == "ERROR"


def test_validate_verification_receipt_rejects_candidate_mismatch():
    commands, profile_hash = _profile()
    checks = [_check(c) for c in commands]
    receipt = _receipt(checks, outcome="PASS", profile_hash=profile_hash)
    receipt["candidate"] = {
        "candidate_id": "22222222-2222-4222-8222-222222222222",
        "commit": COMMIT,
    }
    with pytest.raises(VerificationReceiptRejectedError):
        _validate(receipt, commands, profile_hash)


def test_validate_verification_receipt_rejects_commit_mismatch():
    commands, profile_hash = _profile()
    checks = [_check(c) for c in commands]
    receipt = _receipt(checks, outcome="PASS", profile_hash=profile_hash)
    receipt["candidate"]["commit"] = {"object_format": "sha1", "oid": "b" * 40}
    with pytest.raises(VerificationReceiptRejectedError):
        _validate(receipt, commands, profile_hash)


def test_validate_verification_receipt_rejects_profile_hash_mismatch():
    commands, profile_hash = _profile()
    checks = [_check(c) for c in commands]
    receipt = _receipt(checks, outcome="PASS", profile_hash=content_digest(b"other"))
    with pytest.raises(VerificationReceiptRejectedError):
        _validate(receipt, commands, profile_hash)


def test_validate_verification_receipt_rejects_missing_command():
    commands, profile_hash = _profile()
    checks = [_check(commands[0])]
    receipt = _receipt(checks, outcome="PASS", profile_hash=profile_hash)
    with pytest.raises(VerificationReceiptRejectedError):
        _validate(receipt, commands, profile_hash)


def test_validate_verification_receipt_rejects_extra_command():
    commands, profile_hash = _profile()
    checks = [_check(c) for c in commands] + [_check(commands[0])]
    receipt = _receipt(checks, outcome="PASS", profile_hash=profile_hash)
    with pytest.raises(VerificationReceiptRejectedError):
        _validate(receipt, commands, profile_hash)


def test_validate_verification_receipt_rejects_out_of_order_commands():
    commands, profile_hash = _profile()
    checks = [_check(commands[1]), _check(commands[0])]
    receipt = _receipt(checks, outcome="PASS", profile_hash=profile_hash)
    with pytest.raises(VerificationReceiptRejectedError):
        _validate(receipt, commands, profile_hash)


def test_validate_verification_receipt_rejects_invocation_digest_mismatch():
    commands, profile_hash = _profile()
    checks = [_check(c) for c in commands]
    checks[0]["invocation_digest"] = content_digest(b"tampered")
    receipt = _receipt(checks, outcome="PASS", profile_hash=profile_hash)
    with pytest.raises(VerificationReceiptRejectedError):
        _validate(receipt, commands, profile_hash)


def test_validate_verification_receipt_rejects_worker_declared_outcome_mismatch():
    commands, profile_hash = _profile()
    checks = [_check(c) for c in commands]
    # Worker claims PASS but a command actually failed.
    checks[0]["exit_code"] = 1
    receipt = _receipt(checks, outcome="PASS", profile_hash=profile_hash)
    with pytest.raises(VerificationReceiptRejectedError):
        _validate(receipt, commands, profile_hash)


def test_validate_verification_receipt_rejects_malformed_schema():
    commands, profile_hash = _profile()
    with pytest.raises(VerificationReceiptRejectedError):
        _validate({"protocol": "orcest.verification-receipt/1"}, commands, profile_hash)


def test_validate_verification_receipt_rejects_wrong_protocol():
    commands, profile_hash = _profile()
    checks = [_check(c) for c in commands]
    receipt = _receipt(checks, outcome="PASS", profile_hash=profile_hash)
    receipt["protocol"] = "orcest.attempt-result-accepted/1"
    with pytest.raises(VerificationReceiptRejectedError):
        _validate(receipt, commands, profile_hash)
