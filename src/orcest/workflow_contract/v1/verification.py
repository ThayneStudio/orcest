"""Deterministic v1 Verification Profile materialization and receipt admission.

Implements the "Effective pinned policy" and "Verification Receipt" sections
of ``docs/wiki/review-and-consensus.md``: exactly one v1 Verification Profile
(``default``) is materialized from the repository's pinned command set, and
every worker-submitted ``orcest.verification-receipt/1`` is admitted only
after the controller independently recomputes its outcome from the checks
alone -- the worker's declared ``outcome`` is never trusted on its own.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from orcest.workflow_contract.v1.digest import (
    verification_command_invocation_digest,
    verification_profile_digest,
)
from orcest.workflow_contract.v1.protocol import ProtocolValidationError, validate_envelope
from orcest.workflow_contract.v1.protocol_registry import VERIFICATION_RECEIPT_PROTOCOL

__all__ = [
    "DEFAULT_VERIFICATION_PROFILE_ID",
    "VerificationReceiptRejectedError",
    "materialize_verification_profile",
    "command_invocation_digest",
    "verification_profile_hash",
    "verification_profile_from_effective_policy",
    "recompute_verification_outcome",
    "validate_verification_receipt",
]

DEFAULT_VERIFICATION_PROFILE_ID = "default"


class VerificationReceiptRejectedError(ValueError):
    """Raised when a Verification Receipt is malformed, mismatched, or invalid.

    Callers must reject the whole Attempt Result submission on this error
    rather than invent a Result -- "Missing or malformed receipts are
    rejected without inventing a Result."
    """


def _normalize_command(raw: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(raw, Mapping):
        raise VerificationReceiptRejectedError("a verification command must be a JSON object")
    try:
        command_id = raw["id"]
        argv = raw["argv"]
        timeout_seconds = raw["timeoutSeconds"]
    except KeyError as exc:
        raise VerificationReceiptRejectedError(f"verification command is missing {exc}") from exc
    cwd = raw.get("cwd", ".")
    environment = raw.get("environment") or {}
    if not isinstance(command_id, str) or not command_id:
        raise VerificationReceiptRejectedError("verification command id must be a non-empty string")
    if not isinstance(argv, Sequence) or isinstance(argv, (str, bytes)) or not argv:
        raise VerificationReceiptRejectedError(
            f"verification command {command_id!r} argv must be a non-empty array"
        )
    if not all(isinstance(item, str) for item in argv):
        raise VerificationReceiptRejectedError(
            f"verification command {command_id!r} argv must contain only strings"
        )
    if not isinstance(cwd, str) or not cwd:
        raise VerificationReceiptRejectedError(
            f"verification command {command_id!r} cwd must be a non-empty string"
        )
    if not isinstance(environment, Mapping) or not all(
        isinstance(key, str) and isinstance(value, str) for key, value in environment.items()
    ):
        raise VerificationReceiptRejectedError(
            f"verification command {command_id!r} environment must be a string-to-string map"
        )
    if not isinstance(timeout_seconds, int) or isinstance(timeout_seconds, bool):
        raise VerificationReceiptRejectedError(
            f"verification command {command_id!r} timeoutSeconds must be an integer"
        )
    return {
        "id": command_id,
        "argv": list(argv),
        "cwd": cwd,
        "timeoutSeconds": timeout_seconds,
        "environment": dict(sorted(environment.items())),
    }


def materialize_verification_profile(
    commands: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Normalize the repository's pinned commands into the frozen v1 Verification
    Profile's ordered command list.

    Declared order is preserved exactly: "Arrival order cannot add, remove, or
    renumber a command or slot" (review-and-consensus.md).
    """
    if not commands:
        raise VerificationReceiptRejectedError(
            "a Verification Profile requires at least one command"
        )
    normalized = [_normalize_command(command) for command in commands]
    seen_ids: set[str] = set()
    for command in normalized:
        if command["id"] in seen_ids:
            raise VerificationReceiptRejectedError(
                f"duplicate verification command id {command['id']!r}"
            )
        seen_ids.add(command["id"])
    return normalized


def command_invocation_digest(command: Mapping[str, Any]) -> str:
    """The ``invocation_digest`` a controller/worker recomputes for one pinned command."""
    return verification_command_invocation_digest(command)


def verification_profile_hash(commands: Sequence[Mapping[str, Any]]) -> str:
    """The frozen v1 Verification Profile's ``profile_hash``."""
    return verification_profile_digest(list(commands))


def verification_profile_from_effective_policy(
    effective_policy: Any,
) -> tuple[str, list[dict[str, Any]], str]:
    """Extract and materialize the pinned ``default`` Verification Profile from a
    Snapshot's effective policy document (``effective_policy["verification"]``,
    the same ``spec.verification`` shape ``project_bundle.py`` materializes).

    Returns ``(profile_id, commands, profile_hash)``.
    """
    verification = (
        effective_policy.get("verification") if isinstance(effective_policy, Mapping) else None
    )
    if not isinstance(verification, Mapping):
        raise VerificationReceiptRejectedError(
            "effective policy is missing a spec.verification Verification Profile"
        )
    profile_id = verification.get("profile", DEFAULT_VERIFICATION_PROFILE_ID)
    if profile_id != DEFAULT_VERIFICATION_PROFILE_ID:
        raise VerificationReceiptRejectedError(
            f"v1 has exactly one Verification Profile {DEFAULT_VERIFICATION_PROFILE_ID!r}, "
            f"got {profile_id!r}"
        )
    commands_raw = verification.get("commands")
    if not isinstance(commands_raw, Sequence) or isinstance(commands_raw, (str, bytes)):
        raise VerificationReceiptRejectedError(
            "effective policy verification.commands must be an array"
        )
    commands = materialize_verification_profile(commands_raw)
    return profile_id, commands, verification_profile_hash(commands)


def recompute_verification_outcome(checks: Sequence[Mapping[str, Any]]) -> str:
    """Recompute ``PASS``/``FAIL``/``ERROR`` from ``checks`` alone.

    - ``PASS``: every command ran to completion (``EXITED``) with exit code 0.
    - ``FAIL``: complete, trustworthy execution with at least one ordinary
      nonzero exit or process signal; later commands may legitimately be
      ``NOT_RUN`` once such a failure has occurred.
    - ``ERROR``: a ``TIMED_OUT`` termination, or a ``NOT_RUN`` that no earlier
      ordinary failure explains, or an unrecognized termination -- the
      required PASS/FAIL answer is unknown.
    """
    saw_ordinary_failure = False
    for check in checks:
        termination = check.get("termination")
        if termination == "EXITED":
            if check.get("exit_code") != 0:
                saw_ordinary_failure = True
        elif termination == "SIGNALED":
            saw_ordinary_failure = True
        elif termination == "NOT_RUN":
            if not saw_ordinary_failure:
                return "ERROR"
        else:
            # TIMED_OUT, or anything the schema did not already reject.
            return "ERROR"
    return "FAIL" if saw_ordinary_failure else "PASS"


def validate_verification_receipt(
    receipt: Any,
    *,
    expected_candidate_id: str,
    expected_commit: Mapping[str, Any],
    expected_profile_id: str,
    expected_profile_hash: str,
    expected_commands: Sequence[Mapping[str, Any]],
) -> str:
    """Validate a worker-submitted ``orcest.verification-receipt/1`` against the
    controller's own trusted Candidate/profile bindings, then independently
    recompute its outcome.

    Returns the recomputed, admitted outcome (``PASS``/``FAIL``/``ERROR``).
    Raises :class:`VerificationReceiptRejectedError` for anything that must
    reject the whole Attempt Result submission: a schema violation, a
    Candidate/profile/generation/assignment mismatch, or a worker-declared
    outcome that disagrees with the recomputed one.
    """
    try:
        validated = validate_envelope(receipt)
    except ProtocolValidationError as exc:
        raise VerificationReceiptRejectedError(str(exc)) from exc
    protocol = validated.get("protocol", validated.get("protocol_version"))
    if protocol != VERIFICATION_RECEIPT_PROTOCOL:
        raise VerificationReceiptRejectedError(
            f"VERIFY Attempt Results require {VERIFICATION_RECEIPT_PROTOCOL}, got {protocol!r}"
        )

    candidate = validated["candidate"]
    if candidate["candidate_id"] != expected_candidate_id:
        raise VerificationReceiptRejectedError(
            "verification receipt candidate_id does not match the bound Candidate"
        )
    commit = candidate["commit"]
    if (
        commit["object_format"] != expected_commit["object_format"]
        or commit["oid"] != expected_commit["oid"]
    ):
        raise VerificationReceiptRejectedError(
            "verification receipt commit does not match the bound Candidate"
        )
    if validated["profile_id"] != expected_profile_id:
        raise VerificationReceiptRejectedError(
            "verification receipt profile_id does not match the pinned Verification Profile"
        )
    if validated["profile_hash"] != expected_profile_hash:
        raise VerificationReceiptRejectedError(
            "verification receipt profile_hash does not match the pinned Verification Profile"
        )

    checks = validated["checks"]
    if len(checks) != len(expected_commands):
        raise VerificationReceiptRejectedError(
            "verification receipt checks do not match the pinned command count"
        )
    for index, (check, command) in enumerate(zip(checks, expected_commands)):
        if check["command_id"] != command["id"]:
            raise VerificationReceiptRejectedError(
                f"verification receipt checks[{index}] does not match the pinned command order"
            )
        if check["invocation_digest"] != command_invocation_digest(command):
            raise VerificationReceiptRejectedError(
                f"verification receipt checks[{index}] invocation_digest does not match "
                "the pinned command"
            )

    recomputed = recompute_verification_outcome(checks)
    if recomputed != validated["outcome"]:
        raise VerificationReceiptRejectedError(
            f"verification receipt declared outcome {validated['outcome']!r} does not match "
            f"the recomputed outcome {recomputed!r}"
        )
    return recomputed
