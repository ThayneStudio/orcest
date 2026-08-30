"""Golden digest vectors: byte-exact, cross-process-stable output.

Issue #668 acceptance criterion: "Golden canonicalization and digest vectors
are stable across processes." These vectors were computed once by calling
the domain-separated digest helpers directly and are checked in; any future
change to canonicalization or a digest formula that alters output bytes will
break this test, which is the point.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from orcest.workflow_contract.v1 import digest

FIXTURE_PATH = Path(__file__).parent / "golden" / "digest_vectors.json"


def _load_vectors() -> list[dict[str, Any]]:
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


def _call(fn_name: str, args: dict[str, Any]) -> str:
    if fn_name == "workflow_blob_digest":
        return digest.workflow_blob_digest(
            args["media_kind"], bytes.fromhex(args["normalized_bytes_hex"])
        )
    if fn_name == "capability_public_key_digest":
        return digest.capability_public_key_digest(
            bytes.fromhex(args["public_verification_key_hex"]),
            signature_algorithm=args["signature_algorithm"],
        )
    if fn_name == "launch_capability_claims_digest":
        return digest.launch_capability_claims_digest(args["canonical_claims"])
    if fn_name == "subject_refs_digest":
        return digest.subject_refs_digest(args["subject_refs"])
    if fn_name == "review_assignment_digest":
        return digest.review_assignment_digest(**args)
    if fn_name == "health_scope_digest":
        return digest.health_scope_digest(args["scope_identity"])
    raise AssertionError(f"unhandled fixture function: {fn_name}")


@pytest.mark.parametrize("vector", _load_vectors(), ids=lambda v: v["name"])
def test_golden_digest_vector(vector: dict[str, Any]) -> None:
    actual = _call(vector["fn"], vector["args"])
    assert actual == vector["expected_digest"]
    assert digest.is_valid_content_digest(actual)


def test_golden_vectors_are_unique() -> None:
    vectors = _load_vectors()
    digests = [v["expected_digest"] for v in vectors]
    assert len(digests) == len(set(digests)), "golden vectors must not collide"


def test_media_kind_changes_workflow_blob_digest() -> None:
    vectors = {v["name"]: v for v in _load_vectors()}
    config = vectors["workflow_blob_digest_config_json"]
    policy = vectors["workflow_blob_digest_policy_json_same_bytes"]
    assert config["args"]["normalized_bytes_hex"] == policy["args"]["normalized_bytes_hex"]
    assert config["expected_digest"] != policy["expected_digest"]


def test_digest_stable_across_repeated_invocation() -> None:
    for vector in _load_vectors():
        first = _call(vector["fn"], vector["args"])
        second = _call(vector["fn"], vector["args"])
        assert first == second == vector["expected_digest"]
