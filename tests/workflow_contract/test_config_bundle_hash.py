"""Tests for orcest.workflow_contract.v1.digest.config_bundle_hash.

No worked example for this exact hash appears in the wiki (unlike
``workflow_blob_digest``'s golden vectors), so these tests cross-check the
implementation against a hand-rolled reference computation of the documented
formula, plus the determinism/sensitivity properties the acceptance criteria
require.
"""

from __future__ import annotations

import hashlib

from orcest.workflow_contract.v1.canonical import canonical_json_bytes
from orcest.workflow_contract.v1.digest import CONFIG_BUNDLE_DOMAIN_LINE, config_bundle_hash


def _reference_hash(value: object) -> str:
    payload = CONFIG_BUNDLE_DOMAIN_LINE.encode("utf-8") + canonical_json_bytes(value)
    return f"sha256:{hashlib.sha256(payload).hexdigest()}"


def test_config_bundle_hash_matches_reference_formula() -> None:
    value = {"b": 2, "a": [1, 2, 3], "nested": {"z": True, "y": None}}
    assert config_bundle_hash(value) == _reference_hash(value)


def test_config_bundle_hash_domain_line_is_exact() -> None:
    assert CONFIG_BUNDLE_DOMAIN_LINE == "orcest-config-bundle/v1\n"


def test_config_bundle_hash_deterministic_regardless_of_key_order() -> None:
    a = {"x": 1, "y": 2}
    b = {"y": 2, "x": 1}
    assert config_bundle_hash(a) == config_bundle_hash(b)


def test_config_bundle_hash_sensitive_to_byte_changes() -> None:
    a = {"files": {"a.md": {"digest": "sha256:" + "0" * 64}}}
    b = {"files": {"a.md": {"digest": "sha256:" + "1" * 64}}}
    assert config_bundle_hash(a) != config_bundle_hash(b)


def test_config_bundle_hash_domain_separated_from_generic_policy_digest() -> None:
    from orcest.workflow_contract.v1.digest import policy_digest

    value = {"same": "shape"}
    assert config_bundle_hash(value) != policy_digest(value)
