"""Domain-separated digest helpers for Workflow-Control v1.

Content digests in v1 use lowercase ``sha256:<64 hexadecimal digits>``
(see the "Representation conventions" section of ``domain-model.md``).

Two digest shapes appear in the normative spec:

1. **Explicit byte-level domain-separated formulas.** These hash a fixed
   preamble of ``ascii(domain_tag) || 0x00``, zero or more ASCII literal
   fields each terminated by ``0x00``, and then a length-prefixed
   (``uint64_be``) trailing variable-length byte string. Every explicit
   formula in the wiki (Workflow Blob's ``blob_digest``, Capability Signing
   Key's ``public_key_digest``, the Attempt Claim/Launch Attestation's
   ``launch_capability_digest``) follows exactly this shape --
   :func:`domain_digest` implements it generically and the named functions
   below bind it to each entity's exact domain tag.

2. **Bare canonical-JSON digests** (``sha256(canonical_json({...}))``, e.g.
   ``subject_refs_digest``, ``assignment_digest``). The wiki gives these no
   extra domain tag because the hashed JSON object's own field shape (a
   discriminant field, or a context-specific array) already prevents
   cross-entity collision within that one call site. Implementing these
   verbatim (no added tag) is required for exact interoperability with the
   spec's worked digest values.

Where the wiki requires "the digest of the complete canonical X" without
giving an explicit byte formula (e.g. Policy Update's ``policy_hash``,
Attempt Result's ``result_digest``, a request/response ``request_digest`` /
``response_digest``), this module still MUST NOT invent a bespoke, ad hoc
``hashlib.sha256`` call at the use site: it defines one uniform
domain-separated envelope, :func:`generic_domain_digest`, so that two
different semantic digests can never collide merely because they happened to
hash byte-identical canonical JSON. Every such digest in the codebase MUST be
produced by a named function in this module.

``tests/workflow_contract/test_no_shadow_contracts.py`` enforces that no
other file in the repository calls ``hashlib.sha256``/``hashlib.new("sha256"``
directly -- every sha256 content digest is produced here.
"""

from __future__ import annotations

import hashlib
import hmac
import re
import struct
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from orcest.workflow_contract.v1.canonical import canonical_json_bytes

__all__ = [
    "CONTENT_DIGEST_RE",
    "is_valid_content_digest",
    "require_valid_content_digest",
    "sha256_hex",
    "content_digest",
    "domain_digest",
    "generic_domain_digest",
    "bare_canonical_digest",
    "CONFIG_BUNDLE_DOMAIN_LINE",
    "config_bundle_hash",
    "WORKFLOW_BLOB_DOMAIN",
    "workflow_blob_digest",
    "CAPABILITY_PUBLIC_KEY_DOMAIN",
    "capability_public_key_digest",
    "LAUNCH_CAPABILITY_CLAIMS_DOMAIN",
    "launch_capability_claims_digest",
    "WORK_ITEM_DISCOVERY_SET_DOMAIN",
    "work_item_discovery_set_digest",
    "CHANGE_REQUEST_SEARCH_SET_DOMAIN",
    "change_request_search_set_digest",
    "HEALTH_SCOPE_DOMAIN",
    "health_scope_digest",
    "subject_refs_digest",
    "review_assignment_digest",
    "policy_digest",
    "result_digest",
    "request_digest",
    "response_digest",
    "specification_digest",
    "hmac_sha256",
    "hmac_sha256_hex",
    "SECRET_VERSION_INTEGRITY_DOMAIN",
    "secret_version_integrity_preimage",
    "secret_version_integrity_tag",
    "SECRET_STAGING_ATTESTATION_DOMAIN",
    "secret_staging_attestation_preimage",
    "secret_staging_attestation",
    "transition_digest",
    "activity_idempotency_digest",
    "receipt_digest",
    "checkpoint_digest",
    "affected_run_ids_digest",
    "failure_evidence_digest",
]

CONTENT_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")


def is_valid_content_digest(value: object) -> bool:
    return isinstance(value, str) and bool(CONTENT_DIGEST_RE.fullmatch(value))


def require_valid_content_digest(value: object, *, field: str = "digest") -> str:
    if not is_valid_content_digest(value):
        raise ValueError(f"{field} must match sha256:<64 lowercase hex>, got {value!r}")
    assert isinstance(value, str)
    return value


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def content_digest(data: bytes) -> str:
    """Return the ``sha256:<hex>`` content digest of raw preimage bytes."""
    return f"sha256:{sha256_hex(data)}"


def _u64_be(n: int) -> bytes:
    if not isinstance(n, int) or isinstance(n, bool) or n < 0 or n > 0xFFFFFFFFFFFFFFFF:
        raise ValueError(f"value is not representable as an unsigned 64-bit integer: {n!r}")
    return struct.pack(">Q", n)


def domain_digest(domain_tag: str, *, literals: Sequence[str] = (), payload: bytes) -> str:
    """Generic implementation of the wiki's explicit byte-level digest shape.

    Computes::

        sha256:<hex(SHA256(
            ascii(domain_tag) || 0x00 ||
            (ascii(literal) || 0x00 for literal in literals) ||
            uint64_be(len(payload)) || payload
        ))>
    """
    parts = [domain_tag.encode("ascii"), b"\x00"]
    for literal in literals:
        parts.append(literal.encode("ascii"))
        parts.append(b"\x00")
    parts.append(_u64_be(len(payload)))
    parts.append(payload)
    return content_digest(b"".join(parts))


def bare_canonical_digest(value: Any) -> str:
    """``sha256(canonical_json(value))`` with no domain tag.

    Reserved for the small, spec-enumerated set of digests whose normative
    formula is exactly this bare form (``subject_refs_digest``,
    ``assignment_digest``, and similar tagged-union-discriminated objects).
    New call sites should prefer :func:`generic_domain_digest` unless they
    are implementing one of those exact named formulas.
    """
    return content_digest(canonical_json_bytes(value))


def generic_domain_digest(domain_tag: str, value: Any) -> str:
    """Domain-separated digest of a canonical JSON payload.

    Computes ``sha256(ascii(domain_tag) || 0x00 || canonical_json(value))``.
    This is the module's uniform envelope for every "digest of the complete
    canonical X" requirement in the wiki that does not specify its own exact
    byte formula (policy, evidence, results, requests, responses,
    specifications). It guarantees two different named digests never collide
    merely because their canonical JSON payloads happen to be byte-identical.
    """
    return domain_digest(domain_tag, payload=canonical_json_bytes(value))


# --- Explicit byte-level formulas from the wiki -----------------------------

CONFIG_BUNDLE_DOMAIN_LINE = "orcest-config-bundle/v1\n"


def config_bundle_hash(normalized_bundle_json: Any) -> str:
    """The repository-configuration workflow hash (``docs/wiki/repository-configuration.md``,
    "Normalization and bundle hash").

    This is the one exception to the two shapes described in this module's
    docstring: the wiki's exact formula prepends a literal newline-terminated
    domain line to the canonical JSON bytes with no ``0x00`` separator and no
    length prefix, rather than either :func:`domain_digest`'s or
    :func:`generic_domain_digest`'s envelope::

        "sha256:" + lowercase_hex(
          SHA256(UTF8("orcest-config-bundle/v1\\n") || canonical_json_bytes)
        )
    """
    payload = CONFIG_BUNDLE_DOMAIN_LINE.encode("utf-8") + canonical_json_bytes(
        normalized_bundle_json
    )
    return content_digest(payload)


WORKFLOW_BLOB_DOMAIN = "orcest-workflow-blob-v1"


def workflow_blob_digest(media_kind: str, normalized_bytes: bytes) -> str:
    """``blob_digest`` for a Workflow Blob (domain-model.md, "Workflow Blob").

    ``blob_digest = sha256:hex(SHA256(
        ascii("orcest-workflow-blob-v1") || 0x00 ||
        utf8(media_kind) || 0x00 ||
        uint64_be(byte_length) ||
        normalized_bytes
    ))``
    """
    return domain_digest(WORKFLOW_BLOB_DOMAIN, literals=[media_kind], payload=normalized_bytes)


CAPABILITY_PUBLIC_KEY_DOMAIN = "orcest-capability-public-key-v1"


def capability_public_key_digest(
    public_verification_key: bytes, *, signature_algorithm: str = "ED25519"
) -> str:
    """``public_key_digest`` for a Capability Signing Key.

    ``public_key_digest = sha256:hex(SHA256(
        ascii("orcest-capability-public-key-v1") || 0x00 ||
        ascii("ED25519") || 0x00 || uint64_be(32) || public_verification_key
    ))``
    """
    if len(public_verification_key) != 32:
        raise ValueError(
            "public_verification_key must be exactly 32 bytes (Ed25519), "
            f"got {len(public_verification_key)}"
        )
    return domain_digest(
        CAPABILITY_PUBLIC_KEY_DOMAIN,
        literals=[signature_algorithm],
        payload=public_verification_key,
    )


LAUNCH_CAPABILITY_CLAIMS_DOMAIN = "orcest-launch-capability-claims-v1"


def launch_capability_claims_digest(canonical_claims: Mapping[str, Any]) -> str:
    """``launch_capability_digest`` over normalized ``orcest.launch-capability/1`` claims.

    ``sha256:hex(SHA256(
        ascii("orcest-launch-capability-claims-v1") || 0x00 ||
        uint64_be(byte_length(canonical_claims_json)) || canonical_claims_json
    ))``
    """
    payload = canonical_json_bytes(canonical_claims)
    return domain_digest(LAUNCH_CAPABILITY_CLAIMS_DOMAIN, payload=payload)


WORK_ITEM_DISCOVERY_SET_DOMAIN = "orcest-work-item-discovery-set-v1"


def work_item_discovery_set_digest(member_rows: Any) -> str:
    """Digest of a Work Item Discovery membership set.

    ``sha256(ascii("orcest-work-item-discovery-set-v1") || 0x00 || canonical_json(member_rows))``
    """
    return generic_domain_digest(WORK_ITEM_DISCOVERY_SET_DOMAIN, member_rows)


CHANGE_REQUEST_SEARCH_SET_DOMAIN = "orcest-change-request-search-set-v1"


def change_request_search_set_digest(member_rows: Any) -> str:
    """Digest of a Change Request Search membership set.

    Same shape as the work-item-discovery set.
    """
    return generic_domain_digest(CHANGE_REQUEST_SEARCH_SET_DOMAIN, member_rows)


HEALTH_SCOPE_DOMAIN = "orcest-health-scope-v1"


def health_scope_digest(scope_identity: Any) -> str:
    """``scope_id`` for a Health Probe/Observation scope.

    ``ascii("orcest-health-scope-v1") || 0x00 || canonical_json(scope_identity)``
    """
    return generic_domain_digest(HEALTH_SCOPE_DOMAIN, scope_identity)


# --- Bare canonical-JSON formulas (no domain tag, per exact wiki wording) ---


def subject_refs_digest(subject_refs: Iterable[str]) -> str:
    """``subject_refs_digest = sha256(canonical_json([subject_ref, ...]))``."""
    return bare_canonical_digest(list(subject_refs))


def review_assignment_digest(
    *,
    assignment_kind: str,
    panel_round: int,
    reviewer_slot: str | None,
    adjudication_round: int | None,
    adjudicator_slot: str | None,
    role: str,
    subject_refs_digest: str,
    context_digest: str,
    disputed_finding_ids_digest: str | None,
) -> str:
    """``assignment_digest`` for an Activity Review Assignment.

    Excludes the relational ``activity_id`` (see domain-model.md, "Activity
    Review Assignment") to avoid a cycle with the Activity idempotency key.
    """
    return bare_canonical_digest(
        {
            "assignment_kind": assignment_kind,
            "panel_round": panel_round,
            "reviewer_slot": reviewer_slot,
            "adjudication_round": adjudication_round,
            "adjudicator_slot": adjudicator_slot,
            "role": role,
            "subject_refs_digest": subject_refs_digest,
            "context_digest": context_digest,
            "disputed_finding_ids_digest": disputed_finding_ids_digest,
        }
    )


# --- Generic domain-separated envelopes for un-formalized "digest of X" ----
#
# The wiki specifies these only as "the digest of the complete canonical X";
# it does not give an exact byte formula. Centralizing them here, each under
# its own domain tag, is what satisfies "domain-separated digest helpers for
# ... policy, assignments, evidence, results, and requests" without
# duplicating an ad hoc hashlib.sha256 call at every use site.


def policy_digest(effective_policy_json: Any) -> str:
    """Digest of a complete canonical effective ``POLICY_JSON`` document (``policy_hash``)."""
    return generic_domain_digest("orcest-policy-v1", effective_policy_json)


def specification_digest(specification_json: Any) -> str:
    """Digest of normalized specification inputs (``specification_hash``: title/body/comments)."""
    return generic_domain_digest("orcest-specification-v1", specification_json)


def result_digest(result_json: Any) -> str:
    """Digest of a complete canonical semantic Result body (``result_digest``)."""
    return generic_domain_digest("orcest-result-v1", result_json)


def request_digest(request_json: Any) -> str:
    """Digest of a complete canonical immutable request/CAS field set (``request_digest``)."""
    return generic_domain_digest("orcest-request-v1", request_json)


_ACTIVITY_IDEMPOTENCY_FIELDS = (
    "reducer_version",
    "run_id",
    "specification_generation",
    "policy_hash",
    "created_transition_sequence",
    "kind",
    "execution_class",
    "semantic_input_digest",
    "candidate_id",
    "forge_observation_id",
    "role",
    "repair_cycle",
    "recovery_cycle",
    "strategy_index",
    "recovery_tactic",
    "recovery_evidence_id",
    "rescue_epoch",
)


def activity_idempotency_digest(fields: Mapping[str, Any]) -> str:
    """Activity ``idempotency_key`` as ``sha256(canonical_json({...exact field set...}))``."""
    if set(fields) != set(_ACTIVITY_IDEMPOTENCY_FIELDS):
        raise ValueError(
            "activity idempotency preimage must contain exactly the domain field set, "
            f"got {sorted(fields)!r}"
        )
    return bare_canonical_digest({name: fields[name] for name in _ACTIVITY_IDEMPOTENCY_FIELDS})


def transition_digest(fields: Mapping[str, Any]) -> str:
    """Digest of normalized reducer inputs and outputs (``transition_digest``)."""
    return generic_domain_digest("orcest-transition-v1", fields)


def receipt_digest(fields: Mapping[str, Any]) -> str:
    """Digest of complete canonical Credential Rotation Receipt provenance (``receipt_digest``)."""
    return generic_domain_digest("orcest-credential-rotation-receipt-v1", fields)


def checkpoint_digest(fields: Mapping[str, Any]) -> str:
    """Digest of normalized Secret Provision Checkpoint fields (``checkpoint_digest``)."""
    return generic_domain_digest("orcest-secret-provision-checkpoint-v1", fields)


def affected_run_ids_digest(member_rows: Sequence[Mapping[str, Any]]) -> str:
    """Digest of a Secret Version's frozen active-Run membership (``affected_run_ids_digest``).

    Required even when the membership is empty (no Run currently waits on this
    Secret). The exact byte-sorted ``(secret_id, version, run_ordinal, run_id)``
    membership rows are owned by the Wait Condition / Human Boundary fanout leaf;
    this leaf only ever freezes the empty set, via the same named function later
    leaves must reuse so the formula never diverges by call site.
    """
    return generic_domain_digest("orcest-affected-run-ids-v1", list(member_rows))


def failure_evidence_digest(evidence: Any) -> str:
    """Digest of complete canonical non-secret failure evidence (a Checkpoint's
    ``failure_evidence_digest``). ``evidence`` MUST NOT contain secret bytes,
    a reusable Secret Store locator, or a secret-derived unkeyed digest."""
    return generic_domain_digest("orcest-failure-evidence-v1", evidence)


def response_digest(response_json: Any) -> str:
    """Digest of a complete canonical terminal replay response (``response_digest``).

    Per the wiki, the transport-only ``replayed`` projection is always
    excluded from the preimage; callers must strip it before calling this
    function.
    """
    if isinstance(response_json, Mapping) and "replayed" in response_json:
        raise ValueError(
            "response_digest preimage must exclude the transport-only 'replayed' field"
        )
    return generic_domain_digest("orcest-response-v1", response_json)


# --- Keyed integrity authenticators (Secret Store only) ---------------------
#
# Persistence requires a domain-separated keyed authenticator over the
# canonical Secret Version key and exact stored bytes, using a controller-only
# Secret Store integrity key. The resulting tag MUST remain inside the Secret
# Store: SQLite may store only an opaque attestation UUID, never the tag, an
# unkeyed secret-derived digest, or the secret bytes.


def hmac_sha256(key: bytes, payload: bytes) -> bytes:
    """HMAC-SHA256(key, payload) as raw bytes.

    The Secret Store uses this instead of an unkeyed SHA-256 of secret bytes so
    that ordinary database pages, Redis payloads, and logs cannot be grepped
    for a secret by hashing candidate values.
    """
    if not isinstance(key, (bytes, bytearray)) or not key:
        raise ValueError("HMAC key must be non-empty bytes")
    if not isinstance(payload, (bytes, bytearray)):
        raise TypeError(f"HMAC payload must be bytes, got {type(payload)!r}")
    return hmac.new(bytes(key), bytes(payload), "sha256").digest()


def hmac_sha256_hex(key: bytes, payload: bytes) -> str:
    return hmac_sha256(key, payload).hex()


SECRET_VERSION_INTEGRITY_DOMAIN = "orcest-secret-version-v1"


def secret_version_integrity_preimage(
    *, secret_id: str, version: int, secret_bytes: bytes
) -> bytes:
    """Preimage for the Secret Version keyed integrity authenticator.

    ``ascii("orcest-secret-version-v1") || 0x00 || ascii(secret_id) || 0x00 ||
    uint64_be(version) || uint64_be(byte_length) || secret_bytes``
    """
    if not isinstance(secret_id, str) or not secret_id:
        raise ValueError("secret_id must be a non-empty string")
    if not isinstance(version, int) or isinstance(version, bool) or version < 1:
        raise ValueError(f"version must be a positive integer, got {version!r}")
    if not isinstance(secret_bytes, (bytes, bytearray)):
        raise TypeError(f"secret_bytes must be bytes, got {type(secret_bytes)!r}")
    return b"".join(
        (
            SECRET_VERSION_INTEGRITY_DOMAIN.encode("ascii"),
            b"\x00",
            secret_id.encode("ascii"),
            b"\x00",
            _u64_be(version),
            _u64_be(len(secret_bytes)),
            bytes(secret_bytes),
        )
    )


def secret_version_integrity_tag(
    integrity_key: bytes,
    *,
    secret_id: str,
    version: int,
    secret_bytes: bytes,
) -> str:
    """Return the lowercase-hex keyed integrity tag for one Secret Version.

    This value is controller-only Secret Store metadata. It is not a content
    digest and MUST NOT be written to SQLite, Redis, logs, or API bodies.
    """
    return hmac_sha256_hex(
        integrity_key,
        secret_version_integrity_preimage(
            secret_id=secret_id, version=version, secret_bytes=secret_bytes
        ),
    )


SECRET_STAGING_ATTESTATION_DOMAIN = "orcest-secret-staging-v1"


def secret_staging_attestation_preimage(*, staging_id: str, secret_bytes: bytes) -> bytes:
    """Preimage for an operation-bound Secret Store staging attestation.

    ``ascii("orcest-secret-staging-v1") || 0x00 || ascii(staging_id) || 0x00 ||
    uint64_be(byte_length) || secret_bytes``
    """
    if not isinstance(staging_id, str) or not staging_id:
        raise ValueError("staging_id must be a non-empty string")
    if not isinstance(secret_bytes, (bytes, bytearray)):
        raise TypeError(f"secret_bytes must be bytes, got {type(secret_bytes)!r}")
    return b"".join(
        (
            SECRET_STAGING_ATTESTATION_DOMAIN.encode("ascii"),
            b"\x00",
            staging_id.encode("ascii"),
            b"\x00",
            _u64_be(len(secret_bytes)),
            bytes(secret_bytes),
        )
    )


def secret_staging_attestation(
    integrity_key: bytes, *, staging_id: str, secret_bytes: bytes
) -> str:
    """Keyed equality attestation for staged secret bytes.

    Proves byte identity for a staging id without an unkeyed digest of the
    secret. The hex tag stays in Secret Store incoming metadata.
    """
    return hmac_sha256_hex(
        integrity_key,
        secret_staging_attestation_preimage(staging_id=staging_id, secret_bytes=secret_bytes),
    )
