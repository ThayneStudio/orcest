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
