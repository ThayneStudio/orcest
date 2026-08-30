"""Workflow-Control v1 versioned contract and digest registry.

This package is the single code-owned definition of the Workflow-Control v1
identities, enums, tagged unions, canonical JSON serialization, digests, and
protocol versions described in the normative wiki:

- ``docs/wiki/domain-model.md``
- ``docs/wiki/workflow-lifecycle.md``
- ``docs/wiki/worker-protocol.md``

Every other component (SQLite migrations, HTTP handlers, Redis envelopes,
forge adapters) MUST import identities, enums, canonicalization, digests, and
protocol-version literals from here rather than redefining them. This is
enforced by ``tests/workflow_contract/test_no_shadow_contracts.py``.

Nothing in this package implements lifecycle transition logic: it defines the
closed vocabulary and wire contracts that lifecycle logic (implemented by
other Workflow Control v1 issues) is built on top of.
"""

from orcest.workflow_contract.v1 import (
    digest as digest,
    enums as enums,
    protocol as protocol,
    protocol_registry as protocol_registry,
)
from orcest.workflow_contract.v1.canonical import canonical_json_bytes, canonical_json_text
from orcest.workflow_contract.v1.digest import content_digest, is_valid_content_digest
from orcest.workflow_contract.v1.identity import is_lowercase_uuid, require_lowercase_uuid
from orcest.workflow_contract.v1.protocol import (
    ProtocolValidationError,
    validate_envelope,
)

__all__ = [
    "digest",
    "enums",
    "protocol",
    "protocol_registry",
    "canonical_json_bytes",
    "canonical_json_text",
    "content_digest",
    "is_valid_content_digest",
    "is_lowercase_uuid",
    "require_lowercase_uuid",
    "ProtocolValidationError",
    "validate_envelope",
]
