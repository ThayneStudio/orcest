"""Legacy/v1 pool, stream, and reaper isolation (operations-and-rollout.md).

Every Capacity Pool, VM template, pool-manager principal, Worker Session,
Redis ACL, and stream namespace belongs to exactly one of ``LEGACY`` or
``V1_CLONE_FIXED``. The legacy PEL reaper may reassign or ACK only explicit
legacy streams/groups. v1 session loss is accepted only through the
authenticated worker-loss endpoint.
"""

from __future__ import annotations

from collections.abc import Mapping

# Must match orcest.workflow_store.v1.offer_projection.OFFER_STREAM_PREFIX.
# Duplicated so fleet reaper isolation does not import the run-store writer.
V1_OFFER_STREAM_PREFIX = "tasks:activity:v1:"

__all__ = [
    "V1_OFFER_STREAM_PREFIX",
    "legacy_pel_denied",
    "is_legacy_pel_allowlisted",
    "is_v1_protocol_stream",
    "validate_capacity_pool_isolation",
]

_V1_MARKERS = (V1_OFFER_STREAM_PREFIX, f":{V1_OFFER_STREAM_PREFIX}")


def is_v1_protocol_stream(stream_key: str) -> bool:
    """True when ``stream_key`` is a v1 offer/protocol stream, with or without prefix."""
    return any(marker in stream_key for marker in _V1_MARKERS)


def _tasks_suffix(stream_key: str) -> tuple[str, ...] | None:
    parts = stream_key.split(":")
    try:
        index = parts.index("tasks")
    except ValueError:
        return None
    return tuple(parts[index:])


def is_legacy_pel_allowlisted(stream_key: str) -> bool:
    """True only for legacy worker task streams the PEL reaper may touch."""
    if is_v1_protocol_stream(stream_key):
        return False
    rest = _tasks_suffix(stream_key)
    if rest is None:
        return False
    if len(rest) == 2 and rest[1] not in {"issue", "activity"}:
        return True
    return len(rest) == 3 and rest[1] == "issue" and bool(rest[2])


def legacy_pel_denied(stream_key: str) -> bool:
    """True when a legacy reaper must refuse ACK/XCLAIM/synthetic Result."""
    return not is_legacy_pel_allowlisted(stream_key)


def validate_capacity_pool_isolation(
    *,
    template_class: str,
    template_id: str,
    pool_manager_principal_id: str,
    redis_acl_identity: str,
    redis_prefix: str,
    consumer_group: str,
    stream_namespace: str,
    reaper_authority: str,
    clone_credential_removal_attested: bool,
    existing: Mapping[str, tuple[str, str]],
) -> str | None:
    """Return a closed rejection code, or ``None`` when the pool is isolated.

    ``existing`` maps identity field name to ``(value, template_class)`` rows
    already committed. A field reused by the other class is dual-class and
    fail-closed.
    """
    if template_class == "V1_CLONE_FIXED":
        if reaper_authority != "V1_AUTHENTICATED_LOSS":
            return "REAPER_AUTHORITY_MISMATCH"
        if not clone_credential_removal_attested:
            return "CLONE_CREDENTIAL_REMOVAL_UNATTESTED"
        if not stream_namespace.startswith(V1_OFFER_STREAM_PREFIX):
            return "STREAM_NAMESPACE_CLASS_MISMATCH"
    elif template_class == "LEGACY":
        if reaper_authority != "LEGACY_PEL_ALLOWLIST":
            return "REAPER_AUTHORITY_MISMATCH"
        if is_v1_protocol_stream(stream_namespace):
            return "STREAM_NAMESPACE_CLASS_MISMATCH"
    else:
        return "UNKNOWN_TEMPLATE_CLASS"

    proposed = {
        "template_id": template_id,
        "pool_manager_principal_id": pool_manager_principal_id,
        "redis_acl_identity": redis_acl_identity,
        "redis_prefix": redis_prefix,
        "consumer_group": consumer_group,
        "stream_namespace": stream_namespace,
    }
    for field, value in proposed.items():
        prior = existing.get(f"{field}:{value}")
        if prior is None:
            continue
        prior_value, prior_class = prior
        if prior_value == value and prior_class != template_class:
            return "DUAL_CLASS_IDENTITY"
    return None
