"""Redis projection for durable Activity/Attempt offers (Workflow-Control v1).

SQLite is the sole authority for Activity/Attempt identity and assignment;
Redis is a disposable, reconstructible cache of currently-claimable offers.
This module drains ``PENDING`` ``ACTIVITY``-sourced outbox rows into a
per-Worker-Profile Redis stream as the flat non-secret activity-offer
envelope (see :func:`activity_offer_protocol`), and republishes every
durable, current ``OFFERED`` Attempt after Redis loss. A ``CLAIMED`` Attempt is never
republished as schedulable work -- its worker re-establishes liveness,
reports a result, or reaches its durable deadline instead.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from orcest.shared.redis_client import RedisClient
from orcest.workflow_contract.v1.protocol import (
    get_envelope_schema,
    known_protocol_literals,
    validate_object,
)
from orcest.workflow_store.store import OutboxRecord, RunStore

__all__ = [
    "OFFER_STREAM_PREFIX",
    "activity_offer_protocol",
    "dispatch_pending_offers",
    "offer_stream_key",
    "reconstruct_open_offers",
]

OFFER_STREAM_PREFIX = "tasks:activity:v1:"


def activity_offer_protocol() -> str:
    """The sole registered ``orcest.activity-offer/<n>`` literal.

    Resolved by prefix, never hard-coded here, so this module stays in step
    with whichever version :mod:`orcest.workflow_contract.v1.protocol_registry`
    -- the one file permitted to register a protocol literal -- currently
    defines.
    """
    matches = sorted(
        literal
        for literal in known_protocol_literals()
        if literal.startswith("orcest.activity-offer/")
    )
    if len(matches) != 1:
        raise RuntimeError(f"expected one activity-offer protocol, got {matches!r}")
    return matches[0]


def offer_stream_key(worker_profile: str) -> str:
    """The v1 Redis stream for one Worker Profile's claimable offers."""
    return f"{OFFER_STREAM_PREFIX}{worker_profile}"


def _envelope_fields(outbox_payload: Mapping[str, Any], *, redis_epoch: int) -> dict[str, str]:
    protocol = activity_offer_protocol()
    envelope = {
        "protocol": protocol,
        "protocol_version": "1",
        "redis_epoch": redis_epoch,
        "outbox_id": outbox_payload["outbox_id"],
        "attempt_id": outbox_payload["attempt_id"],
        "activity_id": outbox_payload["activity_id"],
        "generation": outbox_payload["generation"],
        "worker_profile": outbox_payload["worker_profile"],
        "claim_deadline_ms": outbox_payload["claim_deadline_ms"],
    }
    schema = get_envelope_schema(protocol)
    validate_object(envelope, schema.schema, path=protocol)
    return {key: str(value) for key, value in envelope.items()}


def _publish(store: RunStore, redis: RedisClient, row: OutboxRecord, *, redis_epoch: int) -> None:
    payload = json.loads(row.payload_json)
    fields = _envelope_fields(payload, redis_epoch=redis_epoch)
    stream_key = offer_stream_key(payload["worker_profile"])
    entry_id = redis.xadd(stream_key, fields)
    store.mark_outbox_redis_delivered(
        row.outbox_id, redis_epoch=redis_epoch, redis_entry_id=entry_id
    )


def dispatch_pending_offers(
    store: RunStore, redis: RedisClient, *, redis_epoch: int, limit: int = 100
) -> int:
    """Drain due ``PENDING`` Activity offers into Redis; mark them ``DELIVERED``.

    A timeout after a successful Redis append may cause a retry to publish an
    identical notification for the same outbox row -- this is safe because
    the claim endpoint deduplicates by durable Attempt identity and
    generation, never by Redis entry ID. Returns the number dispatched.
    """
    dispatched = 0
    for row in store.list_pending_activity_offers(limit=limit):
        current = store.get_outbox(row.outbox_id)
        if current is None or current.state != "PENDING":
            continue
        _publish(store, redis, current, redis_epoch=redis_epoch)
        dispatched += 1
    return dispatched


def reconstruct_open_offers(store: RunStore, redis: RedisClient, *, redis_epoch: int) -> int:
    """Republish every current, unexpired ``OFFERED`` Attempt with a fresh epoch.

    Called after a Redis flush/restart once the caller has recreated this
    controller's stream namespace. Republishing uses the durable outbox
    row's stable payload regardless of any prior Redis entry ID -- the
    ``redis_epoch`` written here is diagnostic reconstruction metadata, never
    a claim or result fence.
    """
    republished = 0
    for _attempt, outbox_record in store.list_open_activity_offers():
        _publish(store, redis, outbox_record, redis_epoch=redis_epoch)
        republished += 1
    return republished
