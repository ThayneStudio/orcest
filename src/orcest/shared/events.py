"""CloudEvents-shaped orcest event envelopes and the spool publisher.

Spec: docs/superpowers/specs/2026-08-17-stall-detection-and-monitor-design.md §8-§9.
The taxonomy is locked (additive-only after v1). Envelope field names follow
CloudEvents 1.0 so ``(source, id)`` is the end-to-end idempotency key.

Events must never carry raw tool arguments/output, prompts, or credentials —
only names, hashes, error classes, and counters (redaction rule, spec §8).
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from orcest.shared.redis_client import RedisClient

logger = logging.getLogger(__name__)

EVENTS_STREAM = "events"
DEFAULT_EVENTS_MAXLEN = 50000

_TYPE_SUFFIXES = (
    "task.enqueued",
    "task.started",
    "task.bootstrap",
    "task.active",
    "task.waiting",
    "task.suspect",
    "task.stuck",
    "task.looping",
    "task.killed",
    "task.completed",
    "task.failed",
    "task.reaped",
    "task.activity",
    "fleet.pressure",
    "fleet.kill_limit",
)
EVENT_TYPES: frozenset[str] = frozenset("net.orcest." + s for s in _TYPE_SUFFIXES)

# Payload keys make_event() always sets itself. A caller-supplied `data` dict
# that reuses one of these would silently clobber the identity fields set
# below (dict.update() has no notion of "already occupied"), so make_event
# rejects any collision instead of overwriting silently.
_RESERVED_DATA_KEYS = frozenset({"work", "attempt", "head_sha", "worker_id", "provider"})


def _now_rfc3339() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def make_event(
    event_type: str,
    *,
    source_project: str,
    task_id: str,
    repo: str,
    resource_type: str,
    resource_id: int,
    attempt: int,
    head_sha: str = "",
    worker_id: str = "",
    provider: str = "",
    data: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a v1 envelope. ``data`` extras are merged after identity fields."""
    if event_type not in EVENT_TYPES:
        raise ValueError(f"unknown event type: {event_type}")
    if data:
        collision = _RESERVED_DATA_KEYS.intersection(data)
        if collision:
            raise ValueError(
                f"data key {sorted(collision)[0]!r} collides with a reserved "
                "make_event payload field (work/attempt/head_sha/worker_id/provider)"
            )
    payload: dict[str, Any] = {
        "work": {
            "repo": repo,
            "resource_type": resource_type,
            "resource_id": resource_id,
        },
        "attempt": attempt,
        "head_sha": head_sha,
        "worker_id": worker_id,
        "provider": provider,
    }
    if data:
        payload.update(data)
    return {
        "id": str(uuid.uuid4()),
        "source": f"urn:orcest:{source_project}",
        "type": event_type,
        "subject": task_id,
        "time": _now_rfc3339(),
        "data": payload,
    }


class EventPublisher:
    """Fire-and-forget spool writer. Never raises into the caller."""

    def __init__(self, redis: RedisClient, maxlen: int = DEFAULT_EVENTS_MAXLEN) -> None:
        self._redis = redis
        self._maxlen = maxlen
        self._error_count = 0

    def publish(self, envelope: dict[str, Any]) -> None:
        try:
            self._redis.xadd_capped(EVENTS_STREAM, {"envelope": json.dumps(envelope)}, self._maxlen)
        except Exception:
            # Decimated logging, mirroring worker output-stream error handling.
            self._error_count += 1
            if self._error_count in (1, 10, 100) or self._error_count % 1000 == 0:
                logger.warning(
                    "event publish failed (%d failures so far)",
                    self._error_count,
                    exc_info=True,
                )
