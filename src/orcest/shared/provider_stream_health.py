"""Pure dwell-based detector for stranded provider task streams.

A provider stream is *stranded* when it carries pending or lagging work
but none of its registered consumers is backed by a live worker
heartbeat. ``PoolManager`` (see ``fleet/pool_manager.py``) is the single
transition owner: it reads Redis, feeds the raw numbers into
``ProviderStreamHealthTracker.evaluate``, and publishes the resulting
snapshot. This module performs no I/O and takes an explicit clock so the
state machine can be tested without a real Redis or wall-clock sleeps.

Each provider has two independently evaluated streams -- the PR-task
stream (``tasks:{provider}``) and the issue-task stream
(``tasks:issue:{provider}``). Tracker state is keyed by
``(provider, stream)`` so dwell timers, ``transitioned_at``, and health
cannot leak between those members. Published snapshots use
``stream_health_snapshot_key`` (``provider-stream-health:{provider}:pr``
and ``...:issue``) rather than a provider-only key, so a stranded member
cannot be overwritten or hidden by its sibling. There is no provider-level
aggregate: consumers render the per-stream snapshots as published.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Literal

# Required dwell before a raw-stranded reading becomes the officially
# published "stranded" state. Short blips (a worker mid-rotation, a
# reconcile pass racing a fresh claim) must not page anyone.
DEFAULT_STRANDED_DWELL_SECONDS = 300.0

# Global (cross-project, unprefixed) snapshot key prefix. PoolManager is
# the single writer; ``orcest status`` and the live dashboard only ever
# consume keys under this prefix, never recompute health.
STREAM_HEALTH_KEY_PREFIX = "provider-stream-health:"
STREAM_HEALTH_KIND_PR = "pr"
STREAM_HEALTH_KIND_ISSUE = "issue"

TransitionKind = Literal["stranded", "recovered"] | None


def stream_health_snapshot_key(provider: str, *, issue: bool = False) -> str:
    """Return the unprefixed Redis key for one provider stream-health snapshot.

    Keys are ``provider-stream-health:{provider}:pr`` and
    ``provider-stream-health:{provider}:issue``. Provider names cannot
    contain ``:`` (see ``require_valid_provider_name``), so a PR key
    cannot collide with an issue key or with a different provider.
    """
    kind = STREAM_HEALTH_KIND_ISSUE if issue else STREAM_HEALTH_KIND_PR
    return f"{STREAM_HEALTH_KEY_PREFIX}{provider}:{kind}"


class StreamHealthState(str, Enum):
    UNKNOWN = "unknown"
    HEALTHY = "healthy"
    STRANDED = "stranded"


@dataclass(frozen=True)
class ProviderStreamHealth:
    """Canonical per-stream health snapshot for one provider task stream."""

    provider: str
    stream: str
    pending: int | None
    lag: int | None
    registered_consumers: int | None
    live_consumers: int | None
    state: StreamHealthState
    observed_at: float
    transitioned_at: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "stream": self.stream,
            "pending": self.pending,
            "lag": self.lag,
            "registered_consumers": self.registered_consumers,
            "live_consumers": self.live_consumers,
            "state": self.state.value,
            "observed_at": self.observed_at,
            "transitioned_at": self.transitioned_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ProviderStreamHealth:
        return cls(
            provider=str(data["provider"]),
            stream=str(data["stream"]),
            pending=data.get("pending"),
            lag=data.get("lag"),
            registered_consumers=data.get("registered_consumers"),
            live_consumers=data.get("live_consumers"),
            state=StreamHealthState(data["state"]),
            observed_at=float(data["observed_at"]),
            transitioned_at=float(data["transitioned_at"]),
        )


@dataclass
class _Tracked:
    health: ProviderStreamHealth
    candidate_since: float | None


class ProviderStreamHealthTracker:
    """Per-stream dwell tracking carried across reconcile passes.

    One instance lives for the life of the owning process. State is keyed
    by ``(provider, stream)`` so the PR-task and issue-task streams of the
    same provider keep independent health, dwell candidates, and
    ``transitioned_at`` values. A Redis read error for a pass must never
    fabricate a recovery or reset a dwell timer already in progress, so
    ``evaluate`` leaves both the published state and the candidate-since
    timer untouched on ``read_error=True``. A read error (or missing
    evaluation) for one stream does not touch any other stream's state.
    """

    def __init__(self, dwell_seconds: float = DEFAULT_STRANDED_DWELL_SECONDS) -> None:
        self._dwell_seconds = dwell_seconds
        self._tracked: dict[tuple[str, str], _Tracked] = {}

    def evaluate(
        self,
        provider: str,
        stream: str,
        *,
        now: float,
        pending: int | None,
        lag: int | None,
        registered_consumers: int | None,
        live_consumers: int | None,
        read_error: bool,
    ) -> tuple[ProviderStreamHealth, TransitionKind]:
        identity = (provider, stream)
        prior = self._tracked.get(identity)
        prior_health = prior.health if prior else None
        prior_state = prior_health.state if prior_health else StreamHealthState.UNKNOWN
        candidate_since = prior.candidate_since if prior else None

        if read_error:
            transitioned_at = prior_health.transitioned_at if prior_health else now
            snapshot = ProviderStreamHealth(
                provider=provider,
                stream=stream,
                pending=None,
                lag=None,
                registered_consumers=None,
                live_consumers=None,
                state=prior_state,
                observed_at=now,
                transitioned_at=transitioned_at,
            )
            self._tracked[identity] = _Tracked(health=snapshot, candidate_since=candidate_since)
            return snapshot, None

        has_work = (pending or 0) > 0 or (lag or 0) > 0
        no_live_consumers = (live_consumers or 0) == 0
        raw_stranded = has_work and no_live_consumers

        if raw_stranded:
            if candidate_since is None:
                candidate_since = now
            dwell_elapsed = now - candidate_since
            if dwell_elapsed >= self._dwell_seconds or prior_state == StreamHealthState.STRANDED:
                new_state = StreamHealthState.STRANDED
            else:
                new_state = StreamHealthState.HEALTHY
        else:
            candidate_since = None
            new_state = StreamHealthState.HEALTHY

        transition: TransitionKind = None
        if new_state == StreamHealthState.STRANDED and prior_state != StreamHealthState.STRANDED:
            transition = "stranded"
        elif prior_state == StreamHealthState.STRANDED and new_state != StreamHealthState.STRANDED:
            transition = "recovered"

        transitioned_at = (
            now
            if transition is not None
            else (prior_health.transitioned_at if prior_health else now)
        )

        snapshot = ProviderStreamHealth(
            provider=provider,
            stream=stream,
            pending=pending,
            lag=lag,
            registered_consumers=registered_consumers,
            live_consumers=live_consumers,
            state=new_state,
            observed_at=now,
            transitioned_at=transitioned_at,
        )
        self._tracked[identity] = _Tracked(health=snapshot, candidate_since=candidate_since)
        return snapshot, transition
