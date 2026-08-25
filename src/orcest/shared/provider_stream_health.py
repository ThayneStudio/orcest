"""Pure dwell-based detector for stranded provider task streams.

A provider stream is *stranded* when it carries pending or lagging work
but none of its registered consumers is backed by a live worker
heartbeat. ``PoolManager`` (see ``fleet/pool_manager.py``) is the single
transition owner: it reads Redis, feeds the raw numbers into
``ProviderStreamHealthTracker.evaluate``, and publishes the resulting
snapshot. This module performs no I/O and takes an explicit clock so the
state machine can be tested without a real Redis or wall-clock sleeps.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Literal

# Required dwell before a raw-stranded reading becomes the officially
# published "stranded" state. Short blips (a worker mid-rotation, a
# reconcile pass racing a fresh claim) must not page anyone.
DEFAULT_STRANDED_DWELL_SECONDS = 300.0

TransitionKind = Literal["stranded", "recovered"] | None


class StreamHealthState(str, Enum):
    UNKNOWN = "unknown"
    HEALTHY = "healthy"
    STRANDED = "stranded"


@dataclass(frozen=True)
class ProviderStreamHealth:
    """Canonical per-provider stream health snapshot."""

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
    """Per-provider dwell tracking carried across reconcile passes.

    One instance lives for the life of the owning process. A Redis read
    error for a pass must never fabricate a recovery or reset a dwell
    timer already in progress, so ``evaluate`` leaves both the published
    state and the candidate-since timer untouched on ``read_error=True``.
    """

    def __init__(self, dwell_seconds: float = DEFAULT_STRANDED_DWELL_SECONDS) -> None:
        self._dwell_seconds = dwell_seconds
        self._tracked: dict[str, _Tracked] = {}

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
        prior = self._tracked.get(provider)
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
            self._tracked[provider] = _Tracked(health=snapshot, candidate_since=candidate_since)
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
        self._tracked[provider] = _Tracked(health=snapshot, candidate_since=candidate_since)
        return snapshot, transition
