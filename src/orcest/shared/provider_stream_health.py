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

Restart restore (issue #636) seeds the tracker only from a *validated
committed STRANDED* snapshot. ``HEALTHY`` records are display state: they
must not resume an in-progress dwell candidate, because ``transitioned_at``
is the last committed transition, not ``candidate_since``. Validation is
pure (version, identity, TTL, timestamp order/freshness); Redis I/O stays
in ``PoolManager``.

Credential boundary: a principal that holds the shared Redis password can
SET ``provider-stream-health:*`` and forge a STRANDED snapshot that will
bypass dwell after restart. Per-worker Redis ACLs and snapshot signing are
separate scope; this module authenticates nothing.
"""

from __future__ import annotations

import math
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

# Wire-format version for published snapshots. Restore rejects any other
# version (including a missing field) so an older display record cannot
# become control state.
STREAM_HEALTH_SNAPSHOT_VERSION = 1

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
            "version": STREAM_HEALTH_SNAPSHOT_VERSION,
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
        """Display-oriented deserializer. Restore must use
        ``parse_committed_stranded_snapshot`` instead: this path is
        permissive and does not authenticate identity, TTL, or version.
        """
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


def parse_committed_stranded_snapshot(
    payload: Any,
    *,
    expected_provider: str,
    expected_stream: str,
    now: float,
    ttl_seconds: int,
    max_age_seconds: float,
) -> ProviderStreamHealth | None:
    """Return a committed STRANDED snapshot if *payload* is safe to restore.

    Missing, expired, non-expiring, malformed, identity-mismatched, future,
    reversed, non-finite, stale, HEALTHY, UNKNOWN, and unsupported-version
    records are all absent (``None``). ``ttl_seconds`` is the Redis TTL of
    the key (positive remaining seconds); ``-1`` (no expire) and ``-2``
    (missing) are rejected here so the tracker never talks to Redis.

    ``transitioned_at`` is the committed transition time and may be older
    than ``max_age_seconds``. Freshness is required of ``observed_at`` only.
    A HEALTHY snapshot is never used to infer ``candidate_since``.
    """
    try:
        return _parse_committed_stranded_snapshot(
            payload,
            expected_provider=expected_provider,
            expected_stream=expected_stream,
            now=now,
            ttl_seconds=ttl_seconds,
            max_age_seconds=max_age_seconds,
        )
    except (TypeError, ValueError, KeyError):
        return None


def _parse_committed_stranded_snapshot(
    payload: Any,
    *,
    expected_provider: str,
    expected_stream: str,
    now: float,
    ttl_seconds: int,
    max_age_seconds: float,
) -> ProviderStreamHealth:
    if not isinstance(payload, dict):
        raise TypeError("snapshot payload must be an object")
    if not _positive_int(ttl_seconds):
        raise ValueError("snapshot TTL must be a positive int")
    if not _finite_number(now) or not _finite_number(max_age_seconds) or max_age_seconds <= 0:
        raise ValueError("now and max_age_seconds must be finite and max_age positive")
    version = payload.get("version")
    if not _positive_int(version) or version != STREAM_HEALTH_SNAPSHOT_VERSION:
        raise ValueError("unsupported snapshot version")

    provider = payload["provider"]
    stream = payload["stream"]
    if not isinstance(provider, str) or not isinstance(stream, str):
        raise TypeError("provider and stream must be strings")
    if provider != expected_provider or stream != expected_stream:
        raise ValueError("snapshot identity does not match the monitor target")

    state = StreamHealthState(payload["state"])
    if state != StreamHealthState.STRANDED:
        raise ValueError("only committed STRANDED snapshots may be restored")

    observed_at = _finite_timestamp(payload["observed_at"])
    transitioned_at = _finite_timestamp(payload["transitioned_at"])
    if observed_at > now or transitioned_at > now:
        raise ValueError("snapshot timestamps must not be in the future")
    if transitioned_at > observed_at:
        raise ValueError("transitioned_at must be <= observed_at")
    if now - observed_at > max_age_seconds:
        raise ValueError("observed_at is older than the snapshot freshness window")

    return ProviderStreamHealth(
        provider=provider,
        stream=stream,
        pending=_optional_count(payload.get("pending")),
        lag=_optional_count(payload.get("lag")),
        registered_consumers=_optional_count(payload.get("registered_consumers")),
        live_consumers=_optional_count(payload.get("live_consumers")),
        state=state,
        observed_at=observed_at,
        transitioned_at=transitioned_at,
    )


def _positive_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def _finite_number(value: Any) -> bool:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    return math.isfinite(float(value))


def _finite_timestamp(value: Any) -> float:
    if not _finite_number(value):
        raise ValueError("timestamp must be a finite number")
    ts = float(value)
    if ts <= 0:
        raise ValueError("timestamp must be positive")
    return ts


def _optional_count(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError("count must be an int or null")
    return value


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

    def has_state(self, provider: str, stream: str) -> bool:
        """Return True if this identity has already been evaluated or restored."""
        return (provider, stream) in self._tracked

    def restore_committed(self, health: ProviderStreamHealth) -> bool:
        """Seed in-memory state from a validated committed STRANDED snapshot.

        Returns True only when the identity was empty and *health* is
        STRANDED. ``candidate_since`` is deliberately left unset: a
        committed transition time is not an in-progress dwell candidate.
        HEALTHY / UNKNOWN snapshots are ignored so they cannot bypass dwell
        after a restart.
        """
        if health.state is not StreamHealthState.STRANDED:
            return False
        identity = (health.provider, health.stream)
        if identity in self._tracked:
            return False
        self._tracked[identity] = _Tracked(health=health, candidate_since=None)
        return True

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
