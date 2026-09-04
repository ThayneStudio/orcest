"""Bounded health inspection for the project-local results stream."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

import redis as redis_lib

from orcest.shared.redis_client import RedisClient

RESULTS_STREAM = "results"
RESULTS_GROUP = "orchestrator"
RESULT_PENDING_STALE_IDLE_SECONDS = 15 * 60
RESULT_PENDING_STALE_DELIVERIES = 10
RESULT_PENDING_SAMPLE_LIMIT = 100


@dataclass(frozen=True)
class ResultStreamHealth:
    stream: str
    group: str
    stream_exists: bool
    retained_entries: int
    pending: int
    lag: int
    consumers: int
    oldest_pending_idle_seconds: int | None
    max_delivery_count: int
    sampled_pending: int
    inspection_error: str | None = None

    @property
    def work(self) -> int:
        return self.pending + self.lag

    @property
    def unconsumed(self) -> bool:
        """True when the results consumer group has zero active consumers
        but still has pending or lagging work -- i.e. nobody is reading."""
        return self.consumers == 0 and self.work > 0

    @property
    def stale(self) -> bool:
        return (
            self.oldest_pending_idle_seconds is not None
            and self.oldest_pending_idle_seconds >= RESULT_PENDING_STALE_IDLE_SECONDS
        ) or self.max_delivery_count >= RESULT_PENDING_STALE_DELIVERIES


def inspect_result_stream(redis: RedisClient) -> ResultStreamHealth:
    """Inspect the prefixed project results stream without reading result bodies."""
    return inspect_result_stream_raw(redis, redis._prefixed(RESULTS_STREAM))


def inspect_result_stream_raw(redis: RedisClient, stream: str) -> ResultStreamHealth:
    """Inspect a fully-qualified results stream.

    The stream's raw XLEN is only retained-entry count. Unprocessed work is the
    orchestrator consumer group's pending plus lag. Per-entry inspection is
    capped to bound work on large stuck PELs.
    """
    try:
        key_type = str(cast(Any, redis.client.type(stream)))
        if key_type == "none":
            return _result(stream, stream_exists=False, retained_entries=0)
        if key_type != "stream":
            return _result(
                stream,
                stream_exists=True,
                inspection_error=f"{stream}: expected stream, found {key_type}",
            )

        retained_entries = int(cast(Any, redis.client.xlen(stream)))
        groups = cast(list[dict[str, Any]], redis.client.xinfo_groups(stream))
        matching = [group for group in groups if _text(group.get("name")) == RESULTS_GROUP]
        if not matching:
            error = (
                f"{stream}: results consumer group {RESULTS_GROUP!r} is missing"
                if retained_entries > 0
                else None
            )
            return _result(
                stream,
                stream_exists=True,
                retained_entries=retained_entries,
                inspection_error=error,
            )

        group = matching[0]
        pending = _non_negative_int(group.get("pending"))
        raw_lag = _int_or_none(group.get("lag"))
        consumers = _non_negative_int(group.get("consumers"))
        if pending is None or raw_lag is None or consumers is None:
            return _result(
                stream,
                stream_exists=True,
                retained_entries=retained_entries,
                inspection_error=f"{stream}: results consumer group work is unavailable",
            )
        lag = max(raw_lag, 0)

        oldest_idle_seconds: int | None = None
        max_delivery_count = 0
        sampled_pending = 0
        if pending > 0:
            rows = cast(
                list[dict[str, Any]],
                redis.client.xpending_range(
                    stream,
                    RESULTS_GROUP,
                    min="-",
                    max="+",
                    count=min(pending, RESULT_PENDING_SAMPLE_LIMIT),
                ),
            )
            sampled_pending = len(rows)
            if sampled_pending == 0:
                return _result(
                    stream,
                    stream_exists=True,
                    retained_entries=retained_entries,
                    pending=pending,
                    lag=lag,
                    consumers=consumers,
                    inspection_error=f"{stream}: pending result entries are unavailable",
                )
            oldest_idle_ms = 0
            for row in rows:
                idle_ms = _non_negative_int(row.get("time_since_delivered"))
                deliveries = _non_negative_int(row.get("times_delivered"))
                if idle_ms is None or deliveries is None:
                    return _result(
                        stream,
                        stream_exists=True,
                        retained_entries=retained_entries,
                        pending=pending,
                        lag=lag,
                        consumers=consumers,
                        inspection_error=f"{stream}: pending result metadata is malformed",
                    )
                oldest_idle_ms = max(oldest_idle_ms, idle_ms)
                max_delivery_count = max(max_delivery_count, deliveries)
            oldest_idle_seconds = oldest_idle_ms // 1000

        return _result(
            stream,
            stream_exists=True,
            retained_entries=retained_entries,
            pending=pending,
            lag=lag,
            consumers=consumers,
            oldest_pending_idle_seconds=oldest_idle_seconds,
            max_delivery_count=max_delivery_count,
            sampled_pending=sampled_pending,
        )
    except (redis_lib.RedisError, TypeError, ValueError) as exc:
        return _result(stream, inspection_error=f"{stream}: {type(exc).__name__}")


def format_result_stream_warning(health: ResultStreamHealth) -> str | None:
    """Return a secret-free warning for unconsumed, stale, or unreadable result handling."""
    if health.inspection_error is not None:
        return f"RESULT STREAM UNHEALTHY {health.inspection_error}"
    if health.unconsumed:
        return (
            f"UNCONSUMED result handling on {health.stream}: pending={health.pending} "
            f"lag={health.lag} consumers=0"
        )
    if not health.stale:
        return None

    oldest = (
        "unknown"
        if health.oldest_pending_idle_seconds is None
        else f"{health.oldest_pending_idle_seconds}s"
    )
    return (
        f"STALE result handling on {health.stream}: pending={health.pending} "
        f"lag={health.lag} oldest_pending_idle={oldest} "
        f"max_deliveries={health.max_delivery_count} "
        f"sampled={health.sampled_pending}/{health.pending}"
    )


def _result(
    stream: str,
    *,
    stream_exists: bool = False,
    retained_entries: int = 0,
    pending: int = 0,
    lag: int = 0,
    consumers: int = 0,
    oldest_pending_idle_seconds: int | None = None,
    max_delivery_count: int = 0,
    sampled_pending: int = 0,
    inspection_error: str | None = None,
) -> ResultStreamHealth:
    return ResultStreamHealth(
        stream=stream,
        group=RESULTS_GROUP,
        stream_exists=stream_exists,
        retained_entries=retained_entries,
        consumers=consumers,
        pending=pending,
        lag=lag,
        oldest_pending_idle_seconds=oldest_pending_idle_seconds,
        max_delivery_count=max_delivery_count,
        sampled_pending=sampled_pending,
        inspection_error=inspection_error,
    )


def _non_negative_int(value: Any) -> int | None:
    parsed = _int_or_none(value)
    if parsed is None or parsed < 0:
        return None
    return parsed


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _text(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)
