"""Bounded health inspection for the project-local results stream."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

import redis as redis_lib

from orcest.shared.models import RESULTS_GROUP, RESULTS_STREAM
from orcest.shared.redis_client import RedisClient

RESULT_PENDING_STALE_IDLE_SECONDS = 15 * 60
RESULT_PENDING_STALE_DELIVERIES = 10
RESULT_CONSUMER_LIVE_IDLE_SECONDS = 15 * 60
RESULT_PENDING_PAGE_SIZE = 100
RESULT_PENDING_INSPECTION_LIMIT = 1000


@dataclass(frozen=True)
class ResultStreamHealth:
    stream: str
    group: str
    stream_exists: bool
    retained_entries: int
    pending: int
    lag: int
    consumers: int
    live_consumers: int
    youngest_consumer_idle_seconds: int | None
    sampled_oldest_pending_idle_seconds: int | None
    sampled_max_delivery_count: int
    sampled_pending: int
    pending_inspection_complete: bool
    inspection_error: str | None = None

    @property
    def work(self) -> int:
        return self.pending + self.lag

    @property
    def unconsumed(self) -> bool:
        """True when work exists but no consumer interacted within the grace period."""
        return self.live_consumers == 0 and self.work > 0

    @property
    def oldest_pending_idle_seconds(self) -> int | None:
        """Exact oldest idle time, or None when pending inspection was incomplete."""
        if not self.pending_inspection_complete:
            return None
        return self.sampled_oldest_pending_idle_seconds

    @property
    def max_delivery_count(self) -> int | None:
        """Exact maximum deliveries, or None when pending inspection was incomplete."""
        if not self.pending_inspection_complete:
            return None
        return self.sampled_max_delivery_count

    @property
    def stale(self) -> bool:
        return (
            self.sampled_oldest_pending_idle_seconds is not None
            and self.sampled_oldest_pending_idle_seconds >= RESULT_PENDING_STALE_IDLE_SECONDS
        ) or self.sampled_max_delivery_count >= RESULT_PENDING_STALE_DELIVERIES


def inspect_result_stream(redis: RedisClient) -> ResultStreamHealth:
    """Inspect the prefixed project results stream without reading result bodies."""
    return inspect_result_stream_raw(redis, redis._prefixed(RESULTS_STREAM))


def inspect_result_stream_raw(redis: RedisClient, stream: str) -> ResultStreamHealth:
    """Inspect a fully-qualified results stream.

    The stream's raw XLEN is only retained-entry count. Unprocessed work is the
    orchestrator consumer group's pending plus lag. Per-entry inspection uses
    bounded pages. If the PEL exceeds the hard inspection ceiling, health is
    conservatively unknown/unhealthy instead of treating a partial sample as
    proof that all entries are fresh.
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
            return _result(
                stream,
                stream_exists=True,
                retained_entries=retained_entries,
                inspection_error=f"{stream}: results consumer group {RESULTS_GROUP!r} is missing",
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

        (
            live_consumers,
            youngest_consumer_idle_seconds,
            consumer_error,
        ) = _inspect_consumer_liveness(redis, stream, consumers)
        if consumer_error is not None:
            return _result(
                stream,
                stream_exists=True,
                retained_entries=retained_entries,
                pending=pending,
                lag=lag,
                consumers=consumers,
                inspection_error=consumer_error,
            )

        (
            sampled_pending,
            sampled_oldest_pending_idle_seconds,
            sampled_max_delivery_count,
            pending_inspection_complete,
            pending_error,
        ) = _inspect_pending_entries(redis, stream, pending)

        return _result(
            stream,
            stream_exists=True,
            retained_entries=retained_entries,
            pending=pending,
            lag=lag,
            consumers=consumers,
            live_consumers=live_consumers,
            youngest_consumer_idle_seconds=youngest_consumer_idle_seconds,
            sampled_oldest_pending_idle_seconds=sampled_oldest_pending_idle_seconds,
            sampled_max_delivery_count=sampled_max_delivery_count,
            sampled_pending=sampled_pending,
            pending_inspection_complete=pending_inspection_complete,
            inspection_error=pending_error,
        )
    except (redis_lib.RedisError, TypeError, ValueError) as exc:
        return _result(stream, inspection_error=f"{stream}: {type(exc).__name__}")


def format_result_stream_warning(health: ResultStreamHealth) -> str | None:
    """Return a secret-free warning for unconsumed, stale, or unreadable result handling."""
    if health.inspection_error is not None:
        return f"RESULT STREAM UNHEALTHY {health.inspection_error}"
    if health.unconsumed:
        youngest = (
            "none"
            if health.youngest_consumer_idle_seconds is None
            else f"{health.youngest_consumer_idle_seconds}s"
        )
        return (
            f"UNCONSUMED result handling on {health.stream}: pending={health.pending} "
            f"lag={health.lag} live_consumers={health.live_consumers}/"
            f"{health.consumers} youngest_consumer_idle={youngest}"
        )
    if not health.stale:
        return None

    oldest = (
        "unknown"
        if health.sampled_oldest_pending_idle_seconds is None
        else f"{health.sampled_oldest_pending_idle_seconds}s"
    )
    return (
        f"STALE result handling on {health.stream}: pending={health.pending} "
        f"lag={health.lag} oldest_pending_idle={oldest} "
        f"max_deliveries={health.sampled_max_delivery_count} "
        f"sampled={health.sampled_pending}/{health.pending}"
    )


def format_result_stream_metrics(health: ResultStreamHealth) -> tuple[tuple[str, str], ...]:
    """Return stable, secret-free metric/value pairs for both status surfaces."""
    incomplete_suffix = " (sample)" if not health.pending_inspection_complete else ""
    oldest = (
        "--"
        if health.sampled_oldest_pending_idle_seconds is None
        else f"{health.sampled_oldest_pending_idle_seconds}s{incomplete_suffix}"
    )
    max_deliveries = (
        "0" if health.pending == 0 else f"{health.sampled_max_delivery_count}{incomplete_suffix}"
    )
    youngest_consumer = (
        "--"
        if health.youngest_consumer_idle_seconds is None
        else f"{health.youngest_consumer_idle_seconds}s"
    )
    coverage = f"{health.sampled_pending}/{health.pending}"
    if not health.pending_inspection_complete:
        coverage += " incomplete"
    return (
        ("Stream", health.stream),
        ("Retained XLEN", str(health.retained_entries)),
        ("Pending", str(health.pending)),
        ("Lag", str(health.lag)),
        ("Oldest pending idle", oldest),
        ("Max deliveries", max_deliveries),
        ("Pending inspected", coverage),
        ("Live/registered consumers", f"{health.live_consumers}/{health.consumers}"),
        ("Newest consumer idle", youngest_consumer),
    )


def unavailable_result_stream_health(stream: str) -> ResultStreamHealth:
    """Represent the real disconnected-snapshot state without optional fields."""
    return _result(
        stream,
        pending_inspection_complete=False,
        inspection_error=f"{stream}: inspection unavailable",
    )


def _result(
    stream: str,
    *,
    stream_exists: bool = False,
    retained_entries: int = 0,
    pending: int = 0,
    lag: int = 0,
    consumers: int = 0,
    live_consumers: int = 0,
    youngest_consumer_idle_seconds: int | None = None,
    sampled_oldest_pending_idle_seconds: int | None = None,
    sampled_max_delivery_count: int = 0,
    sampled_pending: int = 0,
    pending_inspection_complete: bool = True,
    inspection_error: str | None = None,
) -> ResultStreamHealth:
    return ResultStreamHealth(
        stream=stream,
        group=RESULTS_GROUP,
        stream_exists=stream_exists,
        retained_entries=retained_entries,
        consumers=consumers,
        live_consumers=live_consumers,
        youngest_consumer_idle_seconds=youngest_consumer_idle_seconds,
        pending=pending,
        lag=lag,
        sampled_oldest_pending_idle_seconds=sampled_oldest_pending_idle_seconds,
        sampled_max_delivery_count=sampled_max_delivery_count,
        sampled_pending=sampled_pending,
        pending_inspection_complete=pending_inspection_complete,
        inspection_error=inspection_error,
    )


def _inspect_consumer_liveness(
    redis: RedisClient, stream: str, registered_consumers: int
) -> tuple[int, int | None, str | None]:
    try:
        rows = cast(
            list[dict[str, Any]],
            redis.client.xinfo_consumers(stream, RESULTS_GROUP),
        )
    except (redis_lib.RedisError, TypeError, ValueError) as exc:
        return 0, None, f"{stream}: result consumer liveness {type(exc).__name__}"
    if len(rows) != registered_consumers:
        return 0, None, f"{stream}: result consumer liveness is unavailable"

    idle_values: list[int] = []
    for row in rows:
        idle_ms = _non_negative_int(row.get("idle"))
        if idle_ms is None:
            return 0, None, f"{stream}: result consumer liveness is malformed"
        idle_values.append(idle_ms)

    live_consumers = sum(
        idle_ms < RESULT_CONSUMER_LIVE_IDLE_SECONDS * 1000 for idle_ms in idle_values
    )
    youngest_idle_seconds = min(idle_values) // 1000 if idle_values else None
    return live_consumers, youngest_idle_seconds, None


def _inspect_pending_entries(
    redis: RedisClient, stream: str, pending: int
) -> tuple[int, int | None, int, bool, str | None]:
    if pending == 0:
        return 0, None, 0, True, None

    target = min(pending, RESULT_PENDING_INSPECTION_LIMIT)
    cursor = "-"
    sampled_pending = 0
    oldest_idle_ms = 0
    max_delivery_count = 0
    seen_ids: set[str] = set()

    while sampled_pending < target:
        count = min(RESULT_PENDING_PAGE_SIZE, target - sampled_pending)
        try:
            rows = cast(
                list[dict[str, Any]],
                redis.client.xpending_range(
                    stream,
                    RESULTS_GROUP,
                    min=cursor,
                    max="+",
                    count=count,
                ),
            )
        except (redis_lib.RedisError, TypeError, ValueError) as exc:
            return (
                sampled_pending,
                oldest_idle_ms // 1000 if sampled_pending else None,
                max_delivery_count,
                False,
                f"{stream}: pending result inspection {type(exc).__name__} after "
                f"{sampled_pending} of {pending} entries",
            )
        if not rows:
            return (
                sampled_pending,
                oldest_idle_ms // 1000 if sampled_pending else None,
                max_delivery_count,
                False,
                f"{stream}: pending result inspection ended after "
                f"{sampled_pending} of {pending} entries",
            )

        last_id: str | None = None
        for row in rows:
            raw_id = row.get("message_id")
            if not isinstance(raw_id, (str, bytes)):
                return (
                    sampled_pending,
                    oldest_idle_ms // 1000 if sampled_pending else None,
                    max_delivery_count,
                    False,
                    (f"{stream}: pending result metadata is malformed"),
                )
            message_id = _text(raw_id)
            idle_ms = _non_negative_int(row.get("time_since_delivered"))
            deliveries = _non_negative_int(row.get("times_delivered"))
            if not message_id or message_id in seen_ids or idle_ms is None or deliveries is None:
                return (
                    sampled_pending,
                    oldest_idle_ms // 1000 if sampled_pending else None,
                    max_delivery_count,
                    False,
                    (f"{stream}: pending result metadata is malformed"),
                )
            seen_ids.add(message_id)
            sampled_pending += 1
            oldest_idle_ms = max(oldest_idle_ms, idle_ms)
            max_delivery_count = max(max_delivery_count, deliveries)
            last_id = message_id

        if last_id is None:
            return (
                sampled_pending,
                oldest_idle_ms // 1000 if sampled_pending else None,
                max_delivery_count,
                False,
                (f"{stream}: pending result metadata is malformed"),
            )
        cursor = f"({last_id}"

    oldest_idle_seconds = oldest_idle_ms // 1000
    complete = sampled_pending == pending
    error = None
    if not complete:
        error = (
            f"{stream}: pending result inspection incomplete: inspected "
            f"{sampled_pending} of {pending} entries"
        )
    return sampled_pending, oldest_idle_seconds, max_delivery_count, complete, error


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
