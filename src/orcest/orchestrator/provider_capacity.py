"""Capacity-aware admission for legacy provider task publication.

Quota eligibility remains owned by :mod:`provider_pool`.  This module only
handles operational capacity, using non-secret worker/task/provider identities
from the shared task Redis database.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from fractions import Fraction

import redis

from orcest.shared.coordination import RedisLock
from orcest.shared.models import CONSUMER_GROUP, PROVIDER_NAME_RE, task_stream_name
from orcest.shared.redis_client import RedisClient

CAPACITY_RESERVATION_TTL_SECONDS = 300
_CAPACITY_LOCK_TTL_SECONDS = 30
_CAPACITY_LOCK_KEY = "providers:capacity:selection"
_CAPACITY_ROUND_ROBIN_KEY = "providers:capacity:round_robin"
_CAPACITY_RESERVATION_PREFIX = "providers:capacity:reservations:"
_HEARTBEAT_PATTERN = "workers:heartbeat:*"
_HEARTBEAT_PREFIX = "workers:heartbeat:"
_MAX_HEARTBEAT_KEYS = 1024
_MAX_SCAN_PAGES = 32
_SCAN_PAGE_SIZE = 128
# Current workers write a 150-second TTL.  The upper bound admits rollout
# jitter while rejecting immortal/obviously stale keys as live capacity.
_MAX_HEARTBEAT_TTL_SECONDS = 300
_MAX_HEARTBEAT_PAYLOAD_BYTES = 16 * 1024
_MAX_PENDING_ENTRIES_PER_STREAM = 2048


class CapacityReadError(RuntimeError):
    """A capacity input could not be proved complete and well formed."""


@dataclass(frozen=True)
class ProviderLoadSnapshot:
    """One bounded, secret-free view of a provider backend's load."""

    provider: str
    live_workers: tuple[str, ...]
    busy_consumers: tuple[str, ...]
    unread_lag: int
    reservations: int

    @property
    def outstanding(self) -> int:
        return len(self.busy_consumers) + self.unread_lag + self.reservations

    @property
    def effective_spare(self) -> int:
        return max(0, len(self.live_workers) - self.outstanding)

    @property
    def load_per_live_worker(self) -> Fraction | None:
        if not self.live_workers:
            return None
        return Fraction(self.outstanding, len(self.live_workers))

    def to_safe_dict(self) -> dict[str, object]:
        return {
            "provider": self.provider,
            "live_workers": list(self.live_workers),
            "busy_consumers": list(self.busy_consumers),
            "unread_lag": self.unread_lag,
            "reservations": self.reservations,
            "effective_spare": self.effective_spare,
        }


@dataclass
class CapacityReservation:
    """A short-lived claim on one backend slot while a task is prepared."""

    redis: RedisClient
    provider: str
    task_id: str
    ttl_seconds: int = CAPACITY_RESERVATION_TTL_SECONDS
    _released: bool = False

    @property
    def key(self) -> str:
        return f"{_CAPACITY_RESERVATION_PREFIX}{self.provider}"

    def refresh(self) -> bool:
        """Extend an unexpired claim, serialized with new selections."""
        if self._released:
            return False
        lock: RedisLock | None = None
        try:
            lock = RedisLock(
                self.redis,
                _CAPACITY_LOCK_KEY,
                ttl=_CAPACITY_LOCK_TTL_SECONDS,
                owner=self.task_id,
            )
            if not lock.acquire():
                return False
            now = time.time()
            score = self.redis.zscore(self.key, self.task_id)
            if score is None or score <= now:
                self.redis.zrem(self.key, self.task_id)
                self._released = True
                return False
            # Re-derive the selected backend immediately before publication.
            # The reservation itself should exactly fill one slot; a worker
            # disappearance or newly-visible task that makes outstanding work
            # exceed live workers invalidates the claim without side effects.
            known, unknown = read_provider_loads(self.redis, [self.provider])
            snapshot = known.get(self.provider)
            if (
                self.provider in unknown
                or snapshot is None
                or not snapshot.live_workers
                or snapshot.reservations < 1
                or snapshot.outstanding > len(snapshot.live_workers)
            ):
                self.redis.zrem(self.key, self.task_id)
                self._released = True
                return False
            # Owner verification and renewal are one Redis operation. A
            # selector paused past its lock TTL must not resume and renew a
            # claim after a newer selector has observed the released lock.
            return self.redis.zadd_expiring_if_value(
                _CAPACITY_LOCK_KEY,
                self.task_id,
                self.key,
                self.task_id,
                now + self.ttl_seconds,
                self.ttl_seconds * 2,
            )
        except Exception:
            # Capacity failures are fail-closed. Never let transport/auth
            # exception text escape into outer publication logs.
            return False
        finally:
            if lock is not None and lock.is_held:
                try:
                    lock.release()
                except Exception:
                    pass

    def release(self) -> None:
        """Best-effort removal after success, failure, or an aborted publish."""
        if self._released:
            return
        try:
            self.redis.zrem(self.key, self.task_id)
        except Exception:
            # The expiration score and Redis TTL remain the cleanup backstop;
            # do not turn a successful publish into an apparent failure.
            pass
        finally:
            self._released = True


def _exact_count(value: object, field: str) -> int:
    if type(value) is not int or value < 0:
        raise CapacityReadError(f"malformed {field}")
    return value


def _scan_live_workers(redis_client: RedisClient) -> tuple[dict[str, set[str]], set[str]]:
    keys: list[str] = []
    cursor = 0
    for _ in range(_MAX_SCAN_PAGES):
        cursor, page = redis_client.scan_page(
            cursor,
            match=_HEARTBEAT_PATTERN,
            count=_SCAN_PAGE_SIZE,
        )
        keys.extend(page)
        if len(keys) > _MAX_HEARTBEAT_KEYS:
            raise CapacityReadError("worker heartbeat scan exceeded its bound")
        if cursor == 0:
            break
    else:
        raise CapacityReadError("worker heartbeat scan was incomplete")

    live: dict[str, set[str]] = {}
    unknown: set[str] = set()
    for key in keys:
        if not key.startswith(_HEARTBEAT_PREFIX):
            raise CapacityReadError("worker heartbeat scan returned an unexpected key")
        worker_id = key.removeprefix(_HEARTBEAT_PREFIX)
        if not worker_id:
            raise CapacityReadError("worker heartbeat identity is empty")
        raw, ttl, oversized = redis_client.get_with_ttl_bounded(
            key,
            max_bytes=_MAX_HEARTBEAT_PAYLOAD_BYTES,
        )
        if oversized:
            raise CapacityReadError("worker heartbeat payload exceeds its bound")
        if raw is None or ttl == -2:
            # Expired between SCAN and the atomic read: it is no longer live.
            continue
        try:
            payload = json.loads(raw)
        except (json.JSONDecodeError, TypeError) as exc:
            raise CapacityReadError("worker heartbeat payload is malformed") from exc
        if not isinstance(payload, dict):
            raise CapacityReadError("worker heartbeat payload is malformed")
        backend = payload.get("backend")
        if not isinstance(backend, str) or PROVIDER_NAME_RE.fullmatch(backend) is None:
            raise CapacityReadError("worker heartbeat backend is malformed")
        if type(ttl) is not int or ttl <= 0 or ttl > _MAX_HEARTBEAT_TTL_SECONDS:
            unknown.add(backend)
            continue
        live.setdefault(backend, set()).add(worker_id)
    return live, unknown


def _group_row(rows: object, stream: str) -> dict[str, object]:
    if not isinstance(rows, list):
        raise CapacityReadError(f"consumer-group metadata malformed for {stream}")
    matches: list[dict[str, object]] = []
    for row in rows:
        if not isinstance(row, dict):
            raise CapacityReadError(f"consumer-group metadata malformed for {stream}")
        if row.get("name") == CONSUMER_GROUP:
            matches.append(row)
    if len(matches) != 1:
        raise CapacityReadError(f"consumer group is missing or duplicated for {stream}")
    return matches[0]


def _stream_load(redis_client: RedisClient, stream: str) -> tuple[int, set[str]]:
    try:
        rows, entries, stream_length = redis_client.xgroup_pending_snapshot(
            stream,
            CONSUMER_GROUP,
            pending_limit=_MAX_PENDING_ENTRIES_PER_STREAM + 1,
        )
    except redis.ResponseError as exc:
        raise CapacityReadError(f"consumer-group metadata unavailable for {stream}") from exc
    group = _group_row(rows, stream)
    pending = _exact_count(group.get("pending"), f"pending count for {stream}")
    raw_lag = group.get("lag")
    if type(raw_lag) is int and raw_lag == -1:
        # Redis reports unknown lag on a newly-created, genuinely empty
        # MKSTREAM group. It is safe to resolve only this exact shape to zero;
        # unknown lag on a non-empty stream remains unknown.
        if pending == 0 and stream_length == 0:
            lag = 0
        else:
            raise CapacityReadError(f"unread lag is unknown for {stream}")
    else:
        lag = _exact_count(raw_lag, f"unread lag for {stream}")
    if pending > _MAX_PENDING_ENTRIES_PER_STREAM:
        raise CapacityReadError(f"pending coverage exceeds bound for {stream}")
    if pending == 0:
        if entries:
            raise CapacityReadError(f"pending coverage inconsistent for {stream}")
        return lag, set()
    if not isinstance(entries, list) or len(entries) != pending:
        raise CapacityReadError(f"pending coverage incomplete for {stream}")
    message_ids: set[str] = set()
    busy: set[str] = set()
    for row in entries:
        if not isinstance(row, dict):
            raise CapacityReadError(f"pending metadata malformed for {stream}")
        message_id = row.get("message_id")
        consumer = row.get("consumer")
        if (
            not isinstance(message_id, str)
            or not message_id
            or message_id in message_ids
            or not isinstance(consumer, str)
            or not consumer
        ):
            raise CapacityReadError(f"pending identity malformed for {stream}")
        message_ids.add(message_id)
        busy.add(consumer)
    return lag, busy


def _reservation_count(redis_client: RedisClient, provider: str, now: float) -> int:
    key = f"{_CAPACITY_RESERVATION_PREFIX}{provider}"
    redis_client.zremrangebyscore(key, "-inf", now)
    return redis_client.zcard(key)


def read_provider_loads(
    redis_client: RedisClient,
    providers: list[str],
) -> tuple[dict[str, ProviderLoadSnapshot], dict[str, str]]:
    """Read known snapshots and return provider-only reasons for unknown ones."""
    ordered = list(dict.fromkeys(providers))
    try:
        live_by_provider, heartbeat_unknown = _scan_live_workers(redis_client)
    except Exception:
        # A heartbeat that cannot be attributed to a backend might represent
        # any candidate. Failing every candidate closed is the only way not to
        # reinterpret that incomplete fleet census as proven idle capacity.
        return {}, {provider: "worker heartbeat read unavailable" for provider in ordered}

    known: dict[str, ProviderLoadSnapshot] = {}
    unknown: dict[str, str] = {}
    now = time.time()
    for provider in ordered:
        if provider in heartbeat_unknown:
            unknown[provider] = "worker heartbeat TTL is invalid"
            continue
        try:
            pr_lag, pr_busy = _stream_load(redis_client, task_stream_name(provider, issue=False))
            issue_lag, issue_busy = _stream_load(
                redis_client, task_stream_name(provider, issue=True)
            )
            reservations = _reservation_count(redis_client, provider, now)
        except CapacityReadError as exc:
            unknown[provider] = str(exc)[:160] or "capacity read unavailable"
            continue
        except Exception:
            unknown[provider] = "capacity metadata read unavailable"
            continue
        known[provider] = ProviderLoadSnapshot(
            provider=provider,
            live_workers=tuple(sorted(live_by_provider.get(provider, set()))),
            busy_consumers=tuple(sorted(pr_busy | issue_busy)),
            unread_lag=pr_lag + issue_lag,
            reservations=reservations,
        )
    return known, unknown


def reserve_provider_capacity(
    redis_client: RedisClient,
    providers: list[str],
    task_id: str,
    logger: logging.Logger,
    *,
    reservation_ttl_seconds: int = CAPACITY_RESERVATION_TTL_SECONDS,
) -> CapacityReservation | None:
    """Select the least-loaded provider with proven spare capacity and reserve it."""
    ordered = list(dict.fromkeys(providers))
    if not ordered or reservation_ttl_seconds < 1:
        return None
    lock: RedisLock | None = None
    try:
        lock = RedisLock(
            redis_client,
            _CAPACITY_LOCK_KEY,
            ttl=_CAPACITY_LOCK_TTL_SECONDS,
            owner=task_id,
        )
        if not lock.acquire():
            logger.info("Provider capacity selection is busy; deferring task %s", task_id)
            return None
        known, unknown = read_provider_loads(redis_client, ordered)
        for provider, reason in unknown.items():
            logger.warning("Provider %s capacity is unknown: %s", provider, reason)
        eligible = [
            known[provider]
            for provider in ordered
            if provider in known and known[provider].effective_spare > 0
        ]
        if not eligible:
            logger.info("No provider has proven spare capacity for task %s", task_id)
            return None
        ratios = [
            Fraction(snapshot.outstanding, len(snapshot.live_workers)) for snapshot in eligible
        ]
        minimum = min(ratios)
        tied = [snapshot for snapshot in eligible if snapshot.load_per_live_worker == minimum]
        sequence = redis_client.incr(_CAPACITY_ROUND_ROBIN_KEY)
        selected = tied[(sequence - 1) % len(tied)]
        reservation = CapacityReservation(
            redis=redis_client,
            provider=selected.provider,
            task_id=task_id,
            ttl_seconds=reservation_ttl_seconds,
        )
        # Validate the lock owner in the same Redis operation that writes the
        # reservation. A process paused beyond the lock TTL cannot consume a
        # slot after a newer selector has acted on the same snapshot.
        if not redis_client.zadd_expiring_if_value(
            _CAPACITY_LOCK_KEY,
            task_id,
            reservation.key,
            task_id,
            time.time() + reservation_ttl_seconds,
            reservation_ttl_seconds * 2,
        ):
            logger.warning("Provider capacity selection lease expired for task %s", task_id)
            return None
        logger.info(
            "Reserved provider %s capacity for task %s (live=%d busy=%d unread=%d reservations=%d)",
            selected.provider,
            task_id,
            len(selected.live_workers),
            len(selected.busy_consumers),
            selected.unread_lag,
            selected.reservations,
        )
        return reservation
    except Exception:
        # Do not render arbitrary Redis exception strings: capacity diagnostics
        # are deliberately limited to provider/worker/task identities.
        logger.warning("Provider capacity selection failed for task %s", task_id)
        return None
    finally:
        if lock is not None and lock.is_held:
            try:
                lock.release()
            except Exception:
                logger.debug("Failed to release provider capacity selection lock")
