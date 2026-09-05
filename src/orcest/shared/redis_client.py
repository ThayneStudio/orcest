"""Redis connection and stream helper methods.

Thin wrapper around redis-py providing connection pooling and typed
stream operations with simplified return types.
"""

from __future__ import annotations

import json
import logging
import types
from enum import Enum
from typing import Any, cast

import redis

from orcest.shared.config import RedisConfig

logger = logging.getLogger(__name__)


class ConsumerGroupInspection(str, Enum):
    """Result of inspecting consumer-group metadata without mutating Redis."""

    EXISTS = "exists"
    MISSING = "missing"


def is_redis_oom_error(error: BaseException) -> bool:
    """Return True for a classified Redis noeviction/maxmemory OOM write rejection.

    Shared by orchestrator and worker startup so both apply the identical retry
    policy: only this classified write rejection is retried, everything else
    (wrong type, ACL, authentication, protocol, malformed response) stays fatal.
    """
    if not isinstance(error, redis.ResponseError):
        return False
    oom_cls = getattr(redis.exceptions, "OutOfMemoryError", None)
    if oom_cls is not None and isinstance(error, oom_cls):
        return True
    text = str(error).lower()
    return "oom" in text and "maxmemory" in text


_ROUND_ROBIN_TURN_SCRIPT = r"""
local current = redis.call("GET", KEYS[1])
if current then
    for i = 3, #ARGV do
        if ARGV[i] == current then
            return current
        end
    end
end

local count = tonumber(ARGV[2])
if not count or count <= 0 then
    return false
end
local sequence = redis.call("INCR", KEYS[2])
local index = ((sequence - 1) % count) + 3
local selected = ARGV[index]
redis.call("SET", KEYS[1], selected, "EX", ARGV[1])
return selected
"""


_MONOTONIC_VERSION_SCRIPT = r"""
local current = tonumber(redis.call("GET", KEYS[1]) or "0")
local now = redis.call("TIME")
local candidate = tonumber(now[1]) * 1000000 + tonumber(now[2])
if candidate <= current then
    candidate = current + 1
end
local encoded = string.format("%.0f", candidate)
redis.call("SET", KEYS[1], encoded)
return encoded
"""


_XADD_CAPPED_EXPIRE_SCRIPT = r"""
local xargs = {}
for i = 4, #ARGV do
  table.insert(xargs, ARGV[i])
end
local entry_id = redis.call('XADD', KEYS[1], 'MAXLEN', ARGV[1], ARGV[2], '*', unpack(xargs))
redis.call('EXPIRE', KEYS[1], ARGV[3])
return entry_id
"""


class RedisClient:
    """Redis connection with stream helper methods."""

    def __init__(self, config: RedisConfig):
        self._pool = redis.ConnectionPool(
            host=config.host,
            port=config.port,
            db=config.db,
            password=config.password,
            decode_responses=True,
            socket_timeout=config.socket_timeout,
            socket_connect_timeout=config.socket_connect_timeout,
        )
        self._client: redis.Redis = redis.Redis(connection_pool=self._pool)
        self._prefix = f"{config.key_prefix}:" if config.key_prefix else ""

    @classmethod
    def from_client(cls, client: redis.Redis, key_prefix: str = "test") -> "RedisClient":
        """Create a RedisClient wrapping a pre-built redis client.

        Useful in tests to inject a fakeredis instance without opening a real
        connection.
        """
        # NOTE: __init__ is intentionally skipped via object.__new__. If __init__
        # gains new instance attributes, mirror them here to avoid AttributeError.
        instance: RedisClient = object.__new__(cls)
        instance._client = client
        instance._pool = client.connection_pool
        instance._prefix = f"{key_prefix}:" if key_prefix else ""
        return instance

    @property
    def client(self) -> redis.Redis:
        """Raw redis client for operations not covered by helpers."""
        return self._client

    @property
    def key_prefix(self) -> str:
        """The key prefix this client prepends to every key (without the trailing colon).

        Empty string when no prefix is configured.
        """
        return self._prefix[:-1] if self._prefix else ""

    def close(self) -> None:
        """Close the connection pool and release all connections."""
        self._pool.disconnect()

    def __enter__(self) -> "RedisClient":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: types.TracebackType | None,
    ) -> None:
        self.close()

    def _prefixed(self, key: str) -> str:
        """Prepend the key prefix to a Redis key."""
        return self._prefix + key

    def health_check(self) -> bool:
        """Returns True if Redis is reachable."""
        try:
            return self._client.ping()  # type: ignore[return-value]
        except (
            redis.ConnectionError,
            redis.TimeoutError,
            redis.ResponseError,
            redis.AuthenticationError,
        ):
            return False

    def xadd(self, stream: str, fields: dict[str, str]) -> str:
        """Add entry to stream. Returns the entry ID."""
        entry_id: str = self._client.xadd(self._prefixed(stream), fields)  # type: ignore[assignment, arg-type]
        return entry_id

    def xreadgroup(
        self,
        group: str,
        consumer: str,
        stream: str,
        count: int = 1,
        block_ms: int | None = 5000,
        pending: bool = False,
        pending_start_id: str = "0",
    ) -> list[tuple[str, dict[str, str]]]:
        """Read entries from a consumer group.

        Returns list of (entry_id, fields) tuples.
        Returns empty list on timeout or when no entries are available.

        Args:
            group: Consumer group name.
            consumer: Consumer name within the group.
            stream: Stream name to read from.
            count: Maximum number of entries to return.
            block_ms: Milliseconds to block waiting for data.
                ``None`` means non-blocking (return immediately).
                ``0`` means block indefinitely.
                A positive integer means block for that many milliseconds.
            pending: If True, read pending entries (delivered but not ACKed)
                instead of new ones.
            pending_start_id: When ``pending`` is true, return entries with IDs
                greater than this ID. Advancing this cursor lets callers make one
                bounded pass over a consumer's PEL even when earlier entries must
                remain unacknowledged. Ignored when reading new entries.
        """
        entry_id = pending_start_id if pending else ">"
        result = cast(
            list[tuple[str, list[tuple[str, dict[str, str]]]]],
            self._client.xreadgroup(
                groupname=group,
                consumername=consumer,
                streams={self._prefixed(stream): entry_id},
                count=count,
                block=block_ms,
            ),
        )
        if not result:
            return []
        # result shape: [[stream_name, [(id, fields), ...]]]
        return result[0][1]

    def xreadgroup_multi(
        self,
        streams: dict[str, str],
        group: str,
        consumer: str,
        count: int = 1,
        block: int | None = None,
    ) -> list[tuple[str, str, dict[str, str]]]:
        """Read entries from multiple streams via XREADGROUP.

        Unlike ``xreadgroup()``, the *streams* dict keys are fully-qualified
        (already-prefixed) stream names. This allows reading from streams
        with different prefixes in a single call, which is required for
        multi-project workers.

        Args:
            streams: Mapping of fully-qualified stream name to entry ID
                (``">"`` for new entries, ``"0"`` for pending).
            group: Consumer group name.
            consumer: Consumer name within the group.
            count: Maximum number of entries to return.
            block: Milliseconds to block waiting for data.
                ``None`` means non-blocking.
                ``0`` means block indefinitely.

        Returns:
            List of ``(stream_name, entry_id, fields)`` tuples.
            Returns empty list on timeout or when no entries are available.
        """
        if not streams:
            return []
        result = cast(
            list[tuple[str, list[tuple[str, dict[str, str]]]]],
            self._client.xreadgroup(
                groupname=group,
                consumername=consumer,
                streams=streams,  # type: ignore[arg-type]
                count=count,
                block=block,
            ),
        )
        if not result:
            return []
        entries: list[tuple[str, str, dict[str, str]]] = []
        for stream_name, stream_entries in result:
            for entry_id, fields in stream_entries:
                entries.append((stream_name, entry_id, fields))
        return entries

    def xack(self, stream: str, group: str, entry_id: str) -> int:
        """Acknowledge a stream entry. Returns number acknowledged."""
        result: int = self._client.xack(self._prefixed(stream), group, entry_id)  # type: ignore[assignment]
        return result

    def xadd_capped(
        self,
        stream: str,
        fields: dict[str, str],
        maxlen: int,
        *,
        approximate: bool = True,
    ) -> str:
        """Add entry to a capped stream (MAXLEN).

        Args:
            stream: Stream name.
            fields: Field dict to add.
            maxlen: Maximum stream length. Must be positive.
            approximate: When True (default), use Redis ``MAXLEN ~`` for cheaper
                trimming. When False, trim exactly to ``maxlen``.
        """
        if maxlen < 1:
            raise ValueError(f"maxlen must be positive, got {maxlen}")
        if not fields:
            raise ValueError("fields must be a non-empty dict")
        entry_id: str = self._client.xadd(  # type: ignore[assignment]
            self._prefixed(stream),
            fields,  # type: ignore[arg-type]
            maxlen=maxlen,
            approximate=approximate,
        )
        return entry_id

    def _xadd_capped_expire(
        self,
        fq_stream: str,
        fields: dict[str, str],
        maxlen: int,
        ttl_seconds: int,
        *,
        approximate: bool,
    ) -> str:
        if maxlen < 1:
            raise ValueError(f"maxlen must be positive, got {maxlen}")
        if not fields:
            raise ValueError("fields must be a non-empty dict")
        if ttl_seconds < 1:
            raise ValueError(f"ttl_seconds must be positive, got {ttl_seconds}")
        trim_op = "~" if approximate else "="
        flattened_fields = [item for pair in fields.items() for item in pair]
        entry_id = cast(
            str,
            self._client.eval(
                _XADD_CAPPED_EXPIRE_SCRIPT,
                1,
                fq_stream,
                trim_op,
                str(maxlen),
                str(ttl_seconds),
                *flattened_fields,
            ),
        )
        return entry_id

    def xadd_capped_expire(
        self,
        stream: str,
        fields: dict[str, str],
        maxlen: int,
        ttl_seconds: int,
        *,
        approximate: bool = True,
    ) -> str:
        """Atomically XADD (MAXLEN-capped) and EXPIRE a stream in one round trip.

        Runs as a single Lua script server-side, so a fresh stream always ends
        up with a TTL and a failed append can never leave a keyed stream with
        no expiry -- there is no gap between the two commands for a crash or
        network failure to land in.

        Args:
            stream: Stream name.
            fields: Field dict to add.
            maxlen: Maximum stream length. Must be positive.
            ttl_seconds: TTL to set on the stream key after the append. Must
                be positive.
            approximate: When True (default), use Redis ``MAXLEN ~`` for cheaper
                trimming. When False, trim exactly to ``maxlen``.
        """
        return self._xadd_capped_expire(
            self._prefixed(stream), fields, maxlen, ttl_seconds, approximate=approximate
        )

    def xadd_capped_expire_raw(
        self,
        fq_stream: str,
        fields: dict[str, str],
        maxlen: int,
        ttl_seconds: int,
        *,
        approximate: bool = True,
    ) -> str:
        """Same as ``xadd_capped_expire`` but for a fully-qualified stream name.

        Used by multi-project workers that need to publish to streams with
        different prefixes than the client's own prefix.
        """
        return self._xadd_capped_expire(
            fq_stream, fields, maxlen, ttl_seconds, approximate=approximate
        )

    def xread_after(
        self,
        stream: str,
        last_id: str = "0-0",
        count: int = 100,
    ) -> list[tuple[str, dict[str, str]]]:
        """Read entries from a stream after last_id (non-blocking).

        Returns list of (entry_id, fields) tuples.
        Returns empty list if the stream doesn't exist, has no new entries,
        or a Redis error occurs (logged as a warning).

        Args:
            stream: Stream name.
            last_id: Read entries after this ID. Defaults to ``"0-0"`` (all).
            count: Maximum number of entries to return. Must be positive.
        """
        if count < 1:
            raise ValueError(f"count must be positive, got {count}")
        try:
            result = cast(
                list[tuple[str, list[tuple[str, dict[str, str]]]]],
                self._client.xread(
                    {self._prefixed(stream): last_id},
                    count=count,
                    block=None,
                ),
            )
        except (
            redis.ConnectionError,
            redis.TimeoutError,
            redis.ResponseError,
            redis.AuthenticationError,
        ):
            logger.warning(
                "xread_after failed for stream %s (last_id=%s)",
                stream,
                last_id,
                exc_info=True,
            )
            return []
        if not result:
            return []
        return result[0][1]

    def stream_unread_count(self, stream: str, group: str) -> int:
        """Get the number of stream entries not yet delivered to any consumer.

        Returns the consumer group's lag — entries appended to the stream that
        no consumer has claimed yet. Pending entries (claimed but not ACKed)
        are excluded: they are in flight, not waiting in the queue. Returns 0
        if the stream or group doesn't exist.
        """
        try:
            groups: list[dict[str, Any]] = self._client.xinfo_groups(self._prefixed(stream))  # type: ignore[assignment]
        except redis.ResponseError:
            return 0
        # Runtime safety net: redis-py's stubs type xinfo_groups as ResponseT (a
        # broad union), so the # type: ignore[assignment] above is required to
        # narrow to list[dict[str, Any]].  In practice the command always returns
        # a list, but we keep this guard to handle unexpected responses from custom
        # Redis proxies or future library changes without raising an AttributeError.
        if not isinstance(groups, list):
            logger.warning(
                "xinfo_groups returned unexpected type %s for stream %r",
                type(groups).__name__,
                stream,
            )
            return 0
        for g in groups:
            if g.get("name") == group:
                # lag can be -1 (unknown) on empty streams; treat as 0.
                return max(g.get("lag") or 0, 0)
        return 0

    def xpending_count(self, stream: str, group: str, entry_id: str) -> int:
        """Return how many times a specific pending entry has been delivered.

        Queries XPENDING with the exact entry ID range to retrieve the
        delivery count for that entry.  Returns 0 if the entry is not in
        the pending list (already ACKed) or if an error occurs.

        Args:
            stream: Stream name.
            group: Consumer group name.
            entry_id: The stream entry ID to look up.
        """
        try:
            entries = self._client.xpending_range(
                self._prefixed(stream), group, min=entry_id, max=entry_id, count=1
            )
        except Exception:
            logger.warning(
                "xpending_count failed for stream %s entry %s; treating as 0 deliveries",
                stream,
                entry_id,
                exc_info=True,
            )
            return 0
        if not entries:
            return 0
        count = entries[0].get("times_delivered", 0)  # type: ignore[index]
        return int(count)

    def xdel(self, stream: str, *entry_ids: str) -> int:
        """Delete entries from a stream by ID. Returns 0 immediately if no IDs given."""
        if not entry_ids:
            return 0
        result: int = self._client.xdel(self._prefixed(stream), *entry_ids)  # type: ignore[assignment]
        return result

    def xadd_raw(self, fq_stream: str, fields: dict[str, str]) -> str:
        """Add entry to stream using a fully-qualified (already-prefixed) name.

        Returns the entry ID.
        """
        entry_id: str = self._client.xadd(fq_stream, fields)  # type: ignore[assignment, arg-type]
        return entry_id

    def xadd_capped_raw(
        self,
        fq_stream: str,
        fields: dict[str, str],
        maxlen: int,
        *,
        approximate: bool = True,
    ) -> str:
        """Add entry to a capped stream using a fully-qualified (already-prefixed) name.

        Used by multi-project workers that need to publish to streams with
        different prefixes than the client's own prefix.
        """
        if maxlen < 1:
            raise ValueError(f"maxlen must be positive, got {maxlen}")
        if not fields:
            raise ValueError("fields must be a non-empty dict")
        entry_id: str = self._client.xadd(  # type: ignore[assignment]
            fq_stream,
            fields,  # type: ignore[arg-type]
            maxlen=maxlen,
            approximate=approximate,
        )
        return entry_id

    def xack_raw(self, fq_stream: str, group: str, entry_id: str) -> int:
        """Acknowledge a stream entry using a fully-qualified stream name."""
        result: int = self._client.xack(fq_stream, group, entry_id)  # type: ignore[assignment]
        return result

    def xpending_count_raw(self, fq_stream: str, group: str, entry_id: str) -> int:
        """Return delivery count for a pending entry using a fully-qualified stream name."""
        try:
            entries = self._client.xpending_range(
                fq_stream, group, min=entry_id, max=entry_id, count=1
            )
        except Exception:
            logger.warning(
                "xpending_count_raw failed for stream %s entry %s; treating as 0 deliveries",
                fq_stream,
                entry_id,
                exc_info=True,
            )
            return 0
        if not entries:
            return 0
        count = entries[0].get("times_delivered", 0)  # type: ignore[index]
        return int(count)

    @staticmethod
    def _is_missing_stream_error(error: redis.ResponseError) -> bool:
        text = str(error).lower()
        return "no such key" in text

    def _inspect_consumer_group_fq(self, fq_stream: str, group: str) -> ConsumerGroupInspection:
        """Inspect XINFO GROUPS on a fully-qualified stream without writing."""
        try:
            groups = cast(list[dict[str, Any]], self._client.xinfo_groups(fq_stream))
        except redis.ResponseError as e:
            if self._is_missing_stream_error(e):
                return ConsumerGroupInspection.MISSING
            raise

        if not isinstance(groups, list):
            raise TypeError(
                f"xinfo_groups returned {type(groups).__name__} for stream {fq_stream!r}"
            )
        for item in groups:
            if not isinstance(item, dict):
                raise TypeError(
                    f"xinfo_groups returned malformed group entry for stream {fq_stream!r}"
                )
            if item.get("name") == group:
                return ConsumerGroupInspection.EXISTS
        return ConsumerGroupInspection.MISSING

    def inspect_consumer_group_raw(self, fq_stream: str, group: str) -> ConsumerGroupInspection:
        """Return whether *group* exists on a fully-qualified stream.

        Missing streams are reported as ``MISSING``; other Redis failures are
        left for callers to handle according to their own retry policy.
        """
        return self._inspect_consumer_group_fq(fq_stream, group)

    def inspect_consumer_group(self, stream: str, group: str) -> ConsumerGroupInspection:
        """Return whether *group* exists on a prefixed stream."""
        return self._inspect_consumer_group_fq(self._prefixed(stream), group)

    def _create_consumer_group_fq(self, fq_stream: str, group: str) -> None:
        try:
            self._client.xgroup_create(name=fq_stream, groupname=group, id="0", mkstream=True)
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    def _ensure_consumer_group_fq(self, fq_stream: str, group: str) -> None:
        status = self._inspect_consumer_group_fq(fq_stream, group)
        if status is ConsumerGroupInspection.EXISTS:
            return
        self._create_consumer_group_fq(fq_stream, group)

    def ensure_consumer_group_raw(self, fq_stream: str, group: str) -> None:
        """Create consumer group on a fully-qualified stream name.

        Also creates the stream if needed (MKSTREAM).
        Idempotent -- safe to call on every startup.
        """
        self._ensure_consumer_group_fq(fq_stream, group)

    def set_nx_ex_raw(self, fq_key: str, value: str, ttl: int) -> bool:
        """SET key value NX EX ttl using a fully-qualified key."""
        return self._client.set(fq_key, value, nx=True, ex=ttl) is not None

    def get_raw(self, fq_key: str) -> str | None:
        """GET using a fully-qualified key."""
        val = self._client.get(fq_key)
        return str(val) if val is not None else None

    def delete_raw(self, *fq_keys: str) -> int:
        """DEL using fully-qualified keys."""
        if not fq_keys:
            return 0
        result: int = self._client.delete(*fq_keys)  # type: ignore[assignment]
        return result

    def set_ex_raw(self, fq_key: str, value: str | int, ttl: int) -> None:
        """SET key value EX ttl using a fully-qualified key."""
        self._client.set(fq_key, value, ex=ttl)

    def incr_raw(self, fq_key: str) -> int:
        """INCR using a fully-qualified key."""
        result: int = self._client.incr(fq_key)  # type: ignore[assignment]
        return result

    def expire_raw(self, fq_key: str, seconds: int) -> None:
        """EXPIRE key seconds using a fully-qualified key."""
        self._client.expire(fq_key, seconds)

    def ensure_consumer_group(self, stream: str, group: str) -> None:
        """Create consumer group if it doesn't exist.

        Also creates the stream if needed (MKSTREAM).
        Idempotent -- safe to call on every startup.
        """
        self._ensure_consumer_group_fq(self._prefixed(stream), group)

    # ------------------------------------------------------------------
    # Key/value wrapper methods (auto-prefix)
    # ------------------------------------------------------------------

    def set_nx_ex(self, key: str, value: str, ttl: int) -> bool:
        """SET key value NX EX ttl. Returns True if set."""
        return self._client.set(self._prefixed(key), value, nx=True, ex=ttl) is not None

    def set_ex(self, key: str, value: str | int, ttl: int) -> None:
        """SET key value EX ttl."""
        self._client.set(self._prefixed(key), value, ex=ttl)

    def set_value(self, key: str, value: str | int) -> None:
        """SET key value without expiry."""
        self._client.set(self._prefixed(key), value)

    def get(self, key: str) -> str | None:
        """GET key."""
        val = self._client.get(self._prefixed(key))
        return str(val) if val is not None else None

    def exists(self, key: str) -> bool:
        """EXISTS key."""
        return bool(self._client.exists(self._prefixed(key)))

    def delete(self, *keys: str) -> int:
        """DEL key [key ...]. Returns 0 immediately if no keys given."""
        if not keys:
            return 0
        result: int = self._client.delete(*(self._prefixed(k) for k in keys))  # type: ignore[assignment]
        return result

    def incr(self, key: str) -> int:
        """INCR key."""
        result: int = self._client.incr(self._prefixed(key))  # type: ignore[assignment]
        return result

    def next_monotonic_version(self, key: str) -> float:
        """Return a Redis-clock-backed, strictly increasing version."""
        result = cast(
            Any,
            self._client.eval(
                _MONOTONIC_VERSION_SCRIPT,
                1,
                self._prefixed(key),
            ),
        )
        return float(result)

    def expire(self, key: str, seconds: int) -> bool:
        """EXPIRE key seconds."""
        return bool(self._client.expire(self._prefixed(key), seconds))

    def persist(self, key: str) -> bool:
        """PERSIST key. Removes any TTL so wall-clock expiry cannot delete it."""
        return bool(self._client.persist(self._prefixed(key)))

    def ttl(self, key: str) -> int:
        """TTL key."""
        result: int = self._client.ttl(self._prefixed(key))  # type: ignore[assignment]
        return result

    def hgetall(self, key: str) -> dict[str, str]:
        """HGETALL key."""
        result: dict[str, str] = self._client.hgetall(self._prefixed(key))  # type: ignore[assignment]
        return result

    def hget(self, key: str, field: str) -> str | None:
        """HGET key field."""
        val = self._client.hget(self._prefixed(key), field)
        return str(val) if val is not None else None

    def hset(self, key: str, field: str, value: str) -> int:
        """HSET key field value."""
        result: int = self._client.hset(self._prefixed(key), field, value)  # type: ignore[assignment]
        return result

    def hset_mapping(self, key: str, mapping: dict[str, str]) -> int:
        """HSET key field value [field value ...] in a single round trip.

        No-ops (and issues no command) when ``mapping`` is empty.
        """
        if not mapping:
            return 0
        result: int = self._client.hset(self._prefixed(key), mapping=mapping)  # type: ignore[assignment,arg-type]
        return result

    def hset_json_if_newer(
        self,
        key: str,
        field: str,
        value: str,
        minted_at: float,
    ) -> bool:
        """Atomically HSET a JSON value when its timestamp is newer.

        The stored JSON object is expected to contain ``minted_at``. WATCH is
        used instead of a read-then-write sequence so concurrent per-project
        orchestrators cannot let an older OAuth rotation overwrite a newer one.
        """
        fq_key = self._prefixed(key)
        while True:
            pipe = self._client.pipeline()
            try:
                pipe.watch(fq_key)
                current = pipe.hget(fq_key, field)
                if current:
                    try:
                        current_obj = json.loads(str(current))
                        current_minted_at = float(current_obj.get("minted_at", 0))
                    except (json.JSONDecodeError, TypeError, ValueError, AttributeError):
                        current_minted_at = 0.0
                    if current_minted_at >= minted_at:
                        pipe.unwatch()
                        return False
                pipe.multi()
                pipe.hset(fq_key, field, value)
                pipe.execute()
                return True
            except redis.WatchError:
                continue
            finally:
                pipe.reset()

    def hgetall_raw(self, fq_key: str) -> dict[str, str]:
        """HGETALL on a fully-qualified key name."""
        result: dict[str, str] = self._client.hgetall(fq_key)  # type: ignore[assignment]
        return result

    def hset_raw(self, fq_key: str, field: str, value: str) -> int:
        """HSET on a fully-qualified key name."""
        result: int = self._client.hset(fq_key, field, value)  # type: ignore[assignment]
        return result

    # ------------------------------------------------------------------
    # Set operations (auto-prefix)
    # ------------------------------------------------------------------

    def sadd(self, key: str, *members: str) -> int:
        """SADD key member [member ...]. Returns 0 immediately if no members given."""
        if not members:
            return 0
        result: int = self._client.sadd(self._prefixed(key), *members)  # type: ignore[assignment]
        return result

    def srem(self, key: str, *members: str) -> int:
        """SREM key member [member ...]. Returns 0 immediately if no members given."""
        if not members:
            return 0
        result: int = self._client.srem(self._prefixed(key), *members)  # type: ignore[assignment]
        return result

    def smembers(self, key: str) -> set[str]:
        """SMEMBERS key."""
        result: set[str] = self._client.smembers(self._prefixed(key))  # type: ignore[assignment]
        return result

    def scard(self, key: str) -> int:
        """SCARD key."""
        result: int = self._client.scard(self._prefixed(key))  # type: ignore[assignment]
        return result

    def sismember(self, key: str, member: str) -> bool:
        """SISMEMBER key member."""
        return bool(self._client.sismember(self._prefixed(key), member))

    def zadd(self, key: str, mapping: dict[str, float]) -> int:
        """ZADD key score member [score member ...]. Returns 0 if mapping is empty."""
        if not mapping:
            return 0
        result: int = self._client.zadd(self._prefixed(key), mapping)  # type: ignore[assignment]
        return result

    def zadd_expiring(self, key: str, mapping: dict[str, float], ttl: int) -> int:
        """Atomically ZADD members and refresh the containing key's TTL."""
        if not mapping:
            return 0
        if ttl < 1:
            raise ValueError(f"ttl must be positive, got {ttl}")
        with self._client.pipeline(transaction=True) as pipe:
            pipe.zadd(self._prefixed(key), mapping)
            pipe.expire(self._prefixed(key), ttl)
            results = pipe.execute()
        return int(results[0])

    def zrangebyscore(
        self,
        key: str,
        min_score: float | str,
        max_score: float | str,
        start: int = 0,
        num: int | None = None,
    ) -> list[str]:
        """ZRANGEBYSCORE key min max [LIMIT start num]."""
        kwargs: dict[str, Any] = {}
        if num is not None:
            kwargs["start"] = start
            kwargs["num"] = num
        result: list[str] = self._client.zrangebyscore(  # type: ignore[assignment]
            self._prefixed(key), min_score, max_score, **kwargs
        )
        return result

    def zrem(self, key: str, *members: str) -> int:
        """ZREM key member [member ...]. Returns 0 immediately if no members given."""
        if not members:
            return 0
        result: int = self._client.zrem(self._prefixed(key), *members)  # type: ignore[assignment]
        return result

    def zremrangebyscore(
        self,
        key: str,
        min_score: float | str,
        max_score: float | str,
    ) -> int:
        """ZREMRANGEBYSCORE key min max."""
        result: int = self._client.zremrangebyscore(  # type: ignore[assignment]
            self._prefixed(key), min_score, max_score
        )
        return result

    def zscore(self, key: str, member: str) -> float | None:
        """ZSCORE key member."""
        result: float | None = self._client.zscore(  # type: ignore[assignment]
            self._prefixed(key), member
        )
        return float(result) if result is not None else None

    def zcard(self, key: str) -> int:
        """ZCARD key."""
        result: int = self._client.zcard(self._prefixed(key))  # type: ignore[assignment]
        return result

    def lpush(self, key: str, *values: str) -> int:
        """LPUSH key value [value ...]. Returns 0 immediately if no values given."""
        if not values:
            return 0
        result: int = self._client.lpush(self._prefixed(key), *values)  # type: ignore[assignment]
        return result

    def ltrim(self, key: str, start: int, end: int) -> None:
        """LTRIM key start end."""
        self._client.ltrim(self._prefixed(key), start, end)

    def lrange(self, key: str, start: int, end: int) -> list[str]:
        """LRANGE key start end."""
        result: list[str] = self._client.lrange(self._prefixed(key), start, end)  # type: ignore[assignment]
        return result

    def llen(self, key: str) -> int:
        """LLEN key."""
        result: int = self._client.llen(self._prefixed(key))  # type: ignore[assignment]
        return result

    # ------------------------------------------------------------------
    # Additional hash operations (auto-prefix)
    # ------------------------------------------------------------------

    def hlen(self, key: str) -> int:
        """HLEN key."""
        result: int = self._client.hlen(self._prefixed(key))  # type: ignore[assignment]
        return result

    def hdel(self, key: str, *fields: str) -> int:
        """HDEL key field [field ...]. Returns 0 immediately if no fields given."""
        if not fields:
            return 0
        result: int = self._client.hdel(self._prefixed(key), *fields)  # type: ignore[assignment]
        return result

    # ------------------------------------------------------------------
    # Stream info operations (auto-prefix)
    # ------------------------------------------------------------------

    def xinfo_consumers(self, stream: str, group: str) -> list[dict[str, Any]]:
        """XINFO CONSUMERS stream group."""
        result: list[dict[str, Any]] = self._client.xinfo_consumers(self._prefixed(stream), group)  # type: ignore[assignment]
        return result

    def xgroup_pending_snapshot(
        self,
        stream: str,
        group: str,
        *,
        pending_limit: int,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
        """Atomically read XINFO GROUPS, bounded XPENDING details, and XLEN."""
        if pending_limit < 1:
            raise ValueError(f"pending_limit must be positive, got {pending_limit}")
        fq_stream = self._prefixed(stream)
        with self._client.pipeline(transaction=True) as pipe:
            pipe.xinfo_groups(fq_stream)
            pipe.xpending_range(
                fq_stream,
                group,
                min="-",
                max="+",
                count=pending_limit,
            )
            pipe.xlen(fq_stream)
            raw = cast(list[Any], pipe.execute())
        if len(raw) != 3:
            raise TypeError("stream capacity transaction returned malformed response count")
        return (
            cast(list[dict[str, Any]], raw[0]),
            cast(list[dict[str, Any]], raw[1]),
            int(raw[2]),
        )

    def scan_iter(self, match: str) -> list[str]:
        """SCAN with match pattern. Returns list of unprefixed keys."""
        return [
            k.removeprefix(self._prefix)
            for k in self._client.scan_iter(match=self._prefixed(match))
        ]

    def scan_page(
        self,
        cursor: int = 0,
        *,
        match: str = "*",
        count: int = 100,
    ) -> tuple[int, list[str]]:
        """Return one bounded SCAN page with unprefixed key names."""
        if count < 1:
            raise ValueError(f"count must be positive, got {count}")
        next_cursor, keys = cast(
            Any,
            self._client.scan(
                cursor=cursor,
                match=self._prefixed(match),
                count=count,
            ),
        )
        if not isinstance(keys, (list, tuple)):
            raise TypeError(f"SCAN returned malformed keys payload: {type(keys).__name__}")
        return int(next_cursor), [str(key).removeprefix(self._prefix) for key in keys]

    def xlen(self, stream: str) -> int:
        """XLEN stream."""
        result: int = self._client.xlen(self._prefixed(stream))  # type: ignore[assignment]
        return result

    def xrange(
        self,
        stream: str,
        min_id: str = "-",
        max_id: str = "+",
        count: int | None = None,
    ) -> list[tuple[str, dict[str, str]]]:
        """XRANGE over a prefixed stream with typed string fields."""
        return cast(
            list[tuple[str, dict[str, str]]],
            self._client.xrange(
                self._prefixed(stream),
                min=min_id,
                max=max_id,
                count=count,
            ),
        )

    def xtrim_minid(self, stream: str, minid: str) -> int:
        """XTRIM stream MINID minid. Removes entries older than minid.

        Returns the number of entries removed.
        """
        result: int = self._client.xtrim(  # type: ignore[assignment]
            self._prefixed(stream), minid=minid, approximate=False
        )
        return result

    def xtrim_acked_entries(self, stream: str, group: str) -> int:
        """Reclaim delivered+ACKed entries from *stream* for consumer *group*.

        Trims via XTRIM MINID using the group's LOWEST still-pending entry id
        (so the un-ACKed PEL entry -- and everything appended after it -- is
        preserved). When nothing is pending, falls back to the group's
        last-delivered-id, which still never removes undelivered (lag) entries
        because their ids are strictly greater than last-delivered-id. Returns
        the number of entries removed (0 if the stream/group is missing, empty,
        or there is nothing safe to trim).
        """
        fq_stream = self._prefixed(stream)
        while True:
            pipe = self._client.pipeline()
            try:
                # WATCH covers consumer-group/Pending Entries List mutations:
                # XREADGROUP and XACK are stream-key writes. If a claim lands
                # between either metadata read and XTRIM, EXEC aborts and the
                # safe boundary is recomputed.
                pipe.watch(fq_stream)
                summary = pipe.xpending(fq_stream, group)
                minid: str | None = None
                if isinstance(summary, dict) and summary.get("pending"):
                    minid = summary.get("min")
                if not minid or minid == "0-0":
                    groups = pipe.xinfo_groups(fq_stream)
                    last_delivered = None
                    if isinstance(groups, list):
                        for info in groups:
                            if info.get("name") == group:
                                last_delivered = info.get("last-delivered-id")
                                break
                    if not last_delivered or last_delivered == "0-0":
                        pipe.unwatch()
                        return 0
                    milliseconds, sequence = str(last_delivered).split("-", 1)
                    minid = f"{milliseconds}-{int(sequence) + 1}"
                pipe.multi()
                pipe.xtrim(fq_stream, minid=minid, approximate=False)
                result = pipe.execute()
                return int(result[0] if result else 0)
            except redis.WatchError:
                continue
            except redis.ResponseError:
                # NOGROUP / missing stream means there is nothing safe to trim.
                return 0
            except (IndexError, KeyError):
                # fakeredis can expose malformed missing-group metadata.
                return 0
            finally:
                pipe.reset()

    def claim_round_robin_turn(
        self,
        turn_key: str,
        sequence_key: str,
        live_identities: list[str],
        ttl_seconds: int,
    ) -> str | None:
        """Atomically claim/read a leased round-robin turn.

        All callers see the same selected identity until the lease expires.
        The first caller after expiry advances the durable sequence exactly
        once, so stable live identities receive eventual service independent
        of process timing or wall-clock bucket parity.
        """
        if not live_identities:
            return None
        result = self._client.eval(
            _ROUND_ROBIN_TURN_SCRIPT,
            2,
            self._prefixed(turn_key),
            self._prefixed(sequence_key),
            str(max(1, int(ttl_seconds))),
            str(len(live_identities)),
            *live_identities,
        )
        return str(result) if result else None

    def xinfo_groups(self, stream: str) -> list[dict[str, Any]]:
        """XINFO GROUPS stream."""
        result: list[dict[str, Any]] = self._client.xinfo_groups(self._prefixed(stream))  # type: ignore[assignment]
        return result

    def xinfo_groups_raw(self, fq_stream: str) -> list[dict[str, Any]]:
        """XINFO GROUPS on a fully-qualified stream name (no prefix added)."""
        result: list[dict[str, Any]] = self._client.xinfo_groups(fq_stream)  # type: ignore[assignment]
        return result

    def xinfo_consumers_raw(self, fq_stream: str, group: str) -> list[dict[str, Any]]:
        """XINFO CONSUMERS on a fully-qualified stream name (no prefix added)."""
        result: list[dict[str, Any]] = self._client.xinfo_consumers(fq_stream, group)  # type: ignore[assignment]
        return result

    def delconsumer_raw(self, fq_stream: str, group: str, consumer: str) -> int:
        """XGROUP DELCONSUMER on a fully-qualified stream name.

        Returns the number of pending entries that belonged to *consumer*
        before Redis removed it. Pending entries are not made claimable by this
        command, so callers must not use it to clean up a consumer with pending
        work unless they intentionally want Redis to discard that consumer's
        PEL state. Returns 0 if the consumer or group does not exist.
        """
        try:
            result = cast(Any, self._client.xgroup_delconsumer(fq_stream, group, consumer))
            return int(result)
        except redis.ResponseError:
            return 0

    def xautoclaim_raw(
        self,
        fq_stream: str,
        group: str,
        consumer: str,
        min_idle_ms: int,
        start_id: str = "0-0",
        count: int = 100,
    ) -> tuple[str, list[tuple[str, dict[str, str]]]]:
        """XAUTOCLAIM on a fully-qualified stream name.

        Reclaims up to *count* PEL entries idle for at least *min_idle_ms*,
        transferring ownership to *consumer*. Returns ``(next_cursor, claimed)``
        where ``claimed`` is a list of ``(entry_id, fields)``. Entries that no
        longer exist in the stream (deleted) are dropped by Redis and not
        returned. Returns ``("0-0", [])`` on error or empty PEL.
        """
        try:
            result = cast(
                Any,
                self._client.xautoclaim(
                    fq_stream,
                    group,
                    consumer,
                    min_idle_time=min_idle_ms,
                    start_id=start_id,
                    count=count,
                ),
            )
        except redis.ResponseError:
            return "0-0", []
        # redis-py returns [next_cursor, [(id, fields), ...], [deleted_ids]].
        if not result:
            return "0-0", []
        next_cursor = str(result[0])
        claimed = [(str(eid), fields) for eid, fields in (result[1] or [])]
        return next_cursor, claimed

    def xrevrange(self, stream: str, count: int) -> list[tuple[str, dict[str, str]]]:
        """XREVRANGE stream + - COUNT count."""
        result: list[Any] = self._client.xrevrange(self._prefixed(stream), count=count)  # type: ignore[assignment]
        return result

    def pipeline(self, transaction: bool = True) -> "PrefixedPipeline":
        """Create a pipeline that automatically prefixes keys."""
        return PrefixedPipeline(self._client.pipeline(transaction=transaction), self._prefix)


class PrefixedPipeline:
    """Pipeline wrapper that automatically prefixes keys.

    Supports the same context-manager protocol as ``redis.client.Pipeline``:
    the underlying pipeline is reset on exit so resources are released even
    if an exception interrupts the batch.
    """

    def __init__(self, pipe: redis.client.Pipeline, prefix: str):  # type: ignore[type-arg]
        self._pipe = pipe
        self._prefix = prefix

    def __enter__(self) -> "PrefixedPipeline":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: types.TracebackType | None,
    ) -> None:
        self._pipe.reset()

    def _prefixed(self, key: str) -> str:
        return self._prefix + key

    def hincrby(self, key: str, field: str, amount: int = 1) -> "PrefixedPipeline":
        self._pipe.hincrby(self._prefixed(key), field, amount)
        return self

    def hset(self, key: str, field: str, value: str) -> "PrefixedPipeline":
        self._pipe.hset(self._prefixed(key), field, value)
        return self

    def expire(self, key: str, seconds: int) -> "PrefixedPipeline":
        self._pipe.expire(self._prefixed(key), seconds)
        return self

    def incr(self, key: str) -> "PrefixedPipeline":
        self._pipe.incr(self._prefixed(key))
        return self

    def decr(self, key: str) -> "PrefixedPipeline":
        self._pipe.decr(self._prefixed(key))
        return self

    def delete(self, *keys: str) -> "PrefixedPipeline":
        """DEL key [key ...] (queued in pipeline). No-op if no keys given."""
        if keys:
            self._pipe.delete(*(self._prefixed(k) for k in keys))
        return self

    def sadd(self, key: str, *members: str) -> "PrefixedPipeline":
        """SADD key member [member ...] (queued in pipeline). No-op if no members given."""
        if members:
            self._pipe.sadd(self._prefixed(key), *members)
        return self

    def srem(self, key: str, *members: str) -> "PrefixedPipeline":
        """SREM key member [member ...] (queued in pipeline). No-op if no members given."""
        if members:
            self._pipe.srem(self._prefixed(key), *members)
        return self

    def hdel(self, key: str, *fields: str) -> "PrefixedPipeline":
        """HDEL key field [field ...] (queued in pipeline). No-op if no fields given."""
        if fields:
            self._pipe.hdel(self._prefixed(key), *fields)
        return self

    def execute(self) -> list[Any]:
        """Execute all queued commands. Returns a list with one result per command."""
        return self._pipe.execute()
