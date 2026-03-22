"""Redis connection and stream helper methods.

Thin wrapper around redis-py providing connection pooling and typed
stream operations with simplified return types.
"""

from __future__ import annotations

import logging
import types
from typing import Any

import redis

from orcest.shared.config import RedisConfig

logger = logging.getLogger(__name__)


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
        self._prefix = config.key_prefix + ":"

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
        instance._prefix = key_prefix + ":"
        return instance

    @property
    def client(self) -> redis.Redis:
        """Raw redis client for operations not covered by helpers."""
        return self._client

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
                instead of new ones. Uses ID ``"0"`` instead of ``">"``.
        """
        entry_id = "0" if pending else ">"
        result = self._client.xreadgroup(
            groupname=group,
            consumername=consumer,
            streams={self._prefixed(stream): entry_id},
            count=count,
            block=block_ms,
        )
        if not result:
            return []
        # result shape: [[stream_name, [(id, fields), ...]]]
        return result[0][1]  # type: ignore[index]

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
        result = self._client.xreadgroup(
            groupname=group,
            consumername=consumer,
            streams=streams,  # type: ignore[arg-type]
            count=count,
            block=block,
        )
        if not result:
            return []
        entries: list[tuple[str, str, dict[str, str]]] = []
        for stream_name, stream_entries in result:  # type: ignore[union-attr]
            for entry_id, fields in stream_entries:
                entries.append((stream_name, entry_id, fields))
        return entries

    def xack(self, stream: str, group: str, entry_id: str) -> int:
        """Acknowledge a stream entry. Returns number acknowledged."""
        result: int = self._client.xack(self._prefixed(stream), group, entry_id)  # type: ignore[assignment]
        return result

    def xadd_capped(self, stream: str, fields: dict[str, str], maxlen: int) -> str:
        """Add entry to a capped stream (approximate MAXLEN).

        Args:
            stream: Stream name.
            fields: Field dict to add.
            maxlen: Approximate maximum stream length. Must be positive.
        """
        if maxlen < 1:
            raise ValueError(f"maxlen must be positive, got {maxlen}")
        if not fields:
            raise ValueError("fields must be a non-empty dict")
        entry_id: str = self._client.xadd(  # type: ignore[assignment]
            self._prefixed(stream),
            fields,  # type: ignore[arg-type]
            maxlen=maxlen,
            approximate=True,
        )
        return entry_id

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
            result = self._client.xread({self._prefixed(stream): last_id}, count=count, block=None)
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
        return result[0][1]  # type: ignore[index]

    def stream_queue_depth(self, stream: str, group: str) -> int:
        """Get total unprocessed entries for a consumer group.

        Returns the sum of pending (delivered but not ACKed) and lag
        (not yet delivered). Returns 0 if the stream or group doesn't exist.
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
                pending = g.get("pending", 0)
                # lag can be -1 (unknown) on empty streams; treat as 0.
                lag = max(g.get("lag") or 0, 0)
                return pending + lag
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

    def xadd_capped_raw(self, fq_stream: str, fields: dict[str, str], maxlen: int) -> str:
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
            approximate=True,
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

    def ensure_consumer_group_raw(self, fq_stream: str, group: str) -> None:
        """Create consumer group on a fully-qualified stream name.

        Also creates the stream if needed (MKSTREAM).
        Idempotent -- safe to call on every startup.
        """
        try:
            self._client.xgroup_create(
                name=fq_stream, groupname=group, id="0", mkstream=True
            )
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

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

    def ensure_consumer_group(self, stream: str, group: str) -> None:
        """Create consumer group if it doesn't exist.

        Also creates the stream if needed (MKSTREAM).
        Idempotent -- safe to call on every startup.
        """
        try:
            self._client.xgroup_create(
                name=self._prefixed(stream), groupname=group, id="0", mkstream=True
            )
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    # ------------------------------------------------------------------
    # Key/value wrapper methods (auto-prefix)
    # ------------------------------------------------------------------

    def set_nx_ex(self, key: str, value: str, ttl: int) -> bool:
        """SET key value NX EX ttl. Returns True if set."""
        return self._client.set(self._prefixed(key), value, nx=True, ex=ttl) is not None

    def set_ex(self, key: str, value: str | int, ttl: int) -> None:
        """SET key value EX ttl."""
        self._client.set(self._prefixed(key), value, ex=ttl)

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

    def expire(self, key: str, seconds: int) -> bool:
        """EXPIRE key seconds."""
        return bool(self._client.expire(self._prefixed(key), seconds))

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

    def scan_iter(self, match: str) -> list[str]:
        """SCAN with match pattern. Returns list of unprefixed keys."""
        return [
            k.removeprefix(self._prefix)
            for k in self._client.scan_iter(match=self._prefixed(match))
        ]

    def xlen(self, stream: str) -> int:
        """XLEN stream."""
        result: int = self._client.xlen(self._prefixed(stream))  # type: ignore[assignment]
        return result

    def xinfo_groups(self, stream: str) -> list[dict[str, Any]]:
        """XINFO GROUPS stream."""
        result: list[dict[str, Any]] = self._client.xinfo_groups(self._prefixed(stream))  # type: ignore[assignment]
        return result

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
