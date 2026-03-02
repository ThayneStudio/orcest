"""Redis connection and stream helper methods.

Thin wrapper around redis-py providing connection pooling and typed
stream operations with simplified return types.
"""

from __future__ import annotations

import logging

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
        )
        self._client: redis.Redis[str] = redis.Redis(connection_pool=self._pool)  # type: ignore[type-arg]

    @classmethod
    def from_client(cls, client: redis.Redis[str]) -> "RedisClient":
        """Create a RedisClient wrapping a pre-built redis client.

        Useful in tests to inject a fakeredis instance without opening a real
        connection.
        """
        instance: RedisClient = object.__new__(cls)
        instance._client = client
        instance._pool = client.connection_pool
        return instance

    @property
    def client(self) -> redis.Redis[str]:  # type: ignore[type-arg]
        """Raw redis client for operations not covered by helpers."""
        return self._client

    def close(self) -> None:
        """Close the connection pool and release all connections."""
        self._pool.disconnect()

    def __enter__(self) -> "RedisClient":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

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
        entry_id: str = self._client.xadd(stream, fields)  # type: ignore[assignment, arg-type]
        return entry_id

    def xreadgroup(
        self,
        group: str,
        consumer: str,
        stream: str,
        count: int = 1,
        block_ms: int | None = 5000,
    ) -> list[tuple[str, dict[str, str]]]:
        """Read new entries from a consumer group.

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
        """
        result = self._client.xreadgroup(
            groupname=group,
            consumername=consumer,
            streams={stream: ">"},
            count=count,
            block=block_ms,
        )
        if not result:
            return []
        # result shape: [[stream_name, [(id, fields), ...]]]
        return result[0][1]  # type: ignore[index]

    def xack(self, stream: str, group: str, entry_id: str) -> int:
        """Acknowledge a stream entry. Returns number acknowledged."""
        result: int = self._client.xack(stream, group, entry_id)  # type: ignore[assignment]
        return result

    def xadd_capped(self, stream: str, fields: dict[str, str], maxlen: int = 2000) -> str:
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
        entry_id: str = self._client.xadd(  # type: ignore[assignment, arg-type]
            stream, fields, maxlen=maxlen, approximate=True
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
            result = self._client.xread({stream: last_id}, count=count, block=None)
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

    def ensure_consumer_group(self, stream: str, group: str) -> None:
        """Create consumer group if it doesn't exist.

        Also creates the stream if needed (MKSTREAM).
        Idempotent -- safe to call on every startup.
        """
        try:
            self._client.xgroup_create(name=stream, groupname=group, id="0", mkstream=True)
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise
