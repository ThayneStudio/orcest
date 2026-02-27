"""Distributed locking using Redis SET NX EX with owner verification.

Uses Lua scripts for atomic release and refresh to prevent race conditions
where a lock could be released by a non-owner.
"""

import uuid

from orcest.shared.redis_client import RedisClient


class RedisLock:
    """Distributed lock backed by Redis SET NX EX."""

    def __init__(
        self,
        redis_client: RedisClient,
        key: str,
        ttl: int = 1800,  # 30 minutes
        owner: str | None = None,
    ):
        self.redis = redis_client
        self.key = key
        self.ttl = ttl
        self.owner = owner or str(uuid.uuid4())
        self._held = False

    def acquire(self) -> bool:
        """Attempt to acquire the lock. Returns True if successful."""
        result = self.redis.client.set(
            self.key, self.owner, nx=True, ex=self.ttl
        )
        self._held = result is not None
        return self._held

    def release(self) -> bool:
        """Release the lock, but only if we still own it.

        Uses a Lua script for atomic check-and-delete.
        """
        lua_script = """
        if redis.call("GET", KEYS[1]) == ARGV[1] then
            return redis.call("DEL", KEYS[1])
        else
            return 0
        end
        """
        result = self.redis.client.register_script(lua_script)(
            keys=[self.key], args=[self.owner]
        )
        self._held = False
        return result == 1

    def refresh(self) -> bool:
        """Refresh the TTL, but only if we still own it.

        Uses a Lua script for atomic check-and-expire.
        """
        lua_script = """
        if redis.call("GET", KEYS[1]) == ARGV[1] then
            return redis.call("EXPIRE", KEYS[1], ARGV[2])
        else
            return 0
        end
        """
        result = self.redis.client.register_script(lua_script)(
            keys=[self.key], args=[self.owner, str(self.ttl)]
        )
        return result == 1

    @property
    def is_held(self) -> bool:
        return self._held


def make_pr_lock_key(pr_number: int) -> str:
    """Generate the Redis key for a PR lock."""
    return f"lock:pr:{pr_number}"
