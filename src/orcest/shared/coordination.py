"""Distributed locking using Redis SET NX EX with owner verification.

Uses Lua scripts for atomic release and refresh to prevent race conditions
where a lock could be released by a non-owner.
"""

import types
import uuid

from orcest.shared.config import RunnerConfig
from orcest.shared.redis_client import RedisClient

# Lua script for atomic check-and-delete (release).
_RELEASE_SCRIPT = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
else
    return 0
end
"""

# Lua script for atomic check-and-expire (refresh).
_REFRESH_SCRIPT = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("EXPIRE", KEYS[1], ARGV[2])
else
    return 0
end
"""


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
        self.key = redis_client._prefixed(key)
        self.ttl = ttl
        self.owner = owner or str(uuid.uuid4())
        self._held = False
        # Register Lua scripts once per lock instance rather than on every
        # call.  register_script returns a Script object that uses EVALSHA
        # with automatic EVAL fallback, so the Redis server caches the
        # compiled script by SHA.
        self._release_script = self.redis.client.register_script(_RELEASE_SCRIPT)
        self._refresh_script = self.redis.client.register_script(_REFRESH_SCRIPT)

    def acquire(self) -> bool:
        """Attempt to acquire the lock. Returns True if successful."""
        result = self.redis.client.set(self.key, self.owner, nx=True, ex=self.ttl)
        self._held = result is not None
        return self._held

    def release(self) -> bool:
        """Release the lock, but only if we still own it.

        Uses a Lua script for atomic check-and-delete.
        Returns True if the lock was actually deleted (we were the owner).
        Always clears _held since after calling release() we no longer
        consider ourselves the holder regardless of outcome.
        """
        result = self._release_script(keys=[self.key], args=[self.owner])
        self._held = False
        return result == 1

    def refresh(self) -> bool:
        """Refresh the TTL, but only if we still own it.

        Uses a Lua script for atomic check-and-expire.
        """
        result = self._refresh_script(keys=[self.key], args=[self.owner, str(self.ttl)])
        return result == 1

    def __enter__(self) -> "RedisLock":
        if not self.acquire():
            raise RuntimeError(f"Failed to acquire lock: {self.key}")
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: types.TracebackType | None,
    ) -> None:
        self.release()

    @property
    def is_held(self) -> bool:
        """Advisory flag indicating whether this client believes it holds the lock.

        This reflects client-side state only and does NOT guarantee that the
        lock is still present in Redis.  The flag becomes stale when the TTL
        expires without a refresh, or when the process is paused long enough
        for the key to vanish.  The heartbeat loop in the worker mitigates
        this in normal operation, but callers that need a hard guarantee should
        call ``verify()`` instead.
        """
        return self._held

    def verify(self) -> bool:
        """Check whether this client still owns the lock in Redis.

        Performs a GET and compares the stored value against our owner token.
        Returns True only if the key exists *and* its value matches our owner.

        Unlike ``is_held``, this reflects actual Redis state at the moment of
        the call, at the cost of a round-trip.
        """
        return self.redis.client.get(self.key) == self.owner


def make_pr_lock_key(repo: str, pr_number: int) -> str:
    """Generate the Redis key for a PR lock."""
    return f"lock:pr:{repo}:{pr_number}"


def make_issue_lock_key(repo: str, issue_number: int) -> str:
    """Generate the Redis key for an issue lock."""
    return f"lock:issue:{repo}:{issue_number}"


def make_pending_task_key(repo: str, resource_type: str, resource_id: int) -> str:
    """Generate the Redis key for a pending task marker."""
    return f"pending:{resource_type}:{repo}:{resource_id}"


def compute_pending_task_ttl(runner_config: RunnerConfig) -> int:
    """Compute the pending-task marker TTL from a live RunnerConfig.

    TTL = timeout × max_retries + 5-minute buffer.  This bounds the
    crash-orphaned-marker window to the actual worst-case runner duration.
    Callers with a loaded config should use this instead of the module-level
    fallback constant so that non-default timeout/max_retries values are
    reflected in the TTL.
    """
    return runner_config.timeout * runner_config.max_retries + 300


# Fallback TTL computed from RunnerConfig *defaults*.  Used as the default
# argument for set_pending_task; callers that have a live RunnerConfig should
# pass compute_pending_task_ttl(runner_config) instead.
_PENDING_TASK_TTL = compute_pending_task_ttl(RunnerConfig())


def set_pending_task(
    redis_client: RedisClient,
    repo: str,
    resource_type: str,
    resource_id: int,
    task_id: str,
    ttl: int = _PENDING_TASK_TTL,
) -> bool:
    """Mark a task as pending for a resource. Returns True if set (no existing pending task).

    Uses SET NX EX for atomic check-and-set with a TTL safety net.
    """
    key = make_pending_task_key(repo, resource_type, resource_id)
    return redis_client.set_nx_ex(key, task_id, ttl)


def clear_pending_task(
    redis_client: RedisClient,
    repo: str,
    resource_type: str,
    resource_id: int,
) -> None:
    """Clear the pending task marker for a resource."""
    key = make_pending_task_key(repo, resource_type, resource_id)
    redis_client.delete(key)
