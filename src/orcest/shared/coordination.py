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
        self.key = key
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
        return self._held


def make_pr_lock_key(repo: str, pr_number: int) -> str:
    """Generate the Redis key for a PR lock."""
    return f"lock:pr:{repo}:{pr_number}"


def make_issue_lock_key(repo: str, issue_number: int) -> str:
    """Generate the Redis key for an issue lock."""
    return f"lock:issue:{repo}:{issue_number}"


def make_pending_task_key(repo: str, resource_type: str, resource_id: int) -> str:
    """Generate the Redis key for a pending task marker."""
    return f"pending:{resource_type}:{repo}:{resource_id}"


# Pending-task marker TTL: runner default timeout × default max_retries + 5-minute
# buffer.  The orchestrator sets this marker but doesn't know the worker's runtime
# runner config, so we derive it from RunnerConfig defaults.  This is much tighter
# than the previous 7200 s (2 h), bounding the crash-orphaned-marker window.
_DEFAULT_RUNNER = RunnerConfig()
_PENDING_TASK_TTL = _DEFAULT_RUNNER.timeout * _DEFAULT_RUNNER.max_retries + 300  # +5-min buffer


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
    return redis_client.client.set(key, task_id, nx=True, ex=ttl) is not None


def clear_pending_task(
    redis_client: RedisClient,
    repo: str,
    resource_type: str,
    resource_id: int,
) -> None:
    """Clear the pending task marker for a resource."""
    key = make_pending_task_key(repo, resource_type, resource_id)
    redis_client.client.delete(key)
