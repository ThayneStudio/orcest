"""Tests for orcest.shared.coordination using fakeredis."""

import pytest

from orcest.shared.config import RunnerConfig
from orcest.shared.coordination import (
    RedisLock,
    compute_pending_task_ttl,
    make_issue_lock_key,
    make_pr_lock_key,
)


def test_acquire_succeeds_on_free_key(fake_redis_client):
    """Acquiring a free lock succeeds and sets is_held."""
    lock = RedisLock(fake_redis_client, "test-lock")
    assert lock.acquire() is True
    assert lock.is_held is True


def test_acquire_fails_when_held(fake_redis_client):
    """A second owner cannot acquire a lock already held."""
    lock1 = RedisLock(fake_redis_client, "test-lock", owner="owner-1")
    lock2 = RedisLock(fake_redis_client, "test-lock", owner="owner-2")

    assert lock1.acquire() is True
    assert lock2.acquire() is False


def test_release_by_owner_succeeds(fake_redis_client):
    """The owner can release the lock, removing the key."""
    lock = RedisLock(fake_redis_client, "test-lock")
    lock.acquire()

    assert lock.release() is True
    assert fake_redis_client.get("test-lock") is None


def test_release_by_non_owner_fails(fake_redis_client):
    """A non-owner cannot release the lock; the key persists."""
    lock1 = RedisLock(fake_redis_client, "test-lock", owner="owner-1")
    lock2 = RedisLock(fake_redis_client, "test-lock", owner="owner-2")

    lock1.acquire()
    assert lock2.release() is False
    # Key should still exist, owned by lock1.
    assert fake_redis_client.get("test-lock") == "owner-1"


def test_refresh_extends_ttl(fake_redis_client):
    """Refreshing by the owner succeeds."""
    lock = RedisLock(fake_redis_client, "test-lock", ttl=10)
    lock.acquire()

    assert lock.refresh() is True


def test_refresh_by_non_owner_fails(fake_redis_client):
    """A non-owner cannot refresh the lock TTL."""
    lock1 = RedisLock(fake_redis_client, "test-lock", owner="owner-1")
    lock2 = RedisLock(fake_redis_client, "test-lock", owner="owner-2")

    lock1.acquire()
    assert lock2.refresh() is False


def test_make_pr_lock_key_format():
    """make_pr_lock_key produces the expected key pattern."""
    assert make_pr_lock_key("owner/repo", 42) == "lock:pr:owner/repo:42"


def test_make_issue_lock_key_format():
    """make_issue_lock_key produces the expected key pattern."""
    assert make_issue_lock_key("owner/repo", 7) == "lock:issue:owner/repo:7"


def test_acquire_sets_ttl(fake_redis_client):
    """After acquiring, the key has a positive TTL."""
    lock = RedisLock(fake_redis_client, "test-lock", ttl=300)
    lock.acquire()

    ttl = fake_redis_client.ttl("test-lock")
    assert ttl > 0


def test_release_by_non_owner_preserves_held_state(fake_redis_client):
    """A non-owner's release() must not change the non-owner's _held state."""
    lock1 = RedisLock(fake_redis_client, "test-lock", owner="owner-1")
    lock2 = RedisLock(fake_redis_client, "test-lock", owner="owner-2")

    lock1.acquire()
    # lock2 never acquired, so is_held should stay False
    assert lock2.is_held is False
    lock2.release()
    assert lock2.is_held is False


def test_context_manager_acquires_and_releases(fake_redis_client):
    with RedisLock(fake_redis_client, "test-lock") as lock:
        assert lock.is_held is True
    assert lock.is_held is False


def test_context_manager_raises_when_lock_held(fake_redis_client):
    lock1 = RedisLock(fake_redis_client, "test-lock", owner="owner-1")
    lock1.acquire()
    with pytest.raises(RuntimeError, match="Failed to acquire lock"):
        with RedisLock(fake_redis_client, "test-lock", owner="owner-2"):
            pass


def test_context_manager_releases_on_exception(fake_redis_client):
    with pytest.raises(ValueError):
        with RedisLock(fake_redis_client, "test-lock") as lock:
            raise ValueError("boom")
    assert lock.is_held is False
    # Key should be gone — a new lock can acquire immediately
    assert RedisLock(fake_redis_client, "test-lock").acquire() is True


def test_verify_returns_true_when_lock_held(fake_redis_client):
    """verify() returns True when this client owns the lock in Redis."""
    lock = RedisLock(fake_redis_client, "test-lock")
    lock.acquire()
    assert lock.verify() is True


def test_verify_returns_false_after_release(fake_redis_client):
    """verify() returns False once the lock has been released."""
    lock = RedisLock(fake_redis_client, "test-lock")
    lock.acquire()
    lock.release()
    assert lock.verify() is False


def test_verify_returns_false_for_non_owner(fake_redis_client):
    """verify() returns False when another client holds the lock."""
    lock1 = RedisLock(fake_redis_client, "test-lock", owner="owner-1")
    lock2 = RedisLock(fake_redis_client, "test-lock", owner="owner-2")
    lock1.acquire()
    assert lock2.verify() is False


def test_verify_returns_false_when_key_expired(fake_redis_client):
    """verify() returns False when the Redis key no longer exists (simulated expiry)."""
    lock = RedisLock(fake_redis_client, "test-lock")
    lock.acquire()
    # Manually delete the key to simulate TTL expiry.
    fake_redis_client.client.delete(lock.key)
    # is_held is still True (advisory/stale), but verify() reflects reality.
    assert lock.is_held is True
    assert lock.verify() is False


def test_compute_pending_task_ttl_uses_runner_values():
    """compute_pending_task_ttl returns timeout × max_retries + 300."""
    rc = RunnerConfig(timeout=1800, max_retries=3)
    assert compute_pending_task_ttl(rc) == 1800 * 3 + 300


def test_compute_pending_task_ttl_reflects_non_default_values():
    """Non-default timeout/max_retries produce a larger TTL than the default constant."""
    rc = RunnerConfig(timeout=3600, max_retries=5)
    ttl = compute_pending_task_ttl(rc)
    assert ttl == 3600 * 5 + 300
    assert ttl > compute_pending_task_ttl(RunnerConfig())
