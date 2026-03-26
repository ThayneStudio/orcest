"""Tests for orcest.shared.coordination using fakeredis."""

import pytest

from orcest.shared.config import RunnerConfig
from orcest.shared.coordination import (
    _BACKOFF_COOLDOWNS_SECONDS,
    RedisLock,
    clear_backoff,
    compute_pending_task_ttl,
    get_backoff_cooldown_seconds,
    get_backoff_step,
    make_issue_lock_key,
    make_pr_lock_key,
    set_backoff_cooldown,
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


# ---------------------------------------------------------------------------
# Backoff functions
# ---------------------------------------------------------------------------


def test_get_backoff_cooldown_seconds_first_step():
    """Step 0 returns the first cooldown value."""
    assert get_backoff_cooldown_seconds(0) == _BACKOFF_COOLDOWNS_SECONDS[0]


def test_get_backoff_cooldown_seconds_last_step():
    """The last valid step returns the final cooldown value."""
    last = len(_BACKOFF_COOLDOWNS_SECONDS) - 1
    assert get_backoff_cooldown_seconds(last) == _BACKOFF_COOLDOWNS_SECONDS[last]


def test_get_backoff_cooldown_seconds_clamps_negative():
    """Negative step is clamped to 0."""
    assert get_backoff_cooldown_seconds(-1) == _BACKOFF_COOLDOWNS_SECONDS[0]
    assert get_backoff_cooldown_seconds(-99) == _BACKOFF_COOLDOWNS_SECONDS[0]


def test_get_backoff_cooldown_seconds_clamps_overflow():
    """Step beyond the table is clamped to the last entry (max cooldown)."""
    max_cooldown = _BACKOFF_COOLDOWNS_SECONDS[-1]
    assert get_backoff_cooldown_seconds(len(_BACKOFF_COOLDOWNS_SECONDS)) == max_cooldown
    assert get_backoff_cooldown_seconds(999) == max_cooldown


def test_get_backoff_cooldown_seconds_middle_step():
    """A middle step returns the expected Fibonacci-like value."""
    assert get_backoff_cooldown_seconds(3) == _BACKOFF_COOLDOWNS_SECONDS[3]


def test_set_backoff_cooldown_stores_step(fake_redis_client):
    """set_backoff_cooldown writes the step value to Redis."""
    set_backoff_cooldown(fake_redis_client, "owner/repo", 42, step=2)
    assert get_backoff_step(fake_redis_client, "owner/repo", 42) == 2


def test_set_backoff_cooldown_sets_ttl(fake_redis_client):
    """set_backoff_cooldown sets a positive TTL on the Redis key."""
    set_backoff_cooldown(fake_redis_client, "owner/repo", 42, step=1)
    assert fake_redis_client.ttl("backoff:pr:owner/repo:42") > 0


def test_set_backoff_cooldown_ttl_matches_step_duration(fake_redis_client):
    """The TTL is approximately equal to the cooldown for the given step."""
    repo, number, step = "owner/repo", 99, 2
    expected_cooldown = get_backoff_cooldown_seconds(step)
    set_backoff_cooldown(fake_redis_client, repo, number, step=step)
    key = f"backoff:pr:{repo}:{number}"
    ttl = fake_redis_client.ttl(key)
    # Allow a small margin for processing time.
    assert expected_cooldown - 2 <= ttl <= expected_cooldown


def test_set_backoff_cooldown_overwrites_previous_step(fake_redis_client):
    """A second call to set_backoff_cooldown overwrites the prior step."""
    set_backoff_cooldown(fake_redis_client, "owner/repo", 7, step=0)
    set_backoff_cooldown(fake_redis_client, "owner/repo", 7, step=3)
    assert get_backoff_step(fake_redis_client, "owner/repo", 7) == 3


def test_get_backoff_step_returns_none_when_not_set(fake_redis_client):
    """get_backoff_step returns None when no backoff key exists."""
    assert get_backoff_step(fake_redis_client, "owner/repo", 1) is None


def test_get_backoff_step_returns_stored_step(fake_redis_client):
    """get_backoff_step returns the integer step that was stored."""
    set_backoff_cooldown(fake_redis_client, "owner/repo", 5, step=4)
    assert get_backoff_step(fake_redis_client, "owner/repo", 5) == 4


def test_get_backoff_step_returns_none_after_key_deleted(fake_redis_client):
    """get_backoff_step returns None once the key has been deleted."""
    set_backoff_cooldown(fake_redis_client, "owner/repo", 10, step=1)
    fake_redis_client.delete("backoff:pr:owner/repo:10")
    assert get_backoff_step(fake_redis_client, "owner/repo", 10) is None


def test_get_backoff_step_returns_none_on_corrupt_value(fake_redis_client):
    """get_backoff_step returns None (treating as no backoff) when Redis has a non-integer value."""
    # Write directly using the prefixed key to bypass the int-only set_ex path.
    prefixed_key = fake_redis_client._prefixed("backoff:pr:owner/repo:77")
    fake_redis_client.client.set(prefixed_key, "not-an-int")
    result = get_backoff_step(fake_redis_client, "owner/repo", 77)
    assert result is None


def test_clear_backoff_removes_key(fake_redis_client):
    """clear_backoff deletes the backoff key so get_backoff_step returns None."""
    set_backoff_cooldown(fake_redis_client, "owner/repo", 3, step=1)
    clear_backoff(fake_redis_client, "owner/repo", 3)
    assert get_backoff_step(fake_redis_client, "owner/repo", 3) is None


def test_clear_backoff_is_idempotent(fake_redis_client):
    """Calling clear_backoff when no key exists does not raise."""
    clear_backoff(fake_redis_client, "owner/repo", 999)
    assert get_backoff_step(fake_redis_client, "owner/repo", 999) is None


def test_backoff_keys_are_isolated_per_pr(fake_redis_client):
    """Backoff state for one PR number does not affect another."""
    set_backoff_cooldown(fake_redis_client, "owner/repo", 1, step=2)
    set_backoff_cooldown(fake_redis_client, "owner/repo", 2, step=5)
    assert get_backoff_step(fake_redis_client, "owner/repo", 1) == 2
    assert get_backoff_step(fake_redis_client, "owner/repo", 2) == 5
    clear_backoff(fake_redis_client, "owner/repo", 1)
    assert get_backoff_step(fake_redis_client, "owner/repo", 1) is None
    assert get_backoff_step(fake_redis_client, "owner/repo", 2) == 5


# ---------------------------------------------------------------------------
# RedisLock raw_key tests
# ---------------------------------------------------------------------------


def test_raw_key_bypasses_prefix(fake_redis_client):
    """When raw_key=True, the lock key is used as-is without auto-prefixing."""
    fq_key = "myproject:lock:pr:owner/repo:42"
    lock = RedisLock(fake_redis_client, fq_key, raw_key=True)
    assert lock.key == fq_key
    assert lock.acquire() is True
    # The raw key should be stored in Redis directly
    assert fake_redis_client.client.get(fq_key) == lock.owner


def test_raw_key_false_uses_prefix(fake_redis_client):
    """When raw_key=False (default), the key is auto-prefixed."""
    lock = RedisLock(fake_redis_client, "lock:pr:owner/repo:42")
    # fake_redis_client uses prefix "test:" (from conftest)
    assert lock.key == "test:lock:pr:owner/repo:42"


def test_raw_key_lock_contention(fake_redis_client):
    """Two locks on the same raw key contend correctly."""
    fq_key = "myproject:lock:pr:owner/repo:42"
    lock1 = RedisLock(fake_redis_client, fq_key, owner="worker-1", raw_key=True)
    lock2 = RedisLock(fake_redis_client, fq_key, owner="worker-2", raw_key=True)
    assert lock1.acquire() is True
    assert lock2.acquire() is False


def test_raw_key_release_and_refresh(fake_redis_client):
    """Release and refresh work correctly with raw_key=True."""
    fq_key = "myproject:lock:pr:owner/repo:42"
    lock = RedisLock(fake_redis_client, fq_key, ttl=300, raw_key=True)
    lock.acquire()
    assert lock.refresh() is True
    assert lock.release() is True
    assert fake_redis_client.client.get(fq_key) is None


def test_raw_key_does_not_collide_with_prefixed(fake_redis_client):
    """A raw-key lock and a prefixed lock with the same logical key
    should write to different Redis keys and not interfere."""
    logical_key = "lock:pr:owner/repo:42"
    # Prefixed lock writes to "test:lock:pr:owner/repo:42"
    prefixed_lock = RedisLock(fake_redis_client, logical_key, owner="worker-A")
    # Raw lock writes to "lock:pr:owner/repo:42" (no prefix)
    raw_lock = RedisLock(fake_redis_client, logical_key, owner="worker-B", raw_key=True)

    assert prefixed_lock.acquire() is True
    assert raw_lock.acquire() is True  # Different key, should succeed
    assert prefixed_lock.key != raw_lock.key
