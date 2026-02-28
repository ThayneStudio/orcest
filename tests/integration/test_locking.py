"""Distributed locking tests with real Redis."""

from __future__ import annotations

import threading
import time

import pytest

from orcest.shared.coordination import RedisLock, make_pr_lock_key
from orcest.shared.redis_client import RedisClient
from orcest.worker.heartbeat import Heartbeat


@pytest.mark.integration
class TestLocking:
    """Verify RedisLock behaviour against a live Redis instance."""

    def test_acquire_release_real_redis(self, real_redis_client: RedisClient) -> None:
        """Basic acquire / release cycle; key removed after release."""
        key = make_pr_lock_key(1)
        lock = RedisLock(real_redis_client, key, ttl=30, owner="w1")

        assert lock.acquire() is True
        assert real_redis_client.client.exists(key) == 1

        assert lock.release() is True
        assert real_redis_client.client.exists(key) == 0

    def test_contention_two_workers(self, real_redis_client: RedisClient) -> None:
        """Second worker cannot acquire until the first releases."""
        key = make_pr_lock_key(1)
        lock1 = RedisLock(real_redis_client, key, ttl=30, owner="w1")
        lock2 = RedisLock(real_redis_client, key, ttl=30, owner="w2")

        assert lock1.acquire() is True
        assert lock2.acquire() is False

        assert lock1.release() is True
        assert lock2.acquire() is True

    def test_lua_release_atomicity(self, real_redis_client: RedisClient) -> None:
        """A non-owner cannot release the lock; key retains owner."""
        key = make_pr_lock_key(1)
        lock1 = RedisLock(real_redis_client, key, ttl=30, owner="w1")
        lock2 = RedisLock(real_redis_client, key, ttl=30, owner="w2")

        assert lock1.acquire() is True
        # Wrong owner tries to release
        assert lock2.release() is False

        # Key still held by original owner
        assert real_redis_client.client.exists(key) == 1
        assert real_redis_client.client.get(key) == "w1"

    def test_lua_refresh_atomicity(self, real_redis_client: RedisClient) -> None:
        """Only the owner can refresh the TTL."""
        key = make_pr_lock_key(1)
        lock1 = RedisLock(real_redis_client, key, ttl=30, owner="w1")
        lock2 = RedisLock(real_redis_client, key, ttl=30, owner="w2")

        assert lock1.acquire() is True
        assert lock2.refresh() is False
        assert lock1.refresh() is True

    def test_ttl_expiry(self, real_redis_client: RedisClient) -> None:
        """Lock disappears after TTL expires."""
        key = make_pr_lock_key(1)
        lock = RedisLock(real_redis_client, key, ttl=1, owner="w1")
        assert lock.acquire() is True

        time.sleep(3)

        assert real_redis_client.client.exists(key) == 0

        # A fresh lock can now acquire the same key
        lock2 = RedisLock(real_redis_client, key, ttl=30, owner="w2")
        assert lock2.acquire() is True

    def test_heartbeat_prevents_expiry(self, real_redis_client: RedisClient) -> None:
        """Heartbeat keeps the lock alive past its natural TTL."""
        key = make_pr_lock_key(1)
        lock = RedisLock(real_redis_client, key, ttl=2, owner="w1")
        assert lock.acquire() is True

        hb = Heartbeat(lock, interval=0.5)
        hb.start()

        # Sleep well past the 2-second TTL
        time.sleep(4)
        assert real_redis_client.client.exists(key) == 1

        hb.stop()

        # Without heartbeat the TTL (refreshed to 2s) will expire
        time.sleep(3)
        assert real_redis_client.client.exists(key) == 0

    def test_thread_contention(self, real_redis_client: RedisClient) -> None:
        """Exactly one of N concurrent threads acquires the lock."""
        key = make_pr_lock_key(42)
        n_threads = 10
        barrier = threading.Barrier(n_threads)
        results_lock = threading.Lock()
        winners: list[str] = []

        def _try_acquire(owner: str) -> None:
            lock = RedisLock(real_redis_client, key, ttl=30, owner=owner)
            barrier.wait()  # synchronise start
            if lock.acquire():
                with results_lock:
                    winners.append(owner)

        threads = [
            threading.Thread(target=_try_acquire, args=(f"t-{i}",)) for i in range(n_threads)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(winners) == 1

        # Release and verify another thread can acquire afterwards
        winner_lock = RedisLock(real_redis_client, key, ttl=30, owner=winners[0])
        assert winner_lock.release() is True

        new_lock = RedisLock(real_redis_client, key, ttl=30, owner="late")
        assert new_lock.acquire() is True
        new_lock.release()
