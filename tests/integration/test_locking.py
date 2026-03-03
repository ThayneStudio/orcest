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
        key = make_pr_lock_key("owner/repo", 1)
        lock = RedisLock(real_redis_client, key, ttl=30, owner="w1")

        assert lock.acquire() is True
        assert real_redis_client.client.exists(key) == 1

        assert lock.release() is True
        assert real_redis_client.client.exists(key) == 0

    def test_contention_two_workers(self, real_redis_client: RedisClient) -> None:
        """Second worker cannot acquire until the first releases."""
        key = make_pr_lock_key("owner/repo", 1)
        lock1 = RedisLock(real_redis_client, key, ttl=30, owner="w1")
        lock2 = RedisLock(real_redis_client, key, ttl=30, owner="w2")

        assert lock1.acquire() is True
        assert lock2.acquire() is False

        assert lock1.release() is True
        assert lock2.acquire() is True

    def test_lua_release_atomicity(self, real_redis_client: RedisClient) -> None:
        """A non-owner cannot release the lock; key retains owner."""
        key = make_pr_lock_key("owner/repo", 1)
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
        key = make_pr_lock_key("owner/repo", 1)
        lock1 = RedisLock(real_redis_client, key, ttl=30, owner="w1")
        lock2 = RedisLock(real_redis_client, key, ttl=30, owner="w2")

        assert lock1.acquire() is True
        assert lock2.refresh() is False
        assert lock1.refresh() is True

    def test_ttl_expiry(self, real_redis_client: RedisClient) -> None:
        """Lock disappears after TTL expires."""
        key = make_pr_lock_key("owner/repo", 1)
        lock = RedisLock(real_redis_client, key, ttl=1, owner="w1")
        assert lock.acquire() is True

        time.sleep(3)

        assert real_redis_client.client.exists(key) == 0

        # A fresh lock can now acquire the same key
        lock2 = RedisLock(real_redis_client, key, ttl=30, owner="w2")
        assert lock2.acquire() is True

    def test_heartbeat_prevents_expiry(self, real_redis_client: RedisClient) -> None:
        """Heartbeat keeps the lock alive past its natural TTL."""
        key = make_pr_lock_key("owner/repo", 1)
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
        key = make_pr_lock_key("owner/repo", 42)
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

    def test_no_overlapping_work_on_same_pr(self, real_redis_client: RedisClient) -> None:
        """Workers holding locks for the same PR never overlap in time."""
        key = make_pr_lock_key("owner/repo", 1)
        windows: list[tuple[str, float, float]] = []
        windows_lock = threading.Lock()
        barrier = threading.Barrier(5)

        def _worker(owner: str) -> None:
            lock = RedisLock(real_redis_client, key, ttl=5, owner=owner)
            barrier.wait()
            for _ in range(2):  # Each worker attempts 2 acquisitions
                if lock.acquire():
                    hb = Heartbeat(lock, interval=0.5)
                    hb.start()
                    start = time.monotonic()
                    time.sleep(0.1)  # Simulate work
                    end = time.monotonic()
                    hb.stop()
                    lock.release()
                    with windows_lock:
                        windows.append((owner, start, end))

        threads = [threading.Thread(target=_worker, args=(f"w-{i}",)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Verify no two windows overlap
        sorted_windows = sorted(windows, key=lambda w: w[1])
        for i in range(len(sorted_windows) - 1):
            _, _, end_i = sorted_windows[i]
            _, start_next, _ = sorted_windows[i + 1]
            assert end_i <= start_next, (
                f"Overlap detected: window {i} ended at {end_i}, "
                f"window {i + 1} started at {start_next}"
            )

    def test_lock_expires_after_worker_death(self, real_redis_client: RedisClient) -> None:
        """A crashed worker's lock expires, allowing another worker to proceed."""
        key = make_pr_lock_key("owner/repo", 1)
        lock1 = RedisLock(real_redis_client, key, ttl=2, owner="w1")
        assert lock1.acquire() is True

        # Start heartbeat, then simulate crash (stop heartbeat but don't release)
        hb = Heartbeat(lock1, interval=0.5)
        hb.start()
        hb.stop()  # Crash: heartbeat dies but lock not released

        # Lock should still be held immediately
        assert real_redis_client.client.exists(key) == 1

        # Wait for TTL to expire
        time.sleep(3)
        assert real_redis_client.client.exists(key) == 0

        # Another worker can now acquire
        lock2 = RedisLock(real_redis_client, key, ttl=30, owner="w2")
        assert lock2.acquire() is True
        lock2.release()
