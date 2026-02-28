"""Unit tests for the dashboard data-fetching layer."""

from orcest.dashboard import fetch_snapshot


def test_empty_redis_returns_valid_snapshot(fake_redis_client):
    """Returns a valid snapshot when Redis has no orcest data."""
    snap = fetch_snapshot(fake_redis_client)
    assert snap.redis_ok is True
    assert snap.queue_depths == {}
    assert snap.results_depth == 0
    assert snap.locks == []
    assert snap.consumer_groups == []
    assert snap.recent_results == []
    assert snap.attempt_counts == {}


def test_queue_depths(fake_redis_client):
    """Reports correct queue depths for task streams."""
    fake_redis_client.xadd("tasks:claude", {"id": "1", "repo": "org/repo"})
    fake_redis_client.xadd("tasks:claude", {"id": "2", "repo": "org/repo"})
    fake_redis_client.xadd("tasks:noop", {"id": "3", "repo": "org/repo"})

    snap = fetch_snapshot(fake_redis_client)

    assert snap.queue_depths["tasks:claude"] == 2
    assert snap.queue_depths["tasks:noop"] == 1


def test_results_depth(fake_redis_client):
    """Reports the results stream length."""
    fake_redis_client.xadd("results", {"task_id": "t1", "status": "completed"})
    fake_redis_client.xadd("results", {"task_id": "t2", "status": "failed"})

    snap = fetch_snapshot(fake_redis_client)

    assert snap.results_depth == 2


def test_active_locks(fake_redis_client):
    """Shows active PR locks with owner and TTL."""
    fake_redis_client.client.set("lock:pr:42", "worker-1", ex=1800)

    snap = fetch_snapshot(fake_redis_client)

    assert len(snap.locks) == 1
    assert snap.locks[0].pr == "42"
    assert snap.locks[0].owner == "worker-1"
    assert snap.locks[0].ttl > 0


def test_recent_results(fake_redis_client):
    """Reads recent results in reverse chronological order."""
    for i in range(5):
        fake_redis_client.xadd("results", {
            "task_id": f"task-{i}",
            "worker_id": "w1",
            "status": "completed",
            "resource_type": "pr",
            "resource_id": str(i),
            "duration_seconds": "30",
            "summary": f"Fixed PR {i}",
        })

    snap = fetch_snapshot(fake_redis_client, max_results=3)

    assert len(snap.recent_results) == 3
    # Most recent first (resource_id 4, 3, 2)
    assert snap.recent_results[0].resource_id == "4"
    assert snap.recent_results[1].resource_id == "3"
    assert snap.recent_results[2].resource_id == "2"


def test_attempt_counts(fake_redis_client):
    """Reports PR attempt counters."""
    fake_redis_client.client.hset("pr:42:attempts", mapping={"count": "3", "head_sha": "abc"})

    snap = fetch_snapshot(fake_redis_client)

    assert snap.attempt_counts == {"PR #42": 3}


def test_disconnected_redis(fake_redis_client, mocker):
    """Returns redis_ok=False when Redis is unreachable."""
    mocker.patch.object(fake_redis_client, "health_check", return_value=False)

    snap = fetch_snapshot(fake_redis_client)

    assert snap.redis_ok is False
    assert snap.queue_depths == {}
