"""Task A4: pool-manager emits net.orcest.task.reaped when it force-destroys a
worker VM.

Fix round 1: the event's data.reason must be honest per call site, not
hardcoded to a single value for every _coordinate_reaped_vm caller.
Covers the two extremes: the real ceiling-timeout path (_health_check,
reason "ceiling" -- see test_pool_manager_activity.py for the other two
_health_check reasons, "needs_reap" and "activity_stale", added by B11's
activity-aware reaper) and a non-timeout path (_check_done_workers, reason
"done_cleanup", elapsed_seconds omitted).

Arrange blocks mirror
TestHealthCheckReapCoordination.test_reaped_vm_publishes_transient_failure_and_clears_marker
in tests/fleet/test_pool_manager.py.
"""

from __future__ import annotations

import json
import time

from orcest.fleet.pool_manager import PoolManager
from orcest.shared.events import EVENTS_STREAM
from orcest.shared.models import CONSUMER_GROUP, Task, TaskType

from .test_pool_manager import _make_config, _make_proxmox


def _build(fake_redis_client):
    config = _make_config(max_task_duration=3600, vm_id_start=300)
    config.pool.vm_id_end = 399
    proxmox = _make_proxmox()
    manager = PoolManager(
        config=config,
        proxmox=proxmox,
        redis=fake_redis_client,
        key_prefix="test",
    )
    return manager, proxmox


def _claim_task(rc, worker_id: str) -> Task:
    task = Task.create(
        task_type=TaskType.FIX_CI,
        repo="owner/repo",
        token="ghp_x",
        resource_type="pr",
        resource_id=42,
        prompt="fix",
        branch="fix-branch",
        key_prefix="test",
    )
    rc.ensure_consumer_group("tasks:claude", CONSUMER_GROUP)
    rc.xadd("tasks:claude", task.to_dict())
    claimed = rc.xreadgroup(
        group=CONSUMER_GROUP, consumer=worker_id, stream="tasks:claude", block_ms=None
    )
    assert len(claimed) == 1
    return task


def _reaped_events(rc) -> list[dict]:
    entries = rc.xrevrange(EVENTS_STREAM, count=10)
    envs = [json.loads(f["envelope"]) for _id, f in entries]
    return [e for e in envs if e["type"] == "net.orcest.task.reaped"]


def test_reaped_event_emitted(fake_redis_client):
    """Health-check ceiling reap: reason is honestly "ceiling" with elapsed_seconds."""
    rc = fake_redis_client  # prefix 'test'
    manager, _proxmox = _build(rc)
    worker_id = "orcest-worker-305"
    task = _claim_task(rc, worker_id)
    # Mark VM 305 active and over-duration.
    rc.hset("pool:active", "305", str(time.time() - 99999))

    manager._health_check()

    reaped = _reaped_events(rc)
    assert len(reaped) == 1
    assert reaped[0]["data"]["reason"] == "ceiling"
    assert reaped[0]["data"]["worker_id"] == worker_id
    assert reaped[0]["subject"] == task.id
    assert isinstance(reaped[0]["data"]["elapsed_seconds"], float)
    assert reaped[0]["data"]["elapsed_seconds"] > 0


def test_reaped_event_done_cleanup_reports_honest_reason_without_elapsed(fake_redis_client):
    """Done-worker cleanup is not a timeout: reason must say "done_cleanup" and
    elapsed_seconds (genuinely unknown here) must be omitted, not reported as 0.0."""
    rc = fake_redis_client  # prefix 'test'
    manager, _proxmox = _build(rc)
    worker_id = "orcest-worker-305"
    task = _claim_task(rc, worker_id)
    # Worker reported done; nothing tracks this task's elapsed runtime.
    rc.set_value(f"pool:done:{worker_id}", "1")

    destroyed = manager._check_done_workers()

    assert destroyed == [305]
    reaped = _reaped_events(rc)
    assert len(reaped) == 1
    assert reaped[0]["data"]["reason"] == "done_cleanup"
    assert reaped[0]["data"]["worker_id"] == worker_id
    assert reaped[0]["subject"] == task.id
    assert "elapsed_seconds" not in reaped[0]["data"]
