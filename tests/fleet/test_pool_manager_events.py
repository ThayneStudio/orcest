"""Task A4: pool-manager emits net.orcest.task.reaped when it force-destroys a
worker VM that exceeded max_task_duration.

Arrange block mirrors
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


def test_reaped_event_emitted(fake_redis_client):
    rc = fake_redis_client  # prefix 'test'
    manager, _proxmox = _build(rc)
    worker_id = "orcest-worker-305"
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
    # Mark VM 305 active and over-duration.
    rc.hset("pool:active", "305", str(time.time() - 99999))

    manager._health_check()

    entries = rc.xrevrange(EVENTS_STREAM, count=10)
    envs = [json.loads(f["envelope"]) for _id, f in entries]
    reaped = [e for e in envs if e["type"] == "net.orcest.task.reaped"]
    assert len(reaped) == 1
    assert reaped[0]["data"]["reason"] == "max_task_duration"
    assert reaped[0]["data"]["worker_id"] == worker_id
    assert reaped[0]["subject"] == task.id
    assert isinstance(reaped[0]["data"]["elapsed_seconds"], float)
    assert reaped[0]["data"]["elapsed_seconds"] > 0
