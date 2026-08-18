"""Task B11: the pool reaper's ``_health_check`` becomes activity-aware.

Below the ``max_task_duration`` ceiling, a fresh ``workers:activity:{worker_id}``
record (written by the worker-side liveness tracker, see
worker/liveness_tracker.py's ``_write_activity_record``) now blocks
destruction outright. Destruction below the ceiling only fires when:

- the record's ``needs_reap`` field is ``"1"`` (the watchdog already latched
  a kill decision), or
- the record is absent-or-stale (worker died, or activity tracking never
  started) AND the consumer still holds pending stream entries -- proving
  there is a task to recover rather than an idle/just-claimed worker.

The ceiling itself is unconditional: ``elapsed > max_task_duration`` always
destroys, regardless of how fresh the activity record is.

Arrange blocks mirror TestHealthCheckReapCoordination in
tests/fleet/test_pool_manager.py and the reason-field checks in
tests/fleet/test_pool_manager_events.py.
"""

from __future__ import annotations

import json
import time

import pytest

from orcest.fleet.pool_manager import PoolManager
from orcest.shared.events import EVENTS_STREAM
from orcest.shared.models import CONSUMER_GROUP, Task, TaskType

from .test_pool_manager import _make_config, _make_proxmox

pytestmark = pytest.mark.unit

_WORKER_ID = "orcest-worker-305"
_ACTIVITY_KEY = f"workers:activity:{_WORKER_ID}"


def _build(fake_redis_client, max_task_duration: int = 25200):
    config = _make_config(max_task_duration=max_task_duration, vm_id_start=300)
    config.pool.vm_id_end = 399
    proxmox = _make_proxmox()
    manager = PoolManager(
        config=config,
        proxmox=proxmox,
        redis=fake_redis_client,
        key_prefix="test",
    )
    return manager, proxmox


def _write_activity_record(rc, *, needs_reap: bool, last_liveness_ts: float) -> None:
    """Write a ``workers:activity:{worker_id}`` hash the way LivenessTracker does:
    unprefixed (global, cross-project) key, via the ``*_raw`` helpers."""
    rc.hset_raw(_ACTIVITY_KEY, "task_id", "task-1")
    rc.hset_raw(_ACTIVITY_KEY, "state", "active")
    rc.hset_raw(_ACTIVITY_KEY, "last_liveness_ts", str(last_liveness_ts))
    rc.hset_raw(_ACTIVITY_KEY, "ladder_since", str(last_liveness_ts))
    rc.hset_raw(_ACTIVITY_KEY, "needs_reap", "1" if needs_reap else "0")
    rc.hset_raw(_ACTIVITY_KEY, "snapshot", json.dumps({}))


def _claim_pending_task(rc, worker_id: str) -> Task:
    """Give *worker_id* a pending stream entry (a claimed-but-unacked task)."""
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


class TestActivityAwareHealthCheck:
    def test_fresh_record_blocks_destroy_below_ceiling(self, fake_redis_client):
        """A fresh, non-needs_reap activity record blocks destruction even
        though the worker has been active for a while -- below the ceiling
        only the watchdog decides, not elapsed time alone."""
        rc = fake_redis_client
        manager, proxmox = _build(rc, max_task_duration=25200)
        now = time.time()
        rc.hset("pool:active", "305", str(now - 10000))  # elapsed 10000s < 25200 ceiling
        _write_activity_record(rc, needs_reap=False, last_liveness_ts=now)

        manager._health_check()

        proxmox.stop_vm.assert_not_called()
        proxmox.destroy_vm.assert_not_called()

    def test_ceiling_destroys_despite_fresh_activity(self, fake_redis_client):
        """The ceiling is unconditional: it destroys even a worker with a
        rock-solid fresh activity record."""
        rc = fake_redis_client
        manager, proxmox = _build(rc, max_task_duration=3600)
        now = time.time()
        rc.hset("pool:active", "305", str(now - 99999))  # elapsed 99999s > 3600 ceiling
        _write_activity_record(rc, needs_reap=False, last_liveness_ts=now)

        manager._health_check()

        proxmox.stop_vm.assert_called_once_with(305)
        proxmox.destroy_vm.assert_called_once_with(305)

    def test_needs_reap_flag_destroys_immediately(self, fake_redis_client):
        """needs_reap == "1" fires immediately, well below the ceiling."""
        rc = fake_redis_client
        manager, proxmox = _build(rc, max_task_duration=25200)
        now = time.time()
        rc.hset("pool:active", "305", str(now - 10))  # elapsed 10s, nowhere near ceiling
        _write_activity_record(rc, needs_reap=True, last_liveness_ts=now)

        manager._health_check()

        proxmox.stop_vm.assert_called_once_with(305)
        proxmox.destroy_vm.assert_called_once_with(305)

    def test_absent_record_with_pending_entries_destroys(self, fake_redis_client):
        """No activity record at all (worker died before ever reporting, or
        its TTL already expired) combined with a pending stream entry proves
        there is a task to recover -- destroy."""
        rc = fake_redis_client
        manager, proxmox = _build(rc, max_task_duration=25200)
        now = time.time()
        rc.hset("pool:active", "305", str(now - 10))
        _claim_pending_task(rc, _WORKER_ID)
        # No activity record written for _WORKER_ID.

        manager._health_check()

        proxmox.stop_vm.assert_called_once_with(305)
        proxmox.destroy_vm.assert_called_once_with(305)

    def test_absent_record_without_pending_entries_not_destroyed(self, fake_redis_client):
        """No activity record and no pending stream entries: could just be an
        idle worker whose watchdog hasn't ticked yet. Leave it alone."""
        rc = fake_redis_client
        manager, proxmox = _build(rc, max_task_duration=25200)
        now = time.time()
        rc.hset("pool:active", "305", str(now - 10))
        # No activity record, no claimed task.

        manager._health_check()

        proxmox.stop_vm.assert_not_called()
        proxmox.destroy_vm.assert_not_called()

    def test_reaped_event_reason_field(self, fake_redis_client):
        """Each destroy path stamps its own honest reason on the emitted
        net.orcest.task.reaped event's data.reason. All three managers below
        share one fake Redis keyspace but operate on distinct VMIDs/worker
        IDs, so their reaped events can be told apart by reason alone."""
        rc = fake_redis_client

        # A net.orcest.task.reaped event only fires when the reaped consumer
        # actually owns a pending stream entry to recover -- give each
        # worker one (mirrors test_reaped_event_emitted in
        # test_pool_manager_events.py).

        # ceiling: elapsed > max_task_duration, regardless of activity state.
        ceiling_mgr, _p1 = _build(rc, max_task_duration=3600)
        now = time.time()
        rc.hset("pool:active", "305", str(now - 99999))
        _claim_pending_task(rc, "orcest-worker-305")
        ceiling_mgr._health_check()

        # needs_reap: watchdog already latched a kill, well below the ceiling.
        needs_reap_mgr, _p2 = _build(rc, max_task_duration=25200)
        rc.hset("pool:active", "306", str(now - 10))
        rc.hset_raw("workers:activity:orcest-worker-306", "needs_reap", "1")
        rc.hset_raw("workers:activity:orcest-worker-306", "last_liveness_ts", str(now))
        _claim_pending_task(rc, "orcest-worker-306")
        needs_reap_mgr._health_check()

        # activity_stale: no activity record at all, but a pending stream entry.
        stale_mgr, _p3 = _build(rc, max_task_duration=25200)
        rc.hset("pool:active", "307", str(now - 10))
        _claim_pending_task(rc, "orcest-worker-307")
        stale_mgr._health_check()

        reaped = _reaped_events(rc)
        reasons = {e["data"]["reason"]: e for e in reaped}
        assert reasons.keys() == {"ceiling", "needs_reap", "activity_stale"}
        for event in reasons.values():
            assert isinstance(event["data"]["elapsed_seconds"], float)
