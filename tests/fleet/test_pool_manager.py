"""Tests for orcest.fleet.pool_manager."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from orcest.fleet.config import FleetConfig, PoolConfig, ProxmoxConfig, WorkerProfileConfig
from orcest.fleet.pool_manager import (
    REAP_REASON_DONE_CLEANUP,
    REAP_REASON_ORPHAN_PEL,
    PoolManager,
    ReapFence,
)

pytestmark = pytest.mark.unit


# ── Fixtures ─────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _no_drain_wait(monkeypatch):
    monkeypatch.setattr("orcest.fleet.pool_manager._DRAIN_QUIESCE_SECONDS", 0)
    monkeypatch.setattr("orcest.fleet.pool_manager._POST_STOP_PENDING_CHECK_RETRY_SECONDS", 0)


def _make_config(
    pool_size: int = 4,
    template_vm_id: int = 9000,
    vm_id_start: int = 300,
    storage: str = "ssd-pool",
    max_task_duration: int = 3600,
) -> FleetConfig:
    """Build a FleetConfig with pool settings for testing."""
    return FleetConfig(
        proxmox=ProxmoxConfig(node="pve", storage="local-lvm"),
        pool=PoolConfig(
            size=pool_size,
            template_vm_id=template_vm_id,
            vm_id_start=vm_id_start,
            storage=storage,
            max_task_duration=max_task_duration,
        ),
    )


def _make_redis(idle_set: set[str] | None = None) -> MagicMock:
    """Build a mock RedisClient with the needed interface.

    The mock tracks sadd calls so that smembers returns accumulated
    state — needed because _next_vm_id reads smembers to avoid ID collisions.

    Pass *idle_set* to pre-populate the idle pool (e.g. ``{"301"}``).
    The internal set is exposed as ``mock._idle_set`` for tests that need
    to pre-populate it after construction.
    """
    mock = MagicMock()
    mock._idle_set: set[str] = set(idle_set or set())
    mock._sets: dict[str, set[str]] = {
        "pool:provisioning": set(),
        "pool:ambiguous-clones": set(),
    }

    def _sadd(key: str, *values: str) -> int:
        if key == "pool:idle":
            before = len(mock._idle_set)
            mock._idle_set.update(values)
            return len(mock._idle_set) - before
        target = mock._sets.setdefault(key, set())
        before = len(target)
        target.update(values)
        return len(target) - before

    def _srem(key: str, *values: str) -> int:
        target = mock._idle_set if key == "pool:idle" else mock._sets.setdefault(key, set())
        before = len(target)
        target.difference_update(values)
        return before - len(target)

    def _sismember(key: str, value: str) -> bool:
        target = mock._idle_set if key == "pool:idle" else mock._sets.setdefault(key, set())
        return value in target

    def _smembers(key: str) -> set[str]:
        if key == "pool:idle":
            return set(mock._idle_set)
        return set(mock._sets.setdefault(key, set()))

    def _client_type(_key: str) -> str:
        return "stream"

    def _delete(key: str) -> int:
        if key in mock._sets:
            existed = bool(mock._sets[key])
            mock._sets[key].clear()
            return int(existed)
        return 1

    mock.scan_iter.return_value = []
    mock.hgetall.return_value = {}
    # Activity-watchdog record: default to absent (no fresh record) so tests
    # that don't care about the activity-aware reaper get the pre-B11
    # ceiling-only behavior (absent + no pending consumers -> not destroyed).
    mock.hgetall_raw.return_value = {}
    mock.smembers.side_effect = _smembers
    mock.scard.return_value = 0
    mock.hlen.return_value = 0
    mock.exists.return_value = False
    mock.xinfo_groups_raw.return_value = []
    mock.xinfo_consumers_raw.return_value = []
    mock.sadd.side_effect = _sadd
    mock.srem.side_effect = _srem
    mock.sismember.side_effect = _sismember
    mock.delete.side_effect = _delete
    mock.client.type.side_effect = _client_type
    pipe = MagicMock()
    processed_calls = 0

    def _execute_pipeline() -> list[int]:
        nonlocal processed_calls
        calls = pipe.method_calls[processed_calls:]
        processed_calls = len(pipe.method_calls)
        for call in calls:
            if call.args and call[0] == "sadd":
                _sadd(*call.args)
            elif call.args and call[0] == "srem":
                _srem(*call.args)
            elif call.args and call[0] == "delete":
                _delete(str(call.args[0]))
        return [1] * len(calls)

    pipe.execute.side_effect = _execute_pipeline
    mock.pipeline.return_value = pipe
    # Template pointer defaults to unset; PoolManager._resolve_template_vmid
    # then falls back to pool.template_vm_id from config (the legacy path).
    mock.get.return_value = None
    return mock


def _make_proxmox() -> MagicMock:
    """Build a mock ProxmoxClient."""
    mock = MagicMock()
    mock.get_vm_ip.return_value = "10.20.0.50"
    mock.get_vm_status.return_value = "stopped"
    mock.list_vms.return_value = [
        {"vmid": 9000, "name": "orcest-worker-template", "template": True},
    ]
    # Default: the configured template exists (validated before cloning).
    mock.vm_exists.return_value = True
    return mock


def _make_manager(
    config: FleetConfig | None = None,
    proxmox: MagicMock | None = None,
    redis: MagicMock | None = None,
) -> tuple[PoolManager, MagicMock, MagicMock]:
    """Build a PoolManager with mocked dependencies."""
    config = config or _make_config()
    proxmox = proxmox or _make_proxmox()
    redis = redis or _make_redis()
    manager = PoolManager(config=config, proxmox=proxmox, redis=redis)
    return manager, proxmox, redis


# ── Worker ID / VM ID conversion ────────────────────────────


class TestWorkerIdConversion:
    def test_worker_id_to_vm_id(self):
        assert PoolManager._worker_id_to_vm_id("orcest-worker-300") == 300

    def test_worker_id_to_vm_id_large(self):
        assert PoolManager._worker_id_to_vm_id("orcest-worker-9999") == 9999

    def test_worker_id_to_vm_id_bare_integer(self):
        assert PoolManager._worker_id_to_vm_id("300") == 300

    def test_worker_id_to_vm_id_invalid(self):
        assert PoolManager._worker_id_to_vm_id("not-a-worker") is None

    def test_worker_id_to_vm_id_empty(self):
        assert PoolManager._worker_id_to_vm_id("") is None

    def test_worker_id_to_vm_id_prefix_only(self):
        assert PoolManager._worker_id_to_vm_id("orcest-worker-") is None

    def test_worker_id_to_vm_id_wrong_prefix(self):
        assert PoolManager._worker_id_to_vm_id("some-worker-300") is None

    def test_worker_id_to_vm_id_non_numeric_suffix(self):
        assert PoolManager._worker_id_to_vm_id("orcest-worker-abc") is None

    def test_vm_id_to_worker_id(self):
        assert PoolManager._vm_id_to_worker_id(300) == "orcest-worker-300"

    def test_vm_id_to_worker_id_zero(self):
        assert PoolManager._vm_id_to_worker_id(0) == "orcest-worker-0"

    def test_vm_id_to_worker_id_roundtrip(self):
        for vm_id in (100, 300, 9999):
            worker_id = PoolManager._vm_id_to_worker_id(vm_id)
            assert PoolManager._worker_id_to_vm_id(worker_id) == vm_id


class TestTaskStreams:
    def test_discovers_backend_task_streams_from_redis(self):
        manager, _proxmox, redis = _make_manager()
        redis.scan_iter.return_value = [
            "tasks:grok",
            "tasks:issue:codex",
            "tasks:issue",
            "tasks:issue:grok:extra",
            "pool:done:orcest-worker-300",
        ]

        assert manager._task_streams() == (
            "orcest:tasks:claude",
            "orcest:tasks:grok",
            "orcest:tasks:issue:claude",
            "orcest:tasks:issue:codex",
        )

    def test_task_streams_keep_claude_default_when_discovery_fails(self):
        manager, _proxmox, redis = _make_manager()
        redis.scan_iter.side_effect = ConnectionError("Redis down")

        assert manager._task_streams() == (
            "orcest:tasks:claude",
            "orcest:tasks:issue:claude",
        )

    def test_task_streams_seed_configured_backend_when_discovery_fails(self):
        config = _make_config()
        config.pool.worker_backend = "clauder"
        manager, _proxmox, redis = _make_manager(config=config)
        redis.scan_iter.side_effect = ConnectionError("Redis down")

        assert manager._task_streams() == (
            "orcest:tasks:claude",
            "orcest:tasks:clauder",
            "orcest:tasks:issue:claude",
            "orcest:tasks:issue:clauder",
        )

    def test_dynamic_non_stream_task_key_is_ignored(self):
        manager, _proxmox, redis = _make_manager()
        redis.scan_iter.return_value = ["tasks:metadata", "tasks:grok"]

        def redis_type(key: str) -> str:
            return "hash" if key == "orcest:tasks:metadata" else "stream"

        redis.client.type.side_effect = redis_type

        streams, complete = manager._task_streams_with_discovery_status()

        assert complete is True
        assert "orcest:tasks:metadata" not in streams
        assert "orcest:tasks:grok" in streams

    def test_dynamic_type_failure_marks_discovery_incomplete(self):
        manager, _proxmox, redis = _make_manager()
        redis.scan_iter.return_value = ["tasks:metadata"]
        redis.client.type.side_effect = RuntimeError("Redis unavailable")

        _streams, complete = manager._task_streams_with_discovery_status()

        assert complete is False


# ── _check_done_workers ─────────────────────────────────────


class TestCheckDoneWorkers:
    def test_no_done_workers(self):
        manager, proxmox, redis = _make_manager()
        redis.scan_iter.return_value = []

        destroyed = manager._check_done_workers()

        assert destroyed == []
        redis.scan_iter.assert_called_once_with(match="pool:done:*")
        proxmox.stop_vm.assert_not_called()
        proxmox.destroy_vm.assert_not_called()

    def test_destroys_done_worker(self):
        manager, proxmox, redis = _make_manager()
        redis.scan_iter.return_value = ["pool:done:orcest-worker-300"]

        destroyed = manager._check_done_workers()

        assert destroyed == [300]
        proxmox.stop_vm.assert_called_once_with(300)
        proxmox.destroy_vm.assert_called_once_with(300)
        redis.delete.assert_any_call("pool:done:orcest-worker-300")

    def test_done_worker_coordinates_recovery_before_destroy(self):
        manager, proxmox, redis = _make_manager()
        redis.scan_iter.return_value = ["pool:done:orcest-worker-300"]

        with patch.object(manager, "_coordinate_reaped_vm", return_value=True) as coordinate:
            destroyed = manager._check_done_workers()

        assert destroyed == [300]
        coordinate.assert_called_once_with(300, reason=REAP_REASON_DONE_CLEANUP)
        proxmox.destroy_vm.assert_called_once_with(300)
        redis.delete.assert_any_call("pool:done:orcest-worker-300")

    def test_done_worker_recovery_failure_stops_vm_and_leaves_done_key(self):
        manager, proxmox, redis = _make_manager()
        redis.scan_iter.return_value = ["pool:done:orcest-worker-300"]

        with patch.object(manager, "_coordinate_reaped_vm", return_value=False) as coordinate:
            destroyed = manager._check_done_workers()

        assert destroyed == []
        coordinate.assert_called_once_with(300, reason=REAP_REASON_DONE_CLEANUP)
        proxmox.stop_vm.assert_called_once_with(300)
        proxmox.destroy_vm.assert_not_called()
        redis.delete.assert_not_called()
        # Completion is a durable handoff; only successful VM destruction may
        # delete it, and a failed pass must not add an expiry.
        redis.expire.assert_not_called()

    def test_destroys_multiple_done_workers(self):
        manager, proxmox, redis = _make_manager()
        redis.scan_iter.return_value = [
            "pool:done:orcest-worker-300",
            "pool:done:orcest-worker-301",
        ]

        destroyed = manager._check_done_workers()

        assert sorted(destroyed) == [300, 301]
        assert proxmox.stop_vm.call_count == 2
        assert proxmox.destroy_vm.call_count == 2

    def test_handles_unparseable_worker_id(self):
        manager, proxmox, redis = _make_manager()
        redis.scan_iter.return_value = ["pool:done:bad-id"]

        destroyed = manager._check_done_workers()

        assert destroyed == []
        proxmox.stop_vm.assert_not_called()
        # Still cleans up the key
        redis.delete.assert_called_once_with("pool:done:bad-id")

    def test_handles_stop_vm_failure(self):
        manager, proxmox, redis = _make_manager()
        redis.scan_iter.return_value = ["pool:done:orcest-worker-300"]
        proxmox.stop_vm.side_effect = Exception("VM already stopped")

        destroyed = manager._check_done_workers()

        # Still proceeds to destroy
        assert destroyed == [300]
        proxmox.destroy_vm.assert_called_once_with(300)

    def test_removes_from_idle_and_active_sets(self):
        """Verify _destroy_vm (called internally) cleans both tracking sets."""
        manager, proxmox, redis = _make_manager()
        redis.scan_iter.return_value = ["pool:done:orcest-worker-300"]
        pipe = MagicMock()
        redis.pipeline.return_value = pipe

        manager._check_done_workers()

        pipe.srem.assert_any_call("pool:idle", "300")
        pipe.srem.assert_any_call("pool:draining", "orcest-worker-300")
        pipe.hdel.assert_called_once_with("pool:active", "300")
        pipe.delete.assert_any_call("pool:done:orcest-worker-300")
        pipe.delete.assert_any_call("workers:heartbeat:orcest-worker-300")
        pipe.execute.assert_called_once()

    def test_destroy_failure_does_not_block_remaining_done_workers(self):
        """If destroying one done worker fails, the rest are still processed."""
        manager, proxmox, redis = _make_manager()
        redis.scan_iter.return_value = [
            "pool:done:orcest-worker-300",
            "pool:done:orcest-worker-301",
        ]
        pipe = MagicMock()
        call_count = 0

        def pipeline_execute_side_effect():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ConnectionError("Redis connection lost")
            return []

        pipe.execute.side_effect = pipeline_execute_side_effect
        redis.pipeline.return_value = pipe

        destroyed = manager._check_done_workers()

        # Both Proxmox operations are attempted, but the first VM is not
        # reported as a completed lifecycle transition because its durable
        # generation-marker cleanup could not be verified.
        assert destroyed == [301]
        assert proxmox.destroy_vm.call_count == 2
        redis.delete.assert_called_once_with("pool:done:orcest-worker-301")


# ── _destroy_vm ──────────────────────────────────────────────


class TestDestroyVm:
    def test_stops_and_destroys(self):
        manager, proxmox, redis = _make_manager()
        pipe = MagicMock()
        redis.pipeline.return_value = pipe

        manager._destroy_vm(300)

        proxmox.stop_vm.assert_called_once_with(300)
        proxmox.destroy_vm.assert_called_once_with(300)
        pipe.srem.assert_any_call("pool:idle", "300")
        pipe.srem.assert_any_call("pool:draining", "orcest-worker-300")
        pipe.hdel.assert_called_once_with("pool:active", "300")
        pipe.delete.assert_any_call("pool:done:orcest-worker-300")
        pipe.delete.assert_any_call("workers:heartbeat:orcest-worker-300")
        # I1 follow-up (final-review micro-round): the activity-watchdog
        # record is worker_id-keyed and survives VM destruction unless
        # deleted here -- otherwise a stale needs_reap=="1" record can
        # false-kill the replacement VM that later reuses this worker_id.
        pipe.delete.assert_any_call("workers:activity:orcest-worker-300")
        pipe.execute.assert_called_once()

    def test_cleans_redis_even_if_stop_fails(self):
        manager, proxmox, redis = _make_manager()
        pipe = MagicMock()
        redis.pipeline.return_value = pipe
        proxmox.stop_vm.side_effect = Exception("already stopped")

        manager._destroy_vm(300)

        # destroy still called
        proxmox.destroy_vm.assert_called_once_with(300)
        pipe.execute.assert_called_once()

    def test_retains_redis_state_if_destroy_fails(self):
        manager, proxmox, redis = _make_manager()
        pipe = MagicMock()
        redis.pipeline.return_value = pipe
        proxmox.destroy_vm.side_effect = Exception("API error")

        destroyed = manager._destroy_vm(300)

        assert destroyed is False
        pipe.execute.assert_not_called()
        redis.delete.assert_not_called()

    def test_retains_redis_state_if_both_proxmox_calls_fail(self):
        manager, proxmox, redis = _make_manager()
        pipe = MagicMock()
        redis.pipeline.return_value = pipe
        proxmox.stop_vm.side_effect = Exception("stop failed")
        proxmox.destroy_vm.side_effect = Exception("destroy failed")

        destroyed = manager._destroy_vm(300)

        assert destroyed is False
        pipe.execute.assert_not_called()

    def test_redis_pipeline_failure_does_not_raise(self):
        """If the Redis pipeline itself fails, _destroy_vm should not raise."""
        manager, proxmox, redis = _make_manager()
        pipe = MagicMock()
        redis.pipeline.return_value = pipe
        pipe.execute.side_effect = ConnectionError("Redis connection lost")

        # Should not raise
        destroyed = manager._destroy_vm(300)

        assert destroyed is False
        # Proxmox calls still happen
        proxmox.stop_vm.assert_called_once_with(300)
        proxmox.destroy_vm.assert_called_once_with(300)
        # Pipeline was attempted
        pipe.execute.assert_called_once()

    def test_marker_verification_failure_is_not_reported_as_success(self):
        manager, proxmox, redis = _make_manager()
        redis.exists.return_value = True

        destroyed = manager._destroy_vm(300)

        assert destroyed is False
        proxmox.destroy_vm.assert_called_once_with(300)


class TestClearDestroyedWorkerState:
    """I1 follow-up (final-review micro-round): the inventory-proves-absent
    cleanup path (used by ``_retry_provisioning_cleanups`` /
    ``_reconcile_ambiguous_clones`` when Proxmox no longer lists the VMID)
    must also delete the activity record, same as ``_destroy_stopped_vm``."""

    def test_deletes_activity_record_alongside_lifecycle_markers(self):
        manager, proxmox, redis = _make_manager()
        pipe = redis.pipeline.return_value

        result = manager._clear_destroyed_worker_state(300)

        assert result is True
        pipe.delete.assert_any_call("pool:done:orcest-worker-300")
        pipe.delete.assert_any_call("workers:heartbeat:orcest-worker-300")
        pipe.delete.assert_any_call("workers:activity:orcest-worker-300")


class TestDestroyVmRangeGuard:
    """C2: _destroy_vm must never touch a VMID outside the worker range."""

    def test_refuses_to_destroy_orchestrator_vmid(self):
        # Worker range is [300, 399]; the orchestrator VM is 199 (out of range).
        config = _make_config(vm_id_start=300)
        config.pool.vm_id_end = 399
        manager, proxmox, redis = _make_manager(config=config)
        pipe = MagicMock()
        redis.pipeline.return_value = pipe

        manager._destroy_vm(199)

        # No Proxmox lifecycle call may happen for an out-of-range VMID.
        proxmox.stop_vm.assert_not_called()
        proxmox.destroy_vm.assert_not_called()
        # And no Redis tracking pipeline is run for it either.
        pipe.execute.assert_not_called()

    def test_poisoned_done_key_does_not_destroy_orchestrator(self):
        # End-to-end via the done-key path: a poisoned key naming VM 199.
        config = _make_config(vm_id_start=300)
        config.pool.vm_id_end = 399
        manager, proxmox, redis = _make_manager(config=config)
        redis.scan_iter.return_value = ["pool:done:orcest-worker-199"]

        manager._check_done_workers()

        proxmox.stop_vm.assert_not_called()
        proxmox.destroy_vm.assert_not_called()
        # The poisoned key is still cleaned up so it cannot accumulate.
        redis.delete.assert_any_call("pool:done:orcest-worker-199")

    def test_in_range_vmid_still_destroyed(self):
        # Regression: a legitimate in-range worker VM is destroyed as before.
        config = _make_config(vm_id_start=300)
        config.pool.vm_id_end = 399
        manager, proxmox, redis = _make_manager(config=config)
        pipe = MagicMock()
        redis.pipeline.return_value = pipe

        manager._destroy_vm(305)

        proxmox.stop_vm.assert_called_once_with(305)
        proxmox.destroy_vm.assert_called_once_with(305)

    def test_open_ended_range_destroys_high_vmid(self):
        # vm_id_end unset (0) means open-ended above vm_id_start.
        config = _make_config(vm_id_start=300)  # vm_id_end defaults to 0
        manager, proxmox, redis = _make_manager(config=config)
        pipe = MagicMock()
        redis.pipeline.return_value = pipe

        manager._destroy_vm(99999)

        proxmox.destroy_vm.assert_called_once_with(99999)

    def test_unconfigured_start_destroys_nothing(self):
        # Without a configured worker range, nothing is provably a worker VM.
        config = FleetConfig(
            proxmox=ProxmoxConfig(node="pve", storage="local-lvm"),
            pool=PoolConfig(vm_id_start=0),
        )
        manager, proxmox, redis = _make_manager(config=config)

        manager._destroy_vm(300)

        proxmox.stop_vm.assert_not_called()
        proxmox.destroy_vm.assert_not_called()


# ── _clone_and_boot ──────────────────────────────────────────


class TestCloneAndBoot:
    def test_success(self):
        manager, proxmox, redis = _make_manager()
        proxmox.get_vm_ip.return_value = "10.20.0.50"

        vm_id = manager._clone_and_boot()

        assert vm_id == 300
        proxmox.clone_vm.assert_called_once_with(
            template_id=9000,
            new_id=300,
            name="orcest-worker-300",
            linked=True,
        )
        proxmox.set_vm_network.assert_called_once_with(
            300,
            mac="02:4F:52:00:01:2C",
        )
        proxmox.start_vm.assert_called_once_with(300)
        proxmox.get_vm_ip.assert_called_once_with(300)
        redis.pipeline.return_value.sadd.assert_called_with("pool:idle", "300")
        redis.sadd.assert_any_call("pool:provisioning", "300")

    def test_no_template_configured(self):
        config = _make_config(template_vm_id=0)
        manager, proxmox, redis = _make_manager(config=config)

        vm_id = manager._clone_and_boot()

        assert vm_id is None
        proxmox.clone_vm.assert_not_called()

    def test_redis_pointer_overrides_config_template(self):
        """When the Redis pointer is set, its VMID is used (not the config one)."""
        manager, proxmox, redis = _make_manager(config=_make_range_config())
        redis.get.return_value = "9005"
        proxmox.list_vms.return_value = [
            {"vmid": 9005, "name": "orcest-worker-template", "template": True},
        ]
        proxmox.get_vm_ip.return_value = "10.20.0.50"

        manager._clone_and_boot()

        _, kwargs = proxmox.clone_vm.call_args
        assert kwargs["template_id"] == 9005

    def test_falls_back_to_config_when_pointer_unset(self):
        """When the Redis pointer is unset (None), pool.template_vm_id is used."""
        manager, proxmox, redis = _make_manager()
        redis.get.return_value = None
        proxmox.get_vm_ip.return_value = "10.20.0.50"

        manager._clone_and_boot()

        _, kwargs = proxmox.clone_vm.call_args
        assert kwargs["template_id"] == 9000
        # And the pointer is initialised so future cycles read from Redis.
        redis.set_value.assert_called_once_with("pool:current_template_vmid", "9000")
        redis.set_ex.assert_not_called()

    def test_invalid_pointer_fails_closed(self):
        manager, proxmox, redis = _make_manager()
        redis.get.return_value = "not-a-number"

        with pytest.raises(RuntimeError, match="invalid Redis pointer"):
            manager._clone_and_boot()

        proxmox.clone_vm.assert_not_called()

    def test_template_pointer_read_failure_fails_closed(self):
        manager, proxmox, redis = _make_manager()
        redis.get.side_effect = ConnectionError("Redis down")

        with pytest.raises(RuntimeError, match="pointer read failed"):
            manager._clone_and_boot()

        proxmox.clone_vm.assert_not_called()

    def test_pointer_change_picked_up_next_cycle(self):
        """Subsequent _clone_and_boot calls re-read the pointer (no caching)."""
        manager, proxmox, redis = _make_manager(config=_make_range_config())
        proxmox.get_vm_ip.return_value = "10.20.0.50"
        redis.get.side_effect = ["9005", "9006"]
        template_9005 = {"vmid": 9005, "name": "template-a", "template": True}
        template_9006 = {"vmid": 9006, "name": "template-b", "template": True}
        proxmox.list_vms.side_effect = [
            [template_9005],
            [template_9005],
            [template_9006],
            [template_9006],
        ]

        manager._clone_and_boot()
        first = proxmox.clone_vm.call_args[1]["template_id"]
        manager._clone_and_boot()
        second = proxmox.clone_vm.call_args[1]["template_id"]

        assert first == 9005
        assert second == 9006

    def test_clears_and_verifies_old_generation_before_clone(self):
        manager, proxmox, redis = _make_manager()
        pipe = redis.pipeline.return_value

        manager._clone_and_boot()

        pipe.delete.assert_any_call("pool:done:orcest-worker-300")
        pipe.srem.assert_any_call("pool:draining", "orcest-worker-300")
        redis.exists.assert_any_call("pool:done:orcest-worker-300")
        redis.sismember.assert_any_call("pool:draining", "orcest-worker-300")
        # Defense in depth: the pre-reuse chokepoint must also purge the
        # worker_id-keyed activity record -- a surviving needs_reap=="1"
        # from the prior generation would false-kill the fresh replacement.
        pipe.delete.assert_any_call("workers:activity:orcest-worker-300")
        redis.exists.assert_any_call("workers:activity:orcest-worker-300")
        proxmox.clone_vm.assert_called_once()

    def test_refuses_clone_when_old_generation_cleanup_cannot_be_verified(self):
        manager, proxmox, redis = _make_manager()
        redis.exists.return_value = True

        with pytest.raises(RuntimeError, match="Refusing to reuse VMID 300"):
            manager._clone_and_boot()

        proxmox.clone_vm.assert_not_called()

    def test_refuses_allocation_when_proxmox_inventory_is_unavailable(self):
        manager, proxmox, redis = _make_manager()
        proxmox.list_vms.side_effect = RuntimeError("API unavailable")

        assert manager._clone_and_boot() is None

        proxmox.clone_vm.assert_not_called()

    def test_vm_no_ip_destroys(self):
        manager, proxmox, redis = _make_manager()
        proxmox.get_vm_ip.return_value = None
        pipe = MagicMock()
        redis.pipeline.return_value = pipe

        vm_id = manager._clone_and_boot()

        assert vm_id is None
        # VM should be destroyed since it didn't get an IP
        proxmox.stop_vm.assert_called_once_with(300)
        proxmox.destroy_vm.assert_called_once_with(300)

    def test_set_mac_failure_destroys_clone(self):
        """If setting the MAC address fails, the clone should be destroyed."""
        manager, proxmox, redis = _make_manager()
        proxmox.set_vm_network.side_effect = RuntimeError("API error")
        pipe = MagicMock()
        redis.pipeline.return_value = pipe

        vm_id = manager._clone_and_boot()

        assert vm_id is None
        proxmox.clone_vm.assert_called_once()
        proxmox.stop_vm.assert_called_once_with(300)
        proxmox.destroy_vm.assert_called_once_with(300)
        proxmox.start_vm.assert_not_called()

    def test_clone_failure_quarantines_without_name_based_cleanup(self):
        manager, proxmox, redis = _make_manager()
        proxmox.clone_vm.side_effect = RuntimeError("clone failed")

        vm_id = manager._clone_and_boot()

        assert vm_id is None
        proxmox.destroy_vm.assert_not_called()
        assert redis.sismember("pool:ambiguous-clones", "300")

    def test_clone_failure_does_not_destroy_foreign_vmid(self):
        manager, proxmox, redis = _make_manager()
        proxmox.clone_vm.side_effect = RuntimeError("clone failed")

        assert manager._clone_and_boot() is None

        proxmox.destroy_vm.assert_not_called()

    def test_clone_failure_never_calls_destroy_even_for_expected_name(self):
        manager, proxmox, redis = _make_manager()
        proxmox.clone_vm.side_effect = RuntimeError("clone failed")

        vm_id = manager._clone_and_boot()

        assert vm_id is None
        proxmox.destroy_vm.assert_not_called()

    def test_start_vm_failure_destroys_clone(self):
        """If start_vm raises, the cloned VM should be destroyed."""
        manager, proxmox, redis = _make_manager()
        proxmox.start_vm.side_effect = RuntimeError("start failed")
        pipe = MagicMock()
        redis.pipeline.return_value = pipe

        vm_id = manager._clone_and_boot()

        assert vm_id is None
        proxmox.clone_vm.assert_called_once()
        proxmox.stop_vm.assert_called_once_with(300)
        proxmox.destroy_vm.assert_called_once_with(300)
        redis.pipeline.return_value.sadd.assert_not_called()

    def test_uses_linked_clone(self):
        manager, proxmox, redis = _make_manager()
        proxmox.get_vm_ip.return_value = "10.20.0.50"

        manager._clone_and_boot()

        _, kwargs = proxmox.clone_vm.call_args
        assert kwargs["linked"] is True

    def test_correct_vm_naming(self):
        config = _make_config(vm_id_start=42)
        manager, proxmox, redis = _make_manager(config=config)
        proxmox.get_vm_ip.return_value = "10.20.0.50"

        manager._clone_and_boot()

        _, kwargs = proxmox.clone_vm.call_args
        assert kwargs["name"] == "orcest-worker-42"

    def test_no_ip_does_not_add_to_idle_pool(self):
        """When get_vm_ip returns None, the VM should not be in the idle set."""
        manager, proxmox, redis = _make_manager()
        proxmox.get_vm_ip.return_value = None
        pipe = MagicMock()
        redis.pipeline.return_value = pipe

        manager._clone_and_boot()

        redis.pipeline.return_value.sadd.assert_not_called()

    def test_idle_commit_failure_destroys_vm(self):
        """If the idle-pool commit fails, the VM is destroyed to avoid an orphan."""
        manager, proxmox, redis = _make_manager()
        proxmox.get_vm_ip.return_value = "10.20.0.50"
        pipe = MagicMock()
        pipe.execute.side_effect = [[], ConnectionError("Redis down"), []]
        redis.pipeline.return_value = pipe

        vm_id = manager._clone_and_boot()

        assert vm_id is None
        # VM was cloned and booted successfully, but the idle commit failed.
        proxmox.clone_vm.assert_called_once()
        proxmox.start_vm.assert_called_once_with(300)
        # _destroy_vm should have been called to clean up
        proxmox.stop_vm.assert_called_once_with(300)
        proxmox.destroy_vm.assert_called_once_with(300)


class TestCloneRedisPassword:
    """C1: the pool manager must forward the Redis AUTH password to cloned
    worker VMs via render_clone_userdata. The pool-manager container receives
    ORCEST_REDIS_PASSWORD in its own env (delivered by the pool compose stack's
    --env-file / passthrough). Without this, cloned workers cannot AUTH and
    every task stalls (NOAUTH)."""

    def test_clone_passes_redis_password_from_env(self, monkeypatch):
        monkeypatch.setenv("ORCEST_REDIS_PASSWORD", "pool-secret-pw")
        manager, proxmox, redis = _make_manager()
        proxmox.get_vm_ip.return_value = "10.20.0.50"

        with patch("orcest.fleet.pool_manager.render_clone_userdata") as render:
            render.return_value = "#cloud-config\n"
            manager._clone_and_boot()

        render.assert_called_once()
        kwargs = render.call_args.kwargs
        assert kwargs.get("redis_password") == "pool-secret-pw"

    def test_clone_passes_worker_backend_and_runner_mode(self, monkeypatch):
        monkeypatch.delenv("ORCEST_REDIS_PASSWORD", raising=False)
        config = _make_config()
        config.pool.worker_backend = "clauder"
        config.pool.worker_runner_type = "claude"
        config.pool.worker_runner_mode = "interactive"
        manager, proxmox, redis = _make_manager(config=config)
        proxmox.get_vm_ip.return_value = "10.20.0.50"

        with patch("orcest.fleet.pool_manager.render_clone_userdata") as render:
            render.return_value = "#cloud-config\n"
            manager._clone_and_boot()

        render.assert_called_once()
        kwargs = render.call_args.kwargs
        assert kwargs.get("worker_backend") == "clauder"
        assert kwargs.get("worker_runner_type") == "claude"
        assert kwargs.get("worker_runner_mode") == "interactive"

    def test_clone_passes_pool_watchdog_enabled(self, monkeypatch):
        """C1a: PoolConfig.watchdog_enabled must reach render_clone_userdata
        so newly-cloned workers pick up the fleet-level rollback lever."""
        monkeypatch.delenv("ORCEST_REDIS_PASSWORD", raising=False)
        config = _make_config()
        config.pool.watchdog_enabled = False
        manager, proxmox, redis = _make_manager(config=config)
        proxmox.get_vm_ip.return_value = "10.20.0.50"

        with patch("orcest.fleet.pool_manager.render_clone_userdata") as render:
            render.return_value = "#cloud-config\n"
            manager._clone_and_boot()

        render.assert_called_once()
        kwargs = render.call_args.kwargs
        assert kwargs.get("watchdog_enabled") is False

    def test_clone_omits_password_when_env_unset(self, monkeypatch):
        """Backward compat: with no ORCEST_REDIS_PASSWORD in the pool-manager env
        the clone is rendered without a password (empty string), preserving the
        unauthenticated/dev path rather than crashing."""
        monkeypatch.delenv("ORCEST_REDIS_PASSWORD", raising=False)
        manager, proxmox, redis = _make_manager()
        proxmox.get_vm_ip.return_value = "10.20.0.50"

        with patch("orcest.fleet.pool_manager.render_clone_userdata") as render:
            render.return_value = "#cloud-config\n"
            manager._clone_and_boot()

        render.assert_called_once()
        kwargs = render.call_args.kwargs
        assert kwargs.get("redis_password", "") == ""

    def test_cloned_userdata_actually_contains_password(self, monkeypatch):
        """End-to-end through the real render: the booted clone's /opt/orcest/.env
        carries the password so build_redis_config can AUTH."""
        import yaml

        monkeypatch.setenv("ORCEST_REDIS_PASSWORD", "e2e-pw")
        manager, proxmox, redis = _make_manager()
        proxmox.get_vm_ip.return_value = "10.20.0.50"

        manager._clone_and_boot()

        userdata = proxmox.set_cloud_init_userdata.call_args.args[1]
        data = yaml.safe_load(userdata)
        env_file = next(f for f in data["write_files"] if f["path"] == "/opt/orcest/.env")
        assert "ORCEST_REDIS_PASSWORD" in env_file["content"]
        assert "e2e-pw" in env_file["content"]


class TestProvisioningRecovery:
    def test_failed_post_clone_cleanup_is_retried_by_normal_reconcile(self):
        config = _make_config(pool_size=0)
        manager, proxmox, redis = _make_manager(config=config)
        proxmox.set_vm_network.side_effect = RuntimeError("network provisioning failed")
        proxmox.destroy_vm.side_effect = [RuntimeError("destroy unavailable"), None]

        assert manager._clone_and_boot() is None
        assert redis.sismember("pool:provisioning", "300")

        proxmox.set_vm_network.side_effect = None
        proxmox.list_vms.return_value = [
            {"vmid": 9000, "name": "orcest-worker-template", "template": True},
            {"vmid": 300, "name": "orcest-worker-300", "template": False},
        ]
        manager.reconcile()

        assert proxmox.destroy_vm.call_count == 2
        assert not redis.sismember("pool:provisioning", "300")

    def test_confirmed_destroy_releases_allocation_even_if_redis_verify_fails(self):
        """D2b: a Redis blip after a confirmed destroy must not wedge the slot."""
        manager, proxmox, redis = _make_manager()
        manager._allocated_vmids.add(300)
        manager._owned_provisioning_vmids.add(300)
        redis.exists.side_effect = RuntimeError("redis blip")

        assert manager._destroy_stopped_vm(300) is False

        proxmox.destroy_vm.assert_called_once_with(300)
        assert 300 not in manager._allocated_vmids
        assert 300 not in manager._owned_provisioning_vmids
        redis.exists.side_effect = None
        assert manager._next_vm_id(preferred=300) == 300

    def test_failed_destroy_keeps_allocation_reserved(self):
        """The release is gated on Proxmox confirming the VM is gone."""
        manager, proxmox, _redis = _make_manager()
        manager._allocated_vmids.add(300)
        manager._owned_provisioning_vmids.add(300)
        proxmox.destroy_vm.side_effect = RuntimeError("destroy unavailable")

        assert manager._destroy_stopped_vm(300) is False

        assert 300 in manager._allocated_vmids
        assert 300 in manager._owned_provisioning_vmids

    def test_absent_ambiguous_clone_is_released(self):
        manager, proxmox, redis = _make_manager()
        redis.sadd("pool:ambiguous-clones", "300")

        assert manager._reconcile_ambiguous_clones() == set()

        assert not redis.sismember("pool:ambiguous-clones", "300")
        proxmox.destroy_vm.assert_not_called()
        assert manager._next_vm_id() == 300

    def test_present_ambiguous_clone_stays_quarantined(self, caplog):
        manager, proxmox, redis = _make_manager()
        redis.sadd("pool:ambiguous-clones", "300")
        proxmox.list_vms.return_value = [
            {"vmid": 9000, "name": "orcest-worker-template", "template": True},
            {"vmid": 300, "name": "orcest-worker-300", "template": False},
        ]

        assert manager._reconcile_ambiguous_clones() == {300}

        assert redis.sismember("pool:ambiguous-clones", "300")
        proxmox.destroy_vm.assert_not_called()
        assert "operator" in caplog.text.lower() or "inspect" in caplog.text.lower()

    def test_present_ambiguity_quarantines_only_its_own_slot(self):
        """D1: an unresolved quarantine must never halt refill of other slots."""
        config = _make_config(pool_size=1)
        manager, proxmox, redis = _make_manager(config=config)
        redis.sadd("pool:ambiguous-clones", "300")
        proxmox.list_vms.return_value = [
            {"vmid": 9000, "name": "orcest-worker-template", "template": True},
            {"vmid": 300, "name": "orcest-worker-300", "template": False},
        ]

        manager.reconcile()

        # The quarantined VMID is never cloned over, but the pool still refills.
        proxmox.clone_vm.assert_called_once()
        assert proxmox.clone_vm.call_args.kwargs["new_id"] == 301
        assert redis.sismember("pool:ambiguous-clones", "300")

    def test_unresolved_ambiguity_does_not_drain_profiled_fleet_to_zero(self):
        """D1: quarantined slot is skipped; every other provider slot refills."""
        config = FleetConfig(
            proxmox=ProxmoxConfig(node="pve", storage="local-lvm"),
            pool=PoolConfig(
                size=3,
                template_vm_id=9000,
                vm_id_start=300,
                vm_id_end=399,
                worker_profiles=[
                    WorkerProfileConfig(backend="clauder"),
                    WorkerProfileConfig(backend="codex"),
                    WorkerProfileConfig(backend="grok"),
                ],
            ),
        )
        manager, proxmox, redis = _make_manager(config=config)
        redis.sadd("pool:ambiguous-clones", "300")
        proxmox.list_vms.return_value = [
            {"vmid": 9000, "name": "orcest-worker-template", "template": True},
            {"vmid": 300, "name": "orcest-worker-300", "template": False},
        ]

        with patch("orcest.fleet.pool_manager.render_clone_userdata"):
            manager.reconcile()

        assert [call.kwargs["new_id"] for call in proxmox.clone_vm.call_args_list] == [301, 302]
        assert redis.sismember("pool:ambiguous-clones", "300")

    def test_unreadable_quarantine_state_still_suppresses_refill(self):
        """Unknown quarantine state (not a known-blocked slot) blocks cloning."""
        config = _make_config(pool_size=1)
        manager, proxmox, redis = _make_manager(config=config)

        with patch.object(manager, "_reconcile_ambiguous_clones", return_value=None):
            manager.reconcile()

        proxmox.clone_vm.assert_not_called()

    def test_ambiguity_does_not_block_owned_cleanup_retry(self):
        config = _make_config(pool_size=1)
        manager, proxmox, redis = _make_manager(config=config)
        redis.sadd("pool:ambiguous-clones", "300")
        redis.sadd("pool:provisioning", "301")
        proxmox.list_vms.return_value = [
            {"vmid": 9000, "name": "orcest-worker-template", "template": True},
            {"vmid": 300, "name": "orcest-worker-300", "template": False},
            {"vmid": 301, "name": "orcest-worker-301", "template": False},
        ]

        manager.reconcile()

        proxmox.destroy_vm.assert_called_once_with(301)
        # 300 stays quarantined, 301 was reclaimed, so the deficit is met by 302.
        assert proxmox.clone_vm.call_args.kwargs["new_id"] == 302
        assert redis.sismember("pool:ambiguous-clones", "300")

    def test_failed_provisioning_cleanup_blocks_only_that_slot(self):
        """D1: a stuck provisioning cleanup must not stop the rest of the pool."""
        config = _make_config(pool_size=1)
        manager, proxmox, redis = _make_manager(config=config)
        redis.sadd("pool:provisioning", "300")
        proxmox.list_vms.return_value = [
            {"vmid": 9000, "name": "orcest-worker-template", "template": True},
            {"vmid": 300, "name": "orcest-worker-300", "template": False},
        ]
        proxmox.destroy_vm.side_effect = RuntimeError("destroy unavailable")

        manager.reconcile()

        proxmox.destroy_vm.assert_called_once_with(300)
        assert redis.sismember("pool:provisioning", "300")
        proxmox.clone_vm.assert_called_once()
        assert proxmox.clone_vm.call_args.kwargs["new_id"] == 301


def _make_range_config(template_vmid_range: list[int] | None = None) -> FleetConfig:
    """Build a config with a template VMID range (blue/green template mode)."""
    return FleetConfig(
        proxmox=ProxmoxConfig(node="pve", storage="local-lvm"),
        pool=PoolConfig(
            size=4,
            template_vm_id=0,
            template_vmid_range=template_vmid_range or [9000, 9009],
            vm_id_start=300,
            storage="ssd-pool",
        ),
    )


class TestResolveTemplateValidation:
    """A dangling template pointer must not drive an endless clone storm."""

    def test_existing_template_used_without_repoint(self):
        """When the pointer names a live template it is used as-is."""
        manager, proxmox, redis = _make_manager(config=_make_range_config())
        redis.get.return_value = "9005"
        proxmox.list_vms.return_value = [
            {"vmid": 9005, "name": "orcest-worker-template", "template": True},
        ]

        manager._clone_and_boot()

        proxmox.clone_vm.assert_called_once()
        assert proxmox.clone_vm.call_args[1]["template_id"] == 9005
        # No recovery needed → the template pointer is not rewritten.
        redis.set_value.assert_not_called()

    def test_missing_template_recovers_and_repoints(self):
        """A dangling pointer falls back to a live in-range template and repoints."""
        manager, proxmox, redis = _make_manager(config=_make_range_config())
        redis.get.return_value = "9002"  # pointer names a destroyed template
        proxmox.vm_exists.return_value = False
        proxmox.list_vms.return_value = [
            {"vmid": 9001, "name": "orcest-worker-template", "template": True},
            {"vmid": 300, "name": "orcest-worker-300", "template": False},
        ]

        manager._clone_and_boot()

        # The sole unambiguous live template is safe to recover.
        proxmox.clone_vm.assert_called_once()
        assert proxmox.clone_vm.call_args[1]["template_id"] == 9001
        # The pointer is durable for as long as the template remains valid.
        redis.set_value.assert_called_once_with("pool:current_template_vmid", "9001")
        redis.set_ex.assert_not_called()

    def test_missing_pointer_range_mode_discovers_live_template(self):
        """Range-only startup can recover from a missing Redis template pointer."""
        manager, proxmox, redis = _make_manager(config=_make_range_config())
        redis.get.return_value = None
        proxmox.list_vms.return_value = [
            {"vmid": 9004, "name": "orcest-worker-template", "template": True},
            {"vmid": 9005, "name": "orcest-worker-template", "template": False},
        ]

        manager._clone_and_boot()

        proxmox.vm_exists.assert_not_called()
        proxmox.clone_vm.assert_called_once()
        assert proxmox.clone_vm.call_args[1]["template_id"] == 9004
        redis.set_value.assert_called_once_with("pool:current_template_vmid", "9004")

    def test_missing_pointer_with_multiple_templates_fails_closed(self):
        manager, proxmox, redis = _make_manager(config=_make_range_config())
        redis.get.return_value = None
        proxmox.list_vms.return_value = [
            {"vmid": 9000, "name": "orcest-worker-template", "template": True},
            {"vmid": 9004, "name": "orcest-worker-template", "template": True},
        ]

        assert manager._clone_and_boot() is None

        proxmox.clone_vm.assert_not_called()
        redis.set_value.assert_not_called()

    def test_missing_template_no_replacement_returns_none(self):
        """With no live template anywhere, no clone is attempted."""
        manager, proxmox, redis = _make_manager(config=_make_range_config())
        redis.get.return_value = "9002"
        proxmox.vm_exists.return_value = False
        proxmox.list_vms.return_value = [
            {"vmid": 300, "name": "orcest-worker-300", "template": False},
        ]

        vm_id = manager._clone_and_boot()

        assert vm_id is None
        proxmox.clone_vm.assert_not_called()

    def test_inventory_error_fails_closed(self):
        manager, proxmox, redis = _make_manager(config=_make_range_config())
        redis.get.return_value = "9005"
        proxmox.list_vms.side_effect = RuntimeError("API timeout")

        assert manager._clone_and_boot() is None

        proxmox.clone_vm.assert_not_called()

    def test_existing_non_template_pointer_is_rejected(self):
        manager, proxmox, redis = _make_manager(config=_make_range_config())
        redis.get.return_value = "9005"
        proxmox.list_vms.return_value = [
            {"vmid": 9005, "name": "ordinary-vm", "template": False},
        ]

        assert manager._clone_and_boot() is None

        proxmox.clone_vm.assert_not_called()

    def test_template_pointer_outside_configured_authority_is_rejected(self):
        manager, proxmox, redis = _make_manager(config=_make_range_config([9000, 9003]))
        redis.get.return_value = "9010"
        proxmox.list_vms.return_value = [
            {"vmid": 9010, "name": "foreign-template", "template": True},
        ]

        assert manager._clone_and_boot() is None

        proxmox.clone_vm.assert_not_called()

    def test_replacement_ignores_non_template_and_out_of_range(self):
        """Recovery only picks Proxmox templates inside the configured range."""
        manager, proxmox, redis = _make_manager(
            config=_make_range_config(template_vmid_range=[9000, 9003])
        )
        redis.get.return_value = "9002"
        proxmox.vm_exists.return_value = False
        proxmox.list_vms.return_value = [
            {"vmid": 9000, "name": "orcest-worker-template", "template": True},
            {"vmid": 9500, "name": "orcest-worker-template", "template": True},
            {"vmid": 9001, "name": "orcest-worker-template", "template": False},
        ]

        manager._clone_and_boot()

        proxmox.clone_vm.assert_called_once()
        # 9500 is out of range, 9001 is not a template → only 9000 qualifies.
        assert proxmox.clone_vm.call_args[1]["template_id"] == 9000


# ── _detect_active_workers ───────────────────────────────────


class TestDetectActiveWorkers:
    def test_no_idle_workers_noop(self):
        manager, proxmox, redis = _make_manager()
        redis._idle_set = set()

        manager._detect_active_workers()

        redis.smembers.assert_called_once_with("pool:idle")
        redis.xinfo_groups_raw.assert_not_called()

    def test_idle_worker_becomes_active(self):
        manager, proxmox, redis = _make_manager()
        redis._idle_set = {"300"}
        redis.xinfo_groups_raw.return_value = [
            {"name": "workers", "pending": 1},
        ]
        redis.xinfo_consumers_raw.return_value = [
            {"name": "orcest-worker-300", "pending": 1},
        ]
        pipe = MagicMock()
        redis.pipeline.return_value = pipe

        with patch("orcest.fleet.pool_manager.time") as mock_time:
            mock_time.time.return_value = 1000.0
            manager._detect_active_workers()

        pipe.srem.assert_called_once_with("pool:idle", "300")
        pipe.hset.assert_called_once_with("pool:active", "300", "1000.0")
        pipe.execute.assert_called_once()

    def test_idle_worker_stays_idle(self):
        manager, proxmox, redis = _make_manager()
        redis._idle_set = {"300"}
        redis.xinfo_groups_raw.return_value = [
            {"name": "workers", "pending": 0},
        ]
        redis.xinfo_consumers_raw.return_value = [
            {"name": "orcest-worker-300", "pending": 0},
        ]

        manager._detect_active_workers()

        # No pipeline calls for moving to active
        redis.pipeline.assert_not_called()

    def test_handles_xinfo_groups_raw_error(self):
        manager, proxmox, redis = _make_manager()
        redis._idle_set = {"300"}
        redis.xinfo_groups_raw.side_effect = Exception("stream not found")

        # Should not raise
        manager._detect_active_workers()

    def test_handles_xinfo_consumers_raw_error(self):
        manager, proxmox, redis = _make_manager()
        redis._idle_set = {"300"}
        redis.xinfo_groups_raw.return_value = [
            {"name": "workers", "pending": 1},
        ]
        redis.xinfo_consumers_raw.side_effect = Exception("group not found")

        # Should not raise
        manager._detect_active_workers()

    @pytest.mark.parametrize("pending", ["not-an-int", -1, None, 1.5, True])
    def test_malformed_pending_value_marks_discovery_incomplete(self, pending):
        manager, _proxmox, redis = _make_manager()
        redis.xinfo_groups_raw.return_value = [{"name": "workers", "pending": 1}]
        redis.xinfo_consumers_raw.return_value = [
            {"name": "orcest-worker-300", "pending": pending},
        ]

        names, complete = manager._consumers_with_pending_status()

        assert names == set()
        assert complete is False

    def test_non_integer_idle_member_skipped(self):
        manager, proxmox, redis = _make_manager()
        redis._idle_set = {"not-a-number", "300"}
        redis.xinfo_groups_raw.return_value = [
            {"name": "workers", "pending": 1},
        ]
        redis.xinfo_consumers_raw.return_value = [
            {"name": "orcest-worker-300", "pending": 1},
        ]
        pipe = MagicMock()
        redis.pipeline.return_value = pipe

        with patch("orcest.fleet.pool_manager.time") as mock_time:
            mock_time.time.return_value = 1000.0
            manager._detect_active_workers()

        # Only VM 300 should be moved, the invalid member skipped
        pipe.srem.assert_called_once_with("pool:idle", "300")

    def test_pipeline_failure_does_not_crash(self):
        """If the Redis pipeline fails, the error is logged but does not propagate."""
        manager, proxmox, redis = _make_manager()
        redis._idle_set = {"300"}
        redis.xinfo_groups_raw.return_value = [
            {"name": "workers", "pending": 1},
        ]
        redis.xinfo_consumers_raw.return_value = [
            {"name": "orcest-worker-300", "pending": 1},
        ]
        pipe = MagicMock()
        pipe.execute.side_effect = ConnectionError("Redis down")
        redis.pipeline.return_value = pipe

        with patch("orcest.fleet.pool_manager.time") as mock_time:
            mock_time.time.return_value = 1000.0
            # Should not raise
            manager._detect_active_workers()

        # Pipeline was attempted
        pipe.execute.assert_called_once()


# ── idle heartbeat liveness ──────────────────────────────────


class TestIdleHeartbeatLiveness:
    @patch("orcest.fleet.pool_manager.time")
    def test_missing_idle_heartbeat_waits_for_continuous_dwell(self, mock_time):
        manager, proxmox, redis = _make_manager()
        redis._idle_set = {"300"}
        mock_time.time.side_effect = [1000.0, 1299.0]

        manager._replace_idle_workers_missing_heartbeat()
        manager._replace_idle_workers_missing_heartbeat()

        proxmox.stop_vm.assert_not_called()
        proxmox.destroy_vm.assert_not_called()
        assert manager._idle_missing_heartbeat_since == {300: 1000.0}
        redis.set_ex.assert_any_call("pool:write-health", "1000", ttl=600)
        redis.set_ex_raw.assert_not_called()

    @patch("orcest.fleet.pool_manager.time")
    def test_replaces_idle_vm_after_continuous_writable_missing_dwell(self, mock_time):
        manager, proxmox, redis = _make_manager()
        redis._idle_set = {"300"}
        mock_time.time.side_effect = [1000.0, 1301.0, 1301.0]
        mock_time.monotonic.side_effect = [0, 100]

        manager._replace_idle_workers_missing_heartbeat()
        manager._replace_idle_workers_missing_heartbeat()

        proxmox.stop_vm.assert_called_once_with(300)
        proxmox.destroy_vm.assert_called_once_with(300)
        assert "300" not in redis._idle_set
        assert manager._idle_missing_heartbeat_since == {}

    @patch("orcest.fleet.pool_manager.time")
    def test_heartbeat_appearing_during_dwell_clears_missing_state(self, mock_time):
        manager, proxmox, redis = _make_manager()
        redis._idle_set = {"300"}
        redis.exists.side_effect = [False, True]
        mock_time.time.side_effect = [1000.0, 1200.0]

        manager._replace_idle_workers_missing_heartbeat()
        manager._replace_idle_workers_missing_heartbeat()

        proxmox.stop_vm.assert_not_called()
        assert manager._idle_missing_heartbeat_since == {}

    @patch("orcest.fleet.pool_manager.time")
    def test_write_failure_resets_complete_idle_dwell(self, mock_time):
        manager, proxmox, redis = _make_manager()
        redis._idle_set = {"300"}
        redis.set_ex.side_effect = [None, ConnectionError("OOM"), None, None]
        mock_time.time.side_effect = [1000.0, 1200.0, 1400.0, 1699.0]

        manager._replace_idle_workers_missing_heartbeat()
        manager._replace_idle_workers_missing_heartbeat()
        manager._replace_idle_workers_missing_heartbeat()
        manager._replace_idle_workers_missing_heartbeat()

        proxmox.stop_vm.assert_not_called()
        assert manager._idle_liveness_write_healthy_since == 1400.0
        assert manager._idle_missing_heartbeat_since == {300: 1400.0}

    @patch("orcest.fleet.pool_manager.time")
    def test_pool_manager_restart_starts_with_empty_idle_dwell(self, mock_time):
        config = _make_config()
        proxmox = _make_proxmox()
        redis = _make_redis({"300"})
        first = PoolManager(config=config, proxmox=proxmox, redis=redis)
        second = PoolManager(config=config, proxmox=proxmox, redis=redis)
        mock_time.time.side_effect = [1000.0, 1301.0]

        first._replace_idle_workers_missing_heartbeat()
        second._replace_idle_workers_missing_heartbeat()

        proxmox.stop_vm.assert_not_called()
        assert second._idle_missing_heartbeat_since == {300: 1301.0}

    @patch("orcest.fleet.pool_manager.time")
    def test_profiled_breaker_limits_replacements_per_profile(self, mock_time):
        config = _make_config(pool_size=3)
        config.pool.worker_profiles = [WorkerProfileConfig(backend="codex")]
        manager, proxmox, redis = _make_manager(config=config)
        redis._idle_set = {"300", "301", "302"}
        mock_time.time.side_effect = [1000.0, 1301.0, 1301.0, 1301.0]
        mock_time.monotonic.side_effect = [0, 100, 0, 100]

        manager._replace_idle_workers_missing_heartbeat()
        manager._replace_idle_workers_missing_heartbeat()

        assert proxmox.stop_vm.call_count == 2
        assert proxmox.destroy_vm.call_count == 2
        assert redis._idle_set == {"302"}
        assert manager._idle_liveness_breaker_open["codex:codex:"] is True

    @patch("orcest.fleet.pool_manager.time")
    def test_profiled_breaker_clears_after_window(self, mock_time):
        config = _make_config(pool_size=3)
        config.pool.worker_profiles = [WorkerProfileConfig(backend="codex")]
        manager, proxmox, redis = _make_manager(config=config)
        manager._idle_liveness_breaker_events["codex:codex:"] = [1000.0, 1100.0]
        manager._idle_liveness_breaker_open["codex:codex:"] = True
        redis._idle_set = {"302"}
        mock_time.time.return_value = 2001.0

        manager._replace_idle_workers_missing_heartbeat()

        assert manager._idle_liveness_breaker_open["codex:codex:"] is False

    @patch("orcest.fleet.pool_manager.time")
    def test_out_of_range_idle_vmid_does_not_abort_liveness_pass(self, mock_time):
        """A stale pool:idle member below vm_id_start must not raise into reconcile()."""
        manager, proxmox, redis = _make_manager()
        redis._idle_set = {"299", "300"}
        mock_time.time.side_effect = [1000.0, 1301.0, 1301.0]
        mock_time.monotonic.side_effect = [0, 100]

        manager._replace_idle_workers_missing_heartbeat()
        manager._replace_idle_workers_missing_heartbeat()

        proxmox.stop_vm.assert_called_once_with(300)
        proxmox.destroy_vm.assert_called_once_with(300)
        assert "299" in redis._idle_set
        assert 299 not in manager._idle_missing_heartbeat_since

    @patch("orcest.fleet.pool_manager.time")
    def test_per_vm_heartbeat_read_failure_does_not_reset_global_dwell(self, mock_time):
        """A single EXISTS blip must not wipe write-health or other VMs' dwell."""
        manager, proxmox, redis = _make_manager()
        redis._idle_set = {"300", "301"}
        redis.exists.side_effect = [
            False,  # 300 pass 1
            False,  # 301 pass 1
            ConnectionError("blip"),  # 300 pass 2
            False,  # 301 pass 2
        ]
        mock_time.time.side_effect = [1000.0, 1200.0]

        manager._replace_idle_workers_missing_heartbeat()
        manager._replace_idle_workers_missing_heartbeat()

        assert manager._idle_liveness_write_healthy_since == 1000.0
        assert manager._idle_missing_heartbeat_since == {300: 1000.0, 301: 1000.0}
        proxmox.stop_vm.assert_not_called()


# ── _fill_pool ───────────────────────────────────────────────


class TestFillPool:
    def test_fills_deficit(self):
        config = _make_config(pool_size=3)
        manager, proxmox, redis = _make_manager(config=config)
        redis.scard.return_value = 1
        redis.hlen.return_value = 0
        proxmox.get_vm_ip.return_value = "10.20.0.50"

        manager._fill_pool()

        assert proxmox.clone_vm.call_count == 2

    def test_no_deficit(self):
        config = _make_config(pool_size=2)
        manager, proxmox, redis = _make_manager(config=config)
        redis.scard.return_value = 1
        redis.hlen.return_value = 1

        manager._fill_pool()

        proxmox.clone_vm.assert_not_called()

    def test_over_target(self):
        config = _make_config(pool_size=2)
        manager, proxmox, redis = _make_manager(config=config)
        redis.scard.return_value = 2
        redis.hlen.return_value = 1

        manager._fill_pool()

        proxmox.clone_vm.assert_not_called()

    def test_clone_failure_stops_refill_until_reconciled(self):
        config = _make_config(pool_size=3)
        manager, proxmox, redis = _make_manager(config=config)
        redis.scard.return_value = 0
        redis.hlen.return_value = 0
        proxmox.clone_vm.side_effect = [
            RuntimeError("first clone failed"),
            None,  # second succeeds
            None,  # third succeeds
        ]
        proxmox.get_vm_ip.return_value = "10.20.0.50"

        manager._fill_pool()

        assert proxmox.clone_vm.call_count == 1
        proxmox.start_vm.assert_not_called()
        proxmox.destroy_vm.assert_not_called()

    def test_uses_correct_redis_keys(self):
        config = _make_config(pool_size=4)
        manager, proxmox, redis = _make_manager(config=config)
        redis.scard.return_value = 4
        redis.hlen.return_value = 0

        manager._fill_pool()

        redis.scard.assert_called_once_with("pool:idle")
        redis.hlen.assert_called_once_with("pool:active")

    def test_counts_active_towards_total(self):
        """Active VMs count toward the total, reducing deficit."""
        config = _make_config(pool_size=4)
        manager, proxmox, redis = _make_manager(config=config)
        redis.scard.return_value = 1  # 1 idle
        redis.hlen.return_value = 2  # 2 active
        # Total = 3, deficit = 1
        proxmox.get_vm_ip.return_value = "10.20.0.50"

        manager._fill_pool()

        assert proxmox.clone_vm.call_count == 1

    def test_drains_excess_idle(self):
        """When pool size shrinks, excess idle VMs are destroyed."""
        config = _make_config(pool_size=1)
        manager, proxmox, redis = _make_manager(config=config)
        redis.scard.return_value = 3  # 3 idle
        redis.hlen.return_value = 0
        redis._idle_set = {"300", "301", "302"}
        pipe = MagicMock()
        redis.pipeline.return_value = pipe

        manager._fill_pool()

        # Should destroy 2 excess idle VMs (3 idle, target 1)
        assert proxmox.stop_vm.call_count == 2
        assert proxmox.destroy_vm.call_count == 2
        proxmox.clone_vm.assert_not_called()

    def test_drain_does_not_kill_active(self):
        """Draining only removes idle VMs, never active ones."""
        config = _make_config(pool_size=0)
        manager, proxmox, redis = _make_manager(config=config)
        redis.scard.return_value = 1  # 1 idle
        redis.hlen.return_value = 2  # 2 active
        redis._idle_set = {"300"}
        pipe = MagicMock()
        redis.pipeline.return_value = pipe

        manager._fill_pool()

        # Only the 1 idle VM should be destroyed, not the 2 active
        assert proxmox.stop_vm.call_count == 1
        assert proxmox.destroy_vm.call_count == 1

    def test_drain_skips_vm_that_claimed_task_mid_pass(self):
        """M2-conc: a VM that claimed a task is never drained on pool shrink."""
        config = _make_config(pool_size=1)
        manager, proxmox, redis = _make_manager(config=config)
        redis.scard.return_value = 2  # 2 idle
        redis.hlen.return_value = 0
        redis._idle_set = {"300", "301"}
        # VM 300 has claimed a task since _detect_active_workers ran: its
        # consumer now reports a pending entry.
        redis.xinfo_groups_raw.return_value = [{"name": "workers", "pending": 1}]
        redis.xinfo_consumers_raw.return_value = [
            {"name": "orcest-worker-300", "pending": 1},
        ]
        pipe = MagicMock()
        redis.pipeline.return_value = pipe

        manager._fill_pool()

        # excess = 1; VM 300 is busy and must be skipped, so VM 301 is drained.
        proxmox.destroy_vm.assert_called_once_with(301)
        # VM 300 (with the pending task) must NOT be destroyed.
        for call in proxmox.destroy_vm.call_args_list:
            assert call.args[0] != 300

    def test_drain_skips_all_busy_vms(self):
        """If every idle VM is busy, nothing is drained this pass."""
        config = _make_config(pool_size=0)
        manager, proxmox, redis = _make_manager(config=config)
        redis.scard.return_value = 2
        redis.hlen.return_value = 0
        redis._idle_set = {"300", "301"}
        redis.xinfo_groups_raw.return_value = [{"name": "workers", "pending": 1}]
        redis.xinfo_consumers_raw.return_value = [
            {"name": "orcest-worker-300", "pending": 1},
            {"name": "orcest-worker-301", "pending": 1},
        ]
        pipe = MagicMock()
        redis.pipeline.return_value = pipe

        manager._fill_pool()

        # Both idle VMs hold a pending task -> none drained.
        proxmox.destroy_vm.assert_not_called()

    def test_transient_post_stop_check_failure_does_not_reclassify_vm_as_active(self):
        """A single transient Redis failure in the post-stop pending check
        must be retried, not treated as grounds to restart the VM and file
        it into pool:active (a state nothing transitions back to idle)."""
        config = _make_config(pool_size=0)
        manager, proxmox, redis = _make_manager(config=config)
        redis.scard.return_value = 1
        redis.hlen.return_value = 0
        redis._idle_set = {"300"}
        with patch.object(
            manager,
            "_consumers_with_pending_status",
            side_effect=[
                (set(), True),  # pre-reserve check
                (set(), True),  # post-quiesce check
                (set(), False),  # post-stop attempt 1: transient failure
                (set(), True),  # post-stop attempt 2: succeeds, no pending
            ],
        ):
            manager._fill_pool()

        # The drain completes: VM destroyed, never restarted, never marked active.
        proxmox.destroy_vm.assert_called_once_with(300)
        proxmox.start_vm.assert_not_called()
        pipe = redis.pipeline.return_value
        active_hsets = [
            c for c in pipe.hset.call_args_list if c.args and c.args[0] == "pool:active"
        ]
        assert active_hsets == []

    def test_persistent_post_stop_check_failure_restarts_vm_and_marks_active(self):
        """When every bounded retry fails, the conservative branch still
        restarts the VM and files it into pool:active for the health check."""
        config = _make_config(pool_size=0)
        manager, proxmox, redis = _make_manager(config=config)
        redis.scard.return_value = 1
        redis.hlen.return_value = 0
        redis._idle_set = {"300"}
        with patch.object(
            manager,
            "_consumers_with_pending_status",
            side_effect=[
                (set(), True),  # pre-reserve check
                (set(), True),  # post-quiesce check
                (set(), False),  # post-stop attempt 1
                (set(), False),  # post-stop attempt 2
                (set(), False),  # post-stop attempt 3 (bounded: no more calls)
            ],
        ) as check:
            manager._fill_pool()

        assert check.call_count == 5
        proxmox.destroy_vm.assert_not_called()
        proxmox.start_vm.assert_called_once_with(300)
        pipe = redis.pipeline.return_value
        active_hsets = [
            c for c in pipe.hset.call_args_list if c.args and c.args[0] == "pool:active"
        ]
        assert len(active_hsets) == 1
        assert active_hsets[0].args[1] == "300"

    def test_profiled_drain_retries_transient_post_stop_check_failure(self):
        """Same bounded retry protects the heterogeneous drain loop."""
        manager, proxmox, redis = _make_manager(config=self._mixed_config(size=1))
        redis._idle_set = {"301"}
        with patch.object(
            manager,
            "_consumers_with_pending_status",
            side_effect=[
                (set(), True),  # pre-reserve check
                (set(), True),  # post-quiesce check
                (set(), False),  # post-stop attempt 1: transient failure
                (set(), True),  # post-stop attempt 2: succeeds, no pending
            ],
        ):
            drained = manager._drain_profiled_idle_workers([301])

        assert drained == 1
        proxmox.destroy_vm.assert_called_once_with(301)
        proxmox.start_vm.assert_not_called()

    @staticmethod
    def _mixed_config(size: int = 4) -> FleetConfig:
        return FleetConfig(
            proxmox=ProxmoxConfig(node="pve", storage="local-lvm"),
            pool=PoolConfig(
                size=size,
                template_vm_id=9000,
                vm_id_start=300,
                vm_id_end=399,
                worker_profiles=[
                    WorkerProfileConfig(backend="clauder"),
                    WorkerProfileConfig(backend="codex"),
                    WorkerProfileConfig(backend="grok"),
                ],
            ),
        )

    def test_profiled_fill_creates_exact_round_robin_slots(self):
        manager, proxmox, redis = _make_manager(config=self._mixed_config())
        redis._idle_set = set()

        with patch("orcest.fleet.pool_manager.render_clone_userdata") as render:
            manager._fill_pool()

        assert [call.kwargs["new_id"] for call in proxmox.clone_vm.call_args_list] == [
            300,
            301,
            302,
            303,
        ]
        assert [call.kwargs["worker_backend"] for call in render.call_args_list] == [
            "clauder",
            "codex",
            "grok",
            "clauder",
        ]
        assert [call.kwargs["worker_runner_type"] for call in render.call_args_list] == [
            "claude",
            "codex",
            "grok",
            "claude",
        ]

    def test_profiled_scale_down_drains_only_slots_outside_target(self):
        manager, proxmox, redis = _make_manager(config=self._mixed_config(size=1))
        redis._idle_set = {"300", "301", "302"}

        manager._fill_pool()

        assert [call.args[0] for call in proxmox.destroy_vm.call_args_list] == [301, 302]
        proxmox.clone_vm.assert_not_called()

    def test_profiled_fill_replaces_missing_slot_while_active_extra_finishes(self):
        manager, proxmox, redis = _make_manager(config=self._mixed_config(size=2))
        redis._idle_set = {"300"}
        redis.hgetall.return_value = {"302": "1000.0"}
        proxmox.list_vms.return_value = [
            {"vmid": 9000, "name": "orcest-worker-template", "template": True},
            {"vmid": 302, "name": "orcest-worker-302", "template": False},
        ]

        with patch("orcest.fleet.pool_manager.render_clone_userdata") as render:
            manager._fill_pool()

        proxmox.clone_vm.assert_called_once_with(
            template_id=9000,
            new_id=301,
            name="orcest-worker-301",
            linked=True,
        )
        assert render.call_args.kwargs["worker_backend"] == "codex"
        proxmox.destroy_vm.assert_not_called()

    def test_profiled_missing_middle_slot_preserves_profile(self):
        manager, proxmox, redis = _make_manager(config=self._mixed_config(size=3))
        redis._idle_set = {"300", "302"}

        with patch("orcest.fleet.pool_manager.render_clone_userdata") as render:
            manager._fill_pool()

        proxmox.clone_vm.assert_called_once()
        assert proxmox.clone_vm.call_args.kwargs["new_id"] == 301
        assert render.call_args.kwargs["worker_backend"] == "codex"

    def test_profiled_fill_skips_only_the_slot_that_cannot_be_allocated(self):
        """D2a: a slot-local allocation error must not starve later slots."""
        manager, proxmox, redis = _make_manager(config=self._mixed_config(size=3))
        redis._idle_set = set()
        # VM 301 exists in Proxmox but is untracked in Redis, so _next_vm_id
        # rejects the preferred slot 301 while 300 and 302 stay allocatable.
        proxmox.list_vms.return_value = [
            {"vmid": 9000, "name": "orcest-worker-template", "template": True},
            {"vmid": 301, "name": "orcest-worker-301", "template": False},
        ]

        with patch("orcest.fleet.pool_manager.render_clone_userdata"):
            manager._fill_pool()

        assert [call.kwargs["new_id"] for call in proxmox.clone_vm.call_args_list] == [300, 302]

    def test_profiled_fill_stops_on_ambiguous_clone_outcome(self):
        """The ambiguity/ownership signal (None) still halts the fan-out."""
        manager, proxmox, redis = _make_manager(config=self._mixed_config(size=3))
        redis._idle_set = set()

        with patch.object(manager, "_clone_and_boot", return_value=None) as clone:
            manager._fill_pool()

        clone.assert_called_once()
        assert clone.call_args.kwargs["new_id"] == 300

    def test_failed_drain_destroy_is_queued_for_durable_retry(self):
        """D3: a stopped VM whose destroy failed must be reclaimable."""
        config = _make_config(pool_size=0)
        manager, proxmox, redis = _make_manager(config=config)
        redis.scard.return_value = 1
        redis.hlen.return_value = 0
        redis._idle_set = {"300"}
        proxmox.destroy_vm.side_effect = RuntimeError("destroy unavailable")

        manager._fill_pool()

        proxmox.stop_vm.assert_called_once_with(300)
        assert redis.sismember("pool:provisioning", "300")
        assert 300 in manager._owned_provisioning_vmids

        # The next pass reclaims it through the provisioning cleanup retry.
        proxmox.destroy_vm.side_effect = None
        proxmox.list_vms.return_value = [
            {"vmid": 9000, "name": "orcest-worker-template", "template": True},
            {"vmid": 300, "name": "orcest-worker-300", "template": False},
        ]

        assert manager._retry_provisioning_cleanups() == set()

        assert proxmox.destroy_vm.call_args.args[0] == 300
        assert not redis.sismember("pool:provisioning", "300")
        assert 300 not in manager._owned_provisioning_vmids

    def test_failed_profiled_drain_destroy_is_queued_for_durable_retry(self):
        """D3: same durable retry for the heterogeneous drain loop."""
        manager, proxmox, redis = _make_manager(config=self._mixed_config(size=1))
        redis._idle_set = {"300", "301"}
        proxmox.destroy_vm.side_effect = RuntimeError("destroy unavailable")

        manager._fill_pool()

        assert redis.sismember("pool:provisioning", "301")
        assert 301 in manager._owned_provisioning_vmids

    def test_preferred_worker_vmid_must_be_free_and_in_range(self):
        manager, proxmox, _redis = _make_manager(config=self._mixed_config(size=3))
        proxmox.list_vms.return_value = [
            {"vmid": 9000, "name": "orcest-worker-template", "template": True},
            {"vmid": 301, "name": "orcest-worker-301", "template": False},
        ]

        with pytest.raises(RuntimeError, match="already in use"):
            manager._next_vm_id(preferred=301)
        with pytest.raises(RuntimeError, match="outside the configured pool range"):
            manager._next_vm_id(preferred=299)


# ── _health_check ────────────────────────────────────────────


class TestHealthCheck:
    def test_no_active_workers(self):
        manager, proxmox, redis = _make_manager()
        redis.hgetall.return_value = {}

        manager._health_check()

        proxmox.stop_vm.assert_not_called()

    @patch("orcest.fleet.pool_manager.time")
    def test_healthy_worker_not_destroyed(self, mock_time):
        config = _make_config(max_task_duration=3600)
        manager, proxmox, redis = _make_manager(config=config)
        mock_time.time.return_value = 10000.0
        redis.hgetall.return_value = {"300": str(10000.0 - 100)}

        manager._health_check()

        proxmox.stop_vm.assert_not_called()
        proxmox.destroy_vm.assert_not_called()

    @patch("orcest.fleet.pool_manager.time")
    def test_expired_worker_destroyed(self, mock_time):
        config = _make_config(max_task_duration=3600)
        manager, proxmox, redis = _make_manager(config=config)
        pipe = MagicMock()
        redis.pipeline.return_value = pipe
        mock_time.time.return_value = 10000.0
        mock_time.monotonic.side_effect = [0, 100]
        redis.hgetall.return_value = {"300": str(10000.0 - 4000)}

        manager._health_check()

        proxmox.stop_vm.assert_called_once_with(300)
        proxmox.destroy_vm.assert_called_once_with(300)

    @patch("orcest.fleet.pool_manager.time")
    def test_multiple_workers_mixed(self, mock_time):
        config = _make_config(max_task_duration=3600)
        manager, proxmox, redis = _make_manager(config=config)
        pipe = MagicMock()
        redis.pipeline.return_value = pipe
        mock_time.time.return_value = 10000.0
        mock_time.monotonic.side_effect = [0, 100]
        redis.hgetall.return_value = {
            "300": str(10000.0 - 100),  # healthy
            "301": str(10000.0 - 5000),  # expired
        }

        manager._health_check()

        proxmox.stop_vm.assert_called_once_with(301)
        proxmox.destroy_vm.assert_called_once_with(301)

    def test_handles_invalid_timestamp(self):
        manager, proxmox, redis = _make_manager()
        redis.hgetall.return_value = {"300": "not-a-timestamp"}

        # Should not raise
        manager._health_check()
        proxmox.stop_vm.assert_not_called()

    def test_handles_invalid_vm_id(self):
        manager, proxmox, redis = _make_manager()
        redis.hgetall.return_value = {"not-a-number": "5000.0"}

        # Should not raise
        manager._health_check()
        proxmox.stop_vm.assert_not_called()

    @patch("orcest.fleet.pool_manager.time")
    def test_invalid_entry_alongside_valid(self, mock_time):
        """Invalid entries are skipped but valid expired entries are still destroyed."""
        config = _make_config(max_task_duration=3600)
        manager, proxmox, redis = _make_manager(config=config)
        mock_time.time.return_value = 10000.0
        mock_time.monotonic.side_effect = [0, 100]  # _destroy_vm stop-wait
        pipe = MagicMock()
        redis.pipeline.return_value = pipe

        redis.hgetall.return_value = {
            "not-a-number": "5000.0",
            "300": "5000.0",  # elapsed=5000 > 3600
        }

        manager._health_check()

        proxmox.stop_vm.assert_called_once_with(300)
        proxmox.destroy_vm.assert_called_once_with(300)

    def test_uses_correct_redis_key(self):
        manager, proxmox, redis = _make_manager()
        redis.hgetall.return_value = {}

        manager._health_check()

        redis.hgetall.assert_called_once_with("pool:active")

    @patch("orcest.fleet.pool_manager.time")
    def test_exactly_at_max_duration_not_destroyed(self, mock_time):
        """A VM at exactly max_task_duration should not be destroyed (> not >=)."""
        config = _make_config(max_task_duration=3600)
        manager, proxmox, redis = _make_manager(config=config)
        mock_time.time.return_value = 10000.0

        # elapsed = 10000 - 6400 = 3600, which is NOT > 3600
        redis.hgetall.return_value = {"300": "6400.0"}

        manager._health_check()

        proxmox.stop_vm.assert_not_called()

    @patch("orcest.fleet.pool_manager.time")
    def test_destroy_failure_does_not_block_other_expired(self, mock_time):
        """If destroying one timed-out VM fails, the rest are still processed."""
        config = _make_config(max_task_duration=3600)
        manager, proxmox, redis = _make_manager(config=config)
        pipe = MagicMock()
        redis.pipeline.return_value = pipe

        # Both expired
        redis.hgetall.return_value = {
            "300": "5000.0",  # elapsed=5000 > 3600
            "301": "4000.0",  # elapsed=6000 > 3600
        }

        call_count = 0

        def pipeline_execute_side_effect():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ConnectionError("Redis down")
            return []

        pipe.execute.side_effect = pipeline_execute_side_effect
        mock_time.time.return_value = 10000.0
        mock_time.monotonic.side_effect = [0, 100, 0, 100]

        # Should not raise
        manager._health_check()

        # Both VMs should have been attempted
        assert proxmox.stop_vm.call_count == 2


# ── _health_check reap coordination (H2-conc) ────────────────


class TestHealthCheckReapCoordination:
    """H2-conc: reaping an over-duration VM must coordinate Redis, not strand it."""

    def _build(self, fake_redis_client):
        from orcest.fleet.pool_manager import PoolManager

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

    def test_reaped_vm_publishes_transient_failure_and_clears_marker(self, fake_redis_client):
        import time as _time

        from orcest.shared.coordination import get_pending_task, set_pending_task
        from orcest.shared.models import CONSUMER_GROUP, ResultStatus, Task, TaskResult, TaskType

        rc = fake_redis_client  # prefix 'test'
        manager, proxmox = self._build(rc)
        worker_id = "orcest-worker-305"
        # Task stream is single-project here: key_prefix 'test' -> 'test:tasks:claude'.
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
        # Pending marker the orchestrator set when it enqueued the task.
        assert set_pending_task(rc, "owner/repo", "pr", 42, task.id) is True
        # Enqueue the task into the shared stream and have VM 305 claim it (PEL).
        rc.ensure_consumer_group("tasks:claude", CONSUMER_GROUP)
        rc.xadd("tasks:claude", task.to_dict())
        claimed = rc.xreadgroup(
            group=CONSUMER_GROUP, consumer=worker_id, stream="tasks:claude", block_ms=None
        )
        assert len(claimed) == 1
        # Mark VM 305 active and over-duration.
        rc.hset("pool:active", "305", str(_time.time() - 99999))

        manager._health_check()

        # 1) VM destroyed.
        proxmox.stop_vm.assert_called_once_with(305)
        proxmox.destroy_vm.assert_called_once_with(305)
        # 2) Pending marker cleared so the orchestrator can re-enqueue.
        assert get_pending_task(rc, "owner/repo", "pr", 42) is None
        # 3) A transient-FAILED result was published to the project results stream.
        results = rc.xrevrange("results", count=10)
        assert len(results) == 1
        _, fields = results[0]
        published = TaskResult.from_dict(fields)
        assert published.task_id == task.id
        assert published.status == ResultStatus.FAILED
        assert published.summary.startswith("[transient] ")
        assert published.repo == task.repo
        assert published.resource_id == 42
        # 4) The consumer's PEL slot was released (consumer deleted / no pending).
        consumers = {c["name"]: c for c in rc.xinfo_consumers("tasks:claude", CONSUMER_GROUP)}
        assert consumers.get(worker_id, {"pending": 0})["pending"] == 0

    def test_health_reaper_fences_worker_before_pel_inspection(self, fake_redis_client):
        """No late worker write can race Redis recovery after timeout detection."""
        import time as _time

        from orcest.fleet.pool_manager import _StopVmOutcome

        manager, _proxmox = self._build(fake_redis_client)
        fake_redis_client.hset("pool:active", "305", str(_time.time() - 99999))
        events: list[str] = []

        def stopped(_vm_id):
            events.append("stopped")
            return _StopVmOutcome(stopped=True, confirmed_transition=True)

        def coordinated(_vm_id, reason=None, elapsed_seconds=None, killed_at_unix=None):
            assert events == ["stopped"]
            assert elapsed_seconds is not None
            assert killed_at_unix is not None
            events.append("redis-coordinated")
            return True

        with (
            patch.object(manager, "_stop_vm_with_outcome", side_effect=stopped),
            patch.object(manager, "_coordinate_reaped_vm", side_effect=coordinated),
            patch.object(
                manager,
                "_destroy_stopped_vm",
                side_effect=lambda _vm_id: events.append("destroyed") or True,
            ),
        ):
            manager._health_check()

        assert events == ["stopped", "redis-coordinated", "destroyed"]

    def test_health_reaper_stop_failure_never_inspects_or_mutates_pel(self, fake_redis_client):
        """Failure to fence a live writer preserves all Redis recovery state."""
        import time as _time

        from orcest.fleet.pool_manager import _StopVmOutcome

        manager, _proxmox = self._build(fake_redis_client)
        fake_redis_client.hset("pool:active", "305", str(_time.time() - 99999))
        with (
            patch.object(
                manager,
                "_stop_vm_with_outcome",
                return_value=_StopVmOutcome(stopped=False, confirmed_transition=False),
            ),
            patch.object(manager, "_coordinate_reaped_vm") as coordinate,
            patch.object(manager, "_destroy_stopped_vm") as destroy,
        ):
            manager._health_check()

        coordinate.assert_not_called()
        destroy.assert_not_called()
        assert fake_redis_client.hgetall("pool:active").get("305") is not None

    def test_reaper_malformed_entry_is_redacted_and_durable_before_ack(self, fake_redis_client):
        from orcest.shared.models import CONSUMER_GROUP

        rc = fake_redis_client
        manager, _proxmox = self._build(rc)
        worker_id = "orcest-worker-305"
        rc.ensure_consumer_group("tasks:claude", CONSUMER_GROUP)
        entry_id = rc.xadd(
            "tasks:claude",
            {
                "id": "malformed-secret-task",
                "repo": "owner/repo",
                "resource_type": "pr",
                "resource_id": "42",
                "token": "github-secret",
                "credential": "provider-secret",
                "claude_token": "claude-secret",
                "unexpected": "unknown-secret",
            },
        )
        assert rc.xreadgroup(
            group=CONSUMER_GROUP,
            consumer=worker_id,
            stream="tasks:claude",
            block_ms=None,
        )

        assert manager._coordinate_reaped_vm(305, reason=REAP_REASON_ORPHAN_PEL) is True

        rows = rc.client.xrevrange("test:dead-letter", count=10)
        assert len(rows) == 1
        fields = rows[0][1]
        assert fields["token"] == "[REDACTED]"
        assert fields["credential"] == "[REDACTED]"
        assert fields["claude_token"] == "[REDACTED]"
        assert "unexpected" not in fields
        assert all(
            secret not in str(fields)
            for secret in ("github-secret", "provider-secret", "claude-secret", "unknown-secret")
        )
        assert fields["original_entry_id"] == entry_id
        assert rc.client.xpending("test:tasks:claude", CONSUMER_GROUP)["pending"] == 0

    def test_reaper_malformed_ack_failure_is_incomplete_and_idempotent(
        self, fake_redis_client, monkeypatch
    ):
        from orcest.shared.models import CONSUMER_GROUP

        rc = fake_redis_client
        manager, _proxmox = self._build(rc)
        worker_id = "orcest-worker-305"
        rc.ensure_consumer_group("tasks:claude", CONSUMER_GROUP)
        rc.xadd("tasks:claude", {"id": "bad", "token": "secret"})
        assert rc.xreadgroup(
            group=CONSUMER_GROUP,
            consumer=worker_id,
            stream="tasks:claude",
            block_ms=None,
        )
        original_ack = rc.xack_raw
        original_delconsumer = rc.client.xgroup_delconsumer
        deleted_consumers: list[str] = []

        def fail_ack(*_args, **_kwargs):
            raise ConnectionError("ACK unavailable")

        def track_delconsumer(stream, group, consumer):
            deleted_consumers.append(stream)
            return original_delconsumer(stream, group, consumer)

        monkeypatch.setattr(rc, "xack_raw", fail_ack)
        monkeypatch.setattr(rc.client, "xgroup_delconsumer", track_delconsumer)

        assert manager._coordinate_reaped_vm(305, reason=REAP_REASON_ORPHAN_PEL) is False
        assert "test:tasks:claude" not in deleted_consumers
        assert len(rc.client.xrevrange("test:dead-letter", count=10)) == 1
        assert rc.client.xpending("test:tasks:claude", CONSUMER_GROUP)["pending"] == 1
        receipt_keys = list(rc.client.scan_iter(match="test:dead-letter:handoff:*"))
        assert len(receipt_keys) == 1
        assert rc.client.ttl(receipt_keys[0]) == -1

        monkeypatch.setattr(rc, "xack_raw", original_ack)
        assert manager._coordinate_reaped_vm(305, reason=REAP_REASON_ORPHAN_PEL) is True
        assert len(rc.client.xrevrange("test:dead-letter", count=10)) == 1
        assert rc.client.xpending("test:tasks:claude", CONSUMER_GROUP)["pending"] == 0

    def test_reaper_malformed_post_apply_ack_response_loss_is_terminal(
        self, fake_redis_client, monkeypatch
    ):
        from orcest.shared.models import CONSUMER_GROUP

        rc = fake_redis_client
        manager, _proxmox = self._build(rc)
        worker_id = "orcest-worker-305"
        rc.ensure_consumer_group("tasks:claude", CONSUMER_GROUP)
        rc.xadd("tasks:claude", {"id": "bad", "token": "secret"})
        assert rc.xreadgroup(
            group=CONSUMER_GROUP,
            consumer=worker_id,
            stream="tasks:claude",
            block_ms=None,
        )
        original_ack = rc.xack_raw

        def apply_then_lose_response(*args, **kwargs):
            original_ack(*args, **kwargs)
            raise ConnectionError("ACK response lost")

        monkeypatch.setattr(rc, "xack_raw", apply_then_lose_response)

        assert manager._coordinate_reaped_vm(305, reason=REAP_REASON_ORPHAN_PEL) is True
        assert len(rc.client.xrevrange("test:dead-letter", count=10)) == 1
        assert rc.client.xpending("test:tasks:claude", CONSUMER_GROUP)["pending"] == 0
        assert list(rc.client.scan_iter(match="test:dead-letter:handoff:*")) == []

    def test_reaped_vm_recovers_backend_stream_discovered_from_redis(self, fake_redis_client):
        import time as _time

        from orcest.shared.coordination import get_pending_task, set_pending_task
        from orcest.shared.models import CONSUMER_GROUP, ResultStatus, Task, TaskResult, TaskType

        rc = fake_redis_client
        manager, proxmox = self._build(rc)
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
        assert set_pending_task(rc, "owner/repo", "pr", 42, task.id) is True
        rc.ensure_consumer_group("tasks:grok", CONSUMER_GROUP)
        rc.xadd("tasks:grok", task.to_dict())
        claimed = rc.xreadgroup(
            group=CONSUMER_GROUP, consumer=worker_id, stream="tasks:grok", block_ms=None
        )
        assert len(claimed) == 1
        rc.hset("pool:active", "305", str(_time.time() - 99999))

        manager._health_check()

        proxmox.stop_vm.assert_called_once_with(305)
        proxmox.destroy_vm.assert_called_once_with(305)
        assert get_pending_task(rc, "owner/repo", "pr", 42) is None
        results = rc.xrevrange("results", count=10)
        assert len(results) == 1
        published = TaskResult.from_dict(results[0][1])
        assert published.task_id == task.id
        assert published.status == ResultStatus.FAILED
        assert published.summary.startswith("[transient] ")
        consumers = {c["name"]: c for c in rc.xinfo_consumers("tasks:grok", CONSUMER_GROUP)}
        assert consumers.get(worker_id, {"pending": 0})["pending"] == 0

    def test_reaper_finishes_checkpoint_only_credential_handoff_after_marker_ttl(
        self, fake_redis_client
    ):
        """A persistent private checkpoint outranks generic force-reap recovery."""
        from orcest.shared.coordination import get_pending_task, set_pending_task
        from orcest.shared.credential_handoff import (
            CREDENTIAL_CHECKPOINT_TTL_SECONDS,
            HANDOFF_MARKER_TTL_SECONDS,
            credential_checkpoint_key,
            store_credential_checkpoint,
        )
        from orcest.shared.models import (
            CONSUMER_GROUP,
            ResultStatus,
            Task,
            TaskResult,
            TaskType,
        )

        rc = fake_redis_client
        manager, _proxmox = self._build(rc)
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
        assert set_pending_task(rc, "owner/repo", "pr", 42, task.id) is True
        rc.ensure_consumer_group("tasks:claude", CONSUMER_GROUP)
        entry_id = rc.xadd("tasks:claude", task.to_dict())
        assert rc.xreadgroup(
            group=CONSUMER_GROUP,
            consumer=worker_id,
            stream="tasks:claude",
            block_ms=None,
        )
        result = TaskResult(
            task_id=task.id,
            worker_id=worker_id,
            status=ResultStatus.COMPLETED,
            branch=task.branch,
            summary="rotation completed",
            duration_seconds=5,
            resource_type=task.resource_type,
            resource_id=task.resource_id,
            repo=task.repo,
            credential_update='{"refresh_token":"private-rotated-secret"}',
            credential_update_minted_at=0,
        )
        checkpoint = store_credential_checkpoint(
            rc,
            "test:results",
            "test:tasks:claude",
            entry_id,
            task.id,
            result.to_dict(),
        )
        # This is the >30-day condition: any bounded public diagnostic/receipt
        # may already be gone, while the private checkpoint is still present.
        # The checkpoint carries a plaintext OAuth blob, so it is bounded too,
        # but strictly longer-lived than the public markers it must outlive.
        checkpoint_ttl = rc.client.ttl(checkpoint.key)
        assert checkpoint_ttl > HANDOFF_MARKER_TTL_SECONDS
        assert checkpoint_ttl <= CREDENTIAL_CHECKPOINT_TTL_SECONDS

        assert manager._coordinate_reaped_vm(305, reason=REAP_REASON_ORPHAN_PEL) is True

        rows = rc.client.xrevrange("test:results", count=10)
        assert len(rows) == 1
        recovered = TaskResult.from_dict(rows[0][1])
        assert recovered.status is ResultStatus.COMPLETED
        assert recovered.credential_update == '{"refresh_token":"private-rotated-secret"}'
        assert recovered.credential_update_minted_at > 0
        assert get_pending_task(rc, "owner/repo", "pr", 42) == task.id
        assert (
            rc.client.exists(
                credential_checkpoint_key("test:results", "test:tasks:claude", entry_id, task.id)
            )
            == 0
        )
        assert rc.client.xpending("test:tasks:claude", CONSUMER_GROUP)["pending"] == 0

    def test_reaper_deduplicates_already_durable_credential_with_checkpoint(
        self, fake_redis_client
    ):
        """The old publish-before-ACK window finishes without a second secret row."""
        from orcest.shared.coordination import get_pending_task, set_pending_task
        from orcest.shared.credential_handoff import (
            HANDOFF_FINGERPRINT_FIELD,
            handoff_marker_key,
            handoff_payload_fingerprint,
            store_credential_checkpoint,
        )
        from orcest.shared.models import (
            CONSUMER_GROUP,
            ResultStatus,
            Task,
            TaskResult,
            TaskType,
        )

        rc = fake_redis_client
        manager, _proxmox = self._build(rc)
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
        assert set_pending_task(rc, "owner/repo", "pr", 42, task.id) is True
        rc.ensure_consumer_group("tasks:claude", CONSUMER_GROUP)
        entry_id = rc.xadd("tasks:claude", task.to_dict())
        assert rc.xreadgroup(
            group=CONSUMER_GROUP,
            consumer=worker_id,
            stream="tasks:claude",
            block_ms=None,
        )
        result = TaskResult(
            task_id=task.id,
            worker_id=worker_id,
            status=ResultStatus.COMPLETED,
            branch=task.branch,
            summary="rotation completed",
            duration_seconds=5,
            resource_type=task.resource_type,
            resource_id=task.resource_id,
            repo=task.repo,
            credential_update='{"refresh_token":"private-rotated-secret"}',
            credential_update_minted_at=123,
        )
        fields = result.to_dict()
        checkpoint = store_credential_checkpoint(
            rc,
            "test:results",
            "test:tasks:claude",
            entry_id,
            task.id,
            fields,
        )
        fingerprint = handoff_payload_fingerprint(fields)
        result_id = rc.client.xadd(
            "test:results",
            {**fields, HANDOFF_FINGERPRINT_FIELD: fingerprint},
        )
        marker = handoff_marker_key("test:results", "test:tasks:claude", entry_id, task.id)
        rc.client.set(marker, f"{result_id}|{fingerprint}")

        assert manager._coordinate_reaped_vm(305, reason=REAP_REASON_ORPHAN_PEL) is True

        assert len(rc.client.xrevrange("test:results", count=10)) == 1
        assert get_pending_task(rc, "owner/repo", "pr", 42) == task.id
        assert rc.client.exists(checkpoint.key) == 0
        assert rc.client.exists(marker) == 1
        assert rc.client.ttl(marker) > 0
        assert rc.client.xpending("test:tasks:claude", CONSUMER_GROUP)["pending"] == 0

    def test_reaper_intent_without_checkpoint_never_publishes_generic_failure(
        self, fake_redis_client
    ):
        from orcest.shared.coordination import get_pending_task, set_pending_task
        from orcest.shared.credential_handoff import store_credential_checkpoint
        from orcest.shared.models import (
            CONSUMER_GROUP,
            ResultStatus,
            Task,
            TaskResult,
            TaskType,
        )

        rc = fake_redis_client
        manager, _proxmox = self._build(rc)
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
        assert set_pending_task(rc, "owner/repo", "pr", 42, task.id) is True
        rc.ensure_consumer_group("tasks:claude", CONSUMER_GROUP)
        entry_id = rc.xadd("tasks:claude", task.to_dict())
        assert rc.xreadgroup(
            group=CONSUMER_GROUP,
            consumer=worker_id,
            stream="tasks:claude",
            block_ms=None,
        )
        result = TaskResult(
            task_id=task.id,
            worker_id=worker_id,
            status=ResultStatus.COMPLETED,
            branch=task.branch,
            summary="rotation completed",
            duration_seconds=5,
            resource_type=task.resource_type,
            resource_id=task.resource_id,
            repo=task.repo,
            credential_update='{"refresh_token":"private-rotated-secret"}',
            credential_update_minted_at=123,
        )
        checkpoint = store_credential_checkpoint(
            rc,
            "test:results",
            "test:tasks:claude",
            entry_id,
            task.id,
            result.to_dict(),
        )
        rc.client.delete(checkpoint.key)

        assert manager._coordinate_reaped_vm(305, reason=REAP_REASON_ORPHAN_PEL) is False

        assert rc.client.xrevrange("test:results", count=10) == []
        assert get_pending_task(rc, "owner/repo", "pr", 42) == task.id
        assert rc.client.xpending("test:tasks:claude", CONSUMER_GROUP)["pending"] == 1

    def test_reaped_vm_publish_failure_does_not_clear_or_ack(self, fake_redis_client, monkeypatch):
        from orcest.shared.coordination import get_pending_task, set_pending_task
        from orcest.shared.models import CONSUMER_GROUP, Task, TaskType

        rc = fake_redis_client
        manager, _proxmox = self._build(rc)
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
        assert set_pending_task(rc, "owner/repo", "pr", 42, task.id) is True
        rc.ensure_consumer_group("tasks:claude", CONSUMER_GROUP)
        rc.xadd("tasks:claude", task.to_dict())
        claimed = rc.xreadgroup(
            group=CONSUMER_GROUP, consumer=worker_id, stream="tasks:claude", block_ms=None
        )
        assert len(claimed) == 1

        def fail_publish(*args, **kwargs):
            raise ConnectionError("Redis down")

        monkeypatch.setattr(rc, "xadd_capped_raw", fail_publish)

        manager._coordinate_reaped_vm(305, reason=REAP_REASON_ORPHAN_PEL)

        assert get_pending_task(rc, "owner/repo", "pr", 42) == task.id
        assert rc.xrevrange("results", count=10) == []
        consumers = {c["name"]: c for c in rc.xinfo_consumers("tasks:claude", CONSUMER_GROUP)}
        assert consumers[worker_id]["pending"] == 1

    def test_reaped_vm_does_not_publish_failure_when_result_already_exists(self, fake_redis_client):
        from orcest.shared.coordination import get_pending_task, set_pending_task
        from orcest.shared.models import CONSUMER_GROUP, ResultStatus, Task, TaskResult, TaskType

        rc = fake_redis_client
        manager, _proxmox = self._build(rc)
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
        assert set_pending_task(rc, "owner/repo", "pr", 42, task.id) is True
        rc.ensure_consumer_group("tasks:claude", CONSUMER_GROUP)
        rc.xadd("tasks:claude", task.to_dict())
        claimed = rc.xreadgroup(
            group=CONSUMER_GROUP, consumer=worker_id, stream="tasks:claude", block_ms=None
        )
        assert len(claimed) == 1
        rc.xadd(
            "results",
            TaskResult(
                task_id=task.id,
                worker_id=worker_id,
                status=ResultStatus.COMPLETED,
                branch=task.branch,
                summary="already published",
                duration_seconds=5,
                resource_type=task.resource_type,
                resource_id=task.resource_id,
                repo=task.repo,
            ).to_dict(),
        )

        recovered = manager._coordinate_reaped_vm(305, reason=REAP_REASON_ORPHAN_PEL)

        assert recovered is True
        assert get_pending_task(rc, "owner/repo", "pr", 42) == task.id
        results = [TaskResult.from_dict(fields) for _, fields in rc.xrevrange("results", count=10)]
        assert [result.status for result in results] == [ResultStatus.COMPLETED]
        consumers = {c["name"]: c for c in rc.xinfo_consumers("tasks:claude", CONSUMER_GROUP)}
        assert consumers.get(worker_id, {"pending": 0})["pending"] == 0

    def test_reaped_unkeyed_task_checks_default_prefixed_results_for_duplicates(
        self, fake_redis_client
    ):
        from orcest.shared.coordination import get_pending_task, set_pending_task
        from orcest.shared.models import CONSUMER_GROUP, ResultStatus, Task, TaskResult, TaskType

        rc = fake_redis_client
        manager, _proxmox = self._build(rc)
        worker_id = "orcest-worker-305"
        task = Task.create(
            task_type=TaskType.FIX_CI,
            repo="owner/repo",
            token="ghp_x",
            resource_type="pr",
            resource_id=42,
            prompt="fix",
            branch="fix-branch",
            key_prefix="",
        )
        assert set_pending_task(rc, "owner/repo", "pr", 42, task.id) is True
        rc.ensure_consumer_group("tasks:claude", CONSUMER_GROUP)
        rc.xadd("tasks:claude", task.to_dict())
        claimed = rc.xreadgroup(
            group=CONSUMER_GROUP, consumer=worker_id, stream="tasks:claude", block_ms=None
        )
        assert len(claimed) == 1
        rc.xadd(
            "results",
            TaskResult(
                task_id=task.id,
                worker_id=worker_id,
                status=ResultStatus.COMPLETED,
                branch=task.branch,
                summary="already published",
                duration_seconds=5,
                resource_type=task.resource_type,
                resource_id=task.resource_id,
                repo=task.repo,
            ).to_dict(),
        )

        recovered = manager._coordinate_reaped_vm(305, reason=REAP_REASON_ORPHAN_PEL)

        assert recovered is True
        assert get_pending_task(rc, "owner/repo", "pr", 42) == task.id
        results = [TaskResult.from_dict(fields) for _, fields in rc.xrevrange("results", count=10)]
        assert [result.status for result in results] == [ResultStatus.COMPLETED]
        assert rc.client.xrevrange("results", count=10) == []
        consumers = {c["name"]: c for c in rc.xinfo_consumers("tasks:claude", CONSUMER_GROUP)}
        assert consumers.get(worker_id, {"pending": 0})["pending"] == 0

    def test_reaped_vm_xpending_failure_does_not_delete_consumer(self):
        manager, _proxmox, redis = _make_manager()
        redis.xinfo_groups_raw.return_value = [{"name": "workers", "pending": 1}]
        redis.client.xpending_range.side_effect = ConnectionError("Redis down")

        recovered = manager._coordinate_reaped_vm(305, reason=REAP_REASON_ORPHAN_PEL)

        assert recovered is False
        redis.client.xgroup_delconsumer.assert_not_called()
        redis.xack_raw.assert_not_called()

    def test_reaped_vm_xrange_failure_does_not_ack_as_malformed(self):
        manager, _proxmox, redis = _make_manager()
        redis.xinfo_groups_raw.return_value = [{"name": "workers", "pending": 1}]
        redis.client.xpending_range.return_value = [{"message_id": "1-0"}]
        redis.client.xrange.side_effect = ConnectionError("Redis down")

        recovered = manager._coordinate_reaped_vm(305, reason=REAP_REASON_ORPHAN_PEL)

        assert recovered is False
        redis.xack_raw.assert_not_called()
        redis.client.xgroup_delconsumer.assert_not_called()

    def test_read_consumer_pending_pages_beyond_first_batch(self):
        from orcest.shared.models import CONSUMER_GROUP

        manager, _proxmox, redis = _make_manager()
        redis.xinfo_groups_raw.return_value = [{"name": CONSUMER_GROUP, "pending": 101}]
        redis.client.xpending_range.side_effect = [
            [{"message_id": f"{entry_id}-0"} for entry_id in range(1, 101)],
            [{"message_id": "101-0"}],
        ]
        redis.client.xrange.side_effect = lambda _stream, min, max: [
            (min, {"id": str(min), "repo": "owner/repo"})
        ]

        entries = manager._read_consumer_pending(
            "orcest:tasks:grok",
            "orcest-worker-305",
        )

        assert entries is not None
        assert len(entries) == 101
        assert redis.client.xpending_range.call_args_list[0].kwargs["min"] == "-"
        assert redis.client.xpending_range.call_args_list[1].kwargs["min"] == "(100-0"

    def test_health_check_preserves_state_when_recovery_incomplete(self, fake_redis_client):
        import time as _time

        rc = fake_redis_client
        manager, proxmox = self._build(rc)
        rc.hset("pool:active", "305", str(_time.time() - 99999))

        with patch.object(manager, "_coordinate_reaped_vm", return_value=False):
            manager._health_check()

        proxmox.stop_vm.assert_called_once_with(305)
        proxmox.destroy_vm.assert_not_called()
        assert rc.hgetall("pool:active").get("305") is not None

    def test_reap_with_no_pending_entry_still_destroys(self, fake_redis_client):
        # A VM over-duration but with an empty PEL (e.g. already ACKed) must
        # still be destroyed without error and without publishing a result.
        import time as _time

        from orcest.shared.models import CONSUMER_GROUP

        rc = fake_redis_client
        manager, proxmox = self._build(rc)
        rc.ensure_consumer_group("tasks:claude", CONSUMER_GROUP)
        rc.hset("pool:active", "305", str(_time.time() - 99999))

        manager._health_check()

        proxmox.destroy_vm.assert_called_once_with(305)
        assert rc.xrevrange("results", count=10) == []


# ── _reconcile_orphans ────────────────────────────────────────


class TestReconcileOrphans:
    def test_no_orphans(self):
        """When all Proxmox VMs are tracked in Redis, nothing is destroyed."""
        manager, proxmox, redis = _make_manager()
        proxmox.list_vms.return_value = [
            {"vmid": 300, "name": "orcest-worker-300", "status": "running"},
        ]
        redis._idle_set = {"300"}
        redis.hgetall.return_value = {}

        manager._reconcile_orphans()

        proxmox.list_vms.assert_called_once_with(name_prefix="orcest-worker-")
        proxmox.stop_vm.assert_not_called()
        proxmox.destroy_vm.assert_not_called()

    def test_orphan_destroyed(self):
        """VM in Proxmox but not in Redis should be destroyed."""
        manager, proxmox, redis = _make_manager()
        pipe = MagicMock()
        redis.pipeline.return_value = pipe
        proxmox.list_vms.return_value = [
            {"vmid": 300, "name": "orcest-worker-300", "status": "running"},
            {"vmid": 301, "name": "orcest-worker-301", "status": "stopped"},
        ]
        redis._idle_set = {"300"}  # Only 300 is tracked
        redis.hgetall.return_value = {}

        manager._reconcile_orphans()

        # VM 301 is orphaned, should be destroyed
        proxmox.stop_vm.assert_called_once_with(301)
        proxmox.destroy_vm.assert_called_once_with(301)

    def test_active_vm_not_orphaned(self):
        """VM tracked in pool:active should not be treated as orphaned."""
        manager, proxmox, redis = _make_manager()
        proxmox.list_vms.return_value = [
            {"vmid": 300, "name": "orcest-worker-300", "status": "running"},
        ]
        redis._idle_set = set()  # Not in idle
        redis.hgetall.return_value = {"300": "1000.0"}  # But in active

        manager._reconcile_orphans()

        proxmox.stop_vm.assert_not_called()
        proxmox.destroy_vm.assert_not_called()

    def test_template_not_destroyed(self):
        """The template VM itself should never be destroyed."""
        config = _make_config(template_vm_id=9000)
        manager, proxmox, redis = _make_manager(config=config)
        proxmox.list_vms.return_value = [
            {"vmid": 9000, "name": "orcest-worker-9000", "status": "stopped"},
        ]
        redis._idle_set = set()
        redis.hgetall.return_value = {}

        manager._reconcile_orphans()

        proxmox.stop_vm.assert_not_called()
        proxmox.destroy_vm.assert_not_called()

    def test_list_vms_failure_does_not_crash(self):
        """If listing VMs fails, orphan reconciliation is skipped gracefully."""
        manager, proxmox, redis = _make_manager()
        proxmox.list_vms.side_effect = RuntimeError("Proxmox unreachable")

        # Should not raise
        manager._reconcile_orphans()

    def test_corrupt_template_pointer_aborts_orphan_cleanup(self):
        manager, proxmox, redis = _make_manager()
        redis.get.return_value = "corrupt"
        proxmox.list_vms.return_value = [
            {"vmid": 300, "name": "orcest-worker-300", "status": "running"},
        ]

        manager._reconcile_orphans()

        proxmox.destroy_vm.assert_not_called()

    def test_destroy_failure_does_not_block_other_orphans(self):
        """If destroying one orphan fails, the rest are still processed."""
        manager, proxmox, redis = _make_manager()
        pipe = MagicMock()
        redis.pipeline.return_value = pipe
        proxmox.list_vms.return_value = [
            {"vmid": 300, "name": "orcest-worker-300", "status": "stopped"},
            {"vmid": 301, "name": "orcest-worker-301", "status": "stopped"},
        ]
        redis._idle_set = set()
        redis.hgetall.return_value = {}

        call_count = 0

        def pipeline_execute_side_effect():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ConnectionError("Redis down")
            return []

        pipe.execute.side_effect = pipeline_execute_side_effect

        # Should not raise
        manager._reconcile_orphans()

        # Both VMs should have destroy attempts
        assert proxmox.stop_vm.call_count == 2

    def test_no_proxmox_vms(self):
        """No VMs in Proxmox means nothing to reconcile."""
        manager, proxmox, redis = _make_manager()
        proxmox.list_vms.return_value = []
        redis._idle_set = set()
        redis.hgetall.return_value = {}

        manager._reconcile_orphans()

        proxmox.stop_vm.assert_not_called()

    def test_proxmox_template_flag_protects_vm(self):
        """Bug 1: a VM with Proxmox ``template: 1`` is never destroyed.

        Defence in depth: even if the VMID is outside ``template_vmid_range``
        and not the active pointer, a Proxmox-marked template (e.g. the
        result of a partial rebake where the Redis pointer swap failed)
        must survive orphan reconciliation.
        """
        # No range, no pointer; only the Proxmox template flag protects VM 9001.
        config = _make_config(template_vm_id=9000, vm_id_start=300)
        manager, proxmox, redis = _make_manager(config=config)
        proxmox.list_vms.return_value = [
            {
                "vmid": 9001,
                "name": "orcest-worker-template",
                "status": "stopped",
                "template": 1,
            },
        ]
        redis._idle_set = set()
        redis.hgetall.return_value = {}

        manager._reconcile_orphans()

        proxmox.stop_vm.assert_not_called()
        proxmox.destroy_vm.assert_not_called()

    def test_template_in_range_protected(self):
        """Bug 1: every VMID in ``template_vmid_range`` is skipped."""
        config = FleetConfig(
            proxmox=ProxmoxConfig(node="pve", storage="local-lvm"),
            pool=PoolConfig(
                template_vmid_range=[9000, 9009],
                template_vm_id=9000,
                vm_id_start=10000,
            ),
        )
        manager, proxmox, redis = _make_manager(config=config)
        proxmox.list_vms.return_value = [
            # Freshly baked, not yet template-converted, no Redis pointer.
            {"vmid": 9001, "name": "orcest-worker-template", "status": "running"},
        ]
        redis._idle_set = set()
        redis.hgetall.return_value = {}

        manager._reconcile_orphans()

        proxmox.stop_vm.assert_not_called()
        proxmox.destroy_vm.assert_not_called()

    def test_invalid_pointer_does_not_override_proxmox_template_protection(self):
        """An unauthorized pointer is rejected, but real templates remain protected."""
        config = _make_config(template_vm_id=9000, vm_id_start=300)
        manager, proxmox, redis = _make_manager(config=config)
        # Active pointer was swapped to 9001 (no range configured).
        redis.get.return_value = "9001"
        proxmox.list_vms.return_value = [
            {
                "vmid": 9001,
                "name": "orcest-worker-template",
                "status": "stopped",
                "template": True,
            },
        ]
        redis._idle_set = set()
        redis.hgetall.return_value = {}

        manager._reconcile_orphans()

        proxmox.stop_vm.assert_not_called()
        proxmox.destroy_vm.assert_not_called()

    def test_proxmox_template_boolean_flag(self):
        """Bug 1: ``template: True`` (boolean) also protects the VM."""
        config = _make_config(template_vm_id=9000, vm_id_start=300)
        manager, proxmox, redis = _make_manager(config=config)
        proxmox.list_vms.return_value = [
            {
                "vmid": 9001,
                "name": "orcest-worker-template",
                "status": "stopped",
                "template": True,
            },
        ]
        redis._idle_set = set()
        redis.hgetall.return_value = {}

        manager._reconcile_orphans()

        proxmox.destroy_vm.assert_not_called()


# ── _reconcile_stale_redis ──────────────────────────────────


class TestReconcileStaleRedis:
    def test_no_stale_entries(self):
        """When all Redis entries match Proxmox VMs, nothing is removed."""
        manager, proxmox, redis = _make_manager()
        proxmox.list_vms.return_value = [
            {"vmid": 300, "name": "orcest-worker-300", "status": "running"},
        ]
        redis._idle_set = {"300"}
        redis.hgetall.return_value = {}

        manager._reconcile_stale_redis()

        proxmox.list_vms.assert_called_once_with(name_prefix="orcest-worker-")
        redis.srem.assert_not_called()
        redis.hdel.assert_not_called()

    def test_stale_idle_entry_removed(self):
        """Idle entry with no matching Proxmox VM is removed."""
        manager, proxmox, redis = _make_manager()
        proxmox.list_vms.return_value = []  # No VMs in Proxmox
        redis._idle_set = {"300"}
        redis.hgetall.return_value = {}

        manager._reconcile_stale_redis()

        redis.srem.assert_called_once_with("pool:idle", "300")

    def test_stale_active_entry_removed(self):
        """Active entry with no matching Proxmox VM is removed."""
        manager, proxmox, redis = _make_manager()
        proxmox.list_vms.return_value = []  # No VMs in Proxmox
        redis._idle_set = set()
        redis.hgetall.return_value = {"301": "1000.0"}

        manager._reconcile_stale_redis()

        redis.hdel.assert_called_once_with("pool:active", "301")

    def test_stale_idle_entry_clears_reap_fence(self):
        """Idle stale cleanup releases in-memory per-VM reap telemetry."""
        manager, proxmox, redis = _make_manager()
        proxmox.list_vms.return_value = []
        redis._idle_set = {"300"}
        redis.hgetall.return_value = {}
        manager._reap_fences[300] = ReapFence(
            vm_id=300,
            reason="ceiling",
            killed_at_unix=100000.0,
            elapsed_at_kill_seconds=100.0,
        )

        manager._reconcile_stale_redis()

        assert 300 not in manager._reap_fences

    def test_stale_active_entry_clears_reap_fence(self):
        """Active stale cleanup releases in-memory per-VM reap telemetry."""
        manager, proxmox, redis = _make_manager()
        proxmox.list_vms.return_value = []
        redis._idle_set = set()
        redis.hgetall.return_value = {"301": "1000.0"}
        manager._reap_fences[301] = ReapFence(
            vm_id=301,
            reason="ceiling",
            killed_at_unix=100000.0,
            elapsed_at_kill_seconds=100.0,
        )

        manager._reconcile_stale_redis()

        assert 301 not in manager._reap_fences

    def test_stale_exact_slot_releases_process_local_allocation(self):
        """An externally deleted mixed-profile slot can be allocated again."""
        manager, proxmox, redis = _make_manager()
        proxmox.list_vms.return_value = []
        redis._idle_set = {"300"}
        redis.hgetall.return_value = {}
        manager._allocated_vmids.add(300)

        manager._reconcile_stale_redis()

        assert 300 not in manager._allocated_vmids
        assert manager._next_vm_id(preferred=300) == 300

    def test_mixed_stale_and_valid(self):
        """Only stale entries are removed; valid entries are left alone."""
        manager, proxmox, redis = _make_manager()
        proxmox.list_vms.return_value = [
            {"vmid": 300, "name": "orcest-worker-300", "status": "running"},
        ]
        redis._idle_set = {"300", "301"}  # 301 is stale
        redis.hgetall.return_value = {"302": "1000.0"}  # 302 is stale

        manager._reconcile_stale_redis()

        redis.srem.assert_called_once_with("pool:idle", "301")
        redis.hdel.assert_called_once_with("pool:active", "302")

    def test_list_vms_failure_does_not_crash(self):
        """If listing VMs fails, stale reconciliation is skipped gracefully."""
        manager, proxmox, redis = _make_manager()
        proxmox.list_vms.side_effect = RuntimeError("Proxmox unreachable")

        # Should not raise
        manager._reconcile_stale_redis()

    def test_template_vm_not_treated_as_stale(self):
        """Template VM ID in Redis should not be removed even if not in list_vms."""
        config = _make_config(template_vm_id=9000)
        manager, proxmox, redis = _make_manager(config=config)
        proxmox.list_vms.return_value = []
        redis._idle_set = {"9000"}
        redis.hgetall.return_value = {}

        manager._reconcile_stale_redis()

        redis.srem.assert_not_called()

    def test_non_integer_members_skipped(self):
        """Non-integer members in Redis sets are ignored (not crashed on)."""
        manager, proxmox, redis = _make_manager()
        proxmox.list_vms.return_value = []
        redis._idle_set = {"not-a-number", "300"}
        redis.hgetall.return_value = {"bad-id": "1000.0"}

        manager._reconcile_stale_redis()

        # Only the valid integer entry (300) is removed
        redis.srem.assert_called_once_with("pool:idle", "300")
        # bad-id is skipped (no hdel call for it)
        redis.hdel.assert_not_called()

    def test_srem_failure_does_not_block_remaining(self):
        """If srem fails for one entry, the rest are still processed."""
        manager, proxmox, redis = _make_manager()
        proxmox.list_vms.return_value = []  # No VMs in Proxmox
        # Use a list to get deterministic iteration order
        redis._idle_set = {"300", "301"}
        redis.hgetall.return_value = {}

        call_count = 0

        def srem_side_effect(key, member):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ConnectionError("Redis down")
            return 1

        redis.srem.side_effect = srem_side_effect

        # Should not raise
        manager._reconcile_stale_redis()

        # Both srem calls were attempted
        assert redis.srem.call_count == 2

    def test_hdel_failure_does_not_block_remaining(self):
        """If hdel fails for one entry, the rest are still processed."""
        manager, proxmox, redis = _make_manager()
        proxmox.list_vms.return_value = []  # No VMs in Proxmox
        redis._idle_set = set()
        redis.hgetall.return_value = {"300": "1000.0", "301": "1000.0"}

        call_count = 0

        def hdel_side_effect(key, member):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ConnectionError("Redis down")
            return 1

        redis.hdel.side_effect = hdel_side_effect

        # Should not raise
        manager._reconcile_stale_redis()

        # Both hdel calls were attempted
        assert redis.hdel.call_count == 2


# ── _sweep_orphan_pel (H3-conc) ──────────────────────────────


class TestSweepOrphanPel:
    def test_recovers_pending_consumer_for_dead_vmid(self):
        manager, proxmox, redis = _make_manager()
        redis.smembers.side_effect = None
        redis.smembers.return_value = set()  # nothing idle
        redis.hgetall.return_value = {}  # nothing active
        proxmox.list_vms.return_value = []  # not in Proxmox
        redis.xinfo_consumers_raw.return_value = [
            {"name": "orcest-worker-305", "pending": 2},
        ]
        with patch.object(manager, "_coordinate_reaped_vm", return_value=True) as recover:
            manager._sweep_orphan_pel()

        recover.assert_called_once_with(305, reason=REAP_REASON_ORPHAN_PEL)
        redis.delconsumer_raw.assert_not_called()

    def test_leaves_consumer_when_dead_vmid_recovery_is_incomplete(self):
        manager, proxmox, redis = _make_manager()
        redis.smembers.side_effect = None
        redis.smembers.return_value = set()
        redis.hgetall.return_value = {}
        proxmox.list_vms.return_value = []
        redis.xinfo_consumers_raw.return_value = [
            {"name": "orcest-worker-305", "pending": 2},
        ]

        with patch.object(manager, "_coordinate_reaped_vm", return_value=False) as recover:
            manager._sweep_orphan_pel()

        recover.assert_called_once_with(305, reason=REAP_REASON_ORPHAN_PEL)
        redis.delconsumer_raw.assert_not_called()

    def test_keeps_consumer_for_live_idle_vmid(self):
        manager, proxmox, redis = _make_manager()
        redis.smembers.side_effect = None
        redis.smembers.return_value = {"305"}  # 305 is idle -> live
        redis.hgetall.return_value = {}
        proxmox.list_vms.return_value = []
        redis.xinfo_consumers_raw.return_value = [
            {"name": "orcest-worker-305", "pending": 1},
        ]
        manager._sweep_orphan_pel()
        redis.delconsumer_raw.assert_not_called()

    def test_ignores_non_worker_consumer(self):
        manager, proxmox, redis = _make_manager()
        redis.smembers.side_effect = None
        redis.smembers.return_value = set()
        redis.hgetall.return_value = {}
        proxmox.list_vms.return_value = []
        redis.xinfo_consumers_raw.return_value = [
            {"name": "not-a-worker", "pending": 5},
        ]
        manager._sweep_orphan_pel()
        redis.delconsumer_raw.assert_not_called()

    def test_deletes_empty_consumer_for_dead_vmid(self):
        manager, proxmox, redis = _make_manager()
        redis.smembers.side_effect = None
        redis.smembers.return_value = set()
        redis.hgetall.return_value = {}
        proxmox.list_vms.return_value = []
        redis.xinfo_consumers_raw.return_value = [
            {"name": "orcest-worker-305", "pending": 0},
        ]
        manager._sweep_orphan_pel()
        assert redis.delconsumer_raw.call_count == 2  # two task streams
        redis.delconsumer_raw.assert_any_call("orcest:tasks:claude", "workers", "orcest-worker-305")

    def test_never_reaps_a_vmid_outside_the_worker_range(self):
        """D4: a legacy/standalone worker id is not the pool's to declare dead."""
        manager, proxmox, redis = _make_manager()
        redis.smembers.side_effect = None
        redis.smembers.return_value = set()
        redis.hgetall.return_value = {}
        proxmox.list_vms.return_value = []
        # Bare-integer legacy naming: parses to VMID 42, which the pool
        # (vm_id_start=300) never allocated.
        redis.xinfo_consumers_raw.return_value = [
            {"name": "42", "pending": 3},
        ]

        with patch.object(manager, "_coordinate_reaped_vm") as recover:
            manager._sweep_orphan_pel()

        recover.assert_not_called()
        redis.delconsumer_raw.assert_not_called()

    def test_never_reaps_a_worker_with_a_live_heartbeat(self):
        """D4: a live worker's task must not be re-enqueued underneath it."""
        manager, proxmox, redis = _make_manager()
        redis.smembers.side_effect = None
        redis.smembers.return_value = set()  # pool bookkeeping lost the VM
        redis.hgetall.return_value = {}
        proxmox.list_vms.return_value = []
        redis.exists.side_effect = lambda key: key == "workers:heartbeat:orcest-worker-305"
        redis.xinfo_consumers_raw.return_value = [
            {"name": "orcest-worker-305", "pending": 2},
        ]

        with patch.object(manager, "_coordinate_reaped_vm") as recover:
            manager._sweep_orphan_pel()

        recover.assert_not_called()
        redis.delconsumer_raw.assert_not_called()

    def test_never_reaps_when_heartbeat_state_is_unreadable(self):
        manager, proxmox, redis = _make_manager()
        redis.smembers.side_effect = None
        redis.smembers.return_value = set()
        redis.hgetall.return_value = {}
        proxmox.list_vms.return_value = []
        redis.exists.side_effect = RuntimeError("redis blip")
        redis.xinfo_consumers_raw.return_value = [
            {"name": "orcest-worker-305", "pending": 2},
        ]

        with patch.object(manager, "_coordinate_reaped_vm") as recover:
            manager._sweep_orphan_pel()

        recover.assert_not_called()
        redis.delconsumer_raw.assert_not_called()

    def test_skips_sweep_when_proxmox_listing_fails(self):
        manager, proxmox, redis = _make_manager()
        redis.smembers.side_effect = None
        redis.smembers.return_value = set()
        redis.hgetall.return_value = {}
        proxmox.list_vms.side_effect = RuntimeError("pve unavailable")
        redis.xinfo_consumers_raw.return_value = [
            {"name": "orcest-worker-305", "pending": 2},
        ]

        manager._sweep_orphan_pel()

        redis.xinfo_consumers_raw.assert_not_called()
        redis.delconsumer_raw.assert_not_called()


# ── reconcile ────────────────────────────────────────────────


class TestReconcile:
    def test_calls_all_phases(self):
        manager, proxmox, redis = _make_manager()

        with (
            patch.object(manager, "_check_done_workers", return_value=[]) as mock_done,
            patch.object(manager, "_detect_active_workers") as mock_detect,
            patch.object(
                manager, "_reconcile_ambiguous_clones", return_value=set()
            ) as mock_ambiguous,
            patch.object(manager, "_retry_provisioning_cleanups", return_value=set()) as mock_retry,
            patch.object(manager, "_fill_pool") as mock_fill,
            patch.object(manager, "_health_check") as mock_health,
            patch.object(manager, "_reconcile_stale_redis") as mock_stale,
            patch.object(manager, "_sweep_orphan_pel") as mock_sweep,
        ):
            manager.reconcile()

        mock_done.assert_called_once()
        mock_detect.assert_called_once()
        mock_ambiguous.assert_called_once()
        mock_retry.assert_called_once()
        mock_fill.assert_called_once()
        mock_health.assert_called_once()
        mock_stale.assert_called_once()
        mock_sweep.assert_called_once()

    def test_error_does_not_crash(self):
        manager, proxmox, redis = _make_manager()

        with patch.object(manager, "_check_done_workers", side_effect=Exception("Redis down")):
            # Should not raise
            manager.reconcile()

    def test_phases_called_in_order(self):
        manager, proxmox, redis = _make_manager()
        call_order: list[str] = []

        with (
            patch.object(
                manager,
                "_check_done_workers",
                side_effect=lambda: call_order.append("check_done") or [],
            ),
            patch.object(
                manager,
                "_detect_active_workers",
                side_effect=lambda: call_order.append("detect_active"),
            ),
            patch.object(
                manager,
                "_reconcile_ambiguous_clones",
                side_effect=lambda: call_order.append("reconcile_ambiguous") or set(),
            ),
            patch.object(
                manager,
                "_retry_provisioning_cleanups",
                side_effect=lambda: call_order.append("retry_provisioning") or set(),
            ),
            patch.object(
                manager,
                "_fill_pool",
                side_effect=lambda **_kwargs: call_order.append("fill_pool"),
            ),
            patch.object(
                manager,
                "_health_check",
                side_effect=lambda: call_order.append("health_check"),
            ),
            patch.object(
                manager,
                "_reconcile_stale_redis",
                side_effect=lambda: call_order.append("reconcile_stale"),
            ),
            patch.object(
                manager,
                "_sweep_orphan_pel",
                side_effect=lambda: call_order.append("sweep_orphan_pel"),
            ),
        ):
            manager.reconcile()

        assert call_order == [
            "check_done",
            "detect_active",
            "reconcile_ambiguous",
            "retry_provisioning",
            "fill_pool",
            "health_check",
            "reconcile_stale",
            "sweep_orphan_pel",
        ]

    def test_error_is_logged(self):
        manager, proxmox, redis = _make_manager()

        with (
            patch.object(manager, "_check_done_workers", side_effect=RuntimeError("boom")),
            patch("orcest.fleet.pool_manager.logger") as mock_logger,
        ):
            manager.reconcile()
            mock_logger.error.assert_called_once()
            assert "Reconciliation pass failed" in mock_logger.error.call_args[0][0]


# ── run ──────────────────────────────────────────────────────


class TestRun:
    def test_calls_reconcile_in_loop(self):
        manager, proxmox, redis = _make_manager()
        call_count = 0

        def mock_reconcile():
            nonlocal call_count
            call_count += 1
            if call_count >= 3:
                raise KeyboardInterrupt

        with (
            patch.object(manager, "reconcile", side_effect=mock_reconcile),
            patch.object(manager, "_reconcile_orphans"),
        ):
            manager.run(interval=0.01)

        assert call_count == 3

    def test_sigterm_stops_loop(self):
        import os
        import signal as sig

        manager, proxmox, redis = _make_manager()
        call_count = 0

        def mock_reconcile():
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                os.kill(os.getpid(), sig.SIGTERM)

        with (
            patch.object(manager, "reconcile", side_effect=mock_reconcile),
            patch.object(manager, "_reconcile_orphans"),
        ):
            manager.run(interval=0.01)

        assert call_count >= 2

    def test_calls_reconcile_orphans_on_startup(self):
        """run() should call _reconcile_orphans once before the main loop."""
        manager, proxmox, redis = _make_manager()

        with (
            patch.object(manager, "_reconcile_orphans") as mock_orphans,
            patch.object(manager, "reconcile", side_effect=KeyboardInterrupt),
        ):
            manager.run(interval=0.01)

        mock_orphans.assert_called_once()


# ── Integration-style test ───────────────────────────────────


class TestFullCycle:
    """Tests that exercise multiple phases of the reconciliation loop together."""

    def test_done_worker_replaced(self):
        """A done worker is destroyed and a new one is cloned to replace it."""
        config = _make_config(pool_size=2)
        proxmox = _make_proxmox()
        redis = _make_redis()

        manager = PoolManager(config=config, proxmox=proxmox, redis=redis)

        # Pre-populate idle set with VM 301 (simulating an existing idle worker)
        redis._idle_set.add("301")

        # One done worker, one idle -> total currently 1, deficit 1
        redis.scan_iter.return_value = ["pool:done:orcest-worker-300"]
        redis.scard.return_value = 1  # 1 idle remaining after destroy
        redis.hlen.return_value = 0
        redis.hgetall.return_value = {}
        redis.xinfo_groups_raw.return_value = []

        pipe = MagicMock()
        redis.pipeline.return_value = pipe
        proxmox.get_vm_ip.return_value = "10.20.0.51"

        manager.reconcile()

        # Done worker destroyed
        proxmox.stop_vm.assert_any_call(300)
        proxmox.destroy_vm.assert_any_call(300)
        redis.delete.assert_any_call("pool:done:orcest-worker-300")

        # New worker cloned to fill deficit — 300 was destroyed and is free,
        # 301 is in idle set, so next available is 300 (reused after destroy)
        proxmox.clone_vm.assert_called_once_with(
            template_id=9000,
            new_id=300,
            name="orcest-worker-300",
            linked=True,
        )
        # Deterministic MAC assigned before boot
        proxmox.set_vm_network.assert_called_once_with(
            300,
            mac="02:4F:52:00:01:2C",
        )


# ── _next_vm_id upper bound ──────────────────────────────────


class TestNextVmIdUpperBound:
    def test_no_upper_bound_returns_start(self):
        """Without vm_id_end, allocates start ID when none are taken."""
        config = FleetConfig(
            proxmox=ProxmoxConfig(node="pve", storage="local-lvm"),
            pool=PoolConfig(vm_id_start=300, vm_id_end=0),
        )
        manager, proxmox, redis = _make_manager(config=config)
        proxmox.list_vms.return_value = []

        assert manager._next_vm_id() == 300

    def test_no_upper_bound_increments_past_existing(self):
        """Without vm_id_end, increments freely past existing IDs."""
        config = FleetConfig(
            proxmox=ProxmoxConfig(node="pve", storage="local-lvm"),
            pool=PoolConfig(vm_id_start=300, vm_id_end=0),
        )
        manager, proxmox, redis = _make_manager(config=config)
        proxmox.list_vms.return_value = [{"vmid": 300}, {"vmid": 301}, {"vmid": 302}]

        assert manager._next_vm_id() == 303

    def test_upper_bound_allows_valid_id(self):
        """With vm_id_end set, returns a valid ID within the range."""
        config = FleetConfig(
            proxmox=ProxmoxConfig(node="pve", storage="local-lvm"),
            pool=PoolConfig(vm_id_start=300, vm_id_end=305),
        )
        manager, proxmox, redis = _make_manager(config=config)
        proxmox.list_vms.return_value = [{"vmid": 300}]

        assert manager._next_vm_id() == 301

    def test_upper_bound_raises_when_exhausted(self):
        """Raises RuntimeError when all IDs in the range are taken."""
        config = FleetConfig(
            proxmox=ProxmoxConfig(node="pve", storage="local-lvm"),
            pool=PoolConfig(vm_id_start=300, vm_id_end=302),
        )
        manager, proxmox, redis = _make_manager(config=config)
        proxmox.list_vms.return_value = [
            {"vmid": 300},
            {"vmid": 301},
            {"vmid": 302},
        ]

        with pytest.raises(RuntimeError, match="VM ID pool exhausted"):
            manager._next_vm_id()

    def test_upper_bound_raises_when_start_is_after_end(self):
        """Raises a configuration error when the configured range is invalid."""
        config = FleetConfig(
            proxmox=ProxmoxConfig(node="pve", storage="local-lvm"),
            pool=PoolConfig(vm_id_start=303, vm_id_end=302),
        )
        manager, proxmox, redis = _make_manager(config=config)

        with pytest.raises(
            RuntimeError,
            match=(
                r"Invalid VM ID pool range: vm_id_start \(303\) is greater than "
                r"vm_id_end \(302\)"
            ),
        ):
            manager._next_vm_id()
        proxmox.list_vms.assert_not_called()

    def test_upper_bound_raises_includes_range_in_message(self):
        """RuntimeError message includes the configured range."""
        config = FleetConfig(
            proxmox=ProxmoxConfig(node="pve", storage="local-lvm"),
            pool=PoolConfig(vm_id_start=300, vm_id_end=300),
        )
        manager, proxmox, redis = _make_manager(config=config)
        proxmox.list_vms.return_value = [{"vmid": 300}]

        with pytest.raises(RuntimeError, match="300-300"):
            manager._next_vm_id()
