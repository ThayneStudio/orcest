"""Tests for orcest.fleet.pool_manager."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from orcest.fleet.config import FleetConfig, PoolConfig, ProxmoxConfig
from orcest.fleet.pool_manager import PoolManager

pytestmark = pytest.mark.unit


# ── Fixtures ─────────────────────────────────────────────────


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

    def _sadd(key: str, value: str) -> int:
        if key == "pool:idle":
            mock._idle_set.add(value)
        return 1

    def _smembers(key: str) -> set[str]:
        if key == "pool:idle":
            return set(mock._idle_set)
        return set()

    mock.scan_iter.return_value = []
    mock.hgetall.return_value = {}
    mock.smembers.side_effect = _smembers
    mock.scard.return_value = 0
    mock.hlen.return_value = 0
    mock.xinfo_groups.return_value = []
    mock.xinfo_consumers.return_value = []
    mock.sadd.side_effect = _sadd
    mock.pipeline.return_value = MagicMock()
    return mock


def _make_proxmox() -> MagicMock:
    """Build a mock ProxmoxClient."""
    mock = MagicMock()
    mock.get_vm_ip.return_value = "10.20.0.50"
    mock.get_vm_status.return_value = "stopped"
    mock.list_vms.return_value = []
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

        pipe.srem.assert_called_once_with("pool:idle", "300")
        pipe.hdel.assert_called_once_with("pool:active", "300")
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

        # Both VMs are reported as destroyed (pipeline failure in _destroy_vm
        # is caught internally and does not prevent the VM from being counted)
        assert sorted(destroyed) == [300, 301]
        # Both done keys should be deleted regardless of destroy success
        redis.delete.assert_any_call("pool:done:orcest-worker-300")
        redis.delete.assert_any_call("pool:done:orcest-worker-301")


# ── _destroy_vm ──────────────────────────────────────────────


class TestDestroyVm:
    def test_stops_and_destroys(self):
        manager, proxmox, redis = _make_manager()
        pipe = MagicMock()
        redis.pipeline.return_value = pipe

        manager._destroy_vm(300)

        proxmox.stop_vm.assert_called_once_with(300)
        proxmox.destroy_vm.assert_called_once_with(300)
        pipe.srem.assert_called_once_with("pool:idle", "300")
        pipe.hdel.assert_called_once_with("pool:active", "300")
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

    def test_cleans_redis_even_if_destroy_fails(self):
        manager, proxmox, redis = _make_manager()
        pipe = MagicMock()
        redis.pipeline.return_value = pipe
        proxmox.destroy_vm.side_effect = Exception("API error")

        manager._destroy_vm(300)

        # Redis cleanup still happens
        pipe.srem.assert_called_once()
        pipe.hdel.assert_called_once()
        pipe.execute.assert_called_once()

    def test_cleans_redis_even_if_both_proxmox_calls_fail(self):
        manager, proxmox, redis = _make_manager()
        pipe = MagicMock()
        redis.pipeline.return_value = pipe
        proxmox.stop_vm.side_effect = Exception("stop failed")
        proxmox.destroy_vm.side_effect = Exception("destroy failed")

        manager._destroy_vm(300)

        pipe.srem.assert_called_once_with("pool:idle", "300")
        pipe.hdel.assert_called_once_with("pool:active", "300")
        pipe.execute.assert_called_once()

    def test_redis_pipeline_failure_does_not_raise(self):
        """If the Redis pipeline itself fails, _destroy_vm should not raise."""
        manager, proxmox, redis = _make_manager()
        pipe = MagicMock()
        redis.pipeline.return_value = pipe
        pipe.execute.side_effect = ConnectionError("Redis connection lost")

        # Should not raise
        manager._destroy_vm(300)

        # Proxmox calls still happen
        proxmox.stop_vm.assert_called_once_with(300)
        proxmox.destroy_vm.assert_called_once_with(300)
        # Pipeline was attempted
        pipe.execute.assert_called_once()


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
        redis.sadd.assert_called_once_with("pool:idle", "300")

    def test_no_template_configured(self):
        config = _make_config(template_vm_id=0)
        manager, proxmox, redis = _make_manager(config=config)

        vm_id = manager._clone_and_boot()

        assert vm_id is None
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

    def test_clone_failure_returns_none_and_cleans_up(self):
        """Clone failure returns None and attempts best-effort cleanup."""
        manager, proxmox, redis = _make_manager()
        proxmox.clone_vm.side_effect = RuntimeError("clone failed")

        vm_id = manager._clone_and_boot()

        assert vm_id is None
        # Best-effort cleanup: attempts to destroy the potentially-created VM
        proxmox.destroy_vm.assert_called_once_with(300)

    def test_clone_failure_cleanup_ignores_destroy_error(self):
        """If clone fails and cleanup destroy also fails, no exception escapes."""
        manager, proxmox, redis = _make_manager()
        proxmox.clone_vm.side_effect = RuntimeError("clone failed")
        proxmox.destroy_vm.side_effect = RuntimeError("VM does not exist")

        vm_id = manager._clone_and_boot()

        assert vm_id is None

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
        redis.sadd.assert_not_called()

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

        redis.sadd.assert_not_called()

    def test_sadd_failure_destroys_vm(self):
        """If sadd to idle pool fails, the VM should be destroyed to avoid orphan."""
        manager, proxmox, redis = _make_manager()
        proxmox.get_vm_ip.return_value = "10.20.0.50"
        redis.sadd.side_effect = ConnectionError("Redis down")
        pipe = MagicMock()
        redis.pipeline.return_value = pipe

        vm_id = manager._clone_and_boot()

        assert vm_id is None
        # VM was cloned and booted successfully, but sadd failed
        proxmox.clone_vm.assert_called_once()
        proxmox.start_vm.assert_called_once_with(300)
        # _destroy_vm should have been called to clean up
        proxmox.stop_vm.assert_called_once_with(300)
        proxmox.destroy_vm.assert_called_once_with(300)


# ── _detect_active_workers ───────────────────────────────────


class TestDetectActiveWorkers:
    def test_no_idle_workers_noop(self):
        manager, proxmox, redis = _make_manager()
        redis._idle_set = set()

        manager._detect_active_workers()

        redis.smembers.assert_called_once_with("pool:idle")
        redis.xinfo_groups.assert_not_called()

    def test_idle_worker_becomes_active(self):
        manager, proxmox, redis = _make_manager()
        redis._idle_set = {"300"}
        redis.xinfo_groups.return_value = [
            {"name": "workers", "pending": 1},
        ]
        redis.xinfo_consumers.return_value = [
            {"name": "orcest-worker-300", "pending": 1},
        ]
        pipe = MagicMock()
        redis.pipeline.return_value = pipe

        with patch("orcest.fleet.pool_manager.time") as mock_time:
            mock_time.monotonic.return_value = 1000.0
            manager._detect_active_workers()

        pipe.srem.assert_called_once_with("pool:idle", "300")
        pipe.hset.assert_called_once_with("pool:active", "300", "1000.0")
        pipe.execute.assert_called_once()

    def test_idle_worker_stays_idle(self):
        manager, proxmox, redis = _make_manager()
        redis._idle_set = {"300"}
        redis.xinfo_groups.return_value = [
            {"name": "workers", "pending": 0},
        ]
        redis.xinfo_consumers.return_value = [
            {"name": "orcest-worker-300", "pending": 0},
        ]

        manager._detect_active_workers()

        # No pipeline calls for moving to active
        redis.pipeline.assert_not_called()

    def test_handles_xinfo_groups_error(self):
        manager, proxmox, redis = _make_manager()
        redis._idle_set = {"300"}
        redis.xinfo_groups.side_effect = Exception("stream not found")

        # Should not raise
        manager._detect_active_workers()

    def test_handles_xinfo_consumers_error(self):
        manager, proxmox, redis = _make_manager()
        redis._idle_set = {"300"}
        redis.xinfo_groups.return_value = [
            {"name": "workers", "pending": 1},
        ]
        redis.xinfo_consumers.side_effect = Exception("group not found")

        # Should not raise
        manager._detect_active_workers()

    def test_non_integer_idle_member_skipped(self):
        manager, proxmox, redis = _make_manager()
        redis._idle_set = {"not-a-number", "300"}
        redis.xinfo_groups.return_value = [
            {"name": "workers", "pending": 1},
        ]
        redis.xinfo_consumers.return_value = [
            {"name": "orcest-worker-300", "pending": 1},
        ]
        pipe = MagicMock()
        redis.pipeline.return_value = pipe

        with patch("orcest.fleet.pool_manager.time") as mock_time:
            mock_time.monotonic.return_value = 1000.0
            manager._detect_active_workers()

        # Only VM 300 should be moved, the invalid member skipped
        pipe.srem.assert_called_once_with("pool:idle", "300")

    def test_pipeline_failure_does_not_crash(self):
        """If the Redis pipeline fails, the error is logged but does not propagate."""
        manager, proxmox, redis = _make_manager()
        redis._idle_set = {"300"}
        redis.xinfo_groups.return_value = [
            {"name": "workers", "pending": 1},
        ]
        redis.xinfo_consumers.return_value = [
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

    def test_clone_failure_continues(self):
        """One clone failure should not prevent subsequent clones."""
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

        # Called 3 times, first failed, last 2 proceeded
        assert proxmox.clone_vm.call_count == 3
        assert proxmox.start_vm.call_count == 2
        # Best-effort cleanup: failed clone triggers destroy_vm for VM 300
        proxmox.destroy_vm.assert_called_once_with(300)

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
        mock_time.monotonic.return_value = 10000.0
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
        mock_time.monotonic.side_effect = [10000.0, 0, 100]
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
        mock_time.monotonic.side_effect = [10000.0, 0, 100]
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
        mock_time.monotonic.side_effect = [10000.0, 0, 100]  # health_check now + _destroy_vm
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
        mock_time.monotonic.return_value = 10000.0

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
        mock_time.monotonic.side_effect = [10000.0, 0, 100, 0, 100]

        # Should not raise
        manager._health_check()

        # Both VMs should have been attempted
        assert proxmox.stop_vm.call_count == 2


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


# ── reconcile ────────────────────────────────────────────────


class TestReconcile:
    def test_calls_all_phases(self):
        manager, proxmox, redis = _make_manager()

        with (
            patch.object(manager, "_check_done_workers", return_value=[]) as mock_done,
            patch.object(manager, "_detect_active_workers") as mock_detect,
            patch.object(manager, "_fill_pool") as mock_fill,
            patch.object(manager, "_health_check") as mock_health,
            patch.object(manager, "_reconcile_stale_redis") as mock_stale,
        ):
            manager.reconcile()

        mock_done.assert_called_once()
        mock_detect.assert_called_once()
        mock_fill.assert_called_once()
        mock_health.assert_called_once()
        mock_stale.assert_called_once()

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
                "_fill_pool",
                side_effect=lambda: call_order.append("fill_pool"),
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
        ):
            manager.reconcile()

        assert call_order == [
            "check_done",
            "detect_active",
            "fill_pool",
            "health_check",
            "reconcile_stale",
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
        redis.xinfo_groups.return_value = []

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
