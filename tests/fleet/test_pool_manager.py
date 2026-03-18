"""Tests for orcest.fleet.pool_manager."""

from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

import pytest

from orcest.fleet.config import FleetConfig, PoolConfig, ProxmoxConfig
from orcest.fleet.pool_manager import PoolManager, PoolState

pytestmark = pytest.mark.unit


# ── Fixtures ─────────────────────────────────────────────────


def _make_config(
    pool_size: int = 4,
    template_vm_id: int = 9000,
    storage: str = "ssd-pool",
    max_task_duration: int = 3600,
) -> FleetConfig:
    """Build a FleetConfig with pool settings for testing."""
    return FleetConfig(
        proxmox=ProxmoxConfig(node="pve", storage="local-lvm"),
        pool=PoolConfig(
            size=pool_size,
            template_vm_id=template_vm_id,
            storage=storage,
            max_task_duration=max_task_duration,
        ),
    )


def _make_redis() -> MagicMock:
    """Build a mock RedisClient with the needed interface."""
    mock = MagicMock()
    mock._prefixed = MagicMock(side_effect=lambda k: f"orcest:{k}")
    # Default: empty pool state
    mock.scan_iter.return_value = []
    mock.hgetall.return_value = {}
    mock.client.smembers.return_value = set()
    mock.client.scard.return_value = 0
    mock.client.hlen.return_value = 0
    mock.xinfo_groups.return_value = []
    mock.client.xinfo_consumers.return_value = []
    mock.client.pipeline.return_value = MagicMock()
    mock.client.sadd.return_value = 1
    return mock


def _make_proxmox() -> MagicMock:
    """Build a mock ProxmoxClient."""
    mock = MagicMock()
    mock.next_free_vmid.return_value = 300
    mock.get_vm_ip.return_value = "10.20.0.50"
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


# ── PoolState ────────────────────────────────────────────────


class TestPoolState:
    def test_defaults(self):
        state = PoolState()
        assert state.idle == set()
        assert state.active == {}
        assert state.target_size == 4

    def test_custom_values(self):
        state = PoolState(idle={100, 200}, active={300: 1234.5}, target_size=6)
        assert state.idle == {100, 200}
        assert state.active == {300: 1234.5}
        assert state.target_size == 6


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
        redis.client.pipeline.return_value = pipe

        manager._check_done_workers()

        pipe.srem.assert_called_once_with("orcest:pool:idle", "300")
        pipe.hdel.assert_called_once_with("orcest:pool:active", "300")
        pipe.execute.assert_called_once()


# ── _destroy_vm ──────────────────────────────────────────────


class TestDestroyVm:
    def test_stops_and_destroys(self):
        manager, proxmox, redis = _make_manager()
        pipe = MagicMock()
        redis.client.pipeline.return_value = pipe

        manager._destroy_vm(300)

        proxmox.stop_vm.assert_called_once_with(300)
        proxmox.destroy_vm.assert_called_once_with(300)
        pipe.srem.assert_called_once_with("orcest:pool:idle", "300")
        pipe.hdel.assert_called_once_with("orcest:pool:active", "300")
        pipe.execute.assert_called_once()

    def test_cleans_redis_even_if_stop_fails(self):
        manager, proxmox, redis = _make_manager()
        pipe = MagicMock()
        redis.client.pipeline.return_value = pipe
        proxmox.stop_vm.side_effect = Exception("already stopped")

        manager._destroy_vm(300)

        # destroy still called
        proxmox.destroy_vm.assert_called_once_with(300)
        pipe.execute.assert_called_once()

    def test_cleans_redis_even_if_destroy_fails(self):
        manager, proxmox, redis = _make_manager()
        pipe = MagicMock()
        redis.client.pipeline.return_value = pipe
        proxmox.destroy_vm.side_effect = Exception("API error")

        manager._destroy_vm(300)

        # Redis cleanup still happens
        pipe.srem.assert_called_once()
        pipe.hdel.assert_called_once()
        pipe.execute.assert_called_once()

    def test_cleans_redis_even_if_both_proxmox_calls_fail(self):
        manager, proxmox, redis = _make_manager()
        pipe = MagicMock()
        redis.client.pipeline.return_value = pipe
        proxmox.stop_vm.side_effect = Exception("stop failed")
        proxmox.destroy_vm.side_effect = Exception("destroy failed")

        manager._destroy_vm(300)

        pipe.srem.assert_called_once_with("orcest:pool:idle", "300")
        pipe.hdel.assert_called_once_with("orcest:pool:active", "300")
        pipe.execute.assert_called_once()


# ── _clone_and_boot ──────────────────────────────────────────


class TestCloneAndBoot:
    def test_success(self):
        manager, proxmox, redis = _make_manager()
        proxmox.next_free_vmid.return_value = 300
        proxmox.get_vm_ip.return_value = "10.20.0.50"

        vm_id = manager._clone_and_boot()

        assert vm_id == 300
        proxmox.clone_vm.assert_called_once_with(
            template_id=9000,
            new_id=300,
            name="orcest-worker-300",
            storage="ssd-pool",
            linked=True,
        )
        proxmox.start_vm.assert_called_once_with(300)
        proxmox.get_vm_ip.assert_called_once_with(300)
        redis.client.sadd.assert_called_once_with("orcest:pool:idle", "300")

    def test_no_template_configured(self):
        config = _make_config(template_vm_id=0)
        manager, proxmox, redis = _make_manager(config=config)

        vm_id = manager._clone_and_boot()

        assert vm_id is None
        proxmox.clone_vm.assert_not_called()

    def test_vm_no_ip_destroys(self):
        manager, proxmox, redis = _make_manager()
        proxmox.next_free_vmid.return_value = 300
        proxmox.get_vm_ip.return_value = None
        pipe = MagicMock()
        redis.client.pipeline.return_value = pipe

        vm_id = manager._clone_and_boot()

        assert vm_id is None
        # VM should be destroyed since it didn't get an IP
        proxmox.stop_vm.assert_called_once_with(300)
        proxmox.destroy_vm.assert_called_once_with(300)

    def test_clone_failure_propagates(self):
        manager, proxmox, redis = _make_manager()
        proxmox.clone_vm.side_effect = RuntimeError("clone failed")

        with pytest.raises(RuntimeError, match="clone failed"):
            manager._clone_and_boot()

    def test_uses_linked_clone(self):
        manager, proxmox, redis = _make_manager()
        proxmox.next_free_vmid.return_value = 300
        proxmox.get_vm_ip.return_value = "10.20.0.50"

        manager._clone_and_boot()

        _, kwargs = proxmox.clone_vm.call_args
        assert kwargs["linked"] is True

    def test_correct_vm_naming(self):
        manager, proxmox, redis = _make_manager()
        proxmox.next_free_vmid.return_value = 42
        proxmox.get_vm_ip.return_value = "10.20.0.50"

        manager._clone_and_boot()

        _, kwargs = proxmox.clone_vm.call_args
        assert kwargs["name"] == "orcest-worker-42"

    def test_no_ip_does_not_add_to_idle_pool(self):
        """When get_vm_ip returns None, the VM should not be in the idle set."""
        manager, proxmox, redis = _make_manager()
        proxmox.next_free_vmid.return_value = 300
        proxmox.get_vm_ip.return_value = None
        pipe = MagicMock()
        redis.client.pipeline.return_value = pipe

        manager._clone_and_boot()

        redis.client.sadd.assert_not_called()


# ── _detect_active_workers ───────────────────────────────────


class TestDetectActiveWorkers:
    def test_no_idle_workers_noop(self):
        manager, proxmox, redis = _make_manager()
        redis.client.smembers.return_value = set()

        manager._detect_active_workers()

        redis.xinfo_groups.assert_not_called()

    def test_idle_worker_becomes_active(self):
        manager, proxmox, redis = _make_manager()
        redis.client.smembers.return_value = {"300"}
        redis.xinfo_groups.return_value = [
            {"name": "workers", "pending": 1},
        ]
        redis.client.xinfo_consumers.return_value = [
            {"name": "orcest-worker-300", "pending": 1},
        ]
        pipe = MagicMock()
        redis.client.pipeline.return_value = pipe

        with patch("orcest.fleet.pool_manager.time") as mock_time:
            mock_time.time.return_value = 1000.0
            manager._detect_active_workers()

        pipe.srem.assert_called_once_with("orcest:pool:idle", "300")
        pipe.hset.assert_called_once()
        pipe.execute.assert_called_once()

    def test_idle_worker_stays_idle(self):
        manager, proxmox, redis = _make_manager()
        redis.client.smembers.return_value = {"300"}
        redis.xinfo_groups.return_value = [
            {"name": "workers", "pending": 0},
        ]
        redis.client.xinfo_consumers.return_value = [
            {"name": "orcest-worker-300", "pending": 0},
        ]

        manager._detect_active_workers()

        # No pipeline calls for moving to active
        redis.client.pipeline.assert_not_called()

    def test_handles_xinfo_groups_error(self):
        manager, proxmox, redis = _make_manager()
        redis.client.smembers.return_value = {"300"}
        redis.xinfo_groups.side_effect = Exception("stream not found")

        # Should not raise
        manager._detect_active_workers()

    def test_handles_xinfo_consumers_error(self):
        manager, proxmox, redis = _make_manager()
        redis.client.smembers.return_value = {"300"}
        redis.xinfo_groups.return_value = [
            {"name": "workers", "pending": 1},
        ]
        redis.client.xinfo_consumers.side_effect = Exception("group not found")

        # Should not raise
        manager._detect_active_workers()

    def test_non_integer_idle_member_skipped(self):
        manager, proxmox, redis = _make_manager()
        redis.client.smembers.return_value = {"not-a-number", "300"}
        redis.xinfo_groups.return_value = [
            {"name": "workers", "pending": 1},
        ]
        redis.client.xinfo_consumers.return_value = [
            {"name": "orcest-worker-300", "pending": 1},
        ]
        pipe = MagicMock()
        redis.client.pipeline.return_value = pipe

        with patch("orcest.fleet.pool_manager.time") as mock_time:
            mock_time.time.return_value = 1000.0
            manager._detect_active_workers()

        # Only VM 300 should be moved, the invalid member skipped
        pipe.srem.assert_called_once_with("orcest:pool:idle", "300")


# ── _fill_pool ───────────────────────────────────────────────


class TestFillPool:
    def test_fills_deficit(self):
        config = _make_config(pool_size=3)
        manager, proxmox, redis = _make_manager(config=config)
        redis.client.scard.return_value = 1
        redis.client.hlen.return_value = 0
        proxmox.next_free_vmid.side_effect = [300, 301]
        proxmox.get_vm_ip.return_value = "10.20.0.50"

        manager._fill_pool()

        assert proxmox.clone_vm.call_count == 2

    def test_no_deficit(self):
        config = _make_config(pool_size=2)
        manager, proxmox, redis = _make_manager(config=config)
        redis.client.scard.return_value = 1
        redis.client.hlen.return_value = 1

        manager._fill_pool()

        proxmox.clone_vm.assert_not_called()

    def test_over_target(self):
        config = _make_config(pool_size=2)
        manager, proxmox, redis = _make_manager(config=config)
        redis.client.scard.return_value = 2
        redis.client.hlen.return_value = 1

        manager._fill_pool()

        proxmox.clone_vm.assert_not_called()

    def test_clone_failure_continues(self):
        """One clone failure should not prevent subsequent clones."""
        config = _make_config(pool_size=3)
        manager, proxmox, redis = _make_manager(config=config)
        redis.client.scard.return_value = 0
        redis.client.hlen.return_value = 0
        proxmox.next_free_vmid.side_effect = [300, 301, 302]
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

    def test_uses_correct_redis_keys(self):
        config = _make_config(pool_size=4)
        manager, proxmox, redis = _make_manager(config=config)
        redis.client.scard.return_value = 4
        redis.client.hlen.return_value = 0

        manager._fill_pool()

        redis.client.scard.assert_called_once_with("orcest:pool:idle")
        redis.client.hlen.assert_called_once_with("orcest:pool:active")

    def test_counts_active_towards_total(self):
        """Active VMs count toward the total, reducing deficit."""
        config = _make_config(pool_size=4)
        manager, proxmox, redis = _make_manager(config=config)
        redis.client.scard.return_value = 1  # 1 idle
        redis.client.hlen.return_value = 2  # 2 active
        # Total = 3, deficit = 1
        proxmox.next_free_vmid.return_value = 300
        proxmox.get_vm_ip.return_value = "10.20.0.50"

        manager._fill_pool()

        assert proxmox.clone_vm.call_count == 1


# ── _health_check ────────────────────────────────────────────


class TestHealthCheck:
    def test_no_active_workers(self):
        manager, proxmox, redis = _make_manager()
        redis.hgetall.return_value = {}

        manager._health_check()

        proxmox.stop_vm.assert_not_called()

    def test_healthy_worker_not_destroyed(self):
        config = _make_config(max_task_duration=3600)
        manager, proxmox, redis = _make_manager(config=config)
        now = time.time()
        redis.hgetall.return_value = {"300": str(now - 100)}

        manager._health_check()

        proxmox.stop_vm.assert_not_called()
        proxmox.destroy_vm.assert_not_called()

    def test_expired_worker_destroyed(self):
        config = _make_config(max_task_duration=3600)
        manager, proxmox, redis = _make_manager(config=config)
        pipe = MagicMock()
        redis.client.pipeline.return_value = pipe
        now = time.time()
        redis.hgetall.return_value = {"300": str(now - 4000)}

        manager._health_check()

        proxmox.stop_vm.assert_called_once_with(300)
        proxmox.destroy_vm.assert_called_once_with(300)

    def test_multiple_workers_mixed(self):
        config = _make_config(max_task_duration=3600)
        manager, proxmox, redis = _make_manager(config=config)
        pipe = MagicMock()
        redis.client.pipeline.return_value = pipe
        now = time.time()
        redis.hgetall.return_value = {
            "300": str(now - 100),   # healthy
            "301": str(now - 5000),  # expired
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
        pipe = MagicMock()
        redis.client.pipeline.return_value = pipe

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


# ── reconcile ────────────────────────────────────────────────


class TestReconcile:
    def test_calls_all_phases(self):
        manager, proxmox, redis = _make_manager()

        with (
            patch.object(manager, "_check_done_workers", return_value=[]) as mock_done,
            patch.object(manager, "_detect_active_workers") as mock_detect,
            patch.object(manager, "_fill_pool") as mock_fill,
            patch.object(manager, "_health_check") as mock_health,
        ):
            manager.reconcile()

        mock_done.assert_called_once()
        mock_detect.assert_called_once()
        mock_fill.assert_called_once()
        mock_health.assert_called_once()

    def test_error_does_not_crash(self):
        manager, proxmox, redis = _make_manager()

        with patch.object(
            manager, "_check_done_workers", side_effect=Exception("Redis down")
        ):
            # Should not raise
            manager.reconcile()

    def test_phases_called_in_order(self):
        manager, proxmox, redis = _make_manager()
        call_order = []

        with (
            patch.object(manager, "_check_done_workers", side_effect=lambda: call_order.append("check_done") or []),
            patch.object(manager, "_detect_active_workers", side_effect=lambda: call_order.append("detect_active")),
            patch.object(manager, "_fill_pool", side_effect=lambda: call_order.append("fill_pool")),
            patch.object(manager, "_health_check", side_effect=lambda: call_order.append("health_check")),
        ):
            manager.reconcile()

        assert call_order == ["check_done", "detect_active", "fill_pool", "health_check"]

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
            patch("orcest.fleet.pool_manager.time") as mock_time,
        ):
            mock_time.sleep = MagicMock()
            manager.run(interval=5.0)

        assert call_count == 3
        assert mock_time.sleep.call_count == 2  # sleep called between reconciles
        mock_time.sleep.assert_called_with(5.0)


# ── Integration-style test ───────────────────────────────────


class TestFullCycle:
    """Tests that exercise multiple phases of the reconciliation loop together."""

    def test_done_worker_replaced(self):
        """A done worker is destroyed and a new one is cloned to replace it."""
        config = _make_config(pool_size=2)
        proxmox = _make_proxmox()
        redis = _make_redis()

        manager = PoolManager(config=config, proxmox=proxmox, redis=redis)

        # One done worker, one idle -> total currently 1, deficit 1
        redis.scan_iter.return_value = ["pool:done:orcest-worker-300"]
        redis.client.scard.return_value = 1  # 1 idle remaining after destroy
        redis.client.hlen.return_value = 0
        redis.hgetall.return_value = {}
        redis.client.smembers.return_value = {"301"}
        redis.xinfo_groups.return_value = []

        pipe = MagicMock()
        redis.client.pipeline.return_value = pipe
        proxmox.next_free_vmid.return_value = 302
        proxmox.get_vm_ip.return_value = "10.20.0.51"

        manager.reconcile()

        # Done worker destroyed
        proxmox.stop_vm.assert_any_call(300)
        proxmox.destroy_vm.assert_any_call(300)
        redis.delete.assert_any_call("pool:done:orcest-worker-300")

        # New worker cloned to fill deficit
        proxmox.clone_vm.assert_called_once_with(
            template_id=9000,
            new_id=302,
            name="orcest-worker-302",
            storage="ssd-pool",
            linked=True,
        )
