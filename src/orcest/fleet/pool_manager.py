"""Warm pool manager for ephemeral worker VMs.

Maintains a target number of pre-booted worker VMs that process one task
each, then get destroyed and replaced. Uses the Proxmox API for VM lifecycle
and Redis for coordination with workers.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field

from orcest.fleet.config import FleetConfig
from orcest.fleet.proxmox_api import ProxmoxClient
from orcest.shared.redis_client import RedisClient

logger = logging.getLogger(__name__)

# Redis keys (auto-prefixed by RedisClient)
_POOL_IDLE_KEY = "pool:idle"
_POOL_ACTIVE_KEY = "pool:active"
_POOL_DONE_PREFIX = "pool:done:"

# VM naming convention
_VM_NAME_PREFIX = "orcest-worker-"

# Consumer group used by workers (must match worker/loop.py)
_CONSUMER_GROUP = "workers"


@dataclass
class PoolState:
    """Current state of the warm pool."""

    idle: set[int] = field(default_factory=set)
    active: dict[int, float] = field(default_factory=dict)
    target_size: int = 4


class PoolManager:
    """Manages a warm pool of ephemeral worker VMs.

    Each VM processes one task then exits. The pool manager detects
    completed workers via ``pool:done:{worker_id}`` Redis keys, destroys
    the finished VMs, and clones replacements from a template to maintain
    the target pool size.

    State is tracked in Redis so the manager can recover from crashes by
    reconciling Redis state against Proxmox.

    Args:
        config: Fleet configuration with pool settings.
        proxmox: Proxmox API client for VM lifecycle operations.
        redis: Redis client for coordination.
    """

    def __init__(
        self,
        config: FleetConfig,
        proxmox: ProxmoxClient,
        redis: RedisClient,
    ) -> None:
        self._config = config
        self._proxmox = proxmox
        self._redis = redis
        self._pool = config.pool

    def reconcile(self) -> None:
        """Single reconciliation pass.

        Checks for done workers, detects active, replaces VMs, and runs health checks.
        """
        try:
            self._check_done_workers()
            self._detect_active_workers()
            self._fill_pool()
            self._health_check()
        except Exception:
            logger.error("Reconciliation pass failed", exc_info=True)

    def _check_done_workers(self) -> list[int]:
        """Scan for pool:done:* keys, destroy completed VMs, return list of destroyed VM IDs.

        Workers set ``pool:done:{worker_id}`` (e.g. ``pool:done:orcest-worker-300``)
        when they finish their task. This method finds those keys, destroys the
        corresponding VMs, and cleans up Redis state.
        """
        destroyed: list[int] = []
        done_keys = self._redis.scan_iter(match="pool:done:*")

        for key in done_keys:
            # key is like "pool:done:orcest-worker-300"
            worker_id = key.removeprefix(_POOL_DONE_PREFIX)
            vm_id = self._worker_id_to_vm_id(worker_id)
            if vm_id is None:
                logger.warning("Cannot parse VM ID from done key: %s", key)
                self._redis.delete(key)
                continue

            logger.info("Worker %s (VM %d) reported done, destroying", worker_id, vm_id)
            self._destroy_vm(vm_id)
            self._redis.delete(key)
            destroyed.append(vm_id)

        return destroyed

    def _detect_active_workers(self) -> None:
        """Move VMs from idle to active when they pick up a task.

        Checks which consumers in the Redis stream consumer group have
        pending entries (indicating they claimed a task). VMs with pending
        entries are moved from ``pool:idle`` to ``pool:active``.
        """
        idle_members = self._redis.client.smembers(
            self._redis._prefixed(_POOL_IDLE_KEY)
        )
        if not idle_members:
            return

        # Get consumers with pending entries from task streams
        active_consumers: set[str] = set()
        for stream in ("tasks:claude", "tasks:issue:claude"):
            try:
                groups = self._redis.xinfo_groups(stream)
            except Exception:
                continue
            for group in groups:
                if group.get("name") != _CONSUMER_GROUP:
                    continue
                # Check individual consumers in this group
                try:
                    consumers = self._redis.client.xinfo_consumers(
                        self._redis._prefixed(stream), _CONSUMER_GROUP
                    )
                except Exception:
                    continue
                for consumer in consumers:
                    if consumer.get("pending", 0) > 0:
                        active_consumers.add(str(consumer.get("name", "")))

        # Move idle VMs to active if their consumer has pending entries
        now = time.time()
        for member in idle_members:
            member_str = str(member)
            try:
                vm_id = int(member_str)
            except (ValueError, TypeError):
                continue

            worker_id = self._vm_id_to_worker_id(vm_id)
            if worker_id in active_consumers:
                logger.info("VM %d picked up a task, moving to active", vm_id)
                pipe = self._redis.client.pipeline()
                pipe.srem(self._redis._prefixed(_POOL_IDLE_KEY), str(vm_id))
                pipe.hset(
                    self._redis._prefixed(_POOL_ACTIVE_KEY),
                    str(vm_id),
                    str(now),
                )
                pipe.execute()

    def _fill_pool(self) -> None:
        """Clone and boot VMs until pool reaches target size."""
        idle_count = self._redis.client.scard(
            self._redis._prefixed(_POOL_IDLE_KEY)
        )
        active_count = self._redis.client.hlen(
            self._redis._prefixed(_POOL_ACTIVE_KEY)
        )
        total = int(idle_count) + int(active_count)
        deficit = self._pool.size - total

        if deficit <= 0:
            return

        logger.info(
            "Pool deficit: %d (idle=%d, active=%d, target=%d)",
            deficit, idle_count, active_count, self._pool.size,
        )

        for _ in range(deficit):
            try:
                self._clone_and_boot()
            except Exception:
                logger.error("Failed to clone and boot VM", exc_info=True)

    def _destroy_vm(self, vm_id: int) -> None:
        """Stop and destroy a VM, remove from tracking sets."""
        try:
            self._proxmox.stop_vm(vm_id)
        except Exception:
            logger.warning("Failed to stop VM %d (may already be stopped)", vm_id)

        try:
            self._proxmox.destroy_vm(vm_id)
        except Exception:
            logger.error("Failed to destroy VM %d", vm_id, exc_info=True)

        # Remove from all tracking sets regardless of Proxmox errors
        pipe = self._redis.client.pipeline()
        pipe.srem(self._redis._prefixed(_POOL_IDLE_KEY), str(vm_id))
        pipe.hdel(self._redis._prefixed(_POOL_ACTIVE_KEY), str(vm_id))
        pipe.execute()

    def _clone_and_boot(self) -> int | None:
        """Clone a new VM from template, start it, add to idle set.

        Returns the new VM ID, or None if the operation fails.
        """
        template_id = self._pool.template_vm_id
        if not template_id:
            logger.error("No template VM ID configured (pool.template_vm_id)")
            return None

        new_id = self._proxmox.next_free_vmid()
        name = self._vm_id_to_worker_id(new_id)

        logger.info("Cloning VM %d from template %d (name=%s)", new_id, template_id, name)

        self._proxmox.clone_vm(
            template_id=template_id,
            new_id=new_id,
            name=name,
            storage=self._pool.storage,
            linked=True,
        )

        self._proxmox.start_vm(new_id)

        # Wait for guest agent to report an IP (confirms VM is booted)
        ip = self._proxmox.get_vm_ip(new_id)
        if ip is None:
            logger.warning("VM %d did not get an IP, destroying", new_id)
            self._destroy_vm(new_id)
            return None

        logger.info("VM %d booted with IP %s, adding to idle pool", new_id, ip)
        self._redis.client.sadd(
            self._redis._prefixed(_POOL_IDLE_KEY), str(new_id)
        )

        return new_id

    def _health_check(self) -> None:
        """Force-destroy VMs that exceeded max_task_duration."""
        active = self._redis.hgetall(_POOL_ACTIVE_KEY)
        if not active:
            return

        now = time.time()
        max_duration = self._pool.max_task_duration

        for vm_id_str, start_ts_str in active.items():
            try:
                vm_id = int(vm_id_str)
                start_ts = float(start_ts_str)
            except (ValueError, TypeError):
                logger.warning(
                    "Invalid active pool entry: vm_id=%s, start_ts=%s",
                    vm_id_str, start_ts_str,
                )
                continue

            elapsed = now - start_ts
            if elapsed > max_duration:
                logger.warning(
                    "VM %d exceeded max task duration (%.0fs > %ds), force-destroying",
                    vm_id, elapsed, max_duration,
                )
                self._destroy_vm(vm_id)

    def run(self, interval: float = 10.0) -> None:
        """Main loop: reconcile every ``interval`` seconds.

        Runs indefinitely until interrupted (KeyboardInterrupt or SIGTERM).

        Args:
            interval: Seconds between reconciliation passes.
        """
        logger.info(
            "Pool manager starting (target_size=%d, interval=%.1fs)",
            self._pool.size, interval,
        )
        try:
            while True:
                self.reconcile()
                time.sleep(interval)
        except KeyboardInterrupt:
            logger.info("Pool manager interrupted, shutting down.")

    # ── helpers ──────────────────────────────────────────────

    @staticmethod
    def _worker_id_to_vm_id(worker_id: str) -> int | None:
        """Extract VM ID from worker ID (e.g. 'orcest-worker-300' -> 300)."""
        if worker_id.startswith(_VM_NAME_PREFIX):
            suffix = worker_id[len(_VM_NAME_PREFIX):]
            try:
                return int(suffix)
            except ValueError:
                return None
        # Bare integer (e.g. from legacy naming)
        try:
            return int(worker_id)
        except ValueError:
            return None

    @staticmethod
    def _vm_id_to_worker_id(vm_id: int) -> str:
        """Build worker ID from VM ID (e.g. 300 -> 'orcest-worker-300')."""
        return f"{_VM_NAME_PREFIX}{vm_id}"
