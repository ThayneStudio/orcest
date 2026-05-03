"""Warm pool manager for ephemeral worker VMs.

Maintains a target number of pre-booted worker VMs that process one task
each, then get destroyed and replaced. Uses the Proxmox API for VM lifecycle
and Redis for coordination with workers.

IMPORTANT: Only one pool manager instance should run at a time. The VMID
allocation (next_free_vmid + clone_vm) is not atomic, so concurrent
instances could clash.
"""

from __future__ import annotations

import logging
import signal
import threading
import time

from orcest.fleet.cloud_init import render_clone_userdata
from orcest.fleet.config import FleetConfig
from orcest.fleet.proxmox_api import ProxmoxClient, mac_for_vm_id
from orcest.shared.models import CONSUMER_GROUP
from orcest.shared.redis_client import RedisClient

logger = logging.getLogger(__name__)

# Redis keys (auto-prefixed by RedisClient)
_POOL_IDLE_KEY = "pool:idle"
_POOL_ACTIVE_KEY = "pool:active"
_POOL_DONE_PREFIX = "pool:done:"
# Pointer naming the active worker template VMID. Set by `orcest fleet rebake`
# (or initialised from `pool.template_vm_id` on first run for backward compat).
_POOL_CURRENT_TEMPLATE_KEY = "pool:current_template_vmid"

# VM naming convention
_VM_NAME_PREFIX = "orcest-worker-"


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
        key_prefix: str = "orcest",
    ) -> None:
        self._config = config
        self._proxmox = proxmox
        self._redis = redis
        self._pool = config.pool
        self._key_prefix = key_prefix

    def reconcile(self) -> None:
        """Single reconciliation pass.

        Checks for done workers, detects active, replaces VMs, runs health
        checks, and cleans up stale Redis entries.
        """
        try:
            self._check_done_workers()
            self._detect_active_workers()
            self._fill_pool()
            self._health_check()
            self._reconcile_stale_redis()
        except Exception:
            logger.error("Reconciliation pass failed", exc_info=True)

    def _check_done_workers(self) -> list[int]:
        """Scan for pool:done:* keys, destroy completed VMs, return list of destroyed VM IDs.

        Workers set ``pool:done:{worker_id}`` (e.g. ``pool:done:orcest-worker-300``)
        when they finish their task. This method finds those keys, destroys the
        corresponding VMs, and cleans up Redis state.

        Each done key is processed independently so that a failure destroying
        one VM does not prevent the remaining done workers from being cleaned up.
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
            # _destroy_vm handles all exceptions internally (Proxmox and
            # Redis failures are logged and swallowed).  Any partial
            # failure is recovered by _reconcile_orphans (Proxmox side)
            # or _reconcile_stale_redis (Redis side) on subsequent passes.
            self._destroy_vm(vm_id)
            destroyed.append(vm_id)
            # Always delete the done key so it does not accumulate.
            self._redis.delete(key)

        return destroyed

    def _detect_active_workers(self) -> None:
        """Move VMs from idle to active when they pick up a task.

        Checks which consumers in the Redis stream consumer group have
        pending entries (indicating they claimed a task). VMs with pending
        entries are moved from ``pool:idle`` to ``pool:active``.
        """
        idle_members = self._redis.smembers(_POOL_IDLE_KEY)
        if not idle_members:
            return

        # Get consumers with pending entries from task streams
        active_consumers: set[str] = set()
        for stream_name in self._task_streams():
            try:
                groups = self._redis.xinfo_groups_raw(stream_name)
            except Exception:
                continue
            for group in groups:
                if group.get("name") != CONSUMER_GROUP:
                    continue
                try:
                    consumers = self._redis.xinfo_consumers_raw(stream_name, CONSUMER_GROUP)
                except Exception:
                    continue
                for consumer in consumers:
                    if consumer.get("pending", 0) > 0:
                        active_consumers.add(str(consumer.get("name", "")))

        # Move idle VMs to active if their consumer has pending entries
        # Batch all transitions into a single pipeline
        now = time.time()
        transitions: list[int] = []
        for member in idle_members:
            member_str = str(member)
            try:
                vm_id = int(member_str)
            except (ValueError, TypeError):
                continue

            worker_id = self._vm_id_to_worker_id(vm_id)
            if worker_id in active_consumers:
                logger.info("VM %d picked up a task, moving to active", vm_id)
                transitions.append(vm_id)

        if transitions:
            try:
                pipe = self._redis.pipeline()
                for vm_id in transitions:
                    pipe.srem(_POOL_IDLE_KEY, str(vm_id))
                    pipe.hset(_POOL_ACTIVE_KEY, str(vm_id), str(now))
                pipe.execute()
            except Exception:
                logger.error(
                    "Failed to move %d VMs from idle to active; transitions "
                    "will be retried on the next pass",
                    len(transitions),
                    exc_info=True,
                )

    def _fill_pool(self) -> None:
        """Adjust pool to target size: clone new VMs or drain idle excess."""
        idle_count = self._redis.scard(_POOL_IDLE_KEY)
        active_count = self._redis.hlen(_POOL_ACTIVE_KEY)
        total = int(idle_count) + int(active_count)
        deficit = self._pool.size - total

        if deficit > 0:
            logger.info(
                "Pool deficit: %d (idle=%d, active=%d, target=%d)",
                deficit,
                idle_count,
                active_count,
                self._pool.size,
            )
            for _ in range(deficit):
                try:
                    self._clone_and_boot()
                except Exception:
                    logger.error("Failed to clone and boot VM", exc_info=True)
        elif deficit < 0 and int(idle_count) > 0:
            # Excess VMs — destroy idle ones (never kill active workers).
            excess = min(-deficit, int(idle_count))
            logger.info(
                "Pool excess: %d (idle=%d, active=%d, target=%d), draining %d idle",
                -deficit,
                idle_count,
                active_count,
                self._pool.size,
                excess,
            )
            idle_members = list(self._redis.smembers(_POOL_IDLE_KEY))
            for member in idle_members[:excess]:
                try:
                    vm_id = int(member)
                except (ValueError, TypeError):
                    continue
                logger.info("Draining excess idle VM %d", vm_id)
                self._destroy_vm(vm_id)

    def _destroy_vm(self, vm_id: int) -> None:
        """Stop and destroy a VM, remove from tracking sets."""
        try:
            self._proxmox.stop_vm(vm_id)
            # Brief wait for VM to stop before destroying
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                try:
                    if self._proxmox.get_vm_status(vm_id) == "stopped":
                        break
                except Exception:
                    break
                time.sleep(1)
        except Exception:
            logger.warning("Failed to stop VM %d (may already be stopped)", vm_id)

        try:
            self._proxmox.destroy_vm(vm_id)
        except Exception:
            logger.error("Failed to destroy VM %d", vm_id, exc_info=True)

        # Remove from all tracking sets regardless of Proxmox errors.
        # If this pipeline fails, the stale Redis entry will be cleaned up
        # by _reconcile_stale_redis on the next reconciliation pass.
        try:
            pipe = self._redis.pipeline()
            pipe.srem(_POOL_IDLE_KEY, str(vm_id))
            pipe.hdel(_POOL_ACTIVE_KEY, str(vm_id))
            pipe.execute()
        except Exception:
            logger.error(
                "Failed to clean Redis tracking for VM %d; stale entry will be "
                "reconciled on next pass",
                vm_id,
                exc_info=True,
            )

    def _resolve_template_vmid(self) -> int | None:
        """Return the VMID of the currently-active worker template.

        Read order each cycle:
          1. Redis pointer ``pool:current_template_vmid`` (set by ``rebake``).
          2. Single-VMID fallback ``pool.template_vm_id`` (legacy/zero-config).
             When used, the pointer is also initialised so subsequent reads
             come from Redis without restart.

        Returns ``None`` if neither is configured.
        """
        try:
            raw = self._redis.get(_POOL_CURRENT_TEMPLATE_KEY)
        except Exception:
            logger.warning(
                "Failed to read template pointer from Redis; falling back to config",
                exc_info=True,
            )
            raw = None

        if raw:
            try:
                return int(raw)
            except (TypeError, ValueError):
                logger.warning(
                    "Invalid template pointer %r in Redis; falling back to config", raw
                )

        fallback = self._pool.template_vm_id
        if not fallback:
            return None
        # Initialise pointer from config so future reads come from Redis.
        # Best-effort: if Redis is down we still return the fallback ID.
        try:
            self._redis.set_ex(_POOL_CURRENT_TEMPLATE_KEY, str(fallback), ttl=86400 * 365)
        except Exception:
            logger.warning("Failed to initialise template pointer in Redis", exc_info=True)
        return fallback

    def _clone_and_boot(self) -> int | None:
        """Clone a new VM from template, start it, add to idle set.

        Returns the new VM ID, or None if the operation fails.

        Note: next_free_vmid() + clone_vm() is not atomic. This is safe
        as long as only one pool manager instance is running. See module
        docstring.
        """
        template_id = self._resolve_template_vmid()
        if not template_id:
            logger.error(
                "No template VM ID configured "
                "(pool.template_vm_id or Redis pointer pool:current_template_vmid)"
            )
            return None

        if not self._pool.vm_id_start:
            logger.error("No worker VM ID range configured (pool.vm_id_start)")
            return None

        new_id = self._next_vm_id()
        name = self._vm_id_to_worker_id(new_id)

        logger.info("Cloning VM %d from template %d (name=%s)", new_id, template_id, name)

        try:
            self._proxmox.clone_vm(
                template_id=template_id,
                new_id=new_id,
                name=name,
                linked=True,
            )
        except Exception:
            logger.error("Failed to clone VM %d, attempting cleanup", new_id, exc_info=True)
            # clone_vm may have partially created the VM before failing
            # (e.g. Proxmox task timeout after clone completed).  Best-effort
            # cleanup to avoid orphaned VMs in Proxmox.
            try:
                self._proxmox.destroy_vm(new_id)
            except Exception:
                pass  # VM may not exist; either way, nothing more we can do
            return None

        # Assign a deterministic MAC so DHCP leases are recycled when
        # VMs are destroyed and recreated with the same ID.
        try:
            mac = mac_for_vm_id(new_id)
            self._proxmox.set_vm_network(new_id, mac=mac)
        except Exception:
            logger.error("Failed to set MAC on VM %d, destroying", new_id, exc_info=True)
            self._destroy_vm(new_id)
            return None

        # Set cloud-init userdata so the clone starts the worker service
        try:
            userdata = render_clone_userdata(
                redis_host=self._config.orchestrator.host,
                worker_id=name,
                key_prefix=self._key_prefix,
            )
            self._proxmox.set_cloud_init_userdata(
                new_id,
                userdata,
                storage=self._pool.snippet_storage,
            )
        except Exception:
            logger.error("Failed to set cloud-init on VM %d, destroying", new_id, exc_info=True)
            self._destroy_vm(new_id)
            return None

        try:
            self._proxmox.start_vm(new_id)

            # Wait for guest agent to report an IP (confirms VM is booted)
            ip = self._proxmox.get_vm_ip(new_id)
            if ip is None:
                logger.warning("VM %d did not get an IP, destroying", new_id)
                self._destroy_vm(new_id)
                return None
        except Exception:
            logger.error("Failed to boot VM %d, destroying clone", new_id, exc_info=True)
            self._destroy_vm(new_id)
            return None

        logger.info("VM %d booted with IP %s, adding to idle pool", new_id, ip)
        try:
            self._redis.sadd(_POOL_IDLE_KEY, str(new_id))
        except Exception:
            logger.error(
                "Failed to add VM %d to idle pool in Redis, destroying to avoid orphan",
                new_id,
                exc_info=True,
            )
            self._destroy_vm(new_id)
            return None

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
                    vm_id_str,
                    start_ts_str,
                )
                continue

            raw_elapsed = now - start_ts
            if raw_elapsed < 0:
                logger.warning(
                    "VM %d: clock skew detected (start_ts=%.3f now=%.3f, delta=%.3f);"
                    " skipping health check",
                    vm_id,
                    start_ts,
                    now,
                    raw_elapsed,
                )
                continue
            elapsed = raw_elapsed
            if elapsed > max_duration:
                logger.warning(
                    "VM %d exceeded max task duration (%.0fs > %ds), force-destroying",
                    vm_id,
                    elapsed,
                    max_duration,
                )
                # _destroy_vm handles all exceptions internally.
                self._destroy_vm(vm_id)

    def _reconcile_orphans(self) -> None:
        """Detect and clean up orphaned VMs not tracked in Redis.

        Cross-references Proxmox VMs (by name prefix) against the
        ``pool:idle`` and ``pool:active`` Redis sets.  VMs that exist in
        Proxmox but are absent from both sets are either:

        - Leftovers from a pool manager crash between clone and Redis add.
        - VMs whose Redis tracking was lost (e.g. pipeline failure in
          ``_destroy_vm`` after the Proxmox destroy failed).

        Orphans are destroyed to prevent resource leaks.  This method is
        called once at startup and does not need to run every reconciliation
        pass since orphans can only be created by pool manager crashes.
        """
        try:
            proxmox_vms = self._proxmox.list_vms(name_prefix=_VM_NAME_PREFIX)
        except Exception:
            logger.warning("Failed to list VMs for orphan reconciliation", exc_info=True)
            return

        idle_members = self._redis.smembers(_POOL_IDLE_KEY)
        active_members = set(self._redis.hgetall(_POOL_ACTIVE_KEY).keys())
        tracked_vm_ids: set[int] = set()
        for member in idle_members | active_members:
            try:
                tracked_vm_ids.add(int(member))
            except (ValueError, TypeError):
                continue

        for vm_info in proxmox_vms:
            vm_id = vm_info.get("vmid")
            if vm_id is None:
                continue
            vm_id = int(vm_id)

            # Skip any template VMID (active or older blue/green generations
            # awaiting GC). Old templates with live linked clones must not be
            # touched here; they get cleaned up by `orcest fleet gc-templates`
            # once their clones churn out.
            if self._is_template_vmid(vm_id):
                continue

            if vm_id not in tracked_vm_ids:
                logger.warning(
                    "Orphaned VM %d (%s) found in Proxmox but not tracked in Redis, destroying",
                    vm_id,
                    vm_info.get("name", "unknown"),
                )
                # _destroy_vm handles all exceptions internally.
                self._destroy_vm(vm_id)

    def _reconcile_stale_redis(self) -> None:
        """Remove Redis pool entries whose VMs no longer exist in Proxmox.

        This is the complement of ``_reconcile_orphans`` (which finds Proxmox
        VMs not tracked in Redis).  Stale entries occur when:

        - ``_destroy_vm`` successfully destroys the Proxmox VM but the Redis
          pipeline that removes the tracking entry fails (e.g. transient
          connection error).
        - A VM is removed externally (admin action, Proxmox host crash).

        Stale idle entries block ``_fill_pool`` from seeing a deficit.  Stale
        active entries cause ``_health_check`` to repeatedly try to destroy
        a non-existent VM.  Both are cleaned up here.

        Runs on every reconciliation pass (not just startup) because stale
        entries can be created by pipeline failures during normal operation.
        """
        try:
            proxmox_vms = self._proxmox.list_vms(name_prefix=_VM_NAME_PREFIX)
        except Exception:
            logger.warning(
                "Failed to list VMs for stale Redis reconciliation",
                exc_info=True,
            )
            return

        proxmox_vm_ids: set[int] = set()
        for vm_info in proxmox_vms:
            vm_id = vm_info.get("vmid")
            if vm_id is not None:
                try:
                    proxmox_vm_ids.add(int(vm_id))
                except (ValueError, TypeError):
                    continue

        # Check idle set for stale entries
        idle_members = self._redis.smembers(_POOL_IDLE_KEY)
        for member in idle_members:
            try:
                vm_id = int(member)
            except (ValueError, TypeError):
                continue
            if vm_id not in proxmox_vm_ids and not self._is_template_vmid(vm_id):
                logger.warning(
                    "Stale idle entry VM %d not found in Proxmox, removing from Redis",
                    vm_id,
                )
                try:
                    self._redis.srem(_POOL_IDLE_KEY, str(vm_id))
                except Exception:
                    logger.error(
                        "Failed to remove stale idle entry VM %d from Redis",
                        vm_id,
                        exc_info=True,
                    )

        # Check active hash for stale entries
        active_members = self._redis.hgetall(_POOL_ACTIVE_KEY)
        for vm_id_str in active_members:
            try:
                vm_id = int(vm_id_str)
            except (ValueError, TypeError):
                continue
            if vm_id not in proxmox_vm_ids and not self._is_template_vmid(vm_id):
                logger.warning(
                    "Stale active entry VM %d not found in Proxmox, removing from Redis",
                    vm_id,
                )
                try:
                    self._redis.hdel(_POOL_ACTIVE_KEY, str(vm_id))
                except Exception:
                    logger.error(
                        "Failed to remove stale active entry VM %d from Redis",
                        vm_id,
                        exc_info=True,
                    )

    def run(self, interval: float = 10.0) -> None:
        """Main loop: reconcile every ``interval`` seconds.

        Runs indefinitely until interrupted (KeyboardInterrupt or SIGTERM).
        On startup, runs orphan reconciliation to clean up VMs that may have
        been left behind by a previous crash.

        Args:
            interval: Seconds between reconciliation passes.
        """
        logger.info(
            "Pool manager starting (target_size=%d, interval=%.1fs)",
            self._pool.size,
            interval,
        )
        self._reconcile_orphans()
        stop_event = threading.Event()

        def _handle_term(*_: object) -> None:
            stop_event.set()

        signal.signal(signal.SIGTERM, _handle_term)
        try:
            while not stop_event.is_set():
                self.reconcile()
                stop_event.wait(timeout=interval)
        except KeyboardInterrupt:
            pass
        logger.info("Pool manager shutting down.")

    # ── helpers ──────────────────────────────────────────────

    def _task_streams(self) -> tuple[str, ...]:
        """Build fully-qualified shared task stream names."""
        prefix = self._key_prefix
        return (
            f"{prefix}:tasks:claude",
            f"{prefix}:tasks:issue:claude",
        )

    def _is_template_vmid(self, vm_id: int) -> bool:
        """Return True if *vm_id* names a template (active or pending GC).

        Includes the active template (Redis pointer or single-VMID config)
        and every VMID in the configured ``template_vmid_range``.
        """
        try:
            rng = self._pool.template_range()
        except ValueError:
            rng = None
        if rng is not None and rng[0] <= vm_id <= rng[1]:
            return True
        if vm_id == self._pool.template_vm_id and self._pool.template_vm_id != 0:
            return True
        active = self._resolve_template_vmid()
        return active is not None and vm_id == active

    def _next_vm_id(self) -> int:
        """Allocate the next VM ID from the configured pool range.

        Scans existing orcest-worker-* VMs in Proxmox and picks the next
        ID starting from ``pool.vm_id_start``, skipping any that are
        already in use.
        """
        start = self._pool.vm_id_start
        existing: set[int] = set()
        try:
            for vm in self._proxmox.list_vms(name_prefix=_VM_NAME_PREFIX):
                vm_id = vm.get("vmid")
                if vm_id is not None:
                    existing.add(int(vm_id))
        except Exception:
            logger.warning("Failed to list VMs for ID allocation", exc_info=True)

        # Also include IDs tracked in Redis (may not yet be visible in Proxmox)
        for member in self._redis.smembers(_POOL_IDLE_KEY):
            try:
                existing.add(int(member))
            except (ValueError, TypeError):
                pass
        for member in self._redis.hgetall(_POOL_ACTIVE_KEY):
            try:
                existing.add(int(member))
            except (ValueError, TypeError):
                pass

        candidate = start
        while candidate in existing:
            candidate += 1
            if self._pool.vm_id_end and candidate > self._pool.vm_id_end:
                raise RuntimeError(
                    f"VM ID pool exhausted: all IDs in range "
                    f"{self._pool.vm_id_start}-{self._pool.vm_id_end} are in use"
                )
        if self._pool.vm_id_end and candidate > self._pool.vm_id_end:
            raise RuntimeError(
                f"VM ID pool exhausted: all IDs in range "
                f"{self._pool.vm_id_start}-{self._pool.vm_id_end} are in use"
            )
        return candidate

    @staticmethod
    def _worker_id_to_vm_id(worker_id: str) -> int | None:
        """Extract VM ID from worker ID (e.g. 'orcest-worker-300' -> 300)."""
        if worker_id.startswith(_VM_NAME_PREFIX):
            suffix = worker_id[len(_VM_NAME_PREFIX) :]
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
