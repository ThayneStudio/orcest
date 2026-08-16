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
import os
import signal
import threading
import time
from typing import Any, cast

import redis

from orcest.fleet.cloud_init import render_clone_userdata
from orcest.fleet.config import FleetConfig, WorkerProfileConfig
from orcest.fleet.proxmox_api import ProxmoxClient, mac_for_vm_id
from orcest.shared.coordination import (
    clear_pending_task_if_matches,
    make_pending_task_key,
    parse_pending_task_metadata,
)
from orcest.shared.credential_handoff import (
    CredentialRecoveryOutcome,
    handoff_marker_key,
    publish_handoff_once,
    recover_credential_checkpoint,
    safe_dead_letter_fields,
    source_entry_pending_state,
)
from orcest.shared.models import (
    CONSUMER_GROUP,
    DEAD_LETTER_STREAM,
    TRANSIENT_SUMMARY_PREFIX,
    ResultStatus,
    Task,
    TaskResult,
)
from orcest.shared.redis_client import RedisClient

logger = logging.getLogger(__name__)

# Redis keys (auto-prefixed by RedisClient)
_POOL_IDLE_KEY = "pool:idle"
_POOL_ACTIVE_KEY = "pool:active"
_POOL_DRAINING_KEY = "pool:draining"
_POOL_DONE_PREFIX = "pool:done:"
_POOL_PROVISIONING_KEY = "pool:provisioning"
_POOL_AMBIGUOUS_CLONES_KEY = "pool:ambiguous-clones"
_WORKER_HEARTBEAT_PREFIX = "workers:heartbeat:"
# Workers can block on the issue stream for up to five seconds. A drain lease
# must be visible for longer than that before the final PEL check.
_DRAIN_QUIESCE_SECONDS = 5.25
# Results stream + cap, mirroring worker/loop.py so the reaper writes a
# transient-FAILED result to the same place the orchestrator reads.
_RESULTS_STREAM = "results"
_RESULT_MAXLEN = 20000
# Fixed worker_id stamped on reaper-published results (operator-facing).
_REAPER_WORKER_ID = "pool-manager-reaper"
# Pointer naming the active worker template VMID. Set by `orcest fleet rebake`
# (or initialised from `pool.template_vm_id` on first run for backward compat).
_POOL_CURRENT_TEMPLATE_KEY = "pool:current_template_vmid"
_PENDING_READ_BATCH_SIZE = 100

# VM naming convention
_VM_NAME_PREFIX = "orcest-worker-"


def _is_missing_stream_or_group_error(exc: BaseException) -> bool:
    text = str(exc).lower()
    return "no such key" in text or "nogroup" in text or "no such consumer group" in text


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
        # De-spam flag: True once the "no valid template" error has been
        # logged, cleared again as soon as a valid template is resolved.
        self._template_recovery_failed = False
        # Redis is the durable source of truth. These sets bridge transient
        # Redis failures within this process after Proxmox has confirmed clone
        # success, and keep test doubles/inventory lag from reusing an ID.
        self._owned_provisioning_vmids: set[int] = set()
        self._allocated_vmids: set[int] = set()

    def reconcile(self) -> None:
        """Single reconciliation pass.

        Checks for done workers, detects active, replaces VMs, runs health
        checks, and cleans up stale Redis entries.
        """
        try:
            self._check_done_workers()
            self._detect_active_workers()
            ambiguity_clear = self._reconcile_ambiguous_clones()
            provisioning_clear = self._retry_provisioning_cleanups()
            if ambiguity_clear and provisioning_clear:
                self._fill_pool()
            self._health_check()
            self._reconcile_stale_redis()
            self._sweep_orphan_pel()
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

            if not self._is_destroyable_vm_id(vm_id):
                logger.error(
                    "Refusing to destroy done-key VM %d outside configured worker VMID range",
                    vm_id,
                )
                self._redis.delete(key)
                continue

            logger.info("Worker %s (VM %d) reported done, destroying", worker_id, vm_id)
            if not self._coordinate_reaped_vm(vm_id):
                logger.warning(
                    "Worker %s (VM %d) reported done, but Redis recovery is incomplete; "
                    "stopping VM to cap billing and leaving done key for the next "
                    "reconciliation pass",
                    worker_id,
                    vm_id,
                )
                self._stop_vm(vm_id)
                continue
            # _destroy_vm handles all exceptions internally (Proxmox and Redis
            # failures are logged and swallowed). Any partial failure is recovered
            # by _reconcile_orphans (Proxmox side) or _reconcile_stale_redis
            # (Redis side) on subsequent passes.
            if self._destroy_vm(vm_id):
                destroyed.append(vm_id)
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
        active_consumers = self._consumers_with_pending()

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
        if self._pool.worker_profiles:
            self._fill_profiled_pool()
            return

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
                    if self._clone_and_boot() is None:
                        # A failed clone may have an ambiguous outcome or an
                        # owned VM awaiting cleanup. Do not fan out more clone
                        # attempts until the next reconciliation resolves it.
                        break
                except Exception:
                    logger.error("Failed to clone and boot VM", exc_info=True)
                    break
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
            drained = 0
            for member in idle_members:
                if drained >= excess:
                    break
                try:
                    vm_id = int(member)
                except (ValueError, TypeError):
                    continue
                worker_id = self._vm_id_to_worker_id(vm_id)
                pending_consumers, pending_complete = self._consumers_with_pending_status()
                if not pending_complete:
                    logger.warning(
                        "Skipping drain of VM %d: pending-task state is unavailable",
                        vm_id,
                    )
                    continue
                if worker_id in pending_consumers:
                    logger.info(
                        "Skipping drain of VM %d: it has a pending task "
                        "(claimed since active-detection ran this pass)",
                        vm_id,
                    )
                    continue
                # Reserve the worker before the final PEL check. Workers honor
                # pool:draining before every XREADGROUP, which closes the race
                # where an idle VM claimed work after our earlier snapshot.
                try:
                    pipe = self._redis.pipeline()
                    pipe.srem(_POOL_IDLE_KEY, str(vm_id))
                    pipe.sadd(_POOL_DRAINING_KEY, worker_id)
                    pipe.execute()
                except Exception:
                    logger.warning("Failed to reserve VM %d for draining", vm_id, exc_info=True)
                    continue
                time.sleep(_DRAIN_QUIESCE_SECONDS)
                pending_consumers, pending_complete = self._consumers_with_pending_status()
                if not pending_complete or worker_id in pending_consumers:
                    logger.info(
                        "Skipping drain of VM %d: it claimed a task or final pending-state "
                        "inspection failed",
                        vm_id,
                    )
                    try:
                        pipe = self._redis.pipeline()
                        pipe.srem(_POOL_DRAINING_KEY, worker_id)
                        pipe.hset(_POOL_ACTIVE_KEY, str(vm_id), str(time.time()))
                        pipe.execute()
                    except Exception:
                        logger.error(
                            "Failed to restore busy VM %d after drain race", vm_id, exc_info=True
                        )
                    continue
                logger.info("Draining excess idle VM %d", vm_id)
                if not self._stop_vm(vm_id):
                    self._restore_worker_after_failed_drain(vm_id, worker_id)
                    continue
                # Once stopped, no new claim is possible. This post-stop PEL
                # check is the actual destruction commit point.
                pending_consumers, pending_complete = self._consumers_with_pending_status()
                if not pending_complete:
                    logger.warning(
                        "Leaving drained VM %d stopped: post-stop pending-state inspection failed",
                        vm_id,
                    )
                    self._restore_worker_after_failed_drain(
                        vm_id, worker_id, restart=True, active=True
                    )
                    continue
                if worker_id in pending_consumers and not self._coordinate_reaped_vm(vm_id):
                    logger.warning(
                        "Leaving drained VM %d: late task claim could not be recovered",
                        vm_id,
                    )
                    self._restore_worker_after_failed_drain(
                        vm_id, worker_id, restart=True, active=True
                    )
                    continue
                if self._destroy_stopped_vm(vm_id):
                    drained += 1

    def _fill_profiled_pool(self) -> None:
        """Reconcile heterogeneous workers to deterministic VMID/profile slots."""
        start = self._pool.vm_id_start
        if start <= 0:
            logger.error("No worker VM ID range configured (pool.vm_id_start)")
            return
        desired_vmids = set(range(start, start + self._pool.size))
        idle_vmids: set[int] = set()
        for member in self._redis.smembers(_POOL_IDLE_KEY):
            try:
                idle_vmids.add(int(member))
            except (ValueError, TypeError):
                continue
        active_vmids: set[int] = set()
        for member in self._redis.hgetall(_POOL_ACTIVE_KEY):
            try:
                active_vmids.add(int(member))
            except (ValueError, TypeError):
                continue
        tracked_vmids = idle_vmids | active_vmids

        # Scale-down and layout convergence retire only slots outside the exact
        # desired set. Active extras finish normally and are never replaced.
        extra_idle = sorted(idle_vmids - desired_vmids)
        if extra_idle:
            logger.info("Draining heterogeneous-pool slots outside target: %s", extra_idle)
            self._drain_profiled_idle_workers(extra_idle)

        missing = sorted(desired_vmids - tracked_vmids)
        if not missing:
            return
        logger.info(
            "Heterogeneous pool missing desired VMID slots %s (active extras=%s)",
            missing,
            sorted(active_vmids - desired_vmids),
        )
        for vm_id in missing:
            profile = self._pool.worker_profile_for_vmid(vm_id)
            try:
                if self._clone_and_boot(new_id=vm_id, profile=profile) is None:
                    break
            except Exception:
                logger.error(
                    "Failed to clone and boot VM %d for backend %s",
                    vm_id,
                    profile.backend,
                    exc_info=True,
                )
                break

    def _drain_profiled_idle_workers(self, idle_vmids: list[int]) -> int:
        """Drain the supplied out-of-layout idle workers in deterministic order."""
        drained = 0
        for vm_id in idle_vmids:
            worker_id = self._vm_id_to_worker_id(vm_id)
            pending_consumers, pending_complete = self._consumers_with_pending_status()
            if not pending_complete:
                logger.warning(
                    "Skipping drain of VM %d: pending-task state is unavailable",
                    vm_id,
                )
                continue
            if worker_id in pending_consumers:
                logger.info(
                    "Skipping drain of VM %d: it has a pending task "
                    "(claimed since active-detection ran this pass)",
                    vm_id,
                )
                continue
            try:
                pipe = self._redis.pipeline()
                pipe.srem(_POOL_IDLE_KEY, str(vm_id))
                pipe.sadd(_POOL_DRAINING_KEY, worker_id)
                pipe.execute()
            except Exception:
                logger.warning("Failed to reserve VM %d for draining", vm_id, exc_info=True)
                continue
            time.sleep(_DRAIN_QUIESCE_SECONDS)
            pending_consumers, pending_complete = self._consumers_with_pending_status()
            if not pending_complete or worker_id in pending_consumers:
                logger.info(
                    "Skipping drain of VM %d: it claimed a task or final pending-state "
                    "inspection failed",
                    vm_id,
                )
                try:
                    pipe = self._redis.pipeline()
                    pipe.srem(_POOL_DRAINING_KEY, worker_id)
                    pipe.hset(_POOL_ACTIVE_KEY, str(vm_id), str(time.time()))
                    pipe.execute()
                except Exception:
                    logger.error(
                        "Failed to restore busy VM %d after drain race", vm_id, exc_info=True
                    )
                continue
            logger.info("Draining out-of-layout idle VM %d", vm_id)
            if not self._stop_vm(vm_id):
                self._restore_worker_after_failed_drain(vm_id, worker_id)
                continue
            pending_consumers, pending_complete = self._consumers_with_pending_status()
            if not pending_complete:
                logger.warning(
                    "Leaving drained VM %d stopped: post-stop pending-state inspection failed",
                    vm_id,
                )
                self._restore_worker_after_failed_drain(vm_id, worker_id, restart=True, active=True)
                continue
            if worker_id in pending_consumers and not self._coordinate_reaped_vm(vm_id):
                logger.warning(
                    "Leaving drained VM %d: late task claim could not be recovered",
                    vm_id,
                )
                self._restore_worker_after_failed_drain(vm_id, worker_id, restart=True, active=True)
                continue
            if self._destroy_stopped_vm(vm_id):
                drained += 1
        return drained

    def _restore_worker_after_failed_drain(
        self,
        vm_id: int,
        worker_id: str,
        *,
        restart: bool = False,
        active: bool = False,
    ) -> None:
        if restart:
            try:
                self._proxmox.start_vm(vm_id)
            except Exception:
                logger.error("Failed to restart VM %d after aborted drain", vm_id, exc_info=True)
        try:
            pipe = self._redis.pipeline()
            pipe.srem(_POOL_DRAINING_KEY, worker_id)
            if active:
                pipe.srem(_POOL_IDLE_KEY, str(vm_id))
                pipe.hset(_POOL_ACTIVE_KEY, str(vm_id), str(time.time()))
            else:
                pipe.hdel(_POOL_ACTIVE_KEY, str(vm_id))
                pipe.sadd(_POOL_IDLE_KEY, str(vm_id))
            pipe.execute()
        except Exception:
            logger.error("Failed to restore VM %d after aborted drain", vm_id, exc_info=True)

    def _stop_vm(self, vm_id: int) -> bool:
        """Stop a worker VM without touching Redis tracking."""
        try:
            is_template = self._is_template_vmid(vm_id)
        except Exception:
            logger.error("Refusing to stop VM %d: template identity is unavailable", vm_id)
            return False
        if is_template:
            logger.error("Refusing to stop template VM %d through a worker lifecycle path", vm_id)
            return False
        if not self._is_destroyable_vm_id(vm_id):
            logger.error(
                "Refusing to stop VM %d: outside configured worker VMID range "
                "[%s, %s]. This is a safety guard against a poisoned done-key or "
                "misrouted lifecycle action targeting the orchestrator/template/unrelated VM.",
                vm_id,
                self._pool.vm_id_start,
                self._pool.vm_id_end or "open",
            )
            return False
        try:
            self._proxmox.stop_vm(vm_id)
            # Brief wait for VM to stop before destroying
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                try:
                    if self._proxmox.get_vm_status(vm_id) == "stopped":
                        return True
                except Exception:
                    return False
                time.sleep(1)
            try:
                if self._proxmox.get_vm_status(vm_id) == "stopped":
                    return True
            except Exception:
                pass
            logger.error("VM %d did not reach stopped state before timeout", vm_id)
            return False
        except Exception:
            logger.warning("Failed to stop VM %d (may already be stopped)", vm_id)
            try:
                return self._proxmox.get_vm_status(vm_id) == "stopped"
            except Exception:
                return False

    def _destroy_vm(self, vm_id: int) -> bool:
        """Stop and destroy a VM, remove from tracking sets.

        A hard VMID-range guard runs first: only VMIDs inside the configured
        worker range ``[pool.vm_id_start, pool.vm_id_end]`` may ever be touched
        in Proxmox. This is defence-in-depth against a poisoned/garbled
        ``pool:done:*`` key (or any other caller) resolving to the orchestrator
        VM, a template, or some unrelated VMID. Out-of-range requests are
        logged and dropped (no Proxmox call). Every destroy path funnels
        through here, so this one check covers them all.
        """
        if not self._stop_vm(vm_id):
            return False

        return self._destroy_stopped_vm(vm_id)

    def _destroy_stopped_vm(self, vm_id: int) -> bool:
        """Destroy an already-stopped, range-validated worker and clean Redis."""

        destroyed = True
        try:
            self._proxmox.destroy_vm(vm_id)
        except Exception:
            destroyed = False
            logger.error("Failed to destroy VM %d", vm_id, exc_info=True)

        if not destroyed:
            # Retain tracking and lifecycle markers so the same generation is
            # retried. Clearing them after a failed Proxmox destroy can make the
            # still-live VM look reusable.
            return False

        # Destruction is not complete until durable generation markers are gone.
        worker_id = self._vm_id_to_worker_id(vm_id)
        done_key = f"{_POOL_DONE_PREFIX}{worker_id}"
        heartbeat_key = f"{_WORKER_HEARTBEAT_PREFIX}{worker_id}"
        try:
            pipe = self._redis.pipeline()
            pipe.srem(_POOL_IDLE_KEY, str(vm_id))
            pipe.hdel(_POOL_ACTIVE_KEY, str(vm_id))
            pipe.srem(_POOL_DRAINING_KEY, worker_id)
            pipe.srem(_POOL_PROVISIONING_KEY, str(vm_id))
            pipe.srem(_POOL_AMBIGUOUS_CLONES_KEY, str(vm_id))
            pipe.delete(done_key)
            pipe.delete(heartbeat_key)
            pipe.execute()
            if (
                self._redis.exists(done_key)
                or self._redis.exists(heartbeat_key)
                or self._redis.sismember(_POOL_DRAINING_KEY, worker_id)
                or self._redis.sismember(_POOL_PROVISIONING_KEY, str(vm_id))
                or self._redis.sismember(_POOL_AMBIGUOUS_CLONES_KEY, str(vm_id))
            ):
                raise RuntimeError("lifecycle markers remain after cleanup")
        except Exception:
            logger.error(
                "VM %d was destroyed but Redis generation cleanup was not verified; "
                "refusing to report lifecycle success",
                vm_id,
                exc_info=True,
            )
            return False
        self._owned_provisioning_vmids.discard(vm_id)
        self._allocated_vmids.discard(vm_id)
        return True

    def _read_template_pointer(self) -> int | None:
        """Return the configured template VMID without checking it exists.

        Read order each cycle:
          1. Redis pointer ``pool:current_template_vmid`` (set by ``rebake``).
          2. Single-VMID fallback ``pool.template_vm_id`` (legacy/zero-config).
             When used, the pointer is also initialised so subsequent reads
             come from Redis without restart.

        Returns ``None`` if neither is configured.
        """
        try:
            raw = self._redis.get(_POOL_CURRENT_TEMPLATE_KEY)
        except Exception as exc:
            raise RuntimeError(
                "Cannot safely resolve the active template: Redis pointer read failed"
            ) from exc

        if raw:
            try:
                return int(raw)
            except (TypeError, ValueError) as exc:
                raise RuntimeError(
                    f"Cannot safely resolve the active template: invalid Redis pointer {raw!r}"
                ) from exc

        fallback = self._pool.template_vm_id
        if not fallback:
            return None
        # Initialise the missing pointer from config so future reads come from
        # Redis. The candidate is still validated against Proxmox below.
        try:
            self._redis.set_value(_POOL_CURRENT_TEMPLATE_KEY, str(fallback))
        except Exception:
            logger.warning("Failed to initialise template pointer in Redis", exc_info=True)
        return fallback

    def _resolve_template_vmid(self) -> int | None:
        """Return the VMID of a worker template that actually exists in Proxmox.

        Reads the configured template VMID (Redis pointer, then config) and
        validates it is an actual Proxmox template in the configured template
        range (or the explicit legacy template VMID). A dangling or unsafe
        pointer would otherwise
        make every clone fail with ``unable to find configuration file`` and
        drive an endless clone-retry storm.

        When the configured template is missing, search
        ``template_vmid_range`` for a live template and repoint the Redis
        pointer so the recovery is permanent (survives restarts and stops
        the storm after a single cycle).

        Returns ``None`` if no valid template can be found.
        """
        candidate = self._read_template_pointer()
        if candidate is None:
            replacement = self._find_replacement_template()
            if replacement is None:
                return None
            logger.info(
                "No active template pointer configured; using live template VM %d "
                "from pool.template_vmid_range",
                replacement,
            )
            self._repoint_template_pointer(replacement)
            self._template_recovery_failed = False
            return replacement

        try:
            all_vms = self._proxmox.list_vms()
        except Exception:
            logger.warning(
                "Could not verify template VM %d against Proxmox inventory; refusing to clone",
                candidate,
                exc_info=True,
            )
            return None

        candidate_info = next(
            (vm for vm in all_vms if str(vm.get("vmid", "")) == str(candidate)),
            None,
        )
        if (
            self._is_configured_template_id(candidate)
            and candidate_info is not None
            and self._is_proxmox_template(candidate_info)
        ):
            self._template_recovery_failed = False
            return candidate

        replacement = self._find_replacement_template(exclude=candidate)
        if replacement is None:
            if not self._template_recovery_failed:
                logger.error(
                    "Active template VM %d is not a valid configured Proxmox template and no "
                    "replacement template was found — the pool cannot grow "
                    "until a template is baked (run 'orcest fleet rebake')",
                    candidate,
                )
                self._template_recovery_failed = True
            return None

        logger.error(
            "Active template VM %d is not a valid configured Proxmox template; recovering by "
            "repointing the active template to live template VM %d",
            candidate,
            replacement,
        )
        self._repoint_template_pointer(replacement)
        self._template_recovery_failed = False
        return replacement

    def _find_replacement_template(self, exclude: int | None = None) -> int | None:
        """Find a live worker template to recover a dangling template pointer.

        Searches Proxmox for template VMs inside ``template_vmid_range`` (or
        matching the single-VMID config fallback). Recovery is allowed only
        when exactly one candidate exists. Rebake allocates the lowest free
        VMID, so numeric order is not generation order; choosing among multiple
        candidates could silently roll the worker pool backward.
        """
        try:
            rng = self._pool.template_range()
        except ValueError:
            rng = None
        cfg_id = self._pool.template_vm_id
        if rng is None and not cfg_id:
            return None

        try:
            all_vms = self._proxmox.list_vms()
        except Exception:
            logger.warning(
                "Failed to list VMs while searching for a replacement template",
                exc_info=True,
            )
            return None

        candidates: list[int] = []
        for vm in all_vms:
            try:
                raw_vmid = vm.get("vmid")
                if raw_vmid is None:
                    continue
                vmid = int(raw_vmid)
            except (TypeError, ValueError):
                continue
            if exclude is not None and vmid == exclude:
                continue
            if not self._is_proxmox_template(vm):
                continue
            in_range = rng is not None and rng[0] <= vmid <= rng[1]
            if in_range or (cfg_id and vmid == cfg_id):
                candidates.append(vmid)

        if not candidates:
            return None
        if len(candidates) > 1:
            logger.error(
                "Cannot recover the active template pointer: multiple live candidates %s "
                "exist. Restore pool:current_template_vmid explicitly or run a coordinated "
                "rebake; numeric VMID order does not identify the newest generation.",
                sorted(candidates),
            )
            return None
        return candidates[0]

    def _is_configured_template_id(self, vm_id: int) -> bool:
        """Return whether *vm_id* is authorized to be used as a template."""
        try:
            rng = self._pool.template_range()
        except ValueError:
            rng = None
        in_range = rng is not None and rng[0] <= vm_id <= rng[1]
        return in_range or bool(self._pool.template_vm_id == vm_id and vm_id != 0)

    def _repoint_template_pointer(self, vm_id: int) -> None:
        """Repoint the Redis template pointer so a recovery survives restarts."""
        try:
            self._redis.set_value(_POOL_CURRENT_TEMPLATE_KEY, str(vm_id))
        except Exception:
            logger.warning(
                "Failed to repoint template pointer to VM %d in Redis",
                vm_id,
                exc_info=True,
            )

    def _clone_and_boot(
        self,
        *,
        new_id: int | None = None,
        profile: WorkerProfileConfig | None = None,
    ) -> int | None:
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

        new_id = self._next_vm_id(preferred=new_id)
        name = self._vm_id_to_worker_id(new_id)
        profile = profile or self._pool.worker_profile_for_vmid(new_id)

        # VMIDs are intentionally reused. A durable done key or drain member
        # from the prior generation would make the next reconciliation destroy
        # (or immediately stop) this fresh clone, so lifecycle cleanup is a
        # required allocation commit point rather than best-effort hygiene.
        self._prepare_worker_generation(new_id)

        # Reserve the VMID as ambiguous before the API call. If clone_vm raises,
        # its outcome is unknowable: the request may have failed before create,
        # timed out after create, or raced another owner. The durable quarantine
        # prevents both reuse and orphan cleanup from guessing based on a name.
        self._reserve_clone_attempt(new_id)

        logger.info(
            "Cloning VM %d from template %d (name=%s, backend=%s, runner=%s, mode=%s)",
            new_id,
            template_id,
            name,
            profile.backend,
            profile.runner_type,
            profile.runner_mode or "default",
        )

        try:
            self._proxmox.clone_vm(
                template_id=template_id,
                new_id=new_id,
                name=name,
                linked=True,
            )
        except Exception:
            logger.error(
                "Clone outcome for VM %d is ambiguous; quarantining the VMID for operator review",
                new_id,
                exc_info=True,
            )
            return None

        # clone_vm returned successfully, so ownership is now proven. Record it
        # durably before any provisioning step that may require cleanup.
        self._owned_provisioning_vmids.add(new_id)
        self._allocated_vmids.add(new_id)
        try:
            self._redis.sadd(_POOL_PROVISIONING_KEY, str(new_id))
            if not self._redis.sismember(_POOL_PROVISIONING_KEY, str(new_id)):
                raise RuntimeError("provisioning marker was not persisted")
            self._redis.srem(_POOL_AMBIGUOUS_CLONES_KEY, str(new_id))
        except Exception:
            logger.error(
                "Failed to durably record ownership of clone VM %d; destroying",
                new_id,
                exc_info=True,
            )
            self._destroy_vm(new_id)
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
            # C1: forward the Redis AUTH password to the clone so its worker can
            # authenticate to the now-password-protected Redis. The pool-manager
            # container receives ORCEST_REDIS_PASSWORD in its own environment via
            # the pool compose stack's --env-file/passthrough (see
            # ensure_pool_manager + docker-compose.pool.yml). Absent (dev/legacy
            # unauthenticated stack) -> empty string -> clone writes no .env,
            # preserving the old behaviour.
            userdata = render_clone_userdata(
                redis_host=self._config.orchestrator.host,
                worker_id=name,
                key_prefix=self._key_prefix,
                redis_password=os.environ.get("ORCEST_REDIS_PASSWORD", ""),
                worker_backend=profile.backend,
                worker_runner_type=profile.runner_type,
                worker_runner_mode=profile.runner_mode,
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
            pipe = self._redis.pipeline()
            pipe.sadd(_POOL_IDLE_KEY, str(new_id))
            pipe.srem(_POOL_PROVISIONING_KEY, str(new_id))
            pipe.srem(_POOL_AMBIGUOUS_CLONES_KEY, str(new_id))
            pipe.execute()
        except Exception:
            logger.error(
                "Failed to add VM %d to idle pool in Redis, destroying to avoid orphan",
                new_id,
                exc_info=True,
            )
            self._destroy_vm(new_id)
            return None

        self._owned_provisioning_vmids.discard(new_id)
        return new_id

    def _prepare_worker_generation(self, vm_id: int) -> None:
        """Clear and verify durable lifecycle state before reusing *vm_id*."""
        worker_id = self._vm_id_to_worker_id(vm_id)
        done_key = f"{_POOL_DONE_PREFIX}{worker_id}"
        heartbeat_key = f"{_WORKER_HEARTBEAT_PREFIX}{worker_id}"
        try:
            pipe = self._redis.pipeline()
            pipe.delete(done_key)
            pipe.srem(_POOL_DRAINING_KEY, worker_id)
            pipe.srem(_POOL_PROVISIONING_KEY, str(vm_id))
            pipe.srem(_POOL_AMBIGUOUS_CLONES_KEY, str(vm_id))
            pipe.delete(heartbeat_key)
            pipe.execute()
            if (
                self._redis.exists(done_key)
                or self._redis.exists(heartbeat_key)
                or self._redis.sismember(_POOL_DRAINING_KEY, worker_id)
                or self._redis.sismember(_POOL_PROVISIONING_KEY, str(vm_id))
                or self._redis.sismember(_POOL_AMBIGUOUS_CLONES_KEY, str(vm_id))
            ):
                raise RuntimeError("lifecycle markers remain after cleanup")
        except Exception as exc:
            raise RuntimeError(
                f"Refusing to reuse VMID {vm_id}: could not clear stale lifecycle markers"
            ) from exc

    def _reserve_clone_attempt(self, vm_id: int) -> None:
        """Durably quarantine a VMID before invoking the non-idempotent clone API."""
        try:
            self._redis.sadd(_POOL_AMBIGUOUS_CLONES_KEY, str(vm_id))
            if not self._redis.sismember(_POOL_AMBIGUOUS_CLONES_KEY, str(vm_id)):
                raise RuntimeError("ambiguous-clone marker was not persisted")
        except Exception as exc:
            raise RuntimeError(
                f"Refusing to clone VMID {vm_id}: could not persist clone-attempt quarantine"
            ) from exc

    def _retry_provisioning_cleanups(self) -> bool:
        """Retry cleanup of every clone known to have passed clone_vm().

        The Redis set makes cleanup retryable across process restarts. An
        in-memory mirror covers a transient Redis failure after clone success
        without ever upgrading an ambiguous clone result into owned state.
        Returns ``False`` while any cleanup remains so the pool does not create
        more VMs on top of an unresolved provisioning failure.
        """
        try:
            pending = {
                int(member)
                for member in self._redis.smembers(_POOL_PROVISIONING_KEY)
                if str(member).lstrip("-").isdigit()
            }
        except Exception:
            logger.error("Failed to read durable provisioning cleanup state", exc_info=True)
            return False
        pending.update(self._owned_provisioning_vmids)
        if not pending:
            return True

        try:
            inventory = self._proxmox.list_vms()
        except Exception:
            logger.error("Failed to list VMs for provisioning cleanup retry", exc_info=True)
            return False
        existing = {
            int(vm["vmid"]): vm
            for vm in inventory
            if vm.get("vmid") is not None and str(vm.get("vmid")).lstrip("-").isdigit()
        }

        complete = True
        for vm_id in sorted(pending):
            if not self._is_destroyable_vm_id(vm_id):
                logger.error(
                    "Refusing provisioning cleanup for VM %d outside the worker range",
                    vm_id,
                )
                complete = False
                continue
            vm_info = existing.get(vm_id)
            if vm_info is None:
                if not self._clear_destroyed_worker_state(vm_id):
                    complete = False
                continue
            if self._is_proxmox_template(vm_info):
                logger.error(
                    "Refusing to destroy template VM %d during provisioning cleanup", vm_id
                )
                complete = False
                continue
            if not self._destroy_vm(vm_id):
                complete = False
        return complete

    def _reconcile_ambiguous_clones(self) -> bool:
        """Release quarantined VMIDs only when inventory proves no VM exists.

        A present VM remains quarantined because a clone exception cannot prove
        who created it. This is deliberately operator-resolved; an absent VMID
        can be released automatically from an authoritative inventory snapshot.
        """
        try:
            raw_members = self._redis.smembers(_POOL_AMBIGUOUS_CLONES_KEY)
            ambiguous = {int(member) for member in raw_members if str(member).lstrip("-").isdigit()}
        except Exception:
            logger.error("Failed to read ambiguous clone quarantine state", exc_info=True)
            return False
        if not ambiguous:
            return True
        try:
            existing = {
                int(vm["vmid"])
                for vm in self._proxmox.list_vms()
                if vm.get("vmid") is not None and str(vm.get("vmid")).lstrip("-").isdigit()
            }
        except Exception:
            logger.error("Failed to verify ambiguous clone VMIDs against Proxmox", exc_info=True)
            return False

        complete = True
        for vm_id in sorted(ambiguous):
            if vm_id in existing:
                logger.error(
                    "Clone outcome for VM %d remains ambiguous; leaving it quarantined. "
                    "Inspect the VM in Proxmox, then remove it or explicitly clear Redis "
                    "member %s from %s",
                    vm_id,
                    vm_id,
                    _POOL_AMBIGUOUS_CLONES_KEY,
                )
                complete = False
                continue
            try:
                self._redis.srem(_POOL_AMBIGUOUS_CLONES_KEY, str(vm_id))
                if self._redis.sismember(_POOL_AMBIGUOUS_CLONES_KEY, str(vm_id)):
                    raise RuntimeError("quarantine marker remains")
                self._allocated_vmids.discard(vm_id)
            except Exception:
                logger.error(
                    "Failed to release absent ambiguous clone VMID %d", vm_id, exc_info=True
                )
                complete = False
        return complete

    def _clear_destroyed_worker_state(self, vm_id: int) -> bool:
        """Clear lifecycle state after inventory proves the VM is absent."""
        worker_id = self._vm_id_to_worker_id(vm_id)
        done_key = f"{_POOL_DONE_PREFIX}{worker_id}"
        heartbeat_key = f"{_WORKER_HEARTBEAT_PREFIX}{worker_id}"
        try:
            pipe = self._redis.pipeline()
            pipe.srem(_POOL_IDLE_KEY, str(vm_id))
            pipe.hdel(_POOL_ACTIVE_KEY, str(vm_id))
            pipe.srem(_POOL_DRAINING_KEY, worker_id)
            pipe.srem(_POOL_PROVISIONING_KEY, str(vm_id))
            pipe.srem(_POOL_AMBIGUOUS_CLONES_KEY, str(vm_id))
            pipe.delete(done_key)
            pipe.delete(heartbeat_key)
            pipe.execute()
            if (
                self._redis.exists(done_key)
                or self._redis.exists(heartbeat_key)
                or self._redis.sismember(_POOL_DRAINING_KEY, worker_id)
                or self._redis.sismember(_POOL_PROVISIONING_KEY, str(vm_id))
                or self._redis.sismember(_POOL_AMBIGUOUS_CLONES_KEY, str(vm_id))
            ):
                raise RuntimeError("lifecycle markers remain after cleanup")
        except Exception:
            logger.error("Failed to clear destroyed worker state for VM %d", vm_id, exc_info=True)
            return False
        self._owned_provisioning_vmids.discard(vm_id)
        self._allocated_vmids.discard(vm_id)
        return True

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
                # Fence the worker before inspecting its PEL or private recovery
                # state. Otherwise it can publish a late success/rotation while
                # the reaper concurrently publishes failure and ACKs its source.
                # A stopped VM also bounds billing while Redis recovery retries.
                if not self._stop_vm(vm_id):
                    logger.warning(
                        "VM %d exceeded max task duration but could not be stopped; "
                        "preserving Redis state for retry",
                        vm_id,
                    )
                    continue
                try:
                    coordinated = self._coordinate_reaped_vm(vm_id)
                except Exception:
                    logger.error(
                        "Failed to coordinate Redis state for reaped VM %d; "
                        "leaving VM active for retry",
                        vm_id,
                        exc_info=True,
                    )
                    coordinated = False
                if not coordinated:
                    logger.warning(
                        "VM %d exceeded max task duration but Redis recovery is incomplete; "
                        "leaving the fenced VM stopped and preserving Redis state for retry",
                        vm_id,
                    )
                    continue
                self._destroy_stopped_vm(vm_id)

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

        try:
            idle_members = self._redis.smembers(_POOL_IDLE_KEY)
            active_members = set(self._redis.hgetall(_POOL_ACTIVE_KEY).keys())
            provisioning_members = self._redis.smembers(_POOL_PROVISIONING_KEY)
            ambiguous_members = self._redis.smembers(_POOL_AMBIGUOUS_CLONES_KEY)
        except Exception:
            logger.warning("Failed to read Redis state for orphan reconciliation", exc_info=True)
            return
        tracked_vm_ids: set[int] = set()
        for member in idle_members | active_members | provisioning_members | ambiguous_members:
            try:
                tracked_vm_ids.add(int(member))
            except (ValueError, TypeError):
                continue

        for vm_info in proxmox_vms:
            vm_id = vm_info.get("vmid")
            if vm_id is None:
                continue
            vm_id = int(vm_id)

            # Redis tracking (including the pre-clone ambiguity quarantine)
            # is authoritative here. In particular, never turn an ambiguous
            # clone result into ownership by inspecting its deterministic name.
            if vm_id in tracked_vm_ids:
                continue

            # Defence in depth: never destroy a Proxmox template, period.
            # Proxmox marks templates with ``template: 1`` in the API; this
            # protects freshly-baked templates whose Redis pointer hasn't
            # been swapped yet (e.g. ``rebake`` interrupted between bake
            # and pointer SET) and any human-managed template in the range.
            if self._is_proxmox_template(vm_info):
                continue

            # Skip any template VMID (active or older blue/green generations
            # awaiting GC). Old templates with live linked clones must not be
            # touched here; they get cleaned up by `orcest fleet gc-templates`
            # once their clones churn out.
            try:
                if self._is_template_vmid(vm_id):
                    continue
            except Exception:
                logger.error(
                    "Template identity is unavailable; refusing orphan destruction",
                    exc_info=True,
                )
                return

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
                    self._allocated_vmids.discard(vm_id)
                    self._owned_provisioning_vmids.discard(vm_id)
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
                    self._allocated_vmids.discard(vm_id)
                    self._owned_provisioning_vmids.discard(vm_id)
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
            "Pool manager starting (target_size=%d, interval=%.1fs, layout=%s)",
            self._pool.size,
            interval,
            self._pool.worker_layout_signature(),
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
        streams, _complete = self._task_streams_with_discovery_status()
        return streams

    def _task_streams_with_discovery_status(self) -> tuple[tuple[str, ...], bool]:
        """Return task streams plus whether Redis discovery completed."""
        streams = {
            self._fq_task_stream("tasks:claude"),
            self._fq_task_stream("tasks:issue:claude"),
        }
        for backend in self._pool.worker_backends():
            streams.add(self._fq_task_stream(f"tasks:{backend}"))
            streams.add(self._fq_task_stream(f"tasks:issue:{backend}"))
        try:
            for key in self._redis.scan_iter(match="tasks:*"):
                if not self._is_task_stream_key(key):
                    continue
                fq_key = self._fq_task_stream(key)
                raw_type = self._redis.client.type(fq_key)
                key_type = raw_type.decode() if isinstance(raw_type, bytes) else str(raw_type)
                if key_type not in {
                    "none",
                    "string",
                    "list",
                    "set",
                    "zset",
                    "hash",
                    "stream",
                }:
                    raise RuntimeError(f"unexpected Redis TYPE output {key_type!r}")
                if key_type == "stream":
                    streams.add(fq_key)
        except Exception:
            logger.warning("Failed to discover task streams from Redis", exc_info=True)
            return tuple(sorted(streams)), False
        return tuple(sorted(streams)), True

    def _fq_task_stream(self, key: str) -> str:
        return f"{self._key_prefix}:{key}" if self._key_prefix else key

    @staticmethod
    def _is_task_stream_key(key: str) -> bool:
        """Return True for backend task streams, not arbitrary tasks:* keys."""
        parts = key.split(":")
        if len(parts) == 2:
            return parts[0] == "tasks" and bool(parts[1]) and parts[1] != "issue"
        if len(parts) == 3:
            return parts[0] == "tasks" and parts[1] == "issue" and bool(parts[2])
        return False

    def _consumers_with_pending(self) -> set[str]:
        """Names of consumer-group consumers that currently hold a pending entry.

        Read fresh from Redis each call. Used both to promote idle->active and,
        critically, to re-check just before draining an excess idle VM so a VM
        that claimed a task mid-reconcile is never destroyed.
        """
        names, _complete = self._consumers_with_pending_status()
        return names

    def _consumers_with_pending_status(self) -> tuple[set[str], bool]:
        """Return pending consumers and whether every stream read succeeded."""
        names: set[str] = set()
        streams, complete = self._task_streams_with_discovery_status()
        for stream_name in streams:
            try:
                groups = self._redis.xinfo_groups_raw(stream_name)
            except redis.ResponseError as exc:
                if not _is_missing_stream_or_group_error(exc):
                    complete = False
                continue
            except Exception:
                complete = False
                continue
            for group in groups:
                if group.get("name") != CONSUMER_GROUP:
                    continue
                try:
                    consumers = self._redis.xinfo_consumers_raw(stream_name, CONSUMER_GROUP)
                except redis.ResponseError as exc:
                    if not _is_missing_stream_or_group_error(exc):
                        complete = False
                    continue
                except Exception:
                    complete = False
                    continue
                for consumer in consumers:
                    pending_raw = consumer.get("pending")
                    if isinstance(pending_raw, bool) or not isinstance(
                        pending_raw, (int, str, bytes)
                    ):
                        complete = False
                        continue
                    try:
                        pending = int(pending_raw)
                    except (TypeError, ValueError):
                        complete = False
                        continue
                    if pending < 0:
                        complete = False
                        continue
                    if pending > 0:
                        name = str(consumer.get("name", ""))
                        if not name:
                            complete = False
                            continue
                        names.add(name)
        return names, complete

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

    def _is_destroyable_vm_id(self, vm_id: int) -> bool:
        """Return True only if *vm_id* is inside the configured worker range.

        Worker clones are allocated from ``[vm_id_start, vm_id_end]`` (with
        ``vm_id_end == 0`` meaning open-ended). Any VMID below the start, or
        above a configured end, must never be destroyed -- it could be the
        orchestrator VM, a template, or an unrelated guest. When the worker
        range is unconfigured (``vm_id_start <= 0``) nothing is destroyable:
        without a range we cannot prove a VMID is a worker clone.
        """
        return self._pool.contains_worker_vmid(vm_id)

    @staticmethod
    def _is_proxmox_template(vm_info: dict) -> bool:
        """Return True if Proxmox itself reports the VM as a template.

        Proxmox sets ``template: 1`` on VMs converted via ``qm template``.
        We never destroy these regardless of Redis tracking — a template
        with no live clones is harmless to leave alone (use
        ``orcest fleet gc-templates`` to remove explicitly).

        Accepts integer (1) or boolean (True) — Proxmox returns either
        depending on transport. Defaults to False on missing/unknown values.
        """
        flag = vm_info.get("template", 0)
        try:
            return int(flag) == 1
        except (TypeError, ValueError):
            return bool(flag)

    def _next_vm_id(self, preferred: int | None = None) -> int:
        """Allocate the next VM ID from the configured pool range.

        Scans existing orcest-worker-* VMs in Proxmox and picks the next
        ID starting from ``pool.vm_id_start``, skipping any that are
        already in use.
        """
        start = self._pool.vm_id_start
        end = self._pool.vm_id_end
        if end and start > end:
            raise RuntimeError(
                f"Invalid VM ID pool range: vm_id_start ({start}) is greater than vm_id_end ({end})"
            )

        existing: set[int] = set()
        try:
            for vm in self._proxmox.list_vms(name_prefix=_VM_NAME_PREFIX):
                vm_id = vm.get("vmid")
                if vm_id is not None:
                    existing.add(int(vm_id))
        except Exception as exc:
            raise RuntimeError(
                "Cannot allocate a worker VMID while the Proxmox VM inventory is unavailable"
            ) from exc

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
        for key in (_POOL_PROVISIONING_KEY, _POOL_AMBIGUOUS_CLONES_KEY):
            for member in self._redis.smembers(key):
                try:
                    existing.add(int(member))
                except (ValueError, TypeError):
                    pass
        existing.update(self._allocated_vmids)

        if preferred is not None:
            if not self._pool.contains_worker_vmid(preferred):
                raise RuntimeError(
                    f"Requested worker VMID {preferred} is outside the configured pool range"
                )
            if preferred in existing:
                raise RuntimeError(f"Requested worker VMID {preferred} is already in use")
            return preferred

        candidate = start
        while candidate in existing:
            candidate += 1
            if end and candidate > end:
                raise RuntimeError(
                    f"VM ID pool exhausted: all IDs in range {start}-{end} are in use"
                )
        if end and candidate > end:
            raise RuntimeError(f"VM ID pool exhausted: all IDs in range {start}-{end} are in use")
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

    # ── reap coordination (H2-conc) ──────────────────────────

    def _coordinate_reaped_vm(self, vm_id: int) -> bool:
        """Recover orchestrator-visible state for a force-reaped worker VM.

        Reads the reaped consumer's pending stream entries, reconstructs each
        Task, publishes a transient-FAILED result (so the orchestrator
        re-enqueues), clears its pending marker, then ACKs the entry and deletes
        the consumer to release its PEL slot. Result publication is the commit
        point: if it fails, the task is left pending for later recovery instead
        of being silently acknowledged and cleared.

        Returns True only when every stream was either empty or fully recovered.
        A Redis read/publish failure returns False so the caller can leave the VM
        tracking state intact and retry recovery later.
        """
        consumer = self._vm_id_to_worker_id(vm_id)
        task_streams, discovery_complete = self._task_streams_with_discovery_status()
        recovered_all = discovery_complete
        for fq_stream in task_streams:
            unrecovered_entries = False
            entries = self._read_consumer_pending(fq_stream, consumer)
            if entries is None:
                recovered_all = False
                continue
            for entry_id, fields in entries:
                try:
                    task = Task.from_dict(fields)
                except (KeyError, ValueError):
                    logger.warning(
                        "Reaped VM %d: malformed pending entry %s on %s; "
                        "routing redacted diagnostics before ACK",
                        vm_id,
                        entry_id,
                        fq_stream,
                    )
                    malformed_id = fields.get("id") or f"malformed:{entry_id}"
                    try:
                        publish_handoff_once(
                            self._redis,
                            self._fq_task_stream(DEAD_LETTER_STREAM),
                            fq_stream,
                            entry_id,
                            malformed_id,
                            safe_dead_letter_fields(
                                fields,
                                fq_stream,
                                entry_id,
                                "Malformed task payload",
                            ),
                            maxlen=_RESULT_MAXLEN,
                            marker_ttl_seconds=None,
                        )
                    except Exception:
                        logger.error(
                            "Reaped VM %d: failed to publish redacted malformed-task "
                            "diagnostics for entry %s on %s",
                            vm_id,
                            entry_id,
                            fq_stream,
                            exc_info=True,
                        )
                        unrecovered_entries = True
                        continue
                    if not self._safe_xack(fq_stream, entry_id):
                        unrecovered_entries = True
                    else:
                        try:
                            self._redis.client.delete(
                                handoff_marker_key(
                                    self._fq_task_stream(DEAD_LETTER_STREAM),
                                    fq_stream,
                                    entry_id,
                                    malformed_id,
                                )
                            )
                        except Exception:
                            logger.warning(
                                "Reaped malformed entry %s: failed to delete completed "
                                "nonsecret handoff receipt",
                                entry_id,
                                exc_info=True,
                            )
                    continue
                result_stream = (
                    f"{task.key_prefix}:{_RESULTS_STREAM}"
                    if task.key_prefix
                    else self._fq_task_stream(_RESULTS_STREAM)
                )
                credential_recovery = recover_credential_checkpoint(
                    self._redis,
                    task,
                    result_stream,
                    fq_stream,
                    entry_id,
                    self._fq_task_stream(DEAD_LETTER_STREAM),
                    logger,
                    maxlen=_RESULT_MAXLEN,
                )
                if credential_recovery is CredentialRecoveryOutcome.BLOCKED:
                    logger.error(
                        "Reaped VM %d: private credential recovery for task %s is "
                        "incomplete; refusing generic failure/ACK",
                        vm_id,
                        task.id,
                    )
                    unrecovered_entries = True
                    continue
                if credential_recovery is CredentialRecoveryOutcome.RECOVERED:
                    # The exact credential result and source XACK committed
                    # together.  Its orchestrator pending marker intentionally
                    # remains for normal result consumption.
                    continue
                already_resolved = self._task_already_resolved(task)
                if already_resolved is None:
                    unrecovered_entries = True
                    continue
                if already_resolved:
                    logger.info(
                        "Reaped VM %d: task %s already has a result or no longer owns "
                        "its pending marker; ACKing stale PEL entry",
                        vm_id,
                        task.id,
                    )
                    if not self._safe_xack(fq_stream, entry_id):
                        unrecovered_entries = True
                    continue
                if not self._publish_reaped_failure(task):
                    unrecovered_entries = True
                    continue
                self._clear_reaped_pending_marker(task)
                if not self._safe_xack(fq_stream, entry_id):
                    unrecovered_entries = True
            if unrecovered_entries:
                logger.warning(
                    "Reaped VM %d: leaving consumer %s on %s because one or more "
                    "pending entries could not publish a recovery result",
                    vm_id,
                    consumer,
                    fq_stream,
                )
                recovered_all = False
                continue
            # Release the now-empty consumer so its PEL slot does not linger.
            try:
                self._redis.client.xgroup_delconsumer(fq_stream, CONSUMER_GROUP, consumer)
            except Exception:
                logger.warning(
                    "Reaped VM %d: failed to delete consumer %s on %s",
                    vm_id,
                    consumer,
                    fq_stream,
                    exc_info=True,
                )
        return recovered_all

    def _read_consumer_pending(
        self, fq_stream: str, consumer: str
    ) -> list[tuple[str, dict[str, str]]] | None:
        """Return ``[(entry_id, fields), ...]`` currently pending for *consumer*."""
        try:
            groups = self._redis.xinfo_groups_raw(fq_stream)
        except redis.ResponseError as exc:
            if _is_missing_stream_or_group_error(exc):
                return []
            logger.warning(
                "Failed to read consumer groups on %s",
                fq_stream,
                exc_info=True,
            )
            return None
        except Exception:
            logger.warning(
                "Failed to read consumer groups on %s",
                fq_stream,
                exc_info=True,
            )
            return None
        if isinstance(groups, list) and not any(
            group.get("name") == CONSUMER_GROUP for group in groups
        ):
            return []

        pending: list[dict[str, Any]] = []
        start_id = "-"
        while True:
            try:
                batch = cast(
                    Any,
                    self._redis.client.xpending_range(
                        fq_stream,
                        CONSUMER_GROUP,
                        min=start_id,
                        max="+",
                        count=_PENDING_READ_BATCH_SIZE,
                        consumername=consumer,
                    ),
                )
            except redis.ResponseError as exc:
                if _is_missing_stream_or_group_error(exc):
                    return []
                logger.warning(
                    "Failed to read pending entries for consumer %s on %s",
                    consumer,
                    fq_stream,
                    exc_info=True,
                )
                return None
            except Exception:
                logger.warning(
                    "Failed to read pending entries for consumer %s on %s",
                    consumer,
                    fq_stream,
                    exc_info=True,
                )
                return None
            # Guard against non-list responses (e.g. a MagicMock in unit tests):
            # a truthy-but-non-list would otherwise iterate into garbage.
            if not isinstance(batch, list):
                return []
            if not batch:
                break
            pending.extend(batch)
            if len(batch) < _PENDING_READ_BATCH_SIZE:
                break
            last_id = batch[-1].get("message_id")
            if last_id is None:
                break
            start_id = f"({last_id}"

        out: list[tuple[str, dict[str, str]]] = []
        for p in pending:
            raw_entry_id = p.get("message_id")
            if raw_entry_id is None:
                continue
            entry_id = str(raw_entry_id)
            try:
                rng = cast(
                    Any,
                    self._redis.client.xrange(fq_stream, min=entry_id, max=entry_id),
                )
            except Exception:
                logger.warning(
                    "Failed to read pending entry %s for consumer %s on %s",
                    entry_id,
                    consumer,
                    fq_stream,
                    exc_info=True,
                )
                return None
            raw_fields = rng[0][1] if rng else {}
            if not isinstance(raw_fields, dict):
                return None
            fields = {str(key): str(value) for key, value in raw_fields.items()}
            out.append((entry_id, fields))
        return out

    def _clear_reaped_pending_marker(self, task: Task) -> None:
        """Clear the pending marker for *task* using its own key_prefix."""
        try:
            if task.key_prefix:
                project_redis = RedisClient.from_client(
                    self._redis.client, key_prefix=task.key_prefix
                )
            else:
                project_redis = self._redis
            clear_pending_task_if_matches(
                project_redis, task.repo, task.resource_type, task.resource_id, task.id
            )
        except Exception:
            logger.warning(
                "Reaped task %s: failed to clear pending marker for %s #%s",
                task.id,
                task.resource_type,
                task.resource_id,
                exc_info=True,
            )

    def _task_already_resolved(self, task: Task) -> bool | None:
        """Return True when reaper recovery would duplicate completed work."""
        result_state = self._task_result_already_published(task)
        if result_state is None:
            return None
        if result_state:
            return True
        marker_state = self._pending_marker_state(task)
        if marker_state == "unknown":
            return None
        # A different live task proves this PEL entry stale. A missing marker
        # does not prove completion: publish recovery so attempt accounting is
        # repaired instead of silently ACKing lost work.
        return marker_state == "different"

    def _task_result_already_published(self, task: Task) -> bool | None:
        try:
            if task.key_prefix:
                fq_results = f"{task.key_prefix}:{_RESULTS_STREAM}"
                entries = cast(
                    Any,
                    self._redis.client.xrevrange(fq_results, count=_RESULT_MAXLEN),
                )
            else:
                entries = self._redis.xrevrange(_RESULTS_STREAM, count=_RESULT_MAXLEN)
        except Exception:
            logger.warning(
                "Reaped task %s: failed to inspect results stream before recovery",
                task.id,
                exc_info=True,
            )
            return None

        for _entry_id, fields in entries:
            if str(fields.get("task_id", "")) == task.id:
                return True
        return False

    def _pending_marker_state(self, task: Task) -> str:
        key = make_pending_task_key(task.repo, task.resource_type, task.resource_id)
        try:
            if task.key_prefix:
                raw = self._redis.get_raw(f"{task.key_prefix}:{key}")
            else:
                raw = self._redis.get(key)
        except Exception:
            logger.warning(
                "Reaped task %s: failed to inspect pending marker before recovery",
                task.id,
                exc_info=True,
            )
            return "unknown"

        if raw is not None and not isinstance(raw, str):
            return "unknown"
        metadata = parse_pending_task_metadata(raw)
        if metadata is None:
            return "missing"
        return "matches" if metadata.task_id == task.id else "different"

    def _publish_reaped_failure(self, task: Task) -> bool:
        """Publish a transient-FAILED result so the orchestrator re-enqueues."""
        result = TaskResult(
            task_id=task.id,
            worker_id=_REAPER_WORKER_ID,
            status=ResultStatus.FAILED,
            branch=task.branch,
            summary=(
                f"{TRANSIENT_SUMMARY_PREFIX}Worker VM exceeded max task duration "
                "and was force-reaped by the pool manager; task not completed."
            ),
            duration_seconds=0,
            resource_type=task.resource_type,
            resource_id=task.resource_id,
            repo=task.repo,
            snapshot_head_sha=task.snapshot_head_sha,
            decision_reason=task.decision_reason,
            snapshot_failed_checks=task.snapshot_failed_checks,
            snapshot_review_thread_ids=task.snapshot_review_thread_ids,
            snapshot_review_thread_fingerprints=task.snapshot_review_thread_fingerprints,
        )
        try:
            if task.key_prefix:
                fq_results = f"{task.key_prefix}:{_RESULTS_STREAM}"
                self._redis.xadd_capped_raw(fq_results, result.to_dict(), maxlen=_RESULT_MAXLEN)
            else:
                self._redis.xadd_capped(_RESULTS_STREAM, result.to_dict(), maxlen=_RESULT_MAXLEN)
        except Exception:
            logger.warning(
                "Reaped task %s: failed to publish transient-FAILED result",
                task.id,
                exc_info=True,
            )
            return False
        return True

    def _safe_xack(self, fq_stream: str, entry_id: str) -> bool:
        try:
            self._redis.xack_raw(fq_stream, CONSUMER_GROUP, entry_id)
            return True
        except Exception:
            pending = source_entry_pending_state(self._redis, fq_stream, entry_id)
            if pending is False:
                logger.warning(
                    "ACK response was ambiguous for reaped entry %s on %s, but exact "
                    "XPENDING confirms it is terminal",
                    entry_id,
                    fq_stream,
                )
                return True
            logger.warning(
                "Failed to ACK reaped entry %s on %s; exact pending state is %s",
                entry_id,
                fq_stream,
                "present" if pending else "unknown",
                exc_info=True,
            )
            return False

    # ── orphan-PEL sweeper (H3-conc) ─────────────────────────

    def _sweep_orphan_pel(self) -> None:
        """Recover or clean consumers whose worker VM is no longer live.

        A dead ephemeral worker can leave its consumer (orcest-worker-<vmid>)
        registered with pending entries. Redis ties those entries to the
        consumer's PEL until they are explicitly ACKed or claimed; deleting a
        consumer with pending entries discards that recovery handle. For
        PEL-holding consumers we use the same coordinated reaping path as VM
        destruction, which publishes transient FAILED results and ACKs entries
        before removing the consumer. Empty consumers are safe to delete
        directly.

        We only act on a consumer that is PROVABLY dead: its name parses to a
        worker VMID, that VMID is absent from pool:idle and pool:active, and it
        is not an existing Proxmox worker VM.
        """
        live_vm_ids = self._live_vm_ids()
        if live_vm_ids is None:
            return
        recovery_attempted_vm_ids: set[int] = set()
        for fq_stream in self._task_streams():
            try:
                consumers = self._redis.xinfo_consumers_raw(fq_stream, CONSUMER_GROUP)
            except Exception:
                continue
            for consumer in consumers:
                name = str(consumer.get("name", ""))
                pending = int(consumer.get("pending", 0) or 0)
                vm_id = self._worker_id_to_vm_id(name)
                if vm_id is None:
                    # Not a worker consumer (e.g. orchestrator/results consumer)
                    continue
                if vm_id in live_vm_ids:
                    continue
                if pending > 0:
                    if vm_id in recovery_attempted_vm_ids:
                        continue
                    recovery_attempted_vm_ids.add(vm_id)
                    logger.warning(
                        "Orphan PEL: consumer %s (VM %d) has %d pending entries but the "
                        "VM is not live; recovering pending work before deleting consumer",
                        name,
                        vm_id,
                        pending,
                    )
                    if not self._coordinate_reaped_vm(vm_id):
                        logger.warning(
                            "Orphan PEL: recovery for consumer %s (VM %d) is incomplete; "
                            "leaving pending entries attached to the consumer",
                            name,
                            vm_id,
                        )
                    continue
                logger.warning(
                    "Orphan PEL: deleting empty consumer %s (VM %d) because the VM is not live",
                    name,
                    vm_id,
                )
                try:
                    self._redis.delconsumer_raw(fq_stream, CONSUMER_GROUP, name)
                except Exception:
                    logger.error(
                        "Failed to delete orphan consumer %s on %s",
                        name,
                        fq_stream,
                        exc_info=True,
                    )

    def _live_vm_ids(self) -> set[int] | None:
        """VMIDs currently tracked as idle/active and present in Proxmox.

        Returns None when the Proxmox VM listing is unavailable. In that case
        callers cannot prove a missing VMID is dead and must skip destructive
        orphan cleanup.
        """
        live: set[int] = set()
        for member in self._redis.smembers(_POOL_IDLE_KEY):
            try:
                live.add(int(member))
            except (ValueError, TypeError):
                pass
        for member in self._redis.hgetall(_POOL_ACTIVE_KEY):
            try:
                live.add(int(member))
            except (ValueError, TypeError):
                pass
        try:
            for vm in self._proxmox.list_vms(name_prefix=_VM_NAME_PREFIX):
                raw = vm.get("vmid")
                if raw is not None:
                    live.add(int(raw))
        except Exception:
            logger.warning("Orphan-PEL sweep: failed to list Proxmox VMs", exc_info=True)
            return None
        return live
