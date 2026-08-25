"""Warm pool manager for ephemeral worker VMs.

Maintains a target number of pre-booted worker VMs that process one task
each, then get destroyed and replaced. Uses the Proxmox API for VM lifecycle
and Redis for coordination with workers.

IMPORTANT: Only one pool manager instance should run at a time. The VMID
allocation (next_free_vmid + clone_vm) is not atomic, so concurrent
instances could clash.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
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
from orcest.shared.events import EventPublisher, make_event
from orcest.shared.models import (
    CONSUMER_GROUP,
    DEAD_LETTER_STREAM,
    TRANSIENT_SUMMARY_PREFIX,
    ResultStatus,
    Task,
    TaskResult,
    task_stream_name,
)
from orcest.shared.provider_stream_health import (
    ProviderStreamHealth,
    ProviderStreamHealthTracker,
    stream_health_snapshot_key,
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
_POOL_WRITE_HEALTH_KEY = "pool:write-health"
_WORKER_HEARTBEAT_PREFIX = "workers:heartbeat:"
# Workers can block on the issue stream for up to five seconds. A drain lease
# must be visible for longer than that before the final PEL check.
_DRAIN_QUIESCE_SECONDS = 5.25
# The post-stop pending check is a drain's destruction commit point. Its
# conservative failure branch restarts the VM and files it into pool:active —
# a state nothing ever transitions back to idle — so a single transient Redis
# error there would mislabel a healthy idle VM until the health check
# force-destroys it as a phantom hung task. Retry briefly before taking that
# branch.
_POST_STOP_PENDING_CHECK_ATTEMPTS = 3
_POST_STOP_PENDING_CHECK_RETRY_SECONDS = 1.0
# Results stream + cap, mirroring worker/loop.py so the reaper writes a
# transient-FAILED result to the same place the orchestrator reads.
_RESULTS_STREAM = "results"
_RESULT_MAXLEN = 20000
# Fixed worker_id stamped on reaper-published results (operator-facing).
_REAPER_WORKER_ID = "pool-manager-reaper"
# Honest per-call-site reasons for the net.orcest.task.reaped event's
# data.reason field. REAP_REASON_CEILING, REAP_REASON_NEEDS_REAP and
# REAP_REASON_ACTIVITY_STALE are the three _health_check destroy paths (spec
# §6, activity-aware reaper); the remaining reasons recover Redis state for
# VMs destroyed for unrelated reasons and must not be reported as any of
# those three.
REAP_REASON_CEILING = "ceiling"
REAP_REASON_NEEDS_REAP = "needs_reap"
REAP_REASON_ACTIVITY_STALE = "activity_stale"
REAP_REASON_DONE_CLEANUP = "done_cleanup"
REAP_REASON_DRAIN_RACE = "drain_race"
REAP_REASON_ORPHAN_PEL = "orphan_pel"
# Pointer naming the active worker template VMID. Set by `orcest fleet rebake`
# (or initialised from `pool.template_vm_id` on first run for backward compat).
_POOL_CURRENT_TEMPLATE_KEY = "pool:current_template_vmid"
_PENDING_READ_BATCH_SIZE = 100
_IDLE_HEARTBEAT_DWELL_SECONDS = 300.0
_IDLE_LIVENESS_BREAKER_WINDOW_SECONDS = 15 * 60.0
_IDLE_LIVENESS_BREAKER_LIMIT = 2
# Global (cross-project, unprefixed) hash written by the worker-side liveness
# tracker (see worker/liveness_tracker.py's _write_activity_record). Read here
# with hgetall_raw -- never auto-prefixed -- to land in the same keyspace.
_ACTIVITY_KEY_PREFIX = "workers:activity:"

# Global (cross-project, unprefixed) canonical per-stream health snapshots
# (issue #613, extended by #639). PoolManager is the single writer; keys are
# ``provider-stream-health:{provider}:pr`` and ``...:issue`` via
# ``stream_health_snapshot_key``. ``orcest status`` and the live dashboard
# only ever consume those keys, never recompute health.
# TTL comfortably longer than one reconcile interval so a snapshot survives
# normal polling gaps but expires (rather than lying stale forever) if the
# pool manager stops publishing entirely.
_STREAM_HEALTH_TTL_SECONDS = 900

# VM naming convention
_VM_NAME_PREFIX = "orcest-worker-"


@dataclass(frozen=True)
class ReapFence:
    vm_id: int
    reason: str
    killed_at_unix: float
    elapsed_at_kill_seconds: float


@dataclass(frozen=True)
class _StopVmOutcome:
    stopped: bool
    confirmed_transition: bool


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
        self._reap_fences: dict[int, ReapFence] = {}
        self._idle_missing_heartbeat_since: dict[int, float] = {}
        self._idle_liveness_write_healthy_since: float | None = None
        self._idle_liveness_breaker_events: dict[str, list[float]] = {}
        self._idle_liveness_breaker_open: dict[str, bool] = {}
        self._stream_health_tracker = ProviderStreamHealthTracker(
            dwell_seconds=float(self._pool.stream_health_dwell_seconds)
        )
        # EventPublisher instances, cached per project key_prefix ("default"
        # for the pool manager's own prefix). A fresh EventPublisher per call
        # would reset its decimated-error counter every time, defeating the
        # 1/10/100/1000-backoff log suppression in a sustained-failure run.
        self._event_publishers: dict[str, EventPublisher] = {}

    def reconcile(self) -> None:
        """Single reconciliation pass.

        Checks for done workers, detects active, replaces VMs, runs health
        checks, and cleans up stale Redis entries.
        """
        try:
            self._check_done_workers()
            self._detect_active_workers()
            # Both reconcilers report the VMIDs they could not resolve rather
            # than a global "all clear" flag. A single unresolved VMID must
            # only cost its own slot: gating the whole refill on it drains the
            # fleet to zero while _check_done_workers keeps retiring finished
            # workers. ``None`` means the state itself is unknown (Redis or
            # Proxmox read failure), which is the only case that still
            # suppresses cloning entirely.
            ambiguous_blocked = self._reconcile_ambiguous_clones()
            provisioning_blocked = self._retry_provisioning_cleanups()
            self._replace_idle_workers_missing_heartbeat()
            if ambiguous_blocked is not None and provisioning_blocked is not None:
                self._fill_pool(blocked_vmids=ambiguous_blocked | provisioning_blocked)
            self._health_check()
            self._reconcile_stale_redis()
            self._sweep_orphan_pel()
            self._check_stream_health()
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
            if not self._coordinate_reaped_vm(vm_id, reason=REAP_REASON_DONE_CLEANUP):
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

    def _replace_idle_workers_missing_heartbeat(self) -> None:
        """Replace idle VMs whose worker liveness heartbeat never appears.

        The decision is gated by a SET EX sentinel in the same Redis keyspace
        (auto-prefixed) and command class as worker heartbeat writes. If that
        write class is failing, heartbeat absence is unknown and the complete
        dwell restarts after recovery.
        """
        now = time.time()
        try:
            self._redis.set_ex(_POOL_WRITE_HEALTH_KEY, str(int(now)), ttl=600)
        except Exception:
            self._reset_idle_liveness_dwell(
                "Redis heartbeat-class write failed; idle heartbeat absence is unknown"
            )
            return

        if self._idle_liveness_write_healthy_since is None:
            self._idle_liveness_write_healthy_since = now
            logger.info("Redis heartbeat-class writes recovered; idle heartbeat dwell restarted")

        try:
            idle_members = self._redis.smembers(_POOL_IDLE_KEY)
        except Exception:
            logger.warning(
                "Failed to read idle pool state; skipping idle heartbeat liveness check",
                exc_info=True,
            )
            return

        idle_vmids: set[int] = set()
        for member in idle_members:
            try:
                idle_vmids.add(int(member))
            except (ValueError, TypeError):
                continue

        for vm_id in set(self._idle_missing_heartbeat_since) - idle_vmids:
            logger.info(
                "VM %d left pool:idle; clearing missing-heartbeat dwell state",
                vm_id,
            )
            self._idle_missing_heartbeat_since.pop(vm_id, None)

        self._refresh_idle_liveness_breakers(now)

        write_dwell = now - self._idle_liveness_write_healthy_since
        for vm_id in sorted(idle_vmids):
            if not self._pool.contains_worker_vmid(vm_id):
                logger.warning(
                    "Skipping idle heartbeat liveness for VM %d: outside configured "
                    "worker VMID range [%s, %s]",
                    vm_id,
                    self._pool.vm_id_start,
                    self._pool.vm_id_end or "open",
                )
                self._idle_missing_heartbeat_since.pop(vm_id, None)
                continue

            worker_id = self._vm_id_to_worker_id(vm_id)
            heartbeat_present = self._worker_heartbeat_present(worker_id)
            if heartbeat_present is None:
                logger.warning(
                    "heartbeat read failed for idle VM %d; absence is unknown; "
                    "leaving other idle VMs' dwell state intact",
                    vm_id,
                )
                continue
            if heartbeat_present:
                if vm_id in self._idle_missing_heartbeat_since:
                    logger.info(
                        "VM %d published worker heartbeat; clearing missing-heartbeat dwell state",
                        vm_id,
                    )
                self._idle_missing_heartbeat_since.pop(vm_id, None)
                continue

            missing_since = self._idle_missing_heartbeat_since.setdefault(vm_id, now)
            missing_dwell = now - missing_since
            if (
                write_dwell < _IDLE_HEARTBEAT_DWELL_SECONDS
                or missing_dwell < _IDLE_HEARTBEAT_DWELL_SECONDS
            ):
                continue

            profile_key = self._idle_liveness_profile_key(vm_id)
            if profile_key is None:
                continue
            if not self._idle_liveness_breaker_allows(profile_key, now):
                continue

            logger.warning(
                "VM %d has been idle without a worker heartbeat for %.0fs after %.0fs "
                "of healthy Redis heartbeat writes; replacing it",
                vm_id,
                missing_dwell,
                write_dwell,
            )
            if self._drain_and_destroy_idle_vm(vm_id, log_context="idle heartbeat replacement"):
                self._idle_missing_heartbeat_since.pop(vm_id, None)
                self._record_idle_liveness_replacement(profile_key, now)

    def _reset_idle_liveness_dwell(self, message: str) -> None:
        had_state = self._idle_liveness_write_healthy_since is not None or bool(
            self._idle_missing_heartbeat_since
        )
        self._idle_liveness_write_healthy_since = None
        self._idle_missing_heartbeat_since.clear()
        if had_state:
            logger.warning("%s; restarted idle heartbeat dwell for all idle VMs", message)

    def _idle_liveness_profile_key(self, vm_id: int) -> str | None:
        if not self._pool.contains_worker_vmid(vm_id):
            return None
        try:
            profile = self._pool.worker_profile_for_vmid(vm_id)
        except ValueError:
            logger.warning(
                "Skipping idle heartbeat liveness for VM %d: worker profile is unavailable",
                vm_id,
                exc_info=True,
            )
            return None
        return f"{profile.backend}:{profile.runner_type}:{profile.runner_mode}"

    def _refresh_idle_liveness_breakers(self, now: float) -> None:
        for profile_key in list(self._idle_liveness_breaker_events):
            self._idle_liveness_breaker_events[profile_key] = [
                ts
                for ts in self._idle_liveness_breaker_events[profile_key]
                if now - ts < _IDLE_LIVENESS_BREAKER_WINDOW_SECONDS
            ]
            if (
                self._idle_liveness_breaker_open.get(profile_key)
                and len(self._idle_liveness_breaker_events[profile_key])
                < _IDLE_LIVENESS_BREAKER_LIMIT
            ):
                self._idle_liveness_breaker_open[profile_key] = False
                logger.warning(
                    "Idle heartbeat replacement breaker cleared for profile %s",
                    profile_key,
                )

    def _idle_liveness_breaker_allows(self, profile_key: str, now: float) -> bool:
        events = [
            ts
            for ts in self._idle_liveness_breaker_events.get(profile_key, [])
            if now - ts < _IDLE_LIVENESS_BREAKER_WINDOW_SECONDS
        ]
        self._idle_liveness_breaker_events[profile_key] = events
        if len(events) < _IDLE_LIVENESS_BREAKER_LIMIT:
            return True
        if not self._idle_liveness_breaker_open.get(profile_key):
            self._idle_liveness_breaker_open[profile_key] = True
            logger.warning(
                "Idle heartbeat replacement breaker opened for profile %s "
                "(%d replacements in %d seconds)",
                profile_key,
                _IDLE_LIVENESS_BREAKER_LIMIT,
                int(_IDLE_LIVENESS_BREAKER_WINDOW_SECONDS),
            )
        return False

    def _record_idle_liveness_replacement(self, profile_key: str, now: float) -> None:
        events = self._idle_liveness_breaker_events.setdefault(profile_key, [])
        events.append(now)

    def _drain_and_destroy_idle_vm(self, vm_id: int, *, log_context: str) -> bool:
        """Reserve an idle VM, quiesce, stop, and destroy it.

        Shared by excess-idle fill, profiled layout drain, and idle-heartbeat
        replacement so the post-stop pending-check retry and restore semantics
        cannot drift.

        Returns True if the VM was destroyed or queued for destroy retry.
        Returns False if the drain was skipped or aborted and the VM restored.
        """
        worker_id = self._vm_id_to_worker_id(vm_id)
        pending_consumers, pending_complete = self._consumers_with_pending_status()
        if not pending_complete:
            logger.warning(
                "Skipping drain of VM %d (%s): pending-task state is unavailable",
                vm_id,
                log_context,
            )
            return False
        if worker_id in pending_consumers:
            logger.info(
                "Skipping drain of VM %d (%s): it has a pending task "
                "(claimed since active-detection ran this pass)",
                vm_id,
                log_context,
            )
            return False
        # Reserve the worker before the final PEL check. Workers honor
        # pool:draining before every XREADGROUP, which closes the race
        # where an idle VM claimed work after our earlier snapshot.
        try:
            pipe = self._redis.pipeline()
            pipe.srem(_POOL_IDLE_KEY, str(vm_id))
            pipe.sadd(_POOL_DRAINING_KEY, worker_id)
            pipe.execute()
        except Exception:
            logger.warning(
                "Failed to reserve VM %d for draining (%s)",
                vm_id,
                log_context,
                exc_info=True,
            )
            return False

        time.sleep(_DRAIN_QUIESCE_SECONDS)
        pending_consumers, pending_complete = self._consumers_with_pending_status()
        if not pending_complete or worker_id in pending_consumers:
            logger.info(
                "Skipping drain of VM %d (%s): it claimed a task or final pending-state "
                "inspection failed",
                vm_id,
                log_context,
            )
            try:
                pipe = self._redis.pipeline()
                pipe.srem(_POOL_DRAINING_KEY, worker_id)
                pipe.hset(_POOL_ACTIVE_KEY, str(vm_id), str(time.time()))
                pipe.execute()
            except Exception:
                logger.error(
                    "Failed to restore busy VM %d after drain race (%s)",
                    vm_id,
                    log_context,
                    exc_info=True,
                )
            return False

        logger.info("Draining VM %d (%s)", vm_id, log_context)
        if not self._stop_vm(vm_id):
            self._restore_worker_after_failed_drain(vm_id, worker_id)
            return False
        # Once stopped, no new claim is possible. This post-stop PEL
        # check is the actual destruction commit point.
        pending_consumers, pending_complete = self._post_stop_pending_status()
        if not pending_complete:
            logger.warning(
                "Restarting drained VM %d (%s) and marking it active: post-stop "
                "pending-state inspection failed after retries; the health "
                "check will reap it once it exceeds max_task_duration, or "
                "once its activity record and liveness heartbeat both go "
                "stale/absent while work is still pending",
                vm_id,
                log_context,
            )
            self._restore_worker_after_failed_drain(vm_id, worker_id, restart=True, active=True)
            return False
        if worker_id in pending_consumers and not self._coordinate_reaped_vm(
            vm_id, reason=REAP_REASON_DRAIN_RACE
        ):
            logger.warning(
                "Leaving drained VM %d (%s): late task claim could not be recovered",
                vm_id,
                log_context,
            )
            self._restore_worker_after_failed_drain(vm_id, worker_id, restart=True, active=True)
            return False
        if self._destroy_stopped_vm(vm_id):
            return True
        self._mark_pending_destroy(vm_id)
        return True

    def _fill_pool(self, blocked_vmids: set[int] | None = None) -> None:
        """Adjust pool to target size: clone new VMs or drain idle excess.

        Args:
            blocked_vmids: VMIDs that are quarantined or awaiting cleanup.
                They are skipped individually; every other slot is still
                refilled. In the flat (non-profiled) layout the allocator
                already excludes them durably via the Redis quarantine sets,
                so the deficit is simply satisfied by other VMIDs.
        """
        if self._pool.worker_profiles:
            self._fill_profiled_pool(blocked_vmids)
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
                if self._drain_and_destroy_idle_vm(vm_id, log_context="excess idle"):
                    drained += 1

    def _fill_profiled_pool(self, blocked_vmids: set[int] | None = None) -> None:
        """Reconcile heterogeneous workers to deterministic VMID/profile slots."""
        blocked = set(blocked_vmids or ())
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
        quarantined = sorted(vm_id for vm_id in missing if vm_id in blocked)
        if quarantined:
            logger.warning(
                "Skipping quarantined worker slots %s this pass; the remaining "
                "slots are still refilled",
                quarantined,
            )
            missing = [vm_id for vm_id in missing if vm_id not in blocked]
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
                    # Ambiguous clone outcome or an owned VM awaiting cleanup:
                    # stop fanning out clone attempts until the next pass.
                    break
            except Exception:
                # Allocation failures are slot-local (e.g. the VMID is already
                # in use). Aborting here would starve every higher-numbered
                # slot, i.e. every later provider in the round-robin.
                logger.error(
                    "Failed to clone and boot VM %d for backend %s; "
                    "continuing with the remaining slots",
                    vm_id,
                    profile.backend,
                    exc_info=True,
                )
                continue

    def _drain_profiled_idle_workers(self, idle_vmids: list[int]) -> int:
        """Drain the supplied out-of-layout idle workers in deterministic order."""
        drained = 0
        for vm_id in idle_vmids:
            if self._drain_and_destroy_idle_vm(vm_id, log_context="out-of-layout idle"):
                drained += 1
        return drained

    def _mark_pending_destroy(self, vm_id: int) -> None:
        """Queue a stopped-but-undestroyed worker for durable cleanup retry.

        A drain removes the VM from ``pool:idle`` before stopping it, so a
        failed ``destroy_vm`` would otherwise leave a stopped VM that no
        reconciler owns: ``_reconcile_stale_redis`` scans only idle/active,
        ``_health_check`` only active, and ``_reconcile_orphans`` runs once at
        startup. ``pool:provisioning`` is the retry queue drained by
        ``_retry_provisioning_cleanups`` on every pass, so recording the VMID
        there makes the destroy retryable across restarts too.
        """
        try:
            self._redis.sadd(_POOL_PROVISIONING_KEY, str(vm_id))
            if not self._redis.sismember(_POOL_PROVISIONING_KEY, str(vm_id)):
                raise RuntimeError("pending-destroy marker was not persisted")
        except Exception:
            logger.error(
                "Failed to durably queue VM %d for destroy retry; the in-process "
                "mirror will retry it until this manager exits",
                vm_id,
                exc_info=True,
            )
        # In-memory mirror so the retry survives a transient Redis failure for
        # as long as this process lives.
        self._owned_provisioning_vmids.add(vm_id)

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
        return self._stop_vm_with_outcome(vm_id).stopped

    def _stop_vm_with_outcome(self, vm_id: int) -> _StopVmOutcome:
        """Stop a worker VM and report whether this call confirmed the stop.

        ``confirmed_transition`` is false when the stop command failed but a
        follow-up status read proves the VM was already stopped. That case can
        happen after a pool-manager restart, so kill-time fields are unknown.
        """
        try:
            is_template = self._is_template_vmid(vm_id)
        except Exception:
            logger.error("Refusing to stop VM %d: template identity is unavailable", vm_id)
            return _StopVmOutcome(stopped=False, confirmed_transition=False)
        if is_template:
            logger.error("Refusing to stop template VM %d through a worker lifecycle path", vm_id)
            return _StopVmOutcome(stopped=False, confirmed_transition=False)
        if not self._is_destroyable_vm_id(vm_id):
            logger.error(
                "Refusing to stop VM %d: outside configured worker VMID range "
                "[%s, %s]. This is a safety guard against a poisoned done-key or "
                "misrouted lifecycle action targeting the orchestrator/template/unrelated VM.",
                vm_id,
                self._pool.vm_id_start,
                self._pool.vm_id_end or "open",
            )
            return _StopVmOutcome(stopped=False, confirmed_transition=False)
        try:
            self._proxmox.stop_vm(vm_id)
            # Brief wait for VM to stop before destroying
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                try:
                    if self._proxmox.get_vm_status(vm_id) == "stopped":
                        return _StopVmOutcome(stopped=True, confirmed_transition=True)
                except Exception:
                    return _StopVmOutcome(stopped=False, confirmed_transition=False)
                time.sleep(1)
            try:
                if self._proxmox.get_vm_status(vm_id) == "stopped":
                    return _StopVmOutcome(stopped=True, confirmed_transition=True)
            except Exception:
                pass
            logger.error("VM %d did not reach stopped state before timeout", vm_id)
            return _StopVmOutcome(stopped=False, confirmed_transition=False)
        except Exception:
            logger.warning("Failed to stop VM %d (may already be stopped)", vm_id)
            try:
                return _StopVmOutcome(
                    stopped=self._proxmox.get_vm_status(vm_id) == "stopped",
                    confirmed_transition=False,
                )
            except Exception:
                return _StopVmOutcome(stopped=False, confirmed_transition=False)

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
        # I1 follow-up: the activity-watchdog record (workers:activity:{id})
        # is worker_id-keyed, not vm_id-keyed, so it survives VM destruction
        # unless deleted here explicitly. Since I1, a needs_reap record is
        # deliberately re-flushed (not deleted) with a 600s TTL on close() so
        # the reaper reliably sees it -- but that means it can still be
        # sitting there, needs_reap=="1", when the lowest-free-VMID reuse
        # policy hands this same worker_id to a freshly cloned replacement
        # VM. That VM's own tracker hasn't ticked yet, so without this
        # delete the reaper would read the STALE record and destroy the
        # brand-new VM before it ever gets a chance -- a false kill,
        # repeatable for the remainder of the stale record's TTL. The
        # record's entire job (making the reaper fire) is complete once the
        # VM it describes is actually destroyed, so deleting it here fully
        # preserves I1's intent without reopening this hole.
        activity_key = f"{_ACTIVITY_KEY_PREFIX}{worker_id}"
        try:
            pipe = self._redis.pipeline()
            pipe.srem(_POOL_IDLE_KEY, str(vm_id))
            pipe.hdel(_POOL_ACTIVE_KEY, str(vm_id))
            pipe.srem(_POOL_DRAINING_KEY, worker_id)
            pipe.srem(_POOL_PROVISIONING_KEY, str(vm_id))
            pipe.srem(_POOL_AMBIGUOUS_CLONES_KEY, str(vm_id))
            pipe.delete(done_key)
            pipe.delete(heartbeat_key)
            pipe.delete(activity_key)
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
        finally:
            # Proxmox has confirmed the VM is gone, so the in-process
            # allocation guards must be released even when the Redis cleanup
            # above failed. Leaving them set wedges the slot for the lifetime
            # of the process; the durable Redis quarantine sets (which
            # _next_vm_id also reads) remain the safety net for reuse.
            self._owned_provisioning_vmids.discard(vm_id)
            self._allocated_vmids.discard(vm_id)
        self._reap_fences.pop(vm_id, None)
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
                watchdog_enabled=self._pool.watchdog_enabled,
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
        # Defense in depth: both destroy paths (_destroy_stopped_vm,
        # _clear_destroyed_worker_state) already delete the worker_id-keyed
        # activity record, but this pre-reuse chokepoint is the last line --
        # a surviving needs_reap=="1" record on a reused worker_id would
        # false-kill the fresh replacement VM the moment the reaper reads it.
        activity_key = f"{_ACTIVITY_KEY_PREFIX}{worker_id}"
        try:
            pipe = self._redis.pipeline()
            pipe.delete(done_key)
            pipe.srem(_POOL_DRAINING_KEY, worker_id)
            pipe.srem(_POOL_PROVISIONING_KEY, str(vm_id))
            pipe.srem(_POOL_AMBIGUOUS_CLONES_KEY, str(vm_id))
            pipe.delete(heartbeat_key)
            pipe.delete(activity_key)
            pipe.execute()
            if (
                self._redis.exists(done_key)
                or self._redis.exists(heartbeat_key)
                or self._redis.exists(activity_key)
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

    def _retry_provisioning_cleanups(self) -> set[int] | None:
        """Retry cleanup of every clone known to have passed clone_vm().

        The Redis set makes cleanup retryable across process restarts. An
        in-memory mirror covers a transient Redis failure after clone success
        without ever upgrading an ambiguous clone result into owned state.

        Returns the VMIDs whose cleanup is still outstanding, so the caller can
        skip exactly those slots while refilling the rest. Returns ``None`` when
        the cleanup state itself is unknown (Redis or Proxmox read failure), in
        which case no VM may be created at all.
        """
        try:
            pending = {
                int(member)
                for member in self._redis.smembers(_POOL_PROVISIONING_KEY)
                if str(member).lstrip("-").isdigit()
            }
        except Exception:
            logger.error("Failed to read durable provisioning cleanup state", exc_info=True)
            return None
        pending.update(self._owned_provisioning_vmids)
        if not pending:
            return set()

        try:
            inventory = self._proxmox.list_vms()
        except Exception:
            logger.error("Failed to list VMs for provisioning cleanup retry", exc_info=True)
            return None
        existing = {
            int(vm["vmid"]): vm
            for vm in inventory
            if vm.get("vmid") is not None and str(vm.get("vmid")).lstrip("-").isdigit()
        }

        blocked: set[int] = set()
        for vm_id in sorted(pending):
            if not self._is_destroyable_vm_id(vm_id):
                logger.error(
                    "Refusing provisioning cleanup for VM %d outside the worker range",
                    vm_id,
                )
                blocked.add(vm_id)
                continue
            vm_info = existing.get(vm_id)
            if vm_info is None:
                if not self._clear_destroyed_worker_state(vm_id):
                    blocked.add(vm_id)
                continue
            if self._is_proxmox_template(vm_info):
                logger.error(
                    "Refusing to destroy template VM %d during provisioning cleanup", vm_id
                )
                blocked.add(vm_id)
                continue
            if not self._destroy_vm(vm_id):
                blocked.add(vm_id)
        return blocked

    def _reconcile_ambiguous_clones(self) -> set[int] | None:
        """Release quarantined VMIDs only when inventory proves no VM exists.

        A present VM remains quarantined because a clone exception cannot prove
        who created it. This is deliberately operator-resolved; an absent VMID
        can be released automatically from an authoritative inventory snapshot.

        Returns the VMIDs that stay quarantined. The quarantine is per-slot: a
        single unresolved VMID costs its own slot only, never the whole refill
        (which would drain the fleet to zero while finished workers keep being
        retired). Returns ``None`` when the quarantine state itself is unknown,
        which is the only case that suppresses cloning entirely.
        """
        try:
            raw_members = self._redis.smembers(_POOL_AMBIGUOUS_CLONES_KEY)
            ambiguous = {int(member) for member in raw_members if str(member).lstrip("-").isdigit()}
        except Exception:
            logger.error("Failed to read ambiguous clone quarantine state", exc_info=True)
            return None
        if not ambiguous:
            return set()
        try:
            existing = {
                int(vm["vmid"])
                for vm in self._proxmox.list_vms()
                if vm.get("vmid") is not None and str(vm.get("vmid")).lstrip("-").isdigit()
            }
        except Exception:
            logger.error("Failed to verify ambiguous clone VMIDs against Proxmox", exc_info=True)
            return None

        blocked: set[int] = set()
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
                blocked.add(vm_id)
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
                blocked.add(vm_id)
        return blocked

    def _clear_destroyed_worker_state(self, vm_id: int) -> bool:
        """Clear lifecycle state after inventory proves the VM is absent."""
        worker_id = self._vm_id_to_worker_id(vm_id)
        done_key = f"{_POOL_DONE_PREFIX}{worker_id}"
        heartbeat_key = f"{_WORKER_HEARTBEAT_PREFIX}{worker_id}"
        # I1 follow-up: see the matching comment in _destroy_stopped_vm --
        # this record is worker_id-keyed and outlives VM destruction unless
        # deleted here, which can false-kill the replacement VM that
        # eventually reuses this worker_id via lowest-free-VMID reuse.
        activity_key = f"{_ACTIVITY_KEY_PREFIX}{worker_id}"
        try:
            pipe = self._redis.pipeline()
            pipe.srem(_POOL_IDLE_KEY, str(vm_id))
            pipe.hdel(_POOL_ACTIVE_KEY, str(vm_id))
            pipe.srem(_POOL_DRAINING_KEY, worker_id)
            pipe.srem(_POOL_PROVISIONING_KEY, str(vm_id))
            pipe.srem(_POOL_AMBIGUOUS_CLONES_KEY, str(vm_id))
            pipe.delete(done_key)
            pipe.delete(heartbeat_key)
            pipe.delete(activity_key)
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
        finally:
            # Inventory already proved the VM is absent, so the in-process
            # allocation guards are released even when the Redis cleanup or
            # its verification failed. Durable Redis state is retried next pass.
            self._owned_provisioning_vmids.discard(vm_id)
            self._allocated_vmids.discard(vm_id)
        self._reap_fences.pop(vm_id, None)
        return True

    def _health_check(self) -> None:
        """Force-destroy VMs past the max_task_duration ceiling, or whose
        worker activity record (spec §6) proves the task is stuck or dead
        below the ceiling: ``needs_reap == "1"`` fires immediately; an
        absent-or-stale record fires only when the worker's liveness
        heartbeat is also absent (proving the process itself is gone, not
        just quiet on activity reporting -- see ``_activity_reap_reason``),
        the consumer still holds pending stream entries, and elapsed time
        is at least ``activity_stale_min_elapsed``. A fresh activity
        record blocks destruction below the ceiling regardless of elapsed
        time.
        """
        try:
            active = self._redis.hgetall(_POOL_ACTIVE_KEY)
        except Exception:
            logger.warning(
                "Failed to read active pool state; skipping destructive health check",
                exc_info=True,
            )
            return
        if not active:
            return

        now = time.time()
        max_duration = self._pool.max_task_duration
        parsed_active: list[tuple[int, float]] = []

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
            parsed_active.append((vm_id, start_ts))

        # One EXISTS per active worker for the whole pass: the fleet-wide
        # fail-safe probe below and the per-VM activity_stale decision in
        # _activity_reap_reason ask the same question, so they share one
        # memo instead of each hitting Redis.
        heartbeat_cache: dict[str, bool | None] = {}
        activity_stale_infra_fault = self._activity_stale_infra_fault(
            [vm_id for vm_id, _start_ts in parsed_active],
            heartbeat_cache=heartbeat_cache,
        )

        for vm_id, start_ts in parsed_active:
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
                reason = REAP_REASON_CEILING
                logger.warning(
                    "VM %d exceeded max task duration (%.0fs > %ds), force-destroying",
                    vm_id,
                    elapsed,
                    max_duration,
                )
            else:
                # Bind the optional result to its own name: `reason` is a
                # plain str from the ceiling branch above and everything
                # downstream (logging, _coordinate_reaped_vm) requires one,
                # so the "no activity signal" case must exit via `continue`
                # rather than widen `reason` to str | None.
                activity_reason = self._activity_reap_reason(
                    vm_id,
                    now,
                    elapsed=elapsed,
                    activity_stale_infra_fault=activity_stale_infra_fault,
                    heartbeat_cache=heartbeat_cache,
                )
                if activity_reason is None:
                    continue
                reason = activity_reason
                logger.warning(
                    "VM %d: activity watchdog signal (%s) at %.0fs elapsed (ceiling %ds), "
                    "force-destroying",
                    vm_id,
                    reason,
                    elapsed,
                    max_duration,
                )

            # Fence the worker before inspecting its PEL or private recovery
            # state. Otherwise it can publish a late success/rotation while
            # the reaper concurrently publishes failure and ACKs its source.
            # A stopped VM also bounds billing while Redis recovery retries.
            stop_outcome = self._stop_vm_with_outcome(vm_id)
            if not stop_outcome.stopped:
                logger.warning(
                    "VM %d marked for force-destroy (reason=%s) but could not be "
                    "stopped; preserving Redis state for retry",
                    vm_id,
                    reason,
                )
                continue
            fence = self._reap_fences.get(vm_id)
            if fence is None and stop_outcome.confirmed_transition:
                fence = ReapFence(
                    vm_id=vm_id,
                    reason=reason,
                    killed_at_unix=now,
                    elapsed_at_kill_seconds=elapsed,
                )
                self._reap_fences[vm_id] = fence
            event_reason = fence.reason if fence is not None else reason
            event_elapsed = fence.elapsed_at_kill_seconds if fence is not None else None
            event_killed_at = fence.killed_at_unix if fence is not None else None
            try:
                coordinated = self._coordinate_reaped_vm(
                    vm_id,
                    reason=event_reason,
                    elapsed_seconds=event_elapsed,
                    killed_at_unix=event_killed_at,
                )
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
                    "VM %d marked for force-destroy (reason=%s) but Redis recovery is "
                    "incomplete; leaving the fenced VM stopped and preserving Redis "
                    "state for retry",
                    vm_id,
                    reason,
                )
                continue
            self._destroy_stopped_vm(vm_id)

    def _activity_stale_infra_fault(
        self,
        vm_ids: list[int],
        *,
        heartbeat_cache: dict[str, bool | None] | None = None,
    ) -> bool:
        """Return True when activity-stale reaping should fail safe this pass.

        Heartbeat probes are memoized into *heartbeat_cache* (worker_id ->
        True/False/None) so ``_activity_reap_reason`` can reuse them within
        the same health-check pass. The memo may be left incomplete when
        this returns True on the first unreadable heartbeat, which is safe:
        that same True makes ``_activity_reap_reason`` skip the heartbeat
        question entirely.
        """
        if len(vm_ids) < 2:
            return False

        missing = 0
        for vm_id in vm_ids:
            worker_id = self._vm_id_to_worker_id(vm_id)
            heartbeat_present = self._probe_worker_heartbeat(worker_id, heartbeat_cache)
            if heartbeat_present is None:
                logger.warning(
                    "Skipping activity-stale reaping this pass because a liveness "
                    "heartbeat read failed"
                )
                return True
            if heartbeat_present is False:
                missing += 1

        if missing == len(vm_ids):
            logger.warning(
                "Skipping activity-stale reaping this pass because every active "
                "worker liveness heartbeat is absent"
            )
            return True
        return False

    def _activity_reap_reason(
        self,
        vm_id: int,
        now: float,
        *,
        elapsed: float,
        activity_stale_infra_fault: bool = False,
        heartbeat_cache: dict[str, bool | None] | None = None,
    ) -> str | None:
        """Return the reap reason from *vm_id*'s activity record, below the
        max_task_duration ceiling (spec §6).

        ``needs_reap == "1"`` fires immediately -- the watchdog already
        latched a kill decision for this task, so the record is trustworthy
        as-is. The elapsed-time floor below does not apply.

        An absent-or-stale record is different: by itself it is NOT proof of
        death. ``watchdog.enabled: false`` (the ship-dark stage and the
        rollback lever) means no worker ever writes this record at all, and
        the same is true for an old worker image mid-rollout that predates
        the tracker. So an absent-or-stale record only becomes a destroy
        signal when it is corroborated by the worker's liveness heartbeat
        (``workers:heartbeat:{worker_id}``, written by every worker
        regardless of watchdog config -- see ``_worker_heartbeat_present``,
        already used by the orphan-PEL sweep) being ABSENT too, proving the
        worker process itself is gone, not just quiet on activity
        reporting. A present (or unknown/unreadable, which fails safe the
        same way) heartbeat means the worker is alive but not reporting
        activity -- watchdog off, an old image, or a crashed tracker -- and
        is left to the ceiling exactly like the pre-watchdog reaper. Only
        once both the record is absent-or-stale AND the heartbeat is
        provably gone does a pending stream entry (proving there is a task
        to recover, not an idle worker) trigger destruction -- and only
        after *elapsed* reaches ``activity_stale_min_elapsed``. Below that
        floor a young VM may simply not have written either Redis key yet.
        A fresh record blocks destruction outright: returns ``None``.
        """
        worker_id = self._vm_id_to_worker_id(vm_id)
        key = f"{_ACTIVITY_KEY_PREFIX}{worker_id}"
        try:
            record = self._redis.hgetall_raw(key)
        except Exception:
            logger.warning(
                "VM %d: failed to read activity record %s; leaving VM alone this pass",
                vm_id,
                key,
                exc_info=True,
            )
            return None

        # SECURITY: this branch is forgeable under the shared-credential
        # threat model. All workers share one Redis password (accepted risk,
        # 2026-06 audit), so a compromised worker can HSET
        # workers:activity:{other_worker} needs_reap 1 and have this reaper
        # destroy a peer VM. Deliberately NOT corroborated here: every
        # corroboration option either breaks the D-state escalation this
        # flag exists for or under-protects -- heartbeat-gating fails
        # because the worker's heartbeat is still alive when needs_reap
        # legitimately fires (the runner process is fine; its child tree is
        # unkillable), and task_id/PEL matching fails because needs_reap
        # deliberately outlives the attempt via close()'s long-TTL re-flush,
        # so the PEL entry may already be gone at reap time. The forge is
        # also strictly weaker than what the shared password already grants
        # (full queue/lock/result read-write). Real remediation is
        # per-worker Redis ACLs, tracked separately.
        if record.get("needs_reap") == "1":
            return REAP_REASON_NEEDS_REAP

        if not self._activity_record_is_stale(record, now):
            return None

        if elapsed < self._pool.activity_stale_min_elapsed:
            # Missing activity + heartbeat is not proof of death this early:
            # a just-assigned worker may not have written either key yet.
            # needs_reap already returned above; the duration ceiling is
            # handled in _health_check before this method is called.
            return None

        if activity_stale_infra_fault:
            # Activity and heartbeat are two process-level signals, but both
            # are stored in Redis. When they vanish for the whole active fleet
            # in one health-check pass, that is shared infrastructure failure,
            # not independent proof that every young worker died. The durable
            # fix is an out-of-band liveness source; until then, fail safe for
            # activity_stale while preserving ceiling and needs_reap reaping.
            return None

        heartbeat_present = self._probe_worker_heartbeat(worker_id, heartbeat_cache)
        if heartbeat_present is not False:
            # True: worker is alive but not reporting activity (watchdog
            # disabled, old image, crashed tracker) -- leave it to the
            # ceiling. None: unreadable/unknown -- fail safe, don't treat
            # as dead (same contract _sweep_orphan_pel relies on).
            return None

        pending_consumers, pending_complete = self._consumers_with_pending_status()
        if not pending_complete:
            logger.warning(
                "VM %d: activity record is %s and heartbeat is absent, but "
                "pending-task state is unavailable; leaving VM alone this pass",
                vm_id,
                "present" if record else "absent",
            )
            return None
        if worker_id not in pending_consumers:
            return None
        return REAP_REASON_ACTIVITY_STALE

    def _activity_record_is_stale(self, record: dict[str, str], now: float) -> bool:
        """True if *record* is absent or its ``last_liveness_ts`` is older
        than ``activity_stale_after``.

        The record TTLs out at 4x the worker's ``sample_interval`` (120s by
        default), so absence after that window is itself the worker-died
        signal, not merely a missing field -- an empty ``record`` is
        therefore always stale.
        """
        if not record:
            return True
        raw_ts = record.get("last_liveness_ts")
        try:
            last_ts = float(raw_ts)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return True
        return (now - last_ts) > self._pool.activity_stale_after

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
                    self._reap_fences.pop(vm_id, None)
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
                    self._reap_fences.pop(vm_id, None)
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

    def _post_stop_pending_status(self) -> tuple[set[str], bool]:
        """Post-stop pending check with a bounded retry on incomplete reads.

        Called at the destruction commit point of ``_drain_and_destroy_idle_vm``,
        after the VM is provably stopped (so no new claim can appear between
        attempts).
        The incomplete branch there is expensive — it restarts the VM and
        files it into ``pool:active``, which nothing transitions back to
        idle — so a transient Redis failure gets a few quick retries before
        we give up and take the conservative path.
        """
        names: set[str] = set()
        for attempt in range(1, _POST_STOP_PENDING_CHECK_ATTEMPTS + 1):
            names, complete = self._consumers_with_pending_status()
            if complete:
                return names, True
            if attempt < _POST_STOP_PENDING_CHECK_ATTEMPTS:
                logger.warning(
                    "Post-stop pending-state inspection incomplete (attempt %d/%d); retrying",
                    attempt,
                    _POST_STOP_PENDING_CHECK_ATTEMPTS,
                )
                time.sleep(_POST_STOP_PENDING_CHECK_RETRY_SECONDS)
        return names, False

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

    def _coordinate_reaped_vm(
        self,
        vm_id: int,
        reason: str,
        elapsed_seconds: float | None = None,
        killed_at_unix: float | None = None,
    ) -> bool:
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

        ``reason`` is one of the ``REAP_REASON_*`` constants and is stamped
        honestly on the emitted ``net.orcest.task.reaped`` event's
        ``data.reason`` — callers other than ``_health_check`` (done-worker
        cleanup, drain races, orphan-PEL sweep) did not observe a ceiling
        breach or an activity-watchdog signal and must not report
        ``ceiling``, ``needs_reap`` or ``activity_stale``.

        ``elapsed_seconds`` and ``killed_at_unix`` are frozen values captured
        by ``_health_check`` when this process first confirmed the VM stopped.
        Other callers, and restarted managers that find an already-stopped VM
        without an in-memory fence, leave them as ``None`` so the emitted event
        omits unknown fields rather than fabricating kill-time telemetry.
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
                # Note: a pre-ACK reap of a worker wedged in D-state (blocked on
                # uninterruptible I/O, never dead-but-never-progressing) still
                # republishes here as a transient FAILED result, shadowing what
                # would otherwise be a permanent STALLED classification upstream.
                # Rare and accepted -- the retry this produces is harmless.
                if not self._publish_reaped_failure(
                    task,
                    consumer,
                    reason,
                    elapsed_seconds,
                    killed_at_unix,
                ):
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

    def _publish_reaped_failure(
        self,
        task: Task,
        worker_id: str,
        reason: str,
        elapsed_seconds: float | None = None,
        killed_at_unix: float | None = None,
    ) -> bool:
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
        self._emit_reaped_event(task, worker_id, reason, elapsed_seconds, killed_at_unix)
        return True

    def _event_publisher_for(self, key_prefix: str | None) -> EventPublisher:
        """Return the cached EventPublisher for *key_prefix*, creating it once.

        Caching (rather than constructing a fresh EventPublisher per event)
        preserves each publisher's decimated-error counter across calls, so
        the 1/10/100/1000-backoff log suppression actually suppresses during
        a sustained outage instead of re-logging every single failure.
        """
        cache_key = key_prefix or "default"
        publisher = self._event_publishers.get(cache_key)
        if publisher is None:
            if key_prefix:
                project_redis = RedisClient.from_client(self._redis.client, key_prefix=key_prefix)
            else:
                project_redis = self._redis
            publisher = EventPublisher(project_redis)
            self._event_publishers[cache_key] = publisher
        return publisher

    def _emit_reaped_event(
        self,
        task: Task,
        worker_id: str,
        reason: str,
        elapsed_seconds: float | None,
        killed_at_unix: float | None = None,
    ) -> None:
        """Spool a task.reaped event. Never raises (EventPublisher swallows).

        ``elapsed_seconds`` and ``killed_at`` are omitted from the event data
        entirely when unknown. The envelope ``time`` remains the event
        construction time; ``killed_at`` is the frozen VM stop confirmation
        time when this process observed it.
        """
        try:
            publisher = self._event_publisher_for(task.key_prefix)
            data: dict[str, Any] = {"reason": reason}
            if elapsed_seconds is not None:
                data["elapsed_seconds"] = elapsed_seconds
            if killed_at_unix is not None:
                data["killed_at"] = datetime.fromtimestamp(killed_at_unix, timezone.utc).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                )
            publisher.publish(
                make_event(
                    "net.orcest.task.reaped",
                    source_project=task.key_prefix or "default",
                    task_id=task.id,
                    repo=task.repo,
                    resource_type=task.resource_type,
                    resource_id=task.resource_id,
                    attempt=task.attempt,
                    head_sha=task.snapshot_head_sha,
                    worker_id=worker_id,
                    provider=task.provider,
                    data=data,
                )
            )
        except Exception:
            logger.warning(
                "Reaped task %s: failed to emit task.reaped event", task.id, exc_info=True
            )

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

        We only act on a consumer that is PROVABLY a dead pool worker:

        1. Its name parses to a VMID inside the configured worker VMID range.
           ``_worker_id_to_vm_id`` also accepts a bare integer (legacy naming),
           so a standalone/legacy worker can parse to a VMID the pool never
           owned; absence from the pool's own bookkeeping says nothing about
           such a worker's liveness.
        2. That VMID is absent from pool:idle, pool:active and the Proxmox
           worker inventory.
        3. Its TTL-backed ``workers:heartbeat:<worker_id>`` key is gone. This is
           positive proof of death rather than an inference from absence, and
           it is what stops a live worker's task from being re-enqueued while
           it is still executing.
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
                if not self._is_destroyable_vm_id(vm_id):
                    # Not a VMID this pool allocates: a standalone or legacy
                    # worker. The pool's bookkeeping cannot prove it is dead.
                    continue
                if vm_id in live_vm_ids:
                    continue
                heartbeat_present = self._worker_heartbeat_present(name)
                if heartbeat_present is not False:
                    logger.info(
                        "Orphan PEL: leaving consumer %s (VM %d) alone; its liveness "
                        "heartbeat is %s",
                        name,
                        vm_id,
                        "still present" if heartbeat_present else "unreadable",
                    )
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
                    if not self._coordinate_reaped_vm(vm_id, reason=REAP_REASON_ORPHAN_PEL):
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

    def _worker_heartbeat_present(self, worker_id: str) -> bool | None:
        """Return whether *worker_id* still publishes a liveness heartbeat.

        Workers refresh ``workers:heartbeat:<worker_id>`` (a short-TTL key)
        both while idle and while executing a task, so its presence is positive
        proof that the worker is alive. ``None`` means the answer is unknown
        and callers must not treat the worker as dead.
        """
        try:
            return bool(self._redis.exists(f"{_WORKER_HEARTBEAT_PREFIX}{worker_id}"))
        except Exception:
            logger.warning(
                "Failed to read liveness heartbeat for worker %s",
                worker_id,
                exc_info=True,
            )
            return None

    def _probe_worker_heartbeat(
        self, worker_id: str, cache: dict[str, bool | None] | None
    ) -> bool | None:
        """``_worker_heartbeat_present`` memoized into *cache* for one pass.

        A health-check pass asks the same heartbeat question twice per
        worker -- once for the fleet-wide fail-safe, once per activity-stale
        decision -- so a shared dict keeps it to one EXISTS. A cached
        ``None`` (unreadable) stays ``None`` for the rest of the pass, which
        is the same fail-safe answer a re-read would have to be treated as.
        Passing ``cache=None`` reads through, uncached.
        """
        if cache is None:
            return self._worker_heartbeat_present(worker_id)
        if worker_id not in cache:
            cache[worker_id] = self._worker_heartbeat_present(worker_id)
        return cache[worker_id]

    # ── stranded provider-stream detection (issue #613) ─────────

    def _check_stream_health(self) -> None:
        """Compose and publish a health snapshot for every configured stream.

        Workers consume both ``tasks:{provider}`` and
        ``tasks:issue:{provider}``. Each is evaluated independently so a
        stranded member cannot be hidden by a healthy sibling. A provider
        stream is stranded when it carries pending/lag work but has zero
        heartbeat-backed live consumers. This is a continuous dwell-based
        detector, not a one-shot deploy gate: it uses the fleet's configured
        provider backends (``self._pool.worker_backends()``) rather than a
        hard-coded stream prefix or a rollout revision gate, so it keeps
        working long after any particular rollout is over.

        A read failure or unexpected exception for one stream must not
        skip the other stream or abort the rest of reconciliation.
        """
        if not self._pool.stream_health_enabled:
            return
        now = time.time()
        heartbeat_cache: dict[str, bool | None] = {}
        for provider in sorted(self._pool.worker_backends()):
            for issue in (False, True):
                try:
                    self._check_one_provider_stream_health(
                        provider, now, heartbeat_cache, issue=issue
                    )
                except Exception:
                    logger.error(
                        "Stream health check failed for provider %s stream=%s",
                        provider,
                        task_stream_name(provider, issue=issue),
                        exc_info=True,
                    )

    def _check_one_provider_stream_health(
        self,
        provider: str,
        now: float,
        heartbeat_cache: dict[str, bool | None],
        *,
        issue: bool,
    ) -> None:
        stream = self._fq_task_stream(task_stream_name(provider, issue=issue))
        pending, lag, registered, live, read_error = self._read_stream_health_inputs(
            stream, heartbeat_cache
        )
        snapshot, transition = self._stream_health_tracker.evaluate(
            provider,
            stream,
            now=now,
            pending=pending,
            lag=lag,
            registered_consumers=registered,
            live_consumers=live,
            read_error=read_error,
        )
        self._publish_stream_health(snapshot, issue=issue)
        if transition == "stranded":
            logger.error(
                "Provider stream %s (provider=%s) is stranded: pending=%s lag=%s "
                "registered_consumers=%s live_consumers=%s -- work is queued but no "
                "consumer has a live worker heartbeat",
                stream,
                provider,
                snapshot.pending,
                snapshot.lag,
                snapshot.registered_consumers,
                snapshot.live_consumers,
            )
        elif transition == "recovered":
            logger.info(
                "Provider stream %s (provider=%s) recovered: live_consumers=%s",
                stream,
                provider,
                snapshot.live_consumers,
            )

    def _read_stream_health_inputs(
        self, stream: str, heartbeat_cache: dict[str, bool | None]
    ) -> tuple[int | None, int | None, int | None, int | None, bool]:
        """Return (pending, lag, registered_consumers, live_consumers, read_error).

        A missing stream, or a stream with no ``workers`` consumer group yet,
        is empty/healthy -- never a read error and never stranded (unless the
        group-less stream already has undelivered entries, in which case
        those entries are definitionally unconsumed). Any genuine Redis
        failure or malformed reply is a read error: the caller must preserve
        whatever state was last known rather than guess.
        """
        try:
            raw_type = self._redis.client.type(stream)
            key_type = raw_type.decode() if isinstance(raw_type, bytes) else str(raw_type)
        except Exception:
            return None, None, None, None, True

        if key_type == "none":
            return 0, 0, 0, 0, False
        if key_type != "stream":
            return None, None, None, None, True

        try:
            groups = self._redis.xinfo_groups_raw(stream)
        except redis.ResponseError as exc:
            if _is_missing_stream_or_group_error(exc):
                return 0, 0, 0, 0, False
            return None, None, None, None, True
        except Exception:
            return None, None, None, None, True

        worker_groups = [g for g in groups if g.get("name") == CONSUMER_GROUP]
        if not worker_groups:
            # No consumer group means Redis has neither a PEL nor a
            # group-reported lag for this stream -- these entries were never
            # delivered. Report them as synthetic undelivered-lag (not
            # ``pending``) so stranding detection still sees them as queued
            # work without implying Redis itself reported group lag.
            try:
                entries = int(cast(Any, self._redis.client.xlen(stream)))
            except Exception:
                return None, None, None, None, True
            return 0, entries, 0, 0, False

        pending = 0
        lag = 0
        registered = 0
        live = 0
        for group in worker_groups:
            pending_raw = group.get("pending")
            lag_raw = group.get("lag")
            if pending_raw is None or lag_raw is None:
                return None, None, None, None, True
            try:
                pending += int(pending_raw)
                lag += max(int(lag_raw), 0)
            except (TypeError, ValueError):
                return None, None, None, None, True

            try:
                consumers = self._redis.xinfo_consumers_raw(stream, CONSUMER_GROUP)
            except redis.ResponseError as exc:
                if _is_missing_stream_or_group_error(exc):
                    consumers = []
                else:
                    return None, None, None, None, True
            except Exception:
                return None, None, None, None, True

            for consumer in consumers:
                name = consumer.get("name")
                if not isinstance(name, str) or not name:
                    return None, None, None, None, True
                registered += 1
                # An unreadable heartbeat (None) does not count as live --
                # "registered consumers without live worker heartbeats do
                # not count as live" applies equally to an unknown answer.
                if self._probe_worker_heartbeat(name, heartbeat_cache):
                    live += 1

        return pending, lag, registered, live, False

    def _publish_stream_health(self, snapshot: ProviderStreamHealth, *, issue: bool) -> None:
        try:
            self._redis.set_ex_raw(
                stream_health_snapshot_key(snapshot.provider, issue=issue),
                json.dumps(snapshot.to_dict()),
                _STREAM_HEALTH_TTL_SECONDS,
            )
        except Exception:
            logger.warning(
                "Failed to publish stream health state for provider %s stream=%s",
                snapshot.provider,
                snapshot.stream,
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
