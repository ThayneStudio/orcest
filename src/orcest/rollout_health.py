"""Read-only rollout health gates for deployment and post-deploy watches."""

from __future__ import annotations

import json
import re
from collections import Counter
from typing import Any, cast

import redis as redis_lib

from orcest.revision import get_build_revision, revision_is_attested
from orcest.shared.models import CONSUMER_GROUP, require_valid_provider_name, task_stream_name
from orcest.shared.redis_client import RedisClient


def _check(name: str, passed: bool, actual: Any, expected: str) -> dict[str, Any]:
    return {
        "name": name,
        "passed": passed,
        "actual": actual,
        "expected": expected,
    }


def _raw_stream_work(
    redis: RedisClient,
    stream: str,
    *,
    allow_non_stream: bool = False,
) -> tuple[int, int, int, bool, str | None]:
    """Return work state for a stream, failing closed when it is unknowable."""
    try:
        key_type = str(cast(Any, redis.client.type(stream)))
        if key_type == "none":
            return 0, 0, 0, False, None
        if key_type != "stream":
            if allow_non_stream:
                return 0, 0, 0, False, None
            return 0, 0, 0, False, f"{stream}: expected stream, found {key_type}"
        groups = cast(list[dict[str, Any]], redis.client.xinfo_groups(stream))
        if not groups:
            entries = int(cast(Any, redis.client.xlen(stream)))
            return entries, 0, entries, entries > 0, None
        if any(
            group.get("pending") is None
            or group.get("lag") is None
            or group.get("consumers") is None
            for group in groups
        ):
            return 0, 0, 0, False, f"{stream}: consumer group work is unavailable"
        pending = sum(int(group["pending"]) for group in groups)
        lag = sum(max(int(group["lag"]), 0) for group in groups)
        unconsumed = any(
            int(group["consumers"]) == 0
            and (int(group["pending"]) > 0 or max(int(group["lag"]), 0) > 0)
            for group in groups
        )
    except (redis_lib.RedisError, TypeError, ValueError) as exc:
        # Do not include Redis exception text: ACL and connection messages can
        # contain endpoints or other deployment details.
        return 0, 0, 0, False, f"{stream}: {type(exc).__name__}"
    return pending + lag, pending, lag, unconsumed, None


def _raw_stream_length(redis: RedisClient, stream: str) -> tuple[int, str | None]:
    """Read a required stream length without treating wrong type as empty."""
    try:
        key_type = str(cast(Any, redis.client.type(stream)))
        if key_type == "none":
            return 0, None
        if key_type != "stream":
            return 0, f"{stream}: expected stream, found {key_type}"
        return int(cast(Any, redis.client.xlen(stream))), None
    except (redis_lib.RedisError, TypeError, ValueError) as exc:
        return 0, f"{stream}: {type(exc).__name__}"


def _raw_stream_consumer_names(redis: RedisClient, stream: str) -> tuple[set[str], str | None]:
    """Return worker-group consumer names for a required stream."""
    try:
        key_type = str(cast(Any, redis.client.type(stream)))
        if key_type == "none":
            return set(), None
        if key_type != "stream":
            return set(), f"{stream}: expected stream, found {key_type}"
        groups = cast(list[dict[str, Any]], redis.client.xinfo_groups(stream))
        worker_groups = [group for group in groups if str(group.get("name")) == CONSUMER_GROUP]
        if not worker_groups:
            return set(), None
        consumers = cast(list[dict[str, Any]], redis.client.xinfo_consumers(stream, CONSUMER_GROUP))
        names: set[str] = set()
        for consumer in consumers:
            name = consumer.get("name")
            if not isinstance(name, str) or not name:
                return set(), f"{stream}: consumer identity is unavailable"
            names.add(name)
        return names, None
    except (redis_lib.RedisError, TypeError, ValueError) as exc:
        return set(), f"{stream}: {type(exc).__name__}"


def _raw_worker_heartbeats(
    redis: RedisClient,
    *,
    task_prefix: str,
    expected_revision: str,
) -> tuple[dict[str, str], list[str], list[str]]:
    """Return candidate worker IDs by backend plus mismatches and parse errors."""
    heartbeat_prefix = f"{task_prefix}:workers:heartbeat:" if task_prefix else "workers:heartbeat:"
    pattern = f"{heartbeat_prefix}*"
    workers: dict[str, str] = {}
    revision_mismatches: list[str] = []
    errors: list[str] = []
    try:
        keys = list(cast(Any, redis.client.scan_iter(match=pattern)))
        for raw_key in keys:
            key = str(raw_key)
            raw_value = redis.client.get(key)
            if raw_value is None:
                errors.append(f"{key}: heartbeat disappeared during inspection")
                continue
            try:
                payload = json.loads(str(raw_value))
            except (TypeError, ValueError):
                errors.append(f"{key}: malformed heartbeat")
                continue
            if not isinstance(payload, dict):
                errors.append(f"{key}: malformed heartbeat")
                continue
            backend = payload.get("backend")
            revision = payload.get("revision")
            if not isinstance(backend, str) or not isinstance(revision, str):
                errors.append(f"{key}: malformed heartbeat")
                continue
            try:
                require_valid_provider_name(backend)
            except ValueError:
                errors.append(f"{key}: malformed heartbeat")
                continue
            if not key.startswith(heartbeat_prefix):
                errors.append(f"{key}: malformed heartbeat key")
                continue
            worker_id = key.removeprefix(heartbeat_prefix)
            if re.fullmatch(r"orcest-worker-[1-9][0-9]*", worker_id) is None:
                errors.append(f"{key}: malformed heartbeat key")
                continue
            if revision != expected_revision.lower():
                revision_mismatches.append(worker_id)
                continue
            workers[worker_id] = backend
    except (redis_lib.RedisError, TypeError, ValueError) as exc:
        errors.append(f"{pattern}: {type(exc).__name__}")
    return workers, sorted(revision_mismatches), errors


def _provider_metric_total(redis: RedisClient, metric: str) -> tuple[int, str | None]:
    """Read all project-scoped provider counters, rejecting partial data."""
    pattern = f"providers:*:{metric}"
    try:
        total = 0
        for key in redis.scan_iter(pattern):
            parts = key.split(":", 2)
            if len(parts) != 3 or parts[0] != "providers" or parts[2] != metric:
                return 0, f"{pattern}: malformed counter key"
            raw = redis.get(key)
            if raw is None:
                return 0, f"{key}: counter disappeared during inspection"
            value = int(raw)
            if value < 0:
                return 0, f"{key}: counter is negative"
            total += value
    except (redis_lib.RedisError, TypeError, ValueError) as exc:
        return 0, f"{pattern}: {type(exc).__name__}"
    return total, None


def collect_rollout_health(
    redis: RedisClient,
    *,
    expected_revision: str,
    task_prefix: str = "orcest",
    expected_pool_size: int | None = None,
    expected_vmid_start: int | None = None,
    expected_backends: tuple[str, ...] = (),
    baseline_dead_letters: int | None = None,
    baseline_exhausted_skips: int | None = None,
    baseline_rebake_failures: int | None = None,
    max_private_recovery: int = 0,
    require_quiescent: bool = False,
) -> dict[str, Any]:
    """Collect one side-effect-free health snapshot and evaluate rollout gates."""
    revision = get_build_revision()
    checks = [
        _check(
            "checker_revision",
            revision_is_attested(revision) and revision == expected_revision.lower(),
            revision,
            expected_revision.lower(),
        )
    ]

    redis_ok = redis.health_check()
    checks.append(_check("redis", redis_ok, redis_ok, "reachable"))
    if not redis_ok:
        return {
            "ok": False,
            "revision": revision,
            "checks": checks,
            "metrics": {},
        }

    inspection_errors: list[str] = []
    try:
        private_recovery = len(redis.scan_iter("*:private-credential-recovery:*"))
        recovery_intents = len(redis.scan_iter("*:credential-recovery-intent:*"))
    except redis_lib.RedisError as exc:
        private_recovery = -1
        recovery_intents = -1
        inspection_errors.append(f"credential recovery state: {type(exc).__name__}")
    private_total = private_recovery + recovery_intents
    task_queue_depth = 0
    task_pending = 0
    task_lag = 0
    unconsumed_task_streams: list[str] = []
    raw_task_pattern = f"{task_prefix}:tasks:*" if task_prefix else "tasks:*"
    try:
        raw_task_streams = list(cast(Any, redis.client.scan_iter(match=raw_task_pattern)))
    except redis_lib.RedisError as exc:
        raw_task_streams = []
        inspection_errors.append(f"{raw_task_pattern}: {type(exc).__name__}")
    for raw_stream in raw_task_streams:
        stream_name = str(raw_stream)
        work, stream_pending, stream_lag, unconsumed, error = _raw_stream_work(
            redis, stream_name, allow_non_stream=True
        )
        if error is not None:
            inspection_errors.append(error)
        task_queue_depth += work
        task_pending += stream_pending
        task_lag += stream_lag
        if unconsumed:
            unconsumed_task_streams.append(stream_name)

    result_stream = redis._prefixed("results")
    (
        result_work,
        result_pending,
        result_lag,
        unconsumed_results,
        result_error,
    ) = _raw_stream_work(redis, result_stream)
    if result_error is not None:
        inspection_errors.append(result_error)
    queue_depth = task_queue_depth + result_work
    pending = task_pending + result_pending
    lag = task_lag + result_lag
    dead_letter_stream = f"{task_prefix}:dead-letter" if task_prefix else "dead-letter"
    dead_letters, dead_letter_error = _raw_stream_length(redis, dead_letter_stream)
    if dead_letter_error is not None:
        inspection_errors.append(dead_letter_error)
    exhausted_skips, exhausted_error = _provider_metric_total(redis, "exhausted_skip")
    if exhausted_error is not None:
        inspection_errors.append(exhausted_error)
    rebake_failures, rebake_error = _provider_metric_total(redis, "rebake_required_failures")
    if rebake_error is not None:
        inspection_errors.append(rebake_error)

    tracked_worker_ids: set[str] = set()
    tracked_vmids: list[int] = []
    try:
        raw_idle_ids = {
            str(value) for value in cast(set[Any], redis.client.smembers("orcest:pool:idle"))
        }
        raw_active_ids = {
            str(value) for value in cast(list[Any], redis.client.hkeys("orcest:pool:active"))
        }
        pool_idle = len(raw_idle_ids)
        pool_active = len(raw_active_ids)
        if raw_idle_ids & raw_active_ids:
            raise ValueError("worker VMID is both idle and active")
        tracked_vmids = sorted(int(value) for value in raw_idle_ids | raw_active_ids)
        if any(vmid < 1 for vmid in tracked_vmids):
            raise ValueError("worker VMID must be positive")
        tracked_worker_ids = {f"orcest-worker-{vmid}" for vmid in tracked_vmids}
    except (redis_lib.RedisError, TypeError, ValueError) as exc:
        pool_idle = -1
        pool_active = -1
        inspection_errors.append(f"worker pool state: {type(exc).__name__}")

    backend_consumers: dict[str, dict[str, int]] = {}
    backend_heartbeats: dict[str, int] = {}
    expected_backend_counts = dict(Counter(expected_backends))
    worker_revision_mismatches: list[str] = []
    missing_worker_backends: list[str] = []
    unexpected_worker_backends: list[str] = []
    worker_layout_mismatches: list[str] = []
    expected_worker_layout: dict[str, str] = {}
    if expected_backends:
        heartbeat_workers, worker_revision_mismatches, heartbeat_errors = _raw_worker_heartbeats(
            redis,
            task_prefix=task_prefix,
            expected_revision=expected_revision,
        )
        inspection_errors.extend(heartbeat_errors)
    else:
        heartbeat_workers = {}
    exact_pool_layout = expected_vmid_start is not None and bool(expected_backends)
    if exact_pool_layout:
        assert expected_vmid_start is not None
        expected_worker_layout = {
            f"orcest-worker-{expected_vmid_start + index}": backend
            for index, backend in enumerate(expected_backends)
        }
        if tracked_worker_ids != set(expected_worker_layout):
            worker_layout_mismatches.append(
                "tracked-slots:"
                f"{','.join(sorted(tracked_worker_ids)) or 'none'}!=expected-slots:"
                f"{','.join(sorted(expected_worker_layout))}"
            )
        for worker_id, expected_backend in expected_worker_layout.items():
            actual_backend = heartbeat_workers.get(worker_id)
            if actual_backend != expected_backend:
                worker_layout_mismatches.append(
                    f"{worker_id}:{actual_backend or 'missing'}!={expected_backend}"
                )
        candidate_heartbeat_workers = {
            worker_id: backend
            for worker_id, backend in heartbeat_workers.items()
            if worker_id in tracked_worker_ids
        }
    else:
        candidate_heartbeat_workers = heartbeat_workers
    expected_backend_names = set(expected_backend_counts)
    unexpected_worker_backends = sorted(
        f"{worker_id}:{backend}"
        for worker_id, backend in heartbeat_workers.items()
        if backend not in expected_backend_names
        or (exact_pool_layout and worker_id not in tracked_worker_ids)
    )
    for backend, expected_count in expected_backend_counts.items():
        candidate_workers = {
            worker_id
            for worker_id, worker_backend in candidate_heartbeat_workers.items()
            if worker_backend == backend
        }
        stream_counts: dict[str, int] = {}
        for kind, logical_stream in (
            ("pr", task_stream_name(backend)),
            ("issue", task_stream_name(backend, issue=True)),
        ):
            fq_stream = f"{task_prefix}:{logical_stream}" if task_prefix else logical_stream
            consumer_names, error = _raw_stream_consumer_names(redis, fq_stream)
            if error is not None:
                inspection_errors.append(error)
            stream_counts[kind] = len(candidate_workers & consumer_names)
        backend_consumers[backend] = stream_counts
        backend_heartbeats[backend] = len(candidate_workers)
        if (
            any(count != expected_count for count in stream_counts.values())
            or backend_heartbeats[backend] != expected_count
        ):
            missing_worker_backends.append(backend)

    metrics = {
        "queue_depth": queue_depth,
        "pending": pending,
        "lag": lag,
        "task_queue_depth": task_queue_depth,
        "task_pending": task_pending,
        "task_lag": task_lag,
        "result_work": result_work,
        "result_pending": result_pending,
        "result_lag": result_lag,
        "unconsumed_task_streams": sorted(unconsumed_task_streams),
        "unconsumed_results": unconsumed_results,
        "dead_letters": dead_letters,
        "private_credential_checkpoints": private_recovery,
        "credential_recovery_intents": recovery_intents,
        "provider_exhausted_skips": exhausted_skips,
        "provider_rebake_failures": rebake_failures,
        "pool_idle": pool_idle,
        "pool_active": pool_active,
        "backend_consumers": backend_consumers,
        "backend_heartbeats": backend_heartbeats,
        "expected_backend_counts": expected_backend_counts,
        "worker_revision_mismatches": worker_revision_mismatches,
        "missing_worker_backends": sorted(missing_worker_backends),
        "unexpected_worker_backends": unexpected_worker_backends,
        "expected_worker_layout": expected_worker_layout,
        "worker_layout_mismatches": sorted(worker_layout_mismatches),
        "inspection_errors": sorted(inspection_errors),
    }
    checks.append(
        _check(
            "redis_inspection",
            not inspection_errors,
            sorted(inspection_errors),
            "all required Redis state is inspectable",
        )
    )
    checks.append(
        _check(
            "private_recovery_state",
            private_total >= 0 and private_total <= max_private_recovery,
            private_total,
            f"<= {max_private_recovery}",
        )
    )
    checks.append(
        _check(
            "consumer_groups",
            not unconsumed_task_streams and not unconsumed_results,
            sorted(unconsumed_task_streams) + ([result_stream] if unconsumed_results else []),
            "every work-bearing stream has a consumer group with an active consumer",
        )
    )
    if baseline_dead_letters is not None:
        checks.append(
            _check(
                "dead_letters",
                dead_letters <= baseline_dead_letters,
                dead_letters,
                f"<= baseline {baseline_dead_letters}",
            )
        )
    if baseline_exhausted_skips is not None:
        checks.append(
            _check(
                "provider_exhausted_skips",
                exhausted_skips <= baseline_exhausted_skips,
                exhausted_skips,
                f"<= baseline {baseline_exhausted_skips}",
            )
        )
    if baseline_rebake_failures is not None:
        checks.append(
            _check(
                "provider_rebake_failures",
                rebake_failures <= baseline_rebake_failures,
                rebake_failures,
                f"<= baseline {baseline_rebake_failures}",
            )
        )
    if expected_pool_size is not None:
        pool_total = pool_idle + pool_active if pool_idle >= 0 and pool_active >= 0 else -1
        checks.append(
            _check(
                "pool_size",
                pool_total == expected_pool_size,
                pool_total,
                str(expected_pool_size),
            )
        )
    if expected_backends:
        checks.extend(
            [
                _check(
                    "worker_backends",
                    not missing_worker_backends
                    and not unexpected_worker_backends
                    and not worker_layout_mismatches,
                    sorted(missing_worker_backends)
                    + unexpected_worker_backends
                    + sorted(worker_layout_mismatches),
                    "the exact expected worker capacity has heartbeats and matching "
                    "PR/issue consumers",
                ),
                _check(
                    "worker_revisions",
                    not worker_revision_mismatches,
                    worker_revision_mismatches,
                    "all live worker heartbeats report the expected revision",
                ),
            ]
        )
    if require_quiescent:
        checks.extend(
            [
                _check("queue_quiescent", queue_depth == 0, queue_depth, "0"),
                _check("pool_quiescent", pool_active == 0, pool_active, "0"),
            ]
        )

    return {
        "ok": all(check["passed"] for check in checks),
        "revision": revision,
        "checks": checks,
        "metrics": metrics,
    }
