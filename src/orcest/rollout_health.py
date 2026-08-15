"""Read-only rollout health gates for deployment and post-deploy watches."""

from __future__ import annotations

from typing import Any, cast

import redis as redis_lib

from orcest.revision import get_build_revision, revision_is_attested
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
        if any(group.get("pending") is None or group.get("lag") is None for group in groups):
            return 0, 0, 0, False, f"{stream}: consumer group work is unavailable"
        pending = sum(int(group["pending"]) for group in groups)
        lag = sum(max(int(group["lag"]), 0) for group in groups)
    except (redis_lib.RedisError, TypeError, ValueError) as exc:
        # Do not include Redis exception text: ACL and connection messages can
        # contain endpoints or other deployment details.
        return 0, 0, 0, False, f"{stream}: {type(exc).__name__}"
    return pending + lag, pending, lag, False, None


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
    dead_letters, dead_letter_error = _raw_stream_length(redis, redis._prefixed("dead-letter"))
    if dead_letter_error is not None:
        inspection_errors.append(dead_letter_error)
    exhausted_skips, exhausted_error = _provider_metric_total(redis, "exhausted_skip")
    if exhausted_error is not None:
        inspection_errors.append(exhausted_error)
    rebake_failures, rebake_error = _provider_metric_total(redis, "rebake_required_failures")
    if rebake_error is not None:
        inspection_errors.append(rebake_error)

    try:
        pool_idle = int(cast(Any, redis.client.scard("orcest:pool:idle")))
        pool_active = int(cast(Any, redis.client.hlen("orcest:pool:active")))
    except (redis_lib.RedisError, TypeError, ValueError) as exc:
        pool_idle = -1
        pool_active = -1
        inspection_errors.append(f"worker pool state: {type(exc).__name__}")

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
            "every non-empty work stream has a consumer group",
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
