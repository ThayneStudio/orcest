"""Reversible, secret-safe task-stream fencing for staged migrations."""

from __future__ import annotations

import re
from typing import Any, cast

import redis as redis_lib

from orcest.shared.models import require_valid_provider_name
from orcest.shared.redis_client import RedisClient

_QUARANTINE_ID_RE = re.compile(r"^[A-Za-z0-9._-]{1,128}$")


class TaskStreamQuarantineError(RuntimeError):
    """Task streams could not be fenced or restored without data loss."""


def _stream_summary(redis: RedisClient, key: str) -> dict[str, int]:
    if str(cast(Any, redis.client.type(key))) != "stream":
        raise TaskStreamQuarantineError(f"task key {key!r} is not a Redis stream")
    try:
        groups = cast(list[dict[str, Any]], redis.client.xinfo_groups(key))
        pending = 0
        lag = 0
        for group in groups:
            if group.get("pending") is None or group.get("lag") is None:
                raise TaskStreamQuarantineError(f"consumer state is unavailable for {key!r}")
            pending += int(group["pending"])
            lag += max(int(group["lag"]), 0)
        return {
            "length": int(cast(Any, redis.client.xlen(key))),
            "groups": len(groups),
            "pending": pending,
            "lag": lag,
        }
    except (redis_lib.RedisError, TypeError, ValueError) as exc:
        if isinstance(exc, TaskStreamQuarantineError):
            raise
        raise TaskStreamQuarantineError(
            f"could not inspect task stream {key!r}: {type(exc).__name__}"
        ) from exc


def _validate_source_key(key: str, task_prefix: str) -> str:
    prefix = f"{task_prefix}:" if task_prefix else ""
    logical = key.removeprefix(prefix)
    match = re.fullmatch(r"tasks:(?:issue:)?([a-z0-9_]{1,64})", logical)
    if match is None:
        raise TaskStreamQuarantineError(f"unexpected task stream key {key!r}")
    try:
        require_valid_provider_name(match.group(1))
    except ValueError as exc:
        raise TaskStreamQuarantineError(f"unexpected task stream key {key!r}") from exc
    return logical


def quarantine_task_streams(
    redis: RedisClient,
    *,
    task_prefix: str,
    quarantine_id: str,
) -> dict[str, Any]:
    """Atomically rename active provider task streams out of worker discovery."""
    if _QUARANTINE_ID_RE.fullmatch(quarantine_id) is None:
        raise TaskStreamQuarantineError("invalid quarantine ID")
    scan_pattern = f"{task_prefix}:tasks:*" if task_prefix else "tasks:*"
    try:
        sources = sorted({str(key) for key in redis.client.scan_iter(match=scan_pattern)})
    except redis_lib.RedisError as exc:
        raise TaskStreamQuarantineError(
            f"could not enumerate task streams: {type(exc).__name__}"
        ) from exc
    if not sources:
        raise TaskStreamQuarantineError("no active task streams were found")

    mappings: list[dict[str, Any]] = []
    for source in sources:
        logical = _validate_source_key(source, task_prefix)
        destination = (
            f"{task_prefix}:quarantine:{quarantine_id}:{logical}"
            if task_prefix
            else f"quarantine:{quarantine_id}:{logical}"
        )
        summary = _stream_summary(redis, source)
        mappings.append({"source": source, "quarantine": destination, **summary})

    pipe = redis.client.pipeline()
    try:
        watch_keys = [value for item in mappings for value in (item["source"], item["quarantine"])]
        if watch_keys:
            pipe.watch(*watch_keys)
        for item in mappings:
            if str(cast(Any, pipe.type(item["source"]))) != "stream":
                raise TaskStreamQuarantineError("task stream changed during quarantine preflight")
            if int(cast(Any, pipe.exists(item["quarantine"]))) != 0:
                raise TaskStreamQuarantineError("quarantine destination already exists")
        pipe.multi()
        for item in mappings:
            pipe.rename(item["source"], item["quarantine"])
        pipe.execute()
    except redis_lib.WatchError as exc:
        raise TaskStreamQuarantineError("task streams changed during quarantine") from exc
    except redis_lib.RedisError as exc:
        raise TaskStreamQuarantineError(
            f"could not quarantine task streams: {type(exc).__name__}"
        ) from exc
    finally:
        pipe.reset()
    return {"ok": True, "operation": "quarantine", "streams": mappings}


def restore_task_streams(
    redis: RedisClient,
    *,
    task_prefix: str,
    quarantine_id: str,
) -> dict[str, Any]:
    """Restore quarantined streams, replacing only provably empty active streams."""
    if _QUARANTINE_ID_RE.fullmatch(quarantine_id) is None:
        raise TaskStreamQuarantineError("invalid quarantine ID")
    quarantine_prefix = (
        f"{task_prefix}:quarantine:{quarantine_id}:"
        if task_prefix
        else f"quarantine:{quarantine_id}:"
    )
    try:
        quarantined = sorted(
            {str(key) for key in redis.client.scan_iter(match=f"{quarantine_prefix}tasks:*")}
        )
    except redis_lib.RedisError as exc:
        raise TaskStreamQuarantineError(
            f"could not enumerate quarantined streams: {type(exc).__name__}"
        ) from exc
    if not quarantined:
        raise TaskStreamQuarantineError("no quarantined task streams were found")

    mappings: list[dict[str, Any]] = []
    active_prefix = f"{task_prefix}:" if task_prefix else ""
    for quarantine in quarantined:
        logical = quarantine.removeprefix(quarantine_prefix)
        source = f"{active_prefix}{logical}"
        _validate_source_key(source, task_prefix)
        summary = _stream_summary(redis, quarantine)
        active_type = str(cast(Any, redis.client.type(source)))
        if active_type not in {"none", "stream"}:
            raise TaskStreamQuarantineError(f"active task key {source!r} is not a stream")
        if active_type == "stream":
            active_summary = _stream_summary(redis, source)
            if active_summary["length"] or active_summary["pending"] or active_summary["lag"]:
                raise TaskStreamQuarantineError(
                    f"active task stream {source!r} contains work; refusing to overwrite it"
                )
        mappings.append({"source": source, "quarantine": quarantine, **summary})

    pipe = redis.client.pipeline()
    try:
        watch_keys = [value for item in mappings for value in (item["source"], item["quarantine"])]
        pipe.watch(*watch_keys)
        for item in mappings:
            if str(cast(Any, pipe.type(item["quarantine"]))) != "stream":
                raise TaskStreamQuarantineError("quarantined stream changed during restore")
            active_type = str(cast(Any, pipe.type(item["source"])))
            if active_type == "stream":
                active_summary = _stream_summary(redis, item["source"])
                if active_summary["length"] or active_summary["pending"] or active_summary["lag"]:
                    raise TaskStreamQuarantineError("active task streams changed during restore")
            elif active_type != "none":
                raise TaskStreamQuarantineError("active task key changed during restore")
        pipe.multi()
        for item in mappings:
            pipe.delete(item["source"])
            pipe.rename(item["quarantine"], item["source"])
        pipe.execute()
    except redis_lib.WatchError as exc:
        raise TaskStreamQuarantineError("task streams changed during restore") from exc
    except redis_lib.RedisError as exc:
        raise TaskStreamQuarantineError(
            f"could not restore task streams: {type(exc).__name__}"
        ) from exc
    finally:
        pipe.reset()
    return {"ok": True, "operation": "restore", "streams": mappings}
