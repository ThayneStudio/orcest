"""Secret-safe evidence collection for multi-provider rollout canaries."""

from __future__ import annotations

import re
from typing import Any, cast

import redis as redis_lib

from orcest.shared.models import RESULTS_STREAM, require_valid_provider_name
from orcest.shared.redis_client import RedisClient

_CANARY_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")


class CanaryEvidenceError(RuntimeError):
    """A canary could not be proved without exposing task payloads."""


def collect_canary_evidence(
    redis: RedisClient,
    *,
    task_prefix: str,
    canaries: dict[str, str],
) -> dict[str, Any]:
    """Prove one source task and one completed result per provider/task ID.

    Only non-secret identifiers are returned. Task and result payloads are
    inspected in memory but are never included in output or error messages.
    """
    if not canaries:
        raise CanaryEvidenceError("at least one provider canary is required")
    if len(set(canaries.values())) != len(canaries):
        raise CanaryEvidenceError("each provider canary must use a unique task ID")
    for provider, task_id in canaries.items():
        try:
            require_valid_provider_name(provider)
        except ValueError as exc:
            raise CanaryEvidenceError(f"invalid canary provider {provider!r}") from exc
        if not _CANARY_ID_RE.fullmatch(task_id):
            raise CanaryEvidenceError(f"invalid canary task ID for provider {provider}")

    result_stream = redis._prefixed(RESULTS_STREAM)
    try:
        result_entries = list(cast(Any, redis.client.xrange(result_stream)))
    except redis_lib.RedisError as exc:
        raise CanaryEvidenceError(
            f"could not inspect the project result stream: {type(exc).__name__}"
        ) from exc

    evidence: list[dict[str, str]] = []
    for provider, task_id in sorted(canaries.items()):
        logical_streams = (f"tasks:{provider}", f"tasks:issue:{provider}")
        source_matches: list[tuple[str, str, dict[str, Any]]] = []
        for logical_stream in logical_streams:
            source_stream = f"{task_prefix}:{logical_stream}" if task_prefix else logical_stream
            try:
                source_entries = list(cast(Any, redis.client.xrange(source_stream)))
            except redis_lib.RedisError as exc:
                raise CanaryEvidenceError(
                    f"could not inspect source streams for {provider}: {type(exc).__name__}"
                ) from exc
            source_matches.extend(
                (source_stream, str(entry_id), fields)
                for entry_id, fields in source_entries
                if isinstance(fields, dict) and fields.get("id") == task_id
            )
        result_matches = [
            (str(entry_id), fields)
            for entry_id, fields in result_entries
            if isinstance(fields, dict) and fields.get("task_id") == task_id
        ]
        if len(source_matches) != 1:
            raise CanaryEvidenceError(
                f"provider {provider} expected one source entry, found {len(source_matches)}"
            )
        if len(result_matches) != 1:
            raise CanaryEvidenceError(
                f"provider {provider} expected one terminal result, found {len(result_matches)}"
            )
        result_entry_id, result_fields = result_matches[0]
        status = str(result_fields.get("status", ""))
        if status != "completed":
            raise CanaryEvidenceError(f"provider {provider} result status is {status or 'missing'}")
        worker_id = str(result_fields.get("worker_id", ""))
        if not worker_id or not _CANARY_ID_RE.fullmatch(worker_id):
            raise CanaryEvidenceError(f"provider {provider} result has an invalid worker ID")
        source_stream, source_entry_id, _source_fields = source_matches[0]
        evidence.append(
            {
                "provider": provider,
                "task_id": task_id,
                "source_stream": source_stream,
                "source_entry_id": source_entry_id,
                "result_stream": result_stream,
                "result_entry_id": result_entry_id,
                "worker_id": worker_id,
                "status": status,
            }
        )
    return {"ok": True, "canaries": evidence}
