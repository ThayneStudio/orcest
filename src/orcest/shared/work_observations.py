"""Read-only dashboard evidence; never consulted by scheduling decisions.

Observations and first-start evidence are retained in Redis (and its configured
persistence). Attempts are bounded per work item. Loss of Redis is reported as
missing history, not reconstructed success. All public payloads are allowlisted.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from functools import wraps
from typing import Any

from orcest.shared.redis_client import RedisClient

log = logging.getLogger(__name__)


def best_effort(fn: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(fn)
    def guarded(*args: Any, **kwargs: Any) -> Any:
        try:
            return fn(*args, **kwargs)
        except Exception:
            log.warning("Dashboard observation unavailable: %s", fn.__name__)
            return None

    return guarded


def work_key(repo: str, kind: str, number: int) -> str:
    if kind not in {"issue", "pr"} or number < 1:
        raise ValueError("Invalid work reference")
    return f"dashboard:work:{kind}:{repo}:{number}"


def full_key(redis: RedisClient, key: str) -> str:
    return f"{redis.key_prefix}:{key}" if redis.key_prefix else key


@best_effort
def project_observation(redis: RedisClient, repo: str, interval: int, pool: Any) -> None:
    redis.hset_mapping(
        "dashboard:project",
        {
            "repo": repo,
            "prefix": redis.key_prefix,
            "observed_at": str(time.time()),
            "poll_interval": str(interval),
            "accounts": json.dumps(pool.dashboard_accounts() if pool is not None else []),
        },
    )


@best_effort
def observe(redis: RedisClient, repo: str, kind: str, state: Any) -> None:
    now = time.time()
    action = state.action.value
    data = {
        "repo": repo,
        "kind": kind,
        "number": str(state.number),
        "prefix": redis.key_prefix,
        "title": state.title[:500],
        "description": (getattr(state, "body", "") or "")[:6000],
        "action": action,
        "observed_at": str(now),
        "blockers": json.dumps(getattr(state, "open_blockers", [])),
        "branch": getattr(state, "branch", ""),
        "head_sha": getattr(state, "head_sha", ""),
        "needs_human": "1" if action == "skip_labeled" else "0",
        "outcome": "",
        "completed_at": "",
    }
    key = work_key(repo, kind, state.number)
    redis.hset_mapping(key, data)
    redis.client.hsetnx(full_key(redis, key), "discovered_at", str(now))
    redis.client.zadd(full_key(redis, "dashboard:tracked"), {key: now})


@best_effort
def queued(redis: RedisClient, task: Any) -> None:
    redis.hset_mapping(
        work_key(task.repo, task.resource_type, task.resource_id),
        {
            "action": "skip_queued",
            "queued_at": str(time.time()),
            "queued_task_id": task.id,
        },
    )


@best_effort
def attempt_started(
    redis: RedisClient, task: Any, worker_id: str, *, worker_prefix: str | None = None
) -> None:
    now = time.time()
    key = work_key(task.repo, task.resource_type, task.resource_id)
    fq = full_key(redis, key)
    redis.client.hsetnx(fq, "started_at", str(now))
    redis.hset_mapping(
        key,
        {
            "repo": task.repo,
            "kind": task.resource_type,
            "number": str(task.resource_id),
            "prefix": redis.key_prefix,
        },
    )
    redis.hset_mapping(
        f"dashboard:attempt:{task.id}",
        {
            "task_id": task.id,
            "worker_id": worker_id,
            "worker_prefix": redis.key_prefix if worker_prefix is None else worker_prefix,
            "provider": task.provider,
            "model": task.model or "",
            "account_id": task.provider_account,
            "started_at": str(now),
            "work_key": key,
            "status": "running",
            "branch": task.branch or "",
            "head_sha": task.snapshot_head_sha,
            "output_prefix": redis.key_prefix,
        },
    )
    index = full_key(redis, key + ":attempts")
    redis.client.zadd(index, {task.id: now})
    redis.client.zremrangebyrank(index, 0, -51)
    redis.client.expire(full_key(redis, f"dashboard:attempt:{task.id}"), 30 * 86400)


@best_effort
def attempt_finished(redis: RedisClient, task: Any, status: str) -> None:
    redis.hset_mapping(
        f"dashboard:attempt:{task.id}",
        {
            "status": status,
            "finished_at": str(time.time()),
        },
    )


@best_effort
def human_reason(redis: RedisClient, task: Any, reason: str) -> None:
    for field in ("token", "credential", "claude_token"):
        secret = getattr(task, field, "")
        if isinstance(secret, str) and secret:
            reason = reason.replace(secret, "[REDACTED]")
    redis.hset_mapping(
        work_key(task.repo, task.resource_type, task.resource_id),
        {
            "needs_human": "1",
            "human_reason": reason[:2000],
        },
    )


@best_effort
def merged(redis: RedisClient, repo: str, number: int) -> None:
    redis.hset_mapping(
        work_key(repo, "pr", number),
        {
            "outcome": "merged",
            "completed_at": str(time.time()),
            "needs_human": "0",
        },
    )


@best_effort
def link_publication(redis: RedisClient, repo: str, issue: int, pr: str) -> None:
    if pr.isdigit():
        redis.hset(work_key(repo, "issue", issue), "related_pr", pr)


@best_effort
def reconcile_missing(redis: RedisClient, repo: str, token: str, seen: set[str]) -> None:
    """Check at most five previously tracked resources per poll, rotating fairly.

    This is observation only: it never invokes selectors or changes GitHub.
    Missing ready labels do not mark an issue complete. Terminal records are kept
    for 30 days; ongoing work remains tracked.
    """
    from orcest.orchestrator import gh

    index = full_key(redis, "dashboard:tracked")
    keys = redis.client.zrange(index, 0, -1)
    candidates = [key for key in keys if isinstance(key, str) and key not in seen]
    if not candidates:
        return
    cursor = int(redis.hget("dashboard:project", "reconcile_cursor") or 0)
    for offset in range(min(5, len(candidates))):
        key = candidates[(cursor + offset) % len(candidates)]
        item = redis.hgetall(key)
        if not item or item.get("repo") != repo:
            continue
        if (
            item.get("outcome")
            and time.time() - float(item.get("completed_at") or "0") > 30 * 86400
        ):
            redis.client.zrem(index, key)
            redis.client.expire(full_key(redis, key), 86400)
            redis.client.expire(full_key(redis, key + ":attempts"), 86400)
            continue
        if item.get("outcome") == "merged":
            continue
        try:
            number = int(item["number"])
            source = (
                gh.get_pr(repo, number, token)
                if item["kind"] == "pr"
                else gh.get_issue(repo, number, token)
            )
            state = (source.get("state") or "").upper()
            fields = {
                "observed_at": str(time.time()),
                "title": str(source.get("title", item.get("title", "")))[:500],
            }
            if state == "MERGED":
                fields.update(outcome="merged", completed_at=str(time.time()), needs_human="0")
            elif state == "CLOSED":
                fields.update(
                    outcome="closed",
                    completed_at=item.get("completed_at") or str(time.time()),
                    needs_human="0",
                )
            else:
                fields.update(action="not_in_discovery", outcome="", completed_at="")
            if state not in {"OPEN", "CLOSED", "MERGED"}:
                continue
            redis.hset_mapping(key, fields)
        except Exception:
            # Preserve stale evidence on failed reads; do not treat absence as done.
            continue
    redis.hset("dashboard:project", "reconcile_cursor", str(cursor + 5))
