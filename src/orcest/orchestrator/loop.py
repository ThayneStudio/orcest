"""Orchestrator main loop.

Polls GitHub for actionable PRs, enqueues fix tasks to Redis, and consumes
results from workers. Uses graceful shutdown on SIGTERM/SIGINT with
interruptible sleep (1-second chunks) for responsive termination.
"""

import json
import logging
import math
import re
import signal
import sys
import time
import uuid
from typing import Any, Callable

from orcest.orchestrator import gh
from orcest.orchestrator.deployment import DeploymentError, run_deployment
from orcest.orchestrator.gh import GhRateLimitError
from orcest.orchestrator.issue_ops import (
    IssueAction,
    clear_attempts as clear_issue_attempts,
    discover_actionable_issues,
    set_usage_exhausted_cooldown as set_issue_usage_exhausted_cooldown,
)
from orcest.orchestrator.pr_ops import (
    PRAction,
    PRState,
    clear_attempts,
    clear_attempts_if_head_sha,
    clear_review_retrigger,
    clear_total_attempts,
    discover_actionable_prs,
    get_stale_retrigger_sha,
    increment_total_attempts,
    record_self_cancelled_stale_runs,
    set_review_retrigger_sha,
    set_stale_retrigger_sha,
    set_usage_exhausted_cooldown,
)
from orcest.orchestrator.provider_pool import ProviderPool
from orcest.orchestrator.task_publisher import (
    publish_fix_task,
    publish_followup_task,
    publish_issue_task,
    publish_rebase_task,
    rerun_all_transient_ci,
)
from orcest.orchestrator.trace_archiver import TraceArchiver
from orcest.orchestrator.usage_check import get_token_reset_time
from orcest.shared.config import LabelConfig, OrchestratorConfig, ProjectConfig
from orcest.shared.coordination import (
    clear_backoff,
    clear_pending_task_if_matches,
    clear_transient_failure_count,
    compute_pending_task_ttl,
    get_pending_task,
    increment_transient_failure_count,
    set_backoff_cooldown,
)
from orcest.shared.logging import setup_logging
from orcest.shared.models import (
    CONSUMER_GROUP,
    TRANSIENT_SUMMARY_PREFIX,
    ResultStatus,
    Task,
    TaskResult,
    is_claude_provider,
    task_stream_name,
)
from orcest.shared.providers import ProviderEntry
from orcest.shared.redis_client import RedisClient

RESULTS_STREAM = "results"
RESULTS_GROUP = "orchestrator"

# Observability counters (Task 8 hygiene).
# Per-provider under providers:{provider}: namespace, project-scoped via key_prefix.
# exhausted_skip incremented on full pool exhaustion (per provider in the pool).
# rebake_required_failures incremented on clean "rebake worker image" FAILED results.
# credential_refresh_failures incremented when an OAuth provider reports an unusable
# refreshed credential blob, e.g. Grok JSON without a refresh_token.
_PROVIDER_EXHAUSTED_SKIP_KEY = "providers:exhausted_skip"  # aggregate for compat
_PROVIDER_EXHAUSTED_SKIP_TTL_SECONDS = 24 * 3600  # 24 hours
_REBAKE_REQUIRED_FAILURES_TTL_SECONDS = 24 * 3600  # 24 hours
_CREDENTIAL_REFRESH_FAILURES_TTL_SECONDS = 24 * 3600  # 24 hours
_USAGE_EXHAUSTED_RESULT_KEY = "tokens:usage_exhausted_result"
_USAGE_EXHAUSTED_RESULT_TTL_SECONDS = 24 * 3600  # 24 hours
# Credential write-back: rotated OAuth blobs (Grok/Codex) keyed by provider
# account. The canonical store is shared under the task key prefix so all
# project orchestrators that share the same OAuth account see the newest blob.
# Legacy project-prefixed identity fields are still read/written for rollback.
_CREDENTIAL_OVERRIDES_KEY = "providers:credential_overrides"
_SHARED_CREDENTIAL_OVERRIDES_KEY = "providers:credential_overrides"
_TASK_PROVIDER_ACCOUNTS_KEY = "providers:task_accounts"
_TASK_PROVIDER_ACCOUNT_PREFIX = "providers:task_account:"

# Shared project heartbeats used to choose one deterministic issue-discovery
# bypass across the one-container-per-project deployment topology.
_ISSUE_DISCOVERY_PROJECTS_KEY = "issue_discovery:projects"
_ISSUE_DISCOVERY_CYCLE_SEQ_KEY = "issue_discovery:cycle_seq"
_ISSUE_DISCOVERY_TURN_KEY = "issue_discovery:turn"


class _RetryableResultError(RuntimeError):
    """Result handling failed before its durable side effects committed."""


def _configured_task_providers(
    config: OrchestratorConfig,
    token_pools: dict[str, ProviderPool] | None = None,
) -> list[str]:
    """Return provider names whose task streams this orchestrator may touch."""
    providers: list[str] = []

    def add(provider: str) -> None:
        normalized = str(provider or "").strip()
        if normalized and normalized not in providers:
            providers.append(normalized)

    add(config.default_runner)
    for entry in getattr(config, "providers", []) or []:
        add(entry.provider)
    for project in config.projects:
        if project.providers:
            for entry in project.providers:
                add(entry.provider)
        elif project.claude_tokens:
            add("claude")
    for pool in (token_pools or {}).values():
        for provider in pool.provider_names:
            add(provider)
    return providers


def _configured_task_streams(
    config: OrchestratorConfig,
    *,
    issue: bool = False,
    token_pools: dict[str, ProviderPool] | None = None,
) -> list[str]:
    return [
        task_stream_name(provider, issue=issue)
        for provider in _configured_task_providers(config, token_pools=token_pools)
    ]


def _published_provider_for_entry(entry: ProviderEntry, default_runner: str) -> str:
    """Return the provider stream a selected credential should publish to."""
    if (
        entry.provider == "claude"
        and entry.source == "legacy_claude_tokens"
        and is_claude_provider(default_runner)
    ):
        return default_runner
    return entry.provider


def _unclaimed_task_count(
    redis: RedisClient,
    streams: list[str],
    group: str,
    logger: logging.Logger,
) -> int:
    total = 0
    for stream in streams:
        try:
            total += redis.stream_unread_count(stream, group)
        except Exception:
            logger.debug("Failed to count unread task entries on %s", stream, exc_info=True)
    return total


def _issue_discovery_priority(
    task_redis: RedisClient,
    projects: list[ProjectConfig],
    interval_seconds: int,
    logger: logging.Logger,
) -> str | None:
    """Return the shared project identity holding the leased next turn."""
    if not projects:
        return None
    now = time.time()
    identities = [project.key_prefix or project.repo for project in projects]
    try:
        for identity in identities:
            task_redis.hset(_ISSUE_DISCOVERY_PROJECTS_KEY, identity, str(now))
        heartbeats = task_redis.hgetall(_ISSUE_DISCOVERY_PROJECTS_KEY)
        stale_after = max(3, int(interval_seconds) * 3)
        live: list[str] = []
        stale: list[str] = []
        for identity, raw_timestamp in heartbeats.items():
            try:
                is_live = now - float(raw_timestamp) <= stale_after
            except (TypeError, ValueError):
                is_live = False
            (live if is_live else stale).append(identity)
        if stale:
            task_redis.hdel(_ISSUE_DISCOVERY_PROJECTS_KEY, *stale)
        if not live:
            return None
        live.sort()
        if len(projects) > 1:
            # A legacy process polling every project owns this entire cycle;
            # advancing once per call is itself the acknowledgement that the
            # selected project will be visited below.
            sequence = task_redis.incr(_ISSUE_DISCOVERY_CYCLE_SEQ_KEY)
            return live[(sequence - 1) % len(live)]
        return task_redis.claim_round_robin_turn(
            _ISSUE_DISCOVERY_TURN_KEY,
            _ISSUE_DISCOVERY_CYCLE_SEQ_KEY,
            live,
            max(1, int(interval_seconds)),
        )
    except Exception:
        logger.debug("Failed to coordinate shared issue-discovery turn", exc_info=True)
        return None


def _legacy_credential_override_key(key: str) -> str:
    """Return the legacy identity-shaped field for an account key."""
    parts = key.split(":")
    if len(parts) == 2:
        provider, credential_hash = parts
        return f"{provider}::{credential_hash}"
    return key


def _persist_credential_override(
    redis: RedisClient,
    key: str,
    blob: str,
    minted_at: float,
    logger: logging.Logger,
    shared_redis: RedisClient | None = None,
) -> bool:
    """Persist a rotated credential blob.

    ``key`` is the account key returned by ProviderPool. The shared store uses
    that account key directly; the project-prefixed legacy store uses the old
    identity-shaped field for rollback compatibility.
    """
    payload = json.dumps({"blob": blob, "minted_at": minted_at})
    shared_stored = True
    if shared_redis is not None:
        try:
            shared_stored = shared_redis.hset_json_if_newer(
                _SHARED_CREDENTIAL_OVERRIDES_KEY,
                key,
                payload,
                minted_at,
            )
        except Exception as exc:
            # Shared Redis is the canonical cross-project copy. The result must
            # remain pending so this exact rotation can be retried; merely
            # caching it in this process would lose it on restart and leave
            # sibling project orchestrators on an invalidated refresh token.
            raise _RetryableResultError(
                f"failed to persist shared credential override for account {key}"
            ) from exc
    try:
        redis.hset_json_if_newer(
            _CREDENTIAL_OVERRIDES_KEY,
            _legacy_credential_override_key(key),
            payload,
            minted_at,
        )
    except Exception as exc:
        if shared_redis is None:
            raise _RetryableResultError(
                f"failed to persist credential override for account {key}"
            ) from exc
        logger.warning(
            "Failed to persist credential override for account %s", key, exc_info=True
        )
    return shared_stored


def _refresh_shared_credential_override(
    redis: RedisClient,
    pool: "ProviderPool",
    account: str,
    logger: logging.Logger,
) -> None:
    """Refresh one account from the canonical shared override hash."""
    try:
        raw = redis.hget(_SHARED_CREDENTIAL_OVERRIDES_KEY, account)
        if not raw:
            return
        obj = json.loads(raw)
        blob = str(obj.get("blob", ""))
        minted_at = float(obj.get("minted_at", 0))
        if _credential_override_is_usable(account, blob):
            pool.seed_credential_override(account, blob, minted_at)
    except (json.JSONDecodeError, TypeError, ValueError, AttributeError):
        logger.warning("Ignoring malformed shared credential override for %s", account)
    except Exception:
        logger.warning(
            "Failed to refresh shared credential override for %s",
            account,
            exc_info=True,
        )


def _load_credential_overrides(
    redis: RedisClient,
    pool: "ProviderPool",
    logger: logging.Logger,
    shared_redis: RedisClient | None = None,
) -> None:
    """Seed a pool's credential overrides from Redis at startup."""
    stores: list[tuple[str, RedisClient, str]] = []
    if shared_redis is not None:
        stores.append(("shared", shared_redis, _SHARED_CREDENTIAL_OVERRIDES_KEY))
    stores.append(("project", redis, _CREDENTIAL_OVERRIDES_KEY))
    try:
        stored_by_scope = [
            (scope, client.hgetall(key)) for scope, client, key in stores
        ]
    except Exception:
        logger.warning("Failed to load credential overrides", exc_info=True)
        return
    for _scope, stored in stored_by_scope:
        for key, raw in stored.items():
            try:
                obj = json.loads(raw)
                blob = str(obj.get("blob", ""))
                if _credential_override_is_usable(key, blob):
                    pool.seed_credential_override(
                        key, blob, float(obj.get("minted_at", 0))
                    )
            except (json.JSONDecodeError, ValueError, TypeError):
                continue


def _persist_task_provider_account(
    redis: RedisClient,
    task_id: str,
    entry: ProviderEntry,
    ttl_seconds: int,
    logger: logging.Logger,
) -> None:
    """Persist non-secret task -> provider account mapping for restart recovery."""
    try:
        # One expiring key per task makes value+TTL atomic and prevents active
        # projects from extending orphaned mappings in a shared hash forever.
        redis.set_ex(
            f"{_TASK_PROVIDER_ACCOUNT_PREFIX}{task_id}",
            entry.account_key(),
            ttl=max(1, int(ttl_seconds)),
        )
    except Exception:
        logger.debug(
            "Failed to persist provider account mapping for task %s",
            task_id,
            exc_info=True,
        )


def _load_task_provider_account(
    redis: RedisClient,
    task_id: str,
    logger: logging.Logger,
) -> str | None:
    try:
        account = redis.get(f"{_TASK_PROVIDER_ACCOUNT_PREFIX}{task_id}")
        if not account:
            # Rolling-upgrade compatibility for tasks published before the
            # per-task key migration.
            account = redis.hget(_TASK_PROVIDER_ACCOUNTS_KEY, task_id)
    except Exception:
        logger.debug(
            "Failed to load provider account mapping for task %s",
            task_id,
            exc_info=True,
        )
        return None
    if account and ":" in account:
        return account
    return None


def _clear_task_provider_account(
    redis: RedisClient,
    task_id: str,
    logger: logging.Logger,
) -> None:
    try:
        redis.delete(f"{_TASK_PROVIDER_ACCOUNT_PREFIX}{task_id}")
        # Also clear a legacy hash field when completing an in-flight task
        # published by an older orchestrator.
        redis.hdel(_TASK_PROVIDER_ACCOUNTS_KEY, task_id)
    except Exception:
        logger.debug(
            "Failed to clear provider account mapping for task %s",
            task_id,
            exc_info=True,
        )


def _increment_provider_counter(
    redis: RedisClient,
    provider: str,
    metric: str,
    ttl_seconds: int,
    logger: logging.Logger,
) -> None:
    provider = provider.strip()
    metric = metric.strip()
    if not provider or not metric:
        return
    key = f"providers:{provider}:{metric}"
    try:
        count = redis.incr(key)
        if count == 1:
            redis.expire(key, ttl_seconds)
    except Exception:
        logger.debug(
            "Failed to increment provider counter %s for provider %s",
            metric,
            provider,
            exc_info=True,
        )


def _record_credential_refresh_failure_if_needed(
    redis: RedisClient,
    account: str | None,
    blob: str,
    logger: logging.Logger,
) -> None:
    if not account or _credential_override_is_usable(account, blob):
        return
    provider, _sep, _credential_hash = account.partition(":")
    _increment_provider_counter(
        redis,
        provider,
        "credential_refresh_failures",
        _CREDENTIAL_REFRESH_FAILURES_TTL_SECONDS,
        logger,
    )


def _credential_override_is_usable(key: str, blob: str) -> bool:
    """Reject Grok OAuth blobs that no longer contain a refresh token."""
    if not key.startswith("grok:"):
        return True
    stripped = blob.strip()
    if not stripped.startswith("{"):
        return True
    try:
        parsed = json.loads(stripped)
    except (json.JSONDecodeError, ValueError):
        return False
    return _json_has_refresh_token(parsed)


def _json_has_refresh_token(value: object) -> bool:
    if isinstance(value, dict):
        for key, nested in value.items():
            if key == "refresh_token" and isinstance(nested, str) and nested.strip():
                return True
            if isinstance(nested, (dict, list)) and _json_has_refresh_token(nested):
                return True
    elif isinstance(value, list):
        return any(_json_has_refresh_token(item) for item in value)
    return False


_USAGE_EXHAUSTED_COOLDOWN_SECONDS = 1800
_USAGE_EXHAUSTED_PROCESSED_TTL_SECONDS = 24 * 3600
_TRANSIENT_FAILURE_PROCESSED_TTL_SECONDS = 24 * 3600
_REVIEW_RERUN_FAILURE_COOLDOWN_SECONDS = 15 * 60
_REVIEW_RERUN_FAILURE_TTL_SECONDS = 7 * 24 * 3600
_MAX_REVIEW_RERUN_FAILURES = 3
_FAILURE_CONCLUSIONS = frozenset(
    {"FAILURE", "CANCELLED", "TIMED_OUT", "ACTION_REQUIRED", "STALE", "STARTUP_FAILURE"}
)

# Patterns matching Go HTTP / network errors surfaced by the `gh` CLI.
# Used to distinguish transient network failures from permanent merge errors.
_MERGE_NETWORK_PATTERNS: list[re.Pattern[str]] = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"timed?\s*out",
        r"ETIMEDOUT",
        r"connection reset",
        r"ECONNRESET",
        r"ECONNREFUSED",
        r"dial tcp",
        r"TLS handshake",
        r"socket hang up",
        r"no such host",
        r"i/o timeout",
        r"network is unreachable",
        r"HTTP 50[234]",
    ]
]


def _make_usage_exhausted_processed_key(task_id: str) -> str:
    """Redis key tracking USAGE_EXHAUSTED accounting already done for a task."""
    return f"result:{task_id}:usage_exhausted_processed"


def _make_transient_failure_processed_key(task_id: str) -> str:
    """Redis key tracking transient-failure accounting already done for a task."""
    return f"result:{task_id}:transient_failure_processed"


def _usage_exhausted_cooldown_ttl_seconds(result: TaskResult) -> int:
    """Return persisted usage cooldown TTL, preferring result reset timestamp."""
    if result.rate_limit_resets_at:
        ttl = math.ceil(result.rate_limit_resets_at - time.time())
        if ttl > 0:
            return ttl
    return _USAGE_EXHAUSTED_COOLDOWN_SECONDS


def _failed_check_names(checks: list[dict]) -> set[str]:
    names: set[str] = set()
    for check in checks:
        conclusion = (check.get("conclusion") or "").upper()
        state = (check.get("state") or "").upper()
        failed = conclusion in _FAILURE_CONCLUSIONS or (
            not conclusion and state in ("FAILURE", "ERROR")
        )
        if failed:
            name = (
                check.get("detailsUrl")
                or check.get("details_url")
                or check.get("name")
                or check.get("context")
                or check.get("workflowName")
            )
            if name:
                names.add(str(name))
    return names


def _failed_check_fingerprints(checks: list[dict]) -> set[str]:
    fingerprints: set[str] = set()
    for check in checks:
        conclusion = (check.get("conclusion") or "").upper()
        state = (check.get("state") or "").upper()
        failed = conclusion in _FAILURE_CONCLUSIONS or (
            not conclusion and state in ("FAILURE", "ERROR")
        )
        if not failed:
            continue
        payload = {
            "name": str(check.get("name") or ""),
            "context": str(check.get("context") or ""),
            "workflow_name": str(check.get("workflowName") or ""),
            "details_url": str(check.get("detailsUrl") or check.get("details_url") or ""),
            "target_url": str(check.get("targetUrl") or check.get("target_url") or ""),
        }
        if any(payload.values()):
            fingerprints.add(json.dumps(payload, sort_keys=True, separators=(",", ":")))
    return fingerprints


def _snapshot_failed_checks_still_failing(checks: list[dict], snapshot: list[str]) -> bool:
    snapshot_set = set(snapshot)
    if not snapshot_set:
        return True
    current_fingerprints = _failed_check_fingerprints(checks)
    if current_fingerprints & snapshot_set:
        return True
    return bool(_failed_check_names(checks) & snapshot_set)


def _review_thread_ids(threads: list[dict]) -> set[str]:
    ids: set[str] = set()
    for thread in threads:
        thread_id = thread.get("id") or thread.get("node_id")
        if thread_id:
            ids.add(str(thread_id))
    return ids


def _review_thread_fingerprints(threads: list[dict]) -> set[str]:
    fingerprints: set[str] = set()
    for thread in threads:
        comments = []
        for comment in thread.get("comments") or []:
            comments.append(
                {
                    "id": str(comment.get("id") or comment.get("node_id") or ""),
                    "author": str(comment.get("author") or ""),
                    "body": str(comment.get("body") or ""),
                    "created_at": str(comment.get("createdAt") or comment.get("created_at") or ""),
                    "updated_at": str(comment.get("updatedAt") or comment.get("updated_at") or ""),
                }
            )
        payload = {
            "id": str(thread.get("id") or thread.get("node_id") or ""),
            "path": str(thread.get("path") or ""),
            "line": str(thread.get("line") or ""),
            "comments": comments,
        }
        fingerprints.add(json.dumps(payload, sort_keys=True, separators=(",", ":")))
    return fingerprints


def _is_pr_result_stale(project: ProjectConfig, result: TaskResult, logger: logging.Logger) -> bool:
    """Cheaply reject PR results that no longer match the current GitHub snapshot."""
    if result.resource_type != "pr":
        return False
    if not result.snapshot_head_sha:
        logger.info(
            "Dropping snapshot-less legacy PR result for PR #%d",
            result.resource_id,
        )
        return True

    try:
        pr_data = gh.get_pr(project.repo, result.resource_id, project.token)
    except Exception:
        logger.warning(
            "Failed to validate result snapshot for PR #%d; dropping result side-effects",
            result.resource_id,
            exc_info=True,
        )
        return True

    current_sha = str(pr_data.get("headRefOid") or "")
    if not current_sha:
        logger.info(
            "Dropping result for PR #%d: GitHub response did not include headRefOid",
            result.resource_id,
        )
        return True
    if current_sha and current_sha != result.snapshot_head_sha:
        logger.info(
            "Dropping stale result for PR #%d: result SHA %s, current SHA %s",
            result.resource_id,
            result.snapshot_head_sha,
            current_sha,
        )
        return True

    if result.decision_reason == "ci_failure" and result.snapshot_failed_checks:
        checks = pr_data.get("statusCheckRollup") or gh.get_ci_status(
            project.repo, result.resource_id, project.token
        )
        if not _snapshot_failed_checks_still_failing(checks, result.snapshot_failed_checks):
            logger.info(
                "Dropping stale CI result for PR #%d: captured failed checks are no longer failing",
                result.resource_id,
            )
            return True

    if result.decision_reason in ("changes_requested", "followup_threads") and (
        result.snapshot_review_thread_ids
    ):
        try:
            threads = gh.get_unresolved_review_threads(
                project.repo, result.resource_id, project.token
            )
        except Exception:
            logger.warning(
                "Failed to validate review-thread snapshot for PR #%d; "
                "dropping result side-effects",
                result.resource_id,
                exc_info=True,
            )
            return True
        if result.snapshot_review_thread_fingerprints:
            if not set(result.snapshot_review_thread_fingerprints).issubset(
                _review_thread_fingerprints(threads)
            ):
                logger.info(
                    "Dropping stale review result for PR #%d: captured thread content changed",
                    result.resource_id,
                )
                return True
        elif not set(result.snapshot_review_thread_ids).issubset(_review_thread_ids(threads)):
            logger.info(
                "Dropping stale review result for PR #%d: captured threads are resolved",
                result.resource_id,
            )
            return True

    if result.decision_reason == "merge_conflict_rebase":
        mergeable = str(pr_data.get("mergeable") or "").upper()
        merge_state = str(pr_data.get("mergeStateStatus") or "").upper()
        if mergeable != "CONFLICTING" and merge_state != "DIRTY":
            logger.info(
                "Dropping stale rebase result for PR #%d: conflict no longer applies",
                result.resource_id,
            )
            return True

    if result.decision_reason == "proactive_rebase":
        mergeable = str(pr_data.get("mergeable") or "").upper()
        if mergeable == "CONFLICTING":
            logger.info(
                "Dropping stale proactive rebase result for PR #%d: PR is now conflicting",
                result.resource_id,
            )
            return True

    return False


def _mark_usage_exhausted_token(
    result: TaskResult,
    token_pool: ProviderPool | None,
    logger: logging.Logger,
    account_key: str | None = None,
) -> None:
    """Generalized for ProviderPool: mark the entry for the task exhausted.

    Uses get_task_entry (lean ProviderEntry) when available for provider-aware
    fallback query (only claude uses the anthropic usage endpoint; others rely on
    rate_limit_resets_at from worker result). Falls back to legacy get_task_token
    for mocks / old shims. All logging uses only masked identity().
    """
    if token_pool is None:
        return
    entry = token_pool.get_task_entry(result.task_id)
    # Support legacy MagicMock tests and old registration paths that only
    # implement the token shim: if get_task_entry gave no real ProviderEntry,
    # fall back.
    if not isinstance(entry, ProviderEntry):
        cred = token_pool.get_task_token(result.task_id)
        if cred:
            entry = ProviderEntry(provider="claude", credential=cred, model=None)
        else:
            entry = None
    ident = entry.identity() if entry else account_key or "?"
    prov = entry.provider if entry else (account_key.partition(":")[0] if account_key else "?")
    logger.info(
        "USAGE_EXHAUSTED observed for provider %s (id=%s, task=%s) — will mark exhausted",
        prov,
        ident,
        result.task_id,
    )
    cooldown_until = None
    if result.rate_limit_resets_at:
        from datetime import datetime, timezone

        cooldown_until = datetime.fromtimestamp(result.rate_limit_resets_at, tz=timezone.utc)
        logger.info("Rate limit resets at %s (from stream-json)", cooldown_until.isoformat())
    elif entry and is_claude_provider(prov) and hasattr(entry, "credential") and entry.credential:
        try:
            cooldown_until = get_token_reset_time(entry.credential)
        except Exception as e:
            logger.warning("Failed to query token reset time: %s", e)
    marked = token_pool.mark_exhausted(result.task_id, cooldown_until=cooldown_until)
    if marked is not True and account_key:
        token_pool.mark_account_exhausted(account_key, cooldown_until=cooldown_until)


# Maximum number of merge retries for transient network errors before
# falling through to the backoff-and-retry path.
_MAX_MERGE_RETRIES = 5


def _is_network_error(msg: str) -> bool:
    """Return True if *msg* matches any known network error pattern."""
    return any(pat.search(msg) for pat in _MERGE_NETWORK_PATTERNS)


def _is_required_checks_expected_error(msg: str) -> bool:
    """Return True when branch protection says required checks need rerunning."""
    lower = msg.lower()
    return (
        "required status check" in lower
        and "expected" in lower
        and (
            "repository rule violations" in lower
            or "branch protection" in lower
            or "rule violations" in lower
        )
    )


def _is_merge_conflict_error(msg: str) -> bool:
    """Fallback classifier for gh errors when PR merge status cannot be refreshed."""
    lower = msg.lower()
    return (
        "merge conflict" in lower
        or "merge conflicts" in lower
        or "is not mergeable" in lower
        or "cannot be cleanly created" in lower
        or "cannot automatically merge" in lower
    )


def _merge_status_indicates_conflict(pr_data: dict) -> bool | None:
    """Return GitHub's conflict decision, or None when the status is inconclusive."""
    mergeable = pr_data.get("mergeable")
    merge_state_status = pr_data.get("mergeStateStatus")
    if mergeable == "CONFLICTING" or merge_state_status == "DIRTY":
        return True
    if mergeable == "MERGEABLE" and merge_state_status in {
        "BEHIND",
        "BLOCKED",
        "CLEAN",
        "DRAFT",
        "HAS_HOOKS",
        "UNSTABLE",
    }:
        return False
    return None


def _merge_status_summary(pr_data: dict) -> str:
    """Render the GitHub merge status fields for logs."""
    return (
        f"mergeable={pr_data.get('mergeable')!r} "
        f"mergeStateStatus={pr_data.get('mergeStateStatus')!r}"
    )


def _merge_failure_indicates_conflict(
    *, repo: str, pr_number: int, token: str, err_msg: str, logger: logging.Logger
) -> bool:
    """Prefer GitHub merge status; fall back to gh error text if status is unknown."""
    try:
        refreshed_pr = gh.get_pr(repo, pr_number, token)
        status_conflict = _merge_status_indicates_conflict(refreshed_pr)
        if status_conflict is not None:
            return status_conflict
        logger.warning(
            "PR #%d: GitHub merge status was inconclusive after merge failure "
            "(%s), falling back to gh error text",
            pr_number,
            _merge_status_summary(refreshed_pr),
        )
    except Exception as status_err:
        logger.warning(
            "PR #%d: failed to refresh GitHub merge status after merge failure, "
            "falling back to gh error text: %s",
            pr_number,
            status_err,
            exc_info=True,
        )
    return _is_merge_conflict_error(err_msg)


def _is_stale_head_error(msg: str) -> bool:
    """Return True when GitHub rejected an action because the PR head changed."""
    lower = msg.lower()
    return (
        ("match-head-commit" in lower)
        or ("expected_head_sha" in lower)
        or ("expected head sha" in lower)
        or ("head sha" in lower and ("mismatch" in lower or "modified" in lower))
        or ("head commit" in lower and ("changed" in lower or "mismatch" in lower))
        or ("head branch was modified" in lower)
    )


def _exception_message_with_stderr(exc: Exception) -> str:
    """Return exception text plus gh stderr when available."""
    stderr = getattr(exc, "stderr", "")
    if stderr:
        return f"{exc}\n{stderr}"
    return str(exc)


def _make_merge_retries_key(repo: str, pr_number: int) -> str:
    """Redis key for tracking merge retry count due to network errors."""
    return f"pr:{repo}:{pr_number}:merge_retries"


def _increment_merge_retries(redis: RedisClient, repo: str, pr_number: int) -> int:
    """Increment and return the merge retry count. TTL 1 hour."""
    key = _make_merge_retries_key(repo, pr_number)
    pipe = redis.pipeline(transaction=True)
    pipe.incr(key)
    pipe.expire(key, 3600)  # 1-hour TTL
    results = pipe.execute()
    return results[0]


def _make_review_rerun_failure_key(repo: str, pr_number: int, head_sha: str) -> str:
    """Redis key for failed claude-review rerun attempts on a PR SHA."""
    return f"pr:{repo}:{pr_number}:review_rerun_failures:{head_sha}"


def _make_review_rerun_failure_cooldown_key(repo: str, pr_number: int, head_sha: str) -> str:
    """Redis key suppressing claude-review reruns after a failed rerun call."""
    return f"pr:{repo}:{pr_number}:review_rerun_failure_cooldown:{head_sha}"


def _record_review_rerun_failure(
    redis: RedisClient, repo: str, pr_number: int, head_sha: str
) -> int:
    """Increment failed rerun attempts for this PR SHA and set a cooldown."""
    key = _make_review_rerun_failure_key(repo, pr_number, head_sha)
    count = redis.incr(key)
    redis.expire(key, _REVIEW_RERUN_FAILURE_TTL_SECONDS)
    redis.set_ex(
        _make_review_rerun_failure_cooldown_key(repo, pr_number, head_sha),
        "1",
        _REVIEW_RERUN_FAILURE_COOLDOWN_SECONDS,
    )
    return count


def _clear_review_rerun_failures(
    redis: RedisClient, repo: str, pr_number: int, head_sha: str
) -> None:
    """Clear loop-side failed rerun state after a successful rerun."""
    redis.delete(
        _make_review_rerun_failure_key(repo, pr_number, head_sha),
        _make_review_rerun_failure_cooldown_key(repo, pr_number, head_sha),
    )


def _review_rerun_failure_cooldown_active(
    redis: RedisClient, repo: str, pr_number: int, head_sha: str
) -> bool:
    """Return True when a failed claude-review rerun is still cooling down."""
    return redis.exists(_make_review_rerun_failure_cooldown_key(repo, pr_number, head_sha))


def _back_off_pr_retries(
    *,
    repo: str,
    pr_state: PRState,
    logger: logging.Logger,
    redis: RedisClient,
    reason: str,
    step: int,
) -> None:
    """Lengthen a PR's retry cadence instead of escalating it to a human.

    Orcest never abandons a PR or applies the needs-human label on budget
    exhaustion -- a "hard" PR is the worker's job, not a human's. The per-SHA
    attempt counter is cleared and a backoff cooldown is set so the PR is
    retried automatically once the cooldown expires. ``step`` selects the
    cooldown length (see coordination.get_backoff_cooldown_seconds); a larger
    step is used the deeper into the budget we are, so a repeatedly failing PR
    is retried less often -- never zero.
    """
    try:
        set_backoff_cooldown(redis, repo, pr_state.number, step, head_sha=pr_state.head_sha)
        clear_attempts(redis, repo, pr_state.number)
    except Exception as e:
        logger.error(
            "PR #%d: failed to set retry backoff: %s",
            pr_state.number,
            e,
            exc_info=True,
        )
    logger.info(
        "PR #%d: %s -- backing off retries (step %d), not escalating to a human",
        pr_state.number,
        reason,
        step,
    )


def _load_fleet_repo_to_project_map(logger: logging.Logger) -> dict[str, str]:
    """Best-effort load of repo→project_name from a mounted fleet config.

    The orchestrator container has only its own per-project view; when fleet
    config is also bind-mounted (see docker-compose.yml), this gives the
    archiver visibility into ALL projects so per-project subdirectories are
    chosen correctly instead of falling back to ``unknown/``. Silently returns
    an empty mapping if the file is absent or unparseable — orcest still works
    in single-project / unmounted setups.
    """
    candidates = [
        "/home/orcest/fleet-config.yaml",
        "/home/orcest/app/config/fleet.yaml",
        "/etc/orcest/config.yaml",
    ]
    for path in candidates:
        try:
            from orcest.fleet.config import load_config

            fleet_cfg = load_config(path)
        except Exception:
            continue
        if not fleet_cfg.projects:
            continue
        mapping = {p.repo: p.name for p in fleet_cfg.projects if p.repo and p.name}
        if mapping:
            logger.info(
                "Loaded fleet repo→project map from %s: %d project(s)",
                path,
                len(mapping),
            )
            return mapping
    return {}


def run_orchestrator(config: OrchestratorConfig) -> None:
    """Main orchestrator entry point. Polls GitHub in a loop."""
    logger = setup_logging("orchestrator", "main")
    redis = RedisClient(config.redis)

    # Shared task Redis client — all projects publish tasks to this prefix
    # so workers only need to read from one stream.
    task_redis = RedisClient.from_client(redis.client, key_prefix=config.task_key_prefix)

    # Verify Redis connection
    if not redis.health_check():
        logger.error("Cannot connect to Redis. Exiting.")
        sys.exit(1)

    # Build per-project Redis clients once; reuse across all poll cycles
    project_clients = _build_project_clients(config, redis)

    # Ensure consumer groups for shared task streams (so workers don't race)
    for stream in (
        _configured_task_streams(config)
        + _configured_task_streams(config, issue=True)
    ):
        task_redis.ensure_consumer_group(stream, CONSUMER_GROUP)

    # Ensure consumer group for results stream (per-project)
    for _, project_redis in project_clients:
        project_redis.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    # Create per-project provider pools (generalized from legacy TokenPool).
    # Prefer the rich-but-lean ProjectConfig.providers (populated by config load with
    # legacy claude synth + any new providers from YAML, deduped). Fall back to
    # claude_tokens synthesis only for direct test ProjectConfig() constructions that
    # omit .providers. Only lean surface (provider/credential/model) is used.
    token_pools: dict[str, ProviderPool] = {}
    for project in config.projects:
        entries = list(project.providers) if project.providers else []
        if not entries and project.claude_tokens:
            entries = [
                ProviderEntry(
                    provider="claude",
                    credential=t,
                    model=None,
                    source="legacy_claude_tokens",
                )
                for t in project.claude_tokens
            ]
        if entries:
            # Defensive dedup by identity (config load already does this, but tests may not)
            seen: dict[str, ProviderEntry] = {}
            unique = []
            for e in entries:
                ident = e.identity()
                if ident not in seen:
                    seen[ident] = e
                    unique.append(e)
            try:
                token_pools[project.key_prefix] = ProviderPool(unique)
            except ValueError as exc:
                logger.error(
                    "Project %s: provider pool disabled: %s",
                    project.repo,
                    exc,
                )
                continue
            logger.info(
                "Project %s: provider pool with %d entries (providers=%s)",
                project.repo,
                len(unique),
                [e.provider for e in unique],
            )

    # Restore persisted credential write-back overrides (rotated OAuth blobs)
    # so a refreshed token survives an orchestrator restart instead of
    # reverting to the stale config blob.
    for project, project_redis in project_clients:
        pool = token_pools.get(project.key_prefix)
        if pool is not None:
            _load_credential_overrides(project_redis, pool, logger, shared_redis=task_redis)

    # Graceful shutdown
    shutdown = False

    def handle_signal(signum: int, frame: object) -> None:
        nonlocal shutdown
        logger.info("Received signal %d, shutting down gracefully...", signum)
        shutdown = True

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    pending_task_ttl = compute_pending_task_ttl(config.runner)

    # Trace archiver: tails output:* streams to per-task files on
    # config.trace_archive_path. Start silently disables when path is unset,
    # so it's safe to construct unconditionally.
    repo_to_project = {p.repo: p.key_prefix for p in config.projects if p.repo and p.key_prefix}
    # Best-effort enrichment from fleet config (mounted into the container by
    # docker-compose.yml when the trace archive is enabled). Each per-project
    # orchestrator only sees its own ProjectConfig; the fleet config carries
    # ALL projects so traces from other projects get the right key_prefix
    # subdirectory instead of falling to the ``unknown/`` bucket.
    repo_to_project.update(_load_fleet_repo_to_project_map(logger))
    trace_archiver = TraceArchiver(
        redis=redis,
        archive_path=config.trace_archive_path,
        repo_to_project=repo_to_project,
        logger=logger,
    )
    trace_archiver.start()

    repos = ", ".join(p.repo for p in config.projects) if config.projects else "(none)"
    logger.info(
        "Orchestrator started. Projects: %s, poll interval: %ds",
        repos,
        config.polling.interval,
    )

    while not shutdown:
        try:
            _poll_cycle(
                config,
                redis,
                task_redis,
                token_pools,
                logger,
                pending_task_ttl,
                project_clients,
            )
        except Exception as e:
            logger.error("Poll cycle failed: %s", e, exc_info=True)
            # Continue after error -- don't crash the loop

        # Wait for next cycle (interruptible in 1-second chunks)
        for _ in range(config.polling.interval):
            if shutdown:
                break
            time.sleep(1)

    trace_archiver.shutdown()
    logger.info("Orchestrator shut down cleanly.")


def _build_project_clients(
    config: OrchestratorConfig,
    redis: RedisClient,
) -> list[tuple[ProjectConfig, RedisClient]]:
    """Build per-project Redis clients from config."""
    return [
        (project, RedisClient.from_client(redis.client, key_prefix=project.key_prefix))
        for project in config.projects
    ]


def _poll_cycle(
    config: OrchestratorConfig,
    redis: RedisClient,
    task_redis: RedisClient,
    token_pools: dict[str, ProviderPool],
    logger: logging.Logger,
    pending_task_ttl: int,
    project_clients: list[tuple[ProjectConfig, RedisClient]] | None = None,
) -> None:
    """Single orchestrator poll cycle across all configured projects."""
    if project_clients is None:
        project_clients = _build_project_clients(config, redis)

    # Step 1: Consume results per project
    for project, project_redis in project_clients:
        try:
            pool = token_pools.get(project.key_prefix)
            _consume_results_for_project(
                project,
                project_redis,
                config.labels,
                logger,
                token_pool=pool,
                max_transient_failures=config.max_transient_failures,
                shared_credential_redis=task_redis,
            )
        except Exception:
            logger.error("Failed to consume results for %s", project.repo, exc_info=True)

    # M1-conc: reclaim delivered+ACKed task entries (which carry plaintext
    # GitHub PAT + provider credential) once per poll cycle. xtrim_acked_entries
    # trims only up to the LOWEST still-pending id (or last-delivered-id when the
    # PEL is empty), so un-ACKed / undelivered work is never dropped -- it does
    # NOT trim by raw last-delivered-id while entries are still in flight.
    for _task_stream in (
        _configured_task_streams(config, token_pools=token_pools)
        + _configured_task_streams(config, issue=True, token_pools=token_pools)
    ):
        try:
            task_redis.xtrim_acked_entries(_task_stream, CONSUMER_GROUP)
        except Exception:
            logger.debug("Failed to trim ACKed entries from %s", _task_stream, exc_info=True)

    # Step 2: Poll each project.
    #
    # Coordinate the bypass through shared task Redis. Production runs one
    # orchestrator container per project, so an in-process list/counter cannot
    # provide global fairness. A durable leased round-robin turn makes every
    # stable live project eligible eventually, independent of process phase.
    priority_identity = _issue_discovery_priority(
        task_redis,
        [project for project, _project_redis in project_clients],
        config.polling.interval,
        logger,
    )

    total_enqueued = 0
    total_merged = 0
    total_prs = 0
    total_issues = 0
    for project, project_redis in project_clients:
        try:
            pool = token_pools.get(project.key_prefix)
            enqueued, merged, prs_checked, issues_checked = _poll_project(
                project,
                project_redis,
                task_redis,
                config,
                logger,
                pending_task_ttl,
                token_pool=pool,
                force_issue_discovery=(project.key_prefix or project.repo) == priority_identity,
            )
            total_enqueued += enqueued
            total_merged += merged
            total_prs += prs_checked
            total_issues += issues_checked
        except Exception:
            logger.error("Failed to poll %s", project.repo, exc_info=True)

    logger.info(
        "Poll cycle complete. %d tasks enqueued, %d merged, %d PRs checked, %d issues checked.",
        total_enqueued,
        total_merged,
        total_prs,
        total_issues,
    )


def _poll_project(
    project: ProjectConfig,
    project_redis: RedisClient,
    task_redis: RedisClient,
    config: OrchestratorConfig,
    logger: logging.Logger,
    pending_task_ttl: int,
    token_pool: ProviderPool | None = None,
    force_issue_discovery: bool = False,
) -> tuple[int, int, int, int]:
    """Poll a single project for actionable PRs and issues.

    Args:
        project_redis: Per-project Redis client (for pending markers, attempt counters, etc.).
        task_redis: Shared Redis client (for publishing tasks to the common stream).
        token_pool: Optional ProviderPool (parameter retains the name `token_pool`
            during the claude_tokens migration to keep the diff minimal; only the
            lean provider/credential/model/identity() surface is ever used via
            the internal _select_provider_entry / _register_task).

    Returns (enqueued, merged, prs_checked, issues_checked).
    """
    logger = logger.getChild(project.repo)
    repo = project.repo
    token = project.token
    key_prefix = project.key_prefix

    def _select_provider_entry() -> ProviderEntry | None:
        """Pick the next ProviderEntry from the pool using the lean surface only
        (provider/credential/model/identity()).

        Returns None if all entries are exhausted/cooling (caller should skip enqueue
        and the skip counter is incremented for observability).

        Falls back to synthesizing a lean claude entry from project.claude_token when
        no pool was configured for the project (single-token legacy or test paths).
        Never reads or forwards cli_binary/env_var/extras.

        On exhaustion, increments the (transition) per-project Redis skip counter.
        """
        if token_pool is None:
            # No pool was built for this project. That happens either because a
            # single legacy claude_token is configured (return it) OR because the
            # project has NO credentials at all -- in which case publishing a
            # task with credential="" makes every worker fail this project each
            # cycle, burning attempts and GitHub comments with no config-time
            # error. Fail loudly and skip instead.
            cred = project.claude_token or ""
            # Genuinely-no-credential case: neither a providers: list nor any
            # claude_tokens entry is configured. (run() builds no pool for this,
            # so we land here in production -- not just in unit tests.) An empty
            # *list* is the signal; a [""] degenerate token still flows through
            # the legacy single-token path below so existing fixtures keep
            # working.
            if project.providers:
                logger.error(
                    "Project %s has providers configured but no usable provider pool; "
                    "skipping task publish until provider credentials are fixed.",
                    project.repo,
                )
                return None
            if not project.claude_tokens and not project.providers:
                logger.error(
                    "Project %s has no providers and no claude_tokens configured; "
                    "skipping task publish (a credential-less task would fail on "
                    "every worker). Add a providers: entry or claude_tokens to this "
                    "project's orchestrator config.",
                    project.repo,
                )
                return None
            return ProviderEntry(
                provider="claude",
                credential=cred,
                model=None,
                source="legacy_claude_tokens",
            )

        entry = token_pool.next_entry()
        if entry is None:
            # Increment aggregate (compat) + per-provider exhausted_skip counters
            # (Task 8). Uses only provider name from lean surface.
            try:
                count = project_redis.incr(_PROVIDER_EXHAUSTED_SKIP_KEY)
                if count == 1:
                    project_redis.expire(
                        _PROVIDER_EXHAUSTED_SKIP_KEY, _PROVIDER_EXHAUSTED_SKIP_TTL_SECONDS
                    )
            except Exception:
                # Observability is best-effort; never break the poll cycle.
                logger.debug("Failed to increment provider-exhaustion counter", exc_info=True)
            for prov in token_pool.provider_names:
                pkey = f"providers:{prov}:exhausted_skip"
                try:
                    pcount = project_redis.incr(pkey)
                    if pcount == 1:
                        project_redis.expire(pkey, _PROVIDER_EXHAUSTED_SKIP_TTL_SECONDS)
                except Exception:
                    logger.debug(
                        "Failed to increment per-provider exhausted-skip counter for %s",
                        prov,
                        exc_info=True,
                    )
            # Debug log includes masked identities only (from pool __repr__)
            logger.debug("Provider pool exhausted during selection: %r", token_pool)
        return entry

    def _register_task(task_id: str, entry: ProviderEntry | str) -> None:
        """Record which provider entry (lean surface) was used for a task id
        (for later exhaustion tracking via mark_exhausted / task_completed).
        Accepts entry object (preferred) or str for legacy shim paths.
        """
        if token_pool is not None:
            token_pool.register_task(task_id, entry)
            if isinstance(entry, ProviderEntry):
                _persist_task_provider_account(
                    project_redis, task_id, entry, pending_task_ttl, logger
                )

    def _try_publish(
        entry: ProviderEntry,
        publish_fn: Callable[..., Task | None],
        **publish_kwargs: Any,
    ) -> Task | None:
        """Encapsulates register + publish + rollback for the hardened contract.

        Injects lean surface (provider/credential/model/task_id) + claude_token shim,
        key_prefix, task_redis. Calls task_completed on None-return or exception.
        Re-raises so callers can do site-specific logging. Returns publish result.
        """
        task_id = str(uuid.uuid4())
        _register_task(task_id, entry)
        # Use the latest written-back credential blob if the CLI rotated it
        # (OAuth-blob providers). Refresh this account from shared Redis at
        # handoff time because each project orchestrator has its own local pool.
        if token_pool is not None:
            _refresh_shared_credential_override(
                task_redis,
                token_pool,
                entry.account_key(),
                logger,
            )
        cred = (
            token_pool.effective_credential(entry) if token_pool is not None else entry.credential
        )
        published_provider = _published_provider_for_entry(entry, config.default_runner)
        try:
            res = publish_fn(
                **publish_kwargs,
                claude_token=cred if is_claude_provider(published_provider) else "",
                key_prefix=key_prefix,
                task_redis=task_redis,
                provider=published_provider,
                credential=cred,
                model=entry.model,
                task_id=task_id,
            )
            if res is None and token_pool is not None:
                token_pool.task_completed(task_id)
                _clear_task_provider_account(project_redis, task_id, logger)
            return res
        except Exception:
            if token_pool is not None:
                token_pool.task_completed(task_id)
                _clear_task_provider_account(project_redis, task_id, logger)
            raise

    labels = config.labels

    # Discover PRs needing action
    pr_states = discover_actionable_prs(
        repo=repo,
        token=token,
        redis=project_redis,
        label_config=labels,
        max_attempts=config.max_attempts,
        max_total_attempts=config.max_total_attempts,
        stale_pending_timeout_seconds=config.stale_pending_timeout_seconds,
    )

    # Sort: merges first (quick wins), then fixes/followups oldest-first
    # (lowest PR number = longest waiting). Skips don't matter but sort
    # them last so actionable items are processed first.
    _ACTION_PRIORITY = {
        PRAction.MERGE: 0,
        PRAction.UPDATE_BRANCH: 0,
        PRAction.ENQUEUE_FIX: 1,
        PRAction.ENQUEUE_FOLLOWUP: 1,
        PRAction.ENQUEUE_REBASE: 1,
    }
    pr_states.sort(key=lambda ps: (_ACTION_PRIORITY.get(ps.action, 9), ps.number))

    # Pre-compute unclaimed issue tasks for gating issue discovery. Tasks that
    # are claimed and in flight don't block — they're consuming worker capacity,
    # not buffer space. Only undelivered (lag) entries justify deferral.
    issue_task_streams = _configured_task_streams(config, issue=True)
    if token_pool is not None:
        seen_issue_streams = set(issue_task_streams)
        for provider in token_pool.provider_names:
            stream = task_stream_name(provider, issue=True)
            if stream not in seen_issue_streams:
                issue_task_streams.append(stream)
                seen_issue_streams.add(stream)
    unclaimed_issue_tasks = _unclaimed_task_count(
        task_redis,
        issue_task_streams,
        CONSUMER_GROUP,
        logger,
    )

    enqueued = 0
    merged = 0
    for pr_state in pr_states:
        if pr_state.action == PRAction.MERGE:
            logger.info("PR #%d (%s): merging", pr_state.number, pr_state.title)
            try:
                gh.merge_pr(
                    repo,
                    pr_state.number,
                    token,
                    delete_branch=config.delete_branch_on_merge,
                    head_sha=pr_state.head_sha,
                )
                merged += 1
            except Exception as e:
                err_msg = _exception_message_with_stderr(e)
                logger.error(
                    f"Failed to merge PR #{pr_state.number}: {err_msg}",
                    exc_info=True,
                )
                # If the error looks like a merge conflict, enqueue a
                # rebase task so a worker can resolve it automatically.
                is_conflict = _merge_failure_indicates_conflict(
                    repo=repo,
                    pr_number=pr_state.number,
                    token=token,
                    err_msg=err_msg,
                    logger=logger,
                )
                if is_conflict:
                    logger.info(
                        f"PR #{pr_state.number}: merge conflict detected, enqueueing rebase task"
                    )
                    entry = _select_provider_entry()
                    if entry is None:
                        logger.warning(
                            "All providers exhausted, skipping rebase task for PR #%d",
                            pr_state.number,
                        )
                        continue
                    try:
                        task = _try_publish(
                            entry,
                            publish_rebase_task,
                            pr_state=pr_state,
                            repo=repo,
                            token=token,
                            redis=project_redis,
                            default_runner=config.default_runner,
                            merge_error=err_msg[:200],
                            pending_task_ttl=pending_task_ttl,
                            logger=logger,
                        )
                        if task is not None:
                            # registered before publish per contract; id will match
                            enqueued += 1
                    except Exception as rebase_err:
                        logger.error(
                            f"Failed to enqueue rebase task for PR #{pr_state.number}: "
                            f"{rebase_err}",
                            exc_info=True,
                        )
                        # Fall through to the backoff-and-retry path
                        is_conflict = False

                if not is_conflict:
                    if _is_required_checks_expected_error(err_msg):
                        logger.info(
                            "PR #%d: merge blocked because required status checks are "
                            "expected; updating branch so checks can rerun",
                            pr_state.number,
                        )
                        try:
                            if gh.update_branch(
                                repo,
                                pr_state.number,
                                token,
                                expected_head_sha=pr_state.head_sha,
                            ):
                                continue
                        except Exception as update_err:
                            if _is_stale_head_error(_exception_message_with_stderr(update_err)):
                                logger.info(
                                    "PR #%d: update-branch rejected stale discovered "
                                    "head SHA %s after merge rejection; retrying next poll",
                                    pr_state.number,
                                    pr_state.head_sha,
                                )
                                continue
                            logger.warning(
                                "PR #%d: update-branch after merge rejection failed: %s",
                                pr_state.number,
                                update_err,
                                exc_info=True,
                            )

                    if _is_stale_head_error(err_msg):
                        logger.info(
                            "PR #%d: merge rejected stale discovered head SHA %s; "
                            "retrying next poll",
                            pr_state.number,
                            pr_state.head_sha,
                        )
                        continue

                    # Check for transient network errors — retry silently
                    # unless we've exceeded the merge retry budget.
                    # GhRateLimitError is excluded: _run_gh() already
                    # exhausted its own retry budget for rate limits.
                    if not isinstance(e, GhRateLimitError) and _is_network_error(err_msg):
                        retry_count = _increment_merge_retries(project_redis, repo, pr_state.number)
                        if retry_count <= _MAX_MERGE_RETRIES:
                            logger.warning(
                                "PR #%d: transient network error during merge "
                                "(attempt %d/%d), will retry on next poll cycle: %s",
                                pr_state.number,
                                retry_count,
                                _MAX_MERGE_RETRIES,
                                err_msg[:200],
                            )
                            continue  # skip needs-human label

                    # A non-conflict, non-stale, non-network merge failure
                    # (branch protection, a missing required check, etc.) is
                    # not a human-decision blocker -- it usually clears on its
                    # own once CI/branch state settles. Back off and let the
                    # next poll re-evaluate; never escalate to needs-human.
                    logger.warning(
                        "PR #%d: merge failed (%s) -- backing off, will retry",
                        pr_state.number,
                        err_msg[:200],
                    )
                    _back_off_pr_retries(
                        repo=repo,
                        pr_state=pr_state,
                        logger=logger,
                        redis=project_redis,
                        reason="merge failed",
                        step=1,
                    )
            else:
                # Clean up state on successful merge
                try:
                    clear_review_retrigger(project_redis, repo, pr_state.number)
                except Exception:
                    logger.debug(
                        "cleanup failed: clear_review_retrigger for PR #%d",
                        pr_state.number,
                        exc_info=True,
                    )  # Best-effort cleanup; key has TTL anyway
                try:
                    clear_total_attempts(project_redis, repo, pr_state.number)
                except Exception:
                    logger.debug(
                        "cleanup failed: clear_total_attempts for PR #%d",
                        pr_state.number,
                        exc_info=True,
                    )  # Best-effort cleanup; key has TTL anyway
                try:
                    clear_backoff(project_redis, repo, pr_state.number)
                except Exception:
                    logger.debug(
                        "cleanup failed: clear_backoff for PR #%d",
                        pr_state.number,
                        exc_info=True,
                    )  # Best-effort cleanup; key has TTL anyway
                try:
                    gh.post_comment(
                        repo,
                        pr_state.number,
                        "**orcest** merged this PR.",
                        token,
                    )
                except Exception as comment_err:
                    logger.warning(
                        "Merged PR #%d but failed to post comment: %s",
                        pr_state.number,
                        comment_err,
                        exc_info=True,
                    )
                # Run deployment if configured (run_deployment is a no-op when disabled)
                try:
                    if run_deployment(config.deployment, pr_state.number, logger):
                        logger.info("PR #%d: deployment succeeded", pr_state.number)
                except DeploymentError as deploy_err:
                    err_msg = str(deploy_err)
                    logger.error("PR #%d: deployment failed: %s", pr_state.number, err_msg)
                    try:
                        issue_number = gh.create_issue(
                            repo,
                            f"Deployment failed after merge of PR #{pr_state.number}",
                            f"**orcest** deployment failed after merging "
                            f"PR #{pr_state.number} ({pr_state.title}).\n\n"
                            f"Error: {err_msg[:500]}",
                            token,
                            labels=["orcest:needs-human"],
                        )
                        logger.info(
                            "PR #%d: created deployment failure issue #%d",
                            pr_state.number,
                            issue_number,
                        )
                    except Exception as issue_err:
                        logger.error(
                            "PR #%d: failed to create deployment failure issue: %s",
                            pr_state.number,
                            issue_err,
                            exc_info=True,
                        )
                # After successful merge, rebase other open PRs onto updated master.
                # If multiple PRs are merged in the same poll cycle, this loop runs
                # once per merged PR; publish_rebase_task calls set_pending_task (SET
                # NX EX), which silently deduplicates redundant enqueue attempts.
                logger.info(
                    "PR #%d merged; checking for SKIP_GREEN PRs to proactively rebase",
                    pr_state.number,
                )
                for other_pr in pr_states:
                    if other_pr.number == pr_state.number:
                        continue  # skip the one we just merged
                    if other_pr.action != PRAction.SKIP_GREEN:
                        continue  # only proactively rebase green PRs
                    entry = _select_provider_entry()
                    if entry is None:
                        logger.warning(
                            "All providers exhausted, skipping proactive rebase for PR #%d",
                            other_pr.number,
                        )
                        continue
                    try:
                        _try_publish(
                            entry,
                            publish_rebase_task,
                            pr_state=other_pr,
                            repo=repo,
                            token=token,
                            redis=project_redis,
                            default_runner=config.default_runner,
                            merge_error="",
                            pending_task_ttl=pending_task_ttl,
                            logger=logger,
                            proactive=True,
                        )
                    except Exception:
                        logger.warning(
                            "Failed to enqueue rebase for PR #%d",
                            other_pr.number,
                            exc_info=True,
                        )
        elif pr_state.action == PRAction.ENQUEUE_FIX:
            logger.info("PR #%d (%s): enqueueing fix task", pr_state.number, pr_state.title)
            try:
                if rerun_all_transient_ci(
                    pr_state=pr_state,
                    repo=repo,
                    token=token,
                    redis=project_redis,
                    logger=logger,
                ):
                    continue
            except Exception as e:
                logger.warning(
                    "Failed to classify/re-trigger transient CI for PR #%d before "
                    "token selection: %s",
                    pr_state.number,
                    e,
                    exc_info=True,
                )
            entry = _select_provider_entry()
            if entry is None:
                logger.warning(
                    "All providers exhausted, skipping fix task for PR #%d",
                    pr_state.number,
                )
            else:
                try:
                    result = _try_publish(
                        entry,
                        publish_fix_task,
                        pr_state=pr_state,
                        repo=repo,
                        token=token,
                        redis=project_redis,
                        default_runner=config.default_runner,
                        pending_task_ttl=pending_task_ttl,
                        logger=logger,
                        skip_transient_rerun=True,
                    )
                    if result is not None:
                        # pre-registered before publish per hardened contract
                        enqueued += 1
                except Exception as e:
                    logger.error(
                        "Failed to publish fix task for PR #%d: %s",
                        pr_state.number,
                        e,
                        exc_info=True,
                    )
        elif pr_state.action == PRAction.ENQUEUE_FOLLOWUP:
            logger.info("PR #%d (%s): enqueueing followup triage", pr_state.number, pr_state.title)
            entry = _select_provider_entry()
            if entry is None:
                logger.warning(
                    "All providers exhausted, skipping followup task for PR #%d",
                    pr_state.number,
                )
            else:
                try:
                    task = _try_publish(
                        entry,
                        publish_followup_task,
                        pr_state=pr_state,
                        repo=repo,
                        token=token,
                        redis=project_redis,
                        default_runner=config.default_runner,
                        pending_task_ttl=pending_task_ttl,
                        logger=logger,
                    )
                    if task is not None:
                        # pre-registered
                        enqueued += 1
                except Exception as e:
                    logger.error(
                        "Failed to publish followup task for PR #%d: %s",
                        pr_state.number,
                        e,
                        exc_info=True,
                    )
        elif pr_state.action == PRAction.ENQUEUE_REBASE:
            logger.info(
                "PR #%d (%s): merge conflicts detected, enqueueing rebase task",
                pr_state.number,
                pr_state.title,
            )
            entry = _select_provider_entry()
            if entry is None:
                logger.warning(
                    "All providers exhausted, skipping rebase task for PR #%d",
                    pr_state.number,
                )
            else:
                try:
                    task = _try_publish(
                        entry,
                        publish_rebase_task,
                        pr_state=pr_state,
                        repo=repo,
                        token=token,
                        redis=project_redis,
                        default_runner=config.default_runner,
                        pending_task_ttl=pending_task_ttl,
                        logger=logger,
                    )
                    if task is not None:
                        # pre-registered per contract
                        enqueued += 1
                except Exception as e:
                    logger.error(
                        "Failed to publish rebase task for PR #%d: %s",
                        pr_state.number,
                        e,
                        exc_info=True,
                    )
        elif pr_state.action == PRAction.UPDATE_BRANCH:
            logger.info(
                "PR #%d (%s): out-of-date with base, calling update-branch",
                pr_state.number,
                pr_state.title,
            )
            try:
                gh.update_branch(
                    repo,
                    pr_state.number,
                    token,
                    expected_head_sha=pr_state.head_sha,
                )
            except Exception as e:
                if _is_stale_head_error(_exception_message_with_stderr(e)):
                    logger.info(
                        "PR #%d: update-branch rejected stale discovered head SHA %s; "
                        "retrying next poll",
                        pr_state.number,
                        pr_state.head_sha,
                    )
                    continue
                logger.warning(
                    "PR #%d: update-branch failed: %s",
                    pr_state.number,
                    e,
                    exc_info=True,
                )
        elif pr_state.action == PRAction.SKIP_GREEN:
            logger.debug("PR #%d: CI green, skipping", pr_state.number)
        elif pr_state.action == PRAction.RETRIGGER_REVIEW:
            if pr_state.review_run_id is None:
                logger.error(
                    "PR #%d: RETRIGGER_REVIEW action but review_run_id is None, skipping",
                    pr_state.number,
                )
            elif _review_rerun_failure_cooldown_active(
                project_redis, repo, pr_state.number, pr_state.head_sha
            ):
                logger.info(
                    "PR #%d: claude-review rerun failure cooldown active, skipping",
                    pr_state.number,
                )
            else:
                run_id = pr_state.review_run_id
                logger.info(
                    "PR #%d: claude-review passed but no formal review, re-triggering run %d",
                    pr_state.number,
                    run_id,
                )
                try:
                    gh.rerun_workflow(
                        repo,
                        run_id,
                        token,
                    )
                    set_review_retrigger_sha(
                        project_redis, repo, pr_state.number, pr_state.head_sha
                    )
                    _clear_review_rerun_failures(
                        project_redis, repo, pr_state.number, pr_state.head_sha
                    )
                except Exception as e:
                    failure_count = _record_review_rerun_failure(
                        project_redis, repo, pr_state.number, pr_state.head_sha
                    )
                    logger.error(
                        "Failed to re-trigger review for PR #%d (failure %d/%d; cooldown %ds): %s",
                        pr_state.number,
                        failure_count,
                        _MAX_REVIEW_RERUN_FAILURES,
                        _REVIEW_RERUN_FAILURE_COOLDOWN_SECONDS,
                        e,
                        exc_info=True,
                    )
                    if failure_count >= _MAX_REVIEW_RERUN_FAILURES:
                        # Repeated failures to re-trigger claude-review are not
                        # a human-decision blocker. Back off and keep retrying;
                        # never escalate to needs-human.
                        logger.warning(
                            "PR #%d: claude-review re-trigger failed %d times -- "
                            "backing off, will retry",
                            pr_state.number,
                            failure_count,
                        )
                        _back_off_pr_retries(
                            repo=repo,
                            pr_state=pr_state,
                            logger=logger,
                            redis=project_redis,
                            reason="claude-review re-trigger repeatedly failed",
                            step=2,
                        )
        elif pr_state.action == PRAction.RETRIGGER_STALE_CHECKS:
            run_ids = pr_state.stale_run_ids
            # Cooldown guard: skip if we already acted on this SHA
            stale_sha = get_stale_retrigger_sha(project_redis, repo, pr_state.number)
            if stale_sha == pr_state.head_sha:
                logger.debug(
                    "PR #%d: stale checks already handled for SHA %s, skipping",
                    pr_state.number,
                    pr_state.head_sha,
                )
            elif not run_ids:
                # Stale pending checks found but no re-triggerable run IDs
                # (e.g. StatusContext checks). Nothing orcest can act on and no
                # human decision is required -- the checks may still settle.
                # Log and move on; never escalate to needs-human.
                logger.warning(
                    "PR #%d: stale pending checks with no re-triggerable run IDs; "
                    "leaving for the check provider to settle",
                    pr_state.number,
                )
                set_stale_retrigger_sha(
                    project_redis,
                    repo,
                    pr_state.number,
                    pr_state.head_sha,
                    ex=config.stale_pending_timeout_seconds,
                )
            else:
                logger.warning(
                    "PR #%d: stale pending check(s) (>%ds); re-triggering %d run(s) %s",
                    pr_state.number,
                    config.stale_pending_timeout_seconds,
                    len(run_ids),
                    run_ids,
                )
                any_cancel_succeeded = False
                cancelled_count = 0
                cancelled_run_ids: list[int] = []
                for run_id in run_ids:
                    try:
                        gh.cancel_workflow(
                            repo,
                            run_id,
                            token,
                        )
                        any_cancel_succeeded = True
                        cancelled_count += 1
                        cancelled_run_ids.append(run_id)
                        logger.info(
                            "PR #%d: cancelled stale workflow run %d",
                            pr_state.number,
                            run_id,
                        )
                    except Exception as e:
                        logger.warning(
                            "Failed to cancel stale run %d for PR #%d: %s",
                            run_id,
                            pr_state.number,
                            e,
                            exc_info=True,
                        )
                    # Best-effort immediate rerun; gh run rerun requires the
                    # run to be in a completed state, so this will usually fail
                    # while the cancel is still propagating.  If it does fail,
                    # the cancelled run will appear as a CI failure on the next
                    # poll cycle and be handled by the normal fix flow.
                    try:
                        gh.rerun_workflow(
                            repo,
                            run_id,
                            token,
                        )
                        logger.info(
                            "PR #%d: re-triggered stale workflow run %d",
                            pr_state.number,
                            run_id,
                        )
                    except Exception as e:
                        logger.debug(
                            "Could not immediately re-trigger run %d for PR #%d "
                            "(cancel may still be propagating): %s",
                            run_id,
                            pr_state.number,
                            e,
                        )
                # Always set cooldown after attempting — prevents a busy retry
                # loop if the run can't be cancelled or immediately rerun.
                set_stale_retrigger_sha(
                    project_redis,
                    repo,
                    pr_state.number,
                    pr_state.head_sha,
                    ex=config.stale_pending_timeout_seconds,
                )
                if any_cancel_succeeded:
                    record_self_cancelled_stale_runs(
                        project_redis,
                        repo,
                        pr_state.number,
                        pr_state.head_sha,
                        cancelled_run_ids,
                        ttl_seconds=max(config.stale_pending_timeout_seconds * 2, 24 * 3600),
                    )
                    try:
                        gh.post_comment(
                            repo,
                            pr_state.number,
                            f"**orcest** detected CI checks stuck in pending state for"
                            f" more than {config.stale_pending_timeout_seconds // 60}m."
                            f" Cancelled {cancelled_count} of {len(run_ids)} run(s) to self-heal."
                            f" CI will restart once the cancellation propagates.",
                            token,
                        )
                    except Exception as e:
                        logger.error(
                            "Failed to post stale-check comment on PR #%d: %s",
                            pr_state.number,
                            e,
                            exc_info=True,
                        )
        elif pr_state.action == PRAction.SKIP_LOCKED:
            logger.debug("PR #%d: locked, skipping", pr_state.number)
        elif pr_state.action == PRAction.SKIP_MAX_ATTEMPTS:
            _back_off_pr_retries(
                repo=repo,
                pr_state=pr_state,
                logger=logger,
                redis=project_redis,
                reason=f"per-SHA retry budget reached ({config.max_attempts} attempts)",
                step=1,
            )
        elif pr_state.action == PRAction.SKIP_MAX_TOTAL_ATTEMPTS:
            _back_off_pr_retries(
                repo=repo,
                pr_state=pr_state,
                logger=logger,
                redis=project_redis,
                reason=(f"total retry budget reached ({config.max_total_attempts} attempts)"),
                step=7,
            )
            # The total-attempt counter only paces the retry cadence. Reset it
            # now so that, once the long backoff cooldown expires, discovery no
            # longer returns SKIP_MAX_TOTAL_ATTEMPTS and work resumes.
            try:
                clear_total_attempts(project_redis, repo, pr_state.number)
            except Exception as e:
                logger.error(
                    "PR #%d: failed to reset total-attempt counter: %s",
                    pr_state.number,
                    e,
                    exc_info=True,
                )
        elif pr_state.action == PRAction.SKIP_BACKOFF:
            logger.info("PR #%d: in backoff cooldown, skipping", pr_state.number)
        elif pr_state.action == PRAction.SKIP_DRAFT:
            logger.debug("PR #%d: draft, skipping", pr_state.number)
        elif pr_state.action == PRAction.SKIP_PENDING:
            logger.debug(f"PR #{pr_state.number}: CI pending, skipping")
        elif pr_state.action == PRAction.SKIP_QUEUED:
            logger.debug(f"PR #{pr_state.number}: task already queued, skipping")
        elif pr_state.action == PRAction.SKIP_ACTIVE:
            logger.debug(f"PR #{pr_state.number}: task in flight, skipping")
        elif pr_state.action == PRAction.SKIP_LABELED:
            logger.debug(f"PR #{pr_state.number}: terminal label, skipping")
        elif pr_state.action == PRAction.SKIP_NO_CHECKS:
            logger.debug(f"PR #{pr_state.number}: no CI checks, skipping")
        elif pr_state.action == PRAction.SKIP_USAGE_COOLDOWN:
            logger.debug("PR #%d: usage-exhausted cooldown active, skipping", pr_state.number)
        else:
            logger.warning(
                "PR #%d: unhandled action %r, skipping", pr_state.number, pr_state.action
            )

    # Discover issues needing implementation. The only backpressure here is
    # the count of unclaimed issue tasks already on the stream — adding more
    # while a backlog of undelivered tasks exists just bloats the queue.
    # Tasks that workers have already claimed don't block: they're consuming
    # capacity, freeing other idle workers to take new work. PR-fix tasks
    # ride a separate stream and share worker capacity organically.
    # The issue stream is SHARED across all projects (tasks:issue:{runner}).
    # If we let that shared backlog gate every project, one busy project that
    # keeps the stream non-empty would starve issue discovery for every other
    # project indefinitely. To preserve the backpressure intent while keeping
    # fairness, exactly one project per poll cycle (force_issue_discovery,
    # rotated round-robin in _poll_cycle) is allowed to bypass the gate.
    # Downstream set_pending_task + per-issue attempt guards still prevent any
    # double-enqueue, so a forced discovery cannot bloat the queue.
    issue_states: list = []
    if unclaimed_issue_tasks > 0 and not force_issue_discovery:
        logger.info(
            "Issue task queue has %d unclaimed entries; deferring issue "
            "discovery for %s this cycle (another project has priority)",
            unclaimed_issue_tasks,
            repo,
        )
    else:
        try:
            issue_states = discover_actionable_issues(
                repo=repo,
                token=token,
                redis=project_redis,
                label_config=labels,
                max_attempts=config.max_attempts,
            )
        except Exception as e:
            logger.error(f"Issue discovery failed: {e}", exc_info=True)

    # Act on issues
    for issue_state in issue_states:
        if issue_state.action == IssueAction.ENQUEUE_IMPLEMENT:
            logger.info(
                f"Issue #{issue_state.number} ({issue_state.title}): enqueueing implementation task"
            )
            entry = _select_provider_entry()
            if entry is None:
                logger.warning(
                    "All providers exhausted, skipping issue task for issue #%d",
                    issue_state.number,
                )
            else:
                try:
                    task = _try_publish(
                        entry,
                        publish_issue_task,
                        issue_state=issue_state,
                        repo=repo,
                        token=token,
                        redis=project_redis,
                        default_runner=config.default_runner,
                        pending_task_ttl=pending_task_ttl,
                        logger=logger,
                    )
                    if task is not None:
                        # pre-registered per hardened contract
                        enqueued += 1
                except Exception as e:
                    logger.error(
                        f"Failed to publish issue task for issue #{issue_state.number}: {e}",
                        exc_info=True,
                    )
        elif issue_state.action == IssueAction.SKIP_MAX_ATTEMPTS:
            # Budget exhaustion is not a human-decision blocker. Orcest does
            # not label the issue needs-human; it simply stops actively
            # retrying until the issue changes (a new comment/edit resets the
            # counter). A human can still pick it up, but orcest is not
            # asserting that one is required.
            logger.info(
                "Issue #%d: per-issue retry budget reached (%d attempts), "
                "pausing retries -- not escalating to a human",
                issue_state.number,
                config.max_attempts,
            )
        elif issue_state.action == IssueAction.SKIP_QUEUED:
            logger.debug(f"Issue #{issue_state.number}: task already queued, skipping")
        elif issue_state.action == IssueAction.SKIP_LOCKED:
            logger.debug(f"Issue #{issue_state.number}: locked, skipping")
        elif issue_state.action == IssueAction.SKIP_ACTIVE:
            logger.debug(f"Issue #{issue_state.number}: task in flight, skipping")
        elif issue_state.action == IssueAction.SKIP_USAGE_COOLDOWN:
            logger.debug(
                "Issue #%d: usage-exhausted cooldown active, skipping",
                issue_state.number,
            )
        elif issue_state.action == IssueAction.SKIP_LABELED:
            logger.debug(f"Issue #{issue_state.number}: terminal label, skipping")
        elif issue_state.action == IssueAction.SKIP_DEPENDENCY:
            blockers = ", ".join(f"#{n}" for n in issue_state.open_blockers)
            logger.info(
                "Issue #%d: deferred, waiting on open blocker(s): %s",
                issue_state.number,
                blockers or "(unknown)",
            )
        else:
            logger.warning(
                f"Issue #{issue_state.number}: unhandled action {issue_state.action!r}, skipping"
            )

    return enqueued, merged, len(pr_states), len(issue_states)


def _consume_results_for_project(
    project: ProjectConfig,
    redis: RedisClient,
    labels: LabelConfig,
    logger: logging.Logger,
    token_pool: ProviderPool | None = None,
    max_transient_failures: int = 5,
    shared_credential_redis: RedisClient | None = None,
) -> None:
    """Consume any pending results from workers for a single project.

    Non-blocking: reads all available results without waiting.

    First drains pending entries (delivered but not ACKed — can happen if
    the orchestrator was restarted mid-cycle), then reads new entries.
    This prevents orphaned labels from results that were read but never
    processed after a restart.
    """
    logger = logger.getChild(project.repo)
    # Phase 1: Drain pending (unACKed) entries from previous runs
    while True:
        entries = redis.xreadgroup(
            group=RESULTS_GROUP,
            consumer="orchestrator-main",
            stream=RESULTS_STREAM,
            count=10,
            block_ms=None,
            pending=True,
        )
        if not entries:
            break
        for entry_id, fields in entries:
            try:
                result = TaskResult.from_dict(fields)
                _handle_result(
                    project,
                    labels,
                    redis,
                    result,
                    logger,
                    token_pool=token_pool,
                    max_transient_failures=max_transient_failures,
                    shared_credential_redis=shared_credential_redis,
                )
                logger.info(f"Recovered pending result {entry_id}")
            except _RetryableResultError as exc:
                logger.warning(
                    "Deferring pending result %s until durable side effects recover: %s",
                    entry_id,
                    exc,
                )
                # Leave it in the PEL and stop this pass. Immediately reading
                # pending ID 0 again would hot-loop on the same entry.
                return
            except Exception as e:
                logger.error(
                    f"Failed to process pending result {entry_id}: {e}",
                    exc_info=True,
                )
            try:
                redis.xack(RESULTS_STREAM, RESULTS_GROUP, entry_id)
            except Exception as ack_err:
                logger.error(
                    f"Failed to ACK pending result {entry_id}: {ack_err}",
                    exc_info=True,
                )

    # Phase 2: Read new entries
    while True:
        entries = redis.xreadgroup(
            group=RESULTS_GROUP,
            consumer="orchestrator-main",
            stream=RESULTS_STREAM,
            count=10,
            block_ms=None,
        )

        if not entries:
            break

        for entry_id, fields in entries:
            try:
                result = TaskResult.from_dict(fields)
                _handle_result(
                    project,
                    labels,
                    redis,
                    result,
                    logger,
                    token_pool=token_pool,
                    max_transient_failures=max_transient_failures,
                    shared_credential_redis=shared_credential_redis,
                )
            except _RetryableResultError as exc:
                logger.warning(
                    "Leaving result entry %s pending until durable side effects recover: %s",
                    entry_id,
                    exc,
                )
                # Continue through any remaining new entries, but do not ACK
                # this one; the next poll's pending phase will retry it.
                continue
            except Exception as e:
                logger.error(
                    "Failed to process result entry %s: %s",
                    entry_id,
                    e,
                    exc_info=True,
                )
            # Always ACK to prevent infinite reprocessing of
            # malformed or unhandleable entries.
            try:
                redis.xack(RESULTS_STREAM, RESULTS_GROUP, entry_id)
            except Exception as ack_err:
                logger.error(
                    "Failed to ACK result entry %s: %s",
                    entry_id,
                    ack_err,
                    exc_info=True,
                )


def _handle_result(
    project: ProjectConfig,
    labels: LabelConfig,
    redis: RedisClient,
    result: TaskResult,
    logger: logging.Logger,
    token_pool: ProviderPool | None = None,
    max_transient_failures: int = 5,
    shared_credential_redis: RedisClient | None = None,
) -> None:
    """Process a single task result.

    Posts a comment on the resource (PR or issue) with the result summary
    and manages labels:
    - completed: clears issue attempt counter; PR attempts remain until the SHA changes
    - failed: retried automatically; needs-human is added ONLY when the worker
      explicitly reported a human-decision blocker (result.needs_human)
    - blocked: adds blocked label
    - usage_exhausted: no label changes; resource resumes after cooldown
    """
    logger.info(
        "Result for task %s: %s (worker: %s, %ss)",
        result.task_id,
        result.status.value,
        result.worker_id,
        result.duration_seconds,
    )

    def _release_provider_task_tracking() -> None:
        if token_pool is not None:
            token_pool.task_completed(result.task_id)
        _clear_task_provider_account(redis, result.task_id, logger)

    def _load_usage_exhausted_account() -> str | None:
        if token_pool is None:
            return None
        return _load_task_provider_account(redis, result.task_id, logger)

    # Credential write-back: capture a rotated OAuth blob BEFORE any staleness
    # or exhaustion handling pops the task->identity mapping. Valid regardless
    # of task staleness (the token rotation is real either way).
    if result.credential_update and token_pool is not None:
        # New workers obtain this ordering value from the shared Redis clock
        # and a monotonic sequence. Legacy payloads fall back to consume time
        # for wire compatibility.
        minted_at = result.credential_update_minted_at or time.time()
        credential_update_account: str | None = None
        task_entry = token_pool.get_task_entry(result.task_id)
        if task_entry is not None:
            credential_update_account = task_entry.account_key()
        if credential_update_account is None:
            credential_update_account = _load_task_provider_account(
                redis, result.task_id, logger
            )
        if credential_update_account and token_pool.credential_update_is_usable(
            credential_update_account, result.credential_update
        ):
            # Apply locally, but persist independently of whether this local
            # CAS changed state. A prior attempt may have applied the update
            # and then failed the canonical shared write.
            token_pool.apply_credential_update_for_account(
                credential_update_account,
                result.credential_update,
                minted_at,
            )
            shared_stored = _persist_credential_override(
                redis,
                credential_update_account,
                result.credential_update,
                minted_at,
                logger,
                shared_redis=shared_credential_redis,
            )
            if shared_credential_redis is not None:
                # Whether our CAS won or lost, seed the local pool from the
                # canonical winner before this process publishes another task.
                _refresh_shared_credential_override(
                    shared_credential_redis,
                    token_pool,
                    credential_update_account,
                    logger,
                )
            if shared_stored:
                logger.info(
                    "Captured refreshed credential for provider account %s (task %s)",
                    credential_update_account,
                    result.task_id,
                )
            else:
                logger.info(
                    "Ignored stale credential refresh for provider account %s (task %s)",
                    credential_update_account,
                    result.task_id,
                )
        else:
            _record_credential_refresh_failure_if_needed(
                redis,
                credential_update_account,
                result.credential_update,
                logger,
            )

    repo = project.repo
    token = project.token
    resource_id = result.resource_id
    is_issue = result.resource_type == "issue"
    resource_label = "issue" if is_issue else "PR"
    resource_type = result.resource_type or ("issue" if is_issue else "pr")

    # Guard against stale task IDs. GitHub snapshot validation below is the
    # authoritative staleness check for PR tasks; the pending marker is only
    # coordination state that prevents an old result from affecting a newer
    # in-flight task.
    try:
        current_task_id = get_pending_task(redis, repo, resource_type, resource_id)
        if current_task_id is not None and current_task_id != result.task_id:
            logger.warning(
                "Stale result for %s #%d: result task_id=%s but active task_id=%s; "
                "skipping label/comment side-effects",
                resource_label,
                resource_id,
                result.task_id,
                current_task_id,
            )
            if result.status == ResultStatus.USAGE_EXHAUSTED:
                _mark_usage_exhausted_token(
                    result,
                    token_pool,
                    logger,
                    account_key=_load_usage_exhausted_account(),
                )
            _release_provider_task_tracking()
            return
    except Exception as e:
        logger.error(
            "Failed to check pending task ID for %s #%d: %s; proceeding with result processing",
            resource_label,
            resource_id,
            e,
            exc_info=True,
        )

    if result.status == ResultStatus.STALE or _is_pr_result_stale(project, result, logger):
        try:
            clear_pending_task_if_matches(redis, repo, resource_type, resource_id, result.task_id)
        except Exception:
            logger.warning(
                "Failed to clear stale pending task marker for %s #%d",
                resource_label,
                resource_id,
                exc_info=True,
            )
        if not is_issue:
            try:
                clear_attempts_if_head_sha(redis, repo, resource_id, result.snapshot_head_sha)
            except Exception:
                logger.warning(
                    "Failed to clear attempt reservation for stale PR result #%d",
                    resource_id,
                    exc_info=True,
                )
        if result.status == ResultStatus.USAGE_EXHAUSTED:
            _mark_usage_exhausted_token(
                result,
                token_pool,
                logger,
                account_key=_load_usage_exhausted_account(),
            )
        _release_provider_task_tracking()
        return

    # Select the right GitHub functions based on resource type
    _add_label = gh.add_issue_label if is_issue else gh.add_label
    _post_comment = gh.post_issue_comment if is_issue else gh.post_comment

    # Clear the pending-task marker so the orchestrator can enqueue again
    # if needed. This applies to ALL result statuses — the task is no longer
    # pending regardless of whether it succeeded or failed.
    try:
        clear_pending_task_if_matches(redis, repo, resource_type, resource_id, result.task_id)
    except Exception as e:
        logger.error(
            f"Failed to clear pending task marker for {resource_label} #{resource_id}: {e}",
            exc_info=True,
        )

    # Completed issue tasks are resolved by removing the ready label below, so
    # their attempt counter can be cleared. Completed PR tasks are intentionally
    # different: a worker can report success without changing PR head or resolving
    # the failing condition. Leaving the per-SHA guard in place prevents unlimited
    # same-SHA retries; a real pushed commit resets the guard during discovery.
    # Do NOT clear total_attempts here — that cross-SHA circuit breaker should
    # only be reset when the PR is truly resolved (merged), not on intermediate
    # task successes.
    if result.status == ResultStatus.COMPLETED:
        if is_issue:
            try:
                clear_issue_attempts(redis, repo, resource_id)
            except Exception as e:
                logger.error(
                    f"Failed to clear attempt counter for {resource_label} #{resource_id}: {e}",
                    exc_info=True,
                )
        else:
            try:
                clear_backoff(redis, repo, resource_id)
                clear_transient_failure_count(redis, repo, resource_id)
            except Exception as e:
                logger.error(
                    f"Failed to clear transient backoff state for PR #{resource_id}: {e}",
                    exc_info=True,
                )

        # Remove orcest:ready label from completed issues so they are not
        # re-discovered on the next poll cycle.
        if is_issue:
            try:
                gh.remove_issue_label(repo, resource_id, labels.ready, token)
            except Exception as e:
                logger.error(
                    f"Failed to remove ready label from issue #{resource_id}: {e}",
                    exc_info=True,
                )
    elif result.status == ResultStatus.USAGE_EXHAUSTED:
        usage_accounting_processed = False
        try:
            usage_accounting_processed = redis.set_nx_ex(
                _make_usage_exhausted_processed_key(result.task_id),
                "1",
                _USAGE_EXHAUSTED_PROCESSED_TTL_SECONDS,
            )
            if usage_accounting_processed:
                count = redis.incr(_USAGE_EXHAUSTED_RESULT_KEY)
                if count == 1:
                    redis.expire(
                        _USAGE_EXHAUSTED_RESULT_KEY,
                        _USAGE_EXHAUSTED_RESULT_TTL_SECONDS,
                    )
        except Exception:
            logger.debug("Failed to increment usage-exhausted result counter", exc_info=True)

        # Mark the exhausted token in the pool so it's skipped in future rounds.
        # Use the resets_at timestamp from the stream-json rate_limit_event if
        # available; fall back to querying the usage endpoint; final fallback 30 min.
        _mark_usage_exhausted_token(
            result,
            token_pool,
            logger,
            account_key=_load_usage_exhausted_account(),
        )
        usage_cooldown_ttl = _usage_exhausted_cooldown_ttl_seconds(result)
        # PR-specific cooldown: clear per-SHA attempts so PR can be re-enqueued
        # after the cooldown expires.  Issues don't have per-SHA counters.
        if not is_issue:
            cooldown_set = False
            try:
                set_usage_exhausted_cooldown(
                    redis,
                    repo,
                    resource_id,
                    ttl_seconds=usage_cooldown_ttl,
                )
                cooldown_set = True
            except Exception as e:
                logger.error(
                    f"Failed to set usage-exhausted cooldown for PR #{resource_id}: {e}",
                    exc_info=True,
                )
            if cooldown_set:
                try:
                    clear_attempts(redis, repo, resource_id)
                    logger.info(
                        "PR #%d: cleared per-SHA attempt counter after USAGE_EXHAUSTED",
                        resource_id,
                    )
                except Exception as e:
                    logger.error(
                        f"Failed to clear per-SHA attempt counter for PR #{resource_id} "
                        f"after USAGE_EXHAUSTED: {e}",
                        exc_info=True,
                    )
        else:
            try:
                set_issue_usage_exhausted_cooldown(
                    redis,
                    repo,
                    resource_id,
                    ttl_seconds=usage_cooldown_ttl,
                )
            except Exception as e:
                logger.error(
                    f"Failed to set usage-exhausted cooldown for issue #{resource_id}: {e}",
                    exc_info=True,
                )
            try:
                clear_issue_attempts(redis, repo, resource_id)
                logger.info(
                    "Issue #%d: cleared attempt counter after USAGE_EXHAUSTED",
                    resource_id,
                )
            except Exception as e:
                logger.error(
                    f"Failed to clear issue attempt counter for issue #{resource_id} "
                    f"after USAGE_EXHAUSTED: {e}",
                    exc_info=True,
                )

    # Task 8: per-provider rebake_required_failures counter for "clean rebake" style
    # permanent failures (worker image missing the provider CLI). Uses only the
    # lean ProviderEntry.provider from the task registration (no execution details).
    if (
        result.status == ResultStatus.FAILED
        and result.summary
        and "rebake worker image" in result.summary.lower()
    ):
        if token_pool is not None:
            entry = token_pool.get_task_entry(result.task_id)
            if entry and hasattr(entry, "provider"):
                prov = entry.provider
                rkey = f"providers:{prov}:rebake_required_failures"
                try:
                    rcount = redis.incr(rkey)
                    if rcount == 1:
                        redis.expire(rkey, _REBAKE_REQUIRED_FAILURES_TTL_SECONDS)
                except Exception:
                    logger.debug(
                        "Failed to increment rebake-required-failures counter for provider %s",
                        prov,
                        exc_info=True,
                    )

    # Transient failures (clone timeout, provider overload, worker restart)
    # should be retried automatically without burning the cross-SHA PR attempt
    # budget. They retry immediately until max_transient_failures is exceeded;
    # after that they use the normal backoff cadence.
    is_transient = result.status == ResultStatus.FAILED and result.summary.startswith(
        TRANSIENT_SUMMARY_PREFIX
    )

    if is_transient:
        transient_accounting_processed = False
        try:
            transient_accounting_processed = redis.set_nx_ex(
                _make_transient_failure_processed_key(result.task_id),
                "1",
                _TRANSIENT_FAILURE_PROCESSED_TTL_SECONDS,
            )
        except Exception:
            logger.debug("Failed to mark transient failure as processed", exc_info=True)

        try:
            if not transient_accounting_processed:
                logger.debug(
                    "Skipping duplicate transient cleanup for task %s",
                    result.task_id,
                )
            elif is_issue:
                clear_issue_attempts(redis, repo, resource_id)
            else:
                head_sha = result.snapshot_head_sha
                try:
                    attempt_data: dict[str, str] = redis.hgetall(
                        f"pr:{repo}:{resource_id}:attempts"
                    )
                    attempt_head_sha = attempt_data.get("head_sha", "")
                    if attempt_head_sha:
                        head_sha = attempt_head_sha
                except Exception:
                    logger.debug(
                        "Failed to read attempt SHA before transient cleanup for PR #%d",
                        resource_id,
                        exc_info=True,
                    )
                transient_count = increment_transient_failure_count(redis, repo, resource_id)
                if transient_count > max(0, max_transient_failures):
                    # Once the silent retry budget is exhausted, lengthen
                    # the cooldown with each additional transient failure.
                    # get_backoff_cooldown_seconds clamps the step, so this
                    # plateaus rather than growing without bound.
                    backoff_step = transient_count - max(0, max_transient_failures) - 1
                    set_backoff_cooldown(
                        redis,
                        repo,
                        resource_id,
                        backoff_step,
                        head_sha=head_sha,
                    )
                clear_attempts(redis, repo, resource_id)
        except Exception as e:
            logger.error(
                f"Failed to clear attempts for transient failure on "
                f"{resource_label} #{resource_id}: {e}",
                exc_info=True,
            )

    # Manage labels based on result status.
    #
    # needs-human is applied for exactly one reason: the worker's agent
    # explicitly reported a genuine human-decision blocker (result.needs_human).
    # Orcest never infers needs-human from a failure or a budget count -- an
    # ordinary fix-attempt failure is just retried on the next eligible cycle.
    labeled = False
    needs_human = result.status == ResultStatus.FAILED and result.needs_human
    if result.status == ResultStatus.FAILED and not is_transient and not is_issue:
        # The total-attempt counter only paces the retry cadence (see the
        # SKIP_MAX_TOTAL_ATTEMPTS handler); it never escalates.
        try:
            increment_total_attempts(redis, repo, resource_id)
        except Exception as e:
            logger.error(
                f"Failed to increment total-attempt counter for PR #{resource_id}: {e}",
                exc_info=True,
            )
    if needs_human:
        try:
            _add_label(repo, resource_id, labels.needs_human, token)
            labeled = True
        except Exception as e:
            logger.error(
                f"Failed to add needs-human label on {resource_label} #{resource_id}: {e}",
                exc_info=True,
            )
    elif result.status == ResultStatus.BLOCKED:
        try:
            _add_label(repo, resource_id, labels.blocked, token)
            labeled = True
        except Exception as e:
            logger.error(
                f"Failed to add blocked label on {resource_label} #{resource_id}: {e}",
                exc_info=True,
            )

    # Only post comments for non-success statuses (failures, blocked, etc.)
    # Success is silent to avoid comment noise on PRs/issues.
    if result.status != ResultStatus.COMPLETED:
        safe_summary = result.summary[:500] if result.summary else ""

        if result.status == ResultStatus.FAILED and not needs_human:
            # An ordinary (transient or otherwise) fix-attempt failure is
            # retried automatically -- stay silent to avoid accumulating
            # comment noise across attempts.
            _release_provider_task_tracking()
            return
        elif result.status == ResultStatus.FAILED:
            # needs_human is True: the worker's agent explicitly asked for a
            # human decision. Surface its reason so the human knows what to do.
            reason = result.needs_human_reason or "(no reason given)"
            label_note = (
                f"Labeled `{labels.needs_human}`."
                if labeled
                else f"Failed to add `{labels.needs_human}` label — please add it manually."
            )
            body = (
                f"**orcest** needs a human decision on this PR.\n\n"
                f"The worker reported: {reason}\n\n"
                f"{label_note}"
            )
        elif result.status == ResultStatus.BLOCKED:
            label_note = (
                f"Labeling as `{labels.blocked}` — waiting for external input."
                if labeled
                else f"Failed to add `{labels.blocked}` label — please triage manually."
            )
            body = (
                f"**orcest** task `{result.task_id}` is blocked "
                f"({result.duration_seconds}s, "
                f"worker: {result.worker_id}).\n\n"
                f"Summary: {safe_summary}\n\n"
                f"{label_note}"
            )
        elif result.status == ResultStatus.USAGE_EXHAUSTED:
            branch_note = (
                f"Work saved on branch `{result.branch}`. " if result.branch else "Work saved. "
            )
            body = (
                f"**orcest** task `{result.task_id}` paused "
                f"(usage limit reached, "
                f"worker: {result.worker_id}).\n\n"
                f"{branch_note}"
                f"Will resume when capacity is available."
            )
        else:
            body = (
                f"**orcest** task `{result.task_id}`: "
                f"{result.status.value} "
                f"({result.duration_seconds}s, "
                f"worker: {result.worker_id}).\n\n"
                f"Summary: {safe_summary}"
            )

        try:
            _post_comment(repo, resource_id, body, token)
        except Exception as e:
            logger.error(
                f"Failed to post comment on {resource_label} #{resource_id}: {e}",
                exc_info=True,
            )

        logger.info("Result comment: %s...", body[:100])

    # Clean up ProviderPool tracking. This unconditional trailing call is the
    # safety net guaranteeing the UUID -> identity mapping in
    # ProviderPool._task_identities does NOT leak for any non-USAGE_EXHAUSTED,
    # non-rebake result path (SUCCEEDED/COMPLETED, needs_human FAILED, BLOCKED,
    # transient, etc.). mark_exhausted already pops the mapping for exhausted
    # results, so task_completed is a no-op there (safe to call). The durable
    # task -> account mapping is cleared at the same boundary.
    _release_provider_task_tracking()
