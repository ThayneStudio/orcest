"""Task and result dataclasses for Redis stream serialization.

All to_dict values are strings (Redis streams require flat {str: str} entries).
Empty string is used as the None sentinel for optional fields.
"""

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum

# Shared consumer group name used by workers, orchestrator, and pool manager.
CONSUMER_GROUP = "workers"

# Redis stream / key name constants
DEAD_LETTER_STREAM = "dead-letter"

# Wire-protocol prefix used by workers to signal a transient failure.
# The orchestrator parses this to decide whether to retry or label for human review.
TRANSIENT_SUMMARY_PREFIX = "[transient] "

# Fields added by the dead-letter handler that are not part of the original task.
# Shared here so both the writer (worker/loop.py) and the reader (cli.py dead-letters
# command) reference the same canonical set — a rename stays consistent automatically.
DEAD_LETTER_METADATA_FIELDS = frozenset(
    {"dead_letter_reason", "tasks_stream", "original_entry_id", "delivery_count"}
)

# Sensitive fields whose values must be redacted in to_safe_dict(), __repr__(),
# logs, exceptions, and dead-letter displays. This is the core of the
# systematic redaction layer required by the security review before generalizing
# beyond claude_token.
REDACTED_FIELDS = frozenset({"token", "claude_token", "credential"})
CLAUDE_PROVIDER_ALIASES = frozenset({"claude", "clauder"})


def is_claude_provider(provider: str) -> bool:
    """Return True for provider names backed by the Claude CLI."""
    return provider in CLAUDE_PROVIDER_ALIASES


def task_stream_name(provider: str, *, issue: bool = False) -> str:
    """Return the Redis task stream name for a provider."""
    normalized = str(provider or "").strip()
    if not normalized:
        raise ValueError("provider must be non-empty")
    prefix = "tasks:issue" if issue else "tasks"
    return f"{prefix}:{normalized}"


def _sync_claude_for_provider(provider: str, credential: str, claude_token: str) -> tuple[str, str]:
    """Centralize the transition-era 'Claude provider sync' logic.

    For provider=="claude", ensure both credential and claude_token carry the
    (same) secret so that legacy code paths (still reading .claude_token) and
    new paths (reading .credential) both work during the rollout.

    Non-claude providers pass through unchanged.

    This eliminates the previous duplication of the sync across create()
    and to_dict(); from_dict() uses separate one-directional deserialization
    logic (to correctly handle explicit empty `credential`).
    """
    if not is_claude_provider(provider):
        return credential, claude_token
    # Prefer whichever is truthy; if both supplied and different we keep the
    # respective values (rare during transition).
    eff_cred = credential or claude_token
    eff_ct = claude_token or credential
    return eff_cred, eff_ct


class TaskType(str, Enum):
    FIX_PR = "fix_pr"
    FIX_CI = "fix_ci"
    CLASSIFY_CI = "classify_ci"  # Phase 2
    IMPLEMENT_ISSUE = "implement_issue"  # Phase 2
    IMPROVE_CODEBASE = "improve"  # Phase 3
    TRIAGE_FOLLOWUPS = "triage_followups"  # Triage unresolved review threads into issues
    REBASE_PR = "rebase_pr"  # Rebase branch to resolve merge conflicts


class ResultStatus(str, Enum):
    COMPLETED = "completed"
    FAILED = "failed"
    BLOCKED = "blocked"
    USAGE_EXHAUSTED = "usage_exhausted"
    STALE = "stale"


@dataclass
class Task:
    id: str
    type: TaskType
    repo: str  # "owner/repo"
    token: str  # GitHub PAT for clone + gh auth
    claude_token: str  # Claude Code OAuth token (per-task, from org config)
    resource_type: str  # "pr" or "issue"
    resource_id: int  # PR/issue number
    prompt: str  # Full rendered prompt
    branch: str | None  # Existing branch (for PR fixes)
    base_branch: str | None  # Base branch to rebase onto (e.g. "main", "master")
    key_prefix: str  # Redis key prefix for multi-project routing
    created_at: datetime
    # New fields for multi-provider support (Task 2). Defaults ensure
    # backward compatibility with existing Task.create() call sites and
    # legacy serialized payloads. claude_token is retained (and kept populated)
    # during the transition so old workers continue to function.
    provider: str = "claude"
    credential: str = ""
    model: str | None = None
    snapshot_head_sha: str = ""  # PR head SHA this task was derived from
    decision_reason: str = ""  # Why this task was enqueued (ci_failure, changes_requested, etc.)
    snapshot_failed_checks: list[str] | None = None
    snapshot_review_thread_ids: list[str] | None = None
    snapshot_review_thread_fingerprints: list[str] | None = None
    # Stable, non-secret provider account selected by the orchestrator. This is
    # anchored to the configured credential even when ``credential`` carries a
    # later OAuth rotation.
    provider_account: str = ""

    def to_dict(self) -> dict[str, str]:
        """Serialize to flat string dict for Redis stream XADD.

        For the transition period we always emit both the legacy ``claude_token``
        (populated when provider=="claude") and the new ``provider``/``credential``/
        ``model`` fields so that mixed old/new orchestrators and workers can
        interoperate without losing the per-task credential.
        """
        # Keep claude_token populated from credential for claude provider during rollout.
        # Delegated to the shared helper (removes duplication of sync logic
        # that previously lived in to_dict and create; from_dict has separate
        # one-directional handling for explicit credential values).
        _ignored_cred, claude_token_out = _sync_claude_for_provider(
            self.provider, self.credential, self.claude_token
        )
        return {
            "id": self.id,
            "type": self.type.value,
            "repo": self.repo,
            "token": self.token,
            "claude_token": claude_token_out,
            "resource_type": self.resource_type,
            "resource_id": str(self.resource_id),
            "prompt": self.prompt,
            "branch": self.branch or "",
            "base_branch": self.base_branch or "",
            "key_prefix": self.key_prefix,
            "created_at": self.created_at.isoformat(),
            "snapshot_head_sha": self.snapshot_head_sha,
            "decision_reason": self.decision_reason,
            "snapshot_failed_checks": json.dumps(self.snapshot_failed_checks or []),
            "snapshot_review_thread_ids": json.dumps(self.snapshot_review_thread_ids or []),
            "snapshot_review_thread_fingerprints": json.dumps(
                self.snapshot_review_thread_fingerprints or []
            ),
            # New multi-provider fields (always present for new consumers)
            "provider": self.provider,
            "credential": self.credential,
            "model": self.model or "",
            "provider_account": self.provider_account,
        }

    @classmethod
    def from_dict(cls, data: dict[str, str]) -> "Task":
        """Deserialize from Redis stream entry fields.

        Tolerates legacy payloads that only contain ``claude_token`` (pre-Task-2
        orchestrators) by synthesizing ``provider="claude"`` + ``credential`` from it.
        New payloads with explicit provider/credential take precedence.
        """
        # Legacy tolerance + new field synthesis (Step 2.1 / 2.4 requirement)
        claude_token_in = data.get("claude_token", "")
        provider_in = data.get("provider") or "claude"
        # Proper presence check (not "or") so that explicit empty-string "credential"
        # (valid per wire protocol) is respected and does not fall back to claude_token.
        # Absent key -> None; explicit "" -> ""; only fallback on absent.
        raw_credential = data.get("credential")
        credential_in = raw_credential if raw_credential is not None else (claude_token_in or "")
        model_in = data.get("model") or None

        # For claude provider during transition, ensure claude_token is populated
        # so that code paths still reading task.claude_token continue to work.
        # This one-directional fill (cred -> claude) respects explicit credential
        # values (including empty) taking precedence per the new-field contract.
        if is_claude_provider(provider_in) and not claude_token_in and credential_in:
            claude_token_in = credential_in

        return cls(
            id=data["id"],
            type=TaskType(data["type"]),
            repo=data["repo"],
            token=data["token"],
            claude_token=claude_token_in,
            resource_type=data["resource_type"],
            resource_id=int(data["resource_id"]),
            prompt=data["prompt"],
            branch=data["branch"] or None,
            base_branch=data.get("base_branch") or None,
            key_prefix=data.get("key_prefix", ""),
            created_at=datetime.fromisoformat(data["created_at"]),
            snapshot_head_sha=data.get("snapshot_head_sha", ""),
            decision_reason=data.get("decision_reason", ""),
            snapshot_failed_checks=_json_list(data.get("snapshot_failed_checks", "")),
            snapshot_review_thread_ids=_json_list(data.get("snapshot_review_thread_ids", "")),
            snapshot_review_thread_fingerprints=_json_list(
                data.get("snapshot_review_thread_fingerprints", "")
            ),
            # New fields (with legacy-derived defaults)
            provider=provider_in,
            credential=credential_in,
            model=model_in,
            provider_account=data.get("provider_account", ""),
        )

    @classmethod
    def create(
        cls,
        task_type: TaskType,
        repo: str,
        token: str,
        resource_type: str,
        resource_id: int,
        prompt: str,
        branch: str | None = None,
        base_branch: str | None = None,
        claude_token: str = "",
        key_prefix: str = "",
        snapshot_head_sha: str = "",
        decision_reason: str = "",
        snapshot_failed_checks: list[str] | None = None,
        snapshot_review_thread_ids: list[str] | None = None,
        snapshot_review_thread_fingerprints: list[str] | None = None,
        # New params for multi-provider (defaults keep all existing call sites working)
        provider: str = "claude",
        credential: str = "",
        model: str | None = None,
        # task_id allows register-before-publish + failure cleanup contract (Task 5):
        # pre-generate id, register_task(id, entry), pass here so create uses it;
        # on publish failure/None paths, caller calls task_completed to rollback.
        task_id: str | None = None,
        provider_account: str = "",
    ) -> "Task":
        """Factory with auto-generated ID and timestamp.

        During transition, if only claude_token is supplied we derive credential +
        provider so that the embedded secret is available under the new name too.
        Callers may also pass provider/credential/model explicitly (future path).
        Optional task_id supports the hardened register-before-xadd contract in
        orchestrator without changing id generation for legacy callers.
        """
        # Derive for transition compatibility (keep claude_token populated too)
        # Uses shared helper to eliminate duplication of Claude sync logic.
        effective_credential, effective_claude_token = _sync_claude_for_provider(
            provider, credential, claude_token
        )

        return cls(
            id=task_id if task_id is not None else str(uuid.uuid4()),
            type=task_type,
            repo=repo,
            token=token,
            claude_token=effective_claude_token,
            resource_type=resource_type,
            resource_id=resource_id,
            prompt=prompt,
            branch=branch,
            base_branch=base_branch,
            key_prefix=key_prefix,
            created_at=datetime.now(timezone.utc),
            snapshot_head_sha=snapshot_head_sha,
            decision_reason=decision_reason,
            snapshot_failed_checks=snapshot_failed_checks,
            snapshot_review_thread_ids=snapshot_review_thread_ids,
            snapshot_review_thread_fingerprints=snapshot_review_thread_fingerprints,
            provider=provider,
            credential=effective_credential,
            model=model,
            provider_account=provider_account,
        )

    def to_safe_dict(self) -> dict[str, str]:
        """Return a redacted copy of to_dict() suitable for logs, exceptions,
        dead-letter metadata, and any human-visible or persisted diagnostic path.

        All fields listed in REDACTED_FIELDS have their values replaced by
        "[REDACTED]" so that secrets never appear in plaintext outside the
        minimal hot paths (Redis streams consumed only by trusted workers that
        need the real credential at runtime).
        """
        d = self.to_dict()
        for field in REDACTED_FIELDS:
            if field in d:
                d[field] = "[REDACTED]"
        return d

    def __repr__(self) -> str:
        """Safe repr that never leaks raw credentials (addresses security review finding).

        Uses short prefix mask (like ProviderEntry) for the secret fields so
        operators can still correlate "which token was involved" in logs without
        exposing the full secret.
        """

        def _mask(val: str) -> str:
            return (val[:4] + "...") if val else ""

        return (
            f"Task(id={self.id!r}, type={self.type.value!r}, repo={self.repo!r}, "
            f"token={_mask(self.token)!r}, claude_token={_mask(self.claude_token)!r}, "
            f"provider={self.provider!r}, credential={_mask(self.credential)!r}, "
            f"model={self.model!r}, resource_type={self.resource_type!r}, "
            f"resource_id={self.resource_id}, ...)"
        )


@dataclass
class TaskResult:
    task_id: str
    worker_id: str
    status: ResultStatus
    branch: str | None
    summary: str
    duration_seconds: int
    resource_type: str  # "pr" or "issue" -- needed so orchestrator can post comments
    resource_id: int  # PR/issue number
    rate_limit_resets_at: int = 0  # Unix timestamp when rate limit resets (0 = unknown)
    snapshot_head_sha: str = ""
    decision_reason: str = ""
    snapshot_failed_checks: list[str] | None = None
    snapshot_review_thread_ids: list[str] | None = None
    snapshot_review_thread_fingerprints: list[str] | None = None
    # Set only when the worker's agent explicitly reported a genuine
    # human-decision blocker. This is the sole trigger for the needs-human
    # label; orcest never infers it from failure counts.
    needs_human: bool = False
    needs_human_reason: str = ""
    # OAuth-blob providers (Grok/Codex) may refresh their token in place during
    # a run; the worker surfaces the rotated blob here so the orchestrator can
    # persist it. A SECRET — redacted in to_safe_dict, never logged in plaintext.
    credential_update: str = ""
    # Keep newly-added optional fields at the end so callers using the historic
    # positional constructor keep the same argument mapping.
    repo: str = ""  # "owner/repo"; optional for legacy result payloads
    credential_update_minted_at: float = 0.0
    # Non-secret provider-account identity (provider + credential hash). New
    # workers include it so credential write-back survives an orchestrator
    # restart without persisting the credential itself outside the task/result
    # streams. Optional for rolling compatibility with older workers.
    provider_account: str = ""

    def to_dict(self) -> dict[str, str]:
        """Serialize to flat string dict for Redis stream XADD."""
        d = {
            "task_id": self.task_id,
            "worker_id": self.worker_id,
            "status": self.status.value,
            "branch": self.branch or "",
            "summary": self.summary,
            "duration_seconds": str(self.duration_seconds),
            "resource_type": self.resource_type,
            "resource_id": str(self.resource_id),
            "repo": self.repo,
            "snapshot_head_sha": self.snapshot_head_sha,
            "decision_reason": self.decision_reason,
            "snapshot_failed_checks": json.dumps(self.snapshot_failed_checks or []),
            "snapshot_review_thread_ids": json.dumps(self.snapshot_review_thread_ids or []),
            "snapshot_review_thread_fingerprints": json.dumps(
                self.snapshot_review_thread_fingerprints or []
            ),
        }
        if self.rate_limit_resets_at:
            d["rate_limit_resets_at"] = str(self.rate_limit_resets_at)
        if self.needs_human:
            d["needs_human"] = "1"
            d["needs_human_reason"] = self.needs_human_reason
        if self.credential_update:
            d["credential_update"] = self.credential_update
            if self.credential_update_minted_at:
                d["credential_update_minted_at"] = str(self.credential_update_minted_at)
        if self.provider_account:
            d["provider_account"] = self.provider_account
        return d

    def to_safe_dict(self) -> dict[str, str]:
        """to_dict() with the credential blob redacted, for logging/diagnostics."""
        d = self.to_dict()
        if d.get("credential_update"):
            d["credential_update"] = "[REDACTED]"
        return d

    @classmethod
    def from_dict(cls, data: dict[str, str]) -> "TaskResult":
        """Deserialize from Redis stream entry fields."""
        return cls(
            task_id=data["task_id"],
            worker_id=data["worker_id"],
            status=ResultStatus(data["status"]),
            branch=data["branch"] or None,
            summary=data["summary"],
            duration_seconds=int(data["duration_seconds"]),
            resource_type=data["resource_type"],
            resource_id=int(data["resource_id"]),
            repo=data.get("repo", ""),
            rate_limit_resets_at=int(data.get("rate_limit_resets_at", "0")),
            snapshot_head_sha=data.get("snapshot_head_sha", ""),
            decision_reason=data.get("decision_reason", ""),
            snapshot_failed_checks=_json_list(data.get("snapshot_failed_checks", "")),
            snapshot_review_thread_ids=_json_list(data.get("snapshot_review_thread_ids", "")),
            snapshot_review_thread_fingerprints=_json_list(
                data.get("snapshot_review_thread_fingerprints", "")
            ),
            needs_human=data.get("needs_human", "") == "1",
            needs_human_reason=data.get("needs_human_reason", ""),
            credential_update=data.get("credential_update", ""),
            credential_update_minted_at=float(data.get("credential_update_minted_at", "0")),
            provider_account=data.get("provider_account", ""),
        )


def _json_list(raw: str) -> list[str]:
    if not raw:
        return []
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return []
    if not isinstance(value, list):
        return []
    return [str(item) for item in value]
