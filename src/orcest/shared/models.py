"""Task and result dataclasses for Redis stream serialization.

All to_dict values are strings (Redis streams require flat {str: str} entries).
Empty string is used as the None sentinel for optional fields.
"""

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum

# Shared consumer group name used by workers, orchestrator, and pool manager.
CONSUMER_GROUP = "workers"

# Redis stream / key name constants
DEAD_LETTER_STREAM = "orcest:dead-letter"

# Wire-protocol prefix used by workers to signal a transient failure.
# The orchestrator parses this to decide whether to retry or label for human review.
TRANSIENT_SUMMARY_PREFIX = "[transient] "

# Fields added by the dead-letter handler that are not part of the original task.
# Shared here so both the writer (worker/loop.py) and the reader (cli.py dead-letters
# command) reference the same canonical set — a rename stays consistent automatically.
DEAD_LETTER_METADATA_FIELDS = frozenset(
    {"dead_letter_reason", "tasks_stream", "original_entry_id", "delivery_count"}
)


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

    def to_dict(self) -> dict[str, str]:
        """Serialize to flat string dict for Redis stream XADD."""
        return {
            "id": self.id,
            "type": self.type.value,
            "repo": self.repo,
            "token": self.token,
            "claude_token": self.claude_token,
            "resource_type": self.resource_type,
            "resource_id": str(self.resource_id),
            "prompt": self.prompt,
            "branch": self.branch or "",
            "base_branch": self.base_branch or "",
            "key_prefix": self.key_prefix,
            "created_at": self.created_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, str]) -> "Task":
        """Deserialize from Redis stream entry fields."""
        return cls(
            id=data["id"],
            type=TaskType(data["type"]),
            repo=data["repo"],
            token=data["token"],
            claude_token=data.get("claude_token", ""),
            resource_type=data["resource_type"],
            resource_id=int(data["resource_id"]),
            prompt=data["prompt"],
            branch=data["branch"] or None,
            base_branch=data.get("base_branch") or None,
            key_prefix=data.get("key_prefix", ""),
            created_at=datetime.fromisoformat(data["created_at"]),
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
    ) -> "Task":
        """Factory with auto-generated ID and timestamp."""
        return cls(
            id=str(uuid.uuid4()),
            type=task_type,
            repo=repo,
            token=token,
            claude_token=claude_token,
            resource_type=resource_type,
            resource_id=resource_id,
            prompt=prompt,
            branch=branch,
            base_branch=base_branch,
            key_prefix=key_prefix,
            created_at=datetime.now(timezone.utc),
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

    def to_dict(self) -> dict[str, str]:
        """Serialize to flat string dict for Redis stream XADD."""
        return {
            "task_id": self.task_id,
            "worker_id": self.worker_id,
            "status": self.status.value,
            "branch": self.branch or "",
            "summary": self.summary,
            "duration_seconds": str(self.duration_seconds),
            "resource_type": self.resource_type,
            "resource_id": str(self.resource_id),
        }

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
        )
