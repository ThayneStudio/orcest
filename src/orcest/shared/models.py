"""Task and result dataclasses for Redis stream serialization.

All to_dict values are strings (Redis streams require flat {str: str} entries).
Empty string is used as the None sentinel for optional fields.
"""

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum


class TaskType(str, Enum):
    FIX_PR = "fix_pr"
    FIX_CI = "fix_ci"
    CLASSIFY_CI = "classify_ci"  # Phase 2
    IMPLEMENT_ISSUE = "implement_issue"  # Phase 2
    IMPROVE_CODEBASE = "improve"  # Phase 3
    TRIAGE_FOLLOWUPS = "triage_followups"  # Triage unresolved review threads into issues


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
    resource_type: str  # "pr" or "issue"
    resource_id: int  # PR/issue number
    prompt: str  # Full rendered prompt
    branch: str | None  # Existing branch (for PR fixes)
    created_at: datetime

    def to_dict(self) -> dict[str, str]:
        """Serialize to flat string dict for Redis stream XADD."""
        return {
            "id": self.id,
            "type": self.type.value,
            "repo": self.repo,
            "token": self.token,
            "resource_type": self.resource_type,
            "resource_id": str(self.resource_id),
            "prompt": self.prompt,
            "branch": self.branch or "",
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
            resource_type=data["resource_type"],
            resource_id=int(data["resource_id"]),
            prompt=data["prompt"],
            branch=data["branch"] or None,
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
    ) -> "Task":
        """Factory with auto-generated ID and timestamp."""
        return cls(
            id=str(uuid.uuid4()),
            type=task_type,
            repo=repo,
            token=token,
            resource_type=resource_type,
            resource_id=resource_id,
            prompt=prompt,
            branch=branch,
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
