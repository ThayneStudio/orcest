# Phase 1: Core Loop -- Orchestrator + 2 Workers, PR Fixes Only

## Goal

Ship a minimal working system where one orchestrator polls GitHub for PRs with
failing CI or actionable review feedback, enqueues fix tasks to Redis, and two
workers pick up those tasks, run Claude to produce fixes, and report results
back. The target repo is `orcest` itself -- the system manages its own PRs.

By the end of Phase 1, the following loop runs continuously:

```
Orchestrator polls GitHub
  -> discovers PR #42 has failing CI (lint error)
  -> classifies failure as CODE via heuristic
  -> renders prompt with diff + CI logs
  -> publishes task to Redis stream `tasks`
  -> adds `orcest:queued` label, posts comment

Worker-0 picks up task via XREADGROUP
  -> acquires lock `lock:pr:42`
  -> shallow-clones repo, checks out branch
  -> runs `claude --print -p <prompt>`
  -> Claude pushes fix commit
  -> publishes result to Redis stream `results`
  -> releases lock

Orchestrator consumes result
  -> swaps label to `orcest:in-progress` -> removes it
  -> posts completion comment with summary
```

## Scope Boundaries

**In scope:**
- `fix_pr` task type (CI code failures + review feedback)
- Heuristic CI triage (pattern matching, no Claude classification)
- Redis streams for task distribution
- Redis SET NX EX for distributed locking
- GitHub labels and comments for visibility
- Two workers (systemd on bare VMs)
- Orchestrator in Docker Compose

**Out of scope (Phase 2+):**
- Issue processing and `ai-ready` label workflow
- Codebase improvement sweeps
- Claude-based CI classification (`classify_ci` task type)
- Auto-merge (requires branch protection analysis)
- Auto-retry of transient CI failures
- Web dashboard
- Multi-repo support (single repo only)

---

## Module Specifications

### 1. Shared Code (`src/orcest/shared/`)

#### 1.1 `config.py` -- Configuration Loader

Two separate config schemas: one for orchestrator, one for worker. Loaded from
YAML files with environment variable overrides for secrets.

```python
from dataclasses import dataclass, field
from pathlib import Path
import os
import yaml


@dataclass
class RedisConfig:
    host: str = "localhost"
    port: int = 6379
    db: int = 0
    password: str | None = None


@dataclass
class GithubConfig:
    token: str = ""
    repo: str = ""  # "owner/repo" format


@dataclass
class PollingConfig:
    interval: int = 60  # seconds between poll cycles


@dataclass
class LabelConfig:
    queued: str = "orcest:queued"
    in_progress: str = "orcest:in-progress"
    blocked: str = "orcest:blocked"
    needs_human: str = "orcest:needs-human"


@dataclass
class ClaudeConfig:
    timeout: int = 1800  # 30 minutes
    max_retries: int = 3
    retry_backoff: int = 10  # seconds between retries


@dataclass
class OrchestratorConfig:
    redis: RedisConfig = field(default_factory=RedisConfig)
    github: GithubConfig = field(default_factory=GithubConfig)
    polling: PollingConfig = field(default_factory=PollingConfig)
    labels: LabelConfig = field(default_factory=LabelConfig)


@dataclass
class WorkerConfig:
    redis: RedisConfig = field(default_factory=RedisConfig)
    worker_id: str = "worker-0"
    workspace_dir: str = "/tmp/orcest-workspaces"
    claude: ClaudeConfig = field(default_factory=ClaudeConfig)


def load_orchestrator_config(path: str | Path) -> OrchestratorConfig:
    """Load orchestrator config from YAML, with env var overrides."""
    ...


def load_worker_config(path: str | Path) -> WorkerConfig:
    """Load worker config from YAML, with env var overrides."""
    ...
```

**Design decisions:**

- Separate dataclasses per role rather than one monolithic config. Workers
  should not need to know about GitHub tokens or polling intervals.
- Environment variable overrides use a consistent prefix: `ORCEST_REDIS_HOST`,
  `ORCEST_REDIS_PORT`, `GITHUB_TOKEN`, `ORCEST_WORKER_ID`, etc. The `GITHUB_TOKEN`
  env var is special-cased because `gh` CLI already uses it.
- Config loading is a pure function that returns an immutable-ish dataclass.
  No global singletons.
- `RedisConfig.password` comes from `ORCEST_REDIS_PASSWORD` env var only -- never
  stored in the YAML file.

**Env var override rules:**

| Field | Env Var | Notes |
|-------|---------|-------|
| `redis.host` | `ORCEST_REDIS_HOST` | |
| `redis.port` | `ORCEST_REDIS_PORT` | |
| `redis.password` | `ORCEST_REDIS_PASSWORD` | Never in YAML |
| `github.token` | `GITHUB_TOKEN` | Shared with `gh` CLI |
| `github.repo` | `ORCEST_REPO` | |
| `worker_id` | `ORCEST_WORKER_ID` | Overrides YAML for container deploys |
| `workspace_dir` | `ORCEST_WORKSPACE_DIR` | |

**Implementation notes:**

- Use `dacite` or manual dict-to-dataclass mapping. Since we want zero extra
  dependencies beyond what's in `pyproject.toml`, implement manual mapping:
  read YAML dict, overlay env vars, then construct the dataclass tree.
- Validate required fields (e.g., `github.repo` must be non-empty for
  orchestrator, `worker_id` must be non-empty for worker). Raise `ValueError`
  with a clear message on validation failure.

---

#### 1.2 `redis_client.py` -- Redis Connection and Stream Helpers

Thin wrapper around `redis-py` providing connection pooling and typed stream
operations.

```python
import redis


class RedisClient:
    """Redis connection with stream helper methods."""

    def __init__(self, config: RedisConfig):
        self._pool = redis.ConnectionPool(
            host=config.host,
            port=config.port,
            db=config.db,
            password=config.password,
            decode_responses=True,
        )
        self._client = redis.Redis(connection_pool=self._pool)

    @property
    def client(self) -> redis.Redis:
        """Raw redis client for operations not covered by helpers."""
        return self._client

    def health_check(self) -> bool:
        """Returns True if Redis is reachable."""
        try:
            return self._client.ping()
        except redis.ConnectionError:
            return False

    def xadd(self, stream: str, fields: dict[str, str]) -> str:
        """Add entry to stream. Returns the entry ID."""
        return self._client.xadd(stream, fields)

    def xreadgroup(
        self,
        group: str,
        consumer: str,
        stream: str,
        count: int = 1,
        block_ms: int = 5000,
    ) -> list[tuple[str, dict[str, str]]]:
        """
        Read new entries from a consumer group.

        Returns list of (entry_id, fields) tuples.
        Returns empty list on timeout.
        """
        result = self._client.xreadgroup(
            groupname=group,
            consumername=consumer,
            streams={stream: ">"},
            count=count,
            block=block_ms,
        )
        if not result:
            return []
        # result shape: [(stream_name, [(id, fields), ...])]
        return result[0][1]

    def xack(self, stream: str, group: str, entry_id: str) -> int:
        """Acknowledge a stream entry. Returns number acknowledged."""
        return self._client.xack(stream, group, entry_id)

    def ensure_consumer_group(self, stream: str, group: str) -> None:
        """Create consumer group if it doesn't exist.

        Also creates the stream if needed (MKSTREAM).
        """
        try:
            self._client.xgroup_create(
                name=stream, groupname=group, id="0", mkstream=True
            )
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise
```

**Design decisions:**

- `decode_responses=True` so all values come back as `str`, not `bytes`. This
  matches Redis streams' flat string key-value constraint.
- The `xreadgroup` helper returns a simplified `list[tuple[str, dict]]` instead
  of the raw nested structure. Callers don't need to know about the stream
  name dimension since workers read from one stream at a time.
- `ensure_consumer_group` is idempotent -- safe to call on every startup. The
  `BUSYGROUP` error means the group already exists, which is fine.
- Block time of 5000ms (5 seconds) is a reasonable default. Short enough for
  responsive shutdown, long enough to avoid busy-polling.

---

#### 1.3 `coordination.py` -- Distributed Locking

Redis-based distributed lock using `SET NX EX` with owner verification.

```python
import uuid
import threading
import time


class RedisLock:
    """Distributed lock backed by Redis SET NX EX."""

    def __init__(
        self,
        redis_client: RedisClient,
        key: str,
        ttl: int = 1800,  # 30 minutes
        owner: str | None = None,
    ):
        self.redis = redis_client
        self.key = key
        self.ttl = ttl
        self.owner = owner or str(uuid.uuid4())
        self._held = False

    def acquire(self) -> bool:
        """Attempt to acquire the lock. Returns True if successful."""
        result = self.redis.client.set(
            self.key, self.owner, nx=True, ex=self.ttl
        )
        self._held = result is not None
        return self._held

    def release(self) -> bool:
        """Release the lock, but only if we still own it.

        Uses a Lua script for atomic check-and-delete.
        """
        lua_script = """
        if redis.call("GET", KEYS[1]) == ARGV[1] then
            return redis.call("DEL", KEYS[1])
        else
            return 0
        end
        """
        result = self.redis.client.register_script(lua_script)(
            keys=[self.key], args=[self.owner]
        )
        self._held = False
        return result == 1

    def refresh(self) -> bool:
        """Refresh the TTL, but only if we still own it.

        Uses a Lua script for atomic check-and-expire.
        """
        lua_script = """
        if redis.call("GET", KEYS[1]) == ARGV[1] then
            return redis.call("EXPIRE", KEYS[1], ARGV[2])
        else
            return 0
        end
        """
        result = self.redis.client.register_script(lua_script)(
            keys=[self.key], args=[self.owner, str(self.ttl)]
        )
        return result == 1

    @property
    def is_held(self) -> bool:
        return self._held


def make_pr_lock_key(pr_number: int) -> str:
    """Generate the Redis key for a PR lock."""
    return f"lock:pr:{pr_number}"
```

**Design decisions:**

- Lua scripts for release and refresh guarantee atomicity. Without Lua, a
  race between GET and DEL could release another owner's lock.
  `register_script` is used instead of raw `eval` for proper SHA-based
  caching on the Redis server.
- The `owner` field is a UUID by default, but workers should pass their
  `worker_id` for debuggability (you can `GET lock:pr:42` and see which
  worker holds it).
- TTL of 1800s (30 minutes) matches the Claude timeout. If a worker crashes,
  the lock auto-expires and another worker can pick up the task via XPENDING
  redelivery.
- `_held` is a local flag -- it's a hint, not authoritative. The lock could
  expire server-side while `_held` is still True. The heartbeat thread
  prevents this under normal operation.

---

#### 1.4 `models.py` -- Task and Result Dataclasses

```python
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
import uuid


class TaskType(str, Enum):
    FIX_PR = "fix_pr"
    FIX_CI = "fix_ci"
    CLASSIFY_CI = "classify_ci"        # Phase 2
    IMPLEMENT_ISSUE = "implement_issue" # Phase 2
    IMPROVE_CODEBASE = "improve"        # Phase 3


class ResultStatus(str, Enum):
    COMPLETED = "completed"
    FAILED = "failed"
    BLOCKED = "blocked"
    USAGE_EXHAUSTED = "usage_exhausted"


@dataclass
class Task:
    id: str
    type: TaskType
    repo: str                    # "owner/repo"
    token: str                   # GitHub PAT for clone + gh auth
    resource_type: str           # "pr" or "issue"
    resource_id: int             # PR/issue number
    prompt: str                  # Full rendered prompt
    branch: str | None           # Existing branch (for PR fixes)
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

    def to_dict(self) -> dict[str, str]:
        """Serialize to flat string dict for Redis stream XADD."""
        return {
            "task_id": self.task_id,
            "worker_id": self.worker_id,
            "status": self.status.value,
            "branch": self.branch or "",
            "summary": self.summary,
            "duration_seconds": str(self.duration_seconds),
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
        )
```

**Design decisions:**

- All `to_dict` values are strings. Redis streams require flat `{str: str}`
  entries. No nested structures, no bytes.
- `branch` uses empty string `""` as the None sentinel in serialized form.
  Redis streams cannot store `None`.
- `TaskType` and `ResultStatus` inherit from `str` so `.value` returns a
  plain string and they compare naturally with string values.
- The `Task.create()` factory method handles ID generation and timestamps,
  keeping the constructor pure for deserialization.
- The `token` field carries the GitHub PAT inside the task payload. This means
  Redis must be secured (password + network isolation). Phase 2 may move to
  a secrets reference pattern, but for Phase 1 with a single-tenant Redis
  on a private network, inline tokens are acceptable.

---

#### 1.5 `logging.py` -- Structured Logging

```python
import logging
from rich.logging import RichHandler
from rich.console import Console


def setup_logging(
    component: str,
    identifier: str,
    level: str = "INFO",
) -> logging.Logger:
    """
    Configure structured logging with Rich.

    Args:
        component: "orchestrator" or "worker"
        identifier: worker ID or "main" for orchestrator
        level: log level string (DEBUG, INFO, WARNING, ERROR)

    Returns:
        Configured logger instance.
    """
    console = Console(stderr=True)
    handler = RichHandler(
        console=console,
        show_path=False,
        markup=True,
        rich_tracebacks=True,
    )

    # Format includes component and identifier for grep-ability
    fmt = f"[{component}:{identifier}] %(message)s"
    handler.setFormatter(logging.Formatter(fmt))

    logger = logging.getLogger(f"orcest.{component}.{identifier}")
    logger.setLevel(getattr(logging, level.upper()))
    logger.addHandler(handler)

    # Prevent propagation to root logger (avoids duplicate output)
    logger.propagate = False

    return logger
```

**Design decisions:**

- Rich handler writes to stderr, keeping stdout clean for piping.
- Log format includes `[worker:worker-0]` or `[orchestrator:main]` prefix
  so logs from multiple processes can be interleaved and still parsed.
- Each component gets its own named logger (`orcest.worker.worker-0`) to
  avoid cross-contamination in tests.
- Level is configurable via config. Workers default to INFO; set to DEBUG
  for troubleshooting.

---

### 2. Worker (`src/orcest/worker/`)

#### 2.1 `workspace.py` -- Repository Workspace Management

Handles cloning, branch checkout, gh auth configuration, and cleanup.

```python
import subprocess
import tempfile
from pathlib import Path


class Workspace:
    """Manages a temporary repo clone for task execution."""

    def __init__(self, base_dir: str):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self._work_dir: Path | None = None

    @property
    def path(self) -> Path:
        if self._work_dir is None:
            raise RuntimeError("Workspace not initialized. Call setup() first.")
        return self._work_dir

    def setup(self, repo: str, branch: str | None, token: str) -> Path:
        """
        Clone the repo and configure the workspace.

        Args:
            repo: "owner/repo" format
            branch: branch to checkout (None = default branch)
            token: GitHub PAT for clone auth and gh CLI

        Returns:
            Path to the cloned repo directory.
        """
        # Create unique temp directory under base_dir
        self._work_dir = Path(tempfile.mkdtemp(dir=self.base_dir))

        clone_url = f"https://x-access-token:{token}@github.com/{repo}.git"

        # Shallow clone for speed
        cmd = [
            "git", "clone",
            "--depth", "1",
            "--single-branch",
        ]
        if branch:
            cmd.extend(["--branch", branch])

        cmd.extend([clone_url, str(self._work_dir / "repo")])

        subprocess.run(cmd, check=True, capture_output=True, text=True)
        self._work_dir = self._work_dir / "repo"

        # Configure gh CLI auth for this workspace
        # gh uses GITHUB_TOKEN env var, which we'll pass to Claude subprocess
        return self._work_dir

    def cleanup(self) -> None:
        """Remove the workspace directory."""
        if self._work_dir and self._work_dir.exists():
            import shutil
            shutil.rmtree(self._work_dir.parent, ignore_errors=True)
            self._work_dir = None
```

**Design decisions:**

- Shallow clone (`--depth 1 --single-branch`) for speed. Workers don't need
  full history. Claude Code operates on the working tree, not git log.
- Auth via `x-access-token` URL embedding. This is the standard GitHub Apps
  / PAT clone method. The token never touches disk (not in `.git/config`
  after clone since it's in the URL, but the URL is in the remote config).
  Cleanup deletes the entire directory.
- Each task gets its own temp directory. No reuse across tasks -- clean slate
  prevents state leakage.
- The `gh` CLI authenticates via `GITHUB_TOKEN` environment variable, which
  we pass through to the Claude subprocess. No need for `gh auth login`.

**Handling `--depth 1` with PR branches:**

When checking out a PR branch, `--depth 1 --single-branch --branch <branch>`
fetches only that branch's tip. This is sufficient because Claude operates
on the current state of the code, not the commit history. If Claude needs
to see the diff against `main`, the prompt already includes it (rendered by
the orchestrator's `task_publisher`).

---

#### 2.2 `claude_runner.py` -- Claude CLI Subprocess Manager

Executes Claude Code as a subprocess with timeout, retry, and output parsing.

```python
import subprocess
import json
import time
from pathlib import Path
from dataclasses import dataclass


@dataclass
class ClaudeResult:
    """Parsed result from a Claude CLI invocation."""
    success: bool
    summary: str
    duration_seconds: int
    raw_output: str


def run_claude(
    prompt: str,
    work_dir: Path,
    token: str,
    timeout: int = 1800,
    max_retries: int = 3,
    retry_backoff: int = 10,
    logger=None,
) -> ClaudeResult:
    """
    Execute Claude CLI and return parsed result.

    Runs: claude --print --output-format stream-json -p <prompt>

    Args:
        prompt: The full prompt text.
        work_dir: Working directory (cloned repo).
        token: GitHub token (passed as GITHUB_TOKEN env var).
        timeout: Max seconds to wait for Claude.
        max_retries: Number of retry attempts on crash.
        retry_backoff: Seconds between retries.
        logger: Optional logger for status messages.

    Returns:
        ClaudeResult with success flag, summary, and timing.
    """
    import os

    env = os.environ.copy()
    env["GITHUB_TOKEN"] = token

    cmd = [
        "claude",
        "--print",
        "--output-format", "stream-json",
        "-p", prompt,
    ]

    start_time = time.monotonic()

    for attempt in range(1, max_retries + 1):
        try:
            if logger:
                logger.info(f"Claude attempt {attempt}/{max_retries}")

            proc = subprocess.run(
                cmd,
                cwd=work_dir,
                env=env,
                capture_output=True,
                text=True,
                timeout=timeout,
            )

            duration = int(time.monotonic() - start_time)

            if proc.returncode == 0:
                summary = _extract_summary(proc.stdout)
                return ClaudeResult(
                    success=True,
                    summary=summary,
                    duration_seconds=duration,
                    raw_output=proc.stdout,
                )
            else:
                if logger:
                    logger.warning(
                        f"Claude exited with code {proc.returncode}: "
                        f"{proc.stderr[:500]}"
                    )
                # Check for usage exhaustion
                if "usage" in proc.stderr.lower() and "limit" in proc.stderr.lower():
                    return ClaudeResult(
                        success=False,
                        summary="Claude usage limit reached",
                        duration_seconds=duration,
                        raw_output=proc.stderr,
                    )

        except subprocess.TimeoutExpired:
            duration = int(time.monotonic() - start_time)
            if logger:
                logger.error(f"Claude timed out after {timeout}s")
            return ClaudeResult(
                success=False,
                summary=f"Timed out after {timeout}s",
                duration_seconds=duration,
                raw_output="",
            )

        if attempt < max_retries:
            if logger:
                logger.info(f"Retrying in {retry_backoff}s...")
            time.sleep(retry_backoff)

    duration = int(time.monotonic() - start_time)
    return ClaudeResult(
        success=False,
        summary=f"Failed after {max_retries} attempts",
        duration_seconds=duration,
        raw_output="",
    )


def _extract_summary(stream_json_output: str) -> str:
    """
    Extract a human-readable summary from Claude's stream-json output.

    The stream-json format emits one JSON object per line. We look for
    the final 'result' message which contains the assistant's summary.
    Falls back to last N characters of text content if no result found.
    """
    lines = stream_json_output.strip().splitlines()
    last_text = ""

    for line in lines:
        try:
            obj = json.loads(line)
            # stream-json has 'type' field: 'text', 'result', etc.
            if obj.get("type") == "result":
                return obj.get("result", "")[:500]
            if obj.get("type") == "text":
                last_text = obj.get("text", "")
        except json.JSONDecodeError:
            continue

    # Fallback: return last chunk of text output
    return last_text[:500] if last_text else "No summary available"
```

**Design decisions:**

- `claude --print --output-format stream-json` is the non-interactive mode.
  `--print` means Claude won't prompt for confirmation. `stream-json` gives
  us structured output we can parse for the result summary.
- Timeout uses `subprocess.run(timeout=...)` which sends SIGKILL on expiry.
  This is intentionally aggressive -- a hung Claude process should not block
  the worker indefinitely.
- Retry logic is simple: on non-zero exit (crash), wait `retry_backoff`
  seconds and try again. Timeouts are NOT retried (they suggest the task
  is genuinely too large). Usage exhaustion is NOT retried (needs cooldown).
- The `GITHUB_TOKEN` env var is passed through so Claude Code can use `gh`
  commands within the repo's `.claude/` hooks and skills.
- Summary extraction parses stream-json for the `result` message type. This
  gets the final assistant response. Truncated to 500 chars for the
  `TaskResult.summary` field.

**Important: `--print` vs interactive mode.**

Claude Code with `--print` runs in non-interactive (headless) mode. It will:
- Read `.claude/CLAUDE.md` and `.claude/` config from the repo
- Execute the prompt
- Make tool calls (file edits, bash commands, git operations) autonomously
- Exit when done

It will NOT prompt for user confirmation. This is critical for unattended
worker operation.

---

#### 2.3 `heartbeat.py` -- Lock TTL Refresh

Background thread that keeps the Redis lock alive while Claude is running.

```python
import threading
import time


class Heartbeat:
    """Background thread that refreshes a Redis lock's TTL."""

    def __init__(
        self,
        lock: "RedisLock",
        interval: float | None = None,
        logger=None,
    ):
        """
        Args:
            lock: The RedisLock to keep alive.
            interval: Refresh interval in seconds.
                      Defaults to lock.ttl / 3.
            logger: Optional logger.
        """
        self.lock = lock
        self.interval = interval or (lock.ttl / 3)
        self.logger = logger
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Start the heartbeat thread."""
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            daemon=True,
            name=f"heartbeat-{self.lock.key}",
        )
        self._thread.start()

    def stop(self) -> None:
        """Signal the heartbeat thread to stop and wait for it."""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=self.interval + 1)
            self._thread = None

    def _run(self) -> None:
        """Heartbeat loop: refresh TTL until stopped."""
        while not self._stop_event.is_set():
            self._stop_event.wait(timeout=self.interval)
            if self._stop_event.is_set():
                break
            refreshed = self.lock.refresh()
            if self.logger:
                if refreshed:
                    self.logger.debug(f"Heartbeat: refreshed {self.lock.key}")
                else:
                    self.logger.warning(
                        f"Heartbeat: failed to refresh {self.lock.key} "
                        f"(lock lost?)"
                    )
```

**Design decisions:**

- Daemon thread so it doesn't prevent process exit if the main thread crashes.
- Interval defaults to `ttl / 3` (10 minutes for 30-minute TTL). This gives
  two missed heartbeats before the lock expires -- enough margin for GC pauses
  or transient Redis latency.
- Uses `threading.Event.wait()` instead of `time.sleep()` so the thread
  responds immediately to `stop()` rather than sleeping through it.
- If `refresh()` returns False, the lock was lost (expired or stolen). The
  heartbeat logs a warning but continues running -- the worker's main loop
  should check lock status after Claude completes.

---

#### 2.4 `loop.py` -- Worker Main Loop

The central worker loop: block on Redis stream, acquire lock, run Claude,
publish result.

```python
import signal
import sys
import time
from orcest.shared.config import WorkerConfig, load_worker_config
from orcest.shared.redis_client import RedisClient
from orcest.shared.coordination import RedisLock, make_pr_lock_key
from orcest.shared.models import Task, TaskResult, ResultStatus
from orcest.shared.logging import setup_logging
from orcest.worker.workspace import Workspace
from orcest.worker.claude_runner import run_claude, ClaudeResult
from orcest.worker.heartbeat import Heartbeat

TASKS_STREAM = "tasks"
RESULTS_STREAM = "results"
CONSUMER_GROUP = "workers"


def run_worker(config: WorkerConfig) -> None:
    """Main worker entry point. Blocks indefinitely."""
    logger = setup_logging("worker", config.worker_id)
    redis = RedisClient(config.redis)

    # Verify Redis connection
    if not redis.health_check():
        logger.error("Cannot connect to Redis. Exiting.")
        sys.exit(1)

    # Ensure consumer group exists
    redis.ensure_consumer_group(TASKS_STREAM, CONSUMER_GROUP)

    # Graceful shutdown
    shutdown = False

    def handle_signal(signum, frame):
        nonlocal shutdown
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        shutdown = True

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    workspace = Workspace(config.workspace_dir)

    logger.info(f"Worker {config.worker_id} started. Waiting for tasks...")

    while not shutdown:
        # Block waiting for tasks (5 second timeout to check shutdown flag)
        entries = redis.xreadgroup(
            group=CONSUMER_GROUP,
            consumer=config.worker_id,
            stream=TASKS_STREAM,
            count=1,
            block_ms=5000,
        )

        if not entries:
            continue  # Timeout, loop back to check shutdown

        entry_id, fields = entries[0]
        task = Task.from_dict(fields)

        logger.info(
            f"Received task {task.id}: {task.type.value} "
            f"for {task.resource_type} #{task.resource_id}"
        )

        # Try to acquire lock
        lock_key = make_pr_lock_key(task.resource_id)
        lock = RedisLock(
            redis, lock_key, ttl=config.claude.timeout + 60,
            owner=config.worker_id,
        )

        if not lock.acquire():
            logger.warning(
                f"Lock {lock_key} already held, skipping task {task.id}"
            )
            # ACK the message so it's not redelivered to us
            # (another worker has the lock and presumably the same task)
            redis.xack(TASKS_STREAM, CONSUMER_GROUP, entry_id)
            continue

        logger.info(f"Acquired lock {lock_key}")

        # Start heartbeat
        heartbeat = Heartbeat(lock, logger=logger)
        heartbeat.start()

        result = _execute_task(task, config, workspace, logger)

        # Stop heartbeat and release lock
        heartbeat.stop()
        lock.release()
        logger.info(f"Released lock {lock_key}")

        # Publish result
        redis.xadd(RESULTS_STREAM, result.to_dict())
        logger.info(
            f"Published result for task {task.id}: {result.status.value}"
        )

        # ACK the task message
        redis.xack(TASKS_STREAM, CONSUMER_GROUP, entry_id)

    logger.info("Worker shut down cleanly.")


def _execute_task(
    task: Task,
    config: WorkerConfig,
    workspace: Workspace,
    logger,
) -> TaskResult:
    """Execute a single task: clone, run Claude, return result."""
    start = time.monotonic()

    try:
        # Setup workspace
        logger.info(f"Cloning {task.repo} (branch: {task.branch or 'default'})")
        work_dir = workspace.setup(task.repo, task.branch, task.token)

        # Run Claude
        claude_result: ClaudeResult = run_claude(
            prompt=task.prompt,
            work_dir=work_dir,
            token=task.token,
            timeout=config.claude.timeout,
            max_retries=config.claude.max_retries,
            logger=logger,
        )

        duration = int(time.monotonic() - start)

        if claude_result.success:
            status = ResultStatus.COMPLETED
        elif "usage limit" in claude_result.summary.lower():
            status = ResultStatus.USAGE_EXHAUSTED
        else:
            status = ResultStatus.FAILED

        return TaskResult(
            task_id=task.id,
            worker_id=config.worker_id,
            status=status,
            branch=task.branch,
            summary=claude_result.summary,
            duration_seconds=duration,
        )

    except Exception as e:
        duration = int(time.monotonic() - start)
        logger.error(f"Task execution failed: {e}", exc_info=True)
        return TaskResult(
            task_id=task.id,
            worker_id=config.worker_id,
            status=ResultStatus.FAILED,
            branch=task.branch,
            summary=f"Worker exception: {e}",
            duration_seconds=duration,
        )

    finally:
        workspace.cleanup()
```

**Design decisions:**

- Lock TTL is `claude.timeout + 60` -- one minute buffer beyond the Claude
  timeout. The heartbeat keeps it alive during normal operation; the buffer
  protects against a slow cleanup after timeout.
- Lock owner is the `worker_id` for debuggability. You can `redis-cli GET
  lock:pr:42` and see `worker-0`.
- When a worker receives a task but can't acquire the lock, it ACKs the
  message and moves on. This prevents the message from being redelivered
  repeatedly. The assumption is that another worker already holds the lock
  and is working on the same PR.
- Graceful shutdown via signal handlers. The `shutdown` flag is checked on
  every loop iteration. The 5-second XREADGROUP block timeout ensures the
  flag is checked promptly.
- Workspace cleanup happens in `finally` to ensure temp dirs are removed
  even on exceptions.

---

### 3. Orchestrator (`src/orcest/orchestrator/`)

#### 3.1 `gh.py` -- GitHub CLI Wrapper

All GitHub interaction goes through the `gh` CLI. No direct API calls.

```python
import subprocess
import json


def _run_gh(args: list[str], token: str) -> str:
    """
    Execute a gh CLI command and return stdout.

    Raises subprocess.CalledProcessError on non-zero exit.
    """
    import os
    env = os.environ.copy()
    env["GITHUB_TOKEN"] = token
    env["GH_TOKEN"] = token  # gh CLI also checks GH_TOKEN

    result = subprocess.run(
        ["gh"] + args,
        capture_output=True,
        text=True,
        env=env,
        check=True,
    )
    return result.stdout.strip()


def list_open_prs(repo: str, token: str) -> list[dict]:
    """
    List all open PRs, sorted oldest first.

    Returns list of dicts with keys: number, title, headRefName,
    author, createdAt, labels, reviewDecision.
    """
    output = _run_gh([
        "pr", "list",
        "--repo", repo,
        "--state", "open",
        "--json", "number,title,headRefName,author,createdAt,"
                  "labels,reviewDecision",
        "--limit", "100",
    ], token)
    return json.loads(output) if output else []


def get_pr(repo: str, number: int, token: str) -> dict:
    """Get detailed PR info."""
    output = _run_gh([
        "pr", "view", str(number),
        "--repo", repo,
        "--json", "number,title,body,headRefName,baseRefName,state,"
                  "author,labels,reviewDecision,reviews,"
                  "statusCheckRollup,commits,additions,deletions",
    ], token)
    return json.loads(output)


def get_ci_status(repo: str, pr_number: int, token: str) -> list[dict]:
    """
    Get CI check runs for a PR.

    Returns list of dicts with: name, status, conclusion, detailsUrl.
    """
    pr = get_pr(repo, pr_number, token)
    checks = pr.get("statusCheckRollup", [])
    return checks


def get_pr_diff(repo: str, number: int, token: str) -> str:
    """Get the diff for a PR."""
    return _run_gh([
        "pr", "diff", str(number),
        "--repo", repo,
    ], token)


def get_check_run_logs(
    repo: str, run_id: int, token: str
) -> str:
    """
    Get logs for a specific check run.

    Uses gh api to fetch the logs URL, then downloads.
    """
    output = _run_gh([
        "api",
        f"repos/{repo}/actions/runs/{run_id}/logs",
    ], token)
    return output


def add_label(repo: str, number: int, label: str, token: str) -> None:
    """Add a label to a PR/issue."""
    _run_gh([
        "pr", "edit", str(number),
        "--repo", repo,
        "--add-label", label,
    ], token)


def remove_label(repo: str, number: int, label: str, token: str) -> None:
    """Remove a label from a PR/issue. Silently succeeds if not present."""
    try:
        _run_gh([
            "pr", "edit", str(number),
            "--repo", repo,
            "--remove-label", label,
        ], token)
    except subprocess.CalledProcessError:
        pass  # Label wasn't present


def post_comment(repo: str, number: int, body: str, token: str) -> None:
    """Post a comment on a PR/issue."""
    _run_gh([
        "pr", "comment", str(number),
        "--repo", repo,
        "--body", body,
    ], token)


def get_review_comments(repo: str, number: int, token: str) -> list[dict]:
    """Get review comments on a PR."""
    output = _run_gh([
        "api",
        f"repos/{repo}/pulls/{number}/reviews",
    ], token)
    return json.loads(output) if output else []
```

**Design decisions:**

- Every function takes `token` as a parameter rather than reading it from
  the environment. This makes testing straightforward and keeps the dependency
  explicit.
- `_run_gh` sets both `GITHUB_TOKEN` and `GH_TOKEN` for compatibility across
  `gh` CLI versions.
- `remove_label` swallows errors. If the label isn't present, `gh pr edit
  --remove-label` exits non-zero. We don't care.
- `list_open_prs` includes labels in the JSON output so the orchestrator can
  filter out PRs that already have `orcest:queued` or `orcest:in-progress`
  labels without additional API calls.
- `--limit 100` on PR listing is sufficient for Phase 1 (single repo, likely
  <20 concurrent PRs).

---

#### 3.2 `pr_ops.py` -- PR Discovery and State Management

```python
from dataclasses import dataclass
from enum import Enum
from orcest.orchestrator import gh
from orcest.shared.coordination import RedisLock, make_pr_lock_key
from orcest.shared.redis_client import RedisClient


class PRAction(str, Enum):
    """What the orchestrator should do with a PR."""
    ENQUEUE_FIX = "enqueue_fix"      # CI failing or review feedback
    SKIP_LOCKED = "skip_locked"      # Another worker already on it
    SKIP_LABELED = "skip_labeled"    # Already queued/in-progress
    SKIP_GREEN = "skip_green"        # CI passing, nothing to do
    SKIP_DRAFT = "skip_draft"        # Draft PR, ignore


@dataclass
class PRState:
    """Analyzed state of a PR."""
    number: int
    title: str
    branch: str
    action: PRAction
    ci_failures: list[dict]          # Failed check runs
    review_comments: list[dict]      # Actionable review comments
    labels: list[str]


def discover_actionable_prs(
    repo: str,
    token: str,
    redis: RedisClient,
    label_config: "LabelConfig",
) -> list[PRState]:
    """
    Discover PRs that need action.

    Filters:
    1. Skip PRs with orcest labels (already being handled)
    2. Skip PRs with active Redis locks (worker in progress)
    3. Identify PRs with CI failures or review feedback
    """
    prs = gh.list_open_prs(repo, token)
    results = []

    orcest_labels = {
        label_config.queued,
        label_config.in_progress,
        label_config.blocked,
        label_config.needs_human,
    }

    for pr_data in prs:
        number = pr_data["number"]
        title = pr_data["title"]
        branch = pr_data["headRefName"]
        pr_labels = [l["name"] for l in pr_data.get("labels", [])]

        # Skip if already labeled by orcest
        if any(label in orcest_labels for label in pr_labels):
            results.append(PRState(
                number=number, title=title, branch=branch,
                action=PRAction.SKIP_LABELED,
                ci_failures=[], review_comments=[], labels=pr_labels,
            ))
            continue

        # Skip if locked in Redis
        lock_key = make_pr_lock_key(number)
        if redis.client.exists(lock_key):
            results.append(PRState(
                number=number, title=title, branch=branch,
                action=PRAction.SKIP_LOCKED,
                ci_failures=[], review_comments=[], labels=pr_labels,
            ))
            continue

        # Check CI status
        checks = gh.get_ci_status(repo, number, token)
        ci_failures = [
            c for c in checks
            if c.get("conclusion") == "failure"
        ]

        # Check review state
        review_decision = pr_data.get("reviewDecision", "")
        has_actionable_reviews = review_decision == "CHANGES_REQUESTED"

        if ci_failures or has_actionable_reviews:
            results.append(PRState(
                number=number, title=title, branch=branch,
                action=PRAction.ENQUEUE_FIX,
                ci_failures=ci_failures,
                review_comments=[],  # Populated by task_publisher
                labels=pr_labels,
            ))
        else:
            results.append(PRState(
                number=number, title=title, branch=branch,
                action=PRAction.SKIP_GREEN,
                ci_failures=[], review_comments=[], labels=pr_labels,
            ))

    return results
```

**Design decisions:**

- The `discover_actionable_prs` function is the orchestrator's "eyes". It
  returns a list of `PRState` objects with a recommended `action`. The main
  loop acts on these recommendations.
- Label check is done first (cheapest). Lock check second (Redis round-trip).
  CI status check last (GitHub API call per PR).
- `PRState` carries enough context for the task publisher to render a prompt
  without making additional API calls.

---

#### 3.3 `ci_triage.py` -- CI Failure Classification

Heuristic pattern matching to classify CI failures without Claude.

```python
from enum import Enum
import re


class CIFailureType(str, Enum):
    """Classification of a CI failure."""
    TRANSIENT = "transient"     # Network timeout, flaky test
    CODE = "code"               # Lint error, test failure, type error
    DEPENDENCY = "dependency"   # Pip/npm install failure
    UNKNOWN = "unknown"         # Needs Claude classification (Phase 2)


# Pattern -> classification mapping
# Patterns are matched against check run names and log snippets
TRANSIENT_PATTERNS = [
    r"timeout",
    r"ETIMEDOUT",
    r"connection reset",
    r"502 bad gateway",
    r"503 service unavailable",
    r"rate limit",
    r"socket hang up",
    r"ECONNREFUSED",
]

CODE_PATTERNS = [
    r"ruff.*error",
    r"lint.*fail",
    r"mypy.*error",
    r"pytest.*FAILED",
    r"test.*fail",
    r"AssertionError",
    r"SyntaxError",
    r"TypeError",
    r"NameError",
    r"ImportError",
    r"ModuleNotFoundError",
    r"IndentationError",
    r"AttributeError",
    r"compilation failed",
    r"type.?check.*fail",
]

DEPENDENCY_PATTERNS = [
    r"Could not find a version that satisfies",
    r"No matching distribution found",
    r"npm ERR!.*404",
    r"ERESOLVE",
    r"dependency resolution failed",
    r"version conflict",
    r"incompatible",
]


def classify_ci_failure(
    check_name: str,
    logs: str = "",
) -> CIFailureType:
    """
    Classify a CI failure using heuristic pattern matching.

    Args:
        check_name: Name of the failed check run.
        logs: Log output from the check run (may be empty).

    Returns:
        CIFailureType classification.
    """
    text = f"{check_name}\n{logs}".lower()

    for pattern in TRANSIENT_PATTERNS:
        if re.search(pattern, text, re.IGNORECASE):
            return CIFailureType.TRANSIENT

    for pattern in DEPENDENCY_PATTERNS:
        if re.search(pattern, text, re.IGNORECASE):
            return CIFailureType.DEPENDENCY

    for pattern in CODE_PATTERNS:
        if re.search(pattern, text, re.IGNORECASE):
            return CIFailureType.CODE

    return CIFailureType.UNKNOWN
```

**Design decisions:**

- Pattern matching order: transient first (cheapest to handle -- just retry),
  then dependency (needs a different fix strategy), then code (most common).
  Unknown is the fallback.
- Patterns are intentionally broad. False positives are acceptable in Phase 1
  because the worst case is Claude getting a slightly mis-classified prompt.
  Phase 2 adds Claude-based classification for `UNKNOWN` cases.
- Logs may be empty if the GitHub API doesn't expose them easily. The check
  run name alone is often sufficient (e.g., "Ruff" or "pytest").
- In Phase 1, `TRANSIENT` and `DEPENDENCY` both fall through to `UNKNOWN`
  for task enqueueing since we don't handle auto-retry or dependency
  resolution yet. The classification is still performed and logged for
  future use.

---

#### 3.4 `task_publisher.py` -- Task Creation and Enqueueing

Renders prompts from context and publishes tasks to Redis.

```python
from orcest.shared.models import Task, TaskType
from orcest.shared.redis_client import RedisClient
from orcest.orchestrator import gh
from orcest.orchestrator.pr_ops import PRState
from orcest.orchestrator.ci_triage import classify_ci_failure, CIFailureType


TASKS_STREAM = "tasks"


def publish_fix_task(
    pr_state: PRState,
    repo: str,
    token: str,
    redis: RedisClient,
    label_config: "LabelConfig",
    logger=None,
) -> Task:
    """
    Create and publish a fix task for a PR.

    1. Gather context (diff, CI logs, review comments)
    2. Classify CI failures
    3. Render prompt
    4. Publish to Redis stream
    5. Add label and post comment on PR
    """
    # Gather context
    diff = gh.get_pr_diff(repo, pr_state.number, token)

    # Classify CI failures
    failure_summaries = []
    task_type = TaskType.FIX_PR

    for check in pr_state.ci_failures:
        classification = classify_ci_failure(
            check.get("name", ""),
            "",  # logs -- Phase 1 may not fetch full logs
        )
        failure_summaries.append({
            "name": check.get("name", "unknown"),
            "classification": classification.value,
            "details_url": check.get("detailsUrl", ""),
        })

        if classification == CIFailureType.CODE:
            task_type = TaskType.FIX_CI

    # Get review comments if review-driven
    review_summary = ""
    if not pr_state.ci_failures:
        reviews = gh.get_review_comments(repo, pr_state.number, token)
        review_summary = _format_reviews(reviews)

    # Render prompt
    prompt = _render_fix_prompt(
        pr_number=pr_state.number,
        pr_title=pr_state.title,
        branch=pr_state.branch,
        diff=diff,
        ci_failures=failure_summaries,
        review_summary=review_summary,
    )

    # Create task
    task = Task.create(
        task_type=task_type,
        repo=repo,
        token=token,
        resource_type="pr",
        resource_id=pr_state.number,
        prompt=prompt,
        branch=pr_state.branch,
    )

    # Publish to Redis stream
    redis.xadd(TASKS_STREAM, task.to_dict())

    # Update GitHub visibility
    gh.add_label(repo, pr_state.number, label_config.queued, token)
    gh.post_comment(
        repo, pr_state.number,
        f"**orcest** queued task `{task.id}` ({task_type.value}) "
        f"for this PR.",
        token,
    )

    if logger:
        logger.info(
            f"Published {task_type.value} task {task.id} "
            f"for PR #{pr_state.number}"
        )

    return task


def _render_fix_prompt(
    pr_number: int,
    pr_title: str,
    branch: str,
    diff: str,
    ci_failures: list[dict],
    review_summary: str,
) -> str:
    """
    Render the prompt that Claude will receive.

    Uses simple string formatting (no Jinja2 dependency).
    """
    sections = [
        f"# Fix PR #{pr_number}: {pr_title}",
        "",
        f"You are on branch `{branch}`.",
        "Your task is to fix the issues described below, commit your "
        "changes, and push to this branch.",
        "",
    ]

    if ci_failures:
        sections.append("## CI Failures")
        sections.append("")
        for f in ci_failures:
            sections.append(
                f"- **{f['name']}** ({f['classification']})"
            )
            if f.get("details_url"):
                sections.append(f"  Details: {f['details_url']}")
        sections.append("")
        sections.append(
            "Fix the CI failures listed above. Read the error "
            "messages carefully and make targeted fixes."
        )
        sections.append("")

    if review_summary:
        sections.append("## Review Feedback")
        sections.append("")
        sections.append(review_summary)
        sections.append("")
        sections.append("Address all review feedback above.")
        sections.append("")

    sections.extend([
        "## Current Diff (against base branch)",
        "",
        "```diff",
        diff[:10000],  # Truncate very large diffs
        "```",
        "",
        "## Instructions",
        "",
        "1. Read the CI failure details and/or review feedback carefully.",
        "2. Make the minimal changes needed to fix the issues.",
        "3. Run the project's linter/tests to verify your fix.",
        "4. Commit your changes with a descriptive message.",
        "5. Push to the branch.",
        "",
        "Do NOT create new PRs. Push to the existing branch.",
    ])

    return "\n".join(sections)


def _format_reviews(reviews: list[dict]) -> str:
    """Format review comments into a readable summary."""
    if not reviews:
        return ""

    lines = []
    for review in reviews:
        state = review.get("state", "")
        body = review.get("body", "").strip()
        author = review.get("user", {}).get("login", "unknown")

        if state == "CHANGES_REQUESTED" and body:
            lines.append(f"**{author}** requested changes:")
            lines.append(body)
            lines.append("")

    return "\n".join(lines)
```

**Design decisions:**

- Prompt rendering uses simple string formatting, not Jinja2. The prompts
  are straightforward enough that template engines add complexity without
  benefit. Phase 2 may add Jinja2 for more sophisticated prompt templates.
- Diff is truncated to 10,000 characters. Very large diffs should be
  summarized, but for Phase 1, truncation is acceptable. Claude will see
  the actual files when it runs in the repo anyway.
- The task publisher handles both GitHub visibility (labels + comments) and
  Redis publication. This keeps the orchestrator loop clean.
- CI log fetching is deferred in Phase 1. The check run name and details URL
  are included in the prompt so Claude can look them up if needed.

---

#### 3.5 `loop.py` -- Orchestrator Main Loop

```python
import signal
import sys
import time
from orcest.shared.config import OrchestratorConfig, load_orchestrator_config
from orcest.shared.redis_client import RedisClient
from orcest.shared.models import TaskResult
from orcest.shared.logging import setup_logging
from orcest.orchestrator.pr_ops import discover_actionable_prs, PRAction
from orcest.orchestrator.task_publisher import publish_fix_task
from orcest.orchestrator import gh

RESULTS_STREAM = "results"
RESULTS_GROUP = "orchestrator"


def run_orchestrator(config: OrchestratorConfig) -> None:
    """Main orchestrator entry point. Polls GitHub in a loop."""
    logger = setup_logging("orchestrator", "main")
    redis = RedisClient(config.redis)

    # Verify Redis connection
    if not redis.health_check():
        logger.error("Cannot connect to Redis. Exiting.")
        sys.exit(1)

    # Ensure consumer group for results stream
    redis.ensure_consumer_group(RESULTS_STREAM, RESULTS_GROUP)

    # Graceful shutdown
    shutdown = False

    def handle_signal(signum, frame):
        nonlocal shutdown
        logger.info(
            f"Received signal {signum}, shutting down gracefully..."
        )
        shutdown = True

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    logger.info(
        f"Orchestrator started. Repo: {config.github.repo}, "
        f"poll interval: {config.polling.interval}s"
    )

    while not shutdown:
        try:
            _poll_cycle(config, redis, logger)
        except Exception as e:
            logger.error(f"Poll cycle failed: {e}", exc_info=True)
            # Continue after error -- don't crash the loop

        # Wait for next cycle (interruptible)
        for _ in range(config.polling.interval):
            if shutdown:
                break
            time.sleep(1)

    logger.info("Orchestrator shut down cleanly.")


def _poll_cycle(
    config: OrchestratorConfig,
    redis: RedisClient,
    logger,
) -> None:
    """Single orchestrator poll cycle."""

    # Step 1: Consume results from workers
    _consume_results(config, redis, logger)

    # Step 2: Discover PRs needing action
    pr_states = discover_actionable_prs(
        repo=config.github.repo,
        token=config.github.token,
        redis=redis,
        label_config=config.labels,
    )

    # Step 3: Enqueue tasks for actionable PRs
    for pr_state in pr_states:
        if pr_state.action == PRAction.ENQUEUE_FIX:
            logger.info(
                f"PR #{pr_state.number} ({pr_state.title}): "
                f"enqueueing fix task"
            )
            publish_fix_task(
                pr_state=pr_state,
                repo=config.github.repo,
                token=config.github.token,
                redis=redis,
                label_config=config.labels,
                logger=logger,
            )
        elif pr_state.action == PRAction.SKIP_GREEN:
            logger.debug(
                f"PR #{pr_state.number}: CI green, skipping"
            )
        elif pr_state.action == PRAction.SKIP_LOCKED:
            logger.debug(
                f"PR #{pr_state.number}: locked, skipping"
            )
        elif pr_state.action == PRAction.SKIP_LABELED:
            logger.debug(
                f"PR #{pr_state.number}: already labeled, skipping"
            )

    logger.info(
        f"Poll cycle complete. "
        f"{sum(1 for p in pr_states if p.action == PRAction.ENQUEUE_FIX)} "
        f"tasks enqueued, {len(pr_states)} PRs checked."
    )


def _consume_results(
    config: OrchestratorConfig,
    redis: RedisClient,
    logger,
) -> None:
    """
    Consume any pending results from workers.

    Non-blocking: reads all available results without waiting.
    """
    while True:
        entries = redis.xreadgroup(
            group=RESULTS_GROUP,
            consumer="orchestrator-main",
            stream=RESULTS_STREAM,
            count=10,
            block_ms=0,  # Non-blocking
        )

        if not entries:
            break

        for entry_id, fields in entries:
            result = TaskResult.from_dict(fields)
            _handle_result(config, redis, result, logger)
            redis.xack(RESULTS_STREAM, RESULTS_GROUP, entry_id)


def _handle_result(
    config: OrchestratorConfig,
    redis: RedisClient,
    result: TaskResult,
    logger,
) -> None:
    """Process a single task result."""
    logger.info(
        f"Result for task {result.task_id}: {result.status.value} "
        f"(worker: {result.worker_id}, {result.duration_seconds}s)"
    )

    # Format result comment for the PR.
    # Note: posting requires the PR number. See "Open Design Question"
    # section below for the resource_id refinement.
    if result.status.value == "completed":
        body = (
            f"**orcest** task `{result.task_id}` completed "
            f"({result.duration_seconds}s, "
            f"worker: {result.worker_id}).\n\n"
            f"Summary: {result.summary}"
        )
    elif result.status.value == "failed":
        body = (
            f"**orcest** task `{result.task_id}` failed "
            f"({result.duration_seconds}s, "
            f"worker: {result.worker_id}).\n\n"
            f"Summary: {result.summary}"
        )
    elif result.status.value == "usage_exhausted":
        body = (
            f"**orcest** task `{result.task_id}` paused "
            f"(Claude usage limit reached, "
            f"worker: {result.worker_id}).\n\n"
            f"Work saved on branch `{result.branch}`. "
            f"Will resume when capacity is available."
        )
    else:
        body = (
            f"**orcest** task `{result.task_id}`: "
            f"{result.status.value}"
        )

    logger.info(f"Result comment: {body[:100]}...")
```

**Open design question -- result-to-PR mapping:**

The `TaskResult` currently does not include the PR number. The orchestrator
needs this to post comments and manage labels. Options:

1. **Add `resource_id` to `TaskResult`** (recommended): Simple, explicit.
   Workers already have this from the `Task`.
2. **Maintain a `task_id -> pr_number` mapping in Redis**: More complex,
   adds another Redis key to manage.
3. **Parse it from the task stream**: Expensive, fragile.

Phase 1 implementation should go with option 1: add `resource_id: int` and
`resource_type: str` fields to `TaskResult`. This is noted as a refinement
to apply during implementation.

---

### 4. CLI (`src/orcest/cli.py`)

Wire up the existing Click commands to the actual implementations.

```python
"""CLI entry point for orcest."""

import click
from rich.console import Console
from rich.table import Table


@click.group()
def main():
    """Orcest: Autonomous CI/CD orchestration system."""


@main.command()
@click.option(
    "--config", default="config/orchestrator.yaml",
    help="Path to orchestrator config.",
)
def orchestrate(config):
    """Start the orchestrator loop."""
    from orcest.shared.config import load_orchestrator_config
    from orcest.orchestrator.loop import run_orchestrator

    cfg = load_orchestrator_config(config)
    run_orchestrator(cfg)


@main.command()
@click.option(
    "--id", "worker_id", required=True,
    help="Unique worker identifier.",
)
@click.option(
    "--config", default="config/worker.yaml",
    help="Path to worker config.",
)
def work(worker_id, config):
    """Start a worker loop."""
    from orcest.shared.config import load_worker_config
    from orcest.worker.loop import run_worker

    cfg = load_worker_config(config)
    # Override worker_id from CLI flag
    cfg.worker_id = worker_id
    run_worker(cfg)


@main.command()
@click.option(
    "--config", default="config/orchestrator.yaml",
    help="Config file (for Redis connection).",
)
def status(config):
    """Show system status: workers, queue depth, active tasks."""
    from orcest.shared.config import load_orchestrator_config
    from orcest.shared.redis_client import RedisClient

    cfg = load_orchestrator_config(config)
    redis = RedisClient(cfg.redis)

    if not redis.health_check():
        click.echo("Error: Cannot connect to Redis.", err=True)
        raise SystemExit(1)

    console = Console()
    client = redis.client

    # Queue depth
    tasks_len = client.xlen("tasks") or 0
    results_len = client.xlen("results") or 0

    # Active locks
    lock_keys = list(client.scan_iter(match="lock:pr:*"))
    locks = []
    for key in lock_keys:
        owner = client.get(key)
        ttl = client.ttl(key)
        pr_num = key.split(":")[-1]
        locks.append({"pr": pr_num, "owner": owner, "ttl": ttl})

    # Consumer group info
    try:
        groups = client.xinfo_groups("tasks")
    except Exception:
        groups = []

    # Display
    console.print("\n[bold]Orcest System Status[/bold]\n")

    table = Table(title="Queue Depths")
    table.add_column("Stream", style="cyan")
    table.add_column("Pending", style="yellow")
    table.add_row("tasks", str(tasks_len))
    table.add_row("results", str(results_len))
    console.print(table)

    if locks:
        lock_table = Table(title="Active Locks")
        lock_table.add_column("PR", style="cyan")
        lock_table.add_column("Owner", style="green")
        lock_table.add_column("TTL (s)", style="yellow")
        for lock in locks:
            lock_table.add_row(
                lock["pr"], lock["owner"], str(lock["ttl"])
            )
        console.print(lock_table)
    else:
        console.print("[dim]No active locks.[/dim]")

    if groups:
        group_table = Table(title="Consumer Groups")
        group_table.add_column("Group", style="cyan")
        group_table.add_column("Consumers", style="green")
        group_table.add_column("Pending", style="yellow")
        for g in groups:
            group_table.add_row(
                g["name"], str(g["consumers"]), str(g["pending"])
            )
        console.print(group_table)

    console.print()
```

---

### 5. GitHub Visibility

#### Labels

Create these labels in the target repository (manual or via `gh label create`):

| Label | Color | Description |
|-------|-------|-------------|
| `orcest:queued` | `#0E8A16` (green) | Task queued for worker pickup |
| `orcest:in-progress` | `#FBCA04` (yellow) | Worker actively processing |
| `orcest:blocked` | `#D93F0B` (red) | Blocked on dependency or conflict |
| `orcest:needs-human` | `#B60205` (dark red) | Requires human intervention |

**Label lifecycle for a PR fix:**

```
PR has failing CI
  -> orchestrator adds `orcest:queued`
  -> worker picks up task, orchestrator swaps to `orcest:in-progress`
  -> worker completes
     -> success: remove `orcest:in-progress`
     -> failure: swap to `orcest:needs-human`
     -> usage_exhausted: keep `orcest:in-progress` (will resume)
```

#### Comments

Standard comment formats:

**Task queued:**
```
**orcest** queued task `<task-id>` (fix_ci) for this PR.
```

**Task completed:**
```
**orcest** task `<task-id>` completed (347s, worker: worker-0).

Summary: Fixed ruff lint error in src/orcest/shared/config.py -- missing
type annotation on `load_config` return value.
```

**Task failed / needs human:**
```
**orcest** task `<task-id>` failed after 3 attempts (worker: worker-1).

Summary: Unable to resolve failing test in test_coordination.py. The test
expects a specific Redis error message that differs between Redis 6 and 7.

Labeling as `orcest:needs-human` for manual review.
```

---

### 6. Config Files

#### `config/orchestrator.example.yaml`

Already exists. No changes needed for Phase 1. The existing schema matches
the `OrchestratorConfig` dataclass defined above.

#### `config/worker.example.yaml`

Already exists. No changes needed for Phase 1. The existing schema matches
the `WorkerConfig` dataclass defined above.

---

### 7. Redis Streams Design

#### Stream: `tasks`

- **Publisher:** Orchestrator (via `task_publisher.py`)
- **Consumer group:** `workers`
- **Consumers:** `worker-0`, `worker-1`, etc.
- **Message format:** Flat `dict[str, str]` from `Task.to_dict()`
- **Delivery:** Each message delivered to exactly one consumer (via `XREADGROUP`)

#### Stream: `results`

- **Publisher:** Workers (via `worker/loop.py`)
- **Consumer group:** `orchestrator`
- **Consumer:** `orchestrator-main`
- **Message format:** Flat `dict[str, str]` from `TaskResult.to_dict()`
- **Delivery:** Single consumer (orchestrator)

#### Consumer Group Initialization

Both orchestrator and workers call `ensure_consumer_group()` on startup.
This is idempotent -- safe if the group already exists.

```
XGROUP CREATE tasks workers 0 MKSTREAM
XGROUP CREATE results orchestrator 0 MKSTREAM
```

The `MKSTREAM` flag creates the stream if it doesn't exist, avoiding a
chicken-and-egg problem on first boot.

#### Message Lifecycle

```
1. Orchestrator: XADD tasks * field1 val1 field2 val2 ...
2. Worker:       XREADGROUP GROUP workers worker-0 COUNT 1 BLOCK 5000 STREAMS tasks >
3. Worker:       (processes task)
4. Worker:       XADD results * field1 val1 field2 val2 ...
5. Worker:       XACK tasks workers <entry-id>
6. Orchestrator: XREADGROUP GROUP orchestrator orchestrator-main COUNT 10 BLOCK 0 STREAMS results >
7. Orchestrator: (processes result)
8. Orchestrator: XACK results orchestrator <entry-id>
```

#### Failure Recovery

If a worker crashes mid-task:
- The `tasks` stream entry remains pending (unACKed) in the consumer group
- The Redis lock TTL expires after 30 minutes
- On next startup (or via manual intervention), `XPENDING` + `XCLAIM` can
  reassign the message to another consumer

Phase 1 does NOT implement automatic XPENDING/XCLAIM recovery. A crashed
worker's task will sit in pending state until manually resolved or the
worker restarts. This is acceptable for Phase 1 with only 2 workers.

---

### 8. Implementation Order

Each step builds on the previous. Estimated effort in parentheses.

#### Step 1: Foundation (Day 1)

**Files:** `src/orcest/shared/config.py`, `src/orcest/shared/redis_client.py`

- Implement `OrchestratorConfig`, `WorkerConfig`, `RedisConfig` dataclasses
- YAML loading with env var overrides
- Redis connection pool and stream helpers
- Write unit tests: config loading, env var overrides, Redis health check

**Verification:** `pytest tests/test_config.py tests/test_redis_client.py`
(Redis tests need a running Redis, use `docker-compose up redis`)

#### Step 2: Task Model + Locking (Day 1)

**Files:** `src/orcest/shared/models.py`, `src/orcest/shared/coordination.py`

- `Task` and `TaskResult` dataclasses with serialization
- `RedisLock` with acquire/release/refresh
- Write unit tests: round-trip serialization, lock acquire/release, lock
  contention, owner verification

**Verification:** `pytest tests/test_models.py tests/test_coordination.py`

#### Step 3: Structured Logging (Day 1)

**Files:** `src/orcest/shared/logging.py`

- Rich-based structured logging setup
- Component/identifier prefixes
- Quick smoke test: visual verification of log output format

**Verification:** Manual -- run a script that calls `setup_logging` and
emits messages at different levels.

#### Step 4: Worker Core (Day 2)

**Files:** `src/orcest/worker/workspace.py`, `src/orcest/worker/claude_runner.py`

- Workspace: clone, checkout, cleanup
- Claude runner: subprocess execution, timeout, retry, output parsing
- Unit tests: workspace setup/cleanup (mock git), Claude runner with mock
  subprocess

**Verification:** `pytest tests/test_workspace.py tests/test_claude_runner.py`
and manual test: clone a real repo, run Claude on a simple prompt.

#### Step 5: Worker Loop (Day 2)

**Files:** `src/orcest/worker/heartbeat.py`, `src/orcest/worker/loop.py`

- Heartbeat background thread
- Main XREADGROUP loop with lock/unlock/result publishing
- Integration test: publish a mock task to Redis, verify worker picks it
  up and publishes a result

**Verification:** Manual integration test with Redis + mock Claude.

#### Step 6: GitHub Interaction (Day 3)

**Files:** `src/orcest/orchestrator/gh.py`, `src/orcest/orchestrator/pr_ops.py`

- `gh` CLI wrapper functions
- PR discovery and state classification
- Unit tests with mocked subprocess calls

**Verification:** `pytest tests/test_gh.py tests/test_pr_ops.py` and manual
test against the real orcest repo.

#### Step 7: CI Triage (Day 3)

**Files:** `src/orcest/orchestrator/ci_triage.py`

- Pattern matching implementation
- Test with sample CI failure names/logs

**Verification:** `pytest tests/test_ci_triage.py` with a matrix of known
failure patterns.

#### Step 8: Orchestrator Loop (Day 3-4)

**Files:** `src/orcest/orchestrator/task_publisher.py`, `src/orcest/orchestrator/loop.py`

- Prompt rendering
- Task publication with GitHub label/comment updates
- Main poll loop: discover, enqueue, consume results
- Integration test: full cycle with mock GitHub responses

**Verification:** Manual end-to-end: create a PR with a deliberate lint error,
run the orchestrator, verify it enqueues a task.

#### Step 9: CLI Wiring + Integration (Day 4)

**Files:** `src/orcest/cli.py`, integration tests

- Wire `orchestrate`, `work`, `status` commands to implementations
- End-to-end test: `docker-compose up` with orchestrator + Redis, run
  `orcest work --id worker-0` on the host, create a broken PR, watch it
  get fixed

**Verification:** Full loop: broken PR -> task enqueued -> worker fixes ->
result consumed -> comment posted.

#### Step 10: Docker Compose + Deployment (Day 5)

**Files:** `docker-compose.yml`, `Dockerfile`, deployment docs

- Update Docker Compose for orchestrator (already scaffolded)
- Worker deployment via systemd on bare VMs (manual for Phase 1)
- Smoke test: full system running for 1 hour with a deliberately broken PR

**Verification:** System runs unattended, processes at least one PR fix
successfully.

---

### 9. Testing Strategy

#### Unit Tests

| Module | Test File | What's Tested |
|--------|-----------|---------------|
| `shared/config.py` | `tests/test_config.py` | YAML loading, env var overrides, validation |
| `shared/models.py` | `tests/test_models.py` | Serialization round-trips, edge cases |
| `shared/coordination.py` | `tests/test_coordination.py` | Lock acquire/release, owner verification, refresh |
| `shared/redis_client.py` | `tests/test_redis_client.py` | Stream helpers, consumer group creation |
| `worker/workspace.py` | `tests/test_workspace.py` | Clone/checkout/cleanup (mocked git) |
| `worker/claude_runner.py` | `tests/test_claude_runner.py` | Subprocess handling, timeout, retry, parsing |
| `orchestrator/gh.py` | `tests/test_gh.py` | CLI wrapper (mocked subprocess) |
| `orchestrator/ci_triage.py` | `tests/test_ci_triage.py` | Pattern classification matrix |
| `orchestrator/pr_ops.py` | `tests/test_pr_ops.py` | PR filtering logic |

#### Integration Tests

Require a running Redis instance (`docker-compose up redis`).

1. **Task round-trip:** Publish task -> XREADGROUP -> deserialize -> verify
2. **Lock lifecycle:** Acquire -> heartbeat refresh -> release -> verify freed
3. **Worker loop:** Publish mock task -> worker processes -> result appears
4. **Orchestrator loop:** Mock GitHub responses -> verify tasks enqueued

#### End-to-End Test (Manual)

1. Start Redis via `docker-compose up redis`
2. Start orchestrator: `orcest orchestrate --config config/orchestrator.yaml`
3. Start worker: `orcest work --id worker-0 --config config/worker.yaml`
4. Create a PR with a deliberate `ruff` lint error
5. Wait for orchestrator to discover it (~60s)
6. Watch worker pick up the task
7. Verify Claude fixes the lint error and pushes
8. Verify orchestrator posts completion comment
9. Check `orcest status` shows clean state

---

### 10. Deployment Architecture (Phase 1)

```
+--------------------------------------------------+
|  thayne-claude-dev-01.home.prefixa.net           |
|  (Docker Compose)                                |
|                                                  |
|  +--------------+    +-------------------+       |
|  |    Redis     |    |   Orchestrator    |       |
|  |  (container) |<---|   (container)     |       |
|  |  port 6379   |    |                   |       |
|  +------+-------+    +-------------------+       |
|         |                                        |
+---------|----------------------------------------+
          | Redis TCP (private network)
     +----+----+
     |         |
+----v----+ +--v------+
|Worker-0 | |Worker-1  |
|(bare VM)| |(bare VM) |
|systemd  | |systemd   |
|Claude   | |Claude    |
+---------+ +----------+
```

- Orchestrator + Redis run in Docker Compose on the dev server.
- Workers run on bare VMs via systemd. They need `claude` CLI installed
  and authenticated, `gh` CLI installed, and network access to Redis.
- Workers connect to Redis via the private network. Redis is NOT exposed
  to the public internet.

---

### 11. Known Limitations (Phase 1)

1. **No XPENDING recovery.** If a worker crashes, its task sits in pending
   state until the worker restarts or manual `XCLAIM` is performed.

2. **No auto-retry of transient CI failures.** The heuristic classifies them,
   but the orchestrator doesn't call `gh run rerun` yet.

3. **No auto-merge.** Even when CI is green and reviews are clean, the
   orchestrator does not merge PRs in Phase 1.

4. **Single repo only.** The orchestrator config points to one repo. Multi-repo
   support is Phase 3+.

5. **No Claude-based CI classification.** The `UNKNOWN` classification from
   heuristics is logged but not escalated to Claude.

6. **Token in task payload.** The GitHub PAT is sent through Redis as part of
   the task. Redis must be on a trusted network. Phase 2 should move to a
   secrets manager or short-lived token pattern.

7. **No rate limiting.** The orchestrator does not track GitHub API rate limits.
   With a 60-second poll interval and a single repo, this is unlikely to be
   an issue, but it's not handled gracefully.

8. **Result-to-PR mapping needs refinement.** The `TaskResult` should carry
   `resource_type` and `resource_id` so the orchestrator can post comments
   to the right PR. This is a known TODO noted in the orchestrator loop
   section.

---

### 12. Success Criteria

Phase 1 is complete when:

- [ ] `orcest orchestrate` runs continuously, polling GitHub every 60 seconds
- [ ] `orcest work --id worker-0` blocks on Redis, picks up tasks, runs Claude
- [ ] A PR with a deliberate lint error gets automatically fixed within 5 minutes
- [ ] A PR with review feedback ("CHANGES_REQUESTED") gets a fix attempt
- [ ] Labels are correctly applied and removed throughout the lifecycle
- [ ] Comments are posted on PRs for task start and completion
- [ ] `orcest status` displays queue depth, active locks, and consumer groups
- [ ] Two workers can run simultaneously without conflicting on the same PR
- [ ] Graceful shutdown on SIGTERM (no orphaned locks, no lost tasks)
- [ ] The system can run unattended for 24 hours without crashing
