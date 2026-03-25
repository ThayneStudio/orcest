"""Configuration loader for orchestrator and worker components.

Loads from YAML files with environment variable overrides for secrets
and deployment-specific values.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass
class RedisConfig:
    host: str = "localhost"
    port: int = 6379
    db: int = 0
    password: str | None = None
    socket_timeout: int = 30
    socket_connect_timeout: int = 10
    key_prefix: str = "orcest"


@dataclass
class GithubConfig:
    token: str = ""
    repo: str = ""  # "owner/repo" format
    claude_token: str = ""  # Claude Code OAuth token (from org config)


@dataclass
class ProjectConfig:
    """Per-project configuration for the orchestrator."""

    repo: str  # "owner/repo"
    token: str  # GitHub PAT
    claude_token: str  # Claude Code OAuth token
    key_prefix: str  # Redis key prefix for this project


@dataclass
class PollingConfig:
    interval: int = 60  # seconds between poll cycles


@dataclass
class LabelConfig:
    blocked: str = "orcest:blocked"
    needs_human: str = "orcest:needs-human"
    ready: str = "orcest:ready"


@dataclass
class RunnerConfig:
    type: str = "claude"
    timeout: int = 1800  # 30 minutes
    max_retries: int = 3
    retry_backoff: int = 10  # seconds between retries
    extra: dict[str, str] = field(default_factory=dict)


@dataclass
class DeploymentConfig:
    enabled: bool = False
    command: str = ""  # Shell command to run on the orchestrator host after merge
    health_check_url: str = ""  # Optional HTTP endpoint to poll for readiness
    health_check_timeout: int = 30  # Seconds to wait for health check to pass
    rollback_command: str = ""  # Optional command to run if health check fails


@dataclass
class OrchestratorConfig:
    redis: RedisConfig = field(default_factory=RedisConfig)
    github: GithubConfig = field(default_factory=GithubConfig)
    polling: PollingConfig = field(default_factory=PollingConfig)
    labels: LabelConfig = field(default_factory=LabelConfig)
    projects: list[ProjectConfig] = field(default_factory=list)
    deployment: DeploymentConfig = field(default_factory=DeploymentConfig)
    # Runner settings used to compute the pending-task marker TTL.  These
    # should match the timeout/max_retries deployed on worker nodes so that
    # crash-orphaned markers expire no earlier than the actual worst-case
    # runner duration.
    runner: RunnerConfig = field(default_factory=RunnerConfig)
    default_runner: str = "claude"
    max_attempts: int = 3  # Max task attempts per SHA before needs-human
    max_total_attempts: int = 25  # Max total attempts across all SHAs (circuit breaker)
    delete_branch_on_merge: bool = True  # Whether to delete the head branch after merging
    # Seconds a pending CI check may be stuck before being re-triggered (default 2 hours)
    stale_pending_timeout_seconds: int = 7200


@dataclass
class WorkerConfig:
    redis: RedisConfig = field(default_factory=RedisConfig)
    worker_id: str = "worker-0"
    workspace_dir: str = "/tmp/orcest-workspaces"
    backend: str = "claude"
    runner: RunnerConfig = field(default_factory=RunnerConfig)
    ephemeral: bool = False  # When True, process one task and exit
    key_prefixes: list[str] = field(default_factory=list)  # Multi-project prefixes

    def __post_init__(self) -> None:
        if not self.key_prefixes:
            self.key_prefixes = [self.redis.key_prefix]


def _safe_int(value: Any, field_name: str) -> int:
    """Convert a value to int with a clear error message on failure.

    Handles the common YAML edge cases: int already, numeric string,
    None, or truly unconvertible values.
    """
    if value is None:
        raise ValueError(
            f"Config field '{field_name}' is explicitly set to null but an integer is required."
        )
    try:
        return int(value)
    except (ValueError, TypeError) as exc:
        raise ValueError(
            f"Config field '{field_name}' has value {value!r} which cannot be converted to int."
        ) from exc


def _safe_bool(value: Any, field_name: str) -> bool:
    """Validate that a config value is a native bool.

    YAML parses unquoted ``true``/``false`` as Python bools directly.
    If the value is a string (e.g. ``"false"``), it means the user quoted
    it in YAML, which would silently misbehave with a bare ``bool()`` call
    because ``bool("false")`` returns ``True``.  Raise a clear error
    instead so the user can fix their config.
    """
    if value is None:
        raise ValueError(
            f"Config field '{field_name}' is explicitly set to null but a boolean is required."
        )
    if not isinstance(value, bool):
        raise ValueError(
            f"Config field '{field_name}' has value {value!r} which is not a boolean. "
            "Use an unquoted YAML boolean (true or false)."
        )
    return value


def _load_yaml(path: str | Path) -> dict[str, Any]:
    """Load a YAML file and return a dict.

    Returns empty dict if the file does not exist. Raises ValueError
    if the file exists but contains invalid YAML or a non-mapping root.
    """
    path = Path(path)
    if not path.exists():
        return {}
    try:
        with open(path) as f:
            data = yaml.safe_load(f)
    except yaml.YAMLError as exc:
        raise ValueError(f"Failed to parse YAML config file '{path}': {exc}") from exc
    if data is None:
        # Empty file or file with only comments
        return {}
    if not isinstance(data, dict):
        raise ValueError(
            f"Config file '{path}' must contain a YAML mapping at the top level, "
            f"got {type(data).__name__}."
        )
    return data


def _safe_dict(raw: dict[str, Any], key: str) -> dict[str, Any]:
    """Extract a sub-dict from raw config, returning {} if the key is missing or not a dict."""
    value = raw.get(key)
    return value if isinstance(value, dict) else {}


def build_redis_config(raw: dict[str, Any] | None = None) -> RedisConfig:
    """Build RedisConfig from a raw dict with env var overrides.

    Can be called with no arguments to build config purely from
    environment variables (ORCEST_REDIS_HOST, ORCEST_REDIS_PORT,
    ORCEST_REDIS_PASSWORD, ORCEST_REDIS_KEY_PREFIX).
    """
    redis_raw = _safe_dict(raw or {}, "redis")

    host = os.environ.get("ORCEST_REDIS_HOST", redis_raw.get("host", "localhost"))
    port_raw = os.environ.get("ORCEST_REDIS_PORT", redis_raw.get("port", 6379))
    db_raw = redis_raw.get("db", 0)
    # Password comes from env var only -- never stored in YAML
    password = os.environ.get("ORCEST_REDIS_PASSWORD")

    socket_timeout_raw = redis_raw.get("socket_timeout", 30)
    socket_connect_timeout_raw = redis_raw.get("socket_connect_timeout", 10)
    key_prefix = str(
        os.environ.get("ORCEST_REDIS_KEY_PREFIX", redis_raw.get("key_prefix", "orcest"))
    )

    return RedisConfig(
        host=str(host),
        port=_safe_int(port_raw, "redis.port"),
        db=_safe_int(db_raw, "redis.db"),
        password=password,
        socket_timeout=_safe_int(socket_timeout_raw, "redis.socket_timeout"),
        socket_connect_timeout=_safe_int(
            socket_connect_timeout_raw, "redis.socket_connect_timeout"
        ),
        key_prefix=key_prefix,
    )


def load_orchestrator_config(path: str | Path) -> OrchestratorConfig:
    """Load orchestrator config from YAML, with env var overrides.

    Required fields:
        - github.repo must be non-empty (from YAML or ORCEST_REPO env var)

    Raises:
        ValueError: If required fields are missing or empty, if the YAML
            file is malformed, or if numeric fields contain non-numeric values.
    """
    raw = _load_yaml(path)

    # Redis
    redis_config = build_redis_config(raw)

    # GitHub
    github_raw = _safe_dict(raw, "github")
    github_token = os.environ.get("GITHUB_TOKEN", github_raw.get("token", ""))
    github_repo = os.environ.get("ORCEST_REPO", github_raw.get("repo", ""))

    claude_token = os.environ.get(
        "CLAUDE_CODE_OAUTH_TOKEN",
        github_raw.get("claude_token", ""),
    )
    github_config = GithubConfig(
        token=str(github_token),
        repo=str(github_repo),
        claude_token=str(claude_token),
    )

    # Multi-project support: load projects list
    projects_raw = raw.get("projects")
    if projects_raw is not None and not isinstance(projects_raw, list):
        raise ValueError(f"'projects' must be a YAML list, got {type(projects_raw).__name__}")
    if isinstance(projects_raw, list) and projects_raw:
        projects = []
        for i, p in enumerate(projects_raw):
            if not isinstance(p, dict):
                raise ValueError(f"projects[{i}] must be a YAML mapping, got {type(p).__name__}")
            projects.append(
                ProjectConfig(
                    repo=str(p.get("repo", "")),
                    token=str(p.get("token", github_token)),  # default to shared token
                    claude_token=str(p.get("claude_token", claude_token)),  # default to shared
                    key_prefix=str(p.get("key_prefix", redis_config.key_prefix)),
                )
            )
        if len(projects) > 1:
            seen_prefixes: set[str] = set()
            for proj in projects:
                if not proj.key_prefix:
                    raise ValueError(
                        f"projects[].key_prefix is required in multi-project mode "
                        f"(missing for repo '{proj.repo}')"
                    )
                if proj.key_prefix in seen_prefixes:
                    raise ValueError(
                        f"projects[].key_prefix must be unique across projects "
                        f"(duplicate: '{proj.key_prefix}')"
                    )
                seen_prefixes.add(proj.key_prefix)
            repos = [proj.repo for proj in projects]
            if len(set(repos)) != len(repos):
                raise ValueError(
                    "projects[].repo values must be unique "
                    "— duplicate repos would cause double-enqueue"
                )
    else:
        # Backward compatibility: single-project mode
        projects = [
            ProjectConfig(
                repo=str(github_repo),
                token=str(github_token),
                claude_token=str(claude_token),
                key_prefix=str(redis_config.key_prefix),
            )
        ]

    # Polling
    polling_raw = _safe_dict(raw, "polling")
    polling_config = PollingConfig(
        interval=_safe_int(polling_raw.get("interval", 60), "polling.interval"),
    )

    # Labels
    labels_raw = _safe_dict(raw, "labels")
    labels_config = LabelConfig(
        blocked=str(labels_raw.get("blocked", "orcest:blocked")),
        needs_human=str(labels_raw.get("needs_human", "orcest:needs-human")),
        ready=str(labels_raw.get("ready", "orcest:ready")),
    )

    # Runner config — timeout and max_retries drive the pending-task marker TTL.
    # These should match the values deployed on worker nodes.
    runner_raw = _safe_dict(raw, "runner")
    _runner_defaults = RunnerConfig()
    runner_config = RunnerConfig(
        type=str(runner_raw.get("type", _runner_defaults.type)),
        timeout=_safe_int(runner_raw.get("timeout", _runner_defaults.timeout), "runner.timeout"),
        max_retries=_safe_int(
            runner_raw.get("max_retries", _runner_defaults.max_retries), "runner.max_retries"
        ),
        retry_backoff=_safe_int(
            runner_raw.get("retry_backoff", _runner_defaults.retry_backoff), "runner.retry_backoff"
        ),
        extra={str(k): str(v) for k, v in _safe_dict(runner_raw, "extra").items()},
    )

    # Default runner backend
    default_runner = str(
        os.environ.get("ORCEST_DEFAULT_RUNNER", raw.get("default_runner", "claude"))
    )

    # Max attempts per PR before labeling needs-human
    max_attempts = _safe_int(raw.get("max_attempts", 3), "max_attempts")

    # Max total attempts across all SHAs (circuit breaker)
    max_total_attempts = _safe_int(raw.get("max_total_attempts", 10), "max_total_attempts")

    # Whether to delete the head branch after merging
    delete_branch_on_merge = _safe_bool(
        raw.get("delete_branch_on_merge", True), "delete_branch_on_merge"
    )

    # Deployment (CD) config
    deployment_raw = _safe_dict(raw, "deployment")
    deployment_config = DeploymentConfig(
        enabled=_safe_bool(deployment_raw.get("enabled", False), "deployment.enabled"),
        command=str(deployment_raw.get("command", "")),
        health_check_url=str(deployment_raw.get("health_check_url", "")),
        health_check_timeout=_safe_int(
            deployment_raw.get("health_check_timeout", 30), "deployment.health_check_timeout"
        ),
        rollback_command=str(deployment_raw.get("rollback_command", "")),
    )
    if deployment_config.health_check_url and deployment_config.health_check_timeout <= 0:
        raise ValueError(
            f"Config field 'deployment.health_check_timeout' must be a positive integer "
            f"when health_check_url is set, got {deployment_config.health_check_timeout}"
        )

    # Seconds a pending check can be stuck before being re-triggered (default 2 hours)
    stale_pending_timeout_seconds = _safe_int(
        raw.get("stale_pending_timeout_seconds", 7200), "stale_pending_timeout_seconds"
    )
    if stale_pending_timeout_seconds <= 0:
        raise ValueError(
            f"Config field 'stale_pending_timeout_seconds' must be a positive integer, "
            f"got {stale_pending_timeout_seconds!r}."
        )

    config = OrchestratorConfig(
        redis=redis_config,
        github=github_config,
        projects=projects,
        polling=polling_config,
        labels=labels_config,
        deployment=deployment_config,
        runner=runner_config,
        default_runner=default_runner,
        max_attempts=max_attempts,
        max_total_attempts=max_total_attempts,
        delete_branch_on_merge=delete_branch_on_merge,
        stale_pending_timeout_seconds=stale_pending_timeout_seconds,
    )

    # Validate required fields
    using_projects_list = isinstance(projects_raw, list) and bool(projects_raw)
    if not using_projects_list:
        # Single-project (legacy) mode: missing repo → point to ORCEST_REPO
        if not github_config.repo:
            raise ValueError(
                "github.repo is required. "
                "Set it in the config file or via ORCEST_REPO env var."
            )
    else:
        # Multi-project mode: each entry must have a repo field
        empty_repo_entries = [f"projects[{i}]" for i, p in enumerate(projects) if not p.repo]
        if empty_repo_entries:
            raise ValueError(
                f"Each projects[] entry must have a non-empty 'repo' field: "
                f"missing for {', '.join(empty_repo_entries)}."
            )

    return config


def load_worker_config(path: str | Path) -> WorkerConfig:
    """Load worker config from YAML, with env var overrides.

    Required fields:
        - worker_id must be non-empty

    Raises:
        ValueError: If required fields are missing or empty, if the YAML
            file is malformed, or if numeric fields contain non-numeric values.
    """
    raw = _load_yaml(path)

    # Redis
    redis_config = build_redis_config(raw)

    # Worker-level fields
    worker_id = str(os.environ.get("ORCEST_WORKER_ID", raw.get("worker_id", "worker-0")))
    workspace_dir = str(
        os.environ.get("ORCEST_WORKSPACE_DIR", raw.get("workspace_dir", "/tmp/orcest-workspaces"))
    )

    # Runner (construct first so backend can default from runner.type)
    runner_raw = _safe_dict(raw, "runner")
    runner_extra_raw = _safe_dict(runner_raw, "extra")
    _runner_defaults = RunnerConfig()
    runner_config = RunnerConfig(
        type=str(runner_raw.get("type", _runner_defaults.type)),
        timeout=_safe_int(runner_raw.get("timeout", _runner_defaults.timeout), "runner.timeout"),
        max_retries=_safe_int(
            runner_raw.get("max_retries", _runner_defaults.max_retries), "runner.max_retries"
        ),
        retry_backoff=_safe_int(
            runner_raw.get("retry_backoff", _runner_defaults.retry_backoff), "runner.retry_backoff"
        ),
        extra={str(k): str(v) for k, v in runner_extra_raw.items()},
    )

    # Backend — default from runner.type when not explicitly set
    backend = str(raw.get("backend", runner_config.type))

    # Ephemeral mode — process one task and exit (default False)
    ephemeral_raw = raw.get("ephemeral", False)
    ephemeral = _safe_bool(ephemeral_raw, "ephemeral")

    # Multi-project key prefixes: check redis.key_prefixes first, fall back to [redis.key_prefix]
    redis_raw_section = _safe_dict(raw, "redis")
    raw_key_prefixes = redis_raw_section.get("key_prefixes")
    if isinstance(raw_key_prefixes, list) and raw_key_prefixes:
        key_prefixes = [str(p) for p in raw_key_prefixes]
    else:
        key_prefixes = [redis_config.key_prefix]

    config = WorkerConfig(
        redis=redis_config,
        worker_id=worker_id,
        workspace_dir=workspace_dir,
        backend=backend,
        runner=runner_config,
        ephemeral=ephemeral,
        key_prefixes=key_prefixes,
    )

    # Validate required fields
    if not config.worker_id:
        raise ValueError(
            "worker_id is required. Set it in the config file or via ORCEST_WORKER_ID env var."
        )

    return config
