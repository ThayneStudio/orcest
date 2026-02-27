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


def _safe_int(value: Any, field_name: str) -> int:
    """Convert a value to int with a clear error message on failure.

    Handles the common YAML edge cases: int already, numeric string,
    None, or truly unconvertible values.
    """
    if value is None:
        raise ValueError(f"Config field '{field_name}' is null/missing but an integer is required.")
    try:
        return int(value)
    except (ValueError, TypeError) as exc:
        raise ValueError(
            f"Config field '{field_name}' has value {value!r} which cannot be converted to int."
        ) from exc


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


def _build_redis_config(raw: dict[str, Any]) -> RedisConfig:
    """Build RedisConfig from a raw dict with env var overrides."""
    redis_raw = _safe_dict(raw, "redis")

    host = os.environ.get("ORCEST_REDIS_HOST", redis_raw.get("host", "localhost"))
    port_raw = os.environ.get("ORCEST_REDIS_PORT", redis_raw.get("port", 6379))
    db_raw = redis_raw.get("db", 0)
    # Password comes from env var only -- never stored in YAML
    password = os.environ.get("ORCEST_REDIS_PASSWORD")

    return RedisConfig(
        host=str(host),
        port=_safe_int(port_raw, "redis.port"),
        db=_safe_int(db_raw, "redis.db"),
        password=password,
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
    redis_config = _build_redis_config(raw)

    # GitHub
    github_raw = _safe_dict(raw, "github")
    github_token = os.environ.get("GITHUB_TOKEN", github_raw.get("token", ""))
    github_repo = os.environ.get("ORCEST_REPO", github_raw.get("repo", ""))

    github_config = GithubConfig(
        token=str(github_token),
        repo=str(github_repo),
    )

    # Polling
    polling_raw = _safe_dict(raw, "polling")
    polling_config = PollingConfig(
        interval=_safe_int(polling_raw.get("interval", 60), "polling.interval"),
    )

    # Labels
    labels_raw = _safe_dict(raw, "labels")
    labels_config = LabelConfig(
        queued=str(labels_raw.get("queued", "orcest:queued")),
        in_progress=str(labels_raw.get("in_progress", "orcest:in-progress")),
        blocked=str(labels_raw.get("blocked", "orcest:blocked")),
        needs_human=str(labels_raw.get("needs_human", "orcest:needs-human")),
    )

    config = OrchestratorConfig(
        redis=redis_config,
        github=github_config,
        polling=polling_config,
        labels=labels_config,
    )

    # Validate required fields
    if not config.github.repo:
        raise ValueError(
            "github.repo is required. Set it in the config file or via ORCEST_REPO env var."
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
    redis_config = _build_redis_config(raw)

    # Worker-level fields
    worker_id = str(
        os.environ.get("ORCEST_WORKER_ID", raw.get("worker_id", "worker-0"))
    )
    workspace_dir = str(
        os.environ.get("ORCEST_WORKSPACE_DIR", raw.get("workspace_dir", "/tmp/orcest-workspaces"))
    )

    # Claude
    claude_raw = _safe_dict(raw, "claude")
    claude_config = ClaudeConfig(
        timeout=_safe_int(claude_raw.get("timeout", 1800), "claude.timeout"),
        max_retries=_safe_int(claude_raw.get("max_retries", 3), "claude.max_retries"),
        retry_backoff=_safe_int(claude_raw.get("retry_backoff", 10), "claude.retry_backoff"),
    )

    config = WorkerConfig(
        redis=redis_config,
        worker_id=worker_id,
        workspace_dir=workspace_dir,
        claude=claude_config,
    )

    # Validate required fields
    if not config.worker_id:
        raise ValueError(
            "worker_id is required. Set it in the config file or via ORCEST_WORKER_ID env var."
        )

    return config
