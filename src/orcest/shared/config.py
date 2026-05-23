"""Configuration loader for orchestrator and worker components.

Loads from YAML files with environment variable overrides for secrets
and deployment-specific values.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from orcest.shared.providers import ProviderEntry


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
    claude_tokens: list[str]  # Claude Code OAuth tokens (round-robin pool)
    key_prefix: str  # Redis key prefix for this project
    providers: list[ProviderEntry] = field(default_factory=list)
    # New: multi-provider support. Legacy claude_tokens path synthesizes
    # ProviderEntry objects with rich fields (cli_binary/env_var/extras) left None.

    @property
    def claude_token(self) -> str:
        """First token (backward compat for single-token callers)."""
        return self.claude_tokens[0] if self.claude_tokens else ""


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
    timeout: int = 5400  # 90 minutes
    max_retries: int = 3
    retry_backoff: int = 10  # seconds between retries
    # Optional model the worker passes to the Claude CLI. Empty means no
    # --model flag is passed, so the CLI/account default applies.
    model: str = ""
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
    # Per-SHA task attempts before the retry cadence backs off. Orcest does not
    # abandon the PR or escalate to a human at this threshold -- it only slows
    # down; a new commit resets the counter.
    max_attempts: int = 3
    max_total_attempts: int = 50  # Total attempts across all SHAs before backing off
    max_transient_failures: int = 5  # Transient PR failures before the cadence backs off
    delete_branch_on_merge: bool = True  # Whether to delete the head branch after merging
    # Seconds a pending CI check may be stuck before being re-triggered (default 2 hours)
    stale_pending_timeout_seconds: int = 7200
    # Redis key prefix for the shared task stream. All per-project orchestrators
    # publish to this prefix so workers only need to read from one stream.
    # Defaults to redis.key_prefix for backward compatibility with single-project mode.
    task_key_prefix: str = ""
    providers: list[ProviderEntry] = field(default_factory=list)
    # Top-level providers parsed from YAML (inherited deduped into each project's
    # providers list, or used for the single legacy project in fallback mode).


@dataclass
class WorkerConfig:
    redis: RedisConfig = field(default_factory=RedisConfig)
    worker_id: str = "worker-0"
    workspace_dir: str = "/tmp/orcest-workspaces"
    backend: str = "claude"
    runner: RunnerConfig = field(default_factory=RunnerConfig)
    ephemeral: bool = False  # When True, process one task and exit
    providers: list[ProviderEntry] = field(default_factory=list)
    # Optional provider declarations (for future multi-provider worker support)


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


def _safe_str(value: Any, field_name: str) -> str:
    """Convert a value to str, rejecting None with a clear error message.

    Prevents ``str(None)`` from silently producing the literal string ``"None"``
    when a YAML field is set to ``null``.
    """
    if value is None:
        raise ValueError(
            f"Config field '{field_name}' is explicitly set to null but a string is required."
        )
    return str(value)


def _safe_optional_str(value: Any, field_name: str, default: str = "") -> str:
    """Convert an optional string field, treating YAML null as ``default``."""
    if value is None:
        return default
    return _safe_str(value, field_name)


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


# Provider-specific env var name candidates (for credential fallback when omitted
# from YAML). These are prepended to the generic {PROVIDER}_TOKEN/_API_KEY/_KEY
# fallbacks. Adding a new provider here keeps the logic data-driven/future-proof
# without touching the if-ladder.
_PROVIDER_ENV_CANDIDATES: dict[str, list[str]] = {
    "grok": ["XAI_API_KEY", "GROK_API_KEY", "XAI_API_TOKEN"],
    "claude": ["CLAUDE_CODE_OAUTH_TOKEN", "ANTHROPIC_API_KEY"],
    # codex exec specifically reads CODEX_API_KEY (NOT OPENAI_API_KEY) for
    # headless API auth; OPENAI_API_KEY is kept as a last-resort fallback
    # for env-resolution only (the worker still injects CODEX_API_KEY since
    # that's the recipe env_var).
    "codex": ["CODEX_API_KEY", "OPENAI_API_KEY"],
}


def _parse_provider_entry(raw: dict[str, Any], context: str) -> ProviderEntry:
    """Parse a single provider dict from YAML into a ProviderEntry.

    Supports all ProviderEntry fields (provider, credential, model, cli_binary,
    env_var, extras). Credentials may come from the YAML value or fall back to
    conventional environment variables for mixed YAML+env sources.

    For the legacy claude_tokens path, callers synthesize entries directly with
    rich fields (cli_binary, env_var, extras) explicitly left as None/defaults.
    """
    provider = _safe_str(raw.get("provider", ""), f"{context}.provider").strip()
    if not provider:
        raise ValueError(f"{context} must have a non-empty 'provider' field")

    cred_raw = raw.get("credential")
    if cred_raw is not None and str(cred_raw).strip():
        credential = _safe_str(cred_raw, f"{context}.credential").strip()
    else:
        # Support mixed YAML + env var sources for new providers (e.g. grok via XAI_API_KEY)
        candidates: list[str] = [
            f"{provider.upper()}_TOKEN",
            f"{provider.upper()}_API_KEY",
            f"{provider.upper()}_KEY",
        ]
        specific = _PROVIDER_ENV_CANDIDATES.get(provider)
        if specific:
            candidates = specific + candidates

        credential = ""
        for cand in candidates:
            v = os.environ.get(cand)
            if v and str(v).strip():
                credential = str(v).strip()
                break

        if not credential:
            raise ValueError(
                f"Provider entry at {context} for '{provider}' is missing 'credential' "
                f"(not present in YAML and no matching env var among {candidates[:3]}... was set)"
            )

    model = raw.get("model")
    if model is not None:
        model = _safe_str(model, f"{context}.model").strip() or None

    cli_binary = raw.get("cli_binary")
    if cli_binary is not None:
        cli_binary = _safe_str(cli_binary, f"{context}.cli_binary").strip() or None

    env_var = raw.get("env_var")
    if env_var is not None:
        env_var = _safe_str(env_var, f"{context}.env_var").strip() or None

    extras_raw = raw.get("extras", {}) or {}
    if not isinstance(extras_raw, dict):
        raise ValueError(
            f"{context}.extras must be a YAML mapping, got {type(extras_raw).__name__}"
        )
    extras: dict[str, str] = {}
    for k, v in extras_raw.items():
        extras[_safe_str(k, f"{context}.extras key")] = _safe_str(v, f"{context}.extras value")

    return ProviderEntry(
        provider=provider,
        credential=credential,
        model=model,
        cli_binary=cli_binary,
        env_var=env_var,
        extras=extras,
    )


def _parse_providers_list(raw: Any, context: str) -> list[ProviderEntry]:
    """Validate and parse a providers: list (top-level, per-project, or worker).

    - If raw is not a list (absent, null, or wrong type), returns [] (graceful).
    - Each element must be a dict; otherwise raises ValueError with context like
      "providers[0]" or "projects[0].providers[1]".
    - Duplicates within the list (by .identity()) are dropped; first occurrence wins.
      This implements the documented first-wins dedup policy for duplicate entries
      inside any single providers: list in YAML.
    """
    if not isinstance(raw, list):
        return []
    providers: list[ProviderEntry] = []
    seen: set[str] = set()
    for j, e_raw in enumerate(raw):
        if not isinstance(e_raw, dict):
            raise ValueError(f"{context}[{j}] must be a YAML mapping, got {type(e_raw).__name__}")
        entry = _parse_provider_entry(e_raw, f"{context}[{j}]")
        ident = entry.identity()
        if ident not in seen:
            providers.append(entry)
            seen.add(ident)
    return providers


def _dedup_merge(base: list[ProviderEntry], additions: list[ProviderEntry]) -> list[ProviderEntry]:
    """Return base + additions, skipping any addition whose identity() is already present.

    First occurrence (from base) wins. Used for top-level + per-project merge
    and (via synth) for legacy claude dedup.
    """
    result = list(base)
    existing = {e.identity() for e in result}
    for e in additions:
        ident = e.identity()
        if ident not in existing:
            result.append(e)
            existing.add(ident)
    return result


def _with_legacy_claude_synthesis(
    providers: list[ProviderEntry], claude_tokens: list[str]
) -> list[ProviderEntry]:
    """Append synthesized legacy claude entries (rich fields=None) for tokens not
    already present by identity(). Reuses _dedup_merge so the two branches share
    the synthesis+dedup logic.
    """
    synth: list[ProviderEntry] = [
        ProviderEntry(provider="claude", credential=t, model=None) for t in claude_tokens
    ]
    return _dedup_merge(providers, synth)


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
    key_prefix = _safe_str(
        os.environ.get("ORCEST_REDIS_KEY_PREFIX", redis_raw.get("key_prefix", "orcest")),
        "redis.key_prefix",
    )

    return RedisConfig(
        host=_safe_str(host, "redis.host"),
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

    # Claude tokens: prefer comma-separated CLAUDE_CODE_OAUTH_TOKENS env var,
    # fall back to single CLAUDE_CODE_OAUTH_TOKEN, then YAML.
    claude_tokens_env = os.environ.get("CLAUDE_CODE_OAUTH_TOKENS", "")
    if claude_tokens_env:
        claude_tokens = [t.strip() for t in claude_tokens_env.split(",") if t.strip()]
    else:
        single = os.environ.get(
            "CLAUDE_CODE_OAUTH_TOKEN",
            github_raw.get("claude_token", ""),
        )
        claude_tokens = [single] if single else []

    github_config = GithubConfig(
        token=_safe_str(github_token, "github.token"),
        repo=_safe_str(github_repo, "github.repo"),
        claude_token=_safe_str(claude_tokens[0], "github.claude_token") if claude_tokens else "",
    )

    # Top-level providers list (new style; rich fields from YAML, creds via env fallback).
    # Parsing helper dedups any duplicate entries inside the list (first occurrence wins).
    top_providers = _parse_providers_list(raw.get("providers"), "providers")

    # Multi-project support: load projects list
    projects_raw = raw.get("projects")
    if projects_raw is not None and not isinstance(projects_raw, list):
        raise ValueError(f"'projects' must be a YAML list, got {type(projects_raw).__name__}")
    if isinstance(projects_raw, list) and projects_raw:
        projects = []
        for i, p in enumerate(projects_raw):
            if not isinstance(p, dict):
                raise ValueError(f"projects[{i}] must be a YAML mapping, got {type(p).__name__}")
            # Per-project token list: check claude_tokens (list), claude_token (string), or shared
            p_tokens_raw = p.get("claude_tokens")
            if isinstance(p_tokens_raw, list) and p_tokens_raw:
                p_claude_tokens = [
                    _safe_str(t, f"projects[{i}].claude_tokens[{j}]")
                    for j, t in enumerate(p_tokens_raw)
                    if t
                ]
            elif p.get("claude_token"):
                p_claude_tokens = [_safe_str(p["claude_token"], f"projects[{i}].claude_token")]
            else:
                p_claude_tokens = list(claude_tokens)  # inherit from shared

            # Per-project providers: parsed via helper (dedups intra-list), then
            # dedup-merged with top-level (ensures no dup identity between top and
            # per-project lists), then legacy claude synthesis (also deduped).
            # This centralizes dedup and eliminates prior cross-dup and synth-dup.
            p_providers_raw = p.get("providers")
            per_project_providers = _parse_providers_list(
                p_providers_raw, f"projects[{i}].providers"
            )
            p_providers = _dedup_merge(top_providers, per_project_providers)
            p_providers = _with_legacy_claude_synthesis(p_providers, p_claude_tokens)

            projects.append(
                ProjectConfig(
                    repo=_safe_str(p.get("repo", ""), f"projects[{i}].repo"),
                    # Default to shared token when not set per-project
                    token=_safe_str(p.get("token", github_token), f"projects[{i}].token"),
                    claude_tokens=p_claude_tokens,
                    key_prefix=_safe_str(
                        p.get("key_prefix", redis_config.key_prefix),
                        f"projects[{i}].key_prefix",
                    ),
                    providers=p_providers,
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
        # Top providers (deduped by parse helper) + legacy claude synthesis via shared helper.
        single_providers = _with_legacy_claude_synthesis(top_providers, claude_tokens)

        projects = [
            ProjectConfig(
                repo=_safe_str(github_repo, "github.repo"),
                token=_safe_str(github_token, "github.token"),
                claude_tokens=list(claude_tokens),
                key_prefix=_safe_str(redis_config.key_prefix, "redis.key_prefix"),
                providers=single_providers,
            )
        ]

    # Polling
    polling_raw = _safe_dict(raw, "polling")
    polling_config = PollingConfig(
        interval=_safe_int(polling_raw.get("interval", 60), "polling.interval"),
    )

    # Labels
    labels_raw = {k.replace("-", "_"): v for k, v in _safe_dict(raw, "labels").items()}
    labels_config = LabelConfig(
        blocked=_safe_str(labels_raw.get("blocked", "orcest:blocked"), "labels.blocked"),
        needs_human=_safe_str(
            labels_raw.get("needs_human", "orcest:needs-human"), "labels.needs_human"
        ),
        ready=_safe_str(labels_raw.get("ready", "orcest:ready"), "labels.ready"),
    )

    # Runner config — timeout and max_retries drive the pending-task marker TTL.
    # These should match the values deployed on worker nodes.
    runner_raw = {k.replace("-", "_"): v for k, v in _safe_dict(raw, "runner").items()}
    _runner_defaults = RunnerConfig()
    runner_config = RunnerConfig(
        type=_safe_str(runner_raw.get("type", _runner_defaults.type), "runner.type"),
        timeout=_safe_int(runner_raw.get("timeout", _runner_defaults.timeout), "runner.timeout"),
        max_retries=_safe_int(
            runner_raw.get("max_retries", _runner_defaults.max_retries), "runner.max_retries"
        ),
        retry_backoff=_safe_int(
            runner_raw.get("retry_backoff", _runner_defaults.retry_backoff), "runner.retry_backoff"
        ),
        model=_safe_optional_str(runner_raw.get("model"), "runner.model", _runner_defaults.model),
        extra={
            _safe_str(k, "runner.extra key"): _safe_str(v, f"runner.extra[{k!r}]")
            for k, v in _safe_dict(runner_raw, "extra").items()
        },
    )

    # Default runner backend
    default_runner = _safe_str(
        os.environ.get("ORCEST_DEFAULT_RUNNER", raw.get("default_runner", "claude")),
        "default_runner",
    )

    # Max attempts per PR before labeling needs-human
    max_attempts = _safe_int(raw.get("max_attempts", 3), "max_attempts")

    # Max total attempts across all SHAs (hard stop)
    max_total_attempts = _safe_int(raw.get("max_total_attempts", 50), "max_total_attempts")

    # Max transient worker failures before labeling needs-human.
    max_transient_failures = _safe_int(
        os.environ.get(
            "ORCEST_MAX_TRANSIENT_FAILURES",
            raw.get("max_transient_failures", 5),
        ),
        "max_transient_failures",
    )
    if max_transient_failures <= 0:
        raise ValueError(
            "Config field 'max_transient_failures' must be a positive integer, "
            f"got {max_transient_failures!r}."
        )

    # Whether to delete the head branch after merging
    delete_branch_on_merge = _safe_bool(
        raw.get("delete_branch_on_merge", True), "delete_branch_on_merge"
    )

    # Deployment (CD) config
    deployment_raw = _safe_dict(raw, "deployment")
    deployment_config = DeploymentConfig(
        enabled=_safe_bool(deployment_raw.get("enabled", False), "deployment.enabled"),
        command=_safe_str(
            v if (v := deployment_raw.get("command")) is not None else "",
            "deployment.command",
        ),
        health_check_url=_safe_str(
            v if (v := deployment_raw.get("health_check_url")) is not None else "",
            "deployment.health_check_url",
        ),
        health_check_timeout=_safe_int(
            v if (v := deployment_raw.get("health_check_timeout")) is not None else 30,
            "deployment.health_check_timeout",
        ),
        rollback_command=_safe_str(
            v if (v := deployment_raw.get("rollback_command")) is not None else "",
            "deployment.rollback_command",
        ),
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

    # Shared task stream prefix: all per-project orchestrators publish tasks
    # to this prefix so workers only need to read from one stream.
    task_key_prefix = (
        str(os.environ.get("ORCEST_TASK_KEY_PREFIX", raw.get("task_key_prefix", "")))
        or redis_config.key_prefix
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
        max_transient_failures=max_transient_failures,
        delete_branch_on_merge=delete_branch_on_merge,
        stale_pending_timeout_seconds=stale_pending_timeout_seconds,
        task_key_prefix=task_key_prefix,
        providers=top_providers,
    )

    # Validate required fields
    using_projects_list = isinstance(projects_raw, list) and bool(projects_raw)
    if not using_projects_list:
        # Single-project (legacy) mode: missing repo → point to ORCEST_REPO
        if not github_config.repo:
            raise ValueError(
                "github.repo is required. Set it in the config file or via ORCEST_REPO env var."
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
    worker_id = _safe_str(
        os.environ.get("ORCEST_WORKER_ID", raw.get("worker_id", "worker-0")), "worker_id"
    )
    workspace_dir = _safe_str(
        os.environ.get("ORCEST_WORKSPACE_DIR", raw.get("workspace_dir", "/tmp/orcest-workspaces")),
        "workspace_dir",
    )

    # Runner (construct first so backend can default from runner.type)
    runner_raw = {k.replace("-", "_"): v for k, v in _safe_dict(raw, "runner").items()}
    runner_extra_raw = _safe_dict(runner_raw, "extra")
    _runner_defaults = RunnerConfig()
    runner_config = RunnerConfig(
        type=_safe_str(runner_raw.get("type", _runner_defaults.type), "runner.type"),
        timeout=_safe_int(runner_raw.get("timeout", _runner_defaults.timeout), "runner.timeout"),
        max_retries=_safe_int(
            runner_raw.get("max_retries", _runner_defaults.max_retries), "runner.max_retries"
        ),
        retry_backoff=_safe_int(
            runner_raw.get("retry_backoff", _runner_defaults.retry_backoff), "runner.retry_backoff"
        ),
        model=_safe_optional_str(runner_raw.get("model"), "runner.model", _runner_defaults.model),
        extra={
            _safe_str(k, "runner.extra key"): _safe_str(v, f"runner.extra[{k!r}]")
            for k, v in runner_extra_raw.items()
        },
    )

    # Backend — default from runner.type when not explicitly set
    backend = _safe_str(raw.get("backend", runner_config.type), "backend")

    # Ephemeral mode — process one task and exit (default False)
    ephemeral_raw = raw.get("ephemeral", False)
    ephemeral = _safe_bool(ephemeral_raw, "ephemeral")

    # providers list (new multi-provider support; parsed for completeness).
    # Uses shared helper which dedups intra-list duplicates (first wins).
    worker_providers = _parse_providers_list(raw.get("providers"), "providers")

    config = WorkerConfig(
        redis=redis_config,
        worker_id=worker_id,
        workspace_dir=workspace_dir,
        backend=backend,
        runner=runner_config,
        ephemeral=ephemeral,
        providers=worker_providers,
    )

    # Validate required fields
    if not config.worker_id:
        raise ValueError(
            "worker_id is required. Set it in the config file or via ORCEST_WORKER_ID env var."
        )

    return config
