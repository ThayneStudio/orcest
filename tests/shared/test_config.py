"""Unit tests for orchestrator and worker config loading."""

from pathlib import Path

import pytest

from orcest.shared.config import (
    ProjectConfig,
    RunnerConfig,
    build_redis_config,
    load_orchestrator_config,
    load_worker_config,
)

# ---------------------------------------------------------------------------
# Env vars that config.py reads -- we must ensure they are unset in every
# test so the ambient shell environment cannot leak into assertions.
# ---------------------------------------------------------------------------
_ENV_VARS_TO_CLEAR = [
    "ORCEST_REDIS_HOST",
    "ORCEST_REDIS_PORT",
    "ORCEST_REDIS_PASSWORD",
    "ORCEST_REDIS_KEY_PREFIX",
    "GITHUB_TOKEN",
    "ORCEST_REPO",
    "ORCEST_DEFAULT_RUNNER",
    "ORCEST_WORKER_ID",
    "ORCEST_WORKSPACE_DIR",
    "CLAUDE_CODE_OAUTH_TOKEN",
]


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    """Remove all config-related env vars before each test."""
    for var in _ENV_VARS_TO_CLEAR:
        monkeypatch.delenv(var, raising=False)


# -- Orchestrator -----------------------------------------------------------


def test_load_orchestrator_config_from_yaml(tmp_path: Path):
    cfg_file = tmp_path / "orcest.yaml"
    cfg_file.write_text(
        "redis:\n"
        "  host: redis.example.com\n"
        "  port: 6380\n"
        "  db: 2\n"
        "github:\n"
        "  token: ghp_yaml_token\n"
        "  repo: acme/widgets\n"
        "polling:\n"
        "  interval: 30\n"
        "labels:\n"
        "  blocked: custom:blocked\n"
    )

    config = load_orchestrator_config(cfg_file)

    assert config.redis.host == "redis.example.com"
    assert config.redis.port == 6380
    assert config.redis.db == 2
    assert config.github.token == "ghp_yaml_token"
    assert config.github.repo == "acme/widgets"
    assert config.polling.interval == 30
    assert config.labels.blocked == "custom:blocked"
    # Non-overridden label keeps its default
    assert config.labels.needs_human == "orcest:needs-human"


def test_load_orchestrator_config_env_overrides(
    tmp_path: Path,
    monkeypatch,
):
    cfg_file = tmp_path / "orcest.yaml"
    cfg_file.write_text(
        "redis:\n  host: yaml-host\ngithub:\n  token: ghp_yaml_token\n  repo: yaml/repo\n"
    )

    monkeypatch.setenv("ORCEST_REDIS_HOST", "env-host")
    monkeypatch.setenv("ORCEST_REPO", "env/repo")
    monkeypatch.setenv("GITHUB_TOKEN", "ghp_env_token")

    config = load_orchestrator_config(cfg_file)

    assert config.redis.host == "env-host"
    assert config.github.repo == "env/repo"
    assert config.github.token == "ghp_env_token"


def test_load_worker_config_from_yaml(tmp_path: Path):
    cfg_file = tmp_path / "worker.yaml"
    cfg_file.write_text(
        "redis:\n"
        "  host: redis.internal\n"
        "  port: 6381\n"
        "worker_id: worker-5\n"
        "workspace_dir: /data/workspaces\n"
        "backend: noop\n"
        "runner:\n"
        "  type: noop\n"
        "  timeout: 900\n"
        "  max_retries: 5\n"
        "  retry_backoff: 20\n"
    )

    config = load_worker_config(cfg_file)

    assert config.redis.host == "redis.internal"
    assert config.redis.port == 6381
    assert config.worker_id == "worker-5"
    assert config.workspace_dir == "/data/workspaces"
    assert config.backend == "noop"
    assert config.runner.type == "noop"
    assert config.runner.timeout == 900
    assert config.runner.max_retries == 5
    assert config.runner.retry_backoff == 20


def test_load_worker_config_env_overrides(
    tmp_path: Path,
    monkeypatch,
):
    cfg_file = tmp_path / "worker.yaml"
    cfg_file.write_text("worker_id: yaml-worker\nworkspace_dir: /yaml/path\n")

    monkeypatch.setenv("ORCEST_WORKER_ID", "env-worker")
    monkeypatch.setenv("ORCEST_WORKSPACE_DIR", "/env/path")

    config = load_worker_config(cfg_file)

    assert config.worker_id == "env-worker"
    assert config.workspace_dir == "/env/path"


def test_load_orchestrator_config_invalid_yaml(tmp_path: Path):
    cfg_file = tmp_path / "bad.yaml"
    cfg_file.write_text("[ invalid")

    with pytest.raises(ValueError, match="Failed to parse YAML"):
        load_orchestrator_config(cfg_file)


def test_load_orchestrator_config_missing_file(tmp_path: Path):
    missing = tmp_path / "does_not_exist.yaml"

    # _load_yaml returns {} for missing files, but load_orchestrator_config
    # then validates that github.repo is non-empty and raises ValueError.
    with pytest.raises(ValueError, match="github.repo is required"):
        load_orchestrator_config(missing)


def test_delete_branch_on_merge_defaults_to_true(tmp_path: Path):
    cfg_file = tmp_path / "orcest.yaml"
    cfg_file.write_text("github:\n  repo: acme/widgets\n")

    config = load_orchestrator_config(cfg_file)

    assert config.delete_branch_on_merge is True


def test_delete_branch_on_merge_false_from_yaml(tmp_path: Path):
    cfg_file = tmp_path / "orcest.yaml"
    cfg_file.write_text("github:\n  repo: acme/widgets\ndelete_branch_on_merge: false\n")

    config = load_orchestrator_config(cfg_file)

    assert config.delete_branch_on_merge is False


def test_delete_branch_on_merge_quoted_string_raises(tmp_path: Path):
    cfg_file = tmp_path / "orcest.yaml"
    cfg_file.write_text('github:\n  repo: acme/widgets\ndelete_branch_on_merge: "false"\n')

    with pytest.raises(ValueError, match="delete_branch_on_merge"):
        load_orchestrator_config(cfg_file)


def test_delete_branch_on_merge_null_raises(tmp_path: Path):
    cfg_file = tmp_path / "orcest.yaml"
    cfg_file.write_text("github:\n  repo: acme/widgets\ndelete_branch_on_merge: null\n")

    with pytest.raises(ValueError, match="explicitly set to null"):
        load_orchestrator_config(cfg_file)


def test_load_orchestrator_config_runner_defaults(tmp_path: Path):
    """OrchestratorConfig.runner uses RunnerConfig defaults when not specified."""
    cfg_file = tmp_path / "orcest.yaml"
    cfg_file.write_text("github:\n  repo: acme/widgets\n")

    config = load_orchestrator_config(cfg_file)

    defaults = RunnerConfig()
    assert config.runner.timeout == defaults.timeout
    assert config.runner.max_retries == defaults.max_retries


def test_load_orchestrator_config_runner_from_yaml(tmp_path: Path):
    """OrchestratorConfig.runner reflects values from the YAML runner section."""
    cfg_file = tmp_path / "orcest.yaml"
    cfg_file.write_text(
        "github:\n  repo: acme/widgets\nrunner:\n  timeout: 3600\n  max_retries: 5\n"
    )

    config = load_orchestrator_config(cfg_file)

    assert config.runner.timeout == 3600
    assert config.runner.max_retries == 5


# -- Redis socket timeout defaults ------------------------------------------


def test_redis_socket_timeout_defaults_orchestrator(tmp_path: Path):
    """RedisConfig gets default socket timeouts when not set in YAML."""
    cfg_file = tmp_path / "orcest.yaml"
    cfg_file.write_text("github:\n  repo: acme/widgets\n")

    config = load_orchestrator_config(cfg_file)

    assert config.redis.socket_timeout == 30
    assert config.redis.socket_connect_timeout == 10


def test_redis_socket_timeout_from_yaml_orchestrator(tmp_path: Path):
    """RedisConfig reads custom socket timeouts from the YAML redis section."""
    cfg_file = tmp_path / "orcest.yaml"
    cfg_file.write_text(
        "redis:\n  socket_timeout: 5\n  socket_connect_timeout: 3\ngithub:\n  repo: acme/widgets\n"
    )

    config = load_orchestrator_config(cfg_file)

    assert config.redis.socket_timeout == 5
    assert config.redis.socket_connect_timeout == 3


def test_redis_socket_timeout_defaults_worker(tmp_path: Path):
    """WorkerConfig gets default socket timeouts when not set in YAML."""
    cfg_file = tmp_path / "worker.yaml"
    cfg_file.write_text("worker_id: worker-0\n")

    config = load_worker_config(cfg_file)

    assert config.redis.socket_timeout == 30
    assert config.redis.socket_connect_timeout == 10


def test_redis_socket_timeout_from_yaml_worker(tmp_path: Path):
    """WorkerConfig reads custom socket timeouts from the YAML redis section."""
    cfg_file = tmp_path / "worker.yaml"
    cfg_file.write_text(
        "redis:\n  socket_timeout: 15\n  socket_connect_timeout: 7\nworker_id: worker-0\n"
    )

    config = load_worker_config(cfg_file)

    assert config.redis.socket_timeout == 15
    assert config.redis.socket_connect_timeout == 7


# -- build_redis_config (standalone) ----------------------------------------


def test_build_redis_config_no_args():
    """build_redis_config() with no arguments returns defaults."""
    cfg = build_redis_config()

    assert cfg.host == "localhost"
    assert cfg.port == 6379
    assert cfg.db == 0
    assert cfg.password is None
    assert cfg.key_prefix == "orcest"


def test_build_redis_config_none_arg():
    """build_redis_config(None) behaves the same as no arguments."""
    cfg = build_redis_config(None)

    assert cfg.host == "localhost"
    assert cfg.port == 6379
    assert cfg.key_prefix == "orcest"


def test_build_redis_config_from_raw_dict():
    """build_redis_config reads values from a raw dict's 'redis' sub-key."""
    raw = {
        "redis": {
            "host": "redis.internal",
            "port": 6380,
            "db": 3,
            "key_prefix": "myproject",
        }
    }
    cfg = build_redis_config(raw)

    assert cfg.host == "redis.internal"
    assert cfg.port == 6380
    assert cfg.db == 3
    assert cfg.key_prefix == "myproject"


def test_build_redis_config_env_vars_override_raw(monkeypatch):
    """Environment variables take precedence over raw dict values."""
    raw = {
        "redis": {
            "host": "yaml-host",
            "port": 6380,
            "key_prefix": "yaml-prefix",
        }
    }
    monkeypatch.setenv("ORCEST_REDIS_HOST", "env-host")
    monkeypatch.setenv("ORCEST_REDIS_PORT", "6381")
    monkeypatch.setenv("ORCEST_REDIS_PASSWORD", "secret")
    monkeypatch.setenv("ORCEST_REDIS_KEY_PREFIX", "env-prefix")

    cfg = build_redis_config(raw)

    assert cfg.host == "env-host"
    assert cfg.port == 6381
    assert cfg.password == "secret"
    assert cfg.key_prefix == "env-prefix"


def test_build_redis_config_env_vars_only(monkeypatch):
    """build_redis_config() with no raw dict reads purely from env vars."""
    monkeypatch.setenv("ORCEST_REDIS_HOST", "10.0.0.5")
    monkeypatch.setenv("ORCEST_REDIS_PORT", "6382")
    monkeypatch.setenv("ORCEST_REDIS_KEY_PREFIX", "poolmgr")

    cfg = build_redis_config()

    assert cfg.host == "10.0.0.5"
    assert cfg.port == 6382
    assert cfg.key_prefix == "poolmgr"


def test_build_redis_config_password_from_env_only(monkeypatch):
    """Password comes from env var only, never from the raw dict."""
    raw = {"redis": {"password": "yaml-password-should-be-ignored"}}
    monkeypatch.setenv("ORCEST_REDIS_PASSWORD", "env-password")

    cfg = build_redis_config(raw)

    # Password always comes from the env var
    assert cfg.password == "env-password"


def test_build_redis_config_password_none_without_env():
    """Password is None when ORCEST_REDIS_PASSWORD is not set."""
    raw = {"redis": {"host": "somehost"}}

    cfg = build_redis_config(raw)

    assert cfg.password is None


def test_build_redis_config_password_in_yaml_ignored_without_env():
    """Password specified in the YAML raw dict is silently ignored when the
    ORCEST_REDIS_PASSWORD env var is not set.  This ensures the code path
    never reads a password from the config file (security policy)."""
    raw = {"redis": {"password": "yaml-secret-that-must-be-ignored"}}

    cfg = build_redis_config(raw)

    assert cfg.password is None


def test_build_redis_config_invalid_port_raises(monkeypatch):
    """Non-numeric port in env var raises ValueError."""
    monkeypatch.setenv("ORCEST_REDIS_PORT", "not-a-number")

    with pytest.raises(ValueError, match="redis.port"):
        build_redis_config()


# -- Worker ephemeral mode --------------------------------------------------


def test_worker_ephemeral_defaults_to_false(tmp_path: Path):
    """WorkerConfig.ephemeral defaults to False when not set in YAML."""
    cfg_file = tmp_path / "worker.yaml"
    cfg_file.write_text("worker_id: worker-0\n")

    config = load_worker_config(cfg_file)

    assert config.ephemeral is False


def test_worker_ephemeral_true_from_yaml(tmp_path: Path):
    """WorkerConfig.ephemeral=true is correctly parsed from YAML."""
    cfg_file = tmp_path / "worker.yaml"
    cfg_file.write_text("worker_id: worker-0\nephemeral: true\n")

    config = load_worker_config(cfg_file)

    assert config.ephemeral is True


def test_worker_ephemeral_quoted_string_raises(tmp_path: Path):
    """Quoted string 'true' for ephemeral raises ValueError (must be unquoted YAML bool)."""
    cfg_file = tmp_path / "worker.yaml"
    cfg_file.write_text('worker_id: worker-0\nephemeral: "true"\n')

    with pytest.raises(ValueError, match="ephemeral"):
        load_worker_config(cfg_file)


# -- Empty YAML file ----------------------------------------------------------


def test_load_worker_config_empty_yaml_file(tmp_path: Path):
    """An empty YAML file uses defaults (worker_id defaults to 'worker-0')."""
    cfg_file = tmp_path / "empty.yaml"
    cfg_file.write_text("")

    config = load_worker_config(cfg_file)

    assert config.worker_id == "worker-0"


def test_load_worker_config_comments_only_yaml(tmp_path: Path):
    """A YAML file with only comments is treated as empty."""
    cfg_file = tmp_path / "comments.yaml"
    cfg_file.write_text("# This file is intentionally empty\n# No config here\n")

    config = load_worker_config(cfg_file)

    assert config.worker_id == "worker-0"


# -- Non-mapping YAML root ---------------------------------------------------


def test_load_orchestrator_config_non_mapping_root(tmp_path: Path):
    """Config file with a YAML list root raises ValueError."""
    cfg_file = tmp_path / "list-root.yaml"
    cfg_file.write_text("- item1\n- item2\n")

    with pytest.raises(ValueError, match="YAML mapping"):
        load_orchestrator_config(cfg_file)


def test_load_worker_config_non_mapping_root(tmp_path: Path):
    """Config file with a YAML list root raises ValueError."""
    cfg_file = tmp_path / "list-root.yaml"
    cfg_file.write_text("- item1\n- item2\n")

    with pytest.raises(ValueError, match="YAML mapping"):
        load_worker_config(cfg_file)


# -- Empty worker_id validation -----------------------------------------------


def test_load_worker_config_empty_worker_id_raises(tmp_path: Path):
    """Empty string worker_id (from YAML or env) raises ValueError."""
    cfg_file = tmp_path / "worker.yaml"
    cfg_file.write_text('worker_id: ""\n')

    with pytest.raises(ValueError, match="worker_id is required"):
        load_worker_config(cfg_file)


# -- Deployment validation ----------------------------------------------------


def test_deployment_health_check_timeout_zero_raises(tmp_path: Path):
    """health_check_timeout <= 0 with a health_check_url set raises ValueError."""
    cfg_file = tmp_path / "orcest.yaml"
    cfg_file.write_text(
        "github:\n  repo: acme/widgets\n"
        "deployment:\n"
        "  enabled: true\n"
        "  command: deploy.sh\n"
        "  health_check_url: http://localhost/health\n"
        "  health_check_timeout: 0\n"
    )

    with pytest.raises(ValueError, match="health_check_timeout"):
        load_orchestrator_config(cfg_file)


def test_deployment_health_check_timeout_negative_raises(tmp_path: Path):
    """health_check_timeout < 0 with a health_check_url set raises ValueError."""
    cfg_file = tmp_path / "orcest.yaml"
    cfg_file.write_text(
        "github:\n  repo: acme/widgets\n"
        "deployment:\n"
        "  enabled: true\n"
        "  command: deploy.sh\n"
        "  health_check_url: http://localhost/health\n"
        "  health_check_timeout: -5\n"
    )

    with pytest.raises(ValueError, match="health_check_timeout"):
        load_orchestrator_config(cfg_file)


# -- stale_pending_timeout_seconds validation ----------------------------------


def test_stale_pending_timeout_seconds_zero_raises(tmp_path: Path):
    """stale_pending_timeout_seconds <= 0 raises ValueError."""
    cfg_file = tmp_path / "orcest.yaml"
    cfg_file.write_text("github:\n  repo: acme/widgets\nstale_pending_timeout_seconds: 0\n")

    with pytest.raises(ValueError, match="stale_pending_timeout_seconds"):
        load_orchestrator_config(cfg_file)


def test_stale_pending_timeout_seconds_negative_raises(tmp_path: Path):
    """stale_pending_timeout_seconds < 0 raises ValueError."""
    cfg_file = tmp_path / "orcest.yaml"
    cfg_file.write_text("github:\n  repo: acme/widgets\nstale_pending_timeout_seconds: -1\n")

    with pytest.raises(ValueError, match="stale_pending_timeout_seconds"):
        load_orchestrator_config(cfg_file)


# -- Null integer field --------------------------------------------------------


def test_null_integer_field_raises(tmp_path: Path):
    """An explicitly null integer field in YAML raises ValueError."""
    cfg_file = tmp_path / "orcest.yaml"
    cfg_file.write_text("github:\n  repo: acme/widgets\npolling:\n  interval: null\n")

    with pytest.raises(ValueError, match="explicitly set to null"):
        load_orchestrator_config(cfg_file)


# -- Multi-project orchestrator config ----------------------------------------


def test_load_orchestrator_config_multi_project(tmp_path: Path):
    """YAML with two projects produces config.projects with correct entries."""
    cfg_file = tmp_path / "orcest.yaml"
    cfg_file.write_text(
        "github:\n"
        "  token: ghp_shared\n"
        "  repo: fallback/repo\n"
        "  claude_token: claude_shared\n"
        "projects:\n"
        "  - repo: acme/widgets\n"
        "    token: ghp_acme\n"
        "    claude_token: claude_acme\n"
        "    key_prefix: acme\n"
        "  - repo: acme/gadgets\n"
        "    token: ghp_gadgets\n"
        "    claude_token: claude_gadgets\n"
        "    key_prefix: gadgets\n"
    )

    config = load_orchestrator_config(cfg_file)

    assert len(config.projects) == 2
    assert config.projects[0].repo == "acme/widgets"
    assert config.projects[0].token == "ghp_acme"
    assert config.projects[0].claude_token == "claude_acme"
    assert config.projects[0].key_prefix == "acme"
    assert config.projects[1].repo == "acme/gadgets"
    assert config.projects[1].token == "ghp_gadgets"
    assert config.projects[1].claude_token == "claude_gadgets"
    assert config.projects[1].key_prefix == "gadgets"


def test_load_orchestrator_config_single_project_backward_compat(tmp_path: Path):
    """No projects key, just github.repo: a single ProjectConfig is synthesized."""
    cfg_file = tmp_path / "orcest.yaml"
    cfg_file.write_text(
        "github:\n  token: ghp_tok\n  repo: acme/widgets\n  claude_token: claude_tok\n"
    )

    config = load_orchestrator_config(cfg_file)

    assert len(config.projects) == 1
    proj = config.projects[0]
    assert isinstance(proj, ProjectConfig)
    assert proj.repo == "acme/widgets"
    assert proj.token == "ghp_tok"
    assert proj.claude_token == "claude_tok"
    assert proj.key_prefix == "orcest"  # default redis key_prefix


def test_load_orchestrator_config_projects_token_defaults(tmp_path: Path):
    """Project entries without token/claude_token inherit from shared github.* fields."""
    cfg_file = tmp_path / "orcest.yaml"
    cfg_file.write_text(
        "github:\n"
        "  token: ghp_shared\n"
        "  repo: fallback/repo\n"
        "  claude_token: claude_shared\n"
        "projects:\n"
        "  - repo: acme/widgets\n"
        "    key_prefix: widgets\n"
        "  - repo: acme/gadgets\n"
        "    key_prefix: gadgets\n"
    )

    config = load_orchestrator_config(cfg_file)

    assert len(config.projects) == 2
    for proj in config.projects:
        assert proj.token == "ghp_shared"
        assert proj.claude_token == "claude_shared"


def test_load_orchestrator_config_multi_project_empty_key_prefix_raises(tmp_path: Path):
    """Empty key_prefix in any project entry should raise ValueError."""
    cfg_file = tmp_path / "orcest.yaml"
    cfg_file.write_text(
        "github:\n"
        "  repo: fallback/repo\n"
        "redis:\n"
        "  key_prefix: ''\n"
        "projects:\n"
        "  - repo: acme/widgets\n"
        "    key_prefix: ''\n"
        "  - repo: acme/gadgets\n"
        "    key_prefix: gadgets\n"
    )

    with pytest.raises(ValueError, match="key_prefix is required"):
        load_orchestrator_config(cfg_file)


def test_load_orchestrator_config_multi_project_duplicate_key_prefix_raises(tmp_path: Path):
    """Duplicate key_prefix values in multi-project mode should raise ValueError."""
    cfg_file = tmp_path / "orcest.yaml"
    cfg_file.write_text(
        "github:\n"
        "  repo: fallback/repo\n"
        "projects:\n"
        "  - repo: acme/widgets\n"
        "    key_prefix: same\n"
        "  - repo: acme/gadgets\n"
        "    key_prefix: same\n"
    )

    with pytest.raises(ValueError, match="key_prefix must be unique"):
        load_orchestrator_config(cfg_file)


def test_load_orchestrator_config_projects_non_dict_entry_raises(tmp_path: Path):
    """Non-dict entry in projects list should raise ValueError."""
    cfg_file = tmp_path / "orcest.yaml"
    cfg_file.write_text(
        "github:\n"
        "  repo: fallback/repo\n"
        "projects:\n"
        "  - repo: acme/widgets\n"
        "    key_prefix: widgets\n"
        "  - not-a-mapping\n"
    )

    with pytest.raises(ValueError, match=r"projects\[1\] must be a YAML mapping"):
        load_orchestrator_config(cfg_file)


def test_load_orchestrator_config_single_project_in_list(tmp_path: Path):
    """A single-entry projects list with valid key_prefix should work."""
    cfg_file = tmp_path / "orcest.yaml"
    cfg_file.write_text(
        "github:\n"
        "  token: ghp_tok\n"
        "  repo: fallback/repo\n"
        "projects:\n"
        "  - repo: acme/widgets\n"
        "    key_prefix: widgets\n"
    )

    config = load_orchestrator_config(cfg_file)

    assert len(config.projects) == 1
    assert config.projects[0].repo == "acme/widgets"
    assert config.projects[0].key_prefix == "widgets"


def test_load_orchestrator_config_projects_not_a_list_raises(tmp_path: Path):
    """projects: as a scalar or mapping should raise, not silently fall back."""
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text("github:\n  repo: owner/repo\nprojects: not-a-list\n")
    with pytest.raises(ValueError, match="must be a YAML list"):
        load_orchestrator_config(cfg_file)


def test_load_orchestrator_config_empty_repo_in_project_raises(tmp_path: Path):
    """A project entry with an empty repo should raise a clear error."""
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(
        "projects:\n"
        "  - repo: owner/repo-a\n"
        "    key_prefix: prefix-a\n"
        "  - repo: ''\n"
        "    key_prefix: prefix-b\n"
    )
    with pytest.raises(ValueError, match="non-empty 'repo' field"):
        load_orchestrator_config(cfg_file)
