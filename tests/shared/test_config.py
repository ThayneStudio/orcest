"""Unit tests for orchestrator and worker config loading."""

from pathlib import Path

import pytest

from orcest.shared.config import RunnerConfig, load_orchestrator_config, load_worker_config

# ---------------------------------------------------------------------------
# Env vars that config.py reads -- we must ensure they are unset in every
# test so the ambient shell environment cannot leak into assertions.
# ---------------------------------------------------------------------------
_ENV_VARS_TO_CLEAR = [
    "ORCEST_REDIS_HOST",
    "ORCEST_REDIS_PORT",
    "ORCEST_REDIS_PASSWORD",
    "GITHUB_TOKEN",
    "ORCEST_REPO",
    "ORCEST_DEFAULT_RUNNER",
    "ORCEST_WORKER_ID",
    "ORCEST_WORKSPACE_DIR",
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
        "redis:\n"
        "  socket_timeout: 5\n"
        "  socket_connect_timeout: 3\n"
        "github:\n"
        "  repo: acme/widgets\n"
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
        "redis:\n"
        "  socket_timeout: 15\n"
        "  socket_connect_timeout: 7\n"
        "worker_id: worker-0\n"
    )

    config = load_worker_config(cfg_file)

    assert config.redis.socket_timeout == 15
    assert config.redis.socket_connect_timeout == 7
