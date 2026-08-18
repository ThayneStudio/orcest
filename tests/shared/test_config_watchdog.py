"""Unit tests for the activity-watchdog and fleet-health configuration surface.

Covers RunnerConfig.watchdog (WatchdogConfig) parsing on both the worker and
orchestrator loaders, the raised RunnerConfig.timeout / PoolConfig defaults,
and the orchestrator's fleet_health: block.
"""

from pathlib import Path

import pytest

from orcest.fleet.config import PoolConfig
from orcest.shared.config import (
    OrchestratorConfig,
    RunnerConfig,
    WatchdogConfig,
    load_orchestrator_config,
    load_worker_config,
)

pytestmark = pytest.mark.unit

_ENV_VARS_TO_CLEAR = [
    "ORCEST_REDIS_HOST",
    "ORCEST_REDIS_PORT",
    "ORCEST_REDIS_PASSWORD",
    "ORCEST_REDIS_KEY_PREFIX",
    "GITHUB_TOKEN",
    "ORCEST_REPO",
    "ORCEST_WORKER_ID",
    "ORCEST_WORKSPACE_DIR",
]


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for var in _ENV_VARS_TO_CLEAR:
        monkeypatch.delenv(var, raising=False)


# -- RunnerConfig.timeout ceiling -------------------------------------------


def test_runner_config_timeout_default_is_21600():
    assert RunnerConfig().timeout == 21600


# -- WatchdogConfig defaults --------------------------------------------------


def test_watchdog_config_defaults():
    wd = WatchdogConfig()
    assert wd.enabled is True
    assert wd.sample_interval == 30.0
    assert wd.startup_grace == 600.0
    assert wd.idle_window == 600.0
    assert wd.waiting_grace == 1800.0
    assert wd.loop_exact_threshold == 4
    assert wd.loop_error_threshold == 3
    assert wd.loop_pingpong_threshold == 6


# -- worker.yaml watchdog parsing --------------------------------------------


def test_worker_config_watchdog_defaults_when_runner_absent(tmp_path: Path):
    cfg_file = tmp_path / "worker.yaml"
    cfg_file.write_text("worker_id: worker-0\n")

    cfg = load_worker_config(cfg_file)

    assert cfg.runner.watchdog.enabled is True
    assert cfg.runner.watchdog.idle_window == 600.0
    assert cfg.runner.timeout == 21600


def test_worker_config_watchdog_defaults_when_runner_present_but_watchdog_absent(
    tmp_path: Path,
):
    cfg_file = tmp_path / "worker.yaml"
    cfg_file.write_text("worker_id: worker-0\nrunner:\n  max_retries: 5\n")

    cfg = load_worker_config(cfg_file)

    assert cfg.runner.watchdog == WatchdogConfig()
    assert cfg.runner.max_retries == 5


def test_worker_config_watchdog_block_parses(tmp_path: Path):
    cfg_file = tmp_path / "worker.yaml"
    cfg_file.write_text(
        "worker_id: worker-0\n"
        "runner:\n"
        "  watchdog:\n"
        "    enabled: false\n"
        "    idle_window: 120\n"
    )

    cfg = load_worker_config(cfg_file)

    assert cfg.runner.watchdog.enabled is False
    assert cfg.runner.watchdog.idle_window == 120.0
    # Non-overridden fields keep their defaults.
    assert cfg.runner.watchdog.sample_interval == 30.0
    assert cfg.runner.watchdog.loop_exact_threshold == 4


def test_worker_config_watchdog_full_block_parses(tmp_path: Path):
    cfg_file = tmp_path / "worker.yaml"
    cfg_file.write_text(
        "worker_id: worker-0\n"
        "runner:\n"
        "  timeout: 21600\n"
        "  watchdog:\n"
        "    enabled: true\n"
        "    sample_interval: 15\n"
        "    startup_grace: 300\n"
        "    idle_window: 300\n"
        "    waiting_grace: 900\n"
        "    loop_exact_threshold: 2\n"
        "    loop_error_threshold: 2\n"
        "    loop_pingpong_threshold: 4\n"
    )

    cfg = load_worker_config(cfg_file)

    wd = cfg.runner.watchdog
    assert wd.enabled is True
    assert wd.sample_interval == 15.0
    assert wd.startup_grace == 300.0
    assert wd.idle_window == 300.0
    assert wd.waiting_grace == 900.0
    assert wd.loop_exact_threshold == 2
    assert wd.loop_error_threshold == 2
    assert wd.loop_pingpong_threshold == 4


def test_worker_config_watchdog_rejects_null_enabled(tmp_path: Path):
    cfg_file = tmp_path / "worker.yaml"
    cfg_file.write_text("worker_id: worker-0\nrunner:\n  watchdog:\n    enabled:\n")

    with pytest.raises(ValueError, match="explicitly set to null"):
        load_worker_config(cfg_file)


def test_worker_config_watchdog_rejects_non_positive_sample_interval(tmp_path: Path):
    # M5: a zero/negative timer field is a config mistake (spins the
    # watchdog thread in a tight loop), not a valid aggressive tuning.
    cfg_file = tmp_path / "worker.yaml"
    cfg_file.write_text(
        "worker_id: worker-0\nrunner:\n  watchdog:\n    sample_interval: 0\n"
    )

    with pytest.raises(ValueError, match="runner.watchdog.sample_interval"):
        load_worker_config(cfg_file)


# -- orchestrator.yaml watchdog parsing (shares the same RunnerConfig) ------


def test_orchestrator_config_watchdog_defaults_when_absent(tmp_path: Path):
    cfg_file = tmp_path / "orcest.yaml"
    cfg_file.write_text("github:\n  repo: acme/widgets\n")

    cfg = load_orchestrator_config(cfg_file)

    assert cfg.runner.watchdog.enabled is True
    assert cfg.runner.watchdog.idle_window == 600.0
    assert cfg.runner.timeout == 21600


def test_orchestrator_config_watchdog_block_parses(tmp_path: Path):
    cfg_file = tmp_path / "orcest.yaml"
    cfg_file.write_text(
        "github:\n  repo: acme/widgets\nrunner:\n  watchdog:\n    enabled: false\n"
    )

    cfg = load_orchestrator_config(cfg_file)

    assert cfg.runner.watchdog.enabled is False


# -- orchestrator fleet_health: block ----------------------------------------


def test_orchestrator_config_fleet_health_defaults_when_absent(tmp_path: Path):
    cfg_file = tmp_path / "orcest.yaml"
    cfg_file.write_text("github:\n  repo: acme/widgets\n")

    cfg = load_orchestrator_config(cfg_file)

    assert cfg.pressure_min_tasks == 3
    assert cfg.pressure_window == 600
    assert cfg.pressure_hold == 900
    assert cfg.max_kills_per_hour == OrchestratorConfig().max_kills_per_hour
    assert cfg.max_kills_per_hour == 0  # observation-by-default: kills disabled


def test_orchestrator_config_fleet_health_block_parses(tmp_path: Path):
    cfg_file = tmp_path / "orcest.yaml"
    cfg_file.write_text(
        "github:\n"
        "  repo: acme/widgets\n"
        "fleet_health:\n"
        "  pressure_min_tasks: 5\n"
        "  pressure_window: 300\n"
        "  pressure_hold: 600\n"
        "  max_kills_per_hour: 10\n"
    )

    cfg = load_orchestrator_config(cfg_file)

    assert cfg.pressure_min_tasks == 5
    assert cfg.pressure_window == 300
    assert cfg.pressure_hold == 600
    assert cfg.max_kills_per_hour == 10


# -- PoolConfig raised ceiling + activity_stale_after ------------------------


def test_pool_config_defaults_25200_and_300():
    pool = PoolConfig()
    assert pool.max_task_duration == 25200
    assert pool.activity_stale_after == 300
    # Invariant: force-kill ceiling must exceed the runner's own timeout.
    assert pool.max_task_duration > RunnerConfig().timeout
