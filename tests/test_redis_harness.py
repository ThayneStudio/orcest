"""Unit tests for invocation-scoped test Redis harness pieces."""

from __future__ import annotations

import signal
import subprocess
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml

from tests.harness.constants import (
    COMPOSE_FILENAME,
    HARNESS_LABEL,
    MARKER_KEY,
    NONCE_ENV,
    URL_ENV,
    default_compose_file,
    repo_root,
)
from tests.harness.proof import (
    RedisProofError,
    guarded_flushdb,
    parse_redis_url,
    require_test_redis_proof,
    teardown_real_redis_client,
)
from tests.harness.supervisor import (
    CleanupOnce,
    compose_cleanup_cmd,
    generate_project_name,
    resolve_exit_status,
)

ROOT = repo_root()


def _makefile_target(text: str, target: str) -> str:
    lines = text.splitlines()
    header = f"{target}:"
    capturing = False
    block: list[str] = []
    for line in lines:
        if capturing:
            if line.startswith("\t"):
                block.append(line)
                continue
            break
        if line.startswith(header):
            capturing = True
            block.append(line)
    return "\n".join(block)


def test_parse_redis_url_missing_path_is_database_zero() -> None:
    parts = parse_redis_url("redis://127.0.0.1:6379")
    assert parts["db"] == 0
    assert parts["host"] == "127.0.0.1"
    assert parts["port"] == 6379


def test_require_proof_missing_url_and_nonce_are_errors() -> None:
    with pytest.raises(RedisProofError, match=URL_ENV):
        require_test_redis_proof({NONCE_ENV: "abc"})
    with pytest.raises(RedisProofError, match=NONCE_ENV):
        require_test_redis_proof({URL_ENV: "redis://127.0.0.1:9/15"})


def test_require_proof_rejects_database_zero() -> None:
    with pytest.raises(RedisProofError, match="database 0"):
        require_test_redis_proof({URL_ENV: "redis://127.0.0.1:9/0", NONCE_ENV: "abc"})
    with pytest.raises(RedisProofError, match="database 0"):
        require_test_redis_proof({URL_ENV: "redis://127.0.0.1:9", NONCE_ENV: "abc"})


def test_require_proof_accepts_database_15() -> None:
    url, nonce, parts = require_test_redis_proof(
        {URL_ENV: "redis://127.0.0.1:9/15", NONCE_ENV: "abc"}
    )
    assert url.endswith("/15")
    assert nonce == "abc"
    assert parts["db"] == 15


def test_guarded_flushdb_refuses_database_zero() -> None:
    raw = MagicMock()
    with pytest.raises(RedisProofError, match="database 0"):
        guarded_flushdb(raw, "redis://127.0.0.1:9/0", "abc")
    raw.flushdb.assert_not_called()


def test_guarded_flushdb_refuses_missing_and_mismatched_marker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw = MagicMock()
    monkeypatch.setattr("tests.harness.proof.read_invocation_marker", lambda url: None)
    with pytest.raises(RedisProofError, match="missing invocation marker"):
        guarded_flushdb(raw, "redis://127.0.0.1:9/15", "abc")
    raw.flushdb.assert_not_called()

    monkeypatch.setattr("tests.harness.proof.read_invocation_marker", lambda url: "other")
    with pytest.raises(RedisProofError, match="does not match"):
        guarded_flushdb(raw, "redis://127.0.0.1:9/15", "abc")
    raw.flushdb.assert_not_called()


def test_guarded_flushdb_runs_after_matching_marker(monkeypatch: pytest.MonkeyPatch) -> None:
    raw = MagicMock()
    monkeypatch.setattr("tests.harness.proof.read_invocation_marker", lambda url: "abc")
    guarded_flushdb(raw, "redis://127.0.0.1:9/15", "abc")
    raw.flushdb.assert_called_once_with()


def test_teardown_closes_client_when_proof_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    client = MagicMock()
    monkeypatch.setattr("tests.harness.proof.read_invocation_marker", lambda url: None)
    with pytest.raises(RedisProofError):
        teardown_real_redis_client(client, "redis://127.0.0.1:9/15", "abc")
    client.close.assert_called_once_with()
    client.client.flushdb.assert_not_called()


def test_conftest_has_no_implicit_redis_url_default() -> None:
    text = (ROOT / "tests" / "conftest.py").read_text()
    assert "redis://localhost:6379/15" not in text
    assert "Real Redis not available" not in text
    assert "pytest.skip(" not in text


def test_exit_status_signal_and_cleanup_override() -> None:
    assert resolve_exit_status(0, None, True) == 0
    assert resolve_exit_status(7, None, True) == 7
    assert resolve_exit_status(0, signal.SIGINT, True) == 130
    assert resolve_exit_status(0, signal.SIGTERM, True) == 143
    assert resolve_exit_status(0, None, False) == 1
    assert resolve_exit_status(7, None, False) == 7
    assert resolve_exit_status(0, signal.SIGINT, False) == 130
    assert resolve_exit_status(0, signal.SIGTERM, False) == 143
    assert resolve_exit_status(-9, None, True) == 137


def test_cleanup_once_runs_exactly_once() -> None:
    calls = {"n": 0}

    def boom() -> bool:
        calls["n"] += 1
        return False

    cleanup = CleanupOnce(boom)
    assert cleanup() is False
    assert cleanup() is True
    assert cleanup.calls == 1
    assert calls["n"] == 1


def test_compose_cleanup_cmd_targets_exact_project() -> None:
    compose = default_compose_file()
    cmd = compose_cleanup_cmd("orcest-test-rdeadbeef", compose)
    assert cmd == [
        "docker",
        "compose",
        "-p",
        "orcest-test-rdeadbeef",
        "-f",
        str(compose),
        "down",
        "--volumes",
        "--remove-orphans",
    ]


def test_project_name_is_nonce_scoped() -> None:
    nonce = "abcdef0123456789"
    assert generate_project_name(nonce) == "orcest-test-rabcdef012345"


def test_test_compose_file_is_invocation_scoped() -> None:
    path = ROOT / COMPOSE_FILENAME
    data = yaml.safe_load(path.read_text())
    assert "volumes" not in data
    redis = data["services"]["redis"]
    assert redis["ports"] == ["127.0.0.1::6379"]
    assert HARNESS_LABEL in redis["labels"]
    top_networks = data.get("networks") or {}
    for net in top_networks.values():
        assert not net.get("external")
        assert "name" not in net
    named_volume_sources = [
        vol.get("source") for vol in redis.get("volumes") or [] if isinstance(vol, dict)
    ]
    assert all(source is None for source in named_volume_sources)


def test_production_redis_compose_is_not_an_override_target() -> None:
    prod = yaml.safe_load((ROOT / "docker-compose.redis.yml").read_text())
    test = yaml.safe_load((ROOT / COMPOSE_FILENAME).read_text())
    assert prod["services"]["redis"]["ports"] == ["127.0.0.1:6379:6379"]
    assert "redis-data" in (prod.get("volumes") or {})
    assert prod["networks"]["orcest"]["name"] == "orcest"
    assert test["services"]["redis"]["ports"] != prod["services"]["redis"]["ports"]
    assert "redis-data" not in (test.get("volumes") or {})


def test_makefile_correctness_targets_do_not_use_shared_redis() -> None:
    makefile = (ROOT / "Makefile").read_text()
    test_block = _makefile_target(makefile, "test")
    integration_block = _makefile_target(makefile, "test-integration")
    stress_block = _makefile_target(makefile, "test-stress")
    unit_block = _makefile_target(makefile, "test-unit")
    dashboard_block = _makefile_target(makefile, "test-dashboard")
    lint_check_block = _makefile_target(makefile, "lint-check")
    typecheck_block = _makefile_target(makefile, "typecheck")
    check_fast_block = _makefile_target(makefile, "check-fast")
    check_full_block = _makefile_target(makefile, "check-full")

    for block in (
        test_block,
        integration_block,
        stress_block,
        unit_block,
        dashboard_block,
        lint_check_block,
        typecheck_block,
        check_fast_block,
        check_full_block,
    ):
        assert "redis-up" not in block
        assert "redis-down" not in block
        assert "docker-compose.redis.yml" not in block

    assert "$(MAKE) test-unit" in test_block
    assert "$(MAKE) test-integration" in test_block
    assert "$(MAKE) test-stress" in test_block
    assert "$(MAKE) test-dashboard" in test_block

    assert "python3 -m tests.harness.supervisor" in integration_block
    assert "pytest -m integration" in integration_block
    assert "tests/integration" not in integration_block
    assert "python3 -m tests.harness.supervisor" in stress_block
    assert "pytest -m stress" in stress_block

    redis_up = _makefile_target(makefile, "redis-up")
    redis_down = _makefile_target(makefile, "redis-down")
    assert "docker-compose.redis.yml" in redis_up
    assert "docker-compose.redis.yml" in redis_down


def test_pytest_m_integration_selects_inline_and_directory_tests() -> None:
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "--collect-only",
            "-q",
            "-m",
            "integration",
            "tests/worker/test_liveness_signals.py",
            "tests/worker/test_runner_watchdog_integration.py",
            "tests/integration/test_locking.py",
            "tests/stress/test_concurrent_workers.py",
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr
    out = proc.stdout
    assert "tests/worker/test_liveness_signals.py" in out
    assert "tests/worker/test_runner_watchdog_integration.py" in out
    assert "tests/integration/test_locking.py" in out
    assert "test_concurrent_workers" not in out


def test_pytest_m_stress_selects_stress_directory() -> None:
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "--collect-only",
            "-q",
            "-m",
            "stress",
            "tests/stress/test_concurrent_workers.py",
            "tests/integration/test_locking.py",
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr
    assert "tests/stress/test_concurrent_workers.py" in proc.stdout
    assert "tests/integration/test_locking.py" not in proc.stdout


def test_marker_key_is_stable() -> None:
    assert MARKER_KEY == "orcest:test:invocation"
    assert Path(default_compose_file()).name == COMPOSE_FILENAME
