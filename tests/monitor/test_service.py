"""Tests for the monitor service entrypoint helpers.

``run_monitor`` itself starts live uvicorn servers and blocks on a signal --
not something worth integration-testing here (see task-A8 brief). Instead
this covers the piece that matters for correctness without live ports:
``load_monitor_config``'s YAML round-trip and its env-var resolution
failure mode.
"""

from __future__ import annotations

import pytest

from orcest.monitor.config import load_monitor_config


def _write_config(tmp_path, extra: str = "") -> str:
    path = tmp_path / "monitor.yaml"
    path.write_text(
        "db_path: /var/lib/orcest/monitor.db\n"
        "ingest_host: 127.0.0.1\n"
        "ingest_port: 9191\n"
        "query_host: 127.0.0.1\n"
        "query_port: 9190\n"
        "write_token_env: MONITOR_WRITE_TOKEN\n"
        + extra
    )
    return str(path)


def test_load_monitor_config_round_trip(tmp_path, monkeypatch):
    monkeypatch.setenv("MONITOR_WRITE_TOKEN", "write-secret")
    monkeypatch.setenv("MONITOR_READER_TOKEN", "read-secret")
    config_path = _write_config(
        tmp_path,
        extra=(
            "trace_archive_path: /mnt/truenas-logs/orcest-traces\n"
            "readers:\n"
            "  - name: dashboard\n"
            "    token_env: MONITOR_READER_TOKEN\n"
            "    scopes: [events:read, traces:read]\n"
        ),
    )

    cfg = load_monitor_config(config_path)

    assert cfg.db_path == "/var/lib/orcest/monitor.db"
    assert cfg.trace_archive_path == "/mnt/truenas-logs/orcest-traces"
    assert cfg.ingest_host == "127.0.0.1"
    assert cfg.ingest_port == 9191
    assert cfg.query_host == "127.0.0.1"
    assert cfg.query_port == 9190
    assert cfg.write_token == "write-secret"
    assert len(cfg.readers) == 1
    reader = cfg.readers[0]
    assert reader.name == "dashboard"
    assert reader.token == "read-secret"
    assert reader.scopes == frozenset({"events:read", "traces:read"})


def test_load_monitor_config_defaults_when_optional_fields_omitted(tmp_path, monkeypatch):
    monkeypatch.setenv("MONITOR_WRITE_TOKEN", "write-secret")
    config_path = _write_config(tmp_path)

    cfg = load_monitor_config(config_path)

    assert cfg.trace_archive_path is None
    assert cfg.readers == []
    assert cfg.ingest_host == "127.0.0.1"
    assert cfg.query_host == "127.0.0.1"


def test_load_monitor_config_missing_write_token_env_raises(tmp_path, monkeypatch):
    monkeypatch.delenv("MONITOR_WRITE_TOKEN", raising=False)
    config_path = _write_config(tmp_path)

    with pytest.raises(ValueError, match="MONITOR_WRITE_TOKEN"):
        load_monitor_config(config_path)


def test_load_monitor_config_missing_reader_token_env_raises(tmp_path, monkeypatch):
    monkeypatch.setenv("MONITOR_WRITE_TOKEN", "write-secret")
    monkeypatch.delenv("MONITOR_READER_TOKEN", raising=False)
    config_path = _write_config(
        tmp_path,
        extra=(
            "readers:\n"
            "  - name: dashboard\n"
            "    token_env: MONITOR_READER_TOKEN\n"
            "    scopes: [events:read]\n"
        ),
    )

    with pytest.raises(ValueError, match="MONITOR_READER_TOKEN"):
        load_monitor_config(config_path)
