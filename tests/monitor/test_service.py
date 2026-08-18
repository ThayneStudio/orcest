"""Tests for the monitor service entrypoint helpers.

``run_monitor`` starting live uvicorn servers end-to-end (real sockets, real
SIGTERM delivery) isn't worth integration-testing here (see task-A8 brief).
This covers two things without live ports:

- ``load_monitor_config``'s YAML round-trip and its env-var resolution
  failure mode.
- ``run_monitor``'s handling of a listener that dies on its own (e.g. a
  startup bind failure) instead of via a shutdown signal -- it must not hang
  forever, and must surface the failure as ``SystemExit(1)``. The uvicorn
  ``Server`` class is monkeypatched with fakes so no real sockets are opened.
"""

from __future__ import annotations

import time

import pytest

from orcest.monitor import db, service
from orcest.monitor.config import MonitorConfig, load_monitor_config


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


def test_load_monitor_config_empty_write_token_env_raises(tmp_path, monkeypatch):
    """An env var that resolves to the empty string must never authenticate.

    A blank Authorization: Bearer <empty> header would otherwise satisfy a
    naive equality check, so an empty resolved token is treated the same as
    a missing one -- both are hard failures.
    """
    monkeypatch.setenv("MONITOR_WRITE_TOKEN", "")
    config_path = _write_config(tmp_path)

    with pytest.raises(ValueError, match="MONITOR_WRITE_TOKEN"):
        load_monitor_config(config_path)


class _CrashingFakeServer:
    """Stands in for uvicorn.Server: dies immediately, like a bind failure."""

    def __init__(self, config):
        self.config = config
        self.should_exit = False

    def run(self) -> None:
        raise RuntimeError("simulated startup bind failure")


class _HangingFakeServer:
    """Stands in for uvicorn.Server: serves until told to stop."""

    def __init__(self, config):
        self.config = config
        self.should_exit = False

    def run(self) -> None:
        while not self.should_exit:
            time.sleep(0.01)


def test_run_monitor_exits_when_a_listener_crashes_at_startup(tmp_path, monkeypatch):
    """If one listener dies on its own (not via signal), run_monitor must not
    hang forever waiting on shutdown_event -- it must stop the surviving
    listener and raise SystemExit(1) rather than swallowing the failure.
    """
    dbp = str(tmp_path / "m.db")
    db.open_rw(dbp).close()
    cfg = MonitorConfig(db_path=dbp, trace_archive_path=None, write_token="w", readers=[])

    fake_servers = iter([_CrashingFakeServer, _HangingFakeServer])
    created: list = []

    def _fake_server_factory(config):
        instance = next(fake_servers)(config)
        created.append(instance)
        return instance

    monkeypatch.setattr(service.uvicorn, "Server", _fake_server_factory)

    with pytest.raises(SystemExit) as exc_info:
        service.run_monitor(cfg)

    assert exc_info.value.code == 1
    # Both fake servers were constructed (ingest first, then query) and both
    # ended up with should_exit set -- the survivor was told to stop, not
    # left running after the crash.
    assert len(created) == 2
    assert all(s.should_exit for s in created)
