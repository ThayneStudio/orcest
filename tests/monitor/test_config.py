"""Tests for missing-YAML-key error handling in ``load_monitor_config``.

A missing ``db_path``, ``write_token_env``, or reader ``name``/``token_env``
key must raise a ``ValueError`` naming the missing key -- not a bare
``KeyError`` -- matching the style used for missing env vars (see
``test_service.py``'s ``test_load_monitor_config_missing_*_env_raises``).
"""

from __future__ import annotations

import pytest

from orcest.monitor.config import load_monitor_config


def _write(tmp_path, text: str) -> str:
    path = tmp_path / "monitor.yaml"
    path.write_text(text)
    return str(path)


def test_missing_db_path_raises_value_error(tmp_path, monkeypatch):
    monkeypatch.setenv("MONITOR_WRITE_TOKEN", "write-secret")
    config_path = _write(tmp_path, "write_token_env: MONITOR_WRITE_TOKEN\n")

    with pytest.raises(ValueError, match="db_path"):
        load_monitor_config(config_path)


def test_missing_write_token_env_key_raises_value_error(tmp_path):
    config_path = _write(tmp_path, "db_path: /var/lib/orcest/monitor.db\n")

    with pytest.raises(ValueError, match="write_token_env"):
        load_monitor_config(config_path)


def test_missing_reader_name_key_raises_value_error(tmp_path, monkeypatch):
    monkeypatch.setenv("MONITOR_WRITE_TOKEN", "write-secret")
    monkeypatch.setenv("MONITOR_READER_TOKEN", "read-secret")
    config_path = _write(
        tmp_path,
        "db_path: /var/lib/orcest/monitor.db\n"
        "write_token_env: MONITOR_WRITE_TOKEN\n"
        "readers:\n"
        "  - token_env: MONITOR_READER_TOKEN\n"
        "    scopes: [events:read]\n",
    )

    with pytest.raises(ValueError, match="name"):
        load_monitor_config(config_path)


def test_missing_reader_token_env_key_raises_value_error(tmp_path, monkeypatch):
    monkeypatch.setenv("MONITOR_WRITE_TOKEN", "write-secret")
    config_path = _write(
        tmp_path,
        "db_path: /var/lib/orcest/monitor.db\n"
        "write_token_env: MONITOR_WRITE_TOKEN\n"
        "readers:\n"
        "  - name: dashboard\n"
        "    scopes: [events:read]\n",
    )

    with pytest.raises(ValueError, match="token_env"):
        load_monitor_config(config_path)
