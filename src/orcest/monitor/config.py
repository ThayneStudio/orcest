"""Configuration for the monitor service (ingest + query listeners).

Tokens are never stored in YAML directly -- the config file names an
environment variable (``write_token_env`` / ``token_env``) and the actual
secret is resolved from ``os.environ`` at load time. A missing env var is a
hard failure (``ValueError`` naming the variable) so the service never
silently starts with an empty/None token.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

import yaml

VALID_SCOPES = frozenset({"events:read", "traces:read"})


@dataclass
class Reader:
    name: str
    token: str
    scopes: frozenset[str]


@dataclass
class MonitorConfig:
    db_path: str
    trace_archive_path: str | None = None
    ingest_host: str = "0.0.0.0"
    ingest_port: int = 9091
    query_host: str = "0.0.0.0"
    query_port: int = 9090
    write_token: str = ""
    readers: list[Reader] = field(default_factory=list)


def _resolve_env(var_name: str) -> str:
    value = os.environ.get(var_name)
    if value is None:
        raise ValueError(f"missing required environment variable: {var_name}")
    return value


def _load_reader(raw: dict) -> Reader:
    scopes = frozenset(raw.get("scopes", []))
    unknown = scopes - VALID_SCOPES
    if unknown:
        raise ValueError(f"unknown reader scope(s): {sorted(unknown)}")
    return Reader(
        name=raw["name"],
        token=_resolve_env(raw["token_env"]),
        scopes=scopes,
    )


def load_monitor_config(path: str) -> MonitorConfig:
    """Load a :class:`MonitorConfig` from a YAML file.

    ``write_token_env`` and each reader's ``token_env`` are resolved through
    ``os.environ``; a missing variable raises ``ValueError`` naming it.
    """
    with open(path) as f:
        raw = yaml.safe_load(f) or {}

    readers = [_load_reader(r) for r in raw.get("readers", [])]

    return MonitorConfig(
        db_path=raw["db_path"],
        trace_archive_path=raw.get("trace_archive_path"),
        ingest_host=raw.get("ingest_host", "0.0.0.0"),
        ingest_port=raw.get("ingest_port", 9091),
        query_host=raw.get("query_host", "0.0.0.0"),
        query_port=raw.get("query_port", 9090),
        write_token=_resolve_env(raw["write_token_env"]),
        readers=readers,
    )
