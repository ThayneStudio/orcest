"""Invocation-identity checks before destructive test Redis cleanup.

The nonce is an invocation-identity guard against accidental FLUSHDB of
the wrong Redis, not a hostile-security proof: another process with write
access to the selected instance could spoof the database-0 marker.
"""

from __future__ import annotations

import os
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from typing import TypedDict
from urllib.parse import urlparse, urlunparse

import redis

from orcest.shared.config import RedisConfig
from orcest.shared.redis_client import RedisClient
from tests.harness.constants import MARKER_DB, MARKER_KEY, NONCE_ENV, URL_ENV


class RedisProofError(RuntimeError):
    """URL, nonce, or database-0 marker is missing or does not match."""


class RedisUrlParts(TypedDict):
    host: str
    port: int
    db: int
    password: str | None


def parse_redis_url(url: str) -> RedisUrlParts:
    """Parse a Redis URL. A missing path is database 0 (rejected by proof)."""
    parsed = urlparse(url)
    path = parsed.path.lstrip("/")
    try:
        db = int(path) if path else 0
    except ValueError as exc:
        raise RedisProofError(f"{URL_ENV} database path is not an integer: {url!r}") from exc
    return {
        "host": parsed.hostname or "localhost",
        "port": parsed.port or 6379,
        "db": db,
        "password": parsed.password,
    }


def marker_url(url: str) -> str:
    """Rewrite a Redis URL to database 0, where the invocation marker lives."""
    parsed = urlparse(url)
    return urlunparse(parsed._replace(path=f"/{MARKER_DB}"))


def require_test_redis_proof(
    environ: Mapping[str, str] | None = None,
) -> tuple[str, str, RedisUrlParts]:
    """Require a test Redis URL, nonce, and non-zero database.

    Missing or mismatched values are errors, not skips.
    """
    env: Mapping[str, str] = os.environ if environ is None else environ
    url = env.get(URL_ENV, "").strip()
    nonce = env.get(NONCE_ENV, "").strip()
    if not url:
        raise RedisProofError(
            f"{URL_ENV} is required; real-Redis tests must run under "
            "tests.harness.supervisor (no implicit localhost default)."
        )
    if not nonce:
        raise RedisProofError(
            f"{NONCE_ENV} is required and must match the database-0 "
            f"invocation marker at {MARKER_KEY}."
        )
    parts = parse_redis_url(url)
    if parts["db"] == MARKER_DB:
        raise RedisProofError(
            f"{URL_ENV} must not use database {MARKER_DB}; that database "
            "holds the invocation marker and is not a test keyspace."
        )
    return url, nonce, parts


def read_invocation_marker(url: str) -> str | None:
    """Read the database-0 marker for the Redis instance in ``url``."""
    client = redis.from_url(marker_url(url), decode_responses=True)
    try:
        value = client.get(MARKER_KEY)
    except redis.RedisError as exc:
        raise RedisProofError(
            f"failed to read invocation marker on database {MARKER_DB}: {exc}"
        ) from exc
    finally:
        client.close()
    if value is None:
        return None
    return str(value)


def assert_invocation_proof(url: str, nonce: str) -> None:
    """Fail unless database 0 holds this invocation's nonce."""
    marker = read_invocation_marker(url)
    if marker is None:
        raise RedisProofError(
            f"missing invocation marker {MARKER_KEY!r} on database {MARKER_DB}; "
            "refusing destructive Redis commands."
        )
    if marker != nonce:
        raise RedisProofError(
            f"invocation marker {MARKER_KEY!r} does not match {NONCE_ENV}; "
            "refusing destructive Redis commands."
        )


def guarded_flushdb(raw_client: redis.Redis, url: str, nonce: str) -> None:
    """Run FLUSHDB only after the database-0 marker matches this invocation."""
    parts = parse_redis_url(url)
    if parts["db"] == MARKER_DB:
        raise RedisProofError(f"{URL_ENV} must not use database {MARKER_DB}; refusing FLUSHDB.")
    assert_invocation_proof(url, nonce)
    raw_client.flushdb()


def client_from_parts(parts: RedisUrlParts) -> RedisClient:
    return RedisClient(
        RedisConfig(
            host=parts["host"],
            port=parts["port"],
            db=parts["db"],
            password=parts["password"],
            key_prefix="",
        )
    )


def setup_real_redis_client(
    environ: Mapping[str, str] | None = None,
) -> tuple[RedisClient, str, str]:
    """Prove the invocation, then FLUSHDB and return a live client."""
    url, nonce, parts = require_test_redis_proof(environ)
    assert_invocation_proof(url, nonce)
    client = client_from_parts(parts)
    try:
        guarded_flushdb(client.client, url, nonce)
    except BaseException:
        client.close()
        raise
    return client, url, nonce


def teardown_real_redis_client(client: RedisClient, url: str, nonce: str) -> None:
    """Prove the invocation, FLUSHDB, and always close the client."""
    try:
        guarded_flushdb(client.client, url, nonce)
    finally:
        client.close()


@contextmanager
def managed_real_redis_client(
    environ: Mapping[str, str] | None = None,
) -> Iterator[RedisClient]:
    """Yield a proven test Redis client; close even if teardown proof fails."""
    client, url, nonce = setup_real_redis_client(environ)
    try:
        yield client
    finally:
        teardown_real_redis_client(client, url, nonce)
