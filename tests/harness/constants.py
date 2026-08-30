"""Shared names for the invocation-scoped test Redis harness."""

from __future__ import annotations

from pathlib import Path

HARNESS_LABEL = "com.orcest.test.harness"
HARNESS_LABEL_VALUE = "1"
NONCE_LABEL = "com.orcest.test.nonce"
PROJECT_LABEL = "com.orcest.test.project"

URL_ENV = "ORCEST_TEST_REDIS_URL"
NONCE_ENV = "ORCEST_TEST_REDIS_NONCE"
IDENTITY_FILE_ENV = "ORCEST_TEST_REDIS_IDENTITY_FILE"
RELEASE_FILE_ENV = "ORCEST_TEST_REDIS_RELEASE_FILE"
READY_FILE_ENV = "ORCEST_TEST_REDIS_CLIENT_READY_FILE"
CHILD_EXIT_ENV = "ORCEST_TEST_REDIS_CHILD_EXIT"
SPAWN_GRANDCHILD_ENV = "ORCEST_TEST_REDIS_SPAWN_GRANDCHILD"
IGNORE_SIGNALS_ENV = "ORCEST_TEST_REDIS_IGNORE_SIGNALS"
GRACE_SECS_ENV = "ORCEST_TEST_REDIS_SIGNAL_GRACE_SECS"
COMPOSE_FILE_ENV = "ORCEST_TEST_REDIS_COMPOSE_FILE"

MARKER_KEY = "orcest:test:invocation"
MARKER_DB = 0
TEST_DB = 15
PROJECT_PREFIX = "orcest-test-r"
DEFAULT_SIGNAL_GRACE_SECS = 15.0
COMPOSE_FILENAME = "docker-compose.redis.test.yml"

CLIENT_KEY_PREFIX = "orcest:test:client:"


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def default_compose_file() -> Path:
    return repo_root() / COMPOSE_FILENAME
