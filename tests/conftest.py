"""Shared fixtures for all test modules."""

import os
import types
from urllib.parse import urlparse

import fakeredis
import pytest
import redis

from orcest.shared.config import (
    GithubConfig,
    LabelConfig,
    OrchestratorConfig,
    PollingConfig,
    RedisConfig,
    RunnerConfig,
    WorkerConfig,
)
from orcest.shared.models import Task, TaskType
from orcest.shared.redis_client import RedisClient

# --- Marker registration ---


def pytest_collection_modifyitems(config, items):
    """Auto-mark tests based on directory."""
    for item in items:
        path = str(item.fspath)
        if "/stress/" in path:
            item.add_marker(pytest.mark.stress)
        elif "/integration/" in path:
            item.add_marker(pytest.mark.integration)
        else:
            item.add_marker(pytest.mark.unit)


# --- fakeredis fixtures ---


@pytest.fixture
def fake_redis_server():
    """Shared fakeredis server instance."""
    return fakeredis.FakeServer()


@pytest.fixture
def fake_redis_client(fake_redis_server):
    """RedisClient backed by fakeredis with Lua support."""
    fake = fakeredis.FakeRedis(server=fake_redis_server, decode_responses=True)
    client = RedisClient.from_client(fake)
    yield client
    client.close()


@pytest.fixture
def make_fake_redis_client(fake_redis_server):
    """Factory: create additional RedisClients sharing the same fakeredis server."""
    clients: list[RedisClient] = []

    def factory():
        fake = fakeredis.FakeRedis(server=fake_redis_server, decode_responses=True)
        rc = RedisClient.from_client(fake)
        clients.append(rc)
        return rc

    yield factory
    for c in clients:
        c.close()


# --- Config fixtures ---


@pytest.fixture
def redis_config():
    return RedisConfig(host="localhost", port=6379, db=0)


@pytest.fixture
def label_config():
    return LabelConfig()


@pytest.fixture
def orchestrator_config(redis_config, label_config):
    return OrchestratorConfig(
        redis=redis_config,
        github=GithubConfig(token="fake-token-123", repo="owner/testrepo"),
        polling=PollingConfig(interval=1),
        labels=label_config,
    )


@pytest.fixture
def worker_config(redis_config):
    return WorkerConfig(
        redis=redis_config,
        worker_id="test-worker-0",
        workspace_dir="/tmp/orcest-test-workspaces",
        runner=RunnerConfig(timeout=10, max_retries=1, retry_backoff=0),
    )


# --- Task factory ---


@pytest.fixture
def make_task():
    """Factory for creating Task objects with sensible defaults."""
    counter = [0]

    def factory(
        pr_number=None,
        task_type=TaskType.FIX_CI,
        repo="owner/testrepo",
        token="fake-token",
        branch="fix-branch",
        prompt="Fix the tests.",
    ):
        counter[0] += 1
        if pr_number is None:
            pr_number = counter[0]
        return Task.create(
            task_type=task_type,
            repo=repo,
            token=token,
            resource_type="pr",
            resource_id=pr_number,
            prompt=prompt,
            branch=branch,
        )

    return factory


# --- gh mock fixture ---


@pytest.fixture
def gh_mock(mocker):
    """Patch all functions in orcest.orchestrator.gh module."""
    ns = types.SimpleNamespace()
    for fn_name in [
        "list_open_prs",
        "get_pr",
        "get_ci_status",
        "get_pr_diff",
        "get_failed_run_logs",
        "add_label",
        "remove_label",
        "post_comment",
        "get_unresolved_review_threads",
        "get_pr_review_comments",
        "resolve_review_thread",
        "merge_pr",
        "rerun_workflow",
        "create_issue",
    ]:
        mock = mocker.patch(f"orcest.orchestrator.gh.{fn_name}")
        setattr(ns, fn_name, mock)
    return ns


# --- Real Redis fixtures (shared by integration and stress tests) ---


def _get_redis_url():
    return os.environ.get("ORCEST_TEST_REDIS_URL", "redis://localhost:6379/15")


def _parse_redis_url(url: str) -> dict:
    """Parse a Redis URL into host/port/db/password components."""
    parsed = urlparse(url)
    return {
        "host": parsed.hostname or "localhost",
        "port": parsed.port or 6379,
        "db": int(parsed.path.lstrip("/") or "15"),
        "password": parsed.password,
    }


@pytest.fixture(scope="session")
def _check_redis():
    """Skip all integration/stress tests if Redis is not available."""
    url = _get_redis_url()
    r = redis.from_url(url)
    try:
        r.ping()
    except redis.ConnectionError:
        pytest.skip("Real Redis not available. Start Redis or set ORCEST_TEST_REDIS_URL.")
    finally:
        r.close()


@pytest.fixture
def real_redis_client(_check_redis):
    """RedisClient connected to real Redis DB 15, flushed per test."""
    url = _get_redis_url()
    parsed = _parse_redis_url(url)
    config = RedisConfig(
        host=parsed["host"],
        port=parsed["port"],
        db=parsed["db"],
        password=parsed["password"],
    )
    client = RedisClient(config)
    client.client.flushdb()
    yield client
    client.client.flushdb()
    client.close()


@pytest.fixture
def make_real_redis_client(_check_redis):
    """Factory for creating additional real Redis clients (same DB)."""
    url = _get_redis_url()
    parsed = _parse_redis_url(url)
    clients: list[RedisClient] = []

    def factory():
        config = RedisConfig(
            host=parsed["host"],
            port=parsed["port"],
            db=parsed["db"],
            password=parsed["password"],
        )
        c = RedisClient(config)
        clients.append(c)
        return c

    yield factory
    for c in clients:
        c.close()
