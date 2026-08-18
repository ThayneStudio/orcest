"""Tests for task.started/task.completed/task.failed events emitted by the
worker loop's ``_execute_task`` helper (Task A3).

Two harness fixtures drive a single task end-to-end through ``_execute_task``
with a fakeredis-backed ``RedisClient`` (so the events spool can be inspected
afterward) and a stubbed runner: ``worker_harness`` for the success path,
``worker_harness_failing`` for the failure path.
"""

import json
import logging
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from orcest.shared.config import RedisConfig, RunnerConfig, WorkerConfig
from orcest.shared.events import EVENTS_STREAM
from orcest.shared.models import Task, TaskType
from orcest.worker.loop import _execute_task
from orcest.worker.runner import RunnerResult


def _spooled(fake_redis, type_suffix):
    entries = fake_redis.xrevrange(EVENTS_STREAM, count=50)
    envs = [json.loads(f["envelope"]) for _id, f in entries]
    return [e for e in envs if e["type"] == f"net.orcest.{type_suffix}"]


def _make_task(key_prefix: str = "", token: str = "test-token-events") -> Task:
    # resource_type="issue" skips the PR head-sha snapshot validation path
    # entirely, so the harness doesn't need to stub ``gh.get_pr``.
    return Task.create(
        task_type=TaskType.FIX_PR,
        repo="owner/repo",
        token=token,
        resource_type="issue",
        resource_id=7,
        prompt="Do the thing",
        attempt=1,
        key_prefix=key_prefix,
        credential="super-secret-credential-value",
        provider="claude",
    )


def _make_config() -> WorkerConfig:
    return WorkerConfig(
        redis=RedisConfig(host="localhost", port=6379, db=0),
        worker_id="test-worker-events",
        workspace_dir="/tmp/orcest-events-test-workspaces",
        runner=RunnerConfig(timeout=10, max_retries=1, retry_backoff=0),
    )


def _mock_workspace() -> MagicMock:
    ws = MagicMock()
    ws.setup.return_value = Path("/tmp/fake-workspace/repo")
    ws.current_head_sha.return_value = ""
    ws.cleanup.return_value = None
    return ws


def _run_task(
    fake_redis_client, runner_result: RunnerResult, task: Task | None = None
) -> SimpleNamespace:
    config = _make_config()
    if task is None:
        task = _make_task()
    runner = MagicMock()
    runner.run.return_value = runner_result

    _execute_task(
        task,
        config,
        runner,
        _mock_workspace(),
        fake_redis_client,
        logging.getLogger("test.worker_events"),
    )

    return SimpleNamespace(redis=fake_redis_client, worker_id=config.worker_id, task=task)


@pytest.fixture
def worker_harness(fake_redis_client):
    """Drives one task through ``_execute_task`` on the runner-success path."""
    return _run_task(fake_redis_client, RunnerResult(success=True, summary="All checks fixed"))


@pytest.fixture
def worker_harness_failing(fake_redis_client):
    """Drives one task through ``_execute_task`` on the runner-failure path."""
    return _run_task(
        fake_redis_client, RunnerResult(success=False, summary="Could not resolve conflict")
    )


@pytest.mark.unit
def test_started_and_completed_events(worker_harness):
    fake_redis = worker_harness.redis
    started = _spooled(fake_redis, "task.started")
    assert len(started) == 1
    assert started[0]["data"]["worker_id"] == worker_harness.worker_id
    completed = _spooled(fake_redis, "task.completed")
    assert len(completed) == 1
    assert completed[0]["subject"] == started[0]["subject"]
    assert _spooled(fake_redis, "task.failed") == []


@pytest.mark.unit
def test_failed_event_on_failure(worker_harness_failing):
    fake_redis = worker_harness_failing.redis
    failed = _spooled(fake_redis, "task.failed")
    assert len(failed) == 1
    assert failed[0]["data"]["status"] == "failed"
    assert isinstance(failed[0]["data"]["transient"], bool)


@pytest.mark.unit
def test_events_route_to_task_key_prefix_not_worker_client_prefix(fake_redis_client):
    """A task's project events must land on ``<task.key_prefix>:events``, even
    though the worker's own Redis client is built with a different key
    prefix ("test", per the ``fake_redis_client`` fixture — mirroring the
    deployed fleet where the worker's client uses the global "orcest"
    prefix). Regression test for the bug where ``EventPublisher(redis)`` was
    built directly on the worker's own client, silently stranding events on
    a stream the per-project EventRelay never reads.
    """
    secret_token = "ghp_supersecrettoken12345"
    secret_credential = "sk-supersecretcredential67890"
    secret_prompt = "SECRET PROMPT TEXT do not leak me"
    task = Task.create(
        task_type=TaskType.FIX_PR,
        repo="owner/repo",
        token=secret_token,
        resource_type="issue",
        resource_id=7,
        prompt=secret_prompt,
        attempt=1,
        key_prefix="proj",
        credential=secret_credential,
        provider="claude",
    )

    harness = _run_task(
        fake_redis_client, RunnerResult(success=True, summary="All checks fixed"), task=task
    )

    # Nothing should have landed on the worker client's own prefix ("test:events").
    assert _spooled(harness.redis, "task.started") == []
    assert _spooled(harness.redis, "task.completed") == []

    # Events should be on the task's project stream: "proj:events".
    raw_entries = harness.redis.client.xrevrange("proj:events", count=50)
    envs = [json.loads(fields["envelope"]) for _id, fields in raw_entries]
    started = [e for e in envs if e["type"] == "net.orcest.task.started"]
    completed = [e for e in envs if e["type"] == "net.orcest.task.completed"]
    assert len(started) == 1
    assert len(completed) == 1

    # No secret material (token/credential/prompt) may appear anywhere in the
    # serialized envelopes.
    serialized = json.dumps(envs)
    assert secret_token not in serialized
    assert secret_credential not in serialized
    assert secret_prompt not in serialized
