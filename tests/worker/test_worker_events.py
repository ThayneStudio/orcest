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


def _make_task() -> Task:
    # resource_type="issue" skips the PR head-sha snapshot validation path
    # entirely, so the harness doesn't need to stub ``gh.get_pr``.
    return Task.create(
        task_type=TaskType.FIX_PR,
        repo="owner/repo",
        token="test-token-events",
        resource_type="issue",
        resource_id=7,
        prompt="Do the thing",
        attempt=1,
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


def _run_task(fake_redis_client, runner_result: RunnerResult) -> SimpleNamespace:
    config = _make_config()
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

    return SimpleNamespace(redis=fake_redis_client, worker_id=config.worker_id)


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
