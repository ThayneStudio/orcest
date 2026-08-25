"""Issue #613: PoolManager composes and publishes stranded-provider-stream
health snapshots.

PoolManager is the single transition owner: for each configured provider
backend (``self._pool.worker_backends()``) it reads the stream's pending
count, lag, registered consumers, and heartbeat-backed live consumers,
feeds them through ``ProviderStreamHealthTracker`` (tests/shared/
test_provider_stream_health.py covers the pure dwell/transition logic in
isolation), and publishes the canonical snapshot to an unprefixed,
TTL-backed ``provider-stream-health:{provider}`` JSON key. ``orcest
status`` only ever reads that key back (see dashboard.py/cli.py); these
tests only exercise the write side.
"""

from __future__ import annotations

import json
import time

import pytest

from orcest.fleet.pool_manager import PoolManager
from orcest.shared.models import CONSUMER_GROUP, Task, TaskType

from .test_pool_manager import _make_config, _make_proxmox
from .test_pool_manager_activity import _write_heartbeat

pytestmark = pytest.mark.unit

_WORKER_ID = "orcest-worker-305"


def _build(fake_redis_client, dwell_seconds: int = 300):
    config = _make_config(vm_id_start=300)
    config.pool.vm_id_end = 399
    config.pool.stream_health_dwell_seconds = dwell_seconds
    proxmox = _make_proxmox()
    manager = PoolManager(
        config=config,
        proxmox=proxmox,
        redis=fake_redis_client,
        key_prefix="test",
    )
    return manager, proxmox


def _read_state(rc, provider: str = "claude") -> dict | None:
    raw = rc.get_raw(f"provider-stream-health:{provider}")
    return json.loads(raw) if raw is not None else None


def _add_undelivered_entry(rc) -> None:
    """Create tasks:claude + its consumer group with one never-delivered
    entry: lag=1, pending=0, zero registered consumers."""
    task = Task.create(
        task_type=TaskType.FIX_CI,
        repo="owner/repo",
        token="ghp_x",
        resource_type="pr",
        resource_id=1,
        prompt="fix",
        branch="fix-branch",
        key_prefix="test",
    )
    rc.ensure_consumer_group("tasks:claude", CONSUMER_GROUP)
    rc.xadd("tasks:claude", task.to_dict())


def _claim(rc, worker_id: str = _WORKER_ID) -> str:
    """Deliver the next entry to *worker_id*, returning its entry ID."""
    claimed = rc.xreadgroup(
        group=CONSUMER_GROUP, consumer=worker_id, stream="tasks:claude", block_ms=None
    )
    assert len(claimed) == 1
    return claimed[0][0]


class TestNoFalseAlarms:
    def test_no_stream_yet_is_healthy(self, fake_redis_client):
        manager, _ = _build(fake_redis_client)
        manager._check_stream_health()
        state = _read_state(fake_redis_client)
        assert state["state"] == "healthy"
        assert state["pending"] == 0
        assert state["lag"] == 0
        assert state["provider"] == "claude"
        assert state["stream"] == "test:tasks:claude"

    def test_group_with_no_work_is_healthy(self, fake_redis_client):
        rc = fake_redis_client
        rc.ensure_consumer_group("tasks:claude", CONSUMER_GROUP)
        manager, _ = _build(rc)
        manager._check_stream_health()
        assert _read_state(rc)["state"] == "healthy"

    def test_work_with_live_consumer_is_healthy(self, fake_redis_client):
        rc = fake_redis_client
        _add_undelivered_entry(rc)
        entry_id = _claim(rc)
        rc.xack("tasks:claude", CONSUMER_GROUP, entry_id)
        _write_heartbeat(rc, _WORKER_ID)
        _add_undelivered_entry(rc)  # a second, still-undelivered entry -> lag=1

        manager, _ = _build(rc)
        manager._check_stream_health()
        state = _read_state(rc)
        assert state["state"] == "healthy"
        assert state["lag"] == 1
        assert state["registered_consumers"] == 1
        assert state["live_consumers"] == 1

    def test_disabled_toggle_publishes_nothing(self, fake_redis_client):
        rc = fake_redis_client
        _add_undelivered_entry(rc)
        manager, _ = _build(rc)
        manager._pool.stream_health_enabled = False
        manager._check_stream_health()
        assert _read_state(rc) is None


class TestDwellAndStranding:
    def test_pending_with_dead_consumer_does_not_alert_before_dwell(
        self, fake_redis_client, monkeypatch
    ):
        rc = fake_redis_client
        _add_undelivered_entry(rc)
        _claim(rc)  # registered consumer, no heartbeat written -> dead

        manager, _ = _build(rc, dwell_seconds=300)
        now = time.time()
        monkeypatch.setattr(time, "time", lambda: now)
        manager._check_stream_health()

        state = _read_state(rc)
        assert state["state"] == "healthy"
        assert state["pending"] == 1
        assert state["registered_consumers"] == 1
        assert state["live_consumers"] == 0

    def test_pending_with_dead_consumer_strands_after_dwell(self, fake_redis_client, monkeypatch):
        rc = fake_redis_client
        _add_undelivered_entry(rc)
        _claim(rc)

        manager, _ = _build(rc, dwell_seconds=300)
        now = time.time()
        monkeypatch.setattr(time, "time", lambda: now)
        manager._check_stream_health()
        assert _read_state(rc)["state"] == "healthy"

        monkeypatch.setattr(time, "time", lambda: now + 300)
        manager._check_stream_health()
        state = _read_state(rc)
        assert state["state"] == "stranded"
        assert state["pending"] == 1
        assert state["live_consumers"] == 0

    def test_registered_but_dead_consumer_does_not_count_as_live(
        self, fake_redis_client, monkeypatch
    ):
        """A consumer name registered on the group with no
        workers:heartbeat:{id} key must not count toward live_consumers,
        even though it is a 'registered consumer'."""
        rc = fake_redis_client
        _add_undelivered_entry(rc)
        _claim(rc)
        # No heartbeat written for _WORKER_ID.

        manager, _ = _build(rc, dwell_seconds=0)
        manager._check_stream_health()
        state = _read_state(rc)
        assert state["registered_consumers"] == 1
        assert state["live_consumers"] == 0
        assert state["state"] == "stranded"


class TestRecovery:
    def test_recovery_when_heartbeat_appears(self, fake_redis_client, monkeypatch):
        rc = fake_redis_client
        _add_undelivered_entry(rc)
        _claim(rc)

        manager, _ = _build(rc, dwell_seconds=0)
        manager._check_stream_health()
        assert _read_state(rc)["state"] == "stranded"

        _write_heartbeat(rc, _WORKER_ID)
        manager._check_stream_health()
        state = _read_state(rc)
        assert state["state"] == "healthy"
        assert state["live_consumers"] == 1


class TestRedisReadFailure:
    def test_read_error_marks_unknown_when_no_prior_state(self, fake_redis_client, mocker):
        rc = fake_redis_client
        _add_undelivered_entry(rc)
        mocker.patch.object(rc, "xinfo_groups_raw", side_effect=RuntimeError("redis down"))

        manager, _ = _build(rc)
        manager._check_stream_health()
        state = _read_state(rc)
        assert state["state"] == "unknown"
        assert state["pending"] is None

    def test_read_error_preserves_stranded_state_and_never_false_recovers(
        self, fake_redis_client, mocker
    ):
        rc = fake_redis_client
        _add_undelivered_entry(rc)
        _claim(rc)

        manager, _ = _build(rc, dwell_seconds=0)
        manager._check_stream_health()
        assert _read_state(rc)["state"] == "stranded"

        mocker.patch.object(rc, "xinfo_groups_raw", side_effect=RuntimeError("redis down"))
        manager._check_stream_health()
        state = _read_state(rc)
        assert state["state"] == "stranded"
        assert state["pending"] is None


class TestGroupLessStreamSyntheticLag:
    """Issue #635: a stream with no ``workers`` consumer group has never
    delivered any entry, so Redis has neither a PEL nor group lag for it.
    Those entries must surface as synthetic lag, not as ``pending``."""

    def _add_raw_entries(self, rc, count: int) -> None:
        for _ in range(count):
            task = Task.create(
                task_type=TaskType.FIX_CI,
                repo="owner/repo",
                token="ghp_x",
                resource_type="pr",
                resource_id=1,
                prompt="fix",
                branch="fix-branch",
                key_prefix="test",
            )
            rc.xadd("tasks:claude", task.to_dict())

    def test_group_less_stream_reports_synthetic_lag_not_pending(self, fake_redis_client):
        rc = fake_redis_client
        self._add_raw_entries(rc, 2)

        manager, _ = _build(rc)
        manager._check_stream_health()
        state = _read_state(rc)
        assert state["pending"] == 0
        assert state["lag"] == 2
        assert state["registered_consumers"] == 0
        assert state["live_consumers"] == 0

    def test_group_less_stream_strands_after_dwell_with_no_live_consumer(
        self, fake_redis_client, monkeypatch
    ):
        rc = fake_redis_client
        self._add_raw_entries(rc, 1)

        manager, _ = _build(rc, dwell_seconds=300)
        now = time.time()
        monkeypatch.setattr(time, "time", lambda: now)
        manager._check_stream_health()
        assert _read_state(rc)["state"] == "healthy"

        monkeypatch.setattr(time, "time", lambda: now + 300)
        manager._check_stream_health()
        state = _read_state(rc)
        assert state["state"] == "stranded"
        assert state["pending"] == 0
        assert state["lag"] == 1

    def test_missing_stream_stays_zero_work_and_non_stranded(self, fake_redis_client):
        manager, _ = _build(fake_redis_client)
        manager._check_stream_health()
        state = _read_state(fake_redis_client)
        assert state["state"] == "healthy"
        assert state["pending"] == 0
        assert state["lag"] == 0

    def test_xlen_failure_is_a_read_error_not_healthy_zero_work(self, fake_redis_client, mocker):
        rc = fake_redis_client
        self._add_raw_entries(rc, 1)
        mocker.patch.object(rc.client, "xlen", side_effect=RuntimeError("redis down"))

        manager, _ = _build(rc)
        manager._check_stream_health()
        state = _read_state(rc)
        assert state["state"] == "unknown"
        assert state["pending"] is None
        assert state["lag"] is None

    def test_grouped_stream_regression_pending_still_reported_as_pending(
        self, fake_redis_client
    ):
        """A grouped stream's undelivered entries remain Redis-reported
        group lag, not synthetic lag, and delivered-but-unacked entries
        remain pending -- this codepath must be untouched by #635."""
        rc = fake_redis_client
        _add_undelivered_entry(rc)
        _claim(rc)

        manager, _ = _build(rc)
        manager._check_stream_health()
        state = _read_state(rc)
        assert state["pending"] == 1
        assert state["lag"] == 0
        assert state["registered_consumers"] == 1


class TestStateTTL:
    def test_published_state_has_a_ttl(self, fake_redis_client):
        rc = fake_redis_client
        manager, _ = _build(rc)
        manager._check_stream_health()
        ttl = rc.client.ttl("provider-stream-health:claude")
        assert 0 < ttl <= 900


class TestConfiguredProviderNames:
    def test_uses_worker_backends_not_a_hardcoded_prefix_scan(self, fake_redis_client):
        """A stream for a provider that is not part of the fleet's
        configured backends must never be checked or published, even if
        Redis happens to contain a 'tasks:*' stream for it."""
        rc = fake_redis_client
        rc.ensure_consumer_group("tasks:codex", CONSUMER_GROUP)
        task = Task.create(
            task_type=TaskType.FIX_CI,
            repo="owner/repo",
            token="ghp_x",
            resource_type="pr",
            resource_id=1,
            prompt="fix",
            branch="fix-branch",
            key_prefix="test",
        )
        rc.xadd("tasks:codex", task.to_dict())

        manager, _ = _build(rc)
        assert manager._pool.worker_backends() == {"claude"}
        manager._check_stream_health()

        assert _read_state(rc, provider="codex") is None
        assert _read_state(rc, provider="claude") is not None
