"""Issue #613/#639: PoolManager composes and publishes stranded-stream
health snapshots for every provider task stream.

PoolManager is the single transition owner: for each configured provider
backend (``self._pool.worker_backends()``) it reads both the PR-task
stream (``tasks:{provider}``) and the issue-task stream
(``tasks:issue:{provider}``), feeds pending/lag/consumer counts through
``ProviderStreamHealthTracker`` (tests/shared/
test_provider_stream_health.py covers the pure dwell/transition logic in
isolation), and publishes a canonical snapshot to an unprefixed,
TTL-backed ``provider-stream-health:{provider}:pr`` /
``provider-stream-health:{provider}:issue`` JSON key. ``orcest status``
only ever reads those keys back (see dashboard.py/cli.py); these tests
only exercise the write side.
"""

from __future__ import annotations

import json
import time

import pytest

from orcest.fleet.pool_manager import _STREAM_HEALTH_TTL_SECONDS, PoolManager
from orcest.shared.models import CONSUMER_GROUP, Task, TaskType, task_stream_name
from orcest.shared.provider_stream_health import (
    STREAM_HEALTH_SNAPSHOT_VERSION,
    ProviderStreamHealth,
    StreamHealthState,
    stream_health_snapshot_key,
)

from .test_pool_manager import _make_config, _make_proxmox
from .test_pool_manager_activity import _write_heartbeat

pytestmark = pytest.mark.unit

_WORKER_ID = "orcest-worker-305"
_ISSUE_WORKER_ID = "orcest-worker-306"


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


def _read_state(rc, provider: str = "claude", *, issue: bool = False) -> dict | None:
    raw = rc.get_raw(stream_health_snapshot_key(provider, issue=issue))
    return json.loads(raw) if raw is not None else None


def _logical_stream(*, issue: bool = False) -> str:
    return task_stream_name("claude", issue=issue)


def _add_undelivered_entry(rc, *, issue: bool = False) -> None:
    """Create a provider task stream + its consumer group with one
    never-delivered entry: lag=1, pending=0, zero registered consumers."""
    stream = _logical_stream(issue=issue)
    task = Task.create(
        task_type=TaskType.FIX_CI,
        repo="owner/repo",
        token="ghp_x",
        resource_type="issue" if issue else "pr",
        resource_id=1,
        prompt="fix",
        branch="fix-branch",
        key_prefix="test",
    )
    rc.ensure_consumer_group(stream, CONSUMER_GROUP)
    rc.xadd(stream, task.to_dict())


def _claim(rc, worker_id: str = _WORKER_ID, *, issue: bool = False) -> str:
    """Deliver the next entry to *worker_id*, returning its entry ID."""
    claimed = rc.xreadgroup(
        group=CONSUMER_GROUP,
        consumer=worker_id,
        stream=_logical_stream(issue=issue),
        block_ms=None,
    )
    assert len(claimed) == 1
    return claimed[0][0]


def _strand(rc, worker_id: str, *, issue: bool) -> None:
    """Leave pending work on a stream with a registered, dead consumer."""
    _add_undelivered_entry(rc, issue=issue)
    _claim(rc, worker_id, issue=issue)


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
        issue_state = _read_state(fake_redis_client, issue=True)
        assert issue_state["state"] == "healthy"
        assert issue_state["stream"] == "test:tasks:issue:claude"
        assert issue_state["pending"] == 0

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
        assert _read_state(rc, issue=True) is None


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
        # Missing issue stream never calls XINFO, so it stays zero-work/healthy.
        assert _read_state(rc, issue=True)["state"] == "healthy"

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
        assert _read_state(rc, issue=True)["state"] == "healthy"


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

    def test_grouped_stream_regression_pending_still_reported_as_pending(self, fake_redis_client):
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
        pr_ttl = rc.client.ttl(stream_health_snapshot_key("claude"))
        issue_ttl = rc.client.ttl(stream_health_snapshot_key("claude", issue=True))
        assert 0 < pr_ttl <= 900
        assert 0 < issue_ttl <= 900


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
        assert _read_state(rc, provider="codex", issue=True) is None
        assert _read_state(rc, provider="claude") is not None
        assert _read_state(rc, provider="claude", issue=True) is not None


def _is_issue_stream(stream: str) -> bool:
    return ":tasks:issue:" in stream or stream.startswith("tasks:issue:")


class TestPrAndIssueIndependence:
    def test_pr_only_stranded_while_issue_missing_stays_healthy(self, fake_redis_client):
        rc = fake_redis_client
        _strand(rc, _WORKER_ID, issue=False)

        manager, _ = _build(rc, dwell_seconds=0)
        manager._check_stream_health()

        pr = _read_state(rc)
        issue = _read_state(rc, issue=True)
        assert pr["state"] == "stranded"
        assert pr["stream"] == "test:tasks:claude"
        assert pr["pending"] == 1
        assert issue["state"] == "healthy"
        assert issue["stream"] == "test:tasks:issue:claude"
        assert issue["pending"] == 0

    def test_issue_only_stranded_while_pr_missing_stays_healthy(self, fake_redis_client):
        rc = fake_redis_client
        _strand(rc, _ISSUE_WORKER_ID, issue=True)

        manager, _ = _build(rc, dwell_seconds=0)
        manager._check_stream_health()

        pr = _read_state(rc)
        issue = _read_state(rc, issue=True)
        assert pr["state"] == "healthy"
        assert pr["pending"] == 0
        assert issue["state"] == "stranded"
        assert issue["stream"] == "test:tasks:issue:claude"
        assert issue["pending"] == 1
        assert issue["live_consumers"] == 0

    def test_both_streams_stranded(self, fake_redis_client):
        rc = fake_redis_client
        _strand(rc, _WORKER_ID, issue=False)
        _strand(rc, _ISSUE_WORKER_ID, issue=True)

        manager, _ = _build(rc, dwell_seconds=0)
        manager._check_stream_health()

        assert _read_state(rc)["state"] == "stranded"
        assert _read_state(rc, issue=True)["state"] == "stranded"

    def test_mixed_healthy_and_stranded(self, fake_redis_client):
        rc = fake_redis_client
        _strand(rc, _WORKER_ID, issue=False)
        _add_undelivered_entry(rc, issue=True)
        _claim(rc, _ISSUE_WORKER_ID, issue=True)
        _write_heartbeat(rc, _ISSUE_WORKER_ID)

        manager, _ = _build(rc, dwell_seconds=0)
        manager._check_stream_health()

        pr = _read_state(rc)
        issue = _read_state(rc, issue=True)
        assert pr["state"] == "stranded"
        assert pr["live_consumers"] == 0
        assert issue["state"] == "healthy"
        assert issue["live_consumers"] == 1
        assert issue["pending"] == 1

    def test_independent_dwell_timers(self, fake_redis_client, monkeypatch):
        rc = fake_redis_client
        _strand(rc, _WORKER_ID, issue=False)

        manager, _ = _build(rc, dwell_seconds=300)
        now = time.time()
        monkeypatch.setattr(time, "time", lambda: now)
        manager._check_stream_health()
        assert _read_state(rc)["state"] == "healthy"

        _strand(rc, _ISSUE_WORKER_ID, issue=True)
        monkeypatch.setattr(time, "time", lambda: now + 200)
        manager._check_stream_health()
        assert _read_state(rc)["state"] == "healthy"
        assert _read_state(rc, issue=True)["state"] == "healthy"

        monkeypatch.setattr(time, "time", lambda: now + 300)
        manager._check_stream_health()
        assert _read_state(rc)["state"] == "stranded"
        assert _read_state(rc, issue=True)["state"] == "healthy"

        monkeypatch.setattr(time, "time", lambda: now + 500)
        manager._check_stream_health()
        assert _read_state(rc)["state"] == "stranded"
        assert _read_state(rc, issue=True)["state"] == "stranded"

    def test_independent_recovery(self, fake_redis_client):
        rc = fake_redis_client
        _strand(rc, _WORKER_ID, issue=False)
        _strand(rc, _ISSUE_WORKER_ID, issue=True)

        manager, _ = _build(rc, dwell_seconds=0)
        manager._check_stream_health()
        assert _read_state(rc)["state"] == "stranded"
        assert _read_state(rc, issue=True)["state"] == "stranded"

        _write_heartbeat(rc, _ISSUE_WORKER_ID)
        manager._check_stream_health()
        assert _read_state(rc)["state"] == "stranded"
        issue = _read_state(rc, issue=True)
        assert issue["state"] == "healthy"
        assert issue["live_consumers"] == 1


class TestPerStreamReadIsolation:
    def test_malformed_issue_xinfo_does_not_lose_pr_state(self, fake_redis_client, mocker):
        rc = fake_redis_client
        _strand(rc, _WORKER_ID, issue=False)
        _strand(rc, _ISSUE_WORKER_ID, issue=True)

        manager, _ = _build(rc, dwell_seconds=0)
        manager._check_stream_health()
        assert _read_state(rc)["state"] == "stranded"
        assert _read_state(rc, issue=True)["state"] == "stranded"

        real = rc.xinfo_groups_raw

        def _xinfo(stream: str):
            if _is_issue_stream(stream):
                return [{"name": CONSUMER_GROUP, "pending": "nope", "lag": 0}]
            return real(stream)

        mocker.patch.object(rc, "xinfo_groups_raw", side_effect=_xinfo)
        manager._check_stream_health()

        pr = _read_state(rc)
        issue = _read_state(rc, issue=True)
        assert pr["state"] == "stranded"
        assert pr["pending"] == 1
        assert issue["state"] == "stranded"
        assert issue["pending"] is None

    def test_read_error_on_issue_does_not_suppress_pr_or_abort(self, fake_redis_client, mocker):
        rc = fake_redis_client
        _strand(rc, _WORKER_ID, issue=False)
        _strand(rc, _ISSUE_WORKER_ID, issue=True)

        manager, _ = _build(rc, dwell_seconds=0)
        manager._check_stream_health()
        assert _read_state(rc)["state"] == "stranded"
        assert _read_state(rc, issue=True)["state"] == "stranded"

        real = rc.xinfo_groups_raw

        def _xinfo(stream: str):
            if _is_issue_stream(stream):
                raise RuntimeError("redis down")
            return real(stream)

        mocker.patch.object(rc, "xinfo_groups_raw", side_effect=_xinfo)
        manager._check_stream_health()

        pr = _read_state(rc)
        issue = _read_state(rc, issue=True)
        assert pr["state"] == "stranded"
        assert pr["pending"] == 1
        assert issue["state"] == "stranded"
        assert issue["pending"] is None

    def test_unexpected_exception_on_pr_still_publishes_issue(self, fake_redis_client, mocker):
        rc = fake_redis_client
        _strand(rc, _ISSUE_WORKER_ID, issue=True)

        manager, _ = _build(rc, dwell_seconds=0)
        real = manager._read_stream_health_inputs

        def _read(stream: str, heartbeat_cache):
            if not _is_issue_stream(stream):
                raise RuntimeError("boom")
            return real(stream, heartbeat_cache)

        mocker.patch.object(manager, "_read_stream_health_inputs", side_effect=_read)
        manager._check_stream_health()

        assert _read_state(rc) is None
        issue = _read_state(rc, issue=True)
        assert issue is not None
        assert issue["state"] == "stranded"
        assert issue["stream"] == "test:tasks:issue:claude"


def _stranded_snapshot(*, issue: bool = False, **overrides) -> ProviderStreamHealth:
    stream = "test:tasks:issue:claude" if issue else "test:tasks:claude"
    fields = dict(
        provider="claude",
        stream=stream,
        pending=1,
        lag=0,
        registered_consumers=1,
        live_consumers=0,
        state=StreamHealthState.STRANDED,
        observed_at=1_700_000_000.0,
        transitioned_at=1_700_000_000.0 - 400.0,
    )
    fields.update(overrides)
    return ProviderStreamHealth(**fields)


def _put_committed_snapshot(
    rc,
    health: ProviderStreamHealth,
    *,
    issue: bool = False,
    ttl: int = _STREAM_HEALTH_TTL_SECONDS,
    persist: bool = False,
    payload: dict | None = None,
) -> str:
    key = stream_health_snapshot_key(health.provider, issue=issue)
    raw = json.dumps(payload if payload is not None else health.to_dict())
    if persist:
        rc.client.set(key, raw)
    elif ttl > 0:
        rc.set_ex_raw(key, raw, ttl)
    else:
        rc.client.set(key, raw)
        rc.client.expire(key, 0)
    return key


class TestRestartRestore:
    def test_restart_while_still_stranded_preserves_transitioned_at(
        self, fake_redis_client, monkeypatch
    ):
        rc = fake_redis_client
        _strand(rc, _WORKER_ID, issue=False)

        manager, _ = _build(rc, dwell_seconds=0)
        now = 1_700_000_000.0
        monkeypatch.setattr(time, "time", lambda: now)
        manager._check_stream_health()
        first = _read_state(rc)
        assert first["state"] == "stranded"
        assert first["version"] == STREAM_HEALTH_SNAPSHOT_VERSION
        transitioned_at = first["transitioned_at"]

        restarted, _ = _build(rc, dwell_seconds=300)
        monkeypatch.setattr(time, "time", lambda: now + 10)
        restarted._check_stream_health()
        second = _read_state(rc)
        assert second["state"] == "stranded"
        assert second["transitioned_at"] == transitioned_at
        assert second["pending"] == 1
        assert _read_state(rc, issue=True)["state"] == "healthy"

    def test_restart_then_healthy_recovers_once(self, fake_redis_client, monkeypatch, caplog):
        rc = fake_redis_client
        _strand(rc, _WORKER_ID, issue=False)

        manager, _ = _build(rc, dwell_seconds=0)
        now = 1_700_000_000.0
        monkeypatch.setattr(time, "time", lambda: now)
        manager._check_stream_health()
        assert _read_state(rc)["state"] == "stranded"

        _write_heartbeat(rc, _WORKER_ID)
        restarted, _ = _build(rc, dwell_seconds=300)
        monkeypatch.setattr(time, "time", lambda: now + 10)
        with caplog.at_level("INFO"):
            restarted._check_stream_health()
        recovered = _read_state(rc)
        assert recovered["state"] == "healthy"
        assert recovered["live_consumers"] == 1
        assert recovered["transitioned_at"] == now + 10
        recoveries = [
            r for r in caplog.records if r.levelname == "INFO" and "recovered" in r.getMessage()
        ]
        assert len(recoveries) == 1

        caplog.clear()
        monkeypatch.setattr(time, "time", lambda: now + 20)
        with caplog.at_level("INFO"):
            restarted._check_stream_health()
        assert _read_state(rc)["state"] == "healthy"
        assert _read_state(rc)["transitioned_at"] == now + 10
        assert not [
            r for r in caplog.records if r.levelname == "INFO" and "recovered" in r.getMessage()
        ]

    def test_restart_with_unreadable_inputs_keeps_stranded(
        self, fake_redis_client, monkeypatch, mocker
    ):
        rc = fake_redis_client
        _strand(rc, _WORKER_ID, issue=False)

        manager, _ = _build(rc, dwell_seconds=0)
        now = 1_700_000_000.0
        monkeypatch.setattr(time, "time", lambda: now)
        manager._check_stream_health()
        transitioned_at = _read_state(rc)["transitioned_at"]

        mocker.patch.object(rc, "xinfo_groups_raw", side_effect=RuntimeError("redis down"))
        restarted, _ = _build(rc, dwell_seconds=300)
        monkeypatch.setattr(time, "time", lambda: now + 10)
        restarted._check_stream_health()
        state = _read_state(rc)
        assert state["state"] == "stranded"
        assert state["pending"] is None
        assert state["transitioned_at"] == transitioned_at
        assert _read_state(rc, issue=True)["state"] == "healthy"

    def test_healthy_snapshot_does_not_restore_dwell_candidate(
        self, fake_redis_client, monkeypatch
    ):
        rc = fake_redis_client
        _strand(rc, _WORKER_ID, issue=False)
        now = 1_700_000_000.0
        healthy = _stranded_snapshot(
            state=StreamHealthState.HEALTHY,
            live_consumers=1,
            observed_at=now - 10,
            transitioned_at=now - 50,
        )
        _put_committed_snapshot(rc, healthy)
        monkeypatch.setattr(time, "time", lambda: now)

        manager, _ = _build(rc, dwell_seconds=300)
        manager._check_stream_health()
        assert _read_state(rc)["state"] == "healthy"

        monkeypatch.setattr(time, "time", lambda: now + 299)
        manager._check_stream_health()
        assert _read_state(rc)["state"] == "healthy"

        monkeypatch.setattr(time, "time", lambda: now + 300)
        manager._check_stream_health()
        stranded = _read_state(rc)
        assert stranded["state"] == "stranded"
        assert stranded["transitioned_at"] == now + 300

    def test_missing_snapshot_does_not_bypass_dwell(self, fake_redis_client, monkeypatch):
        rc = fake_redis_client
        _strand(rc, _WORKER_ID, issue=False)
        now = 1_700_000_000.0
        monkeypatch.setattr(time, "time", lambda: now)
        manager, _ = _build(rc, dwell_seconds=300)
        manager._check_stream_health()
        assert _read_state(rc)["state"] == "healthy"

    def test_non_expiring_snapshot_does_not_restore(self, fake_redis_client, monkeypatch):
        rc = fake_redis_client
        _strand(rc, _WORKER_ID, issue=False)
        now = 1_700_000_000.0
        health = _stranded_snapshot(observed_at=now - 10, transitioned_at=now - 400)
        _put_committed_snapshot(rc, health, persist=True)
        assert rc.client.ttl(stream_health_snapshot_key("claude")) == -1

        monkeypatch.setattr(time, "time", lambda: now)
        manager, _ = _build(rc, dwell_seconds=300)
        manager._check_stream_health()
        assert _read_state(rc)["state"] == "healthy"

    def test_expired_ttl_does_not_restore(self, fake_redis_client, monkeypatch, mocker):
        rc = fake_redis_client
        _strand(rc, _WORKER_ID, issue=False)
        now = 1_700_000_000.0
        health = _stranded_snapshot(observed_at=now - 10, transitioned_at=now - 400)
        payload = json.dumps(health.to_dict())
        _put_committed_snapshot(rc, health)

        manager, _ = _build(rc, dwell_seconds=300)
        mocker.patch.object(
            manager, "_read_stream_health_snapshot_record", return_value=(payload, 0)
        )
        monkeypatch.setattr(time, "time", lambda: now)
        manager._check_stream_health()
        assert _read_state(rc)["state"] == "healthy"

    def test_malformed_snapshot_does_not_restore(self, fake_redis_client, monkeypatch):
        rc = fake_redis_client
        _strand(rc, _WORKER_ID, issue=False)
        rc.set_ex_raw(stream_health_snapshot_key("claude"), "{not json", 900)
        now = 1_700_000_000.0
        monkeypatch.setattr(time, "time", lambda: now)
        manager, _ = _build(rc, dwell_seconds=300)
        manager._check_stream_health()
        assert _read_state(rc)["state"] == "healthy"

    def test_identity_mismatch_does_not_restore(self, fake_redis_client, monkeypatch):
        rc = fake_redis_client
        _strand(rc, _WORKER_ID, issue=False)
        now = 1_700_000_000.0
        health = _stranded_snapshot(observed_at=now - 10, transitioned_at=now - 400)
        payload = health.to_dict()
        payload["stream"] = "test:tasks:issue:claude"
        _put_committed_snapshot(rc, health, payload=payload)
        monkeypatch.setattr(time, "time", lambda: now)
        manager, _ = _build(rc, dwell_seconds=300)
        manager._check_stream_health()
        assert _read_state(rc)["state"] == "healthy"

    def test_unsupported_version_does_not_restore(self, fake_redis_client, monkeypatch):
        rc = fake_redis_client
        _strand(rc, _WORKER_ID, issue=False)
        now = 1_700_000_000.0
        health = _stranded_snapshot(observed_at=now - 10, transitioned_at=now - 400)
        payload = health.to_dict()
        payload["version"] = 2
        _put_committed_snapshot(rc, health, payload=payload)
        monkeypatch.setattr(time, "time", lambda: now)
        manager, _ = _build(rc, dwell_seconds=300)
        manager._check_stream_health()
        assert _read_state(rc)["state"] == "healthy"

    def test_future_timestamps_do_not_restore(self, fake_redis_client, monkeypatch):
        rc = fake_redis_client
        _strand(rc, _WORKER_ID, issue=False)
        now = 1_700_000_000.0
        future = _stranded_snapshot(observed_at=now + 50, transitioned_at=now - 10)
        _put_committed_snapshot(rc, future)
        monkeypatch.setattr(time, "time", lambda: now)
        manager, _ = _build(rc, dwell_seconds=300)
        manager._check_stream_health()
        assert _read_state(rc)["state"] == "healthy"

    def test_reversed_timestamps_do_not_restore(self, fake_redis_client, monkeypatch):
        rc = fake_redis_client
        _strand(rc, _WORKER_ID, issue=False)
        now = 1_700_000_000.0
        reversed_ts = _stranded_snapshot(observed_at=now - 50, transitioned_at=now - 10)
        _put_committed_snapshot(rc, reversed_ts)
        monkeypatch.setattr(time, "time", lambda: now)
        manager, _ = _build(rc, dwell_seconds=300)
        manager._check_stream_health()
        assert _read_state(rc)["state"] == "healthy"

    def test_redis_restore_read_failure_does_not_abort_or_clobber_snapshot(
        self, fake_redis_client, monkeypatch, mocker
    ):
        rc = fake_redis_client
        _strand(rc, _WORKER_ID, issue=False)
        now = 1_700_000_000.0
        health = _stranded_snapshot(observed_at=now - 10, transitioned_at=now - 400)
        _put_committed_snapshot(rc, health)

        manager, _ = _build(rc, dwell_seconds=300)
        mocker.patch.object(
            manager,
            "_read_stream_health_snapshot_record",
            side_effect=RuntimeError("redis down"),
        )
        monkeypatch.setattr(time, "time", lambda: now)
        manager._check_stream_health()
        state = _read_state(rc)
        assert state["state"] == "stranded"
        assert state["transitioned_at"] == health.transitioned_at
        assert _read_state(rc, issue=True) is None
        assert manager._stream_health_tracker.has_state("claude", "test:tasks:claude") is False

    def test_restore_read_failure_on_pr_still_evaluates_issue(
        self, fake_redis_client, monkeypatch, mocker
    ):
        rc = fake_redis_client
        _strand(rc, _ISSUE_WORKER_ID, issue=True)
        now = 1_700_000_000.0
        monkeypatch.setattr(time, "time", lambda: now)
        manager, _ = _build(rc, dwell_seconds=0)
        real = manager._read_stream_health_snapshot_record

        def _read(key: str):
            if key == stream_health_snapshot_key("claude"):
                raise RuntimeError("redis down")
            return real(key)

        mocker.patch.object(manager, "_read_stream_health_snapshot_record", side_effect=_read)
        manager._check_stream_health()
        assert _read_state(rc, issue=True)["state"] == "stranded"
        assert _read_state(rc) is None

    def test_transient_restore_read_failure_retries_committed_snapshot(
        self, fake_redis_client, monkeypatch, mocker
    ):
        rc = fake_redis_client
        _strand(rc, _WORKER_ID, issue=False)

        manager, _ = _build(rc, dwell_seconds=0)
        now = 1_700_000_000.0
        monkeypatch.setattr(time, "time", lambda: now)
        manager._check_stream_health()
        first = _read_state(rc)
        assert first["state"] == "stranded"
        transitioned_at = first["transitioned_at"]

        restarted, _ = _build(rc, dwell_seconds=300)
        real = restarted._read_stream_health_snapshot_record
        fail_pr = {"value": True}

        def _read(key: str):
            if fail_pr["value"] and key == stream_health_snapshot_key("claude"):
                return None
            return real(key)

        mocker.patch.object(restarted, "_read_stream_health_snapshot_record", side_effect=_read)
        monkeypatch.setattr(time, "time", lambda: now + 10)
        restarted._check_stream_health()
        blip = _read_state(rc)
        assert blip["state"] == "stranded"
        assert blip["transitioned_at"] == transitioned_at
        assert restarted._stream_health_tracker.has_state("claude", "test:tasks:claude") is False

        fail_pr["value"] = False
        monkeypatch.setattr(time, "time", lambda: now + 20)
        restarted._check_stream_health()
        restored = _read_state(rc)
        assert restored["state"] == "stranded"
        assert restored["transitioned_at"] == transitioned_at
        assert restored["pending"] == 1
        assert _read_state(rc, issue=True)["state"] == "healthy"

    def test_restore_retry_gives_up_after_freshness_window(
        self, fake_redis_client, monkeypatch, mocker
    ):
        rc = fake_redis_client
        _strand(rc, _WORKER_ID, issue=False)
        now = 1_700_000_000.0
        health = _stranded_snapshot(observed_at=now - 10, transitioned_at=now - 400)
        _put_committed_snapshot(rc, health)

        manager, _ = _build(rc, dwell_seconds=300)
        real = manager._read_stream_health_snapshot_record
        fail = {"value": True}
        reads = {"n": 0}

        def _read(key: str):
            reads["n"] += 1
            if fail["value"]:
                return None
            return real(key)

        mocker.patch.object(manager, "_read_stream_health_snapshot_record", side_effect=_read)
        monkeypatch.setattr(time, "time", lambda: now)
        manager._check_stream_health()
        assert _read_state(rc)["state"] == "stranded"
        reads_after_first = reads["n"]
        assert reads_after_first >= 1

        monkeypatch.setattr(time, "time", lambda: now + _STREAM_HEALTH_TTL_SECONDS)
        manager._check_stream_health()
        assert _read_state(rc)["state"] == "stranded"
        assert reads["n"] > reads_after_first

        monkeypatch.setattr(time, "time", lambda: now + _STREAM_HEALTH_TTL_SECONDS + 1)
        manager._check_stream_health()
        assert _read_state(rc)["state"] == "healthy"
        reads_after_give_up = reads["n"]

        fail["value"] = False
        monkeypatch.setattr(time, "time", lambda: now + _STREAM_HEALTH_TTL_SECONDS + 2)
        manager._check_stream_health()
        assert _read_state(rc)["state"] == "healthy"
        assert reads["n"] == reads_after_give_up


def _failing_snapshot_pipeline(manager, monkeypatch, fail: dict[str, bool] | None = None):
    """Make stream-health snapshot pipeline reads raise while *fail* is true."""
    original = manager._redis.client.pipeline
    active = fail if fail is not None else {"value": True}

    def _pipeline(*args, **kwargs):
        if active["value"]:
            raise ConnectionError("redis down")
        return original(*args, **kwargs)

    monkeypatch.setattr(manager._redis.client, "pipeline", _pipeline)
    return active


def _snapshot_read_warnings(caplog, key: str | None = None):
    records = [
        record
        for record in caplog.records
        if record.levelname == "WARNING"
        and "Failed to read stream health snapshot" in record.getMessage()
    ]
    if key is not None:
        records = [record for record in records if key in record.getMessage()]
    return records


class TestRestoreReadFailureLogDecimation:
    def test_ninety_consecutive_failures_warn_only_at_one_and_ten(
        self, fake_redis_client, monkeypatch, caplog
    ):
        manager, _ = _build(fake_redis_client)
        _failing_snapshot_pipeline(manager, monkeypatch)
        key = stream_health_snapshot_key("claude")

        with caplog.at_level("WARNING"):
            for _ in range(90):
                assert manager._read_stream_health_snapshot_record(key) is None

        warnings = _snapshot_read_warnings(caplog, key)
        assert len(warnings) == 2
        assert "1 consecutive failures" in warnings[0].getMessage()
        assert "10 consecutive failures" in warnings[1].getMessage()
        assert all(record.exc_info for record in warnings)
        assert manager._stream_health_restore_read_failures[key] == 90

    def test_independent_snapshot_keys_have_independent_counters(
        self, fake_redis_client, monkeypatch, caplog
    ):
        manager, _ = _build(fake_redis_client)
        _failing_snapshot_pipeline(manager, monkeypatch)
        pr_key = stream_health_snapshot_key("claude")
        issue_key = stream_health_snapshot_key("claude", issue=True)

        with caplog.at_level("WARNING"):
            for _ in range(9):
                assert manager._read_stream_health_snapshot_record(pr_key) is None
            assert manager._read_stream_health_snapshot_record(issue_key) is None

        pr_warnings = _snapshot_read_warnings(caplog, pr_key)
        issue_warnings = _snapshot_read_warnings(caplog, issue_key)
        assert len(pr_warnings) == 1
        assert "1 consecutive failures" in pr_warnings[0].getMessage()
        assert len(issue_warnings) == 1
        assert "1 consecutive failures" in issue_warnings[0].getMessage()
        assert manager._stream_health_restore_read_failures[pr_key] == 9
        assert manager._stream_health_restore_read_failures[issue_key] == 1

    def test_successful_read_resets_failure_counter(self, fake_redis_client, monkeypatch, caplog):
        manager, _ = _build(fake_redis_client)
        fail = _failing_snapshot_pipeline(manager, monkeypatch)
        key = stream_health_snapshot_key("claude")

        with caplog.at_level("WARNING"):
            assert manager._read_stream_health_snapshot_record(key) is None
        assert len(_snapshot_read_warnings(caplog, key)) == 1
        assert manager._stream_health_restore_read_failures[key] == 1

        fail["value"] = False
        caplog.clear()
        record = manager._read_stream_health_snapshot_record(key)
        assert record is not None
        assert key not in manager._stream_health_restore_read_failures
        assert _snapshot_read_warnings(caplog, key) == []

        fail["value"] = True
        with caplog.at_level("WARNING"):
            assert manager._read_stream_health_snapshot_record(key) is None
        warnings = _snapshot_read_warnings(caplog, key)
        assert len(warnings) == 1
        assert "1 consecutive failures" in warnings[0].getMessage()
        assert manager._stream_health_restore_read_failures[key] == 1

    def test_deadline_abandonment_discards_failure_counter(
        self, fake_redis_client, monkeypatch, caplog
    ):
        rc = fake_redis_client
        _strand(rc, _WORKER_ID, issue=False)
        now = 1_700_000_000.0
        health = _stranded_snapshot(observed_at=now - 10, transitioned_at=now - 400)
        _put_committed_snapshot(rc, health)

        manager, _ = _build(rc, dwell_seconds=300)
        _failing_snapshot_pipeline(manager, monkeypatch)
        key = stream_health_snapshot_key("claude")
        stream = "test:tasks:claude"

        assert manager._restore_committed_stream_health("claude", stream, now, issue=False) is False
        assert manager._stream_health_restore_read_failures[key] == 1

        with caplog.at_level("WARNING"):
            assert (
                manager._restore_committed_stream_health(
                    "claude", stream, now + _STREAM_HEALTH_TTL_SECONDS + 1, issue=False
                )
                is True
            )
        assert key not in manager._stream_health_restore_read_failures
        assert any(
            "Giving up stream health restore" in record.getMessage() for record in caplog.records
        )

    def test_failed_read_does_not_double_warn_in_caller(
        self, fake_redis_client, monkeypatch, caplog
    ):
        manager, _ = _build(fake_redis_client)
        _failing_snapshot_pipeline(manager, monkeypatch)
        key = stream_health_snapshot_key("claude")

        with caplog.at_level("WARNING"):
            assert (
                manager._restore_committed_stream_health(
                    "claude", "test:tasks:claude", 1_700_000_000.0, issue=False
                )
                is False
            )

        failure_warnings = [
            record
            for record in caplog.records
            if record.levelname == "WARNING" and "Failed to" in record.getMessage()
        ]
        assert len(failure_warnings) == 1
        assert key in failure_warnings[0].getMessage()
        assert "Failed to restore stream health" not in failure_warnings[0].getMessage()

    def test_restore_keeps_retrying_during_decimated_failures(
        self, fake_redis_client, monkeypatch, caplog
    ):
        rc = fake_redis_client
        _strand(rc, _WORKER_ID, issue=False)
        now = 1_700_000_000.0
        health = _stranded_snapshot(observed_at=now - 10, transitioned_at=now - 400)
        _put_committed_snapshot(rc, health)

        manager, _ = _build(rc, dwell_seconds=300)
        _failing_snapshot_pipeline(manager, monkeypatch)
        stream = "test:tasks:claude"

        with caplog.at_level("WARNING"):
            for _ in range(90):
                assert (
                    manager._restore_committed_stream_health("claude", stream, now, issue=False)
                    is False
                )

        assert _read_state(rc)["state"] == "stranded"
        assert _read_state(rc)["transitioned_at"] == health.transitioned_at
        assert manager._stream_health_tracker.has_state("claude", stream) is False
        assert manager._restore_committed_stream_health("claude", stream, now, issue=False) is False
        key = stream_health_snapshot_key("claude")
        warnings = _snapshot_read_warnings(caplog, key)
        assert len(warnings) == 2
        assert manager._stream_health_restore_read_failures[key] == 91
