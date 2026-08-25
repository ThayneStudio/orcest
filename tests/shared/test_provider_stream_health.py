"""Pure state-machine tests for the stranded provider-stream detector.

ProviderStreamHealthTracker takes an explicit ``now`` and performs no I/O,
so dwell/recovery/read-error behavior is tested without a real clock or
Redis. See fleet/pool_manager.py's ``_check_stream_health`` for the Redis
glue that feeds this tracker.
"""

from __future__ import annotations

import pytest

from orcest.shared.provider_stream_health import (
    STREAM_HEALTH_KEY_PREFIX,
    ProviderStreamHealth,
    ProviderStreamHealthTracker,
    StreamHealthState,
    stream_health_snapshot_key,
)

pytestmark = pytest.mark.unit

_PR_STREAM = "test:tasks:claude"
_ISSUE_STREAM = "test:tasks:issue:claude"


def _evaluate(tracker, now, *, provider="claude", stream=_PR_STREAM, **kwargs):
    defaults = dict(
        pending=0,
        lag=0,
        registered_consumers=0,
        live_consumers=0,
        read_error=False,
    )
    defaults.update(kwargs)
    return tracker.evaluate(provider, stream, now=now, **defaults)


class TestHealthyEmptyUnknown:
    def test_no_work_is_healthy_and_no_transition(self):
        tracker = ProviderStreamHealthTracker(dwell_seconds=300)
        snapshot, transition = _evaluate(tracker, now=1000.0)
        assert snapshot.state == StreamHealthState.HEALTHY
        assert transition is None

    def test_work_with_live_consumer_is_healthy(self):
        tracker = ProviderStreamHealthTracker(dwell_seconds=300)
        snapshot, transition = _evaluate(
            tracker, now=1000.0, pending=5, registered_consumers=1, live_consumers=1
        )
        assert snapshot.state == StreamHealthState.HEALTHY
        assert transition is None

    def test_first_read_error_with_no_prior_state_is_unknown(self):
        tracker = ProviderStreamHealthTracker(dwell_seconds=300)
        snapshot, transition = _evaluate(
            tracker,
            now=1000.0,
            pending=None,
            lag=None,
            registered_consumers=None,
            live_consumers=None,
            read_error=True,
        )
        assert snapshot.state == StreamHealthState.UNKNOWN
        assert transition is None
        assert snapshot.pending is None


class TestDwellAndTransitions:
    def test_stranded_condition_does_not_transition_before_dwell(self):
        tracker = ProviderStreamHealthTracker(dwell_seconds=300)
        snapshot, transition = _evaluate(
            tracker, now=1000.0, pending=3, registered_consumers=1, live_consumers=0
        )
        assert snapshot.state == StreamHealthState.HEALTHY
        assert transition is None

        # still within the dwell window
        snapshot, transition = _evaluate(
            tracker, now=1000.0 + 299, pending=3, registered_consumers=1, live_consumers=0
        )
        assert snapshot.state == StreamHealthState.HEALTHY
        assert transition is None

    def test_stranded_condition_transitions_once_dwell_elapses(self):
        tracker = ProviderStreamHealthTracker(dwell_seconds=300)
        _evaluate(tracker, now=1000.0, pending=3, registered_consumers=1, live_consumers=0)

        snapshot, transition = _evaluate(
            tracker, now=1000.0 + 300, pending=3, registered_consumers=1, live_consumers=0
        )
        assert snapshot.state == StreamHealthState.STRANDED
        assert transition == "stranded"
        assert snapshot.transitioned_at == 1000.0 + 300

        # a further pass while still stranded is not a new transition
        snapshot, transition = _evaluate(
            tracker, now=1000.0 + 400, pending=3, registered_consumers=1, live_consumers=0
        )
        assert snapshot.state == StreamHealthState.STRANDED
        assert transition is None
        assert snapshot.transitioned_at == 1000.0 + 300

    def test_lag_only_work_can_also_strand(self):
        tracker = ProviderStreamHealthTracker(dwell_seconds=300)
        _evaluate(tracker, now=0.0, pending=0, lag=2, registered_consumers=0, live_consumers=0)
        snapshot, transition = _evaluate(
            tracker, now=300.0, pending=0, lag=2, registered_consumers=0, live_consumers=0
        )
        assert snapshot.state == StreamHealthState.STRANDED
        assert transition == "stranded"

    def test_recovery_emits_once_and_resets_dwell(self):
        tracker = ProviderStreamHealthTracker(dwell_seconds=300)
        _evaluate(tracker, now=0.0, pending=3, registered_consumers=1, live_consumers=0)
        _evaluate(tracker, now=300.0, pending=3, registered_consumers=1, live_consumers=0)

        snapshot, transition = _evaluate(
            tracker, now=301.0, pending=3, registered_consumers=1, live_consumers=1
        )
        assert snapshot.state == StreamHealthState.HEALTHY
        assert transition == "recovered"

        # re-stranding must dwell again from scratch, not fire immediately
        snapshot, transition = _evaluate(
            tracker, now=302.0, pending=3, registered_consumers=1, live_consumers=0
        )
        assert snapshot.state == StreamHealthState.HEALTHY
        assert transition is None


class TestReadErrors:
    def test_read_error_preserves_prior_healthy_state(self):
        tracker = ProviderStreamHealthTracker(dwell_seconds=300)
        _evaluate(tracker, now=0.0, pending=0, live_consumers=0)

        snapshot, transition = _evaluate(
            tracker,
            now=1.0,
            pending=None,
            lag=None,
            registered_consumers=None,
            live_consumers=None,
            read_error=True,
        )
        assert snapshot.state == StreamHealthState.HEALTHY
        assert transition is None

    def test_read_error_never_emits_false_recovery(self):
        tracker = ProviderStreamHealthTracker(dwell_seconds=300)
        _evaluate(tracker, now=0.0, pending=3, registered_consumers=1, live_consumers=0)
        _evaluate(tracker, now=300.0, pending=3, registered_consumers=1, live_consumers=0)

        snapshot, transition = _evaluate(
            tracker,
            now=301.0,
            pending=None,
            lag=None,
            registered_consumers=None,
            live_consumers=None,
            read_error=True,
        )
        assert snapshot.state == StreamHealthState.STRANDED
        assert transition is None
        assert snapshot.pending is None

    def test_read_error_does_not_reset_in_progress_dwell(self):
        tracker = ProviderStreamHealthTracker(dwell_seconds=300)
        _evaluate(tracker, now=0.0, pending=3, registered_consumers=1, live_consumers=0)

        _evaluate(
            tracker,
            now=100.0,
            pending=None,
            lag=None,
            registered_consumers=None,
            live_consumers=None,
            read_error=True,
        )

        # dwell clock should still be counted from t=0, not reset by the
        # read error at t=100
        snapshot, transition = _evaluate(
            tracker, now=300.0, pending=3, registered_consumers=1, live_consumers=0
        )
        assert snapshot.state == StreamHealthState.STRANDED
        assert transition == "stranded"


class TestSerialization:
    def test_round_trips_through_dict(self):
        health = ProviderStreamHealth(
            provider="claude",
            stream="test:tasks:claude",
            pending=1,
            lag=2,
            registered_consumers=1,
            live_consumers=0,
            state=StreamHealthState.STRANDED,
            observed_at=123.0,
            transitioned_at=100.0,
        )
        restored = ProviderStreamHealth.from_dict(health.to_dict())
        assert restored == health


class TestSnapshotKeys:
    def test_pr_and_issue_keys_are_collision_free(self):
        pr = stream_health_snapshot_key("claude")
        issue = stream_health_snapshot_key("claude", issue=True)
        assert pr != issue
        assert pr == f"{STREAM_HEALTH_KEY_PREFIX}claude:pr"
        assert issue == f"{STREAM_HEALTH_KEY_PREFIX}claude:issue"
        assert pr.startswith(STREAM_HEALTH_KEY_PREFIX)
        assert issue.startswith(STREAM_HEALTH_KEY_PREFIX)

    def test_different_providers_do_not_collide_with_kind_suffixes(self):
        assert stream_health_snapshot_key("pr") != stream_health_snapshot_key("claude")
        assert stream_health_snapshot_key("issue") != stream_health_snapshot_key(
            "claude", issue=True
        )
        assert stream_health_snapshot_key("claude") != stream_health_snapshot_key("clauder")


class TestIndependentStreams:
    def test_pr_stranded_does_not_affect_healthy_issue_stream(self):
        tracker = ProviderStreamHealthTracker(dwell_seconds=300)
        _evaluate(tracker, now=0.0, pending=3, registered_consumers=1, live_consumers=0)
        _evaluate(
            tracker,
            now=0.0,
            stream=_ISSUE_STREAM,
            pending=0,
            registered_consumers=1,
            live_consumers=1,
        )

        pr, pr_transition = _evaluate(
            tracker, now=300.0, pending=3, registered_consumers=1, live_consumers=0
        )
        issue, issue_transition = _evaluate(
            tracker,
            now=300.0,
            stream=_ISSUE_STREAM,
            pending=0,
            registered_consumers=1,
            live_consumers=1,
        )

        assert pr.state == StreamHealthState.STRANDED
        assert pr_transition == "stranded"
        assert pr.stream == _PR_STREAM
        assert issue.state == StreamHealthState.HEALTHY
        assert issue_transition is None
        assert issue.stream == _ISSUE_STREAM
        assert issue.transitioned_at == 0.0
        assert pr.transitioned_at == 300.0

    def test_issue_stranded_does_not_affect_healthy_pr_stream(self):
        tracker = ProviderStreamHealthTracker(dwell_seconds=300)
        _evaluate(tracker, now=0.0, pending=0, registered_consumers=1, live_consumers=1)
        _evaluate(
            tracker,
            now=0.0,
            stream=_ISSUE_STREAM,
            pending=4,
            registered_consumers=1,
            live_consumers=0,
        )

        pr, pr_transition = _evaluate(
            tracker, now=300.0, pending=0, registered_consumers=1, live_consumers=1
        )
        issue, issue_transition = _evaluate(
            tracker,
            now=300.0,
            stream=_ISSUE_STREAM,
            pending=4,
            registered_consumers=1,
            live_consumers=0,
        )

        assert pr.state == StreamHealthState.HEALTHY
        assert pr_transition is None
        assert issue.state == StreamHealthState.STRANDED
        assert issue_transition == "stranded"

    def test_both_streams_can_strand_independently(self):
        tracker = ProviderStreamHealthTracker(dwell_seconds=300)
        _evaluate(tracker, now=0.0, pending=2, registered_consumers=1, live_consumers=0)
        _evaluate(
            tracker,
            now=0.0,
            stream=_ISSUE_STREAM,
            pending=5,
            registered_consumers=1,
            live_consumers=0,
        )

        pr, pr_transition = _evaluate(
            tracker, now=300.0, pending=2, registered_consumers=1, live_consumers=0
        )
        issue, issue_transition = _evaluate(
            tracker,
            now=300.0,
            stream=_ISSUE_STREAM,
            pending=5,
            registered_consumers=1,
            live_consumers=0,
        )

        assert pr.state == StreamHealthState.STRANDED
        assert issue.state == StreamHealthState.STRANDED
        assert pr_transition == "stranded"
        assert issue_transition == "stranded"

    def test_dwell_timers_do_not_leak_between_streams(self):
        tracker = ProviderStreamHealthTracker(dwell_seconds=300)
        _evaluate(tracker, now=0.0, pending=3, registered_consumers=1, live_consumers=0)
        _evaluate(
            tracker,
            now=200.0,
            stream=_ISSUE_STREAM,
            pending=3,
            registered_consumers=1,
            live_consumers=0,
        )

        pr, _ = _evaluate(tracker, now=300.0, pending=3, registered_consumers=1, live_consumers=0)
        issue, issue_transition = _evaluate(
            tracker,
            now=300.0,
            stream=_ISSUE_STREAM,
            pending=3,
            registered_consumers=1,
            live_consumers=0,
        )
        assert pr.state == StreamHealthState.STRANDED
        assert issue.state == StreamHealthState.HEALTHY
        assert issue_transition is None

        issue, issue_transition = _evaluate(
            tracker,
            now=500.0,
            stream=_ISSUE_STREAM,
            pending=3,
            registered_consumers=1,
            live_consumers=0,
        )
        pr, pr_transition = _evaluate(
            tracker, now=500.0, pending=3, registered_consumers=1, live_consumers=0
        )
        assert issue.state == StreamHealthState.STRANDED
        assert issue_transition == "stranded"
        assert issue.transitioned_at == 500.0
        assert pr.state == StreamHealthState.STRANDED
        assert pr_transition is None
        assert pr.transitioned_at == 300.0

    def test_recovery_of_one_stream_does_not_recover_the_other(self):
        tracker = ProviderStreamHealthTracker(dwell_seconds=300)
        _evaluate(tracker, now=0.0, pending=3, registered_consumers=1, live_consumers=0)
        _evaluate(
            tracker,
            now=0.0,
            stream=_ISSUE_STREAM,
            pending=3,
            registered_consumers=1,
            live_consumers=0,
        )
        _evaluate(tracker, now=300.0, pending=3, registered_consumers=1, live_consumers=0)
        _evaluate(
            tracker,
            now=300.0,
            stream=_ISSUE_STREAM,
            pending=3,
            registered_consumers=1,
            live_consumers=0,
        )

        pr, pr_transition = _evaluate(
            tracker, now=301.0, pending=3, registered_consumers=1, live_consumers=1
        )
        issue, issue_transition = _evaluate(
            tracker,
            now=301.0,
            stream=_ISSUE_STREAM,
            pending=3,
            registered_consumers=1,
            live_consumers=0,
        )

        assert pr.state == StreamHealthState.HEALTHY
        assert pr_transition == "recovered"
        assert issue.state == StreamHealthState.STRANDED
        assert issue_transition is None
        assert issue.transitioned_at == 300.0

    def test_read_error_on_one_stream_preserves_the_other(self):
        tracker = ProviderStreamHealthTracker(dwell_seconds=300)
        _evaluate(tracker, now=0.0, pending=3, registered_consumers=1, live_consumers=0)
        _evaluate(tracker, now=300.0, pending=3, registered_consumers=1, live_consumers=0)
        _evaluate(
            tracker,
            now=300.0,
            stream=_ISSUE_STREAM,
            pending=0,
            registered_consumers=0,
            live_consumers=0,
        )

        pr, pr_transition = _evaluate(
            tracker,
            now=301.0,
            pending=None,
            lag=None,
            registered_consumers=None,
            live_consumers=None,
            read_error=True,
        )
        issue, issue_transition = _evaluate(
            tracker,
            now=301.0,
            stream=_ISSUE_STREAM,
            pending=0,
            registered_consumers=0,
            live_consumers=0,
        )

        assert pr.state == StreamHealthState.STRANDED
        assert pr_transition is None
        assert pr.pending is None
        assert issue.state == StreamHealthState.HEALTHY
        assert issue_transition is None
        assert issue.pending == 0

    def test_read_error_on_issue_does_not_reset_pr_dwell(self):
        tracker = ProviderStreamHealthTracker(dwell_seconds=300)
        _evaluate(tracker, now=0.0, pending=3, registered_consumers=1, live_consumers=0)
        _evaluate(
            tracker,
            now=100.0,
            stream=_ISSUE_STREAM,
            pending=None,
            lag=None,
            registered_consumers=None,
            live_consumers=None,
            read_error=True,
        )

        pr, pr_transition = _evaluate(
            tracker, now=300.0, pending=3, registered_consumers=1, live_consumers=0
        )
        issue, _ = _evaluate(
            tracker,
            now=300.0,
            stream=_ISSUE_STREAM,
            pending=None,
            lag=None,
            registered_consumers=None,
            live_consumers=None,
            read_error=True,
        )

        assert pr.state == StreamHealthState.STRANDED
        assert pr_transition == "stranded"
        assert issue.state == StreamHealthState.UNKNOWN
