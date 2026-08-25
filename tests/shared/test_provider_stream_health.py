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
    STREAM_HEALTH_SNAPSHOT_VERSION,
    ProviderStreamHealth,
    ProviderStreamHealthTracker,
    StreamHealthState,
    parse_committed_stranded_snapshot,
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
        payload = health.to_dict()
        assert payload["version"] == STREAM_HEALTH_SNAPSHOT_VERSION
        restored = ProviderStreamHealth.from_dict(payload)
        assert restored == health

    def test_from_dict_still_accepts_unversioned_display_records(self):
        payload = {
            "provider": "claude",
            "stream": "test:tasks:claude",
            "pending": 1,
            "lag": 0,
            "registered_consumers": 1,
            "live_consumers": 0,
            "state": "stranded",
            "observed_at": 123.0,
            "transitioned_at": 100.0,
        }
        restored = ProviderStreamHealth.from_dict(payload)
        assert restored.state == StreamHealthState.STRANDED
        assert restored.provider == "claude"


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


def _stranded_health(**overrides) -> ProviderStreamHealth:
    fields = dict(
        provider="claude",
        stream=_PR_STREAM,
        pending=3,
        lag=1,
        registered_consumers=1,
        live_consumers=0,
        state=StreamHealthState.STRANDED,
        observed_at=1100.0,
        transitioned_at=1000.0,
    )
    fields.update(overrides)
    return ProviderStreamHealth(**fields)


def _parse(payload, **overrides):
    kwargs = dict(
        expected_provider="claude",
        expected_stream=_PR_STREAM,
        now=1200.0,
        ttl_seconds=900,
        max_age_seconds=900.0,
    )
    kwargs.update(overrides)
    return parse_committed_stranded_snapshot(payload, **kwargs)


class TestParseCommittedStrandedSnapshot:
    def test_valid_stranded_snapshot_is_returned(self):
        health = _stranded_health()
        parsed = _parse(health.to_dict())
        assert parsed == health

    def test_healthy_snapshot_is_absent(self):
        payload = _stranded_health(state=StreamHealthState.HEALTHY).to_dict()
        assert _parse(payload) is None

    def test_unknown_snapshot_is_absent(self):
        payload = _stranded_health(state=StreamHealthState.UNKNOWN).to_dict()
        assert _parse(payload) is None

    def test_missing_payload_is_absent(self):
        assert _parse(None) is None

    def test_malformed_payload_is_absent(self):
        assert _parse("{not json") is None
        assert _parse(["stranded"]) is None
        assert _parse({"version": 1}) is None

    def test_expired_and_non_expiring_ttl_are_absent(self):
        payload = _stranded_health().to_dict()
        assert _parse(payload, ttl_seconds=0) is None
        assert _parse(payload, ttl_seconds=-1) is None
        assert _parse(payload, ttl_seconds=-2) is None
        assert _parse(payload, ttl_seconds=True) is None

    def test_identity_mismatch_is_absent(self):
        payload = _stranded_health().to_dict()
        assert _parse(payload, expected_provider="xai") is None
        assert _parse(payload, expected_stream=_ISSUE_STREAM) is None
        swapped = dict(payload)
        swapped["provider"] = "xai"
        assert _parse(swapped) is None
        swapped = dict(payload)
        swapped["stream"] = _ISSUE_STREAM
        assert _parse(swapped) is None

    def test_unsupported_version_is_absent(self):
        payload = _stranded_health().to_dict()
        missing = dict(payload)
        missing.pop("version")
        assert _parse(missing) is None
        payload["version"] = 2
        assert _parse(payload) is None
        payload["version"] = 0
        assert _parse(payload) is None
        payload["version"] = 1.0
        assert _parse(payload) is None
        payload["version"] = True
        assert _parse(payload) is None

    def test_non_finite_timestamps_are_absent(self):
        payload = _stranded_health().to_dict()
        for field in ("observed_at", "transitioned_at"):
            for value in (float("nan"), float("inf"), float("-inf"), None, "1000"):
                bad = dict(payload)
                bad[field] = value
                assert _parse(bad) is None

    def test_future_timestamps_are_absent(self):
        payload = _stranded_health(observed_at=1300.0, transitioned_at=1000.0).to_dict()
        assert _parse(payload, now=1200.0) is None
        payload = _stranded_health(observed_at=1100.0, transitioned_at=1250.0).to_dict()
        assert _parse(payload, now=1200.0) is None

    def test_reversed_timestamps_are_absent(self):
        payload = _stranded_health(observed_at=1000.0, transitioned_at=1100.0).to_dict()
        assert _parse(payload) is None

    def test_stale_observed_at_is_absent(self):
        payload = _stranded_health(observed_at=200.0, transitioned_at=100.0).to_dict()
        assert _parse(payload, now=1200.0, max_age_seconds=900.0) is None

    def test_old_transitioned_at_is_allowed_when_observed_at_is_fresh(self):
        health = _stranded_health(observed_at=1100.0, transitioned_at=10.0)
        parsed = _parse(health.to_dict(), now=1200.0, max_age_seconds=900.0)
        assert parsed is not None
        assert parsed.transitioned_at == 10.0


class TestRestoreCommitted:
    def test_still_stranded_retains_state_and_transition_time(self):
        tracker = ProviderStreamHealthTracker(dwell_seconds=300)
        health = _stranded_health()
        assert tracker.restore_committed(health) is True
        snapshot, transition = _evaluate(
            tracker, now=1200.0, pending=3, registered_consumers=1, live_consumers=0
        )
        assert snapshot.state == StreamHealthState.STRANDED
        assert transition is None
        assert snapshot.transitioned_at == 1000.0
        assert snapshot.pending == 3

    def test_recovered_inputs_emit_one_recovery(self):
        tracker = ProviderStreamHealthTracker(dwell_seconds=300)
        tracker.restore_committed(_stranded_health())
        snapshot, transition = _evaluate(
            tracker, now=1200.0, pending=3, registered_consumers=1, live_consumers=1
        )
        assert snapshot.state == StreamHealthState.HEALTHY
        assert transition == "recovered"
        assert snapshot.transitioned_at == 1200.0

        snapshot, transition = _evaluate(
            tracker, now=1201.0, pending=3, registered_consumers=1, live_consumers=1
        )
        assert snapshot.state == StreamHealthState.HEALTHY
        assert transition is None
        assert snapshot.transitioned_at == 1200.0

    def test_unreadable_inputs_keep_stranded_with_unknown_metrics(self):
        tracker = ProviderStreamHealthTracker(dwell_seconds=300)
        tracker.restore_committed(_stranded_health())
        snapshot, transition = _evaluate(
            tracker,
            now=1200.0,
            pending=None,
            lag=None,
            registered_consumers=None,
            live_consumers=None,
            read_error=True,
        )
        assert snapshot.state == StreamHealthState.STRANDED
        assert transition is None
        assert snapshot.pending is None
        assert snapshot.lag is None
        assert snapshot.transitioned_at == 1000.0

    def test_healthy_snapshot_does_not_restore_dwell_candidate(self):
        tracker = ProviderStreamHealthTracker(dwell_seconds=300)
        healthy = _stranded_health(
            state=StreamHealthState.HEALTHY, live_consumers=1, transitioned_at=500.0
        )
        assert tracker.restore_committed(healthy) is False
        assert tracker.has_state("claude", _PR_STREAM) is False

        snapshot, transition = _evaluate(
            tracker, now=1200.0, pending=3, registered_consumers=1, live_consumers=0
        )
        assert snapshot.state == StreamHealthState.HEALTHY
        assert transition is None
        snapshot, transition = _evaluate(
            tracker, now=1499.0, pending=3, registered_consumers=1, live_consumers=0
        )
        assert snapshot.state == StreamHealthState.HEALTHY
        snapshot, transition = _evaluate(
            tracker, now=1500.0, pending=3, registered_consumers=1, live_consumers=0
        )
        assert snapshot.state == StreamHealthState.STRANDED
        assert transition == "stranded"
        assert snapshot.transitioned_at == 1500.0

    def test_restore_does_not_overwrite_existing_state(self):
        tracker = ProviderStreamHealthTracker(dwell_seconds=300)
        _evaluate(tracker, now=0.0, pending=0, live_consumers=1)
        assert tracker.restore_committed(_stranded_health()) is False
        snapshot, transition = _evaluate(
            tracker, now=1200.0, pending=3, registered_consumers=1, live_consumers=0
        )
        assert snapshot.state == StreamHealthState.HEALTHY
        assert transition is None

    def test_restore_does_not_leak_between_streams(self):
        tracker = ProviderStreamHealthTracker(dwell_seconds=300)
        tracker.restore_committed(_stranded_health())
        snapshot, transition = _evaluate(
            tracker,
            now=1200.0,
            stream=_ISSUE_STREAM,
            pending=3,
            registered_consumers=1,
            live_consumers=0,
        )
        assert snapshot.state == StreamHealthState.HEALTHY
        assert transition is None
        pr, pr_transition = _evaluate(
            tracker, now=1200.0, pending=3, registered_consumers=1, live_consumers=0
        )
        assert pr.state == StreamHealthState.STRANDED
        assert pr_transition is None
        assert pr.transitioned_at == 1000.0
