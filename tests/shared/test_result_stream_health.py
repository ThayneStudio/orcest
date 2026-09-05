"""Focused result-stream completeness and liveness tests."""

import pytest
import redis as redis_lib

from orcest.shared.models import RESULTS_GROUP, RESULTS_STREAM
from orcest.shared.result_stream_health import (
    RESULT_CONSUMER_LIVE_IDLE_SECONDS,
    RESULT_PENDING_INSPECTION_LIMIT,
    RESULT_PENDING_PAGE_SIZE,
    RESULT_PENDING_STALE_DELIVERIES,
    RESULT_PENDING_STALE_IDLE_SECONDS,
    format_result_stream_metrics,
    format_result_stream_warning,
    inspect_result_stream_raw,
)

pytestmark = pytest.mark.unit


def _pending_row(
    sequence: int,
    *,
    idle_seconds: int = 0,
    deliveries: int = 1,
) -> dict[str, object]:
    return {
        "message_id": f"{sequence}-0",
        "consumer": "orchestrator-main",
        "time_since_delivered": idle_seconds * 1000,
        "times_delivered": deliveries,
    }


def _mock_group(mocker, redis, *, pending: int, lag: int, consumer_idle_seconds: int = 0):
    mocker.patch.object(
        redis.client,
        "xinfo_groups",
        return_value=[
            {
                "name": RESULTS_GROUP,
                "consumers": 1,
                "pending": pending,
                "lag": lag,
            }
        ],
    )
    mocker.patch.object(
        redis.client,
        "xinfo_consumers",
        return_value=[
            {
                "name": "orchestrator-main",
                "pending": pending,
                "idle": consumer_idle_seconds * 1000,
            }
        ],
    )


def test_101st_pending_entry_age_breach_is_inspected(fake_redis_client, mocker):
    stream = fake_redis_client._prefixed(RESULTS_STREAM)
    fake_redis_client.client.xadd(stream, {"task_id": "retained"})
    _mock_group(mocker, fake_redis_client, pending=101, lag=0)
    pending = mocker.patch.object(
        fake_redis_client.client,
        "xpending_range",
        side_effect=[
            [_pending_row(i) for i in range(1, RESULT_PENDING_PAGE_SIZE + 1)],
            [
                _pending_row(
                    101,
                    idle_seconds=RESULT_PENDING_STALE_IDLE_SECONDS,
                )
            ],
        ],
    )

    health = inspect_result_stream_raw(fake_redis_client, stream)

    assert health.pending_inspection_complete is True
    assert health.sampled_pending == 101
    assert health.oldest_pending_idle_seconds == RESULT_PENDING_STALE_IDLE_SECONDS
    assert health.stale is True
    assert pending.call_args_list[1].kwargs["min"] == "(100-0"


def test_101st_pending_entry_delivery_breach_is_inspected(fake_redis_client, mocker):
    stream = fake_redis_client._prefixed(RESULTS_STREAM)
    fake_redis_client.client.xadd(stream, {"task_id": "retained"})
    _mock_group(mocker, fake_redis_client, pending=101, lag=0)
    mocker.patch.object(
        fake_redis_client.client,
        "xpending_range",
        side_effect=[
            [_pending_row(i) for i in range(1, RESULT_PENDING_PAGE_SIZE + 1)],
            [_pending_row(101, deliveries=RESULT_PENDING_STALE_DELIVERIES)],
        ],
    )

    health = inspect_result_stream_raw(fake_redis_client, stream)

    assert health.pending_inspection_complete is True
    assert health.max_delivery_count == RESULT_PENDING_STALE_DELIVERIES
    assert health.stale is True


def test_pending_inspection_over_ceiling_is_explicitly_incomplete_and_unhealthy(
    fake_redis_client, mocker
):
    stream = fake_redis_client._prefixed(RESULTS_STREAM)
    fake_redis_client.client.xadd(stream, {"task_id": "retained"})
    pending_count = RESULT_PENDING_INSPECTION_LIMIT + 1
    _mock_group(mocker, fake_redis_client, pending=pending_count, lag=0)
    next_sequence = 1

    def pending_page(_stream, _group, *, min, max, count):
        nonlocal next_sequence
        assert max == "+"
        if next_sequence == 1:
            assert min == "-"
        else:
            assert min == f"({next_sequence - 1}-0"
        rows = [_pending_row(i) for i in range(next_sequence, next_sequence + count)]
        next_sequence += count
        return rows

    pending = mocker.patch.object(
        fake_redis_client.client,
        "xpending_range",
        side_effect=pending_page,
    )

    health = inspect_result_stream_raw(fake_redis_client, stream)

    assert pending.call_count == RESULT_PENDING_INSPECTION_LIMIT // RESULT_PENDING_PAGE_SIZE
    assert health.pending_inspection_complete is False
    assert health.sampled_pending == RESULT_PENDING_INSPECTION_LIMIT
    assert health.oldest_pending_idle_seconds is None
    assert health.max_delivery_count is None
    assert health.inspection_error == (
        f"{stream}: pending result inspection incomplete: inspected "
        f"{RESULT_PENDING_INSPECTION_LIMIT} of {pending_count} entries"
    )
    assert format_result_stream_warning(health).startswith("RESULT STREAM UNHEALTHY")
    metrics = dict(format_result_stream_metrics(health))
    assert metrics["Pending inspected"] == (
        f"{RESULT_PENDING_INSPECTION_LIMIT}/{pending_count} incomplete"
    )


def test_pending_page_error_preserves_secret_free_partial_coverage(fake_redis_client, mocker):
    stream = fake_redis_client._prefixed(RESULTS_STREAM)
    fake_redis_client.client.xadd(stream, {"task_id": "retained"})
    _mock_group(mocker, fake_redis_client, pending=101, lag=0)
    mocker.patch.object(
        fake_redis_client.client,
        "xpending_range",
        side_effect=[
            [_pending_row(i) for i in range(1, RESULT_PENDING_PAGE_SIZE + 1)],
            redis_lib.ResponseError("NOPERM secret endpoint detail"),
        ],
    )

    health = inspect_result_stream_raw(fake_redis_client, stream)

    assert health.sampled_pending == 100
    assert health.pending_inspection_complete is False
    assert health.inspection_error == (
        f"{stream}: pending result inspection ResponseError after 100 of 101 entries"
    )
    assert "secret endpoint detail" not in str(health)


def test_stale_registered_result_consumer_does_not_count_as_live(fake_redis_client, mocker):
    stream = fake_redis_client._prefixed(RESULTS_STREAM)
    fake_redis_client.client.xadd(stream, {"task_id": "retained"})
    _mock_group(
        mocker,
        fake_redis_client,
        pending=0,
        lag=1,
        consumer_idle_seconds=RESULT_CONSUMER_LIVE_IDLE_SECONDS,
    )

    health = inspect_result_stream_raw(fake_redis_client, stream)

    assert health.consumers == 1
    assert health.live_consumers == 0
    assert health.unconsumed is True
    assert "live_consumers=0/1" in format_result_stream_warning(health)


def test_recently_active_result_consumer_keeps_fresh_lag_healthy(fake_redis_client, mocker):
    stream = fake_redis_client._prefixed(RESULTS_STREAM)
    fake_redis_client.client.xadd(stream, {"task_id": "retained"})
    _mock_group(
        mocker,
        fake_redis_client,
        pending=0,
        lag=1,
        consumer_idle_seconds=RESULT_CONSUMER_LIVE_IDLE_SECONDS - 1,
    )

    health = inspect_result_stream_raw(fake_redis_client, stream)

    assert health.live_consumers == 1
    assert health.unconsumed is False
    assert health.stale is False
    assert format_result_stream_warning(health) is None


def test_results_constants_are_shared_by_worker_and_orchestrator_modules():
    from orcest.orchestrator.loop import (
        RESULTS_GROUP as orchestrator_group,
        RESULTS_STREAM as orchestrator_stream,
    )
    from orcest.worker.loop import RESULTS_STREAM as worker_stream

    assert orchestrator_group == RESULTS_GROUP
    assert orchestrator_stream == RESULTS_STREAM
    assert worker_stream == RESULTS_STREAM
