"""Capacity-aware provider admission tests."""

from __future__ import annotations

import json
import logging
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from orcest.orchestrator.provider_capacity import (
    CapacityReservation,
    read_provider_loads,
    reserve_provider_capacity,
)
from orcest.shared.models import CONSUMER_GROUP, task_stream_name
from orcest.shared.redis_client import RedisClient


def _heartbeat(
    redis_client: RedisClient,
    worker_id: str,
    backend: str,
    *,
    ttl: int = 150,
) -> None:
    # This is the exact shape emitted by current workers, including the
    # optional provider_cli health object hardened in #797.
    redis_client.set_ex(
        f"workers:heartbeat:{worker_id}",
        json.dumps(
            {
                "backend": backend,
                "provider_cli": {
                    "provider": backend,
                    "status": "ok",
                    "version": "1.2.3",
                },
                "revision": "abc123",
            },
            sort_keys=True,
            separators=(",", ":"),
        ),
        ttl=ttl,
    )


def _streams(redis_client: RedisClient, *providers: str) -> None:
    for provider in providers:
        redis_client.ensure_consumer_group(task_stream_name(provider), CONSUMER_GROUP)
        redis_client.ensure_consumer_group(task_stream_name(provider, issue=True), CONSUMER_GROUP)


def _claim(
    redis_client: RedisClient,
    provider: str,
    worker_id: str,
    *,
    issue: bool,
) -> None:
    stream = task_stream_name(provider, issue=issue)
    redis_client.xadd(stream, {"id": f"task-{provider}-{worker_id}-{issue}"})
    assert redis_client.xreadgroup(
        CONSUMER_GROUP,
        worker_id,
        stream,
        block_ms=None,
    )


def _lag(redis_client: RedisClient, provider: str, count: int, *, issue: bool) -> None:
    stream = task_stream_name(provider, issue=issue)
    for index in range(count):
        redis_client.xadd(stream, {"id": f"lag-{provider}-{issue}-{index}"})


def test_production_shape_routes_busy_clauder_backlog_to_idle_codex(
    fake_redis_client: RedisClient,
) -> None:
    """Two busy clauder workers + two unread tasks loses to idle Codex."""
    _streams(fake_redis_client, "clauder", "codex")
    _heartbeat(fake_redis_client, "orcest-worker-10000", "clauder")
    _heartbeat(fake_redis_client, "orcest-worker-10001", "clauder")
    _heartbeat(fake_redis_client, "orcest-worker-10002", "codex")
    _claim(fake_redis_client, "clauder", "orcest-worker-10000", issue=True)
    _claim(fake_redis_client, "clauder", "orcest-worker-10001", issue=True)
    _lag(fake_redis_client, "clauder", 2, issue=True)

    reservation = reserve_provider_capacity(
        fake_redis_client,
        ["clauder", "codex"],
        "task-next-issue",
        logging.getLogger("test"),
    )

    assert reservation is not None
    assert reservation.provider == "codex"
    known, unknown = read_provider_loads(fake_redis_client, ["clauder", "codex"])
    assert unknown == {}
    assert known["clauder"].busy_consumers == (
        "orcest-worker-10000",
        "orcest-worker-10001",
    )
    assert known["clauder"].unread_lag == 2
    assert known["codex"].reservations == 1
    reservation.release()


@pytest.mark.parametrize("busy_issue", [False, True])
def test_busy_worker_on_either_stream_is_unavailable_to_other_family(
    fake_redis_client: RedisClient,
    busy_issue: bool,
) -> None:
    _streams(fake_redis_client, "clauder", "codex")
    _heartbeat(fake_redis_client, "worker-codex", "codex")
    _heartbeat(fake_redis_client, "worker-clauder", "clauder")
    _claim(fake_redis_client, "codex", "worker-codex", issue=busy_issue)

    reservation = reserve_provider_capacity(
        fake_redis_client,
        ["codex", "clauder"],
        f"cross-stream-{busy_issue}",
        logging.getLogger("test"),
    )

    assert reservation is not None
    assert reservation.provider == "clauder"


def test_same_busy_consumer_across_both_streams_is_counted_once(
    fake_redis_client: RedisClient,
) -> None:
    _streams(fake_redis_client, "codex")
    _heartbeat(fake_redis_client, "worker-busy", "codex")
    _heartbeat(fake_redis_client, "worker-idle", "codex")
    _claim(fake_redis_client, "codex", "worker-busy", issue=False)
    _claim(fake_redis_client, "codex", "worker-busy", issue=True)

    known, unknown = read_provider_loads(fake_redis_client, ["codex"])

    assert unknown == {}
    assert known["codex"].busy_consumers == ("worker-busy",)
    assert known["codex"].effective_spare == 1


def test_duplicate_provider_accounts_do_not_multiply_worker_capacity(
    fake_redis_client: RedisClient,
) -> None:
    _streams(fake_redis_client, "codex")
    _heartbeat(fake_redis_client, "only-codex-worker", "codex")

    first = reserve_provider_capacity(
        fake_redis_client,
        ["codex", "codex", "codex"],
        "first",
        logging.getLogger("test"),
    )
    second = reserve_provider_capacity(
        fake_redis_client,
        ["codex", "codex"],
        "second",
        logging.getLogger("test"),
    )

    assert first is not None
    assert second is None


def test_equal_capacity_provider_ties_are_shared_deterministic_round_robin(
    fake_redis_client: RedisClient,
) -> None:
    _streams(fake_redis_client, "clauder", "codex")
    _heartbeat(fake_redis_client, "worker-clauder", "clauder")
    _heartbeat(fake_redis_client, "worker-codex", "codex")
    selected: list[str] = []
    for index in range(4):
        reservation = reserve_provider_capacity(
            fake_redis_client,
            ["clauder", "codex"],
            f"tie-{index}",
            logging.getLogger("test"),
        )
        assert reservation is not None
        selected.append(reservation.provider)
        reservation.release()
    assert selected == ["clauder", "codex", "clauder", "codex"]


def test_unknown_or_zero_live_provider_loses_to_known_healthy_provider(
    fake_redis_client: RedisClient,
    mocker,
) -> None:
    _streams(fake_redis_client, "clauder", "codex", "grok")
    _heartbeat(fake_redis_client, "worker-clauder", "clauder")
    _heartbeat(fake_redis_client, "worker-codex", "codex")
    original = fake_redis_client.xgroup_pending_snapshot

    def flaky(stream: str, group: str, **kwargs):
        if stream == task_stream_name("clauder"):
            raise RuntimeError("read failed")
        return original(stream, group, **kwargs)

    mocker.patch.object(
        fake_redis_client,
        "xgroup_pending_snapshot",
        side_effect=flaky,
    )
    reservation = reserve_provider_capacity(
        fake_redis_client,
        ["clauder", "grok", "codex"],
        "prefer-known",
        logging.getLogger("test"),
    )
    assert reservation is not None
    assert reservation.provider == "codex"


def test_incomplete_pending_read_is_unknown_not_idle(
    fake_redis_client: RedisClient,
    mocker,
) -> None:
    _streams(fake_redis_client, "clauder", "codex")
    _heartbeat(fake_redis_client, "worker-clauder", "clauder")
    _heartbeat(fake_redis_client, "worker-codex", "codex")
    _claim(fake_redis_client, "clauder", "worker-clauder", issue=False)
    original = fake_redis_client.xgroup_pending_snapshot

    def incomplete(stream: str, group: str, **kwargs):
        if stream == task_stream_name("clauder"):
            rows, _entries, length = original(stream, group, **kwargs)
            return rows, [], length
        return original(stream, group, **kwargs)

    mocker.patch.object(fake_redis_client, "xgroup_pending_snapshot", side_effect=incomplete)
    reservation = reserve_provider_capacity(
        fake_redis_client,
        ["clauder", "codex"],
        "pending-incomplete",
        logging.getLogger("test"),
    )
    assert reservation is not None
    assert reservation.provider == "codex"


def test_stale_immortal_heartbeat_is_unknown_not_idle(
    fake_redis_client: RedisClient,
) -> None:
    _streams(fake_redis_client, "clauder", "codex")
    fake_redis_client.set_value(
        "workers:heartbeat:stale-clauder",
        json.dumps({"backend": "clauder", "revision": "old"}),
    )
    _heartbeat(fake_redis_client, "worker-codex", "codex")

    reservation = reserve_provider_capacity(
        fake_redis_client,
        ["clauder", "codex"],
        "stale-heartbeat",
        logging.getLogger("test"),
    )
    assert reservation is not None
    assert reservation.provider == "codex"


def test_failed_heartbeat_read_defers_all_candidates(
    fake_redis_client: RedisClient,
    mocker,
) -> None:
    _streams(fake_redis_client, "codex")
    _heartbeat(fake_redis_client, "worker-codex", "codex")
    mocker.patch.object(fake_redis_client, "scan_page", side_effect=ConnectionError("down"))
    assert (
        reserve_provider_capacity(
            fake_redis_client,
            ["codex"],
            "unknown",
            logging.getLogger("test"),
        )
        is None
    )


def test_unattributable_malformed_heartbeat_fails_every_candidate_closed(
    fake_redis_client: RedisClient,
) -> None:
    """Without a backend, malformed live evidence cannot spare one provider safely."""
    _streams(fake_redis_client, "clauder", "codex")
    _heartbeat(fake_redis_client, "worker-codex", "codex")
    fake_redis_client.set_ex("workers:heartbeat:unknown", "not-json", ttl=150)

    assert (
        reserve_provider_capacity(
            fake_redis_client,
            ["clauder", "codex"],
            "malformed-global",
            logging.getLogger("test"),
        )
        is None
    )


def test_expired_reservation_is_pruned_and_slot_recovers(
    fake_redis_client: RedisClient,
) -> None:
    _streams(fake_redis_client, "codex")
    _heartbeat(fake_redis_client, "worker-codex", "codex")
    first = reserve_provider_capacity(
        fake_redis_client,
        ["codex"],
        "crashed-publisher",
        logging.getLogger("test"),
    )
    assert first is not None
    # Model a crashed publisher whose member deadline elapsed while the
    # containing Redis key is still alive for another cleanup interval.
    fake_redis_client.zadd(first.key, {first.task_id: 0.0})
    second = reserve_provider_capacity(
        fake_redis_client,
        ["codex"],
        "replacement-publisher",
        logging.getLogger("test"),
    )
    assert second is not None


def test_reservation_revalidation_aborts_when_its_worker_becomes_busy(
    fake_redis_client: RedisClient,
) -> None:
    _streams(fake_redis_client, "codex")
    _heartbeat(fake_redis_client, "worker-codex", "codex")
    reservation = reserve_provider_capacity(
        fake_redis_client,
        ["codex"],
        "selected-before-race",
        logging.getLogger("test"),
    )
    assert reservation is not None
    _claim(fake_redis_client, "codex", "worker-codex", issue=False)

    assert reservation.refresh() is False
    assert fake_redis_client.zcard(reservation.key) == 0


def test_concurrent_orchestrators_cannot_reserve_one_worker_twice(
    fake_redis_client: RedisClient,
    make_fake_redis_client,
) -> None:
    _streams(fake_redis_client, "codex")
    _heartbeat(fake_redis_client, "worker-codex", "codex")
    second_client = make_fake_redis_client()
    barrier = threading.Barrier(2)

    def select(client: RedisClient, task_id: str) -> CapacityReservation | None:
        barrier.wait()
        return reserve_provider_capacity(
            client,
            ["codex"],
            task_id,
            logging.getLogger("test"),
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(
            executor.map(
                lambda args: select(*args),
                [(fake_redis_client, "project-a"), (second_client, "project-b")],
            )
        )
    assert sum(result is not None for result in results) == 1


def test_capacity_diagnostics_are_secret_free(fake_redis_client: RedisClient) -> None:
    _streams(fake_redis_client, "codex")
    _heartbeat(fake_redis_client, "worker-codex", "codex")
    secret = "credential-super-secret"
    reservation = reserve_provider_capacity(
        fake_redis_client,
        ["codex"],
        "safe-task-id",
        logging.getLogger("test"),
    )
    assert reservation is not None
    snapshots, _unknown = read_provider_loads(fake_redis_client, ["codex"])
    rendered = repr(snapshots["codex"].to_safe_dict()) + repr(reservation)
    assert secret not in rendered
