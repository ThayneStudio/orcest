"""Real-Redis proof for capacity routing and cross-orchestrator reservation."""

from __future__ import annotations

import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from orcest.orchestrator import provider_capacity
from orcest.orchestrator.provider_capacity import (
    CapacityReservation,
    reserve_provider_capacity,
)
from orcest.shared.coordination import RedisLock
from orcest.shared.models import CONSUMER_GROUP, task_stream_name
from orcest.shared.redis_client import RedisClient


def _prepare_backend(redis_client: RedisClient, backend: str, workers: list[str]) -> None:
    redis_client.ensure_consumer_group(task_stream_name(backend), CONSUMER_GROUP)
    redis_client.ensure_consumer_group(task_stream_name(backend, issue=True), CONSUMER_GROUP)
    for worker in workers:
        redis_client.set_ex(
            f"workers:heartbeat:{worker}",
            json.dumps(
                {
                    "backend": backend,
                    "provider_cli": {
                        "provider": backend,
                        "status": "ok",
                        "version": "1.2.3",
                    },
                    "revision": "integration-revision",
                },
                sort_keys=True,
                separators=(",", ":"),
            ),
            ttl=150,
        )


@pytest.mark.integration
def test_live_shape_and_atomic_reservation_route_to_idle_codex(
    real_redis_client: RedisClient,
    make_real_redis_client,
) -> None:
    """Production evidence shape routes once to Codex across two clients."""
    rc = real_redis_client
    _prepare_backend(rc, "clauder", ["orcest-worker-10000", "orcest-worker-10001"])
    _prepare_backend(rc, "codex", ["orcest-worker-10002"])
    issue_stream = task_stream_name("clauder", issue=True)
    for index in range(4):
        rc.xadd(issue_stream, {"id": f"clauder-task-{index}"})
    for worker in ("orcest-worker-10000", "orcest-worker-10001"):
        assert rc.xreadgroup(
            CONSUMER_GROUP,
            worker,
            issue_stream,
            block_ms=None,
        )

    sibling = make_real_redis_client()
    barrier = threading.Barrier(2)

    def select(client: RedisClient, task_id: str) -> CapacityReservation | None:
        barrier.wait()
        return reserve_provider_capacity(
            client,
            ["clauder", "codex"],
            task_id,
            logging.getLogger("integration.capacity"),
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        selections = list(
            executor.map(
                lambda args: select(*args),
                [(rc, "project-a-task"), (sibling, "project-b-task")],
            )
        )

    winners = [selection for selection in selections if selection is not None]
    assert len(winners) == 1
    assert winners[0].provider == "codex"


@pytest.mark.integration
def test_selector_paused_past_lock_expiry_cannot_write_stale_reservation(
    real_redis_client: RedisClient,
    make_real_redis_client,
    monkeypatch,
) -> None:
    rc = real_redis_client
    sibling = make_real_redis_client()
    _prepare_backend(rc, "codex", ["only-worker"])
    monkeypatch.setattr(provider_capacity, "_CAPACITY_LOCK_TTL_SECONDS", 1)
    original_read = provider_capacity.read_provider_loads
    snapshot_ready = threading.Event()
    resume_stale = threading.Event()

    def delayed_read(client: RedisClient, providers: list[str]):
        result = original_read(client, providers)
        if threading.current_thread().name.startswith("stale-selector"):
            snapshot_ready.set()
            assert resume_stale.wait(timeout=10)
        return result

    monkeypatch.setattr(provider_capacity, "read_provider_loads", delayed_read)
    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="stale-selector") as executor:
        stale_future = executor.submit(
            reserve_provider_capacity,
            rc,
            ["codex"],
            "stale-task",
            logging.getLogger("integration.capacity"),
        )
        assert snapshot_ready.wait(timeout=10)
        time.sleep(1.1)
        current = reserve_provider_capacity(
            sibling,
            ["codex"],
            "current-task",
            logging.getLogger("integration.capacity"),
        )
        assert current is not None
        resume_stale.set()
        stale = stale_future.result(timeout=10)

    assert stale is None
    assert rc.zcard("providers:capacity:reservations:codex") == 1
    assert rc.zscore("providers:capacity:reservations:codex", "stale-task") is None
    assert rc.zscore("providers:capacity:reservations:codex", "current-task") is not None
    current.release()


@pytest.mark.integration
def test_refresh_paused_past_lock_expiry_cannot_renew_reservation(
    real_redis_client: RedisClient,
    make_real_redis_client,
    monkeypatch,
) -> None:
    rc = real_redis_client
    sibling = make_real_redis_client()
    _prepare_backend(rc, "codex", ["only-worker"])
    reservation = reserve_provider_capacity(
        rc,
        ["codex"],
        "refreshing-task",
        logging.getLogger("integration.capacity"),
    )
    assert reservation is not None
    score_before = rc.zscore(reservation.key, reservation.task_id)
    assert score_before is not None

    monkeypatch.setattr(provider_capacity, "_CAPACITY_LOCK_TTL_SECONDS", 1)
    original_read = provider_capacity.read_provider_loads
    snapshot_ready = threading.Event()
    resume_stale = threading.Event()

    def delayed_read(client: RedisClient, providers: list[str]):
        result = original_read(client, providers)
        if threading.current_thread().name.startswith("stale-refresh"):
            snapshot_ready.set()
            assert resume_stale.wait(timeout=10)
        return result

    monkeypatch.setattr(provider_capacity, "read_provider_loads", delayed_read)
    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="stale-refresh") as executor:
        refresh_future = executor.submit(reservation.refresh)
        assert snapshot_ready.wait(timeout=10)
        time.sleep(1.1)
        newer_owner = RedisLock(
            sibling,
            "providers:capacity:selection",
            ttl=10,
            owner="newer-owner",
        )
        assert newer_owner.acquire()
        resume_stale.set()
        assert refresh_future.result(timeout=10) is False

    assert rc.zscore(reservation.key, reservation.task_id) == score_before
    assert newer_owner.release()
    reservation.release()
