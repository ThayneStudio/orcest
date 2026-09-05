"""Real-Redis proof for capacity routing and cross-orchestrator reservation."""

from __future__ import annotations

import json
import logging
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from orcest.orchestrator.provider_capacity import (
    CapacityReservation,
    reserve_provider_capacity,
)
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
