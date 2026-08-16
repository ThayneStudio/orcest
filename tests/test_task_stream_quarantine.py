import json

import pytest

from orcest.task_stream_quarantine import (
    TaskStreamQuarantineError,
    quarantine_task_streams,
    restore_task_streams,
)

pytestmark = pytest.mark.unit


def test_quarantine_fails_closed_when_no_task_streams_exist(fake_redis_client):
    with pytest.raises(TaskStreamQuarantineError, match="no active task streams"):
        quarantine_task_streams(
            fake_redis_client,
            task_prefix="orcest",
            quarantine_id="release-1",
        )


def test_quarantine_and_restore_preserve_stream_payload_and_groups(fake_redis_client):
    source = "orcest:tasks:codex"
    fake_redis_client.client.xadd(source, {"id": "task-1", "credential": "secret"})
    fake_redis_client.client.xgroup_create(source, "workers", id="0")
    fake_redis_client.client.xreadgroup("workers", "orcest-worker-300", {source: ">"})

    # The retained PEL entry is a deliberate, operator-acknowledged exception.
    report = quarantine_task_streams(
        fake_redis_client,
        task_prefix="orcest",
        quarantine_id="release-1",
        force=True,
    )

    quarantine = "orcest:quarantine:release-1:tasks:codex"
    assert fake_redis_client.client.type(source) == "none"
    assert fake_redis_client.client.type(quarantine) == "stream"
    assert report["streams"][0]["pending"] == 1
    assert report["forced"] is True
    assert report["in_flight_streams"] == [source]
    assert "secret" not in json.dumps(report)

    # Candidate workers may create an empty active stream/group while the old
    # work remains fenced. Restore may replace that stream, but never work.
    fake_redis_client.client.xgroup_create(source, "workers", id="0", mkstream=True)
    restored = restore_task_streams(
        fake_redis_client,
        task_prefix="orcest",
        quarantine_id="release-1",
    )

    assert restored["ok"] is True
    assert fake_redis_client.client.type(quarantine) == "none"
    entries = fake_redis_client.client.xrange(source)
    assert entries[0][1]["credential"] == "secret"
    assert fake_redis_client.client.xinfo_groups(source)[0]["pending"] == 1


def test_quarantine_rejects_existing_destination(fake_redis_client):
    fake_redis_client.client.xadd("orcest:tasks:grok", {"id": "task-1"})
    fake_redis_client.client.xadd("orcest:quarantine:release-1:tasks:grok", {"id": "older-task"})

    with pytest.raises(TaskStreamQuarantineError, match="destination already exists"):
        quarantine_task_streams(
            fake_redis_client,
            task_prefix="orcest",
            quarantine_id="release-1",
        )

    assert fake_redis_client.client.type("orcest:tasks:grok") == "stream"


def test_restore_refuses_to_overwrite_active_work(fake_redis_client):
    fake_redis_client.client.xadd(
        "orcest:quarantine:release-1:tasks:issue:grok", {"id": "old-task"}
    )
    fake_redis_client.client.xadd("orcest:tasks:issue:grok", {"id": "new-task"})

    with pytest.raises(TaskStreamQuarantineError, match="contains work"):
        restore_task_streams(
            fake_redis_client,
            task_prefix="orcest",
            quarantine_id="release-1",
        )

    assert fake_redis_client.client.xlen("orcest:tasks:issue:grok") == 1


def test_restore_deduplicates_scan_results_without_losing_payload(fake_redis_client, mocker):
    quarantine = "orcest:quarantine:release-1:tasks:grok"
    active = "orcest:tasks:grok"
    fake_redis_client.client.xadd(quarantine, {"id": "old-task"})
    mocker.patch.object(
        fake_redis_client.client,
        "scan_iter",
        return_value=iter([quarantine, quarantine]),
    )

    report = restore_task_streams(
        fake_redis_client,
        task_prefix="orcest",
        quarantine_id="release-1",
    )

    assert report["ok"] is True
    assert len(report["streams"]) == 1
    assert fake_redis_client.client.xlen(active) == 1
    assert fake_redis_client.client.type(quarantine) == "none"


def test_quarantine_refuses_while_a_worker_heartbeat_is_live(fake_redis_client):
    """Fencing a stream a live worker holds deliveries on orphans its ACK."""
    source = "orcest:tasks:codex"
    fake_redis_client.client.xadd(source, {"id": "task-1"})
    fake_redis_client.client.set(
        "orcest:workers:heartbeat:orcest-worker-10000",
        json.dumps({"backend": "codex", "revision": "a" * 40}),
    )

    with pytest.raises(TaskStreamQuarantineError, match="orcest-worker-10000"):
        quarantine_task_streams(
            fake_redis_client,
            task_prefix="orcest",
            quarantine_id="release-1",
        )

    assert fake_redis_client.client.type(source) == "stream"
    assert fake_redis_client.client.type("orcest:quarantine:release-1:tasks:codex") == "none"


def test_quarantine_refuses_while_deliveries_are_pending(fake_redis_client):
    source = "orcest:tasks:grok"
    fake_redis_client.client.xadd(source, {"id": "task-1"})
    fake_redis_client.client.xgroup_create(source, "workers", id="0")
    fake_redis_client.client.xreadgroup("workers", "orcest-worker-10000", {source: ">"})

    with pytest.raises(TaskStreamQuarantineError, match="pending deliveries"):
        quarantine_task_streams(
            fake_redis_client,
            task_prefix="orcest",
            quarantine_id="release-1",
        )

    assert fake_redis_client.client.xinfo_groups(source)[0]["pending"] == 1


def test_quarantine_proceeds_for_a_drained_fleet(fake_redis_client):
    source = "orcest:tasks:clauder"
    fake_redis_client.client.xadd(source, {"id": "task-1"})
    fake_redis_client.client.xgroup_create(source, "workers", id="0")

    report = quarantine_task_streams(
        fake_redis_client,
        task_prefix="orcest",
        quarantine_id="release-1",
    )

    assert report["forced"] is False
    assert report["live_workers"] == []
    assert report["in_flight_streams"] == []
    assert fake_redis_client.client.type("orcest:quarantine:release-1:tasks:clauder") == "stream"


@pytest.mark.parametrize("quarantine_id", ["", "bad/id", "x" * 129])
def test_quarantine_rejects_unsafe_id(fake_redis_client, quarantine_id):
    with pytest.raises(TaskStreamQuarantineError, match="invalid quarantine ID"):
        quarantine_task_streams(
            fake_redis_client,
            task_prefix="orcest",
            quarantine_id=quarantine_id,
        )
