from __future__ import annotations

from pathlib import Path

import pytest

from orcest.workflow_store import DEFAULT_REDUCER_VERSION, RunStore
from orcest.workflow_store.v1.observability import collect_observability

pytestmark = pytest.mark.unit

RUN_ID = "11111111-1111-4111-8111-111111111111"
TRANSITION_ID = "22222222-2222-4222-8222-222222222222"
DIGEST = "sha256:" + "a" * 64


def _seed_transition(store: RunStore, project_id: str = "project-a") -> None:
    with store.transaction():
        store.create_run(
            run_id=RUN_ID,
            project_id=project_id,
            work_item_key="work-1",
            state="ADMITTED",
            specification_generation=1,
        )
        store.append_transition(
            run_id=RUN_ID,
            transition_id=TRANSITION_ID,
            prior_state="ADMITTED",
            trigger_kind="INTERNAL",
            trigger_id="internal-1",
            next_state="PLANNING",
            reducer_version=DEFAULT_REDUCER_VERSION,
            input_digest=DIGEST,
            specification_generation=1,
        )


def test_snapshot_rebuilds_from_durable_state_and_events_are_replay_safe(tmp_path: Path) -> None:
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        _seed_transition(store)
        first = collect_observability(store.conn, now_ms=2_000_000_000_000)
        replay = collect_observability(store.conn, now_ms=2_000_000_000_000)

        assert first.metrics == replay.metrics
        assert first.events == replay.events
        assert len(first.events) == 1
        assert first.events[0].event_id.startswith("sha256:")

    with RunStore(tmp_path, verify_local_filesystem=False) as restarted:
        rebuilt = collect_observability(restarted.conn, now_ms=2_000_000_000_000)

    assert rebuilt.metrics == first.metrics
    assert rebuilt.events == first.events


def test_packets_are_bounded_and_do_not_echo_arbitrary_project_text(tmp_path: Path) -> None:
    secret = "token with spaces and model output"
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        _seed_transition(store, project_id=secret)
        snapshot = collect_observability(
            store.conn,
            now_ms=2_000_000_000_000,
            redis_rebuild_ok=False,
            audit_write_ok=False,
        )

    rendered = snapshot.packet.to_json()
    assert len(rendered.encode()) <= 32_768
    assert secret not in rendered
    assert secret not in repr(snapshot.metrics)
    assert secret not in repr(snapshot.events)
    assert {gate.stage for gate in snapshot.packet.gates} == set(range(6))
