"""Projection Outbox delivery and rebuild (issue #694).

Every ledger Transition already enqueues a durable ``kind = RUN_STATUS``
Projection Outbox row (see ``orcest.workflow_reducer.ledger.apply``); this
suite drives that row through a fake forge adapter to exercise delivery,
transient-failure retry, and SQLite-authoritative rebuild after the forge
projection is deleted or drifts.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from orcest.workflow_store.store import RunStore
from orcest.workflow_store.v1.projection_dispatch import (
    ProjectionDeliveryError,
    dispatch_pending_projections,
    extract_projection_marker,
    rebuild_all_projections,
    rebuild_run_projection,
    render_projection_marker,
)

pytestmark = pytest.mark.unit


def _uid() -> str:
    return str(uuid.uuid4())


@pytest.fixture
def store(tmp_path: Path) -> RunStore:
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        yield store


def _create_recovering_run(store: RunStore, run_id: str, *, project_id: str = "project-a") -> None:
    from orcest.workflow_contract.v1.digest import request_digest

    with store.transaction():
        store.create_run(
            run_id=run_id,
            project_id=project_id,
            work_item_key=f"work-{run_id}",
            state="RECOVERING",
            specification_generation=1,
        )
        payload = {"recovery_origin_state": "BUILDING"}
        store.put_revisioned_object(
            object_kind="run_pointers",
            object_id=run_id,
            expected_revision=0,
            payload_digest=request_digest(payload),
            payload=payload,
        )


def _cancel(store: RunStore, run_id: str) -> None:
    from orcest.workflow_reducer.ledger import load_view

    view = load_view(store, run_id)
    assert view is not None
    store.submit_management_command(
        command_id=_uid(),
        run_id=run_id,
        kind="CANCEL",
        expected_last_transition_sequence=view.next_transition_sequence - 1,
        payload={},
        authenticated_principal_id="ops-lead",
        authorization_context_digest="sha256:" + "0" * 64,
    )


@dataclass
class _FakeAdapter:
    calls: list[dict] = field(default_factory=list)
    fail_next: int = 0

    def mutate_projection(self, *, target_kind, target_id, run_id, projection_kind, payload):
        if self.fail_next > 0:
            self.fail_next -= 1
            raise ProjectionDeliveryError("transient forge outage")
        self.calls.append(
            {
                "target_kind": target_kind,
                "target_id": target_id,
                "run_id": run_id,
                "projection_kind": projection_kind,
                "payload": dict(payload),
            }
        )


def test_dispatch_delivers_pending_run_status_projection(store: RunStore) -> None:
    run_id = _uid()
    _create_recovering_run(store, run_id)
    _cancel(store, run_id)
    adapter = _FakeAdapter()

    dispatched = dispatch_pending_projections(store, adapter)

    assert dispatched == 1
    assert len(adapter.calls) == 1
    call = adapter.calls[0]
    assert call["target_kind"] == "WORK_ITEM"
    assert call["target_id"] == f"work-{run_id}"
    assert call["run_id"] == run_id
    assert call["projection_kind"] == "RUN_STATUS"
    assert call["payload"]["state"] == "CANCELLED"
    assert call["payload"]["reason_code"] == "CANCEL"

    row = store.get_latest_projection_outbox_for_run(run_id)
    assert row is not None
    assert row.state == "DELIVERED"

    assert dispatch_pending_projections(store, adapter) == 0
    assert len(adapter.calls) == 1


def test_dispatch_leaves_transient_failure_pending_for_retry(store: RunStore) -> None:
    run_id = _uid()
    _create_recovering_run(store, run_id)
    _cancel(store, run_id)
    adapter = _FakeAdapter(fail_next=1)

    assert dispatch_pending_projections(store, adapter) == 0
    row = store.get_latest_projection_outbox_for_run(run_id)
    assert row is not None
    assert row.state == "PENDING"

    assert dispatch_pending_projections(store, adapter) == 1
    row = store.get_latest_projection_outbox_for_run(run_id)
    assert row is not None
    assert row.state == "DELIVERED"


def test_rebuild_redelivers_after_forge_projection_is_deleted(store: RunStore) -> None:
    run_id = _uid()
    _create_recovering_run(store, run_id)
    _cancel(store, run_id)
    adapter = _FakeAdapter()

    dispatch_pending_projections(store, adapter)
    assert len(adapter.calls) == 1  # forge now has the label/comment...

    # ...then someone deletes it out from under Orcest. Nothing in SQLite
    # changed, and rebuild must not depend on the row still being PENDING.
    result = rebuild_run_projection(store, adapter, run_id)

    assert result.rebuilt is True
    assert len(adapter.calls) == 2
    assert adapter.calls[1]["payload"]["state"] == "CANCELLED"


def test_rebuild_reports_no_projection_for_unknown_run(store: RunStore) -> None:
    adapter = _FakeAdapter()
    result = rebuild_run_projection(store, adapter, _uid())
    assert result.rebuilt is False
    assert result.projection_outbox_id is None


def test_rebuild_all_projections_sweeps_every_run_independently(store: RunStore) -> None:
    run_a, run_b = _uid(), _uid()
    _create_recovering_run(store, run_a)
    _create_recovering_run(store, run_b)
    _cancel(store, run_a)
    _cancel(store, run_b)
    adapter = _FakeAdapter()

    results = rebuild_all_projections(store, adapter)

    assert {r.run_id for r in results} == {run_a, run_b}
    assert all(r.rebuilt for r in results)
    assert len(adapter.calls) == 2


def test_projection_marker_round_trips() -> None:
    run_id = _uid()
    marker = render_projection_marker(run_id=run_id, projection_kind="RUN_STATUS")
    body = f"Status: cancelled\n\n{marker}"

    assert extract_projection_marker(body) == ("RUN_STATUS", run_id)
    assert extract_projection_marker("no marker here") is None
