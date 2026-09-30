from __future__ import annotations

from pathlib import Path

import pytest

from orcest.workflow_contract.v1.digest import request_digest
from orcest.workflow_reducer.contract import default_view
from orcest.workflow_reducer.ledger import apply, load_view
from orcest.workflow_reducer.types import Trigger
from orcest.workflow_store import RunStore

pytestmark = pytest.mark.unit

RUN_ID = "11111111-1111-1111-1111-111111111111"


class _Ids:
    def __init__(self) -> None:
        self.n = 0

    def __call__(self) -> str:
        self.n += 1
        return f"{self.n:08x}-0000-4000-8000-000000000000"


def test_admit_spec_supersede_internal_are_three_transitions(tmp_path: Path) -> None:
    ids = _Ids()
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        with store.transaction():
            admitted = apply(
                store,
                default_view(None, "ADMIT"),
                Trigger(
                    kind="ADMIT",
                    trigger_id="obs-work-1",
                    facts={
                        "snapshot_id": "22222222-2222-2222-2222-222222222222",
                        "base_observation_id": "obs-base-1",
                        "project_id": "project-a",
                        "work_item_key": "work-1",
                    },
                ),
                run_id=RUN_ID,
                id_factory=ids,
            )
        assert admitted.transition.prior_state == "NONE"
        assert admitted.transition.next_state == "ADMITTED"
        assert admitted.transition.specification_generation == 0
        assert admitted.transition.input_digest == request_digest(
            {
                "trigger_kind": "ADMIT",
                "trigger_id": "obs-work-1",
                "facts": {
                    "snapshot_id": "22222222-2222-2222-2222-222222222222",
                    "base_observation_id": "obs-base-1",
                    "project_id": "project-a",
                    "work_item_key": "work-1",
                },
                "prior_state": "NONE",
                "reason_code": "ADMIT",
            }
        )
        view = load_view(store, RUN_ID)
        assert view is not None
        assert view.pending_snapshot_id == "22222222-2222-2222-2222-222222222222"
        with store.transaction():
            installed = apply(
                store,
                view,
                Trigger(
                    kind="SPEC_SUPERSEDE",
                    trigger_id="22222222-2222-2222-2222-222222222222",
                    facts={"install": True},
                ),
                run_id=RUN_ID,
                id_factory=ids,
            )
        assert installed.transition.next_state == "ADMITTED"
        assert installed.transition.specification_generation == 1
        view = load_view(store, RUN_ID)
        assert view is not None
        assert view.generation_installed
        with store.transaction():
            planned = apply(
                store,
                view,
                Trigger(kind="INTERNAL", trigger_id=str(view.pending_internal_sequence), facts={}),
                run_id=RUN_ID,
                id_factory=ids,
            )
        assert planned.transition.next_state == "PLANNING"
        assert planned.planned_activity_ids
        assert planned.outbox_ids
        assert store.conn.execute("SELECT COUNT(*) FROM transitions").fetchone()[0] == 3


def test_duplicate_trigger_replays_without_second_effect(tmp_path: Path) -> None:
    ids = _Ids()
    view = default_view(None, "ADMIT")
    trigger = Trigger(
        kind="ADMIT",
        trigger_id="obs-work-1",
        facts={
            "snapshot_id": "22222222-2222-2222-2222-222222222222",
            "base_observation_id": "obs-base-1",
            "project_id": "project-a",
            "work_item_key": "work-1",
        },
    )
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        with store.transaction():
            first = apply(store, view, trigger, run_id=RUN_ID, id_factory=ids)
        with store.transaction():
            replayed = apply(store, view, trigger, run_id=RUN_ID, id_factory=ids)
        assert replayed.replayed is True
        assert replayed.transition.transition_id == first.transition.transition_id
        assert store.conn.execute("SELECT COUNT(*) FROM transitions").fetchone()[0] == 1


def test_admit_observation_cannot_be_reused_as_forge_observation(tmp_path: Path) -> None:
    ids = _Ids()
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        with store.transaction():
            apply(
                store,
                default_view(None, "ADMIT"),
                Trigger(
                    kind="ADMIT",
                    trigger_id="obs-work-1",
                    facts={
                        "snapshot_id": "22222222-2222-2222-2222-222222222222",
                        "base_observation_id": "obs-base-1",
                        "project_id": "project-a",
                        "work_item_key": "work-1",
                    },
                ),
                run_id=RUN_ID,
                id_factory=ids,
            )
        view = load_view(store, RUN_ID)
        assert view is not None
        with store.transaction():
            replayed = apply(
                store,
                view,
                Trigger(
                    kind="FORGE_OBSERVATION",
                    trigger_id="obs-work-1",
                    facts={"kind": "WORK_ITEM_SNAPSHOT"},
                ),
                run_id=RUN_ID,
                id_factory=ids,
            )
        assert replayed.replayed is True
        assert replayed.transition.trigger_kind == "ADMIT"
        assert store.conn.execute("SELECT COUNT(*) FROM transitions").fetchone()[0] == 1


def test_reordered_equivalent_reviews_share_aggregating_pointers(tmp_path: Path) -> None:
    def run_order(order: tuple[str, str]) -> tuple[str, str]:
        ids = _Ids()
        root = tmp_path / "-".join(order)
        with RunStore(root, verify_local_filesystem=False) as store:
            with store.transaction():
                store.create_run(
                    run_id=RUN_ID,
                    project_id="project-a",
                    work_item_key="work-1",
                    state="REVIEWING",
                    specification_generation=1,
                )
            for index, attempt_id in enumerate(order):
                complete = index == len(order) - 1
                current = default_view(
                    "REVIEWING",
                    "ATTEMPT_RESULT",
                    panel_complete=complete,
                    filling_review_slots=("slot-a", "slot-b") if complete else ("slot-a",),
                    unfilled_review_slots=() if complete else ("slot-b",),
                    revision=index,
                )
                with store.transaction():
                    apply(
                        store,
                        current,
                        Trigger(
                            kind="ATTEMPT_RESULT",
                            trigger_id=attempt_id,
                            facts={
                                "outcome": "SUCCEEDED",
                                "activity_kind": "REVIEW",
                                "fills_slot": True,
                                "panel_complete": complete,
                            },
                        ),
                        run_id=RUN_ID,
                        id_factory=ids,
                    )
            loaded = load_view(store, RUN_ID)
            assert loaded is not None
            last = store.list_transitions(RUN_ID)[-1]
            assert loaded.state is not None
            return loaded.state, last.next_state

    left = run_order(("attempt-a", "attempt-b"))
    right = run_order(("attempt-b", "attempt-a"))
    assert left[0] == right[0] == "AGGREGATING"


def test_offer_gate_closed_plans_activity_without_outbox(tmp_path: Path) -> None:
    ids = _Ids()
    view = default_view("ADMITTED", "INTERNAL", offer_permitted=False)
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        with store.transaction():
            store.create_run(
                run_id=RUN_ID,
                project_id="project-a",
                work_item_key="work-1",
                state="ADMITTED",
                specification_generation=1,
            )
            applied = apply(
                store,
                view,
                Trigger(kind="INTERNAL", trigger_id="1", facts={}),
                run_id=RUN_ID,
                id_factory=ids,
            )
        assert applied.transition.next_state == "PLANNING"
        assert applied.planned_activity_ids
        assert applied.outbox_ids == ()
        assert store.conn.execute("SELECT COUNT(*) FROM outbox").fetchone()[0] == 0
