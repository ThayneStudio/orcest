"""Reconciled single-Publication creation (issue #692).

Covers: Publication/Effect/Checkpoint creation and crash-resume, the
complete marker search precedence/cardinality router, ref CAS never
overwriting a foreign commit, the ownership Human Boundary, Terminal
Duplicate Cleanup Reservation/Action processing, and the ACTIVE-gating
invariant.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from orcest.workflow_contract.v1.digest import request_digest
from orcest.workflow_contract.v1.publication import (
    decide_ref_cas,
    deterministic_publication_ref,
    render_run_marker,
)
from orcest.workflow_store import (
    DEFAULT_REDUCER_VERSION,
    ChangeRequestSearchMemberInput,
    IdempotencyConflictError,
    RunStore,
    RunStoreError,
)

pytestmark = pytest.mark.unit

RUN_ID = "11111111-1111-4111-8111-111111111111"
PUBLICATION_ID = "22222222-2222-4222-8222-222222222222"
ACTIVITY_ID = "33333333-3333-4333-8333-333333333333"
OUTBOX_ID = "44444444-4444-4444-8444-444444444444"
CANDIDATE_ID = "55555555-5555-4555-8555-555555555555"
SECRET_ID = "66666666-6666-4666-8666-666666666666"
CHECKPOINT_ID_1 = "77777777-7777-4777-8777-777777777771"
CHECKPOINT_ID_2 = "77777777-7777-4777-8777-777777777772"
CHECKPOINT_ID_3 = "77777777-7777-4777-8777-777777777773"
SEARCH_RESULT_ID = "88888888-8888-4888-8888-888888888888"
FORGE_OBS_ID = "99999999-9999-4999-8999-999999999999"
RESERVATION_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
RECONCILIATION_FACT_ID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
HUMAN_BOUNDARY_ID = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
ACTION_ID = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
ACTION_OUTBOX_ID = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"
CONTROLLER_FACT_ID = "ffffffff-ffff-4fff-8fff-ffffffffffff"

DESIRED_COMMIT = {"object_format": "sha1", "oid": "a" * 40}
BASE_COMMIT = {"object_format": "sha1", "oid": "b" * 40}


def _create_run(store: RunStore, run_id: str = RUN_ID) -> None:
    store.create_run(
        run_id=run_id,
        project_id="project-a",
        work_item_key=f"work-{run_id}",
        state="APPROVED",
        reducer_version=DEFAULT_REDUCER_VERSION,
    )


def _plan_effect(store: RunStore, *, publication_id: str = PUBLICATION_ID) -> tuple:
    return store.plan_publish_effect(
        publication_id=publication_id,
        run_id=RUN_ID,
        activity_id=ACTIVITY_ID,
        activity_ordinal=1,
        specification_generation=0,
        policy_hash="sha256:" + "0" * 64,
        created_transition_sequence=1,
        candidate_id=CANDIDATE_ID,
        desired_commit=DESIRED_COMMIT,
        publication_secret_id=SECRET_ID,
        publication_secret_version=1,
        base_ref="refs/heads/main",
        base_commit=BASE_COMMIT,
        base_movement_policy="REBASE_BEFORE_PUBLICATION",
        deterministic_branch=deterministic_publication_ref(RUN_ID),
        run_marker=render_run_marker(run_id=RUN_ID, publication_id=publication_id),
        semantic_input={"publication_id": publication_id},
        semantic_input_digest=request_digest({"publication_id": publication_id}),
        idempotency_key=request_digest({"kind": "PUBLISH", "publication_id": publication_id}),
        outbox_id=OUTBOX_ID,
    )


@pytest.fixture
def store(tmp_path: Path) -> RunStore:
    with RunStore(tmp_path, verify_local_filesystem=False) as s:
        _create_run(s)
        yield s


def test_plan_publish_effect_creates_publication_effect_activity_outbox(store: RunStore) -> None:
    publication, effect, activity, outbox = _plan_effect(store)

    assert publication.publication_id == PUBLICATION_ID
    assert publication.run_id == RUN_ID
    assert publication.state == "PLANNED"
    assert publication.effect_generation == 1
    assert effect.mode == "INITIAL"
    assert effect.effect_generation == 1
    assert effect.expected_remote_commit is None
    assert activity.kind == "PUBLISH"
    assert activity.execution_class == "CONTROLLER"
    assert activity.state == "ACTIVE"
    assert outbox.publication_id == PUBLICATION_ID
    assert outbox.effect_generation == 1
    assert outbox.source_kind == "ACTIVITY"


def test_plan_publish_effect_replay_is_idempotent(store: RunStore) -> None:
    first = _plan_effect(store)
    second = _plan_effect(store)
    assert first == second


def test_plan_publish_effect_replay_conflict_raises(store: RunStore) -> None:
    _plan_effect(store)
    with pytest.raises(IdempotencyConflictError):
        store.plan_publish_effect(
            publication_id=PUBLICATION_ID,
            run_id=RUN_ID,
            activity_id=ACTIVITY_ID,
            activity_ordinal=1,
            specification_generation=0,
            policy_hash="sha256:" + "0" * 64,
            created_transition_sequence=1,
            candidate_id="ffffffff-ffff-4fff-8fff-fffffffffffe",  # different candidate
            desired_commit=DESIRED_COMMIT,
            publication_secret_id=SECRET_ID,
            publication_secret_version=1,
            base_ref="refs/heads/main",
            base_commit=BASE_COMMIT,
            base_movement_policy="REBASE_BEFORE_PUBLICATION",
            deterministic_branch=deterministic_publication_ref(RUN_ID),
            run_marker=render_run_marker(run_id=RUN_ID, publication_id=PUBLICATION_ID),
            semantic_input={"publication_id": PUBLICATION_ID},
            semantic_input_digest=request_digest({"publication_id": PUBLICATION_ID}),
            idempotency_key=request_digest({"kind": "PUBLISH", "publication_id": PUBLICATION_ID}),
            outbox_id=OUTBOX_ID,
        )


def test_plan_publish_effect_replay_conflict_checks_operation_digest(store: RunStore) -> None:
    _plan_effect(store)
    with pytest.raises(IdempotencyConflictError):
        store.plan_publish_effect(
            publication_id=PUBLICATION_ID,
            run_id=RUN_ID,
            activity_id=ACTIVITY_ID,
            activity_ordinal=1,
            specification_generation=0,
            policy_hash="sha256:" + "0" * 64,
            created_transition_sequence=1,
            candidate_id=CANDIDATE_ID,
            desired_commit={"object_format": "sha1", "oid": "c" * 40},
            publication_secret_id=SECRET_ID,
            publication_secret_version=1,
            base_ref="refs/heads/main",
            base_commit=BASE_COMMIT,
            base_movement_policy="REBASE_BEFORE_PUBLICATION",
            deterministic_branch=deterministic_publication_ref(RUN_ID),
            run_marker=render_run_marker(run_id=RUN_ID, publication_id=PUBLICATION_ID),
            semantic_input={"publication_id": PUBLICATION_ID},
            semantic_input_digest=request_digest({"publication_id": PUBLICATION_ID}),
            idempotency_key=request_digest({"kind": "PUBLISH", "publication_id": PUBLICATION_ID}),
            outbox_id=OUTBOX_ID,
        )


def test_plan_publish_effect_second_generation_increments(store: RunStore) -> None:
    _plan_effect(store)
    publication2, effect2, _activity2, _outbox2 = store.plan_publish_effect(
        publication_id=PUBLICATION_ID,
        run_id=RUN_ID,
        activity_id="33333333-3333-4333-8333-333333333334",
        activity_ordinal=2,
        specification_generation=0,
        policy_hash="sha256:" + "0" * 64,
        created_transition_sequence=2,
        candidate_id=CANDIDATE_ID,
        desired_commit={"object_format": "sha1", "oid": "c" * 40},
        publication_secret_id=SECRET_ID,
        publication_secret_version=1,
        base_ref="refs/heads/main",
        base_commit=BASE_COMMIT,
        base_movement_policy="REBASE_BEFORE_PUBLICATION",
        deterministic_branch=deterministic_publication_ref(RUN_ID),
        run_marker=render_run_marker(run_id=RUN_ID, publication_id=PUBLICATION_ID),
        semantic_input={"publication_id": PUBLICATION_ID, "gen": 2},
        semantic_input_digest=request_digest({"publication_id": PUBLICATION_ID, "gen": 2}),
        idempotency_key=request_digest({"kind": "PUBLISH", "gen": 2}),
        outbox_id="44444444-4444-4444-8444-444444444445",
    )
    assert publication2.effect_generation == 2
    assert effect2.effect_generation == 2


def test_ref_read_checkpoint_advances_to_branch_observed(store: RunStore) -> None:
    _plan_effect(store)
    checkpoint = store.record_publication_effect_checkpoint(
        publication_effect_checkpoint_id=CHECKPOINT_ID_1,
        publication_id=PUBLICATION_ID,
        effect_generation=1,
        suboperation_kind="REF_READ",
        status="OBSERVED_ABSENT",
        forge_observation_id=FORGE_OBS_ID,
        observed_external_revision=None,
    )
    assert checkpoint.checkpoint_sequence == 1
    publication = store.get_publication(PUBLICATION_ID)
    assert publication is not None
    assert publication.state == "BRANCH_OBSERVED"


def test_checkpoint_sequence_strictly_increases_and_resumes(store: RunStore) -> None:
    _plan_effect(store)
    store.record_publication_effect_checkpoint(
        publication_effect_checkpoint_id=CHECKPOINT_ID_1,
        publication_id=PUBLICATION_ID,
        effect_generation=1,
        suboperation_kind="BASE_READ_PRE",
        status="OBSERVED_SATISFIED",
        forge_observation_id=FORGE_OBS_ID,
    )
    store.record_publication_effect_checkpoint(
        publication_effect_checkpoint_id=CHECKPOINT_ID_2,
        publication_id=PUBLICATION_ID,
        effect_generation=1,
        suboperation_kind="REF_READ",
        status="OBSERVED_ABSENT",
        forge_observation_id=FORGE_OBS_ID,
    )
    checkpoints = store.list_publication_effect_checkpoints(PUBLICATION_ID, 1)
    assert [c.checkpoint_sequence for c in checkpoints] == [1, 2]
    assert [c.suboperation_kind for c in checkpoints] == ["BASE_READ_PRE", "REF_READ"]


def test_checkpoint_replay_returns_existing_row(store: RunStore) -> None:
    _plan_effect(store)
    first = store.record_publication_effect_checkpoint(
        publication_effect_checkpoint_id=CHECKPOINT_ID_1,
        publication_id=PUBLICATION_ID,
        effect_generation=1,
        suboperation_kind="BASE_READ_PRE",
        status="OBSERVED_SATISFIED",
        forge_observation_id=FORGE_OBS_ID,
    )
    second = store.record_publication_effect_checkpoint(
        publication_effect_checkpoint_id=CHECKPOINT_ID_1,
        publication_id=PUBLICATION_ID,
        effect_generation=1,
        suboperation_kind="BASE_READ_PRE",
        status="OBSERVED_SATISFIED",
        forge_observation_id=FORGE_OBS_ID,
    )
    assert second == first
    assert len(store.list_publication_effect_checkpoints(PUBLICATION_ID, 1)) == 1


def test_request_ready_checkpoint_replay_with_fresh_id_reuses_existing_row(
    store: RunStore,
) -> None:
    _plan_effect(store)
    first = store.record_publication_effect_checkpoint(
        publication_effect_checkpoint_id=CHECKPOINT_ID_1,
        publication_id=PUBLICATION_ID,
        effect_generation=1,
        suboperation_kind="REF_CREATE",
        status="REQUEST_READY",
        request_idempotency_key="ref-create-1",
    )
    second = store.record_publication_effect_checkpoint(
        publication_effect_checkpoint_id=CHECKPOINT_ID_2,
        publication_id=PUBLICATION_ID,
        effect_generation=1,
        suboperation_kind="REF_CREATE",
        status="REQUEST_READY",
        request_idempotency_key="ref-create-1",
    )
    assert second == first
    with pytest.raises(IdempotencyConflictError):
        store.record_publication_effect_checkpoint(
            publication_effect_checkpoint_id=CHECKPOINT_ID_3,
            publication_id=PUBLICATION_ID,
            effect_generation=1,
            suboperation_kind="REF_CREATE",
            status="REQUEST_READY",
            request_idempotency_key="ref-create-2",
        )


def test_checkpoint_matrix_rejects_missing_request_key(store: RunStore) -> None:
    _plan_effect(store)
    with pytest.raises(ValueError, match="requires request_idempotency_key"):
        store.record_publication_effect_checkpoint(
            publication_effect_checkpoint_id=CHECKPOINT_ID_1,
            publication_id=PUBLICATION_ID,
            effect_generation=1,
            suboperation_kind="REF_CREATE",
            status="REQUEST_READY",
        )


def test_checkpoint_matrix_rejects_forbidden_observation(store: RunStore) -> None:
    _plan_effect(store)
    with pytest.raises(ValueError, match="forbids forge_observation_id"):
        store.record_publication_effect_checkpoint(
            publication_effect_checkpoint_id=CHECKPOINT_ID_1,
            publication_id=PUBLICATION_ID,
            effect_generation=1,
            suboperation_kind="REF_CREATE",
            status="REQUEST_READY",
            request_idempotency_key="req-key-1",
            forge_observation_id=FORGE_OBS_ID,
        )


def test_stale_generation_checkpoint_is_audit_only(store: RunStore) -> None:
    _plan_effect(store)
    store.plan_publish_effect(
        publication_id=PUBLICATION_ID,
        run_id=RUN_ID,
        activity_id="33333333-3333-4333-8333-333333333334",
        activity_ordinal=2,
        specification_generation=0,
        policy_hash="sha256:" + "0" * 64,
        created_transition_sequence=2,
        candidate_id=CANDIDATE_ID,
        desired_commit={"object_format": "sha1", "oid": "c" * 40},
        publication_secret_id=SECRET_ID,
        publication_secret_version=1,
        base_ref="refs/heads/main",
        base_commit=BASE_COMMIT,
        base_movement_policy="REBASE_BEFORE_PUBLICATION",
        deterministic_branch=deterministic_publication_ref(RUN_ID),
        run_marker=render_run_marker(run_id=RUN_ID, publication_id=PUBLICATION_ID),
        semantic_input={"publication_id": PUBLICATION_ID, "gen": 2},
        semantic_input_digest=request_digest({"publication_id": PUBLICATION_ID, "gen": 2}),
        idempotency_key=request_digest({"kind": "PUBLISH", "gen": 2}),
        outbox_id="44444444-4444-4444-8444-444444444445",
    )
    # generation 1 is now stale (current generation is 2)
    store.record_publication_effect_checkpoint(
        publication_effect_checkpoint_id=CHECKPOINT_ID_1,
        publication_id=PUBLICATION_ID,
        effect_generation=1,
        suboperation_kind="REF_READ",
        status="OBSERVED_ABSENT",
        forge_observation_id=FORGE_OBS_ID,
    )
    publication = store.get_publication(PUBLICATION_ID)
    assert publication is not None
    assert publication.state == "PLANNED"  # untouched by the stale-generation checkpoint


def _member(**kwargs: object) -> ChangeRequestSearchMemberInput:
    base = dict(
        member_class="LIVE",
        change_request_external_id="1",
        observed_head=DESIRED_COMMIT,
        source_ref=deterministic_publication_ref(RUN_ID),
        run_marker=render_run_marker(run_id=RUN_ID, publication_id=PUBLICATION_ID),
        observed_body_revision="rev-1",
        ownership_status="POSITIVE",
        proof_kind="LIVE_ASSOCIATION",
    )
    base.update(kwargs)
    return ChangeRequestSearchMemberInput(**base)  # type: ignore[arg-type]


def test_one_live_requires_fresh_exact_object_confirmation(store: RunStore) -> None:
    _plan_effect(store)
    with pytest.raises(ValueError, match="fresh exact-object"):
        store.record_change_request_search_result(
            change_request_search_result_id=SEARCH_RESULT_ID,
            forge_observation_id=FORGE_OBS_ID,
            publication_effect_checkpoint_id=CHECKPOINT_ID_1,
            publication_id=PUBLICATION_ID,
            effect_generation=1,
            project_id="project-a",
            run_marker=render_run_marker(run_id=RUN_ID, publication_id=PUBLICATION_ID),
            deterministic_ref=deterministic_publication_ref(RUN_ID),
            external_revision="search-rev-1",
            members=(_member(),),
            fresh_exact_object_confirmed=False,
        )


def test_one_live_advances_to_change_request_observed(store: RunStore) -> None:
    _plan_effect(store)
    record, outcome = store.record_change_request_search_result(
        change_request_search_result_id=SEARCH_RESULT_ID,
        forge_observation_id=FORGE_OBS_ID,
        publication_effect_checkpoint_id=CHECKPOINT_ID_1,
        publication_id=PUBLICATION_ID,
        effect_generation=1,
        project_id="project-a",
        run_marker=render_run_marker(run_id=RUN_ID, publication_id=PUBLICATION_ID),
        deterministic_ref=deterministic_publication_ref(RUN_ID),
        external_revision="search-rev-1",
        members=(_member(),),
        fresh_exact_object_confirmed=True,
    )
    assert outcome.outcome == "ONE_LIVE"
    assert record.live_cardinality == "ONE"
    publication = store.get_publication(PUBLICATION_ID)
    assert publication is not None
    assert publication.state == "CHANGE_REQUEST_OBSERVED"
    assert publication.change_request_external_id == "1"


def test_change_request_search_result_replay_after_terminalization_is_idempotent(
    store: RunStore,
) -> None:
    _plan_effect(store)
    members = (
        _member(
            member_class="TERMINAL",
            change_request_external_id="5",
            terminal_state="CLOSED",
        ),
    )
    first = store.record_change_request_search_result(
        change_request_search_result_id=SEARCH_RESULT_ID,
        forge_observation_id=FORGE_OBS_ID,
        publication_effect_checkpoint_id=CHECKPOINT_ID_1,
        publication_id=PUBLICATION_ID,
        effect_generation=1,
        project_id="project-a",
        run_marker=render_run_marker(run_id=RUN_ID, publication_id=PUBLICATION_ID),
        deterministic_ref=deterministic_publication_ref(RUN_ID),
        external_revision="search-rev-1",
        members=members,
        terminal_publication_effect_checkpoint_id=CHECKPOINT_ID_2,
    )
    second = store.record_change_request_search_result(
        change_request_search_result_id=SEARCH_RESULT_ID,
        forge_observation_id=FORGE_OBS_ID,
        publication_effect_checkpoint_id=CHECKPOINT_ID_1,
        publication_id=PUBLICATION_ID,
        effect_generation=1,
        project_id="project-a",
        run_marker=render_run_marker(run_id=RUN_ID, publication_id=PUBLICATION_ID),
        deterministic_ref=deterministic_publication_ref(RUN_ID),
        external_revision="search-rev-1",
        members=members,
        terminal_publication_effect_checkpoint_id=CHECKPOINT_ID_2,
    )
    assert second == first


def test_change_request_search_result_replay_conflict_raises(store: RunStore) -> None:
    _plan_effect(store)
    store.record_change_request_search_result(
        change_request_search_result_id=SEARCH_RESULT_ID,
        forge_observation_id=FORGE_OBS_ID,
        publication_effect_checkpoint_id=CHECKPOINT_ID_1,
        publication_id=PUBLICATION_ID,
        effect_generation=1,
        project_id="project-a",
        run_marker=render_run_marker(run_id=RUN_ID, publication_id=PUBLICATION_ID),
        deterministic_ref=deterministic_publication_ref(RUN_ID),
        external_revision="search-rev-1",
        members=(),
    )
    with pytest.raises(IdempotencyConflictError):
        store.record_change_request_search_result(
            change_request_search_result_id=SEARCH_RESULT_ID,
            forge_observation_id=FORGE_OBS_ID,
            publication_effect_checkpoint_id=CHECKPOINT_ID_1,
            publication_id=PUBLICATION_ID,
            effect_generation=1,
            project_id="project-a",
            run_marker=render_run_marker(run_id=RUN_ID, publication_id=PUBLICATION_ID),
            deterministic_ref=deterministic_publication_ref(RUN_ID),
            external_revision="search-rev-2",
            members=(),
        )


def test_complete_publication_effect_requires_base_read_post(store: RunStore) -> None:
    _plan_effect(store)
    store.record_change_request_search_result(
        change_request_search_result_id=SEARCH_RESULT_ID,
        forge_observation_id=FORGE_OBS_ID,
        publication_effect_checkpoint_id=CHECKPOINT_ID_1,
        publication_id=PUBLICATION_ID,
        effect_generation=1,
        project_id="project-a",
        run_marker=render_run_marker(run_id=RUN_ID, publication_id=PUBLICATION_ID),
        deterministic_ref=deterministic_publication_ref(RUN_ID),
        external_revision="search-rev-1",
        members=(_member(),),
        fresh_exact_object_confirmed=True,
    )
    with pytest.raises(RunStoreError, match="BASE_READ_POST"):
        store.complete_publication_effect(
            publication_effect_checkpoint_id=CHECKPOINT_ID_2,
            publication_id=PUBLICATION_ID,
            effect_generation=1,
            forge_observation_id=FORGE_OBS_ID,
            observed_external_revision="a" * 40,
        )


def test_complete_publication_effect_sets_active(store: RunStore) -> None:
    _plan_effect(store)
    store.record_change_request_search_result(
        change_request_search_result_id=SEARCH_RESULT_ID,
        forge_observation_id=FORGE_OBS_ID,
        publication_effect_checkpoint_id=CHECKPOINT_ID_1,
        publication_id=PUBLICATION_ID,
        effect_generation=1,
        project_id="project-a",
        run_marker=render_run_marker(run_id=RUN_ID, publication_id=PUBLICATION_ID),
        deterministic_ref=deterministic_publication_ref(RUN_ID),
        external_revision="search-rev-1",
        members=(_member(),),
        fresh_exact_object_confirmed=True,
    )
    store.record_publication_effect_checkpoint(
        publication_effect_checkpoint_id=CHECKPOINT_ID_2,
        publication_id=PUBLICATION_ID,
        effect_generation=1,
        suboperation_kind="BASE_READ_POST",
        status="OBSERVED_SATISFIED",
        forge_observation_id=FORGE_OBS_ID,
    )
    store.complete_publication_effect(
        publication_effect_checkpoint_id=CHECKPOINT_ID_3,
        publication_id=PUBLICATION_ID,
        effect_generation=1,
        forge_observation_id=FORGE_OBS_ID,
        observed_external_revision="a" * 40,
    )
    publication = store.get_publication(PUBLICATION_ID)
    assert publication is not None
    assert publication.state == "ACTIVE"
    checkpoints = store.list_publication_effect_checkpoints(PUBLICATION_ID, 1)
    assert checkpoints[-1].suboperation_kind == "COMPLETE"
    assert checkpoints[-1].status == "COMPLETED"


def test_complete_publication_effect_replay_after_active_returns_checkpoint(
    store: RunStore,
) -> None:
    _plan_effect(store)
    store.record_change_request_search_result(
        change_request_search_result_id=SEARCH_RESULT_ID,
        forge_observation_id=FORGE_OBS_ID,
        publication_effect_checkpoint_id=CHECKPOINT_ID_1,
        publication_id=PUBLICATION_ID,
        effect_generation=1,
        project_id="project-a",
        run_marker=render_run_marker(run_id=RUN_ID, publication_id=PUBLICATION_ID),
        deterministic_ref=deterministic_publication_ref(RUN_ID),
        external_revision="search-rev-1",
        members=(_member(),),
        fresh_exact_object_confirmed=True,
    )
    store.record_publication_effect_checkpoint(
        publication_effect_checkpoint_id=CHECKPOINT_ID_2,
        publication_id=PUBLICATION_ID,
        effect_generation=1,
        suboperation_kind="BASE_READ_POST",
        status="OBSERVED_SATISFIED",
        forge_observation_id=FORGE_OBS_ID,
    )
    first = store.complete_publication_effect(
        publication_effect_checkpoint_id=CHECKPOINT_ID_3,
        publication_id=PUBLICATION_ID,
        effect_generation=1,
        forge_observation_id=FORGE_OBS_ID,
        observed_external_revision="a" * 40,
    )
    second = store.complete_publication_effect(
        publication_effect_checkpoint_id=CHECKPOINT_ID_3,
        publication_id=PUBLICATION_ID,
        effect_generation=1,
        forge_observation_id=FORGE_OBS_ID,
        observed_external_revision="a" * 40,
    )
    assert second == first


def test_stale_complete_checkpoint_is_audit_only(store: RunStore) -> None:
    _plan_effect(store)
    store.plan_publish_effect(
        publication_id=PUBLICATION_ID,
        run_id=RUN_ID,
        activity_id="33333333-3333-4333-8333-333333333334",
        activity_ordinal=2,
        specification_generation=0,
        policy_hash="sha256:" + "0" * 64,
        created_transition_sequence=2,
        candidate_id=CANDIDATE_ID,
        desired_commit={"object_format": "sha1", "oid": "c" * 40},
        publication_secret_id=SECRET_ID,
        publication_secret_version=1,
        base_ref="refs/heads/main",
        base_commit=BASE_COMMIT,
        base_movement_policy="REBASE_BEFORE_PUBLICATION",
        deterministic_branch=deterministic_publication_ref(RUN_ID),
        run_marker=render_run_marker(run_id=RUN_ID, publication_id=PUBLICATION_ID),
        semantic_input={"publication_id": PUBLICATION_ID, "gen": 2},
        semantic_input_digest=request_digest({"publication_id": PUBLICATION_ID, "gen": 2}),
        idempotency_key=request_digest({"kind": "PUBLISH", "gen": 2}),
        outbox_id="44444444-4444-4444-8444-444444444445",
    )
    checkpoint = store.complete_publication_effect(
        publication_effect_checkpoint_id=CHECKPOINT_ID_1,
        publication_id=PUBLICATION_ID,
        effect_generation=1,
        forge_observation_id=FORGE_OBS_ID,
        observed_external_revision="a" * 40,
    )
    assert checkpoint.effect_generation == 1
    publication = store.get_publication(PUBLICATION_ID)
    assert publication is not None
    assert publication.effect_generation == 2
    assert publication.state == "PLANNED"


def test_multiple_live_selects_bytewise_lowest_and_does_not_link(store: RunStore) -> None:
    _plan_effect(store)
    members = (
        _member(change_request_external_id="9"),
        _member(change_request_external_id="10"),
    )
    record, outcome = store.record_change_request_search_result(
        change_request_search_result_id=SEARCH_RESULT_ID,
        forge_observation_id=FORGE_OBS_ID,
        publication_effect_checkpoint_id=CHECKPOINT_ID_1,
        publication_id=PUBLICATION_ID,
        effect_generation=1,
        project_id="project-a",
        run_marker=render_run_marker(run_id=RUN_ID, publication_id=PUBLICATION_ID),
        deterministic_ref=deterministic_publication_ref(RUN_ID),
        external_revision="search-rev-1",
        members=members,
    )
    assert outcome.outcome == "MULTIPLE_LIVE"
    assert outcome.retained_live_external_id == "10"
    assert record.live_cardinality == "MULTIPLE"
    publication = store.get_publication(PUBLICATION_ID)
    assert publication is not None
    assert publication.state == "PLANNED"  # unmutated -- no linkage at MULTIPLE-live


def test_zero_live_no_terminal_does_not_mutate(store: RunStore) -> None:
    _plan_effect(store)
    _record, outcome = store.record_change_request_search_result(
        change_request_search_result_id=SEARCH_RESULT_ID,
        forge_observation_id=FORGE_OBS_ID,
        publication_effect_checkpoint_id=CHECKPOINT_ID_1,
        publication_id=PUBLICATION_ID,
        effect_generation=1,
        project_id="project-a",
        run_marker=render_run_marker(run_id=RUN_ID, publication_id=PUBLICATION_ID),
        deterministic_ref=deterministic_publication_ref(RUN_ID),
        external_revision="search-rev-1",
        members=(),
    )
    assert outcome.outcome == "ZERO_LIVE_NO_TERMINAL"
    publication = store.get_publication(PUBLICATION_ID)
    assert publication is not None
    assert publication.state == "PLANNED"


def test_zero_live_closed_terminal_closes_publication_without_reservation(
    store: RunStore,
) -> None:
    _plan_effect(store)
    members = (
        _member(
            member_class="TERMINAL",
            change_request_external_id="5",
            terminal_state="CLOSED",
        ),
    )
    record, outcome = store.record_change_request_search_result(
        change_request_search_result_id=SEARCH_RESULT_ID,
        forge_observation_id=FORGE_OBS_ID,
        publication_effect_checkpoint_id=CHECKPOINT_ID_1,
        publication_id=PUBLICATION_ID,
        effect_generation=1,
        project_id="project-a",
        run_marker=render_run_marker(run_id=RUN_ID, publication_id=PUBLICATION_ID),
        deterministic_ref=deterministic_publication_ref(RUN_ID),
        external_revision="search-rev-1",
        members=members,
        terminal_publication_effect_checkpoint_id=CHECKPOINT_ID_2,
    )
    assert outcome.outcome == "ZERO_LIVE_CLOSED_TERMINAL"
    assert record.live_cardinality == "ZERO"
    publication = store.get_publication(PUBLICATION_ID)
    assert publication is not None
    assert publication.state == "CLOSED"
    assert publication.change_request_external_id == "5"
    assert publication.initial_link_terminal_state == "CLOSED"
    assert publication.terminal_duplicate_cleanup_reservation_id is None


def test_positive_closed_terminal_is_audit_only_when_live_exists(store: RunStore) -> None:
    _plan_effect(store)
    members = (
        _member(change_request_external_id="1"),
        _member(
            member_class="TERMINAL",
            change_request_external_id="5",
            terminal_state="CLOSED",
        ),
    )
    _record, outcome = store.record_change_request_search_result(
        change_request_search_result_id=SEARCH_RESULT_ID,
        forge_observation_id=FORGE_OBS_ID,
        publication_effect_checkpoint_id=CHECKPOINT_ID_1,
        publication_id=PUBLICATION_ID,
        effect_generation=1,
        project_id="project-a",
        run_marker=render_run_marker(run_id=RUN_ID, publication_id=PUBLICATION_ID),
        deterministic_ref=deterministic_publication_ref(RUN_ID),
        external_revision="search-rev-1",
        members=members,
        fresh_exact_object_confirmed=True,
    )
    assert outcome.outcome == "ONE_LIVE"
    assert outcome.selected_external_id == "1"


def test_merged_terminal_closes_publication_and_creates_reservation(store: RunStore) -> None:
    _plan_effect(store)
    members = (
        _member(member_class="LIVE", change_request_external_id="1"),
        _member(
            member_class="LIVE",
            change_request_external_id="2",
            ownership_status="INCOMPATIBLE",
            proof_kind=None,
            ownership_defect_codes=("REF_MISMATCH",),
        ),
        _member(
            member_class="TERMINAL",
            change_request_external_id="9",
            terminal_state="MERGED",
            merge_commit=DESIRED_COMMIT,
        ),
    )
    record, outcome = store.record_change_request_search_result(
        change_request_search_result_id=SEARCH_RESULT_ID,
        forge_observation_id=FORGE_OBS_ID,
        publication_effect_checkpoint_id=CHECKPOINT_ID_1,
        publication_id=PUBLICATION_ID,
        effect_generation=1,
        project_id="project-a",
        run_marker=render_run_marker(run_id=RUN_ID, publication_id=PUBLICATION_ID),
        deterministic_ref=deterministic_publication_ref(RUN_ID),
        external_revision="search-rev-1",
        members=members,
        terminal_publication_effect_checkpoint_id=CHECKPOINT_ID_2,
        terminal_duplicate_cleanup_reservation_id=RESERVATION_ID,
    )
    assert outcome.outcome == "MERGED_TERMINAL"
    assert record.live_cardinality == "MULTIPLE"
    publication = store.get_publication(PUBLICATION_ID)
    assert publication is not None
    assert publication.state == "CLOSED"
    assert publication.change_request_external_id == "9"
    assert publication.initial_link_terminal_state == "MERGED"
    assert publication.terminal_duplicate_cleanup_reservation_id == RESERVATION_ID

    reservation = store.get_terminal_duplicate_cleanup_reservation(RESERVATION_ID)
    assert reservation is not None
    assert reservation.state == "ACTIVE"
    assert len(reservation.members) == 2
    planned = {m.change_request_external_id: m.planned_action for m in reservation.members}
    assert planned["1"] == "CLOSE"  # POSITIVE + canonical-empty external reliance
    assert planned["2"] == "RECORD_ONLY"  # INCOMPATIBLE


def test_merged_terminal_replaces_prior_one_live_association(store: RunStore) -> None:
    _plan_effect(store)
    store.record_change_request_search_result(
        change_request_search_result_id=SEARCH_RESULT_ID,
        forge_observation_id=FORGE_OBS_ID,
        publication_effect_checkpoint_id=CHECKPOINT_ID_1,
        publication_id=PUBLICATION_ID,
        effect_generation=1,
        project_id="project-a",
        run_marker=render_run_marker(run_id=RUN_ID, publication_id=PUBLICATION_ID),
        deterministic_ref=deterministic_publication_ref(RUN_ID),
        external_revision="search-rev-1",
        members=(_member(change_request_external_id="1"),),
        fresh_exact_object_confirmed=True,
    )
    store.record_change_request_search_result(
        change_request_search_result_id="88888888-8888-4888-8888-888888888889",
        forge_observation_id="99999999-9999-4999-8999-999999999998",
        publication_effect_checkpoint_id=CHECKPOINT_ID_2,
        publication_id=PUBLICATION_ID,
        effect_generation=1,
        project_id="project-a",
        run_marker=render_run_marker(run_id=RUN_ID, publication_id=PUBLICATION_ID),
        deterministic_ref=deterministic_publication_ref(RUN_ID),
        external_revision="search-rev-2",
        members=(
            _member(member_class="LIVE", change_request_external_id="1"),
            _member(
                member_class="TERMINAL",
                change_request_external_id="9",
                terminal_state="MERGED",
                merge_commit=DESIRED_COMMIT,
            ),
        ),
        terminal_publication_effect_checkpoint_id=CHECKPOINT_ID_3,
        terminal_duplicate_cleanup_reservation_id=RESERVATION_ID,
    )
    publication = store.get_publication(PUBLICATION_ID)
    assert publication is not None
    assert publication.state == "CLOSED"
    assert publication.change_request_external_id == "9"
    assert publication.initial_link_retained_external_id == "9"


def test_terminal_duplicate_cleanup_action_processes_one_member_at_a_time(
    store: RunStore,
) -> None:
    _plan_effect(store)
    members = (
        _member(member_class="LIVE", change_request_external_id="1"),
        _member(member_class="LIVE", change_request_external_id="2"),
        _member(
            member_class="TERMINAL",
            change_request_external_id="9",
            terminal_state="MERGED",
        ),
    )
    store.record_change_request_search_result(
        change_request_search_result_id=SEARCH_RESULT_ID,
        forge_observation_id=FORGE_OBS_ID,
        publication_effect_checkpoint_id=CHECKPOINT_ID_1,
        publication_id=PUBLICATION_ID,
        effect_generation=1,
        project_id="project-a",
        run_marker=render_run_marker(run_id=RUN_ID, publication_id=PUBLICATION_ID),
        deterministic_ref=deterministic_publication_ref(RUN_ID),
        external_revision="search-rev-1",
        members=members,
        terminal_publication_effect_checkpoint_id=CHECKPOINT_ID_2,
        terminal_duplicate_cleanup_reservation_id=RESERVATION_ID,
    )

    action1 = store.record_terminal_duplicate_cleanup_action(
        terminal_duplicate_cleanup_action_id=ACTION_ID,
        terminal_duplicate_cleanup_reservation_id=RESERVATION_ID,
        outbox_id=ACTION_OUTBOX_ID,
    )
    assert action1.member_ordinal == 0
    assert action1.change_request_external_id == "1"
    assert action1.planned_action == "CLOSE"

    # Replay before completion returns the same still-active action.
    replay = store.record_terminal_duplicate_cleanup_action(
        terminal_duplicate_cleanup_action_id="ffffffff-ffff-4fff-8fff-fffffffffff1",
        terminal_duplicate_cleanup_reservation_id=RESERVATION_ID,
        outbox_id="ffffffff-ffff-4fff-8fff-fffffffffff2",
    )
    assert replay.terminal_duplicate_cleanup_action_id == ACTION_ID

    action1_done, reservation_after_1 = store.complete_terminal_duplicate_cleanup_action(
        terminal_duplicate_cleanup_action_id=ACTION_ID,
        outcome="CLOSED",
        forge_observation_id=FORGE_OBS_ID,
    )
    assert action1_done.outcome == "CLOSED"
    assert reservation_after_1.state == "ACTIVE"
    assert reservation_after_1.next_member_ordinal == 1

    action2 = store.record_terminal_duplicate_cleanup_action(
        terminal_duplicate_cleanup_action_id="ffffffff-ffff-4fff-8fff-fffffffffff3",
        terminal_duplicate_cleanup_reservation_id=RESERVATION_ID,
        outbox_id="ffffffff-ffff-4fff-8fff-fffffffffff4",
    )
    assert action2.member_ordinal == 1
    assert action2.change_request_external_id == "2"

    _action2_done, reservation_after_2 = store.complete_terminal_duplicate_cleanup_action(
        terminal_duplicate_cleanup_action_id="ffffffff-ffff-4fff-8fff-fffffffffff3",
        outcome="CLOSED",
        forge_observation_id="ffffffff-ffff-4fff-8fff-fffffffffff5",
    )
    assert reservation_after_2.state == "COMPLETED"


def test_complete_terminal_duplicate_cleanup_action_rejects_wrong_outcome(
    store: RunStore,
) -> None:
    _plan_effect(store)
    members = (
        _member(member_class="LIVE", change_request_external_id="1"),
        _member(
            member_class="TERMINAL",
            change_request_external_id="9",
            terminal_state="MERGED",
        ),
    )
    store.record_change_request_search_result(
        change_request_search_result_id=SEARCH_RESULT_ID,
        forge_observation_id=FORGE_OBS_ID,
        publication_effect_checkpoint_id=CHECKPOINT_ID_1,
        publication_id=PUBLICATION_ID,
        effect_generation=1,
        project_id="project-a",
        run_marker=render_run_marker(run_id=RUN_ID, publication_id=PUBLICATION_ID),
        deterministic_ref=deterministic_publication_ref(RUN_ID),
        external_revision="search-rev-1",
        members=members,
        terminal_publication_effect_checkpoint_id=CHECKPOINT_ID_2,
        terminal_duplicate_cleanup_reservation_id=RESERVATION_ID,
    )
    store.record_terminal_duplicate_cleanup_action(
        terminal_duplicate_cleanup_action_id=ACTION_ID,
        terminal_duplicate_cleanup_reservation_id=RESERVATION_ID,
        outbox_id=ACTION_OUTBOX_ID,
    )
    with pytest.raises(ValueError, match="requires outcome"):
        store.complete_terminal_duplicate_cleanup_action(
            terminal_duplicate_cleanup_action_id=ACTION_ID,
            outcome="MARKER_DETACHED",
            forge_observation_id=FORGE_OBS_ID,
        )


def _create_reconcile_activity(store: RunStore, activity_id: str = ACTIVITY_ID) -> None:
    store.create_activity(
        activity_id=activity_id,
        run_id=RUN_ID,
        activity_ordinal=1,
        specification_generation=0,
        policy_hash="sha256:" + "0" * 64,
        kind="RECONCILE",
        execution_class="CONTROLLER",
        state="ACTIVE",
        created_transition_sequence=1,
        semantic_input={"publication_id": PUBLICATION_ID},
        semantic_input_digest=request_digest({"publication_id": PUBLICATION_ID}),
        idempotency_key=request_digest({"kind": "RECONCILE", "activity_id": activity_id}),
    )


def test_record_publication_ownership_conflict_creates_fact_and_boundary(store: RunStore) -> None:
    _create_reconcile_activity(store)
    fact, boundary = store.record_publication_ownership_conflict(
        reconciliation_fact_id=RECONCILIATION_FACT_ID,
        activity_id=ACTIVITY_ID,
        run_id=RUN_ID,
        created_transition_sequence=1,
        publication_id=PUBLICATION_ID,
        publication_effect_generation=1,
        ownership_evidence_digest=request_digest({"evidence": "conflict"}),
        human_boundary_id=HUMAN_BOUNDARY_ID,
        resume_state="PUBLISHING",
        minimum_request="A foreign account owns this deterministic ref/marker.",
        ownership_project_id="project-a",
        ownership_deterministic_ref=deterministic_publication_ref(RUN_ID),
        ownership_change_request_external_id="1",
        ownership_run_marker=render_run_marker(run_id=RUN_ID, publication_id=PUBLICATION_ID),
    )
    assert fact.kind == "OWNERSHIP_CONFLICT"
    assert boundary.reason == "PUBLICATION_OWNERSHIP_CONFLICT"
    assert boundary.created_from_id == RECONCILIATION_FACT_ID
    refetched = store.get_reconciliation_fact(RECONCILIATION_FACT_ID)
    assert refetched is not None
    assert refetched.fact_digest == fact.fact_digest


def test_record_reconciliation_fact_requires_ownership_evidence(store: RunStore) -> None:
    with pytest.raises(ValueError, match="ownership_evidence_digest"):
        store.record_reconciliation_fact(
            reconciliation_fact_id=RECONCILIATION_FACT_ID,
            activity_id=ACTIVITY_ID,
            run_id=RUN_ID,
            kind="OWNERSHIP_CONFLICT",
            created_transition_sequence=1,
        )


def test_record_reconciliation_fact_redundant_requires_duplicate_members(store: RunStore) -> None:
    with pytest.raises(ValueError, match="redundant duplicate member"):
        store.record_reconciliation_fact(
            reconciliation_fact_id=RECONCILIATION_FACT_ID,
            activity_id=ACTIVITY_ID,
            run_id=RUN_ID,
            kind="REDUNDANT_PUBLICATIONS_PROVEN",
            created_transition_sequence=1,
            retained_live_external_id="1",
            duplicate_members=(),
        )


def test_record_reconciliation_fact_redundant_publications_proven(store: RunStore) -> None:
    _create_reconcile_activity(store)
    fact = store.record_reconciliation_fact(
        reconciliation_fact_id=RECONCILIATION_FACT_ID,
        activity_id=ACTIVITY_ID,
        run_id=RUN_ID,
        kind="REDUNDANT_PUBLICATIONS_PROVEN",
        created_transition_sequence=1,
        publication_id=None,
        retained_live_external_id="1",
        duplicate_search_revision="search-rev-1",
        duplicate_set_digest="sha256:" + "d" * 64,
        duplicate_members=(("1", "RETAIN"), ("2", "CLOSE")),
    )
    assert fact.kind == "REDUNDANT_PUBLICATIONS_PROVEN"
    assert [(m.change_request_external_id, m.disposition) for m in fact.duplicate_members] == [
        ("1", "RETAIN"),
        ("2", "CLOSE"),
    ]


def test_record_publish_controller_operation_fact_rejects_forge_transient(store: RunStore) -> None:
    _plan_effect(store)
    with pytest.raises(ValueError, match="FORGE_TRANSIENT"):
        store.record_publish_controller_operation_fact(
            controller_operation_fact_id=CONTROLLER_FACT_ID,
            activity_id=ACTIVITY_ID,
            operation_kind="PUBLISH",
            failure_category="FORGE_TRANSIENT",
            operation_digest=request_digest({"op": "publish"}),
        )


def test_record_publish_controller_operation_fact_records_failure(store: RunStore) -> None:
    _plan_effect(store)
    fact = store.record_publish_controller_operation_fact(
        controller_operation_fact_id=CONTROLLER_FACT_ID,
        activity_id=ACTIVITY_ID,
        operation_kind="PUBLISH",
        failure_category="CREDENTIAL",
        operation_digest=request_digest({"op": "publish"}),
    )
    assert fact.outcome == "FAILED"
    assert fact.failure_category == "CREDENTIAL"


def test_decide_ref_cas_used_end_to_end_with_publish_effect(store: RunStore) -> None:
    publication, effect, _activity, _outbox = _plan_effect(store)
    decision = decide_ref_cas(
        observed_ref_commit=None,
        expected_remote_commit=effect.expected_remote_commit,
        desired_commit=effect.desired_commit["oid"],
    )
    assert decision.action == "MUTATE"
    assert decision.mutation_suboperation == "REF_CREATE"

    # A foreign SHA already on the ref must never be blindly overwritten.
    foreign_decision = decide_ref_cas(
        observed_ref_commit="foreign-sha",
        expected_remote_commit=effect.expected_remote_commit,
        desired_commit=effect.desired_commit["oid"],
    )
    assert foreign_decision.action == "FOREIGN_SHA"
    assert publication.state == "PLANNED"


def test_legacy_exclusion_requires_at_least_one_predicate(store: RunStore) -> None:
    with pytest.raises(ValueError, match="at least one of"):
        store.is_change_request_excluded_from_legacy_engine()


def test_legacy_exclusion_true_while_run_is_live(store: RunStore) -> None:
    _plan_effect(store)
    store.record_publication_effect_checkpoint(
        publication_effect_checkpoint_id=CHECKPOINT_ID_1,
        publication_id=PUBLICATION_ID,
        effect_generation=1,
        suboperation_kind="REF_READ",
        status="OBSERVED_ABSENT",
        forge_observation_id=FORGE_OBS_ID,
    )
    assert store.is_change_request_excluded_from_legacy_engine(
        deterministic_ref=deterministic_publication_ref(RUN_ID)
    )


def test_legacy_exclusion_false_for_unknown_ref(store: RunStore) -> None:
    _plan_effect(store)
    assert not store.is_change_request_excluded_from_legacy_engine(
        deterministic_ref="refs/heads/orcest/run/unrelated"
    )


def test_legacy_exclusion_aggregates_all_matching_publications(store: RunStore) -> None:
    _plan_effect(store)
    store.conn.execute(
        "UPDATE runs SET state = 'CLOSED', terminal_outcome = 'CLOSED' WHERE run_id = ?",
        (RUN_ID,),
    )
    second_run_id = "11111111-1111-4111-8111-111111111112"
    second_publication_id = "22222222-2222-4222-8222-222222222223"
    _create_run(store, run_id=second_run_id)
    store.plan_publish_effect(
        publication_id=second_publication_id,
        run_id=second_run_id,
        activity_id="33333333-3333-4333-8333-333333333334",
        activity_ordinal=1,
        specification_generation=0,
        policy_hash="sha256:" + "0" * 64,
        created_transition_sequence=1,
        candidate_id=CANDIDATE_ID,
        desired_commit=DESIRED_COMMIT,
        publication_secret_id=SECRET_ID,
        publication_secret_version=1,
        base_ref="refs/heads/main",
        base_commit=BASE_COMMIT,
        base_movement_policy="REBASE_BEFORE_PUBLICATION",
        deterministic_branch=deterministic_publication_ref(RUN_ID),
        run_marker=render_run_marker(run_id=second_run_id, publication_id=second_publication_id),
        semantic_input={"publication_id": second_publication_id},
        semantic_input_digest=request_digest({"publication_id": second_publication_id}),
        idempotency_key=request_digest(
            {"kind": "PUBLISH", "publication_id": second_publication_id}
        ),
        outbox_id="44444444-4444-4444-8444-444444444445",
    )
    assert store.is_change_request_excluded_from_legacy_engine(
        deterministic_ref=deterministic_publication_ref(RUN_ID)
    )
