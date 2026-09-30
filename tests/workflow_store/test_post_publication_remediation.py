"""Post-publication monitoring and remediation (issue #693, V1-26).

Covers what the Publication-creation leaf (#692) explicitly deferred:
SHA-fenced ``UPDATE``-mode Publication Effects (post-publication
remediation), the CAS-mismatch supersede path, the ``CLOSE_PUBLICATION``
possible-create and head-bound completion paths, ``REPAIR_RUN_MARKER``
planning/completion, the closed Controller Operation Fact failure-category
matrix for those three cleanup kinds, monitoring Schedule creation, and the
``create_activity`` Change-Request-head SHA fence.
"""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest

from orcest.workflow_contract.v1.digest import request_digest
from orcest.workflow_contract.v1.publication import (
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
from tests.workflow_store import test_publications as publication_helpers

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
# Post-activation tests must not reuse these: _activate_publication already
# consumes CHECKPOINT_ID_1/2/3 for its own COMPLETE_MARKER_SEARCH/BASE_READ_POST/
# COMPLETE checkpoints.
CHECKPOINT_ID_4 = "77777777-7777-4777-8777-777777777774"
CHECKPOINT_ID_5 = "77777777-7777-4777-8777-777777777775"
SEARCH_RESULT_ID = "88888888-8888-4888-8888-888888888888"
FORGE_OBS_ID = "99999999-9999-4999-8999-999999999999"
HEAD_OBS_ID = "99999999-9999-4999-8999-999999999998"
PROJECT_ID = "12345678-1234-4234-8234-123456789011"

DESIRED_COMMIT = {"object_format": "sha1", "oid": "a" * 40}
BASE_COMMIT = {"object_format": "sha1", "oid": "b" * 40}
REMEDIATED_COMMIT = {"object_format": "sha1", "oid": "c" * 40}


def _uid() -> str:
    return str(uuid.uuid4())


def _create_run(store: RunStore, run_id: str = RUN_ID, project_id: str = PROJECT_ID) -> None:
    store.create_run(
        run_id=run_id,
        project_id=project_id,
        work_item_key=f"work-{run_id}",
        state="APPROVED",
        reducer_version=DEFAULT_REDUCER_VERSION,
    )


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


def _plan_effect(store: RunStore, **overrides: object) -> tuple:
    params = dict(
        publication_id=PUBLICATION_ID,
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
        run_marker=render_run_marker(run_id=RUN_ID, publication_id=PUBLICATION_ID),
        semantic_input={"publication_id": PUBLICATION_ID},
        semantic_input_digest=request_digest({"publication_id": PUBLICATION_ID}),
        idempotency_key=request_digest({"kind": "PUBLISH", "publication_id": PUBLICATION_ID}),
        outbox_id=OUTBOX_ID,
    )
    params.update(overrides)
    return store.plan_publish_effect(**params)  # type: ignore[arg-type]


def _activate_publication(store: RunStore) -> None:
    """Drive Publication PLANNED -> ACTIVE via the ONE_LIVE happy path,
    linking it to change_request_external_id="1" with an observed head of
    ``DESIRED_COMMIT`` -- mirrors test_publications.py's
    test_complete_publication_effect_sets_active."""
    _plan_effect(store)
    publication_helpers._record_search(
        store,
        change_request_search_result_id=SEARCH_RESULT_ID,
        forge_observation_id=FORGE_OBS_ID,
        publication_effect_checkpoint_id=CHECKPOINT_ID_1,
        publication_id=PUBLICATION_ID,
        effect_generation=1,
        project_id=PROJECT_ID,
        run_marker=render_run_marker(run_id=RUN_ID, publication_id=PUBLICATION_ID),
        deterministic_ref=deterministic_publication_ref(RUN_ID),
        external_revision="search-rev-1",
        members=(_member(),),
        fresh_exact_object_confirmed=True,
    )
    publication_helpers._checkpoint(
        store,
        publication_effect_checkpoint_id=CHECKPOINT_ID_2,
        publication_id=PUBLICATION_ID,
        effect_generation=1,
        suboperation_kind="BASE_READ_POST",
        status="OBSERVED_SATISFIED",
        forge_observation_id=_uid(),
    )
    publication_helpers._complete_publication(
        store,
        publication_effect_checkpoint_id=CHECKPOINT_ID_3,
        publication_id=PUBLICATION_ID,
        effect_generation=1,
        forge_observation_id=_uid(),
        observed_external_revision=DESIRED_COMMIT["oid"],
    )


@pytest.fixture
def store(tmp_path: Path) -> RunStore:
    with RunStore(tmp_path, verify_local_filesystem=False) as s:
        publication_helpers._seed_project(s)
        _create_run(s)
        yield s


def _plan_update_effect(store: RunStore, **overrides: object) -> tuple:
    params = dict(
        publication_id=PUBLICATION_ID,
        run_id=RUN_ID,
        activity_id="33333333-3333-4333-8333-333333333334",
        activity_ordinal=2,
        specification_generation=0,
        policy_hash="sha256:" + "0" * 64,
        created_transition_sequence=2,
        candidate_id="55555555-5555-4555-8555-555555555556",
        desired_commit=REMEDIATED_COMMIT,
        change_request_head_observation_id=HEAD_OBS_ID,
        observed_change_request_head=DESIRED_COMMIT,
        publication_secret_id=SECRET_ID,
        publication_secret_version=1,
        base_ref="refs/heads/main",
        base_commit=BASE_COMMIT,
        base_movement_policy="REBASE_BEFORE_PUBLICATION",
        semantic_input={"publication_id": PUBLICATION_ID, "gen": 2},
        semantic_input_digest=request_digest({"publication_id": PUBLICATION_ID, "gen": 2}),
        idempotency_key=request_digest({"kind": "PR_REMEDIATE", "gen": 2}),
        outbox_id="44444444-4444-4444-8444-444444444445",
    )
    params.update(overrides)
    return store.plan_publish_update_effect(**params)  # type: ignore[arg-type]


def _record_ref_update_checkpoint(
    store: RunStore,
    *,
    effect_generation: int,
    status: str,
    checkpoint_id: str = CHECKPOINT_ID_4,
) -> str:
    request_key = request_digest(
        {"kind": "REF_UPDATE", "generation": effect_generation, "checkpoint": checkpoint_id}
    )
    publication_helpers._checkpoint(
        store,
        publication_id=PUBLICATION_ID,
        effect_generation=effect_generation,
        suboperation_kind="REF_UPDATE",
        status="REQUEST_READY",
        request_idempotency_key=request_key,
    )
    effect = store.get_publication_effect(PUBLICATION_ID, effect_generation)
    assert effect is not None
    observed_external_revision = (
        effect.desired_commit["oid"] if status == "OBSERVED_SATISFIED" else "d" * 40
    )
    observation_id = _uid()
    publication_helpers._checkpoint(
        store,
        publication_effect_checkpoint_id=checkpoint_id,
        publication_id=PUBLICATION_ID,
        effect_generation=effect_generation,
        suboperation_kind="REF_UPDATE",
        status=status,
        request_idempotency_key=request_key,
        forge_observation_id=observation_id,
        observed_external_revision=observed_external_revision,
    )
    return observation_id


# -- plan_publish_update_effect ---------------------------------------------


def test_plan_publish_update_effect_requires_active_publication(store: RunStore) -> None:
    _plan_effect(store)  # publication stays PLANNED
    with pytest.raises(RunStoreError, match="not ACTIVE"):
        _plan_update_effect(store)


def test_plan_publish_update_effect_creates_second_generation(store: RunStore) -> None:
    _activate_publication(store)
    publication, effect, activity, outbox = _plan_update_effect(store)

    assert publication.effect_generation == 2
    assert publication.candidate_id == "55555555-5555-4555-8555-555555555556"
    assert effect.mode == "UPDATE"
    assert effect.effect_generation == 2
    assert effect.expected_remote_commit == DESIRED_COMMIT["oid"]
    assert effect.desired_commit == REMEDIATED_COMMIT
    assert activity.kind == "PUBLISH"
    assert activity.execution_class == "CONTROLLER"
    assert activity.change_request_head_observation_id == HEAD_OBS_ID
    assert activity.observed_change_request_head_json is not None
    assert outbox.publication_id == PUBLICATION_ID
    assert outbox.effect_generation == 2


def test_plan_publish_update_effect_supersedes_prior_effect(store: RunStore) -> None:
    _activate_publication(store)
    _plan_update_effect(store)
    prior = store.get_publication_effect(PUBLICATION_ID, 1)
    assert prior is not None
    assert prior.superseded is True


def test_plan_publish_update_effect_replay_is_idempotent(store: RunStore) -> None:
    _activate_publication(store)
    first = _plan_update_effect(store)
    second = _plan_update_effect(store)
    assert first == second


def test_plan_publish_update_effect_replay_conflict_raises(store: RunStore) -> None:
    _activate_publication(store)
    _plan_update_effect(store)
    with pytest.raises(IdempotencyConflictError):
        _plan_update_effect(store, candidate_id="55555555-5555-4555-8555-555555555559")


def test_plan_publish_update_effect_replay_conflict_on_desired_commit_mismatch(
    store: RunStore,
) -> None:
    """A replay with a different Effect-only field (not mirrored onto the
    Activity row) must still be rejected, not silently return the stale
    Effect/Outbox."""
    _activate_publication(store)
    _plan_update_effect(store)
    with pytest.raises(IdempotencyConflictError):
        _plan_update_effect(store, desired_commit={"object_format": "sha1", "oid": "d" * 40})


def test_plan_publish_update_effect_replay_conflict_on_base_ref_mismatch(store: RunStore) -> None:
    _activate_publication(store)
    _plan_update_effect(store)
    with pytest.raises(IdempotencyConflictError):
        _plan_update_effect(store, base_ref="refs/heads/other")


def test_plan_publish_update_effect_replay_conflict_on_outbox_id_mismatch(
    store: RunStore,
) -> None:
    """A replay with a different ``outbox_id`` (not part of the lookup key,
    which matches on source_id + destination) must still be rejected."""
    _activate_publication(store)
    _plan_update_effect(store)
    with pytest.raises(IdempotencyConflictError):
        _plan_update_effect(store, outbox_id="44444444-4444-4444-8444-444444444446")


def test_plan_publish_update_effect_requires_linked_publication(store: RunStore) -> None:
    _plan_effect(store)
    # Force ACTIVE without ever linking a Change Request, to exercise the
    # defensive guard directly (unreachable through the ordinary ONE_LIVE
    # path, which always links before activating).
    store.conn.execute(
        "UPDATE publications SET state = 'ACTIVE' WHERE publication_id = ?", (PUBLICATION_ID,)
    )
    store.conn.commit()
    with pytest.raises(RunStoreError, match="no linked Change Request"):
        _plan_update_effect(store)


# -- CAS-mismatch supersede (item 3: "observed-head and base CAS") ----------


def test_ref_update_cas_mismatch_supersedes_update_effect(store: RunStore) -> None:
    _activate_publication(store)
    _plan_update_effect(store)
    _record_ref_update_checkpoint(store, effect_generation=2, status="CAS_MISMATCH")
    effect = store.get_publication_effect(PUBLICATION_ID, 2)
    assert effect is not None
    assert effect.superseded is True
    outbox = store.conn.execute(
        "SELECT state FROM outbox WHERE publication_id = ? AND effect_generation = ?",
        (PUBLICATION_ID, 2),
    ).fetchone()
    assert outbox["state"] == "SUPERSEDED"


def test_ref_read_cas_mismatch_does_not_affect_stale_generation(store: RunStore) -> None:
    _activate_publication(store)
    _plan_update_effect(store)
    _plan_update_effect(
        store,
        activity_id="33333333-3333-4333-8333-333333333335",
        activity_ordinal=3,
        candidate_id="55555555-5555-4555-8555-555555555557",
        idempotency_key=request_digest({"kind": "PR_REMEDIATE", "gen": 3}),
        outbox_id="44444444-4444-4444-8444-444444444446",
        created_transition_sequence=3,
        semantic_input={"publication_id": PUBLICATION_ID, "gen": 3},
        semantic_input_digest=request_digest({"publication_id": PUBLICATION_ID, "gen": 3}),
    )
    # A CAS_MISMATCH recorded against a superseded (non-current) generation
    # must not resurrect it as superseded-again (already is) nor touch the
    # current generation's outbox.
    _record_ref_update_checkpoint(store, effect_generation=2, status="CAS_MISMATCH")
    current_outbox = store.conn.execute(
        "SELECT state FROM outbox WHERE publication_id = ? AND effect_generation = ?",
        (PUBLICATION_ID, 3),
    ).fetchone()
    assert current_outbox["state"] == "PENDING"


# -- complete_publication_update_effect --------------------------------------


def test_complete_publication_update_effect_requires_ref_update_satisfied(store: RunStore) -> None:
    _activate_publication(store)
    _plan_update_effect(store)
    with pytest.raises(RunStoreError, match="REF_READ/REF_UPDATE"):
        store.complete_publication_update_effect(
            publication_effect_checkpoint_id=CHECKPOINT_ID_4,
            publication_id=PUBLICATION_ID,
            effect_generation=2,
            forge_observation_id=FORGE_OBS_ID,
            observed_external_revision=REMEDIATED_COMMIT["oid"],
        )


def test_complete_publication_update_effect_updates_observed_commit(store: RunStore) -> None:
    _activate_publication(store)
    _plan_update_effect(store)
    observation_id = _record_ref_update_checkpoint(
        store, effect_generation=2, status="OBSERVED_SATISFIED"
    )
    checkpoint = store.complete_publication_update_effect(
        publication_effect_checkpoint_id=CHECKPOINT_ID_5,
        publication_id=PUBLICATION_ID,
        effect_generation=2,
        forge_observation_id=observation_id,
        observed_external_revision=REMEDIATED_COMMIT["oid"],
    )
    assert checkpoint.status == "COMPLETED"
    publication = store.get_publication(PUBLICATION_ID)
    assert publication is not None
    assert publication.state == "ACTIVE"
    assert publication.observed_remote_commit == REMEDIATED_COMMIT["oid"]
    assert publication.effect_generation == 2


def test_complete_publication_update_effect_stale_generation_is_audit_only(store: RunStore) -> None:
    _activate_publication(store)
    _plan_update_effect(store)
    # A third generation supersedes the second before it ever completes.
    _plan_update_effect(
        store,
        activity_id="33333333-3333-4333-8333-333333333335",
        activity_ordinal=3,
        candidate_id="55555555-5555-4555-8555-555555555557",
        idempotency_key=request_digest({"kind": "PR_REMEDIATE", "gen": 3}),
        outbox_id="44444444-4444-4444-8444-444444444446",
        created_transition_sequence=3,
        semantic_input={"publication_id": PUBLICATION_ID, "gen": 3},
        semantic_input_digest=request_digest({"publication_id": PUBLICATION_ID, "gen": 3}),
    )
    observation_id = _record_ref_update_checkpoint(
        store, effect_generation=2, status="OBSERVED_SATISFIED"
    )
    store.complete_publication_update_effect(
        publication_effect_checkpoint_id=CHECKPOINT_ID_5,
        publication_id=PUBLICATION_ID,
        effect_generation=2,
        forge_observation_id=observation_id,
        observed_external_revision=REMEDIATED_COMMIT["oid"],
    )
    publication = store.get_publication(PUBLICATION_ID)
    assert publication is not None
    assert publication.effect_generation == 3
    assert publication.observed_remote_commit != REMEDIATED_COMMIT["oid"]


def test_complete_publication_update_effect_closed_publication_is_audit_only(
    store: RunStore,
) -> None:
    _activate_publication(store)
    _plan_update_effect(store)
    observation_id = _record_ref_update_checkpoint(
        store, effect_generation=2, status="OBSERVED_SATISFIED"
    )
    close_activity, _outbox = store.plan_close_publication_activity(
        activity_id="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        run_id=RUN_ID,
        activity_ordinal=3,
        specification_generation=0,
        policy_hash="sha256:" + "0" * 64,
        created_transition_sequence=3,
        semantic_input={"cancel": 1},
        semantic_input_digest=request_digest({"cancel": 1}),
        idempotency_key=request_digest({"kind": "CLOSE_PUBLICATION"}),
        outbox_id="eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
        change_request_head_observation_id=HEAD_OBS_ID,
        observed_change_request_head=DESIRED_COMMIT,
    )
    store.complete_close_publication_head_bound(
        activity_id=close_activity.activity_id,
        forge_observation_id=HEAD_OBS_ID,
    )

    checkpoint = store.complete_publication_update_effect(
        publication_effect_checkpoint_id=CHECKPOINT_ID_5,
        publication_id=PUBLICATION_ID,
        effect_generation=2,
        forge_observation_id=observation_id,
        observed_external_revision=REMEDIATED_COMMIT["oid"],
    )

    assert checkpoint.status == "COMPLETED"
    publication = store.get_publication(PUBLICATION_ID)
    assert publication is not None
    assert publication.state == "CLOSED"
    assert publication.effect_generation == 2
    assert publication.observed_remote_commit == DESIRED_COMMIT["oid"]
    assert publication.last_observation_id == HEAD_OBS_ID


# -- plan_close_publication_activity / completion ----------------------------


def test_plan_close_publication_activity_possible_create(store: RunStore) -> None:
    _create_run(store, run_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
    _plan_effect(
        store,
        run_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        activity_id="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        outbox_id="cccccccc-cccc-4ccc-8ccc-cccccccccccc",
        semantic_input={"publication_id": PUBLICATION_ID, "run": "b"},
        semantic_input_digest=request_digest({"publication_id": PUBLICATION_ID, "run": "b"}),
        idempotency_key=request_digest({"kind": "PUBLISH", "run": "b"}),
    )
    activity, outbox = store.plan_close_publication_activity(
        activity_id="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        run_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        activity_ordinal=2,
        specification_generation=0,
        policy_hash="sha256:" + "0" * 64,
        created_transition_sequence=2,
        semantic_input={"cancel": True},
        semantic_input_digest=request_digest({"cancel": True}),
        idempotency_key=request_digest({"kind": "CLOSE_PUBLICATION"}),
        outbox_id="eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
    )
    assert activity.kind == "CLOSE_PUBLICATION"
    assert activity.execution_class == "CONTROLLER"
    assert activity.state == "ACTIVE"
    assert activity.change_request_head_observation_id is None
    assert outbox.source_id == activity.activity_id


def test_plan_close_publication_activity_supersedes_prior_active(store: RunStore) -> None:
    first, first_outbox = store.plan_close_publication_activity(
        activity_id="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        run_id=RUN_ID,
        activity_ordinal=2,
        specification_generation=0,
        policy_hash="sha256:" + "0" * 64,
        created_transition_sequence=2,
        semantic_input={"cancel": 1},
        semantic_input_digest=request_digest({"cancel": 1}),
        idempotency_key=request_digest({"kind": "CLOSE_PUBLICATION", "n": 1}),
        outbox_id="eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
    )
    second, _second_outbox = store.plan_close_publication_activity(
        activity_id="dddddddd-dddd-4ddd-8ddd-ddddddddddde",
        run_id=RUN_ID,
        activity_ordinal=3,
        specification_generation=0,
        policy_hash="sha256:" + "0" * 64,
        created_transition_sequence=3,
        semantic_input={"cancel": 2},
        semantic_input_digest=request_digest({"cancel": 2}),
        idempotency_key=request_digest({"kind": "CLOSE_PUBLICATION", "n": 2}),
        outbox_id="eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeef",
    )
    assert second.state == "ACTIVE"
    superseded = store.get_activity(first.activity_id)
    assert superseded is not None
    assert superseded.state == "SUPERSEDED"
    superseded_outbox = store.get_outbox(first_outbox.outbox_id)
    assert superseded_outbox is not None
    assert superseded_outbox.state == "SUPERSEDED"


def test_plan_close_publication_activity_replay_is_idempotent(store: RunStore) -> None:
    kwargs = dict(
        activity_id="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        run_id=RUN_ID,
        activity_ordinal=2,
        specification_generation=0,
        policy_hash="sha256:" + "0" * 64,
        created_transition_sequence=2,
        semantic_input={"cancel": 1},
        semantic_input_digest=request_digest({"cancel": 1}),
        idempotency_key=request_digest({"kind": "CLOSE_PUBLICATION"}),
        outbox_id="eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
    )
    first = store.plan_close_publication_activity(**kwargs)  # type: ignore[arg-type]
    second = store.plan_close_publication_activity(**kwargs)  # type: ignore[arg-type]
    assert first == second


def test_plan_close_publication_activity_replay_conflict_on_outbox_destination_mismatch(
    store: RunStore,
) -> None:
    """A replay with a different outbox-only field (not mirrored onto the
    Activity row) must still be rejected, not silently return the stale
    Outbox."""
    kwargs = dict(
        activity_id="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        run_id=RUN_ID,
        activity_ordinal=2,
        specification_generation=0,
        policy_hash="sha256:" + "0" * 64,
        created_transition_sequence=2,
        semantic_input={"cancel": 1},
        semantic_input_digest=request_digest({"cancel": 1}),
        idempotency_key=request_digest({"kind": "CLOSE_PUBLICATION"}),
        outbox_id="eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
    )
    store.plan_close_publication_activity(**kwargs)  # type: ignore[arg-type]
    with pytest.raises(IdempotencyConflictError):
        store.plan_close_publication_activity(  # type: ignore[arg-type]
            **{**kwargs, "outbox_destination": "controller-close-publication/2"}
        )


def test_complete_close_publication_possible_create_closes_publication(store: RunStore) -> None:
    _plan_effect(store)  # publication stays PLANNED: no Change Request ever created
    activity, _outbox = store.plan_close_publication_activity(
        activity_id="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        run_id=RUN_ID,
        activity_ordinal=2,
        specification_generation=0,
        policy_hash="sha256:" + "0" * 64,
        created_transition_sequence=2,
        semantic_input={"cancel": 1},
        semantic_input_digest=request_digest({"cancel": 1}),
        idempotency_key=request_digest({"kind": "CLOSE_PUBLICATION"}),
        outbox_id="eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
    )
    fact, publication = store.complete_close_publication_possible_create(
        controller_operation_fact_id="ffffffff-ffff-4fff-8fff-ffffffffffff",
        activity_id=activity.activity_id,
        forge_observation_id=FORGE_OBS_ID,
        operation_digest=request_digest({"op": "close"}),
    )
    assert fact.outcome == "SUCCEEDED"
    assert fact.operation_kind == "CLOSE_PUBLICATION"
    assert fact.candidate_id is None
    assert publication.state == "CLOSED"
    activity_after = store.get_activity(activity.activity_id)
    assert activity_after is not None
    assert activity_after.state == "SUCCEEDED"


def test_complete_close_publication_possible_create_replay_is_idempotent(store: RunStore) -> None:
    _plan_effect(store)
    activity, _outbox = store.plan_close_publication_activity(
        activity_id="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        run_id=RUN_ID,
        activity_ordinal=2,
        specification_generation=0,
        policy_hash="sha256:" + "0" * 64,
        created_transition_sequence=2,
        semantic_input={"cancel": 1},
        semantic_input_digest=request_digest({"cancel": 1}),
        idempotency_key=request_digest({"kind": "CLOSE_PUBLICATION"}),
        outbox_id="eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
    )
    kwargs = dict(
        controller_operation_fact_id="ffffffff-ffff-4fff-8fff-ffffffffffff",
        activity_id=activity.activity_id,
        forge_observation_id=FORGE_OBS_ID,
        operation_digest=request_digest({"op": "close"}),
    )
    first = store.complete_close_publication_possible_create(**kwargs)  # type: ignore[arg-type]
    second = store.complete_close_publication_possible_create(**kwargs)  # type: ignore[arg-type]
    assert first == second


def test_complete_close_publication_possible_create_superseded_is_noop(
    store: RunStore,
) -> None:
    _plan_effect(store)
    first, _first_outbox = store.plan_close_publication_activity(
        activity_id="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        run_id=RUN_ID,
        activity_ordinal=2,
        specification_generation=0,
        policy_hash="sha256:" + "0" * 64,
        created_transition_sequence=2,
        semantic_input={"cancel": 1},
        semantic_input_digest=request_digest({"cancel": 1}),
        idempotency_key=request_digest({"kind": "CLOSE_PUBLICATION", "n": 1}),
        outbox_id="eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
    )
    store.plan_close_publication_activity(
        activity_id="dddddddd-dddd-4ddd-8ddd-ddddddddddde",
        run_id=RUN_ID,
        activity_ordinal=3,
        specification_generation=0,
        policy_hash="sha256:" + "0" * 64,
        created_transition_sequence=3,
        semantic_input={"cancel": 2},
        semantic_input_digest=request_digest({"cancel": 2}),
        idempotency_key=request_digest({"kind": "CLOSE_PUBLICATION", "n": 2}),
        outbox_id="eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeef",
    )

    fact, publication = store.complete_close_publication_possible_create(
        controller_operation_fact_id="ffffffff-ffff-4fff-8fff-ffffffffffff",
        activity_id=first.activity_id,
        forge_observation_id=FORGE_OBS_ID,
        operation_digest=request_digest({"op": "close"}),
    )

    assert fact is None
    assert publication.state == "PLANNED"
    first_after = store.get_activity(first.activity_id)
    assert first_after is not None
    assert first_after.state == "SUPERSEDED"
    facts = store.conn.execute(
        "SELECT COUNT(*) FROM controller_operation_facts WHERE activity_id = ?",
        (first.activity_id,),
    ).fetchone()[0]
    assert facts == 0


def test_complete_close_publication_possible_create_rejects_head_bound_activity(
    store: RunStore,
) -> None:
    _activate_publication(store)
    activity, _outbox = store.plan_close_publication_activity(
        activity_id="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        run_id=RUN_ID,
        activity_ordinal=2,
        specification_generation=0,
        policy_hash="sha256:" + "0" * 64,
        created_transition_sequence=2,
        semantic_input={"cancel": 1},
        semantic_input_digest=request_digest({"cancel": 1}),
        idempotency_key=request_digest({"kind": "CLOSE_PUBLICATION"}),
        outbox_id="eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
        change_request_head_observation_id=HEAD_OBS_ID,
        observed_change_request_head=DESIRED_COMMIT,
    )
    with pytest.raises(RunStoreError, match="pre-link"):
        store.complete_close_publication_possible_create(
            controller_operation_fact_id="ffffffff-ffff-4fff-8fff-ffffffffffff",
            activity_id=activity.activity_id,
            forge_observation_id=FORGE_OBS_ID,
            operation_digest=request_digest({"op": "close"}),
        )


def test_complete_close_publication_head_bound_closes_publication(store: RunStore) -> None:
    _activate_publication(store)
    activity, _outbox = store.plan_close_publication_activity(
        activity_id="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        run_id=RUN_ID,
        activity_ordinal=2,
        specification_generation=0,
        policy_hash="sha256:" + "0" * 64,
        created_transition_sequence=2,
        semantic_input={"cancel": 1},
        semantic_input_digest=request_digest({"cancel": 1}),
        idempotency_key=request_digest({"kind": "CLOSE_PUBLICATION"}),
        outbox_id="eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
        change_request_head_observation_id=HEAD_OBS_ID,
        observed_change_request_head=DESIRED_COMMIT,
    )
    activity_after, publication = store.complete_close_publication_head_bound(
        activity_id=activity.activity_id,
        forge_observation_id=FORGE_OBS_ID,
    )
    assert activity_after.state == "SUCCEEDED"
    assert publication.state == "CLOSED"
    facts = store.conn.execute(
        "SELECT COUNT(*) FROM controller_operation_facts WHERE activity_id = ?",
        (activity.activity_id,),
    ).fetchone()[0]
    assert facts == 0


def test_complete_close_publication_head_bound_superseded_is_noop(store: RunStore) -> None:
    _activate_publication(store)
    first, _outbox = store.plan_close_publication_activity(
        activity_id="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        run_id=RUN_ID,
        activity_ordinal=2,
        specification_generation=0,
        policy_hash="sha256:" + "0" * 64,
        created_transition_sequence=2,
        semantic_input={"cancel": 1},
        semantic_input_digest=request_digest({"cancel": 1}),
        idempotency_key=request_digest({"kind": "CLOSE_PUBLICATION", "n": 1}),
        outbox_id="eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
        change_request_head_observation_id=HEAD_OBS_ID,
        observed_change_request_head=DESIRED_COMMIT,
    )
    store.plan_close_publication_activity(
        activity_id="dddddddd-dddd-4ddd-8ddd-ddddddddddde",
        run_id=RUN_ID,
        activity_ordinal=3,
        specification_generation=0,
        policy_hash="sha256:" + "0" * 64,
        created_transition_sequence=3,
        semantic_input={"cancel": 2},
        semantic_input_digest=request_digest({"cancel": 2}),
        idempotency_key=request_digest({"kind": "CLOSE_PUBLICATION", "n": 2}),
        outbox_id="eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeef",
        change_request_head_observation_id=HEAD_OBS_ID,
        observed_change_request_head=REMEDIATED_COMMIT,
    )
    # Stale evidence bound to the now-superseded first Activity must not
    # terminalize or mutate the Run/Publication.
    activity_after, publication = store.complete_close_publication_head_bound(
        activity_id=first.activity_id,
        forge_observation_id=FORGE_OBS_ID,
    )
    assert activity_after.state == "SUPERSEDED"
    assert publication.state == "ACTIVE"


# -- REPAIR_RUN_MARKER --------------------------------------------------------


def test_plan_and_complete_repair_run_marker_activity(store: RunStore) -> None:
    _activate_publication(store)
    activity, outbox = store.plan_repair_run_marker_activity(
        activity_id="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        run_id=RUN_ID,
        activity_ordinal=2,
        specification_generation=0,
        policy_hash="sha256:" + "0" * 64,
        created_transition_sequence=2,
        change_request_head_observation_id=HEAD_OBS_ID,
        observed_change_request_head=DESIRED_COMMIT,
        semantic_input={"marker": "MISSING"},
        semantic_input_digest=request_digest({"marker": "MISSING"}),
        idempotency_key=request_digest({"kind": "REPAIR_RUN_MARKER"}),
        outbox_id="eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
    )
    assert activity.kind == "REPAIR_RUN_MARKER"
    assert activity.execution_class == "CONTROLLER"
    assert activity.change_request_head_observation_id == HEAD_OBS_ID
    assert outbox.source_id == activity.activity_id

    completed = store.complete_repair_run_marker(
        activity_id=activity.activity_id,
        forge_observation_id=FORGE_OBS_ID,
    )
    assert completed.state == "SUCCEEDED"
    facts = store.conn.execute(
        "SELECT COUNT(*) FROM controller_operation_facts WHERE activity_id = ?",
        (activity.activity_id,),
    ).fetchone()[0]
    assert facts == 0
    publication = store.get_publication(PUBLICATION_ID)
    assert publication is not None
    assert publication.state == "ACTIVE"


def test_plan_repair_run_marker_activity_supersedes_prior_active(store: RunStore) -> None:
    _activate_publication(store)
    first, first_outbox = store.plan_repair_run_marker_activity(
        activity_id="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        run_id=RUN_ID,
        activity_ordinal=2,
        specification_generation=0,
        policy_hash="sha256:" + "0" * 64,
        created_transition_sequence=2,
        change_request_head_observation_id=HEAD_OBS_ID,
        observed_change_request_head=DESIRED_COMMIT,
        semantic_input={"marker": "MISSING"},
        semantic_input_digest=request_digest({"marker": "MISSING"}),
        idempotency_key=request_digest({"kind": "REPAIR_RUN_MARKER", "n": 1}),
        outbox_id="eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
    )
    store.plan_repair_run_marker_activity(
        activity_id="dddddddd-dddd-4ddd-8ddd-ddddddddddde",
        run_id=RUN_ID,
        activity_ordinal=3,
        specification_generation=0,
        policy_hash="sha256:" + "0" * 64,
        created_transition_sequence=3,
        change_request_head_observation_id=HEAD_OBS_ID,
        observed_change_request_head=REMEDIATED_COMMIT,
        semantic_input={"marker": "MISSING", "n": 2},
        semantic_input_digest=request_digest({"marker": "MISSING", "n": 2}),
        idempotency_key=request_digest({"kind": "REPAIR_RUN_MARKER", "n": 2}),
        outbox_id="eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeef",
    )
    superseded = store.get_activity(first.activity_id)
    assert superseded is not None
    assert superseded.state == "SUPERSEDED"
    superseded_outbox = store.get_outbox(first_outbox.outbox_id)
    assert superseded_outbox is not None
    assert superseded_outbox.state == "SUPERSEDED"


def test_plan_repair_run_marker_activity_replay_is_idempotent(store: RunStore) -> None:
    _activate_publication(store)
    kwargs = dict(
        activity_id="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        run_id=RUN_ID,
        activity_ordinal=2,
        specification_generation=0,
        policy_hash="sha256:" + "0" * 64,
        created_transition_sequence=2,
        change_request_head_observation_id=HEAD_OBS_ID,
        observed_change_request_head=DESIRED_COMMIT,
        semantic_input={"marker": "MISSING"},
        semantic_input_digest=request_digest({"marker": "MISSING"}),
        idempotency_key=request_digest({"kind": "REPAIR_RUN_MARKER"}),
        outbox_id="eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
    )
    first = store.plan_repair_run_marker_activity(**kwargs)  # type: ignore[arg-type]
    second = store.plan_repair_run_marker_activity(**kwargs)  # type: ignore[arg-type]
    assert first == second


def test_plan_repair_run_marker_activity_replay_conflict_on_outbox_destination_mismatch(
    store: RunStore,
) -> None:
    """A replay with a different outbox-only field (not mirrored onto the
    Activity row) must still be rejected, not silently return the stale
    Outbox."""
    _activate_publication(store)
    kwargs = dict(
        activity_id="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        run_id=RUN_ID,
        activity_ordinal=2,
        specification_generation=0,
        policy_hash="sha256:" + "0" * 64,
        created_transition_sequence=2,
        change_request_head_observation_id=HEAD_OBS_ID,
        observed_change_request_head=DESIRED_COMMIT,
        semantic_input={"marker": "MISSING"},
        semantic_input_digest=request_digest({"marker": "MISSING"}),
        idempotency_key=request_digest({"kind": "REPAIR_RUN_MARKER"}),
        outbox_id="eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
    )
    store.plan_repair_run_marker_activity(**kwargs)  # type: ignore[arg-type]
    with pytest.raises(IdempotencyConflictError):
        store.plan_repair_run_marker_activity(  # type: ignore[arg-type]
            **{**kwargs, "outbox_destination": "controller-repair-run-marker/2"}
        )


def test_complete_repair_run_marker_replay_after_terminal_is_noop(store: RunStore) -> None:
    _activate_publication(store)
    activity, _outbox = store.plan_repair_run_marker_activity(
        activity_id="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        run_id=RUN_ID,
        activity_ordinal=2,
        specification_generation=0,
        policy_hash="sha256:" + "0" * 64,
        created_transition_sequence=2,
        change_request_head_observation_id=HEAD_OBS_ID,
        observed_change_request_head=DESIRED_COMMIT,
        semantic_input={"marker": "MISSING"},
        semantic_input_digest=request_digest({"marker": "MISSING"}),
        idempotency_key=request_digest({"kind": "REPAIR_RUN_MARKER"}),
        outbox_id="eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
    )
    store.complete_repair_run_marker(
        activity_id=activity.activity_id, forge_observation_id=FORGE_OBS_ID
    )
    replay = store.complete_repair_run_marker(
        activity_id=activity.activity_id,
        forge_observation_id="a1111111-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
    )
    assert replay.state == "SUCCEEDED"


# -- record_publish_controller_operation_fact: closed failure matrix --------


def test_record_publish_controller_operation_fact_accepts_repair_run_marker_failure(
    store: RunStore,
) -> None:
    _activate_publication(store)
    activity, _outbox = store.plan_repair_run_marker_activity(
        activity_id="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        run_id=RUN_ID,
        activity_ordinal=2,
        specification_generation=0,
        policy_hash="sha256:" + "0" * 64,
        created_transition_sequence=2,
        change_request_head_observation_id=HEAD_OBS_ID,
        observed_change_request_head=DESIRED_COMMIT,
        semantic_input={"marker": "MISSING"},
        semantic_input_digest=request_digest({"marker": "MISSING"}),
        idempotency_key=request_digest({"kind": "REPAIR_RUN_MARKER"}),
        outbox_id="eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
    )
    fact = store.record_publish_controller_operation_fact(
        controller_operation_fact_id="ffffffff-ffff-4fff-8fff-ffffffffffff",
        activity_id=activity.activity_id,
        operation_kind="REPAIR_RUN_MARKER",
        failure_category="CREDENTIAL",
        operation_digest=request_digest({"op": "repair"}),
    )
    assert fact.outcome == "FAILED"
    assert fact.operation_kind == "REPAIR_RUN_MARKER"
    activity_after = store.get_activity(activity.activity_id)
    assert activity_after is not None
    assert activity_after.state == "FAILED"


@pytest.mark.parametrize(
    "operation_kind", ["CLOSE_PUBLICATION", "CLOSE_REDUNDANT_PUBLICATION", "REPAIR_RUN_MARKER"]
)
def test_record_publish_controller_operation_fact_rejects_wide_categories(
    store: RunStore, operation_kind: str
) -> None:
    _plan_effect(store)
    for category in ("STORAGE", "INTEGRITY_SUSPECTED"):
        with pytest.raises(ValueError, match="permits only"):
            store.record_publish_controller_operation_fact(
                controller_operation_fact_id=_uid(),
                activity_id=ACTIVITY_ID,
                operation_kind=operation_kind,
                failure_category=category,
                operation_digest=request_digest({"op": "x", "c": category, "k": operation_kind}),
            )


# -- ensure_publication_monitoring_schedules ---------------------------------


class _Project:
    def __init__(self, project_id: str, forge_instance_id: str) -> None:
        self.project_id = project_id
        self.forge_instance_id = forge_instance_id


def _seed_project(store: RunStore) -> _Project:
    publication_helpers._seed_project(store)
    return _Project(PROJECT_ID, publication_helpers.FORGE_INSTANCE_ID)


def test_ensure_publication_monitoring_schedules_requires_active(tmp_path: Path) -> None:
    with RunStore(tmp_path, verify_local_filesystem=False) as s:
        project = _seed_project(s)
        _create_run(s, project_id=project.project_id)
        _plan_effect(s)  # PLANNED, not ACTIVE
        with pytest.raises(RunStoreError, match="not ACTIVE"):
            s.ensure_publication_monitoring_schedules(
                publication_id=PUBLICATION_ID,
                change_request_poll_schedule_id=_uid(),
                ci_poll_schedule_id=_uid(),
                forge_instance_id=project.forge_instance_id,
                minimum_interval_ms=60_000,
                next_due_at_ms=0,
            )


def test_ensure_publication_monitoring_schedules_creates_both_schedules(tmp_path: Path) -> None:
    with RunStore(tmp_path, verify_local_filesystem=False) as s:
        project = _seed_project(s)
        _create_run(s, project_id=project.project_id)
        _activate_publication(s)
        cr_poll, ci_poll = s.ensure_publication_monitoring_schedules(
            publication_id=PUBLICATION_ID,
            change_request_poll_schedule_id=_uid(),
            ci_poll_schedule_id=_uid(),
            forge_instance_id=project.forge_instance_id,
            minimum_interval_ms=60_000,
            next_due_at_ms=0,
        )
        assert cr_poll.schedule_kind == "CHANGE_REQUEST_POLL"
        assert cr_poll.target_kind == "PUBLICATION"
        assert cr_poll.target_id == PUBLICATION_ID
        assert cr_poll.publication_id == PUBLICATION_ID
        assert ci_poll.schedule_kind == "CI_POLL"
        assert ci_poll.publication_id == PUBLICATION_ID


def test_ensure_publication_monitoring_schedules_is_idempotent(tmp_path: Path) -> None:
    with RunStore(tmp_path, verify_local_filesystem=False) as s:
        project = _seed_project(s)
        _create_run(s, project_id=project.project_id)
        _activate_publication(s)
        first = s.ensure_publication_monitoring_schedules(
            publication_id=PUBLICATION_ID,
            change_request_poll_schedule_id=_uid(),
            ci_poll_schedule_id=_uid(),
            forge_instance_id=project.forge_instance_id,
            minimum_interval_ms=60_000,
            next_due_at_ms=0,
        )
        second = s.ensure_publication_monitoring_schedules(
            publication_id=PUBLICATION_ID,
            change_request_poll_schedule_id=_uid(),
            ci_poll_schedule_id=_uid(),
            forge_instance_id=project.forge_instance_id,
            minimum_interval_ms=60_000,
            next_due_at_ms=0,
        )
        assert first[0].forge_observation_schedule_id == second[0].forge_observation_schedule_id
        assert first[1].forge_observation_schedule_id == second[1].forge_observation_schedule_id


# -- create_activity: Change-Request-head SHA fence --------------------------


def test_create_activity_accepts_change_request_head_fence(store: RunStore) -> None:
    activity, _attempt, _outbox = store.create_activity(
        activity_id="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        run_id=RUN_ID,
        activity_ordinal=2,
        specification_generation=0,
        policy_hash="sha256:" + "0" * 64,
        kind="PR_REMEDIATE",
        execution_class="WORKER",
        state="PLANNED",
        created_transition_sequence=2,
        semantic_input={"x": 1},
        semantic_input_digest=request_digest({"x": 1}),
        idempotency_key=request_digest({"kind": "PR_REMEDIATE"}),
        change_request_head_observation_id=HEAD_OBS_ID,
        observed_change_request_head=DESIRED_COMMIT,
    )
    assert activity.change_request_head_observation_id == HEAD_OBS_ID
    assert activity.observed_change_request_head_json is not None


def test_create_activity_rejects_head_fence_for_disallowed_kind(store: RunStore) -> None:
    with pytest.raises(ValueError, match="may not carry"):
        store.create_activity(
            activity_id="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
            run_id=RUN_ID,
            activity_ordinal=2,
            specification_generation=0,
            policy_hash="sha256:" + "0" * 64,
            kind="BUILD",
            execution_class="WORKER",
            state="PLANNED",
            created_transition_sequence=2,
            semantic_input={"x": 1},
            semantic_input_digest=request_digest({"x": 1}),
            idempotency_key=request_digest({"kind": "BUILD"}),
            change_request_head_observation_id=HEAD_OBS_ID,
            observed_change_request_head=DESIRED_COMMIT,
        )


def test_create_activity_requires_head_fence_fields_together(store: RunStore) -> None:
    with pytest.raises(ValueError, match="must be given together"):
        store.create_activity(
            activity_id="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
            run_id=RUN_ID,
            activity_ordinal=2,
            specification_generation=0,
            policy_hash="sha256:" + "0" * 64,
            kind="REBASE",
            execution_class="WORKER",
            state="PLANNED",
            created_transition_sequence=2,
            semantic_input={"x": 1},
            semantic_input_digest=request_digest({"x": 1}),
            idempotency_key=request_digest({"kind": "REBASE"}),
            change_request_head_observation_id=HEAD_OBS_ID,
        )


def test_create_activity_replay_conflict_checks_head_fence(store: RunStore) -> None:
    store.create_activity(
        activity_id="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        run_id=RUN_ID,
        activity_ordinal=2,
        specification_generation=0,
        policy_hash="sha256:" + "0" * 64,
        kind="REBASE",
        execution_class="WORKER",
        state="PLANNED",
        created_transition_sequence=2,
        semantic_input={"x": 1},
        semantic_input_digest=request_digest({"x": 1}),
        idempotency_key=request_digest({"kind": "REBASE"}),
        change_request_head_observation_id=HEAD_OBS_ID,
        observed_change_request_head=DESIRED_COMMIT,
    )
    with pytest.raises(IdempotencyConflictError):
        store.create_activity(
            activity_id="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
            run_id=RUN_ID,
            activity_ordinal=2,
            specification_generation=0,
            policy_hash="sha256:" + "0" * 64,
            kind="REBASE",
            execution_class="WORKER",
            state="PLANNED",
            created_transition_sequence=2,
            semantic_input={"x": 1},
            semantic_input_digest=request_digest({"x": 1}),
            idempotency_key=request_digest({"kind": "REBASE"}),
            change_request_head_observation_id=HEAD_OBS_ID,
            observed_change_request_head=REMEDIATED_COMMIT,
        )
