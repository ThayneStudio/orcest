"""Reconciled single-Publication creation (issue #692).

Covers: Publication/Effect/Checkpoint creation and crash-resume, the
complete marker search precedence/cardinality router, ref CAS never
overwriting a foreign commit, the ownership Human Boundary, Terminal
Duplicate Cleanup Reservation/Action processing, and the ACTIVE-gating
invariant.
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import replace
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
    ForgeObservationInput,
    IdempotencyConflictError,
    RunStore,
    RunStoreError,
    change_request_search_observation_fact,
    is_change_request_excluded_from_legacy_database,
    load_legacy_change_request_exclusion_snapshot,
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
CHECKPOINT_ID_4 = "77777777-7777-4777-8777-777777777774"
SEARCH_RESULT_ID = "88888888-8888-4888-8888-888888888888"
FORGE_OBS_ID = "99999999-9999-4999-8999-999999999999"
RESERVATION_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
RECONCILIATION_FACT_ID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
HUMAN_BOUNDARY_ID = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
ACTION_ID = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
ACTION_OUTBOX_ID = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"
CONTROLLER_FACT_ID = "ffffffff-ffff-4fff-8fff-ffffffffffff"
RECONCILE_ACTIVITY_ID = "33333333-3333-4333-8333-333333333334"
PROJECT_ID = "12345678-1234-4234-8234-123456789011"
FORGE_INSTANCE_ID = "12345678-1234-4234-8234-123456789012"
FORGE_SECRET_ID = "12345678-1234-4234-8234-123456789013"
SOURCE_SECRET_ID = "12345678-1234-4234-8234-123456789014"

DESIRED_COMMIT = {"object_format": "sha1", "oid": "a" * 40}
BASE_COMMIT = {"object_format": "sha1", "oid": "b" * 40}


def _uid() -> str:
    return str(uuid.uuid4())


def _seed_project(store: RunStore) -> None:
    now = int(time.time() * 1000)
    for secret_id, purpose in (
        (FORGE_SECRET_ID, "FORGE_API"),
        (SOURCE_SECRET_ID, "SOURCE_READ"),
        (SECRET_ID, "PUBLICATION"),
    ):
        store.conn.execute(
            "INSERT INTO secret_current_versions(secret_id, purpose, owner_scope_kind, "
            "owner_scope_id, current_version, last_operation_id, created_at_ms, updated_at_ms) "
            "VALUES (?, ?, 'PROJECT', 'project-a', 1, ?, ?, ?)",
            (secret_id, purpose, _uid(), now, now),
        )
    store.conn.execute(
        "INSERT INTO forge_instances(forge_instance_id, adapter_kind, canonical_origin, "
        "credential_secret_id, registration_provenance_version, created_at_ms) "
        "VALUES (?, 'GITHUB', 'github.com/test-org', ?, 1, ?)",
        (FORGE_INSTANCE_ID, FORGE_SECRET_ID, now),
    )
    store.conn.execute(
        "INSERT INTO projects(project_id, forge_instance_id, installation_or_account_ref, "
        "repository_external_id, repository_locator, default_ref, trusted_base_policy_ref, "
        "budget_policy_ref, budget_reset_window_ref, source_read_secret_id, "
        "publication_secret_id, registration_source_read_secret_version, "
        "registration_publication_secret_version, registration_revision, "
        "registration_operation_id, work_item_discovery_schedule_id, registration_state) "
        "VALUES (?, ?, 'installation-a', 'repo-a', 'test-org/test-repo', 'main', "
        "'base-v1', 'budget-v1', 'window-v1', ?, ?, 1, 1, 1, ?, ?, 'ACTIVE')",
        (PROJECT_ID, FORGE_INSTANCE_ID, SOURCE_SECRET_ID, SECRET_ID, _uid(), _uid()),
    )
    store.conn.commit()


def _create_run(store: RunStore, run_id: str = RUN_ID) -> None:
    store.create_run(
        run_id=run_id,
        project_id=PROJECT_ID,
        work_item_key=f"work-{run_id}",
        state="APPROVED",
        reducer_version=DEFAULT_REDUCER_VERSION,
    )


def _plan_effect(
    store: RunStore, *, publication_id: str = PUBLICATION_ID, **overrides: object
) -> tuple:
    params = dict(
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
    params.update(overrides)
    return store.plan_publish_effect(**params)  # type: ignore[arg-type]


def _replan_effect(store: RunStore, **overrides: object) -> tuple:
    params = dict(
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
    params.update(overrides)
    return store.plan_publish_effect(**params)  # type: ignore[arg-type]


def _bound_observation(
    store: RunStore,
    *,
    kind: str,
    external_revision: str,
    fact: object,
    forge_observation_id: str | None = None,
    publication_id: str = PUBLICATION_ID,
    effect_generation: int = 1,
    cleanup_reservation_id: str | None = None,
    cleanup_action_id: str | None = None,
    cleanup_operation_digest: str | None = None,
) -> str:
    if (
        forge_observation_id is not None
        and store.get_forge_observation(forge_observation_id) is not None
    ):
        return forge_observation_id
    publication = store.get_publication(publication_id)
    effect = store.get_publication_effect(publication_id, effect_generation)
    assert publication is not None and effect is not None
    schedule_kind = {
        "BASE_HEAD": "BASE_HEAD_POLL",
        "REF_ABSENT": "REF_POLL",
        "REF_HEAD": "REF_POLL",
        "CHANGE_REQUEST_ABSENT": "CHANGE_REQUEST_SEARCH",
        "CHANGE_REQUEST_DISCOVERED": "CHANGE_REQUEST_SEARCH",
        "CHANGE_REQUEST_SEARCH_RESULT": "COMPLETE_MARKER_SEARCH",
        "CHANGE_REQUEST_HEAD": "CHANGE_REQUEST_POLL",
        "CHANGE_REQUEST_CLOSED": "CHANGE_REQUEST_POLL",
        "CHANGE_REQUEST_MARKER": "CHANGE_REQUEST_POLL",
    }[kind]
    schedule_row = store.conn.execute(
        "SELECT forge_observation_schedule_id FROM forge_observation_schedules "
        "WHERE schedule_kind = ? AND publication_id = ? AND state = 'ACTIVE' "
        "AND COALESCE(terminal_duplicate_cleanup_reservation_id, '') = COALESCE(?, '')",
        (schedule_kind, publication_id, cleanup_reservation_id),
    ).fetchone()
    if schedule_row is None:
        schedule = store.create_forge_observation_schedule(
            forge_observation_schedule_id=_uid(),
            schedule_kind=schedule_kind,
            project_id=PROJECT_ID,
            forge_instance_id=FORGE_INSTANCE_ID,
            target_kind="PUBLICATION",
            target_id=publication_id,
            run_id=publication.run_id,
            publication_id=publication_id,
            terminal_duplicate_cleanup_reservation_id=cleanup_reservation_id,
            minimum_interval_ms=1,
            next_due_at_ms=0,
        )
        schedule_id = schedule.forge_observation_schedule_id
    else:
        schedule_id = schedule_row["forge_observation_schedule_id"]
        store.conn.execute(
            "UPDATE forge_observation_schedules SET next_due_at_ms = 0 "
            "WHERE forge_observation_schedule_id = ?",
            (schedule_id,),
        )
    request = store.create_due_forge_observation_request(
        forge_observation_request_id=_uid(),
        forge_observation_schedule_id=schedule_id,
        now_ms=int(time.time() * 1000),
        controller_mode="RUNNING",
        controller_mode_revision=1,
        credential_purpose="PUBLICATION",
        credential_secret_id=SECRET_ID,
        credential_secret_version=1,
        outbox_id=_uid(),
        controller_activity_id=None if cleanup_action_id else effect.activity_id,
        effect_generation=effect_generation,
        controller_operation_digest=None if cleanup_action_id else effect.operation_digest,
        terminal_duplicate_cleanup_action_id=cleanup_action_id,
        terminal_cleanup_operation_digest=cleanup_operation_digest,
    )
    assert request is not None
    store.record_forge_observation_request_attempt(request.forge_observation_request_id)
    completion = store.complete_forge_observation_request(
        forge_observation_request_id=request.forge_observation_request_id,
        observations=[
            ForgeObservationInput(
                kind=kind,
                external_revision=external_revision,
                fact=fact,
            )
        ],
        forge_observation_id_factory=lambda: forge_observation_id or _uid(),
    )
    assert len(completion.observation_ids) == 1
    observation_id = completion.observation_ids[0]
    observation = store.get_forge_observation(observation_id)
    assert observation is not None
    assert observation.publication_effect_generation == effect_generation
    assert observation.controller_activity_id == (None if cleanup_action_id else effect.activity_id)
    assert observation.controller_operation_digest == (
        None if cleanup_action_id else effect.operation_digest
    )
    assert observation.credential_purpose == "PUBLICATION"
    assert observation.credential_secret_id == effect.publication_secret_id
    assert observation.credential_secret_version == effect.publication_secret_version
    assert observation.terminal_duplicate_cleanup_reservation_id == cleanup_reservation_id
    assert observation.terminal_duplicate_cleanup_action_id == cleanup_action_id
    assert observation.terminal_cleanup_operation_digest == cleanup_operation_digest
    return observation_id


def _checkpoint(
    store: RunStore,
    *,
    publication_effect_checkpoint_id: str | None = None,
    publication_id: str = PUBLICATION_ID,
    effect_generation: int = 1,
    suboperation_kind: str,
    status: str,
    request_idempotency_key: str | None = None,
    forge_observation_id: str | None = None,
    observed_external_revision: str | None = None,
):
    if forge_observation_id is not None:
        observed_external_revision = (
            observed_external_revision or f"{suboperation_kind.lower()}-rev"
        )
        publication = store.get_publication(publication_id)
        effect = store.get_publication_effect(publication_id, effect_generation)
        assert publication is not None and effect is not None
        kind = {
            "BASE_READ_PRE": "BASE_HEAD",
            "BASE_READ_POST": "BASE_HEAD",
            "REF_READ": "REF_ABSENT" if status == "OBSERVED_ABSENT" else "REF_HEAD",
            "REF_CREATE": "REF_HEAD",
            "REF_UPDATE": "REF_HEAD",
            "CHANGE_REQUEST_SEARCH": (
                "CHANGE_REQUEST_ABSENT"
                if status == "OBSERVED_ABSENT"
                else "CHANGE_REQUEST_DISCOVERED"
            ),
            "CHANGE_REQUEST_CREATE": "CHANGE_REQUEST_DISCOVERED",
        }[suboperation_kind]
        if suboperation_kind in ("BASE_READ_PRE", "BASE_READ_POST"):
            observed_head = (
                effect.base_commit
                if status == "OBSERVED_SATISFIED"
                else {"object_format": "sha1", "oid": "d" * 40}
            )
            fact = {
                "base_ref": effect.base_ref,
                "base_movement_policy": effect.base_movement_policy,
                "observed_head": observed_head,
            }
        elif suboperation_kind == "REF_READ" and status == "OBSERVED_ABSENT":
            fact = {
                "deterministic_ref": publication.deterministic_branch,
                "nonexistence_token": observed_external_revision,
            }
        elif suboperation_kind in ("REF_READ", "REF_CREATE", "REF_UPDATE"):
            fact = {
                "deterministic_ref": publication.deterministic_branch,
                "observed_head": (
                    effect.desired_commit
                    if status == "OBSERVED_SATISFIED"
                    else {"object_format": "sha1", "oid": "d" * 40}
                ),
            }
        elif suboperation_kind == "CHANGE_REQUEST_SEARCH" and status == "OBSERVED_ABSENT":
            fact = {
                "deterministic_ref": publication.deterministic_branch,
                "run_marker": publication.run_marker,
                "nonexistence_token": observed_external_revision,
            }
        else:
            fact = {
                "deterministic_ref": publication.deterministic_branch,
                "run_marker": publication.run_marker,
                "change_request_external_id": "1",
                "observed_head": effect.desired_commit,
            }
        _bound_observation(
            store,
            kind=kind,
            external_revision=observed_external_revision,
            fact=fact,
            forge_observation_id=forge_observation_id,
            publication_id=publication_id,
            effect_generation=effect_generation,
        )
    return store.record_publication_effect_checkpoint(
        publication_effect_checkpoint_id=publication_effect_checkpoint_id or _uid(),
        publication_id=publication_id,
        effect_generation=effect_generation,
        suboperation_kind=suboperation_kind,
        status=status,
        request_idempotency_key=request_idempotency_key,
        forge_observation_id=forge_observation_id,
        observed_external_revision=observed_external_revision,
    )


@pytest.fixture
def store(tmp_path: Path) -> RunStore:
    with RunStore(tmp_path, verify_local_filesystem=False) as s:
        _seed_project(s)
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


@pytest.mark.parametrize(
    "overrides",
    [
        {"activity_ordinal": 2},
        {"specification_generation": 1},
        {"policy_hash": "sha256:" + "1" * 64},
        {"created_transition_sequence": 2},
        {
            "semantic_input": {"publication_id": PUBLICATION_ID, "changed": True},
            "semantic_input_digest": request_digest(
                {"publication_id": PUBLICATION_ID, "changed": True}
            ),
        },
        {"outbox_id": "44444444-4444-4444-8444-444444444446"},
    ],
)
def test_plan_publish_effect_replay_conflict_checks_activity_and_outbox_fields(
    store: RunStore, overrides: dict[str, object]
) -> None:
    _plan_effect(store)
    with pytest.raises(IdempotencyConflictError):
        _plan_effect(store, **overrides)


def test_plan_publish_effect_second_generation_increments(store: RunStore) -> None:
    _publication1, effect1, activity1, outbox1 = _plan_effect(store)
    publication2, effect2, _activity2, _outbox2 = _replan_effect(store)
    assert publication2.effect_generation == 2
    assert publication2.expected_remote_commit is None
    assert effect2.effect_generation == 2
    assert effect2.expected_remote_commit is None
    assert store.get_publication_effect(PUBLICATION_ID, 1) == replace(effect1, superseded=True)
    assert store.get_activity(activity1.activity_id).state == "SUPERSEDED"  # type: ignore[union-attr]
    assert store.get_outbox(outbox1.outbox_id).state == "SUPERSEDED"  # type: ignore[union-attr]


def test_plan_publish_effect_replan_supersedes_pending_old_generation_requests(
    store: RunStore,
) -> None:
    _plan_effect(store)
    schedule = store.create_forge_observation_schedule(
        forge_observation_schedule_id=_uid(),
        schedule_kind="BASE_HEAD_POLL",
        project_id=PROJECT_ID,
        forge_instance_id=FORGE_INSTANCE_ID,
        target_kind="PUBLICATION",
        target_id=PUBLICATION_ID,
        run_id=RUN_ID,
        publication_id=PUBLICATION_ID,
        minimum_interval_ms=1,
        next_due_at_ms=0,
    )
    old_request_outbox_id = _uid()
    old_request = store.create_due_forge_observation_request(
        forge_observation_request_id=_uid(),
        forge_observation_schedule_id=schedule.forge_observation_schedule_id,
        now_ms=0,
        controller_mode="RUNNING",
        controller_mode_revision=1,
        credential_purpose="PUBLICATION",
        credential_secret_id=SECRET_ID,
        credential_secret_version=1,
        outbox_id=old_request_outbox_id,
        controller_activity_id=ACTIVITY_ID,
        effect_generation=1,
        controller_operation_digest=store.get_publication_effect(
            PUBLICATION_ID, 1
        ).operation_digest,  # type: ignore[union-attr]
    )
    assert old_request is not None

    _publication2, effect2, activity2, _outbox2 = _replan_effect(store)

    old_after = store.get_forge_observation_request(old_request.forge_observation_request_id)
    assert old_after is not None
    assert old_after.state == "SUPERSEDED"
    old_outbox = store.get_outbox(old_request_outbox_id)
    assert old_outbox is not None
    assert old_outbox.state == "SUPERSEDED"
    new_request = store.create_due_forge_observation_request(
        forge_observation_request_id=_uid(),
        forge_observation_schedule_id=schedule.forge_observation_schedule_id,
        now_ms=2,
        controller_mode="RUNNING",
        controller_mode_revision=1,
        credential_purpose="PUBLICATION",
        credential_secret_id=SECRET_ID,
        credential_secret_version=1,
        outbox_id=_uid(),
        controller_activity_id=activity2.activity_id,
        effect_generation=2,
        controller_operation_digest=effect2.operation_digest,
    )
    assert new_request is not None
    assert new_request.effect_generation == 2


def test_plan_publish_effect_replan_after_provisional_ref_uses_concrete_cas(
    store: RunStore,
) -> None:
    _plan_effect(store)
    _checkpoint(
        store,
        suboperation_kind="BASE_READ_PRE",
        status="OBSERVED_SATISFIED",
        forge_observation_id=_uid(),
        observed_external_revision="base-pre-rev",
    )
    _checkpoint(
        store,
        suboperation_kind="REF_READ",
        status="OBSERVED_ABSENT",
        forge_observation_id=_uid(),
        observed_external_revision="ref-absent-rev",
    )
    _checkpoint(
        store,
        suboperation_kind="REF_CREATE",
        status="REQUEST_READY",
        request_idempotency_key="create-ref-once",
    )
    _checkpoint(
        store,
        suboperation_kind="REF_CREATE",
        status="OBSERVED_SATISFIED",
        forge_observation_id=_uid(),
        observed_external_revision=DESIRED_COMMIT["oid"],
    )

    publication2, effect2, _activity2, _outbox2 = _replan_effect(store)

    assert publication2.expected_remote_commit == DESIRED_COMMIT["oid"]
    assert effect2.expected_remote_commit == DESIRED_COMMIT["oid"]


def test_plan_publish_effect_replan_after_ambiguous_ref_reconciliation_uses_concrete_cas(
    store: RunStore,
) -> None:
    _plan_effect(store)
    _checkpoint(
        store,
        suboperation_kind="BASE_READ_PRE",
        status="OBSERVED_SATISFIED",
        forge_observation_id=_uid(),
        observed_external_revision="base-pre-rev",
    )
    _checkpoint(
        store,
        suboperation_kind="REF_READ",
        status="OBSERVED_ABSENT",
        forge_observation_id=_uid(),
        observed_external_revision="ref-absent-rev",
    )
    _checkpoint(
        store,
        suboperation_kind="REF_CREATE",
        status="REQUEST_READY",
        request_idempotency_key="create-ref-once",
    )
    _checkpoint(
        store,
        suboperation_kind="REF_CREATE",
        status="AMBIGUOUS",
        request_idempotency_key="create-ref-once",
    )
    _checkpoint(
        store,
        suboperation_kind="REF_READ",
        status="OBSERVED_SATISFIED",
        forge_observation_id=_uid(),
        observed_external_revision=DESIRED_COMMIT["oid"],
    )

    publication2, effect2, _activity2, _outbox2 = _replan_effect(store)

    assert publication2.expected_remote_commit == DESIRED_COMMIT["oid"]
    assert effect2.expected_remote_commit == DESIRED_COMMIT["oid"]


def test_plan_publish_effect_replan_does_not_adopt_foreign_observed_ref(
    store: RunStore,
) -> None:
    _plan_effect(store)
    _checkpoint(
        store,
        suboperation_kind="BASE_READ_PRE",
        status="OBSERVED_SATISFIED",
        forge_observation_id=_uid(),
        observed_external_revision="base-pre-rev",
    )
    _checkpoint(
        store,
        suboperation_kind="REF_READ",
        status="OBSERVED_SATISFIED",
        forge_observation_id=_uid(),
        observed_external_revision="foreign-ref-commit",
    )

    publication2, effect2, _activity2, _outbox2 = _replan_effect(store)

    assert publication2.expected_remote_commit is None
    assert effect2.expected_remote_commit is None


def test_plan_publish_effect_linked_replan_never_adopts_stale_foreign_ref(
    store: RunStore,
) -> None:
    _plan_effect(store)
    foreign_commit = "d" * 40
    _checkpoint(
        store,
        suboperation_kind="BASE_READ_PRE",
        status="OBSERVED_SATISFIED",
        forge_observation_id=_uid(),
        observed_external_revision="base-pre-rev",
    )
    _checkpoint(
        store,
        suboperation_kind="REF_READ",
        status="OBSERVED_SATISFIED",
        forge_observation_id=_uid(),
        observed_external_revision=foreign_commit,
    )
    _record_search(
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
    )

    publication2, effect2, _activity2, _outbox2 = _replan_effect(store)

    assert publication2.expected_remote_commit == DESIRED_COMMIT["oid"]
    assert effect2.expected_remote_commit == DESIRED_COMMIT["oid"]


@pytest.mark.parametrize(
    "override",
    [
        {"deterministic_branch": "refs/heads/not-the-publication-ref"},
        {"run_marker": "<!-- orcest:v1:run=other;publication=other -->"},
    ],
)
def test_plan_publish_effect_replan_rejects_immutable_target_change(
    store: RunStore, override: dict[str, object]
) -> None:
    _plan_effect(store)

    with pytest.raises(RunStoreError, match="cannot change"):
        _replan_effect(store, **override)


def test_ref_read_checkpoint_advances_to_branch_observed(store: RunStore) -> None:
    _plan_effect(store)
    base_observation_id = _bound_observation(
        store,
        kind="BASE_HEAD",
        external_revision="base-pre-rev",
        fact={
            "base_ref": "refs/heads/main",
            "base_movement_policy": "REBASE_BEFORE_PUBLICATION",
            "observed_head": BASE_COMMIT,
        },
    )
    _checkpoint(
        store,
        suboperation_kind="BASE_READ_PRE",
        status="OBSERVED_SATISFIED",
        forge_observation_id=base_observation_id,
        observed_external_revision="base-pre-rev",
    )
    checkpoint = _checkpoint(
        store,
        publication_effect_checkpoint_id=CHECKPOINT_ID_1,
        publication_id=PUBLICATION_ID,
        effect_generation=1,
        suboperation_kind="REF_READ",
        status="OBSERVED_ABSENT",
        forge_observation_id=FORGE_OBS_ID,
        observed_external_revision="ref-absent-rev",
    )
    assert checkpoint.checkpoint_sequence == 2
    publication = store.get_publication(PUBLICATION_ID)
    assert publication is not None
    assert publication.state == "BRANCH_OBSERVED"


def test_checkpoint_sequence_strictly_increases_and_resumes(store: RunStore) -> None:
    _plan_effect(store)
    _checkpoint(
        store,
        publication_effect_checkpoint_id=CHECKPOINT_ID_1,
        publication_id=PUBLICATION_ID,
        effect_generation=1,
        suboperation_kind="BASE_READ_PRE",
        status="OBSERVED_SATISFIED",
        forge_observation_id=FORGE_OBS_ID,
    )
    _checkpoint(
        store,
        publication_effect_checkpoint_id=CHECKPOINT_ID_2,
        publication_id=PUBLICATION_ID,
        effect_generation=1,
        suboperation_kind="REF_READ",
        status="OBSERVED_ABSENT",
        forge_observation_id=_uid(),
    )
    checkpoints = store.list_publication_effect_checkpoints(PUBLICATION_ID, 1)
    assert [c.checkpoint_sequence for c in checkpoints] == [1, 2]
    assert [c.suboperation_kind for c in checkpoints] == ["BASE_READ_PRE", "REF_READ"]


def test_checkpoint_replay_returns_existing_row(store: RunStore) -> None:
    _plan_effect(store)
    first = _checkpoint(
        store,
        publication_effect_checkpoint_id=CHECKPOINT_ID_1,
        publication_id=PUBLICATION_ID,
        effect_generation=1,
        suboperation_kind="BASE_READ_PRE",
        status="OBSERVED_SATISFIED",
        forge_observation_id=FORGE_OBS_ID,
    )
    second = _checkpoint(
        store,
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
    _ensure_initial_search_phase(store)
    first = _checkpoint(
        store,
        publication_effect_checkpoint_id=CHECKPOINT_ID_1,
        publication_id=PUBLICATION_ID,
        effect_generation=1,
        suboperation_kind="REF_CREATE",
        status="REQUEST_READY",
        request_idempotency_key="ref-create-1",
    )
    second = _checkpoint(
        store,
        publication_effect_checkpoint_id=CHECKPOINT_ID_2,
        publication_id=PUBLICATION_ID,
        effect_generation=1,
        suboperation_kind="REF_CREATE",
        status="REQUEST_READY",
        request_idempotency_key="ref-create-1",
    )
    assert second == first
    with pytest.raises(IdempotencyConflictError):
        _checkpoint(
            store,
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
        _checkpoint(
            store,
            publication_effect_checkpoint_id=CHECKPOINT_ID_1,
            publication_id=PUBLICATION_ID,
            effect_generation=1,
            suboperation_kind="REF_CREATE",
            status="REQUEST_READY",
        )


def test_checkpoint_matrix_rejects_forbidden_observation(store: RunStore) -> None:
    _plan_effect(store)
    with pytest.raises(ValueError, match="forbids forge_observation_id"):
        _checkpoint(
            store,
            publication_effect_checkpoint_id=CHECKPOINT_ID_1,
            publication_id=PUBLICATION_ID,
            effect_generation=1,
            suboperation_kind="REF_CREATE",
            status="REQUEST_READY",
            request_idempotency_key="req-key-1",
            forge_observation_id=FORGE_OBS_ID,
        )


def test_checkpoint_status_must_match_authenticated_observation_fact(
    store: RunStore,
) -> None:
    _plan_effect(store)
    observation_id = _bound_observation(
        store,
        kind="BASE_HEAD",
        external_revision="moved-base-rev",
        fact={
            "base_ref": "refs/heads/main",
            "base_movement_policy": "REBASE_BEFORE_PUBLICATION",
            "observed_head": {"object_format": "sha1", "oid": "d" * 40},
        },
    )

    with pytest.raises(ValueError, match="cannot claim satisfied for a moved base"):
        store.record_publication_effect_checkpoint(
            publication_effect_checkpoint_id=_uid(),
            publication_id=PUBLICATION_ID,
            effect_generation=1,
            suboperation_kind="BASE_READ_PRE",
            status="OBSERVED_SATISFIED",
            forge_observation_id=observation_id,
            observed_external_revision="moved-base-rev",
        )


def test_ref_read_satisfied_requires_the_desired_head(store: RunStore) -> None:
    _plan_effect(store)
    _checkpoint(
        store,
        suboperation_kind="BASE_READ_PRE",
        status="OBSERVED_SATISFIED",
        forge_observation_id=_uid(),
    )
    publication = store.get_publication(PUBLICATION_ID)
    assert publication is not None
    observation_id = _bound_observation(
        store,
        kind="REF_HEAD",
        external_revision="foreign-ref-rev",
        fact={
            "deterministic_ref": publication.deterministic_branch,
            "observed_head": {"object_format": "sha1", "oid": "d" * 40},
        },
    )

    with pytest.raises(ValueError, match="cannot claim satisfied for a foreign head"):
        store.record_publication_effect_checkpoint(
            publication_effect_checkpoint_id=_uid(),
            publication_id=PUBLICATION_ID,
            effect_generation=1,
            suboperation_kind="REF_READ",
            status="OBSERVED_SATISFIED",
            forge_observation_id=observation_id,
            observed_external_revision="foreign-ref-rev",
        )


def test_checkpoint_rejects_observation_missing_completed_request_membership(
    store: RunStore,
) -> None:
    _plan_effect(store)
    observation_id = _bound_observation(
        store,
        kind="BASE_HEAD",
        external_revision="base-pre-rev",
        fact={
            "base_ref": "refs/heads/main",
            "base_movement_policy": "REBASE_BEFORE_PUBLICATION",
            "observed_head": BASE_COMMIT,
        },
    )
    store.conn.execute(
        "DELETE FROM forge_observation_request_results WHERE forge_observation_id = ?",
        (observation_id,),
    )

    with pytest.raises(RunStoreError, match="result of a completed current Forge request"):
        store.record_publication_effect_checkpoint(
            publication_effect_checkpoint_id=_uid(),
            publication_id=PUBLICATION_ID,
            effect_generation=1,
            suboperation_kind="BASE_READ_PRE",
            status="OBSERVED_SATISFIED",
            forge_observation_id=observation_id,
            observed_external_revision="base-pre-rev",
        )


def test_checkpoint_rejects_stale_observation_after_newer_target_fact(
    store: RunStore,
) -> None:
    _plan_effect(store)
    stale_observation_id = _bound_observation(
        store,
        kind="BASE_HEAD",
        external_revision="base-pre-rev-1",
        fact={
            "base_ref": "refs/heads/main",
            "base_movement_policy": "REBASE_BEFORE_PUBLICATION",
            "observed_head": BASE_COMMIT,
        },
    )
    _bound_observation(
        store,
        kind="BASE_HEAD",
        external_revision="base-pre-rev-2",
        fact={
            "base_ref": "refs/heads/main",
            "base_movement_policy": "REBASE_BEFORE_PUBLICATION",
            "observed_head": BASE_COMMIT,
        },
    )

    with pytest.raises(RunStoreError, match="completed current Forge request"):
        store.record_publication_effect_checkpoint(
            publication_effect_checkpoint_id=_uid(),
            publication_id=PUBLICATION_ID,
            effect_generation=1,
            suboperation_kind="BASE_READ_PRE",
            status="OBSERVED_SATISFIED",
            forge_observation_id=stale_observation_id,
            observed_external_revision="base-pre-rev-1",
        )


def test_checkpoint_rejects_noncanonical_observation_payload_digest(
    store: RunStore,
) -> None:
    _plan_effect(store)
    observation_id = _bound_observation(
        store,
        kind="BASE_HEAD",
        external_revision="base-pre-rev",
        fact={
            "base_ref": "refs/heads/main",
            "base_movement_policy": "REBASE_BEFORE_PUBLICATION",
            "observed_head": BASE_COMMIT,
        },
    )
    store.conn.execute(
        "UPDATE forge_observations SET payload_digest = ? WHERE forge_observation_id = ?",
        ("sha256:" + "f" * 64, observation_id),
    )

    with pytest.raises(RunStoreError, match="payload digest is not canonical"):
        store.record_publication_effect_checkpoint(
            publication_effect_checkpoint_id=_uid(),
            publication_id=PUBLICATION_ID,
            effect_generation=1,
            suboperation_kind="BASE_READ_PRE",
            status="OBSERVED_SATISFIED",
            forge_observation_id=observation_id,
            observed_external_revision="base-pre-rev",
        )


def test_one_observation_cannot_satisfy_pre_and_post_base_phases(
    store: RunStore,
) -> None:
    _plan_effect(store)
    base_observation_id = _bound_observation(
        store,
        kind="BASE_HEAD",
        external_revision="base-stable-rev",
        fact={
            "base_ref": "refs/heads/main",
            "base_movement_policy": "REBASE_BEFORE_PUBLICATION",
            "observed_head": BASE_COMMIT,
        },
    )
    _checkpoint(
        store,
        suboperation_kind="BASE_READ_PRE",
        status="OBSERVED_SATISFIED",
        forge_observation_id=base_observation_id,
        observed_external_revision="base-stable-rev",
    )
    _checkpoint(
        store,
        suboperation_kind="REF_READ",
        status="OBSERVED_ABSENT",
        forge_observation_id=_uid(),
    )
    _record_search(
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
    )

    with pytest.raises(RunStoreError, match="cannot satisfy multiple Publication phases"):
        store.record_publication_effect_checkpoint(
            publication_effect_checkpoint_id=_uid(),
            publication_id=PUBLICATION_ID,
            effect_generation=1,
            suboperation_kind="BASE_READ_POST",
            status="OBSERVED_SATISFIED",
            forge_observation_id=base_observation_id,
            observed_external_revision="base-stable-rev",
        )


def test_base_mismatch_supersedes_effect_activity_and_pending_outbox(store: RunStore) -> None:
    _publication, _effect, activity, outbox = _plan_effect(store)

    _checkpoint(
        store,
        suboperation_kind="BASE_READ_PRE",
        status="BASE_MISMATCH",
        forge_observation_id=_uid(),
        observed_external_revision="moved-base-rev",
    )

    effect_after = store.get_publication_effect(PUBLICATION_ID, 1)
    activity_after = store.get_activity(activity.activity_id)
    outbox_after = store.get_outbox(outbox.outbox_id)
    assert effect_after is not None and effect_after.superseded
    assert activity_after is not None and activity_after.state == "SUPERSEDED"
    assert outbox_after is not None and outbox_after.state == "SUPERSEDED"
    with pytest.raises(RunStoreError, match="base-mismatched"):
        _checkpoint(
            store,
            suboperation_kind="BASE_READ_PRE",
            status="OBSERVED_SATISFIED",
            forge_observation_id=_uid(),
            observed_external_revision="original-base-rev",
        )


def test_post_link_base_mismatch_supersedes_effect_and_blocks_completion(
    store: RunStore,
) -> None:
    _plan_effect(store)
    _record_search(
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
    )
    _checkpoint(
        store,
        suboperation_kind="BASE_READ_POST",
        status="BASE_MISMATCH",
        forge_observation_id=_uid(),
        observed_external_revision="moved-base-rev",
    )

    effect_after = store.get_publication_effect(PUBLICATION_ID, 1)
    activity_after = store.get_activity(ACTIVITY_ID)
    assert effect_after is not None and effect_after.superseded
    assert activity_after is not None and activity_after.state == "SUPERSEDED"
    with pytest.raises(RunStoreError, match="base-mismatched"):
        _checkpoint(
            store,
            suboperation_kind="BASE_READ_POST",
            status="OBSERVED_SATISFIED",
            forge_observation_id=_uid(),
            observed_external_revision="original-base-rev",
        )


def test_ambiguous_mutation_requires_prior_same_key_request_ready(store: RunStore) -> None:
    _plan_effect(store)
    _ensure_initial_search_phase(store)

    with pytest.raises(RunStoreError, match="prior same-key REQUEST_READY"):
        _checkpoint(
            store,
            suboperation_kind="REF_CREATE",
            status="AMBIGUOUS",
            request_idempotency_key="never-prepared",
        )

    _checkpoint(
        store,
        suboperation_kind="REF_CREATE",
        status="REQUEST_READY",
        request_idempotency_key="prepared-key",
    )
    with pytest.raises(RunStoreError, match="prior same-key REQUEST_READY"):
        _checkpoint(
            store,
            suboperation_kind="REF_CREATE",
            status="AMBIGUOUS",
            request_idempotency_key="different-key",
        )


def test_stale_generation_checkpoint_is_audit_only(store: RunStore) -> None:
    _plan_effect(store)
    _ensure_initial_search_phase(store)
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
    _checkpoint(
        store,
        publication_effect_checkpoint_id=CHECKPOINT_ID_1,
        publication_id=PUBLICATION_ID,
        effect_generation=1,
        suboperation_kind="REF_READ",
        status="OBSERVED_ABSENT",
        forge_observation_id=_uid(),
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


def _ensure_initial_search_phase(
    store: RunStore,
    *,
    publication_id: str = PUBLICATION_ID,
    effect_generation: int = 1,
) -> None:
    publication = store.get_publication(publication_id)
    assert publication is not None
    checkpoints = store.list_publication_effect_checkpoints(publication_id, effect_generation)
    if not any(c.suboperation_kind == "BASE_READ_PRE" for c in checkpoints):
        observation_id = _bound_observation(
            store,
            kind="BASE_HEAD",
            external_revision="base-pre-rev",
            fact={
                "base_ref": "refs/heads/main",
                "base_movement_policy": "REBASE_BEFORE_PUBLICATION",
                "observed_head": BASE_COMMIT,
            },
            publication_id=publication_id,
            effect_generation=effect_generation,
        )
        _checkpoint(
            store,
            publication_id=publication_id,
            effect_generation=effect_generation,
            suboperation_kind="BASE_READ_PRE",
            status="OBSERVED_SATISFIED",
            forge_observation_id=observation_id,
            observed_external_revision="base-pre-rev",
        )
    checkpoints = store.list_publication_effect_checkpoints(publication_id, effect_generation)
    if not any(c.suboperation_kind == "REF_READ" for c in checkpoints):
        observation_id = _bound_observation(
            store,
            kind="REF_ABSENT",
            external_revision="ref-absent-rev",
            fact={
                "deterministic_ref": publication.deterministic_branch,
                "nonexistence_token": "ref-absent-rev",
            },
            publication_id=publication_id,
            effect_generation=effect_generation,
        )
        _checkpoint(
            store,
            publication_id=publication_id,
            effect_generation=effect_generation,
            suboperation_kind="REF_READ",
            status="OBSERVED_ABSENT",
            forge_observation_id=observation_id,
            observed_external_revision="ref-absent-rev",
        )


def _ensure_create_provenance(
    store: RunStore,
    *,
    publication_id: str,
    effect_generation: int,
) -> tuple[str, str]:
    existing = store.conn.execute(
        "SELECT publication_effect_checkpoint_id, request_idempotency_key "
        "FROM publication_effect_checkpoints WHERE publication_id = ? "
        "AND effect_generation = ? AND suboperation_kind = 'CHANGE_REQUEST_CREATE' "
        "AND status = 'AMBIGUOUS' ORDER BY checkpoint_sequence DESC LIMIT 1",
        (publication_id, effect_generation),
    ).fetchone()
    if existing is not None:
        return existing["publication_effect_checkpoint_id"], existing["request_idempotency_key"]

    publication = store.get_publication(publication_id)
    assert publication is not None
    preliminary_revision = f"preliminary-{_uid()}"
    preliminary_fact = change_request_search_observation_fact(
        publication_id=publication_id,
        effect_generation=effect_generation,
        run_marker=publication.run_marker,
        deterministic_ref=publication.deterministic_branch,
        external_revision=preliminary_revision,
        members=(),
    )
    preliminary_observation_id = _bound_observation(
        store,
        kind="CHANGE_REQUEST_SEARCH_RESULT",
        external_revision=preliminary_revision,
        fact=preliminary_fact,
        publication_id=publication_id,
        effect_generation=effect_generation,
    )
    store.record_change_request_search_result(
        change_request_search_result_id=_uid(),
        forge_observation_id=preliminary_observation_id,
        publication_effect_checkpoint_id=_uid(),
        publication_id=publication_id,
        effect_generation=effect_generation,
        project_id=PROJECT_ID,
        run_marker=publication.run_marker,
        deterministic_ref=publication.deterministic_branch,
        external_revision=preliminary_revision,
        members=(),
    )
    absent_observation_id = _bound_observation(
        store,
        kind="CHANGE_REQUEST_ABSENT",
        external_revision="change-request-absent-rev",
        fact={
            "deterministic_ref": publication.deterministic_branch,
            "run_marker": publication.run_marker,
            "nonexistence_token": "change-request-absent-rev",
        },
        publication_id=publication_id,
        effect_generation=effect_generation,
    )
    _checkpoint(
        store,
        publication_id=publication_id,
        effect_generation=effect_generation,
        suboperation_kind="CHANGE_REQUEST_SEARCH",
        status="OBSERVED_ABSENT",
        forge_observation_id=absent_observation_id,
        observed_external_revision="change-request-absent-rev",
    )
    request_key = _uid()
    _checkpoint(
        store,
        publication_id=publication_id,
        effect_generation=effect_generation,
        suboperation_kind="CHANGE_REQUEST_CREATE",
        status="REQUEST_READY",
        request_idempotency_key=request_key,
    )
    ambiguous_checkpoint_id = _uid()
    _checkpoint(
        store,
        publication_effect_checkpoint_id=ambiguous_checkpoint_id,
        publication_id=publication_id,
        effect_generation=effect_generation,
        suboperation_kind="CHANGE_REQUEST_CREATE",
        status="AMBIGUOUS",
        request_idempotency_key=request_key,
    )
    return ambiguous_checkpoint_id, request_key


def _record_search(store: RunStore, **kwargs: object):
    publication_id = str(kwargs.get("publication_id", PUBLICATION_ID))
    effect_generation = int(kwargs.get("effect_generation", 1))
    _ensure_initial_search_phase(
        store,
        publication_id=publication_id,
        effect_generation=effect_generation,
    )
    members = tuple(kwargs.get("members", ()))
    positive_members = [member for member in members if member.ownership_status == "POSITIVE"]
    if positive_members:
        create_checkpoint_id, create_request_key = _ensure_create_provenance(
            store,
            publication_id=publication_id,
            effect_generation=effect_generation,
        )
        publication = store.get_publication(publication_id)
        effect = store.get_publication_effect(publication_id, effect_generation)
        assert publication is not None and effect is not None
        members = tuple(
            replace(
                member,
                proof_kind="AMBIGUOUS_CREATE_RECONCILED",
                proof_publication_effect_generation=effect_generation,
                proof_create_checkpoint_id=create_checkpoint_id,
                proof_create_request_idempotency_key=create_request_key,
                creator_installation_or_account_ref="installation-a",
                proof_deterministic_ref=publication.deterministic_branch,
                proof_run_marker=publication.run_marker,
                proof_desired_commit=effect.desired_commit,
                proof_observed_head=member.observed_head,
                head_evidence_observation_id=str(kwargs["forge_observation_id"]),
            )
            if member.ownership_status == "POSITIVE"
            else member
            for member in members
        )
    publication = store.get_publication(publication_id)
    assert publication is not None
    external_revision = str(kwargs["external_revision"])
    observation_fact = change_request_search_observation_fact(
        publication_id=publication_id,
        effect_generation=effect_generation,
        run_marker=str(kwargs["run_marker"]),
        deterministic_ref=str(kwargs["deterministic_ref"]),
        external_revision=external_revision,
        members=members,
    )
    _bound_observation(
        store,
        kind="CHANGE_REQUEST_SEARCH_RESULT",
        external_revision=external_revision,
        fact=observation_fact,
        forge_observation_id=str(kwargs["forge_observation_id"]),
        publication_id=publication_id,
        effect_generation=effect_generation,
    )
    kwargs["members"] = members
    return store.record_change_request_search_result(**kwargs)  # type: ignore[arg-type]


def _complete_publication(store: RunStore, **kwargs: object):
    publication_id = str(kwargs.get("publication_id", PUBLICATION_ID))
    effect_generation = int(kwargs.get("effect_generation", 1))
    publication = store.get_publication(publication_id)
    effect = store.get_publication_effect(publication_id, effect_generation)
    assert publication is not None and effect is not None
    _bound_observation(
        store,
        kind="CHANGE_REQUEST_HEAD",
        external_revision=str(kwargs["observed_external_revision"]),
        fact={
            "change_request_external_id": publication.change_request_external_id,
            "observed_head": effect.desired_commit,
        },
        forge_observation_id=str(kwargs["forge_observation_id"]),
        publication_id=publication_id,
        effect_generation=effect_generation,
    )
    return store.complete_publication_effect(**kwargs)  # type: ignore[arg-type]


def _complete_cleanup_action(store: RunStore, **kwargs: object):
    action_id = str(kwargs["terminal_duplicate_cleanup_action_id"])
    action = store.conn.execute(
        "SELECT * FROM terminal_duplicate_cleanup_actions "
        "WHERE terminal_duplicate_cleanup_action_id = ?",
        (action_id,),
    ).fetchone()
    assert action is not None
    if action["planned_action"] != "RECORD_ONLY" and action["state"] != "COMPLETED":
        reservation = store.conn.execute(
            "SELECT * FROM terminal_duplicate_cleanup_reservations "
            "WHERE terminal_duplicate_cleanup_reservation_id = ?",
            (action["terminal_duplicate_cleanup_reservation_id"],),
        ).fetchone()
        assert reservation is not None
        source_member = store.conn.execute(
            "SELECT c.* FROM change_request_search_members c "
            "JOIN change_request_search_results s "
            "ON s.change_request_search_result_id = c.change_request_search_result_id "
            "WHERE s.forge_observation_id = ? AND c.member_class = 'LIVE' "
            "AND c.change_request_external_id = ?",
            (
                reservation["selecting_search_observation_id"],
                action["change_request_external_id"],
            ),
        ).fetchone()
        assert source_member is not None
        outcome = str(kwargs["outcome"])
        observation_id = _bound_observation(
            store,
            kind=(
                "CHANGE_REQUEST_CLOSED"
                if action["planned_action"] == "CLOSE"
                else "CHANGE_REQUEST_MARKER"
            ),
            external_revision=f"cleanup-{_uid()}",
            fact={
                "terminal_duplicate_cleanup_action_id": action_id,
                "change_request_external_id": action["change_request_external_id"],
                "planned_action": action["planned_action"],
                "expected_head": json.loads(source_member["observed_head_json"]),
                "expected_body_revision": source_member["observed_body_revision"],
                "expected_marker_set_digest": source_member["marker_set_digest"],
                "outcome": outcome,
            },
            cleanup_reservation_id=action["terminal_duplicate_cleanup_reservation_id"],
            cleanup_action_id=action_id,
            cleanup_operation_digest=action["operation_digest"],
        )
        kwargs["forge_observation_id"] = observation_id
    return store.complete_terminal_duplicate_cleanup_action(**kwargs)  # type: ignore[arg-type]


def test_one_live_rejects_unbound_boolean_confirmation(store: RunStore) -> None:
    _plan_effect(store)
    _ensure_initial_search_phase(store)
    member = _member()
    fact = change_request_search_observation_fact(
        publication_id=PUBLICATION_ID,
        effect_generation=1,
        run_marker=render_run_marker(run_id=RUN_ID, publication_id=PUBLICATION_ID),
        deterministic_ref=deterministic_publication_ref(RUN_ID),
        external_revision="search-rev-1",
        members=(member,),
    )
    _bound_observation(
        store,
        kind="CHANGE_REQUEST_SEARCH_RESULT",
        external_revision="search-rev-1",
        fact=fact,
        forge_observation_id=FORGE_OBS_ID,
    )
    with pytest.raises(ValueError, match="incomplete ownership proof"):
        store.record_change_request_search_result(
            change_request_search_result_id=SEARCH_RESULT_ID,
            forge_observation_id=FORGE_OBS_ID,
            publication_effect_checkpoint_id=CHECKPOINT_ID_1,
            publication_id=PUBLICATION_ID,
            effect_generation=1,
            project_id=PROJECT_ID,
            run_marker=render_run_marker(run_id=RUN_ID, publication_id=PUBLICATION_ID),
            deterministic_ref=deterministic_publication_ref(RUN_ID),
            external_revision="search-rev-1",
            members=(member,),
            fresh_exact_object_confirmed=True,
        )


def test_exact_create_proof_requires_the_create_response_observation(
    store: RunStore,
) -> None:
    _plan_effect(store)
    _record_search(
        store,
        change_request_search_result_id=_uid(),
        forge_observation_id=_uid(),
        publication_effect_checkpoint_id=_uid(),
        publication_id=PUBLICATION_ID,
        effect_generation=1,
        project_id=PROJECT_ID,
        run_marker=render_run_marker(run_id=RUN_ID, publication_id=PUBLICATION_ID),
        deterministic_ref=deterministic_publication_ref(RUN_ID),
        external_revision="pre-create-search-rev",
        members=(),
    )
    _checkpoint(
        store,
        suboperation_kind="CHANGE_REQUEST_SEARCH",
        status="OBSERVED_ABSENT",
        forge_observation_id=_uid(),
        observed_external_revision="change-request-absent-rev",
    )
    request_key = _uid()
    _checkpoint(
        store,
        suboperation_kind="CHANGE_REQUEST_CREATE",
        status="REQUEST_READY",
        request_idempotency_key=request_key,
    )
    publication = store.get_publication(PUBLICATION_ID)
    effect = store.get_publication_effect(PUBLICATION_ID, 1)
    assert publication is not None and effect is not None
    create_observation_id = _bound_observation(
        store,
        kind="CHANGE_REQUEST_DISCOVERED",
        external_revision="create-response-rev",
        fact={
            "deterministic_ref": publication.deterministic_branch,
            "run_marker": publication.run_marker,
            "change_request_external_id": "1",
            "observed_head": effect.desired_commit,
        },
    )
    create_checkpoint = _checkpoint(
        store,
        suboperation_kind="CHANGE_REQUEST_CREATE",
        status="OBSERVED_SATISFIED",
        request_idempotency_key=request_key,
        forge_observation_id=create_observation_id,
        observed_external_revision="create-response-rev",
    )

    current_search_observation_id = _uid()
    member = _member(
        proof_kind="EXACT_CREATE_RESPONSE",
        proof_publication_effect_generation=1,
        proof_create_checkpoint_id=create_checkpoint.publication_effect_checkpoint_id,
        proof_create_request_idempotency_key=request_key,
        creator_installation_or_account_ref="installation-a",
        proof_deterministic_ref=publication.deterministic_branch,
        proof_run_marker=publication.run_marker,
        proof_desired_commit=effect.desired_commit,
        proof_observed_head=effect.desired_commit,
        head_evidence_observation_id=current_search_observation_id,
    )
    search_fact = change_request_search_observation_fact(
        publication_id=PUBLICATION_ID,
        effect_generation=1,
        run_marker=publication.run_marker,
        deterministic_ref=publication.deterministic_branch,
        external_revision="post-create-search-rev",
        members=(member,),
    )
    _bound_observation(
        store,
        kind="CHANGE_REQUEST_SEARCH_RESULT",
        external_revision="post-create-search-rev",
        fact=search_fact,
        forge_observation_id=current_search_observation_id,
    )

    with pytest.raises(RunStoreError, match="exact publication effect"):
        store.record_change_request_search_result(
            change_request_search_result_id=_uid(),
            forge_observation_id=current_search_observation_id,
            publication_effect_checkpoint_id=_uid(),
            publication_id=PUBLICATION_ID,
            effect_generation=1,
            project_id=PROJECT_ID,
            run_marker=publication.run_marker,
            deterministic_ref=publication.deterministic_branch,
            external_revision="post-create-search-rev",
            members=(member,),
        )


def test_one_live_advances_to_change_request_observed(store: RunStore) -> None:
    _plan_effect(store)
    record, outcome = _record_search(
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
    assert outcome.outcome == "ONE_LIVE"
    assert record.live_cardinality == "ONE"
    publication = store.get_publication(PUBLICATION_ID)
    assert publication is not None
    assert publication.state == "CHANGE_REQUEST_OBSERVED"
    assert publication.change_request_external_id == "1"


def test_linked_replan_fresh_same_association_restores_change_request_observed(
    store: RunStore,
) -> None:
    _plan_effect(store)
    _record_search(
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
    )
    _checkpoint(
        store,
        suboperation_kind="BASE_READ_POST",
        status="BASE_MISMATCH",
        forge_observation_id=_uid(),
        observed_external_revision="moved-base-rev",
    )
    _replan_effect(store)
    _ensure_initial_search_phase(store, effect_generation=2)
    publication = store.get_publication(PUBLICATION_ID)
    effect = store.get_publication_effect(PUBLICATION_ID, 2)
    assert publication is not None and effect is not None
    search_observation_id = _uid()
    member = replace(
        _member(observed_head=effect.desired_commit),
        proof_kind="LIVE_ASSOCIATION",
        proof_publication_effect_generation=2,
        proof_create_checkpoint_id=None,
        proof_create_request_idempotency_key=None,
        creator_installation_or_account_ref="installation-a",
        proof_deterministic_ref=publication.deterministic_branch,
        proof_run_marker=publication.run_marker,
        proof_desired_commit=effect.desired_commit,
        proof_observed_head=effect.desired_commit,
        head_evidence_observation_id=search_observation_id,
    )
    observation_fact = change_request_search_observation_fact(
        publication_id=PUBLICATION_ID,
        effect_generation=2,
        run_marker=publication.run_marker,
        deterministic_ref=publication.deterministic_branch,
        external_revision="search-rev-2",
        members=(member,),
    )
    _bound_observation(
        store,
        kind="CHANGE_REQUEST_SEARCH_RESULT",
        external_revision="search-rev-2",
        fact=observation_fact,
        forge_observation_id=search_observation_id,
        effect_generation=2,
    )
    store.record_change_request_search_result(
        change_request_search_result_id=_uid(),
        forge_observation_id=search_observation_id,
        publication_effect_checkpoint_id=_uid(),
        publication_id=PUBLICATION_ID,
        effect_generation=2,
        project_id=PROJECT_ID,
        run_marker=publication.run_marker,
        deterministic_ref=publication.deterministic_branch,
        external_revision="search-rev-2",
        members=(member,),
    )

    publication_after = store.get_publication(PUBLICATION_ID)
    assert publication_after is not None
    assert publication_after.state == "CHANGE_REQUEST_OBSERVED"
    assert publication_after.change_request_external_id == "1"
    assert publication_after.observed_remote_commit == effect.desired_commit["oid"]
    _checkpoint(
        store,
        effect_generation=2,
        suboperation_kind="BASE_READ_POST",
        status="OBSERVED_SATISFIED",
        forge_observation_id=_uid(),
        observed_external_revision="base-post-rev",
    )


def test_conflicting_one_live_after_prior_association_raises(store: RunStore) -> None:
    _plan_effect(store)
    _record_search(
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
        members=(_member(change_request_external_id="1"),),
        fresh_exact_object_confirmed=True,
    )
    with pytest.raises(RunStoreError, match="different live change request association"):
        _record_search(
            store,
            change_request_search_result_id="88888888-8888-4888-8888-888888888889",
            forge_observation_id="99999999-9999-4999-8999-999999999998",
            publication_effect_checkpoint_id=CHECKPOINT_ID_2,
            publication_id=PUBLICATION_ID,
            effect_generation=1,
            project_id=PROJECT_ID,
            run_marker=render_run_marker(run_id=RUN_ID, publication_id=PUBLICATION_ID),
            deterministic_ref=deterministic_publication_ref(RUN_ID),
            external_revision="search-rev-2",
            members=(_member(change_request_external_id="2"),),
            fresh_exact_object_confirmed=True,
        )


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
    first = _record_search(
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
        members=members,
        terminal_publication_effect_checkpoint_id=CHECKPOINT_ID_2,
    )
    second = _record_search(
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
        members=members,
        terminal_publication_effect_checkpoint_id=CHECKPOINT_ID_2,
    )
    assert second == first


def test_change_request_search_result_replay_conflict_raises(store: RunStore) -> None:
    _plan_effect(store)
    _record_search(
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
        members=(),
    )
    with pytest.raises(IdempotencyConflictError):
        _record_search(
            store,
            change_request_search_result_id=SEARCH_RESULT_ID,
            forge_observation_id=FORGE_OBS_ID,
            publication_effect_checkpoint_id=CHECKPOINT_ID_1,
            publication_id=PUBLICATION_ID,
            effect_generation=1,
            project_id=PROJECT_ID,
            run_marker=render_run_marker(run_id=RUN_ID, publication_id=PUBLICATION_ID),
            deterministic_ref=deterministic_publication_ref(RUN_ID),
            external_revision="search-rev-2",
            members=(),
        )


def test_complete_publication_effect_requires_base_read_post(store: RunStore) -> None:
    _plan_effect(store)
    _record_search(
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
    with pytest.raises(RunStoreError, match="BASE_READ_POST"):
        _complete_publication(
            store,
            publication_effect_checkpoint_id=CHECKPOINT_ID_2,
            publication_id=PUBLICATION_ID,
            effect_generation=1,
            forge_observation_id=_uid(),
            observed_external_revision="a" * 40,
        )


def test_complete_marker_search_cannot_regress_after_base_read_post(store: RunStore) -> None:
    _plan_effect(store)
    _record_search(
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
    )
    _checkpoint(
        store,
        publication_effect_checkpoint_id=CHECKPOINT_ID_2,
        publication_id=PUBLICATION_ID,
        effect_generation=1,
        suboperation_kind="BASE_READ_POST",
        status="OBSERVED_SATISFIED",
        forge_observation_id=_uid(),
    )

    with pytest.raises(RunStoreError, match="cannot follow BASE_READ_POST"):
        _record_search(
            store,
            change_request_search_result_id=_uid(),
            forge_observation_id=_uid(),
            publication_effect_checkpoint_id=CHECKPOINT_ID_3,
            publication_id=PUBLICATION_ID,
            effect_generation=1,
            project_id=PROJECT_ID,
            run_marker=render_run_marker(run_id=RUN_ID, publication_id=PUBLICATION_ID),
            deterministic_ref=deterministic_publication_ref(RUN_ID),
            external_revision="search-rev-2",
            members=(),
        )


def test_complete_publication_effect_sets_active(store: RunStore) -> None:
    _plan_effect(store)
    _record_search(
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
    _checkpoint(
        store,
        publication_effect_checkpoint_id=CHECKPOINT_ID_2,
        publication_id=PUBLICATION_ID,
        effect_generation=1,
        suboperation_kind="BASE_READ_POST",
        status="OBSERVED_SATISFIED",
        forge_observation_id=_uid(),
    )
    _complete_publication(
        store,
        publication_effect_checkpoint_id=CHECKPOINT_ID_3,
        publication_id=PUBLICATION_ID,
        effect_generation=1,
        forge_observation_id=_uid(),
        observed_external_revision="a" * 40,
    )
    publication = store.get_publication(PUBLICATION_ID)
    assert publication is not None
    assert publication.state == "ACTIVE"
    checkpoints = store.list_publication_effect_checkpoints(PUBLICATION_ID, 1)
    assert checkpoints[-1].suboperation_kind == "COMPLETE"
    assert checkpoints[-1].status == "COMPLETED"
    activity = store.get_activity(ACTIVITY_ID)
    assert activity is not None
    assert activity.state == "SUCCEEDED"
    with pytest.raises(RunStoreError, match="cannot replan"):
        _replan_effect(store)


def test_complete_publication_effect_replay_after_active_returns_checkpoint(
    store: RunStore,
) -> None:
    _plan_effect(store)
    _record_search(
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
    _checkpoint(
        store,
        publication_effect_checkpoint_id=CHECKPOINT_ID_2,
        publication_id=PUBLICATION_ID,
        effect_generation=1,
        suboperation_kind="BASE_READ_POST",
        status="OBSERVED_SATISFIED",
        forge_observation_id=_uid(),
    )
    complete_observation_id = _uid()
    first = _complete_publication(
        store,
        publication_effect_checkpoint_id=CHECKPOINT_ID_3,
        publication_id=PUBLICATION_ID,
        effect_generation=1,
        forge_observation_id=complete_observation_id,
        observed_external_revision="a" * 40,
    )
    second = _complete_publication(
        store,
        publication_effect_checkpoint_id=CHECKPOINT_ID_3,
        publication_id=PUBLICATION_ID,
        effect_generation=1,
        forge_observation_id=complete_observation_id,
        observed_external_revision="a" * 40,
    )
    assert second == first


def test_complete_publication_effect_replay_after_active_with_fresh_checkpoint_id(
    store: RunStore,
) -> None:
    _plan_effect(store)
    _record_search(
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
    _checkpoint(
        store,
        publication_effect_checkpoint_id=CHECKPOINT_ID_2,
        publication_id=PUBLICATION_ID,
        effect_generation=1,
        suboperation_kind="BASE_READ_POST",
        status="OBSERVED_SATISFIED",
        forge_observation_id=_uid(),
    )
    complete_observation_id = _uid()
    first = _complete_publication(
        store,
        publication_effect_checkpoint_id=CHECKPOINT_ID_3,
        publication_id=PUBLICATION_ID,
        effect_generation=1,
        forge_observation_id=complete_observation_id,
        observed_external_revision="a" * 40,
    )
    second = _complete_publication(
        store,
        publication_effect_checkpoint_id=CHECKPOINT_ID_4,
        publication_id=PUBLICATION_ID,
        effect_generation=1,
        forge_observation_id=complete_observation_id,
        observed_external_revision="a" * 40,
    )

    assert second == first
    checkpoints = store.list_publication_effect_checkpoints(PUBLICATION_ID, 1)
    assert [c.suboperation_kind for c in checkpoints].count("COMPLETE") == 1


def test_stale_complete_checkpoint_is_audit_only(store: RunStore) -> None:
    _plan_effect(store)
    _record_search(
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
    )
    _checkpoint(
        store,
        publication_effect_checkpoint_id=CHECKPOINT_ID_2,
        suboperation_kind="BASE_READ_POST",
        status="OBSERVED_SATISFIED",
        forge_observation_id=_uid(),
    )
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
    checkpoint = _complete_publication(
        store,
        publication_effect_checkpoint_id=CHECKPOINT_ID_3,
        publication_id=PUBLICATION_ID,
        effect_generation=1,
        forge_observation_id=_uid(),
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
    record, outcome = _record_search(
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
        members=members,
    )
    assert outcome.outcome == "MULTIPLE_LIVE"
    assert outcome.retained_live_external_id == "10"
    assert record.live_cardinality == "MULTIPLE"
    publication = store.get_publication(PUBLICATION_ID)
    assert publication is not None
    assert publication.state == "BRANCH_OBSERVED"  # no Change Request linkage at MULTIPLE-live


def test_zero_live_no_terminal_does_not_mutate(store: RunStore) -> None:
    _plan_effect(store)
    _record, outcome = _record_search(
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
        members=(),
    )
    assert outcome.outcome == "ZERO_LIVE_NO_TERMINAL"
    publication = store.get_publication(PUBLICATION_ID)
    assert publication is not None
    assert publication.state == "BRANCH_OBSERVED"


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
    record, outcome = _record_search(
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
    with pytest.raises(RunStoreError, match="cannot replan"):
        _replan_effect(store)


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
    _record, outcome = _record_search(
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
    record, outcome = _record_search(
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
    _record_search(
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
        members=(_member(change_request_external_id="1"),),
        fresh_exact_object_confirmed=True,
    )
    _record_search(
        store,
        change_request_search_result_id="88888888-8888-4888-8888-888888888889",
        forge_observation_id="99999999-9999-4999-8999-999999999998",
        publication_effect_checkpoint_id=CHECKPOINT_ID_2,
        publication_id=PUBLICATION_ID,
        effect_generation=1,
        project_id=PROJECT_ID,
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
    _record_search(
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

    with pytest.raises(RunStoreError, match="exact action/CAS result"):
        store.complete_terminal_duplicate_cleanup_action(
            terminal_duplicate_cleanup_action_id=ACTION_ID,
            outcome="CLOSED",
            forge_observation_id=FORGE_OBS_ID,
        )

    action1_done, reservation_after_1 = _complete_cleanup_action(
        store,
        terminal_duplicate_cleanup_action_id=ACTION_ID,
        outcome="CLOSED",
        forge_observation_id=_uid(),
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

    _action2_done, reservation_after_2 = _complete_cleanup_action(
        store,
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
    _record_search(
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
        _complete_cleanup_action(
            store,
            terminal_duplicate_cleanup_action_id=ACTION_ID,
            outcome="MARKER_DETACHED",
            forge_observation_id=FORGE_OBS_ID,
        )


def test_complete_terminal_duplicate_cleanup_action_replay_conflict_raises(
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
    _record_search(
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
        members=members,
        terminal_publication_effect_checkpoint_id=CHECKPOINT_ID_2,
        terminal_duplicate_cleanup_reservation_id=RESERVATION_ID,
    )
    store.record_terminal_duplicate_cleanup_action(
        terminal_duplicate_cleanup_action_id=ACTION_ID,
        terminal_duplicate_cleanup_reservation_id=RESERVATION_ID,
        outbox_id=ACTION_OUTBOX_ID,
    )
    _complete_cleanup_action(
        store,
        terminal_duplicate_cleanup_action_id=ACTION_ID,
        outcome="CLOSED",
        forge_observation_id=FORGE_OBS_ID,
    )
    with pytest.raises(IdempotencyConflictError):
        _complete_cleanup_action(
            store,
            terminal_duplicate_cleanup_action_id=ACTION_ID,
            outcome="CLOSED",
            forge_observation_id="99999999-9999-4999-8999-999999999998",
        )


def _create_reconcile_activity(
    store: RunStore, activity_id: str = ACTIVITY_ID, activity_ordinal: int = 1
) -> None:
    store.create_activity(
        activity_id=activity_id,
        run_id=RUN_ID,
        activity_ordinal=activity_ordinal,
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
    _plan_effect(store)
    _create_reconcile_activity(store, activity_id=RECONCILE_ACTIVITY_ID, activity_ordinal=2)
    fact, boundary = store.record_publication_ownership_conflict(
        reconciliation_fact_id=RECONCILIATION_FACT_ID,
        activity_id=RECONCILE_ACTIVITY_ID,
        run_id=RUN_ID,
        created_transition_sequence=1,
        publication_id=PUBLICATION_ID,
        publication_effect_generation=1,
        ownership_evidence_digest=request_digest({"evidence": "conflict"}),
        human_boundary_id=HUMAN_BOUNDARY_ID,
        resume_state="PUBLISHING",
        minimum_request="A foreign account owns this deterministic ref/marker.",
        ownership_project_id=PROJECT_ID,
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
    assert store.is_change_request_excluded_from_legacy_engine(
        deterministic_ref=deterministic_publication_ref(RUN_ID)
    )
    assert is_change_request_excluded_from_legacy_database(
        store.state_root,
        deterministic_ref=deterministic_publication_ref(RUN_ID),
    )


def test_legacy_exclusion_false_for_unknown_ref(store: RunStore) -> None:
    _plan_effect(store)
    assert not store.is_change_request_excluded_from_legacy_engine(
        deterministic_ref="refs/heads/orcest/run/unrelated"
    )


def test_legacy_exclusion_snapshot_is_repository_scoped(store: RunStore) -> None:
    _plan_effect(store)
    store.conn.execute(
        "UPDATE publications SET change_request_external_id = '776' WHERE publication_id = ?",
        (PUBLICATION_ID,),
    )
    owned_ref = deterministic_publication_ref(RUN_ID)

    matching = load_legacy_change_request_exclusion_snapshot(
        store.state_root,
        repository_locator="TEST-ORG/TEST-REPO",
    )
    unrelated = load_legacy_change_request_exclusion_snapshot(
        store.state_root,
        repository_locator="other-org/other-repo",
    )

    assert matching.excludes(deterministic_ref=owned_ref)
    assert matching.excludes(change_request_external_id="776")
    assert not unrelated.excludes(deterministic_ref=owned_ref)
    assert not unrelated.excludes(change_request_external_id="776")
    assert store.is_change_request_excluded_from_legacy_engine(
        deterministic_ref=owned_ref,
        repository_locator="test-org/test-repo",
    )
    assert not store.is_change_request_excluded_from_legacy_engine(
        deterministic_ref=owned_ref,
        repository_locator="other-org/other-repo",
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
