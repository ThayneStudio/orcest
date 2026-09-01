from __future__ import annotations

import json
import time
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest

from orcest.workflow_contract.v1.digest import request_digest
from orcest.workflow_reducer.ledger import load_view
from orcest.workflow_store import ForgeObservationInput, RunStore

pytestmark = pytest.mark.unit

RUN_ID = "11111111-1111-4111-8111-111111111111"
SNAPSHOT_ID = "22222222-2222-4222-8222-222222222222"
ADMIT_TRANSITION_ID = "33333333-3333-4333-8333-333333333333"
PROJECTION_ID = "44444444-4444-4444-8444-444444444444"
INSTALL_TRANSITION_ID = "55555555-5555-4555-8555-555555555555"


class _Ids:
    def __init__(self) -> None:
        self.n = 0

    def __call__(self) -> str:
        self.n += 1
        return f"{self.n:08x}-0000-4000-8000-000000000000"


class _Project:
    def __init__(self, project_id: str, forge_instance_id: str, source_read_secret_id: str) -> None:
        self.project_id = project_id
        self.forge_instance_id = forge_instance_id
        self.source_read_secret_id = source_read_secret_id


def _uid() -> str:
    return str(uuid.uuid4())


def _now_ms() -> int:
    return int(time.time() * 1000)


def _seed_project(store: RunStore) -> _Project:
    now = _now_ms()

    def _secret(purpose: str) -> str:
        secret_id = _uid()
        store.conn.execute(
            "INSERT INTO secret_current_versions(secret_id, purpose, owner_scope_kind, "
            "owner_scope_id, provider_account_ref, current_version, last_operation_id, "
            "created_at_ms, updated_at_ms) VALUES (?, ?, 'PROJECT', ?, NULL, 1, ?, ?, ?)",
            (secret_id, purpose, _uid(), _uid(), now, now),
        )
        return secret_id

    forge_api_secret = _secret("FORGE_API")
    source_read_secret = _secret("SOURCE_READ")
    publication_secret = _secret("PUBLICATION")
    forge_instance_id = _uid()
    store.conn.execute(
        "INSERT INTO forge_instances(forge_instance_id, adapter_kind, canonical_origin, "
        "credential_secret_id, registration_provenance_version, created_at_ms) "
        "VALUES (?, 'GITHUB', ?, ?, 1, ?)",
        (forge_instance_id, f"github.com/{_uid()}", forge_api_secret, now),
    )
    project_id = _uid()
    store.conn.execute(
        "INSERT INTO projects(project_id, forge_instance_id, installation_or_account_ref, "
        "repository_external_id, repository_locator, default_ref, trusted_base_policy_ref, "
        "budget_policy_ref, budget_reset_window_ref, source_read_secret_id, "
        "publication_secret_id, registration_source_read_secret_version, "
        "registration_publication_secret_version, registration_revision, "
        "registration_operation_id, work_item_discovery_schedule_id, registration_state) "
        "VALUES (?, ?, 'inst', ?, 'org/repo', 'main', 'base-v1', 'budget-v1', "
        "'window-v1', ?, ?, 1, 1, 1, ?, ?, 'ACTIVE')",
        (
            project_id,
            forge_instance_id,
            _uid(),
            source_read_secret,
            publication_secret,
            _uid(),
            _uid(),
        ),
    )
    store.conn.commit()
    return _Project(project_id, forge_instance_id, source_read_secret)


def _schedule(store: RunStore, project: _Project, kind: str, target_id: str):
    return store.create_forge_observation_schedule(
        forge_observation_schedule_id=_uid(),
        schedule_kind=kind,
        project_id=project.project_id,
        forge_instance_id=project.forge_instance_id,
        target_kind="WORK_ITEM",
        target_id=target_id,
        minimum_interval_ms=1,
        next_due_at_ms=0,
    )


def _complete_one(
    store: RunStore,
    project: _Project,
    schedule_id: str,
    observation: ForgeObservationInput,
) -> str:
    request = store.create_due_forge_observation_request(
        forge_observation_request_id=_uid(),
        forge_observation_schedule_id=schedule_id,
        now_ms=_now_ms(),
        controller_mode="RUNNING",
        controller_mode_revision=1,
        credential_purpose="PROJECT_SOURCE_READ",
        credential_secret_id=project.source_read_secret_id,
        credential_secret_version=1,
        outbox_id=_uid(),
    )
    assert request is not None
    store.record_forge_observation_request_attempt(request.forge_observation_request_id)
    completion = store.complete_forge_observation_request(
        forge_observation_request_id=request.forge_observation_request_id,
        observations=[observation],
    )
    assert len(completion.observation_ids) == 1
    return completion.observation_ids[0]


def _seed_observations(root: Path) -> tuple[str, str]:
    with RunStore(root, verify_local_filesystem=False) as store:
        project = _seed_project(store)
        base_schedule = _schedule(store, project, "BASE_HEAD_POLL", "issue-1")
        base_id = _complete_one(
            store,
            project,
            base_schedule.forge_observation_schedule_id,
            ForgeObservationInput(
                kind="BASE_HEAD",
                external_revision="base-rev-1",
                fact={
                    "base_ref": "refs/heads/main",
                    "base_commit": {"object_format": "sha1", "oid": "a" * 40},
                },
            ),
        )
        work_schedule = _schedule(store, project, "WORK_ITEM_POLL", "issue-1")
        work_id = _complete_one(
            store,
            project,
            work_schedule.forge_observation_schedule_id,
            ForgeObservationInput(
                kind="WORK_ITEM_SNAPSHOT",
                external_revision="work-rev-1",
                fact={"title": "Implement thing", "body": "Pinned body"},
            ),
        )
        return work_id, base_id


def _admit_kwargs(work_id: str) -> dict[str, object]:
    return {
        "run_id": RUN_ID,
        "snapshot_id": SNAPSHOT_ID,
        "work_item_observation_id": work_id,
        "transition_id": ADMIT_TRANSITION_ID,
        "projection_outbox_id": PROJECTION_ID,
        "projection_idempotency_key": f"run-status:{RUN_ID}:admit",
        "normalized_workflow": {
            "profiles": {"implementation": {"prompt": "prompts/implement.md"}},
            "verification": {"default": ["pytest -q"]},
        },
        "normalized_prompt_blobs": [
            {
                "path": "prompts/implement.md",
                "git_blob": "sha1:" + "b" * 40,
                "normalized_bytes": b"Implement from the pinned snapshot.\n",
            }
        ],
        "effective_policy": {
            "server_policy_revision": "policy-1",
            "base_movement_policy": "REBASE_BEFORE_PUBLICATION",
            "claim_timeout_ms": 1000,
        },
        "server_policy_revision": "policy-1",
        "trusted_base_policy_ref": "base-v1",
        "budget_policy_ref": "budget-v1",
        "budget_reset_window_ref": "window-v1",
        "base_movement_policy": "REBASE_BEFORE_PUBLICATION",
    }


def _open(root: Path) -> Iterator[RunStore]:
    with RunStore(root, verify_local_filesystem=False) as store:
        yield store


def test_admission_restarts_through_three_committed_boundaries(tmp_path: Path) -> None:
    work_id, base_id = _seed_observations(tmp_path)

    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        admitted = store.admit_work_item_from_observations(**_admit_kwargs(work_id))
        assert admitted.transition is not None
        assert admitted.transition.admit_base_observation_id == base_id

    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        view = load_view(store, RUN_ID)
        assert view is not None
        assert view.state == "ADMITTED"
        assert view.pending_snapshot_id == SNAPSHOT_ID
        assert view.current_snapshot_id is None
        generation = store.install_pending_snapshot_generation(
            run_id=RUN_ID,
            transition_id=INSTALL_TRANSITION_ID,
        )
        assert generation.specification_generation == 1
        assert generation.snapshot_id == SNAPSHOT_ID

    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        view = load_view(store, RUN_ID)
        assert view is not None
        assert view.state == "ADMITTED"
        assert view.current_snapshot_id == SNAPSHOT_ID
        assert view.pending_snapshot_id is None
        planned = store.plan_initial_activity(run_id=RUN_ID, id_factory=_Ids())
        assert planned.transition.next_state == "PLANNING"
        assert planned.planned_activity_ids
        assert store.conn.execute("SELECT COUNT(*) FROM transitions").fetchone()[0] == 3

    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        replay = store.admit_work_item_from_observations(
            **{
                **_admit_kwargs(work_id),
                "snapshot_id": "66666666-6666-4666-8666-666666666666",
            }
        )
        assert replay.replayed is True
        assert replay.run_id == RUN_ID
        assert store.conn.execute("SELECT COUNT(*) FROM runs").fetchone()[0] == 1


def test_snapshot_capture_retains_ordered_provenance_and_blobs(tmp_path: Path) -> None:
    work_id, base_id = _seed_observations(tmp_path)

    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        store.admit_work_item_from_observations(**_admit_kwargs(work_id))
        snapshot = store.get_work_item_snapshot(SNAPSHOT_ID)
        assert snapshot is not None
        assert snapshot.source_kind == "FORGE_OBSERVATION"
        assert snapshot.source_id == work_id
        assert snapshot.work_item_observation_id == work_id
        assert snapshot.base_observation_id == base_id
        assert snapshot.normalized_prompt_blobs[0]["path"] == "prompts/implement.md"
        assert store.get_workflow_blob(snapshot.normalized_workflow_blob_digest) is not None
        assert store.get_workflow_blob(snapshot.effective_policy_blob_digest) is not None


def test_unrelated_base_observation_does_not_change_admitted_snapshot(tmp_path: Path) -> None:
    work_id, base_id = _seed_observations(tmp_path)

    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        project_id = store.get_forge_observation(work_id).project_id  # type: ignore[union-attr]
        project = store.conn.execute(
            "SELECT * FROM projects WHERE project_id = ?", (project_id,)
        ).fetchone()
        unrelated = _Project(
            project_id,
            project["forge_instance_id"],
            project["source_read_secret_id"],
        )
        other_base_schedule = _schedule(store, unrelated, "BASE_HEAD_POLL", "issue-2")
        _complete_one(
            store,
            unrelated,
            other_base_schedule.forge_observation_schedule_id,
            ForgeObservationInput(
                kind="BASE_HEAD",
                external_revision="base-rev-unrelated",
                fact={
                    "base_ref": "refs/heads/main",
                    "base_commit": {"object_format": "sha1", "oid": "c" * 40},
                },
            ),
        )
        store.admit_work_item_from_observations(**_admit_kwargs(work_id))
        snapshot = store.get_work_item_snapshot(SNAPSHOT_ID)
        assert snapshot is not None
        assert snapshot.base_observation_id == base_id
        assert snapshot.base_commit == {"object_format": "sha1", "oid": "a" * 40}


def test_later_snapshot_installation_plans_mandatory_replan(tmp_path: Path) -> None:
    work_id, _ = _seed_observations(tmp_path)

    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        store.admit_work_item_from_observations(**_admit_kwargs(work_id))
        store.install_pending_snapshot_generation(
            run_id=RUN_ID,
            transition_id=INSTALL_TRANSITION_ID,
        )
        installed = store.get_work_item_snapshot(SNAPSHOT_ID)
        assert installed is not None
        second = store.capture_work_item_snapshot(
            snapshot_id="77777777-7777-4777-8777-777777777777",
            run_id=RUN_ID,
            source_kind="FORGE_OBSERVATION",
            source_id="synthetic-observation-after-plan",
            work_item_observation_id=work_id,
            base_observation_id=installed.base_observation_id,
            project_id=installed.project_id,
            work_item_external_id="issue-1",
            forge_revision="work-rev-2",
            title="Implement thing",
            body="Pinned body changed",
            base_ref="refs/heads/main",
            base_commit={"object_format": "sha1", "oid": "a" * 40},
            workflow_schema_version="1",
            normalized_workflow={"profiles": {"implementation": {"prompt": "p"}}},
            effective_policy={
                "server_policy_revision": "policy-1",
                "base_movement_policy": "REBASE_BEFORE_PUBLICATION",
            },
            server_policy_revision="policy-1",
            trusted_base_policy_ref="base-v1",
            budget_policy_ref="budget-v1",
            budget_reset_window_ref="window-v1",
            base_movement_policy="REBASE_BEFORE_PUBLICATION",
        )
        store._set_run_snapshot_pointers(
            run_id=RUN_ID,
            current_snapshot_id=SNAPSHOT_ID,
            pending_snapshot_id=second.snapshot_id,
            supersede_requested=False,
            supersede_requested_transition_sequence=None,
        )
        stored = store.get_revisioned_object("run_pointers", RUN_ID)
        assert stored is not None
        revision, _, payload_json = stored
        pointers = json.loads(payload_json)
        pointers["pending_snapshot_id"] = second.snapshot_id
        pointers["pending_internal_sequence"] = None
        store.put_revisioned_object(
            object_kind="run_pointers",
            object_id=RUN_ID,
            expected_revision=revision,
            payload_digest=request_digest(pointers),
            payload=pointers,
        )

        generation = store.install_pending_snapshot_generation(
            run_id=RUN_ID,
            transition_id="88888888-8888-4888-8888-888888888888",
        )
        assert generation.specification_generation == 2
        view = load_view(store, RUN_ID)
        assert view is not None
        assert view.state == "REPLANNING"
        assert any(activity.kind == "REPLAN" for activity in view.activities)
