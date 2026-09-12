"""A first unseen Attempt Result in MAINTENANCE creates no Result Request or
workflow row (operations-and-rollout.md operational-modes table:
"MAINTENANCE ... no first worker mutation", and the required failure-injection
case "first unseen Result arrives in MAINTENANCE").

``RunStore.submit_attempt_result`` enforces this today via the controller
gate's ``first_result_mutation`` permission (see ``_controller_gate_evaluation``
in store.py), raising ``WorkflowGateClosedError`` before any row is touched.
The literal HTTP 503 ``orcest.error/1`` / ``CONTROLLER_MAINTENANCE`` wire body
described alongside this case cannot be exercised end to end yet: there is no
HTTP transport for the attempts/results endpoints (only project registration
and run-commands have HTTP front doors so far). This test pins the store-level
half of the contract so a future HTTP layer has a known-good gate to wrap.

Closes fim.first_unseen_result_arrives_maintenance_including_semantic (see
coverage.py).
"""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest

from orcest.workflow_store import (
    AttemptOfferInput,
    ControlLayout,
    QuotaConfig,
    RunStore,
    StorageLock,
    WorkflowGateClosedError,
    activity_offer_protocol,
)
from orcest.workflow_store.v1.candidates import CandidateObjectStore

pytestmark = pytest.mark.unit

AUTHZ_DIGEST = "sha256:" + "a" * 64
FUTURE_MS = 4_102_444_800_000


def _uid() -> str:
    return str(uuid.uuid4())


@pytest.fixture
def control_root(tmp_path: Path) -> Path:
    return tmp_path / "control"


@pytest.fixture
def candidate_store(control_root: Path) -> CandidateObjectStore:
    layout = ControlLayout(root=control_root)
    layout.initialize()
    return CandidateObjectStore(
        layout,
        quota=QuotaConfig(min_free_bytes=0, max_object_bytes=1024 * 1024),
        lock=StorageLock(layout.storage_lock_path),
    )


@pytest.fixture
def run_store(control_root: Path) -> RunStore:
    with RunStore(control_root, verify_local_filesystem=False) as store:
        result = store.apply_controller_mode_operation(
            controller_mode_operation_id=_uid(),
            operation_kind="INITIALIZE",
            expected_mode_revision=0,
            expected_mode=None,
            requested_mode="MAINTENANCE",
            authenticated_principal_id="bootstrap-service",
            authorization_context_digest=AUTHZ_DIGEST,
        )
        assert result.status == "SUCCEEDED"
        yield store


def test_submit_attempt_result_in_maintenance_creates_no_row(run_store, candidate_store) -> None:
    assert run_store.get_controller_mode().mode == "MAINTENANCE"
    result_request_id = _uid()

    with pytest.raises(WorkflowGateClosedError):
        run_store.submit_attempt_result(
            candidate_store=candidate_store,
            result_request_id=result_request_id,
            attempt_id=_uid(),
            activity_id=_uid(),
            generation=1,
            worker_id="worker-1",
            worker_session_id=_uid(),
            attempt_capability_digest="sha256:" + "2" * 64,
            outcome="SUCCEEDED",
            receipt={"ok": True},
            summary="done",
        )

    row = run_store.conn.execute(
        "SELECT COUNT(*) AS n FROM result_requests WHERE result_request_id = ?",
        (result_request_id,),
    ).fetchone()
    assert row["n"] == 0

    total = run_store.conn.execute("SELECT COUNT(*) AS n FROM result_requests").fetchone()
    assert total["n"] == 0


def test_replay_of_an_already_ledgered_key_still_works_in_maintenance(
    run_store, candidate_store
) -> None:
    """MAINTENANCE permits exact read-only replay of an already-ledgered Result
    Request key; only *first* mutation is fail-closed. Prove the gate is keyed
    on novelty, not blanket-closed for every result_request_id, by moving to
    RUNNING to create one accepted ledger row, then returning to MAINTENANCE
    and replaying the same key."""
    run = _uid()
    activity = _uid()
    attempt = _uid()
    outbox = _uid()
    worker_session = _uid()
    capability_digest = "sha256:" + "2" * 64
    result_request_id = _uid()

    running = run_store.apply_controller_mode_operation(
        controller_mode_operation_id=_uid(),
        operation_kind="SET_MODE",
        expected_mode_revision=1,
        expected_mode="MAINTENANCE",
        requested_mode="RUNNING",
        authenticated_principal_id="mode-operator",
        authorization_context_digest=AUTHZ_DIGEST,
    )
    assert running.status == "SUCCEEDED"

    with run_store.transaction():
        run_store.create_run(
            run_id=run,
            project_id="project-a",
            work_item_key="work-1",
            state="BUILDING",
            specification_generation=1,
        )
    run_store.create_activity(
        activity_id=activity,
        run_id=run,
        activity_ordinal=1,
        specification_generation=1,
        policy_hash="sha256:" + "0" * 64,
        kind="BUILD",
        candidate_id=None,
        execution_class="WORKER",
        state="READY",
        created_transition_sequence=1,
        semantic_input={},
        semantic_input_digest="sha256:" + "1" * 64,
        idempotency_key="sha256:" + "3" * 64,
        attempt=AttemptOfferInput(
            attempt_id=attempt,
            generation=1,
            protocol_version=activity_offer_protocol(),
            worker_profile="codex",
            offered_at_ms=FUTURE_MS,
            claim_timeout_ms=300_000,
        ),
        outbox_id=outbox,
    )
    with run_store.transaction():
        run_store.conn.execute(
            "UPDATE attempts SET state = 'CLAIMED', claimed_worker_id = 'worker-1', "
            "claimed_worker_session_id = ?, claimed_at_ms = ?, execution_deadline_ms = ?, "
            "capability_auth_expires_at_ms = ?, attempt_capability_digest = ? "
            "WHERE attempt_id = ?",
            (
                worker_session,
                FUTURE_MS,
                FUTURE_MS + 300_000,
                FUTURE_MS + 86_400_000,
                capability_digest,
                attempt,
            ),
        )
        run_store.conn.execute(
            "UPDATE activities SET state = 'ACTIVE' WHERE activity_id = ?", (activity,)
        )

    accepted = run_store.submit_attempt_result(
        candidate_store=candidate_store,
        result_request_id=result_request_id,
        attempt_id=attempt,
        activity_id=activity,
        generation=1,
        worker_id="worker-1",
        worker_session_id=worker_session,
        attempt_capability_digest=capability_digest,
        outcome="SUCCEEDED",
        receipt={"ok": True},
        summary="done",
        now_ms=FUTURE_MS + 1,
    )
    assert accepted.request.replayed is False

    back_to_maintenance = run_store.apply_controller_mode_operation(
        controller_mode_operation_id=_uid(),
        operation_kind="SET_MODE",
        expected_mode_revision=running.mode_revision,
        expected_mode="RUNNING",
        requested_mode="MAINTENANCE",
        authenticated_principal_id="mode-operator",
        authorization_context_digest=AUTHZ_DIGEST,
    )
    assert back_to_maintenance.status == "SUCCEEDED"

    replay = run_store.submit_attempt_result(
        candidate_store=candidate_store,
        result_request_id=result_request_id,
        attempt_id=attempt,
        activity_id=activity,
        generation=1,
        worker_id="worker-1",
        worker_session_id=worker_session,
        attempt_capability_digest=capability_digest,
        outcome="SUCCEEDED",
        receipt={"ok": True},
        summary="done",
        now_ms=FUTURE_MS + 2,
    )
    assert replay.request.replayed is True

    total = run_store.conn.execute("SELECT COUNT(*) AS n FROM result_requests").fetchone()
    assert total["n"] == 1
