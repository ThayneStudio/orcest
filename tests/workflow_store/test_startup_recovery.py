"""Controller restart recovery orchestration (issue #695,
persistence-and-recovery.md "Controller restart recovery" / "Redis
reconstruction")."""

from __future__ import annotations

import time
import uuid
from pathlib import Path

import pytest

from orcest.workflow_contract.v1.digest import capability_public_key_digest, request_digest
from orcest.workflow_store import AttemptOfferInput, RunStore, activity_offer_protocol
from orcest.workflow_store.v1.fs import ControlLayout, QuotaConfig, StorageLock
from orcest.workflow_store.v1.offer_projection import dispatch_pending_offers, offer_stream_key
from orcest.workflow_store.v1.secret_provision import reconcile_pending_secret_provision_operation
from orcest.workflow_store.v1.secrets import SecretStore
from orcest.workflow_store.v1.startup_recovery import run_startup_recovery, sweep_due_deadlines

pytestmark = pytest.mark.unit

AUTHZ_DIGEST = "sha256:" + "a" * 64
POLICY_HASH = "sha256:" + "0" * 64
SEMANTIC_DIGEST = "sha256:" + "1" * 64


def _uid() -> str:
    return str(uuid.uuid4())


def _now_ms() -> int:
    return int(time.time() * 1000)


def _select_capability_key(store: RunStore) -> None:
    key_id = _uid()
    public_key = bytes([9]) * 32
    result = store.apply_capability_key_operation(
        capability_key_operation_id=_uid(),
        kind="REGISTER",
        expected_registry_revision=0,
        expected_issuance_key_id=None,
        target_capability_signing_key_id=key_id,
        register_public_verification_key=public_key,
        register_public_key_digest=capability_public_key_digest(public_key),
        register_private_signing_secret_ref="bootstrap:0",
        register_not_before_ms=0,
        private_key_proof_valid=True,
        authenticated_principal_id="key-operator",
        authorization_context_digest=AUTHZ_DIGEST,
    )
    assert result.status == "SUCCEEDED"
    result = store.apply_capability_key_operation(
        capability_key_operation_id=_uid(),
        kind="SELECT",
        expected_registry_revision=1,
        expected_issuance_key_id=None,
        target_capability_signing_key_id=key_id,
        authenticated_principal_id="key-operator",
        authorization_context_digest=AUTHZ_DIGEST,
    )
    assert result.status == "SUCCEEDED"


@pytest.fixture
def control_root(tmp_path: Path) -> Path:
    return tmp_path / "control"


@pytest.fixture
def run_store(control_root: Path) -> RunStore:
    with RunStore(control_root, verify_local_filesystem=False) as store:
        yield store


@pytest.fixture
def secret_store(control_root: Path) -> SecretStore:
    layout = ControlLayout(root=control_root)
    layout.initialize()
    quota = QuotaConfig(
        min_free_bytes=0,
        max_object_bytes=1024 * 1024,
        max_store_bytes=8 * 1024 * 1024,
        max_objects=1024,
    )
    lock = StorageLock(layout.storage_lock_path)
    return SecretStore(layout, quota=quota, lock=lock)


def _offer(
    run_store: RunStore,
    *,
    run_id: str,
    activity_id: str,
    attempt_id: str,
    outbox_id: str,
    offered_at_ms: int,
    worker_profile: str = "codex",
) -> None:
    with run_store.transaction():
        run_store.create_run(
            run_id=run_id,
            project_id="project-a",
            work_item_key=f"work-{run_id}",
            state="ADMITTED",
            specification_generation=1,
        )
    run_store.create_activity(
        activity_id=activity_id,
        run_id=run_id,
        activity_ordinal=1,
        specification_generation=1,
        policy_hash=POLICY_HASH,
        kind="BUILD",
        execution_class="WORKER",
        state="READY",
        created_transition_sequence=1,
        semantic_input={"a": 1},
        semantic_input_digest=SEMANTIC_DIGEST,
        idempotency_key="sha256:" + activity_id.replace("-", "")[:64].ljust(64, "0"),
        attempt=AttemptOfferInput(
            attempt_id=attempt_id,
            generation=1,
            protocol_version=activity_offer_protocol(),
            worker_profile=worker_profile,
            offered_at_ms=offered_at_ms,
            claim_timeout_ms=300_000,
        ),
        outbox_id=outbox_id,
    )


def _create_waiting_run_with_due_timer(run_store: RunStore, run_id: str) -> str:
    with run_store.transaction():
        run_store.create_run(
            run_id=run_id,
            project_id="project-a",
            work_item_key=f"work-{run_id}",
            state="WAITING",
            specification_generation=1,
        )
        payload = {"wait_condition_id": None, "wait_reason": "BACKOFF"}
        run_store.put_revisioned_object(
            object_kind="run_pointers",
            object_id=run_id,
            expected_revision=0,
            payload_digest=request_digest(payload),
            payload=payload,
        )
        wait = run_store.create_wait_condition(
            wait_condition_id=_uid(),
            run_id=run_id,
            reason="BACKOFF",
            resume_state="BUILDING",
            specification_generation=1,
            policy_hash=POLICY_HASH,
            created_from_kind="RECOVERY_EVIDENCE",
            created_from_id=_uid(),
            created_transition_sequence=1,
            not_before_ms=_now_ms() - 1_000,
        )
        payload = {"wait_condition_id": wait.wait_condition_id, "wait_reason": "BACKOFF"}
        run_store.put_revisioned_object(
            object_kind="run_pointers",
            object_id=run_id,
            expected_revision=1,
            payload_digest=request_digest(payload),
            payload=payload,
        )
        return wait.wait_condition_id


def _offer_expired_claim_window(
    run_store: RunStore, *, run_id: str, activity_id: str, attempt_id: str
) -> None:
    _offer(
        run_store,
        run_id=run_id,
        activity_id=activity_id,
        attempt_id=attempt_id,
        outbox_id=_uid(),
        offered_at_ms=_now_ms() - 400_000,
    )


# -- sweep_due_deadlines -----------------------------------------------------


def test_sweep_due_deadlines_wakes_due_wait_and_expires_due_claim(run_store: RunStore) -> None:
    waiting_run_id = _uid()
    wait_condition_id = _create_waiting_run_with_due_timer(run_store, waiting_run_id)

    offer_run_id, activity_id, attempt_id = _uid(), _uid(), _uid()
    _offer_expired_claim_window(
        run_store, run_id=offer_run_id, activity_id=activity_id, attempt_id=attempt_id
    )

    result = sweep_due_deadlines(run_store)

    assert result.woken_wait_condition_ids == (wait_condition_id,)
    assert result.expired_claim_attempt_ids == (attempt_id,)
    assert result.expired_execution_attempt_ids == ()

    waiting_row = run_store.conn.execute(
        "SELECT state FROM runs WHERE run_id = ?", (waiting_run_id,)
    ).fetchone()
    assert waiting_row["state"] == "RECOVERING"
    attempt = run_store.get_attempt(attempt_id)
    assert attempt.state == "EXPIRED"

    # Idempotent: a second sweep over the same durable state fires nothing more.
    again = sweep_due_deadlines(run_store)
    assert again.woken_wait_condition_ids == ()
    assert again.expired_claim_attempt_ids == ()


def test_sweep_due_deadlines_expires_due_execution_deadline(run_store: RunStore) -> None:
    run_id, activity_id, attempt_id = _uid(), _uid(), _uid()
    _offer(
        run_store,
        run_id=run_id,
        activity_id=activity_id,
        attempt_id=attempt_id,
        outbox_id=_uid(),
        offered_at_ms=0,
    )
    with run_store.transaction():
        run_store.conn.execute(
            "UPDATE attempts SET state = 'CLAIMED', claimed_worker_id = ?, "
            "claimed_worker_session_id = ?, claimed_at_ms = 0, execution_deadline_ms = ?, "
            "attempt_capability_digest = ? WHERE attempt_id = ?",
            ("worker-1", _uid(), _now_ms() - 1_000, "sha256:" + "3" * 64, attempt_id),
        )

    result = sweep_due_deadlines(run_store)

    assert result.expired_execution_attempt_ids == (attempt_id,)
    attempt = run_store.get_attempt(attempt_id)
    assert attempt.state == "EXPIRED"
    assert attempt.terminal_reason == "EXECUTION_DEADLINE"


# -- run_startup_recovery -----------------------------------------------------


def test_run_startup_recovery_rebuilds_offers_without_a_synthetic_result(
    run_store: RunStore, secret_store: SecretStore, fake_redis_client
) -> None:
    run_id, activity_id, attempt_id = _uid(), _uid(), _uid()
    _offer(
        run_store,
        run_id=run_id,
        activity_id=activity_id,
        attempt_id=attempt_id,
        outbox_id=_uid(),
        offered_at_ms=_now_ms(),
    )

    report = run_startup_recovery(run_store, fake_redis_client, secret_store)

    assert report.redis_epoch == 1
    assert report.republished_offers == 1
    # Rebuilding is pure republication from durable state: the Attempt
    # remains OFFERED, and no Result Request/Attempt Terminal Fact exists.
    attempt = run_store.get_attempt(attempt_id)
    assert attempt.state == "OFFERED"
    assert (
        run_store.conn.execute("SELECT COUNT(*) c FROM attempt_terminal_facts").fetchone()["c"] == 0
    )
    assert run_store.conn.execute("SELECT COUNT(*) c FROM result_requests").fetchone()["c"] == 0

    entries = fake_redis_client.xrange(offer_stream_key("codex"), count=10)
    assert len(entries) == 1
    _entry_id, fields = entries[0]
    assert fields["redis_epoch"] == "1"
    assert fields["attempt_id"] == attempt_id

    # A second startup recovery pass advances the epoch again and stays safe.
    second = run_startup_recovery(run_store, fake_redis_client, secret_store)
    assert second.redis_epoch == 2


def test_run_startup_recovery_expires_due_deadlines_before_reconstructing(
    run_store: RunStore, secret_store: SecretStore, fake_redis_client
) -> None:
    run_id, activity_id, attempt_id = _uid(), _uid(), _uid()
    _offer_expired_claim_window(
        run_store, run_id=run_id, activity_id=activity_id, attempt_id=attempt_id
    )
    # Simulate the original notification already having gone out during
    # normal operation, before the controller ever restarted.
    dispatch_pending_offers(run_store, fake_redis_client, redis_epoch=1)
    fake_redis_client.client.flushall()

    report = run_startup_recovery(run_store, fake_redis_client, secret_store)

    assert report.deadlines.expired_claim_attempt_ids == (attempt_id,)
    # The just-expired Attempt is never republished as schedulable work.
    assert report.republished_offers == 0
    assert report.dispatched_offers == 0
    assert fake_redis_client.xrange(offer_stream_key("codex"), count=10) == []


def test_run_startup_recovery_resumes_pending_secret_provision_operation(
    run_store: RunStore, secret_store: SecretStore, fake_redis_client
) -> None:
    _select_capability_key(run_store)
    secret_id = _uid()
    op_id = _uid()
    staging = secret_store.stage(b"resume-me")
    accepted = run_store.begin_secret_provision_operation(
        secret_provision_operation_id=op_id,
        mode="PROVISION",
        secret_id=secret_id,
        expected_prior_version=None,
        purpose="FORGE_API",
        owner_scope_kind="FORGE_INSTALLATION",
        owner_scope_id="installation-1",
        provider_account_ref="installation-1",
        authenticated_principal_id="operator-1",
        authorization_context_digest=AUTHZ_DIGEST,
        secret_store_staging_receipt_id=staging.staging_id,
        secret_integrity_attestation_id=staging.attestation_id,
    )
    assert accepted.state == "PENDING"

    report = run_startup_recovery(run_store, fake_redis_client, secret_store)

    assert report.resumed_secret_provision_operation_ids == (op_id,)
    resumed = run_store.get_secret_provision_operation(op_id)
    assert resumed.state == "COMPLETED"

    # Idempotent: reconciling an already-COMPLETED operation is a no-op replay.
    replay = reconcile_pending_secret_provision_operation(run_store, secret_store, op_id)
    assert replay.state == "COMPLETED"


def test_run_startup_recovery_reports_pending_storage_restoration_and_outbox(
    run_store: RunStore, secret_store: SecretStore, fake_redis_client
) -> None:
    op_id = _uid()
    digest = "sha256:" + "c" * 64
    accepted = run_store.begin_storage_restoration_operation(
        operation_id=op_id,
        object_kind="CANDIDATE_ARTIFACT",
        object_id=digest,
        expected_byte_length=10,
        media_kind=None,
        authenticated_principal_id="controller-storage-reconciler",
        authorization_context_digest=AUTHZ_DIGEST,
        staged_object_key="incoming/" + _uid(),
    )
    assert accepted.state == "PENDING"

    run_id, activity_id, attempt_id = _uid(), _uid(), _uid()
    _offer(
        run_store,
        run_id=run_id,
        activity_id=activity_id,
        attempt_id=attempt_id,
        outbox_id=_uid(),
        offered_at_ms=_now_ms(),
    )

    # A HEALTH_PROBE_REQUEST-sourced Outbox row (this leaf never dispatches
    # it -- that belongs to the Health Probe subsystem's own delivery path)
    # so the generic multi-kind reconciliation report has something to show.
    run_store.conn.execute(
        "INSERT INTO outbox(outbox_id, source_kind, source_id, destination, protocol_version, "
        "payload_digest, payload_json, next_delivery_at_ms, state, delivery_count, created_at_ms) "
        "VALUES (?, 'HEALTH_PROBE_REQUEST', ?, 'FORGE', '1', ?, '{}', 0, 'PENDING', 0, 0)",
        (_uid(), _uid(), request_digest({"a": 1})),
    )

    report = run_startup_recovery(run_store, fake_redis_client, secret_store)

    assert report.pending_storage_restoration_operation_ids == (op_id,)
    assert "HEALTH_PROBE_REQUEST" in report.pending_outbox_ids_by_source_kind
    assert len(report.pending_outbox_ids_by_source_kind["HEALTH_PROBE_REQUEST"]) == 1
    # ACTIVITY-sourced rows are drained by this same pass, so they never
    # linger in the reconciliation report.
    assert "ACTIVITY" not in report.pending_outbox_ids_by_source_kind
