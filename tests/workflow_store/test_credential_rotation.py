"""Idempotent ATTEMPT_ROTATION Credential Rotation Request/Receipt operations."""

from __future__ import annotations

import dataclasses
import uuid
from pathlib import Path

import pytest

from orcest.workflow_contract.v1.digest import capability_public_key_digest, request_digest
from orcest.workflow_store import (
    AttemptOfferInput,
    CasMismatchError,
    RunStore,
    activity_offer_protocol,
)
from orcest.workflow_store.v1.credential_rotation import (
    CredentialRotationReplayConflictError,
    apply_credential_rotation,
)
from orcest.workflow_store.v1.fs import ControlLayout, QuotaConfig, StorageLock
from orcest.workflow_store.v1.secret_provision import provision_or_adopt_secret
from orcest.workflow_store.v1.secrets import SecretStore

pytestmark = pytest.mark.unit

RUN_ID = "11111111-1111-4111-8111-111111111111"
ACTIVITY_ID = "22222222-2222-4222-8222-222222222222"
ATTEMPT_ID = "33333333-3333-4333-8333-333333333333"
OUTBOX_ID = "44444444-4444-4444-8444-444444444444"
KEY_ID = "77777777-7777-7777-7777-777777777777"
AUTHZ_DIGEST = "sha256:" + "a" * 64
FUTURE_MS = 4_102_444_800_000
PROVIDER_ACCOUNT = "account-a"
INSTALLATION = "installation-1"


def _digest(value: object) -> str:
    return request_digest(value)


def _initialize_mode(store: RunStore) -> None:
    result = store.apply_controller_mode_operation(
        controller_mode_operation_id="aaaaaaaa-1111-4111-9111-aaaaaaaaaaaa",
        operation_kind="INITIALIZE",
        expected_mode_revision=0,
        expected_mode=None,
        requested_mode="MAINTENANCE",
        authenticated_principal_id="controller-bootstrap",
        authorization_context_digest=AUTHZ_DIGEST,
    )
    assert result.status == "SUCCEEDED"


def _select_capability_key(store: RunStore) -> None:
    public_key = bytes([9]) * 32
    result = store.apply_capability_key_operation(
        capability_key_operation_id="bbbbbbbb-2222-4222-9222-bbbbbbbbbbbb",
        kind="REGISTER",
        expected_registry_revision=0,
        expected_issuance_key_id=None,
        target_capability_signing_key_id=KEY_ID,
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
        capability_key_operation_id="cccccccc-3333-4333-9333-cccccccccccc",
        kind="SELECT",
        expected_registry_revision=1,
        expected_issuance_key_id=None,
        target_capability_signing_key_id=KEY_ID,
        authenticated_principal_id="key-operator",
        authorization_context_digest=AUTHZ_DIGEST,
    )
    assert result.status == "SUCCEEDED"


@pytest.fixture
def secret_store(tmp_path: Path) -> SecretStore:
    layout = ControlLayout(root=tmp_path / "control")
    layout.initialize()
    quota = QuotaConfig(
        min_free_bytes=0,
        max_object_bytes=1024 * 1024,
        max_store_bytes=8 * 1024 * 1024,
        max_objects=1024,
    )
    lock = StorageLock(layout.storage_lock_path)
    return SecretStore(layout, quota=quota, lock=lock)


@pytest.fixture
def store(tmp_path: Path) -> RunStore:
    with RunStore(tmp_path / "db", verify_local_filesystem=False) as store:
        _initialize_mode(store)
        _select_capability_key(store)
        store.apply_controller_mode_operation(
            controller_mode_operation_id="dddddddd-4444-4444-9444-dddddddddddd",
            operation_kind="SET_MODE",
            expected_mode_revision=1,
            expected_mode="MAINTENANCE",
            requested_mode="RUNNING",
            authenticated_principal_id="operator",
            authorization_context_digest=AUTHZ_DIGEST,
        )
        store.create_run(
            run_id=RUN_ID,
            project_id="project-a",
            work_item_key="work-1",
            state="ADMITTED",
            specification_generation=1,
        )
        store.create_activity(
            activity_id=ACTIVITY_ID,
            run_id=RUN_ID,
            activity_ordinal=1,
            specification_generation=1,
            policy_hash="sha256:" + "0" * 64,
            kind="BUILD",
            execution_class="WORKER",
            state="READY",
            created_transition_sequence=1,
            semantic_input={},
            semantic_input_digest="sha256:" + "1" * 64,
            idempotency_key="sha256:" + "2" * 64,
            attempt=AttemptOfferInput(
                attempt_id=ATTEMPT_ID,
                generation=1,
                protocol_version=activity_offer_protocol(),
                worker_profile="codex",
                offered_at_ms=FUTURE_MS,
                claim_timeout_ms=300_000,
                execution_profile_id="codex-default",
                provider="codex",
                model="gpt-5.3-codex",
                provider_account_ref=PROVIDER_ACCOUNT,
                provider_family="openai",
                model_family="codex",
                classification_revision="class-rev-1",
            ),
            outbox_id=OUTBOX_ID,
        )
        yield store


def _claim(store: RunStore, secret_id: str, **overrides):
    launch_claims = {
        "protocol": "orcest.launch-capability/1",
        "jti": "66666666-6666-4666-8666-666666666666",
        "attempt_id": ATTEMPT_ID,
        "activity_id": ACTIVITY_ID,
        "generation": 1,
        "worker_session_id": "session-1",
        "launch_nonce_id": "55555555-5555-4555-8555-555555555555",
        "runner_principal_id": "runner-1",
        "runner_registration_revision": 7,
        "issued_at_ms": FUTURE_MS + 1,
        "execution_deadline_ms": FUTURE_MS + 200_000,
        "audience": f"/api/v1/attempts/{ATTEMPT_ID}/launch-attestations",
        "capability_signing_key_id": KEY_ID,
        "signature_algorithm": "ED25519",
    }
    kwargs = dict(
        attempt_claim_id="99999999-9999-4999-8999-999999999999",
        attempt_id=ATTEMPT_ID,
        activity_id=ACTIVITY_ID,
        generation=1,
        offer_outbox_id=OUTBOX_ID,
        worker_id="worker-1",
        worker_session_id="session-1",
        worker_profile="codex",
        worker_build_revision="git-sha",
        request_digest=_digest({"claim": 1}),
        execution_deadline_ms=FUTURE_MS + 200_000,
        attempt_capability_jti="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        attempt_capability_digest=_digest({"attempt-cap": 1}),
        source_access_kind="SCOPED_CREDENTIAL",
        source_access_descriptor={"clone_url": "https://example.test/repo.git"},
        source_access_descriptor_digest=_digest({"clone_url": "https://example.test/repo.git"}),
        response_contract_digest=_digest({"claim-response": 1}),
        source_read_secret_ref="secret:source:7",
        provider_secret_ref=secret_id,
        launch_nonce_id="55555555-5555-4555-8555-555555555555",
        launch_capability_jti="66666666-6666-4666-8666-666666666666",
        launch_capability_claims=launch_claims,
    )
    kwargs.update(overrides)
    return store.claim_attempt(**kwargs)


def _accept_launch(store: RunStore, claim):
    return store.accept_launch_attestation(
        launch_attestation_id="12121212-1212-4121-8121-121212121212",
        attempt_id=ATTEMPT_ID,
        activity_id=ACTIVITY_ID,
        attempt_generation=1,
        worker_id="worker-1",
        worker_session_id="session-1",
        pool_manager_id="pool-1",
        runner_principal_id="runner-1",
        runner_image_digest="sha256:" + "3" * 64,
        runner_registration_revision=7,
        launch_nonce_id=claim.claim.launch_nonce_id,
        launch_capability_digest=claim.claim.launch_capability_digest,
        launch_capability_signing_key_id=claim.claim.launch_capability_signing_key_id,
        launch_capability_signature_algorithm=claim.claim.launch_capability_signature_algorithm,
        workspace_instance_id="13131313-1313-4131-8131-131313131313",
        context_instance_id="14141414-1414-4141-8141-141414141414",
        invocation_instance_id="15151515-1515-4151-8151-151515151515",
        workspace_parent_id=None,
        context_parent_id=None,
        invocation_parent_id=None,
        fresh_workspace=True,
        fresh_context=True,
        fresh_invocation=True,
        prepared_at_ms=FUTURE_MS + 10,
        attested_at_ms=FUTURE_MS + 20,
        runner_signing_key_id="runner-key-1",
        runner_signature_algorithm="ED25519",
        signature="base64-signature",
        attestation_digest=_digest({"attestation": 1}),
        response_contract_digest=_digest({"launch-response": 1}),
        provider_material_descriptor={
            "provider": "codex",
            "model": "gpt-5.3-codex",
            "provider_account_ref": PROVIDER_ACCOUNT,
            "secret_id": claim.claim.provider_secret_ref,
            "version": 1,
        },
    )


def _provision_initial_secret(store: RunStore, secret_store: SecretStore, secret_id: str) -> None:
    result = provision_or_adopt_secret(
        store,
        secret_store,
        secret_provision_operation_id=str(uuid.uuid4()),
        mode="PROVISION",
        secret_id=secret_id,
        expected_prior_version=None,
        purpose="FORGE_API",
        owner_scope_kind="FORGE_INSTALLATION",
        owner_scope_id=INSTALLATION,
        authenticated_principal_id="operator-1",
        authorization_context_digest=AUTHZ_DIGEST,
        secret_bytes=b"v1-bytes",
        provider_account_ref=INSTALLATION,
    )
    assert result.state == "COMPLETED"
    assert result.new_version == 1


@pytest.fixture
def rotation_context(store: RunStore, secret_store: SecretStore):
    secret_id = str(uuid.uuid4())
    _provision_initial_secret(store, secret_store, secret_id)
    claim = _claim(store, secret_id)
    _accept_launch(store, claim)
    return secret_id


def _rotate(store: RunStore, secret_store: SecretStore, secret_id: str, **overrides):
    kwargs = dict(
        credential_rotation_request_id=str(uuid.uuid4()),
        attempt_id=ATTEMPT_ID,
        activity_id=ACTIVITY_ID,
        attempt_generation=1,
        worker_id="worker-1",
        worker_session_id="session-1",
        attempt_capability_digest=_digest({"attempt-cap": 1}),
        launch_attestation_id="12121212-1212-4121-8121-121212121212",
        secret_id=secret_id,
        expected_prior_version=1,
        secret_bytes=b"v2-bytes",
        provider_account_ref=PROVIDER_ACCOUNT,
    )
    kwargs.update(overrides)
    return apply_credential_rotation(store, secret_store, **kwargs)


def test_applied_rotation_installs_version_receipt_and_cas(
    store: RunStore, secret_store: SecretStore, rotation_context: str
) -> None:
    secret_id = rotation_context

    result = _rotate(store, secret_store, secret_id)

    assert result.disposition == "APPLIED"
    assert result.accepted_version == 2
    assert result.current_version == 2
    assert result.response_http_status == 200
    assert b"v2-bytes" not in result.response_json.encode()
    assert result.credential_rotation_receipt_id is not None

    version = store.get_secret_version(secret_id, 2)
    assert version is not None
    assert version.creation_receipt_id == result.credential_rotation_receipt_id

    receipt = store.get_credential_rotation_receipt(result.credential_rotation_receipt_id)
    assert receipt is not None
    assert receipt.source_kind == "ATTEMPT_ROTATION"
    assert receipt.source_id == result.credential_rotation_request_id
    assert receipt.credential_rotation_request_id == result.credential_rotation_request_id
    assert receipt.attempt_id == ATTEMPT_ID
    assert receipt.launch_attestation_id == "12121212-1212-4121-8121-121212121212"
    assert receipt.new_version == 2

    current = store.get_secret_current_version(secret_id)
    assert current is not None
    assert current.current_version == 2

    assert secret_store.read_value(secret_id, 2) == b"v2-bytes"


def test_applied_replay_returns_identical_result_without_second_version(
    store: RunStore, secret_store: SecretStore, rotation_context: str
) -> None:
    secret_id = rotation_context
    request_id = str(uuid.uuid4())

    first = _rotate(store, secret_store, secret_id, credential_rotation_request_id=request_id)
    replay = _rotate(store, secret_store, secret_id, credential_rotation_request_id=request_id)

    assert replay.replayed is True
    assert first.replayed is False
    assert replay == dataclasses.replace(first, replayed=True)
    assert store.get_secret_version(secret_id, 3) is None
    assert (
        store.conn.execute(
            "SELECT COUNT(*) FROM credential_rotation_requests "
            "WHERE credential_rotation_request_id = ?",
            (request_id,),
        ).fetchone()[0]
        == 1
    )


def test_applied_replay_with_different_bytes_conflicts(
    store: RunStore, secret_store: SecretStore, rotation_context: str
) -> None:
    secret_id = rotation_context
    request_id = str(uuid.uuid4())
    _rotate(store, secret_store, secret_id, credential_rotation_request_id=request_id)

    with pytest.raises(CredentialRotationReplayConflictError):
        _rotate(
            store,
            secret_store,
            secret_id,
            credential_rotation_request_id=request_id,
            secret_bytes=b"different-bytes",
        )


def test_stale_prior_version_yields_cas_lost_with_no_side_effects(
    store: RunStore, secret_store: SecretStore, rotation_context: str
) -> None:
    secret_id = rotation_context

    result = _rotate(store, secret_store, secret_id, expected_prior_version=99)

    assert result.disposition == "CAS_LOST"
    assert result.credential_rotation_receipt_id is None
    assert result.accepted_version is None
    assert result.current_version == 1
    assert result.response_http_status == 409

    current = store.get_secret_current_version(secret_id)
    assert current is not None
    assert current.current_version == 1
    assert store.get_secret_version(secret_id, 2) is None
    assert (
        store.conn.execute("SELECT COUNT(*) FROM credential_rotation_receipts").fetchone()[0]
        == 1  # only the initial MANAGEMENT_PROVISION receipt
    )


def test_cas_lost_replay_matches_and_conflicts_correctly(
    store: RunStore, secret_store: SecretStore, rotation_context: str
) -> None:
    secret_id = rotation_context
    request_id = str(uuid.uuid4())

    first = _rotate(
        store,
        secret_store,
        secret_id,
        credential_rotation_request_id=request_id,
        expected_prior_version=99,
    )
    replay = _rotate(
        store,
        secret_store,
        secret_id,
        credential_rotation_request_id=request_id,
        expected_prior_version=99,
    )
    assert replay == dataclasses.replace(first, replayed=True)
    assert replay.disposition == "CAS_LOST"

    with pytest.raises(CredentialRotationReplayConflictError):
        _rotate(
            store,
            secret_store,
            secret_id,
            credential_rotation_request_id=request_id,
            expected_prior_version=99,
            secret_bytes=b"other-losing-bytes",
        )


def test_rotation_rejects_mismatched_attempt_fence(
    store: RunStore, secret_store: SecretStore, rotation_context: str
) -> None:
    secret_id = rotation_context

    with pytest.raises(CasMismatchError):
        _rotate(store, secret_store, secret_id, provider_account_ref="wrong-account")

    with pytest.raises(CasMismatchError):
        _rotate(
            store,
            secret_store,
            secret_id,
            attempt_capability_digest=_digest({"attempt-cap": "wrong"}),
        )

    with pytest.raises(CasMismatchError):
        _rotate(
            store,
            secret_store,
            secret_id,
            launch_attestation_id="00000000-0000-4000-8000-000000000000",
        )


def test_rotation_rejects_secret_id_not_bound_to_attempt(
    store: RunStore, secret_store: SecretStore, rotation_context: str
) -> None:
    """An Attempt fenced to one secret cannot rotate a different secret_id.

    Even with a correct ``expected_prior_version`` for the other secret,
    the fence must be scoped to ``attempt.provider_secret_ref`` -- not just
    to "any claimed model-backed Attempt" -- mirroring the
    ``provider_secret_ref`` drift check ``accept_launch_attestation``
    performs against the frozen claim.
    """
    bound_secret_id = rotation_context
    other_secret_id = str(uuid.uuid4())
    _provision_initial_secret(store, secret_store, other_secret_id)
    assert other_secret_id != bound_secret_id

    with pytest.raises(CasMismatchError):
        _rotate(store, secret_store, other_secret_id)

    current = store.get_secret_current_version(other_secret_id)
    assert current is not None
    assert current.current_version == 1


def test_rotation_denied_at_or_after_execution_deadline(
    store: RunStore, secret_store: SecretStore, rotation_context: str
) -> None:
    secret_id = rotation_context
    store.conn.execute(
        "UPDATE attempts SET execution_deadline_ms = ? WHERE attempt_id = ?",
        (0, ATTEMPT_ID),
    )

    with pytest.raises(CasMismatchError):
        _rotate(store, secret_store, secret_id)


def test_replay_denied_after_execution_deadline_passes(
    store: RunStore, secret_store: SecretStore, rotation_context: str
) -> None:
    secret_id = rotation_context
    request_id = str(uuid.uuid4())
    _rotate(store, secret_store, secret_id, credential_rotation_request_id=request_id)

    store.conn.execute(
        "UPDATE attempts SET execution_deadline_ms = ? WHERE attempt_id = ?",
        (0, ATTEMPT_ID),
    )

    with pytest.raises(CasMismatchError):
        _rotate(store, secret_store, secret_id, credential_rotation_request_id=request_id)


def test_secret_version_fanout_cursor_is_restartable_and_idempotent(
    store: RunStore, secret_store: SecretStore, rotation_context: str
) -> None:
    secret_id = rotation_context
    result = _rotate(store, secret_store, secret_id)

    # No Wait Condition/Human Boundary leaf exists yet, so real membership is
    # always empty and this Secret Version's fanout is already complete.
    fanout = store.conn.execute(
        "SELECT * FROM secret_version_fanouts WHERE secret_id = ? AND version = 2", (secret_id,)
    ).fetchone()
    assert fanout["member_count"] == 0
    assert fanout["fanout_completed_at_ms"] is not None

    # Exercise the restartable cursor mechanics directly against a synthetic
    # membership, proving crash-resume idempotency independent of that
    # future leaf's real membership query.
    store.conn.execute(
        "UPDATE secret_version_fanouts SET member_count = 2, next_member_ordinal = 0, "
        "fanout_completed_at_ms = NULL WHERE secret_id = ? AND version = 2",
        (secret_id,),
    )
    fake_run_a = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    fake_run_b = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
    store.conn.execute(
        "INSERT INTO secret_version_runs(secret_id, version, run_ordinal, run_id) "
        "VALUES (?, 2, 0, ?), (?, 2, 1, ?)",
        (secret_id, fake_run_a, secret_id, fake_run_b),
    )
    store.conn.commit()

    store.run_secret_version_fanout(secret_id, 2)
    mid = store.conn.execute(
        "SELECT next_member_ordinal, fanout_completed_at_ms FROM secret_version_fanouts "
        "WHERE secret_id = ? AND version = 2",
        (secret_id,),
    ).fetchone()
    assert mid["next_member_ordinal"] == 2
    assert mid["fanout_completed_at_ms"] is not None
    completed_at = mid["fanout_completed_at_ms"]

    # Idempotent replay -- a startup/crash reconciliation sweep calling this
    # again must not re-advance the cursor or move the completion time.
    store.run_secret_version_fanout(secret_id, 2)
    again = store.conn.execute(
        "SELECT next_member_ordinal, fanout_completed_at_ms FROM secret_version_fanouts "
        "WHERE secret_id = ? AND version = 2",
        (secret_id,),
    ).fetchone()
    assert again["next_member_ordinal"] == 2
    assert again["fanout_completed_at_ms"] == completed_at
    assert result.disposition == "APPLIED"
