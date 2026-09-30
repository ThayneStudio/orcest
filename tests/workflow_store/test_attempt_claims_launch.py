from __future__ import annotations

from pathlib import Path

import pytest

from orcest.workflow_contract.v1.digest import capability_public_key_digest, request_digest
from orcest.workflow_store import (
    AttemptOfferInput,
    CasMismatchError,
    IdempotencyConflictError,
    RunStore,
    WorkflowGateClosedError,
    activity_offer_protocol,
)

pytestmark = pytest.mark.unit

RUN_ID = "11111111-1111-4111-8111-111111111111"
ACTIVITY_ID = "22222222-2222-4222-8222-222222222222"
ATTEMPT_ID = "33333333-3333-4333-8333-333333333333"
OUTBOX_ID = "44444444-4444-4444-8444-444444444444"
KEY_ID = "77777777-7777-7777-7777-777777777777"
KEY_ID_2 = "88888888-8888-8888-8888-888888888888"
AUTHZ_DIGEST = "sha256:" + "a" * 64
FUTURE_MS = 4_102_444_800_000


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


def _public_key(seed: int) -> bytes:
    return bytes([seed]) * 32


def _register_key(
    store: RunStore,
    *,
    key_id: str = KEY_ID,
    operation_id: str = "bbbbbbbb-2222-4222-9222-bbbbbbbbbbbb",
    expected_revision: int = 0,
    expected_key: str | None = None,
) -> None:
    public_key = _public_key(1 if key_id == KEY_ID else 2)
    result = store.apply_capability_key_operation(
        capability_key_operation_id=operation_id,
        kind="REGISTER",
        expected_registry_revision=expected_revision,
        expected_issuance_key_id=expected_key,
        target_capability_signing_key_id=key_id,
        register_public_verification_key=public_key,
        register_public_key_digest=capability_public_key_digest(public_key),
        register_private_signing_secret_ref=f"secret:{key_id}:1",
        register_not_before_ms=0,
        private_key_proof_valid=True,
        authenticated_principal_id="key-operator",
        authorization_context_digest=AUTHZ_DIGEST,
    )
    assert result.status == "SUCCEEDED"


def _select_key(store: RunStore, *, key_id: str = KEY_ID, revision: int = 1) -> None:
    result = store.apply_capability_key_operation(
        capability_key_operation_id="cccccccc-3333-4333-9333-cccccccccccc",
        kind="SELECT",
        expected_registry_revision=revision,
        expected_issuance_key_id=None,
        target_capability_signing_key_id=key_id,
        authenticated_principal_id="key-operator",
        authorization_context_digest=AUTHZ_DIGEST,
    )
    assert result.status == "SUCCEEDED"


@pytest.fixture
def store(tmp_path: Path) -> RunStore:
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        _initialize_mode(store)
        _register_key(store)
        _select_key(store)
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
                provider_account_ref="account-a",
                provider_family="openai",
                model_family="codex",
                classification_revision="class-rev-1",
            ),
            outbox_id=OUTBOX_ID,
        )
        yield store


def _claim_kwargs(**overrides):
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
        request_digest=_digest({"claim": 1, "redis_epoch": 8}),
        execution_deadline_ms=FUTURE_MS + 200_000,
        attempt_capability_jti="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        attempt_capability_digest=_digest({"attempt-cap": 1}),
        source_access_kind="SCOPED_CREDENTIAL",
        source_access_descriptor={"clone_url": "https://example.test/repo.git"},
        source_access_descriptor_digest=_digest({"clone_url": "https://example.test/repo.git"}),
        response_contract_digest=_digest({"claim-response": 1}),
        source_read_secret_ref="secret:source:7",
        provider_secret_ref="secret:provider:12",
        launch_nonce_id="55555555-5555-4555-8555-555555555555",
        launch_capability_jti="66666666-6666-4666-8666-666666666666",
        launch_capability_claims=launch_claims,
    )
    kwargs.update(overrides)
    return kwargs


def _claim(store: RunStore, **overrides):
    return store.claim_attempt(**_claim_kwargs(**overrides))


def _launch_kwargs(**overrides):
    claim = overrides.pop("claim", None)
    if claim is None:
        raise AssertionError("claim is required")
    kwargs = dict(
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
            "provider_account_ref": "account-a",
            "secret_id": "provider-secret",
            "version": 12,
        },
    )
    kwargs.update(overrides)
    return kwargs


def test_claim_replay_returns_same_claim_and_no_second_authority_grant(store: RunStore) -> None:
    first = _claim(store)
    replay = _claim(store)

    assert replay.claim == first.claim
    assert replay.attempt.attempt_claim_id == first.claim.attempt_claim_id
    assert replay.can_rematerialize_source is True
    assert replay.can_rematerialize_launch_capability is True
    assert store.conn.execute("SELECT COUNT(*) FROM attempt_claims").fetchone()[0] == 1
    assert store.get_attempt(ATTEMPT_ID).provider == "codex"
    assert store.get_attempt(ATTEMPT_ID).provider_account_ref == "account-a"


def test_claim_rejects_wrong_session_and_stale_generation(store: RunStore) -> None:
    _claim(store)

    with pytest.raises(IdempotencyConflictError):
        _claim(store, worker_session_id="session-2")
    with pytest.raises(IdempotencyConflictError):
        _claim(store, worker_id="worker-2")
    with pytest.raises(CasMismatchError):
        store.claim_attempt(
            **_claim_kwargs(
                attempt_claim_id="abababab-abab-4aba-8aba-abababababab",
                generation=2,
            )
        )


def test_launch_acceptance_consumes_one_shot_and_rejects_reused_isolation(
    store: RunStore,
) -> None:
    claim = _claim(store)
    accepted = store.accept_launch_attestation(**_launch_kwargs(claim=claim))
    replay = store.accept_launch_attestation(**_launch_kwargs(claim=claim))

    assert accepted.status == "AVAILABLE"
    assert replay.attestation == accepted.attestation
    assert replay.can_rematerialize_provider is True
    assert store.get_attempt(ATTEMPT_ID).launch_capability_consumed_at_ms is not None
    assert store.conn.execute("SELECT COUNT(*) FROM launch_attestations").fetchone()[0] == 1

    with pytest.raises(CasMismatchError):
        store.accept_launch_attestation(
            **_launch_kwargs(
                claim=claim,
                launch_attestation_id="23232323-2323-4232-8232-232323232323",
                workspace_instance_id="24242424-2424-4242-8242-242424242424",
                context_instance_id="25252525-2525-4252-8252-252525252525",
                invocation_instance_id="26262626-2626-4262-8262-262626262626",
            )
        )


def test_reused_workspace_context_or_invocation_cannot_attest_new_attempt(
    store: RunStore,
) -> None:
    claim = _claim(store)
    store.accept_launch_attestation(**_launch_kwargs(claim=claim))

    with pytest.raises(CasMismatchError):
        store.accept_launch_attestation(
            **_launch_kwargs(
                claim=claim,
                launch_attestation_id="33333333-3333-4333-8333-333333333334",
                context_instance_id="34343434-3434-4343-8343-343434343434",
                invocation_instance_id="35353535-3535-4353-8353-353535353535",
            )
        )


def test_launch_replay_after_terminalization_is_expired_without_provider(
    store: RunStore,
) -> None:
    claim = _claim(store)
    store.accept_launch_attestation(**_launch_kwargs(claim=claim))
    store.conn.execute("UPDATE attempts SET state = 'FAILED' WHERE attempt_id = ?", (ATTEMPT_ID,))

    replay = store.accept_launch_attestation(**_launch_kwargs(claim=claim))

    assert replay.status == "EXPIRED"
    assert replay.can_rematerialize_provider is False


def test_revoked_claim_key_denies_capability_rematerialization_on_replay(
    store: RunStore,
) -> None:
    _claim(store)
    _register_key(
        store,
        key_id=KEY_ID_2,
        operation_id="54545454-5454-4454-8454-545454545454",
        expected_revision=2,
        expected_key=KEY_ID,
    )
    retire = store.apply_capability_key_operation(
        capability_key_operation_id="56565656-5656-4465-8565-565656565656",
        kind="RETIRE",
        expected_registry_revision=3,
        expected_issuance_key_id=KEY_ID,
        target_capability_signing_key_id=KEY_ID,
        replacement_issuance_key_id=KEY_ID_2,
        authenticated_principal_id="key-operator",
        authorization_context_digest=AUTHZ_DIGEST,
    )
    assert retire.status == "SUCCEEDED"
    revoke = store.apply_capability_key_operation(
        capability_key_operation_id="57575757-5757-4475-8575-575757575757",
        kind="REVOKE",
        expected_registry_revision=4,
        expected_issuance_key_id=KEY_ID_2,
        target_capability_signing_key_id=KEY_ID,
        authenticated_principal_id="key-operator",
        authorization_context_digest=AUTHZ_DIGEST,
    )
    assert revoke.status == "SUCCEEDED"

    replay = _claim(store)

    assert replay.can_rematerialize_source is True
    assert replay.can_rematerialize_launch_capability is False
    assert replay.can_rematerialize_attempt_capability is False


def test_revoked_launch_capability_key_denies_even_exact_replay(store: RunStore) -> None:
    claim = _claim(store)
    store.accept_launch_attestation(**_launch_kwargs(claim=claim))
    _register_key(
        store,
        key_id=KEY_ID_2,
        operation_id="45454545-4545-4454-8454-454545454545",
        expected_revision=2,
        expected_key=KEY_ID,
    )
    retire = store.apply_capability_key_operation(
        capability_key_operation_id="46464646-4646-4464-8464-464646464646",
        kind="RETIRE",
        expected_registry_revision=3,
        expected_issuance_key_id=KEY_ID,
        target_capability_signing_key_id=KEY_ID,
        replacement_issuance_key_id=KEY_ID_2,
        authenticated_principal_id="key-operator",
        authorization_context_digest=AUTHZ_DIGEST,
    )
    assert retire.status == "SUCCEEDED"
    replay = store.accept_launch_attestation(**_launch_kwargs(claim=claim))
    assert replay.status == "AVAILABLE"

    revoke = store.apply_capability_key_operation(
        capability_key_operation_id="47474747-4747-4474-8474-474747474747",
        kind="REVOKE",
        expected_registry_revision=4,
        expected_issuance_key_id=KEY_ID_2,
        target_capability_signing_key_id=KEY_ID,
        authenticated_principal_id="key-operator",
        authorization_context_digest=AUTHZ_DIGEST,
    )
    assert revoke.status == "SUCCEEDED"
    with pytest.raises(WorkflowGateClosedError):
        store.accept_launch_attestation(**_launch_kwargs(claim=claim))
