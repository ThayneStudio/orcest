"""Read-only offer-gate snapshot combining mode, issuance key, capacity, and
budget evidence (issue #680)."""

from __future__ import annotations

import time
import uuid
from pathlib import Path

import pytest

from orcest.workflow_contract.v1.digest import capability_public_key_digest
from orcest.workflow_store.store import CapacityReportEntryInput, RunStore

pytestmark = pytest.mark.unit

AUTHZ_DIGEST = "sha256:" + "a" * 64


def _uid() -> str:
    return str(uuid.uuid4())


def _now_ms() -> int:
    return int(time.time() * 1000)


@pytest.fixture
def store(tmp_path: Path) -> RunStore:
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        yield store


def _public_key(seed: int) -> bytes:
    return bytes([seed % 256]) * 32


def _initialize_mode(store: RunStore) -> None:
    store.apply_controller_mode_operation(
        controller_mode_operation_id=_uid(),
        operation_kind="INITIALIZE",
        expected_mode_revision=0,
        expected_mode=None,
        requested_mode="MAINTENANCE",
        authenticated_principal_id="mode-operator",
        authorization_context_digest=AUTHZ_DIGEST,
    )


def _set_mode_running(store: RunStore, *, expected_mode_revision: int = 1) -> None:
    store.apply_controller_mode_operation(
        controller_mode_operation_id=_uid(),
        operation_kind="SET_MODE",
        expected_mode_revision=expected_mode_revision,
        expected_mode="MAINTENANCE",
        requested_mode="RUNNING",
        authenticated_principal_id="mode-operator",
        authorization_context_digest=AUTHZ_DIGEST,
    )


def _activate_controller(store: RunStore) -> None:
    key_id = _uid()
    store.apply_capability_key_operation(
        capability_key_operation_id=_uid(),
        kind="REGISTER",
        expected_registry_revision=0,
        expected_issuance_key_id=None,
        target_capability_signing_key_id=key_id,
        register_public_verification_key=_public_key(1),
        register_public_key_digest=capability_public_key_digest(_public_key(1)),
        register_private_signing_secret_ref="bootstrap:0",
        register_not_before_ms=0,
        private_key_proof_valid=True,
        authenticated_principal_id="key-operator",
        authorization_context_digest=AUTHZ_DIGEST,
    )
    store.apply_capability_key_operation(
        capability_key_operation_id=_uid(),
        kind="SELECT",
        expected_registry_revision=1,
        expected_issuance_key_id=None,
        target_capability_signing_key_id=key_id,
        authenticated_principal_id="key-operator",
        authorization_context_digest=AUTHZ_DIGEST,
    )
    _initialize_mode(store)
    _set_mode_running(store)


def test_fresh_controller_is_mode_blocked(store: RunStore) -> None:
    evaluation = store.evaluate_offer_gate()
    assert evaluation.disposition == "MODE_BLOCKED"
    assert evaluation.selected_issuance_key_id is None
    assert evaluation.capacity_available is False
    assert evaluation.budget_available is False


def test_running_mode_without_key_is_issuance_key_unavailable(store: RunStore) -> None:
    _initialize_mode(store)
    _set_mode_running(store)
    evaluation = store.evaluate_offer_gate()
    assert evaluation.disposition == "ISSUANCE_KEY_UNAVAILABLE"


def test_running_mode_with_selected_key_is_offer_allowed(store: RunStore) -> None:
    _activate_controller(store)
    evaluation = store.evaluate_offer_gate()
    assert evaluation.disposition == "OFFER_ALLOWED"
    assert evaluation.selected_issuance_key_id is not None
    assert evaluation.controller_mode == "RUNNING"


def test_capacity_available_requires_both_profile_and_pool_health(store: RunStore) -> None:
    _activate_controller(store)
    now = _now_ms()
    profile_id = "codex"
    pool_id = "default"
    store.submit_capacity_report(
        capacity_report_id=_uid(),
        pool_manager_id="pool-manager-1",
        report_id=_uid(),
        idempotency_key=_uid(),
        report_sequence=1,
        observed_at_ms=now,
        expires_at_ms=now + 60_000,
        configured_max_ttl_ms=300_000,
        entries=[
            CapacityReportEntryInput(
                scope_kind="WORKER_PROFILE",
                scope_id=profile_id,
                capacity_pool_id=pool_id,
                worker_profile=profile_id,
                available_slots=2,
            ),
        ],
        authenticated_principal_id="pool-manager-principal",
        authorization_context_digest=AUTHZ_DIGEST,
    )

    partial = store.evaluate_offer_gate(
        worker_profile_scope_id=profile_id, capacity_pool_scope_id=pool_id
    )
    assert partial.worker_profile_health is not None
    assert partial.capacity_pool_health is None
    assert partial.capacity_available is False

    store.submit_capacity_report(
        capacity_report_id=_uid(),
        pool_manager_id="pool-manager-1",
        report_id=_uid(),
        idempotency_key=_uid(),
        report_sequence=2,
        observed_at_ms=now,
        expires_at_ms=now + 60_000,
        configured_max_ttl_ms=300_000,
        entries=[
            CapacityReportEntryInput(
                scope_kind="CAPACITY_POOL",
                scope_id=pool_id,
                capacity_pool_id=pool_id,
                available_slots=4,
            ),
        ],
        authenticated_principal_id="pool-manager-principal",
        authorization_context_digest=AUTHZ_DIGEST,
    )

    full = store.evaluate_offer_gate(
        worker_profile_scope_id=profile_id, capacity_pool_scope_id=pool_id
    )
    assert full.capacity_available is True
    assert full.disposition == "OFFER_ALLOWED"


def test_budget_available_reflects_latest_report(store: RunStore) -> None:
    _activate_controller(store)
    now = _now_ms()
    project_id = _uid()
    store.submit_budget_report(
        budget_report_id=_uid(),
        project_id=project_id,
        accounting_scope_id="default",
        budget_policy_ref="default",
        budget_reset_window_ref="default",
        window_id="window-1",
        window_start_ms=now - 1_000,
        reset_at_ms=now + 3_600_000,
        source_sequence=1,
        source_revision="rev-1",
        limit_microunits=100,
        consumed_microunits=100,
        authenticated_principal_id="budget-accounting-service",
        authorization_context_digest=AUTHZ_DIGEST,
        max_budget_report_age_ms=600_000,
    )
    exhausted = store.evaluate_offer_gate(project_id=project_id, accounting_scope_id="default")
    assert exhausted.budget_report is not None
    assert exhausted.budget_available is False

    store.submit_budget_report(
        budget_report_id=_uid(),
        project_id=project_id,
        accounting_scope_id="default",
        budget_policy_ref="default",
        budget_reset_window_ref="default",
        window_id="window-1",
        window_start_ms=now - 1_000,
        reset_at_ms=now + 3_600_000,
        source_sequence=2,
        source_revision="rev-2",
        limit_microunits=100,
        consumed_microunits=1,
        authenticated_principal_id="budget-accounting-service",
        authorization_context_digest=AUTHZ_DIGEST,
        max_budget_report_age_ms=600_000,
    )
    available = store.evaluate_offer_gate(project_id=project_id, accounting_scope_id="default")
    assert available.budget_available is True


def test_absent_scopes_are_neutral_not_available(store: RunStore) -> None:
    _activate_controller(store)
    evaluation = store.evaluate_offer_gate()
    assert evaluation.worker_profile_health is None
    assert evaluation.capacity_pool_health is None
    assert evaluation.provider_account_health is None
    assert evaluation.budget_report is None
    assert evaluation.capacity_available is False
    assert evaluation.budget_available is False
    assert evaluation.disposition == "OFFER_ALLOWED"
