"""Authenticated Run command acceptance (issue #694).

Exercises ``RunStore.submit_management_command`` for the two closed
``management_command.kind`` values -- ``CANCEL`` and
``RESOLVE_HUMAN_BOUNDARY`` -- against workflow-lifecycle.md "Authenticated
Run commands": global command-id idempotency, the
``expected_last_transition_sequence`` fence, and routing
``RESOLVE_HUMAN_BOUNDARY`` through the same reason-bound Human Resolution
path as :mod:`tests.workflow_store.test_human_boundary`.
"""

from __future__ import annotations

import time
import uuid
from pathlib import Path

import pytest

from orcest.workflow_store.store import (
    CasMismatchError,
    IdempotencyConflictError,
    RunStore,
)

pytestmark = pytest.mark.unit

MISSING_AUTHORITY_CONSEQUENCES = {
    "AUTHORITY_GRANTED": "Resumes the paused publish with the granted authority."
}


def _uid() -> str:
    return str(uuid.uuid4())


def _now_ms() -> int:
    return int(time.time() * 1000)


@pytest.fixture
def store(tmp_path: Path) -> RunStore:
    with RunStore(tmp_path, verify_local_filesystem=False) as store:
        yield store


def _last_transition_sequence(store: RunStore, run_id: str) -> int:
    from orcest.workflow_reducer.ledger import load_view

    view = load_view(store, run_id)
    assert view is not None
    return view.next_transition_sequence - 1


def _create_recovering_run(
    store: RunStore,
    run_id: str,
    *,
    project_id: str = "project-a",
    recovery_origin_state: str = "BUILDING",
) -> None:
    """Directly seed a RECOVERING run (mirrors test_human_boundary.py)."""
    from orcest.workflow_contract.v1.digest import request_digest

    with store.transaction():
        store.create_run(
            run_id=run_id,
            project_id=project_id,
            work_item_key=f"work-{run_id}",
            state="RECOVERING",
            specification_generation=1,
        )
        payload = {"recovery_origin_state": recovery_origin_state}
        store.put_revisioned_object(
            object_kind="run_pointers",
            object_id=run_id,
            expected_revision=0,
            payload_digest=request_digest(payload),
            payload=payload,
        )


def _enter_missing_authority_boundary(store: RunStore, run_id: str) -> str:
    outcome = store.submit_recovery_evidence(
        recovery_evidence_id=_uid(),
        run_id=run_id,
        source_kind="CONTROLLER_OPERATION",
        source_id=_uid(),
        facts={"outcome": "FAILED", "failure_category": "POLICY"},
        exhausted_autonomous=True,
        human_boundary_reason="MISSING_AUTHORITY",
        human_boundary_minimum_request=(
            "Need admin authorization to force-push the protected branch."
        ),
        human_boundary_choice_consequences=MISSING_AUTHORITY_CONSEQUENCES,
        accepted_at_ms=_now_ms(),
    )
    assert outcome.selected_tactic == "ENTER_HUMAN_BOUNDARY"
    assert outcome.human_boundary is not None
    return outcome.human_boundary.human_boundary_id


# -- CANCEL -------------------------------------------------------------


def test_cancel_command_advances_run_and_stores_response(store: RunStore) -> None:
    run_id = _uid()
    _create_recovering_run(store, run_id)
    expected = _last_transition_sequence(store, run_id)
    command_id = _uid()

    result = store.submit_management_command(
        command_id=command_id,
        run_id=run_id,
        kind="CANCEL",
        expected_last_transition_sequence=expected,
        payload={},
        authenticated_principal_id="ops-lead",
        authorization_context_digest="sha256:" + "0" * 64,
    )

    assert result.replayed is False
    assert result.human_resolution_id is None
    assert result.response_http_status == 200
    import json as _json

    body = _json.loads(result.public_response_json())
    assert body["outcome"] == "ACCEPTED"
    assert body["kind"] == "CANCEL"
    assert body["command_id"] == command_id
    assert body["run_id"] == run_id
    assert body["replayed"] is False

    run_row = store.conn.execute("SELECT state FROM runs WHERE run_id = ?", (run_id,)).fetchone()
    assert run_row["state"] == "CANCELLED"


def test_cancel_command_replays_identical_body(store: RunStore) -> None:
    run_id = _uid()
    _create_recovering_run(store, run_id)
    expected = _last_transition_sequence(store, run_id)
    command_id = _uid()
    kwargs = dict(
        command_id=command_id,
        run_id=run_id,
        kind="CANCEL",
        expected_last_transition_sequence=expected,
        payload={},
        authenticated_principal_id="ops-lead",
        authorization_context_digest="sha256:" + "0" * 64,
    )

    first = store.submit_management_command(**kwargs)
    second = store.submit_management_command(**kwargs)

    assert first.replayed is False
    assert second.replayed is True
    assert second.result_transition_sequence == first.result_transition_sequence
    assert second.public_response_json() != first.public_response_json()
    import json as _json

    assert _json.loads(second.public_response_json())["replayed"] is True


def test_cancel_command_id_reuse_with_different_body_conflicts(store: RunStore) -> None:
    run_id = _uid()
    _create_recovering_run(store, run_id)
    expected = _last_transition_sequence(store, run_id)
    command_id = _uid()

    store.submit_management_command(
        command_id=command_id,
        run_id=run_id,
        kind="CANCEL",
        expected_last_transition_sequence=expected,
        payload={},
        authenticated_principal_id="ops-lead",
        authorization_context_digest="sha256:" + "0" * 64,
    )

    with pytest.raises(IdempotencyConflictError):
        store.submit_management_command(
            command_id=command_id,
            run_id=run_id,
            kind="CANCEL",
            expected_last_transition_sequence=expected,
            payload={},
            authenticated_principal_id="someone-else",
            authorization_context_digest="sha256:" + "0" * 64,
        )


def test_cancel_command_stale_transition_sequence_fails_closed(store: RunStore) -> None:
    run_id = _uid()
    _create_recovering_run(store, run_id)
    expected = _last_transition_sequence(store, run_id)

    with pytest.raises(CasMismatchError):
        store.submit_management_command(
            command_id=_uid(),
            run_id=run_id,
            kind="CANCEL",
            expected_last_transition_sequence=expected + 1,
            payload={},
            authenticated_principal_id="ops-lead",
            authorization_context_digest="sha256:" + "0" * 64,
        )

    run_row = store.conn.execute("SELECT state FROM runs WHERE run_id = ?", (run_id,)).fetchone()
    assert run_row["state"] == "RECOVERING"


def test_cancel_command_rejects_nonempty_payload(store: RunStore) -> None:
    run_id = _uid()
    _create_recovering_run(store, run_id)
    expected = _last_transition_sequence(store, run_id)

    with pytest.raises(ValueError):
        store.submit_management_command(
            command_id=_uid(),
            run_id=run_id,
            kind="CANCEL",
            expected_last_transition_sequence=expected,
            payload={"unexpected": "field"},
            authenticated_principal_id="ops-lead",
            authorization_context_digest="sha256:" + "0" * 64,
        )


# -- RESOLVE_HUMAN_BOUNDARY -----------------------------------------------


def test_resolve_human_boundary_command_resumes_run(store: RunStore) -> None:
    run_id = _uid()
    _create_recovering_run(store, run_id, recovery_origin_state="BUILDING")
    boundary_id = _enter_missing_authority_boundary(store, run_id)
    expected = _last_transition_sequence(store, run_id)
    command_id = _uid()

    result = store.submit_management_command(
        command_id=command_id,
        run_id=run_id,
        kind="RESOLVE_HUMAN_BOUNDARY",
        expected_last_transition_sequence=expected,
        payload={
            "human_boundary_id": boundary_id,
            "resolution_kind": "AUTHORITY_GRANTED",
            "resolution": {"granted_authority": "force-push", "scope": "run-scoped"},
        },
        authenticated_principal_id="ops-lead",
        authorization_context_digest="sha256:" + "0" * 64,
    )

    assert result.human_resolution_id is not None
    stored_resolution = store.get_human_resolution(result.human_resolution_id)
    assert stored_resolution is not None
    assert stored_resolution.source_kind == "MANAGEMENT_COMMAND"
    assert stored_resolution.source_id == command_id

    run_row = store.conn.execute(
        "SELECT state, human_boundary_id FROM runs WHERE run_id = ?", (run_id,)
    ).fetchone()
    assert run_row["state"] == "RECOVERING"
    assert run_row["human_boundary_id"] is None

    replay = store.submit_management_command(
        command_id=command_id,
        run_id=run_id,
        kind="RESOLVE_HUMAN_BOUNDARY",
        expected_last_transition_sequence=expected,
        payload={
            "human_boundary_id": boundary_id,
            "resolution_kind": "AUTHORITY_GRANTED",
            "resolution": {"granted_authority": "force-push", "scope": "run-scoped"},
        },
        authenticated_principal_id="ops-lead",
        authorization_context_digest="sha256:" + "0" * 64,
    )
    assert replay.replayed is True
    assert replay.human_resolution_id == result.human_resolution_id


def test_resolve_human_boundary_command_rejects_specification_amended(store: RunStore) -> None:
    run_id = _uid()
    _create_recovering_run(store, run_id, recovery_origin_state="BUILDING")
    boundary_id = _enter_missing_authority_boundary(store, run_id)
    expected = _last_transition_sequence(store, run_id)

    with pytest.raises(ValueError):
        store.submit_management_command(
            command_id=_uid(),
            run_id=run_id,
            kind="RESOLVE_HUMAN_BOUNDARY",
            expected_last_transition_sequence=expected,
            payload={
                "human_boundary_id": boundary_id,
                "resolution_kind": "SPECIFICATION_AMENDED",
                "resolution": {},
            },
            authenticated_principal_id="ops-lead",
            authorization_context_digest="sha256:" + "0" * 64,
        )


def test_resolve_human_boundary_command_requires_current_boundary(store: RunStore) -> None:
    run_id = _uid()
    _create_recovering_run(store, run_id, recovery_origin_state="BUILDING")
    _enter_missing_authority_boundary(store, run_id)
    expected = _last_transition_sequence(store, run_id)

    with pytest.raises(Exception):
        store.submit_management_command(
            command_id=_uid(),
            run_id=run_id,
            kind="RESOLVE_HUMAN_BOUNDARY",
            expected_last_transition_sequence=expected,
            payload={
                "human_boundary_id": _uid(),
                "resolution_kind": "AUTHORITY_GRANTED",
                "resolution": {"granted_authority": "force-push", "scope": "run-scoped"},
            },
            authenticated_principal_id="ops-lead",
            authorization_context_digest="sha256:" + "0" * 64,
        )


# -- denial audit ----------------------------------------------------------


def test_record_management_command_denial_is_durable(store: RunStore) -> None:
    denial = store.record_management_command_denial(
        code="CAPABILITY_DENIED",
        message="principal lacks CANCEL authority for this project",
        http_status=403,
        run_id=_uid(),
        command_id=_uid(),
        kind="CANCEL",
        authenticated_principal_id="intruder",
    )

    fetched = store.list_management_command_denials(run_id=denial.run_id)
    assert [item.denial_id for item in fetched] == [denial.denial_id]
    assert fetched[0].code == "CAPABILITY_DENIED"
    assert fetched[0].http_status == 403
