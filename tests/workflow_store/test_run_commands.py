"""Authenticated ``POST /api/v1/runs/{run_id}/commands`` transport/RBAC
(issue #694): schema validation, principal authentication, exact
Project/Run/command/resolution-kind authorization, and the durable denial
audit for every fail-closed rejection.
"""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path

import pytest

from orcest.workflow_contract.v1.protocol_registry import MANAGEMENT_COMMAND_PROTOCOL
from orcest.workflow_store.store import RunStore
from orcest.workflow_store.v1.run_commands import (
    RunCommandPrincipalRecord,
    ServerRunCommandCatalog,
    TransportError,
    handle_run_command,
    submit_run_command,
)
from orcest.workflow_store.v1.run_commands_http import (
    handle_run_command_http,
    match_run_command_path,
)

pytestmark = pytest.mark.unit

PROJECT_A = "project-a"
PROJECT_B = "project-b"
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


def _create_recovering_run(
    store: RunStore,
    run_id: str,
    *,
    project_id: str = PROJECT_A,
    recovery_origin_state: str = "BUILDING",
) -> None:
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
    assert outcome.human_boundary is not None
    return outcome.human_boundary.human_boundary_id


def _last_transition_sequence(store: RunStore, run_id: str) -> int:
    from orcest.workflow_reducer.ledger import load_view

    view = load_view(store, run_id)
    assert view is not None
    return view.next_transition_sequence - 1


def _cancel_body(*, command_id: str, run_id: str, expected: int) -> bytes:
    return json.dumps(
        {
            "protocol": MANAGEMENT_COMMAND_PROTOCOL,
            "command_id": command_id,
            "run_id": run_id,
            "expected_last_transition_sequence": expected,
            "kind": "CANCEL",
            "payload": {},
        }
    ).encode("utf-8")


def _resolve_body(
    *,
    command_id: str,
    run_id: str,
    expected: int,
    boundary_id: str,
    resolution_kind: str,
    resolution: dict,
) -> bytes:
    return json.dumps(
        {
            "protocol": MANAGEMENT_COMMAND_PROTOCOL,
            "command_id": command_id,
            "run_id": run_id,
            "expected_last_transition_sequence": expected,
            "kind": "RESOLVE_HUMAN_BOUNDARY",
            "payload": {
                "human_boundary_id": boundary_id,
                "resolution_kind": resolution_kind,
                "resolution": resolution,
            },
        }
    ).encode("utf-8")


def _catalog(*records: RunCommandPrincipalRecord) -> ServerRunCommandCatalog:
    return ServerRunCommandCatalog(principals={r.principal_id: r for r in records})


# -- path matching ----------------------------------------------------------


def test_match_run_command_path() -> None:
    run_id = _uid()
    assert match_run_command_path(f"/api/v1/runs/{run_id}/commands") == run_id
    assert match_run_command_path(f"/api/v1/runs/{run_id}/other") is None
    assert match_run_command_path("/api/v1/projects/registrations") is None


# -- CANCEL happy path / replay ---------------------------------------------


def test_cancel_command_round_trip_via_http_handler(store: RunStore) -> None:
    run_id = _uid()
    _create_recovering_run(store, run_id)
    expected = _last_transition_sequence(store, run_id)
    command_id = _uid()
    catalog = _catalog(
        RunCommandPrincipalRecord(
            principal_id="ops-lead", authorized_command_kinds=frozenset({"CANCEL"})
        )
    )

    status, headers, payload = handle_run_command_http(
        method="POST",
        path=f"/api/v1/runs/{run_id}/commands",
        headers={},
        body=_cancel_body(command_id=command_id, run_id=run_id, expected=expected),
        principal_id="ops-lead",
        run_store=store,
        catalog=catalog,
    )
    assert status == 200
    assert headers["Content-Type"] == "application/json"
    body = json.loads(payload)
    assert body["outcome"] == "ACCEPTED"
    assert body["replayed"] is False
    assert store.get_run(run_id).state == "CANCELLED"

    status2, _, payload2 = handle_run_command_http(
        method="POST",
        path=f"/api/v1/runs/{run_id}/commands",
        headers={},
        body=_cancel_body(command_id=command_id, run_id=run_id, expected=expected),
        principal_id="ops-lead",
        run_store=store,
        catalog=catalog,
    )
    assert status2 == 200
    assert json.loads(payload2)["replayed"] is True


# -- authentication / authorization -----------------------------------------


def test_unauthenticated_request_is_401_and_audited(store: RunStore) -> None:
    run_id = _uid()
    _create_recovering_run(store, run_id)
    expected = _last_transition_sequence(store, run_id)
    catalog = _catalog()

    with pytest.raises(TransportError) as exc:
        handle_run_command(
            store,
            catalog=catalog,
            raw_body=_cancel_body(command_id=_uid(), run_id=run_id, expected=expected),
            path_run_id=run_id,
            authenticated_principal_id=None,
        )
    assert exc.value.http_status == 401
    denials = store.list_management_command_denials(run_id=run_id)
    assert len(denials) == 1
    assert denials[0].code == "AUTH_INVALID"


def test_unknown_principal_is_401(store: RunStore) -> None:
    run_id = _uid()
    _create_recovering_run(store, run_id)
    expected = _last_transition_sequence(store, run_id)
    catalog = _catalog()

    with pytest.raises(TransportError) as exc:
        submit_run_command(
            store,
            catalog=catalog,
            raw_body=_cancel_body(command_id=_uid(), run_id=run_id, expected=expected),
            path_run_id=run_id,
            authenticated_principal_id="stranger",
        )
    assert exc.value.http_status == 401


def test_principal_without_command_kind_is_403_and_audited(store: RunStore) -> None:
    run_id = _uid()
    _create_recovering_run(store, run_id)
    expected = _last_transition_sequence(store, run_id)
    catalog = _catalog(
        RunCommandPrincipalRecord(principal_id="readonly", authorized_command_kinds=frozenset())
    )

    with pytest.raises(TransportError) as exc:
        handle_run_command(
            store,
            catalog=catalog,
            raw_body=_cancel_body(command_id=_uid(), run_id=run_id, expected=expected),
            path_run_id=run_id,
            authenticated_principal_id="readonly",
        )
    assert exc.value.http_status == 403
    assert exc.value.code == "CAPABILITY_DENIED"
    denials = store.list_management_command_denials(run_id=run_id)
    assert denials[0].code == "CAPABILITY_DENIED"
    assert store.get_run(run_id).state == "RECOVERING"


def test_principal_without_command_kind_cannot_probe_run_existence(store: RunStore) -> None:
    run_id = _uid()
    _create_recovering_run(store, run_id)
    missing_run_id = _uid()
    catalog = _catalog(
        RunCommandPrincipalRecord(principal_id="readonly", authorized_command_kinds=frozenset())
    )

    for candidate_run_id in (run_id, missing_run_id):
        with pytest.raises(TransportError) as exc:
            submit_run_command(
                store,
                catalog=catalog,
                raw_body=_cancel_body(command_id=_uid(), run_id=candidate_run_id, expected=0),
                path_run_id=candidate_run_id,
                authenticated_principal_id="readonly",
            )
        assert exc.value.http_status == 403
        assert exc.value.code == "CAPABILITY_DENIED"


def test_principal_scoped_to_other_project_is_hidden_as_not_found(store: RunStore) -> None:
    run_id = _uid()
    _create_recovering_run(store, run_id, project_id=PROJECT_A)
    expected = _last_transition_sequence(store, run_id)
    catalog = _catalog(
        RunCommandPrincipalRecord(
            principal_id="scoped-op",
            authorized_command_kinds=frozenset({"CANCEL"}),
            authorized_project_ids=frozenset({PROJECT_B}),
        )
    )

    with pytest.raises(TransportError) as exc:
        submit_run_command(
            store,
            catalog=catalog,
            raw_body=_cancel_body(command_id=_uid(), run_id=run_id, expected=expected),
            path_run_id=run_id,
            authenticated_principal_id="scoped-op",
        )
    assert exc.value.http_status == 404
    assert exc.value.code == "RUN_NOT_FOUND"


def test_project_scoped_principal_cannot_probe_run_existence(store: RunStore) -> None:
    out_of_scope_run_id = _uid()
    _create_recovering_run(store, out_of_scope_run_id, project_id=PROJECT_A)
    missing_run_id = _uid()
    catalog = _catalog(
        RunCommandPrincipalRecord(
            principal_id="scoped-op",
            authorized_command_kinds=frozenset({"CANCEL"}),
            authorized_project_ids=frozenset({PROJECT_B}),
        )
    )

    for candidate_run_id in (out_of_scope_run_id, missing_run_id):
        with pytest.raises(TransportError) as exc:
            submit_run_command(
                store,
                catalog=catalog,
                raw_body=_cancel_body(command_id=_uid(), run_id=candidate_run_id, expected=0),
                path_run_id=candidate_run_id,
                authenticated_principal_id="scoped-op",
            )
        assert exc.value.http_status == 404
        assert exc.value.code == "RUN_NOT_FOUND"


# -- schema / fencing failures ------------------------------------------------


def test_unknown_command_kind_is_422_and_audited(store: RunStore) -> None:
    run_id = _uid()
    _create_recovering_run(store, run_id)
    expected = _last_transition_sequence(store, run_id)
    catalog = _catalog(
        RunCommandPrincipalRecord(
            principal_id="ops-lead", authorized_command_kinds=frozenset({"CANCEL"})
        )
    )
    body = json.dumps(
        {
            "protocol": MANAGEMENT_COMMAND_PROTOCOL,
            "command_id": _uid(),
            "run_id": run_id,
            "expected_last_transition_sequence": expected,
            "kind": "DELETE_RUN",
            "payload": {},
        }
    ).encode("utf-8")

    with pytest.raises(TransportError) as exc:
        handle_run_command(
            store,
            catalog=catalog,
            raw_body=body,
            path_run_id=run_id,
            authenticated_principal_id="ops-lead",
        )
    assert exc.value.http_status == 422
    assert exc.value.code == "SCHEMA_INVALID"
    denials = store.list_management_command_denials(run_id=run_id)
    assert denials[0].code == "SCHEMA_INVALID"
    assert store.get_run(run_id).state == "RECOVERING"


def test_run_not_found_is_404(store: RunStore) -> None:
    run_id = _uid()
    catalog = _catalog(
        RunCommandPrincipalRecord(
            principal_id="ops-lead", authorized_command_kinds=frozenset({"CANCEL"})
        )
    )

    with pytest.raises(TransportError) as exc:
        submit_run_command(
            store,
            catalog=catalog,
            raw_body=_cancel_body(command_id=_uid(), run_id=run_id, expected=0),
            path_run_id=run_id,
            authenticated_principal_id="ops-lead",
        )
    assert exc.value.http_status == 404
    assert exc.value.code == "RUN_NOT_FOUND"


def test_run_not_found_is_audited_via_handle_run_command(store: RunStore) -> None:
    run_id = _uid()
    catalog = _catalog(
        RunCommandPrincipalRecord(
            principal_id="ops-lead", authorized_command_kinds=frozenset({"CANCEL"})
        )
    )

    with pytest.raises(TransportError) as exc:
        handle_run_command(
            store,
            catalog=catalog,
            raw_body=_cancel_body(command_id=_uid(), run_id=run_id, expected=0),
            path_run_id=run_id,
            authenticated_principal_id="ops-lead",
        )
    assert exc.value.http_status == 404
    denials = store.list_management_command_denials(run_id=run_id)
    assert len(denials) == 1
    assert denials[0].code == "RUN_NOT_FOUND"


def test_path_and_body_run_id_mismatch_is_422(store: RunStore) -> None:
    run_id = _uid()
    other_run_id = _uid()
    _create_recovering_run(store, run_id)
    expected = _last_transition_sequence(store, run_id)
    catalog = _catalog(
        RunCommandPrincipalRecord(
            principal_id="ops-lead", authorized_command_kinds=frozenset({"CANCEL"})
        )
    )

    with pytest.raises(TransportError) as exc:
        submit_run_command(
            store,
            catalog=catalog,
            raw_body=_cancel_body(command_id=_uid(), run_id=run_id, expected=expected),
            path_run_id=other_run_id,
            authenticated_principal_id="ops-lead",
        )
    assert exc.value.http_status == 422


def test_stale_transition_sequence_is_409(store: RunStore) -> None:
    run_id = _uid()
    _create_recovering_run(store, run_id)
    expected = _last_transition_sequence(store, run_id)
    catalog = _catalog(
        RunCommandPrincipalRecord(
            principal_id="ops-lead", authorized_command_kinds=frozenset({"CANCEL"})
        )
    )

    with pytest.raises(TransportError) as exc:
        submit_run_command(
            store,
            catalog=catalog,
            raw_body=_cancel_body(command_id=_uid(), run_id=run_id, expected=expected + 1),
            path_run_id=run_id,
            authenticated_principal_id="ops-lead",
        )
    assert exc.value.http_status == 409
    assert exc.value.code == "STALE_RUN"


def test_duplicate_command_id_different_body_is_409(store: RunStore) -> None:
    run_id = _uid()
    _create_recovering_run(store, run_id)
    expected = _last_transition_sequence(store, run_id)
    command_id = _uid()
    catalog = _catalog(
        RunCommandPrincipalRecord(
            principal_id="ops-lead", authorized_command_kinds=frozenset({"CANCEL"})
        )
    )

    submit_run_command(
        store,
        catalog=catalog,
        raw_body=_cancel_body(command_id=command_id, run_id=run_id, expected=expected),
        path_run_id=run_id,
        authenticated_principal_id="ops-lead",
    )

    other_run_id = _uid()
    _create_recovering_run(store, other_run_id)
    other_expected = _last_transition_sequence(store, other_run_id)
    with pytest.raises(TransportError) as exc:
        submit_run_command(
            store,
            catalog=catalog,
            raw_body=_cancel_body(
                command_id=command_id, run_id=other_run_id, expected=other_expected
            ),
            path_run_id=other_run_id,
            authenticated_principal_id="ops-lead",
        )
    assert exc.value.http_status == 409
    assert exc.value.code == "IDEMPOTENCY_CONFLICT"


# -- RESOLVE_HUMAN_BOUNDARY ---------------------------------------------------


def test_resolve_human_boundary_round_trip(store: RunStore) -> None:
    run_id = _uid()
    _create_recovering_run(store, run_id, recovery_origin_state="BUILDING")
    boundary_id = _enter_missing_authority_boundary(store, run_id)
    expected = _last_transition_sequence(store, run_id)
    catalog = _catalog(
        RunCommandPrincipalRecord(
            principal_id="ops-lead",
            authorized_command_kinds=frozenset({"RESOLVE_HUMAN_BOUNDARY"}),
            authorized_resolution_kinds=frozenset({"AUTHORITY_GRANTED"}),
        )
    )

    result = submit_run_command(
        store,
        catalog=catalog,
        raw_body=_resolve_body(
            command_id=_uid(),
            run_id=run_id,
            expected=expected,
            boundary_id=boundary_id,
            resolution_kind="AUTHORITY_GRANTED",
            resolution={"granted_authority": "force-push", "scope": "run-scoped"},
        ),
        path_run_id=run_id,
        authenticated_principal_id="ops-lead",
    )
    body = json.loads(result.body_json)
    assert body["outcome"] == "ACCEPTED"
    assert body["human_resolution_id"] is not None
    assert store.get_run(run_id).state == "RECOVERING"


def test_resolve_human_boundary_wrong_resolution_kind_authority_is_403(store: RunStore) -> None:
    run_id = _uid()
    _create_recovering_run(store, run_id, recovery_origin_state="BUILDING")
    boundary_id = _enter_missing_authority_boundary(store, run_id)
    expected = _last_transition_sequence(store, run_id)
    catalog = _catalog(
        RunCommandPrincipalRecord(
            principal_id="ops-lead",
            authorized_command_kinds=frozenset({"RESOLVE_HUMAN_BOUNDARY"}),
            authorized_resolution_kinds=frozenset({"SECURITY_ACTION_AUTHORIZED"}),
        )
    )

    with pytest.raises(TransportError) as exc:
        submit_run_command(
            store,
            catalog=catalog,
            raw_body=_resolve_body(
                command_id=_uid(),
                run_id=run_id,
                expected=expected,
                boundary_id=boundary_id,
                resolution_kind="AUTHORITY_GRANTED",
                resolution={"granted_authority": "force-push", "scope": "run-scoped"},
            ),
            path_run_id=run_id,
            authenticated_principal_id="ops-lead",
        )
    assert exc.value.http_status == 403
