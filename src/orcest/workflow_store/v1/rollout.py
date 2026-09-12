"""Durable staged migration from the legacy engine to Workflow-Control v1.

operations-and-rollout.md Stages 0-5: each stage has a machine-verifiable
entry/exit/rollback checklist. A project, ref, or PR is writable by at most
one engine. Stage 3 fails closed on duplicate publication or stale overwrite.
Stage 5 drains and archives leftover legacy work, keeps historical read-only
tooling, strips raw task credentials from new streams, and waits out the
central-controller backup/restore observation period.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from orcest.workflow_contract.v1 import enums
from orcest.workflow_contract.v1.canonical import canonical_json_text
from orcest.workflow_contract.v1.digest import request_digest, response_digest
from orcest.workflow_contract.v1.identity import require_lowercase_uuid
from orcest.workflow_contract.v1.protocol import validate_envelope
from orcest.workflow_contract.v1.protocol_registry import (
    ROLLOUT_OPERATION_PROTOCOL,
    ROLLOUT_OPERATION_RESULT_PROTOCOL,
)
from orcest.workflow_store.store import (
    CONTROLLER_ID,
    SCHEMA_VERSION,
    CasMismatchError,
    IdempotencyConflictError,
    RunStore,
    _now_ms,
    _require_digest,
    open_read_only,
)
from orcest.workflow_store.v1.isolation import validate_capacity_pool_isolation

__all__ = [
    "DEFAULT_OBSERVATION_PERIOD_MS",
    "ChecklistItemResult",
    "LegacyRolloutControls",
    "RolloutCapacityPoolRecord",
    "RolloutChecklist",
    "RolloutOperationResult",
    "RolloutProjectRecord",
    "RolloutProjection",
    "allow_rollout_project",
    "apply_rollout_operation",
    "archive_legacy_work",
    "engine_write_scope",
    "evaluate_rollout_checklist",
    "get_rollout_projection",
    "load_legacy_rollout_controls",
    "record_rollout_evidence",
    "register_rollout_capacity_pool",
]

DEFAULT_OBSERVATION_PERIOD_MS = 7 * 86_400_000
_AUTHZ = "authorization_context_digest"

_STAGE_ITEMS: dict[int, dict[str, tuple[str, ...]]] = {
    0: {
        "ENTRY": ("schema_current", "writer_lock"),
        "EXIT": (
            "controller_maintenance",
            "issuance_key_selected",
            "projects_registered",
            "no_ordinary_forge_io",
            "no_owned_work",
            "backup_restore_evidence",
        ),
        "ROLLBACK": ("no_v1_publication",),
    },
    1: {
        "ENTRY": ("stage_0_exit", "pools_both_classes"),
        "EXIT": (
            "disjoint_pool_inventory",
            "pel_reaper_isolated",
            "clone_credential_attested",
            "synthetic_canary_evidence",
        ),
        "ROLLBACK": ("no_v1_publication",),
    },
    2: {
        "ENTRY": ("stage_1_exit", "single_pilot_project"),
        "EXIT": ("pilot_publication_disabled", "pilot_legacy_issue_excluded"),
        "ROLLBACK": ("no_v1_publication",),
    },
    3: {
        "ENTRY": ("stage_2_exit",),
        "EXIT": (
            "no_duplicate_publication",
            "no_stale_overwrite",
            "no_ownership_conflict",
        ),
        "ROLLBACK": ("v1_keeps_publications",),
    },
    4: {
        "ENTRY": ("stage_3_exit", "allowlist_exclusive"),
        "EXIT": ("one_engine_per_project", "open_pr_unambiguous"),
        "ROLLBACK": ("v1_keeps_publications",),
    },
    5: {
        "ENTRY": ("stage_4_exit",),
        "EXIT": (
            "legacy_drained",
            "legacy_archived",
            "historical_readonly_retained",
            "raw_credentials_removed",
            "observation_period_elapsed",
            "representative_runs_evidence",
        ),
        "ROLLBACK": ("v1_keeps_publications",),
    },
}


@dataclass(frozen=True, slots=True)
class RolloutProjection:
    controller_id: str
    stage: int
    stage_revision: int
    status: str
    last_operation_id: str | None
    entered_at_ms: int | None
    observation_started_at_ms: int | None
    publication_enabled: bool
    legacy_admissions_frozen: bool
    raw_task_credentials_removed: bool
    historical_readonly_retained: bool


@dataclass(frozen=True, slots=True)
class ChecklistItemResult:
    item_id: str
    passed: bool
    evidence_code: str
    detail: str
    stage: int | None = None
    gate: str | None = None


@dataclass(frozen=True, slots=True)
class RolloutChecklist:
    stage: int
    gate: str
    items: tuple[ChecklistItemResult, ...]

    @property
    def passed(self) -> bool:
        return bool(self.items) and all(item.passed for item in self.items)


@dataclass(frozen=True, slots=True)
class RolloutOperationResult:
    rollout_operation_id: str
    operation_kind: str
    status: str
    response_http_status: int
    response_json: str
    response_digest: str
    committed_at_ms: int
    rejection_code: str | None = None
    stage: int | None = None
    stage_revision: int | None = None
    replayed: bool = False
    checklist: RolloutChecklist | None = None


@dataclass(frozen=True, slots=True)
class RolloutCapacityPoolRecord:
    capacity_pool_id: str
    template_class: str
    template_id: str
    pool_manager_principal_id: str
    redis_acl_identity: str
    redis_prefix: str
    consumer_group: str
    stream_namespace: str
    reaper_authority: str
    clone_credential_removal_attested: bool


@dataclass(frozen=True, slots=True)
class RolloutProjectRecord:
    project_id: str
    repository_locator: str
    engine: str
    cohort: str | None
    publication_enabled: bool
    intake_enabled: bool


@dataclass(frozen=True)
class LegacyRolloutControls:
    """Read-only controls the legacy orchestrator loads once per poll."""

    issue_intake_engine: str | None
    legacy_admissions_frozen: bool
    omit_raw_task_credentials: bool
    lookup_unavailable: bool = False

    def excludes_issue_intake(self) -> bool:
        return (
            self.lookup_unavailable
            or self.legacy_admissions_frozen
            or (self.issue_intake_engine == "V1")
        )


def get_rollout_projection(store: RunStore) -> RolloutProjection:
    row = store.conn.execute(
        "SELECT * FROM rollout_projection WHERE controller_id = ?", (CONTROLLER_ID,)
    ).fetchone()
    assert row is not None
    return _row_to_projection(row)


def evaluate_rollout_checklist(
    store: RunStore,
    *,
    stage: int,
    gate: str,
    now_ms: int | None = None,
    observation_period_ms: int = DEFAULT_OBSERVATION_PERIOD_MS,
) -> RolloutChecklist:
    enums.parse_enum("rollout_checklist.gate", gate)
    if stage not in _STAGE_ITEMS:
        raise ValueError(f"unknown rollout stage {stage}")
    clock = _now_ms() if now_ms is None else now_ms
    items = tuple(
        replace(
            _evaluate_item(
                store,
                item_id=item_id,
                now_ms=clock,
                observation_period_ms=observation_period_ms,
            ),
            stage=stage,
            gate=gate,
        )
        for item_id in _STAGE_ITEMS[stage][gate]
    )
    return RolloutChecklist(stage=stage, gate=gate, items=items)


def apply_rollout_operation(
    store: RunStore,
    *,
    rollout_operation_id: str,
    operation_kind: str,
    expected_stage: int,
    expected_stage_revision: int,
    authenticated_principal_id: str,
    authorization_context_digest: str,
    requested_stage: int | None = None,
    evidence_item_id: str | None = None,
    now_ms: int | None = None,
    observation_period_ms: int = DEFAULT_OBSERVATION_PERIOD_MS,
    authority_revoked: bool = False,
) -> RolloutOperationResult:
    require_lowercase_uuid(rollout_operation_id, field="rollout_operation_id")
    enums.parse_enum("rollout_operation.kind", operation_kind)
    _require_digest(authorization_context_digest, field=_AUTHZ)
    clock = _now_ms() if now_ms is None else now_ms
    req_digest = request_digest(
        {
            "protocol_version": ROLLOUT_OPERATION_PROTOCOL,
            "operation_kind": operation_kind,
            "expected_stage": expected_stage,
            "expected_stage_revision": expected_stage_revision,
            "requested_stage": requested_stage,
            "evidence_item_id": evidence_item_id,
            "authenticated_principal_id": authenticated_principal_id,
            "authorization_context_digest": authorization_context_digest,
        }
    )
    with store.transaction():
        existing = store.conn.execute(
            "SELECT * FROM rollout_operations WHERE rollout_operation_id = ?",
            (rollout_operation_id,),
        ).fetchone()
        if existing is not None:
            if (
                existing["authenticated_principal_id"] == authenticated_principal_id
                and existing["request_digest"] == req_digest
            ):
                return _row_to_operation(existing, replayed=True)
            raise IdempotencyConflictError("rollout operation id reused with a conflicting body")
        if authority_revoked:
            return _commit_operation(
                store,
                operation_id=rollout_operation_id,
                operation_kind=operation_kind,
                expected_stage=expected_stage,
                expected_stage_revision=expected_stage_revision,
                requested_stage=requested_stage,
                principal=authenticated_principal_id,
                authz=authorization_context_digest,
                req_digest=req_digest,
                rejection="AUTHORITY_REVOKED",
                now_ms=clock,
            )
        projection = get_rollout_projection(store)
        if (
            projection.stage != expected_stage
            or projection.stage_revision != expected_stage_revision
        ):
            raise CasMismatchError("rollout stage revision CAS lost")
        rejection, checklist, new_proj = _plan_operation(
            store,
            projection=projection,
            operation_kind=operation_kind,
            requested_stage=requested_stage,
            evidence_item_id=evidence_item_id,
            now_ms=clock,
            observation_period_ms=observation_period_ms,
        )
        result = _commit_operation(
            store,
            operation_id=rollout_operation_id,
            operation_kind=operation_kind,
            expected_stage=expected_stage,
            expected_stage_revision=expected_stage_revision,
            requested_stage=requested_stage,
            principal=authenticated_principal_id,
            authz=authorization_context_digest,
            req_digest=req_digest,
            rejection=rejection,
            now_ms=clock,
            projection=None if rejection else new_proj,
            checklist=checklist,
        )
        if rejection is None and checklist is not None:
            _persist_checklist(store, rollout_operation_id, checklist)
        return result


def register_rollout_capacity_pool(
    store: RunStore,
    *,
    capacity_pool_id: str,
    template_class: str,
    template_id: str,
    pool_manager_principal_id: str,
    redis_acl_identity: str,
    redis_prefix: str,
    consumer_group: str,
    stream_namespace: str,
    reaper_authority: str,
    clone_credential_removal_attested: bool,
    now_ms: int | None = None,
) -> RolloutCapacityPoolRecord:
    require_lowercase_uuid(capacity_pool_id, field="capacity_pool_id")
    enums.parse_enum("capacity_pool.template_class", template_class)
    enums.parse_enum("rollout_capacity_pool.reaper_authority", reaper_authority)
    clock = _now_ms() if now_ms is None else now_ms
    with store.transaction():
        existing_map = _existing_pool_identities(store)
        rejection = validate_capacity_pool_isolation(
            template_class=template_class,
            template_id=template_id,
            pool_manager_principal_id=pool_manager_principal_id,
            redis_acl_identity=redis_acl_identity,
            redis_prefix=redis_prefix,
            consumer_group=consumer_group,
            stream_namespace=stream_namespace,
            reaper_authority=reaper_authority,
            clone_credential_removal_attested=clone_credential_removal_attested,
            existing=existing_map,
        )
        if rejection is not None:
            raise CasMismatchError(rejection)
        store.conn.execute(
            "INSERT INTO rollout_capacity_pools("
            "capacity_pool_id, template_class, template_id, pool_manager_principal_id, "
            "redis_acl_identity, redis_prefix, consumer_group, stream_namespace, "
            "reaper_authority, clone_credential_removal_attested, created_at_ms) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                capacity_pool_id,
                template_class,
                template_id,
                pool_manager_principal_id,
                redis_acl_identity,
                redis_prefix,
                consumer_group,
                stream_namespace,
                reaper_authority,
                1 if clone_credential_removal_attested else 0,
                clock,
            ),
        )
    return RolloutCapacityPoolRecord(
        capacity_pool_id=capacity_pool_id,
        template_class=template_class,
        template_id=template_id,
        pool_manager_principal_id=pool_manager_principal_id,
        redis_acl_identity=redis_acl_identity,
        redis_prefix=redis_prefix,
        consumer_group=consumer_group,
        stream_namespace=stream_namespace,
        reaper_authority=reaper_authority,
        clone_credential_removal_attested=clone_credential_removal_attested,
    )


def allow_rollout_project(
    store: RunStore,
    *,
    project_id: str,
    repository_locator: str,
    engine: str,
    publication_enabled: bool,
    intake_enabled: bool,
    cohort: str | None = None,
    cohort_cap: int | None = None,
    now_ms: int | None = None,
) -> RolloutProjectRecord:
    require_lowercase_uuid(project_id, field="project_id")
    enums.parse_enum("rollout_project.engine", engine)
    locator = repository_locator.strip()
    if not locator:
        raise ValueError("repository_locator must not be empty")
    clock = _now_ms() if now_ms is None else now_ms
    with store.transaction():
        registered = store.conn.execute(
            "SELECT 1 FROM projects WHERE project_id = ? OR repository_locator = ? COLLATE NOCASE",
            (project_id, locator),
        ).fetchone()
        if registered is None:
            raise CasMismatchError("PROJECT_NOT_REGISTERED")
        conflict = store.conn.execute(
            "SELECT engine FROM rollout_projects WHERE repository_locator = ? COLLATE NOCASE "
            "AND project_id != ?",
            (locator, project_id),
        ).fetchone()
        if conflict is not None:
            raise CasMismatchError("DUAL_ENGINE_OWNERSHIP")
        current = store.conn.execute(
            "SELECT engine FROM rollout_projects WHERE project_id = ?",
            (project_id,),
        ).fetchone()
        if current is not None and current["engine"] != engine:
            raise CasMismatchError("DUAL_ENGINE_OWNERSHIP")
        if cohort_cap is not None:
            count_row = store.conn.execute(
                "SELECT COUNT(*) AS n FROM rollout_projects WHERE cohort IS ? AND engine = 'V1'",
                (cohort,),
            ).fetchone()
            already = int(count_row["n"])
            if current is None and already >= cohort_cap:
                raise CasMismatchError("COHORT_CAP_EXCEEDED")
        store.conn.execute(
            "INSERT INTO rollout_projects("
            "project_id, repository_locator, engine, cohort, publication_enabled, "
            "intake_enabled, enabled_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(project_id) DO UPDATE SET "
            "repository_locator = excluded.repository_locator, "
            "engine = excluded.engine, cohort = excluded.cohort, "
            "publication_enabled = excluded.publication_enabled, "
            "intake_enabled = excluded.intake_enabled",
            (
                project_id,
                locator,
                engine,
                cohort,
                1 if publication_enabled else 0,
                1 if intake_enabled else 0,
                clock,
            ),
        )
    return RolloutProjectRecord(
        project_id=project_id,
        repository_locator=locator,
        engine=engine,
        cohort=cohort,
        publication_enabled=publication_enabled,
        intake_enabled=intake_enabled,
    )


def record_rollout_evidence(
    store: RunStore,
    *,
    evidence_id: str,
    stage: int,
    item_id: str,
    now_ms: int | None = None,
) -> None:
    require_lowercase_uuid(evidence_id, field="evidence_id")
    clock = _now_ms() if now_ms is None else now_ms
    with store.transaction():
        store.conn.execute(
            "INSERT INTO rollout_evidence(evidence_id, stage, item_id, recorded_at_ms) "
            "VALUES (?, ?, ?, ?) ON CONFLICT(stage, item_id) DO NOTHING",
            (evidence_id, stage, item_id, clock),
        )


def archive_legacy_work(
    store: RunStore,
    *,
    archive_id: str,
    kind: str,
    identity: str,
    project_id: str | None = None,
    now_ms: int | None = None,
) -> None:
    require_lowercase_uuid(archive_id, field="archive_id")
    enums.parse_enum("rollout_legacy_archive.kind", kind)
    clock = _now_ms() if now_ms is None else now_ms
    with store.transaction():
        store.conn.execute(
            "INSERT INTO rollout_legacy_archive("
            "archive_id, project_id, kind, identity, archived_at_ms, readonly) "
            "VALUES (?, ?, ?, ?, ?, 1) "
            "ON CONFLICT(kind, identity) DO NOTHING",
            (archive_id, project_id, kind, identity, clock),
        )


def engine_write_scope(
    store: RunStore,
    *,
    repository_locator: str,
    change_request_external_id: str | None = None,
    deterministic_ref: str | None = None,
) -> frozenset[str]:
    """Engines allowed to write this project/ref/PR. Never contains both."""
    return _engine_write_scope_conn(
        store.conn,
        repository_locator=repository_locator,
        change_request_external_id=change_request_external_id,
        deterministic_ref=deterministic_ref,
    )


def load_legacy_rollout_controls(
    state_root: Path | str,
    *,
    repository_locator: str,
) -> LegacyRolloutControls:
    if not repository_locator.strip():
        raise ValueError("repository_locator must not be empty")
    db_path = Path(state_root) / "workflow.db"
    conn = open_read_only(db_path)
    try:
        row = conn.execute(
            "SELECT engine, intake_enabled FROM rollout_projects "
            "WHERE repository_locator = ? COLLATE NOCASE",
            (repository_locator,),
        ).fetchone()
        projection = conn.execute(
            "SELECT legacy_admissions_frozen, raw_task_credentials_removed "
            "FROM rollout_projection WHERE controller_id = ?",
            (CONTROLLER_ID,),
        ).fetchone()
        frozen = bool(projection["legacy_admissions_frozen"]) if projection is not None else False
        omitted = (
            bool(projection["raw_task_credentials_removed"]) if projection is not None else False
        )
        return LegacyRolloutControls(
            issue_intake_engine=_issue_intake_engine(row),
            legacy_admissions_frozen=frozen,
            omit_raw_task_credentials=omitted,
        )
    finally:
        conn.close()


def _engine_write_scope_conn(
    conn: sqlite3.Connection,
    *,
    repository_locator: str,
    change_request_external_id: str | None,
    deterministic_ref: str | None,
) -> frozenset[str]:
    projection = conn.execute(
        "SELECT legacy_admissions_frozen FROM rollout_projection WHERE controller_id = ?",
        (CONTROLLER_ID,),
    ).fetchone()
    frozen = bool(projection["legacy_admissions_frozen"]) if projection is not None else False
    project = conn.execute(
        "SELECT engine FROM rollout_projects WHERE repository_locator = ? COLLATE NOCASE",
        (repository_locator,),
    ).fetchone()
    project_engine = None if project is None else str(project["engine"])
    if change_request_external_id is not None or deterministic_ref is not None:
        v1_owned = _publication_excludes(
            conn,
            repository_locator=repository_locator,
            change_request_external_id=change_request_external_id,
            deterministic_ref=deterministic_ref,
        )
        if v1_owned:
            return frozenset({"V1"})
        if frozen:
            return frozenset()
        return frozenset({"LEGACY"})
    if frozen and project_engine != "V1":
        return frozenset()
    if project_engine == "V1":
        return frozenset({"V1"})
    return frozenset({"LEGACY"})


def _publication_excludes(
    conn: sqlite3.Connection,
    *,
    repository_locator: str,
    change_request_external_id: str | None,
    deterministic_ref: str | None,
) -> bool:
    from orcest.workflow_reducer.graph import is_terminal_state

    conditions: list[str] = []
    params: list[str] = []
    if change_request_external_id is not None:
        conditions.append("p.change_request_external_id = ?")
        params.append(change_request_external_id)
    if deterministic_ref is not None:
        conditions.append("p.deterministic_branch = ?")
        params.append(deterministic_ref)
    if not conditions:
        return False
    params.append(repository_locator)
    rows = conn.execute(
        "SELECT runs.state AS run_state, r.state AS reservation_state "
        "FROM publications p "
        "JOIN runs ON runs.run_id = p.run_id "
        "JOIN projects project ON project.project_id = runs.project_id "
        "LEFT JOIN terminal_duplicate_cleanup_reservations r "
        "ON r.terminal_duplicate_cleanup_reservation_id = "
        "p.terminal_duplicate_cleanup_reservation_id "
        f"WHERE ({' OR '.join(conditions)}) "
        "AND project.repository_locator = ? COLLATE NOCASE",
        tuple(params),
    ).fetchall()
    return any(
        (not is_terminal_state(row["run_state"])) or row["reservation_state"] == "ACTIVE"
        for row in rows
    )


def _plan_operation(
    store: RunStore,
    *,
    projection: RolloutProjection,
    operation_kind: str,
    requested_stage: int | None,
    evidence_item_id: str | None,
    now_ms: int,
    observation_period_ms: int,
) -> tuple[str | None, RolloutChecklist | None, RolloutProjection | None]:
    if operation_kind == "INITIALIZE":
        if projection.status != "UNINITIALIZED" or projection.stage_revision != 0:
            return "STAGE_ORDER", None, None
        checklist = evaluate_rollout_checklist(store, stage=0, gate="ENTRY", now_ms=now_ms)
        if not checklist.passed:
            return _rejection_for_checklist(checklist), checklist, None
        return (
            None,
            checklist,
            RolloutProjection(
                controller_id=CONTROLLER_ID,
                stage=0,
                stage_revision=1,
                status="ACTIVE",
                last_operation_id=None,
                entered_at_ms=now_ms,
                observation_started_at_ms=None,
                publication_enabled=False,
                legacy_admissions_frozen=False,
                raw_task_credentials_removed=False,
                historical_readonly_retained=False,
            ),
        )
    if operation_kind == "FREEZE_LEGACY":
        if projection.stage < 4:
            return "STAGE_ORDER", None, None
        updated = _replace(
            projection,
            stage_revision=projection.stage_revision + 1,
            legacy_admissions_frozen=True,
            status="ACTIVE",
        )
        return None, None, updated
    if operation_kind == "REMOVE_CREDENTIALS":
        if projection.stage < 5 or not projection.legacy_admissions_frozen:
            return "LEGACY_DRAIN_INCOMPLETE", None, None
        updated = _replace(
            projection,
            stage_revision=projection.stage_revision + 1,
            raw_task_credentials_removed=True,
        )
        return None, None, updated
    if operation_kind == "ARCHIVE_LEGACY":
        if projection.stage < 5:
            return "STAGE_ORDER", None, None
        updated = _replace(
            projection,
            stage_revision=projection.stage_revision + 1,
            historical_readonly_retained=True,
        )
        return None, None, updated
    if operation_kind == "ROLLBACK":
        return _plan_rollback(
            store,
            projection=projection,
            requested_stage=requested_stage,
            now_ms=now_ms,
            observation_period_ms=observation_period_ms,
        )
    if operation_kind != "ADVANCE":
        return "UNKNOWN_KIND", None, None
    return _plan_advance(
        store,
        projection=projection,
        requested_stage=requested_stage,
        now_ms=now_ms,
        observation_period_ms=observation_period_ms,
    )


def _plan_advance(
    store: RunStore,
    *,
    projection: RolloutProjection,
    requested_stage: int | None,
    now_ms: int,
    observation_period_ms: int,
) -> tuple[str | None, RolloutChecklist | None, RolloutProjection | None]:
    if requested_stage is None:
        return "STAGE_ORDER", None, None
    completing = projection.stage == 5 and requested_stage == 5 and projection.status == "ACTIVE"
    if not completing and requested_stage != projection.stage + 1:
        return "STAGE_ORDER", None, None
    if projection.status not in {"ACTIVE", "ENTERING"}:
        return "STAGE_ORDER", None, None
    exit_stage = projection.stage
    exit_checklist = evaluate_rollout_checklist(
        store,
        stage=exit_stage,
        gate="EXIT",
        now_ms=now_ms,
        observation_period_ms=observation_period_ms,
    )
    if not exit_checklist.passed:
        return _rejection_for_checklist(exit_checklist), exit_checklist, None
    if completing:
        updated = _replace(
            projection, stage_revision=projection.stage_revision + 1, status="RETIRED"
        )
        return None, exit_checklist, updated
    entry_checklist = evaluate_rollout_checklist(
        store,
        stage=requested_stage,
        gate="ENTRY",
        now_ms=now_ms,
        observation_period_ms=observation_period_ms,
    )
    if not entry_checklist.passed:
        return _rejection_for_checklist(entry_checklist), entry_checklist, None
    publication_enabled = projection.publication_enabled or requested_stage >= 3
    frozen = projection.legacy_admissions_frozen or requested_stage >= 5
    observation = now_ms if requested_stage == 5 else projection.observation_started_at_ms
    combined = RolloutChecklist(
        stage=requested_stage,
        gate="ENTRY",
        items=exit_checklist.items + entry_checklist.items,
    )
    updated = RolloutProjection(
        controller_id=CONTROLLER_ID,
        stage=requested_stage,
        stage_revision=projection.stage_revision + 1,
        status="ACTIVE",
        last_operation_id=None,
        entered_at_ms=now_ms,
        observation_started_at_ms=observation,
        publication_enabled=publication_enabled,
        legacy_admissions_frozen=frozen,
        raw_task_credentials_removed=projection.raw_task_credentials_removed,
        historical_readonly_retained=projection.historical_readonly_retained,
    )
    return None, combined, updated


def _plan_rollback(
    store: RunStore,
    *,
    projection: RolloutProjection,
    requested_stage: int | None,
    now_ms: int,
    observation_period_ms: int,
) -> tuple[str | None, RolloutChecklist | None, RolloutProjection | None]:
    if requested_stage is None or requested_stage >= projection.stage:
        return "STAGE_ORDER", None, None
    if requested_stage < 0:
        return "STAGE_ORDER", None, None
    checklist = evaluate_rollout_checklist(
        store,
        stage=projection.stage,
        gate="ROLLBACK",
        now_ms=now_ms,
        observation_period_ms=observation_period_ms,
    )
    if not checklist.passed:
        return _rejection_for_checklist(checklist), checklist, None
    if _has_v1_publication(store) and requested_stage < 3:
        return "PUBLICATION_EXISTS", checklist, None
    updated = RolloutProjection(
        controller_id=CONTROLLER_ID,
        stage=requested_stage,
        stage_revision=projection.stage_revision + 1,
        status="ROLLED_BACK",
        last_operation_id=None,
        entered_at_ms=now_ms,
        observation_started_at_ms=None,
        publication_enabled=requested_stage >= 3,
        legacy_admissions_frozen=False,
        raw_task_credentials_removed=(
            projection.raw_task_credentials_removed if requested_stage >= 5 else False
        ),
        historical_readonly_retained=projection.historical_readonly_retained,
    )
    return None, checklist, updated


_CHECKLIST_ITEM_REJECTION_CODES: dict[str, str] = {
    "controller_maintenance": "CONTROLLER_NOT_MAINTENANCE",
    "issuance_key_selected": "ISSUANCE_KEY_MISSING",
    "no_ordinary_forge_io": "FORGE_IO_WHILE_MAINTENANCE",
    "no_owned_work": "OWNED_WORK_PRESENT",
    "backup_restore_evidence": "BACKUP_EVIDENCE_MISSING",
    "pel_reaper_isolated": "REAPER_AUTHORITY_MISMATCH",
    "clone_credential_attested": "CLONE_CREDENTIAL_REMOVAL_UNATTESTED",
    "disjoint_pool_inventory": "POOL_INVENTORY_INCOMPLETE",
    "single_pilot_project": "PILOT_NOT_SINGLE_PROJECT",
    "pilot_publication_disabled": "PUBLICATION_NOT_DISABLED",
    "no_duplicate_publication": "DUPLICATE_PUBLICATION",
    "no_stale_overwrite": "STALE_OVERWRITE",
    "one_engine_per_project": "DUAL_ENGINE_OWNERSHIP",
    "legacy_drained": "LEGACY_DRAIN_INCOMPLETE",
    "legacy_archived": "ARCHIVE_INCOMPLETE",
    "historical_readonly_retained": "HISTORICAL_READONLY_MISSING",
    "raw_credentials_removed": "CREDENTIALS_STILL_PRESENT",
    "observation_period_elapsed": "OBSERVATION_PERIOD_OPEN",
    "representative_runs_evidence": "REPRESENTATIVE_RUNS_MISSING",
    "no_v1_publication": "PUBLICATION_EXISTS",
}


def _rejection_for_checklist(checklist: RolloutChecklist) -> str:
    """Map the first failed item with a dedicated code to that code.

    Falls back to the generic ``CHECKLIST_FAILED`` for items without one
    (e.g. nested ``stage_N_exit`` gates), so operators can distinguish an
    expected, named wait condition (like ``OBSERVATION_PERIOD_OPEN``) from an
    unexpected checklist failure that should page someone.
    """
    for item in checklist.items:
        if not item.passed and item.item_id in _CHECKLIST_ITEM_REJECTION_CODES:
            return _CHECKLIST_ITEM_REJECTION_CODES[item.item_id]
    return "CHECKLIST_FAILED"


def _evaluate_item(
    store: RunStore,
    *,
    item_id: str,
    now_ms: int,
    observation_period_ms: int,
) -> ChecklistItemResult:
    conn = store.conn
    if item_id == "schema_current":
        version = int(conn.execute("PRAGMA user_version").fetchone()[0])
        return _item(item_id, version == SCHEMA_VERSION, "schema", f"user_version={version}")
    if item_id == "writer_lock":
        return _item(item_id, store._lock_fd is not None, "writer", "controller lock held")
    if item_id == "controller_maintenance":
        mode = conn.execute(
            "SELECT mode FROM controller_mode WHERE controller_id = ?", (CONTROLLER_ID,)
        ).fetchone()
        ok = mode is not None and mode["mode"] == "MAINTENANCE"
        return _item(item_id, ok, "mode", str(None if mode is None else mode["mode"]))
    if item_id == "issuance_key_selected":
        row = conn.execute(
            "SELECT current_issuance_key_id FROM capability_key_registry WHERE registry_id = ?",
            (CONTROLLER_ID,),
        ).fetchone()
        ok = row is not None and row["current_issuance_key_id"] is not None
        return _item(item_id, ok, "issuance", "selected" if ok else "missing")
    if item_id == "projects_registered":
        count = _count(conn, "SELECT COUNT(*) FROM projects")
        return _item(item_id, count > 0, "projects", f"count={count}")
    if item_id == "no_ordinary_forge_io":
        count = _count(
            conn,
            "SELECT COUNT(*) FROM forge_observation_requests WHERE state != 'PENDING'",
        )
        return _item(item_id, count == 0, "forge_io", f"non_pending={count}")
    if item_id == "no_owned_work":
        count = _count(
            conn,
            "SELECT COUNT(*) FROM runs WHERE state NOT IN ('MERGED', 'CLOSED', 'CANCELLED')",
        )
        return _item(item_id, count == 0, "runs", f"nonterminal={count}")
    if item_id == "backup_restore_evidence":
        return _evidence_item(conn, 0, "backup_restore_drill", item_id)
    if item_id.startswith("stage_") and item_id.endswith("_exit"):
        prior = int(item_id.split("_")[1])
        nested = evaluate_rollout_checklist(
            store,
            stage=prior,
            gate="EXIT",
            now_ms=now_ms,
            observation_period_ms=observation_period_ms,
        )
        return _item(item_id, nested.passed, "prior_exit", f"stage={prior}")
    if item_id == "pools_both_classes":
        classes = {
            row["template_class"]
            for row in conn.execute("SELECT DISTINCT template_class FROM rollout_capacity_pools")
        }
        ok = classes == {"LEGACY", "V1_CLONE_FIXED"}
        return _item(item_id, ok, "pools", ",".join(sorted(classes)))
    if item_id == "disjoint_pool_inventory":
        return _item(item_id, _pools_disjoint(conn), "isolation", "unique identities")
    if item_id == "pel_reaper_isolated":
        bad = _count(
            conn,
            "SELECT COUNT(*) FROM rollout_capacity_pools WHERE "
            "(template_class = 'LEGACY' AND reaper_authority != 'LEGACY_PEL_ALLOWLIST') "
            "OR (template_class = 'V1_CLONE_FIXED' "
            "AND reaper_authority != 'V1_AUTHENTICATED_LOSS')",
        )
        return _item(item_id, bad == 0, "reaper", f"mismatched={bad}")
    if item_id == "clone_credential_attested":
        bad = _count(
            conn,
            "SELECT COUNT(*) FROM rollout_capacity_pools "
            "WHERE template_class = 'V1_CLONE_FIXED' AND clone_credential_removal_attested = 0",
        )
        return _item(item_id, bad == 0, "clone_credential", f"unattested={bad}")
    if item_id == "synthetic_canary_evidence":
        return _evidence_item(conn, 1, "synthetic_canary", item_id)
    if item_id == "single_pilot_project":
        count = _count(conn, "SELECT COUNT(*) FROM rollout_projects WHERE engine = 'V1'")
        return _item(item_id, count == 1, "pilot", f"v1_projects={count}")
    if item_id == "pilot_publication_disabled":
        enabled = _count(
            conn,
            "SELECT COUNT(*) FROM rollout_projects WHERE engine = 'V1' AND publication_enabled = 1",
        )
        return _item(item_id, enabled == 0, "publication", f"enabled={enabled}")
    if item_id == "pilot_legacy_issue_excluded":
        count = _count(
            conn,
            "SELECT COUNT(*) FROM rollout_projects WHERE engine = 'V1' AND intake_enabled = 1",
        )
        return _item(item_id, count == 1, "issue_exclusion", f"v1_intake={count}")
    if item_id == "pilot_publication_enabled":
        enabled = _count(
            conn,
            "SELECT COUNT(*) FROM rollout_projects WHERE engine = 'V1' AND publication_enabled = 1",
        )
        proj = get_rollout_projection(store)
        ok = enabled == 1 or proj.publication_enabled
        return _item(item_id, ok, "publication", f"enabled={enabled}")
    if item_id == "no_duplicate_publication":
        return _item(item_id, not _has_unresolved_duplicate(conn), "duplicate", "none")
    if item_id == "no_stale_overwrite":
        return _item(item_id, not _has_unresolved_stale_overwrite(conn), "overwrite", "none")
    if item_id == "no_ownership_conflict":
        count = _count(
            conn,
            "SELECT COUNT(*) FROM runs r "
            "JOIN human_boundaries h ON h.human_boundary_id = r.human_boundary_id "
            "WHERE h.reason = 'PUBLICATION_OWNERSHIP_CONFLICT'",
        )
        return _item(item_id, count == 0, "ownership", f"open={count}")
    if item_id == "allowlist_exclusive":
        return _item(item_id, _allowlist_exclusive(conn), "allowlist", "exclusive")
    if item_id == "one_engine_per_project":
        dual = _count(
            conn,
            "SELECT COUNT(*) FROM ("
            "SELECT repository_locator FROM rollout_projects "
            "GROUP BY repository_locator HAVING COUNT(DISTINCT engine) > 1)",
        )
        return _item(item_id, dual == 0, "engine", f"dual={dual}")
    if item_id == "open_pr_unambiguous":
        return _item(item_id, not _has_unresolved_duplicate(conn), "pr_owner", "unambiguous")
    if item_id == "legacy_admissions_frozen":
        proj = get_rollout_projection(store)
        return _item(
            item_id,
            proj.legacy_admissions_frozen,
            "freeze",
            str(proj.legacy_admissions_frozen),
        )
    if item_id == "legacy_drained":
        remaining = _legacy_owned_nonterminal_run_count(conn)
        archived = _count(conn, "SELECT COUNT(*) FROM rollout_legacy_archive WHERE kind = 'TASK'")
        return _item(
            item_id,
            remaining == 0,
            "drain",
            f"nonterminal={remaining} archived_tasks={archived}",
        )
    if item_id == "legacy_archived":
        kinds = {
            row["kind"] for row in conn.execute("SELECT DISTINCT kind FROM rollout_legacy_archive")
        }
        ok = {"TASK", "PR", "ISSUE", "EVIDENCE"} <= kinds or _has_evidence(
            conn, 5, "legacy_archived"
        )
        return _item(item_id, ok, "archive", ",".join(sorted(kinds)))
    if item_id == "historical_readonly_retained":
        proj = get_rollout_projection(store)
        readonly = _count(conn, "SELECT COUNT(*) FROM rollout_legacy_archive WHERE readonly = 1")
        ok = proj.historical_readonly_retained or readonly > 0
        return _item(item_id, ok, "readonly", f"rows={readonly}")
    if item_id == "raw_credentials_removed":
        proj = get_rollout_projection(store)
        return _item(
            item_id,
            proj.raw_task_credentials_removed,
            "credentials",
            str(proj.raw_task_credentials_removed),
        )
    if item_id == "observation_period_elapsed":
        proj = get_rollout_projection(store)
        started = proj.observation_started_at_ms
        ok = started is not None and (now_ms - started) >= observation_period_ms
        return _item(item_id, ok, "observation", f"started={started}")
    if item_id == "representative_runs_evidence":
        return _evidence_item(conn, 5, "representative_runs", item_id)
    if item_id == "no_v1_publication":
        return _item(item_id, not _has_v1_publication(store), "publication", "absent")
    if item_id == "v1_keeps_publications":
        return _item(item_id, True, "publication", "retained")
    return _item(item_id, False, "unknown", item_id)


def _issue_intake_engine(row: sqlite3.Row | None) -> str | None:
    if row is None:
        return None
    engine = str(row["engine"])
    if engine == "V1" and not bool(row["intake_enabled"]):
        return None
    return engine


def _legacy_owned_nonterminal_run_count(conn: sqlite3.Connection) -> int:
    return _count(
        conn,
        "SELECT COUNT(*) FROM runs r "
        "LEFT JOIN rollout_projects rp ON rp.project_id = r.project_id "
        "WHERE r.state NOT IN ('MERGED', 'CLOSED', 'CANCELLED') "
        "AND COALESCE(rp.engine, 'LEGACY') != 'V1'",
    )


def _item(item_id: str, passed: bool, evidence_code: str, detail: str) -> ChecklistItemResult:
    return ChecklistItemResult(item_id, passed, evidence_code, detail)


def _evidence_item(
    conn: sqlite3.Connection, stage: int, token: str, item_id: str
) -> ChecklistItemResult:
    ok = _has_evidence(conn, stage, token)
    return _item(item_id, ok, "evidence", token)


def _has_evidence(conn: sqlite3.Connection, stage: int, item_id: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM rollout_evidence WHERE stage = ? AND item_id = ?",
        (stage, item_id),
    ).fetchone()
    return row is not None


def _count(conn: sqlite3.Connection, sql: str, params: Sequence[object] = ()) -> int:
    return int(conn.execute(sql, tuple(params)).fetchone()[0])


def _has_v1_publication(store: RunStore) -> bool:
    return _count(store.conn, "SELECT COUNT(*) FROM publications") > 0


def _has_unresolved_duplicate(conn: sqlite3.Connection) -> bool:
    active = _count(
        conn,
        "SELECT COUNT(*) FROM terminal_duplicate_cleanup_reservations WHERE state = 'ACTIVE'",
    )
    if active:
        return True
    multiple = _count(
        conn,
        "SELECT COUNT(*) FROM change_request_search_results r "
        "JOIN ("
        "  SELECT change_request_search_result_id FROM change_request_search_members "
        "  WHERE member_class = 'LIVE' GROUP BY change_request_search_result_id "
        "  HAVING COUNT(*) > 1"
        ") m ON m.change_request_search_result_id = r.change_request_search_result_id",
    )
    return multiple > 0


def _has_unresolved_stale_overwrite(conn: sqlite3.Connection) -> bool:
    mismatches = conn.execute(
        "SELECT publication_id, effect_generation FROM publication_effect_checkpoints "
        "WHERE suboperation_kind IN ('REF_CREATE', 'REF_UPDATE') AND status = 'CAS_MISMATCH'"
    ).fetchall()
    for row in mismatches:
        recovered = conn.execute(
            "SELECT 1 FROM publication_effect_checkpoints "
            "WHERE publication_id = ? AND effect_generation = ? "
            "AND suboperation_kind IN ('REF_CREATE', 'REF_UPDATE') "
            "AND status = 'OBSERVED_SATISFIED' AND recorded_at_ms >= ("
            "  SELECT MIN(recorded_at_ms) FROM publication_effect_checkpoints "
            "  WHERE publication_id = ? AND effect_generation = ? AND status = 'CAS_MISMATCH'"
            ")",
            (
                row["publication_id"],
                row["effect_generation"],
                row["publication_id"],
                row["effect_generation"],
            ),
        ).fetchone()
        if recovered is None:
            return True
    return False


def _pools_disjoint(conn: sqlite3.Connection) -> bool:
    for column in (
        "template_id",
        "pool_manager_principal_id",
        "redis_acl_identity",
        "redis_prefix",
        "consumer_group",
        "stream_namespace",
    ):
        row = conn.execute(
            f"SELECT COUNT(*) AS n FROM ("
            f"SELECT {column} FROM rollout_capacity_pools GROUP BY {column} "
            f"HAVING COUNT(DISTINCT template_class) > 1)"
        ).fetchone()
        if int(row["n"]) > 0:
            return False
    return _count(conn, "SELECT COUNT(*) FROM rollout_capacity_pools") >= 2


def _allowlist_exclusive(conn: sqlite3.Connection) -> bool:
    v1 = _count(conn, "SELECT COUNT(*) FROM rollout_projects WHERE engine = 'V1'")
    return v1 >= 1


def _existing_pool_identities(store: RunStore) -> dict[str, tuple[str, str]]:
    mapping: dict[str, tuple[str, str]] = {}
    rows = store.conn.execute("SELECT * FROM rollout_capacity_pools").fetchall()
    fields = (
        "template_id",
        "pool_manager_principal_id",
        "redis_acl_identity",
        "redis_prefix",
        "consumer_group",
        "stream_namespace",
    )
    for row in rows:
        for field in fields:
            mapping[f"{field}:{row[field]}"] = (str(row[field]), str(row["template_class"]))
    return mapping


def _replace(projection: RolloutProjection, **changes: Any) -> RolloutProjection:
    return replace(projection, **changes)


def _commit_operation(
    store: RunStore,
    *,
    operation_id: str,
    operation_kind: str,
    expected_stage: int,
    expected_stage_revision: int,
    requested_stage: int | None,
    principal: str,
    authz: str,
    req_digest: str,
    rejection: str | None,
    now_ms: int,
    projection: RolloutProjection | None = None,
    checklist: RolloutChecklist | None = None,
) -> RolloutOperationResult:
    status = "REJECTED" if rejection else "SUCCEEDED"
    body: dict[str, object] = {
        "protocol_version": ROLLOUT_OPERATION_RESULT_PROTOCOL,
        "rollout_operation_id": operation_id,
        "operation_kind": operation_kind,
        "status": status,
        "replayed": False,
    }
    result_stage = None if rejection else (None if projection is None else projection.stage)
    result_revision = (
        None if rejection else (None if projection is None else projection.stage_revision)
    )
    if rejection:
        body["rejection_code"] = rejection
        http_status = 403 if rejection == "AUTHORITY_REVOKED" else 409
    else:
        body["stage"] = result_stage
        body["stage_revision"] = result_revision
        http_status = 200
    validate_envelope(body)
    body_json = canonical_json_text(body)
    resp_digest = response_digest({"http_status": http_status, "body": body})
    store.conn.execute(
        "INSERT INTO rollout_operations("
        "rollout_operation_id, protocol_version, operation_kind, expected_stage, "
        "expected_stage_revision, requested_stage, authenticated_principal_id, "
        "authorization_context_digest, request_digest, status, rejection_code, "
        "result_stage, result_stage_revision, response_http_status, response_json, "
        "response_digest, committed_at_ms) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            operation_id,
            ROLLOUT_OPERATION_PROTOCOL,
            operation_kind,
            expected_stage,
            expected_stage_revision,
            requested_stage,
            principal,
            authz,
            req_digest,
            status,
            rejection,
            result_stage,
            result_revision,
            http_status,
            body_json,
            resp_digest,
            now_ms,
        ),
    )
    if projection is not None:
        store.conn.execute(
            "UPDATE rollout_projection SET stage = ?, stage_revision = ?, status = ?, "
            "last_operation_id = ?, entered_at_ms = ?, observation_started_at_ms = ?, "
            "publication_enabled = ?, legacy_admissions_frozen = ?, "
            "raw_task_credentials_removed = ?, historical_readonly_retained = ? "
            "WHERE controller_id = ? AND stage_revision = ?",
            (
                projection.stage,
                projection.stage_revision,
                projection.status,
                operation_id,
                projection.entered_at_ms,
                projection.observation_started_at_ms,
                1 if projection.publication_enabled else 0,
                1 if projection.legacy_admissions_frozen else 0,
                1 if projection.raw_task_credentials_removed else 0,
                1 if projection.historical_readonly_retained else 0,
                CONTROLLER_ID,
                expected_stage_revision,
            ),
        )
        store.conn.execute(
            "UPDATE rollout_projects SET publication_enabled = ? WHERE engine = 'V1'",
            (1 if projection.publication_enabled else 0,),
        )
    row = store.conn.execute(
        "SELECT * FROM rollout_operations WHERE rollout_operation_id = ?",
        (operation_id,),
    ).fetchone()
    assert row is not None
    result = _row_to_operation(row, replayed=False)
    if checklist is not None:
        return RolloutOperationResult(
            rollout_operation_id=result.rollout_operation_id,
            operation_kind=result.operation_kind,
            status=result.status,
            response_http_status=result.response_http_status,
            response_json=result.response_json,
            response_digest=result.response_digest,
            committed_at_ms=result.committed_at_ms,
            rejection_code=result.rejection_code,
            stage=result.stage,
            stage_revision=result.stage_revision,
            replayed=False,
            checklist=checklist,
        )
    return result


def _persist_checklist(store: RunStore, operation_id: str, checklist: RolloutChecklist) -> None:
    for item in checklist.items:
        store.conn.execute(
            "INSERT INTO rollout_checklist_results("
            "rollout_operation_id, stage, gate, item_id, passed, evidence_code, detail) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                operation_id,
                checklist.stage if item.stage is None else item.stage,
                checklist.gate if item.gate is None else item.gate,
                item.item_id,
                1 if item.passed else 0,
                item.evidence_code,
                item.detail,
            ),
        )


def _row_to_projection(row: sqlite3.Row) -> RolloutProjection:
    return RolloutProjection(
        controller_id=row["controller_id"],
        stage=int(row["stage"]),
        stage_revision=int(row["stage_revision"]),
        status=row["status"],
        last_operation_id=row["last_operation_id"],
        entered_at_ms=row["entered_at_ms"],
        observation_started_at_ms=row["observation_started_at_ms"],
        publication_enabled=bool(row["publication_enabled"]),
        legacy_admissions_frozen=bool(row["legacy_admissions_frozen"]),
        raw_task_credentials_removed=bool(row["raw_task_credentials_removed"]),
        historical_readonly_retained=bool(row["historical_readonly_retained"]),
    )


def _row_to_operation(row: sqlite3.Row, *, replayed: bool) -> RolloutOperationResult:
    return RolloutOperationResult(
        rollout_operation_id=row["rollout_operation_id"],
        operation_kind=row["operation_kind"],
        status=row["status"],
        response_http_status=int(row["response_http_status"]),
        response_json=row["response_json"],
        response_digest=row["response_digest"],
        committed_at_ms=int(row["committed_at_ms"]),
        rejection_code=row["rejection_code"],
        stage=row["result_stage"],
        stage_revision=row["result_stage_revision"],
        replayed=replayed,
    )
