"""Durable, secret-free observability for Workflow-Control v1.

The collector is deliberately a read model.  It neither records telemetry in
the workflow database nor changes workflow state.  Gauges are reconstructed
from authoritative rows on every collection and lifecycle events use durable
row identities, making a repeated collection/replay idempotent for a sink that
deduplicates on ``event_id``.
"""

from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import dataclass
from typing import Any, Final

from orcest.workflow_contract.v1.digest import generic_domain_digest
from orcest.workflow_store.store import SCHEMA_VERSION

__all__ = [
    "Alert",
    "DiagnosticPacket",
    "MetricSample",
    "ObservabilitySnapshot",
    "ObservabilityThresholds",
    "ReleaseGate",
    "StructuredEvent",
    "collect_observability",
]

_PROTOCOL: Final = "workflow-observability-v1"
_SAFE_VALUE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,127}$")
_MAX_DIAGNOSTICS = 64
_MAX_PACKET_BYTES = 32_768


@dataclass(frozen=True, slots=True)
class ObservabilityThresholds:
    """Operator-owned thresholds; none of these values grants workflow authority."""

    outbox_warning_ms: int = 60_000
    outbox_critical_ms: int = 300_000
    publication_warning_ms: int = 900_000
    publication_critical_ms: int = 3_600_000
    report_warning_ms: int = 300_000
    report_critical_ms: int = 900_000
    timer_lag_warning_ms: int = 30_000
    timer_lag_critical_ms: int = 300_000
    backup_warning_ms: int = 86_400_000
    backup_critical_ms: int = 172_800_000
    restore_drill_warning_ms: int = 30 * 86_400_000
    restore_drill_critical_ms: int = 90 * 86_400_000


@dataclass(frozen=True, slots=True)
class MetricSample:
    name: str
    value: int
    labels: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True, slots=True)
class StructuredEvent:
    event_id: str
    kind: str
    occurred_at_ms: int
    project_id: str | None
    correlation_id: str
    attributes: tuple[tuple[str, str | int], ...]
    protocol: str = _PROTOCOL


@dataclass(frozen=True, slots=True)
class Alert:
    code: str
    severity: str
    owner: str
    active: bool
    observed: int
    threshold: int


@dataclass(frozen=True, slots=True)
class ReleaseGate:
    stage: int
    code: str
    passed: bool
    signal: str


@dataclass(frozen=True, slots=True)
class DiagnosticPacket:
    generated_at_ms: int
    health: str
    alerts: tuple[Alert, ...]
    gates: tuple[ReleaseGate, ...]
    facts: tuple[tuple[str, int], ...]
    protocol: str = _PROTOCOL
    truncated: bool = False

    def to_json(self) -> str:
        body = {
            "protocol": self.protocol,
            "generated_at_ms": self.generated_at_ms,
            "health": self.health,
            "truncated": self.truncated,
            "alerts": [
                {
                    "code": a.code,
                    "severity": a.severity,
                    "owner": a.owner,
                    "active": a.active,
                    "observed": a.observed,
                    "threshold": a.threshold,
                }
                for a in self.alerts
            ],
            "gates": [
                {"stage": g.stage, "code": g.code, "passed": g.passed, "signal": g.signal}
                for g in self.gates
            ],
            "facts": dict(self.facts),
        }
        encoded = json.dumps(body, sort_keys=True, separators=(",", ":"))
        if len(encoded.encode()) > _MAX_PACKET_BYTES:
            raise ValueError("diagnostic packet exceeds its fixed byte bound")
        return encoded


@dataclass(frozen=True, slots=True)
class ObservabilitySnapshot:
    generated_at_ms: int
    metrics: tuple[MetricSample, ...]
    events: tuple[StructuredEvent, ...]
    packet: DiagnosticPacket


def _labels(**values: str | None) -> tuple[tuple[str, str], ...]:
    answer: list[tuple[str, str]] = []
    for key, value in sorted(values.items()):
        if value is None:
            continue
        rendered = (
            value
            if _SAFE_VALUE.fullmatch(value)
            else "hash:" + generic_domain_digest("observability-label-v1", value)[7:23]
        )
        answer.append((key, rendered))
    return tuple(answer)


def _safe_scope(value: str) -> str:
    return value if _SAFE_VALUE.fullmatch(value) else _labels(scope=value)[0][1]


def _age(now_ms: int, timestamp: Any) -> int:
    return max(0, now_ms - int(timestamp)) if timestamp is not None else 0


def _scalar(conn: sqlite3.Connection, sql: str, parameters: tuple[Any, ...] = ()) -> int:
    row = conn.execute(sql, parameters).fetchone()
    return int(row[0] or 0)


def _event_id(kind: str, durable_id: str) -> str:
    return generic_domain_digest(
        "workflow-observability-event-v1", {"kind": kind, "durable_id": durable_id}
    )


def collect_observability(
    conn: sqlite3.Connection,
    *,
    now_ms: int,
    thresholds: ObservabilityThresholds = ObservabilityThresholds(),
    redis_rebuild_ok: bool = True,
    audit_write_ok: bool = True,
    competing_writer: bool = False,
    disk_free_bytes: int | None = None,
    disk_critical_bytes: int = 1_073_741_824,
    backup_age_ms: int | None = None,
    restore_drill_age_ms: int | None = None,
) -> ObservabilitySnapshot:
    """Reconcile gauges, events, alerts, and release gates from durable state.

    Runtime-only health inputs are closed booleans/integers and cannot carry
    adapter responses, model text, credentials, or other arbitrary content.
    """
    metrics: list[MetricSample] = [
        MetricSample("workflow_schema_info", 1, _labels(version=str(SCHEMA_VERSION))),
        MetricSample("workflow_singleton_writer_healthy", int(not competing_writer)),
        MetricSample("workflow_redis_rebuild_healthy", int(redis_rebuild_ok)),
    ]

    integrity = str(conn.execute("PRAGMA quick_check").fetchone()[0]) == "ok"
    foreign_key_errors = sum(1 for _ in conn.execute("PRAGMA foreign_key_check"))
    metrics.extend(
        [
            MetricSample(
                "workflow_storage_integrity_healthy", int(integrity and not foreign_key_errors)
            ),
            MetricSample("workflow_storage_foreign_key_errors", foreign_key_errors),
        ]
    )

    for row in conn.execute(
        "SELECT project_id, state, COUNT(*), MIN(created_at_ms) FROM runs "
        "GROUP BY project_id, state ORDER BY project_id, state"
    ):
        metrics.append(
            MetricSample(
                "workflow_runs", int(row[2]), _labels(project_id=str(row[0]), state=str(row[1]))
            )
        )
        metrics.append(
            MetricSample(
                "workflow_run_oldest_age_ms",
                _age(now_ms, row[3]),
                _labels(project_id=str(row[0]), state=str(row[1])),
            )
        )

    for table, name in (("activities", "workflow_activities"), ("attempts", "workflow_attempts")):
        for row in conn.execute(
            f"SELECT state, COUNT(*) FROM {table} GROUP BY state ORDER BY state"
        ):
            metrics.append(MetricSample(name, int(row[1]), _labels(state=str(row[0]))))

    pending_outbox = _scalar(conn, "SELECT COUNT(*) FROM outbox WHERE state = 'PENDING'")
    pending_projection = _scalar(
        conn, "SELECT COUNT(*) FROM projection_outbox WHERE state = 'PENDING'"
    )
    oldest_outbox = _scalar(
        conn,
        "SELECT COALESCE(MAX(? - created_at_ms), 0) FROM outbox WHERE state = 'PENDING'",
        (now_ms,),
    )
    rollout_stage = 0
    rollout_revision = 0
    try:
        rollout_row = conn.execute(
            "SELECT stage, stage_revision, status FROM rollout_projection "
            "WHERE controller_id = 'ORCEST_V1'"
        ).fetchone()
        if rollout_row is not None:
            rollout_stage = int(rollout_row[0])
            rollout_revision = int(rollout_row[1])
            metrics.append(
                MetricSample(
                    "workflow_rollout_stage",
                    rollout_stage,
                    _labels(status=str(rollout_row[2])),
                )
            )
            metrics.append(MetricSample("workflow_rollout_revision", rollout_revision))
    except sqlite3.OperationalError:
        pass

    oldest_projection = _scalar(
        conn,
        "SELECT COALESCE(MAX(? - created_at_ms), 0) FROM projection_outbox WHERE state = 'PENDING'",
        (now_ms,),
    )
    metrics.extend(
        [
            MetricSample("workflow_outbox_pending", pending_outbox),
            MetricSample("workflow_projection_outbox_pending", pending_projection),
            MetricSample("workflow_outbox_oldest_age_ms", max(0, oldest_outbox)),
            MetricSample("workflow_projection_outbox_oldest_age_ms", max(0, oldest_projection)),
        ]
    )

    wait_count = _scalar(conn, "SELECT COUNT(*) FROM runs WHERE wait_condition_id IS NOT NULL")
    boundary_count = _scalar(conn, "SELECT COUNT(*) FROM runs WHERE human_boundary_id IS NOT NULL")
    metrics.extend(
        [
            MetricSample("workflow_waits_active", wait_count),
            MetricSample("workflow_human_boundaries_active", boundary_count),
        ]
    )
    for row in conn.execute(
        "SELECT w.reason, COUNT(*) FROM runs r JOIN wait_conditions w "
        "ON w.wait_condition_id = r.wait_condition_id GROUP BY w.reason ORDER BY w.reason"
    ):
        metrics.append(
            MetricSample("workflow_waits_active", int(row[1]), _labels(reason=str(row[0])))
        )

    latest_capacity = conn.execute(
        "SELECT MAX(accepted_at_ms), MAX(report_sequence) FROM capacity_reports"
    ).fetchone()
    # No report ever received is a distinct signal from a report received a
    # long time ago; like oldest_outbox/oldest_projection above, default to 0
    # (healthy) rather than treating "never" as the worst possible age.
    capacity_age = _age(now_ms, latest_capacity[0]) if latest_capacity[0] is not None else 0
    latest_budget = conn.execute(
        "SELECT MAX(accepted_at_ms), MAX(source_sequence) FROM budget_reports"
    ).fetchone()
    budget_age = _age(now_ms, latest_budget[0]) if latest_budget[0] is not None else 0
    metrics.extend(
        [
            MetricSample("workflow_capacity_report_age_ms", capacity_age),
            MetricSample("workflow_capacity_report_revision", int(latest_capacity[1] or 0)),
            MetricSample("workflow_budget_report_age_ms", budget_age),
            MetricSample("workflow_budget_report_source_sequence", int(latest_budget[1] or 0)),
        ]
    )
    for row in conn.execute(
        "SELECT availability, COUNT(*) FROM budget_reports "
        "GROUP BY availability ORDER BY availability"
    ):
        metrics.append(
            MetricSample("workflow_budget_reports", int(row[1]), _labels(availability=str(row[0])))
        )

    due_lag = _scalar(
        conn,
        "SELECT COALESCE(MAX(? - w.not_before_ms), 0) FROM runs r JOIN wait_conditions w "
        "ON w.wait_condition_id = r.wait_condition_id WHERE w.not_before_ms <= ?",
        (now_ms, now_ms),
    )
    forge_failures = _scalar(conn, "SELECT COUNT(*) FROM forge_request_failure_facts")
    reducer_errors = _scalar(
        conn,
        "SELECT COUNT(*) FROM transitions WHERE next_state = 'NEEDS_HUMAN' "
        "AND trigger_kind = 'INTERNAL'",
    )
    metrics.extend(
        [
            MetricSample("workflow_timer_due_lag_ms", max(0, due_lag)),
            MetricSample("workflow_forge_failures_total", forge_failures),
            MetricSample("workflow_reducer_errors_total", reducer_errors),
        ]
    )

    receipt_total = _scalar(conn, "SELECT COUNT(*) FROM review_receipts") + _scalar(
        conn, "SELECT COUNT(*) FROM adjudication_receipts"
    )
    consensus_total = _scalar(conn, "SELECT COUNT(*) FROM consensus_decisions")
    metrics.extend(
        [
            MetricSample("workflow_receipts_accepted_total", receipt_total),
            MetricSample("workflow_consensus_decisions_total", consensus_total),
        ]
    )

    duplicate_active = _scalar(
        conn, "SELECT COUNT(*) FROM terminal_duplicate_cleanup_reservations WHERE state = 'ACTIVE'"
    )
    duplicate_complete = _scalar(
        conn, "SELECT COUNT(*) FROM terminal_duplicate_cleanup_actions WHERE state = 'COMPLETED'"
    )
    metrics.extend(
        [
            MetricSample("workflow_duplicate_repairs_active", duplicate_active),
            MetricSample("workflow_duplicate_repairs_completed_total", duplicate_complete),
        ]
    )

    publication_age = _scalar(
        conn,
        "SELECT COALESCE(MAX(? - created_at_ms), 0) FROM publications WHERE state != 'CLOSED'",
        (now_ms,),
    )
    ownership_conflicts = _scalar(
        conn, "SELECT COUNT(*) FROM reconciliation_facts WHERE kind = 'OWNERSHIP_CONFLICT'"
    )
    metrics.extend(
        [
            MetricSample("workflow_publication_oldest_latency_ms", max(0, publication_age)),
            MetricSample("workflow_publication_ownership_conflicts_total", ownership_conflicts),
        ]
    )

    # Relational candidate references are protected by foreign keys.  Missing
    # object bytes are reported by the request-first storage health-probe path;
    # this collector never probes storage and turns an exception into authority.
    unavailable_storage = _scalar(
        conn,
        "SELECT COUNT(*) FROM health_observations h WHERE h.scope_kind IN ('STORAGE', 'SECRET') "
        "AND h.kind = 'UNAVAILABLE' AND h.health_sequence = (SELECT MAX(h2.health_sequence) "
        "FROM health_observations h2 WHERE h2.scope_kind = h.scope_kind "
        "AND h2.scope_id = h.scope_id)",
    )
    # The typed Health Observation is authority for object availability.  Do
    # not inspect its subject bindings (which may include storage identifiers)
    # merely to split this safe aggregate by object kind.  This stub metric is
    # independent of the missing_candidates alert input passed to _alerts()
    # below, which reuses unavailable_storage.
    missing_candidates_metric = 0
    metrics.extend(
        [
            MetricSample("workflow_missing_live_objects", unavailable_storage),
            MetricSample("workflow_missing_live_candidates", missing_candidates_metric),
        ]
    )

    events = _collect_events(conn)
    alerts = _alerts(
        oldest_outbox=max(oldest_outbox, oldest_projection),
        publication_age=publication_age,
        capacity_age=capacity_age,
        due_lag=due_lag,
        ownership_conflicts=ownership_conflicts,
        missing_candidates=unavailable_storage,
        integrity_ok=integrity and foreign_key_errors == 0,
        redis_rebuild_ok=redis_rebuild_ok,
        audit_write_ok=audit_write_ok,
        competing_writer=competing_writer,
        disk_free_bytes=disk_free_bytes,
        disk_critical_bytes=disk_critical_bytes,
        backup_age_ms=backup_age_ms,
        restore_drill_age_ms=restore_drill_age_ms,
        thresholds=thresholds,
    )
    gates = _gates(
        integrity_ok=integrity and foreign_key_errors == 0,
        redis_rebuild_ok=redis_rebuild_ok,
        oldest_outbox=max(oldest_outbox, oldest_projection),
        receipt_total=receipt_total,
        consensus_total=consensus_total,
        ownership_conflicts=ownership_conflicts,
        duplicate_active=duplicate_active,
        publication_age=publication_age,
        backup_age_ms=backup_age_ms,
        restore_drill_age_ms=restore_drill_age_ms,
        thresholds=thresholds,
    )
    active = tuple(a for a in alerts if a.active)
    facts_all = tuple(sorted({m.name: m.value for m in metrics if not m.labels}.items()))
    truncated = len(active) > _MAX_DIAGNOSTICS or len(facts_all) > _MAX_DIAGNOSTICS
    active = active[:_MAX_DIAGNOSTICS]
    facts = facts_all[:_MAX_DIAGNOSTICS]
    packet = DiagnosticPacket(
        now_ms,
        "FAIL" if active or any(not g.passed for g in gates) else "PASS",
        active,
        gates,
        facts,
        truncated=truncated,
    )
    packet.to_json()  # enforce the byte bound before returning the packet
    return ObservabilitySnapshot(now_ms, tuple(metrics), events, packet)


def _collect_events(conn: sqlite3.Connection) -> tuple[StructuredEvent, ...]:
    rows = conn.execute(
        "SELECT t.transition_id, t.created_at_ms, r.project_id, t.run_id, t.trigger_kind, "
        "t.next_state, t.specification_generation FROM transitions t JOIN runs r USING (run_id) "
        "ORDER BY t.created_at_ms, t.transition_id"
    )
    events: list[StructuredEvent] = [
        StructuredEvent(
            _event_id("transition", str(row[0])),
            "workflow.transition",
            int(row[1]),
            _safe_scope(str(row[2])),
            str(row[0]),
            (
                ("run_id", str(row[3])),
                ("trigger_kind", str(row[4])),
                ("state", str(row[5])),
                ("generation", int(row[6])),
            ),
        )
        for row in rows
    ]

    for table, receipt_kind in (
        ("review_receipts", "review"),
        ("adjudication_receipts", "adjudication"),
    ):
        for row in conn.execute(
            f"SELECT receipt_id, accepted_at_ms, run_id, activity_id FROM {table} "
            "ORDER BY accepted_at_ms, receipt_id"
        ):
            events.append(
                StructuredEvent(
                    _event_id(f"{receipt_kind}-receipt", str(row[0])),
                    "workflow.receipt.accepted",
                    int(row[1]),
                    None,
                    str(row[0]),
                    (("activity_id", str(row[3])), ("receipt_kind", receipt_kind)),
                )
            )

    for row in conn.execute(
        "SELECT e.publication_id, e.effect_generation, e.created_at_ms, r.project_id, "
        "e.mode, e.activity_id FROM publication_effects e "
        "JOIN publications p USING (publication_id) JOIN runs r USING (run_id) "
        "ORDER BY e.created_at_ms, e.publication_id, e.effect_generation"
    ):
        durable_id = f"{row[0]}/{row[1]}"
        events.append(
            StructuredEvent(
                _event_id("publication-effect", durable_id),
                "workflow.publication.effect",
                int(row[2]),
                _safe_scope(str(row[3])),
                str(row[5]),
                (("effect_generation", int(row[1])), ("mode", str(row[4]))),
            )
        )

    for row in conn.execute(
        "SELECT forge_request_failure_fact_id, recorded_at_ms, failure_kind, "
        "forge_observation_request_id FROM forge_request_failure_facts "
        "ORDER BY recorded_at_ms, forge_request_failure_fact_id"
    ):
        events.append(
            StructuredEvent(
                _event_id("forge-failure", str(row[0])),
                "workflow.forge.failure",
                int(row[1]),
                None,
                str(row[0]),
                (("failure_kind", str(row[2])), ("request_id", str(row[3]))),
            )
        )

    for row in conn.execute(
        "SELECT terminal_duplicate_cleanup_action_id, created_at_ms, state, planned_action "
        "FROM terminal_duplicate_cleanup_actions "
        "ORDER BY created_at_ms, terminal_duplicate_cleanup_action_id"
    ):
        events.append(
            StructuredEvent(
                _event_id("duplicate-repair", str(row[0])),
                "workflow.duplicate.repair",
                int(row[1]),
                None,
                str(row[0]),
                (("action", str(row[3])), ("state", str(row[2]))),
            )
        )

    events.sort(key=lambda item: (item.occurred_at_ms, item.event_id))
    return tuple(events)


def _level(value: int, warning: int, critical: int) -> tuple[str, bool, int]:
    if value >= critical:
        return "PAGE", True, critical
    if value >= warning:
        return "WARNING", True, warning
    return "WARNING", False, warning


def _alerts(**kw: Any) -> tuple[Alert, ...]:
    t: ObservabilityThresholds = kw["thresholds"]
    alerts: list[Alert] = []
    for code, value, warning, critical, owner in (
        (
            "OUTBOX_AGE",
            kw["oldest_outbox"],
            t.outbox_warning_ms,
            t.outbox_critical_ms,
            "controller-oncall",
        ),
        (
            "PUBLICATION_STALLED",
            kw["publication_age"],
            t.publication_warning_ms,
            t.publication_critical_ms,
            "release-oncall",
        ),
        (
            "CAPACITY_REPORT_STALE",
            kw["capacity_age"],
            t.report_warning_ms,
            t.report_critical_ms,
            "fleet-oncall",
        ),
        (
            "TIMER_LAG",
            kw["due_lag"],
            t.timer_lag_warning_ms,
            t.timer_lag_critical_ms,
            "controller-oncall",
        ),
    ):
        severity, active, threshold = _level(int(value), warning, critical)
        alerts.append(Alert(code, severity, owner, active, int(value), threshold))
    page_checks = (
        ("STORAGE_INTEGRITY", not kw["integrity_ok"], "storage-oncall"),
        ("COMPETING_WRITER", kw["competing_writer"], "controller-oncall"),
        ("AUDIT_WRITE_FAILURE", not kw["audit_write_ok"], "security-oncall"),
        ("REDIS_REBUILD_FAILURE", not kw["redis_rebuild_ok"], "controller-oncall"),
        ("MISSING_LIVE_OBJECT", kw["missing_candidates"] > 0, "storage-oncall"),
        ("PUBLICATION_OWNERSHIP_CONFLICT", kw["ownership_conflicts"] > 0, "release-oncall"),
    )
    alerts.extend(
        Alert(code, "PAGE", owner, bool(active), int(bool(active)), 1)
        for code, active, owner in page_checks
    )
    if kw["disk_free_bytes"] is not None:
        active = int(kw["disk_free_bytes"]) < int(kw["disk_critical_bytes"])
        alerts.append(
            Alert(
                "DISK_PRESSURE",
                "PAGE",
                "storage-oncall",
                active,
                int(kw["disk_free_bytes"]),
                int(kw["disk_critical_bytes"]),
            )
        )
    for code, value, warning, critical in (
        ("BACKUP_AGE", kw["backup_age_ms"], t.backup_warning_ms, t.backup_critical_ms),
        (
            "RESTORE_DRILL_AGE",
            kw["restore_drill_age_ms"],
            t.restore_drill_warning_ms,
            t.restore_drill_critical_ms,
        ),
    ):
        if value is not None:
            severity, active, threshold = _level(int(value), warning, critical)
            alerts.append(Alert(code, severity, "storage-oncall", active, int(value), threshold))
    return tuple(alerts)


def _gates(**kw: Any) -> tuple[ReleaseGate, ...]:
    t: ObservabilityThresholds = kw["thresholds"]
    stage0 = (
        kw["integrity_ok"]
        and kw["redis_rebuild_ok"]
        and kw["backup_age_ms"] is not None
        and kw["backup_age_ms"] < t.backup_critical_ms
        and kw["restore_drill_age_ms"] is not None
        and kw["restore_drill_age_ms"] < t.restore_drill_critical_ms
    )
    stage1 = kw["receipt_total"] > 0 and kw["consensus_total"] > 0
    stage2 = kw["oldest_outbox"] < t.outbox_critical_ms
    stage3 = (
        kw["ownership_conflicts"] == 0
        and kw["duplicate_active"] == 0
        and kw["publication_age"] < t.publication_critical_ms
    )
    return (
        ReleaseGate(
            0, "DURABLE_FOUNDATION", stage0, "integrity+redis-rebuild+backup+restore-drill"
        ),
        ReleaseGate(1, "SYNTHETIC_PROTOCOL", stage1, "accepted-receipt+consensus"),
        ReleaseGate(2, "PREPUBLICATION_PILOT", stage2, "bounded-outbox-age"),
        ReleaseGate(3, "PUBLICATION_PILOT", stage3, "ownership+duplicate-repair+latency"),
        ReleaseGate(4, "COHORT_EXPANSION", stage3 and stage2, "publication-pilot+queue-health"),
        ReleaseGate(
            5, "LEGACY_RETIREMENT", stage0 and stage1 and stage2 and stage3, "all-prior-gates"
        ),
    )
