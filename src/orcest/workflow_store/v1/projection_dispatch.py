"""Projection Outbox delivery and rebuild (forge-integration.md "Projection
reconciliation").

Forge labels, status comments, and Checks are Projections, never workflow
authority (architecture.md "Authority boundary"): they are derived, at-least-
once, idempotently reconciled reflections of the durable Run pointed to by
``ProjectionOutbox.kind = RUN_STATUS`` rows the reducer already writes
(:func:`orcest.workflow_reducer.ledger.apply`). This module is the delivery
side -- draining pending rows into the forge -- plus the rebuild side that
replays the latest durable row straight from SQLite so deleting every forge
projection, or the forge drifting out from under one, never loses Run state
or blocks the reducer.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from orcest.workflow_store.store import ProjectionOutboxRecord, RunStore

__all__ = [
    "ForgeProjectionAdapter",
    "ProjectionDeliveryError",
    "ProjectionRebuildResult",
    "dispatch_pending_projections",
    "extract_projection_marker",
    "rebuild_all_projections",
    "rebuild_run_projection",
    "render_projection_marker",
]

_MARKER_RE = re.compile(r"<!-- orcest-projection:(?P<kind>[A-Z_]+):(?P<run_id>[0-9a-f-]{36}) -->")


def render_projection_marker(*, run_id: str, projection_kind: str) -> str:
    """The hidden marker every Orcest status comment MUST carry
    (forge-integration.md "Projection reconciliation"): it names the Run ID
    and projection kind so the adapter can find and replace its own prior
    comment instead of appending an unbounded history.
    """
    return f"<!-- orcest-projection:{projection_kind}:{run_id} -->"


def extract_projection_marker(text: str) -> tuple[str, str] | None:
    """``(projection_kind, run_id)`` from a marker rendered by
    :func:`render_projection_marker`, or ``None`` if ``text`` carries none."""
    match = _MARKER_RE.search(text)
    if match is None:
        return None
    return match.group("kind"), match.group("run_id")


class ProjectionDeliveryError(Exception):
    """A forge projection mutation failed transiently. The row this raised
    for stays ``PENDING`` and is retried later; it MUST NOT be raised for a
    permanent/programming error, which should surface instead so it is not
    silently retried forever.
    """


class ForgeProjectionAdapter(Protocol):
    """The narrow ``mutate_projection`` forge capability
    (forge-integration.md "Adapter boundary"): add/remove configured labels
    and write an idempotently marked status comment. Implementations own
    finding and replacing their own prior comment via
    :func:`extract_projection_marker`/:func:`render_projection_marker` and
    must be safe to call more than once with the same payload.
    """

    def mutate_projection(
        self,
        *,
        target_kind: str,
        target_id: str,
        run_id: str,
        projection_kind: str,
        payload: Mapping[str, Any],
    ) -> None: ...


@dataclass(frozen=True, slots=True)
class ProjectionRebuildResult:
    run_id: str
    rebuilt: bool
    projection_outbox_id: str | None = None


def _deliver(store: RunStore, adapter: ForgeProjectionAdapter, row: ProjectionOutboxRecord) -> None:
    payload = json.loads(row.payload_json)
    adapter.mutate_projection(
        target_kind=row.target_kind,
        target_id=row.target_id,
        run_id=row.run_id,
        projection_kind=row.kind,
        payload=payload,
    )
    store.mark_projection_outbox_delivered(row.projection_outbox_id)


def dispatch_pending_projections(
    store: RunStore, adapter: ForgeProjectionAdapter, *, limit: int = 100
) -> int:
    """Drain due ``PENDING`` Projection Outbox rows into the forge; mark
    each ``DELIVERED`` on success. A transient :class:`ProjectionDeliveryError`
    for one row leaves it ``PENDING`` for a later pass and does not stop the
    rest of the batch -- projection delivery loss never blocks or rewinds
    the reducer, which already committed its Transition durably.
    """
    dispatched = 0
    for row in store.list_pending_projection_outbox(limit=limit):
        current = store.get_projection_outbox(row.projection_outbox_id)
        if current is None or current.state != "PENDING":
            continue
        try:
            _deliver(store, adapter, current)
        except ProjectionDeliveryError:
            continue
        dispatched += 1
    return dispatched


def rebuild_run_projection(
    store: RunStore,
    adapter: ForgeProjectionAdapter,
    run_id: str,
    *,
    kind: str = "RUN_STATUS",
    target_kind: str = "WORK_ITEM",
) -> ProjectionRebuildResult:
    """Recompute and unconditionally redeliver one Run's current desired
    forge projection straight from its latest durable Projection Outbox row
    -- SQLite remains authority even after the forge label/status comment
    was deleted or drifted. Unlike :func:`dispatch_pending_projections`,
    delivery here is explicit operator/reconciliation-triggered work: a
    failure propagates instead of being silently retried later.
    """
    latest = store.get_latest_projection_outbox_for_run(run_id, kind=kind, target_kind=target_kind)
    if latest is None:
        return ProjectionRebuildResult(run_id=run_id, rebuilt=False)
    _deliver(store, adapter, latest)
    return ProjectionRebuildResult(
        run_id=run_id, rebuilt=True, projection_outbox_id=latest.projection_outbox_id
    )


def rebuild_all_projections(
    store: RunStore,
    adapter: ForgeProjectionAdapter,
    *,
    kind: str = "RUN_STATUS",
    target_kind: str = "WORK_ITEM",
) -> list[ProjectionRebuildResult]:
    """:func:`rebuild_run_projection` for every Run that has ever had a
    durable projection of this kind/target. One Run's delivery failure is
    recorded and does not stop the sweep from reconciling the rest.
    """
    results: list[ProjectionRebuildResult] = []
    for run_id in store.list_run_ids_with_projection_outbox(kind=kind, target_kind=target_kind):
        try:
            results.append(
                rebuild_run_projection(store, adapter, run_id, kind=kind, target_kind=target_kind)
            )
        except ProjectionDeliveryError:
            results.append(ProjectionRebuildResult(run_id=run_id, rebuilt=False))
    return results
