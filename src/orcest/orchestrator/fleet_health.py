"""FleetHealthMonitor: fleet-wide pressure detector + kill-budget limit mirror.

Background thread mirroring ``EventRelay``/``TraceArchiver``'s lifecycle
(``start``/``stop``/``_run`` daemon thread, ``_pass_once`` driving one unit
of work). Reads new entries from the prefixed ``events`` spool stream (see
``orcest.shared.events``), tracks a rolling window of distinct tasks that
have emitted a ``net.orcest.task.suspect`` envelope, and when enough
distinct tasks are concurrently suspect within ``pressure_window`` seconds,
opens the fleet-wide kill-pressure gate (Redis key ``orcest:fleet:pressure``)
that ``LivenessTracker`` on every worker consults before escalating a
STUCK/LOOPING kill (spec: fleet kill-pressure gate, global-constraints.md).

This module also mirrors the configured ``max_kills_per_hour`` into the
global ``orcest:fleet:kill_budget:limit`` key so workers -- which have no
access to orchestrator config -- can read the fleet-wide hourly kill budget.

Persists a cursor at Redis key ``fleet_health:cursor`` (this monitor's own,
distinct from the event relay's) so restarts resume where they left off.
Malformed spool entries (missing/invalid ``envelope`` JSON, or a
``task.suspect`` envelope missing ``subject``/``time``) are skipped with a
log; the cursor still advances past them so a single bad entry can never
wedge the monitor. Redis/emit failures are logged and swallowed so a
transient Redis blip can never kill the background thread.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from collections import deque
from collections.abc import Callable
from datetime import datetime
from typing import Any

from orcest.shared.events import EVENTS_STREAM, EventPublisher, make_event
from orcest.shared.redis_client import RedisClient

logger = logging.getLogger(__name__)

_CURSOR_KEY = "fleet_health:cursor"
_XREAD_COUNT = 500
_SLEEP_BETWEEN_PASSES_SECONDS = 1.0

# Global (cross-project, unprefixed) Redis keys -- see global-constraints.md.
_PRESSURE_KEY = "orcest:fleet:pressure"
_KILL_BUDGET_LIMIT_KEY = "orcest:fleet:kill_budget:limit"
_KILL_BUDGET_LIMIT_TTL_SECONDS = 7 * 24 * 3600

_SUSPECT_EVENT_TYPE = "net.orcest.task.suspect"
_PRESSURE_EVENT_TYPE = "net.orcest.fleet.pressure"


def _parse_rfc3339(value: str) -> float:
    """Parse a CloudEvents ``time`` field (RFC3339 UTC, e.g. ``...Z``) to epoch seconds."""
    v = value[:-1] + "+00:00" if value.endswith("Z") else value
    return datetime.fromisoformat(v).timestamp()


class FleetHealthMonitor:
    """Background thread detecting fleet-wide kill pressure from task.suspect events."""

    def __init__(
        self,
        redis: RedisClient,
        *,
        pressure_min_tasks: int,
        pressure_window: int,
        pressure_hold: int,
        max_kills_per_hour: int,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._redis = redis
        self._publisher = EventPublisher(redis)
        self._pressure_min_tasks = pressure_min_tasks
        self._pressure_window = pressure_window
        self._pressure_hold = pressure_hold
        self._max_kills_per_hour = max_kills_per_hour
        self._clock = clock
        # deque of (unix_ts, task_id) for observed task.suspect envelopes,
        # oldest first (entries arrive in spool/cursor order).
        self._suspects: deque[tuple[float, str]] = deque()
        self._shutdown = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        logger.info("Fleet health monitor starting.")
        # Mirror synchronously at startup so the limit is visible to workers
        # immediately, without waiting for the first background pass.
        self._mirror_kill_budget_limit()
        self._thread = threading.Thread(target=self._run, name="fleet-health", daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        self._shutdown.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)

    def _run(self) -> None:
        while not self._shutdown.is_set():
            try:
                self._pass_once()
            except Exception:
                logger.error("Fleet health pass raised", exc_info=True)
            self._shutdown.wait(timeout=_SLEEP_BETWEEN_PASSES_SECONDS)

    def _pass_once(self) -> None:
        """Drain new spool entries, age out the suspect window, evaluate pressure.

        Also refreshes the kill-budget limit mirror on every pass.
        """
        self._mirror_kill_budget_limit()

        cursor = self._redis.get(_CURSOR_KEY) or "0-0"
        entries = self._redis.xread_after(EVENTS_STREAM, cursor, _XREAD_COUNT)
        if entries:
            last_id = cursor
            for entry_id, fields in entries:
                last_id = entry_id
                self._process_entry(entry_id, fields)
            self._redis.set_value(_CURSOR_KEY, last_id)

        self._age_out()
        self._evaluate_pressure()

    def _process_entry(self, entry_id: str, fields: dict[str, str]) -> None:
        raw = fields.get("envelope")
        if not raw:
            logger.warning("Skipping spool entry %s: missing envelope field", entry_id)
            return
        try:
            envelope: dict[str, Any] = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            logger.warning("Skipping malformed spool entry %s", entry_id, exc_info=True)
            return
        if envelope.get("type") != _SUSPECT_EVENT_TYPE:
            return

        task_id = envelope.get("subject")
        time_str = envelope.get("time")
        if not task_id or not time_str:
            logger.warning(
                "Skipping malformed task.suspect envelope %s: missing subject/time", entry_id
            )
            return
        try:
            ts = _parse_rfc3339(time_str)
        except (ValueError, TypeError):
            logger.warning(
                "Skipping task.suspect envelope %s: unparsable time %r", entry_id, time_str
            )
            return
        self._suspects.append((ts, task_id))

    def _age_out(self) -> None:
        cutoff = self._clock() - self._pressure_window
        while self._suspects and self._suspects[0][0] < cutoff:
            self._suspects.popleft()

    def _evaluate_pressure(self) -> None:
        distinct_task_ids = sorted({task_id for _, task_id in self._suspects})
        if len(distinct_task_ids) < self._pressure_min_tasks:
            return

        try:
            already_held = self._redis.get_raw(_PRESSURE_KEY) is not None
        except Exception:
            logger.warning("Fleet pressure key read failed", exc_info=True)
            return

        try:
            self._redis.set_ex_raw(_PRESSURE_KEY, "1", self._pressure_hold)
        except Exception:
            logger.warning("Fleet pressure key write failed", exc_info=True)
            return

        if already_held:
            # Condition persists across passes -- TTL refreshed above, but
            # the event fires only once per pressure episode.
            return

        envelope = make_event(
            _PRESSURE_EVENT_TYPE,
            source_project=self._redis.key_prefix or "fleet",
            task_id="fleet",
            repo="",
            resource_type="",
            resource_id=0,
            attempt=0,
            data={"suspect_tasks": distinct_task_ids, "window_seconds": self._pressure_window},
        )
        try:
            self._publisher.publish(envelope)
        except Exception:
            logger.warning("Fleet pressure event publish failed", exc_info=True)

    def _mirror_kill_budget_limit(self) -> None:
        try:
            self._redis.set_ex_raw(
                _KILL_BUDGET_LIMIT_KEY,
                str(self._max_kills_per_hour),
                _KILL_BUDGET_LIMIT_TTL_SECONDS,
            )
        except Exception:
            logger.warning("Kill-budget limit mirror write failed", exc_info=True)
