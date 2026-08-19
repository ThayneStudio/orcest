"""Event relay: drains the events spool to the monitor ingest endpoint.

Background thread mirroring ``TraceArchiver``'s lifecycle (``start``/``stop``/
``_run`` daemon thread). Reads new entries from the prefixed ``events`` spool
stream (see ``orcest.shared.events``), POSTs them in batches to the monitor's
``/ingest/v1/events`` endpoint, and persists a cursor at Redis key
``event_relay:cursor`` so restarts resume where they left off.

``EventRelay.start()`` is a no-op (logged) when ``ingest_url`` is ``None`` --
the orchestrator otherwise unaffected, mirroring the trace archiver's
"unset path disables the feature" pattern.

Malformed spool entries (missing/invalid ``envelope`` JSON) are skipped with
a log; the cursor still advances past them so a single bad entry can never
wedge the relay.
"""

from __future__ import annotations

import json
import logging
import threading
from typing import Any

import requests

from orcest.shared.events import EVENTS_STREAM
from orcest.shared.redis_client import RedisClient

logger = logging.getLogger(__name__)

_CURSOR_KEY = "event_relay:cursor"
_XREAD_COUNT = 500
_SLEEP_BETWEEN_PASSES_SECONDS = 1.0
_INITIAL_BACKOFF_SECONDS = 1.0
_MAX_BACKOFF_SECONDS = 60.0
_POST_TIMEOUT_SECONDS = 10


def _entry_id_tuple(entry_id: str) -> tuple[int, int]:
    """Parse a Redis stream entry id ("<ms>-<seq>") into a comparable tuple."""
    ms, _, seq = entry_id.partition("-")
    return (int(ms), int(seq or 0))


class EventRelay:
    """Background thread that drains the events spool to the monitor."""

    def __init__(
        self,
        redis: RedisClient,
        ingest_url: str | None,
        write_token: str,
        project_prefixes: list[str] | None = None,
    ) -> None:
        self._redis = redis
        self._ingest_url = ingest_url
        self._write_token = write_token
        self._project_prefixes = project_prefixes or []
        self._shutdown = threading.Event()
        self._thread: threading.Thread | None = None
        # Exponential backoff on POST failure, reset to the initial value on
        # any 2xx response. Also doubles as the idle-pass sleep interval
        # (both start at 1s per the spec).
        self._backoff = _INITIAL_BACKOFF_SECONDS
        # Entry ids we've already logged a "malformed spool entry" warning
        # for. While the POST for a batch keeps failing, the cursor doesn't
        # advance, so the same malformed entries would otherwise be re-read
        # (and re-warned) on every retry pass. Bounded: pruned in _pass_once
        # whenever the cursor advances past an id, so this never grows past
        # the current unacknowledged batch.
        self._warned_malformed_ids: set[str] = set()

    def start(self) -> None:
        if self._ingest_url is None:
            logger.info("Event relay disabled (monitor_ingest_url unset).")
            return
        logger.info("Event relay enabled, forwarding events to %s", self._ingest_url)
        self._warn_on_misconfiguration()
        self._thread = threading.Thread(target=self._run, name="event-relay", daemon=True)
        self._thread.start()

    def _warn_on_misconfiguration(self) -> None:
        """Loudly warn about configurations that would silently drop events.

        This relay only ever drains the ``events`` spool stream under its own
        Redis client's key prefix. Any configured project whose key_prefix
        differs from that would have its events published to a stream this
        relay never reads (worker/pool events are spooled under
        ``task.key_prefix:events`` -- see EventPublisher usage in
        worker/loop.py and fleet/pool_manager.py). Also warn if a write
        token is required but empty, since every POST would then 401.
        """
        relay_prefix = self._redis.key_prefix
        mismatched = sorted({p for p in self._project_prefixes if p and p != relay_prefix})
        if mismatched:
            logger.warning(
                "Event relay is configured to drain Redis key prefix %r, but "
                "project(s) with key_prefix %s are configured -- events "
                "published under those prefixes will NEVER be relayed to the "
                "monitor. Run one EventRelay per project key_prefix, or align "
                "redis.key_prefix with the project(s) it should drain.",
                relay_prefix,
                mismatched,
            )
        if not self._write_token:
            logger.warning(
                "Event relay ingest_url is set but the write token is empty -- "
                "every POST to %s will be rejected with 401.",
                self._ingest_url,
            )

    def stop(self, timeout: float = 5.0) -> None:
        self._shutdown.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)

    def _run(self) -> None:
        while not self._shutdown.is_set():
            try:
                ok = self._pass_once()
            except Exception:
                logger.error("Event relay pass raised", exc_info=True)
                ok = False
            self._backoff = (
                _INITIAL_BACKOFF_SECONDS if ok else min(self._backoff * 2, _MAX_BACKOFF_SECONDS)
            )
            self._shutdown.wait(timeout=self._backoff)

    def _pass_once(self) -> bool:
        """Read up to one batch of spool entries and POST them.

        Returns True if this pass made forward progress cleanly (nothing to
        do, or the POST succeeded), False on a POST failure so the caller can
        back off.
        """
        cursor = self._redis.get(_CURSOR_KEY) or "0-0"
        # xread_after returns [] both when there is genuinely nothing new and
        # when the underlying Redis call errored (RedisClient.xread_after
        # swallows connection errors to []). We treat both cases as "idle"
        # and just retry on the next pass -- acceptable because the outer
        # backoff loop still polls at a bounded (<=60s) cadence, but it means
        # a Redis outage is invisible here rather than surfaced as a failure.
        entries = self._redis.xread_after(EVENTS_STREAM, cursor, _XREAD_COUNT)
        if not entries:
            return True

        events: list[dict[str, Any]] = []
        last_id = cursor
        for entry_id, fields in entries:
            last_id = entry_id
            raw = fields.get("envelope")
            if not raw:
                logger.warning("Skipping spool entry %s: missing envelope field", entry_id)
                continue
            try:
                events.append(json.loads(raw))
            except (json.JSONDecodeError, TypeError):
                # While the POST for this batch keeps failing, the cursor
                # doesn't advance and the same malformed entry gets re-read
                # (and would otherwise be re-warned) every retry pass. Log
                # each entry id at most once.
                if entry_id not in self._warned_malformed_ids:
                    self._warned_malformed_ids.add(entry_id)
                    logger.warning("Skipping malformed spool entry %s", entry_id, exc_info=True)

        if not events:
            # Every entry in this batch was malformed. Still advance the
            # cursor past them -- otherwise a single bad entry wedges the
            # relay on every pass forever.
            self._redis.set_value(_CURSOR_KEY, last_id)
            self._prune_warned_malformed_ids(last_id)
            return True

        if not self._post(events):
            return False

        self._redis.set_value(_CURSOR_KEY, last_id)
        self._prune_warned_malformed_ids(last_id)
        return True

    def _prune_warned_malformed_ids(self, cursor: str) -> None:
        """Drop tracked malformed-entry ids once the cursor advances past them.

        Keeps ``_warned_malformed_ids`` bounded to the current unacknowledged
        batch instead of growing for the life of the process.
        """
        if not self._warned_malformed_ids:
            return
        cursor_tuple = _entry_id_tuple(cursor)
        self._warned_malformed_ids = {
            eid for eid in self._warned_malformed_ids if _entry_id_tuple(eid) > cursor_tuple
        }

    def _post(self, events: list[dict[str, Any]]) -> bool:
        # `start()` refuses to launch the relay thread when ingest_url is
        # None, so reaching here without a URL means the relay was driven
        # directly (a caller bypassing start()). Report it rather than
        # handing None to requests.post, which would raise a MissingSchema
        # deep inside urllib3 instead of naming the real problem.
        ingest_url = self._ingest_url
        if ingest_url is None:
            logger.error("Event relay _post called with no ingest URL configured; dropping batch")
            return False
        try:
            resp = requests.post(
                ingest_url,
                json={"events": events},
                headers={"Authorization": f"Bearer {self._write_token}"},
                timeout=_POST_TIMEOUT_SECONDS,
            )
        except requests.RequestException:
            logger.warning("Event relay POST to %s failed", ingest_url, exc_info=True)
            return False
        if 200 <= resp.status_code < 300:
            return True
        logger.warning("Event relay POST to %s returned status %s", ingest_url, resp.status_code)
        return False
