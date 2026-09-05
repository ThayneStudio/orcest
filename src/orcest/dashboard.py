"""Live TUI dashboard for orcest system monitoring.

Provides a top-like full-screen terminal interface that auto-refreshes
every few seconds, showing queue depths, active locks, consumer groups,
and recent task results.  Press 'w' to view live worker output.

Usage:
    orcest status          # launches the TUI
    orcest status --once   # single-shot output (old behavior)
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import redis as redis_lib
from rich.markup import escape as rich_escape

from orcest.shared.models import DEAD_LETTER_STREAM, RESULTS_STREAM
from orcest.shared.output_streams import is_output_continuation
from orcest.shared.provider_stream_health import (
    STREAM_HEALTH_KEY_PREFIX,
    ProviderStreamHealth,
    StreamHealthState,
)
from orcest.shared.redis_client import RedisClient
from orcest.shared.result_stream_health import (
    ResultStreamHealth,
    format_result_stream_metrics,
    format_result_stream_warning,
    inspect_result_stream,
    unavailable_result_stream_health,
)

# Global (cross-project, unprefixed) key prefix PoolManager publishes canonical
# per-stream health snapshots under (``provider-stream-health:{provider}:pr``
# and ``...:issue``). This module only ever reads that published state, it
# never recomputes health itself.

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data layer — testable without Textual
# ---------------------------------------------------------------------------


@dataclass
class LockInfo:
    pr: str
    owner: str
    ttl: int


@dataclass
class ConsumerGroupInfo:
    stream: str
    name: str
    consumers: int
    pending: int


@dataclass
class RecentResult:
    task_id: str
    worker_id: str
    status: str
    resource_type: str
    resource_id: str
    duration_seconds: int
    summary: str


@dataclass
class DeadLetterEntry:
    entry_id: str
    task_type: str
    repo: str
    resource_type: str
    resource_id: str
    timestamp_ms: int | None
    reason: str | None


@dataclass
class SystemSnapshot:
    """Complete point-in-time view of orcest system state."""

    redis_ok: bool
    fetched_at: datetime
    # Always populated. A disconnected snapshot carries an explicit unavailable
    # value, so renderers never need an unreachable Redis-OK/None fallback.
    result_stream_health: ResultStreamHealth
    queue_depths: dict[str, int] = field(default_factory=dict)
    results_depth: int = 0
    dead_letter_count: int = 0
    locks: list[LockInfo] = field(default_factory=list)
    consumer_groups: list[ConsumerGroupInfo] = field(default_factory=list)
    recent_results: list[RecentResult] = field(default_factory=list)
    attempt_counts: dict[str, int] = field(default_factory=dict)
    dead_letter_entries: list[DeadLetterEntry] = field(default_factory=list)
    provider_health: dict[str, dict[str, int]] = field(default_factory=dict)
    # Task 8: per-provider counters e.g. "claude": {"exhausted_skip": N, "rebake..."}
    stream_health: list[ProviderStreamHealth] = field(default_factory=list)
    # issue #613/#639: canonical per-stream snapshots published by PoolManager.


def fetch_snapshot(redis: RedisClient, max_results: int = 20) -> SystemSnapshot:
    """Query Redis and return a complete system snapshot."""
    if not redis.health_check():
        return SystemSnapshot(
            redis_ok=False,
            fetched_at=datetime.now(timezone.utc),
            result_stream_health=unavailable_result_stream_health(redis._prefixed(RESULTS_STREAM)),
        )

    try:
        return _fetch_snapshot_inner(redis, max_results)
    except (
        redis_lib.ConnectionError,
        redis_lib.TimeoutError,
        redis_lib.AuthenticationError,
    ):
        logger.warning("Redis disconnected during snapshot fetch", exc_info=True)
        return SystemSnapshot(
            redis_ok=False,
            fetched_at=datetime.now(timezone.utc),
            result_stream_health=unavailable_result_stream_health(redis._prefixed(RESULTS_STREAM)),
        )


def _fetch_snapshot_inner(redis: RedisClient, max_results: int) -> SystemSnapshot:
    """Build the snapshot after health_check has passed.

    Separated from fetch_snapshot so the outer function can catch
    connection-level errors that occur after the health check.
    """
    # Discover task streams
    task_streams = redis.scan_iter(match="tasks:*")

    result_stream_health = inspect_result_stream(redis)
    results_depth = result_stream_health.retained_entries

    try:
        dead_letter_count: int = redis.xlen(DEAD_LETTER_STREAM)
    except redis_lib.ResponseError:
        dead_letter_count = 0

    # Dead-letter entries (most recent first, up to 5)
    dead_letter_entries: list[DeadLetterEntry] = []
    try:
        dl_raw: list[Any] = redis.xrevrange(DEAD_LETTER_STREAM, count=5)
    except redis_lib.ResponseError:
        dl_raw = []
    for entry_id, fields in dl_raw:
        try:
            ms = int(entry_id.split("-")[0])
        except (ValueError, IndexError):
            ms = None
        dead_letter_entries.append(
            DeadLetterEntry(
                entry_id=entry_id,
                task_type=fields.get("type", "?"),
                repo=fields.get("repo", "?"),
                resource_type=fields.get("resource_type", "?"),
                resource_id=fields.get("resource_id", "?"),
                timestamp_ms=ms,
                reason=fields.get("dead_letter_reason"),
            )
        )

    # Active locks
    lock_keys = redis.scan_iter(match="lock:pr:*")
    locks = []
    for key in lock_keys:
        owner: str = redis.get(key) or "(expired)"
        ttl: int = redis.ttl(key)
        pr_num = key.removeprefix("lock:pr:")
        locks.append(LockInfo(pr=pr_num, owner=owner, ttl=ttl))

    # Consumer groups + real queue depths (pending + lag, not XLEN)
    consumer_groups = []
    queue_depths: dict[str, int] = {}
    for stream_key in sorted(task_streams):
        try:
            for g in redis.xinfo_groups(stream_key):
                consumer_groups.append(
                    ConsumerGroupInfo(
                        stream=stream_key,
                        name=g["name"],
                        consumers=g["consumers"],
                        pending=g["pending"],
                    )
                )
                pending = g.get("pending", 0)
                lag = max(g.get("lag") or 0, 0)
                queue_depths[stream_key] = queue_depths.get(stream_key, 0) + pending + lag
        except redis_lib.ResponseError:
            pass  # Stream has no consumer groups

    # Recent results (most recent first)
    recent_results = []
    try:
        entries: list[Any] = redis.xrevrange("results", count=max_results)
    except redis_lib.ResponseError:
        entries = []
    for _entry_id, fields in entries:
        try:
            recent_results.append(
                RecentResult(
                    task_id=fields.get("task_id", ""),
                    worker_id=fields.get("worker_id", ""),
                    status=fields.get("status", ""),
                    resource_type=fields.get("resource_type", ""),
                    resource_id=fields.get("resource_id", ""),
                    duration_seconds=int(fields.get("duration_seconds", 0)),
                    summary=fields.get("summary", ""),
                )
            )
        except (ValueError, TypeError):
            logger.debug("Skipping malformed result entry: %s", _entry_id)

    # Attempt counters
    attempt_counts = {}
    attempt_keys = redis.scan_iter(match="pr:*:*:attempts")
    for key in attempt_keys:
        data: dict[str, str] = redis.hgetall(key)
        if data:
            # Key format: "pr:<repo>:<number>:attempts"
            inner = key.removeprefix("pr:").removesuffix(":attempts")
            # inner is "<repo>:<number>" — extract the PR number (last segment)
            parts = inner.rsplit(":", 1)
            pr_num = parts[-1] if parts else inner
            try:
                attempt_counts[f"PR #{pr_num}"] = int(data.get("count", 0))
            except (ValueError, TypeError):
                pass

    # Provider health counters (Task 8).
    #
    # NOTE on the scan pattern: RedisClient.scan_iter(match=...) auto-prefixes
    # the pattern with this client's bound project prefix and strips that prefix
    # from the returned keys (see RedisClient.scan_iter). So "providers:*" here
    # matches `{prefix}:providers:*` in the raw Redis keyspace and returns
    # already-prefix-stripped keys like "providers:claude:rebake_required_failures".
    # This is intentionally single-project (this CLI binds to one --prefix); the
    # TypeScript dashboard uses "*:providers:*" because that runtime operates
    # multi-project on the raw keyspace without auto-prefixing.
    provider_health: dict[str, dict[str, int]] = {}
    try:
        prov_keys = redis.scan_iter(match="providers:*")
        for k in prov_keys:
            if not k.startswith("providers:"):
                continue
            val = redis.get(k) or "0"
            # key format after strip: "providers:{prov}:{metric}"
            parts = k.split(":", 2)
            if len(parts) == 3:
                _, prov, metric = parts
                if prov not in provider_health:
                    provider_health[prov] = {}
                try:
                    provider_health[prov][metric] = int(val)
                except (ValueError, TypeError):
                    pass
    except Exception:
        logger.debug("Failed to collect provider health counters", exc_info=True)

    # Stream health (issue #613/#639): read-only consumer of PoolManager's
    # canonical, unprefixed per-stream state keys. This CLI never
    # recomputes stranded-stream health, only renders what was published.
    # A malformed record for one stream is skipped so its sibling still
    # appears in the snapshot.
    stream_health: list[ProviderStreamHealth] = []
    try:
        for raw_key in redis.client.scan_iter(match=f"{STREAM_HEALTH_KEY_PREFIX}*"):
            key = raw_key.decode() if isinstance(raw_key, bytes) else str(raw_key)
            raw_value = redis.get_raw(key)
            if raw_value is None:
                continue
            try:
                stream_health.append(ProviderStreamHealth.from_dict(json.loads(raw_value)))
            except (TypeError, ValueError, KeyError):
                logger.debug("Malformed stream health record at %s", key)
    except Exception:
        logger.debug("Failed to collect provider stream health", exc_info=True)
    stream_health.sort(key=lambda h: (h.provider, h.stream))

    return SystemSnapshot(  # noqa: E501
        redis_ok=True,
        fetched_at=datetime.now(timezone.utc),
        queue_depths=queue_depths,
        results_depth=results_depth,
        result_stream_health=result_stream_health,
        dead_letter_count=dead_letter_count,
        locks=locks,
        consumer_groups=consumer_groups,
        recent_results=recent_results,
        attempt_counts=attempt_counts,
        dead_letter_entries=dead_letter_entries,
        provider_health=provider_health,
        stream_health=stream_health,
    )


def discover_workers(redis: RedisClient) -> list[str]:
    """Return sorted list of worker IDs that have output streams."""
    try:
        streams = redis.scan_iter(match="output:*")
        return sorted(s.removeprefix("output:") for s in streams)
    except (
        redis_lib.ConnectionError,
        redis_lib.TimeoutError,
        redis_lib.ResponseError,
        redis_lib.AuthenticationError,
    ):
        logger.warning("discover_workers failed", exc_info=True)
        return []


# ---------------------------------------------------------------------------
# Output formatter — testable without Textual
# ---------------------------------------------------------------------------


def format_stream_json_line(line: str) -> str | None:
    """Parse a Claude stream-json line into readable output.

    Returns a formatted string for display, or None to skip the line.
    Handles assistant text, tool use summaries, and task boundary markers.
    """
    line = line.strip()
    if not line:
        return None

    try:
        obj = json.loads(line)
    except ValueError:
        return None

    if not isinstance(obj, dict):
        return None

    # Task boundary markers (published by worker loop)
    msg_type = obj.get("type")
    if msg_type == "task_start":
        resource = rich_escape(str(obj.get("resource", "?")))
        task_id = rich_escape(str(obj.get("task_id", "?")))
        return f"{'─' * 3} Task {task_id}: {resource} {'─' * 40}"
    if msg_type == "task_end":
        status = rich_escape(str(obj.get("status", "?")))
        task_id = rich_escape(str(obj.get("task_id", "?")))
        return f"{'─' * 3} End {task_id}: {status} {'─' * 42}"

    # Assistant messages with content blocks.
    # stream-json wraps messages: {"type":"assistant","message":{"role":...,"content":[...]}}
    msg = obj.get("message", obj)
    if msg.get("role") != "assistant" or not isinstance(msg.get("content"), list):
        return None

    parts: list[str] = []
    for block in msg["content"]:
        if not isinstance(block, dict):
            continue
        block_type = block.get("type")

        if block_type == "text":
            text = block.get("text", "")
            if text:
                parts.append(rich_escape(text))

        elif block_type == "tool_use":
            name = str(block.get("name", "?"))
            inp = block.get("input", {})
            if not isinstance(inp, dict):
                inp = {}
            if name == "Bash":
                cmd = str(inp.get("command", "?"))[:120]
                parts.append(f"  $ {rich_escape(cmd)}")
            elif name in ("Read", "Edit", "Write"):
                path = rich_escape(str(inp.get("file_path", "?")))
                parts.append(f"  {name} {path}")
            elif name == "Glob":
                parts.append(f"  Glob {rich_escape(str(inp.get('pattern', '?')))}")
            elif name == "Grep":
                parts.append(f"  Grep {rich_escape(str(inp.get('pattern', '?')))}")
            else:
                parts.append(f"  {rich_escape(name)}")

    return "\n".join(parts) if parts else None


# ---------------------------------------------------------------------------
# TUI layer
# ---------------------------------------------------------------------------


def _format_ttl(seconds: int) -> str:
    """Format TTL seconds into human-readable string."""
    if seconds == -1:
        return "no TTL"
    if seconds < 0:
        return "expired"
    if seconds < 60:
        return f"{seconds}s"
    minutes = seconds // 60
    secs = seconds % 60
    if minutes < 60:
        return f"{minutes}m {secs}s"
    hours = minutes // 60
    mins = minutes % 60
    return f"{hours}h {mins}m"


def _format_duration(seconds: int) -> str:
    """Format duration seconds into human-readable string."""
    if seconds < 0:
        seconds = 0
    if seconds < 60:
        return f"{seconds}s"
    minutes = seconds // 60
    secs = seconds % 60
    if minutes < 60:
        return f"{minutes}m {secs}s"
    hours = minutes // 60
    mins = minutes % 60
    return f"{hours}h {mins}m"


def _format_stranded_stream_banner(h: ProviderStreamHealth) -> str:
    """Rich-markup warning for a stranded provider stream.

    ``repr()`` runs first so the quoted stream name is a Python literal, then
    ``rich_escape`` makes that literal safe for markup. Escaping first and then
    applying ``!r`` doubles backslashes (e.g. ``tasks:xai\\\\[test\\\\]``) and
    can re-expose brackets to Rich markup parsing.
    """
    return (
        f"STRANDED provider stream {rich_escape(repr(h.stream))} "
        f"(provider={rich_escape(h.provider)}): pending={h.pending} lag={h.lag} "
        f"registered_consumers={h.registered_consumers} "
        f"live_consumers={h.live_consumers} -- work is queued but no consumer "
        "has a live worker heartbeat"
    )


def truncate(s: str, n: int = 60) -> str:
    """Truncate string to at most n characters (including '...' suffix)."""
    return s[: n - 3] + "..." if len(s) > n else s


def _status_style(status: str) -> str:
    """Return a Rich markup color for a result status."""
    s = status.lower()
    if s == "completed":
        return "green"
    if s == "failed":
        return "red"
    if s == "usage_exhausted":
        return "magenta"
    return "white"


def run_dashboard(redis: RedisClient, refresh_interval: float = 3.0) -> None:
    """Launch the Textual TUI dashboard."""
    from rich.text import Text
    from textual.app import App, ComposeResult
    from textual.containers import VerticalScroll
    from textual.widgets import DataTable, Footer, Header, RichLog, Static

    class OrcestDashboard(App):
        """Live TUI dashboard for orcest system monitoring."""

        TITLE = "Orcest Dashboard"

        CSS = """
        Screen {
            layout: vertical;
        }
        #health-bar {
            height: 1;
            background: $success;
            color: $text;
            padding: 0 1;
        }
        #health-bar.disconnected {
            background: $error;
        }
        #stream-health-banner {
            height: auto;
            background: $error;
            color: $text;
            padding: 0 1;
            display: none;
        }
        #stream-health-banner.visible {
            display: block;
        }
        #result-health-banner {
            height: auto;
            background: $error;
            color: $text;
            padding: 0 1;
            display: none;
        }
        #result-health-banner.visible {
            display: block;
        }
        .section-title {
            text-style: bold;
            color: $text;
            padding: 1 0 0 1;
        }
        DataTable {
            height: auto;
            max-height: 12;
            margin: 0 1;
        }
        #results-table {
            height: 1fr;
            max-height: 100%;
        }
        #overview-container {
            height: 1fr;
        }
        #worker-container {
            height: 1fr;
            display: none;
        }
        #worker-container.visible {
            display: block;
        }
        #overview-container.hidden {
            display: none;
        }
        #worker-header {
            height: 1;
            background: $primary;
            color: $text;
            padding: 0 1;
        }
        #worker-log {
            height: 1fr;
            margin: 0 1;
        }
        """

        BINDINGS = [
            ("q", "quit", "Quit"),
            ("r", "refresh", "Refresh"),
            ("w", "toggle_worker", "Worker output"),
            ("escape", "back_to_overview", "Back"),
        ]

        def __init__(self) -> None:
            super().__init__()
            self._worker_view = False
            self._worker_ids: list[str] = []
            self._worker_idx = 0
            self._current_worker_id: str | None = None
            self._last_ids: dict[str, str] = {}

        def compose(self) -> ComposeResult:
            yield Header(show_clock=True)
            yield Static("", id="health-bar")
            yield Static("", id="stream-health-banner")
            yield Static("", id="result-health-banner")
            with VerticalScroll(id="overview-container"):
                yield Static("Queue Depths", classes="section-title")
                yield DataTable(id="queues-table")
                yield Static("Result Stream Health", classes="section-title")
                yield DataTable(id="result-health-table")
                yield Static("Provider Stream Health", classes="section-title")
                yield DataTable(id="stream-health-table")
                yield Static("Active Locks", classes="section-title")
                yield DataTable(id="locks-table")
                yield Static(
                    "Workers (Consumer Groups)",
                    classes="section-title",
                )
                yield DataTable(id="groups-table")
                yield Static("Recent Results", classes="section-title")
                yield DataTable(id="results-table")
                yield Static("Dead Letters", classes="section-title")
                yield DataTable(id="dead-letters-table")
            with VerticalScroll(id="worker-container"):
                yield Static("", id="worker-header")
                yield RichLog(id="worker-log", wrap=True, markup=True)
            yield Footer()

        def on_mount(self) -> None:
            queues = self.query_one("#queues-table", DataTable)
            queues.add_columns("Stream", "Work", "Retained XLEN")

            result_health = self.query_one("#result-health-table", DataTable)
            result_health.add_columns("Metric", "Value")

            stream_health = self.query_one("#stream-health-table", DataTable)
            stream_health.add_columns(
                "Provider",
                "Stream",
                "State",
                "Pending",
                "Lag",
                "Registered",
                "Live",
                "Observed",
                "Since",
            )

            locks = self.query_one("#locks-table", DataTable)
            locks.add_columns("PR", "Worker", "TTL")

            groups = self.query_one("#groups-table", DataTable)
            groups.add_columns("Stream", "Group", "Consumers", "Pending")

            results = self.query_one("#results-table", DataTable)
            results.add_columns(
                "Status",
                "Type",
                "Resource",
                "Worker",
                "Duration",
                "Summary",
            )

            dead_letters_tbl = self.query_one("#dead-letters-table", DataTable)
            dead_letters_tbl.add_columns("Time", "Type", "Repo", "Resource", "Reason")

            self._update_display()
            self.set_interval(refresh_interval, self._update_display)

        def _update_display(self) -> None:
            # In worker view, skip the full snapshot (avoids unnecessary
            # scan_iter / xlen / xrevrange calls) and only health-check.
            if self._worker_view:
                redis_ok = redis.health_check()
                health = self.query_one("#health-bar", Static)
                if redis_ok:
                    ts = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
                    health.update(f" REDIS OK | Refreshed {ts}")
                    health.remove_class("disconnected")
                    self._update_worker_output()
                else:
                    health.update(" REDIS DISCONNECTED")
                    health.add_class("disconnected")
                return

            snapshot = fetch_snapshot(redis)

            # Health bar
            health = self.query_one("#health-bar", Static)
            if snapshot.redis_ok:
                ts = snapshot.fetched_at.strftime("%H:%M:%S UTC")
                health.update(f" REDIS OK | Refreshed {ts}")
                health.remove_class("disconnected")
            else:
                health.update(" REDIS DISCONNECTED")
                health.add_class("disconnected")
                return

            self._update_overview(snapshot)

        def _update_overview(self, snapshot: SystemSnapshot) -> None:
            # Queue depths
            queues = self.query_one("#queues-table", DataTable)
            queues.clear()
            for stream, depth in snapshot.queue_depths.items():
                queues.add_row(stream, str(depth), "--")
            if not snapshot.queue_depths:
                queues.add_row("(no task streams)", "0", "--")
            result_health = snapshot.result_stream_health
            result_work = result_health.work
            queues.add_row("results work", str(result_work), "--")
            queues.add_row("results retained", "--", str(snapshot.results_depth))
            dl_count = snapshot.dead_letter_count
            dl_text = Text(str(dl_count), style="red bold") if dl_count > 0 else Text(str(dl_count))
            queues.add_row(DEAD_LETTER_STREAM, dl_text, str(dl_count))

            result_table = self.query_one("#result-health-table", DataTable)
            result_table.clear()
            for metric, value in format_result_stream_metrics(result_health):
                result_table.add_row(metric, value)

            result_banner = self.query_one("#result-health-banner", Static)
            warning = format_result_stream_warning(result_health)
            if warning:
                result_banner.update(warning)
                result_banner.add_class("visible")
            else:
                result_banner.update("")
                result_banner.remove_class("visible")

            self._update_stream_health(snapshot)

            # Active locks
            locks_table = self.query_one("#locks-table", DataTable)
            locks_table.clear()
            if snapshot.locks:
                for lock in snapshot.locks:
                    locks_table.add_row(
                        f"#{lock.pr}",
                        lock.owner,
                        _format_ttl(lock.ttl),
                    )
            else:
                locks_table.add_row("--", "No active locks", "--")

            # Consumer groups
            groups_table = self.query_one("#groups-table", DataTable)
            groups_table.clear()
            if snapshot.consumer_groups:
                for g in snapshot.consumer_groups:
                    groups_table.add_row(
                        g.stream,
                        g.name,
                        str(g.consumers),
                        str(g.pending),
                    )
            else:
                groups_table.add_row("--", "No groups", "--", "--")

            # Recent results
            results_table = self.query_one("#results-table", DataTable)
            results_table.clear()
            if snapshot.recent_results:
                for r in snapshot.recent_results:
                    summary = (r.summary[:80] + "...") if len(r.summary) > 80 else r.summary
                    results_table.add_row(
                        Text(r.status.upper(), style=_status_style(r.status)),
                        r.resource_type,
                        f"#{r.resource_id}",
                        r.worker_id,
                        _format_duration(r.duration_seconds),
                        summary,
                    )
            else:
                results_table.add_row(
                    "--",
                    "--",
                    "--",
                    "--",
                    "--",
                    "No results yet",
                )

            # Dead-letter entries
            dl_table = self.query_one("#dead-letters-table", DataTable)
            dl_table.clear()
            if snapshot.dead_letter_entries:
                for entry in snapshot.dead_letter_entries:
                    ts = (
                        datetime.fromtimestamp(entry.timestamp_ms / 1000, tz=timezone.utc).strftime(
                            "%Y-%m-%d %H:%M UTC"
                        )
                        if entry.timestamp_ms is not None
                        else entry.entry_id
                    )
                    reason_str = truncate(entry.reason) if entry.reason is not None else "?"
                    dl_table.add_row(
                        ts,
                        entry.task_type,
                        entry.repo,
                        f"{entry.resource_type} #{entry.resource_id}",
                        Text(reason_str, style="red")
                        if entry.reason is not None
                        else Text(reason_str),
                    )
            else:
                dl_table.add_row("--", "--", "--", "--", "No dead-lettered tasks")

        def _update_stream_health(self, snapshot: SystemSnapshot) -> None:
            """Render PoolManager's canonical provider stream-health snapshots.

            This dashboard never recomputes stranded-stream health, it only
            displays what was already published to Redis (issue #613/#640).
            """
            now_ts = snapshot.fetched_at.timestamp()
            state_styles = {
                StreamHealthState.STRANDED: "bold red",
                StreamHealthState.HEALTHY: "green",
                StreamHealthState.UNKNOWN: "dim",
            }

            table = self.query_one("#stream-health-table", DataTable)
            table.clear()
            if snapshot.stream_health:
                for h in snapshot.stream_health:
                    table.add_row(
                        rich_escape(h.provider),
                        rich_escape(h.stream),
                        Text(h.state.value.upper(), style=state_styles.get(h.state, "white")),
                        "?" if h.pending is None else str(h.pending),
                        "?" if h.lag is None else str(h.lag),
                        "?" if h.registered_consumers is None else str(h.registered_consumers),
                        "?" if h.live_consumers is None else str(h.live_consumers),
                        _format_duration(int(max(now_ts - h.observed_at, 0))) + " ago",
                        _format_duration(int(max(now_ts - h.transitioned_at, 0))) + " ago",
                    )
            else:
                table.add_row("--", "--", "--", "--", "--", "--", "--", "--", "--")

            banner = self.query_one("#stream-health-banner", Static)
            stranded = [h for h in snapshot.stream_health if h.state == StreamHealthState.STRANDED]
            if stranded:
                lines = [_format_stranded_stream_banner(h) for h in stranded]
                banner.update("\n".join(lines))
                banner.add_class("visible")
            else:
                banner.update("")
                banner.remove_class("visible")

        def _update_worker_output(self) -> None:
            """Read new output entries from the current worker's stream."""
            if not self._worker_ids:
                return
            if self._worker_idx >= len(self._worker_ids):
                self._worker_idx = 0
                self._current_worker_id = self._worker_ids[0]
                # Update header so the displayed worker name matches
                self._update_worker_header()

            worker_id = self._worker_ids[self._worker_idx]
            stream = f"output:{worker_id}"
            last_id = self._last_ids.get(worker_id, "0-0")

            # xread_after already catches Redis errors internally and
            # returns [].  No outer try/except needed here.
            entries = redis.xread_after(stream, last_id)
            if not entries:
                return

            log = self.query_one("#worker-log", RichLog)
            for entry_id, fields in entries:
                self._last_ids[worker_id] = entry_id
                # Continuation chunks exist so TraceArchiver can reconstruct
                # the verbatim line; live-tail only shows the first 4 KiB.
                if is_output_continuation(fields):
                    continue
                # Task boundary markers have "type" field
                if "type" in fields:
                    line_data = json.dumps(fields)
                else:
                    line_data = fields.get("line", "")
                formatted = format_stream_json_line(line_data)
                if formatted:
                    log.write(formatted)

        def _update_worker_header(self) -> None:
            """Refresh the worker-header label to match current index."""
            if self._worker_ids and self._worker_idx < len(self._worker_ids):
                wid = rich_escape(self._worker_ids[self._worker_idx])
                header = self.query_one("#worker-header", Static)
                idx = self._worker_idx + 1
                total = len(self._worker_ids)
                header.update(f" Worker: {wid} ({idx}/{total}) | w: next worker | Escape: back")

        def _show_worker_view(self) -> None:
            """Switch to worker output view."""
            self._worker_view = True
            overview = self.query_one("#overview-container")
            overview.add_class("hidden")
            worker = self.query_one("#worker-container")
            worker.add_class("visible")
            self._update_worker_header()

        def _show_overview(self) -> None:
            """Switch back to overview."""
            self._worker_view = False
            overview = self.query_one("#overview-container")
            overview.remove_class("hidden")
            worker = self.query_one("#worker-container")
            worker.remove_class("visible")

        def _resolve_worker_index(self) -> None:
            """Set _worker_idx from _current_worker_id.

            If the previously selected worker disappeared from the
            refreshed list, fall back to index 0 and update
            _current_worker_id to stay consistent.
            """
            if self._current_worker_id is not None and self._current_worker_id in self._worker_ids:
                self._worker_idx = self._worker_ids.index(self._current_worker_id)
            else:
                self._worker_idx = 0
                self._current_worker_id = self._worker_ids[0] if self._worker_ids else None

        def action_toggle_worker(self) -> None:
            """Toggle into worker view or cycle to next worker."""
            # Refresh worker list
            self._worker_ids = discover_workers(redis)
            if not self._worker_ids:
                return  # No workers with output

            if self._worker_view:
                # Resolve current position after refresh
                self._resolve_worker_index()
                prev_worker = self._current_worker_id
                # Cycle to next worker
                self._worker_idx = (self._worker_idx + 1) % len(self._worker_ids)
                self._current_worker_id = self._worker_ids[self._worker_idx]
                if self._current_worker_id != prev_worker:
                    # Clear log for new worker
                    log = self.query_one("#worker-log", RichLog)
                    log.clear()
                    # Reset cursor to stream start for this worker
                    self._last_ids.pop(self._current_worker_id, None)
                self._show_worker_view()
                self._update_worker_output()
            else:
                # Enter worker view
                self._worker_idx = 0
                self._current_worker_id = self._worker_ids[self._worker_idx]
                log = self.query_one("#worker-log", RichLog)
                log.clear()
                self._last_ids.pop(self._current_worker_id, None)
                self._show_worker_view()
                self._update_worker_output()

        def action_back_to_overview(self) -> None:
            if self._worker_view:
                self._show_overview()
                self._update_display()

        def action_refresh(self) -> None:
            self._update_display()

    app = OrcestDashboard()
    app.run()
