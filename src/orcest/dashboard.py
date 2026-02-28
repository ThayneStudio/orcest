"""Live TUI dashboard for orcest system monitoring.

Provides a top-like full-screen terminal interface that auto-refreshes
every few seconds, showing queue depths, active locks, consumer groups,
and recent task results.

Usage:
    orcest status          # launches the TUI
    orcest status --once   # single-shot output (old behavior)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone

import redis as redis_lib

from orcest.shared.redis_client import RedisClient

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
class SystemSnapshot:
    """Complete point-in-time view of orcest system state."""

    redis_ok: bool
    fetched_at: datetime
    queue_depths: dict[str, int] = field(default_factory=dict)
    results_depth: int = 0
    locks: list[LockInfo] = field(default_factory=list)
    consumer_groups: list[ConsumerGroupInfo] = field(default_factory=list)
    recent_results: list[RecentResult] = field(default_factory=list)
    attempt_counts: dict[str, int] = field(default_factory=dict)


def fetch_snapshot(redis: RedisClient, max_results: int = 20) -> SystemSnapshot:
    """Query Redis and return a complete system snapshot."""
    if not redis.health_check():
        return SystemSnapshot(
            redis_ok=False,
            fetched_at=datetime.now(timezone.utc),
        )

    client = redis.client

    # Queue depths
    task_streams = list(client.scan_iter(match="tasks:*"))
    queue_depths = {}
    for stream_key in sorted(task_streams):
        try:
            queue_depths[stream_key] = client.xlen(stream_key) or 0
        except redis_lib.ResponseError:
            pass  # Key exists but is not a stream

    try:
        results_depth = client.xlen("results") or 0
    except redis_lib.ResponseError:
        results_depth = 0

    # Active locks
    lock_keys = list(client.scan_iter(match="lock:pr:*"))
    locks = []
    for key in lock_keys:
        owner = client.get(key) or "(expired)"
        ttl = client.ttl(key)
        pr_num = key.removeprefix("lock:pr:")
        locks.append(LockInfo(pr=pr_num, owner=owner, ttl=ttl))

    # Consumer groups
    consumer_groups = []
    for stream_key in sorted(task_streams):
        try:
            for g in client.xinfo_groups(stream_key):
                consumer_groups.append(ConsumerGroupInfo(
                    stream=stream_key,
                    name=g["name"],
                    consumers=g["consumers"],
                    pending=g["pending"],
                ))
        except redis_lib.ResponseError:
            pass  # Stream has no consumer groups

    # Recent results (most recent first)
    recent_results = []
    try:
        entries = client.xrevrange("results", count=max_results)
    except redis_lib.ResponseError:
        entries = []
    for _entry_id, fields in entries:
        try:
            recent_results.append(RecentResult(
                task_id=fields.get("task_id", ""),
                worker_id=fields.get("worker_id", ""),
                status=fields.get("status", ""),
                resource_type=fields.get("resource_type", ""),
                resource_id=fields.get("resource_id", ""),
                duration_seconds=int(fields.get("duration_seconds", 0)),
                summary=fields.get("summary", ""),
            ))
        except (ValueError, TypeError):
            logger.debug("Skipping malformed result entry: %s", _entry_id)

    # Attempt counters
    attempt_counts = {}
    attempt_keys = list(client.scan_iter(match="pr:*:attempts"))
    for key in attempt_keys:
        data = client.hgetall(key)
        if data:
            # Key format: "pr:<number>:attempts"
            pr_num = key.removeprefix("pr:").removesuffix(":attempts")
            try:
                attempt_counts[f"PR #{pr_num}"] = int(data.get("count", 0))
            except (ValueError, TypeError):
                pass

    return SystemSnapshot(
        redis_ok=True,
        fetched_at=datetime.now(timezone.utc),
        queue_depths=queue_depths,
        results_depth=results_depth,
        locks=locks,
        consumer_groups=consumer_groups,
        recent_results=recent_results,
        attempt_counts=attempt_counts,
    )


# ---------------------------------------------------------------------------
# TUI layer
# ---------------------------------------------------------------------------


def _format_ttl(seconds: int) -> str:
    """Format TTL seconds into human-readable string."""
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
    if seconds < 60:
        return f"{seconds}s"
    minutes = seconds // 60
    secs = seconds % 60
    return f"{minutes}m {secs}s"


def _status_style(status: str) -> str:
    """Return a Rich markup color for a result status."""
    s = status.lower()
    if s == "completed":
        return "green"
    if s == "failed":
        return "red"
    if s == "blocked":
        return "yellow"
    if s == "usage_exhausted":
        return "magenta"
    return "white"


def run_dashboard(redis: RedisClient, refresh_interval: float = 3.0) -> None:
    """Launch the Textual TUI dashboard."""
    from textual.app import App, ComposeResult
    from textual.containers import VerticalScroll
    from textual.widgets import DataTable, Footer, Header, Static

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
        """

        BINDINGS = [
            ("q", "quit", "Quit"),
            ("r", "refresh", "Refresh"),
        ]

        def compose(self) -> ComposeResult:
            yield Header(show_clock=True)
            yield Static("", id="health-bar")
            with VerticalScroll():
                yield Static("Queue Depths", classes="section-title")
                yield DataTable(id="queues-table")
                yield Static("Active Locks", classes="section-title")
                yield DataTable(id="locks-table")
                yield Static("Workers (Consumer Groups)", classes="section-title")
                yield DataTable(id="groups-table")
                yield Static("Recent Results", classes="section-title")
                yield DataTable(id="results-table")
            yield Footer()

        def on_mount(self) -> None:
            queues = self.query_one("#queues-table", DataTable)
            queues.add_columns("Stream", "Pending")

            locks = self.query_one("#locks-table", DataTable)
            locks.add_columns("PR", "Worker", "TTL")

            groups = self.query_one("#groups-table", DataTable)
            groups.add_columns("Stream", "Group", "Consumers", "Pending")

            results = self.query_one("#results-table", DataTable)
            results.add_columns("Status", "Type", "Resource", "Worker", "Duration", "Summary")

            self._update_display()
            self.set_interval(refresh_interval, self._update_display)

        def _update_display(self) -> None:
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

            # Queue depths
            queues = self.query_one("#queues-table", DataTable)
            queues.clear()
            for stream, depth in snapshot.queue_depths.items():
                queues.add_row(stream, str(depth))
            queues.add_row("results", str(snapshot.results_depth))
            if not snapshot.queue_depths:
                queues.add_row("(no task streams)", "0")

            # Active locks
            locks_table = self.query_one("#locks-table", DataTable)
            locks_table.clear()
            if snapshot.locks:
                for lock in snapshot.locks:
                    locks_table.add_row(
                        f"#{lock.pr}", lock.owner, _format_ttl(lock.ttl)
                    )
            else:
                locks_table.add_row("--", "No active locks", "--")

            # Consumer groups
            groups_table = self.query_one("#groups-table", DataTable)
            groups_table.clear()
            if snapshot.consumer_groups:
                for g in snapshot.consumer_groups:
                    groups_table.add_row(
                        g.stream, g.name, str(g.consumers), str(g.pending)
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
                        r.status.upper(),
                        r.resource_type,
                        f"#{r.resource_id}",
                        r.worker_id,
                        _format_duration(r.duration_seconds),
                        summary,
                    )
            else:
                results_table.add_row("--", "--", "--", "--", "--", "No results yet")

        def action_refresh(self) -> None:
            self._update_display()

    app = OrcestDashboard()
    app.run()
