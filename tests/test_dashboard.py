"""Unit tests for the dashboard data-fetching and formatting layers."""

import asyncio
import json
import os
import shutil
import subprocess
import time

import pytest
import redis as redis_lib

from orcest.dashboard import (
    DeadLetterEntry,
    _format_duration,
    _format_stranded_stream_banner,
    _format_ttl,
    _status_style,
    discover_workers,
    fetch_snapshot,
    format_stream_json_line,
    run_dashboard,
)
from orcest.shared.provider_stream_health import (
    STREAM_HEALTH_KEY_PREFIX,
    ProviderStreamHealth,
    StreamHealthState,
    stream_health_snapshot_key,
)


def test_empty_redis_returns_valid_snapshot(fake_redis_client):
    """Returns a valid snapshot when Redis has no orcest data."""
    snap = fetch_snapshot(fake_redis_client)
    assert snap.redis_ok is True
    assert snap.queue_depths == {}
    assert snap.results_depth == 0
    assert snap.locks == []
    assert snap.consumer_groups == []
    assert snap.recent_results == []
    assert snap.attempt_counts == {}


def test_queue_depths(fake_redis_client):
    """Reports pending+lag from consumer groups, not XLEN."""
    # Without consumer groups, queue depths are empty
    fake_redis_client.xadd("tasks:claude", {"id": "1", "repo": "org/repo"})
    snap = fetch_snapshot(fake_redis_client)
    assert snap.queue_depths == {}

    # Create consumer group and add entries — depth reflects undelivered work
    fake_redis_client.ensure_consumer_group("tasks:claude", "workers")
    fake_redis_client.xadd("tasks:claude", {"id": "2", "repo": "org/repo"})
    snap = fetch_snapshot(fake_redis_client)
    assert snap.queue_depths["tasks:claude"] > 0

    # Read all entries — they become pending (delivered, not ACKed)
    entries = fake_redis_client.xreadgroup("workers", "w1", "tasks:claude", count=10, block_ms=0)
    snap = fetch_snapshot(fake_redis_client)
    assert snap.queue_depths["tasks:claude"] == len(entries)  # pending = delivered count

    # ACK all entries — depth drops to 0
    for entry_id, _fields in entries:
        fake_redis_client.xack("tasks:claude", "workers", entry_id)
    snap = fetch_snapshot(fake_redis_client)
    assert snap.queue_depths["tasks:claude"] == 0


def test_results_depth(fake_redis_client):
    """Reports the results stream length."""
    fake_redis_client.xadd("results", {"task_id": "t1", "status": "completed"})
    fake_redis_client.xadd("results", {"task_id": "t2", "status": "failed"})

    snap = fetch_snapshot(fake_redis_client)

    assert snap.results_depth == 2


def test_active_locks(fake_redis_client):
    """Shows active PR locks with owner and TTL."""
    fake_redis_client.set_ex("lock:pr:42", "worker-1", 1800)

    snap = fetch_snapshot(fake_redis_client)

    assert len(snap.locks) == 1
    assert snap.locks[0].pr == "42"
    assert snap.locks[0].owner == "worker-1"
    assert snap.locks[0].ttl > 0


def test_recent_results(fake_redis_client):
    """Reads recent results in reverse chronological order."""
    for i in range(5):
        fake_redis_client.xadd(
            "results",
            {
                "task_id": f"task-{i}",
                "worker_id": "w1",
                "status": "completed",
                "resource_type": "pr",
                "resource_id": str(i),
                "duration_seconds": "30",
                "summary": f"Fixed PR {i}",
            },
        )

    snap = fetch_snapshot(fake_redis_client, max_results=3)

    assert len(snap.recent_results) == 3
    # Most recent first (resource_id 4, 3, 2)
    assert snap.recent_results[0].resource_id == "4"
    assert snap.recent_results[1].resource_id == "3"
    assert snap.recent_results[2].resource_id == "2"


def test_attempt_counts(fake_redis_client):
    """Reports PR attempt counters."""
    fake_redis_client.hset("pr:test-org/test-repo:42:attempts", "count", "3")
    fake_redis_client.hset("pr:test-org/test-repo:42:attempts", "head_sha", "abc")

    snap = fetch_snapshot(fake_redis_client)

    assert snap.attempt_counts == {"PR #42": 3}


def test_dead_letter_count_zero_when_empty(fake_redis_client):
    """Dead-letter count is 0 when the stream does not exist."""
    snap = fetch_snapshot(fake_redis_client)
    assert snap.dead_letter_count == 0


def test_dead_letter_count_in_snapshot(fake_redis_client):
    """Reports dead-letter stream length in snapshot."""
    fake_redis_client.xadd("dead-letter", {"id": "t1", "type": "fix_ci"})
    fake_redis_client.xadd("dead-letter", {"id": "t2", "type": "fix_ci"})

    snap = fetch_snapshot(fake_redis_client)

    assert snap.dead_letter_count == 2


def test_dead_letter_entries_empty_when_no_stream(fake_redis_client):
    """Dead-letter entries list is empty when the stream does not exist."""
    snap = fetch_snapshot(fake_redis_client)
    assert snap.dead_letter_entries == []


def test_dead_letter_entries_populated(fake_redis_client):
    """Fetches last N dead-letter entries with task details."""
    fake_redis_client.xadd(
        "dead-letter",
        {
            "id": "task-abc",
            "type": "fix_ci",
            "repo": "org/repo",
            "resource_type": "pr",
            "resource_id": "42",
            "dead_letter_reason": "max deliveries exceeded",
        },
    )

    snap = fetch_snapshot(fake_redis_client)

    assert len(snap.dead_letter_entries) == 1
    entry = snap.dead_letter_entries[0]
    assert isinstance(entry, DeadLetterEntry)
    assert entry.task_type == "fix_ci"
    assert entry.repo == "org/repo"
    assert entry.resource_type == "pr"
    assert entry.resource_id == "42"
    assert entry.reason == "max deliveries exceeded"
    assert entry.timestamp_ms > 0


def test_dead_letter_entries_capped_at_five(fake_redis_client):
    """At most 5 dead-letter entries are returned in the snapshot."""
    for i in range(8):
        fake_redis_client.xadd(
            "dead-letter",
            {"id": f"task-{i}", "type": "fix_ci", "repo": "org/repo"},
        )

    snap = fetch_snapshot(fake_redis_client)

    assert snap.dead_letter_count == 8
    assert len(snap.dead_letter_entries) == 5


def test_dead_letter_entries_most_recent_first(fake_redis_client):
    """Dead-letter entries are returned most recent first."""
    for i in range(3):
        fake_redis_client.xadd(
            "dead-letter",
            {"id": f"task-{i}", "type": "fix_ci", "repo": f"org/repo-{i}"},
        )

    snap = fetch_snapshot(fake_redis_client)

    # Most recent entry has the highest repo index
    assert snap.dead_letter_entries[0].repo == "org/repo-2"
    assert snap.dead_letter_entries[2].repo == "org/repo-0"


def test_disconnected_redis(fake_redis_client, mocker):
    """Returns redis_ok=False when Redis is unreachable."""
    mocker.patch.object(fake_redis_client, "health_check", return_value=False)

    snap = fetch_snapshot(fake_redis_client)

    assert snap.redis_ok is False
    assert snap.queue_depths == {}


def test_connection_lost_during_fetch(fake_redis_client, mocker):
    """Returns redis_ok=False when Redis disconnects mid-fetch."""
    mocker.patch.object(
        fake_redis_client.client,
        "scan_iter",
        side_effect=redis_lib.ConnectionError("connection lost"),
    )

    snap = fetch_snapshot(fake_redis_client)

    assert snap.redis_ok is False
    assert snap.queue_depths == {}


# ---------------------------------------------------------------------------
# Tests for format_stream_json_line (output formatter)
# ---------------------------------------------------------------------------


def _assistant_msg(*content_blocks: dict) -> str:
    """Build a stream-json assistant message line."""
    return json.dumps(
        {
            "type": "message",
            "role": "assistant",
            "content": list(content_blocks),
        }
    )


@pytest.mark.unit
class TestFormatStreamJsonLine:
    """Tests for the format_stream_json_line output formatter."""

    def test_format_assistant_text(self):
        """Assistant text block is returned as-is."""
        line = _assistant_msg({"type": "text", "text": "All tests pass now."})
        result = format_stream_json_line(line)
        assert result == "All tests pass now."

    def test_format_tool_use_bash(self):
        """Bash tool use shows '$ command'."""
        line = _assistant_msg(
            {
                "type": "tool_use",
                "name": "Bash",
                "input": {"command": "npm test"},
            }
        )
        result = format_stream_json_line(line)
        assert result == "  $ npm test"

    def test_format_tool_use_bash_truncates_long_command(self):
        """Bash command is truncated to 120 chars."""
        long_cmd = "x" * 200
        line = _assistant_msg(
            {
                "type": "tool_use",
                "name": "Bash",
                "input": {"command": long_cmd},
            }
        )
        result = format_stream_json_line(line)
        assert result == f"  $ {'x' * 120}"

    def test_format_tool_use_read(self):
        """Read tool shows 'Read /path'."""
        line = _assistant_msg(
            {
                "type": "tool_use",
                "name": "Read",
                "input": {"file_path": "/src/main.py"},
            }
        )
        result = format_stream_json_line(line)
        assert result == "  Read /src/main.py"

    def test_format_tool_use_edit(self):
        """Edit tool shows 'Edit /path'."""
        line = _assistant_msg(
            {
                "type": "tool_use",
                "name": "Edit",
                "input": {"file_path": "/src/main.py"},
            }
        )
        result = format_stream_json_line(line)
        assert result == "  Edit /src/main.py"

    def test_format_tool_use_write(self):
        """Write tool shows 'Write /path'."""
        line = _assistant_msg(
            {
                "type": "tool_use",
                "name": "Write",
                "input": {"file_path": "/src/new_file.py"},
            }
        )
        result = format_stream_json_line(line)
        assert result == "  Write /src/new_file.py"

    def test_format_tool_use_glob(self):
        """Glob tool shows 'Glob pattern'."""
        line = _assistant_msg(
            {
                "type": "tool_use",
                "name": "Glob",
                "input": {"pattern": "**/*.py"},
            }
        )
        result = format_stream_json_line(line)
        assert result == "  Glob **/*.py"

    def test_format_tool_use_grep(self):
        """Grep tool shows 'Grep pattern'."""
        line = _assistant_msg(
            {
                "type": "tool_use",
                "name": "Grep",
                "input": {"pattern": "def main"},
            }
        )
        result = format_stream_json_line(line)
        assert result == "  Grep def main"

    def test_format_tool_use_generic(self):
        """Unknown tool name shows just the tool name."""
        line = _assistant_msg(
            {
                "type": "tool_use",
                "name": "WebSearch",
                "input": {"query": "python docs"},
            }
        )
        result = format_stream_json_line(line)
        assert result == "  WebSearch"

    def test_format_tool_use_non_dict_input(self):
        """Tool use with non-dict input does not crash."""
        for bad_input in ("a string", ["a", "list"], 42, None):
            line = _assistant_msg(
                {
                    "type": "tool_use",
                    "name": "Bash",
                    "input": bad_input,
                }
            )
            result = format_stream_json_line(line)
            assert result is not None  # falls through to "$ ?"

    def test_format_system_message_skipped(self):
        """System messages return None."""
        line = json.dumps({"role": "system", "cost_usd": 0.05})
        assert format_stream_json_line(line) is None

    def test_format_tool_result_skipped(self):
        """Tool result messages return None."""
        line = json.dumps({"role": "tool", "content": "ok"})
        assert format_stream_json_line(line) is None

    def test_format_invalid_json(self):
        """Malformed JSON returns None without crashing."""
        assert format_stream_json_line("not json at all") is None
        assert format_stream_json_line("{incomplete") is None

    def test_format_non_dict_json(self):
        """Valid JSON that is not a dict returns None without crashing."""
        assert format_stream_json_line("[1, 2, 3]") is None
        assert format_stream_json_line('"just a string"') is None
        assert format_stream_json_line("42") is None
        assert format_stream_json_line("null") is None

    def test_format_empty_line(self):
        """Empty or whitespace-only lines return None."""
        assert format_stream_json_line("") is None
        assert format_stream_json_line("   ") is None

    def test_format_task_start_marker(self):
        """task_start marker renders as separator line."""
        line = json.dumps(
            {
                "type": "task_start",
                "task_id": "abc123",
                "resource": "pr #42",
            }
        )
        result = format_stream_json_line(line)
        assert result is not None
        assert "abc123" in result
        assert "pr #42" in result
        assert "─" in result

    def test_format_task_end_marker(self):
        """task_end marker renders as separator line with status."""
        line = json.dumps(
            {
                "type": "task_end",
                "task_id": "abc123",
                "status": "completed",
            }
        )
        result = format_stream_json_line(line)
        assert result is not None
        assert "abc123" in result
        assert "completed" in result
        assert "─" in result

    def test_format_mixed_content_blocks(self):
        """Message with text + tool use returns both parts."""
        line = _assistant_msg(
            {"type": "text", "text": "Let me check the file."},
            {"type": "tool_use", "name": "Read", "input": {"file_path": "/a.py"}},
        )
        result = format_stream_json_line(line)
        assert result is not None
        assert "Let me check the file." in result
        assert "Read /a.py" in result

    def test_format_escapes_rich_markup_in_text(self):
        """Rich markup characters in text are escaped to prevent rendering."""
        line = _assistant_msg({"type": "text", "text": "Check [bold]this[/bold]"})
        result = format_stream_json_line(line)
        assert result is not None
        # Escaped brackets should have a backslash prefix
        assert "\\[bold]" in result
        assert "\\[/bold]" in result

    def test_format_escapes_rich_markup_in_bash_command(self):
        """Rich markup in Bash commands is escaped."""
        line = _assistant_msg(
            {
                "type": "tool_use",
                "name": "Bash",
                "input": {"command": "echo '[red]error[/red]'"},
            }
        )
        result = format_stream_json_line(line)
        assert result is not None
        assert "\\[red]" in result
        assert "\\[/red]" in result

    def test_format_escapes_rich_markup_in_task_start(self):
        """Rich markup in task_start fields is escaped."""
        line = json.dumps(
            {
                "type": "task_start",
                "task_id": "[bold]id",
                "resource": "[red]res",
            }
        )
        result = format_stream_json_line(line)
        assert result is not None
        assert "\\[bold]" in result
        assert "\\[red]" in result

    def test_format_stream_json_line_content_not_list(self):
        """Assistant message where content is a string instead of a list returns None."""
        line = json.dumps(
            {
                "role": "assistant",
                "content": "just a plain string",
            }
        )
        assert format_stream_json_line(line) is None

    def test_format_stream_json_line_empty_text_block(self):
        """Assistant message with empty text block returns None."""
        line = _assistant_msg({"type": "text", "text": ""})
        assert format_stream_json_line(line) is None


# ---------------------------------------------------------------------------
# Tests for _format_ttl
# ---------------------------------------------------------------------------


class TestFormatTtl:
    """Tests for the _format_ttl helper."""

    def test_format_ttl_no_ttl(self):
        """TTL of -1 means the key has no TTL set."""
        assert _format_ttl(-1) == "no TTL"

    def test_format_ttl_expired(self):
        """TTL of -2 means the key has expired or does not exist."""
        assert _format_ttl(-2) == "expired"

    def test_format_ttl_seconds(self):
        """TTL under 60 seconds is shown as seconds only."""
        assert _format_ttl(45) == "45s"

    def test_format_ttl_minutes(self):
        """TTL between 60 and 3600 seconds is shown as minutes and seconds."""
        assert _format_ttl(125) == "2m 5s"

    def test_format_ttl_hours(self):
        """TTL over 3600 seconds is shown as hours and minutes."""
        assert _format_ttl(3725) == "1h 2m"


# ---------------------------------------------------------------------------
# Tests for _format_duration
# ---------------------------------------------------------------------------


class TestFormatDuration:
    """Tests for the _format_duration helper."""

    def test_format_duration_seconds(self):
        """Duration under 60 seconds is shown as seconds only."""
        assert _format_duration(30) == "30s"

    def test_format_duration_minutes(self):
        """Duration over 60 seconds is shown as minutes and seconds."""
        assert _format_duration(90) == "1m 30s"

    def test_format_duration_hours(self):
        """Duration over 3600 seconds is shown as hours and minutes."""
        assert _format_duration(7200) == "2h 0m"
        assert _format_duration(3725) == "1h 2m"

    def test_format_duration_negative_clamped_to_zero(self):
        """Negative duration is clamped to 0s."""
        assert _format_duration(-5) == "0s"


# ---------------------------------------------------------------------------
# Tests for _status_style
# ---------------------------------------------------------------------------


class TestStatusStyle:
    """Tests for the _status_style helper."""

    def test_status_style_all_values(self):
        """Each known status returns the correct Rich color."""
        assert _status_style("completed") == "green"
        assert _status_style("failed") == "red"
        assert _status_style("blocked") == "yellow"
        assert _status_style("usage_exhausted") == "magenta"
        assert _status_style("anything_else") == "white"


# ---------------------------------------------------------------------------
# Tests for discover_workers
# ---------------------------------------------------------------------------


class TestDiscoverWorkers:
    """Tests for the discover_workers function."""

    def test_discover_workers_returns_sorted_ids(self, fake_redis_client):
        """Returns worker IDs sorted, extracted from output stream keys."""
        fake_redis_client.xadd("output:worker-2", {"line": "hello"})
        fake_redis_client.xadd("output:worker-1", {"line": "world"})

        result = discover_workers(fake_redis_client)
        assert result == ["worker-1", "worker-2"]

    def test_discover_workers_connection_error_returns_empty(self, fake_redis_client, mocker):
        """Returns empty list when scan_iter raises ConnectionError."""
        mocker.patch.object(
            fake_redis_client.client,
            "scan_iter",
            side_effect=redis_lib.ConnectionError("connection refused"),
        )

        result = discover_workers(fake_redis_client)
        assert result == []


# ---------------------------------------------------------------------------
# Tests for fetch_snapshot consumer groups
# ---------------------------------------------------------------------------


class TestFetchSnapshotConsumerGroups:
    """Tests for consumer group discovery in fetch_snapshot."""

    def test_fetch_snapshot_consumer_groups(self, fake_redis_client):
        """Consumer groups are populated after ensure_consumer_group."""
        fake_redis_client.ensure_consumer_group("tasks:claude", "workers")

        snap = fetch_snapshot(fake_redis_client)

        assert len(snap.consumer_groups) >= 1
        group = snap.consumer_groups[0]
        assert group.stream == "tasks:claude"
        assert group.name == "workers"


def _put_stream_health(rc, health: ProviderStreamHealth, *, issue: bool) -> None:
    rc.set_ex_raw(
        stream_health_snapshot_key(health.provider, issue=issue),
        json.dumps(health.to_dict()),
        900,
    )


def _sample_health(
    *,
    provider: str = "claude",
    stream: str,
    state: StreamHealthState = StreamHealthState.HEALTHY,
    pending: int | None = 0,
) -> ProviderStreamHealth:
    return ProviderStreamHealth(
        provider=provider,
        stream=stream,
        pending=pending,
        lag=0,
        registered_consumers=0,
        live_consumers=0,
        state=state,
        observed_at=1.0,
        transitioned_at=1.0,
    )


class TestFetchSnapshotStreamHealthIdentities:
    """Issue #639: snapshot discovery must surface both stream identities."""

    def test_discovers_pr_and_issue_identities(self, fake_redis_client):
        pr = _sample_health(stream="test:tasks:claude", pending=0)
        issue = _sample_health(
            stream="test:tasks:issue:claude",
            state=StreamHealthState.STRANDED,
            pending=2,
        )
        _put_stream_health(fake_redis_client, pr, issue=False)
        _put_stream_health(fake_redis_client, issue, issue=True)

        snap = fetch_snapshot(fake_redis_client)

        by_stream = {h.stream: h for h in snap.stream_health}
        assert set(by_stream) == {pr.stream, issue.stream}
        assert by_stream[pr.stream].state == StreamHealthState.HEALTHY
        assert by_stream[issue.stream].state == StreamHealthState.STRANDED
        assert by_stream[issue.stream].pending == 2
        assert [h.stream for h in snap.stream_health] == sorted([pr.stream, issue.stream])

    def test_malformed_record_does_not_drop_sibling(self, fake_redis_client):
        valid = _sample_health(
            stream="test:tasks:claude",
            state=StreamHealthState.STRANDED,
            pending=1,
        )
        _put_stream_health(fake_redis_client, valid, issue=False)
        fake_redis_client.set_ex_raw(
            stream_health_snapshot_key("claude", issue=True),
            "{not-json",
            900,
        )

        snap = fetch_snapshot(fake_redis_client)

        assert len(snap.stream_health) == 1
        assert snap.stream_health[0].stream == valid.stream
        assert snap.stream_health[0].state == StreamHealthState.STRANDED

    def test_malformed_pr_does_not_drop_issue(self, fake_redis_client):
        valid = _sample_health(stream="test:tasks:issue:claude", pending=0)
        fake_redis_client.set_ex_raw(
            stream_health_snapshot_key("claude"),
            json.dumps({"provider": "claude"}),
            900,
        )
        _put_stream_health(fake_redis_client, valid, issue=True)

        snap = fetch_snapshot(fake_redis_client)

        assert len(snap.stream_health) == 1
        assert snap.stream_health[0].stream == valid.stream

    def test_scan_prefix_matches_kind_suffixed_keys(self, fake_redis_client):
        pr_key = stream_health_snapshot_key("claude")
        issue_key = stream_health_snapshot_key("claude", issue=True)
        assert pr_key.startswith(STREAM_HEALTH_KEY_PREFIX)
        assert issue_key.startswith(STREAM_HEALTH_KEY_PREFIX)
        _put_stream_health(
            fake_redis_client, _sample_health(stream="test:tasks:claude"), issue=False
        )
        _put_stream_health(
            fake_redis_client,
            _sample_health(stream="test:tasks:issue:claude"),
            issue=True,
        )

        scanned = {
            k.decode() if isinstance(k, bytes) else str(k)
            for k in fake_redis_client.client.scan_iter(match=f"{STREAM_HEALTH_KEY_PREFIX}*")
        }
        assert pr_key in scanned
        assert issue_key in scanned


# ---------------------------------------------------------------------------
# Tests for fetch_snapshot provider stream health (issue #640)
# ---------------------------------------------------------------------------


def _stream_health(provider, stream, state, **overrides):
    now = time.time()
    defaults = dict(
        provider=provider,
        stream=stream,
        pending=3,
        lag=1,
        registered_consumers=2,
        live_consumers=0 if state == StreamHealthState.STRANDED else 2,
        state=state,
        observed_at=now,
        transitioned_at=now - 400,
    )
    defaults.update(overrides)
    return ProviderStreamHealth(**defaults)


def _publish_stream_health(redis, provider, stream, state, *, issue=False, **overrides):
    health = _stream_health(provider, stream, state, **overrides)
    redis.client.set(
        stream_health_snapshot_key(provider, issue=issue),
        json.dumps(health.to_dict()),
    )
    return health


class TestFetchSnapshotProviderStreamHealth:
    """PoolManager's canonical stream-health snapshots feed the dashboard
    read-only -- this only exercises collection, never recomputes health."""

    def test_healthy_record_included(self, fake_redis_client):
        _publish_stream_health(
            fake_redis_client, "claude", "tasks:claude", StreamHealthState.HEALTHY
        )

        snap = fetch_snapshot(fake_redis_client)

        assert len(snap.stream_health) == 1
        assert snap.stream_health[0].provider == "claude"
        assert snap.stream_health[0].state == StreamHealthState.HEALTHY

    def test_stranded_record_included(self, fake_redis_client):
        _publish_stream_health(fake_redis_client, "xai", "tasks:xai", StreamHealthState.STRANDED)

        snap = fetch_snapshot(fake_redis_client)

        assert len(snap.stream_health) == 1
        assert snap.stream_health[0].state == StreamHealthState.STRANDED
        assert snap.stream_health[0].live_consumers == 0

    def test_malformed_json_skipped(self, fake_redis_client):
        fake_redis_client.client.set("provider-stream-health:broken", "{not json")

        snap = fetch_snapshot(fake_redis_client)

        assert snap.stream_health == []

    def test_missing_required_field_skipped(self, fake_redis_client):
        fake_redis_client.client.set(
            stream_health_snapshot_key("claude"), json.dumps({"provider": "claude"})
        )

        snap = fetch_snapshot(fake_redis_client)

        assert snap.stream_health == []

    def test_unsupported_state_value_skipped(self, fake_redis_client):
        health = _stream_health("claude", "tasks:claude", StreamHealthState.HEALTHY).to_dict()
        health["state"] = "degraded"
        fake_redis_client.client.set(stream_health_snapshot_key("claude"), json.dumps(health))

        snap = fetch_snapshot(fake_redis_client)

        assert snap.stream_health == []

    def test_multiple_providers_sorted_by_provider(self, fake_redis_client):
        _publish_stream_health(fake_redis_client, "xai", "tasks:xai", StreamHealthState.HEALTHY)
        _publish_stream_health(
            fake_redis_client, "claude", "tasks:claude", StreamHealthState.HEALTHY
        )

        snap = fetch_snapshot(fake_redis_client)

        assert [h.provider for h in snap.stream_health] == ["claude", "xai"]


# ---------------------------------------------------------------------------
# Tests for live dashboard rendering of provider stream health (issue #640)
# ---------------------------------------------------------------------------


def _run_async(coro):
    """Drive a Textual pilot scenario without a pytest-asyncio plugin."""
    return asyncio.run(coro)


def _build_dashboard_app(redis, refresh_interval=3.0):
    """Construct run_dashboard's Textual App without entering its blocking
    App.run() loop, so tests can drive it headlessly via run_test()."""
    from textual.app import App

    captured = {}
    original_run = App.run

    def _capture_run(self):
        captured["app"] = self

    App.run = _capture_run
    try:
        run_dashboard(redis, refresh_interval=refresh_interval)
    finally:
        App.run = original_run
    return captured["app"]


class TestDashboardStreamHealthRendering:
    """Live TUI rendering of canonical provider stream-health snapshots."""

    def test_stranded_state_shows_prominent_banner(self, fake_redis_client):
        from textual.widgets import DataTable, Static

        _publish_stream_health(fake_redis_client, "xai", "tasks:xai", StreamHealthState.STRANDED)

        async def scenario():
            app = _build_dashboard_app(fake_redis_client)
            async with app.run_test() as pilot:
                await pilot.pause()
                banner = app.query_one("#stream-health-banner", Static)
                assert "visible" in banner.classes
                table = app.query_one("#stream-health-table", DataTable)
                assert table.row_count == 1

        _run_async(scenario())

    def test_healthy_state_hides_banner(self, fake_redis_client):
        from textual.widgets import Static

        _publish_stream_health(
            fake_redis_client, "claude", "tasks:claude", StreamHealthState.HEALTHY
        )

        async def scenario():
            app = _build_dashboard_app(fake_redis_client)
            async with app.run_test() as pilot:
                await pilot.pause()
                banner = app.query_one("#stream-health-banner", Static)
                assert "visible" not in banner.classes

        _run_async(scenario())

    def test_multiple_providers_and_streams_render_without_collision(self, fake_redis_client):
        from textual.widgets import DataTable, Static

        _publish_stream_health(
            fake_redis_client, "claude", "tasks:claude", StreamHealthState.HEALTHY
        )
        _publish_stream_health(fake_redis_client, "xai", "tasks:xai", StreamHealthState.STRANDED)

        async def scenario():
            app = _build_dashboard_app(fake_redis_client)
            async with app.run_test() as pilot:
                await pilot.pause()
                table = app.query_one("#stream-health-table", DataTable)
                assert table.row_count == 2
                banner = app.query_one("#stream-health-banner", Static)
                assert "visible" in banner.classes

        _run_async(scenario())

    def test_stranded_stream_disappears_on_refresh(self, fake_redis_client):
        from textual.widgets import DataTable, Static

        _publish_stream_health(fake_redis_client, "xai", "tasks:xai", StreamHealthState.STRANDED)

        async def scenario():
            app = _build_dashboard_app(fake_redis_client)
            async with app.run_test() as pilot:
                await pilot.pause()
                banner = app.query_one("#stream-health-banner", Static)
                assert "visible" in banner.classes

                fake_redis_client.client.delete(stream_health_snapshot_key("xai"))
                app.action_refresh()
                await pilot.pause()

                banner = app.query_one("#stream-health-banner", Static)
                assert "visible" not in banner.classes
                table = app.query_one("#stream-health-table", DataTable)
                assert table.row_count == 1  # placeholder "--" row, stream gone

        _run_async(scenario())

    def test_malformed_record_treated_as_unavailable_not_crash(self, fake_redis_client):
        from textual.widgets import DataTable, Static

        fake_redis_client.client.set("provider-stream-health:broken", "{not json")

        async def scenario():
            app = _build_dashboard_app(fake_redis_client)
            async with app.run_test() as pilot:
                await pilot.pause()
                banner = app.query_one("#stream-health-banner", Static)
                assert "visible" not in banner.classes
                table = app.query_one("#stream-health-table", DataTable)
                assert table.row_count == 1  # placeholder row, no crash

        _run_async(scenario())

    def test_stranded_banner_quotes_then_escapes_bracketed_stream_names(self):
        """repr() first, then rich_escape, so [ and ] stay quoted literals.

        The previous ``rich_escape(h.stream)!r`` order doubled backslashes
        (``tasks:xai\\\\[test\\\\]``) and could re-expose brackets to Rich
        markup parsing (issue #644).
        """
        from rich.markup import escape as rich_escape
        from rich.text import Text

        stream = "tasks:xai[test]"
        health = _stream_health("xai", stream, StreamHealthState.STRANDED)

        markup = _format_stranded_stream_banner(health)

        assert rich_escape(repr(stream)) in markup
        assert f"{rich_escape(stream)!r}" not in markup
        assert r"\\[" not in markup
        assert r"\\]" not in markup

        plain = Text.from_markup(markup).plain
        assert repr(stream) in plain
        assert r"\[" not in plain
        assert r"\]" not in plain

    def test_stranded_stream_name_with_brackets_renders_cleanly(self, fake_redis_client):
        from textual.widgets import Static

        stream = "tasks:xai[test]"
        health = _publish_stream_health(
            fake_redis_client, "xai", stream, StreamHealthState.STRANDED
        )

        async def scenario():
            app = _build_dashboard_app(fake_redis_client)
            async with app.run_test() as pilot:
                await pilot.pause()
                banner = app.query_one("#stream-health-banner", Static)
                assert "visible" in banner.classes
                expected = _format_stranded_stream_banner(health)
                assert expected in banner.content
                assert repr(stream) in str(banner.visual)
                assert r"\[" not in str(banner.visual)
                assert r"\\[" not in banner.content

        _run_async(scenario())


# ---------------------------------------------------------------------------
# M2-sec: TypeScript dashboard auth must FAIL CLOSED (source-text guards).
#
# The fail-open bug lives in the TypeScript dashboard (dashboard/server/*.ts),
# a different codebase from the Python ``orcest.dashboard`` module exercised
# above — a Python pytest cannot import or call the TS ``isAuthorized``. The
# faithful regression pin is the vitest test in dashboard/server/auth.test.ts.
# These coarse text-assertions are a pytest-runnable guard so ``make
# test-unit`` (Python-only) still fails if the fail-open code or the
# all-interfaces port binding is reintroduced.
# ---------------------------------------------------------------------------


class TestDashboardAuthFailsClosed:
    """M2-sec: the TS dashboard must deny when no token is configured and the
    published port must bind to loopback only."""

    def _repo_root(self):
        from pathlib import Path

        return Path(__file__).resolve().parents[1]

    def _run_dashboard_guard(self, *, env=None):
        guard_env = os.environ.copy()
        if env:
            guard_env.update(env)
        return subprocess.run(
            ["sh", "dashboard/scripts/check-tracked-files.sh"],
            cwd=self._repo_root(),
            check=False,
            capture_output=True,
            text=True,
            env=guard_env,
        )

    def test_isauthorized_does_not_fail_open(self):
        """The fail-open shortcut ``if (!DASHBOARD_TOKEN) return true;`` must be
        gone from every dashboard server source file."""
        server_dir = self._repo_root() / "dashboard" / "server"
        for ts_file in server_dir.glob("*.ts"):
            text = ts_file.read_text()
            assert "return true" not in text or "!DASHBOARD_TOKEN" not in text, (
                f"{ts_file} still fails open: a missing DASHBOARD_TOKEN must DENY, not allow"
            )
            # The specific buggy line must not reappear in any form.
            assert "if (!DASHBOARD_TOKEN) return true" not in text, (
                f"{ts_file} reintroduced the fail-open auth shortcut"
            )

    def test_auth_module_extracted_and_fails_closed(self):
        """Auth logic lives in an importable, side-effect-free auth.ts that
        fails closed when no token is configured."""
        auth_ts = self._repo_root() / "dashboard" / "server" / "auth.ts"
        assert auth_ts.exists(), "auth logic must be extracted into dashboard/server/auth.ts"
        text = auth_ts.read_text()
        assert "export function isAuthorized" in text
        # Fail closed on an unset token.
        assert "if (!token) return false" in text
        # The constant-time comparison must be preserved.
        assert "timingSafeEqual" in text
        # auth.ts must NOT import index.ts (which has the server.listen side
        # effect) — that is the whole point of extracting it.
        assert "./index" not in text

    def test_index_imports_isauthorized_from_auth_module(self):
        """index.ts must consume the shared isAuthorized rather than keep its
        own (previously fail-open) copy."""
        index_ts = self._repo_root() / "dashboard" / "server" / "index.ts"
        text = index_ts.read_text()
        assert 'from "./auth.js"' in text, "index.ts must import isAuthorized from ./auth.js"
        # index.ts must no longer define its own isAuthorized.
        assert "function isAuthorized" not in text

    def test_published_dashboard_port_binds_to_loopback(self):
        """docker-compose.dashboard.yml must publish the port on 127.0.0.1, not
        on all interfaces."""
        compose = self._repo_root() / "docker-compose.dashboard.yml"
        text = compose.read_text()
        assert "127.0.0.1:8080:8080" in text, (
            "dashboard port must bind to loopback (127.0.0.1:8080:8080)"
        )
        assert '"8080:8080"' not in text, "dashboard port must not be published on all interfaces"

    def test_compose_requires_dashboard_token(self):
        """Direct docker compose usage must fail before starting a tokenless
        dashboard that would pass health checks but reject every real request."""
        compose = self._repo_root() / "docker-compose.dashboard.yml"
        text = compose.read_text()
        assert "image: ${DASHBOARD_IMAGE:-orcest-dashboard:latest}" in text
        assert "name: ${ORCEST_DOCKER_NETWORK:-orcest}" in text
        assert "DASHBOARD_TOKEN=${DASHBOARD_TOKEN:?DASHBOARD_TOKEN is required}" in text
        assert "LOCK_TTL_SECONDS=180" not in text
        assert "- LOCK_TTL_SECONDS" in text
        assert "- DASHBOARD_LOCK_TTL_SECONDS" in text
        assert "- ORCEST_LOCK_TTL_SECONDS" in text

    def test_dashboard_revision_is_baked_into_image_not_runtime_environment(self):
        repo_root = self._repo_root()
        compose_text = (repo_root / "docker-compose.dashboard.yml").read_text()
        dockerfile_text = (repo_root / "dashboard" / "Dockerfile").read_text()
        deploy_text = (
            repo_root / "dashboard" / "scripts" / "deploy-compose-dashboard.sh"
        ).read_text()

        assert "- ORCEST_BUILD_REVISION=" not in compose_text
        assert "ENV ORCEST_BUILD_REVISION=" not in dockerfile_text
        assert "> /app/.orcest-revision" in dockerfile_text
        assert 'previous_revision=""' in deploy_text
        assert "org.opencontainers.image.revision" in deploy_text
        assert 'DASHBOARD_EXPECTED_REVISION="$previous_revision" check_published' in deploy_text

    def test_deploy_dashboard_wires_required_env_files(self):
        """The dashboard compose deploy target must pass Redis auth and require
        a dashboard token before starting the fail-closed service."""
        makefile = self._repo_root() / "Makefile"
        text = makefile.read_text()
        assert "DASHBOARD_REDIS_ENV ?= /opt/orcest/.redis.env" in text
        assert "DASHBOARD_ENV ?= /opt/orcest/.dashboard.env" in text
        assert 'set -- --env-file "$(DASHBOARD_REDIS_ENV)"' in text
        assert 'set -- "$$@" --env-file "$(DASHBOARD_ENV)"' in text
        assert "DASHBOARD_TOKEN" in text
        assert "dashboard/scripts/deploy-compose-dashboard.sh" in text
        assert 'DASHBOARD_ENV_FILE="$$published_env_file"' in text
        assert "DASHBOARD_REMOTE_COMPOSE" in text
        assert "/api/ready" in text
        assert "DASHBOARD_AUDIT_LEVEL ?= moderate" in text
        assert "npm audit --audit-level=$(DASHBOARD_AUDIT_LEVEL)" in text
        assert "npm run build && npm run check:bundle-runtime" in text

    def test_dashboard_ci_runs_bundle_runtime_smoke_after_build(self):
        """CI must run the production bundle runtime smoke, not stop at build."""
        ci = self._repo_root() / ".github" / "workflows" / "ci.yml"
        makefile = self._repo_root() / "Makefile"
        ci_text = ci.read_text()
        make_text = makefile.read_text()
        assert "run: make test-dashboard" in ci_text
        build_index = make_text.index("npm run build")
        runtime_index = make_text.index("npm run check:bundle-runtime")
        assert build_index < runtime_index

    def test_dashboard_build_uses_root_relative_assets_for_spa_deep_links(self):
        """Deep-linked SPA routes must request /assets/... rather than
        resolving relative bundle URLs beneath the route path."""
        vite_config = self._repo_root() / "dashboard" / "vite.config.ts"
        text = vite_config.read_text()
        assert 'base: "/"' in text
        assert 'base: "./"' not in text

    def test_dashboard_deploy_scripts_do_not_mask_asset_listing_failures(self):
        """Deploy and smoke gates must not hide a failed container asset
        listing behind a successful tr process."""
        makefile = self._repo_root() / "Makefile"
        smoke_compose = self._repo_root() / "dashboard" / "scripts" / "smoke-compose.sh"
        deploy_script = self._repo_root() / "dashboard" / "scripts" / "deploy-compose-dashboard.sh"
        asset_script = self._repo_root() / "dashboard" / "scripts" / "list-published-assets.sh"

        assert (
            "ls -1 dist/assets/index-*.js dist/assets/index-*.css' | tr" not in makefile.read_text()
        )
        assert "deploy-compose-dashboard.sh $compose" in smoke_compose.read_text()
        deploy_text = deploy_script.read_text()
        assert 'dashboard_image="${DASHBOARD_IMAGE:-orcest-dashboard:latest}"' in deploy_text
        assert 'dashboard_image="$(env_file_value DASHBOARD_IMAGE)"' in deploy_text
        assert (
            'rollback_image="${DASHBOARD_ROLLBACK_IMAGE:-orcest-dashboard:rollback-$$}"'
            in deploy_text
        )
        assert (
            'deploy_lock_dir="${DASHBOARD_DEPLOY_LOCK_DIR:-.dashboard-deploy.lock}"' in deploy_text
        )
        assert 'deploy_lock_held="${DASHBOARD_DEPLOY_LOCK_HELD:-0}"' in deploy_text
        assert "acquire_deploy_lock()" in deploy_text
        assert 'acquire_deploy_lock "$@"' in deploy_text
        assert deploy_text.index('acquire_deploy_lock "$@"') < deploy_text.index(
            "previous_container="
        )
        assert "DASHBOARD_DEPLOY_LOCK_HELD=1 but dashboard deploy lock is missing" in deploy_text
        assert "release_deploy_lock()" in deploy_text
        assert "restore_candidate_compose()" in deploy_text
        assert "activate_rollback_compose()" in deploy_text
        assert "refresh_compose_state()" in deploy_text
        assert deploy_text.index("validate_compose_state_paths || exit 1") < deploy_text.index(
            'previous_container="'
        )
        assert deploy_text.index("candidate_tag_replaced=1") < deploy_text.index(
            'DASHBOARD_NODE_VERSION="$node_version" "$@" build dashboard'
        )
        assert deploy_text.index("if ! refresh_compose_state") < deploy_text.index(
            'echo "Dashboard published readiness verified"'
        )
        assert "handle_signal()" in deploy_text
        assert "Dashboard deploy interrupted after candidate start; rolling back" in deploy_text
        assert "Dashboard deploy interrupted before candidate start" in deploy_text
        assert 'docker tag "$previous_image_id" "$rollback_image"' in deploy_text
        assert "restorable_image_name()" in deploy_text
        assert "restore_rollback_image_tag()" in deploy_text
        assert "restore_previous_image_tag()" in deploy_text
        assert 'DASHBOARD_NODE_VERSION="$node_version" "$@" build dashboard' in deploy_text
        assert '"$@" images -q dashboard' not in deploy_text
        assert 'candidate_image="$dashboard_image"' in deploy_text
        assert "docker image inspect -f '{{.Id}}' \"$candidate_image\"" in deploy_text
        assert 'check_candidate_bundle_runtime "$candidate_image"' in deploy_text
        assert 'collect_candidate_assets "$candidate_image"' in deploy_text
        assert "candidate_may_be_live=1" in deploy_text
        assert '"$@" up -d --no-build --force-recreate dashboard' in deploy_text
        assert 'rollback_compose_image="$rollback_image"' in deploy_text
        assert "restore_previous_image_tag" in deploy_text
        assert 'restore_rollback_image_tag "$previous_image_name"' in deploy_text
        assert 'rollback_compose_image="$dashboard_image"' in deploy_text
        assert 'rollback_compose_image="$previous_image_name"' in deploy_text
        assert (
            'DASHBOARD_IMAGE="$rollback_compose_image" "$@" up -d --no-build '
            "--force-recreate dashboard" in deploy_text
        )
        assert "running_image_id" in deploy_text
        assert "did not match candidate image" in deploy_text
        assert 'rollback_dashboard "$@" || true' in deploy_text
        assert 'check_bundle_runtime "$@"' in deploy_text
        assert "node scripts/check-bundle-runtime.mjs" in deploy_text
        assert "Dashboard bundle runtime check failed" in deploy_text
        assert "published_dashboard_network()" in deploy_text
        assert "printf 'container:%s\\n' \"$container\"" in deploy_text
        assert 'DASHBOARD_PUBLISHED_DOCKER_NETWORK="$published_network"' in deploy_text
        assert "DASHBOARD_VERIFY_HOST_PUBLISHED" in deploy_text
        assert (
            'host_published_network="${DASHBOARD_HOST_PUBLISHED_DOCKER_NETWORK:-host}"'
            in deploy_text
        )
        assert 'if truthy "$verify_host_published"; then' in deploy_text
        assert 'DASHBOARD_PUBLISHED_DOCKER_NETWORK="$host_published_network"' in deploy_text
        assert "sh dashboard/scripts/check-published.sh || return $?" in deploy_text
        assert 'check_published "$expected_assets" "$@"' in deploy_text
        assert 'check_published_unpinned "$@"' in deploy_text
        smoke_text = smoke_compose.read_text()
        assert "deploy-compose-dashboard.sh $compose" in smoke_text
        assert "DASHBOARD_VERIFY_HOST_PUBLISHED=1" in smoke_text
        assert "smoke_image=" in smoke_text
        assert "python3 -" in smoke_text
        assert "18080 + $$ % 20000" not in smoke_text
        assert "ports: !override" not in smoke_text
        assert 'compose_file="$(mktemp)"' in smoke_text
        assert '-f "$compose_file"' in smoke_text
        assert 'context: "$repo_root/dashboard"' in smoke_text
        assert "-f docker-compose.dashboard.yml -f" not in smoke_text
        assert "image: $smoke_image" in smoke_text
        assert 'network_name="${DASHBOARD_SMOKE_NETWORK:-${project}-network}"' in smoke_text
        assert "ORCEST_DOCKER_NETWORK=$network_name" in smoke_text
        assert 'docker network create "$network_name"' in smoke_text
        assert "DASHBOARD_STRICT_DEGRADED=1" in smoke_text
        assert "redis_cli HSET tasks:metadata purpose dashboard-smoke" in smoke_text
        assert "redis_cli XADD tasks:claude 1-0" in smoke_text
        assert "verify_seeded_snapshot_contract" in smoke_text
        assert 'queueDepths["tasks:claude"] !== 1' in smoke_text
        assert "non-stream tasks:metadata appeared in queue_depths" in smoke_text
        assert "non-stream tasks:metadata appeared in consumer_groups" in smoke_text
        assert "non-stream tasks:metadata appeared in queued_tasks" in smoke_text
        assert '--network "$network_name"' in smoke_text
        assert "--network orcest" not in smoke_text
        assert 'DASHBOARD_IMAGE="$smoke_image"' in smoke_text
        assert 'DASHBOARD_COMPOSE_STATE_FILE="$compose_state_file"' in smoke_text
        assert 'docker image rm "$smoke_image"' in smoke_text
        smoke_image = self._repo_root() / "dashboard" / "scripts" / "smoke-image.sh"
        smoke_image_text = smoke_image.read_text()
        assert "node scripts/check-bundle-runtime.mjs" in smoke_image_text
        assert "if docker exec -i \\" in smoke_image_text
        assert 'docker run --rm -i --network "container:$container"' in smoke_text
        asset_text = asset_script.read_text()
        assert "exec -T dashboard sh -lc" in asset_text
        assert "tr '\\n' ' ' <\"$asset_file\"" in asset_text

        make_text = makefile.read_text()
        assert "DASHBOARD_REMOTE_COMPOSE_STATE_FILE ?=" in make_text
        assert "Could not seed dashboard last-known-good Compose state" in make_text
        assert make_text.index(
            "Could not seed dashboard last-known-good Compose state"
        ) < make_text.index("rsync -az --delete")

    def test_dashboard_image_smoke_forwards_embedded_node_program(self, tmp_path):
        """The image smoke must keep Docker stdin open so its HTTP/auth/asset
        assertions execute inside the container instead of Node receiving EOF."""
        fake_bin = tmp_path / "bin"
        fake_bin.mkdir()
        marker = tmp_path / "node-program.js"
        docker = fake_bin / "docker"
        docker.write_text(
            """#!/usr/bin/env sh
set -eu
case "${1:-}" in
  run)
    printf '%s\n' smoke-container
    ;;
  exec)
    interactive=0
    bundle=0
    assets=0
    for arg do
      [ "$arg" = "-i" ] && interactive=1
      [ "$arg" = "scripts/check-bundle-runtime.mjs" ] && bundle=1
      [ "$arg" = "sh" ] && assets=1
    done
    if [ "$assets" = "1" ]; then
      printf '%s\n' dist/assets/index-smoke.js dist/assets/index-smoke.css
    elif [ "$bundle" = "0" ] && [ "$interactive" = "1" ]; then
      cat > "$STDIN_MARKER"
    fi
    ;;
  logs|rm)
    ;;
esac
exit 0
"""
        )
        docker.chmod(0o755)

        env = os.environ.copy()
        env["PATH"] = f"{fake_bin}:{env['PATH']}"
        env["STDIN_MARKER"] = str(marker)
        result = subprocess.run(
            ["sh", "dashboard/scripts/smoke-image.sh", "dashboard:test"],
            cwd=self._repo_root(),
            capture_output=True,
            text=True,
            env=env,
        )

        assert result.returncode == 0, result.stderr
        program = marker.read_text()
        assert "async function main()" in program
        assert 'const deepLinkPath = "/work/results"' in program

    def test_dashboard_compose_smoke_forwards_seeded_snapshot_program(self, tmp_path):
        """The Compose smoke must execute its seeded Redis contract program,
        not accept a successful Node process that received an empty stdin."""
        fake_bin = tmp_path / "bin"
        fake_bin.mkdir()
        marker = tmp_path / "node-programs.js"
        up_marker = tmp_path / "dashboard-up"
        docker = fake_bin / "docker"
        docker.write_text(
            """#!/usr/bin/env sh
set -eu
has_arg() {
  expected="$1"
  shift
  for arg do
    [ "$arg" = "$expected" ] && return 0
  done
  return 1
}
if [ "${1:-}" = "compose" ]; then
  action=""
  for arg do
    case "$arg" in ps|build|up|exec|logs|down|rm) action="$arg"; break ;; esac
  done
  case "$action" in
    ps) [ -f "$UP_MARKER" ] && printf '%s\n' dashboard-container ;;
    up) : > "$UP_MARKER" ;;
    exec|build|logs|down|rm) ;;
  esac
  exit 0
fi
case "${1:-}" in
  network|rm)
    exit 0
    ;;
  exec)
    exit 0
    ;;
  inspect)
    case "${3:-}" in
      "{{.Image}}") printf '%s\n' sha256:candidate ;;
      "{{.Config.Image}}") printf '%s\n' dashboard:test ;;
    esac
    exit 0
    ;;
  image)
    [ "${2:-}" = "inspect" ] && printf '%s\n' sha256:candidate
    exit 0
    ;;
  run)
    if has_arg --input-type=module "$@"; then
      if has_arg -i "$@"; then
        cat >> "$STDIN_MARKER"
        printf '\n' >> "$STDIN_MARKER"
      fi
      exit 0
    fi
    if has_arg scripts/check-bundle-runtime.mjs "$@"; then
      exit 0
    fi
    if has_arg sh "$@"; then
      printf '%s\n' dist/assets/index-smoke.js dist/assets/index-smoke.css
    fi
    exit 0
    ;;
esac
exit 0
"""
        )
        docker.chmod(0o755)

        env = os.environ.copy()
        env["PATH"] = f"{fake_bin}:{env['PATH']}"
        env["STDIN_MARKER"] = str(marker)
        env["UP_MARKER"] = str(up_marker)
        env["DASHBOARD_SMOKE_IMAGE"] = "dashboard:test"
        env["DASHBOARD_SMOKE_NODE_IMAGE"] = "node:test"
        result = subprocess.run(
            ["sh", "dashboard/scripts/smoke-compose.sh"],
            cwd=self._repo_root(),
            capture_output=True,
            text=True,
            env=env,
        )

        assert result.returncode == 0, result.stderr
        programs = marker.read_text()
        assert "non-stream tasks:metadata appeared in queue_depths" in programs
        assert 'queueDepths["tasks:claude"] !== 1' in programs

    def test_dashboard_deploy_refuses_concurrent_lock(self, tmp_path):
        """Concurrent dashboard deploys must not race image tags or rollback
        state; an existing lock fails before compose/docker mutation."""
        lock_dir = tmp_path / "dashboard-deploy.lock"
        lock_dir.mkdir()
        (lock_dir / "info").write_text("pid=123\nhost=existing\n")
        calls = tmp_path / "compose-calls.log"
        compose = tmp_path / "compose"
        compose.write_text(
            """#!/usr/bin/env sh
set -eu
printf '%s\\n' "$*" >> "$COMPOSE_CALLS"
exit 99
"""
        )
        compose.chmod(0o755)

        fake_bin = tmp_path / "bin"
        fake_bin.mkdir()
        docker = fake_bin / "docker"
        docker.write_text(
            """#!/usr/bin/env sh
set -eu
echo "docker should not be called while deploy lock is held" >&2
exit 99
"""
        )
        docker.chmod(0o755)

        env = os.environ.copy()
        env["PATH"] = f"{fake_bin}:{env['PATH']}"
        env["COMPOSE_CALLS"] = str(calls)
        env["DASHBOARD_DEPLOY_LOCK_DIR"] = str(lock_dir)

        result = subprocess.run(
            ["sh", "dashboard/scripts/deploy-compose-dashboard.sh", str(compose)],
            cwd=self._repo_root(),
            capture_output=True,
            text=True,
            env=env,
        )

        assert result.returncode == 1
        assert f"Dashboard deploy lock is already held: {lock_dir}" in result.stderr
        assert "pid=123" in result.stderr
        assert "host=existing" in result.stderr
        assert "Refusing to run a concurrent dashboard deploy" in result.stderr
        assert not calls.exists()
        assert lock_dir.exists()

    def test_dashboard_deploy_validates_compose_state_before_mutation(self, tmp_path):
        """An invalid state path must fail before Compose or Docker can inspect,
        build, tag, or start anything."""
        compose_file = tmp_path / "docker-compose.yml"
        compose_file.write_text("services: {}\n")
        state_target = tmp_path / "state-target.yml"
        state_target.write_text("services: {}\n")
        state_link = tmp_path / "compose-state.yml"
        state_link.symlink_to(state_target)
        compose_calls = tmp_path / "compose-calls.log"
        docker_calls = tmp_path / "docker-calls.log"
        fake_bin = tmp_path / "bin"
        fake_bin.mkdir()
        for name, calls in (("compose", compose_calls), ("docker", docker_calls)):
            command = fake_bin / name
            command.write_text(f"#!/usr/bin/env sh\nprintf '%s\\n' \"$*\" >> {calls}\nexit 99\n")
            command.chmod(0o755)

        env = os.environ.copy()
        env["PATH"] = f"{fake_bin}:{env['PATH']}"
        env["DASHBOARD_COMPOSE_FILE"] = str(compose_file)
        env["DASHBOARD_COMPOSE_STATE_FILE"] = str(state_link)
        env["DASHBOARD_DEPLOY_LOCK_DIR"] = str(tmp_path / "deploy.lock")
        result = subprocess.run(
            ["sh", "dashboard/scripts/deploy-compose-dashboard.sh", str(fake_bin / "compose")],
            cwd=self._repo_root(),
            capture_output=True,
            text=True,
            env=env,
        )

        assert result.returncode == 1
        assert "state file must be a regular file, not a symlink" in result.stderr
        assert not compose_calls.exists()
        assert not docker_calls.exists()

    def test_dashboard_deploy_validates_candidate_before_live_start(
        self,
        tmp_path,
    ):
        """A bad newly built bundle must fail before compose replaces the
        currently served dashboard."""
        calls = tmp_path / "compose-calls.log"
        docker_calls = tmp_path / "docker-calls.log"
        compose = tmp_path / "compose"
        compose.write_text(
            """#!/usr/bin/env sh
set -eu
printf '%s\\n' "$*" >> "$COMPOSE_CALLS"
case "$1" in
  ps)
    exit 0
    ;;
    build)
      exit 0
      ;;
    up)
      echo "live service should not be started before candidate validation" >&2
      exit 99
    ;;
esac
exit 1
"""
        )
        compose.chmod(0o755)

        fake_bin = tmp_path / "bin"
        fake_bin.mkdir()
        docker = fake_bin / "docker"
        docker.write_text(
            """#!/usr/bin/env sh
set -eu
  printf '%s\\n' "$*" >> "$DOCKER_CALLS"
  if [ "${1:-}" = "image" ] && [ "${2:-}" = "inspect" ]; then
    printf '%s\\n' sha256:candidate
    exit 0
  fi
  if [ "${1:-}" = "run" ]; then
    exit 1
  fi
exit 0
"""
        )
        docker.chmod(0o755)

        env = os.environ.copy()
        for key in [
            "DASHBOARD_TOKEN",
            "DASHBOARD_READY_ATTEMPTS",
            "DASHBOARD_READY_INTERVAL_MS",
            "DASHBOARD_ALLOW_DEGRADED",
            "DASHBOARD_STRICT_DEGRADED",
            "DASHBOARD_ALLOW_UNPINNED_ASSETS",
        ]:
            env.pop(key, None)
        env["PATH"] = f"{fake_bin}:{env['PATH']}"
        env["COMPOSE_CALLS"] = str(calls)
        env["DOCKER_CALLS"] = str(docker_calls)
        env["DASHBOARD_TOKEN"] = "test-token"
        env["DASHBOARD_NODE_IMAGE"] = "fake-node"
        lock_dir = tmp_path / "deploy.lock"
        env["DASHBOARD_DEPLOY_LOCK_DIR"] = str(lock_dir)

        result = subprocess.run(
            ["sh", "dashboard/scripts/deploy-compose-dashboard.sh", str(compose)],
            cwd=self._repo_root(),
            capture_output=True,
            text=True,
            env=env,
        )

        assert result.returncode == 1
        assert "Dashboard candidate bundle runtime check failed" in result.stderr
        assert "build dashboard" in calls.read_text()
        assert "images -q dashboard" not in calls.read_text()
        assert "up " not in calls.read_text()
        docker_text = docker_calls.read_text()
        assert "image inspect -f {{.Id}} orcest-dashboard:latest" in docker_text
        assert (
            "run --rm orcest-dashboard:latest node scripts/check-bundle-runtime.mjs" in docker_text
        )
        assert "image rm orcest-dashboard:latest" in docker_text
        assert not lock_dir.exists()

    def test_dashboard_deploy_falls_back_to_explicit_candidate_image(
        self,
        tmp_path,
    ):
        """Compose image discovery can fail against a stale running container;
        the deploy wrapper should still validate the explicit service image."""
        calls = tmp_path / "compose-calls.log"
        docker_calls = tmp_path / "docker-calls.log"
        compose = tmp_path / "compose"
        compose.write_text(
            """#!/usr/bin/env sh
set -eu
printf '%s\\n' "$*" >> "$COMPOSE_CALLS"
case "$1" in
  ps)
    printf '%s\n' previous-container
    exit 0
    ;;
    build)
      exit 0
      ;;
    up)
      exit 1
      ;;
  logs|rm)
    exit 0
    ;;
esac
exit 1
"""
        )
        compose.chmod(0o755)

        fake_bin = tmp_path / "bin"
        fake_bin.mkdir()
        docker = fake_bin / "docker"
        docker.write_text(
            """#!/usr/bin/env sh
set -eu
printf '%s\\n' "$*" >> "$DOCKER_CALLS"
case "$1" in
  inspect)
    case "$3" in
      "{{.Image}}")
        printf '%s\n' sha256:previous
        ;;
      "{{.Config.Image}}")
        printf '%s\n' custom-previous:old
        ;;
    esac
    exit 0
    ;;
    image)
      if [ "${2:-}" = "inspect" ] && [ "${3:-}" = "-f" ] &&
        [ "${5:-}" = "orcest-dashboard:latest" ]; then
        printf '%s\\n' sha256:candidate
        exit 0
      fi
      ;;
    run)
      if [ "${3:-}" = "orcest-dashboard:latest" ] && [ "${4:-}" = "node" ]; then
        exit 0
      fi
      if [ "${3:-}" = "orcest-dashboard:latest" ] && [ "${4:-}" = "sh" ]; then
        printf '%s\\n' dist/assets/index-candidate.js dist/assets/index-candidate.css
        exit 0
      fi
      ;;
  tag)
    exit 0
    ;;
esac
exit 1
"""
        )
        docker.chmod(0o755)

        env = os.environ.copy()
        env.pop("DASHBOARD_IMAGE", None)
        env["PATH"] = f"{fake_bin}:{env['PATH']}"
        env["COMPOSE_CALLS"] = str(calls)
        env["DOCKER_CALLS"] = str(docker_calls)
        env["DASHBOARD_TOKEN"] = "test-token"
        env["DASHBOARD_NODE_IMAGE"] = "fake-node"

        result = subprocess.run(
            ["sh", "dashboard/scripts/deploy-compose-dashboard.sh", str(compose)],
            cwd=self._repo_root(),
            capture_output=True,
            text=True,
            env=env,
        )

        assert result.returncode == 1
        assert "Dashboard compose start failed" in result.stderr
        calls_text = calls.read_text()
        assert "build dashboard" in calls_text
        assert "images -q dashboard" not in calls_text
        assert "up -d --no-build --force-recreate dashboard" in calls_text
        docker_text = docker_calls.read_text()
        assert "image inspect -f {{.Id}} orcest-dashboard:latest" in docker_text
        assert (
            "run --rm orcest-dashboard:latest node scripts/check-bundle-runtime.mjs" in docker_text
        )
        assert "run --rm orcest-dashboard:latest sh -lc" in docker_text
        assert "image inspect custom-previous:old" not in docker_text
        assert (
            "run --rm custom-previous:old node scripts/check-bundle-runtime.mjs" not in docker_text
        )
        assert "tag sha256:previous orcest-dashboard:rollback-" in docker_text
        assert "up -d --no-build --force-recreate dashboard" in calls_text

    def test_dashboard_deploy_validates_env_file_dashboard_image(
        self,
        tmp_path,
    ):
        """The deploy wrapper must validate the image selected through Compose
        env files, not only the hard-coded default image name."""
        calls = tmp_path / "compose-calls.log"
        docker_calls = tmp_path / "docker-calls.log"
        env_file = tmp_path / ".dashboard.env"
        env_file.write_text('DASHBOARD_IMAGE="custom-dashboard:env" # comment\n')
        compose = tmp_path / "compose"
        compose.write_text(
            """#!/usr/bin/env sh
set -eu
printf '%s\\n' "$*" >> "$COMPOSE_CALLS"
case "$1" in
  ps)
    exit 0
    ;;
  build)
    exit 0
    ;;
  up)
    echo "live service should not be started before candidate validation" >&2
    exit 99
    ;;
esac
exit 1
"""
        )
        compose.chmod(0o755)

        fake_bin = tmp_path / "bin"
        fake_bin.mkdir()
        docker = fake_bin / "docker"
        docker.write_text(
            """#!/usr/bin/env sh
set -eu
printf '%s\\n' "$*" >> "$DOCKER_CALLS"
if [ "${1:-}" = "image" ] && [ "${2:-}" = "inspect" ] && [ "${5:-}" = "custom-dashboard:env" ]; then
  printf '%s\\n' sha256:candidate
  exit 0
fi
if [ "${1:-}" = "run" ] && [ "${3:-}" = "custom-dashboard:env" ] && [ "${4:-}" = "node" ]; then
  exit 1
fi
if [ "${1:-}" = "image" ] && [ "${2:-}" = "rm" ]; then
  exit 0
fi
exit 1
"""
        )
        docker.chmod(0o755)

        env = os.environ.copy()
        env.pop("DASHBOARD_IMAGE", None)
        env["PATH"] = f"{fake_bin}:{env['PATH']}"
        env["COMPOSE_CALLS"] = str(calls)
        env["DOCKER_CALLS"] = str(docker_calls)
        env["DASHBOARD_TOKEN"] = "test-token"
        env["DASHBOARD_NODE_IMAGE"] = "fake-node"
        env["DASHBOARD_ENV_FILE"] = str(env_file)

        result = subprocess.run(
            ["sh", "dashboard/scripts/deploy-compose-dashboard.sh", str(compose)],
            cwd=self._repo_root(),
            capture_output=True,
            text=True,
            env=env,
        )

        assert result.returncode == 1
        assert "Dashboard candidate bundle runtime check failed" in result.stderr
        assert "up " not in calls.read_text()
        docker_text = docker_calls.read_text()
        assert "image inspect -f {{.Id}} custom-dashboard:env" in docker_text
        assert "run --rm custom-dashboard:env node scripts/check-bundle-runtime.mjs" in docker_text
        assert "image rm custom-dashboard:env" in docker_text

    def test_dashboard_runtime_bundle_check_is_packaged_with_image(self):
        """Deploy gates execute the production bundle inside the runtime image,
        so the script and DOM runtime must be available after npm --omit=dev."""
        repo_root = self._repo_root()
        package = json.loads((repo_root / "dashboard" / "package.json").read_text())
        package_lock = json.loads((repo_root / "dashboard" / "package-lock.json").read_text())
        dockerfile = (repo_root / "dashboard" / "Dockerfile").read_text()
        runtime_script = (
            repo_root / "dashboard" / "scripts" / "check-bundle-runtime.mjs"
        ).read_text()

        assert package["scripts"]["check:bundle-runtime"] == "node scripts/check-bundle-runtime.mjs"
        assert "happy-dom" in package["dependencies"]
        assert "happy-dom" not in package["devDependencies"]
        assert "happy-dom" in package_lock["packages"][""]["dependencies"]
        assert "dev" not in package_lock["packages"]["node_modules/happy-dom"]
        assert (
            "COPY --from=builder /app/scripts/check-bundle-runtime.mjs "
            "./scripts/check-bundle-runtime.mjs" in dockerfile
        )
        assert "new Window" in runtime_script
        assert "class SmokeWebSocket" in runtime_script
        assert "Dashboard bundle runtime verified" in runtime_script

    def test_dashboard_deploy_removes_failed_fresh_service(self, tmp_path):
        """A first dashboard deploy that fails published readiness must not
        leave the newly started bad service running."""
        calls = tmp_path / "compose-calls.log"
        compose = tmp_path / "compose"
        compose.write_text(
            """#!/usr/bin/env sh
  set -eu
  printf '%s\\n' "$*" >> "$COMPOSE_CALLS"
  printf 'DASHBOARD_IMAGE=%s\\n' "${DASHBOARD_IMAGE:-}" >> "$COMPOSE_CALLS"
  case "$1" in
    ps)
      if [ -f "$COMPOSE_CALLS.up" ]; then
        printf '%s\\n' candidate-container
      fi
      exit 0
      ;;
    build)
      exit 0
      ;;
    up)
      : > "$COMPOSE_CALLS.up"
      exit 0
      ;;
  exec)
    printf '%s\\n' dist/assets/index-new.js dist/assets/index-new.css
    exit 0
    ;;
  logs|rm)
    exit 0
    ;;
esac
exit 1
"""
        )
        compose.chmod(0o755)

        fake_bin = tmp_path / "bin"
        fake_bin.mkdir()
        docker = fake_bin / "docker"
        docker.write_text(
            """#!/usr/bin/env sh
set -eu
  if [ "${1:-}" = "image" ] && [ "${2:-}" = "inspect" ]; then
    printf '%s\\n' sha256:candidate
    exit 0
  fi
  if [ "${1:-}" = "inspect" ] && [ "${3:-}" = "{{.Image}}" ]; then
    printf '%s\\n' sha256:candidate
    exit 0
  fi
  if [ "${1:-}" = "run" ]; then
    if [ "${3:-}" = "orcest-dashboard:latest" ] && [ "${4:-}" = "node" ]; then
      exit 0
    fi
    if [ "${3:-}" = "orcest-dashboard:latest" ] && [ "${4:-}" = "sh" ]; then
      printf '%s\\n' dist/assets/index-new.js dist/assets/index-new.css
      exit 0
    fi
  exit 1
fi
exit 0
"""
        )
        docker.chmod(0o755)

        env = os.environ.copy()
        for key in [
            "DASHBOARD_TOKEN",
            "DASHBOARD_READY_ATTEMPTS",
            "DASHBOARD_READY_INTERVAL_MS",
            "DASHBOARD_ALLOW_DEGRADED",
            "DASHBOARD_STRICT_DEGRADED",
            "DASHBOARD_ALLOW_UNPINNED_ASSETS",
        ]:
            env.pop(key, None)
        env["PATH"] = f"{fake_bin}:{env['PATH']}"
        env["COMPOSE_CALLS"] = str(calls)
        env["DASHBOARD_TOKEN"] = "test-token"
        env["DASHBOARD_READY_ATTEMPTS"] = "1"
        env["DASHBOARD_NODE_IMAGE"] = "fake-node"

        result = subprocess.run(
            ["sh", "dashboard/scripts/deploy-compose-dashboard.sh", str(compose)],
            cwd=self._repo_root(),
            capture_output=True,
            text=True,
            env=env,
        )

        assert result.returncode == 1
        assert "Dashboard did not become published-ready" in result.stderr
        assert "No previous dashboard image is available for rollback" in result.stderr
        calls_text = calls.read_text()
        assert "build dashboard" in calls_text
        assert "images -q dashboard" not in calls_text
        assert "up -d --no-build --force-recreate dashboard" in calls_text
        assert "exec -T dashboard" in calls_text
        assert "rm -sf dashboard" in calls_text

    def test_dashboard_deploy_signal_before_start_restores_previous_image_tag(
        self,
        tmp_path,
    ):
        """A deferred signal returned by foreground Compose build must restore
        the tag that build replaced, even though no candidate container ran."""
        calls = tmp_path / "compose-calls.log"
        docker_calls = tmp_path / "docker-calls.log"
        compose_file = tmp_path / "docker-compose.yml"
        compose_file.write_text("services:\n  dashboard:\n    image: dashboard:test\n")
        compose = tmp_path / "compose"
        compose.write_text(
            """#!/usr/bin/env sh
set -eu
printf '%s\n' "$*" >> "$COMPOSE_CALLS"
case "$1" in
  ps) printf '%s\n' previous-container ;;
  exec) printf '%s\n' dist/assets/index-previous.js dist/assets/index-previous.css ;;
  build) kill -TERM "$PPID" ;;
  up) echo "candidate must not start" >&2; exit 99 ;;
esac
exit 0
"""
        )
        compose.chmod(0o755)

        fake_bin = tmp_path / "bin"
        fake_bin.mkdir()
        docker = fake_bin / "docker"
        docker.write_text(
            """#!/usr/bin/env sh
set -eu
printf '%s\n' "$*" >> "$DOCKER_CALLS"
case "$1" in
  inspect)
    case "$3" in
      "{{.Image}}") printf '%s\n' sha256:previous ;;
      "{{.Config.Image}}") printf '%s\n' dashboard:test ;;
    esac
    ;;
  image)
    if [ "${2:-}" = "inspect" ]; then
      printf '%s\n' sha256:candidate
    fi
    ;;
  tag)
    ;;
  run) ;;
esac
exit 0
"""
        )
        docker.chmod(0o755)

        env = os.environ.copy()
        env["PATH"] = f"{fake_bin}:{env['PATH']}"
        env["COMPOSE_CALLS"] = str(calls)
        env["DOCKER_CALLS"] = str(docker_calls)
        env["DASHBOARD_IMAGE"] = "dashboard:test"
        env["DASHBOARD_NODE_IMAGE"] = "node:test"
        env["DASHBOARD_COMPOSE_FILE"] = str(compose_file)
        env["DASHBOARD_COMPOSE_STATE_FILE"] = str(tmp_path / "compose-state.yml")
        env["DASHBOARD_DEPLOY_LOCK_DIR"] = str(tmp_path / "deploy.lock")
        result = subprocess.run(
            ["sh", "dashboard/scripts/deploy-compose-dashboard.sh", str(compose)],
            cwd=self._repo_root(),
            capture_output=True,
            text=True,
            env=env,
        )

        assert result.returncode == 143
        assert "interrupted before candidate start" in result.stderr
        assert "up " not in calls.read_text()
        docker_text = docker_calls.read_text()
        assert "tag sha256:previous orcest-dashboard:rollback-" in docker_text
        assert any(
            line.startswith("tag orcest-dashboard:rollback-") and line.endswith(" dashboard:test")
            for line in docker_text.splitlines()
        )

    def test_dashboard_rollback_uses_known_good_compose_and_restores_candidate_file(
        self,
        tmp_path,
    ):
        """A candidate config that cannot start must be rolled back with the
        persisted stable config, without replacing the synced candidate file."""
        calls = tmp_path / "compose-calls.log"
        docker_calls = tmp_path / "docker-calls.log"
        failed_marker = tmp_path / "candidate-failed"
        rollback_signal_marker = tmp_path / "rollback-signaled"
        compose_file = tmp_path / "docker-compose.yml"
        compose_state = tmp_path / "compose-state.yml"
        candidate_config = "services:\n  dashboard:\n    x-release: candidate\n"
        stable_config = "services:\n  dashboard:\n    x-release: stable\n"
        compose_file.write_text(candidate_config)
        compose_state.write_text(stable_config)
        compose = tmp_path / "compose"
        compose.write_text(
            """#!/usr/bin/env sh
set -eu
release=$(sed -n 's/.*x-release: //p' "$DASHBOARD_COMPOSE_FILE")
printf '%s|%s|DASHBOARD_IMAGE=%s\n' "$1" "$release" "${DASHBOARD_IMAGE:-}" >> "$COMPOSE_CALLS"
case "$1" in
  ps) printf '%s\n' previous-container ;;
  exec) printf '%s\n' dist/assets/index-previous.js dist/assets/index-previous.css ;;
  build|logs) ;;
  up)
    if [ "$release" = "candidate" ] && [ ! -e "$FAILED_MARKER" ]; then
      : > "$FAILED_MARKER"
      exit 1
    fi
    if [ "$release" = "stable" ] && [ ! -e "$ROLLBACK_SIGNAL_MARKER" ]; then
      : > "$ROLLBACK_SIGNAL_MARKER"
      kill -TERM "$PPID"
    fi
    ;;
esac
exit 0
"""
        )
        compose.chmod(0o755)

        fake_bin = tmp_path / "bin"
        fake_bin.mkdir()
        docker = fake_bin / "docker"
        docker.write_text(
            """#!/usr/bin/env sh
set -eu
printf '%s\n' "$*" >> "$DOCKER_CALLS"
case "$1" in
  inspect)
    case "$3" in
      "{{.Image}}") printf '%s\n' sha256:previous ;;
      "{{.Config.Image}}") printf '%s\n' dashboard:test ;;
    esac
    ;;
  image)
    [ "${2:-}" = "inspect" ] && printf '%s\n' sha256:candidate
    ;;
  tag|rm)
    ;;
  run)
    for arg do
      [ "$arg" = "scripts/check-bundle-runtime.mjs" ] && exit 0
      if [ "$arg" = "sh" ]; then
        printf '%s\n' dist/assets/index-candidate.js dist/assets/index-candidate.css
        exit 0
      fi
    done
    ;;
esac
exit 0
"""
        )
        docker.chmod(0o755)

        env = os.environ.copy()
        env["PATH"] = f"{fake_bin}:{env['PATH']}"
        env["COMPOSE_CALLS"] = str(calls)
        env["DOCKER_CALLS"] = str(docker_calls)
        env["FAILED_MARKER"] = str(failed_marker)
        env["ROLLBACK_SIGNAL_MARKER"] = str(rollback_signal_marker)
        env["DASHBOARD_IMAGE"] = "dashboard:test"
        env["DASHBOARD_NODE_IMAGE"] = "node:test"
        env["DASHBOARD_COMPOSE_FILE"] = str(compose_file)
        env["DASHBOARD_COMPOSE_STATE_FILE"] = str(compose_state)
        env["DASHBOARD_DEPLOY_LOCK_DIR"] = str(tmp_path / "deploy.lock")
        result = subprocess.run(
            ["sh", "dashboard/scripts/deploy-compose-dashboard.sh", str(compose)],
            cwd=self._repo_root(),
            capture_output=True,
            text=True,
            env=env,
        )

        assert result.returncode == 143
        assert "using last-known-good Compose configuration" in result.stderr
        assert "interrupted after candidate start" in result.stderr
        up_calls = [line for line in calls.read_text().splitlines() if line.startswith("up|")]
        assert up_calls == [
            "up|candidate|DASHBOARD_IMAGE=dashboard:test",
            "up|stable|DASHBOARD_IMAGE=dashboard:test",
            "up|stable|DASHBOARD_IMAGE=dashboard:test",
        ]
        assert compose_file.read_text() == candidate_config
        assert compose_state.read_text() == stable_config
        assert not list(tmp_path.glob("docker-compose.yml.*"))

    def test_dashboard_deploy_signal_rolls_back_live_candidate(self, tmp_path):
        """If a deploy is interrupted after the candidate may be live, the trap
        must roll back before removing the pinned previous image."""
        calls = tmp_path / "compose-calls.log"
        docker_calls = tmp_path / "docker-calls.log"
        compose = tmp_path / "compose"
        compose.write_text(
            """#!/usr/bin/env sh
set -eu
printf '%s\\n' "$*" >> "$COMPOSE_CALLS"
printf 'DASHBOARD_IMAGE=%s\\n' "${DASHBOARD_IMAGE:-}" >> "$COMPOSE_CALLS"
case "$1" in
  ps)
    if [ -f "$COMPOSE_CALLS.rollback" ]; then
      printf '%s\\n' previous-container
    elif [ -f "$COMPOSE_CALLS.candidate" ]; then
      printf '%s\\n' candidate-container
    else
      printf '%s\\n' previous-container
    fi
    exit 0
    ;;
  build|logs|rm)
    exit 0
    ;;
  up)
    if [ -f "$COMPOSE_CALLS.candidate" ]; then
      : > "$COMPOSE_CALLS.rollback"
      exit 0
    fi
    : > "$COMPOSE_CALLS.candidate"
    kill -TERM "$PPID"
    exit 0
    ;;
  exec)
    if [ "${4:-}" = "node" ]; then
      exit 0
    fi
    printf '%s\\n' dist/assets/index-previous.js dist/assets/index-previous.css
    exit 0
    ;;
esac
exit 1
"""
        )
        compose.chmod(0o755)

        fake_bin = tmp_path / "bin"
        fake_bin.mkdir()
        docker = fake_bin / "docker"
        docker.write_text(
            """#!/usr/bin/env sh
set -eu
printf '%s\\n' "$*" >> "$DOCKER_CALLS"
case "$1" in
  image)
    if [ "${2:-}" = "inspect" ] && [ "${3:-}" = "-f" ]; then
      printf '%s\\n' sha256:candidate
      exit 0
    fi
    [ "${2:-}" = "rm" ] && exit 0
    ;;
  inspect)
    if [ "${2:-}" = "-f" ] && [ "${3:-}" = "{{.Image}}" ] &&
      [ "${4:-}" = "candidate-container" ]; then
      printf '%s\\n' sha256:candidate
      exit 0
    fi
    case "$3" in
      "{{.Image}}")
        printf '%s\\n' sha256:previous
        ;;
      "{{.Config.Image}}")
        printf '%s\\n' orcest-dashboard:latest
        ;;
    esac
    exit 0
    ;;
  tag)
    exit 0
    ;;
  run)
    for arg do
      [ "$arg" = "fake-node" ] && exit 0
    done
    if [ "${3:-}" = "orcest-dashboard:latest" ] && [ "${4:-}" = "node" ]; then
      exit 0
    fi
    if [ "${3:-}" = "orcest-dashboard:latest" ] && [ "${4:-}" = "sh" ]; then
      printf '%s\\n' dist/assets/index-candidate.js dist/assets/index-candidate.css
      exit 0
    fi
    ;;
esac
exit 1
"""
        )
        docker.chmod(0o755)

        env = os.environ.copy()
        env["PATH"] = f"{fake_bin}:{env['PATH']}"
        env["COMPOSE_CALLS"] = str(calls)
        env["DOCKER_CALLS"] = str(docker_calls)
        env["DASHBOARD_TOKEN"] = "test-token"
        env["DASHBOARD_NODE_IMAGE"] = "fake-node"
        env["DASHBOARD_DEPLOY_LOCK_DIR"] = str(tmp_path / "deploy.lock")

        result = subprocess.run(
            ["sh", "dashboard/scripts/deploy-compose-dashboard.sh", str(compose)],
            cwd=self._repo_root(),
            capture_output=True,
            text=True,
            env=env,
        )

        assert result.returncode == 143
        assert "Dashboard deploy interrupted after candidate start; rolling back" in result.stderr
        assert "Dashboard rollback published readiness verified" in result.stdout
        calls_text = calls.read_text()
        assert calls_text.count("up -d --no-build --force-recreate dashboard") == 2
        assert "DASHBOARD_IMAGE=orcest-dashboard:latest" in calls_text
        docker_text = docker_calls.read_text()
        assert "tag sha256:previous orcest-dashboard:rollback-" in docker_text
        assert "image rm orcest-dashboard:rollback-" in docker_text
        assert not (tmp_path / "deploy.lock").exists()

    def test_dashboard_rollback_checks_readiness_when_previous_assets_missing(
        self,
        tmp_path,
    ):
        """Rollback to a known previous image should still prove readiness when
        the old asset list could not be captured before deploy."""
        calls = tmp_path / "compose-calls.log"
        docker_calls = tmp_path / "docker-calls.log"
        compose = tmp_path / "compose"
        compose.write_text(
            """#!/usr/bin/env sh
  set -eu
  printf '%s\\n' "$*" >> "$COMPOSE_CALLS"
  printf 'DASHBOARD_IMAGE=%s\\n' "${DASHBOARD_IMAGE:-}" >> "$COMPOSE_CALLS"
  case "$1" in
    ps)
      if [ -f "$COMPOSE_CALLS.candidate" ]; then
        printf '%s\\n' candidate-container
      else
        printf '%s\\n' previous-container
      fi
      exit 0
      ;;
    build)
      exit 0
      ;;
    up|logs)
      if [ "$1" = "up" ]; then
        : > "$COMPOSE_CALLS.candidate"
      fi
      exit 0
      ;;
  exec)
    exit 1
    ;;
esac
exit 1
"""
        )
        compose.chmod(0o755)

        fake_bin = tmp_path / "bin"
        fake_bin.mkdir()
        docker = fake_bin / "docker"
        docker.write_text(
            """#!/usr/bin/env sh
set -eu
printf '%s\\n' "$*" >> "$DOCKER_CALLS"
case "$1" in
    image)
      if [ "${2:-}" = "inspect" ] && [ "${3:-}" = "-f" ]; then
        printf '%s\\n' sha256:candidate
        exit 0
      fi
      [ "${2:-}" = "inspect" ] && exit 0
      ;;
    inspect)
      if [ "${2:-}" = "-f" ] && [ "${3:-}" = "{{.Image}}" ] &&
        [ "${4:-}" = "candidate-container" ]; then
        printf '%s\\n' sha256:candidate
        exit 0
      fi
      case "$3" in
        "{{.Image}}")
          printf '%s\\n' sha256:previous
        ;;
      "{{.Config.Image}}")
        printf '%s\\n' orcest-dashboard:latest
        ;;
    esac
    exit 0
    ;;
    tag)
      exit 0
      ;;
    run)
      if [ "${3:-}" = "orcest-dashboard:latest" ] && [ "${4:-}" = "node" ]; then
        exit 0
      fi
      if [ "${3:-}" = "orcest-dashboard:latest" ] && [ "${4:-}" = "sh" ]; then
        printf '%s\\n' dist/assets/index-new.js dist/assets/index-new.css
        exit 0
      fi
      if [ "${3:-}" = "fake-node" ]; then
        exit 0
      fi
      exit 0
      ;;
esac
exit 1
"""
        )
        docker.chmod(0o755)

        env = os.environ.copy()
        env["PATH"] = f"{fake_bin}:{env['PATH']}"
        env["COMPOSE_CALLS"] = str(calls)
        env["DOCKER_CALLS"] = str(docker_calls)
        env["DASHBOARD_TOKEN"] = "test-token"
        env["DASHBOARD_READY_ATTEMPTS"] = "1"
        env["DASHBOARD_NODE_IMAGE"] = "fake-node"

        result = subprocess.run(
            ["sh", "dashboard/scripts/deploy-compose-dashboard.sh", str(compose)],
            cwd=self._repo_root(),
            capture_output=True,
            text=True,
            env=env,
        )

        assert result.returncode == 1
        assert "Dashboard rollback readiness verified without asset pin" in result.stdout
        assert "previous asset list was unavailable" in result.stderr
        calls_text = calls.read_text()
        assert "build dashboard" in calls_text
        assert "images -q dashboard" not in calls_text
        assert "up -d --no-build --force-recreate dashboard" in calls_text
        docker_text = docker_calls.read_text()
        assert "tag sha256:previous orcest-dashboard:rollback-" in docker_text
        assert "tag orcest-dashboard:rollback-" in docker_text
        assert "orcest-dashboard:latest" in docker_text
        assert "DASHBOARD_IMAGE=orcest-dashboard:latest" in calls_text
        assert "run --rm orcest-dashboard:latest sh -lc" in docker_text
        assert "DASHBOARD_ALLOW_UNPINNED_ASSETS=1" in docker_text
        assert "DASHBOARD_EXPECTED_ASSETS=" in docker_text
        assert "--network container:candidate-container" in docker_text
        assert "DASHBOARD_BASE_URL=http://127.0.0.1:8080" in docker_text

    def test_dashboard_rollback_restores_custom_previous_image_tag(
        self,
        tmp_path,
    ):
        """Rollback should recreate compose from the previous stable image name
        when the old dashboard was not using the default dashboard tag."""
        calls = tmp_path / "compose-calls.log"
        docker_calls = tmp_path / "docker-calls.log"
        compose = tmp_path / "compose"
        compose.write_text(
            """#!/usr/bin/env sh
set -eu
printf '%s\\n' "$*" >> "$COMPOSE_CALLS"
printf 'DASHBOARD_IMAGE=%s\\n' "${DASHBOARD_IMAGE:-}" >> "$COMPOSE_CALLS"
case "$1" in
  ps)
    if [ -f "$COMPOSE_CALLS.candidate" ]; then
      printf '%s\\n' candidate-container
    else
      printf '%s\\n' previous-container
    fi
    exit 0
    ;;
  build)
    exit 0
    ;;
  up|logs)
    if [ "$1" = "up" ]; then
      : > "$COMPOSE_CALLS.candidate"
    fi
    exit 0
    ;;
  exec)
    exit 1
    ;;
esac
exit 1
"""
        )
        compose.chmod(0o755)

        fake_bin = tmp_path / "bin"
        fake_bin.mkdir()
        docker = fake_bin / "docker"
        docker.write_text(
            """#!/usr/bin/env sh
set -eu
printf '%s\\n' "$*" >> "$DOCKER_CALLS"
case "$1" in
  image)
    if [ "${2:-}" = "inspect" ] && [ "${3:-}" = "-f" ]; then
      printf '%s\\n' sha256:candidate
      exit 0
    fi
    [ "${2:-}" = "rm" ] && exit 0
    ;;
  inspect)
    if [ "${2:-}" = "-f" ] && [ "${3:-}" = "{{.Image}}" ] &&
      [ "${4:-}" = "candidate-container" ]; then
      printf '%s\\n' sha256:candidate
      exit 0
    fi
    case "$3" in
      "{{.Image}}")
        printf '%s\\n' sha256:previous
        ;;
      "{{.Config.Image}}")
        printf '%s\\n' custom-previous:old
        ;;
    esac
    exit 0
    ;;
  tag)
    exit 0
    ;;
  run)
    if [ "${3:-}" = "orcest-dashboard:latest" ] && [ "${4:-}" = "node" ]; then
      exit 0
    fi
    if [ "${3:-}" = "orcest-dashboard:latest" ] && [ "${4:-}" = "sh" ]; then
      printf '%s\\n' dist/assets/index-new.js dist/assets/index-new.css
      exit 0
    fi
    for arg do
      [ "$arg" = "fake-node" ] && exit 0
    done
    ;;
esac
exit 1
"""
        )
        docker.chmod(0o755)

        env = os.environ.copy()
        env["PATH"] = f"{fake_bin}:{env['PATH']}"
        env["COMPOSE_CALLS"] = str(calls)
        env["DOCKER_CALLS"] = str(docker_calls)
        env["DASHBOARD_TOKEN"] = "test-token"
        env["DASHBOARD_READY_ATTEMPTS"] = "1"
        env["DASHBOARD_NODE_IMAGE"] = "fake-node"

        result = subprocess.run(
            ["sh", "dashboard/scripts/deploy-compose-dashboard.sh", str(compose)],
            cwd=self._repo_root(),
            capture_output=True,
            text=True,
            env=env,
        )

        assert result.returncode == 1
        assert "Dashboard rollback readiness verified without asset pin" in result.stdout
        calls_text = calls.read_text()
        docker_text = docker_calls.read_text()
        assert "tag sha256:previous orcest-dashboard:rollback-" in docker_text
        assert "tag orcest-dashboard:rollback-" in docker_text
        assert "custom-previous:old" in docker_text
        assert "DASHBOARD_IMAGE=custom-previous:old" in calls_text
        assert "DASHBOARD_IMAGE=orcest-dashboard:rollback-" not in calls_text

    def test_published_readiness_warns_on_degraded_snapshots_by_default(self):
        """A successful dashboard deploy should still verify snapshot delivery
        without rolling back a healthy build for pre-existing partial data."""
        check_script = self._repo_root() / "dashboard" / "scripts" / "check-published.sh"
        text = check_script.read_text()
        assert 'DASHBOARD_ALLOW_DEGRADED="${DASHBOARD_ALLOW_DEGRADED:-}"' not in text
        assert 'allow_degraded="${DASHBOARD_ALLOW_DEGRADED:-0}"' not in text
        assert 'allow_degraded="$(env_file_value DASHBOARD_ALLOW_DEGRADED)"' in text
        assert 'strict_degraded="$(env_file_value DASHBOARD_STRICT_DEGRADED)"' in text
        assert "DASHBOARD_ALLOW_DEGRADED=$allow_degraded" in text
        assert "DASHBOARD_STRICT_DEGRADED=$strict_degraded" in text
        assert '-e DASHBOARD_ALLOW_DEGRADED="$allow_degraded"' not in text
        assert "message.snapshot.redis_ok !== true" in text
        assert "snapshot websocket reported Redis unavailable" in text
        assert "message.snapshot.degraded_sections" in text
        assert "strictDegraded && !allowDegraded" in text
        assert "Warning: ${degradedMessage}" in text
        assert "snapshot reported degraded sections" in text

    def test_published_readiness_requires_expected_assets_by_default(self):
        """Standalone published readiness must not silently downgrade to a
        readiness-only check without proving the served bundle identity."""
        check_script = self._repo_root() / "dashboard" / "scripts" / "check-published.sh"
        text = check_script.read_text()
        assert 'allow_unpinned_assets="${DASHBOARD_ALLOW_UNPINNED_ASSETS:-0}"' not in text
        assert 'allow_unpinned_assets="$(env_file_value DASHBOARD_ALLOW_UNPINNED_ASSETS)"' in text
        assert "DASHBOARD_ALLOW_UNPINNED_ASSETS=$allow_unpinned_assets" in text
        assert '-e DASHBOARD_ALLOW_UNPINNED_ASSETS="$allow_unpinned_assets"' not in text
        assert "const allowUnpinnedAssets = " in text
        assert "if (allowUnpinnedAssets) return;" in text
        assert "DASHBOARD_EXPECTED_ASSETS must include a ${kind} asset" in text
        assert "DASHBOARD_ALLOW_UNPINNED_ASSETS=1 for readiness-only checks" in text
        assert "await expectStatus(jsPath, 401);" in text
        assert "await expectStatus(cssPath, 401);" in text
        assert 'const deepLinkPath = "/work/results";' in text
        assert 'assetPathsFromHtml(deepLinkText, ".js", deepLinkPath)' in text
        assert 'fetchAsset(deepLinkJsPath, deepLinkPath, cookieHeaders, "javascript", "JS")' in text
        assert "dashboard deep-link HTML did not reference both JS and CSS assets" in text

    def test_published_readiness_env_file_knobs_are_not_clobbered_by_defaults(self):
        """Env-file readiness knobs should survive unless the caller
        deliberately supplies an override in the shell environment."""
        check_script = self._repo_root() / "dashboard" / "scripts" / "check-published.sh"
        text = check_script.read_text()
        assert 'attempts="${DASHBOARD_READY_ATTEMPTS:-60}"' not in text
        assert 'interval_ms="${DASHBOARD_READY_INTERVAL_MS:-1000}"' not in text
        assert 'ready_attempts="$(env_file_value DASHBOARD_READY_ATTEMPTS)"' in text
        assert 'ready_interval_ms="$(env_file_value DASHBOARD_READY_INTERVAL_MS)"' in text
        assert (
            'published_docker_network="$(env_file_value DASHBOARD_PUBLISHED_DOCKER_NETWORK)"'
            in text
        )
        assert 'published_docker_network="${published_docker_network:-host}"' in text
        assert 'set -- "$@" --network "$published_docker_network"' in text
        assert "DASHBOARD_READY_ATTEMPTS=$ready_attempts" in text
        assert "DASHBOARD_READY_INTERVAL_MS=$ready_interval_ms" in text
        assert '-e DASHBOARD_READY_ATTEMPTS="$attempts"' not in text
        assert '-e DASHBOARD_READY_INTERVAL_MS="$interval_ms"' not in text

    def test_published_readiness_parses_compose_env_file_values(self, tmp_path):
        """The readiness checker must strip Compose-style quotes/comments before
        passing env-file values into the Docker smoke container."""
        env_file = tmp_path / ".dashboard.env"
        env_file.write_text(
            "\n".join(
                [
                    'DASHBOARD_TOKEN="secret-token" # comment',
                    "DASHBOARD_READY_ATTEMPTS = '2' # comment",
                    "DASHBOARD_READY_INTERVAL_MS=250 # comment",
                    'DASHBOARD_ALLOW_DEGRADED="true" # comment',
                    "DASHBOARD_STRICT_DEGRADED='1' # comment",
                    "DASHBOARD_ALLOW_UNPINNED_ASSETS='1' # comment",
                    "DASHBOARD_PUBLISHED_DOCKER_NETWORK='container:dashboard-1' # comment",
                    "",
                ]
            )
        )
        docker_calls = tmp_path / "docker-calls.log"
        fake_bin = tmp_path / "bin"
        fake_bin.mkdir()
        docker = fake_bin / "docker"
        docker.write_text(
            """#!/usr/bin/env sh
set -eu
for arg do
  printf '<%s>\\n' "$arg" >> "$DOCKER_CALLS"
done
exit 0
"""
        )
        docker.chmod(0o755)

        env = os.environ.copy()
        for key in [
            "DASHBOARD_TOKEN",
            "DASHBOARD_READY_ATTEMPTS",
            "DASHBOARD_READY_INTERVAL_MS",
            "DASHBOARD_ALLOW_DEGRADED",
            "DASHBOARD_STRICT_DEGRADED",
            "DASHBOARD_ALLOW_UNPINNED_ASSETS",
            "DASHBOARD_PUBLISHED_DOCKER_NETWORK",
        ]:
            env.pop(key, None)
        env["PATH"] = f"{fake_bin}:{env['PATH']}"
        env["DOCKER_CALLS"] = str(docker_calls)
        env["DASHBOARD_ENV_FILE"] = str(env_file)
        env["DASHBOARD_NODE_IMAGE"] = "fake-node"
        env["DASHBOARD_EXPECTED_ASSETS"] = "dist/assets/index-test.js dist/assets/index-test.css"
        env["DASHBOARD_EXPECTED_REVISION"] = "a" * 40

        result = subprocess.run(
            ["sh", "dashboard/scripts/check-published.sh"],
            cwd=self._repo_root(),
            capture_output=True,
            text=True,
            env=env,
        )

        assert result.returncode == 0, result.stderr
        args = docker_calls.read_text()
        assert "<--env-file>" not in args
        assert "<--network>" in args
        assert "<container:dashboard-1>" in args
        # The token must reach the container through the environment, never on
        # the host `docker` argv, which is world-readable via /proc/<pid>/cmdline
        # for the lifetime of the readiness loop.
        assert "<DASHBOARD_TOKEN=secret-token>" not in args
        assert "<DASHBOARD_TOKEN>" in args
        assert "<DASHBOARD_READY_ATTEMPTS=2>" in args
        assert "<DASHBOARD_READY_INTERVAL_MS=250>" in args
        assert "<DASHBOARD_ALLOW_DEGRADED=true>" in args
        assert "<DASHBOARD_STRICT_DEGRADED=1>" in args
        assert "<DASHBOARD_ALLOW_UNPINNED_ASSETS=1>" in args
        assert f"<DASHBOARD_EXPECTED_REVISION={'a' * 40}>" in args

    def test_remote_deploy_shell_quotes_embedded_single_quotes(self):
        """The remote deploy helper must preserve single-quoted grep regexes
        when it sends scripts over SSH stdin."""
        makefile = self._repo_root() / "Makefile"
        text = makefile.read_text()
        assert "DASHBOARD_SHELL_QUOTE = '$(subst ','\\'',$(1))'" in text
        assert "printf '%s\\n' $(call DASHBOARD_SHELL_QUOTE,$(1))" in text

        result = subprocess.run(
            [
                "make",
                "-n",
                "sync-dashboard-remote-unlocked",
                "DASHBOARD_REMOTE=dashboard.example.invalid",
            ],
            cwd=self._repo_root(),
            check=True,
            capture_output=True,
            text=True,
        )
        output = result.stdout
        assert "grep -Eq '\\''^ORCEST_REDIS_PASSWORD=.+$'\\'' .redis.env" in output
        assert "grep -Eq '\\''^DASHBOARD_TOKEN=.+$'\\'' .dashboard.env" in output
        assert "grep -Eq '^ORCEST_REDIS_PASSWORD=.+$' .redis.env" not in output
        assert "grep -Eq '^DASHBOARD_TOKEN=.+$' .dashboard.env" not in output

    def test_remote_deploy_preflights_env_before_destructive_sync(self):
        """Remote deploy must validate target-local env/secrets before any
        rsync --delete mutates the target dashboard directory."""
        result = subprocess.run(
            [
                "make",
                "-n",
                "sync-dashboard-remote-unlocked",
                "DASHBOARD_REMOTE=dashboard.example.invalid",
            ],
            cwd=self._repo_root(),
            check=True,
            capture_output=True,
            text=True,
        )

        output = result.stdout
        env_check_index = output.index("grep -Eq '\\''^ORCEST_REDIS_PASSWORD=.+$'\\'' .redis.env")
        sync_index = output.index("rsync -az --delete --exclude")
        assert env_check_index < sync_index

    def test_remote_deploy_lock_covers_sync_and_compose_deploy(self):
        """The pve-test remote deploy lock must be held before rsync starts and
        remain held while the remote compose deploy runs."""
        result = subprocess.run(
            [
                "make",
                "-n",
                "deploy-dashboard-remote",
                "DASHBOARD_REMOTE=dashboard.example.invalid",
            ],
            cwd=self._repo_root(),
            check=True,
            capture_output=True,
            text=True,
        )

        output = result.stdout
        acquire_index = output.index("target=deploy-dashboard-remote")
        sync_index = output.index("make sync-dashboard-remote-unlocked")
        deploy_index = output.index("DASHBOARD_DEPLOY_LOCK_HELD=1")
        release_index = output.index("release_remote_lock")
        assert acquire_index < sync_index < deploy_index
        assert release_index < acquire_index
        assert "DASHBOARD_DEPLOY_LOCK_DIR='\\''/opt/orcest/.dashboard-deploy.lock'\\''" in output

    def test_remote_sync_rejects_unsafe_dashboard_remote_dir(self):
        """Remote sync must not rsync-delete the dashboard into the compose root."""
        result = subprocess.run(
            [
                "make",
                "sync-dashboard-remote",
                "DASHBOARD_REMOTE=dashboard.example.invalid",
                "DASHBOARD_REMOTE_DIR=/opt/orcest",
            ],
            cwd=self._repo_root(),
            check=False,
            capture_output=True,
            text=True,
        )

        assert result.returncode != 0
        combined_output = result.stdout + result.stderr
        assert "Refusing unsafe DASHBOARD_REMOTE_DIR=/opt/orcest" in combined_output
        assert "dashboard.example.invalid sh" not in combined_output

    def test_remote_sync_rejects_unsafe_compose_root(self):
        """Remote sync must reject protected or traversal compose roots before rsync."""
        for unsafe_root in ["/", "/opt", "/etc", "..", "/opt/orcest/../other"]:
            result = subprocess.run(
                [
                    "make",
                    "sync-dashboard-remote",
                    "DASHBOARD_REMOTE=dashboard.example.invalid",
                    f"DASHBOARD_REMOTE_ORCEST_DIR={unsafe_root}",
                    f"DASHBOARD_REMOTE_DIR={unsafe_root.rstrip('/')}/dashboard",
                ],
                cwd=self._repo_root(),
                check=False,
                capture_output=True,
                text=True,
            )

            assert result.returncode != 0
            combined_output = result.stdout + result.stderr
            normalized_root = unsafe_root.rstrip("/")
            assert (
                f"Refusing unsafe DASHBOARD_REMOTE_ORCEST_DIR={normalized_root}" in combined_output
            )
            assert "dashboard.example.invalid sh" not in combined_output

    def test_remote_sync_rejects_compose_state_inside_deleted_dashboard_tree(self):
        """The last-known-good state must survive rsync --delete, so it cannot
        be configured inside the synchronized dashboard source directory."""
        for unsafe_state in [
            "/opt/orcest/dashboard",
            "/opt/orcest/dashboard/compose-state.yml",
        ]:
            result = subprocess.run(
                [
                    "make",
                    "check-dashboard-remote-paths",
                    "DASHBOARD_REMOTE=dashboard.example.invalid",
                    f"DASHBOARD_REMOTE_COMPOSE_STATE_FILE={unsafe_state}",
                ],
                cwd=self._repo_root(),
                check=False,
                capture_output=True,
                text=True,
            )

            assert result.returncode != 0
            combined_output = result.stdout + result.stderr
            assert "must be outside DASHBOARD_REMOTE_DIR" in combined_output
            assert "dashboard.example.invalid sh" not in combined_output

    def test_remote_sync_rejects_relative_compose_state_path(self):
        """The state file is used from multiple remote working directories, so
        require one unambiguous absolute location."""
        result = subprocess.run(
            [
                "make",
                "check-dashboard-remote-paths",
                "DASHBOARD_REMOTE=dashboard.example.invalid",
                "DASHBOARD_REMOTE_COMPOSE_STATE_FILE=dashboard/.compose-state.yml",
            ],
            cwd=self._repo_root(),
            check=False,
            capture_output=True,
            text=True,
        )

        assert result.returncode != 0
        combined_output = result.stdout + result.stderr
        assert "must be an absolute path outside DASHBOARD_REMOTE_DIR" in combined_output
        assert "dashboard.example.invalid sh" not in combined_output

    def test_remote_sync_preserves_excluded_target_files(self):
        """Remote sync should delete stale tracked files without deleting target
        local secrets or generated files covered by rsync excludes."""
        makefile = self._repo_root() / "Makefile"
        text = makefile.read_text()

        assert "rsync -az --delete --delete-excluded" not in text
        assert "rsync -az --delete $(DASHBOARD_RSYNC_EXCLUDES)" in text
        assert "--exclude='.env'" in text
        assert "--exclude='.env.*'" in text
        assert "--exclude='*.env'" in text
        assert "--exclude='.npmrc*'" in text

    def test_remote_sync_rejects_symlinked_target_dirs_before_delete(self):
        """Remote sync must reject symlinked directories before rsync --delete."""
        makefile = self._repo_root() / "Makefile"
        text = makefile.read_text()

        assert "$(DASHBOARD_REMOTE_EXEC) sh -eu" in text
        assert 'test ! -L "$$orcest_dir" && test ! -L "$$remote_dir"' in text
        assert "Remote dashboard directories must not be symlinks" in text
        assert "pwd -P" in text
        assert "Remote dashboard directories must resolve to configured paths" in text

    def test_remote_sync_inspects_configured_docker_network(self):
        """Remote network preflight must follow ORCEST_DOCKER_NETWORK instead
        of hard-coding the default network name."""
        result = subprocess.run(
            [
                "make",
                "-n",
                "sync-dashboard-remote-unlocked",
                "DASHBOARD_REMOTE=dashboard.example.invalid",
            ],
            cwd=self._repo_root(),
            check=True,
            capture_output=True,
            text=True,
        )

        output = result.stdout
        assert "ORCEST_DOCKER_NETWORK" in output
        assert "^[[:space:]]*ORCEST_DOCKER_NETWORK[[:space:]]*=" in output
        assert 'sprintf("%c", 39)' in output
        assert "end=index(body, q)" in output
        assert "[[:space:]]+#.*" in output
        assert "/opt/orcest" in output
        assert ".redis.env" in output
        assert ".dashboard.env" in output
        assert "docker compose version >/dev/null; network_name=" in output
        assert "docker compose version >/dev/null && network_name=" not in output
        assert 'cd "/opt/orcest" && docker compose version' not in output
        assert 'docker network inspect "$network_name"' in output
        assert "docker network inspect orcest" not in output

    def test_remote_deploy_propagates_published_readiness_overrides(self):
        """Known degraded snapshots and slow starts need an explicit remote
        deploy override path; unset local knobs should not be forced remotely."""
        result = subprocess.run(
            [
                "make",
                "-n",
                "deploy-dashboard-remote",
                "DASHBOARD_REMOTE=dashboard.example.invalid",
                "DASHBOARD_ALLOW_DEGRADED=1",
                "DASHBOARD_STRICT_DEGRADED=1",
                "DASHBOARD_READY_ATTEMPTS=2",
                "DASHBOARD_READY_INTERVAL_MS=250",
                "DASHBOARD_ALLOW_UNPINNED_ASSETS=1",
            ],
            cwd=self._repo_root(),
            check=True,
            capture_output=True,
            text=True,
        )
        output = result.stdout
        assert "DASHBOARD_ALLOW_DEGRADED='\\''1'\\''" in output
        assert "DASHBOARD_STRICT_DEGRADED='\\''1'\\''" in output
        assert "DASHBOARD_READY_ATTEMPTS='\\''2'\\''" in output
        assert "DASHBOARD_READY_INTERVAL_MS='\\''250'\\''" in output
        assert "DASHBOARD_ALLOW_UNPINNED_ASSETS='\\''1'\\''" in output
        assert 'DASHBOARD_ALLOW_DEGRADED="1"' not in output

    def test_dashboard_dockerfile_does_not_copy_npmrc(self):
        """The dashboard image must not copy local npm config into build layers
        or the final runtime image. Future .npmrc auth tokens should not leak
        through the Dockerfile."""
        dockerfile = self._repo_root() / "dashboard" / "Dockerfile"
        text = dockerfile.read_text()
        assert ".npmrc" not in text
        assert "NPM_CONFIG_ENGINE_STRICT=true" in text
        assert "NPM_CONFIG_AUDIT=false" in text
        assert "NPM_CONFIG_FUND=false" in text
        assert "NPM_CONFIG_PROGRESS=false" in text
        assert "NPM_CONFIG_UPDATE_NOTIFIER=false" in text
        assert "npm ci --omit=dev" in text

    def test_dashboard_dockerignore_excludes_local_secrets(self):
        """The dashboard Docker context should exclude local env and npm
        credential files by default."""
        dockerignore = self._repo_root() / "dashboard" / ".dockerignore"
        entries = {
            line.strip()
            for line in dockerignore.read_text().splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        }
        assert ".env" in entries
        assert ".env.*" in entries
        assert "*.env" in entries
        assert ".npmrc*" in entries

    def test_dashboard_clean_copy_runs_inside_container_filesystem(self):
        """Clean-copy Docker validation should not install node_modules into a
        host bind mount, which can make Vitest workers lose preload files."""
        makefile = self._repo_root() / "Makefile"
        text = makefile.read_text()
        assert "DASHBOARD_NPM_ENV =" in text
        assert "NPM_CONFIG_UPDATE_NOTIFIER=false" in text
        assert (
            "docker run --rm -i -e HOME=/tmp $(DASHBOARD_NPM_ENV) -w /app $(DASHBOARD_NODE_IMAGE)"
            in text
        )
        assert "tar -C /app -xf - && $(1)" in text
        assert '-v "$$tmpdir:/app"' not in text

    def test_dashboard_clean_copy_native_runner_avoids_docker_pull(self):
        """CI can reuse setup-node's pinned Node instead of pulling Docker Hub."""
        result = subprocess.run(
            ["make", "-n", "check-dashboard-clean-copy", "DASHBOARD_CLEAN_COPY_RUNNER=native"],
            cwd=self._repo_root(),
            check=True,
            capture_output=True,
            text=True,
        )

        assert "tmpdir=$(mktemp -d)" in result.stdout
        assert 'tar -C "$tmpdir" -xf -' in result.stdout
        assert "npm run check:node" in result.stdout
        assert "docker run" not in result.stdout

    def test_dashboard_tracked_guard_rejects_unstaged_edits(self, tmp_path):
        """Dashboard Make targets copy the working tree and deploy root compose
        files, so the guard must fail when tracked verification files have
        unstaged content."""
        script = self._repo_root() / "dashboard" / "scripts" / "check-tracked-files.sh"
        repo = tmp_path / "repo"
        (repo / "dashboard" / "scripts").mkdir(parents=True)
        (repo / "dashboard" / "server").mkdir(parents=True)
        (repo / ".github" / "workflows").mkdir(parents=True)
        (repo / "tests").mkdir()
        (repo / "dashboard" / "scripts" / "check-tracked-files.sh").write_text(script.read_text())
        (repo / "dashboard" / "scripts" / "check-tracked-files.sh").chmod(0o755)
        (repo / "dashboard" / "server" / "index.ts").write_text("export const value = 1;\n")
        (repo / "docker-compose.dashboard.yml").write_text("services: {}\n")
        (repo / "Makefile").write_text("test:\n\ttrue\n")
        (repo / ".github" / "workflows" / "ci.yml").write_text("name: ci\n")
        (repo / "tests" / "test_dashboard.py").write_text("def test_placeholder():\n    pass\n")

        subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
        subprocess.run(["git", "add", "."], cwd=repo, check=True)
        subprocess.run(
            [
                "git",
                "-c",
                "user.email=dashboard@example.test",
                "-c",
                "user.name=Dashboard Test",
                "commit",
                "-q",
                "-m",
                "initial",
            ],
            cwd=repo,
            check=True,
        )
        (repo / "dashboard" / "server" / "index.ts").write_text("export const value = 2;\n")

        result = subprocess.run(
            ["sh", "dashboard/scripts/check-tracked-files.sh"],
            cwd=repo,
            check=False,
            capture_output=True,
            text=True,
        )

        assert result.returncode != 0
        assert "Dashboard verification has unstaged tracked file changes." in result.stderr
        assert "dashboard/server/index.ts" in result.stderr

    @pytest.mark.parametrize(
        ("failed_query", "expected_error"),
        [
            ("untracked", "could not inspect untracked files"),
            ("ignored", "could not inspect ignored untracked dashboard files"),
            ("status", "could not inspect dashboard worktree status"),
            ("diff", "could not inspect unstaged dashboard changes"),
        ],
    )
    def test_dashboard_tracked_guard_fails_closed_when_git_query_fails(
        self,
        tmp_path,
        failed_query,
        expected_error,
    ):
        """Every Git inventory probe must fail the guard when Git fails,
        including probes whose output is filtered without POSIX pipefail."""
        script = self._repo_root() / "dashboard" / "scripts" / "check-tracked-files.sh"
        repo = tmp_path / "repo"
        fake_bin = tmp_path / "bin"
        (repo / "dashboard" / "scripts").mkdir(parents=True)
        fake_bin.mkdir()
        copied_script = repo / "dashboard" / "scripts" / "check-tracked-files.sh"
        copied_script.write_text(script.read_text())
        copied_script.chmod(0o755)
        (repo / "dashboard" / "tracked.txt").write_text("tracked\n")

        subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
        subprocess.run(["git", "add", "."], cwd=repo, check=True)
        subprocess.run(
            [
                "git",
                "-c",
                "user.email=dashboard@example.test",
                "-c",
                "user.name=Dashboard Test",
                "commit",
                "-q",
                "-m",
                "initial",
            ],
            cwd=repo,
            check=True,
        )

        fake_git = fake_bin / "git"
        fake_git.write_text(
            """#!/bin/sh
command_name=$1
shift
saw_ignored=0
for arg do
  if [ "$arg" = "--ignored" ]; then
    saw_ignored=1
  fi
done

should_fail=0
case "$DASHBOARD_TEST_FAILED_GIT_QUERY:$command_name:$saw_ignored" in
  untracked:ls-files:0|ignored:ls-files:1|status:status:*|diff:diff:*)
    should_fail=1
    ;;
esac
if [ "$should_fail" -eq 1 ]; then
  echo "simulated Git query failure" >&2
  exit 71
fi
exec "$DASHBOARD_TEST_REAL_GIT" "$command_name" "$@"
"""
        )
        fake_git.chmod(0o755)
        real_git = shutil.which("git")
        assert real_git is not None
        env = os.environ.copy()
        env.update(
            {
                "DASHBOARD_TEST_FAILED_GIT_QUERY": failed_query,
                "DASHBOARD_TEST_REAL_GIT": real_git,
                "PATH": f"{fake_bin}{os.pathsep}{env['PATH']}",
            }
        )

        result = subprocess.run(
            ["sh", "dashboard/scripts/check-tracked-files.sh"],
            cwd=repo,
            check=False,
            capture_output=True,
            text=True,
            env=env,
        )

        assert result.returncode == 1
        assert "simulated Git query failure" in result.stderr
        assert expected_error in result.stderr

    def test_dashboard_tracked_guard_allows_copy_excluded_ignored_files(self):
        """Generated dashboard artifacts that copy/deploy excludes skip must not
        block local verification."""
        repo_root = self._repo_root()
        paths = [
            repo_root / "dashboard" / "dist" / "guard-allowed.js",
            repo_root / "dashboard" / "build" / "guard-allowed.js",
            repo_root / "dashboard" / "vite.config.ts.timestamp-guard.mjs",
        ]
        for path in paths:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("generated\n")

        try:
            result = self._run_dashboard_guard()
        finally:
            for path in paths:
                path.unlink(missing_ok=True)

        assert result.returncode == 0, result.stderr

    def test_dashboard_tracked_guard_rejects_ignored_files_that_would_be_copied(
        self,
        tmp_path,
    ):
        """Ignored files still need to fail if the dashboard copy/deploy excludes
        would include them."""
        repo_root = self._repo_root()
        ignored_file = repo_root / "dashboard" / "guard-copied.fixture"
        excludes_file = tmp_path / "dashboard-excludes"
        gitconfig = tmp_path / "gitconfig"
        excludes_file.write_text("dashboard/guard-copied.fixture\n")
        gitconfig.write_text(f"[core]\n\texcludesFile = {excludes_file}\n")
        ignored_file.write_text("ignored but copied\n")

        try:
            result = self._run_dashboard_guard(env={"GIT_CONFIG_GLOBAL": str(gitconfig)})
        finally:
            ignored_file.unlink(missing_ok=True)

        assert result.returncode == 1
        assert "Ignored untracked dashboard files that would be copied:" in result.stderr
        assert "dashboard/guard-copied.fixture" in result.stderr
