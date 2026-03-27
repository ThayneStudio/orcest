"""Unit tests for the dashboard data-fetching and formatting layers."""

import json

import pytest
import redis as redis_lib

from orcest.dashboard import (
    DeadLetterEntry,
    _format_duration,
    _format_ttl,
    _status_style,
    discover_workers,
    fetch_snapshot,
    format_stream_json_line,
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
    fake_redis_client.xadd("orcest:dead-letter", {"id": "t1", "type": "fix_ci"})
    fake_redis_client.xadd("orcest:dead-letter", {"id": "t2", "type": "fix_ci"})

    snap = fetch_snapshot(fake_redis_client)

    assert snap.dead_letter_count == 2


def test_dead_letter_entries_empty_when_no_stream(fake_redis_client):
    """Dead-letter entries list is empty when the stream does not exist."""
    snap = fetch_snapshot(fake_redis_client)
    assert snap.dead_letter_entries == []


def test_dead_letter_entries_populated(fake_redis_client):
    """Fetches last N dead-letter entries with task details."""
    fake_redis_client.xadd(
        "orcest:dead-letter",
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
            "orcest:dead-letter",
            {"id": f"task-{i}", "type": "fix_ci", "repo": "org/repo"},
        )

    snap = fetch_snapshot(fake_redis_client)

    assert snap.dead_letter_count == 8
    assert len(snap.dead_letter_entries) == 5


def test_dead_letter_entries_most_recent_first(fake_redis_client):
    """Dead-letter entries are returned most recent first."""
    for i in range(3):
        fake_redis_client.xadd(
            "orcest:dead-letter",
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
