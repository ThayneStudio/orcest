"""Tests for the repetition (livelock) detector.

See ``.superpowers/sdd/2026-08-17-activity-watchdog/task-B5-brief.md`` for
the binding six-test contract.
"""

from __future__ import annotations

from orcest.worker.repetition import RepetitionDetector


def test_exact_repeat_trips_at_threshold():
    d = RepetitionDetector()
    for _ in range(3):
        d.observe_tool_call("Read", {"path": "/a"})
    assert d.verdict() is None

    d.observe_tool_call("Read", {"path": "/a"})
    v = d.verdict()
    assert v is not None
    assert v.stream == "exact"
    assert v.count == 4


def test_timestamps_and_uuids_normalized():
    d = RepetitionDetector()
    d.observe_tool_call(
        "Bash",
        {
            "cmd": "run",
            "request_id": "550e8400-e29b-41d4-a716-446655440000",
            "ts": "2026-08-17T12:00:00Z",
        },
    )
    d.observe_tool_call(
        "Bash",
        {
            "cmd": "run",
            "request_id": "123e4567-e89b-12d3-a456-426614174000",
            "ts": "2026-08-17T13:30:05.123456Z",
        },
    )
    d.observe_tool_call(
        "Bash",
        {
            "cmd": "run",
            "request_id": "abcdefab-cdef-abcd-efab-cdefabcdefab",
            "ts": "2026-08-17T09:15:42+00:00",
        },
    )
    # Still just a streak of 3 -- distinct UUIDs/timestamps should hash identically.
    assert d.verdict() is None

    d.observe_tool_call(
        "Bash",
        {
            "cmd": "run",
            "request_id": "00000000-0000-0000-0000-000000000000",
            "ts": "2026-08-17T23:59:59Z",
        },
    )
    v = d.verdict()
    assert v is not None
    assert v.stream == "exact"
    assert v.count == 4


def test_error_class_ignores_args():
    d = RepetitionDetector()
    d.observe_tool_error("Bash", "PermissionError")
    d.observe_tool_error("Bash", "PermissionError")
    assert d.verdict() is None

    d.observe_tool_error("Bash", "PermissionError")
    v = d.verdict()
    assert v is not None
    assert v.stream == "error_class"
    assert v.count == 3


def test_pingpong_alternation():
    d = RepetitionDetector()
    calls = [("Read", {"path": "/a"}), ("Grep", {"pattern": "x"})] * 6  # 6 A/B cycles
    for name, args in calls:
        d.observe_tool_call(name, args)

    v = d.verdict()
    assert v is not None
    assert v.stream == "ping_pong"
    assert v.count == 12


def test_novel_call_resets():
    d = RepetitionDetector()
    for _ in range(3):
        d.observe_tool_call("Read", {"path": "/a"})
    assert d.verdict() is None

    d.observe_tool_call("Read", {"path": "/b"})
    assert d.verdict() is None

    for _ in range(3):
        d.observe_tool_call("Read", {"path": "/a"})
    assert d.verdict() is None


def test_verdict_hashes_contain_no_raw_args():
    d = RepetitionDetector()
    secret_path = "/very/secret/path/with/a/token-abcdefabcdefabcdef"
    for _ in range(4):
        d.observe_tool_call("Read", {"path": secret_path})

    v = d.verdict()
    assert v is not None
    assert len(v.hashes) > 0
    for h in v.hashes:
        assert secret_path not in h
        assert len(h) == 16
        assert all(c in "0123456789abcdef" for c in h)
