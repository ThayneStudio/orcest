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


def test_distinct_decimal_offsets_do_not_collapse():
    # Regression: digits are a subset of the hex alphabet, so a naive
    # "8+ hex chars" strip would treat pure-decimal offsets like
    # 100000000/200000000/... as volatile ids and collapse genuinely
    # different calls into the same hash -- a false LOOPING verdict that
    # would kill a productive runner. 8-9 digit pure-decimal runs must
    # survive normalization intact.
    d = RepetitionDetector()
    for offset in (100000000, 200000000, 300000000, 400000000):
        d.observe_tool_call("Read", {"path": "/a", "offset": offset})

    assert d.verdict() is None


def test_git_sha_still_normalized():
    # Genuinely-hex identifiers (e.g. 40-char git SHAs) must still be
    # stripped so two calls differing only in the SHA hash identically.
    d = RepetitionDetector()
    d.observe_tool_call(
        "Bash", {"cmd": "git show", "sha": "a1b2c3d4e5f60718293a4b5c6d7e8f9012345678"}
    )
    d.observe_tool_call(
        "Bash", {"cmd": "git show", "sha": "0011223344556677889900aabbccddeeff01122"}
    )
    for _ in range(2):
        d.observe_tool_call(
            "Bash", {"cmd": "git show", "sha": "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef"}
        )

    v = d.verdict()
    assert v is not None
    assert v.stream == "exact"
    assert v.count == 4
