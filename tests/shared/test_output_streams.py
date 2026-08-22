"""Byte-budget and chunk-protocol tests for worker output streams (#585)."""

from __future__ import annotations

import pytest

from orcest.shared.output_streams import (
    OUTPUT_BUDGET_PROJECTS,
    OUTPUT_BUDGET_WORKERS,
    OUTPUT_CHUNK_PART_FIELD,
    OUTPUT_CHUNK_PARTS_FIELD,
    OUTPUT_STREAM_BUDGET_BYTES,
    OUTPUT_STREAM_MAX_ENTRY_BYTES,
    OUTPUT_STREAM_MAXLEN,
    OutputLineAssembler,
    is_output_continuation,
    iter_capped_output_fields,
    output_streams_worst_case_bytes,
)


def _payload_bytes(fields: dict[str, str]) -> int:
    return sum(len(v.encode("utf-8")) for v in fields.values())


def test_incident_average_line_round_trips_without_truncation():
    """25–48 KB/entry was the common case in #585, not an outlier."""
    line = "x" * (48 * 1024)
    chunks = iter_capped_output_fields({"line": line, "task_id": "task-1"})
    assert len(chunks) > 1
    assert "".join(c["line"] for c in chunks) == line
    for chunk in chunks:
        assert _payload_bytes(chunk) <= OUTPUT_STREAM_MAX_ENTRY_BYTES
        assert chunk["task_id"] == "task-1"
        assert is_output_continuation(chunk) is (chunk[OUTPUT_CHUNK_PART_FIELD] != "0")
    assembled = OutputLineAssembler()
    complete: list[dict[str, str]] = []
    for chunk in chunks:
        complete.extend(assembled.push("output:w1", chunk))
    assert len(complete) == 1
    assert complete[0]["line"] == line
    assert OUTPUT_CHUNK_PART_FIELD not in complete[0]
    assert OUTPUT_CHUNK_PARTS_FIELD not in complete[0]


def test_assembler_concatenates_in_order_and_strips_chunk_meta():
    assembler = OutputLineAssembler()
    assert assembler.push("s", {"line": "aa", "part": "0", "parts": "2", "task_id": "t"}) == []
    done = assembler.push("s", {"line": "bb", "part": "1", "parts": "2", "task_id": "t"})
    assert done == [{"line": "aabb", "task_id": "t"}]


def test_assembler_flushes_incomplete_when_a_new_line_starts():
    assembler = OutputLineAssembler()
    assembler.push("s", {"line": "aa", "part": "0", "parts": "3", "task_id": "t"})
    out = assembler.push("s", {"line": "next\n", "task_id": "t"})
    assert out[0]["line"] == "aa"
    assert out[1]["line"] == "next\n"


def test_assembler_isolates_streams():
    assembler = OutputLineAssembler()
    assembler.push("a", {"line": "A", "part": "0", "parts": "2"})
    assembler.push("b", {"line": "B", "part": "0", "parts": "2"})
    assert assembler.push("a", {"line": "a", "part": "1", "parts": "2"}) == [{"line": "Aa"}]
    assert assembler.push("b", {"line": "b", "part": "1", "parts": "2"}) == [{"line": "Bb"}]


def test_is_output_continuation_skips_only_non_first_chunks():
    assert is_output_continuation({"line": "x"}) is False
    assert is_output_continuation({"line": "x", "part": "0", "parts": "3"}) is False
    assert is_output_continuation({"line": "x", "part": "1", "parts": "3"}) is True


def test_worst_case_bytes_uses_per_entry_cap_not_measured_average():
    """The bound is entry-size × MAXLEN, independent of how lines are chunked."""
    worst = output_streams_worst_case_bytes(OUTPUT_BUDGET_PROJECTS, OUTPUT_BUDGET_WORKERS)
    assert worst <= OUTPUT_STREAM_BUDGET_BYTES
    assert (
        OUTPUT_BUDGET_PROJECTS * OUTPUT_BUDGET_WORKERS * OUTPUT_STREAM_MAXLEN * (48 * 1024) * 2
        > OUTPUT_STREAM_BUDGET_BYTES
    )


def test_stderr_tag_survives_chunking_and_reassembly():
    line = "e" * (OUTPUT_STREAM_MAX_ENTRY_BYTES * 2)
    chunks = iter_capped_output_fields({"line": line, "stream": "stderr", "task_id": "t"})
    assembler = OutputLineAssembler()
    complete: list[dict[str, str]] = []
    for chunk in chunks:
        complete.extend(assembler.push("output:w", chunk))
    assert complete == [{"line": line, "stream": "stderr", "task_id": "t"}]


def test_output_streams_worst_case_rejects_non_positive() -> None:
    with pytest.raises(ValueError, match="positive"):
        output_streams_worst_case_bytes(0, 1)
