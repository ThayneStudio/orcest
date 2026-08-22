"""Byte budget and chunk protocol for worker output Redis streams.

Redis ``XADD MAXLEN`` counts entries, not bytes. Stream-json tool results
average 25–48 KB (#585) and can be hundreds of KiB, so a count cap alone
cannot bind RSS.

Each Redis entry is therefore capped at ``OUTPUT_STREAM_MAX_ENTRY_BYTES``.
Lines that exceed that size are split across consecutive entries tagged
``part`` / ``parts``. Live-tail (dashboard) shows the first chunk; the
trace archiver concatenates parts so the durable ``.jsonl`` stays verbatim.

Arithmetic (check these numbers against the live fleet):

    projects          = 8   (live 4: orcest, bbr-platform, asemly,
                             transit-platform; + headroom)
    workers           = 8   (pool.size default 4; live leftovers pushed
                             *:output:* to 24 keys; + headroom)
    streams           = 8 × 8 = 64
    max entry payload = 4 KiB (a 48 KB tool result becomes ~12 entries;
                             without a per-entry cap, MAXLEN=20000 on a
                             1.5 KiB average still reached 29.9 MB in one
                             stream, and 48 KB/entry at 20000 is ~968 MB)
    MAXLEN            = 512
    overhead factor   = 2   (listpack / radix tree / field names / IDs)
    bytes             = 64 × 512 × 4 KiB × 2 = 256 MiB

256 MiB leaves ~768 MiB of the 1 GiB ceiling for :results, :events,
task streams, locks, and AOF. The previous MAXLEN=20000 assumed
"4 workers × ~50 MB" and ignored per-project namespacing.

TTL is 8 h: covers pool.max_task_duration (7 h) if a task is silent
after task_start, plus 1 h for dashboard/archiver drain after the last
write. Keys with TTL=-1 were the other half of #585.
"""

from __future__ import annotations

from dataclasses import dataclass, field

OUTPUT_BUDGET_PROJECTS = 8
OUTPUT_BUDGET_WORKERS = 8
OUTPUT_STREAM_MAX_ENTRY_BYTES = 4096
OUTPUT_STREAM_MAXLEN = 512
OUTPUT_STREAM_OVERHEAD_FACTOR = 2
OUTPUT_STREAM_BUDGET_BYTES = 256 * 1024 * 1024
OUTPUT_STREAM_TTL_SECONDS = 8 * 3600

# Chunk index fields written only when a line is split. Values are decimal
# strings; 10 digits covers a line far larger than the 256 MiB budget.
OUTPUT_CHUNK_PART_FIELD = "part"
OUTPUT_CHUNK_PARTS_FIELD = "parts"
_CHUNK_INDEX_DIGITS = 10
_CHUNK_VALUE_RESERVE_BYTES = _CHUNK_INDEX_DIGITS * 2
# UTF-8 code points are at most 4 bytes; the split loop must be able to
# emit at least one code point per chunk so it cannot stall.
_MIN_CHUNK_LINE_BYTES = 4

if (
    OUTPUT_BUDGET_PROJECTS
    * OUTPUT_BUDGET_WORKERS
    * OUTPUT_STREAM_MAXLEN
    * OUTPUT_STREAM_MAX_ENTRY_BYTES
    * OUTPUT_STREAM_OVERHEAD_FACTOR
    > OUTPUT_STREAM_BUDGET_BYTES
):
    raise ValueError(
        "output-stream worst-case bytes exceed OUTPUT_STREAM_BUDGET_BYTES; "
        "re-derive MAXLEN from the comment arithmetic"
    )


def output_streams_worst_case_bytes(projects: int, workers: int) -> int:
    """Upper bound on output-stream RSS for a project × worker fan-out.

    Assumes every stream is full of max-size entries. The budget test uses
    this so the MAXLEN estimate cannot silently drift.
    """
    if projects < 1 or workers < 1:
        raise ValueError(f"projects and workers must be positive, got {projects}, {workers}")
    return (
        projects
        * workers
        * OUTPUT_STREAM_MAXLEN
        * OUTPUT_STREAM_MAX_ENTRY_BYTES
        * OUTPUT_STREAM_OVERHEAD_FACTOR
    )


def _utf8_chunks(data: bytes, size: int) -> list[bytes]:
    """Split ``data`` into pieces of at most ``size`` bytes on code-point boundaries."""
    if size < 1:
        raise ValueError(f"size must be positive, got {size}")
    chunks: list[bytes] = []
    i = 0
    n = len(data)
    while i < n:
        end = min(i + size, n)
        if end < n:
            while end > i and (data[end] & 0xC0) == 0x80:
                end -= 1
            if end == i:
                end = min(i + 1, n)
                while end < n and (data[end] & 0xC0) == 0x80:
                    end += 1
        chunks.append(data[i:end])
        i = end
    return chunks


def iter_capped_output_fields(fields: dict[str, str]) -> list[dict[str, str]]:
    """Yield Redis entries whose value payloads fit ``OUTPUT_STREAM_MAX_ENTRY_BYTES``.

    Marker fields (task_start / task_end, no ``line``) are left intact.
    Short lines return the original dict. Long lines are split into
    consecutive entries with ``part`` / ``parts`` so TraceArchiver can
    reconstruct the original payload.
    """
    line = fields.get("line")
    if line is None:
        return [fields]
    other_bytes = sum(len(v.encode("utf-8")) for k, v in fields.items() if k != "line")
    allow_single = OUTPUT_STREAM_MAX_ENTRY_BYTES - other_bytes
    encoded = line.encode("utf-8")
    if allow_single >= _MIN_CHUNK_LINE_BYTES and len(encoded) <= allow_single:
        return [fields]
    allow = OUTPUT_STREAM_MAX_ENTRY_BYTES - other_bytes - _CHUNK_VALUE_RESERVE_BYTES
    if allow < _MIN_CHUNK_LINE_BYTES:
        return [{**fields, "line": ""}]
    pieces = _utf8_chunks(encoded, allow)
    if len(pieces) == 1:
        return [{**fields, "line": pieces[0].decode("utf-8")}]
    n = len(pieces)
    other = {k: v for k, v in fields.items() if k != "line"}
    return [
        {
            **other,
            "line": piece.decode("utf-8"),
            OUTPUT_CHUNK_PART_FIELD: str(i),
            OUTPUT_CHUNK_PARTS_FIELD: str(n),
        }
        for i, piece in enumerate(pieces)
    ]


def is_output_continuation(fields: dict[str, str]) -> bool:
    """True for second-and-later chunks of a split line (skip in live-tail)."""
    part = fields.get(OUTPUT_CHUNK_PART_FIELD)
    return part is not None and part != "0"


def _strip_chunk_meta(fields: dict[str, str]) -> dict[str, str]:
    return {
        k: v
        for k, v in fields.items()
        if k not in (OUTPUT_CHUNK_PART_FIELD, OUTPUT_CHUNK_PARTS_FIELD)
    }


def _parse_nonneg_int(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        n = int(value)
    except ValueError:
        return None
    if n < 0:
        return None
    return n


@dataclass
class _PendingLine:
    parts: int
    chunks: list[str | None]
    template: dict[str, str] = field(default_factory=dict)


class OutputLineAssembler:
    """Reassemble ``part`` / ``parts`` chunks from a per-worker output stream.

    One in-flight line is tracked per stream. Incomplete lines (MAXLEN
    trimmed the start, or the worker died mid-split) are flushed as the
    concatenation of whatever chunks arrived so a later line or task
    boundary cannot stall the archive.
    """

    def __init__(self) -> None:
        self._pending: dict[str, _PendingLine] = {}

    def push(self, stream: str, fields: dict[str, str]) -> list[dict[str, str]]:
        """Return complete field dicts ready to archive (0, 1, or 2 items)."""
        out: list[dict[str, str]] = []
        parts = _parse_nonneg_int(fields.get(OUTPUT_CHUNK_PARTS_FIELD))
        part = _parse_nonneg_int(fields.get(OUTPUT_CHUNK_PART_FIELD))
        if parts is None or parts <= 1 or part is None:
            flushed = self.flush(stream)
            if flushed is not None:
                out.append(flushed)
            out.append(_strip_chunk_meta(fields) if parts == 1 else fields)
            return out
        pending = self._pending.get(stream)
        if pending is None or part == 0 or pending.parts != parts:
            flushed = self.flush(stream)
            if flushed is not None:
                out.append(flushed)
            pending = _PendingLine(
                parts=parts,
                chunks=[None] * parts,
                template={
                    k: v
                    for k, v in fields.items()
                    if k
                    not in (
                        "line",
                        OUTPUT_CHUNK_PART_FIELD,
                        OUTPUT_CHUNK_PARTS_FIELD,
                    )
                },
            )
            self._pending[stream] = pending
        if 0 <= part < parts:
            pending.chunks[part] = fields.get("line", "")
        if all(chunk is not None for chunk in pending.chunks):
            line = "".join(chunk for chunk in pending.chunks if chunk is not None)
            assembled = {**pending.template, "line": line}
            del self._pending[stream]
            out.append(assembled)
        return out

    def flush(self, stream: str) -> dict[str, str] | None:
        """Return a leftover partial line for ``stream``, if any."""
        pending = self._pending.pop(stream, None)
        if pending is None:
            return None
        if all(chunk is None for chunk in pending.chunks):
            return None
        return {**pending.template, "line": "".join(chunk or "" for chunk in pending.chunks)}

    def flush_all(self) -> list[tuple[str, dict[str, str]]]:
        leftovers: list[tuple[str, dict[str, str]]] = []
        for stream in list(self._pending):
            leftover = self.flush(stream)
            if leftover is not None:
                leftovers.append((stream, leftover))
        return leftovers
