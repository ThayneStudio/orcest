# Redis OOM Outage Remediation — Implementation Plan

> **Historical archive — 2026-09-30:** This August planning record is
> preserved for incident and design provenance. Its status, checklists,
> implementation steps, and agent instructions describe the original
> session; they are not current execution instructions or deployment
> evidence. See the [archive disposition](../local-planning-archive-disposition.md)
> for implemented work and remaining operational evidence.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make it impossible for worker output to exhaust Redis, and impossible for a Redis outage to make the pool manager destroy healthy VMs.

**Architecture:** Ten tasks in five phases. Phase 0 makes the orchestrator survive starting up against a full Redis — without it the rest of Phase B is inert during the exact event it targets. Phase A caps output entry *bytes* at the producer. Phase B turns the trace archiver into a pressure valve: a per-project disk cursor removes its Redis dependency so it keeps draining and `XTRIM`-ing while Redis is wedged (`XTRIM` is not `denyoom`). Phase C gates the pool-manager reap behind a writability probe with hysteresis, a minimum-age grace, and a correlation breaker. Phase D surfaces stranded work, fixes reap telemetry, and updates the docs.

**Tech Stack:** Python 3.12, pytest (`pytest.mark.unit`), `fakeredis` via `tests/conftest.py` fixtures, Redis Streams, Proxmox API.

**Spec:** `docs/superpowers/specs/2026-08-21-redis-oom-outage-remediation.md`

**Review history:** revised after five independent reviews (one fable, four parallel: API correctness, test quality, adversarial design, regressions). Findings that changed the design are marked **[review]** so executors understand why a non-obvious choice was made.

## Global Constraints

- Redis `--maxmemory 1gb` and `--maxmemory-policy noeviction` are **unchanged**. Do not edit `docker-compose.redis.yml`. (Spec R1, explicit operator decision.)
- `XADD`, `HSET`, `SET` and `XGROUP CREATE` are `denyoom` and fail at the ceiling. `XTRIM`, `XDEL` and `DEL` are **not** `denyoom` and succeed. Any recovery path may use only the latter.
- **Never trim an output stream entry that was not durably archived.** "Archived" means a line was actually written to an open handle — not merely that `_process_entry` returned without raising.
- Task streams are global (`orcest:tasks:issue:<provider>`); output/results/events streams are per-project (`<project>:output:<worker-id>`).
- Python 3.12, type hints on every new function. `make lint` and `make test-unit` must pass before every commit.
- Do not use the word "load-bearing" in code, comments, commits or PR text (`.claude/CLAUDE.md`). Say what depends on the thing, or what breaks without it.

---

## File Structure

| File | Responsibility | Task |
|---|---|---|
| `src/orcest/orchestrator/loop.py` | Tolerate OOM during startup; start archiver before the Redis-writing bootstrap | 0 |
| `src/orcest/worker/loop.py` | `_OUTPUT_STREAM_MAXLEN`, `_MAX_OUTPUT_ENTRY_BYTES`, `_truncate_output_fields` | 1 |
| `src/orcest/orchestrator/trace_archiver.py` | Per-project disk cursor; archived-only trim with tail retention; truncation marker | 2, 3 |
| `tests/conftest.py` | `make_fake_redis_client` gains a `key_prefix` argument | 2 |
| `src/orcest/fleet/config.py` | 5 new `PoolConfig` fields + load/render | 4 |
| `src/orcest/fleet/pool_manager.py` | Writability probe + hysteresis, min-age, breaker, fence memo | 5, 6, 8 |
| `src/orcest/orchestrator/fleet_health.py` | Stranded-stream detection reusing `rollout_health._raw_stream_work` | 7 |
| `.claude/CLAUDE.md`, `README.md`, `src/orcest/cli.py`, `docs/monitor-exposure-runbook.md` | Doc updates | 9 |

**Existing tests this plan modifies** (each named in its task; an earlier draft wrongly claimed these would pass untouched):
- `tests/orchestrator/test_trace_archiver.py` — cursor-hash assertions (~180, ~193) and `TestArchiverSecurity::test_rejects_path_traversal_task_id`
- `tests/fleet/test_pool_manager_activity.py` — three `activity_stale` fixtures seeded at `now - 10`
- `tests/conftest.py` — the `make_fake_redis_client` factory

---

# Phase 0 — Let the orchestrator start against a full Redis

## Task 0: Defer consumer-group bootstrap and start the archiver first

**[review] This is the most important task in the plan.** Everything in Phase B assumes the archiver runs during an OOM. It cannot today: `orchestrator/loop.py:1315-1320` calls `ensure_consumer_group` unguarded, `redis_client.py:469` re-raises anything without `BUSYGROUP` in the message, `OutOfMemoryError` subclasses `ResponseError`, and `XGROUP CREATE` is `denyoom`. `trace_archiver.start()` is at line **1426** — 106 lines later. At the ceiling the process dies before the pressure valve exists, which is exactly the crash-looping the spec records (bbr 10 restarts, orcest 6).

**Files:**
- Modify: `src/orcest/orchestrator/loop.py:1313-1320` (bootstrap), `:1426` (archiver start)
- Test: `tests/orchestrator/test_startup_under_oom.py` (create)

**Interfaces:**
- Produces: `_ensure_consumer_groups_best_effort(config, task_redis, project_clients, logger) -> bool` — `True` when every group was ensured, `False` when at least one was deferred. Idempotent; safe to re-run from the poll loop.

- [ ] **Step 1: Write the failing test**

Create `tests/orchestrator/test_startup_under_oom.py`:

```python
"""The orchestrator must reach trace_archiver.start() on a full Redis.

XGROUP CREATE is denyoom. Before this change startup raised at the consumer
group bootstrap, 106 lines before the archiver started -- so the one component
that can free memory (XTRIM is not denyoom) never ran during an OOM event.
See spec §2.4 and the crash-loop record in §1.
"""

from __future__ import annotations

import logging

import pytest
import redis as redis_lib

from orcest.orchestrator.loop import _ensure_consumer_groups_best_effort

pytestmark = pytest.mark.unit


class _OomRedis:
    def __init__(self) -> None:
        self.calls = 0

    def ensure_consumer_group(self, stream: str, group: str) -> None:
        self.calls += 1
        raise redis_lib.exceptions.OutOfMemoryError(
            "command not allowed when used memory > 'maxmemory'."
        )


class _OkRedis:
    def __init__(self) -> None:
        self.calls = 0

    def ensure_consumer_group(self, stream: str, group: str) -> None:
        self.calls += 1


def test_oom_during_bootstrap_is_deferred_not_raised(orchestrator_config):
    r = _OomRedis()
    complete = _ensure_consumer_groups_best_effort(
        orchestrator_config, r, [("asemly", r)], logging.getLogger("t")
    )
    assert complete is False
    assert r.calls > 0  # it tried


def test_healthy_redis_reports_complete(orchestrator_config):
    r = _OkRedis()
    complete = _ensure_consumer_groups_best_effort(
        orchestrator_config, r, [("asemly", r)], logging.getLogger("t")
    )
    assert complete is True


def test_non_oom_errors_still_propagate(orchestrator_config):
    class _Broken:
        def ensure_consumer_group(self, stream: str, group: str) -> None:
            raise redis_lib.exceptions.ResponseError("NOPERM this is a real bug")

    with pytest.raises(redis_lib.exceptions.ResponseError):
        _ensure_consumer_groups_best_effort(
            orchestrator_config, _Broken(), [("asemly", _Broken())], logging.getLogger("t")
        )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/orchestrator/test_startup_under_oom.py -v`
Expected: FAIL — `ImportError: cannot import name '_ensure_consumer_groups_best_effort'`

- [ ] **Step 3: Write minimal implementation**

`src/orcest/orchestrator/loop.py` does **not** import `redis` today (its imports are
json/logging/math/os/re/signal/sys/time/uuid plus orcest modules), so add the exception directly —
`run_orchestrator` binds a local `redis = RedisClient(...)` at `:1298`, and a module-level
`import redis` would shadow confusingly:

```python
from redis.exceptions import OutOfMemoryError
```

Then:

```python
def _ensure_consumer_groups_best_effort(
    config: OrchestratorConfig,
    task_redis: RedisClient,
    project_clients: list[tuple[ProjectConfig, RedisClient]],
    logger: logging.Logger,
) -> bool:
    """Ensure task/results consumer groups, tolerating a full Redis.

    XGROUP CREATE is denyoom, so at the maxmemory ceiling this is the first
    thing that fails at startup -- and failing here kills the process before the
    trace archiver (the only component that can free memory, since XTRIM is not
    denyoom) has started. On a running fleet the groups already exist, so
    deferring is safe: the poll loop re-ensures them once Redis recovers.

    Returns True when every group was ensured. Non-OOM ResponseErrors still
    propagate -- those are real bugs (bad ACL, wrong type) and must not be
    swallowed.
    """
    complete = True
    targets: list[tuple[RedisClient, str, str]] = [
        (task_redis, stream, CONSUMER_GROUP)
        for stream in _configured_task_streams(config)
        + _configured_task_streams(config, issue=True)
    ]
    targets += [(rc, RESULTS_STREAM, RESULTS_GROUP) for _, rc in project_clients]
    for client, stream, group in targets:
        try:
            client.ensure_consumer_group(stream, group)
        except OutOfMemoryError:
            complete = False
            logger.warning(
                "Deferring consumer-group bootstrap for %s/%s: Redis is at its "
                "memory ceiling. Continuing startup so the trace archiver can "
                "run and reclaim memory.",
                stream,
                group,
            )
    return complete
```

Replace the two bootstrap loops at `:1313-1320` with a single call to it, **move
`trace_archiver.start()` (currently line 1426) to immediately before that call** so the archiver is
running before anything issues a `denyoom` command, and re-invoke
`_ensure_consumer_groups_best_effort` from the `while not shutdown` loop (`:1457`) while it last
returned `False`.

**The consumer-group bootstrap is not the only startup path that dies under OOM.**
`_backfill_retained_task_provider_accounts` (`:1382`) persists mappings with `redis.set_ex`
(denyoom, `:437-441`); its caller catches every exception and calls `sys.exit(1)` (`:1393-1400`).
Mapping keys carry the same ~16.7 h TTL and expire during a long outage, so a retained task with an
expired mapping reproduces the crash loop and the daemon archiver thread gets only the sub-second
window before exit. Give it the same treatment: catch `OutOfMemoryError` specifically, log that the
backfill is deferred, and continue — its own docstring calls it a rolling-upgrade bridge that
"fails closed and retries", so deferring is safe. Every other exception must still `sys.exit(1)`.

Add a test alongside the others:

```python
def test_backfill_oom_does_not_exit(orchestrator_config, mocker):
    from orcest.orchestrator import loop as orch_loop

    mocker.patch.object(
        orch_loop,
        "_backfill_retained_task_provider_accounts",
        side_effect=redis_lib.exceptions.OutOfMemoryError("maxmemory"),
    )
    # run_orchestrator must reach the poll loop rather than SystemExit; assert on
    # whatever seam the existing tests/orchestrator/test_loop.py uses to stop the
    # loop after one pass.
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/orchestrator/test_startup_under_oom.py tests/orchestrator/test_loop.py -v`
Expected: all pass

- [ ] **Step 5: Commit**

```bash
git add src/orcest/orchestrator/loop.py tests/orchestrator/test_startup_under_oom.py
git commit -m "fix(orchestrator): survive startup against a full Redis so the archiver can reclaim memory"
```

---

# Phase A — Bound output entry bytes

## Task 1: Cap output entries by bytes, with an output-only MAXLEN

Spec R2. One stream held ~297 MB at `entries-added=6062` — median 328 B, mean 50,708 B, max 265,829 B. The 20000-entry cap never engaged.

**[review] The numbers, stated once so they can be checked.** Worst case with the archiver down
is `_OUTPUT_STREAM_COUNT_BUDGET x _OUTPUT_STREAM_MAXLEN x _MAX_OUTPUT_ENTRY_BYTES` = 48 x 1000 x
8192 = **375 MB**. Task 3's untrimmable tail adds a permanent healthy-state floor of
48 x 100 x 8192 = **37.5 MB**. Against a 1 GB ceiling shared with a saturated events spool
(~135 MB) and the ~5 MB queue working set, that is ~178 MB steady and ~520 MB at worst. An earlier
draft used a 16 KB cap and a 200-entry tail: 750 MB worst case and a 150 MB floor, over the budget
its own test asserted. 8 KB is still 25x the measured 328 B median line, and spec §2 measured 8 KB
truncation at 5.9% of original volume versus 10.5% for 16 KB.

**[review] Do not lower `_STREAM_MAXLEN`.** It has seven call sites and only two are output; the rest cap the **results** and **dead-letter** streams (`worker/loop.py:612, 628, 630, 702, 1903`). The pool manager caps that same results stream at `_RESULT_MAXLEN = 20000` (`pool_manager.py:74`), so lowering the worker constant would leave two producers disagreeing 20× on one stream — and `MAXLEN ~` evicts head entries regardless of PEL state, destroying unread results during exactly the orchestrator outages this plan is about. Four assertions in `tests/worker/test_loop.py` also pin `_STREAM_MAXLEN` as the results maxlen.

**Files:**
- Modify: `src/orcest/worker/loop.py:78` (new constants), `:2229-2239` (`_publish_task_output`)
- Modify: `tests/worker/test_loop.py` — **six** assertions pin the OUTPUT publish at
  `maxlen=_STREAM_MAXLEN` (~796, ~829, ~921, ~926, ~931, ~941) and go red when
  `_publish_task_output` switches constants. Lines 3081 and 4166 are genuinely results-stream and
  must stay on `_STREAM_MAXLEN`.
- Test: `tests/worker/test_output_truncation.py` (create)

**Interfaces:**
- Produces: `_MAX_OUTPUT_ENTRY_BYTES: int = 8192`, `_OUTPUT_STREAM_MAXLEN: int = 1000`, `_OUTPUT_STREAM_COUNT_BUDGET: int = 48`, and `_truncate_output_fields(fields: dict[str, str], max_bytes: int = _MAX_OUTPUT_ENTRY_BYTES) -> dict[str, str]`, which adds `truncated_bytes: str` only when truncation occurred. Task 3 reads `truncated_bytes`.

- [ ] **Step 1: Write the failing test**

Create `tests/worker/test_output_truncation.py`:

```python
"""Output stream entries are capped by BYTES, not just entry count.

The 2026-08-21 OOM was caused by a byte-blind cap: median entry 328 B but mean
50,708 B and max 265,829 B, so MAXLEN ~ 20000 never engaged before Redis hit
its 1 GB ceiling. See spec §2.
"""

from __future__ import annotations

import pytest

from orcest.worker.loop import (
    _MAX_OUTPUT_ENTRY_BYTES,
    _OUTPUT_STREAM_COUNT_BUDGET,
    _OUTPUT_STREAM_MAXLEN,
    _STREAM_MAXLEN,
    _truncate_output_fields,
)

pytestmark = pytest.mark.unit


def test_small_line_is_passed_through_unchanged():
    fields = {"line": "hello", "task_id": "abc"}
    assert _truncate_output_fields(fields) == fields


def test_oversized_line_is_truncated_to_the_cap():
    out = _truncate_output_fields({"line": "x" * 200_000, "task_id": "abc"})
    assert len(out["line"].encode("utf-8")) <= _MAX_OUTPUT_ENTRY_BYTES
    assert out["task_id"] == "abc"


def test_truncation_records_dropped_byte_count_in_band():
    out = _truncate_output_fields({"line": "x" * 200_000, "task_id": "abc"})
    kept = len(out["line"].encode("utf-8"))
    assert out["truncated_bytes"] == str(200_000 - kept)


def test_untruncated_entry_has_no_truncated_bytes_field():
    assert "truncated_bytes" not in _truncate_output_fields({"line": "s", "task_id": "a"})


def test_multibyte_boundary_is_not_split_into_invalid_utf8():
    out = _truncate_output_fields({"line": "あ" * 100_000, "task_id": "abc"})
    assert "�" not in out["line"]
    assert len(out["line"].encode("utf-8")) <= _MAX_OUTPUT_ENTRY_BYTES


def test_entries_without_a_line_field_are_untouched():
    fields = {"type": "task_start", "task_id": "abc"}
    assert _truncate_output_fields(fields) == fields


def test_input_dict_is_not_mutated():
    fields = {"line": "x" * 200_000, "task_id": "abc"}
    _truncate_output_fields(fields)
    assert len(fields["line"]) == 200_000


def test_results_stream_cap_is_untouched():
    # _STREAM_MAXLEN also caps the results and dead-letter streams, which the
    # pool manager independently caps at 20000. They must not diverge.
    assert _STREAM_MAXLEN == 20000


def test_fleet_wide_output_worst_case_fits_well_inside_maxmemory():
    # The archiver-down bound: every output stream at its cap, every entry at
    # the byte cap, across the largest fleet shape we support. Must leave room
    # for the events spool (~135MB) and the queue working set inside 1GB.
    worst = _OUTPUT_STREAM_COUNT_BUDGET * _OUTPUT_STREAM_MAXLEN * _MAX_OUTPUT_ENTRY_BYTES
    assert worst < 512 * 1024 * 1024, f"{worst / 1048576:.0f}MB exceeds the output budget"


def test_trim_tail_residue_stays_small():
    # _TRIM_TAIL_KEEP entries per stream are deliberately never trimmed, so they
    # are a permanent floor in healthy operation. Keep it far below the events
    # spool (~135MB) so steady state stays dominated by real queue data.
    from orcest.orchestrator.trace_archiver import _TRIM_TAIL_KEEP

    floor = _OUTPUT_STREAM_COUNT_BUDGET * _TRIM_TAIL_KEEP * _MAX_OUTPUT_ENTRY_BYTES
    assert floor < 64 * 1024 * 1024, f"{floor / 1048576:.0f}MB of untrimmable tail"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/worker/test_output_truncation.py -v`
Expected: FAIL — `ImportError: cannot import name '_MAX_OUTPUT_ENTRY_BYTES'`

- [ ] **Step 3: Write minimal implementation**

In `src/orcest/worker/loop.py`, beside `_STREAM_MAXLEN` (leave that line as-is):

```python
# Per-entry byte cap for OUTPUT streams only. _STREAM_MAXLEN is byte-blind: on
# 2026-08-21 one stream reached ~297MB at just 6062 entries (median 328B, mean
# 50,708B, max 265,829B) and took all of Redis with it. This cap sits far above
# the median line (328B), so ordinary output is untouched; it clips only the rare
# giant stream-json blob, recording the loss in-band as `truncated_bytes`.
# Fleet-wide worst case with the archiver down: 48 x 1000 x 8192 = 375MB.
_MAX_OUTPUT_ENTRY_BYTES = 8192
# Entry cap for OUTPUT streams only -- deliberately NOT _STREAM_MAXLEN, which
# also caps the results and dead-letter streams (pool_manager pins results at
# 20000). Steady state is near zero once the archiver trims; this value only has
# to survive an archiver outage.
_OUTPUT_STREAM_MAXLEN = 1000
# Largest fleet shape the output budget is sized for: projects x pool size,
# 4 x 12. Growing the pool past this needs the budget re-checked -- see
# test_fleet_wide_output_worst_case_fits_well_inside_maxmemory.
_OUTPUT_STREAM_COUNT_BUDGET = 48
```

Add above `_publish_task_output`:

```python
def _truncate_output_fields(
    fields: dict[str, str],
    max_bytes: int = _MAX_OUTPUT_ENTRY_BYTES,
) -> dict[str, str]:
    """Cap ``fields['line']`` at *max_bytes* UTF-8 bytes.

    Returns *fields* unchanged when there is no ``line`` or it already fits.
    Never splits a multi-byte character: the payload is decoded with
    ``errors="ignore"`` so a straddling code point is dropped whole rather than
    becoming U+FFFD.
    """
    line = fields.get("line")
    if line is None:
        return fields
    encoded = line.encode("utf-8")
    if len(encoded) <= max_bytes:
        return fields
    kept = encoded[:max_bytes].decode("utf-8", errors="ignore")
    truncated = dict(fields)
    truncated["line"] = kept
    truncated["truncated_bytes"] = str(len(encoded) - len(kept.encode("utf-8")))
    return truncated
```

And change `_publish_task_output` to truncate and use the output-only cap:

```python
def _publish_task_output(
    redis: RedisClient,
    stream: str,
    raw_stream: bool,
    fields: dict[str, str],
) -> None:
    fields = _truncate_output_fields(fields)
    if raw_stream:
        redis.xadd_capped_raw(stream, fields, maxlen=_OUTPUT_STREAM_MAXLEN)
    else:
        redis.xadd_capped(stream, fields, maxlen=_OUTPUT_STREAM_MAXLEN)
```

- [ ] **Step 4: Run tests to verify they pass**

First update the six output-stream assertions in `tests/worker/test_loop.py` (~796, ~829, ~921,
~926, ~931, ~941) from `maxlen=_STREAM_MAXLEN` to `maxlen=_OUTPUT_STREAM_MAXLEN`, adding that name
to the import at line 30. Leave 3081 and 4166 alone — those are the results stream.

Run: `python -m pytest tests/worker/test_output_truncation.py -v` → 10 passed
Run: `python -m pytest tests/worker -q && make lint` → no regressions

- [ ] **Step 5: Commit**

```bash
git add src/orcest/worker/loop.py tests/worker/test_output_truncation.py tests/worker/test_loop.py
git commit -m "fix(worker): cap output stream entries by bytes with an output-only MAXLEN"
```

---

# Phase B — Make the archiver a pressure valve

## Task 2: Per-project disk cursors

Spec R3, §2.3. `HSET` is `denyoom`, so under OOM the cursor persist failed, the pump `break`ed, and the next pass re-processed entries `_process_entry` had **already written to disk** — producing the 8 byte-identical trace blocks.

**[review] The cursor must be per project and must not sit in the archive root.** `RedisClient.scan_iter` returns **unprefixed** names (`redis_client.py:650`) and all four project containers render the same `trace_archive_path`, so `output:orcest-worker-10001` names a different stream in each project — one shared file would clobber across projects and silently strand streams. A file in the archive root also breaks `TestArchiverSecurity::test_rejects_path_traversal_task_id`, which asserts the root stays empty, so the cursor lives in a `.state/` subdirectory.

**Files:**
- Modify: `src/orcest/orchestrator/trace_archiver.py:27` (constants), `:56-70` (`__init__`), `:143-183` (`_pump_output_streams`)
- Modify: `tests/conftest.py` (`make_fake_redis_client` gains `key_prefix`)
- Modify: `tests/orchestrator/test_trace_archiver.py` (cursor-hash assertions ~180/~193; the security test's root assertion)
- Test: `tests/orchestrator/test_trace_archiver_cursor.py` (create)

**Interfaces:**
- Produces: `TraceArchiver._cursor_path() -> Path | None`, `._load_cursors() -> dict[str, str]`, `._save_cursors() -> bool`, and `self._cursors: dict[str, str]`.
- Real constructor (all four required): `TraceArchiver(redis, archive_path, repo_to_project, logger)`.

- [ ] **Step 1: Make the fixture support a key prefix, then write the failing test**

The fixture has no callers anywhere in `tests/`, so this is safe. In `tests/conftest.py`:

```python
    def factory(key_prefix: str = "test"):
        fake = fakeredis.FakeRedis(server=fake_redis_server, decode_responses=True)
        rc = RedisClient.from_client(fake, key_prefix=key_prefix)
```

Create `tests/orchestrator/test_trace_archiver_cursor.py`:

```python
"""Archiver cursors live on disk, per project -- never in Redis.

HSET is denyoom, so at the ceiling the old cursor persist failed, the pump broke
out, and the next pass re-archived entries already written to disk: the
2026-08-21 incident trace is 8 byte-identical copies of one block. scan_iter
returns UNPREFIXED names, so every project sees the same stream key and the
cursor file must be namespaced. See spec §2.3.
"""

from __future__ import annotations

import json
import logging

import pytest

from orcest.orchestrator.trace_archiver import TraceArchiver

pytestmark = pytest.mark.unit


def _archiver(tmp_path, redis):
    return TraceArchiver(
        redis=redis,
        archive_path=str(tmp_path),
        repo_to_project={},
        logger=logging.getLogger("test"),
    )


def test_cursor_roundtrips_through_disk(tmp_path, fake_redis_client):
    a = _archiver(tmp_path, fake_redis_client)
    a._cursors["output:w1"] = "5-0"
    assert a._save_cursors() is True
    assert _archiver(tmp_path, fake_redis_client)._load_cursors()["output:w1"] == "5-0"


def test_pump_does_not_write_the_redis_cursor_hash(tmp_path, fake_redis_client):
    c = fake_redis_client
    c.client.xadd(c._prefixed("output:w1"), {"type": "task_start", "task_id": "t1"})
    c.client.xadd(c._prefixed("output:w1"), {"line": "hi", "task_id": "t1"})
    _archiver(tmp_path, c)._pump_output_streams()
    assert not c.hgetall("trace_archiver:cursors")


def test_cursor_file_is_namespaced_per_project(tmp_path, make_fake_redis_client):
    a = _archiver(tmp_path, make_fake_redis_client("asemly"))
    b = _archiver(tmp_path, make_fake_redis_client("bbr-platform"))
    assert a._cursor_path() != b._cursor_path()


def test_two_projects_do_not_clobber_each_other(tmp_path, make_fake_redis_client):
    a = _archiver(tmp_path, make_fake_redis_client("asemly"))
    b = _archiver(tmp_path, make_fake_redis_client("bbr-platform"))
    a._cursors["output:w1"] = "100-0"
    a._save_cursors()
    b._cursors["output:w1"] = "5-0"
    b._save_cursors()
    reloaded = _archiver(tmp_path, make_fake_redis_client("asemly"))._load_cursors()
    assert reloaded["output:w1"] == "100-0"


def test_cursor_survives_a_redis_that_rejects_all_writes(tmp_path):
    import redis as redis_lib

    class Wedged:
        key_prefix = "asemly"

        def __getattr__(self, name):
            def boom(*a, **k):
                raise redis_lib.exceptions.OutOfMemoryError(
                    "command not allowed when used memory > 'maxmemory'."
                )

            return boom

    a = _archiver(tmp_path, Wedged())
    a._cursors["output:w1"] = "9-0"
    assert a._save_cursors() is True
    assert a._load_cursors()["output:w1"] == "9-0"


def test_missing_cursor_file_falls_back_to_the_redis_hash(tmp_path, fake_redis_client):
    # Deploy transition: without this the first run replays every stream from
    # 0-0 and re-archives everything resident -- the duplicate artifact this
    # task removes. HGETALL is a read and works at the ceiling.
    fake_redis_client.hset("trace_archiver:cursors", "output:w1", "42-0")
    assert _archiver(tmp_path, fake_redis_client)._load_cursors()["output:w1"] == "42-0"


def test_corrupt_cursor_file_yields_empty_mapping(tmp_path, fake_redis_client):
    a = _archiver(tmp_path, fake_redis_client)
    # _cursor_path() does not mkdir; nothing has written a cursor yet.
    a._cursor_path().parent.mkdir(parents=True, exist_ok=True)
    a._cursor_path().write_text("{not json", encoding="utf-8")
    assert _archiver(tmp_path, fake_redis_client)._load_cursors() == {}


def test_cursor_lives_outside_the_archive_root(tmp_path, fake_redis_client):
    # TestArchiverSecurity::test_rejects_path_traversal_task_id asserts the
    # archive root stays empty; state must not pollute it.
    a = _archiver(tmp_path, fake_redis_client)
    a._cursors["output:w1"] = "5-0"
    a._save_cursors()
    assert a._cursor_path().parent.name == ".state"
    assert not [p for p in tmp_path.iterdir() if p.name != ".state"]


def test_cursor_file_is_written_atomically_and_privately(tmp_path, fake_redis_client):
    a = _archiver(tmp_path, fake_redis_client)
    a._cursors["output:w1"] = "5-0"
    a._save_cursors()
    path = a._cursor_path()
    assert json.loads(path.read_text(encoding="utf-8"))["output:w1"] == "5-0"
    assert path.stat().st_mode & 0o777 == 0o600
    assert not list(path.parent.glob("*.tmp"))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/orchestrator/test_trace_archiver_cursor.py -v`
Expected: FAIL — `AttributeError: 'TraceArchiver' object has no attribute '_cursor_path'`

- [ ] **Step 3: Write minimal implementation**

Add near `_CURSOR_HASH_KEY` (keep that constant — `_load_cursors` still reads it):

```python
# Cursor state lives on the archive volume, NOT in Redis. HSET is denyoom, so a
# Redis at its ceiling cannot accept a cursor write -- and the entry is already
# on disk by then, so refusing to advance means re-archiving it next pass. A
# local cursor also lets the pump keep draining and XTRIM-ing (not denyoom)
# during an OOM event.
#
# One file PER PROJECT: scan_iter returns unprefixed stream names, so all four
# project archivers see the same "output:orcest-worker-NNNNN" key for different
# streams. In a .state/ subdirectory so the archive root stays clean for
# TestArchiverSecurity::test_rejects_path_traversal_task_id.
_STATE_DIRNAME = ".state"
_CURSOR_FILENAME_TEMPLATE = "cursors-{prefix}.json"
```

Add to `TraceArchiver`:

```python
def _cursor_path(self) -> Path | None:
    if self._archive_path is None:
        return None
    prefix = getattr(self._redis, "key_prefix", "default") or "default"
    safe = re.sub(r"[^A-Za-z0-9_.-]", "_", str(prefix))
    return self._archive_path / _STATE_DIRNAME / _CURSOR_FILENAME_TEMPLATE.format(prefix=safe)


def _load_cursors(self) -> dict[str, str]:
    """Read the cursor map, falling back to the legacy Redis hash on first run.

    A corrupt file yields an empty map, replaying from the start of each stream
    -- safe, because task_start finalizes a stale open handle rather than
    appending to it.
    """
    path = self._cursor_path()
    if path is None:
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        return {str(k): str(v) for k, v in raw.items()} if isinstance(raw, dict) else {}
    except ValueError:
        return {}
    except OSError:
        pass
    try:
        legacy = self._redis.hgetall(_CURSOR_HASH_KEY) or {}
        return {str(k): str(v) for k, v in legacy.items()}
    except Exception:
        return {}


def _save_cursors(self) -> bool:
    """Persist the whole cursor map atomically. Returns True on success.

    Called once per stream per pass, not per entry: _XREAD_COUNT is 500 and the
    archive volume is NFS, so per-entry rewrites would throttle the pump exactly
    when it most needs to drain. Correctness rests on archive-before-trim, not
    cursor-before-trim -- a stale-low cursor only re-reads entries still present.
    """
    path = self._cursor_path()
    if path is None:
        return False
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._write_file_0o600(tmp, json.dumps(self._cursors, indent=0) + "\n")
        os.replace(tmp, path)
    except OSError:
        self._logger.warning("Cursor persist failed", exc_info=True)
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass
        return False
    return True
```

In `__init__`, after `self._logger` is assigned, add
`self._cursors: dict[str, str] = self._load_cursors()`. Ensure `import os` and `import re` exist at
module top.

In `_pump_output_streams`, make the **interim** change precisely (Task 3 rewrites the method again):
read `last_id` from `self._cursors`, and after each `_process_entry` set
`self._cursors[stream] = entry_id` in place of the old `hset`, then call `self._save_cursors()` once
after each stream's entry loop. Both halves are required — `test_cursor_persists_across_pumps`
(rewritten in Step 4 to read `_load_cursors()`, which reads disk) stays red unless the interim pump
actually persists.

- [ ] **Step 4: Update the two existing tests, then run**

In `tests/orchestrator/test_trace_archiver.py`: change the cursor-hash assertions (~180, ~193) to read `archiver._load_cursors()` instead of `hgetall("trace_archiver:cursors")`, and relax the security test's root assertion:

```python
    contents = {p.name for p in archive_root.iterdir()} - {".state"}
    assert contents == set(), f"unexpected entries under archive root: {contents}"
```

Run: `python -m pytest tests/orchestrator/ -v`
Expected: all pass

- [ ] **Step 5: Commit**

```bash
git add src/orcest/orchestrator/trace_archiver.py tests/conftest.py tests/orchestrator/test_trace_archiver_cursor.py tests/orchestrator/test_trace_archiver.py
git commit -m "fix(archiver): per-project disk cursors so OOM cannot cause re-archiving"
```

---

## Task 3: Trim only what was durably archived, keeping a tail

Spec R3, §2.2. Output streams have `groups: 0`; nothing trims them. `XTRIM` is not `denyoom`, so this keeps recovering memory while Redis is wedged.

**[review] Two corrections to the obvious implementation.**

*Trim only genuinely-archived entries.* "Did `_process_entry` raise?" is the wrong test: `_handle_task_start` catches its own `OSError` from `os.open`, logs, and **returns** (`trace_archiver.py:222-224`), leaving `_stream_to_current_task` unset — so every following `_handle_line` hits `if not task_id: return` and drops the line silently. The same happens after a restart mid-task, when a valid cursor points past the `task_start`. Without a positive archived-signal, `XTRIM` would destroy a whole task's output.

*A count floor is not tail retention.* `XTRIM MINID` removes everything below the threshold id, so gating on `xlen > 200` and then trimming to the cursor leaves **one** entry, not 200 — verified empirically against fakeredis. To keep a tail for `orcest status`'s worker view, the trim id must be the older of the cursor and the id `_TRIM_TAIL_KEEP` entries from the tail.

**Files:**
- Modify: `src/orcest/orchestrator/trace_archiver.py` (`_process_entry`, `_handle_task_start`, `_handle_line`, `_pump_output_streams`), `src/orcest/shared/redis_client.py` (add `xrevrange` if absent)
- Test: `tests/orchestrator/test_trace_archiver_cursor.py` (extend)

**Interfaces:**
- Produces: `_process_entry(...) -> bool` (True = durably archived), `_handle_task_start(...) -> bool`, `_handle_line(...) -> bool`, `TraceArchiver._flush_task_handle(stream) -> None`, `TraceArchiver._trim_id_keeping_tail(stream, trim_to) -> str | None`, module-level `_stream_id_sort_key`.

- [ ] **Step 1: Write the failing test**

Append to `tests/orchestrator/test_trace_archiver_cursor.py`:

```python
def _seed_task(redis, stream="output:w1", task_id="t1", lines=4):
    key = redis._prefixed(stream)
    redis.client.xadd(key, {"type": "task_start", "task_id": task_id})
    for i in range(lines):
        redis.client.xadd(key, {"line": f"l{i}", "task_id": task_id})


def test_archived_entries_are_trimmed_once_past_the_tail(tmp_path, fake_redis_client, monkeypatch):
    monkeypatch.setattr("orcest.orchestrator.trace_archiver._TRIM_TAIL_KEEP", 2)
    _seed_task(fake_redis_client, lines=10)
    a = _archiver(tmp_path, fake_redis_client)
    a._pump_output_streams()
    assert fake_redis_client.client.xlen(fake_redis_client._prefixed("output:w1")) == 2


def test_tail_is_preserved_for_the_dashboard(tmp_path, fake_redis_client, monkeypatch):
    monkeypatch.setattr("orcest.orchestrator.trace_archiver._TRIM_TAIL_KEEP", 5)
    _seed_task(fake_redis_client, lines=20)
    a = _archiver(tmp_path, fake_redis_client)
    a._pump_output_streams()
    assert fake_redis_client.client.xlen(fake_redis_client._prefixed("output:w1")) == 5


def test_nothing_is_trimmed_when_archiving_is_disabled(fake_redis_client):
    _seed_task(fake_redis_client)
    before = fake_redis_client.client.xlen(fake_redis_client._prefixed("output:w1"))
    TraceArchiver(
        redis=fake_redis_client,
        archive_path=None,
        repo_to_project={},
        logger=logging.getLogger("t"),
    )._pump_output_streams()
    assert fake_redis_client.client.xlen(fake_redis_client._prefixed("output:w1")) == before


def test_lines_dropped_for_lack_of_a_handle_are_not_trimmed(
    tmp_path, fake_redis_client, monkeypatch
):
    # The silent-drop path: task_start's os.open fails, so no handle exists and
    # every later line is discarded WITHOUT raising. Trimming here would destroy
    # the whole task's output.
    monkeypatch.setattr("orcest.orchestrator.trace_archiver._TRIM_TAIL_KEEP", 0)
    _seed_task(fake_redis_client, lines=10)
    a = _archiver(tmp_path, fake_redis_client)
    monkeypatch.setattr(a, "_handle_task_start", lambda *args, **kw: False)
    a._pump_output_streams()
    assert fake_redis_client.client.xlen(fake_redis_client._prefixed("output:w1")) == 11
    assert a._cursors.get("output:w1", "0-0") == "0-0"


def test_a_raising_entry_is_not_trimmed_away(tmp_path, fake_redis_client, monkeypatch):
    monkeypatch.setattr("orcest.orchestrator.trace_archiver._TRIM_TAIL_KEEP", 0)
    _seed_task(fake_redis_client, lines=10)
    a = _archiver(tmp_path, fake_redis_client)

    def _boom(*args, **kw):
        raise RuntimeError("archive failed")

    monkeypatch.setattr(a, "_process_entry", _boom)
    a._pump_output_streams()
    assert fake_redis_client.client.xlen(fake_redis_client._prefixed("output:w1")) == 11


def test_handle_is_flushed_before_trimming(tmp_path, fake_redis_client, mocker, monkeypatch):
    monkeypatch.setattr("orcest.orchestrator.trace_archiver._TRIM_TAIL_KEEP", 0)
    _seed_task(fake_redis_client, lines=10)
    a = _archiver(tmp_path, fake_redis_client)
    calls = []
    mocker.patch.object(a, "_flush_task_handle", side_effect=lambda s: calls.append("flush"))
    mocker.patch.object(a._redis, "xtrim_minid", side_effect=lambda *x: calls.append("trim"))
    a._pump_output_streams()
    assert calls.index("flush") < calls.index("trim")


def test_trim_failure_does_not_abort_the_pump(tmp_path, fake_redis_client, mocker, monkeypatch):
    monkeypatch.setattr("orcest.orchestrator.trace_archiver._TRIM_TAIL_KEEP", 0)
    _seed_task(fake_redis_client, lines=10)
    a = _archiver(tmp_path, fake_redis_client)
    spy = mocker.patch.object(a._redis, "xtrim_minid", side_effect=RuntimeError("nope"))
    a._pump_output_streams()
    assert spy.call_count == 1  # the trim path really was entered
    assert a._cursors["output:w1"] != "0-0"


def test_truncation_marker_reaches_the_archive(tmp_path, fake_redis_client):
    c = fake_redis_client
    key = c._prefixed("output:w1")
    c.client.xadd(key, {"type": "task_start", "task_id": "t1"})
    c.client.xadd(key, {"line": "clipped", "task_id": "t1", "truncated_bytes": "9000"})
    a = _archiver(tmp_path, c)
    a._pump_output_streams()
    a._finalize_task("t1", status="complete")
    written = next(tmp_path.rglob("t1*.jsonl")).read_text(encoding="utf-8")
    assert "9000" in written and "orcest_truncated" in written
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/orchestrator/test_trace_archiver_cursor.py -k "trim or tail or flush or truncation" -v`
Expected: FAIL — nothing is trimmed; `_flush_task_handle` and `_TRIM_TAIL_KEEP` do not exist

- [ ] **Step 3: Write minimal implementation**

Add beside `_STATE_DIRNAME`:

```python
# Entries left in Redis behind the archived cursor so `orcest status`'s worker
# output view has something to tail. Real retention, not a trigger: XTRIM MINID
# deletes everything below the threshold, so trimming to the cursor itself would
# leave exactly one entry.
_TRIM_TAIL_KEEP = 100


def _stream_id_sort_key(entry_id: str) -> tuple[int, int]:
    ms, _, seq = entry_id.partition("-")
    return int(ms), int(seq or 0)
```

Change `_process_entry`, `_handle_task_start` and `_handle_line` to return a tri-state, because
"not archived" splits into two cases that need opposite handling:

```python
_SKIP = "skip"  # no handle will ever exist for this entry; advance past it
```

- `_handle_task_start`: `False` on the `OSError` branch (transient — retry next pass); `_SKIP` on
  the invalid-`task_id` branch (`trace_archiver.py:189-193`) — that entry is permanently
  unarchivable; `True` otherwise.
- `_handle_line`: `_SKIP` on both early returns (no current task for the stream, or no open handle
  — a restart-orphaned line, or one after `task_end` popped the mapping at `:261-262`); `True`
  after a successful write.
- `task_end`: `True`.

**[review] Without the `_SKIP` distinction every one of those paths halts the stream at the same
entry forever** — the cursor can never advance past it, so the pump re-reads 500 entries and logs a
warning once per second, indefinitely, and that stream is never trimmed again. Recovery would
depend on producer-side `MAXLEN` evicting past the bad entry, which never happens for a dead
worker's stream.

Then add:

```python
def _flush_task_handle(self, stream: str) -> None:
    """fsync the handle for *stream*'s current task before trimming Redis.

    Handles are line-buffered and otherwise only fsync'd at _finalize_task, so
    without this a host crash could lose lines from both Redis and disk.
    """
    task_id = self._stream_to_current_task.get(stream)
    handle = self._open_files.get(task_id) if task_id else None
    if handle is None:
        return
    try:
        handle.flush()
        os.fsync(handle.fileno())
    except OSError:
        self._logger.warning("Flush before trim failed for %s", stream, exc_info=True)


def _trim_id_keeping_tail(self, stream: str, trim_to: str) -> str | None:
    """Return the MINID to trim to, never newer than the archived cursor.

    Keeps up to _TRIM_TAIL_KEEP recent entries for the live dashboard view while
    never removing anything not yet archived. None means "not enough entries to
    trim yet".
    """
    if _TRIM_TAIL_KEEP <= 0:
        return trim_to
    try:
        tail = self._redis.xrevrange(stream, count=_TRIM_TAIL_KEEP)
    except Exception:
        return trim_to
    if len(tail) < _TRIM_TAIL_KEEP:
        return None
    oldest_kept = tail[-1][0]
    return min(oldest_kept, trim_to, key=_stream_id_sort_key)
```

Add `RedisClient.xrevrange(stream: str, count: int) -> list[tuple[str, dict[str, str]]]` mirroring `xread_after`'s prefixing, if it does not already exist. Replace `_pump_output_streams`:

```python
def _pump_output_streams(self) -> None:
    if self._archive_path is None:
        # Nothing durable to write to, so nothing may be trimmed either.
        return
    for stream in self._redis.scan_iter(match="output:*"):
        last_id = self._cursors.get(stream, "0-0")
        entries = self._redis.xread_after(stream, last_id, count=_XREAD_COUNT)
        if not entries:
            continue
        trim_to: str | None = None
        for entry_id, fields in entries:
            try:
                archived = self._process_entry(stream, entry_id, fields)
            except Exception:
                self._logger.error(
                    "Failed to archive stream=%s entry=%s; halting this stream "
                    "until the next pass",
                    stream,
                    entry_id,
                    exc_info=True,
                )
                break
            if archived is _SKIP:
                # No handle will ever exist for this entry: an invalid task_id on
                # task_start, a line orphaned by a restart mid-task, or a line
                # after task_end. Halting would wedge the stream forever, because
                # the cursor can never move past it. Advance, but do not make it
                # trim-eligible -- trim_to stays where it was.
                self._cursors[stream] = entry_id
                continue
            if not archived:
                # Transient failure (os.open or write errored). Retry next pass;
                # advancing would let XTRIM destroy output that never reached disk.
                self._logger.warning(
                    "Entry %s on %s was not archived; halting this stream until "
                    "the next pass",
                    entry_id,
                    stream,
                )
                break
            self._cursors[stream] = entry_id
            trim_to = entry_id
        if trim_to is None or not self._save_cursors():
            continue
        minid = self._trim_id_keeping_tail(stream, trim_to)
        if minid is None:
            continue
        self._flush_task_handle(stream)
        # XTRIM is NOT denyoom, so this succeeds even at the ceiling -- the one
        # lever that actively recovers memory during an OOM event.
        try:
            self._redis.xtrim_minid(stream, minid)
        except Exception:
            self._logger.warning(
                "Trim failed for %s at %s; entries stay until the next pass",
                stream,
                minid,
                exc_info=True,
            )
```

Finally, satisfy spec R4 by making truncation visible on disk. In `_handle_line`, before writing the payload:

```python
        dropped = fields.get("truncated_bytes")
        if dropped:
            handle.write(json.dumps({"orcest_truncated": {"bytes_dropped": int(dropped)}}) + "\n")
```

**[review]** Without this the archive keeps a silently clipped, invalid-JSON line, and `format_stream_json_line` (`dashboard.py:283`) returns `None` on the parse failure — so a 265 KB assistant message disappears from `orcest trace` entirely rather than appearing shortened.

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/orchestrator/ -v && make test-unit`
Expected: all pass

- [ ] **Step 5: Commit**

```bash
git add src/orcest/orchestrator/trace_archiver.py src/orcest/shared/redis_client.py tests/orchestrator/test_trace_archiver_cursor.py
git commit -m "fix(archiver): trim only durably-archived entries, keeping a dashboard tail"
```

---

# Phase C — Gate the pool-manager reap

## Task 4: Add reap-gating config to PoolConfig

**Files:**
- Modify: `src/orcest/fleet/config.py:179-232` (`PoolConfig`), `:641-644` (loader), `:747-750` (renderer)
- Test: `tests/fleet/test_config.py` (extend)

**Interfaces:**
- Produces: `activity_reap_min_age: int = 600`, `activity_reap_max_per_window: int = 2`, `activity_reap_window_seconds: int = 900`, `require_redis_writable_for_reap: bool = True`, `redis_writable_settle_seconds: int = 300`.

- [ ] **Step 1: Write the failing test**

Append to `tests/fleet/test_config.py` (house style is `save_config` → `load_config`; there is no `render_config`):

```python
def test_reap_gating_defaults():
    from orcest.fleet.config import PoolConfig

    p = PoolConfig()
    assert p.activity_reap_min_age == 600
    assert p.activity_reap_max_per_window == 2
    assert p.activity_reap_window_seconds == 900
    assert p.require_redis_writable_for_reap is True
    assert p.redis_writable_settle_seconds == 300


def test_reap_gating_round_trips_through_yaml(tmp_path):
    from orcest.fleet.config import FleetConfig, PoolConfig, load_config, save_config

    path = tmp_path / "config.yaml"
    save_config(
        FleetConfig(
            pool=PoolConfig(activity_reap_min_age=120, require_redis_writable_for_reap=False)
        ),
        path,
    )
    loaded = load_config(path)
    assert loaded.pool.activity_reap_min_age == 120
    assert loaded.pool.require_redis_writable_for_reap is False
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/fleet/test_config.py -k reap_gating -v`
Expected: FAIL — `AttributeError: 'PoolConfig' object has no attribute 'activity_reap_min_age'`

- [ ] **Step 3: Write minimal implementation**

In `PoolConfig`, after `activity_stale_after`:

```python
    # Below this many seconds of elapsed task time a VM is never destroyed for
    # activity_stale. A latched needs_reap="1" still fires immediately -- that is
    # the D-state escalation. On 2026-08-21 the reaper destroyed VMs at 42s,
    # 111s, 143s, 175s and 215s elapsed against a 25200s ceiling because Redis
    # OOM had erased every liveness signal.
    activity_reap_min_age: int = 600
    # Correlation breaker: more than this many DISTINCT VMs reaped for
    # activity_stale inside the window means an infrastructure fault, not that
    # many workers dying at once.
    activity_reap_max_per_window: int = 2
    activity_reap_window_seconds: int = 900
    # Require a proven-writable Redis before trusting an absent liveness key as
    # evidence of death. Heartbeats are SET (denyoom), so a Redis at its ceiling
    # erases them fleet-wide while reads still succeed and report them absent.
    require_redis_writable_for_reap: bool = True
    # How long Redis must have been continuously writable before an absent
    # heartbeat is believed again. Must exceed WORKER_LIVENESS_TTL (150s) plus
    # one HEARTBEAT_INTERVAL (60s), or healthy workers are reaped in the window
    # between Redis recovering and their next heartbeat write.
    redis_writable_settle_seconds: int = 300
```

Loader, beside `activity_stale_after`:

```python
        activity_reap_min_age=int(pl.get("activity_reap_min_age", 600)),
        activity_reap_max_per_window=int(pl.get("activity_reap_max_per_window", 2)),
        activity_reap_window_seconds=int(pl.get("activity_reap_window_seconds", 900)),
        require_redis_writable_for_reap=bool(pl.get("require_redis_writable_for_reap", True)),
        redis_writable_settle_seconds=int(pl.get("redis_writable_settle_seconds", 300)),
```

Renderer, beside `"activity_stale_after"`:

```python
            "activity_reap_min_age": config.pool.activity_reap_min_age,
            "activity_reap_max_per_window": config.pool.activity_reap_max_per_window,
            "activity_reap_window_seconds": config.pool.activity_reap_window_seconds,
            "require_redis_writable_for_reap": config.pool.require_redis_writable_for_reap,
            "redis_writable_settle_seconds": config.pool.redis_writable_settle_seconds,
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/fleet/test_config.py -v`

- [ ] **Step 5: Commit**

```bash
git add src/orcest/fleet/config.py tests/fleet/test_config.py
git commit -m "feat(fleet): add reap-gating knobs to PoolConfig"
```

---

## Task 5: Writability probe with recovery hysteresis

Spec R5. Reads succeeded and honestly reported keys absent, so every existing fail-safe branch was bypassed.

**[review] A bare "is Redis writable right now?" probe re-opens the kill window at *recovery*.** Heartbeats have `WORKER_LIVENESS_TTL = 150` and refresh at `lock.ttl / 3` = 60 s (`worker/loop.py:75-76`, `heartbeat.py:40`). During the outage every key expires. The instant an operator frees memory the canary succeeds, but live workers have up to 60 s before their next refresh — and `elapsed` is ~17,600 s after a 4h53m outage, so the min-age grace does nothing. Without hysteresis the fix still destroys 2 healthy VMs (breaker-capped) on **every** recovery. Hence `redis_writable_settle_seconds = 300`.

**Files:**
- Modify: `src/orcest/fleet/pool_manager.py` (`__init__`, `_health_check`, `_activity_reap_reason`)
- Modify: `tests/fleet/test_pool_manager_activity.py` — three `activity_stale` arrangements need the
  probe pre-settled (see Step 3)
- Test: `tests/fleet/test_pool_manager_reap_gating.py` (create)

**Interfaces:**
- Produces: `PoolManager._redis_is_writable(now: float) -> bool`, backed by `self._writable_this_pass: bool | None`, `self._writable_since: float | None`, `self._unwritable_passes: int`. `_activity_reap_reason` gains `elapsed: float`.

- [ ] **Step 1: Write the failing test**

Create `tests/fleet/test_pool_manager_reap_gating.py`:

```python
"""Reap gating: a Redis that cannot accept writes is not evidence of death.

Heartbeats are SET (denyoom). At the ceiling those writes are refused, the keys
TTL out at WORKER_LIVENESS_TTL=150, and EXISTS then succeeds and honestly
returns 0 -- so the pre-existing fail-safe branches (which only catch read
*exceptions*) never fire. See spec §3.
"""

from __future__ import annotations

import time

import pytest
import redis as redis_lib

from orcest.fleet.pool_manager import PoolManager

from .test_pool_manager import _make_config, _make_proxmox

pytestmark = pytest.mark.unit


def _oom(*a, **k):
    raise redis_lib.exceptions.OutOfMemoryError(
        "command not allowed when used memory > 'maxmemory'."
    )


def _pm(fake_redis_client):
    # key_prefix must match the fixture's "test" prefix, or every key the manager
    # writes lands under "orcest:" and the assertions read the wrong namespace.
    return PoolManager(_make_config(), _make_proxmox(), fake_redis_client, key_prefix="test")


def test_probe_is_false_when_redis_rejects_writes(fake_redis_client, mocker):
    pm = _pm(fake_redis_client)
    mocker.patch.object(pm._redis, "set_ex_raw", side_effect=_oom)
    assert pm._redis_is_writable(time.time()) is False


def test_probe_is_false_until_the_settle_window_elapses(fake_redis_client, mocker):
    pm = _pm(fake_redis_client)
    t = time.time()
    mocker.patch.object(pm._redis, "set_ex_raw", side_effect=_oom)
    assert pm._redis_is_writable(t) is False
    mocker.patch.object(pm._redis, "set_ex_raw", return_value=None)
    pm._writable_this_pass = None
    assert pm._redis_is_writable(t + 10) is False  # writable, but not settled
    pm._writable_this_pass = None
    assert pm._redis_is_writable(t + 400) is True


def test_activity_stale_is_suppressed_while_redis_is_unwritable(fake_redis_client, mocker):
    pm = _pm(fake_redis_client)
    mocker.patch.object(pm._redis, "set_ex_raw", side_effect=_oom)
    mocker.patch.object(pm, "_worker_heartbeat_present", return_value=False)
    mocker.patch.object(
        pm, "_consumers_with_pending_status", return_value=({"orcest-worker-10003"}, True)
    )
    assert pm._activity_reap_reason(10003, time.time(), elapsed=9999.0) is None


def test_needs_reap_still_fires_while_redis_is_unwritable(fake_redis_client, mocker):
    pm = _pm(fake_redis_client)
    mocker.patch.object(pm._redis, "set_ex_raw", side_effect=_oom)
    mocker.patch.object(pm._redis, "hgetall_raw", return_value={"needs_reap": "1"})
    assert pm._activity_reap_reason(10003, time.time(), elapsed=9999.0) is not None


def test_probe_memo_is_reset_each_health_check_pass(fake_redis_client, mocker):
    pm = _pm(fake_redis_client)
    mocker.patch.object(pm._redis, "hgetall", return_value={})
    pm._writable_this_pass = True
    pm._health_check()
    assert pm._writable_this_pass is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/fleet/test_pool_manager_reap_gating.py -v`
Expected: FAIL — `AttributeError: 'PoolManager' object has no attribute '_redis_is_writable'`

- [ ] **Step 3: Write minimal implementation**

Initialise in `__init__`: `self._writable_this_pass: bool | None = None`, `self._writable_since: float | None = None`, `self._unwritable_passes: int = 0`. Add:

```python
# Class attribute on PoolManager (referenced as self._WRITABILITY_CANARY_KEY).
_WRITABILITY_CANARY_KEY = "orcest:pool:reap_canary"


def _redis_is_writable(self, now: float) -> bool:
    """True if Redis has accepted denyoom writes for long enough to trust an
    absent liveness key.

    Liveness evidence is written with denyoom commands. At the ceiling those
    writes are refused while reads still succeed, so an absent key means "Redis
    is full", not "the worker is dead". Probing with the same command class is
    the only way to separate them -- and the probe must then *settle*, because
    for up to one HEARTBEAT_INTERVAL after recovery a healthy worker's key is
    still legitimately absent.
    """
    if self._writable_this_pass is not None:
        return self._writable_this_pass
    try:
        self._redis.set_ex_raw(self._WRITABILITY_CANARY_KEY, str(now), 60)
        raw_writable = True
    except Exception:
        raw_writable = False

    if not raw_writable:
        self._writable_since = None
        self._unwritable_passes += 1
        n = self._unwritable_passes
        while n % 10 == 0:
            n //= 10
        if n == 1:
            logger.warning(
                "Redis is not accepting writes (pass #%d); suppressing all "
                "activity_stale reaps -- absent liveness keys are not proof of death",
                self._unwritable_passes,
            )
        self._writable_this_pass = False
        return False

    self._unwritable_passes = 0
    if self._writable_since is None:
        self._writable_since = now
    settled = (now - self._writable_since) >= self._pool.redis_writable_settle_seconds
    self._writable_this_pass = settled
    return settled
```

Change `_activity_reap_reason` to `(self, vm_id: int, now: float, elapsed: float) -> str | None` and insert, after the `needs_reap` branch and before `_activity_record_is_stale`:

```python
        if self._pool.require_redis_writable_for_reap and not self._redis_is_writable(now):
            return None
```

Reset the memo at the top of `_health_check` with `self._writable_this_pass = None`, and pass
`elapsed=elapsed` at the call site.

**This breaks three existing tests, and it must be fixed here, not in Task 6.** On a fresh
`PoolManager` the first probe against fakeredis *succeeds* but sets `_writable_since = now`, so
`0 >= 300` is False and every `activity_stale` path returns `None`. That turns
`test_absent_record_with_pending_entries_destroys`,
`test_true_worker_death_no_record_no_heartbeat_pending_destroys` and `test_reaped_event_reason_field`
(stale case) in `tests/fleet/test_pool_manager_activity.py` red at *this* task. Task 6's fixture
reseed to `now - 1200` addresses only the min-age grace and leaves them red. Pre-settle the probe in
those three arrangements:

```python
    manager._writable_since = 0.0   # probe already settled; this test is not about hysteresis
```

Add `tests/fleet/test_pool_manager_activity.py` to this task's Files list.

**Production consequence to record in Task 9:** every pool-manager restart now suppresses
`activity_stale` reaps for 300 s, because `_writable_since` starts unset.

**[review]** Use `self._pool` (bound at `__init__:134`), not `self._config.pool` — every existing reap-path reader uses `self._pool`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/fleet/ -v` — the three pre-settled activity tests must be green too.

- [ ] **Step 5: Commit**

```bash
git add src/orcest/fleet/pool_manager.py tests/fleet/test_pool_manager_reap_gating.py tests/fleet/test_pool_manager_activity.py
git commit -m "fix(fleet): gate activity_stale reaps on a settled-writable Redis"
```

---

## Task 6: Min-age grace, correlation breaker, and fenced-VM passthrough

Spec R5, R6.

**[review] The gates must not strand VMs that are already fenced.** `_health_check` re-derives the reason every pass and only reaches `_coordinate_reaped_vm` when it is non-None. Once Redis goes unwritable or the breaker trips, an already-stopped VM's coordination retry stops running — leaving it stopped, holding its `pool:active` slot and an unrecovered PEL entry, until the 25200 s ceiling. A fenced VM must short-circuit all gating.

**Accepted deviation from R6.** The spec asks that a correlated event "never destroy more than a
bounded *fraction* of the pool". The breaker bounds a *rate*: it self-resets once the window drains
(nothing is recorded while suppressed), so a sustained correlated fault with a settled-writable
Redis still destroys 2 VMs per 15 minutes — the whole pool of 4 in about half an hour. There is no
latch and no operator acknowledgement. That is adequate for the incident class this plan targets,
because the writability gate stops the Redis-OOM case outright, but it does not literally satisfy
R6. Record it as a deviation with a follow-up rather than claiming R6 is met.

**Files:**
- Modify: `src/orcest/fleet/pool_manager.py`
- Modify: `tests/fleet/test_pool_manager_activity.py` — three `activity_stale` fixtures seed `pool:active` at `now - 10`, below the new grace, and will fail
- Test: `tests/fleet/test_pool_manager_reap_gating.py` (extend)

**Interfaces:**
- Produces: `_record_activity_reap(now)`, `_activity_reap_breaker_tripped(now) -> bool` (logs only on transition), `self._recent_activity_reaps: deque[float]`, `self._counted_activity_reaps: set[int]`, `self._breaker_open: bool`, `self._fenced_at: dict[int, tuple[float, float]]` (Task 8 uses the last).

- [ ] **Step 1: Write the failing test**

Append to `tests/fleet/test_pool_manager_reap_gating.py`:

```python
def _stale_pm(fake_redis_client, mocker):
    pm = _pm(fake_redis_client)
    mocker.patch.object(pm, "_worker_heartbeat_present", return_value=False)
    mocker.patch.object(
        pm, "_consumers_with_pending_status", return_value=({"orcest-worker-10003"}, True)
    )
    mocker.patch.object(pm, "_redis_is_writable", return_value=True)
    return pm


def test_young_vm_is_never_reaped_for_activity_stale(fake_redis_client, mocker):
    pm = _stale_pm(fake_redis_client, mocker)
    assert pm._activity_reap_reason(10003, time.time(), elapsed=143.0) is None


def test_old_vm_is_still_reaped_for_activity_stale(fake_redis_client, mocker):
    pm = _stale_pm(fake_redis_client, mocker)
    assert pm._activity_reap_reason(10003, time.time(), elapsed=1200.0) is not None


def test_needs_reap_ignores_the_min_age_grace(fake_redis_client, mocker):
    pm = _pm(fake_redis_client)
    mocker.patch.object(pm._redis, "hgetall_raw", return_value={"needs_reap": "1"})
    assert pm._activity_reap_reason(10003, time.time(), elapsed=5.0) is not None


def test_breaker_trips_after_too_many_distinct_vms(fake_redis_client):
    pm = _pm(fake_redis_client)
    now = time.time()
    pm._record_activity_reap(now)
    pm._record_activity_reap(now)
    assert pm._activity_reap_breaker_tripped(now) is True


def test_breaker_resets_once_the_window_drains(fake_redis_client):
    pm = _pm(fake_redis_client)
    now = time.time()
    pm._record_activity_reap(now)
    pm._record_activity_reap(now)
    assert pm._activity_reap_breaker_tripped(now + 901) is False


def test_breaker_logs_only_on_the_trip_transition(fake_redis_client, caplog):
    pm = _pm(fake_redis_client)
    now = time.time()
    pm._record_activity_reap(now)
    pm._record_activity_reap(now)
    with caplog.at_level("ERROR"):
        for _ in range(5):
            pm._activity_reap_breaker_tripped(now)
    assert sum("breaker" in r.message.lower() for r in caplog.records) == 1


def test_already_fenced_vm_bypasses_every_gate(fake_redis_client, mocker):
    # Otherwise the coordination retry for a stopped VM stops running the moment
    # Redis goes unwritable or the breaker trips, stranding it to the ceiling.
    pm = _pm(fake_redis_client)
    mocker.patch.object(pm._redis, "set_ex_raw", side_effect=_oom)
    pm._fenced_at[10003] = (time.time(), 143.0)
    assert pm._activity_reap_reason(10003, time.time(), elapsed=143.0) is not None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/fleet/test_pool_manager_reap_gating.py -k "young or breaker or fenced" -v`
Expected: FAIL — young VM is reaped; `_record_activity_reap` and `_fenced_at` do not exist

- [ ] **Step 3: Write minimal implementation**

Add `from collections import deque`; initialise `self._recent_activity_reaps: deque[float] = deque()`, `self._counted_activity_reaps: set[int] = set()`, `self._breaker_open: bool = False`, `self._fenced_at: dict[int, tuple[float, float]] = {}`. Add:

```python
def _record_activity_reap(self, now: float) -> None:
    self._recent_activity_reaps.append(now)


def _activity_reap_breaker_tripped(self, now: float) -> bool:
    """True once activity_stale destroys of DISTINCT VMs exceed the cap.

    Many workers going stale at the same instant is an infrastructure fault, not
    many workers dying at once: on 2026-08-21 five sub-ceiling destroys landed
    inside 26 minutes and cycled the whole pool.
    """
    window = self._pool.activity_reap_window_seconds
    while self._recent_activity_reaps and now - self._recent_activity_reaps[0] > window:
        self._recent_activity_reaps.popleft()
    tripped = len(self._recent_activity_reaps) >= self._pool.activity_reap_max_per_window
    if tripped and not self._breaker_open:
        logger.error(
            "Activity-stale reap breaker TRIPPED: %d distinct VMs destroyed in %ds "
            "exceeds the cap of %d. Suppressing further activity_stale reaps -- "
            "investigate shared infrastructure (Redis writability, network) first.",
            len(self._recent_activity_reaps),
            window,
            self._pool.activity_reap_max_per_window,
        )
    elif not tripped and self._breaker_open:
        logger.info("Activity-stale reap breaker cleared")
    self._breaker_open = tripped
    return tripped
```

In `_activity_reap_reason`, immediately after the `needs_reap` branch:

```python
        if vm_id in self._fenced_at:
            # Already stopped; this pass is a coordination retry, not a new kill
            # decision. Gating it would strand the VM until the ceiling. Note this
            # sits AFTER the hgetall_raw read, so a Redis failing *reads* still
            # strands a fenced VM via the existing read-exception guard -- the
            # incident class was write failure, so that gap is accepted. The
            # reason is reported as activity_stale even if the VM was fenced for
            # a needs_reap that has since TTL'd out.
            return REAP_REASON_ACTIVITY_STALE
```

then the Task 5 writability gate, then:

```python
        if elapsed < self._pool.activity_reap_min_age:
            return None
        if self._activity_reap_breaker_tripped(now):
            return None
```

In `_health_check`, count each VM once, after a successful fence:

```python
            if reason == REAP_REASON_ACTIVITY_STALE and vm_id not in self._counted_activity_reaps:
                self._counted_activity_reaps.add(vm_id)
                self._record_activity_reap(now)
```

- [ ] **Step 4: Update the three existing fixtures, then run**

In `tests/fleet/test_pool_manager_activity.py` the `activity_stale` cases seed `rc.hset("pool:active", "305", str(now - 10))`. Change those three to `str(now - 1200)` (above `activity_reap_min_age`): `test_absent_record_with_pending_entries_destroys`, `test_true_worker_death_no_record_no_heartbeat_pending_destroys`, `test_reaped_event_reason_field`. Leave the `needs_reap` and `ceiling` cases at `now - 10` — they must keep passing, which proves the grace is scoped correctly.

Run: `python -m pytest tests/fleet/ -v && make lint`
Expected: all pass

- [ ] **Step 5: Commit**

```bash
git add src/orcest/fleet/pool_manager.py tests/fleet/test_pool_manager_reap_gating.py tests/fleet/test_pool_manager_activity.py
git commit -m "feat(fleet): min-age grace and correlation breaker for activity_stale reaps"
```

---

# Phase D — Stranded work, telemetry, docs

## Task 7: Detect task streams with work and no consumers

Spec §4.

**Scope (explicit deviation):** detection only. It does **not** clear the `SET NX` pending marker, dead-letter the entries, or stop `ProviderPool` re-drawing the dead provider — so asemly #1289 and its nine dependents stay stalled until the ~16.7 h TTL. **R7's second clause is therefore not satisfied by this plan**; record it as an accepted deviation with a follow-up, not as met.

**[review] Reuse `rollout_health._raw_stream_work`.** It already TYPE-checks the key and fails *closed* when `lag`/`pending`/`consumers` are null — the exact hazard a hand-rolled detector hits, since an unguarded `xlen` on a non-stream key raises `WRONGTYPE` and would abort `_pass_once` roughly once a second in four containers. Note also that `consumers` counts *registered* consumers, so a stream stranded behind dead consumers is not detected until `XGROUP DELCONSUMER` runs; that limitation is recorded in project memory.

**Files:**
- Modify: `src/orcest/orchestrator/fleet_health.py`
- Test: `tests/orchestrator/test_stranded_streams.py` (create)

**Interfaces:**
- Produces: `find_stranded_task_streams(redis: RedisClient) -> list[tuple[str, int]]` and `FleetHealthMonitor._report_stranded_streams(now: float | None = None) -> None`, with `self._stranded_since: dict[str, float]` for dwell-gated, transition-only logging.

- [ ] **Step 1: Write the failing test**

Create `tests/orchestrator/test_stranded_streams.py`:

```python
"""A provider stream with queued work and no consumers strands that work.

asemly #1289 was enqueued to orcest:tasks:issue:grok (consumers=0, lag=3) and
never claimed; its SET NX pending marker then blocked re-publication under any
other provider for ~16.7h, stalling nine dependent issues. See spec §4.
"""

from __future__ import annotations

import pytest

from orcest.orchestrator.fleet_health import find_stranded_task_streams

pytestmark = pytest.mark.unit


def test_stream_with_entries_and_no_consumers_is_reported(fake_redis_client):
    # Order matters: fakeredis 2.34.1 only counts lag for entries added AFTER the
    # group exists, and reports lag 0 (not None) otherwise. Real Redis reports it
    # either way -- the spec observed lag=3 in production.
    c = fake_redis_client.client
    c.xgroup_create("orcest:tasks:issue:grok", "workers", id="$", mkstream=True)
    c.xadd("orcest:tasks:issue:grok", {"task_id": "t1"})
    assert find_stranded_task_streams(fake_redis_client) == [("orcest:tasks:issue:grok", 1)]


def test_stream_with_a_registered_consumer_is_not_reported(fake_redis_client):
    # NOTE: `consumers` counts registered, not live, consumers. A stream behind a
    # dead consumer is not detected until XGROUP DELCONSUMER runs.
    c = fake_redis_client.client
    c.xadd("orcest:tasks:issue:clauder", {"task_id": "t1"})
    c.xgroup_create("orcest:tasks:issue:clauder", "workers", id="0")
    c.xreadgroup("workers", "w1", {"orcest:tasks:issue:clauder": ">"}, count=1)
    assert find_stranded_task_streams(fake_redis_client) == []


def test_empty_stream_is_not_reported(fake_redis_client):
    fake_redis_client.client.xgroup_create(
        "orcest:tasks:issue:clauder", "workers", id="$", mkstream=True
    )
    assert find_stranded_task_streams(fake_redis_client) == []


def test_non_stream_key_under_the_prefix_is_ignored(fake_redis_client):
    fake_redis_client.client.set("orcest:tasks:some-lock", "held")
    assert find_stranded_task_streams(fake_redis_client) == []


def test_stranded_stream_is_reported_once_after_the_dwell(fake_redis_client, caplog, mocker):
    from orcest.orchestrator.fleet_health import FleetHealthMonitor

    fake_redis_client.client.xadd("orcest:tasks:issue:grok", {"task_id": "t1"})
    mon = mocker.MagicMock()
    mon._redis = fake_redis_client
    mon._stranded_since = {}
    with caplog.at_level("ERROR"):
        FleetHealthMonitor._report_stranded_streams(mon, now=1000.0)  # starts dwell
        FleetHealthMonitor._report_stranded_streams(mon, now=1001.0)  # within dwell
        assert not [r for r in caplog.records if "stranded" in r.message]
        FleetHealthMonitor._report_stranded_streams(mon, now=1400.0)  # dwell satisfied
        FleetHealthMonitor._report_stranded_streams(mon, now=1500.0)  # already reported
    assert sum("stranded" in r.message for r in caplog.records) == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/orchestrator/test_stranded_streams.py -v`
Expected: FAIL — `ImportError: cannot import name 'find_stranded_task_streams'`

- [ ] **Step 3: Write minimal implementation**

In `src/orcest/orchestrator/fleet_health.py`:

```python
_STRANDED_DWELL_SECONDS = 300.0
_REPORTED = -1.0


def find_stranded_task_streams(redis: RedisClient) -> list[tuple[str, int]]:
    """Return (stream, unclaimed) for task streams holding work no one can claim.

    Delegates to rollout_health._raw_stream_work, which TYPE-checks the key and
    fails closed when consumer-group counters are unavailable -- a raw xlen here
    would raise WRONGTYPE on any non-stream key under the prefix and abort the
    whole health pass.
    """
    from orcest.rollout_health import _raw_stream_work

    stranded: list[tuple[str, int]] = []
    # Hardcodes the "orcest" task_key_prefix (OrchestratorConfig.task_key_prefix,
    # env ORCEST_TASK_KEY_PREFIX). A non-default prefix silently detects nothing.
    for raw in redis.client.scan_iter(match="orcest:tasks:*", count=100):
        name = raw if isinstance(raw, str) else raw.decode()
        work, _pending, _lag, unconsumed, err = _raw_stream_work(
            redis, name, allow_non_stream=True
        )
        if err or not unconsumed:
            continue
        stranded.append((name, work))
    return sorted(stranded)
```

And on `FleetHealthMonitor` — initialise `self._stranded_since: dict[str, float] = {}`, and call `self._report_stranded_streams()` **last** in `_pass_once`, wrapped in its own `try/except` so it can never abort the pressure-gate work above it:

```python
def _report_stranded_streams(self, now: float | None = None) -> None:
    """Log a stranded task stream once, after it has persisted for the dwell.

    _pass_once runs ~1/s in each of four project containers, and a stream can be
    briefly consumer-less during normal VM recycling, so this needs both a dwell
    and transition-only logging. Expect one line per container, not one total.
    """
    now = time.monotonic() if now is None else now
    stranded = find_stranded_task_streams(self._redis)
    names = {name for name, _ in stranded}
    for name, depth in stranded:
        first_seen = self._stranded_since.setdefault(name, now)
        if first_seen == _REPORTED:
            continue
        if now - first_seen < _STRANDED_DWELL_SECONDS:
            continue
        self._stranded_since[name] = _REPORTED
        logger.error(
            "Task stream %s has held %d entries with no consumer for over %ds -- no "
            "worker for this provider has attached, so that work is stranded and its "
            "pending markers block retry under another provider until they expire",
            name,
            depth,
            int(_STRANDED_DWELL_SECONDS),
        )
    for gone in set(self._stranded_since) - names:
        del self._stranded_since[gone]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/orchestrator/test_stranded_streams.py -v && make test-unit`

- [ ] **Step 5: Commit**

```bash
git add src/orcest/orchestrator/fleet_health.py tests/orchestrator/test_stranded_streams.py
git commit -m "feat(orchestrator): warn when a task stream holds work with no consumers"
```

---

## Task 8: Report kill-time, not publish-time, in reap telemetry

Spec R8, §5. VM 10003 was stopped at 09:55:22Z at 143 s elapsed; its event says 14:08:15Z / `elapsed_seconds: 15316`, because `elapsed` is recomputed from `pool:active` on every failed retry. This fabricated a 4.4-hour hang.

**[review] Keep re-asserting the Proxmox stop.** `_health_check` currently calls `_stop_vm` every retry pass, and the fence exists so a worker cannot publish a late success while the reaper publishes failure. Memoise only the timestamps; a VM restarted by HA or an operator during a 4-hour retry loop must be re-fenced.

**Files:**
- Modify: `src/orcest/fleet/pool_manager.py` (`_health_check`, `_coordinate_reaped_vm`, `_publish_reaped_failure`, the reap-event emitter)
- Test: `tests/fleet/test_pool_manager_reap_gating.py` (extend)

**Interfaces:**
- Produces: `_fence_vm(vm_id, now, elapsed) -> tuple[float, float] | None`, `_clear_fence(vm_id) -> None`. `_coordinate_reaped_vm` gains `killed_at: float | None = None` — **optional**, because four other callers (`pool_manager.py:207, 393, 537, 2489`) pass none.

- [ ] **Step 1: Write the failing test**

Append to `tests/fleet/test_pool_manager_reap_gating.py`:

```python
def test_elapsed_is_frozen_at_first_fence(fake_redis_client, mocker):
    pm = _pm(fake_redis_client)
    mocker.patch.object(pm, "_stop_vm", return_value=True)
    t0 = time.time()
    assert pm._fence_vm(10003, now=t0, elapsed=143.0) == (t0, 143.0)
    assert pm._fence_vm(10003, now=t0 + 4000, elapsed=4143.0) == (t0, 143.0)


def test_fence_keeps_reasserting_the_stop(fake_redis_client, mocker):
    pm = _pm(fake_redis_client)
    stop = mocker.patch.object(pm, "_stop_vm", return_value=True)
    t0 = time.time()
    pm._fence_vm(10003, now=t0, elapsed=143.0)
    pm._fence_vm(10003, now=t0 + 10, elapsed=153.0)
    assert stop.call_count == 2  # a VM restarted by HA must be re-fenced


def test_fence_memo_is_cleared_after_destroy(fake_redis_client, mocker):
    pm = _pm(fake_redis_client)
    mocker.patch.object(pm, "_stop_vm", return_value=True)
    t0 = time.time()
    pm._fence_vm(10003, now=t0, elapsed=143.0)
    pm._counted_activity_reaps.add(10003)
    pm._clear_fence(10003)
    assert pm._fence_vm(10003, now=t0 + 10, elapsed=153.0) == (t0 + 10, 153.0)
    assert 10003 not in pm._counted_activity_reaps


def test_reaped_event_reports_kill_time_not_retry_time(fake_redis_client, mocker):
    """The actual bug: a task fenced at 1200s reported as a 15316s hang.

    _coordinate_reaped_vm publishes nothing unless the consumer really holds a
    pending entry, so this has to seed and claim a task the way
    tests/fleet/test_pool_manager_events.py does.
    """
    from .test_pool_manager_events import _build, _claim_task, _reaped_events

    rc = fake_redis_client  # prefix 'test'
    manager, _proxmox = _build(rc)
    _claim_task(rc, "orcest-worker-305")

    t0 = time.time()
    rc.hset("pool:active", "305", str(t0 - 1200))  # elapsed 1200s, past min-age
    manager._writable_since = 0.0  # probe settled; this test is not about hysteresis
    mocker.patch.object(manager, "_worker_heartbeat_present", return_value=False)

    # Pass 1: fence succeeds, Redis recovery fails -> retry, no event yet.
    mocker.patch.object(manager, "_coordinate_reaped_vm", return_value=False)
    mocker.patch("time.time", return_value=t0)
    manager._health_check()
    assert not _reaped_events(rc)

    # Pass 2, four hours later: coordination succeeds. The event must report the
    # elapsed frozen at the fence, not now - start_ts.
    mocker.stopall()
    mocker.patch.object(manager, "_worker_heartbeat_present", return_value=False)
    mocker.patch("time.time", return_value=t0 + 14400)
    manager._health_check()

    reaped = _reaped_events(rc)
    assert reaped, "no reaped event published"
    data = reaped[-1]["data"]
    assert 1195 <= data["elapsed_seconds"] <= 1205, data["elapsed_seconds"]
    assert data["killed_at"]
```

`_build`, `_claim_task` and `_reaped_events` are real helpers at
`tests/fleet/test_pool_manager_events.py:29,42,62`; `tests/fleet/__init__.py` exists so the relative
import resolves. If patching `time.time` globally proves too broad, thread an injectable clock into
`_health_check` instead — but do not drop the two-pass structure, which is the only thing that
actually reproduces the bug.

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/fleet/test_pool_manager_reap_gating.py -k fence -v`
Expected: FAIL — `AttributeError: 'PoolManager' object has no attribute '_fence_vm'`

- [ ] **Step 3: Write minimal implementation**

```python
def _fence_vm(self, vm_id: int, now: float, elapsed: float) -> tuple[float, float] | None:
    """Stop *vm_id* and remember when. Returns (killed_at, elapsed_at_kill).

    The stop is re-asserted every pass (it is idempotent) so a VM restarted by
    HA or an operator during a long retry loop cannot run unfenced. Only the
    timestamps are memoised: Redis recovery can fail for hours and _health_check
    retries the whole reap each pass, so recomputing `elapsed` would report the
    retry duration instead of the task's real runtime.
    """
    if not self._stop_vm(vm_id):
        # Re-assert failed. Returning the memo lets coordination proceed against a
        # VM that may have been restarted -- accepted, because the alternative is
        # never completing recovery for a VM Proxmox will not answer for. The
        # ACK/publish race this fence guards is narrower than a permanently
        # stranded PEL entry.
        return self._fenced_at.get(vm_id)
    self._fenced_at.setdefault(vm_id, (now, elapsed))
    return self._fenced_at[vm_id]


def _clear_fence(self, vm_id: int) -> None:
    self._fenced_at.pop(vm_id, None)
    self._counted_activity_reaps.discard(vm_id)
```

In `_health_check`, replace the `if not self._stop_vm(vm_id):` block with a `_fence_vm` call, unpack `killed_at, elapsed_at_kill`, and pass `elapsed_seconds=elapsed_at_kill, killed_at=killed_at`. Call `self._clear_fence(vm_id)` immediately after `self._destroy_stopped_vm(vm_id)`.

Add `killed_at: float | None = None` to `_coordinate_reaped_vm` and thread it through `_publish_reaped_failure` (`:2303`) and `_emit_reaped_event` (`:2361`), adding it to the event payload only when not `None` and formatting it RFC3339 to match every other timestamp in the taxonomy.

**Known limitation to document, not fix:** `_fenced_at` is process-local, so a pool-manager restart mid-retry loses the memo. It cannot be persisted to Redis — the scenario is Redis refusing writes.

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/fleet/ -v && make test-unit && make lint`

- [ ] **Step 5: Commit**

```bash
git add src/orcest/fleet/pool_manager.py tests/fleet/test_pool_manager_reap_gating.py
git commit -m "fix(fleet): freeze kill time at fence so reap events report real runtime"
```

---

## Task 9: Update the documentation this changes

**[review] An earlier draft had no documentation phase while changing behaviour four documents describe explicitly.** `.claude/CLAUDE.md` even instructs: *"Update the Architecture bullet list above if the high-level description needs refreshing."*

**Files:**
- Modify: `.claude/CLAUDE.md`, `README.md`, `src/orcest/cli.py` (the `trace` docstring), `docs/monitor-exposure-runbook.md`, `docs/superpowers/specs/2026-08-17-stall-detection-and-monitor-design.md`

- [ ] **Step 1: Update `.claude/CLAUDE.md`**

The "Activity watchdog" bullet enumerates `_health_check`'s reap conditions verbatim. Add the three new gates (settled-writable Redis, `activity_reap_min_age`, correlation breaker) and note that a fenced VM bypasses them so its coordination retry still runs. Add output-stream bounding to the Architecture list — output streams are unmentioned there despite being what consumed 1.08 GB.

- [ ] **Step 2: Update `README.md`**

Same condition list at lines ~57-79; `watchdog.enabled: false` is no longer the only rollback lever. In the trace-archive sections (~182-184, ~613-625), state that output streams are trimmed once archived and that lines over 8 KB are clipped on disk with an `orcest_truncated` marker.

- [ ] **Step 3: Update the `trace` CLI docstring**

`src/orcest/cli.py:1529-1532` says *"Older traces that lived only in Redis are gone once their stream MAXLEN trimmed them out."* Now materially more aggressive (1000 entries, actively trimmed to a 200-entry tail). Reword.

- [ ] **Step 4: Update `docs/monitor-exposure-runbook.md`**

§7 "Watchdog rollout": add `require_redis_writable_for_reap: false` and `activity_reap_min_age: 0`
as rollback levers, noting they need `fleet update` plus a config re-render. Record that **every
pool-manager restart now suppresses `activity_stale` reaps for `redis_writable_settle_seconds`
(300 s)**, because `_writable_since` starts unset — expected, not a fault. §8 "Redis memory sizing": it reasons about the 1 GB budget without mentioning output streams, which is what actually filled it — add the new bounds and an `XINFO STREAM` check. Add the recovery note from spec §1: restarting orchestrator containers does **not** clear an OOM.

- [ ] **Step 5: Add a superseded-by note and commit**

`docs/superpowers/specs/2026-08-17-stall-detection-and-monitor-design.md:374` describes the archiver as *"XREAD after a cursor persisted in Redis"*. Add a one-line pointer to this spec.

```bash
git add .claude/CLAUDE.md README.md src/orcest/cli.py docs/
git commit -m "docs: record output-stream bounding and the new reap gates"
```

---

## Deployment

Three layers, all touched:

1. **Worker code** (`worker/loop.py`, Task 1) — push to public `origin/master`, then `orcest fleet rebake`.
2. **Container code** (`orchestrator/*`, `fleet/pool_manager.py`, Tasks 0, 2, 3, 5-8) — `orcest fleet update`.
3. **Host CLI + rendered config** (`fleet/config.py`, Task 4) — `pip install` on the Proxmox host **and** a config re-render: `upload_fleet_config` copies `/etc/orcest/config.yaml` as-is, so the new keys only appear after `save_config` runs. The loader also ships inside the pool-manager image.

Run fleet commands **from the release source directory**.

Deploy order is not constrained: new workers under an old non-trimming archiver is today's behaviour at a tighter cap, and a new trimming archiver against old 20000-cap workers is also fine. Both mid-deploy config directions are safe — the loader uses `pl.get(...)` defaults and rejects no unknown keys.

Rollback levers: `require_redis_writable_for_reap: false`, `activity_reap_min_age: 0`, `activity_reap_max_per_window: <large>` — all config, no code redeploy. Phase A has **no runtime lever**: `_MAX_OUTPUT_ENTRY_BYTES` is a module constant, so disabling truncation means a code change plus a full rebake. Consider rendering it clone-time later, as `PoolConfig.watchdog_enabled` already is.

Knowingly deferred: interactive/PTY workers still never write `workers:activity:*` (`loop.py:2434-2441` gates the tracker on `_BaseCliRunner`), so `_activity_record_is_stale` stays permanently true for the clauder fleet and their reap safety rests on heartbeat corroboration plus the new gates. Sufficient for this incident class; revisit if heartbeat corroboration is ever weakened.

## Verification after deploy

- `XINFO STREAM <project>:output:<worker>` — `entries-added` climbing while `length` sits near `_TRIM_TAIL_KEEP` proves Task 3 is trimming.
- `redis-cli INFO memory` — `used_memory` plateaus in the low tens of MB.
- Fresh trace: `md5sum` repeated segments; they must differ (Task 2).
- Restart an orchestrator container while Redis is near the ceiling; it must reach `trace_archiver.start()` (Task 0).
- One `stranded` error per affected stream **per container** (four total, not one), after a 5-minute dwell (Task 7).
- A reaped event's `elapsed_seconds` must match the task's real runtime, not the retry duration (Task 8).
