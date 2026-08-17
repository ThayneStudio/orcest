# Events Pipeline & Read-Only Monitor Implementation Plan (Plan A)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Orcest publishes CloudEvents-shaped lifecycle events to a spool stream; a relay delivers them to a new monitor container exposing a scoped, read-only query API (plus a private ingest listener) that external agents can consume.

**Architecture:** Producers (orchestrator, worker, pool manager) XADD envelopes to a capped `events` Redis stream, fire-and-forget. An `EventRelay` thread in the orchestrator (mirroring `TraceArchiver`) POSTs batches to the monitor's ingest listener with a persisted cursor and backoff. The monitor is a FastAPI app with two listeners: Docker-internal ingest (write token, idempotent insert) and public query (GET/HEAD-only, scoped bearer tokens, SQLite opened read-only, trace archive mounted read-only).

**Tech Stack:** Python 3.12, FastAPI + uvicorn (new optional extra `monitor`), SQLite (WAL), `requests` (already a dep) for the relay, `fakeredis` for tests.

**Spec:** `docs/superpowers/specs/2026-08-17-stall-detection-and-monitor-design.md` (§8–§11, §13 step 1)

## Global Constraints

- Python 3.12+, type hints everywhere; `make lint` (ruff) and `make test-unit` must pass after every task.
- Event envelope uses CloudEvents 1.0 attribute names exactly: `id`, `source`, `type`, `subject`, `time`, `data`. Idempotency key is `(source, id)`.
- Event type strings are `net.orcest.` + suffix; the v1 taxonomy is locked and additive-only (spec §8 table).
- Events NEVER contain raw tool arguments, raw tool output, prompts, or credentials. Producers must not put `Task.token`, `Task.credential`, `Task.claude_token`, or `Task.prompt` into any event.
- Event emission must never raise into a producer's main path: swallow-and-log, matching the output-streaming pattern at `src/orcest/worker/loop.py:2349-2359`.
- The monitor query listener accepts only GET/HEAD (405 otherwise) and opens SQLite `mode=ro` + `PRAGMA query_only=1`.
- Scopes: `events:read` (events/timeline/work/fleet endpoints), `traces:read` (trace endpoint). Ingest uses a single write token. Token compare is timing-safe over sha256 (same pattern as `dashboard/server/auth.ts`).
- New runtime deps (fastapi, uvicorn) go ONLY in the `monitor` optional extra — the worker/orchestrator images must not require them.
- Spool stream name (unprefixed): `events`; MAXLEN default 50000. Relay cursor Redis key: `event_relay:cursor`.

---

### Task A1: Shared events module (envelope, taxonomy, publisher)

**Files:**
- Create: `src/orcest/shared/events.py`
- Test: `tests/shared/test_events.py`

**Interfaces:**
- Produces: `EVENT_TYPES: frozenset[str]`, `EVENTS_STREAM = "events"`, `DEFAULT_EVENTS_MAXLEN = 50000`, `make_event(event_type, *, source_project, task_id, repo, resource_type, resource_id, attempt, head_sha="", worker_id="", provider="", data=None) -> dict`, `class EventPublisher` with `publish(envelope: dict) -> None` (fire-and-forget) constructed as `EventPublisher(redis: RedisClient, maxlen: int = DEFAULT_EVENTS_MAXLEN)`.
- Consumes: `RedisClient.xadd_capped(stream, fields, maxlen)` (`src/orcest/shared/redis_client.py:229`).

- [ ] **Step 1: Write the failing tests**

```python
# tests/shared/test_events.py
import json
from datetime import datetime

import pytest

from orcest.shared.events import (
    DEFAULT_EVENTS_MAXLEN,
    EVENT_TYPES,
    EVENTS_STREAM,
    EventPublisher,
    make_event,
)


def test_taxonomy_is_locked_v1_set():
    suffixes = {
        "task.enqueued", "task.started", "task.bootstrap", "task.active",
        "task.waiting", "task.suspect", "task.stuck", "task.looping",
        "task.killed", "task.completed", "task.failed", "task.reaped",
        "task.activity", "fleet.pressure", "fleet.kill_limit",
    }
    assert EVENT_TYPES == frozenset("net.orcest." + s for s in suffixes)


def test_make_event_envelope_shape():
    env = make_event(
        "net.orcest.task.started",
        source_project="myproj",
        task_id="abc123",
        repo="owner/repo",
        resource_type="pr",
        resource_id=42,
        attempt=2,
        head_sha="deadbeef",
        worker_id="w1",
        provider="claude",
        data={"extra": 1},
    )
    assert set(env) == {"id", "source", "type", "subject", "time", "data"}
    assert env["source"] == "urn:orcest:myproj"
    assert env["type"] == "net.orcest.task.started"
    assert env["subject"] == "abc123"
    # RFC3339 UTC with trailing Z
    datetime.strptime(env["time"], "%Y-%m-%dT%H:%M:%SZ")
    assert env["data"]["work"] == {
        "repo": "owner/repo", "resource_type": "pr", "resource_id": 42,
    }
    assert env["data"]["attempt"] == 2
    assert env["data"]["head_sha"] == "deadbeef"
    assert env["data"]["worker_id"] == "w1"
    assert env["data"]["provider"] == "claude"
    assert env["data"]["extra"] == 1
    # unique ids
    env2 = make_event(
        "net.orcest.task.started", source_project="myproj", task_id="abc123",
        repo="owner/repo", resource_type="pr", resource_id=42, attempt=2,
    )
    assert env2["id"] != env["id"]


def test_make_event_rejects_unknown_type():
    with pytest.raises(ValueError):
        make_event(
            "net.orcest.task.exploded", source_project="p", task_id="t",
            repo="o/r", resource_type="pr", resource_id=1, attempt=0,
        )


class _FakeRedis:
    def __init__(self, fail=False):
        self.fail = fail
        self.calls = []

    def xadd_capped(self, stream, fields, maxlen):
        if self.fail:
            raise ConnectionError("redis down")
        self.calls.append((stream, fields, maxlen))
        return "1-1"


def test_publisher_xadds_json_envelope():
    r = _FakeRedis()
    pub = EventPublisher(r)  # type: ignore[arg-type]
    env = make_event(
        "net.orcest.task.completed", source_project="p", task_id="t",
        repo="o/r", resource_type="pr", resource_id=1, attempt=0,
    )
    pub.publish(env)
    stream, fields, maxlen = r.calls[0]
    assert stream == EVENTS_STREAM
    assert maxlen == DEFAULT_EVENTS_MAXLEN
    assert json.loads(fields["envelope"])["type"] == "net.orcest.task.completed"


def test_publisher_swallows_redis_errors():
    pub = EventPublisher(_FakeRedis(fail=True))  # type: ignore[arg-type]
    env = make_event(
        "net.orcest.task.completed", source_project="p", task_id="t",
        repo="o/r", resource_type="pr", resource_id=1, attempt=0,
    )
    pub.publish(env)  # must not raise
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/shared/test_events.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'orcest.shared.events'`

- [ ] **Step 3: Implement `src/orcest/shared/events.py`**

```python
"""CloudEvents-shaped orcest event envelopes and the spool publisher.

Spec: docs/superpowers/specs/2026-08-17-stall-detection-and-monitor-design.md §8-§9.
The taxonomy is locked (additive-only after v1). Envelope field names follow
CloudEvents 1.0 so ``(source, id)`` is the end-to-end idempotency key.

Events must never carry raw tool arguments/output, prompts, or credentials —
only names, hashes, error classes, and counters (redaction rule, spec §8).
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from orcest.shared.redis_client import RedisClient

logger = logging.getLogger(__name__)

EVENTS_STREAM = "events"
DEFAULT_EVENTS_MAXLEN = 50000

_TYPE_SUFFIXES = (
    "task.enqueued", "task.started", "task.bootstrap", "task.active",
    "task.waiting", "task.suspect", "task.stuck", "task.looping",
    "task.killed", "task.completed", "task.failed", "task.reaped",
    "task.activity", "fleet.pressure", "fleet.kill_limit",
)
EVENT_TYPES: frozenset[str] = frozenset("net.orcest." + s for s in _TYPE_SUFFIXES)


def _now_rfc3339() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def make_event(
    event_type: str,
    *,
    source_project: str,
    task_id: str,
    repo: str,
    resource_type: str,
    resource_id: int,
    attempt: int,
    head_sha: str = "",
    worker_id: str = "",
    provider: str = "",
    data: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a v1 envelope. ``data`` extras are merged after identity fields."""
    if event_type not in EVENT_TYPES:
        raise ValueError(f"unknown event type: {event_type}")
    payload: dict[str, Any] = {
        "work": {
            "repo": repo,
            "resource_type": resource_type,
            "resource_id": resource_id,
        },
        "attempt": attempt,
        "head_sha": head_sha,
        "worker_id": worker_id,
        "provider": provider,
    }
    if data:
        payload.update(data)
    return {
        "id": str(uuid.uuid4()),
        "source": f"urn:orcest:{source_project}",
        "type": event_type,
        "subject": task_id,
        "time": _now_rfc3339(),
        "data": payload,
    }


class EventPublisher:
    """Fire-and-forget spool writer. Never raises into the caller."""

    def __init__(self, redis: RedisClient, maxlen: int = DEFAULT_EVENTS_MAXLEN):
        self._redis = redis
        self._maxlen = maxlen
        self._error_count = 0

    def publish(self, envelope: dict[str, Any]) -> None:
        try:
            self._redis.xadd_capped(
                EVENTS_STREAM, {"envelope": json.dumps(envelope)}, self._maxlen
            )
        except Exception:
            # Decimated logging, mirroring worker output-stream error handling.
            self._error_count += 1
            if self._error_count in (1, 10, 100) or self._error_count % 1000 == 0:
                logger.warning(
                    "event publish failed (%d failures so far)",
                    self._error_count,
                    exc_info=True,
                )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/shared/test_events.py -v`
Expected: 5 passed

- [ ] **Step 5: Lint and commit**

```bash
make lint && git add src/orcest/shared/events.py tests/shared/test_events.py && git commit -m "feat: add CloudEvents-shaped event envelope, taxonomy, and spool publisher"
```

---

### Task A2: `Task.attempt` field + orchestrator stamping + `task.enqueued`

**Files:**
- Modify: `src/orcest/shared/models.py` (Task dataclass ~line 103, `to_dict` ~137, `from_dict` ~180, `create` ~236)
- Modify: `src/orcest/orchestrator/task_publisher.py` (the four `Task.create(` sites at lines 611/702/779/852 and the two `stream_redis.xadd(tasks_stream, ...)` sites at 499/938)
- Test: `tests/shared/test_models.py` (append), `tests/orchestrator/test_task_publisher_events.py` (new)

**Interfaces:**
- Consumes: `get_total_attempt_count(redis, repo, pr_number)` (`src/orcest/orchestrator/pr_ops.py:189`); `make_event`/`EventPublisher` from Task A1.
- Produces: `Task.attempt: int` (default 0, round-trips through `to_dict`/`from_dict`; missing key deserializes to 0). Publishers emit `net.orcest.task.enqueued` (with `data={"decision_reason": task.decision_reason, "task_type": task.type.value}`) immediately after each successful task xadd.

- [ ] **Step 1: Write failing model round-trip test**

```python
# append to tests/shared/test_models.py
def test_task_attempt_roundtrip_and_legacy_default():
    from datetime import datetime, timezone
    from orcest.shared.models import Task, TaskType

    task = Task.create(
        task_type=TaskType.FIX_PR, repo="o/r", token="t", prompt="p",
        resource_type="pr", resource_id=7, branch=None, attempt=3,
    )
    assert task.attempt == 3
    d = task.to_dict()
    assert d["attempt"] == "3"
    assert Task.from_dict(d).attempt == 3
    # legacy payload without the key
    del d["attempt"]
    assert Task.from_dict(d).attempt == 0
```

Note: mirror the existing `Task.create` keyword usage already present in `tests/shared/test_models.py` — if the factory's required kwargs differ (check the top of that file for an existing `Task.create(` call and copy its required arguments), adjust the call accordingly; the assertion targets are only `attempt`.

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/shared/test_models.py -k attempt -v`
Expected: FAIL with `TypeError: ... unexpected keyword argument 'attempt'`

- [ ] **Step 3: Implement the model change**

In `src/orcest/shared/models.py`:
- Add field to the `Task` dataclass (after `provider_account: str = ""`): `attempt: int = 0  # orchestrator cross-SHA total-attempt counter at enqueue (spec §8)`
- In `to_dict`, add `"attempt": str(self.attempt),`
- In `from_dict`, add `attempt=int(data.get("attempt", "0") or "0"),`
- In `create`, add keyword param `attempt: int = 0` and pass `attempt=attempt` to the constructor.

- [ ] **Step 4: Run model tests**

Run: `pytest tests/shared/test_models.py -v`
Expected: all pass (existing tests unaffected — new field defaults everywhere)

- [ ] **Step 5: Write failing publisher test**

```python
# tests/orchestrator/test_task_publisher_events.py
import json

from orcest.shared.events import EVENTS_STREAM


def test_enqueued_event_emitted_after_publish(monkeypatch):
    """publish_fix_task emits net.orcest.task.enqueued to the events spool.

    Build this test on the existing publish-path fixtures: open
    tests/orchestrator/ and find the existing test module covering
    publish_fix_task (grep for 'publish_fix_task'); copy its
    fixture/arrange section verbatim (fake redis, pr_state, config), then
    after invoking publish_fix_task assert on the spool:
    """
    # ... arrange copied from existing publish_fix_task test ...
    # act: publish_fix_task(...)
    # assert:
    entries = fake_redis.xrevrange(EVENTS_STREAM, count=10)
    envs = [json.loads(fields["envelope"]) for _id, fields in entries]
    enq = [e for e in envs if e["type"] == "net.orcest.task.enqueued"]
    assert len(enq) == 1
    assert enq[0]["data"]["work"]["resource_id"] == pr_state.number
    assert enq[0]["data"]["attempt"] == 0
    assert enq[0]["data"]["decision_reason"] != ""
    # secrets must not leak
    assert "token" not in json.dumps(enq[0])
```

(The docstring instruction is part of the test-writing step: locate the existing `publish_fix_task` test, reuse its arrange block, and complete this test with real names. The assert block above is the required contract.)

- [ ] **Step 6: Run test to verify it fails**

Run: `pytest tests/orchestrator/test_task_publisher_events.py -v`
Expected: FAIL — no `task.enqueued` entry in spool

- [ ] **Step 7: Implement stamping + emission in `task_publisher.py`**

- Import: `from orcest.orchestrator.pr_ops import get_total_attempt_count` (already imported names exist in this module — extend the existing import), `from orcest.shared.events import EventPublisher, make_event`.
- In each of the four `publish_*_task` functions, when constructing the task pass `attempt=get_total_attempt_count(redis, repo, <resource-number>)` (for issue tasks use the issue number as the counter key — the helper is keyed on `(repo, number)` and works for both).
- Add a module-level helper and call it immediately after each successful `stream_redis.xadd(tasks_stream, task.to_dict())` (both sites):

```python
def _emit_enqueued(redis: RedisClient, task: Task) -> None:
    """Spool a task.enqueued event. Never raises (EventPublisher swallows)."""
    EventPublisher(redis).publish(
        make_event(
            "net.orcest.task.enqueued",
            source_project=task.key_prefix or "default",
            task_id=task.id,
            repo=task.repo,
            resource_type=task.resource_type,
            resource_id=task.resource_id,
            attempt=task.attempt,
            head_sha=task.snapshot_head_sha,
            provider=task.provider,
            data={"decision_reason": task.decision_reason, "task_type": task.type.value},
        )
    )
```

The spool must live in the per-project `redis` (prefixed) client, NOT `stream_redis` (shared task-stream client) — the relay reads the project-prefixed `events` stream.

- [ ] **Step 8: Run the orchestrator test suite**

Run: `pytest tests/orchestrator/ -x -q`
Expected: all pass

- [ ] **Step 9: Commit**

```bash
make lint && git add -A src/orcest/shared/models.py src/orcest/orchestrator/task_publisher.py tests/ && git commit -m "feat: stamp Task.attempt from cross-SHA counter and emit task.enqueued"
```

---

### Task A3: Worker lifecycle events (`task.started` / `task.completed` / `task.failed`)

**Files:**
- Modify: `src/orcest/worker/loop.py` (around `publish_task_start` ~2272 and `publish_task_end` ~2251, and the result mapping ~2400-2422)
- Test: `tests/worker/test_worker_events.py` (new)

**Interfaces:**
- Consumes: `EventPublisher`, `make_event` (A1); `Task` fields incl. `attempt` (A2); the worker's existing per-project `RedisClient` used for `_task_output_stream` writes.
- Produces: on task pickup, `net.orcest.task.started`; on completion, exactly one of `net.orcest.task.completed` (result SUCCESS) or `net.orcest.task.failed` with `data={"status": status.value, "transient": bool, "summary_head": summary[:200]}`. `summary_head` is truncated summary text (safe: summaries are operator-facing already and never contain credentials by the existing result contract).

- [ ] **Step 1: Write failing test**

```python
# tests/worker/test_worker_events.py
import json

from orcest.shared.events import EVENTS_STREAM


def _spooled(fake_redis, type_suffix):
    entries = fake_redis.xrevrange(EVENTS_STREAM, count=50)
    envs = [json.loads(f["envelope"]) for _id, f in entries]
    return [e for e in envs if e["type"] == f"net.orcest.{type_suffix}"]


def test_started_and_completed_events(worker_harness):
    """Use the existing worker-loop test harness: tests/worker/ already has
    end-to-end tests that drive one task through the loop with fakeredis and
    a stubbed runner (grep tests/worker/ for publish_task_end usage or the
    fixture that runs a single task). Reuse that fixture as `worker_harness`
    (success path) and add these asserts after the task completes:
    """
    fake_redis = worker_harness.redis
    started = _spooled(fake_redis, "task.started")
    assert len(started) == 1
    assert started[0]["data"]["worker_id"] == worker_harness.worker_id
    completed = _spooled(fake_redis, "task.completed")
    assert len(completed) == 1
    assert completed[0]["subject"] == started[0]["subject"]
    assert _spooled(fake_redis, "task.failed") == []


def test_failed_event_on_failure(worker_harness_failing):
    fake_redis = worker_harness_failing.redis
    failed = _spooled(fake_redis, "task.failed")
    assert len(failed) == 1
    assert failed[0]["data"]["status"] == "failed"
    assert isinstance(failed[0]["data"]["transient"], bool)
```

(As in A2: the two harness fixtures are the existing single-task worker-loop fixtures in `tests/worker/` — success and failure variants; wire them in, keeping the assert contract exactly.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/worker/test_worker_events.py -v`
Expected: FAIL — no spool entries

- [ ] **Step 3: Implement emission in `worker/loop.py`**

Where `task_start` marker is written (`_publish_task_start`, ~loop.py:2272), also publish via a module-scoped publisher created once per task-processing call:

```python
event_publisher = EventPublisher(redis)

def _emit(event_type: str, data: dict[str, Any] | None = None) -> None:
    event_publisher.publish(
        make_event(
            event_type,
            source_project=task.key_prefix or "default",
            task_id=task.id,
            repo=task.repo,
            resource_type=task.resource_type,
            resource_id=task.resource_id,
            attempt=task.attempt,
            head_sha=task.snapshot_head_sha,
            worker_id=worker_id,
            provider=task.provider,
            data=data,
        )
    )
```

- After the `task_start` marker: `_emit("net.orcest.task.started")`.
- In `publish_task_end(status)` (~2251), after the marker XADD: emit `task.completed` when `status == ResultStatus.SUCCESS`, else `task.failed` with `{"status": status.value, "transient": summary.startswith(TRANSIENT_SUMMARY_PREFIX), "summary_head": summary[:200]}` (thread `summary` into `publish_task_end` from the result-mapping site; it is available where `publish_task_end` is called).

- [ ] **Step 4: Run tests**

Run: `pytest tests/worker/test_worker_events.py tests/worker/ -x -q`
Expected: all pass

- [ ] **Step 5: Commit**

```bash
make lint && git add src/orcest/worker/loop.py tests/worker/test_worker_events.py && git commit -m "feat: emit task.started/completed/failed events from worker loop"
```

---

### Task A4: Pool-manager `task.reaped` event

**Files:**
- Modify: `src/orcest/fleet/pool_manager.py` (`_publish_reaped_failure`, line 2089)
- Test: `tests/fleet/test_pool_manager_events.py` (new)

**Interfaces:**
- Consumes: A1 publisher; the reconstructed `Task` available in `_coordinate_reaped_vm` (line 1753).
- Produces: `net.orcest.task.reaped` with `data={"reason": "max_task_duration", "elapsed_seconds": <float>}` emitted alongside the transient-FAILED result publication. (Plan B extends `reason` values; this task hardcodes today's only reason.)

- [ ] **Step 1: Write failing test** — locate the existing `_publish_reaped_failure` / `_coordinate_reaped_vm` tests in `tests/fleet/` (grep `reaped`), reuse their arrange block, and assert:

```python
# tests/fleet/test_pool_manager_events.py (assert contract)
import json
from orcest.shared.events import EVENTS_STREAM

def test_reaped_event_emitted(reap_fixture):
    fake_redis = reap_fixture.project_redis
    entries = fake_redis.xrevrange(EVENTS_STREAM, count=10)
    envs = [json.loads(f["envelope"]) for _id, f in entries]
    reaped = [e for e in envs if e["type"] == "net.orcest.task.reaped"]
    assert len(reaped) == 1
    assert reaped[0]["data"]["reason"] == "max_task_duration"
    assert reaped[0]["data"]["worker_id"] == reap_fixture.worker_id
```

- [ ] **Step 2: Run to verify it fails** — `pytest tests/fleet/test_pool_manager_events.py -v` → FAIL
- [ ] **Step 3: Implement** — in `_publish_reaped_failure`, after the `TaskResult` publication, build the envelope with `make_event("net.orcest.task.reaped", ..., worker_id=<the reaped consumer id>, data={"reason": "max_task_duration", "elapsed_seconds": elapsed})` using the reconstructed `Task`'s identity fields, published via `EventPublisher(<project-prefixed redis client used for the result>)`.
- [ ] **Step 4: Run** — `pytest tests/fleet/ -x -q` → all pass
- [ ] **Step 5: Commit** — `make lint && git add -A src/orcest/fleet/pool_manager.py tests/fleet/ && git commit -m "feat: emit task.reaped event from pool reaper"`

---

### Task A5: Monitor config, database, and ingest app

**Files:**
- Create: `src/orcest/monitor/__init__.py` (empty), `src/orcest/monitor/config.py`, `src/orcest/monitor/db.py`, `src/orcest/monitor/ingest_app.py`
- Modify: `pyproject.toml` (add extra)
- Test: `tests/monitor/__init__.py` (empty), `tests/monitor/test_db.py`, `tests/monitor/test_ingest.py`

**Interfaces:**
- Produces:
  - `MonitorConfig` dataclass: `db_path: str`, `trace_archive_path: str | None`, `ingest_host: str = "0.0.0.0"`, `ingest_port: int = 9091`, `query_host: str = "0.0.0.0"`, `query_port: int = 9090`, `write_token: str`, `readers: list[Reader]`; `Reader` dataclass: `name: str`, `token: str`, `scopes: frozenset[str]`. `load_monitor_config(path: str) -> MonitorConfig` reads YAML where tokens are given as `token_env` names (resolved via `os.environ`, missing env → `ValueError`).
  - `db.open_rw(db_path) -> sqlite3.Connection` (WAL, creates schema), `db.open_ro(db_path) -> sqlite3.Connection` (`mode=ro` URI + `PRAGMA query_only=1`), `db.insert_events(conn, envelopes: list[dict]) -> int` (accepted count, `INSERT OR IGNORE` on `(source, id)`; malformed envelopes — missing required envelope key, type not in `EVENT_TYPES` — are skipped and counted in the return's complement).
  - `create_ingest_app(cfg: MonitorConfig) -> FastAPI` with `POST /ingest/v1/events` body `{"events": [envelope...]}` → `{"accepted": int, "skipped": int}`; 401 without/with-wrong `Authorization: Bearer <write_token>`.
- Schema (in `db.py`):

```sql
CREATE TABLE IF NOT EXISTS events (
  source TEXT NOT NULL,
  id TEXT NOT NULL,
  type TEXT NOT NULL,
  subject TEXT NOT NULL,           -- task_id
  time TEXT NOT NULL,              -- RFC3339
  repo TEXT NOT NULL DEFAULT '',
  resource_type TEXT NOT NULL DEFAULT '',
  resource_id INTEGER NOT NULL DEFAULT 0,
  attempt INTEGER NOT NULL DEFAULT 0,
  data TEXT NOT NULL,              -- full envelope JSON, verbatim
  ingested_at TEXT NOT NULL,
  PRIMARY KEY (source, id)
);
CREATE INDEX IF NOT EXISTS idx_events_type_time ON events(type, time);
CREATE INDEX IF NOT EXISTS idx_events_task ON events(subject, time);
CREATE INDEX IF NOT EXISTS idx_events_work ON events(repo, resource_type, resource_id, time);
```

- [ ] **Step 1: Add the `monitor` extra to `pyproject.toml`**

```toml
[project.optional-dependencies]
monitor = [
    "fastapi>=0.110",
    "uvicorn>=0.29",
]
```

and append to the existing `dev` extra: `"fastapi>=0.110", "uvicorn>=0.29", "httpx>=0.27",` (httpx is FastAPI's TestClient transport). Run `pip install -e ".[dev]"`.

- [ ] **Step 2: Write failing db tests**

```python
# tests/monitor/test_db.py
import sqlite3

import pytest

from orcest.monitor import db
from orcest.shared.events import make_event


def _env(task_id="t1", type_="net.orcest.task.started"):
    return make_event(
        type_, source_project="p", task_id=task_id, repo="o/r",
        resource_type="pr", resource_id=5, attempt=1,
    )


def test_insert_is_idempotent(tmp_path):
    conn = db.open_rw(str(tmp_path / "m.db"))
    e = _env()
    assert db.insert_events(conn, [e]) == 1
    assert db.insert_events(conn, [e]) == 0  # duplicate (source,id) ignored
    rows = conn.execute("SELECT subject, repo, resource_id FROM events").fetchall()
    assert rows == [("t1", "o/r", 5)]


def test_malformed_envelopes_skipped(tmp_path):
    conn = db.open_rw(str(tmp_path / "m.db"))
    bad_type = _env(); bad_type["type"] = "net.orcest.task.exploded"
    missing = {"id": "x", "source": "urn:orcest:p"}
    assert db.insert_events(conn, [bad_type, missing, _env("t2")]) == 1


def test_ro_connection_rejects_writes(tmp_path):
    path = str(tmp_path / "m.db")
    db.open_rw(path).close()
    ro = db.open_ro(path)
    with pytest.raises(sqlite3.OperationalError):
        ro.execute("INSERT INTO events(source,id,type,subject,time,data,ingested_at) VALUES('a','b','c','d','e','f','g')")
```

- [ ] **Step 3: Run to verify failure** — `pytest tests/monitor/test_db.py -v` → ModuleNotFoundError

- [ ] **Step 4: Implement `config.py` and `db.py`**

`db.py` core (implement fully, including `open_rw` running the schema DDL and `PRAGMA journal_mode=WAL`):

```python
def open_ro(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, check_same_thread=False)
    conn.execute("PRAGMA query_only=1")
    return conn


_REQUIRED = ("id", "source", "type", "subject", "time", "data")


def insert_events(conn: sqlite3.Connection, envelopes: list[dict]) -> int:
    accepted = 0
    for env in envelopes:
        if not all(k in env for k in _REQUIRED) or env["type"] not in EVENT_TYPES:
            continue
        work = env["data"].get("work", {}) if isinstance(env["data"], dict) else {}
        cur = conn.execute(
            "INSERT OR IGNORE INTO events"
            " (source,id,type,subject,time,repo,resource_type,resource_id,attempt,data,ingested_at)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (
                env["source"], env["id"], env["type"], env["subject"], env["time"],
                work.get("repo", ""), work.get("resource_type", ""),
                int(work.get("resource_id", 0) or 0),
                int(env["data"].get("attempt", 0) or 0) if isinstance(env["data"], dict) else 0,
                json.dumps(env), datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            ),
        )
        accepted += cur.rowcount
    conn.commit()
    return accepted
```

`config.py`: plain dataclasses + `yaml.safe_load`; resolve every `token_env`/`write_token_env` through `os.environ` and raise `ValueError` naming the missing variable. Scopes validated against `{"events:read", "traces:read"}`.

- [ ] **Step 5: Run db tests** — `pytest tests/monitor/test_db.py -v` → pass

- [ ] **Step 6: Write failing ingest tests**

```python
# tests/monitor/test_ingest.py
from fastapi.testclient import TestClient

from orcest.monitor.config import MonitorConfig
from orcest.monitor.ingest_app import create_ingest_app
from orcest.shared.events import make_event


def _cfg(tmp_path):
    return MonitorConfig(
        db_path=str(tmp_path / "m.db"), trace_archive_path=None,
        write_token="write-secret", readers=[],
    )


def _env():
    return make_event(
        "net.orcest.task.started", source_project="p", task_id="t",
        repo="o/r", resource_type="pr", resource_id=1, attempt=0,
    )


def test_ingest_requires_write_token(tmp_path):
    client = TestClient(create_ingest_app(_cfg(tmp_path)))
    r = client.post("/ingest/v1/events", json={"events": [_env()]})
    assert r.status_code == 401
    r = client.post(
        "/ingest/v1/events", json={"events": [_env()]},
        headers={"Authorization": "Bearer wrong"},
    )
    assert r.status_code == 401


def test_ingest_accepts_and_dedupes(tmp_path):
    client = TestClient(create_ingest_app(_cfg(tmp_path)))
    env = _env()
    h = {"Authorization": "Bearer write-secret"}
    r = client.post("/ingest/v1/events", json={"events": [env]}, headers=h)
    assert r.json() == {"accepted": 1, "skipped": 0}
    r = client.post("/ingest/v1/events", json={"events": [env]}, headers=h)
    assert r.json() == {"accepted": 0, "skipped": 1}
```

- [ ] **Step 7: Run to verify failure**, then **Step 8: implement `ingest_app.py`**: FastAPI app factory; bearer extraction; timing-safe compare (`hmac.compare_digest` over `hashlib.sha256(token.encode()).digest()`); rw connection opened at app startup and reused; `skipped = len(events) - accepted`.

- [ ] **Step 9: Run** — `pytest tests/monitor/ -v` → pass. **Step 10: Commit**

```bash
make lint && git add -A pyproject.toml src/orcest/monitor tests/monitor && git commit -m "feat: monitor config, event store, and private ingest listener"
```

---

### Task A6: Monitor query app — auth scopes, method gate, events/timeline/work/fleet endpoints

**Files:**
- Create: `src/orcest/monitor/query_app.py`, `src/orcest/monitor/auth.py`
- Test: `tests/monitor/test_query.py`

**Interfaces:**
- Consumes: `db.open_ro`, `MonitorConfig`/`Reader` (A5).
- Produces: `create_query_app(cfg) -> FastAPI` with:
  - `GET /api/v1/health` → `{"ok": true}` unauthenticated
  - `GET /api/v1/events?type=&repo=&resource_id=&since=&limit=` (limit default 100, max 1000; `since` RFC3339 compared lexically against `time` — valid because both are zero-padded UTC `%Y-%m-%dT%H:%M:%SZ`) → `{"events": [envelope...]}` newest-first
  - `GET /api/v1/tasks/{task_id}/timeline` → `{"task_id", "events": [envelope...]}` time-ascending
  - `GET /api/v1/work/{repo:path}/{resource_type}/{resource_id}` → `{"work": {...}, "attempts": [{"attempt": n, "task_ids": [...], "head_shas": [...], "first_time", "last_time", "last_type"}]}` grouped by `attempt` ascending
  - `GET /api/v1/fleet` → `{"pressure": <latest fleet.pressure envelope or null>, "kill_limit": <latest fleet.kill_limit envelope or null>, "active_tasks": [{"task_id", "last_type", "time"}]}` where active_tasks = tasks whose latest event is not terminal (`task.completed`/`task.failed`/`task.killed`/`task.reaped`)
  - `auth.py`: `resolve_reader(cfg, authorization_header) -> Reader | None` (timing-safe); dependency raising 401 (no/unknown token) / 403 (missing scope)
  - Non-GET/HEAD → 405 via middleware, before auth.

- [ ] **Step 1: Write failing tests**

```python
# tests/monitor/test_query.py
from fastapi.testclient import TestClient

from orcest.monitor import db
from orcest.monitor.config import MonitorConfig, Reader
from orcest.monitor.query_app import create_query_app
from orcest.shared.events import make_event


def _seed(tmp_path):
    path = str(tmp_path / "m.db")
    conn = db.open_rw(path)
    envs = []
    for attempt, task_id in [(0, "t-a"), (1, "t-b")]:
        for suffix in ["task.enqueued", "task.started", "task.failed"]:
            envs.append(make_event(
                f"net.orcest.{suffix}", source_project="p", task_id=task_id,
                repo="o/r", resource_type="pr", resource_id=9, attempt=attempt,
            ))
    envs.append(make_event(
        "net.orcest.task.started", source_project="p", task_id="t-live",
        repo="o/r", resource_type="pr", resource_id=10, attempt=0,
    ))
    db.insert_events(conn, envs)
    return path


def _client(tmp_path, scopes=frozenset({"events:read"})):
    cfg = MonitorConfig(
        db_path=_seed(tmp_path), trace_archive_path=None, write_token="w",
        readers=[Reader(name="r", token="read-secret", scopes=scopes)],
    )
    return TestClient(create_query_app(cfg))


H = {"Authorization": "Bearer read-secret"}


def test_health_unauthenticated(tmp_path):
    assert _client(tmp_path).get("/api/v1/health").json() == {"ok": True}


def test_auth_required_and_method_gate(tmp_path):
    c = _client(tmp_path)
    assert c.get("/api/v1/events").status_code == 401
    assert c.post("/api/v1/events", headers=H).status_code == 405
    assert c.delete("/api/v1/events", headers=H).status_code == 405


def test_events_filtering(tmp_path):
    c = _client(tmp_path)
    r = c.get("/api/v1/events?type=net.orcest.task.failed", headers=H)
    assert r.status_code == 200
    assert {e["type"] for e in r.json()["events"]} == {"net.orcest.task.failed"}


def test_task_timeline_ascending(tmp_path):
    c = _client(tmp_path)
    evs = c.get("/api/v1/tasks/t-a/timeline", headers=H).json()["events"]
    assert [e["subject"] for e in evs] == ["t-a"] * 3
    assert evs[0]["type"] == "net.orcest.task.enqueued"


def test_work_grouping_by_attempt(tmp_path):
    c = _client(tmp_path)
    r = c.get("/api/v1/work/o/r/pr/9", headers=H).json()
    assert [a["attempt"] for a in r["attempts"]] == [0, 1]
    assert r["attempts"][0]["task_ids"] == ["t-a"]
    assert r["attempts"][1]["last_type"] == "net.orcest.task.failed"


def test_fleet_active_tasks(tmp_path):
    c = _client(tmp_path)
    r = c.get("/api/v1/fleet", headers=H).json()
    assert [t["task_id"] for t in r["active_tasks"]] == ["t-live"]
    assert r["pressure"] is None
```

- [ ] **Step 2: Run to verify failure** — `pytest tests/monitor/test_query.py -v` → ModuleNotFoundError
- [ ] **Step 3: Implement `auth.py` + `query_app.py`.** Method gate as pure ASGI middleware (registered first). Auth as a FastAPI dependency on an `APIRouter` covering everything except `/api/v1/health`. All SQL through the single ro connection; the `work` endpoint groups in Python from `SELECT ... WHERE repo=? AND resource_type=? AND resource_id=? ORDER BY time` (row counts are small; no premature SQL aggregation). `repo:path` converter handles the `owner/repo` slash. Active-task query:

```sql
SELECT subject, type, time FROM events e
WHERE type LIKE 'net.orcest.task.%'
  AND time = (SELECT MAX(time) FROM events WHERE subject = e.subject)
  AND type NOT IN ('net.orcest.task.completed','net.orcest.task.failed',
                   'net.orcest.task.killed','net.orcest.task.reaped')
GROUP BY subject
```

- [ ] **Step 4: Run** — `pytest tests/monitor/ -v` → pass. **Step 5: Commit**

```bash
make lint && git add -A src/orcest/monitor tests/monitor && git commit -m "feat: monitor read-only query API with scoped bearer auth"
```

---

### Task A7: Trace endpoint behind `traces:read`

**Files:**
- Modify: `src/orcest/monitor/query_app.py`
- Test: `tests/monitor/test_trace_endpoint.py`

**Interfaces:**
- Consumes: trace archive layout from `src/orcest/orchestrator/trace_archiver.py`: files at `{root}/{project}/YYYY/MM/DD/{task_id}.jsonl`, index pointer file `{root}/index/by-task-id/{task_id[:2]}/{task_id}` whose content is the path of the trace file relative to root; task_id validated against `_TASK_ID_RE` (`\A[A-Za-z0-9][A-Za-z0-9_-]{0,127}\Z`).
- Produces: `GET /api/v1/tasks/{task_id}/trace?tail=N` (tail default 200, max 5000) → `{"task_id", "lines": [str...]}`; 403 without `traces:read`; 404 when task unknown or archive disabled; 400 on task_id failing the allowlist regex; resolved path must stay under the archive root (reject traversal).

- [ ] **Step 1: Write failing tests**

```python
# tests/monitor/test_trace_endpoint.py
from fastapi.testclient import TestClient

from orcest.monitor import db
from orcest.monitor.config import MonitorConfig, Reader
from orcest.monitor.query_app import create_query_app


def _client(tmp_path, scopes):
    archive = tmp_path / "traces"
    trace = archive / "proj" / "2026" / "08" / "17" / "task1.jsonl"
    trace.parent.mkdir(parents=True)
    trace.write_text("".join(f'{{"line": {i}}}\n' for i in range(10)))
    ptr = archive / "index" / "by-task-id" / "ta" / "task1"
    ptr.parent.mkdir(parents=True)
    ptr.write_text("proj/2026/08/17/task1.jsonl")
    dbp = str(tmp_path / "m.db"); db.open_rw(dbp).close()
    cfg = MonitorConfig(
        db_path=dbp, trace_archive_path=str(archive), write_token="w",
        readers=[Reader(name="r", token="tok", scopes=scopes)],
    )
    return TestClient(create_query_app(cfg))


H = {"Authorization": "Bearer tok"}


def test_scope_enforced(tmp_path):
    c = _client(tmp_path, frozenset({"events:read"}))
    assert c.get("/api/v1/tasks/task1/trace", headers=H).status_code == 403


def test_tail_returns_last_lines(tmp_path):
    c = _client(tmp_path, frozenset({"events:read", "traces:read"}))
    r = c.get("/api/v1/tasks/task1/trace?tail=3", headers=H)
    assert r.status_code == 200
    assert r.json()["lines"] == ['{"line": 7}', '{"line": 8}', '{"line": 9}']


def test_bad_task_id_rejected(tmp_path):
    c = _client(tmp_path, frozenset({"events:read", "traces:read"}))
    assert c.get("/api/v1/tasks/..%2F..%2Fetc/trace", headers=H).status_code == 400


def test_unknown_task_404(tmp_path):
    c = _client(tmp_path, frozenset({"events:read", "traces:read"}))
    assert c.get("/api/v1/tasks/nosuchtask/trace", headers=H).status_code == 404
```

- [ ] **Step 2: Run to verify failure**, **Step 3: Implement**: validate task_id against the same regex (copy the pattern; add a comment naming its origin `trace_archiver._TASK_ID_RE`), read pointer, `resolved = (root / pointer_content).resolve()`, require `resolved.is_relative_to(root.resolve())`, tail with a bounded read (`collections.deque(f, maxlen=tail)`).
- [ ] **Step 4: Run** — pass. **Step 5: Commit** — `git commit -m "feat: monitor trace endpoint behind traces:read scope"`

---

### Task A8: Dual-listener entrypoint, `orcest monitor` CLI, OpenAPI taxonomy contract

**Files:**
- Create: `src/orcest/monitor/service.py`
- Modify: `src/orcest/cli.py` (add command to the `main` group)
- Test: `tests/monitor/test_service.py`, `tests/monitor/test_openapi_contract.py`

**Interfaces:**
- Produces: `run_monitor(cfg: MonitorConfig) -> None` — starts two `uvicorn.Server` instances (ingest on `ingest_host:ingest_port`, query on `query_host:query_port`) in threads and blocks until SIGTERM/SIGINT; CLI `orcest monitor --config <path>`; query app OpenAPI declares the taxonomy enum on the `type` query param of `/api/v1/events` (`Query(default=None, json_schema_extra={"enum": sorted(EVENT_TYPES)})`).

- [ ] **Step 1: Write failing contract test**

```python
# tests/monitor/test_openapi_contract.py
from fastapi.testclient import TestClient

from orcest.monitor import db
from orcest.monitor.config import MonitorConfig
from orcest.monitor.query_app import create_query_app
from orcest.shared.events import EVENT_TYPES


def test_openapi_type_enum_matches_taxonomy(tmp_path):
    dbp = str(tmp_path / "m.db"); db.open_rw(dbp).close()
    cfg = MonitorConfig(db_path=dbp, trace_archive_path=None, write_token="w", readers=[])
    spec = TestClient(create_query_app(cfg)).get("/api/v1/openapi.json").json()
    params = spec["paths"]["/api/v1/events"]["get"]["parameters"]
    type_param = next(p for p in params if p["name"] == "type")
    assert set(type_param["schema"]["enum"]) == EVENT_TYPES
```

- [ ] **Step 2: Run to verify failure**, **Step 3: Implement**: give `create_query_app` `openapi_url="/api/v1/openapi.json"` (exempt from auth alongside health — it is the consumer contract), add the enum to the param, write `service.py` (uvicorn servers with `install_signal_handlers=False` in threads; main thread waits on a `threading.Event` set by a signal handler), and register the Click command:

```python
@main.command("monitor")
@click.option("--config", "config_path", required=True, type=click.Path(exists=True))
def monitor_cmd(config_path: str) -> None:
    """Run the read-only monitor service (ingest + query listeners)."""
    from orcest.monitor.config import load_monitor_config
    from orcest.monitor.service import run_monitor

    run_monitor(load_monitor_config(config_path))
```

(Imports stay inside the command so the base CLI works without the `monitor` extra installed.)

- [ ] **Step 4: Write and pass a smoke test** (`test_service.py`): call `run_monitor` in a thread with port 0 is awkward with uvicorn — instead unit-test the piece that matters: `load_monitor_config` round-trip from a YAML fixture with `token_env` resolution (set env via `monkeypatch.setenv`), and that a missing env var raises `ValueError` naming it.
- [ ] **Step 5: Run all monitor tests + lint, commit** — `git commit -m "feat: monitor service entrypoint, CLI command, and OpenAPI taxonomy contract"`

---

### Task A9: Event relay (orchestrator → monitor ingest)

**Files:**
- Create: `src/orcest/orchestrator/event_relay.py`
- Modify: `src/orcest/shared/config.py` (OrchestratorConfig: add `monitor_ingest_url: str | None = None`, `monitor_write_token_env: str = "MONITOR_WRITE_TOKEN"`, `events_maxlen: int = 50000` — follow the existing dataclass/YAML parse pattern in that file), `src/orcest/orchestrator/loop.py` (start relay next to `TraceArchiver` construction at line 1417)
- Test: `tests/orchestrator/test_event_relay.py`

**Interfaces:**
- Consumes: `RedisClient.xread_after` (`redis_client.py:249`) over the prefixed `events` stream; `requests` (existing dependency).
- Produces: `class EventRelay` mirroring `TraceArchiver`'s lifecycle: `__init__(redis: RedisClient, ingest_url: str | None, write_token: str)`, `start()` (no-op with a log when `ingest_url` is None), `stop()`, background `_run()` loop: read up to 500 entries after cursor (cursor persisted at Redis key `event_relay:cursor`, starting at `"0-0"`), POST `{"events": [...]}` with `Authorization: Bearer <token>` and `timeout=10`, advance cursor ONLY on HTTP 2xx, exponential backoff 1s→60s on failure, 1s sleep between idle passes. Malformed spool entries (no/invalid `envelope` JSON) are skipped with a log and the cursor advances past them (they must not wedge the relay).

- [ ] **Step 1: Write failing tests**

```python
# tests/orchestrator/test_event_relay.py
import json

from orcest.orchestrator.event_relay import EventRelay
from orcest.shared.events import EVENTS_STREAM, make_event


class _FakeRedis:
    def __init__(self):
        self.entries = []  # list of (id, fields)
        self.kv = {}

    def xadd_capped(self, stream, fields, maxlen):
        eid = f"{len(self.entries)+1}-0"
        self.entries.append((eid, fields))
        return eid

    def xread_after(self, stream, cursor, count):
        return [(i, f) for i, f in self.entries if _id_gt(i, cursor)][:count]

    def get(self, key):
        return self.kv.get(key)

    def set(self, key, value):
        self.kv[key] = value


def _id_gt(a, b):
    pa = tuple(int(x) for x in a.split("-")); pb = tuple(int(x) for x in b.split("-"))
    return pa > pb


def _spool(r, n):
    for i in range(n):
        env = make_event(
            "net.orcest.task.started", source_project="p", task_id=f"t{i}",
            repo="o/r", resource_type="pr", resource_id=i, attempt=0,
        )
        r.xadd_capped(EVENTS_STREAM, {"envelope": json.dumps(env)}, 100)


def test_pass_posts_batch_and_advances_cursor(monkeypatch):
    r = _FakeRedis(); _spool(r, 3)
    posted = []

    def fake_post(url, json=None, headers=None, timeout=None):
        posted.append(json)
        class R: status_code = 200
        return R()

    relay = EventRelay(r, "http://monitor:9091/ingest/v1/events", "tok")
    monkeypatch.setattr("orcest.orchestrator.event_relay.requests.post", fake_post)
    relay._pass_once()
    assert len(posted[0]["events"]) == 3
    assert r.kv["event_relay:cursor"] == "3-0"
    relay._pass_once()
    assert len(posted) == 1  # nothing new -> no POST


def test_cursor_holds_on_http_failure(monkeypatch):
    r = _FakeRedis(); _spool(r, 2)

    def fail_post(url, json=None, headers=None, timeout=None):
        class R: status_code = 503
        return R()

    relay = EventRelay(r, "http://monitor:9091/ingest/v1/events", "tok")
    monkeypatch.setattr("orcest.orchestrator.event_relay.requests.post", fail_post)
    relay._pass_once()
    assert r.kv.get("event_relay:cursor") is None  # not advanced


def test_malformed_entry_skipped(monkeypatch):
    r = _FakeRedis()
    r.xadd_capped(EVENTS_STREAM, {"envelope": "not json"}, 100)
    _spool(r, 1)
    posted = []

    def fake_post(url, json=None, headers=None, timeout=None):
        posted.append(json)
        class R: status_code = 200
        return R()

    relay = EventRelay(r, "http://monitor:9091/ingest/v1/events", "tok")
    monkeypatch.setattr("orcest.orchestrator.event_relay.requests.post", fake_post)
    relay._pass_once()
    assert len(posted[0]["events"]) == 1
    assert r.kv["event_relay:cursor"] == "2-0"
```

- [ ] **Step 2: Run to verify failure**, **Step 3: Implement** `event_relay.py`: structure copied from `TraceArchiver` (`start`/`stop`/`_run` daemon thread); factor the single pass into `_pass_once()` exactly as the tests drive it so the thread loop is `while not stopping: _pass_once(); sleep`. Backoff state lives on the instance and resets on any 2xx.
- [ ] **Step 4: Config plumbing**: add the three `OrchestratorConfig` fields following the file's existing pattern (dataclass field + YAML key parse + example in `config/orchestrator.example.yaml`); in `orchestrator/loop.py` construct/start next to `TraceArchiver` (line 1417): `EventRelay(redis, config.monitor_ingest_url, os.environ.get(config.monitor_write_token_env, ""))`, and stop it where the archiver is stopped.
- [ ] **Step 5: Run** — `pytest tests/orchestrator/test_event_relay.py tests/shared/ -q` and `make test-unit` → pass. **Step 6: Commit** — `git commit -m "feat: event relay delivering the spool to the monitor ingest listener"`

---

### Task A10: Container, compose, config examples, runbook, docs

**Files:**
- Create: `Dockerfile.monitor`, `docker-compose.monitor.yml`, `config/monitor.example.yaml`, `docs/monitor-exposure-runbook.md`
- Modify: `README.md` (component list), `.claude/CLAUDE.md` (Dashboard/Architecture notes), `config/orchestrator.example.yaml` (monitor block, if not done in A9)

**Interfaces:**
- Consumes: everything above. No code interfaces produced; deployment contract only.

- [ ] **Step 1: Write `Dockerfile.monitor`** (mirror the existing orchestrator Dockerfile's base/pattern — check the repo root Dockerfile first):

```dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir ".[monitor]"
USER nobody
ENTRYPOINT ["orcest", "monitor", "--config", "/etc/orcest/monitor.yaml"]
```

- [ ] **Step 2: Write `docker-compose.monitor.yml`** (follow `docker-compose.dashboard.yml` conventions — required env via `:?`, mem limit, loopback publish for the query port, internal-only ingest):

```yaml
services:
  monitor:
    build:
      context: .
      dockerfile: Dockerfile.monitor
    restart: unless-stopped
    environment:
      MONITOR_WRITE_TOKEN: "${MONITOR_WRITE_TOKEN:?set in .env}"
      MONITOR_TOKEN_ADMIN: "${MONITOR_TOKEN_ADMIN:?set in .env}"
    volumes:
      - ./config/monitor.yaml:/etc/orcest/monitor.yaml:ro
      - monitor-db:/var/lib/orcest-monitor
      - "${ORCEST_TRACE_ARCHIVE_HOST_PATH:-/mnt/truenas-logs/orcest-traces}:/traces:ro"
    ports:
      - "127.0.0.1:9090:9090"   # query — reach via SSH tunnel or cloudflared
    # ingest port 9091 is NOT published; producers reach it on the compose network
    networks: [orcest]
    mem_limit: 512m
volumes:
  monitor-db: {}
networks:
  orcest:
    external: true
```

Adjust the network name to the one the existing compose files actually declare (read `docker-compose.yml` first and reuse its network; per project memory, every env var each container needs must be listed explicitly for Compose passthrough).

- [ ] **Step 3: Write `config/monitor.example.yaml`**

```yaml
db_path: /var/lib/orcest-monitor/monitor.db
trace_archive_path: /traces
ingest_port: 9091
query_port: 9090
write_token_env: MONITOR_WRITE_TOKEN
readers:
  - name: admin
    token_env: MONITOR_TOKEN_ADMIN
    scopes: [events:read, traces:read]
  # - name: grok-watcher
  #   token_env: MONITOR_TOKEN_GROK
  #   scopes: [events:read]
```

- [ ] **Step 4: Write `docs/monitor-exposure-runbook.md`** — operator steps, in order: (1) mint tokens (`openssl rand -hex 32`) into the compose `.env`; (2) add the monitor block to each project's `orchestrator.yaml` (`monitor_ingest_url: http://monitor:9091/ingest/v1/events`) and pass `MONITOR_WRITE_TOKEN` through the orchestrator's compose env; (3) `docker compose -f docker-compose.monitor.yml up -d`; verify `curl -s localhost:9090/api/v1/health`; (4) external exposure: install `cloudflared` as a compose sidecar pointing at `monitor:9090`, create the tunnel + hostname, add a Cloudflare Access application with a Service Auth policy, mint one Access service token per consumer; note explicitly that the app-level bearer token is still required (Access is the perimeter, scopes are the authorization) and that ingest (9091) must never get a tunnel hostname; (5) verification commands with `CF-Access-Client-Id`/`CF-Access-Client-Secret` + `Authorization: Bearer` headers; (6) revocation: delete the reader from `monitor.yaml` + restart, and/or revoke the Access service token.

- [ ] **Step 5: Update `README.md` and `.claude/CLAUDE.md`** — add the monitor to the architecture lists: one bullet each for the events spool, relay, and monitor (two listeners, scopes, tunnel exposure), and a pointer to the spec + runbook.

- [ ] **Step 6: Build check + full test suite**

Run: `docker build -f Dockerfile.monitor -t orcest-monitor:dev . && make test`
Expected: image builds; full suite passes

- [ ] **Step 7: Commit**

```bash
git add -A && git commit -m "feat: monitor container, compose, config examples, and exposure runbook"
```

---

## Self-Review Notes

- Spec coverage (Plan A scope = spec §8, §9, §10, §11 monitor/orchestrator config, §13 step 1): envelope+taxonomy (A1), work identity + attempt (A2), producer lifecycle events (A2–A4), spool+relay+idempotent ingest (A1/A9/A5), two-listener monitor with scopes/method-gate/ro-DB (A5–A7), trace endpoint + scope split (A7), OpenAPI contract + CLI (A8), deployment + runbook + docs (A10). Watchdog-emitted event types (`task.bootstrap`…`task.activity`, `fleet.*`) are defined in the taxonomy now (A1) but only emitted by Plan B — intentional: the taxonomy is the locked contract.
- `task.waiting` reason, signal snapshots, pressure detection: Plan B (they require the watchdog).
- Deployment reminder for the executor: per project memory, code changes here need host CLI reinstall + `orcest fleet update` (containers); A3's worker change also needs a template rebake — but Plan A ships dark (events only), so rebake can ride along with Plan B's.
