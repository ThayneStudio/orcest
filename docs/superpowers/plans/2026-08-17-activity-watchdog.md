# Activity Watchdog & Confidence-Gated Kills Implementation Plan (Plan B)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace wall-clock-only kills with a multi-signal liveness ladder (BOOTSTRAP→ACTIVE→WAITING→SUSPECT→STUCK / LOOPING) that kills only on corroborated stall or repetition, raises wall-clock to a backstop ceiling, and makes the pool reaper activity-aware.

**Architecture:** A worker-side `LivenessTracker` samples four signals (stream liveness, process-tree CPU delta, workspace mtimes, repetition hashes) every 30s and walks a pure state-machine ladder. Kills verify death (D-state → VM-destroy escalation via a `needs_reap` flag in a global activity record the pool reaper now consults). Fleet gates (pressure flag + kill budget in global Redis keys) suppress escalation storms; an orchestrator-side detector emits `fleet.pressure`. Every transition is published through Plan A's event spool.

**Tech Stack:** Python 3.12, stdlib only for sampling (`/proc`), `fakeredis` for tests. Depends on Plan A being merged (`orcest.shared.events`, worker `_emit`, relay, monitor).

**Spec:** `docs/superpowers/specs/2026-08-17-stall-detection-and-monitor-design.md` (§4–§7, §8 fleet-pressure, §11–§13)

## Global Constraints

- Python 3.12+, type hints everywhere; `make lint` and `make test-unit` pass after every task.
- `watchdog.enabled: false` must reproduce today's behavior exactly (fixed wall-clock watchdog at `timeout`). This is the rollback lever.
- The ladder never kills on a single stale evaluation: STUCK and LOOPING each require the condition on two consecutive evaluations (spec §5 persistence).
- CEILING kills are exempt from the fleet kill budget and pressure gate.
- Events carry tool **names**, **hashes**, and **error classes** only — never raw args/output (spec §8 redaction rule).
- Global (cross-project, unprefixed) Redis keys introduced here: `workers:activity:{worker_id}` (hash, TTL 4×sample_interval), `orcest:fleet:pressure` (string, EX pressure_hold), `orcest:fleet:kill_budget:limit` (string), `orcest:fleet:kill_budget:{YYYYMMDDHH}` (counter, EX 7200).
- Config defaults (spec §11): sample_interval 30, startup_grace 600, idle_window 600, waiting_grace 1800, loop thresholds 4/3/6, `RunnerConfig.timeout` 21600, `pool.max_task_duration` 25200, `activity_stale_after` 300, pressure_min_tasks 3, pressure_window 600, pressure_hold 900, max_kills_per_hour 6.
- All new worker modules live flat under `src/orcest/worker/` (repo convention).

---

### Task B1: Configuration surface

**Files:**
- Modify: `src/orcest/shared/config.py` (`RunnerConfig`, ~line 69), `src/orcest/fleet/config.py` (`PoolConfig` ~line 208 and its parse ~line 612), `src/orcest/shared/config.py` `OrchestratorConfig` (fleet-health block), `config/worker.yaml`, `config/worker.example.yaml`, `config/orchestrator.example.yaml`
- Test: `tests/shared/test_config_watchdog.py`, extend existing fleet config tests

**Interfaces:**
- Produces: `WatchdogConfig` dataclass in `shared/config.py`:

```python
@dataclass
class WatchdogConfig:
    enabled: bool = True
    sample_interval: float = 30.0
    startup_grace: float = 600.0
    idle_window: float = 600.0
    waiting_grace: float = 1800.0
    loop_exact_threshold: int = 4
    loop_error_threshold: int = 3
    loop_pingpong_threshold: int = 6
```

`RunnerConfig.watchdog: WatchdogConfig` (default factory), `RunnerConfig.timeout` default 5400→21600. `PoolConfig.max_task_duration` default 7200→25200, new `PoolConfig.activity_stale_after: int = 300`. `OrchestratorConfig` new fields: `pressure_min_tasks: int = 3`, `pressure_window: int = 600`, `pressure_hold: int = 900`, `max_kills_per_hour: int = 6` (YAML block `fleet_health:`).

- [ ] **Step 1: Write failing tests** — YAML round-trip: a `worker.yaml` snippet with a `watchdog:` block parses into `WatchdogConfig` (and absent block → defaults with `enabled: True`); `runner.timeout` default is 21600; `PoolConfig` defaults 25200/300; orchestrator `fleet_health:` block parses. Follow the parse-test patterns already in `tests/shared/` / `tests/fleet/` for these config classes.

```python
# tests/shared/test_config_watchdog.py (contract)
def test_watchdog_defaults_when_absent(tmp_path):
    cfg = load_worker_config(_write_yaml(tmp_path, {"runner": {}}))
    assert cfg.runner.watchdog.enabled is True
    assert cfg.runner.watchdog.idle_window == 600.0
    assert cfg.runner.timeout == 21600

def test_watchdog_block_parses(tmp_path):
    cfg = load_worker_config(_write_yaml(tmp_path, {
        "runner": {"watchdog": {"enabled": False, "idle_window": 120}}}))
    assert cfg.runner.watchdog.enabled is False
    assert cfg.runner.watchdog.idle_window == 120.0
```

(`_write_yaml` and the loader name: copy from the existing worker-config tests in `tests/shared/` — the loader and its fixtures already exist there.)

- [ ] **Step 2: Run to verify failure**, **Step 3: Implement** all config fields following each file's existing dataclass+parse pattern; update `config/worker.yaml` (`timeout: 21600` + full `watchdog:` block with defaults spelled out) and both example YAMLs; update the `PoolConfig.max_task_duration` docstring invariant text (`fleet/config.py:203-207`) to the new numbers (25200 > 21600 + grace).
- [ ] **Step 4:** `pytest tests/shared/ tests/fleet/ -q` — fix any test asserting the old defaults (there is at least one around pending-TTL/checkpoint bounds; update its expected values to derive from the config constant rather than a literal).
- [ ] **Step 5: Commit** — `git commit -m "feat: watchdog/fleet-health config surface; raise wall-clock defaults to ceiling values"`

---

### Task B2: Raw Redis helpers for global keys

**Files:**
- Modify: `src/orcest/shared/redis_client.py`
- Test: `tests/shared/test_redis_client.py` (append)

**Interfaces:**
- Produces (add only those not already present — grep first; `hset_raw` at line 567 and `xadd_raw` at 360 already exist): `get_raw(fq_key) -> str | None`, `set_ex_raw(fq_key, value, ttl) -> None`, `incr_raw(fq_key) -> int`, `expire_raw(fq_key, ttl) -> None`, `hgetall_raw(fq_key) -> dict[str, str]`. Each is a thin passthrough to the underlying client with NO key-prefixing, mirroring the docstring style of `hset_raw`.

- [ ] **Step 1: Write failing tests** (fakeredis, same fixture style as the existing raw-method tests in `tests/shared/test_redis_client.py`): set/get round-trip with TTL visible via the underlying client's `ttl()`, incr twice → 2, `hgetall_raw` returns what `hset_raw` wrote, and a prefixed-client instance still does NOT prefix these (`client.get_raw("k")` reads literal key `"k"`).
- [ ] **Step 2–4:** Run failing → implement → run passing (`pytest tests/shared/test_redis_client.py -q`).
- [ ] **Step 5: Commit** — `git commit -m "feat: raw redis helpers for global fleet keys"`

---

### Task B3: Process-tree CPU and workspace samplers

**Files:**
- Create: `src/orcest/worker/liveness_signals.py`
- Test: `tests/worker/test_liveness_signals.py`

**Interfaces:**
- Produces:
  - `class ProcessTreeSampler: __init__(root_pid: int)`, `sample() -> float | None` — returns cumulative CPU seconds (`utime+stime` of every live descendant of root_pid found via `/proc/*/stat` ppid-walk, PLUS root's `cutime+cstime` which accumulates reaped children), `None` if root is gone. Also `state_of_tree() -> list[str]` returning the `/proc/[pid]/stat` state chars of live tree members (for D-state verification in B8).
  - `class WorkspaceSampler: __init__(root: Path, max_entries: int = 20000)`, `changed_since(ts: float) -> bool` — bounded os.walk; True on first mtime > ts; caps traversal at max_entries files (return False past cap only if nothing newer found; log once when cap hit — silent-cap rule from spec review).
- CPU idle policy (3 consecutive zero deltas) lives in the ladder, NOT here — samplers return raw facts.

- [ ] **Step 1: Write failing tests**

```python
# tests/worker/test_liveness_signals.py
import os
import subprocess
import time
from pathlib import Path

from orcest.worker.liveness_signals import ProcessTreeSampler, WorkspaceSampler


def test_cpu_sampler_sees_busy_child():
    # parent sh spawns a python child that burns CPU: tree sampling must see it
    proc = subprocess.Popen(
        ["sh", "-c", "python3 -c 'x=0\nwhile True: x+=1' & wait"],
        start_new_session=True,
    )
    try:
        s = ProcessTreeSampler(proc.pid)
        a = s.sample(); time.sleep(1.0); b = s.sample()
        assert a is not None and b is not None and b - a > 0.2
    finally:
        proc.kill(); proc.wait()


def test_cpu_sampler_idle_process_near_zero():
    proc = subprocess.Popen(["sleep", "30"], start_new_session=True)
    try:
        s = ProcessTreeSampler(proc.pid)
        a = s.sample(); time.sleep(0.5); b = s.sample()
        assert b - a < 0.05
    finally:
        proc.kill(); proc.wait()


def test_cpu_sampler_counts_reaped_children():
    # sh runs a short CPU burst child to completion; cutime must capture it
    proc = subprocess.Popen(
        ["sh", "-c", "python3 -c 'x=0\nfor i in range(10**7): x+=1'; sleep 30"],
        start_new_session=True,
    )
    try:
        s = ProcessTreeSampler(proc.pid)
        deadline = time.time() + 15
        while time.time() < deadline:
            v = s.sample()
            if v is not None and v > 0.1:
                break
            time.sleep(0.3)
        assert v is not None and v > 0.1
    finally:
        proc.kill(); proc.wait()


def test_cpu_sampler_gone_process_returns_none():
    proc = subprocess.Popen(["true"]); proc.wait()
    assert ProcessTreeSampler(proc.pid).sample() is None


def test_workspace_sampler(tmp_path):
    (tmp_path / "a.txt").write_text("x")
    ws = WorkspaceSampler(tmp_path)
    ts = time.time()
    time.sleep(0.05)
    assert ws.changed_since(ts) is False
    (tmp_path / "b.txt").write_text("y")
    assert ws.changed_since(ts) is True
```

- [ ] **Step 2: Run to verify failure**, **Step 3: Implement.** `/proc` walk: read `/proc/[pid]/stat`, parse field 4 (ppid), fields 14/15 (utime/stime), 16/17 (cutime/cstime), field 3 (state); handle the comm-with-spaces parsing by splitting after the last `)`. Ticks→seconds via `os.sysconf("SC_CLK_TCK")`. Build the descendant set with one pass over `/proc/[0-9]*/stat` collecting pid→ppid, then closure from root. Vanished-pid reads (`FileNotFoundError`/`ProcessLookupError`) are skipped.
- [ ] **Step 4: Run** — `pytest tests/worker/test_liveness_signals.py -v` → pass (these are real-process tests; keep each under pytest-timeout norms used in this repo).
- [ ] **Step 5: Commit** — `git commit -m "feat: process-tree CPU and workspace activity samplers"`

---

### Task B4: Stream liveness classifier

**Files:**
- Create: `src/orcest/worker/stream_liveness.py`
- Test: `tests/worker/test_stream_liveness.py`

**Interfaces:**
- Produces:

```python
@dataclass(frozen=True)
class StreamSignal:
    kind: str            # "progress" | "waiting" | "output"
    reason: str = ""     # waiting only: "api_retry" | "rate_limit"
    tool_name: str = ""  # progress only, when a tool_use block was parsed
    tool_args: dict | None = None   # parsed args (LOCAL use only — fed to hasher, never emitted)
    tool_error_class: str = ""      # when a tool_result with is_error was parsed

def classify_line(line: str) -> StreamSignal: ...
```

Classification rules:
- JSON with `type` in `{"content_block_delta", "content_block_start", "content_block_stop", "message_start"}` → `progress` (raw Claude API stream events).
- Claude Code message-level JSON: `{"type": "assistant", "message": {"content": [...]}}` → `progress`; each `tool_use` content block yields `tool_name`/`tool_args`; `{"type": "user"}` messages whose content has `tool_result` blocks with `is_error: true` → `progress` with `tool_error_class` = first line of the result content, truncated to 120 chars.
- `{"type": "system", "subtype": "api_retry"}` → `waiting`/`api_retry`. Lines matching the existing rate-limit signals (reuse the predicate exposed by `claude_runner._check_rate_limit_event` — import and delegate rather than duplicating its patterns) → `waiting`/`rate_limit`.
- Grok ACP: `{"method": "session/update"}` or `{"sessionUpdate": "agent_message_chunk"}` anywhere in the object → `progress`.
- Anything else (non-JSON, unknown JSON) → `output` (weak liveness: freshens S1 equally — spec §4 degradation clause; the strong/weak distinction is preserved only in the snapshot for triage).

- [ ] **Step 1: Write failing tests** — one test per rule above (10 lines each: literal JSON strings → assert kind/fields), including: multi-tool assistant message yields the first tool block, malformed JSON → `output`, and `tool_error_class` truncation.
- [ ] **Step 2–4:** Run failing → implement → `pytest tests/worker/test_stream_liveness.py -v` pass.
- [ ] **Step 5: Commit** — `git commit -m "feat: provider-tolerant stream liveness classifier"`

---

### Task B5: Repetition detector

**Files:**
- Create: `src/orcest/worker/repetition.py`
- Test: `tests/worker/test_repetition.py`

**Interfaces:**
- Produces:

```python
@dataclass(frozen=True)
class RepetitionVerdict:
    stream: str          # "exact" | "error_class" | "ping_pong"
    count: int
    hashes: tuple[str, ...]   # the offending normalized hashes (safe to emit)

class RepetitionDetector:
    def __init__(self, exact_threshold: int = 4, error_threshold: int = 3,
                 pingpong_threshold: int = 6): ...
    def observe_tool_call(self, name: str, args: dict | None) -> None: ...
    def observe_tool_error(self, name: str, error_class: str) -> None: ...
    def verdict(self) -> RepetitionVerdict | None: ...
    def recent_hashes(self, n: int = 20) -> list[dict]:  # [{"tool": name, "hash": h}] for task.activity
```

Normalization before hashing (spec §4 S4): serialize args with `json.dumps(..., sort_keys=True)`, then regex-strip volatile substrings: UUIDs, ISO timestamps, unix-epoch numbers ≥10 digits, hex runs ≥8 chars; hash = `sha256(...).hexdigest()[:16]` over `f"{name}:{normalized}"`. Error-class hash ignores args entirely: `sha256(f"{name}:{error_class}")`. A verdict clears when a **novel** exact hash arrives (ladder-reset rule). Ping-pong: last `2*pingpong_threshold` exact hashes alternate between exactly two values.

- [ ] **Step 1: Write failing tests**

```python
# tests/worker/test_repetition.py (contract; write all of these)
def test_exact_repeat_trips_at_threshold():      # 3 identical -> None; 4th -> verdict(stream="exact", count=4)
def test_timestamps_and_uuids_normalized():      # same call with different uuid/timestamp in args still counts as identical
def test_error_class_ignores_args():             # same tool+error, 3 different args -> verdict(stream="error_class")
def test_pingpong_alternation():                 # A,B,A,B,A,B,A,B,A,B,A,B (6 cycles) -> verdict(stream="ping_pong")
def test_novel_call_resets():                    # 3 identical, 1 novel, 3 identical -> None throughout
def test_verdict_hashes_contain_no_raw_args():   # verdict.hashes are 16-char hex strings
```

- [ ] **Step 2–4:** Run failing → implement → pass.
- [ ] **Step 5: Commit** — `git commit -m "feat: repetition detector with exact/error-class/ping-pong hash streams"`

---

### Task B6: Ladder state machine (pure)

**Files:**
- Create: `src/orcest/worker/liveness_ladder.py`
- Test: `tests/worker/test_liveness_ladder.py`

**Interfaces:**
- Consumes: `WatchdogConfig` (B1), `RepetitionVerdict` (B5).
- Produces:

```python
class LadderState(str, Enum):
    BOOTSTRAP = "bootstrap"; ACTIVE = "active"; WAITING = "waiting"
    SUSPECT = "suspect"; STUCK = "stuck"; LOOPING = "looping"

@dataclass(frozen=True)
class Decision:
    state: LadderState
    transitioned: bool                 # state changed this evaluation
    kill: str | None                   # None | "stuck" | "looping" | "ceiling"
    snapshot: dict                     # per-signal last-fresh ts, since, reason — event-safe

class LivenessLadder:
    def __init__(self, cfg: WatchdogConfig, ceiling: float, started_at: float): ...
    def note_stream(self, now: float, sig: StreamSignal) -> None: ...   # progress/output freshen S1; waiting sets waiting-mode+reason
    def evaluate(self, now: float, cpu_seconds: float | None,
                 workspace_changed: bool,
                 rep_verdict: RepetitionVerdict | None) -> Decision: ...
```

Semantics (pure, no I/O, no wall clock reads — `now` injected):
- BOOTSTRAP until first `progress`-kind stream signal or `started_at + startup_grace`.
- S1 fresh = last stream signal (progress/output) within `idle_window`. S2 idle = 3 consecutive evaluations with zero CPU delta. S3 fresh = workspace_changed on any of the evaluations inside the window (track last-changed ts).
- WAITING entered on a waiting signal; while `now - last_waiting_ts < waiting_grace` staleness cannot escalate past SUSPECT; snapshot carries `reason`.
- SUSPECT when S1 stale AND S2 idle AND S3 stale. STUCK when the SUSPECT condition has held for a further full `idle_window` (i.e. two consecutive stale windows). `kill="stuck"` only on the STUCK transition.
- LOOPING when `rep_verdict` is non-None on two consecutive evaluations; `kill="looping"` on the transition. Repetition is evaluated even when S1–S3 read active (that is its purpose).
- CEILING: `now - started_at >= ceiling` → `kill="ceiling"` regardless of state.
- Any fresh signal (or verdict clearing) from SUSPECT returns to ACTIVE (`transitioned=True`).

- [ ] **Step 1: Write failing tests** — pure fake-clock tests, one per rule; use `cfg = WatchdogConfig(sample_interval=30, startup_grace=100, idle_window=100, waiting_grace=200)` and step `now` manually:

```python
def test_bootstrap_exempt_until_first_progress_or_grace():
def test_active_survives_past_old_timeout_with_output():   # freshen S1 every 30s for 3000s: never SUSPECT
def test_all_stale_goes_suspect_not_kill():                 # kill is None on SUSPECT transition
def test_stuck_requires_second_stale_window_then_kills():
def test_single_fresh_signal_resets_suspect_to_active():    # cpu delta alone rescues
def test_waiting_blocks_escalation_within_grace():
def test_waiting_escalates_after_grace_expires():
def test_looping_requires_two_consecutive_verdicts():
def test_looping_fires_even_while_cpu_and_output_active():
def test_ceiling_kills_regardless_of_activity():
def test_snapshot_records_signal_ages_and_reason():
```

Each test drives `note_stream`/`evaluate` with explicit `now` values and asserts `Decision.state`/`kill`. Write them all concretely (they are the executable spec of §5).

- [ ] **Step 2–4:** Run failing → implement → `pytest tests/worker/test_liveness_ladder.py -v` all pass.
- [ ] **Step 5: Commit** — `git commit -m "feat: liveness ladder state machine with persistence and waiting/ceiling semantics"`

---

### Task B7: LivenessTracker glue (signals → ladder → events, activity record, fleet gates)

**Files:**
- Create: `src/orcest/worker/liveness_tracker.py`
- Test: `tests/worker/test_liveness_tracker.py`

**Interfaces:**
- Consumes: B2–B6; `EventPublisher`/`make_event` (Plan A); `RedisClient` raw helpers.
- Produces:

```python
class LivenessTracker:
    """Owns one task's liveness: feed lines in, get kill decisions out.

    Thread-safety: observe_line is called from the runner's stdout loop;
    tick() from the watchdog thread. Internal lock around ladder access.
    """
    def __init__(self, cfg: WatchdogConfig, ceiling: float, *,
                 redis: RedisClient, emit: Callable[[str, dict], None],
                 worker_id: str, root_pid: int, workspace: Path,
                 clock: Callable[[], float] = time.monotonic): ...
    def observe_line(self, line: str) -> None: ...
    def tick(self) -> str | None:   # returns kill trigger ("stuck"|"looping"|"ceiling") or None
    def tree_states(self) -> list[str]: ...   # passthrough to ProcessTreeSampler.state_of_tree (B8 verify-death)
    def mark_needs_reap(self) -> None: ...
    def close(self) -> None: ...    # delete activity record
```

Behavior per tick: sample CPU/workspace, `ladder.evaluate`, then:
1. On any `transitioned=True`: `emit("net.orcest.task.<state>", {"snapshot": decision.snapshot, **({"reason": ...} for waiting)})`.
2. Every 10th tick (~300s): `emit("net.orcest.task.activity", {"snapshot": ..., "recent_tool_hashes": detector.recent_hashes(20), "cpu_seconds": ..., })`.
3. Write activity record via `hset_raw`/`expire_raw`: key `workers:activity:{worker_id}`, fields `task_id`, `state`, `last_liveness_ts` (unix), `ladder_since`, `needs_reap` ("0"/"1"), TTL `int(4 * cfg.sample_interval)`.
4. Fleet gates before returning a `stuck`/`looping` kill (never for `ceiling`):
   - `get_raw("orcest:fleet:pressure")` set → suppress (stay SUSPECT/LOOPING, emit nothing extra — the suspect/looping events already fired), return None.
   - Budget: `limit = int(get_raw("orcest:fleet:kill_budget:limit") or "6")`; `n = incr_raw(f"orcest:fleet:kill_budget:{hour}")` + `expire_raw(..., 7200)` where `hour = time.strftime('%Y%m%d%H', time.gmtime())`; if `n > limit`: emit `net.orcest.fleet.kill_limit` (once per task) and return None. `limit <= 0` means kills disabled (observation phase).

- [ ] **Step 1: Write failing tests** using fakeredis + a recorded `emit` list + injected fake clock + monkeypatched samplers (patch `ProcessTreeSampler.sample`/`WorkspaceSampler.changed_since` on the instances):

```python
def test_transitions_emit_events_with_snapshot():
def test_activity_record_written_with_ttl_and_state():
def test_pressure_flag_suppresses_stuck_kill_but_suspect_event_still_emitted():
def test_kill_budget_zero_defers_kill_and_emits_kill_limit_once():
def test_budget_increments_and_allows_within_limit():
def test_ceiling_kill_bypasses_gates():
def test_close_deletes_activity_record():
```

- [ ] **Step 2–4:** Run failing → implement → pass (`pytest tests/worker/test_liveness_tracker.py -v`).
- [ ] **Step 5: Commit** — `git commit -m "feat: liveness tracker gluing signals, ladder, events, activity record, fleet gates"`

---

### Task B8: Runner integration (watchdog thread, kill + verify-death, ceiling)

**Files:**
- Modify: `src/orcest/worker/_runner_base.py` (`_run_cli_agent`, lines 239–520: watchdog block at 361–374, stdout loop wall-clock check at 415, timeout result at 481–496), `src/orcest/worker/loop.py` (construct tracker where the runner is invoked, ~2387; thread `_emit` from Task A3)
- Test: `tests/worker/test_runner_watchdog_integration.py`

**Interfaces:**
- Consumes: `LivenessTracker` (B7).
- Produces: `_run_cli_agent(..., tracker: LivenessTracker | None = None)`. With `tracker=None` or `watchdog.enabled=False` (loop passes None then): the existing fixed watchdog runs unchanged. With a tracker:
  - The fixed watchdog thread is replaced by a loop thread: `while not cancelled.wait(cfg.sample_interval): trigger = tracker.tick(); if trigger: killed_trigger = trigger; _kill_process_tree(proc); break`.
  - The stdout loop calls `tracker.observe_line(line)` (alongside `on_output`) and drops its inline wall-clock check (the tracker owns CEILING).
  - After a ladder kill: wait 2s, `states = tracker.tree_states()`; `verified = "D" not in states`; if not verified: `tracker.mark_needs_reap()`. Emit `net.orcest.task.killed` with `{"trigger": killed_trigger, "verified": verified}` (via the tracker's `emit`).
  - Results: trigger `stuck`/`looping` → `RunnerResult(success=False, transient=False, summary=f"STALLED({trigger}): " + json.dumps(snapshot_head))` where snapshot_head is the decision snapshot trimmed to ≤500 chars; trigger `ceiling` → today's `RunnerResult(success=False, summary=f"Timed out after {ceiling}s", transient=True)` including the existing Claude reclassification path (`_timeout_claude_result`) untouched.

- [ ] **Step 1: Write failing integration tests** with fake provider scripts (pattern: `tests/worker/` already runs `_run_cli_agent`-level tests with stub binaries — reuse that harness; use a tiny WatchdogConfig: `sample_interval=0.2, startup_grace=0.5, idle_window=1.0, waiting_grace=2.0`, ceiling=60):

```python
def test_productive_slow_task_survives_past_idle_window():
    # script prints a JSON progress line every 0.5s for 5s then exits 0
    # -> success, no kill events

def test_silent_hang_killed_as_stuck_with_snapshot():
    # script prints one line then sleeps 600 -> killed; result.transient is False
    # and summary startswith "STALLED(stuck)"; emitted events include
    # task.suspect then task.stuck then task.killed(verified=True)

def test_loop_killed_as_looping():
    # script prints the same assistant tool_use JSON line every 0.3s forever
    # -> killed with STALLED(looping) even though output is continuous

def test_waiting_script_not_killed_within_grace():
    # script prints {"type":"system","subtype":"api_retry"} then sleeps 1.5s
    # then prints progress and exits 0 -> success

def test_disabled_watchdog_preserves_wall_clock_timeout():
    # tracker=None, timeout=1: sleeping script killed at ~1s with
    # "Timed out after 1s", transient=True (existing behavior pinned)
```

- [ ] **Step 2: Run to verify failure**, **Step 3: Implement** the integration exactly as the Interfaces block describes. Keep the diff inside `_run_cli_agent` minimal: the abort-event/lock-lost path, auth-prompt path, stderr drain, and retry loop are untouched.
- [ ] **Step 4: Run** — `pytest tests/worker/test_runner_watchdog_integration.py tests/worker/ -q` → all pass (existing runner tests must pass unmodified except any that assert the old default timeout constant).
- [ ] **Step 5: Commit** — `git commit -m "feat: activity watchdog integrated into generic runner with verified kills"`

---

### Task B9: Consolidate `claude_runner.py` onto the base runner

**Files:**
- Modify: `src/orcest/worker/claude_runner.py` (duplicated loop: `run_claude` at 368, watchdog 568–585, per-line checks 631–670), `src/orcest/worker/_runner_base.py` (add hooks)
- Test: existing `tests/worker/` claude-runner suites (must pass unchanged); extend only where a hook needs direct cover

**Interfaces:**
- Produces: `_runner_base` grows two overridable `Runner` hooks so Claude specifics survive the consolidation: `classify_timeout(stdout_lines, stderr_lines, timeout) -> RunnerResult | None` (Claude implementation delegates to the logic in `_timeout_claude_result`, `claude_runner.py:242` — rate-limit/usage-exhausted reclassification) and `postprocess_result(result, stdout_lines, stderr_lines) -> RunnerResult` (Claude: existing summary/needs-human/usage parsing). `ClaudeRunner.run` calls the base `run` (which calls `_run_cli_agent`); `run_claude`'s duplicated subprocess loop is deleted. The module drift warning at `_runner_base.py:9-15` is removed.

- [ ] **Step 1: Pin current behavior** — run the existing claude-runner tests and note the passing set: `pytest tests/worker/ -k claude -q`. This is the acceptance bar; no new tests are written until a gap is found.
- [ ] **Step 2: Diff the two loops** — read `run_claude` (368–860) against `_run_cli_agent` (239–520) and list every behavioral difference into the commit message draft (known set: timeout reclassification via `_timeout_claude_result`; rate-limit event scanning during streaming; stream-json result extraction; interactive-auth nuances). Anything on that list must map to an existing base hook (`detect_auth_prompt`, `extract_credential_update`) or one of the two new hooks.
- [ ] **Step 3: Implement the hooks in `_runner_base.py`** — `classify_timeout` called at the timeout-result site (481–486) before constructing the generic timeout result; `postprocess_result` called in `_finish`. Default implementations return `None` / `result` (no behavior change for other providers).
- [ ] **Step 4: Port `ClaudeRunner`** — implement the two hooks from the diff list, make `run` delegate to base, delete the duplicated loop and its private helpers that no longer have callers (keep `_timeout_claude_result` etc. as the hook bodies).
- [ ] **Step 5: Run the full worker suite** — `pytest tests/worker/ -q` and `make test-unit`. Every pre-existing test passes. If a difference from Step 2 has no covering test, add one targeted test reproducing the old behavior through the new path before moving on.
- [ ] **Step 6: Commit** — `git commit -m "refactor: consolidate claude runner onto the generic runner loop via timeout/result hooks"`

---

### Task B10: Orchestrator fleet health (pressure detector + budget limit mirror)

**Files:**
- Create: `src/orcest/orchestrator/fleet_health.py`
- Modify: `src/orcest/orchestrator/loop.py` (start next to `TraceArchiver`/`EventRelay` at ~1417; at startup, mirror the limit: `set_raw`-style write of `str(config.max_kills_per_hour)` to `orcest:fleet:kill_budget:limit` — add `set_raw` in B2 if not already, else use `set_ex_raw` with a 7-day TTL refreshed each loop pass)
- Test: `tests/orchestrator/test_fleet_health.py`

**Interfaces:**
- Consumes: events spool (`xread_after`), raw helpers (B2), `make_event`/`EventPublisher`.
- Produces: `class FleetHealthMonitor` (TraceArchiver-style thread lifecycle) with `_pass_once()`: read new spool entries after cursor `fleet_health:cursor`; keep a deque of `(unix_ts, task_id)` for `task.suspect` envelopes; drop entries older than `pressure_window`; when distinct task_ids ≥ `pressure_min_tasks` AND the pressure key is unset: `set_ex_raw("orcest:fleet:pressure", "1", pressure_hold)` + emit `net.orcest.fleet.pressure` with `{"suspect_tasks": [ids], "window_seconds": w}` (identity fields: repo="" resource_id=0 — fleet events use the fleet pseudo-identity; `make_event` accepts empty repo). While the condition persists on later passes, refresh the key TTL; emit no duplicate event until the key has expired once.

- [ ] **Step 1: Write failing tests** (fakeredis; drive `_pass_once` directly with injected clock):

```python
def test_three_distinct_suspects_in_window_sets_pressure_and_emits_once():
def test_two_suspects_do_not_trip():
def test_old_suspects_age_out_of_window():
def test_key_refreshed_but_event_not_duplicated_while_held():
def test_limit_mirrored_at_startup():
```

- [ ] **Step 2–4:** Run failing → implement → pass.
- [ ] **Step 5: Commit** — `git commit -m "feat: fleet pressure detector and kill-budget limit mirror"`

---

### Task B11: Activity-aware pool reaper + final docs

**Files:**
- Modify: `src/orcest/fleet/pool_manager.py` (`_health_check`, 1223–1291), `.claude/CLAUDE.md`, `README.md`, memory of rollout in `docs/monitor-exposure-runbook.md` (append watchdog rollout section)
- Test: `tests/fleet/test_pool_manager.py` (extend the existing `_health_check` tests)

**Interfaces:**
- Consumes: `hgetall_raw` (B2); the worker-id-for-vmid derivation already used by `_coordinate_reaped_vm`/`_worker_heartbeat_present` (reuse that exact helper).
- Produces: `_health_check` destroy condition becomes (spec §6): destroy iff `elapsed > max_task_duration` (ceiling) OR activity record has `needs_reap == "1"` OR (activity record absent-or-stale — `now - float(last_liveness_ts) > activity_stale_after` or key missing — AND the consumer has pending entries). `task.reaped` events (Task A4) gain `reason` values: `"ceiling"`, `"needs_reap"`, `"activity_stale"` replacing the single hardcoded `"max_task_duration"`.

- [ ] **Step 1: Write failing tests** extending the existing `_health_check` fixture set:

```python
def test_active_record_blocks_destroy_below_ceiling():
    # elapsed 10000s (< 25200 ceiling), fresh workers:activity record -> NOT destroyed
def test_ceiling_destroys_despite_fresh_activity():
def test_needs_reap_flag_destroys_immediately():
def test_absent_record_with_pending_entries_destroys():
def test_absent_record_without_pending_entries_not_destroyed():
def test_reaped_event_reason_field():   # each path emits matching reason
```

- [ ] **Step 2–4:** Run failing → implement → `pytest tests/fleet/ -q` pass.
- [ ] **Step 5: Docs** — update `.claude/CLAUDE.md` (Architecture: liveness ladder + monitor bullets; note `watchdog.enabled` rollback lever) and append the rollout section to the runbook mirroring spec §13 steps 2–3: enable with `max_kills_per_hour: 0` (observation), watch `task.suspect` false-positive rate via the monitor for several days, then set the real budget. Note the three deploy layers (host CLI, `fleet update`, `fleet rebake`) — worker changes in this plan require the rebake.
- [ ] **Step 6: Full suite + commit**

```bash
make test && git add -A && git commit -m "feat: activity-aware pool reaper; watchdog rollout docs"
```

---

## Self-Review Notes

- Spec coverage (Plan B scope = §4–§7, §8 fleet events, §11–§13): signals S1–S4 (B3–B5), ladder incl. BOOTSTRAP/WAITING/persistence/ceiling (B6), kill verify + D-state escalation (B8 + B11 `needs_reap`), fleet gates + pressure + budget mirror (B7/B10), activity record + reaper (B7/B11), timeout migration + rollback lever (B1/B8), consolidation of the duplicated runner loop (B9), rollout docs (B11).
- `task.waiting` `reason` field: produced by B4 classifier → B6 snapshot → B7 emission — satisfies the review requirement without touching traces.
- Interactive runner (`claude_interactive_runner.py:431`) keeps its wall-clock behavior: it is user-facing, not fleet-facing; out of scope (spec silent — flagged here as a conscious exclusion).
- Order matters: B1–B7 are independent of Plan A only up to B7 (`EventPublisher`); execute Plan A first, or at minimum A1–A3.
