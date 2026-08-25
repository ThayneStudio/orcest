# Stall Detection & Read-Only Monitor — Design

**Date:** 2026-08-17
**Status:** Draft for review

## 1. Problem

Orcest kills runners on wall-clock time alone, in two independent layers:

- The runner watchdog (`src/orcest/worker/_runner_base.py:361`) computes its
  deadline once at spawn and never resets on activity. Deployed
  `RunnerConfig.timeout` is 1800s (`config/worker.yaml`); a runner streaming
  useful output every second dies at 1800s exactly like a hung one.
- The pool reaper (`src/orcest/fleet/pool_manager.py:1256`) force-destroys
  worker VMs at `pool.max_task_duration` (7200s) using a timestamp the pool
  manager stamped, consulting no heartbeat or activity signal.

A runner working a hard problem for hours is indistinguishable from a stuck
one. Conversely, a runner in a retry loop that logs, touches files, and burns
CPU every iteration *looks* alive to naive activity checks while making zero
progress.

Additionally, when a runner **is** stuck there is no way to triage why
(beyond `orcest trace`), and no way for external systems — e.g. Grok bots,
Claude agents — to observe orcest's health without being handed access to
the lab network.

### Goals

1. Kill a runner early **only** on corroborated evidence it is producing no
   useful work ("confidence-gated"); wall-clock becomes a raised backstop.
2. Attach machine-readable evidence (which signals went quiet, since when)
   to every early kill so the cause can be triaged and prevented.
3. Keep orcest lightweight: orcest tracks cheap signals and publishes
   events; interpretation and evidence-bundling are done by consumers
   querying a monitor service.
4. Expose that data to external agents in a secure, authorized, strictly
   read-only, vendor-neutral way. No inbound holes to the lab; Proxmox never
   exposed.

### Non-goals

- Semantic judgment of "differently-worded but unproductive" loops. Hash
  detection catches exact/structural repetition; the semantic call is
  exactly what external agents make by reading timelines from the monitor.
- MCP server (fast-follow, generated from the OpenAPI spec — see §10).
- Automating Cloudflare Tunnel/Access setup (documented runbook only).
- Any write/command path from external consumers into orcest.

## 2. Prior art this design is built on

- **Temporal's activity heartbeats**: liveness = short heartbeat-gap
  timeout, fully independent of the long schedule-to-close ceiling. A task
  may run for hours if liveness events keep arriving; it dies early only
  when they stop.
- **CI inactivity timeouts** (Travis/CircleCI ~10 min no-output defaults;
  Jenkins `timeout(activity: true)`): inactivity and wall-clock are always
  two orthogonal limits, never folded into one.
- **Kubernetes `startupProbe`**: exempt the known-slow bootstrap phase from
  liveness checks entirely instead of tuning one threshold for both phases.
- **OpenHands `StuckDetector`** thresholds: 4+ identical action/observation
  repeats, 3+ same-action errors, 6+ ping-pong alternations, compared
  semantically (IDs/timestamps stripped).
- **systemd watchdog hysteresis + `StartLimitBurst`**: require ≥2 missed
  intervals before declaring death; rate-limit the kill mechanism itself.
- **CircleCI's documented false-kill cause**: stdout buffering. Prefer
  structured stream events over raw stdout recency.

No surveyed agent platform (Claude Code, Codex cloud, Cursor, Jules) ships
first-party stuck detection; this gap is acknowledged in their own issue
trackers. We are building it, not integrating it.

## 3. Architecture overview

```
┌────────────── worker VM ──────────────┐
│ provider CLI (claude / grok / …)      │
│   │ stream-json stdout                │
│   ▼                                   │
│ LivenessTracker (new)                 │
│   signals: stream, cpu-tree,          │
│            workspace, repetition      │
│   ladder:  BOOTSTRAP→ACTIVE→WAITING→  │
│            SUSPECT→STUCK / LOOPING    │
│   │                                   │
│   ├─► task:activity:{worker_id}  ─────┼──► pool reaper (consults before destroy)
│   └─► orcest:events spool stream ─────┼──┐
└───────────────────────────────────────┘  │
                                           ▼
┌─────────── orchestrator VM ───────────────────────────────┐
│ orchestrator: enqueue/attempt events → orcest:events      │
│               fleet-pressure detector → orcest:events     │
│ event relay (new, alongside trace archiver):              │
│   XREAD orcest:events → POST monitor /ingest (retry/back- │
│   off, cursor, never blocks producers)                    │
│ monitor container (new):                                  │
│   ingest listener  — Docker-internal only, write token    │
│   query listener   — GET/HEAD only, scoped read tokens,   │
│                      SQLite ro, trace archive ro mount    │
└───────────────────────┬───────────────────────────────────┘
                        │ Cloudflare Tunnel (outbound-only)
                        ▼
        Cloudflare Access (service tokens per consumer)
                        ▼
        external read-only consumers (Grok bots, Claude agents, curl)
```

Kill decisions stay inside orcest (worker ladder + pool reaper). External
consumers are read-only by construction; they inform the human, they never
command orcest.

## 4. Liveness signals (worker-side)

A `LivenessTracker` runs in the worker alongside the runner, sampling every
`sample_interval` (default **30s**). It is provider-agnostic and fed by the
existing runner plumbing (`on_output` / stream parsers in
`_runner_base.py`).

**S1 — Stream liveness (primary).** Parsed stream-json events, not raw
stdout recency:

- Progress evidence: `content_block_delta` (text/thinking/input_json),
  `content_block_start`/`content_block_stop` (tool-call boundaries),
  `message_start`.
- `api_retry` / rate-limit backoff events → transition to `WAITING` with a
  `reason` (`api_retry` | `rate_limit`); they are "waiting on provider,"
  not progress and not stall.
- Providers without a parsed event vocabulary (registry recipes lacking a
  stream parser) degrade to raw output-line recency for S1, with the
  buffering caveat accepted.

**S2 — Process-tree CPU delta.** Aggregate `utime+stime` across the entire
process tree plus reaped children's `cutime`/`cstime` (top-PID-only sampling
misclassifies agents spawning short-lived `git`/`grep`/compiler children as
idle). CPU counts as idle only after **3 consecutive zero-delta samples**
(hysteresis, ≈90s at the default interval).

**S3 — Workspace activity.** Any file mtime change under the task workspace
since the last sample (cheap bounded walk; no attempt to filter `.git`
churn — git activity is activity).

**S4 — Repetition (livelock detector).** Two normalized hash streams over
parsed tool calls, IDs/timestamps stripped before hashing:

- **Exact-repeat**: hash `(tool_name, normalized_args)`; trip at
  **≥4 identical consecutive** hashes.
- **Error-class**: hash `(tool_name, normalized_error_class)` **ignoring
  args**; trip at **≥3 consecutive** — catches "same failing write,
  slightly different args each round."
- **Ping-pong**: two distinct exact-repeat hashes alternating for
  **≥6 cycles**.

S4 exists because a retry loop reads ACTIVE on S1–S3. S4 requires parsed
tool events; providers without them run S1–S3 only.

## 5. The ladder

```
BOOTSTRAP  from spawn until first S1 progress event or startup_grace
           (default 600s), whichever first. Ladder disabled; ceiling still
           applies.
ACTIVE     any of S1/S2/S3 fresh within its window.
WAITING    provider backoff in flight (S1 api_retry/rate-limit). Own grace
           (waiting_grace, default 1800s) before it can escalate; carries
           reason. Existing usage-exhausted handling is unchanged.
SUSPECT    S1, S2, and S3 ALL stale past their windows (idle_window,
           default 600s). Publishes task.suspect. Kills nothing.
STUCK      still all-stale after a second full idle_window (persistence —
           two consecutive stale evaluations, not one sample). Kill.
LOOPING    an S4 threshold tripped, and tripped again on the next
           evaluation (persistence). Kill with distinct reason. S1–S3
           state is irrelevant here by design.
CEILING    hard wall-clock backstop (see §7). Kill regardless of state.
```

Any single fresh signal (or a novel tool-call hash, for LOOPING) resets the
ladder to ACTIVE.

**Kill semantics** (STUCK / LOOPING / CEILING):

1. Publish the terminal event **first** (signal snapshot: per-signal
   last-fresh timestamps, ladder history, tripped hash stream + count).
2. Kill via existing `_kill_process_tree`.
3. **Verify death.** If any tree member remains in `D` state
   (`/proc/[pid]/stat`), the kill silently failed — publish
   `task.killed` with `verified: false` and write a `needs_reap` flag into
   the activity record; the pool reaper destroys the VM (the hammer that
   works on D-state). Never report an unverified kill as success.
4. Result classification: STUCK/LOOPING kills produce a **permanent**
   (non-transient) `RunnerResult` with the machine-readable reason in the
   summary; CEILING keeps today's transient timeout semantics. Rationale:
   a corroborated stall/loop will almost certainly repeat on blind retry;
   the orchestrator's per-SHA attempt budget still applies if a human or
   new commit changes the input.

**Fleet gates** (evaluated before entering STUCK/LOOPING, i.e. before any
kill step runs):

- If `orcest:fleet:pressure` is set (§8), SUSPECT→STUCK escalation is
  suspended: keep publishing `task.suspect` into the timeline (they are
  data), but do not kill and do not page.
- Global kill rate-limit: at most `max_kills_per_hour` (default **0** =
  observation mode, no kills; amended post-implementation from 6)
  ladder-triggered kills fleet-wide. The orchestrator mirrors the
  configured limit to `orcest:fleet:kill_budget:limit`; workers INCR an
  hourly Redis counter and compare against it, so the fleet-wide policy
  has a single source of truth and the observation-phase value (`0`) is
  changeable without a template rebake. On breach, emit
  `fleet.kill_limit` and defer further ladder kills (CEILING is exempt —
  it is the backstop).

## 6. Activity record & pool reaper change

Every sample, the tracker writes `{key_prefix}:task:activity:{worker_id}`
(Redis hash, TTL = 4 × sample_interval): `task_id`, `state`,
`last_liveness_ts`, `ladder_since`, `needs_reap`, compact signal snapshot.

> **(amended post-implementation)** The activity record key is the
> **global, unprefixed** `workers:activity:{worker_id}` (see
> `worker/liveness_tracker.py:_ACTIVITY_KEY_PREFIX` and
> `fleet/pool_manager.py:_ACTIVITY_KEY_PREFIX`), not
> `{key_prefix}:task:activity:{worker_id}` as originally specified. The pool
> manager is a cross-project, single-instance component with no per-project
> Redis client, so it must read a key it can resolve without a project
> `key_prefix`; keying under the project prefix would have made every
> project's activity record invisible to the one reaper that consults them.

`PoolManager._health_check` changes from `elapsed > max_duration → destroy`
to destroy only when:

- the hard ceiling is exceeded (`elapsed > max_task_duration`, raised — §7), or
- `needs_reap` is set (failed kill / D-state escalation), or
- the activity record is **absent or stale** (> `activity_stale_after`,
  default 300s) while the consumer still has pending entries — the
  "worker VM silently died" case the current reaper was really for.

A worker VM actively updating its activity record is never destroyed early,
regardless of elapsed time below the ceiling.

> **(amended post-implementation)** The absent-or-stale destroy path
> additionally requires the worker's **liveness heartbeat**
> (`workers:heartbeat:{worker_id}`, written by every worker regardless of
> watchdog config) to be absent too — see
> `PoolManager._activity_reap_reason`. An absent-or-stale activity record is
> not by itself proof of death: `watchdog.enabled: false` and pre-watchdog
> worker images never write the record at all, so treating its absence alone
> as fatal would reap perfectly healthy workers on any watchdog-disabled
> fleet or during a rolling image upgrade. Corroborating with the heartbeat
> (already used by the orphan-PEL sweep) distinguishes "watchdog quiet" from
> "process gone."
>
> **(amended post-implementation, issue #615)** The same path also requires
> the VM's `pool:active` elapsed time to be at least
> `activity_stale_min_elapsed` (default 600s). Below that floor, missing
> activity plus missing heartbeat is not enough to infer a dead task — a
> young VM may not have written either Redis key yet. `needs_reap` and
> `elapsed > max_task_duration` bypass the floor.

> **(amended post-implementation)** The `task.reaped` event's `data.reason`
> field (§8) uses the honest, implementation-level vocabulary
> `ceiling` / `needs_reap` / `activity_stale` / `done_cleanup` /
> `drain_race` / `orphan_pel` (see `pool_manager.py`'s `REAP_REASON_*`
> constants), not just the three §6 destroy conditions above. The first
> three (`ceiling`, `needs_reap`, `activity_stale`) correspond directly to
> this section's destroy conditions; `done_cleanup`, `drain_race`, and
> `orphan_pel` are the pre-existing reap-coordination call sites (normal
> task-done cleanup, a lost drain race, and the orphan-PEL sweep) that also
> route through the same `_coordinate_reaped_vm` path and must report their
> actual cause rather than being mislabeled as one of the three watchdog
> reasons they never observed.

## 7. Timeout migration

| Knob | Today | After |
|---|---|---|
| `RunnerConfig.timeout` | 1800s deployed; the primary killer | Becomes the CEILING. Default raised to **21600s (6h)**; deployed config updated. |
| `pool.max_task_duration` | 7200s; independent killer | Raised to **ceiling + 3600s grace (25200s)**; only fires per §6. Invariant `max_task_duration > timeout + grace` retained (`fleet/config.py:203`). |
| (new) `watchdog.*` | — | The primary early-kill mechanism (§4–5). |
| `compute_pending_task_ttl` | derived from timeout | Recomputed from the new ceiling; formula unchanged. |

`watchdog.enabled: false` restores exactly today's behavior (pure
wall-clock at the configured `timeout`), as the rollout/rollback lever.

**Consolidation:** the watchdog logic currently duplicated between
`_runner_base.py` and `claude_runner.py` (drift warning at
`_runner_base.py:9-15`) is unified: the `LivenessTracker` and kill/verify
path live in `_runner_base.py` only; `claude_runner.py`'s copy is removed as
part of this work, not left to drift a third way.

## 8. Events: identity, envelope, taxonomy

**Work identity.** Every task-scoped event carries:

- `work`: `(repo, resource_type, resource_id)` — the stable grouping key.
  `Task.id` is a fresh UUID per enqueue (`shared/models.py:273`), so it
  cannot group retries; and `snapshot_head_sha` changes when a worker
  pushes, so SHA cannot either (a looping agent that commits every round
  would evade SHA-grouping).
- `attempt`: the orchestrator's cross-SHA total-attempt counter for the
  resource (the `max_total_attempts` counter), stamped on the `Task` at
  publish (new field, default `0` for old payloads).
- `task_id` (per-execution key; joins to the trace archive),
  `head_sha` (context), `worker_id`, `provider`.

**Envelope.** Hand-rolled JSON using CloudEvents 1.0 attribute names — no
SDK:

```json
{
  "id": "«uuid4»",
  "source": "urn:orcest:«project-key-prefix»",
  "type": "net.orcest.task.suspect",
  "subject": "«task_id»",
  "time": "2026-08-17T21:04:05Z",
  "data": { "work": {...}, "attempt": 2, "head_sha": "…", "worker_id": "…",
             "provider": "claude", ...event-specific fields }
}
```

`(source, id)` is the idempotency key end-to-end.

**Taxonomy (locked, additive-only after v1, published as an enum in the
OpenAPI spec):**

| type (suffix after `net.orcest.`) | emitted by | notes |
|---|---|---|
| `task.enqueued` | orchestrator | includes `decision_reason`, `attempt` |
| `task.started` | worker | replaces nothing; `task_start` marker stays |
| `task.bootstrap` | worker | ladder entered/exited bootstrap |
| `task.active` | worker | transition back to ACTIVE (resets) |
| `task.waiting` | worker | **carries `reason`: `api_retry` \| `rate_limit`** |
| `task.suspect` | worker | signal snapshot; always emitted, even under fleet pressure |
| `task.stuck` | worker | pre-kill declaration + snapshot |
| `task.looping` | worker | tripped stream (`exact`\|`error_class`\|`ping_pong`) + count + hashes |
| `task.killed` | worker | `trigger` (stuck/looping/ceiling), `verified: bool` |
| `task.completed` / `task.failed` | worker | terminal result status + summary |
| `task.reaped` | pool manager | reaper destroy, with reason per §6 |
| `task.activity` | worker | periodic (default 300s) aggregate: lines, liveness-event count, cpu-seconds delta, files-changed count, last 20 tool-call hashes |
| `fleet.pressure` | orchestrator | ≥K tasks SUSPECT in window (§ below) |
| `fleet.kill_limit` | worker/orchestrator | kill rate-limit breached |

> **(amended post-implementation)** Three taxonomy rows behave more
> narrowly than the table above implies:
>
> - `task.bootstrap` is emitted **exactly once**, at tracker start (on the
>   transition into `BOOTSTRAP`) — not on every bootstrap entry/exit; there
>   is only ever one bootstrap phase per task (`_bootstrap_emitted` latch in
>   `liveness_tracker.py`).
> - `task.activity` (the periodic aggregate snapshot) carries a
>   `deferred_kill` field: `true` when the ladder's evaluation this tick
>   would have escalated to STUCK/LOOPING but a fleet gate (pressure or
>   kill-budget) deferred it, keeping the reported state at SUSPECT instead.
>   This is also what's written into the activity record's `deferred_kill`
>   hash field the pool reaper can read.
> - `fleet.kill_limit` is emitted **only on an actual budget breach**
>   (an attempted kill that the hourly counter rejects) — never during
>   observation mode (`max_kills_per_hour: 0`), where every ladder kill is
>   gate-deferred before a budget check is even attempted and reported as
>   `task.suspect` with `deferred_kill: true` instead (see §13).

**Fleet pressure detection** (orchestrator, the only fleet-view component):
if ≥ `pressure_min_tasks` (default 3) distinct tasks emit `task.suspect`
within `pressure_window` (default 600s), emit **one** `fleet.pressure`
event and SET `orcest:fleet:pressure` EX `pressure_hold` (default 900s,
refreshed while the condition persists). Individual `task.suspect` events
continue to flow into timelines throughout — pressure suppresses
escalation and paging, not data.

**Redaction rule:** events and timelines carry tool **names**, normalized
**hashes**, and error **classes** — never raw tool arguments or output
(these can contain file contents and secrets). Raw content exists only in
the trace archive, behind the `traces:read` scope.

## 9. Delivery pipeline

- **Spool:** producers (worker loop, orchestrator, pool manager) XADD
  envelopes to `{key_prefix}:events` (MAXLEN ~50k). Emission is fire-and-
  forget with the same swallow-and-decimate error handling as the existing
  output streaming — producers never block on the monitor.
- **Relay:** a thread in the orchestrator container (same pattern and home
  as `trace_archiver.py`): XREAD after a cursor persisted in Redis, batch-
  POST to the monitor ingest endpoint, exponential backoff on failure,
  cursor advances only on 2xx. At-least-once.
- **Ingest:** `INSERT OR IGNORE` keyed on `(source, id)` → duplicates are
  no-ops. Monitor down for an hour = events delivered an hour late, none
  lost (within stream MAXLEN).

## 10. Monitor service

New top-level `monitor/` (Python 3.12, FastAPI + uvicorn, SQLite WAL —
matching repo language conventions; the TS dashboard remains a separate,
unchanged component). One container, **two listeners**:

- **Ingest** — bound to the Docker-internal network only, never routed
  through the tunnel. `POST /ingest/v1/events` (batch), auth: single write
  token (`MONITOR_WRITE_TOKEN`), timing-safe compare.
- **Query** — the only listener the tunnel reaches. **GET/HEAD only**
  (405 otherwise, enforced in-app because Cloudflare Access cannot filter
  by HTTP method). SQLite opened `mode=ro` + `PRAGMA query_only=1` on
  every query connection. Trace archive mounted read-only.

> **(amended post-implementation)** The two listeners are on distinct ports,
> not just distinct network bindings: **ingest listens on 9091**, **query on
> 9090** (`config/monitor.example.yaml`'s `ingest_port`/`query_port`;
> `docker-compose.monitor.yml` publishes only 9090 to the host — 9091 is
> deliberately never published, since only the Docker-internal relay needs
> it).

**Query API (v1, OpenAPI spec published at `/api/v1/openapi.json`):**

- `GET /api/v1/health` — unauthenticated liveness (mirrors dashboard).
- `GET /api/v1/events?type=&repo=&resource_id=&since=&limit=` — filtered
  feed. **(amended post-implementation)** the resource filter's query param
  is `resource_id` (matching `Task.resource_id`'s integer type), not the
  originally-specified `resource=`.
- `GET /api/v1/tasks/{task_id}/timeline` — ordered state transitions,
  last-N normalized tool-call records (name + hash + error class), errors,
  activity aggregates.
- `GET /api/v1/work/{owner}/{name}/{resource_type}/{resource_id}` — all
  attempts for a work item, grouped: attempt list with task_ids, SHAs,
  outcomes, and per-attempt timeline summaries. This is the "am I looking
  at retry 3 of the same work?" endpoint. **(amended post-implementation)**
  the repo is addressed as two path segments (`{owner}/{name}`), not the
  originally-specified single `{repo}` segment.
- `GET /api/v1/fleet` — current pressure state, kill-budget usage, active
  task states.
- `GET /api/v1/tasks/{task_id}/trace?tail=N` — raw trace lines from the
  archive. **Requires `traces:read`.**

**Auth:** static bearer tokens in monitor config, each with scopes from
`{events:read, traces:read}`. Rotation/revocation = config edit + container
restart. Layered in front (operator-configured, runbook in docs):
Cloudflare Tunnel (outbound-only `cloudflared`) → Cloudflare Access with
per-consumer service tokens. Defense in depth: Access identity → app
bearer scope → method allowlist → ro database.

**MCP fast-follow (explicitly out of v1):** a read-only MCP server
generated from the same OpenAPI spec, for zero-config Claude/Grok tool
discovery. Deliberately not rushed.

## 11. Configuration surface

```yaml
# worker.yaml (RunnerConfig)
runner:
  timeout: 21600            # now the CEILING
  watchdog:
    enabled: true
    sample_interval: 30
    startup_grace: 600
    idle_window: 600
    waiting_grace: 1800
    loop_exact_threshold: 4
    loop_error_threshold: 3
    loop_pingpong_threshold: 6

# orchestrator.yaml
monitor:
  ingest_url: http://monitor:9090/ingest/v1/events
  write_token_env: MONITOR_WRITE_TOKEN
  events_maxlen: 50000
fleet_health:
  pressure_min_tasks: 3
  pressure_window: 600
  pressure_hold: 900
  max_kills_per_hour: 6

# fleet /etc/orcest/config.yaml
pool:
  max_task_duration: 25200
  activity_stale_after: 300
  activity_stale_min_elapsed: 600

# monitor config (monitor.yaml / env)
readers:
  - name: grok-watcher
    token_env: MONITOR_TOKEN_GROK
    scopes: [events:read]
  - name: thayne-admin
    token_env: MONITOR_TOKEN_ADMIN
    scopes: [events:read, traces:read]
```

> **(amended post-implementation)** The `orchestrator.yaml` block above does
> not match the shipped config surface:
>
> - The monitor keys are **flat top-level fields**, not nested under a
>   `monitor:` block: `monitor_ingest_url` and `monitor_write_token_env`
>   (see `shared/config.py`'s `OrchestratorConfig` and
>   `config/orchestrator.example.yaml`). `fleet_health:` remains a nested
>   block as originally specified — only the monitor keys flattened.
> - `events_maxlen` was **dropped entirely**; there is no per-deployment
>   override. `EventPublisher` always uses the fixed default
>   `DEFAULT_EVENTS_MAXLEN = 50000` (`shared/events.py`). A configurable cap
>   here was judged not worth the surface area versus one hardcoded value
>   consistent across every producer.
> - The **ingest URL's port is 9091**, not 9090 (query stays 9090 — see the
>   §10 amendment above); the example line above should read
>   `http://monitor:9091/ingest/v1/events`.
> - The kill-budget **worker-side fallback is `0`, fail-closed**: when a
>   worker cannot read `orcest:fleet:kill_budget:limit` (key absent, read
>   error, or the orchestrator hasn't started `FleetHealthMonitor` yet), it
>   treats the limit as `0` rather than the configured `max_kills_per_hour`
>   default — the same fail-closed value used deliberately for observation
>   mode (§13), so a monitor outage can never accidentally raise the
>   effective kill budget above what an operator explicitly set
>   (`worker/liveness_tracker.py`'s `_KILL_BUDGET_DEFAULT_LIMIT = 0`).

## 12. Testing

- **Ladder state machine:** pure unit tests with a fake clock — every
  transition, persistence requirements, WAITING exemption, bootstrap
  gating, pressure/kill-limit gates, reset-on-activity.
- **Signal samplers:** CPU-tree aggregation against a spawned test process
  tree (including short-lived children); mtime walker; hash normalizer
  (IDs/timestamps stripped; error-class ignores args).
- **Runner integration:** fake provider scripts emitting stream-json —
  productive-slow (deltas forever → survives past old timeout),
  silent-hang (→ SUSPECT→STUCK kill with snapshot), exact-loop and
  error-loop (→ LOOPING), rate-limited (`api_retry` → WAITING, no kill),
  D-state simulation for verify-death (mock).
- **Reaper:** unit tests that an active activity record blocks destroy
  below ceiling; stale/absent record and `needs_reap` allow it.
- **Relay/ingest:** at-least-once with dup delivery → single row; monitor
  outage → cursor holds, catch-up on recovery.
- **Monitor API:** scope enforcement (events token 403s on trace route),
  method allowlist, ro-database (write attempt fails), work-grouping
  endpoint across multi-attempt fixtures.
- **Contract:** taxonomy enum in OpenAPI matches emitter constants
  (single source of truth module, imported by both).

## 13. Rollout

Three deploy layers apply (host CLI pip/pipx, `fleet update` for
containers, `fleet rebake` for worker template — worker changes need all
three):

1. Ship with `watchdog.enabled: false` + monitor container up → events
   flowing (enqueue/start/complete), API queryable, zero behavior change.
2. Enable watchdog on the fleet with kills gated to SUSPECT-only reporting
   (`max_kills_per_hour: 0`) → observe false-positive rate against real
   workloads for a few days via the monitor itself.
3. Raise ceilings + enable kills (`max_kills_per_hour: 6`).
4. Operator sets up Cloudflare Tunnel/Access per runbook, mints reader
   tokens, points external agents at the query API.

> **(amended post-implementation)** Three refinements to this rollout, all
> confirmed by the shipped implementation:
>
> - **Step 2's "SUSPECT-only reporting" mechanism**: with `max_kills_per_hour:
>   0`, the ladder never transitions into `STUCK`/`LOOPING` at all — the
>   fleet kill-budget gate defers the escalation at evaluation time, so the
>   reported state stays `SUSPECT` and the emitted events are `task.suspect`
>   (unchanged) carrying `deferred_kill: true` in the `task.activity`
>   aggregate and the activity record (see the §8 amendment above), not
>   `task.stuck`/`task.looping` events that never fire. This is what makes
>   observation mode a true dry run: the corroborated-kill decision is
>   computed and visible, just never acted on.
> - **The `watchdog.enabled` toggle does not need the three-layer deploy /
>   rebake described above.** It is `PoolConfig.watchdog_enabled`, a
>   fleet-level knob rendered into each worker's `runner.watchdog.enabled`
>   at **clone time** via cloud-init userdata (`cloud_init.py`'s
>   `render_clone_userdata`), not baked into the template image. Flipping it
>   is a fleet config edit (`/etc/orcest/config.yaml` + `fleet update`) that
>   takes effect on the *next* clone generation — already-running VMs keep
>   whatever value they were cloned with, but no `fleet rebake` is required
>   to change it fleet-wide.
> - **Interactive PTY runners are outside ladder coverage entirely, in every
>   step above.** `ClaudeInteractiveRunner` drives Claude Code through a PTY
>   with its own execution loop (not `_BaseCliRunner._run_cli_agent`), so the
>   watchdog wiring in `worker/loop.py` never attaches a `LivenessTracker` to
>   it regardless of `watchdog.enabled`. It keeps a fixed, pinned timeout of
>   **5400s** (`cloud_init.py`'s `_INTERACTIVE_RUNNER_TIMEOUT_SECONDS`) as
>   its only ceiling. This is accepted, not a gap to close in this rollout:
>   closing it means moving that profile to a headless/stream-json mode the
>   ladder can parse, which is separate follow-on work, not a rollout step.

## 14. Residual risks (accepted)

- **Semantically-varying unproductive loops** evade S4 — by design, this
  is the external watcher's job, fed by the timeline endpoint.
- **A single >10-min silent legitimate wait** that emits no stream events,
  burns no CPU, and touches no files (e.g. an extreme provider-side stall
  that never surfaces an `api_retry`) reaches SUSPECT and, after a second
  window, STUCK. Two full windows (≥20 min of absolute silence on all
  signals) is the accepted confidence bar; tune `idle_window` upward per
  deployment if real workloads show otherwise (step 2 of rollout measures
  exactly this).
- **Provider stream-vocabulary drift** (especially Grok's thinly-documented
  event schema) degrades S1/S4 to coarser signals; the design degrades
  gracefully rather than failing closed.
