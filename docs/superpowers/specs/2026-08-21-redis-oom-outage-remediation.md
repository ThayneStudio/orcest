# Redis OOM Outage Remediation — Spec

> **Historical archive — 2026-09-30:** This August planning record is
> preserved for incident and design provenance. Its status, checklists,
> implementation steps, and agent instructions describe the original
> session; they are not current execution instructions or deployment
> evidence. See the [archive disposition](../local-planning-archive-disposition.md)
> for implemented work and remaining operational evidence.

**Incident:** 2026-08-21, fleet-wide outage 09:17:55Z → 14:10:22Z (4h53m).
**Status:** findings verified by adversarial review; two initial hypotheses were
refuted and are recorded here so they are not re-investigated.

## 1. What happened

Redis reached its 1 GB `maxmemory` under `maxmemory-policy noeviction` and began
rejecting every `denyoom` write fleet-wide. 4444 `OutOfMemoryError` lines appear in
the pool-manager log across the window. Six asemly tasks started 09:08–09:52 all
ended `incomplete` (across codex, clauder **and** grok — simultaneous three-provider
failure and recovery rules out credential expiry), then no traces at all were written
for 4h18m, then normal service resumed at 14:11:06 after an operator XTRIM freed
memory.

Recovery note for the runbook: the operator restarted all four orchestrator
containers first (14:07:58–14:08:38). That did **not** work — they came back, hit OOM
again and crash-looped (bbr 10 restarts, orcest 6). Restarting a *client* cannot free
server memory. Memory was actually freed at ~14:10:22.

## 2. Root cause: output streams are unbounded in bytes

`<project>:output:<worker-id>` streams are capped by **entry count**
(`_STREAM_MAXLEN = 20000`, `src/orcest/worker/loop.py:78`), applied as
`XADD ... MAXLEN ~ 20000`. Entry *size* is unbounded.

Measured on the two fattest streams:

| stream | n | median | mean | max |
|---|---|---|---|---|
| `bbr-platform:output:orcest-worker-10001` | 566 | 328 B | 50,708 B | 194,097 B |
| `orcest:output:orcest-worker-10001` | 500 | 297 B | 54,558 B | 265,829 B |

A 155× gap between median and mean: a handful of very large entries (headless
stream-json blobs, not PTY chunks — `os.read` caps those at 4096 B) carry nearly all
the bytes. The top 10% of entries hold ~38% of the volume.

**The cap never engaged.** `bbr-platform:output:orcest-worker-10001` held ~297 MB at
`entries-added=6062`, under a third of the 20000 limit. Entry count was never the
binding dial. At ~50 KB/entry a *single* stream at a full 20000 entries would be
~1 GB — the cap cannot bind before Redis dies.

Truncating each entry to 8 KB would take that stream from 27.4 MB to 1.6 MB (5.9%);
to 16 KB, 2.87 MB (10.5%).

### 2.1 Working set vs. log buffer

| Category | Keys | Memory |
|---|---|---|
| **OUTPUT** | 24 | **45.8 MB** (post-trim; was 1.08 GB) |
| RESULTS | 29 | 4.1 MB |
| EVENTS | 3 | 1.0 MB |
| TASKS | 8 | 0.2 MB |
| COORD (locks, pending, pool state) | 19 | 0.004 MB |
| DEAD-LETTER | 1 | 0.005 MB |

Queue-critical working set is **5.3 MB**. Worst case with `events` saturated at
MAXLEN 50000 (~674 B/entry × 4 projects) is ~135 MB. **Redis stays at 1 GB** — that
is ~5× more than the queue can ever need. The fix is bounding the log buffer, not
resizing Redis.

### 2.2 Nothing drains the output streams

Output streams have `groups: 0`. The only `XTRIM` calls
(`redis_client.py:680,690`) are MINID-based and target *task* streams with consumer
groups, so they never touch output. The trace archiver reads output streams but never
trims them.

### 2.3 The amplifying feedback loop

`TraceArchiver._pump_output_streams` (`trace_archiver.py:143-183`) persists its
per-stream cursor with `HSET` **to Redis**. `HSET` is `denyoom`, so under OOM the
cursor write fails and the archiver `break`s out of the stream. Next pass it re-reads
from the stale cursor and **re-processes entries it already wrote to disk** — the
entry is written by `_process_entry` *before* the cursor persist, so the existing
"avoid duplicate writes" comment does not hold.

Consequence: under memory pressure the archiver stops draining and starts duplicating.
The incident trace for task `4fa8841e` is 10352 bytes = **8 byte-identical copies** of
one 1294-byte block (all md5 `0f9127bc…`).

### 2.4 Command flags that shape the fix

Verified via `COMMAND INFO`:

| command | denyoom? | usable at ceiling |
|---|---|---|
| `XADD` | yes | **no** |
| `HSET` | yes | **no** |
| `SET` | yes | **no** |
| `XTRIM` | **no** | **yes** |
| `XDEL` | **no** | **yes** |

`XTRIM` works while Redis is wedged. An archiver whose cursor does not depend on
Redis can therefore keep draining *and* trimming during an OOM event, actively
relieving pressure instead of amplifying it.

## 3. Second fault: the pool-manager reap is ungated and fails unsafe

Every worker liveness signal is a Redis **write**. Under OOM those writes are
rejected, the keys TTL out, and reads then succeed and *honestly* report the keys
absent. The reaper concludes every worker is dead — simultaneously.

`_activity_reap_reason` (`pool_manager.py:1382`) requires three conditions, and all
three degenerate:

- **Activity record stale** — free for the whole clauder fleet.
  `loop.py:2434-2441` only builds a `tracker_factory` for `_BaseCliRunner`
  subclasses; `ClaudeInteractiveRunner` is excluded, so interactive workers **never
  write `workers:activity:{id}` at all**, and `_activity_record_is_stale` returns
  `True` for an empty record unconditionally.
- **Heartbeat absent** — a `SET ... EX 150` (`WORKER_LIVENESS_TTL = 150`), i.e. the
  only guard that actually holds here, and it is exactly what OOM erases.
- **Pending stream entries** — true by definition for any worker doing work.

The existing fail-safe branches (`return None` on read exceptions) protect against
Redis being *unreachable*. They cannot fire here because no read ever failed.

**There is no circuit breaker.** `grep -niE "kill_budget|budget|grace|min_age|pressure"
src/orcest/fleet/pool_manager.py` returns nothing. The kill budget and pressure gate
live only in `worker/liveness_tracker.py` (`_KILL_BUDGET_DEFAULT_LIMIT = 0`), so the
operator-facing statement "kills are off, budget 0" describes a **different code path**
from the one that did the killing.

Blast radius — 7 force-destroys, all inside the OOM window:

| time (UTC) | VM | elapsed | ceiling |
|---|---|---|---|
| 09:29:23 | 10000 | **42 s** | 25200 s |
| 09:33:52 | 10002 | 215 s | 25200 s |
| 09:33:54 | 10000 | 175 s | 25200 s |
| 09:54:50 | 10001 | **111 s** | 25200 s |
| 09:55:22 | 10003 | **143 s** | 25200 s |
| 14:08:15–18 | 10003, 10001, 10000 | 15317 s | 25200 s |

VMs 10000 and 10002 had claimed real tasks and were producing output when killed
(2859 and 344 trace lines respectively). Pool target is 4, so this cycled the fleet.

`_stop_vm` runs at `pool_manager.py:1353` **before** `_coordinate_reaped_vm`, so the
"preserving Redis state for retry" gate protects only bookkeeping — the VM is
hard-stopped and its work destroyed on the first pass regardless.

## 4. Third fault: stranded tasks on a consumer-less provider stream

`orcest:tasks:issue:grok` currently holds `len=3, consumers=0, lag=3`. No grok worker
has ever attached. Tasks assigned `provider: grok` by round-robin are enqueued
successfully, never claimed, and never emit `task.started`.

`set_pending_task` is `SET NX` with a ~16.7 h TTL, so while the stranded task's
`<project>:pending:issue:<repo>:<n>` marker lives, every later poll logs "Pending task
already exists … skipping publish" and the issue **cannot be retried under a different
provider**.

asemly #1289 drew grok and has stalled a chain of nine correctly-deferred dependents
(1290–1298, 1320). Nothing alerts on `consumers == 0 && lag > 0`, and nothing alerts
on `task.enqueued` with no matching `task.started`.

Note: task streams are **global** under the `orcest:` prefix
(`orcest:tasks:issue:<provider>`), while output/results/events streams are
per-project. Scanning `asemly:tasks*` returns nothing and looks like an empty queue.

## 5. Fourth fault: reap telemetry reports publish-time, not kill-time

VM 10003 was powered off at **09:55:22Z at 143 s elapsed**. Its `task.reaped` event is
stamped **14:08:15Z** with `elapsed_seconds: 15316`, because the pool manager spent
4h13m retrying a reap it could not publish, with the clock running from a
`pool:active` HSET that only landed at 09:52:58.

This is not cosmetic: it fabricated a 4.4-hour hang that did not happen and misdirected
the first several hours of this investigation.

## 6. Explicitly refuted — do not re-investigate

- **"The 5400 s runner timeout failed to fire."** False. Task `4fa8841e` lived ~12
  minutes (started 09:43:42Z, VM stopped 09:55:22Z). Control case the same morning:
  task `faf2e79b` timed out correctly at 5403 s with
  `[transient] Timed out after 5400s`.
- **"The `terminal_output[-8:]` window caused a wedge on the trust dialog."** False.
  Running the production `_looks_like_workspace_trust_prompt` against the real captured
  1050-byte chunk returns `True` — minimum window needed is **1**, not 8. Claude writes
  the ~1293-byte frame in a single `write()` and `_read_available` reads 4096 B, so real
  renders span 1–2 chunks. The window also slides one chunk per iteration, so detection
  is a span condition, not an alignment lottery. The "8 repaints" were the archiver
  duplication bug of §2.3.

## 7. Requirements

- **R1** Redis `maxmemory` stays at **1 gb** and `maxmemory-policy` stays
  `noeviction`. No resizing, no second instance. (Explicit operator decision.)
- **R2** A single worker's output must not be able to exhaust Redis, regardless of
  task duration or per-line payload size.
- **R3** The trace archiver must keep draining and trimming while Redis is at the
  ceiling, and must never write the same entry to an archive file twice.
- **R4** Full-fidelity output remains on disk at
  `/mnt/truenas-logs/orcest-traces/<project>/YYYY/MM/DD/`. Any truncation must be
  explicit, bounded, and recorded in-band.
- **R5** The pool manager must not destroy VMs for `activity_stale` when the
  liveness signal is unavailable for infrastructure reasons rather than worker death.
- **R6** A correlated fleet-wide staleness event must never destroy more than a
  bounded fraction of the pool.
- **R7** Work enqueued to a provider stream with no consumers must be detected and
  surfaced, and must not permanently block the issue behind a pending marker.
- **R8** Reap telemetry must report when the VM was actually killed.

## 8. Out of scope

Interactive-runner hardening (widening `terminal_output[-8:]`, the
`workspace_trust_confirmed` single-shot latch, dead `_drain_startup_output`) — none of
it contributed to this incident; track separately. Per-worker Redis ACLs for the
forgeable `needs_reap` flag remain tracked separately per the 2026-06 audit.
