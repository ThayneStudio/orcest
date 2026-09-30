# Validated Backlog Remediation — Spec

> **Historical archive — 2026-09-30:** This August planning record is
> preserved for incident and design provenance. Its status, checklists,
> implementation steps, and agent instructions describe the original
> session; they are not current execution instructions or deployment
> evidence. See the [archive disposition](../local-planning-archive-disposition.md)
> for implemented work and remaining operational evidence.

**Status:** Gate 1 approved; revised Gate 2 reviewed by Claude Fable and
awaiting user approval; no GitHub changes have been made.
**Validated against:** `origin/master` at `98952430fcff259aac538e36829ddab367bdd5d6`.
**Independent review:** Claude Fable, 2026-08-24. Its two initial Gate 2
blockers are resolved; its second-pass recommendation was `APPROVE WITH
NON-BLOCKING EDITS`, and those edits are incorporated below.
**Scope:** GitHub issue planning for open issues #535, #542, #546, #547,
#588, #604, and #610–#616. This document specifies future work; this planning
session does not implement it.

## 1. Purpose

Convert the validated backlog into a set of single-worker issues with accurate
claims, explicit non-goals, acceptance criteria, labels, rollout guidance, and
machine-readable dependencies. Close issues already completed by merged work.

The result must let the Orcest fleet select safe, independent work without
re-deriving the 2026-08-21 Redis incident or inheriting claims that validation
refuted.

## 2. Relationship to the existing outage documents

The untracked documents below remain untouched:

- `docs/superpowers/specs/2026-08-21-redis-oom-outage-remediation.md`
- `docs/superpowers/plans/2026-08-21-redis-oom-outage-remediation.md`

They preserve the incident evidence and the original response design. This
spec supersedes their backlog decomposition for work that remains after PR #591
and PR #601 landed. In particular:

- Output entries are now byte-capped, count-capped, and TTL-bound by PR #601.
- Correlated loss of every active-worker heartbeat is now guarded by PR #591.
- The old plan predates the validation of issues #610–#616 and therefore mixes
  completed work, remaining defects, and optional enhancements.

Production timestamps, byte totals, and duplicate-file counts are retained as
incident evidence. Repository validation established the corresponding code
mechanisms but did not independently reconstruct every production measurement.
The tracking epic must repeat this supersession note and link both documents so
future readers do not mistake their older issue split for the active backlog.

## 3. Goals and success criteria

### 3.1 Goals

1. A full Redis using `noeviction` must not permanently kill a worker process,
   crash-loop the orchestrator, or cause idle-VM replacement churn.
2. A young active VM must not be destroyed from absent inferred liveness alone.
3. Reap events must describe the first successful fence, not a later Redis
   coordination retry.
4. A provider queue containing work but no live consumer must become an
   autonomous, transition-based operator signal.
5. Output publication must set MAXLEN and TTL in one atomic server-side unit.
6. Trace archival must not duplicate an entry because Redis refused a cursor
   write, and it must never advance or trim past data that did not reach disk.
7. Provider CLI pins must be upgraded deliberately and monitored for drift.
8. Credential and agent-skill documentation must describe the behavior the
   current architecture actually supports.
9. Completed or duplicate Grok-integrity issues must leave the open backlog.

### 3.2 Backlog success criteria

- Every runnable leaf has `orcest:ready` and the appropriate existing type and
  priority labels.
- Work-item dependencies use both GitHub native `blocked-by` relationships and
  same-repository `Blocked by #N` body lines.
- No leaf is blocked by the tracking epic.
- The manual Gemini/Cursor smoke test is not sent to the fleet.
- Existing issues are rewritten rather than duplicated when their core problem
  remains valid.
- New issues are created only for scopes that validation proved should be split
  from an existing issue or shared by multiple existing issues.

## 4. Non-goals

- No application code, tests, deployment, rebake, PR, or feature branch in this
  planning session.
- No Redis resize, eviction-policy change, or second Redis instance.
- No automatic provider CLI upgrades or automatic worker-template rebakes.
- No automatic rerouting, deletion, or pending-marker release for tasks on a
  stranded provider stream. Detection is required; task recovery remains a
  separately decidable policy because it can create duplicate execution.
- No restoration of broad child-process credential whitelisting.
- No support for inherited `ANTHROPIC_API_KEY` as a Claude authentication mode.
  The approved policy in this spec is to require `CLAUDE_CODE_OAUTH_TOKEN` and
  make the migration explicit.
- No guarantee that reap fence timestamps survive a pool-manager process
  restart. When the first fence cannot be proven after restart, telemetry must
  omit unknown values rather than fabricate them.
- No exact-once guarantee for a host crash in the interval after the archive
  file is fsynced but before the atomic local cursor update is fsynced and
  renamed. The required guarantee covers Redis write refusal and ordinary
  retry loops; every detected disk failure must stop cursor advancement and
  trimming.

## 5. Backlog structure and phases

One new tracking epic will index all retained work and this spec. It is tracking
only and is never a blocker or `orcest:ready`.

### Phase 1 — Stop active failure and unsafe destruction

- #616: Codex CLI bump and JSON-contract refresh.
- #615: minimum elapsed floor for `activity_stale`.
- #610: idle-worker heartbeat detection, writability guard, replacement, and
  churn breaker.
- New shared leaf: read-first, OOM-aware consumer-group inspection primitive.
- New worker-startup leaf, blocked by the shared consumer-group primitive.
- New manual Codex rebake/smoke leaf, blocked by #616.

### Phase 2 — Recovery, telemetry, and durable output correctness

- #611, blocked by the shared consumer-group primitive.
- #613: stranded provider-stream transition detection.
- #614: first-fence telemetry.
- #612: loss-safe, Redis-independent trace cursoring.
- #604: atomic capped output append plus TTL.

### Phase 3 — Controlled reclamation and maintenance

- New trace-trimming and cursor-GC leaf, blocked by #612.
- New provider-pin freshness leaf, blocked by the manual Codex rebake/smoke
  leaf so maintenance follows verified deployment.
- #535: remove the misleading `ANTHROPIC_API_KEY` contract and document the
  supported migration.
- #588: manual Gemini/Cursor discovery and activation smoke tests.

Housekeeping is immediate: close #542 and #546 as completed by PR #551, and
close #547 as a duplicate of #546 that PR #551 also resolved.

## 6. Workstream A — Redis bootstrap and worker liveness

### 6.1 New shared leaf: read-first consumer-group inspection

**Purpose:** Give worker and orchestrator startup one tested primitive that can
distinguish an already-existing group from a genuinely missing group without
issuing a `denyoom` write.

**Interface:** Add an internal Redis-client operation that returns a typed
result equivalent to `exists` or `missing` after `XINFO GROUPS`. Existing groups
return without `XGROUP CREATE`. Missing groups still use the existing create
path and preserve `BUSYGROUP` race handling.

**Error behavior:**

- `WRONGTYPE`, authentication, ACL, protocol, and malformed-response errors
  propagate.
- Redis OOM from creation propagates as `OutOfMemoryError`; caller policy is
  intentionally not hidden inside the shared primitive.
- Redis's `ERR no such key` response from `XINFO GROUPS` is the normal
  absent-stream case and maps to `missing`; other response errors propagate.
- A group created between inspection and creation is success through the
  existing `BUSYGROUP` handling.

**Tests:** existing group under a Redis client that rejects writes, absent
stream returning `ERR no such key`, missing group creation, creation race,
wrong type, ACL error, prefixed and raw paths.

### 6.2 New worker-startup leaf

**Purpose:** Keep the worker process alive when a required group is genuinely
missing while Redis rejects writes, preventing systemd from exhausting its
start limit.

**Dependency:** blocked by the shared consumer-group primitive.

**Behavior:**

- Existing groups pass the read-first inspection without a write.
- A missing group plus OOM enters an in-process exponential retry loop, capped
  at 60 seconds between attempts, with transition/rate-limited logging.
- Shutdown remains responsive while waiting.
- Non-OOM failures remain immediate fatal configuration errors.
- The worker heartbeat begins only after all required groups exist; #610 owns
  the fleet-side grace that prevents this waiting state from causing churn.

**Tests:** OOM-then-success in one process, repeated OOM without process exit,
shutdown during wait, existing group at maxmemory, and non-OOM fail-fast.

### 6.3 Update #610: idle worker detection and replacement

**New title:** `Detect and replace idle VMs whose worker heartbeat never appears`

Move worker group-bootstrap handling to the new startup leaf. Keep #610 for the
fleet-side invariant that a running VM without a live worker must not count as
usable capacity.

**State:** Maintain an in-memory first-missing timestamp per idle VM. Default
dwell is 300 seconds (twice the 150-second heartbeat TTL). Seeing a heartbeat or
leaving `pool:idle` clears the timestamp. A pool-manager restart resets the
timestamp, which safely delays replacement.

**Write-awareness:** The pool manager refreshes one dedicated, unprefixed
`pool:write-health` TTL sentinel with the same `SET ... EX` command class used
for worker heartbeats. Track `write_healthy_since` only while that write
succeeds on every pass. Any OOM or other write failure clears both
`write_healthy_since` and the affected VM's first-missing timestamp. An absent
heartbeat becomes actionable only after the sentinel and the absence have both
remained continuous for 300 seconds. During write refusal or the post-recovery
settle window, absence is unknown rather than dead. Operator notes explain that
even a transient write error safely restarts the full dwell, so repeated resets
are visible but are not mistaken for a replacement bug.

**Recovery:** Destroy and refill an idle VM after the dwell. Apply a per-profile
breaker of at most two idle-liveness replacements in 15 minutes; log once when
the breaker opens and once when it clears. Cover both flat and profiled pools.

**Non-goals:** changing systemd limits or owning consumer-group retry.

### 6.4 Update #611: orchestrator degradation during OOM

**New title:** `Make orchestrator bootstrap degrade gracefully during Redis noeviction OOM`

**Dependency:** blocked by the shared consumer-group primitive.

Start the trace archiver before the first startup operation that can perform a
`denyoom` write. Existing groups use read-first inspection. Missing-group OOM
and provider-account-backfill OOM are deferred and retried from the main loop.
Every non-OOM exception remains fatal. Starting the archiver early is useful for
disk durability even before trimming lands; the dependent trim issue owns Redis
reclamation.

**Tests:** startup reaches the archiver and loop under group-create OOM and
backfill OOM; deferred work succeeds later; wrong type and ACL failures still
terminate startup.

## 7. Workstream B — Reap safety and telemetry

### 7.1 Update #615: minimum elapsed floor

Keep the title. Correct the body to say there is no minimum/below-ceiling
elapsed comparison for `activity_stale`; a maximum-duration comparison does
exist.

Add a configurable minimum elapsed value, default 600 seconds. Below the floor,
`activity_stale` alone cannot destroy a VM. `needs_reap` and the absolute task
ceiling bypass the floor. Do not remove or weaken tests for genuine old worker
death.

**Tests:** young single active VM, young mixed fleet with one missing heartbeat,
the same cases after the floor, immediate `needs_reap`, and unconditional
ceiling behavior.

### 7.2 Update #614: first-fence telemetry

**New title:** `Preserve kill-time elapsed and killed_at across reap coordination retries`

On the first confirmed transition to stopped, capture an in-memory fence record:

```text
ReapFence(vm_id, reason, killed_at_unix, elapsed_at_kill_seconds)
```

Reassert the idempotent stop on every retry, but never overwrite the first
record. Thread its values through result recovery and the event payload.
`killed_at` is RFC3339 UTC; `elapsed_seconds` is the frozen elapsed value.
Clear the record only after coordination commits and VM cleanup completes.

If the pool manager restarts and finds a VM already stopped without a fence
record, the values are unknown: omit `killed_at` and `elapsed_seconds` rather
than recomputing them from publish time. Envelope `time` remains event
construction time and must not be described as durable storage time.

**Tests:** failed-then-successful multi-pass coordination, repeated stop
assertion, memo cleanup, and restart/unknown behavior.

## 8. Workstream C — Output stream and trace durability

### 8.1 Update #604: atomic capped append and TTL

Keep the title. Add a Redis-client operation that executes capped `XADD` and
`EXPIRE` as one server-side atomic unit and one network round trip for prefixed
and raw streams. The worker output callback uses only this operation.

**Invariants:** MAXLEN and chunking remain unchanged; a successful first append
always leaves a positive TTL; an append failure creates neither a partial key
nor a misleading success result.

**Tests:** new stream, existing stream TTL refresh, raw/prefixed keys, chunked
lines, XADD failure, and a command-count assertion proving the old sequential
round trips are gone.

### 8.2 Update #612: loss-safe local cursoring

**New title:** `Make trace archival cursoring loss-safe under Redis write refusal`

Remove trimming from this issue and move it to the dependent leaf below. Replace
the Redis cursor hash as the active cursor with a per-project state file on the
archive volume:

```json
{
  "version": 1,
  "project": "orcest",
  "streams": {
    "output:orcest-worker-10001": {
      "last_id": "1720000000000-0",
      "last_seen": "2026-08-24T00:00:00Z"
    }
  }
}
```

Write it atomically with mode `0600`. On first deployment only, seed it from
the legacy Redis hash when no local file exists. A corrupt local file disables
advancement and raises a visible health error; it must not silently replay or
skip.

Entry handlers return a positive `archived` result only after bytes reached an
open file, the handle was flushed, and the archive file was fsynced. At each
cursor-persist boundary the order is: append, flush and fsync the archive file,
atomically write and fsync the `0600` cursor temp file, rename it, and fsync the
parent directory where supported. The temp file must be in the final cursor
file's directory so the rename is atomic on one filesystem. Only after that
sequence may the committed in-memory and local cursors advance. Redis cursor
writes are no longer part of the pump. A local cursor persist failure halts that
stream, may retain a process-local written high-water solely to prevent an
immediate duplicate, and never advances the committed cursor or makes the entry
trim-eligible. A restart in the post-archive-fsync/pre-cursor-fsync gap may
replay, as stated in the non-goals.

**Tests:** Redis rejects every write, cursor persist failure, archive open/write
failure, process-local replay without duplicate content, project namespace
separation, migration, corrupt state, and stale legacy-hash independence.

### 8.3 New dependent leaf: trim archived output and collect stale cursors

**Dependency:** blocked by #612. Trimming cannot safely land before archival has
a positive durable-write signal and Redis-independent cursor.

After a cursor file has been durably persisted in the archive-fsync-first order
defined by #612, trim with `XTRIM MINID` only up to the older of the archived
cursor and the 128th-newest stream entry. This preserves late-dashboard
scrollback while allowing `XTRIM` to reclaim memory during OOM. Trimming reads
only the committed durable cursor and can never outrun the preceding archive
and cursor fsyncs.

Never trim when archiving is disabled, the entry was skipped or dropped, local
cursor persistence failed, or cursor state is corrupt. Trim failure logs and
retries without blocking archival.

Prune local cursor records for streams absent longer than 32 hours (the 8-hour
output TTL plus generous restart/archive lag) and remove corresponding legacy
Redis hash fields. Emit transition-only warnings for trim or GC failure.

**Tests:** retained tail, no trim past cursor, archive-disabled behavior,
write/open failure, OOM-compatible trim, dashboard late attach, cursor GC, and
trim retry.

## 9. Workstream D — Provider streams and provider tooling

### 9.1 Update #613: live-consumer-aware stranded detection

**New title:** `Alert when a work-bearing provider stream has no live heartbeat-backed consumers`

The detector composes, rather than directly reuses, the existing primitives:

```text
ProviderStreamHealth(
  stream,
  provider,
  pending,
  lag,
  registered_consumers,
  live_heartbeat_consumers,
  state
)
```

A stream is stranded when `(pending > 0 or lag > 0)` and there are zero
heartbeat-backed consumers. Registered-but-dead consumers do not count as live.
Use configured provider stream names, not a hard-coded prefix or rollout
revision gate.

Require a 300-second dwell. Emit one structured error on transition to stranded
and one info record on recovery. Redis read errors produce `unknown`, preserve
the previous state, and never emit a false recovery. The pool manager publishes
its canonical snapshot as a TTL-backed, unprefixed per-provider JSON state key
containing the fields above plus observation and transition timestamps.
`orcest status` reads that snapshot and renders a prominent warning; it does
not recompute health or emit transitions. The existing aggregate queue-depth
display remains.

**Non-goals:** task deletion, provider reselection, pending-marker release, and
CloudEvent fan-out. The pool manager is the single canonical transition owner
to avoid one alert per project orchestrator.

### 9.2 Update #616: Codex 0.149.1 and parser contract

**New title:** `Bump Codex CLI to 0.149.1 and refresh its JSON contract`

Update both pins and add a test that fails when the shell and Python literals
diverge. Switch Orcest from the accepted-but-hidden `--experimental-json` alias
to documented `--json`. Capture fresh successful, tool-use, and ordinary
failure JSONL fixtures from 0.149.1. Keep deterministic synthetic rate-limit,
overload, exhaustion, and authentication-refresh fixtures, label their
provenance in the test data, and validate those parser paths plus summary,
NEEDS_HUMAN, and prompt-via-stdin behavior. This fleet-runnable leaf ends with
the implementation PR. Its closing comment links the already-filed manual
rebake/smoke issue as the release handoff; closing #616 satisfies that issue's
blocker but does not claim the template was deployed. The manual leaf below
owns rebake and live smoke verification.

The new CLI source explicitly accepts `max`, `ultra`, and unknown custom effort
strings. No Orcest reasoning-effort flag is added.

**Non-goal:** automated future upgrades; §9.4 owns freshness.

### 9.3 New manual leaf: rebake and smoke Codex 0.149.1

**Title:** `Ops: rebake Codex 0.149.1 and smoke one real task`

**Dependency:** blocked by #616. This issue is intentionally not
`orcest:ready` because it needs template-publishing authority and authenticated
runtime access that a fleet issue worker does not have.

After #616 merges, record the current active template as the rollback target
before rebaking. Then rebake the worker template, verify the new active template
contains Codex 0.149.1, and run one real task that exercises JSON output and a
tool call. Record the new template identifier, CLI version, and task result
before closing. Roll back the active template on smoke failure without
reverting unrelated backlog work.

### 9.4 New dependent leaf: provider-pin freshness tracking

**Dependency:** blocked by the manual Codex rebake/smoke leaf so the active
Codex incompatibility is fixed and verified before maintenance automation is
introduced.

Add a weekly, non-deployment workflow that:

- asserts duplicated in-repository pins and checksums agree;
- compares the stable npm releases for Claude and Codex with their pins;
- verifies the pinned Grok artifact is still downloadable and matches its
  digest, without claiming a latest version when xAI exposes no authoritative
  machine-readable stable channel;
- creates or updates one deduplicated maintenance issue per stale provider,
  recording pinned version, observed stable version, first-seen date, and the
  provider-specific fixture/smoke checklist;
- never edits a pin, merges a PR, rebakes, or closes an incompatibility issue
  automatically.

Network or registry failure is reported as workflow infrastructure failure, not
as a stale provider.

## 10. Workstream E — Credential and skill contracts

### 10.1 Update #535: remove misleading API-key support

**New title:** `Remove misleading ANTHROPIC_API_KEY support claims and document OAuth migration`

The current task schema carries a credential value but not its source variable
or authentication kind. Resolving `ANTHROPIC_API_KEY` in the orchestrator and
then injecting that value as `CLAUDE_CODE_OAUTH_TOKEN` is not a sound
compatibility contract. Broad worker inheritance would reintroduce cross-
provider secret exposure.

Remove `ANTHROPIC_API_KEY` from Claude credential candidates and from README and
environment examples. Add an explicit migration note: inherited-only users must
configure `CLAUDE_CODE_OAUTH_TOKEN`. When configuration contains only the old
variable for a Claude-enabled deployment or task path, fail validation with
that migration instruction instead of silently publishing an unusable task or
treating the deployment as credential-free. An unrelated
`ANTHROPIC_API_KEY` elsewhere in the environment is not by itself an error.

The repository Actions secret `CLAUDE_CODE_OAUTH_TOKEN` was confirmed present
on 2026-08-24 (secret value was not and must not be read). The repository
therefore already provisions the supported secret name for Actions; this fact
does not prove which jobs consume it. The issue aligns application
configuration and documentation without rotating or exposing that secret.

**Tests:** explicit OAuth token, old-key-only migration error, no cross-provider
environment exposure, and documentation/config examples in agreement.

### 10.2 Update #588: manual harness smoke tests

**New title:** `Smoke-test spec skill discovery and activation in Gemini CLI and Cursor`

Code-layout validation is complete: both harnesses support `.agents/skills`
directly. Correct the Gemini acceptance path to `/skills list`, a matching user
prompt, consent to `activate_skill`, and confirmation that the canonical body
governs behavior. For Cursor, confirm discovery through `/` search and canonical
instruction loading.

Update the Windows caveat to explain that a broken provider-specific symlink
does not prevent Gemini or Cursor from discovering `.agents/skills`; retain the
warning for harnesses that depend on their provider directory.

This remains a manual issue without `orcest:ready` because authenticated Gemini
and Cursor runtimes are unavailable in the worker environment.

## 11. Backlog housekeeping

- **#542:** add `security` and `code-review`; comment that PR #551 replaced the
  mutable installer with a pinned, digest-verified binary; close completed.
- **#546:** add `security` and `code-review`; comment that PR #551 implemented
  cloud-init digest verification and tests; close completed.
- **#547:** add `duplicate` and `security`; comment that it duplicates #546 and
  both were resolved by PR #551; close as not planned/duplicate.

No new Grok-integrity issue is needed.

## 12. Dependency and failure semantics

- Shared consumer-group inspection blocks only the worker-startup leaf and
  #611. It does not block #610's fleet-side idle detection.
- #612 blocks trimming because trim safety depends on durable cursoring.
- #616 blocks the manual Codex rebake/smoke leaf, which in turn blocks
  provider-pin freshness.
- #613 is defense in depth and is not blocked by #610 or #611.
- The epic is never a blocker.
- Blocked leaves retain `orcest:ready`; Orcest's dependency resolver defers them
  until their work blockers close. Resolver blocking depends on an open native
  or same-repository body edge, not on whether the blocking issue itself has
  `orcest:ready`; a manual blocker therefore still defers its dependent.
- GitHub work dependencies use resolver state rather than a terminal label.

The installed `gh` is 2.45.0 and lacks `--blocked-by` and `--parent`. Filing
therefore uses `gh api graphql` for native `addBlockedBy` and `addSubIssue`
relationships; both mutations and their input fields were verified by schema
introspection on 2026-08-24. Same-repository `Blocked by #N` body lines remain
mandatory and are sufficient for resolver safety during filing. Epic hierarchy
is best-effort; the epic's child index remains authoritative if sub-issue
attachment fails.

### Dependency-safe filing order

1. Create the unblocked shared consumer-group issue first and retain its number.
2. For reused issues gaining blockers, remove any pre-existing `orcest:ready`
   first, then rewrite the body with every required `Blocked by #N` line.
3. Create new dependent leaves with their blocker body lines but without
   `orcest:ready`. Create the manual Codex rebake/smoke leaf before pin
   freshness so its number is available for the latter's body.
4. Add native blocked-by relationships, then read back and verify both native
   edges and body lines. A failed edge is retried and remains unready until both
   representations exist.
5. Apply `orcest:ready` only after that verification. Unblocked leaves may be
   labeled as soon as their final bodies are present.
6. Create the tracking epic last, after every leaf number is known; write its
   complete child index, then attach best-effort native sub-issues.

This ordering prevents a dependency-gated issue from becoming fleet-selectable
even briefly before the resolver can see its blocker.

## 13. Test strategy

Each leaf owns focused tests listed in its section and must run its affected
suite. Cross-cutting completion checks are:

- `make lint`
- `make test-unit`
- fleet tests for pool-manager changes
- orchestrator tests for startup, stream health, and trace archival
- worker tests for startup, output publication, and Codex integration
- a real Codex smoke task in the manual rebake/smoke leaf
- authenticated manual Gemini and Cursor checks for #588

Tests must assert negative behavior as well as the happy path: non-OOM failures
remain loud, unknown liveness does not destroy, failed archive writes do not
advance/trim, and maintenance workflows never auto-upgrade.

## 14. Migration, rollout, and rollback

Future implementation rollout order follows the issue phases, not this planning
session:

1. Merge urgent independent Phase 1 leaves and the shared primitive, then its
   worker-startup dependent.
2. After #616 merges, complete the separately tracked manual rebake and smoke;
   pool-manager/orchestrator changes deploy through the normal fleet path.
3. Land caller integrations after the shared primitive.
4. Prefer landing #612 before #611 so early archiver startup cannot amplify
   retry duplicates while Redis refuses legacy cursor writes. This is soft
   ordering, not a hard dependency: if #611 lands first, duplicates are an
   accepted temporary tradeoff for preventing the bootstrap crash-loop.
5. Land local trace cursoring before trimming.
6. Enable scheduled freshness only after the Codex rebake/smoke issue closes.

Mixed-version safety requirements:

- A new read-first group helper must preserve the old creation behavior when a
  group is absent.
- New idle detection starts with an empty dwell map and therefore cannot reap
  immediately after deploy.
- Local cursor migration reads the legacy Redis hash once and does not delete it
  until the dependent GC work lands.
- New output append+TTL behavior retains the current stream/chunk format.
- New reap-event fields are additive and optional.

Rollback is by reverting the individual implementation PR or disabling the new
detector/configured dwell where the leaf introduces a knob. Redis size and
eviction policy remain unchanged.

## 15. Operations and observability

Operators must be able to distinguish:

- Redis unreadable, Redis readable-but-write-refusing, and Redis recovered but
  still inside the settle window;
- an idle worker in boot grace, a confirmed dead worker, and a replacement
  breaker that has opened;
- a provider stream with registered-but-dead consumers from one with live
  heartbeat-backed consumers;
- trace archival failure, cursor persistence failure, and trim failure;
- known first-fence telemetry from post-restart unknown telemetry;
- a stale provider pin from an upstream registry outage.

Transition-based logs must include the relevant VM, stream, provider, dwell,
queue depth, or pinned/observed version. Per-pass log spam is not acceptable.

## 16. Completeness review

Every validated issue is represented:

- Updated and retained: #535, #588, #604, #610–#616.
- Closed: #542, #546, #547.
- New scopes required by validation: shared group inspection, worker startup
  retry, manual Codex release verification, archived-output trimming/cursor
  GC, provider-pin freshness, and the tracking epic.

**Forcing question:** List everything this system touches that still has no
spec section.

**Answer:** Nothing in the approved scope. Runtime behavior, internal data
shapes, error paths, tests, dependencies, migration, rollout, rollback,
observability, documentation, manual verification, and backlog mutations are
covered. Automatic stranded-task recovery, API-key authentication, cross-
restart fence persistence, and exact-once host-crash archival are explicit
non-goals rather than silent omissions.

## 17. Coverage table

| Spec section | Issue(s) | Verb | Phase |
|---|---|---|---|
| 1–3 Purpose, relationship, goals | Tracking epic; all leaves | include | all |
| 4 Non-goals | Epic and each affected leaf's Non-goals section | cut | all |
| 5 Backlog structure | Tracking epic | include | all |
| 6.1 Shared group inspection | New `Redis: inspect consumer groups read-first before XGROUP CREATE` | include | 1 |
| 6.2 Worker startup | New `Worker: retry consumer-group bootstrap through Redis maxmemory OOM` | include | 1 |
| 6.3 Idle liveness | #610 | include | 1 |
| 6.4 Orchestrator startup | #611 | include | 2 |
| 7.1 Reap floor | #615 | include | 1 |
| 7.2 Reap telemetry | #614 | include | 2 |
| 8.1 Atomic output TTL | #604 | include | 2 |
| 8.2 Archive cursor | #612 | include | 2 |
| 8.3 Trim and cursor GC | New `Trace archiver: trim durably archived output and garbage-collect stale cursors` | include | 3 |
| 9.1 Stranded stream alert | #613 | include | 2 |
| 9.2 Codex bump | #616 | include | 1 |
| 9.3 Codex rebake/smoke | New `Ops: rebake Codex 0.149.1 and smoke one real task` | include/manual | 1/manual |
| 9.4 Pin freshness | New `Ops: detect stale provider CLI pins without auto-upgrading` | deprioritize | 3 |
| 10.1 Claude credential contract | #535 | deprioritize | 3 |
| 10.2 Harness smoke tests | #588 | deprioritize | 3/manual |
| 11 Housekeeping | #542, #546, #547 | include/close | immediate |
| 12 Dependencies | Native blocked-by plus body lines; epic index | include | all |
| 13 Test strategy | Every implementation leaf | include | all |
| 14 Migration and rollout | Epic summary and relevant leaf acceptance | include | all |
| 15 Operations | #610–#614, new trim leaf, new freshness leaf | include | 1–3 |
| 16 Completeness | Tracking epic | include | all |

Every in-scope section maps to at least one issue, an explicit cut, or an
immediate close action. There are no approved deferrals.

## 18. Gate 2 issue-graph dry run

Every fleet-runnable leaf receives `orcest:ready`, including dependency-gated
leaves after the safe filing sequence in §12 has verified both blocker
representations. The dependency resolver holds those leaves. The tracking epic
and the two manual leaves do not receive `orcest:ready`.

| Title | Repo | Type | Labels | Blocked by | Phase |
|---|---|---|---|---|---|
| `Epic: remediate the validated Redis and provider backlog` | ThayneStudio/orcest | new epic | `enhancement`, `important` | — | — |
| `Redis: inspect consumer groups read-first before XGROUP CREATE` | ThayneStudio/orcest | new leaf | `bug`, `critical`, `orcest:ready` | — | 1 |
| `Bump Codex CLI to 0.149.1 and refresh its JSON contract` (#616) | ThayneStudio/orcest | reused leaf | `bug`, `important`, `orcest:ready` | — | 1 |
| `activity_stale still has no elapsed-time floor` (#615) | ThayneStudio/orcest | reused leaf | `bug`, `critical`, `orcest:ready` | — | 1 |
| `Detect and replace idle VMs whose worker heartbeat never appears` (#610) | ThayneStudio/orcest | reused leaf | `bug`, `critical`, `orcest:ready` | — | 1 |
| `Alert when a work-bearing provider stream has no live heartbeat-backed consumers` (#613) | ThayneStudio/orcest | reused leaf | `bug`, `important`, `orcest:ready` | — | 2 |
| `Preserve kill-time elapsed and killed_at across reap coordination retries` (#614) | ThayneStudio/orcest | reused leaf | `bug`, `important`, `orcest:ready` | — | 2 |
| `Make trace archival cursoring loss-safe under Redis write refusal` (#612) | ThayneStudio/orcest | reused leaf | `bug`, `important`, `orcest:ready` | — | 2 |
| `Batch worker output stream writes with TTL updates` (#604) | ThayneStudio/orcest | reused leaf | `bug`, `code-review`, `minor`, `orcest:ready` | — | 2 |
| `Remove misleading ANTHROPIC_API_KEY support claims and document OAuth migration` (#535) | ThayneStudio/orcest | reused leaf | `bug`, `documentation`, `code-review`, `minor`, `orcest:ready` | — | 3 |
| `Smoke-test spec skill discovery and activation in Gemini CLI and Cursor` (#588) | ThayneStudio/orcest | reused manual leaf | `documentation`, `code-review`, `help wanted`, `minor` | — | 3/manual |
| `Worker: retry consumer-group bootstrap through Redis maxmemory OOM` | ThayneStudio/orcest | new leaf | `bug`, `critical`, `orcest:ready` | new read-first group issue | 1 |
| `Make orchestrator bootstrap degrade gracefully during Redis noeviction OOM` (#611) | ThayneStudio/orcest | reused leaf | `bug`, `important`, `orcest:ready` | new read-first group issue | 2 |
| `Trace archiver: trim durably archived output and garbage-collect stale cursors` | ThayneStudio/orcest | new leaf | `enhancement`, `important`, `orcest:ready` | #612 | 3 |
| `Ops: rebake Codex 0.149.1 and smoke one real task` | ThayneStudio/orcest | new manual leaf | `enhancement`, `important`, `help wanted` | #616 | 1/manual |
| `Ops: detect stale provider CLI pins without auto-upgrading` | ThayneStudio/orcest | new leaf | `enhancement`, `minor`, `orcest:ready` | new Codex rebake/smoke issue | 3 |

All fifteen leaves are represented. Thirteen receive `orcest:ready`; #588 and
the Codex rebake/smoke leaf are manual. Four ready leaves are dependency-gated:
the worker-startup leaf and #611 by the new read-first group issue, trimming by
#612, and pin freshness by the manual rebake/smoke issue. The manual
rebake/smoke leaf is itself blocked by #616.

## 19. Immediate close actions

| Issue | Labels to add | Close reason | Closing comment |
|---|---|---|---|
| #542 | `security`, `code-review` | completed | Superseded by PR #551's pinned, digest-verified Grok binary path. |
| #546 | `security`, `code-review` | completed | Implemented by PR #551 in cloud-init with checksum and regression coverage. |
| #547 | `duplicate`, `security` | not planned | Duplicate of #546; both technical scopes were resolved by PR #551. |
