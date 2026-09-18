# Fleet hardening readiness

Started 2026-09-05. This is the execution record for the approved hardening goal,
not a declaration that the fleet is ready for broader access.

## Scope and release boundary

Keep browser access on localhost and leave Cloudflare routes unchanged. Use
synthetic data and isolated services for destructive recovery tests. Validate
candidate changes before replacing any live service. Preserve task ownership,
credentials, and rollback inputs during any eventual rollout.

The previous dashboard release demonstrated a real issue-to-PR-to-merge flow and
a 60-minute live watch. That evidence does not substitute for the recovery
scenarios or 24-hour observation below. Workflow v1 remains a separate capability;
its store tests do not prove recovery of the currently deployed legacy queue.

## Readiness gates

| Gate | Evidence required | Current state |
| --- | --- | --- |
| Review findings | Every comment fixed or explicitly explained; review of final candidate | PR #817 approved with no blocking findings and merged; post-merge CI passed |
| Supported runtime | Supported LTS Node, consistent build/deploy pins, type checks and image smoke | Node 24.20.0 passes local checks; candidate container authentication/assets/bundle smoke passed |
| Dependency security | Audit locked dependencies; no unexplained high/critical findings | Updated lockfile reports zero npm advisories on 2026-09-05 |
| Data correctness | Expiration, partial discovery, stale observations, completion evidence, account/worker distinction | Dashboard regression suite passes; previous candidate read live scoped data successfully; restored candidate awaits fleet connectivity |
| Session/output behavior | Expiry, logout, token rotation, reconnect, bounded connections and output queues | Existing tests cover these boundaries; local process rehearsal passed |
| Queue recovery | Crash before ACK, durable pending state, replay, ownership safety under concurrency | Existing unit and real-Redis tests identified; local process rehearsal passed; managed Redis integration/concurrency: 12 passed |
| External outages | GitHub/Redis failures pause safely and recover without lost outcomes or repeated effects | Unit result-replay coverage passes; local process rehearsal passed |
| Restore and rollback | Restore data into an isolated instance; verify candidate rollback with exact artifacts | Local restore and real-script rollback rehearsals passed; deployment-specific validation remains |
| Sustained operation | 24 hours of timestamped measurements with representative work and no unexplained failure | Interrupted after 14h34m on 2026-09-06; a new full interval is required |

## PR 814 comment dispositions

- Escalation state, capped-discovery reconciliation, legacy login race, proxy
  rate limiting, and refreshed credential redaction were fixed in PR 814.
- Cookie transport handling now uses Express's configured proxy trust for both
  session and legacy cookies. Compose forwards `DASHBOARD_TRUSTED_PROXIES`.
- Empty Redis work hashes are omitted before deriving identity. Physical keys
  and attempt indices stay paired. Expiration does not trigger a false inventory
  truncation warning.
- The entry-point ternary and authentication redirect were reformatted.
- The explicit HTML Accept check is retained intentionally: `Accept: */*` from
  CLI clients must receive JSON 401, not a browser login redirect. A regression
  test records this behavior.

## Coverage audit

| Scenario | Existing executable evidence | What it does not prove |
| --- | --- | --- |
| Result publication/ACK interruptions | `tests/worker/test_loop.py`, `tests/orchestrator/test_loop.py` | Most use mocked calls or fakeredis rather than killing VMs |
| Shared provider capacity and expired leases | `tests/integration/test_provider_capacity_routing.py` | Executed under the invocation-owned Redis harness; 12 integration/stress scenarios passed |
| Replacement consumer recovery | `tests/integration/test_mixed_provider_streams.py` | Exercises Redis clients, not complete VM replacement |
| Concurrent claims/locks | `tests/stress/test_concurrent_workers.py` | Simulated worker threads, not paid agent runs |
| Real subprocess liveness | `tests/worker/test_runner_watchdog_integration.py` | Synthetic provider scripts, not provider service availability |
| Browser sign-in and output | Dashboard HTTP/WebSocket tests and `check-fleet-e2e.mjs` | Not a 24-hour browser session or fleet soak |

Local validation after the initial review fixes: 804 dashboard tests passed;
type checks, production build, bundle smoke and Python-writer/Redis/dashboard
end-to-end flow passed. Focused coordinator/worker recovery unit suite: 402
passed, one skipped, one integration test deselected. The skip is an obsolete dead-letter lock-order
test tracked by issue #398; current terminal handoff and failed-handoff tests run. Node 24 and the dependency
updates also passed the dashboard suite, type checks, build and Redis E2E.

The dependency update moves off Node 20, which is end-of-life according to the
[Node release schedule](https://nodejs.org/en/about/previous-releases). Parser
overrides select patched `body-parser` and `qs` releases where Express's
transitive ranges would retain vulnerable versions. Revisit these overrides
when Express's dependency ranges incorporate the fixes.

## Process rehearsal

After building the dashboard, run from the repository root:

```sh
ORCEST_TEST_REDIS_SERVER=/path/to/redis-server \
  .venv/bin/python dashboard/scripts/check-recovery.py
```

Use the pinned Node runtime on PATH. This script creates its own loopback Redis
and dashboard processes and synthetic work. It kills a claimant before ACK,
crashes Redis with AOF enabled, checks unavailable/readiness responses, recovers
pending work, restores an RDB into another owned instance, and verifies session
invalidation on dashboard restart. It never reads deployment credentials or
targets a configurable external service. A bounded two-second read cache may
temporarily return the last successful work response during an outage.

## Sustained observation acceptance

Start only after the candidate revision and scenario results are recorded.
Record revision identities, service restarts, worker liveness, queue/PEL age,
work progression, account availability, dashboard errors/latency, and process
memory at a fixed cadence. Preserve sample gaps as gaps, not healthy samples.
Include representative task activity; idle uptime alone does not pass the gate.
Distinguish expected CI/dependency/usage waits from stalled coordination.

The final record must list unresolved findings and their dispositions, recovery
and restore results, workload exercised, and the full observation interval.
Do not report the goal complete while required work or observation remains.

The local process rehearsal passed all five checks on 2026-09-05. The watchdog
subprocess suite timed out on macOS, where Linux `/proc` sampling is unavailable;
that run was stopped and is not counted as passing. The isolated Linux run passed all nine scenarios.

The first Node 24 container smoke caught a read-only `navigator` global in the
browser smoke helper. The helper now installs its fixture with `defineProperty`;
the Node 24 bundle smoke passes. No production browser code changed for this fix.

## Rollback rehearsal result

On 2026-09-05, a dedicated local Docker VM ran the real
`dashboard/scripts/deploy-compose-dashboard.sh` against a uniquely named,
synthetic Compose project. The previous dashboard was built from merged commit
`27b9c5648500121e772940a8909f128ff82ac606` (Node 20); the candidate was built
from `9dcca93` (Node 24). An invalid candidate Redis port forced published
readiness to fail after replacement.

The script rejected the candidate and restored the exact previous image ID and
last-known-good Compose configuration. The restored dashboard reported its
baseline revision and successful readiness. Redis kept the same container ID,
a sentinel value, and the synthetic queued task. The candidate configuration
file was restored for correction after rollback. The test project and its
resources were then removed. This is executable rollback evidence for this
runtime transition; it is not a claim that a live rollback or deployment occurred.


## Previous candidate observation (interrupted)

[PR #817](https://github.com/ThayneStudio/orcest/pull/817) received an approving
review with no blocking findings. Orcest merged it as
`306ed138089e5ba7a2b131c63840c13fad617549`. The
[post-merge CI run](https://github.com/ThayneStudio/orcest/actions/runs/34001255401)
passed lint, typecheck, unit, integration, dashboard, Docker image and Compose
smoke checks.

During the previous observation, the merged candidate ran locally on loopback port 4319 under Node 24.20.0,
reading the existing fleet through an SSH tunnel. Production service images and
Cloudflare routes were unchanged during that observation. Runtime source matches the previously
validated `bbcb9b4` build; the later changes affect qualification tooling and
this record. A local SHA-256 manifest pins the running build and browser assets.

Candidate readiness reports the merged revision. Browser sign-in renders all
four projects, four worker heartbeats and three configured provider accounts.
Issue #815 resolves to its verified PR #816, and its completed execution output
renders through the authenticated output transport. HTTP checks verified sign-out
revocation. This retained execution predates the observation window and does not
count as new work during the window.

The observation started at **2026-09-06T02:06:47Z**, with an earliest full-interval
end of **2026-09-07T02:06:47Z**. It samples the local process identity/memory,
readiness, request latency, scoped work transitions, provider availability,
worker liveness, queue depth/age and pending delivery state every minute. Every
five minutes it reads production service identities, start times, restart counts
and health. Hourly disposable sessions verify logout revocation; the long-lived
measurement session must expire normally and reauthenticate after twelve hours.
Sampling gaps remain failures, never backfilled healthy samples. A passing
observation requires new execution and verified completion evidence; naturally
idle fleet time alone cannot qualify it.

The protected local evidence directory is `/private/tmp/orcest-observation-306ed13`.
It contains the observation script, artifact manifest, timestamped samples,
process metadata and eventual result. Its credential file is excluded from all
reports and source control. A half-hourly task follow-up checks the actual
process and evidence, reporting meaningful failures or the final qualification
result. The 24-hour gate remains incomplete until the evidence is audited.


## Observation interruption and local restoration — 2026-09-17

The September 6 observation did **not** qualify. Its last verified sample was
2026-09-06T16:40:47Z: 875 samples over approximately 14 hours 34 minutes, with
zero failed samples, three new execution attempts, and one verified delivery.
The local candidate and collector subsequently disappeared together with their
`/private/tmp` directories. The cause was not established. The raw samples are
unavailable; these figures come from the prior recorded check results and the
preserved interruption report. Do not combine this interval with a future run
or describe it as a completed 24-hour observation.

The candidate was restored into an isolated worktree under a persistent local
validation directory, with Node 24.20.0 verified against the published checksum.
The original checkout and its uncommitted vision documents were preserved.
Current master has no dashboard source differences from the reviewed candidate.
A new advisory affected the locked Vitest test tooling; Vitest and its associated
packages were updated from 4.1.9 to 4.1.11. The dependency audit now reports zero
advisories. This changes development tooling, not the dashboard runtime.

Fresh validation:

- Type checks, production build, and browser bundle smoke passed.
- All 804 dashboard tests passed with the patched test runner.
- The isolated Python writer → Redis → authenticated dashboard → live output →
  CI wait → verified completion → logout contract check passed.
- All five isolated process-recovery checks passed: claimant crash before ACK,
  Redis outage, AOF restart/replay, independent RDB restore, and session
  invalidation after dashboard restart.

The restored candidate binds only `127.0.0.1:4318`. Its generated local access
token is kept in a mode-0600 configuration file outside the repository. Local
sign-in works independently of the fleet data connection. The management host
failed DNS resolution on this Mac, so no fleet tunnel or production credentials
have been restored. The dashboard must report unavailable data until that is
resolved; an empty local dataset is not a substitute for live fleet evidence.
No live service or Cloudflare configuration was changed.

### Resuming the sustained observation

1. Restore the management-host route and read back the actual fleet service
   revisions, dashboard scope configuration, and worker/account inventory.
2. Configure the local read connection through a loopback SSH tunnel; keep
   credentials outside version control with restrictive file permissions.
3. Verify readiness, browser sign-in, scoped work data and authenticated output.
   Pin a manifest of all running build/browser artifacts and record process
   identities before starting the clock.
4. Store samples, logs, manifests, and the final report in persistent storage,
   never a temporary directory. Treat termination, restarts and sample gaps as
   interruptions. A restarted collector begins a new interval.
5. Complete a new full 24-hour interval with representative execution and
   independently verified delivery. Audit the evidence before declaring this
   gate passed or considering fleet rollout.


## Independent local development

The [local harness](../dashboard-local-harness.md) now drives the actual dashboard
with synthetic work using the real Python observation writers, owned Redis,
sign-in and output transports. It needs no live-fleet route or provider accounts.
Development and fault testing can continue while Tailscale is on another tailnet.
The dashboard visibly identifies this environment. Synthetic outcomes never
qualify the live-fleet delivery/observation gate.

The harness acceptance check verifies dependency waits before execution, queue
claiming, streamed output, CI waiting after execution, completion and dependency
release, provider cooldown recovery, Redis unavailability/reconnection, dashboard
restart/session invalidation, reset and logout. The dashboard suite now has 805
passing tests, including the simulation label. Runtime setup and controls are
repeatable from the documented harness command.


## Remote access restored — September 18, 2026 UTC

Read-only access is restored through `pve-test.lab.prefixa.net` to the
orchestrator VM at `10.20.1.129`. The earlier DNS/connectivity failure above is
historical. Live service health and revisions were read back before connecting
the isolated localhost candidate. No fleet service was deployed or restarted.

The new twenty-four-hour observation began at 02:11:26 UTC on September 18.
Its artifacts are stored persistently under `live-validation-20260918`, separate
from the thirteen-hour synthetic run. Neither interval is qualified while still
running. See [current readiness evidence](dashboard-local-readiness.md) for
browser checks, remaining gaps, and pinned-candidate limitations.
