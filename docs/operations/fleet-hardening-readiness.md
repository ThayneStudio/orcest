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
| Review findings | Every comment fixed or explicitly explained; review of final candidate | Original PR fixes implemented and dispositions recorded; candidate review pending |
| Supported runtime | Supported LTS Node, consistent build/deploy pins, type checks and image smoke | Node 24.20.0 passes local checks; candidate container authentication/assets/bundle smoke passed |
| Dependency security | Audit locked dependencies; no unexplained high/critical findings | Updated lockfile reports zero npm advisories on 2026-09-05 |
| Data correctness | Expiration, partial discovery, stale observations, completion evidence, account/worker distinction | Dashboard regression suite passes; live candidate validation pending |
| Session/output behavior | Expiry, logout, token rotation, reconnect, bounded connections and output queues | Existing tests cover these boundaries; local process rehearsal passed |
| Queue recovery | Crash before ACK, durable pending state, replay, ownership safety under concurrency | Existing unit and real-Redis tests identified; local process rehearsal passed; managed Redis integration/concurrency: 12 passed |
| External outages | GitHub/Redis failures pause safely and recover without lost outcomes or repeated effects | Unit result-replay coverage passes; local process rehearsal passed |
| Restore and rollback | Restore data into an isolated instance; verify candidate rollback with exact artifacts | Local isolated restore rehearsal passed; prior rollout evidence must be checked for applicability |
| Sustained operation | 24 hours of timestamped measurements with representative work and no unexplained failure | Not started |

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
