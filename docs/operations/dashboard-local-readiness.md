# Local dashboard readiness milestone

Approved by Austin on 2026-09-17 as the next milestone of the existing dashboard
hardening goal. Local development uses the isolated fake-fleet harness and does
not depend on Tailscale connectivity.

## Objective

Make the dashboard ready for a separately validated fleet rollout by completing
interactive browser QA, sustained local validation, and PR review/CI. Preserve
both the real sign-in/output path and the distinction between synthetic evidence
and live-fleet qualification. Keep production services and Cloudflare unchanged.

## Completion criteria

- [ ] Browser QA: card details, incrementally streamed output, project/search
  filters, mobile layout, sign-in/logout, and understandable outage/stale states.
  Record findings, fix defects, and verify fixes in the browser.
- [ ] Sustained local validation: record timestamped samples and exercise
  reconnects, normal session expiry, memory behavior, and stale observations.
  Preserve interruptions and failures in durable evidence; do not silently
  restart or count gaps as healthy time.
- [ ] Open a PR containing the local harness and dependency patch; address review
  findings and obtain passing CI for the final candidate. Report remaining
  limitations and link the validation evidence.

## Current evidence

The harness is implemented and runs the real dashboard server, observation
writers, isolated Redis, sign-in/session handling and output WebSocket. The
805-test dashboard suite passes. The harness acceptance check passes dependency
progression, live output, provider cooldown recovery, Redis outage/recovery,
dashboard restart/session invalidation, reset and logout. These checks do not
substitute for the interactive and sustained validation above.

Implementation is on `codex/dashboard-validation-resume`, including commits
`ebb4876` (dependency patch and readiness record) and `0937c9f` (local harness).
See [local harness instructions](../dashboard-local-harness.md).

## Separate rollout gates

- Restore access to the Orcest tailnet and reconnect read-only live data.
- Complete a fresh 24-hour live-fleet observation with representative execution
  and independently verified delivery. The previous interrupted interval and
  synthetic harness activity cannot qualify this gate.
- Validate deployment against the current fleet, preserving rollback inputs and
  existing task ownership, before any live rollout.

The broader hardening goal remains incomplete until its required gates pass.
Draft [PR #833](https://github.com/ThayneStudio/orcest/pull/833) contains this work.
The first candidate passed required CI checks; review fixes need their own final
CI result. The [local review report](dashboard-harness-review.md) records findings
and verification. Automated GitHub review has not posted an approval.
The local acceptance run uses a thirteen-hour, separately pinned harness so normal
twelve-hour session expiry can be observed. The collector smoke passed 24 samples
and 21 output cursor reconnect checks over two minutes with zero errors; it does
not qualify the longer run. Browser control became available on September 18 UTC; see the validation
record below. These items remain open until all required evidence is available.


## Restored remote validation — September 18, 2026 UTC

The active goal now includes real-fleet validation through the restored management
host connection. The initial candidate at `http://localhost:4319` reads the same
four-project scope as the deployed dashboard through a loopback SSH tunnel.
Production containers, scheduling, worker VMs, and Cloudflare remain unchanged.
Credentials stay in private local configuration outside this repository.

Browser checks on candidate `b64c619` verified sign-in, sign-out, scoped project
and search results, exceptional attention items, completed task context, attempt
history and retained output. The fleet view distinguishes three configured
provider accounts from the pool and four worker records. Missing provider usage
is shown as **Not reported**. Fleet and task detail layouts were inspected at
390 × 844 as well as desktop size. Synthetic incremental output also worked.
Real incremental output still needs an active execution; the fleet was idle
during these checks. Interactive Redis outage/recovery passed in the isolated harness; no outage was
injected into the real fleet.

A filter race found during browser QA is fixed in this PR: old cards are hidden
while the selected project/search request is pending, and superseded responses
cannot replace newer results. Regression tests cover delayed responses and failed
project changes. Browser verification in the isolated harness confirmed the
updating message, matching recovered results, and no unrelated cards during a
filter change. The full dashboard suite now passes 807 tests; type checks and
the production build also pass.

Initial observation record (superseded by the update below):

- Synthetic: `local-soak-20260917-v2`, started September 17 at 21:56:35 UTC,
  planned thirteen hours. At September 18 02:15 UTC it had 260 samples, 211
  reconnect checks, stale/recovery evidence and zero failures. Normal session
  expiry had not yet occurred.
- Real fleet: `live-validation-20260918/observation`, started September 18 at
  02:11:26 UTC, planned twenty-four hours. Initial samples passed. New execution
  and independently verified delivery are still required; idle elapsed time
  cannot qualify this gate. The collector requires a separate evidence audit.

Both runs pin `b64c619`; they do not silently adopt subsequent browser fixes.
Audit their artifacts and revision coverage before attributing evidence to a
final candidate. Preserve any restart or interruption as a separate interval.
Required CI passed for `b64c619`; subsequent commits require fresh checks.


### Current validation interval and worker detail fix

The current real-data candidate is `http://localhost:4320`, pinned to `61f10be`.
The first real interval was stopped to expand evidence collection. Its successor
failed at the third sample because work coverage was incomplete; the exact cause
was not captured. A subsequent five-minute diagnostic was healthy. Both intervals
are retained and excluded from the fresh interval, `live-validation-20260918-v3`,
which started September 18 at 02:43:23 UTC. At 03:08 UTC its 26 samples had no
failures; new execution, incremental output and delivery were still unobserved.
The synthetic interval continues independently: 313 samples, 251 reconnect checks
and zero failures at 03:08 UTC. Neither interval qualifies until its full duration,
session-expiry and workload requirements pass a separate audit.

Current real-candidate browser QA confirmed the filter loading state, matching
results, PR context and keyboard focus restoration. A further browser-path defect
was found: a worker's **View work** link bypassed detail initialization when its
work was outside the filtered results. The link now uses the normal initialization,
opens Output for executing work, clears prior selection state, and restores focus
when closed. A regression test failed before the fix and passed afterward. Browser
QA with all work filtered out verified live simulated output and focus restoration.
All 808 dashboard tests, type checks covering 102 files, and the production build
pass. Required CI passed for `61f10be`; this additional fix requires fresh CI.

The current browser fix does not change server code. Compare built server artifacts
before carrying forward server-only endurance evidence; neither synthetic activity
nor historical deliveries satisfy the fresh real-execution gate.
