# Local dashboard readiness milestone

## Qualified private rollout — October 2, 2026 UTC

The local milestone and separate dashboard rollout gates are complete. Independent
reviews audited the raw samples, pinned manifests and terminal summaries before
deployment; the published runtime and browser were checked afterward. Earlier
September 18 and interrupted September 30/October 1 intervals remain unqualified
and contribute no time to these completed runs.

| Interval | Completed evidence |
| --- | --- |
| Synthetic: September 30 23:54:52 UTC to October 1 12:54:52 UTC | One unchanged thirteen-hour run; 781 samples, zero failures, 623 cursor reconnect checks, explicit stale/fresh recovery and normal twelve-hour session expiry. Maximum sampled RSS: 101748 KiB. |
| Live: October 1 00:36:10 UTC to October 2 00:36:10 UTC | One unchanged twenty-four-hour run; 1441 samples, zero failures, 1417 complete and 24 partial samples. All 19 stale episodes recovered explicitly under the same IDs; longest approximately 180.14 seconds. Session expiry/reauthentication and final logout revocation passed. |

The live run observed advancing output for fifteen new attempts and independently
verified five merged GitHub heads for the same attempts. Orcest task
`567952bb-bf9a-42a4-bd76-bc7a63a3d4c0` delivered
[PR #839](https://github.com/ThayneStudio/orcest/pull/839), head
`122b76a236a0b531e420ce689580d3455b93c7fa`, merged October 1 at 00:54:30 UTC.
Synthetic outcomes, historical deliveries and idle uptime did not establish
fresh live execution or delivery. Manually completed
[PR #841](https://github.com/ThayneStudio/orcest/pull/841) is separate from this
fleet-delivery evidence; its never-claimed queued task and matching pending
marker were retired through independently reviewed atomic guards.

Qualification covered dashboard `795042202e8037baf9216ed0225f95e46b3bfe82`,
source-only observation producer backport
`8a09f0bd6f4a80b18038c04230e5814cbfa1536a`, and workers
`cb936ecb539fe0af4a5aaafbaf4e262bd575acb1`. This deliberately validated mixed
runtime is not a whole-fleet revision upgrade or workflow v1 adoption.

The exact staged dashboard image
`sha256:23f62f362c6f2dedf81a6a11f4549135100ab6c7fc2d1f9dc968339a3fbbdd54`
started on the fleet host at October 2 15:27:29 UTC without rebuilding. Independent
readback verified all 21 pinned runtime/browser artifacts and served asset
hashes, the full candidate revision above, healthy readiness, unauthorized HTTP 401,
authorized HTTP 200 and the four intended project scopes. Protected old-image
rollback inputs remain intact. See [private access and rollback](../fleet-dashboard.md#qualified-private-release--october-2-2026-utc).

Published browser QA at 15:33:36–15:33:48 UTC passed fourteen checks with no
console errors, page errors or failed requests: sign-in, project filters,
search/details, actual retained output for an executing Transit task, fleet
account/worker/pool distinctions, exceptional blockers, mobile layout and
sign-out. A separate read-only historical lookup probe was OPEN immediately
before logout and closed with code 1008, “Session ended”; the old cookie then
returned HTTP 401. That probe establishes subscription revocation only, not
execution or output. Actual nonempty task output was checked separately.
The first failed browser attempt is preserved: its task socket completed
naturally, so it could not establish logout revocation.

No live provider cooldown was available during browser QA. Synthetic tests
cover that behavior. No destructive outage or VM replacement was injected into
the real fleet. Production v1 pilot, isolation, retirement and backup/restore
observation gates remain unqualified under
[issue #667](https://github.com/ThayneStudio/orcest/issues/667).

Raw samples, manifests, summaries and independent audits are preserved privately
under `/home/thayne/orcest-closeout/`: `synthetic-final-20261001/`,
`live-producer-8a09f0bd-20261001/`, `published-browser-client/` and `evidence/`.
These are private evidence locations, not public artifact links. Credentials,
raw fleet samples and environment files are excluded from this repository.

Independent replay checked raw timing, RSS, physical source observations,
stale-ID recovery and output-cursor progression. Authentication expiry,
reauthentication and logout, full inventory pagination/accounting, and
per-sample process identity/readiness were enforced by assertions in the
unchanged reviewed collector. The evidence does not serialize a complete
HTTP or inventory trace, so those assertion-backed checks are not a claim
that every authentication response or inventory page was independently replayed.

## Historical milestone and validation records

The dated records below preserve original observations and unresolved states at
those times. Their pending gates, deployed revisions and initial local-only
boundary are superseded by the qualification and authorized rollout above.

Approved by Austin on 2026-09-17 as the next milestone of the existing dashboard
hardening goal. Local development uses the isolated fake-fleet harness and does
not depend on Tailscale connectivity.

## Objective

Make the dashboard ready for a separately validated fleet rollout by completing
interactive browser QA, sustained local validation, and PR review/CI. Preserve
both the real sign-in/output path and the distinction between synthetic evidence
and live-fleet qualification. Keep production services and Cloudflare unchanged.

## Completion criteria

- [x] Browser QA: card details, incrementally streamed output, project/search
  filters, mobile layout, sign-in/logout, and understandable outage/stale states.
  Record findings, fix defects, and verify fixes in the browser.
- [x] Sustained local validation: record timestamped samples and exercise
  reconnects, normal session expiry, memory behavior, and stale observations.
  Preserve interruptions and failures in durable evidence; do not silently
  restart or count gaps as healthy time.
- [x] Open a PR containing the local harness and dependency patch; address review
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


## September 30 closeout

PR #833 was independently reviewed by a Codex subagent and merged as
`eeb9555b086d152fb5daec0040f2bae3360b6b52`. Fresh validation passed all
808 dashboard tests, type checks, build, bundle runtime, isolated harness
acceptance, and all five process recovery checks. Claude Code review is
unavailable; its workflow conclusion is not review approval. Tooling and
browser fixes are integrated separately from deployment qualification.

The September 18 intervals have no recovered final qualification report on
this Linux workstation or the inspected fleet host. They remain unqualified.
The deployed dashboard still reports `bf6967085f6b64cd14ad0d884011ac08c7d6583e`;
the localhost candidate reports `c021b2dc4cf5538de84b502bbb507fffb6cdc046` and
reads the intended four projects through an SSH tunnel to `10.20.1.129`.

A new thirteen-hour synthetic interval started at 2026-09-30T23:19:40Z.
The reviewed `dashboard/scripts/observe-live-fleet.mjs` collector provides
separate live qualification: pinned process/artifact identity, normal session
expiry, logout, full project inventory, new execution with advancing output,
and independently verified delivery for the same attempt and observed full
publication head SHA. A missing head SHA stays pending until refreshed; it
cannot establish delivery. Known stale-source
notices are attributed to individual work IDs and physical project-source IDs.
Each stale record must explicitly become fresh under the same ID within ten
minutes; disappearing records never prove recovery. Overlapping episodes can
keep the fleet-wide partial-coverage union open longer, and that duration is
recorded only as a diagnostic. Qualification requires all episodes closed,
at least one complete sample, and the independently intended project scope;
unavailable feeds, unexplained notices, sampling gaps and process changes
fail the interval. Idle uptime alone cannot qualify.

Run the live collector from `dashboard/`, with the pinned Node runtime:

```sh
node scripts/observe-live-fleet.mjs \
  --url http://127.0.0.1:44319 --pid LOCAL_CANDIDATE_PID \
  --revision FULL_CANDIDATE_SHA --token-file PRIVATE_TOKEN_FILE \
  --projects ThayneStudio/orcest,ThayneStudio/transit-platform,bluebamboollc/bbr-platform,dewdropsllc/asemly \
  --state-dir NEW_PERSISTENT_EVIDENCE_DIRECTORY
node --test scripts/live-evidence.check.mjs
```

The token file must contain only the candidate token and have private file
permissions. The collector owns neither the dashboard nor its tunnel and
never alters fleet state. Its evidence contains output metadata, not output
text or credentials. Audit both completed intervals before deployment.
