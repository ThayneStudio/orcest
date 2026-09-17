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
not qualify the longer run. Interactive browser QA is awaiting macOS Computer
Use permissions. These items remain open until evidence is available.
