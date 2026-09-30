# Local harness review — 2026-09-17

## Review summary

Twelve changed files reviewed across three independent groups. Fixed: zero
critical, five important and five minor findings. This local review does not
represent a GitHub approval. The first two automated GitHub review attempts
failed internally without posting a review despite green workflow conclusions.

## Critical issues

None.

## Important issues (fixed)

- `dashboard/scripts/check-local-harness.mjs:20` and
  `dashboard/scripts/soak-local-harness.mjs:130`: spawn errors and malformed
  output could escape cleanup. Asynchronous errors now enter normal failure
  handling.
- `dashboard/scripts/soak-local-harness.mjs:57`: interruption during cleanup
  could preserve qualification. Interruptions wake sampling, clear qualification
  and return a nonzero exit status.
- `dashboard/scripts/soak-local-harness.mjs:77` and
  `dashboard/scripts/check-local-harness.mjs:115`: setup and timeout paths could
  leave owned processes behind. Setup is protected by cleanup; shutdown allows
  the supervisor's full cleanup duration before terminating its own process group.
- `dashboard/scripts/soak-local-harness.mjs:135`: reconnect checks lacked cursor
  validation. Require valid, advancing cursors and reject premature closure.
- `dashboard/scripts/soak-local-harness.mjs:152`: a final unsampled suspension
  could count toward the requested duration. Require measured elapsed time, a
  final sample, gap validation and final artifact checks.

## Minor issues (fixed)

- `dashboard/package.json:10`: quote interpreter paths containing spaces.
- `dashboard/scripts/local-harness.py:285`: reject non-finite intervals.
- `dashboard/scripts/local-harness.py:302`: avoid interpolating unescaped data
  directory names into Redis configuration; start Redis in that directory.
- `dashboard/scripts/soak-local-harness.mjs:96`: measure session age from the
  login request to avoid falsely reporting a normal expiry as early.
- `docs/operations/fleet-hardening-readiness.md:25`: distinguish historical live
  connectivity from the restored local candidate awaiting access.

## Issues requiring discussion

None from code review. Interactive browser QA and the sustained observation
remain acceptance gates, and the live-fleet gate remains separate.

## Files with no findings

- `dashboard/package-lock.json`
- `dashboard/server/workView.ts`
- `dashboard/src/FleetDashboard.dom.test.tsx`
- `dashboard/src/FleetDashboard.tsx`
- `dashboard/src/lib/workTypes.ts`
- `docs/dashboard-local-harness.md`
- `docs/operations/dashboard-local-readiness.md`

## Verification

Nine isolated collector control-flow regressions passed, including interruption
during cleanup and a sampling gap crossing the thirteen-hour cutoff. A spawn
failure cleanup check passed. Real isolated Redis accepted directory names with
quotes, backslashes and newlines; non-finite intervals were rejected. An
interpreter path containing spaces worked. Lint and formatting checks passed.

The dashboard suite passed all 805 tests and type checking covered 102 files
across three projects after integration. An initial sandboxed invocation could
not bind loopback test ports; its failures are retained separately from the
successful invocation with loopback access.

The preliminary sustained interval observed source staleness and recovery with
no recorded application errors, then was deliberately interrupted to adopt these
collector fixes. It does not qualify or contribute time to the fresh interval.
