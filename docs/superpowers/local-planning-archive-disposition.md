# Local planning archive disposition

Reviewed 2026-09-30 against the implementation and GitHub issue states. These
five previously untracked August records are retained for provenance. Their
original statuses and checkboxes describe those sessions; they do not authorize
new work, demonstrate deployment, or define the current backlog. References to
old provider versions and line numbers are historical. No actual credentials,
private keys, or authenticated URLs were identified in the archived content.

| Historical record | Disposition |
| --- | --- |
| [Redis outage specification](specs/2026-08-21-redis-oom-outage-remediation.md) | Preserve incident measurements and refuted hypotheses. Its original backlog was superseded by the August 24 validation below. |
| [Redis outage implementation plan](plans/2026-08-21-redis-oom-outage-remediation.md) | Archive the original decomposition. Do not execute its unchecked steps: later implementation changed the details and landed the fixes. |
| [Validated backlog specification](specs/2026-08-24-validated-backlog-remediation.md) | Archive the corrected decomposition. Closed issues and implementation now supersede its pending-approval status; manual operational checks remain separately tracked below. |
| [Harness engineering specification](specs/2026-08-29-harness-engineering-remediation.md) | Preserve the design rationale. All seven filed implementation leaves are closed with corresponding code on master. |
| [Harness issue creation plan](plans/2026-08-29-harness-engineering-issue-creation.md) | Preserve the verified issue/dependency filing record. Do not refile the issues or repeat its filing procedure. |

## Implementation evidence

This is a repository implementation mapping, not a production qualification.

| Workstream | Merged implementation evidence |
| --- | --- |
| Bound worker output and protect fleet reaping during Redis pressure | [#601](https://github.com/ThayneStudio/orcest/pull/601), [#591](https://github.com/ThayneStudio/orcest/pull/591); worker output limits and fleet health checks |
| Read-first consumer-group inspection and worker OOM bootstrap retry | [#623](https://github.com/ThayneStudio/orcest/pull/623), [#626](https://github.com/ThayneStudio/orcest/pull/626) |
| Idle heartbeat replacement, degraded orchestrator bootstrap, elapsed floor, and preserved reap telemetry | [#624](https://github.com/ThayneStudio/orcest/pull/624), [#638](https://github.com/ThayneStudio/orcest/pull/638), [#630](https://github.com/ThayneStudio/orcest/pull/630), [#625](https://github.com/ThayneStudio/orcest/pull/625) |
| Local trace cursors, safe trimming, atomic output append/TTL | [#628](https://github.com/ThayneStudio/orcest/pull/628), [#632](https://github.com/ThayneStudio/orcest/pull/632), [#633](https://github.com/ThayneStudio/orcest/pull/633); later cursor namespace fixes [#642](https://github.com/ThayneStudio/orcest/pull/642) and [#654](https://github.com/ThayneStudio/orcest/pull/654) |
| Stranded provider stream signals | [#634](https://github.com/ThayneStudio/orcest/pull/634), with later independent PR/issue stream monitoring and persisted recovery fixes |
| OAuth credential contract and historical Codex JSON pin | [#627](https://github.com/ThayneStudio/orcest/pull/627), [#637](https://github.com/ThayneStudio/orcest/pull/637); the pinned version in the August plan is historical |
| Generation-scoped publication, typed delivery verification, completion gate, retry resume | [#663](https://github.com/ThayneStudio/orcest/pull/663), [#699](https://github.com/ThayneStudio/orcest/pull/699), [#708](https://github.com/ThayneStudio/orcest/pull/708), [#723](https://github.com/ThayneStudio/orcest/pull/723); issues #655, #657, #659, #660 are closed |
| Hermetic Redis tests, locked development inputs, canonical check DAG | [#704](https://github.com/ThayneStudio/orcest/pull/704), [#662](https://github.com/ThayneStudio/orcest/pull/662), [#711](https://github.com/ThayneStudio/orcest/pull/711); issues #656, #658, #661 are closed |
| Provider heartbeat desired-pin health | [#785](https://github.com/ThayneStudio/orcest/pull/785) implements version health against desired pins. It does not implement the weekly registry/checksum comparisons and deduplicated maintenance issue workflow required by #621. |

## Operational evidence and issue bookkeeping

The following were open at this archive review. Their state must be resolved
explicitly rather than inferred from an old planning checklist:

- [#588](https://github.com/ThayneStudio/orcest/issues/588) requests manual
  Gemini/Cursor skill discovery and activation smoke tests. The archive does
  not contain completed smoke evidence.
- [#620](https://github.com/ThayneStudio/orcest/issues/620) requests a rebake and
  real-task smoke of the historical Codex 0.149.1 pin. Current rollout evidence
  or an explicit supersession decision must govern its closure.
- [#621](https://github.com/ThayneStudio/orcest/issues/621) still requires weekly
  registry/checksum comparisons and deduplicated maintenance issues. The
  heartbeat desired-pin health delivered in #785 is related evidence, but
  does not satisfy that remaining workflow. Keep this issue open.
- [#622](https://github.com/ThayneStudio/orcest/issues/622) is the old Redis/provider
  epic. Reconcile it after the operational leaves above are resolved.

The incident's timestamps, byte measurements, and claimed production outcomes
are retained as original evidence. This archive review checked repository
implementation and issue states; it did not reconstruct the August production
incident or rerun fleet deployment and manual provider smoke tests.
