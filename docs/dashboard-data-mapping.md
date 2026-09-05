# Fleet dashboard: prototype-to-system mapping

Date: 2026-09-05

Status: source audit and integration direction. The first implementation is
covered in the [fleet dashboard runbook](fleet-dashboard.md); live deployment
remains unverified. This document applies the [product vision](vision.md)
to the approved local prototype: **Upcoming → In progress → Done**, with activity
and waiting reasons on cards, and a separate Fleet view.

## Audit baseline

Source inspected: fetched `origin/master` at
`ba6e8de72d343000748b57960c7c7bef279f2e49`. The local checkout is behind that
revision and contains the uncommitted vision documentation. Source links below
are pinned to the inspected revision rather than assuming the checkout matches.
The running fleet's revision, configuration, monitor availability, and v1 rollout
have not been audited. Prototype examples are fictional and are not live evidence.

## Main finding

The current dashboard can show executions, but cannot yet supply a complete
work-lifecycle board. It receives queue entries, locks, recent results, operational
counters, and VM pool records. Work waiting before enqueue, the continuity of an
issue through its PR, and verified delivery are not represented together.

Orcest already computes much of the missing information. The smallest useful
change is to expose those observations through a read-only work view, not to put
another copy of scheduling policy in the browser or dashboard server.

There is also substantial workflow-control v1 code: runs, activities, attempts,
publications, wait conditions, human boundaries, and transition history. Reuse
those records for work owned by v1. Do not create a competing lifecycle store or
make the dashboard depend on a wholesale v1 migration. The latest source includes
a legacy PR ownership fence backed by an optional read-only `workflow.db` mount;
that is evidence of coexistence support, not proof that v1 owns the live fleet.
[Sources: v1 store][store], [v1 vocabulary][enums], [ownership configuration][readme]

## Mapping the approved experience

| Prototype element | Evidence available today | Gap / recommended mapping |
| --- | --- | --- |
| Project name, work reference, title | Issue/PR discovery has titles and repository identity. Snapshot records retain repository, resource type, resource ID, and prefixes. | Publish an explicit project catalogue and work identity, title, source URL, and bounded description. Include configured projects with no queued work. |
| Upcoming, dependency wait | `IssueState.action = SKIP_DEPENDENCY`, with `open_blockers`. Native dependency relationships and body references are checked. | Publish observed issues even when no task is enqueued. Keep first-run work Upcoming; include blocker references and the re-evaluation condition. |
| Upcoming, queued | `queued_tasks`, stream/group backlog, pending markers. | Use queue evidence for activity, not lifecycle. An already-started item queuing a follow-up remains In progress. Admission intent alone does not prove successful enqueue. |
| In progress, executing | Locks/pending task metadata, task-start/activity events, worker liveness. | Correlate a live attempt to its work item. Locks alone do not prove the agent is making progress. |
| In progress, waiting for CI / review / retry | `PRState.action`, CI/review analysis, Redis cooldowns, result reset times; v1 wait conditions where applicable. | Expose the actual reason, observation time, and next check or reset time. Do not infer it from lack of output or an empty queue. |
| In progress, monitoring a PR | PR discovery can return pending checks, green/no action, merge eligibility, or review follow-up. | Retain the card between agent attempts. An existing observed PR establishes started work even if Orcest has never launched an agent for it. |
| Done | Merge observations; v1 terminal outcomes; legacy issue handoff verification. | Task success is insufficient. Handoff verification may only prove a PR exists. Preserve In progress until the relevant outcome is observed. Distinguish merged/completed from closed or cancelled. |
| Needs you inbox | Result `needs_human` and `needs_human_reason`, labels, unverifiable delivery state; v1 human boundaries. | Persist the current actionable reason, scope, and resolution condition. Do not equate every failed task, exhausted account, or dead letter with a human blocker. |
| Live output | Existing task-output WebSocket and task selection helpers. | Reuse with exact task, worker, and output-prefix identity. Show last-session output separately from a live session. |
| Context and timeline | Monitor task/work event endpoints, archived traces, task snapshot metadata; v1 transitions/publications. | Join attempts and verified issue/PR relationships. Mark missing history and avoid fabricated start times. |
| Workers busy / total | Pool idle/active VM records; worker heartbeat/activity records elsewhere in Orcest. | Publish a reconciled worker-to-task/backend mapping. Existing worker discovery lists output streams, not a census of living VMs. |
| Account usage | Non-secret `provider_account` identity on tasks/results; provider-pool cooldowns; Claude usage helper. | Add a public account inventory and safe availability snapshot. Existing provider health is counters, not remaining quota. Percentages remain unknown unless actually reported. |

Evidence: [dashboard public types][types], [snapshot assembly][snapshot],
[issue discovery][issues], [PR discovery][prs], [task/result records][models],
[issue handoff verification][delivery], [worker discovery][workers],
[provider pool][pool], [usage helper][usage].

## Keep lifecycle independent of execution

The proposed read view needs separate facts for:

- **Lifecycle:** Upcoming, In progress, Done; allow unknown when historical
  evidence cannot establish whether work has started.
- **Activity:** ready, queued, executing, waiting, monitoring, or terminal.
- **Reason:** dependency, capacity, provider cooldown, CI, review, scheduled
  retry, delivery verification, access, or an explicit unknown reason.
- **Attention:** whether there is a current human-action requirement; this does
  not erase the item's lifecycle or remove it from work detail/search.
- **Freshness and coverage:** when the source was checked and whether the result
  is complete, stale, unavailable, or limited to a preview.

These are presentation concepts, not proposed replacements for existing scheduler
enums. The backend maps authoritative records into this view.

| Observed situation | Lifecycle and presentation |
| --- | --- |
| Ready-labeled issue, open prerequisite, no execution history | Upcoming · Waiting for prerequisite · Not started |
| First task actually queued | Upcoming · Queued |
| Implementation starts | In progress · Executing |
| Implementation finishes and PR checks run | In progress · Waiting for CI · No active worker |
| CI fails and a fix task queues | In progress · Queued for CI fix |
| Provider limit ends an attempt | In progress · Waiting for retry, with known reset time or unknown timing |
| Dependency appears after work started | In progress · Waiting for dependency |
| PR handoff verified, PR still open | In progress · Monitoring PR |
| Current delivery evidence establishes completion | Done · Specific observed outcome |
| Issue disappears from the ready-label query | Reconcile its source state; absence alone is neither completion nor cancellation |
| A source poll fails or was skipped | Retain last-known state with its age; do not clear cards or report zero work |
| Evidence never established a start time | Lifecycle unavailable until reconciled; no invented “Not started” or elapsed time |

Cancelled, superseded, or closed-without-delivery work must not appear as successful
Done cards. Keep its explicit outcome in history; whether the UI later groups
those outcomes in a separate history view is not required for the first live slice.

### Work identity and issue-to-PR continuity

Use an opaque work identity scoped to its project and source instance. Preserve
the source-native reference separately. A GitHub issue number alone is not an
identity; repository, resource kind, and instance matter. Redis prefixes are
routing information, not the product's permanent identity model.

For legacy records, use an adapter mapping from the existing project/prefix,
repository, resource kind, and resource ID. Preserve v1 IDs for v1-owned work.
Do not collapse different ownership domains merely because their display numbers
match. If v1 ownership lookup is unavailable, mark it unknown rather than treating
the work as legacy-owned.

An issue card should collect its implementation and subsequent PR activity when
an explicit, verified relationship exists. Preserve PRs as linked resources and
attempts as separate executions. Standalone PRs remain work items in their own
right. Ambiguous or many-to-many links must remain explicit; never merge cards
by title, a coincident issue number, or an unverified branch-name convention.

Persist first-start evidence or recover it from authoritative history. A restart,
expired lock, trimmed result, or worker replacement must not move started work
back to Upcoming. Attempt counters are retry budgets and may be reset; they are
not reliable execution history.

## Reuse the existing transport

| Existing surface | What it supplies | Limits to preserve |
| --- | --- | --- |
| `GET /api/snapshot`, `/ws/snapshot` | Operational snapshot; shared WebSocket refresh every 2 seconds | Queued preview is capped at 50 entries per stream; recent results default to 20 per read. `degraded_sections` must remain visible. These are not an exhaustive work inventory. |
| `GET /api/workers` | Worker IDs discovered from retained output streams | Historical output can outlive a VM; quiet/warm workers need not have output. Do not use this as the worker denominator. |
| `/ws/task-output` | Formatted task output, polled every 500 ms | Requires worker ID, with task ID, prefix, and historical mode supported. Prefix ambiguity and missing/truncated output are already handled; retain those protections. |
| Monitor `/api/v1/tasks/{task_id}/timeline` | Events for one task | Availability depends on event relay and retained monitor data. |
| Monitor `/api/v1/work/{owner}/{name}/{resource_type}/{resource_id}` | Attempt summaries from events | Not a title/dependency/current-lifecycle endpoint. Current grouping uses attempt numbers, which can repeat; retain distinct task IDs. |
| Monitor `/api/v1/tasks/{task_id}/trace` | Archived trace tail, default 200 lines, maximum 5,000 | Requires separate trace scope and configured archives. A tail is not guaranteed full history. |

Sources: [HTTP server][server], [WebSocket routing][upgrade],
[output socket][output], [monitor queries][monitor].

Live Redis output is bounded (512 entries, 4 KiB per-entry payload, 8-hour TTL).
Large output lines are chunked; the live viewer does not provide a verbatim full
archive. Keep “output unavailable,” “retained tail,” and “session ended” distinct
from “no session yet.” Reuse archive access for older evidence when available.
[Output retention source][retention]

## Smallest useful backend addition

Add a read-only work projection: a query-friendly copy of observed work and
execution evidence. It must not control eligibility, assign work, or write source
state on behalf of the dashboard.

1. **Legacy discovery publishes its observations.** Capture the existing issue/PR
   selector results before they are discarded, including skipped items and
   action reasons. Include the actual result of scheduling decisions, so “could
   enqueue” is not reported as “queued.” Preserve the current backpressure and
   fairness behavior. Discovery skipped because of backlog is a stale observation,
   not an empty project. Selector cascades often short-circuit: report a known
   blocker without claiming the list contains every possible blocker.
2. **Lifecycle evidence survives queue cleanup.** Retain first-start evidence,
   confirmed issue/PR associations, observed terminal outcomes, and last-known
   metadata. Reconcile tracked items that leave discovery queries. A periodic
   reconciliation can repair missed best-effort events; event delivery alone
   cannot guarantee a complete work inventory.
3. **V1 exposes a read adapter.** Use its snapshots, runs, activities, attempts,
   publications, waits, boundaries, and transitions where configured. The public
   projection is additive; it does not introduce parallel v1 transition logic.
   Audit the deployed ownership mode before enabling either adapter.
4. **The dashboard server serves the projection.** Add versioned, paginated work
   list/detail responses with project filters, coverage, source timestamps, and
   stable identities. Reuse the existing authenticated update connection for
   notifications or refreshed data. Do not poll GitHub per browser/card, and do
   not call discovery functions from HTTP reads: some perform coordination cleanup.

For legacy-only installations, a bounded, rebuildable observation cache in the
existing infrastructure is sufficient to start. Its lifetime and checkpoint
strategy must preserve known lifecycle history or explicitly report uncertainty
after loss. Do not promise durable history by storing it only in expiring Redis
keys. Final storage choice and migration belong in the implementation change,
after checking available v1 and monitor deployment; a new database service is not
a prerequisite of this dashboard mapping.

Suggested public fields: work/project IDs, native source references, title,
description summary, lifecycle, current activity/reason, blockers, next condition,
first-start evidence, latest observation, coverage, linked resources, active
attempt IDs, previous attempt IDs, outcome evidence, and attention requirement.
Existing diagnostic snapshot fields remain available separately.

### Fleet data addition

Publish a safe inventory of configured provider accounts with opaque IDs, provider,
display name, compatible models/profiles, eligible projects, availability, known
cooldown expiry, and timestamped quota windows. Use existing `provider_account`
correlation; never serialize complete task/result objects, which contain secrets.

Claude has a usage helper, currently also called by a manually triggered workflow.
That is not a continuously refreshed fleet quota feed. Quota probes should be
bounded and shared across viewers. Report unsupported or failed probes as unknown;
do not add percentages across accounts or count model variants as separate budgets.

Publish a separate worker inventory joining pool records, heartbeat/activity,
backend/profile, and current task. Count busy workers from correlated active work,
not retained output streams or configured credentials. Label partial/stale counts
and distinguish warm, starting, busy, and unavailable compute when evidenced.

### Attention and completion

Reuse genuine human-boundary signals and delivery-verification blockers. A label
may outlive a result; keep its observed state and explain when the detailed reason
is unavailable. Do not advertise automatic recovery for an exception whose
backend lacks it: legacy `UNVERIFIABLE` delivery currently needs an explicit
operator resolution path. The prototype's “Simulate access restored” is not a
production command and must not be shipped as one.

Handoff verification owns whether an implementation produced the expected handoff;
it does not establish CI, review, merge, or deployment success. Show that as an
intermediate outcome. For the first live board, only report completion when the
current integration provides explicit evidence, and name the boundary (for example,
“Merged”). Project-specific delivery policies remain future `.orcest` work.

## Implementation sequence and acceptance checks

These are recommended slices, not filed issues or a commitment to build the entire
future workflow system in this change.

1. **Observation and identity:** publish pre-enqueue work plus source freshness;
   add the work read contract and legacy/v1 ownership boundary. Prove a dependency-
   blocked issue appears without any Redis task or worker.
2. **Lifecycle and delivery continuity:** correlate starts, retries, handoffs,
   and terminal evidence. Prove CI waits and requeued fixes stay In progress,
   restarts do not reset known work, and agent success does not falsely mark Done.
3. **Connect Board and detail:** carry the approved prototype design into the
   existing dashboard stack; reuse task output and monitor context. Prove project
   filtering, dependency navigation, output switching, unavailable history, and
   partial snapshots. Keep the standalone mock prototype available for comparison.
4. **Connect Fleet and attention:** add account/worker observations and exception
   details. Prove an exhausted account consumes no implied VM, retained output does
   not count a dead worker, and no credential fields enter public responses.

Cross-cutting cases: same-number issues in different projects, issue/PR number
collisions, shared task streams, repeated attempt counters, stale-SHA results,
missing v1 ownership data, a failed or skipped source poll, and more work than
the queue/result preview limits. None may silently duplicate work, drop tracked
cards, fabricate success, or report a partial count as the complete fleet.

Before production rollout, verify the deployed revision and enabled subsystems,
choose the first project for a read-only rollout, and compare projected cards
against source observations and active tasks. This source audit did not access
live credentials or modify scheduling behavior.

[types]: https://github.com/ThayneStudio/orcest/blob/ba6e8de72d343000748b57960c7c7bef279f2e49/dashboard/src/lib/types.ts
[snapshot]: https://github.com/ThayneStudio/orcest/blob/ba6e8de72d343000748b57960c7c7bef279f2e49/dashboard/server/snapshot.ts
[issues]: https://github.com/ThayneStudio/orcest/blob/ba6e8de72d343000748b57960c7c7bef279f2e49/src/orcest/orchestrator/issue_ops.py
[prs]: https://github.com/ThayneStudio/orcest/blob/ba6e8de72d343000748b57960c7c7bef279f2e49/src/orcest/orchestrator/pr_ops.py
[models]: https://github.com/ThayneStudio/orcest/blob/ba6e8de72d343000748b57960c7c7bef279f2e49/src/orcest/shared/models.py
[delivery]: https://github.com/ThayneStudio/orcest/blob/ba6e8de72d343000748b57960c7c7bef279f2e49/src/orcest/orchestrator/issue_delivery.py
[workers]: https://github.com/ThayneStudio/orcest/blob/ba6e8de72d343000748b57960c7c7bef279f2e49/dashboard/server/workers.ts
[pool]: https://github.com/ThayneStudio/orcest/blob/ba6e8de72d343000748b57960c7c7bef279f2e49/src/orcest/orchestrator/provider_pool.py
[usage]: https://github.com/ThayneStudio/orcest/blob/ba6e8de72d343000748b57960c7c7bef279f2e49/src/orcest/orchestrator/usage_check.py
[store]: https://github.com/ThayneStudio/orcest/blob/ba6e8de72d343000748b57960c7c7bef279f2e49/src/orcest/workflow_store/store.py
[enums]: https://github.com/ThayneStudio/orcest/blob/ba6e8de72d343000748b57960c7c7bef279f2e49/src/orcest/workflow_contract/v1/enums.py
[readme]: https://github.com/ThayneStudio/orcest/blob/ba6e8de72d343000748b57960c7c7bef279f2e49/README.md
[server]: https://github.com/ThayneStudio/orcest/blob/ba6e8de72d343000748b57960c7c7bef279f2e49/dashboard/server/index.ts
[upgrade]: https://github.com/ThayneStudio/orcest/blob/ba6e8de72d343000748b57960c7c7bef279f2e49/dashboard/server/upgrade.ts
[output]: https://github.com/ThayneStudio/orcest/blob/ba6e8de72d343000748b57960c7c7bef279f2e49/dashboard/server/taskOutputSocket.ts
[monitor]: https://github.com/ThayneStudio/orcest/blob/ba6e8de72d343000748b57960c7c7bef279f2e49/src/orcest/monitor/query_app.py
[retention]: https://github.com/ThayneStudio/orcest/blob/ba6e8de72d343000748b57960c7c7bef279f2e49/src/orcest/shared/output_streams.py
