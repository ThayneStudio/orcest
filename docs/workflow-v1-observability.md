# Workflow-Control v1 observability

The v1 observability snapshot is a read-only reconciliation of the SQLite
authority. It is rebuilt after every controller start; Redis and in-process
counters are never used to reconstruct gauges. Lifecycle events carry a stable
event ID derived from the durable row identity. Consumers must upsert or
deduplicate on that ID, so reducer and delivery replay cannot double-count an
event.

Diagnostic packets use the closed `workflow-observability-v1` schema,
contain at most 64 alerts/facts, and are limited to 32 KiB. Only stable IDs,
project scope, protocol/state/reason codes, counts, ages, revisions, and closed
health booleans are admitted. Prompts, work-item bodies, Candidate bytes, raw
forge/model payloads, tokens, capability URLs, Secret references/values, and
arbitrary exception text are forbidden in metric labels, events, logs, and
alert payloads. Storage failures reach the snapshot only through durable health
facts; the collector does not probe an object and create lifecycle authority.

## Ownership, severity, and retention

`controller-oncall` owns queue, deadline, Redis rebuild, and writer alerts;
`fleet-oncall` owns capacity; `release-oncall` owns publication and ownership;
`storage-oncall` owns integrity, disk, backup, and restore drill; and
`security-oncall` owns audit failures. `PAGE` requires immediate response.
`WARNING` is ticketed during the current business day. Alert delivery changes
no Run or human-boundary state.

Metrics retain 30 days, structured events and diagnostic packets 90 days, and
security/audit events according to the audit retention policy (at least one
year). Telemetry expiry never deletes workflow, outbox, receipt, restoration,
backup, or audit-authority rows.

## Default thresholds and rollout signals

Outbox age warns at 60 seconds and pages at 5 minutes. Publication latency
warns at 15 minutes and pages at 1 hour. Capacity-report age warns at 5 minutes
and pages at 15 minutes. Due-timer lag warns at 30 seconds and pages at 5
minutes. Backup age warns at 24 hours and pages at 48 hours; restore-drill age
warns at 30 days and pages at 90 days. Missing live objects, SQLite/FK failure,
a competing writer, Redis rebuild failure, publication ownership conflict, and
audit write failure page immediately. Deployments may tighten these values but
must not loosen verification or consensus gates.

Every snapshot reports a pass/fail signal for all accepted stages:

| Stage | Gate signal |
| --- | --- |
| 0 | integrity, Redis rebuild, and current backup/restore-drill evidence |
| 1 | accepted synthetic receipt and consensus evidence |
| 2 | pending work remains below the critical outbox-age bound |
| 3 | no ownership conflict, unfinished duplicate repair, or stalled publication |
| 4 | Stage 3 plus queue health for cohort expansion |
| 5 | every preceding gate passes before legacy retirement |

An operator must evaluate each signal with the fuller stage checklist in the
accepted operations wiki; these signals make the checklist observable and do
not grant rollout or telemetry lifecycle authority.
