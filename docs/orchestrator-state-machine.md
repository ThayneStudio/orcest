# Orchestrator State Model

GitHub PRs and issues are the source of truth. Redis is coordination state:
queues, locks, pending markers, cooldowns, and retry counters. Redis state may
delay work for the current snapshot, but it must not permanently redefine what
GitHub says is actionable.

## PR Snapshots

Every PR task is tied to the GitHub snapshot that caused it to be enqueued.
The minimum snapshot identity is the PR head SHA plus the decision reason:

- `ci_failure`: CI was failing for the captured failed check names.
- `changes_requested`: unresolved review threads needed fixes.
- `followup_threads`: approved PR still had unresolved review threads to triage.
- `merge_conflict_rebase`: GitHub reported merge conflicts.
- `proactive_rebase`: branch was proactively updated after an upstream merge.

Workers perform cheap validation before running. If the head SHA changed, or
the captured CI/review predicate no longer applies, the task is stale and is
dropped. This is not a lock and cannot prevent every race; it only prevents
known-stale work from running. The next heartbeat re-reads GitHub and enqueues
new work if the current snapshot still requires it.

## Redis Coordination

Pending markers store task id plus PR snapshot metadata. A marker for an older
head SHA is stale and should not block the current GitHub snapshot. Redis locks
are worker concurrency controls only; the system does not use GitHub lock labels
or rely on labels as locks.

Attempt counters are retry budget, not liveness. Active work is represented by
a matching pending marker or a Redis lock. A bare attempt counter must not
silently suppress a PR that GitHub still says is actionable.

## Result Handling

Worker results carry the same PR snapshot metadata as their task. Before the
orchestrator applies labels, comments, backoff, or attempt escalation, it checks
that the current GitHub PR still matches the result snapshot. Stale results are
ACKed after matching coordination cleanup and do not mutate GitHub.

If GitHub validation cannot be performed, the worker treats that as transient
infrastructure failure before execution. During result handling, validation
fails closed: the result is ACKed after coordination cleanup, but GitHub labels,
comments, backoff, cooldowns, and attempt escalation are not mutated. The next
heartbeat re-reads GitHub and enqueues new work if the source-of-truth snapshot
still requires it.

## Decision Coverage

Tests should cover the decision tree, not just line coverage:

- pending marker for current SHA skips as queued
- pending marker for old SHA is cleared and current GitHub state is evaluated
- worker drops old-SHA tasks before running
- worker drops CI/review tasks whose captured predicate no longer applies
- stale results never add labels, comments, backoff, or terminal attempts
- PR attempts without active pending/lock state do not produce silent stuck work
- Redis loss may cause extra retries, but must not permanently suppress work
