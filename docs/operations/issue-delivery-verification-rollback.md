# Issue delivery verification: old-orchestrator rollback

Durable delivery verification owns issue completion. A provider `COMPLETED`
result no longer removes `orcest:ready`. An older orchestrator that completes
issues from a zero-exit result will race that gate.

Verified delivery is a handoff-snapshot assertion only. It does not prove
implementation correctness, absence of unrelated GitHub mutations, CI/review
eligibility, or hostile-provider containment.

## Why rollback is unsafe without a drain

Nonterminal verification jobs, copied expected outcomes, admission ledger
entries, and issue dispatch barriers have no wall-clock TTL. They outlive:

- the results stream `MAXLEN` cap
- pending-task marker expiry
- ordinary publication cleanup

An old orchestrator does not consult that state. If it consumes a still-queued
completed result, or rediscovers an issue whose barrier it cannot see, it can
remove `orcest:ready` or dispatch a duplicate implementation.

`UNVERIFIABLE` is operator-blocked. There is no raw Redis delete, silent
barrier clear, or automatic provider retry in this release. A future resolution
path needs its own design.

## Required sequence

1. Pause issue dispatch and result consumption. Stop or fence the running
   orchestrator so it cannot `XREADGROUP` project `results` streams or publish
   issue tasks.
2. Inventory affected state in each project Redis:
   - `issue:verification:due`
   - `issue:verification:active`
   - `issue:verification:unverifiable`
   - `issue:result:quarantine`
   - per-issue `issue:{repo}:{n}:verification:{generation}` hashes
   - dispatch barriers `issue:{repo}:{n}:dispatch-barrier`
3. Drain or quarantine every affected completed result and verification job:
   - ACK or dead-letter remaining completed issue results so an old binary
     cannot treat them as fresh completions.
   - Keep `PENDING` / `UNVERIFIABLE` jobs until they are verified, marked
     ineffective by the current binary, or explicitly quarantined for operator
     follow-up. Do not delete nonterminal jobs to "make rollback easier".
4. Prove `delivery_state_blocks_old_orchestrator_rollback` (or the equivalent
   key inventory) is empty for every project prefix.
5. Only then roll the orchestrator image back.

Rolling forward again is safe: jobs have no TTL, so a current orchestrator can
resume due membership via the reconciler.

## In-process disable

`issue_delivery_verifier.enabled: false` restores immediate `orcest:ready`
removal only for newly consumed completed issue results. It does not process or
clear existing verification jobs or dispatch barriers, so any in-flight job can
keep issue discovery blocked while verification processing is disabled. Disable
it only after the same pause and drain, or on a fleet that never admitted
verification jobs.
