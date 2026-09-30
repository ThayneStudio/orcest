# Harness Engineering Follow-up — GitHub Issue Creation Plan

> **Historical archive — 2026-09-30:** This August planning record is
> preserved for incident and design provenance. Its status, checklists,
> implementation steps, and agent instructions describe the original
> session; they are not current execution instructions or deployment
> evidence. See the [archive disposition](../local-planning-archive-disposition.md)
> for implemented work and remaining operational evidence.

**Status:** Filed and verified 2026-08-30.

**Source specification:** `docs/superpowers/specs/2026-08-29-harness-engineering-remediation.md`

**Evidence date:** 2026-08-29. The live backlog contained six open issues: #588, #620, #621, #622, #649, and #653. Three independent reviewers searched the full open/closed corpus and then approved the revised specification.

## Filed issues

- [#655 — issue tasks: add generation-scoped idempotent publication](https://github.com/ThayneStudio/orcest/issues/655)
- [#657 — issue tasks: add a complete typed GitHub delivery verifier](https://github.com/ThayneStudio/orcest/issues/657), blocked by #655
- [#659 — issue tasks: gate completion through durable delivery verification](https://github.com/ThayneStudio/orcest/issues/659), blocked by #657
- [#660 — issue tasks: resume expected refs with bounded retry context](https://github.com/ThayneStudio/orcest/issues/660), blocked by #659
- [#656 — tests: make Redis-backed suites invocation-scoped and parallel-safe](https://github.com/ThayneStudio/orcest/issues/656)
- [#658 — dev: lock Python development, test, and build inputs](https://github.com/ThayneStudio/orcest/issues/658)
- [#661 — dev: add a canonical check DAG and make CI invoke its leaves](https://github.com/ThayneStudio/orcest/issues/661), blocked by #656 and #658

## Outcome

Create seven engineering issues. Do not create an umbrella epic and do not create a GitHub-authority/ruleset issue under the current #573 policy.

The seven leaves are intentionally smaller than the original three findings:

- Issue-task delivery needs four ordered changes because publication safety, GitHub observation, durable enforcement, and prompt resume have different rollback and test boundaries.
- Hermetic Redis remains one atomic safety issue: the supervisor and destructive fixture guard must land together.
- Dependency locking is independent of Redis and becomes its own issue.
- The canonical check DAG depends on both the Redis harness and locked development inputs.

## Backlog decisions

- **No duplicate exists for verified issue-task delivery.** Open #649 protects merge-time evidence for PRs; it does not verify an `IMPLEMENT_ISSUE` handoff or govern `orcest:ready` removal.
- **No duplicate exists for invocation-scoped Redis tests.** Closed #37/#38 fixed fixture/thread isolation inside one run, not concurrent worktree ownership.
- **No duplicate exists for the development/build lock or check DAG.** Closed #14, #35, and #387 cover earlier CI/runtime/coverage work only.
- **Preserve #147 and #567.** Integration event policy and independent master verification are controlling behavior, not work to reopen.
- **Do not add this work to #622.** That epic covers a separate Redis/provider backlog and already contains stale checklist state.
- **Do not file authority work.** #573 says rulesets/branch protection are a settled “not planned” decision and must not be refiled. The approved spec records the current trusted-worker boundary. An authority clarification requires a separate explicit owner request.

## Dependency graph

```text
1. generation + idempotent issue-task publication
   └─ 2. typed GitHub delivery adapter + shadow evidence
      └─ 3. durable enforcing verification job
         └─ 4. bounded retry prompt + authoritative resume

5. hermetic Redis test harness ──┐
                                 ├─ 7. canonical check DAG / CI leaf parity
6. locked dev/test/build inputs ─┘
```

Issues 1, 5, and 6 can start immediately and in parallel. Blocked leaves retain `orcest:ready`; body-declared `Blocked by #N` references or GitHub-native relationships defer dispatch until prerequisites close.

## Issue 1 — Generation-scoped, idempotent issue-task publication

**Proposed title:** `issue tasks: add generation-scoped idempotent publication`

**Labels:** `bug`, `critical`, `orcest:ready`

**Dependencies:** None.

**Purpose:** Establish the orchestrator-owned expected outcome and eliminate ambiguous cross-Redis publication before any completion gate depends on it.

**Required scope:**

- Reserve a monotonic repository/issue generation, task ID, prompt-input hash, expected head owner/branch, attempt, and pending marker atomically in project Redis.
- Treat `prompt_content_sha256` as audit/correlation metadata only: it neither proves revision approval nor participates in delivery verification; revision-bound admission is outside this issue.
- Add optional `expected_branch` and `issue_generation` task fields without changing `Task.branch` checkout semantics.
- Publish through a task-stream idempotency script keyed by task ID; retry returns the original stream ID rather than appending a duplicate.
- Record `prepared`, `published`, and `ambiguous` states. A lost `XADD` response reconciles through the stream-side receipt and never triggers blind rollback.
- Roll back only failures proven to occur before publication.
- Garbage-collect records only after no stream receipt, pending marker, result, verification job, or retry record references them.

**Acceptance tests:**

- New/legacy task round-trips and prompt/expected-branch equality.
- Changing or omitting the prompt hash cannot independently drive a delivery outcome.
- Duplicate publish returns one stream entry.
- Lost response after successful append reconciles to the original entry.
- Definite pre-publish failure cleans up; ambiguous outcome preserves state.
- Generation monotonicity and stale-generation CAS refusal.
- Reference-aware cleanup never deletes active publication state.

## Issue 2 — Typed, complete GitHub delivery observation

**Proposed title:** `issue tasks: add a complete typed GitHub delivery verifier`

**Labels:** `enhancement`, `critical`, `orcest:ready`

**Dependencies:** `Blocked by #655`.

**Purpose:** Build the authoritative, read-only GitHub boundary and shadow evidence before enabling label mutations.

**Required scope:**

- Fetch the repository default branch and exact expected remote ref/OID.
- Fetch all candidate PRs for the expected same-repository head branch.
- Return typed state, draft flag, base/head repositories and refs, head OID, number/URL, and complete canonical `closingIssuesReferences`.
- Fully paginate every connection and expose completeness. Page caps, missing cursors, null/truncated fields, and partial relations cannot verify.
- Distinguish transient transport/rate errors from persistent authentication, permission, and schema failures.
- Treat the prompt hash (`prompt_content_sha256` / `prompt_input_hash`) as audit/correlation metadata only; it never participates in a verification outcome or substitutes for live GitHub evidence.
- Add bounded read-only shadow evidence/counters without changing labels or dispatch behavior.

**Acceptance tests:**

- Complete multi-page candidates and closing references.
- Cap/truncation/null/malformed response failure.
- Exact ref/OID/default-branch behavior.
- Canonical issue relation succeeds; matching PR body text alone fails.
- Missing or changed prompt-hash metadata does not change the outcome for identical live GitHub evidence.
- Error taxonomy and secret-free evidence.

## Issue 3 — Durable verification jobs and issue-completion gate

**Proposed title:** `issue tasks: gate completion through durable delivery verification`

**Labels:** `bug`, `critical`, `orcest:ready`

**Dependencies:** `Blocked by #657`.

**Purpose:** Replace zero-exit completion with a crash-safe, resource-level delivery state machine.

**Required scope:**

- Admit every issue-result status to a per-task, first-payload CAS ledger before any side effect.
- Replay an identical fingerprint through its recorded route. Conflicting status/payload freezes a nonterminal job as `UNVERIFIABLE`; after terminal state it is quarantined without changing that state or newer generations.
- For the winning completed result, atomically create/validate both the durable verification job and due-index entry before ACKing the result stream and releasing provider tracking.
- Add a due scheduler plus an idempotent reconciler for missing/stale due membership.
- Make every nonterminal job an issue discovery barrier independent of result-stream retention and pending-marker TTL.
- Give nonterminal jobs, copied expected outcomes, and dispatch barriers no wall-clock TTL; retain the admission ledger by reference until its generation and every related side effect are terminal.
- Implement `PENDING`, immutable `VERIFIED`, `INEFFECTIVE`, and `UNVERIFIABLE` transitions with capped backoff and operator alerts.
- Define `UNVERIFIABLE` as operator-blocked/nonterminal: retain `orcest:ready` and the durable barrier, never auto-redispatch, and do not invent a Redis-delete/override path. Operator resolution is outside this issue pending an approved audited, generation-CAS design/runbook.
- Verify when at least one complete candidate is open, non-draft, same repository/expected head, targets the default branch, matches the live remote OID, and canonically closes the exact issue.
- Treat provider result echoes/claims as diagnostic only: mismatch can neither verify nor make delivery ineffective and emits a bounded counter/event.
- If multiple candidates qualify, verify successfully, select the lowest PR number for stable evidence, and increment an ambiguity counter.
- During eventual-consistency grace, non-verifying observations remain pending. After grace, complete evidence with no valid candidate becomes ineffective. Auth/permission/schema/corrupt Orcest state becomes unverifiable.
- Persist `VERIFIED` before removing `orcest:ready`; finish label/checkpoint/cleanup as an idempotent generation-scoped saga without re-querying mutable state.
- For ineffective delivery, keep `orcest:ready`, durably store minimal machine-derived retry state/cooldown, then generation-CAS cleanup.
- Reconcile legacy tasks by canonical issue relation without recomputing a branch slug. If no handoff can be proven, block as unverifiable rather than redispatching.
- Emit structured result-admission and phase-transition events plus counts for due/pending/verified/ineffective/unverifiable, ambiguity, echo mismatch, and verification latency; alert on unverifiable jobs and oldest-pending age.
- Document and test that rollback to the old orchestrator requires pausing issue dispatch/result consumption and draining or quarantining affected state.

**Acceptance tests:**

- Result-stream trimming and pending-marker expiry during a long verification outage cannot dispatch duplicate work.
- Advancing beyond ordinary pending/result/cleanup TTLs while `PENDING` or `UNVERIFIABLE` preserves the job, copied outcome, ledger reference, and dispatch barrier.
- Reference-aware cleanup cannot erase active/nonterminal state.
- Crash between job/due admission phases is impossible; crash after atomic admission/before ACK replays safely.
- Identical-fingerprint replay resumes the recorded route without duplicating admission, events, or side effects.
- FAILED/BLOCKED/USAGE then COMPLETED and COMPLETED then non-success conflicts cannot both mutate state.
- Post-terminal conflicts are quarantined without demoting `VERIFIED`, resurrecting `INEFFECTIVE`, or touching a newer generation.
- No branch, branch only, wrong repo/owner/ref/issue/base/OID, draft/closed PR, one valid PR, and multiple valid PRs with stable lowest-number evidence plus one ambiguity increment.
- Echo/claim mismatch with otherwise valid live GitHub evidence still verifies and records only diagnostic mismatch telemetry.
- Transport/rate backoff, grace expiry, operator-blocked auth/schema failures, and scheduler restart recovery.
- Crash at every verified/ineffective side-effect boundary.
- Existing prior-attempt valid PR skips another provider dispatch.
- Structured lifecycle events, phase counters/latency, oldest-pending alerting, and unverifiable alerting are secret-free and idempotent under replay.
- PR-task behavior and admitted non-success issue outward behavior remain unchanged.

## Issue 4 — Bounded retry prompts and authoritative resume

**Proposed title:** `issue tasks: resume expected refs with bounded retry context`

**Labels:** `enhancement`, `important`, `orcest:ready`

**Dependencies:** `Blocked by #659`.

**Purpose:** Use A3's machine-derived ineffective record to resume useful work without injecting agent prose or duplicating branches/PRs.

**Required scope:**

- Store only the latest repository/issue/generation retry record, capped at 4 KiB.
- Allowlist task/generation, expected ref, authoritative remote head, canonicalized PR number/URL, reason code, and timestamps.
- Exclude PR/issue titles, bodies, comments, provider traces, and model summaries.
- Render canonical fixed-schema JSON in a fenced diagnostic block with validated refs/SHAs and escaped control characters.
- Resume only the authoritative same-repository expected ref by explicitly fetching/checking it out from the default workspace.
- Clear retry context/cooldown only with a matching generation CAS after verified delivery.

**Acceptance tests:**

- Existing expected branch and partial PR are resumed; unexpected owner/ref is rejected.
- JSON size, schema, escaping, URL canonicalization, and forbidden-field tests.
- Stale generation cannot overwrite or delete newer context.
- Successful verification clears the matching context; duplicate result replay remains idempotent.

## Issue 5 — Invocation-scoped Redis test harness

**Proposed title:** `tests: make Redis-backed suites invocation-scoped and parallel-safe`

**Labels:** `bug`, `important`, `orcest:ready`

**Dependencies:** None.

**Purpose:** Prevent concurrent worktrees from sharing, flushing, stopping, or cleaning up one Redis test service.

**Required scope:**

- Add a dedicated test-only Compose file with no named volume/shared network and Docker-assigned loopback port. Do not merge overrides into production-like `docker-compose.redis.yml`.
- Add a Python supervisor that creates a unique project/nonce, discovers the assigned port, seeds a database-0 invocation marker, exports a database-15 URL, and runs the child in its own process group.
- Forward `INT`/`TERM`, wait then bounded-escalate, clean exactly once, and return exact normal status or 130/143. Cleanup failure overrides only a successful child.
- Clean the exact project with `docker compose -p <generated-project> -f <test-compose> down --volumes --remove-orphans`.
- Label test containers/project for bounded manual stale-run discovery; normal cleanup deletes only the exact owned project.
- Remove the unsafe fixture default. Require URL and matching nonce, reject database 0, and validate the marker before both setup and teardown `FLUSHDB`. Missing proof is a failure, not a skip. Always close clients in `finally`.
- Run `pytest -m integration` and `pytest -m stress` so inline markers outside their directories are included.
- Preserve `make test` as a compatibility entry point using invocation-scoped services and the existing full local behavior through `test-unit`, managed `test-integration`, managed `test-stress`, and `test-dashboard`. #661 owns `check-fast`/`check-full` and CI wiring. No correctness target may depend on `redis-up/down`.

**Acceptance tests:**

- Two small blocking harness clients overlap with distinct projects, ports, nonces, and keyspaces; neither can stop/flush the other.
- Success, failure, `INT`, and `TERM` process-group/cleanup/exit semantics, including proof that no owned containers, networks, or anonymous/named volumes remain.
- Wrong/missing URL/nonce and database 0 refuse both destructive fixture calls.
- Inline integration selection is preserved; integration and stress pass once each through the supervisor.
- Stale labels are discoverable without broad automatic deletion.

## Issue 6 — Locked Python development/test/build environment

**Proposed title:** `dev: lock Python development, test, and build inputs`

**Labels:** `enhancement`, `important`, `orcest:ready`

**Dependencies:** None.

**Purpose:** Stop CI and agents from resolving a new dev/build environment on every install.

**Required scope:**

- Generate a development lock from the `dev` extra including PEP 517 build requirements; constrain shared runtime packages by the runtime lock where compatible.
- Pin/document Python 3.12, pip, and pip-tools used for regeneration.
- Install the lock, then editable Orcest with `--no-deps --no-build-isolation`.
- Key caches from the dev lock and any runtime constraint it consumes.
- Regenerate into a temporary file and compare without mutating the checkout.
- Update README bootstrap instructions to the locked path.
- Keep the monitor image's unlocked install outside this issue.

**Acceptance tests:**

- Lock regeneration is clean under the documented toolchain.
- A clean environment installs without resolving unpinned build/dev versions and runs the fast Python checks.
- Cache-key and README structural coverage.
- Runtime production lock behavior is unchanged.

## Issue 7 — Canonical local check DAG and CI leaf parity

**Proposed title:** `dev: add a canonical check DAG and make CI invoke its leaves`

**Labels:** `enhancement`, `important`, `orcest:ready`

**Dependencies:** `Blocked by #656` and `Blocked by #658`.

**Purpose:** Make repository checks agent-runnable without claiming that one local aggregate duplicates every CI-only image build.

**Required scope:**

- Add `lint-check` running both `ruff check src/ tests/` and `ruff format --check src/ tests/`.
- Add `typecheck` running `mypy src/`.
- Preserve `test-unit`'s unit-marker and `src/orcest` coverage contract.
- Make `test-integration` and `test-stress` use #656's managed supervisor with `pytest -m integration` and `pytest -m stress` respectively.
- Preserve `test-dashboard`'s existing clean-copy behavior.
- Define `check-fast = lint-check + typecheck + test-unit`.
- Define `check-full = check-fast + test-integration + test-stress + test-dashboard`.
- Keep root/dashboard image builds and dashboard Compose/image smokes CI-only until a separate issue makes their tags/ports worktree-safe.
- Preserve CI jobs `lint`, `typecheck`, `test`, `dashboard`, `integration`, and `docker`, because master verification requires them.
- Make deterministic jobs invoke the same leaves and locked install path.
- Remove the fixed-port Actions Redis service when integration owns its ephemeral harness.
- Preserve integration's master-push/schedule/manual predicate from #147. Keep stress local-only and advisory network checks non-blocking.

**Acceptance tests:**

- Structural coverage for all six job names and integration trigger semantics.
- CI calls named leaves rather than restating Ruff/mypy/pytest commands.
- Behavioral command capture proves each leaf recipe, including both Ruff commands, selectors, coverage, managed Redis use, and dashboard clean-copy behavior.
- Make rule-database or behavioral command-capture coverage proves composite transitive prerequisites.
- No correctness target depends on shared `redis-up/down`.
- #567 master verification inputs remain unchanged.

## Explicitly excluded authority issue

Do not create the previously proposed `decision: define Orcest's provider-neutral GitHub mutation authority contract` issue. It would reopen the policy question that #573 closed as settled.

The approved spec documents the current facts instead:

- #649 improves the trusted merge path but is not hostile-agent enforcement.
- The Claude hook has narrow, installation-dependent command coverage.
- Raw GitHub permissions couple desired and undesired mutations.
- Token separation improves auditability/blast radius but is not a sufficient ref/operation boundary.

Only an explicit repository-owner request to reconsider the threat model authorizes preparation of a human-owned authority clarification. Rulesets additionally require an explicit superseding decision for #573.

## Filing procedure used

1. Re-check the live open backlog and default branch immediately before filing.
2. Ensure every issue body is self-contained; do not rely on the local spec being visible on GitHub. Link the spec only after it exists on the default branch.
3. Create issues 1, 5, and 6 first.
4. Create issue 2 with `Blocked by #655`.
5. Create issue 3 blocked by #657, then issue 4 blocked by #659.
6. Create issue 7 blocked by both #656 and #658.
7. Apply the proposed labels, including `orcest:ready` on blocked engineering leaves; Orcest's dependency resolver will defer them.
8. Verify title/body/labels, dependency rendering, and that none was added to #622.
9. Do not create an epic or authority issue.

## Deferred findings

Playwright installation/browser journeys, narrow architecture checks, provider-neutral docs, file-growth ratchets, target-repository harness contracts, parallel-safe dashboard image smokes, and monitor-image locking remain plausible later issues. They are intentionally outside this seven-issue batch.
