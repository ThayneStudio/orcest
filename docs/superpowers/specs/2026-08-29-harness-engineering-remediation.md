# Harness Engineering Remediation Specification

> **Historical archive — 2026-09-30:** This August planning record is
> preserved for incident and design provenance. Its status, checklists,
> implementation steps, and agent instructions describe the original
> session; they are not current execution instructions or deployment
> evidence. See the [archive disposition](../local-planning-archive-disposition.md)
> for implemented work and remaining operational evidence.

**Status:** Reviewed and revised

**Evidence baseline:** `origin/master` at `a7eed62a5f9470d825182d91fa97d0cd9680fb8f`, fetched 2026-08-29

**Source:** [Harness engineering: leveraging Codex in an agent-first world](https://openai.com/index/harness-engineering/)

**Review record:** Three independent reviewers audited issue-result correctness, GitHub authority/security, and test/CI feasibility against the baseline above. This revision incorporates their blocking findings.

## Summary

Orcest already has strong conventional quality controls: typed Python, Ruff, mypy, focused tests, guarded PR merging, provider abstractions, and operational checks. The highest-value gaps exposed by the Harness Engineering review are narrower:

1. Orcest treats provider exit success as proof that an issue implementation was delivered, even when no usable closing pull request exists.
2. Redis-backed local test commands share a fixed Compose project, port, and database, so concurrent agents and worktrees can interfere with one another.
3. Local and CI verification restate commands independently and CI resolves floating development and build dependencies.
4. Orcest's current GitHub model trusts provider processes holding raw write credentials. Its merge gate governs cooperative Orcest behavior; it is not a hostile-agent authority boundary.

This specification defines engineering changes for the first three gaps. For the fourth, it accurately records the existing trusted-worker/no-ruleset posture established by #573. It does not reopen that decision or authorize a new authority issue.

## Harness principles applied

- **Judge outcomes, not agent narration.** Live GitHub state is the authority for whether an issue task produced a usable handoff.
- **Make feedback deterministic and agent-runnable.** Local commands and CI share named leaf targets and locked inputs.
- **Assume concurrent agents.** Test services are invocation-scoped rather than owned by a checkout or host.
- **Turn important conventions into mechanisms.** Prompts and optional provider hooks can reduce mistakes, but they do not replace verifiable state or least privilege.

## Goals

- Separate provider execution success from verified issue-delivery success.
- Keep `orcest:ready` until a live, actionable PR is proven to close the exact issue.
- Make publication, verification, label changes, and retries crash-safe and idempotent.
- Prevent result-stream trimming, TTL expiry, or stale results from causing duplicate issue work.
- Resume authoritative partial work without injecting unbounded agent output into later prompts.
- Make Redis-backed suites safe to run simultaneously from multiple worktrees.
- Give humans, agents, and CI a documented family of verification targets.
- Lock the Python development, test, and build inputs consumed by CI.

## Non-goals

- Proving that an implementation is correct or that the provider performed no unrelated GitHub mutation. Verified delivery proves only that a qualifying handoff snapshot exists.
- Requiring CI, review approval, or mergeability before accepting an issue-task handoff. Those are PR-lifecycle concerns. Open issue #649 separately strengthens current-head merge evidence.
- Automatically merging the pull request created by an issue task.
- Reopening #573, proposing rulesets, or changing repository settings without explicit owner authorization to reconsider that settled policy.
- Treating token naming, actor naming, prompts, or provider-specific hooks as sufficient security boundaries.
- Adding Playwright journeys, broad architecture rules, file-size ratchets, documentation gardening, or a target-repository manifest in this batch.
- Changing #147's deliberate integration event policy.

## Trust model

The specification distinguishes three postures:

- **Cooperative provider:** prompts, orchestration checks, and optional hooks reduce mistakes.
- **Faulty or instruction-disobeying provider:** outcome verification judges the requested handoff, but it cannot prevent unrelated mutations made with a raw credential.
- **Hostile or compromised provider:** raw secrets and general GitHub write access must be absent. Enforcement would require a constrained trusted publisher/broker or separately owner-approved platform rules. That architecture is outside this specification.

`orcest:ready` is a privileged prompt-admission control. An issue author can edit title/body text that Orcest renders into a provider prompt. This batch records a hash of the exact prompt inputs used for each task generation for audit/correlation, but it does not claim that the maintainer who applied the label approved a particular historical revision or enforce reapproval after edits. Strong revision-bound admission requires a separate design.

## System invariants

1. Only the orchestrator can transition issue delivery to verified.
2. Provider echoes and claims are diagnostic in both directions: they can neither prove success nor force failure when authoritative live state verifies.
3. Every issue result enters a durable per-task admission ledger before status-specific side effects. A completed result is ACKed only after its verification job and due index are atomically durable.
4. Discovery treats every nonterminal verification job as an issue-level dispatch barrier, independent of the expiring pending-task marker.
5. Nonterminal verification state and its copied expected outcome do not expire. Reference-aware cleanup occurs only after a terminal saga finishes.
6. Missing or corrupt Orcest state becomes `UNVERIFIABLE`, never provider failure and never automatic redispatch.
7. Terminal verification phases are immutable. `VERIFIED` is persisted before mutating labels and never reclassifies from later GitHub state or a conflicting result payload.
8. Every task generation and cleanup uses compare-and-set semantics so stale results cannot erase newer work.
9. No fixture may issue `FLUSHDB` unless it has validated the invocation identity of the selected ephemeral Redis instance.
10. CI and local composites are composed from the same named leaf commands, while CI-only checks remain explicitly listed.
11. #573 remains the authority policy until the repository owner explicitly authorizes reconsideration.

## Workstream A — Verified issue-task delivery

### Current failure

`publish_issue_task()` renders a deterministic branch name inside the prompt but creates `Task(branch=None)`. The worker maps any successful provider exit to `ResultStatus.COMPLETED` and returns no branch. `_handle_result()` then clears attempts and removes `orcest:ready` without proving a branch or PR exists.

The current result PEL cannot serve as a long-lived verification queue. Worker result streams are capped, pending entry bodies can be trimmed, and the issue pending marker has a finite TTL. Leaving a completed result unACKed during a long GitHub outage can therefore lose the record and allow another provider dispatch.

### A1. Generation and idempotent publication contract

Reserve a monotonic issue generation in the project Redis before publishing:

```text
IssueTaskGeneration v1
  repo
  issue_number
  generation
  task_id
  prompt_content_sha256
  expected_head_owner
  expected_branch
  created_at
  publish_state: prepared | published | ambiguous
  stream_entry_id?
```

The expected branch is derived once from the snapshotted issue number/title. The same value feeds the prompt renderer and record. Add optional `expected_branch` and `issue_generation` fields to `Task`; do not overload `Task.branch`, which means an existing branch that workspace setup should check out.

Publication must tolerate a lost `XADD` response across project Redis and task-stream Redis:

1. Atomically reserve the pending marker, generation, expected outcome, and attempt in project Redis.
2. Publish with a task-stream-side idempotency script keyed by deterministic `task_id`. The script returns the original stream ID on retry instead of appending a duplicate.
3. Mark the project record `published` with that stream ID.
4. Roll back project state only after a provably pre-publish failure. A timeout or lost response becomes `ambiguous`; it keeps the generation and reconciles through the task-stream idempotency key.

Generation and publication records remain until no task-stream receipt, pending marker, result, verification job, or retry context references them. A reference-aware sweeper may remove terminal or provably orphaned records; a wall-clock TTL alone may not.

### A2. Typed GitHub adapter and shadow evidence

Add a typed adapter that obtains:

- the repository default branch;
- the exact expected remote branch ref and head OID, if it exists;
- all candidate PRs for the expected same-repository head branch;
- state, draft status, base repository/ref, head repository owner/ref/OID, URL, and number;
- the complete canonical `closingIssuesReferences` connection for each candidate.

Every paginated connection must report completeness. Page-cap exhaustion, missing cursors, null/truncated fields, malformed responses, or partial closing-reference data cannot verify. The adapter exposes typed error classes that distinguish transient transport/rate failures from persistent authentication, permission, and schema failures.

Before enforcement, unit tests and a read-only shadow path may emit bounded evidence without changing labels or dispatch behavior.

### A3. Durable verification job and enforcing state machine

Before any issue-result status reaches existing or new side effects, admit it through a per-task ledger:

```text
IssueResultAdmission v1
  repo
  issue_number
  generation
  task_id
  result_status
  result_fingerprint
  phase: admitted | routed | terminal | quarantined
  admitted_at
```

The first payload for a task wins a CAS reservation. An identical fingerprint is a replay and resumes the already recorded route. A different fingerprint or status is a conflict and cannot enter a second handling path:

- If the owning job is nonterminal, freeze it as `UNVERIFIABLE` and alert.
- If the original route is already terminal, quarantine and alert the later payload without changing terminal state or creating an issue-wide barrier over a newer generation.
- In particular, a conflict after immutable `VERIFIED` cannot demote it, and a conflict after `INEFFECTIVE` cannot resurrect stale work.

Non-success issue results retain their existing outward behavior only after admission and routing through this ledger. The ledger remains referenced until the generation and every result/job side effect are terminal.

When the winning admitted status is `COMPLETED`, create a separate durable verification job before ACKing the result stream entry:

```text
IssueVerificationJob v1
  repo
  issue_number
  generation
  task_id
  result_fingerprint
  copied expected outcome
  phase: due | pending | verified | ineffective | unverifiable
  reason_code
  first_checked_at?
  next_check_at
  observed branch/PR evidence?
  ready_removed_at?
  cleanup_completed_at?
```

Verification-job admission uses a CAS on repository/issue/generation and the already admitted result fingerprint. The job record and its due-time sorted-set member are created or validated atomically in one project-Redis transaction/script. Only after both are durable may Orcest ACK the result and release provider/account tracking.

An identical completed-result replay repairs/validates both the job and due index before ACK. A periodic reconciler also scans nonterminal jobs for missing/stale due membership and repairs it idempotently. This is defense in depth; it does not replace atomic admission. A job that is not due is skipped quietly rather than raised as a generic retryable result error, avoiding one warning per orchestrator poll.

```text
any issue result
  └─ durable result-ledger CAS
      ├─ identical replay ──> resume recorded route
      ├─ conflicting payload ──> freeze nonterminal job or quarantine after terminal
      ├─ winning non-success ──> existing status handling through ledger
      └─ winning COMPLETED
          └─ atomic job + due-index admission
              ├─ corrupt or missing expected state ──> UNVERIFIABLE
              └─ durable ──> ACK result + release provider tracking ──> DUE

DUE/PENDING verification
  ├─ transient transport/rate failure ──> PENDING with capped backoff
  ├─ auth/permission/schema/incomplete pagination ──> UNVERIFIABLE
  ├─ one or more exact valid PRs ──> persist immutable VERIFIED
  ├─ no exact PR and grace remains ──> PENDING
  └─ no exact PR after grace ──> INEFFECTIVE

VERIFIED saga
  └─ persist phase → remove ready label → checkpoint label mutation
     → generation-CAS cleanup → clear dispatch barrier

INEFFECTIVE saga
  └─ persist machine-derived retry record + cooldown
     → generation-CAS cleanup → keep ready label → clear dispatch barrier after cleanup

UNVERIFIABLE
  └─ keep ready label and durable dispatch barrier; alert for operator resolution
```

#### Valid handoff

Delivery is verified when at least one fully observed candidate satisfies all of these conditions:

- Base repository equals the task repository.
- Head repository owner and branch exactly match the orchestrator-owned expected outcome.
- PR is open and not draft.
- Base ref equals the repository's current default branch.
- Head OID is non-empty and equals the authoritative expected remote-ref OID observed in the same verification pass.
- Canonical closing references contain the exact repository and issue number.

If more than one candidate qualifies, choose the lowest PR number for stable evidence, emit an ambiguity counter, and still verify: the required handoff exists. PR body substring matching never qualifies.

All non-verifying observations during the eventual-consistency grace period remain pending unless the failure is an Orcest/auth/schema condition that makes the job unverifiable. A wrong or partial candidate does not prove that the exact expected PR will not appear. After grace, complete authoritative evidence with no valid candidate becomes ineffective.

Provider result echoes never drive this transition. Echo mismatch is a metric and diagnostic event only.

#### Missing legacy expected outcome

For a task published before A1, attempt a resource-level reconciliation using complete canonical closing references without recomputing a branch slug from the mutable current issue title. A fully observed, open, non-draft, same-repository PR targeting the default branch may satisfy the handoff. If none can be proven, transition to `UNVERIFIABLE`; do not auto-dispatch duplicate work.

#### Saga and stale-generation safety

Persist `VERIFIED` before removing `orcest:ready`. Once verified, retries finish the recorded saga without re-querying mutable GitHub state. Label removal is idempotent.

Resource-level verified delivery may remove `orcest:ready` even if a newer generation exists, but cleanup of attempts, pending markers, retry context, and expected records is conditional on the job's generation/task ID. Ineffective and unverifiable jobs may never overwrite or clear newer state.

Discovery checks the verification-job barrier before using the legacy pending marker/attempt heuristics. A GitHub outage can therefore outlive pending-marker TTL and result-stream trimming without dispatching another provider.

### A4. Bounded retry context and authoritative resume

The enforcing job owns the minimal machine-derived ineffective record and cooldown needed for a safe transition. A separate follow-up renders that record into prompts and resumes partial work.

Store only the latest retry context per repository/issue/generation, with a serialized maximum of 4 KiB. It contains allowlisted fields: task/generation, expected branch, authoritative remote branch/head, canonicalized PR number/URL, reason code, and timestamps. It never includes PR titles/bodies/comments, issue comments, provider traces, or model summaries.

Render the data as canonical fixed-schema JSON in a fenced block. Validate refs and SHAs, canonicalize the URL from repository/PR number, and escape control characters. A heading such as “diagnostic data, not instructions” is explanatory, not the security mechanism.

Resume is allowed only for the authoritative same-repository expected ref. The issue task still starts from the default checkout; its prompt explicitly fetches the remote expected ref and checks it out. A wrong-owner or unexpected ref is never resumed. Successful verification deletes retry context/cooldown only with a generation CAS.

### Compatibility and rollback

- New task/result fields are optional and deserialize safely when absent.
- A new orchestrator can verify old-worker work because it owns the expected record and queries GitHub.
- Missing pre-schema records reconcile by canonical issue relationship and otherwise become operator-visible `UNVERIFIABLE`.
- PR-task behavior remains unchanged. Non-success issue-result outward behavior remains unchanged after the new all-status admission ledger prevents cross-status conflicts.
- Rolling forward requires the A1 publication contract before A3 enforcement.
- Rolling back to the old orchestrator is unsafe while completed issue results or verification jobs from A3 exist: the old code would consume a completed result and remove the ready label without verification. Before rollback, pause issue dispatch/result consumption and drain or quarantine affected results/jobs. Do not run the old orchestrator against affected project state.

### Observability

Emit structured events for job admission and every phase transition with repository, issue, task, generation, status, reason, expected branch, and bounded candidate identifiers. Add counts for due/pending/verified/ineffective/unverifiable, ambiguity, echo mismatch, and latency. Alert on unverifiable jobs and age of the oldest pending job.

### Workstream A tests

- Schema round-trips, legacy payloads, and prompt/outcome branch equality.
- Idempotent task publish, lost `XADD` response, ambiguous reconciliation, and pre-publish cleanup.
- Result-stream trimming after durable admission does not lose verification.
- Crash after job creation but before due-index creation cannot produce an unindexed job; crash after atomic scheduler admission but before result ACK replays idempotently.
- Pending-marker expiry during a verification outage cannot dispatch a second task.
- Identical duplicate results; FAILED/BLOCKED/USAGE then COMPLETED and COMPLETED then non-success conflicts; conflicts after terminal VERIFIED/INEFFECTIVE are quarantined without changing terminal or newer-generation state.
- Stale generation cannot overwrite or delete newer pending/retry state.
- Complete pagination, cap/truncation/null failures, and canonical closing-reference matching.
- No branch/PR, branch only, wrong repo/owner/ref/issue/base/OID, draft/closed PR, ambiguous valid PRs, and exact valid PR.
- Grace behavior, transport/rate backoff, and operator-blocked auth/permission/schema failures.
- Crash/retry at durable verified phase, label mutation, checkpoint, generation cleanup, and barrier removal.
- Legacy reconciliation without title-slug recomputation.
- Retry JSON validation/escaping and same-ref resume commands.
- An old-orchestrator rollback test proving why affected state must be drained/quarantined.

## Workstream B — Hermetic Redis-backed test harness

### Current failure

`make test`, `redis-up`, and `redis-down` operate one repository-level Compose project at `127.0.0.1:6379`. Fixtures default to database 15, skip when Redis is absent, and call `FLUSHDB` around each test. Concurrent worktrees can stop or erase each other's state, and a mistaken URL can point cleanup at an unrelated Redis instance.

### Dedicated ephemeral service

Use a new test-only Compose file; do not extend `docker-compose.redis.yml`, whose fixed port, named volume, restart policy, and explicitly shared network can survive Compose merges.

The test service uses no named volume or shared network and publishes Redis through long syntax with `host_ip: 127.0.0.1`, `target: 6379`, and `published: "0"`. A small Python supervisor:

1. Generates a unique sanitized Compose project name and random invocation nonce.
2. Starts the service and discovers the Docker-assigned host port with `docker compose ... port redis 6379`.
3. Writes the nonce to a reserved key in database 0.
4. Exports `ORCEST_TEST_REDIS_URL` for database 15 and the test-only nonce.
5. Starts the requested command in its own process group.
6. On `INT`/`TERM`, forwards the signal to the process group, waits a bounded grace period, escalates if needed, and cleans exactly once.
7. Runs `docker compose down --volumes --remove-orphans` for the exact project.

Exit semantics are explicit: normal runs return the exact child status; `INT` returns 130; `TERM` returns 143; cleanup failure changes the result to nonzero only when the child succeeded. Containers carry labels identifying the harness and creation time for bounded manual cleanup of stale runs. The normal supervisor never deletes a project it did not create.

The nonce is an invocation-identity guard against accidental destructive cleanup, not a hostile-security proof; another process with Redis write access could spoof it.

### Fixture guard

Remove the unsafe default URL. Managed real-Redis tests require both URL and nonce. Before setup and teardown `FLUSHDB`, the fixture:

- rejects database 0;
- reads database 0 and checks the invocation marker;
- fails, rather than skips or warns, on missing/wrong proof;
- closes the client in `finally`, including when the teardown guard fails.

Direct `pytest` may still use an explicitly provisioned external Redis only if the operator seeds matching proof. Otherwise real-Redis tests fail closed.

### Test selectors and Make behavior

- `test-unit`: `pytest -m unit` with existing coverage behavior.
- `test-integration`: managed Redis plus `pytest -m integration`, including inline integration markers outside `tests/integration`.
- `test-stress`: managed Redis plus `pytest -m stress`.
- `test-dashboard`: existing clean-copy dashboard check.

Retire or alias the existing shared-service `make test` target to the canonical local full-check aggregate. No correctness target may depend on `redis-up` or `redis-down`. Those names may remain only as documented manual-development helpers.

### Workstream B tests

- Two small blocking harness clients overlap and prove distinct projects, containers, ports, nonces, and keyspaces.
- One invocation cannot stop or flush the other.
- Normal success/failure and `INT`/`TERM` prove process-group forwarding, bounded escalation, one cleanup, and exact exit semantics.
- Wrong/missing URL or nonce and database 0 refuse both setup and teardown flushes.
- Integration and stress selectors include inline markers as documented and each suite passes once through the supervisor.
- Stale labels are discoverable without granting the normal supervisor broad deletion behavior.

## Workstream C1 — Locked development, test, and build inputs

This work is independent of Redis isolation.

Generate a dedicated development lock from the `dev` extra, including PEP 517 build requirements. Pin and document Python 3.12, pip, and pip-tools used for regeneration. Prefer compiling with all build dependencies and constrain shared runtime packages by the existing runtime lock where compatible.

CI installs the lock, then installs Orcest editable with both `--no-deps` and `--no-build-isolation`. Cache keys hash the development lock and any runtime constraint it consumes. A check target regenerates to a temporary file and compares it with the committed lock without mutating the checkout. README bootstrap instructions use the same locked path.

Acceptance requires a clean, network-independent-after-artifact-download environment to install without resolving new versions and run the fast checks. The monitor image's unlocked `.[monitor]` install remains a separately scoped reproducibility gap.

## Workstream C2 — Canonical local check DAG and CI leaf parity

“Parity” means that CI and local composites call the same leaf commands. It does not mean one local aggregate exactly duplicates every CI-only build.

Leaf targets:

- `lint-check`: Ruff lint plus Ruff format check.
- `typecheck`: mypy.
- `test-unit`, `test-integration`, `test-stress`, and `test-dashboard` as defined above.

Local composites:

```text
check-fast = lint-check + typecheck + test-unit
check-full = check-fast + test-integration + test-stress + test-dashboard
```

Keep root image builds and dashboard image/Compose smokes CI-only in this batch. They are not currently parallel-safe: a shared image tag can cross worktrees, and the Compose smoke's reserve-then-release port selection has a TOCTOU race. A separate issue must make those leaves parallel-safe before adding them to `check-full`.

CI preserves the six job names required by `master-verified.yml`: `lint`, `typecheck`, `test`, `dashboard`, `integration`, and `docker`. Python/dashboard/integration jobs invoke the same Make leaves used locally. The integration job removes its fixed-port Actions Redis service once the leaf owns an ephemeral service, but preserves the semantic event gate from #147. Stress remains local-only. The Docker job may continue using buildx actions; its CI-only build is documented rather than falsely represented as local parity.

Structural regression tests assert:

- all six job names remain;
- integration still runs only on master push, schedule, or manual dispatch;
- deterministic jobs call the named leaves and locked install path;
- local composites contain the documented transitive prerequisites using Make's rule database or behavioral command capture, not only `make -n` text matching;
- no correctness target depends on shared `redis-up/down`.

Network advisory checks stay non-blocking and outside deterministic aggregates. The independent master verification from #567 remains unchanged.

## Workstream D — Current GitHub authority contract

### Controlling policy

#573 is explicit: Orcest and served repositories will not use branch protection/rulesets, and the proposal must not be refiled. This specification records that as the current default; it does not create a new decision issue or offer platform enforcement as an actionable exit.

Open #649 should proceed independently. It improves freshness and action-time validation on Orcest's trusted merge path. It does not require an allowlisted independent reviewer, prevent direct base pushes/merges, or protect against a provider using raw credentials outside that path.

The tracked Claude hook attempts to deny recognizable `gh` merge invocations in Claude sessions where this project/user hook is installed. It is not installed by fleet cloud-init into served repositories, does not cover Codex/Grok, and is not a server-side boundary. Curl, Python, other binaries, direct base pushes, and non-Bash tools are outside its evidenced protection.

### Credential coupling

- GitHub `Contents: write` enables authenticated pushes and merge API operations.
- Pull Requests write enables PR creation and review/approval mutations.
- Issues write enables ordinary issue workflows and mutation of `orcest:ready` and `orcest:needs-human`, which form part of Orcest's execution control plane.

Separating worker/reviewer/merger tokens can reduce blast radius and improve auditability, but it cannot express “push this exact task ref and create this PR, never merge/approve/control-label” while raw general credentials remain in the provider process. It is not a sufficient authority boundary.

Current master serializes `Task.token` into the task stream, exports it as `GH_TOKEN`/`GITHUB_TOKEN`, and uses it through the workspace credential helper. A future hostile-provider design must remove raw write tokens from the provider-consumed wire and environment. A constrained trusted publisher inside the worker boundary is valid if the provider cannot access its credential and it validates repository, ref, ancestry, mutation type, and control-label operations; a separate network broker is not mandatory.

### Owner-authorized clarification only

No authority issue belongs in the default creation batch. If the repository owner explicitly asks to reconsider the threat model, prepare a human-owned `security` + `question` issue, assign a named owner, forbid all `orcest:*` labels, and require an explicit recorded approval before closure. It should clarify cooperative/faulty/hostile posture and operation permissions without treating #573 as open. Reconsidering platform enforcement requires a separate explicit owner instruction that supersedes #573.

## Delivery sequence and issue boundaries

```text
A1 generation + idempotent publication
  └─ A2 typed GitHub adapter + shadow evidence
      └─ A3 durable enforcing verification job
          └─ A4 bounded retry prompt + authoritative resume

B hermetic Redis harness ──┐
                           ├─ C2 canonical check DAG / CI leaf parity
C1 locked dev/build inputs ─┘

D current authority contract: no issue by default
```

A1, B, and C1 can proceed independently. A2 waits for A1; A3 waits for A2; A4 waits for A3. C2 waits for both B and C1. Each enforcing state transition and its minimal durable state lives in A3; A4 owns only prompt rendering and resume behavior.

## Rollout and rollback

- Land A1 before A2/A3, then deploy the orchestrator publication contract before relying on new worker echoes.
- A2 may ship read-only. A3 enforcement begins only after its scheduler, barrier, and recovery tests pass together.
- Watch job counts, oldest pending age, unverifiable alerts, ineffective reasons, and generation conflicts.
- Before rolling A3 back, pause issue dispatch/result consumption and drain or quarantine affected jobs/results. Old code is not compatible with A3 state.
- B, C1, and C2 roll out independently of issue-result behavior.

## Acceptance summary

The specification is satisfied when:

- A zero-exit issue task without an exact live closing PR cannot lose `orcest:ready`.
- A correct current or prior-attempt PR is accepted idempotently without another provider run.
- Result trimming, pending-marker expiry, GitHub outages, and stale generations cannot dispatch duplicate work.
- Missing/corrupt Orcest state blocks for operator review rather than blaming or rerunning a provider.
- Retry prompts carry only canonical, bounded, system-derived data and resume only the expected same-repository ref.
- Two Redis-backed harness invocations run concurrently without sharing ports, containers, keyspaces, or cleanup authority.
- CI and local checks call the same deterministic leaves from a locked development/build environment while CI-only checks remain explicit.
- The authority section accurately records the trusted-worker/no-ruleset policy and does not refile #573.
