# Workflow Control v1 — GitHub Issue Manifest

**Status:** Proposed; no GitHub issues or milestones have been created or
modified.

**Source specification:** `docs/wiki/` as accepted in
`docs/wiki/spec-writing-plan.md` on 2026-08-27 and hardened by the recorded
2026-08-29 adversarial follow-up.

**Purpose:** Convert the accepted workflow-control specification into
dependency-ordered, independently testable implementation leaves. This plan
does not weaken or reinterpret the normative wiki.

## Filing prerequisites

Before creating any issue:

1. Land the accepted `docs/wiki/` migration on the default branch so each issue
   can link to an immutable source revision.
2. Re-check the live open and closed GitHub backlog for duplicates and record
   any superseding issue number in this manifest.
3. Replace every symbolic dependency such as `V1-08` with the real issue
   number and add the native GitHub dependency relationship where available.
4. Confirm that Orcest defers `orcest:ready` issues whose declared blockers are
   open. If that check is unavailable, apply `orcest:ready` only as each leaf
   becomes unblocked.
5. Keep the seven harness-engineering issues in
   `docs/superpowers/plans/2026-08-29-harness-engineering-issue-creation.md` as
   a separate batch. They support implementation quality but do not replace
   any normative v1 leaf below.

Use a `Workflow Control v1` milestone rather than an umbrella issue. Issue
bodies must remain self-contained: the wiki is the canonical contract, but an
agent should understand the required change and acceptance tests from the
issue alone.

## Backlog rules

- Each issue owns one durable boundary, protocol, reducer capability, or
  rollout gate with an independently reversible change surface.
- Every implementation issue adds schema migrations, deterministic tests,
  replay tests, and migration notes for the objects it introduces.
- No worker- or model-authored prose receives lifecycle authority.
- No issue may substitute Redis for SQLite or durable object storage.
- `needs-human` remains restricted to the specification's closed exceptional
  reasons. Routine failures need an autonomous retry, wait, diagnosis, or
  recovery path.
- Production rollout issues may validate implementation choices, but cannot
  defer or weaken normative correctness.

## Dependency graph

```text
V1-01 contract registry
├─ V1-02 SQLite/run-store substrate ───────────────┐
│  ├─ V1-04 controller mode + signing keys         │
│  │  └─ V1-05 secret provision/adoption           │
│  ├─ V1-08 deterministic reducer                  │
│  └─ V1-23 restart/backup/retention               │
├─ V1-03 durable object stores ── V1-05 ──────────┤
└─ V1-06 repository configuration compiler         │
                                                   │
V1-04 + V1-05 + V1-06 ── V1-07 project onboarding │
V1-07 + V1-08 ────────── V1-09 forge observation  │
V1-06 + V1-08 + V1-09 ── V1-10 snapshot/admission │
V1-10 ────────────────── V1-11 activity/attempt    │
V1-04 + V1-05 + V1-11 ── V1-12 claim/launch       │
V1-03 + V1-12 ────────── V1-13 Candidate transfer │
V1-11 + V1-12 + V1-13 ── V1-14 Result/terminal    │
V1-05 + V1-12 + V1-14 ── V1-15 rotation           │
V1-10 + V1-14 ────────── V1-16 work loop          │
V1-09 + V1-14 + V1-16 ── V1-17 recovery/waits     │
V1-13 + V1-14 + V1-16 ── V1-18 verification       │
V1-12 + V1-17 + V1-18 ── V1-19 consensus          │
V1-09 + V1-18 + V1-19 ── V1-20 publication        │
V1-17 + V1-20 ────────── V1-21 PR lifecycle       │
V1-04 + V1-07 + V1-21 ── V1-22 API/projection     │
all durable object owners ─ V1-23 recovery/storage │
V1-12..V1-23 ─────────── V1-24 system validation  │
V1-23 + V1-24 ────────── V1-25 staged rollout ────┘
```

`V1-01` is the first executable leaf. `V1-02`, `V1-03`, and `V1-06` can begin
in parallel as soon as it establishes the canonical identities and
serialization rules.

## Issue V1-01 — Versioned contract registry and conformance fixtures

**Proposed title:** `workflow v1: add the versioned contract and digest registry`

**Labels:** `enhancement`, `critical`, `orcest:ready`

**Dependencies:** None.

**Canonical owners:** `domain-model.md`, `workflow-lifecycle.md`,
`worker-protocol.md`.

**Purpose:** Give all later components one code-owned definition of v1 IDs,
enums, tagged unions, canonical JSON, digests, and protocol versions.

**Required scope:**

- Define the closed v1 identity, state, reason, trigger, outcome, receipt, and
  operation enums without lifecycle logic in model output.
- Implement canonical serialization and domain-separated digest helpers for
  Workflow Blobs, policy, assignments, evidence, results, and requests.
- Add strict versioned request/response validators that reject unknown fields.
- Check in golden fixtures for every normative JSON/YAML example and important
  digest preimage.
- Expose compatibility helpers used by SQLite migrations, HTTP handlers, Redis
  envelopes, and forge adapters.

**Acceptance tests:**

- Every normative enum and tagged union round-trips through one registry.
- Unknown versions, fields, enum values, and ambiguous nullability fail closed.
- Golden canonicalization and digest vectors are stable across processes.
- A repository-wide test prevents feature code from defining shadow protocol
  constants or non-domain-separated digests.

## Issue V1-02 — SQLite single-writer and durable transaction substrate

**Proposed title:** `workflow v1: add the SQLite single-writer run-store substrate`

**Labels:** `enhancement`, `critical`, `orcest:ready`

**Dependencies:** `Blocked by V1-01`.

**Canonical owners:** `persistence-and-recovery.md`, `domain-model.md`.

**Purpose:** Establish the durable authority and transactional primitives used
by every workflow object before feature tables are added.

**Required scope:**

- Add schema versioning, forward migrations, foreign-key enforcement, WAL,
  `synchronous=FULL`, local-filesystem checks, and one controller writer.
- Add typed transaction helpers for immutable facts, monotonic revisions,
  source-unique inserts, generation-independent trigger consumption, and CAS.
- Add base Transition, Outbox, Projection Outbox, controller-state, and durable
  operation/idempotency primitives.
- Define startup integrity checks and fail-closed behavior for unsupported
  schema or reducer versions.
- Provide transaction fault injection before commit, after commit, and before
  response acknowledgement.

**Acceptance tests:**

- Two writers cannot acquire workflow authority concurrently.
- Replay returns the committed row/response and never duplicates a Transition
  or Outbox effect.
- Crash tests cover every transaction boundary and WAL recovery.
- Foreign-key, uniqueness, sequence, and CAS violations fail atomically.
- Unsupported schema/reducer versions enter the specified maintenance mode.

## Issue V1-03 — Durable Workflow Blob, Candidate, and Secret stores

**Proposed title:** `workflow v1: add durable object stores and integrity primitives`

**Labels:** `enhancement`, `critical`, `orcest:ready`

**Dependencies:** `Blocked by V1-01`.

**Canonical owners:** `architecture.md`, `persistence-and-recovery.md`,
`domain-model.md`.

**Purpose:** Implement the byte-storage boundary that makes SQLite references
durable and keeps secrets outside SQLite and Redis.

**Required scope:**

- Implement domain-separated Workflow Blob and Candidate identities with
  no-clobber write, fsync-file, fsync-directory, validate, and atomic promotion.
- Implement a Secret Store adapter with versioned opaque references and keyed
  integrity attestations; never expose values through logs, SQLite, Redis, or
  ordinary API responses.
- Add storage locks and the exact write-before-reference ordering required by
  the schema.
- Provide exact-object read/verify APIs suitable for startup audit,
  restoration, backup, and garbage collection.
- Add quota and free-space rejection before accepting bytes.

**Acceptance tests:**

- Crashes at every write/promotion/reference boundary leave either no object or
  the exact durable object, never a dangling live reference.
- Same bytes in different Workflow Blob media kinds do not alias.
- Secret values are absent from captured logs, database pages, Redis payloads,
  exceptions, and API fixtures.
- No-clobber and integrity mismatch paths fail closed under concurrent writers.

## Issue V1-04 — Controller Mode and capability signing-key registry

**Proposed title:** `workflow v1: implement controller modes and capability key operations`

**Labels:** `enhancement`, `critical`, `orcest:ready`

**Dependencies:** `Blocked by V1-01`, `Blocked by V1-02`, `Blocked by V1-03`.

**Canonical owners:** `domain-model.md`, `architecture.md`,
`operations-and-rollout.md`.

**Purpose:** Create the fail-closed bootstrap and operational gate required
before the controller may dispatch or accept workflow mutations.

**Required scope:**

- Implement Controller Mode initialization and the closed mode transition
  matrix, including nested maintenance-prior behavior and idempotent operations.
- Implement the revision-0 capability-key registry bootstrap, REGISTER, SELECT,
  RETIRE, and emergency REVOKE operations with CAS and stored responses.
- Bind issued capabilities to signer key ID, algorithm, claim digest, JTI, and
  the exact immutable workflow assignment.
- Retain verification material for every promised replay lifetime and fail
  closed when the selected issuance key is absent or invalid.
- Gate offer creation, forge scheduling, Result mutation, and management
  operations exactly as required by the current mode.

**Acceptance tests:**

- A new controller can initialize in maintenance, register a key, and select it
  without fabricated history.
- Every legal and illegal mode/key transition is table-tested.
- Key rotation preserves accepted replay; revoked-key behavior matches the
  emergency contract.
- No Attempt/outbox is planned without both an issuing key and a permitting
  controller mode.

## Issue V1-05 — Secret provisioning, adoption, and version authority

**Proposed title:** `workflow v1: add idempotent secret provision and adoption operations`

**Labels:** `enhancement`, `critical`, `orcest:ready`

**Dependencies:** `Blocked by V1-02`, `Blocked by V1-03`, `Blocked by V1-04`.

**Canonical owners:** `domain-model.md`, `persistence-and-recovery.md`,
`repository-configuration.md`.

**Purpose:** Allow authenticated server-side secret setup without SSH or raw
secret material entering repository configuration.

**Required scope:**

- Add authenticated MANAGEMENT_PROVISION provision/adopt operations with
  request digest, byte-bound keyed metadata, target-version reservation, and
  exact PENDING/COMPLETED/REJECTED responses.
- Implement deterministic target-version CAS and safe reservation release for
  terminal rejection.
- Persist Secret Reference, Secret Version, creation receipt, authority
  principal, and immutable operation provenance.
- Reconcile response loss, staged-object loss, stale prior versions, revoked
  authority, and integrity conflicts without leaking secret bytes.
- Retain operation-bound replay metadata for the operation's audit lifetime.

**Acceptance tests:**

- Identical replay returns the same response; the same key with different
  secret bytes conflicts without revealing either value.
- Crash at every staging/install/SQLite boundary resumes the same target
  version or reaches a closed rejection.
- A corrected request can reuse a version reservation released by rejection.
- Stage-0 private signing and forge credentials can be adopted with real
  receipts before Project registration.

## Issue V1-06 — Repository-owned configuration compiler and bundle pinning

**Proposed title:** `workflow v1: compile and pin repository-owned workflow configuration`

**Labels:** `enhancement`, `critical`, `orcest:ready`

**Dependencies:** `Blocked by V1-01`.

**Canonical owners:** `repository-configuration.md`, `domain-model.md`.

**Purpose:** Make `.orcest/` the deterministic repository-owned policy input
while keeping authority, secrets, and server policy outside the repository.

**Required scope:**

- Parse and strictly validate `.orcest/project.yaml`, workflow configuration,
  prompts, implementation/repair profiles, verification commands, review
  slots, budgets, publication policy, and recovery limits.
- Resolve the trusted default branch to one immutable commit and fetch every
  referenced file at that exact revision.
- Normalize configuration, server policy, execution-profile classifications,
  and prompt bytes into versioned Workflow Blobs and one policy/bundle hash.
- Reject executable repository state machines, unknown fields, secret values,
  PR-head policy, path traversal, cyclic references, and nondeterministic order.
- Provide a local `orcest project prepare/validate` command that performs the
  same compilation without mutating a server.

**Acceptance tests:**

- Equivalent input produces the same normalized bundle across machines.
- Any referenced-byte or server-policy change changes the appropriate digest.
- PR-head configuration cannot control execution.
- Invalid references and all closed-schema violations produce actionable,
  secret-free diagnostics.

## Issue V1-07 — Authenticated Project registration and client onboarding

**Proposed title:** `workflow v1: add authenticated project onboarding and registration`

**Labels:** `enhancement`, `critical`, `orcest:ready`

**Dependencies:** `Blocked by V1-04`, `Blocked by V1-05`, `Blocked by V1-06`.

**Canonical owners:** `repository-configuration.md`, `domain-model.md`,
`architecture.md`.

**Purpose:** Replace root SSH/Proxmox configuration with an idempotent client
registration flow while preserving server-owned authorization.

**Required scope:**

- Add Project and Project Registration Operation storage with REGISTER and
  revision-CAS revalidation semantics.
- Resolve and pin installation/account identity plus source-read, publication,
  and forge-observation Secret References before accepting registration.
- Add the authenticated HTTPS `orcest project onboard` client/server flow with
  exact public response and separate internal resolution digest.
- Atomically create revision 1, reciprocal registration provenance, and the
  paused/active WORK_ITEM_DISCOVERY schedule required by controller mode.
- Forbid repository files from creating authority, principals, credentials,
  installations, or arbitrary controller policy.

**Acceptance tests:**

- Lost responses replay byte-identically; key/body conflicts and stale revision
  updates fail without changing Project state.
- Onboarding requires no Proxmox SSH and writes no secret value locally.
- Registration and discovery schedule either both commit or neither does.
- Concurrent registrations cannot steal an installation or rewrite authority.

## Issue V1-08 — Deterministic reducer and Transition engine

**Proposed title:** `workflow v1: implement the pure reducer and transition ledger`

**Labels:** `enhancement`, `critical`, `orcest:ready`

**Dependencies:** `Blocked by V1-01`, `Blocked by V1-02`.

**Canonical owners:** `workflow-lifecycle.md`, `domain-model.md`,
`persistence-and-recovery.md`.

**Purpose:** Put all lifecycle authority in deterministic code with durable,
single-consumption inputs.

**Required scope:**

- Implement the closed state graph, transition preconditions, trigger mapping,
  internal continuation arbitration, and generation handling as pure code.
- Persist exactly one source-unique Transition per Run and causal input,
  independent of later specification-generation changes.
- Atomically update Run pointers, plan Activities/Attempts, and write outboxes
  in the reducer transaction; perform no external I/O inside the reducer.
- Implement stale, duplicate, superseded, and same-state audit reductions.
- Generate exhaustive table-driven transition fixtures from the canonical
  contract registry.

**Acceptance tests:**

- Reordered equivalent input sets reach the same state and durable pointers.
- Duplicate triggers, response loss, and restart never repeat a semantic effect.
- Every legal state/trigger pair has one expected reduction and every unlisted
  pair fails closed.
- Reducer tests run without Redis, forge access, model access, or network I/O.

## Issue V1-09 — Durable forge observation scheduling and discovery

**Proposed title:** `workflow v1: add durable forge observation schedules and requests`

**Labels:** `enhancement`, `critical`, `orcest:ready`

**Dependencies:** `Blocked by V1-02`, `Blocked by V1-04`, `Blocked by V1-07`,
`Blocked by V1-08`.

**Canonical owners:** `forge-integration.md`, `domain-model.md`,
`persistence-and-recovery.md`.

**Purpose:** Replace implicit polling with crash-safe, authenticated,
source-unique forge observations.

**Required scope:**

- Add schedules, requests, request outboxes, typed observations, transient
  Forge Request Failure Facts, and stable response/replay handling.
- Implement repository-wide WORK_ITEM_DISCOVERY plus per-item work snapshot,
  base-head, publication, change-request, CI, and feedback observation kinds.
- Pin forge installation and logical credential identity/version per request.
- Coalesce due work deterministically, fence stale responses, and preserve
  PAUSED state across registration/discovery races.
- Rebuild schedules and outboxes after restart without synthesizing evidence.

**Acceptance tests:**

- Crash before/after external read or response persistence produces one
  observation/failure fact and a delivered/superseded outbox.
- Pagination, partial data, stale revisions, auth loss, and rate limits follow
  the closed adapter error taxonomy.
- Maintenance mode performs no ordinary scheduled reads.
- Discovery ordering and child schedule creation are deterministic.

## Issue V1-10 — Snapshot capture, policy composition, and Run admission

**Proposed title:** `workflow v1: implement immutable snapshot capture and admission`

**Labels:** `enhancement`, `critical`, `orcest:ready`

**Dependencies:** `Blocked by V1-06`, `Blocked by V1-08`, `Blocked by V1-09`.

**Canonical owners:** `workflow-lifecycle.md`, `domain-model.md`,
`forge-integration.md`.

**Purpose:** Turn an eligible forge Work Item into a fully pinned Run without
leaving a crash window or reading mutable policy later.

**Required scope:**

- Capture ordered Work Item and BASE_HEAD observations, normalized Workflow
  Blobs, effective server policy, budget, execution classification, and prompts.
- Implement Snapshot, Snapshot Generation, Policy Update, and bundle/policy
  hash invariants.
- Implement the restartable three-transaction admission chain:
  `ADMIT -> SPEC_SUPERSEDE -> INTERNAL/PLANNING`.
- Enforce one active Run per Work Item and retain source observation provenance.
- Handle specification, workflow, policy-only, and base changes with the exact
  safe-boundary semantics and mandatory new Plan.

**Acceptance tests:**

- Crashes at no-Run, ADMITTED/pending, ADMITTED/installed, and PLANNING phases
  reconcile without duplicate Runs or skipped planning.
- Policy composition always selects the correct ordered Work Item/base/policy
  inputs and is reconstructible later.
- Concurrent admission and source changes are fenced by immutable revisions.
- An unrelated default-branch commit does not stale a Run when its relevant
  bundle and base inputs are unchanged.

## Issue V1-11 — Durable Activity, Attempt, and Redis offer projection

**Proposed title:** `workflow v1: add durable activity attempts and Redis offer projection`

**Labels:** `enhancement`, `critical`, `orcest:ready`

**Dependencies:** `Blocked by V1-08`, `Blocked by V1-10`.

**Canonical owners:** `domain-model.md`, `worker-protocol.md`,
`persistence-and-recovery.md`.

**Purpose:** Make SQLite the authority for worker execution while keeping Redis
disposable and reconstructible.

**Required scope:**

- Persist Activity identity, semantic input, typed review assignment, ordered
  subjects/findings, generation, repeat cycle, and idempotency key.
- Persist immutable Attempt assignments, deadlines, provider/model/family
  classification, session fence, launch provenance, and terminal state.
- Atomically create OFFERED Attempt plus outbox; project secret-free versioned
  envelopes into Redis and rebuild them after Redis loss.
- Add source-unique Attempt Claim and result-delivery routing records.
- Ensure offer reapers and legacy workers cannot synthesize v1 Results or claim
  v1 work.

**Acceptance tests:**

- Redis flush/restart reconstructs exactly the durable open offers.
- Duplicate outbox delivery creates one logical offer.
- No Redis payload contains a secret or grants lifecycle authority.
- Activity/Attempt identity and assignment survive controller restart exactly.

## Issue V1-12 — Worker claim, launch isolation, liveness, capacity, and budget

**Proposed title:** `workflow v1: implement fenced worker claim and launch protocols`

**Labels:** `enhancement`, `critical`, `orcest:ready`

**Dependencies:** `Blocked by V1-04`, `Blocked by V1-05`, `Blocked by V1-11`.

**Canonical owners:** `worker-protocol.md`, `architecture.md`,
`workflow-lifecycle.md`.

**Purpose:** Admit an untrusted worker to one immutable Attempt using
short-lived, exact-bound capabilities and independently attested isolation.

**Required scope:**

- Implement durable claim replay, session fencing, claim/execution deadlines,
  signer-bound Attempt capability issuance, and bounded auth rematerialization.
- Implement one-shot Launch Attestation through the trusted runner shim with
  invocation/context/workspace isolation identity and provider-secret binding.
- Implement liveness sequencing, controller commands, authenticated Capacity
  Reports, Worker Loss Reports, cumulative Budget Reports, expiry, and fanout.
- Freeze provider/account/model/family/classification and enforce independence
  and budget gates before offer/claim.
- Implement claim-deadline classification and panel-wide staffing coalescence.

**Acceptance tests:**

- Duplicate/ambiguous claim and launch calls replay without creating a second
  session or authority grant.
- Expired, stale-generation, wrong-worker, wrong-session, and revoked-key
  capabilities cannot mutate an Attempt.
- Capacity/budget loss and expiry deterministically block or resume dispatch.
- A reused invocation/context/workspace cannot count as an independent review.

## Issue V1-13 — Candidate upload, download, promotion, and import

**Proposed title:** `workflow v1: implement durable Candidate artifact transfer`

**Labels:** `enhancement`, `critical`, `orcest:ready`

**Dependencies:** `Blocked by V1-03`, `Blocked by V1-12`.

**Canonical owners:** `worker-protocol.md`, `persistence-and-recovery.md`,
`domain-model.md`.

**Purpose:** Move complete Git commit objects across the worker boundary without
granting workers publication authority or admitting partial artifacts.

**Required scope:**

- Add Candidate Upload creation, bounded byte transfer, validation, strict
  upload expiry, no-clobber promotion, and exact 410 replay behavior.
- Verify object format/OID, base ancestry, repository identity, bundle/snapshot
  binding, size limits, and immutable upload/Attempt association.
- Persist PROMOTED before Candidate/Result consumption and reconcile every
  crash boundary.
- Implement capability-scoped Candidate download and controller-imported
  Candidate admission with durable Controller Operation Fact.
- Deduplicate the same Run/specification/commit without treating repeated
  non-progress as a new Candidate generation.

**Acceptance tests:**

- Partial, corrupt, oversized, expired, wrong-base, and wrong-repository uploads
  never create a Candidate.
- Expiry and finalization are serialized at equality.
- Crash after promotion but before Candidate creation resumes the same upload.
- Imported and worker-produced Candidates satisfy the same integrity boundary.

## Issue V1-14 — Global Result registry and Attempt terminal facts

**Proposed title:** `workflow v1: add idempotent Results and terminal-fact reduction`

**Labels:** `enhancement`, `critical`, `orcest:ready`

**Dependencies:** `Blocked by V1-11`, `Blocked by V1-12`, `Blocked by V1-13`.

**Canonical owners:** `worker-protocol.md`, `domain-model.md`,
`workflow-lifecycle.md`.

**Purpose:** Resolve every worker Result, timeout, loss, cancellation, and stale
request exactly once through one global endpoint identity.

**Required scope:**

- Add the global Result Request registry spanning accepted, expired-current,
  already-terminal, stale-attempt, and accepted semantic-replay outcomes.
- Persist normalized Attempt Result with result digest, closed failure union,
  evidence refs, retry delay, Candidate/receipt output, and exact response.
- Implement strict-before execution deadline acceptance and at/after deadline
  audit/terminal behavior with the bounded expired-token verification carve-out.
- Add source-unique Attempt Terminal Facts for deadline, loss, cancellation,
  supersession, and post-terminal request audit.
- Ensure every terminal input receives the required same-state or lifecycle
  Transition without duplicating terminal authority.

**Acceptance tests:**

- One idempotency key cannot bind different bodies across accepted/late paths.
- Accepted-before-deadline replay remains valid after Attempt terminality.
- Worker loss, timeout, Result, and cancellation races have one deterministic
  winner and retain source-unique audit facts for the others.
- Every closed failure class maps to the specified autonomous recovery input.

## Issue V1-15 — Credential rotation handoff and Secret Version fanout

**Proposed title:** `workflow v1: implement fenced credential rotation handoff`

**Labels:** `enhancement`, `important`, `orcest:ready`

**Dependencies:** `Blocked by V1-05`, `Blocked by V1-12`, `Blocked by V1-14`.

**Canonical owners:** `worker-protocol.md`, `domain-model.md`,
`persistence-and-recovery.md`.

**Purpose:** Rotate provider credentials during execution without exposing
values or losing idempotency across Secret Store and SQLite crashes.

**Required scope:**

- Add Credential Rotation Request/Receipt identities, authority provenance,
  launch-attestation binding, keyed request metadata, and exact replay.
- Implement APPLIED and STALE_PRIOR outcomes with pre-deadline-only mutation.
- Atomically commit Secret Version, receipt, Secret Reference CAS, frozen Run
  membership, and restartable fanout intent after durable Secret Store write.
- Apply member transitions independently and idempotently with a durable cursor.
- Retain byte-bound replay metadata through receipt/operation retention.

**Acceptance tests:**

- Crash at every Secret write/reference/receipt/fanout boundary self-heals.
- Concurrent rotations cannot overwrite a newer version.
- Stale/replayed requests never create an additional version.
- Every affected Run observes the same immutable version event at most once.

## Issue V1-16 — PLAN/BUILD/DIAGNOSE/REPLAN work loop

**Proposed title:** `workflow v1: implement the deterministic pre-PR work loop`

**Labels:** `enhancement`, `critical`, `orcest:ready`

**Dependencies:** `Blocked by V1-10`, `Blocked by V1-14`.

**Canonical owners:** `workflow-lifecycle.md`, `repository-configuration.md`.

**Purpose:** Drive repository work through mandatory planning and bounded
repair loops while treating model output only as typed data.

**Required scope:**

- Implement mandatory PLAN/REPLAN using the pinned implementation profile and
  closed `orcest.plan/1` validation.
- Implement BUILD, DIAGNOSE, and repair/rebase Activity planning with exact
  mode envelope and `orcest.diagnosis/1` validation.
- Enforce requirement/step DAG bounds, candidate-context binding, verification
  mapping, base/snapshot identity, and no lifecycle directives in model prose.
- Detect same-commit repeated non-progress and enter deterministic diagnosis or
  recovery without minting a false Candidate generation.
- Implement policy-only replan with non-gating prior Candidate context.

**Acceptance tests:**

- Every specification generation has an accepted Plan before build/review.
- Invalid/malformed structured output remains correctable or reaches the
  deadline path; it cannot directly mutate lifecycle state.
- Policy-only updates replan and only reuse a Candidate after exact identity
  revalidation.
- Repeat thresholds and diagnosis paths are deterministic and bounded.

## Issue V1-17 — Recovery Evidence, waits, health probes, and Human Boundaries

**Proposed title:** `workflow v1: implement autonomous recovery and exceptional boundaries`

**Labels:** `enhancement`, `critical`, `orcest:ready`

**Dependencies:** `Blocked by V1-09`, `Blocked by V1-14`, `Blocked by V1-16`.

**Canonical owners:** `workflow-lifecycle.md`, `domain-model.md`,
`persistence-and-recovery.md`.

**Purpose:** Ensure every routine failure self-heals or waits on durable
evidence, reserving human decisions for the closed exceptional boundary.

**Required scope:**

- Implement Recovery Evidence with frozen ordered Health Observation
  membership, deterministic tactic selection, retry counters, and resume state.
- Implement the exhaustive Wait reason/wake matrix, panel membership,
  minimum-sequence predicate recheck, timers, dependency waits, and OR wakes.
- Add request-first Health Probe Facts and restartable affected-Run fanout for
  forge, provider, storage, and secret integrity.
- Implement bounded retry/replace/diagnose/replan/remediate ladders and
  deterministic repeated-gate thresholds.
- Implement allowlisted Human Boundary packets and proof-bound Human Resolution
  that resume the same Run; reject generic or convenience escalation.

**Acceptance tests:**

- Every closed failure input reaches exactly one recovery, wait, terminal, or
  exceptional path.
- A wake arriving before Wait insertion cannot be lost.
- Restart during health/timer/fanout processing applies each member once.
- Routine capacity, rate, CI, provider, storage, and evidence failures never
  enter `needs-human` while a self-healing path exists.

## Issue V1-18 — Deterministic verification execution and receipts

**Proposed title:** `workflow v1: add deterministic Candidate verification gates`

**Labels:** `enhancement`, `critical`, `orcest:ready`

**Dependencies:** `Blocked by V1-13`, `Blocked by V1-14`, `Blocked by V1-16`.

**Canonical owners:** `review-and-consensus.md`, `worker-protocol.md`.

**Purpose:** Validate the exact Candidate against ordered repository/server
commands before reviews can count.

**Required scope:**

- Materialize exactly one v1 verification profile from the pinned command set.
- Execute in an isolated exact-Candidate workspace with bounded resources and
  immutable Snapshot/Plan/Candidate bindings.
- Emit and admit closed Verification Receipts for PASS, FAIL, and ERROR with
  normalized command evidence and no lifecycle authority in prose.
- Map PASS/FAIL to successful typed Activity output and ERROR to retryable
  verification failure on the same Activity.
- Invalidate receipts on every relevant identity change.

**Acceptance tests:**

- Command order, environment, time/resource limits, and evidence digest are
  deterministic.
- Missing or malformed receipts are rejected without inventing a Result.
- Receipt admission rejects any Candidate, plan, profile, generation, or
  assignment mismatch.
- Post-PR replacement Candidates pass the same full verification gate.

## Issue V1-19 — Independent review, adjudication, and consensus

**Proposed title:** `workflow v1: implement independent review and consensus panels`

**Labels:** `enhancement`, `critical`, `orcest:ready`

**Dependencies:** `Blocked by V1-12`, `Blocked by V1-17`, `Blocked by V1-18`.

**Canonical owners:** `review-and-consensus.md`, `domain-model.md`.

**Purpose:** Require independently launched adversarial review and deterministic
consensus before publication.

**Required scope:**

- Persist frozen panel round, slot, role, subject membership, context digest,
  provider/model family separation, and trusted Launch Attestation identity.
- Admit Review Receipts and Adjudication Receipts using the closed schemas,
  fills-slot uniqueness, finding identity, and exact assignment provenance.
- Implement all-or-none panel staffing, capacity substitution, abstention, and
  same-Activity adjudication retry semantics.
- Implement the deterministic APPROVED/REMEDIATE/ADJUDICATE reducer with fresh
  full panels after complete overrule and no vote carry-over.
- Invalidate all receipts/decisions on Candidate or policy identity changes.

**Acceptance tests:**

- Reused conversation/workspace, family-policy violations, duplicate slots,
  and stale context never count toward consensus.
- Receipt arrival order does not change the final decision.
- ABSTAIN and INCONCLUSIVE never fill a required approval slot.
- Post-PR replacement Candidates receive a fresh full gate.

## Issue V1-20 — Publication checkpoints, duplicate repair, and PR creation

**Proposed title:** `workflow v1: implement reconciled single-publication creation`

**Labels:** `enhancement`, `critical`, `orcest:ready`

**Dependencies:** `Blocked by V1-09`, `Blocked by V1-18`, `Blocked by V1-19`.

**Canonical owners:** `forge-integration.md`, `workflow-lifecycle.md`,
`domain-model.md`.

**Purpose:** Publish an approved Candidate exactly once after proving forge
ownership, duplicate state, and base identity.

**Required scope:**

- Implement Publication, immutable Publication Effect, ordered checkpoints,
  effect-bound outboxes, forge observations, and Controller Operation Facts.
- Implement the complete marker-search loop: live/terminal member set, ZERO
  create-and-research, ONE selection, MULTIPLE repair, terminal ownership proof,
  merged precedence, and fresh proof before linkage.
- Implement deterministic ref creation/update CAS, pre/post base reads,
  provisional change request, and ACTIVE linkage.
- Add marker repair, redundant-publication cleanup reservations, exact close
  proofs, and unconditional live-Run exclusion from the legacy engine.
- Reconcile response loss and foreign heads without overwriting external work.

**Acceptance tests:**

- Crash at every ref/search/create/link checkpoint resumes one Publication.
- Duplicate, mixed live/terminal, merged, closed, marker mismatch, and ownership
  ambiguity cases reach the specified deterministic outcome.
- Stale CAS never overwrites a foreign commit.
- A PR is not considered ACTIVE until fresh base/head/linkage evidence matches.

## Issue V1-21 — PR monitoring, remediation, cancellation, and terminal closure

**Proposed title:** `workflow v1: implement post-publication monitoring and remediation`

**Labels:** `enhancement`, `critical`, `orcest:ready`

**Dependencies:** `Blocked by V1-17`, `Blocked by V1-20`.

**Canonical owners:** `workflow-lifecycle.md`, `forge-integration.md`.

**Purpose:** Hand the final Candidate to GitHub while continuing deterministic
CI/feedback repair without making the first PR the primary work loop.

**Required scope:**

- Monitor complete typed PR head, CI, review feedback, merge, and close
  observations through durable forge schedules.
- Implement SHA-fenced PR_REMEDIATE/REBASE/CLOSE_PUBLICATION/marker-repair
  Activities and full verification/consensus for every replacement Candidate.
- Enforce current observed-head and base CAS before ref update.
- Implement cancellation precedence, possible-create reconciliation, close
  cleanup, later-command audit, and no orphan PR window.
- Implement merged success, non-merge closure, cancellation, and supersession
  terminal projections; v1 observes rather than initiates merge.

**Acceptance tests:**

- CI/feedback observation storms coalesce without duplicate remediation.
- External head movement always defeats stale Orcest writes.
- Cancellation at every publication checkpoint leaves no unowned open PR.
- Merge/close observations preempt ordinary remediation deterministically.

## Issue V1-22 — Projection and authenticated workflow-management APIs

**Proposed title:** `workflow v1: add rebuildable projections and management commands`

**Labels:** `enhancement`, `important`, `orcest:ready`

**Dependencies:** `Blocked by V1-04`, `Blocked by V1-07`, `Blocked by V1-21`.

**Canonical owners:** `architecture.md`, `workflow-lifecycle.md`,
`forge-integration.md`.

**Purpose:** Expose narrow operator/client controls and reconstructible forge UI
without making labels or API projections workflow authority.

**Required scope:**

- Implement Projection Outbox and idempotent forge label/status/check updates.
- Rebuild projections from SQLite after deletion or forge drift.
- Implement exact authenticated Run command responses for cancel, retry, resume,
  policy resolution, ownership resolution, and exceptional Human Resolution.
- Enforce TLS, RBAC, request digest/idempotency, stored terminal response, audit,
  and closed endpoint surface.
- Keep fleet/Proxmox lifecycle, general principal administration, and arbitrary
  workflow mutation outside v1.

**Acceptance tests:**

- Deleting every forge projection does not lose Run state and reconciliation
  restores the intended view.
- Duplicate commands replay; key/body conflicts fail without a second effect.
- Unauthorized or unknown management operations fail closed and are audited.
- Projection delivery loss never blocks or rewinds the durable reducer.

## Issue V1-23 — Restart, Redis rebuild, restoration, backup, and retention

**Proposed title:** `workflow v1: implement durable restart and storage recovery operations`

**Labels:** `enhancement`, `critical`, `orcest:ready`

**Dependencies:** `Blocked by V1-02`, `Blocked by V1-03`, `Blocked by V1-05`,
`Blocked by V1-09`, `Blocked by V1-11`, `Blocked by V1-15`,
`Blocked by V1-17`, `Blocked by V1-20`.

**Canonical owners:** `persistence-and-recovery.md`,
`operations-and-rollout.md`.

**Purpose:** Prove that every accepted workflow fact survives controller/Redis
loss and every live byte object is restorable without silent weakening.

**Required scope:**

- Implement startup audit, outbox/request reconciliation, deadline scan,
  schedule rebuild, Redis epoch reconstruction, and safe dispatch resumption.
- Add exact-object Storage Restoration Operations/Facts and affected-Run fanout
  for Candidate, Workflow Blob, and Secret Version integrity failures.
- Implement the three-branch zero-CLAIMED backup barrier: in-place MAINTENANCE,
  in-place already-paused mode, or temporary pause/restore.
- Back up SQLite, live byte roots, pending operation staging, replay metadata,
  capability verification keys, and encrypted Secret Store material as one
  manifest-bound unit.
- Implement retention/GC with storage lock, live-root recheck, grace periods,
  terminal-operation metadata retention, and no deletion of audit dependencies.

**Acceptance tests:**

- Redis deletion during active Runs rebuilds offers/projections without a
  synthetic Result.
- Backup/restore drills preserve every accepted idempotency replay.
- Missing/corrupt live objects suspend only affected Runs and auto-resume after
  exact verified restoration.
- GC racing with upload, rotation, restoration, or backup cannot delete a live
  object.

## Issue V1-24 — Observability, compatibility, and failure-injection gate

**Proposed title:** `workflow v1: add release observability and adversarial conformance gates`

**Labels:** `enhancement`, `critical`, `orcest:ready`

**Dependencies:** `Blocked by V1-12`, `Blocked by V1-14`, `Blocked by V1-17`,
`Blocked by V1-19`, `Blocked by V1-21`, `Blocked by V1-22`,
`Blocked by V1-23`.

**Canonical owners:** `operations-and-rollout.md`, all protocol pages.

**Purpose:** Turn the specification's crash, replay, security, and autonomy
requirements into a release-blocking executable gate.

**Required scope:**

- Add metrics, structured events, bounded diagnostics, queue/outbox ages,
  capacity/budget health, wait/human-boundary counts, storage integrity, and
  reducer/adapter failure alerts without secret or model-content leakage.
- Implement the full failure-injection matrix across SQLite commit, outbox,
  Redis loss, worker loss, forge partial failure, Candidate/Secret storage,
  duplicate publication, backup/restore, and rolling protocol compatibility.
- Add pure reducer conformance vectors for every documented lifecycle case.
- Verify legacy/v1 pool, stream, principal, credential, ref, and Publication
  isolation; legacy code cannot read, claim, ACK, delete, or synthesize v1 work.
- Add version skew and rollback tests for controller, worker, runner shim, pool
  manager, forge adapter, and stored protocol objects.

**Acceptance tests:**

- Every normative failure scenario has one automated, named expected outcome.
- No routine injected failure reaches an unauthorized Human Boundary.
- Metrics/events are idempotent under replay and contain no secrets.
- Unsupported version combinations fail before dispatch or mutation.

## Issue V1-25 — Dark launch and staged legacy migration

**Proposed title:** `workflow v1: stage dark launch, pilot publication, and legacy retirement`

**Labels:** `enhancement`, `critical`, `orcest:ready`

**Dependencies:** `Blocked by V1-23`, `Blocked by V1-24`.

**Canonical owners:** `operations-and-rollout.md`, `architecture.md`,
`forge-integration.md`.

**Purpose:** Introduce the durable workflow engine without allowing legacy and
v1 controllers to own the same work, ref, or PR.

**Required scope:**

- Implement Stage 0 schema/store/key/secret/project bootstrap with no ordinary
  forge I/O while initialized maintenance is active.
- Isolate legacy and v1 pools, templates, principals, ACLs, Redis prefixes, and
  reaper authority before synthetic protocol canaries.
- Run one-project pre-publication shadow/pilot, then controlled publication only
  after duplicate/stale-overwrite and rollback gates pass.
- Expand by explicit project allowlist/caps with live-Run stable-ID/ref exclusion
  from the legacy engine.
- Retire legacy behavior only after all active legacy work drains and rollback,
  restore, and evidence requirements pass.

**Acceptance tests:**

- Each rollout stage has a machine-verifiable entry/exit checklist and rollback
  point.
- A project/ref/PR can never be writable by both engines.
- Stage 3 fails on any duplicate publication or stale overwrite.
- Stage 5 demonstrates Redis rebuild, backup restore, protocol rollback, and
  successful representative Runs before legacy removal.

## Filing order

Create issues in topological order so every dependency can use a real issue
number. Recommended batches:

1. **Foundation:** V1-01 through V1-08.
2. **Intake and execution:** V1-09 through V1-15.
3. **Work loop and consensus:** V1-16 through V1-19.
4. **Forge handoff and management:** V1-20 through V1-22.
5. **Production enablement:** V1-23 through V1-25.

After each batch:

- verify title, labels, milestone, body, and dependency rendering;
- verify no issue duplicates or silently broadens an existing open issue;
- link the exact default-branch spec revision and canonical owner sections;
- record newly assigned issue numbers back into this manifest; and
- confirm only dispatchable leaves are visible to the currently deployed
  dependency/label resolver.

## Explicitly deferred from this backlog

Do not add the following to Workflow Control v1 unless new evidence changes the
accepted specification:

- PostgreSQL or active-active/multi-writer controllers;
- a general-purpose internal Git hosting service;
- migration of a live agent process or uncommitted workspace;
- arbitrary repository-defined executable state machines;
- dynamic weakening of verification or consensus for cost/capacity;
- general fleet, Proxmox, principal-lifecycle, or broad administrative RBAC;
- controller-initiated merge; and
- a separate ADR hierarchy without a concrete cross-page decision requiring it.
