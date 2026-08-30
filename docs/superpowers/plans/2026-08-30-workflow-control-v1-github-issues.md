# Workflow Control v1 — GitHub Issue Manifest

**Status:** Filed 2026-08-30. Documentation PR #666, milestone
`Workflow Control v1`, tracking epic #667, and leaves #668 through #698.

**Source specification:** `docs/wiki/` as accepted in
`docs/wiki/spec-writing-plan.md` on 2026-08-27 and hardened by the recorded
2026-08-29 adversarial follow-up.

**Purpose:** Convert the accepted workflow-control specification into
dependency-ordered, independently testable implementation leaves. This plan
does not weaken or reinterpret the normative wiki.

## Spec progress

- [x] Explore repository conventions, labels, skills, and existing issues.
- [x] Write the complete specification.
- [x] Complete independent architecture, protocol, autonomy, and implementation
  reviews.
- [x] Gate 1: the user approved the accepted specification.
- [x] Draft the issue graph, coverage table, and filing dry-run.
- [x] Gate 2: after receiving the manifest, the user explicitly directed Orcest
  to continue and create the GitHub issues.
- [x] File every issue in topological order and verify both dependency encodings.
- [ ] Report the pull request and filed issue URLs, then stop without starting
  implementation.

## Filed issue map

Tracking epic: [#667](https://github.com/ThayneStudio/orcest/issues/667).

| Manifest ID | GitHub issue |
| --- | --- |
| V1-01 | [#668](https://github.com/ThayneStudio/orcest/issues/668) |
| V1-02 | [#669](https://github.com/ThayneStudio/orcest/issues/669) |
| V1-03 | [#670](https://github.com/ThayneStudio/orcest/issues/670) |
| V1-04 | [#671](https://github.com/ThayneStudio/orcest/issues/671) |
| V1-05 | [#672](https://github.com/ThayneStudio/orcest/issues/672) |
| V1-06 | [#673](https://github.com/ThayneStudio/orcest/issues/673) |
| V1-07 | [#674](https://github.com/ThayneStudio/orcest/issues/674) |
| V1-08 | [#675](https://github.com/ThayneStudio/orcest/issues/675) |
| V1-09 | [#676](https://github.com/ThayneStudio/orcest/issues/676) |
| V1-10 | [#677](https://github.com/ThayneStudio/orcest/issues/677) |
| V1-11 | [#678](https://github.com/ThayneStudio/orcest/issues/678) |
| V1-12 | [#679](https://github.com/ThayneStudio/orcest/issues/679) |
| V1-13 | [#680](https://github.com/ThayneStudio/orcest/issues/680) |
| V1-14 | [#681](https://github.com/ThayneStudio/orcest/issues/681) |
| V1-15 | [#682](https://github.com/ThayneStudio/orcest/issues/682) |
| V1-16 | [#683](https://github.com/ThayneStudio/orcest/issues/683) |
| V1-17 | [#684](https://github.com/ThayneStudio/orcest/issues/684) |
| V1-18 | [#685](https://github.com/ThayneStudio/orcest/issues/685) |
| V1-19 | [#686](https://github.com/ThayneStudio/orcest/issues/686) |
| V1-20 | [#687](https://github.com/ThayneStudio/orcest/issues/687) |
| V1-21 | [#688](https://github.com/ThayneStudio/orcest/issues/688) |
| V1-22 | [#689](https://github.com/ThayneStudio/orcest/issues/689) |
| V1-23 | [#690](https://github.com/ThayneStudio/orcest/issues/690) |
| V1-24 | [#691](https://github.com/ThayneStudio/orcest/issues/691) |
| V1-25 | [#692](https://github.com/ThayneStudio/orcest/issues/692) |
| V1-26 | [#693](https://github.com/ThayneStudio/orcest/issues/693) |
| V1-27 | [#694](https://github.com/ThayneStudio/orcest/issues/694) |
| V1-28 | [#695](https://github.com/ThayneStudio/orcest/issues/695) |
| V1-29 | [#696](https://github.com/ThayneStudio/orcest/issues/696) |
| V1-30 | [#697](https://github.com/ThayneStudio/orcest/issues/697) |
| V1-31 | [#698](https://github.com/ThayneStudio/orcest/issues/698) |

## Filing prerequisites

Before creating any issue:

1. Push the accepted `docs/wiki/` migration and open its documentation pull
   request. Each filed issue links the immutable spec commit and pull request;
   after merge, replace that provisional reference with the default-branch
   path. Issue bodies remain self-contained while the pull request is open.
2. Re-check the live open and closed GitHub backlog for duplicates and record
   any superseding issue number in this manifest.
3. Replace every symbolic dependency such as `V1-08` with the real issue
   number and add the native GitHub dependency relationship where available.
4. Confirm that Orcest defers `orcest:ready` issues whose declared blockers are
   open. If that check is unavailable, apply `orcest:ready` only as each leaf
   becomes unblocked.
5. Keep the already-filed related batch separate: #656, #658, and #661 are
   harness/development work; #655, #657, #659, and #660 are legacy issue-task
   delivery work. They inform migration and testing but do not replace a
   normative v1 leaf below.

Create a `Workflow Control v1` milestone and one tracking epic. The epic is
never a blocker and never receives `orcest:ready`; work dependencies exist only
between leaves. Issue bodies must remain self-contained: the wiki is the
canonical contract, but an agent should understand the required change and
acceptance tests from the issue alone.

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
- Create every leaf initially with only its type/priority labels. After all
  body blockers and native blocked-by relationships are attached and verified,
  apply `orcest:ready` to all leaves in one final pass. This prevents a
  create-to-link dispatch race; never apply `orcest:blocked` for work blockers.

## Existing issue relationship map

- #655 (closed), #657, #659, and #660 implement legacy issue-task publication,
  delivery verification, completion gating, and retry context. V1-11, V1-15,
  V1-18, V1-25, and V1-26 replace or integrate those behaviors only as part of
  the new durable Run engine; the legacy issues remain independently valid until
  the Stage-5 retirement in V1-31.
- #649 is related current-head review evidence, not Candidate verification or
  independent consensus; it does not replace V1-23 or V1-24.
- #653 and #664 are legacy Redis/GC follow-ups, not the durable restoration and
  retention contract in V1-28.
- #656, #658, and #661 are the separate harness/development batch used by V1-30
  where applicable; they are not blockers unless an implementation issue later
  names them explicitly.

## Dependency graph

```text
V1-01 contract registry
├─ V1-02 SQLite/run-store substrate
│  ├─ V1-04 controller mode + signing keys
│  │  └─ V1-05 secret provision/adoption
│  └─ V1-08 deterministic reducer
├─ V1-03 durable object stores
└─ V1-06 repository configuration compiler

V1-04 + V1-05 + V1-06 ── V1-07 project onboarding
V1-07 + V1-08 ────────── V1-09 forge observation
V1-06 + V1-08 + V1-09 ── V1-10 snapshot/admission
V1-10 ────────────────── V1-11 activity/attempt
V1-04 + V1-05 + V1-11 ── V1-12 claim/launch
V1-04 + V1-05 + V1-11 ── V1-13 capacity/budget/loss
V1-03 + V1-12 ────────── V1-14 Candidate transfer
V1-11 + V1-12 + V1-13 + V1-14 ── V1-15 Result/terminal
V1-12 + V1-13 + V1-15 ── V1-16 liveness/deadlines
V1-05 + V1-12 + V1-15 ── V1-17 rotation
V1-10 + V1-15 ────────── V1-18 work loop
V1-09 + V1-13 + V1-15 + V1-18 ── V1-19 recovery evidence
V1-19 ────────────────── V1-20 waits/timers/dependencies
V1-09 + V1-19 + V1-20 ── V1-21 health/integrity
V1-19 + V1-20 + V1-21 ── V1-22 human boundaries
V1-14 + V1-15 + V1-18 ── V1-23 verification
V1-12 + V1-13 + V1-20 + V1-23 ── V1-24 consensus
V1-09 + V1-22 + V1-23 + V1-24 ── V1-25 publication
V1-20 + V1-21 + V1-25 ── V1-26 PR lifecycle
V1-04 + V1-07 + V1-22 + V1-26 ── V1-27 API/projection
V1-02 + V1-03 + V1-05 + V1-09 + V1-11 + V1-16 + V1-17 + V1-21 + V1-22 + V1-25 + V1-26 + V1-27 ── V1-28 restart/backup/retention
V1-12 + V1-13 + V1-15 + V1-16 + V1-20 + V1-21 + V1-22 + V1-24 + V1-26 + V1-27 + V1-28 ── V1-29 observability
V1-12 + V1-13 + V1-15 + V1-16 + V1-20 + V1-21 + V1-24 + V1-26 + V1-27 + V1-28 + V1-29 ── V1-30 conformance/failure injection
V1-28 + V1-29 + V1-30 ── V1-31 staged rollout
```

`V1-01` is the first executable leaf. `V1-02`, `V1-03`, and `V1-06` can begin
in parallel as soon as it establishes the canonical identities and
serialization rules.

## Coverage table

Every normative page maps to implementation leaves. The current-state page is
evidence for migration rather than a second normative contract. Explicit
deferrals remain approved non-goals and produce no fleet issue.

| Spec section | Issue(s) | Verb | Phase |
| --- | --- | --- | --- |
| Architecture, authority, topology, trust | V1-03, V1-04, V1-07, V1-12, V1-27, V1-29, V1-31 | include | 1–5 |
| Domain identities, schemas, invariants, retention | V1-01 through V1-30 | include | 1–5 |
| Lifecycle, reducer, recovery, commands, terminality | V1-08, V1-10, V1-13, V1-15, V1-16, V1-18 through V1-27 | include | 1–4 |
| Persistence, outboxes, replay, storage, recovery | V1-02 through V1-05, V1-08 through V1-17, V1-19 through V1-21, V1-28 | include | 1–5 |
| Worker claim, launch, artifact, Result, rotation | V1-01, V1-11 through V1-17, V1-23 | include | 1–3 |
| Verification, review, adjudication, consensus | V1-12, V1-13, V1-20, V1-23, V1-24 | include | 2–3 |
| Forge intake, observations, publication, PR lifecycle | V1-07, V1-09, V1-10, V1-25 through V1-27, V1-31 | include | 1–5 |
| Repository configuration, prompts, onboarding | V1-05 through V1-07, V1-10, V1-18 | include | 1–3 |
| Operations, backup, observability, testing, rollout | V1-04, V1-13, V1-16, V1-28 through V1-31 | include | 1–5 |
| Current orchestrator behavior and migration evidence | V1-28, V1-30, V1-31 | deprioritize | 5 |
| PostgreSQL, multi-writer, internal Git hosting, live workspace migration, executable repo state machines, dynamic gate weakening, general admin plane, controller merge | none | cut | — |

## Filing dry-run

The epic is tracking only. It is never a blocker and receives neither
`orcest:ready` nor `orcest:blocked`. Every work leaf receives `orcest:ready`;
open work dependencies are encoded both natively and in the issue body.

| Title | Repo | Type | Labels | Blocked by | Phase | Milestone |
| --- | --- | --- | --- | --- | --- | --- |
| Epic: Workflow Control v1 | ThayneStudio/orcest | epic | enhancement | — | — | Workflow Control v1 |
| V1-01 contract and digest registry | ThayneStudio/orcest | leaf | enhancement, critical, orcest:ready | — | 1 | Workflow Control v1 |
| V1-02 SQLite run-store substrate | ThayneStudio/orcest | leaf | enhancement, critical, orcest:ready | V1-01 | 1 | Workflow Control v1 |
| V1-03 durable object stores | ThayneStudio/orcest | leaf | enhancement, critical, orcest:ready | V1-01 | 1 | Workflow Control v1 |
| V1-04 controller modes and signing keys | ThayneStudio/orcest | leaf | enhancement, critical, orcest:ready | V1-01, V1-02, V1-03 | 1 | Workflow Control v1 |
| V1-05 secret provision and adoption | ThayneStudio/orcest | leaf | enhancement, critical, orcest:ready | V1-02, V1-03, V1-04 | 1 | Workflow Control v1 |
| V1-06 repository configuration compiler | ThayneStudio/orcest | leaf | enhancement, critical, orcest:ready | V1-01 | 1 | Workflow Control v1 |
| V1-07 project onboarding | ThayneStudio/orcest | leaf | enhancement, critical, orcest:ready | V1-04, V1-05, V1-06 | 1 | Workflow Control v1 |
| V1-08 reducer and Transition engine | ThayneStudio/orcest | leaf | enhancement, critical, orcest:ready | V1-01, V1-02 | 1 | Workflow Control v1 |
| V1-09 forge observation scheduler | ThayneStudio/orcest | leaf | enhancement, critical, orcest:ready | V1-02, V1-04, V1-07, V1-08 | 2 | Workflow Control v1 |
| V1-10 snapshots and admission | ThayneStudio/orcest | leaf | enhancement, critical, orcest:ready | V1-06, V1-08, V1-09 | 2 | Workflow Control v1 |
| V1-11 Activity/Attempt and Redis projection | ThayneStudio/orcest | leaf | enhancement, critical, orcest:ready | V1-08, V1-10 | 2 | Workflow Control v1 |
| V1-12 worker claim and launch | ThayneStudio/orcest | leaf | enhancement, critical, orcest:ready | V1-04, V1-05, V1-11 | 2 | Workflow Control v1 |
| V1-13 capacity, budget, and loss | ThayneStudio/orcest | leaf | enhancement, critical, orcest:ready | V1-04, V1-05, V1-11 | 2 | Workflow Control v1 |
| V1-14 Candidate transfer | ThayneStudio/orcest | leaf | enhancement, critical, orcest:ready | V1-03, V1-12 | 2 | Workflow Control v1 |
| V1-15 Results and terminal facts | ThayneStudio/orcest | leaf | enhancement, critical, orcest:ready | V1-11, V1-12, V1-13, V1-14 | 2 | Workflow Control v1 |
| V1-16 liveness and deadlines | ThayneStudio/orcest | leaf | enhancement, critical, orcest:ready | V1-12, V1-13, V1-15 | 2 | Workflow Control v1 |
| V1-17 credential rotation | ThayneStudio/orcest | leaf | enhancement, important, orcest:ready | V1-05, V1-12, V1-15 | 2 | Workflow Control v1 |
| V1-18 pre-PR work loop | ThayneStudio/orcest | leaf | enhancement, critical, orcest:ready | V1-10, V1-15 | 3 | Workflow Control v1 |
| V1-19 Recovery Evidence and tactics | ThayneStudio/orcest | leaf | enhancement, critical, orcest:ready | V1-09, V1-13, V1-15, V1-18 | 3 | Workflow Control v1 |
| V1-20 waits, timers, dependencies | ThayneStudio/orcest | leaf | enhancement, critical, orcest:ready | V1-19 | 3 | Workflow Control v1 |
| V1-21 health and integrity probes | ThayneStudio/orcest | leaf | enhancement, critical, orcest:ready | V1-09, V1-19, V1-20 | 3 | Workflow Control v1 |
| V1-22 Human Boundaries | ThayneStudio/orcest | leaf | enhancement, important, orcest:ready | V1-19, V1-20, V1-21 | 3 | Workflow Control v1 |
| V1-23 verification gates | ThayneStudio/orcest | leaf | enhancement, critical, orcest:ready | V1-14, V1-15, V1-18 | 3 | Workflow Control v1 |
| V1-24 review and consensus | ThayneStudio/orcest | leaf | enhancement, critical, orcest:ready | V1-12, V1-13, V1-20, V1-23 | 3 | Workflow Control v1 |
| V1-25 publication creation | ThayneStudio/orcest | leaf | enhancement, critical, orcest:ready | V1-09, V1-22, V1-23, V1-24 | 4 | Workflow Control v1 |
| V1-26 post-publication lifecycle | ThayneStudio/orcest | leaf | enhancement, critical, orcest:ready | V1-20, V1-21, V1-25 | 4 | Workflow Control v1 |
| V1-27 projection and management API | ThayneStudio/orcest | leaf | enhancement, important, orcest:ready | V1-04, V1-07, V1-22, V1-26 | 4 | Workflow Control v1 |
| V1-28 restart, backup, and retention | ThayneStudio/orcest | leaf | enhancement, critical, orcest:ready | V1-02, V1-03, V1-05, V1-09, V1-11, V1-16, V1-17, V1-21, V1-22, V1-25, V1-26, V1-27 | 5 | Workflow Control v1 |
| V1-29 observability | ThayneStudio/orcest | leaf | enhancement, important, orcest:ready | V1-12, V1-13, V1-15, V1-16, V1-20, V1-21, V1-22, V1-24, V1-26, V1-27, V1-28 | 5 | Workflow Control v1 |
| V1-30 conformance and fault injection | ThayneStudio/orcest | leaf | enhancement, critical, orcest:ready | V1-12, V1-13, V1-15, V1-16, V1-20, V1-21, V1-24, V1-26, V1-27, V1-28, V1-29 | 5 | Workflow Control v1 |
| V1-31 staged migration | ThayneStudio/orcest | leaf | enhancement, critical, orcest:ready | V1-28, V1-29, V1-30 | 5 | Workflow Control v1 |

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
  exceptions, and redacted or unauthorized API fixtures; authorized scoped
  claim/launch fixtures are handled by their dedicated protocol tests.
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
- Provide the canonical local `orcest project init`, `lint`, `explain`, and
  `simulate` commands; `lint` and `explain` perform compilation checks without
  mutating a server.

**Acceptance tests:**

- Equivalent input produces the same normalized bundle across machines.
- Any referenced-byte or server-policy change changes the appropriate digest.
- PR-head configuration cannot control execution.
- Invalid references and all closed-schema violations produce actionable,
  secret-free diagnostics.
- `init`, `lint`, `explain`, and `simulate` exist with the canonical exit/output
  contract; `lint` and `explain` perform no local or server mutation.

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

- Success produces one observation and a delivered Outbox; transient failure
  produces one Forge Request Failure Fact while Request and Outbox remain
  pending; a stale post-I/O response supersedes the Request and delivers the
  reciprocal Outbox.
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

## Issue V1-12 — Worker claim and launch isolation

**Proposed title:** `workflow v1: implement fenced worker claim and launch isolation`

**Labels:** `enhancement`, `critical`, `orcest:ready`

**Dependencies:** `Blocked by V1-04`, `Blocked by V1-05`, `Blocked by V1-11`.

**Canonical owners:** `worker-protocol.md`, `architecture.md`,
`domain-model.md`.

**Purpose:** Admit an untrusted worker to one immutable Attempt using
short-lived, exact-bound capabilities and independently attested isolation.

**Required scope:**

- Implement durable claim replay, session fencing, signer-bound Attempt
  capability issuance, and bounded equivalent-bearer rematerialization.
- Implement one-shot Launch Attestation through the trusted runner shim with
  invocation/context/workspace isolation and exact provider-secret binding.
- Freeze provider/account/model/family/classification and execution profile in
  the claim/capability/attestation digest.
- Return only the scoped material authorized for that Attempt and enforce the
  cryptographic-expiry lookup rules for accepted replay.

**Acceptance tests:**

- Duplicate or ambiguous claim/launch calls replay without a second session or
  authority grant.
- Stale-generation, wrong-worker, wrong-session, expired, or revoked-key claims
  cannot mutate an Attempt.
- A reused invocation/context/workspace cannot count as an independent launch.
- Capability and attestation replays retain the exact immutable claim set.

## Issue V1-13 — Capacity, budget, and worker-loss evidence

**Proposed title:** `workflow v1: persist capacity, budget, and worker-loss reports`

**Labels:** `enhancement`, `critical`, `orcest:ready`

**Dependencies:** `Blocked by V1-04`, `Blocked by V1-05`, `Blocked by V1-11`.

**Canonical owners:** `worker-protocol.md`, `domain-model.md`,
`workflow-lifecycle.md`.

**Purpose:** Give dispatch and recovery one authenticated, restart-safe source
for worker availability, cumulative budget, and worker-loss decisions.

**Required scope:**

- Implement Capacity Report, cumulative Budget Report, and Worker Loss Report
  ledgers with principal/pool/session binding, monotonic sequence, TTL, request
  digest, stored response, and ACCEPTED/STALE behavior.
- Derive source-unique Health Observations and frozen affected-Run membership
  from accepted reports; never synthesize reports from Redis state.
- Implement deterministic expiry, budget-window reset, policy-expansion wake,
  and highest-applicable-observation selection.
- Gate new offers on current capacity, budget, provider/account classification,
  controller mode, and issuance-key availability.

**Acceptance tests:**

- Duplicate, stale, out-of-order, over-TTL, and unauthorized reports cannot
  change dispatch or recovery.
- Redis loss reconstructs report-derived projections without inventing usage.
- Budget consumption/reset and report expiry fan out once per affected Run.
- A loss report can terminalize only the exact currently bound Attempt.

## Issue V1-14 — Candidate upload, download, promotion, and import

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
- Persist PROMOTED before later Result consumption and reconcile every artifact
  write/validation/promotion crash boundary.
- Implement capability-scoped Candidate download and controller-imported
  Candidate admission with its durable Controller Operation Fact.
- Leave worker-produced Candidate row creation and same-commit deduplication to
  the V1-15 Attempt Result transaction.

**Acceptance tests:**

- Partial, corrupt, oversized, expired, wrong-base, and wrong-repository uploads
  never become consumable artifacts.
- Expiry and promotion are serialized at equality.
- Crash after promotion resumes the same immutable upload.
- Controller imports satisfy the same object integrity boundary.

## Issue V1-15 — Global Result registry and Attempt terminal facts

**Proposed title:** `workflow v1: add idempotent Results and terminal-fact reduction`

**Labels:** `enhancement`, `critical`, `orcest:ready`

**Dependencies:** `Blocked by V1-11`, `Blocked by V1-12`, `Blocked by V1-13`,
`Blocked by V1-14`.

**Canonical owners:** `worker-protocol.md`, `domain-model.md`,
`workflow-lifecycle.md`.

**Purpose:** Resolve every worker Result, deadline, worker loss, and stale Result
request exactly once through one global endpoint identity.

**Required scope:**

- Add the global Result Request registry spanning accepted, expired-current,
  already-terminal, stale-attempt, and identical semantic-replay outcomes.
- Persist normalized Attempt Result with result digest, closed failure union,
  evidence refs, retry delay, receipt output, and exact response.
- Atomically consume a PROMOTED upload, create/deduplicate the worker-produced
  Candidate, accept the Result, and preserve same-commit non-progress semantics.
- Implement strict-before execution-deadline acceptance and the bounded expired
  token carve-out for late rejection/audit.
- Implement only the closed Attempt Terminal Fact kinds: `CLAIM_DEADLINE`,
  `EXECUTION_DEADLINE`, `WORKER_LOST`, and `RESULT_AFTER_TERMINAL`.
- Route cancellation and supersession through their canonical Management/Forge
  source records and audit Transitions, never synthetic Terminal Facts.

**Acceptance tests:**

- One request key cannot bind different bodies across accepted/late paths;
  identical result digest may use the specified semantic replay path.
- Candidate finalization and Result acceptance either both commit or neither.
- Deadline, worker-loss, and Result races have one deterministic winner and
  source-unique audit for later inputs.
- Every closed failure class maps to the specified autonomous recovery input.

## Issue V1-16 — Liveness, control, and persistent deadlines

**Proposed title:** `workflow v1: implement liveness control and deadline processing`

**Labels:** `enhancement`, `critical`, `orcest:ready`

**Dependencies:** `Blocked by V1-12`, `Blocked by V1-13`, `Blocked by V1-15`.

**Canonical owners:** `worker-protocol.md`, `workflow-lifecycle.md`,
`persistence-and-recovery.md`.

**Purpose:** Detect lost or expired execution and communicate bounded control
without relying on Redis leases for lifecycle authority.

**Required scope:**

- Implement liveness sequence validation, current controller command delivery,
  acknowledgement, and endpoint-specific retry identities.
- Persist fixed claim/execution deadlines and scope/deadline-unique Timer Facts.
- Apply the complete claim-deadline capacity classifier using frozen report
  evidence, mode, and issuance-key gates.
- Coalesce panel staffing rechecks and create replacement Attempts/outboxes only
  when the complete panel can be staffed.
- Use V1-15 Terminal Facts for deadline/loss outcomes; Timer Facts are evidence,
  not a second terminal trigger.

**Acceptance tests:**

- Liveness replay, skipped sequence, ambiguity, and controller restart preserve
  one ordered conversation.
- Equality belongs to deadline expiry, never first Result acceptance.
- Claim deadline never creates an offer when mode/key/capacity gates fail.
- Peer panel expiry cannot create partial staffing or duplicate continuations.

## Issue V1-17 — Credential rotation handoff and Secret Version fanout

**Proposed title:** `workflow v1: implement fenced credential rotation handoff`

**Labels:** `enhancement`, `important`, `orcest:ready`

**Dependencies:** `Blocked by V1-05`, `Blocked by V1-12`, `Blocked by V1-15`.

**Canonical owners:** `worker-protocol.md`, `domain-model.md`,
`persistence-and-recovery.md`.

**Purpose:** Rotate provider credentials during execution without exposing
values or losing idempotency across Secret Store and SQLite crashes.

**Required scope:**

- Add Credential Rotation Request/Receipt identities, authority provenance,
  launch-attestation binding, keyed request metadata, and exact replay.
- Implement only the closed `APPLIED` and `CAS_LOST` outcomes with
  pre-deadline-only mutation.
- Atomically commit Secret Version, receipt, Secret Reference CAS, frozen Run
  membership, and restartable fanout intent after durable Secret Store write.
- Apply member transitions independently and idempotently with a durable cursor.
- Retain byte-bound replay metadata through receipt/operation retention.

**Acceptance tests:**

- Crash at every Secret write/reference/receipt/fanout boundary self-heals.
- Concurrent rotation yields exactly one `APPLIED` and deterministic `CAS_LOST`
  for a stale prior version.
- Replay never creates an additional version.
- Every affected Run observes the immutable version event at most once.

## Issue V1-18 — PLAN/BUILD/DIAGNOSE/REPLAN work loop

**Proposed title:** `workflow v1: implement the deterministic pre-PR work loop`

**Labels:** `enhancement`, `critical`, `orcest:ready`

**Dependencies:** `Blocked by V1-10`, `Blocked by V1-15`.

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
- Invalid structured output remains correctable or reaches the deadline path;
  it cannot directly mutate lifecycle state.
- Policy-only updates replan and only reuse a Candidate after exact identity
  revalidation.
- Repeat thresholds and diagnosis paths are deterministic and bounded.

## Issue V1-19 — Recovery Evidence and deterministic recovery tactics

**Proposed title:** `workflow v1: implement evidence-bound autonomous recovery tactics`

**Labels:** `enhancement`, `critical`, `orcest:ready`

**Dependencies:** `Blocked by V1-09`, `Blocked by V1-13`, `Blocked by V1-15`,
`Blocked by V1-18`.

**Canonical owners:** `workflow-lifecycle.md`, `domain-model.md`.

**Purpose:** Turn each accepted failure into one deterministic retry, replace,
diagnose, replan, remediate, wait, terminal, or exceptional decision.

**Required scope:**

- Persist Recovery Evidence with causal source, frozen ordered Health
  Observation membership, digest, failure fingerprint, counters, and resume state.
- Implement the closed failure-source/category mapping and deterministic tactic
  selector; models cannot choose lifecycle actions.
- Implement bounded retry/replace/diagnose/replan/remediate ladders and
  repeated-gate thresholds.
- Preserve fallback and provider/account decisions against mutable registry
  changes by binding exact evidence revisions.

**Acceptance tests:**

- Every closed failure input maps to one allowed tactic or fails closed.
- Replay and input reordering preserve the chosen tactic and counters.
- Recovery cannot weaken verification, consensus, security, or budget policy.
- Repeated non-progress reaches the configured diagnosis/remediation boundary.

## Issue V1-20 — Durable waits, timers, and dependency wakes

**Proposed title:** `workflow v1: implement durable wait and wake processing`

**Labels:** `enhancement`, `critical`, `orcest:ready`

**Dependencies:** `Blocked by V1-19`.

**Canonical owners:** `workflow-lifecycle.md`, `domain-model.md`,
`persistence-and-recovery.md`.

**Purpose:** Persist every temporary pause and resume it only from exact newer
evidence without losing a wake during insertion or restart.

**Required scope:**

- Implement the exhaustive Wait reason/wake compatibility matrix, resume state,
  minimum sequence/revision, panel membership, and causal evidence binding.
- Under the writer lock, recheck each wake predicate before inserting a Wait;
  use successor Evidence when the predicate is already satisfied.
- Implement Wait Timer Facts, health-expiry membership, external dependency
  observations, rate-limit wake clamping, and OR-wake semantics.
- Coalesce review-panel staffing waits and preserve all-or-none assignment.

**Acceptance tests:**

- A wake arriving before Wait insertion cannot be lost.
- Wrong reason/source/revision cannot close a Wait.
- Timer replay across generation change applies at most once per Run.
- Panel capacity cannot wake or staff only a subset of required slots.

## Issue V1-21 — Health probes and exact-object integrity recovery

**Proposed title:** `workflow v1: implement request-first health and integrity probes`

**Labels:** `enhancement`, `critical`, `orcest:ready`

**Dependencies:** `Blocked by V1-09`, `Blocked by V1-19`, `Blocked by V1-20`.

**Canonical owners:** `domain-model.md`, `persistence-and-recovery.md`,
`workflow-lifecycle.md`.

**Purpose:** Convert forge/provider/storage/secret health checks into durable,
replayable evidence and suspend only Runs that reference an unavailable object.

**Required scope:**

- Implement Health Probe Request/Fact, closed probe-kind/outcome matrix,
  implementation/input/evidence digests, and reciprocal Health Observation.
- Write request/outbox before probe I/O and reconcile response loss by the same
  deterministic request identity.
- Freeze affected Run membership and apply restartable per-Run fanout for
  Candidate, Workflow Blob, Secret Version, provider, and forge health.
- Use the canonical two-transition integrity path: Health Observation, then
  Recovery Evidence; restoration success resumes from exact verified evidence.

**Acceptance tests:**

- Probe crash/replay creates one Fact/Observation and one reduction per member.
- Missing/corrupt objects suspend only exact referencing Runs.
- Stale or unrelated health evidence cannot wake a Run.
- Successful recheck/restoration automatically resumes without human action.

## Issue V1-22 — Exceptional Human Boundaries and proof-bound resolution

**Proposed title:** `workflow v1: implement exceptional human boundary resolution`

**Labels:** `enhancement`, `important`, `orcest:ready`

**Dependencies:** `Blocked by V1-19`, `Blocked by V1-20`, `Blocked by V1-21`.

**Canonical owners:** `workflow-lifecycle.md`, `domain-model.md`.

**Purpose:** Escalate only closed, exceptional authority/integrity decisions and
resume the same Run from a minimal auditable decision packet.

**Required scope:**

- Implement the allowlisted Human Boundary reasons, unique active boundary,
  decision packet, resume state, and required proof/evidence bindings.
- Implement source-derived Human Resolution identity, authenticated principal,
  reason-specific payload union, stored response, and boundary closure.
- Support canonical policy/ownership/specification/storage/secret resolution
  effects only through their reason-bound schemas.
- Reject generic retry, arbitrary lifecycle directives, missing proof, stale
  resolution, and convenience escalation.

**Acceptance tests:**

- Every non-allowlisted escalation attempt fails closed.
- Duplicate resolution replays and conflicting payloads cannot produce a second
  transition.
- Routine failures remain in autonomous recovery/wait paths.
- Resolution resumes the exact suspended Run and cannot broaden authority.

## Issue V1-23 — Deterministic verification execution and receipts

**Proposed title:** `workflow v1: add deterministic Candidate verification gates`

**Labels:** `enhancement`, `critical`, `orcest:ready`

**Dependencies:** `Blocked by V1-14`, `Blocked by V1-15`, `Blocked by V1-18`.

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

## Issue V1-24 — Independent review, adjudication, and consensus

**Proposed title:** `workflow v1: implement independent review and consensus panels`

**Labels:** `enhancement`, `critical`, `orcest:ready`

**Dependencies:** `Blocked by V1-12`, `Blocked by V1-13`, `Blocked by V1-20`,
`Blocked by V1-23`.

**Canonical owners:** `review-and-consensus.md`, `domain-model.md`.

**Purpose:** Require independently launched adversarial review and deterministic
consensus before publication.

**Required scope:**

- Persist frozen panel round, slot, role, subject membership, context digest,
  provider/model family separation, and trusted Launch Attestation identity.
- Admit Review and Adjudication Receipts using the closed schemas, fills-slot
  uniqueness, finding identity, and exact assignment provenance.
- Implement all-or-none panel staffing, capacity substitution, abstention, and
  same-Activity adjudication retry semantics.
- Implement APPROVED/REMEDIATE/ADJUDICATE with fresh full panels after complete
  overrule and no vote carry-over.
- Invalidate receipts/decisions on Candidate or policy identity changes.

**Acceptance tests:**

- Reused context/workspace, family-policy violations, duplicate slots, and stale
  context never count.
- Receipt arrival order does not change the decision.
- ABSTAIN and INCONCLUSIVE never fill a required approval slot.
- Post-PR replacement Candidates receive a fresh full gate.

## Issue V1-25 — Publication checkpoints, duplicate proof, and PR creation

**Proposed title:** `workflow v1: implement reconciled single-publication creation`

**Labels:** `enhancement`, `critical`, `orcest:ready`

**Dependencies:** `Blocked by V1-09`, `Blocked by V1-22`, `Blocked by V1-23`,
`Blocked by V1-24`.

**Canonical owners:** `forge-integration.md`, `workflow-lifecycle.md`,
`domain-model.md`.

**Purpose:** Publish an approved Candidate exactly once after proving forge
ownership, duplicate state, and base identity.

**Required scope:**

- Implement Publication, immutable Effect, ordered checkpoints, effect-bound
  outboxes, observations, and Controller Operation Facts.
- Implement complete marker search: live/terminal set, ZERO create/research, ONE
  selection, MULTIPLE pre-link duplicate cleanup, terminal ownership proof,
  merged precedence, and fresh proof before linkage.
- Implement deterministic ref CAS, pre/post base reads, provisional change
  request, and ACTIVE linkage.
- Persist redundant-publication cleanup reservations and exact close proofs;
  leave linked-PR marker repair to V1-26.
- Enforce unconditional live-Run exclusion from the legacy engine.

**Acceptance tests:**

- Crash at every ref/search/create/link checkpoint resumes one Publication.
- Duplicate, mixed live/terminal, merged, closed, and ownership cases reach the
  specified deterministic outcome.
- Positive incompatible ownership creates exactly the
  `PUBLICATION_OWNERSHIP_CONFLICT` Human Boundary with no ref/link/create
  mutation; incomplete or uncertain ownership proof remains autonomous
  retry/wait and cannot escalate early.
- Stale CAS never overwrites a foreign commit.
- A PR is not ACTIVE until fresh base/head/linkage evidence matches.

## Issue V1-26 — PR monitoring, remediation, cancellation, and terminal closure

**Proposed title:** `workflow v1: implement post-publication monitoring and remediation`

**Labels:** `enhancement`, `critical`, `orcest:ready`

**Dependencies:** `Blocked by V1-20`, `Blocked by V1-21`, `Blocked by V1-25`.

**Canonical owners:** `workflow-lifecycle.md`, `forge-integration.md`.

**Purpose:** Continue deterministic CI/feedback repair after publication without
making the first PR the primary work loop.

**Required scope:**

- Monitor complete typed PR head, CI, review feedback, merge, and close
  observations through durable forge schedules.
- Implement SHA-fenced PR_REMEDIATE, REBASE, CLOSE_PUBLICATION, and linked
  REPAIR_RUN_MARKER Activities plus the full replacement-Candidate gate.
- Enforce observed-head and base CAS before ref update.
- Implement cancellation precedence, possible-create reconciliation, close
  cleanup, later-command audit, and no orphan PR window.
- Implement merged success, non-merge closure, cancellation, and supersession;
  v1 observes rather than initiates merge.

**Acceptance tests:**

- Observation storms coalesce without duplicate remediation.
- External head movement defeats stale Orcest writes.
- Cancellation at every checkpoint leaves no unowned open PR.
- Positively owned merged evidence wins over pending cancellation; the first
  cancellation source stays immutable, and stale close/head evidence cannot
  terminalize or mutate the Run.

## Issue V1-27 — Projection and authenticated workflow-management APIs

**Proposed title:** `workflow v1: add rebuildable projections and management commands`

**Labels:** `enhancement`, `important`, `orcest:ready`

**Dependencies:** `Blocked by V1-04`, `Blocked by V1-07`, `Blocked by V1-22`,
`Blocked by V1-26`.

**Canonical owners:** `architecture.md`, `workflow-lifecycle.md`,
`forge-integration.md`.

**Purpose:** Expose narrow operator/client controls and reconstructible forge UI
without making labels or API projections workflow authority.

**Required scope:**

- Implement Projection Outbox and idempotent forge label/status/check updates.
- Rebuild projections from SQLite after deletion or forge drift.
- Implement only the closed Run command kinds `CANCEL` and
  `RESOLVE_HUMAN_BOUNDARY` with their exact authenticated responses.
- Treat retry/resume as reducer outcomes, policy amendment as the canonical
  Forge Observation path, and ownership/policy decisions as reason-bound Human
  Resolution effects rather than generic commands.
- Enforce TLS, RBAC, request idempotency, stored response, audit, and the closed
  endpoint surface.

**Acceptance tests:**

- Deleting every forge projection does not lose Run state and reconciliation
  restores the intended view.
- Duplicate commands replay; key/body conflicts fail without a second effect.
- Unknown command kinds and unauthorized operations fail closed and are audited.
- Projection delivery loss never blocks or rewinds the reducer.

## Issue V1-28 — Restart, Redis rebuild, restoration, backup, and retention

**Proposed title:** `workflow v1: implement durable restart and storage recovery operations`

**Labels:** `enhancement`, `critical`, `orcest:ready`

**Dependencies:** `Blocked by V1-02`, `Blocked by V1-03`, `Blocked by V1-05`,
`Blocked by V1-09`, `Blocked by V1-11`, `Blocked by V1-16`,
`Blocked by V1-17`, `Blocked by V1-21`, `Blocked by V1-22`,
`Blocked by V1-25`, `Blocked by V1-26`, `Blocked by V1-27`.

**Canonical owners:** `persistence-and-recovery.md`,
`operations-and-rollout.md`.

**Purpose:** Prove that every accepted workflow fact survives controller/Redis
loss and every live byte object is restorable without silent weakening.

**Required scope:**

- Implement startup audit, outbox/request reconciliation, deadline scan,
  schedule rebuild, Redis epoch reconstruction, and safe dispatch resumption.
- Add exact-object Storage Restoration Operations/Facts and affected-Run fanout
  for Candidate, Workflow Blob, and Secret Version failures.
- Implement the three-branch zero-CLAIMED backup barrier: in-place MAINTENANCE,
  in-place already-paused mode, or temporary pause/restore.
- Back up SQLite, live byte roots, pending staging, replay metadata, capability
  verification keys, and encrypted Secret Store material as one bound unit.
- Implement retention/GC with storage lock, live-root recheck, grace, terminal
  metadata retention, and no deletion of audit dependencies.

**Acceptance tests:**

- Redis deletion during active Runs rebuilds offers/projections without a
  synthetic Result.
- Backup/restore preserves every accepted idempotency replay.
- Maintenance-in-place, already-paused, and temporary-pause backup branches all
  require zero CLAIMED Attempts; timeout aborts safely and temporary mode
  restoration is revision-CAS fenced.
- Restored Management Commands, Human Boundaries, and Human Resolutions retain
  their exact request/replay identity and cannot repeat an effect.
- Missing/corrupt live objects suspend only affected Runs and auto-resume after
  exact verified restoration.
- GC races cannot delete a live or pending operation object.

## Issue V1-29 — Release observability and operational alerts

**Proposed title:** `workflow v1: add release observability and bounded diagnostics`

**Labels:** `enhancement`, `important`, `orcest:ready`

**Dependencies:** `Blocked by V1-12`, `Blocked by V1-13`, `Blocked by V1-15`,
`Blocked by V1-16`, `Blocked by V1-20`, `Blocked by V1-21`,
`Blocked by V1-22`, `Blocked by V1-24`, `Blocked by V1-26`,
`Blocked by V1-27`, `Blocked by V1-28`.

**Canonical owners:** `operations-and-rollout.md`, `architecture.md`.

**Purpose:** Make durable workflow health, age, recovery, and authority failures
visible without exposing secrets or granting telemetry lifecycle authority.

**Required scope:**

- Add metrics and structured events for queue/outbox age, capacity/budget,
  waits/boundaries, storage integrity, reducer errors, forge failures, receipt
  gates, duplicate repair, and publication latency.
- Add bounded secret-free diagnostic packets and alerts for every release
  checklist threshold.
- Rebuild gauges from durable state after restart and make event emission
  idempotent under replay.
- Document alert ownership, severity, retention, and rollout-stage thresholds.

**Acceptance tests:**

- Metrics match a direct durable-state reconciliation after restart.
- Replay does not double-count lifecycle events.
- Secret/model content never enters labels, logs, metrics, or alert payloads.
- Each rollout gate has an observable pass/fail signal.

## Issue V1-30 — Adversarial conformance and failure-injection gate

**Proposed title:** `workflow v1: add adversarial conformance and compatibility gates`

**Labels:** `enhancement`, `critical`, `orcest:ready`

**Dependencies:** `Blocked by V1-12`, `Blocked by V1-13`, `Blocked by V1-15`,
`Blocked by V1-16`, `Blocked by V1-20`, `Blocked by V1-21`, `Blocked by V1-24`,
`Blocked by V1-26`, `Blocked by V1-27`, `Blocked by V1-28`,
`Blocked by V1-29`.

**Canonical owners:** `operations-and-rollout.md`, all protocol pages.

**Purpose:** Turn the crash, replay, security, autonomy, and version-skew
contracts into a release-blocking executable suite.

**Required scope:**

- Implement pure reducer vectors for every documented conformance case.
- Implement the full fault matrix across SQLite, outbox, Redis, worker/forge,
  Candidate/Secret storage, duplicate publication, and backup/restore.
- Verify legacy/v1 pool, stream, principal, credential, ref, Publication, and
  reaper isolation; legacy cannot claim, ACK, delete, or synthesize v1 work.
- Test version skew and rollback for controller, worker, runner shim, pool
  manager, forge adapter, and stored protocol objects.

**Acceptance tests:**

- Every normative failure scenario has one named automated expected outcome.
- No routine injected failure reaches an unauthorized Human Boundary.
- Unsupported version combinations fail before dispatch or mutation.
- Duplicate publication and stale overwrite fail the release gate.

## Issue V1-31 — Dark launch and staged legacy migration

**Proposed title:** `workflow v1: stage dark launch, pilot publication, and legacy retirement`

**Labels:** `enhancement`, `critical`, `orcest:ready`

**Dependencies:** `Blocked by V1-28`, `Blocked by V1-29`, `Blocked by V1-30`.

**Canonical owners:** `operations-and-rollout.md`, `architecture.md`,
`forge-integration.md`.

**Purpose:** Introduce the durable workflow engine without allowing legacy and
v1 controllers to own the same work, ref, or PR.

**Required scope:**

- Implement Stage 0 schema/store/key/secret/project bootstrap with no ordinary
  forge I/O while initialized maintenance is active.
- Isolate legacy and v1 pools, templates, principals, ACLs, Redis prefixes, and
  reaper authority before synthetic canaries.
- Run one-project pre-publication pilot, then controlled publication only after
  duplicate/stale-overwrite and rollback gates pass.
- Expand by explicit project allowlist/caps with live-Run ID/ref exclusion from
  the legacy engine.
- Retire legacy behavior only after active legacy work drains and the required
  evidence/observation period passes.

**Acceptance tests:**

- Each rollout stage has a machine-verifiable entry/exit checklist and rollback.
- A project/ref/PR can never be writable by both engines.
- Stage 3 fails on any duplicate publication or stale overwrite.
- Stage 5 drains and archives all legacy work, retains historical read-only
  tooling, removes raw task credentials, completes the central-controller
  backup/restore observation period, and demonstrates representative Runs
  before legacy removal.

## Filing order

Create issues in topological order so every dependency can use a real issue
number. Recommended batches:

1. **Foundation:** V1-01 through V1-08.
2. **Intake and execution:** V1-09 through V1-17.
3. **Work loop and consensus:** V1-18 through V1-24.
4. **Forge handoff and management:** V1-25 through V1-27.
5. **Production enablement:** V1-28 through V1-31.

After each batch:

- verify title, labels, milestone, body, and dependency rendering;
- verify no issue duplicates or silently broadens an existing open issue;
- link the exact default-branch spec revision and canonical owner sections;
- record newly assigned issue numbers back into this manifest; and
- confirm only dispatchable leaves are visible to the currently deployed
  dependency/label resolver.

Create all leaves without `orcest:ready`, attach body and native dependencies,
attach the tracking parent, and verify the complete graph. Only then apply
`orcest:ready` to all 31 leaves in one final pass. An open blocker will defer a
ready leaf; do not use the terminal `orcest:blocked` label for these waits.

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
