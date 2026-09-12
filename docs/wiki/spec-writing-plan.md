# Orcest Workflow-Control Specification Writing Plan

Status: Completed 2026-08-27

## Purpose

This plan records how the accepted v1 specification was produced. It does not
authorize an implementation and is not itself normative. The writing effort
is complete because the pages indexed in [README](README.md) define one coherent, testable
contract for taking an admitted work item through implementation, independent
review, publication, and post-publication remediation.

The specification must preserve a visible distinction between:

- facts verified in the current repository;
- accepted design decisions;
- requirements proposed for v1; and
- deliberately deferred work.

## Design baseline

The first draft should treat these decisions as its baseline. Any proposed
change to one of them must be raised as a cross-page review issue instead of
being changed implicitly in one page.

1. The forge owns work-item intake and the final pull request, CI, review, and
   merge state. Orcest owns the pre-PR implementation, verification, review,
   consensus, and recovery lifecycle.
2. Orcest durably records its workflow state, including Attempt claim state,
   generation fencing, and persistent deadlines. Redis carries disposable
   queue, wake-up, renewable liveness lease, heartbeat, and cache state that can
   be rebuilt from the durable store.
3. v1 consolidates project workflow ownership in one central controller and
   uses a local SQLite database in WAL mode with `synchronous=FULL`, on local
   persistent storage, behind that single writer. PostgreSQL and a
   multi-controller topology are deferred until scale requires them.
4. A transactional outbox records a schedulable activity before it is
   published to Redis. Redis loss may duplicate execution but must not lose or
   manufacture a durable workflow transition.
5. Workers receive read-only source access. They claim activities through an
   authenticated controller endpoint and receive attempt-scoped capabilities;
   they do not receive authority to publish branches or open pull requests.
   SQLite and Redis store only opaque secret references. A controller-owned
   Secret Store durably owns forge and provider credentials and their rotated
   versions.
6. Workers submit immutable candidate artifacts to the controller. The
   controller validates and stores them durably before accepting a candidate.
   Active, uncommitted worker state remains retryable rather than durable.
7. New activity delivery uses protocol-versioned streams so incompatible old
   workers cannot consume new messages.
8. Workflow transitions are selected by a deterministic, code-owned reducer.
   Repository configuration parameterizes the reducer and is pinned for a run;
   candidate changes cannot alter an active run's policy.
9. Execution is at least once. Result acceptance is at most once for a fenced
   activity generation. External side effects, including branch and pull
   request creation, are idempotent and reconciled rather than claimed to be
   transactional or exactly once.
10. Acceptance policy is never weakened because a provider or worker is
    unavailable. Orcest retries, replaces, diagnoses, replans, adjudicates, or
    waits before considering human intervention.
11. `needs-human` is exceptional and requires an allowlisted reason proving
    that information, authority, or judgment cannot be obtained autonomously.
    Capacity, timeouts, disagreement, malformed output, failing tests, and
    exhausted retries are recovery inputs, not human-escalation reasons.
12. Post-PR remediation is owned by Orcest but fenced to the forge head SHA it
    observed. Run-owned PRs and legacy-managed PRs must not be processed by both
    engines.

## Required v1 defaults

The normative pages must either adopt these defaults or record an explicit
cross-page change before review. They are not open-ended placeholders.

- One Run remains active through pre-PR work, publication, PR monitoring, and
  remediation. `needs-human` is an exceptional, resumable waiting state rather
  than a terminal outcome. Merge, non-merge closure, and cancellation are
  terminal outcomes.
- Admission commits the unique active Run before requesting the forge label
  projection from `orcest:ready` to `orcest:working`. Startup reconciliation
  scans both labels. A `working` work item with no active Run or Publication is
  restored or readmitted autonomously rather than left invisible.
- A Work Item Snapshot pins the forge and repository identity, title, body,
  base commit, and normalized workflow hash. Comments are not specification
  inputs unless repository policy explicitly declares them as such.
- Explicit cancellation before the Publication reaches
  `CHANGE_REQUEST_OBSERVED` cancels directly only when no durable Change
  Request-create call may be in flight and either no Publication/create
  workflow exists or a current search checkpoint names the exact matching
  `CHANGE_REQUEST_ABSENT` Forge Observation. If a Publication/create
  workflow exists without that current absence proof, including before a create
  request is ready as well as when one is ready or ambiguous, cancellation
  remains nonterminal while Orcest reconciles that stable request; only that
  typed absence plus the successful cleanup Fact permits `CANCELLED`, while
  discovery requires an idempotent owned close. At or after
  observation, only authenticated unmerged closure (`CANCELLED`) or merge
  (`MERGED`) terminalizes the race. Work Item closure alone does not close an
  already observed Change Request. A changed specification hash
  supersedes the current specification generation at a safe activity boundary
  and starts a new generation without weakening its gates.
- A Candidate is an immutable, content-addressed Git bundle with one proposed
  tip. The controller validates it against the pinned base, persists and fsyncs
  it, and only then commits the database reference. Orphan artifacts are safe
  to collect; a live database reference to a missing artifact is forbidden.
  The verified commit SHA identifies the Candidate for review and publication;
  the bundle digest addresses its stored representation.
- The default consensus policy requires two independent valid approvals and no
  unresolved blocker on the exact Candidate. Repository policy may strengthen
  this requirement. Missing capacity may delay or substitute an allowed role,
  but it never lowers the configured threshold.
- Publication uses a deterministic branch and run marker, points to the exact
  approved commit, checks for an existing side effect before retry, and uses
  compare-and-swap semantics for later remediation.
- Workflow v1 includes the authenticated worker, pool-manager, and
  budget-accounting control-plane
  APIs required by its protocols plus narrow Run-command, storage-restoration,
  Secret-provision, idempotent project-registration, controller-mode, and
  capability-key-operation surfaces (project registration is used by
  `orcest project onboard`). Fleet/Proxmox provisioning, server
  enrollment, principal lifecycle, general administration, and RBAC remain a
  companion management specification. The architecture must reserve that
  boundary and the repository-configuration page must define the exact
  project-registration inputs without expanding workflow v1 into a complete
  Proxmox management API.
- The v1 Secret Store is a controller-only local backend on persistent local
  storage with `0600` permissions and atomic, fsynced immutable-version
  installation. SQLite stores stable secret references and versions, never
  secret values. Rotation persists a new secret version before committing its
  reference; orphan versions are recoverable garbage, while a live reference
  to a missing secret fails closed. The persistence page owns backup and
  rotation reconciliation. A later hosted deployment may replace this backend
  with a dedicated secret manager.

## Normative vocabulary

Use these terms consistently throughout the specification:

- **Work Item:** forge-neutral intake item; a GitHub issue is one adapter's
  representation.
- **Work Item Snapshot:** immutable specification and source inputs pinned for
  one specification generation.
- **Run:** the durable lifecycle admitted for a Work Item.
- **Activity:** one durable planned unit of work.
- **Attempt:** one fenced execution generation of an Activity.
- **Candidate:** one immutable, controller-admitted proposed commit artifact.
- **Verification Receipt:** structured deterministic-tool evidence for one
  Candidate.
- **Review Receipt:** one independent reviewer's structured verdict and
  findings for one Candidate.
- **Consensus Decision:** the reducer's deterministic aggregation result.
- **Publication:** the reconciled external branch and Change Request side
  effect for an approved Candidate.
- **Change Request:** forge-neutral published review object; a GitHub pull
  request is one adapter's representation.
- **Forge Observation:** an external snapshot bound to a forge revision, such
  as a PR head SHA.
- **Projection:** a non-authoritative label, comment, Check, dashboard value,
  or event derived from durable state.

Avoid an unqualified global “source of truth.” Authority is domain-specific.
Avoid “completed” without naming the completed entity, “blocked” for temporary
conditions, and “exactly once” for external operations.

For this specification, deterministic means:

> Given the same persisted inputs, receipts, and ordered external observations,
> the reducer emits the same transition and next activity plan. Agent output
> may vary, but it cannot vary lifecycle semantics.

## Cross-page invariants

Every normative page must preserve the applicable invariants below. The domain
model and lifecycle pages will assign their canonical identifiers and formal
expressions.

1. At most one active Run exists for a project and Work Item.
2. Activity and Attempt identities are immutable; attempt generation only
   increases.
3. Only the current fenced attempt generation can have a result accepted.
4. An Activity and its outbox record commit before dispatch.
5. Redis loss cannot erase, invent, or permanently suppress durable planned
   work.
6. Active uncommitted invocation state is retryable; admitted Candidate
   boundaries are durable.
7. SQLite never references a Candidate artifact that is not durably present.
8. Every verification, review, and consensus receipt binds to one exact
   Candidate; a new Candidate invalidates all earlier receipts.
9. The reducer alone selects transitions, and review aggregation is independent
   of receipt arrival order.
10. Worker, provider, timeout, budget, or capacity failures never weaken
    acceptance policy.
11. Workers cannot publish Change Requests or mutate workflow authority,
    publication branches, or another Run's artifacts.
12. Candidate code executes without forge-write, controller-write, or
    unrelated-Run credentials.
13. External side effects are idempotent and reconciled, not assumed exactly
    once.
14. Publication and remediation compare-and-swap against an observed forge
    revision, and one engine owns a run-associated Change Request at a time.
15. Secrets never enter normal Redis envelopes, workflow rows, candidate
    artifacts, traces, projections, or events; only opaque Secret Store
    references may appear there.
16. `needs-human` requires a controller-issued allowlisted reason proving an
    information, authority, irreversible-action, integrity, or policy boundary.

## Canonical page ownership

Each requirement must have one canonical home. Other pages should link to it
and describe only their side of an interface.

| Page | Canonically owns | Depends on |
| --- | --- | --- |
| `architecture.md` | Authority boundary, components, deployment topology, trust zones, controller identity, forge-neutral adapter seams, goals and non-goals | Current-state evidence and the design baseline |
| `domain-model.md` | Identifiers and schemas for work items, intake snapshots, runs, activities, attempts, candidates, reviews, transitions, publications, and external observations; uniqueness and immutability rules | Architecture vocabulary |
| `workflow-lifecycle.md` | Reducer inputs and outputs, state graph, legal transitions, specification/base changes, cancellation, retry and autonomous recovery ladder, typed exceptional escalation | Domain objects and architecture authority |
| `worker-protocol.md` | Stream/version negotiation, delivery envelope, authenticated claim, leases/heartbeats, attempt generations, credential and capability scope, artifact upload/admission, result receipts | Domain identifiers, lifecycle transition contract, persistence atomicity assumptions |
| `review-and-consensus.md` | Verification evidence, reviewer independence, role staffing, structured verdicts, approval and blocker rules, invalidation on candidate change, remediation and adjudication | Candidate model, lifecycle, worker protocol |
| `persistence-and-recovery.md` | SQLite settings and transactions, schema persistence boundaries, outbox, Redis reconstruction, artifact and Secret Store durability, credential-rotation reconciliation, backup/restore, crash matrix, retention and garbage collection | Domain model and side-effect boundaries from worker/forge protocols |
| `forge-integration.md` | Intake adapter, label/status projection, issue mutation policy, deterministic publication identity, branch/PR reconciliation, CI/review observation, SHA-fenced remediation, legacy-loop exclusion | Architecture boundary, lifecycle, publication records, persistence recovery |
| `repository-configuration.md` | `.orcest` layout and schema, validation, defaulting, policy limits, trusted-revision lookup, normalization/hash pinning, CLI lint/simulate/onboard contract | Reducer parameters, reviewer roles, forge capabilities |
| `operations-and-rollout.md` | Controller restart and drain behavior, rolling protocol compatibility, migrations, monitoring, backup drills, failure injection, staged adoption and rollback | All protocol pages |

Cross-cutting security requirements stay with their enforcement point:

- `architecture.md` defines actors, assets, trust boundaries, and threat
  assumptions.
- `worker-protocol.md` defines worker authentication and least privilege.
- `persistence-and-recovery.md` defines integrity and storage protections.
- `forge-integration.md` defines forge permissions and publication authority.
- `repository-configuration.md` defines trusted configuration loading.
- `operations-and-rollout.md` verifies and audits those controls.

## Required repository evidence

Authors must inspect the implementation before stating how v1 replaces or
preserves current behavior. At minimum, the first draft must cite or summarize
evidence from these areas:

| Evidence area | Repository starting points | Question it must answer |
| --- | --- | --- |
| Current state and recovery promises | `docs/wiki/current-orchestrator-state-model.md`, `README.md`, `src/orcest/shared/coordination.py` | Which GitHub/Redis invariants exist today, and which authority moves into the run store? |
| Issue discovery and admission | `src/orcest/orchestrator/issue_ops.py`, `src/orcest/orchestrator/issue_deps.py`, `src/orcest/orchestrator/loop.py` | How does `orcest:ready` become one durable active run, and how are `ready`/`working` projections reconciled? |
| Task publication and result handling | `src/orcest/orchestrator/task_publisher.py`, `src/orcest/orchestrator/loop.py`, `src/orcest/shared/models.py` | Where are task credentials and snapshots carried now, and where will planning, fencing, and receipt acceptance occur? |
| Worker execution and workspace authority | `src/orcest/worker/loop.py`, `src/orcest/worker/workspace.py`, `src/orcest/worker/_runner_base.py` | What access must be removed from workers, and what is lost when an active invocation dies? |
| Redis streams and recovery | `src/orcest/shared/redis_client.py`, `src/orcest/shared/coordination.py`, `tests/integration/test_task_flow.py`, `tests/integration/test_mixed_provider_streams.py` | Which data is delivery-only, how is it rebuilt, and how do versioned consumers coexist? |
| Provider selection and failure handling | `src/orcest/orchestrator/provider_pool.py`, `src/orcest/shared/providers.py`, worker runner modules | How are independence, fallback, rate limiting, and policy-preserving replacement represented? |
| Credential storage and rotation | `src/orcest/shared/credential_handoff.py`, `src/orcest/orchestrator/loop.py`, `src/orcest/fleet/config.py` | Which raw secrets currently cross Redis, where do rotated values survive, and how will durable secret references be reconciled? |
| Fleet loss and lifecycle reports | `src/orcest/fleet/pool_manager.py`, `src/orcest/fleet/orchestrator.py`, fleet tests | How does a lost VM become a fenced attempt result, and what topology can enforce a single logical writer? |
| Existing SQLite precedent | `src/orcest/monitor/db.py`, `tests/monitor/test_db.py` | Which SQLite practices can be reused, and which durability requirements are new? |
| GitHub snapshot and PR behavior | `src/orcest/orchestrator/pr_ops.py`, `src/orcest/orchestrator/gh.py`, PR tests | How are observations tied to head SHA, and how will publication/remediation be reconciled without races? |
| Deployment and upgrade behavior | `src/orcest/fleet/deploy/`, `README.md`, `docs/operations/` | Where will local durable storage live, and which changes require drain versus restart-safe recovery? |

Each normative page must include an "Evidence and migration" section listing
the current paths it affects, existing guarantees retained, guarantees being
replaced, and unknowns that require a spike or failure-injection test.

## Writing sequence

### Gate 0: evidence freeze

Before drafting normative requirements:

1. Complete `current-orchestrator-state-model.md` and verify it against code
   and tests.
2. Record open discrepancies without repairing them in the current-state page.
3. Verify the migration from the current per-project orchestrators to one v1
   controller for all projects, including pool-manager callback routing and
   project failure isolation.
4. Inventory current worker credentials, Redis streams, task versions, issue
   labels, PR markers, and externally visible side effects.

Exit gate: reviewers can trace every claimed current invariant to code or a
test, and topology is stated without contradiction.

### Gate 1: system contract

Draft in order:

1. `architecture.md`
2. `domain-model.md`
3. the state graph and transition table in `workflow-lifecycle.md`

These pages establish vocabulary and ownership before lower-level protocol
pages introduce fields or transitions.

Exit gate: every durable object has an owner and stable identifier; every
state transition names its triggering fact, required preconditions, durable
write, emitted activity, and idempotency key; no LLM output selects a state
transition directly.

### Gate 2: execution and decision protocols

After Gate 1 stabilizes, draft these pages in parallel:

- `worker-protocol.md`
- `review-and-consensus.md`
- `persistence-and-recovery.md`

Reconcile them in one joint protocol review. The same terms must describe an
activity from outbox creation through Redis delivery, claim, attempt outcome,
candidate admission, review, and reducer transition.

Exit gate:

- every message has a protocol version and compatibility rule;
- claim and generation fencing prevent two results from being accepted;
- claimed Attempt state, generation, and deadlines survive Redis loss through
  the durable store;
- credentials and upload capabilities have bounded audience and lifetime;
- normal queue messages and workflow rows contain secret references rather
  than raw credentials, and rotation is recoverable across every storage
  boundary;
- a candidate is recoverable before its result is accepted;
- review policy cannot be weakened by missing capacity;
- the recovery ladder covers infrastructure, provider, worker, validation,
  disagreement, repeated-fix, and waiting conditions;
- `needs-human` is reachable only through a typed exceptional boundary; and
- the crash matrix covers every transaction and external-storage boundary.

### Gate 3: boundaries and configuration

Draft in order:

1. `forge-integration.md`
2. `repository-configuration.md`

The forge protocol must settle publication and remediation semantics before
configuration exposes policy choices. Configuration may select among supported
code paths; it may not define arbitrary states or executable control flow.

Exit gate:

- admission cannot create two active runs for one configured work item;
- issue labels are explicitly projections and startup reconciliation scans all
  states necessary to recover ownership;
- branch and PR creation can be safely retried after any ambiguous response;
- remediation uses compare-and-swap semantics against the observed head SHA;
- the active workflow uses a normalized configuration pinned from a trusted
  base revision; and
- forge-specific details are contained behind capability-checked adapters.

### Gate 4: operations and rollout

Draft `operations-and-rollout.md` only after the protocols stabilize. It must
translate every guarantee into an observable, operable, and testable behavior.

Exit gate:

- controller-only restart works with active runs;
- Redis loss and rebuild are exercised, not merely described;
- old workers cannot consume the new protocol streams;
- backup/restore consistently includes the database, Candidate artifacts,
  Snapshot-referenced Workflow Blobs, the encrypted Secret Store, and accepted
  pending restoration/Secret-provision staging roots;
- staged rollout prevents the new run engine and legacy PR loop from owning the
  same PR;
- rollback behavior is defined after both pre-PR work and publication; and
- retention cannot delete an artifact required by a live or auditable run.

## Required failure and conformance scenarios

The combined specification is not reviewable without expected outcomes for at
least these scenarios:

- Kill the controller after activity/outbox insertion, Redis publication,
  worker claim, artifact upload, candidate admission, publication branch push,
  PR creation, and publication-record update.
- Flush or restart Redis while activities are queued and while workers are
  running; rebuild deliverable work from SQLite without inventing completion.
- Flush Redis after a worker claim; preserve the claimed Attempt generation and
  deadline while rebuilding only its disposable liveness state.
- Deliver one activity to two workers and accept only one fenced generation.
- Submit an old-generation result after its replacement has started.
- Destroy a worker with uncommitted edits, then retry from the last durable
  candidate boundary.
- Run old and new worker versions simultaneously and prove stream isolation.
- Return independent reviews in every order and obtain the same decision.
- Lose reviewer/provider capacity and preserve the configured acceptance
  policy while waiting or selecting an allowed substitute.
- Modify `.orcest` in a candidate and prove the active run does not change.
- Change the issue specification or base branch during a run and apply the
  configured deterministic policy.
- Advance the PR head during remediation and reject the stale push without
  overwriting external work.
- Crash after PR creation and discover the existing PR on reconciliation.
- Crash after writing a rotated secret but before updating its reference, and
  after updating the reference but before acknowledging rotation; reconcile to
  a usable version without exposing the value through Redis or SQLite.
- Exercise every `needs-human` reason and prove routine failures cannot reach
  it, then resume the same Run after the exceptional boundary is resolved.

## Review gates

The full specification requires four focused reviews before acceptance:

1. **Architecture review:** ownership, deployment topology, portability, and
   non-goals are internally consistent.
2. **Protocol and failure review:** an adversarial reviewer traces duplicates,
   reordering, partial failure, credential leakage, stale generations, and
   concurrent external mutation across page boundaries.
3. **Autonomy review:** every nonterminal failure has a self-healing or waiting
   path; human escalation requires a minimal decision packet and an allowlisted
   exceptional reason.
4. **Implementation-readiness review:** maintainers can derive schemas, APIs,
   reducer tests, migrations, rollout stages, and operational alerts without
   inventing missing semantics.

A page may be marked `Accepted` only when its dependencies are accepted or are
accepted in the same review set, its terms match the domain model, its failure
behavior is explicit, and all review findings are resolved or recorded as a
deliberate deferral.

## Review record

Overall v1 specification verdict: **ACCEPTED — 2026-08-27**.

This record separates normative design acceptance from production rollout
evidence. The resolved findings below are incorporated into the canonical
pages. Items explicitly labeled as rollout or implementation validation gates
are deliberately deferred until an implementation exists; they do not permit
an implementation to weaken or omit the normative contract.

| Review class | Resolved findings recorded in the normative pages | Verdict |
| --- | --- | --- |
| Architecture | Closed forge/controller/worker authority; portable controller-owned pre-PR state; SQLite/Artifact/Secret durability; narrow authenticated HTTPS management surfaces; launch-isolation and capability-key trust boundaries; legacy coexistence and live-Publication exclusion. | `PASS` |
| Protocol and failure | Global idempotency identities; trigger single-consumption; claim/result/deadline fences; durable pre-I/O intents and outboxes; restartable publication/checkpoint/search/credential operations; exact crash, stale-response, and replay behavior. | `PASS` |
| Autonomy | Evidence-only recovery application; deterministic retry/diagnosis/wait ladder; complete capacity/panel staffing; integrity-probe chain; exceptional, resumable, proof-bound Human Boundaries; duplicate and ownership self-repair before escalation. | `PASS` |
| Implementation readiness | Closed schemas/enums/digests/nullability; transaction and projection ownership; migration/backup/retention rules; deterministic conformance cases. Adapter, harness, sandbox, scale, and failure-injection experiments are retained as explicit production-enablement gates. | `PASS` |

Acceptance followed three independent, read-only whole-wiki gates covering
lifecycle/autonomy, schema/protocol/persistence, and implementation readiness.
Each final gate reported zero medium or material findings. Earlier Claude
Fable review rounds informed the design and their findings were incorporated;
a fresh post-repair Fable rerun was unavailable because the CLI quota was
exhausted, so it was not counted as a final acceptance gate. The mechanical
gate also verified every local link and anchor, every fenced JSON/YAML example,
heading identity, whitespace, and stale terminology.

A 2026-08-29 independent adversarial follow-up identified five additional
cross-page hardening gaps, all resolved in the normative pages: authenticated
cumulative Budget Reports and deterministic expiry/fanout; source-unique Forge
Request Failure Facts; an exhaustive Wait reason/wake matrix with race-safe
Wait insertion; the closed Controller Operation failure-category matrix; and
the Stage-3 requirement that duplicate publication or stale overwrite fails
the gate. The same pass rechecked Result replay semantics and confirmed that
only an identical semantic digest replays; a different body remains the
pre-registry `RESULT_ALREADY_ACCEPTED` conflict.

Any later finding must name its canonical owner page and either be resolved
there or be recorded as a deliberate non-normative rollout deferral with a
reason; unresolved normative semantics cannot be deferred.

## Full-spec acceptance criteria

The v1 specification is complete when:

- all nine accepted pages exist and the index points to their canonical topic;
- requirements use consistent identifiers, states, reason codes, and protocol
  versions;
- every state is recoverable from SQLite plus durable Candidate, Workflow
  Blob, Secret Store, and accepted restoration/provision-staging storage, with
  Redis reconstructed as disposable coordination;
- the authority boundary is explicit before, during, and after PR publication;
- deterministic code, not model prose, performs transition and consensus
  decisions;
- workers cannot publish code or mutate workflow authority directly;
- publication and post-PR remediation tolerate crashes and concurrent forge
  changes without duplicate PRs or overwritten commits;
- temporary failures use autonomous recovery without weakening correctness,
  quorum, or security requirements;
- human escalation is both exceptional and mechanically auditable;
- the implementation delta and staged migration from the current system are
  enumerated; and
- failure-injection scenarios have unambiguous expected results suitable for
  automated tests.

## Explicit deferrals

Unless later evidence makes them necessary for v1, keep these out of the first
specification:

- PostgreSQL and active-active/multi-writer controllers;
- a general-purpose internal Git hosting service;
- preservation or migration of an agent process's live, uncommitted workspace;
- arbitrary repository-defined state machines or executable workflow code;
- weakening consensus dynamically in response to cost or capacity; and
- a separate ADR hierarchy before a real cross-page decision requires one.
