# Workflow-Control Architecture

> **Status:** Accepted normative v1 specification (2026-08-27)
>
> **Canonical owner:** system authority, component topology, trust boundaries,
> controller identity, and forge-neutral integration seams.

This page defines the v1 architecture for the workflow Orcest owns after a
forge Work Item is admitted and before, during, and after publication of its
Change Request. Object identities are defined in the [domain
model](domain-model.md), and legal state changes are defined in the [workflow
lifecycle](workflow-lifecycle.md).

`MUST`, `SHOULD`, and `MAY` on this page are normative v1 requirements.

## Goals

Workflow-control v1 MUST:

1. turn an eligible forge Work Item into a durably tracked Run before any
   worker is dispatched;
2. own implementation, verification, independent review, consensus,
   publication, and post-publication remediation as one continuous Run;
3. make every lifecycle decision reproducible from durable inputs and ordered
   external observations;
4. tolerate controller and Redis restarts without losing planned work or
   accepting a stale result;
5. keep candidate code and ephemeral workers outside the controller's trust
   boundary;
6. preserve configured correctness and review gates when workers, providers,
   or external systems are unavailable;
7. recover autonomously whenever progress does not require new authority,
   missing specification information, or an irreversible human decision; and
8. isolate forge-specific behavior behind adapters so a future forge can be
   added without changing the reducer or domain model.

## Non-goals

Workflow-control v1 does not provide:

- a hosted, active-active, or multi-writer controller;
- PostgreSQL or a general-purpose internal Git hosting service;
- durable preservation of a live agent process or its uncommitted workspace;
- arbitrary repository-defined states or executable workflow control flow;
- fleet, Proxmox VM/image/network provisioning, or remote fleet lifecycle;
- general server administration, principal issuance, or RBAC role management;
- enabling or initiating Change Request merge; v1 observes forge-managed merge
  or closure after the final handoff;
- a guarantee that a worker executes only once; or
- a distributed transaction spanning Orcest, Redis, artifact storage, and a
  forge.

Authenticated project registration inputs belong to the [repository
configuration specification](repository-configuration.md). The workflow
controller exposes only the narrow worker, pool-manager, budget-report, Run-command,
launch-attestation, storage-restoration, secret-provisioning,
project-registration, controller-mode, and capability-key surfaces
defined by the normative pages. Fleet provisioning, server enrollment,
general administrative RBAC, principal lifecycle, Proxmox access, and remote
fleet lifecycle remain a companion management specification.

## Authority boundary

Authority is domain-specific. No component is an unqualified global source of
truth.

| Domain | Authoritative owner | Orcest behavior |
| --- | --- | --- |
| Work Item title, body, state, dependencies, and labels | Forge | Observe through an adapter and pin accepted inputs in a Work Item Snapshot. |
| Intake eligibility | Forge facts plus trusted repository policy | Admit at most one active Run for a project and Work Item. Labels are projections, not locks. |
| Pinned specification generation | Orcest Run Store | Continue from the immutable snapshot or supersede it according to the lifecycle rules. |
| Pre-PR plans, activities, attempts, candidates, receipts, consensus, and recovery | Orcest Run Store and Candidate Store | The controller is the only writer of workflow authority. |
| Task delivery, renewable liveness, wake-ups, and caches | Redis | Reconstruct from the Run Store; never infer durable completion from Redis absence or presence. |
| Provider and forge secret values | Orcest Secret Store | Reference by opaque identifier and version everywhere else. |
| Final branch and Change Request state | Forge | Publish and update through reconciled compare-and-swap operations. |
| CI, reviews, Change Request head, merge, and closure | Forge | Record ordered Forge Observations and let the reducer determine the next Orcest action. |
| Status labels, comments, Checks, dashboards, and events | Projection target | Repair from authoritative state; never use them to prove a workflow transition. |

The ownership handoff is therefore:

```text
Forge Work Item
    |
    | eligible intake observation
    v
Orcest Run
    plan -> build -> verify -> review -> consensus -> recover as needed
    |
    | exact approved Candidate, reconciled publication
    v
Forge Change Request
    CI/review observation -> Orcest remediation -> fenced update -> observation
    |
    v
merged, closed without merge, or explicitly cancelled
```

Publication does not end the Run. It changes which system owns the reviewed
external state: the forge owns the Change Request, while Orcest continues to
own any remediation activities it initiates in response.

A forge-observed merge does end semantic Run work. If its complete ownership
search also finds live same-marker duplicates, the terminal transaction creates
the bounded controller-owned Terminal Duplicate Cleanup Reservation. That
ledger may reconcile/close an exact-owned reliance-free duplicate or detach
only its Orcest marker under CAS, but it cannot reopen the Run, alter the
selected merged Change Request, or perform implementation work.

## Deployment topology

v1 has one central workflow controller for all registered projects and one
logical writer to the workflow database.

```text
                         +-----------------------+
                         | Forge adapters        |
                         | intake / PR / CI      |
                         +-----------+-----------+
                                     |
                          observations|side effects
                                     |
+------------------+       +---------v----------+       +------------------+
| Secret Store     |<----->| Central controller |<----->| Candidate Store  |
| local, durable   | refs  | reducer / APIs     | refs  | local, durable   |
+------------------+       +---------+----------+       +------------------+
                                     |
                          transaction|outbox
                                     v
                           +---------+----------+
                           | SQLite Run Store   |
                           | WAL, FULL sync     |
                           +---------+----------+
                                     |
                               dispatch/wakeup
                                     v
                           +---------+----------+
                           | Redis              |
                           | disposable         |
                           +----+-----------+---+
                                |           |
                           +----v---+   +---v----------+
                           | workers|   | pool manager |
                           +--------+   +--------------+
```

The controller process MUST serialize all workflow writes. Reads and external
I/O MAY be concurrent, but their results MUST enter the state machine as
ordered, immutable inputs. A project-specific adapter failure MUST not stop the
controller from advancing unrelated projects.

The SQLite Run Store, Candidate Store, and Secret Store MUST reside on durable
local storage owned by the controller host. The SQLite database MUST use WAL
mode and `synchronous=FULL`. It MUST NOT be placed on the current network trace
archive or another filesystem whose SQLite locking and fsync semantics have not
been validated. Backup and restore atomicity are owned by [persistence and
recovery](persistence-and-recovery.md).

PostgreSQL and more than one active controller writer are explicitly deferred.
A future topology MAY replace storage implementations, but it MUST preserve
the domain identities and invariants in the [domain model](domain-model.md).

## Components

### Central controller

The controller is the only component permitted to:

- admit, supersede, cancel, or terminate a Run;
- invoke the deterministic reducer;
- create Activities and fenced Attempt generations;
- issue and consume one-shot launch capabilities and validate signed Launch
  Attestations from the pinned launch-isolation boundary;
- accept a worker result or a Candidate;
- create Verification Receipts, Review Receipts, and Consensus Decisions after
  validating their schema and binding;
- issue worker and pool-manager capabilities;
- authenticate and authorize the narrow Run-command, storage-restoration,
  secret-provisioning, project-registration, controller-mode, and
  capability-key inputs, plus authenticated budget-accounting reports, before
  they can affect durable state;
- resolve Secret References to secret values;
- create or update publication branches and Change Requests, execute the
  exact fenced `CLOSE_REDUNDANT_PUBLICATION` repair selected by durable
  reconciliation or the exact-CAS `REPAIR_RUN_MARKER` repair for an already-
  linked proved-owned Change Request, and finish a durable post-merge Terminal
  Duplicate Cleanup Reservation without reopening the Run; and
- write authoritative workflow transitions.

The controller MUST distinguish command handling from reducer evaluation.
Commands and external observations are validated inputs. Only the code-owned
reducer selects a lifecycle transition or plans the next Activity.

### Authenticated control surface

The v1 controller surface is closed by purpose:

| Surface | Authorized caller | Authority and canonical owner |
| --- | --- | --- |
| Attempt claim, liveness, source/Candidate transfer, Result, and credential rotation | Registered worker session with an Attempt-scoped capability | [Worker protocol](worker-protocol.md); cannot select lifecycle state or publish. |
| `POST /api/v1/attempts/{attempt_id}/launch-attestations` | Snapshot-pinned registered runner-shim principal with the exact Attempt/session-bound one-shot launch capability; path, body, and capability Attempt IDs must match | [Domain model](domain-model.md#launch-attestation); may attest launch isolation and receive exact scoped provider material for that Attempt only, never Result or workflow authority. |
| Capacity and exact-session loss reports | Registered pool-manager principal | [Worker protocol](worker-protocol.md); supplies durable health/loss input only. |
| `POST /api/v1/budget-reports` | Registered budget-accounting service principal authorized for the exact Project/accounting scope | [Domain model](domain-model.md#budget-report); records cumulative integer consumption and can only supply the deterministic offer gate and restartable budget-wake fanout. It cannot select lifecycle state, weaken policy, or report for another scope. |
| `POST /api/v1/runs/{run_id}/commands` | Server-authenticated management principal with exact Project/Run/action RBAC | [Workflow lifecycle](workflow-lifecycle.md); only closed `CANCEL` and `RESOLVE_HUMAN_BOUNDARY` commands. |
| `POST /api/v1/storage/restorations` | Server-authenticated storage operator, or the internal storage reconciler for the separate backup path | [Domain model](domain-model.md) and [persistence](persistence-and-recovery.md); stages one exact object and cannot choose Run state. |
| `POST /api/v1/secrets/provisioning-operations` | Server-authenticated secret operator with exact secret purpose/owner/account and provision-or-adopt authority | [Domain model](domain-model.md#secret-provision-operation); accepts one durable staged `PENDING` operation before asynchronous install can create its exact target version, never general identity/RBAC authority. |
| `POST /api/v1/projects/registrations` used by `orcest project onboard` | Server-authenticated onboarding principal with forge-installation and Project-registration authority | [Repository configuration](repository-configuration.md); registers and validates one Project but does not provision fleet infrastructure. |
| `POST /api/v1/controller-mode-operations` | Server-authenticated operations principal with controller-mode authority; the registered bootstrap service alone may `INITIALIZE`, and the registered storage reconciler alone may `RESTORE_BACKUP` | [Domain model](domain-model.md#controller-mode-and-operation); CASes the closed durable mode projection, never a Run state. |
| `POST /api/v1/capability-key-operations` | Server-authenticated key-operator principal with capability-registry authority | [Domain model](domain-model.md#capability-key-registry-and-operation); registers/selects/retires/revokes verifier versions by exact Registry CAS. |

Every controller endpoint—worker, runner-shim, pool-manager, budget report, Run command,
storage restoration, secret provisioning, project onboarding/registration, and
any future endpoint on the same listener—MUST use HTTPS with full certificate
chain and hostname/SPIFFE identity validation appropriate to its registered
principal. Clients fail closed on validation error. Plaintext HTTP, opportunistic
TLS, and disabling certificate validation are forbidden even on loopback,
cluster, VPN, or otherwise private networks. This transport rule is global;
endpoint-specific capabilities and RBAC remain additional requirements.

Fresh-store Stage 0 is fail-closed: mode `INITIALIZE` to revision-1
`MAINTENANCE`; provision/adopt the exact controller capability-signing Secret
with its real creation Receipt; key `REGISTER`; separate key `SELECT`; then
other Secrets and Projects. The narrow management endpoints may perform these
ordered operations in maintenance, but ordinary Run work and Forge Observation
Schedules remain frozen until an authenticated mode exit. No process-local key,
synthetic Secret/Receipt, or combined register/select bypass exists.

Every surface uses server-owned authentication, RBAC, request bounds, and
idempotency. Repository files, workers, forge comments, and bearer material
for one surface grant no authority on another. An implementation MUST reject
unknown general-admin or fleet-lifecycle operations rather than treating this
narrow API set as an implicit Proxmox, identity, or RBAC management API.

### Run Store

The Run Store durably holds the objects in the [domain model](domain-model.md),
including immutable Attempt Claims, claimed Attempt state, monotonically
increasing generations, persistent workflow/authentication deadlines, bounded
global Result-request, Candidate-upload, credential-rotation, and health-probe
ledgers, durable Health Probe Requests and Forge Observation Schedules/Requests
with outbox intent before external probe/poll/search I/O, Controller
Mode and Capability Key Registry operation ledgers, durable
capability-signing public-verification keys and key state,
terminal duplicate cleanup reservations/members/actions, and the transactional
outbox. It MUST support rebuilding all deliverable work
without consulting Redis.

The Run Store MUST NOT contain raw forge tokens, provider credentials, upload
bearer values, or rotated credential values. It stores stable Secret References
and their versions.

### Candidate Store

The Candidate Store holds immutable content-addressed Git bundles. A worker
uploads through an Attempt-scoped controller capability; it never writes the
store directly. The controller MUST validate the bundle, determine its single
proposed tip, verify its relationship to the pinned base, persist and fsync it,
and only then commit a live Candidate reference in SQLite.

Candidate content is untrusted even after admission. Admission establishes
integrity and shape, not safety or correctness. Code from a Candidate MUST run
without forge-write, controller-write, Secret Store, or unrelated-Run
credentials.

### Secret Store

The v1 Secret Store is a controller-only local backend on durable local
storage. Secret files MUST be accessible only to the controller identity, use
`0600` permissions, and be replaced atomically with file and parent-directory
fsync. Rotation MUST persist a new version before SQLite commits the new Secret
Reference. A live reference to a missing version fails closed.

Secret values MUST NOT enter ordinary Redis messages, workflow rows, Candidate
artifacts, traces, events, logs, or projections. A later hosted deployment MAY
replace the backend with a dedicated secret manager without changing a Secret
Reference.

### Redis

Redis is a delivery and liveness mechanism, not workflow authority. It MAY
hold:

- protocol-versioned Activity notifications;
- disposable consumer-group delivery state;
- renewable worker liveness leases and heartbeats;
- wake-up signals;
- bounded operational caches; and
- opaque Secret References when a protocol field requires one.

An Activity and its outbox record MUST commit before dispatch. Deleting all
Redis state MUST be recoverable by replaying undelivered or unfinished durable
Activities. Redis loss MAY cause repeated delivery or execution; it MUST NOT
erase, invent, accept, or permanently suppress a workflow result.

### Workers

Workers are ephemeral and untrusted with respect to workflow authority. A
worker receives a protocol-versioned notification, then authenticates to the
controller and atomically claims the Activity. A successful Claim may return
the exact Attempt-scoped read-only source bootstrap. Model-backed provider
material remains withheld until the registered runner shim submits an accepted
Launch Attestation through that same Attempt's path; deterministic `VERIFY`
receives none.

Workers MUST have read-only source access. They MUST NOT receive forge-write
credentials, publication authority, direct SQLite/Candidate Store/Secret Store
access, or a capability valid for another Run or Attempt. They MAY return
nondeterministic agent output, but they cannot select transitions, lower a
gate, accept their own Candidate, or request `needs-human` directly.

Live workspace edits are not durable. A lost worker is recovered from the last
admitted Candidate or pinned base, not by claiming to restore uncommitted work.
The [worker protocol](worker-protocol.md) owns endpoint and envelope details.

### Pool manager

The pool manager owns VM capacity and reports worker lifecycle facts. In v1 it
MUST authenticate to the controller and report a lost or reaped worker against
the Attempt identity it observed. It MUST NOT manufacture a successful result,
change an Attempt generation, or write authoritative results directly to
Redis.

Capacity loss is an observation. The reducer decides whether to replace a
worker, select another compatible provider, or enter a resumable waiting state.

### Launch-isolation boundary

Every model-backed Attempt launches through a registered pool manager plus an
attested runner shim in an allowlisted immutable runner image. The installed
Snapshot's effective `POLICY_JSON` and `policy_hash` pin the exact pool/runner
principal, image digest, `runner_signature_algorithm`,
`runner_signing_key_id`, and `runner_registration_revision`; a mutable
registry lookup cannot reclassify an active Attempt.

Claim creates a one-shot launch nonce/capability bound to the Attempt and exact
Worker Session. The initial claim response may provide read-only source
bootstrap material but MUST withhold provider material. The runner shim prepares
a fresh workspace, empty non-resumed context, and unique invocation identity,
then signs the canonical Launch Attestation. Only after the controller accepts
that Attestation strictly before the execution deadline and atomically consumes
the nonce may it return scoped provider material and permit that one invocation
to begin. Workspace, context, and invocation IDs are globally non-reusable and
all parent/resume fields are null in v1.

The pool manager, runner shim, image, and runner signing key are trusted only to attest
and enforce launch isolation. They are not trusted to claim that output is
correct, manufacture a Result or Receipt, choose a lifecycle state, fill a
review slot, or publish. The rest of the worker process, model provider,
Candidate code, and agent output remain untrusted.

### Forge adapters

The GitHub adapter is the first implementation of these forge-neutral
capabilities:

- read and fingerprint Work Items and declared dependencies;
- resolve trusted repository revisions and source commits;
- observe labels, state changes, and explicit cancellation inputs;
- publish a deterministic branch and Change Request marker;
- find an existing publication after an ambiguous response;
- observe Change Request head revisions, CI, reviews, merge, and closure;
- update a publication branch with compare-and-swap semantics; and
- conditionally close one exact, proven equivalent and unreviewed redundant
  Change Request using stable ID/head/marker/ref/revision/operation fences;
- classify each complete-search member from exact registered creator,
  create/association, Effect, ref/marker/commit/head evidence; and
- conditionally close or detach only the Run marker from a non-selected live
  duplicate under a durable terminal Reservation and exact CAS fences.

Complete-search reduction is deterministic across the boundary: membership is
split into LIVE and TERMINAL rows, cardinality counts LIVE rows only, and an
exhaustive positive ownership proof is required for every member. A
`TERMINAL/MERGED/POSITIVE` member wins before cardinality or conflict routing;
the bytewise-lowest such ID fixes `MERGED`, while its Reservation remains
durable after Run terminalization until every LIVE member has a closed cleanup
outcome or bounded audit retention.

Adapters MUST return normalized domain inputs and capability errors. Reducer
code MUST NOT branch on GitHub-specific field names, CLI output, label API
shapes, or pull-request numbers. The [forge integration
specification](forge-integration.md) owns exact adapter operations and
reconciliation.

### Repository configuration

The controller loads `.orcest` only from the trusted base revision selected at
admission. It validates and normalizes the supported schema, stores its hash in
the Work Item Snapshot, and uses that pinned configuration throughout one
specification generation. Candidate changes to `.orcest` MUST NOT alter the
active generation.

Configuration MAY choose parameters and supported strategies. It MUST NOT add
states, execute arbitrary controller code, bypass a mandatory gate, or grant a
worker more authority. See [repository configuration](repository-configuration.md).

## Trust model

### Trusted computing base

The v1 trusted computing base consists of:

- the central controller and code-owned reducer;
- SQLite and its local filesystem;
- Candidate Store and Secret Store admission code;
- the durable controller `CapabilitySigningKey` public-verification registry
  and its Secret-Store private key versions, only for capability issuance and
  authentication;
- forge and repository adapters for the operations they implement;
- the registered budget-accounting service principal and normalized server
  policy mapping, only for authenticated cumulative consumption in its exact
  Project/accounting scope;
- the trusted-base `.orcest` configuration after validation;
- operator-controlled controller host identity and backups; and
- the registered pool-manager/runner-shim/image/signing-key boundary, only for
  the narrow claim that a one-shot workspace/context/invocation launch is fresh
  and non-resumed.

Redis, workers outside that narrow attested launch boundary, candidate code,
agent output, provider responses, issue text,
review text, CI output, and webhook/poll responses are untrusted inputs. Forge
observations are authoritative only after adapter authentication and shape
validation; they are still not permission to violate Orcest policy.

### Threats and required controls

| Threat | Required architectural control |
| --- | --- |
| Duplicate or reordered delivery | Durable Attempt generation fencing and idempotent receipt keys. |
| Stale worker finishes after replacement | Accept results only for the current generation. |
| Candidate attempts to alter its workflow | Pin normalized policy from the trusted base; candidate config has no effect. |
| Candidate exfiltrates credentials | Execute without forge-write/controller-write/unrelated credentials and never inject Secret Store contents not required for the Attempt. |
| Worker publishes or overwrites code | Give workers read-only source access; controller alone publishes with compare-and-swap. |
| Two approvals reuse a conversation, workspace, or invocation | Require one signed, nonce-bound Launch Attestation from the Snapshot-pinned runner shim/image/key per model-backed Attempt; enforce globally unique IDs, null parent/resume fields, and exact Result/Receipt binding. |
| Attested runner claims workflow authority | Trust its signature only for launch isolation; the controller independently validates every Result/Receipt and the reducer remains sole transition authority. |
| Capability key is rotated, retired, or revoked | Every capability carries exact key ID/`ED25519`; retain referenced verifiers; retirement permits deterministic rematerialization only within the original window and expired-token signature-equality lookup only for an exact retained Launch Attestation, never authentication; revocation denies both immediately. |
| Process-local mode or loaded signer is stale | CAS the durable Controller Mode and Capability Key Registry revision in the writer transaction; a Redis flag or loaded key alone grants neither dispatch nor issuance. |
| Probe response arrives after restart | Commit a Health Probe Request/outbox before I/O and accept one exact reciprocal Fact/Observation; an unrequested callback has no health authority. |
| Redis disclosure | Store no raw credentials or authoritative-only state in Redis. |
| Agent asks to weaken quorum or involve a person | Reducer ignores lifecycle directives in prose and applies fixed policy and typed escalation rules. |
| Ambiguous forge response | Reconcile using deterministic branch, marker, expected revision, and external lookup before retry. |
| Duplicate controller-marked Change Requests | Freeze a complete ownership-classified proof. Before merge, retain the bytewise-lowest live stable ID and close only one proven equivalent/unreviewed duplicate through a durable `CLOSE_REDUNDANT_PUBLICATION` Activity. If any positively owned merged member exists, select the bytewise-lowest merged ID as irreversible terminal authority and process every remaining live member through a durable post-terminal Reservation: exact-owned reliance-free close, exact-CAS marker detach for relied-on work, or bounded audit-only retention. Changed proof/ambiguity returns to reconciliation and never weakens the selected merge. |
| One project poisons the controller | Bound input sizes and work per project; isolate adapter errors and transactions by project/Run. |
| Missing Candidate or secret behind a live reference | Fail closed as an integrity failure and run storage reconciliation; never continue with substituted content. |

## Failure and availability contract

The architecture provides these guarantees:

```text
worker execution       at least once
result acceptance      at most once per current fenced generation
durable transition     one result per (Run, trigger kind, trigger identity)
external side effect   idempotent and reconciled
```

It does not promise exactly-once worker execution or exactly-once external
calls. A controller restart MUST be safe while Runs are active; draining is an
operational optimization for planned Redis, worker-image, or incompatible
protocol changes, not a correctness requirement for a controller-only restart.

Temporary unavailability produces a persisted retry or waiting condition. It
does not weaken verification, review, credential, or publication rules. Human
intervention is allowed only by the exceptional policy in the [workflow
lifecycle](workflow-lifecycle.md).

## Architectural invariants

The formal expressions and canonical identifiers live in the [domain
model](domain-model.md). Architecturally:

- one project and Work Item has at most one active Run;
- every durable unit and receipt binds to immutable identities;
- dispatch follows, never precedes, durable planning;
- Redis is reconstructible;
- admitted Candidate boundaries are durable while active invocations are
  retryable;
- exact-Candidate gates are invalidated by a new Candidate;
- the reducer is the sole transition authority;
- unavailable capacity never weakens policy;
- workers and candidate code have least privilege;
- every model-backed invocation has one accepted one-shot Launch Attestation
  before provider material is released, while the attester has no result or
  workflow authority;
- external mutations are fenced and reconciled;
- one engine owns a run-associated Change Request at a time;
- secret values remain confined to the Secret Store and scoped in-memory use;
  and
- `needs-human` requires a controller-issued allowlisted boundary reason.

## Evidence and migration

### Current evidence retained

- `docs/wiki/current-orchestrator-state-model.md` and
  `src/orcest/orchestrator/pr_ops.py` already treat PR head SHA plus the action
  predicate as a stale-work fence. v1 generalizes that discipline to Attempt
  generations, exact Candidates, and Forge Observations.
- `src/orcest/shared/coordination.py` uses owner-checked Redis locks and
  snapshot-bearing pending markers. v1 retains owner/fence validation but
  moves authoritative claims and deadlines into SQLite.
- `src/orcest/shared/config.py` already supports multiple projects in one
  orchestrator process and rejects duplicate repositories or Redis prefixes.
  This is evidence that central multi-project iteration exists even though the
  deployed topology is still per-project.
- `src/orcest/fleet/pool_manager.py` detects reaped tasks and emits a transient
  failure. v1 retains autonomous lost-worker recovery while moving the report
  behind the authenticated controller API.
- `src/orcest/monitor/db.py` demonstrates local SQLite WAL use and idempotent
  inserts. The workflow database is separate and adds `synchronous=FULL`,
  migrations, durable deadlines, and transactional scheduling.
- `src/orcest/orchestrator/issue_deps.py` already excludes comments from body
  dependency parsing and treats unknown dependency state as blocking. v1 keeps
  the fail-closed dependency behavior.

### Behavior replaced

- `README.md` and `src/orcest/fleet/deploy/docker-compose.yml` describe one
  orchestrator container per project, while v1 requires one central controller
  across registered projects.
- `src/orcest/orchestrator/issue_ops.py` discovers only `orcest:ready` and uses
  Redis attempts, pending markers, and locks as the active-work picture. v1
  admits a durable Run first and reconciles both `ready` and `working`
  projections.
- `src/orcest/shared/models.py` serializes raw forge and provider credentials in
  `Task`; `src/orcest/worker/_runner_base.py` passes the forge token to the
  agent environment. v1 replaces this with Secret References, post-claim
  scoped material, and read-only source access.
- `src/orcest/orchestrator/task_publisher.py` tells an issue worker to create a
  branch, push it, and open a PR. v1 workers upload a Candidate and only the
  controller publishes.
- `src/orcest/fleet/pool_manager.py` currently writes a synthetic failed
  `TaskResult` to a project's Redis results stream after a reap. v1 sends an
  authenticated lifecycle observation to the central controller.
- `src/orcest/shared/credential_handoff.py` stores recoverable rotated secret
  material in private Redis keys. v1 moves durable credential versions into
  the controller-owned Secret Store and leaves only opaque references in
  Redis.

### Deliberately deferred rollout and implementation validation

These experiments are not prerequisites for reviewing the normative
architecture. Production enablement deliberately defers until repository
evidence demonstrates these implementation and rollout gates:

1. No current deployment unit mounts local durable workflow, Candidate Store,
   and Secret Store paths into one central controller. The rollout spec must
   select paths, ownership, backup boundaries, and migration from generated
   per-project Compose stacks.
2. Pool-manager callbacks are Redis `TaskResult` writes today. The worker and
   pool-manager API needs authentication, replay protection, and routing by
   Run/Activity/Attempt identity.
3. There is no Candidate bundle upload or admission implementation. The worker
   protocol must specify limits and the persistence spec must prove fsync and
   orphan cleanup ordering.
4. `src/orcest/orchestrator/gh.py` can inspect and mutate existing PRs but does
   not create a controller-owned PR with a deterministic Run marker. The forge
   adapter needs create/find/reconcile and compare-and-swap update operations.
5. There is no controller Secret Store. Existing fleet configuration and
   Redis-based rotation paths contain raw values and require a staged migration
   that cannot strand a live credential reference.
6. Current worker clone authentication is a write-capable token supplied as
   `GITHUB_TOKEN`. The GitHub implementation must prove a read-only clone
   credential while preserving private-repository access and preventing the
   agent CLI from recovering a controller write token.
