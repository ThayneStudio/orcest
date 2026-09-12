# Persistence and Recovery

> **Status:** Accepted normative v1 specification (2026-08-27)
>
> **Canonical owner:** SQLite and filesystem durability, transactional outbox,
> Redis reconstruction, Secret Store rotation, backup/restore, and garbage
> collection.

This page defines the durable workflow store, the disposable Redis projection,
candidate-artifact and secret durability, and recovery after partial failure.
It does not redefine the identities in the [domain model](domain-model.md),
lifecycle policy, [worker payloads](worker-protocol.md), consensus rules, or
forge-side publication semantics. Those pages consume the normative atomicity
and recovery contracts defined here.

## Durability contract

Orcest v1 has four storage classes:

| Storage | Owns | Authority |
| --- | --- | --- |
| SQLite | Runs, snapshots, Activities, Attempts, receipts, transitions, Publications, durable deadlines, and delivery intent | Authoritative for the Orcest-owned workflow lifecycle |
| Candidate store | Validated, immutable Git bundles addressed by SHA-256 | Authoritative for admitted pre-publication code artifacts |
| Secret Store | Immutable versions of forge and provider credentials | Authoritative for secret values and rotation history |
| Redis | Versioned delivery notifications, wake-ups, renewable liveness leases, heartbeats, and caches | Disposable and reconstructible |

The forge remains authoritative for Work Item intake and observed Change
Request, CI, review, and merge state. An external observation becomes a reducer
input only after Orcest persists it. Avoid describing either GitHub or SQLite
as an unqualified global source of truth.

The following guarantees are mandatory:

1. A worker Activity, its `OFFERED` Attempt, and its delivery intent commit in
   one SQLite transaction before any Redis publication.
2. Attempt `OFFERED`/`CLAIMED` state, generation, claimed worker/session,
   claim deadline, and execution deadline survive loss of the controller or
   Redis.
3. Execution is at least once, while result acceptance is at most once for the
   current fenced Activity generation.
4. Redis loss can cause duplicate notification or execution, but cannot erase,
   invent, accept, or permanently suppress durable workflow work.
5. An admitted Candidate is recoverable. An active invocation's uncommitted
   workspace is not; it is retried from the last durable boundary.
6. SQLite never references a Candidate or secret version that was not made
   durable first.
7. External side effects are recorded and reconciled, not assumed to share a
   transaction with SQLite.

Unless a field explicitly names a different opaque Secret Store attestation,
every SHA-256 identity in this page is lowercase
`sha256:<64 lowercase hexadecimal characters>` over its stated canonical or
domain-separated preimage. A bare hex string is not equivalent.

## Deployment and filesystem

The v1 controller MUST be the only workflow writer for every registered
project. HTTP handlers, schedulers, reconcilers, and pool-manager callbacks
MUST submit commands to that one logical writer; they MUST NOT open independent
read/write connections. Read-only query connections MAY be opened with SQLite
URI `mode=ro` and `PRAGMA query_only=ON`.

Every controller HTTP endpoint MUST be exposed and called only over HTTPS with
certificate validation, including Project registration, Run management,
storage restoration, secret provisioning/adoption, and every worker/pool call.
Plaintext HTTP is forbidden on private networks too. Secret-bearing multipart
or octet-stream handlers additionally bypass ordinary body logging, tracing,
error reflection, and general controller staging as their kind-specific
protocol requires.

The controller MUST mount one persistent local filesystem at
`/var/lib/orcest/control`. The normative layout is:

```text
/var/lib/orcest/control/                 mode 0700
├── controller.lock                     mode 0600
├── storage.lock                        mode 0600
├── workflow.db                         mode 0600
├── candidates/                         mode 0700
│   ├── objects/sha256/ab/<64-hex>.bundle
│   ├── incoming/
│   └── quarantine/
└── secrets/                            mode 0700
    ├── <secret-id>/versions/<version> mode 0600
    ├── incoming/
    └── quarantine/
```

The database, its `-wal` and `-shm` files, Candidate objects, and Secret Store
MUST reside on persistent local block storage. NFS, SMB, FUSE, the existing
TrueNAS trace mount, and other network filesystems are forbidden for this
state root. Candidate `incoming`, `objects`, and `quarantine` directories MUST
share a filesystem so publication can use an atomic rename. The controller
MUST hold a nonblocking exclusive OS file lock on `controller.lock` for its
entire writer lifetime; failure to acquire it MUST abort startup. The
controller MUST fail startup if it cannot verify a local filesystem, the owner
UID, the required modes, free space above the configured safety floor, or
write and fsync behavior.

Every Candidate or secret operation that can make bytes reachable or
unreachable MUST participate in one controller-owned storage mutation lock
backed by `storage.lock`. Candidate finalization and secret creation or
rotation hold it continuously from the final pre-write reference check,
through file installation and fsync, through the SQLite reference transaction.
Garbage collection holds the same lock from its reference snapshot through
quarantine rename and its audit transaction. The backup barrier holds it while
mutations are paused and the database snapshot plus exact referenced-file set
are fixed. Byte-upload staging that cannot create a live reference MAY occur
outside this lock. The singleton writer lock does not substitute for this
protocol: artifact validation and the authorized backup process may perform
filesystem I/O outside an ordinary SQLite write transaction.

The lock order is fixed: enter controller command serialization, acquire
`storage.lock` when the operation needs it, and only then begin the SQLite
transaction that creates, removes, or audits reachability. No code may wait for
`storage.lock` while holding an open SQLite transaction. Read-only validation
and unreferenced incoming-byte staging may precede the lock, but every
authority/current-reference check is repeated after acquiring it.

Only the controller service account and an explicitly authorized backup
process may read the state root. Candidate artifacts are untrusted code even
though they are not secrets. They MUST NOT be executed with forge-write,
controller-write, Secret Store, or unrelated-Run credentials.

## SQLite profile

Every read/write connection MUST set and verify:

```sql
PRAGMA journal_mode = WAL;
PRAGMA synchronous = FULL;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 5000;
PRAGMA trusted_schema = OFF;
PRAGMA wal_autocheckpoint = 1000;
```

Failure to obtain `wal`, `FULL`, or enabled foreign keys is a fatal startup
error. The controller MUST use `BEGIN IMMEDIATE` for every state-changing
command. A command either commits all reducer state, transition rows, Activity
plans, and outbox rows or commits none of them. Long-running Git validation,
forge calls, Redis calls, artifact copies, and model execution MUST occur
outside a SQLite transaction.

Schema migrations MUST:

- be monotonic and identified by `PRAGMA user_version` plus an applied-migration
  table;
- run before the controller accepts claims or dispatches work;
- run under an exclusive migration lock with no other writer;
- be transactional unless SQLite itself requires a documented two-phase
  migration; and
- leave the previous database backup intact until startup validation succeeds.

At startup the controller MUST run `PRAGMA quick_check`, run
`PRAGMA foreign_key_check`, and audit every live Candidate, every
Snapshot-referenced Workflow Blob, and every live Secret Reference. The
Workflow Blob audit recomputes its canonical media-kind/length/content digest;
a row merely being readable is insufficient. A failed `quick_check`,
unsupported schema, or broken relational invariant fails closed globally:
dispatch, receipt acceptance, and publication remain disabled.

There is one bounded startup-repair exception to a failing
`foreign_key_check`: `quick_check` MUST pass, and every violation MUST be only a
missing `WorkflowBlob` parent for a Snapshot configuration, effective-policy,
or prompt reference whose exact digest and required media kind remain frozen in
the child row/column. Workflow mutation remains globally disabled during this
exception. Under the exclusive writer and shared storage mutation lock, the
storage reconciler may install only those exact content-addressed rows from a
verified complete backup or an already accepted authenticated Restoration
Operation. The `FULL` repair transaction verifies bytes/media/digest, reruns
`quick_check`, `foreign_key_check`, and the complete Workflow Blob reference
audit, then inserts the normal Storage Restoration Fact/Health Observation/
frozen fanout before commit. Checks run again after commit before workflow
mutation is enabled. A violation involving any other parent/table, a child that
cannot freeze exact digest/media kind, an extra missing Blob, or a failed
post-repair check remains global fail-closed.

When relational integrity remains valid but a Candidate object, an existing
Snapshot-referenced Workflow Blob row's bytes, or Secret version is missing or
corrupt, the controller MUST instead persist the exact integrity fact, suspend
only the referencing Runs and affected Project/credential scope, and attempt
the per-object restoration protocol below before those Runs can dispatch. It
MUST still block every operation that could consume the missing object.
Unaffected Projects and Runs MAY continue. Full `integrity_check` belongs in
backup and restore drills rather than every ordinary restart.

WAL checkpoints are maintenance, not commits. The controller SHOULD run a
passive checkpoint periodically and a truncating checkpoint only during a
backup barrier or controlled maintenance. WAL size, checkpoint age, busy
responses, transaction latency, and filesystem free space MUST be observable.

## Schema persistence boundaries

The domain model owns complete field definitions and lifecycle enumerations.
The physical schema MUST nevertheless enforce the following keys and
relationships. Structured JSON MAY retain a normalized policy or provider
payload, but it MUST NOT replace relational identity, generation, deadline,
digest, state, or foreign-key columns.

Controller-generated object IDs use lowercase canonical UUIDv4. Persistent
timestamps and deadlines use integer Unix milliseconds in UTC. Git object
identities use `{object_format, oid}` and MUST support SHA-1 without assuming
that every repository uses it. Ordering uses durable integer sequences rather
than UUID or timestamp order.

| Table | Required key and persistence constraints |
| --- | --- |
| `controller_mode` | Singleton `ORCEST_V1`; revision-0/null bootstrap or positive initialized CAS revision; closed durable operating mode; conditional dispatch-paused intake policy; conditional maintenance-prior projection; exact last successful Controller Mode Operation |
| `controller_mode_operations` | Caller UUID; exact authenticated idempotent `orcest.controller-mode-operation/1` `INITIALIZE`/`SET_MODE`/`RESTORE_BACKUP` request and CAS; conditional verified-backup projection; terminal success/rejection, resulting projection, and exact stored response |
| `forge_instances` | UUID `forge_instance_id`; unique canonical origin; code-owned adapter kind; logical credential Secret ID plus positive registration-provenance version; each external operation freezes the then-current exact Secret Reference |
| `projects` | UUID `project_id`; unique forge/repository identity and immutable non-secret installation/account registration; trusted `default_ref`; positive monotonic registration revision plus exact successful registration Operation and reciprocal Project discovery Schedule; versioned server-owned trusted-base, budget-policy, and budget-reset-window references; `ACTIVE`/`SUSPENDED`/`REMOVED` registration state; immutable logical source-read/publication Secret IDs plus registration-provenance versions copied from that Operation; live versions are frozen only by each Claim/Publication Effect |
| `project_registration_operations` | Controller UUID; exact authenticated principal/caller idempotency identity; immutable `orcest.project-registration/1` request/auth decision and resolved forge/repository/base evidence; internal exact resolved forge-API/source-read/publication Secret References, successful discovery Schedule identity, and separate internal resolution digest; terminal success/rejection, conditional Project/revision/Schedule or rejection code, canonical public response HTTP/body/digest/time; durable replay ledger |
| `workflow_blobs` | Domain-separated canonical `sha256:<hex>` `blob_digest` primary key over media kind, byte length, and normalized bytes; `media_kind` (`CONFIG_JSON`, `PROMPT_UTF8`, `POLICY_JSON`, or `SERVER_POLICY_JSON`), exact byte length, and bounded canonical normalized bytes stored in SQLite; immutable and content-addressed |
| `policy_updates` | UUID update ID; unique per-Project sequence and authenticated `SERVER_ROLLOUT` UUID source; exact server policy revision/blob plus trusted-base, budget-policy, and reset-window references; registered rollout-service principal/authorization; immutable ordered server input |
| `policy_update_compositions` | Immutable `(policy_update_id, run_id)` fan-out input freezing the exact latest accepted `WORK_ITEM_SNAPSHOT` and trusted `BASE_HEAD` Forge Observations visible when the Policy Update transaction commits; prevents either input from changing during replay |
| `budget_reports` | Caller UUID global replay identity; exact Project/accounting scope, installed budget/reset policy refs, window and cumulative integer limit/consumption, positive source sequence/revision, controller-derived availability, authenticated reporting principal, frozen affected-Run digest/cursor/completion, report digest, and exact stored response |
| `budget_report_runs` | Zero-based bytewise-Run-ID ordered membership frozen for an `AVAILABLE` Report; unique Report/Run pair and exact rows reproduce the Report wake-fanout digest |
| `work_item_snapshots` | UUID `snapshot_id`; unique `(run_id, snapshot_sequence)` immutable capture; exact Forge Observation or Policy Update source plus exact composed Work Item and base Observation IDs; forge/repository identity; base commit; specification/workflow/policy hashes and policy-specific `supersession_key`; reducer and server policy versions; registration-policy references; normalized workflow, prompt, and effective-policy blob references |
| `snapshot_prompt_blobs` | `(snapshot_id, path)`; path-sorted prompt membership with exact trusted Git blob identity and foreign key to its `PROMPT_UTF8` Workflow Blob |
| `snapshot_generations` | `(run_id, specification_generation)` primary key; unique installed `snapshot_id` and exact installing Transition; pending Snapshot captures have no row |
| `runs` | UUID `run_id`; project and Work Item identity; current specification generation and Snapshot; pending Snapshot plus exact supersede-request flag/capture Transition; nullable current Candidate plus policy-only prior-generation Candidate context; lifecycle state; closed recovery-origin/entry/resume pointers; current Recovery Evidence, Wait Condition, Human Boundary, durable pending-dependency observation/set/Transition pointer, coalesced current-panel staffing continuation, and immutable accepted cancellation source kind/ID while cleanup is pending; terminal outcome; partial unique index allowing at most one active Run per project and Work Item |
| `activities` | UUID `activity_id`; unique `(run_id, activity_ordinal)` and `(run_id, idempotency_key)`; immutable specification generation/policy hash, kind, execution class, normalized semantic input digest, exact Candidate/causal Forge/separate Change Request head Observation and head bindings, role, repair/recovery cycles, strategy/tactic/evidence, rescue epoch, and creating Transition; lifecycle state and current Attempt generation projection |
| `activity_review_assignments` | `activity_id` primary key and exact `REVIEW`/`ADJUDICATE` `assignment_kind` tagged union; positive panel round, conditional reviewer/adjudicator slot and adjudication round, immutable role/context, ordered-subject membership digest, disputed-Finding membership digest, and complete assignment digest; created atomically with its Activity |
| `activity_review_subjects` | Nonempty zero-based contiguous ordered `(activity_id, subject_ordinal)` membership; unique normalized subject reference per review/adjudication Activity; exact rows reproduce the Assignment's subject digest |
| `activity_adjudication_findings` | Zero-based contiguous ordered `(activity_id, finding_ordinal)` membership; unique Finding ID per adjudication Activity; exact rows reproduce the assignment's disputed-Finding digest |
| `attempts` | UUID `attempt_id` primary key plus unique `(activity_id, generation)` fence; state is `OFFERED`, `CLAIMED`, `SUCCEEDED`, `FAILED`, `ABSTAINED`, `EXPIRED`, or `SUPERSEDED`; immutable `execution_profile_id`, resolved worker/provider/model/non-secret `provider_account_ref` selection, canonical server-derived `provider_family`/`model_family`, and Snapshot-policy-pinned `classification_revision`, plus the exact provider Secret Reference frozen only by Claim; claim deadline; exact Attempt Claim pointer and authenticated worker/session; execution deadline plus literal-v1 capability authentication expiry and capability claims digest/JTI; one-shot launch nonce/capability and accepted Launch Attestation for model-backed work; terminal reason; generation is positive and never reused |
| `capability_key_registry` | Singleton `ORCEST_V1`; nonnegative monotonic registry revision with exact revision-0/null-key/null-operation bootstrap; nullable current issuance key after initial registration or during fail-closed pause; exact last successful Capability Key Operation pointer after revision 0 |
| `capability_key_operations` | Caller UUID; exact authenticated idempotent `orcest.capability-key-operation/1` `REGISTER`/`SELECT`/`RETIRE`/`REVOKE` request, expected registry/current-key CAS, target/replacement and conditional registration material, terminal success/rejection, resulting registry/current key, and exact stored response |
| `capability_signing_keys` | UUID public `kid`; immutable `ED25519` algorithm, canonical public verification key and domain-separated digest, matching private-signing Secret Reference, not-before time, monotonic `ACTIVE`/`RETIRED`/`REVOKED` state, and authenticated state-change evidence; verifier retention is reference- and cryptographic-expiry-bound |
| `attempt_claims` | Caller claim UUID; exact `orcest.attempt-claim/1` Attempt/Activity/generation/offer-outbox and worker/session/profile/build request; canonical request digest; immutable claim/execution/auth-expiry times; Attempt capability JTI/claims digest and signing-key/algorithm identity; conditional launch nonce/JTI/domain-separated claims digest and signing-key/algorithm identity; exact source access kind and frozen source-read/provider Secret References plus access descriptor; stable non-secret response-contract digest; no bearer or raw secret |
| `launch_attestations` | Caller UUID replay identity; exact Attempt/session/nonce; exact launch-capability signing-key/algorithm identity; registered pool/runner principal, image, registration revision, distinct runner signing key/algorithm/signature; globally fresh workspace/context/invocation UUIDs with null parents and true freshness flags; complete attestation digest/times; immutable signed launch-isolation evidence |
| `attempt_terminal_facts` | UUID fact/reducer-trigger ID; exact Attempt fence; `CLAIM_DEADLINE`/`EXECUTION_DEADLINE`/`WORKER_LOST`/audit-only `RESULT_AFTER_TERMINAL`; closed persisted source identity; deadline proof or exact loss Health Observation; for claim deadline, frozen capacity classification/disposition, current logical provider Secret version, ordered Health membership digest, and Controller Mode/Capability Registry projections; replacement-offer disposition is evidence-only and is consumed only by a later Recovery Evidence transition; immutable fact digest/time |
| `attempt_terminal_fact_health_observations` | Zero-based contiguous ordered Health Observation membership frozen only for `CLAIM_DEADLINE`; at most the highest applicable unexpired fact per policy scope; exact rows reproduce the Terminal Fact capacity digest |
| `result_requests` | Caller Result `idempotency_key` global primary identity; exact Attempt/session/capability signer and complete canonical Result-body digest; closed `ACCEPTED`/`UPLOAD_EXPIRED`/`STALE_ATTEMPT`/`EXPIRED_CURRENT`/`ALREADY_TERMINAL` disposition with conditional Result/upload/stale-fence/Terminal-Fact bindings, deadline proof, and exact stored response; the sole Result-endpoint replay registry |
| `attempt_results` | Unique `attempt_id` and `(activity_id, generation)`; exact accepted Launch Attestation for model-backed work; required canonical `result_digest`, schema version, outcome (`SUCCEEDED`, `FAILED_RETRYABLE`, `FAILED_PERMANENT`, or `ABSTAINED`), Candidate/receipt reference, closed normalized failure fields, bounded normalized non-secret `structured_output` and its digest, and acceptance time; exactly one accepted result per Attempt |
| `credential_rotation_requests` | Worker request UUID; exact current Attempt/session/capability/Launch/account/Secret-prior authority and opaque Secret Store keyed body attestation; `APPLIED`/`CAS_LOST` disposition; conditional Receipt/new version; observed current version; stored closed response HTTP/body/digest/time; immutable request/replay ledger with no secret bytes or unkeyed body digest |
| `outbox` | UUID `outbox_id`; closed Activity, Health Probe Request, Forge Observation Request, Secret Provision Operation, or Terminal Duplicate Cleanup Action source identity; destination; nullable Attempt ID/generation; nullable exact `publication_id`/`effect_generation` for Activity publication effects or terminal cleanup; protocol version, payload digest, non-secret normalized payload, next-delivery time, state, delivery count, and last Redis epoch/entry observed |
| `projection_outbox` | UUID intent ID; exact Run/Transition source; projection kind and stable external target; nullable Publication Effect binding; canonical payload digest and stable idempotency key; `PENDING`/`DELIVERED`/`SUPERSEDED` state and retry metadata; never lifecycle authority |
| `candidate_uploads` | UUID `upload_id`; exact Attempt ID/generation; unique request idempotency key; incoming path; expected and computed size/digest/tip; nullable promoted Artifact Object/storage key and promotion time; nullable consumed Candidate; `RECEIVING`/`VALIDATED`/`PROMOTED`/`CONSUMED`/`EXPIRED` state; expiry; never Candidate authority by itself |
| `artifact_objects` | Canonical `sha256:<hex>` `bundle_digest` primary key; relative storage key, byte length, and durable-install time; storage keys are unique and remain below the Candidate root; inventory alone is not Candidate authority |
| `candidates` | UUID `candidate_id`; per-Run Candidate generation; `WORKER_ATTEMPT` or `FORGE_IMPORT` provenance; required producing Activity; exclusive worker Attempt/generation or import Forge Observation binding; exact base and verified tip `{object_format, oid}`; foreign key to `artifact_objects`; at most one Candidate created per producing Activity |
| `verification_receipts` | UUID receipt ID; exact Candidate ID/commit; unique producing Attempt; profile binding; structured outcome and evidence digest |
| `review_receipts` | UUID receipt ID; exact Candidate ID/commit; unique producing Attempt and copied execution/family/classification-revision/Launch-Attestation provenance; exact assignment panel/slot/role/context and subject-membership-digest binding; bounded canonical ordered assessments for every frozen subject; `APPROVE`/`BLOCK`/`ABSTAIN` verdict, conditional `abstention_code`, structured findings, receipt digest, and controller-derived `fills_slot` |
| `adjudication_receipts` | UUID receipt ID; exact Candidate/commit and unique producing Attempt with copied execution/family/classification-revision/Launch-Attestation provenance; exact assignment panel/adjudication/default-slot/context, subject-membership digest, and disputed-Finding binding; conditional `abstention_code`; exactly one disposition per finding when non-abstaining; evidence references, bounded `new_findings`, receipt digest, and controller-derived `fills_slot` |
| `consensus_decisions` | UUID decision ID; exact Candidate ID/commit and panel round; policy hash; `APPROVED`/`REMEDIATE`/`ADJUDICATE` aggregate; unresolved finding IDs and digest; exactly one immutable Decision per `(candidate_id, panel_round)` |
| `consensus_decision_inputs` | Ordered `(consensus_decision_id, input_ordinal)` membership containing only applicable `default` Verification and frozen Review Receipt IDs; Adjudication Receipts are forbidden |
| `transitions` | `(run_id, transition_sequence)` primary key; immutable prior state, trigger identity, conditional exact ADMIT base-observation anchor, next state, reducer version, and persisted input digest; sequence strictly increases within a Run |
| `publications` | UUID `publication_id`; unique `run_id`; approved `candidate_id`; positive monotonic current `effect_generation`; deterministic branch and Run marker; expected and observed remote commit; exact initial complete-marker search revision/full-set digest/live-cardinality/retained-ID projection plus conditional deterministic terminal-member selection proof and reciprocal Terminal Duplicate Cleanup Reservation pointer; state and reconciliation timestamps; nullable-together last duplicate-reconciliation Fact/search-revision/set-digest projection; one Publication per Run |
| `publication_effects` | Immutable `(publication_id, effect_generation)` intent; exact `PUBLISH` Activity; `INITIAL` or `UPDATE` mode; desired Candidate/commit; expected remote commit or explicit nonexistence; exact frozen publication Secret Reference; base ref/commit and frozen base-movement policy; canonical operation digest; creating Transition and time |
| `publication_effect_checkpoints` | UUID checkpoint ID; immutable per-effect sequence; closed suboperation/status including the initial complete-marker search gate; stable request idempotency key when applicable; exact Forge Observation/external revision evidence; checkpoint digest and recorded time |
| `reconciliation_facts` | UUID fact/reducer-trigger ID; unique controller `RECONCILE` Activity; nullable exact Publication Effect; closed result kind including positive redundant proof or `NO_ACTIONABLE_DUPLICATE`; observed ref commit, ownership evidence, pinned-base relationship, safe-fetch/admission-or-validation proof, conditional deterministic retained-Change-Request identity/head/observation, duplicate-search revision/set digest for either duplicate outcome, additional cleanup-member digest only for positive redundant proof, fact digest, and recorded time |
| `reconciliation_fact_observations` | Ordered non-empty membership `(reconciliation_fact_id, observation_ordinal)` containing the canonically sorted exact Forge Observations examined by the controller |
| `reconciliation_duplicate_members` | Frozen zero-based ordered member proof for one `REDUNDANT_PUBLICATIONS_PROVEN` Fact; exactly one retained lowest stable Change Request ID followed by one or more close members, each with exact head, identity/unreviewed observations, revision, and equivalence digest |
| `controller_operation_facts` | UUID fact/reducer-trigger ID; unique controller Activity; `IMPORT` success/failure, definitive controller failure, or successful `CLOSE_PUBLICATION` absence proof; pre-I/O operation digest, kind-specific Candidate/Forge output bindings or failure evidence, fact digest, and record time; successful `PUBLISH`/`RECONCILE`, linked cancellation close, and successful redundant close use their more-specific facts |
| `controller_operation_fact_observations` | Ordered membership of exact Forge Observations bound to a Controller Operation Fact |
| `forge_observation_schedules` | UUID durable cadence; closed project-discovery/poll/search kind; exact Project/Forge authority and Project/Work Item/Publication target; optional Run/Publication and post-terminal cleanup Reservation; conditional latest discovery search revision/semantic-set digest; positive server interval, due time, monotonic CAS revision, optional latest Request, `ACTIVE`/`PAUSED`/`CLOSED` state, and schedule digest |
| `forge_observation_requests` | UUID durable pre-I/O retry identity; exact schedule revision/sequence, copied authority/target/optional cleanup-Reservation bindings, and creating non-maintenance Controller Mode revision/projection; tagged Project-source or Publication-effect credential purpose plus exact frozen Secret Reference; conditional exclusive controller Activity/effect/operation or terminal-cleanup Action/operation readback fence; ordinary prior observation/external-revision fence or discovery search/set fence; reciprocal Outbox; stable adapter idempotency key/request digest; positive next outbound-attempt ordinal plus nullable latest transient-failure/retry projection; `PENDING`/`COMPLETED`/`SUPERSEDED` projection and conditional ordered-result/discovery-set digests/times |
| `forge_request_failure_facts` | UUID immutable failed read/search/poll transport-attempt Fact; unique Request/positive pre-I/O attempt ordinal; exact copied Request scope/digest; closed transient failure kind/code/evidence, deterministic retry boundary, Fact digest/time; reducer trigger only when Run-bound |
| `forge_observation_request_observations` | Zero-based contiguous ordered membership of every Forge Observation returned by a completed Request; Observation IDs are unique within one Request but may reuse an exactly coalesced prior row under the closed delivery-ID/adjacency rules; ordered membership reproduces its result digest |
| `forge_observations` | UUID observation ID; `project_id`; `target_kind` (`WORK_ITEM` or `PUBLICATION`) and opaque `target_id`; nullable Run/Publication, `publication_effect_generation`, and post-terminal cleanup Reservation references; conditional creating Forge Observation Request plus copied credential union; typed complete-search/marker snapshots; `CHANGE_REQUEST_SEARCH_RESULT` live cardinality and digest of both ordered live and terminal memberships; mutually exclusive nullable-together controller Activity/operation or terminal-cleanup Action/operation identity for an authenticated mutation result; terminal cleanup is exactly either Request-backed with a non-null Request and copied credentials or direct mutation with a NULL creating Request and both credential fields NULL, with exact Reservation/Effect/Action/operation/Outbox reciprocals; adapter event identity; external revision; payload digest; sequence unique and monotonic per `(project_id, target_kind, target_id)` |
| `change_request_search_members` | Immutable child rows for `CHANGE_REQUEST_SEARCH_RESULT`, independently contiguous and bytewise stable-ID ordered within `LIVE` and `TERMINAL`; exact head and body/marker evidence, conditional terminal state/merge commit, source ref/Run marker, closed ownership status/proof kind plus exact create/Effect/creator/ref/marker/commit evidence/defects and digest, external-reliance digest, and member digest; stable ID unique across both classes per Observation |
| `terminal_duplicate_cleanup_reservations` | UUID post-terminal cleanup owner; exact Project/Run/Publication, selected positive merged member/search/provenance, frozen complete-search pair, ordered member count/digest, `ACTIVE`/`COMPLETED` state, restart cursor, reciprocal Publication pointer, and creating terminal Transition/times |
| `terminal_duplicate_cleanup_members` | Complete bytewise stable-ID ordered copy of every LIVE member from the selected merged search; exact head/body/marker CAS and ownership/reliance proofs; deterministic `CLOSE`/`DETACH_MARKER`/`RECORD_ONLY` action |
| `terminal_duplicate_cleanup_actions` | Per-member positive retry generation; copied action, conditional record reason or exact CAS/idempotency/outbox mutation identity, `PENDING`/`ACTIVE`/`COMPLETED`/`SUPERSEDED` projection, closed outcome and optional exact Forge Observation; immutable `action_input_digest`, conditional terminal `result_digest`, and completion time |
| `observation_counters` | Primary key `(project_id, target_kind, target_id)`; next positive Forge Observation sequence for targets that may exist before a Run |
| `wait_conditions` | UUID `wait_condition_id`; owning Run; typed reason/resume state; exact specification/Candidate/policy/Forge bindings; persistent `not_before_ms` and/or typed wake kind/identity; immutable ordered Health and panel-slot membership digests; exact persisted creation source; condition digest and creating Transition; immutable history, current only through `runs.wait_condition_id` |
| `wait_condition_health_observations` | Zero-based contiguous, canonical ordered Health Observation membership actually consulted when the Wait was selected; exact rows reproduce its mandatory digest |
| `wait_condition_panel_slots` | Zero-based contiguous complete membership of every unfilled frozen REVIEW or sole ADJUDICATE slot for a panel-scoped CAPACITY Wait; exact Activity/assignment/round/slot identities reproduce its digest |
| `timer_facts` | UUID timer fact/reducer-trigger ID; nullable Run; exact closed Wait, Health, Budget Report, Attempt, or Recovery deadline scope/ID; copied deadline, controller fire time, scheduled/startup scan-pass source, mandatory affected-Run membership digest, fact digest, and record time; immutable and scope/deadline-unique |
| `timer_fact_runs` | Canonically ordered frozen `(timer_fact_id, run_ordinal, run_id)` membership used only by global Health-expiry Timer Facts; exact replay-safe fan-out input |
| `recovery_evidence` | UUID `recovery_evidence_id`; unique per-Run recovery sequence and source identity; exact Activity/Attempt/specification/Candidate/Forge bindings; category and failure fingerprint; closed selected tactic plus post-application strategy/counters/fallback/eligibility; evidence digest; append-only |
| `recovery_evidence_health_observations` | Canonically ordered `(recovery_evidence_id, observation_ordinal)` membership containing at most the highest applicable unexpired Health Observation per policy-relevant scope used for fallback/capacity selection; exact IDs reproduce a mandatory digest |
| `capacity_reports` | Controller UUID `capacity_report_id`; unique authenticated pool-manager `report_id` plus request idempotency key; exact `orcest.capacity-report/1` protocol, strictly increasing report revision, bounded normalized scope/slot/session body and digest, principal/authorization, frozen controller acceptance time and positive configured maximum TTL, bounded expiry, canonical response/digest; immutable request/replay ledger |
| `capacity_report_observations` | Ordered `(capacity_report_id, observation_ordinal)` membership naming each Health Observation atomically accepted from that report |
| `worker_loss_reports` | Authenticated report/idempotency identity; exact worker/session/Attempt fence, closed reason, bounded normalized body, principal/authorization, `ACCEPTED`/`STALE` outcome with conditional LOST Health Observation and Attempt Terminal Fact, canonical response/digest, and acceptance time; immutable request/replay ledger |
| `health_probe_facts` | UUID source ID; closed controller-owned forge/provider/storage/Secret probe kind, per-scope probe sequence, exact canonical subject bindings/scope/outcome/conditional object and integrity-failure target, bounded timing/TTL and immutable implementation/input/evidence/complete-fact digests, plus its exact atomically created Health Observation, frozen affected-Run digest, and mutable next-member fanout cursor/completion time |
| `health_probe_fact_runs` | Canonically byte-sorted zero-based contiguous `(health_probe_fact_id, run_ordinal, run_id)` membership frozen with the Fact/Observation; exact replay-safe fanout input whose processed prefix is owned by the Fact cursor |
| `health_probe_requests` | UUID durable pre-I/O intent; exact probe/canonical subject bindings/scope/conditional object/provider Secret version and implementation/input; expected prior per-scope Health sequence; bounded request window; `PENDING`/`COMPLETED`/`SUPERSEDED` projection; exact outbox and conditional reciprocal Fact pointer |
| `health_observations` | UUID `health_observation_id`; closed scope kind/ID and monotonic per-scope sequence; typed health fact and exact capacity-report/worker-loss/storage-restoration/Health-Probe-Fact source; capacity or probe subject bindings, optional observed revision and expiry, bounded non-secret payload digest; immutable |
| `storage_restoration_operations` | Caller UUID operation ID; exact bounded multipart `orcest.storage-management/1` request with object/byte length, conditional Candidate/Blob digest, conditional exact Workflow Blob media kind, and durable quarantined upload reference; authenticated principal/authorization and metadata digests; accepted time; deterministic `PENDING` 202 projection or stored `RESTORED`/`REJECTED` HTTP/body/digest; conditional resulting Fact or rejection; opaque Secret Store attestation reference for Secret replay; immutable request/idempotency fields, never raw bytes or a Secret tag |
| `storage_restoration_facts` | UUID restoration fact/reducer-trigger ID; exact shared Candidate Artifact, Secret Version, or Workflow Blob identity; conditional equal Candidate SHA-256/domain-separated Workflow Blob digest plus media/length, or opaque Secret integrity attestation; verified backup or authenticated operation source; required paired `RECOVERED` Health Observation; conditional manifest or principal/authorization proof; verification digest and record time; immutable source-unique fact applied independently to frozen affected Runs |
| `storage_restoration_fact_runs` | Canonically ordered frozen `(storage_restoration_fact_id, run_ordinal, run_id)` membership of active Runs affected when restoration commits; exact replay-safe fan-out input |
| `secret_version_runs` | Canonically ordered frozen `(secret_id, version, run_ordinal, run_id)` membership of active exact Secret Waits/Boundaries satisfied when a new version becomes current; digest-bound replay-safe fanout |
| `secret_version_fanouts` | One durable restartable fanout intent per Secret Version; next member ordinal, `PENDING`/`DELIVERED` projection, and update time; membership and per-Run Transitions remain authority |
| `management_commands` | Caller UUID `command_id` global idempotency key; exact `orcest.management/1` protocol, Run and expected Transition fence; `CANCEL` or `RESOLVE_HUMAN_BOUNDARY` closed payload; authenticated principal/authorization digest; result Transition and optional Human Resolution; exact stored `200` response body/digest; immutable accepted command only |
| `human_boundaries` | UUID `human_boundary_id`; owning Run; allowlisted reason and exact Snapshot/Candidate/policy/Forge/Publication-effect bindings; conditional exact ownership Project/ref/Change Request/Run-marker bindings; resume state; bounded minimum request, evidence, attempted strategies, allowed choices/resolution kinds, packet digest, and creating Transition; immutable history |
| `human_resolutions` | UUID `human_resolution_id`; unique Human Boundary and source-derived stable-text idempotency key exactly equal to `source_id`; exact persisted source ID plus authenticated principal, allowed resolution kind and bounded secret-free structured resolution; copied exact boundary bindings and digest; at most one accepted resolution per boundary |
| `secret_refs` | UUID `secret_id`; purpose, owner scope, current positive version, and rotation time; no secret value |
| `secret_versions` | `(secret_id, version)`; exact immutable creation Receipt; durable path whose exact bytes have mandatory controller-only Secret Store integrity metadata; mandatory frozen affected-Run membership digest; no secret value, keyed tag, or unkeyed secret-derived digest in SQLite; foreign key to `secret_refs` |
| `secret_provision_operations` | Caller UUID idempotency/source ID; exact `orcest.secret-provision/1` `PROVISION`/`ADOPT_EXISTING` request; Secret/prior-version/immutable-target version, purpose/owner/account scope, authenticated principal/RBAC, opaque Secret Store staging/integrity proof; monotonic `PENDING`/`COMPLETED`/`REJECTED` projection with conditional resulting Receipt/version or terminal rejection and latest checkpoint; canonical terminal response/digest; request digest/time; immutable request and authority fields |
| `secret_provision_checkpoints` | UUID checkpoint ID; exact Secret Provision Operation and increasing sequence; closed verification/install phase and success/retryable/terminal outcome; conditional bounded non-secret failure evidence and next retry; digest/time; append-only |
| `credential_rotation_receipts` | UUID receipt ID; unique `ATTEMPT_ROTATION` request identity or authenticated `MANAGEMENT_PROVISION` operation; exact Secret/prior/new version, provider-account scope, tagged source authority, opaque integrity attestation, digest/time; immutable provenance for every Secret Version with no secret value |
| `controller_state` | Singleton `ORCEST_V1`; positive schema/reducer/compatibility versions; nonnegative monotonically increasing Redis reconstruction epoch and initialization/update times; controller operating mode is owned by its separate durable mode projection/operation ledger |

All foreign keys MUST use explicit deletion behavior. Domain history uses
`RESTRICT`; it MUST NOT disappear through a cascading project or Run deletion.
Receipt, Candidate, Attempt, Activity, transition, and Publication identity
columns are immutable after insertion. Terminal cleanup is an explicit audited
operation, not `ON DELETE CASCADE`.

Schema notation has a normative default: every `FOREIGN KEY` declaration in
this page that does not contain an inline `ON DELETE` clause means
`ON DELETE NO ACTION` (equivalently, an implementation MAY use `RESTRICT` when
it has the same immediate/deferred behavior). Any deletion exception MUST be
written inline on that declaration. This default applies to shorthand
declarations and to composite foreign keys as well; prose cannot silently
introduce a cascade or a set-null action.

The following database constraints or equivalent guarded transactions are
required:

```text
PRIMARY KEY ControllerMode(controller_id)
CHECK ControllerMode.controller_id = 'ORCEST_V1'
CHECK ControllerMode.mode_revision >= 0
CHECK ControllerMode bootstrap/initialized union:
  mode_revision = 0 => mode, dispatch_paused_intake_policy,
                       maintenance_prior_mode,
                       maintenance_prior_dispatch_paused_intake_policy,
                       last_operation_id IS NULL
  mode_revision > 0 => mode IN (
                         'RUNNING', 'INTAKE_PAUSED', 'DISPATCH_PAUSED',
                         'DRAINING', 'MAINTENANCE'
                       ) AND last_operation_id IS NOT NULL
CHECK ControllerMode.dispatch_paused_intake_policy IN (
  'ALLOW_ADMISSION', 'PAUSE_ADMISSION'
) exactly when mode = 'DISPATCH_PAUSED'; otherwise it is NULL
CHECK ControllerMode maintenance-prior union:
  mode != 'MAINTENANCE' OR mode IS NULL
    => maintenance_prior_mode,
       maintenance_prior_dispatch_paused_intake_policy IS NULL
  mode = 'MAINTENANCE'
    => maintenance_prior_mode IS NULL OR maintenance_prior_mode IN (
         'RUNNING', 'INTAKE_PAUSED', 'DISPATCH_PAUSED', 'DRAINING'
       ); maintenance_prior_dispatch_paused_intake_policy is non-null in the
       closed intake-policy enum exactly when maintenance_prior_mode =
       'DISPATCH_PAUSED', otherwise NULL
FOREIGN KEY ControllerMode.last_operation_id
  -> ControllerModeOperation(controller_mode_operation_id)
  DEFERRABLE INITIALLY DEFERRED
PRIMARY KEY ControllerModeOperation(controller_mode_operation_id)
CHECK ControllerModeOperation.protocol_version =
  'orcest.controller-mode-operation/1'
CHECK ControllerModeOperation.operation_kind IN (
  'INITIALIZE', 'SET_MODE', 'RESTORE_BACKUP'
)
CHECK ControllerModeOperation.expected_mode_revision >= 0
CHECK ControllerModeOperation.expected_mode and requested_mode are each NULL
  or in ('RUNNING', 'INTAKE_PAUSED', 'DISPATCH_PAUSED', 'DRAINING',
         'MAINTENANCE')
CHECK ControllerModeOperation operation-kind union:
  INITIALIZE => expected_mode_revision = 0 AND expected_mode IS NULL;
                requested_mode = 'MAINTENANCE';
                requested_dispatch_paused_intake_policy,
                backup_manifest_digest, backup_prior_mode,
                backup_prior_dispatch_paused_intake_policy IS NULL
  SET_MODE => expected_mode_revision > 0 AND expected_mode IS NOT NULL;
              requested_mode IS NOT NULL;
              backup_manifest_digest, backup_prior_mode,
              backup_prior_dispatch_paused_intake_policy IS NULL
  RESTORE_BACKUP => expected_mode_revision > 0 AND expected_mode IS NOT NULL;
                    backup_manifest_digest IS NOT NULL;
                    expected_mode = 'MAINTENANCE' =>
                      requested_mode = 'MAINTENANCE' AND
                      requested_dispatch_paused_intake_policy IS NULL AND
                      backup_prior_mode equals the backed-up row's nullable
                        maintenance_prior_mode and may be NULL only when
                        it carries bootstrap-null ancestry AND
                      backup_prior_dispatch_paused_intake_policy equals the
                        backed-up row's nullable prior policy;
                    expected_mode = 'DISPATCH_PAUSED' AND the restored
                      expected-revision row's dispatch policy =
                      'PAUSE_ADMISSION' =>
                      requested_mode = 'DISPATCH_PAUSED' AND
                      requested_dispatch_paused_intake_policy =
                        'PAUSE_ADMISSION' AND both backup-prior fields NULL;
                    every other initialized expected projection =>
                      requested_mode = 'DISPATCH_PAUSED' AND
                      requested_dispatch_paused_intake_policy =
                        'PAUSE_ADMISSION' AND both backup-prior fields NULL
CHECK ControllerModeOperation requested_dispatch_paused_intake_policy is
  non-null in the closed intake-policy enum exactly when requested_mode =
  'DISPATCH_PAUSED'; otherwise it is NULL
CHECK ControllerModeOperation.status IN ('SUCCEEDED', 'REJECTED')
CHECK ControllerModeOperation result union:
  SUCCEEDED => rejection_code IS NULL,
               result_mode_revision = expected_mode_revision + 1,
               result_mode = requested_mode,
               result_dispatch_paused_intake_policy =
                 requested_dispatch_paused_intake_policy,
               response_http_status = 200
  REJECTED  => rejection_code IN (
                 'CAS_LOST', 'ALREADY_INITIALIZED', 'NOT_INITIALIZED',
                 'NO_CHANGE', 'TRANSITION_NOT_ALLOWED',
                 'AUTHORITY_REVOKED', 'INTEGRITY_CONFLICT'
               ), result_mode_revision, result_mode,
               result_dispatch_paused_intake_policy IS NULL,
               response_http_status = 403 only for AUTHORITY_REVOKED and
                 409 otherwise
CHECK ControllerModeOperation.result_mode_revision IS NOT NULL exactly when
  status = 'SUCCEEDED'; a rejected Operation has result_mode_revision IS NULL
UNIQUE ControllerModeOperation(result_mode_revision)
GUARDED ControllerMode.last_operation_id names a `SUCCEEDED` Operation whose
  result_mode_revision, result_mode, and
  result_dispatch_paused_intake_policy exactly equal the singleton's current
  mode_revision, mode, and nullable dispatch policy. Its operation kind and
  exact prior/backup projection reproduce the singleton's conditional
  maintenance-prior fields. Every successful result revision is unique; a
  rejected Operation can never be the projection pointer
GUARDED authenticated_principal_id, authorization_context_digest,
  request_digest, response_json, response_digest, and completed_at_ms are
  non-null and bounded. response_digest covers exact status/body except only
  the transport `replayed` projection. Exact response protocol is
  orcest.controller-mode-result/1: common fields are
  controller_mode_operation_id/operation_kind/status/replayed; SUCCEEDED adds
  mode_revision, mode, and nullable dispatch_paused_intake_policy; REJECTED
  adds only the closed rejection_code
GUARDED same authenticated principal/operation ID/request digest returns the
  stored response; conflicting reuse is 409. Success conditionally matches
  expected revision/mode, increments the revision exactly once, writes the
  requested closed projection and reciprocal last_operation_id, and persists
  the Operation/response in one writer transaction. Rejection changes no mode
GUARDED a new store contains exactly the revision-0/null bootstrap singleton.
  Before opening ordinary endpoints, only the registered controller bootstrap
  service principal may commit INITIALIZE, producing revision 1 MAINTENANCE
  with no prior-mode fields. INITIALIZE after that is ALREADY_INITIALIZED;
  SET_MODE/RESTORE_BACKUP before it is NOT_INITIALIZED
GUARDED SET_MODE accepts every distinct initialized mode pair; a same-mode
  request is NO_CHANGE except a different DISPATCH_PAUSED intake policy is a
  real revision. Entering MAINTENANCE copies the exact prior mode/policy into
  the singleton; leaving MAINTENANCE clears them
GUARDED RESTORE_BACKUP is authorized only to the registered storage-reconciler
  service principal after the named manifest passes the complete restore
  checks. Its CAS is the restored positive mode revision/projection and its
  three result branches are exhaustive. A MAINTENANCE backup increments in
  place and copies its exact stored maintenance-prior projection, including
  bootstrap-null ancestry. An exact DISPATCH_PAUSED/PAUSE_ADMISSION backup
  increments in that same safe projection. Every other initialized backup is
  atomically installed at the next revision as
  DISPATCH_PAUSED/PAUSE_ADMISSION under the restore barrier before any ordinary
  endpoint opens. The latter two branches have NULL maintenance-prior fields;
  only the MAINTENANCE branch carries backup_prior fields. Restore never
  resumes an operational backed-up mode automatically and never restores bytes
  first and attempts to pause later
GUARDED mode permissions are closed: RUNNING permits ordinary workflow;
  INTAKE_PAUSED blocks admission; DISPATCH_PAUSED blocks new OFFERED Attempt
  creation, offer-Outbox delivery, and claims and applies its frozen intake
  policy; DRAINING blocks admission, new OFFERED Attempt creation, offer-Outbox
  delivery, and claims but accepts already-
  authorized Results and forge reconciliation; MAINTENANCE blocks first
  workflow mutation except its named recovery procedures. Narrow authenticated
  Controller Mode, Capability Key, Secret Provision, and Project Registration
  management Operations retain their own contracts; Secret Provision and
  Project Registration may run during Stage-0 bootstrap MAINTENANCE but cannot
  claim work, reduce a Run, or start ordinary Forge Schedule I/O.
  Among worker workflow endpoints it may return only an exact read-only
  ResultRequest response already present under the same global key; it cannot
  create a replay row or use another worker ledger. An unseen Result Request receives
  HTTP 503 with this exact five-field body:

  ```json
  {
    "protocol": "orcest.error/1",
    "code": "CONTROLLER_MAINTENANCE",
    "retryable": true,
    "message": "controller is in maintenance mode",
    "retry_after_seconds": 60
  }
  ```

  The wire value of `retry_after_seconds` is the literal integer `60`; no
  additional field is permitted. This creates no ResultRequest,
  AttemptResult, Fact, Evidence, or Transition. The same closed response
  applies when an unused key's semantic Result digest matches an accepted
  Result stored under another key, because that semantic replay would require
  a new ledger row. Retrying the same key/body after maintenance exits is
  ordinary first acceptance when authority is still live
PRIMARY KEY ForgeInstance(forge_instance_id)
UNIQUE ForgeInstance(canonical_origin)
CHECK ForgeInstance.adapter_kind IN ('GITHUB')
CHECK ForgeInstance.registration_provenance_version > 0
FOREIGN KEY ForgeInstance(
  credential_secret_id, registration_provenance_version
) -> SecretVersion(secret_id, version)
GUARDED credential_secret_id is the registered logical Forge credential and
  registration_provenance_version names the exact verified Receipt/authority
  used when the Forge Instance registration was installed. Rotation advances
  the logical Secret's current reference but never rewrites this provenance;
  every later forge operation resolves and freezes an exact current
  `(credential_secret_id, version)` under the per-Secret lock
PRIMARY KEY Project(project_id)
UNIQUE Project(forge_instance_id, repository_external_id)
CHECK Project.registration_state IN ('ACTIVE', 'SUSPENDED', 'REMOVED')
CHECK Project.registration_revision > 0
CHECK Project.installation_or_account_ref, default_ref,
  trusted_base_policy_ref, budget_policy_ref, budget_reset_window_ref,
  source_read_secret_id, publication_secret_id,
  registration_source_read_secret_version, and
  registration_publication_secret_version, and
  work_item_discovery_schedule_id are non-null registration values;
  both versions are positive
FOREIGN KEY Project(source_read_secret_id,
  registration_source_read_secret_version)
  -> SecretVersion(secret_id, version)
FOREIGN KEY Project(publication_secret_id,
  registration_publication_secret_version)
  -> SecretVersion(secret_id, version)
FOREIGN KEY Project.registration_operation_id
  -> ProjectRegistrationOperation(project_registration_operation_id)
  DEFERRABLE INITIALLY DEFERRED
FOREIGN KEY Project.work_item_discovery_schedule_id
  -> ForgeObservationSchedule(forge_observation_schedule_id)
  DEFERRABLE INITIALLY DEFERRED
PRIMARY KEY ProjectRegistrationOperation(project_registration_operation_id)
CHECK ProjectRegistrationOperation.protocol_version =
  'orcest.project-registration/1'
UNIQUE ProjectRegistrationOperation(authenticated_principal_id, idempotency_key)
CHECK ProjectRegistrationOperation.mode IN ('REGISTER', 'REVALIDATE')
CHECK ProjectRegistrationOperation mode union:
  REGISTER          => requested_project_id,
                       expected_registration_revision IS NULL
  REVALIDATE         => requested_project_id IS NOT NULL AND
                       expected_registration_revision > 0
CHECK ProjectRegistrationOperation.request_json, request_digest,
  authorization_context_digest, resolved_forge_instance_id, resolution_digest,
  installation_or_account_ref, resolved_repository_external_id,
  resolved_base_commit, response_http_status, response_json, response_digest,
  completed_at_ms are non-null and bounded
CHECK ProjectRegistrationOperation.status IN ('SUCCEEDED', 'REJECTED')
CHECK ProjectRegistrationOperation result union:
  SUCCEEDED => result_project_id IS NOT NULL,
               result_registration_revision > 0,
               result_work_item_discovery_schedule_id IS NOT NULL,
               resolved_forge_api_secret_ref,
               resolved_source_read_secret_ref,
               resolved_publication_secret_ref IS NOT NULL,
               rejection_code IS NULL;
               response_http_status = 200
  REJECTED  => result_project_id, result_registration_revision,
               result_work_item_discovery_schedule_id IS NULL,
               resolved_forge_api_secret_ref,
               resolved_source_read_secret_ref,
               resolved_publication_secret_ref IS NULL,
               rejection_code IS NOT NULL and code-owned/bounded;
               response_http_status = 409 only for
                 STABLE_REPOSITORY_OWNERSHIP_CONFLICT and 422 for
                 WORKFLOW_INVALID, CAPABILITY_UNSUPPORTED, or
                 POLICY_VALIDATION_FAILED
FOREIGN KEY ProjectRegistrationOperation.requested_project_id
  -> Project(project_id)
FOREIGN KEY ProjectRegistrationOperation.result_project_id
  -> Project(project_id) DEFERRABLE INITIALLY DEFERRED
FOREIGN KEY ProjectRegistrationOperation.resolved_forge_instance_id
  -> ForgeInstance(forge_instance_id)
FOREIGN KEY ProjectRegistrationOperation.resolved_forge_api_secret_ref
  -> SecretVersion(secret_id, version)
FOREIGN KEY ProjectRegistrationOperation.resolved_source_read_secret_ref
  -> SecretVersion(secret_id, version)
FOREIGN KEY ProjectRegistrationOperation.resolved_publication_secret_ref
  -> SecretVersion(secret_id, version)
FOREIGN KEY ProjectRegistrationOperation.result_work_item_discovery_schedule_id
  -> ForgeObservationSchedule(forge_observation_schedule_id)
  DEFERRABLE INITIALLY DEFERRED
GUARDED request_digest covers exact canonical bounded request_json.
  resolution_digest separately covers the authenticated resolution decision,
  authorization_context_digest, internal resolved Secret References, and
  resolved forge/repository/base identities plus the successful discovery
  Schedule identity; it is never a public response
  field. response_digest covers only response_http_status and the exact closed
  public orcest.project-registration-result/1 response_json. Stored response_json
  contains replayed=false; response_digest excludes exactly that transport-only
  field and identical replay changes only it to true. A REJECTED body contains
  exactly protocol, idempotency_key, mode, status, closed rejection_code,
  bounded sorted diagnostics, and replayed, with no Project/result fields.
  Neither JSON
  may contain raw credentials,
  Secret References/paths, bearer material, or runner signing material
GUARDED a successful Operation resolves three distinct logical Secrets owned by
  FORGE_INSTALLATION with owner_scope_id/provider_account_ref equal to its
  installation_or_account_ref and respective purposes FORGE_API, SOURCE_READ,
  and PUBLICATION. The FORGE_API Secret ID equals the resolved ForgeInstance's
  credential_secret_id and its version equals registration_provenance_version;
  the other two IDs/versions copy exactly to Project source/publication fields.
  Each version is current and creation-provenance-valid at resolution. These
  internal references are covered by resolution_digest and absent from the
  public response
GUARDED authentication, syntax/size/RBAC checks, forge reads, and repository
  workflow/policy validation are read-only before the writer transaction. That
  transaction claims the principal/idempotency key and inserts one terminal
  Operation and response; SUCCEEDED also inserts/updates the exact Project.
  REGISTER creates revision 1. REVALIDATE atomically requires the
  requested Project's registration_revision equal
  expected_registration_revision, requires requested default/trusted-base/
  budget/reset references and installation_or_account_ref equal the current
  Project, and installs exactly that value + 1 while refreshing only mutable
  locator/readiness projections. Installation/account migration is outside
  REVALIDATE. A CAS
  or authority-reference mismatch is 409 and inserts neither Operation nor
  Project mutation. Authority-bearing changes arrive only through an
  authenticated SERVER_ROLLOUT Policy Update. REJECTED changes no Project
GUARDED same principal/key/request_digest returns stored response_json; same
  principal/key with another digest returns 409 without mutation. A different
  principal has a distinct replay identity but must pass exact Project and
  stable-repository ownership authority/uniqueness fences
UNIQUE ProjectRegistrationOperation(result_project_id,
  result_registration_revision)
  WHERE status = 'SUCCEEDED'
GUARDED Project.registration_operation_id names a SUCCEEDED Operation whose
  result_project_id and result_registration_revision equal that exact Project
  and current registration_revision. Project revision/pointer and the reciprocal
  Operation result commit atomically through the deferred foreign keys. The
  Operation's result_work_item_discovery_schedule_id equals
  Project.work_item_discovery_schedule_id. That Schedule is the sole
  non-CLOSED PROJECT/WORK_ITEM_DISCOVERY identity for this Project, targets the
  Project, and has null Run/Publication. The Project's
  installation_or_account_ref, logical source-read/publication
  Secret IDs, and registration-provenance versions equal the Operation's exact
  request and internally resolved refs. Those versions prove registration but
  are not mutable-current credentials. Each AttemptClaim freezes the then-current
  version for `source_read_secret_id`, and each PublicationEffect freezes the
  then-current version for `publication_secret_id`; mutable installation-
  registry changes cannot rewrite this provenance or either frozen use
PRIMARY KEY Run(run_id)
UNIQUE active Run(project_id, work_item_external_id)
  WHERE terminal_outcome IS NULL
PRIMARY KEY WorkflowBlob(blob_digest)
CHECK WorkflowBlob.media_kind IN (
  'CONFIG_JSON', 'PROMPT_UTF8', 'POLICY_JSON', 'SERVER_POLICY_JSON'
)
CHECK WorkflowBlob.byte_length = length(normalized_bytes)
CHECK WorkflowBlob.blob_digest = "sha256:" + lowercase_hex(SHA256(
  ascii("orcest-workflow-blob-v1") || 0x00 || utf8(media_kind) || 0x00 ||
  uint64_be(byte_length) || normalized_bytes
))
PRIMARY KEY PolicyUpdate(policy_update_id)
UNIQUE PolicyUpdate(project_id, policy_update_sequence)
UNIQUE PolicyUpdate(project_id, source_kind, source_id)
CHECK PolicyUpdate.source_kind = 'SERVER_ROLLOUT'
CHECK PolicyUpdate.authenticated_principal_id, authorization_context_digest,
  source_id, default_ref, trusted_base_policy_ref, budget_policy_ref,
  and budget_reset_window_ref are non-null and bounded
GUARDED PolicyUpdate source_id names the authenticated server-rollout request
  or deployment identity authorized for the exact Project/policy revision; a
  repository workflow and an unauthenticated live-registry mutation cannot
  create it
GUARDED the Policy Update writer transaction atomically updates the Project's
  current default_ref and trusted-base/budget/reset projections to the exact
  values carried by the Update. Active Runs remain bound to installed Snapshot
  copies until their source-unique update reaches the lifecycle safe boundary
FOREIGN KEY PolicyUpdate.server_policy_blob_digest
  -> WorkflowBlob(blob_digest), with media_kind = 'SERVER_POLICY_JSON'
PRIMARY KEY BudgetReport(budget_report_id)
UNIQUE BudgetReport(project_id, accounting_scope_id, source_sequence)
UNIQUE BudgetReport(project_id, accounting_scope_id, source_revision)
FOREIGN KEY BudgetReport.project_id -> Project(project_id)
CHECK BudgetReport.source_sequence > 0
CHECK BudgetReport.limit_microunits > 0
CHECK BudgetReport.consumed_microunits >= 0
CHECK BudgetReport.window_start_ms < BudgetReport.reset_at_ms
CHECK BudgetReport.accepted_at_ms < BudgetReport.expires_at_ms
CHECK BudgetReport.expires_at_ms <= BudgetReport.reset_at_ms
CHECK BudgetReport.availability IN ('AVAILABLE', 'EXHAUSTED')
CHECK BudgetReport.availability =
  CASE WHEN consumed_microunits < limit_microunits
       THEN 'AVAILABLE' ELSE 'EXHAUSTED' END
CHECK BudgetReport.next_member_ordinal >= 0
CHECK BudgetReport.response_http_status = 200
GUARDED BudgetReport accounting_scope_id, window_id, source_revision,
  authenticated_principal_id, authorization_context_digest, report_digest,
  affected_run_ids_digest, response_json, and response_digest are non-null and
  bounded. budget_policy_ref and budget_reset_window_ref equal the Project
  registration projections current in the accepting writer transaction; the
  authenticated principal is registered for that exact Project/scope, and
  limit/unit/window/freshness validate against the installed POLICY_JSON;
  expires_at_ms equals min(reset_at_ms,
  accepted_at_ms + installed max_budget_report_age_ms)
GUARDED a new Report source_sequence is strictly greater than every prior
  accepted sequence for the Project/scope. The latest applicable Report used
  by offer planning is the greatest sequence whose exact policy refs and
  current window match and controller time is strictly before expires_at_ms;
  an old-window, expired, absent, or superseded-policy row cannot authorize an
  offer. At equality the writer inserts/reuses BUDGET_REPORT_EXPIRY before
  evaluating the gate
GUARDED availability is controller-derived from the normalized integer fields.
  report_digest covers every immutable Report field including accepted_at_ms
  and expires_at_ms, but excludes mutable fanout progress. response_json is the canonical
  `orcest.budget-report-result/1` public body, response_digest covers HTTP
  status plus that body except transport `replayed`, and exact
  budget_report_id/report_digest replay returns it. Conflicting ID, sequence,
  or revision reuse changes no state
CHECK BudgetReport fanout union:
  AVAILABLE => affected_run_ids_digest reproduces the complete membership,
               next_member_ordinal <= member count, and
               fanout_completed_at_ms is non-null exactly when
               next_member_ordinal = member count
  EXHAUSTED => affected_run_ids_digest is the canonical empty-list digest,
               next_member_ordinal = 0, fanout_completed_at_ms IS NOT NULL,
               and no BudgetReportRun row exists
PRIMARY KEY BudgetReportRun(budget_report_id, member_ordinal)
UNIQUE BudgetReportRun(budget_report_id, run_id)
CHECK BudgetReportRun.member_ordinal >= 0 and ordinals for one Report are
  zero-based and contiguous
FOREIGN KEY BudgetReportRun.budget_report_id
  -> BudgetReport(budget_report_id)
FOREIGN KEY BudgetReportRun.run_id -> Run(run_id)
GUARDED BudgetReportRun rows exist only for AVAILABLE; they are bytewise
  run_id ordered, contain every and only current same-Project WAITING/BUDGET
  Run whose immutable wake identity accepts this scope, policy/window, and
  source sequence at Report acceptance, and reproduce
  `SHA-256(canonical length-prefixed ordered run_id sequence)`, including the
  canonical empty sequence, as affected_run_ids_digest.
  Report plus full membership commit atomically; each member Transition and
  cursor advance is a separate idempotent writer transaction, and membership
  is never recomputed after acceptance
PRIMARY KEY PolicyUpdateComposition(policy_update_id, run_id)
FOREIGN KEY PolicyUpdateComposition.policy_update_id
  -> PolicyUpdate(policy_update_id)
FOREIGN KEY PolicyUpdateComposition.work_item_observation_id
  -> ForgeObservation(forge_observation_id)
FOREIGN KEY PolicyUpdateComposition.base_observation_id
  -> ForgeObservation(forge_observation_id)
GUARDED PolicyUpdateComposition observations belong to the Run's Project and:
  work_item_observation_id is target WORK_ITEM, kind WORK_ITEM_SNAPSHOT, and
    has the highest accepted sequence for that Work Item visible in the
    Policy Update transaction
  base_observation_id is kind BASE_HEAD for the exact trusted base selected by
    Project registration and is the eligible Work-Item- or Publication-targeted
    BASE_HEAD consumed by the greatest Run Transition sequence visible in the
    same transaction. The consuming Transition is either the ADMIT anchor or
    that Run's unique FORGE_OBSERVATION Transition; target-local observation
    sequence, adapter time, and a merely inserted but unconsumed base row are
    ineligible for cross-target selection
UNIQUE Snapshot(snapshot_id)
UNIQUE Snapshot(run_id, snapshot_sequence)
UNIQUE Snapshot(run_id, snapshot_id)
UNIQUE Snapshot(run_id, source_kind, source_id)
FOREIGN KEY Snapshot.work_item_observation_id
  -> ForgeObservation(forge_observation_id)
FOREIGN KEY Snapshot.base_observation_id
  -> ForgeObservation(forge_observation_id)
GUARDED Snapshot.work_item_observation_id is a same-Project/same-Work-Item
  WORK_ITEM_SNAPSHOT and Snapshot.base_observation_id is an applicable trusted
  BASE_HEAD; Snapshot title/body/comments and base_ref/base_commit equal those
  exact observations. The selected BASE_HEAD is the eligible base consumed by
  the greatest Transition sequence for that Run, counting the ADMIT anchor and
  FORGE_OBSERVATION transitions alike; per-target observation sequence and
  wall/adapter time cannot choose between Work Item and Publication targets
GUARDED Snapshot.supersession_key equals generation_input_hash when normalized
  base policy is REBASE_BEFORE_PUBLICATION or PIN; for
  SUPERSEDE_AT_BOUNDARY it equals SHA-256 of canonical JSON containing exactly
  generation_input_hash and canonical base_commit {object_format, oid};
  snapshot_hash covers supersession_key
FOREIGN KEY Snapshot.normalized_workflow_blob_digest
  -> WorkflowBlob(blob_digest), with media_kind = 'CONFIG_JSON'
FOREIGN KEY Snapshot.effective_policy_blob_digest
  -> WorkflowBlob(blob_digest), with media_kind = 'POLICY_JSON'
CHECK Snapshot source exclusive union:
  FORGE_OBSERVATION => source_id references ForgeObservation
  POLICY_UPDATE     => source_id references PolicyUpdate
PRIMARY KEY SnapshotPromptBlob(snapshot_id, path)
FOREIGN KEY SnapshotPromptBlob.snapshot_id -> Snapshot(snapshot_id)
FOREIGN KEY SnapshotPromptBlob.blob_digest
  -> WorkflowBlob(blob_digest), with media_kind = 'PROMPT_UTF8'
PRIMARY KEY SnapshotGeneration(run_id, specification_generation)
UNIQUE SnapshotGeneration(snapshot_id)
UNIQUE SnapshotGeneration(run_id, specification_generation, snapshot_id)
FOREIGN KEY SnapshotGeneration(run_id, snapshot_id)
  -> Snapshot(run_id, snapshot_id)
FOREIGN KEY SnapshotGeneration(run_id, installed_transition_sequence)
  -> Transition(run_id, transition_sequence)
GUARDED installed_transition_sequence names the exact SPEC_SUPERSEDE
  Transition whose trigger_id is snapshot_id; no other trigger installs a
  Snapshot
DEFERRABLE FOREIGN KEY Run(run_id, specification_generation, current_snapshot_id)
  -> SnapshotGeneration(run_id, specification_generation, snapshot_id)
DEFERRABLE FOREIGN KEY Run(run_id, pending_snapshot_id)
  -> Snapshot(run_id, snapshot_id)
CHECK Run.supersede_requested IN (0, 1)
CHECK Run.supersede_requested = 1 exactly when
  supersede_requested_transition_sequence IS NOT NULL
CHECK Run.supersede_requested = 1 => pending_snapshot_id IS NOT NULL
FOREIGN KEY Run(run_id, supersede_requested_transition_sequence)
  -> Transition(run_id, transition_sequence)
DEFERRABLE FOREIGN KEY Run(run_id, current_candidate_id)
  -> Candidate(run_id, candidate_id)
DEFERRABLE FOREIGN KEY Run(run_id, policy_replan_candidate_id)
  -> Candidate(run_id, candidate_id)
CHECK Run.current_candidate_id and policy_replan_candidate_id are not both
  non-null
CHECK Run panel-staffing projection is all-null or all-non-null:
  panel_staffing_candidate_id, panel_staffing_panel_round,
  panel_staffing_kind, latest_staffing_recheck_transition_sequence
CHECK non-null panel_staffing_panel_round > 0
CHECK non-null panel_staffing_kind IN ('REVIEW', 'ADJUDICATE')
FOREIGN KEY Run(run_id, panel_staffing_candidate_id)
  -> Candidate(run_id, candidate_id)
FOREIGN KEY Run(run_id, latest_staffing_recheck_transition_sequence)
  -> Transition(run_id, transition_sequence)
CHECK Run admission/Snapshot projection:
  state = 'ADMITTED' AND specification_generation = 0
    => current_snapshot_id IS NULL AND pending_snapshot_id IS NOT NULL
  otherwise specification_generation > 0 AND current_snapshot_id IS NOT NULL
GUARDED the only generation-0 interval is the newly admitted Run after ADMIT
  captured pending Snapshot 1 and before its SPEC_SUPERSEDE installation;
  no Activity, Attempt, or worker outbox exists in that interval
GUARDED supersede_requested may be true only with a differing
  pending_snapshot_id while one current CLAIMED Attempt or fenced controller
  Activity prevents installation. Its Transition is the latest ordered
  Snapshot-capture Transition that set the fence. A later capture replaces the
  pending pointer/sequence, coalescing back to the installed Snapshot clears
  pending pointer plus flag/sequence, and SPEC_SUPERSEDE, cancellation, or
  terminalization clears all three atomically. While true, a Result or Terminal
  Transition may complete/fence current work but cannot emit next semantic work
GUARDED the panel-staffing projection is non-null only in REVIEWING or
  ADJUDICATING and names the exact current Candidate, its positive panel round,
  and matching REVIEW or ADJUDICATE kind. Its sequence is the latest accepted
  peer Result/Attempt-Terminal Transition that left unfilled slots plus at
  least one peer CLAIMED. Replacing all four fields atomically discharges the
  older INTERNAL obligation. Only T(INTERNAL,
  latest_staffing_recheck_transition_sequence) may evaluate staffing after no
  peer remains CLAIMED; offer creation or one bound all-slot panel Wait clears
  the projection. If its Candidate/round/kind is stale or every slot is filled,
  only that same latest INTERNAL continuation clears it with the required
  same-state audit Transition and creates no offer/Wait. With unfilled PLANNED
  panel slots and no live offer, exactly this projection or that Wait must exist
GUARDED Run.policy_replan_candidate_id is non-null only while state REPLANNING
  for an installed explicit policy-only Snapshot and exactly one current REPLAN
  bound to that Snapshot/context; it names a same-Run Candidate from the prior
  specification generation, while current_candidate_id is NULL
GUARDED policy-only safe-boundary installation atomically moves the prior
  current_candidate_id to policy_replan_candidate_id, clears current_candidate_id
  and every old Plan/gate eligibility, installs the new Snapshot Generation,
  and creates the bound REPLAN. A valid accepted REPLAN plus exact identity
  recheck atomically moves the pointer back to current_candidate_id, clears
  policy_replan_candidate_id, and plans full VERIFY/review under the new Plan.
  A BUILD decision, another supersession, cancellation, recovery path that does
  not retain the context, or any other exit clears policy_replan_candidate_id
GUARDED a PUBLISHING Run whose current INITIAL Effect was superseded by a
  SUPERSEDE_AT_BOUNDARY base mismatch and which has the resulting base-only
  pending_snapshot_id is in the durable pending-specification subphase. The
  stale PUBLISH Activity/Effect is not dispatchable, no higher Effect or other
  semantic work may be planned, and the sole eligible lifecycle continuation
  is T(SPEC_SUPERSEDE, pending_snapshot_id). Restart resumes that installation;
  it never resumes the superseded publication operation or treats ordinary
  PUBLISHING as authority to skip the pending generation
CHECK Run recovery projection is closed:
  state = 'RECOVERING' => recovery_origin_state IS NOT NULL,
                          recovery_entry_source_kind IS NOT NULL,
                          recovery_entry_source_id IS NOT NULL
  state != 'RECOVERING' => recovery_origin_state, recovery_activity_id,
                           recovery_entry_source_kind,
                           recovery_entry_source_id,
                           recovery_resume_wait_condition_id,
                           recovery_resume_human_boundary_id,
                           recovery_resume_human_resolution_id IS NULL
CHECK Run.recovery_origin_state is a nonterminal lifecycle state other than
  RECOVERING; recovery_resume_human_boundary_id and
  recovery_resume_human_resolution_id are both NULL or both non-NULL
CHECK while RECOVERING exactly one resume shape holds: one non-null
  recovery_resume_wait_condition_id, one non-null Human Boundary/Resolution
  pair, or neither; the Wait shape and Human shape cannot coexist
FOREIGN KEY Run.recovery_activity_id -> Activity(activity_id)
FOREIGN KEY Run.recovery_resume_wait_condition_id
  -> WaitCondition(wait_condition_id)
FOREIGN KEY Run.recovery_resume_human_boundary_id
  -> HumanBoundary(human_boundary_id)
FOREIGN KEY Run.recovery_resume_human_resolution_id
  -> HumanResolution(human_resolution_id)
GUARDED recovery_activity_id, resume Wait, and resume Boundary/Resolution all
  belong to the same Run. A Wait resume copies that WaitCondition.resume_state;
  a Human resume copies that HumanBoundary.resume_state. The exact source that
  entered recovery matches recovery_entry_source_kind/source_id and the entry
  Transition. Every Transition entering RECOVERING atomically commits these
  fields and exactly one next typed Recovery Evidence. It creates no recovery
  Activity, Attempt, Outbox, Wait, Boundary, or tactic effect; only the later
  T(RECOVERY_EVIDENCE, recovery_evidence_id) may apply that Evidence. Leaving
  RECOVERING clears every recovery entry/resume field
CHECK Run.pending_dependency_observation_id,
  pending_dependency_set_digest, and pending_dependency_transition_sequence
  are either all NULL or all non-NULL
FOREIGN KEY Run.pending_dependency_observation_id
  -> ForgeObservation(forge_observation_id)
FOREIGN KEY Run(run_id, pending_dependency_transition_sequence)
  -> Transition(run_id, transition_sequence)
GUARDED the pending dependency observation is the latest accepted same-Run
  DEPENDENCY_STATE observation whose canonical required-dependency set is not
  authoritatively satisfied; the digest reproduces that frozen set, and the
  referenced same-state FORGE_OBSERVATION Transition has trigger_id equal to
  pending_dependency_observation_id. Observation-order replacement/clear,
  safe-boundary continuation, and any superseding cancellation or terminal
  input update all three fields in one writer transaction. At the first safe
  boundary, `T(INTERNAL, boundary_transition_sequence)` rechecks the exact set;
  if still unsatisfied it clears the triple, enters RECOVERING with that
  boundary state as origin, and appends EXTERNAL_DEPENDENCY Recovery Evidence
  selecting WAIT_EXTERNAL. Only its later RECOVERY_EVIDENCE Transition creates
  `WAITING/EXTERNAL_DEPENDENCY`
PRIMARY KEY Activity(activity_id)
UNIQUE Activity(run_id, activity_ordinal)
UNIQUE Activity(run_id, activity_id)
UNIQUE Activity(run_id, idempotency_key)
CHECK Activity.kind IN (
  'PLAN', 'BUILD', 'VERIFY', 'REVIEW', 'REMEDIATE', 'DIAGNOSE', 'REPLAN',
  'ADJUDICATE', 'REBASE', 'PR_REMEDIATE', 'IMPORT', 'PUBLISH',
  'CLOSE_PUBLICATION', 'CLOSE_REDUNDANT_PUBLICATION', 'REPAIR_RUN_MARKER',
  'RECONCILE'
)
CHECK Activity.execution_class IN ('WORKER', 'CONTROLLER')
GUARDED Activity execution class is WORKER for PLAN, BUILD, VERIFY, REVIEW,
  REMEDIATE, DIAGNOSE, REPLAN, ADJUDICATE, REBASE, and PR_REMEDIATE; it is
  CONTROLLER for IMPORT, PUBLISH, CLOSE_PUBLICATION,
  CLOSE_REDUNDANT_PUBLICATION, REPAIR_RUN_MARKER, and RECONCILE
CHECK Activity.state IN (
  'PLANNED', 'READY', 'ACTIVE', 'SUCCEEDED', 'FAILED', 'CANCELLED',
  'SUPERSEDED'
)
GUARDED Activity state/Attempt/outbox union (deferred and startup-checked):
  WORKER + PLANNED => no current nonterminal Attempt and no dispatchable
    worker outbox;
  WORKER + READY => exactly one current OFFERED Attempt and its one
    non-superseded worker outbox;
  WORKER + ACTIVE => exactly one current CLAIMED Attempt and no dispatchable
    worker outbox;
  CONTROLLER + READY => no Attempt and exactly one non-superseded controller
    outbox; CONTROLLER + ACTIVE => no Attempt and its committed controller
    operation/outbox fence;
  SUCCEEDED, FAILED, CANCELLED, or SUPERSEDED (either execution class) => no
    nonterminal Attempt and no dispatchable outbox. A terminal Activity cannot
    be returned to PLANNED/READY or receive a retry generation
GUARDED current_attempt_generation is NULL for PLANNED, controller, and every
  terminal Activity; it is the positive generation of the sole nonterminal
  worker Attempt for READY or ACTIVE. All state/Attempt/outbox changes below
  are one writer transaction; no intermediate row combination is observable
FOREIGN KEY Activity(run_id, created_transition_sequence)
  -> Transition(run_id, transition_sequence)
GUARDED Activity.idempotency_key equals sha256(canonical_json({
  reducer_version, run_id, specification_generation, policy_hash,
  created_transition_sequence, kind, execution_class,
  semantic_input_digest, candidate_id, forge_observation_id,
  change_request_head_observation_id, observed_change_request_head,
  role, repair_cycle, recovery_cycle,
  strategy_index, recovery_tactic, recovery_evidence_id, rescue_epoch
}))
CHECK Activity.change_request_head_observation_id and
  observed_change_request_head are both NULL or both NOT NULL
FOREIGN KEY Activity.change_request_head_observation_id
  -> ForgeObservation(forge_observation_id)
GUARDED non-null Activity.change_request_head_observation_id references a
  same-Run/same-Publication CHANGE_REQUEST_DISCOVERED, CHANGE_REQUEST_HEAD, or
  CHANGE_REQUEST_FEEDBACK, or CHANGE_REQUEST_MARKER Observation, and
  observed_change_request_head equals
  its normalized exact head; a BASE_HEAD observation cannot supply this fence
GUARDED CLOSE_REDUNDANT_PUBLICATION input_ref is the complete immutable
  orcest.redundant-publication-cleanup/1 object. It names an exact current
  REDUNDANT_PUBLICATIONS_PROVEN ReconciliationFact, Publication/effect,
  Project/ref/Run-marker, retained ID/head/observation, one CLOSE-member
  duplicate ID/head/identity observation, the Fact's complete search revision,
  that member's unreviewed observation/revision and equivalence digest, and a
  domain-separated operation_digest. Its
  change_request_head_observation_id and observed_change_request_head exactly
  copy the duplicate member. The Activity is permitted only in PR_MONITORING
  with no cancellation and no active Publication mutation; its semantic input
  digest covers the complete protocol object
GUARDED REPAIR_RUN_MARKER input_ref is the complete immutable
  `orcest.run-marker-repair/1` object. It binds the exact current Publication/
  Effect, run_id, Project, deterministic ref, change_request_external_id,
  expected_head, marker_observation_id, expected_body_revision,
  expected_marker_set_digest, repair_kind MISSING or DUPLICATED_IDENTICAL,
  desired single canonical marker derived only from run_id/publication_id,
  ownership_proof_digest, and
  operation_digest. It is legal only when
  the durable Publication association matches, the observed object has no
  conflicting valid v1 or legacy marker, and the proof establishes exact
  controller ownership; it can repair syntax/count only and cannot transfer
  ownership. change_request_head_observation_id/head and semantic_input_digest
  copy and cover those exact bindings. Planning is legal only in PR_MONITORING
  without cancellation or another Publication mutation. Activity and its
  Effect-bound Outbox commit before I/O. Immediately before
  `repair_change_request_marker_if_exact_owned`, the controller re-reads and
  CASes stable ID/head/body revision/ref/marker-set digest and the ownership
  proof. Any mismatch first persists the exact new Marker/head observation;
  ambiguity retains the same ACTIVE operation rather than blind retry
PRIMARY KEY ActivityReviewAssignment(activity_id)
CHECK ActivityReviewAssignment.assignment_kind IN ('REVIEW', 'ADJUDICATE')
CHECK ActivityReviewAssignment.panel_round > 0
CHECK ActivityReviewAssignment.role is a non-empty bounded code/policy-owned ID
CHECK ActivityReviewAssignment.context_digest matches canonical SHA-256 syntax
CHECK ActivityReviewAssignment.assignment_digest matches canonical SHA-256 syntax
CHECK ActivityReviewAssignment.subject_refs_digest matches canonical SHA-256 syntax
CHECK ActivityReviewAssignment.disputed_finding_ids_digest IS NULL OR
  matches canonical SHA-256 syntax
FOREIGN KEY ActivityReviewAssignment(activity_id)
  -> Activity(activity_id)
GUARDED ActivityReviewAssignment assignment_kind equals the owning
  Activity.kind, role equals Activity.role, and tagged-union matrix is:
  REVIEW => the owning Activity.kind is REVIEW;
            reviewer_slot IS NOT NULL;
            adjudication_round, adjudicator_slot IS NULL;
            disputed_finding_ids_digest IS NULL and
              ActivityAdjudicationFinding membership is empty
  ADJUDICATE => the owning Activity.kind is ADJUDICATE;
                reviewer_slot IS NULL;
                adjudication_round > 0 and adjudicator_slot IS NOT NULL;
                disputed_finding_ids_digest IS NOT NULL and
                membership is non-empty
GUARDED v1 ADJUDICATE assignments additionally require
  adjudication_round = 1, adjudicator_slot = 'default', and role = 'adjudicator'
GUARDED panel_round is scoped to the owning Activity.candidate_id, not the Run
  or Project. The first panel for a Candidate is round 1; all REVIEW Activities
  in one panel share that Candidate/round; ADJUDICATE names the exact disputed
  panel; and only the decisive all-overruled adjudication path may allocate
  `max(panel_round for candidate) + 1`. A new Candidate starts again at round 1
  and can never inherit or consume a prior Candidate's panel number
PRIMARY KEY ActivityReviewSubject(activity_id, subject_ordinal)
UNIQUE ActivityReviewSubject(activity_id, subject_ref)
CHECK ActivityReviewSubject.subject_ordinal >= 0 and ordinals for an Activity
  are zero-based and contiguous
CHECK ActivityReviewSubject.subject_ref is normalized, bounded, and non-empty
FOREIGN KEY ActivityReviewSubject.activity_id
  -> ActivityReviewAssignment(activity_id)
GUARDED every ActivityReviewAssignment has a nonempty subject membership sorted
  in the closed v1 semantic order: literal `snapshot:overall` first, followed
  by exactly one `plan:requirement:<requirement_key>` for each accepted Plan
  requirement in its canonical requirement order. No role-added or
  repository-added subject exists in v1. subject_refs_digest equals
  sha256(canonical_json(ordered ActivityReviewSubject.subject_ref array))
GUARDED ActivityReviewAssignment.context_digest preimage includes the exact
  subject_refs_digest, so a subject-set change cannot reuse the context
PRIMARY KEY ActivityAdjudicationFinding(activity_id, finding_ordinal)
UNIQUE ActivityAdjudicationFinding(activity_id, finding_id)
CHECK ActivityAdjudicationFinding.finding_ordinal >= 0 and ordinals for an
  Activity are zero-based and contiguous
FOREIGN KEY ActivityAdjudicationFinding.activity_id
  -> ActivityReviewAssignment(activity_id)
GUARDED every ActivityAdjudicationFinding.finding_id resolves to an exact
  accepted disputed Review Finding for the owning Candidate and panel; rows
  are sorted by ascending UTF-8 bytes of normalized finding_id
GUARDED ActivityReviewAssignment.disputed_finding_ids_digest equals SHA-256 of
  the canonical length-prefixed ActivityAdjudicationFinding.finding_id sequence
  in ordinal order, and assignment_digest equals sha256(canonical_json({
    assignment_kind, panel_round, reviewer_slot, adjudication_round,
    adjudicator_slot, role, context_digest, subject_refs_digest,
    disputed_finding_ids_digest
  })); it covers the semantic tagged union but excludes relational activity_id
  so Activity.semantic_input_digest and Activity ID allocation are acyclic
GUARDED every REVIEW or ADJUDICATE Activity has exactly one assignment, every
  other Activity has none, and Activity, assignment, complete subject and
  adjudication-Finding memberships, and Transition/Attempt/outbox commit
  atomically; Activity.semantic_input_digest includes assignment_digest
PRIMARY KEY CapabilityKeyRegistry(registry_id)
CHECK CapabilityKeyRegistry.registry_id = 'ORCEST_V1'
CHECK CapabilityKeyRegistry.registry_revision >= 0
CHECK CapabilityKeyRegistry bootstrap/initialized union:
  registry_revision = 0 => current_issuance_key_id, last_operation_id IS NULL
  registry_revision > 0 => last_operation_id IS NOT NULL
FOREIGN KEY CapabilityKeyRegistry.current_issuance_key_id
  -> CapabilitySigningKey(capability_signing_key_id)
FOREIGN KEY CapabilityKeyRegistry.last_operation_id
  -> CapabilityKeyOperation(capability_key_operation_id)
PRIMARY KEY CapabilityKeyOperation(capability_key_operation_id)
CHECK CapabilityKeyOperation.protocol_version =
  'orcest.capability-key-operation/1'
CHECK CapabilityKeyOperation.kind IN ('REGISTER', 'SELECT', 'RETIRE', 'REVOKE')
CHECK CapabilityKeyOperation.expected_registry_revision >= 0
CHECK CapabilityKeyOperation.target_capability_signing_key_id IS NOT NULL
CHECK CapabilityKeyOperation.status IN ('SUCCEEDED', 'REJECTED')
CHECK CapabilityKeyOperation.rejection_code IS NULL OR
  CapabilityKeyOperation.rejection_code IN (
  'CAS_LOST', 'KEY_ALREADY_EXISTS', 'KEY_NOT_ACTIVE',
  'CURRENT_KEY_REQUIRES_REPLACEMENT', 'AUTHORITY_REVOKED',
  'INTEGRITY_CONFLICT'
)
CHECK CapabilityKeyOperation capability-registration union:
  kind = 'REGISTER' => register_public_verification_key,
                       register_public_key_digest,
                       register_private_signing_secret_ref,
                       register_not_before_ms IS NOT NULL
  kind != 'REGISTER' => all register_* fields are NULL
CHECK CapabilityKeyOperation replacement union:
  RETIRE of current issuance key => replacement_issuance_key_id IS NOT NULL
  REVOKE of current issuance key => replacement_issuance_key_id may name one
                                    ACTIVE replacement or be NULL
  RETIRE/REVOKE of noncurrent key, REGISTER, SELECT
    => replacement_issuance_key_id IS NULL
CHECK CapabilityKeyOperation expected_issuance_key_id is NULL exactly when the
  caller expects no selected issuance key
CHECK CapabilityKeyOperation result union:
  SUCCEEDED => rejection_code IS NULL,
               result_registry_revision = expected_registry_revision + 1,
               response_http_status = 200
  REJECTED  => rejection_code IS NOT NULL and result_registry_revision,
               result_issuance_key_id are both NULL;
               response_http_status = 403 for AUTHORITY_REVOKED and 409 for
                 every other closed rejection code
UNIQUE CapabilityKeyOperation(result_registry_revision)
  WHERE status = 'SUCCEEDED'
GUARDED a successful Operation's result_issuance_key_id is the exact resulting
  Registry projection: initial REGISTER at revision 0 produces revision 1 and
  NULL issuance; a later REGISTER retains expected_issuance_key_id; SELECT uses
  target_capability_signing_key_id; RETIRE/REVOKE of a noncurrent key retains
  expected_issuance_key_id; RETIRE of the current key uses its required
  replacement; REVOKE of the current key uses its nullable replacement.
  Therefore NULL is valid for initial REGISTER, when issuance was already
  disabled during a later REGISTER, or when an emergency REVOKE of the selected
  key intentionally supplies no replacement. The emergency successful result
  MUST store result_issuance_key_id = NULL, exactly matching the Registry
FOREIGN KEY CapabilityKeyOperation.target_capability_signing_key_id
  -> CapabilitySigningKey(capability_signing_key_id)
  DEFERRABLE INITIALLY DEFERRED
FOREIGN KEY CapabilityKeyOperation.expected_issuance_key_id
  -> CapabilitySigningKey(capability_signing_key_id)
FOREIGN KEY CapabilityKeyOperation.replacement_issuance_key_id
  -> CapabilitySigningKey(capability_signing_key_id)
FOREIGN KEY CapabilityKeyOperation.result_issuance_key_id
  -> CapabilitySigningKey(capability_signing_key_id)
FOREIGN KEY CapabilityKeyOperation.register_private_signing_secret_ref
  -> SecretVersion(secret_id, version)
GUARDED every Operation has non-null bounded authenticated_principal_id,
  authorization_context_digest, request_digest, response_http_status,
  response_json, response_digest, and completed_at_ms. Its request digest
  covers protocol, kind, expected registry/current-key CAS, target/replacement,
  and the exact conditional REGISTER fields. The stable response protocol is
  orcest.capability-key-operation-result/1 with common
  capability_key_operation_id/kind/status/replayed; SUCCEEDED adds
  registry_revision and nullable current_issuance_key_id, while REJECTED adds
  only rejection_code. response_digest excludes exactly replayed. The response
  contains no private key or Secret value
GUARDED one writer transaction claims capability_key_operation_id, checks the
  exact registry revision/current-key CAS, applies the closed key state or
  selection change, increments registry_revision exactly once, updates
  last_operation_id, and stores the terminal response. REGISTER also inserts
  the reciprocal CapabilitySigningKey after verifying the public/private pair;
  same operation ID/body returns the stored response and conflicting reuse is
  an idempotency conflict. Rejected CAS/authority/integrity changes no registry
  or key row
GUARDED a new store contains exactly the revision-0/null-key/null-operation
  Registry singleton and no CapabilitySigningKey. The first successful
  Operation is REGISTER with expected revision 0 and expected issuance key
  NULL; it atomically creates one ACTIVE key plus Registry revision 1 while
  leaving current_issuance_key_id NULL. A separate successful SELECT CAS then
  advances to revision 2 or later and selects that ACTIVE key. Claim issuance,
  offer planning, and offer delivery remain disabled between those Operations.
  A synthetic selected bootstrap key or combined register-and-select write is
  forbidden
GUARDED CapabilityKeyRegistry.last_operation_id names a `SUCCEEDED` Operation
  whose result_registry_revision and nullable result_issuance_key_id exactly
  equal the singleton's current registry_revision and
  current_issuance_key_id. Every successful result revision is unique; a
  rejected Operation can never be the projection pointer
GUARDED a successful RETIRE/REVOKE updates only the target key's monotonic
  state/evidence; retirement_change_id or revocation_change_id equals this
  Operation ID and principal/authorization digest copy it exactly. SELECT
  changes no key state. REGISTER creates ACTIVE but does not silently select it
GUARDED the successful kind matrix is closed: REGISTER inserts exactly its
  target ACTIVE key and reciprocal registration pointer; SELECT requires its
  target already ACTIVE and selects it; RETIRE requires its target ACTIVE,
  moves it to RETIRED, and when current selects the required distinct ACTIVE
  replacement atomically; REVOKE requires its target ACTIVE or RETIRED, moves
  it to REVOKED, and when current either selects a distinct ACTIVE replacement
  or clears issuance. A replacement can never equal the target
GUARDED CapabilityKeyRegistry.current_issuance_key_id is NULL only at revision
  0, after initial REGISTER and before SELECT, or during a durable fail-closed
  dispatch pause. Otherwise it names an
  ACTIVE, not-before-satisfied key. Claim issuance compares and copies the
  registry revision and selected key in the same transaction as AttemptClaim;
  repository policy, Redis, or process flags cannot choose a signer. Emergency
  REVOKE of the selected key without replacement atomically clears it and
  pauses new-offer planning, delivery, and claim issuance; a later successful
  SELECT of an ACTIVE key is required to resume. This is a Registry gate and
  does not mutate Controller Mode. Existing Results, deadlines, and controller
  reconciliation continue under their ordinary mode/authority
PRIMARY KEY CapabilitySigningKey(capability_signing_key_id)
CHECK CapabilitySigningKey.signature_algorithm = 'ED25519'
CHECK CapabilitySigningKey.public_verification_key is exactly 32 bytes
CHECK CapabilitySigningKey.public_key_digest is the domain-separated SHA-256
  digest of signature_algorithm and the canonical public-verification bytes
FOREIGN KEY CapabilitySigningKey.private_signing_secret_ref
  -> SecretVersion(secret_id, version)
FOREIGN KEY CapabilitySigningKey.registration_operation_id
  -> CapabilityKeyOperation(capability_key_operation_id)
  DEFERRABLE INITIALLY DEFERRED
UNIQUE CapabilitySigningKey.registration_operation_id
GUARDED registration_operation_id names the unique successful REGISTER
  Operation whose target key ID, ED25519 public bytes/digest, private-signing
  SecretRef, and not-before time exactly equal this row; key and Operation
  commit atomically through the deferred references
CHECK CapabilitySigningKey.registered_at_ms, not_before_ms,
  public_verification_key, public_key_digest, and private_signing_secret_ref
  plus registration_operation_id are non-null; key ID, algorithm, public
  bytes/digest, and Secret Reference are immutable
CHECK CapabilitySigningKey.state IN ('ACTIVE', 'RETIRED', 'REVOKED')
CHECK CapabilitySigningKey retirement evidence group
  (retired_at_ms, retirement_change_id, retirement_principal_id,
   retirement_authorization_digest) is all NULL or all non-null
CHECK CapabilitySigningKey revocation evidence group
  (revoked_at_ms, revocation_change_id, revocation_principal_id,
   revocation_authorization_digest) is all NULL or all non-null
CHECK CapabilitySigningKey state/evidence union:
  ACTIVE  => retirement and revocation evidence groups are NULL
  RETIRED => retirement evidence is non-null and revocation evidence is NULL
  REVOKED => revocation evidence is non-null; retirement evidence is either
             all NULL for ACTIVE->REVOKED or retained non-null for
             RETIRED->REVOKED
UNIQUE CapabilitySigningKey(retirement_change_id)
  WHERE retirement_change_id IS NOT NULL
UNIQUE CapabilitySigningKey(revocation_change_id)
  WHERE revocation_change_id IS NOT NULL
FOREIGN KEY CapabilitySigningKey.retirement_change_id
  -> CapabilityKeyOperation(capability_key_operation_id)
FOREIGN KEY CapabilitySigningKey.revocation_change_id
  -> CapabilityKeyOperation(capability_key_operation_id)
GUARDED retirement_change_id and revocation_change_id name successful
  RETIRE/REVOKE Operations respectively whose target key, authenticated
  principal, and authorization digest exactly equal the corresponding state-
  change evidence group
GUARDED new claim-set issuance resolves an ACTIVE row and requires
  controller_now_ms >= not_before_ms. RETIRED forbids a new claim set but
  permits equivalent rematerialization and verification of an already-issued
  exact pinned claim set through its expiry; REVOKED rejects capability
  authentication, capability-backed endpoint replay, and the launch equality
  lookup immediately. It retains immutable audit ledgers but they cannot be
  retrieved through that revoked worker authority. Repository policy and worker input cannot select or
  change key state. Allowed monotonic moves are ACTIVE->RETIRED,
  ACTIVE->REVOKED, and RETIRED->REVOKED only; retirement evidence is never
  erased by later revocation
GUARDED after a launch capability's time expiry, an ACTIVE or RETIRED key may
  establish signature equality with the exact frozen claims solely to look up
  an already-retained matching Launch Attestation for the same registered
  runner/session and return its EXPIRED/null-material projection. This proof is
  not authentication and cannot accept, rematerialize, or mutate; the narrow
  lookup ends when the Attestation/key reference leaves authorized retention,
  and REVOKED denies it immediately
PRIMARY KEY Attempt(attempt_id)
UNIQUE Attempt(activity_id, generation)
UNIQUE Attempt(attempt_id, activity_id, generation)
UNIQUE nonterminal Attempt(activity_id)
  WHERE state IN ('OFFERED', 'CLAIMED')
CHECK Attempt.generation > 0
CHECK Attempt.offered_at_ms, claim_timeout_ms, and claim_deadline_ms are
  non-null positive integers for every Attempt
CHECK Attempt.claim_deadline_ms =
  Attempt.offered_at_ms + Attempt.claim_timeout_ms
GUARDED Attempt.claim_timeout_ms is the exact server-bounded value copied from
  the installed Snapshot's immutable effective-policy input at Attempt
  creation. Attempt, Activity, OFFERED outbox, offered_at_ms, timeout, and
  derived deadline commit atomically; replay never recomputes them from live
  policy or queue time
CHECK Attempt.state IN (
  'OFFERED', 'CLAIMED', 'SUCCEEDED', 'FAILED', 'ABSTAINED', 'EXPIRED',
  'SUPERSEDED'
)
GUARDED model-backed Attempt.execution_profile_id resolves in the installed
  Snapshot's immutable `POLICY_JSON` execution/classification mapping to
  exactly the stored worker_profile/provider/model/
  provider_account_ref four-value assignment; those four values and the
  separate execution_profile_id match its claim capability; provider_family
  and model_family are canonical non-secret server-derived classifications for
  that exact provider/model under the stored server-registry
  classification_revision; all three are non-null, immutable, and
  capability-bound; later registry mutation never reclassifies the Attempt;
  policy_hash covers the complete execution/family/revision mapping, and the
  controller retains that policy blob and revision for the Run's audit lifetime
  and rejects an unknown family ID or revision;
  provider_account_ref is non-secret and distinct from provider_secret_ref;
  deterministic VERIFY has NULL execution_profile_id/provider/model/
  provider_account_ref/provider_family/model_family/classification_revision and
  a registered deterministic worker_profile
CHECK Attempt state/nullability matrix:
  OFFERED => attempt_claim_id, claimed_worker_id, claimed_worker_session_id,
             claimed_at_ms, execution_deadline_ms,
             capability_auth_expires_at_ms, attempt_capability_jti,
             attempt_capability_digest, attempt_capability_signing_key_id,
             attempt_capability_signature_algorithm, launch_nonce_id,
             launch_capability_digest, launch_attestation_id,
             launch_capability_consumed_at_ms, provider_secret_ref IS NULL
  CLAIMED => attempt_claim_id, claimed_worker_id, claimed_worker_session_id,
             claimed_at_ms, execution_deadline_ms,
             capability_auth_expires_at_ms, attempt_capability_jti,
             attempt_capability_digest, attempt_capability_signing_key_id,
             attempt_capability_signature_algorithm IS NOT NULL
CHECK Attempt provider Secret binding:
  model-backed CLAIMED or formerly claimed => provider_secret_ref IS NOT NULL
  deterministic VERIFY in every state => provider_secret_ref IS NULL
CHECK Attempt.capability_auth_expires_at_ms =
  Attempt.execution_deadline_ms + 86400000 for every claimed or formerly
  claimed Attempt; the literal v1 grace is exactly 86,400,000 milliseconds
GUARDED Attempt.attempt_claim_id IS NULL exactly while the Attempt has never
  committed a claim; every formerly claimed terminal Attempt retains its Claim
  pointer and immutable claimant/deadline/auth-expiry/capability fields. An
  OFFERED Attempt expired or superseded without claim retains all of them NULL
UNIQUE Attempt(attempt_claim_id) WHERE attempt_claim_id IS NOT NULL
DEFERRABLE FOREIGN KEY Attempt.attempt_claim_id
  -> AttemptClaim(attempt_claim_id)
FOREIGN KEY Attempt.attempt_capability_signing_key_id
  -> CapabilitySigningKey(capability_signing_key_id)
CHECK Attempt.attempt_capability_signature_algorithm IS NULL OR
  Attempt.attempt_capability_signature_algorithm = 'ED25519'
PRIMARY KEY AttemptClaim(attempt_claim_id)
CHECK AttemptClaim.protocol_version = 'orcest.attempt-claim/1'
UNIQUE AttemptClaim(attempt_id)
UNIQUE AttemptClaim(worker_session_id, attempt_claim_id)
FOREIGN KEY AttemptClaim(attempt_id, activity_id, attempt_generation)
  -> Attempt(attempt_id, activity_id, generation)
FOREIGN KEY AttemptClaim.offer_outbox_id -> Outbox(outbox_id)
FOREIGN KEY AttemptClaim.source_read_secret_ref
  -> SecretVersion(secret_id, version)
FOREIGN KEY AttemptClaim.provider_secret_ref
  -> SecretVersion(secret_id, version)
FOREIGN KEY AttemptClaim.attempt_capability_signing_key_id
  -> CapabilitySigningKey(capability_signing_key_id)
FOREIGN KEY AttemptClaim.launch_capability_signing_key_id
  -> CapabilitySigningKey(capability_signing_key_id)
CHECK AttemptClaim attempt_id, activity_id, attempt_generation, offer_outbox_id,
  worker_id, worker_session_id, worker_profile, worker_build_revision,
  request_digest, claimed_at_ms, execution_deadline_ms,
  capability_auth_expires_at_ms, capability_key_registry_revision,
  attempt_capability_jti,
  attempt_capability_digest, attempt_capability_signing_key_id,
  attempt_capability_signature_algorithm, source_access_kind,
  source_access_descriptor_json, source_access_descriptor_digest,
  and response_contract_digest are non-null and bounded
CHECK AttemptClaim.attempt_generation > 0
CHECK AttemptClaim.capability_auth_expires_at_ms =
  AttemptClaim.execution_deadline_ms + 86400000
CHECK AttemptClaim.capability_key_registry_revision > 0
GUARDED at Claim commit capability_key_registry_revision equals the singleton's
  current revision and attempt_capability_signing_key_id plus conditional
  launch_capability_signing_key_id both equal its current ACTIVE issuance key;
  the Claim then preserves this historical selection without a mutable-registry
  foreign key
CHECK AttemptClaim launch union:
  model-backed => launch_nonce_id, launch_capability_jti,
                  launch_capability_digest, launch_capability_signing_key_id,
                  launch_capability_signature_algorithm IS NOT NULL
  VERIFY       => launch_nonce_id, launch_capability_jti,
                  launch_capability_digest, launch_capability_signing_key_id,
                  launch_capability_signature_algorithm IS NULL
CHECK AttemptClaim.attempt_capability_signature_algorithm = 'ED25519'
CHECK AttemptClaim.launch_capability_signature_algorithm IS NULL OR
  AttemptClaim.launch_capability_signature_algorithm = 'ED25519'
CHECK AttemptClaim.source_access_kind IN (
  'SCOPED_CREDENTIAL', 'BROKERED_ARCHIVE'
)
CHECK AttemptClaim source access union:
  SCOPED_CREDENTIAL => source_read_secret_ref IS NOT NULL
  BROKERED_ARCHIVE  => source_read_secret_ref IS NULL
CHECK AttemptClaim provider binding:
  model-backed => provider_secret_ref IS NOT NULL
  VERIFY       => provider_secret_ref IS NULL
UNIQUE AttemptClaim(attempt_capability_jti)
UNIQUE AttemptClaim(launch_capability_jti)
  WHERE launch_capability_jti IS NOT NULL
UNIQUE AttemptClaim(launch_nonce_id) WHERE launch_nonce_id IS NOT NULL
GUARDED AttemptClaim.request_digest covers the canonical request including its
  caller UUID and exact Attempt/Activity/generation/outbox/worker/session/
  profile/build bindings plus submitted positive diagnostic redis_epoch. That
  epoch is digest-bound for exact replay but remains forbidden as a claim or
  Result fence. source_access_descriptor_digest covers the exact
  non-secret registered repository, pinned commit, access kind/expiry, and
  nullable versioned source Secret Reference; it contains no bearer, signed
  URL, or secret. response_contract_digest covers the
  complete non-secret claim response projection including attempt_claim_id,
  capability registry revision/JTI/claims digest/signing-key/algorithm,
  deadlines, conditional
  launch identities/signing-key/algorithm, and
  source descriptor plus its frozen source Secret identity, and maps wire outbox_id to durable offer_outbox_id, but excludes
  Attempt/launch bearer bytes and source secret material
GUARDED AttemptClaim.attempt_capability_digest is the domain-separated digest
  of canonical normalized signed Attempt capability claims including its JTI,
  exact capability_signing_key_id, and signature_algorithm, not bearer text or
  signature randomness. The signer ID and algorithm exactly match the
  referenced CapabilitySigningKey. The bearer may be equivalently
  reminted only from those pinned claims. For model-backed work,
  launch_capability_digest equals:
  "sha256:" + lowercase_hex(SHA256(
    ascii("orcest-launch-capability-claims-v1") || 0x00 ||
    uint64_be(byte_length(canonical_claims_json)) || canonical_claims_json
  )), where canonical_claims_json contains exactly protocol
  orcest.launch-capability/1, launch JTI, Attempt/Activity/generation,
  worker/session, nonce, runner principal/registration revision,
  capability-signing key ID, signature algorithm, issued-at,
  execution deadline, and launch-attestation endpoint audience. It excludes
  bearer/signature serialization and remains stable across equivalent remint
GUARDED source_read_secret_ref, when present, has secret_id equal to the
  Project's immutable logical source_read_secret_id and freezes the exact
  SecretRef version current under the Secret Store writer lock at claim
  commit. For model-backed work the same claim transaction resolves the
  selected logical provider-account Secret's exact current version and copies
  that one provider_secret_ref into both Attempt and AttemptClaim; the OFFERED
  Attempt had no versioned provider binding. Later rotation changes neither
  frozen binding
GUARDED one claim transaction conditionally changes OFFERED to CLAIMED and
  atomically inserts AttemptClaim, sets Attempt.attempt_claim_id and every
  exact copied claimant/deadline/capability/launch/provider-Secret field, and
  moves Activity to
  ACTIVE. Same authenticated session/claim UUID/request digest returns the same
  row and non-secret response contract. It rematerializes source and launch
  authority only while controller time is strictly before execution_deadline_ms;
  after that deadline but before capability_auth_expires_at_ms it may
  rematerialize only a bearer for the exact unchanged pinned signed Attempt
  claims, including their original operations claim, while server-side endpoint
  policy permits Result reconciliation only. Exact canonical claims,
  JTI, issued-at, expiry, key ID, algorithm, and deterministic Ed25519 signature
  bytes reproduce the original; only outer bearer serialization may vary.
  At/after auth expiry it
  rematerializes no capability. Every rematerialized source credential is for
  the exact frozen source version; any key/body
  conflict is 409 IDEMPOTENCY_CONFLICT, and another key or session after claim
  is 409 ATTEMPT_ALREADY_CLAIMED
GUARDED model-backed CLAIMED Attempt has globally unique non-null
  launch_nonce_id and launch_capability_digest. Its launch_attestation_id and
  launch_capability_consumed_at_ms are both NULL before attestation or both
  non-null after one-shot consumption. Deterministic VERIFY has all four launch
  fields NULL in every state
UNIQUE Attempt(launch_nonce_id) WHERE launch_nonce_id IS NOT NULL
UNIQUE Attempt(launch_attestation_id) WHERE launch_attestation_id IS NOT NULL
PRIMARY KEY LaunchAttestation(launch_attestation_id)
CHECK LaunchAttestation.protocol_version = 'orcest.launch-attestation/1'
UNIQUE LaunchAttestation(attempt_id)
UNIQUE LaunchAttestation(launch_nonce_id)
UNIQUE LaunchAttestation(workspace_instance_id)
UNIQUE LaunchAttestation(context_instance_id)
UNIQUE LaunchAttestation(invocation_instance_id)
FOREIGN KEY LaunchAttestation(attempt_id, activity_id, attempt_generation)
  -> Attempt(attempt_id, activity_id, generation)
CHECK LaunchAttestation workspace_parent_id, context_parent_id,
  invocation_parent_id are all NULL
CHECK LaunchAttestation fresh_workspace = 1 AND fresh_context = 1
  AND fresh_invocation = 1
CHECK LaunchAttestation launch_attestation_id, worker_id, worker_session_id,
  pool_manager_id, runner_principal_id, runner_image_digest,
  runner_registration_revision, launch_nonce_id, launch_capability_digest,
  launch_capability_signing_key_id, launch_capability_signature_algorithm,
  workspace_instance_id, context_instance_id, invocation_instance_id,
  prepared_at_ms, attested_at_ms, runner_signing_key_id,
  runner_signature_algorithm,
  signature, and attestation_digest are non-null and bounded
FOREIGN KEY LaunchAttestation.launch_capability_signing_key_id
  -> CapabilitySigningKey(capability_signing_key_id)
CHECK LaunchAttestation.launch_capability_signature_algorithm = 'ED25519'
DEFERRABLE FOREIGN KEY Attempt.launch_attestation_id
  -> LaunchAttestation(launch_attestation_id)
GUARDED LaunchAttestation worker/session/nonce/capability equal the exact
  current CLAIMED model-backed Attempt and its AttemptClaim; the copied
  launch_capability_digest equals the exact domain-separated normalized-claims
  digest stored/exposed by that Claim; launch capability signing key ID and
  algorithm exactly equal the Claim and referenced CapabilitySigningKey; pool
  manager, runner principal, image, registration revision, runner signing key,
  and runner signature algorithm resolve to one
  allowlisted registered launch-isolation boundary for that Worker Session;
  signature verifies over attestation_digest, which covers every normalized
  immutable field except signature
GUARDED first acceptance requires controller_now_ms < Attempt.execution_deadline_ms
  and an unconsumed launch capability. One writer transaction inserts the
  Attestation, conditionally consumes the exact nonce/capability, and sets
  Attempt.launch_attestation_id and launch_capability_consumed_at_ms. Identical
  authenticated ID/digest replay always returns protocol
  orcest.launch-accepted/1 plus the same Attestation/Attempt identities. It
  returns status AVAILABLE and rematerializes provider assignment/material only
  from the pinned Secret Reference while the Attempt remains current CLAIMED and
  controller_now_ms < execution_deadline_ms; otherwise HTTP 200 returns status
  EXPIRED and provider NULL. SQLite stores neither provider bytes nor a
  secret-derived response digest;
  the AVAILABLE provider object includes flat non-secret secret_id and version
  fields copied from Attempt.provider_secret_ref, includes no nested
  provider_secret_ref object, and never resolves a mutable current version;
  after acceptance, a consumed or time-expired launch capability may be
  used only as signature-equality proof over the same frozen claims, under the
  same registered runner/session and an ACTIVE or RETIRED retained verifier,
  to look up the same Attestation ID/digest and return that EXPIRED projection;
  this is not authentication and cannot insert an Attestation, rematerialize
  material, extend authority, or mutate state; REVOKED denies it;
  any ID, nonce, Attempt, workspace, context, or invocation reuse with different
  content is an integrity conflict. Invalid/missing/parented/resumed evidence
  inserts no Attestation, Result, or Transition and leaves the claim for
  deadline/loss recovery
PRIMARY KEY AttemptTerminalFact(attempt_terminal_fact_id)
UNIQUE AttemptTerminalFact(attempt_id, kind, source_kind, source_id)
FOREIGN KEY AttemptTerminalFact(attempt_id, activity_id, attempt_generation)
  -> Attempt(attempt_id, activity_id, generation)
CHECK AttemptTerminalFact.kind IN (
  'CLAIM_DEADLINE', 'EXECUTION_DEADLINE', 'WORKER_LOST',
  'RESULT_AFTER_TERMINAL'
)
CHECK AttemptTerminalFact source/evidence matrix:
  CLAIM_DEADLINE => source_kind = 'TIMER_FACT',
                    expected_deadline_ms IS NOT NULL,
                    controller_now_ms >= expected_deadline_ms,
                    health_observation_id IS NULL,
                    capacity_disposition IN (
                      'COMPATIBLE_AVAILABLE', 'NO_COMPATIBLE_AVAILABLE'
                    ), health_observation_ids_digest IS NOT NULL,
                    controller_mode_revision, controller_mode,
                    capability_registry_revision,
                    replacement_offer_disposition IS NOT NULL,
                    selected_issuance_key_id is nullable only when no ACTIVE
                      selected key exists,
                    resolved_provider_secret_ref is non-null exactly for a
                      model-backed Attempt,
                    complete AttemptTerminalFactHealthObservation membership
  EXECUTION_DEADLINE => source_kind IN (
                          'TIMER_FACT', 'RESULT_REQUEST'
                        ), expected_deadline_ms IS NOT NULL,
                           controller_now_ms >= expected_deadline_ms,
                           health_observation_id IS NULL,
                           capacity_disposition,
                           health_observation_ids_digest,
                           resolved_provider_secret_ref,
                           controller_mode_revision, controller_mode,
                           capability_registry_revision,
                           selected_issuance_key_id,
                           replacement_offer_disposition IS NULL,
                           capacity membership empty
  WORKER_LOST => source_kind = 'HEALTH_OBSERVATION',
                 expected_deadline_ms, controller_now_ms IS NULL,
                 health_observation_id IS NOT NULL,
                 capacity_disposition,
                 health_observation_ids_digest,
                 resolved_provider_secret_ref,
                 controller_mode_revision, controller_mode,
                 capability_registry_revision, selected_issuance_key_id,
                 replacement_offer_disposition IS NULL,
                 capacity membership empty
  RESULT_AFTER_TERMINAL => source_kind = 'RESULT_REQUEST',
                           expected_deadline_ms, controller_now_ms,
                           health_observation_id, capacity_disposition,
                           health_observation_ids_digest,
                           resolved_provider_secret_ref,
                           controller_mode_revision, controller_mode,
                           capability_registry_revision,
                           selected_issuance_key_id,
                           replacement_offer_disposition IS NULL,
                           capacity membership empty
CHECK AttemptTerminalFact.replacement_offer_disposition IS NULL OR IN (
  'OFFER_ALLOWED', 'MODE_BLOCKED', 'ISSUANCE_KEY_UNAVAILABLE'
)
FOREIGN KEY AttemptTerminalFact.resolved_provider_secret_ref
  -> SecretVersion(secret_id, version)
FOREIGN KEY AttemptTerminalFact.selected_issuance_key_id
  -> CapabilitySigningKey(capability_signing_key_id)
GUARDED when a current fact wins the terminal fence:
  CLAIM_DEADLINE, EXECUTION_DEADLINE => Attempt.state = EXPIRED and
                                        terminal_reason equals the fact kind
  CLAIM_DEADLINE's ordinary non-panel reduction also sets its Activity to
    PLANNED, enters RECOVERING, and appends exactly one zero-counter Recovery
    Evidence row in the same T(ATTEMPT_TERMINAL) transaction. It creates no
    replacement Attempt, worker Outbox, or Wait in that transaction. A panel
    reduction with an unfilled slot and a peer CLAIMED is the sole exception:
    it remains REVIEWING/ADJUDICATING, appends no Recovery Evidence, creates
    no Wait/offer, and replaces the coalesced panel-staffing pointer.
  WORKER_LOST => Attempt.state = FAILED and terminal_reason = WORKER_LOST
  RESULT_AFTER_TERMINAL => the Attempt was already terminal and no terminal,
                           counter, Recovery Evidence, or work field changes
FOREIGN KEY AttemptTerminalFact.health_observation_id
  -> HealthObservation(health_observation_id)
PRIMARY KEY AttemptTerminalFactHealthObservation(
  attempt_terminal_fact_id, observation_ordinal
)
UNIQUE AttemptTerminalFactHealthObservation(
  attempt_terminal_fact_id, health_observation_id
)
CHECK AttemptTerminalFactHealthObservation.observation_ordinal >= 0 and
  ordinals per Fact are zero-based and contiguous
FOREIGN KEY AttemptTerminalFactHealthObservation.attempt_terminal_fact_id
  -> AttemptTerminalFact(attempt_terminal_fact_id)
FOREIGN KEY AttemptTerminalFactHealthObservation.health_observation_id
  -> HealthObservation(health_observation_id)
GUARDED CLAIM_DEADLINE membership contains at most the highest applicable
  unexpired Health Observation per pinned-policy-relevant worker/profile/
  provider-account/pool scope and sorts by
  (scope_kind, scope_id, health_sequence, health_observation_id). Its ordered
  IDs reproduce the mandatory health_observation_ids_digest, including the
  canonical digest of an empty membership. The code-owned classifier uses only
  that membership, immutable Attempt assignment, and the exact installed
  Snapshot policy that supplied this Attempt's claim_timeout_ms. Under the
  logical provider Secret lock, a model-backed classifier resolves and stores
  the verified current version for the assignment's logical provider account;
  only provider-account evidence for that exact resolved version is eligible:
  COMPATIBLE_AVAILABLE requires one complete target whose frozen
  subject_bindings prove the required worker profile, registered pool/session
  and positive slot, provider/account/exact newly resolved Secret version when
  model-backed,
  and Snapshot-pinned runner-launch compatibility, with no higher applicable
  unexpired Observation in any required scope making that target unavailable.
  The latest unexpired exact-version provider evidence is disqualifying only
  when UNAVAILABLE, RATE_LIMITED, or EXHAUSTED; AVAILABLE or no provider-account
  evidence is neutral. Absence is never synthesized as availability. Partial
  worker/session evidence, an AVAILABLE pool without the required profile, an
  unknown scope, or empty membership yields NO_COMPATIBLE_AVAILABLE. Redis
  absence and later observations cannot change the classified Fact
GUARDED CLAIM_DEADLINE freezes ControllerMode revision/mode and Capability Key
  Registry revision/selected key in the same writer transaction. Replacement
  disposition uses closed precedence: MODE_BLOCKED when mode is
  DISPATCH_PAUSED, DRAINING, or MAINTENANCE; otherwise
  ISSUANCE_KEY_UNAVAILABLE when no selected ACTIVE not-before-satisfied key
  exists; otherwise OFFER_ALLOWED. This disposition is evidence only: the
  terminal transaction never creates generation g+1, a worker Outbox, or a
  capacity Wait. The later T(RECOVERY_EVIDENCE, recovery_evidence_id) applies
  the deterministic selected tactic and, after rechecking current offer gates,
  creates either generation g+1/outbox or the typed capacity Wait.
GUARDED expected_deadline_ms equals the Attempt claim or execution deadline
  named by kind; WORKER_LOST HealthObservation proves the exact claimant/session
GUARDED AttemptTerminalFact.source_id encoding:
  TIMER_FACT          => exact timer_fact_id whose scope is the matching
                         ATTEMPT_CLAIM_DEADLINE or
                         ATTEMPT_EXECUTION_DEADLINE
  RESULT_REQUEST      => exact ResultRequest.result_request_id whose
                         attempt_terminal_fact_id names this Fact
  HEALTH_OBSERVATION  => exact health_observation_id
PRIMARY KEY ResultRequest(result_request_id)
CHECK ResultRequest.protocol_version = 'orcest.attempt-result/1'
UNIQUE ResultRequest(attempt_terminal_fact_id)
  WHERE attempt_terminal_fact_id IS NOT NULL
CHECK ResultRequest.accepted_result_created IS NULL OR
  ResultRequest.accepted_result_created IN (0, 1)
FOREIGN KEY ResultRequest(attempt_id, activity_id, attempt_generation)
  -> Attempt(attempt_id, activity_id, generation)
FOREIGN KEY ResultRequest.attempt_capability_signing_key_id
  -> CapabilitySigningKey(capability_signing_key_id)
FOREIGN KEY ResultRequest.accepted_result_attempt_id
  -> AttemptResult(attempt_id) DEFERRABLE INITIALLY DEFERRED
FOREIGN KEY ResultRequest.candidate_upload_id
  -> CandidateUpload(upload_id)
FOREIGN KEY ResultRequest.attempt_terminal_fact_id
  -> AttemptTerminalFact(attempt_terminal_fact_id)
CHECK ResultRequest claimed_worker_id, claimed_worker_session_id,
  attempt_capability_signing_key_id, attempt_capability_signature_algorithm,
  attempt_capability_digest, result_body_digest, disposition,
  response_http_status, response_json, response_digest, and recorded_at_ms
  are non-null and bounded
CHECK ResultRequest.attempt_capability_signature_algorithm = 'ED25519'
CHECK ResultRequest.disposition IN (
  'ACCEPTED', 'UPLOAD_EXPIRED', 'STALE_ATTEMPT',
  'EXPIRED_CURRENT', 'ALREADY_TERMINAL'
)
CHECK ResultRequest closed tagged union:
  ACCEPTED => accepted_result_attempt_id = attempt_id,
              accepted_result_created IS NOT NULL,
              candidate_upload_id, controller_now_ms, execution_deadline_ms,
              capability_auth_expires_at_ms, attempt_terminal_fact_id IS NULL,
              response_http_status = 200 and response_json is exact
                orcest.attempt-result-accepted/1
  UPLOAD_EXPIRED => accepted_result_attempt_id, accepted_result_created,
                    controller_now_ms, execution_deadline_ms,
                    capability_auth_expires_at_ms,
                    attempt_terminal_fact_id IS NULL,
                    candidate_upload_id IS NOT NULL,
                    response_http_status = 410 and response_json is the exact
                      shared orcest.candidate-upload-expired/1 body
  STALE_ATTEMPT => accepted_result_attempt_id, accepted_result_created,
                   candidate_upload_id, attempt_terminal_fact_id IS NULL,
                   controller_now_ms, execution_deadline_ms,
                   capability_auth_expires_at_ms IS NULL,
                   stale_reason IN (
                     'GENERATION_SUPERSEDED', 'CLAIM_BINDING_CHANGED',
                     'RUN_BINDING_CHANGED', 'TERMINAL_BEFORE_DEADLINE'
                   ), current_attempt_generation IS NULL or positive,
                   response_http_status = 409 and response_json is exact
                     orcest.error/1 ATTEMPT_STALE
  EXPIRED_CURRENT => accepted_result_attempt_id, accepted_result_created,
                     candidate_upload_id IS NULL,
                     controller_now_ms, execution_deadline_ms,
                     capability_auth_expires_at_ms,
                     attempt_terminal_fact_id IS NOT NULL,
                     execution_deadline_ms <= controller_now_ms,
                     controller_now_ms < capability_auth_expires_at_ms,
                     capability_auth_expires_at_ms =
                       execution_deadline_ms + 86400000,
                     response_http_status = 410 and response code is
                       EXECUTION_DEADLINE_EXCEEDED
  ALREADY_TERMINAL => same deadline/fact nullability as EXPIRED_CURRENT,
                      response_http_status = 409 and response code is
                        ATTEMPT_STALE
CHECK ResultRequest.stale_reason and current_attempt_generation are both NULL
  for every disposition except STALE_ATTEMPT
GUARDED every ResultRequest immutable Attempt/session/capability signer,
  algorithm, and normalized-claims digest binding equals the submitted claimed
  Attempt and AttemptClaim. Authentication and this global primary-key lookup
  occur before disposition selection. Exact same key plus every binding/body
  digest returns its stored response; any mismatch is IDEMPOTENCY_CONFLICT
  without revealing the prior request
GUARDED ACCEPTED points to the one exact AttemptResult. The creating request
  sets accepted_result_created=true and commits with that Result and reducer
  effects; when current Controller Mode permits Result-request mutation, a
  semantic replay under another unused key sets it false and inserts only this
  mapping. MAINTENANCE instead returns its closed 503 without inserting that
  new key. response_digest covers status/body except exactly the
  transport replayed projection, which is false only for the creating request's
  first acknowledgement and true for every later retrieval or semantic replay
GUARDED non-ACCEPTED response_digest covers the complete exact status/body;
  those closed responses have no transport replayed field and replay byte-for-byte
GUARDED UPLOAD_EXPIRED names the exact CandidateUpload submitted by the Result,
  bound to the same Attempt and already serialized to EXPIRED at or after its
  durable expiry. It commits with no Result, Candidate, Receipt, Terminal Fact,
  Recovery Evidence, or Transition and leaves the Attempt `CLAIMED`; a fresh
  upload/Result identity may still succeed only under the ordinary strict
  execution and upload deadlines
GUARDED STALE_ATTEMPT is selected only after authenticating the submitted
  immutable Attempt capability and global request key, strictly before that
  Attempt's execution deadline, when the durable generation, claim, Run, or
  pre-deadline terminal fence no longer matches. It stores exact HTTP 409
  `{protocol:"orcest.error/1", code:"ATTEMPT_STALE", attempt_id,
  current_attempt_generation, retryable:false}` and creates no AttemptResult,
  Candidate, Receipt, Terminal Fact, Recovery Evidence, or Transition
GUARDED each late disposition binds the Attempt's durable deadline/auth expiry
  and a source-unique Terminal Fact with source_kind RESULT_REQUEST and
  source_id=result_request_id. EXPIRED_CURRENT uses kind EXECUTION_DEADLINE,
  fences the current CLAIMED Attempt, and creates timeout Recovery Evidence in
  its sole ATTEMPT_TERMINAL Transition. ALREADY_TERMINAL uses kind
  RESULT_AFTER_TERMINAL after a prior non-Result terminal cause and reduces
  exactly once to a same-state ATTEMPT_TERMINAL audit Transition with no
  Recovery Evidence, counter, or work change. At or after auth expiry an unseen
  key authenticates nothing and no ResultRequest is inserted
PRIMARY KEY Outbox(outbox_id)
CHECK Outbox.source_kind IN (
  'ACTIVITY', 'HEALTH_PROBE_REQUEST', 'FORGE_OBSERVATION_REQUEST',
  'SECRET_PROVISION_OPERATION', 'TERMINAL_DUPLICATE_CLEANUP_ACTION'
)
CHECK Outbox.source_id IS NOT NULL
UNIQUE worker Outbox(activity_id, attempt_generation, destination)
  WHERE source_kind = 'ACTIVITY' AND attempt_generation IS NOT NULL
UNIQUE controller Outbox(activity_id, destination)
  WHERE source_kind = 'ACTIVITY' AND attempt_generation IS NULL
UNIQUE internal Outbox(source_kind, source_id, destination)
  WHERE source_kind IN (
    'HEALTH_PROBE_REQUEST', 'FORGE_OBSERVATION_REQUEST',
    'SECRET_PROVISION_OPERATION', 'TERMINAL_DUPLICATE_CLEANUP_ACTION'
  )
CHECK Outbox.state IN ('PENDING', 'DELIVERED', 'SUPERSEDED')
CHECK Outbox.attempt_id and attempt_generation are both NULL or both NOT NULL
FOREIGN KEY worker Outbox(attempt_id, activity_id, attempt_generation)
  -> Attempt(attempt_id, activity_id, generation)
FOREIGN KEY Outbox.activity_id -> Activity(activity_id)
GUARDED Outbox source union:
  ACTIVITY => activity_id IS NOT NULL and source_id = activity_id
  HEALTH_PROBE_REQUEST => activity_id IS NULL and source_id resolves to the
                          exact HealthProbeRequest that reciprocally names
                          this outbox_id
  FORGE_OBSERVATION_REQUEST => activity_id IS NULL and source_id resolves to
                               the exact ForgeObservationRequest whose
                               outbox_id reciprocally names this row
  SECRET_PROVISION_OPERATION => activity_id IS NULL and source_id resolves to
                                the exact SecretProvisionOperation
  TERMINAL_DUPLICATE_CLEANUP_ACTION => activity_id IS NULL and source_id
    resolves to the exact TerminalDuplicateCleanupAction whose outbox_id
    reciprocally names this row
GUARDED every non-Activity source has attempt_id and attempt_generation NULL.
  Publication fields are NULL except for TERMINAL_DUPLICATE_CLEANUP_ACTION,
  where both are required and equal the Reservation's immutable selected
  Publication Effect generation. Its normalized payload/digest
  carries the exact source ID and immutable Request/Operation digest, and the
  Request/Operation/Action plus Outbox commit before external I/O. An Activity Outbox
  cannot be referenced by an internal source. Health Probe, Forge Observation,
  Secret Provision, and mutating terminal-cleanup sources are PENDING when
  their Outbox commits; their
  later closed projections retain that immutable parent binding
CHECK Outbox publication_id and effect_generation are both NULL or both
  NOT NULL
FOREIGN KEY publication Outbox(publication_id, effect_generation)
  -> PublicationEffect(publication_id, effect_generation)
GUARDED a publication Outbox payload and payload_digest include its exact
  Publication Effect binding, and the PUBLISH Activity, immutable Effect, and
  Outbox row are inserted atomically by the creating Transition
GUARDED worker Outbox rows have no Publication Effect binding. A controller
  Outbox for PUBLISH always carries the exact immutable Publication Effect;
  publication RECONCILE and CLOSE_PUBLICATION rows carry it when their Activity
  is effect-bound. Every non-null binding equals the Activity's immutable
  semantic input. A CLOSE_REDUNDANT_PUBLICATION or REPAIR_RUN_MARKER Outbox
  remains source_kind = ACTIVITY and MUST have non-null
  publication_id/effect_generation equal to the current existing immutable
  Effect fence in its input; creating it never inserts or increments a
  PublicationEffect. A TERMINAL_DUPLICATE_CLEANUP_ACTION mutation Outbox has
  the same required existing Effect fence and never creates a new Effect; its
  exact-owned close or marker-detach operation identity commits with the Action
  before I/O. A controller row with no
  publication-side-effect binding has both fields NULL
PRIMARY KEY ProjectionOutbox(projection_outbox_id)
UNIQUE ProjectionOutbox(idempotency_key)
CHECK ProjectionOutbox.kind IN (
  'RUN_STATUS'
)
CHECK ProjectionOutbox.target_kind IN ('WORK_ITEM', 'CHANGE_REQUEST')
CHECK ProjectionOutbox.state IN ('PENDING', 'DELIVERED', 'SUPERSEDED')
FOREIGN KEY ProjectionOutbox(run_id, source_transition_sequence)
  -> Transition(run_id, transition_sequence)
CHECK ProjectionOutbox publication_id and publication_effect_generation
  are both NULL or both NOT NULL
FOREIGN KEY ProjectionOutbox(publication_id, publication_effect_generation)
  -> PublicationEffect(publication_id, effect_generation)
GUARDED RUN_STATUS payload is the complete desired external status projection
  for the exact source Transition and stable Work Item/Publication target,
  including adapter-normalized label, comment/status, and check projection
  components where supported. Those components are not additional outbox
  kinds; target_external_id, payload_json/payload_digest, delivery_count, and
  next_delivery_ms are non-null and bounded. The idempotency key derives from
  Run/source Transition/kind/target and nullable Effect. Source Transition and
  its rows commit atomically; a newer same-target desired state may supersede
  an undelivered older row, but delivery never changes lifecycle authority
PRIMARY KEY ArtifactObject(bundle_digest)
UNIQUE ArtifactObject(storage_key)
CHECK ArtifactObject.bundle_digest IS NOT NULL AND
  ArtifactObject.bundle_digest matches
  `sha256:<64 lowercase hexadecimal characters>`
CHECK ArtifactObject.storage_key IS NOT NULL, relative, normalized, and below
  the Candidate root; it has the exact form
  `objects/sha256/<first-two-hex>/<64-hex>.bundle` and its 64-hex component
  equals the hexadecimal suffix of bundle_digest
CHECK ArtifactObject.byte_length IS NOT NULL AND ArtifactObject.byte_length > 0
CHECK ArtifactObject.installed_at_ms IS NOT NULL AND
  ArtifactObject.installed_at_ms > 0
GUARDED ArtifactObject.byte_length equals the durable regular file's exact byte
  length and the file is owned by the controller, mode `0600`, and is not a
  symlink. The object path is resolved beneath the configured Candidate root;
  absolute paths, `..` components, alternate encodings, and path aliases are
  invalid. The row is inserted only after the file and its parent directories
  are fsynced, and the digest is SHA-256 of the exact installed bytes. An
  existing row may be reused only when all immutable inventory fields and the
  verified file agree; no operation may replace or retarget it
PRIMARY KEY CandidateUpload(upload_id)
UNIQUE CandidateUpload(attempt_id, request_idempotency_key)
FOREIGN KEY CandidateUpload(attempt_id, activity_id, attempt_generation)
  -> Attempt(attempt_id, activity_id, generation)
FOREIGN KEY CandidateUpload.promoted_bundle_digest
  -> ArtifactObject(bundle_digest)
FOREIGN KEY CandidateUpload.consumed_candidate_id
  -> Candidate(candidate_id)
CHECK CandidateUpload.state IN (
  'RECEIVING', 'VALIDATED', 'PROMOTED', 'CONSUMED', 'EXPIRED'
)
CHECK CandidateUpload.expires_at_ms IS NOT NULL
GUARDED CandidateUpload.expires_at_ms <= the bound Attempt.execution_deadline_ms
CHECK CandidateUpload state/reachability matrix:
  RECEIVING => computed_digest, verified_tip, promoted_bundle_digest,
               promoted_storage_key, promoted_at_ms,
               consumed_candidate_id IS NULL
  VALIDATED => computed_digest, verified_tip IS NOT NULL
               AND promoted_bundle_digest, promoted_storage_key,
                   promoted_at_ms, consumed_candidate_id IS NULL
  PROMOTED  => computed_digest, verified_tip, promoted_bundle_digest,
               promoted_storage_key, promoted_at_ms IS NOT NULL
               AND consumed_candidate_id IS NULL
  CONSUMED  => consumed_candidate_id IS NOT NULL
               AND promoted fields either name that Candidate's Artifact
                   Object or are cleared together after same-commit reuse
  EXPIRED   => consumed_candidate_id IS NULL
               AND promoted_bundle_digest, promoted_storage_key IS NULL;
               computed_digest, computed_bytes, verified_tip are all retained
                 iff validation previously succeeded, otherwise all NULL;
               promoted_at_ms is non-null only when the row previously reached
               PROMOTED and is retained as historical audit
GUARDED first validation, promotion, and Candidate/Result finalization each
  require controller_now_ms < CandidateUpload.expires_at_ms. Validation also
  requires RECEIVING; promotion requires VALIDATED; finalization requires
  PROMOTED. Equality is expired. Under the single writer, an expiration check
  that wins conditionally changes an unused RECEIVING or VALIDATED upload to
  EXPIRED. For PROMOTED it holds the shared storage mutation lock and atomically
  changes the row to EXPIRED while clearing the promoted bundle/storage-key
  live-reference fields; it retains computed digest/size/tip and promoted_at_ms
  as audit and leaves the Artifact Object for ordinary orphan grace. CONSUMED
  is immutable and never expires. Candidate content PUT against a row whose
  expiry wins returns exact HTTP 410
  {protocol: orcest.candidate-upload-expired/1, upload_id, state: EXPIRED,
   code: UPLOAD_EXPIRED, expires_at_ms}; it is derived only from this durable
  identity/state/deadline. A Result-path expiry stores byte-equivalent status
  and body in its UPLOAD_EXPIRED ResultRequest
PRIMARY KEY Candidate(candidate_id)
UNIQUE Candidate(run_id, candidate_id)
UNIQUE Candidate(producing_activity_id)
UNIQUE Candidate(producing_attempt_id)
  WHERE producing_attempt_id IS NOT NULL
UNIQUE Candidate(run_id, candidate_generation)
CHECK Candidate.candidate_generation > 0
CHECK Candidate provenance exclusive union:
  WORKER_ATTEMPT => producing_attempt_id, attempt_generation IS NOT NULL
                    AND import_forge_observation_id IS NULL
  FORGE_IMPORT   => producing_attempt_id, attempt_generation IS NULL
                    AND import_forge_observation_id IS NOT NULL
FOREIGN KEY Candidate.producing_activity_id -> Activity(activity_id)
FOREIGN KEY Candidate(producing_attempt_id, producing_activity_id, attempt_generation)
  -> Attempt(attempt_id, activity_id, generation)
FOREIGN KEY Candidate.import_forge_observation_id
  -> ForgeObservation(forge_observation_id)
UNIQUE Candidate(run_id, specification_generation, commit_object_format, commit_oid)
UNIQUE Candidate(candidate_id, commit_object_format, commit_oid)
PRIMARY KEY AttemptResult(attempt_id)
UNIQUE AttemptResult(activity_id, generation)
FOREIGN KEY AttemptResult.launch_attestation_id
  -> LaunchAttestation(launch_attestation_id)
CHECK AttemptResult.outcome IN (
  'SUCCEEDED', 'FAILED_RETRYABLE', 'FAILED_PERMANENT', 'ABSTAINED'
)
CHECK AttemptResult.result_schema_version, result_digest, outcome,
  accepted_at_ms are non-null and bounded
CHECK AttemptResult.failure_class IS NULL OR AttemptResult.failure_class IN (
  'INFRASTRUCTURE', 'PROVIDER_UNAVAILABLE', 'PROVIDER_RATE_LIMIT',
  'INCOMPATIBLE_WORKER', 'INVALID_AGENT_OUTPUT', 'VALIDATION_FAILURE',
  'CREDENTIAL_UNAVAILABLE', 'SOURCE_READ_FAILED', 'VERIFICATION_ERROR',
  'BASE_CONFLICT', 'POLICY_DENIED', 'SPECIFICATION_CONFLICT',
  'MISSING_AUTHORITY', 'INTEGRITY_FAILURE'
)
CHECK AttemptResult normalized failure union:
  outcome IN ('FAILED_RETRYABLE', 'FAILED_PERMANENT') => failure_class,
    failure_code, failure_evidence_refs, failure_evidence_digest IS NOT NULL;
    failure_evidence_refs is a bounded canonical UTF-8-byte-sorted unique
    array, possibly empty; failure_retry_after_ms IS NOT NULL exactly for
    failure_class = 'PROVIDER_RATE_LIMIT'
  outcome IN ('SUCCEEDED', 'ABSTAINED') => failure_class, failure_code,
    failure_retry_after_ms, failure_evidence_digest IS NULL and
    failure_evidence_refs is the canonical empty array
GUARDED failure_evidence_digest covers exact class, code, nullable retry time,
  and canonical evidence array. failure_retry_after_ms is an absolute Unix-
  millisecond time and is not clamped to the producing Attempt's execution
  deadline; that deadline only fences Result acceptance. For PROVIDER_RATE_LIMIT,
  the Result transaction freezes evaluation_time_ms = accepted_at_ms and the
  installed Snapshot's positive server-bounded max_provider_rate_limit_wait_ms;
  the resulting Recovery Evidence sets next_eligible_at_ms exactly to
  min(max(failure_retry_after_ms, evaluation_time_ms),
      evaluation_time_ms + max_provider_rate_limit_wait_ms).
  Policy normalization rejects a bound whose addition cannot fit signed
  64-bit Unix milliseconds; worker values above the representable horizon
  still clamp to the computed upper bound.
  The worker-protocol closed matrix enforces failure class, Activity kind, and
  outcome combinations
CHECK AttemptResult Activity/outcome payload union:
  PLAN, REPLAN + SUCCEEDED => structured_output is exact normalized
    orcest.plan/1 and output_digest matches; candidate_id, receipt_id IS NULL
  DIAGNOSE + SUCCEEDED => structured_output is exact normalized
    orcest.diagnosis/1 and output_digest matches; candidate_id, receipt_id IS NULL
  BUILD, REMEDIATE, PR_REMEDIATE, REBASE + SUCCEEDED => candidate_id IS NOT NULL;
    receipt_id, structured_output, output_digest IS NULL
  VERIFY + SUCCEEDED => receipt_id names exact PASS or FAIL VerificationReceipt;
    candidate_id, structured_output, output_digest IS NULL
  VERIFY + FAILED_RETRYABLE/VERIFICATION_ERROR => receipt_id names exact ERROR
    VerificationReceipt; candidate_id, structured_output, output_digest IS NULL
  REVIEW, ADJUDICATE + SUCCEEDED => receipt_id names exact schema-valid
    fills_slot=true Receipt; candidate_id, structured_output, output_digest IS NULL
  REVIEW, ADJUDICATE + ABSTAINED => receipt_id names exact schema-valid
    fills_slot=false Receipt; candidate_id, structured_output, output_digest IS NULL
  every other worker failure => exact normalized failure fields/summary only; candidate_id,
  receipt_id, structured_output, output_digest IS NULL
CHECK AttemptResult.receipt_id is NULL or a lowercase UUID
GUARDED AttemptResult.receipt_id is a closed polymorphic lookup, not a
  permissive cross-table reference: for `VERIFY` it names exactly one
  VerificationReceipt; for `REVIEW` it names exactly one ReviewReceipt; and
  for `ADJUDICATE` it names exactly one AdjudicationReceipt. In each allowed
  branch the receipt's producing Attempt, Activity, and generation equal this
  AttemptResult's exact fence, its Candidate/commit binding (when present)
  equals the Result union, and its receipt kind/outcome is allowed by the
  Activity/outcome matrix above. `receipt_id` is NULL for every other Activity
  or outcome. A matching UUID in a different receipt table cannot satisfy the
  branch, and a receipt row cannot be accepted without this guarded lookup.
GUARDED structured_output and output_digest are both NULL or both non-null;
  every output is bounded, canonical, non-secret, included in normalized result
  digest, and no AttemptResult has a credential-rotation Receipt/request field
GUARDED a model-backed AttemptResult.launch_attestation_id is non-null and
  equals the producing Attempt's accepted Launch Attestation; deterministic
  VERIFY requires NULL. result_digest is the canonical result_body_digest of
  every ResultRequest that points to this Result and includes this field;
  ResultRequest is the only Result endpoint idempotency registry
PRIMARY KEY VerificationReceipt(verification_receipt_id)
UNIQUE VerificationReceipt(producing_attempt_id)
UNIQUE VerificationReceipt(candidate_id, activity_id, producing_attempt_id,
  attempt_generation, profile_id, profile_hash)
CHECK VerificationReceipt.verification_receipt_id, candidate_id,
  commit_object_format, commit_oid, activity_id, producing_attempt_id,
  attempt_generation, profile_id, profile_hash, outcome, checks,
  evidence_digest, and created_at_ms are non-null and bounded
CHECK VerificationReceipt.outcome IN ('PASS', 'FAIL', 'ERROR')
CHECK VerificationReceipt.profile_id = 'default'
CHECK VerificationReceipt.profile_hash matches
  `sha256:<64 lowercase hexadecimal characters>`
FOREIGN KEY VerificationReceipt(
  producing_attempt_id, activity_id, attempt_generation
) -> Attempt(attempt_id, activity_id, generation)
FOREIGN KEY VerificationReceipt(candidate_id, commit_object_format, commit_oid)
  -> Candidate(candidate_id, commit_object_format, commit_oid)
PRIMARY KEY ReviewReceipt(review_receipt_id)
UNIQUE ReviewReceipt(producing_attempt_id)
FOREIGN KEY ReviewReceipt(
  producing_attempt_id, activity_id, attempt_generation
) -> Attempt(attempt_id, activity_id, generation)
FOREIGN KEY ReviewReceipt.activity_id
  -> ActivityReviewAssignment(activity_id)
PRIMARY KEY AdjudicationReceipt(adjudication_receipt_id)
UNIQUE AdjudicationReceipt(producing_attempt_id)
FOREIGN KEY AdjudicationReceipt(
  producing_attempt_id, activity_id, attempt_generation
) -> Attempt(attempt_id, activity_id, generation)
FOREIGN KEY AdjudicationReceipt.activity_id
  -> ActivityReviewAssignment(activity_id)
GUARDED ReviewReceipt exact trusted execution provenance fields
  (execution_profile_id, worker_profile, provider, model,
   provider_account_ref, provider_family, model_family, classification_revision,
   worker_id, worker_session_id, launch_attestation_id)
  equal the producing Attempt's immutable execution fields and authenticated
  claimant/session/accepted Launch Attestation; provider_family and
  model_family are never worker-supplied
GUARDED AdjudicationReceipt has the identical producing-Attempt execution/
  family/claimant copy rule
CHECK ReviewReceipt and AdjudicationReceipt execution_profile_id,
  worker_profile, provider, model, provider_account_ref, provider_family,
  model_family, classification_revision, worker_id, and worker_session_id
  and launch_attestation_id are non-null
GUARDED ReviewReceipt panel_round, reviewer_slot, role, context_digest, and
  subject_refs_digest equal its producing ActivityReviewAssignment and
  assignment_kind is REVIEW
GUARDED ReviewReceipt.assessments is bounded canonical JSON containing exactly
  the ActivityReviewSubject rows in frozen ordinal order and reproduces the
  Assignment.subject_refs_digest; each entry is
  {subject_ref, outcome, evidence_refs}, outcome is SATISFIED, VIOLATED, or
  UNVERIFIABLE, subject_ref is unique, and each evidence_refs list is sorted
  and duplicate-free
GUARDED AdjudicationReceipt panel_round, adjudication_round, adjudicator_slot,
  role, context_digest, and subject_refs_digest equal its producing
  ActivityReviewAssignment, assignment_kind is ADJUDICATE, its
  disputed_finding_ids equal the ordered
  ActivityAdjudicationFinding membership and reproduce the assignment's
  disputed_finding_ids_digest, and its dispositions cover exactly that
  membership unless it validly abstains
GUARDED ReviewReceipt.receipt_digest and AdjudicationReceipt.receipt_digest each
  cover the complete normalized worker receipt plus all copied trusted
  execution, provider_family/model_family, classification_revision, claimant,
  launch-attestation, assignment, and derived fills_slot fields; ReviewReceipt
  coverage includes the complete canonical assessments and
  subject_refs_digest
UNIQUE ReviewReceipt(candidate_id, panel_round, reviewer_slot)
  WHERE fills_slot = 1
CHECK ReviewReceipt:
  verdict = 'ABSTAIN' => abstention_code IS NOT NULL AND fills_slot = 0
  verdict IN ('APPROVE', 'BLOCK') => abstention_code IS NULL
UNIQUE AdjudicationReceipt(
  candidate_id, panel_round, adjudication_round, adjudicator_slot
)
  WHERE fills_slot = 1
CHECK AdjudicationReceipt:
  abstention_code IS NOT NULL => dispositions is empty AND fills_slot = 0
  abstention_code IS NULL => exactly one disposition per disputed_finding_id
  any INCONCLUSIVE disposition => fills_slot = 0
  fills_slot = 1 => every disposition is SUSTAIN or OVERRULE
PRIMARY KEY ConsensusDecision(consensus_decision_id)
UNIQUE ConsensusDecision(candidate_id, panel_round)
CHECK ConsensusDecision.outcome IN ('APPROVED', 'REMEDIATE', 'ADJUDICATE')
PRIMARY KEY ConsensusDecisionInput(consensus_decision_id, input_ordinal)
UNIQUE ConsensusDecisionInput(consensus_decision_id, receipt_kind, receipt_id)
CHECK ConsensusDecisionInput.receipt_kind IN ('VERIFICATION', 'REVIEW')
GUARDED ConsensusDecision insertion requires Run.state = 'AGGREGATING', every
  required default Verification and frozen Review input, canonical input order,
  and no Adjudication Receipt
PRIMARY KEY ForgeObservationSchedule(forge_observation_schedule_id)
CHECK ForgeObservationSchedule.schedule_kind IN (
  'WORK_ITEM_DISCOVERY', 'WORK_ITEM_POLL', 'BASE_HEAD_POLL', 'REF_POLL',
  'CHANGE_REQUEST_SEARCH', 'CHANGE_REQUEST_POLL', 'CI_POLL',
  'COMPLETE_MARKER_SEARCH'
)
CHECK ForgeObservationSchedule.state IN ('ACTIVE', 'PAUSED', 'CLOSED')
CHECK ForgeObservationSchedule.target_kind IN (
  'PROJECT', 'WORK_ITEM', 'PUBLICATION'
)
CHECK ForgeObservationSchedule.minimum_interval_ms > 0
CHECK ForgeObservationSchedule.schedule_revision >= 0
CHECK ForgeObservationSchedule kind/target union:
  WORK_ITEM_DISCOVERY => target_kind = 'PROJECT' AND target_id = project_id
                         AND run_id, publication_id IS NULL
  WORK_ITEM_POLL => target_kind = 'WORK_ITEM'
  BASE_HEAD_POLL => target_kind IN ('WORK_ITEM', 'PUBLICATION')
  REF_POLL, CHANGE_REQUEST_SEARCH, CHANGE_REQUEST_POLL, CI_POLL,
  COMPLETE_MARKER_SEARCH
    => target_kind = 'PUBLICATION' AND run_id, publication_id IS NOT NULL
CHECK ForgeObservationSchedule cleanup union:
  terminal_duplicate_cleanup_reservation_id IS NOT NULL =>
    schedule_kind IN ('CHANGE_REQUEST_POLL', 'COMPLETE_MARKER_SEARCH') AND
    target_kind = 'PUBLICATION' AND run_id, publication_id IS NOT NULL AND the
    exact Reservation is ACTIVE and copies this Run/Publication;
  terminal_duplicate_cleanup_reservation_id IS NULL => ordinary Run/pre-Run
    schedule ownership
GUARDED run_id/publication_id are nullable only for a pre-admission Work Item
  target or PROJECT discovery, where both are NULL. A Publication target binds
  the exact Run/Publication, and a Work Item target with a Run binds that same
  Run's Project/Work Item identity
CHECK ForgeObservationSchedule discovery projection union:
  WORK_ITEM_DISCOVERY => last_discovery_search_revision and
                         last_discovery_set_digest are both NULL before first
                         completed discovery or both non-null afterward
  every other kind => both fields are NULL
FOREIGN KEY ForgeObservationSchedule.project_id -> Project(project_id)
FOREIGN KEY ForgeObservationSchedule.forge_instance_id
  -> ForgeInstance(forge_instance_id)
FOREIGN KEY ForgeObservationSchedule.last_request_id
  -> ForgeObservationRequest(forge_observation_request_id)
  DEFERRABLE INITIALLY DEFERRED
FOREIGN KEY ForgeObservationSchedule.terminal_duplicate_cleanup_reservation_id
  -> TerminalDuplicateCleanupReservation(
       terminal_duplicate_cleanup_reservation_id
     ) DEFERRABLE INITIALLY DEFERRED
UNIQUE ForgeObservationSchedule(last_request_id)
  WHERE last_request_id IS NOT NULL
UNIQUE null-normalized ForgeObservationSchedule(
  project_id, schedule_kind, target_kind, target_id, run_id, publication_id,
  terminal_duplicate_cleanup_reservation_id
)
  WHERE state IN ('ACTIVE', 'PAUSED')
GUARDED next_due_at_ms and schedule_digest are non-null and bounded;
  schedule_digest covers normalized authority, target, kind, cadence, and the
  nullable Run/Publication/cleanup-Reservation binding, but excludes mutable
  revision, due/latest-request, state, and discovery-result projection fields.
  Only ACTIVE may create a Request. Terminalization closes/supersedes ordinary
  Run schedules/Requests/outboxes and creates or retains only Reservation-bound
  cleanup polling/search schedules; Reservation completion closes them and
  supersedes any pre-I/O PENDING Request/outbox
PRIMARY KEY ForgeObservationRequest(forge_observation_request_id)
CHECK ForgeObservationRequest.protocol_version =
  'orcest.forge-observation-request/1'
CHECK ForgeObservationRequest.request_sequence > 0
CHECK ForgeObservationRequest.schedule_revision >= 0
CHECK ForgeObservationRequest.created_under_controller_mode_revision > 0
CHECK ForgeObservationRequest.created_under_controller_mode IN (
  'RUNNING', 'INTAKE_PAUSED', 'DISPATCH_PAUSED', 'DRAINING'
)
CHECK ForgeObservationRequest.state IN ('PENDING', 'COMPLETED', 'SUPERSEDED')
CHECK ForgeObservationRequest.next_attempt_ordinal > 0
UNIQUE ForgeObservationRequest(
  forge_observation_schedule_id, request_sequence
)
UNIQUE ForgeObservationRequest(request_idempotency_key)
UNIQUE ForgeObservationRequest(forge_observation_schedule_id)
  WHERE state = 'PENDING'
UNIQUE ForgeObservationRequest(outbox_id)
FOREIGN KEY ForgeObservationRequest.forge_observation_schedule_id
  -> ForgeObservationSchedule(forge_observation_schedule_id)
FOREIGN KEY ForgeObservationRequest.credential_secret_ref
  -> SecretVersion(secret_id, version)
FOREIGN KEY ForgeObservationRequest.outbox_id -> Outbox(outbox_id)
FOREIGN KEY ForgeObservationRequest.last_failure_fact_id
  -> ForgeRequestFailureFact(forge_request_failure_fact_id)
  DEFERRABLE INITIALLY DEFERRED
FOREIGN KEY ForgeObservationRequest.controller_activity_id
  -> Activity(activity_id)
FOREIGN KEY ForgeObservationRequest.terminal_duplicate_cleanup_reservation_id
  -> TerminalDuplicateCleanupReservation(
       terminal_duplicate_cleanup_reservation_id
     )
FOREIGN KEY ForgeObservationRequest.terminal_duplicate_cleanup_action_id
  -> TerminalDuplicateCleanupAction(terminal_duplicate_cleanup_action_id)
FOREIGN KEY ForgeObservationRequest.created_under_controller_mode_revision
  -> ControllerModeOperation(result_mode_revision)
FOREIGN KEY ForgeObservationRequest(
  publication_id, effect_generation
) -> PublicationEffect(publication_id, effect_generation)
CHECK ForgeObservationRequest completion union:
  PENDING => result_observation_ids_digest, result_discovery_search_revision,
             result_discovery_set_digest, completed_at_ms IS NULL
  COMPLETED => result_observation_ids_digest, completed_at_ms IS NOT NULL;
               result_discovery_search_revision/result_discovery_set_digest
               are both non-null exactly for WORK_ITEM_DISCOVERY and NULL for
               every other kind
  SUPERSEDED => result_observation_ids_digest equals the canonical empty-list
                digest AND result_discovery_search_revision,
                result_discovery_set_digest IS NULL AND completed_at_ms IS NOT NULL
CHECK ForgeObservationRequest transient-failure projection:
  PENDING => last_failure_fact_id and next_retry_ms are both NULL before a
             failed outbound attempt or both non-null afterward; a later
             outbound attempt may replace both only with its greater ordinal
  COMPLETED, SUPERSEDED => last_failure_fact_id, next_retry_ms IS NULL
GUARDED a non-null last_failure_fact_id reciprocally names this Request, has
  request_attempt_ordinal < next_attempt_ordinal, and retry_not_before_ms =
  next_retry_ms. For Project discovery/pre-admission or terminal cleanup,
  reaching next_retry_ms makes the same Request directly eligible for another
  pre-I/O attempt. For an active Run, time alone is not redelivery authority:
  its Wait/recovery path must first select the exact redelivery/reconciliation
  tactic. Neither branch creates a new Request or synthetic Observation
GUARDED request_kind, Project/Forge authority, target, nullable Run/Publication,
  nullable terminal_duplicate_cleanup_reservation_id, and schedule_revision
  copy the exact Schedule projection used to create the Request. request_kind
  follows the Schedule's closed kind/target matrix. The created-under mode
  revision names the successful mode Operation whose result equals
  created_under_controller_mode and that exact non-maintenance projection
  permitted creation
CHECK ForgeObservationRequest credential/effect union:
  credential_purpose = 'PROJECT_SOURCE_READ' => credential_secret_ref is the
    exact current verified version of Project.source_read_secret_id and
    controller_activity_id, effect_generation, controller_operation_digest,
    terminal_duplicate_cleanup_reservation_id,
    terminal_duplicate_cleanup_action_id,
    terminal_cleanup_operation_digest IS NULL
  credential_purpose = 'PUBLICATION' => credential_secret_ref equals the exact
    PublicationEffect.publication_secret_ref and effect_generation IS NOT NULL;
    either controller_activity_id/controller_operation_digest are both
    non-null and all cleanup fields are NULL, or the cleanup Reservation is
    non-null, controller fields are NULL, and cleanup Action/operation are both
    non-null for mutation readback or both NULL for proof-refresh
GUARDED the controller pair and cleanup Action pair are each nullable together
  and mutually exclusive. The non-null controller pair copies the exact
  Activity/Publication Effect/operation fence for an effect or controller-
  operation readback. The cleanup shape copies its Schedule's exact active
  Reservation and existing selected Effect; when the Action pair is present it
  equals the current Action/operation, otherwise it authorizes proof refresh
  only. Ordinary polling/search uses Project source credentials and cannot
  claim controller-operation success. Rotation never rewrites an existing
  Request
CHECK ForgeObservationRequest prior-fence union:
  WORK_ITEM_DISCOVERY => expected_prior_observation_sequence,
                         expected_external_revision IS NULL;
                         expected_discovery_search_revision and
                         expected_discovery_set_digest are nullable together
                         and exactly copy the Schedule projection
  every other kind => expected_discovery_search_revision,
                      expected_discovery_set_digest IS NULL;
                      expected_prior_observation_sequence >= 0 and
                      expected_external_revision is NULL only when the target
                      has no prior external revision
GUARDED request_idempotency_key, request_digest, and created_at_ms
  are non-null and bounded; request_digest covers every normalized immutable
  Request field, including created_under_controller_mode_revision/mode and all
  nullable cleanup-Reservation/Action bindings
PRIMARY KEY ForgeRequestFailureFact(forge_request_failure_fact_id)
UNIQUE ForgeRequestFailureFact(
  forge_observation_request_id, request_attempt_ordinal
)
FOREIGN KEY ForgeRequestFailureFact.forge_observation_request_id
  -> ForgeObservationRequest(forge_observation_request_id)
FOREIGN KEY ForgeRequestFailureFact.run_id -> Run(run_id)
FOREIGN KEY ForgeRequestFailureFact.publication_id
  -> Publication(publication_id)
FOREIGN KEY ForgeRequestFailureFact.terminal_duplicate_cleanup_reservation_id
  -> TerminalDuplicateCleanupReservation(
       terminal_duplicate_cleanup_reservation_id
     )
CHECK ForgeRequestFailureFact.request_attempt_ordinal > 0
CHECK ForgeRequestFailureFact.failure_kind IN (
  'TIMEOUT', 'RATE_LIMIT', 'UNAVAILABLE'
)
GUARDED project_id, nullable run_id/publication_id/cleanup-Reservation, and
  request_digest copy the exact immutable Request. failure_code and
  failure_evidence_digest are bounded non-secret values; fact_digest covers
  every immutable Fact field, retry_not_before_ms is the deterministic bounded
  adapter-reset or policy-backoff result, and raw response/credential data is
  forbidden
GUARDED before I/O the writer reserves ordinal = next_attempt_ordinal and
  increments the Request. A failure for that ordinal inserts this Fact and
  atomically installs the reciprocal Request failure/retry projection only
  while the Request/outbox remain PENDING. Exact replay returns the Fact;
  conflicting content is integrity failure. COMPLETED/SUPERSEDED wins the same
  state CAS and forbids a late Fact. A Run-bound Fact appends its unique
  FORGE_REQUEST_FAILURE Transition and zero-counter FORGE_TRANSIENT Recovery
  Evidence; Project discovery, pre-admission work, and terminal cleanup have
  no Run Transition and retry directly from this durable Fact
PRIMARY KEY ForgeObservationRequestObservation(
  forge_observation_request_id, observation_ordinal
)
UNIQUE ForgeObservationRequestObservation(
  forge_observation_request_id, forge_observation_id
)
CHECK ForgeObservationRequestObservation.observation_ordinal >= 0 and ordinals
  for one Request are zero-based and contiguous
FOREIGN KEY ForgeObservationRequestObservation.forge_observation_request_id
  -> ForgeObservationRequest(forge_observation_request_id)
FOREIGN KEY ForgeObservationRequestObservation.forge_observation_id
  -> ForgeObservation(forge_observation_id)
GUARDED a member Observation was either created by this Request or is the
  same existing row under one of two closed dedupe rules, always with exact
  target/authority/credential/effect/cleanup binding equality: the same
  non-null adapter_event_id
  plus identical normalized content may be reused regardless of adjacency, or
  a row without an event ID may be reused only when it is the immediately
  preceding identical payload for that target. Intervening content forbids the
  latter. A coalesced row may appear in many Request memberships, retains its
  immutable original created_by_forge_observation_request_id, and creates no
  new Observation sequence or lifecycle Transition
GUARDED Request result kinds are closed by request_kind: WORK_ITEM_POLL may
  produce only WORK_ITEM_SNAPSHOT/DEPENDENCY_STATE; WORK_ITEM_DISCOVERY
  produces zero or more WORK_ITEM_SNAPSHOT rows ordered by bytewise stable Work
  Item external ID; BASE_HEAD_POLL only
  BASE_HEAD; REF_POLL exactly one REF_ABSENT or REF_HEAD;
  CHANGE_REQUEST_SEARCH exactly one CHANGE_REQUEST_ABSENT or
  CHANGE_REQUEST_DISCOVERED; CHANGE_REQUEST_POLL only current Change Request
  identity/head/marker/merge/close; CI_POLL only CHANGE_REQUEST_FEEDBACK; and
  COMPLETE_MARKER_SEARCH exactly one CHANGE_REQUEST_SEARCH_RESULT. A completed
  discovery requires its adapter search revision and domain-separated semantic
  set digest over the ordered `(work_item_external_id, work_item_revision,
  ForgeObservation.payload_digest)` triples, including the empty set. The
  exact preimage is
  `sha256(ascii("orcest-work-item-discovery-set-v1") || 0x00 ||
  canonical_json(ordered triples))`; controller Observation IDs do not enter it
GUARDED every non-discovery result Observation copies the Request's Project/
  target/Run/Publication, credential-purpose, cleanup-Reservation, and
  mutually exclusive controller-Activity or cleanup-Action operation binding.
  A discovery member instead targets its exact WORK_ITEM while copying the
  Request's Project, Forge authority, PROJECT_SOURCE_READ credential, and null
  Run/Publication/cleanup fields. For a controller/effect readback it also
  copies effect_generation and the applicable operation pair wherever that
  Observation kind defines them; a missing or changed field rejects the entire
  completion
GUARDED multi-member result order is closed: discovery sorts by bytewise stable
  Work Item external ID; WORK_ITEM_POLL orders WORK_ITEM_SNAPSHOT before
  DEPENDENCY_STATE; an open CHANGE_REQUEST_POLL emits DISCOVERED when the object
  is not yet associated, otherwise HEAD, followed by MARKER; a merged or closed
  poll emits only its terminal kind. Every other Request kind produces exactly
  one member. Newly allocated Observation sequences follow membership order
GUARDED a due Request transaction CASes an ACTIVE Schedule revision, allocates
  the next request_sequence, stores the Request plus exactly one source-kind
  FORGE_OBSERVATION_REQUEST Outbox, increments the Schedule revision, sets
  last_request_id, advances next_due_at_ms by at least minimum_interval_ms, and
  commits before forge I/O. No other PENDING Request may exist for the Schedule.
  Completion either atomically
  stores normalized Observations, complete ordered membership and digest,
  marks Request COMPLETED, and marks its Outbox DELIVERED, or records
  SUPERSEDED with no Observation and marks that Request's Outbox DELIVERED on a
  closed/different-last-request/scope/prior-observation CAS mismatch. The stale
  response was consumed even though its content produced no Observation, so
  the Outbox is not left retryable or relabeled as a successful observation.
  ACTIVE or PAUSED may complete while immutable schedule_digest and
  last_request_id still match; pure pause/reactivation revision changes do not
  invalidate that in-flight read. Discovery additionally CASes its exact
  expected search-revision/set-digest pair, stores the result pair, and
  increments the Schedule revision. Same discovery search revision plus the
  same set is replay; the same revision with a different set is an integrity
  error. Before each transport call the writer reserves the Request's current
  next_attempt_ordinal, increments it, and durably records the outbound
  attempt. TIMEOUT, RATE_LIMIT, or UNAVAILABLE for that ordinal atomically
  inserts/replays ForgeRequestFailureFact, installs last_failure_fact_id and
  next_retry_ms, and leaves Request/outbox PENDING. Restart scans every ACTIVE
  due Schedule and PENDING Request, honors next_retry_ms plus any Run-bound
  recovery authorization, and reuses the same adapter idempotency key and next
  ordinal. Request/Schedule state is not a
  reducer trigger; each accepted Observation and each Run-bound transient
  failure Fact is reduced exactly once. A pre-admission/discovery or terminal-
  cleanup Fact retries directly without a Run Transition, and no failure path
  creates a synthetic Observation
GUARDED ordinary Forge scheduling is mode-gated at the writer and outbox
  publisher. RUNNING, INTAKE_PAUSED, DISPATCH_PAUSED, and DRAINING permit the
  due-Request/read-reconciliation protocol; MAINTENANCE creates no new ordinary
  Forge Observation Request, delivers no such Outbox, and applies no first
  response completion. An ACTIVE due Schedule and any PENDING Request remain
  durable for post-maintenance resumption with the same identities. Only the
  separately named maintenance/recovery procedures may perform their exact
  allowlisted forge read, and they cannot be used to advance an ordinary
  Schedule implicitly
GUARDED Project registration or the Run Transition that first needs a read
  creates or CAS-reuses the one non-CLOSED Schedule identity. Every due request,
  pause, reactivate, or close increments schedule_revision. Suspension pauses
  Project schedules; reactivation resumes only still-required schedules. Run
  terminalization closes ordinary Run schedules and atomically supersedes
  their PENDING Requests/outboxes with empty membership; when terminal merge
  creates an ACTIVE cleanup Reservation, that same transaction creates or
  retains only its Reservation-bound CHANGE_REQUEST_POLL/
  COMPLETE_MARKER_SEARCH schedules. Reservation completion or a permanently
  ended target/kind closes those schedules and supersedes any pre-I/O PENDING
  Request/outbox. PAUSED may
  finish its existing PENDING Request but creates none; CLOSED creates and
  finishes none. A late response to a superseded Request is exact replay and
  cannot create an Observation
GUARDED a successful REGISTER transaction creates its PROJECT-target
  WORK_ITEM_DISCOVERY Schedule in state ACTIVE at revision 0, with no last
  Request or discovery result pair and immediate eligibility under its
  registered cadence. The terminal registration Operation, Project revision-1
  pointer, and Schedule commit atomically; REGISTER never exposes a Project
  without its durable discovery owner. REVALIDATE CAS-reuses that same
  non-CLOSED identity and never creates a second one. A discovery completion atomically creates or
  CAS-reuses Run-null WORK_ITEM_POLL and BASE_HEAD_POLL schedules for every
  returned Work Item before those reads and closes such schedules for absent
  items only when no active Run owns them. If the Project or discovery Schedule
  is PAUSED at completion, every child is created or retained PAUSED; discovery
  completion cannot activate it, and only Project reactivation may CAS a still-
  required child to ACTIVE. ADMIT atomically closes the selected
  Work Item's pre-Run schedules, supersedes any PENDING Requests and source
  Outboxes, and creates Run-bound revision-0 replacements; pre-Run and Run-bound
  identities may never both be non-CLOSED
GUARDED every PENDING, COMPLETED, and SUPERSEDED Request is retained as audit/
  replay input. A coalesced Observation, its immutable creating Request, and
  every Request-result membership remain retained through the last referencing
  membership and lifecycle Transition; GC cannot delete the creator while a
  later membership reuses the Observation
PRIMARY KEY ObservationCounter(project_id, target_kind, target_id)
CHECK ObservationCounter.target_kind IN ('WORK_ITEM', 'PUBLICATION')
CHECK ObservationCounter.target_id is non-null, normalized, and non-empty
CHECK ObservationCounter.next_observation_sequence > 0
FOREIGN KEY ObservationCounter.project_id -> Project(project_id)
GUARDED a newly created counter starts with next_observation_sequence = 1.
For every Forge Observation, the single writer atomically locks or creates the
counter row, allocates its current positive next_observation_sequence as the
Observation's observation_sequence, and increments the stored next value by
exactly one in the same transaction that inserts the Observation and its
Request-result membership. The allocator is the only source of observation
sequences; an Observation cannot be inserted by choosing a sequence directly.
The next value is therefore positive and is one greater than the greatest
allocated sequence (zero is the only prior-sequence value used by a caller
whose target has no Observation). Counter allocation is idempotent on the
durable Request/Observation identity and never rewinds or reuses a sequence.
PRIMARY KEY ForgeObservation(forge_observation_id)
UNIQUE ForgeObservation(project_id, target_kind, target_id, observation_sequence)
UNIQUE ForgeObservation(project_id, adapter_event_id)
  WHERE adapter_event_id IS NOT NULL
CHECK ForgeObservation.observation_sequence > 0
FOREIGN KEY ForgeObservation(project_id, target_kind, target_id)
  -> ObservationCounter(project_id, target_kind, target_id)
FOREIGN KEY ForgeObservation.created_by_forge_observation_request_id
  -> ForgeObservationRequest(forge_observation_request_id)
FOREIGN KEY ForgeObservation.terminal_duplicate_cleanup_reservation_id
  -> TerminalDuplicateCleanupReservation(
       terminal_duplicate_cleanup_reservation_id
     ) DEFERRABLE INITIALLY DEFERRED
GUARDED created_by_forge_observation_request_id is non-null exactly when this
  Observation was first created by a scheduled read result and copies that
  Request's credential union and applicable Activity/effect/cleanup bindings.
  Request membership is
  not globally unique: a later completed Request may reuse the same non-null
  delivery ID/identical content, or the immediately preceding no-event-ID
  identical target payload, only with exact authority/credential/effect
  equality and without allocating a new Observation sequence or Transition.
  A direct Effect/checkpoint controller mutation response outside this
  scheduled readback protocol has created_by_forge_observation_request_id,
  credential_purpose, credential_secret_ref, and cleanup fields all NULL and
  instead uses the independent Effect/checkpoint/controller-operation
  provenance. A direct terminal-cleanup mutation is also outside the scheduled
  readback protocol: its creating Request is NULL and both credential fields
  are NULL. Only a Request-backed terminal-cleanup proof/readback may carry a
  non-null Request and copied credentials. Webhooks are schedule wake hints,
  not direct Observation insertion authority
CHECK ForgeObservation.kind IN (
  'WORK_ITEM_SNAPSHOT', 'DEPENDENCY_STATE', 'BASE_HEAD', 'REF_ABSENT',
  'REF_HEAD', 'CHANGE_REQUEST_DISCOVERED', 'CHANGE_REQUEST_HEAD',
  'CHANGE_REQUEST_ABSENT', 'CHANGE_REQUEST_MARKER',
  'CHANGE_REQUEST_FEEDBACK', 'CHANGE_REQUEST_SEARCH_RESULT',
  'CHANGE_REQUEST_MERGED', 'CHANGE_REQUEST_CLOSED'
)
CHECK ForgeObservation target/kind matrix:
  WORK_ITEM_SNAPSHOT, DEPENDENCY_STATE => target_kind = 'WORK_ITEM'
  REF_ABSENT, REF_HEAD, CHANGE_REQUEST_ABSENT, CHANGE_REQUEST_DISCOVERED,
  CHANGE_REQUEST_HEAD, CHANGE_REQUEST_MARKER, CHANGE_REQUEST_FEEDBACK,
  CHANGE_REQUEST_SEARCH_RESULT, CHANGE_REQUEST_MERGED,
  CHANGE_REQUEST_CLOSED
    => target_kind = 'PUBLICATION'
  BASE_HEAD => target_kind IN ('WORK_ITEM', 'PUBLICATION')
CHECK ForgeObservation.live_cardinality IS NULL OR IN (
  'ZERO', 'ONE', 'MULTIPLE'
)
GUARDED ForgeObservation.live_cardinality, complete_search_revision, and
  duplicate_set_digest are non-null exactly for
  CHANGE_REQUEST_SEARCH_RESULT and null for every other kind
GUARDED CHANGE_REQUEST_ABSENT binds the exact registered Project/repository,
  Publication, deterministic normalized ref, syntactically
  valid Run marker, adapter search revision, and normalized stable
  nonexistence token. Its optional effect/controller fields follow its parent
  Request union; a cancellation/publication-operation absence proof requires
  the exact current Effect and Activity/operation fence, while an ordinary
  monitoring search has them NULL. Its payload digest covers all bindings; a timeout, empty
  unversioned response, REF_ABSENT, or omitted marker search cannot create it
GUARDED CHANGE_REQUEST_SEARCH_RESULT is the only observation that proves a
  complete same-marker set. It requires run_id/publication_id, has NULL
  publication effect/controller fields for an ordinary post-link monitoring
  read. For a pre-link COMPLETE_MARKER_SEARCH owned by the current INITIAL
  Effect, it instead copies non-null publication_effect_generation,
  controller_activity_id = PUBLISH Activity, and
  controller_operation_digest = Effect.operation_digest from its
  PUBLICATION-purpose effect-readback Request; only this stronger shape may
  back a PublicationEffectCheckpoint. Both shapes bind exact registered source
  repository ID, Run marker/deterministic ref, and complete_search_revision
  equal to external_revision. Its independently ordered LIVE and TERMINAL
  child memberships, derived live_cardinality, and duplicate_set_digest obey
  the complete relational contract below. An
  individual discovery/head/feedback/merge/close observation other than
  CHANGE_REQUEST_MARKER cannot assert this digest; subject to the generic
  duplicate-row exclusions below, it may supersede an in-flight duplicate
  proof and require a new complete search. Marker observations use only the
  marker-specific guards below
PRIMARY KEY ChangeRequestSearchMember(
  forge_observation_id, member_class, member_ordinal
)
UNIQUE ChangeRequestSearchMember(
  forge_observation_id, change_request_external_id
)
FOREIGN KEY ChangeRequestSearchMember.forge_observation_id
  -> ForgeObservation(forge_observation_id)
CHECK ChangeRequestSearchMember.member_class IN ('LIVE', 'TERMINAL')
CHECK ChangeRequestSearchMember terminal union:
  member_class = 'LIVE' => terminal_state, merge_commit IS NULL;
  member_class = 'TERMINAL' => terminal_state IN ('CLOSED', 'MERGED');
  terminal_state = 'MERGED' => merge_commit IS NOT NULL;
  terminal_state = 'CLOSED' => merge_commit IS NULL
CHECK ChangeRequestSearchMember.ownership_status IN (
  'POSITIVE', 'INCOMPATIBLE', 'INCOMPLETE'
)
CHECK ChangeRequestSearchMember.proof_kind IS NULL OR IN (
  'EXACT_CREATE_RESPONSE', 'AMBIGUOUS_CREATE_RECONCILED', 'LIVE_ASSOCIATION'
)
CHECK ChangeRequestSearchMember ownership proof union:
  exactly one of these closed branches is true:
    (ownership_status = 'POSITIVE' AND proof_kind = 'EXACT_CREATE_RESPONSE'
      AND proof_publication_effect_generation, creator_installation_or_account_ref,
      proof_deterministic_ref, proof_run_marker, proof_desired_commit,
      proof_observed_head, head_evidence_observation_id, observed_body_revision,
      marker_set_digest, ownership_proof_digest, proof_create_checkpoint_id,
      proof_create_request_idempotency_key are all non-null
      AND ownership_defect_codes is empty
      AND proof_create_checkpoint_id is CHANGE_REQUEST_CREATE/OBSERVED_SATISFIED
          with that exact request key and effect generation);
    (ownership_status = 'POSITIVE' AND proof_kind =
      'AMBIGUOUS_CREATE_RECONCILED'
      AND proof_publication_effect_generation, creator_installation_or_account_ref,
      proof_deterministic_ref, proof_run_marker, proof_desired_commit,
      proof_observed_head, head_evidence_observation_id, observed_body_revision,
      marker_set_digest, ownership_proof_digest, proof_create_checkpoint_id,
      proof_create_request_idempotency_key are all non-null
      AND ownership_defect_codes is empty
      AND proof_create_checkpoint_id is the exact prior
          CHANGE_REQUEST_CREATE/AMBIGUOUS source reconciled by this search
          with that exact request key and effect generation);
    (ownership_status = 'POSITIVE' AND proof_kind = 'LIVE_ASSOCIATION'
      AND proof_publication_effect_generation, creator_installation_or_account_ref,
      proof_deterministic_ref, proof_run_marker, proof_desired_commit,
      proof_observed_head, head_evidence_observation_id, observed_body_revision,
      marker_set_digest, ownership_proof_digest are all non-null
      AND ownership_defect_codes is empty
      AND proof_create_checkpoint_id, proof_create_request_idempotency_key are
          both null
      AND the exact durable Publication association supplies authority);
    (ownership_status IN ('INCOMPATIBLE', 'INCOMPLETE')
      AND proof_kind is null AND ownership_proof_digest is non-null
      AND ownership_defect_codes is a canonically sorted nonempty unique subset
          of (CREATE_PROVENANCE_MISSING, CREATOR_AUTHORITY_MISMATCH,
              EFFECT_GENERATION_MISMATCH, REF_MISMATCH, MARKER_MISMATCH,
              DESIRED_COMMIT_MISMATCH, HEAD_UNPROVEN,
              DURABLE_ASSOCIATION_MISMATCH));
  No other combination is valid. In particular, a non-null proof kind or
  incomplete positive proof cannot be treated as POSITIVE, and fields retained
  on a non-positive row are evidence only and grant no authority.
FOREIGN KEY ChangeRequestSearchMember.proof_create_checkpoint_id
  -> PublicationEffectCheckpoint(publication_effect_checkpoint_id)
FOREIGN KEY ChangeRequestSearchMember.head_evidence_observation_id
  -> ForgeObservation(forge_observation_id)
GUARDED every ChangeRequestSearchMember belongs to a
  CHANGE_REQUEST_SEARCH_RESULT. Within each member_class its ordinals are
  zero-based and contiguous and its rows sort by bytewise adapter-normalized
  change_request_external_id; the cross-class unique prevents one stable ID
  appearing in both lists. Every row binds exact observed_head, deterministic
  source_ref, syntactically valid Run marker, normalized non-Orcest reliance
  digest, exact body revision/marker-set digest, and mechanical ownership union.
  POSITIVE requires Project, registered creator installation/account,
  Publication, Effect generation, stable ID, ref, marker, desired commit,
  observed head, body revision, checkpoint/association, and head-evidence
  Observation to agree without overwrite. ownership_proof_digest covers that
  complete normalized union; member_digest covers every normalized member field
  except parent ID and ordinal
GUARDED a CHANGE_REQUEST_SEARCH_RESULT contains every and only matching object
  from its one complete adapter revision. Open/unmerged objects are LIVE;
  closed/merged objects are TERMINAL. live_cardinality is ZERO, ONE, or
  MULTIPLE according only to LIVE row count. Its duplicate_set_digest is
  exactly
  `"sha256:" + lowercase_hex(SHA256(ascii("orcest-change-request-search-set-v1") || 0x00 || canonical_json({search_revision: external_revision, live: [normalized LIVE member fields including ownership_proof_digest and member_digest in ordinal order], terminal: [normalized TERMINAL member fields including ownership_proof_digest and member_digest in ordinal order]})))`.
  The canonical JSON excludes controller parent IDs and ordinals. Observation,
  both complete memberships, counts/cardinality, payload digest, Request-result
  membership, and accepting Transition commit atomically. Missing/extra rows,
  wrong class/order/nullability, a duplicate ID, or either digest mismatch
  rejects the response before it becomes lifecycle authority. payload_digest
  covers registered source repository, ref, Run marker, search revision,
  live_cardinality, and duplicate_set_digest, thereby binding both child lists
GUARDED reduction uses closed precedence. Any TERMINAL/MERGED/POSITIVE row
  selects the bytewise-lowest such stable ID before every other member and
  invokes the terminal Reservation path below at any live cardinality. Without
  that stronger fact, any INCOMPATIBLE member produces only the positive-evidence
  ownership-conflict path. Otherwise any INCOMPLETE member selects autonomous
  fresh evidence collection/backoff and authorizes no association, terminal
  result, cleanup mutation, or Human Boundary. Ordinary live-cardinality
  routing is legal only when every member is POSITIVE and none is MERGED
GUARDED in PR_MONITORING without cancellation, an active Publication mutation,
  current CLOSE_REDUNDANT_PUBLICATION, or current REPAIR_RUN_MARKER, a
  SEARCH_RESULT whose
  `(complete_search_revision, duplicate_set_digest)` differs from the
  Publication's stored pair first supersedes/fences any current RECONCILE and
  may then plan exactly one effect-fenced duplicate RECONCILE; the same stored
  pair records only the required same-state Transition and plans no work. Under
  those same exclusions, an individual alternate-ID observation
  other than CHANGE_REQUEST_MARKER may supersede an in-flight duplicate
  RECONCILE and schedule the complete read, but never a Fact. A current
  CLOSE_REDUNDANT_PUBLICATION or REPAIR_RUN_MARKER is reduced only by its
  controller-Activity-specific observation rows, so the generic changed-search
  rule cannot plan work through either Activity
GUARDED every CHANGE_REQUEST_MARKER binds stable Change Request ID/head,
  deterministic source ref, exact body revision, canonically ordered Orcest/
  legacy marker occurrences, and marker_set_digest in payload_digest. An
  ordinary monitoring Marker observation has no controller-operation fields;
  a repair-success row follows the stronger exact binding below
GUARDED in PR_MONITORING after cancellation, active Publication mutation,
  CLOSE_REDUNDANT_PUBLICATION, and current REPAIR_RUN_MARKER have been excluded,
  every applicable CHANGE_REQUEST_MARKER Transition first supersedes any
  current RECONCILE. An exact observation proving one desired marker
  plans no work. Exact MISSING or DUPLICATED_IDENTICAL evidence plus the durable
  ownership proof plans one effect-fenced REPAIR_RUN_MARKER. Incomplete or
  conflicting ownership evidence plans the marker-bound ownership RECONCILE;
  no marker observation enters the generic alternate-ID duplicate row
CHECK ForgeObservation.controller_activity_id and controller_operation_digest
  are both NULL or both NOT NULL
CHECK ForgeObservation.terminal_duplicate_cleanup_action_id and
  terminal_cleanup_operation_digest are both NULL or both NOT NULL
CHECK NOT (
  controller_activity_id IS NOT NULL AND
  terminal_duplicate_cleanup_action_id IS NOT NULL
)
CHECK ForgeObservation cleanup union:
  terminal_duplicate_cleanup_reservation_id IS NULL =>
    terminal_duplicate_cleanup_action_id,
    terminal_cleanup_operation_digest IS NULL;
  terminal_duplicate_cleanup_reservation_id IS NOT NULL =>
    target_kind = 'PUBLICATION' AND run_id, publication_id,
    publication_effect_generation, controller_activity_id,
    controller_operation_digest IS NULL AND one of exactly these two forms:
    (a) REQUEST_BACKED_CLEANUP_PROOF_OR_READBACK:
      created_by_forge_observation_request_id IS NOT NULL,
      credential_purpose and credential_secret_ref are both non-null and
      byte-for-byte equal to the creating Request's credential provenance,
      and terminal_duplicate_cleanup_action_id /
      terminal_cleanup_operation_digest are both NULL for proof refresh or
      both non-null for Action readback; or
    (b) DIRECT_TERMINAL_CLEANUP_MUTATION:
      kind is CHANGE_REQUEST_CLOSED or CHANGE_REQUEST_MARKER,
      created_by_forge_observation_request_id IS NULL,
      credential_purpose and credential_secret_ref are both NULL,
      terminal_duplicate_cleanup_action_id and
      terminal_cleanup_operation_digest are both non-null, and the exact
      Reservation, frozen Publication Effect, Cleanup Action, operation
      identity, Outbox, and Observation reciprocal bindings all match.
  In form (a), a non-null Action pair has the same exact Action/operation/
  Outbox/Reservation/Effect bindings; in form (b), every one of those
  bindings is mandatory. No other Reservation-bound credential or creator
  combination is valid
FOREIGN KEY ForgeObservation.controller_activity_id
  -> Activity(activity_id)
FOREIGN KEY ForgeObservation.terminal_duplicate_cleanup_action_id
  -> TerminalDuplicateCleanupAction(terminal_duplicate_cleanup_action_id)
  DEFERRABLE INITIALLY DEFERRED
GUARDED non-null controller operation fields are allowed only on (a) the
  effect-bound CHANGE_REQUEST_SEARCH_RESULT for an exact current PUBLISH/
  INITIAL Effect COMPLETE_MARKER_SEARCH, (b) CHANGE_REQUEST_CLOSED for an exact
  current CLOSE_PUBLICATION or CLOSE_REDUNDANT_PUBLICATION Activity, or (c)
  CHANGE_REQUEST_MARKER for an exact current REPAIR_RUN_MARKER Activity. Every
  shape has target_kind PUBLICATION, the Activity's Run/Publication/current
  effect generation, and operation digest equal to the immutable pre-I/O
  Activity/Effect input. The search-result shape carries the exact registered
  repository/ref/Run marker and complete set proof but does not require a
  linked stable object. A close shape additionally carries the exact stable
  Change Request ID/final head/source ref/Run marker; for
  CLOSE_REDUNDANT_PUBLICATION it equals the one named CLOSE member and never
  the retained Publication.change_request_external_id. A webhook or ordinary
  monitoring poll leaves both fields NULL and cannot terminalize an applicable
  controller Activity or back an Effect checkpoint
GUARDED a Terminal Duplicate Cleanup Action mutation result is the disjoint
  post-terminal exception: terminal_duplicate_cleanup_action_id and
  terminal_cleanup_operation_digest are both non-null and exact, the controller
  Activity pair remains NULL, publication_effect_generation is the
  Reservation's frozen existing Effect, and the Action's operation key/digest/
  outbox plus its reciprocal forge_observation_id relation bind the response.
  A direct terminal-cleanup mutation response uses the
  DIRECT_TERMINAL_CLEANUP_MUTATION form above: its creating Request ID is NULL
  and both copied credential fields are NULL. Its exact Reservation, frozen
  Publication Effect, Cleanup Action, operation identity, mutation Outbox,
  and reciprocal Observation bindings are all mandatory. A request-backed
  Action readback instead has a non-null creating Request and copies both
  credential fields from that Request; its Request/Reservation/Action/
  operation/Effect/Outbox/Observation bindings are likewise exact. A mere
  matching external shape is not sufficient
  CLOSE completion requires a matching CHANGE_REQUEST_CLOSED for the member
  ID/head; DETACH_MARKER requires a matching CHANGE_REQUEST_MARKER proving that
  only this Run marker was removed under the expected head/body/marker-set CAS.
  An ordinary observation with the same external shape cannot complete an
  Action
GUARDED a controller-bound CHANGE_REQUEST_MARKER observation for
  REPAIR_RUN_MARKER copies the exact Project/Publication/effect, deterministic
  ref, stable Change Request ID/head, and operation_digest; its body revision
  succeeds the frozen revision and its normalized marker-set digest proves
  exactly one desired canonical marker and no conflicting v1/legacy marker.
  One writer transaction accepts that observation, completes only the current
  Activity, and appends its FORGE_OBSERVATION Transition without changing
  effect_generation or planning duplicate work. Only after that exact
  controller-bound success predicate fails, and before reducing any marker
  mismatch, cancellation, retained-head advance, merge, and closure precedence
  is applied. After those cases are excluded, a typed pre-call or adapter
  mismatch first persists its exact current observation; only that
  Transition supersedes the repair. A changed CHANGE_REQUEST_SEARCH_RESULT
  then plans duplicate RECONCILE through this repair-specific row, while
  individual mutable evidence schedules a new complete search or ownership
  reconciliation. The generic changed-search rule is inapplicable while the
  repair is current. Ambiguity or restart retains ACTIVE and reuses the same
  operation identity. Unbound observation cannot complete it
GUARDED acceptance of an exact controller-bound CHANGE_REQUEST_CLOSED
  Observation for CLOSE_REDUNDANT_PUBLICATION requires that Activity to remain
  the current ACTIVE cleanup and every frozen complete-search/member/head/
  equivalence/unreviewed/Publication-effect fence to remain true. One writer
  transaction inserts the Observation, marks only that cleanup Activity
  successful, appends its same-state FORGE_OBSERVATION Transition, and plans a
  fresh authenticated complete marker search. It does not reuse the closed
  Activity's Fact or plan RECONCILE from the individual close Observation; only
  a later changed CHANGE_REQUEST_SEARCH_RESULT may plan the next reconciliation.
  It does not change Publication.state, Run terminal outcome, or
  Publication.effect_generation. A typed pre-call or adapter mismatch first
  persists the exact current Forge Observation evidence; only its
  FORGE_OBSERVATION Transition supersedes the stale cleanup. A changed complete
  SEARCH_RESULT may plan fresh RECONCILE through the cleanup-specific mismatch
  row; individual evidence only schedules the complete search. A definitive
  evidence-less controller failure instead inserts a
  failed ControllerOperationFact. Ambiguity or restart leaves the cleanup
  ACTIVE and reconciles the same operation. None of these paths may complete
  the Activity from an unbound observation
UNIQUE ForgeObservation(
  publication_id, publication_effect_generation, forge_observation_id
)
  WHERE publication_id IS NOT NULL
    AND publication_effect_generation IS NOT NULL
GUARDED acceptance of a ForgeObservation with an applicable active run_id and
  its sole FORGE_OBSERVATION Transition commit in the same writer transaction,
  even when from_state = to_state; admission instead consumes both the eligible
  pre-admission WORK_ITEM_SNAPSHOT trigger and the exact trusted BASE_HEAD
  anchor in its sole ADMIT Transition when the Run is created
PRIMARY KEY Publication(publication_id)
UNIQUE Publication(run_id)
UNIQUE Publication(publication_id, effect_generation)
CHECK Publication.effect_generation > 0
CHECK Publication.state IN (
  'PLANNED', 'BRANCH_OBSERVED', 'CHANGE_REQUEST_OBSERVED', 'ACTIVE', 'CLOSED'
)
CHECK Publication initial-link proof union:
  initial_link_search_revision, initial_link_set_digest,
  initial_link_cardinality are all NULL or all NOT NULL;
  initial_link_terminal_state, initial_link_terminal_search_observation_id,
  initial_link_terminal_member_ordinal are all NULL or all NOT NULL;
  initial_link_cardinality IS NULL => initial_link_retained_external_id,
    initial_link_terminal_state, initial_link_terminal_search_observation_id,
    initial_link_terminal_member_ordinal,
    terminal_duplicate_cleanup_reservation_id IS NULL;
  initial_link_terminal_state IS NOT NULL
    => initial_link_retained_external_id IS NOT NULL;
  initial_link_terminal_state IS NULL AND initial_link_cardinality = 'ZERO'
    => initial_link_retained_external_id IS NULL;
  initial_link_terminal_state IS NULL AND
    initial_link_cardinality IN ('ONE', 'MULTIPLE')
    => initial_link_retained_external_id IS NOT NULL;
  initial_link_terminal_state = 'CLOSED'
    => initial_link_cardinality = 'ZERO' AND
       terminal_duplicate_cleanup_reservation_id IS NULL;
  initial_link_terminal_state = 'MERGED'
    => terminal_duplicate_cleanup_reservation_id IS NOT NULL;
  initial_link_terminal_state IS NULL
    => terminal_duplicate_cleanup_reservation_id IS NULL
CHECK non-null Publication.initial_link_cardinality IN (
  'ZERO', 'ONE', 'MULTIPLE'
)
CHECK Publication.initial_link_terminal_state IS NULL OR IN ('MERGED', 'CLOSED')
CHECK Publication.initial_link_terminal_member_ordinal IS NULL OR
  initial_link_terminal_member_ordinal >= 0
FOREIGN KEY Publication.initial_link_terminal_search_observation_id
  -> ForgeObservation(forge_observation_id)
FOREIGN KEY Publication.terminal_duplicate_cleanup_reservation_id
  -> TerminalDuplicateCleanupReservation(
       terminal_duplicate_cleanup_reservation_id
     ) DEFERRABLE INITIALLY DEFERRED
CHECK Publication.last_duplicate_reconciliation_fact_id,
  last_duplicate_search_revision, and last_duplicate_set_digest are all NULL
  or all NOT NULL
FOREIGN KEY Publication.last_duplicate_reconciliation_fact_id
  -> ReconciliationFact(reconciliation_fact_id)
  DEFERRABLE INITIALLY DEFERRED
GUARDED a non-null last duplicate projection names the latest same-Publication
  REDUNDANT_PUBLICATIONS_PROVEN or NO_ACTIONABLE_DUPLICATE Fact and copies its
  complete_search_revision and duplicate_set_digest exactly. The projection updates atomically with that
  Fact's Transition and is historical replay/gating authority
GUARDED before change_request_external_id is first set, or any merge/close
  observation can become terminal authority, the current INITIAL Effect has an
  OBSERVED_SATISFIED COMPLETE_MARKER_SEARCH checkpoint naming the exact
  CHANGE_REQUEST_SEARCH_RESULT whose revision/set digest/cardinality are copied
  to the Publication. Cardinality counts only the Observation's LIVE child
  rows; its full-set digest covers both LIVE and TERMINAL memberships. ZERO
  with no TERMINAL row permits only absence/create reconciliation followed by
  another fresh COMPLETE_MARKER_SEARCH; search or create output never links.
  Only after every member is POSITIVE and no TERMINAL/MERGED row exists, ONE
  requires initial_link_retained_external_id to be the sole LIVE stable ID and
  only a fresh exact-object observation may link it; positive CLOSED terminal
  rows remain audit-only. Under those same prerequisites, MULTIPLE retains the
  bytewise-lowest LIVE stable ID as audit/
  cleanup choice but forbids linkage or terminalization until cleanup plus a
  fresh complete search proves ONE
GUARDED the first NULL-to-non-NULL change_request_external_id transaction
  either (a) requires initial_link_cardinality = ONE and a fresh exact-object
  read and sets the ID equal to the sole LIVE retained ID, or (b) is the
  guarded positive terminal-selection transaction below. The later separately guarded
  autonomous duplicate-repair reassociation is the only other v1 exception;
  it does not rewrite the historical initial-link proof
GUARDED the Publication terminal-selection group is non-null only for the
  closed ownership-precedence branches. Any POSITIVE MERGED TERMINAL member
  wins at every LIVE cardinality; the writer selects the bytewise-lowest such
  stable ID, sets terminal state MERGED, and creates the reciprocal Terminal
  Duplicate Cleanup Reservation for every LIVE member. Without a positive
  merged member, ZERO LIVE with positive CLOSED TERMINAL members selects the
  bytewise-lowest closed stable ID and has no Reservation. The retained ID and
  observed_remote_commit copy the selected row; terminal state, exact current
  SEARCH_RESULT ID, and TERMINAL member ordinal resolve to that child row. The
  same transaction sets the Publication association/state CLOSED, appends
  COMPLETE, completes/fences PUBLISH and every unfinished work item, and sets
  the Run outcome MERGED or CLOSED. It creates no ref or Change Request;
  ownership/status precedence then bytewise ID—not arrival order—selects it
GUARDED every terminal Run with a linked Change Request sets
  Publication.state = 'CLOSED' in the same transaction. Run.terminal_outcome
  plus the exact merge, close, cancellation observation/fact, or the guarded
  complete-search terminal member distinguishes MERGED, CLOSED, and
  CANCELLED; Publication does not invent parallel terminal reasons
GUARDED while the owning Run is nonterminal, every legacy selector excludes
  both Publication.change_request_external_id when non-null and the
  deterministic source ref unconditionally. This engine-ownership projection
  does not depend on finding, parsing, or repairing a body marker and survives
  controller restart. Terminalization does not release the selected ID/ref or
  any unresolved member ID/ref while the reciprocal Terminal Duplicate Cleanup
  Reservation is ACTIVE; each remains excluded until its CLOSE,
  MARKER_DETACHED, or RETAINED_AUDIT outcome, and a still-present syntactically
  valid marker remains independently excluded. Otherwise only terminal outcome
  or an explicit authenticated migration releases legacy exclusion
PRIMARY KEY TerminalDuplicateCleanupReservation(
  terminal_duplicate_cleanup_reservation_id
)
UNIQUE TerminalDuplicateCleanupReservation(
  selected_search_observation_id, selected_merged_member_ordinal
)
UNIQUE TerminalDuplicateCleanupReservation(publication_id)
FOREIGN KEY TerminalDuplicateCleanupReservation.project_id
  -> Project(project_id)
FOREIGN KEY TerminalDuplicateCleanupReservation.run_id -> Run(run_id)
FOREIGN KEY TerminalDuplicateCleanupReservation.publication_id
  -> Publication(publication_id) DEFERRABLE INITIALLY DEFERRED
FOREIGN KEY TerminalDuplicateCleanupReservation.selected_search_observation_id
  -> ForgeObservation(forge_observation_id)
FOREIGN KEY TerminalDuplicateCleanupReservation(
  publication_id, proof_publication_effect_generation
) -> PublicationEffect(publication_id, effect_generation)
FOREIGN KEY TerminalDuplicateCleanupReservation(
  run_id, created_transition_sequence
) -> Transition(run_id, transition_sequence)
CHECK TerminalDuplicateCleanupReservation.state IN ('ACTIVE', 'COMPLETED')
CHECK TerminalDuplicateCleanupReservation.member_count >= 0
CHECK TerminalDuplicateCleanupReservation.next_member_ordinal >= 0 AND
  next_member_ordinal <= member_count
CHECK TerminalDuplicateCleanupReservation completion union:
  state = 'ACTIVE' => next_member_ordinal < member_count AND
                      completed_at_ms IS NULL;
  state = 'COMPLETED' => next_member_ordinal = member_count AND
                         completed_at_ms IS NOT NULL
GUARDED Run terminalization does not cascade-delete, detach, or complete this
  Reservation. The Reservation, its selected Search Observation/member proof,
  every ordered LIVE member, current Action/Outbox, and Reservation-bound
  Schedule/Request remain durable until the cursor reaches member_count;
  terminal-Run collection and ordinary Run Schedule closure must preserve this
  graph. Only the Reservation's own ordered cleanup continuation may advance
  next_member_ordinal or mark it COMPLETED.
GUARDED selected_search_observation_id/member ordinal resolves to the exact
  TERMINAL/MERGED/POSITIVE ChangeRequestSearchMember chosen by the owning
  Publication: it is the bytewise-lowest stable ID among every such row in that
  Observation. Selected ID/head/merge commit, Effect generation, registered
  creator installation/account, deterministic ref, Run marker, complete-search
  revision, and duplicate_set_digest copy that row/Observation exactly.
  reservation_digest covers all immutable fields plus members_digest
PRIMARY KEY TerminalDuplicateCleanupMember(
  terminal_duplicate_cleanup_reservation_id, member_ordinal
)
UNIQUE TerminalDuplicateCleanupMember(
  terminal_duplicate_cleanup_reservation_id, change_request_external_id
)
FOREIGN KEY TerminalDuplicateCleanupMember.
  terminal_duplicate_cleanup_reservation_id
  -> TerminalDuplicateCleanupReservation(
       terminal_duplicate_cleanup_reservation_id
     )
CHECK TerminalDuplicateCleanupMember.planned_action IN (
  'CLOSE', 'DETACH_MARKER', 'RECORD_ONLY'
)
CHECK TerminalDuplicateCleanupMember.ownership_status IN (
  'POSITIVE', 'INCOMPATIBLE', 'INCOMPLETE'
)
GUARDED Reservation members are zero-based contiguous and reproduce member_count
  and members_digest. They contain every and only LIVE member of the selected
  Search Observation in the same bytewise stable-ID order; search_member_ordinal,
  stable ID, head, body revision, marker-set digest, ownership status/proof,
  and reliance digest copy the source row. CLOSE requires POSITIVE and canonical-
  empty external reliance; DETACH_MARKER requires POSITIVE, nonempty reliance,
  and exact head/body/marker CAS evidence; INCOMPATIBLE, INCOMPLETE, or another
  unsafe row is RECORD_ONLY. member_digest covers the complete normalized copy
PRIMARY KEY TerminalDuplicateCleanupAction(
  terminal_duplicate_cleanup_action_id
)
UNIQUE TerminalDuplicateCleanupAction(
  terminal_duplicate_cleanup_reservation_id, member_ordinal, action_generation
)
FOREIGN KEY TerminalDuplicateCleanupAction(
  terminal_duplicate_cleanup_reservation_id, member_ordinal
) -> TerminalDuplicateCleanupMember(
       terminal_duplicate_cleanup_reservation_id, member_ordinal
     )
FOREIGN KEY TerminalDuplicateCleanupAction.outbox_id -> Outbox(outbox_id)
  DEFERRABLE INITIALLY DEFERRED
FOREIGN KEY TerminalDuplicateCleanupAction.forge_observation_id
  -> ForgeObservation(forge_observation_id)
CHECK TerminalDuplicateCleanupAction.action_generation > 0
CHECK TerminalDuplicateCleanupAction.action_input_digest IS NOT NULL
CHECK TerminalDuplicateCleanupAction.action_kind IN (
  'CLOSE', 'DETACH_MARKER', 'RECORD_ONLY'
)
CHECK TerminalDuplicateCleanupAction.record_reason IS NULL OR IN (
  'EXTERNAL_RELIANCE', 'INCOMPLETE_PROOF', 'INCOMPATIBLE_OWNER', 'CAS_UNSAFE'
)
CHECK TerminalDuplicateCleanupAction.state IN (
  'PENDING', 'ACTIVE', 'COMPLETED', 'SUPERSEDED'
)
CHECK TerminalDuplicateCleanupAction.outcome IS NULL OR IN (
  'CLOSED', 'MARKER_DETACHED', 'RETAINED_AUDIT'
)
CHECK TerminalDuplicateCleanupAction input union:
  action_kind IN ('CLOSE', 'DETACH_MARKER') => record_reason IS NULL AND
    expected_head, expected_body_revision, expected_marker_set_digest,
    operation_idempotency_key, operation_digest, outbox_id IS NOT NULL;
  action_kind = 'RECORD_ONLY' => record_reason IS NOT NULL AND
    expected_head, expected_body_revision, expected_marker_set_digest,
    operation_idempotency_key, operation_digest, outbox_id IS NULL
CHECK TerminalDuplicateCleanupAction result union:
  state IN ('PENDING', 'ACTIVE') => outcome, forge_observation_id,
                                    completed_at_ms, result_digest IS NULL;
  state = 'COMPLETED' => outcome, completed_at_ms,
                         result_digest IS NOT NULL;
  outcome = 'CLOSED' => action_kind = 'CLOSE' AND
                        forge_observation_id IS NOT NULL;
  outcome = 'MARKER_DETACHED' => action_kind = 'DETACH_MARKER' AND
                                 forge_observation_id IS NOT NULL;
  outcome = 'RETAINED_AUDIT' => action_kind = 'RECORD_ONLY' AND
                                forge_observation_id IS NULL;
  state = 'SUPERSEDED' => outcome IS NULL AND
                          forge_observation_id, completed_at_ms,
                          result_digest IS NOT NULL
UNIQUE TerminalDuplicateCleanupAction(
  terminal_duplicate_cleanup_reservation_id
) WHERE state IN ('PENDING', 'ACTIVE')
GUARDED exactly one nonterminal Action exists for the Reservation's current
  next_member_ordinal. CLOSE and DETACH_MARKER copy the Member CAS preimage and
  commit their reciprocal effect-fenced Outbox before I/O. RECORD_ONLY commits
  terminal immediately without an Outbox/Observation. action_input_digest
  covers the immutable input; the separate result_digest covers the complete
  normalized terminal projection and is present exactly for COMPLETED or
  SUPERSEDED. Same member/generation replay is exact and a higher generation
  follows only persisted mismatch evidence
GUARDED Publication.terminal_duplicate_cleanup_reservation_id and the
  Reservation.publication_id are reciprocal. Reservation, every immutable
  Member, the selected merged Publication association/proof, COMPLETE
  checkpoint, terminal MERGED Transition, closure of ordinary Run schedules,
  and either zero-member completion or creation/retention of the Reservation-
  bound cleanup schedules plus first Action/outbox commit atomically. Semantic
  work is already fenced; this cleanup is post-terminal controller
  reconciliation and cannot reopen or rewrite the selected merge
PRIMARY KEY PublicationEffect(publication_id, effect_generation)
CHECK PublicationEffect.effect_generation > 0
CHECK PublicationEffect.mode IN ('INITIAL', 'UPDATE')
CHECK PublicationEffect.base_movement_policy IN (
  'REBASE_BEFORE_PUBLICATION', 'PIN', 'SUPERSEDE_AT_BOUNDARY'
)
UNIQUE PublicationEffect(activity_id)
FOREIGN KEY PublicationEffect.publication_id -> Publication(publication_id)
FOREIGN KEY PublicationEffect.activity_id -> Activity(activity_id)
FOREIGN KEY PublicationEffect.publication_secret_ref
  -> SecretVersion(secret_id, version)
GUARDED PublicationEffect.publication_secret_ref is non-null, its secret_id
  equals the Project's immutable logical publication_secret_id, and its
  version is the exact current SecretRef version frozen under the Secret Store
  writer lock in the same transaction as the Effect/Activity/outbox. Every
  forge mutation rematerializes only this version; rotation affects only a
  later Effect generation
GUARDED UPDATE PublicationEffect.expected_remote_commit equals the exact
  ordered Change Request head on which its post-link remediation/import/rebase
  chain was admitted, or a later head explicitly revalidated in the Effect
  transaction; that head is included in operation_digest
GUARDED PublicationEffect.base_movement_policy equals the creating Activity's
  installed Snapshot policy and operation_digest covers it
DEFERRABLE FOREIGN KEY Publication(publication_id, effect_generation)
  -> PublicationEffect(publication_id, effect_generation)
PRIMARY KEY PublicationEffectCheckpoint(publication_effect_checkpoint_id)
UNIQUE PublicationEffectCheckpoint(
  publication_id, effect_generation, checkpoint_sequence
)
CHECK PublicationEffectCheckpoint.checkpoint_sequence > 0
UNIQUE PublicationEffectCheckpoint(
  publication_id, effect_generation, suboperation_kind, status,
  request_idempotency_key
)
  WHERE request_idempotency_key IS NOT NULL
UNIQUE PublicationEffectCheckpoint(
  publication_id, effect_generation, suboperation_kind, status,
  forge_observation_id
)
  WHERE forge_observation_id IS NOT NULL
UNIQUE PublicationEffectCheckpoint(publication_id, effect_generation, status)
  WHERE status = 'COMPLETED'
FOREIGN KEY PublicationEffectCheckpoint(publication_id, effect_generation)
  -> PublicationEffect(publication_id, effect_generation)
FOREIGN KEY PublicationEffectCheckpoint(
  publication_id, effect_generation, forge_observation_id
)
  -> ForgeObservation(
       publication_id, publication_effect_generation, forge_observation_id
     )
CHECK PublicationEffectCheckpoint.suboperation_kind IN (
  BASE_READ_PRE, REF_READ, REF_CREATE, REF_UPDATE,
  COMPLETE_MARKER_SEARCH, CHANGE_REQUEST_SEARCH, CHANGE_REQUEST_CREATE,
  BASE_READ_POST, COMPLETE
)
CHECK PublicationEffectCheckpoint.status IN (
  REQUEST_READY, OBSERVED_ABSENT, OBSERVED_SATISFIED, AMBIGUOUS,
  BASE_MISMATCH, CAS_MISMATCH, COMPLETED
)
CHECK PublicationEffectCheckpoint status matrix:
  BASE_READ_PRE  => status IN (OBSERVED_SATISFIED, BASE_MISMATCH),
                    request key forbidden, observation/revision required
  REF_READ       => status IN (OBSERVED_ABSENT, OBSERVED_SATISFIED),
                    request key forbidden, observation/revision required
  REF_CREATE     => REQUEST_READY or AMBIGUOUS requires request key and
                    forbids observation/revision; OBSERVED_SATISFIED or
                    CAS_MISMATCH requires observation/revision and permits a
                    request key only when that exact request was issued
  REF_UPDATE     => the same status/nullability matrix as REF_CREATE
  COMPLETE_MARKER_SEARCH => status = OBSERVED_SATISFIED,
                            request key forbidden,
                            exact CHANGE_REQUEST_SEARCH_RESULT observation and
                            revision required
  CHANGE_REQUEST_SEARCH => status IN (
                            OBSERVED_ABSENT, OBSERVED_SATISFIED
                          ), request key forbidden,
                          observation/revision required
  CHANGE_REQUEST_CREATE => REQUEST_READY or AMBIGUOUS requires request key
                           and forbids observation/revision;
                           OBSERVED_SATISFIED requires observation/revision and
                           permits a request key only when that exact request
                           was issued
  BASE_READ_POST => status IN (OBSERVED_SATISFIED, BASE_MISMATCH),
                    request key forbidden, observation/revision required
  COMPLETE       => status = COMPLETED, request key forbidden,
                    observation/revision required and names either the final
                    linked Change Request/head after ONE LIVE or the exact
                    current CHANGE_REQUEST_SEARCH_RESULT selected by the
                    positive terminal branch
GUARDED PublicationEffect mode/suboperation matrix:
  INITIAL => BASE_READ_PRE, REF_READ, optional REF_CREATE or REF_UPDATE,
             COMPLETE_MARKER_SEARCH; ownership precedence first selects a
             positive MERGED terminal at any LIVE cardinality, ownership
             conflict, or incomplete-proof reread/backoff. With every member
             POSITIVE and none merged: ZERO LIVE/no terminal performs
             CHANGE_REQUEST_SEARCH, optional CHANGE_REQUEST_CREATE, then fresh
             COMPLETE_MARKER_SEARCH; ONE LIVE performs fresh exact-object read,
             BASE_READ_POST, COMPLETE; MULTIPLE LIVE repeats one proof-bound
             cleanup plus fresh COMPLETE_MARKER_SEARCH until ONE; ZERO LIVE
             with positive CLOSED terminal members appends COMPLETE and
             terminates without search/create
  UPDATE  => REF_READ, REF_UPDATE, COMPLETE
GUARDED checkpoint_sequence follows the above code-owned order; an omitted
  optional mutation is represented by its read/search observation, never a
  fabricated checkpoint
GUARDED CHANGE_REQUEST_SEARCH/OBSERVED_ABSENT references only an exact
  CHANGE_REQUEST_ABSENT Forge Observation with matching Project/repository,
  Publication/effect, ref, Run marker, search revision, and stable
  nonexistence token. OBSERVED_SATISFIED references exact discovery evidence
GUARDED COMPLETE_MARKER_SEARCH/OBSERVED_SATISFIED references only an exact
  effect-readback CHANGE_REQUEST_SEARCH_RESULT for this Project/ref/Run marker,
  with the same Publication/effect/PUBLISH Activity/operation digest. Its normalized
  complete-search revision/full-set digest and ZERO/ONE/MULTIPLE LIVE
  cardinality copy to the Publication. A positive MERGED terminal records the
  deterministic selected proof and terminalizes at any cardinality. Otherwise,
  only after every member is POSITIVE, ONE/MULTIPLE retain the sole or bytewise-
  lowest LIVE ID and ZERO plus positive CLOSED terminal rows records the closed
  proof. MULTIPLE without a positive merged member cannot advance to linkage or
  terminal authority and must complete duplicate cleanup plus a fresh search
GUARDED while an INITIAL PUBLISH is suspended at an all-POSITIVE,
  no-MERGED COMPLETE_MARKER_SEARCH/MULTIPLE branch, the current PUBLISH Activity may coexist
  with at most one subordinate current duplicate RECONCILE or
  CLOSE_REDUNDANT_PUBLICATION Activity. That subordinate work is bound to the
  same Publication/Effect/search proof, cannot emit a Publication mutation,
  and no other worker/controller Activity may be current for the Run. The
  PUBLISH outbox/checkpoint remains suspended until a fresh complete search
  resolves the branch
GUARDED BASE_READ_PRE, REF_READ, COMPLETE_MARKER_SEARCH,
  CHANGE_REQUEST_SEARCH, and BASE_READ_POST may
  repeat only with a new Forge Observation and higher sequence within their
  original phase; the explicit initial-publication loop additionally permits a
  fresh COMPLETE_MARKER_SEARCH after CHANGE_REQUEST_SEARCH, after every
  terminal response/ambiguity reconciliation for CHANGE_REQUEST_CREATE, and
  after one completed duplicate cleanup. No other earlier-phase repeat follows
  a later mutation/phase except REF_READ immediately before its paired ref-
  mutation retry, and BASE_READ_POST may repeat only after the provisional
  Change Request is observed
GUARDED BASE_READ_PRE/BASE_READ_POST result is fixed by the effect's installed
  base policy and observed trusted commit:
  every policy + observed commit equals reviewed base_commit
    => OBSERVED_SATISFIED;
  PIN + any successful trusted-base read
    => OBSERVED_SATISFIED and effect may continue;
  REBASE_BEFORE_PUBLICATION + differing commit
    => BASE_MISMATCH, superseded effect, exact-base REBASE/full gate;
  SUPERSEDE_AT_BOUNDARY + differing commit
    => BASE_MISMATCH, superseded effect, base-only Snapshot whose
       supersession_key covers that commit and pending pointer commit as
       effects of the FORGE_OBSERVATION Transition; a separately replayable
       SPEC_SUPERSEDE Transition then installs it and plans REPLAN;
  a BASE_READ_POST mismatch in either non-PIN branch retains the exact owned
  provisional ref/Change Request/head for a higher INITIAL effect;
  a higher INITIAL effect that already owns that provisional object still runs
  BASE_READ_PRE before any ref mutation. If that read mismatches, it is
  superseded before mutation while retaining the same provisional object/head,
  and the pinned policy deterministically repeats REBASE/full gate or
  SPEC_SUPERSEDE/full replan. Absence of a provisional object is not a
  precondition for this fence
GUARDED observed_external_revision equals the referenced
  ForgeObservation.external_revision
PRIMARY KEY ReconciliationFact(reconciliation_fact_id)
UNIQUE ReconciliationFact(activity_id)
FOREIGN KEY ReconciliationFact(run_id, activity_id)
  -> Activity(run_id, activity_id)
CHECK ReconciliationFact.kind IN (
  'EFFECT_PRESENT', 'EFFECT_ABSENT', 'PRELINK_REF_IMPORTABLE',
  'PRELINK_REF_RECONSTRUCT_REQUIRED', 'REDUNDANT_PUBLICATIONS_PROVEN',
  'NO_ACTIONABLE_DUPLICATE', 'OWNERSHIP_CONFLICT'
)
CHECK ReconciliationFact publication_id and effect_generation
  are both NULL or both NOT NULL
FOREIGN KEY ReconciliationFact(publication_id, effect_generation)
  -> PublicationEffect(publication_id, effect_generation)
FOREIGN KEY ReconciliationFact.retained_observation_id
  -> ForgeObservation(forge_observation_id)
PRIMARY KEY ReconciliationFactObservation(
  reconciliation_fact_id, observation_ordinal
)
UNIQUE ReconciliationFactObservation(
  reconciliation_fact_id, forge_observation_id
)
FOREIGN KEY ReconciliationFactObservation.reconciliation_fact_id
  -> ReconciliationFact(reconciliation_fact_id)
FOREIGN KEY ReconciliationFactObservation.forge_observation_id
  -> ForgeObservation(forge_observation_id)
GUARDED each Reconciliation Fact has at least one observation membership;
  ordinals are zero-based, contiguous, and sort the IDs canonically
CHECK ReconciliationFact field matrix:
  PRELINK_REF_IMPORTABLE => observed_ref_commit, safe_fetch_proof_digest,
                            candidate_admission_proof_digest IS NOT NULL;
                            validation_failure_digest IS NULL
  PRELINK_REF_RECONSTRUCT_REQUIRED => observed_ref_commit,
                                      safe_fetch_proof_digest,
                                      validation_failure_digest IS NOT NULL;
                                      candidate_admission_proof_digest IS NULL
  REDUNDANT_PUBLICATIONS_PROVEN => retained_change_request_external_id,
                                   retained_head, retained_observation_id,
                                   complete_search_revision,
                                   duplicate_members_digest,
                                   duplicate_set_digest IS NOT NULL;
                                   observed_ref_commit,
                                   pinned_base_relationship,
                                   safe_fetch_proof_digest,
                                   candidate_admission_proof_digest,
                                   validation_failure_digest IS NULL
  NO_ACTIONABLE_DUPLICATE => complete_search_revision,
                             duplicate_set_digest IS NOT NULL;
                             observed_ref_commit,
                             pinned_base_relationship,
                             safe_fetch_proof_digest,
                             candidate_admission_proof_digest,
                             validation_failure_digest,
                             retained_change_request_external_id,
                             retained_head, retained_observation_id,
                             duplicate_members_digest IS NULL
  all other kinds => observed_ref_commit, pinned_base_relationship,
                     safe_fetch_proof_digest,
                     candidate_admission_proof_digest,
                     validation_failure_digest,
                     retained_change_request_external_id, retained_head,
                     retained_observation_id, complete_search_revision,
                     duplicate_members_digest, duplicate_set_digest IS NULL
CHECK ReconciliationFact.pinned_base_relationship IS NULL OR IN (
  'EXACT_PINNED_BASE', 'DESCENDANT_OF_PINNED_BASE',
  'DIVERGED_FROM_PINNED_BASE', 'UNPROVEN'
)
GUARDED either pre-link kind requires pinned_base_relationship and proves safe
  registered-source fetch plus no incompatible marker, legacy/human owner, or
  Change Request; PRELINK_REF_IMPORTABLE additionally requires
  EXACT_PINNED_BASE or DESCENDANT_OF_PINNED_BASE and ordinary Candidate
  admission proof
GUARDED either pre-link kind's observed_ref_commit equals the exact commit in
  its causal REF_HEAD ForgeObservation membership and the immutable input of
  its producing RECONCILE Activity
GUARDED OWNERSHIP_CONFLICT has positive incompatible ownership evidence after
  autonomous store/backup/forge repair; absence, timeout, forge unavailability,
  or duplicate objects carrying the same syntactically valid Orcest
  Run/Publication marker are insufficient. For same-marker duplicates, the
  reconciler deterministically retains the lowest stable external ID and may
  close another only after exact evidence proves it equivalent and unreviewed.
  Ambiguity produces typed recovery/wait evidence. Only positive incompatible
  ownership may create the Human Boundary
GUARDED NO_ACTIONABLE_DUPLICATE proves one complete canonical same-marker
  search at complete_search_revision and has no duplicate-member child rows,
  retained-association mutation, cleanup Activity, or Publication Effect.
  duplicate_set_digest equals the digest of its bytewise-ordered normalized
  result identities, heads, marker/ref equivalence, and reliance states,
  including the canonical empty set. The Fact transaction stores its exact
  `(complete_search_revision, duplicate_set_digest)` and Fact pointer on the
  Publication. That same pair cannot plan another duplicate RECONCILE; only a
  later complete proof whose revision or digest differs may do so. This outcome
  is forbidden before first linkage while initial_link_cardinality is
  MULTIPLE: all safely closable members require REDUNDANT_PUBLICATIONS_PROVEN;
  positive reliance/incompatible ownership requires OWNERSHIP_CONFLICT; and
  incomplete or unavailable evidence waits/retries without a Fact
GUARDED either duplicate-search kind has one exact causal
  CHANGE_REQUEST_SEARCH_RESULT in its observation membership and producing
  RECONCILE input. complete_search_revision and duplicate_set_digest copy that
  Observation exactly. No discovery/head/feedback/marker/merge/close row may
  substitute as complete-set proof. A later accepted individual object
  observation other than CHANGE_REQUEST_MARKER may, under the generic-row
  exclusions, supersede an in-flight duplicate RECONCILE and request a fresh
  complete search, but cannot authorize a Fact from a fabricated set digest.
  CHANGE_REQUEST_MARKER uses the marker-specific supersession/repair/ownership
  guards and never this generic invalidation rule
PRIMARY KEY ReconciliationDuplicateMember(
  reconciliation_fact_id, member_ordinal
)
UNIQUE ReconciliationDuplicateMember(
  reconciliation_fact_id, change_request_external_id
)
FOREIGN KEY ReconciliationDuplicateMember.reconciliation_fact_id
  -> ReconciliationFact(reconciliation_fact_id)
FOREIGN KEY ReconciliationDuplicateMember.identity_observation_id
  -> ForgeObservation(forge_observation_id)
FOREIGN KEY ReconciliationDuplicateMember.unreviewed_observation_id
  -> ForgeObservation(forge_observation_id)
CHECK ReconciliationDuplicateMember.disposition IN ('RETAIN', 'CLOSE')
GUARDED a REDUNDANT_PUBLICATIONS_PROVEN Fact has at least two child rows with
  zero-based contiguous ordinals sorted by bytewise adapter-normalized stable
  change_request_external_id. Ordinal zero is the sole RETAIN row and every
  later row is CLOSE. RETAIN has NULL unreviewed_observation_id and
  unreviewed_proof_revision; every CLOSE row has both non-null. Every row's
  observed_head equals its identity observation, every named observation is in
  the parent Fact's observation membership, and equivalence_proof_digest binds
  the exact Project/ref/marker/Publication/head identity
GUARDED ReconciliationFact.retained_change_request_external_id equals ordinal
  zero's change_request_external_id, retained_head equals ordinal zero's
  observed_head, and retained_observation_id equals ordinal zero's
  identity_observation_id. Every duplicate child names a LIVE member of the
  exact causal complete-search Observation; its LIVE membership contains only
  open, unmerged objects carrying the same valid v1 marker, registered Project,
  deterministic ref, and current head at one adapter search revision, and every
  source Search Member has ownership_status POSITIVE. A positive MERGED
  terminal would already have won terminal precedence; INCOMPATIBLE or
  INCOMPLETE input cannot create either duplicate Fact. TERMINAL
  search members remain complete audit input but never become RETAIN/CLOSE
  children. Each CLOSE member has current proof of no non-Orcest review,
  discussion, merge, or reliance. duplicate_members_digest equals
  sha256(canonical_json(ordered child rows excluding parent ID)), while
  duplicate_set_digest and complete_search_revision equal the exact causal
  CHANGE_REQUEST_SEARCH_RESULT full-set digest covering both LIVE and TERMINAL
  memberships. Fact, observation membership, child rows,
  successful RECONCILE completion, and
  T(RECONCILIATION_FACT, reconciliation_fact_id) commit atomically. Before
  first linkage, that transaction MUST leave
  Publication.change_request_external_id NULL: the retained ID is only the
  deterministic cleanup candidate, and at most the first CLOSE-member cleanup
  Activity/outbox commits with the Fact. After linkage, a current exact
  post-link reconciliation may retain or update the Publication association
  only under its ordinary stable-ID/head/effect guards; it is not an
  unconditional REDUNDANT Fact effect. A later member is never scheduled from
  the old Fact
PRIMARY KEY ControllerOperationFact(controller_operation_fact_id)
UNIQUE ControllerOperationFact(activity_id)
FOREIGN KEY ControllerOperationFact(run_id, activity_id)
  -> Activity(run_id, activity_id)
CHECK ControllerOperationFact.kind IN (
  'IMPORT', 'PUBLISH', 'CLOSE_PUBLICATION',
  'CLOSE_REDUNDANT_PUBLICATION', 'REPAIR_RUN_MARKER', 'RECONCILE'
)
CHECK ControllerOperationFact.outcome IN ('SUCCEEDED', 'FAILED')
CHECK ControllerOperationFact outcome matrix:
  SUCCEEDED => failure_category, failure_evidence_digest IS NULL
  FAILED    => failure_category, failure_evidence_digest IS NOT NULL
               AND output_candidate_id IS NULL
  PUBLISH, CLOSE_REDUNDANT_PUBLICATION, REPAIR_RUN_MARKER, RECONCILE
    => outcome = FAILED
CHECK ControllerOperationFact failure category matrix:
  outcome = SUCCEEDED => failure_category IS NULL
  kind = IMPORT AND outcome = FAILED => failure_category IN (
    'SOURCE_READ', 'BASE_CONFLICT', 'CREDENTIAL', 'STORAGE',
    'INTEGRITY_SUSPECTED', 'POLICY'
  )
  kind IN ('PUBLISH', 'RECONCILE') AND outcome = FAILED =>
    failure_category IN (
      'BASE_CONFLICT', 'CREDENTIAL', 'STORAGE',
      'INTEGRITY_SUSPECTED', 'POLICY'
    )
  kind IN (
    'CLOSE_PUBLICATION', 'CLOSE_REDUNDANT_PUBLICATION', 'REPAIR_RUN_MARKER'
  ) AND outcome = FAILED => failure_category IN (
    'BASE_CONFLICT', 'CREDENTIAL', 'POLICY'
  )
GUARDED FORGE_TRANSIENT is not a ControllerOperationFact category. Temporary
  read/search/poll failure uses ForgeRequestFailureFact; ambiguous write or
  response loss remains under checkpointed reconciliation. A failed Fact's
  same-transaction RecoveryEvidence copies failure_category exactly, with no
  adapter-text remapping
CHECK ControllerOperationFact successful output matrix:
  IMPORT => output_candidate_id IS NOT NULL
  CLOSE_PUBLICATION => output_candidate_id IS NULL AND exact Forge Observation
                       membership contains exactly one matching
                       CHANGE_REQUEST_ABSENT evidence
                       proving stable marker/ref/search-revision/nonexistence
                       before any linked Change Request was observed; every
                       failure-only field is NULL and no ref/head/Change-
                       Request-success field is populated
FOREIGN KEY ControllerOperationFact.output_candidate_id
  -> Candidate(candidate_id)
PRIMARY KEY ControllerOperationFactObservation(
  controller_operation_fact_id, observation_ordinal
)
UNIQUE ControllerOperationFactObservation(
  controller_operation_fact_id, forge_observation_id
)
FOREIGN KEY ControllerOperationFactObservation.controller_operation_fact_id
  -> ControllerOperationFact(controller_operation_fact_id)
FOREIGN KEY ControllerOperationFactObservation.forge_observation_id
  -> ForgeObservation(forge_observation_id)
GUARDED ControllerOperationFact.kind equals the Activity kind; operation_digest
  equals the identity committed before external I/O; observation membership is
  canonical; Fact insertion atomically terminalizes that Activity
FOREIGN KEY publication ForgeObservation(
  publication_id, publication_effect_generation
)
  -> PublicationEffect(publication_id, effect_generation)
PRIMARY KEY WaitCondition(wait_condition_id)
UNIQUE WaitCondition(run_id, wait_condition_id)
CHECK WaitCondition.reason IN (
  'CAPACITY', 'RATE_LIMIT', 'BUDGET', 'BACKOFF', 'EXTERNAL_DEPENDENCY',
  'FORGE_UNAVAILABLE', 'STORAGE_RECOVERY', 'SECRET_RECOVERY', 'EVIDENCE'
)
CHECK WaitCondition.wake_kind IS NULL OR wake_kind IN (
  'CAPACITY', 'RATE_LIMIT_RESET', 'BUDGET_WINDOW', 'DEPENDENCY',
  'FORGE', 'STORAGE', 'SECRET', 'EVIDENCE'
)
CHECK WaitCondition has not_before_ms IS NOT NULL OR wake_kind IS NOT NULL
CHECK WaitCondition wake_kind and wake_identity are both NULL or both NOT NULL
CHECK WaitCondition.health_observation_ids_digest IS NOT NULL
CHECK WaitCondition.panel_slots_digest IS NOT NULL
CHECK WaitCondition reason/wake compatibility matrix:
  CAPACITY => wake_kind = 'CAPACITY' AND not_before_ms IS NULL
  RATE_LIMIT => not_before_ms IS NOT NULL AND
                (wake_kind IS NULL OR wake_kind = 'RATE_LIMIT_RESET')
  BUDGET => not_before_ms IS NOT NULL AND wake_kind = 'BUDGET_WINDOW'
  BACKOFF => not_before_ms IS NOT NULL AND wake_kind, wake_identity IS NULL
  EXTERNAL_DEPENDENCY => wake_kind = 'DEPENDENCY'
  FORGE_UNAVAILABLE => not_before_ms IS NOT NULL AND wake_kind = 'FORGE'
  STORAGE_RECOVERY => wake_kind = 'STORAGE'
  SECRET_RECOVERY => wake_kind = 'SECRET'
  EVIDENCE => not_before_ms IS NOT NULL AND wake_kind = 'EVIDENCE'
GUARDED EXTERNAL_DEPENDENCY, STORAGE_RECOVERY, and SECRET_RECOVERY may have a
  bounded non-null not_before_ms as their specified reconciliation fallback;
  otherwise it is NULL. No other reason/wake/timer combination is valid
GUARDED condition_digest covers every normalized immutable predicate/binding,
  both membership digests, created_from_kind/id, and creating Transition; it
  excludes only informational created_at_ms
GUARDED when both not_before_ms and a wake predicate are present, satisfaction
  is OR: either a due WAIT_CONDITION_NOT_BEFORE Timer Fact or an exact matching
  persisted wake input may clear the current condition after all immutable
  bindings revalidate. It is never an implicit AND and Redis notification alone
  satisfies neither branch
GUARDED reason = BUDGET requires wake_identity to contain exactly project_id,
  accounting_scope_id, budget_policy_ref, budget_reset_window_ref, exhausted
  budget_report_id, window_id, reset_at_ms, and minimum_source_sequence equal
  to exhausted source_sequence + 1. The Timer arm only starts reconciliation;
  the event arm and any later offer require the latest exact applicable
  authenticated BudgetReport to be AVAILABLE and at least that sequence
GUARDED the single writer holds its transaction while applying WAIT_BUDGET and
  re-reads the latest exact applicable BudgetReport. It inserts the Wait only
  when that Report is the causal exhausted Report. A later AVAILABLE Report
  instead creates no Wait and appends a successor RecoveryEvidence selecting
  the origin-valid retry/resume tactic; a later EXHAUSTED Report creates no
  stale Wait and appends source-unique BUDGET Evidence bound to that Report.
  Only the successor Transition continues, so Report fanout cannot race just
  before Wait insertion and strand the Run
GUARDED reason = FORGE_UNAVAILABLE requires wake_identity to contain exactly
  forge_instance_id, forge_observation_schedule_id, target kind/ID, causal
  forge_request_failure_fact_id, minimum Schedule revision, and the nonempty
  sorted allowed result Observation kinds. A later matching accepted Forge
  Observation or verified FORGE_CONNECTIVITY/AVAILABLE HealthObservation may
  propose wake, but all Request/Schedule/Run/Publication/generation/policy
  bindings are revalidated before clearing the Wait
GUARDED the writer holds its transaction while applying a FORGE_TRANSIENT
  WAIT_EXTERNAL tactic and rechecks the exact Request/Schedule, minimum
  revision, allowed Observation set, and connectivity evidence. If qualifying
  success is already current, it inserts no Wait and appends a source-linked
  successor RecoveryEvidence selecting the origin-valid retry/reconciliation
  tactic. Otherwise it inserts the timer-plus-FORGE Wait. Request completion
  or connectivity acceptance immediately before this transaction therefore
  cannot become a missed, already-consumed wake
GUARDED reason = EVIDENCE requires a bounded non-null not_before_ms,
  wake_kind = EVIDENCE, and non-null wake_identity; an event-only or timer-only
  Evidence Wait is invalid. wake_identity is the exact canonical
  `orcest.evidence-wake/1` object containing protocol, project_id, target_kind
  WORK_ITEM or PUBLICATION, target_id, positive minimum_observation_sequence,
  sorted unique nonempty allowed_observation_kinds, and predicate_digest. The
  predicate digest binds the applicable Candidate, panel/dispute set,
  specification generation, policy, and Change Request head. The minimum is
  exactly the highest accepted sequence for that target examined by the
  selecting reduction plus one; the Timer and event arms retain OR semantics
GUARDED the single SQLite writer holds its transaction while selecting an
  EVIDENCE Wait and re-reads the target observation counter/membership. If an
  already accepted Observation has the exact target, an allowed kind, sequence
  at least the minimum, and still-current predicate bindings, the transaction
  MUST NOT insert or briefly project the Wait. It instead appends one successor
  RecoveryEvidence with source_kind RECOVERY_EVIDENCE, source_id equal to the
  current Evidence, and the deterministic retry/replacement tactic. Otherwise
  it atomically inserts the timer-plus-event Wait and its Transition. A later
  candidate wake repeats the same minimum-revision/predicate recheck before
  clearing; stale, below-minimum, or mismatched input is audit-only and leaves
  the Wait current
GUARDED SECRET_RECOVERY Waits bind a logical Secret ID plus positive
  minimum_version, never a mutable-current tag or one frozen old SecretRef.
  A failed Claim/Effect version ordinarily sets minimum_version to old version
  + 1; only a verified current version meeting that fence can wake it. The
  creation transaction first holds the logical-Secret lock and rechecks the
  verified current version. If it already meets minimum_version, it MUST NOT
  insert or briefly project the stale Wait; the same recovery reduction takes
  the satisfied path and creates the permitted fresh Attempt/Effect generation
  or next exact Recovery Evidence using that version
CHECK WaitCondition.created_from_kind IN (
  'INTERNAL', 'ATTEMPT_RESULT', 'ATTEMPT_TERMINAL', 'CONTROLLER_OPERATION',
  'RECOVERY_EVIDENCE', 'HEALTH_OBSERVATION',
  'FORGE_OBSERVATION', 'POLICY_UPDATE',
  'MANAGEMENT_COMMAND', 'SECRET_VERSION',
  'STORAGE_RESTORATION',
  'TIMER_FACT'
)
GUARDED WaitCondition.created_from_id resolves to the exact table selected by
  created_from_kind and is the Transition's persisted trigger/input
GUARDED a Forge Request Failure Fact or Budget Report can source the preceding
  RecoveryEvidence but never directly create a Wait; the resulting
  WaitCondition.created_from_kind is RECOVERY_EVIDENCE
GUARDED when a REVIEW/ADJUDICATE planning Transition freezes a panel but its
  pinned health membership cannot produce a complete legal staffing
  selection, every slot Activity/Assignment/subject membership commits as
  PLANNED with no OFFERED Attempt/outbox and the same transaction creates one
  CAPACITY Wait. Its created_from_kind/id equals the planning Transition's
  actual trigger—ATTEMPT_RESULT for verification/adjudication-result planning
  or INTERNAL for aggregation planning—not a fabricated Health Observation.
  A later matching capacity wake never creates offers directly. It clears the
  Wait, enters RECOVERING, and appends CAPACITY Recovery Evidence selecting
  STAFF_PANEL with the newly consulted Health membership and
  resumed_wait_condition_id naming this Wait. The immutable planned-slot
  membership/digest is inherited only through that pointer and is not copied
  into another child relation. Only the separate
  T(RECOVERY_EVIDENCE,recovery_evidence_id) transaction may staff the panel
PRIMARY KEY WaitConditionHealthObservation(
  wait_condition_id, observation_ordinal
)
UNIQUE WaitConditionHealthObservation(
  wait_condition_id, health_observation_id
)
FOREIGN KEY WaitConditionHealthObservation.wait_condition_id
  -> WaitCondition(wait_condition_id)
FOREIGN KEY WaitConditionHealthObservation.health_observation_id
  -> HealthObservation(health_observation_id)
CHECK WaitConditionHealthObservation.observation_ordinal >= 0
GUARDED rows are zero-based contiguous and sorted by
  `(scope_kind, scope_id, health_sequence, health_observation_id)`. They are
  every and only the highest-applicable unexpired observations consulted by
  the creating transaction and reproduce health_observation_ids_digest;
  non-health-selected Waits use no rows and the canonical-empty digest
PRIMARY KEY WaitConditionPanelSlot(wait_condition_id, slot_ordinal)
UNIQUE WaitConditionPanelSlot(wait_condition_id, activity_id)
UNIQUE WaitConditionPanelSlot(
  wait_condition_id, assignment_kind, panel_round, slot_id
)
FOREIGN KEY WaitConditionPanelSlot.wait_condition_id
  -> WaitCondition(wait_condition_id)
FOREIGN KEY WaitConditionPanelSlot.activity_id -> Activity(activity_id)
CHECK WaitConditionPanelSlot.slot_ordinal >= 0
CHECK WaitConditionPanelSlot.assignment_kind IN ('REVIEW', 'ADJUDICATE')
GUARDED panel rows exist exactly for a panel-scoped CAPACITY Wait, are
  zero-based contiguous, sort by assignment kind then Snapshot-configured slot
  order, and include every and only its current unfilled planned Activities.
  Each row's assignment_kind/panel_round/slot_id exactly equals its Activity
  Review Assignment; REVIEW uses its reviewer slot and ADJUDICATE uses the sole
  `default` slot. Rows reproduce panel_slots_digest. All other Waits have no
  rows and the canonical-empty digest
GUARDED a panel planning transaction commits the Wait, both complete
  memberships, every Activity/Assignment/subject row, and its one Transition
  atomically. Those Activities have no Attempt or outbox. A STAFF_PANEL
  Evidence transaction either creates exactly one current higher-generation
  OFFERED Attempt and outbox for every still-current named slot in canonical
  order, all atomically, or creates none and atomically inserts a replacement
  panel CAPACITY Wait with newly frozen complete memberships. Partial staffing
  cannot commit or be reconstructed from Redis
GUARDED Receipt acceptance computes filling status before any panel Wait is
  inserted. A fills_slot=true Receipt that completes the panel enters
  AGGREGATING and creates no Wait; one that leaves unfilled slots either keeps
  valid claimed peers and creates no Wait, or freezes only the remaining
  undispatched unfilled slots after superseding their OFFERED peers. A current
  panel Wait therefore has no live claimed Attempt for any member and cannot be
  filled by a late Result; only its typed wake/STAFF_PANEL path may dispatch
  those slots
GUARDED RecoveryEvidence.selected_tactic = 'STAFF_PANEL' requires a non-null
  resumed_wait_condition_id naming a panel-scoped CAPACITY Wait. The Evidence
  owns its newly consulted Health rows/digest; staffing reads the immutable
  slots only through that Wait and revalidates every Activity remains current
  and unfilled. Any membership change takes the offer-none/new-Wait branch
DEFERRABLE FOREIGN KEY Run(run_id, wait_condition_id)
  -> WaitCondition(run_id, wait_condition_id)
PRIMARY KEY TimerFact(timer_fact_id)
UNIQUE TimerFact(scope_kind, scope_id, fired_for_ms)
CHECK TimerFact.scope_kind IN (
  'WAIT_CONDITION_NOT_BEFORE', 'HEALTH_OBSERVATION_EXPIRY',
  'BUDGET_REPORT_EXPIRY',
  'ATTEMPT_CLAIM_DEADLINE', 'ATTEMPT_EXECUTION_DEADLINE',
  'RECOVERY_ELIGIBILITY'
)
CHECK TimerFact.source_kind IN (
  'SCHEDULED_SWEEP', 'STARTUP_RECONCILIATION'
)
CHECK TimerFact.source_id is a lowercase controller-assigned UUID scan-pass ID
CHECK TimerFact.controller_now_ms >= fired_for_ms
CHECK TimerFact.affected_run_ids_digest is non-null and equals SHA-256 of the
  canonical length-prefixed ordered TimerFactRun.run_id sequence, including
  the canonical empty sequence
GUARDED TimerFact.scope_id resolves by scope_kind to the exact Wait Condition,
  Health Observation, Budget Report, Attempt, or Recovery Evidence and
  fired_for_ms equals its current not_before_ms, expires_at_ms, claim_deadline_ms,
  execution_deadline_ms, or next_eligible_at_ms respectively
GUARDED TimerFact.run_id equals the resolved object's Run for Wait, Attempt,
  and Recovery scopes; it is NULL only for a global Health Observation or
  Budget Report expiry
PRIMARY KEY TimerFactRun(timer_fact_id, run_ordinal)
UNIQUE TimerFactRun(timer_fact_id, run_id)
CHECK TimerFactRun.run_ordinal >= 0 and ordinals are zero-based and contiguous
FOREIGN KEY TimerFactRun.timer_fact_id -> TimerFact(timer_fact_id)
FOREIGN KEY TimerFactRun.run_id -> Run(run_id)
GUARDED a HEALTH_OBSERVATION_EXPIRY Timer Fact freezes, in its insertion
  transaction, the canonically sorted active Runs whose current Wait Condition
  explicitly binds that Health Observation as health/wake evidence; offered
  Activities and Recovery Evidence are not members, and every other Timer Fact
  has empty membership. The Fact, complete membership, and digest
  commit atomically; replay validates those rows and never recomputes membership
GUARDED a BUDGET_REPORT_EXPIRY Timer Fact always has empty TimerFactRun
  membership and no direct Run Transition. Its insertion makes the scoped
  Report ineligible; planned-Activity reconciliation waits for a newer Report
PRIMARY KEY RecoveryEvidence(recovery_evidence_id)
UNIQUE RecoveryEvidence(run_id, recovery_sequence)
UNIQUE RecoveryEvidence(run_id, source_kind, source_id)
UNIQUE RecoveryEvidence(run_id, recovery_evidence_id)
CHECK RecoveryEvidence.source_kind IN (
  'INTERNAL', 'ATTEMPT_RESULT', 'ATTEMPT_TERMINAL', 'CONTROLLER_OPERATION',
  'FORGE_REQUEST_FAILURE', 'HEALTH_OBSERVATION', 'FORGE_OBSERVATION',
  'BUDGET_REPORT', 'RECONCILIATION_FACT', 'POLICY_UPDATE',
  'MANAGEMENT_COMMAND', 'SECRET_VERSION', 'RECOVERY_EVIDENCE',
  'STORAGE_RESTORATION', 'TIMER_FACT'
)
GUARDED RecoveryEvidence.source_id resolves to the exact immutable object
  selected by source_kind and every copied Activity/Attempt/specification/
  Candidate/Forge binding matches that source and owning Run
GUARDED source_kind RECOVERY_EVIDENCE is used only for the successor created
  when an EVIDENCE Wait predicate is already satisfied under the writer lock;
  source_id names the same-Run immediately causal Recovery Evidence and the
  successor selects the deterministic retry/replacement tactic rather than
  creating a stale Wait
GUARDED source_kind FORGE_REQUEST_FAILURE resolves to the exact Run-bound
  ForgeRequestFailureFact and category = FORGE_TRANSIENT. source_kind
  BUDGET_REPORT resolves to the exact same-Project applicable EXHAUSTED Report
  and category = BUDGET. source_kind CONTROLLER_OPERATION with a failed Fact
  copies that Fact's closed failure_category exactly; it cannot manufacture
  FORGE_TRANSIENT or BUDGET
CHECK RecoveryEvidence resume source group:
  resumed_wait_condition_id IS NOT NULL => resumed_human_boundary_id,
                                           human_resolution_id IS NULL
  resumed_human_boundary_id IS NOT NULL => human_resolution_id
                                            IS NOT NULL AND
                                            resumed_wait_condition_id IS NULL
  otherwise all three are NULL
FOREIGN KEY RecoveryEvidence.resumed_wait_condition_id
  -> WaitCondition(wait_condition_id)
FOREIGN KEY RecoveryEvidence.resumed_human_boundary_id
  -> HumanBoundary(human_boundary_id)
FOREIGN KEY RecoveryEvidence.human_resolution_id
  -> HumanResolution(human_resolution_id)
GUARDED every non-null resume pointer equals the corresponding Run recovery-
  resume pointer, belongs to that Run, and is included in evidence_digest.
  Wake/resolution entry persists this evidence or its exact INTERNAL
  continuation before planning any recovery work
CHECK RecoveryEvidence.category IN (
  'WORKER_LOST', 'TIMEOUT', 'PROVIDER_TRANSIENT',
  'PROVIDER_RATE_LIMIT', 'CAPACITY', 'BUDGET', 'INVALID_RESULT', 'CREDENTIAL',
  'SOURCE_READ', 'VERIFICATION_ERROR', 'VERIFICATION_FAILURE',
  'REPEATED_NON_PROGRESS', 'REVIEW_DISAGREEMENT', 'BASE_CONFLICT',
  'FORGE_TRANSIENT', 'EXTERNAL_DEPENDENCY', 'STORAGE',
  'INTEGRITY_SUSPECTED', 'POLICY'
)
CHECK RecoveryEvidence.health_observation_ids_digest is non-null and matches
  canonical SHA-256 syntax, including the canonical empty-list digest
PRIMARY KEY RecoveryEvidenceHealthObservation(
  recovery_evidence_id, observation_ordinal
)
UNIQUE RecoveryEvidenceHealthObservation(
  recovery_evidence_id, health_observation_id
)
CHECK RecoveryEvidenceHealthObservation.observation_ordinal >= 0 and ordinals
  for one Recovery Evidence are zero-based and contiguous
FOREIGN KEY RecoveryEvidenceHealthObservation.recovery_evidence_id
  -> RecoveryEvidence(recovery_evidence_id)
FOREIGN KEY RecoveryEvidenceHealthObservation.health_observation_id
  -> HealthObservation(health_observation_id)
GUARDED membership contains at most the highest applicable unexpired ordered
  Health Observation for each pinned-policy-relevant scope at record creation,
  is sorted by (scope_kind, scope_id, health_sequence,
  health_observation_id), reproduces health_observation_ids_digest, and commits
  atomically with RecoveryEvidence, its selected fallback/counters, and the
  creating Transition; evidence_digest covers the membership digest
CHECK RecoveryEvidence.selected_tactic IN (
  'RECONCILE', 'REDELIVER', 'RETRY_EXECUTION', 'REPLACE_CAPACITY',
  'STAFF_PANEL', 'REPAIR_SCHEMA', 'PROBE_INTEGRITY', 'DIAGNOSE', 'REPLAN',
  'ALTERNATIVE_CANDIDATE',
  'ADJUDICATE', 'REBASE', 'IMPORT_EXTERNAL_HEAD',
  'RECONSTRUCT_FOREIGN_HEAD', 'ENTER_HUMAN_BOUNDARY', 'WAIT_BACKOFF',
  'WAIT_CAPACITY', 'WAIT_RATE_LIMIT', 'WAIT_BUDGET', 'WAIT_EXTERNAL',
  'WAIT_EVIDENCE'
)
GUARDED every selected tactic that would create an OFFERED Attempt/outbox is
  conditional on the current Controller Mode and Capability Key Registry
  permitting offers. A closed gate leaves the exact selected Activity PLANNED
  and creates neither row; it consumes no Attempt generation or claim deadline
GUARDED category = 'INTEGRITY_SUSPECTED' may select only PROBE_INTEGRITY and
  names one exact current CANDIDATE_ARTIFACT, WORKFLOW_BLOB, or SECRET_VERSION.
  Its T(RECOVERY_EVIDENCE, recovery_evidence_id) transaction creates the
  reciprocal HealthProbeRequest/source-tagged Outbox before I/O and creates no
  storage/secret Wait. Only that Request's Fact/Observation transition may
  append confirmed STORAGE or CREDENTIAL Evidence; only the later transition
  for that confirmed Evidence may create STORAGE_RECOVERY or SECRET_RECOVERY
GUARDED an AVAILABLE integrity Health Observation keeps the Run RECOVERING and
  appends one exact-source RecoveryEvidence selecting the ordinary origin-valid
  retry/resume tactic; it creates no storage/secret Wait. An UNAVAILABLE
  integrity Observation appends STORAGE for CANDIDATE_ARTIFACT/WORKFLOW_BLOB or
  CREDENTIAL for SECRET_VERSION, and only that new Evidence may select
  WAIT_EXTERNAL and create the typed recovery Wait. The probe Result and Wait
  therefore cannot collapse into one Transition
GUARDED an accepted VERIFY/FAIL below the installed Snapshot's
  maxRepairCyclesBeforeDiagnosis follows ordinary remediation. When the same
  normalized verification-failure fingerprint reaches that threshold, the
  ATTEMPT_RESULT transaction stores the Receipt, completes VERIFY, and appends
  one VERIFICATION_FAILURE RecoveryEvidence with the post-application counters
  and selected_tactic = DIAGNOSE; it creates no REMEDIATE Activity. Likewise,
  a canonical REMEDIATE Consensus Decision or sustained/new adjudication
  blocker below the threshold follows ordinary remediation, while the same
  normalized blocker fingerprint at the threshold appends one
  REVIEW_DISAGREEMENT RecoveryEvidence selecting DIAGNOSE and plans no
  remediation. The source-bound Evidence row, fingerprint, and counters make
  restart choose the same branch
DEFERRABLE FOREIGN KEY Run(run_id, current_recovery_evidence_id)
  -> RecoveryEvidence(run_id, recovery_evidence_id)
PRIMARY KEY CapacityReport(capacity_report_id)
UNIQUE CapacityReport(pool_manager_id, report_id)
UNIQUE CapacityReport(pool_manager_id, idempotency_key)
UNIQUE CapacityReport(pool_manager_id, report_sequence)
CHECK CapacityReport.protocol = 'orcest.capacity-report/1'
CHECK CapacityReport.report_sequence > 0
CHECK CapacityReport pool_manager_id, report_id, idempotency_key, protocol,
  report_sequence, observed_at_ms, expires_at_ms, configured_max_ttl_ms,
  normalized_body,
  authenticated_principal_id, payload_digest, authorization_context_digest,
  response_body,
  response_digest, and accepted_at_ms are non-null and bounded
CHECK CapacityReport.configured_max_ttl_ms > 0
CHECK CapacityReport.expires_at_ms > CapacityReport.accepted_at_ms
CHECK CapacityReport.expires_at_ms <= CapacityReport.accepted_at_ms +
  CapacityReport.configured_max_ttl_ms
GUARDED CapacityReport accepted Health Observation membership is canonical and
  one-for-one with its ordered entries and belongs to the same transaction;
  authenticated_principal_id is the registered principal for pool_manager_id;
  one accepted_at_ms is sampled after authentication and canonical-body
  validation and copied as every entry's effective_at_ms;
  identical pool_manager_id/report_id/idempotency_key/body replay returns the
  stored response, while any conflicting reuse is IDEMPOTENCY_CONFLICT;
  stored response_body has replayed=false, response_digest excludes exactly
  that transport projection, and identical replay changes only it to true
PRIMARY KEY CapacityReportObservation(
  capacity_report_id, observation_ordinal
)
CHECK CapacityReportObservation.observation_ordinal >= 0
UNIQUE CapacityReportObservation(health_observation_id)
FOREIGN KEY CapacityReportObservation.capacity_report_id
  -> CapacityReport(capacity_report_id)
FOREIGN KEY CapacityReportObservation.health_observation_id
  -> HealthObservation(health_observation_id)
PRIMARY KEY WorkerLossReport(worker_loss_report_id)
UNIQUE WorkerLossReport(pool_manager_id, idempotency_key)
CHECK WorkerLossReport exact worker_id, worker_session_id, attempt_id,
  pool_manager_id, idempotency_key, activity_id, attempt_generation, reason,
  observed_at_ms, payload_digest, outcome,
  authenticated_principal_id, authorization_context_digest, response_body,
  response_digest,
  and accepted_at_ms are non-null
GUARDED WorkerLossReport.authenticated_principal_id is the registered principal
  for pool_manager_id and is authorized for the named worker/session
CHECK WorkerLossReport.reason IN (
  'VM_DESTROYED', 'VM_MISSING', 'CEILING_TIMEOUT', 'OPERATOR_DRAIN'
)
CHECK WorkerLossReport.outcome IN ('ACCEPTED', 'STALE')
CHECK WorkerLossReport outcome/result matrix:
  ACCEPTED => health_observation_id, attempt_terminal_fact_id IS NOT NULL
  STALE    => health_observation_id, attempt_terminal_fact_id IS NULL
FOREIGN KEY WorkerLossReport(attempt_id, activity_id, attempt_generation)
  -> Attempt(attempt_id, activity_id, generation)
GUARDED STALE still references that existing Attempt triple but its current
  state/generation/session fence no longer matches; an unknown triple returns
  404 ATTEMPT_UNKNOWN before ledger insertion and creates no WorkerLossReport
GUARDED WorkerLossReport's stored response has replayed=false and
  response_digest covers the complete stable body except exactly replayed;
  identical replay changes only that projection to true, while conflicting
  reuse is IDEMPOTENCY_CONFLICT
FOREIGN KEY WorkerLossReport.health_observation_id
  -> HealthObservation(health_observation_id)
FOREIGN KEY WorkerLossReport.attempt_terminal_fact_id
  -> AttemptTerminalFact(attempt_terminal_fact_id)
PRIMARY KEY HealthProbeRequest(health_probe_request_id)
CHECK HealthProbeRequest.probe_kind IN (
  'FORGE_CONNECTIVITY', 'PROVIDER_ACCOUNT_STATUS',
  'STORAGE_OBJECT_INTEGRITY', 'SECRET_VERSION_INTEGRITY'
)
CHECK HealthProbeRequest.state IN ('PENDING', 'COMPLETED', 'SUPERSEDED')
CHECK HealthProbeRequest.requested_at_ms < HealthProbeRequest.not_after_ms
CHECK HealthProbeRequest.expected_prior_health_sequence >= 0
UNIQUE HealthProbeRequest.outbox_id
FOREIGN KEY HealthProbeRequest.outbox_id -> Outbox(outbox_id)
UNIQUE HealthProbeRequest.health_probe_fact_id
  WHERE health_probe_fact_id IS NOT NULL
DEFERRABLE FOREIGN KEY HealthProbeRequest.health_probe_fact_id
  -> HealthProbeFact(health_probe_fact_id)
CHECK HealthProbeRequest target union:
  FORGE_CONNECTIVITY => scope_kind = 'FORGE';
                        forge_credential_secret_ref IS NOT NULL;
                        object_kind, object_id,
                        provider_secret_ref IS NULL
  PROVIDER_ACCOUNT_STATUS => scope_kind = 'PROVIDER_ACCOUNT';
                             provider_secret_ref IS NOT NULL;
                             forge_credential_secret_ref,
                             object_kind, object_id IS NULL
  STORAGE_OBJECT_INTEGRITY => scope_kind = 'STORAGE';
                              object_kind IN (
                                'CANDIDATE_ARTIFACT', 'WORKFLOW_BLOB'
                              ); object_id IS NOT NULL;
                              provider_secret_ref,
                              forge_credential_secret_ref IS NULL
  SECRET_VERSION_INTEGRITY => scope_kind = 'SECRET';
                              object_kind = 'SECRET_VERSION';
                              object_id IS NOT NULL;
                              provider_secret_ref,
                              forge_credential_secret_ref IS NULL
CHECK HealthProbeRequest completion union:
  PENDING    => health_probe_fact_id IS NULL
  COMPLETED  => health_probe_fact_id IS NOT NULL
  SUPERSEDED => health_probe_fact_id IS NULL
FOREIGN KEY HealthProbeRequest.provider_secret_ref
  -> SecretVersion(secret_id, version)
FOREIGN KEY HealthProbeRequest.forge_credential_secret_ref
  -> SecretVersion(secret_id, version)
GUARDED expected_prior_health_sequence, probe_implementation_id,
  subject_bindings, probe_input_json, probe_input_digest,
  request_digest, requested_at_ms, not_after_ms, outbox_id, scope_kind, and
  scope_id are non-null and bounded. request_digest covers every immutable
  field except state and the derived Fact pointer. scope_id is exactly
  `sha256:` plus lowercase SHA-256 of
  `ascii("orcest-health-scope-v1") || 0x00 ||
  canonical_json(subject_bindings)` using the closed tagged Domain scope
  identity. FORGE_CONNECTIVITY subject bindings include the exact registered
  Forge Instance and frozen `(credential_secret_id,version)` reference; for
  STORAGE integrity the identity includes storage-domain ID plus
  exact object_kind/object_id; for SECRET integrity it includes the exact
  Secret ID/version. Per-object failures therefore cannot share a broad
  storage or logical-Secret scope sequence. object_id remains the separately
  guarded typed target and is never used as untagged cross-domain scope text
GUARDED HealthProbeRequest and its exact code-owned probe-dispatch Outbox row
  commit before external I/O. A retry uses the same Request/outbox/adapter
  identity and MUST NOT begin I/O at or after not_after_ms. A stale intent may
  become SUPERSEDED with no Fact. Redis and a raw adapter callback cannot create
  either health authority or a replacement Request
PRIMARY KEY HealthProbeFact(health_probe_fact_id)
UNIQUE HealthProbeFact(health_probe_request_id)
UNIQUE HealthProbeFact(
  probe_kind, scope_kind, scope_id, probe_sequence
)
UNIQUE HealthProbeFact(health_observation_id)
CHECK HealthProbeFact.probe_sequence > 0
CHECK HealthProbeFact.probe_kind IN (
  'FORGE_CONNECTIVITY', 'PROVIDER_ACCOUNT_STATUS',
  'STORAGE_OBJECT_INTEGRITY', 'SECRET_VERSION_INTEGRITY'
)
CHECK HealthProbeFact probe/scope/outcome/object union:
  FORGE_CONNECTIVITY => scope_kind = 'FORGE', scope_id hashes the exact
    tagged forge_instance_id plus frozen forge_credential_secret_ref identity,
    forge_credential_secret_ref IS NOT NULL,
    outcome IN ('AVAILABLE', 'UNAVAILABLE'), object_kind, object_id,
    provider_secret_ref, integrity_failure_code IS NULL
  PROVIDER_ACCOUNT_STATUS => scope_kind = 'PROVIDER_ACCOUNT', scope_id hashes
    provider, non-secret provider_account_ref, and exact provider_secret_ref;
    provider_secret_ref IS NOT NULL;
    outcome IN ('AVAILABLE', 'UNAVAILABLE', 'RATE_LIMITED', 'EXHAUSTED'),
    object_kind, object_id, forge_credential_secret_ref,
    integrity_failure_code IS NULL
  STORAGE_OBJECT_INTEGRITY => scope_kind = 'STORAGE',
    outcome IN ('AVAILABLE', 'UNAVAILABLE'),
    object_kind IN ('CANDIDATE_ARTIFACT', 'WORKFLOW_BLOB'), object_id is the
    exact bundle or domain-separated Workflow Blob digest, scope_id hashes
    subject_bindings containing the controller storage-domain ID plus that
    exact object_kind/object_id,
    provider_secret_ref, forge_credential_secret_ref IS NULL, and
    integrity_failure_code IS NULL for AVAILABLE or IN
      ('MISSING', 'UNREADABLE', 'DIGEST_MISMATCH') for UNAVAILABLE
  SECRET_VERSION_INTEGRITY => scope_kind = 'SECRET',
    outcome IN ('AVAILABLE', 'UNAVAILABLE'),
    object_kind = 'SECRET_VERSION', object_id is its canonical key,
    scope_id hashes subject_bindings containing that exact Secret ID/version,
    and
    provider_secret_ref, forge_credential_secret_ref IS NULL, and
    integrity_failure_code IS NULL for AVAILABLE or IN
      ('MISSING', 'UNREADABLE', 'KEYED_ATTESTATION_MISMATCH') for UNAVAILABLE
CHECK HealthProbeFact subject_bindings, effective_at_ms, probe_implementation_id,
  probe_input_digest, evidence_digest, probe_fact_digest, health_observation_id,
  affected_run_ids_digest, fanout_cursor_ordinal, recorded_at_ms are non-null
  and bounded
CHECK HealthProbeFact.fanout_cursor_ordinal >= 0
CHECK HealthProbeFact expiry/TTL union:
  expires_at_ms IS NULL => configured_max_ttl_ms IS NULL
  expires_at_ms IS NOT NULL => configured_max_ttl_ms > 0 AND
    effective_at_ms < expires_at_ms AND
    expires_at_ms <= effective_at_ms + configured_max_ttl_ms
CHECK integrity probes have expires_at_ms IS NULL and
  configured_max_ttl_ms IS NULL
CHECK HealthProbeFact.scope_id = "sha256:" + lowercase_hex(SHA256(
  ascii("orcest-health-scope-v1") || 0x00 ||
  canonical_json(HealthProbeFact.subject_bindings)
))
DEFERRABLE FOREIGN KEY HealthProbeFact.health_observation_id
  -> HealthObservation(health_observation_id)
DEFERRABLE FOREIGN KEY HealthProbeFact.health_probe_request_id
  -> HealthProbeRequest(health_probe_request_id)
FOREIGN KEY HealthProbeFact.provider_secret_ref
  -> SecretVersion(secret_id, version)
FOREIGN KEY HealthProbeFact.forge_credential_secret_ref
  -> SecretVersion(secret_id, version)
GUARDED probe_fact_digest is the domain-separated digest of every normalized
  immutable Fact source field except derived health_observation_id and
  informational recorded_at_ms; it explicitly includes
  affected_run_ids_digest and excludes mutable fanout cursor/completion. Request completion, HealthProbeFact, its one
  HealthObservation, and complete frozen HealthProbeFactRun membership/digest
  commit atomically; the Request becomes COMPLETED and both reciprocal Fact
  pointers agree. Fact kind/subject_bindings/scope/object,
  conditional provider or Forge credential SecretRef, implementation, and
  input digest equal the Request exactly. Completion
  requires the Request's expected_prior_health_sequence still equal the
  scope's current highest sequence and assigns probe_sequence to that value
  plus one; otherwise the Request becomes SUPERSEDED without a Fact and a new
  intent may be planned. The
  Observation copies scope, outcome, observed_revision, effective/expiry times,
  and code-owned subject bindings, uses source_kind HEALTH_PROBE_FACT and
  source_id=health_probe_fact_id, and receives the next health_sequence.
  Replay by Fact ID plus identical probe_fact_digest returns that row;
  conflicting reuse
  fails. Repository content, worker input, raw adapter callbacks, and free-form
  diagnostics cannot insert either object directly. Secret evidence includes
  no raw bytes, unkeyed secret digest, or keyed tag
PRIMARY KEY HealthProbeFactRun(health_probe_fact_id, run_ordinal)
UNIQUE HealthProbeFactRun(health_probe_fact_id, run_id)
CHECK HealthProbeFactRun.run_ordinal >= 0 and ordinals for one Fact are
  zero-based and contiguous
FOREIGN KEY HealthProbeFactRun.health_probe_fact_id
  -> HealthProbeFact(health_probe_fact_id)
FOREIGN KEY HealthProbeFactRun.run_id -> Run(run_id)
GUARDED membership contains exactly the active Runs whose current Snapshot,
  Candidate, Activity/Attempt, Publication, Wait, or Boundary references the
  Fact's exact scope/subject bindings at Fact commit, sorted by bytewise run_id.
  The ordered IDs reproduce affected_run_ids_digest, including the canonical
  empty set; membership is never recomputed during replay
GUARDED HealthProbeFact.fanout_cursor_ordinal is no greater than its membership
  cardinality. fanout_completed_at_ms is NULL exactly while the cursor is less
  than cardinality and non-null exactly when equal. The fanout reconciler reads
  only the member at the cursor. In one writer transaction it inserts or finds
  that Run's unique T(HEALTH_OBSERVATION, health_observation_id), applies the
  exact health row when still relevant, and advances the cursor by one. If the
  Run's referenced work, version, Candidate, Publication, Wait, or Boundary is
  superseded or the Observation is otherwise unrelated, that Transition is a
  same-state audit and changes no counters, Recovery Evidence, Wait, or work.
  Crash recovery resumes at the cursor; an existing Transition is successful
  replay and no later Run may join this frozen fanout
PRIMARY KEY HealthObservation(health_observation_id)
UNIQUE HealthObservation(scope_kind, scope_id, health_sequence)
UNIQUE HealthObservation(scope_kind, scope_id, source_kind, source_id)
CHECK HealthObservation.scope_kind IN (
  'WORKER_SESSION', 'WORKER_PROFILE', 'PROVIDER_ACCOUNT', 'CAPACITY_POOL',
  'FORGE', 'STORAGE', 'SECRET'
)
CHECK HealthObservation.kind IN (
  'AVAILABLE', 'UNAVAILABLE', 'RATE_LIMITED', 'EXHAUSTED', 'LOST', 'RECOVERED'
)
CHECK HealthObservation.source_kind IN (
  'CAPACITY_REPORT', 'WORKER_LOSS_REPORT', 'STORAGE_RESTORATION',
  'HEALTH_PROBE_FACT'
)
CHECK HealthObservation.expires_at_ms IS NULL
   OR HealthObservation.expires_at_ms > effective_at_ms
GUARDED CAPACITY_REPORT source_id is canonical
  capacity_report_id, observed_revision is that authenticated principal's
  strictly increasing report_sequence, subject
  bindings match its registered scope, expires_at_ms is non-null, and kind is
  controller-derived from available_slots/session readiness rather than copied
GUARDED WORKER_LOSS_REPORT HealthObservation is kind LOST for the exact
  authenticated worker/session, source_id is worker_loss_report_id, and
  observed_revision is NULL; its health_sequence remains controller-assigned
GUARDED HEALTH_PROBE_FACT source_id is the exact health_probe_fact_id and all
  scope/outcome/object/revision/time/evidence bindings satisfy the closed Fact
  matrix; the reciprocal Fact.health_observation_id names this Observation.
  HealthObservation.subject_bindings exactly equals the Fact and reproduces
  scope_id with the domain-separated health-scope formula; it may not be
  reconstructed from object_id or mutable registration at read time.
  Provider-account evidence binds provider, account, and the exact versioned
  provider_secret_ref copied Request->Fact->Observation; rotation requires a
  new Request/Fact/Observation and old-version availability cannot authorize a
  new Claim or Launch
GUARDED STORAGE_RESTORATION HealthObservation has kind RECOVERED, source_id is
  the exact storage_restoration_fact_id, and scope is STORAGE for a Candidate
  Artifact or Workflow Blob and SECRET for a Secret Version; object/scope
  bindings and verification evidence match that Fact. Fact, Observation, and
  frozen affected-Run fan-out commit atomically; the reducer trigger remains
  STORAGE_RESTORATION with the Fact ID, never the Health Observation
PRIMARY KEY StorageRestorationOperation(operation_id)
CHECK StorageRestorationOperation.protocol = 'orcest.storage-management/1'
CHECK StorageRestorationOperation.authenticated_principal_id,
  authorization_context_digest, object_kind, object_id, byte_length,
  staged_storage_key, accepted_at_ms, and state are non-null
CHECK StorageRestorationOperation.state IN ('PENDING', 'RESTORED', 'REJECTED')
CHECK StorageRestorationOperation.backup_manifest_digest IS NULL
GUARDED StorageRestorationOperation integrity matrix:
  CANDIDATE_ARTIFACT => expected_digest and staged_digest are non-null, equal,
                        and name the live ArtifactObject SHA-256;
                        workflow_blob_media_kind IS NULL
  WORKFLOW_BLOB      => expected_digest and staged_digest are non-null, equal,
                        and name the live WorkflowBlob domain-separated digest;
                        workflow_blob_media_kind is the exact code-owned kind
                        and byte_length is the exact normalized byte length
  SECRET_VERSION     => expected_digest and staged_digest are NULL; the caller
                        cannot submit a secret digest, and staged verification
                        occurs inside the Secret Store against controller-only
                        keyed integrity metadata;
                        secret_request_attestation_id is a non-null opaque
                        Secret Store metadata reference;
                        workflow_blob_media_kind IS NULL
  CANDIDATE_ARTIFACT, WORKFLOW_BLOB => secret_request_attestation_id IS NULL
GUARDED StorageRestorationOperation staging matrix:
  CANDIDATE_ARTIFACT, WORKFLOW_BLOB => staged_storage_key is a normalized path
                                       below the general restoration quarantine
  SECRET_VERSION => staged_storage_key is a normalized operation-specific path
                    below protected Secret Store incoming storage; its `0600`
                    bytes and keyed request authenticator never leave that store
CHECK StorageRestorationOperation terminal matrix:
  PENDING  => resulting_fact_id, rejection_code, terminal_http_status,
              terminal_response_json, terminal_response_digest,
              terminal_at_ms IS NULL
  RESTORED => resulting_fact_id, terminal_at_ms IS NOT NULL and
              rejection_code IS NULL;
              terminal_http_status = 200 and terminal_response_json,
              terminal_response_digest IS NOT NULL
  REJECTED => resulting_fact_id IS NULL and rejection_code, terminal_at_ms
              IS NOT NULL; terminal_http_status, terminal_response_json,
              terminal_response_digest IS NOT NULL; terminal_http_status is
              409 for OBJECT_NO_LONGER_LIVE/INTEGRITY_CONFLICT, 403 for
              AUTHORIZATION_REVOKED, or 422 for STAGED_OBJECT_INVALID
CHECK StorageRestorationOperation.rejection_code IS NULL OR is one of
  'OBJECT_NO_LONGER_LIVE', 'AUTHORIZATION_REVOKED', 'STAGED_OBJECT_INVALID',
  'INTEGRITY_CONFLICT'
FOREIGN KEY StorageRestorationOperation.resulting_fact_id
  -> StorageRestorationFact(storage_restoration_fact_id)
GUARDED secret_request_attestation_id resolves only inside the Secret Store to
  the operation-bound keyed request authenticator retained for the audit life
  of this operation; SQLite, Redis, logs, and APIs never contain its tag or key
GUARDED a PENDING Operation has one deterministic, unstored HTTP 202 projection
  containing exactly protocol orcest.storage-restoration-accepted/1,
  operation_id, state=PENDING, object_kind, and object_id. A terminal response
  uses orcest.storage-restoration-result/1 and contains exactly those identities,
  the terminal state, and either storage_restoration_fact_id for RESTORED or the
  closed rejection_code for REJECTED. The terminal digest covers HTTP status
  and exact body. Exact operation/payload replay returns the current projection;
  there is no GET dependency or replay-only body field
PRIMARY KEY StorageRestorationFact(storage_restoration_fact_id)
UNIQUE StorageRestorationFact(
  object_kind, object_id, source_kind, source_id
)
CHECK StorageRestorationFact.object_kind IN (
  'CANDIDATE_ARTIFACT', 'SECRET_VERSION', 'WORKFLOW_BLOB'
)
CHECK StorageRestorationFact.health_observation_id, verification_digest,
  and recorded_at_ms are non-null
CHECK StorageRestorationFact integrity matrix:
  CANDIDATE_ARTIFACT => expected_digest, restored_digest are matching non-null
                        SHA-256 values; workflow_blob_media_kind,
                        workflow_blob_byte_length,
                        secret_integrity_attestation_id IS NULL
  WORKFLOW_BLOB => expected_digest, restored_digest are matching non-null
                   domain-separated Workflow Blob digests;
                   workflow_blob_media_kind and workflow_blob_byte_length are
                   non-null and exactly match the restored row;
                   secret_integrity_attestation_id IS NULL
  SECRET_VERSION => expected_digest, restored_digest IS NULL
                    AND secret_integrity_attestation_id IS NOT NULL;
                    workflow_blob_media_kind,
                    workflow_blob_byte_length IS NULL
CHECK StorageRestorationFact.source_kind IN (
  'BACKUP_RESTORE', 'AUTHENTICATED_STORAGE_OPERATION'
)
CHECK StorageRestorationFact source/auth matrix:
  BACKUP_RESTORE => backup_manifest_digest IS NOT NULL
                    AND authenticated_principal_id,
                        authorization_context_digest IS NULL
  AUTHENTICATED_STORAGE_OPERATION => backup_manifest_digest IS NULL
                                     AND authenticated_principal_id,
                                         authorization_context_digest
                                         IS NOT NULL
FOREIGN KEY authenticated StorageRestorationFact.source_id
  -> StorageRestorationOperation(operation_id)
DEFERRABLE FOREIGN KEY StorageRestorationFact.health_observation_id
  -> HealthObservation(health_observation_id)
GUARDED StorageRestorationFact.object_id resolves by object_kind to an exact
  live ArtifactObject bundle digest, canonical Secret Version key, or
  WorkflowBlob digest. Candidate/Blob expected_digest equals that owner-table
  digest. Candidate restored_digest is recomputed after durable install;
  Workflow Blob restored_digest is recomputed from exact media kind, unsigned
  length, and normalized bytes using the domain-separated formula. Secret
  restoration instead resolves the exact version's controller-only keyed
  metadata and stores only the opaque attestation ID. verification_digest
  covers source, kind-appropriate integrity result, byte/mode/path checks, and
  owner cross-check but never the secret tag or secret-derived bytes
GUARDED StorageRestorationFact.health_observation_id names the exact
  STORAGE_RESTORATION-sourced RECOVERED Observation for this Fact/object/scope;
  object repair, Fact, Observation, membership, and fan-out effects commit as
  one writer transaction
GUARDED BACKUP_RESTORE source_id is an immutable backup-manifest/restore
  identity whose complete marker and entire manifest validate;
  AUTHENTICATED_STORAGE_OPERATION source/principal/authorization/payload match
  its accepted operation exactly
GUARDED StorageRestorationOperation.resulting_fact_id, when non-null, names an
  AUTHENTICATED_STORAGE_OPERATION Fact with source_id = operation_id and exact
  same object and kind-appropriate digest/attestation result
PRIMARY KEY StorageRestorationFactRun(
  storage_restoration_fact_id, run_ordinal
)
UNIQUE StorageRestorationFactRun(storage_restoration_fact_id, run_id)
CHECK StorageRestorationFactRun.run_ordinal >= 0
FOREIGN KEY StorageRestorationFactRun.storage_restoration_fact_id
  -> StorageRestorationFact(storage_restoration_fact_id)
FOREIGN KEY StorageRestorationFactRun.run_id -> Run(run_id)
GUARDED StorageRestorationFactRun membership is the canonically sorted,
  distinct set of active Runs whose current exact-object STORAGE_RECOVERY/
  SECRET_RECOVERY Wait Condition or INTEGRITY_FAILURE Human Boundary matches
  the restored object at commit; the Fact and complete
  membership are inserted atomically
PRIMARY KEY ManagementCommand(command_id)
CHECK ManagementCommand.protocol = 'orcest.management/1'
CHECK ManagementCommand.kind IN ('CANCEL', 'RESOLVE_HUMAN_BOUNDARY')
CHECK ManagementCommand.result_transition_sequence, response_http_status,
  response_json, response_digest IS NOT NULL
CHECK ManagementCommand.response_http_status = 200
CHECK ManagementCommand kind/payload union:
  CANCEL => human_boundary_id, resolution_kind, resolution,
            human_resolution_id IS NULL
  RESOLVE_HUMAN_BOUNDARY => human_boundary_id, resolution_kind, resolution,
                            human_resolution_id IS NOT NULL
FOREIGN KEY ManagementCommand(run_id, result_transition_sequence)
  -> Transition(run_id, transition_sequence)
FOREIGN KEY ManagementCommand(human_boundary_id, run_id)
  -> HumanBoundary(human_boundary_id, run_id)
GUARDED ManagementCommand.response_json is the exact closed
  orcest.management-result/1 body containing protocol, command_id, run_id,
  kind, outcome=ACCEPTED, result_transition_sequence, conditional exact
  human_resolution_id, and replayed=false. response_digest covers the HTTP
  status and every body field except exactly replayed. Identical command replay
  changes only replayed to true; no rejected request creates this row
CHECK Run.cancellation_source_kind and cancellation_source_id are both NULL or
  both NOT NULL
CHECK Run.cancellation_source_kind IS NULL OR cancellation_source_kind IN (
  'MANAGEMENT_COMMAND', 'FORGE_OBSERVATION'
)
GUARDED Run.cancellation_source_kind/source_id mapping:
  MANAGEMENT_COMMAND => exact accepted CANCEL command_id for the Run
  FORGE_OBSERVATION   => exact same-Run Work Item closure forge_observation_id
GUARDED a non-null cancellation source is immutable, keeps the Run nonterminal
  while an owned or possibly-created Change Request is reconciled/closed, and
  is retained after terminal convergence as audit provenance
PRIMARY KEY HumanBoundary(human_boundary_id)
UNIQUE HumanBoundary(run_id, human_boundary_id)
UNIQUE HumanBoundary(human_boundary_id, run_id)
CHECK HumanBoundary.reason IN (
  'MISSING_AUTHORITY', 'REQUIRED_SECRET_OR_PERMISSION',
  'IRREVERSIBLE_DECISION', 'SPECIFICATION_CONFLICT',
  'SECURITY_POLICY_BOUNDARY', 'INTEGRITY_FAILURE',
  'UNSATISFIABLE_REQUIREMENTS', 'PUBLICATION_OWNERSHIP_CONFLICT'
)
CHECK HumanBoundary publication_id and publication_effect_generation
  are both NULL or both NOT NULL
CHECK HumanBoundary.created_from_kind IN (
  'RECOVERY_EVIDENCE', 'RECONCILIATION_FACT'
)
GUARDED HumanBoundary.created_from_id resolves to the exact owning-Run source;
  RECONCILIATION_FACT is allowed only for positive OWNERSHIP_CONFLICT, while
  every other reason follows allowlisted exhausted Recovery Evidence whose
  selected tactic is ENTER_HUMAN_BOUNDARY
GUARDED HumanBoundary.reason = 'PUBLICATION_OWNERSHIP_CONFLICT' requires
  created_from_kind = 'RECONCILIATION_FACT' and created_from_id resolves to an
  OWNERSHIP_CONFLICT Fact for the copied Run/Publication Effect binding; its
  ownership_project_id, ownership_deterministic_ref,
  ownership_change_request_external_id, and ownership_run_marker are all
  non-null and equal the registered Project, Publication, ordered Forge
  Observations, and syntactically valid Orcest v1 marker. Those four fields are
  NULL for every other boundary reason; the packet has exactly one choice,
  selecting continued ORCEST_V1 ownership through
  PUBLICATION_OWNERSHIP_RESOLVED, and contains no legacy/handoff choice
DEFERRABLE FOREIGN KEY Run(run_id, human_boundary_id)
  -> HumanBoundary(run_id, human_boundary_id)
PRIMARY KEY HumanResolution(human_resolution_id)
UNIQUE HumanResolution(human_boundary_id)
UNIQUE HumanResolution(human_boundary_id, idempotency_key)
CHECK HumanResolution.idempotency_key is bounded stable text and equals
  HumanResolution.source_id byte-for-byte after canonical source-kind encoding
CHECK HumanResolution.source_kind IN (
  'MANAGEMENT_COMMAND', 'FORGE_OBSERVATION', 'SECRET_VERSION',
  'STORAGE_RESTORATION'
)
CHECK HumanResolution.authenticated_principal_id IS NOT NULL
CHECK HumanResolution.resolution_kind IN (
  'AUTHORITY_GRANTED', 'SECRET_OR_PERMISSION_PROVIDED',
  'IRREVERSIBLE_ACTION_AUTHORIZED', 'SPECIFICATION_AMENDED',
  'SECURITY_ACTION_AUTHORIZED', 'INTEGRITY_RESTORED',
  'ENVIRONMENT_CAPABILITY_PROVIDED', 'PUBLICATION_OWNERSHIP_RESOLVED'
)
CHECK HumanResolution publication_id and publication_effect_generation
  are both NULL or both NOT NULL
GUARDED HumanResolution ownership_project_id,
  ownership_deterministic_ref, ownership_change_request_external_id, and
  ownership_run_marker equal the current HumanBoundary values for
  PUBLICATION_OWNERSHIP_RESOLVED and are all NULL for every other resolution
FOREIGN KEY HumanResolution(human_boundary_id, run_id)
  -> HumanBoundary(human_boundary_id, run_id)
FOREIGN KEY HumanResolution.forge_observation_id
  -> ForgeObservation(forge_observation_id)
GUARDED HumanResolution.source_kind/source_id mapping:
  MANAGEMENT_COMMAND => source_id/idempotency_key is lowercase command_id UUID
  FORGE_OBSERVATION   => source_id/idempotency_key is lowercase
                         forge_observation_id UUID
  SECRET_VERSION      => source_id is `<lowercase UUID>:<base-10 version>`
                         with no leading zero, idempotency_key is that exact
                         text, and that exact SecretVersion exists
  STORAGE_RESTORATION => source_id/idempotency_key is lowercase
                         storage_restoration_fact_id UUID and references
                         StorageRestorationFact(storage_restoration_fact_id)
GUARDED STORAGE_RESTORATION HumanResolution authority matrix:
  BACKUP_RESTORE => authenticated_principal_id is the registered controller
                    storage-reconciler service principal and resolution
                    evidence binds the verified backup authorization and
                    backup_manifest_digest
  AUTHENTICATED_STORAGE_OPERATION => authenticated_principal_id and
                                     authorization evidence equal the accepted
                                     operation and Restoration Fact authority
GUARDED REQUIRED_SECRET_OR_PERMISSION is resolved only by an exact newer
  SECRET_VERSION or authenticated MANAGEMENT_COMMAND; no generic permission
  fact or free-form source exists
GUARDED SECRET_VERSION HumanResolution uses the registered controller
  Secret-Store verifier/reconciler service principal, not a synthetic user or
  worker identity. The exact version is verified current and satisfies the
  Boundary's Secret/minimum-version binding; resolution evidence copies its
  immutable creation_receipt_id and, when present for that source, the matching
  rotation/provisioning authority. resolution_digest covers the Secret Version
  key, creation Receipt, Boundary bindings, service principal, and verification
  evidence. Automatic resolution adds no authority beyond proving that the
  already-authorized version is now available
GUARDED PUBLICATION_OWNERSHIP_RESOLVED requires source_kind =
  MANAGEMENT_COMMAND and resolution equal the closed object:
  {
    "selected_engine": "ORCEST_V1",
    "project_id": <exact registered Project ID>,
    "deterministic_ref": <exact Publication ref>,
    "change_request_external_id": <exact observed Change Request ID>,
    "run_marker": <exact syntactically valid Orcest v1 marker>,
    "publication_id": <exact copied Publication ID>,
    "effect_generation": <exact copied Publication Effect generation>
  }
  Every value must equal the current Boundary, Publication Effect, registered
  Project, and ordered Forge Observations. Selecting the legacy engine or
  transferring/removing an Orcest v1 marker is outside the v1 protocol.
FOREIGN KEY HumanBoundary(publication_id, publication_effect_generation)
  -> PublicationEffect(publication_id, effect_generation)
FOREIGN KEY HumanResolution(publication_id, publication_effect_generation)
  -> PublicationEffect(publication_id, effect_generation)
FOREIGN KEY ManagementCommand.human_resolution_id
  -> HumanResolution(human_resolution_id)
GUARDED SPECIFICATION_AMENDED installation is canonical only as
  `T(SPEC_SUPERSEDE, snapshot_id)`: its safe-boundary transaction commits the
  Forge-authenticated Human Resolution, boundary clear, Snapshot Generation,
  Run generation/Candidate update, SPEC_SUPERSEDE Transition, and planned work.
  The causal FORGE_OBSERVATION Transition merely captured the pending Snapshot
  earlier and never installs a generation
PRIMARY KEY Transition(run_id, transition_sequence)
CHECK Transition.transition_sequence > 0
CHECK Transition.trigger_kind IN (
  'ADMIT', 'INTERNAL', 'ATTEMPT_RESULT', 'ATTEMPT_TERMINAL',
  'CONTROLLER_OPERATION', 'FORGE_REQUEST_FAILURE', 'FORGE_OBSERVATION',
  'HEALTH_OBSERVATION', 'BUDGET_REPORT',
  'MANAGEMENT_COMMAND', 'POLICY_UPDATE', 'PUBLICATION_CHECKPOINT',
  'RECONCILIATION_FACT', 'RECOVERY_EVIDENCE', 'SECRET_VERSION',
  'SPEC_SUPERSEDE', 'STORAGE_RESTORATION', 'TIMER_FACT'
)
CHECK Transition.from_state is 'NONE' or a closed lifecycle Run state;
  Transition.to_state is always a real closed lifecycle Run state and never
  'NONE'
GUARDED Transition.from_state = 'NONE' only for transition_sequence = 1,
  trigger_kind = 'ADMIT', and creation of that Run; every later Transition's
  from_state equals the durable preceding Run state
GUARDED Transition.trigger_kind/trigger_id resolves exactly as follows:
  ADMIT                  => eligible WORK_ITEM_SNAPSHOT forge_observation_id;
                            anchored_base_observation_id is the exact trusted
                            BASE_HEAD composed into capture-sequence 1
  INTERNAL               => unsigned base-10 prior transition_sequence for
                            the same Run, with no leading zero
  ATTEMPT_RESULT         => accepted AttemptResult.attempt_id
  ATTEMPT_TERMINAL       => AttemptTerminalFact.attempt_terminal_fact_id
  CONTROLLER_OPERATION   => ControllerOperationFact.controller_operation_fact_id
  FORGE_REQUEST_FAILURE  => ForgeRequestFailureFact.forge_request_failure_fact_id
  FORGE_OBSERVATION      => ForgeObservation.forge_observation_id
  HEALTH_OBSERVATION     => HealthObservation.health_observation_id
  BUDGET_REPORT          => BudgetReport.budget_report_id
  MANAGEMENT_COMMAND     => ManagementCommand.command_id, including CANCEL
  POLICY_UPDATE          => PolicyUpdate.policy_update_id
  PUBLICATION_CHECKPOINT =>
                            PublicationEffectCheckpoint.publication_effect_checkpoint_id
  RECONCILIATION_FACT    => ReconciliationFact.reconciliation_fact_id
  RECOVERY_EVIDENCE      => RecoveryEvidence.recovery_evidence_id whose selected
                            tactic is being applied
  SECRET_VERSION         => `<lowercase UUID>:<base-10 version>` with no
                            leading zero and an exact SecretVersion row
  SPEC_SUPERSEDE         => Snapshot.snapshot_id installed at safe boundary
  STORAGE_RESTORATION    => StorageRestorationFact.storage_restoration_fact_id
  TIMER_FACT             => TimerFact.timer_fact_id only for
                            WAIT_CONDITION_NOT_BEFORE or
                            HEALTH_OBSERVATION_EXPIRY
GUARDED FORGE_REQUEST_FAILURE is legal only for a Run-bound Fact; Project
  discovery/pre-admission and terminal-cleanup Facts retry without a Run
  Transition. BUDGET_REPORT is legal only for a frozen BudgetReportRun member;
  Report fanout replay looks up the generation-independent Transition before
  advancing that member cursor
CHECK Transition.anchored_base_observation_id is non-null exactly when
  trigger_kind = 'ADMIT'; otherwise it is NULL
FOREIGN KEY Transition.anchored_base_observation_id
  -> ForgeObservation(forge_observation_id)
GUARDED an ADMIT anchor is kind BASE_HEAD, is trusted for that Project/default
  ref, and is the greatest eligible Work-Item-target BASE_HEAD
  observation_sequence accepted before the admission writer transaction. It
  equals capture-sequence-1 Snapshot.base_observation_id and commits with
  the Run/Snapshot/ADMIT Transition. For one Run, neither its ADMIT trigger nor
  anchor may equal an Observation consumed by any other ADMIT or
  FORGE_OBSERVATION Transition; both are ordered at the ADMIT transition
  sequence and neither can later become a reducer trigger
UNIQUE Transition(run_id, trigger_kind, trigger_id)
UNIQUE Transition(run_id, trigger_id)
  WHERE trigger_kind IN ('ADMIT', 'FORGE_OBSERVATION')
GUARDED Transition.specification_generation is the immutable evaluated
  generation for audit and digest reconstruction, never part of replay identity
GUARDED every accepted ForgeObservation applicable to an active Run has exactly
  one FORGE_OBSERVATION Transition, including a same-state Transition when it
  changes no lifecycle state. Admission is not a single-observation exception:
  its eligible WORK_ITEM_SNAPSHOT trigger and exact anchored BASE_HEAD are both
  consumed and ordered by the one ADMIT Transition as guarded above. Later
  deterministic work reads the persisted projection through INTERNAL and
  cannot reuse either observation under another generation
GUARDED one reducer invocation for one `(run_id, trigger_kind, trigger_id)`
  writes exactly one Run Transition. Activities, Attempts, Outboxes, Facts,
  memberships, and projection intents produced by that reduction may commit
  atomically with it, but they are outputs rather than additional Transitions.
  Any required next decision uses the one closed follow-up trigger named by the
  lifecycle; a transaction never folds two reducer steps into one Transition
GUARDED an INTERNAL continuation has the exact identity
  `(run_id, 'INTERNAL', entering_transition_sequence)` and the reducer applies
  this closed precedence: first, pending cancellation fences/supersedes
  semantic work and permits only cancellation reconciliation; otherwise an
  eligible pending_snapshot_id schedules the separate SPEC_SUPERSEDE and no
  dependency or state work; otherwise a still-unsatisfied/unknown pending
  dependency appends its Recovery Evidence and no state work; otherwise, when
  the coalesced panel-staffing pointer names this entering sequence and no peer
  remains CLAIMED, its one all-or-none staffing evaluation applies; otherwise
  the state-specific continuation applies, except that RECOVERING has no
  INTERNAL work path because all recovery work is owned by RECOVERY_EVIDENCE. One
  entering Transition has at most one
  such continuation. Whenever work remains, that continuation records exactly
  one Transition, even if same-state; a later chained continuation uses the
  new entering sequence and never reuses the prior trigger ID
GUARDED an Attempt-claim or execution-deadline Timer Fact never creates a
  TIMER_FACT Transition: its exact AttemptTerminalFact is the sole Run trigger.
  A Recovery-eligibility Timer Fact likewise has no direct Run Transition; it
  makes the immutable RecoveryEvidence eligible, and only
  T(RECOVERY_EVIDENCE, recovery_evidence_id) applies its tactic. Timer Fact
  insertion/cursor bookkeeping is never an extra lifecycle transition
GUARDED a PUBLICATION_CHECKPOINT Transition references only an `AMBIGUOUS`
  checkpoint with forge_observation_id IS NULL whose reconciliation decision
  has not already been reduced. `REQUEST_READY` is an output of the preceding
  Transition/outbox transaction, not a reducer trigger. An observation-backed
  checkpoint commits as an effect of its sole FORGE_OBSERVATION Transition and
  is never reduced again. Controller restart alone appends no checkpoint and
  no Transition; it resumes the highest durable checkpoint/outbox identity
GUARDED replay of every trigger looks up the member Transition independent of
  the Run's current generation and returns its original evaluated
  specification_generation; generation advance cannot apply any persisted
  causal input twice. ADMIT/FORGE_OBSERVATION additionally share the stronger
  cross-kind `(run_id, trigger_id)` identity
FOREIGN KEY Candidate.bundle_digest -> ArtifactObject.bundle_digest
FOREIGN KEY every receipt/decision(candidate_id, commit_object_format, commit_oid)
  -> Candidate(candidate_id, commit_object_format, commit_oid)
PRIMARY KEY SecretProvisionOperation(secret_provision_operation_id)
CHECK SecretProvisionOperation.protocol_version = 'orcest.secret-provision/1'
CHECK SecretProvisionOperation.mode IN ('PROVISION', 'ADOPT_EXISTING')
CHECK SecretProvisionOperation.owner_scope_kind IN (
  'PROJECT', 'FORGE_INSTALLATION', 'CONTROLLER'
)
CHECK SecretProvisionOperation.secret_id, purpose, owner_scope_id,
  authenticated_principal_id, authorization_context_digest,
  secret_store_staging_receipt_id, secret_integrity_attestation_id,
  target_version, request_digest, and created_at_ms are non-null and bounded
CHECK SecretProvisionOperation.target_version > 0
CHECK SecretProvisionOperation.state IN ('PENDING', 'COMPLETED', 'REJECTED')
CHECK SecretProvisionOperation outcome projection:
  PENDING   => credential_rotation_receipt_id, new_version, rejection_code,
               terminal_http_status, terminal_response_json,
               terminal_response_digest IS NULL
  COMPLETED => credential_rotation_receipt_id IS NOT NULL AND new_version > 0;
               new_version = target_version;
               rejection_code IS NULL; terminal_http_status = 200;
               terminal_response_json,
               terminal_response_digest IS NOT NULL
  REJECTED  => credential_rotation_receipt_id, new_version IS NULL;
               rejection_code, terminal_http_status, terminal_response_json,
               terminal_response_digest IS NOT NULL
CHECK SecretProvisionOperation rejected HTTP mapping:
  CAS_LOST             => terminal_http_status = 409
  AUTHORITY_REVOKED    => terminal_http_status = 403
  STAGED_OBJECT_INVALID => terminal_http_status = 422
  INTEGRITY_CONFLICT   => terminal_http_status = 409
CHECK SecretProvisionOperation.expected_prior_version IS NULL OR
  expected_prior_version > 0
UNIQUE SecretProvisionOperation(credential_rotation_receipt_id)
  WHERE credential_rotation_receipt_id IS NOT NULL
UNIQUE SecretProvisionOperation(secret_id, new_version)
  WHERE new_version IS NOT NULL
UNIQUE SecretProvisionOperation(secret_id, target_version)
  WHERE state IN ('PENDING', 'COMPLETED')
FOREIGN KEY SecretProvisionOperation.credential_rotation_receipt_id
  -> CredentialRotationReceipt(credential_rotation_receipt_id)
FOREIGN KEY SecretProvisionOperation(secret_id, new_version)
  -> SecretVersion(secret_id, version)
FOREIGN KEY SecretProvisionOperation.last_checkpoint_id
  -> SecretProvisionCheckpoint(secret_provision_checkpoint_id)
GUARDED owner_scope_kind PROJECT resolves owner_scope_id to exact Project;
  FORGE_INSTALLATION is the sole pre-Project owner and requires owner_scope_id
  = provider_account_ref = the canonical installation_or_account_ref plus one
  of the three distinct purposes FORGE_API, SOURCE_READ, or PUBLICATION;
  CONTROLLER accepts only a code-owned controller scope ID. Purpose and
  provider_account_ref nullability are code-owned for every scope. Neither
  registration nor rotation may retag an owner or reuse one purpose as another
GUARDED the authenticated principal has server-owned SECRET_PROVISION for mode
  PROVISION or SECRET_ADOPT_EXISTING for mode ADOPT_EXISTING on the exact
  Secret/owner/purpose/account/CAS. Repository policy cannot grant either
GUARDED PROVISION streams bytes directly to protected Secret Store staging;
  ADOPT_EXISTING consumes an adapter-internal one-use protected locator. The
  locator and bytes never enter SQLite/Redis/logs/traces/response; SQLite stores
  only the opaque staging receipt and integrity attestation
GUARDED PENDING acceptance allocates target_version under the single writer and
  shared storage lock without changing SecretRef.current_version. It is 1 when
  expected_prior_version is NULL and otherwise expected_prior_version + 1;
  every request/proof/checkpoint digest and storage identity binds it
  immutably. Restart and retry reuse it and never allocate another version
GUARDED PENDING acceptance also commits exactly one
  `SECRET_PROVISION_OPERATION` Outbox source row whose source_id is this
  Operation and whose payload binds request_digest and target_version. The
  request's protected streaming/adoption, fsync, and opaque staging proof have
  already occurred as bounded authenticated intake; no asynchronous
  VERIFY_STAGING or INSTALL_VERSION reconciliation I/O begins before this
  durable intent. A terminal checkpoint
  atomically marks that same Outbox delivered; restart resumes a PENDING
  Operation from this durable source rather than a separate retry ledger
GUARDED a later Operation may reuse a target released by REJECTED only while
  holding the per-Secret/storage lock and only after proving there is no Secret
  Version, current PENDING/COMPLETED Operation, or installed immutable target at
  that key; rejected staging for the old Operation must already be safely
  quarantined. The rejected Operation retains its target as audit evidence but
  is not a live reservation
GUARDED identical authenticated operation ID, request_digest, principal,
  authorization, and Secret Store proof returns the same Operation. While
  PENDING, the endpoint deterministically projects 202 from immutable fields
  and the latest checkpoint; once terminal, it returns the stored terminal
  response. Any reuse mismatch is an integrity conflict
GUARDED terminal_response_digest covers terminal_http_status and the exact
  canonical terminal_response_json; terminal replay returns both unchanged
PRIMARY KEY SecretProvisionCheckpoint(secret_provision_checkpoint_id)
UNIQUE SecretProvisionCheckpoint(
  secret_provision_operation_id, checkpoint_sequence
)
CHECK SecretProvisionCheckpoint.checkpoint_sequence > 0
CHECK SecretProvisionCheckpoint.phase IN ('VERIFY_STAGING', 'INSTALL_VERSION')
CHECK SecretProvisionCheckpoint.outcome IN (
  'SUCCEEDED', 'FAILED_RETRYABLE', 'FAILED_TERMINAL'
)
CHECK SecretProvisionCheckpoint conditional evidence:
  SUCCEEDED        => failure_code, failure_evidence_digest, next_retry_ms IS NULL
  FAILED_RETRYABLE => failure_code IN (
                        'SECRET_STORE_UNAVAILABLE', 'TRANSIENT_STORAGE_ERROR',
                        'TRANSIENT_DATABASE_BUSY'
                      ); failure_evidence_digest, next_retry_ms IS NOT NULL
                      and bounded
  FAILED_TERMINAL  => failure_code IN (
                        'CAS_LOST', 'AUTHORITY_REVOKED',
                        'STAGED_OBJECT_INVALID', 'INTEGRITY_CONFLICT'
                      ); failure_evidence_digest IS NOT NULL;
                      next_retry_ms IS NULL
UNIQUE SecretProvisionCheckpoint(secret_provision_operation_id)
  WHERE outcome IN ('SUCCEEDED', 'FAILED_TERMINAL')
FOREIGN KEY SecretProvisionCheckpoint.secret_provision_operation_id
  -> SecretProvisionOperation(secret_provision_operation_id)
GUARDED checkpoint_sequence is allocated as max+1 in the single-writer
  transaction; last_checkpoint_id is NULL only before the first attempt and,
  otherwise, names that Operation's greatest committed sequence
GUARDED outcome SUCCEEDED is valid only for phase INSTALL_VERSION
GUARDED a PENDING Operation has exactly one generic source-tagged Outbox row
  with source_kind = SECRET_PROVISION_OPERATION and source_id equal to the
  Operation ID. Acceptance inserts it; a FAILED_RETRYABLE checkpoint advances
  that same Outbox row's next eligible time and returns it to PENDING;
  COMPLETED or REJECTED makes it DELIVERED. There is no second provision-
  specific outbox relation. Startup reconstructs the same due set from the
  PENDING Operation, checkpoint chain, and generic Outbox, so Redis loss is
  irrelevant
GUARDED COMPLETED requires exactly one INSTALL_VERSION/SUCCEEDED checkpoint;
  that checkpoint, Credential Rotation Receipt, Secret Version, SecretRef CAS,
  frozen affected-Run membership/digest, durable Secret Version fanout intent,
  Operation projection, and source Outbox delivery commit in one writer
  transaction. Per-Run Transitions are not part of this transaction
GUARDED REJECTED requires its latest checkpoint outcome FAILED_TERMINAL and
  rejection_code equal that checkpoint's closed failure_code. The checkpoint,
  terminal projection, canonical non-secret terminal_response_json/digest, and
  code-owned terminal_http_status, and source Outbox delivery commit atomically;
  no Receipt, Version, reference CAS,
  Run fanout, or lifecycle Transition is created
PRIMARY KEY CredentialRotationRequest(credential_rotation_request_id)
CHECK CredentialRotationRequest.protocol_version = 'orcest.credential-rotation/1'
UNIQUE CredentialRotationRequest(credential_rotation_receipt_id)
  WHERE credential_rotation_receipt_id IS NOT NULL
FOREIGN KEY CredentialRotationRequest(
  attempt_id, activity_id, attempt_generation
) -> Attempt(attempt_id, activity_id, generation)
DEFERRABLE FOREIGN KEY CredentialRotationRequest.credential_rotation_receipt_id
  -> CredentialRotationReceipt(credential_rotation_receipt_id)
CHECK CredentialRotationRequest attempt_id, activity_id, attempt_generation,
  worker_id, worker_session_id, attempt_capability_digest,
  launch_attestation_id, provider_account_ref, secret_id,
  expected_prior_version, secret_request_attestation_id, request_digest,
  disposition, current_version, response_http_status, response_json,
  response_digest, and accepted_at_ms are non-null and bounded
CHECK CredentialRotationRequest.attempt_generation > 0
CHECK CredentialRotationRequest.expected_prior_version > 0
CHECK CredentialRotationRequest.current_version > 0
CHECK CredentialRotationRequest.disposition IN ('APPLIED', 'CAS_LOST')
CHECK CredentialRotationRequest disposition union:
  APPLIED     => credential_rotation_receipt_id IS NOT NULL;
                 accepted_version > 0;
                 current_version = accepted_version;
                 response_http_status = 200
  CAS_LOST    => credential_rotation_receipt_id, accepted_version IS NULL;
                  response_http_status = 409
GUARDED `CAS_LOST` is the credential-rotation request disposition for a
  lost expected-prior CAS. SecretProvisionOperation also uses `CAS_LOST` as a
  terminal rejection code, but each remains in its own closed protocol and
  durable Operation/Request identity; neither is a Receipt identity. Both are
  stable 409/no-new-Version outcomes
GUARDED CredentialRotationRequest.accepted_at_ms < the referenced
  Attempt.execution_deadline_ms
GUARDED CredentialRotationRequest.response_digest covers response_http_status
  and exact canonical orcest.credential-rotation-result/1 body containing only
  request ID, disposition, Secret ID, expected prior version, current version,
  and conditional accepted version/Receipt ID; fields from the other variant,
  replay flags, free-form error, and opaque attestation are forbidden
GUARDED request_digest covers every canonical non-secret authority field plus
  secret_request_attestation_id. The Secret Store uses that opaque controller-
  only keyed request attestation to decide body equality; raw bytes, an unkeyed
  body digest, key, or tag never enter SQLite/API/logs. Same exact authenticated
  request ID/bindings/digest/attestation returns stored status/body; conflict
  creates no new authority
GUARDED first acceptance requires exact current CLAIMED model-backed Attempt,
  worker/session/capability, accepted Launch Attestation, account/Secret/prior,
  and controller_now_ms < execution_deadline_ms. APPLIED atomically inserts the
  Request, reciprocal Receipt, Version, SecretRef CAS, frozen membership/digest,
  and durable fanout intent. CAS_LOST inserts only Request/stored response
  with observed current version and no Receipt/Version/ref/fanout/Result/
  Transition. Before the execution deadline, a terminal Attempt permits only
  exact existing Request replay, never new rotation authority. At or after the
  execution deadline the rotation endpoint denies both first acceptance and
  replay; only the Result endpoint uses capability-authentication grace
PRIMARY KEY CredentialRotationReceipt(credential_rotation_receipt_id)
UNIQUE CredentialRotationReceipt(source_kind, source_id)
UNIQUE CredentialRotationReceipt(secret_id, new_version)
CHECK CredentialRotationReceipt.source_kind IN (
  'ATTEMPT_ROTATION', 'MANAGEMENT_PROVISION'
)
CHECK CredentialRotationReceipt.new_version > 0
CHECK CredentialRotationReceipt.expected_prior_version IS NULL OR
  expected_prior_version > 0
CHECK CredentialRotationReceipt source-authority tagged union:
  ATTEMPT_ROTATION => source_id is the lowercase worker request idempotency UUID;
                      credential_rotation_request_id IS NOT NULL;
                      attempt_id, activity_id, attempt_generation, worker_id,
                      worker_session_id, attempt_capability_digest,
                      launch_attestation_id IS NOT NULL;
                      management_operation_id, authenticated_principal_id,
                      authorization_context_digest IS NULL
  MANAGEMENT_PROVISION => source_id is lowercase management_operation_id UUID;
                          credential_rotation_request_id IS NULL;
                          management_operation_id, authenticated_principal_id,
                          authorization_context_digest IS NOT NULL;
                          attempt_id, activity_id, attempt_generation, worker_id,
                          worker_session_id, attempt_capability_digest,
                          launch_attestation_id IS NULL
FOREIGN KEY attempt CredentialRotationReceipt(
  attempt_id, activity_id, attempt_generation
) -> Attempt(attempt_id, activity_id, generation)
FOREIGN KEY CredentialRotationReceipt.launch_attestation_id
  -> LaunchAttestation(launch_attestation_id)
DEFERRABLE FOREIGN KEY CredentialRotationReceipt.credential_rotation_request_id
  -> CredentialRotationRequest(credential_rotation_request_id)
FOREIGN KEY CredentialRotationReceipt.management_operation_id
  -> SecretProvisionOperation(secret_provision_operation_id)
GUARDED ATTEMPT_ROTATION first acceptance requires the exact current CLAIMED
  model-backed Attempt/session/capability, its accepted Launch Attestation,
  matching provider_account_ref and provider Secret ID, current Reference at
  expected_prior_version, and controller_now_ms < execution_deadline_ms. Once
  Result acceptance or any terminal fence wins, only identical existing
  CredentialRotationRequest response replay is allowed and only while
  controller_now_ms < execution_deadline_ms; Receipt identity alone is never
  a replay or authentication key, and replay cannot create a new version
GUARDED ATTEMPT_ROTATION source_id equals credential_rotation_request_id and
  the reciprocal APPLIED Request's Receipt/version/current version and every
  Attempt/session/capability/Attestation/account/Secret/prior binding match;
  CAS_LOST cannot have a Receipt
GUARDED MANAGEMENT_PROVISION requires the exact authenticated server secret-
  management operation, principal, authorization context, purpose/scope, and
  expected prior version; repository content and generic management commands
  cannot supply it
CHECK CredentialRotationReceipt.secret_integrity_attestation_id,
  receipt_digest, and created_at_ms are non-null
GUARDED CredentialRotationReceipt.receipt_digest covers every canonical
  non-secret provenance field, including source union, exact version CAS,
  account/scope, launch binding when applicable, authenticated management
  authority when applicable, and opaque integrity-attestation ID
GUARDED MANAGEMENT_PROVISION Operation, Receipt, SecretVersion with reciprocal
  creation receipt, SecretRef create/CAS, frozen SecretVersionRun membership,
  membership digest, and durable SecretVersionFanout intent commit in one
  write-before-reference transaction; per-Run reductions commit separately
PRIMARY KEY SecretVersion(secret_id, version)
CHECK SecretVersion.version > 0
CHECK SecretVersion.creation_receipt_id IS NOT NULL
UNIQUE SecretVersion(creation_receipt_id)
DEFERRABLE FOREIGN KEY SecretVersion.creation_receipt_id
  -> CredentialRotationReceipt(credential_rotation_receipt_id)
DEFERRABLE FOREIGN KEY CredentialRotationReceipt(secret_id, new_version)
  -> SecretVersion(secret_id, version)
FOREIGN KEY SecretVersion.secret_id -> SecretRef(secret_id)
CHECK SecretVersion.affected_run_ids_digest is non-null and equals SHA-256 of
  the canonical length-prefixed ordered SecretVersionRun.run_id membership,
  including the canonical empty list
CHECK SecretVersion.storage_path is non-null, relative, normalized, and unique
GUARDED every SecretVersion path has matching immutable controller-only Secret
  Store metadata containing a domain-separated keyed authenticator over the
  canonical Secret Version key and exact stored bytes; neither its key nor tag
  is stored in SQLite or exposed through workflow, Redis, projection, or
  management output
PRIMARY KEY SecretVersionRun(secret_id, version, run_ordinal)
UNIQUE SecretVersionRun(secret_id, version, run_id)
CHECK SecretVersionRun.run_ordinal >= 0 and ordinals per Secret Version are
  zero-based and contiguous
FOREIGN KEY SecretVersionRun(secret_id, version)
  -> SecretVersion(secret_id, version)
FOREIGN KEY SecretVersionRun.run_id -> Run(run_id)
PRIMARY KEY SecretVersionFanout(secret_id, version)
CHECK SecretVersionFanout.state IN ('PENDING', 'DELIVERED')
CHECK SecretVersionFanout.next_run_ordinal >= 0
FOREIGN KEY SecretVersionFanout(secret_id, version)
  -> SecretVersion(secret_id, version)
GUARDED membership is the canonically byte-sorted active Runs whose current
  SECRET_RECOVERY Wait or REQUIRED_SECRET_OR_PERMISSION Boundary names the
  Secret ID and a minimum version satisfied by this newly current version; the
  reference/version update, full membership/digest, and one fanout intent
  commit atomically. The intent starts PENDING for nonempty membership and
  DELIVERED for empty membership
GUARDED SecretVersionFanout.next_run_ordinal is at most the membership count;
  PENDING means it is smaller than the count and DELIVERED means it equals the
  count. Empty membership commits DELIVERED at ordinal 0. Startup and ordinary
  reconciliation enumerate PENDING intents; Redis is not involved
GUARDED each fanout step loads exactly next_run_ordinal and, in one independent
  writer transaction, appends/replays that member Run's
  T(SECRET_VERSION, canonical Secret Version key) Transition and advances the
  cursor by one. If the frozen member no longer has the named current Wait or
  Boundary, it still records the specified same-state stale-member Transition.
  Generation-independent trigger uniqueness makes retry idempotent. A prefix
  of member Transitions after a crash is valid and resumes at the next ordinal
PRIMARY KEY SecretRef(secret_id)
DEFERRABLE FOREIGN KEY SecretRef(secret_id, current_version)
  -> SecretVersion(secret_id, version)
```

### Project registration transaction

`POST /api/v1/projects/registrations` accepts exact protocol
`orcest.project-registration/1` and a lowercase UUID `Idempotency-Key` equal
to the body field. Authentication, bounded schema validation, RBAC and
server-registration resolution, forge repository/base reads, and trusted
workflow/policy validation occur read-only before the commit. They never place
credentials, Secret References, forge bodies, or runner signing material in
the Operation request/response.

One writer transaction claims `(authenticated_principal_id, idempotency_key)`,
persists the immutable terminal ProjectRegistrationOperation and canonical
response, and, only for `SUCCEEDED`, inserts or CAS-updates the Project.
The successful Operation freezes the non-secret installation/account reference
and internally resolved versioned source-read/publication Secret References
authorized by it; these internal refs are never request/response JSON fields.
The Project copies their logical Secret IDs plus registration-provenance
versions, not mutable-current versions. Claims and Publication Effects resolve
and freeze current versions independently under their own writer transactions.
`REGISTER` requires no requested Project/revision and creates registration
revision 1 with `registration_operation_id` pointing to that Operation.
The same writer transaction creates the unique PROJECT-target
`WORK_ITEM_DISCOVERY` Schedule as ACTIVE revision 0, with no prior Request or
discovery result and `next_due_at_ms = completed_at_ms`. It copies the Schedule
ID to `Project.work_item_discovery_schedule_id` and
`ProjectRegistrationOperation.result_work_item_discovery_schedule_id`; the
Operation's internal `resolution_digest` covers it. A
crash therefore exposes neither a Project without discovery ownership nor a
Schedule without its Project/registration provenance.
`REVALIDATE` requires the exact Project and
`expected_registration_revision`, updates only while the current row equals
that value, requires requested default/trusted-base/budget/reset references
and installation/account reference equal the current Project, and installs
value + 1 while refreshing only mutable
  locator/readiness projections and replacing `registration_operation_id` with
  this successful Operation. It retains the same discovery Schedule and copies
  its ID to the new successful Operation result. It copies the Operation's exact logical Secret
  IDs and registration-provenance versions into the Project; a later mutable
  installation-registry change cannot rewrite them. Changing installation/account registration is outside
`REVALIDATE`. The reciprocal successful Operation result and
Project pointer/revision use deferred exact-match foreign keys and commit
atomically. A revision or authority-reference mismatch
returns 409 and commits neither an Operation nor a Project change. Changes to
authority-bearing references use an authenticated SERVER_ROLLOUT Policy Update,
not registration. An accepted validation/capability rejection commits only the
`REJECTED` Operation and response.

An identical same-principal/key/request-digest replay returns the stored
response and sets its transport `replayed` projection; conflicting same-key
content returns 409. A crash before commit safely repeats read-only resolution;
a crash after commit returns the stored result. Redis never participates.
Operation request/response bytes and authorization evidence remain in ordinary
SQLite backup and retention at least as long as the resulting Project or the
registration idempotency window, whichever is longer.

### Snapshot and workflow-input transactions

Workflow Blob bytes live in SQLite so the ordinary database backup contains
the exact non-secret configuration, prompts, effective policy, and server
policy inputs needed for replay. Before a Snapshot or Policy Update row can
reference a blob, the writer verifies `byte_length` and the exact
domain-separated `orcest-workflow-blob-v1` digest over media kind, unsigned
64-bit big-endian length, and normalized bytes; inserts it with no-replace
semantics; and verifies digest, bytes, length, and media kind on conflict.
Identical bytes in different media kinds intentionally have different IDs.
Snapshot prompt membership is stored as
relational, path-sorted rows rather than relying on an unchecked JSON array.
The server's normalized configuration, prompt, and policy size ceilings apply
before the bytes enter SQLite.

Admission's first writer transaction commits the generation-0 `ADMITTED` Run,
capture-sequence-1 Snapshot as `pending_snapshot_id`, every configuration,
prompt, and effective-policy Workflow Blob and prompt membership row, the
`NONE -> ADMITTED` ADMIT Transition whose `anchored_base_observation_id`
equals that Snapshot's exact trusted `base_observation_id`, selected as the
greatest eligible Work-Item-target BASE_HEAD observation_sequence accepted
before the writer transaction, and Projection
intent. The eligible Work Item trigger and anchored base are both consumed and
ordered at this Transition sequence. It installs no
Snapshot Generation and creates no Activity/Attempt/outbox. The unique next
reduction `T(SPEC_SUPERSEDE, snapshot_id)` inserts generation 1, installs that
Snapshot, and clears the pending pointer while remaining `ADMITTED`; only then
does `T(INTERNAL, spec_transition_sequence)` enter `PLANNING` and atomically
create the initial PLAN Activity. It includes the OFFERED Attempt/outbox only
when Controller Mode and issuance-key gates permit; otherwise PLAN remains
PLANNED for the offer reconciler. Each transaction is
independently replayable, so restart may resume either missing continuation
without letting ADMIT or INTERNAL install a Snapshot. A server Policy Update transaction persists its exact
`SERVER_POLICY_JSON` bytes, allocates the next per-Project update sequence, and
records its authenticated source identity. The same transaction compare-and-
swaps the Project's `default_ref` and trusted-base/budget/reset registration
references to the exact values on the Update (the Policy Update row owns the
exact server policy revision), and inserts one
Policy Update Composition for every
then-active Run. Each composition freezes two independently ordered inputs as
they exist in that transaction: the exact latest accepted `WORK_ITEM_SNAPSHOT`
Observation for the Run's Work Item and the exact latest accepted applicable
trusted `BASE_HEAD` Observation. The latter may have advanced without causing
a specification Snapshot to be installed. Merely changing controller
configuration does not mutate Project or installed Run policy. Replaying the
same source identity and canonical update digest returns its existing row and
composition set; reuse with a different revision, blob, registration-policy
reference, principal, or frozen Observation pair is an integrity conflict.

Policy Update fan-out is restartable: after the update commits, the writer
enumerates eligible active Runs in stable Run-ID order and creates at most one
Snapshot per `(run_id, POLICY_UPDATE, policy_update_id)`. For each Run it
loads the immutable Policy Update Composition rather than making a new
"latest" choice. The frozen Work Item Observation supplies title, body,
opted-in comments, and forge revision, including not-yet-installed
specification B. The independently frozen base Observation supplies the exact
trusted `base_ref` and `base_commit`; repository workflow and prompt blobs are
loaded and normalized from that exact commit. The writer combines those inputs
with the Policy Update's server policy and Project registration references and
persists the complete resulting `POLICY_JSON` and hashes. It MUST NOT compose
from `Run.current_snapshot_id` or that Snapshot's base merely because it is
installed. Thus pending specification B plus independently advanced base C and
policy update P produces B+C+P, never installed A+old-base+P that would
overwrite B or ignore C. A delayed or retried repository read remains pinned
to the composition's base Observation and fails/retries normally if that exact
commit cannot yet be fetched; it cannot silently substitute a newer head.

A live execution-registry mapping, provider/model family classification, or
classification-revision change reaches an active Run only through this Policy
Update capture and the lifecycle's safe Snapshot-generation boundary. Attempt
creation never consults a mutable live registry or rewrites an existing
Attempt's pinned execution/classification values.

A crash between Runs leaves the Policy Update and completed source-unique
captures durable; reconciliation resumes only the missing captures using the
same persisted composition even if a newer Work Item observation has since
arrived or the trusted base has advanced again. Every
Run remains governed by its installed Snapshot until its own safe-boundary
Snapshot Generation transaction commits.

A later eligible Work Item/base observation or Policy Update that requires
Snapshot capture always inserts a new immutable Snapshot at the next per-Run
`snapshot_sequence`, whether its resulting `supersession_key` equals or differs
from the installed key. The capture includes the
complete recomputed `POLICY_JSON`, exact registration-policy references, server
policy revision, reducer version, and policy-specific `supersession_key`. It
then deterministically replaces or clears `Run.pending_snapshot_id` under the
Run-local capture order by comparing that key with the installed Snapshot's
key. When a differing capture finds a current CLAIMED Attempt or fenced
controller Activity, the same Transition sets `supersede_requested = true` and
stores its exact sequence in `supersede_requested_transition_sequence`; a
newer differing capture replaces both pointer and sequence. Without such
in-flight work it leaves the pair false/NULL and schedules the safe-boundary
install. Coalescing back to the installed key clears the pending pointer and
the pair atomically. Replacing
that pointer does not delete an older pending capture or its source input. This
represents `A -> B -> C` while retaining B for audit, and `A -> B -> A` while
clearing the pending request rather than installing B.

Publication `ACTIVE` cuts off only an input whose sole normative change is a
trusted-base advance under `SUPERSEDE_AT_BOUNDARY`. That base-only input remains
an ordered durable audit input with its normal one-time Transition but MUST NOT
create a Snapshot, set `pending_snapshot_id`, or install a new Snapshot
Generation for that Run; later conflict/head feedback follows ordinary
post-publication remediation under the installed Snapshot. Specification,
workflow, and effective-policy changes continue to capture/coalesce pending
Snapshots and install at their ordinary safe boundary after `ACTIVE`; a Policy
Update is not suppressed merely because it also carries the latest required
base binding. Capture and `SPEC_SUPERSEDE` guards therefore test the normalized
changed-input class, not Publication state alone, so restart cannot turn a
post-`ACTIVE` base-only observation into a generation or incorrectly discard a
specification/workflow/policy change.

For `REBASE_BEFORE_PUBLICATION` and `PIN`, a base-only capture has
`supersession_key = generation_input_hash` and does not itself create pending
supersession. For `SUPERSEDE_AT_BOUNDARY`, the key additionally covers the
canonical base commit, so the same base-only capture becomes pending and is
installed at the safe boundary. A Forge Observation that creates or coalesces
a capture is still reduced exactly once under its observation ID; the Snapshot
is a transaction output, never a second observation trigger.

At a safe boundary, a specification, workflow, or trusted-base change commits
one transaction that increments `specification_generation` by exactly one,
inserts the Snapshot Generation for the selected pending Snapshot, updates
`current_snapshot_id`, clears `pending_snapshot_id` plus
`supersede_requested`/its sequence, supersedes prior unfinished
work, clears the current Candidate and stale Wait/Human pointers, appends the
`SPEC_SUPERSEDE` Transition, and plans `REPLAN`.

For an explicit policy-only Snapshot—identical specification hash, workflow
hash, base ref, and base commit, but a different policy hash—the safe-boundary
transaction instead retains the current Candidate when present while making
all old-policy Verification, Review, Adjudication, and Consensus rows
ineligible for gating. It increments and installs the generation, clears stale
Wait/Human pointers and the pending Snapshot/supersede flag/sequence, and
appends the `SPEC_SUPERSEDE` Transition for the
pending Snapshot; the Policy Update source was already reduced once when the
capture committed. It always enters `REPLANNING` and atomically plans a
`REPLAN` Activity bound to the newly installed Snapshot and, when present, the
retained Candidate. Direct entry to `VERIFYING` is forbidden.

Only a schema-valid successful `REPLAN` under that exact policy-only identity
may retain the same Candidate and route it to the new generation's complete
default Verification and fresh review path without a `BUILD`. If the new Plan
requires code change or does not prove that identity, normal lifecycle planning
selects `BUILD`/replacement work. Every Review/Adjudication assignment freezes
its closed subject list from this newly accepted Plan (`snapshot:overall`
followed by each new Plan requirement), never from the superseded Plan. No old
gate or review subject carries over, even when the new policy is weaker.

Every accepted Attempt Result and Attempt Terminal transaction re-runs the
global continuation precedence under the writer before it emits destination
work. It may store the Result/Receipt/Candidate or terminalize/fence the current
Attempt, but cancellation first, then a true
`supersede_requested`/eligible pending Snapshot, then the pending dependency
pointer suppresses ordinary next semantic work. A panel Result/Terminal that
still has unfilled slots and any peer CLAIMED atomically replaces the four Run
panel-staffing fields with its own Candidate/round/kind/Transition and
discharges the older continuation. Once no peer remains claimed, only the
latest stored sequence evaluates all still-unfilled slots all-or-none; it
clears the projection when it creates offers or one complete panel Wait.

### Durable recovery, waiting, and exceptional boundaries

Every entry to `RECOVERING` persists the exact nonterminal origin, optional
origin Activity, entry trigger kind/ID, and the closed resume-source shape in
the same writer transaction as the entry Transition. A wake from `WAITING`
copies the immutable Wait Condition `resume_state` and retains its ID; a
resolved Human Boundary copies the Boundary `resume_state` and retains both
Boundary and Resolution IDs. A direct recovery entry leaves both resume shapes
empty. Recovery never infers an origin from the prior state after restart. The
entry transaction also appends exactly one next typed Recovery Evidence and
plans no recovery work. Only that Evidence's later
`T(RECOVERY_EVIDENCE, recovery_evidence_id)` may create its selected Activity,
Attempt/outbox, Wait, Human Boundary, probe, or other tactic effect. There is
no RECOVERING-specific INTERNAL shortcut, including for durable-store repair
or restart. Startup rejects or repairs no `RECOVERING` projection by guesswork:
it resumes the stored unapplied Evidence or fails closed on a broken binding.
Leaving recovery clears every entry/resume field atomically.

An ordered unsatisfied or unknown `DEPENDENCY_STATE` input commits its frozen
required-dependency-set digest, observation ID, and same-state Transition
sequence as the Run's pending-dependency triple. It prevents planning new
semantic work after the current Attempt reaches a safe boundary. Newer ordered
dependency evidence may replace or clear the triple; the safe-boundary
continuation either clears it after authoritative satisfaction or atomically
turns it into the bound recovery/wait path. Restart resumes from the triple and
its exact Transition, never from Redis or by re-reading a mutable forge view.

Entering `WAITING` inserts one immutable Wait Condition and sets the Run's
`wait_condition_id` in the same Transition transaction. The writer enforces
that the pointer is non-null exactly while the Run is `WAITING`, belongs to the
same Run, and that its specification, Candidate, policy, and Forge bindings
match the transition. Leaving or replacing the wait clears or changes only the
Run pointer; historical conditions remain immutable. A timer, health fact, or
external wake is persisted before reducer evaluation, and the reducer rechecks
the condition digest and current bindings rather than treating the wake-up as
proof that the condition is satisfied. Each row stores the exact
`created_from_kind/created_from_id` persisted input. `condition_digest` is not
an identity key: if the same predicate becomes necessary after satisfaction or
supersession, the writer creates a new Wait Condition and Transition rather
than resurrecting the historical row.

Wall clock and Redis timer delivery are never reducer inputs. The controller
materializes every due time as one immutable Timer Fact. A single-writer
sweeper scans durable Wait Condition `not_before_ms`, Health Observation and
Budget Report `expires_at_ms`, Attempt claim/execution deadlines, and Recovery
Evidence `next_eligible_at_ms` values in stable
`(fired_for_ms, scope_kind, scope_id)` order.
For each due source lacking its unique fact, one transaction re-reads the exact
scope/deadline, requires `controller_now_ms >= fired_for_ms`, and inserts or
finds the scope/deadline-unique Timer Fact. For global Health expiry, that insertion
transaction also freezes the exact affected Runs in `timer_fact_runs`; replay
never discovers consumers from mutable current state. Only a
WAIT_CONDITION_NOT_BEFORE or HEALTH_OBSERVATION_EXPIRY Fact directly evaluates
the owning Run or frozen members using `T(TIMER_FACT, timer_fact_id)`: a current
matching Wait Condition is cleared into `RECOVERING`, and a matching health
expiry makes that observation ineligible for subsequent deterministic
selection. It never retracts an already planned or `OFFERED` Activity. The
reducer still rechecks all generation/Candidate/policy/wake bindings. If a
frozen member's formerly bound Wait Condition was superseded before its fanout
turn, the same trigger writes only the legal same-state audit Transition; it
  never changes current work.
A BUDGET_REPORT_EXPIRY Fact has empty membership and no Run Transition. Offer
planning at or after the deadline first inserts/reuses that Fact and treats the
Report as ineligible; startup and accepted-report scans leave affected
Activities PLANNED until a newer authenticated Report exists.
An execution-deadline Timer Fact deterministically creates the matching
Attempt Terminal Fact and fences/reduces through the separate canonical
`T(ATTEMPT_TERMINAL, attempt_terminal_fact_id)`. A claim deadline uses the same
terminal transaction for the due-time proof and capacity snapshot, but its
decision is Evidence-only: one transaction re-reads the due current `OFFERED`
Attempt and deadline, inserts the Timer Fact, freezes the highest applicable
unexpired capacity Health Observations and digest into the `CLAIM_DEADLINE`
Terminal Fact, resolves the logical provider account's exact current verified
Secret version, freezes the Controller Mode and Capability Registry
projections, records its closed capacity/replacement dispositions, expires the
Attempt, sets its Activity to `PLANNED`, enters `RECOVERING`, and appends one
zero-counter Recovery Evidence row. That transaction creates no replacement
Attempt, worker Outbox, or capacity Wait. The later
`T(RECOVERY_EVIDENCE, recovery_evidence_id)` transaction is the only place that
applies the selected tactic: after rechecking current gates it creates
generation `g + 1` and its Outbox for `COMPATIBLE_AVAILABLE` plus
`OFFER_ALLOWED`, or creates the typed capacity Wait for
`NO_COMPATIBLE_AVAILABLE` plus `OFFER_ALLOWED`. A blocked mode or unavailable
issuance key leaves the Activity `PLANNED` and the Evidence pending for the
deterministic offer/recovery continuation; it creates no generation or Wait.
For REVIEW/ADJUDICATE, if an unfilled slot has a peer `CLAIMED`, the terminal
transaction is the sole exception: it remains in the panel lifecycle state,
appends no Recovery Evidence, creates no Wait or offer, preserves that peer,
and atomically replaces the Run's four panel-staffing fields with the latest
coalesced pointer. If no peer remains claimed, the later Recovery Evidence
transition evaluates the complete panel and performs the all-or-none offer or
panel-Wait action. Neither branch changes recovery counters. A
`RECOVERY_ELIGIBILITY` Timer Fact makes the exact Recovery Evidence eligible;
applying its already selected tactic uses
`T(RECOVERY_EVIDENCE, recovery_evidence_id)`. The clock never becomes the
tactic input; applying a selected tactic uses only that canonical Recovery
Evidence trigger.

Redis delayed jobs MAY wake the sweeper early, but deleting them loses no
authority. Startup, ordinary periodic recovery, and Redis reconstruction all
run the same due-source scan until a stable pass. The unique
`(scope_kind, scope_id, fired_for_ms)` constraint makes duplicate wake
delivery, repeated scans, and a crash after fact commit idempotent; replay
returns the existing fact/Transition and never clears a later Wait Condition.

Every recovery classification appends Recovery Evidence at the next per-Run
sequence in the same transaction that advances the recovery projection. The
row stores the closed selected tactic, post-application counters, strategy
index, selected fallback, and next eligibility time;
`Run.current_recovery_evidence_id` points to that row. The tactic-to-Activity or
Wait mapping is the closed lifecycle table and cannot be supplied by repository
configuration or model output. An identical
`(run_id, source_kind, source_id)` returns the existing row
and cannot advance a counter twice. Health Observation sequences are allocated
atomically per scope. Expiry is applied only by a persisted timer fact, and a
Transition records the exact Health Observation IDs it consumed so replay does
not depend on current Redis leases or arrival time.

Capacity enters through authenticated `orcest.capacity-report/1` requests. One
transaction validates the pool-manager scope and strictly increasing report
revision, inserts the immutable Capacity Report and its canonically ordered
Health Observations and membership rows, applies every affected Run wake in
stable Run order, and stores the complete non-secret response. The same
pool-manager report/idempotency identities and body return that response
byte-for-byte; conflicting reuse is rejected. Redis capacity counters are only
projections of this ledger.
Expiry inserts a Timer Fact and makes the old Observation ineligible; it does
not fabricate an opposite Health Observation.

Budget authority enters only through authenticated
`POST /api/v1/budget-reports` requests from the registered accounting service:

```json
{
  "protocol": "orcest.budget-report/1",
  "budget_report_id": "lowercase-uuid",
  "project_id": "lowercase-uuid",
  "accounting_scope_id": "bounded-server-scope",
  "budget_policy_ref": "server-policy-ref",
  "budget_reset_window_ref": "server-policy-ref",
  "window_id": "bounded-window-id",
  "window_start_ms": 0,
  "reset_at_ms": 1,
  "source_sequence": 1,
  "source_revision": "bounded-accounting-revision",
  "limit_microunits": 1,
  "consumed_microunits": 0
}
```

Authentication supplies the principal and authorization-context digest; the
caller cannot put either in the body. In one writer transaction the controller
validates exact Project/scope/policy/window/freshness authority, derives
`AVAILABLE` or `EXHAUSTED`, inserts the Report, freezes all matching current
`WAITING/BUDGET` Run members in bytewise Run-ID order, initializes the cursor,
and stores this exact response (with `replayed` excluded from its digest):

```json
{
  "protocol": "orcest.budget-report-result/1",
  "budget_report_id": "lowercase-uuid",
  "project_id": "lowercase-uuid",
  "accounting_scope_id": "bounded-server-scope",
  "source_sequence": 1,
  "availability": "AVAILABLE",
  "reset_at_ms": 1,
  "expires_at_ms": 1,
  "affected_run_count": 0,
  "replayed": false
}
```

First acceptance and per-member fanout re-read Controller Mode. The first four
initialized non-maintenance modes permit them, subject to ordinary offer
gates; MAINTENANCE permits only exact read-only replay of an already stored
budget_report_id/body response. An unseen Report gets the same exact five-field
`CONTROLLER_MAINTENANCE` HTTP 503 used by the controller mode contract and
inserts no row. Fanout paused by maintenance retains its cursor and resumes
after authenticated mode exit.

Exact Report ID/body replay returns the stored HTTP 200 response with only
`replayed` projected true. Conflicting ID, sequence, or source-revision reuse
fails closed and writes no Report or membership. After acceptance, each member
uses a separate transaction that looks up the generation-independent
`T(BUDGET_REPORT,budget_report_id)`, revalidates its exact current Wait, writes
the Report's unexpired/current policy authority, writes the wake or required
stale-member same-state Transition, and advances the cursor. A crash after any
prefix resumes the next ordinal without recomputing membership. `EXHAUSTED`
has empty fanout; at a safe planning boundary it is the
only source of BUDGET Recovery Evidence. A budget-reset Timer merely starts an
accounting refresh. Acceptance and startup also rescan durable `PLANNED`
Activities for the same Project/scope; this is a derivable offer-reconciliation
scan, not another Report membership or lifecycle input. Missing/stale/mismatched
evidence creates no synthetic Report, Evidence, or Wait. Offer reconciliation
remains closed until the latest exact applicable Report is authenticated,
fresh, and `AVAILABLE`.

Authoritative early worker loss uses the authenticated Worker Loss Report
ledger, not a generic capacity report or Redis lease disappearance. One writer
transaction first validates the exact current claimed Attempt/session. When it
matches, the transaction inserts an `ACCEPTED` report, exact-session `LOST`
Health Observation, source-bound `WORKER_LOST` Attempt Terminal Fact, and its
canonical transition. An unknown exact Attempt triple returns
`404 ATTEMPT_UNKNOWN` and creates no Worker Loss Report.
When the triple exists but its current state/generation/session does not match,
the transaction inserts only a `STALE` report and stable response; both result references remain NULL and no
Health Observation, terminal fact, or Transition is created. Identical report
replay returns its stored response and cannot terminalize twice.

The management endpoint verifies authentication and server-side authorization
before submitting a command to the writer. Inside one `BEGIN IMMEDIATE`
transaction, the writer first looks up the global caller-supplied `command_id`.
The same canonical payload digest, authenticated principal, and authorization
context returns its stored HTTP 200 `orcest.management-result/1` response,
Transition, and optional Human Resolution, changing only the non-digested
transport `replayed` projection to true; different reuse is
`IDEMPOTENCY_CONFLICT`. For a new command it verifies protocol and
closed kind, compares
`expected_last_transition_sequence` with the Run, rejects secret-bearing or
unauthorized payloads, and for `RESOLVE_HUMAN_BOUNDARY` verifies the exact
current boundary, permitted resolution kind, and every copied binding.

Acceptance inserts the immutable Management Command, applies `CANCEL` or
inserts the Human Resolution, and appends the resulting Transition atomically;
the command stores those result identities and the exact closed response before
commit. A rejected stale,
unauthorized, malformed, or unsupported request creates no Management Command
and changes no lifecycle row, although the management plane records its
separate security audit event. Repository configuration, prompts, forge
comments, and worker output cannot create or authenticate this command.

An authenticated explicit `CANCEL` remains legal at every nonterminal state.
Work Item closure is also a cancellation source before publication authority
moves to a linked Change Request. Immediate terminal cancellation is legal
only when no Change Request has been observed, no durable
`CHANGE_REQUEST_CREATE/REQUEST_READY` or `AMBIGUOUS` checkpoint means that one
may exist, and either no Publication/create workflow exists at all or a current
`CHANGE_REQUEST_SEARCH/OBSERVED_ABSENT` checkpoint proves absence. Otherwise
the cancellation transaction stores immutable
`Run.cancellation_source_kind/source_id`, fences and supersedes semantic worker
work, retains `terminal_outcome IS NULL`, and plans exactly one current,
idempotently keyed controller `CLOSE_PUBLICATION` Activity and outbox for the
known cancellation phase. The source is the accepted Management Command or
exact Work Item Forge Observation.

If cancellation is already pending, a fresh authenticated `CANCEL` with the
current Run Transition fence still inserts its Management Command and one
same-state `T(MANAGEMENT_COMMAND, command_id)` audit Transition. It does not
replace the immutable first cancellation source, allocate another cleanup
Activity/outbox while the current phase and inputs remain valid, or repeat an
external close. Exact command-ID replay returns that Transition; a stale fence
or conflicting ID remains rejected.

The first cleanup Activity for a possible create binds only the immutable
Project/ref/marker/current-effect/create-request/search identity; its Change
Request head fields are null. Observation-backed absence permits its successful
Controller Operation Fact to terminally cancel and clean the branch. Discovery
never edits that Activity: the Forge Observation reduction supersedes it and
atomically plans a new `CLOSE_PUBLICATION` Activity/outbox whose immutable
semantic inputs and head fields bind the stable Change Request ID, marker,
deterministic ref, effect generation, exact head Observation, and normalized
head. A still-newer ordered head Observation supersedes that head-bound Activity
and atomically plans its replacement. Thus at most one cleanup Activity is
current even though reconciliation and close may require multiple immutable
Activities. The Run remains active and reserves the marker/ref and Work Item
throughout cleanup.

The reconciliation-only dispatcher searches and observes but never calls
`close_change_request_if_owned`. The head-bound dispatcher re-observes the
Change Request immediately before that call and proceeds only when stable ID,
Run marker, deterministic source ref, current head, effect generation, and the
Activity's exact head Observation prove ownership. A mismatch appends a Forge
Observation whose reduction creates the next immutable cleanup Activity; it
never closes an unverified object, mutates Activity inputs, or overwrites the
head. Once a Change Request exists, the Run becomes `CANCELLED` only when an
authenticated ordered Forge Observation proves that exact owned Change Request
is closed
unmerged. Merge winning the race produces `MERGED`, not cancellation. The
Activity/outbox identity and durable generic Run cancellation source make retry
and startup reconstruction convergent without an orphan side-effect window.
Cancellation never deletes the deterministic publication ref in v1. After
current reconciliation proves no open owned Change Request remains, that ref
is retained as a reserved terminal audit artifact bound to this Publication;
another Run cannot adopt, publish through, or garbage-collect it as an orphan.

Entering `NEEDS_HUMAN` inserts the bounded Human Boundary packet and sets
`Run.human_boundary_id` in the same Transition transaction. The pointer is
non-null exactly in `NEEDS_HUMAN`, belongs to the same Run, and is cleared
without editing the packet. Human Resolution acceptance conditionally verifies
that this boundary is still current; the resolution kind is allowed; all
Snapshot, Candidate, policy, Forge Observation, Publication, and effect
generation bindings match; and the authenticated principal has the required
server authority. The transaction inserts the one Human Resolution, appends
the resolution-triggered Transition, and clears or replaces the boundary
pointer. Secret bytes are rejected from both packet and resolution payloads.

One compound path has stricter atomicity. When the current Human Boundary is
`SPECIFICATION_CONFLICT` or `UNSATISFIABLE_REQUIREMENTS` and an authorized
ordered Forge Observation proves an amended specification, the observation and
its immutable pending Snapshot MAY have committed earlier. If a claimed
Attempt must first be fenced, they necessarily do commit earlier; persistence
MUST NOT pretend the forge read and later safe-boundary installation share a
transaction.

At the safe boundary, one writer transaction rechecks that exact Boundary,
Forge Observation, pending Snapshot, and absence of an acceptable old-
generation Attempt. It inserts the Human Resolution with
`source_kind = FORGE_OBSERVATION`, `resolution_kind = SPECIFICATION_AMENDED`,
and `authenticated_principal_id` equal to the forge adapter's verified stable
editor identity; its copied `forge_observation_id` remains the Boundary binding,
while the bounded resolution names the amendment Observation and records the
authorization-proof digest without credential material. The same transaction
clears `Run.human_boundary_id`, supersedes old unfinished work, increments the
generation, inserts the Snapshot Generation, installs the Snapshot, clears the
pending Snapshot and old Candidate, appends the single `SPEC_SUPERSEDE`
Transition keyed by `snapshot_id`, and plans `REPLAN` Activities/Attempts/
outbox. It does not append an intermediate `HUMAN_RESOLUTION` Transition. A
replay first finds that unique `SPEC_SUPERSEDE` Transition and returns its
Human Resolution and planned work even though the boundary is no longer
current.

SQLite cannot express every temporal invariant as a `CHECK`. The single writer
MUST enforce the remainder with a conditional update in the same transaction:

```sql
UPDATE attempts
   SET state = :terminal_attempt_state,
       terminal_reason = :terminal_reason
 WHERE activity_id = :activity_id
   AND attempt_id = :attempt_id
   AND generation = :generation
   AND state = 'CLAIMED'
   AND claimed_worker_session_id = :worker_session_id
   AND attempt_capability_signing_key_id = :attempt_capability_signing_key_id
   AND attempt_capability_signature_algorithm = :attempt_capability_signature_algorithm
   AND attempt_capability_digest = :attempt_capability_digest
   AND execution_profile_id IS :execution_profile_id
   AND worker_profile = :worker_profile
   AND provider IS :provider
   AND model IS :model
   AND provider_account_ref IS :provider_account_ref
   AND provider_family IS :provider_family
   AND model_family IS :model_family
   AND classification_revision IS :classification_revision
   AND launch_attestation_id IS :launch_attestation_id
   AND execution_deadline_ms > :controller_now_ms
   AND EXISTS (
       SELECT 1
         FROM activities
        WHERE activity_id = :activity_id
          AND current_attempt_generation = :generation
          AND state = 'ACTIVE'
   );
```

Zero affected rows means stale, duplicate, or illegal input. The controller
MUST NOT insert an Attempt Result, accepted receipt, Candidate reference,
transition, or next Activity when that guard fails. After this guard succeeds,
the same transaction inserts the unique Attempt Result with required
`result_digest`, inserts the creating `ACCEPTED` Result Request with
`accepted_result_created = true`, and updates the Activity/Run through the
reducer.

For every model-backed kind, the guard additionally requires non-null
`launch_attestation_id`, non-null `launch_capability_consumed_at_ms`, and the
exact LaunchAttestation row for this Attempt/session; the request Result and
any Review/Adjudication Receipt repeat that ID and their canonical digests cover
it. Deterministic VERIFY requires all launch values NULL. A Candidate-producing
first Result also joins its exact `PROMOTED` CandidateUpload and requires
`:controller_now_ms < CandidateUpload.expires_at_ms`; accepted-Result replay is
resolved before this first-acceptance-only upload predicate.

The global ResultRequest primary-key lookup precedes every disposition and
first-acceptance guard. Exact reuse of the same key and complete immutable
Attempt/session/capability-signer/body bindings returns its stored response;
any mismatch is `IDEMPOTENCY_CONFLICT`. In `MAINTENANCE`, this exact-key lookup
is the entire allowed path: an unused key returns the closed 503 and inserts
nothing, even when its body digest matches an accepted Result under another
key. Outside `MAINTENANCE`, if an Attempt Result already exists and
an authenticated unused-key retry has the same Attempt/session/signer and
`result_body_digest = AttemptResult.result_digest`, it remains a semantic replay
after the execution deadline only while
`controller_now_ms < capability_auth_expires_at_ms`. The writer inserts one
`ACCEPTED` Result Request with `accepted_result_created = false`, points it to
that Result, and applies no reducer input. A different digest for an Attempt
that already has a Result is `RESULT_ALREADY_ACCEPTED` and creates no Request.

For an unseen, schema-valid Candidate-producing request before the execution
deadline, the upload-expiry predicate is serialized before Result acceptance.
If controller time is at or after the named upload's durable expiry, the writer
CASes the unused upload to `EXPIRED` (including promoted-reference clearing
under the storage lock), inserts a `UPLOAD_EXPIRED` Result Request, and stores
the exact HTTP 410 `orcest.candidate-upload-expired/1` body containing only
protocol, upload ID, `state = EXPIRED`, code `UPLOAD_EXPIRED`, and the immutable
expiry. It inserts no Result, Candidate, Receipt, Terminal Fact, Recovery
Evidence, or Transition.

If no accepted result exists, `controller_now_ms` is a controller-derived time
sampled for the transaction. A first request received at or after
`execution_deadline_ms` but strictly before
`capability_auth_expires_at_ms` cannot win merely because the deadline sweeper
has not yet terminalized the Attempt. In that same writer transaction, the controller
inserts the Result Request with `EXPIRED_CURRENT` or `ALREADY_TERMINAL` and its
own source-unique Attempt Terminal Fact with `source_kind = RESULT_REQUEST`.
`EXPIRED_CURRENT` uses kind `EXECUTION_DEADLINE` with the exact stored deadline,
controller time, and complete canonical Result-body digest. Reducing
`T(ATTEMPT_TERMINAL, attempt_terminal_fact_id)` changes the still-current
`CLAIMED` Attempt to `EXPIRED`, returns the owning Activity to `PLANNED`,
records timeout Recovery Evidence, and applies deterministic recovery only for
disposition `EXPIRED_CURRENT`; that branch
  stores the bounded canonical `410 EXECUTION_DEADLINE_EXCEEDED` response. If another
non-Result input already made the Attempt terminal and no Attempt Result was
accepted, disposition `ALREADY_TERMINAL` stores a request-sourced
`RESULT_AFTER_TERMINAL` Fact and canonical `409 ATTEMPT_STALE` response as
audit-only evidence. Its source-unique ATTEMPT_TERMINAL reduction appends one
same-state Transition and changes no Recovery Evidence, counter, or work.
Accepted-Result replay or
conflict lookup has precedence and never creates a late disposition.
Both branches insert neither an Attempt Result nor an ACCEPTED Result Request.
Replaying the same authenticated key, bindings, capability, and body returns
the stored status/body byte-for-byte; different reuse of the key is
`IDEMPOTENCY_CONFLICT`, never a new acceptance. The ledger, Fact, conditional
expiry/recovery, and response commit in one `BEGIN IMMEDIATE` transaction, so
concurrent retries cannot split the late request from its proof or race a late
result past expiry.

At or after `capability_auth_expires_at_ms`, the Attempt capability cannot
authenticate even the Result endpoint. The request returns the closed auth/
capability denial without inserting a Result Request, Terminal Fact, Result,
Receipt, Candidate, Recovery Evidence, or Transition;
the ordinary durable deadline Timer Fact sweeper terminalizes any still-current
Attempt. Before authentication expiry but at/after execution deadline, only the
two Result operations above—Result Request timeout rejection and exact
accepted-Result replay—are permitted. Claim, launch, liveness, source/Candidate
download, upload, credential rotation, and every other Attempt endpoint deny;
the grace is authentication for deterministic Result reconciliation, never
execution authority.
This statement concerns Attempt-capability authorization. The separate consumed
launch-capability lookup may still return the already accepted Attestation's
`EXPIRED`/null-provider projection under the exact immutable lookup guard above;
it grants no Attempt endpoint or workflow-mutation authority.

Deadline sweeps bind `source_id` to the exact persisted Timer Fact; late
submission uses its authenticated Result Request ID; worker loss uses the
exact Health Observation ID. A duplicate full source identity returns the
existing Fact and its Transition, if any, without inserting Recovery Evidence,
incrementing counters, or applying the reducer twice. Once another terminal
cause has fenced the Attempt, a later distinct terminal Fact or late request is
retained as bounded audit evidence and appends exactly one source-unique
same-state `T(ATTEMPT_TERMINAL, attempt_terminal_fact_id)` Transition. That
Transition mutates no Attempt/Activity/Run projection, Recovery Evidence,
counter, Wait, or work; replay returns it rather than appending another.

An accepted worker result maps `SUCCEEDED` to terminal Attempt state
`SUCCEEDED`, either failure outcome to `FAILED`, and `ABSTAINED` to
`ABSTAINED`. `EXPIRED` and `SUPERSEDED` are controller-derived
terminal states and are not valid worker result outcomes. `ABSTAINED` leaves
the Activity's semantic slot unfilled and returns that Activity to `PLANNED`.
Its acceptance atomically terminalizes the Attempt, updates Activity before
committing the Run's `RECOVERING` projection, and appends Recovery Evidence
sourced by the exact ATTEMPT_RESULT; it MUST NOT directly offer a replacement
or create a Wait. Only
the later source-unique `T(RECOVERY_EVIDENCE, recovery_evidence_id)` may apply
`RETRY_EXECUTION`, `REPLACE_CAPACITY`, `WAIT_EVIDENCE`, or `WAIT_CAPACITY`,
without treating the abstention as success.
Receipt presence is validated by Activity kind and outcome, not by requiring a
successful Attempt: the closed `VERIFY` exception accepts a schema-valid
`ERROR` Verification Receipt on its `FAILED_RETRYABLE` Result with failure
class `VERIFICATION_ERROR`. No other failed Result carries a Receipt.
Any missing required Receipt, malformed Result/Receipt, invalid assignment or
subject membership, or forbidden kind/outcome/output combination is a 4xx
schema rejection before the terminal conditional update. It inserts no
AttemptResult, Receipt, ResultRequest, Attempt Terminal Fact,
Recovery Evidence, or Transition and leaves the exact current Attempt CLAIMED.
The worker may correct the request while all claim/deadline fences remain
valid; only a schema-valid `FAILED_RETRYABLE/VERIFICATION_ERROR` Result carrying
Verification `ERROR` enters recovery through ATTEMPT_RESULT.

The writer MUST also preserve the state correspondence that cannot be safely
expressed as independent row checks: a worker Activity in `READY` has exactly
one current `OFFERED` Attempt and non-superseded outbox intent; an `ACTIVE`
worker Activity has exactly one current `CLAIMED` Attempt; a recoverable
terminal Attempt is paired with Activity `PLANNED` before Run `RECOVERING`; and
a terminal Activity has no nonterminal Attempt. Startup audit treats a
violation as integrity failure rather than guessing which row wins.

Forge Observations and Transitions require an ordered insertion transaction.
The writer allocates and advances an observation counter for the exact
`(project_id, target_kind, target_id)` or the owning Run's transition counter
in the same transaction that inserts the immutable row. This permits ordered
Work Item observations before a Run exists and ordered Publication
observations after handoff. An adapter event ID, when present, is a durable
delivery-deduplication key within its Project. Replaying the same event ID and
identical normalized observation returns its existing row; the same event ID
with different normalized content is an integrity conflict. For polling or an
adapter without an event ID, the writer compares the normalized observation
identity only with the latest row for that exact target. That identity includes
the adapter/schema version, external revision, normalized payload digest, and
applicable Run, Publication, and publication-effect binding; it excludes fetch
time and delivery metadata. If it is identical, the writer returns that latest
row without allocating a sequence. If any different observation intervened,
an identical later payload receives a new sequence. This preserves an external
`A -> B -> A` change while suppressing repeated polls of unchanged `A`.
Receipt arrival order does not become semantic order: consensus reads the
canonical receipt set and sorts by the keys defined by the review protocol.

## Reducer transaction and transactional outbox

A reducer command that makes worker work schedulable MUST commit these changes
in one transaction, but only after the current Controller Mode permits offer
planning and the Capability Key Registry has an ACTIVE selected issuance key.
If either gate is closed, it may commit the Activity as `PLANNED` but MUST NOT
perform steps 4-5; the offer reconciler performs those two steps atomically
after both gates open. A panel without a complete legal staffing set follows
the stricter Wait/STAFF_PANEL all-or-none transaction above. When eligible:

1. Persist the triggering receipt or ordered external observation.
2. Insert the immutable transition and update the Run state.
3. Insert the new Activity when this is a new semantic unit of work, or retain
   the existing Activity when this is only an execution retry. For a new
   `REVIEW` or `ADJUDICATE` Activity, also insert its one immutable Activity
   Review Assignment, complete nonempty ordered subject membership, and any
   ordered adjudication-Finding membership.
4. Insert the current generation's `OFFERED` Attempt with its persistent claim
   deadline. For a replacement, first make the prior generation terminal and
   then insert generation `g + 1`.
5. Insert the unique outbox row for that exact
   `(activity_id, attempt_generation, destination)`.

The transaction first resolves every trigger by
`(run_id, trigger_kind, trigger_id)`, independent of the Run's current
specification generation. `ADMIT` and `FORGE_OBSERVATION` additionally resolve
the cross-kind `(run_id, trigger_id)` identity. If its Transition exists, it
returns that Transition, including its original evaluated generation, and its
ordered `planned_activity_ids`. Otherwise it allocates each Activity's consecutive
`activity_ordinal` from `Run.next_activity_ordinal`, records the new
`created_transition_sequence`, derives the exact Domain idempotency key from
the pinned reducer version, Run/generation/Transition, kind/execution class,
installed policy hash, semantic-input digest, Candidate/causal-Forge/separate-
head-Observation/head bindings, role, repair/recovery cycles,
strategy/tactic/evidence, and rescue epoch, and inserts
the Activities and Transition atomically. `(run_id, idempotency_key)` is
unique, so replay of the same Transition returns the same Activity and cannot
allocate another ordinal or outbox.

A recovery-planned Activity binds the exact Recovery Evidence ID and selected
tactic as fields in that key. A later repair or recovery decision may plan an
otherwise similar Activity because its creating Transition and applicable
cycle/evidence/tactic/epoch inputs differ. Replaying the same decision derives
the same key. Neither kind alone nor a partial semantic digest is an
idempotency key, so legitimate repeated repair cycles are not blocked.

The first schedulable Attempt generation is `1`. Activity
`current_attempt_generation`, when materialized as a query projection, MUST
match its unique nonterminal Attempt. Controller-class Activities have a
controller-destination outbox row with `attempt_generation IS NULL` and do not
manufacture a worker Attempt. No Redis or controller-side external operation
occurs inside the planning transaction.

The state transitions that create or consume worker offers are a closed
transaction matrix:

| Transaction | Required pre-state | Atomic durable result |
| --- | --- | --- |
| initial or recovery planning | New or current worker Activity is `PLANNED`; no current nonterminal Attempt | If current Controller Mode, selected issuance key, latest exact applicable authenticated `AVAILABLE` Budget Report, and all capacity/staffing gates permit an offer, insert generation 1 (or the selected next generation) as `OFFERED`, its worker Outbox, and set Activity `READY`. If any offer gate is closed, retain `PLANNED` with no Attempt or worker Outbox. |
| offer reconciliation | Same Activity remains `PLANNED`; no current nonterminal Attempt | Recheck all immutable Activity/Run/Candidate/policy bindings and every mode/key/budget/capacity/staffing gate under the writer lock; on success insert the next `OFFERED` Attempt and Outbox and atomically change Activity `PLANNED -> READY`. A current `EXHAUSTED` Report may source only the typed budget-Recovery-Evidence path; missing/stale/mismatched evidence leaves the exact `PLANNED` projection with no invented Fact/Wait. No timer or mutable counter authorizes an offer. |
| worker claim | Activity `READY`; exact current Attempt is `OFFERED` and its Outbox is current | Atomically change Attempt `OFFERED -> CLAIMED`, insert the Attempt Claim and reciprocal fields, and change Activity `READY -> ACTIVE`; a stale or conflicting claim changes nothing. |
| accepted successful Result | Activity `ACTIVE`; exact Attempt is `CLAIMED` | Persist the Result and terminal Attempt state, then set Activity `SUCCEEDED` in the same transaction; no later generation is legal. |
| accepted recoverable terminal Result/Fact | Activity `ACTIVE`; exact Attempt is `CLAIMED` | Terminalize that Attempt and atomically return its Activity to `PLANNED` before committing the Run's `RECOVERING` projection and typed Recovery Evidence. No replacement Attempt, Outbox, or recovery tactic is created by this transaction; the later Evidence transition may offer one. |
| same-Activity retry generation | Activity `PLANNED`; prior Attempt is terminal and the selected Recovery Evidence permits execution retry | In one writer transaction retain the immutable Activity, insert exactly generation `g + 1` as `OFFERED` with its Outbox, and change Activity `PLANNED -> READY`. The prior generation is never reused and no terminal Activity can enter this branch. |
| terminalization without recovery | Any current Activity and Attempt | Persist the terminal Attempt/Fact and set Activity `FAILED`, `CANCELLED`, or `SUPERSEDED`; the terminal guard leaves no nonterminal Attempt or dispatchable Outbox and prohibits retry. |

The `PLANNED` state is therefore the durable handoff between lifecycle
recovery and offer reconciliation; it is not an unstated or inferred
recoverable state. A recovery reduction may commit `Run = RECOVERING` while
the Activity is already `PLANNED`, but it must never expose an `ACTIVE`
Activity with a terminal Attempt or an `ACTIVE` Activity whose next generation
has not been offered.

The outbox envelope and every claim response reconstruct `review_slot` from
the durable Activity Review Assignment, ordered subject rows, and ordered
Finding rows, never from a Redis payload or worker echo. A higher Attempt
generation for the same Activity reuses the identical assignment, subject set,
and context. Startup validation and dispatch fail closed if a
`REVIEW`/`ADJUDICATE` Activity lacks its assignment or nonempty subject set,
has noncanonical membership/digests, or if any other Activity has one.

Publication effects use a second, durable fence. Creating a Publication sets
`effect_generation = 1` and inserts immutable Publication Effect generation 1
in the same transaction. Planning any different desired commit or external
mutation inserts generation `g + 1` and advances the Publication's current
generation by exactly one in the same transaction that records the current
`PUBLISH` Activity, its controller outbox intent, expected remote revision,
reducer Transition, and updated Publication state. Retrying the same desired
mutation retains the same immutable effect row, generation,
outbox/idempotency identity, and compare-and-swap expectation. The publication
outbox payload and every adapter request carry
`(publication_id, effect_generation)`.

The deterministic INTERNAL continuation from the Transition that entered
`APPROVED` selects the latest applicable trusted `BASE_HEAD` only from the
Run's consumed projection. The candidates are its ADMIT
`anchored_base_observation_id` and exact BASE_HEAD Observations already consumed
by that Run's unique FORGE_OBSERVATION Transitions. It chooses the candidate
whose consuming Run Transition has the greatest `transition_sequence`, then
binds that exact observation into any rebase, pending base-only Snapshot, or
new Effect decision. Per-target observation sequence, adapter arrival order,
forge timestamp, and an accepted-but-unreduced row cannot win this selector,
and the continuation never reuses the observation ID as its trigger.

Initial branch creation and Change Request creation are ordered suboperations
of the same `INITIAL` effect generation, not reasons to increment the fence.
Each adapter call derives a stable idempotency identity from the immutable
effect identity plus its code-owned suboperation kind. Read-back observations
advance the Publication state within that generation. An authoritative change
to desired commit, expected remote commit, base binding, or mutation mode
creates the next immutable effect; an ambiguous response or retry does not.
The one controller outbox intent remains reconcilable until every required
suboperation is observed; an effect with no checkpoint starts at its first
code-owned read.

For post-link work, `change_request_head_observation_id` is a separate durable
fence from the causal `forge_observation_id`. It references only an exact
`CHANGE_REQUEST_DISCOVERED`, `CHANGE_REQUEST_HEAD`, or
`CHANGE_REQUEST_FEEDBACK`/`CHANGE_REQUEST_MARKER` Observation and supplies
`observed_change_request_head`; both fields are null together otherwise. A
post-link `REBASE` may therefore bind causal `BASE_HEAD` in
`forge_observation_id` while separately retaining the Change Request head that
an eventual update may compare-and-swap. `PR_REMEDIATE` and Change Request
`IMPORT` normally use the same Observation for both roles. A head-bound
`CLOSE_PUBLICATION` likewise binds its discovery/head Observation in both
roles; `REPAIR_RUN_MARKER` binds its exact Marker Observation/head in both;
the pre-discovery cancellation predecessor has both head fields null.
The separate ID and head are part of the Activity semantic digest and idempotency key. The
`expected_remote_commit` of the resulting `UPDATE` Publication Effect is that
exact retained head, or a later ordered head explicitly revalidated before the
Effect transaction, and is included in `operation_digest`. A different head
requires a new Activity/Candidate as applicable and a new Effect generation; a
base-only Observation can never supply or weaken this fence.

This replacement rule is state-independent once the Publication has reached
`CHANGE_REQUEST_OBSERVED`. For any nonterminal, non-cancelling Run, a newer
authenticated ordered Change Request-head Observation atomically advances the
Publication's observed head, supersedes every Activity, claimed/offered
Attempt, gate, Consensus Decision, Publication Effect, Wait, or Human Boundary
bound to the old head, and plans one exact-observation-bound controller
`IMPORT` into `PR_REMEDIATING`. It applies while reviewing, aggregating,
approved, publishing, remediating, recovering, waiting, or paused, not only
while `PR_MONITORING`. Merge/close observations and the cancellation cleanup
rules take precedence. Replay of the Observation finds its single
FORGE_OBSERVATION Transition and cannot allocate a second import path.

Checkpoint sequence allocation is an atomic per-effect increment by the single
writer. Before any mutating adapter call, the writer appends the applicable
`REQUEST_READY` checkpoint with its stable UUID request key and commits it with
the still-current outbox work. The executor reuses that key for every retry.
After a read or write response, it first persists the normalized Forge
Observation when one exists, then appends the observation-backed checkpoint in
the same transaction that advances Publication state. The writer enforces the
closed mode/order/status/nullability matrix above. An `INITIAL` effect follows
`BASE_READ_PRE -> REF_READ -> optional REF_CREATE/REF_UPDATE ->
COMPLETE_MARKER_SEARCH`, then the closed branch: ZERO LIVE/no terminal performs
the ownership-precedence reducer first. Any positive MERGED terminal selects
the lowest stable ID, appends COMPLETE, terminalizes MERGED, and reserves every
LIVE member for post-terminal cleanup. Otherwise INCOMPATIBLE routes to the
ownership-conflict path and INCOMPLETE to autonomous exact reread/backoff, with
no association. Only when all members are POSITIVE and none is MERGED does
ZERO LIVE/no terminal perform `CHANGE_REQUEST_SEARCH`, optional stable
`CHANGE_REQUEST_CREATE`, and a fresh `COMPLETE_MARKER_SEARCH`; ONE LIVE performs
a fresh exact-object read then `BASE_READ_POST -> COMPLETE`; MULTIPLE LIVE
performs one proof-bound cleanup and a fresh `COMPLETE_MARKER_SEARCH`; ZERO LIVE
with positive CLOSED terminal authority appends COMPLETE and terminalizes
without create. That complete-search return edge is the sole phase loop. An
`UPDATE` follows `REF_READ -> REF_UPDATE -> COMPLETE`.
`AMBIGUOUS` selects read reconciliation with the same request key;
`BASE_MISMATCH` aborts stale initial-base authority; and `CAS_MISMATCH` never
authorizes overwrite. A skipped optional mutation is proven by its read/search
observation, not a fabricated checkpoint.
Replay of the same request or observation source returns its prior checkpoint,
while equal content from a distinct source receives the next sequence.

After a crash the executor reads the highest committed checkpoint for the
effect and derives the next code-owned suboperation from it, the immutable
effect, Publication state, and Forge Observations—never from an in-memory
cursor. A current `PUBLISH` Activity remains `ACTIVE` across controller restart
or an ambiguous response. Only a `COMPLETE/COMPLETED` checkpoint may mark it
`SUCCEEDED`; that checkpoint has exactly two legal evidence shapes: the final
linked Change Request/head Observation after fresh ONE-LIVE proof and exact-
object read, or the exact current complete-search Observation/member used by a
positive terminal selection. Discovery or creation alone remains pre-link and
must return through fresh COMPLETE_MARKER_SEARCH; only the ONE branch's exact-
object read sets `CHANGE_REQUEST_OBSERVED`, and only its matching post-link base
read plus `COMPLETE` makes Publication `ACTIVE`.

A positively owned MERGED terminal member has precedence at every LIVE
cardinality. Its terminalizing FORGE_OBSERVATION writer transaction selects the
bytewise-lowest positive merged ID, appends COMPLETE, freezes the selected
association/proof, fences PUBLISH and all semantic work, commits Run `MERGED`,
and atomically creates the reciprocal Terminal Duplicate Cleanup Reservation,
all LIVE Member rows, and the first Action/outbox when one is needed. The
Reservation is controller post-terminal work; it never changes the selected
merge or reopens the Run.

Startup and every terminal audit continuation scan ACTIVE Reservations in
stable ID order and resume exactly `next_member_ordinal`. CLOSE uses only the
exact-owned/reliance-free close primitive; DETACH_MARKER removes only this Run
marker under the frozen head/body/marker CAS while preserving all other body
and external work; RECORD_ONLY mutates nothing and stores its bounded reason.
Only one Action generation for the current ordinal is nonterminal. An
ambiguous response leaves it ACTIVE with no Transition. An authenticated
success or mismatch Observation reduces exactly once through
`T(FORGE_OBSERVATION, observation_id)`: success terminalizes the Action and
schedules one INTERNAL continuation; mismatch supersedes it and begins the
exact fresh read/search whose later Forge Transition creates a higher Action
generation or terminal RECORD_ONLY. RECORD_ONLY and selection of each next
ordinal use exactly one `T(INTERNAL, prior_transition_sequence)`. Cursor advance
and next Action/outbox—or Reservation completion—commit atomically. Every such
Transition is same-state `MERGED`; none changes the selected ID/head/merge.

Before applying an adapter response, the writer conditionally verifies that
the Publication, current `PUBLISH` Activity, and effect generation still own
the mutation. A resulting Forge Observation records the generation that
produced it. A response from an older generation MAY be inserted as ordered
audit evidence, but it cannot update the Publication, complete the Activity,
or transition the Run. This check is required even when the adapter reports
success and even when its request idempotency key matches an older effect.
Historical Outbox, Forge Observation, Human Boundary, and Human Resolution
rows reference the immutable Publication Effect ledger, not the Publication's
mutable current-generation projection; advancing the projection therefore
cannot orphan a historical foreign key.

The controller outbox dispatcher begins a controller-class Activity by
conditionally checking its immutable input and current Run bindings, moving it
from `READY` to `ACTIVE`, and recording the dispatch attempt before commit. It
performs filesystem or forge work only after that commit. A controller restart
never infers success or blindly repeats an `ACTIVE` operation. A `PUBLISH`
Activity follows its checkpoint-resumption rule above and remains `ACTIVE`;
another controller Activity terminalizes only through an immutable Controller
Operation Fact or the more specific reconciliation/publication/forge fact
required by its protocol. Successful `PUBLISH` creates no Controller Operation
Fact and completes through its final checkpoint plus Forge Observation.
Successful `RECONCILE` likewise terminalizes only through the Reconciliation
Fact transaction below; a failed `PUBLISH` or `RECONCILE` uses a failed
Controller Operation Fact. A linked `CLOSE_PUBLICATION` waits for the exact
close/merge Forge Observation; only stable-request absence before linkage may
produce a successful Controller Operation Fact, and only through the exact
matching `CHANGE_REQUEST_ABSENT` Observation. A
`CLOSE_REDUNDANT_PUBLICATION` may create only a failed Controller Operation
Fact; success waits for the exact authenticated `CHANGE_REQUEST_CLOSED`
Observation bound to its Activity and operation digest. That success does not
close the Publication or Run and creates no new Publication Effect. The writer
requires `REPAIR_RUN_MARKER` success to wait for the exact authenticated
controller-bound `CHANGE_REQUEST_MARKER` Observation proving the one desired
marker; it too uses a failed Fact only for definitive evidence-less failure,
stays ACTIVE on ambiguity, and creates no new Publication Effect. The writer
requires the exact
Activity still be current, its
pre-I/O `operation_digest` match, and every Candidate, Reconciliation Fact, or
canonically ordered Forge Observation output already be durable. Inserting a
Controller Operation Fact and terminalizing its Activity commit atomically;
`T(CONTROLLER_OPERATION, controller_operation_fact_id)` is the only reducer
trigger for that Fact. A failed Fact stores typed failure evidence and can plan
the code-owned `RECONCILE` path; it cannot synthesize an Activity ID/digest
trigger. A successful `IMPORT` or absence-proving `CLOSE_PUBLICATION` Fact
binds its required Candidate or absence observations. An active, ambiguous, or
successful `PUBLISH` creates no Controller Operation Fact and follows its
checkpoint protocol. A controller restart or ambiguous adapter response for
`CLOSE_REDUNDANT_PUBLICATION` or `REPAIR_RUN_MARKER` likewise leaves that
Activity `ACTIVE` and
reconciles the same stable operation; neither condition creates a failed Fact.
`activity_id` uniqueness
makes identical response replay a no-op; conflicting terminal content is an
integrity error.

A `RECONCILE` Activity produces exactly one immutable Reconciliation Fact.
External reads first persist every normalized Forge Observation they rely on;
the observations and their per-target sequences may therefore commit before
the Fact. The Fact transaction conditionally requires the exact `RECONCILE`
Activity to remain `ACTIVE`, its Run/specification/Candidate and optional
Publication Effect bindings to remain current, every canonically ordered
observation membership to exist, and the recomputed fact digest to match. It
inserts the Fact and non-empty membership, marks `RECONCILE` successful,
appends `T(RECONCILIATION_FACT, reconciliation_fact_id)`, applies the resulting
Run/Publication projection, and plans the reducer-selected next work in one
writer transaction.

The closed outcomes have no generic fall-through:

- `EFFECT_PRESENT` attaches the exact observed effect/checkpoint state and
  continues from the publication state proven by those observations.
- `EFFECT_ABSENT` is accepted only while the original preconditions and
  expected revision still match, then plans the permitted controller operation
  with the same stable side-effect identity.
- `PRELINK_REF_IMPORTABLE` binds the exact foreign ref commit plus safe-fetch
  and absence-of-conflicting-owner proof, proves the installed Snapshot's exact
  pinned-base relationship is `EXACT_PINNED_BASE` or
  `DESCENDANT_OF_PINNED_BASE`, and stores ordinary Candidate-admission proof.
  Only then does it plan controller `IMPORT`; it never adopts, publishes, or
  approves the foreign commit directly.
- `PRELINK_REF_RECONSTRUCT_REQUIRED` records a safely fetched head whose pinned-
  base relationship or Candidate admission failed without positive
  incompatible ownership evidence. It stores the typed validation-failure
  digest and produces Recovery Evidence selecting
  `RECONSTRUCT_FOREIGN_HEAD`; it never retries `IMPORT` for the same evidence.
- `REDUNDANT_PUBLICATIONS_PROVEN` freezes the complete ordered LIVE subset of
  one exact same-marker search, copies its causal
  CHANGE_REQUEST_SEARCH_RESULT revision/full-set digest, retains the bytewise-
  lowest LIVE stable Change Request ID, and plans at most one
  `CLOSE_REDUNDANT_PUBLICATION` for the first CLOSE member. Its immutable
  `orcest.redundant-publication-cleanup/1` Activity/outbox commits before the
  adapter call. Immediately before
  `close_change_request_if_exact_unreviewed_duplicate`, the controller rechecks
  the current Publication/effect, complete set, retained-lowest rule, both
  exact heads, marker/ref equivalence, and absence of non-Orcest reliance. A
  typed mismatch first persists its exact current Forge Observation evidence;
  that Observation's FORGE_OBSERVATION Transition then supersedes the cleanup
  and either plans fresh RECONCILE from a changed complete SEARCH_RESULT or
  schedules a fresh complete search from individual evidence. A definitive
  evidence-less failure uses a failed Controller Operation Fact. An ambiguous
  response or restart leaves the Activity ACTIVE and retains the same operation
  identity for read reconciliation. Only
  an exact authenticated close Observation atomically completes that Activity,
  leaves the canonical Publication/Run nonterminal, and schedules a fresh
  complete marker search for any next duplicate. It neither reuses the old Fact
  nor plans RECONCILE from that individual close; only a later changed typed
  SEARCH_RESULT may do so. It never increments Publication.effect_generation.
  Before first linkage, the current PUBLISH remains suspended at
  COMPLETE_MARKER_SEARCH/MULTIPLE while one subordinate duplicate RECONCILE or
  CLOSE_REDUNDANT_PUBLICATION is current. The Fact and cleanup never set
  Publication.change_request_external_id; its retained ID is proof/cleanup
  selection only. Each close schedules a fresh COMPLETE_MARKER_SEARCH, and
  only a resulting ONE proof plus a fresh exact-object read may link.
- A pre-link complete search with ZERO LIVE members and at least one owned
  TERMINAL member bypasses duplicate reconciliation and create. Its sole
  writer transaction selects the bytewise-lowest MERGED stable ID if present,
  otherwise the bytewise-lowest CLOSED ID; copies the exact terminal member/
  search proof into Publication, associates that object, appends COMPLETE,
  fences all unfinished work, and terminalizes Publication/Run. It never calls
  the create adapter or derives authority from event arrival order.
- `NO_ACTIONABLE_DUPLICATE` records a complete same-marker search whose
  canonical ordered result set yields no cleanup. It atomically copies the
  Fact, complete-search revision, and duplicate-set digest to the Publication,
  changes no retained association, and plans no Activity or Effect. The same
  revision/digest pair is exhausted; only a later changed pair may plan a new
  duplicate RECONCILE.
- `OWNERSHIP_CONFLICT` requires positive incompatible ownership evidence after
  autonomous store, verified-backup, marker, ref, and Change Request
  reconciliation. Duplicate same-valid-marker objects are repaired
  autonomously only with exact equivalence/unreviewed proof by retaining the
  lowest stable external ID; uncertainty stays in typed reconciliation/wait.
  The same transaction creates the bound
  `PUBLICATION_OWNERSHIP_CONFLICT` Human Boundary. Absence, timeout, or
  temporary forge failure cannot produce this outcome.

`activity_id` uniqueness and the canonical `fact_digest` make completion
idempotent. Replay with the same digest returns the committed Fact, Transition,
and planned outputs; reuse for different content is an integrity conflict. A
stale reconciliation may retain newly ordered Forge Observations as audit
facts, but it cannot insert a Reconciliation Fact, complete the Activity, or
advance the Run.

The ordinary worker outbox dispatcher selects `PENDING`, due rows whose
referenced Attempt remains `OFFERED` at the same generation. A Redis rebuild
also selects a prior-epoch `DELIVERED` row when its Attempt is still current
and `OFFERED`. The dispatcher publishes a non-secret notification containing
at least:

```text
protocol_version
redis_epoch
outbox_id
activity_id
attempt_id
generation
```

The protocol-versioned stream name and provider/role routing are defined by the
worker protocol. Prompts, provider credentials, forge credentials, upload
capabilities, and secret values MUST NOT be placed in the notification.

After a successful Redis append, the dispatcher records `DELIVERED`, the
returned entry ID, delivery count, and current Redis epoch. A timeout may mean
the append succeeded, so retrying MAY create an identical notification.
Consumers and the claim endpoint MUST deduplicate by durable Attempt identity
and generation, never by Redis entry ID. Marking an outbox row delivered does
not complete an Activity.

## Claim, fencing, and persistent deadlines

Redis delivery grants no authority. A worker MUST claim an exact `attempt_id`
and matching `(activity_id, generation)` through the authenticated controller
API. Its `OFFERED` Attempt already exists. The single writer handles the claim
in one `BEGIN IMMEDIATE` transaction:

1. Verify that the Activity is `READY`, the exact Attempt is `OFFERED`, its
   generation is current, and `controller_now_ms < claim_deadline_ms`. Verify
   the authenticated worker/session capability against the Attempt's exact
   execution profile, four-value assignment, provider/model families, and
   classification revision. For review work, also revalidate the complete
   durable Activity Review Assignment and both applicable membership digests.
2. Conditionally update that Attempt to `CLAIMED` with claimant identity,
   the unguessable authenticated worker-session ID, claim time, Attempt
   capability claims digest/JTI, an absolute execution deadline derived from
   pinned policy, and `capability_auth_expires_at_ms` equal to that deadline plus
   the literal v1 `86400000` milliseconds. For model-backed work also allocate
   the globally unique launch nonce and store the domain-separated digest of
   normalized signed launch claims/JTI, never bearer bytes.
3. Insert the immutable AttemptClaim carrying the caller UUID, complete exact
   request binding/digest, copied deadlines and capability/launch identities,
   exact versioned source access/Secret descriptor, and non-secret response
   descriptor/digest; set the reciprocal Attempt pointer and move the Activity
   to `ACTIVE`.
4. Commit before returning the Attempt capability, source-bootstrap material,
   and, for model-backed work, one-shot launch capability. Provider material is
   withheld until Launch Attestation acceptance; deterministic VERIFY receives
   neither launch nor provider material.

If the claim response is lost, the same authenticated worker session and claim
UUID/request digest MAY retry and receive the same AttemptClaim, an equivalent
non-secret contract, and only currently valid sensitive fields. Source/launch
authority and source Secret materialization end strictly at the execution
deadline; before that time any source credential names the same frozen Secret
version. During the later Result-auth grace, only the same Attempt capability
bearer for the exact unchanged pinned signed claims may be rematerialized;
server-side endpoint policy permits only Result reconciliation and no distinct
claim set is signed. At/after authentication expiry no capability is
rematerialized. Bearer bytes are never stored. Reuse of the key
with another body is `IDEMPOTENCY_CONFLICT`; another key or session receives
`ATTEMPT_ALREADY_CLAIMED`. The capability
remains bound to the same Activity, generation, worker identity, audience, and
execution deadline/authentication expiry; it also binds the immutable
execution/family/classification values and assignment digest when applicable.
Only the domain-separated canonical claims digest/JTI, not bearer bytes, is
durable.

For model-backed work, source materialization and scrubbing precede model
launch. The trusted registered runner shim constructs fresh workspace,
context, and invocation IDs and submits the signed
`orcest.launch-attestation/1` object with the one-shot launch bearer only to
HTTPS `POST /api/v1/attempts/{attempt_id}/launch-attestations`. One writer
transaction rechecks the exact CLAIMED Attempt/session, strict execution
deadline, nonce/capability, registered runner principal/image/revision/signing
key, null parent IDs, true freshness flags, global instance-ID uniqueness, and
signature; then it inserts LaunchAttestation and consumes the launch capability.
Only the sensitive accepted response materializes the exact provider bytes for
the Attempt's pinned versioned `provider_secret_ref`. Identical ID/digest replay returns the
same Attestation and may rematerialize equivalent provider bytes only while the
Attempt remains current and before its deadline; conflicting identity/content
reuse is rejected. No provider bytes, launch bearer, or signature enter Redis,
ordinary logs, or SQLite. Missing or invalid attestation leaves the claim
unchanged for correction, deadline, or exact-session loss recovery and cannot
produce a Result or Transition.

Claim and execution deadlines are absolute UTC Unix milliseconds stored in
SQLite and are never extended by a Redis heartbeat. An unclaimed `OFFERED`
Attempt at or beyond its claim deadline is made terminal before any later
Recovery-Evidence transition offers a replacement or waits for capacity. Redis leases provide prompt
liveness and fleet utilization only. Losing a heartbeat or flushing Redis MUST
NOT by itself fence a `CLAIMED` Attempt: the controller first
probes/reconciles the worker and waits for an authoritative pool loss report or
the execution deadline. A pool-manager loss report is accepted only when it
names the exact Attempt ID, Activity, generation, and worker-session identity
currently stored.

Attempt liveness is the deliberate non-ledgered exception. For the exact
current claimed Attempt/session, Redis stores only the greatest authenticated
positive liveness sequence and renewable lease. A lower or duplicate sequence
cannot rewind it; after Redis loss the next greater worker sequence establishes
the disposable high-water mark again. SQLite stores no liveness idempotency key
or original response. Each response is freshly derived from current durable
control plus the Redis write outcome, and no liveness call extends either
deadline or creates a reducer input.

When an `OFFERED` claim deadline is reached, the writer marks that generation
`EXPIRED` only in the one transaction that re-reads the due deadline, persists
the matching `ATTEMPT_CLAIM_DEADLINE` Timer Fact, freezes the ordered highest-
applicable unexpired Health membership and digest, inserts the immutable
`CLAIM_DEADLINE` Attempt Terminal Fact with its capacity disposition, exact
logical-provider Secret version, Controller Mode/Capability Registry
projections, and replacement-offer disposition, then applies the sole
`T(ATTEMPT_TERMINAL, attempt_terminal_fact_id)`. For an ordinary Activity this
terminal transaction sets the Activity to `PLANNED`, enters `RECOVERING`, and
appends exactly one zero-counter Recovery Evidence row. It creates no
generation, worker Outbox, or Wait. The later
`T(RECOVERY_EVIDENCE, recovery_evidence_id)` transition alone applies the
frozen deterministic tactic: it may create generation `g + 1` and its Outbox
for `COMPATIBLE_AVAILABLE` plus `OFFER_ALLOWED`, or the exact
`WAITING/CAPACITY` Wait Condition for `NO_COMPATIBLE_AVAILABLE` plus
`OFFER_ALLOWED`, after rechecking current gates. `MODE_BLOCKED` or
`ISSUANCE_KEY_UNAVAILABLE` leaves the Activity `PLANNED` and the Evidence
pending; it cannot create either result. For a panel with an unfilled slot and
a peer `CLAIMED`, the terminal transaction is the sole exception: it remains
in REVIEWING/ADJUDICATING, appends no Recovery Evidence, creates no Wait or
offer, preserves that peer, and replaces the Run panel-staffing pointer with
the latest coalesced Terminal Transition. With no claimed peer, the later
Evidence transition evaluates the complete panel and performs the all-or-none
offer or panel-Wait action. It never synthesizes a Health Observation from
absence; only a later authenticated Capacity Report can append the observation
that wakes a Wait. No branch advances failure, repair-cycle, diagnosis, rescue,
or other recovery counters; an unclaimed offer is delivery/capacity state, not
failed execution.

When a `CLAIMED` execution deadline passes or an authoritative loss is
accepted, the writer first inserts or finds the source-unique
`EXECUTION_DEADLINE` or `WORKER_LOST` Attempt Terminal Fact. The same reduction
transaction records the Attempt outcome, fences that generation, appends its
Recovery Evidence with post-application counters, returns the owning Activity
to `PLANNED`, and enters `RECOVERING` without applying the tactic. A loss fact
references the exact Health
Observation proving the claimant/session loss. If that Evidence selects
execution retry, only the later
`T(RECOVERY_EVIDENCE, recovery_evidence_id)` transaction may insert generation
`g + 1` and its outbox, subject to the current offer gates.

An old worker may finish later, but its upload or result cannot pass the
current-generation conditional guard. Routine timeout, worker loss, Redis
loss, and provider unavailability feed autonomous recovery; none is a valid
`needs-human` reason.

## Candidate artifact admission

A Candidate is one immutable Git bundle containing one proposed tip. The
worker protocol defines authentication and upload limits; the controller owns
durable admission. Creating an upload first commits a `candidate_uploads` row
bound to the exact current Attempt and request idempotency key. Returning an
upload ID does not create a Candidate.

The byte-upload phase MUST:

1. Stream to a unique temporary file under `candidates/incoming` without
   following links, while enforcing configured compressed-byte,
   uncompressed-object, and object-count limits and computing SHA-256.
2. Close and reopen the file through the trusted path; verify its digest,
   length, Git bundle structure, single proposed tip, object connectivity,
   pinned base relationship, and `{object_format, oid}` commit identity.
3. Set mode `0600`, fsync, close, install without replacement at an
   upload-ID-specific incoming path, and fsync the incoming directory.
4. In SQLite, conditionally change the matching upload from `RECEIVING` to
   `VALIDATED` and record its controller-derived digest, size, and commit only
   when `controller_now_ms < expires_at_ms`. At equality or later, atomically
   change the still-unused row to `EXPIRED`; the durable bytes are never
   validated by a later retry. An identical timely retry returns `VALIDATED`;
   different bytes for the idempotency key are rejected.

Only finalization of a successful Candidate-producing Attempt result promotes
the validated upload. Finalization MUST acquire the shared storage mutation
lock before its final recheck and hold it through object installation, every
required fsync, the promotion transaction, and the Candidate/reference Result
transaction. Finalization MUST:

1. Recheck the current `CLAIMED` Attempt and its `VALIDATED` or already
   `PROMOTED` upload, with controller time strictly before both the upload
   expiry and Attempt execution deadline. A promoted retry must match every
   stored digest, size, tip, and storage key.
2. For a `VALIDATED` upload, install the bundle without replacement at
   `candidates/objects/sha256/<first-two>/<64-hex>.bundle` using an atomic
   no-clobber operation. If that object already exists, verify its regular-file
   type, owner, mode, length, and digest rather than replacing it.
3. Fsync the incoming and destination directories and every newly created
   parent directory.
4. In a first SQLite transaction, recheck the exact upload/Attempt binding and
   require controller time still be strictly before `expires_at_ms`; insert or
   verify the immutable `artifact_objects` inventory row, and conditionally
   change the upload from `VALIDATED` to `PROMOTED` with the final bundle
   digest, storage key, and promotion time. Commit `PROMOTED` before creating
   any Candidate or accepting any Result. An already identical, unexpired
   `PROMOTED` row makes this step idempotent.
5. In a second SQLite transaction, recheck the full Attempt Result guard,
   including the current generation and execution deadline, require
   `controller_now_ms < expires_at_ms`, and require the exact upload still be
   `PROMOTED`. Resolve the unique
   `(run_id, specification_generation, commit)` identity. For a new Candidate,
   insert the Candidate referencing the promoted Artifact Object. For an
   existing Candidate, retain its recorded bundle; if the promoted
   representation differs, clear the upload's live promoted-object reference
   while retaining its computed digest/size/tip as audit data. Mark the upload
   `CONSUMED` with the resolved Candidate, accept the Attempt Result, and
   persist the reducer Transition and any next Activity/Attempt/outbox rows.
6. If the initial or either transactional recheck observes `controller_now_ms >=
   expires_at_ms`, conditionally change the still-unused `VALIDATED` or
   `PROMOTED` row to `EXPIRED`. For `PROMOTED`, clear its promoted-bundle and
   storage-key live references atomically while retaining `promoted_at_ms`
   as historical audit, leaving the installed object for ordinary orphan
   grace. Reject the first Result without
   creating a Candidate or Attempt Result. Release the storage mutation lock
   after either the expiry transaction or successful second database
   transaction commits; return acceptance only for the latter.

The controller MUST calculate both the canonical `sha256:<64-hex>` bundle
digest and verified tip `{object_format, oid}`; worker-supplied values are
assertions only. A crash after an incoming or object file is durable but before
the promotion transaction leaves a `VALIDATED` row plus a possible orphan; the
reconciler verifies the exact content-addressed object and, only while strictly
before upload expiry, commits `PROMOTED`; otherwise it expires the row and
leaves the object for orphan grace. A crash after `PROMOTED` but before the Candidate
transaction leaves an explicit durable pre-reference boundary. Recovery and
backup verify that Artifact Object, then resume admission only if the Attempt,
generation, capability, upload, and deadline guards still pass. Otherwise the
writer expires the upload, clears its live promoted-object reference, and
leaves the object for ordinary orphan grace/GC. A crash after the second
transaction leaves a recoverable Candidate and accepted Result even if the
response was lost. Database commit of `PROMOTED` before file and directory
fsync, or Candidate/Result commit before `PROMOTED`, is forbidden.

Upload expiry is reconstructed from `candidate_uploads.expires_at_ms`, not a
Redis timer and not a workflow Transition. Normal and startup sweepers use the
same conditional state changes as the request path. A due `RECEIVING` or
`VALIDATED` row may become `EXPIRED` under the single writer. A due `PROMOTED`
row is expired only while holding the shared storage mutation lock, clearing
its promoted object/storage-key live references in the same transaction. The
request path and sweeper therefore serialize: a first Result either commits
`CONSUMED` strictly before expiry or observes/commits `EXPIRED`, never both.

Candidate storage is content addressed by bundle digest; review and
publication bind to the verified commit and Candidate ID. Within one Run and
specification generation, an already-admitted verified commit resolves to its
existing Candidate ID. A new upload representation of that commit does not
create a second Candidate; its promoted bundle remains unreferenced unless it
is the representation already recorded for the Candidate and is therefore
eligible for normal orphan collection. Equal commits do not imply equal bundle
representations, and equal bundle digests do not authorize reuse across
Attempts without the normal admission and generation checks.

### Controller-imported Candidates

An externally advanced run-owned publication is admitted through a
controller-class `IMPORT` Activity, never a fabricated worker Attempt or
Attempt Result. The Activity binds the exact current Forge Observation,
Publication effect generation, observed head, specification generation, and
base policy. Using only the controller's scoped source-read credential, the
import executor fetches that commit into an untrusted staging area, constructs
a one-tip bundle, and applies the same object-count, expanded-size,
connectivity, base-relationship, hook, commit-identity, and path-safety checks
as worker admission. Candidate code is not executed during import.

After staging and validation, the executor acquires the shared storage mutation
lock. It then rechecks that the `IMPORT` Activity is current and `ACTIVE`, the
bound Forge Observation and external head remain the reducer input, the
Publication effect generation has not been superseded, and the specification
generation is current. It installs and fsyncs the content-addressed object
before one SQLite transaction:

1. Repeat the current Activity, observation, head, Publication-effect, and
   specification guards.
2. Resolve the unique
   `(run_id, specification_generation, object_format, commit_oid)` identity.
3. For a new identity, insert or reuse `artifact_objects`, insert a Candidate
   with `provenance_kind = FORGE_IMPORT`, the producing `IMPORT` Activity, and
   `import_forge_observation_id`, while leaving both worker Attempt fields null.
4. Insert the successful `IMPORT` Controller Operation Fact with the resolved
   `output_candidate_id` and exact Forge Observation membership. Its insertion
   atomically marks the controller Activity successful and invokes
   `T(CONTROLLER_OPERATION, controller_operation_fact_id)`.
5. In that reduction, select the resolved Candidate, invalidate gates as
   required by the domain policy, and persist the Transition plus next
   Activity/Attempt/outbox rows.

The lock is released only after this transaction commits. If the content
identity already exists, the transaction reuses that Candidate after all of
the same fencing and admission checks; it does not overwrite the Candidate's
original provenance or bundle representation. The reducer applies the normal
same-commit progress and gate-eligibility rules. A stale import loses the
conditional guard and leaves at most an unreferenced bundle. A crash after
filesystem durability but before commit is ordinary orphan recovery; a crash
after commit recovers the successful `IMPORT` Activity and Candidate without
refetching for authority.

## Secret Store and rotation

A Secret Reference is `(secret_id, version)`, where `secret_id` is a lowercase
canonical UUIDv4 and `version` is a positive integer. Neither value contains a
provider, repository, account name, or secret-derived hash. SQLite and Redis
may contain that versioned reference and non-secret purpose/scope metadata;
they MUST NOT contain the corresponding secret value.

The v1 Secret Store is controller-only local storage under the state root.
Every directory MUST be `0700` and every immutable secret-version file MUST be
`0600`. The controller MUST reject symlinks, hard links, wrong ownership, group
or world access, and paths escaping the Secret Store. Secret values MUST NOT
enter logs, traces, metrics, errors, Candidate artifacts, normal events,
outbox rows, Redis envelopes, or projections.

An Attempt-scoped rotation is first identified by its caller UUID and exact
`orcest.credential-rotation/1` authority tuple. The secret body streams only to
protected Secret Store incoming storage, whose opaque operation-bound keyed
request attestation proves equality without exposing an unkeyed digest. A
submitted stale prior version commits only a `CAS_LOST`
CredentialRotationRequest and stored HTTP 409 response, then quarantines the
losing stage under the storage lock; it creates no Receipt, Version, reference,
fanout, Result, or Transition. The current-prior `APPLIED` path and the final
installation phase of a Management Provision Operation use this common
write-before-reference protocol:

1. Under the single writer, authenticate the closed source union and allocate
   the next version plus UUID Receipt identity without modifying the live
   reference. `ATTEMPT_ROTATION` must be the exact current `CLAIMED`
   model-backed Attempt/session/capability with accepted Launch Attestation,
   matching provider account/Secret/prior version, and controller time before
   its execution deadline, and the request identity/digest and opaque keyed
   request-attestation ID are frozen for the eventual ledger row.
   `MANAGEMENT_PROVISION` must be an authenticated,
   authorized server secret-management operation with exact scope and prior
   version. Strictly before the execution deadline, a terminal Attempt permits
   only exact existing CredentialRotationRequest response replay; Receipt
   identity alone is never a replay/authentication key. At or after the deadline
   the rotation endpoint denies replay.
2. Write the value to a unique `secrets/incoming` file opened exclusively.
3. Acquire the shared storage mutation lock, then recheck the expected current
   version, allocated version, request idempotency identity, and absence or
   exact-byte identity of the immutable destination. If an Attempt rotation's
   expected prior is no longer current, commit its `CAS_LOST` Request and
   exact response and stop before installation.
4. Compute a domain-separated keyed integrity authenticator over the canonical
   Secret Version key and exact stored bytes using the controller-only Secret
   Store integrity key. Set mode `0600`, fsync, close, install the immutable
   version and its controller-only integrity metadata without replacement, and
   fsync the incoming, version, metadata, and newly created parent directories.
5. In one SQLite transaction, insert the `APPLIED` CredentialRotationRequest
   when the source is an Attempt rotation, then insert the reciprocal Credential
   Rotation Receipt with its
   complete tagged non-secret authority and opaque integrity-attestation ID;
   insert `secret_versions` with its required `creation_receipt_id` and only
   non-secret path/identity metadata; and compare-and-swap
   `secret_refs.current_version` for the same `secret_id` from the expected
   prior version; freeze the canonically ordered `secret_version_runs`
   membership of every active exact Secret Wait/Boundary satisfied by the new
   version and its `affected_run_ids_digest`; and insert its durable
   `secret_version_fanouts` intent. Reciprocal Receipt/Version foreign keys are
   deferred within this transaction. No per-Run Transition is part of this
   install transaction. For initial creation the same
   transaction inserts the Secret Reference and version before making that
   version current.
6. Store the exact closed response status/body/digest in the Request, release
   the storage mutation lock, and acknowledge rotation only after commit.
   Management Provision stores its already-defined Operation terminal response
   instead of a CredentialRotationRequest.

The fanout reconciler then walks the frozen membership in ordinal order. Each
member uses its own writer transaction to append or replay
`T(SECRET_VERSION, canonical Secret Version key)` and advance the durable
fanout cursor. A still-matching Wait/Boundary resumes; a member that changed
after membership selection records the defined same-state stale-member
Transition. Generation-independent Transition uniqueness makes each retry
return the original reduction rather than applying the version twice. A crash
after any prefix is expected: restart resumes only the missing ordinals from
SQLite until the intent becomes `DELIVERED`.

A crash before step 5 leaves only an operation-bound staged or orphan version
whose keyed request proof cannot grant authority without its exact request. A
crash after step 5 but before acknowledgement is reconciled by reading the
CredentialRotationRequest and current reference, verifying the live file and
original creation Receipt, and returning the stored response. A stale rotation cannot
overwrite a newer version. A live reference to a missing or unreadable version
disables use of that credential and fails closed; the controller MUST NOT fall
back silently to an older version.

If an idempotent retry encounters its allocated version file but no committed
Request/rotation Receipt, the controller verifies through the opaque protected
request attestation that the bytes match the exact request and retries the same
authority checks and compare-and-swap. Different
bytes at the same
`(secret_id, version)` are an integrity conflict and are never replaced.

Initial provisioning and explicit adoption use authenticated
`POST /api/v1/secrets/provisioning-operations` with protocol
`orcest.secret-provision/1`. Its bounded multipart metadata contains caller
UUID `secret_provision_operation_id`, mode `PROVISION` or `ADOPT_EXISTING`,
`secret_id`, nullable `expected_prior_version`, code-owned `purpose`, exact
`owner_scope_kind/owner_scope_id`, and conditional non-secret
`provider_account_ref`. `PROVISION` streams one exact secret body directly to
protected Secret Store staging. `ADOPT_EXISTING` carries only an adapter-
internal one-use protected locator that the Secret Store consumes; neither the
locator nor bytes are persisted outside that store. Transport authentication
and server RBAC are required for the exact mode/scope/CAS.

After protected staging/adoption is fsynced and keyed verification succeeds,
the first writer/storage-lock transaction inserts a `PENDING`
SecretProvisionOperation with its immutable positive `target_version` and its
due generic Outbox row with `source_kind = SECRET_PROVISION_OPERATION` and
`source_id = secret_provision_operation_id`. Allocation verifies the expected prior version
and reserves the exact no-clobber `(secret_id, target_version)` identity but
does not update the live SecretRef. Only then may the endpoint acknowledge
acceptance. The exact staged object, opaque staging
receipt, and keyed integrity metadata are durable retry roots; a retry uses the
Operation identity, target version, and staging receipt and never asks the
client to resend the secret or reallocates a version.

The reconciler appends `VERIFY_STAGING` and `INSTALL_VERSION` checkpoints. A
retryable failure appends `FAILED_RETRYABLE`, advances `last_checkpoint_id`,
and reschedules the same outbox row in one transaction. After the Secret Store
has verified and no-clobber-installed the exact immutable version and fsynced
its file and directory, one writer transaction appends the sole
`INSTALL_VERSION/SUCCEEDED` checkpoint; inserts the `MANAGEMENT_PROVISION`
CredentialRotationReceipt and SecretVersion with reciprocal
`creation_receipt_id`; performs the SecretRef create/CAS; freezes affected-Run
membership/digest and inserts its durable fanout intent; marks the Operation
`COMPLETED`; and delivers that same generic Outbox. Per-Run Transitions are applied
later by the ordinal reconciler in independent idempotent transactions. A crash
after Secret Store install but before the completion transaction is
resolved by verifying the same Operation-bound staged/installed object and
replaying the transaction; it never allocates a different version.

The initial `PENDING` response is the deterministic unstored HTTP 202
projection with exactly `protocol = orcest.secret-provision-accepted/1`,
`secret_provision_operation_id`, `state = PENDING`, `secret_id`, and
`target_version`. A same-request replay while still pending derives those same
canonical bytes; there is no `replayed`, prior/current version, mode, retry
time, Receipt, or diagnostic field.

A `COMPLETED` HTTP 200 terminal body has exactly
`protocol = orcest.secret-provision-result/1`, Operation ID,
`state = COMPLETED`, canonical `secret_version_key`, `target_version`, equal
`new_version`, and `credential_rotation_receipt_id`. A `REJECTED` terminal body
has exactly that result protocol, Operation ID, `state = REJECTED`, `secret_id`,
`target_version`, and its closed `rejection_code`; its HTTP status follows the
schema mapping. Fields from another variant and `replayed` are forbidden.
No response contains bytes, prior/current version, reusable locator, integrity
verifier, free-form provider/storage error, or undeclared field.

A terminal checkpoint with `CAS_LOST`,
`AUTHORITY_REVOKED`, `STAGED_OBJECT_INVALID`, or `INTEGRITY_CONFLICT` atomically
marks the Operation `REJECTED`, stores the canonical rejection response, and
delivers that same generic Outbox without creating a Receipt, Version, reference
mutation, fanout, or Transition. An
identical authenticated operation ID and complete request/proof returns the
same Operation: while `PENDING`, replay deterministically projects a 202 body
from immutable Operation fields; after completion or
rejection, replay returns `terminal_response_json`. If async completion races a
lost 202 response, the retry returns whichever current durable projection won,
never a stale acceptance body. Conflicting reuse is rejected.

Migration of every pre-existing Secret Version MUST run as an explicitly
operator-authorized `ADOPT_EXISTING` operation after its controller-only keyed
verifier is established. The migration creates the real Operation and
creation Receipt and links the Version; it cannot synthesize historical worker
rotation authority or a placeholder Receipt.

Existing Secret Versions MUST be migrated by computing and persisting the
keyed verifier only in controller-protected Secret Store metadata under the
shared storage lock as part of that adoption before per-object authenticated
restoration is enabled. An unkeyed digest of secret bytes is forbidden because
it can disclose low-entropy values. For `SECRET_VERSION`, the restoration
operation resolves its expected verifier from that protected metadata and
recomputes the keyed tag over staged bytes; it never trusts or returns a
caller-supplied SHA of the secret. The integrity key is held outside ordinary
workflow state through the Secret Store root of trust. SQLite retains only an
opaque `secret_integrity_attestation_id` on a successful restoration Fact;
neither the key nor tag enters the database or API.

Attempt-scoped credentials and upload capabilities SHOULD be signed or minted
from controller keys referenced through the Secret Store. Their bearer values
MUST be returned only from authenticated controller endpoints and held in
worker memory or a worker-local protected temporary file for the duration of
the Attempt. Redis receives only Secret References or non-secret capability
IDs.

## Per-object storage restoration

A missing or corrupt live Candidate object, Secret version, or Workflow Blob
does not authorize
the controller to substitute another Candidate, roll back a Secret Reference,
or fail every Project when SQLite remains internally consistent. The writer
first commits an exact `STORAGE_OBJECT_INTEGRITY` or
`SECRET_VERSION_INTEGRITY` HealthProbeRequest and outbox before inspection I/O.
The dispatcher holds the shared storage mutation lock, reuses that request
identity, and only its reciprocal HealthProbeFact may record the
`UNAVAILABLE` Health Observation for the exact `STORAGE` or `SECRET` scope.
The Fact/Observation completion freezes the affected Run set. For each member,
the exact `T(HEALTH_OBSERVATION, health_observation_id)` transition fences every
Attempt/effect that could consume the object, preserves the applicable resume
state, enters `RECOVERING`, and appends Observation-sourced Recovery Evidence
selecting `WAIT_EXTERNAL`: category `STORAGE` for a Candidate Artifact or
Workflow Blob and category `CREDENTIAL` for a Secret Version. It does not
create a Wait Condition in that transition. The separate
`T(RECOVERY_EVIDENCE, recovery_evidence_id)` reduction creates the bound
`STORAGE_RECOVERY` or `SECRET_RECOVERY` Wait and enters `WAITING`. Thus each Run
has exactly the canonical two-transition evidence path; restart resumes the
missing Evidence reduction rather than inferring or combining it.
An unrelated Human Boundary remains current with the integrity input retained
for revalidation after resolution. Any prior Wait is superseded while its
underlying resume state is preserved.
Cancellation cleanup retains `PR_MONITORING` as resume state and no semantic
work is planned. It suspends result/receipt acceptance and publication for the
affected Run and Project/credential scope and leaves unrelated scopes
operational. Detection and restoration share the same lock, so restoration
cannot race between the `UNAVAILABLE` observation and these Wait rows. A raw
filesystem exception, startup scan, or untracked probe result never creates
health/lifecycle authority directly.

The controller MUST then attempt autonomous exact-object restoration in this
order:

1. Recheck the canonical path and any known incoming or quarantine entry under
   the shared storage mutation lock; a concurrent finalizer or collector may
   already have repaired or moved the object.
2. For autonomous restore, enumerate only backups carrying a complete marker
   and a valid manifest in deterministic newest-first order. Verify the entire
   selected manifest and the exact object's path, type, mode, byte length, and
   SHA-256 before using it. A Candidate copy must equal its
   `artifact_objects.bundle_digest`; a Secret copy must be the exact immutable
   `(secret_id, version)` encrypted envelope, match that envelope's
   authenticated backup-manifest digest, decrypt only into protected Secret
   Store staging, and reproduce its restored metadata's mandatory keyed
   integrity verifier and the SQLite SecretVersion's immutable
   `creation_receipt_id`. Restoration never creates or substitutes a creation
   Receipt. A Workflow
   Blob copy must come from a verified standalone SQLite
   backup and reproduce its exact frozen media kind, normalized byte length,
   and domain-separated live Snapshot-referenced digest. Conflicting
   valid Secret or Blob copies are an integrity ambiguity, not a majority vote.
   For an authenticated Storage Restoration Operation, use only
   its exact already-fsynced kind-specific staging object after rechecking its metadata,
   kind-specific SHA or Secret Store verification, principal, authorization,
   and operation ID; no backup manifest is involved.
3. For a Candidate or Secret, copy the verified bytes to a unique incoming file, set the required mode,
   and fsync it. Acquire the storage mutation lock, repeat the live-reference
   and destination checks, quarantine any corrupt destination without
   unlinking it, install without replacement, fsync the file and all affected
   directories, and recompute the Candidate SHA or Secret Store keyed
   authenticator as appropriate. For a Workflow Blob, stage
   and normalize the bytes, recompute the domain-separated identity from exact
   media kind/length/content, then restore only that exact digest under an
   expected-digest SQLite repair transaction and the same storage barrier;
   mutable forge content is never a fallback.
4. While retaining the storage lock, in one SQLite transaction insert or find
   the source-unique Storage Restoration Fact, including exact shared object,
   conditional matching digest or Secret Store attestation ID,
   complete-backup restore identity and manifest or accepted authenticated
   storage-operation identity, and installed-object verification proof. Append
   the exact-source `RECOVERED` Health Observation with
   `source_kind = STORAGE_RESTORATION` and
   `source_id = storage_restoration_fact_id`; freeze only the canonically sorted
   Runs whose current exact-object Wait Condition or `INTEGRITY_FAILURE` Human
   Boundary matches; then reduce each member with
   `T(STORAGE_RESTORATION, storage_restoration_fact_id)`.
   An exact current storage/secret Wait Condition is cleared into `RECOVERING`,
   copies its resume binding, and appends kind-correct `STORAGE` or `CREDENTIAL`
   Recovery Evidence only after all its bindings revalidate; the separate
   Evidence Transition resumes or selects the next tactic. An exact current
   `INTEGRITY_FAILURE` Human Boundary instead receives the one
   `INTEGRITY_RESTORED` Human Resolution sourced from this Fact and verified
   controller principal, clears the boundary, and enters `RECOVERING` in that
   same transaction. The frozen affected-Run membership is stored in
   `storage_restoration_fact_runs`; replay reduces exactly those rows in
   ordinal order. If a member binding is superseded before its fanout turn, the
   same trigger writes only the legal same-state audit Transition. The
   Resolution is an output, never the trigger.

The lock remains held from the final reference check in step 3 through the
recovery transaction in step 4. A crash before installation leaves staging
bytes only; a crash after fsync but before the recovery transaction is
discovered by the first recheck and completed idempotently. Replay of the same
object/source identity and proof returns the prior Fact, Health
Observation, Transitions, and any Resolution; a different object, source,
manifest, principal, authorization, or installed proof under that identity is
an integrity conflict. Restoration never changes the durable Candidate,
Secret, or Workflow Blob identity and never exposes secret bytes in evidence,
manifests returned through APIs, or logs.

The storage reconciler is the only Fact writer. It may run automatically from
server-configured verified backups. An explicit request uses authenticated,
bounded multipart `POST /api/v1/storage/restorations` with protocol
`orcest.storage-management/1`, caller UUID `operation_id`, exact object,
kind-appropriate expected identity, byte length, and a binary part containing
the exact replacement bytes. Candidate and Workflow Blob requests carry their
content digest; Workflow Blob metadata also carries exact media kind and
normalized byte length used by the domain-separated verifier. Both stream to
general restoration quarantine. For a Secret
Version the controller resolves the expected verifier server-side from the
exact Secret Store metadata; the caller does not submit or receive any secret
digest or tag, and the stream goes directly to an exclusive `0600`
operation-specific path below protected Secret Store incoming storage. The
controller fsyncs the staged object and performs the kind-specific SHA or keyed verification before
one transaction authenticates/authorizes the caller and inserts the immutable
Storage Restoration Operation. The binary bytes never enter SQLite, Redis,
logs, traces, workflow APIs, or evidence. The same operation ID returns an
existing operation only for the same metadata, kind-specific verified staged
identity, principal, and authorization digest. For Secret bytes, the Secret
Store uses an operation-bound keyed request authenticator to reject different
bytes without placing its tag or a secret-derived digest in SQLite or the API;
SQLite retains only its opaque `secret_request_attestation_id`, and the Secret
Store retains the keyed metadata for the operation's audit lifetime so an exact
replay remains decidable after staging bytes are removed. Conflicting reuse is
rejected and the newly staged orphan is collected under the storage lock.
Acceptance leaves the operation `PENDING` and authorizes an attempt; it is not
a Fact or successful restore. The endpoint returns deterministic HTTP 202
`orcest.storage-restoration-accepted/1` derived from immutable Operation fields;
it does not require a separate GET. A `PENDING` operation and its exact staged object
are a live retry root. Temporary install, backup, or storage failure leaves
both intact for the reconciler. After durable installation and verification,
the recovery transaction inserts the
`AUTHENTICATED_STORAGE_OPERATION` Fact with `source_id = operation_id` and
atomically changes the operation to `RESTORED`, fills `resulting_fact_id`, and
stores HTTP 200 plus the exact closed `orcest.storage-restoration-result/1`
body/digest and `terminal_at_ms`. If a storage-lock-protected final recheck proves one of
the closed deterministic rejection reasons, a transaction instead changes it
to `REJECTED` with the exact code, mapped HTTP status, closed result body/digest,
and time; uncertainty or transient I/O never becomes rejection. Same operation/
payload replay returns the current pending projection or stored terminal result,
so a lost 202 racing terminal reconciliation converges without another stage or
Fact. Repository configuration, workers, and Run comments cannot
create the operation or Fact.

If no verified complete backup contains the exact object, the controller
persists each failed source and selected next-eligible retry in Recovery
Evidence and continues configured autonomous reconciliation. Only after all
configured local and backup sources are deterministically exhausted may the
affected Run enter an `INTEGRITY_FAILURE` Human Boundary. A verified later
restoration may satisfy it only through an authenticated `INTEGRITY_RESTORED`
Human Resolution bound to that object and Run. SQLite corruption, failed
foreign-key checks, or an unreadable schema remains a global fail-closed
condition because safe per-object scope cannot then be proven.

### Controller State physical schema

`ControllerState` is the durable singleton for storage/reducer compatibility
and Redis reconstruction metadata. It is separate from the Controller Mode
projection and never replaces that mode's operation ledger.

```text
PRIMARY KEY ControllerState(controller_id)
CHECK ControllerState.controller_id = 'ORCEST_V1'
CHECK ControllerState.schema_version > 0
CHECK ControllerState.reducer_version > 0
CHECK ControllerState.compatibility_version > 0
CHECK ControllerState.redis_epoch >= 0
CHECK ControllerState.initialized_at_ms IS NOT NULL AND
  ControllerState.initialized_at_ms > 0
CHECK ControllerState.updated_at_ms IS NOT NULL AND
  ControllerState.updated_at_ms >= ControllerState.initialized_at_ms
GUARDED exactly one ControllerState row exists. A new store creates the
  singleton during the transactional schema bootstrap with the running
  schema_version, reducer_version, compatibility_version, and
  redis_epoch = 0; ordinary endpoints remain closed until migration and
  startup validation confirm that row and an exactly compatible reducer.
  Version fields change only in an exclusive, monotonic schema migration;
  an unsupported schema or compatibility version fails closed before claims,
  Result acceptance, or publication
GUARDED a full Redis reconstruction increments redis_epoch exactly once with
  an atomic single-writer `UPDATE` before deleting/rebuilding the controller's
  Redis namespace. The new value is strictly greater than the committed old
  value; it is never reset, decremented, or allocated in Redis. A crashed or
  repeated rebuild therefore obtains a later epoch and can safely republish
  the durable offer/outbox set. A routine restart that does not rebuild does
  not invent an epoch
GUARDED every complete backup includes the singleton row and its exact
  compatibility versions. Restore rejects a missing, duplicate, malformed, or
  incompatible row before ordinary endpoints open. After a verified restore,
  the restored row is durably committed, redis_epoch is atomically advanced
  once under the restore barrier, and Redis is treated as empty/stale until a
  complete epoch-qualified reconstruction writes its matching marker. Restore
  never trusts or restores Redis keys and never serves a restored epoch as a
  current rebuild marker
```

## Redis reconstruction

`controller_state.redis_epoch` is a monotonically increasing integer. A full
Redis rebuild is required when Redis's allowlisted controller-epoch marker is
missing or does not match SQLite. It performs this sequence while new dispatch
is paused:

1. Open and validate SQLite, Candidate storage, and the Secret Store; apply
   scoped storage suspension and autonomous exact-object restoration when only
   an individual live object is unavailable.
2. In SQLite, increment and commit `redis_epoch`.
3. Delete and recreate only the exact, configured workflow stream, group,
   liveness, and cache keys owned by this controller, or select a fresh
   epoch-qualified namespace. Never use a broad wildcard deletion in a shared
   Redis database, and never delete unrelated fleet-manager state.
4. Rebuild timer authority by scanning durable Wait Condition `not_before_ms`,
   Health Observation `expires_at_ms`, Attempt claim/execution deadlines, and
   Recovery Evidence `next_eligible_at_ms`; insert/reuse every due
   scope/deadline-unique Timer Fact and apply its closed owner path until a
   stable pass: direct `T(TIMER_FACT, timer_fact_id)` only for Wait/Health,
   one source-bound Attempt Terminal Fact/Transition for Attempt deadlines,
   and only `T(RECOVERY_EVIDENCE, recovery_evidence_id)` after a recovery-
   eligibility Fact. Redis timer keys are not consulted as evidence.
5. After the Timer-Fact pass has reduced expired claim/execution deadlines,
   republish every current, unexpired `OFFERED` Attempt from its outbox row with the new
   epoch, regardless of the outbox row's prior Redis entry ID.
   Terminal Duplicate Cleanup Actions are controller-subsystem work, not worker
   stream entries; reconstruct each ACTIVE Reservation from its durable cursor
   and reciprocal Action/outbox without placing it in a worker queue.
6. Do not republish an unexpired `CLAIMED` Attempt as schedulable work. Permit
   its worker to re-establish disposable liveness, report a result, or reach
   its durable deadline.
7. Rebuild derived capacity, cooldown, wake-up, and projection caches only
   from SQLite and fresh external observations.
8. After the sweep reaches a stable pass, write the matching Redis epoch marker
   and resume ordinary dispatch.

The Redis marker is a rebuild-completion projection, not authority. A crash
before step 8 causes another epoch and another safe replay. A crash after step
8 is recovered by ordinary outbox reconciliation.

`redis_epoch` is diagnostic reconstruction metadata, not a claim or result
fence. A worker MAY prefer a current-epoch notification or discard an obvious
duplicate when it already has the current one, but it MUST submit an otherwise
well-formed old-epoch offer to the controller for the authoritative decision.
The claim endpoint decides from durable Attempt, outbox, worker-session, and
generation state. An old notification is harmless because it cannot bypass
those checks, not because the worker is trusted to discard it.

A reconstructed liveness lease may use the pinned restart-grace duration, but
that grace MUST NOT extend the Attempt's fixed execution deadline. Repeated
controller or Redis restarts therefore cannot keep an Attempt alive past its
durable execution bound.

Redis stream pending state, ACK state, attempt counters, current task payloads,
private credential checkpoints, and pool liveness MUST NOT be required to
reconstruct workflow truth in v1. ACK and trim operations are storage hygiene.
If Redis is unavailable, the controller continues to accept already-claimed
Attempt results when their capabilities and generations remain valid, but does
not dispatch new work.

## Controller restart recovery

An ordinary controller-only restart does not require draining workers. Startup
recovery MUST:

1. Acquire the single-writer lock and reject a second controller.
2. Apply migrations, validate SQLite structure/relations, enumerate referenced
   objects without treating a raw filesystem/Secret-Store read as lifecycle
   authority, and load pinned reducer/configuration versions required by
   nonterminal Runs. Database integrity failure stops globally.
3. Validate and load the reciprocal Controller Mode projection/last Operation
   and enforce it before opening mutation endpoints. A new revision-0/null row
   admits only the registered bootstrap service's one INITIALIZE to revision
   1/MAINTENANCE; ordinary endpoints remain closed before that commit. A
   restored initialized row is forced through the authenticated
   RESTORE_BACKUP CAS: MAINTENANCE remains in place with copied prior ancestry,
   exact DISPATCH_PAUSED/PAUSE_ADMISSION remains in place, and every other mode
   is installed as DISPATCH_PAUSED/PAUSE_ADMISSION at the next revision before
   ordinary endpoints open. No branch resumes an operational backed-up mode
   automatically. Validate the Capability
   Key Registry revision/current key/last Operation; a null or non-ACTIVE
   issuance selection keeps new-offer planning, delivery, and claims paused.
4. Reconstruct every PENDING HealthProbeRequest and its source-tagged Outbox.
   Every startup Candidate, Workflow Blob, or Secret integrity determination
   first commits or resumes that request before probe I/O and can become
   lifecycle authority only through its reciprocal Fact/Observation. Resume
   each PENDING SecretProvisionOperation from its durable source-tagged Outbox
   and checkpoints. Scan every ACTIVE Terminal Duplicate Cleanup Reservation;
   validate its complete selected-search/member/digest graph and resume exactly
   its cursor ordinal, Reservation-bound polling/search Schedules/Requests, and
   existing nonterminal Action/outbox, creating no new semantic Run work.
5. From those typed Facts, resume incomplete Candidate, Secret, and exact-object
   backup reconciliation before accepting claims in each affected scope.
   Verify every Capability
   Signing Key public digest/state transition, retained reference/expiry
   horizon, and available private/public pair before enabling claim, launch, or
   Result authentication; a missing verifier is global fail-closed for those
   capability endpoints, not an invitation to remint under a different key.
6. Accept either valid `ADMITTED` phase without guessing: generation 0 with its
   capture-sequence-1 pending Snapshot resumes only the unique
   `SPEC_SUPERSEDE`, while generation 1 with that Snapshot installed resumes
   only `T(INTERNAL, spec_transition_sequence)` to plan `PLAN`. Neither restart
   continuation can combine, skip, or repeat the three admission transactions.
7. Materialize every due Wait/Health deadline through the durable Timer Fact
   sweeper before using any rebuilt timer or health cache.
8. Reconcile outbox rows and Redis epoch state.
9. Leave unexpired `CLAIMED` Attempts claimed; rebuild their liveness only from
   authenticated worker contact or pool observation.
10. Schedule any remaining Attempt recovery using the same Timer Fact and
   Attempt Terminal Fact reducer path used during normal operation.
11. Reconcile Publications and other external side effects through their owning
   protocol before repeating them.

The controller MUST not infer successful work from a missing Redis entry,
worker, artifact, or response. It also MUST not infer failure merely because
the controller was unavailable while a valid worker continued executing.

## Backup and restore

A complete backup unit consists of a standalone SQLite snapshot, including its
Workflow Blob bytes, every Candidate object, `VALIDATED`/`PROMOTED` upload, and
every Secret version plus protected integrity metadata referenced by that
snapshot; the complete Capability Signing Key registry, immutable public
verification bytes/state-change evidence, Capability Key Registry/Operation
ledger, and every referenced private-signing Secret Version; the Controller
Mode projection/Operation ledger; every exact staging object for a `PENDING` Storage Restoration
Operation; every exact protected stage for a `PENDING` Secret Provision
Operation; and the protected operation-bound keyed replay metadata for every
retained Secret-valued Storage Restoration Operation and Secret Provision
Operation, including terminal `RESTORED`, `REJECTED`, and `COMPLETED` rows; plus
the protected request-bound keyed replay metadata for every retained
CredentialRotationRequest, including both `APPLIED` and `CAS_LOST`.
Pending provision checkpoints and opaque staging receipts are included with
their Operation. Each live Secret or pending Secret-operation stage and all
Secret Operation/rotation-Request replay metadata are stored in authenticated encrypted backup
envelopes; neither plaintext bytes nor an unkeyed plaintext digest appears in
the manifest. Terminal, conflicting, and pre-acceptance operation staging bytes
are not backup roots, but retained terminal operation-bound replay metadata is.
The SQLite snapshot necessarily includes every Terminal Duplicate Cleanup
Reservation, its selected complete-search Observation and Search Members,
ordered cleanup Members, all Action generations, source Outboxes, result
Observations, Reservation-bound Schedules/Requests, digests, and cursor. An
`ACTIVE` Reservation makes that entire
relational graph a backup and retention root even though its Run is terminal.
Redis,
WAL/SHM files, caches, traces, incomplete `RECEIVING` upload bytes, and active
worker workspaces are not backup inputs.

Backup staging and completed backup directories MUST be outside
`/var/lib/orcest/control` on a separately configured backup target. A directory
inside the primary state root, including a `backups` child, is not a backup and
MUST NOT be advertised as a restorable copy. The destination SHOULD use a
separate storage failure domain; its mount, retention, and replication are
deployment configuration, not workflow state. All temporary database
snapshots, copied objects, manifests, and completion markers are created below
that destination, never below the live state root.

The v1 backup procedure MUST use an exclusive controller backup barrier:

1. Freeze the current durable Controller Mode projection and select exactly one
   branch. If it is `MAINTENANCE`, remain in place and preserve its exact
   nullable maintenance-prior ancestry. If it is already exactly
   `DISPATCH_PAUSED/PAUSE_ADMISSION`, remain in that projection and record no
   no-op mode Operation. For every other initialized projection, commit one
   authenticated CAS-valid `SET_MODE` to
   `DISPATCH_PAUSED/PAUSE_ADMISSION` and retain that pause Operation as the
   restore fence. In every branch reject new claims and wait until the
   serialized SQLite count of `CLAIMED` Attempts is zero. The first two
   in-place branches require that count to reach zero without changing mode;
   if it cannot, delay or skip the backup. Never stall a current Result behind
   the barrier.
2. Acquire the shared storage mutation lock and, in the single-writer boundary,
   recheck zero `CLAIMED` Attempts and the exact branch projection—unchanged
   `MAINTENANCE`, unchanged `DISPATCH_PAUSED/PAUSE_ADMISSION`, or the committed
   third-branch pause revision—before marking the bounded barrier active.
   Pause outbox publication, admission, rotation,
   object restoration, and garbage collection. No claim can race this check.
3. Complete a WAL checkpoint and use SQLite's backup API to create a standalone
   database snapshot in the backup destination's unique staging directory,
   without copying a live database file directly.
4. Read the referenced Candidate, validated/promoted upload, Secret-version,
   `PENDING` Storage Restoration Operation, `PENDING` Secret Provision
   Operation, all retained Secret-valued restoration/provision Operation sets,
   and all retained CredentialRotationRequest identities from that snapshot.
   Copy
   the Candidate/upload and pending Candidate/Workflow-Blob restoration staging
   files. For each live Secret and pending Secret restoration/provision, copy
   the exact stage only while its snapshotted Operation is `PENDING`. For every
   retained Secret-valued restoration/provision Operation in any state,
   revalidate and copy its protected operation-bound keyed replay metadata.
   For every retained CredentialRotationRequest, revalidate and copy its
   protected request-bound keyed replay metadata; neither disposition requires
   retaining the submitted staging bytes.
   Write all Secret bytes and metadata only into distinct authenticated
   encrypted backup envelopes.
5. Verify every size and digest available from the database. Secret files MUST
   additionally pass their controller-only keyed verification before sealing;
   each Workflow Blob MUST reproduce its media-kind/length/domain-separated
   identity rather than a raw-content-only hash;
   a mismatch aborts this backup unit but is not itself lifecycle authority.
   After releasing the bounded barrier, the controller commits/resumes the
   exact HealthProbeRequest/outbox and only its Fact/Observation may suspend or
   restore affected Runs;
   the backup manifest authenticates only the envelope ciphertext and never
   records an unkeyed plaintext digest or keyed Secret Store tag.
6. Write a manifest containing schema version, reducer compatibility version,
   creation time, relative paths, sizes, modes, and SHA-256 digests. Fsync each
   ordinary file and each Secret envelope—not plaintext secret bytes—and the
   completed backup directory before atomically marking the backup complete.
7. Before `backup_barrier_max_ms` elapses, release the storage/barrier locks.
   Only the third branch restores its exact frozen prior operational mode; an
   in-place `MAINTENANCE` or exact `DISPATCH_PAUSED/PAUSE_ADMISSION` backup
   leaves that projection unchanged. If the bound is reached, omit the
   completion marker, retain only collectible incomplete backup staging,
   release immediately, and resume service. The barrier never extends an
   Attempt deadline and never returns a backup-specific retryable Result
   response.

After the barrier releases on success or abort, the backup controller submits
one fresh idempotent Controller Mode Operation only when it committed the
third-branch pause. That Operation restores the exact prior mode/intake-policy
from the pause Operation only if the mode revision still equals that pause's
result. An intervening operator change is never overwritten. A crash in that
branch leaves the controller durably paused; retry derives the same prior
projection from the committed pause ledger and performs the same CAS rather
than consulting process memory. An in-place `MAINTENANCE` or exact
`DISPATCH_PAUSED/PAUSE_ADMISSION` backup has no restore Operation to infer or
retry.

Backup directories MUST be `0700` and files `0600`. A backup contains live
credentials and MUST be encrypted before it leaves the protected controller
host. Encryption-key custody and off-host schedule are operational policy, but
an unencrypted off-host backup is nonconforming.

Restore MUST target a new empty state root. Before controller startup it MUST:

- verify the manifest and all file digests and modes;
- run `PRAGMA integrity_check` and `PRAGMA foreign_key_check`;
- prove every live Candidate exists and passes validation; decrypt each Secret
  envelope only into protected Secret Store staging and prove the exact live
  Secret Reference target against its restored controller-only keyed metadata;
- prove every `VALIDATED`/`PROMOTED` upload has its expected bytes, and mark
  restored `RECEIVING` uploads `EXPIRED` so the worker must create a fresh
  upload;
- prove every restored `PENDING` Storage Restoration Operation has its exact
  staged object and, for Secret restoration, its operation-bound keyed metadata
  inside the protected Secret Store; restore these retry roots before the
  storage reconciler runs, while ignoring terminal/orphan staging;
- prove every restored `PENDING` Secret Provision Operation has its exact
  protected stage, opaque staging receipt, keyed operation metadata, and
  checkpoint chain and exact `SECRET_PROVISION_OPERATION` Outbox source;
  rebuild only disposable delivery from that row and resume the same
  operation without client resubmission or a second Secret Store object;
- restore and verify the operation-bound keyed replay metadata for every
  retained Secret-valued Storage Restoration and Secret Provision Operation,
  including terminal rows, before enabling either endpoint's replay lookup;
  terminal staging bytes remain absent and are never reconstructed from replay
  metadata;
- restore and verify the request-bound keyed replay metadata for every retained
  CredentialRotationRequest before enabling credential-rotation replay; no
  rotation secret body is reconstructed from that metadata;
- validate every Terminal Duplicate Cleanup Reservation against its selected
  complete-search membership/digests, reciprocal Publication, ordered Members,
  Action generations, Reservation-bound Schedules/Requests, source Outboxes,
  result Observations, and restart cursor;
  resume an `ACTIVE` reservation only at its durable ordinal and never infer
  completion from the terminal Run alone;
- verify every Capability Signing Key public-key digest, state evidence,
  referenced private Secret Version, and retained private/public pair before
  enabling claim, launch, or Result authentication; an unknown/missing signer
  or algorithm mismatch keeps those endpoints fail-closed;
- reject unsupported schema or reducer versions;
- preserve the failed/current state root until the restored controller passes
  reconciliation; and
- start with an empty or newly namespaced Redis projection and perform a full
   reconstruction.

Because a backup can predate a forge side effect, restore MUST reconcile every
nonterminal Publication and every ACTIVE Terminal Duplicate Cleanup Reservation
before attempting branch, Change Request creation, or cleanup mutation. It
MUST NOT assume that external state rolled back with SQLite.

Backup restore is not considered implemented until an automated drill restores
to a fresh root, rebuilds empty Redis, and resumes representative `OFFERED`,
Candidate-review, Publication, and remediation Runs. A conforming backup was
captured only after the bounded barrier proved zero `CLAIMED` Attempts, so the
restore fixture MUST NOT fabricate or require one. Separately, a controller
process-crash/restart test against the same live state root MUST preserve an
unexpired `CLAIMED` Attempt/session/deadline and accept its still-valid Result;
that fixture is not a backup restore and does not weaken the barrier.

## Retention and garbage collection

v1 MUST retain durable Run rows, transitions, receipts, Publications, and every
referenced Candidate object by default. Automatic deletion of terminal Run
history is deferred until audit-retention requirements are set. Storage
pressure pauses new admission before it deletes referenced state.

Every Budget Report, its frozen Run membership/cursor, and every Forge Request
Failure Fact is retained with its Project/Request and every Transition,
Recovery Evidence, or Wait that references it. Automatic GC has no age-only
rule for these rows; any future authorized purge must first remove the complete
referencing audit graph and cannot leave an idempotency key or fanout cursor
unreplayable.

A Capability Signing Key row, its immutable public verification bytes/digest,
state-change evidence, and referenced private-signing Secret Version are GC
roots while any retained Attempt, Attempt Claim, Launch Attestation, Result
Request, capability audit row, or backup manifest references the key, and at
least through the greatest cryptographic expiry of every capability it signed.
Retirement never deletes verification material. Revocation ends
authentication, not evidence retention; removal is allowed only by a later
authorized retention procedure after both the reference and expiry proofs pass.

The automatic collector may remove only:

- incomplete `incoming` files older than 24 hours with no active upload;
- incoming files of `EXPIRED` uploads that had reached `VALIDATED`, after the
  configured grace period and a repeated check that no Candidate, promoted
  object, accepted Result, or current Attempt references them;
- final Candidate objects with no database reference, after seven days;
- staging objects of `RESTORED` or `REJECTED` Storage Restoration Operations,
  plus conflicting or pre-acceptance restoration orphans, only after seven
  days and the operation/reference checks below; and
- Secret Store staging that never acquired an accepted Secret Provision
  Operation, only after the registration idempotency/backup retention window
  and the exclusive no-reference proof below; and
- quarantine files after a second seven-day grace period and a repeated
  no-reference check.

Candidate—including expired validated incoming-file—Secret Store, Workflow
Blob restoration, Storage Restoration Operation staging, and orphan Secret
Provision staging collection MUST
run under the controller's shared storage mutation lock backed by
`storage.lock`. It holds that lock from its
SQLite reference snapshot through the quarantine rename, directory fsync, and
non-secret audit transaction. On a later deletion pass it reacquires the same
lock, repeats the no-reference check, and only then unlinks. It MUST never
follow links or trust filename age alone. Candidate finalization, import,
Secret rotation, backup, restoration, and GC therefore cannot race an object
from referenced to quarantined. Orphan cleanup failure is retryable and does
not alter SQLite workflow state.

For an expired `VALIDATED` upload, cleanup also requires the row remain
`EXPIRED`, its exact Attempt be terminal or superseded, and every promoted and
consumed live-reference field be `NULL`; a historical `promoted_at_ms` is
allowed. Under the lock the collector renames the exact
upload-ID-specific incoming path to quarantine without following links, fsyncs
both directories, and records the non-secret cleanup audit. It never derives a
Candidate from those bytes. A crash after rename follows the ordinary
quarantine recheck and cannot make the upload live again.

A Restoration Operation staging object is a live GC root while its operation
is `PENDING`, regardless of age. After `RESTORED` or `REJECTED`, the collector
may quarantine it only while holding the shared lock and after rechecking the
operation state, conditional resulting Fact, exact staged path, and every
Candidate/Workflow Blob/Secret Store reference. It records the quarantine move
and directory fsync before release, then observes the ordinary second grace and
locked no-reference recheck before unlink. An upload whose request failed
before an Operation row committed, or a newly staged conflicting replay, uses
the same path-bound orphan procedure and first grace; filename age alone never
proves it orphaned. Secret operation-bound keyed authenticator metadata is not
staging garbage: it remains in the Secret Store for the operation's audit
lifetime so same-ID replay remains decidable, and may be removed only with an
authorized retention transaction that atomically purges the corresponding
durable Operation record. Every complete backup carries this protected replay
metadata for retained Secret-valued Operations even after terminal staging is
gone.

A Secret Provision staging object, its opaque staging receipt, keyed operation
metadata, and checkpoint chain are live retry roots while the Operation is
`PENDING`, regardless of age; they are also included in every complete backup.
They MUST NOT enter the orphan or age-based collector. After `COMPLETED`, the
installed object is the immutable Secret Version and follows that Version's
audit-reference retention; the Operation and checkpoints remain audit roots.
After `REJECTED`, the staged bytes cease to be a retry root but may be
quarantined only after the terminal grace while holding the shared storage
lock and rechecking the Operation state, exact target/storage identity,
source-tagged generic Outbox, every checkpoint/Receipt/Version reference, and retained backup
manifests. The immutable Operation, authority/proof metadata, checkpoints, and
stored rejection response remain audit/replay roots after staging cleanup.
The operation-bound keyed replay metadata for `COMPLETED` and `REJECTED`
Operations remains in every complete backup and may be purged only atomically
with an authorized retention purge of the durable Operation; terminal stage
bytes are not retained merely for replay.
Only a protected stage for which an exclusive storage-lock reconciliation
proves there is no Operation, source-tagged generic Outbox row, checkpoint,
Receipt, Version, or retained backup-manifest reference may be quarantined after the registration
idempotency/backup retention window. The collector repeats that proof after a
second grace before deletion. Secret bytes remain inside the protected Secret
Store throughout, and keyed request/integrity material is never copied into a
general controller quarantine or non-secret cleanup audit.

A Secret Version row, exact bytes, and keyed integrity metadata are one GC root
while that version is current or referenced by any retained Project, Snapshot,
Attempt, Transition, Human Resolution, creation Receipt, Storage Restoration
Operation/Fact, or retained backup manifest. V1 automatic GC never removes
such a version, including a noncurrent version whose creation Receipt remains
in retained audit history. Only after every referencing workflow/audit row and
manifest independently passes its declared retention and is explicitly removed
may an authorized locked retention procedure quarantine and delete the Version
row, bytes, and keyed metadata as one audited unit. V1 has no Secret Version
tombstone and no seven-day shortcut.

CredentialRotationRequest keyed replay metadata is not orphan staging and is
retained for the Request's complete audit/idempotency lifetime, including
`CAS_LOST`. Ordinary GC cannot remove it. An authorized retention procedure
may remove it only in the same storage-lock transaction that removes the
durable Request and after proving that no retained Receipt, Secret Version,
Attempt, backup manifest, or audit row references it; the procedure never
retains or reconstructs the submitted secret body.

Candidate-store reachability roots are Candidate bundle references and the
promoted-object reference of a currently `PROMOTED` upload. An
`artifact_objects` inventory row alone, a `CONSUMED` upload's audit digest, or
an `EXPIRED` upload is not a live root. Removing an orphan file and its
unreferenced inventory row is one audited GC operation under the lock.

An operator-requested purge of referenced terminal history requires a separate
audited feature and is not part of v1 automatic GC.

An `ACTIVE` Terminal Duplicate Cleanup Reservation is a GC root for its
Publication/Run, selected complete-search Observation and Search Members,
ordered cleanup Members, every Action generation, Reservation-bound Schedule/
Request, mutation Outbox, result Observation, and ownership/reliance proof.
Terminal-Run collection MUST NOT
delete or detach any part of that graph. After the Reservation is `COMPLETED`,
the same graph remains retained until both the Run/Publication audit-retention
policy and every referenced Observation/Outbox retention policy authorize one
locked, audited purge. No age-only or orphan rule applies to these rows.

## Crash and recovery matrix

| Crash or loss boundary | Durable observation after restart | Required recovery and result |
| --- | --- | --- |
| Before worker Activity/assignment/Attempt/outbox transaction commits | None of its rows exists | Reducer trigger remains unconsumed or is replayed; no work was scheduled |
| After Activity/assignment/`OFFERED` Attempt/outbox commit, before Redis append | All required rows and assignment/member digests exist | Dispatcher reconstructs the exact review slot when applicable and publishes the exact Attempt ID and generation |
| Redis append succeeds but response or outbox update is lost | Outbox appears due; Redis may already contain a row | Republish; duplicate notification cannot win a second claim |
| After outbox Redis metadata update | Activity remains authoritative | Continue; Redis entry ID is diagnostic only |
| Redis is flushed with queued offers | Current `OFFERED` Attempts and outbox rows remain | Increment the epoch and republish each current unexpired offer |
| Redis timer keys are flushed or controller restarts after a Wait, Health, Attempt, or Recovery deadline | Durable deadline source remains | Stable sweeper inserts/reuses the scope/deadline-unique Timer Fact, freezes global Health affected-Run membership, and reduces its canonical trigger path; wall clock or a missing Redis key alone changes nothing |
| EVIDENCE Wait selection races an accepted matching Forge Observation or crashes after the under-lock recheck | The writer transaction contains either the timer-plus-event Wait and its Transition, or no Wait plus exactly one successor Recovery Evidence sourced by the prior Evidence | Re-read exact target/kinds/minimum sequence/predicate under the same writer identity. Replay returns the committed branch; it never inserts a stale Wait after satisfying evidence, and the successor's own Recovery-Evidence Transition applies the deterministic retry/replacement tactic |
| Timer Fact commits but wake response/process is lost | Fact and any per-Run Transition or source-bound Terminal Fact are durable | Replay follows only the scope's closed owner path: direct Timer Transition for Wait/Health, no Run Transition for Budget Report expiry, Attempt Terminal Transition for claim/execution deadline, or the already-selected Recovery-Evidence Transition for eligibility. It never invents a generic timer reduction, clears a replacement Wait, or expires health/Budget/Attempt twice |
| Admission ADMIT transaction commits, then controller crashes before Snapshot install | Generation-0 ADMITTED Run, pending capture, ADMIT Transition, and projection intent exist; no Snapshot Generation or worker work exists | Resume the unique `SPEC_SUPERSEDE` by pending Snapshot ID; ADMIT replay never installs it |
| Initial SPEC_SUPERSEDE commits, then controller crashes before planning | Generation-1 Snapshot and its same-state SPEC_SUPERSEDE Transition exist; no PLAN work exists | Resume `T(INTERNAL, spec_transition_sequence)` and create one PLAN Activity; create its Attempt/outbox only if the durable mode/key gates permit, otherwise leave it PLANNED for the offer reconciler; never install twice |
| Health probe crashes before I/O, after I/O, or around completion commit | PENDING Request/outbox survives before completion; after commit the Request is COMPLETED and one reciprocal Fact/ordered Observation coexist | Reuse the exact Request/adapter identity before its not-after time and commit once; a raw callback, new untracked request, or conflicting response cannot insert health authority |
| Health probe completion commits, then the controller crashes after any prefix of affected-Run reduction | Fact/Observation, immutable bytewise-ordered `HealthProbeFactRun` membership/digest, and exact cursor survive | Resume only the member at `fanout_cursor_ordinal`; advance it in the same transaction as that member's unique Health-Observation Transition. A superseded or unrelated member receives a same-state audit transition, and no later Run joins the frozen fanout |
| Claim deadline passes before a worker wins | `OFFERED` Attempt and deadline remain | Insert/reuse the scoped `ATTEMPT_CLAIM_DEADLINE` Timer Fact, derive the source-bound `CLAIM_DEADLINE` Terminal Fact, expire the Attempt, set its Activity `PLANNED`, and append zero-counter Recovery Evidence/enter `RECOVERING`; only the later Evidence transition deterministically waits or offers `g + 1`, without advancing recovery counters |
| Two workers receive one notification | One `OFFERED` Attempt exists | One `OFFERED` to `CLAIMED` transaction wins; the other receives conflict |
| Claim commits but response is lost | Attempt, reciprocal AttemptClaim, worker/session, deadlines/auth grace, exact source descriptor, and capability/launch claims digests exist | Same authenticated session/Attempt Claim ID/request digest returns the stable contract and rematerializes only currently valid sensitive fields from the exact frozen identities; another key/session cannot claim |
| Claim transaction crashes before or after inserting its reciprocal rows | Before commit, Attempt remains `OFFERED` and no AttemptClaim exists; after commit, both sides and Activity `ACTIVE` coexist | Retry the exact claim safely or return the committed claim; a partial Attempt/AttemptClaim pointer is forbidden by the transaction and deferred exact-match guards |
| Redis is flushed after claim | `CLAIMED` Attempt survives without liveness | Do not requeue it; accept authenticated result or pool loss, or wait for its execution deadline |
| Worker dies with uncommitted edits | No Candidate was admitted | Fence on exact loss/deadline and retry from the last durable Candidate/base boundary |
| First Result reaches the writer at or after its execution deadline but before the sweeper | No Attempt Result exists; durable deadline is due | The same transaction inserts the `EXPIRED_CURRENT` Result Request and request-sourced `EXECUTION_DEADLINE` Terminal Fact, stores the rejection response, expires the still-current Attempt, and runs timeout recovery |
| A new late Result request arrives after a prior non-Result input terminalized the Attempt and no Attempt Result was accepted | Terminal Attempt and prior transition exist; this request key is unseen | Insert its `ALREADY_TERMINAL` ledger row and source-unique `RESULT_AFTER_TERMINAL` audit Fact with stored `409 ATTEMPT_STALE`; append exactly one same-state ATTEMPT_TERMINAL audit Transition and no Recovery Evidence, counter, or work change |
| Accepted Result response is lost and replay arrives after the deadline with a new idempotency key | One Result and its creating Result Request exist | Identical authenticated Attempt/session/signer/digest replay adds only an `ACCEPTED` Result Request with `accepted_result_created=false` and returns the committed result |
| Replacement generation starts, then an authenticated old Result arrives strictly before its own deadline | Activity current generation is newer | Insert/replay one `STALE_ATTEMPT/GENERATION_SUPERSEDED` ResultRequest with exact stored 409/current-generation response; admit no Result/artifact/Receipt/fact/recovery/Transition |
| Candidate upload stops before fsync | Incomplete `incoming` file only | Never reference it; remove after incoming grace |
| Upload content validation reaches its expiry | Fsynced incoming bytes may exist, but no `VALIDATED` authority committed | At equality or later the serialized validation transaction changes the unused upload to `EXPIRED` and returns exact HTTP 410 `orcest.candidate-upload-expired/1`; repeat PUT derives the same body, never validates those bytes, and locked grace/GC handles the file |
| `VALIDATED` upload becomes `EXPIRED` before promotion | Exact upload row and fsynced incoming path remain; no durable Candidate authority exists | After grace, collect only under the shared storage lock with a repeated no-reference/terminal-Attempt check, quarantine rename, and directory fsync |
| Candidate object is installed/fsynced before the `PROMOTED` transaction commits | `VALIDATED` upload and possible durable unreferenced object | Recheck strict upload expiry: before it, verify the exact no-clobber object and idempotently commit `PROMOTED`; at or after it, CAS the upload to `EXPIRED` and leave the object for orphan grace |
| `PROMOTED` commits before the Candidate/Result transaction, then upload expiry is reached | Durable upload-to-Artifact-Object boundary exists; no Candidate or Result exists | Under the shared lock, the serialized finalization/expiry CAS either consumes it strictly before expiry or changes it to `EXPIRED`, clears promoted live references, rejects first Result, and releases the object to orphan GC |
| First Candidate Result loses the upload-expiry race | Upload is atomically `EXPIRED`; no Candidate/Result/Receipt/Transition exists | Commit one global `UPLOAD_EXPIRED` Result Request with the same exact HTTP 410 body as content PUT; exact key/body replay returns it and conflicting global key reuse fails |
| Candidate DB transaction commits but response is lost | Candidate, accepted result, transition, and next outbox are present | Idempotent retry returns the committed Candidate; no second acceptance |
| Imported bundle is fsynced but its controller transaction has not committed | Durable unreferenced object and current or stale `IMPORT` Activity | Repeat all observation/head/specification/effect guards; admit once if still current, otherwise leave it collectible |
| Attempt rotation staging/version write is durable but its APPLIED transaction has not committed | No CredentialRotationRequest/Receipt/Version authority exists and the prior current version is unchanged | Verify only through the operation-bound keyed request attestation and retry the exact current-prior transaction before the execution deadline; otherwise quarantine as an orphan under the storage lock |
| Attempt rotation loses the prior-version CAS | One immutable `CAS_LOST` CredentialRotationRequest and stored HTTP 409 exist; there is no Receipt/Version/ref/fanout | Strictly before execution deadline, exact same request/keyed-byte proof replays the stored response; at/after it rotation authentication denies. Conflicting reuse fails and losing stage cleanup cannot create authority |
| Rotation reference/frozen-membership/fanout-intent transaction commits but acknowledgement or fanout process is lost | APPLIED Request, new live version, complete ordered affected-Run membership/digest, durable fanout cursor, reciprocal credential-rotation Receipt, and a possibly empty prefix of per-Run Transitions exist | Before the execution deadline, exact Request replay returns the stored response; independently resume the first member ordinal lacking its Transition. At/after the execution deadline the rotation endpoint denies even replay, while ordinary audit/fanout recovery still converges |
| Backup/restore runs with retained APPLIED or CAS_LOST Credential Rotation Requests | SQLite Request rows and protected keyed replay metadata are manifest-listed; no rotation body/stage is a backup root | Restore and verify the keyed metadata before enabling rotation replay; exact pre-deadline replay remains decidable without secret exposure or new version authority |
| Secret Provision protected staging succeeds but `PENDING` Operation acceptance does not commit | Operation-bound Secret Store stage may exist without SQLite authority | Do not install a Version; retry exact acceptance when the authenticated idempotency identity is available, otherwise retain through the registration/backup window and collect only after exclusive no-reference proof |
| Accepted Secret Provision is `PENDING` when controller/Redis/Secret Store client restarts | Operation, protected stage/proofs, checkpoint prefix, and source-tagged Outbox survive | Rebuild disposable due delivery from the durable Outbox, reverify the same Operation-bound stage, and append the next checkpoint without requesting bytes or creating another object |
| Secret Provision version install is durable but completion transaction or response is lost | The accepted `PENDING` Operation and exact installed object/proofs identify its immutable target Version; no SQLite creation authority may yet exist | Verify exact identity and CAS, then atomically append/reuse the sole install-success checkpoint, Receipt, Version/reference/frozen membership/fanout intent, `COMPLETED` projection, and stored non-secret response; never allocate a new version, and replay per-Run members separately |
| Secret Provision retry proves a terminal CAS/authority/staging/integrity failure, then crashes | The Operation remains PENDING or the atomic FAILED_TERMINAL checkpoint/REJECTED response/outbox delivery all exist | Replay the same proof; commit the closed rejection once with no Version/fanout, or return the stored rejection. Collect rejected staging only after locked terminal grace/rechecks |
| Backup/restore or GC runs while Secret Provision is `PENDING` | The complete backup includes its exact encrypted protected stage, keyed operation metadata, and checkpoint chain | Restore and resume the same retry root; age-based GC is forbidden, and orphan GC requires the repeated exclusive proof that no Operation/outbox/checkpoint/Receipt/Version/manifest references the stage |
| Backup/restore runs after terminal Secret restoration/provision staging was collected | Durable terminal Operation remains and its operation-bound keyed replay metadata is manifest-listed even though stage bytes are absent | Restore and verify the protected replay metadata before accepting endpoint replay; return the exact stored terminal result, never reconstruct staging or accept conflicting bytes |
| GC races retention purge of terminal Secret operation replay metadata | Retained Operation or atomic purge outcome is authoritative | Ordinary GC retains metadata; the authorized transaction removes durable Operation and protected replay metadata together, so no retained Operation becomes unreplayable |
| Startup `foreign_key_check` reports only missing Snapshot-referenced WorkflowBlob parents while `quick_check` passes | Child rows preserve exact digest/media-kind requirements; all workflow mutation is disabled | Under the exclusive writer/storage barrier restore only exact verified rows, rerun all database/blob audits inside and after the FULL transaction, commit normal restoration Fact/Health/fanout, and enable only when clean; any other violation remains global fail-closed |
| Live Secret Version, Candidate object, or Snapshot-referenced Workflow Blob is missing or corrupt but SQLite remains relationally sound | Exact references and affected scopes remain knowable | Suspend only affected Runs/Projects, search verified complete backups for that exact object, perform its kind-specific locked/transactional restore, and revalidate; never substitute or roll back |
| Restored object is installed/fsynced before its Storage Restoration Fact transaction | Exact live identity is readable, but no successful restoration fact/wake exists | Re-verify source manifest, object bytes/mode/path, principal, authorization, and unavailable-source identity under the lock; then commit the one fact and reducer effects |
| Storage Restoration Fact/wake or integrity-resolution transaction commits but response is lost | Source-unique Fact, Fact-sourced `RECOVERED` Health Observation, frozen affected-Run membership, exact Transitions, and any Human Resolution exist | Replay walks the same membership and returns those rows; it never creates another health fact, wakes, resolves, or restores twice |
| Accepted Storage Restoration Operation remains pending across restart, or its terminal staging cleanup crashes | The `PENDING` row and stage remain a retry root, or a `RESTORED`/`REJECTED` row and quarantined stage remain auditable | Retry only the pending exact operation; collect terminal staging only through the shared-lock, grace, row/state/path/reference rechecks while retaining Secret attestation metadata for replay |
| Exact live object is absent from every verified complete backup | Scoped Recovery Evidence proves exhausted sources | Keep the affected scope closed and enter `INTEGRITY_FAILURE` only after the autonomous ladder is exhausted; unrelated scopes continue |
| Controller stops with active workers | Claims and deadlines remain in SQLite | Restart without drain; accept still-valid results and sweep deadlines normally |
| Capability key is retired/revoked or a restore lacks a referenced verifier | Registry state/evidence and every signer FK remain durable | Retirement permits only exact prior-claim verification/rematerialization through expiry; revocation denies immediately; a missing/mismatched verifier keeps claim/launch/Result endpoints closed until exact backup restoration, never key substitution |
| Project registration crashes before or after its writer transaction | Before commit, no Operation, Project, or discovery Schedule exists; after a successful REGISTER commit, terminal Operation/response, Project revision 1/registration pointer, one ACTIVE revision-0 WORK_ITEM_DISCOVERY Schedule, and reciprocal Project/Operation Schedule pointers coexist | Repeat read-only validation before commit, or replay the stored same-principal/key response after it; never repair a partially registered Project by inventing a Schedule, and a stale expected registration revision returns 409 with no ledger or Project mutation |
| Authenticated Budget Report commit or HTTP response is lost | Report, complete frozen affected-Run membership/digest, cursor, and stored response all exist or none do | Exact ID/body replay returns the stored response; conflicting reuse fails. Resume fanout from the first member ordinal without its generation-independent BUDGET_REPORT Transition and never recompute membership |
| AVAILABLE Budget Report fanout crashes after any member prefix, or a member changes generation/state before its turn | Immutable ordered membership and cursor name the unprocessed suffix; prior member Transitions are durable | Replay returns prior member Transitions, writes the required current wake or stale-member same-state Transition exactly once, and advances one ordinal transactionally. A reset Timer alone never emits an offer |
| Forge read/search/poll fails after outbound I/O, or failure-Fact commit/acknowledgement is lost | The Request remains PENDING; its reserved attempt ordinal has either no Fact yet or one source-unique ForgeRequestFailureFact plus retry projection | Insert/replay the exact ordinal Fact under the Request-state CAS. Run-bound work reduces it once through FORGE_TRANSIENT; pre-admission/discovery/terminal cleanup retries directly. Success/supersession rejects a late Fact and no path fabricates an Observation |
| Forge adapter response arrives after its Request's schedule/target fence became stale | The response transaction marks the Request SUPERSEDED with empty membership and its reciprocal Outbox DELIVERED; no Observation exists | Exact response replay returns that terminal stale disposition; startup does not redeliver the consumed Outbox, while a still-ACTIVE Schedule may create a new fenced Request |
| Management command transaction commits but response is lost | Command, Transition, and optional Human Resolution share one committed identity | An authenticated identical replay returns the stored result; it never cancels or resolves twice |
| Change Request create may have succeeded, its response is lost/ambiguous, and cancellation arrives before Change Request observation | Stable create-request checkpoint plus immutable cancellation source and search-only cleanup Activity/outbox remain | Keep the Run nonterminal; terminally cancel only when exact matching `CHANGE_REQUEST_ABSENT` marker/ref/search-revision/nonexistence evidence commits, or supersede the search Activity and atomically plan an exact discovered-head cleanup |
| Cancellation commits while Publication is `CHANGE_REQUEST_OBSERVED`, before close begins | Run remains nonterminal with immutable cancellation source, fenced semantic work, and durable head-bound `CLOSE_PUBLICATION` Activity/outbox | Re-observe by stable marker/ref/Change Request/head/effect, close only if the Activity's immutable head still proves, and retain the active Run until closure observation |
| Cancellation discovers a Change Request or observes a new head while another cleanup phase is current | Old immutable cleanup Activity/outbox and ordered Forge Observation remain | Supersede the old Activity and atomically plan exactly one observation/head-bound replacement; never add the head to or dispatch a stale Activity |
| Owned Change Request close succeeds but controller response is lost | Cancellation intent and close Activity/outbox remain; external object may already be closed | Re-observe by stable identity, record exact closure, and converge to `CANCELLED`; never repeat close blindly or treat a concurrent merge as cancellation |
| Management command carries a stale Run Transition or Human Boundary fence | No accepted Management Command exists | Reject without lifecycle mutation and retain only the separate security audit fact |
| Authorized amended-spec Forge Observation/Snapshot commits while an old Attempt can still finish | Ordered observation and pending Snapshot exist; Human Boundary remains current | Fence or await the Attempt; do not insert the Forge-sourced Human Resolution before the safe boundary |
| Compound forge amendment transaction commits but response/reducer process is lost | Human Resolution, cleared Boundary, Snapshot Generation, `SPEC_SUPERSEDE`, Run generation, and planned work all exist | Replay by the Snapshot trigger returns the committed Resolution/Transition; never install or plan twice |
| SQLite commit succeeds but a forge call has not begun | Publication intent exists without observation | Forge protocol checks deterministic identity, then performs or discovers side effect |
| Forge side effect succeeds but recording it fails | External state may exist; Publication is incomplete | Reconcile by deterministic marker/identity before retry; never create blindly |
| Initial COMPLETE_MARKER_SEARCH response/checkpoint commits before linkage and acknowledgement is lost | Exact SEARCH_RESULT, both ordered LIVE/TERMINAL memberships, complete ownership unions/digests, full-set digest, checkpoint, Publication live-cardinality/retained/conditional-terminal projection, and its Transition coexist | Replay the same Request/checkpoint. A bytewise-lowest `TERMINAL/MERGED/POSITIVE` member wins at every live cardinality and terminalizes with a durable cleanup Reservation for every LIVE member. Otherwise any `INCOMPATIBLE` member routes ownership conflict and any `INCOMPLETE` member routes autonomous evidence/backoff. Only an all-`POSITIVE`, no-merged set reaches the ordinary branches: ZERO/no-terminal absence/create then a fresh search; ONE fresh exact-object read; MULTIPLE one cleanup plus a fresh search; or ZERO with positive CLOSED terminal selection. No branch chooses from arrival order or partial evidence |
| Positive merged terminal selection commits and the controller dies before or during duplicate cleanup | Publication association/terminal proof, COMPLETE checkpoint, terminal `MERGED` Transition, reciprocal Reservation, complete ordered LIVE-member copy, cursor, and zero or one current Action/outbox are one consistent graph | Keep the Run terminal and resume only the Reservation's durable `next_member_ordinal`. Never recreate semantic work, select another merge, release legacy exclusion, or delete the graph because the Run is terminal |
| Terminal duplicate CLOSE or DETACH response is lost or ambiguous | Current Action is `ACTIVE`; its frozen Effect/CAS/idempotency identity and outbox remain, and the external object may already have changed | Reconcile the same operation. Only the exact Action-bound `CHANGE_REQUEST_CLOSED` or `CHANGE_REQUEST_MARKER` Observation completes it; ambiguity alone emits no Transition and never issues a blind distinct mutation |
| Terminal duplicate member changes before CLOSE/DETACH, or a mismatch response commits | Exact mismatch Observation and terminal `SUPERSEDED` Action result survive | Its sole `FORGE_OBSERVATION` Transition starts a fresh exact read/search; that later Forge Transition either creates a higher Action generation with newly proven CAS fields or completes the member as `RETAINED_AUDIT`. It never mutates from stale evidence |
| Cleanup Action success or RECORD_ONLY commits and controller dies before the next ordinal | Terminal Action result and same-state `MERGED` Transition exist; Reservation cursor may still name that member until its continuation commits | Replay finds the Transition and invokes exactly one `INTERNAL` continuation, which atomically advances the cursor and creates the next Action/outbox or completes the Reservation. No member is skipped or processed twice |
| Backup, terminal-history GC, or legacy selection runs while terminal duplicate cleanup is active | Complete backup includes the Reservation graph; ACTIVE keeps every selected/member ID/ref and proof row live | Restore/resume from the exact cursor. GC removes nothing from the graph, and legacy excludes the selected ID/ref and every unresolved member until `CLOSED`, `MARKER_DETACHED`, or `RETAINED_AUDIT`; a still-valid marker remains independently excluded afterward |
| Controller restarts with `PUBLISH` `ACTIVE` | Immutable effect and highest checkpoint survive | Keep `PUBLISH` active and resume its next read/reconciliation suboperation; do not route restart alone to generic controller failure |
| `REQUEST_READY` commits but mutation response is ambiguous or lost | Stable suboperation request key and preconditions survive | Append/reuse `AMBIGUOUS`, reconcile by read using the same key, and advance only from an observation-backed checkpoint |
| Publication effect generation `g + 1` commits before a response for `g` | Both immutable effect rows exist; Publication points at `g + 1` | Record the old response as audit observation only; it cannot mutate Publication, Activity, or Run |
| Reconciliation reads/observations commit before its Fact transaction | Ordered Forge Observations exist; `RECONCILE` is still `ACTIVE` or has become stale | Revalidate every current binding; atomically write the one Fact and its reducer effects only when current, otherwise retain observations as audit evidence |
| Reconciliation Fact transaction commits but its response is lost | Fact, ordered observation membership, completed `RECONCILE`, Transition, and deterministic outputs all exist | Replay by Activity/fact digest returns the committed result; never attach, import, retry, or create an ownership boundary twice |
| Typed complete same-marker search and duplicate proof commit, then the controller crashes before or after planning cleanup | `CHANGE_REQUEST_SEARCH_RESULT`, `REDUNDANT_PUBLICATIONS_PROVEN`, its copied search revision/set digest, complete ordered cleanup members/digest, retained-lowest proof, and zero or one first-member `CLOSE_REDUNDANT_PUBLICATION` Activity/outbox are transactionally consistent | Replay the Fact transaction; before linkage keep `change_request_external_id` NULL and PUBLISH suspended at MULTIPLE, plan neither a second cleanup nor another member from the old proof, and perform no Publication Effect increment |
| Complete same-marker search yields no actionable duplicate and the Fact response is lost | `NO_ACTIONABLE_DUPLICATE`, exact search revision/set digest, completed RECONCILE, Transition, and Publication last-proof projection coexist | Replay that pair without cleanup or association change; only a later changed revision or digest can replan duplicate reconciliation |
| Marker repair call succeeds but its response is lost, or body/head/search changes before the call | Effect-bound `REPAIR_RUN_MARKER` Activity/outbox and exact observation/revision/marker-set/ownership proof survive | Reconcile the same operation on ambiguity. Cancellation, retained-head advance, merge, and closure win first. Otherwise persist the exact current observation and let its repair-specific Transition supersede the repair; a changed typed search may then plan duplicate `RECONCILE`, while individual evidence schedules a complete search or ownership reconciliation. The generic search row cannot plan while repair is current. Complete only from an exact controller-bound observation proving one desired marker; never transfer ownership or increment the Effect |
| Redundant-close adapter call succeeds but its response is lost or ambiguous | Exact cleanup Activity/outbox/operation digest and frozen duplicate head/proof remain; the external object may be closed | Reconcile the same stable operation. Only an authenticated matching `CHANGE_REQUEST_CLOSED` Observation with current Publication/effect and controller Activity/digest completes cleanup; never close blindly or synthesize success |
| Duplicate head, retained object, complete set, marker/ref equivalence, or unreviewed proof changes before close | Old cleanup Activity/outbox and ordered observations remain audit evidence | Persist the exact current Forge Observation evidence first; its `FORGE_OBSERVATION` Transition supersedes the cleanup without side effect. A changed complete search may plan fresh `RECONCILE`; individual evidence only schedules a fresh complete search. A definitive evidence-less failure uses a failed Controller Operation Fact; ambiguity/restart stays `ACTIVE` on the same operation |
| Exact redundant-close Observation commits but response/projection is lost | Cleanup Activity is successful; retained canonical Publication and Run remain nonterminal; a fresh authenticated complete search is durably scheduled | Replay the FORGE_OBSERVATION Transition without closing the Publication/Run, repeating the close, reusing the old Fact, or planning from the individual close; only a later changed typed complete search may plan `RECONCILE` and any later duplicate requires a newly frozen complete-set Fact |
| Pre-link import proof is stale, lacks a pinned-base relationship/reconstruction, or ownership changed | Foreign ref observation remains audit evidence; no valid importability Fact applies | Re-observe and reconstruct safely; do not admit/publish the head, and use ownership conflict only after positive incompatible authority evidence |
| Backup fails before completion marker | Incomplete backup directory | Ignore or remove it; last complete backup remains usable |
| Controller stops during Project Policy Update fan-out | Policy Update, immutable per-Run Work Item/base Observation pairs, and a prefix of source-unique result Snapshots exist | Use each persisted pair and create only missing captures; later Work Item or base observations cannot change replay input |
| Crash after GC quarantine rename | Object is quarantined and still unreferenced in the lock-held snapshot | Reacquire the shared lock, recheck live references, then restore or delete deterministically |

## Evidence and migration

The proposed store replaces several current Redis-authoritative workflow
mechanisms while retaining their useful safety lessons:

- `docs/wiki/current-orchestrator-state-model.md` documents today's contract:
  GitHub snapshots determine actionable PR work, while Redis loss may cause
  retries but must not suppress work. v1 retains stale-snapshot fencing for
  forge observations but moves Orcest-owned pre-PR workflow authority into
  SQLite.
- `src/orcest/shared/coordination.py` implements owner-checked Redis locks,
  expiring pending markers, attempt counters, and cooldowns. These remain valid
  disposable coordination patterns, but they cannot represent v1 durable
  Attempt generation or deadlines.
- `src/orcest/orchestrator/task_publisher.py` currently creates a Redis pending
  marker, increments a Redis attempt count, and then appends the task. It rolls
  those operations back on append failure, but has no transaction spanning the
  three writes. The SQLite Activity/`OFFERED` Attempt/outbox transaction
  replaces that planning sequence.
- `src/orcest/shared/models.py` currently serializes the GitHub token, Claude
  token, and generalized provider credential into the Redis Task payload.
  `tests/integration/test_task_flow.py` confirms Task payloads are the stream
  delivery unit. v1 removes those values from Redis and returns bounded
  credentials only after an authenticated claim.
- `src/orcest/shared/credential_handoff.py` stores a plaintext rotated
  credential in a private Redis checkpoint so a worker or reaper can finish a
  crash-safe handoff. `src/orcest/orchestrator/loop.py` stores rotated shared
  overrides in Redis. The local Secret Store and write-before-reference
  rotation protocol replace both as credential authority.
- `src/orcest/fleet/pool_manager.py::_coordinate_reaped_vm` currently rebuilds
  a lost task from its Redis pending-list entry, publishes a transient failure,
  then ACKs it. In v1 the pool manager reports exact Attempt ID, Activity,
  generation, and worker-session identity to the controller; only the
  controller writer fences the Attempt and schedules recovery.
- `src/orcest/shared/redis_client.py` and
  `tests/integration/test_mixed_provider_streams.py` demonstrate consumer
  groups, provider-specific streams, pending recovery, and duplicate-tolerant
  delivery. v1 retains those mechanics behind new protocol-versioned streams,
  while SQLite makes their loss recoverable.
- `src/orcest/monitor/db.py` provides useful SQLite precedent for WAL,
  relational idempotency, and read-only query connections. It does not set
  `synchronous=FULL`, enforce foreign keys, verify local storage, coordinate a
  single workflow writer, or cover artifacts and secrets; workflow v1 must add
  those requirements rather than reuse the monitor profile unchanged.
- `docker-compose.redis.yml` gives Redis a persistent AOF volume, but v1 must
  remain correct if that volume is empty. Current
  `src/orcest/fleet/deploy/docker-compose.yml` has no workflow state volume and
  runs one orchestrator container per project. Deployment must add the local
  control-state mount and consolidate workflow writes in the central
  controller.
- `src/orcest/fleet/config.py::save_config` and
  `src/orcest/fleet/orchestrator.py::write_project_files` already use temporary
  files, rename, and `0600` for credential-bearing configuration. They do not
  fsync the file and parent directory, so they are precedent for permissions,
  not an implementation of Secret Store durability.
- `src/orcest/fleet/config.py::load_config` and `save_config` currently read and
  write raw GitHub, Proxmox, monitor, Claude, and generalized provider
  credentials in `/etc/orcest/config.yaml`. Migration must durably import each
  value into the Secret Store before replacing its configuration use with a
  versioned Secret Reference; a missing imported version fails closed.

Implementation must add failure-injection tests at every row of the crash
matrix. Two deployment facts require an explicit pre-rollout validation rather
than an assumption: the chosen `/var/lib/orcest/control` Docker volume must be
proven to use local storage rather than the existing network trace mount, and
pool-manager loss callbacks must be routed to the one central writer instead
of publishing authoritative synthetic results directly to Redis.

Off-host backup transport and encryption-key custody remain operational inputs;
this page requires encryption but does not select an operator's backup product.
That choice cannot weaken the consistency barrier, manifest verification, or
restore drill defined above.
