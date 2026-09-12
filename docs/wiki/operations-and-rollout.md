# Operations and Rollout

Status: Accepted normative v1 specification (2026-08-27)
Target: workflow-control v1

## Scope

This page turns the workflow-control contracts into deployable, observable,
recoverable behavior. It defines the v1 topology, startup and shutdown order,
compatible upgrades, backup drills, failure injection, staged adoption, and
rollback. Storage transaction details remain canonical in
[Persistence and recovery](persistence-and-recovery.md); this page defines how
operators exercise and verify them.

## v1 deployment topology

One central controller process owns workflow state for every enabled project.
It is the only SQLite writer and the only actor with controller publication
authority. The process may use internal concurrency, but all durable mutations
pass through one serialized write boundary.

```text
forge events/polls ──> central controller ──> SQLite + outbox
                           │    │                    │
                           │    ├──> Candidate Store│
                           │    ├──> Secret Store   │
                           │    └──> forge effects  │
                           │                         v
                           └──────────────────────> Redis
                                                     │
                                        pool manager + worker VMs
```

The controller host MUST provide one local persistent state-root mount
containing SQLite/WAL, Candidate Store, and controller-only Secret Store
subdirectories. Candidate staging and final objects share this filesystem so
their admission rename is atomic; the shared storage-mutation lock and backup
barrier cover the whole root. Redis is a separate disposable instance or
service for queues, wakeups, liveness leases, and caches. Backup destinations
MUST be outside the primary state-root mount.

SQLite and its WAL MUST reside on local block storage, not NFS, SMB, a FUSE
mount, or a synchronously replicated shared filesystem. Candidate and secret
backups may be copied to remote storage only through the consistent backup
procedure.

Redis is not in the recovery set for Run correctness. Persistence MAY remain
enabled to improve restart time, but restoring an old Redis image must never
override SQLite. Startup always treats Redis as a projection to validate or
rebuild.

The pool manager is a separate authenticated client of the controller. It
manages worker capacity and reports VM loss, but cannot write workflow rows or
forge refs directly. Workers claim one Activity at a time through the
versioned protocol and are disposable after their result handoff.

The trusted runner shim has one narrower surface: HTTPS
`POST /api/v1/attempts/{attempt_id}/launch-attestations`. A registered runner-
shim principal plus the one-shot launch capability may submit or exactly
replay only the signed Attestation for that Attempt/session/nonce and receive
the scoped `orcest.launch-accepted/1` response. It grants no claim, Result,
workflow, source-write, or publication authority. Runner signing keys are
distinct from controller Capability Signing Keys, and the controller verifies
the runner principal/image/key/registration binding pinned in the installed
Snapshot before consuming the launch nonce.

Every controller API listener and client MUST use HTTPS with certificate
validation; plaintext HTTP is forbidden even on the private fleet network.
This includes worker claim/liveness/source/upload/Result/launch/rotation calls,
pool capacity/loss reports, Run management, storage restoration, secret
provisioning/adoption, and Project registration/onboarding. Endpoints that
carry raw Secret Store input additionally require the protected streaming and
no-log controls in the persistence contract; TLS is necessary but never permits
logging or buffering those bodies outside their protected store.

Legacy and v1 capacity are separate registered worker classes. Every Capacity
Pool, VM template, pool-manager principal, Worker Session, Redis ACL, and stream
namespace belongs to exactly one of `LEGACY` or `V1_CLONE_FIXED`.
`V1_CLONE_FIXED` is the v1 template class that implements Attempt-scoped clone
credential removal and the v1 claim/result protocol. No template or principal
may report capacity for both classes. Legacy PEL recovery is allowlisted only
for legacy streams/groups; v1 session loss is accepted only through the
authenticated exact-session worker-loss endpoint.

## Availability and isolation requirements

The central controller is a v1 single point of scheduling availability, not a
single point of durable data loss. Its restart must pause progress and then
resume from the persistent stores without operator reconstruction.

One project's malformed forge response, invalid repository policy, exhausted
provider, or failing projection MUST NOT stop polling, dispatch, result
acceptance, or publication reconciliation for another project. The scheduler
MUST use bounded per-project work batches and fair rotation. Adapter calls,
provider limits, and effect retries have project-scoped circuit breakers.

A failed SQLite `quick_check`, an unallowlisted foreign-key violation,
competing writer, or state-root filesystem failure disables all workflow
mutation. The sole repair mode for `foreign_key_check` failure requires
`quick_check` to pass and every violation to be exclusively a missing
WorkflowBlob parent whose exact digest/media kind remains frozen in its
Snapshot child reference. All workflow mutation stays disabled while the
exclusive storage reconciler restores only those exact rows from a verified
complete backup or accepted pending Restoration Operation, reruns all database
and Blob audits, and records normal restoration facts/fanout. Any other
violation remains global fail-closed. A missing live Candidate or Secret
Version, or an existing but corrupt Snapshot-referenced Workflow Blob, first invokes
verified kind-specific per-object backup restoration and, while SQLite
integrity permits, suspends only Runs/projects that reference that object.
Unrelated projects continue. No affected Run may dispatch, accept a result, or
publish from unverifiable state.

Every exact-object missing/corrupt decision begins with a durable Health Probe
Request and committed outbox before storage or Secret Store I/O. Only its
reciprocal Health Probe Fact/Health Observation may fence affected Runs or
start restoration. Startup scans may enqueue the Request, but cannot turn a
filesystem exception or raw probe return directly into lifecycle authority.

## Operational modes

The controller exposes explicit, durable operator modes:

| Mode | Intake | Offer planning/delivery/claim | Result acceptance | Forge reconciliation |
| --- | --- | --- | --- | --- |
| `RUNNING` | yes | yes | yes | yes |
| `INTAKE_PAUSED` | no new Runs | yes | yes | yes |
| `DISPATCH_PAUSED` | configurable admission only | no new offers, delivery, or claims | yes | yes |
| `DRAINING` | no | no new offers, delivery, or claims | yes for existing Attempts | yes |
| `MAINTENANCE` | no | no | exact read-only existing ResultRequest-key replay only; no first worker mutation | read-only unless the named recovery procedure permits an effect |

Ordinary Forge Observation Schedule creation/completion and its Outbox delivery
are permitted in the first four initialized modes, including read-only
reconciliation during `DISPATCH_PAUSED` and `DRAINING`. `MAINTENANCE` leaves a
due ACTIVE Schedule or PENDING Request durable without creating, delivering,
or completing ordinary work; the same identity resumes after maintenance.
Only an explicitly named maintenance/recovery procedure may perform its exact
allowlisted forge read.

For a first unseen worker Result in `MAINTENANCE`, the controller returns HTTP
`503` with the exact five-field body
`{"protocol":"orcest.error/1","code":"CONTROLLER_MAINTENANCE","retryable":true,"message":"controller is in maintenance mode","retry_after_seconds": 60}`.
The wire value is the literal integer `60`; it creates no Result Request or
workflow row. An already-ledgered exact key remains a read-only replay, and the
same key/body may be retried normally after maintenance while its capability is
still authentic.

The narrow authenticated Controller Mode, Capability Key, Secret Provision,
and Project Registration management surfaces remain separately available under
their own RBAC/idempotency contracts. In particular, bootstrap MAINTENANCE may
provision/adopt the capability-signing Secret before key REGISTER/SELECT and
may register Projects after installation Secrets exist; neither permission
opens Run mutation or ordinary Schedule I/O.

The Budget Report surface is a workflow-input surface, not a bootstrap
management exception. The first four non-maintenance modes may accept and fan
out a Report subject to their ordinary offer gates. `MAINTENANCE` permits only
exact read-only replay of an existing Report ledger row and leaves an
incomplete fanout cursor paused until authenticated mode exit.

Mode changes use HTTPS `POST /api/v1/controller-mode-operations`, protocol
`orcest.controller-mode-operation/1`, a caller UUID operation ID, and exact
`INITIALIZE`, `SET_MODE`, or `RESTORE_BACKUP` kind, expected revision/mode,
requested closed projection, and conditional verified-backup identity. The
durable
Controller Mode Operation ledger, not an in-memory admin flag, authenticates,
CASes, audits, and replays them. Same principal/ID/body returns its stored
terminal response; conflicting reuse is 409. A label or repository file cannot
select a mode. Restart loads the singleton before opening workflow endpoints
and preserves the last committed projection; it must not silently turn a drain
into normal intake.
The exact terminal response protocol is `orcest.controller-mode-result/1`:
both shapes contain `controller_mode_operation_id`, `status`, and transport-
only `replayed`, plus `operation_kind`; success adds `mode_revision`, `mode`,
and nullable
`dispatch_paused_intake_policy`, while rejection adds only the closed
`rejection_code`.

A new database contains only the singleton revision-0/null bootstrap row.
Before opening ordinary endpoints, the registered controller bootstrap service
principal commits the sole `INITIALIZE`, producing revision 1 `MAINTENANCE`
with no prior-mode fields. Reinitialization is rejected. `SET_MODE` is the only
ordinary initialized transition; entering maintenance freezes the exact prior
mode/policy and leaving clears it. An isolated verified restore uses
`RESTORE_BACKUP`: the registered storage-reconciler CASes the restored positive
revision/projection through one of three exhaustive branches. A MAINTENANCE
backup increments in place and copies its exact stored prior ancestry. An exact
DISPATCH_PAUSED/PAUSE_ADMISSION backup increments in that same safe projection.
Every other initialized backup is atomically installed at the next revision as
DISPATCH_PAUSED/PAUSE_ADMISSION under the restore barrier before any ordinary
endpoint opens. Only the first branch carries backup-prior fields; the latter
two have none. The verified manifest is required, and a later authenticated
`SET_MODE` is required to resume an operational mode.

`DISPATCH_PAUSED` and `DRAINING` do not create a new `OFFERED` Attempt or
publish an existing offer Outbox; they therefore cannot consume claim-deadline
time while dispatch is intentionally disabled. `DRAINING` does not cancel running Attempts. It waits until no Attempt is
claimed and no publication effect is in flight, or until an operator chooses a
separate explicit cancellation procedure. Hard deadlines and worker-loss
reconciliation continue during the drain.

A consistent backup uses a bounded backup sub-barrier, not an unbounded
`MAINTENANCE` pause over live workers. It freezes the prior durable mode
projection and selects one of three branches. If that projection is already
`MAINTENANCE`, backup remains in place, preserves the exact nullable
maintenance-prior ancestry, and never performs a temporary `SET_MODE` round
trip. If it is already exactly `DISPATCH_PAUSED/PAUSE_ADMISSION`, backup also
remains in place and submits no otherwise-invalid no-change mode Operation.
Either in-place branch still requires the serialized durable `CLAIMED` count
to reach zero; a nonzero count delays the backup and is not silently copied as
a restorable worker execution. From every other initialized projection, one
CAS-valid `SET_MODE` enters `DISPATCH_PAUSED/PAUSE_ADMISSION`, keeps Result
acceptance available, and drains the durable `CLAIMED` Attempt count to zero.
In one serialized writer boundary it then
rechecks zero claims plus the exact branch projection—unchanged MAINTENANCE,
unchanged exact DISPATCH_PAUSED/PAUSE_ADMISSION, or the committed third-branch
pause revision—before acquiring the storage
barrier. If either condition fails, it delays or skips the backup. The barrier
has a server-configured `backup_barrier_max_ms`; exceeding it aborts the
incomplete backup, releases the barrier, and creates no complete marker. Backup
never extends an execution deadline or returns a backup-specific retryable
response that could make a current Result cross its deadline.
After either success or abort in the third branch, one new Controller Mode
Operation atomically uses `SET_MODE` to restore the exact frozen prior
mode/intake policy only if the current revision
still equals the backup pause operation's result. An intervening operator mode
change wins and is never overwritten; the backup reports that it did not
restore. If the process crashes, it remains durably paused; retrying the restore
derives the exact prior projection from the committed pause Operation and uses
a fresh idempotent CAS Operation rather than guessing a process flag.
The restore-to-prior step is omitted for either in-place branch. An existing
`MAINTENANCE` row retains its stored prior projection byte-for-byte; an
existing exact `DISPATCH_PAUSED/PAUSE_ADMISSION` row remains intentionally
paused and does not create a no-op Operation or revision.

## Startup sequence

The controller MUST become ready in this order:

1. acquire the singleton writer lock and refuse startup if another writer owns
   the volume;
2. validate filesystem owner/mode, local-filesystem policy, available space,
   configured clock source, and controller identity;
3. open SQLite with the required pragmas, validate schema version and migration
   state, and run the startup integrity checks; if and only if the bounded
   missing-WorkflowBlob-parent exception matches, keep all workflow mutation
   disabled and finish its exclusive exact-row repair and post-repair checks;
4. load and validate the reciprocal durable Controller Mode and Capability Key
   Registry projections/last Operations before opening workflow endpoints;
   if the mode row is revision 0/null, run the one registered-service
   `INITIALIZE` to revision 1/MAINTENANCE before opening ordinary endpoints;
   enforce the stored mode, and when issuance selection is null or non-ACTIVE
   keep new-offer planning, delivery, and claims fail-closed;
5. reconcile accepted PENDING Candidate/restoration/provision/rotation staging
   and database reachability without treating an object read as health
   authority; do not inspect a live referenced object's integrity outside the
   request-first step below;
6. for every nonterminal Run's live Candidate, Snapshot-referenced Workflow
   Blob, and live Secret Reference, commit or resume its exact durable Health
   Probe Request/source Outbox before integrity I/O, then accept only the
   reciprocal Fact/Observation as missing/corrupt authority; complete exact-
   object restoration before allowing an affected Run to dispatch; verify the
   complete Capability Signing Key public registry, state evidence, retained
   references/expiry horizon, and matching private/public pairs before any
   claim, launch, or Result endpoint accepts authentication;
7. accept both canonical restartable `ADMITTED` projections: generation 0 with
   its first pending Snapshot resumes only `SPEC_SUPERSEDE`, while generation 1
   with that Snapshot installed resumes only its separate INTERNAL planning
   continuation; never combine or skip those transactions;
8. connect to Redis, create only protocol-compatible stream/group names, and
   rebuild all deliverable outbox work and disposable liveness/cache indexes;
9. start deadline, outbox, pool, publication, Forge Observation Schedule/
   Request, Secret Provision Operation/source-tagged generic outbox, and typed
   Health-Probe-Request/outbox reconcilers; they resume only durable pending
   pre-I/O identities. Health probes complete with exactly one reciprocal
   Fact/Observation and frozen Run membership, then resume its durable fanout
   cursor before treating the probe as fully delivered;
10. activate/resume the durable WORK_ITEM_DISCOVERY and per-target poll/search
   Schedules. Every scan/read uses its pre-I/O Forge Observation Request/outbox;
   discovery's complete revision/set digest may be empty, while every active
   Run's Work Item is still polled by stable ID regardless of labels/state and
   every linked nonterminal Change Request retains its required schedules; and
11. enable new intake last, unless the durable operational mode keeps it paused.

Liveness readiness is false until steps 1–9 finish. Intake readiness is a
separate signal that becomes true after step 10. A process can be alive and
serving diagnostics without being permitted to dispatch.

Startup reconciliation is idempotent. If interrupted, the next process repeats
it from the durable generation and outbox state.

## Shutdown and controller-only restart

A normal controller shutdown stops new intake and claims, waits for the current
SQLite transaction to commit or roll back, flushes audit output, and exits. It
does not mark active workers lost merely because the controller is unavailable.

A compatible controller-only restart does not require draining workers:

- already claimed Attempts remain durable with their generation and hard
  deadline;
- workers retry claim, heartbeat, upload, and result requests while the API is
  unavailable;
- Redis entries left unacknowledged remain redeliverable, while acknowledged
  claims are reconstructed from SQLite;
- after restart, liveness uses the protocol's grace and pool evidence before
  declaring loss; and
- a result is accepted at most once for its fenced generation, and an admitted
  Candidate remains durable.

An incompatible API, schema, artifact, secret, or reducer change requires the
release procedure's drain or dual-version compatibility window. The release
manifest MUST state which class applies.

## Redis loss and rebuild

When Redis becomes unavailable, the controller keeps SQLite authoritative. It
MAY plan Activities and commit outbox rows, but dispatch pauses. Workers retry
coordination calls and retain a finished result locally until the controller
accepts or definitively rejects it. No Redis error converts an Attempt to
success or failure by itself.

After Redis returns or is flushed, the controller:

1. increments and persists a Redis projection epoch;
2. recreates versioned streams, groups, scheduler indexes, and pool wakeups;
3. selects every current, unexpired `OFFERED` Attempt joined to its committed
   outbox row, including rows marked `DELIVERED` in a prior Redis epoch, and
   republishes that exact Attempt/outbox identity;
4. refuses to synthesize an offer from an Activity without its durable current
   Attempt and outbox pair;
5. recreates liveness entries only from fresh authenticated worker/pool
   evidence, never by assuming every durable claim is alive;
6. retains each durable claimed generation and hard deadline while applying a
   bounded recovery grace; and
7. validates queue counts against SQLite before declaring dispatch ready.

Old Redis PEL entries, delivery counts, locks, or completion markers do not
override the rebuild. Duplicated deliveries are expected and fenced by the
claim API. The Redis projection epoch is diagnostic and selects rebuild
namespaces; it is not a claim, heartbeat, upload, or Result fence. An
otherwise-current claimed Attempt remains valid across a controller restart
or epoch increment, and acceptance is decided from its durable Attempt,
session, generation, capability, and deadline bindings.

## Versioned rolling upgrades

Every deployed component reports build revision, schema range, queue protocol
range, controller API range, reducer version, registered capabilities, and
worker profile identity. The controller publishes a compatibility matrix in
its readiness output.

New Activity protocols use new physical stream names, including the major
protocol version. A consumer group is never reused across incompatible major
versions. An old worker therefore cannot receive a new envelope accidentally.
The controller MAY dual-publish only when one logical Activity has an explicit,
single-winner claim across both versions; otherwise it publishes to exactly one
compatible stream.

A rolling worker upgrade proceeds as follows:

1. deploy idle canary workers with the new image and protocol;
2. run synthetic claim, heartbeat, Candidate, receipt, loss, and credential
   isolation tests;
3. enable the revision for a bounded share of compatible Activities;
4. let old workers finish their claimed Attempts;
5. stop publishing old-protocol Activities; and
6. destroy old idle workers only after their stream and claim inventory is
   empty.

Controller API changes MUST accept the immediately preceding compatible worker
minor version for the published overlap window. A major break uses a drain and
new streams. Database migrations are forward-only and transactional where
SQLite permits. A migration requiring filesystem rewrites or table rebuilds
runs in `MAINTENANCE` from a verified backup and records a durable migration
journal.

## Backup and restore operations

The backup unit contains one barrier-consistent SQLite snapshot, including the
Controller Mode projection/Operation ledger, every
Candidate artifact and `VALIDATED`/`PROMOTED` upload referenced by that
snapshot, its SQLite-resident Workflow Blobs, the referenced Secret Store
versions, the complete Capability Signing Key public-verifier/state registry
plus Capability Key Operation/current-issuance projection and every referenced
private-signing Secret Version, every staging object for a `PENDING` Storage Restoration Operation,
the exact protected stage and checkpoint chain for every `PENDING` Secret
Provision Operation, and protected operation-bound keyed replay metadata for
every retained Secret-valued Storage Restoration and Secret Provision
Operation—including terminal `RESTORED`, `REJECTED`, and `COMPLETED` rows—and
protected request-bound keyed replay metadata for every retained `APPLIED` or
`CAS_LOST` Credential Rotation Request, the
normalized project registration/configuration needed to interpret it, and a
checksummed manifest. It also contains every Terminal Duplicate Cleanup
Reservation's selected complete-search Observation/Search Members, ordered
cleanup Members, Action generations, mutation Outboxes/result Observations,
ownership/reliance proofs, digests, and restart cursor. An `ACTIVE` Reservation
makes that complete graph a root even though the Run is terminal. Secret bytes,
pending Secret-operation staging, and all
Secret Operation/rotation-Request replay metadata are sealed in authenticated encrypted
envelopes. Terminal or orphan restoration/provision staging bytes and Redis are
deliberately excluded; retained terminal replay metadata is not.

The operator command MUST:

1. choose the same three branches as the persistence protocol: remain in place
   for `MAINTENANCE`; remain in place for exact
   `DISPATCH_PAUSED/PAUSE_ADMISSION`; otherwise CAS to that paused projection.
   Leave current Result acceptance enabled in the paused branches and wait for
   durable `CLAIMED` Attempt count zero; `MAINTENANCE` permits no first Result,
   so a nonzero count delays the backup. In every branch atomically recheck
   zero claims and the exact closed branch projection
   while establishing the persistence page's bounded backup barrier, without
   accepting new artifact or secret references across it;
2. use SQLite's supported online backup/snapshot mechanism rather than copying
   a live database and WAL independently;
3. copy or snapshot the exact manifest-listed artifact, pending restoration,
   pending Secret Provision, and Secret generations; include staging bytes only
   for snapshotted PENDING Operations, but include the protected operation-bound
   keyed replay metadata for every retained Secret-valued operation in any
   state and every retained Credential Rotation Request in either disposition,
   all only in protected encrypted form;
4. fsync the destination staging set, write its checksum manifest last, and
   atomically mark the set complete; and
5. release the barrier and report duration, bytes, oldest included Transition
   sequence, and whether the configured barrier bound was approached. On bound
   expiry, release immediately, leave staging incomplete, and report backup
   failure rather than delaying a worker Result or marking the backup complete.

Secret backups MUST be encrypted outside the controller host and retain
controller-only access. Monitoring and logs expose counts and versions, never
secret values.

Restore MUST install and verify every retained Secret-valued restoration/
provision Operation's protected keyed replay metadata before either management
endpoint accepts replay, and every retained Credential Rotation Request's
protected keyed replay metadata before the rotation endpoint accepts replay.
It restores stage bytes only for `PENDING` Operations;
terminal staging remains absent. Terminal keyed replay metadata may be deleted
only in the same authorized retention transaction that purges its durable
Operation or Request, never as ordinary staging GC.

A restore occurs on an isolated target. Before starting the controller it MUST
verify checksums, schema compatibility, SQLite integrity, every live Candidate
commit/bundle pair, every live SecretRef/version, every Capability Signing Key
public-key digest/state/referencing foreign key and retained private/public
pair, its registration Operation, registry revision/current-issuance/last-
operation reciprocal bindings, every Terminal Duplicate Cleanup Reservation's
selected search/member/digest graph and restart cursor, and filesystem
permissions. Claim, launch, and Result endpoints remain
disabled until that key-registry verification passes.
Redis starts empty and is rebuilt. Network forge writes remain disabled until a
read-only reconciliation report lists all external effects newer than or
different from the backup. The operator then either accepts reconciliation or
abandons the restore; the system never force-pushes a backup's old view.

At least quarterly, and before the first production rollout, an automated drill
MUST restore the latest backup to an isolated environment, rebuild empty Redis,
and replay reducer/conformance checks without external writes. Backup success
without a successful restore drill is not sufficient.

## Observability

Metrics and structured logs MUST expose stable IDs, project scope, protocol
version, and state/reason codes. They MUST NOT contain prompts, issue bodies,
Candidate content, raw forge payloads, tokens, capability URLs, or Secret Store
values by default.

Required metric families include:

- controller build/schema/reducer information and singleton-writer health;
- Runs by phase, wait reason, project, and age;
- Activities and Attempts by state, generation, profile, and outcome;
- oldest uncommitted and undispatched outbox age;
- Redis projection epoch, rebuild state, queue/PEL counts, and duplicate/fence
  rejection counts by protocol;
- Capacity Pools/Sessions by worker class and template, plus cross-class Redis,
  claim, capacity, loss, and synthetic-Result denial counts;
- Candidate admission latency, bytes, integrity failures, and missing-live
  reference count;
- verification/review receipt validity, consensus outcome, and remediation
  cycles;
- provider capacity/rate-limit wait and substitution counts without account
  secrets;
- authenticated capacity-report age/revision, durable Health Observation age,
  Timer Fact due lag, and duplicate/stale wake counts;
- authenticated Budget Report age/source sequence, normalized consumption and
  availability by non-secret Project/scope, wake-fanout cursor age, and blocked
  offer count; plus Forge Request Failure Fact kind/rate/retry age without raw
  response content;
- publication effect age, ambiguous responses, duplicate discoveries, and CAS
  conflicts;
- SecretRef rotation/reconciliation state and missing-live version count;
- backup age, pre-barrier drain time, barrier duration/aborts, manifest
  verification, restore-drill age, and GC candidates;
- Storage Restoration Fact outcome/source kind and affected-object counts,
  without object or secret content; and
- `needs-human` entries and resumptions by allowlisted reason.

Page immediately on any missing live Candidate, Snapshot-referenced Workflow
Blob, Secret Version, or accepted pending-restoration staging root; failed
SQLite integrity check; competing writer; publication ownership conflict; or
audit write failure. Alert on sustained outbox age, overdue hard deadlines,
Redis rebuild failure, stalled publication, provider-wide capacity loss,
backup/drill age, disk pressure, or one project monopolizing scheduler time.
Alerting a human does not change lifecycle state to `needs-human`.

Every transition, generation change, receipt acceptance/rejection,
publication effect, configuration generation, secret rotation, mode change,
and administrative action has a structured audit record and correlation ID.

## Capacity, retention, and maintenance

The scheduler enforces global, project, provider/profile, and Activity-kind
limits. A project with unavailable capacity waits without consuming all poll
or database writer time. Reserved review capacity SHOULD prevent implementation
work from starving quorum Activities.

Every Project registration names a server-owned budget policy with exact
accounting scope, normalized integer unit, positive limit, reset-window rule,
next-reset computation, Report-freshness bound, and registered reporting
principal. Current consumption enters only through the authenticated
`orcest.budget-report/1` ledger. `EXHAUSTED` blocks every new offer under that
policy; `AVAILABLE` freezes and wakes matching `WAITING/BUDGET` Runs through a
restartable cursor. A persisted reset Timer only requests a fresh Report and
never authorizes work. Policy expansion likewise needs a later authenticated
Report under the new installed policy. No budget path weakens verification or
consensus.

Project registration freezes immutable logical source-read and publication
Secret IDs plus the exact versions that prove registration provenance. It does
not make those versions mutable-current aliases. Each Claim resolves and
freezes the then-current source-read version (and its provider version where
applicable), while each Publication Effect freezes the then-current
publication version in the same writer transaction as its outbox. Rotation can
therefore affect only later Claims/Effects; replay and restore never follow a
mutable tag.

Forge Instance registration likewise stores one logical
`credential_secret_id` plus the positive Secret version that proves its
registration provenance, not a mutable versioned alias. Every later adapter
operation resolves and freezes the exact verified current version under the
Secret lock. A `FORGE_CONNECTIVITY` Health Probe Request, its Fact/Observation,
canonical subject bindings, scope hash, and evidence all copy that exact
Secret Reference; prior-version availability cannot authorize a new operation.

Disk-pressure response proceeds in safe order: stop new intake, compact
disposable logs/caches, run reference-safe GC, and then pause dispatch. The
controller MUST never delete a live/auditable Candidate, Snapshot-referenced
Workflow Blob, Secret Store version, or exact staging object/Secret Store
metadata for a `PENDING` Storage Restoration Operation to remain available.
It likewise MUST never delete a `PENDING` Secret Provision Operation's exact
protected stage, opaque staging receipt, keyed operation metadata, checkpoint
chain, or source-tagged generic Outbox. Protected operation-bound keyed replay metadata for
every retained Secret-valued restoration/provision Operation remains a backup
and GC root in all states; terminal staging bytes may expire, but terminal
replay metadata may leave only atomically with authorized durable Operation
retention purge.
Protected request-bound keyed replay metadata for every retained `APPLIED` or
`CAS_LOST` Credential Rotation Request follows the same rule and may leave
only atomically with authorized durable Request retention purge.
Every current or retained-record-referenced Secret Version remains a combined
row/bytes/keyed-metadata GC root; ordinary age-based maintenance cannot remove
it. Only an explicit authorized retention procedure after all referencing
workflow/audit rows and backup manifests have independently expired may remove
that unit under the storage lock. V1 deploys no Secret tombstone shortcut.
An `ACTIVE` Terminal Duplicate Cleanup Reservation and its complete relational
graph are never terminal-Run garbage: the selected/member IDs and deterministic
ref remain legacy-excluded, and backup/GC retains every proof, Action, Outbox,
and result Observation until the Reservation completes and Run/Publication
audit retention independently permits one locked purge.

Capability-signing key rotation registers a new `ACTIVE` ED25519 key/version,
verifies its public/private pair, and only then changes issuance selection.
Retiring the predecessor stops new claim-set issuance but permits equivalent
rematerialization/verification of already-issued exact claims and keeps its public verifier and
private Secret Reference backed up and available through every signed
capability's maximum expiry and every retained reference. Emergency revocation
is an authenticated, idempotent registry change that immediately denies all
authentication/replay under that key; it does not delete evidence or replace
Timer-Fact recovery. Unknown signer IDs, algorithm mismatches, missing retired
verifiers, and failed key-pair checks keep claim/launch/Result endpoints
fail-closed.
The sole expired-token use under a retired key is verification of exact frozen
launch claims to locate a retained accepted Attestation and return its
`EXPIRED`/null-material projection. Revocation denies that lookup; it never
becomes launch or Result authority.

All key changes use HTTPS `POST /api/v1/capability-key-operations` with
protocol `orcest.capability-key-operation/1` and a caller UUID operation ID.
The authenticated key administrator supplies exactly one closed
`REGISTER`/`SELECT`/`RETIRE`/`REVOKE` request plus expected registry revision
and expected current issuance key. The durable Operation ledger owns replay;
same ID/body returns its stored terminal response and conflicting reuse is
409. REGISTER proves the public/private pair before commit. Retiring the
selected key requires an ACTIVE replacement. Revoking the selected key may
select an ACTIVE replacement or atomically clear issuance selection and
fail-close new-offer planning, delivery, and claims until a later successful
SELECT. This Registry gate does not rewrite Controller Mode. Repository YAML,
workers, Redis, and process-local flags cannot invoke or emulate this API.
The exact terminal response protocol is
`orcest.capability-key-operation-result/1`: both shapes contain
`capability_key_operation_id`, `kind`, `status`, and transport-only `replayed`;
success adds `registry_revision` and nullable `current_issuance_key_id`, while
rejection adds only the closed `rejection_code`.

Periodic maintenance includes SQLite checkpoint health and integrity checks,
outbox reconciliation, Redis-from-SQLite inventory comparison, request-first
Health Probe validation of referenced Candidate/Workflow Blob/Secret objects,
Secret and terminal/orphan restoration-
and provision-staging scans, Credential Rotation Request replay-metadata
validation, Secret Provision retry reconciliation, forge full
reconciliation, Capability Signing Key verifier/reference/expiry audits, audit
export, Terminal Duplicate Cleanup Reservation cursor/action reconciliation,
backup, restore drill, and retention GC. A
pending restoration or Secret Provision operation is always a reachability
root, never an orphan-scan candidate. Each job is idempotent, lease-fenced, and
visible in metrics.

## Staged migration from the current system

### Stage 0: implementation dark launch

- Add persistent controller, Candidate, and Secret Store volumes and backup
  jobs.
- Deploy the central controller with intake, dispatch, and forge writes
  disabled. Commit the registered bootstrap service's sole Controller Mode
  `INITIALIZE` from revision 0/null to revision 1/MAINTENANCE; do not synthesize
  an initialized row. While still in bootstrap `MAINTENANCE`, use the protected
  provisioning/adoption path to create the controller-owned private-signing
  Secret Version and its real creation provenance; this named bootstrap
  control operation is permitted before ordinary workflow mutation opens.
  Only after that exact Secret Reference exists, REGISTER its matching public
  verifier and then SELECT it through two authenticated Capability Key
  Operations, while dispatch remains fail-closed between every step.
- Inventory the forge-API, source-read, publication, provider, and remaining
  controller Secret References required by every legacy installation/project,
  and verify the already-provisioned capability-signing reference/provenance,
  without deleting or rewriting the legacy fleet config. Before any Project
  registration, provision or adopt three distinct `FORGE_INSTALLATION`-owned
  logical Secrets for each canonical `installation_or_account_ref`, with exact
  purposes `FORGE_API`, `SOURCE_READ`, and `PUBLICATION`, plus each remaining
  required Secret Version. For pre-existing bytes, establish its
  controller-only keyed verifier and submit an
  explicitly operator-authorized `ADOPT_EXISTING`
  `orcest.secret-provision/1` operation. Require its real management creation
  Receipt and Version link; the installation owner, purpose, and account
  identity must match exactly and cannot be retagged after creation. Never
  fabricate worker rotation provenance or a placeholder Receipt.
- Only after those referenced Secret Versions exist, use a pre-existing
  registered budget-accounting service principal and install each Project's
  exact accounting-scope/unit/limit/reset/freshness policy objects. Project
  registration may reference only those existing server objects; it cannot
  bootstrap them or grant reporting authority.
- Only after those referenced Secret Versions and budget objects exist, use a
  pre-existing registered authority to submit one authenticated `REGISTER`
  `orcest.project-registration/1` operation for each project in the current
  fleet config. Its transaction creates Project revision 1, copies the logical
  source-read/publication Secret IDs plus exact registration-provenance
  versions resolved by that operation, and installs the
  reciprocal `registration_operation_id` pointer. The same transaction creates
  the Project's sole ACTIVE revision-0 `WORK_ITEM_DISCOVERY` Schedule due
  immediately and stores the reciprocal internal Schedule pointer on both the
  Project and successful Operation. Migration may not synthesize registration
  history, either pointer, or a later repair Schedule. The legacy fleet config
  remains untouched until cutover; ordinary discovery I/O still waits for a
  non-maintenance Controller Mode.
- Run schema, bundle-hash, forge-capability, and read-only reconciliation tests
  for every project.

Exit: the controller can inventory all current projects and restore its backup
without owning work.

### Stage 1: protocol and synthetic canary

- Register distinct Capacity Pools and immutable VM templates for every legacy
  worker class and every v1 `V1_CLONE_FIXED` Worker Profile. Use disjoint
  pool-manager principals, Worker Session registrations, Redis ACLs, stream
  namespaces, and consumer groups; no pool/template identity is dual-class.
- Deploy v1 worker and pool-manager clients on versioned streams only from the
  registered `V1_CLONE_FIXED` templates, and attest the clone-credential
  removal behavior before advertising capacity.
- Register the budget-accounting service principal for each pilot
  Project/scope, exercise authenticated cumulative `EXHAUSTED` and `AVAILABLE`
  Reports, and keep offers fail-closed when the Report is missing, stale, or
  policy-mismatched.
- Register each v1 template's trusted runner-shim principal, pool binding,
  immutable image digest, signing key/algorithm, and registration revision in
  the server-owned execution mapping pinned into Snapshot `POLICY_JSON`. Test
  one-shot fresh/no-parent Launch Attestation before enabling model-backed
  capacity; repository configuration cannot select or relax this mapping.
- Constrain the legacy PEL reaper to its explicit legacy stream/group allowlist.
  It may reassign or ACK legacy entries only. For v1, disable PEL-driven loss,
  synthetic Result publication, and pool-manager ACK authority; exact claimed
  session loss goes only through the authenticated v1 loss endpoint.
- Run synthetic Activities through claim, loss, Candidate admission,
  verification, review, and Redis rebuild with no repository publication.
- Verify old workers cannot read the new stream and v1 workers receive no raw
  forge/provider secret in Redis; a model-backed claim withholds provider
  material until its signed Launch Attestation commits.
- Inject a legacy worker/reaper against a v1 offer and pending entry. Prove its
  read/claim/ACK/delete/republish attempts are denied, any synthetic Result call
  is unauthorized, and no v1 Attempt, Result, Receipt, Terminal Fact, or
  Transition changes. Then prove the registered v1 principal can report the
  exact session loss only through `orcest.worker-loss/1`, producing
  `FAILED/WORKER_LOST` once.

Exit: protocol and failure tests pass on the production topology, pool/template
class inventory is disjoint, and no legacy PEL authority reaches a v1 stream.

### Stage 2: one-project pre-publication pilot

- Select one project, validate its `.orcest` bundle, and enable new admissions.
- Make the legacy issue path ignore that project's ready and working labels.
- Keep new publication disabled; complete pre-PR workflows against synthetic or
  operator-approved pilot Work Items and inspect approved Candidates.

Exit: restart, Redis flush, worker loss, review ordering, and policy pinning
tests preserve the pilot Run.

### Stage 3: controlled publication pilot

- Enable controller-only publication for the pilot.
- Apply the run marker and shared engine-ownership predicate. Before linkage or
  any merge/close observation can become terminal authority, complete the
  initial durable `COMPLETE_MARKER_SEARCH` Request and reduce its two ordered
  memberships. Cardinality counts only LIVE open/unmerged objects; TERMINAL
  closed/merged objects remain in the same full-set digest. Prove every member's
  closed ownership classification from exact create/effect/creator/ref/marker/
  desired-commit/head/body evidence and persist the resulting
  `ownership_status`, `proof_kind`, `ownership_proof_digest`,
  `external_reliance_digest`, and per-member `member_digest`. Exercise
  precedence before cardinality: the bytewise-lowest `POSITIVE` merged terminal
  wins at any live count and creates a restartable cleanup Reservation for all
  LIVE members; otherwise `INCOMPATIBLE` routes ownership conflict and
  `INCOMPLETE` routes autonomous evidence/backoff. Only an all-`POSITIVE`,
  no-merged set reaches the ordinary branches: zero live/no terminal runs
  absence/create then a fresh search, one live requires a fresh exact-object
  read, multiple live performs proof-bound cleanup one at a time with a fresh
  search after each, and zero live with positive closed terminals chooses the
  lowest stable ID without creating.
- Prove the legacy PR loop excludes every Change Request associated with a
  nonterminal v1 Publication by stable ID/ref before the first one is opened,
  independent of whether its body marker is present, missing, duplicated, or
  temporarily unreadable. After terminal merge, prove the selected ID/ref and
  every unresolved terminal-cleanup member remain excluded until their durable
  outcomes; a remaining valid marker continues to exclude independently.
- Exercise CI failure, requested changes, concurrent head advance, merge, and
  non-merge closure.

Exit: a live-v1 Publication's Change Request is reconciled through terminal
state without legacy action even if marker repair or post-terminal duplicate
cleanup is required, and no duplicate PR or stale overwrite exists. Any
unresolved duplicate or overwrite fails Stage 3.

### Stage 4: project expansion

- Enable projects in bounded cohorts.
- Stop and remove each corresponding per-project orchestrator only after its
  Redis legacy work and PR inventory not associated with any nonterminal v1
  Publication or ACTIVE Terminal Duplicate Cleanup Reservation are empty or
  explicitly retained under the legacy engine.
- Compare fairness, provider demand, backup size, SQLite latency, and pool
  capacity at each cohort gate.

Exit: all selected projects have exactly one engine for issue intake and every
open PR has an unambiguous owner.

### Stage 5: legacy retirement

- Freeze new legacy admissions, drain its tasks, archive compatibility
  evidence, and retain read-only tooling for unmarked historical PRs.
- Remove raw task credentials from new streams before removing the legacy
  parsers that redact them.
- Retire per-project controller stacks only after the central controller and
  backup/restore drills meet the agreed observation period.

## Rollback

Release rollback normally means deploying the previous compatible v1
controller and worker revisions against the same durable state. The release
manifest MUST prove backward schema and protocol compatibility before this is
offered.

Before any v1 Publication exists for a project, rollout rollback may stop new
admissions and leave or cancel its pre-publication Runs under the v1 controller.
Legacy intake can resume only after the ready/working projections and unique
ownership records are reconciled. Legacy code cannot resume an in-progress v1
Run from Redis.

After a v1 Publication exists, every Change Request associated with that
nonterminal Publication remains owned by a v1 controller until terminal.
Positive-merged terminalization extends that exclusion to the selected object
and every unresolved Reservation member until its cleanup outcome; a valid
remaining marker independently excludes afterward. Marker repair and terminal
cleanup are evidence maintenance, not ownership switches: rollback MUST deploy
the last compatible v1 release or hold that project paused, and MUST NOT hand
the object to the legacy loop merely because its marker is absent or malformed.
Restoring an old database snapshot after external publication is prohibited
unless read-only reconciliation first accounts for every newer external effect
and the persistence recovery procedure can roll forward. Prefer roll-forward
repair.

An irreversible database migration, lost controller publication credential,
or incompatible Candidate format blocks the rollout before activation unless
a tested converter and rollback path exists.

## Required failure-injection matrix

These cases are release-blocking automated or rehearsed tests. “Recover” below
always means using the same Run and configured acceptance policy.

| Injection point | Required observable result |
| --- | --- |
| controller dies after ADMIT capture or after first Snapshot installation | ADMIT consumes the eligible Work Item observation plus the greatest eligible pre-transaction Work-Item-target BASE_HEAD sequence as its immutable anchor, and leaves generation-0 ADMITTED with a pending Snapshot and no worker work; only `SPEC_SUPERSEDE` installs generation 1, then only its INTERNAL continuation creates PLAN. Restart resumes the missing unique step without reselecting the anchor, installing, or planning twice |
| one entering Transition has cancellation, a pending Snapshot, an unsatisfied dependency, panel-staffing work, and state-local work simultaneously eligible | its single INTERNAL continuation applies the closed precedence cancellation, then Snapshot, then dependency, then the latest coalesced panel pointer, then state-local work; recovery is never INTERNAL. It writes one Transition, and any chained step uses that new entering sequence, so crash/replay cannot combine or reorder branches |
| a differing Snapshot capture races a current claimed Result or fenced controller Activity | capture stores/replaces pending_snapshot_id plus true supersede_requested and its exact capture Transition. The current work may complete/fence, but emits no next semantic work; the global continuation installs only the latest pending Snapshot and clears flag/sequence, while coalescing back, cancellation, or terminalization clears the pair without a stale install |
| base/spec/policy capture reaches a safe boundary | the source Transition only captures/sets pending; exactly one separate `SPEC_SUPERSEDE` Transition installs it. ADMIT, FORGE_OBSERVATION, POLICY_UPDATE, and INTERNAL never install a Snapshot |
| controller dies after Activity/outbox commit | restart publishes the committed outbox row; no Activity is lost |
| controller dies after Redis publish but before dispatch acknowledgement | republish may duplicate delivery; one claim generation wins |
| controller dies before or after the Attempt/AttemptClaim reciprocal claim transaction, including response loss | before commit the offer remains claimable with no partial Claim; after commit the exact Claim ID, source descriptor, deadlines/auth expiry, capability/launch JTIs and claims digests survive. Same session/Claim ID/body returns the stable contract and only currently valid sensitive fields from the exact frozen identities; another key/session cannot claim |
| response timeout is injected for claim, launch, upload create/content, Result, rotation, capacity, loss, and liveness | every durable endpoint retries its exact declared identity/body within its authority window and converges to one outcome; content PUT switches to the closed expiry response once due; liveness alone sends the next higher sequence and creates no replay ledger |
| Result request UUID is reused across accepted, upload-expired, stale, and deadline branches | one global Result Request primary-key lookup wins; exact same Attempt/session/signer/body bindings replay its stored response, and cross-disposition or cross-Attempt reuse is `IDEMPOTENCY_CONFLICT` without a second Result/Fact/Transition |
| authenticated old-generation/claim/Run Result arrives strictly before its own deadline | one `STALE_ATTEMPT` Result Request stores the exact 409/current-generation response and closed stale reason; no Result, Terminal Fact, recovery input, or Transition is created, and exact replay is stable |
| capability-signing key is retired, revoked, absent after restore, or presented with another algorithm | retired verifier authenticates only otherwise-valid previously issued capabilities through exact expiry and issues none; revoked/unknown/mismatched keys deny immediately; restore keeps capability endpoints disabled until registry/public-key/private-pair/reference audits pass |
| selected capability key is emergency-revoked without a replacement | one authenticated Capability Key Operation clears the durable issuance pointer and pauses new-offer planning, delivery, and claims atomically without rewriting Controller Mode; existing deadline recovery remains durable, and only a later CAS-valid SELECT resumes issuance |
| a new store bootstraps Capability Signing Keys, or crashes between bootstrap operations | revision 0 has no key or last Operation; the authenticated REGISTER alone commits revision 1 plus one ACTIVE verifier and still leaves issuance null, so dispatch stays closed; only a separate CAS-valid SELECT advances the Registry and opens issuance. Replay returns either committed operation exactly and never combines REGISTER+SELECT or skips a revision |
| bootstrap capability-signing Secret provisioning/adoption crashes before key REGISTER | bootstrap MAINTENANCE permits only the narrow protected Secret Operation; REGISTER remains impossible until the real creation Receipt, exact Secret Version/current reference, private/public match, and durable provenance commit. Retry resumes that Operation, never synthesizes a signing Secret or combines provision/REGISTER/SELECT |
| retry, panel staffing, startup recovery, or ordinary planning becomes eligible while mode is DISPATCH_PAUSED, DRAINING, or MAINTENANCE, or while the Capability Registry has no selected existing ACTIVE issuance key | the Activity remains PLANNED and no OFFERED Attempt/outbox is created or delivered; when both gates later permit issuance, the offer reconciler creates only the one fenced pair (or all-or-none panel set) rather than backdating claim-deadline time |
| capability key operation response is lost or its expected registry revision races | exact operation ID/body replays the stored terminal result; a CAS loser changes neither key state nor issuance selection and cannot double-increment registry revision |
| Controller Mode operation response is lost, races another mode change, or controller restarts while drained/paused | same principal/operation/body replays the stored terminal result; CAS loser changes no projection, and startup enforces the durable mode before endpoints open |
| a new store opens, INITIALIZE is retried, or an ordinary mode operation arrives before initialization | only the registered bootstrap service may commit revision 0/null to revision 1/MAINTENANCE; exact replay is stable, a second initializer is ALREADY_INITIALIZED, and pre-initialization SET_MODE/RESTORE_BACKUP is NOT_INITIALIZED with no workflow endpoint opened |
| a verified backup is restored from any initialized mode, including MAINTENANCE | registered storage reconciliation commits RESTORE_BACKUP against the restored revision: MAINTENANCE stays in place with its prior ancestry, exact DISPATCH_PAUSED/PAUSE_ADMISSION stays in place, and every other mode is atomically installed as DISPATCH_PAUSED/PAUSE_ADMISSION before endpoints open; no branch resumes operational work automatically |
| backup begins while the controller is already MAINTENANCE | it takes the bounded storage snapshot in place, preserves the exact nullable maintenance-prior projection and any bootstrap-null ancestry, and performs no pause/resume SET_MODE round trip; repeated in-place backup/restore therefore never invents a nested prior mode |
| backup begins while the controller is already exactly DISPATCH_PAUSED/PAUSE_ADMISSION | it drains/rechecks claims and takes the bounded snapshot in place; it writes no NO_CHANGE mode Operation, does not increment the mode revision, and remains intentionally paused afterward |
| third-branch backup succeeds/fails or crashes after its newly committed dispatch pause | cleanup restores the exact prior mode only with a CAS against the pause revision; an intervening operator mode wins, while a crash remains safely paused until the idempotent restore is retried from the pause Operation; either in-place branch has no restore round trip |
| model-backed claim/Launch Attestation boundary is injected | claim contains no provider material and exposes the domain-separated launch claims digest; equivalent launch-bearer remint preserves that digest. Only a signed fresh/no-parent Attestation from the Snapshot-pinned runner principal/image/key/pool consumes the one-shot nonce and releases pinned provider material; nonce or workspace/context/invocation reuse and unattested Results fail closed |
| accepted Launch Attestation is replayed before deadline, after deadline, and after Attempt terminalization | exact replay always returns the accepted identities; only current/strictly-before returns `AVAILABLE` with rematerialized provider bytes and flat non-secret `provider.secret_id`/`provider.version`. Under the same runner/session, an ACTIVE or RETIRED retained verifier may use consumed/time-expired launch claims only as signature-equality proof—not authentication—for exact retained-Attestation lookup and `EXPIRED`/provider-null; REVOKED denies, and lookup cannot insert, rematerialize, or mutate |
| controller dies during artifact upload before admission | partial staging data is unreferenced and collectible; worker retries upload |
| controller dies after artifact fsync before Candidate row | orphan artifact is discovered; retry may adopt the same digest |
| controller dies after Candidate commit | the live artifact verifies and one reducer transition consumes the accepted result |
| Redis is flushed with queued work | queues rebuild from Activities/outbox; no completion is invented |
| Redis is flushed with running work | claim generation/deadline survive; disposable liveness is re-established or loss is fenced after grace |
| controller process crashes with an unexpired `CLAIMED` Attempt, then restarts on the same live state root | the exact Claim/session/deadline/signer survive and a still-valid Result is accepted normally; this is a crash/restart fixture, never a backup-restore fixture |
| Redis is flushed while a Wait Condition, Health Observation, Budget Report, Attempt, or Recovery Evidence deadline becomes due | startup scans the durable deadline and inserts exactly one scope/deadline-unique Timer Fact; reducer replay wakes/expires at most once without Redis, while Budget expiry has no Run Transition and only closes its future offer authority |
| one envelope reaches two workers | claim protocol selects one current Attempt; rejected worker stops and cannot submit |
| old-generation result arrives | API records a fenced rejection and does not alter Activity/Candidate state |
| first result arrives at/after execution deadline but before capability-auth expiry | API durably ledgers it and returns `410 EXECUTION_DEADLINE_EXCEEDED` when it expires the current claim or `409 ATTEMPT_STALE` when an earlier cause already terminalized it; the latter stores `RESULT_AFTER_TERMINAL` and appends one same-state audit Transition with no counters/evidence/work, while an identical replay of a result accepted before the deadline remains idempotent during the same auth grace |
| first unseen Result arrives in MAINTENANCE, including a semantic replay under another unused key | return exact HTTP 503 body `{"protocol":"orcest.error/1","code":"CONTROLLER_MAINTENANCE","retryable":true,"message":"controller is in maintenance mode","retry_after_seconds": 60}` and create no Result Request or workflow row; only the same exact already-ledgered global key remains read-only replay, and the unseen same key/body may be retried normally after maintenance only while its capability is still authentic |
| any Attempt-capability request arrives at/after its fixed authentication expiry | authentication fails and no request/replay/terminal/workflow ledger is inserted; durable Timer Fact reconciliation remains the timeout authority |
| non-Result Attempt endpoint is called during post-deadline auth grace | launch, liveness, source, Candidate/upload, and rotation calls deny and insert no authority; only Result late-rejection/exact accepted replay paths authenticate. Exact accepted Launch replay uses only its separate consumed launch-capability lookup and returns null material |
| Result payload union is fuzzed across Activity/outcome kinds | only the exact Candidate, receipt, structured plan/diagnosis, or failure variant is accepted; accepted failures persist the closed normalized class/code/retry/evidence mapping and required result digest; malformed/mixed fields and any credential-rotation binding get 4xx with the claim unchanged and no Result/Transition |
| liveness responses are lost, reordered, duplicated, or Redis is flushed | no durable request/idempotency ledger exists; a higher exact-session sequence establishes current disposable control, lower/duplicate updates never rewind it, and each response is freshly derived rather than replayed |
| an unclaimed offer reaches its claim deadline while capacity, provider rotation, mode, or issuance-key changes race | the one writer terminal transaction freezes highest-applicable unexpired Health rows, the exact current logical-provider Secret version, capacity disposition, Controller Mode revision/mode, Capability Registry revision/selected ACTIVE key, replacement-offer disposition, Timer/Terminal Facts, Attempt expiry, and the sole terminal Transition; it returns the Activity to `PLANNED` and appends zero-counter Recovery Evidence. Only a later Evidence Transition may create the higher-generation replacement or bound capacity Wait, and only compatible capacity plus permitted mode plus ACTIVE issuance creates that replacement. Later input cannot alter replay and no branch increments recovery counters |
| an accepted worker Result reports `INFRASTRUCTURE`, versus an authenticated pool manager reporting exact worker-session loss | the worker Result path terminalizes the Attempt as ordinary Result `FAILED` and appends `WORKER_LOST` Recovery Evidence sourced by the exact Attempt Result, with no `WORKER_LOST` Terminal Fact or pool-loss `terminal_reason`; the disjoint pool-manager path creates the `WORKER_LOST` Terminal Fact, sets `FAILED/WORKER_LOST`, and appends TerminalFact-sourced Recovery Evidence. Only the latter TerminalFact/`terminal_reason` path is controller-only |
| a no-capacity claim deadline occurs in a review/adjudication panel | after the terminal transaction proves no peer claim remains, it appends panel-scoped `STAFF_PANEL` Evidence; the later Evidence Transition atomically offers all still-current unfilled slots or creates one bound Wait. If another unfilled slot still has a `CLAIMED` peer, it expires only the due Attempt, leaves its Activity `PLANNED`, updates the coalesced staffing pointer, and creates no Recovery Evidence, `RECOVERING` state, Wait, or offer for that slot |
| worker is destroyed with uncommitted edits | authenticated exact-session loss sets the Attempt to `FAILED/WORKER_LOST`; retry starts from the last admitted Candidate |
| old and new workers coexist | disjoint Capacity Pools/templates, principals, ACLs, streams, and groups prevent an old worker consuming a v1 envelope |
| legacy worker/PEL reaper is injected against a v1 offer and pending entry | Redis read/claim/ACK/delete/republish is denied, synthetic Result submission is unauthorized, and no v1 workflow row changes; only the authenticated v1 exact-session loss endpoint may produce `FAILED/WORKER_LOST` |
| review receipts arrive in all permutations | the same receipt set produces the same Consensus Decision digest |
| remediation produces a new Candidate after one or more review/adjudication rounds | panel_round restarts at 1 for that Candidate; a new round for the same Candidate is exactly prior maximum plus one, and no Activity/Wait/receipt may mix Candidate-local rounds |
| reviewer/provider capacity disappears | configured substitutes are tried or Run waits; quorum is unchanged |
| multiple panel Results/Terminal Facts race while peer slots remain claimed | each accepted safe boundary atomically replaces the Run's one Candidate/round/kind/staffing-sequence pointer and discharges the older INTERNAL obligation; only the latest pointer evaluates after no peer remains claimed, creating all offers or one all-slot Wait, while a stale/filled panel clears it without work. This pointer or that Wait is the sole NO_CAP exception permitting unfilled PLANNED panel slots with no live offer |
| all compatible capacity disappears and later an authenticated pool report proves a compatible Worker Profile available | ordered Health Observation satisfies only matching current capacity waits; stale/duplicate report identity cannot wake twice or lower a gate |
| review/adjudication panel is planned with no complete legal staffing selection | all frozen slot Activities/assignments/subjects remain `PLANNED` with no offers; the actual planning Transition creates one durable CAPACITY Wait, and a later matching report offers slots only after complete staffing is possible |
| provider rate-limit Result supplies a past or excessively distant absolute retry time | acceptance remains fenced by the Attempt deadline; persisted recovery eligibility clamps deterministically to controller acceptance time through that time plus the Snapshot-pinned server maximum, and restart selects the same wake |
| EVIDENCE Wait selection races an accepted target Observation at, below, or above its minimum sequence | the writer re-reads the exact `orcest.evidence-wake/1` target, allowed kinds, minimum sequence, and Candidate/panel/dispute/generation/policy/head predicate before insert. Below-minimum or mismatched input is audit-only; an already-satisfying current Observation creates no Wait and instead appends the source-linked next Recovery Evidence selecting deterministic retry/replacement; otherwise the timer-plus-event Wait commits and later wake repeats the same predicate check |
| provider credential rotates after availability was observed for the prior version | old provider Health evidence cannot authorize a Claim/Launch for the new version; a new durable Health Probe Request/Fact/Observation binds the exact provider/account/Secret version before use |
| Capacity Report or Worker Loss Report response is lost | exact report replay returns identical object IDs/outcome with only transport `replayed` changed from false to true; response digest excludes exactly that field and no Observation/Terminal Fact/Transition is duplicated |
| forge/provider/integrity probe crashes before I/O, after I/O, or before response commit; response is duplicated/reordered/malformed or injected as a raw adapter callback | only a durable pre-I/O Health Probe Request/outbox may run; restart reuses that identity, and only its reciprocal schema-valid Fact may atomically complete it with one matrix-valid Observation. Exact replay returns it, conflicting content fails, and raw callbacks/free-form diagnostics create no health authority |
| forge polling/search crashes before I/O, after transient transport failure, after an ambiguous adapter response, during result commit, or success races FORGE_UNAVAILABLE Wait insertion | an ACTIVE Schedule and at most one PENDING `ForgeObservationRequest` with reciprocal source-tagged Outbox survive; every outbound attempt has a pre-I/O ordinal. TIMEOUT/RATE_LIMIT/UNAVAILABLE commits or replays one `ForgeRequestFailureFact`, retry boundary, and Run-bound FORGE_TRANSIENT reduction or direct pre-admission/cleanup retry; it fabricates no Observation. Restart reuses Request/idempotency/ordinal state. Under-lock Wait creation detects already-current success/connectivity and creates successor Evidence, not a stale Wait. Completion atomically stores only request-kind-allowed ordered Observations and marks both projections; ACTIVE or PAUSED may complete when immutable scope/last-request/prior fences match, while a response that loses a close/replacement/real stale fence atomically marks Request SUPERSEDED plus Outbox DELIVERED |
| an ordinary Forge Schedule becomes due or a PENDING response arrives in MAINTENANCE | due Schedule, Request, and Outbox projections remain unchanged and no I/O/completion/Observation occurs; the same identity resumes only after an authenticated non-maintenance mode permits it, while named recovery reads cannot advance the ordinary Schedule |
| successful initial Project REGISTER response is lost or controller crashes | the one writer transaction contains the terminal registration Operation/response, Project revision 1/pointer, and its ACTIVE revision-0 WORK_ITEM_DISCOVERY Schedule, or none of them | exact replay returns the stored registration result; no startup repair invents a missing schedule, and ordinary discovery waits for the current Controller Mode gate |
| Work Item discovery returns empty, repeats unchanged items, changes a set, or reuses one adapter revision with conflicting content | the completed Request freezes the domain-separated bytewise stable-ID set digest, including empty; unchanged immediately preceding Observations may be membership-coalesced without a new sequence/Transition, changed members allocate sequences in membership order, and same revision/different set fails integrity rather than mutating the Schedule projection |
| discovery adds/removes a Work Item, completes while its Project/Schedule is paused, or ADMIT races its pre-Run polls | completion creates/reuses Run-null Work-Item/base schedules before reads and closes absent-item schedules only without an active Run; a paused parent creates/retains paused children and only Project reactivation may activate them. ADMIT closes/supersedes the selected pre-Run schedules/Requests/outboxes and creates Run-bound revision-0 replacements atomically, so both identities are never active together |
| initial publication complete-marker search returns live and terminal members in any mix, or its response is lost | the PUBLICATION-purpose effect-readback Request and INITIAL Effect checkpoint atomically freeze the exact PUBLISH Activity/operation fence, complete revision, independently ordered LIVE and TERMINAL memberships, every member's create/effect/creator/ref/marker/desired-commit/head/body ownership proof and reliance digest, and one full-set digest; ZERO/ONE/MULTIPLE counts LIVE only. The bytewise-lowest `TERMINAL/MERGED/POSITIVE` member wins at every live count and terminalizes with a durable cleanup Reservation for all LIVE members. Without that stronger fact, `INCOMPATIBLE` routes ownership conflict and `INCOMPLETE` routes autonomous evidence/backoff. Only an all-`POSITIVE`, no-merged set reaches ZERO/no-terminal absence/create plus a fresh search, ONE plus fresh exact-object read, MULTIPLE one-at-a-time cleanup plus fresh search, or ZERO with lowest positive CLOSED selection. Exact Request/checkpoint replay never chooses from a partial set or arrival order |
| probe completion commits and fanout crashes after any affected-Run prefix | the Fact retains its bytewise ordered `HealthProbeFactRun` membership/digest and cursor; restart resumes the next ordinal only, advances with that Run's unique Transition, records same-state audit for a superseded/unrelated member, and never recomputes membership |
| the logical Forge credential rotates while a connectivity probe or adapter operation is pending | Forge Instance registration provenance remains immutable; each request freezes the exact verified current Secret version under the Secret lock, and its Request/Fact/Observation/scope/digests copy that version. Evidence for the prior version cannot authorize the later operation or be silently rebound |
| two Health Probe Requests freeze the same prior sequence for one scope and complete out of order | the first committed completion assigns prior-plus-one and atomically creates its Fact/Observation; the other CAS fails to `SUPERSEDED` with no Fact and may be replanned from the new highest sequence |
| capacity-report expiry is set before, equal to, just after, and beyond the acceptance-time TTL bounds | after auth/body validation one controller `accepted_at_ms` and positive configured maximum are frozen; only `accepted_at_ms < expires_at_ms <= accepted_at_ms + configured_max_ttl_ms` commits, regardless of caller `observed_at_ms` |
| Budget Report is missing/stale, reports exact-limit exhaustion, response is lost, AVAILABLE races WAIT_BUDGET insertion or fanout crashes/expires after any prefix, or reset Timer fires first | missing/stale/exhausted evidence creates no offer; exact-limit derives EXHAUSTED. Exact ID/body replay returns the stored response, conflicting reuse fails, and AVAILABLE membership/cursor resumes from the first missing generation-independent Transition without recomputation; an expired/current-mismatched member gets only its same-state audit. Under-lock Wait insertion observes a newer Report and creates successor Evidence instead of a stale Wait. A Timer only triggers report reconciliation or expiry and cannot wake/offer by itself |
| Candidate edits `.orcest` | active normalized policy/hash and reducer plan remain unchanged |
| Work Item spec changes | current work is superseded at a safe boundary; a new specification generation invalidates old approvals |
| pending Work Item change B and trusted base observation C exist when server Policy Update P starts, then controller crashes during per-Run fan-out | immutable Policy Update Composition freezes B, C, and P for every then-active Run; restart creates only missing B+C+P captures and never substitutes installed A or a later observation |
| policy-only Snapshot supersedes a Run with a retained Candidate | the new generation always enters `REPLANNING` with one Snapshot/Candidate-bound `REPLAN`, never directly `VERIFYING`; only a valid new Plan may reuse the Candidate without `BUILD`, and all Verification/review gates plus frozen review subjects derive from that new Plan/policy |
| active Work Item closes or loses both labels | stable-ID active-Run polling still records the mutation and applies cancellation/specification rules |
| dependency becomes unknown/open while an Attempt is claimed, then controller/Redis restarts | ordered same-state Transition and pending-dependency observation/set/Transition triple survive without hot-fencing the claim; the first safe boundary revalidates and enters the exact external-dependency recovery/wait path, while a newer satisfied observation may clear it |
| one target is observed `A -> B -> A` | three ordered observations exist; only consecutive identical polls may coalesce |
| base advances | configured rebase-before-publication, pin, or supersede-at-boundary policy runs against the observed immutable SHAs |
| trusted base alone advances under `SUPERSEDE_AT_BOUNDARY` after Publication is `ACTIVE` | accept and order the audit input, but create no Snapshot/pending pointer/SPEC_SUPERSEDE for that base-only change; later conflict/head feedback uses ordinary post-publication remediation under the installed Snapshot |
| specification text, workflow configuration, or effective policy changes after Publication is `ACTIVE` | capture/coalesce the normalized Snapshot and install it only at the ordinary safe boundary; the base-only cutoff cannot discard or defer these authority changes merely because their capture also freezes the latest trusted base |
| controller dies after publication ref creation | restart finds the deterministic ref and does not create another |
| controller dies after Change Request creation | restart reconciles the stable create identity, records no direct link from the create/search response, and completes a fresh `COMPLETE_MARKER_SEARCH`; only an all-positive/no-merged ONE-LIVE branch plus a fresh exact-object read may link, while a positive MERGED terminal wins at any live count and positive CLOSED terminal authority is selectable only with ZERO live, so neither terminal branch creates again |
| controller dies after Publication row update | reconciliation observes the same external identity and continues monitoring |
| PR head advances during remediation | stale CAS fails; external commit is not overwritten and all gates bind to the new Candidate |
| external actor advances the owned PR head during monitoring, gating, remediation, approval, publication, waiting, recovery, or a Human Boundary | merge/close/cancellation precedence is checked first; otherwise every old-head Activity/Attempt/gate/Wait/Boundary/effect is superseded, controller IMPORT provenance admits the exact observed commit, and the full gate reruns before any later update |
| base advances before the initial ref mutation | current initial effect is superseded before any write; default policy plans `REBASE`, reruns all gates, and a later approval creates a higher-generation `INITIAL` effect on the same Publication |
| base advances after provisional Change Request observation but before initial effect completion | the controller retains ownership of the same provisional ref/Change Request, supersedes the stale effect, rebases and reruns all gates, then uses a higher-generation `INITIAL` effect; the Publication is not marked `ACTIVE` under stale approval |
| a `PUBLISHING` SUPERSEDE_AT_BOUNDARY base mismatch commits its pending Snapshot and the controller restarts | the stale Activity/effect stays non-dispatchable and the Run remains in its durable pending-specification subphase; only `T(SPEC_SUPERSEDE,pending_snapshot_id)` may install/enter REPLANNING, so no higher Effect or resumed mutation can bypass the generation boundary |
| base advances again before that higher `INITIAL` effect's pre-read | the higher effect is superseded before any ref mutation while preserving the same provisional object/head; policy repeats exact rebase or Snapshot supersession/full gate, never bypassing the pre-read because a Change Request already exists |
| deterministic publication ref contains a foreign SHA before linkage | stale effect is superseded; ownership reconciliation either proves an earlier effect, safely imports and fully gates the exact commit, selects one evidence-bound base-rooted reconstruction when admission fails without incompatible ownership, or reaches the typed ownership boundary; a later higher-generation CAS never overwrites unverified state and no `PUBLISHING` deadlock occurs |
| pre-link foreign SHA is valid Git but fails the pinned-base relationship without incompatible ownership evidence | reconciliation never retries `IMPORT`; one evidence-bound `REMEDIATE` reconstruction produces an ordinary base-rooted Candidate and full gate, or its typed failure advances normal recovery |
| pre-link foreign SHA carries positive incompatible ownership evidence | reconciliation reaches only `PUBLICATION_OWNERSHIP_CONFLICT`; it never reconstructs or overwrites the ref |
| a changed typed complete `CHANGE_REQUEST_SEARCH_RESULT` contains no positive merged terminal, proves every member `POSITIVE`, and finds multiple LIVE open/unmerged Change Requests carrying the same valid Orcest Run/Publication marker | after the applicable controller-Activity exclusions, its Transition first supersedes/fences any current `RECONCILE`; one new duplicate `RECONCILE` may produce `REDUNDANT_PUBLICATIONS_PROVEN`, which copies the complete-search revision/full-set digest, freezes the bytewise-ID-ordered LIVE cleanup membership/digest, retains ordinal zero/the lowest LIVE stable ID, and plans at most the first CLOSE member as one `CLOSE_REDUNDANT_PUBLICATION`; no Publication Effect is inserted or incremented. Before linkage the PUBLISH remains suspended at MULTIPLE and the retained ID is not installed as the Publication association. A positive merged terminal takes the terminal-Reservation branch first; incompatible or incomplete ownership never reaches this Fact |
| a changed post-link typed complete `CHANGE_REQUEST_SEARCH_RESULT` finds zero or one actionable same-marker object | after the applicable controller-Activity exclusions, its Transition first supersedes/fences any current `RECONCILE`; one new duplicate `RECONCILE` may produce `NO_ACTIONABLE_DUPLICATE`, which copies the exact complete-search revision and canonical ordered-result digest (including empty), updates the Publication's last-duplicate proof projection, changes no retained association, and creates no cleanup Activity or Publication Effect; replay of the same pair cannot schedule another RECONCILE, while a later changed revision or digest may. This outcome is invalid for pre-link MULTIPLE |
| an individual discovery/head/feedback/merge/close observation other than `CHANGE_REQUEST_MARKER` arrives while duplicate reconciliation is in flight and no close/marker-repair controller Activity is current | it may supersede stale duplicate evidence and request a new complete read, but cannot assert a set digest or authorize either duplicate Fact kind; Marker observations use the marker-specific path, and only the next typed complete search result can prove the set |
| the associated owned Change Request has a missing or byte-identical duplicated Orcest marker | the marker-specific Transition first supersedes any current `RECONCILE`; only an exact ownership proof may then plan effect-fenced `REPAIR_RUN_MARKER`. Its `orcest.run-marker-repair/1` input/outbox commits before `repair_change_request_marker_if_exact_owned`, binds the exact head/body revision/current marker-set digest, and can normalize to one canonical marker without transferring ownership or incrementing the Effect |
| run-marker repair response is lost, the body/head changes, a complete search changes, or controller restarts | ambiguity/restart retains the same ACTIVE Activity/operation. Cancellation, retained-head advance, merge, and closure take precedence. After excluding them, a mismatch first persists the exact current observation and its marker-repair-specific Transition supersedes the repair; a changed typed complete search may then plan duplicate `RECONCILE`, while individual mutable evidence schedules a fresh complete search or ownership reconciliation. The generic changed-search row cannot plan while repair is current. Only an exact controller-bound marker observation proving one desired marker completes it; conflicting v1/legacy ownership can never be repaired away |
| a linked live-v1 Change Request loses, duplicates, or corrupts its body marker while the legacy selector runs | the legacy selector excludes every nonterminal Publication's stable Change Request ID and deterministic ref unconditionally, independent of marker parsing; after positive-merged terminalization it also excludes the selected ID/ref and every unresolved terminal-cleanup member until its outcome, with a surviving valid marker independently excluded afterward. V1 schedules marker proof/repair or terminal cleanup, and no legacy worker, reviewer, or closer acquires authority |
| a duplicate head, marker/ref equivalence, complete-set search revision, retained object, or unreviewed proof changes after cleanup planning | pre-call or adapter mismatch first persists the exact current Forge Observation evidence; its `FORGE_OBSERVATION` Transition supersedes the immutable cleanup Activity/outbox. A changed typed complete search may plan fresh `RECONCILE`; individual close/head/marker/review evidence only schedules a fresh complete search. A definitive evidence-less failure uses a failed Controller Operation Fact; ambiguity/restart leaves cleanup `ACTIVE` on the same operation, and `close_change_request_if_exact_unreviewed_duplicate` is never called from stale proof |
| redundant-close response is lost or ambiguous | the same durable Activity/operation identity is reconciled; the controller does not issue a blind new close, and only an exact authenticated `CHANGE_REQUEST_CLOSED` Observation bound to the Activity, operation digest, current Publication/effect, duplicate ID, and head may complete it |
| exact redundant-close Observation commits and the controller crashes before acknowledging it | the cleanup Activity is complete, the lowest-ID canonical Publication and Run remain nonterminal, no Publication Effect generation changed, and a fresh authenticated complete marker search is scheduled; replay neither closes the Run nor schedules another member from the stale Fact, and only a later changed typed search result may plan `RECONCILE` from a newly frozen complete set |
| a complete search proves a positive merged terminal while LIVE duplicates still exist | the same terminalizing transaction freezes the lowest positive merged ID/proof, completes/fences PUBLISH, sets Run `MERGED`, and creates one reciprocal Terminal Duplicate Cleanup Reservation containing every LIVE row in bytewise stable-ID order | semantic Run work stays terminal; cleanup survives restart and processes only the Reservation cursor. No LIVE row is installed as the Publication association and no new Publication Effect is created |
| terminal duplicate cleanup classifies a member as positive and reliance-free, positive with external reliance, or unsafe/incompatible/incomplete | immutable Member planning selects respectively `CLOSE`, CAS-safe `DETACH_MARKER`, or `RECORD_ONLY` with its bounded reason | CLOSE uses only the exact-owned close primitive; DETACH removes only this Run marker and preserves all other body/external work; RECORD_ONLY performs no forge mutation. Every selected/member ID/ref remains legacy-excluded until its terminal action outcome, with a surviving valid marker independently excluded afterward |
| terminal duplicate CLOSE/DETACH response is lost or its CAS evidence changes | the one current Action generation, existing Effect fence, stable operation identity/outbox, Reservation-bound polling/search Schedule/Request, and exact preimage survive; ambiguity leaves it `ACTIVE`, while a typed mismatch persists an exact Action-bound Observation | reconcile the same operation without a blind retry. Success completes only from the exact Action-bound close/marker Observation. Mismatch supersedes through its one Forge Transition, performs a fresh exact read/search, then creates a higher generation or `RETAINED_AUDIT` |
| controller crashes after one terminal cleanup outcome but before selecting the next member | terminal Action result and same-state `MERGED` Transition survive with the Reservation cursor | exactly one INTERNAL continuation advances the cursor and commits the next Action/outbox or Reservation completion. Backup/restore validates and resumes that graph; terminal-history GC cannot delete it while ACTIVE |
| stale Publication effect returns after a newer intent | effect-generation guard records it as stale and cannot advance Publication state |
| Attempt credential rotation dies before its APPLIED transaction | no Request/Receipt/Version authority exists; the old live version remains usable and exact protected keyed body proof permits only a strict-before-deadline retry or locked orphan cleanup |
| Attempt credential rotation loses the prior-version CAS | one `CAS_LOST` Request and exact stored 409 commit without Receipt/Version/ref/fanout; same key/body proof replays before deadline and conflicting bytes or binding cannot overwrite current material |
| Attempt credential rotation dies after APPLIED reference update or response loss | reciprocal Request/Receipt/Version/ref/fanout coexist; exact replay before deadline returns stored 200 without a second version, while at/after deadline the endpoint denies and autonomous fanout still resumes |
| each `needs-human` reason is injected | only the matching allowlisted boundary enters resumable waiting with a decision packet |
| Wait wake or Human Resolution commits, then controller crashes before recovery work is planned | the entry transaction already retained the exact recovery origin, trigger, exclusive Wait or Boundary/Resolution resume pointers, and one typed Recovery Evidence while planning no work; restart reduces only that Evidence and never guesses the prior state or invents a RECOVERING INTERNAL shortcut |
| panel planning cannot staff every frozen REVIEW slot or the sole ADJUDICATE slot | the planning Transition commits all Activities/Assignments/subjects plus one CAPACITY Wait and complete ordered Health/slot memberships, with no Attempt/outbox. A wake appends STAFF_PANEL Evidence whose resume pointer inherits that immutable Wait slot set and whose own membership freezes the new Health evidence; its separate transaction either offers every still-current slot atomically or none and creates a replacement Wait, so crash/replay cannot expose a partial panel |
| a Wait has both a durable timer and typed wake, or an EVIDENCE wait receives neither promptly | either branch may satisfy it after all bindings revalidate; the other replay becomes stale. EVIDENCE always has a bounded timer, so startup Timer Fact reconciliation cannot leave an indefinite event-only wait |
| Secret rotation reaches a wait's minimum version before the wait-creation transaction acquires the Secret lock | the reducer re-reads the verified current reference, creates no stale SECRET wait, and applies the permitted retry or next Recovery Evidence against that exact version |
| integrity probe proves a Candidate/Workflow Blob or Secret Version failure, then the controller crashes between lifecycle reductions | the HEALTH_OBSERVATION Transition enters RECOVERING and appends STORAGE or CREDENTIAL Recovery Evidence respectively; only the separate RECOVERY_EVIDENCE Transition creates the bound recovery Wait. Restart resumes the missing trigger without direct HEALTH-to-WAIT collapse or duplicate evidence |
| integrity probe instead proves the exact object available | the Health Observation keeps the Run RECOVERING and appends one exact-source Evidence selecting the ordinary origin-valid retry/resume tactic; it creates no storage/secret Wait and cannot be interpreted as a negative probe after restart |
| authorized forge specification amendment is observed while a specification-conflict Human Boundary is current, then controller crashes before the safe boundary | observation and pending Snapshot remain durable; no Human Resolution is claimed until the safe-boundary transaction can install that exact Snapshot |
| controller dies after compound specification-amendment safe-boundary commit but before response/projection | one `SPEC_SUPERSEDE` Transition keyed by Snapshot, one Forge-authenticated Human Resolution, one installed generation, and one `REPLAN` plan replay idempotently; the cleared Boundary is not resolved twice |
| routine timeout/capacity/test/review failures repeat | recovery ladder continues to wait, replace, diagnose, replan, or adjudicate; no human reason is manufactured |
| the same verification failure reaches `maxRepairCyclesBeforeDiagnosis` | the accepted FAIL Receipt and one source-bound `VERIFICATION_FAILURE` Recovery Evidence commit with the post-application counters and `DIAGNOSE`; no new remediation is planned, and restart reduces only that Evidence |
| the same consensus blocker or sustained/new adjudication blocker reaches `maxRepairCyclesBeforeDiagnosis` | the Decision or Receipt and one source-bound `REVIEW_DISAGREEMENT` Recovery Evidence commit with `DIAGNOSE`; no remediation Activity is created before the separate Evidence Transition |
| GC races admission or backup | the shared storage-mutation lock and backup manifest references preserve every live or manifest-listed object |
| live Candidate, Secret Version, or Workflow Blob becomes missing or corrupt | affected Runs suspend; verified kind-specific exact-object backup restoration—including SQLite-row repair for a Workflow Blob—is attempted before exceptional integrity escalation |
| startup `foreign_key_check` finds only missing Snapshot-referenced WorkflowBlob parents while `quick_check` passes | all workflow mutation remains disabled; inject a verified exact-row repair under the exclusive writer/storage barrier, require clean repeated database/blob audits plus normal restoration Fact/fanout before enabling; any additional violation stays global fail-closed |
| all configured backups lack a live object, then an authorized operator supplies the exact bytes through the restoration operation | staged bytes are verified against the live object and its kind-specific integrity proof—SHA-256 for Candidate/Workflow Blob or the Secret Store's controller-only keyed attestation; one Storage Restoration Fact and matching Human Resolution wake the affected Run without exposing a secret or substituting another object |
| provisional Change Request cancellation races a merge | durable cancellation intent fences new work; exact owned-close observation yields `CANCELLED`, merge observation yields `MERGED`; terminal outcome is never rewritten and the live-v1 Publication object never enters legacy ownership, regardless of marker state |
| cancellation arrives before any Change Request authority exists | immediate terminal cancellation is allowed only when no Publication/create workflow exists at all, or when a current exact CHANGE_REQUEST_SEARCH/OBSERVED_ABSENT proof exists and no CREATE REQUEST_READY/AMBIGUOUS checkpoint makes an object possible; every other branch persists cancellation and reconciles the stable create/search identity without orphaning an object |
| cancellation completes after current reconciliation proves no open owned Change Request | the deterministic publication ref remains a reserved terminal audit artifact bound to the cancelled Publication; v1 neither deletes it nor permits a later Run to adopt or publish through it |
| Change Request creation succeeds externally, its response is lost, and cancellation arrives before `CHANGE_REQUEST_OBSERVED` commits | durable `CHANGE_REQUEST_CREATE/REQUEST_READY` or `AMBIGUOUS` evidence prevents immediate terminal cancellation; reconciliation finds and safely closes the exact owned object or requires an exact `CHANGE_REQUEST_ABSENT` observation binding repository/ref/Run-marker/search revision/nonexistence token before `CANCELLED`, so no open Change Request is orphaned |
| cancellation search discovers a Change Request, then its head moves before close | the search-only cleanup is superseded by an exact head-bound cleanup; head movement supersedes that Activity and plans one replacement, so no Activity input mutates and only the current head fence may close |
| current-head feedback contains both merge conflict and failing checks/review threads, then facts arrive in another order | conflict always selects one exact-head-fenced `REBASE`; without conflict the canonical failing-check/review/thread set selects one `PR_REMEDIATE`; arrival order and prose never select an Activity kind |
| `VALIDATED` Candidate upload expires before Result finalization | upload becomes `EXPIRED`; incoming bytes are removed under the shared storage lock after the declared grace and cannot become a Candidate |
| upload expiry races validation, promotion, or first Candidate Result finalization | inject controller time immediately before, equal to, and after `expires_at_ms`; only strictly-before transactions advance, equality/later atomically leave the unused upload `EXPIRED`, a promoted live reference is cleared under the storage lock, and no Candidate/Result is created |
| content PUT and first Candidate Result independently hit the same upload expiry | both return byte-equivalent HTTP 410 `orcest.candidate-upload-expired/1` containing exact upload ID/state/code/expiry; PUT derives it from the upload row, while Result durably replays it through one `UPLOAD_EXPIRED` Result Request |
| backup is requested while a claimed Attempt is submitting near its execution deadline | controller remains in pre-barrier `DISPATCH_PAUSED`, accepts the valid Result normally, and acquires the bounded backup barrier only after durable claimed count is zero; backup never causes `EXECUTION_DEADLINE_EXCEEDED` |
| backup barrier exceeds `backup_barrier_max_ms` | incomplete staging remains unmarked and collectible; barrier releases and workflow service resumes without changing an Attempt deadline or result |
| backup and restore run while a `StorageRestorationOperation` is `PENDING` | the exact staged object and, for a Secret, its operation-bound keyed metadata are present only in the authenticated encrypted backup unit; restore recreates the same pending operation and deterministic restoration/rejection replay without inventing a second Fact |
| backup/restore runs with retained terminal Secret restoration/provision Operations whose staging was collected | no terminal stage bytes are copied, but every operation-bound keyed replay record is present in its authenticated encrypted envelope and restored before replay acceptance; exact terminal replay remains decidable |
| backup/restore runs with retained APPLIED or CAS_LOST Credential Rotation Requests | no rotation body/stage is copied, but each request-bound keyed replay record is encrypted, manifest-listed, restored, and verified before endpoint replay; retained Request replay remains decidable without exposing bytes |
| terminal Secret operation replay metadata cleanup races backup or Operation retention | ordinary GC cannot remove it; only the authorized transaction that purges the durable Operation may remove the metadata, so no retained Operation loses conflict/replay evidence |
| controller or Secret Store fails at each Secret Provision checkpoint, including after durable staging and after version install but before SQLite completion | the accepted `PENDING` Operation, exact protected stage/opaque metadata, checkpoint chain, and source-tagged generic Outbox survive; restart verifies and resumes the same Operation without client resubmission, duplicate version, or fabricated Receipt |
| controller crashes after a Secret Version commit or after any prefix of its affected-Run fanout | Receipt, Version/current-reference CAS, frozen ordered membership/digest, and durable fanout cursor survive; restart resumes the first missing ordinal in independent idempotent transactions, including a same-state Transition for a member whose Wait/Boundary became stale |
| Secret Provision 202 response is lost while asynchronous completion or rejection races retry | while pending, replay returns byte-identical `orcest.secret-provision-accepted/1` with only Operation ID, `PENDING`, Secret ID, and target version; otherwise it returns the stored closed `COMPLETED` or `REJECTED` result body/status. No variant adds `replayed`, prior/current/mode/retry, or fields from another variant |
| Secret Provision retry discovers lost CAS, revoked authority, invalid staging, or an integrity conflict | one closed `FAILED_TERMINAL` checkpoint atomically yields stored `REJECTED` response and completes the same source-tagged generic Outbox; it creates no Version/fanout, exact replay remains stable, and locked grace GC may later collect only the rejected stage |
| a corrected Secret Provision follows a terminally rejected Operation for the same prior version | the rejected Operation remains audit/replay evidence but no longer reserves its target; only after locked proof of no Version, no live target reservation, no installed target, and safe quarantine of old staging may the corrected Operation reserve and install the same prior-plus-one version |
| backup and restore run while a `SecretProvisionOperation` is `PENDING` | secret bytes and keyed operation metadata occur only in the authenticated encrypted envelope; restore proves the exact stage/checkpoints/source-tagged Outbox, rebuilds only disposable delivery, and completes or retries the same Operation ID |
| orphan Secret Provision staging races acceptance or GC | the shared storage lock and registration idempotency/backup window prevent deletion of accepted staging; only a repeated exclusive no-Operation/outbox/checkpoint/Receipt/Version/manifest proof permits quarantine and later deletion |
| backup is restored with empty Redis | integrity checks pass, queues rebuild, and forge effects remain disabled until reconciliation |
| accepted Management Command response is lost | exact Command, Transition/Resolution, HTTP 200, and `orcest.management-result/1` body/digest coexist; replay changes only the non-digested `replayed` projection and never applies the command twice |
| Storage Restoration 202 is lost while reconciliation reaches RESTORED or REJECTED | retry returns either the deterministic closed pending 202 projection or the stored closed terminal body/status; rejection-code mappings are exact and no GET, second stage, or second Fact is required |
| Project registration accepted business rejection response is lost | same principal/key/body returns the exact stored `orcest.project-registration-result/1` status/body with only transport `replayed` changed; its public response digest excludes replayed and every internal Secret/forge resolution, while the separate resolution digest binds those internals; ownership conflict is 409 and the other closed business rejections are 422, with no partial Project |
| any controller endpoint is attempted over plaintext HTTP, including onboarding or a raw-secret multipart call | transport is rejected before authentication/body acceptance and no ledger or staging authority is created; HTTPS certificate validation and secret no-log streaming are exercised end to end |

The test harness MUST assert durable rows, generations, artifact/secret
existence, queue projection, external object count and SHA, audit events, and
the next reducer output—not merely process exit status.

## Release acceptance checklist

A workflow-control release cannot advance a rollout stage until:

- schema migration and downgrade/roll-forward strategy are recorded;
- a fresh consistent backup and most recent restore drill pass;
- compatibility matrix and stream isolation pass on deployed images;
- all required failure-injection cases affected by the release pass;
- no live Candidate, Snapshot-referenced Workflow Blob, or Secret Reference
  integrity error exists;
- Redis rebuild matches SQLite deliverable inventory;
- each enabled Project/scope has a fresh authenticated Budget Report whose
  derived availability agrees with the normalized cumulative integers, and
  reset-Timer/fanout replay tests prove that no mutable counter authorizes an
  offer;
- engine ownership audit finds no Change Request associated with a live v1
  Publication—or selected/unresolved by an ACTIVE terminal cleanup
  Reservation—eligible for the legacy loop, regardless of marker state;
- canary Runs reach expected deterministic decisions under receipt reordering;
- observability and paging paths are tested; and
- rollback commands, identities, and previous artifacts are captured before
  enabling intake or forge writes.

## Evidence and migration

Current evidence:

- `README.md` and `src/orcest/fleet/deploy/docker-compose.yml` describe one
  orchestrator Compose project per repository, with raw credentials passed as
  container environment variables.
- `src/orcest/fleet/deploy/docker-compose.redis.yml` uses Redis AOF and a named
  volume because Redis currently owns queue and much coordination state.
- `src/orcest/fleet/deploy/docker-compose.pool.yml` runs one pool manager that
  manages ephemeral workers against the shared Redis service.
- `src/orcest/fleet/orchestrator.py` deploys and stops per-project stacks.
- `src/orcest/fleet/pool_manager.py` recovers reaped worker PEL entries and
  reports transient results in the current protocol.
- `docs/operations/reliability-milestone-rollout.md` and
  `docs/operations/pve-test-mixed-provider-rehearsal.md` provide existing drain,
  image attestation, Redis backup, health gate, and rollback practices.

The v1 rollout retains ephemeral one-task workers, pool image attestation,
drain gates, provider heterogeneity, head-SHA fencing, and rehearsed rollback.
It replaces per-project workflow owners and Redis-as-recovery-authority with a
central single-writer controller and a database/Candidate/Workflow-Blob/Secret/
pending-restoration recovery set.

Implementation must add the controller service and persistent mounts, protocol
compatibility/readiness endpoints, operational modes,
SQLite/Candidate/Workflow-Blob/Secret/pending-restoration backup tooling,
Redis rebuild tooling,
project fairness, new metrics/alerts,
engine ownership audit, failure-injection harness, and staged enablement flags.
Exact host paths, systemd/Compose layout, backup destination, service account,
and alert thresholds remain deployment choices, but they must satisfy the
contracts above and be recorded in the environment runbook.
