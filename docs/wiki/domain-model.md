# Workflow-Control Domain Model

> **Status:** Accepted normative v1 specification (2026-08-27)
>
> **Canonical owner:** workflow identities, durable object schemas,
> relationships, uniqueness, immutability, and cross-page invariants.

This page defines the terms and identities shared by every v1 protocol. The
[architecture](architecture.md) owns component authority, and the [workflow
lifecycle](workflow-lifecycle.md) owns legal state transitions.

The requirements on this page are normative for workflow-control v1.

## Representation conventions

- Controller-generated opaque identifiers MUST be lowercase canonical UUIDv4
  strings. UUID generation is nondeterministic, but after persistence the value
  is an immutable input; reducer behavior MUST NOT depend on UUID ordering.
- External identifiers MUST be stored as non-empty opaque strings. Adapters
  MUST NOT coerce a forge identifier to an integer in the core domain.
- Counters and generations are unsigned integers. Unless stated otherwise,
  they start at `1` and only increase.
- Commit identifiers MUST be represented by `{object_format, oid}`. v1 MUST
  support `sha1`; it MUST NOT assume every Git repository uses SHA-1.
- Content digests in v1 use lowercase `sha256:<64 hexadecimal digits>`.
- Persistent timestamps and deadlines are integer Unix milliseconds in UTC.
  Ordering is determined by durable sequence numbers, not wall-clock values.
- Enum values are the uppercase ASCII values written in this document.
- Optional values are SQL `NULL`, not an empty-string sentinel.
- Structured payloads MUST be normalized as UTF-8 JSON with sorted object keys,
  no insignificant whitespace, LF newlines, and Unicode normalized to NFC
  before hashing.

## Identity hierarchy

```text
Controller Mode / Operation            durable global workflow gate
Capability Key Registry / Operation    durable global signer selection
Forge Instance
└── Project
    ├── Policy Update                  ordered explicit server-policy input
    └── Work Item
        └── Run                         at most one active
            ├── Workflow Blob           content-addressed config/prompt bytes
            ├── Work Item Snapshot      immutable observed input
            ├── Snapshot Generation     one installed Snapshot per generation
            ├── Activity                ordered durable plan item
            │   ├── Review Assignment   exact review/adjudication slot input
            │   ├── Attempt             monotonically fenced generation
            │   │   ├── Attempt Claim       immutable claim/request contract
            │   │   ├── Launch Attestation trusted fresh-invocation evidence
            │   │   ├── Result Request      global Result replay/fence registry
            │   │   ├── Candidate Upload    staged non-authoritative artifact
            │   │   ├── Credential Rotation Request
            │   │   └── Attempt Terminal Fact
            │   └── Controller Operation Fact
            ├── Candidate               immutable admitted Git artifact
            │   ├── Verification Receipt
            │   ├── Review Receipt
            │   ├── Adjudication Receipt
            │   └── Consensus Decision
            ├── Publication             at most one reconciled Change Request
            │   ├── Publication Effect  immutable intent per effect generation
            │   │   └── Publication Effect Checkpoint
            │   ├── Reconciliation Fact
            │   │   └── Reconciliation Duplicate Member
            │   ├── Terminal Duplicate Cleanup Reservation
            │   │   ├── Terminal Duplicate Cleanup Member
            │   │   └── Terminal Duplicate Cleanup Action
            │   └── Forge Observation
            │       └── Change Request Search Member
            ├── Wait Condition          at most one current while waiting
            ├── Recovery Evidence       append-only ordered recovery inputs
            ├── Capacity Report         authenticated replay ledger
            ├── Worker Loss Report      authenticated loss replay ledger
            ├── Health Probe Request    durable pre-I/O probe intent
            ├── Health Probe Fact       controller-owned typed probe result
            ├── Forge Observation Schedule durable polling cadence
            ├── Forge Observation Request durable pre-I/O observation intent
            ├── Health Observation      ordered capacity/health input
            ├── Timer Fact              persisted deadline evaluation
            ├── Capability Signing Key  durable capability verifier
            ├── Storage Restoration Operation authenticated staging/replay
            ├── Storage Restoration Fact verified immutable-object repair
            ├── Management Command      authenticated Run-scoped input
            ├── Human Boundary          at most one current exceptional packet
            │   └── Human Resolution    at most one accepted resolution
            └── Transition              append-only ordered history
```

The diagram groups objects by the workflow they can affect; it does not imply
that every object beneath Run is Run-owned. Capacity Reports, Worker Loss
Reports, global-scope Health Observations and Timer Facts, and Storage
Restoration Facts, Health Probe Requests/Facts, Controller Mode/Operations,
and the Capability Key Registry/Operations are source-, object-, or
controller-scoped and may atomically fan out to
multiple Runs as their sections define. Their immutable membership, not the
diagram position, determines ownership and replay.

## Core keys

### Controller Mode and Operation

Controller operating mode is a durable singleton, never a Redis flag or
process-local switch.

| Projection field | Requirement |
| --- | --- |
| `controller_id` | Exact literal `ORCEST_V1`; singleton primary key. |
| `mode_revision` | Nonnegative monotonic compare-and-swap revision; exactly `0` only for the uninitialized bootstrap row. |
| `mode` | `NULL` exactly at revision `0`; otherwise `RUNNING`, `INTAKE_PAUSED`, `DISPATCH_PAUSED`, `DRAINING`, or `MAINTENANCE`. |
| `dispatch_paused_intake_policy` | `ALLOW_ADMISSION` or `PAUSE_ADMISSION` only when mode is `DISPATCH_PAUSED`; otherwise `NULL`. |
| `maintenance_prior_mode` / `maintenance_prior_dispatch_paused_intake_policy` | Prior initialized projection retained when entering `MAINTENANCE` or restoring a backup; both `NULL` for bootstrap initialization, for any later in-place restore that truthfully copies that bootstrap-null ancestry, and outside `MAINTENANCE`, with the policy present only when the prior mode is `DISPATCH_PAUSED`. |
| `last_operation_id` | Exact successful Controller Mode Operation, or `NULL` only at revision `0`. |

| Operation field | Requirement |
| --- | --- |
| `controller_mode_operation_id` | Caller-assigned lowercase UUID and immutable replay identity. |
| `protocol_version` | Exact literal `orcest.controller-mode-operation/1`. |
| `operation_kind` | `INITIALIZE`, `SET_MODE`, or `RESTORE_BACKUP`. |
| `expected_mode_revision` / `expected_mode` | Exact current projection CAS; `INITIALIZE` requires `0`/`NULL`, while initialized operations require a positive revision and non-`NULL` mode. |
| `requested_mode` / `requested_dispatch_paused_intake_policy` | Requested closed projection; intake policy is non-`NULL` exactly for `DISPATCH_PAUSED`. |
| `backup_manifest_digest` / `backup_prior_mode` / `backup_prior_dispatch_paused_intake_policy` | Manifest is required only for `RESTORE_BACKUP`. Prior fields are required only when the backed-up current mode is `MAINTENANCE`; they copy that row's stored maintenance-prior projection and may both be `NULL` only for bootstrap-null ancestry. They are both `NULL` for the two `DISPATCH_PAUSED` result branches and outside `RESTORE_BACKUP`. |
| `authenticated_principal_id` / `authorization_context_digest` | Exact operations principal and bounded authorization proof. |
| `request_digest` | Digest of the protocol and all immutable request/CAS fields. |
| `status` / `rejection_code` | `SUCCEEDED` with `NULL` code, or `REJECTED` with `CAS_LOST`, `ALREADY_INITIALIZED`, `NOT_INITIALIZED`, `NO_CHANGE`, `TRANSITION_NOT_ALLOWED`, `AUTHORITY_REVOKED`, or `INTEGRITY_CONFLICT`. |
| `result_mode_revision` / `result_mode` / `result_dispatch_paused_intake_policy` | Exact resulting projection for success; all `NULL` for rejection. |
| `response_http_status` / `response_json` / `response_digest` | Exact canonical terminal replay response; success `200`, CAS/transition/integrity `409`, revoked authority `403`; transport `replayed` alone is excluded from its digest. |
| `completed_at_ms` | Informational commit time. |

Same-principal, same-operation-ID, same-digest replay returns the stored
response; conflicting reuse is `409`. A success CASes and increments the mode
revision, updates the singleton and reciprocal `last_operation_id`, and
commits the immutable Operation in one writer transaction. Before any endpoint
opens, a new store contains only the revision-`0`/`NULL` bootstrap row. The
registered controller bootstrap service principal MUST commit exactly one
`INITIALIZE` operation from `0`/`NULL` to revision `1`/`MAINTENANCE`, with
nullable intake policy and no prior-mode fields. No operator or repository
principal may initialize it, and ordinary mode operations reject while the
row remains uninitialized.

For `RESTORE_BACKUP`, the requested projection is derived rather than chosen:
`MAINTENANCE`/`NULL` for a maintenance backup, exact
`DISPATCH_PAUSED/PAUSE_ADMISSION` for an already-safe backup, and
`DISPATCH_PAUSED/PAUSE_ADMISSION` for every other initialized backup. A request
whose fields do not encode that branch is `TRANSITION_NOT_ALLOWED` before any
restore mutation.

The initialized transition matrix is closed. `SET_MODE` permits every
distinct pair among the five modes. A same-mode request is `NO_CHANGE` unless
the mode is `DISPATCH_PAUSED` and the requested intake policy differs, in which
case it is a real successful revision. Entering `MAINTENANCE` copies the prior
mode/policy into the maintenance-prior fields; leaving it clears them. A
`RESTORE_BACKUP` operation is accepted only from the registered storage-
reconciler service principal after the backup has passed the full restore
checks. It takes the restored positive revision/mode/policy as its exact CAS
and follows exactly one branch. A backed-up `MAINTENANCE` row remains
`MAINTENANCE`, increments revision in place, and copies its stored prior mode,
prior policy, and maintenance permission envelope without nesting maintenance.
A backed-up exact `DISPATCH_PAUSED/PAUSE_ADMISSION` row remains that exact safe
projection and increments revision in place. Every other backed-up initialized
projection is CASed to `DISPATCH_PAUSED/PAUSE_ADMISSION` before any ordinary
endpoint is enabled, and the successful restore installs that safe projection
at the incremented revision. The latter two branches have no maintenance-prior
fields. Restore never resumes an operational backed-up mode automatically. Backup
retention therefore includes the mode row, last successful Operation, and
prior-mode fields; an authenticated later `SET_MODE` is required to resume.

`RESTORE_BACKUP` runs under the exclusive storage barrier. The three branches
above are exhaustive: maintenance in place; exact dispatch-paused/
pause-admission in place; or one CAS from any other initialized projection to
dispatch-paused/pause-admission followed by restore finalization. The third
branch cannot restore bytes and only later attempt to pause: its safe-mode CAS
and restored-state installation are one fail-closed recovery transaction. No
branch temporarily enables intake, offer planning, first Result acceptance,
ordinary Forge scheduling, or publication mutation.

The mode behavior matrix is
closed: `RUNNING` permits ordinary intake/dispatch/results/forge work;
`INTAKE_PAUSED` blocks new admission only; `DISPATCH_PAUSED` blocks new claims
and either permits or pauses admission according to its frozen intake policy;
`DRAINING` blocks admission and new claims but accepts already-authorized
results and forge reconciliation; `MAINTENANCE` blocks all workflow mutation
except named recovery procedures. Narrow authenticated Controller Mode,
Capability Key, Secret Provision, and Project Registration management
operations remain governed by their own contracts; in particular the latter
two may run during Stage 0 while ordinary Schedule I/O stays frozen. This is
not permission to claim work or mutate a Run. Maintenance may still return a
read-only exact response already present in an idempotency ledger; it cannot
create the ledger row. Restart reloads this
projection before endpoints open. A mode change is control-plane state, not a
Run Transition, and cannot retroactively invalidate accepted durable input.

| Mode | New admission | New claims | First Result / exact replay | Forge reconciliation |
| --- | --- | --- | --- | --- |
| `RUNNING` | yes | yes | yes / yes | yes |
| `INTAKE_PAUSED` | no | yes | yes / yes | yes |
| `DISPATCH_PAUSED` | according to frozen intake policy | no; create no new `OFFERED` Attempt/outbox | yes / yes | yes |
| `DRAINING` | no | no; create no new `OFFERED` Attempt/outbox | yes / yes | yes |
| `MAINTENANCE` | no | no | no / read-only existing-ledger replay only | named maintenance/recovery procedure only |

In `MAINTENANCE`, an exact Result-request key already present in the global
ledger returns its stored response without mutation. Lookup is by the exact
presented `result_request_id` first: same key and body/bindings replays, while
same key with conflicting content returns the ordinary read-only idempotency
conflict. An unseen key returns maintenance before any accepted-Result semantic
replay lookup, even when its body would otherwise match a Result accepted under
another key. It returns HTTP `503` with the exact body
`{"protocol":"orcest.error/1","code":"CONTROLLER_MAINTENANCE",
"retryable":true,"message":"controller is in maintenance mode",
"retry_after_seconds": 60}` and creates no Result Request,
Attempt Result, Fact, Evidence, or Transition. If maintenance ends while its
capability remains authentic, the worker retries the same key/body normally;
the response does not extend either workflow or authentication deadline.

The endpoint is `POST /api/v1/controller-mode-operations`. Its terminal body
has exact protocol `orcest.controller-mode-result/1`,
`controller_mode_operation_id`, `operation_kind`, `status`, and `replayed`; success additionally
has `mode_revision`, `mode`, and nullable
`dispatch_paused_intake_policy`, while rejection instead has only the closed
`rejection_code`. `ALREADY_INITIALIZED`, `NOT_INITIALIZED`, and `NO_CHANGE`
map to `409`, as do `CAS_LOST`, `TRANSITION_NOT_ALLOWED`, and
`INTEGRITY_CONFLICT`; `AUTHORITY_REVOKED` maps to `403`. Stored JSON has
`replayed=false`; transport replay may change only that excluded projection.

### Forge Instance

A Forge Instance is one configured API authority, such as `github.com` or a
specific GitHub Enterprise installation.

| Field | Requirement |
| --- | --- |
| `forge_instance_id` | Controller-assigned UUID; immutable primary key. |
| `adapter_kind` | Code-owned adapter enum, initially `GITHUB`. |
| `canonical_origin` | Normalized scheme and authority; unique. |
| `credential_secret_id` | Logical Secret UUID whose current verified version supplies adapter authority; never a mutable versioned reference. |
| `registration_provenance_version` | Positive Secret version whose Credential Rotation Receipt and installation/account authorization were verified when this Forge Instance registration was installed. It is audit provenance, not a forever-current credential. |

Forge credential rotation updates the logical Secret's current reference; it
does not rewrite the Forge Instance. Every external operation resolves and
freezes the exact current version under the Secret lock. In particular, each
`FORGE_CONNECTIVITY` Health Probe Request stores that exact version and every
Fact, Observation, scope identity, subject binding, and digest copies it.

### Project

A Project is one registered source repository under one Forge Instance.

| Field | Requirement |
| --- | --- |
| `project_id` | Controller-assigned UUID; immutable primary key. |
| `forge_instance_id` | Required Forge Instance reference. |
| `installation_or_account_ref` | Exact non-secret registered forge installation/account authority selected at initial registration. |
| `repository_external_id` | Stable opaque repository identifier returned by the forge. |
| `repository_locator` | Human-readable owner/name or equivalent; mutable projection only. |
| `default_ref` | Trusted default branch/ref selected by server-owned Project registration. |
| `trusted_base_policy_ref` | Versioned server-owned registration policy defining allowed base refs and resolution rules; repository configuration cannot replace it. |
| `budget_policy_ref` / `budget_reset_window_ref` | Versioned server-owned Project budget and reset-window policies; repository configuration can consume but cannot increase or redefine them. |
| `source_read_secret_id` / `publication_secret_id` | Logical Secret UUIDs authorized respectively for source reads and controller-only publication. Rotation changes versions, never these identities. |
| `registration_source_read_secret_version` / `registration_publication_secret_version` | Exact positive versions resolved and validated by `registration_operation_id`; immutable provenance for that registration revision, not the version future work must use. |
| `registration_revision` | Positive monotonically increasing revision installed by the latest successful Project Registration Operation. |
| `registration_operation_id` | Exact successful Project Registration Operation that installed `registration_revision`; required. |
| `work_item_discovery_schedule_id` | Exact Project-scoped `WORK_ITEM_DISCOVERY` Forge Observation Schedule created by `REGISTER` and retained across revalidation; required. |
| `registration_state` | `ACTIVE`, `SUSPENDED`, or `REMOVED`. |

`(forge_instance_id, repository_external_id)` MUST be unique. Repository
renames therefore do not create a new Project.

### Project Registration Operation

A Project Registration Operation is the immutable request/response replay
ledger for `POST /api/v1/projects/registrations`.

| Field | Requirement |
| --- | --- |
| `project_registration_operation_id` | Controller-assigned UUID primary identity. |
| `protocol_version` | Exact literal `orcest.project-registration/1`. |
| `authenticated_principal_id` / `idempotency_key` | Exact transport principal and caller-supplied lowercase UUID; the pair is unique and is the request replay identity. |
| `mode` | `REGISTER` when `requested_project_id` is `NULL`, or `REVALIDATE` when it names an existing Project. |
| `requested_project_id` | `NULL` only for `REGISTER`; exact existing Project UUID only for `REVALIDATE`. |
| `expected_registration_revision` | `NULL` only for `REGISTER`; required positive current Project revision for `REVALIDATE`. It is included in `request_digest`. |
| `installation_or_account_ref` | Exact non-secret server registration reference from the request; immutable across `REVALIDATE`. |
| `request_json` / `request_digest` | Canonical bounded non-secret request from the repository-configuration contract and its digest. Raw credentials and Secret References are forbidden. |
| `authorization_context_digest` | Immutable server RBAC/forge-installation/policy-reference decision for that exact request. |
| `resolved_forge_instance_id` / `resolved_repository_external_id` / `resolved_base_commit` | Exact stable forge/repository/base resolution used for validation; required after forge resolution. |
| `resolved_forge_api_secret_ref` / `resolved_source_read_secret_ref` / `resolved_publication_secret_ref` | Three distinct internal versioned Secret References with purposes `FORGE_API`, `SOURCE_READ`, and `PUBLICATION`, resolved from the same authorized forge installation/account; required for `SUCCEEDED` and `NULL` for `REJECTED`. They are covered by `resolution_digest` but never appear in the public request or response. |
| `resolution_digest` | Required digest of the complete canonical internal resolution: request/authorization digests, installation/account binding, resolved forge/repository/base identities, conditional resolved Secret References, and successful discovery-Schedule identity. It is never returned publicly. |
| `status` | `SUCCEEDED` or `REJECTED`. Authentication failures and malformed requests do not create an Operation. |
| `result_project_id` / `result_registration_revision` | Required only for `SUCCEEDED`; exact Project and installed positive revision. Both `NULL` for `REJECTED`. |
| `result_work_item_discovery_schedule_id` | Exact Project `WORK_ITEM_DISCOVERY` Schedule for `SUCCEEDED`; newly created by `REGISTER` and reused by `REVALIDATE`. `NULL` for rejection and internal-only, not a public response field. |
| `rejection_code` | Required only for `REJECTED`: `STABLE_REPOSITORY_OWNERSHIP_CONFLICT`, `WORKFLOW_INVALID`, `CAPABILITY_UNSUPPORTED`, or `POLICY_VALIDATION_FAILED`. No secret or arbitrary forge body. |
| `response_http_status` / `response_json` / `response_digest` | Exact canonical public replay response. Success is `200`; ownership conflict is `409`; the other rejection codes are `422`. The digest covers only HTTP status and fields present in the public response, excluding exactly the transport `replayed` projection. |
| `completed_at_ms` | Informational commit time. |

The mode/nullability union is closed. Authentication, syntax/size validation,
authority checks, and forge resolution before the commit are read-only. The
single writer transaction claims `(authenticated_principal_id,
idempotency_key)` and inserts the terminal immutable Operation. Reuse by the
same principal and identical digest returns the stored response; a different
digest is `409`. A key presented by another principal is a different composite
identity but cannot update an existing Project without that principal's exact
Project authority. Cross-principal stable-repository ownership is still fenced
by the Project uniqueness and authorization checks.

A successful transaction atomically inserts the immutable Operation/response
and inserts or updates the Project to exactly
`result_registration_revision`; `REGISTER` installs revision `1`, while
`REVALIDATE` atomically requires
`Project.registration_revision = expected_registration_revision` and installs
exactly `expected_registration_revision + 1`. Revalidation also requires the
requested default ref, trusted-base policy ref, budget policy ref, and budget
reset-window ref, plus `installation_or_account_ref`, equal the current Project
values. It may refresh only mutable
repository locator/readiness projections after re-proving stable forge
identity, installation-use authority, bundle validity, and capability health.
Changing a default ref or authority-bearing policy reference through this
endpoint is `409`; such changes enter only through an ordered
`SERVER_ROLLOUT` Policy Update. A revision CAS mismatch is also `409` and
changes neither Project nor registration ledger. A request that passes initial
authentication/syntax but ends in a bounded rejection stores only the immutable
Operation/response and changes no Project. A client retry after a crash before
this transaction safely repeats read-only validation; a retry after the
transaction returns the stored response. Redis is never authoritative for
registration. Operation request/response bytes and authorization evidence are
included in the controller backup and retained at least as long as the
resulting Project or registration idempotency window, whichever is longer.

Successful `(result_project_id, result_registration_revision)` is unique. The
successful Operation's result fields and the Project's
`registration_operation_id/registration_revision` form a reciprocal deferrable
binding and MUST match exactly. `REGISTER` inserts both sides atomically;
`REVALIDATE` changes the Project pointer and revision in the same CAS transaction
that inserts the new successful Operation. A rejected Operation can never be a
Project's registration pointer.
`REGISTER` also inserts the Project's sole `ACTIVE`, revision-0
`WORK_ITEM_DISCOVERY` Schedule with `target_kind = PROJECT`,
`target_id = project_id`, null Run/Publication, no last Request/discovery pair,
and `next_due_at_ms = completed_at_ms`, and installs reciprocal Project and
Operation schedule pointers in that same transaction. `REVALIDATE` names and
retains that exact Schedule; it cannot create a second non-closed discovery
identity. A successful registration response cannot become visible without
both Project and Schedule, and a crash cannot leave an active Project
undiscoverable. `ACTIVE` here is only the Schedule's durable desired cadence;
when registration occurs during Stage-0 `MAINTENANCE`, the Controller Mode gate
still forbids Request creation/delivery/completion until a later authenticated
mode exit. Registration never starts discovery I/O in its transaction.
The successful Operation's resolved `FORGE_API` Secret ID MUST equal the
resolved Forge Instance's `credential_secret_id`; its exact current version
and creation provenance are verified under the installation owner before
success. The other resolved Secret IDs MUST equal the resulting
Project's `source_read_secret_id` and `publication_secret_id`, and their
versions MUST equal its `registration_source_read_secret_version` and
`registration_publication_secret_version`. Resolution is permitted only from the installation/account covered by
`authorization_context_digest`; revalidation cannot substitute credentials
from another authority. These internal bindings are backup/audit state, not
part of the public replay response.

The Project records logical credential identity and registration provenance;
it is not a mutable “current Secret Reference.” At creation of each Attempt
Claim or Publication Effect, the controller resolves the applicable logical
Secret's verified current version under the Secret lock and freezes that exact
`SecretReference` on the new immutable object. Rotation cannot rewrite an
existing Claim or Effect, and replay keeps using its frozen version. A failure
of a frozen version enters `SECRET_RECOVERY` with that logical Secret ID and a
minimum acceptable version; after a satisfying version arrives, recovery
creates a new Claim/Attempt generation or Publication Effect generation rather
than mutating the old object.

Both success and rejection use protocol
`orcest.project-registration-result/1`. A rejected body contains exactly
`protocol`, `idempotency_key`, derived `mode`, `status = REJECTED`, the closed
`rejection_code`, bounded sorted `diagnostics`, and transport `replayed`. It
contains no Project/result identity. The successful body is the exact schema in
[Repository configuration](repository-configuration.md#local-cli-contract) and
has `status = SUCCEEDED`. Stored JSON uses `replayed = false`; identical replay
changes only that field to `true`. `response_digest` omits exactly `replayed`
for both variants, while the HTTP status is included in the digest preimage.
Internal Secret References, authorization evidence, and resolved fields absent
from that public body are forbidden from `response_digest`; they are covered
only by `resolution_digest`.
The Operation and resulting Project also copy the same
`installation_or_account_ref`. Changing that reference is `409` in v1 and
requires a separately specified authenticated installation/account migration;
it is neither revalidation nor a repository/Policy Update.

### Work Item

A Work Item is the forge-neutral identity of an intake item. It is not a copy
of the mutable issue.

```text
WorkItemKey = (project_id, work_item_external_id)
```

`work_item_external_id` is the adapter's stable opaque identity. Display
numbers and URLs are projections and MAY change without changing this key.

## Work Item Snapshot

A Work Item Snapshot is one immutable, self-contained capture of proposed Run
inputs. Installation into a specification generation is a separate immutable
association so multiple observed changes can be retained and coalesced before
a safe boundary.

| Field | Requirement |
| --- | --- |
| `snapshot_id` | Controller-assigned UUID. |
| `run_id` | Owning Run. |
| `snapshot_sequence` | Strictly increasing capture sequence within the Run. |
| `source_kind` / `source_id` | `FORGE_OBSERVATION` plus its exact ordered observation, or `POLICY_UPDATE` plus its exact ordered server Policy Update. |
| `work_item_observation_id` | Latest accepted ordered `WORK_ITEM_SNAPSHOT` Forge Observation whose specification/config inputs are composed into this capture; equals `source_id` when `source_kind = FORGE_OBSERVATION` for that observation. |
| `base_observation_id` | Latest accepted ordered trusted `BASE_HEAD` Forge Observation composed into `base_ref`/`base_commit`. |
| `project_id` / `work_item_external_id` | Exact Work Item identity. |
| `forge_revision` | Adapter-provided revision token, updated timestamp, or fingerprint used to detect later change. |
| `title` | Normalized title observed at admission. |
| `body` | Normalized body observed at admission. |
| `specification_comments` | Ordered normalized comments only when pinned repository policy explicitly opts them in; otherwise empty. |
| `base_ref` | Trusted source ref selected for the generation. |
| `base_commit` | Exact `{object_format, oid}` resolved from `base_ref`. |
| `workflow_schema_version` | Parsed `.orcest` schema version. |
| `workflow_hash` | Exact `sha256:`-prefixed lowercase digest of the normalized, validated workflow bundle defined by repository configuration. |
| `normalized_workflow_blob_digest` | Digest reference to a `CONFIG_JSON` Workflow Blob containing the fully defaulted canonical configuration. |
| `normalized_prompt_blobs` | Lexicographically path-sorted entries `{path, git_blob, blob_digest}`, where `blob_digest` references a `PROMPT_UTF8` Workflow Blob. |
| `effective_policy_blob_digest` | Digest reference to a `POLICY_JSON` Workflow Blob containing the complete normalized effective policy. |
| `server_policy_revision` | Exact server policy revision incorporated into the effective policy. |
| `trusted_base_policy_ref` / `budget_policy_ref` / `budget_reset_window_ref` | Exact versioned Project registration policies incorporated into this capture. |
| `policy_hash` | Digest of the normalized effective policy after intersecting repository parameters with server-enforced profiles, limits, and minimums. |
| `reducer_version` | Code-owned reducer semantics pinned for replay and upgrade compatibility. |
| `specification_hash` | Digest of title, body, and opted-in specification comments. |
| `generation_input_hash` | Digest of `specification_hash`, `workflow_schema_version`, `workflow_hash`, and `policy_hash`; equality decides whether specification/policy inputs changed. |
| `supersession_key` | Policy-specific generation-coalescing key computed exactly below; pending selection and safe-boundary supersession compare this key, not `generation_input_hash` alone. |
| `snapshot_hash` | Digest of all authoritative fields above except `snapshot_id`. |
| `captured_at_ms` | Observation time; informational, not ordering authority. |

`(run_id, snapshot_sequence)` MUST be unique. The referenced normalized
workflow, prompt, and effective-policy bytes MUST be sufficient to replay an
active generation without reading the mutable forge ref; hashes without durable
content are not sufficient.

For one Run, “latest accepted trusted `BASE_HEAD`” means the eligible
Work-Item- or Publication-targeted observation whose consuming
`FORGE_OBSERVATION` Transition has the greatest `transition_sequence`, plus
the generation-1 Snapshot's `base_observation_id` explicitly anchored at the
Run's `ADMIT` Transition sequence. The
observation must name the Project's current trusted ref and must already have
been consumed exactly once by that Run. Per-target `observation_sequence`,
adapter time, and arrival order cannot compare observations across the two
targets; Transition sequence is the sole cross-target order. Every Snapshot
capture, Policy Update composition, rebase, and approval continuation uses
this selector and stores the selected `base_observation_id`.

Comments are not specification inputs by default. A label change, CI event, or
ordinary discussion comment MUST NOT change the `specification_hash`.

The Snapshot's normalized base-movement policy fixes `supersession_key`:

```text
REBASE_BEFORE_PUBLICATION or PIN:
  supersession_key = generation_input_hash

SUPERSEDE_AT_BOUNDARY:
  supersession_key = sha256(canonical_json({
    generation_input_hash,
    base_commit
  }))
```

`base_commit` uses its canonical `{object_format, oid}` representation. The
field is part of `snapshot_hash`. A base-only capture therefore coalesces with
the installed generation under `REBASE_BEFORE_PUBLICATION` or `PIN`, but is a
pending generation input under `SUPERSEDE_AT_BOUNDARY`.

### Workflow Blob

A Workflow Blob is immutable content needed to replay a Snapshot.

| Field | Requirement |
| --- | --- |
| `blob_digest` | Primary content identity computed by the domain-separated formula below. |
| `media_kind` | `CONFIG_JSON`, `PROMPT_UTF8`, `POLICY_JSON`, or `SERVER_POLICY_JSON`. |
| `byte_length` | Exact normalized byte count within server bounds. |
| `normalized_bytes` | Canonical JSON bytes or normalized prompt UTF-8 bytes. |

The identity is exactly:

```text
blob_digest = "sha256:" + lowercase_hex(SHA256(
  ascii("orcest-workflow-blob-v1") || 0x00 ||
  utf8(media_kind) || 0x00 ||
  uint64_be(byte_length) ||
  normalized_bytes
))
```

`byte_length` is the exact unsigned 64-bit length of `normalized_bytes` before
hashing. The blob row/bytes MUST be durable before a Snapshot references it.
Only an equal `blob_digest`, `media_kind`, `byte_length`, and byte sequence may
reuse a row. Identical bytes under different media kinds deliberately have
different identities. Any mismatch/collision or missing referenced blob is an
integrity failure; fetching the mutable forge copy is not a replay fallback.

### Effective policy hash inputs

`policy_hash` is the digest of the complete canonical `POLICY_JSON` object. It
MUST include:

- the server policy revision and exact Project trusted-base, budget, and reset
  window policy references;
- the normalized repository-selected execution profiles, exact allowed
  worker/provider/model/account mappings, provider/model family classification
  IDs and immutable classification revision, provider/model constraints, and
  their intersection with server allowlists;
- sandbox, filesystem, network, tool, credential-purpose, artifact-size,
  result-size, and execution-deadline constraints;
- the single v1 deterministic verification profile ID `default` and its full
  ordered required command set;
- review/adjudication roles, slots, independence rules, approval threshold,
  blocker rule, abstention rules, and panel limits;
- recovery thresholds, stable fallback order, server-owned `claim_timeout_ms`,
  wait/backoff bounds including exact `max_provider_rate_limit_wait_ms`, and
  the complete resolved budget accounting scope, integer unit, positive limit,
  reset computation, Report-freshness bound, authorized reporting principal,
  and offer/wake behavior;
- base-movement behavior constrained by the trusted-base policy;
- publication authority, compare-and-swap, and ownership rules; and
- protocol compatibility minimums and every server security boundary that can
  affect admission, execution, gating, or publication.

Current usage counters, health, timestamps, secret values, and model output are
not policy inputs. Repository configuration may strengthen a supported gate or
select within server bounds; it cannot reduce a server minimum. Hash equality
requires byte-identical canonical effective policy, not merely equal repository
YAML.

### Policy Update

A Policy Update is an explicit ordered server input that may install a new
effective policy for active Runs in one Project. Merely changing controller
configuration does not alter an active Run.

| Field | Requirement |
| --- | --- |
| `policy_update_id` | Controller-assigned UUID. |
| `project_id` / `policy_update_sequence` | Target Project and strictly increasing per-Project sequence. |
| `server_policy_revision` / `server_policy_blob_digest` | Exact new revision and durable normalized `SERVER_POLICY_JSON` bytes. |
| `default_ref` | Exact server-selected trusted default ref installed by this rollout. |
| `trusted_base_policy_ref` / `budget_policy_ref` / `budget_reset_window_ref` | Exact server-owned Project policy references. |
| `source_kind` / `source_id` | Exact `SERVER_ROLLOUT` and the controller-assigned rollout UUID; no other v1 source kind is valid. |
| `authenticated_principal_id` | Registered server rollout service principal that produced this ordered input. |
| `created_at_ms` | Informational time. |

`(project_id, policy_update_sequence)` is unique. The rollout service creates
the Policy Update from a pre-registered server policy revision through its
internal rollout transaction; v1 exposes no administrative policy-apply API.
When a rollout changes `default_ref`, the rollout service holds the Project's
admission/policy barrier, resolves the new ref, persists and fully reduces one
matching trusted `BASE_HEAD` observation for every active Run's Work Item, and
only then inserts the Policy Update and changes the Project projection. The
rollout transaction updates the Project's current `default_ref` and three
policy-reference projections to the exact values carried by the Update. New
admission cannot interleave the barrier; active Runs remain bound to installed
Snapshots until each consumes the ordered Policy Update and reaches its safe
boundary. Thus policy capture always finds a latest accepted base observation
whose ref matches this Update, never an old-ref commit. Replay of the same source
identity and digest is idempotent; conflicting reuse fails closed. An active
Run remains bound to its installed Snapshot when its reducer consumes this
Policy Update exactly once to capture/coalesce a pending Snapshot. The Snapshot
is installed only by the later safe-boundary continuation. At capture time the
reducer intersects these
server inputs with the repository configuration/specification from the Run's
latest accepted ordered `WORK_ITEM_SNAPSHOT` Forge Observation and the exact
base from its latest accepted trusted `BASE_HEAD` observation, including either
newer pending input not yet installed. It persists all three source IDs and the
resulting `POLICY_JSON`/`policy_hash` on the new Snapshot. It MUST NOT copy
specification/config/base fields from the installed Snapshot when a newer
corresponding observation exists; therefore pending specification B and base C
followed by policy P captures `B + C + P`, never `A + P`. A repository event
cannot manufacture a Policy Update.

### Budget Report

A Budget Report is the immutable, authenticated scheduling input that proves
consumption for one Project accounting scope and reset window. Repository
configuration and worker/model output cannot create one. A server-registered
budget-accounting principal submits reports through the narrow controller
surface; the controller derives availability from normalized integers.

| Field | Requirement |
| --- | --- |
| `budget_report_id` | Caller-assigned lowercase UUIDv4 and global idempotency identity. |
| `project_id` / `accounting_scope_id` | Exact Project and server-owned bounded non-secret accounting scope. |
| `budget_policy_ref` / `budget_reset_window_ref` | Exact current registered Project policy references used to interpret the report. |
| `window_id` / `window_start_ms` / `reset_at_ms` | Stable bounded window identity and exact interval, with `window_start_ms < reset_at_ms`. |
| `source_sequence` / `source_revision` | Positive strictly increasing sequence per `(project_id, accounting_scope_id)` and stable provider/accounting revision. |
| `limit_microunits` / `consumed_microunits` | Positive limit and nonnegative cumulative consumption in the policy's normalized integer micro-unit; floating point is forbidden. |
| `availability` | Controller-derived `AVAILABLE` exactly when `consumed_microunits < limit_microunits`, otherwise `EXHAUSTED`. |
| `authenticated_principal_id` / `authorization_context_digest` | Registered budget-accounting service and immutable authorization decision. |
| `affected_run_ids_digest` / `next_member_ordinal` / `fanout_completed_at_ms` | Frozen wake-fanout membership digest plus restartable cursor/completion projection. |
| `accepted_at_ms` / `expires_at_ms` | Controller acceptance time and exact freshness deadline derived as `min(reset_at_ms, accepted_at_ms + installed max_budget_report_age_ms)`; acceptance requires `accepted_at_ms < expires_at_ms`. |
| `report_digest` | Domain-separated digest of every immutable field, including both controller times. |
| `response_http_status` / `response_json` / `response_digest` | Exact stored `200` `orcest.budget-report-result/1` response; digest excludes only transport `replayed`. |

`(project_id, accounting_scope_id, source_sequence)` and
`(project_id, accounting_scope_id, source_revision)` are unique. Exact
`budget_report_id`/digest replay returns the stored response; conflicting ID or
revision reuse fails closed. Reports with a policy-reference mismatch are
rejected before persistence. The latest applicable Report is the greatest
accepted sequence for the exact current scope, policy references, and window
whose freshness deadline has not been reached. An old-window, expired, or
superseded-policy Report cannot authorize an offer. At `expires_at_ms`, the
controller first persists or reuses the Report's `BUDGET_REPORT_EXPIRY` Timer
Fact; that Fact has no Run Transition and merely makes the Report ineligible
for all later offer/wake checks.

`RUNNING`, `INTAKE_PAUSED`, `DISPATCH_PAUSED`, and `DRAINING` may accept a
first Report and advance its wake fanout; their ordinary offer gates still
apply. `MAINTENANCE` permits only exact read-only replay of an already stored
Report response. An unseen Report returns the controller's exact maintenance
503 without a row or membership, and an incomplete prior fanout remains
durable until mode exit.

An `EXHAUSTED` latest Report is a closed offer-planning gate. At the next safe
planning boundary, the reducer creates zero-counter `BUDGET` Recovery Evidence
sourced by that Report and selects `WAIT_BUDGET`; only its later Evidence
Transition creates the Wait. It never interrupts an already `CLAIMED` Attempt
and never weakens a verification or consensus gate. An offer is legal only
when the latest applicable Report is `AVAILABLE`; absence or stale accounting
evidence leaves work durably `PLANNED` for Report reconciliation and creates no
invented Evidence or Wait rather than assuming unused budget.

An accepted `AVAILABLE` Report freezes, in bytewise Run-ID order, every current
same-Project `WAITING/BUDGET` Run whose wake identity accepts its scope,
policy/window, and minimum source sequence. `EXHAUSTED` freezes an empty wake
membership. Child `Budget Report Run` rows use contiguous zero-based
`(budget_report_id, member_ordinal)` identity and unique
`(budget_report_id, run_id)` membership; their ordered Run IDs reproduce
`affected_run_ids_digest = SHA-256(canonical length-prefixed bytewise-Run-ID
sequence)`, including the canonical empty sequence. Report and membership
commit atomically. Fanout
then applies `T(BUDGET_REPORT, budget_report_id)` once per member and advances
`next_member_ordinal` transactionally. Each member rechecks that the Report is
still unexpired/current and the Wait binding still matches; otherwise it
receives the required same-state audit Transition. The reset-boundary Timer
arm only makes the Wait eligible for budget reconciliation: a replacement offer still requires a
current authenticated `AVAILABLE` Report for the new/current window. Policy
expansion wakes only through a later Report under the installed expanded
policy, never from a mutable configuration lookup.

### Snapshot Generation

A Snapshot Generation installs one captured Snapshot as the normative input of
one Run generation.

| Field | Requirement |
| --- | --- |
| `run_id` / `specification_generation` | Immutable composite identity; generation starts at `1` and increases by one. |
| `snapshot_id` | Installed Work Item Snapshot; unique across installations. |
| `installed_transition_sequence` | Exact `SPEC_SUPERSEDE` Transition that installed it; no other trigger kind may install a Snapshot. |

There is exactly one Snapshot Generation for every installed
`(run_id, specification_generation)`. Pending Snapshots have no Snapshot
Generation row. This permits an observed A -> B -> C sequence to retain B for
audit while installing only the latest eligible C at the safe boundary.

## Run

A Run is the durable lifecycle Orcest owns for a Work Item.

| Field | Requirement |
| --- | --- |
| `run_id` | Controller-assigned UUID. |
| `project_id` / `work_item_external_id` | Owning Work Item key. |
| `state` | One state defined by the [workflow lifecycle](workflow-lifecycle.md). |
| `specification_generation` | `0` only for newly admitted `ADMITTED` before its first `SPEC_SUPERSEDE`; otherwise current positive Snapshot generation. |
| `current_snapshot_id` | `NULL` exactly in that pre-install admission interval; otherwise Snapshot installed for the current generation. |
| `pending_snapshot_id` | Latest ordered Snapshot whose `supersession_key` differs from the installed Snapshot and awaits a safe specification-generation boundary, or `NULL`. |
| `supersede_requested` / `supersede_requested_transition_sequence` | Boolean safe-boundary fence plus exact Snapshot-capture Transition that most recently set it. Both are non-`NULL`/true exactly while a differing `pending_snapshot_id` waits for current claimed or controller work to reach a safe boundary; otherwise false/`NULL`. |
| `current_candidate_id` | Exact Candidate currently being gated, or `NULL`. |
| `policy_replan_candidate_id` | Prior-generation Candidate retained only as read-only context while a mandatory policy-only `REPLAN` is current; otherwise `NULL`. It is not current and has no eligible old Plan or gate. |
| `publication_id` | The Run's Publication after it is first planned, or `NULL`. |
| `next_activity_ordinal` | Next per-Run Activity ordinal. |
| `next_transition_sequence` | Next append-only Transition sequence. |
| `recovery_origin_state` / `recovery_activity_id` | Nonterminal state being autonomously recovered and its Activity when applicable; origin is required while `RECOVERING`, Activity may be `NULL`, and both are `NULL` otherwise. |
| `recovery_entry_source_kind` / `recovery_entry_source_id` | Exact persisted trigger that most recently entered the current `RECOVERING` episode; non-`NULL` exactly while `RECOVERING`. |
| `recovery_resume_wait_condition_id` | Exact satisfied Wait Condition whose `resume_state` was copied to `recovery_origin_state`, or `NULL` when this recovery episode did not resume a Wait. |
| `recovery_resume_human_boundary_id` / `recovery_resume_human_resolution_id` | Exact resolved Boundary/Resolution pair whose `resume_state` was copied to `recovery_origin_state`, or both `NULL` when this episode did not resume a Human Boundary. |
| `recovery_strategy_index` / `rescue_epoch` | Durable deterministic position in the recovery ladder. |
| `current_recovery_evidence_id` | Latest ordered Recovery Evidence applied to the recovery projection, or `NULL`. |
| `wait_condition_id` | Current Wait Condition exactly while `WAITING`, otherwise `NULL`. |
| `human_boundary_id` | Current Human Boundary exactly while `NEEDS_HUMAN`, otherwise `NULL`. |
| `pending_dependency_observation_id` / `pending_dependency_set_digest` | Latest ordered unsatisfied/unknown `DEPENDENCY_STATE` observation and canonical required-dependency set awaiting the next safe boundary, or both `NULL`. |
| `pending_dependency_transition_sequence` | Exact same-state `FORGE_OBSERVATION` Transition that installed the pending dependency pointer; non-`NULL` exactly with that pointer. |
| `panel_staffing_candidate_id` / `panel_staffing_panel_round` / `panel_staffing_kind` / `latest_staffing_recheck_transition_sequence` | Coalesced current-panel staffing continuation. All are non-`NULL` together only in `REVIEWING` or `ADJUDICATING`: exact current Candidate, positive panel round, `REVIEW` or `ADJUDICATE`, and latest peer Result/Terminal Transition whose safe boundary must re-evaluate all unfilled slots. Otherwise all `NULL`. |
| `cancellation_source_kind` / `cancellation_source_id` | First `MANAGEMENT_COMMAND/command_id` or pre-cutoff `FORGE_OBSERVATION/forge_observation_id` that started reconciliation-required cancellation, otherwise both `NULL`; immutable once set and retained after terminal cleanup. |
| `terminal_outcome` | `MERGED`, `CLOSED`, `CANCELLED`, or `NULL`. |
| `created_at_ms` / `updated_at_ms` | Informational timestamps. |

A Run is **active** while `terminal_outcome IS NULL`, including while it is
waiting, in `NEEDS_HUMAN`, published, or remediating a Change Request.

Every transition into `RECOVERING` sets one exact entry source and a
nonterminal `recovery_origin_state`. Resuming a Wait or Human Boundary copies
that object's immutable `resume_state`; it never guesses the origin from the
current state. Exactly one of the Wait-resume pointer, Human-resume pair, or
neither is populated. The entry Transition also appends the next typed
Recovery Evidence in that same transaction. No `INTERNAL` continuation may
attach an accepted object, retry, offer, wait, or otherwise apply recovery
work. A `RECOVERING` Run therefore
cannot exist without a persisted legal next reduction; leaving recovery clears
all entry/resume fields atomically.

`supersede_requested` is set when a differing pending Snapshot is captured
while a current claimed Attempt or fenced controller Activity prevents
immediate installation. The setting Transition is stored, later captures
replace the pointer in observation order, and a capture that coalesces back to
the installed Snapshot clears the flag/pointer with `pending_snapshot_id`.
Every later Result or Attempt Terminal boundary may complete/fence its current
Attempt but emits no next semantic work while this flag, cancellation, or the
pending dependency fence wins the global continuation precedence. Snapshot
installation, pending-Snapshot clearing, cancellation, or terminalization
clears the supersede pair atomically; Redis cannot clear it.

The panel staffing projection coalesces pending peer-boundary work. Each
accepted panel Result or Attempt Terminal Transition that leaves unfilled slots
and at least one peer `CLAIMED` replaces all four fields with its own current
Candidate/panel/kind/sequence; the older `INTERNAL` obligation is discharged by
that monotonic replacement and cannot later reduce. When no peer remains
claimed, only `T(INTERNAL,latest_staffing_recheck_transition_sequence)` may
perform the all-or-none staffing evaluation. Mode/key closure leaves that
single pointer pending; reopening uses the same pointer and full global
precedence. Creating offers or the panel Wait clears the projection. This is
the sole v1 `NO_CAP` panel-state exception: a panel may temporarily have
unfilled `PLANNED` slots and no live offer only while this exact continuation
or a bound panel Wait exists.

The pending dependency triple is a durable safe-boundary fence. A newer
dependency observation replaces or clears it in observation order. It survives
Redis/controller restart, prevents planning new semantic work once the current
Attempt is safely fenced, and is cleared only when the dependency becomes
authoritatively satisfied, a superseding Snapshot/cancellation/terminal input
makes it inapplicable, or its exact safe-boundary continuation creates the
bound recovery/wait path.

The Run Store MUST enforce a partial unique constraint equivalent to:

```sql
UNIQUE (project_id, work_item_external_id)
WHERE terminal_outcome IS NULL
```

Admission MUST insert the active Run, capture-sequence-1 pending Snapshot,
`ADMIT` Transition with `anchored_base_observation_id` equal to that Snapshot's
`base_observation_id`, and working Projection intent in one transaction. A
separate `SPEC_SUPERSEDE` transaction installs generation 1, and a following
`INTERNAL` transaction plans the initial Activity. Each committed phase is
restartable. A forge `orcest:working` label is only a Projection and cannot
satisfy the uniqueness constraint.

## Activity

An Activity is one durable, reducer-planned unit of work. It may be executed by
a worker or by a controller subsystem.

| Field | Requirement |
| --- | --- |
| `activity_id` | Controller-assigned UUID; immutable. |
| `run_id` | Owning Run. |
| `activity_ordinal` | Strictly increasing within the Run. |
| `specification_generation` | Generation whose policy and Snapshot govern it. |
| `policy_hash` | Exact installed effective policy governing this Activity. |
| `kind` | Code-owned kind from the lifecycle Activity taxonomy. |
| `execution_class` | `WORKER` or `CONTROLLER`. |
| `state` | `PLANNED`, `READY`, `ACTIVE`, `SUCCEEDED`, `FAILED`, `CANCELLED`, or `SUPERSEDED`. |
| `input_ref` | Normalized immutable input object or digest. |
| `candidate_id` | Exact Candidate input when the Activity operates on code. |
| `forge_observation_id` | Exact causal external snapshot, such as feedback or a new `BASE_HEAD`, when applicable. |
| `change_request_head_observation_id` / `observed_change_request_head` | Exact `CHANGE_REQUEST_HEAD`, `CHANGE_REQUEST_DISCOVERED`, `CHANGE_REQUEST_FEEDBACK`, or `CHANGE_REQUEST_MARKER` observation and its normalized `{object_format, oid}` for `PR_REMEDIATE`, post-link `REBASE`, Change Request `IMPORT`, head-bound `CLOSE_PUBLICATION`, `CLOSE_REDUNDANT_PUBLICATION`, `REPAIR_RUN_MARKER`, or any later head-fenced publication; otherwise both `NULL`. |
| `role` | Builder, verifier, reviewer role, adjudicator, or controller role required by policy. |
| `repair_cycle` / `recovery_cycle` | Nonnegative semantic repair and recovery-cycle ordinals for this plan. |
| `strategy_index` | Deterministic self-healing strategy index selected at planning. |
| `recovery_tactic` / `recovery_evidence_id` | Closed lifecycle tactic and exact Recovery Evidence selecting it, or `NULL` for non-recovery planning. |
| `rescue_epoch` | Nonnegative rescue epoch at planning. |
| `created_transition_sequence` | Transition that planned it. |
| `semantic_input_digest` | Digest of all exact semantic inputs, including ordered finding/receipt IDs and, for `REVIEW`/`ADJUDICATE`, the exact Activity Review Assignment `assignment_digest`. |
| `idempotency_key` | Deterministic digest defined below; unique within the Run. |

`(run_id, activity_ordinal)` MUST be unique. The controller increments
`next_activity_ordinal` and inserts the Activity in the same transaction as the
Transition that planned it. Retrying reducer evaluation cannot allocate a
second ordinal for the same committed transition.

The Activity idempotency key is:

```text
sha256(canonical_json({
  reducer_version,
  run_id,
  specification_generation,
  policy_hash,
  created_transition_sequence,
  kind,
  execution_class,
  semantic_input_digest,
  candidate_id,
  forge_observation_id,
  change_request_head_observation_id,
  observed_change_request_head,
  role,
  repair_cycle,
  recovery_cycle,
  strategy_index,
  recovery_tactic,
  recovery_evidence_id,
  rescue_epoch
}))
```

`(run_id, idempotency_key)` MUST be unique. Replaying the same reducer
Transition derives the same key and returns the existing Activity; it cannot
allocate a second ordinal or outbox. A later repair or recovery decision with
otherwise identical semantic input is distinct because its creating Transition
and the applicable cycle/evidence/tactic/epoch fields differ. These fields are
authoritative reducer inputs, not worker-provided values.

An Activity's identity and immutable inputs never change. A different
Candidate, Snapshot generation, Forge Observation, or semantic purpose requires
a new Activity. Retrying execution creates a new Attempt generation under the
same Activity. A committed `PLANNED` Activity has no nonterminal Attempt;
offering an eligible generation atomically changes it to `READY` and creates
that generation's `OFFERED` Attempt. Claim atomically changes
`READY`/`OFFERED` to `ACTIVE`/`CLAIMED`. A terminal Activity has no
nonterminal Attempt and can never be reopened by a retry or Result.
Every offer transaction also resolves the latest applicable authenticated
Budget Report under the Project accounting scope and installed policy. Only
`AVAILABLE` permits the offer; `EXHAUSTED`, absent, stale-window, or
policy-mismatched evidence leaves the Activity `PLANNED` without creating an
Attempt or Outbox. Exact current `EXHAUSTED` may source the typed BUDGET
Recovery-Evidence/Wait path. Absence or stale/mismatched evidence creates no
invented Fact or Wait: startup and accepted Report handling rescan durable
`PLANNED` Activities, and the next branch remains closed until a current
authenticated Report exists.

When `observed_change_request_head` is non-`NULL`, it is normalized from the
exact `change_request_head_observation_id`, not a mutable field on the Forge
Observation object. Planning verifies and stores both atomically. For
`PR_REMEDIATE`, Change Request `IMPORT`, head-bound `CLOSE_PUBLICATION`, and
`CLOSE_REDUNDANT_PUBLICATION`, that
observation normally also is `forge_observation_id`. For a post-link `REBASE`, `forge_observation_id` may
instead be the causal `BASE_HEAD`; its `input_ref` binds that new base while
`change_request_head_observation_id` independently fences the current Change
Request head. Claim and Result acceptance compare both immutable Activity
bindings, never a newly fetched forge head. A later ordered head observation
supersedes or fences the old Activity before its Result can be accepted.

A same-commit `REPEATED_NON_PROGRESS` upload is not the new Candidate required
for successful completion of a replacement Activity. The controller may retain
the producing Attempt's valid artifact Result as audit evidence, but terminally
fails that semantic Activity and never retries it; the resulting Recovery
Evidence selects any next tactic in a distinct Activity. Its new evidence/cycle
key makes that Activity distinct, and the controller never works around
non-progress by recreating the completed producer Activity under the same
idempotency key.

### v1 Activity kinds

The reducer recognizes this fixed set:

| Kind | Class | Purpose |
| --- | --- | --- |
| `PLAN` | `WORKER` | Produce a structured implementation plan from the pinned Snapshot using the repository's `implementation` profile and prompt plus the controller's fixed planning envelope. |
| `BUILD` | `WORKER` | Produce the first Candidate for a plan. |
| `VERIFY` | `WORKER` | Run policy-selected deterministic commands in a credential-free Candidate sandbox and produce a Verification Receipt. |
| `REVIEW` | `WORKER` | Produce one independent Review Receipt for a configured role. |
| `REMEDIATE` | `WORKER` | Produce a new Candidate addressing exact verification or review findings. |
| `DIAGNOSE` | `WORKER` | Explain repeated failure using evidence without changing acceptance policy. |
| `REPLAN` | `WORKER` | Produce an alternative structured plan after diagnosis or specification supersession. |
| `ADJUDICATE` | `WORKER` | Resolve conflicting evidence or a disputed blocker. |
| `REBASE` | `WORKER` | Produce a Candidate rebased onto an exact newer base. |
| `PR_REMEDIATE` | `WORKER` | Produce a Candidate for exact CI/review feedback bound to a Forge Observation. |
| `IMPORT` | `CONTROLLER` | Fetch, validate, bundle, and admit an externally advanced run-owned Change Request head from an exact Forge Observation. |
| `PUBLISH` | `CONTROLLER` | Reconcile creation or fenced update of the publication. |
| `CLOSE_PUBLICATION` | `CONTROLLER` | Perform one immutable cancellation phase: either reconcile a possible stable create request or attempt an idempotent close of one exact observed run-owned Change Request head. Discovery or head movement requires a new Activity. |
| `CLOSE_REDUNDANT_PUBLICATION` | `CONTROLLER` | Idempotently close exactly one proven equivalent, open, unmerged, and unreviewed redundant Change Request while retaining the deterministic canonical Change Request. It is a reconciliation repair, never cancellation or publication authority. |
| `REPAIR_RUN_MARKER` | `CONTROLLER` | Restore exactly one canonical Orcest v1 Run/Publication marker on an already-linked, exactly proved run-owned Change Request, or collapse byte-identical duplicate copies; never adopt or transfer ownership. |
| `RECONCILE` | `CONTROLLER` | Repair an ambiguous external or local projection without changing policy. |

Repository configuration may parameterize these Activities but cannot define a
new kind in v1.

For `CLOSE_REDUNDANT_PUBLICATION`, `input_ref` is the immutable normalized
`orcest.redundant-publication-cleanup/1` object. It contains exactly:

| Field | Requirement |
| --- | --- |
| `reconciliation_fact_id` | Exact `REDUNDANT_PUBLICATIONS_PROVEN` Fact that selected the cleanup. |
| `publication_id` / `effect_generation` | Exact current Publication and Effect fence. |
| `project_id` / `deterministic_ref` / `run_marker` | Exact registered forge ownership binding shared by the retained and redundant objects. |
| `retained_change_request_external_id` / `retained_head` / `retained_observation_id` | Canonical retained object, exact observed head, and current identity/head observation. |
| `duplicate_change_request_external_id` / `duplicate_head` / `duplicate_observation_id` | The one redundant object this Activity may close, exact expected head, and exact observation copied to `change_request_head_observation_id`/`observed_change_request_head`. |
| `complete_search_revision` | Exact complete-search revision frozen by the parent Fact. |
| `equivalence_proof_digest` / `unreviewed_observation_id` / `unreviewed_proof_revision` | Exact proof copied from the Fact member. |
| `operation_digest` | Domain-separated digest of the protocol tag and every field above. |

The Activity is planned either in `PR_MONITORING` with no cancellation intent
and no active Publication mutation, or while an `INITIAL` `PUBLISH` is
suspended at a pre-link `COMPLETE_MARKER_SEARCH/MULTIPLE` checkpoint. In that
pre-link case the Publication association remains `NULL`, the retained fields
come only from the current complete-search/Reconciliation proof, and no other
Publication mutation may run until the set is reduced to one. Its outbox uses
`source_kind = ACTIVITY`, the Activity ID as `source_id`, and the same stable
`operation_digest`; Activity and outbox commit atomically before adapter I/O.
It does not create or increment a Publication Effect. Immediately before the
external call, the controller re-reads and revalidates that the Run remains in
the exact planning state (`PR_MONITORING` or pre-link `PUBLISHING`) and
revalidates the current Publication generation, retained object, redundant
object, marker, ref, both heads,
equivalence, and absence of non-Orcest review/discussion/merge activity. Any
pre-call or adapter-returned mismatch MUST first persist the exact current
Forge Observation or observations that prove the changed object/head/marker/
ref/review/search revision; reducing that observation supersedes the
Activity/outbox and returns to `RECONCILE`. If a definitive failure supplies
no external state, only a failed Controller Operation Fact may enter recovery.
An ambiguous or unavailable result remains bound to the same active operation
for read reconciliation. No path falls through to a generic close.

For `REPAIR_RUN_MARKER`, `input_ref` is the immutable normalized
`orcest.run-marker-repair/1` object:

| Field | Requirement |
| --- | --- |
| `publication_id` / `effect_generation` | Exact current Publication and immutable Effect fence. |
| `run_id` / `project_id` / `deterministic_ref` / `change_request_external_id` | Exact owning Run, registered Project, and already-linked stable forge identity. |
| `expected_head` / `marker_observation_id` / `expected_body_revision` | Exact head and current `CHANGE_REQUEST_MARKER` Observation/body revision copied into the Activity head fence. |
| `repair_kind` | `MISSING` or `DUPLICATED_IDENTICAL`; a conflicting valid or non-identical marker is never repairable. |
| `expected_marker_set_digest` | Digest of the complete canonical marker occurrences in the observed body. |
| `desired_marker` | Exactly the canonical marker derived from `run_id` and `publication_id`; no caller or model supplies it. |
| `ownership_proof_digest` | Digest of the matching durable Publication association, Project, deterministic ref, stable Change Request ID/head, Effect, current marker set/body revision, and proof that no incompatible v1, legacy, or human ownership claim exists. |
| `operation_digest` | Domain-separated digest of the protocol tag and every field above. |

The reducer may plan this Activity only in `PR_MONITORING`, without
cancellation or another Publication mutation, after the exact observation and
durable records prove all input fields. Activity and Effect-bound Outbox commit
before I/O. Immediately before mutation the controller re-reads and CASes the
stable ID, head, body revision, ref, current marker-set digest, and ownership
proof. Mismatch first persists a new Forge Observation and supersedes the
Activity; ambiguity retains the same operation identity. Success exists only
as an authenticated `CHANGE_REQUEST_MARKER` Observation bound to the Activity
and operation digest and proving exactly one `desired_marker`. It completes the
Activity and remains `PR_MONITORING`; no Reconciliation Fact, Publication
Effect increment, Candidate, or gate is created.

## Activity Review Assignment

An Activity Review Assignment is the durable tagged-union input for exactly
one `REVIEW` or `ADJUDICATE` Activity. It is the authority from which worker
`review_slot` payloads are projected; a Redis envelope or worker-supplied slot
is never the source of this assignment.

| Field | Requirement |
| --- | --- |
| `activity_id` | Primary identity and exact `REVIEW` or `ADJUDICATE` Activity reference. |
| `assignment_kind` | `REVIEW` or `ADJUDICATE`; must equal `Activity.kind`. |
| `panel_round` | Positive frozen review panel round. |
| `reviewer_slot` | Required bounded slot ID only for `REVIEW`; otherwise `NULL`. |
| `adjudication_round` / `adjudicator_slot` | Exactly `1` and `default`, respectively, for `ADJUDICATE` in v1; otherwise both `NULL`. |
| `role` | Exact configured reviewer or adjudicator role; must equal `Activity.role`. |
| `subject_refs_digest` | SHA-256 of the complete ordered Activity Review Subject membership defined below; required for both assignment kinds. |
| `context_digest` | Digest of the exact controller-frozen Candidate, Snapshot, policy, evidence, and subject context presented to this slot; its canonical preimage includes `subject_refs_digest`. |
| `disputed_finding_ids_digest` | SHA-256 of the canonical length-prefixed ordered disputed-finding membership for `ADJUDICATE`; `NULL` for `REVIEW`. |
| `assignment_digest` | Digest of the normalized semantic tagged-union fields plus `disputed_finding_ids_digest`, using the exact formula below. |

The tagged union is closed. A `REVIEW` assignment has `reviewer_slot` non-null,
both adjudication fields null, and no disputed-finding membership. An
`ADJUDICATE` assignment has `reviewer_slot` null,
`adjudication_round = 1`, `adjudicator_slot = default`, `role = adjudicator`,
and a non-empty canonically sorted set of accepted disputed finding IDs for
this exact Candidate and panel. Canonical order is ascending UTF-8 byte order
of normalized `finding_id`. Persistence normalizes that set as
Activity Adjudication Finding rows
`(activity_id, finding_ordinal, finding_id)`: ordinal is zero-based and
contiguous; both `(activity_id, finding_ordinal)` and
`(activity_id, finding_id)` are unique; and the ordered rows MUST reproduce
`disputed_finding_ids_digest`.

`panel_round` is scoped to one exact Candidate, not the Run. The first panel
for each Candidate is round `1`; a fresh panel after decisive adjudication
increments that Candidate's prior round by one. Selecting a different
Candidate resets its independent sequence to `1`. Consequently every durable
panel identity and uniqueness rule includes `candidate_id` through its
Activity/Receipt binding; a round number alone is never cross-Candidate
identity.

### Activity Review Subject

An Activity Review Subject is one immutable member of the closed v1 subject
list presented to a `REVIEW` or `ADJUDICATE` Activity:

| Field | Requirement |
| --- | --- |
| `activity_id` | Exact owning Activity Review Assignment. |
| `subject_ordinal` | Zero-based position in the controller-frozen presentation and required-assessment order. |
| `subject_ref` | Either the literal `snapshot:overall` or `plan:requirement:<requirement_key>` for one requirement in the accepted Plan Result. |

`(activity_id, subject_ordinal)` is the primary identity,
`(activity_id, subject_ref)` is unique and ordinals are contiguous from zero.
Every v1 Assignment has exactly the literal `snapshot:overall` at ordinal zero,
followed by exactly one `plan:requirement:<requirement_key>` for every entry in
the accepted `orcest.plan/1` `requirements` array, preserving that array's
semantic order. No role, repository configuration, prompt, worker, or live
registry may narrow, extend, reorder, or rename this closed subject list. The
controller freezes it before Activity creation. The digest is exactly:

```text
subject_refs_digest = sha256(canonical_json([
  subject_ref at ordinal 0,
  subject_ref at ordinal 1,
  ...
]))
```

The canonical context document hashed into `context_digest` MUST contain that
same `subject_refs_digest` and ordered array. A context whose displayed subject
list, stored membership, or digest differs is invalid and cannot be dispatched.

The assignment digest excludes the relational `activity_id`, avoiding a
cycle with the Activity idempotency key, and is exactly:

```text
sha256(canonical_json({
  assignment_kind,
  panel_round,
  reviewer_slot,
  adjudication_round,
  adjudicator_slot,
  role,
  subject_refs_digest,
  context_digest,
  disputed_finding_ids_digest
}))
```

The Activity, its Assignment, complete subject membership, complete
adjudication-finding membership, Transition, and any new Attempt/outbox commit
atomically. `Activity.semantic_input_digest` includes `assignment_digest`;
replay of the creating Transition must therefore recover the same Activity,
Assignment, and memberships. A `REVIEW` or `ADJUDICATE` Activity without
exactly one valid Assignment and its complete subject membership is not
dispatchable. Claim construction projects the protocol `review_slot` from
this row and both applicable ordered memberships. Claim and Result acceptance
compare that projection back to the durable Assignment and memberships.

## Attempt

An Attempt is one fenced execution generation of an Activity. `attempt_id` is
the durable object identity used by worker APIs and offers; the Activity and
generation remain the natural fencing identity.

```text
AttemptID    = attempt_id
AttemptFence = (activity_id, generation)
```

| Field | Requirement |
| --- | --- |
| `attempt_id` | Controller-assigned UUID; immutable primary key. |
| `activity_id` | Parent Activity. |
| `generation` | Strictly increasing per Activity, starting at `1`. |
| `state` | `OFFERED`, `CLAIMED`, `SUCCEEDED`, `FAILED`, `ABSTAINED`, `EXPIRED`, or `SUPERSEDED`. |
| `protocol_version` | Exact worker/control protocol required. |
| `execution_profile_id` | Exact registered execution-profile identity resolved by the controller for this Attempt. |
| `worker_profile` / `provider` / `model` / `provider_account_ref` | Exact four-value non-secret execution assignment resolved from `execution_profile_id`; the account reference is `NULL` only for credential-free deterministic `VERIFY`. |
| `provider_family` / `model_family` | Canonical non-secret independence-classification IDs frozen from the registered execution profile for every model-backed Attempt; both `NULL` for deterministic `VERIFY`. |
| `classification_revision` | Immutable non-secret server-registry revision ID under which both family classifications were resolved; required for every model-backed Attempt and `NULL` for deterministic `VERIFY`. |
| `provider_secret_ref` | Exact versioned provider Secret Reference resolved and frozen by Claim for a model-backed Attempt; `NULL` while `OFFERED` and for deterministic `VERIFY`. |
| `offered_at_ms` / `claim_timeout_ms` / `claim_deadline_ms` | Durable offer time, positive server-policy timeout copied from the Snapshot, and exact sum; none is based on Redis delivery time. |
| `claimed_worker_id` / `claimed_worker_session_id` | Authenticated stable Worker identity and exact registered session, or both `NULL`. |
| `claimed_at_ms` | Durable claim time, or `NULL`. |
| `execution_deadline_ms` | Persistent deadline established by claim. |
| `capability_auth_expires_at_ms` | Exact cryptographic Attempt-capability expiry, equal to `execution_deadline_ms + 86_400_000`; permits only the bounded post-deadline Result behavior defined below. |
| `last_liveness_observed_ms` | Informational durable checkpoint, if policy persists one; Redis heartbeat remains disposable. |
| `attempt_capability_jti` / `attempt_capability_digest` | Exact JTI and domain-separated normalized-claims digest of the issued Attempt capability; both `NULL` while `OFFERED` and never bearer bytes. |
| `attempt_capability_signing_key_id` / `attempt_capability_signature_algorithm` | Exact `CapabilitySigningKey` and algorithm copied into the signed Attempt-capability claims; both `NULL` while `OFFERED`. |
| `attempt_claim_id` | Exact immutable Attempt Claim that performed `OFFERED -> CLAIMED`, or `NULL` while `OFFERED`. |
| `launch_nonce_id` / `launch_capability_digest` | Controller-issued one-shot launch identity and domain-separated normalized signed-claims digest for a model-backed claimed Attempt; both `NULL` before claim and for deterministic `VERIFY`. |
| `launch_attestation_id` / `launch_capability_consumed_at_ms` | Exact accepted Launch Attestation and informational one-shot consumption time for a model-backed Attempt; both `NULL` until acceptance and always `NULL` for deterministic `VERIFY`. |
| `terminal_reason` | Typed outcome or recovery reason. |

An `OFFERED` Attempt and its dispatch outbox row MUST commit in the same SQLite
transaction. Claim is an atomic conditional update from `OFFERED` to `CLAIMED`
for the current Activity generation and is valid only when
`controller_now_ms < claim_deadline_ms`. At equality the matching Timer Fact
may expire it. For a model-backed Attempt, that claim transaction also creates
the globally unique `launch_nonce_id` and one-shot launch-capability digest
bound to the exact Attempt and worker session. The claim and execution
deadlines, capability-auth expiry, and Claim survive Redis loss.

The effective server policy freezes `claim_timeout_ms`; the v1 default is
`300_000`, the accepted range is `30_000..3_600_000`, and repository
configuration cannot override it. Every new Attempt satisfies exactly
`claim_deadline_ms = offered_at_ms + claim_timeout_ms`. Redelivery never
changes any of those fields. At a due claim deadline, the capacity classifier
uses only the Terminal Fact's frozen health membership; it never extends the
same offer because a compatible worker might appear later.

The Activity/Attempt pair is closed: `PLANNED` has no nonterminal Attempt,
`READY` has exactly one current `OFFERED` Attempt, and `ACTIVE` has exactly
one current `CLAIMED` Attempt. An accepted kind-valid, semantically filling
success sets both Attempt and Activity to `SUCCEEDED`. An accepted failure or
abstention, or a deadline/loss requiring recovery, terminalizes the Attempt
and returns `ACTIVE`/`READY` to `PLANNED` before recovery is entered. A retry
creates the next generation only through an atomic `PLANNED -> READY` offer;
closed mode, issuance-key, capacity, or other gates leave the Activity
`PLANNED` with no new Attempt/outbox. An explicitly permanent Activity
`FAILED`, `SUPERSEDED`, or `CANCELLED` has no retry or success path.

The code-owned v1 constant is exactly:

```text
result_auth_grace_ms = 86_400_000
capability_auth_expires_at_ms = execution_deadline_ms + result_auth_grace_ms
```

The signed Attempt capability's cryptographic `exp` is
`capability_auth_expires_at_ms`, not the workflow execution deadline. Before
`execution_deadline_ms` it authorizes only the exact endpoints/scopes in its
claims. At or after `execution_deadline_ms` but strictly before
`capability_auth_expires_at_ms`, it authorizes only the Result endpoint to
return an exact already-accepted Result replay or to insert/replay a bounded
late-disposition Result Request and timeout rejection. Source access, Candidate/artifact
upload, credential rotation, liveness, launch, and any new Result/output
acceptance end at `execution_deadline_ms`. At or after
`capability_auth_expires_at_ms`, authentication fails, no Result Request or
other workflow row is inserted, and the durable execution-deadline Timer Fact
remains the recovery authority.

Creating generation `g + 1` MUST first make generation `g` terminal as
`FAILED`, `ABSTAINED`, `EXPIRED`, or `SUPERSEDED`. A `SUCCEEDED`
Attempt normally completes its Activity and therefore does not have a higher
execution generation. Only one nonterminal Attempt may exist for an Activity.
`(activity_id, generation)` MUST be unique. A result may be accepted only when
all of these match the
durable row:

```text
attempt_id
activity_id
generation
claimed_worker_id and claimed_worker_session_id
capability identity
execution_profile_id, exact four-value execution assignment, and frozen
provider_family/model_family/classification_revision
for a model-backed Attempt, exact accepted launch_attestation_id and consumed
one-shot launch capability
expected immutable input bindings
Attempt state == CLAIMED
Activity state == ACTIVE
Run specification generation
controller_now_ms < execution_deadline_ms
```

The controller checks for an already accepted identical Result before applying
the deadline rule so a replay can recover its prior response. For a first
submission, `controller_now_ms >= execution_deadline_ms` is
authoritatively late even if the
deadline sweeper has not run: the same writer transaction inserts the
source-idempotent `EXECUTION_DEADLINE` Attempt Terminal Fact, fences the Attempt
`EXPIRED`, records timeout Recovery Evidence, and rejects the Result. A first
late Result cannot win a race merely because cleanup was delayed.

For model-backed Activities, `execution_profile_id` resolves through the
installed Snapshot's immutable effective-policy copy of the server registry to
exactly one allowed `(worker_profile, provider, model,
provider_account_ref)` four-value execution assignment plus exact
`provider_family` and `model_family` independence-classification IDs before the
Attempt is inserted. The controller also freezes the exact immutable
`classification_revision` that assigned both families. All seven copied values
and `execution_profile_id` are immutable and must match claim capability.
Independence is evaluated from the copied family IDs and revision and never by
re-reading or reclassifying through a mutable registry. `provider_account_ref`
is a non-secret account identity and never substitutes for the separately
versioned `provider_secret_ref`. For credential-free deterministic `VERIFY`,
`execution_profile_id`, `provider`, `model`, `provider_account_ref`,
`provider_family`, `model_family`, and `classification_revision` are `NULL`,
while `worker_profile` names the registered deterministic verification runner.
A repository cannot supply or rewrite this resolution mapping.
Changing an execution mapping, family classification, or classification
revision affects an active Run only through an explicit Policy Update and safe
generation boundary; it cannot silently alter a later Attempt in the installed
generation. The controller retains the immutable classification revision for
the Run's audit lifetime.

## Capability Signing Key

A Capability Signing Key is one durable controller capability-signing and
public-verification key version. It signs Attempt and one-shot launch
capabilities; it is distinct from the runner key that signs a Launch
Attestation.

| Field | Requirement |
| --- | --- |
| `capability_signing_key_id` | Controller-assigned lowercase UUID and exact `kid` copied into every signed capability. |
| `registration_operation_id` | Exact successful Capability Key Operation that registered this immutable key. |
| `signature_algorithm` | Exact code-owned literal `ED25519` in v1; copied into every signed capability and never inferred from an untrusted header alone. |
| `public_verification_key` / `public_key_digest` | Canonical 32-byte Ed25519 public key and its domain-separated SHA-256 digest. Public, immutable, and required. |
| `private_signing_secret_ref` | Exact versioned controller Secret Reference for the matching private signing key; never exposed outside controller signing code. |
| `registered_at_ms` / `not_before_ms` | Informational registration time and first controller time at which issuance is permitted. |
| `state` | Monotonic projection `ACTIVE`, `RETIRED`, or `REVOKED`. |
| `retired_at_ms` / `retirement_change_id` / `retirement_principal_id` / `retirement_authorization_digest` | All `NULL` or all non-`NULL`; required for `RETIRED` and retained if that key is later revoked. They identify the exact idempotent authenticated retirement. |
| `revoked_at_ms` / `revocation_change_id` / `revocation_principal_id` / `revocation_authorization_digest` | All `NULL` unless `REVOKED`, when all are required and identify the exact idempotent authenticated revocation. |

The key ID is globally unique and public-key bytes/digest never change. New
capability issuance requires `state = ACTIVE` and
`controller_now_ms >= not_before_ms`. `RETIRED` ends issuance but does not
invalidate a previously issued, otherwise valid capability: the controller may
use the retained public key until that capability's exact cryptographic expiry,
including the bounded accepted-Result, late-Result, or accepted-Launch lookup
paths. `REVOKED` fails capability authentication immediately, including replay;
durable Timer Facts and ordinary autonomous recovery remain available. Key
state cannot be selected by repository configuration or a worker.
The row's identity, algorithm, key material, and registration times are
immutable. Only the closed `ACTIVE -> RETIRED` or
`ACTIVE -> REVOKED`, plus emergency `RETIRED -> REVOKED`, state/evidence
projection may change through its idempotent server key-registry operations.
Retirement evidence is immutable and retained across later revocation;
`RETIRED` cannot become `ACTIVE`, and `REVOKED` cannot be reversed.

`public_key_digest` is exactly:

```text
"sha256:" + lowercase_hex(SHA256(
  ascii("orcest-capability-public-key-v1") || 0x00 ||
  ascii("ED25519") || 0x00 || uint64_be(32) || public_verification_key
))
```

The signed claims of every Attempt and launch capability contain exact
`capability_signing_key_id`, `signature_algorithm`, `issued_at_ms`, and
cryptographic expiry. Authentication first resolves the exact durable key ID,
requires the claim algorithm to equal the registry value, then verifies the
signature with that row's public key. Unknown keys, algorithm substitution, or
key/digest mismatch fail closed. The normalized-claims digest includes the key
ID and algorithm but excludes signature serialization.

The complete key row, public verification bytes, private Secret Version, state
change evidence, and every referenced retired verifier are in the controller's
encrypted backup unit. A key row/public verifier MUST remain available while
any retained Attempt Claim, Launch Attestation, Result Request, capability
audit row, or backup manifest references it, and at least through the maximum
cryptographic expiry of every capability it signed. Retirement never deletes
verification material. Restore verifies `public_key_digest`, its matching
private/public pair while the private key is retained, and all referencing
foreign keys before enabling claim, launch, or Result endpoints.

## Capability Key Registry and Operation

The controller has one durable Capability Key Registry projection and an
immutable operation ledger. Redis, process configuration, and the active
process's loaded signer are caches only.

| Registry field | Requirement |
| --- | --- |
| `registry_id` | Exact literal `ORCEST_V1`; singleton primary key. |
| `registry_revision` | Nonnegative monotonic compare-and-swap revision; exactly `0` only for the absent-key bootstrap projection. |
| `current_issuance_key_id` | Exact `ACTIVE` Capability Signing Key used for new claims, or `NULL` at revision `0`, after bootstrap `REGISTER`, or during a fail-closed recovery interval in which issuance is disabled. |
| `last_operation_id` | Exact successful Capability Key Operation that installed the current projection, or `NULL` exactly at revision `0`. |

| Operation field | Requirement |
| --- | --- |
| `capability_key_operation_id` | Caller-assigned lowercase UUID and immutable idempotency identity. |
| `protocol_version` | Exact literal `orcest.capability-key-operation/1`. |
| `kind` | `REGISTER`, `SELECT`, `RETIRE`, or `REVOKE`. |
| `expected_registry_revision` / `expected_issuance_key_id` | Exact current Registry CAS inputs. Initial `REGISTER` requires `0`/`NULL`; afterward the key ID may be `NULL` only when the expected projection has issuance disabled. |
| `target_capability_signing_key_id` | Key registered, selected, retired, or revoked. |
| `replacement_issuance_key_id` | Required when `RETIRE` targets the current issuance key. When `REVOKE` targets it, this may name another `ACTIVE` key or be `NULL` for emergency fail-closed issuance disablement. Otherwise `NULL`. |
| `register_public_verification_key` / `register_public_key_digest` / `register_private_signing_secret_ref` / `register_not_before_ms` | Required only for `REGISTER`; exact immutable key material/provenance fields to install. Raw private key bytes are forbidden. |
| `authenticated_principal_id` / `authorization_context_digest` | Exact server key-operator principal and bounded authorization proof. |
| `request_digest` | Digest of protocol and every immutable request/CAS field. |
| `status` / `rejection_code` | `SUCCEEDED` with `NULL` code, or `REJECTED` with one of `CAS_LOST`, `KEY_ALREADY_EXISTS`, `KEY_NOT_ACTIVE`, `CURRENT_KEY_REQUIRES_REPLACEMENT`, `AUTHORITY_REVOKED`, or `INTEGRITY_CONFLICT`. |
| `result_registry_revision` / `result_issuance_key_id` | Exact resulting projection for `SUCCEEDED`; both `NULL` for `REJECTED`. On success the key field copies the resulting projection and is therefore `NULL` after initial or later non-selecting `REGISTER` while issuance is disabled, and after emergency `REVOKE` of the selected key without replacement; otherwise it names the resulting selected `ACTIVE` key. |
| `response_http_status` / `response_json` / `response_digest` | Exact canonical terminal replay response. Success is `200`; CAS/existence/state/replacement/integrity conflicts are `409`; revoked authority is `403`. Transport-only `replayed` is excluded from the digest. |
| `completed_at_ms` | Informational commit time. |

The operation ID is globally unique. Same-principal, same-ID, same-digest
replay returns the stored response; conflicting reuse is `409`. Every accepted
operation CASes the exact current Registry revision and issuance-key identity
in one writer transaction. `REGISTER` creates a new `ACTIVE` immutable key but
does not silently select it and requires controller proof that the referenced
private Secret Version matches the submitted public verifier. That Secret's
immutable creation Receipt must prove `owner_scope_kind = CONTROLLER`,
`owner_scope_id = ORCEST_V1`, purpose
`CAPABILITY_SIGNING_PRIVATE_KEY`, and the exact versioned reference in the
Operation; a generic Controller Secret or installation/provider credential is
ineligible. `SELECT`
requires an existing `ACTIVE` target.
Retiring the current issuance key requires atomic selection of the explicit
replacement. Revoking it atomically selects the explicit replacement or clears
`current_issuance_key_id`; the latter immediately disables new claims until a
later successful `SELECT` and durably gates planning/delivery of new offers so
they do not burn claim deadlines without issuable capabilities. This registry
gate composes with, but does not rewrite, Controller Mode. Thus no claim can be signed by a key whose
projection was already retired or revoked. A successful operation increments the Registry
revision exactly once; rejection changes no key or projection. Key creation
and `registration_operation_id` are reciprocal and deferrable.
Migration of an existing signer creates an explicit authenticated `REGISTER`
Operation that truthfully adopts the already-staged matching Secret/public
pair before endpoint enablement; no synthetic bootstrap row bypasses this
provenance.

Before any capability endpoint opens, a new store contains the singleton
revision-`0`, null-key, null-operation projection and no signing-key rows. The
first successful operation MUST be `REGISTER` with expected `0`/`NULL`; it
atomically creates registry revision `1` and one `ACTIVE` key while leaving
`current_issuance_key_id = NULL`. Issuance remains disabled until a separate
successful `SELECT` CAS advances the registry to revision `2` or later. A
synthetic selected bootstrap key, combined register-and-select operation, or
process-configured signer is invalid.

Attempt Claim creation reads and CASes this projection in the same writer
transaction, stores `capability_key_registry_revision`, and copies the same
key ID/algorithm into both capability claims. A stale loaded signer cannot
issue. Registration and selection are narrow authenticated controller
operations; repository configuration and workers cannot call them.

Fresh-store Stage 0 is closed and ordered. The bootstrap service first commits
Controller Mode `INITIALIZE` to revision-1 `MAINTENANCE`. The registered Secret
operator then provisions or adopts one `CONTROLLER/ORCEST_V1` Secret with
purpose `CAPABILITY_SIGNING_PRIVATE_KEY`; normal Secret Provision completion
creates its real Credential Rotation Receipt and Version. Only then may a key
operator `REGISTER` the matching public/private pair at Registry revision 1,
followed by a separate `SELECT` at revision 2. Other credential Secrets and
Projects may be provisioned/registered only after that selection. No synthetic
Secret Version, Receipt, key row, combined register/select, or process-configured
signer satisfies a Stage 0 step. Ordinary Forge schedules remain frozen while
Mode is `MAINTENANCE`; leaving maintenance is a separate authenticated mode
operation after desired control-plane enrollment.

The endpoint is `POST /api/v1/capability-key-operations`. Its terminal body
has exact protocol `orcest.capability-key-operation-result/1`,
`capability_key_operation_id`, `kind`, `status`, and `replayed`; success
additionally has `registry_revision` and nullable `current_issuance_key_id`,
while rejection instead has only the closed `rejection_code`. Stored JSON has
`replayed=false`; transport replay may change only that excluded projection.

## Attempt Claim

An Attempt Claim is the immutable request/response contract that atomically
changes one offered Attempt to claimed. Sensitive bearers are rematerialized
from its identities; they are never stored in this object.

| Field | Requirement |
| --- | --- |
| `attempt_claim_id` | Caller-assigned lowercase UUID and claim idempotency identity. |
| `protocol_version` | Exact literal `orcest.attempt-claim/1`. |
| `attempt_id` / `activity_id` / `attempt_generation` / `offer_outbox_id` | Exact current offered Attempt fence and durable offer source. |
| `worker_id` / `worker_session_id` / `worker_profile` / `worker_build_revision` | Exact authenticated registered claimant/session/profile and non-secret build revision. |
| `request_digest` | Digest of the complete canonical non-secret claim request and bindings. |
| `claimed_at_ms` / `execution_deadline_ms` / `capability_auth_expires_at_ms` | Controller claim time, workflow deadline, and exact deadline plus v1 result-auth grace. |
| `attempt_capability_jti` / `attempt_capability_digest` | Exact signed Attempt-capability identity and domain-separated normalized-claims digest; never bearer bytes. |
| `attempt_capability_signing_key_id` / `attempt_capability_signature_algorithm` | Exact durable capability-signing key and `ED25519` claim value for the Attempt capability. |
| `capability_key_registry_revision` | Exact Capability Key Registry revision whose selected issuance key signed both capabilities. |
| `launch_nonce_id` / `launch_capability_jti` / `launch_capability_digest` | Exact model-backed one-shot launch identities/digest; all `NULL` for deterministic `VERIFY`. |
| `launch_capability_signing_key_id` / `launch_capability_signature_algorithm` | Exact durable capability-signing key and `ED25519` claim value for the launch capability; both `NULL` for deterministic `VERIFY`. |
| `source_access_kind` | `SCOPED_CREDENTIAL` or `BROKERED_ARCHIVE`. |
| `source_read_secret_ref` | Exact versioned source-read Secret Reference for `SCOPED_CREDENTIAL`; `NULL` for `BROKERED_ARCHIVE`. |
| `provider_secret_ref` | Exact versioned provider Secret Reference for a model-backed Claim; `NULL` for deterministic `VERIFY`. |
| `source_access_descriptor_json` / `source_access_descriptor_digest` | Canonical bounded non-secret descriptor binding registered repository, pinned commit, access kind, and access expiry; it contains no bearer, signed URL, or secret. |
| `response_contract_digest` | Digest of the complete stable non-secret claim response fields, including capability JTIs/digests, deadlines, launch fields, source descriptor, Activity inputs, and assignment; sensitive rematerialized values are excluded. |

`attempt_claim_id` and `attempt_id` are each unique. Attempt and Claim have a
reciprocal deferrable binding: `Attempt.attempt_claim_id` names this Claim and
every copied Attempt/fence/session/deadline/digest field matches. Claim
acceptance requires the Attempt still be current `OFFERED`, the authenticated
session/profile/build be allowed, and `controller_now_ms < claim_deadline_ms`.
One writer transaction inserts the Claim, changes Attempt/Activity to
`CLAIMED`/`ACTIVE`, sets the Claim pointer, claimant, deadlines, capability
identities, source access binding, and model launch fields, and marks the offer
outbox delivered.

Within that transaction, the controller resolves the verified current version
of `Project.source_read_secret_id` when scoped source access is used and the
registered provider-account logical Secret when the Attempt is model-backed.
It freezes those exact references on the Claim and Attempt before returning
any scoped material. Rotation cannot rewrite them; a newer version is usable
only by a later Attempt Claim.

The same authenticated worker session, `attempt_claim_id`, and
`request_digest` returns the same immutable non-secret Claim contract and may
rematerialize only still-authorized sensitive values from its exact frozen
Secret versions and normalized signed-capability claims/JTIs/key IDs/expiries.
Capability rematerialization uses the identical canonical claims, `issued_at`,
expiry, JTI, key ID/algorithm, canonical bearer serialization, and
deterministic Ed25519 signature bytes from the original key Secret; it is
replay, never issuance. It never substitutes a newer Secret version, account,
capability claim, or deadline. A `RETIRED` key may reproduce that exact bearer
only within its original endpoint/cryptographic authority window; `REVOKED`
reproduces none. If the frozen version is revoked or unavailable, replay fails that
sensitive subfield closed rather than changing the contract. Strictly before
the execution deadline, the response may include the exact frozen source and
launch/Attempt capabilities. From the execution deadline until
capability-auth expiry, only
the Attempt capability may be rematerialized and the controller enforces its
Result-only reconciliation semantics; source and launch material remain absent.
At or after auth expiry no bearer or secret is rematerialized. Reuse of that
key with a different request/binding is an idempotency conflict. Any request
for an already claimed Attempt under another key or session returns `409
ATTEMPT_ALREADY_CLAIMED` and creates no Claim or lifecycle input. Thus a lost
claim response is replayable, while a second worker can never acquire the
Attempt.

`attempt_capability_digest` is computed over normalized signed Attempt
capability claims, not the bearer serialization. `launch_capability_digest` is
exactly:

```text
"sha256:" + lowercase_hex(SHA256(
  ascii("orcest-launch-capability-claims-v1") || 0x00 ||
  uint64_be(byte_length(canonical_claims_json)) ||
  canonical_claims_json
))
```

`canonical_claims_json` is canonical JSON containing exactly protocol
`orcest.launch-capability/1`, launch capability JTI, Attempt/Activity/
generation, worker/session, launch nonce, runner principal, runner registration
revision, capability-signing key ID, signature algorithm, issued-at time,
execution deadline, and the launch-attestation endpoint audience. Signature and bearer serialization are excluded. The Claim
stores and its response exposes this controller-computed digest. A Launch
Attestation MUST copy it exactly.

## Launch Attestation

A Launch Attestation is the signed, durable evidence that the registered
launch-isolation boundary prepared one fresh, non-resumed workspace, context,
and one-shot invocation identity for one exact claimed Attempt and will enforce
that launch. It is accepted before the provider invocation begins. It proves
launch isolation only; it cannot validate agent output, fill a gate, choose a
transition, or grant publication authority.

| Field | Requirement |
| --- | --- |
| `launch_attestation_id` | Caller-assigned lowercase UUID, primary replay identity, and exact signed source identity. |
| `protocol_version` | Literal `orcest.launch-attestation/1`. |
| `attempt_id` / `activity_id` / `attempt_generation` | Exact current claimed Attempt fence. |
| `worker_id` / `worker_session_id` | Exact authenticated claimant/session. |
| `pool_manager_id` / `runner_principal_id` | Exact registered pool manager and attested runner-shim principal authorized for that Worker Session. |
| `runner_image_digest` / `runner_registration_revision` | Exact allowlisted immutable runner image and server registration revision used for launch. |
| `launch_nonce_id` / `launch_capability_digest` | Exact unconsumed one-shot identity and domain-separated normalized signed-claims digest issued/exposed by the Attempt Claim; the bearer value and signature are never durable. |
| `launch_capability_signing_key_id` / `launch_capability_signature_algorithm` | Exact `CapabilitySigningKey` and `ED25519` value copied from the accepted launch-capability claims. |
| `workspace_instance_id` / `context_instance_id` / `invocation_instance_id` | Fresh caller-assigned lowercase UUIDs for the isolated workspace, conversation/context, and model invocation. |
| `workspace_parent_id` / `context_parent_id` / `invocation_parent_id` | All `NULL` in v1, affirming no copied workspace, resumed context, or parent invocation. |
| `fresh_workspace` / `fresh_context` / `fresh_invocation` | All literal `true`; for invocation this attests a fresh prepared one-shot identity, not that provider execution already began. |
| `prepared_at_ms` / `attested_at_ms` | Bounded preparation and signature times; informational and non-ordering. |
| `runner_signing_key_id` / `runner_signature_algorithm` / `signature` | Exact registered runner Attestation-signing-key revision, code-owned algorithm, and signature over `attestation_digest`; distinct from the controller capability-signing key. |
| `attestation_digest` | Digest of the protocol and every normalized immutable field above except `signature`. |

`launch_attestation_id`, `attempt_id`, `launch_nonce_id`,
`workspace_instance_id`, `context_instance_id`, and `invocation_instance_id`
are each globally unique among accepted Launch Attestations. An identical
authenticated ID/digest replay returns the accepted object; conflicting reuse
is an integrity violation. Acceptance requires the Attempt still be the exact
current `CLAIMED` generation, its worker/session and launch nonce match,
`controller_now_ms < execution_deadline_ms`, the nonce be unconsumed, the
launch capability key ID/algorithm equal the Attempt Claim and pass the
`CapabilitySigningKey` state/verification rules,
runner principal/key/image equal the mapping in the installed Snapshot's
effective `POLICY_JSON` at `runner_registration_revision`, and the signature
verify. That complete mapping and revision are inputs to `policy_hash`; a live
registry edit cannot reclassify an active Attempt. In one writer transaction
the controller inserts the Attestation, conditionally consumes the one-shot
launch capability, and sets `Attempt.launch_attestation_id`.

The controller MUST withhold provider material and permission to start a model
invocation until that transaction commits. The registered runner shim may then
use the scoped material only for the exact attested invocation. A missing,
invalid, parented, resumed, reused, or mismatched attestation is rejected
without an Attempt Result or lifecycle Transition; the claimed Attempt remains
fenced and can self-heal through its ordinary execution deadline or an exact
authenticated worker-loss report. Deterministic `VERIFY` has no model launch,
nonce, or Launch Attestation.

Launch Attestation is the deliberate authentication/idempotency exception to
ordinary Attempt-scoped JSON calls: it authenticates with the one-shot launch
capability, and `launch_attestation_id` plus `attestation_digest` is its replay
identity. It does not carry an Attempt capability or a separate
`idempotency_key`. Acceptance returns one closed response union:

```json
{
  "protocol": "orcest.launch-accepted/1",
  "launch_attestation_id": "lowercase-uuid",
  "attempt_id": "lowercase-uuid",
  "status": "AVAILABLE",
  "provider": {
    "provider": "exact-provider",
    "model": "exact-model",
    "provider_account_ref": "non-secret-account-ref",
    "secret_id": "lowercase-uuid",
    "version": 7,
    "material": "sensitive-attempt-scoped-opaque-value"
  }
}
```

`AVAILABLE` is returned only while the Attempt remains current `CLAIMED` and
`controller_now_ms < execution_deadline_ms`; the non-secret
`provider.secret_id/provider.version` MUST equal the ID/version of the exact
`provider_secret_ref` frozen on the Attempt Claim,
and provider material is rematerialized only from that version and never
stored in the Attestation. A replay at or after the execution deadline, or
after the Attempt is terminal, returns HTTP `200` with the same protocol/identities,
`status = EXPIRED`, and `provider = null`. It proves the
Attestation was accepted but grants no launch or provider authority. Conflicting
Attestation ID/digest reuse remains an integrity error. For this lookup only,
the controller may verify a consumed or workflow-expired capability and use it solely to find an already
accepted exact `launch_attestation_id`/`attestation_digest`; it cannot accept a
new Attestation, consume another nonce, rematerialize provider material, or
write any object. Before cryptographic expiry this is bounded capability
authentication. At or after cryptographic expiry, the same registered
runner/session transport principal may present the original signed token only
as signature-equality proof for the already retained Attestation: an `ACTIVE`
or `RETIRED` retained public verifier must validate it and every signed claim,
JTI, stored launch-capability digest, Attestation ID, and presented/stored
`attestation_digest` must match exactly. This expired-token carve-out is not
capability authentication,
cannot remint a bearer or authorize any mutation/material, and returns only
the stored `EXPIRED`/provider-null projection. `REVOKED` denies both ordinary
authentication and this equality lookup immediately.
An unknown or non-identical request is rejected. Liveness is not a
durable command: it uses monotonic session sequence/current-control semantics
and never this Attestation or Claim replay identity.

## Attempt Terminal Fact

An Attempt Terminal Fact is the immutable non-worker-result input used to
expire or fence an Attempt.

| Field | Requirement |
| --- | --- |
| `attempt_terminal_fact_id` | Controller-assigned UUID and reducer trigger ID. |
| `attempt_id` / `activity_id` / `attempt_generation` | Exact Attempt fence. |
| `kind` | `CLAIM_DEADLINE`, `EXECUTION_DEADLINE`, `WORKER_LOST`, or `RESULT_AFTER_TERMINAL`. |
| `source_kind` / `source_id` | `TIMER_FACT` plus `timer_fact_id`, `RESULT_REQUEST` plus `result_request_id`, or `HEALTH_OBSERVATION` plus `health_observation_id`. |
| `expected_deadline_ms` / `controller_now_ms` | Required for a deadline kind and must prove `controller_now_ms >= expected_deadline_ms` for the matching durable deadline; otherwise `NULL`. |
| `capacity_disposition` | `COMPATIBLE_AVAILABLE` or `NO_COMPATIBLE_AVAILABLE` for `CLAIM_DEADLINE`; otherwise `NULL`. |
| `health_observation_ids_digest` | SHA-256 of the complete ordered Attempt Terminal Fact Health Observation membership for `CLAIM_DEADLINE`, including the canonical empty membership; otherwise `NULL`. |
| `resolved_provider_secret_ref` | Exact current verified Secret version for the Attempt assignment's logical provider account, resolved under the Secret lock for a model-backed `CLAIM_DEADLINE`; `NULL` for credential-free `VERIFY` and every other kind. |
| `controller_mode_revision` / `controller_mode` | Exact Controller Mode projection read in the `CLAIM_DEADLINE` transaction; otherwise both `NULL`. |
| `capability_registry_revision` / `selected_issuance_key_id` | Registry revision is required for every `CLAIM_DEADLINE`. Selected key is non-`NULL` only when the projection names an existing `ACTIVE` key; it is `NULL` for unselected/invalid issuance. Both fields are `NULL` only for another Fact kind. |
| `replacement_offer_disposition` | For `CLAIM_DEADLINE`, `OFFER_ALLOWED`, `MODE_BLOCKED`, or `ISSUANCE_KEY_UNAVAILABLE`, evaluated in that precedence; otherwise `NULL`. |
| `health_observation_id` | Required only for `WORKER_LOST` and must prove the exact claimant/session loss; otherwise `NULL`. |
| `fact_digest` / `recorded_at_ms` | Digest of normalized immutable fields and informational time. |

The kind/source matrix is closed. `CLAIM_DEADLINE` requires a matching
`ATTEMPT_CLAIM_DEADLINE` Timer Fact, one capacity disposition, and the complete
health membership below. `EXECUTION_DEADLINE` requires a matching
`ATTEMPT_EXECUTION_DEADLINE` Timer Fact or the exact `ResultRequest` with
disposition `EXPIRED_CURRENT` that proves the deadline.
`WORKER_LOST` requires
the exact single loss Health Observation through `health_observation_id`, while
its capacity fields and membership remain empty. This Terminal-Fact kind is
only the authenticated pool-loss path; an accepted worker `INFRASTRUCTURE`
Result uses `T(ATTEMPT_RESULT, attempt_id)` and its own Recovery Evidence
instead. `RESULT_AFTER_TERMINAL`
requires a `RESULT_REQUEST` source whose disposition is `ALREADY_TERMINAL`; all
deadline, capacity, and health fields are `NULL`/empty. A late request is bounded audit input and is not
an Attempt Result. `(attempt_id, kind, source_kind, source_id)` is unique. Once
the fact wins the terminal fence, `CLAIM_DEADLINE` and `EXECUTION_DEADLINE` set
the Attempt to `EXPIRED` with their matching terminal reason, while a
pool-loss `WORKER_LOST` Fact sets it to `FAILED` with
`terminal_reason = WORKER_LOST`. An `INFRASTRUCTURE` Result never creates this
Fact and never supplies that terminal reason; its accepted Result sets the
Attempt `FAILED` and its Result-sourced Recovery Evidence enters recovery.
`RESULT_AFTER_TERMINAL` never wins a terminal fence. Once
the Attempt is terminal, any later deadline/loss Fact and every accepted
`ALREADY_TERMINAL` Result Request create exactly one source-unique Fact. Each is
retained as bounded audit evidence and reduces exactly once to a same-state audit
Transition; it cannot change terminal state, counters, or work.
The reducer transition uses `T(ATTEMPT_TERMINAL,
attempt_terminal_fact_id)`; Attempt ID or deadline content alone is not a
sufficient trigger identity.

### Attempt Terminal Fact Health Observation

For `CLAIM_DEADLINE`, persistence freezes the health evidence used by the
capacity classifier as rows
`(attempt_terminal_fact_id, observation_ordinal, health_observation_id)`.
Ordinals are zero-based and contiguous, Health Observation IDs are unique, and
the controller includes at most the highest applicable unexpired observation
for each policy-relevant worker/profile/provider-account/pool scope. Rows sort
by `(scope_kind, scope_id, health_sequence, health_observation_id)` and their
ordered IDs reproduce `health_observation_ids_digest`. No other Attempt
Terminal Fact kind has membership rows.

The code-owned compatibility classifier resolves the model-backed assignment's
logical provider account to its verified current Secret version while holding
the per-Secret lock, stores that exact `resolved_provider_secret_ref`, and then
evaluates only the immutable Attempt assignment, pinned policy, resolved
version, and this membership. It records
`COMPATIBLE_AVAILABLE` exactly when the membership contains an unexpired
`AVAILABLE` worker-profile observation and an unexpired `AVAILABLE`
worker-session observation whose registered pool/profile/session bindings form
one compatible launch target. For a model-backed assignment, the classifier
also selects the latest unexpired observation for its exact provider account
and newly resolved exact provider Secret version when one exists: `UNAVAILABLE`,
`RATE_LIMITED`, or `EXHAUSTED` is disqualifying, while `AVAILABLE` or no
provider-account observation is not. Absence of provider evidence is neutral,
not synthesized availability and not a reason to disqualify an otherwise
healthy target. When no target passes that exact rule it records
`NO_COMPATIBLE_AVAILABLE`. Absence from Redis, a live query performed after
the Fact, or a later Health Observation cannot change that disposition.

The same transaction freezes the Controller Mode and Capability Registry
projections. `OFFER_ALLOWED` requires a mode that permits offer planning and,
because every worker claim requires an Attempt capability, a non-`NULL`
selected `ACTIVE` issuance key at that exact Registry revision. A blocking
mode takes precedence as `MODE_BLOCKED`; otherwise a missing, retired, revoked,
or mismatched selected key is `ISSUANCE_KEY_UNAVAILABLE`. A loaded process key
or Redis flag cannot satisfy this gate.

## Result Request

A Result Request is the single global idempotency registry for every
schema-valid Result submission that the controller admits to one of the five
closed dispositions below. Accepted, upload-expired, stale, and late requests
cannot claim the same key in different tables. The registry is not a general
request or error log: malformed/schema-invalid bodies, authentication or
authorization denials, and semantic conflicts (including
`RESULT_ALREADY_ACCEPTED`) are rejected before Result Request admission and
create no Result Request row. A controller MAY inspect an existing key to
return its exact replay or conflict response, but that lookup never converts a
rejected request into a new registry entry.

| Field | Requirement |
| --- | --- |
| `result_request_id` | Caller `idempotency_key`; lowercase UUID and global primary identity for the Result endpoint. |
| `protocol_version` | Exact literal `orcest.attempt-result/1`. |
| `attempt_id` / `activity_id` / `attempt_generation` | Exact submitted Attempt fence. |
| `claimed_worker_id` / `claimed_worker_session_id` | Exact authenticated submitter bindings. |
| `attempt_capability_signing_key_id` / `attempt_capability_signature_algorithm` / `attempt_capability_digest` | Exact authenticated capability verifier and normalized-claims digest. |
| `result_body_digest` | Digest of the complete canonical submitted Result body, including every referenced output identity/digest and excluding only the transport idempotency key and bearer. |
| `disposition` | Exactly `ACCEPTED`, `UPLOAD_EXPIRED`, `STALE_ATTEMPT`, `EXPIRED_CURRENT`, or `ALREADY_TERMINAL`. |
| `accepted_result_attempt_id` / `accepted_result_created` | For `ACCEPTED`, exact `AttemptResult.attempt_id` plus a boolean recording whether this request created it; otherwise both `NULL`. |
| `candidate_upload_id` | Required only for `UPLOAD_EXPIRED`; exact expired upload named by the submitted Result. |
| `stale_reason` / `current_attempt_generation` | For `STALE_ATTEMPT`, reason is `GENERATION_SUPERSEDED`, `CLAIM_BINDING_CHANGED`, `RUN_BINDING_CHANGED`, or `TERMINAL_BEFORE_DEADLINE`, and current generation is copied when one exists; otherwise both `NULL`. |
| `controller_now_ms` / `execution_deadline_ms` / `capability_auth_expires_at_ms` | Required only for `EXPIRED_CURRENT` or `ALREADY_TERMINAL`, proving `execution_deadline_ms <= controller_now_ms < capability_auth_expires_at_ms` and the fixed grace; otherwise all `NULL`. |
| `attempt_terminal_fact_id` | Required only for `EXPIRED_CURRENT` or `ALREADY_TERMINAL`; exact Fact sourced by this request. Kind is `EXECUTION_DEADLINE` for `EXPIRED_CURRENT` and `RESULT_AFTER_TERMINAL` for `ALREADY_TERMINAL`. |
| `response_http_status` / `response_json` / `response_digest` | Exact bounded canonical non-secret response. `ACCEPTED` is `200`; `UPLOAD_EXPIRED` and `EXPIRED_CURRENT` are `410`; `STALE_ATTEMPT` and `ALREADY_TERMINAL` are `409`. Digest covers status/body except the accepted response's transport-only `replayed` projection. |
| `recorded_at_ms` | Informational record time. |

The tagged union is closed. `UPLOAD_EXPIRED` returns code `UPLOAD_EXPIRED` and
creates no Attempt Result, Candidate, Receipt, Terminal Fact, Recovery
Evidence, or Transition. Before the execution deadline, a validly
authenticated submission whose durable Attempt/generation/claim/Run fence is
already stale records `STALE_ATTEMPT` and exact `stale_reason`, returns the
closed `orcest.error/1` body `{code:"ATTEMPT_STALE", attempt_id,
current_attempt_generation, retryable:false}`, and creates no Attempt Result,
Terminal Fact, Recovery Evidence, or Transition. `EXPIRED_CURRENT` returns
`EXECUTION_DEADLINE_EXCEEDED`, creates its source-unique Terminal Fact, expires
the current Attempt, and performs timeout recovery. `ALREADY_TERMINAL` returns
`ATTEMPT_STALE`; its source-unique `RESULT_AFTER_TERMINAL` Fact is retained as
audit and creates exactly one same-state `T(ATTEMPT_TERMINAL, fact_id)`, with
no counters or other effect. `ACCEPTED` points to the one exact Attempt Result;
semantic replay under a new request ID sets `accepted_result_created = false`
and creates no Result, Receipt, Candidate, Transition, or reducer input. For
`ACCEPTED`, `result_body_digest = AttemptResult.result_digest` exactly.

Authentication, schema validation, and semantic-conflict checks occur before
Result Request admission. An existing key with the exact same
Attempt/session/capability/body bindings returns its stored outcome; a mismatch
returns `409 IDEMPOTENCY_CONFLICT` without revealing the prior request. An
unused key is inserted atomically with its Result or upload/deadline fence and
response only after admission to one of the five dispositions. The accepted response projects
`replayed = false` only when `accepted_result_created = true` on its first
acknowledgement; every later retrieval and every semantic-replay request
projects `true`. That field alone is excluded from `response_digest`.

For either late disposition, the tagged Terminal Fact has
`source_kind = RESULT_REQUEST` and `source_id = result_request_id`; its kind is
selected exactly by the disposition mapping above. The full
body need not be retained after `result_body_digest` and bounded audit metadata
are durable. At or after `capability_auth_expires_at_ms`, an unseen key cannot
authenticate and no Result Request or workflow row is inserted. Timer
reconciliation remains independent.

### Worker endpoint retry identities

An ambiguous transport response never permits a client to invent a second
semantic request. The worker-facing retry contract is closed:

| Operation | Required retry after timeout |
| --- | --- |
| Attempt claim | Same authenticated session, `attempt_claim_id`, and canonical request digest. Another key/session cannot acquire the already-claimed Attempt. |
| Launch Attestation | Same `launch_attestation_id`, `attestation_digest`, and original one-shot launch token. Before crypto expiry it authenticates only exact accepted-object lookup after consumption/workflow expiry. After crypto expiry it authenticates nothing, but the same registered runner/session may use an `ACTIVE`/`RETIRED` retained verifier solely for exact signature-equality lookup of that retained Attestation and the `EXPIRED`/provider-null projection; `REVOKED` denies. |
| Candidate Upload creation | Same Attempt plus `request_idempotency_key` and identical creation body. |
| Candidate content `PUT` | Same `candidate_upload_id` and identical declared content before expiry; at expiry every retry receives the closed upload-expired response without accepting or comparing bytes. |
| Attempt Result | Same global `result_request_id` and exact body/capability bindings. Outside `MAINTENANCE`, a new key may be a semantic replay only through the explicit `ACCEPTED` disposition rule above; in `MAINTENANCE`, only lookup by the already-existing exact key is permitted and every unseen key gets the closed `503` without a ledger row. |
| Credential rotation | Same `credential_rotation_request_id`, request digest, and opaque keyed Secret Store request attestation, strictly before the execution deadline. |
| Liveness | No durable idempotency key or replay ledger exists. After an ambiguous response the worker sends the next strictly greater liveness sequence; the controller derives and returns current control state without extending durable time or authority. |

An endpoint-specific authority deadline still applies to every retry. A
network timeout neither extends that deadline nor allows a new key to bypass a
fence. Read-only source/archive transfer may resume only within the exact
Claim/source descriptor and deadline; it creates no durable command identity.

## Attempt Result

An Attempt Result is the controller-validated terminal report for one claimed
Attempt. It is separate from raw agent output.

| Field | Requirement |
| --- | --- |
| `attempt_id` / `activity_id` / `generation` | Exact Attempt identity and fence. |
| `launch_attestation_id` | Exact accepted Launch Attestation for a model-backed Attempt; `NULL` only for deterministic `VERIFY`. |
| `outcome` | `SUCCEEDED`, `FAILED_RETRYABLE`, `FAILED_PERMANENT`, or `ABSTAINED`. |
| `result_schema_version` | Code-owned schema version. |
| `candidate_id` | Admitted Candidate produced by the Attempt, if any. |
| `receipt_id` | Verification, Review, or Adjudication Receipt produced by the Attempt, if any. |
| `failure_class` / `failure_code` | Required only for `FAILED_RETRYABLE` or `FAILED_PERMANENT`. Class is one closed worker-protocol value; code is a bounded normalized non-secret stable code. |
| `failure_retry_after_ms` | Required only when `failure_class = PROVIDER_RATE_LIMIT`; otherwise `NULL`. It is an absolute Unix epoch in milliseconds supplied as bounded evidence, not a duration and not an extension of the completed Attempt. |
| `failure_evidence_refs` / `failure_evidence_digest` | For a failure, a canonically byte-sorted unique bounded array of typed non-secret evidence references, possibly empty, and the digest of class/code/retry/evidence; otherwise empty and `NULL`. |
| `structured_output` / `output_digest` | Bounded normalized non-secret plan, diagnosis, or other kind-specific output and its digest when no separate Receipt owns it. |
| `summary` | Bounded untrusted text for evidence and projections only. |
| `result_digest` | Required domain-separated digest of the complete canonical semantic Result, including all nullable tagged-union and normalized failure fields, exact launch/Attempt/output bindings, and summary; excludes only the transport request ID and bearer. |
| `accepted_at_ms` | Informational timestamp. |

The `failure_class` enum is closed to:

```text
INFRASTRUCTURE
PROVIDER_UNAVAILABLE
PROVIDER_RATE_LIMIT
INCOMPATIBLE_WORKER
INVALID_AGENT_OUTPUT
VALIDATION_FAILURE
CREDENTIAL_UNAVAILABLE
SOURCE_READ_FAILED
VERIFICATION_ERROR
BASE_CONFLICT
POLICY_DENIED
SPECIFICATION_CONFLICT
MISSING_AUTHORITY
INTEGRITY_FAILURE
```

Each class's allowed Activity/outcome and reducer-category mapping is the
closed table in [Worker protocol](worker-protocol.md#failure-results). The
controller stores `failure_code` as the worker protocol's bounded canonical
UTF-8 value without provider display text, stack traces, or secret material.
`failure_evidence_refs` uses only the protocol's typed non-secret reference
forms. Array ordering and uniqueness are validation requirements rather than
presentation normalization after acceptance.

`INFRASTRUCTURE` is the worker-Result source of the existing `WORKER_LOST`
recovery category: an accepted Result sets its Attempt to `FAILED` through
`T(ATTEMPT_RESULT, attempt_id)`, returns the Activity to `PLANNED`, and appends
AttemptResult-sourced Recovery Evidence. It does not create an Attempt
Terminal Fact or set `terminal_reason = WORKER_LOST`. The only other source of
that category is the authenticated pool-loss path in [Worker Loss Report](#worker-loss-report),
whose `WORKER_LOSS_REPORT -> WORKER_SESSION/LOST -> WORKER_LOST` chain creates
Terminal-Fact-sourced Recovery Evidence and is the only path that writes
`terminal_reason = WORKER_LOST` without a Result. No other disappearance,
capacity, lease, or Redis signal is a `WORKER_LOST` source.

For `PROVIDER_RATE_LIMIT`, the installed effective policy supplies positive
`max_provider_rate_limit_wait_ms` (v1 default `86_400_000`, server maximum
`604_800_000`; repository policy may only lower it). From the Result's
controller `accepted_at_ms`, the reducer computes exactly:

```text
rate_limit_wait_until_ms = min(
  max(failure_retry_after_ms, accepted_at_ms),
  accepted_at_ms + max_provider_rate_limit_wait_ms
)
```

That value is copied to `RecoveryEvidence.next_eligible_at_ms` and the
resulting `RATE_LIMIT` Wait Condition's `not_before_ms`. The old Attempt's
execution deadline is solely its Result-acceptance fence and does not clamp a
later recovery wait. Overflow, a non-integer/negative timestamp, or a value
outside the wire bound is an invalid Result; a past timestamp clamps to
`accepted_at_ms` and permits immediate deterministic recovery.

`attempt_id` and `(activity_id, generation)` MUST each be unique in accepted
results. Duplicate submissions with the same `result_digest` return the
existing accepted Result and its referenced objects;
different submissions for the same key are rejected and audited. An Attempt
Result cannot directly request a Run state. A valid result maps `SUCCEEDED` to
Attempt state `SUCCEEDED`, either failure outcome to `FAILED`, and `ABSTAINED`
to `ABSTAINED`. Activity `SUCCEEDED` is set in the same transaction only when
the required output is semantically filling; a same-commit
`REPEATED_NON_PROGRESS` artifact is the explicit audit exception described
above and terminally fails its semantic Activity without reopening it.
`EXPIRED` and `SUPERSEDED` are controller-derived Attempt states, not worker
result outcomes.

For a `VERIFY` Activity, a `PASS` or `FAIL` Verification Receipt is required by
a `SUCCEEDED` Result: both are valid completed tool outputs, although only
`PASS` satisfies the gate. An `ERROR` Verification Receipt is instead required
by a `FAILED_RETRYABLE` Result with `failure_class = VERIFICATION_ERROR`; the
Attempt becomes `FAILED`, the Activity is returned to `PLANNED`, and a higher
Attempt generation may retry that same frozen verification Activity only
through Recovery Evidence and the `PLANNED -> READY` offer transition. This is
the sole failure-result case that carries a Receipt. Other failure Results
carry neither a Receipt nor Candidate.

The Activity/outcome payload union is closed:

| Activity and outcome | Required payload | Forbidden payload |
| --- | --- | --- |
| `PLAN` or `REPLAN` + `SUCCEEDED` | `structured_output` with exact `orcest.plan/1` and matching `output_digest` | Candidate and Receipt |
| `DIAGNOSE` + `SUCCEEDED` | `structured_output` with exact `orcest.diagnosis/1` and matching `output_digest` | Candidate and Receipt |
| `BUILD`, `REMEDIATE`, `PR_REMEDIATE`, or `REBASE` + `SUCCEEDED` | Exact admitted `candidate_id` | Receipt and `structured_output` |
| `VERIFY` + `SUCCEEDED` | Exact `PASS` or `FAIL` Verification Receipt | Candidate and `structured_output` |
| `VERIFY` + `FAILED_RETRYABLE/VERIFICATION_ERROR` | Exact `ERROR` Verification Receipt | Candidate and `structured_output` |
| `REVIEW` or `ADJUDICATE` + `SUCCEEDED` | Exact schema-valid filling Receipt | Candidate and `structured_output` |
| `REVIEW` or `ADJUDICATE` + `ABSTAINED` | Exact schema-valid non-filling Receipt | Candidate and `structured_output` |
| Any other worker failure outcome | Exact normalized failure fields and summary only | Candidate, Receipt, and `structured_output` |

Every `structured_output` field is part of the canonical Result body and
result digest. Credential rotation is a separate endpoint and durable
Credential Rotation Request; Attempt Result has no
`credential_rotation_receipt_id` or other rotation binding.

The failure union is closed by [Worker protocol](worker-protocol.md): unknown
classes, a missing class/code, a retry time outside `PROVIDER_RATE_LIMIT`,
duplicate/unsorted/untyped evidence, secret-bearing evidence, or a failure
field on `SUCCEEDED`/`ABSTAINED` is rejected before any Result Request row is
created. A semantically different result for an Attempt that already has an
accepted Result is `RESULT_ALREADY_ACCEPTED`, a pre-registry semantic conflict;
it creates no Result Request row. `result_digest` is the sole semantic replay
comparator used by the
global Result Request registry; implementations do not recompute replay
identity from mutable receipts, Candidates, or summaries later.

`ABSTAINED` is valid only for a Review or Adjudication Attempt carrying the
corresponding schema-valid Receipt with controller-derived
`fills_slot = false`. It preserves audit evidence but does not complete the
Activity's semantic slot. The reducer returns the Activity to `PLANNED`, then
either offers a higher Attempt generation using the next policy-eligible
assignment or leaves it planned while the Run waits for capacity or evidence.
It never counts as verification,
approval, a blocker resolution, or Activity success.

## Candidate Upload

A Candidate Upload is durable staged transfer state for one Candidate-producing
Attempt. It is not a Candidate or workflow result.

| Field | Requirement |
| --- | --- |
| `candidate_upload_id` | Controller-assigned UUID; wire field and URL segment `upload_id`. |
| `request_idempotency_key` | Caller UUID for upload creation; unique with `attempt_id`. |
| `attempt_id` / `activity_id` / `attempt_generation` | Exact Candidate-producing Attempt fence. |
| `declared_digest` / `declared_bytes` / `proposed_tip` | Exact bounded creation request. |
| `computed_digest` / `computed_bytes` / `verified_tip` | Controller validation result; conditional on state. |
| `state` | `RECEIVING`, `VALIDATED`, `PROMOTED`, `CONSUMED`, or `EXPIRED`. |
| `expires_at_ms` | Immutable expiry no later than the Attempt execution deadline; equality is expired. |
| `promoted_bundle_digest` / `promoted_storage_key` / `promoted_at_ms` | Conditional durable promotion fields defined by persistence. |
| `consumed_candidate_id` | Exact Candidate only for `CONSUMED`; otherwise `NULL`. |

Every operation that could validate, promote, consume, or finalize the upload
requires `controller_now_ms < expires_at_ms`. The single writer atomically
changes an unused upload to `EXPIRED` at equality or later. Content `PUT` and a
Result Request naming that expired upload both return HTTP `410` with exactly:

```json
{
  "protocol": "orcest.candidate-upload-expired/1",
  "upload_id": "lowercase-uuid",
  "state": "EXPIRED",
  "code": "UPLOAD_EXPIRED",
  "expires_at_ms": 1787778000000
}
```

The body is derived only from immutable upload identity/expiry and durable
state. A content `PUT` observes/sets `EXPIRED` before accepting or comparing
new bytes and therefore returns this same response even when its body differs.
A Result submission stores the same body/status in its `ResultRequest` with
disposition `UPLOAD_EXPIRED`; it creates no Attempt Result or Candidate.
`UPLOAD_EXPIRED` is non-retryable for this upload, but the reducer may recover
the Activity through its ordinary Attempt deadline/loss path.

For `EXPIRED`, `consumed_candidate_id`, `promoted_bundle_digest`, and
`promoted_storage_key` are `NULL`. Controller-computed digest/size/tip remain
non-`NULL` only if validation had already succeeded; `promoted_at_ms` remains
non-`NULL` only as audit proof that promotion occurred before expiry. Clearing
the two promoted live-reference fields and entering `EXPIRED` is atomic under
the storage mutation lock; the unreferenced Artifact Object follows ordinary
orphan grace and cannot be revived through this upload.

## Candidate

A Candidate is one immutable, controller-admitted proposed Git commit artifact.

| Field | Requirement |
| --- | --- |
| `candidate_id` | Controller-assigned UUID. |
| `run_id` | Owning Run. |
| `specification_generation` | Snapshot generation under which it was produced. |
| `candidate_generation` | Strictly increasing within the Run. |
| `provenance_kind` | `WORKER_ATTEMPT` or `FORGE_IMPORT`. |
| `producing_activity_id` | Exact Candidate-producing `BUILD`, remediation, `REBASE`, or controller `IMPORT` Activity. |
| `producing_attempt_id` / `attempt_generation` | Exact accepted worker producer for `WORKER_ATTEMPT`; otherwise `NULL`. |
| `import_forge_observation_id` | Exact external-head Forge Observation for `FORGE_IMPORT`; otherwise `NULL`. |
| `parent_candidate_id` | Prior Candidate when this is a remediation, or `NULL`. |
| `base_commit` | Exact commit against which the Candidate was built. |
| `commit` | Controller-derived single proposed tip `{object_format, oid}`. |
| `bundle_digest` | SHA-256 digest of the stored Git bundle bytes. |
| `bundle_size` | Verified byte size. |
| `storage_key` | Non-secret Candidate Store locator. |
| `admission_version` | Candidate validation algorithm version. |
| `persisted_at_ms` | Time persistence and fsync completed. |

`candidate_id` is the durable object identity. `(run_id,
specification_generation, commit.object_format, commit.oid)` is the unique
content identity, and `bundle_digest` addresses the stored representation.
`(run_id, candidate_generation)` MUST also be unique. The controller MUST
derive the commit and digest rather than trusting worker claims.

Candidate provenance is an exclusive union. `WORKER_ATTEMPT` requires a valid
producing Attempt and forbids `import_forge_observation_id`. `FORGE_IMPORT`
requires a controller-class `IMPORT` Activity plus an exact, current
`import_forge_observation_id` and forbids worker Attempt fields. An import
fetches the observed commit through the controller, constructs and validates a
one-tip bundle under the ordinary Candidate admission limits, and persists the
artifact before the Candidate row. Importing external code grants it no trust;
the new Candidate invalidates old receipts and passes the full configured gate.

When a later, normally fenced and validated producing Attempt proposes a commit
whose unique content identity already exists, Candidate admission resolves to
the existing `candidate_id`; it does not create another Candidate generation or
replace the existing bundle representation merely because the worker uploaded
different bytes. The later Attempt Result may reference that Candidate only
after the ordinary generation, capability, base, object, and digest checks
succeed. The producer Attempt becomes `SUCCEEDED` because its artifact is
valid, but a remediation Activity that required a replacement Candidate becomes
`FAILED` with `REPEATED_NON_PROGRESS`. The reducer leaves Candidate generation
and prior receipt eligibility unchanged and enters its fingerprint-driven
recovery or diagnosis path. It MUST NOT offer another Attempt generation for
that completed producer Activity.

The Candidate Store write and fsync happen before the Candidate row commits.
An orphan stored bundle is permitted and collectible. A live Candidate row
whose bytes are absent or whose digest does not match is forbidden and enters
integrity recovery. Candidate rows and bytes are immutable.

Only one Candidate is current for a Run at a time. Selecting a new current
Candidate invalidates every prior Candidate's verification, review,
adjudication, and consensus receipts for gating purposes; the rows remain audit
history.

An explicit policy-only Snapshot installation is the sole case in which a
Candidate produced under an earlier `specification_generation` may be retained
as `policy_replan_candidate_id`. It is not current or gate-eligible: the old
Plan, Activities, receipts, and Decisions are ineligible, and a mandatory
`REPLAN` under the new Snapshot/policy receives the Candidate only as immutable
context. If the accepted new Plan and reducer recheck prove
`specification_hash`, `workflow_hash`, `base_ref`, `base_commit`, and Candidate
content/commit are still identical, the reducer may reselect that Candidate as
current and run full verification/review under the new Plan and policy without
a `BUILD`. Any mismatch clears the context and requires the ordinary build or
replan path. New review subjects always derive from the accepted new Plan,
never implicitly from the old Plan.

## Verification Receipt

A Verification Receipt is structured evidence produced by deterministic tools
against one exact Candidate.

| Field | Requirement |
| --- | --- |
| `verification_receipt_id` | Controller-assigned UUID. |
| `candidate_id` / `candidate_commit` | Exact Candidate binding. |
| `activity_id` / `attempt_id` / `attempt_generation` | Producer binding. |
| `profile_id` / `profile_hash` | Pinned verification profile and normalized command/environment hash. |
| `outcome` | `PASS`, `FAIL`, or `ERROR`. |
| `checks` | Ordered structured command results and artifact digests. |
| `evidence_digest` | Digest of the normalized receipt. |

`ERROR` is not `PASS` and is handled by recovery rather than by weakening the
gate. Full command isolation and evidence rules belong to [review and
consensus](review-and-consensus.md).

The exact Verification Receipt identity is its Candidate plus producing
Activity/Attempt generation and `profile_id = default`/`profile_hash`; v1 has
no separate verification-set generation.

## Review Receipt

A Review Receipt is one independent reviewer's structured output against one
exact Candidate.

| Field | Requirement |
| --- | --- |
| `review_receipt_id` | Controller-assigned UUID. |
| `candidate_id` / `candidate_commit` | Exact Candidate binding. |
| `activity_id` / `attempt_id` / `attempt_generation` | Producer binding. |
| `panel_round` / `reviewer_slot` | Required panel position. |
| `role` | Configured independent review role. |
| `subject_refs_digest` | Exact digest of the producing Activity's complete ordered Activity Review Subject membership. |
| `context_digest` | Exact controller-frozen review subject/evidence context from the Activity and claim. |
| `execution_profile_id` / `worker_profile` / `provider` / `model` / `provider_account_ref` | Trusted exact execution assignment copied from the producing Attempt. |
| `provider_family` / `model_family` | Exact immutable independence-classification IDs copied from the producing Attempt. |
| `classification_revision` | Exact immutable classification revision copied from the producing Attempt. |
| `worker_id` / `worker_session_id` | Exact authenticated claimant/session copied from the Attempt. |
| `launch_attestation_id` | Exact accepted Launch Attestation copied from the producing Attempt and Result. |
| `assessments` | Controller-normalized ordered structured entries `{subject_ref, outcome, evidence_refs}` for every member of the closed frozen Activity Review Subject list; outcome is `SATISFIED`, `VIOLATED`, or `UNVERIFIABLE`. |
| `verdict` | `APPROVE`, `BLOCK`, or `ABSTAIN`. |
| `abstention_code` | `NULL` unless verdict is `ABSTAIN`; then one protocol-allowed abstention code. |
| `fills_slot` | Controller-derived boolean; true only for a schema-valid `APPROVE` or `BLOCK` Receipt satisfying the slot's identity and independence constraints. |
| `findings` | Structured evidence with stable finding IDs, severity, location, and reproduction instructions when applicable. |
| `receipt_digest` | Digest of the normalized receipt. |

`attempt_id` MUST be unique among Review Receipts. At most one Receipt with
`fills_slot = true` may exist for `(candidate_id, panel_round, reviewer_slot)`.
An `ABSTAIN` Receipt has `fills_slot = false`, remains audit evidence, and does
not prevent a replacement Attempt from filling that slot. A missing receipt,
invalid schema, timeout, provider failure, or generic uncertainty produces no
valid Receipt and follows recovery. Only a schema-valid, policy-permitted
intentional abstention produces an `ABSTAIN` Receipt.
All context and execution/claim identity fields are controller-added and MUST
equal the immutable Assignment/Activity/Attempt rows; worker JSON cannot assert
or alter them. `assessments` is persisted as bounded canonical structured data
in the frozen subject order; duplicate or unknown subjects are invalid, and
each evidence-reference list is duplicate-free and sorted. `receipt_digest`
covers the complete assessments plus all controller-added fields, including
both family IDs, `classification_revision`, and `launch_attestation_id`.
Receipt equality and
independence use these persisted values, not a later profile lookup.

## Adjudication Receipt

An Adjudication Receipt is one independent adjudicator's structured resolution
of disputed findings on one exact Candidate. It does not rewrite or delete the
Review Receipts being adjudicated.

| Field | Requirement |
| --- | --- |
| `adjudication_receipt_id` | Controller-assigned UUID. |
| `candidate_id` / `candidate_commit` | Exact Candidate binding. |
| `activity_id` / `attempt_id` / `attempt_generation` | Producer binding. |
| `panel_round` / `adjudication_round` / `adjudicator_slot` | Review panel being adjudicated and required independent adjudicator position. |
| `role` / `subject_refs_digest` / `context_digest` | Exact configured adjudicator role, ordered Activity Review Subject digest, and controller-frozen dispute/evidence context from the Activity and claim. |
| `execution_profile_id` / `worker_profile` / `provider` / `model` / `provider_account_ref` | Trusted exact execution assignment copied from the producing Attempt. |
| `provider_family` / `model_family` | Exact immutable independence-classification IDs copied from the producing Attempt. |
| `classification_revision` | Exact immutable classification revision copied from the producing Attempt. |
| `worker_id` / `worker_session_id` | Exact authenticated claimant/session copied from the Attempt. |
| `launch_attestation_id` | Exact accepted Launch Attestation copied from the producing Attempt and Result. |
| `disputed_finding_ids` | Canonically sorted non-empty set presented to the adjudicator. |
| `dispositions` | For a non-abstaining Receipt, exactly one structured disposition per disputed finding: `SUSTAIN`, `OVERRULE`, or `INCONCLUSIVE`; empty for an abstention. |
| `abstention_code` | `NULL` for a disposition Receipt; otherwise one policy-permitted Review abstention code. |
| `fills_slot` | Controller-derived boolean; true only when the Receipt is schema-valid, `abstention_code IS NULL`, and every assigned dispute has a decisive `SUSTAIN` or `OVERRULE` disposition. |
| `evidence_refs` | Structured deterministic or location-bound evidence supporting each disposition. |
| `new_findings` | Bounded structured findings using the Review finding schema; empty when none were independently discovered. |
| `receipt_digest` | Digest of the normalized Receipt. |

`attempt_id` MUST be unique among Adjudication Receipts. At most one Receipt
with `fills_slot = true` may exist for `(candidate_id, panel_round,
adjudication_round, adjudicator_slot)`. A Receipt containing `INCONCLUSIVE` has
`fills_slot = false` and does not prevent replacement. A valid adjudicator
abstention has an allowed `abstention_code`, no dispositions, and likewise does
not prevent replacement. `SUSTAIN` preserves the blocker, `OVERRULE` makes it
eligible for deterministic consensus removal under policy, and `INCONCLUSIVE`
resolves nothing. No disposition is an approval, and free-form lifecycle advice
is ignored.
The controller, not worker output, adds the role, context, execution assignment,
both family IDs, and claimant/session fields and validates exact equality with
the immutable Assignment/Activity/Attempt before accepting the Receipt.
`receipt_digest` covers every controller-added field, including both family
IDs and `classification_revision`.

## Consensus Decision

A Consensus Decision is the reducer's deterministic aggregation over one exact
set of Verification and Review Receipts.

| Field | Requirement |
| --- | --- |
| `consensus_decision_id` | Controller-assigned UUID. |
| `candidate_id` / `candidate_commit` | Exact Candidate binding. |
| `panel_round` | Exact frozen review panel. |
| `policy_hash` | Pinned normalized acceptance policy. |
| `input_receipt_ids` | Canonically sorted immutable set of applicable `default` Verification and frozen Review Receipt IDs. Adjudication happens after this Decision and is not retroactively added. |
| `outcome` | `APPROVED`, `REMEDIATE`, or `ADJUDICATE`. |
| `unresolved_finding_ids` | Canonically sorted blockers, if any. |
| `decision_digest` | Digest of normalized inputs and outcome. |

`(candidate_id, panel_round)` MUST be unique. The reducer
sorts by stable receipt and finding keys before aggregation.
Receipt arrival order MUST NOT affect the decision. A summarizer MAY produce a
projection after the decision; it cannot choose the outcome or discard a
finding.

Missing reviewer/adjudicator capacity is handled outside `AGGREGATING`—in
`REVIEWING` or `ADJUDICATING`—and creates no Consensus Decision. Once written, a
Consensus Decision for a panel is immutable and is never regenerated in that
panel. Successful adjudication that removes all blockers opens a fresh full
`panel_round` rather than revising the old Decision.

The default pre-publication policy is two independent valid approvals, all
required deterministic verification passing, and no unresolved blocker on the
exact Candidate. Repository policy may strengthen but not dynamically weaken
this threshold.

## Publication

A Publication is the Run's reconciled external branch and Change Request.

| Field | Requirement |
| --- | --- |
| `publication_id` | Controller-assigned UUID; one per Run. |
| `run_id` | Unique owning Run. |
| `candidate_id` / `approved_commit` | Exact Candidate authorized for the current publication operation. |
| `effect_generation` | Positive monotonically increasing fence for Publication mutations, starting at `1`. |
| `deterministic_branch` | Adapter-normalized branch name derived from Project and Run identity. |
| `run_marker` | Stable opaque marker containing `run_id` and publication identity, but no secret. |
| `expected_remote_commit` | Compare-and-swap expectation, or explicit nonexistence for initial creation. |
| `change_request_external_id` | Opaque adapter identity once observed. |
| `observed_remote_commit` | Last reconciled branch/Change Request commit. |
| `initial_link_search_revision` / `initial_link_set_digest` / `initial_link_cardinality` / `initial_link_retained_external_id` | Latest complete-marker proof before initial durable linkage. Revision/digest/actionable cardinality (`ZERO`, `ONE`, or `MULTIPLE`) are non-`NULL` after the first proof; cardinality counts only live open/unmerged members. Retained ID is the sole or bytewise-lowest live ID for ordinary `ONE`/`MULTIPLE`, the selected positive merged terminal ID at any cardinality, the selected positive closed terminal ID for `ZERO`, and otherwise `NULL`. They remain audit projections after linkage/termination. |
| `initial_link_terminal_state` / `initial_link_terminal_search_observation_id` / `initial_link_terminal_member_ordinal` | All non-`NULL` when a complete search selects a `POSITIVE` owned terminal member before ordinary linkage. State is `MERGED` or `CLOSED`; the Observation is the exact current `CHANGE_REQUEST_SEARCH_RESULT`, and ordinal selects its canonical terminal member. A `MERGED` selection is permitted at every live cardinality and creates the exact terminal cleanup reservation below; `CLOSED` selection requires `ZERO` live. Otherwise all `NULL`. |
| `terminal_duplicate_cleanup_reservation_id` | Exact Reservation created by a positive merged-terminal selection, or `NULL`; reciprocal and immutable once set. |
| `last_duplicate_reconciliation_fact_id` / `last_duplicate_search_revision` / `last_duplicate_set_digest` | Latest `REDUNDANT_PUBLICATIONS_PROVEN` or `NO_ACTIONABLE_DUPLICATE` proof and exact complete-search pair; all nullable together before duplicate reconciliation. |
| `state` | `PLANNED`, `BRANCH_OBSERVED`, `CHANGE_REQUEST_OBSERVED`, `ACTIVE`, or `CLOSED`. |
| `last_observation_id` | Latest ordered Forge Observation applied. |

`run_id` and `change_request_external_id` (when non-null) MUST each be unique.
Initial publication and later remediation are idempotent workflows, not atomic
calls. Before any retry the adapter searches by deterministic branch and Run
marker. A discovered side effect must match the expected Project and commit or
fail closed.

Before `change_request_external_id` is first set—or any merge/close fact can
become terminal authority—the current `INITIAL` Effect MUST complete a
`COMPLETE_MARKER_SEARCH` for its exact Project/ref/Run marker. The result
freezes two separately ordered memberships: live open/unmerged matches and
terminal closed/merged matches. `initial_link_cardinality` counts only the live
membership. Every member also freezes the closed mechanical ownership proof;
an ownership-unknown result is not silently considered owned.

Search reduction uses fixed precedence. First, if any terminal merged member
has `ownership_status = POSITIVE`, choose the bytewise-lowest such ID regardless
of live cardinality, establish its exact association/proof, terminalize
`MERGED`, and create the Terminal Duplicate Cleanup Reservation for every live
member. Second, absent that stronger merged fact, any `INCOMPATIBLE` member
enters the exceptional ownership-conflict path. Third, any `INCOMPLETE` member
causes autonomous fresh evidence collection/backoff with no association or
terminal outcome. Only when every member is positive and none is merged does
the live-cardinality routing below apply.

`ZERO` live with no terminal member permits only the ordinary exact
`CHANGE_REQUEST_SEARCH` and optional idempotent create path, followed by a
fresh `COMPLETE_MARKER_SEARCH`; create/search output never links directly.
`ONE` live freezes that sole ID, requires a fresh exact-object observation, and
then permits linkage even when positive closed terminal audit members also
exist. `MULTIPLE`
live freezes the bytewise-lowest live ID as a retained candidate but does not
link it; the controller must finish duplicate reconciliation/cleanup one object
at a time and repeat the complete search until it proves `ONE` live.

`ZERO` live with one-or-more positive closed terminal members MUST NOT create.
After the stronger merged/incompatible/incomplete precedence above, the
controller deterministically selects the bytewise-lowest stable `CLOSED`
member; it stores the selected stable ID/head plus the exact search
Observation/member ordinal as the Publication association/proof,
completes/fences the `PUBLISH` Effect, sets Publication `CLOSED`, and reduces
the Run directly to `CLOSED`. Positive closed terminal members remain audit-only
while any live member exists. A positive merged member, by contrast, is final
authority at every live cardinality and uses the Reservation path rather than
waiting for live convergence. After a fresh `ONE` live proof, a fresh
exact-object poll supplies later terminal authority; an old trigger is never
reused.

`CHANGE_REQUEST_OBSERVED` is the ordinary authority cutoff, but a durable
`CHANGE_REQUEST_CREATE/REQUEST_READY` or `AMBIGUOUS` checkpoint means the
external side effect is already possible even while Publication remains
`BRANCH_OBSERVED`. Immediate pre-CR cancellation is legal only when no such
checkpoint exists and a current `CHANGE_REQUEST_SEARCH/OBSERVED_ABSENT`
checkpoint is backed by an exact `CHANGE_REQUEST_ABSENT` Forge Observation, or
when no Publication/create workflow exists at all. `REF_ABSENT` proves only
that the deterministic Git ref is absent and can never satisfy this Change
Request predicate. Otherwise cancellation stores the generic Run
cancellation source and first reconciles the stable create request/search.
At or after an observed or possible Change Request, the cancellation intent
reserves the run-owned Change Request and drives an idempotent
`CLOSE_PUBLICATION` operation. A pre-discovery Activity binds only the stable
create/search identity. Discovery atomically supersedes it and plans a new
Activity bound to the exact Change Request Observation and head; a later head
observation repeats that replacement rather than mutating Activity inputs. At
most one cleanup Activity is current. The Run remains nonterminal until
an exact authenticated unmerged-close observation yields `CANCELLED` or a
merge observation yields `MERGED`. The controller must re-prove stable Change
Request ID, marker, deterministic ref, head, and effect generation before each
close attempt. If reconciliation proves the stable create request produced no
Change Request, the controller may terminally cancel through its exact
Controller Operation Fact. It never closes an unverified or mismatched object.

Every publication intent, outbox effect, adapter request, and resulting Forge
Observation binds `(publication_id, effect_generation)`. Planning a different
authoritative publication intent increments the generation before emitting an
effect. Ordered suboperations of the same immutable intent retain its
generation. A response from an older effect generation is audit evidence only
and cannot update Publication state. Retrying the same generation preserves its
stable idempotency identity and expected remote revision.

After initial association, the ordinary nonterminal exception to treating
`change_request_external_id` as fixed is autonomous post-link duplicate repair:
reducing a current `REDUNDANT_PUBLICATIONS_PROVEN` Fact may atomically replace
it with that Fact's bytewise-lowest retained live ID and matching
head/observation, without changing `effect_generation`. A pre-link Fact stores
only its retained-candidate proof and MUST NOT set or replace the association;
only fresh `ONE`-live exact-object linkage, positive-closed zero-live
selection, or positive-merged selection at any cardinality may establish it.
Post-link replacement is legal only because the complete frozen
member proof establishes the same Project, deterministic ref, valid Run and
Publication marker, and head. It grants no publication mutation authority;
every redundant close remains a separately fenced Activity. No webhook,
worker output, repository policy, or partial search may change the association.
The distinct terminal exception is a complete-search positive-owned merged
member: its bytewise-lowest merged ID becomes the permanent terminal
association before the post-terminal Reservation begins. Neither cleanup nor
later evidence can change it again.

### Publication Effect

A Publication Effect is the immutable intended external mutation workflow for
one Publication generation. It preserves historical generation targets while
`Publication.effect_generation` advances.

| Field | Requirement |
| --- | --- |
| `publication_id` / `effect_generation` | Composite identity; generation starts at `1` and increases by exactly one for each new intent. |
| `activity_id` | Exact controller `PUBLISH` Activity that owns this intent. |
| `mode` | `INITIAL` or `UPDATE`. |
| `candidate_id` / `desired_commit` | Exact approved Candidate and commit to publish. |
| `expected_remote_commit` | Compare-and-swap expectation, or explicit nonexistence for initial ref creation. |
| `publication_secret_ref` | Exact verified-current version of the owning Project's logical `publication_secret_id`, frozen when this Effect is created and used by every retry of this generation. |
| `base_ref` / `base_commit` | Exact reviewed target base/ref observation for this intent. |
| `base_movement_policy` | Exact `REBASE_BEFORE_PUBLICATION`, `PIN`, or `SUPERSEDE_AT_BOUNDARY` value copied from the installed Snapshot policy. |
| `operation_digest` | Digest of all immutable intent fields and adapter-normalized Project/ref/Change Request target. |
| `created_transition_sequence` / `created_at_ms` | Creating Transition and informational time. |

The Publication Effect row, Publication update, controller Activity, and
publication outbox intent commit atomically. Initial branch creation and Change
Request creation are ordered suboperations of one `INITIAL` intent, not reasons
to change its generation; their idempotency identities add a code-owned
suboperation kind to `(publication_id, effect_generation)`. An `UPDATE` intent
has one expected and desired head. A changed authoritative field creates the
next generation and a new row. Historical outboxes, observations, Human
Boundaries, and resolutions reference this immutable composite identity, never
a mutable-current-row composite foreign key.

Effect creation resolves `publication_secret_ref` under the Secret lock and
requires its Secret ID equal `Project.publication_secret_id`. Rotation never
changes an existing Effect or its stable adapter request. If that version is
rejected/revoked, the Effect is superseded and recovery waits for the logical
Secret with `minimum_version = publication_secret_ref.version + 1`; after wake,
a higher Effect generation freezes the newly verified current version.

### Publication Effect Checkpoint

A Publication Effect Checkpoint is one immutable ordered fact in a resumable
Publication Effect. It prevents controller restart or an ambiguous adapter
response from turning `PUBLISH` into a blind retry.

| Field | Requirement |
| --- | --- |
| `publication_effect_checkpoint_id` | Controller-assigned UUID. |
| `publication_id` / `effect_generation` | Exact immutable Publication Effect. |
| `checkpoint_sequence` | Strictly increasing within that Publication Effect. |
| `suboperation_kind` | `BASE_READ_PRE`, `REF_READ`, `REF_CREATE`, `REF_UPDATE`, `COMPLETE_MARKER_SEARCH`, `CHANGE_REQUEST_SEARCH`, `CHANGE_REQUEST_CREATE`, `BASE_READ_POST`, or `COMPLETE`. |
| `status` | `REQUEST_READY`, `OBSERVED_ABSENT`, `OBSERVED_SATISFIED`, `AMBIGUOUS`, `BASE_MISMATCH`, `CAS_MISMATCH`, or `COMPLETED`, valid only in the closed matrix below. |
| `request_idempotency_key` | Stable UUID committed before a mutation request and reused by retries; otherwise `NULL`. |
| `forge_observation_id` / `observed_external_revision` | Exact durable observation/revision proving an external result, or `NULL` before one exists. |
| `checkpoint_digest` | Digest of the Publication Effect identity and normalized checkpoint fields. |
| `recorded_at_ms` | Informational time. |

`(publication_id, effect_generation, checkpoint_sequence)` is unique. Replay
of the same request or observation source is idempotent; equal checkpoint
content from a distinct source is not globally deduplicated. Before external
mutation, `REQUEST_READY` and its idempotency key commit with the applicable
outbox work. After a response, an observation-backed checkpoint commits before
progress advances. `AMBIGUOUS` requires read reconciliation using the same key
and expected revision. `CAS_MISMATCH` never authorizes overwrite.

The checkpoint matrix is closed; “required” means non-`NULL` and “forbidden”
means `NULL`:

| Suboperation | Allowed status | Request key | Forge Observation / external revision |
| --- | --- | --- | --- |
| `BASE_READ_PRE` | `OBSERVED_SATISFIED`, `BASE_MISMATCH` | forbidden | required |
| `REF_READ` | `OBSERVED_ABSENT`, `OBSERVED_SATISFIED` | forbidden | required, including the adapter's normalized nonexistence revision |
| `REF_CREATE` | `REQUEST_READY` | required | forbidden |
| `REF_CREATE` | `AMBIGUOUS` | required | forbidden |
| `REF_CREATE` | `OBSERVED_SATISFIED`, `CAS_MISMATCH` | optional only when reconciliation adopted an effect without issuing this request | required |
| `REF_UPDATE` | `REQUEST_READY` | required | forbidden |
| `REF_UPDATE` | `AMBIGUOUS` | required | forbidden |
| `REF_UPDATE` | `OBSERVED_SATISFIED`, `CAS_MISMATCH` | optional only when reconciliation adopted an effect without issuing this request | required |
| `COMPLETE_MARKER_SEARCH` | `OBSERVED_SATISFIED` | forbidden | required exact `CHANGE_REQUEST_SEARCH_RESULT` observation, including separately ordered live/terminal memberships and live-only `ZERO`/`ONE`/`MULTIPLE`, whose copied Publication Effect generation, `PUBLISH` Activity, and operation digest equal this checkpoint's immutable Effect |
| `CHANGE_REQUEST_SEARCH` | `OBSERVED_ABSENT` | forbidden | required exact `CHANGE_REQUEST_ABSENT` observation |
| `CHANGE_REQUEST_SEARCH` | `OBSERVED_SATISFIED` | forbidden | required exact `CHANGE_REQUEST_DISCOVERED` observation |
| `CHANGE_REQUEST_CREATE` | `REQUEST_READY` | required | forbidden |
| `CHANGE_REQUEST_CREATE` | `AMBIGUOUS` | required | forbidden |
| `CHANGE_REQUEST_CREATE` | `OBSERVED_SATISFIED` | optional only when reconciliation adopted the created object without issuing this request | required |
| `BASE_READ_POST` | `OBSERVED_SATISFIED`, `BASE_MISMATCH` | forbidden | required |
| `COMPLETE` | `COMPLETED` | forbidden | required; names either the final linked Change Request/head observation after `ONE` live, the exact current search used by the `ZERO`-live positive-closed selection, or the exact current search used by a positive-owned merged selection at any live cardinality |

`INITIAL` effects follow `BASE_READ_PRE -> REF_READ -> (REF_CREATE or
REF_UPDATE when needed) -> COMPLETE_MARKER_SEARCH`. From each complete result:

- after applying positive-merged, incompatible, and incomplete ownership
  precedence, `ZERO` live and no terminal member performs `CHANGE_REQUEST_SEARCH`, may
  perform `CHANGE_REQUEST_CREATE` with its stable request identity when that
  exact search is absent, and then MUST loop to a fresh
  `COMPLETE_MARKER_SEARCH`; neither discovery nor create response links;
- `ONE` live performs a fresh exact-object read, links only that member, then
  performs `BASE_READ_POST -> COMPLETE`;
- `MULTIPLE` live performs one proof-bound duplicate cleanup at a time and
  loops to a fresh `COMPLETE_MARKER_SEARCH`; and
- a positive owned `MERGED` terminal member at any live cardinality performs
  deterministic merged selection, appends `COMPLETE`, terminalizes, and creates
  the durable Reservation for every live member; otherwise `ZERO` live with
  positive owned `CLOSED` terminal members performs the deterministic closed
  selection and terminates without search/create.

This is the sole allowed phase loop: a fresh `COMPLETE_MARKER_SEARCH` may
follow `CHANGE_REQUEST_SEARCH`, `CHANGE_REQUEST_CREATE` (any terminal response,
including ambiguity reconciliation), or one completed duplicate cleanup.
Positive closed terminal audit members do not affect a nonzero-live branch.
An incomplete proof forces fresh evidence/backoff, and positive incompatible
ownership forces reconciliation rather than selecting a member. An absent
optional mutation step is represented by the read/search observation, not a
fabricated checkpoint.
`UPDATE` effects follow `REF_READ -> REF_UPDATE -> COMPLETE`. A checkpoint that
violates mode, order, status, or nullability is rejected as an integrity error.
Read/search suboperations (`BASE_READ_PRE`, `REF_READ`,
`COMPLETE_MARKER_SEARCH`, `CHANGE_REQUEST_SEARCH`, and `BASE_READ_POST`) MAY repeat during ambiguous
reconciliation only with a new Forge Observation ID and a higher checkpoint
sequence. Repeats remain within their original phase except the explicit fresh
complete-search loop above, `REF_READ` immediately before its paired
`REF_CREATE`/`REF_UPDATE` retry, and `BASE_READ_POST` after a provisional Change
Request is already linked. A replay of the same observation is idempotent and
creates no checkpoint. Change Request discovery/creation alone leaves the
Publication pre-link; only fresh `ONE` plus exact-object read sets
`CHANGE_REQUEST_OBSERVED`. It becomes `ACTIVE` only with `COMPLETE` after the
matching post-link base read, or becomes `CLOSED` through positive-closed
zero-live selection or positive-merged selection at any cardinality.

For either base-read suboperation, equality with the effect's reviewed
`base_commit` records `OBSERVED_SATISFIED` under every policy. A differing
commit is policy-specific:

- `REBASE_BEFORE_PUBLICATION` records `BASE_MISMATCH`, supersedes the effect,
  and follows exact-base `REBASE` plus the full gate;
- `PIN` records `OBSERVED_SATISFIED` despite the difference and continues the
  same effect, because the read itself satisfied the pinned policy; and
- `SUPERSEDE_AT_BOUNDARY` records `BASE_MISMATCH`, supersedes the effect,
  captures a base-only Snapshot whose `supersession_key` includes the observed
  commit, and follows safe-boundary specification supersession and replanning.

At `BASE_READ_POST`, either mismatch path retains the owned provisional ref,
Change Request, and exact head for the later higher-generation `INITIAL`
effect. No policy may reinterpret a checkpoint recorded under another policy.

The current checkpoint is the highest committed sequence, not Redis state. A
controller restart leaves the owning `PUBLISH` Activity `ACTIVE` and resumes
from that checkpoint. Only a `COMPLETED` checkpoint can complete the Activity.

## Reconciliation Fact

A Reconciliation Fact is an immutable controller-derived result of one exact
`RECONCILE` Activity.

| Field | Requirement |
| --- | --- |
| `reconciliation_fact_id` | Controller-assigned UUID and reducer trigger ID. |
| `run_id` / `activity_id` | Exact Run and controller `RECONCILE` Activity. |
| `publication_id` / `effect_generation` | Publication/effect being reconciled when applicable. |
| `kind` | `EFFECT_PRESENT`, `EFFECT_ABSENT`, `PRELINK_REF_IMPORTABLE`, `PRELINK_REF_RECONSTRUCT_REQUIRED`, `REDUNDANT_PUBLICATIONS_PROVEN`, `NO_ACTIONABLE_DUPLICATE`, or `OWNERSHIP_CONFLICT`. |
| `forge_observation_ids` | Canonically sorted non-empty observations examined. |
| `observed_ref_commit` | Exact foreign ref commit for either pre-link-ref kind; it MUST equal both the causal `REF_HEAD` Forge Observation and the immutable input of the producing `RECONCILE` Activity. It is `NULL` for every other v1 kind. |
| `ownership_evidence_digest` | Digest of normalized marker, Change Request, deterministic-ref, legacy-engine, Project, and durable-store ownership evidence. |
| `pinned_base_relationship` | `EXACT_PINNED_BASE`, `DESCENDANT_OF_PINNED_BASE`, `DIVERGED_FROM_PINNED_BASE`, or `UNPROVEN`; required for either pre-link-ref kind. |
| `safe_fetch_proof_digest` | Required only for a pre-link-ref kind; proves the commit is fetchable from the registered source without executing it. |
| `candidate_admission_proof_digest` / `validation_failure_digest` | Admission proof is required for `PRELINK_REF_IMPORTABLE`; a bounded typed failure digest is required for `PRELINK_REF_RECONSTRUCT_REQUIRED`; otherwise both are `NULL`. |
| `retained_change_request_external_id` / `retained_head` / `retained_observation_id` | Required only for `REDUNDANT_PUBLICATIONS_PROVEN`; the lowest adapter-normalized stable ID in its complete frozen member set and that object's exact head/observation. |
| `complete_search_revision` | Required for either duplicate-search kind; exact adapter revision/token proving the candidate member set is complete. |
| `duplicate_members_digest` | Required only for `REDUNDANT_PUBLICATIONS_PROVEN`; digest of the canonical ordered Duplicate Member rows below. |
| `duplicate_set_digest` | Required for either duplicate-search kind; digest of the complete canonical same-marker search result, including the canonical empty set. It exactly copies the causal `CHANGE_REQUEST_SEARCH_RESULT` Observation. |
| `fact_digest` / `recorded_at_ms` | Canonical digest and informational time. |

`activity_id` is unique among Reconciliation Facts. `PRELINK_REF_IMPORTABLE`
is valid only when reconciliation finds no syntactically valid marker for a
different Run/Publication, no legacy/human ownership record, no incompatible
Change Request, safe registered-source fetch, and a proved
`EXACT_PINNED_BASE` or `DESCENDANT_OF_PINNED_BASE` relationship that satisfies
ordinary Candidate admission. It authorizes controller `IMPORT` plus the full
Candidate gate, never adoption or overwrite. A safely fetchable head whose
base relationship or Candidate admission fails without positive incompatible
ownership evidence produces `PRELINK_REF_RECONSTRUCT_REQUIRED`, not another
import attempt. Both pre-link kinds bind the same exact causal `REF_HEAD`
observation and foreign commit frozen on the producing `RECONCILE` Activity;
the reducer rejects a Fact whose commit differs from either binding. Its
Recovery Evidence must select the closed
`RECONSTRUCT_FOREIGN_HEAD` tactic. Only positive incompatible ownership
evidence may instead produce `OWNERSHIP_CONFLICT`.
`OWNERSHIP_CONFLICT` requires positive incompatible ownership evidence. A
duplicate marker is autonomously reconciled by stable identities. A
`REDUNDANT_PUBLICATIONS_PROVEN` Fact requires one retained live object plus at
least one redundant live member, all open and unmerged, all carrying the exact same valid
v1 Run/Publication marker, registered Project, deterministic source ref, and
observed head, and every source Search Member MUST have
`ownership_status = POSITIVE` with a valid proof digest. `OWNERSHIP_CONFLICT`
for a complete search
requires at least one exact `INCOMPATIBLE` member and names that member/proof in
`ownership_evidence_digest`; `INCOMPLETE` cannot produce this Fact or a Human
Boundary. `REDUNDANT_PUBLICATIONS_PROVEN` also requires a complete adapter search at one frozen search
revision and current evidence that every redundant member has no non-Orcest
review, discussion, merge, or other external reliance. The retained object is
the bytewise-lowest adapter-normalized stable Change Request ID across the
live set; no worker, repository setting, arrival order, or model chooses
it. The Fact's `complete_search_revision` and `duplicate_set_digest` copy its
exact causal `CHANGE_REQUEST_SEARCH_RESULT` and therefore authenticate both
the live and terminal audit memberships; `duplicate_members_digest`
additionally proves only the ordered actionable live cleanup membership.
Terminal members are never `RETAIN`/`CLOSE` rows and cannot block live-set
convergence. Its reduction stores the
Fact ID, search revision, and set digest in the Publication's last-duplicate
projection before any cleanup Activity becomes visible.

The duplicate-search field matrix is closed. For
`REDUNDANT_PUBLICATIONS_PROVEN`, `complete_search_revision`,
`duplicate_set_digest`, `duplicate_members_digest`, all three retained-object
fields, exactly one `RETAIN` row, and one or more `CLOSE` rows are present (at
least two Duplicate Member rows total). Those rows correspond one-for-one to
every `LIVE` member of the causal search Observation and to no `TERMINAL`
member. For
`NO_ACTIONABLE_DUPLICATE`, only `complete_search_revision` and
`duplicate_set_digest` are present; the member digest and retained-object
fields are `NULL` and there are no Duplicate Member rows. Every duplicate-search
field is `NULL` and no Duplicate Member row exists for every other Fact kind.

`NO_ACTIONABLE_DUPLICATE` is the terminal result of a complete duplicate
search whose normalized live set contains no safely closable redundant
member. It stores `complete_search_revision` and the exact
`duplicate_set_digest` copied from its causal search Observation, has no
Duplicate Member rows,
changes no retained association, creates no cleanup Activity, and remains in
`PR_MONITORING`. It is invalid before initial durable linkage: a pre-link
`MULTIPLE` proof must yield `REDUNDANT_PUBLICATIONS_PROVEN` when every
non-retained member is exactly closable; positive non-Orcest reliance or an
incompatible ownership claim yields `OWNERSHIP_CONFLICT`; and unavailable or
incomplete evidence waits/retries without emitting a duplicate-search Fact.
The Fact transaction updates the Publication's three
last-duplicate fields. An unchanged `(complete_search_revision,
duplicate_set_digest)` cannot plan another duplicate `RECONCILE`; only a later
accepted `CHANGE_REQUEST_SEARCH_RESULT` carrying a different revision or set
digest can. Individual object observations may schedule that complete read but
cannot bypass the pair fence.

The frozen child relation `Reconciliation Duplicate Member` has these fields:

| Field | Requirement |
| --- | --- |
| `reconciliation_fact_id` / `member_ordinal` | Parent Fact plus contiguous zero-based ordinal. |
| `disposition` | `RETAIN` for ordinal zero and exactly one row; `CLOSE` for every other row. |
| `change_request_external_id` | Adapter-normalized stable ID, unique within the Fact; rows are ordered by its bytewise value. |
| `observed_head` / `identity_observation_id` | Exact head and current `CHANGE_REQUEST_DISCOVERED`, `CHANGE_REQUEST_HEAD`, or `CHANGE_REQUEST_FEEDBACK` observation. |
| `unreviewed_observation_id` / `unreviewed_proof_revision` | Exact current feedback/read observation and adapter revision proving the closed unreviewed predicate; required for `CLOSE`, `NULL` for `RETAIN`. |
| `equivalence_proof_digest` | Digest of exact Project/ref/marker/Publication/head equivalence and the member's normalized identity. |

`duplicate_members_digest` is
`sha256(canonical_json(ordered member rows excluding the parent ID))`.
`retained_change_request_external_id`, `retained_head`, and
`retained_observation_id` MUST respectively equal the ordinal-zero member's
`change_request_external_id`, `observed_head`, and
`identity_observation_id`. Every observation
named by a member also appears in `forge_observation_ids`. A missing member,
noncontiguous ordinal, changed head, review/discussion activity, incomplete
search, or digest mismatch rejects the Fact. The reducer closes at most one
`CLOSE` member per `CLOSE_REDUNDANT_PUBLICATION` Activity and performs a fresh
complete marker search after each proven close; only a changed typed search
result may then plan another `RECONCILE`. It never treats the old Fact as proof
that the remaining set is still current. Provably equivalent redundant objects are
therefore repaired deterministically, while
unavailable or insufficient evidence waits and retries rather than escalating.
Mere collision, absence, or temporary forge failure is not conflict. The
reducer uses `T(RECONCILIATION_FACT, reconciliation_fact_id)`.

## Controller Operation Fact

A Controller Operation Fact is the immutable terminal input for `IMPORT`, for
definitive controller-Activity failures, and for the one successful
`CLOSE_PUBLICATION` case that proves a possible create request produced no
Change Request. Successful `PUBLISH` terminalizes through its exact
observation-backed completion checkpoint; successful `RECONCILE` through its
Reconciliation Fact; and a linked close through the authenticated close/merge
Forge Observation. This prevents a mutable Activity row or synthesized digest
from acting as a reducer trigger without creating two authoritative success
inputs.

| Field | Requirement |
| --- | --- |
| `controller_operation_fact_id` | Controller-assigned UUID and reducer trigger ID. |
| `run_id` / `activity_id` | Exact Run and current controller Activity. |
| `kind` | Exact Activity kind: `IMPORT`, `PUBLISH`, `CLOSE_PUBLICATION`, `CLOSE_REDUNDANT_PUBLICATION`, `REPAIR_RUN_MARKER`, or `RECONCILE`. |
| `outcome` | `SUCCEEDED` or `FAILED`. |
| `operation_digest` | Exact stable operation identity committed before external I/O. |
| `output_candidate_id` / `forge_observation_ids` | Canonical output bindings required by the Activity kind and outcome; otherwise `NULL` or empty. A successful pre-link `CLOSE_PUBLICATION` Fact contains exactly the current causal `CHANGE_REQUEST_ABSENT` observation. A Controller Operation Fact never points to a Reconciliation Fact. |
| `failure_category` / `failure_evidence_digest` | Required exactly for `FAILED`; category is one of `SOURCE_READ`, `BASE_CONFLICT`, `CREDENTIAL`, `STORAGE`, `INTEGRITY_SUSPECTED`, or `POLICY`; otherwise both `NULL`. |
| `fact_digest` / `recorded_at_ms` | Digest of normalized immutable fields and informational time. |

At most one Controller Operation Fact may terminally resolve an Activity.
Insertion atomically makes the Activity terminal and invokes
`T(CONTROLLER_OPERATION, controller_operation_fact_id)`. `PUBLISH`,
`RECONCILE`, `CLOSE_REDUNDANT_PUBLICATION`, and `REPAIR_RUN_MARKER` may create this Fact
only with `FAILED`. A successful `RECONCILE`
inserts no Controller Operation Fact: its Reconciliation Fact, Activity
success, and `T(RECONCILIATION_FACT, reconciliation_fact_id)` commit
atomically. A successful `PUBLISH` completes with its final checkpoint and
Forge Observation, not this Fact. `CLOSE_PUBLICATION/SUCCEEDED` is permitted
only when the Fact names the exact current `CHANGE_REQUEST_ABSENT` Forge
Observation whose source repository/ref/Run marker and search revision match
the cleanup Activity and Publication; after linkage, close or merge observation
terminalizes the cleanup and no successful Controller Operation Fact is
created. A resumable active
`PUBLISH` checkpoint or ambiguous response does not create a terminal fact.
For successful pre-link `CLOSE_PUBLICATION`, `output_candidate_id = NULL`,
`forge_observation_ids` contains exactly the one bound
`CHANGE_REQUEST_ABSENT`, and every failure-only field is `NULL`; no ref,
Candidate, or empty local result may occupy an absent-observation field.
Successful `CLOSE_REDUNDANT_PUBLICATION` likewise terminalizes only through an
authenticated `CHANGE_REQUEST_CLOSED` Forge Observation for the exact
redundant ID/head and stable operation identity. That observation does not
close the Publication or terminate the Run because it is not the retained
`Publication.change_request_external_id`. An ambiguous close response remains
an active reconciliation problem and creates no success Fact.
Successful `REPAIR_RUN_MARKER` likewise terminalizes only through the exact
controller-bound `CHANGE_REQUEST_MARKER` Observation described by its Activity;
an ambiguous repair remains active and no success Fact is synthesized.

The failed-Fact category matrix is closed. `IMPORT` permits all six categories.
`PUBLISH` and `RECONCILE` permit `BASE_CONFLICT`, `CREDENTIAL`, `STORAGE`,
`INTEGRITY_SUSPECTED`, or `POLICY`. `CLOSE_PUBLICATION`,
`CLOSE_REDUNDANT_PUBLICATION`, and `REPAIR_RUN_MARKER` permit only
`BASE_CONFLICT`, `CREDENTIAL`, or `POLICY`. A failed Fact appends Recovery
Evidence with the identical category and a code-owned tactic; no text or
adapter status may remap it. `FORGE_TRANSIENT` is forbidden here because only
a Forge Request Failure Fact may establish that category. Ambiguous I/O,
restart, timeout, and temporary unavailability never become a definitive
Controller Operation Fact.

## Forge Observation Schedule and Request

A Forge Observation Schedule is the durable controller-owned cadence for a
read that can create Forge Observations. A Forge Observation Request is one
immutable, pre-I/O execution of that schedule. Webhooks may advance a schedule
as wake hints but never replace either object.

| Schedule field | Requirement |
| --- | --- |
| `forge_observation_schedule_id` | Controller-assigned UUID and durable schedule identity. |
| `schedule_kind` | `WORK_ITEM_DISCOVERY`, `WORK_ITEM_POLL`, `BASE_HEAD_POLL`, `REF_POLL`, `CHANGE_REQUEST_SEARCH`, `CHANGE_REQUEST_POLL`, `CI_POLL`, or `COMPLETE_MARKER_SEARCH`. |
| `project_id` / `forge_instance_id` | Exact registered repository and Forge authority. |
| `target_kind` / `target_id` | `PROJECT`, `WORK_ITEM`, or `PUBLICATION` plus stable target identity; the kind matrix below is closed. |
| `run_id` / `publication_id` | Exact active Run/Publication when applicable; both `NULL` for Project-scoped `WORK_ITEM_DISCOVERY` and pre-admission Work Item polling. |
| `terminal_duplicate_cleanup_reservation_id` | Exact `ACTIVE` terminal Reservation for a post-merge `CHANGE_REQUEST_POLL` or `COMPLETE_MARKER_SEARCH`; otherwise `NULL`. Such a Schedule is controller cleanup work rather than active-Run semantic work. |
| `minimum_interval_ms` / `next_due_at_ms` | Positive server-owned cadence and durable next eligible request time. |
| `schedule_revision` | Nonnegative monotonic CAS revision. |
| `last_request_id` | Exact latest Request, or `NULL` before the first one. |
| `last_discovery_search_revision` / `last_discovery_set_digest` | Exact last completed adapter list-search revision and semantic set digest for `WORK_ITEM_DISCOVERY`, nullable together before its first completion; both `NULL` for every other kind. |
| `state` | `ACTIVE`, `PAUSED`, or `CLOSED`; only `ACTIVE` can create a Request. |
| `schedule_digest` | Digest of normalized authority, target, kind, and cadence fields. |

| Request field | Requirement |
| --- | --- |
| `forge_observation_request_id` | Controller-assigned UUID and immutable retry identity. |
| `protocol_version` | Exact literal `orcest.forge-observation-request/1`. |
| `forge_observation_schedule_id` / `schedule_revision` | Exact Schedule and CAS revision from which this Request was created. |
| `request_sequence` | Positive monotonic sequence within the Schedule. |
| `request_kind` / `project_id` / `forge_instance_id` / `target_kind` / `target_id` / `run_id` / `publication_id` | Exact copied Schedule bindings. |
| `terminal_duplicate_cleanup_reservation_id` | Exact copied nullable Schedule binding. |
| `created_under_controller_mode_revision` / `created_under_controller_mode` | Exact non-maintenance Controller Mode projection that permitted Request creation; included in `request_digest`. |
| `credential_purpose` / `credential_secret_ref` | `PROJECT_SOURCE_READ` plus the exact current version of `Project.source_read_secret_id` for ordinary repository polling/search, or `PUBLICATION` plus the exact `PublicationEffect.publication_secret_ref` for an effect/controller-operation or terminal-cleanup readback. `FORGE_CONNECTIVITY` credentials belong only to Health Probe Request. |
| `controller_activity_id` / `effect_generation` / `controller_operation_digest` | All required for an effect/controller-operation readback and exact copied Activity/Publication Effect/operation fence; otherwise all `NULL`. |
| `terminal_duplicate_cleanup_action_id` / `terminal_cleanup_operation_digest` | Required together only for a cleanup mutation readback; in that case `effect_generation` and Reservation are required while controller Activity/operation fields are `NULL`. For a Reservation proof-refresh read without a current mutation Action, both are `NULL` but Reservation/effect remain required. |
| `expected_prior_observation_sequence` / `expected_external_revision` | Exact prior target projection fence for every non-discovery Request; sequence is nonnegative and revision is nullable only when no prior external revision exists. Both are `NULL` for `WORK_ITEM_DISCOVERY`. |
| `expected_discovery_search_revision` / `expected_discovery_set_digest` | Exact pair copied from the discovery Schedule, nullable together before its first completion; both `NULL` for every non-discovery Request. |
| `request_idempotency_key` / `request_digest` | Stable UUID adapter identity and digest of all normalized immutable fields. |
| `state` | `PENDING`, `COMPLETED`, or `SUPERSEDED`. |
| `outbox_id` | Exact reciprocal Outbox row committed before I/O. |
| `next_attempt_ordinal` | Positive next outbound transport-attempt ordinal; initialized to `1` and incremented in the pre-I/O transaction. |
| `last_failure_fact_id` / `next_retry_ms` | Exact latest transient Forge Request Failure Fact and its deterministic retry boundary while `PENDING`, otherwise both `NULL`. |
| `result_observation_ids_digest` | Digest of the ordered result membership; `NULL` while `PENDING`, canonical empty for `SUPERSEDED`, and required for `COMPLETED`. |
| `result_discovery_search_revision` / `result_discovery_set_digest` | Required exactly for a completed `WORK_ITEM_DISCOVERY`, including an empty result; both `NULL` for pending/superseded discovery and every other kind. |
| `created_at_ms` / `completed_at_ms` | Informational times; completion is non-`NULL` exactly when not `PENDING`. |

The kind/target matrix is closed: `WORK_ITEM_DISCOVERY` targets `PROJECT`, has
`target_id = project_id`, and produces zero or more `WORK_ITEM_SNAPSHOT`
Observations in bytewise Work Item stable-ID order; each result targets its own
discovered `WORK_ITEM` in the same Project. `WORK_ITEM_POLL` targets `WORK_ITEM` and may
produce `WORK_ITEM_SNAPSHOT` or `DEPENDENCY_STATE`; `BASE_HEAD_POLL` targets
either kind and produces only `BASE_HEAD`; `REF_POLL` targets `PUBLICATION` and
produces exactly one `REF_ABSENT` or `REF_HEAD`; `CHANGE_REQUEST_SEARCH`
targets `PUBLICATION` and produces exactly one `CHANGE_REQUEST_ABSENT` or
`CHANGE_REQUEST_DISCOVERED`; `CHANGE_REQUEST_POLL` and `CI_POLL` target
`PUBLICATION` and produce only current Change Request identity/head/
marker/merge/close or `CHANGE_REQUEST_FEEDBACK` respectively; and
`COMPLETE_MARKER_SEARCH` targets `PUBLICATION` and produces exactly one
`CHANGE_REQUEST_SEARCH_RESULT`. A Request cannot produce an Observation
outside its row.

Multi-result order is closed. `WORK_ITEM_DISCOVERY` uses bytewise Work Item
stable ID. `WORK_ITEM_POLL` emits `WORK_ITEM_SNAPSHOT` before
`DEPENDENCY_STATE` when both are present. An open `CHANGE_REQUEST_POLL` emits
`CHANGE_REQUEST_DISCOVERED` when identity was not yet associated, otherwise
`CHANGE_REQUEST_HEAD`, then emits `CHANGE_REQUEST_MARKER`; a merged or closed
read emits only `CHANGE_REQUEST_MERGED` or `CHANGE_REQUEST_CLOSED`,
respectively. Every other non-discovery kind emits exactly one Observation.
New per-target `observation_sequence` values are allocated in this membership
order in the completion transaction; a reused coalesced Observation keeps its
existing sequence and creates no new reduction. Thus terminal state and
retained-head fencing precede marker/repair evaluation deterministically.

Every non-discovery result Observation copies the Request's
Project/target/Run/Publication and credential-purpose binding. A
`WORK_ITEM_DISCOVERY` result instead copies the Project and credential binding,
has `run_id = publication_id = NULL`, and uses the exact discovered Work Item
stable ID as its `WORK_ITEM` target; its ordered membership proves the complete
normalized discovery result, including empty. Its adapter list-search revision
is persisted in `result_discovery_search_revision`; its semantic set digest is
`sha256(ascii("orcest-work-item-discovery-set-v1") || 0x00 ||
canonical_json(bytewise stable-ID-ordered
[(work_item_external_id, work_item_revision, forge_observation.payload_digest)]))`,
not a digest of controller-assigned Observation IDs. An effect/controller-operation readback also
copies its exact `effect_generation`, `controller_activity_id`, and
`controller_operation_digest` into every Observation kind that defines those
fields; a terminal cleanup readback instead copies the Reservation, Effect,
nullable Cleanup Action, and cleanup-operation bindings from its exclusive
Request union. A response lacking or changing one is rejected. Ordinary
monitoring Requests have every controller/effect/cleanup field `NULL` and
cannot claim controller-operation success. The Request digest covers the
tagged credential, effect, and cleanup union.

Result membership is the immutable child relation
`(forge_observation_request_id, observation_ordinal, forge_observation_id)`.
Ordinals are zero-based and contiguous, Observation IDs are unique within the
Request, and the ordered IDs reproduce `result_observation_ids_digest`. An
existing Observation may belong to a later Request membership only when
target, authority, credential, and optional effect/operation bindings match
exactly and either its same non-`NULL` `adapter_event_id` plus content is being
idempotently replayed, or it has no adapter event ID and is the immediately
preceding identical payload for that target. Intervening target content forbids
the latter reuse. Its immutable
`created_by_forge_observation_request_id` remains the Request that first
inserted it; only direct mutation results owned by an immutable effect/
checkpoint have no creating Request or copied credential fields.
`COMPLETED`, its normalized Forge Observations, result membership, and Outbox
delivery commit in one writer transaction. Discovery completion additionally
CASes the Schedule revision and exact expected prior discovery pair, copies the
result pair into the Schedule, and increments the Schedule revision; this is
required even for an empty result. Same adapter revision plus the same semantic
set is stable replay, while one revision paired with different content fails
integrity validation. Completion accepts an `ACTIVE` or `PAUSED` Schedule when
its immutable identity/digest are unchanged and `last_request_id` still names
this Request; the Request's creation revision need not equal the current
revision after a pure pause/reactivation CAS. `CLOSED`, a different last
Request, changed immutable scope, or a failed target/discovery prior fence is a
schedule-scope mismatch and records
`SUPERSEDED` with no Observations. Because that disposition follows an adapter
response, its reciprocal Outbox becomes `DELIVERED`, never `SUPERSEDED`; the
latter means only that dispatch was fenced before any I/O. The durable Schedule
may create a fresh Request if still active.

Creating a due Request requires no existing `PENDING` Request for that Schedule,
CASes the exact `ACTIVE` Schedule revision, sets `request_sequence` to the next
positive value, increments the Schedule revision, stores this Request as
`last_request_id`, advances `next_due_at_ms` by at least
`minimum_interval_ms`, and commits the Request plus reciprocal Outbox before any
forge I/O. A partial unique constraint permits at most one `PENDING` Request per
Schedule. Adapter timeout, controller restart, and Redis loss retry the
same Request/idempotency key; they never create a synthetic Observation.
Startup scans every `ACTIVE` due Schedule and every `PENDING` Request. The
same request ID/digest returns the existing result; conflicting reuse fails
closed. Request/Schedule state is not a reducer trigger: only each accepted
resulting Forge Observation is reduced exactly once.
`(forge_observation_schedule_id, request_sequence)` is unique, as is
`last_request_id` when non-`NULL`. At most one non-`CLOSED` Schedule exists for
the same `(project_id, schedule_kind, target_kind, target_id, run_id,
publication_id, terminal_duplicate_cleanup_reservation_id)` null-normalized
identity.

Schedule lifecycle is a closed controller projection. Project registration
creates the Project-scoped discovery Schedule. Completing a discovery Request
atomically creates or CAS-reuses Run-null `WORK_ITEM_POLL` and `BASE_HEAD_POLL`
Schedules for every returned Work Item before either read. If the discovery
Schedule or Project is paused at completion, those children are created or
kept `PAUSED`; completion never reactivates them, and only Project reactivation
may CAS them to `ACTIVE`. Using the complete
discovery set, it closes those Run-null schedules for absent Work Items that
have no active Run. The `ADMIT` Transition closes that Work Item's Run-null
schedules, superseding any pending Request/outbox, and creates the corresponding
Run-bound schedules at revision 0. Thus pre-admission and active-Run schedule
identities never remain concurrently active. Any later Run Transition that
first needs another read creates an `ACTIVE` Schedule at revision 0, or
CAS-reuses the existing non-closed identity. A successful
Project suspension CASes each Project-scoped Schedule to `PAUSED`; reactivation
CASes only still-required schedules back to `ACTIVE` and makes them immediately
eligible. `PAUSED` permits an already-`PENDING` read to finish but creates no
new Request. Project removal, a Run terminal Transition, or a lifecycle change
that permanently ends that target/kind CASes the Schedule to `CLOSED` and
atomically marks any `PENDING` Request `SUPERSEDED` with canonical empty result
membership and its still-pending reciprocal Outbox `SUPERSEDED` before the
close commits. The sole terminal-Run exception is a Schedule carrying the exact
new `ACTIVE` Terminal Duplicate Cleanup Reservation: terminalization closes the
ordinary Run schedules and creates or retains only its cleanup poll/search
Schedules, which close atomically when the Reservation completes. A late adapter response for that Request
is then exact replay of the superseded outcome and creates no Observation.
Every activation, pause, close, or due-Request creation increments
`schedule_revision`; the writer compares the exact prior revision and state.
No other component mutates `state`, cadence, `last_request_id`, or
`next_due_at_ms`. Startup therefore scans only `ACTIVE` due schedules and
retries only still-`PENDING` Requests, while a terminal Run cannot leave a
restart-scannable Run-owned schedule.

Controller Mode gates Schedule execution independently of Schedule state.
`RUNNING`, `INTAKE_PAUSED`, `DISPATCH_PAUSED`, and `DRAINING` permit ordinary
Forge Observation Request creation, dispatch, and completion; creation freezes
that exact mode/revision, while dispatch and completion re-read the current
projection. `MAINTENANCE` creates no ordinary Request, sends no pending Outbox,
and commits no response-driven completion or supersession. It leaves due times,
pending Requests, and pending Outboxes unchanged so restart or mode exit retries
the same identities. A response received after maintenance began is not
workflow evidence and is retried/reconciled with the same adapter idempotency
key after exit. V1 names no Forge Observation Schedule kind as a maintenance
recovery exception: maintenance recovery reads are the exact Storage/Secret
integrity, backup-restore, and key/mode/bootstrap management paths defined by
their own durable operations, not an ordinary repository poll disguised as
recovery.

### Forge Request Failure Fact

A Forge Request Failure Fact is the immutable outcome of one failed transport
attempt for a durable `PENDING` Forge Observation Request. It is the only v1
authority for turning a read/search/poll timeout, rate limit, or temporary
unavailability into `FORGE_TRANSIENT` recovery. It never represents a
publication mutation: ambiguous writes remain governed by Publication Effect
checkpoints and reconciliation.

| Field | Requirement |
| --- | --- |
| `forge_request_failure_fact_id` | Controller-assigned UUID and reducer trigger ID when the Request is Run-bound. |
| `forge_observation_request_id` / `request_attempt_ordinal` | Exact still-`PENDING` Request and positive outbound attempt ordinal committed before that I/O; unique together. |
| `project_id` / `run_id` / `publication_id` / `terminal_duplicate_cleanup_reservation_id` | Exact copied Request scope; nullable fields remain byte-for-byte equal to the Request. |
| `failure_kind` | Closed `TIMEOUT`, `RATE_LIMIT`, or `UNAVAILABLE`. Authentication, authorization, validation, not-found, CAS, ownership, and ambiguous-write outcomes use their separately specified reconciliation paths. |
| `failure_code` / `failure_evidence_digest` | Bounded non-secret stable adapter code and digest of normalized transport evidence. Raw response bodies and credentials are forbidden. |
| `retry_not_before_ms` | Exact deterministic retry boundary. `RATE_LIMIT` uses the bounded authenticated adapter reset when valid; other kinds use the code-owned backoff derived from the Request attempt ordinal and installed policy. |
| `request_digest` / `fact_digest` / `recorded_at_ms` | Exact copied Request digest, domain-separated complete Fact digest, and informational controller time. |

Before each outbound attempt, the writer increments and commits the Request's
attempt ordinal and reciprocal Outbox delivery projection. A transport failure
for that ordinal atomically inserts this Fact, stores it as
`last_failure_fact_id`, sets `next_retry_ms`, and leaves the Request and Outbox
`PENDING`; exact retry returns the Fact, while different content for the same
ordinal is an integrity conflict. Success or supersession wins by the same
Request-state CAS and rejects a late failure Fact.

For an active Run-bound Request, Fact insertion appends exactly one
`T(FORGE_REQUEST_FAILURE, forge_request_failure_fact_id)`, zero-counter
Recovery Evidence with category `FORGE_TRANSIENT` and tactic `WAIT_EXTERNAL`,
and enters `RECOVERING`. Only the later Evidence Transition creates
`WAITING/FORGE_UNAVAILABLE`, whose timer arm is `retry_not_before_ms` and whose
event arm names the exact Schedule/target and a later successful Forge
Observation or verified `FORGE_CONNECTIVITY/AVAILABLE` Health Observation.
For Project discovery/pre-admission work or a terminal cleanup Reservation,
there is no Run Transition; the persisted Fact and `next_retry_ms` drive the
same Request/outbox retry directly. Restart scans both pending Requests and
their latest Fact. No branch synthesizes a Forge or Health Observation.

## Forge Observation

A Forge Observation is an immutable normalized external snapshot. It can
precede Run admission, so Work Item targets do not require a Run or Publication.

| Field | Requirement |
| --- | --- |
| `forge_observation_id` | Controller-assigned UUID. |
| `project_id` | Observed Project. |
| `target_kind` | `WORK_ITEM` or `PUBLICATION`. |
| `target_id` | Stable Work Item external ID or internal `publication_id`, interpreted by `target_kind`. |
| `run_id` / `publication_id` | Related authority, nullable for a pre-admission Work Item observation. |
| `created_by_forge_observation_request_id` | Exact Forge Observation Request that first inserted this scheduled-read Observation; `NULL` only for a mutation result that did not use a read Request. A later Request may reference an eligible coalesced row without changing this field. |
| `credential_purpose` / `credential_secret_ref` | Exact copied creating-Request credential union when `created_by_forge_observation_request_id` is non-`NULL`; otherwise both `NULL`. |
| `publication_effect_generation` | Required exact effect generation for an observation produced by a Publication mutation, controller-owned close, or effect-bound readback such as pre-link `COMPLETE_MARKER_SEARCH`; otherwise `NULL`. |
| `controller_activity_id` / `controller_operation_digest` | Nullable together. Required for an effect/controller-operation readback and when the observation is the successful result of `CLOSE_PUBLICATION`, `CLOSE_REDUNDANT_PUBLICATION`, or `REPAIR_RUN_MARKER`; they bind the exact Activity and its pre-I/O stable operation identity. Otherwise `NULL`. |
| `terminal_duplicate_cleanup_reservation_id` | Exact terminal Reservation for a cleanup proof-refresh or Action result; otherwise `NULL`. |
| `terminal_duplicate_cleanup_action_id` / `terminal_cleanup_operation_digest` | Nullable together. Required exactly for a terminal Reservation `CLOSE`/`DETACH_MARKER` success or mismatch readback and equal its immutable Action/operation; in that case controller Activity fields are `NULL`. |
| `kind` | Code-owned observation kind. |
| `external_revision` | Head SHA, issue revision, check-suite revision, merge commit, or adapter token. |
| `adapter_event_id` | External delivery ID when available. |
| `actor_principal_id` / `actor_authorization_digest` | Verified forge actor and authorization evidence for an authored specification/control change; otherwise `NULL`. |
| `payload_digest` | Digest of normalized fields consumed by the reducer. |
| `observation_sequence` | Controller-assigned monotonic sequence per `(project_id, target_kind, target_id)`. |
| `observed_at_ms` | Informational time. |

The v1 observation-kind set is closed:

| Kind | Allowed target | Required normalized fact |
| --- | --- | --- |
| `WORK_ITEM_SNAPSHOT` | `WORK_ITEM` | Exact open/closed state, labels, title/body, opted-in marked comments, and work-item revision. Authored changes include verified actor fields. |
| `DEPENDENCY_STATE` | `WORK_ITEM` | Canonically ordered dependency identities, observed revisions, and satisfied/open/unknown states. |
| `BASE_HEAD` | `WORK_ITEM` or `PUBLICATION` | Trusted registered base ref and exact `{object_format, oid}`. |
| `REF_ABSENT` | `PUBLICATION` | Deterministic publication ref plus adapter nonexistence revision/token. |
| `REF_HEAD` | `PUBLICATION` | Deterministic publication ref and exact head commit. |
| `CHANGE_REQUEST_ABSENT` | `PUBLICATION` | Exact registered source repository ID, deterministic source ref, Orcest v1 Run marker, adapter search revision, and stable Change Request nonexistence token. It proves only that the bound search found no matching Change Request. |
| `CHANGE_REQUEST_DISCOVERED` | `PUBLICATION` | Stable Change Request ID, source ref, base ref, marker, open state, and exact head. Covers discovery after create. |
| `CHANGE_REQUEST_HEAD` | `PUBLICATION` | Stable Change Request ID and exact current head. |
| `CHANGE_REQUEST_FEEDBACK` | `PUBLICATION` | Stable Change Request ID/head; `mergeability` in `CLEAN`, `CONFLICTING`, or `UNKNOWN`; every configured required check as stable key plus `PENDING`, `PASS`, or `FAIL`; current-head `CHANGES_REQUESTED` reviews; and unresolved current-head discussion threads. Review/thread facts include stable external ID and authenticated author principal plus controller-derived `author_is_orcest`; all collections are canonically ordered. |
| `CHANGE_REQUEST_SEARCH_RESULT` | `PUBLICATION` | Exact registered source repository ID, Run marker and deterministic ref, adapter complete-search revision equal to `external_revision`, separately ordered live and terminal membership below, `live_cardinality` (`ZERO`, `ONE`, or `MULTIPLE`) derived only from live rows, and `duplicate_set_digest` over both lists including empty. Ordinary post-link monitoring has `publication_effect_generation = NULL`; pre-link `COMPLETE_MARKER_SEARCH` for an `INITIAL` Effect instead requires the exact non-`NULL` Effect generation, `PUBLISH` Activity, and operation digest copied from its Request and checkpoint. |
| `CHANGE_REQUEST_MARKER` | `PUBLICATION` | Stable Change Request ID/head, deterministic source ref, exact body revision, canonically ordered Orcest/legacy marker occurrences and `marker_set_digest`; a controller-owned repair success proves the exact single desired marker and carries the Activity/operation binding, while terminal duplicate detach proves only the selected Run marker absent/all other elements preserved and carries the Cleanup Action/operation binding. |
| `CHANGE_REQUEST_MERGED` | `PUBLICATION` | Stable Change Request ID, final head, and exact merge commit. |
| `CHANGE_REQUEST_CLOSED` | `PUBLICATION` | Stable Change Request ID, final head, and unmerged closed state; when it proves a controller-owned close, exact controller Activity/operation or terminal Cleanup Action/operation binding from the applicable exclusive union. |

No adapter or repository may introduce another kind. A newly Request-produced
row names that creating Request and, for a non-discovery Request, its Project,
target, Run/Publication, credential, and optional effect/controller-operation
bindings equal the Request byte-for-byte. A discovery-created row follows the
Project-to-Work-Item exception above. A later Request may include an existing
row only under the exact event-id replay or adjacent no-ID coalescing rule; this
creates membership and a completed Request but no new Observation or
Transition. Direct mutation responses have
`created_by_forge_observation_request_id`, `credential_purpose`,
and `credential_secret_ref` all `NULL`; they still require the independent
effect/checkpoint provenance defined here. Webhooks are wake hints and never
create an Observation directly. Every row requires an
adapter-derived `external_revision`; absence is represented by the adapter's
stable normalized nonexistence token, not `NULL`. A Publication observation
created from an effect also requires its immutable
`publication_effect_generation`; monitoring-only observations leave it
`NULL`. A specification-changing observation can resolve a Human Boundary only
when both actor fields prove server-recognized edit authority at that exact
revision.

A close or marker-repair observation bearing controller Activity/operation fields is accepted only when
the adapter response or subsequent exact read correlates the affected object to
that already-committed operation identity. Both fields, the Publication/effect
generation, stable Change Request ID, marker, source ref, and final head must
match the Activity. Marker repair additionally requires exact body revision,
prior marker-set digest, and desired-marker equality. A webhook or poll that
merely notices an independently closed or edited object leaves both
controller-operation fields `NULL` and cannot claim success for a controller
Activity. A terminal-cleanup observation instead requires its exclusive Action
fields, exact Reservation member/CAS/effect binding, and `NULL` controller
Activity fields; its adapter response or subsequent exact read must likewise
correlate the affected object to that already-committed Action operation.

`CHANGE_REQUEST_SEARCH_RESULT` owns an immutable `Change Request Search
Member` child relation:

| Field | Requirement |
| --- | --- |
| `forge_observation_id` / `member_class` / `member_ordinal` | Exact parent search Observation, `LIVE` or `TERMINAL`, and a zero-based contiguous ordinal within that class. |
| `change_request_external_id` | Adapter-normalized stable ID, unique across both classes in one result; each class is independently ordered by this byte string. |
| `observed_head` | Exact normalized `{object_format, oid}` final/current head. |
| `terminal_state` / `merge_commit` | Both `NULL` for `LIVE`. `terminal_state` is `CLOSED` or `MERGED` for `TERMINAL`; normalized merge commit `{object_format, oid}` is required only for `MERGED`. |
| `source_ref` / `run_marker` | Exact deterministic source ref and syntactically valid marker for this Run/Publication; a row with another binding is not a member. |
| `observed_body_revision` / `marker_set_digest` | Exact adapter-normalized body revision and digest of the canonically ordered marker occurrences used by the ownership classifier. |
| `ownership_status` | Closed controller-derived value `POSITIVE`, `INCOMPATIBLE`, or `INCOMPLETE`. |
| `proof_kind` | `EXACT_CREATE_RESPONSE`, `AMBIGUOUS_CREATE_RECONCILED`, or `LIVE_ASSOCIATION` exactly when `ownership_status = POSITIVE`; otherwise `NULL`. |
| `proof_publication_effect_generation` / `proof_create_checkpoint_id` / `proof_create_request_idempotency_key` | Exact immutable creation provenance. Effect generation is required for every `POSITIVE` row. `EXACT_CREATE_RESPONSE` requires an `OBSERVED_SATISFIED` `CHANGE_REQUEST_CREATE` checkpoint and its request key; `AMBIGUOUS_CREATE_RECONCILED` requires the exact prior `AMBIGUOUS` create checkpoint and request key plus the later search evidence; `LIVE_ASSOCIATION` requires both create fields `NULL` and the exact durable Publication association. For a non-positive row these fields are nullable evidence; every present value must still be internally valid, but grants no authority. |
| `creator_installation_or_account_ref` | Exact registered non-secret forge installation/account that authorized the proven creation or association; required for `POSITIVE` and equal to the Project/Forge Instance registration. |
| `proof_deterministic_ref` / `proof_run_marker` / `proof_desired_commit` / `proof_observed_head` | Exact normalized ref, marker, immutable Effect desired commit, and head evidence. For `POSITIVE`, ref/marker equal this member and Publication, `proof_observed_head = observed_head`, and the proof-kind-specific create/association record binds that head to `proof_desired_commit` without overwrite. |
| `head_evidence_observation_id` | Exact object/head/search Observation used by the ownership classifier; required for `POSITIVE` and bound to this stable ID/head. |
| `ownership_defect_codes` | Canonically sorted set drawn only from `CREATE_PROVENANCE_MISSING`, `CREATOR_AUTHORITY_MISMATCH`, `EFFECT_GENERATION_MISMATCH`, `REF_MISMATCH`, `MARKER_MISMATCH`, `DESIRED_COMMIT_MISMATCH`, `HEAD_UNPROVEN`, or `DURABLE_ASSOCIATION_MISMATCH`. Empty for `POSITIVE`; non-empty otherwise. A positive contradictory fact uses `INCOMPATIBLE`; absence, stale evidence, or an unavailable proof uses `INCOMPLETE`. |
| `ownership_proof_digest` | Domain-separated digest of the complete normalized ownership-status/proof union, including every create/effect/creator/ref/marker/desired-commit/head/body and defect binding above. |
| `external_reliance_digest` | Digest of normalized non-Orcest review/discussion/merge/reliance evidence; required for every member, including canonical empty evidence. |
| `member_digest` | Domain-separated digest of every normalized field above except the parent ID and ordinal. |

The result contains every and only matching object returned by the one complete
adapter search revision. An open, unmerged object is `LIVE`; a closed or merged
object is `TERMINAL`. The same stable ID cannot appear twice or in both lists.
`live_cardinality` is `ZERO`, `ONE`, or `MULTIPLE` according to the number of
`LIVE` rows only. Terminal rows are retained indefinitely with the Observation
for ownership/recovery audit and do not increase live cardinality. They do not
block ordinary live convergence except that any `POSITIVE` owned `MERGED`
terminal row has the terminal precedence defined below.

Ownership classification is code-owned and mechanical. `POSITIVE` requires
all proof-kind-specific fields and exact Project, registered creator,
Publication, Effect generation, stable ID, deterministic ref, marker, desired
commit, observed head, body revision, and evidence-Observation bindings; one
missing binding is never treated as positive. A proved contradiction is
`INCOMPATIBLE` and enters the typed ownership-conflict path unless a positive
owned merged terminal row has already established the stronger terminal fact.
Merely missing, stale, rate-limited, or ambiguous proof is `INCOMPLETE`; it
causes another exact read/complete search with bounded backoff and authorizes
neither association, terminalization, cleanup mutation, nor a Human Boundary.

The classifier is an exhaustive tagged union, not a best-effort score:

| Status/proof tag | Mechanical acceptance predicate |
| --- | --- |
| `POSITIVE/EXACT_CREATE_RESPONSE` | Exact satisfied create checkpoint and request key join the same Effect generation; registered creator, Project/ref/marker, desired commit, observed head/body/marker evidence, and all proof digests are present and equal. |
| `POSITIVE/AMBIGUOUS_CREATE_RECONCILED` | Exact prior ambiguous create checkpoint/request key joins the same Effect generation, and the complete search supplies the same creator/ref/marker/desired-commit/head proof. |
| `POSITIVE/LIVE_ASSOCIATION` | Durable Publication association supplies the same Effect generation/creator/ref/marker/desired-commit/head proof; create checkpoint and request key are both absent. |
| `INCOMPATIBLE` or `INCOMPLETE` | `proof_kind` is absent; `ownership_proof_digest` covers the non-positive status and its non-empty canonically ordered defects; any retained provenance is evidence only. |

Every other nullability or cross-field combination is rejected before a search
Observation becomes lifecycle authority. This is the same mechanical union
specified in [persistence and recovery](persistence-and-recovery.md), so a
positive member can never be inferred merely from a marker or a matching ID.

`duplicate_set_digest` is exactly:

```text
"sha256:" + lowercase_hex(SHA256(
  ascii("orcest-change-request-search-set-v1") || 0x00 ||
  canonical_json({
    "search_revision": external_revision,
    "live": [LIVE member fields including ownership_proof_digest and member_digest in ordinal order],
    "terminal": [TERMINAL member fields including ownership_proof_digest and member_digest in ordinal order]
  })
))
```

The canonical JSON uses the normalized field names above and contains no
controller-assigned Observation ID or ordinal. The Observation payload,
memberships, count/cardinality, and digest commit atomically. A missing member,
wrong class/order, count mismatch, duplicate ID, invalid terminal nullability,
or digest mismatch rejects the result before it becomes a trigger.

`CHANGE_REQUEST_SEARCH_RESULT` is the only pre-reconciliation input that proves
a complete same-marker set. An individual discovery, head,
feedback, merge, or close observation may invalidate an in-flight duplicate
search and request a new complete read, but cannot itself assert a set digest
or authorize duplicate `RECONCILE`. Both duplicate Reconciliation Fact kinds
copy the complete-search revision/digest from their exact causal search-result
Observation.

### Terminal Duplicate Cleanup Reservation

A positive owned merged terminal search member is final forge authority even
when the same complete search still contains live same-marker objects. The
controller selects the bytewise-lowest `change_request_external_id` among
`TERMINAL/MERGED/POSITIVE` rows, atomically establishes that exact Publication
association and terminal proof, fences semantic/publication work, sets the Run
to `MERGED`, and creates one durable `Terminal Duplicate Cleanup Reservation`.
The cleanup is controller-owned post-terminal reconciliation, not Run semantic
work; it survives controller restart and Run terminalization and can never
change or weaken the selected merge.

| Field | Requirement |
| --- | --- |
| `terminal_duplicate_cleanup_reservation_id` | Controller-assigned UUID; unique per selected terminal search Observation/merged member. |
| `project_id` / `run_id` / `publication_id` | Exact terminal Run and associated Publication. |
| `selected_search_observation_id` / `selected_merged_member_ordinal` | Exact complete search and its selected `TERMINAL/MERGED/POSITIVE` member. |
| `selected_change_request_external_id` / `selected_head` / `selected_merge_commit` | Exact selected stable ID, final head, and merge commit copied from that member. |
| `proof_publication_effect_generation` / `creator_installation_or_account_ref` / `deterministic_ref` / `run_marker` | Exact selected ownership proof and Publication authority. |
| `complete_search_revision` / `duplicate_set_digest` | Exact full search pair that selected the merge. |
| `member_count` / `members_digest` | Count and domain-separated digest of the ordered reservation members below; zero is valid. |
| `state` / `next_member_ordinal` | Closed monotonic projection `ACTIVE` or `COMPLETED` and the first unresolved ordinal. A zero-member reservation is created `COMPLETED`. |
| `reservation_digest` | Digest of every immutable field and ordered member digest. |
| `created_transition_sequence` / `created_at_ms` / `completed_at_ms` | Exact terminalizing Transition, informational creation time, and completion time required only for `COMPLETED`. |

The immutable `Terminal Duplicate Cleanup Member` relation contains every and
only `LIVE` member of the selecting search, in the same bytewise stable-ID
order; the selected terminal row is never a cleanup member:

| Field | Requirement |
| --- | --- |
| `terminal_duplicate_cleanup_reservation_id` / `member_ordinal` | Parent and zero-based contiguous ordinal. |
| `search_member_ordinal` / `change_request_external_id` | Exact source `LIVE` member and stable ID; stable IDs are unique in the reservation. |
| `observed_head` / `observed_body_revision` / `marker_set_digest` | Exact member CAS evidence frozen by the search. |
| `ownership_status` / `ownership_proof_digest` / `external_reliance_digest` | Exact source member classifications and proofs. |
| `planned_action` | Code-owned `CLOSE`, `DETACH_MARKER`, or `RECORD_ONLY`. `CLOSE` requires `POSITIVE` plus canonical-empty external reliance. `DETACH_MARKER` requires `POSITIVE`, non-empty reliance, and exact head/body/marker CAS evidence. Every `INCOMPATIBLE`, `INCOMPLETE`, or otherwise unsafe member is `RECORD_ONLY`. |
| `member_digest` | Domain-separated digest of every normalized field except parent and ordinal. |

One `Terminal Duplicate Cleanup Action` generation at a time processes the
reservation's `next_member_ordinal`:

| Field | Requirement |
| --- | --- |
| `terminal_duplicate_cleanup_action_id` | Controller-assigned UUID. |
| `terminal_duplicate_cleanup_reservation_id` / `member_ordinal` / `action_generation` | Exact member and positive monotonically increasing retry/reconciliation generation; unique. |
| `action_kind` | Exact copied `CLOSE`, `DETACH_MARKER`, or `RECORD_ONLY`. |
| `record_reason` | `EXTERNAL_RELIANCE`, `INCOMPLETE_PROOF`, `INCOMPATIBLE_OWNER`, or `CAS_UNSAFE` exactly for `RECORD_ONLY`; otherwise `NULL`. |
| `expected_head` / `expected_body_revision` / `expected_marker_set_digest` | Required exact frozen CAS preimage for `CLOSE` or `DETACH_MARKER`; `NULL` for `RECORD_ONLY`. |
| `operation_idempotency_key` / `operation_digest` / `outbox_id` | Required stable mutation identity, normalized operation digest, and reciprocal Outbox for `CLOSE` or `DETACH_MARKER`; all `NULL` for `RECORD_ONLY`. |
| `state` | Closed monotonic projection `PENDING`, `ACTIVE`, `COMPLETED`, or `SUPERSEDED`. |
| `outcome` | `CLOSED`, `MARKER_DETACHED`, `RETAINED_AUDIT`, or `NULL` until terminal. `RETAINED_AUDIT` is valid only for `RECORD_ONLY`. |
| `forge_observation_id` / `completed_at_ms` | Completion time is required exactly when terminal. Observation is required for a completed mutation or mismatch-superseded Action and `NULL` for `RECORD_ONLY`, which completes without external I/O; both are `NULL` while nonterminal. |
| `action_input_digest` / `result_digest` | Domain-separated digest of immutable action input, plus a separate normalized terminal-result digest required exactly for `COMPLETED` or `SUPERSEDED`. |

`(terminal_duplicate_cleanup_reservation_id, member_ordinal,
action_generation)` is unique, and at most one nonterminal Action exists per
Reservation. Reservation creation and all zero-member completion occur in the terminalizing
writer transaction; a zero-member Reservation creates no cleanup Schedule,
Request, Action, or Outbox. Otherwise the controller creates the first Action and,
for a mutation, its effect-fenced Outbox before I/O. `CLOSE` calls only the
exact-owned, reliance-free close primitive. `DETACH_MARKER` removes only this
Run marker from the non-selected object under exact head/body/marker CAS and
preserves all other body and external work. `RECORD_ONLY` records a bounded
reason and mutates nothing. A CAS mismatch or ambiguous response never retries
blindly: it records the exact Forge Observation, supersedes the Action, and
uses a fresh read/search to either create a higher Action generation or finish
as `RETAINED_AUDIT`. The controller processes members in ordinal order and
marks the Reservation `COMPLETED` only when every member has one terminal
outcome. These cleanup changes append same-state terminal audit Transitions;
they do not reopen the Run or alter its selected stable ID, head, or merge
commit.

`REF_ABSENT` and `CHANGE_REQUEST_ABSENT` are disjoint facts. A
`CHANGE_REQUEST_SEARCH/OBSERVED_ABSENT` checkpoint MUST name a
`CHANGE_REQUEST_ABSENT` observation with the exact Publication, source
repository, deterministic ref, Run marker, search revision, and nonexistence
token frozen by that search. `REF_ABSENT` is valid only for `REF_READ` and
cannot prove that a Change Request is absent. A successful pre-link
`CLOSE_PUBLICATION` Fact uses the same current `CHANGE_REQUEST_ABSENT`
observation; a ref read, empty local result, timeout, or synthesized token is
not an absence proof.

The same non-null `adapter_event_id` MUST be idempotently recorded. For sources
without a delivery ID, only a normalized payload identical to the immediately
preceding accepted observation for the same target is coalesced. An A -> B -> A
sequence therefore records all three ordered facts; it MUST NOT be collapsed by
a global payload-digest uniqueness rule.
`(project_id, target_kind, target_id, observation_sequence)` MUST be unique.
Conflicting external facts are ordered by the target's `observation_sequence`
and resolved by adapter-specific revision rules before reducer evaluation. A
remediation Activity binds to the exact Forge Observation and head commit that
caused it.

Every accepted Forge Observation applicable to an active Run or its retained
terminal cleanup Reservation is consumed by exactly one
`T(FORGE_OBSERVATION, forge_observation_id)` Transition. When it
changes no lifecycle state, the reducer still appends a same-state Transition
that records any latest-observation or Snapshot-capture effects. An observation
used as the admission Work Item trigger and the separate trusted-base
Observation named by `anchored_base_observation_id` are both consumed by the
single `T(ADMIT, work_item_forge_observation_id)` and are never also consumed
as `FORGE_OBSERVATION`. Transition uniqueness for these identities is
independent of specification generation. Later work that consults
an already-consumed observation uses an `INTERNAL` continuation from the
Transition that established the relevant state; it MUST NOT reuse the old
Forge Observation as a new trigger.

## Wait Condition

A Wait Condition is the immutable durable predicate that makes a `WAITING` Run
resumable without Redis or operator reconstruction.

| Field | Requirement |
| --- | --- |
| `wait_condition_id` | Controller-assigned UUID. |
| `run_id` | Owning Run. |
| `reason` | `CAPACITY`, `RATE_LIMIT`, `BUDGET`, `BACKOFF`, `EXTERNAL_DEPENDENCY`, `FORGE_UNAVAILABLE`, `STORAGE_RECOVERY`, `SECRET_RECOVERY`, or `EVIDENCE`. |
| `resume_state` | Nonterminal state to revalidate, never a blind destination. |
| `specification_generation` / `candidate_id` / `policy_hash` / `forge_observation_id` | Exact applicable bindings; nullable only when the Run has no such current object. |
| `not_before_ms` | Earliest timer satisfaction time, or `NULL`. |
| `wake_kind` | `CAPACITY`, `RATE_LIMIT_RESET`, `BUDGET_WINDOW`, `DEPENDENCY`, `FORGE`, `STORAGE`, `SECRET`, or `EVIDENCE`; `NULL` only for timer-only `BACKOFF` or `RATE_LIMIT`. Timer authority is `not_before_ms` plus its scoped Timer Fact, not a second wake kind. |
| `wake_identity` | Bounded canonical non-secret object naming the exact scope, target, and minimum revision/event needed by `wake_kind`; `NULL` only when `wake_kind` is `NULL`. |
| `health_observation_ids_digest` | SHA-256 of the complete ordered bound Health Observation membership below; required for `CAPACITY` and other health-selected waits, otherwise the canonical-empty digest. |
| `panel_slots_digest` | SHA-256 of the complete ordered panel-slot membership below; non-empty exactly for panel-scoped `CAPACITY`, canonical-empty otherwise. |
| `created_from_kind` / `created_from_id` | Exact closed trigger kind/ID reduced to create this condition: `ATTEMPT_RESULT`, `ATTEMPT_TERMINAL`, `CONTROLLER_OPERATION`, `RECOVERY_EVIDENCE`, `HEALTH_OBSERVATION`, `FORGE_OBSERVATION`, `POLICY_UPDATE`, `MANAGEMENT_COMMAND`, `STORAGE_RESTORATION`, `SECRET_VERSION`, `TIMER_FACT`, or `INTERNAL`. Forge Request Failure and Budget Report first create Recovery Evidence; their Wait therefore names that Evidence, never the original Fact/Report directly. |
| `condition_digest` | Digest of all immutable predicate and binding fields. |
| `created_transition_sequence` / `created_at_ms` | Creating Transition and informational time. |

At least one of `not_before_ms` or `wake_kind` MUST be present. A Wait
Condition is current exactly when `Run.wait_condition_id` names it and the Run
is `WAITING`; at most one can be current. Satisfaction or supersession clears
that Run reference in the same Transition transaction but never edits or
deletes the Wait Condition. A timer firing or external observation is merely a
candidate trigger until the reducer rechecks `condition_digest` and every
current binding. `condition_digest` is not object identity or a global dedupe
key. If the same predicate becomes necessary again after satisfaction or
supersession, the controller creates a new `wait_condition_id` and Transition;
it never resurrects the old row.
`EVIDENCE` always has a non-`NULL` `not_before_ms`, `wake_kind = EVIDENCE`, and
the exact `orcest.evidence-wake/1` identity below; an event-only or timer-only
evidence wait is invalid.
When both `not_before_ms` and `wake_kind` are present, their candidate-wake
semantics are logical OR, never AND: either the exact due Timer Fact or one
exact matching event input may propose satisfaction. The reducer still
rechecks the full immutable condition and every current binding, so satisfying
one arm cannot bypass a stale Candidate, generation, policy, or external
revision fence.

The reason/wake compatibility matrix is closed:

| `reason` | Required timer/event shape |
| --- | --- |
| `CAPACITY` | `wake_kind = CAPACITY`; `not_before_ms` is `NULL`; exact frozen Health and optional panel-slot memberships are required. |
| `RATE_LIMIT` | `not_before_ms` is required; `wake_kind` is `NULL` for timer-only recovery or `RATE_LIMIT_RESET` for a typed earlier reset event. |
| `BUDGET` | `not_before_ms` equals the persisted budget reset boundary and `wake_kind = BUDGET_WINDOW`; the exact budget identity below is required. |
| `BACKOFF` | `not_before_ms` is required and `wake_kind`/`wake_identity` are both `NULL`. |
| `EXTERNAL_DEPENDENCY` | `wake_kind = DEPENDENCY`; `not_before_ms` is optional only as a bounded reconciliation fallback. |
| `FORGE_UNAVAILABLE` | `wake_kind = FORGE` and `not_before_ms` is required; either arm proposes revalidation. |
| `STORAGE_RECOVERY` | `wake_kind = STORAGE`; `not_before_ms` is optional only as a bounded restoration retry. |
| `SECRET_RECOVERY` | `wake_kind = SECRET`; `not_before_ms` is optional only as a bounded rotation/reconciliation retry. |
| `EVIDENCE` | `wake_kind = EVIDENCE` and `not_before_ms` are both required, with the exact evidence identity below. |

No other combination is valid. `health_observation_ids_digest` is nonempty
only when the selecting decision actually consulted Health Observations;
`panel_slots_digest` is nonempty only for panel `CAPACITY`.

For `wake_kind = BUDGET_WINDOW`, `wake_identity` contains exactly
`project_id`, `accounting_scope_id`, `budget_policy_ref`,
`budget_reset_window_ref`, exhausted `budget_report_id`, `window_id`,
`reset_at_ms`, and `minimum_source_sequence = exhausted source_sequence + 1`.
The timer arm at `reset_at_ms` merely starts reconciliation; work cannot be
offered until the latest applicable authenticated Budget Report is
`AVAILABLE`. The event arm accepts only an `AVAILABLE` Report matching all
fields and at least the minimum sequence.

Budget Wait insertion is serialized against Report acceptance. While applying
`WAIT_BUDGET`, the writer re-reads the latest exact applicable Report. If it is
still the Evidence's exhausted Report, it may insert the Wait. If a later
`AVAILABLE` Report already satisfies the minimum, it inserts no Wait and
appends one successor Recovery Evidence selecting the ordinary origin-valid
retry/resume tactic. If a later `EXHAUSTED` Report has superseded the causal
one, it likewise inserts no stale Wait and appends a new source-unique BUDGET
Recovery Evidence bound to that newer Report. Only the successor's Transition
may continue. Thus Report acceptance immediately before Wait creation cannot
miss the Run or strand it until another accounting event.

For `wake_kind = FORGE`, `wake_identity` contains exactly `forge_instance_id`,
`forge_observation_schedule_id`, target kind/ID, the causal
`forge_request_failure_fact_id`, minimum Schedule revision, and a nonempty
sorted set of result Observation kinds allowed by that Schedule. A matching
later accepted Forge Observation or verified `FORGE_CONNECTIVITY/AVAILABLE`
Health Observation may propose wake; both still revalidate Request/Schedule,
Run, Publication, generation, and policy bindings.

Forge-unavailable Wait insertion is serialized with Request completion and
Health/Observation acceptance. While applying a FORGE_TRANSIENT
`WAIT_EXTERNAL` tactic, the writer rechecks the exact Request, Schedule,
target, minimum revision, and allowed observations. If qualifying success or
verified connectivity is already current, it inserts no Wait and appends one
successor Recovery Evidence selecting the origin-valid retry/reconciliation
tactic. If not, it inserts the timer-plus-FORGE Wait. A successful response
immediately before Wait creation therefore cannot become an already-consumed
wake that strands the Run.

Wait Condition Health Observation membership is persisted as
`(wait_condition_id, observation_ordinal, health_observation_id)`. Ordinals are
zero-based/contiguous, IDs are unique, and rows are sorted by
`(scope_kind, scope_id, health_sequence, health_observation_id)`. They contain
the highest-applicable unexpired observations actually used by the planning
Transition and reproduce `health_observation_ids_digest`; later health cannot
rewrite this historical decision.

A panel-scoped `CAPACITY` Wait additionally persists `Wait Condition Panel
Slot` rows:

| Field | Requirement |
| --- | --- |
| `wait_condition_id` / `slot_ordinal` | Parent Wait and contiguous zero-based canonical slot order. |
| `activity_id` | Exact already-planned unfilled `REVIEW` or sole `ADJUDICATE` Activity. |
| `assignment_kind` / `panel_round` | Exact Activity Review Assignment kind and Candidate-local panel round. |
| `slot_id` | Reviewer slot ID, or exact `default` for the sole adjudicator. |

Activity IDs and `(assignment_kind,panel_round,slot_id)` are each unique in a
Wait. Rows include every and only the currently unfilled panel Activities,
each of which has no live `OFFERED` or `CLAIMED` Attempt after the creating
transaction's peer-offer supersession; they sort by assignment kind then configured slot order, and reproduce
`panel_slots_digest`. The Wait, both complete memberships, Activities/
Assignments/subjects, and creating Transition commit atomically. A panel wait
cannot be reconstructed from Redis or a mutable roster.

For `wake_kind = SECRET`, `wake_identity` contains exactly logical `secret_id`,
registered `purpose`, and positive `minimum_version`; a lower version cannot
wake it. A Wait caused by a frozen Claim or Publication Effect uses
`minimum_version = frozen_ref.version + 1` and retains the failed exact
reference in its evidence. Under the per-Secret lock, both Wait selection and
the `T(RECOVERY_EVIDENCE,...)` reduction that would insert it re-read the
logical Secret's current verified reference. If its version already meets the
minimum, the reducer selects/applies the corresponding retry tactic or appends
the next retry Recovery Evidence and creates no Wait. A concurrent rotation
therefore cannot strand a stale Secret Wait.

For `wake_kind = EVIDENCE`, `wake_identity` is the exact bounded canonical
object:

```json
{
  "protocol": "orcest.evidence-wake/1",
  "project_id": "<project UUID>",
  "target_kind": "WORK_ITEM|PUBLICATION",
  "target_id": "<stable target identity>",
  "minimum_observation_sequence": 1,
  "allowed_observation_kinds": ["<sorted closed Forge Observation kind>"],
  "predicate_digest": "sha256:<hex>"
}
```

The minimum sequence is positive and exactly one greater than the highest
accepted sequence for that target examined by the Recovery Evidence planning
decision. `allowed_observation_kinds` is non-empty, duplicate-free, and sorted
by the Domain's closed kind string; `predicate_digest` binds the exact current
Candidate/panel/disputed-finding or other code-owned evidence predicate and
every applicable generation/policy/head fence. Only a matching accepted Forge
Observation at or above that sequence may satisfy the event arm.

The `T(RECOVERY_EVIDENCE,...)` writer transaction that would insert an
`EVIDENCE` Wait first rechecks the exact target, minimum sequence, allowed
kinds, predicate digest, and current bindings under the serialized writer. If
matching evidence is already accepted, it inserts no Wait; it appends one
successor Recovery Evidence with `source_kind = RECOVERY_EVIDENCE`,
`source_id =` the current Evidence ID, and the deterministic ordinary
retry/replacement tactic for the same category. The next
`T(RECOVERY_EVIDENCE,...)` applies that tactic. If no match exists, the Wait is
inserted atomically with both timer and event arms. This same predicate check
runs on wake, so a stale revision can neither create nor satisfy a Wait.

## Recovery Evidence

Recovery Evidence is an append-only, per-Run ordered record of classified
failure and the deterministic recovery projection after applying it.

| Field | Requirement |
| --- | --- |
| `recovery_evidence_id` | Controller-assigned UUID. |
| `run_id` / `recovery_sequence` | Owning Run and strictly increasing per-Run sequence. |
| `source_kind` / `source_id` | Exact closed Transition input kind/ID: `ATTEMPT_RESULT`, `ATTEMPT_TERMINAL`, `CONTROLLER_OPERATION`, `FORGE_REQUEST_FAILURE`, `HEALTH_OBSERVATION`, `FORGE_OBSERVATION`, `BUDGET_REPORT`, `RECONCILIATION_FACT`, `RECOVERY_EVIDENCE`, `POLICY_UPDATE`, `STORAGE_RESTORATION`, `SECRET_VERSION`, `MANAGEMENT_COMMAND`, `TIMER_FACT`, or `INTERNAL`. `RECOVERY_EVIDENCE` is legal only for the stale-EVIDENCE-Wait avoidance chain defined above. |
| `resumed_wait_condition_id` | Exact satisfied/superseded Wait Condition whose `resume_state` was copied into `Run.recovery_origin_state`, otherwise `NULL`. |
| `resumed_human_boundary_id` / `human_resolution_id` | Exact resolved Boundary/Resolution whose `resume_state` was copied into `Run.recovery_origin_state`, or both `NULL`. |
| `activity_id` / `attempt_id` | Affected execution, or `NULL` for controller/external recovery. |
| `specification_generation` / `candidate_id` / `forge_observation_id` | Exact recovery bindings when applicable. |
| `category` | Code-owned lifecycle recovery category. |
| `failure_fingerprint` | Digest of the normalized category, scope, exact bindings, and bounded evidence. |
| `strategy_index` | Code-owned recovery-ladder position after applying this evidence. |
| `selected_tactic` | One closed lifecycle recovery-tactic enum value selected after applying this evidence. |
| `attempt_count` / `repair_cycle_count` / `diagnosis_count` / `rescue_epoch` | Nonnegative counters after applying this evidence. |
| `selected_fallback` | Selected profile/account/tactic identifier from pinned stable order, or `NULL`. |
| `health_observation_ids_digest` | SHA-256 of the canonical ordered Health Observation membership actually consulted for fallback/capacity selection; required even for the empty list. |
| `next_eligible_at_ms` | Persistent next eligibility time, or `NULL`. |
| `evidence_digest` / `recorded_at_ms` | Canonical record digest and informational time. |

`(run_id, recovery_sequence)` and `(run_id, source_kind, source_id)` MUST be
unique. `Run.current_recovery_evidence_id` is a projection of the latest
applied record. Counters and fallbacks never live only in Redis; replaying the
same ordered Recovery Evidence MUST choose the same next tactic.

Every Transition entering `RECOVERING` appends exactly one Recovery Evidence
from that Transition's persisted source in the same writer transaction. An
`INTERNAL` trigger may itself be that persisted source, but it cannot enter
`RECOVERING` first and append the Evidence later. A Wait wake
copies the immutable condition's `resume_state` into
`Run.recovery_origin_state`, sets the exact Wait pointer on Run and Evidence,
and classifies the wake from the closed Wait reason. A Human Resolution does
the same with the Boundary's `resume_state` and both Human IDs. The next
reduction is always `T(RECOVERY_EVIDENCE,recovery_evidence_id)`; it revalidates
the frozen origin and either resumes it, selects another tactic, or creates a
new Wait. Direct `WAITING -> work-state` and generic
`NEEDS_HUMAN -> work-state` shortcuts are illegal.

Panel-scoped `STAFF_PANEL` Evidence MUST have a non-`NULL`
`resumed_wait_condition_id`. Its immutable planned-slot membership and digest
are inherited solely through that exact Wait Condition's child rows; the
Evidence does not duplicate or silently recompute them. Its own ordered Health
membership records the newer evidence consulted at wake. Applying the tactic
revalidates that every inherited Activity/slot remains current and unfilled;
any change offers nothing and creates a fresh fully bound panel Wait.

Persistence normalizes the health evidence as Recovery Evidence Health
Observation rows `(recovery_evidence_id, observation_ordinal,
health_observation_id)`. At creation, the controller freezes at most the highest
applicable unexpired ordered Health Observation for each policy-relevant scope,
sorts rows by `(scope_kind, scope_id, health_sequence,
health_observation_id)`, and requires zero-based contiguous ordinals and unique
Health Observation IDs. The ordered IDs reproduce
`health_observation_ids_digest`; `evidence_digest` covers that digest. The
Recovery Evidence, complete membership, selected fallback/counters, and
Transition commit atomically. Later Health Observations cannot reclassify the
historical selection; a later recovery input creates a new record.

## Capacity Report

A Capacity Report is the durable authenticated request/replay ledger for one
pool-manager capacity submission.

| Field | Requirement |
| --- | --- |
| `capacity_report_id` | Controller-assigned UUID and stable internal identity. |
| `pool_manager_id` / `report_id` | Authenticated registered pool manager and caller-assigned UUID; unique source identity. |
| `idempotency_key` | Caller-assigned UUID, unique for that pool manager. |
| `report_sequence` | Positive strictly increasing accepted revision per pool manager; gaps allowed. |
| `observed_at_ms` / `expires_at_ms` | Bounded caller evidence time and required controller-validated expiry. |
| `configured_max_ttl_ms` | Positive server-owned maximum capacity TTL frozen at acceptance. |
| `entries` | Canonically ordered non-empty bounded list of registered session/profile/pool scopes and normalized evidence. |
| `authenticated_principal_id` / `authorization_context_digest` | Exact transport principal and registration/RBAC proof. |
| `payload_digest` | Digest of protocol and complete canonical request body. |
| `health_observation_ids` | Ordered one-for-one IDs inserted for `entries`. |
| `response_digest` / `accepted_at_ms` | Digest of the complete stable response and informational acceptance time. |

`(pool_manager_id, report_id)`, `(pool_manager_id, idempotency_key)`, and
`(pool_manager_id, report_sequence)` are unique. A replay with both caller
identities and the same `payload_digest` returns the stored response, including
the same Health Observation IDs/sequences and wake results. Reuse with a
different body is an idempotency conflict. A previously unseen report whose
sequence is not greater than the last accepted sequence is rejected. The
Capacity Report, all Health Observations, per-Run wake Transitions/outboxes, and
response commit in one writer transaction; a crash exposes either none or the
complete replayable result.
For every report and resulting capacity Health Observation,
`expires_at_ms > accepted_at_ms` and
`expires_at_ms <= accepted_at_ms + configured_max_ttl_ms`. The controller's
accepted time, not caller `observed_at_ms`, is the TTL origin; `effective_at_ms`
for those Observations equals `accepted_at_ms`. A report violating either bound
is rejected without a ledger or Health Observation.

## Worker Loss Report

A Worker Loss Report is the durable authenticated request/replay ledger for
positive loss or isolation of one exact claimed Worker Session.

| Field | Requirement |
| --- | --- |
| `worker_loss_report_id` | Controller-assigned UUID. |
| `pool_manager_id` / `idempotency_key` | Authenticated registered pool manager and caller UUID; unique source identity. |
| `worker_id` / `worker_session_id` | Exact registered worker and non-reused session. |
| `attempt_id` / `activity_id` / `attempt_generation` | Exact claimed Attempt fence supplied by the report. |
| `reason` | `VM_DESTROYED`, `VM_MISSING`, `CEILING_TIMEOUT`, or `OPERATOR_DRAIN`. |
| `observed_at_ms` | Bounded evidence time. |
| `authenticated_principal_id` / `authorization_context_digest` | Exact transport principal and registered session/pool authority proof. |
| `payload_digest` | Digest of the canonical request. |
| `outcome` | `ACCEPTED` or `STALE`. |
| `health_observation_id` / `attempt_terminal_fact_id` | Both required for `ACCEPTED`; both `NULL` for `STALE`. |
| `response_digest` / `accepted_at_ms` | Stable response digest and informational acceptance time. |

`(pool_manager_id, idempotency_key)` is unique. Same-body replay returns the
same outcome and object IDs; conflicting reuse fails. `ACCEPTED` requires the
exact current claimed Attempt/session and positive loss/isolation evidence.
An otherwise authorized report whose exact
`(attempt_id, activity_id, attempt_generation)` does not exist returns
`404 ATTEMPT_UNKNOWN` and creates no Worker Loss Report; its separate bounded
security/operational audit is not lifecycle authority. `STALE` is reserved for
an existing Attempt triple whose state, current generation, or claimed session
no longer matches.
In one writer transaction the controller inserts the report, a
`WORKER_SESSION/LOST` Health Observation sourced by
`WORKER_LOSS_REPORT/worker_loss_report_id`, and the matching `WORKER_LOST`
Attempt Terminal Fact sourced by that Health Observation, then reduces the
terminal fact. A stale report inserts only its replay ledger and cannot append
a Health Observation, terminal fact, or Transition. Capacity
`UNAVAILABLE`—including an unreachable session hint—never substitutes for this
endpoint or fences a claimed Attempt.

## Health Probe Request

A Health Probe Request is durable pre-I/O intent. The controller MUST commit
it and its Outbox row before calling a forge, provider, Storage, or Secret
Store. Restart resumes the same request identity; it never fabricates a Fact
from an unrecorded response.

| Field | Requirement |
| --- | --- |
| `health_probe_request_id` | Controller-assigned UUID and immutable request/replay identity. |
| `probe_kind` | `FORGE_CONNECTIVITY`, `PROVIDER_ACCOUNT_STATUS`, `STORAGE_OBJECT_INTEGRITY`, or `SECRET_VERSION_INTEGRITY`. |
| `scope_kind` / `scope_id` | Exact closed scope and canonical ID defined below. |
| `expected_prior_health_sequence` | Nonnegative exact highest accepted sequence for this scope when intent is created; completion CASes it and assigns the next sequence. |
| `object_kind` / `object_id` | Exact integrity target when required; otherwise both `NULL`. |
| `forge_credential_secret_ref` | Exact current version of `ForgeInstance.credential_secret_id`, frozen for `FORGE_CONNECTIVITY`; otherwise `NULL`. |
| `provider_secret_ref` | Exact provider-account Secret Reference for `PROVIDER_ACCOUNT_STATUS`; otherwise `NULL`. |
| `subject_bindings` | Exact canonical scope-identity document, including the kind-specific object and frozen credential-version bindings; copied byte-for-byte to the resulting Fact and Observation. |
| `probe_implementation_id` / `probe_input_json` / `probe_input_digest` | Code-owned implementation and complete bounded canonical non-secret input. |
| `requested_at_ms` / `not_after_ms` | Controller times; retry must not begin I/O at or after `not_after_ms`. |
| `state` | Monotonic `PENDING`, `COMPLETED`, or `SUPERSEDED`. |
| `outbox_id` | Exact durable probe-dispatch Outbox row committed with this Request. |
| `health_probe_fact_id` | Exact completed Fact, required only for `COMPLETED`; `NULL` otherwise. |
| `request_digest` | Digest of every immutable field above except state/fact pointer. |

Request ID is unique. A retry uses the same request and adapter request
identity. Completion atomically inserts the Fact and Health Observation, sets
the reciprocal Fact pointer, and marks the Request `COMPLETED`; a stale or
cancelled intent becomes `SUPERSEDED` without a Fact. A conflicting response
for a completed request is an integrity error.

## Health Probe Fact

A Health Probe Fact is the sole durable controller-owned source for non-worker
health probes. An adapter or arbitrary controller call cannot insert a Health
Observation directly.

| Field | Requirement |
| --- | --- |
| `health_probe_fact_id` | Controller-assigned UUID and source identity. |
| `health_probe_request_id` | Exact unique completed Health Probe Request; reciprocal and immutable. |
| `probe_kind` | `FORGE_CONNECTIVITY`, `PROVIDER_ACCOUNT_STATUS`, `STORAGE_OBJECT_INTEGRITY`, or `SECRET_VERSION_INTEGRITY`. |
| `probe_sequence` | Positive monotonic sequence within `(probe_kind, scope_kind, scope_id)`. |
| `scope_kind` / `scope_id` | Exact scope selected by the closed matrix below. |
| `outcome` | Exact Health Observation kind selected by the matrix below. |
| `object_kind` / `object_id` | Exact integrity target when required by the matrix; otherwise both `NULL`. |
| `forge_credential_secret_ref` | Exact version copied from the Request for `FORGE_CONNECTIVITY`; otherwise `NULL`. |
| `provider_secret_ref` | Exact version copied from the Request for `PROVIDER_ACCOUNT_STATUS`; otherwise `NULL`. |
| `integrity_failure_code` | For an integrity probe with `outcome = UNAVAILABLE`, `MISSING`, `UNREADABLE`, `DIGEST_MISMATCH`, or `KEYED_ATTESTATION_MISMATCH` as permitted below; `NULL` for a positive integrity result and every non-integrity probe. |
| `observed_revision` | Authenticated forge/provider revision when one exists; otherwise `NULL`. |
| `effective_at_ms` / `expires_at_ms` | Controller time and optional bounded expiry. Integrity-probe results do not expire and require `expires_at_ms = NULL`. |
| `configured_max_ttl_ms` | Positive server-owned maximum when `expires_at_ms` is non-`NULL`; otherwise `NULL`. |
| `probe_implementation_id` / `probe_input_digest` / `evidence_digest` | Code-owned probe version, digest of exact non-secret inputs, and bounded normalized evidence digest. |
| `subject_bindings` | Exact canonical scope-identity document copied from the Request, including kind-specific object and frozen credential-version bindings. |
| `probe_fact_digest` | Required domain-separated digest of every normalized immutable source field, explicitly including `affected_run_ids_digest`; it excludes only derived `health_observation_id`, mutable fanout cursor/completion, and informational `recorded_at_ms`. |
| `health_observation_id` | Exact Health Observation inserted atomically from this Fact. |
| `affected_run_ids_digest` | Digest of the canonical ordered Health Probe Fact Run membership frozen at completion, including the canonical empty set. |
| `fanout_cursor_ordinal` / `fanout_completed_at_ms` | Mutable delivery projection only: next zero-based member to reduce and completion time when the cursor equals membership cardinality. Neither is health or lifecycle authority. |
| `recorded_at_ms` | Informational commit time. |

`scope_id` is always
`sha256:` plus lowercase SHA-256 of
`ascii("orcest-health-scope-v1") || 0x00 || canonical_json(scope_identity)`.
The tagged `scope_identity` is closed: forge instance plus exact frozen forge
credential Secret ID/version for `FORGE`; provider,
provider-account ref, and exact provider Secret ID/version for
`PROVIDER_ACCOUNT`; pool-manager/pool for `CAPACITY_POOL`; pool-manager and
worker profile for `WORKER_PROFILE`; pool-manager/worker/session/pool/profile
for `WORKER_SESSION`; exact storage `object_kind/object_id` for `STORAGE`; and
the exact canonical Secret Version key for `SECRET`. `subject_bindings` stores
this exact identity document so the digest is independently reproducible.

The scope/outcome union is closed:

| `probe_kind` | Exact scope and target | Allowed `outcome` |
| --- | --- | --- |
| `FORGE_CONNECTIVITY` | `FORGE` plus exact `forge_instance_id` and frozen `forge_credential_secret_ref`; no object | `AVAILABLE` or `UNAVAILABLE` |
| `PROVIDER_ACCOUNT_STATUS` | `PROVIDER_ACCOUNT` plus exact non-secret `provider_account_ref` and frozen `provider_secret_ref`; no object | `AVAILABLE`, `UNAVAILABLE`, `RATE_LIMITED`, or `EXHAUSTED` |
| `STORAGE_OBJECT_INTEGRITY` | `STORAGE`; object is exact `CANDIDATE_ARTIFACT` bundle digest or `WORKFLOW_BLOB` domain-separated digest | `AVAILABLE` with no code, or `UNAVAILABLE` with `MISSING`, `UNREADABLE`, or `DIGEST_MISMATCH` |
| `SECRET_VERSION_INTEGRITY` | `SECRET`; object is exact canonical Secret Version key | `AVAILABLE` with no code, or `UNAVAILABLE` with `MISSING`, `UNREADABLE`, or `KEYED_ATTESTATION_MISMATCH` |

The controller assigns both sequences and commits the Request completion, Fact,
its one Health Observation, and complete frozen Run membership in the same writer transaction. `(probe_kind, scope_kind,
scope_id, probe_sequence)` and `health_observation_id` are unique. Replay by
Fact ID and identical `probe_fact_digest` returns the same Observation;
conflicting content under that ID is an integrity error. Repository content, workers, raw
adapter callbacks, and free-form diagnostics cannot create this Fact. Secret
probe evidence contains only the canonical Secret Version key and opaque
Secret Store attestation/result identity—never raw secret bytes, an unkeyed
secret digest, or a keyed tag.

An integrity probe never drives lifecycle state through its Request or Fact
identity. The Request/outbox commits before I/O; completion atomically creates
the Fact and exact `AVAILABLE` or `UNAVAILABLE` Observation; only
`T(HEALTH_OBSERVATION,health_observation_id)` appends Recovery Evidence and
enters or remains in `RECOVERING`. A negative
`CANDIDATE_ARTIFACT`/`WORKFLOW_BLOB` result maps
to category `STORAGE`; a negative `SECRET_VERSION` result maps to `CREDENTIAL`.
A positive result disproves the suspicion and selects the ordinary retry/resume
tactic; it never creates a storage/secret Wait. Only a negative result's later,
separate `T(RECOVERY_EVIDENCE,recovery_evidence_id)` selects `WAIT_EXTERNAL`
and creates `STORAGE_RECOVERY` or `SECRET_RECOVERY`. Neither Fact creation nor
the Health Transition may create that Wait directly.
Completion requires the scope's current highest `health_sequence` equal the
Request's `expected_prior_health_sequence` and assigns
`probe_sequence = health_sequence = expected_prior_health_sequence + 1`;
otherwise the Request becomes `SUPERSEDED` and a needed fresh probe receives a
new Request ID.
Every Fact field also frozen by its Request—including scope, subject bindings,
object, forge/provider SecretRef, implementation, and probe-input digest—MUST equal the
Request exactly; a response cannot broaden or retarget probe intent.
For a forge/provider probe with an expiry,
`effective_at_ms < expires_at_ms <= effective_at_ms +
configured_max_ttl_ms`; the positive server-owned maximum is frozen in the
Fact and `probe_input_digest`. Caller/adapter time cannot extend it.
Forge connectivity evidence applies only to the exact
`forge_credential_secret_ref` copied by Request, Fact, Observation, scope, and
digests; credential rotation requires a new probe and cannot let old-version
availability authorize new-version forge work. Provider-account availability or rate evidence applies only to the exact
`provider_secret_ref` copied by Request, Fact, and Observation; rotation
requires a new Request and Observation and cannot make old-version evidence
authorize a new Claim.

### Health Probe Fact Run

The immutable child relation
`(health_probe_fact_id, run_ordinal, run_id)` freezes every active Run whose
current Snapshot, Candidate, Activity/Attempt, Publication, Wait, or Boundary
references the Fact's exact scope/subject bindings at Fact commit. `run_ordinal`
is zero-based and contiguous; rows sort by bytewise `run_id`; both
`(health_probe_fact_id, run_ordinal)` and `(health_probe_fact_id, run_id)` are
unique; and the ordered IDs reproduce `affected_run_ids_digest`. A Run not in
this frozen set cannot consume that Fact's Observation later merely because it
begins using the same scope.

The fanout reconciler processes only `fanout_cursor_ordinal`, in order. For
each member it reduces exactly one
`T(HEALTH_OBSERVATION, health_observation_id)` and atomically advances the
cursor; an existing Transition is successful replay. If the Run still uses the
exact subject and the Observation is lifecycle-relevant, the applicable
health/recovery row runs. If its work, generation, Secret version, Candidate,
Publication, Wait, or Boundary was superseded—or the Observation is otherwise
unrelated to its current state—the required Transition is same-state audit and
changes no counters, evidence, waits, or work. Crash recovery resumes at the
cursor and never recomputes membership. The immutable Fact/Observation and
membership commit together; cursor advancement is the only mutable fanout
projection.

## Health Observation

A Health Observation is an immutable, ordered capacity or dependency-health
input used by recovery and Wait Conditions.

| Field | Requirement |
| --- | --- |
| `health_observation_id` | Controller-assigned UUID. |
| `scope_kind` / `scope_id` | Closed scope kind plus the canonical `orcest-health-scope-v1` hash derived from `subject_bindings`, never a raw stable identity. |
| `health_sequence` | Strictly increasing within `(scope_kind, scope_id)`. |
| `kind` | `AVAILABLE`, `UNAVAILABLE`, `RATE_LIMITED`, `EXHAUSTED`, `LOST`, or `RECOVERED`. |
| `source_kind` / `source_id` | `CAPACITY_REPORT`, `WORKER_LOSS_REPORT`, `STORAGE_RESTORATION`, or `HEALTH_PROBE_FACT` plus the exact durable source identity. |
| `subject_bindings` | For capacity reports, exact registered pool/worker/profile/session bindings and reported available-slot count; otherwise the code-owned scope bindings. |
| `observed_revision` | Monotonic provider/adapter revision when one exists, otherwise `NULL`. |
| `effective_at_ms` / `expires_at_ms` | Persistent effective time and optional expiry; expiry must be later than effective time. For capacity, these copy the Report's controller `accepted_at_ms` and validated expiry. |
| `payload_digest` | Digest of bounded normalized non-secret health fields. |

`(scope_kind, scope_id, health_sequence)` is unique. The reducer uses the
highest applicable unexpired sequence; expiry becomes effective only through a
persisted timer trigger. Health arrival order, wall-clock timestamps, or Redis
lease presence cannot choose a fallback. `payload_digest` is not identity: a
later authenticated capacity observation receives a new
`health_observation_id` and sequence even when its normalized health payload is
equal to an earlier observation. Replay of the same `source_kind/source_id` is
idempotent; implementations MUST enforce uniqueness of that source identity
within its scope.

Capacity reports arrive only at authenticated
`POST /api/v1/capacity-reports`. For `CAPACITY_REPORT`, `source_id` is
`capacity_report_id`; the referenced ledger plus this Observation's scope
resolves the canonical external tuple
`(pool_manager_id, report_id, scope_kind, scope_id)`, and
`observed_revision` is that principal's strictly increasing
`report_sequence`. The authenticated pool-manager principal may report only
its registered `WORKER_SESSION`, `WORKER_PROFILE`, and `CAPACITY_POOL` scopes.
The controller assigns `health_sequence` and derives `AVAILABLE` exactly when
`available_slots > 0`, otherwise `UNAVAILABLE`; it never trusts a submitted
kind. A session report binds exact worker ID, worker session ID, pool, and
profile, and `AVAILABLE` additionally requires `SESSION_READY`. These reports
require `expires_at_ms`; expiry is applied only by the corresponding Timer
Fact. Capacity-level `UNAVAILABLE` prevents new claims but cannot fence an
already claimed Attempt; only exact session `LOST` evidence may do that.

`LOST` is valid only for `scope_kind = WORKER_SESSION` and
`source_kind = WORKER_LOSS_REPORT`; its subject bindings must equal that
report's worker/session/pool/profile and current claimed Attempt. A Capacity
Report can produce only `AVAILABLE` or `UNAVAILABLE`.
`HEALTH_PROBE_FACT` requires `source_id = health_probe_fact_id`; scope, kind,
revision, effective/expiry time, and exact object bindings copy its closed
matrix. No additional generic adapter/probe source kind exists. Timer expiry
never inserts a Health Observation.

`STORAGE_RESTORATION` is valid only with `kind = RECOVERED`,
`scope_kind = STORAGE` or `SECRET`, and
`source_id = storage_restoration_fact_id`. Its subject bindings name the exact
restored object kind/ID and kind-specific integrity proof. The restoration
Fact and this Observation commit atomically. The lifecycle reducer consumes
the Storage Restoration Fact; the paired Health Observation is the ordered
health projection and MUST NOT cause a second Transition for the same repair.

## Timer Fact

A Timer Fact is an immutable, persisted proof that the controller evaluated
one durable deadline. Redis timers and wall-clock passage are only wake-up
hints; neither is a reducer input.

| Field | Requirement |
| --- | --- |
| `timer_fact_id` | Controller-assigned UUID. It is a reducer trigger ID only for a Wait deadline or Health expiry; Budget, Attempt, and Recovery scopes feed the typed objects below. |
| `run_id` | Owning Run for Wait, Attempt, and Recovery scopes; `NULL` for global Health Observation or Budget Report expiry. |
| `scope_kind` | `WAIT_CONDITION_NOT_BEFORE`, `HEALTH_OBSERVATION_EXPIRY`, `BUDGET_REPORT_EXPIRY`, `ATTEMPT_CLAIM_DEADLINE`, `ATTEMPT_EXECUTION_DEADLINE`, or `RECOVERY_ELIGIBILITY`. |
| `scope_id` | Exact `wait_condition_id`, `health_observation_id`, `budget_report_id`, `attempt_id`, or `recovery_evidence_id` selected by `scope_kind`. |
| `fired_for_ms` | Exact persisted deadline copied from the scoped object's `not_before_ms`, `expires_at_ms`, claim/execution deadline, or `next_eligible_at_ms`. |
| `controller_now_ms` | Controller time proving the scope-specific due predicate below. |
| `source_kind` / `source_id` | `SCHEDULED_SWEEP` or `STARTUP_RECONCILIATION` plus a controller-assigned UUID for that durable scan pass. |
| `affected_run_ids` | For global `HEALTH_OBSERVATION_EXPIRY`, canonically sorted active Runs whose current Wait Condition explicitly binds the observation as health/wake evidence; otherwise empty. Offers and Recovery Evidence are not members. This is a frozen part of the Fact, not a query-time projection. |
| `affected_run_ids_digest` | SHA-256 of the canonical length-prefixed ordered `affected_run_ids`; required even for the empty list. |
| `fact_digest` / `recorded_at_ms` | Digest of every normalized immutable field and informational record time. |

`(scope_kind, scope_id, fired_for_ms)` is unique. The due predicate is
`controller_now_ms >= fired_for_ms` for every scope. Claim and Result
acceptance require controller time strictly before their deadlines, so a
unique Attempt Timer Fact inserted at equality cannot preempt a valid
acceptance. The referenced object's
deadline MUST still equal `fired_for_ms` when the fact is inserted. The
controller persists the Timer Fact before any consequence. Direct
`T(TIMER_FACT, timer_fact_id)` reduction is valid only for
`WAIT_CONDITION_NOT_BEFORE` and `HEALTH_OBSERVATION_EXPIRY`.
`BUDGET_REPORT_EXPIRY` has empty Run membership and no direct Transition; its
existence makes that Report ineligible, and the durable planned-Activity scan
waits for a newer authenticated Report. An Attempt-deadline
Fact is only the immutable source of an Attempt Terminal Fact and is consumed
through `T(ATTEMPT_TERMINAL, attempt_terminal_fact_id)`. A
`RECOVERY_ELIGIBILITY` Fact makes its exact not-yet-consumed Recovery Evidence
eligible; applying the already selected tactic uses
`T(RECOVERY_EVIDENCE, recovery_evidence_id)`, never the Timer identity. On
startup and after Redis loss it scans current Wait Conditions, Health
Observations and Budget Reports with an expiry, Attempts, and Recovery
Evidence, then re-derives every due fact from those durable fields. Replaying
the same fact is idempotent; a stale fact is audit-only.

`ATTEMPT_CLAIM_DEADLINE` has stricter atomicity. After acquiring the single
writer, the controller re-reads the still-current `OFFERED` Attempt and exact
deadline, inserts or reuses the due Timer Fact, resolves and freezes the exact
current provider Secret version when applicable plus the Mode/Registry offer
gate, freezes the highest-applicable unexpired health membership for that exact
resolved version, derives both capacity and replacement-offer dispositions,
inserts the complete Attempt Terminal Fact, expires the Attempt, returns its
Activity to `PLANNED`, and commits its sole
`T(ATTEMPT_TERMINAL, attempt_terminal_fact_id)` Transition. Except for the
panel exception below, a compatible replacement appends zero-counter Recovery
Evidence selecting `RETRY_EXECUTION`; the next Evidence Transition creates the
replacement and performs the atomic `PLANNED -> READY` offer.
`OFFER_ALLOWED + NO_COMPATIBLE_AVAILABLE` instead appends zero-counter
Evidence selecting `WAIT_CAPACITY`; its next Evidence Transition creates the
exact capacity Wait. For a non-panel Activity or a panel with no claimed peer,
`MODE_BLOCKED` or `ISSUANCE_KEY_UNAVAILABLE` appends zero-counter Recovery
Evidence selecting `RETRY_EXECUTION`, creates neither Attempt/outbox nor Wait,
and leaves the Activity `PLANNED` with the Terminal Transition's single
deterministic dispatch continuation pending; the mode/key reconciler may
reduce that Evidence/`INTERNAL` continuation only after current serialized
gates permit offers. These effects commit in one transaction and never change
failure, repair, or diagnosis counters.
For a `REVIEW` or `ADJUDICATE` claim deadline, the capacity-Wait branch is
always panel-scoped: it freezes every currently unfilled Activity/slot in the
same Candidate/panel round (all unfilled reviewer slots or the sole
`ADJUDICATE/default` slot), not merely the expired Attempt's slot. It may enter
that Wait only when none of those slots has a `CLAIMED` Attempt; in the same
transaction it supersedes every remaining peer `OFFERED` Attempt/outbox so all
named slots are truly undispatched. If any peer remains `CLAIMED`, that
condition takes precedence over every claim-deadline disposition: the Run
stays `REVIEWING`/`ADJUDICATING`, preserves that claim, leaves the expired slot
Activity `PLANNED`, appends no Recovery Evidence, creates no Wait/Attempt/outbox
or offer, and retains only the coalesced Terminal-Transition
panel-staffing pointer for the next peer Result/Terminal safe boundary. Its
eventual pointer reduction selects `STAFF_PANEL`, which offers the complete
still-unfilled membership all-or-none under the ordinary independence and
offer gates.

A global Health-expiry Timer Fact is inserted once with `run_id = NULL` and
freezes only those current Wait Conditions that explicitly bind the expiring
observation in `affected_run_ids` in the same transaction. An already planned
or offered Activity remains governed by its own immutable inputs and deadline;
health expiry does not retract it. The controller
independently reduces the same
`T(TIMER_FACT, timer_fact_id)` in each Run. Per-Run Transition uniqueness makes
the fanout replay-safe; no duplicate Timer Fact is created per consumer.
Persistence normalizes the frozen list as Timer Fact membership rows
`(timer_fact_id, ordinal, run_id)`: `ordinal` is zero-based and contiguous;
both `(timer_fact_id, ordinal)` and `(timer_fact_id, run_id)` are unique; and
the ordered rows MUST reproduce `affected_run_ids_digest`. The Timer Fact and
all membership rows commit atomically. Membership is immutable, and startup
replay reads it rather than recomputing which Runs are affected.
Each frozen member consumes the Fact exactly once: a still-current bound Wait
Condition clears to deterministic recovery, while a condition superseded before
its fanout turn receives only the legal same-state audit Transition. Per-Run
Transition uniqueness for this Timer Fact is independent of later specification
generation changes; replay returns the member's original Transition.

## Storage Restoration Operation

A Storage Restoration Operation is the durable authenticated staging/replay
ledger for `POST /api/v1/storage/restorations`. It authorizes restoration
reconciliation but is not itself a reducer input.

| Field | Requirement |
| --- | --- |
| `operation_id` | Caller-assigned lowercase UUID and global idempotency identity. |
| `protocol` | Exact literal `orcest.storage-management/1`. |
| `authenticated_principal_id` / `authorization_context_digest` | Exact transport principal and object-scoped storage authority proof. |
| `object_kind` / `object_id` / `byte_length` | Exact bounded restoration target and submitted length. |
| `expected_digest` / `staged_digest` / `workflow_blob_media_kind` | Kind-specific Candidate/Workflow Blob fields; secret restoration requires all `NULL`. |
| `secret_request_attestation_id` | Opaque Secret Store keyed request-attestation identity required only for `SECRET_VERSION`. |
| `staged_storage_key` | Controller quarantine or protected Secret Store incoming locator; never returned. |
| `payload_digest` | Canonical non-secret request/provenance digest; for a Secret it includes only the opaque keyed-attestation identity, never secret-derived material. |
| `state` | `PENDING`, `RESTORED`, or `REJECTED`. |
| `resulting_fact_id` | Exact Storage Restoration Fact for `RESTORED`; otherwise `NULL`. |
| `rejection_code` | Required only for `REJECTED`: `OBJECT_NO_LONGER_LIVE`, `AUTHORIZATION_REVOKED`, `STAGED_OBJECT_INVALID`, or `INTEGRITY_CONFLICT`. |
| `terminal_http_status` / `terminal_response_json` / `terminal_response_digest` | All `NULL` for `PENDING`; required terminal. `RESTORED` is `200`; rejected mappings are respectively `409`, `403`, `422`, and `409`. Digest covers status and exact body. |
| `accepted_at_ms` / `terminal_at_ms` | Accepted time always present; terminal time present exactly when terminal. |

The initial accepted response is deterministic and unstored:

```json
{
  "protocol": "orcest.storage-restoration-accepted/1",
  "operation_id": "lowercase-uuid",
  "state": "PENDING",
  "object_kind": "WORKFLOW_BLOB",
  "object_id": "sha256:64-lowercase-hex"
}
```

It uses HTTP `202`. Terminal bodies use protocol
`orcest.storage-restoration-result/1` and contain exactly operation ID, terminal
state, object kind/ID, and either `storage_restoration_fact_id` for `RESTORED`
or `rejection_code` for `REJECTED`; fields from the other variant are omitted.

The same authenticated operation ID and `payload_digest` returns the current
Operation. While `PENDING`, the controller derives the exact `202` projection
from immutable fields. Once terminal it returns the stored status/body/digest.
Conflicting request reuse fails without altering the original. A response-loss
race therefore observes either the same pending projection or the terminal
response, never another staging object or restoration Fact. Terminal commit
atomically stores the response and either links the exact Fact or records the
closed rejection. Rejected staging leaves the live retry root and follows the
quarantine/grace rules in persistence; Operation and auth/integrity metadata
remain retained for replay/audit.

## Storage Restoration Fact

A Storage Restoration Fact is immutable proof that an exact live object was
restored and revalidated. It does not grant workflow or publication authority.

Before restoration, the integrity detector follows the closed Health Probe
Request/outbox protocol under the shared storage mutation lock. Probe
completion atomically inserts its reciprocal Fact plus one exact `STORAGE` or
`SECRET` `UNAVAILABLE` Health Observation; it does not alter a Run or create a
Wait. For each nonterminal referencing Run that is not at an unrelated Human
Boundary, the separate `T(HEALTH_OBSERVATION,health_observation_id)` fences
consumers, preserves the exact recovery origin, enters `RECOVERING`, and
appends mapped `STORAGE` or `CREDENTIAL` Recovery Evidence. Only the following
`T(RECOVERY_EVIDENCE,recovery_evidence_id)` selects `WAIT_EXTERNAL` and creates
the bound `STORAGE_RECOVERY` or `SECRET_RECOVERY` Wait. An existing Wait is
superseded while its resume origin is preserved; cancellation-in-progress
preserves `PR_MONITORING` as origin and dispatches no semantic work. Runs at an
unrelated Human Boundary consume the accepted Observation as same-state audit
input and revalidate object health when that boundary clears.

| Field | Requirement |
| --- | --- |
| `storage_restoration_fact_id` | Controller-assigned UUID and reducer trigger ID. |
| `affected_run_ids` | Canonically sorted distinct active Runs whose current exact-object `STORAGE_RECOVERY`/`SECRET_RECOVERY` Wait Condition or `INTEGRITY_FAILURE` Human Boundary matches this object when restoration commits; may be empty. |
| `object_kind` | `CANDIDATE_ARTIFACT`, `SECRET_VERSION`, or `WORKFLOW_BLOB`. |
| `object_id` | Candidate bundle digest, canonical Secret Version key, or Workflow Blob digest selected by `object_kind`. |
| `expected_digest` / `restored_digest` | Required matching SHA-256 values for `CANDIDATE_ARTIFACT` and `WORKFLOW_BLOB`; both `NULL` for `SECRET_VERSION`. |
| `workflow_blob_media_kind` / `workflow_blob_byte_length` | Exact media kind and unsigned normalized byte length for `WORKFLOW_BLOB`; otherwise both `NULL`. The restored digest is recomputed with the Workflow Blob domain-separated formula. |
| `secret_integrity_attestation_id` | Required only for `SECRET_VERSION`; opaque Secret Store verification record proving its controller-only keyed authenticator matched. |
| `health_observation_id` | Exact `STORAGE` or `SECRET` `RECOVERED` Health Observation inserted atomically from this Fact. |
| `source_kind` / `source_id` | `BACKUP_RESTORE` plus immutable backup-manifest/restore identity, or `AUTHENTICATED_STORAGE_OPERATION` plus immutable management-operation identity. |
| `backup_manifest_digest` | Required for `BACKUP_RESTORE`; otherwise `NULL`. |
| `authenticated_principal_id` / `authorization_context_digest` | Required for `AUTHENTICATED_STORAGE_OPERATION`; otherwise `NULL`. |
| `verification_digest` / `recorded_at_ms` | Digest of restore source, object identity, the kind-specific non-secret verification record, and owner-table cross-check; informational record time. It never incorporates raw secret bytes, an unkeyed secret digest, or a Secret Store keyed tag. |

`(object_kind, object_id, source_kind, source_id)` is unique. The Fact is
object-scoped, not Run-owned. Its `affected_run_ids` snapshot and the restored
object, its exact `RECOVERED` Health Observation, and all fanout work commit
atomically. The controller independently reduces
`T(STORAGE_RESTORATION, storage_restoration_fact_id)` for each listed Run in
stable `run_id` order; per-Run Transition uniqueness is independent of later
specification generations and makes fanout replay-safe.
A later Run sees the already-restored object and needs no historical
transition. Candidate restore
must cross-check the live Candidate/artifact row, Secret restore the Secret
Store's exact version metadata, and Workflow Blob restore the live Snapshot
reference before this fact can exist. The persistence page owns the physical
tables, backup-manifest validation, and atomic restore protocol; see
[Persistence and recovery](persistence-and-recovery.md). A valid fact may wake
a storage wait or satisfy the exact `INTEGRITY_RESTORED` Human Resolution by
`T(STORAGE_RESTORATION, storage_restoration_fact_id)`.
If a frozen member's matching Wait/Boundary is superseded before its fanout
turn, the Fact produces only the legal same-state audit Transition.

The controller's storage reconciler is the only component that inserts this
Fact. It may act automatically from a server-configured, integrity-verified
backup manifest, or after the narrow authenticated workflow-management endpoint
`POST /api/v1/storage/restorations` accepts protocol
`orcest.storage-management/1` as an authenticated bounded multipart request:
canonical metadata contains caller UUID `operation_id`, `object_kind`,
`object_id`, conditional `expected_digest`, and `byte_length`, while the binary
part is the exact replacement object. Candidate and Workflow Blob bytes are
staged in controller-owned quarantine. Secret bytes are streamed directly into
a protected temporary Secret Store version and never enter general controller
quarantine. The endpoint verifies length and the kind-specific integrity proof
before acceptance, stores the
authenticated principal and authorization digest, and is idempotent by
`operation_id`. For Candidate/Workflow Blob, caller `expected_digest` is
required. For `SECRET_VERSION`, it is forbidden: the controller resolves the
expected verifier from Secret Store metadata and verifies the staged bytes
inside the Secret Store. Secret-operation idempotency compares canonical
non-secret metadata plus the Secret Store's keyed request authenticator; no
SQLite `payload_digest`, `staged_digest`, log field, or response may contain an
unkeyed hash or keyed tag derived from the raw secret. It does not accept
`backup_manifest_digest`; an
`AUTHENTICATED_STORAGE_OPERATION` Fact therefore has that field `NULL` and
uses `operation_id` as `source_id`. Staged bytes never enter SQLite, Redis,
logs, or ordinary workflow APIs. Repository configuration, workers, and Run
comments cannot invoke this endpoint. Acceptance authorizes an attempt to
restore but does not create the Fact. The reconciler creates the Fact only
after atomic installation, kind-specific integrity verification, and the
owner-table cross-check succeed. The autonomous `BACKUP_RESTORE` path remains separate and
requires its verified `backup_manifest_digest`.

Restoration is object-kind-specific. `CANDIDATE_ARTIFACT` uses durable
artifact-file installation and fsync before the fact transaction.
`SECRET_VERSION` uses the Secret Store's atomic replacement protocol before
the controller verifies its metadata reference. Secret exact-byte integrity is
proved only by a domain-separated keyed authenticator (HMAC or equivalent)
whose key and tag remain in controller-only Secret Store metadata. SQLite and
the API retain only `secret_integrity_attestation_id`; no unkeyed hash of
secret bytes, keyed tag, or caller-supplied expected secret digest is allowed.
`WORKFLOW_BLOB` is not a filesystem artifact: after canonical
media-kind/length/domain-separated-digest validation, the single SQLite writer
repairs or inserts the exact `workflow_blobs` row and
inserts the Storage Restoration Fact plus its affected-Run fanout work in the
same `synchronous=FULL` transaction. A quarantined byte source is deleted only
after that transaction commits; no workflow blob repair may bypass SQLite or
be represented as a file install.

A referenced Workflow Blob row that is wholly absent is a startup-reachability
failure, not the ordinary Run-scoped runtime path above: the missing parent row
prevents safe owner/FK traversal. Before any controller API, reducer, outbox,
worker dispatch, or adapter loop is enabled, the storage reconciler holds an
exclusive global startup barrier, derives the exact expected digest and media
kind only from the frozen child references, obtains the exact normalized bytes
from a verified backup or authenticated staged restoration source, recomputes
the domain-separated identity, and inserts the row through the single SQLite
writer. It then runs the full foreign-key and database integrity checks before
enabling service. Failure leaves the controller globally disabled and follows
the storage recovery procedure; it never fetches mutable repository content.
The ordinary scoped Storage Restoration Fact/fanout path applies only when the
referenced row exists but its bytes are corrupt, unreadable, or fail canonical
validation. Detailed exclusive-barrier and transaction ordering belong to
[persistence and recovery](persistence-and-recovery.md).

## Management Command

A Management Command is one immutable authenticated Run-scoped input accepted
through the controller management API. It is not a repository instruction.

| Field | Requirement |
| --- | --- |
| `command_id` | Caller-assigned lowercase UUIDv4 and global idempotency identity. |
| `protocol` | Exactly `orcest.management/1` in v1. |
| `run_id` | Exact target Run. |
| `kind` | `CANCEL` or `RESOLVE_HUMAN_BOUNDARY`. |
| `expected_last_transition_sequence` | Compare-and-swap fence supplied by the caller. |
| `human_boundary_id` | Required only for `RESOLVE_HUMAN_BOUNDARY`; exact current boundary. |
| `resolution_kind` / `resolution` | Required only for `RESOLVE_HUMAN_BOUNDARY`; closed typed secret-free resolution defined below. |
| `authenticated_principal_id` / `authorization_context_digest` | Server-authenticated identity and immutable digest of the RBAC decision/scope. |
| `payload_digest` | Digest of protocol, target, fence, kind, and normalized payload. |
| `result_transition_sequence` / `human_resolution_id` | Transition produced and Resolution produced when applicable. |
| `response_http_status` / `response_json` / `response_digest` | Exact accepted response; status is `200`, JSON is the closed body below, and digest covers every field except the transport-only `replayed` projection. |
| `accepted_at_ms` | Informational controller time. |

`command_id` is unique. An authenticated replay with the same digest returns
the stored outcome; reuse with a different digest is an idempotency conflict.
Acceptance, the resulting Transition, and any Human Resolution commit in one
writer transaction. The controller rejects a stale Run fence or boundary,
unauthorized principal, unsupported protocol/kind, secret-bearing payload, or
invalid resolution without changing lifecycle state. Rejected requests are
security audit events but not accepted Management Commands. RBAC principals,
credential issuance, and role administration belong to the companion
server-management specification; repository configuration cannot grant this
authority.

The exact accepted body is:

```json
{
  "protocol": "orcest.management-result/1",
  "command_id": "lowercase-uuid",
  "run_id": "lowercase-uuid",
  "kind": "CANCEL",
  "outcome": "ACCEPTED",
  "result_transition_sequence": 42,
  "human_resolution_id": null,
  "replayed": false
}
```

`kind` echoes `CANCEL` or `RESOLVE_HUMAN_BOUNDARY`; the latter requires the
exact non-null accepted `human_resolution_id`, while `CANCEL` requires `null`.
The stored `response_json` uses `replayed = false`. An identical replay returns
the same status/body with only `replayed = true`; `response_digest`
intentionally omits exactly that one transport projection and no other field.

## Human Boundary

A Human Boundary is the controller-issued immutable exceptional decision
packet for one exact Run state.

| Field | Requirement |
| --- | --- |
| `human_boundary_id` | Controller-assigned UUID. |
| `run_id` | Owning Run. |
| `reason` | `MISSING_AUTHORITY`, `REQUIRED_SECRET_OR_PERMISSION`, `IRREVERSIBLE_DECISION`, `SPECIFICATION_CONFLICT`, `SECURITY_POLICY_BOUNDARY`, `INTEGRITY_FAILURE`, `UNSATISFIABLE_REQUIREMENTS`, or `PUBLICATION_OWNERSHIP_CONFLICT`. |
| `specification_generation` / `candidate_id` / `policy_hash` / `forge_observation_id` | Exact boundary bindings; nullable only when inapplicable. |
| `publication_id` / `publication_effect_generation` | Exact publication fence when applicable, otherwise `NULL`. |
| `ownership_project_id` / `ownership_deterministic_ref` / `ownership_change_request_external_id` / `ownership_run_marker` | Exact registered Project, adapter-normalized ref, observed Change Request ID, and syntactically valid Orcest v1 marker for `PUBLICATION_OWNERSHIP_CONFLICT`; all four are non-`NULL` exactly for that reason and otherwise `NULL`. |
| `resume_state` | State to revalidate after a matching resolution. |
| `minimum_request` | Bounded normalized description of the smallest unavailable decision, information, secret, permission, authority, or restoration. |
| `evidence_refs` / `attempted_strategy_digests` | Canonically sorted bounded references proving the boundary and exhausted applicable autonomy. |
| `choices` | Ordered bounded objects `{choice_id, resolution_kind, consequence}` permitted for this boundary. |
| `required_resolution_kinds` | Canonically sorted non-empty subset of the resolution enum accepted for this reason. |
| `created_from_kind` / `created_from_id` | `RECOVERY_EVIDENCE` plus the exact exhausted `recovery_evidence_id`, or `RECONCILIATION_FACT` plus the exact `OWNERSHIP_CONFLICT` `reconciliation_fact_id`. |
| `packet_digest` | Digest of the complete normalized packet. |
| `created_transition_sequence` / `created_at_ms` | Creating Transition and informational time. |

The packet is at most 65,536 normalized bytes; individual prose fields are at
most 2,048 Unicode scalar values, and evidence, strategy, and choice arrays
contain at most 128 entries. A Human Boundary is current exactly when
`Run.human_boundary_id` names it and the Run is `NEEDS_HUMAN`; at most one can
be current. Clearing or superseding the Run reference never mutates the packet.
Every worker, deadline, health, forge, policy, storage, or secret problem must
first traverse its autonomous recovery path and can create a boundary only
through the resulting Recovery Evidence. The sole direct exception is the
positive ownership-conflict Reconciliation Fact defined above. That Fact and
packet MUST bind the exact Project, deterministic ref, observed Change Request,
Orcest v1 Run marker, Publication, and immutable Publication Effect generation;
a packet with an absent or merely inferred ownership binding is invalid.

For this reason, `choices` contains exactly one v1 choice whose consequence is
continued Orcest ownership and whose resolution kind is
`PUBLICATION_OWNERSHIP_RESOLVED`; a legacy-engine choice or marker-transfer
choice is invalid.

## Human Resolution

A Human Resolution is one immutable, authenticated resolution accepted for an
exact Human Boundary.

| Field | Requirement |
| --- | --- |
| `human_resolution_id` | Controller-assigned UUID. |
| `human_boundary_id` / `run_id` | Exact current boundary and owning Run. |
| `idempotency_key` | Exact source-derived stable text identity, unique for the boundary and equal to `source_id`: Management Command `command_id`, Forge Observation `forge_observation_id`, canonical Secret Version key `<lowercase UUID>:<base-10 version>`, or Storage Restoration Fact `storage_restoration_fact_id`. |
| `source_kind` / `source_id` / `authenticated_principal_id` | `MANAGEMENT_COMMAND`, `FORGE_OBSERVATION`, `SECRET_VERSION`, or `STORAGE_RESTORATION`, plus the exact persisted resolution-source identity and authenticated/verified authority identity. The source is also the Transition trigger except for `SPECIFICATION_AMENDED`, whose source is the authorizing Forge Observation while its canonical Transition is `SPEC_SUPERSEDE` with the captured Snapshot ID. `SECRET_VERSION` uses the canonical composite key; `STORAGE_RESTORATION` uses `storage_restoration_fact_id`. |
| `resolution_kind` | `AUTHORITY_GRANTED`, `SECRET_OR_PERMISSION_PROVIDED`, `IRREVERSIBLE_ACTION_AUTHORIZED`, `SPECIFICATION_AMENDED`, `SECURITY_ACTION_AUTHORIZED`, `INTEGRITY_RESTORED`, `ENVIRONMENT_CAPABILITY_PROVIDED`, or `PUBLICATION_OWNERSHIP_RESOLVED`. |
| `resolution` | Closed, reason-specific bounded structured object; secret values are forbidden and only a Secret Reference/version may appear. |
| `specification_generation` / `candidate_id` / `policy_hash` / `forge_observation_id` | Exact copied boundary bindings. |
| `publication_id` / `publication_effect_generation` | Exact copied publication fence when applicable. |
| `ownership_project_id` / `ownership_deterministic_ref` / `ownership_change_request_external_id` / `ownership_run_marker` | Exact copied ownership bindings for `PUBLICATION_OWNERSHIP_RESOLVED`; all four are otherwise `NULL`. |
| `resolution_digest` / `accepted_at_ms` | Digest of the normalized resolution and informational acceptance time. |

At most one Human Resolution may be accepted for a Human Boundary. The same
idempotency key and digest returns that Resolution; any different reuse is an
integrity conflict. Acceptance requires the boundary still be current, the
resolution kind be explicitly allowed by its packet and reason, every binding
match, `idempotency_key = source_id` under the closed source-kind encoding, and
the authenticated principal have server-side authority for that exact action.
A repository file, prompt, issue comment, or generic `continue` text cannot
define or satisfy a Human Resolution.

`PUBLICATION_OWNERSHIP_RESOLVED` has one closed v1 resolution object:

```json
{
  "selected_engine": "ORCEST_V1",
  "project_id": "exact-registered-project-id",
  "deterministic_ref": "exact-adapter-normalized-ref",
  "change_request_external_id": "exact-observed-change-request-id",
  "run_marker": "exact-syntactically-valid-orcest-v1-marker",
  "publication_id": "lowercase-uuid",
  "effect_generation": 1
}
```

It is accepted only from an authenticated `MANAGEMENT_COMMAND`, and every
value MUST equal the current Boundary's copied ownership bindings, its Run and
registered Project, the immutable Publication Effect, and the ordered Forge
Observations. `selected_engine` has no other v1 value. Acceptance asserts only
that this exact object remains assigned to the Orcest v1 engine; it grants no
blind overwrite and resumes through ownership `RECONCILE` before another
publication effect. Selecting a legacy engine, transferring or removing the
marker as an ownership handoff, or handing the Change Request to a legacy loop is not a v1 Human
Resolution and requires a future separately specified migration protocol.
This does not prohibit the code-owned post-merge Reservation from detaching
only its marker from a non-selected relied-on duplicate under exact CAS.

For `INTEGRITY_RESTORED`, a `BACKUP_RESTORE` Fact uses the authenticated
controller storage-reconciler service principal as the Human Resolution
authority and retains the verified manifest as source evidence; the Fact's
operator-principal fields remain `NULL`. An
`AUTHENTICATED_STORAGE_OPERATION` Fact instead copies the authenticated
operator principal and authorization context into the Resolution. Both paths
must satisfy the same exact boundary, object, and kind-specific integrity
bindings; a Secret Version binds the opaque Secret Store attestation rather
than an expected or restored digest.

For an automatic `SECRET_OR_PERMISSION_PROVIDED` Resolution sourced from
`SECRET_VERSION`, `authenticated_principal_id` is the registered controller
Secret-Store verifier/reconciler service principal—not the worker, original
provisioning operator, or a synthetic user. The normalized `resolution` and
evidence contain the exact verified-current `SecretVersionKey`, its immutable
`creation_receipt_id`, the Receipt's source identity, and the opaque current
Secret Store integrity-attestation ID. Acceptance atomically rechecks that the
version remains current and that the creation Receipt/Version/attestation chain
matches the Boundary's requested Secret ID and minimum version. The
`resolution_digest` covers all those non-secret fields. Original provisioning
authority remains attributable through the Credential Rotation Receipt; the
service principal attests only current verification and fanout authority.

`SPECIFICATION_AMENDED` has one stricter source rule: it is accepted only from
an ordered `WORK_ITEM_SNAPSHOT` Forge Observation whose verified
`actor_principal_id` and `actor_authorization_digest` prove edit authority and
whose normalized specification actually changes the conflicting or
unsatisfiable input. Its `T(FORGE_OBSERVATION,forge_observation_id)` captures
the pending Snapshot and remains `NEEDS_HUMAN`; it never installs it. The
separately replayable `T(SPEC_SUPERSEDE,snapshot_id)` transaction installs the
new generation, inserts the Human Resolution with
`idempotency_key = forge_observation_id`, clears the exact boundary, and plans
`REPLAN`. A Management Command cannot synthesize `SPECIFICATION_AMENDED`; an
unauthorized or non-changing edit is only audit input.

## Transition

A Transition is the immutable audit record of one reducer decision.

| Field | Requirement |
| --- | --- |
| `run_id` / `transition_sequence` | Per-Run append-only primary identity. |
| `from_state` / `to_state` | Lifecycle states. |
| `trigger_kind` / `trigger_id` | Exact persisted input that caused evaluation. |
| `anchored_base_observation_id` | Required exactly for `ADMIT`; exact trusted `BASE_HEAD` Observation composed into capture-sequence 1 and ordered at this Transition sequence. `NULL` for every other trigger. |
| `specification_generation` | Generation evaluated. |
| `candidate_id` | Candidate evaluated, if applicable. |
| `reason_code` | Code-owned reason. |
| `planned_activity_ids` | Ordered list inserted in the same transaction. |
| `transition_digest` | Digest of normalized reducer inputs and outputs. |
| `reducer_version` | Exact code-owned reducer version that produced the Transition. |
| `committed_at_ms` | Informational time. |

`trigger_kind` is closed in v1. The exact mapping is:

| `trigger_kind` | Persisted input and exact `trigger_id` |
| --- | --- |
| `ADMIT` | Eligible admission `WORK_ITEM_SNAPSHOT` Forge Observation as `trigger_id`, plus the exact capture-sequence-1 `BASE_HEAD` in `anchored_base_observation_id`. |
| `INTERNAL` | Exact prior Transition whose deterministic continuation is reduced; unsigned decimal `transition_sequence`. |
| `ATTEMPT_RESULT` | Accepted Attempt Result; its primary-key `attempt_id`. |
| `ATTEMPT_TERMINAL` | Attempt Terminal Fact; `attempt_terminal_fact_id`. |
| `CONTROLLER_OPERATION` | Controller Operation Fact; `controller_operation_fact_id`. |
| `FORGE_REQUEST_FAILURE` | Run-bound Forge Request Failure Fact; `forge_request_failure_fact_id`. Project discovery and terminal-cleanup retry Facts have no Run Transition. |
| `FORGE_OBSERVATION` | Forge Observation outside admission; `forge_observation_id`. |
| `HEALTH_OBSERVATION` | Health Observation; `health_observation_id`. |
| `BUDGET_REPORT` | Authenticated Budget Report fanout member; `budget_report_id`. |
| `MANAGEMENT_COMMAND` | Accepted Management Command, including `CANCEL`; `command_id`. |
| `POLICY_UPDATE` | Policy Update; `policy_update_id`. |
| `PUBLICATION_CHECKPOINT` | Exact newly committed `AMBIGUOUS` Publication Effect Checkpoint requiring read reconciliation; `publication_effect_checkpoint_id`. `REQUEST_READY` is prior-transition/outbox output, and observation-backed checkpoints are effects of their sole `FORGE_OBSERVATION` Transition. Restart alone is not a trigger. |
| `RECONCILIATION_FACT` | Reconciliation Fact; `reconciliation_fact_id`. |
| `RECOVERY_EVIDENCE` | Recovery Evidence whose selected tactic is being applied; `recovery_evidence_id`. |
| `SECRET_VERSION` | Secret Store version; canonical Secret Version key defined below. |
| `SPEC_SUPERSEDE` | Pending Snapshot installed at the safe boundary; `snapshot_id`. |
| `STORAGE_RESTORATION` | Storage Restoration Fact; `storage_restoration_fact_id`. |
| `TIMER_FACT` | Timer Fact for `WAIT_CONDITION_NOT_BEFORE` or `HEALTH_OBSERVATION_EXPIRY` only; `timer_fact_id`. Attempt-deadline and Recovery-eligibility Timer Facts are never direct Run triggers. |

No other `trigger_kind` is valid. `trigger_kind/trigger_id` names the persisted
causal input, not an object first created as an effect of the Transition.
Wait/boundary creation uses the exact allowed kind and ID of the object named
by its `created_from_kind/created_from_id`. Created Candidate, Receipt, Wait
Condition, Human Boundary, Resolution, Publication Effect, outbox, digest, or
mutable Activity state is an output and cannot substitute for that causal
identity.

Transitions are never updated or deleted while their Run is retained. Every
first accepted durable causal input in the closed trigger mapping is reduced
to exactly one Transition for each affected Run, including a stale,
superseded, terminal, or otherwise no-op input; identical replay returns that
Transition. Rejected wire input, an idempotent replay that creates no new
causal object, controller restart, and mutable/outbox state are not triggers.
The Run row is the efficient current-state projection; the ordered Transition
log is the audit trail. Re-evaluating the same trigger after a crash MUST find the
existing transition by `(run_id, trigger_kind, trigger_id)` and return it
rather than plan new work. This identity is independent of
`specification_generation`; that field records the generation evaluated by the
first reduction and is never part of replay authorization. Every durable
causal input is therefore consumed exactly once by a Run even if an internal
continuation or Snapshot supersession later advances its generation. For
`ADMIT` and `FORGE_OBSERVATION`, the additional cross-kind identity is
`(run_id, forge_observation_id)` across the ADMIT trigger, ADMIT anchored base,
and every FORGE_OBSERVATION trigger. The two distinct ADMIT observations may
therefore be consumed only by that one admission Transition and neither can
authorize a later observation reduction.
`from_state = NONE` is valid only for transition sequence `1` with trigger
`ADMIT`; every later Transition has a real lifecycle `from_state`, and every
Transition has a real `to_state`. A same-state Transition is still a consumed
input and durable effect boundary, never “no Transition.”

## Outbox record

An Outbox record durably requests delivery or a controller side effect.

| Field | Requirement |
| --- | --- |
| `outbox_id` | Controller-assigned UUID. |
| `source_kind` / `source_id` | `ACTIVITY` plus `activity_id`, `HEALTH_PROBE_REQUEST` plus `health_probe_request_id`, `FORGE_OBSERVATION_REQUEST` plus `forge_observation_request_id`, `SECRET_PROVISION_OPERATION` plus `secret_provision_operation_id`, or `TERMINAL_DUPLICATE_CLEANUP_ACTION` plus `terminal_duplicate_cleanup_action_id`; closed immutable parent identity. |
| `activity_id` | Required exactly for `source_kind = ACTIVITY`; otherwise `NULL`. |
| `attempt_id` | Attempt offered to a worker, or `NULL` for a controller operation. |
| `attempt_generation` | Attempt generation for worker delivery, or `NULL` for a controller operation. |
| `publication_id` / `effect_generation` | Exact immutable Publication Effect fence for `PUBLISH` or another controller Publication side effect, including `CLOSE_REDUNDANT_PUBLICATION`, `REPAIR_RUN_MARKER`, and a terminal duplicate cleanup mutation; otherwise both `NULL`. The fields are nullable together. |
| `destination` | Code-owned versioned stream or controller subsystem. |
| `payload_digest` | Digest of the non-secret normalized envelope, including the Publication Effect binding when present. |
| `state` | `PENDING`, `DELIVERED`, or `SUPERSEDED`. |
| `delivery_count` / `next_delivery_ms` | Reconstructible delivery bookkeeping. |

For a non-Activity source, Attempt fields are `NULL`. Publication Effect fields
are also `NULL` except for `TERMINAL_DUPLICATE_CLEANUP_ACTION`, where both are
required and equal the Reservation's immutable terminal Effect fence.
The Activity, `OFFERED` Attempt when applicable, and Outbox record MUST commit
in one transaction. A Health Probe Request, Forge Observation Request, or
pending Secret Provision Operation commits with its exact Outbox source binding
before I/O. Planning an
authoritative `PUBLISH` side effect commits its controller Activity, new
immutable Publication Effect, and effect-bound Outbox record in that same
transaction. Planning `CLOSE_REDUNDANT_PUBLICATION` instead references the
already-current immutable Effect as a fence and commits only its repair
Activity/outbox; it never creates or increments an Effect.
`REPAIR_RUN_MARKER` uses that same existing-Effect fencing rule and likewise
creates no Effect generation. A terminal cleanup mutation uses its Reservation
and selected Effect as authority, commits its Action/outbox before I/O, and
creates no Run Activity or new Effect generation. Marking an outbox
record `DELIVERED` does not prove claim,
execution, or success.

## Projection Outbox record

A Projection Outbox record requests one idempotent forge-visible status
projection. It never authorizes or proves a lifecycle transition.

| Field | Requirement |
| --- | --- |
| `projection_outbox_id` | Controller-assigned UUID. |
| `run_id` / `source_transition_sequence` | Exact Run Transition whose committed state is being projected. |
| `kind` | Sole v1 value `RUN_STATUS`; label/comment/check details are fields of one desired complete projection, not additional kinds. |
| `target_kind` / `target_external_id` | `WORK_ITEM` or `CHANGE_REQUEST` plus its stable forge external ID. |
| `publication_id` / `publication_effect_generation` | Exact immutable Effect fence when the target/result is effect-bound; otherwise both `NULL`. |
| `payload_json` / `payload_digest` | Bounded canonical complete desired projection and its digest; a diagnostic may name a non-secret Secret Reference, but never raw bytes, a bearer, an authenticated URL, or a secret-derived unkeyed digest. |
| `idempotency_key` | Stable code-owned identity derived from Run, source Transition, kind, target, and nullable Effect binding; unique. |
| `state` | `PENDING`, `DELIVERED`, or `SUPERSEDED`. |
| `delivery_count` / `next_delivery_ms` | Reconstructible retry bookkeeping. |

The source Transition and every Projection Outbox row it creates commit in one
writer transaction. A newer `RUN_STATUS` projection for the same target may
supersede an undelivered older row, but neither delivery nor supersession
changes Run state. Restart rebuilds delivery from these rows, never from a
label or Redis.

## Secret Reference

A Secret Reference is the only domain value that identifies and authorizes
resolution of one exact secret version. Non-secret logical `secret_id`,
positive version/provenance numbers, opaque Secret Store receipt or
attestation IDs, and their bounded metadata may appear where this specification
explicitly defines them, but none grants access to bytes without the exact
versioned reference and endpoint capability. Raw secret bytes, reusable
bearers, and secret-derived unkeyed digests are forbidden in domain rows and
ordinary messages.

```text
SecretReference = (secret_id UUID, version unsigned integer)
SecretVersionKey = lowercase_uuid(secret_id) + ":" + base10(version)
```

The decimal version has no leading zero and is greater than zero. This
canonical composite key is the `SECRET_VERSION` Transition `trigger_id` and
the Human Resolution `source_id`; v1 has no separate `secret_version_id`.

Every Secret Version has these durable non-secret fields in SQLite:

| Field | Requirement |
| --- | --- |
| `secret_id` / `version` | Canonical Secret Version composite primary identity. |
| `creation_receipt_id` | Exact Credential Rotation Receipt that originally created this immutable version; required and never changed by restoration. |
| `storage_path` | Normalized controller-only Secret Store path; never the value. |
| `affected_run_ids_digest` | Digest of the frozen active-Run membership described below. |

### Secret Provision Operation

A Secret Provision Operation is the narrow authenticated management input that
creates or adopts one Secret Version. It is not a general Secret Store or RBAC
administration API.

| Field | Requirement |
| --- | --- |
| `secret_provision_operation_id` | Caller-assigned lowercase UUID; request idempotency identity and `MANAGEMENT_PROVISION` source ID. |
| `protocol_version` | Exact literal `orcest.secret-provision/1`. |
| `mode` | `PROVISION` for newly supplied bytes or `ADOPT_EXISTING` for an explicitly operator-authorized existing Secret Store object. |
| `secret_id` / `expected_prior_version` | Exact logical Secret and compare-and-swap prior version; prior is `NULL` only for initial creation. |
| `target_version` | Positive version frozen under the per-Secret lock when `PENDING` is accepted: exactly `1` for initial creation, otherwise `expected_prior_version + 1`. It is never reallocated and is covered by every request/proof/checkpoint digest. |
| `purpose` | Code-owned non-secret purpose enum appropriate to the requested credential. |
| `owner_scope_kind` / `owner_scope_id` | `PROJECT` plus exact `project_id`, `FORGE_INSTALLATION` plus exact canonical `installation_or_account_ref`, or `CONTROLLER` plus a code-owned controller scope ID. |
| `provider_account_ref` | Exact non-secret provider/installation/account registration when the purpose requires one; otherwise `NULL`. |
| `authenticated_principal_id` / `authorization_context_digest` | Exact transport principal and immutable server RBAC decision for this secret, owner, purpose, account, mode, and prior-version CAS. |
| `secret_store_staging_receipt_id` / `secret_integrity_attestation_id` | Opaque Secret Store proof, required when the Operation is accepted, that protected staging or explicit existing-object adoption produced verified immutable bytes. Neither value is a secret-derived digest or reusable access handle. |
| `state` | Monotonic projection `PENDING`, `COMPLETED`, or `REJECTED`; acceptance starts at `PENDING` and exactly one terminal checkpoint advances it. |
| `credential_rotation_receipt_id` / `new_version` | Both required only for `COMPLETED`, where `new_version = target_version`; both `NULL` for `PENDING` and `REJECTED`. |
| `rejection_code` | Required only for `REJECTED` and equal to the terminal checkpoint code; otherwise `NULL`. |
| `terminal_http_status` / `terminal_response_json` / `terminal_response_digest` | Required together for either terminal state and `NULL` while `PENDING`. Status is `200` for `COMPLETED`; `REJECTED` maps `CAS_LOST` or `INTEGRITY_CONFLICT` to `409`, `AUTHORITY_REVOKED` to `403`, and `STAGED_OBJECT_INVALID` to `422`. The digest covers status plus canonical bounded non-secret body. |
| `last_checkpoint_id` | Latest Secret Provision Checkpoint, or `NULL` before its first install attempt; mutable projection only. |
| `request_digest` / `created_at_ms` | Digest of all canonical non-secret request/provenance fields and informational acceptance time. |

The operation endpoint is
`POST /api/v1/secrets/provisioning-operations`. Transport authentication,
server-owned `SECRET_PROVISION` or `SECRET_ADOPT_EXISTING` authority, and exact
owner/account scope are mandatory. Protected secret bytes for `PROVISION` are
streamed directly into the Secret Store over the authenticated request path
and are excluded from request normalization, SQLite, logs, traces, Redis, and
the response. `ADOPT_EXISTING` accepts only an adapter-internal one-use
protected locator; the operator explicitly authorizes adoption and the Secret
Store returns the opaque staging receipt and keyed integrity attestation. The
locator is consumed and never stored in SQLite. Migration of a pre-existing
secret MUST use `ADOPT_EXISTING`; it cannot fabricate historical worker or
rotation provenance.

`FORGE_INSTALLATION` is the sole pre-Project owner in v1. Its closed allowed
purposes are `FORGE_API`, `SOURCE_READ`, and `PUBLICATION`, each a distinct
logical Secret, and each requires
`provider_account_ref = owner_scope_id = installation_or_account_ref`, and its
authorization/request/Receipt digests cover that exact non-secret identity.
This lets Stage 0 provision or adopt the installation credential before a
Project exists. Project registration may resolve it only after verifying the
current Secret Version, purpose, installation/account identity, creation
Receipt, and forge authorization. It installs the `FORGE_API` Secret/provenance
on Forge Instance, and the distinct `SOURCE_READ` and `PUBLICATION` logical
Secret IDs/provenance versions on Project. The latter two may share neither a
Secret ID nor purpose. Registration cannot relabel a Project- or
Controller-owned Secret as installation-owned or use one purpose in another
field.

The sole Stage-0 controller-owned purpose is
`CAPABILITY_SIGNING_PRIVATE_KEY`, with
`owner_scope_kind = CONTROLLER`, `owner_scope_id = ORCEST_V1`, and
`provider_account_ref = NULL`. It is permitted in bootstrap `MAINTENANCE` only
through the authenticated Secret Provision endpoint and produces the same real
Operation, Checkpoint, Credential Rotation Receipt, Version, keyed integrity
attestation, and audit retention as every other provision/adoption. Before the
Capability Registry has a selected `ACTIVE` key, every other Secret purpose is
rejected; after selection, the normal owner/purpose authorization matrix
applies. Raw or synthetic signing material can never be registered directly.

The controller records the canonical non-secret request digest before
acknowledgment. An identical authenticated replay returns the current projection
of the same Operation; reuse of the UUID with a different non-secret request,
principal, or staged/adopted object is a conflict.

Acceptance freezes `target_version` while holding the per-Secret lock, then
commits the `PENDING` Operation and retry outbox only after the
Secret Store has durably returned the opaque staging receipt and keyed
integrity attestation. Retrying uses the Operation UUID and staging receipt;
it never asks the client to resend bytes after acceptance and never creates a
second Secret Store object. Secret Store verification, completed Operation,
Receipt, Version, current-version CAS, affected-Run membership, and per-Run
fanout intents use the write-before-reference transaction. No response contains
secret bytes, a reusable store locator, or an integrity verifier.

Initial accepted `PENDING` response is the deterministic, unstored projection:

```json
{
  "protocol": "orcest.secret-provision-accepted/1",
  "secret_provision_operation_id": "lowercase-uuid",
  "state": "PENDING",
  "secret_id": "lowercase-uuid",
  "target_version": 1
}
```

It is returned with HTTP `202`. A same-body retry reads the current Operation:
if it is still `PENDING`, it derives those exact bytes again from immutable
fields; if terminal, it returns the stored terminal response. The response is
chosen from one SQLite read snapshot, so an async completion racing a lost
`202` yields either a valid pending projection or the terminal result and never
allocates another target, stages again, or installs two versions. Only terminal
responses are stored in `terminal_response_json/digest`.

Terminal bodies are closed:

```json
{
  "protocol": "orcest.secret-provision-result/1",
  "secret_provision_operation_id": "lowercase-uuid",
  "state": "COMPLETED",
  "secret_version_key": "lowercase-uuid:1",
  "target_version": 1,
  "new_version": 1,
  "credential_rotation_receipt_id": "lowercase-uuid"
}
```

```json
{
  "protocol": "orcest.secret-provision-result/1",
  "secret_provision_operation_id": "lowercase-uuid",
  "state": "REJECTED",
  "secret_id": "lowercase-uuid",
  "target_version": 1,
  "rejection_code": "CAS_LOST"
}
```

The status mapping in the field table is mandatory; free-form
provider/storage errors and fields from the other terminal variant are
excluded.

#### Secret Provision Checkpoint

Each accepted install/retry appends an immutable checkpoint:

| Field | Requirement |
| --- | --- |
| `secret_provision_checkpoint_id` | Controller-assigned UUID. |
| `secret_provision_operation_id` / `checkpoint_sequence` | Exact Operation and strictly increasing positive sequence. |
| `phase` | `VERIFY_STAGING` or `INSTALL_VERSION`. |
| `outcome` | `SUCCEEDED`, `FAILED_RETRYABLE`, or `FAILED_TERMINAL`. Invalid syntax/initial authority is rejected before Operation acceptance and has no checkpoint. |
| `failure_code` / `failure_evidence_digest` / `next_retry_ms` | For `FAILED_RETRYABLE`, code is `SECRET_STORE_UNAVAILABLE`, `TRANSIENT_STORAGE_ERROR`, or `TRANSIENT_DATABASE_BUSY`, evidence and retry time are required. For `FAILED_TERMINAL`, code is `CAS_LOST`, `AUTHORITY_REVOKED`, `STAGED_OBJECT_INVALID`, or `INTEGRITY_CONFLICT`, evidence is required, and retry time is `NULL`. All are `NULL` for `SUCCEEDED`. |
| `checkpoint_digest` / `recorded_at_ms` | Digest of normalized non-secret fields and informational time. |

`(secret_provision_operation_id, checkpoint_sequence)` is unique. At most one
terminal checkpoint exists, and `SUCCEEDED` is valid only for
`INSTALL_VERSION`. `SUCCEEDED` atomically installs
the exact `target_version`, Receipt, current reference, fanout, terminal replay
response, `COMPLETED` projection, and delivered retry outbox. `FAILED_TERMINAL`
atomically installs the exact rejection code/evidence, terminal replay response,
`REJECTED` projection, and delivered retry outbox without a Version or Receipt.
The same Operation replay returns that stored terminal response.

A `PENDING` Operation is rebuilt
from SQLite/outbox after Redis or controller loss; staged bytes, staging
receipt, keyed-attestation metadata, and all pending checkpoints are in the
same backup unit as the Secret Store and MUST NOT be garbage-collected. A
missing or corrupt staged object enters storage reconciliation and retry; it
does not fabricate a version or silently abandon the Operation. After
`COMPLETED`, the staged object has become the immutable Secret Version and
follows its audit-reference retention. `REJECTED` removes the staged object
from the live retry root and quarantines it; it may be collected only after a
configured grace period under the exclusive storage lock, while the Operation,
authentication/authorization evidence, opaque staging/attestation metadata,
checkpoints, and terminal response remain retained for replay and audit. A staged object with no accepted
Operation may be collected only after the registration idempotency/backup
retention window and an exclusive reconciliation proves no Operation,
Receipt, or Version references it.

`(secret_id, target_version)` is unique only among Operations whose state is
`PENDING` or `COMPLETED`. The transaction that marks an Operation `REJECTED`
releases that reservation but retains its target for audit. A later corrected
Operation may reserve the same target only under the per-Secret/storage lock
after proving there is no Secret Version, current `PENDING`/`COMPLETED`
Operation, installed target object, or unexpired quarantine that could be
mistaken for the new request. If quarantine cannot yet be safely removed, the
corrected request waits or fails closed; it never clobbers or adopts the
rejected stage.

### Credential Rotation Request

A Credential Rotation Request is the immutable idempotency/response ledger for
an Attempt-scoped rotation request, including a losing prior-version CAS. It is
not an Attempt Result or lifecycle trigger.

| Field | Requirement |
| --- | --- |
| `credential_rotation_request_id` | Caller-assigned lowercase UUID and global idempotency identity. |
| `protocol_version` | Exact literal `orcest.credential-rotation/1`. |
| `attempt_id` / `activity_id` / `attempt_generation` | Exact current claimed model-backed Attempt fence. |
| `worker_id` / `worker_session_id` | Exact authenticated claimant/session. |
| `attempt_capability_digest` / `launch_attestation_id` | Exact Attempt capability claims digest and accepted Launch Attestation. |
| `provider_account_ref` | Exact non-secret account authorized by the Attempt assignment. |
| `secret_id` / `expected_prior_version` | Exact logical Secret and positive submitted prior version. |
| `secret_request_attestation_id` | Opaque Secret Store keyed request-attestation identity proving replay byte equality; no raw bytes, unkeyed digest, key, or tag enters SQLite. |
| `request_digest` | Digest of every canonical non-secret request/authority field above, including the opaque attestation identity. |
| `disposition` | `APPLIED` or `CAS_LOST`. |
| `credential_rotation_receipt_id` / `accepted_version` | Required only for `APPLIED`; both `NULL` for `CAS_LOST`. |
| `current_version` | Current positive Secret version after evaluation; equals `accepted_version` for `APPLIED`, and the already-current version for `CAS_LOST`. |
| `response_http_status` / `response_json` / `response_digest` | Canonical stored replay response: `200` for `APPLIED`, `409` for `CAS_LOST`; digest covers status and exact body. |
| `accepted_at_ms` | Informational controller time, strictly before the Attempt execution deadline. |

The terminal response protocol is `orcest.credential-rotation-result/1` and
contains exactly request ID, disposition, Secret ID, expected prior version,
current version, and conditional accepted version/Receipt ID. Fields for the
other disposition are omitted. The opaque request-attestation identity is not
returned.

```json
{
  "protocol": "orcest.credential-rotation-result/1",
  "credential_rotation_request_id": "lowercase-uuid",
  "disposition": "APPLIED",
  "secret_id": "lowercase-uuid",
  "expected_prior_version": 3,
  "current_version": 4,
  "accepted_version": 4,
  "credential_rotation_receipt_id": "lowercase-uuid"
}
```

The `CAS_LOST` body uses the same first six fields with
`disposition = CAS_LOST` and omits `accepted_version` and
`credential_rotation_receipt_id`. No arbitrary provider error or secret
material is returned.

`credential_rotation_request_id` is unique. The same authenticated request ID,
Attempt/session/capability/Attestation/account/Secret/prior bindings,
`request_digest`, and Secret Store keyed byte-equality proof returns the stored
status/body while `controller_now_ms < execution_deadline_ms`. Reuse with
different bytes, key, attestation, or authority is an idempotency conflict.
This exact already-ledgered replay remains available after Attempt
terminalization when controller time is still before that deadline; it is a
response lookup and grants no new secret, version, or workflow authority.
First acceptance has the same strict deadline. At or after that deadline the
rotation endpoint cannot authenticate acceptance or replay: rotation authority
has ended even though the Attempt capability remains cryptographically
verifiable for bounded Result-endpoint handling.

For `APPLIED`, one per-Secret writer/storage transaction inserts the Request,
Credential Rotation Receipt and Secret Version, advances the current reference,
freezes affected-Run membership, and creates its durable fanout intent. For
`CAS_LOST`, the transaction stores only this request/response ledger and the
observed current version; it creates no Receipt, Version, reference change,
fanout, Result, or Transition. Any staged losing bytes are quarantined/removed
under the Secret Store lock while the opaque request attestation remains for
replay audit.

### Credential Rotation Receipt

A Credential Rotation Receipt is the immutable creation/provenance ledger for
every new Secret Version, including initial management provisioning.

| Field | Requirement |
| --- | --- |
| `credential_rotation_receipt_id` | Controller-assigned UUID. |
| `source_kind` / `source_id` | `ATTEMPT_ROTATION` plus the worker request idempotency UUID, or `MANAGEMENT_PROVISION` plus the authenticated secret-management operation UUID. The pair is unique. |
| `credential_rotation_request_id` | Exact `APPLIED` Credential Rotation Request for `ATTEMPT_ROTATION`; otherwise `NULL`. It equals `source_id`. |
| `secret_id` / `expected_prior_version` / `new_version` | Exact Secret Reference, compare-and-swap predecessor (`NULL` only for initial creation), and newly created positive version. |
| `purpose` / `owner_scope_kind` / `owner_scope_id` | Exact registered Secret purpose and owner copied from the logical Secret. A `FORGE_INSTALLATION` management Receipt copies the canonical installation/account owner from its Operation. |
| `provider_account_ref` | Exact non-secret account/scope when applicable. |
| `attempt_id` / `activity_id` / `attempt_generation` / `worker_id` / `worker_session_id` / `attempt_capability_digest` / `launch_attestation_id` | Required only for `ATTEMPT_ROTATION`; exact current authenticated Attempt/session/capability and accepted one-shot launch authority. |
| `management_operation_id` / `authenticated_principal_id` / `authorization_context_digest` | Required only for `MANAGEMENT_PROVISION`; exact server-authenticated secret-management operation and authority. |
| `secret_integrity_attestation_id` | Opaque controller-only Secret Store record proving the installed immutable bytes passed their keyed integrity check; never a secret-derived hash or keyed tag in SQLite. |
| `receipt_digest` / `created_at_ms` | Digest of all canonical non-secret provenance fields and informational commit time. |

The tagged union is closed: fields for the other source kind are `NULL`.
`(secret_id, new_version)` and `(source_kind, source_id)` are unique. An
`ATTEMPT_ROTATION` Receipt is valid only for a current model-backed Attempt
whose exact accepted `launch_attestation_id` is copied into and covered by
`receipt_digest`; deterministic `VERIFY` cannot rotate credentials. A
`MANAGEMENT_PROVISION` Receipt binds the exact immutable Secret Provision
Operation, including purpose/owner/account, its authenticated principal, and
authorization digest. The
Receipt and its `APPLIED` Credential Rotation Request have a reciprocal
deferrable binding: Request Receipt/version/current-version fields and every
Attempt/session/capability/Attestation/account/Secret/prior field match exactly.
`CAS_LOST` can never have a Receipt.
Secret Store write/verification, Secret Version, Receipt, current-reference
compare-and-swap, frozen affected-Run membership/digest, and durable fanout
intent commit under the write-before-reference protocol; the reciprocal
Version/Receipt references are deferrable within that transaction. Per-Run
`SECRET_VERSION` Transitions then commit independently and idempotently in
membership ordinal order. Repository content, a
worker without the exact rotation capability, and an unauthenticated operator
cannot create a Receipt. Restoring the same immutable Secret Version preserves
its original `creation_receipt_id` and never fabricates new creation authority.

Each Secret Version row also stores `affected_run_ids_digest`, the SHA-256 of a
canonical ordered membership required even when empty. At the transaction that
makes a new version current, the writer freezes the byte-sorted active Runs
whose current `SECRET_RECOVERY` Wait Condition or
`REQUIRED_SECRET_OR_PERMISSION` Human Boundary names that exact Secret ID and a
minimum version satisfied by the new version. Persistence normalizes rows as
`(secret_id, version, run_ordinal, run_id)` with zero-based contiguous ordinals
and unique Run IDs. The version/reference update, complete membership/digest,
and durable fanout intent commit atomically. Individual per-Run Transitions do
not join that transaction; restart resumes the first membership ordinal without
a Transition.

The controller reduces `T(SECRET_VERSION, SecretVersionKey)` once for each
frozen member in ordinal order. A still-matching Wait or Boundary follows its
closed lifecycle row; a binding superseded before fanout gets only a same-state
audit Transition. Per-Run Transition uniqueness is independent of later
specification generations, and replay returns the original Transition. A Run
that begins after activation observes the current Secret Reference directly and
does not reuse historical fanout.

The Secret Store owns value, purpose, creation time, and integrity metadata.
SQLite MAY store non-sensitive purpose and scope metadata for validation, but
never the secret value. Raw Attempt capability bearer values are likewise not
domain fields; only a digest or JTI needed for revocation may be durable.

## Projection

A Projection is any derived external or operator-facing representation:

- `orcest:ready`, `orcest:working`, or exceptional status labels;
- issue/PR comments and markers;
- forge Checks or commit statuses;
- Redis event streams;
- monitoring SQLite data; or
- CLI/dashboard output.

A projection operation binds to a durable source object and uses an idempotency
marker where supported. Projection failure schedules repair and never rolls
back or invents the authoritative transition.

## Lifecycle state taxonomy

The Run `state` enum is owned by the [workflow
lifecycle](workflow-lifecycle.md). v1 states are:

```text
ADMITTED            PLANNING          BUILDING
VERIFYING           REVIEWING         AGGREGATING
REMEDIATING         DIAGNOSING        REPLANNING
ADJUDICATING        APPROVED          PUBLISHING
PR_MONITORING       PR_REMEDIATING    RECOVERING
WAITING             NEEDS_HUMAN
MERGED              CLOSED            CANCELLED
```

Only `MERGED`, `CLOSED`, and `CANCELLED` are terminal. `NEEDS_HUMAN` is an
exceptional resumable waiting state.

## Formal cross-page invariants

These expressions are canonical for v1.

### I1 — one active Run

```text
count(Run where WorkItemKey = K and terminal_outcome is NULL) <= 1
```

### I2 — immutable identities and monotonic generations

Controller Mode/Capability Key Operations, Project Registration Operations,
Workflow Blobs, Policy Updates, Snapshots, Snapshot Generations, Activities,
Activity Review Assignments and their subject/finding memberships, Attempts,
Attempt Claims, Capability Signing Keys, Launch Attestations, Attempt Terminal
Facts, Result Requests,
Candidate Uploads, Credential Rotation Requests, Candidates, Receipts,
Decisions, Wait Conditions, Recovery Evidence, immutable Health Probe Request
input fields, immutable Health Probe Fact evidence fields, Health/Forge
Observations and Change Request Search Member relations, immutable Forge Observation Request input fields and
ordered result memberships, immutable Forge Observation Schedule identity/
authority/cadence fields, and immutable Health Probe Fact Run memberships,
Publication Effects and Checkpoints, Terminal Duplicate Cleanup Reservation
and Member inputs and Cleanup Action immutable inputs, Reconciliation Facts, Controller
Operation Facts, Timer Facts, Capacity/Worker Loss Reports, Storage
Restoration Operations and Facts, Management Commands, Human Boundaries, Human Resolutions,
Secret Provision Operation request/provenance fields and their Checkpoints,
Credential Rotation Receipts, and Transitions are immutable; the Operation
state and last-checkpoint fields are only the monotonic projections defined in
that section. For a per-parent sequence or generation, a
later created value is strictly greater; equality is idempotent only under that
object's stated identity and digest rule.
The Controller Mode and Capability Key Registry revisions/pointers are only
the closed CAS projections defined by those operations. Capability Signing Key identity, algorithm, key material, and registration
times are immutable; only its closed monotonic retirement/revocation
projection may change under the rule in that object's section.
Forge Observation Schedule `schedule_revision`, `last_request_id`,
`next_due_at_ms`, `last_discovery_search_revision`,
`last_discovery_set_digest`, and `state`; Forge Observation Request `state`,
result digest, discovery result pair, and completion time; Health Probe Request
`state` and Fact pointer; and Health
Probe Fact fanout cursor/completion are only the closed monotonic mutable
projections defined by their sections. Reservation state/cursor and Cleanup
Action state/result are only the closed monotonic projections in that section.
Their Schedule/Request/Fact/Reservation/Action identities,
immutable inputs, and membership digests never change.

### I3 — current-generation result acceptance

An Attempt Result is accepted only when its Attempt is the Activity's unique
current nonterminal `CLAIMED` generation, every immutable binding matches, and
its exact `claimed_worker_id` and `claimed_worker_session_id` match the
authenticated submitter. This includes the Activity Review Assignment for a
review/adjudication Result and the Attempt's exact frozen
`provider_family`/`model_family`/`classification_revision`. Every model-backed
Result and its Receipt, when present, copy the exact accepted
`launch_attestation_id`; deterministic `VERIFY` carries `NULL`. Also,
`controller_now_ms < execution_deadline_ms`.
Strictly before the capability-auth expiry, the sole post-deadline success
response is replay of an identical Result accepted before the deadline; a
first late submission may instead create or replay only the bounded
late-disposition Result Request and timeout rejection, which is not an
accepted Result. At or
after that expiry the capability cannot authenticate either path and creates no
workflow or late-request row.
An exact already-ledgered Credential Rotation Request may replay after Attempt
terminalization only while `controller_now_ms < execution_deadline_ms`; it is a
no-new-authority response lookup. At or after the execution deadline, the
Attempt capability authenticates no rotation, source, upload, launch, liveness,
or other non-Result endpoint path.

### I4 — plan before dispatch

No worker notification may be emitted unless its Activity, `OFFERED` Attempt,
required Activity Review Assignment and complete subject/finding memberships
when applicable, and Outbox row
are already committed in one SQLite transaction.
Every planned Activity uses the canonical deterministic idempotency key;
replaying one reducer Transition returns the same Activity, while a later
repair/recovery cycle has a distinct evidence/cycle-bound key.

### I5 — Redis is reconstructible

Deleting Redis cannot change Controller Mode/Capability Key Registry or any
of their Operations, Project Registration Operation, Workflow Blob, Policy Update, Snapshot
installation, Run, Activity or Activity Review Assignment/membership,
Attempt/Attempt Claim/Launch Attestation/Terminal Fact, Result Request,
Capability Signing Key, Candidate Upload, Candidate, Receipt, Decision,
Publication/Effect/Checkpoint/Reconciliation Fact, Forge/Health Observation
and Health Probe Request/Fact, Forge Observation Schedule/Request, Wait
Condition, Recovery Evidence, Timer Fact,
Storage Restoration Operation/Fact, Capacity/Worker Loss Report, Controller Operation
Fact, Management Command, Human Boundary/Resolution, or Transition. Every
Secret Provision Operation/Checkpoint and Credential Rotation Request/Receipt
is durable as well.
Every required notification and due Timer Fact can be re-derived from unfinished
durable state.

### I6 — durable Candidate boundary

A live invocation's workspace MAY be lost. An admitted Candidate and its
accepted worker producer result or controller import fact MUST survive
controller/Redis/worker restart.

### I7 — no dangling Candidate reference

```text
Candidate row exists => stored bytes exist and sha256(bytes) = bundle_digest
```

The converse may temporarily be false for an orphan artifact.

### I8 — exact-Candidate evidence

Every Verification Receipt, Review Receipt, Adjudication Receipt, and Consensus
Decision binds both `candidate_id` and exact commit. A different current
Candidate makes prior evidence ineligible for gating. Their producing
Activities/panel sets also bind the installed Snapshot generation and
`policy_hash`; a policy-only generation update makes all old-policy evidence
ineligible even when the Candidate is retained.

### I9 — reducer and order independence

Only the reducer changes Run state. Given the same durable state, normalized
input set, and ordered Forge Observations, Policy Updates, health facts, and
management inputs, it emits the same output. Timer expiry enters only through
the unique scoped Timer Fact. Review aggregation canonicalizes
receipts, so arrival order cannot change consensus.
A Forge Observation is reduced exactly once per Run across generations; a
later decision reads its durable projection through `INTERNAL`. Snapshot
pending/coalescing compares `supersession_key`, whose only base-sensitive v1
form is `SUPERSEDE_AT_BOUNDARY`.
A Transition consumes every other durable causal input under the same
generation-independent `(run_id, trigger_kind, trigger_id)` rule. The stored
`specification_generation` is audit output describing the first evaluation,
not another identity dimension or permission to reapply an input.
A Policy Update composes with the highest accepted Work Item observation
sequence, including pending input; it cannot regress specification state to the
installed Snapshot.

### I10 — policy is not weakened by failure

Worker, provider, timeout, budget, or capacity failure can change recovery
strategy or wait duration, but not required verification, reviewer
independence, approval count, blocker rule, or publication fence.

### I11 — worker authority exclusion

A worker cannot admit/supersede/cancel a Run, accept a result or Candidate,
write another Run's artifact, update a publication branch, or create/modify a
Change Request.

### I12 — Candidate execution isolation

Candidate code runs without forge-write, controller-write, Secret Store, or
unrelated-Run credentials. Any required credential is purpose- and
Attempt-scoped and cannot grant these authorities. A model-backed Attempt gets
provider material only after a registered runner shim's signed, one-shot
Launch Attestation proves globally unique fresh workspace, context, and
invocation identities with no resume/parent; the attester gains no workflow or
result authority.

### I13 — external effects are reconciled

No external call is described as exactly once. Every retriable mutation has a
stable idempotency identity and a read-after-ambiguity reconciliation rule.
Every Publication mutation binds its monotonic `effect_generation`; a stale
generation response cannot advance Publication state. Checkpoint order/status
must satisfy the closed suboperation matrix, and restart resumes the same active
effect rather than inventing a new mutation.
Cancellation is likewise reconciled whenever a Change Request is observed or
a durable `CHANGE_REQUEST_CREATE/REQUEST_READY` or `AMBIGUOUS` checkpoint says
one may exist. It cannot become terminal until reconciliation proves the stable
create request produced no Change Request or an exact owned unmerged-close
observation arrives; a racing merge wins as `MERGED`.
Terminal duplicate cleanup is the same kind of reconciled effect: every
close/detach commits an immutable Action/outbox before I/O, uses exact member
and Effect CAS fences, and advances only from an authenticated Observation or
source-unique `INTERNAL` continuation. It never authorizes a different merge
outcome.

### I14 — revision fencing and single engine ownership

Publication and remediation compare-and-swap against the recorded expected
forge revision. Exactly one of the v1 Run engine or legacy PR engine may own a
run-associated Change Request. While a v1 Publication's Run is live, the legacy
engine excludes both its exact associated Change Request stable ID and its
deterministic source ref unconditionally; marker state and repair phase are not
predicate inputs. It also excludes any Change Request carrying a
syntactically valid Orcest v1 marker even if its durable Run is temporarily
unknown. Terminalization does not release a stable ID/ref named by an `ACTIVE`
Terminal Duplicate Cleanup Reservation: every unresolved member and the
Reservation's deterministic ref remain excluded until that member is closed,
its marker is detached under exact CAS, or its bounded `RETAINED_AUDIT` action
completes. A still-present syntactically valid marker remains excluded
independently after Reservation completion. Otherwise the ID/ref exclusion
ends only at terminal Run outcome or an explicit authenticated
engine-migration transaction; v1 defines no such migration.
Neither engine may infer write authority from an unknown marker.

### I15 — secret confinement

Raw secret values never appear in normal Redis envelopes, workflow rows,
Candidate artifacts, traces, projections, or events. Only versioned Secret
References may appear there. A Secret Version's SQLite row, Secret Store bytes,
keyed integrity metadata, and immutable creation Receipt remain live while any
retained Transition, Human Resolution, Snapshot/policy binding, Attempt, Wait
Condition, Human Boundary, or other retained audit object references that
version or Receipt. Secret Version garbage collection is permitted only after
every such referencing audit row has left its retention window; v1 has no
tombstone that substitutes for the retained bytes or provenance.

### I16 — exceptional human boundary

`NEEDS_HUMAN` can be entered only with a current immutable controller-issued
Human Boundary carrying an allowlisted reason, exact lifecycle bindings, the
minimum request, required resolution kinds, and durable proof that applicable
self-heal strategies were exhausted. It is resumed only by one idempotently
accepted authenticated typed Human Resolution for that exact current boundary.
Repository configuration, prompts, workers, and issue/PR comments can neither
create a human gate nor satisfy one.

## Deletion and retention constraints

- Active Runs and any object reachable from them MUST NOT be garbage-collected.
- A Secret Version row, its Secret Store bytes/keyed integrity metadata, and
  Credential Rotation Receipt MUST NOT be collected while any retained audit
  or workflow row references the version or Receipt.
- Attempt Claims, Result Requests, Credential Rotation Requests, and
  their stored response contracts follow their Attempt/secret audit retention;
  deleting Redis or expiring a bearer never deletes their replay authority.
- Capability Signing Keys and their public verifiers remain retained through
  every referenced capability's maximum cryptographic expiry and every
  referencing audit object's retention; retirement is never deletion.
- Controller Mode and Capability Key Registry projections and their successful
  and rejected operation ledgers remain in the controller backup/audit unit;
  Health Probe Requests remain with their Fact/Observation or supersession
  evidence, including the pre-I/O Outbox identity.
- Forge Observation Schedules, every `PENDING`, `COMPLETED`, or `SUPERSEDED`
  Request, reciprocal
  Outbox, credential/effect fence, and ordered result membership remain in the
  controller backup/replay unit while their Project/Run or request idempotency
  window is retained.
- Every Forge Request Failure Fact remains with its Request, outbound-attempt
  ordinal, retry projection, Recovery Evidence, Wait, and Transition. A
  pending Request cannot lose its latest failure/retry authority during GC.
- Budget Reports, their authorization and response replay fields, frozen
  Report-Run membership/cursor, and every dependent Recovery Evidence, Wait,
  or Transition remain through the Project/Run audit and report-idempotency
  retention windows.
- Every Health Probe Fact retains its immutable ordered Fact-Run membership and
  membership digest with all affected Run audit rows; its cursor/completion
  projection remains in the same backup unit until fanout and retention finish.
- Terminal Duplicate Cleanup Reservations, their complete Search Observation
  and ownership memberships, every Member/Action generation, reciprocal
  Outbox, success/mismatch Observation, and selected terminal Publication proof
  remain in the controller backup/replay unit until the Reservation completes
  and the Run/Publication audit retention window expires. Terminal Run garbage
  collection never deletes an `ACTIVE` Reservation.
- Candidate Upload rows and promoted bytes remain until they are consumed,
  expire and pass orphan cleanup, or every referencing Result Request/Attempt
  leaves retention. An expired response remains reconstructible from the row.
- Current and historical Wait Conditions, Recovery Evidence, Health and Forge
  Observations used by retained decisions, Timer/Storage Restoration/Controller
  Operation Facts, Human Boundaries, and Human Resolutions follow their Run's
  audit retention.
- A coalesced Forge Observation, its creating Request, and every Request-result
  membership remain until the last referencing Request membership and Run
  Transition leave retention, including pre-admission discovery history. A
  `CHANGE_REQUEST_SEARCH_RESULT` retains both complete live and terminal child
  memberships and their digest for the same interval.
- Candidate bytes MUST NOT be deleted before all referencing Runs and audit
  retention windows expire.
- Projection retention does not determine domain retention.
- Deleting a terminal Run's audit history is an explicit retention operation,
  not a workflow transition.

Exact retention periods, backup, and garbage-collection transactions belong to
[persistence and recovery](persistence-and-recovery.md).

## Evidence and migration

### Current evidence retained

- `src/orcest/shared/models.py` already uses UUID task IDs and carries PR
  snapshot fields on Tasks and Results. v1 retains immutable work identity but
  separates Activity, Attempt generation, Candidate, and Observation instead
  of overloading one Task row.
- `src/orcest/orchestrator/pr_ops.py` keys retry budgets and stale checks to a
  PR head SHA. That exact-revision principle becomes Candidate and Forge
  Observation binding.
- `src/orcest/shared/coordination.py` stores pending metadata with task ID,
  head SHA, reason, and creation time. These fields inform the durable Activity
  and Attempt model while the Redis marker itself becomes disposable.
- `src/orcest/monitor/db.py` uses `(source, id)` to deduplicate at-least-once
  event delivery. v1 applies the same explicit idempotency principle to
  transitions, results, receipts, and side effects.
- `src/orcest/shared/providers.py` exposes non-secret provider/account
  identities derived from credentials. v1 keeps non-secret provider identity
  but replaces credential-derived durable identity with stable Secret
  References and versions.

### Behavior replaced

- Current `Task` records combine plan, delivery, attempt, prompt, source token,
  provider secret, and external resource identity. v1 decomposes these into
  immutable domain objects with separate authority.
- Current issue attempts use the string sentinel `issue` because an issue has
  no head SHA. v1 Work Item Snapshots provide a real specification and base
  identity for issue-originated work.
- Current attempt and pending keys expire from Redis. v1 Attempt claim,
  generation, and deadlines persist in SQLite; only liveness leases expire in
  Redis.
- Current `TaskResult.needs_human` lets agent output directly trigger a label.
  v1 treats agent text as evidence only; the controller creates an allowlisted
  human-boundary reason through the reducer.
- Current credential rotation can place raw updated credentials in result
  streams and private Redis checkpoints. v1 Attempt Results contain only
  Secret References; the Secret Store owns rotation values.

### Deliberately deferred rollout and implementation validation

These experiments are not prerequisites for reviewing the normative domain
contract. Production enablement deliberately defers until repository evidence
demonstrates these implementation and rollout gates.

1. No current SQLite schema implements these constraints. The persistence spec
   must turn the logical model into migrations, foreign keys, partial unique
   indexes, transaction boundaries, and corruption checks.
2. Git object format and bundle admission are absent today. The Candidate
   implementation must prove one-tip extraction, base reachability, object and
   byte limits, digest verification, and safe handling of malicious bundles.
3. GitHub issue data currently uses issue number as identity. The adapter must
   select and persist a stable repository and Work Item external identity that
   survives repository rename and does not collide across forge instances.
4. Current events do not provide a total order for reducer inputs. The Run
   Store must allocate observation and transition sequences transactionally.
5. Current provider independence is not represented. The review protocol must
   define which provider/model/account combinations may fill distinct reviewer
   slots without exposing secret-derived identity.
6. Current result summaries and prompts are unbounded workflow-bearing text.
   Every protocol must define size limits and separate structured decision
   fields from projection-only text.
