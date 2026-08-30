# Worker Protocol

> **Status:** Accepted normative v1 specification (2026-08-27)
>
> **Canonical owner:** versioned worker delivery and control-plane API,
> authentication, claim fencing, liveness, Candidate upload, typed Attempt
> results, credential handoff, and pool-manager capacity/loss reports.

## Purpose and scope

This page defines the v1 contract between the Orcest controller, disposable
Redis delivery, fleet workers, and the pool manager. It owns activity delivery,
authenticated claim, Attempt fencing, worker liveness, source and provider
credential delivery, candidate upload, typed result submission, and worker-loss
and capacity reporting.

The durable object definitions and uniqueness constraints are owned by
[Domain model](domain-model.md). Legal workflow transitions and autonomous
recovery policy are owned by [Workflow lifecycle](workflow-lifecycle.md).
Artifact and Secret Store atomicity are owned by
[Persistence and recovery](persistence-and-recovery.md). This page defines the
protocol boundaries those pages must support.

The protocol has three primary safety goals:

1. Redis loss or duplicate delivery may repeat execution, but cannot accept two
   outcomes for one fenced Attempt generation.
2. A worker can read only the source, Candidate, instructions, and credentials
   required by its claimed Attempt. It cannot publish a Change Request or alter
   workflow authority.
3. A Candidate is durable and controller-verified before an Activity outcome
   referring to it is accepted.

## Authority boundaries

- The controller MUST be the only component that creates Activities, allocates
  Attempt generations, accepts Attempt outcomes, admits Candidates, and asks
  the forge adapter to publish code.
- Redis MUST carry only disposable delivery, liveness, heartbeat, wake-up, and
  cache data. A Redis stream delivery MUST NOT constitute an accepted claim.
- The worker MUST treat its workspace and agent process as disposable. Work
  that has not crossed the controller's Candidate-admission boundary may be
  lost and retried.
- The pool manager MAY create and destroy worker VMs, but MUST report worker
  loss through the controller rather than manufacturing an Activity outcome.
- Candidate code MUST execute without forge-write credentials, publication
  credentials, controller administration credentials, or credentials for an
  unrelated Run.

Publication ownership and terminal cleanup are controller-only concerns. A
worker offer, claim, launch attestation, upload, or Attempt Result MUST NOT
carry or establish a Change Request ownership proof, complete-marker search
membership, `ownership_status`, `ownership_proof_digest`, or
`external_reliance_digest`. Workers never select a retained or merged member,
create a terminal duplicate-cleanup Reservation/Member/Action, close or detach
a Change Request, repair a Run marker, or emit a `MERGED`/`CLOSED` Run
outcome. The controller derives those fields from its authenticated Forge
Observations and durable Effect/Transition fences after Candidate admission;
the worker protocol can only deliver the Candidate or typed Attempt evidence
that the controller needs for that reduction.

## Protocol versions and compatibility

The v1 Redis stream for a Worker Profile is:

```text
tasks:activity:v1:<worker-profile>
```

The consumer group is `orcest-workers-v1`. A Worker Profile is a validated
lowercase identifier naming a compatible worker image/capability set, such as
`codex`, `claude`, or `grok`; it is not an authorization decision.

A server-registered **Execution Profile** is different: it is the
project-allowlisted policy resource selected by repository fields such as
`implementation.profile`, `verification.repair.profile`, reviewer-slot
`profile`, or adjudicator `profile`. Before creating an Attempt, the controller
resolves that Execution Profile and the pinned fallback selection to one
immutable execution assignment:

```text
execution_profile_id -> worker_profile, provider, model,
                        provider_account_ref
```

The exact four-value execution assignment remains distinct from the
server-derived, non-secret `provider_family`, `model_family`, and
`classification_revision` metadata frozen on the Attempt for later
independence checks. The Attempt also freezes the code-owned tool and sandbox
limits required by that resolved assignment.

`worker_profile` is only the fleet routing and compatibility value used in the
Redis stream and authenticated worker claim. It is not an Execution Profile
ID and cannot choose a provider or model. `provider` and `model` are nullable
only for credential-free deterministic execution such as `VERIFY`. The v1
Verification Profile ID `default` names the frozen verification command set;
it is neither an Execution Profile nor a Worker Profile. Workers MUST NOT
resolve, substitute, or reinterpret any of these identifiers.

Legacy streams such as `tasks:claude`, `tasks:issue:claude`, and their other
provider variants MUST remain separate. A v1 worker MUST NOT consume a legacy
stream, and a legacy worker MUST never be added to `orcest-workers-v1`. A
deployment MUST prove that at least one compatible worker can consume every
configured v1 Worker Profile before the controller publishes to it.

Capacity registration MUST also separate worker classes. A Capacity Pool and
VM template is registered as exactly one of `LEGACY` or `V1_CLONE_FIXED`; a
pool-manager principal, Worker Session, and template identity cannot cross
that class boundary. `V1_CLONE_FIXED` identifies the v1 image/template class
that implements the Attempt-scoped read-only clone handoff and credential
scrubbing contract in this protocol. Its sessions may consume only
`tasks:activity:v1:*` through `orcest-workers-v1`. Legacy sessions and their PEL
reaper may access only allowlisted legacy streams/groups. The controller rejects
a capacity observation, claim, ACK, or loss report whose registered pool,
template class, Worker Profile, session, or stream class disagrees.

Every Redis offer and HTTP body carries a protocol media identifier. Within
major version 1, readers MUST reject missing required fields and MUST ignore
unknown fields. A change that alters the meaning or type of an existing field,
weakens an authorization check, or changes claim/result ordering requires a new
major version and a new stream namespace.

## Redis activity offer

An outbox dispatcher publishes one flat Redis stream entry with these required
UTF-8 string fields:

| Field | Meaning |
| --- | --- |
| `protocol` | Literal `orcest.activity-offer/1` |
| `protocol_version` | Literal base-10 `1` |
| `redis_epoch` | Base-10 positive diagnostic reconstruction epoch recorded by the dispatcher; never an authority fence |
| `outbox_id` | Immutable UUID identifying the durable outbox intent |
| `attempt_id` | Durable Attempt UUID used by the worker API |
| `activity_id` | Durable Activity UUID |
| `generation` | Base-10 positive Attempt generation |
| `worker_profile` | Worker Profile named by the stream suffix |
| `claim_deadline_ms` | Base-10 integer Unix-millisecond offer deadline |

`claim_deadline_ms` is immutable: the controller freezes
`offered_at_ms`, the server-bounded `claim_timeout_ms` from the installed
Snapshot policy, and their exact sum when it creates the Attempt/outbox. Queue
delay, Redis rebuild, and live configuration cannot extend it.

The offer MUST NOT contain a repository token, provider credential, prompt,
issue body, Candidate download capability, Attempt capability, secret value, or
raw Secret Store locator. It is an untrusted hint to call the controller.

The dispatcher MUST publish only after the transaction containing the Activity,
Attempt, and outbox row commits. Republishing the same `outbox_id` is allowed.
Redis reconstruction republishes every current, claimable outbox intent; it
does not infer work from Redis entries.

A worker performs only these operations on an offer:

1. Validate the required fields, major protocol version, stream/Worker Profile match,
   and local Worker Profile capability.
2. Call the authenticated claim endpoint.
3. ACK the Redis entry after the controller confirms a durable claim or says
   the offer is terminally stale or already claimed by another worker.
4. Leave the entry pending while a claim response is ambiguous and retry the
   same claim. It MUST NOT begin the agent invocation before claim succeeds.

A malformed offer MAY be copied to a bounded diagnostic stream after redaction,
but the controller remains responsible for redispatching its durable Activity.
Dead-letter state MUST NOT suppress the Activity.

`redis_epoch` lets operators and workers recognize which Redis reconstruction
published an offer. It is neither part of Attempt identity nor a precondition
for claim, liveness, upload, or Result acceptance. A worker SHOULD prefer a
current-epoch notification when it has both, but it MUST still let the
controller decide an otherwise well-formed old-epoch offer by durable
`attempt_id`, `activity_id`, `generation`, and `outbox_id`.

## Authentication and capability model

All controller endpoints in this page MUST use HTTPS with certificate
validation. Plaintext HTTP is forbidden, including on the private fleet
network.

The v1 API uses two bearer credential classes:

- A **Worker Session credential** identifies one boot of one worker VM. Its
  claims bind `worker_id`, an unguessable `worker_session_id`, allowed Worker
  Profiles, worker build revision, issued time, and expiration. Fleet
  provisioning owns issuance and delivery. Reusing a VM ID creates a new
  `worker_session_id`; an old session cannot act for the replacement.
- An **Attempt capability** is returned only after claim. Its signed claims
  bind `attempt_id`, `activity_id`, `generation`, `worker_session_id`,
  `execution_profile_id`, the exact `worker_profile`/`provider`/`model`/
  `provider_account_ref` assignment, `provider_family`, `model_family`,
  `classification_revision`, the Activity Review Assignment's
  `assignment_digest` when applicable, permitted endpoint operations, its JTI,
  exact `capability_key_registry_revision`, `capability_signing_key_id`, and
  `signature_algorithm`,
  the durable execution deadline, and
  `capability_auth_expires_at_ms = execution_deadline_ms + 86400000`. The
  literal v1 authentication grace is exactly 86,400,000 milliseconds. Fields that are
  canonically null for deterministic `VERIFY` or a non-review Activity remain
  signed as null rather than being inferred. Every Attempt endpoint verifies
  both the signed claims and the current durable Attempt, Activity, and
  applicable Assignment rows.

Bearer values MUST contain at least 256 bits of entropy or equivalent signed
security. SQLite may store a verifier, signature claims, or key reference, but
not the bearer value. Signing/encryption keys live in the Secret Store.
Controller logs, traces, metrics, error bodies, Redis, and Candidate artifacts
MUST redact bearer credentials and materialized forge/provider secrets.

Every Attempt and launch capability carries the exact durable
`capability_signing_key_id` (`kid`) and literal v1 `signature_algorithm =
ED25519`. The controller resolves that public verification-key row before
checking the signature and requires the claims, registry algorithm, and stored
normalized-claims digest to agree. `ACTIVE` keys may issue; `RETIRED` keys issue
nothing for a new claim set but may equivalently rematerialize and verify an
already-issued exact pinned claim set through its cryptographic expiry;
`REVOKED` keys deny capability authentication, capability-backed endpoint
replay, and the launch equality lookup immediately. This does not erase a
non-capability management audit row, but no worker response may be retrieved by
proving a revoked capability. Unknown keys and algorithm substitution fail closed. These
controller capability keys are distinct from the registered runner key that
signs a Launch Attestation.

One narrow retained-verifier exception applies after a launch capability's
time expiry: an `ACTIVE` or `RETIRED` public key may establish signature
equality with the exact frozen claims solely to locate the same retained
`launch_attestation_id`/digest and return `EXPIRED` with no provider material
or mutation. This is lookup proof, not authentication: it cannot authorize a
new Attestation or any other endpoint. It requires the same registered
runner/session and exists only while both the Attestation and its signing-key
reference remain under declared audit retention; it cannot reconstruct a
purged object. A `REVOKED` key denies even this lookup.

An Attempt capability cannot claim another Activity, extend the durable
execution deadline, access a different Candidate, change workflow policy, or
publish to a forge. Revoking, superseding, or terminalizing the Attempt makes
the capability unable to authorize new execution/read/write authority. Before
the execution deadline, a terminalized Attempt may use the credential-rotation
endpoint only to replay an already-ledgered exact CredentialRotationRequest
response; that lookup grants no new credential authority. At or after the
execution deadline but strictly before
`capability_auth_expires_at_ms`, it authenticates only the Result endpoint's
durable Result Request timeout rejection and exact accepted-Result replay.
Every other Attempt endpoint denies. At or after authentication expiry it
authenticates nothing and creates no ledger row; the Timer Fact sweeper owns
timeout recovery. The Result replay exception exposes no new object and
performs no workflow mutation other than an immutable `ACCEPTED` Result Request
mapping to the already accepted Result.

## Common HTTP conventions

Requests use `Content-Type: application/json` unless an endpoint explicitly
accepts bytes. Worker-session calls send:

```text
Authorization: Bearer <worker-session-credential>
X-Orcest-Protocol: 1
```

Attempt-scoped calls use the same headers with the Attempt capability. Every
mutating JSON request includes an immutable `idempotency_key` UUID except the
claim request, whose `attempt_claim_id` is its durable replay identity; the
Launch Attestation request, whose `launch_attestation_id` plus one-shot launch
capability is its replay identity; and liveness, whose strictly increasing
session sequence is disposable current-control evidence rather than a durable
request ledger. The raw Candidate upload instead uses its controller-issued
`upload_id` plus declared and computed digest; credential rotation uses
`credential_rotation_request_id`, the durable Credential Rotation Request
identity. Reusing an idempotency identity with an identical canonical
request returns the original result; reusing it with different content returns
`409 IDEMPOTENCY_CONFLICT`.

Unless an endpoint below defines a narrower closed exact response (including
Candidate upload expiry and `STALE_ATTEMPT` Result fencing), errors use this
generic body:

```json
{
  "protocol": "orcest.error/1",
  "code": "ATTEMPT_STALE",
  "retryable": false,
  "message": "safe non-secret summary",
  "retry_after_seconds": 0
}
```

The stable status/code classes are:

| HTTP | Codes | Worker action |
| --- | --- | --- |
| `400` / `422` | `MALFORMED`, `SCHEMA_INVALID`, `DIGEST_MISMATCH` | Correct locally if possible; otherwise submit a typed failure or let the deadline recover it |
| `401` / `403` | `AUTH_INVALID`, `CAPABILITY_DENIED` | Stop; never retry with the same credential indefinitely |
| `404` | `ATTEMPT_UNKNOWN`, `UPLOAD_UNKNOWN` | Treat the offer/result as stale and stop |
| `409` | `ATTEMPT_STALE`, `ATTEMPT_ALREADY_CLAIMED`, `IDEMPOTENCY_CONFLICT`, `RESULT_ALREADY_ACCEPTED` | Do not run or overwrite work; reconcile as described below |
| `410` | `CLAIM_EXPIRED`, `EXECUTION_DEADLINE_EXCEEDED` | ACK the stale offer and stop |
| `410` | `UPLOAD_EXPIRED` | Never reuse that upload; if the Attempt is still current and before its execution deadline, create a fresh upload and submit a new Result request identity/body |
| `429` | `CONTROLLER_RATE_LIMIT` | Retry after the stated delay without weakening the deadline policy |
| `503` | `CONTROLLER_MAINTENANCE` | Reuse the same request identity/body after the bounded delay only while its endpoint authority remains live; the controller stored no new request row |
| `500` / `503` | `CONTROLLER_UNAVAILABLE`, `STORE_UNAVAILABLE` | Retry with bounded backoff while the Attempt remains current |

An HTTP timeout is always an ambiguous result. Retry uses the identity declared
by that endpoint: ordinary JSON `idempotency_key`; `attempt_claim_id` for
claim; `launch_attestation_id` plus the same signed attestation digest for
launch; `upload_id` plus the same complete-body digest for upload content;
the Result body's `idempotency_key` as its durable Result Request ID;
`credential_rotation_request_id` for rotation; and the documented report
identity for capacity or loss. Liveness has no replay key: after timeout the
caller sends the next strictly higher session sequence. A caller MUST NOT
silently substitute a fresh identity for an ambiguously committed mutation,
and a durable keyed retry is attempted only while that endpoint's declared
authentication/deadline authority window remains open.

## Durable claim

### Request

```http
POST /api/v1/attempts/{attempt_id}/claim
```

```json
{
  "protocol": "orcest.attempt-claim/1",
  "attempt_claim_id": "1cbd0ee4-5fc9-4d1d-96e3-05b424209aeb",
  "redis_epoch": 8,
  "outbox_id": "c7c782e5-021b-47c1-baf7-dac03374a7ee",
  "activity_id": "de814cb9-6ca8-478b-9ff5-a71090f13ea6",
  "generation": 3,
  "worker": {
    "worker_id": "orcest-worker-10002",
    "worker_session_id": "81633657-fbb5-47a5-a62e-2c6282437395",
    "worker_profile": "codex",
    "build_revision": "git-sha-or-image-digest"
  }
}
```

The controller MUST perform one SQLite transaction that verifies all of the
following before changing state:

- the path `attempt_id`, body `(activity_id, generation)`, and outbox
  `outbox_id` refer to the same durable delivery intent;
- the pre-existing Attempt is the Activity's current `OFFERED` generation;
- controller time is strictly less than the durable claim deadline;
- the Run and specification generation are active;
- durable Controller Mode currently permits claims (`RUNNING` or
  `INTAKE_PAUSED`); `DISPATCH_PAUSED`, `DRAINING`, and `MAINTENANCE` reject even
  a previously published unclaimed offer without changing its Attempt;
- the worker session is valid and permits the required Worker Profile; and
- no incompatible worker session already owns the Attempt; and
- the durable Capability Key Registry has a selected `ACTIVE`, not-before-
  satisfied issuance key; the Claim transaction copies its exact registry
  revision/key into both capability claim sets or rejects without claiming.

The controller validates `redis_epoch` as a positive diagnostic value and may
record whether it differs from the current reconstruction epoch, but MUST NOT
reject or alter a claim for that difference. In particular, a controller or
Redis restart does not stale an `OFFERED` Attempt or any already `CLAIMED`
Attempt. The canonical claim `request_digest` still includes the submitted
epoch so one `attempt_claim_id` cannot replay different bytes; that digest
binding does not make the epoch authoritative.
Only the durable Attempt, generation, session, capability, immutable
inputs, state, and deadlines fence subsequent calls.

On success one transaction inserts the immutable Attempt Claim, records the
worker/session/profile/build identities, claim time, fixed execution deadline,
literal-v1 capability-authentication expiry, Attempt capability JTI/claims
digest, conditional launch nonce/JTI/claims digest, exact source access kind/
versioned Secret Reference/descriptor, and stable non-secret response-contract
digest; points the Attempt at that Claim; changes Attempt/Activity to
`CLAIMED`/`ACTIVE`; and delivers the offer outbox. Claim is exactly one durable
compare-and-swap, not a Redis lock. Bearers, source credentials, and brokered
archive URLs/bytes are rematerialized and never stored in the Claim.

Repeating the request from the same worker session with the same
`attempt_claim_id` and canonical request digest returns the same durable Claim
and non-secret response contract. Source material and the launch capability are
rematerialized only while controller time is strictly before the execution
deadline. During the later Result-auth grace the controller may rematerialize
only a bearer for the exact unchanged pinned signed claims, including the
original operations claim; server-side endpoint policy makes those claims
effective only for Result reconciliation. At/after auth expiry it rematerializes
no capability. Any equivalent bearer remains bound to the original identity and
deadline. Rematerialization uses the exact original canonical claims, JTI,
issued-at, expiry, key ID, and algorithm and reproduces the deterministic
Ed25519 signature bytes; only an outer transport serialization may be
equivalent rather than byte-identical. Same-key different content returns
`409 IDEMPOTENCY_CONFLICT`; another key or session after claim receives
`409 ATTEMPT_ALREADY_CLAIMED`. If a recovery transaction has
superseded the generation, every claimant receives `409 ATTEMPT_STALE`.

### Response

The success body is `orcest.attempt-claim-result/1` and includes:

```json
{
  "protocol": "orcest.attempt-claim-result/1",
  "attempt_claim_id": "1cbd0ee4-5fc9-4d1d-96e3-05b424209aeb",
  "attempt_id": "uuid",
  "activity_id": "uuid",
  "run_id": "uuid",
  "generation": 3,
  "specification_generation": 2,
  "snapshot_id": "uuid",
  "kind": "BUILD",
  "claimed_at_ms": 1787756400000,
  "execution_deadline_ms": 1787778000000,
  "capability_auth_expires_at_ms": 1787864400000,
  "capability_key_registry_revision": 42,
  "attempt_capability_jti": "uuid",
  "attempt_capability_digest": "sha256:64-lowercase-hex",
  "attempt_capability_signing_key_id": "uuid",
  "attempt_capability_signature_algorithm": "ED25519",
  "attempt_capability": "sensitive-bearer-value",
  "launch": {
    "launch_nonce_id": "uuid",
    "launch_capability_jti": "uuid",
    "launch_capability_digest": "sha256:64-lowercase-hex",
    "launch_capability_signing_key_id": "uuid",
    "launch_capability_signature_algorithm": "ED25519",
    "launch_capability": "sensitive-one-shot-bearer-value",
    "attestation_url": "/api/v1/attempts/uuid/launch-attestations"
  },
  "workflow_hash": "sha256:...",
  "policy_hash": "sha256:...",
  "generation_input_hash": "sha256:...",
  "snapshot_hash": "sha256:...",
  "execution": {
    "execution_profile_id": "codex-default",
    "worker_profile": "codex",
    "provider": "codex",
    "model": "configured-model",
    "provider_account_ref": "provider/account/non-secret-id",
    "provider_family": "openai",
    "model_family": "codex-family",
    "classification_revision": "registry-revision-id"
  },
  "instructions": {
    "format": "markdown",
    "text": "compiled instructions from the pinned trusted workflow"
  },
  "source": {
    "source_access_kind": "SCOPED_CREDENTIAL",
    "repository_id": "forge-neutral-id",
    "clone_url": "https://forge.example/owner/repo.git",
    "brokered_archive": null,
    "base_commit": {"object_format": "sha1", "oid": "40-hex-oid"},
    "parent_candidate_id": null,
    "parent_commit": {"object_format": "sha1", "oid": "40-hex-oid"},
    "forge_observation_id": null,
    "change_request_head_observation_id": null,
    "observed_change_request_head": null
  },
  "candidate_input": null,
  "review_slot": null,
  "verification_profile_id": null,
  "verification_commands": [],
  "credentials": {
    "source_read": {
      "secret_id": "uuid",
      "version": 7,
      "expires_at_ms": 1787778000000,
      "material_b64": "sensitive-base64"
    },
    "provider": null
  },
  "limits": {
    "max_bundle_bytes": 268435456,
    "max_result_bytes": 1048576,
    "max_output_bytes": 16777216
  }
}
```

Optional fields are present as JSON `null` or empty arrays rather than omitted
when their absence affects Activity-kind dispatch. `candidate_input`, when
present, contains the exact `candidate_id`, verified `{object_format, oid}`
commit, canonical `sha256:<hex>` bundle digest,
and an Attempt-scoped download URL. For a `VERIFY` Activity,
`verification_profile_id` is the literal `default` and
`verification_commands` is its frozen ordered command list; all other Activity
kinds receive null and an empty list. `review_slot` and verification commands
use the schemas in
[Review and consensus](review-and-consensus.md).

On Claim replay, a sensitive bearer/material slot is JSON null once that exact
authority is no longer currently valid: source and launch material end at the
execution deadline (and launch material also ends when consumed), and the
Attempt bearer ends at capability-auth expiry. During Result-auth grace only
the Attempt bearer for the exact original signed claims may be rematerialized;
server-side policy enforces Result-only effective use without changing them.
Non-secret identities, descriptors, deadlines, and claims digests remain in the
response contract; their digest never depends on the response-only material.

`attempt_claim_id` is the exact caller UUID from the accepted request. The
durable `AttemptClaim.response_contract_digest` covers it and every non-secret
field of this contract, including the internal mapping from request `outbox_id`
to durable `offer_outbox_id`; response-only bearer/secret bytes and reminted
access locators are excluded.

For a `REVIEW` Activity, `review_slot` is this closed tagged-union variant:

```json
{
  "kind": "REVIEW",
  "panel_round": 1,
  "reviewer_slot": "correctness",
  "role": "correctness",
  "context_digest": "sha256:64-lowercase-hex",
  "subject_refs": ["snapshot:overall", "plan:requirement:r1"]
}
```

For an `ADJUDICATE` Activity, it is this variant:

```json
{
  "kind": "ADJUDICATE",
  "panel_round": 1,
  "adjudication_round": 1,
  "adjudicator_slot": "default",
  "role": "adjudicator",
  "context_digest": "sha256:64-lowercase-hex",
  "subject_refs": ["snapshot:overall", "plan:requirement:r1"],
  "disputed_finding_ids": ["uuid"]
}
```

`panel_round` is positive. The `REVIEW` variant binds the exact frozen
reviewer slot, role, immutable context, and nonempty ordered subject list and
contains no adjudication fields.
The `ADJUDICATE` variant binds a nonempty, duplicate-free list of normalized
Finding UUIDs in ascending UTF-8 byte order; in v1 its `adjudication_round` is
the literal `1`, its `adjudicator_slot` is the literal `default`, and its role
is the literal `adjudicator`. `subject_refs` is the controller-frozen nonempty
ordered list for both variants: literal `snapshot:overall` first, followed by
exactly one `plan:requirement:<requirement_key>` for every accepted Plan
requirement in canonical requirement order. V1 permits no role-added or
repository-added subject. Its canonical array digest is bound into both the
Assignment and `context_digest`.
`context_digest` is the controller-computed SHA-256 digest of the immutable
role-specific review or dispute context. Every other Activity kind receives
`review_slot: null`.

The controller derives this object from the durable Activity Review Assignment,
its ordered subject-membership rows, and, for adjudication, its ordered
Finding-membership rows; the worker cannot choose or modify any member. A
Review or Adjudication Receipt
MUST repeat the applicable fields in its receipt schema exactly. In particular,
a Review Receipt repeats its panel, reviewer slot, role, and context digest; an
Adjudication Receipt repeats its panel, adjudication round, adjudicator slot,
and context digest, and its dispositions cover exactly the assigned disputed
Finding IDs. Review assessments cover exactly `subject_refs` in that order;
the controller canonicalizes assessments into that order, and unknown,
duplicate, or missing subjects are invalid. The controller supplies the
adjudicator role as trusted assignment
metadata rather than a receipt field. A mismatch is an invalid Receipt, not a
new assignment or dispute. Independent Review contexts exclude other Review
outputs; Adjudication contexts include only the frozen original dispute
evidence permitted by the consensus contract and exclude earlier adjudicator
output.

For `PR_REMEDIATE`, `source.forge_observation_id`,
`source.change_request_head_observation_id`, and
`source.observed_change_request_head` are non-null and exactly match the
immutable Activity input; the causal and head observations normally match, and
the head uses the canonical `{object_format, oid}` shape. A post-link `REBASE`
may instead bind a causal `BASE_HEAD` in `forge_observation_id` while the
separate head-observation field and normalized head fence the current Change
Request. Other Activity kinds receive null unless their lifecycle contract
explicitly requires those bindings. The worker cannot fetch or infer a newer
head for workflow use.

The claim succeeds only when `execution.worker_profile` exactly matches the
durable Attempt assignment, the Redis stream suffix, and the authenticated
worker's claimed Worker Profile. `execution.execution_profile_id`, `provider`,
`model`, and `provider_account_ref` are the controller-resolved immutable
assignment. `provider_family`, `model_family`, and `classification_revision`
are its controller-frozen independence classification; the worker cannot
substitute any of them. For `VERIFY`, `execution_profile_id`, `provider`,
`model`, `provider_account_ref`, both family fields,
`classification_revision`, `launch`, and `credentials.provider` are null while
`worker_profile` names the
server-selected credential-free verification runner. The Verification Profile
remains the separate literal `verification_profile_id: "default"`.

For every model-backed kind, `launch` is non-null and
`credentials.provider` is null in the claim response. The launch nonce and
bearer are one-shot, bound to this Attempt/session/deadline, and marked
sensitive. They grant only Launch Attestation submission, not Result,
Candidate, forge, or provider access.

`launch.launch_capability_digest` is controller-computed as:

```text
"sha256:" + lowercase_hex(SHA256(
  ascii("orcest-launch-capability-claims-v1") || 0x00 ||
  uint64_be(byte_length(canonical_claims_json)) || canonical_claims_json
))
```

The canonical JSON contains exactly protocol `orcest.launch-capability/1`,
launch capability JTI, Attempt/Activity/generation, worker/session, launch
nonce, runner principal and registration revision, issued-at time, execution
deadline, launch-attestation endpoint audience, and the exact capability
signing-key ID and signature algorithm. Bearer/signature bytes are excluded, so
an equivalent remint retains the same digest. The Launch Attestation copies the
digest, signer ID, and algorithm exactly. The controller resolves that durable
public verifier by `kid`; an untrusted token header cannot select a different
algorithm or key.

`credentials.source_read.material_b64` is the only raw Secret Store value in a
model-backed claim response while source authority is still valid. It is
response-only, MUST NOT be persisted by the
worker outside its ephemeral bootstrap credential context, and MUST be scrubbed
as described below. Attempt Claim replay before the execution deadline
rematerializes only the exact frozen source Secret version/access descriptor;
it never follows a mutable current
Secret Reference. The source contract's `source_access_kind` exactly copies
the durable Attempt Claim and has one of these variants:

- `SCOPED_CREDENTIAL` requires a non-null `clone_url`, a null
  `brokered_archive`, and an Attempt-scoped `credentials.source_read`. That
  credential authorizes fetch of only the claimed repository, MUST be
  read-only, and has a TTL ending at `expires_at_ms`, which MUST be no later
  than `execution_deadline_ms`. If the forge cannot mint a credential with
  that scope and lifetime, the controller MUST use the brokered mode instead
  of exposing a standing controller credential.
- `BROKERED_ARCHIVE` requires a null `clone_url`, a null
  `credentials.source_read`, and this `brokered_archive` object:

  ```json
  {
    "format": "git-bundle",
    "object_format": "sha1",
    "base_commit": "40-hex-oid",
    "digest": "sha256:64-lowercase-hex",
    "bytes": 123456,
    "expires_at_ms": 1787778000000,
    "download_url": "/api/v1/attempts/uuid/source"
  }
  ```

  The Attempt capability authorizes that URL only. The controller returns a
  complete `application/x-git-bundle` response with `Content-Length` and a
  standard `Digest` header. The bundle MUST contain the exact pinned base and
  the Git objects required for the Activity; the worker verifies the declared
  digest, object format, and base before use. `expires_at_ms` MUST be no later
  than `execution_deadline_ms`.

The controller MUST reject a claim response that populates both modes or
neither mode. Neither mode supplies a publication, branch-push, issue-write,
or Change-Request credential. The worker MUST configure no push URL and no
credential helper that grants forge-write access. A brokered source download
MUST NOT redirect outside the authenticated controller. Source access expires
with the Attempt and cannot be reused by a later generation.

The source-materialization phase is a trusted worker bootstrap step, separate
from model execution and Candidate commands. The registered runner shim first
allocates a globally fresh `workspace_instance_id` and empty isolated workspace,
then materializes and verifies the exact source directly into that workspace;
copying or resuming an existing workspace is forbidden. Immediately afterward,
the worker MUST remove the source credential
from memory, environment, URL/config, helper, and temporary files before it
starts a model invocation or any repository/Candidate-supplied command. It
MUST also remove any fetch URL carrying authority and configure no push remote.
Provider material is injected only into the isolated model-client credential
channel after accepted Launch Attestation; it is not inherited by repository/
Candidate command processes and is removed before Candidate packaging. A
`VERIFY` Attempt receives no provider credential, and every verification
command runs credential-free.

### Launch Attestation and provider materialization

After exact source materialization and credential scrubbing in the already
identified fresh workspace, the registered trusted runner shim allocates fresh
conversation/context and model-invocation IDs. Before starting the model it
attests all three causal identities and calls:

```http
POST /api/v1/attempts/{attempt_id}/launch-attestations
Authorization: Bearer <one-shot-launch-capability>
```

with a signed body:

```json
{
  "protocol_version": "orcest.launch-attestation/1",
  "launch_attestation_id": "uuid",
  "attempt_id": "uuid",
  "activity_id": "uuid",
  "attempt_generation": 3,
  "worker_id": "orcest-worker-10002",
  "worker_session_id": "uuid",
  "pool_manager_id": "registered-pool-manager-id",
  "runner_principal_id": "registered-runner-shim-id",
  "runner_image_digest": "sha256:64-lowercase-hex",
  "runner_registration_revision": "immutable-registration-revision",
  "launch_nonce_id": "uuid",
  "launch_capability_digest": "sha256:64-lowercase-hex",
  "launch_capability_signing_key_id": "uuid",
  "launch_capability_signature_algorithm": "ED25519",
  "workspace_instance_id": "uuid",
  "context_instance_id": "uuid",
  "invocation_instance_id": "uuid",
  "workspace_parent_id": null,
  "context_parent_id": null,
  "invocation_parent_id": null,
  "fresh_workspace": true,
  "fresh_context": true,
  "fresh_invocation": true,
  "prepared_at_ms": 1787756401000,
  "attested_at_ms": 1787756402000,
  "runner_signing_key_id": "registered-key-revision",
  "runner_signature_algorithm": "code-owned-algorithm",
  "attestation_digest": "sha256:64-lowercase-hex",
  "signature": "base64-signature"
}
```

`attestation_digest` covers the normalized protocol and every immutable body
field except `signature`. The launch-capability signer ID and algorithm must
equal its Attempt Claim and durable public verifier. The authenticated runner
principal, image, runner signing key/algorithm, registration revision, and pool binding MUST equal the installed
Snapshot `POLICY_JSON` mapping for the claimed execution/worker profile. The
controller additionally requires the exact current `CLAIMED` Attempt/session,
nonce and capability digest, an unconsumed one-shot capability, globally unused
attestation/workspace/context/invocation IDs, null parent IDs, all three
freshness flags true, a valid signature, and controller time strictly before
the execution deadline.

Acceptance atomically inserts the immutable Attestation, consumes the launch
capability, and links it to the Attempt before returning this sensitive body:

```json
{
  "protocol": "orcest.launch-accepted/1",
  "launch_attestation_id": "uuid",
  "attempt_id": "uuid",
  "status": "AVAILABLE",
  "provider": {
    "provider": "exact-provider",
    "model": "exact-model",
    "provider_account_ref": "non-secret-account-ref",
    "secret_id": "uuid",
    "version": 12,
    "material": "sensitive-attempt-scoped-opaque-value"
  }
}
```

Only `status = AVAILABLE` releases the exact pinned Attempt-scoped provider
material, and only to the attested shim while the Attempt remains current
`CLAIMED` and controller time is strictly before its deadline. Material is
excluded from SQLite, Redis, logs, traces, and signature/digest material. An
`AVAILABLE` provider object exposes the exact non-secret `secret_id`/`version`
from the Attempt's frozen provider Secret Reference so the shim can audit which
immutable material it received; it never exposes a path, tag, or mutable-current
lookup. Those two fields are flat members of `provider` exactly as shown;
there is no nested `provider_secret_ref` wire object. Applicable provider
availability evidence must bind that same
provider/account/Secret version; an observation for an older version cannot
authorize materialization. An
identical authenticated `launch_attestation_id` and digest replay always
returns the accepted identities. While current and before deadline it may
rematerialize equivalent pinned provider bytes as `AVAILABLE`; at/after the
deadline or after terminalization it returns HTTP 200 with the same protocol/
identities, `status = EXPIRED`, and `provider = null`. That response proves
acceptance but grants no launch or provider authority. Conflicting ID, nonce,
Attempt, or instance-ID reuse is rejected. A missing, invalid, parented,
resumed, or reused Attestation releases no provider material and creates no
Result or Transition. The claim remains until corrected or recovered by its
deadline/exact-session loss path. Deterministic `VERIFY` skips this endpoint.

The shim starts exactly the attested invocation after acceptance and includes
`launch_attestation_id` in the Attempt Result. Review and Adjudication Receipts
copy that same ID through controller validation. No worker may resume a prior
conversation, attach a parent invocation, or reuse a workspace/context/
invocation ID across Attempts.

After an Attestation has already been accepted, the launch endpoint may use
the consumed or time-expired launch capability only as signature-equality
proof over the exact frozen claims, under the same registered runner/session
and a retained `ACTIVE` or `RETIRED` verifier, to look up that same
`launch_attestation_id`/`attestation_digest` and return the
`EXPIRED`/null-provider replay above. This lookup is not authentication and
cannot accept a new Attestation, rematerialize provider bytes, extend a
deadline, or mutate any workflow row; a `REVOKED` verifier denies it. It is
distinct from post-deadline Attempt-capability authentication, which remains
Result-only.

The controller persists the resolved Execution Profile assignment—including
the exact `(worker_profile, provider, model, provider_account_ref)`—before
dispatch. A worker may reject an unsupported local provider binary or
assignment capability with a typed `INCOMPATIBLE_WORKER` failure, but it MUST
NOT substitute another Execution Profile, Worker Profile, provider, model, or
account itself.

### Candidate download

When the Activity consumes a Candidate, `candidate_input` has this exact
shape:

```json
{
  "candidate_id": "uuid",
  "commit": {"object_format": "sha1", "oid": "40-hex-oid"},
  "bundle_digest": "sha256:64-lowercase-hex",
  "bundle_bytes": 123456,
  "download_url": "/api/v1/candidates/uuid/bundle"
}
```

`GET` on the URL uses the Attempt capability. The controller verifies that the
Candidate is the immutable input of that exact Attempt, then returns
`application/x-git-bundle`, `Content-Length`, and a standard `Digest` header
matching `bundle_digest`. It does not redirect to an origin outside the
authenticated controller. V1 requires a complete response and does not support
ranges.

The worker computes the digest, verifies the bundle and exact commit, and
constructs a fresh read-only input checkout. A mismatch is an
`INTEGRITY_FAILURE`; the worker does not try another Candidate with the same
commit or fetch Candidate bytes from the forge.

## Liveness, control, and deadlines

The claimed state, generation, claim time, fixed execution deadline, and
terminal outcome are durable SQLite state. The execution deadline cannot be
extended by a worker heartbeat.

While running, a worker sends at most one liveness update per configured
interval:

```http
PUT /api/v1/attempts/{attempt_id}/liveness
```

```json
{
  "protocol": "orcest.attempt-liveness/1",
  "activity_id": "uuid",
  "generation": 3,
  "sequence": 18,
  "observed_at_ms": 1787757480000,
  "state": "ACTIVE",
  "progress": {
    "workspace_head": {"object_format": "sha1", "oid": "40-hex-oid"},
    "signal": "TOOL_OUTPUT"
  }
}
```

`state` is one of `STARTING`, `ACTIVE`, `WAITING_PROVIDER`, `VALIDATING`, or
`SUBMITTING`. Progress is operational evidence only and MUST NOT cause a
workflow transition. `sequence` is positive and strictly increasing for the
exact Attempt/worker session. The controller verifies the durable Attempt and
session, compares it with the disposable current high-water mark when present,
then writes the newest sequence and renewable liveness lease to Redis. A lower
or duplicate sequence never rewinds state; Redis loss may forget the high-water
mark and the next authenticated update simply establishes it again. Liveness
has no idempotency key, durable request row, or original-response replay.

The response body is `orcest.attempt-liveness-result/1` with `attempt_id`,
`activity_id`, `generation`, `control` (`CONTINUE` or `CANCEL`), unchanged
`execution_deadline_ms`, and `liveness_recorded`. If SQLite is available but
Redis liveness storage is not, the controller returns `202` with `CONTINUE` and
`liveness_recorded: false`; the worker continues and retries. Redis loss MUST
NOT erase the durable claim or increment the Attempt generation.
Every response is freshly derived from the current durable control state and
disposable lease result. After an ambiguous response the worker sends its next
higher sequence; it does not expect byte-identical replay of an earlier body.

After controller restart, missing Redis liveness alone is not evidence that the
worker died. The controller reconstructs disposable state and allows the
configured restart grace or waits for a positively fenced pool-manager loss
report/fixed deadline before superseding the Attempt.

If a liveness response says `CANCEL`, or returns `ATTEMPT_STALE`, the worker
MUST stop the agent process, make no Candidate submission, and discard the
workspace. A result that races supersession is accepted only if its transaction
wins before the superseding transaction; the single SQLite writer defines that
order.

## Pool-manager capacity report

Capacity is durable controller input, not a Redis lease or a conclusion drawn
from the consumer pending-entry list. A pool manager reports its current
bounded evidence through:

```http
POST /api/v1/capacity-reports
```

The request uses HTTPS, `X-Orcest-Protocol: 1`, and the pool manager's own
bearer or mutually authenticated TLS principal. Controller registration binds
that principal to exact Capacity Pool IDs, Worker Profile IDs, and Worker
Sessions and defines one authoritative v1 pool-manager source for each reported
scope. A Worker Session or Attempt capability cannot call this endpoint, and
repository configuration cannot grant this authority.

```json
{
  "protocol": "orcest.capacity-report/1",
  "idempotency_key": "1cbd0ee4-5fc9-4d1d-96e3-05b424209aeb",
  "report_id": "c7c782e5-021b-47c1-baf7-dac03374a7ee",
  "report_sequence": 42,
  "observed_at_ms": 1787757540000,
  "expires_at_ms": 1787757840000,
  "observations": [
    {
      "scope_kind": "WORKER_SESSION",
      "scope_id": "81633657-fbb5-47a5-a62e-2c6282437395",
      "availability": "AVAILABLE",
      "capacity_pool_id": "default",
      "worker_profile": "codex",
      "available_slots": 1,
      "session_evidence": {
        "worker_id": "orcest-worker-10002",
        "worker_session_id": "81633657-fbb5-47a5-a62e-2c6282437395",
        "state": "SESSION_READY"
      }
    },
    {
      "scope_kind": "WORKER_PROFILE",
      "scope_id": "codex",
      "availability": "AVAILABLE",
      "capacity_pool_id": "default",
      "worker_profile": "codex",
      "available_slots": 2,
      "session_evidence": null
    },
    {
      "scope_kind": "CAPACITY_POOL",
      "scope_id": "default",
      "availability": "AVAILABLE",
      "capacity_pool_id": "default",
      "worker_profile": null,
      "available_slots": 4,
      "session_evidence": null
    }
  ]
}
```

`report_id` is a source idempotency identity and `report_sequence` is a
positive, strictly increasing revision for the authenticated pool-manager
principal. The controller accepts gaps but rejects a previously unseen report
whose sequence is not greater than that principal's last accepted sequence.
`observations` contains `1..serverMaximum` entries, has no duplicate scope, and
is sorted first by the code-owned scope-kind order `WORKER_SESSION`,
`WORKER_PROFILE`, `CAPACITY_POOL`, then by `scope_id`. The stable external
source identity resolved through the durable report ledger for one entry is
`(authenticated_pool_manager_id, report_id, scope_kind, scope_id)`.

Replaying the same `report_id` and `idempotency_key` with the same canonical
body returns the original result, including the same Health Observation IDs
and sequences. Reusing either identity with different content returns
`409 IDEMPOTENCY_CONFLICT`. A fresh higher-sequence report creates a new Health
Observation even when its payload equals the previous report; replay cannot
extend an expired observation. The controller persists `report_sequence` as
the observation's `observed_revision`, derives its `source_kind` as
`CAPACITY_REPORT`, assigns one durable controller `capacity_report_id`, uses
that ID as every entry's `source_id`, and assigns a controller UUID and the
next `health_sequence` within each `(scope_kind, scope_id)`. The ledger plus
each observation's scope resolves the external tuple above. It persists the
validated pool, Worker Profile, available-slot count, and any exact
worker/session evidence as the Health Observation's `subject_bindings`.

The only v1 capacity scope kinds accepted here are `WORKER_SESSION`,
`WORKER_PROFILE`, and `CAPACITY_POOL`; `availability` is `AVAILABLE` or
`UNAVAILABLE`. The controller independently derives the Health Observation
`kind`: `available_slots > 0` requires and produces `AVAILABLE`, while
`available_slots == 0` requires and produces `UNAVAILABLE`. Negative counts,
an enum/count mismatch, or a count above the registered pool/profile ceiling
is rejected. All entries in one report validate and commit atomically.

Scope validation is exact:

- For `WORKER_SESSION`, `scope_id` equals
  `session_evidence.worker_session_id`, `available_slots` is `0` or `1`, and
  the controller verifies the registered worker ID, session, pool, and Worker
  Profile mapping. `AVAILABLE` requires `SESSION_READY`.
  `UNAVAILABLE` permits `SESSION_STOPPED`, `VM_MISSING`, `DRAIN_COMPLETE`, or
  `SESSION_UNREACHABLE`. Reusing a VM or worker ID does not reuse a session.
- For `WORKER_PROFILE`, `scope_id` equals `worker_profile`,
  `session_evidence` is null, and the pool/profile pair is registered to the
  authenticated principal.
- For `CAPACITY_POOL`, `scope_id` equals `capacity_pool_id`, and
  `worker_profile` and `session_evidence` are null.

`observed_at_ms` is bounded non-authoritative evidence time. After completing
authentication and canonical-body validation, the controller samples one
`accepted_at_ms`, freezes the positive server-owned
`configured_max_ttl_ms` in the durable Capacity Report, and uses that acceptance
time as every entry's `effective_at_ms`. `expires_at_ms` is required and MUST
satisfy both `expires_at_ms > accepted_at_ms` and
`expires_at_ms <= accepted_at_ms + configured_max_ttl_ms`; comparison with the
caller-supplied observation time is not an authority check. Expiry changes
reducer eligibility only when the
controller persists the corresponding Timer Fact; neither wall-clock
comparison nor disappearance of a Redis key can synthesize a health change.
Expiry removes the report as current proof and does not imply the opposite
availability.

The response is:

```json
{
  "protocol": "orcest.capacity-report-result/1",
  "capacity_report_id": "uuid",
  "report_id": "c7c782e5-021b-47c1-baf7-dac03374a7ee",
  "report_sequence": 42,
  "replayed": false,
  "health_observations": [
    {
      "health_observation_id": "uuid",
      "scope_kind": "WORKER_SESSION",
      "scope_id": "81633657-fbb5-47a5-a62e-2c6282437395",
      "health_sequence": 12,
      "kind": "AVAILABLE",
      "effective_at_ms": 1787757541000,
      "expires_at_ms": 1787757840000
    },
    {
      "health_observation_id": "uuid",
      "scope_kind": "WORKER_PROFILE",
      "scope_id": "codex",
      "health_sequence": 9,
      "kind": "AVAILABLE",
      "effective_at_ms": 1787757541000,
      "expires_at_ms": 1787757840000
    },
    {
      "health_observation_id": "uuid",
      "scope_kind": "CAPACITY_POOL",
      "scope_id": "default",
      "health_sequence": 5,
      "kind": "AVAILABLE",
      "effective_at_ms": 1787757541000,
      "expires_at_ms": 1787757840000
    }
  ],
  "woken_wait_condition_ids": ["uuid"]
}
```

The response contains exactly one Health Observation result for every request
entry, in the request's canonical scope order. The durable response stores
`replayed = false`; its response digest excludes exactly that transport field.
An identical report replay derives the same body with only `replayed = true`.

In the same SQLite writer transaction, the controller stores the accepted
report identity and Health Observations, evaluates their bound Wait Conditions
in the canonical scope-kind order and then by
`(scope_id, health_sequence, wait_condition_id)`, stores
any resulting Transitions/outbox rows, and stores the response for replay. An
`AVAILABLE` observation only proposes a wake: the
reducer MUST revalidate the latest applicable unexpired ordered observations,
the current Run and Attempt inputs, and compatibility with the exact Execution
Profile assignment `(worker_profile, provider, model, provider_account_ref)`
before it clears a capacity wait or creates an offer. `UNAVAILABLE` can cause a
capacity wait only through the lifecycle reducer. Neither value lowers policy,
cancels a Run, or fences a claimed Attempt. The response lists only Wait
Conditions actually cleared by that atomic reduction. A crash yields either no
accepted report or the complete replayable report, observations, wakes, and
response; startup reduction cannot lose a committed capacity trigger.

Health Observations and their expiry Timer Facts live in SQLite. Redis may
cache current capacity and wake hints, but it is a disposable projection. On
controller startup or Redis reconstruction, the controller first recovers
overdue persisted timers, then rebuilds the cache from the latest applicable
ordered Health Observations; it never infers capacity from Redis leases, stream
entries, or pending consumers. The pool manager extends capacity evidence only
with a fresh `report_id` and higher `report_sequence`. Failure to receive a
refresh leaves capacity unknown after expiry; it does not fabricate
`AVAILABLE`, `UNAVAILABLE`, or worker loss.

A `WORKER_SESSION/UNAVAILABLE` capacity report alone cannot supersede or fence
a claimed Attempt. Positively stopping or isolating that exact session uses the
worker-loss endpoint below, which supplies the authoritative Attempt binding.

## Pool-manager worker-loss report

The pool manager uses its own authenticated principal and sends:

```http
POST /api/v1/workers/{worker_id}/losses
```

```json
{
  "protocol": "orcest.worker-loss/1",
  "idempotency_key": "uuid",
  "worker_session_id": "uuid",
  "attempt_id": "uuid",
  "activity_id": "uuid",
  "generation": 3,
  "observed_at_ms": 1787757540000,
  "reason": "VM_DESTROYED"
}
```

Reasons are `VM_DESTROYED`, `VM_MISSING`, `CEILING_TIMEOUT`, or
`OPERATOR_DRAIN`. A report whose worker session, Attempt, or generation does
not match the current durable claim is accepted only as a durable `STALE`
Worker Loss Report replay record. A matching report atomically records an
`ACCEPTED` Worker Loss Report, a `WORKER_SESSION/LOST` Health Observation with
`source_kind = WORKER_LOSS_REPORT` and
`source_id = worker_loss_report_id`, and the `WORKER_LOST` Attempt Terminal
Fact sourced by that Health Observation; the reducer consumes the terminal
fact and appends the corresponding `WORKER_LOST` Recovery Evidence sourced by
that Terminal Fact. It does not directly create a replacement Attempt and is
not a worker Result.

That same transaction terminalizes the exact current Attempt as
`state = FAILED` with `terminal_reason = WORKER_LOST` before reducing the
Terminal Fact. It atomically returns the Activity from `ACTIVE` to `PLANNED`
before the Run enters recovery. The reducer may later offer a higher Attempt
generation, select another configured assignment, or wait for capacity, but
only through the lifecycle's `PLANNED -> READY` transition. No Attempt Result
or Receipt is fabricated.
An already terminal or mismatched Attempt yields the `STALE` report path and
cannot have its state or reason rewritten.

`OPERATOR_DRAIN` is authoritative only after the pool manager has positively
stopped or isolated that exact worker session. Merely requesting a drain is a
capacity observation and cannot fence a live Attempt.

A boot failure before an authenticated worker session claims an Attempt is a
fleet capacity observation, not a worker-loss report. The `OFFERED` Attempt
remains durable until another worker claims it or its claim deadline recovers
it.

After authentication and pool/session authorization, a report whose exact
`attempt_id`/`activity_id`/`generation` triple has no durable Attempt returns
`404 ATTEMPT_UNKNOWN` and creates no Worker Loss Report. `STALE` is a durable
idempotent response only when that Attempt triple exists but is no longer the
current matching claimed session/generation.

The pool manager MUST NOT infer success, publish a synthetic successful or
failed receipt, or ACK workflow authority on the worker's behalf. It may retry
the same report until the idempotent response is durable.

Legacy PEL reaping is not a v1 loss mechanism. A legacy worker or reaper MUST
NOT read, claim, ACK, delete, or republish an entry in a
`tasks:activity:v1:*` stream/group and MUST NOT publish a synthetic v1 Result.
Redis ACLs and distinct pool/template registrations enforce that boundary.
Pending-entry age, consumer disappearance, `XCLAIM`, or `XAUTOCLAIM` alone
cannot alter a v1 Attempt; only the authenticated, exact-session loss request
above can produce `FAILED/WORKER_LOST`.

The response is `orcest.worker-loss-result/1` with
`worker_loss_report_id`, the exact Attempt triple, `accepted`, `stale`,
`replayed`, and nullable `health_observation_id` and
`attempt_terminal_fact_id`. The two resulting IDs are non-null exactly for an
accepted current loss. `stale: true` is a durable successful idempotent no-op,
not permission to affect the replacement worker. Reuse of the pool manager's
idempotency key with a different canonical body is
`409 IDEMPOTENCY_CONFLICT`. The durable response stores `replayed = false` and
its response digest excludes exactly that transport projection; an identical
replay returns the same body with only `replayed = true`.

## Candidate upload and admission

Only an Activity kind declared by the code-owned workflow as
Candidate-producing may attach a Candidate. Review, verification,
adjudication, diagnosis, and planning Activities cannot mutate or replace the
Candidate they inspect.

### Create an upload

```http
POST /api/v1/attempts/{attempt_id}/candidate-uploads
```

```json
{
  "protocol": "orcest.candidate-upload-create/1",
  "idempotency_key": "uuid",
  "activity_id": "uuid",
  "generation": 3,
  "media_type": "application/x-git-bundle",
  "declared_bytes": 123456,
  "declared_digest": "sha256:64-lowercase-hex",
  "proposed_tip": {"object_format": "sha1", "oid": "40-hex-oid"}
}
```

After durably inserting a `RECEIVING` upload row, the controller returns:

```json
{
  "protocol": "orcest.candidate-upload-create-result/1",
  "upload_id": "uuid",
  "state": "RECEIVING",
  "upload_url": "/api/v1/candidate-uploads/uuid/content",
  "expires_at_ms": 1787778000000
}
```

The expiration is no later than the durable Attempt execution deadline, not its
later capability-authentication expiry. This response does not create a
Candidate.

### Upload bytes

```http
PUT /api/v1/candidate-uploads/{upload_id}/content
Content-Type: application/x-git-bundle
Digest: sha-256=<base64-digest>
```

The request body is the complete raw bundle. V1 does not support partial or
ranged upload resumption. After an ambiguous response the worker may repeat the
complete body while the upload is unexpired: identical bytes return the existing
state, while different bytes for the same upload return
`409 IDEMPOTENCY_CONFLICT`. Once expiry wins, the closed 410 response below is
derived before the body can create any upload authority.

The worker creates exactly one advertised ref named
`refs/orcest/candidate`. The controller validates in a quarantined object
database before acceptance:

- declared and computed size/digest agree and are within pinned limits;
- `git bundle verify` and `git fsck` succeed;
- there is exactly one advertised ref and it resolves to a commit;
- the commit equals `proposed_tip` and is descended from the expected pinned
  parent commit;
- prerequisite and object-count/unpacked-size limits are satisfied;
- no object is installed into the trusted Candidate store before validation;
  and
- the bundle and commit are not required to execute hooks or candidate-owned
  code during validation.

Validation failure destroys or quarantines the staged bytes and returns a
typed non-secret error. It never mutates an existing Candidate.

After validation and incoming-file fsync, the controller commits the upload as
`VALIDATED` and returns `orcest.candidate-upload-result/1` with `upload_id`,
`state`, controller-derived `computed_digest`, `computed_bytes`, and
`verified_tip` `{object_format, oid}`. `VALIDATED` is still staged upload state,
not Candidate authority. The validation transaction requires controller time
strictly before the durable upload `expires_at_ms`; equality or later
atomically changes the unused upload to `EXPIRED` and returns expiry instead
of `VALIDATED`. A successful result finalization changes an unexpired upload
through `PROMOTED` to `CONSUMED`; expiry changes an unused upload to `EXPIRED`.

For content `PUT`, expiry is a deterministic terminal response. The writer
CASes an otherwise unused upload to `EXPIRED` and returns HTTP `410` with this
exact closed body:

```json
{
  "protocol": "orcest.candidate-upload-expired/1",
  "upload_id": "uuid",
  "state": "EXPIRED",
  "code": "UPLOAD_EXPIRED",
  "expires_at_ms": 1787778000000
}
```

Any later content PUT against that expired upload returns the same status/body;
it cannot validate, promote, or extend the upload. The Result endpoint uses
this exact HTTP status/body when its named upload expires before first Result
finalization and stores it durably in that Result Request.

### Accept a result and Candidate

Candidate admission is completed by the Attempt result transaction, not by the
byte upload. A successful Candidate-producing result names the `upload_id`.
Under the shared storage-mutation lock, the controller first installs and
fsyncs the verified artifact. A first SQLite transaction rechecks the upload
and Attempt binding and controller time strictly before both the durable upload
expiry and execution deadline, inserts or verifies the immutable Artifact
Object, and commits the upload as `PROMOTED` with its final digest, storage key,
tip, and promotion time. A second SQLite transaction repeats both strict
deadline guards, requires that exact `PROMOTED` upload, inserts or resolves the
Candidate identity, changes the upload to `CONSUMED`, accepts the typed Result,
and records the workflow fact consumed by the reducer. At equality or later,
the serialized writer instead changes an unused `VALIDATED` or `PROMOTED`
upload to `EXPIRED`; for `PROMOTED` it clears the live Artifact Object/storage
reference atomically, retains the promotion time as audit, and leaves the
object for orphan grace. No Candidate or Attempt Result is inserted. A crash
between transactions leaves a durable,
verifiable `PROMOTED` boundary that reconciliation can resume only through the
same full Result and upload-expiry guards; it is not Candidate authority.
SQLite MUST never commit `PROMOTED` before the file and directories are fsynced,
or a live Candidate reference before `PROMOTED` commits.

`candidate_id` is the durable object identity. Within one Run and specification
generation, `(object_format, oid)` is the unique content identity used with
`candidate_id` by receipts and publication. Uploading a different bundle
representation for an already admitted commit resolves to that Candidate only
after the new producing Attempt passes the normal admission and generation
checks; the bundle digest identifies stored bytes and grants no reuse authority.
A different verified commit creates a new Candidate and, when selected current,
invalidates prior-Candidate receipts as specified in
[Review and consensus](review-and-consensus.md).

Resolving to the already-current same-commit Candidate is an accepted artifact
admission fact, not proof of implementation progress. The reducer does not
increment Candidate generation or satisfy a remediation Activity with it; it
records the non-progress fingerprint and advances autonomous
diagnosis/replanning policy.

## Attempt result

```http
POST /api/v1/attempts/{attempt_id}/result
```

```json
{
  "protocol": "orcest.attempt-result/1",
  "idempotency_key": "uuid",
  "attempt_id": "uuid",
  "activity_id": "uuid",
  "generation": 3,
  "launch_attestation_id": "uuid-or-null-for-verify",
  "outcome": "SUCCEEDED",
  "candidate_upload_id": "uuid-or-null",
  "receipt": null,
  "structured_output": null,
  "failure": null,
  "summary": "non-authoritative, bounded text"
}
```

`outcome` is one of:

- `SUCCEEDED`: the Activity produced its required typed output and the
  controller confirms that it semantically fills the Activity. A `BUILD` or
  remediation Activity requires a new valid Candidate upload; verification,
  review, and adjudication require the corresponding receipt. An upload that
  resolves to the already-current Candidate is retained only as
  `REPEATED_NON_PROGRESS` audit evidence and does not count as successful
  Activity output.
- `FAILED_RETRYABLE`: execution did not produce the required output and the
  same strategy may be attempted again. `failure` is required.
- `FAILED_PERMANENT`: the selected Attempt strategy cannot produce a valid
  output. `failure` is required. “Permanent” applies only to that strategy;
  the reducer still replaces, diagnoses, replans, adjudicates, or waits.
- `ABSTAINED`: a review or adjudication Attempt produced a schema-valid
  non-filling Receipt: Review `ABSTAIN`, adjudicator abstention, or an
  Adjudication Receipt containing `INCONCLUSIVE`. The Attempt becomes
  `ABSTAINED`, the Activity is atomically returned to `PLANNED`, and the
  Result transaction appends typed Recovery Evidence sourced by that exact
  Attempt Result before the Run enters `RECOVERING`. Only reduction of that evidence may select
  `RETRY_EXECUTION`, `REPLACE_CAPACITY`, `WAIT_EVIDENCE`, or `WAIT_CAPACITY`
  and then offer a higher generation or create a Wait. This never counts as
  approval or blocker resolution.

For `SUCCEEDED`, the required typed output depends on Activity kind. `BUILD`,
`REMEDIATE`, `REBASE`, and `PR_REMEDIATE` name a validated Candidate upload;
`PLAN`, `DIAGNOSE`, and `REPLAN` carry bounded normalized `structured_output`
that passes the lifecycle's exact versioned
[planning contract](workflow-lifecycle.md#planning-contract) schemas;
`VERIFY` names a decisive `PASS` or `FAIL` Verification Receipt;
`REVIEW` names an `APPROVE` or `BLOCK` Review Receipt; and `ADJUDICATE` names a
decisive, slot-filling Adjudication Receipt. Controller-class `IMPORT`,
`PUBLISH`, `CLOSE_PUBLICATION`, `CLOSE_REDUNDANT_PUBLICATION`,
`REPAIR_RUN_MARKER`, and `RECONCILE` Activities never accept a worker Result.
`FAILED_RETRYABLE` and `FAILED_PERMANENT` cannot carry a Candidate. They do not
carry a Receipt except for the sole v1 exception: a `VERIFY`
`FAILED_RETRYABLE` result MUST carry its schema-valid `ERROR` Verification
Receipt and failure class `VERIFICATION_ERROR`. That Attempt becomes `FAILED`,
the Activity is atomically returned to `PLANNED`, and the reducer may offer a
higher generation of the same `VERIFY` Activity only through the lifecycle's
`PLANNED -> READY` offer transition. The controller rejects every other
outcome/output combination before changing Attempt state.

The payload union is exact: successful `PLAN`/`REPLAN` requires
`structured_output` with protocol `orcest.plan/1`; successful `DIAGNOSE`
requires protocol `orcest.diagnosis/1`; Candidate-producing success requires
only `candidate_upload_id`; Verification/Review/Adjudication success or
abstention uses only its typed `receipt`; and ordinary failure uses only its
typed `failure`/summary. The sole failure Receipt exception is the `VERIFY`
case above. Forbidden union members are JSON null. `structured_output` is
bounded, normalized, included in the Result digest, and must reproduce its
schema's output digest. Credential rotation is separate: a Result has no
`credential_rotation_receipt_id` or other rotation binding.

Every model-backed Result requires the exact non-null
`launch_attestation_id` already accepted for its producing Attempt; its
normalized Result digest and any Review/Adjudication Receipt digest bind that
ID. `VERIFY` requires null. A missing, different, reused, or unattested ID is a
4xx fence/schema rejection with no Result, Receipt, or Transition.

A missing required Receipt, malformed Result or Receipt, unknown/duplicate
subject, or invalid kind/outcome/output combination receives a 4xx schema
rejection before Result Request admission. It inserts no Attempt Result,
Receipt, Result Request, terminal fact, Recovery Evidence, or Transition and does not change the
`CLAIMED` Attempt. The same worker may correct and resubmit while the claim and
deadlines remain current; otherwise only the ordinary execution-deadline or
authenticated worker-loss path fences it. Only the schema-valid
`FAILED_RETRYABLE` + `VERIFICATION_ERROR` + Verification `ERROR` combination
enters recovery through `T(ATTEMPT_RESULT, attempt_id)`.

### Failure results

Failure has this schema:

```json
{
  "class": "PROVIDER_RATE_LIMIT",
  "code": "provider-specific-non-secret-code",
  "retry_after_ms": 1787760000000,
  "evidence_refs": ["trace:bounded-reference"]
}
```

The accepted worker-authored failure classes and their exact default mapping
are closed in v1:

| Worker failure class | Allowed Activity/evidence | Required Result outcome | Lifecycle `RecoveryInput` | Default reducer treatment |
| --- | --- | --- | --- | --- |
| `INFRASTRUCTURE` | Any Worker Activity; local sandbox or runner failed after claim | `FAILED_RETRYABLE` | `WORKER_LOST` | The accepted Result terminalizes this Attempt as an ordinary Result failure and appends AttemptResult-sourced `WORKER_LOST` Recovery Evidence; a pool-manager loss report is a separate TerminalFact-sourced path. Controller/Redis delivery errors do not use this class. |
| `PROVIDER_UNAVAILABLE` | Model-backed Worker Activity; provider request failed transiently | `FAILED_RETRYABLE` | `PROVIDER_TRANSIENT` | Reconcile an ambiguous provider response, then retry or select the next allowed provider. |
| `PROVIDER_RATE_LIMIT` | Model-backed Worker Activity; bounded reset evidence when available | `FAILED_RETRYABLE` | `PROVIDER_RATE_LIMIT` | Select the next allowed account/provider or enter `WAITING/RATE_LIMIT` until the persisted wake time. |
| `INCOMPATIBLE_WORKER` | Any Worker Activity; claimed Worker Profile lacks a capability required by its frozen resolved execution assignment | `FAILED_PERMANENT` | `CAPACITY` | Reject this worker strategy, resolve the next allowed Execution Profile assignment, or enter `WAITING/CAPACITY`. |
| `INVALID_AGENT_OUTPUT` | `PLAN`, `BUILD`, `REVIEW`, `REMEDIATE`, `DIAGNOSE`, `REPLAN`, `ADJUDICATE`, `REBASE`, or `PR_REMEDIATE`; agent output cannot satisfy its schema | `FAILED_RETRYABLE` | `INVALID_RESULT` | Apply the pinned schema-repair attempt, then replace execution capacity. |
| `VALIDATION_FAILURE` | Candidate-producing Activity; proposed workspace/bundle cannot pass local pre-upload validation | `FAILED_RETRYABLE` | `INVALID_RESULT` | Retry cleanly, then diagnose repeated identical validation failure. |
| `CREDENTIAL_UNAVAILABLE` | Model-backed Activity; Attempt-scoped provider material is invalid, revoked, or unusable | `FAILED_RETRYABLE` | `CREDENTIAL` | Reconcile rotation/refresh and use the next allowed credential; otherwise `WAITING/SECRET_RECOVERY`. |
| `SOURCE_READ_FAILED` | Any Worker Activity whose frozen source or Candidate input cannot be read after local retry and integrity checks | `FAILED_RETRYABLE` | `SOURCE_READ` | Reconcile the read-only credential or brokered archive, then rematerialize the same frozen input. |
| `VERIFICATION_ERROR` | `VERIFY` only, with the schema-valid `ERROR` Receipt required above | `FAILED_RETRYABLE` | `VERIFICATION_ERROR` | Return the frozen verification Activity to `PLANNED` before recovery; offer a higher Attempt generation only through the lifecycle's `PLANNED -> READY` transition. |
| `BASE_CONFLICT` | `BUILD`, `REMEDIATE`, `REBASE`, or `PR_REMEDIATE`; exact pinned base/parent cannot be applied | `FAILED_PERMANENT` | `BASE_CONFLICT` | Plan deterministic rebase/remediation or refresh the applicable base binding or the Activity's `observed_change_request_head` normalized from `change_request_head_observation_id`. |
| `POLICY_DENIED` | Any Worker Activity; a requested operation is forbidden by the pinned sandbox/server policy | `FAILED_PERMANENT` | `POLICY` | Try a policy-permitted tactic; only code-owned exceptional-boundary evaluation may later pause for a human. |
| `SPECIFICATION_CONFLICT` | `PLAN`, `BUILD`, `REMEDIATE`, `DIAGNOSE`, `REPLAN`, or `PR_REMEDIATE`; structured evidence names incompatible pinned requirements | `FAILED_PERMANENT` | `POLICY` | Independently diagnose and apply declared precedence before exceptional-boundary evaluation. |
| `MISSING_AUTHORITY` | Any Worker Activity; structured evidence identifies an operation outside granted authority | `FAILED_PERMANENT` | `POLICY` | Seek an authorized autonomous alternative or permission refresh before exceptional-boundary evaluation. |
| `INTEGRITY_FAILURE` | Any Worker Activity; structured evidence suspects one exact bound live Candidate Artifact, Workflow Blob, or Secret Version is corrupt or mismatched | `FAILED_PERMANENT` | `INTEGRITY_SUSPECTED` | Fail closed and select `PROBE_INTEGRITY`; never refetch an unbound substitute or let worker testimony create a recovery Wait. Only the reciprocal Health Probe Fact/Observation may confirm `STORAGE` for Candidate/Workflow Blob or `CREDENTIAL` for Secret Version, and only the subsequent Recovery-Evidence transition creates the typed Wait. |

“Model-backed” means every Worker Activity except deterministic `VERIFY`; a
repository cannot make verification provider-dependent by configuration. The
controller rejects a class, outcome, or Activity combination not present in
this table. `retry_after_ms` is an absolute Unix-millisecond time allowed only
for `PROVIDER_RATE_LIMIT`. It does not extend execution authority: the Attempt
deadline still fences acceptance. On acceptance the controller deterministically
clamps the Run's wait to
`min(max(retry_after_ms, accepted_at_ms), accepted_at_ms +
max_provider_rate_limit_wait_ms)`, using the positive server-bounded maximum
frozen in the installed Snapshot policy.

For an accepted failure, `AttemptResult` stores the exact normalized
`failure_class`, bounded stable `failure_code`, conditional
`failure_retry_after_ms`, canonical UTF-8-byte-sorted unique
`failure_evidence_refs` array, and its failure-evidence digest. Non-failure
Results store the closed null/empty counterpart; free-form failure payload is
not retained as an alternate reducer input.

The remaining lifecycle Recovery Inputs are not directly selected by a worker
Result, except that the accepted `INFRASTRUCTURE` failure above maps to the
`WORKER_LOST` Recovery Input through its own ordinary Result transaction. That
worker path terminalizes the Attempt as a Result `FAILED` and appends
`WORKER_LOST` Recovery Evidence sourced by the exact `AttemptResult`; it does
not create a `WORKER_LOST` Attempt Terminal Fact or set the pool-loss
`terminal_reason`. The disjoint controller path is an authenticated,
exact-session pool-manager loss report: it creates the `WORKER_LOST` Attempt
Terminal Fact, sets the Attempt to `FAILED` with `terminal_reason = WORKER_LOST`,
and appends Recovery Evidence sourced by that Terminal Fact. Only that
TerminalFact/`terminal_reason` path is controller-only. `TIMEOUT` comes from a
persistent deadline; `CAPACITY` also comes from eligible
pool observations; `VERIFICATION_FAILURE` comes from a successful `VERIFY`
Result carrying `FAIL`; `REPEATED_NON_PROGRESS` comes from same-commit
Candidate admission; `REVIEW_DISAGREEMENT` comes from the canonical Receipt
set; `FORGE_TRANSIENT` comes from the forge adapter; and confirmed reciprocal
Health Probe Facts produce `STORAGE` for Candidate/Workflow Blob failure or
`CREDENTIAL` for Secret Version failure. `EXTERNAL_DEPENDENCY` is produced
only from a persisted dependency observation, never free-form worker prose.
This list plus the mapping table accounts for every v1 Recovery Input in
[Workflow lifecycle](workflow-lifecycle.md#recovery-inputs).

Failure class is evidence for the deterministic recovery reducer; it is not a
terminality instruction. In particular, the three `POLICY`-mapped classes are
only claims to investigate; the controller independently proves any
exceptional boundary. A worker cannot set `needs-human`. It may report
structured evidence for an exceptional boundary, but only the controller
reducer can emit an allowlisted, resumable `needs-human` reason.

The controller canonicalizes and hashes the semantic result body, excluding
only the transport `idempotency_key` and bearer credential. That
`result_body_digest` is also the required `AttemptResult.result_digest` when
accepted. Capability authentication, schema validation, and semantic-conflict
checks occur before Result Request admission; only a request admitted to one of
the five closed dispositions may claim the global `result_request_id`. In one transaction it
first enforces the durable Controller Mode: `RUNNING`, `INTAKE_PAUSED`,
`DISPATCH_PAUSED`, and `DRAINING` permit first Results and exact replay;
`MAINTENANCE` permits only read-only retrieval of an already-existing exact
Result Request response and creates no ledger or workflow mutation. An unseen
key receives HTTP `503` with this exact five-field body, even when its semantic
`result_body_digest` matches an accepted Result stored under another key:

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
additional field is permitted. If maintenance ends while the capability
remains authentic, the client retries the same key/body normally.
This response is not durably claimed by the key and extends no deadline. It then
verifies the Attempt capability, current generation, worker session, Activity
kind, schema, referenced upload/receipt, and controller time strictly before
the fixed `execution_deadline_ms`; for a Candidate-producing first Result it
also requires controller time strictly before the referenced durable upload's
`expires_at_ms`; accepts at most one result; stores that digest as immutable
`AttemptResult.result_digest`; and emits
the next reducer input/outbox work. Receipt validity rules are defined by
[Review and consensus](review-and-consensus.md).

If a Candidate-producing Result reaches the writer before its execution
deadline but at or after its named upload's durable `expires_at_ms`, the writer
serializes the unused upload to `EXPIRED`, inserts one Result Request with
disposition `UPLOAD_EXPIRED`, and returns HTTP `410` with the exact
`orcest.candidate-upload-expired/1` body above. It inserts no Attempt Result,
Candidate, Receipt, Terminal Fact, Recovery Evidence, or Transition. Exact
key/body/binding replay returns that stored response. The Attempt remains
`CLAIMED`; strictly before its execution deadline the worker may create a new
upload and submit a fresh Result identity/body, but it cannot reuse or extend
the expired upload.

If authentication succeeds strictly before the submitted Attempt's execution
deadline but its durable generation, claim binding, Run binding, or terminal
state is already stale, the endpoint inserts one `STALE_ATTEMPT` Result Request
and returns exact HTTP 409
`{protocol:"orcest.error/1", code:"ATTEMPT_STALE", attempt_id,
current_attempt_generation, retryable:false}`. The ledger stores the closed
reason `GENERATION_SUPERSEDED`, `CLAIM_BINDING_CHANGED`,
`RUN_BINDING_CHANGED`, or `TERMINAL_BEFORE_DEADLINE`; it creates no Result,
Receipt, Candidate, Terminal Fact, Recovery Evidence, or Transition. Same key/
body/bindings replays that response and conflicting reuse is an idempotency
conflict.

A first acceptance observed by the controller at or after `execution_deadline_ms`
but strictly before `capability_auth_expires_at_ms` MUST fail even if the
timeout-recovery transaction has not run. The result
endpoint admits no Candidate or Receipt and atomically inserts a durable Result
Request ledger row (request key, complete canonical body digest,
Attempt/session/capability bindings, deadline proof, disposition, and stored
response) plus its source-unique audit Terminal Fact. If the Attempt is still
the current `CLAIMED` generation, disposition `EXPIRED_CURRENT` fences it
`EXPIRED`, emits the `TIMEOUT` Recovery Input, and returns
`410 EXECUTION_DEADLINE_EXCEEDED`. If an earlier non-Result input already terminalized
it and no Result was accepted, `ALREADY_TERMINAL` retains the request/fact as
audit-only evidence, appends exactly one source-unique same-state
`ATTEMPT_TERMINAL` audit Transition, makes no counter, Recovery Evidence, or
work change, and returns
`409 ATTEMPT_STALE`. Accepted-Result replay/conflict lookup always precedes
this branch. Identical
authenticated replay returns the stored status/body and fact; reuse of the key
with a different body or binding returns `409 IDEMPOTENCY_CONFLICT`. A result
transaction that commits strictly before the deadline wins; timeout recovery
then observes a terminal Attempt.

At or after `capability_auth_expires_at_ms`, capability authentication fails
and the controller inserts no Result Request, Terminal
Fact, Result, Receipt, Candidate, Recovery Evidence, or Transition. The
ordinary Timer Fact sweeper remains responsible for any missing execution-
deadline recovery. During the post-deadline authentication grace, every
Attempt-scoped endpoint other than the two Result reconciliation paths above
is denied.

Controller cancellation is not a worker-authored result outcome. After the
controller durably supersedes the Attempt, a `CANCEL` control response tells
the worker to stop and every subsequent result is stale.

Outside `MAINTENANCE`, an identical semantic replay, even under a different
unused idempotency key,
returns `200` with the original accepted result identity and `replayed: true`,
including after the execution deadline only while controller time is strictly
before `capability_auth_expires_at_ms`. For this endpoint only, a
post-execution-deadline Attempt capability may authenticate an exact replay lookup but can authorize
no new workflow-authority write: the controller verifies its signature and
full Attempt/session binding, finds the already accepted semantic digest, and
returns only the existing result. When the retry uses a different previously
unused idempotency key, the controller inserts one `ACCEPTED` Result Request
with `accepted_result_created = false` pointing to the existing Result; it does not insert another
Result, Receipt, Transition, or reducer input. A different result for an
Attempt with an accepted Result returns `409 RESULT_ALREADY_ACCEPTED` as a
semantic conflict before Result Request admission and creates no new registry
row. If no
Result was accepted, the Result Request late disposition returns the stored
`410 EXECUTION_DEADLINE_EXCEEDED` for `EXPIRED_CURRENT` or `409 ATTEMPT_STALE` for
`ALREADY_TERMINAL`. A schema error leaves the Attempt claim current so
the worker may repair its output before the deadline; schema retry limits and
eventual replacement are lifecycle policy, not weaker schema validation.

The accepted response is `orcest.attempt-result-accepted/1` and returns the
exact `attempt_id`, `activity_id`, `generation`, accepted `outcome`, nullable
controller-assigned `candidate_id` and `receipt_id`, and `replayed`. It contains
no raw secret, upload capability, or agent transcript.

The body `idempotency_key` is the global `result_request_id` across all five
closed Result dispositions: `ACCEPTED`, `UPLOAD_EXPIRED`, `STALE_ATTEMPT`,
`EXPIRED_CURRENT`, and `ALREADY_TERMINAL`. The controller never maintains a separate accepted and
late namespace. Same key plus exact Attempt/session/capability signer/body
bindings returns its stored response; reuse across dispositions or with any
different binding is `409 IDEMPOTENCY_CONFLICT`.

An operational backup barrier MUST NOT make a valid first Result wait past its
fixed execution deadline. Before acquiring that barrier, the controller pauses
new claims and drains until the durable count of `CLAIMED` Attempts is zero;
Result acceptance remains available throughout that pre-barrier drain. The
single writer checks zero claims and the paused mode in the same serialized
boundary that admits the bounded barrier, so no new claim can race it. If any
claim exists, backup is skipped or delayed—never answered by making the worker
retry a current Result. The barrier cannot extend an Attempt deadline, and an
implementation MUST NOT return a backup-specific retryable response for a
current first Result. Exact replay of an already accepted Result retains the
ordinary replay semantics.

After result acceptance the controller records a durable worker-release
outbox intent. The worker may exit after receiving the accepted/replayed
response. If the response is lost, retry is safe; if the worker disappears,
the pool manager reconciles release from controller state. A Redis
`pool:done:*` key is not a workflow durability boundary in v1.

## Credential rotation handoff

Some provider CLIs rotate OAuth material during an Attempt. Rotation is
independent of whether the Activity later succeeds and therefore has its own
endpoint:

```http
POST /api/v1/attempts/{attempt_id}/credential-rotations
Content-Type: application/octet-stream
X-Orcest-Activity-ID: <activity-id>
X-Orcest-Attempt-Generation: <positive-integer>
X-Orcest-Provider-Account: <non-secret-account-ref>
X-Orcest-Prior-Secret-Version: <integer>
X-Orcest-Credential-Rotation-Request-ID: <lowercase-uuid>
```

The body is the exact new credential bytes and MUST bypass ordinary request
body logging, tracing, diagnostics, and error reflection. The Attempt
capability must name the same provider account. First acceptance requires the
exact current `CLAIMED` model-backed Attempt, authenticated worker/session and
capability, its accepted `launch_attestation_id`, matching provider account,
Secret ID and current prior version, and controller time strictly before
`execution_deadline_ms`. The controller validates the credential format
without exposing it and obtains an opaque Secret Store keyed request-
attestation identity proving body equality; raw bytes, an unkeyed body digest,
key, or tag never enter SQLite, logs, or the API.

If the submitted prior is current, one transaction stores an `APPLIED`
Credential Rotation Request and exact response, inserts its reciprocal
`ATTEMPT_ROTATION` Credential Rotation Receipt and Secret Version,
compare-and-swaps the reference, and freezes membership plus durable fanout
intent. If the prior is stale, it stores only a `CAS_LOST` Request and exact
response naming the current non-secret version; it creates no Receipt, Version,
reference mutation, fanout, Result, or Transition and never overwrites newer
material. Once a Result or non-Result terminal fence wins before the execution
deadline, no new rotation authority exists and only an exact existing Request
may replay. At or after `execution_deadline_ms`, the rotation endpoint denies
both first acceptance and replay; the post-deadline capability grace is
Result-only.

The closed `orcest.credential-rotation-result/1` body contains exactly request
ID, disposition, Secret ID, expected prior version, current version, and, only
for `APPLIED`, accepted version plus Receipt ID. `APPLIED` is HTTP 200;
`CAS_LOST` is HTTP 409 and omits the applied-only fields. It contains no
Attempt/account field, replay flag, credential, body verifier, or free-form
error. The exact tagged union is:

```json
{
  "protocol": "orcest.credential-rotation-result/1",
  "credential_rotation_request_id": "lowercase-uuid",
  "disposition": "APPLIED",
  "secret_id": "lowercase-uuid",
  "expected_prior_version": 7,
  "current_version": 8,
  "accepted_version": 8,
  "credential_rotation_receipt_id": "lowercase-uuid"
}
```

```json
{
  "protocol": "orcest.credential-rotation-result/1",
  "credential_rotation_request_id": "lowercase-uuid",
  "disposition": "CAS_LOST",
  "secret_id": "lowercase-uuid",
  "expected_prior_version": 7,
  "current_version": 8
}
```

Retrying after an ambiguous response uses the same request UUID and
canonical non-secret authority plus Secret Store keyed byte-equality proof;
exact replay returns stored status/body, while any binding or byte conflict is
`409 IDEMPOTENCY_CONFLICT`. Attempt Result never names a rotation Receipt.
Orphan Secret Store versions and a crash after reference update are reconciled
as specified by [Persistence and recovery](persistence-and-recovery.md).

## Failure and recovery behavior

| Failure | Required behavior |
| --- | --- |
| Redis loses an unclaimed offer | Outbox reconciliation republishes the same current dispatch intent |
| Redis loses state after claim | Durable claimed Attempt/generation/deadline remain; disposable liveness is rebuilt with restart grace |
| Redis epoch changes after offer or claim | Treat the epoch as diagnostic; accept an otherwise-current claim or Attempt-scoped call under durable identity/fence/deadline checks |
| Offer is delivered twice | Only one worker session wins claim; both entries are eventually ACKed |
| Claim response is lost | Same worker retries the exact Attempt Claim ID/request digest and receives the same durable non-secret contract; only still-valid sensitive fields rematerialize (source/launch strictly before execution deadline, the exact unchanged Attempt-capability claims during auth grace with server-side Result-only enforcement, none at/after auth expiry), and another key/session cannot acquire it |
| Launch Attestation response is lost | Identical authenticated Attestation ID/digest returns the accepted identities; while current/before deadline it returns `AVAILABLE` with equivalent pinned provider material, otherwise `EXPIRED` with provider null; nonce and invocation are not consumed twice |
| Launch Attestation is missing, invalid, parented, resumed, or reuses an ID | Release no provider material; create no Result/Transition; retain the claim until corrected, deadline, or exact-session loss |
| Worker dies before Candidate upload | Authenticated exact-session loss records Attempt `FAILED/WORKER_LOST`, returns the Activity to `PLANNED` before recovery, and the reducer later schedules a higher generation or waits for capacity |
| Worker dies after upload but before result | Staged upload is not a Candidate and is collected after expiry; the Attempt is terminalized, the Activity returns to `PLANNED`, and the reducer retries only through `PLANNED -> READY` |
| Controller dies after artifact fsync but before `PROMOTED` commit | Validated upload plus a possible orphan object remains; reconciliation verifies it and commits `PROMOTED` or leaves it for GC |
| Controller dies after `PROMOTED` but before Candidate/Result commit | Durable promotion remains non-authoritative; retry resumes only while controller time is strictly before both upload and execution deadlines; otherwise the upload expires, its live promoted reference is cleared, and no Candidate/Result is created |
| Upload expiry races validation, promotion, or first Result finalization | The single writer and, for promotion/finalization, storage-mutation lock serialize the race; only a transition committed strictly before expiry may advance, while equality/later changes the unused upload to `EXPIRED` |
| Controller dies after DB result commit | Replay returns the accepted result; reducer/outbox resumes |
| First Result reaches the controller at or after the execution deadline but before capability-auth expiry | If it still fences the current claim, ledger and reject with `410 EXECUTION_DEADLINE_EXCEEDED`; if an earlier non-Result cause already terminalized it, ledger and reject with `409 ATTEMPT_STALE`; admit no output in either case |
| Any Attempt-capability call arrives at or after capability-auth expiry | Reject authentication and insert no request/replay/terminal/workflow ledger; timeout remains the Timer Fact sweeper's responsibility |
| Accepted Result response is lost and replay arrives after the deadline but before auth expiry | Return the exact prior accepted Result with `replayed: true`; exact same key reads its Result Request, while a new unused key may add only an `ACCEPTED` Result Request with `accepted_result_created=false`, never a new workflow Result or Transition |
| Old generation finishes late | Result and upload finalization are rejected without workflow mutation |
| Pinned credential is unavailable before claim commits | Claim fails closed without an owner; reducer waits or uses an allowed replacement |
| Credential materialization fails after claim commits | Durable Attempt Claim remains; only the same session/Claim ID/request digest retries exact-version materialization, or the fixed deadline/loss fence recovers it |
| Credential rotation APPLIED commit or response is lost | Exact Request, reciprocal Receipt/Version/reference/fanout intent, opaque keyed replay proof, and stored response survive; same request replays without a second version only strictly before execution deadline, and at/after it rotation authentication is denied while fanout recovery continues |
| Credential rotation loses its prior-version CAS | Store only `CAS_LOST` request/409 response/current version; create no Receipt/Version/fanout/Result/Transition, quarantine losing bytes under lock, replay exactly only before execution deadline, and deny rotation auth at/after it |
| Liveness Redis write fails or response is lost | Worker continues within fixed deadline and sends a higher sequence; there is no durable response ledger, generation change, or acceptance-rule change |
| Pool manager repeats a loss report | Idempotent matching report is returned; a stale worker session cannot affect a replacement |
| Backup is requested while a worker is claimed or submitting near deadline | Backup remains in pre-barrier drain or is skipped; Result acceptance stays available and the barrier begins only after durable claimed count reaches zero |
| Verification produces typed `ERROR` | Accept only the `FAILED_RETRYABLE/VERIFICATION_ERROR` combination with its Receipt; return the Activity to `PLANNED` before recovery, then retry only through a later `PLANNED -> READY` offer |
| Result or required Receipt is missing/malformed | Return 4xx and insert no Result/Receipt/fact/Transition; keep the exact claim unchanged so the worker may correct it until deadline/loss; never infer success, recovery, or `needs-human` |

Execution remains at least once. Result acceptance is at most once per current
generation. Neither the queue, heartbeat, pool-manager report, process exit
code, nor agent prose can independently mark an Activity successful.

## Security requirements

- Worker claim and Launch Attestation responses MUST be marked sensitive and
  omitted from HTTP access bodies, error trackers, and traces.
- Secret-bearing environment variables and provider auth files MUST exist only
  in the isolated worker credential context and be removed during cleanup.
- A source-read credential MUST be repository-read-only, Attempt-scoped, and
  expire no later than the execution deadline; it MUST be scrubbed before any
  model or Candidate command starts. Provider material MUST be isolated from
  Candidate command processes and scrubbed before packaging.
- Every model-backed workspace/context/invocation MUST be fresh under the
  attested runner shim. Review and verification workspaces MUST be read-only
  with respect to the Candidate store and forge.
- Candidate bundle parsing MUST enforce byte, object-count, unpacked-size,
  path, and time limits to prevent resource-exhaustion attacks.
- Git hooks, filters, credential helpers, repository-owned executables, and
  candidate-modified `.orcest` policy MUST NOT run during controller-side
  admission.
- Repository commands execute only inside the worker isolation boundary with
  no controller or forge-write credential.
- Results and receipts MUST have bounded strings, arrays, annotations, and
  evidence references. Human-facing projections escape untrusted text.
- A worker session or Attempt capability compromise is contained to its
  declared audience and deadline; every cross-Run access is denied even if an
  object identifier is guessed.

## Conformance tests

An implementation is not conformant until automated tests demonstrate:

- a queue entry contains none of the source/provider/bearer secrets returned
  by claim;
- an incompatible legacy worker never consumes a v1 stream;
- a failure-injection legacy worker/reaper principal cannot read, `XCLAIM`,
  `XAUTOCLAIM`, ACK, delete, or republish a v1 stream entry and cannot call the
  v1 Result endpoint to synthesize success/failure; none of those attempts
  creates an Attempt Result, Receipt, terminal fact, or Transition;
- every selected server Execution Profile resolves before dispatch to one
  persisted exact Worker Profile/provider/model/provider-account assignment,
  plus frozen provider/model-family classification and classification
  revision, and a worker cannot substitute any element;
- a model-backed claim contains no provider material and exposes only its
  one-shot nonce/capability plus its domain-separated normalized-claims digest;
  equivalent bearer remint preserves the digest, and a valid signed fresh/no-parent Launch Attestation
  consumes it exactly once before returning pinned provider material, while a
  reused nonce/workspace/context/invocation, bad signature/image/principal, or
  parent/resume field returns no provider material and changes no workflow row;
- exact accepted Launch replay before the deadline returns the same identities
  and `AVAILABLE` material only while current; consumed/time-expired launch-
  capability lookup after deadline or terminalization uses only
  signature-equality proof for the same retained Attestation under the same
  runner/session and an ACTIVE/RETIRED verifier, returns only `EXPIRED` with
  null provider, and cannot authenticate, insert, or mutate; REVOKED denies;
- a model-backed Result or Review/Adjudication Receipt without the exact
  accepted Launch Attestation is rejected, and deterministic VERIFY accepts no
  launch object at all;
- `REVIEW` and `ADJUDICATE` claims carry only their exact tagged `review_slot`
  variant reconstructed from the durable Activity Review Assignment and
  ordered membership, and a Receipt that changes a round, slot, role, context
  digest, or disputed Finding coverage is rejected;
- two simultaneous claim requests yield exactly one durable owner;
- claims are rejected without mutation in `DISPATCH_PAUSED`, `DRAINING`, and
  `MAINTENANCE`; first Results remain accepted in the first two modes, while
  `MAINTENANCE` returns only an exact response already in the Result Request
  registry under the same global key and creates no new ledger or workflow
  row; an unseen key gets exact `503 CONTROLLER_MAINTENANCE` even if another
  key already accepted the same semantic Result, and the same key/body remains
  available for a normal post-maintenance retry while its capability is
  authentic;
- an idempotent same-session Attempt Claim ID/request-digest retry returns the
  same durable response contract and rematerializes only currently valid
  sensitive fields from the exact frozen source/capability identities; another
  key/session cannot claim, and crash injection
  leaves either both reciprocal Attempt/Claim rows or neither;
- Redis flush after claim preserves generation and deadline and does not
  fabricate worker loss;
- an otherwise-current old-epoch offer can still claim its durable `OFFERED`
  Attempt, and liveness, upload, or Result from an unexpired pre-restart claim
  is accepted without an epoch comparison;
- stale worker-session and stale-generation liveness, uploads, results, and
  pool-loss reports are rejected;
- loss reported for a reused VM ID cannot fence the replacement session;
- a matching authenticated v1 loss report atomically sets the exact current
  Attempt to `FAILED` with reason `WORKER_LOST`, while PEL age/reaping and stale
  or cross-class reports cannot change Attempt state;
- an unauthorized principal or out-of-scope pool manager cannot report
  capacity, and one source identity replay returns the original ordered Health
  Observations while conflicting content and stale report revisions mutate
  nothing;
- equal payloads in fresh higher-sequence capacity reports create new ordered
  Health Observations without allowing a replay to extend expiry;
- Capacity Report acceptance freezes one controller `accepted_at_ms` and
  positive configured TTL; expiry equality with acceptance, or expiry beyond
  `accepted_at_ms + configured_max_ttl_ms`, is rejected, while equality with the
  upper bound is accepted regardless of `observed_at_ms`;
- a `WORKER_SESSION/AVAILABLE` report requires exact registered ready-session
  evidence, and session reuse, availability/count mismatch, or a value above
  the registered ceiling is rejected;
- Redis flush/startup rebuilds capacity only from SQLite Health Observations
  and persisted expiry timers, and an exact compatible `AVAILABLE` observation
  wakes its bound capacity Wait Condition without lowering policy;
- a session-level `UNAVAILABLE` report cannot fence a claimed Attempt without
  a matching authenticated worker-loss report;
- liveness has no idempotency key or durable response ledger: increasing exact-
  session sequences refresh disposable current control, lower/duplicate values
  do not rewind, and Redis loss permits the next higher sequence to establish a
  new high-water mark;
- ambiguous endpoint timeouts reuse the exact endpoint-declared identity and
  body only within its authority window, while liveness alone advances to the
  next sequence; a timeout never causes a fresh Result/rotation/claim identity;
- a schema-valid authenticated stale Result strictly before its own deadline
  creates/replays exactly one `STALE_ATTEMPT` ledger response and no Result,
  terminal fact, recovery input, or Transition;
- claim-deadline expiry atomically freezes the ordered highest-applicable
  unexpired Health evidence, exact current logical-provider Secret version,
  capacity disposition, Controller Mode projection, Capability Registry/
  selected-key projection, and replacement-offer disposition with its Timer/
  Terminal Facts; absent exact-version provider evidence is neutral rather than
  synthetic availability. The terminal transaction expires the Attempt, returns
  its Activity to `PLANNED`, and appends zero-counter Recovery Evidence; a later
  Evidence Transition alone creates the higher-generation replacement or the
  bound Wait. Replacement is created only when capacity is compatible, mode
  permits offers, and the selected issuance key is ACTIVE; later observations
  cannot change replay, and no branch advances recovery counters.
- a no-capacity claim deadline in a review/adjudication panel appends
  panel-scoped `STAFF_PANEL` Evidence only after proving no peer claim remains,
  superseding peer offers, and freezing every currently unfilled slot. Its later
  Evidence Transition offers all still-current slots atomically or creates one
  bound Wait; when another unfilled slot still has a `CLAIMED` peer, the
  terminal transaction expires only the due Attempt, leaves its Activity
  `PLANNED`, updates the coalesced staffing pointer, and creates no Recovery
  Evidence, `RECOVERING` state, Wait, or offer for that slot. A surviving peer
  defers staffing reduction to the next safe boundary.
- source clone credentials are Attempt-scoped and cannot push, brokered source
  archives carry no source credential, and workers have no publication
  endpoint;
- a clone credential and any authority-bearing fetch URL are gone before model
  or Candidate commands start, and provider material is absent from Candidate
  command environments and Candidate packaging;
- a Candidate artifact is durable before the accepting SQLite transaction;
- after Redis loss or controller restart, REVIEW/ADJUDICATE claim reconstruction
  returns the identical durable ordered `subject_refs`; a later role/policy
  lookup cannot add, drop, or reorder them, and receipt validation rejects any
  missing, duplicate, or unknown subject;
- every crash point around upload promotion and result commit reconciles to an
  orphan or one accepted Candidate, never a missing live artifact;
- validation, promotion, and first Candidate Result finalization at upload
  expiry equality all reject, atomically leave the upload `EXPIRED`, and create
  no Candidate/Result, while content PUT and Result both return the exact HTTP
  410 `orcest.candidate-upload-expired/1` body and the Result path durably
  replays it from `UPLOAD_EXPIRED`; an already accepted Result replay remains stable;
- credential rotation survives crashes on both sides of Secret Store/reference
  commit without exposing material; APPLIED creates reciprocal Request/Receipt/
  Version/ref/fanout, CAS_LOST creates only its stored 409 ledger, keyed byte
  conflict is rejected, exact replay is allowed only before execution deadline,
  and Attempt Result cannot contain a rotation binding;
- a valid Verification `ERROR` is accepted only with
  `FAILED_RETRYABLE/VERIFICATION_ERROR`, returns the Activity to `PLANNED`
  before recovery, and a higher generation can later submit one decisive
  `PASS` or `FAIL` through the `PLANNED -> READY` transition;
- no other failed Attempt Result can carry a Receipt;
- the exact Activity/outcome Result tagged union rejects mixed Candidate,
  Receipt, structured plan/diagnosis output, failure, and credential-rotation
  fields without changing the claim;
- a first Result at or after `execution_deadline_ms` is rejected even when the
  timeout sweeper has not run, while an exact replay of an already accepted
  Result still succeeds strictly before `capability_auth_expires_at_ms`; every
  non-Result Attempt endpoint denies during that grace and all capability calls
  deny without a ledger at or after auth expiry;
- Result request IDs share one global namespace across accepted, upload-expired,
  stale-attempt, current-expired, and already-terminal dispositions;
  cross-disposition/key-body
  collisions never create a second workflow object;
- capability claims bind an exact durable signer key ID and ED25519 algorithm;
  retired keys accept only otherwise-valid prior claims through expiry,
  revoked/unknown/mismatched keys fail even replay, and restored endpoints stay
  closed until referenced public verifiers are audited;
- backup preparation with a claimed near-deadline Attempt never acquires the
  barrier or returns a backup-specific retryable Result response; the Result
  transaction can commit normally before its deadline, and the later bounded
  barrier starts only after the durable claimed count is zero;
- every allowed worker failure class is rejected outside its Activity/outcome
  row and deterministically emits the named lifecycle Recovery Input;
- worker `INTEGRITY_FAILURE` can create only `INTEGRITY_SUSPECTED` Evidence and
  an exact pre-I/O probe intent; only the reciprocal negative Fact/Observation
  creates typed `STORAGE` or `CREDENTIAL` Evidence, and only its separate
  Evidence Transition creates the recovery Wait;
- the pinned repeated-failure threshold routes the same normalized VERIFY
  failure to `VERIFICATION_FAILURE/DIAGNOSE` and the same consensus or
  adjudication blocker to `REVIEW_DISAGREEMENT/DIAGNOSE`, without planning one
  more remediation or entering a human boundary;
- a malformed result cannot transition the workflow or request a human; and
- every endpoint rejects plaintext HTTP before accepting a body, including the
  raw-secret rotation endpoint; and
- controller restart plus result replay releases the worker without duplicate
  acceptance.

## Evidence and migration

Current evidence that this protocol replaces:

- `src/orcest/shared/models.py` serializes the full prompt, raw GitHub token,
  legacy Claude token, and provider credential into Redis. V1 replaces it with
  a secret-free offer and post-claim materialization.
- `src/orcest/worker/loop.py` treats `XREADGROUP` delivery and a Redis lock as
  the claim, retains the source PEL entry through execution, publishes a result
  to Redis, and signals `pool:done:*`. V1 moves claim/result/release authority
  to the controller database and API.
- `src/orcest/worker/workspace.py` embeds a clone token temporarily and installs
  a credential helper specifically so the agent can push. V1 requires a
  read-only Attempt-scoped clone credential or controller-brokered source Git
  bundle, and no push-capable helper.
- `src/orcest/worker/_runner_base.py` places both forge and provider credentials
  in the agent environment. V1 retains provider execution but removes forge
  write authority and bounds both credentials to the Attempt.
- `src/orcest/fleet/pool_manager.py` uses consumer PEL state as an activity
  signal and publishes synthetic failed results when a VM is reaped. V1 uses a
  worker-session-fenced loss report; the reducer owns the resulting recovery.
- `src/orcest/shared/credential_handoff.py` and
  `src/orcest/orchestrator/loop.py` recover rotated credential blobs through
  Redis task/result state. V1 uses the Secret Store endpoint and durable version
  ordering.
- `tests/integration/test_mixed_provider_streams.py` verifies provider stream
  isolation and same-consumer PEL recovery. V1 preserves Worker Profile stream
  isolation but replaces same-VMID PEL recovery with a new worker session and
  durable Attempt reconciliation.

Existing guarantees retained are at-least-once execution, stale-work fencing,
provider-specific routing, bounded credential exposure, and retry after worker
loss. Replaced guarantees are PEL-based ownership, Redis result durability,
worker-authored forge publication, and agent-controlled `needs-human` output.

Deferred rollout validation gates:

These are implementation/cutover tests for the applicable rollout stage, not
unresolved normative protocol decisions or prerequisites for accepting this
specification:

- prove the chosen worker-session issuance and TLS bootstrap works through the
  current Proxmox/cloud-init path without placing credentials in Redis or image
  layers;
- establish safe Git bundle limits against representative large repositories
  and malicious object graphs;
- failure-inject every API/storage ordering boundary in the table above; and
- verify each provider CLI works with read-only forge access and the pinned
  Candidate checkout while preserving OAuth rotation handoff.
