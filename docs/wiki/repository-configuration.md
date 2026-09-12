# Repository Configuration

Status: Accepted normative v1 specification (2026-08-27)
Target: workflow-control v1

## Scope

This page defines the repository-owned `.orcest` bundle, its deterministic
normalization and pinning, and the local CLI contract used to prepare and
validate a project. Repository policy parameterizes the code-owned reducer; it
does not define arbitrary states, transitions, scripts in the controller, or
new human-escalation reasons.

Machine-readable YAML owns lifecycle parameters. Markdown owns static agent
instructions. Keeping those concerns separate lets maintainers write useful
context without making prose an executable state machine.

The supported parameters refer to objects and states in [Domain model](domain-model.md),
[Workflow lifecycle](workflow-lifecycle.md), and
[Review and consensus](review-and-consensus.md). Server-side project authority
and publication behavior are defined in [Forge integration](forge-integration.md).

## Repository layout

The v1 bundle has one entry point and one workflow:

```text
.orcest/
├── project.yaml
├── workflows/
│   └── implementation.yaml
└── prompts/
    ├── implement.md
    ├── repair.md
    ├── review-correctness.md
    ├── review-security.md
    └── adjudicate.md
```

`project.yaml` and its referenced workflow are required. Prompt filenames are
examples; only paths explicitly referenced by the workflow belong to the
normalized bundle. v1 does not recursively load the directory, discover
plugins, or execute `.orcest/WORKFLOW.md`. A repository MAY include a
`WORKFLOW.md` for humans, but it has no control-plane meaning unless a future
schema explicitly adds it.

All referenced files MUST be regular Git blobs at the same trusted commit,
under `.orcest/`, using repository-relative, slash-separated paths. Absolute
paths, `..`, symlinks, submodule entries, case-colliding paths, YAML includes,
and network references are invalid.

## `project.yaml` schema

```yaml
apiVersion: orcest.dev/v1
kind: Project
spec:
  workflow: .orcest/workflows/implementation.yaml
  base:
    changePolicy: rebase-before-publication
  intake:
    readyLabel: orcest:ready
    workingLabel: orcest:working
    specificationComments: none
```

The exact v1 fields are:

| Field | Type and default | Meaning |
| --- | --- | --- |
| `apiVersion` | required literal `orcest.dev/v1` | Parser and normalization contract. |
| `kind` | required literal `Project` | Rejects a file of the wrong type. |
| `spec.workflow` | required path | One v1 Workflow document. |
| `spec.base.changePolicy` | `rebase-before-publication`, `pin`, or `supersede-at-boundary`; default `rebase-before-publication` | Supported reducer policy when the source base advances. |
| `spec.intake.readyLabel` | nonempty string; default `orcest:ready` | Admission-request projection. |
| `spec.intake.workingLabel` | nonempty string; default `orcest:working` | Active-Run projection. |
| `spec.intake.specificationComments` | `none` or `maintainer-marked`; default `none` | Whether explicitly marked comments join title/body as specification input. |

The trusted base ref is authenticated Project registration, not repository
policy. Repository code cannot redirect the controller to a different ref.
The ready and working labels MUST differ. A server MAY fix their values at
project registration; if so, repository values must match.
`rebase-before-publication` continues current work against the pinned base and,
when the trusted base advanced, produces a new Candidate on the observed base
and reruns all gates before publication. `pin` continues against the admitted
base until the forge accepts it or reports a conflict.
`supersede-at-boundary` applies base-only movement through the lifecycle's safe
generation boundary only before the Publication becomes `ACTIVE`; afterward
base-only movement is ordered audit input and cannot capture, pend, or install
a Snapshot. This exception does not suppress an authorized specification
change, normalized workflow change, or server Policy Update after `ACTIVE`;
those inputs still capture/coalesce and install a new generation, and a
combined change is not classified as base-only. No mode hot-reloads workflow policy or carries Candidate
approvals to a new commit.

In `maintainer-marked` mode, only text between the exact markers below in a
comment whose forge author association is owner, member, or collaborator is
included:

```html
<!-- orcest:spec-begin -->
additional requirements
<!-- orcest:spec-end -->
```

Selected sections are ordered by stable forge comment ID and hashed with that
ID and author ID. Edited or deleted selected text therefore changes the
specification hash. Replies and unmarked discussion remain excluded.

## Workflow schema

```yaml
apiVersion: orcest.dev/v1
kind: Workflow
metadata:
  name: implementation
spec:
  implementation:
    profile: codex-default
    prompt: .orcest/prompts/implement.md
    timeoutSeconds: 7200
    alternateProfiles: [claude-default, grok-default]
  verification:
    profile: default
    commands:
      - id: unit
        argv: [make, test]
        cwd: .
        timeoutSeconds: 1800
        environment:
          CI: "true"
    repair:
      profile: codex-default
      prompt: .orcest/prompts/repair.md
      timeoutSeconds: 7200
      alternateProfiles: [claude-default, grok-default]
    maxRepairCyclesBeforeDiagnosis: 4
  review:
    approvalsRequired: 2
    requireDistinctProviderFamily: false
    requireDistinctModelFamily: false
    slots:
      - id: correctness
        profile: claude-review
        prompt: .orcest/prompts/review-correctness.md
        alternates: [codex-review]
        timeoutSeconds: 3600
      - id: security
        profile: codex-review
        prompt: .orcest/prompts/review-security.md
        alternates: [claude-review]
        timeoutSeconds: 3600
    adjudicator:
      profile: claude-review
      prompt: .orcest/prompts/adjudicate.md
      alternates: [codex-review]
      timeoutSeconds: 3600
  publication:
    requiredChecks: [test]
    externalHeadPolicy: verify-and-adopt
  recovery:
    maxAttemptsPerActivityBeforeDiagnosis: 3
    maxDiagnosesBeforeReplan: 2
    maxProviderRateLimitWaitMs: 86400000
```

`apiVersion` and `kind` are required literals matching the example.
`metadata.name` is a required lowercase identifier matching
`[a-z][a-z0-9-]{0,63}` and is stored for audit only; v1 still loads exactly the
one path selected by `project.yaml`. `spec.implementation`,
`spec.verification`, and `spec.review` are required. Verification contains at
least one command, its `repair` object is required, Review contains at least
two slots, and its `adjudicator` object is required. `spec.publication` and
`spec.recovery` may be omitted only when all of their materialized defaults
below are accepted. No additional top-level or `spec` fields are valid.

### Implementation

| Field | Requirement |
| --- | --- |
| `profile` | Required server-registered execution profile allowed for this project. |
| `prompt` | Required static Markdown instruction path. |
| `timeoutSeconds` | Integer `60..serverMaximum`; default `7200`. This is an Attempt hard deadline, not an acceptance gate. |
| `alternateProfiles` | Ordered, unique profile IDs; default empty. An alternate must satisfy the same Activity role and capability requirements. |

Every `profile`, `alternateProfiles`, or `alternates` entry under
`implementation`, `repair`, a review slot, or `adjudicator` is a
server-registered **execution profile ID**, not a Worker Profile or an
arbitrary command-line fragment. (`verification.profile: default` is the
separate code-owned deterministic verification profile described below.) An
execution-profile resolution for an Attempt yields one exact
`(worker_profile, provider, model, provider_account_ref)` assignment plus its
code-owned harness mode and capabilities. The server registry also supplies
non-secret `provider_family` and `model_family` classifications under an
immutable `classification_revision`; the controller freezes all three on the
Attempt. These are controller-owned evidence for review independence, not
repository-selectable execution values. The non-secret
`provider_account_ref` is selected by the controller from the profile's
server-owned allowed account order; repository YAML cannot name an account.
For every model-backed execution/worker profile, the same server registry
supplies a non-secret launch-isolation mapping: registered capacity pool,
runner-shim principal, immutable runner image digest, exact
`runner_signature_algorithm`, `runner_signing_key_id`, and
`runner_registration_revision`. The normalized
effective policy materializes that complete mapping. Repository YAML cannot
name, replace, or relax any launch-isolation value.

The Project's server-owned `budget_policy_ref` and
`budget_reset_window_ref` resolve into one closed scheduling policy: bounded
`accounting_scope_id`, integer micro-unit definition, positive limit,
reset-window identity and deterministic next-reset computation, positive
`max_budget_report_age_ms`, and the registered budget-accounting principal
authorized for that Project/scope. The normalized effective policy
materializes this complete resolution. Repository YAML cannot report
consumption, select the accounting principal, change units, increase the
limit, or extend report freshness.

`worker_profile` is only the runner/protocol compatibility class
used for dispatch streams (for example `codex`, `claude`, or `grok`); it is not
selected directly by repository YAML. The controller resolves and persists
both identities and the exact provider/model before creating an Attempt. A
repository may select only execution profiles allowed by authenticated Project
registration. The lifecycle's `PLAN`, `BUILD`, `DIAGNOSE`, and `REPLAN`
Activities all use this implementation execution profile and prompt; a
controller-owned mode envelope gives each one its distinct input and
structured output schema. Orcest, rather than a harness-specific internal
loop, owns those passes, Candidate boundaries, review, repair, and termination.
One harness invocation performs one fenced Attempt and returns one typed
result.

### Verification

`profile` has the sole v1 value `default`. It denotes one `VERIFY` Activity per
Candidate that executes the complete ordered command list and returns one
Verification Receipt. A repository cannot split commands across Activities or
select a weaker subset for a particular Candidate.

`commands` is an ordered list with unique `id` values. Each item has:

- `argv`: a nonempty array of literal UTF-8 arguments. No shell parses it.
- `cwd`: a normalized repository-relative directory, default `.`.
- `timeoutSeconds`: integer `1..serverMaximum`, default `1800`.
- `environment`: optional string-to-string literals restricted to the server's
  non-secret environment allowlist.

Commands execute on the exact Candidate in a credential-free sandbox. The
controller records every configured command result even after one fails unless
a hard sandbox failure prevents further execution. Repository config cannot
request a SecretRef, host path, privileged container, controller socket, or
network credential. Network is disabled by default and can be enabled only by
a server-registered verification profile, never a YAML URL or token.

The effective policy pins a code-owned verification startup/cleanup overhead
in seconds. The `VERIFY` Attempt execution interval is exactly the sum of all
command `timeoutSeconds` values plus that overhead. Bundle validation fails if
the sum exceeds the server's verification maximum; the controller never
silently truncates commands or shortens a declared command timeout.

`repair.profile` and `repair.prompt` define the Activity created for a failed
verification or confirmed blocker. `maxRepairCyclesBeforeDiagnosis` defaults
to `4` and is bounded by server policy. Reaching it for the same normalized
verification or review/adjudicated-blocker fingerprint appends the lifecycle's
typed Recovery Evidence selecting `DIAGNOSE`; only the following Evidence
Transition schedules diagnosis, and later diagnosis may select replanning. It
does not accept failing work or automatically request a human.
The lifecycle's `REMEDIATE`, `PR_REMEDIATE`, and `REBASE` Activities all reuse
this repair profile and prompt with code-owned mode envelopes and result
schemas. `repair.timeoutSeconds` and `repair.alternateProfiles` have the same
validation and meaning as their implementation counterparts and default to
the implementation values.

### Review

`approvalsRequired` is an integer with default and minimum `2`. A repository
may raise it within the server maximum but cannot lower it. In v1, `slots`
contains exactly that many unique role IDs; there are no advisory or extra
gating slots. A server-added mandatory slot also raises the materialized
effective approval threshold by one. Each slot selects a primary execution profile,
static prompt, ordered unique alternates, and `timeoutSeconds` in
`60..serverMaximum` with default `3600`. The server validates profile
capability, project allowlisting, and any required provider/model-family
separation.

`requireDistinctProviderFamily` and `requireDistinctModelFamily` are booleans
with default `false`. The normalized effective value of each is repository
value OR the server-enforced project value, so repository policy may strengthen
but never relax separation. Regardless of those booleans, every slot and every
adjudication is a fresh harness invocation with a fresh Attempt, transcript,
and context assembled independently from the immutable evidence set. The same
Attempt, execution transcript, or reviewer context cannot fill two slots.
`alternates` are execution-profile substitutions for unavailable capacity, not extra votes.
Substitution preserves every slot constraint and never changes
`approvalsRequired`.

The required `adjudicator` object names one execution profile and prompt. Its receipt
does not count as an approval; the fixed reducer uses it only as described in
the consensus contract. `alternates` and `timeoutSeconds` use the same review
slot rules and default to empty and `3600` respectively.

V1 materializes exactly one adjudicator slot with ID `default` and required
adjudication count `1`; neither repository nor server policy may add another
slot or raise/lower that count in v1. The `adjudicator` object configures that
slot. Each frozen panel can create at most one disputed finding set and uses
`adjudication_round = 1`; an abstention or inconclusive result creates a higher
Attempt generation for the same Activity/slot/round. A complete overrule opens
a fresh review `panel_round`, whose later distinct dispute—if any—again uses
adjudication round `1`. Server policy may strengthen the slot's execution
profile independence constraints but not change this topology.

### Publication and recovery

`requiredChecks` is an ordered, unique list of adapter check keys. An empty
list means no forge CI check is configured, not that pre-publication
verification is skipped. Project registration may add mandatory checks that
the repository cannot remove.

`externalHeadPolicy` has the sole v1 value `verify-and-adopt`; spelling it out
makes future schema changes explicit without allowing unsafe overwrite modes.
Every post-publication replacement Candidate reruns the same configured
verification and consensus gate before a compare-and-swap ref update. A
different or weaker post-publication gate is not configurable in v1.

Recovery integers are scheduling thresholds:

| Field | Default | Result when reached |
| --- | --- | --- |
| `maxAttemptsPerActivityBeforeDiagnosis` | `3` | schedule a diagnostic Activity with accumulated typed evidence |
| `maxDiagnosesBeforeReplan` | `2` | schedule a fresh plan/profile selection at the same acceptance policy |
| `maxProviderRateLimitWaitMs` | `86400000` | clamp a provider-supplied absolute retry timestamp to this many milliseconds after controller acceptance |

Server maxima bound cost and denial of service. Exhausting these counters
causes deterministic wait, replacement, diagnosis, or replan behavior. It is
not a configurable path to `needs-human`, an undeclared lifecycle state, or a
weaker quorum.
`maxProviderRateLimitWaitMs` is a positive integer. Its materialized default is
`86_400_000`; v1 server policy rejects any value above its own positive cap,
whose default is `604_800_000`. It controls only the deterministic clamp of a
provider's absolute retry timestamp and never extends an Attempt execution
deadline.
The offer claim timeout is server-owned in v1: default `300_000` ms, accepted
server range `30_000..3_600_000`. Repository YAML has no claim-timeout key and
cannot extend, renew, or recompute an emitted Attempt's immutable
`claim_deadline_ms`.

## Prompt contract

Prompt files are UTF-8 Markdown with no template language, includes, shell
expansion, or embedded secrets. The controller composes a harness request from:

1. a code-owned protocol preamble and output schema;
2. the exact normalized static prompt bytes;
3. a code-owned, length-bounded representation of the Work Item Snapshot and
   prior typed evidence; and
4. attempt-scoped capability locations where the protocol permits them.

Untrusted Work Item, CI, review, and repository text is kept in delimited data
sections. It cannot replace the output schema or introduce lifecycle control
directives. Provider adapters may translate the same logical request into
their native message format, but the logical input digest is persisted so an
Attempt is auditable.

## Parsing and validation

The v1 parser MUST:

- use safe YAML 1.2 scalar rules;
- reject duplicate mapping keys, merge keys, aliases/anchors, explicit tags,
  floats, null where not declared, and every unknown key;
- accept only UTF-8 without NUL bytes;
- enforce per-file and total-bundle size limits before parsing;
- normalize and validate all paths before fetching a referenced blob;
- validate enum values and integer ranges without coercing strings; and
- report errors with file, path, and stable error code.

The complete graph is validated before admission. There is no best-effort
default for a misspelled policy or unavailable required profile.

Server-enforced limits are intersected with repository policy. Repository
policy MAY strengthen approval counts, verification, and timeouts within
limits. It cannot grant credentials or permissions, enable an unregistered
profile, raise resource ceilings beyond server policy, disable audit, define a
new state, or permit candidate code to contact the control plane.

## Normalization and bundle hash

The controller resolves the registration-owned trusted base ref once and reads
every bundle blob by that immutable commit. It then creates a normalized object
using these exact rules:

1. validate both YAML documents and materialize every declared default;
2. represent mappings with lexicographically sorted UTF-8 keys;
3. preserve list order where order is semantic, and reject duplicates where a
   list represents a set;
4. represent integers as base-10 integers and booleans as JSON booleans;
5. normalize all text to Unicode NFC and prompt line endings to LF, remove one
   UTF-8 BOM if present, and ensure one final LF in each prompt;
6. create a `files` map from normalized path to Git blob object ID, media kind,
   byte length, and the domain-separated Workflow Blob digest of the normalized
   bytes; and
7. serialize the whole object as UTF-8 canonical JSON with no insignificant
   whitespace.

The workflow hash is:

```text
"sha256:" + lowercase_hex(
  SHA256(UTF8("orcest-config-bundle/v1\n") || canonical_json_bytes)
)
```

Every Snapshot `blob_digest` uses the canonical [Workflow Blob
identity](domain-model.md#workflow-blob), not raw `SHA-256(bytes)`:

```text
"sha256:" + lowercase_hex(SHA256(
  ascii("orcest-workflow-blob-v1") || 0x00 ||
  utf8(media_kind) || 0x00 ||
  uint64_be(byte_length) ||
  normalized_bytes
))
```

Thus identical bytes with different media kinds have different durable blob
identities. The separate workflow hash still covers the canonical `files` map
and entire normalized bundle.

The effective `policy_hash` covers the fully materialized gating inputs:
verification profile, ordered commands and timeouts, pinned verification
overhead, repair and implementation profile constraints, the complete allowed
server-registry mapping from each Execution Profile ID to its exact
`(worker_profile, provider, model, provider_account_ref)` resolutions, the
canonical `provider_family` and `model_family` IDs for those resolutions and
their immutable `classification_revision`, each model-backed profile's exact
registered capacity pool, runner-shim principal, runner image digest, signature
fields `runner_signature_algorithm`/`runner_signing_key_id`, and
`runner_registration_revision`, required review slots, approval
threshold, both family-separation booleans, adjudicator constraints,
publication checks, base-change policy, recovery thresholds, Attempt deadlines,
the server-owned claim timeout, the positive `maxProviderRateLimitWaitMs` (bounded above by the server-owned
maximum), the complete resolved budget accounting scope/unit/limit/reset,
`max_budget_report_age_ms` and authorized-principal policy, and every
server-enforced strengthening of those values.
Credentials, runner private signing keys, mutable capacity, current provider
health, budget consumption, and display metadata are excluded. All included
launch-isolation fields are non-secret identifiers/digests. An authenticated server policy change is
installed only at a lifecycle policy-generation boundary; it never silently
changes an active Run.

The runner-shim `runner_signing_key_id` above selects the key that signs the
Launch Attestation and is a policy input. The controller's separate
`CapabilitySigningKey`, which signs
Attempt and one-shot launch capabilities, is server security infrastructure and
is not repository-selectable or part of `policy_hash`. Each Attempt Claim
freezes the exact capability key ID/algorithm in its signed claims and durable
row. Retirement/revocation follows the Domain key registry and cannot weaken or
rewrite repository policy.

The Work Item Snapshot stores the trusted source commit, workflow hash, schema
version, complete normalized configuration, normalized bytes of every
referenced static prompt (or an immutable controller-store reference whose
bytes are included in the same backup unit), prompt hashes, and the controller
build/reducer version. A later forge rewrite or outage therefore cannot make an
active policy or prompt unrecoverable. Hash equality is byte equality under
these rules, independent of YAML formatting or map order.

## Pinning and changes during a Run

An active specification generation uses only its stored normalized
configuration. A Candidate that changes `.orcest` does not alter the current
Run, reviewer roster, commands, quorum, recovery policy, or prompts.

When a full intake reconciliation observes a new trusted base commit, the
controller reloads and hashes the bundle from that commit. If the normalized
bundle is unchanged, no policy generation is created solely for formatting or
unrelated source changes. A changed workflow hash is a changed specification
input and is applied only through the lifecycle's safe supersession rule. A
base source change may independently trigger the configured base-change
policy.
For `supersede-at-boundary`, this independent base-only trigger is disabled
after Publication `ACTIVE`; later conflict/head feedback uses ordinary
post-publication remediation under the installed Snapshot.

Historical generations retain their normalized object and hashes even after
the forge deletes or rewrites the original ref.

An authenticated server Policy Update for an active Run MUST build its capture
from that Run's latest accepted Work Item observation and trusted-base
observation, then intersect the newly selected server policy with the pinned
repository bundle from that same composite input. It MUST NOT start from the
installed Snapshot merely because a newer Work Item capture is still pending.
Consequently a pending specification edit cannot be overwritten or
misclassified as a policy-only update; the resulting capture represents all
latest accepted inputs. The input Transition only captures/coalesces it by
Run-local Snapshot sequence; a separate `SPEC_SUPERSEDE` Transition is the
sole installer.

## Local CLI contract

The same schema and normalization library MUST be used by the controller and
CLI. v1 exposes:

```text
orcest project init [--profile <name>]
orcest project lint [--server <profile>] [--revision <git-revision>]
orcest project explain [--server <profile>] [--revision <git-revision>]
orcest project simulate --event <fixture> [--revision <git-revision>]
orcest project onboard --server <profile> --repo <forge-locator>
```

- `init` creates the minimal layout without credentials and refuses to
  overwrite an existing file unless explicitly directed through a separate,
  recoverable update command.
- `lint` performs local structural checks. With `--server`, it also validates
  profile names, forge capabilities, server limits, and registration policy.
- `explain` prints the fully defaulted normalized policy, referenced files,
  trusted commit, hashes, capability choices, and server constraints. It MUST
  redact all authentication material.
- `simulate` runs the real reducer against a typed, non-secret fixture and
  prints transitions and Activities without executing an agent or forge write.
- `onboard` sends an authenticated, idempotent project-registration request to
  the named Orcest server. It never SSHes to Proxmox and never edits a remote
  `/etc/orcest/config.yaml` directly.

The exact v1 endpoint is:

```http
POST /api/v1/projects/registrations
Content-Type: application/json
Idempotency-Key: <lowercase UUID>
```

The server URL MUST be HTTPS and the CLI MUST validate the complete server
certificate and registered hostname/identity. `--server` cannot disable
validation or downgrade to plaintext, including for a private IP, VPN, or local
controller.

```json
{
  "protocol": "orcest.project-registration/1",
  "idempotency_key": "lowercase-uuid-equal-to-header",
  "project_id": null,
  "expected_registration_revision": null,
  "forge": {
    "adapter_kind": "GITHUB",
    "canonical_origin": "https://github.com",
    "installation_or_account_ref": "non-secret-server-registration-ref",
    "repository_locator": "owner/repository"
  },
  "requested_default_ref": "refs/heads/main",
  "trusted_base_policy_ref": "server-policy-ref",
  "budget_policy_ref": "server-policy-ref",
  "budget_reset_window_ref": "server-policy-ref"
}
```

`project_id` and `expected_registration_revision` are both `NULL` for first
registration. For revalidation they are respectively the exact existing
Project UUID and its required positive current registration revision; the
server performs a compare-and-swap and returns `409` on mismatch. Revalidation
requires `requested_default_ref`, `trusted_base_policy_ref`,
`budget_policy_ref`, `budget_reset_window_ref`, and
`forge.installation_or_account_ref` to equal the current Project values; it
refreshes only mutable repository-locator/readiness projections.
Authority-bearing ref or policy changes arrive only through the ordered
`SERVER_ROLLOUT` Policy Update path and never this endpoint. All remaining
request fields are required non-secret strings with code-owned size and
normalization bounds; `adapter_kind` has the
sole v1 value `GITHUB`. Raw credentials, bearer tokens, private keys, Secret
Store paths, and Secret values are forbidden request fields. The
`installation_or_account_ref` and policy references select already registered
server-side objects and confer no authority themselves. Changing the
installation/account binding requires a separately specified authenticated
migration and is not a v1 revalidation or Policy Update.

Fresh-store Stage 0 is ordered before repository onboarding: Controller Mode
`INITIALIZE` to revision-1 `MAINTENANCE`; provision/adopt the exact
`CONTROLLER/ORCEST_V1` `CAPABILITY_SIGNING_PRIVATE_KEY` Secret and its real
creation Receipt; Capability Key `REGISTER`; separate `SELECT`; then other
Secrets, registered budget-accounting principals/policies, and Projects.
Synthetic signing rows or combined register/select are invalid. Stage 0
installation enrollment may then provision the three distinct forge
credential purposes before a Project exists only through
`FORGE_INSTALLATION` Secret Provision Operations whose owner ID and
provider/account reference both equal this exact canonical
`installation_or_account_ref`. Registration separately resolves the current
verified `FORGE_API`, `SOURCE_READ`, and `PUBLICATION` logical Secrets and
creation Receipts, verifies each purpose/installation authorization, and
records the first on Forge Instance plus the latter two on Project with their
provenance versions. The source-read and publication Secret IDs must differ. A
request cannot substitute a Project- or Controller-owned Secret or reuse one
purpose for another.

A successful Project `REGISTER` during this maintenance phase atomically
creates its `ACTIVE` revision-0 `WORK_ITEM_DISCOVERY` Schedule as durable
cadence state only. Controller Mode still suppresses all ordinary Schedule
Request creation, dispatch, and completion until a later authenticated exit
from `MAINTENANCE`; registration does not perform discovery I/O.

The transport-authenticated principal must have server-owned
`PROJECT_REGISTER` authority for a create, or `PROJECT_REVALIDATE` authority over
the exact existing Project, plus use authority for the named forge
installation/account and all three policy references. Budget references must
resolve to the closed accounting scope/unit/limit/reset/freshness policy and a
pre-registered reporting principal authorized for that exact Project scope;
registration cannot create or grant that authority. The server rechecks forge
access, resolves stable forge/repository IDs and the trusted base commit,
loads the trusted `.orcest` bundle from that commit, validates every execution
profile, launch-isolation mapping, adapter capability, limit, and policy
strengthening, and commits the Project only after all checks pass. Authentication
failure is `401`, insufficient authority is `403`, an idempotency/body or
stable-repository ownership conflict is `409`, and bounded schema/capability
failure is `422`; none may partially create or mutate a Project.

The controller durably keys registration replay by
`(authenticated_principal_id, idempotency_key)`. First acceptance stores the
canonical request digest, authorization-context digest, resolved identities,
and canonical response before acknowledging it. An identical replay returns
that response with `replayed: true`; reuse by the same principal with a
different canonical body is `409` and changes no state. The same UUID from another
principal is a different composite replay identity, but it grants no Project or
forge-installation authority and remains subject to ordinary ownership/RBAC
checks.

```json
{
  "protocol": "orcest.project-registration-result/1",
  "idempotency_key": "lowercase-uuid",
  "replayed": false,
  "mode": "REGISTER",
  "status": "SUCCEEDED",
  "project_id": "lowercase-uuid",
  "registration_revision": 1,
  "registration_state": "ACTIVE",
  "forge_instance_id": "lowercase-uuid",
  "installation_or_account_ref": "non-secret-server-registration-ref",
  "repository_external_id": "opaque-stable-id",
  "repository_locator": "owner/repository",
  "default_ref": "refs/heads/main",
  "trusted_base_commit": {"object_format": "sha1", "oid": "40-hex-oid"},
  "workflow_hash": "sha256:64-lowercase-hex",
  "policy_hash": "sha256:64-lowercase-hex",
  "trusted_base_policy_ref": "server-policy-ref",
  "budget_policy_ref": "server-policy-ref",
  "budget_reset_window_ref": "server-policy-ref",
  "readiness": {"ready": true, "diagnostics": []}
}
```

An accepted business-validation rejection has the closed body:

```json
{
  "protocol": "orcest.project-registration-result/1",
  "idempotency_key": "lowercase-uuid",
  "replayed": false,
  "mode": "REGISTER",
  "status": "REJECTED",
  "rejection_code": "WORKFLOW_INVALID",
  "diagnostics": []
}
```

`STABLE_REPOSITORY_OWNERSHIP_CONFLICT` maps to HTTP `409`;
`WORKFLOW_INVALID`, `CAPABILITY_UNSUPPORTED`, and
`POLICY_VALIDATION_FAILED` map to `422`. Authentication failure, malformed
transport/schema framing, an idempotency/body conflict, or revalidation CAS/
authority-reference mismatch is rejected before a terminal registration
Operation and uses the bounded common error response. Success is HTTP `200`.

The durable response stores `replayed = false`. On identical replay the
controller changes only that transport projection to `true`.
`ProjectRegistrationOperation.response_digest` covers HTTP status and every
canonical body field except exactly `replayed`; this rule is identical for
success and rejection. It is a public-response digest and MUST NOT include
resolved Secret References, authorization evidence, or another internal field
absent from the response. The separate internal `resolution_digest` covers the
request/authorization digests, installation binding, resolved forge/repository/
base identities, and conditional source/publication Secret References.

Successful `REGISTER` atomically creates the Project and its sole revision-0
`ACTIVE` Project-targeted `WORK_ITEM_DISCOVERY` Schedule, due immediately, and
stores the reciprocal internal Schedule pointer on the Project/Operation.
`REVALIDATE` retains that exact identity. The public response omits the internal
Schedule ID, but a success cannot commit or replay without it; controller crash
therefore cannot leave a registered Project outside durable discovery.

Diagnostics are bounded, code-owned, non-secret records sorted by stable code
and field path. The response never includes credentials, capability bearers,
Secret References, Secret Store locations, or runner signing material.

Principal issuance, forge-installation enrollment, policy-object creation,
secret-operator principal/RBAC/purpose-policy management, fleet provisioning,
Proxmox lifecycle, and administrative RBAC management are companion management
specifications. The narrow secret-provisioning operation is part of workflow v1
and consumes those pre-existing secret authorities; this registration endpoint
only consumes its own pre-existing authorities and registers or revalidates one
Project.

## Evidence and migration

Current evidence:

- `src/orcest/fleet/config.py` stores organizations, raw credential material,
  projects, and pool configuration in host-managed fleet YAML.
- `src/orcest/fleet/cli.py` implements `fleet onboard` by editing that
  configuration and deploying a per-project stack through the Proxmox/SSH
  path.
- `src/orcest/shared/config.py` defines generated per-project orchestrator and
  runner configuration rather than repository-owned policy.
- `src/orcest/orchestrator/task_publisher.py` currently owns hard-coded prompt
  construction and worker-side PR instructions.
- `README.md` documents that onboarding currently requires fleet-host access
  and creates one Compose project per repository.

The migration retains registered infrastructure authority, provider profile
allowlists, resource limits, and explicit project onboarding. It moves
workflow policy and static prompts into a trusted, pinned repository bundle;
it does not let repository authors provision VMs, mint credentials, or change
controller authority.

Implementation must add the v1 parser/normalizer as one shared library, schema
fixtures and golden hashes, repository blob loading at an immutable commit,
profile capability validation, CLI commands, server registration boundary, and
snapshot persistence. Existing generated YAML remains the legacy engine's
input during staged rollout.

Deliberately deferred rollout and implementation validation gates:

These experiments are not prerequisites for reviewing the normative
configuration contract. Production enablement requires their recorded results:

- golden-test normalization across supported YAML libraries and operating
  systems;
- prove malicious paths, symlinks, YAML aliases/tags, duplicate keys, and
  oversized prompts fail closed;
- change `.orcest` in a Candidate and prove the stored active policy is
  unchanged;
- move the default branch between file reads and prove every blob came from one
  immutable commit;
- validate local and server CLI output against identical bundle hashes; and
- exercise authenticated onboarding without Proxmox login or repository-held
  secrets, including byte-identical replay from the durable registration
  operation, same-principal/body-conflict rejection, cross-principal RBAC
  re-evaluation, registration-revision CAS conflict with no partial update,
  and redaction/rejection of every secret-bearing request or response field.
