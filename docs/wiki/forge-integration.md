# Forge Integration

Status: Accepted normative v1 specification (2026-08-27)
Target: workflow-control v1

## Scope

This page defines the boundary between Orcest's durable workflow controller
and a source-code forge. A GitHub issue and pull request are respectively one
adapter's **Work Item** and **Change Request**. The contract is intentionally
forge-neutral: the reducer consumes typed observations and requests typed
effects; it does not call GitHub-specific commands or interpret arbitrary
webhook payloads.

The forge is authoritative for the existence and externally visible content of
Work Items, repositories, refs, Change Requests, CI checks, reviews, and merge
state. Orcest is authoritative for the admitted Run, its pre-publication work,
the exact Candidate it approved, and its intent to publish or remediate. Forge
labels, comments, Checks, and Orcest dashboards are **Projections**, never the
only record of a workflow transition.

The object identities and durable fields referenced here are defined in
[Domain model](domain-model.md). Transition selection belongs to
[Workflow lifecycle](workflow-lifecycle.md), and storage ordering belongs to
[Persistence and recovery](persistence-and-recovery.md).

## Adapter boundary

Every forge adapter MUST expose stable repository and Work Item identifiers;
mutable owner, repository, issue, and branch names are display attributes. An
adapter MUST declare capabilities at project registration and fail validation
when repository policy requires a capability it lacks.

The v1 controller requires these adapter operations:

| Capability | Required behavior |
| --- | --- |
| `list_work_items_by_labels` | Return the complete stable-ID-ordered open Work Item set matching the configured ready/working labels at one adapter search revision; each normalized member becomes a `WORK_ITEM_SNAPSHOT`. |
| `read_work_item` | Return identity, state, title, body, labels, update revision, and declared dependency data. |
| `resolve_revision` | Resolve a trusted base ref to an immutable commit SHA and read files at that exact commit. |
| `read_ref` | Return the current object ID or a typed not-found result. |
| `create_ref_if_absent` | Create a ref only when absent, or return the observed conflicting object ID. |
| `update_ref_cas` | Move a ref from one expected object ID to one new object ID without overwriting any other value. |
| `publish_commit_cas` | Make a verified Candidate commit and its reachable objects available to the source repository, then create or move one exact ref only if its expected prior value still matches. Object transfer may be retried independently; the ref mutation is a real compare-and-swap. |
| `find_change_request` | Search by exact source repository/ref and Orcest Run marker. Return either the matching object or a typed absence containing the adapter search revision and stable Change Request nonexistence token. |
| `find_change_requests_by_marker` | Return, at one adapter search revision, the complete separately stable-ID-ordered live open/unmerged and terminal closed/merged sets for exact source repository/ref/Run marker, including identity/head/body revision/marker set/state/merge commit, registered creator installation/account evidence, and normalized external-reliance evidence. The controller joins exact create checkpoint/request/Effect or durable live-association provenance and emits each member's closed ownership proof; the complete result is normalized as `CHANGE_REQUEST_SEARCH_RESULT`. |
| `create_change_request` | Create a Change Request for an exact source ref and base ref. |
| `read_change_request` | Return state, merge result, head/base object IDs, CI, reviews, and unresolved discussions. |
| `close_change_request_if_owned` | Idempotently close an open, unmerged Change Request only after its stable ID, Orcest marker, deterministic source ref, and freshly observed head prove ownership by the cancelling Run. Return a typed mismatch instead of closing an unverified object. |
| `close_change_request_if_exact_unreviewed_duplicate` | Idempotently close one exact open, unmerged redundant Change Request only when stable ID, expected head, valid Orcest marker, deterministic source ref, retained canonical live object or selected positive merged Reservation proof, frozen complete-search revision/equivalence proof, current unreviewed proof revision, Publication/effect generation, and stable controller operation identity all match. A typed mismatch returns the normalized current state/revision needed to persist exact Forge Observation evidence; ambiguity retains the operation identity for read reconciliation. Never degrade to an unconditional close. |
| `detach_run_marker_if_exact_terminal_duplicate` | CAS one exact live duplicate body from the Reservation member's head/body revision/marker-set digest to the identical body with only the selected terminal Run marker removed. Require positive ownership, non-empty external reliance, stable ID/ref/Publication/effect/Reservation/Action authority, and preserve every other byte/marker. Return a typed current Marker Observation on mismatch; never close, adopt, transfer, or rewrite external work. |
| `repair_change_request_marker_if_exact_owned` | CAS the body of the exact already-linked Change Request from one observed body revision/marker-set digest to exactly one code-derived Run/Publication marker only for `MISSING` or `DUPLICATED_IDENTICAL`; require stable ID/head/ref/Publication/effect/ownership proof and return a typed current Marker Observation on mismatch. Never adopt, transfer, or overwrite a conflicting marker. |
| `mutate_projection` | Add/remove configured labels and write idempotently marked status comments. |

Webhook delivery MAY be used as a wake-up hint. Correctness MUST NOT depend on
webhook delivery, ordering, or uniqueness. Polling and startup reconciliation
MUST be able to reproduce every required Forge Observation.

Every controller-owned Work Item, trusted-base, deterministic-ref, Change
Request, CI/feedback, initial matching-Change-Request, and complete-marker-set
read uses the Domain's durable Forge Observation Schedule and one reciprocal
Forge Observation Request/Outbox committed before I/O. The closed non-discovery request kinds
are respectively `WORK_ITEM_POLL`, `BASE_HEAD_POLL`, `REF_POLL`,
`CHANGE_REQUEST_POLL`, `CI_POLL`, `CHANGE_REQUEST_SEARCH`, and
`COMPLETE_MARKER_SEARCH`. Webhooks only make a matching Schedule due sooner;
they do not authorize an unscheduled direct read or create an Observation.
Restart redelivers every `PENDING` Request with its same adapter idempotency key
and reconstructs due work from `ACTIVE` Schedules, never Redis.

The repository-wide ready/working scan is separately
`WORK_ITEM_DISCOVERY`: one `PROJECT`-targeted Schedule/Request uses
`list_work_items_by_labels` and commits its zero-or-more
`WORK_ITEM_SNAPSHOT` results in bytewise Work Item stable-ID order. Project
registration creates that Schedule before discovery I/O. The request's
complete result membership, including empty, is durable; discovery never
depends on an unrecorded list call. In the discovery-completion transaction,
each returned Work Item gets Run-null `WORK_ITEM_POLL` and `BASE_HEAD_POLL`
Schedules before either next read. When the Project/discovery Schedule is
paused at completion, every child is created or retained `PAUSED`; completion
cannot reactivate it. The same complete set closes stale Run-null
schedules for absent Work Items with no active Run. Admission atomically closes
the admitted item's pre-Run schedules and creates their Run-bound replacements;
it cannot leave both schedule identities active.

Registration may create the discovery Schedule as `ACTIVE` during Stage-0
`MAINTENANCE`; that state records desired cadence only. The independent
Controller Mode gate prevents creating, delivering, or completing its first
Request until maintenance ends through an authenticated mode operation.

The Request's closed kind/target matrix determines which normalized
Observation kinds it may commit. Completion atomically records the ordered
result membership and Outbox delivery; a stale expected observation/scope
fence discovered from an adapter response records Request `SUPERSEDED` with no
Observation and Outbox `DELIVERED`, because I/O occurred. Outbox
`SUPERSEDED` is reserved for closure/fencing before I/O. Publication mutations retain
their stronger immutable Publication Effect/checkpoint intent; a read already
owned by such an Effect is correlated to that Effect and MUST NOT be issued a
second time through an unrelated Schedule.

Schedule state is a durable controller projection, not a Redis timer. Project
registration and the first Run state that needs a read create or CAS-reactivate
the one schedule identity. Project suspension pauses it; Project reactivation
resumes only still-required reads. Project removal, Run terminalization, or a
permanent lifecycle exit closes the applicable schedules in the same durable
writer transaction and supersedes any pending read plus its still-pending
reciprocal Outbox before closure. A paused
schedule may finish its already-pending read but creates no new one. Each state
change increments the Schedule revision, and late responses after closure are
replays of `SUPERSEDED`, never new Observations.

Run terminalization closes every ordinary Run-owned Schedule. A positive
merged-terminal selection is the closed exception: the same terminal writer
transaction creates or retains only Reservation-bound
`CHANGE_REQUEST_POLL`/`COMPLETE_MARKER_SEARCH` schedules with the selected
Effect credential/fence. They remain restart-scannable controller cleanup work
until the Reservation completes, then close with any pending Request/outbox
superseded before I/O.

Ordinary Schedule I/O is mode-gated. `RUNNING`, `INTAKE_PAUSED`,
`DISPATCH_PAUSED`, and `DRAINING` permit Request creation, delivery, and
completion subject to their ordinary fences. `MAINTENANCE` retains due
Schedules and pending Request/Outbox identities unchanged: it creates, sends,
and completes none. A response racing entry to maintenance is retried with the
same idempotency identity after exit. The only named maintenance recovery reads
are the storage/Secret integrity, backup-restore, mode/key, and Stage-0
management procedures in their own protocols; no Forge Observation Schedule
kind may label an ordinary repository poll as recovery.

GitHub-specific pagination, GraphQL node IDs, REST ETags, installation IDs, and
permission names stay inside the GitHub adapter. The controller sees typed
capability and observation records.

## Project registration and trust

A project registration binds a stable `ProjectId` to:

- a forge adapter and forge installation or account reference;
- the forge's stable repository ID and current display slug;
- a default or explicitly trusted base-ref policy;
- separate logical Secret IDs for read and controller publication authority,
  plus exact registration-provenance versions;
- configured intake and projection labels; and
- limits and an allowlist of supported repository workflow schema versions.

Repository files cannot grant forge authority, select a different forge
installation, reveal a credential, expand a project allowlist, or weaken a
server-enforced security limit. Those are authenticated management-plane
operations. The repository-owned inputs and onboarding validation contract are
specified in [Repository configuration](repository-configuration.md).

The controller MUST use separate logical credentials for source reads and
publication. A worker MAY receive an Attempt-scoped, read-only source
credential solely to fetch its pinned source. Its expiry MUST be no later than
that Attempt's `execution_deadline_ms`; alternatively the controller MAY
broker a content-addressed source archive. Materialized source credentials
MUST be removed before the model is invoked or any Candidate-owned command
runs. Workers and Candidate code never receive the publication credential. If
a forge cannot issue a sufficiently narrow and short-lived read credential,
the controller MUST broker source access without forwarding its own
credential.

Registration does not make one mutable versioned SecretRef authoritative
forever. Each accepted Attempt Claim resolves and freezes the verified current
version of the Project's logical read Secret; each immutable Publication
Effect independently resolves and freezes the current version of its logical
publication Secret. Replay of either object uses only its frozen version.
Rotation cannot rewrite an in-flight request. An unusable version enters a
typed Secret Wait whose identity names the logical Secret ID and minimum
acceptable version; recovery creates a higher Attempt or Effect generation
after the version is satisfied.

## Intake and admission

### Discovery

The configured ready label is an operator's admission request, not proof that
a Run exists. For GitHub the defaults are `orcest:ready` and
`orcest:working`. On every startup and periodic full reconciliation, the
adapter MUST scan open Work Items bearing either label. Incremental event
processing cannot replace this scan.

Discovery scans are not sufficient for admitted work. Independently of labels
or open/closed list filters, the controller MUST read every active Run's Work
Item by stable external ID on startup and periodically thereafter. That
active-ownership scan is how closure, label removal, specification edits,
dependency changes, repository transfer, and base movement remain observable.

For each discovered Work Item, the controller MUST:

1. map the forge repository and Work Item stable IDs to `ProjectId` and
   `WorkItemId`;
2. read the Work Item and resolve the configured trusted base ref to an
   immutable commit;
3. load and normalize `.orcest` policy and referenced prompt files from that
   same trusted commit;
4. verify that every declared dependency is authoritatively satisfied;
5. construct the Work Item Snapshot and its specification and workflow hashes;
6. in one SQLite transaction, choose the eligible Work-Item-targeted
   `BASE_HEAD` with the greatest accepted per-target `observation_sequence`
   before the transaction, then insert or find the unique active Run and, for a
   new Run, store the pending Snapshot, `NONE -> ADMITTED` `ADMIT` Transition,
   its exact `anchored_base_observation_id`, and working Projection intent;
7. reduce the separate `SPEC_SUPERSEDE` transaction that installs generation
   1, then the separate `INTERNAL` transaction that creates the mandatory
   `PLAN` Activity and, only if current Controller Mode plus the selected
   `ACTIVE` Capability Signing Key permit offers, its generation-1 `OFFERED`
   Attempt and outbox; otherwise leave that Activity `PLANNED` for the durable
   mode/key continuation; and
8. only after the applicable commit, dispatch each Projection or Activity
   delivery.

The unique-active-Run constraint is the admission lock. Two controller passes,
duplicate webhooks, or two labels MUST converge on the same Run rather than
enqueueing duplicate work. Unsatisfied or unknown dependencies leave the Work
Item discoverable but unadmitted and are re-observed later; they do not create
a Run or consume an implementation Attempt. Dependencies that become
unsatisfied after admission use the lifecycle's durable waiting contract.

The initial snapshot includes title and body. Comments are excluded unless the
pinned repository policy explicitly selects a supported comment-input mode.
When enabled, the mode MUST define which authors and marker-delimited comment
sections are inputs and how they are ordered and hashed. General discussion is
never silently incorporated into the implementation specification.

### Projection reconciliation

Projection mutations use the Domain's closed
`ProjectionOutbox.kind = RUN_STATUS` and outbox-backed idempotency keys.
Labels, comments, and checks
are fields of that complete desired Run-status payload, not independent
projection kinds or lifecycle authority. The adapter MUST
render a hidden marker containing the Run ID and projection kind on any Orcest
status comment. It MUST update or replace its own projection, not append an
unbounded comment history.

Startup reconciliation applies this table:

| Forge observation | Durable observation | Required action |
| --- | --- | --- |
| ready, no active Run | no prior terminal Run has a pending label-cleanup intent, or absence of ready was observed after cleanup and a later ready-add observation now exists | admit one fresh Run, then project working |
| ready, active Run | any nonterminal phase | remove ready and project working |
| working, active Run | any nonterminal phase | refresh stale projection only |
| working, no active Run | recoverable nonterminal Run or Publication exists | reconstruct scheduling and projection from SQLite |
| working, no active Run | no durable ownership exists | readmit from a fresh snapshot, or restore ready if admission validation fails |
| ready or working | terminal Run whose cleanup has not yet converged | remove workflow labels and project the named terminal result; do not readmit |

A forge failure between removing one label and adding another is harmless:
SQLite retains admission, and the next reconciliation repeats the desired
projection. The controller MUST NOT infer cancellation merely because an
Orcest label was removed.

A terminal Run's lingering ready label is never interpreted as a new request.
The controller first commits and reconciles terminal cleanup and observes the
label absent. Only a causally later authoritative ready-add event, or a later
polling sequence that observes absent and then present, is a fresh admission
request. Thus a user can deliberately re-ready a completed Work Item without
an old failed projection creating an accidental Run.

### Work Item mutation

Every full intake scan compares the current Work Item state and normalized
specification hash with the active Run's snapshot:

- Work Item closure before the Publication reaches
  `CHANGE_REQUEST_OBSERVED` cancels the Run immediately only when no durable
  `CHANGE_REQUEST_CREATE/REQUEST_READY` or `AMBIGUOUS` checkpoint exists and
  either no Publication/create workflow exists or a current search proves
  absence. If a Publication/create workflow exists without that current absence
  proof—even before a create call is ready—or once a create call may have
  reached the forge, closure records cancellation intent and reconciliation
  must prove absence or discover and close the owned Change Request before
  terminalizing. At and
  after observation, the owned Change Request—not Work Item closure—controls
  merge/non-merge termination. An authenticated Run `CANCEL` command remains
  explicit authority to request cancellation at any nonterminal state.
- Closure caused by merging the Run's linked Change Request records `MERGED`,
  not `CANCELLED`.
- After a Change Request is linked, Work Item label or open/closed projection
  drift does not replace the Change Request's merge and close authority.
- A changed specification hash creates a new specification generation at the
  safe boundary defined by the lifecycle. It never edits an existing snapshot
  or preserves Candidate approvals from the old generation.
- A moved base ref is recorded as a new observation and handled by the pinned
  base policy in the lifecycle; it does not silently change an active
  Candidate's identity.

If cancellation is requested while a Change Request exists—or a durable
`CHANGE_REQUEST_CREATE/REQUEST_READY` or `AMBIGUOUS` checkpoint means one may
exist—the transaction fences new work and records durable cancellation intent
plus cleanup outbox bound to the stable create request and Publication. The Run
remains nonterminal while the controller searches using that request identity.
Proven absence means an exact current `CHANGE_REQUEST_ABSENT` observation, not
`REF_ABSENT`, an empty response, or a timeout. It permits terminal cancellation
only through the successful pre-link `CLOSE_PUBLICATION` Controller Operation
Fact that names that observation; discovery
supersedes the search-only cleanup Activity and atomically plans one new
`CLOSE_PUBLICATION` Activity/outbox bound to the stable Change Request ID,
marker, deterministic ref, exact head Observation/head, and effect generation.
A head mismatch records a new observation, supersedes that stale Activity, and
plans another immutable head-bound cleanup rather than overwriting code or
Activity inputs. Only the current head-bound Activity may invoke
`close_change_request_if_owned`. An
authenticated unmerged-close observation produces `CANCELLED`; a merge
observation produces `MERGED`, so there is no terminal-outcome rewrite race.
Until cleanup converges the marker/ref remain reserved from the legacy engine
and cancellation-in-progress is projected. A deterministic ref may be retained
as a terminal audit artifact only after current reconciliation proves that no
open Change Request exists; it cannot be published or adopted by another Run.
The v1 adapter has no delete-ref or delete-branch capability: cancellation
never calls an implied cleanup primitive that is absent from the closed adapter
surface.

## Forge Observations

The adapter MUST normalize each external read into a Forge Observation before
the reducer sees it. An observation includes `ProjectId`, its target identity,
adapter kind, immutable forge object or revision IDs, observed head and base
SHAs where applicable, normalized state, `observed_at`, and a content digest.
The absence taxonomy is closed and typed. `REF_ABSENT` contains only the exact
deterministic Git ref and its adapter nonexistence revision/token.
`CHANGE_REQUEST_ABSENT` instead targets the Publication and contains the exact
registered source repository ID, deterministic source ref, Orcest v1 Run
marker, adapter search revision, and stable Change Request nonexistence token.
It is the only valid evidence for
`CHANGE_REQUEST_SEARCH/OBSERVED_ABSENT` and for successful pre-link
`CLOSE_PUBLICATION` absence reconciliation. The adapter MUST NOT translate one
absence kind into the other or synthesize absence from transport failure.
`CHANGE_REQUEST_SEARCH_RESULT` is the distinct complete same-marker-set
Observation: it binds registered source repository, Run/Publication, marker,
deterministic ref, adapter search revision, independently stable-ID-ordered
live open/unmerged and terminal closed/merged memberships, live-only
cardinality, and their exact full-set digest. An
individual Change Request Observation is never promoted to this kind.
For every member, the controller must emit the exhaustive Search Member
ownership union: one of the three positive proof tags with all corresponding
create/association, creator, Effect, ref, marker, desired-commit, head/body,
and digest joins, or `INCOMPATIBLE`/`INCOMPLETE` with no positive proof tag,
non-empty typed defects, and a digest of that closed evidence. A marker or
stable ID match alone is never positive.
When an observation can authorize a specification amendment or other
actor-sensitive transition, it also includes the actor's stable forge identity
and an adapter-derived authorization/association proof at that revision; a
display name is never authority. For a Human Boundary resolved by an amended
specification, that verified forge actor becomes the Human Resolution's
authenticated principal and the Resolution commits atomically with the
specification-supersession transition.
SQLite assigns an increasing per-target observation sequence when it accepts a
new snapshot. An exact adapter delivery ID is idempotent. A poll snapshot whose
digest equals the immediately preceding observation for that target MAY be
coalesced, but the same digest after an intervening observation is a new
observation with a new sequence; this preserves `A -> B -> A` changes. The
sequence, not delivery order or a forge timestamp alone, orders reducer input.

Every accepted observation applicable to an active Run is reduced exactly once
under its `forge_observation_id`; a no-op still writes a same-state Transition.
The Transition uniqueness fence spans specification generations. The
admission Work Item Observation and capture-sequence-1 trusted-base Observation
are consumed once by the same `ADMIT` Transition, with the latter named by
`anchored_base_observation_id`. Any later state decision that
consults the latest already-reduced forge projection uses an `INTERNAL`
continuation and never reuses that observation ID as a fresh trigger.
A coalesced duplicate creates no new Forge Observation and no new Transition;
it returns the existing durable observation/application result.

CI observations MUST identify the checked head SHA and each required check by a
stable adapter key. Review and discussion observations MUST identify the Change
Request head SHA for which they are valid. A status lacking that binding is
informational and cannot satisfy or block a Candidate gate.

`CHANGE_REQUEST_FEEDBACK` normalization is closed. `mergeability` is `CLEAN`,
`CONFLICTING`, or `UNKNOWN`; every configured required check has one stable key
and `PENDING`, `PASS`, or `FAIL`; review facts retain only current-head
`CHANGES_REQUESTED` decisions; and discussion facts retain unresolved threads
bound to the current head. Each review/thread fact includes its stable external
ID and authenticated author principal, and the controller derives
`author_is_orcest` from registered service identities rather than names or
prose. Collections are sorted by stable key/ID and duplicate IDs are invalid.
Any unresolved current-head thread by a non-Orcest principal is conservatively
actionable in v1; free-form text never decides whether it enters the set.

An adapter read that omits a required revision is an invalid observation. The
controller retries or diagnoses it; it MUST NOT reinterpret missing data as
success.

## Publication identity

Each Run has at most one live Publication. Its deterministic Git ref is:

```text
refs/heads/orcest/run/<lowercase-run-uuid>
```

The source repository is the registered repository unless an authenticated
project registration selects a controller-owned fork. The Change Request body
MUST contain exactly one machine marker of this form:

```html
<!-- orcest:run=<run-uuid>;publication=<publication-uuid> -->
```

The marker is an index and ownership proof only after it matches the durable
Publication, registered repository, source ref, and Change Request stable ID.
An arbitrary user-supplied marker never grants Orcest ownership. Nevertheless,
any syntactically valid Orcest v1 marker reserves the Change Request from the
legacy engine even when its Run or Publication is temporarily unknown. The
controller must then recover durable records from backups and deterministic
refs or resolve ownership under the fail-closed protocol below; unknown does
not mean legacy-owned.

Marker inspection is not the only legacy exclusion. While a v1 Publication's
Run is live, the legacy selector MUST exclude its exact associated Change
Request stable ID, when present, and deterministic source ref unconditionally;
marker state and repair phase are not predicate inputs. That exclusion
ends only when the Run is terminal or a separately specified authenticated
engine-migration transaction transfers ownership; v1 defines no such transfer.
If terminalization creates a Terminal Duplicate Cleanup Reservation, its
deterministic ref and every unresolved member remain excluded until the
Reservation resolution rule below releases them; syntactically valid markers
remain independently excluded in all cases.

The first published ref MUST point to the verified commit SHA of the exact
Candidate named by the accepted Consensus Decision. The bundle digest is a
storage address and MUST NOT be substituted for that Git identity.

On every entry to `APPROVED`, the reducer's next base-policy decision is a
`INTERNAL` continuation from the Transition that entered `APPROVED`. It reads
the latest accepted and already-reduced `BASE_HEAD`; it never reuses that
observation as a fresh trigger. This is the only pre-effect approval-time base
check. The effect then performs the fresh `BASE_READ_PRE` and `BASE_READ_POST`
observations below.

“Latest” is the eligible trusted-base observation consumed at the greatest Run
Transition sequence across both Work Item and Publication observation targets.
Adapter timestamps and independent per-target observation sequences never
break that cross-target order.

## Idempotent publication protocol

Publication is a reconciled sequence of durable intents and external effects.
It is not one distributed transaction.

1. The reducer records `Publication(state=PLANNED)`, the approved Candidate
   SHA, deterministic ref, base ref/SHA, frozen base-movement policy, a monotonically increasing
   `effect_generation`, its current controller `PUBLISH` Activity, and a
   publication outbox action. Every effect and observation in this sequence
   binds that generation.
2. Immediately before a first ref or Change Request mutation, the reconciler
   resolves and records the base ref again as `BASE_READ_PRE`. Under
   `rebase-before-publication`, a base SHA different from the Candidate's
   approved base aborts this effect generation and returns to rebase plus the
   full Candidate gate. Under `pin`, any successful trusted read records
   `OBSERVED_SATISFIED` regardless of equality and the effect continues. Under
   `supersede-at-boundary`, a difference records `BASE_MISMATCH`, aborts the
   effect before mutation, captures a base-only Snapshot whose supersession key
   covers the new commit, and schedules its separate canonical
   `SPEC_SUPERSEDE` installation at the safe boundary. The observation
   Transition never installs it.
3. The effect reconciler records `REF_READ` for the publication ref. If the
   observed value equals the effect's desired SHA, object transfer and the ref
   effect already succeeded. Otherwise it may mutate only when the observed
   value equals the immutable effect expectation: explicit absence records
   `REF_CREATE/REQUEST_READY`; a concrete expected commit records
   `REF_UPDATE/REQUEST_READY`. It then invokes
   `publish_commit_cas(expected=effect expectation, desired=approved SHA)`.
   Any other value is a compare-and-swap mismatch, never overwritten blindly.
4. After observing the desired ref, the controller conditionally commits that
   observation only if the same `effect_generation` is current,
   and advances the Publication to `BRANCH_OBSERVED`.
5. Before initial linkage, the reconciler runs `COMPLETE_MARKER_SEARCH` by exact
   source repository/ref/Run marker and records the complete search revision,
   semantic full-set digest, separately ordered live and terminal memberships,
   live-only actionable cardinality, and conditional retained-lowest ID. Its
   Request and resulting Observation copy the current `INITIAL` Effect
   generation, `PUBLISH` Activity, and operation digest; an ordinary
   post-link marker search has no such effect binding and cannot satisfy this
   checkpoint. Each member includes the complete code-owned ownership proof.
   Any positive owned merged terminal member takes precedence at every live
   cardinality: select the bytewise-lowest merged ID, terminalize `MERGED`, and
   create the durable Reservation for all live members. Absent that stronger
   fact, `INCOMPATIBLE` follows exact ownership reconciliation/conflict and
   `INCOMPLETE` rereads/backs off without association. Only after those guards,
   with zero live and no terminal member it follows the ordinary
   exact `CHANGE_REQUEST_SEARCH` path, optionally records
   `CHANGE_REQUEST_CREATE/REQUEST_READY` after exact absence, and creates using
   the persisted title/body/base; discovery, create success, and ambiguous
   response reconciliation all loop to a fresh complete marker search and
   never link directly. With one live member it freezes that sole ID and reads
   the exact object. With multiple live members it freezes the bytewise-lowest ID only as a
   pre-link retained candidate, suspends the current `PUBLISH` mutation,
   completes duplicate reconciliation/eligible closes one object at a time,
   and repeats the complete search until exactly one live member remains. A
   pre-link `NO_ACTIONABLE_DUPLICATE` is invalid: positive external reliance
   produces the exceptional exact ownership conflict, while unavailable proof
   waits/retries autonomously without choosing an object. Terminal members are
   positive closed audit membership and never block a live set from converging
   to one. With zero live and positive closed terminal members, the reconciler
   never creates: it selects the bytewise-lowest closed ID, stores the exact
   association/search/member proof, and reduces `CLOSED`.
   No many-set member is durably linked or granted merge/close authority.
6. Only after a fresh complete search proves exactly one live member and an exact-object
   observation matches it does the controller commit that stable forge ID,
   number/URL projection, and head SHA as a provisional linked effect in
   Publication state `CHANGE_REQUEST_OBSERVED`. Merge/close observations seen
   while zero/many is unresolved are retained same-state; only a later complete
   search can exercise positive-merged precedence. After linkage a fresh
   exact-object poll is ordinary terminal authority. The initial effect is
   not yet complete and the Publication is not `ACTIVE`.
7. The reconciler resolves the base ref again after Change Request creation and
   records `BASE_READ_POST`.
   If it moved during the unavoidable external race, the controller records
   that observation. Under `rebase-before-publication` the linked object stays
   owned, but the current initial effect is superseded and the Run returns to
   exact-base remediation and all Candidate gates. Under `pin`, the successful
   read is `OBSERVED_SATISFIED` regardless of equality and may complete the
   effect. Under `supersede-at-boundary`, the linked object likewise stays
   owned, the effect is superseded, and a base-only pending Snapshot is captured
   before a separate `SPEC_SUPERSEDE` Transition installs it and replans.
   Either non-PIN path later creates a
   higher-generation `INITIAL` effect that compare-and-swaps the already-owned
   provisional ref and reconciles the same Change Request. The stale approval
   is never treated as a gate on the new base.
8. Only after the second base read is `OBSERVED_SATISFIED` under its exact
   policy does the controller append the completed initial-effect checkpoint,
   reduce ownership and initial head, and set the Publication `ACTIVE`.
   Monitoring begins from that committed link. Repeating any earlier step
   converges on the same ref and Change Request.

Steps 2 and 7 are safeguards for initial publication only; a provisionally
observed Change Request in step 6 is not yet an active Publication. After step
8 makes the Publication `ACTIVE`, later base movement arrives as an ordinary
ordered Forge Observation and does not restart this pre-publication base
check. Post-publication head updates remain fenced solely by the exact observed
head and current Publication effect generation. The controller does not rerun
the initial-create sequence for every remediation update.

`publish_commit_cas` is controller-side Git publication, not a worker push.
The controller imports the Candidate's verified one-tip bundle into a
quarantined local repository, verifies its object format and approved commit,
fetches the registered base as needed, and uploads only the required reachable
objects using isolated Git configuration with repository hooks and local
protocol shortcuts disabled. It then uses an adapter-native atomic compare and
swap or a push with an exact force-with-lease expectation for the one
deterministic ref. A crash after object upload but before ref movement is safe:
repeating object upload is idempotent and the lease still protects the ref.
Implementations MUST size-test bundles, partial object transfer, protected-ref
behavior, and hook-free isolation before enabling an adapter.

If initial publication instead observes a foreign SHA on the deterministic
ref, the controller records that head as a Forge Observation, supersedes the
stale Publication Effect, and runs controller `RECONCILE`. A matching durable
checkpoint may prove an earlier effect. Otherwise, if the ref remains in the
registered repository, has no incompatible owner, and the object can be
safely fetched and validated, `RECONCILE` plans controller `IMPORT`; the exact
commit becomes a pre-link Candidate, traverses the full gate, and a later
approval creates a higher-generation `INITIAL` effect with the foreign SHA as
its compare-and-swap expectation. If safe fetch succeeds but pinned-base or
Candidate admission validation fails, the typed
`PRELINK_REF_RECONSTRUCT_REQUIRED` result selects one evidence-bound
`RECONSTRUCT_FOREIGN_HEAD` remediation that produces an ordinary base-rooted
Candidate and traverses the full gate; it never loops through `IMPORT` for the
same evidence. Incompatible ownership evidence follows
`PUBLICATION_OWNERSHIP_CONFLICT`; missing/corrupt objects follow ordinary
recovery. No path remains indefinitely in `PUBLISHING`, treats the foreign SHA
as approved, or force-overwrites it.

The complete same-marker read first commits a
`CHANGE_REQUEST_SEARCH_RESULT` Forge Observation containing its adapter search
revision, canonical ordered live and terminal identity/head/state/reliance
memberships, each member's exact body/marker evidence and closed
`POSITIVE`/`INCOMPATIBLE`/`INCOMPLETE` ownership proof, and exact full-set
digest. `POSITIVE` is permitted only when the controller can join every
registered creator, create checkpoint/request or durable live association,
Effect generation, deterministic-ref, marker, desired-commit, and observed-head
binding. Positive contradiction is `INCOMPATIBLE`; missing or temporarily
unavailable proof is `INCOMPLETE`, never inferred ownership. An individual
discovery/head/feedback/merge/close observation may
invalidate an in-flight proof and schedule this read, but cannot invent
completeness. Only a search-result pair different from the Publication's last
pair plans a duplicate `RECONCILE`; the producing Fact must copy that pair.

One or more `TERMINAL/MERGED/POSITIVE` members override every live-cardinality
branch. The controller selects the bytewise-lowest stable merged ID, commits
the exact Publication association and merge proof, fences all semantic and
publication work, terminalizes the Run `MERGED`, and atomically creates the
Domain's Terminal Duplicate Cleanup Reservation for every live member. This
precedence applies before initial linkage, after linkage, during remediation or
gating, and while cancellation is reconciling: Orcest is recognizing an
already-authoritative forge merge, not initiating one. Without such a merged
member, any `INCOMPATIBLE` proof takes the typed ownership-conflict path and any
`INCOMPLETE` proof performs another durable read/search/backoff with no
association or mutation. Positive closed terminal members are audit-only while
live members remain; with zero live, the bytewise-lowest positive closed ID is
the ordinary terminal selection.

The terminal Reservation is processed after Run terminalization in canonical
member order. A positive-owned member with canonical-empty external reliance
may call `close_change_request_if_exact_unreviewed_duplicate`; a positive-owned
member with reliance may call `detach_run_marker_if_exact_terminal_duplicate`
under its exact body/head/marker CAS so external work remains untouched. An
incomplete, incompatible, or CAS-unsafe member is retained and receives only a
bounded `RETAINED_AUDIT` Action. Every mutation Action/outbox commits before
I/O under the selected Publication Effect fence. A response mismatch first
commits normalized Observation evidence and a higher proof/action generation;
ambiguity retains the same Action operation identity. Cleanup never changes
the selected merged ID/commit or reopens the Run.

If, absent positive merged precedence, reconciliation discovers multiple controller-created Change Requests for
the same Publication and exact ref, whether during pre-link publication or
post-link monitoring, it freezes one
`REDUNDANT_PUBLICATIONS_PROVEN` Reconciliation Fact from a complete search and
current live identity/head/feedback observations. Terminal membership remains
part of the causal full-set digest but is never cleanup membership. The ordinal-zero member is the
bytewise-lowest adapter-normalized stable forge ID. Post-link it is also the
retained Publication association; pre-link it is only the frozen retained
candidate and does not populate that association. Every other member must be
provably equivalent, open,
unmerged, and without non-Orcest review, discussion, merge, or other external
reliance. The reducer plans at most one `CLOSE_REDUNDANT_PUBLICATION` Activity
for the first redundant member. That Activity and its ordinary Activity outbox
commit before `close_change_request_if_exact_unreviewed_duplicate` is called;
it creates no Publication Effect generation.

Immediately before the call, the adapter/controller re-read must still prove
the retained-lowest rule, current Publication/effect generation, exact marker,
ref, both heads, equivalence digest, and target unreviewed revision. A changed
set, head, marker, review, or revision is first normalized into exact Forge
Observation evidence; that observation's Transition supersedes the Activity
without closing and schedules a fresh complete marker search. Only the later
changed `CHANGE_REQUEST_SEARCH_RESULT` may plan fresh reconciliation. A definitive
evidence-less adapter failure uses a failed Controller Operation Fact, while
an ambiguous response reconciles by the same stable Activity operation
identity. Success is only an authenticated
`CHANGE_REQUEST_CLOSED` Forge Observation carrying the exact cleanup Activity
and operation digest; it completes that Activity but cannot close the retained
Publication or terminate the Run. The controller then performs another fresh
complete marker search; only its changed typed search-result Observation may
plan reconciliation rather than treating the old Fact as current proof. It MUST NOT
close an object when authorship or equivalence is uncertain. The typed
`PUBLICATION_OWNERSHIP_CONFLICT` human boundary is permitted only when
reconciliation records positive incompatible ownership evidence—not from a
duplicate marker, uncertainty, absence, timeout, or forge unavailability.
Unproven duplicates continue typed reconciliation/wait without overwrite. Its decision packet carries the
conflicting stable IDs and the single v1 choice to affirm Orcest ownership of
the exact Project/ref/Change Request/marker/Publication/effect binding. A
legacy handoff or marker transfer is not a packet choice in v1.

After durable linkage, a complete search with no safely closable live member
produces immutable `NO_ACTIONABLE_DUPLICATE`, never a cleanup or Human
Boundary. It freezes the
adapter search revision and digest of both complete normalized live and
terminal same-marker memberships,
and the Publication remembers that pair. Repeated monitoring with the same
pair plans no new `RECONCILE`; only a changed revision or set digest may.
Merge/close observations for a non-retained same-marker object are consumed as
duplicate evidence and schedule a fresh complete search, but the individual
observations never terminalize the retained Publication/Run. The later complete
result can apply positive-merged precedence. A relevant observation received
while duplicate `RECONCILE` is active supersedes that stale search Activity;
only its later complete `CHANGE_REQUEST_SEARCH_RESULT` may plan the replacement.

Marker repair is distinct from duplicate cleanup. An exact
`CHANGE_REQUEST_MARKER` Observation may plan `REPAIR_RUN_MARKER` only for the
already-linked current Change Request when its marker set is `MISSING` or
`DUPLICATED_IDENTICAL`, every durable Project/ref/stable-ID/head/Publication/
effect binding agrees, and no incompatible v1, legacy, or human ownership
claim exists. The Activity/outbox and `orcest.run-marker-repair/1` proof commit
before `repair_change_request_marker_if_exact_owned`. Pre-call and adapter CAS
compare stable ID, head, ref, body revision, marker-set digest, and ownership
proof. Mismatch persists a fresh Marker Observation; ambiguity retains the same
operation. Only a controller-bound Marker Observation proving exactly one
code-derived desired marker completes the Activity. This never adopts an
unlinked object or transfers ownership.

A crash after ref creation, Change Request creation, or the final SQLite update
therefore causes discovery and reconciliation, not duplicate publication.

## Change Request monitoring

Once linked, the Run remains active. The controller periodically records
head-SHA-bound CI, review, discussion, mergeability, and open/merged/closed
observations. The lifecycle turns those observations into wait, remediation,
or terminal decisions.

Every ordinary entry to `PR_MONITORING` also performs a complete exact
source-ref/Run-marker Change Request search. An observation for a non-retained
same-marker object during another post-link state is recorded once but cannot
interrupt the active gate or authorize close; this monitoring search later
rebuilds the complete duplicate proof. Cancellation has precedence and uses
its separately fenced owned-close path.

Every accepted head observation is a global post-link fence, not merely a
`PR_MONITORING` event. If it advances during verification, review,
aggregation, adjudication, remediation, recovery, waiting, approval,
publication, or an exceptional boundary, the reducer supersedes all old-head
work/evidence and enters `PR_REMEDIATING` to import and fully gate the observed
head. Merge, close, and cancellation-race precedence remains as defined by the
lifecycle. Base-only movement after Publication `ACTIVE` is observation-only;
under `SUPERSEDE_AT_BOUNDARY` it cannot capture or install a Snapshot unless a
separate specification/workflow/policy input changed.

For a run-owned Change Request:

- required CI success is valid only for the currently observed head SHA;
- `mergeability = CONFLICTING` takes precedence and produces exactly one
  exact-head-fenced `REBASE` using the latest trusted base observation;
- otherwise, any failing configured required check, current-head
  `CHANGES_REQUESTED` review, or unresolved current-head thread from a
  non-Orcest principal produces one `PR_REMEDIATE` input containing the
  complete canonical fact set; direct prompt text never controls this mapping;
- transient forge or CI unavailability waits and retries without lowering
  gates;
- merged records the merge commit when supplied and terminates the Run as
  `MERGED`; and
- closed without merge terminates as `CANCELLED` when the durable cancellation
  intent is pending, otherwise as `CLOSED`, unless a previously observed merge
  is merely awaiting consistent projection.

Comments and Checks MAY expose workflow progress but cannot be the durable
receipt ledger. Their retention, mutation, or deletion does not alter a Run.

## SHA-fenced remediation

Every post-publication remediation Activity pins an
`observed_change_request_head`. The
worker reads that revision and returns a new Candidate; it never pushes. Before
updating the publication ref, the controller MUST verify that:

1. the Candidate passed the configured verification and consensus gates;
2. the publication row still names the same Change Request and expected head;
3. the forge ref still equals `observed_change_request_head`; and
4. the current controller `PUBLISH` Activity and Publication
   `effect_generation` still own the mutation.

The adapter then performs
`publish_commit_cas(expected=observed_change_request_head,
desired=approved_candidate_sha)`. A mismatch is a normal concurrent-mutation
observation. It rejects the stale effect, records the newly observed head, and
returns to reconciliation without consuming the old approval on the new head.

When an external actor changes the run-owned ref, Orcest MUST NOT overwrite the
change. The default v1 policy plans a controller `IMPORT` Activity bound to the
exact Forge Observation. Using its source-read credential, the controller
fetches the observed commit, constructs and validates a one-tip bundle, proves
the pinned-base relationship required by Candidate admission, and admits a
Candidate with `FORGE_IMPORT` provenance rather than fabricating a worker
Attempt. It then reruns all gates. Transient fetch/capability failure follows
ordinary recovery; object corruption follows storage integrity recovery. A
safely fetched pre-link foreign commit that fails the required pinned-base
relationship without positive incompatible ownership evidence produces
`PRELINK_REF_RECONSTRUCT_REQUIRED`, never another `IMPORT` retry. The
controller plans one `REMEDIATE` Activity bound to the exact foreign-ref
observation, typed validation failure, and pinned base; its brokered read-only
source contains the observed tree/evidence, and it must produce a new ordinary
Candidate rooted at that base. That Candidate traverses the full gate before a
higher `INITIAL` effect. Only positive incompatible ownership evidence produces
`OWNERSHIP_CONFLICT` and may reach the exact
`PUBLICATION_OWNERSHIP_CONFLICT` boundary.

## Engine ownership and legacy coexistence

The new run engine owns a Change Request only when all of these facts agree:

- its marker parses and names a durable Run and Publication;
- the recorded stable forge repository and Change Request IDs match;
- the recorded source ref matches; and
- the project is enabled for workflow-control v1.

The legacy PR loop MUST exclude every Change Request with any syntactically
valid Orcest v1 marker before applying CI, review, rebase, or merge behavior,
including a marker whose durable Run is not currently found. It also
unconditionally excludes the exact stable Change Request ID/ref associated
with every live Orcest Publication until terminal outcome or the separately
specified migration; marker state and repair phase are not predicate inputs.
After a positive merged-terminal selection, that exclusion continues for the
Reservation's deterministic ref and every unresolved live member even though
the Run is terminal. A member is released only after exact owned close,
exact-CAS marker detach, or bounded `RETAINED_AUDIT`; any syntactically valid
marker continues to exclude it independently. Terminal cleanup authority is
not an engine handoff and does not permit the legacy loop to race the
Reservation.
Conversely, the new engine MUST
ignore any other unmarked Change Request unless an explicit migration
transaction first adopts it. For a valid but unknown or mismatched marker, the
new engine searches the primary store, verified backups, deterministic ref,
projection history, and registered stable identities. Until that succeeds,
both engines fail closed and publication remains blocked. If autonomous
recovery proves one owner it repairs the durable association; only irreducible
incompatible authority evidence creates
`PUBLICATION_OWNERSHIP_CONFLICT`. This predicate and recovery classifier are
shared code, not two subtly different filters.

The only v1 ownership resolution selects `ORCEST_V1` and repeats the exact
registered Project, deterministic ref, observed Change Request ID, Orcest v1
marker, Publication, and immutable effect generation. The controller verifies
all of those bindings before reconciliation and still performs no blind
overwrite. Selecting the legacy engine, removing or transferring an Orcest v1
marker as an ownership handoff, or handing a marked Change Request to the legacy loop is outside v1;
such a handoff would require a separately specified migration protocol.
The post-merge Reservation's exact-CAS detach from a non-selected relied-on
duplicate is cleanup, not a selectable engine transfer.

## Error classes and autonomous response

| Adapter outcome | Controller response |
| --- | --- |
| timeout, rate limit, unavailable | for a durable read/search/poll Request, persist the source-unique Forge Request Failure Fact for its pre-I/O attempt ordinal, then follow its closed Run-bound recovery or pre-admission/cleanup retry path |
| unauthenticated, expired credential | rotate or refresh the SecretRef, then retry |
| forbidden by current installation scope | reconcile registration and available authority; escalate only if new authority must be granted |
| not found | full identity reconciliation before treating an object as deleted |
| validation or capability mismatch | reject admission/configuration with an actionable projection |
| CAS mismatch | record new observation and replan; never force overwrite |
| ambiguous write response | read and reconcile by deterministic identity |
| positive incompatible ownership | autonomous reconciliation first; use the typed ownership boundary only after incompatibility is proven |
| unverifiable duplicate | retain identities and wait/reconcile; never infer conflict from uncertainty |

Retry counts and elapsed time alone never produce `needs-human`.

An adapter transport outcome is not itself a Health Observation. When a
server-owned connectivity probe is needed for scheduling or recovery, the
controller first resolves the verified current version of the Forge Instance's
logical `credential_secret_id`, then commits a `FORGE_CONNECTIVITY` Health
Probe Request plus outbox with that exact version and canonical subject
bindings before I/O. Completion commits the reciprocal Health Probe Fact and
its `AVAILABLE` or `UNAVAILABLE` Health Observation atomically; Request, Fact,
Observation, scope hash, subject bindings, and all digests carry the same
frozen Secret Reference. Raw callbacks, arbitrary adapter errors, and timeouts
cannot write health authority directly. Before each scheduled read/search/poll
transport attempt, the writer commits the Request's next attempt ordinal. A
timeout, rate limit, or temporary-unavailable result atomically inserts or
replays the Domain's Forge Request Failure Fact, updates the still-`PENDING`
Request's retry projection, and leaves its reciprocal Outbox pending. An
active Run reduces that exact Fact once to `FORGE_TRANSIENT` recovery; Project
discovery, pre-admission work, and terminal cleanup retry the same Request
without a Run Transition. A successful or superseded Request wins the same
state CAS and rejects a late failure Fact. No branch abbreviates the failure
into a synthetic Forge Observation, Health Observation, or Health Probe Fact.

## Security requirements

- Publication credentials MUST be controller-only versioned SecretRefs of the
  Project's registered logical publication Secret, frozen per Effect, with the minimum
  repository permissions required by the adapter.
- Candidate execution MUST occur without forge-write, controller-write, or
  unrelated-Run credentials.
- Forge payloads, comments, CI logs, and review text are untrusted input. They
  MUST be size-bounded and kept in data fields rather than interpolated into
  executable commands.
- Branch names, markers, repository IDs, and URLs MUST be constructed or
  validated by controller code.
- Logs and projections MUST identify SecretRefs only and MUST redact tokens,
  authenticated clone URLs, and signed capability URLs.
- The controller MUST audit every publication/ref mutation with the expected
  SHA, observed outcome, effect generation, and adapter identity.

## Evidence and migration

Current evidence:

- `src/orcest/orchestrator/issue_ops.py` discovers `orcest:ready` issues but
  relies on Redis locks, pending markers, and attempt counters for active-work
  state.
- `src/orcest/orchestrator/task_publisher.py` currently renders worker
  instructions that push a branch and open a PR, so publication authority is
  inside the worker boundary.
- `src/orcest/shared/models.py` currently places the forge token, provider
  credential, and rendered prompt in the Redis Task payload.
- `src/orcest/orchestrator/pr_ops.py` and
  `src/orcest/orchestrator/loop.py` already bind much PR decision state to a
  `head_sha` and use the forge's expected-head support for some mutations.
- `src/orcest/orchestrator/gh.py` centralizes GitHub reads and mutations and is
  the starting point for the capability-checked adapter.

Retained guarantees include head-SHA stale-work rejection, bounded projections,
dependency checks, and poll-based recovery from missed forge events. Replaced
guarantees are Redis-only issue ownership, worker-side publication, and a
legacy loop that can consider every open PR.

Implementation must add stable forge-ID mappings, observation and Publication
rows, outbox-backed projection/effect reconciliation, controller-only ref and
Change Request APIs, the marker ownership predicate, scans of both ready and
working labels, and legacy-loop exclusion.

Deliberately deferred rollout and implementation validation gates:

These experiments are not prerequisites for reviewing the normative adapter
contract. Production enablement requires their recorded results:

- prove each supported forge adapter's ref update is a real compare-and-swap,
  including branch protection and fork behavior;
- test ambiguous GitHub ref and PR creation responses in a sandbox;
- prove `REF_ABSENT` can satisfy only ref-read absence, while an exact
  `CHANGE_REQUEST_ABSENT` repo/ref/marker/search-revision/nonexistence fact is
  required for Change Request search and pre-link cancellation completion;
- verify stable repository identity across rename and transfer;
- exercise pagination and permission changes during full reconciliation; and
- crash after each external publication effect and prove one ref and one
  Change Request are recovered.
