# Deterministic Workflow Lifecycle

> **Status:** Accepted normative v1 specification (2026-08-27)
>
> **Canonical owner:** Run state graph, reducer semantics, legal transitions,
> specification and base changes, cancellation, autonomous recovery, and
> exceptional human escalation.

This page defines how the [domain objects](domain-model.md) advance within the
[workflow-control architecture](architecture.md). Worker transport and claim
details belong to the [worker protocol](worker-protocol.md); verification and
review evidence belong to [review and consensus](review-and-consensus.md).

The requirements on this page are normative for workflow-control v1.

## Determinism contract

The lifecycle is a code-owned reducer:

```text
reduce(current durable state, one persisted trigger) ->
    next state, durable records, next Activity plans, Projection intents
```

Given the same persisted inputs, receipts, and ordered Forge Observations, the
reducer MUST emit the same Transition and next Activity plan. Agent output may
vary, but it reaches the reducer only after the controller validates it into a
typed Attempt Result, Candidate, Verification Receipt, Review Receipt,
diagnosis, plan, or adjudication receipt. Agent prose cannot select a state,
accept a Candidate, waive a finding, change policy, or request a person.

### Reducer transaction

For every trigger the controller MUST:

1. begin a SQLite write transaction and load the Run, current Snapshot,
   current Candidate, applicable receipts, Activities, Attempts, Publication,
   and prior Transition for the trigger;
2. return the prior result if the trigger identity was already reduced;
3. validate specification generation, Candidate, Attempt fence, and Forge
   Observation bindings;
4. canonicalize unordered inputs by their stable domain keys;
5. apply the state and policy tables on this page;
6. append exactly one Transition, update the Run projection, and insert every
   new Activity, `OFFERED` Attempt, and Outbox record atomically; no trigger,
   including admission, may append a compound pair; and
7. commit before dispatching a message, invoking a worker, mutating the forge,
   or repairing a Projection.

The trigger idempotency identity is:

```text
(run_id, trigger_kind, trigger_id)
```

It is independent of specification generation for every trigger kind.
`Transition.specification_generation` audits the generation evaluated by the
first reduction; it cannot make the same causal input consumable again after
`INTERNAL`, `SPEC_SUPERSEDE`, or any other generation advance. For `ADMIT` and
`FORGE_OBSERVATION`, an additional cross-kind identity is
`(run_id, forge_observation_id)` across both kinds, so one observation cannot
authorize both admission and a later Run reduction.

The reducer MAY perform consecutive internal reductions until it reaches a
state waiting for a persisted external input. It MUST record each reduction as
a separate Transition, so a crash at any boundary resumes unambiguously.

Every accepted Forge Observation applicable to an active Run is scheduled for
exactly one reduction across the Run's lifetime. If it changes no state, the
reducer appends a same-state `T(FORGE_OBSERVATION,
forge_observation_id)` Transition; it does not leave the observation available
for a later state to consume again. A later decision based on the latest
already-reduced forge state is a deterministic `INTERNAL` continuation from
the Transition that entered that decision state.

## State graph

```text
                          +---------------------------+
                          |                           |
                          v                           |
ADMITTED -> PLANNING -> BUILDING -> VERIFYING -> REVIEWING
               ^             ^          |             |
               |             |          |             v
           REPLANNING <- DIAGNOSING <- REMEDIATING <- AGGREGATING
               |                        ^       |       |
               +------------------------+       |       +-> ADJUDICATING
                                                |                 |
                                                +-----------------+

AGGREGATING -> APPROVED -> PUBLISHING -> PR_MONITORING
                                           |       |
                                           |       +-> MERGED
                                           |       +-> CLOSED
                                           v
                                    PR_REMEDIATING
                                           |
                              verify configured gate
                                           |
                                           +-> PUBLISHING

Any active state -> RECOVERING -> prior/next strategy state
Any active state -> WAITING -> recorded resume state
Exceptional boundary -> NEEDS_HUMAN -> RECOVERING or REPLANNING
Explicit cancellation -> CANCELLED
```

`MERGED`, `CLOSED`, and `CANCELLED` are terminal. Every other state, including
`WAITING` and `NEEDS_HUMAN`, is active.

## State definitions

| State | Meaning | Permitted outstanding work |
| --- | --- | --- |
| `ADMITTED` | Restartable admission boundary: either capture-sequence-1 is pending before generation 1, or generation 1 is installed before initial planning. | Exact next `SPEC_SUPERSEDE` or `INTERNAL` planning continuation only. |
| `PLANNING` | A structured implementation plan is being produced or validated. | One current `PLAN` Activity. |
| `BUILDING` | The first Candidate for the current plan is being produced. | One current `BUILD` Activity. |
| `VERIFYING` | Required deterministic checks are running on the exact current Candidate. | The single `VERIFY` Activity containing all ordered `default` commands. |
| `REVIEWING` | Independent reviewers are producing receipts for the exact current Candidate. | One `REVIEW` Activity per unfilled panel slot. |
| `AGGREGATING` | The controller is deterministically computing consensus from a closed receipt set. | No model Activity; reducer work only. |
| `REMEDIATING` | A replacement Candidate is being produced for exact findings or a safely fetchable pre-link foreign publication ref. | One current `REMEDIATE`, `REBASE`, or controller `IMPORT` Activity. |
| `DIAGNOSING` | A worker is explaining repeated non-progress or conflicting evidence. | One `DIAGNOSE` Activity. |
| `REPLANNING` | A new structured plan is being produced without weakening gates. | One `REPLAN` Activity. |
| `ADJUDICATING` | Independent evidence is being gathered for disputed blockers or review conflict. | The sole `ADJUDICATE` Activity for slot `default`. |
| `APPROVED` | The exact current Candidate satisfies every pre-publication gate. | No worker Activity. |
| `PUBLISHING` | The controller is reconciling initial publication or a fenced update, or has fenced/superseded that Activity while an eligible pending Snapshot awaits its separate installation. | One current `PUBLISH` Activity; while it is suspended at a pre-link `COMPLETE_MARKER_SEARCH/MULTIPLE` checkpoint, at most one subordinate duplicate `RECONCILE` or `CLOSE_REDUNDANT_PUBLICATION`; or no current `PUBLISH` only in the durable pending-`SPEC_SUPERSEDE` subphase with `pending_snapshot_id` non-`NULL` and only that continuation eligible. |
| `PR_MONITORING` | A linked Change Request exists and the forge owns current CI/review/merge facts, or cancellation is reconciling a create request whose external result is still unknown. | Observation polling/webhook reconciliation; one `CLOSE_PUBLICATION` Activity while cancellation is current; or, only without cancellation/publication mutation, one `RECONCILE`, `CLOSE_REDUNDANT_PUBLICATION`, or `REPAIR_RUN_MARKER` repair Activity. |
| `PR_REMEDIATING` | Orcest is producing and gating a Candidate for exact PR feedback/head revision. | `PR_REMEDIATE` followed by configured verification/review Activities. |
| `RECOVERING` | The reducer is selecting or applying the next autonomous recovery strategy. | At most one recovery Activity chosen by the ladder. |
| `WAITING` | Progress is paused for a typed temporary condition and has an exact resume state and wake condition. | Timed wake-up or observation only. |
| `NEEDS_HUMAN` | An allowlisted exceptional boundary is proven and a minimal decision packet is published. | Boundary observation and Projection repair only. |
| `MERGED` | A positive-owned run Change Request was observed merged or selected by a complete marker search. | No semantic Run work. A controller-owned Terminal Duplicate Cleanup Reservation may continue its exact post-terminal actions and audit Transitions. |
| `CLOSED` | The run-owned Change Request was observed closed without merge. | None. |
| `CANCELLED` | Explicit cancellation or eligible pre-publication closure cancelled the Run. | None. |

## Admission

### Eligibility

The forge adapter produces an admission observation only when all configured
intake predicates hold, initially:

- the Project is `ACTIVE`;
- the Work Item is open;
- the Work Item carries the configured ready projection;
- no terminal or explicit cancellation projection applies;
- every declared dependency is observed closed or otherwise satisfied; and
- the trusted base revision and `.orcest` configuration can be resolved and
  validated.

An unknown dependency state is not eligible. It is re-observed later; it is not
a human boundary.

### Admission transaction

The controller MUST perform, in one transaction:

- the unique active Run in `ADMITTED`;
- capture-sequence-1 Work Item Snapshot as `pending_snapshot_id`;
- Transition 1 from no Run to `ADMITTED`; and
- an idempotent Projection intent to replace `orcest:ready` with
  `orcest:working`.

The partial unique active-Run constraint, not the forge label, arbitrates
concurrent admission. An existing active Run makes another admission a no-op.

The uniquely scheduled next writer reduction is
`T(SPEC_SUPERSEDE,snapshot_id)`. It installs generation 1 and clears the
pending pointer; only then does `T(INTERNAL,spec_transition_sequence)` enter
`PLANNING` and create the initial `PLAN` Activity. It creates the Attempt/
outbox only when current mode and selected `ACTIVE` issuance key permit offers;
otherwise the Activity remains `PLANNED` and this Transition's dispatch
continuation remains pending. Thus every
Snapshot installation, including generation 1, has one separately replayable
`SPEC_SUPERSEDE` Transition. `ADMIT` never installs a Snapshot, and an
`INTERNAL` Transition never installs one. A projection error leaves a real
active Run and schedules label repair.

Startup and periodic reconciliation MUST scan both ready and working
projections. A working Work Item with no active Run is processed as follows:

1. attach it to an existing run-owned Publication when the Run Store was
   restored and the forge side effect proves the identity;
2. restore a missing Run from the supported backup/recovery path when durable
   records exist there; or
3. readmit it from a fresh Snapshot when it remains eligible and no Run or
   Publication exists.

It MUST NOT be left permanently invisible because `orcest:ready` is absent.

## Happy-path transitions

In the table below, `T(kind,id)` means the canonical trigger idempotency tuple,
and “plan X” means insert the new Activity in `PLANNED`, then offer its
eligible generation only when current Controller Mode, capacity, and the
selected `ACTIVE` Capability Signing Key permit it. The offer operation
atomically changes `PLANNED -> READY` and inserts the generation-1 `OFFERED`
Attempt and Outbox row. When a gate is closed, planning commits the Activity
as `PLANNED` with no Attempt or Outbox and retains the entering Transition's
single pending dispatch continuation; the mode/key/capacity reconciler later
performs the same serialized recheck. Controller-only Activities follow their
separate `READY -> ACTIVE` rule and never have Attempts.
Planning `REVIEW` or `ADJUDICATE` additionally inserts the exact Activity
Review Assignment, complete ordered Activity Review Subject membership, and
complete adjudication-finding membership in that same transaction. Its
`assignment_digest` covers `subject_refs_digest` and is part of
`Activity.semantic_input_digest`; the worker protocol `review_slot` is only a
projection of this durable assignment and memberships.

Every transition directly caused by an accepted worker Result uses
`T(ATTEMPT_RESULT, attempt_id)`. A Candidate or Receipt admitted with that
Result is a durable reducer effect and may be named in the Transition output,
but its content digest or object ID is not a competing trigger identity.

Every first accepted durable trigger in the Domain's closed mapping produces
exactly one Run Transition, even when its only effect is same-state audit;
replay returns that Transition. Admission is deliberately three committed
steps: `ADMIT` creates generation-0 `ADMITTED` plus the pending Snapshot,
`SPEC_SUPERSEDE` installs generation 1 while remaining `ADMITTED`, and the
following `INTERNAL` continuation plans `PLAN` and enters `PLANNING`. No crash
may collapse or duplicate those trigger identities.

### Internal continuation arbitration

One entering Transition may own at most one `INTERNAL` continuation, keyed by
that Transition's sequence. Whenever reducer work remains after an entering
Transition, that one continuation evaluates this fixed precedence and records
one Transition even when the winning action is same-state:

1. if cancellation is pending, fence/supersede semantic work and perform only
   cancellation reconciliation;
2. otherwise, if an eligible `pending_snapshot_id` exists, schedule its
   separate `SPEC_SUPERSEDE` and perform no dependency or state-specific work;
3. otherwise, if a pending dependency is still unsatisfied/unknown at this
   safe boundary, append its Recovery Evidence and perform no state-specific
   work; and
4. otherwise, if the coalesced current-panel staffing pointer names this
   entering Transition and no peer remains `CLAIMED`, perform its all-or-none
   staffing evaluation; and
5. otherwise, perform the one state-specific continuation, including initial
   planning, aggregation, or approval/base-policy evaluation. Recovery work is
   never an `INTERNAL` continuation and is applied only by its exact
   `RECOVERY_EVIDENCE` trigger.

The resulting `INTERNAL` Transition may itself enter a state with one distinct
state-specific continuation (for example `AGGREGATING -> APPROVED` followed by
approval evaluation); its new sequence is the new trigger. The same entering
sequence is never reused for two branches, and cancellation never plans
semantic work in the same transaction.
This precedence guards every accepted Result and Attempt Terminal Transition,
not only rows that explicitly mention it. The controller may accept the exact
Result/Receipt/Candidate or terminalize the Attempt, but before emitting any
next semantic Activity, offer, controller effect, or state-specific work it
rechecks cancellation, `supersede_requested`/`pending_snapshot_id`, and the
pending dependency pointer under the writer lock. If one wins, the Transition
completes/fences only the current work, remains at that safe-boundary Run state,
and leaves exactly the winning continuation; ordinary destination/work in the
table is suppressed. `SPEC_SUPERSEDE` clears the supersede flag/sequence when it
installs or clears the pending Snapshot. A later continuation performs the full
precedence again rather than resuming a cached next Activity.

If step 4 or 5 is an offer/claim-deadline dispatch continuation, it is not consumable while
Controller Mode blocks offers or the Capability Registry lacks a selected
`ACTIVE` issuance key. The reconciler leaves the sequence pending rather than
writing a no-op that would lose the only continuation. A later successful
mode/key reconciliation re-runs the full precedence list; cancellation,
Snapshot, or dependency work may therefore win before dispatch.
That offer reconciler always reduces the still-pending
`T(INTERNAL, entering_transition_sequence)` under the writer lock and reruns
all five precedence steps; Controller Mode or Capability Registry mutation is
never substituted as the Run trigger. It consumes the continuation only when
the resulting transaction actually emits the fenced offer/panel Wait or a
higher-precedence action.

| From | Persisted trigger and preconditions | Durable write | To and emitted work |
| --- | --- | --- | --- |
| no Run | eligible `WORK_ITEM_SNAPSHOT` Forge Observation; no active Run; exact trusted `BASE_HEAD` Observation already accepted | Under the serialized writer, select the eligible Work-Item-targeted `BASE_HEAD` with the greatest accepted per-target `observation_sequence` before this transaction; create Run, capture-sequence-1 pending Snapshot, `NONE -> ADMITTED` Transition whose `anchored_base_observation_id` equals the Snapshot's `base_observation_id`, and working Projection intent | `ADMITTED`; schedule `T(SPEC_SUPERSEDE, snapshot_id)`; `T(ADMIT, work_item_forge_observation_id)` consumes and orders both admission observations at this sequence |
| `ADMITTED` | pending capture-sequence-1 Snapshot exists and no generation is installed | Install generation 1 and clear pending pointer | remain `ADMITTED`; schedule planning continuation; `T(SPEC_SUPERSEDE, snapshot_id)` |
| `ADMITTED` | generation-1 Snapshot is installed and initial plan absent | Transition and initial `PLAN` Activity; create its Attempt/outbox only under the current mode/key offer gates | `PLANNING`; plan `PLAN`; `T(INTERNAL, spec_transition_sequence)` |
| `PLANNING` | accepted successful `PLAN` Attempt Result with valid structured plan for current Snapshot | Store normalized plan and digest; complete Activity | `BUILDING`; plan `BUILD`; `T(ATTEMPT_RESULT, attempt_id)` |
| `BUILDING` | accepted successful `BUILD` Attempt Result references a durably admitted Candidate for current generation | Complete Activity; select Candidate current; increment Candidate generation as an effect of accepting the Result | `VERIFYING`; plan required `VERIFY`; `T(ATTEMPT_RESULT, attempt_id)` |
| `VERIFYING` | accepted worker Result supplies the required `PASS` Receipt for the one `VERIFY` Activity and the planning reduction can select a complete policy-valid reviewer staffing assignment from frozen Health | Complete `VERIFY`; freeze panel, all Review Activities/Assignments/subjects, and offers | `REVIEWING`; `T(ATTEMPT_RESULT, attempt_id)` |
| `VERIFYING` | the same PASS planning reduction cannot select a complete policy-valid reviewer assignment | Complete `VERIFY`; freeze panel Activities/Assignments/subjects but no reviewer offer; insert `WAITING/CAPACITY` sourced by this Result with complete ordered consulted-Health membership/digest and every unfilled reviewer Activity/slot membership/digest | `WAITING`, resume `REVIEWING`; `T(ATTEMPT_RESULT, attempt_id)`; create no Consensus Decision |
| `REVIEWING` | an accepted worker Attempt Result supplies a valid Receipt but the frozen panel does not yet have exactly one `fills_slot = true` Receipt for every gating slot | Store Receipt and applied trigger | `REVIEWING`; dispatch only still-unfilled slots; `T(ATTEMPT_RESULT, attempt_id)` |
| `REVIEWING` | that receipt-planning reduction finds unfilled slots, no complete policy-valid staffing selection, and no unfilled slot has a peer `CLAIMED` Attempt | Store Receipt; atomically supersede every peer `OFFERED` Attempt/outbox; insert `WAITING/CAPACITY` sourced by this exact Attempt Result with complete ordered consulted-Health and every now-undispatched unfilled reviewer Activity/slot membership | `WAITING`, resume `REVIEWING`; `T(ATTEMPT_RESULT, attempt_id)`; create no Consensus Decision |
| `REVIEWING` | the same no-capacity condition exists while any unfilled slot still has a peer `CLAIMED` Attempt | Store Receipt; preserve claimed peers; leave undispatched slots `PLANNED`; create no Wait/offer; replace the coalesced staffing projection with this Result Transition's Candidate/panel/`REVIEW`/sequence | remain `REVIEWING`; `T(ATTEMPT_RESULT, attempt_id)`; each later peer boundary replaces the pointer and only its latest `INTERNAL` may staff |
| `REVIEWING` | an accepted worker Attempt Result supplies the Receipt that gives every frozen gating slot exactly one valid `fills_slot = true` Receipt | Freeze canonical receipt set as a reducer effect | `AGGREGATING`; `T(ATTEMPT_RESULT, attempt_id)` |
| `AGGREGATING` | canonical verification/review input set produces `APPROVED`, or produces `REMEDIATE` below the pinned repeated-failure threshold | Store the panel's sole Consensus Decision as reducer output | `APPROVED` or `REMEDIATING`; `T(INTERNAL, aggregating_transition_sequence)` using the Transition that entered `AGGREGATING` |
| `AGGREGATING` | canonical input produces `ADJUDICATE` and the sole default adjudicator assignment can be offered under the current gates | Store the sole `ADJUDICATE` Decision and its one Activity/Assignment/offer | `ADJUDICATING`; `T(INTERNAL, aggregating_transition_sequence)` |
| `AGGREGATING` | canonical input produces `REMEDIATE` and the same normalized blocker/failure fingerprint reaches `maxRepairCyclesBeforeDiagnosis` | Store the sole `REMEDIATE` Decision; append `REVIEW_DISAGREEMENT` Recovery Evidence with the incremented counters and `selected_tactic = DIAGNOSE`; plan no remediation yet | `RECOVERING`; next `T(RECOVERY_EVIDENCE, recovery_evidence_id)` enters `DIAGNOSING`; current `T(INTERNAL, aggregating_transition_sequence)` |
| `AGGREGATING` | the same deterministic `ADJUDICATE` planning continuation cannot select a compatible adjudicator for the sole default slot | Store the `ADJUDICATE` Decision and sole planned Activity/Assignment, create no offer, and insert `WAITING/CAPACITY` sourced by this exact `INTERNAL` continuation with complete ordered consulted-Health membership and the sole unfilled `ADJUDICATE/default` membership | `WAITING`, resume `ADJUDICATING`; `T(INTERNAL, aggregating_transition_sequence)`; capacity creates no additional Consensus Decision |
| `REMEDIATING` | accepted successful worker Attempt Result references a new admitted Candidate derived from exact findings and current Candidate | Complete Activity; select new Candidate current as an effect of accepting the Result | `VERIFYING`; plan the single `VERIFY` Activity running all ordered `default` commands; `T(ATTEMPT_RESULT, attempt_id)` |
| `ADJUDICATING` | an accepted worker Attempt Result completes decisive adjudication that `SUSTAIN`s/adds a blocker and its normalized fingerprint remains below the pinned repeated-failure threshold | Store finding resolutions/new findings as evidence; do not rewrite original receipts | `REMEDIATING`; plan `REMEDIATE`; `T(ATTEMPT_RESULT, attempt_id)` |
| `ADJUDICATING` | that sustained/new-blocker fingerprint reaches `maxRepairCyclesBeforeDiagnosis` | Store the accepted Receipt/findings; append `REVIEW_DISAGREEMENT` Recovery Evidence selecting `DIAGNOSE`; plan no remediation yet | `RECOVERING`; next `T(RECOVERY_EVIDENCE, recovery_evidence_id)` enters `DIAGNOSING`; current `T(ATTEMPT_RESULT, attempt_id)` |
| `ADJUDICATING` | an accepted worker Attempt Result completes the set that decisively `OVERRULE`s every disputed blocker and no blocker remains | Store resolutions; close the old panel and allocate the next positive `panel_round` with no carried Receipt | `REVIEWING`; plan a wholly fresh `REVIEW` Activity for every gating slot; `T(ATTEMPT_RESULT, attempt_id)` |
| `ADJUDICATING` | accepted Result supplies a non-filling abstention or `INCONCLUSIVE` Receipt | Store it; retain the sole default-slot Activity; append `REVIEW_DISAGREEMENT` Recovery Evidence selecting the deterministic retry/replacement/wait tactic | `RECOVERING`; next `T(RECOVERY_EVIDENCE, recovery_evidence_id)` acts on that same Activity; current `T(ATTEMPT_RESULT, attempt_id)` |
| `APPROVED` | Candidate remains current; policy/config/spec hashes match; base policy satisfied; no Publication exists | Create Publication at `effect_generation = 1`; insert its immutable `INITIAL` Publication Effect, controller Activity, and outbox intent bound to the exact generation and commit | `PUBLISHING`; plan controller `PUBLISH`; `T(INTERNAL, approved_transition_sequence)` |
| `APPROVED` | Candidate remains current; an existing Publication is not `ACTIVE` and its prior `INITIAL` effect is superseded | Increment `Publication.effect_generation`; insert a higher `INITIAL` Publication Effect. For `PLANNED`/unlinked recovery use the latest observed deterministic-ref SHA or explicit nonexistence as CAS expectation; for a provisional `CHANGE_REQUEST_OBSERVED` Publication use its exact head and stable Change Request | `PUBLISHING`; plan controller `PUBLISH`; `T(INTERNAL, approved_transition_sequence)` |
| `APPROVED` | Candidate remains current; all ordinary gates passed; a linked active Publication exists and its expected head observation remains current | Atomically increment `Publication.effect_generation`; insert its immutable `UPDATE` Publication Effect, controller Activity, and outbox intent bound to the Candidate and expected remote head | `PUBLISHING`; plan update-mode `PUBLISH`; `T(INTERNAL, approved_transition_sequence)` |
| any nonterminal state with a Publication, including cancellation reconciliation | accepted complete marker search contains one-or-more `TERMINAL/MERGED` members with `ownership_status = POSITIVE` | Apply terminal precedence before every live-cardinality, conflict, head, cancellation, or ordinary publication branch; select bytewise-lowest positive merged stable ID; atomically establish exact Publication association/search/member proof, append the applicable current `COMPLETE_MARKER_SEARCH`/`COMPLETE` checkpoint when effect-bound, fence all semantic/publication work, set Publication `CLOSED` and Run outcome `MERGED`, and create the Terminal Duplicate Cleanup Reservation containing every `LIVE` member in stable-ID order | `MERGED`; cleanup survives as controller-owned post-terminal work; `T(FORGE_OBSERVATION, observation_id)` |
| any nonterminal state with a Publication, and no positive owned merged terminal member | accepted complete marker search contains any `INCOMPATIBLE` ownership member | Fence ordinary semantic/publication work; append exact-source ownership Recovery Evidence selecting `RECONCILE`; choose or mutate no member | `RECOVERING`; next `T(RECOVERY_EVIDENCE, recovery_evidence_id)` plans ownership `RECONCILE`, whose positive `OWNERSHIP_CONFLICT` Fact creates the exceptional Boundary; current `T(FORGE_OBSERVATION, observation_id)`; positive merged has precedence |
| any nonterminal state with a Publication, and no positive merged or incompatible member | accepted complete marker search contains any `INCOMPLETE` ownership member | Choose, link, terminalize, close, or detach nothing; commit the exact next object read/complete-search request or bounded evidence Wait under the ordinary forge-recovery policy | remain in the current state or enter the exact retry Wait; `T(FORGE_OBSERVATION, observation_id)` |
| `PUBLISHING` | current `INITIAL` effect's `BASE_READ_PRE` observes the same trusted base commit reviewed by the effect | Append `BASE_READ_PRE/OBSERVED_SATISFIED`; retain effect/Activity | remain `PUBLISHING`; continue to `REF_READ`; `T(FORGE_OBSERVATION, observation_id)` |
| `PUBLISHING` | current `INITIAL` effect observes deterministic ref at its exact desired commit | Append `REF_READ/OBSERVED_SATISFIED`; set Publication `BRANCH_OBSERVED` and observed ref head | remain `PUBLISHING`; search/reconcile the Change Request; `T(FORGE_OBSERVATION, observation_id)` |
| `PUBLISHING` before initial linkage | after the three ownership-precedence rows above, current `INITIAL` effect receives complete marker search with `ZERO` live members and no terminal member | Store initial-link revision/full-set digest/`ZERO` with no retained or terminal projection; append `COMPLETE_MARKER_SEARCH/OBSERVED_SATISFIED` | remain `PUBLISHING`; run exact `CHANGE_REQUEST_SEARCH`, optional stable create, then a fresh complete marker search; neither search/create response links; `T(FORGE_OBSERVATION, observation_id)` |
| `PUBLISHING` before initial linkage with current `ZERO`-live/no-terminal proof | exact `CHANGE_REQUEST_SEARCH` returns `CHANGE_REQUEST_ABSENT` | Append `CHANGE_REQUEST_SEARCH/OBSERVED_ABSENT`; if no stable create request/checkpoint already exists, commit `CHANGE_REQUEST_CREATE/REQUEST_READY` and its outbox before I/O | remain `PUBLISHING`; after create/reconciliation, run a fresh complete marker search; `T(FORGE_OBSERVATION, observation_id)` |
| `PUBLISHING` before initial linkage with current `ZERO`-live/no-terminal proof | exact `CHANGE_REQUEST_SEARCH` discovers an object or the stable create request produces/reconciles an object | Append the applicable observation-backed search/create checkpoint but do not set `change_request_external_id` | remain `PUBLISHING`; schedule a fresh complete marker search whose live/terminal membership alone selects the next branch; `T(FORGE_OBSERVATION, observation_id)` |
| `PUBLISHING` before initial linkage | after ownership precedence, current complete marker search proves exactly `ONE` positive live member, with only positive closed terminal audit membership | Store revision/full-set digest/`ONE` and sole live retained ID; clear terminal-selection projection; append checkpoint; require a fresh exact-object read before association | remain `PUBLISHING`; closed terminal audit members do not block linkage and grant no authority; `T(FORGE_OBSERVATION, observation_id)` |
| `PUBLISHING` before initial linkage | after ownership precedence, current complete marker search proves `MULTIPLE` positive live members, with only positive closed terminal audit membership | Store revision/full-set digest/`MULTIPLE` and bytewise-lowest live retained candidate; clear terminal-selection projection; append checkpoint; plan duplicate `RECONCILE`/eligible cleanup without setting `change_request_external_id` | remain `PUBLISHING`; cleanup live members one at a time and repeat complete search until `ONE`; all individual member merge/close observations are same-state only; `T(FORGE_OBSERVATION, observation_id)` |
| `PUBLISHING` before initial linkage | after ownership precedence, current complete marker search proves `ZERO` live members and one-or-more positive `CLOSED` terminal members | Store revision/full-set digest/`ZERO`; select bytewise-lowest closed stable ID; atomically store retained ID/head, terminal search/member proof, and Publication association; append `COMPLETE_MARKER_SEARCH` then `COMPLETE`; complete/fence `PUBLISH` and every unfinished work item | set Publication `CLOSED` and Run `CLOSED`; create nothing; `T(FORGE_OBSERVATION, observation_id)` |
| `PUBLISHING` before initial linkage with current duplicate `RECONCILE` | `REDUNDANT_PUBLICATIONS_PROVEN` proves exactly one retained-lowest member and one-or-more exactly equivalent, open, unmerged, unreviewed `CLOSE` members for the current complete-search pair | Complete `RECONCILE`; store the Fact/search pair without setting `change_request_external_id`; plan one `CLOSE_REDUNDANT_PUBLICATION` for the first `CLOSE` member while the `PUBLISH` Activity remains suspended | remain `PUBLISHING`; `T(RECONCILIATION_FACT, reconciliation_fact_id)` |
| `PUBLISHING` before initial linkage with current duplicate `RECONCILE` | positive non-Orcest reliance or incompatible ownership evidence produces `OWNERSHIP_CONFLICT` for the exact current member set | Complete `RECONCILE`; create the exactly bound exceptional ownership Boundary; do not choose or link a member | `NEEDS_HUMAN`; `T(RECONCILIATION_FACT, reconciliation_fact_id)`; temporary or unavailable proof follows autonomous wait/retry and emits no conflict Fact |
| `PUBLISHING` before initial linkage with current `CLOSE_REDUNDANT_PUBLICATION` | authenticated close Observation binds the exact cleanup Activity/operation, redundant stable ID, expected head, marker/ref, Publication, and effect generation | Complete only the cleanup Activity; keep the `PUBLISH` Activity suspended and schedule a fresh complete marker search | remain `PUBLISHING`; `T(FORGE_OBSERVATION, forge_observation_id)`; the close is never Run closure |
| `PUBLISHING` before initial linkage with current duplicate `RECONCILE` or cleanup | an accepted member/head/marker/reliance Observation invalidates the frozen proof, or a changed complete-search result arrives | Supersede/fence the stale subordinate Activity; an individual input schedules a fresh complete marker search, while only the changed complete-search result may plan the replacement `RECONCILE` | remain `PUBLISHING`; `T(FORGE_OBSERVATION, forge_observation_id)` |
| `PUBLISHING` before initial linkage | fresh `ONE`-live complete-search proof is current and exact-object observation matches its sole stable ID and desired head | Append observation-backed Change Request checkpoint; store stable external ID/head; set Publication `CHANGE_REQUEST_OBSERVED`, not `ACTIVE` | remain `PUBLISHING`; perform `BASE_READ_POST`; `T(FORGE_OBSERVATION, observation_id)` |
| `PUBLISHING` | current `INITIAL` effect's `BASE_READ_POST` observes the same trusted base commit reviewed by the effect | Append `BASE_READ_POST/OBSERVED_SATISFIED` then `COMPLETE/COMPLETED`; set Publication `ACTIVE`; complete `PUBLISH` | `PR_MONITORING`; plan observation; `T(FORGE_OBSERVATION, observation_id)` |
| `PUBLISHING` | current `UPDATE` effect observes its exact desired ref/Change Request head after compare-and-swap | Append `REF_UPDATE/OBSERVED_SATISFIED` then `COMPLETE/COMPLETED`; update observed head; complete `PUBLISH`; Publication remains `ACTIVE` | `PR_MONITORING`; plan observation; `T(FORGE_OBSERVATION, observation_id)` |
| `PR_MONITORING` without cancellation intent | CI/reviews are pending or clean but final merge policy is not yet satisfied | Record Forge Observation | `PR_MONITORING`; schedule the next authenticated head-bound observation; `T(FORGE_OBSERVATION, observation_id)`; do not create a CI Wait Condition |
| any nonterminal post-link state other than `PR_MONITORING`, without cancellation intent | accepted observation targets another stable Change Request ID carrying the same syntactically valid Run/Publication marker and deterministic ref, rather than current `Publication.change_request_external_id` | Retain the ordered observation and append a same-state audit Transition; never close, merge, import, or change the retained association from this partial evidence | remain in the current state; ordinary entry to `PR_MONITORING` performs a fresh complete marker search and may then plan duplicate `RECONCILE`; `T(FORGE_OBSERVATION, observation_id)` |
| any nonterminal post-link state without cancellation intent | ordered Forge Observation proves the run-owned Change Request head advanced beyond the exact head fence, including during `VERIFYING`, `REVIEWING`, `AGGREGATING`, `ADJUDICATING`, remediation, approval, publication, waiting, recovery, or a Human Boundary | Record exact new head; supersede every old-head Activity/Attempt, Candidate gate/Receipt/Decision, Wait, recovery plan, Human Boundary, and stale Publication Effect; retain all audit rows | `PR_REMEDIATING`; plan controller `IMPORT` bound to that observation; `T(FORGE_OBSERVATION, observation_id)`. Merge/close and cancellation-race rows have precedence. |
| `PR_MONITORING` without cancellation intent | current-head `CHANGE_REQUEST_FEEDBACK` reports `mergeability = CONFLICTING` | Record the normalized feedback set; require the latest accepted trusted `BASE_HEAD` observation; conflict takes precedence over checks/review feedback | `PR_REMEDIATING`; plan `REBASE` with causal `BASE_HEAD` and the separate exact Change Request-head fence, or wait for that base observation; `T(FORGE_OBSERVATION, observation_id)` |
| `PR_MONITORING` without cancellation intent | current-head `CHANGE_REQUEST_FEEDBACK` is not conflicting and contains at least one failing configured required check, a current `CHANGES_REQUESTED` review by a non-Orcest principal, or an unresolved current-head thread by a non-Orcest principal | Freeze the byte-sorted failed-check/review/thread fact set from that observation | `PR_REMEDIATING`; plan exactly one `PR_REMEDIATE` bound to that observation, head, and complete fact set; `T(FORGE_OBSERVATION, observation_id)` |
| `PR_REMEDIATING` | accepted successful `PR_REMEDIATE` or `REBASE` Attempt Result references a new admitted Candidate based on the exact observed PR head | Complete Activity; select Candidate as an effect of accepting the Result; retain expected remote head | `VERIFYING`; enter the ordinary full gate with fresh `VERIFY`; `T(ATTEMPT_RESULT, attempt_id)` |
| `PR_REMEDIATING` | a successful Controller Operation Fact proves current `IMPORT` validated and admitted or resolved the exact externally observed PR head as a Candidate | Complete `IMPORT`; select Candidate; retain expected remote head | `VERIFYING`; enter the ordinary full gate with fresh `VERIFY`; `T(CONTROLLER_OPERATION, controller_operation_fact_id)` |
| `REMEDIATING` | a successful Controller Operation Fact proves current `IMPORT` safely fetched and admitted the exact pre-link foreign deterministic-ref head selected by ownership reconciliation | Complete `IMPORT`; select Candidate; retain the foreign ref SHA as the next initial-publication CAS expectation | `VERIFYING`; enter the ordinary full gate with fresh `VERIFY`; `T(CONTROLLER_OPERATION, controller_operation_fact_id)` |

Every Transition entering `APPROVED` immediately schedules one `INTERNAL`
continuation keyed by that Transition sequence. It reads the latest accepted
and already-reduced trusted `BASE_HEAD`: `REBASE_BEFORE_PUBLICATION` rebases on
difference, `PIN` may publish the pinned Candidate regardless of difference,
and `SUPERSEDE_AT_BOUNDARY` cannot publish while a differing
`supersession_key` is pending. The continuation either takes the explicit
`APPROVED` rebase/supersession branch or one of the publication rows above. It
never consumes the old `BASE_HEAD` observation a second time.

The pre-publication default Consensus Decision is `APPROVED` only when all
required verification passes, two independent valid approvals exist, and no
unresolved blocker remains on the exact Candidate. Review arrival order is
irrelevant. Full panel and finding semantics are defined in [review and
consensus](review-and-consensus.md).

Every post-publication replacement Candidate MUST pass the same full configured
verification and consensus gate as a pre-publication Candidate before Orcest
updates the Change Request branch. v1 exposes no weaker post-publication gate.
All receipts bind to the replacement Candidate and prior-Candidate receipts are
ineligible.

### Planning contract

A structured planning Activity is mandatory in every specification generation
in v1; it is never an optional agent prelude. Initial admission uses the
distinct `PLAN` Activity in `PLANNING`. A specification supersession uses
`REPLAN` in `REPLANNING`, as does diagnosis that invalidates an accepted plan.
Neither kind introduces a separate repository-configured planner role,
profile, or prompt. `PLAN`, `BUILD`, `DIAGNOSE`, and `REPLAN` use the pinned
`implementation` execution profile and implementation prompt. `REMEDIATE`,
`PR_REMEDIATE`, and `REBASE` use the pinned repair profile and prompt. The
controller adds a code-owned mode-specific envelope and structured result
schema for each Activity kind.

A valid plan contains a bounded ordered set of intended changes, affected
areas, verification intentions, and stated requirement mappings. It contains
no lifecycle directives and grants no authority. `BUILD` receives the accepted
normalized plan plus the original Snapshot. `REPLAN` receives those inputs and
the accepted diagnosis/evidence. A policy-only `REPLAN` additionally receives
the exact retained Candidate ID, commit, and bundle digest as read-only
context; its Activity/Attempt semantic-input digest covers them. Reusing the
implementation profile keeps the v1 repository schema small; a distinct
planner profile is deferred until operational evidence shows it is necessary.

### Plan Result schema

`PLAN` and `REPLAN` success MUST carry this closed JSON object as the Attempt
Result `structured_output`; unknown or omitted fields are invalid:

```json
{
  "protocol": "orcest.plan/1",
  "mode": "PLAN",
  "snapshot_hash": "sha256:64-lowercase-hex",
  "prior_plan_digest": null,
  "candidate_context": null,
  "diagnosis_digests": [],
  "objective": "bounded summary",
  "requirements": [
    {
      "requirement_key": "r1",
      "source": "BODY",
      "summary": "bounded requirement"
    }
  ],
  "steps": [
    {
      "step_key": "s1",
      "summary": "bounded intended change",
      "path_hints": ["src/example.py"],
      "requirement_keys": ["r1"],
      "depends_on": []
    }
  ],
  "verification_intents": [
    {
      "intent_key": "v1",
      "summary": "bounded observable outcome",
      "requirement_keys": ["r1"]
    }
  ],
  "assumptions": [],
  "risks": []
}
```

Validation and normalization rules are:

- normalized JSON MUST be at most 65,536 bytes; `objective`, each `summary`,
  assumption, and risk is nonempty NFC text of at most 2,048 Unicode scalar
  values, and arrays are bounded as declared below;
- `mode` is exactly the Activity kind, `snapshot_hash` is the current Snapshot,
  and `prior_plan_digest` plus `diagnosis_digests` exactly match the immutable
  Activity input (`PLAN` requires `null` and an empty array); digest arrays are
  duplicate-free and lexicographically sorted;
- `candidate_context` is `null` except for a policy-only `REPLAN`; there it is
  exactly `{candidate_id, commit: {object_format, oid}, bundle_digest}` copied
  from `Run.policy_replan_candidate_id` and the immutable Candidate, and any
  omission or mismatch invalidates the Result;
- `requirements`, `steps`, and `verification_intents` each contain 1 through
  64 entries; their keys are unique lowercase identifiers matching
  `[a-z][a-z0-9-]{0,31}` and array order is semantic;
- `source` is `TITLE`, `BODY`, or `COMMENT:<positive-ordinal>`; a comment source
  is valid only when that comment was pinned in the Snapshot;
- each `path_hints` array has at most 64 unique lexicographically sorted
  repository-relative UTF-8 paths, each at most 256 bytes, with no empty,
  absolute, `.` or `..` segment; path hints grant no filesystem authority;
- every `requirement_keys` and `depends_on` array is duplicate-free and
  lexicographically sorted, every reference exists, and a dependency refers
  only to an earlier step, making the listed graph acyclic without a separate
  topological sort;
- every requirement is referenced by at least one step and at least one
  verification intent; `assumptions` and `risks` contain at most 32 strings
  each; and
- the controller recomputes the normalized output digest. It validates shape
  and references, not the truth of model prose. The Snapshot remains normative
  and is always passed independently to builders and reviewers.

No field can select the next Run state, change verification commands, name a
provider, reduce reviewer slots or approvals, waive a requirement, publish a
Candidate, or request human intervention. Text resembling one of those
directives is retained only as untrusted plan prose and ignored by the reducer.

### Diagnosis Result schema

A successful `DIAGNOSE` Attempt MUST carry this closed JSON object; unknown or
omitted fields are invalid:

```json
{
  "protocol": "orcest.diagnosis/1",
  "snapshot_hash": "sha256:64-lowercase-hex",
  "candidate_id": "uuid-or-null",
  "failure_set_digest": "sha256:64-lowercase-hex",
  "plan_digest": null,
  "plan_assessment": "VIABLE",
  "observations": [
    {
      "evidence_ref": "controller-supplied-opaque-reference",
      "summary": "bounded observed fact"
    }
  ],
  "hypotheses": [
    {
      "hypothesis_key": "h1",
      "category": "CODE_DEFECT",
      "summary": "bounded possible cause",
      "evidence_refs": ["controller-supplied-opaque-reference"]
    }
  ],
  "requirement_conflicts": [],
  "suggested_changes": []
}
```

The normalized object MUST be at most 65,536 bytes. Snapshot, Candidate,
failure-set, and plan bindings MUST exactly equal the immutable Activity input;
`candidate_id` is `null` only when diagnosis has no Candidate input, and
`plan_digest` is `null` only when no plan has yet been accepted.
`plan_assessment` is `VIABLE`, `INVALID`, `INCONCLUSIVE`, or `ABSENT`, with
`ABSENT` required exactly when `plan_digest` is `null`. `observations` and
`hypotheses` each contain 1 through 64 entries; hypothesis keys use the plan-key
syntax and are unique. `category` is one of `CODE_DEFECT`, `PLAN_DEFECT`,
`ENVIRONMENT`, `TOOLING`, `EXTERNAL`, `SPECIFICATION`, or `UNKNOWN`.
`evidence_ref` and every `evidence_refs` member MUST be one of at most 128
opaque references supplied by the controller, and lists are duplicate-free
and lexicographically sorted. `requirement_conflicts` is a duplicate-free,
lexicographically sorted list of at most 64 requirement keys from the accepted
plan, and MUST be empty when `plan_digest` is `null`. `suggested_changes`
contains at most 64 nonempty NFC strings. Every text field is at most 2,048
Unicode scalar values.

The diagnosis is evidence, not authority. The reducer applies pinned counters
and stable strategy order to `plan_assessment`, categories, and evidence; it
may choose only a code-owned permitted tactic. The schema intentionally has no
next-state, retry-count, gate, quorum, publication, cancellation, or
human-escalation field, and lifecycle-like prose in text is ignored.
Diagnosis never creates, classifies, or resolves a review dispute; only the
typed same-subject Review Receipt rule in the consensus specification can do
so.

## Candidate and receipt rules

Selecting a new current Candidate MUST atomically:

1. increment `candidate_generation`;
2. update `Run.current_candidate_id`;
3. mark all unfinished Activities bound to an older Candidate `SUPERSEDED`;
4. make every older Candidate's receipts ineligible for gates; and
5. plan one fresh `VERIFY` Activity containing every ordered `default`
   Verification Profile command before any review or publication.

Old receipts remain immutable audit evidence. They are never copied forward.
v1 has no “safe approval carry-over” rule.

Every frozen reviewer slot and adjudicator slot is represented by a distinct
Activity with exactly one immutable Activity Review Assignment and a complete
ordered Activity Review Subject membership. A controller restart reconstructs
unfilled work from the Activity, Assignment, ordered subject and finding
memberships, current Attempt generation, and Outbox—not from Redis or a
previous claim envelope. Every Assignment's subjects are the closed v1 list:
`snapshot:overall` followed by every accepted Plan requirement in semantic
Plan order. Review Receipt assessments must cover that ordered membership
exactly; every Review or Adjudication Receipt's context digest and applicable
disputed-finding membership must equal the durable Assignment and memberships.
Each Receipt also copies the producing Attempt's frozen `provider_family` and
`model_family` plus `classification_revision`; independence never reclassifies
a historical Attempt through the current execution-profile registry. Every
model-backed Receipt additionally requires and copies the exact accepted
`launch_attestation_id`. Fresh workspace, context, and invocation independence
is satisfied only by that signed registered-runner Attestation and globally
unique IDs, never by a worker assertion.

If Candidate admission resolves an upload to the current Candidate's existing
same-commit content identity, the Attempt may complete for audit purposes but
the reducer MUST classify it as non-progress. It does not increment Candidate
generation, rerun unchanged gates as though code changed, or satisfy a
remediation Activity. Its commit and failure-set fingerprint advance the
diagnosis/replanning ladder.

A Candidate that changes `.orcest` is reviewed as code, but those changes do
not affect its Run. They may affect a later Run only after they land on a
trusted base revision and are loaded into a new Snapshot.

## Alternate and recovery transition table

This table completes the legal non-happy-path transitions. `origin` is the
persisted `recovery_origin_state`; `A` is the exact Activity being recovered.
Claim-deadline replacement, claim-deadline capacity waiting, and every
audit-only later Attempt Terminal Fact follow the state matrix below. A
claim-deadline Terminal-Fact transaction only terminalizes the Attempt, returns
its Activity to `PLANNED`, appends the applicable zero-counter Recovery
Evidence, and enters `RECOVERING`. Only the subsequent
`T(RECOVERY_EVIDENCE, recovery_evidence_id)` reduction may offer a higher
generation or create a capacity Wait; it must revalidate the frozen Fact and
current gates. An audit-only later Fact does not alter the Activity or Run.
None of these paths may bypass the `PLANNED -> READY` offer transition.

| From | Persisted trigger and preconditions | Durable write | To and emitted work |
| --- | --- | --- | --- |
| `VERIFYING` | an accepted worker Attempt Result supplies the single required Verification Receipt with `FAIL` and actionable evidence whose normalized fingerprint remains below `maxRepairCyclesBeforeDiagnosis` | Freeze that receipt; complete the `VERIFY` Activity | `REMEDIATING`; plan `REMEDIATE` against current Candidate and findings; `T(ATTEMPT_RESULT, attempt_id)` |
| `VERIFYING` | that same verification-failure fingerprint reaches `maxRepairCyclesBeforeDiagnosis` | Freeze the Receipt; complete `VERIFY`; append `VERIFICATION_FAILURE` Recovery Evidence with incremented counters and `selected_tactic = DIAGNOSE`; plan no remediation yet | `RECOVERING`; next `T(RECOVERY_EVIDENCE, recovery_evidence_id)` enters `DIAGNOSING`; current `T(ATTEMPT_RESULT, attempt_id)` |
| `VERIFYING` | accepted `FAILED_RETRYABLE` Attempt Result carries schema-valid Verification Receipt `ERROR` and `VERIFICATION_ERROR` | Store Receipt; terminalize the Attempt and atomically return its `VERIFY` Activity to `PLANNED`; append exact-source Verification-error Recovery Evidence and origin | `RECOVERING`; next evidence reduction selects retry/wait; current `T(ATTEMPT_RESULT, attempt_id)` |
| `VERIFYING` | submitted Result omits its required Receipt or the Receipt/Result is malformed or has an invalid outcome/code pairing | Reject the submission with the worker protocol's bounded 4xx response before Result Request admission; insert no Attempt Result, Receipt, Result Request, Recovery Evidence, or Transition | remain `VERIFYING` with the Attempt `CLAIMED` so a corrected pre-deadline submission may arrive; otherwise only its execution-deadline or worker-loss Attempt Terminal Fact can recover it |
| any Activity-owning active state | accepted worker Attempt Result reports a typed recoverable failure | Atomically terminalize the Attempt and return its Activity to `PLANNED`; append Recovery Evidence with failure fingerprint, `recovery_origin_state`, `recovery_activity_id`, and post-application counters | `RECOVERING`; `T(ATTEMPT_RESULT, attempt_id)`; only a later Evidence reduction may offer a replacement |
| any Activity-owning active state | persisted Attempt Terminal Fact proves execution deadline or authoritative worker loss for the exact current nonterminal `CLAIMED` Attempt | Deadline sets Attempt `EXPIRED`; `WORKER_LOST` sets Attempt `FAILED` with that terminal reason; atomically return Activity to `PLANNED`, then append Recovery Evidence with failure fingerprint, origin, Activity, and post-application counters | `RECOVERING`; `T(ATTEMPT_TERMINAL, attempt_terminal_fact_id)`; only a later Evidence reduction may offer a replacement |
| any Activity-owning active state | persisted `CLAIM_DEADLINE` Attempt Terminal Fact applies with frozen `replacement_offer_disposition = OFFER_ALLOWED` and `capacity_disposition = COMPATIBLE_AVAILABLE`, and no panel exception below applies | Mark Attempt `EXPIRED`; atomically return Activity to `PLANNED`; append zero-counter Recovery Evidence selecting `RETRY_EXECUTION` from only the Fact's ordered Health Observation membership and exact resolved provider version | `RECOVERING`; `T(ATTEMPT_TERMINAL, attempt_terminal_fact_id)`; the next Evidence Transition creates `g + 1` and atomically offers it (`PLANNED -> READY`) |
| any non-panel Activity-owning active state, or a panel state with no peer `CLAIMED` Attempt | persisted `CLAIM_DEADLINE` Attempt Terminal Fact applies with frozen `replacement_offer_disposition = OFFER_ALLOWED` and `capacity_disposition = NO_COMPATIBLE_AVAILABLE` | Mark Attempt `EXPIRED`; atomically return Activity to `PLANNED`; append zero-counter Recovery Evidence selecting `WAIT_CAPACITY` with the exact frozen health membership, without synthesized health. For `REVIEW`/`ADJUDICATE`, atomically supersede every peer `OFFERED` Attempt/outbox and freeze every exact currently unfilled panel Activity/slot; all named slots then have no live Attempt and later select panel-scoped `STAFF_PANEL` | `RECOVERING`; `T(ATTEMPT_TERMINAL, attempt_terminal_fact_id)`; the next Evidence Transition creates the unique bound `WAITING/CAPACITY` condition and no offer |
| `REVIEWING` or `ADJUDICATING` | a persisted `CLAIM_DEADLINE` Attempt Terminal Fact applies while another unfilled panel slot has a `CLAIMED` Attempt | Expire only the due Attempt and leave its Activity `PLANNED`; preserve every claimed peer and create no Recovery Evidence, Wait, Attempt, or offer; replace the coalesced staffing projection with this Terminal Transition's Candidate/panel/kind/sequence | remain in the panel state; `T(ATTEMPT_TERMINAL, attempt_terminal_fact_id)`; this explicit panel exception takes precedence over every generic claim-deadline recovery row, and only the latest pointer may later staff or wait |
| `REVIEWING` or `ADJUDICATING` with a current panel staffing pointer | its latest named peer Transition is still current, no unfilled slot has a `CLAIMED` Attempt, offer mode/key gates pass, and one complete legal staffing set exists | Discharge every older obligation through the projection; atomically create offers/outboxes for every exact unfilled slot or none; clear the pointer | remain in the panel state; `T(INTERNAL, latest_staffing_recheck_transition_sequence)` |
| `REVIEWING` or `ADJUDICATING` with a current panel staffing pointer | its latest named peer Transition is still current, no unfilled slot has a `CLAIMED` Attempt, offer mode/key gates pass, and no complete legal staffing set exists | Supersede peer offers if any; freeze complete ordered Health plus every unfilled Activity/slot membership; create one panel `WAITING/CAPACITY`; clear the pointer; create no Decision | `WAITING`, resume this panel state; `T(INTERNAL, latest_staffing_recheck_transition_sequence)` |
| any state with a stale panel staffing pointer | Candidate/panel/kind no longer equal current durable panel work | Clear only the pointer as discharged; create no offer, Wait, Decision, counter, or Evidence | remain current; `T(INTERNAL, latest_staffing_recheck_transition_sequence)` |
| any Activity-owning active state, except the panel exception above | persisted `CLAIM_DEADLINE` Attempt Terminal Fact applies with `MODE_BLOCKED` or `ISSUANCE_KEY_UNAVAILABLE` | Mark Attempt `EXPIRED`; atomically return Activity to `PLANNED`; append zero-counter Recovery Evidence selecting `RETRY_EXECUTION`; create no Attempt, outbox, or Wait; retain the Transition's one pending dispatch continuation | `RECOVERING`; `T(ATTEMPT_TERMINAL, attempt_terminal_fact_id)`; a mode/key reconciler may later reduce the pending Evidence/continuation and, if the selected recovery tactic still applies, perform the `PLANNED -> READY` offer only after current gates pass |
| any state | a source-unique Attempt Terminal Fact applies after its Attempt is already terminal, including `RESULT_AFTER_TERMINAL` from an accepted `ALREADY_TERMINAL` Result Request | Retain the Fact as bounded audit; change no Attempt/Activity/Run state, counters, Evidence, Wait, or offer | remain in the current state; exactly one `T(ATTEMPT_TERMINAL, attempt_terminal_fact_id)` |
| `RECOVERING` | controller `RECONCILE` persists `EFFECT_PRESENT` Reconciliation Fact for the exact current publication identity/preconditions | Complete `RECONCILE`; attach the observed effect and its ordered observations idempotently | publication state implied by the fact/checkpoints; `T(RECONCILIATION_FACT, reconciliation_fact_id)` |
| `RECOVERING` | latest unapplied Recovery Evidence has `next_eligible_at_ms = NULL`, selects retry/redelivery/replacement, and A's semantic inputs are unchanged | Apply its one selected tactic; create the allowed next Attempt/outbox when required | `origin`; offer/redeliver according to tactic; `T(RECOVERY_EVIDENCE, recovery_evidence_id)` |
| `RECOVERING` | latest unapplied Recovery Evidence has future `next_eligible_at_ms` and its unique due `RECOVERY_ELIGIBILITY` Timer Fact now exists | Treat that Fact only as eligibility proof; revalidate the Evidence and apply its already selected tactic | destination selected by the Evidence; `T(RECOVERY_EVIDENCE, recovery_evidence_id)` only, never `T(TIMER_FACT, timer_fact_id)` |
| `RECOVERING` | latest unapplied Recovery Evidence selects `DIAGNOSE` at the pinned non-progress/attempt threshold | Freeze failure set and diagnosis input; plan idempotently keyed `DIAGNOSE` | `DIAGNOSING`; `T(RECOVERY_EVIDENCE, recovery_evidence_id)` |
| `RECOVERING` | latest unapplied `INTEGRITY_SUSPECTED` Recovery Evidence selects `PROBE_INTEGRITY` and names one exact live artifact/blob/Secret version | Commit one reciprocal Health Probe Request/outbox before I/O; create no storage/secret Wait or failure assertion | remain `RECOVERING` awaiting its typed Health Observation; `T(RECOVERY_EVIDENCE, recovery_evidence_id)` |
| `RECOVERING` | that exact integrity probe produces `AVAILABLE` and all object/generation bindings remain current | Complete the suspicion path; append exact-source Recovery Evidence selecting the ordinary origin-valid retry/resume tactic | remain `RECOVERING`; current `T(HEALTH_OBSERVATION, health_observation_id)`; next `T(RECOVERY_EVIDENCE, recovery_evidence_id)` resumes/retries without a storage/secret Wait |
| `DIAGNOSING` | successful current `DIAGNOSE` Attempt yields a valid structured diagnosis, the plan is still viable, and the pinned diagnosis threshold has not been reached | Store diagnosis/evidence digest; complete Activity; append the next Recovery Evidence with controller-selected allowed tactic | `RECOVERING`; current `T(ATTEMPT_RESULT, attempt_id)`; do not plan tactic work until the next `T(RECOVERY_EVIDENCE, recovery_evidence_id)` |
| `DIAGNOSING` | valid diagnosis proves the plan invalid or `maxDiagnosesBeforeReplan` is reached | Store diagnosis/evidence digest; complete Activity; append Recovery Evidence selecting `REPLAN` | `RECOVERING`; current `T(ATTEMPT_RESULT, attempt_id)`; the next reduction plans `REPLAN` with `T(RECOVERY_EVIDENCE, recovery_evidence_id)` |
| `REPLANNING` with `policy_replan_candidate_id` | successful current `REPLAN` Attempt yields a valid new Plan and the reducer rechecks that specification, workflow, exact base, and retained Candidate content/commit still satisfy the policy-only identity | Store the new Plan/digest; clear the policy-only context pointer; reselect that exact Candidate as current; derive every later review subject from this new Plan; old Plan/gates remain ineligible | `VERIFYING`; plan full `default` verification with no `BUILD`; `T(ATTEMPT_RESULT, attempt_id)` |
| `REPLANNING` otherwise, or policy-only identity no longer holds when the new Plan is accepted | Store the valid new Plan/digest; clear any policy-only Candidate context; supersede unfinished plan-bound Activities | `BUILDING`; plan `BUILD`; `T(ATTEMPT_RESULT, attempt_id)` |
| `PUBLISHING` under `REBASE_BEFORE_PUBLICATION` | any `INITIAL` generation's `BASE_READ_PRE` observes a trusted base different from the effect's reviewed `base_commit`, before any provisional Change Request exists | Append `BASE_READ_PRE/BASE_MISMATCH`; supersede the Publication Effect and `PUBLISH` Activity before mutation | `REMEDIATING`; plan `REBASE` against the exact `BASE_HEAD` observation; `T(FORGE_OBSERVATION, observation_id)` |
| `PUBLISHING` under `PIN` | any `INITIAL` generation's `BASE_READ_PRE` successfully observes a trusted commit different from the effect's reviewed `base_commit` | Append `BASE_READ_PRE/OBSERVED_SATISFIED` with the exact observation; retain effect/Activity | remain `PUBLISHING`; continue to `REF_READ`; `T(FORGE_OBSERVATION, observation_id)` |
| `PUBLISHING` under `SUPERSEDE_AT_BOUNDARY` | any `INITIAL` generation's `BASE_READ_PRE` observes a trusted base different from the effect's reviewed `base_commit` | Append `BASE_READ_PRE/BASE_MISMATCH`; supersede effect/Activity before mutation and capture/coalesce the base-only pending Snapshot; install nothing in this observation reduction | remain `PUBLISHING` with only its pending Snapshot continuation eligible; next `T(SPEC_SUPERSEDE, snapshot_id)` enters `REPLANNING`; current `T(FORGE_OBSERVATION, observation_id)` |
| `PUBLISHING` | `INITIAL` `REF_READ` observes a foreign deterministic-ref commit before provisional linkage | Record SHA; supersede effect/Activity; append exact-source ownership Recovery Evidence selecting `RECONCILE` | `RECOVERING`; next Evidence Transition plans ownership reconciliation; current `T(FORGE_OBSERVATION, forge_observation_id)` |
| `PUBLISHING` | pre-link `INITIAL` `REF_CREATE`/`REF_UPDATE` returns `CAS_MISMATCH` and reconciled ref head differs | Append checkpoint/`REF_HEAD`; supersede effect/Activity; append exact-source ownership Recovery Evidence selecting `RECONCILE` | `RECOVERING`; next Evidence Transition plans ownership reconciliation; current `T(FORGE_OBSERVATION, forge_observation_id)` |
| `PUBLISHING` under `REBASE_BEFORE_PUBLICATION` | any `INITIAL` generation's `BASE_READ_POST` observes a trusted base different from the effect's reviewed `base_commit` after the owned provisional Change Request is `CHANGE_REQUEST_OBSERVED` | Append `BASE_READ_POST/BASE_MISMATCH`; keep the owned provisional ref/Change Request and exact head; supersede the effect/Activity | `REMEDIATING`; plan `REBASE`; after its full gate, `APPROVED` creates a higher-generation `INITIAL` effect that updates the same provisional ref/Change Request; `T(FORGE_OBSERVATION, observation_id)` |
| `PUBLISHING` under `PIN` | any `INITIAL` generation's `BASE_READ_POST` successfully observes a trusted commit different from the effect's reviewed `base_commit` | Append `BASE_READ_POST/OBSERVED_SATISFIED` and `COMPLETE/COMPLETED`; retain pinned Candidate/ref/Change Request and record the observed base | `PR_MONITORING`; Publication becomes `ACTIVE`; `T(FORGE_OBSERVATION, observation_id)` |
| `PUBLISHING` under `SUPERSEDE_AT_BOUNDARY` | any `INITIAL` generation's `BASE_READ_POST` observes a trusted base different from the effect's reviewed `base_commit` after the owned provisional Change Request is `CHANGE_REQUEST_OBSERVED` | Append `BASE_READ_POST/BASE_MISMATCH`; retain provisional object/head, supersede effect/Activity, and capture/coalesce the base-only pending Snapshot; install nothing in this observation reduction | remain `PUBLISHING` with only its pending Snapshot continuation eligible; next `T(SPEC_SUPERSEDE, snapshot_id)` enters `REPLANNING`; after its full gate a higher `INITIAL` targets the same provisional object; current `T(FORGE_OBSERVATION, observation_id)` |
| `PUBLISHING` | any linked provisional or active Change Request ref mutation returns/observes a compare-and-swap mismatch | Append `CAS_MISMATCH` checkpoint and ordered `REF_HEAD`/`CHANGE_REQUEST_HEAD` observation; supersede the stale effect/Activity without overwrite | `PR_MONITORING`; reduce the observation and plan import/remediation as appropriate; `T(FORGE_OBSERVATION, observation_id)` |
| `PUBLISHING` | a newly committed `AMBIGUOUS` checkpoint proves the current effect's adapter result is unknown | Retain active Activity/effect and reconcile from that checkpoint with the same request identity | remain `PUBLISHING`; `T(PUBLICATION_CHECKPOINT, checkpoint_id)`; resolving an ambiguous pre-link create always returns through a fresh complete marker search rather than direct linkage |
| `PR_MONITORING` without cancellation, an active Publication mutation, current `CLOSE_REDUNDANT_PUBLICATION`, or current `REPAIR_RUN_MARKER` | any accepted individual observation other than `CHANGE_REQUEST_MARKER`, including merge or external close, targets a non-retained stable Change Request ID with the same valid marker/ref or may invalidate a duplicate proof | Retain the observation without terminalizing the retained Run; supersede any in-flight duplicate `RECONCILE` whose frozen search predates it, and schedule a new authenticated complete marker search; the individual observation cannot assert a complete-set digest | remain `PR_MONITORING`; `T(FORGE_OBSERVATION, forge_observation_id)`; marker-specific rows have precedence |
| `PR_MONITORING` without cancellation, an active Publication mutation, current `CLOSE_REDUNDANT_PUBLICATION`, or current `REPAIR_RUN_MARKER` | after ownership precedence, accepted all-positive `CHANGE_REQUEST_SEARCH_RESULT` has a `(complete_search_revision, duplicate_set_digest)` different from the Publication's stored pair | Supersede and fence any current `RECONCILE`, regardless of its prior reconciliation kind; plan exactly one controller duplicate `RECONCILE` bound to this complete search Observation and current Publication/effect generation | remain `PR_MONITORING`; `T(FORGE_OBSERVATION, forge_observation_id)` |
| `PR_MONITORING` without cancellation or an active Publication mutation | after ownership precedence, accepted all-positive `CHANGE_REQUEST_SEARCH_RESULT` has the same complete-search revision/set digest as the Publication's stored pair | Record the complete search as same-state audit input; retain any correctly fenced current cleanup and plan no `RECONCILE` or new cleanup | remain `PR_MONITORING`; `T(FORGE_OBSERVATION, forge_observation_id)` |
| `PR_MONITORING` without cancellation or an active Publication mutation | current duplicate `RECONCILE` produces `REDUNDANT_PUBLICATIONS_PROVEN` with a complete frozen member set | Complete `RECONCILE`; store the Fact ID, complete-search revision, and set digest on Publication; atomically set the Publication association to the Fact's bytewise-lowest retained ID/head, then plan one `CLOSE_REDUNDANT_PUBLICATION` for the first `CLOSE` member only, with its exact Fact/proof/head bindings and outbox | remain `PR_MONITORING`; `T(RECONCILIATION_FACT, reconciliation_fact_id)` |
| `PR_MONITORING` without cancellation or an active Publication mutation | current duplicate `RECONCILE` produces `NO_ACTIONABLE_DUPLICATE` | Complete `RECONCILE`; store its complete-search revision/set digest and Fact pointer on Publication; change no retained association and plan no cleanup | remain `PR_MONITORING`; `T(RECONCILIATION_FACT, reconciliation_fact_id)`; the same revision/digest cannot replan reconciliation |
| `PR_MONITORING` with current `CLOSE_REDUNDANT_PUBLICATION` | before close success, an accepted observation changes either object/head/marker/ref, the complete set, or the target's unreviewed proof | Supersede cleanup Activity/outbox without external mutation; a changed `CHANGE_REQUEST_SEARCH_RESULT` may plan fresh `RECONCILE`, while any individual observation only schedules the complete marker search | remain `PR_MONITORING`; `T(FORGE_OBSERVATION, forge_observation_id)` |
| `PR_MONITORING` with current `CLOSE_REDUNDANT_PUBLICATION` | authenticated `CHANGE_REQUEST_CLOSED` observation binds the exact cleanup Activity/operation, redundant stable ID, expected final head, marker/ref, Publication, and effect generation | Complete cleanup Activity; retain the canonical Publication association and Run state; schedule a fresh complete marker search instead of reusing the old Fact or planning `RECONCILE` from an individual close | remain `PR_MONITORING`; `T(FORGE_OBSERVATION, forge_observation_id)`; only a later changed `CHANGE_REQUEST_SEARCH_RESULT` may plan the next reconciliation, and this duplicate close is not Run closure |
| `PR_MONITORING` without cancellation, an active Publication mutation, current `CLOSE_REDUNDANT_PUBLICATION`, or current `REPAIR_RUN_MARKER` | accepted `CHANGE_REQUEST_MARKER` Observation for the exact linked stable ID/head/body revision proves exactly one desired marker | Record marker projection and supersede any current `RECONCILE`; change no ownership, Candidate, gate, or Effect | remain `PR_MONITORING`; `T(FORGE_OBSERVATION, forge_observation_id)` |
| `PR_MONITORING` without cancellation, an active Publication mutation, current `CLOSE_REDUNDANT_PUBLICATION`, or current `REPAIR_RUN_MARKER` | accepted `CHANGE_REQUEST_MARKER` Observation for the exact linked stable ID/head/body revision proves `MISSING` or `DUPLICATED_IDENTICAL`, the durable Publication/Project/ref/Effect association matches, and no incompatible v1, legacy, or human ownership claim exists | Supersede any current `RECONCILE`; plan one `REPAIR_RUN_MARKER` Activity and Effect-bound outbox with immutable `orcest.run-marker-repair/1` input; derive the desired marker from Run/Publication only | remain `PR_MONITORING`; `T(FORGE_OBSERVATION, forge_observation_id)` |
| `PR_MONITORING` without cancellation, an active Publication mutation, current `CLOSE_REDUNDANT_PUBLICATION`, or current `REPAIR_RUN_MARKER` | accepted `CHANGE_REQUEST_MARKER` Observation is not exact-repairable because ownership proof is incomplete or its marker set contains any non-identical/conflicting v1, legacy, or human claim | Mutate nothing; supersede any current `RECONCILE` and plan ownership `RECONCILE` bound to the exact Observation | remain `PR_MONITORING` pending reconciliation; `T(FORGE_OBSERVATION, forge_observation_id)`; only a later positive incompatible-ownership Fact may open the boundary |
| `PR_MONITORING` with current `REPAIR_RUN_MARKER` | authenticated `CHANGE_REQUEST_MARKER` Observation binds its Activity/operation and proves the same stable ID/head/ref with exactly one desired marker | Complete repair Activity; retain Publication, Candidate, gates, and Run state | remain `PR_MONITORING`; `T(FORGE_OBSERVATION, forge_observation_id)`; no Effect increment or Reconciliation Fact |
| `PR_MONITORING` with current `REPAIR_RUN_MARKER` | after the exact controller-bound success predicate and the higher-priority cancellation, retained-head advance, merge, and closure rows are excluded, an accepted observation or pre-call/adapter reread changes stable ID/ref/body revision/marker set/ownership proof, including a new same-marker object, or reveals an incompatible ownership claim | Persist/reduce the exact applicable Observation; supersede Activity/outbox without mutation; a changed `CHANGE_REQUEST_SEARCH_RESULT` plans duplicate `RECONCILE`, while individual evidence schedules a complete search or ownership reconciliation | remain `PR_MONITORING` or enter the positive-evidence ownership path according to the observation; `T(FORGE_OBSERVATION, forge_observation_id)` |
| any controller-Activity-owning active state | a failed Controller Operation Fact proves a definitive operation failure before a side effect was proven, and its category is permitted for that Activity by the Domain matrix; process restart, transient forge transport, or an ambiguous `PUBLISH`/`CLOSE_REDUNDANT_PUBLICATION`/`REPAIR_RUN_MARKER` response does not satisfy this predicate | Mark that controller Activity `FAILED`; preserve every side-effect precondition and expected revision; append Recovery Evidence with category exactly equal to the Fact | `RECOVERING`; apply the code-owned category tactic, including controller `RECONCILE` when applicable; `T(CONTROLLER_OPERATION, controller_operation_fact_id)` |
| any active Run with a current Run-bound Forge Observation Request | a source-unique Forge Request Failure Fact proves `TIMEOUT`, `RATE_LIMIT`, or `UNAVAILABLE` for one committed outbound attempt while the Request remains `PENDING` | Preserve Request/outbox identity and current controller Activity; append zero-counter `FORGE_TRANSIENT` Recovery Evidence selecting `WAIT_EXTERNAL` from the exact Fact | `RECOVERING`; next Evidence Transition creates `WAITING/FORGE_UNAVAILABLE`; `T(FORGE_REQUEST_FAILURE, forge_request_failure_fact_id)` |
| any safe worker-offer planning boundary | the latest exact-scope Budget Report under the installed Project policy is current and `EXHAUSTED` | Keep every affected Activity `PLANNED`; create no Attempt/outbox; append zero-counter `BUDGET` Recovery Evidence sourced by that exact Report | `RECOVERING`; only the next Evidence Transition creates `WAITING/BUDGET`; no claimed Attempt is interrupted |
| any safe worker-offer planning boundary | the exact-scope Budget Report is absent, stale-window, or policy-mismatched, so no durable accounting fact proves either availability or exhaustion | Keep every affected Activity `PLANNED`; create no Attempt/outbox, Recovery Evidence, Wait, or additional Transition. Retain the planning trigger's already-committed lifecycle state and make the durable offer reconciler wait for a valid Report | no state change beyond the causal planning Transition; startup and every accepted matching Report rescan these `PLANNED` Activities, and only an authenticated current Report may select the next branch |
| `RECOVERING` | latest unapplied Recovery Evidence selects `WAIT_EVIDENCE` and its exact `orcest.evidence-wake/1` target/minimum-sequence/predicate has no matching accepted Observation under the writer lock | Insert immutable `EVIDENCE` Wait sourced from that Evidence with required timer and event arms, exact resume state, and current bindings; set `Run.wait_condition_id` | `WAITING`; `T(RECOVERY_EVIDENCE, recovery_evidence_id)` |
| `RECOVERING` | latest unapplied Recovery Evidence selects `WAIT_EVIDENCE` but its under-lock exact predicate already has a matching accepted Observation at or above the minimum sequence | Insert no Wait; append one source-unique successor Recovery Evidence bound to the current Evidence and selecting the deterministic ordinary retry/replacement tactic for the same category | remain `RECOVERING`; next `T(RECOVERY_EVIDENCE, successor_recovery_evidence_id)` applies it; current `T(RECOVERY_EVIDENCE, recovery_evidence_id)` |
| `RECOVERING` | latest unapplied BUDGET Recovery Evidence selects `WAIT_BUDGET`, and the under-lock latest exact applicable Report is still its causal `EXHAUSTED` Report | Insert the immutable timer-plus-BUDGET_WINDOW Wait sourced from that Evidence and set `Run.wait_condition_id` | `WAITING`; `T(RECOVERY_EVIDENCE, recovery_evidence_id)` |
| `RECOVERING` | latest unapplied BUDGET Recovery Evidence selects `WAIT_BUDGET`, but the under-lock latest exact applicable Report is a later `AVAILABLE` or `EXHAUSTED` Report | Insert no Wait. For AVAILABLE append a successor Evidence selecting the origin-valid retry/resume tactic; for EXHAUSTED append source-unique BUDGET Evidence bound to the newer Report | remain `RECOVERING`; only the successor's later Transition continues; current `T(RECOVERY_EVIDENCE, recovery_evidence_id)` |
| `RECOVERING` | latest unapplied FORGE_TRANSIENT Recovery Evidence selects `WAIT_EXTERNAL`, and its under-lock exact Request/Schedule predicate has no later successful Observation or verified connectivity evidence | Insert the immutable timer-plus-FORGE Wait sourced from that Evidence and set `Run.wait_condition_id` | `WAITING`; `T(RECOVERY_EVIDENCE, recovery_evidence_id)` |
| `RECOVERING` | latest unapplied FORGE_TRANSIENT Recovery Evidence selects `WAIT_EXTERNAL`, but its under-lock predicate already has a current qualifying successful Observation or verified connectivity evidence | Insert no Wait; append one successor Recovery Evidence selecting the deterministic origin-valid retry/reconciliation tactic | remain `RECOVERING`; only the successor's later Transition continues; current `T(RECOVERY_EVIDENCE, recovery_evidence_id)` |
| `RECOVERING` | latest unapplied Recovery Evidence selects one typed wait tactic other than `WAIT_EVIDENCE`, `WAIT_BUDGET`, or FORGE_TRANSIENT `WAIT_EXTERNAL` | Insert immutable Wait Condition sourced from that evidence with exact resume state, bindings, and timer and/or wake predicate; set `Run.wait_condition_id` | `WAITING`; `T(RECOVERY_EVIDENCE, recovery_evidence_id)` |
| `RECOVERING` | persisted `EFFECT_ABSENT` Reconciliation Fact proves the side effect absent and original preconditions/revision still match | Complete `RECONCILE`; retain its negative observations | `origin`; plan allowed controller work with the same stable side-effect identity; `T(RECONCILIATION_FACT, reconciliation_fact_id)` |
| `RECOVERING` | ownership `RECONCILE` for a pre-link foreign deterministic-ref head produces `PRELINK_REF_IMPORTABLE`, with `observed_ref_commit` equal to both its causal `REF_HEAD` observation and immutable Activity input | Complete `RECONCILE`; retain exact foreign SHA and safe-fetch/ownership evidence; plan controller `IMPORT` with deterministic Activity key | `REMEDIATING`; after import, run the full ordinary gate; `T(RECONCILIATION_FACT, reconciliation_fact_id)` |
| `RECOVERING` | ownership `RECONCILE` produces `PRELINK_REF_RECONSTRUCT_REQUIRED`, bound to that same exact causal observation/Activity commit, because the safely fetched head fails pinned-base relationship or Candidate admission without positive incompatible ownership evidence | Complete `RECONCILE`; append Recovery Evidence selecting `RECONSTRUCT_FOREIGN_HEAD`, bound to the foreign observation and validation failure | remain `RECOVERING`; next `T(RECOVERY_EVIDENCE, recovery_evidence_id)` plans one reconstruction Activity; never retry `IMPORT` for that same evidence |
| `REMEDIATING` | failed Controller Operation Fact proves a pre-link `IMPORT` encountered a deterministic pinned-base or Candidate-admission validation failure not present in its earlier proof | Mark `IMPORT` failed; append Recovery Evidence selecting `RECONSTRUCT_FOREIGN_HEAD` with the exact foreign observation/failure | `RECOVERING`; next `T(RECOVERY_EVIDENCE, recovery_evidence_id)` plans reconstruction; never retry `IMPORT` for that same head/failure |
| `RECOVERING` | ownership `RECONCILE` produces `OWNERSHIP_CONFLICT` after autonomous repair cannot prove a safe owner/import path and binds the exact Project/ref/observed Change Request/valid Orcest v1 marker/Publication/effect | Complete `RECONCILE`; insert the exactly bound `PUBLICATION_OWNERSHIP_CONFLICT` Human Boundary sourced from this fact | `NEEDS_HUMAN`; `T(RECONCILIATION_FACT, reconciliation_fact_id)` |
| `WAITING/CAPACITY` | the current condition was created because an `OFFERED` Attempt expired unclaimed, and a newer ordered Health Observation proves one compatible healthy worker | Clear the current Wait; copy its `resume_state` to `recovery_origin_state`; bind the exact Wait and Health input; append zero-counter `CAPACITY` Recovery Evidence selecting `RETRY_EXECUTION` | `RECOVERING`; the next `T(RECOVERY_EVIDENCE, recovery_evidence_id)` creates generation `g + 1` and resumes; current `T(HEALTH_OBSERVATION, health_observation_id)` |
| `WAITING/CAPACITY` with non-empty panel-slot membership | a newer ordered Health Observation satisfies the Wait's capacity scope and all Snapshot/Candidate/panel bindings remain current | Clear the Wait; copy its `resume_state`; freeze the newly consulted highest-applicable Health membership on `CAPACITY` Recovery Evidence selecting `STAFF_PANEL`, whose exact `resumed_wait_condition_id` inherits the immutable planned Activity/slot membership | `RECOVERING`; current `T(HEALTH_OBSERVATION, health_observation_id)`; the next Evidence Transition alone may staff |
| `RECOVERING` | latest unapplied Recovery Evidence selects `STAFF_PANEL` and its frozen Health membership plus inherited exact Wait-slot membership prove one complete legal independent staffing selection for every named unfilled slot | Atomically create one higher/current `OFFERED` Attempt and outbox for every named slot in canonical slot order; all assignments must be mutually legal as a complete set | Evidence origin `REVIEWING` or `ADJUDICATING`; `T(RECOVERY_EVIDENCE, recovery_evidence_id)` |
| `RECOVERING` | latest unapplied Recovery Evidence selects `STAFF_PANEL` but no complete legal assignment exists for every still-current named slot | Offer none; insert a new `WAITING/CAPACITY` with newly frozen ordered Health and exact still-unfilled Activity/slot memberships/digests | `WAITING`, resume the Evidence origin; `T(RECOVERY_EVIDENCE, recovery_evidence_id)`; quorum/Decision state is unchanged |
| `WAITING` | a persisted Timer Fact for the current condition's exact `not_before_ms` satisfies every binding | Clear current Wait; copy `resume_state` to `recovery_origin_state`; bind exact Wait/Timer and append typed Recovery Evidence | `RECOVERING`; next `T(RECOVERY_EVIDENCE, recovery_evidence_id)` revalidates/resumes or selects another tactic; current `T(TIMER_FACT, timer_fact_id)` |
| `WAITING` | a global `HEALTH_OBSERVATION_EXPIRY` Timer Fact lists this Run and its current Wait Condition explicitly binds that expiring observation | Clear current Wait; copy `resume_state` to origin; make the observation ineligible; bind exact Wait/Timer and append typed Recovery Evidence from the re-evaluated durable health set | `RECOVERING`; next `T(RECOVERY_EVIDENCE, recovery_evidence_id)`; current `T(TIMER_FACT, timer_fact_id)` |
| any state | a global `HEALTH_OBSERVATION_EXPIRY` Timer Fact lists this Run but the formerly bound Wait Condition was superseded before this fanout member reduced | Append a same-state audit Transition; do not clear or replace current work/condition | remain in the current state; `T(TIMER_FACT, timer_fact_id)` |
| `WAITING` | a Health Observation, Forge Observation, authenticated `AVAILABLE` Budget Report fanout member, Secret Version, Storage Restoration Fact, or Management Command satisfies the current condition's exact wake predicate and bindings | Clear current Wait; copy its `resume_state` to origin; bind the exact Wait/source on Run and append typed Recovery Evidence with that same source | `RECOVERING`; next `T(RECOVERY_EVIDENCE, recovery_evidence_id)` revalidates/resumes; current trigger is respectively `HEALTH_OBSERVATION`, `FORGE_OBSERVATION`, `BUDGET_REPORT`, `SECRET_VERSION`, `STORAGE_RESTORATION`, or `MANAGEMENT_COMMAND` with its canonical source ID |
| `WAITING` | a candidate wake is stale, below the minimum revision, expired, or mismatched | Persist/audit the input without clearing the current Wait Condition | remain `WAITING`; append the same-state Transition for that input's exact closed trigger kind/ID; no replacement condition and no policy change |
| `WAITING` | a persisted specification, policy, forge, management, Attempt-terminal, or controller-operation input makes the current Wait Condition inapplicable | Clear `Run.wait_condition_id`; retain the immutable condition and superseding input | state selected by its applicable row; use that input's exact closed Domain trigger kind/ID, never the Wait Condition output |
| any nonterminal state except an unrelated `NEEDS_HUMAN` boundary | exact-object `STORAGE`/`SECRET` `UNAVAILABLE` Health Observation from the committed Health Probe Request/outbox -> Fact+Observation integrity chain proves a live referenced object missing or corrupt | Fence consumers; preserve the current/resume origin; append exact-source Recovery Evidence: `CANDIDATE_ARTIFACT` or `WORKFLOW_BLOB` maps to `STORAGE`, while `SECRET_VERSION` maps to `CREDENTIAL`; select `WAIT_EXTERNAL` | `RECOVERING`; current `T(HEALTH_OBSERVATION, health_observation_id)` only; the separate next `T(RECOVERY_EVIDENCE, recovery_evidence_id)` creates `STORAGE_RECOVERY` or `SECRET_RECOVERY` Wait |
| any state | a Health Probe Fact Run fanout member receives its exact Health Observation after this Run's generation, object/Secret version, Candidate, Publication, Wait, Boundary, or scope binding was superseded, or the Observation is otherwise unrelated to current work | Advance the durable fanout cursor through this member and append the required audit Transition; change no state, counters, Evidence, Wait, or work | remain current; `T(HEALTH_OBSERVATION, health_observation_id)` |
| any active state without cancellation intent | ordered `DEPENDENCY_STATE` Observation says any required dependency is open or unknown | Store/replace the Run pending-dependency pointer and set digest; do not fence a claimed Attempt | remain current; at a safe boundary schedule `T(INTERNAL, boundary_transition_sequence)`; current `T(FORGE_OBSERVATION, forge_observation_id)` |
| any active state | newer ordered `DEPENDENCY_STATE` Observation proves the pending dependency set satisfied before waiting | Clear the matching pending-dependency pointer; retain both observations | remain current; `T(FORGE_OBSERVATION, forge_observation_id)` |
| any active state at a safe boundary with a pending dependency pointer | deterministic continuation rechecks that exact observation/set remains unsatisfied or unknown | Clear pending pointer; append `EXTERNAL_DEPENDENCY` Recovery Evidence selecting `WAIT_EXTERNAL`, with origin equal the boundary state | `RECOVERING`; next Evidence Transition creates `WAITING/EXTERNAL_DEPENDENCY`; `T(INTERNAL, boundary_transition_sequence)` |
| any active state without cancellation intent | an accepted specification/workflow/base/policy Forge Observation or Policy Update requires Snapshot capture, whether its resulting `supersession_key` differs from or equals the installed key, excluding the higher-priority base-only movement after Publication `ACTIVE` | Always retain the new ordered immutable Snapshot; deterministically replace or clear `Run.pending_snapshot_id`; when differing inputs meet a claimed Attempt or fenced controller Activity, set `supersede_requested = true` and its exact capture Transition sequence, otherwise leave it false and schedule the safe-boundary install; coalescing back to installed clears all three | remain in current state for this reduction; respectively `T(FORGE_OBSERVATION, forge_observation_id)` or `T(POLICY_UPDATE, policy_update_id)`; no current or later Result/Terminal emits semantic work ahead of the separate `T(SPEC_SUPERSEDE, snapshot_id)` |
| any post-link state with Publication `ACTIVE` | trusted base alone advances under `SUPERSEDE_AT_BOUNDARY` while specification/workflow/policy inputs are unchanged | Record/reduce the base observation and update the forge projection; do not capture, pend, or install a Snapshot | remain current; base motion affects work only through later exact conflict/feedback remediation; `T(FORGE_OBSERVATION, forge_observation_id)` |
| any active state at safe boundary without cancellation intent | pending Snapshot changes specification text, workflow configuration, or eligible pre-`ACTIVE` trusted-base inputs | Supersede old unfinished work; increment specification generation; install Snapshot; clear pending Snapshot plus supersede flag/sequence and current Candidate | `REPLANNING`; plan `REPLAN`; `T(SPEC_SUPERSEDE, snapshot_id)` |
| any active state at safe boundary without cancellation intent | explicit Policy Update produced a pending Snapshot whose specification, workflow, and exact base are unchanged and only effective policy differs | Supersede old unfinished work and old Plan; increment specification generation; install Snapshot; clear pending Snapshot plus supersede flag/sequence; move the current Candidate, if any, to read-only `policy_replan_candidate_id` context and clear it as current; invalidate every Verification/Review/Adjudication Receipt and Consensus Decision for gating under the old policy | `REPLANNING`; always plan `REPLAN` bound to the new Snapshot/policy and optional Candidate context; `T(SPEC_SUPERSEDE, snapshot_id)`; the Policy Update was already reduced once when it captured this Snapshot |
| `APPROVED` under `REBASE_BEFORE_PUBLICATION`, before completion of the `INITIAL` Publication Effect | deterministic continuation from the Transition that entered `APPROVED` finds the latest accepted and already-reduced trusted `BASE_HEAD` differs from Candidate base | Bind the exact latest base observation and supersede any stale unlinked publication plan | `REMEDIATING`; plan `REBASE`; `T(INTERNAL, approved_transition_sequence)`; never reuse the `BASE_HEAD` observation trigger |
| `APPROVED` under `SUPERSEDE_AT_BOUNDARY`, before Publication `ACTIVE` | deterministic continuation finds the latest base-only pending Snapshot whose `supersession_key` differs | Retain pending Snapshot and schedule its installation; install nothing in this continuation | remain `APPROVED`; next `T(SPEC_SUPERSEDE, snapshot_id)` enters `REPLANNING`; current `T(INTERNAL, approved_transition_sequence)` |
| `REMEDIATING` | accepted successful `REBASE` Attempt Result produces an admitted Candidate | Select new Candidate and invalidate old gates as an effect of accepting the Result | `VERIFYING`; plan the single full `VERIFY` Activity; `T(ATTEMPT_RESULT, attempt_id)` |
| any active state | latest unapplied Recovery Evidence selects `ENTER_HUMAN_BOUNDARY` and proves one allowlisted exceptional boundary after all applicable autonomous tactics | Insert immutable Human Boundary sourced from that evidence, set `Run.human_boundary_id`, fence incompatible work, and supersede any current Wait Condition | `NEEDS_HUMAN`; Projection intent; `T(RECOVERY_EVIDENCE, recovery_evidence_id)`, never the created Boundary ID |
| `NEEDS_HUMAN` with `SPECIFICATION_CONFLICT` or `UNSATISFIABLE_REQUIREMENTS` | ordered authorized changing `WORK_ITEM_SNAPSHOT` Observation | Its `FORGE_OBSERVATION` Transition captures/updates the pending Snapshot and remains `NEEDS_HUMAN`; it installs nothing | `NEEDS_HUMAN`; separately schedule `T(SPEC_SUPERSEDE, snapshot_id)` |
| `NEEDS_HUMAN` with `SPECIFICATION_CONFLICT` or `UNSATISFIABLE_REQUIREMENTS` | exact pending Snapshot captured by the authorized observation remains current | Install that Snapshot, insert `SPECIFICATION_AMENDED` Human Resolution sourced from the observation/editor, clear boundary/current Candidate, invalidate gates, and supersede unfinished work | `REPLANNING`; plan `REPLAN`; canonical `T(SPEC_SUPERSEDE, snapshot_id)`; Resolution idempotency remains `forge_observation_id` |
| `NEEDS_HUMAN` with `REQUIRED_SECRET_OR_PERMISSION` | a newer persisted Secret Version and immutable creation Receipt exactly satisfy the packet and the registered verifier proves it current | Insert `SECRET_OR_PERMISSION_PROVIDED` Resolution; clear boundary; copy its `resume_state` into recovery origin; bind Boundary/Resolution/source and append `CREDENTIAL` Recovery Evidence | `RECOVERING`; next `T(RECOVERY_EVIDENCE, recovery_evidence_id)` validates/resumes; current `T(SECRET_VERSION, secret_version_key)` |
| any state | this Run is in frozen Secret Version membership but its matching Secret Wait or Boundary was superseded before fanout reduction | Append a same-state audit Transition; do not clear or replace current state | remain current; `T(SECRET_VERSION, secret_version_key)` |
| `NEEDS_HUMAN` with `REQUIRED_SECRET_OR_PERMISSION` | accepted `RESOLVE_HUMAN_BOUNDARY` grants the exact missing permission | Insert Resolution; clear boundary; copy resume state; append exact-source `CREDENTIAL` Recovery Evidence | `RECOVERING`; next evidence reduction; current `T(MANAGEMENT_COMMAND, command_id)` |
| `NEEDS_HUMAN` with `INTEGRITY_FAILURE` | persisted Storage Restoration Fact matches and revalidates the exact object | Insert Resolution; clear boundary; copy resume state; append exact-source `STORAGE` Recovery Evidence | `RECOVERING`; next evidence reduction; current `T(STORAGE_RESTORATION, storage_restoration_fact_id)` |
| any state | this Run is in frozen Storage Restoration Fact membership but its matching storage/secret Wait or Integrity Boundary was superseded before fanout reduction | Append a same-state audit Transition; do not clear or replace current state | remain current; `T(STORAGE_RESTORATION, storage_restoration_fact_id)` |
| `NEEDS_HUMAN` with `PUBLICATION_OWNERSHIP_CONFLICT` | authenticated Command supplies exact `ORCEST_V1` ownership resolution | Insert Resolution; clear boundary; copy resume state; append exact-source `POLICY` Recovery Evidence selecting `RECONCILE` | `RECOVERING`; next evidence reduction plans ownership `RECONCILE`; current `T(MANAGEMENT_COMMAND, command_id)` |
| `NEEDS_HUMAN` other than the specification rows | authenticated Command accepts an applicable typed Resolution | Insert Resolution; clear boundary; copy resume state; bind both Human IDs/source and append the closed category/tactic Recovery Evidence | `RECOVERING`; next evidence reduction revalidates/resumes; current `T(MANAGEMENT_COMMAND, command_id)` |
| `MERGED` with an `ACTIVE` Terminal Duplicate Cleanup Reservation | the sole cleanup continuation names a current `RECORD_ONLY` Action, or the prior member's terminal Action has advanced the cursor | Complete `RECORD_ONLY` as `RETAINED_AUDIT` when applicable; advance exactly one ordinal; create the next code-selected Action and mutation Outbox, or mark the Reservation `COMPLETED` when no member remains | remain `MERGED`; `T(INTERNAL, prior_cleanup_transition_sequence)`; no Candidate, Publication association, or terminal outcome changes |
| `MERGED` with an `ACTIVE` Reservation and current `CLOSE` Action | authenticated `CHANGE_REQUEST_CLOSED` Observation binds the exact Reservation/Action, stable ID, expected head, marker/ref, Publication/effect, and operation digest | Mark Action `COMPLETED/CLOSED`, advance no further work in this transaction, and schedule the sole cleanup continuation | remain `MERGED`; `T(FORGE_OBSERVATION, observation_id)` then one `INTERNAL` continuation selects the next ordinal |
| `MERGED` with an `ACTIVE` Reservation and current `DETACH_MARKER` Action | authenticated `CHANGE_REQUEST_MARKER` Observation binds the exact Reservation/Action and CAS preimage and proves this Run marker absent while every other body/marker element is preserved | Mark Action `COMPLETED/MARKER_DETACHED` and schedule the sole cleanup continuation | remain `MERGED`; `T(FORGE_OBSERVATION, observation_id)` then one `INTERNAL` continuation selects the next ordinal |
| `MERGED` with an `ACTIVE` Reservation and current mutation Action | an ordered object/head/body/marker Observation disproves the frozen CAS preimage | Mark the Action `SUPERSEDED`; mutate nothing; commit a fresh exact read/complete-search Request under the Reservation authority | remain `MERGED`; `T(FORGE_OBSERVATION, observation_id)`; its later result creates a higher Action generation or a bounded `RECORD_ONLY` Action |
| `MERGED` with an `ACTIVE` Reservation | any accepted cleanup read/search Observation is stale, unrelated, or cannot strengthen the exact member proof | Retain the observation and either continue bounded backoff or choose `RECORD_ONLY`; never reopen the Run or weaken the selected merge | remain `MERGED`; `T(FORGE_OBSERVATION, observation_id)` |
| any nonterminal state | authenticated ordered Forge Observation proves the exact linked run-owned Change Request merged, with stable ID equal to current `Publication.change_request_external_id` | Store merge observation/commit and terminal outcome; set Publication `CLOSED`; supersede every unfinished Activity/Attempt, Wait, Boundary, and Effect | `MERGED`; terminal; `T(FORGE_OBSERVATION, observation_id)` |
| any nonterminal state without a cancellation intent | authenticated ordered Forge Observation proves the exact linked run-owned Change Request closed without merge, with stable ID equal to current `Publication.change_request_external_id` and not a `CLOSE_REDUNDANT_PUBLICATION` target | Store closure observation/final head and terminal outcome; set Publication `CLOSED`; supersede every unfinished Activity/Attempt, Wait, Boundary, and Effect | `CLOSED`; terminal; `T(FORGE_OBSERVATION, observation_id)` |
| any nonterminal state before Publication `CHANGE_REQUEST_OBSERVED` | no cancellation intent exists; accepted `CANCEL` Management Command or Work Item closure; no create-ready/ambiguous checkpoint; and either no Publication/create workflow exists or the current search checkpoint is `CHANGE_REQUEST_SEARCH/OBSERVED_ABSENT` backed by its exact `CHANGE_REQUEST_ABSENT` Observation | Fence/supersede work; store outcome; close any existing local Publication projection; `REF_ABSENT` never proves CR absence | `CANCELLED`; canonical management/forge trigger |
| any nonterminal state before Publication `CHANGE_REQUEST_OBSERVED` | no cancellation intent exists; accepted `CANCEL` Management Command or Work Item closure; a Publication/create workflow exists; and immediate cancellation is not legal because no current search checkpoint backed by exact `CHANGE_REQUEST_ABSENT` proves absence or a `CHANGE_REQUEST_CREATE/REQUEST_READY`/`AMBIGUOUS` checkpoint makes a side effect possible | Persist the exact management/forge cancellation source; fence semantic work; retain the stable Publication/create/search identity and plan search-only `CLOSE_PUBLICATION` reconciliation | `PR_MONITORING` with cancellation-in-progress projection and no ordinary remediation dispatch; trigger is the exact `MANAGEMENT_COMMAND` or `FORGE_OBSERVATION` source |
| any nonterminal state at or after Publication `CHANGE_REQUEST_OBSERVED` | accepted `CANCEL` Management Command and no cancellation intent exists | Persist `MANAGEMENT_COMMAND/command_id` as the Run cancellation source; fence semantic work; plan idempotent `CLOSE_PUBLICATION` and cleanup outbox bound to exact owned Change Request, marker, ref, effect generation, and last observed head | `PR_MONITORING` with cancellation-in-progress projection and no ordinary remediation dispatch; `T(MANAGEMENT_COMMAND, command_id)` |
| any nonterminal state with cancellation pending | a fresh authenticated `CANCEL` Management Command passes its current Run Transition fence | Insert the accepted command and same-state audit Transition; retain the immutable first cancellation source and the existing `CLOSE_PUBLICATION` Activity/outbox without replanning or closing an unverified object | remain in cancellation-in-progress state; `T(MANAGEMENT_COMMAND, command_id)` |
| any nonterminal state with cancellation pending | an ordered specification or Policy Update input is accepted | Capture/audit the immutable Snapshot or policy input and pending pointer as applicable, but do not install a generation, select a Candidate, or plan semantic work | remain cancellation-in-progress; use the input's canonical trigger |
| any nonterminal state with cancellation pending | an accepted Forge Observation is not an exact merge/close input and does not satisfy the current cleanup discovery/head-replacement predicate | Retain the observation and append a same-state audit Transition; do not plan import, rebase, remediation, or ordinary monitoring work | remain cancellation-in-progress; `T(FORGE_OBSERVATION, observation_id)` |
| cancellation pending before Change Request observation | successful `CLOSE_PUBLICATION` Fact names matching `CHANGE_REQUEST_ABSENT` and proves none exists | Complete cleanup; set Publication `CLOSED`; supersede remaining work; reject `REF_ABSENT`/unbound token | `CANCELLED`; `T(CONTROLLER_OPERATION, controller_operation_fact_id)` |
| cancellation pending before Change Request observation | authenticated discovery observation proves the exact run-owned Change Request and current head | Supersede the reconciliation-only `CLOSE_PUBLICATION`; atomically plan one new `CLOSE_PUBLICATION` Activity/outbox whose immutable inputs bind that observation, Change Request ID, marker, ref, effect generation, and head | remain cancellation-in-progress in `PR_MONITORING`; `T(FORGE_OBSERVATION, observation_id)` |
| cancellation pending with a head-bound `CLOSE_PUBLICATION` | a newer authenticated head observation arrives before close is proven | Supersede the stale cleanup Activity/outbox; atomically plan one replacement bound to the new observation and head; never mutate the old Activity or close using its stale fence | remain cancellation-in-progress in `PR_MONITORING`; `T(FORGE_OBSERVATION, observation_id)` |
| cancellation pending | authenticated ordered observation proves the exact owned Change Request closed unmerged | Complete cleanup; set Publication `CLOSED`; supersede remaining work; store cancellation outcome | `CANCELLED`; `T(FORGE_OBSERVATION, observation_id)` |

No other state transition is legal. An unknown trigger is recorded as rejected
input or leaves the Run unchanged; it cannot fall through to an agent-selected
state.

Controller restart is not a reducer input. Startup resumes a still-`ACTIVE`
`PUBLISH` from its highest durable checkpoint/outbox. It appends no Transition
unless new external I/O first commits an exact `AMBIGUOUS` checkpoint or Forge
Observation covered by the rows above.

Cancellation rows have priority whenever `Run.cancellation_source_kind/source_id`
is non-null, except that an exact linked merge or the complete-search positive
merged-terminal fact has higher terminal precedence. Exact close observations
and the current cancellation cleanup reconciliation may advance termination;
cleanup failure may use the ordinary
controller recovery path. All other specification, policy, base, head,
feedback, and worker inputs are captured/audited but cannot install a new
generation or plan semantic work. The explicit `without cancellation intent`
guards above make this priority part of the closed reducer rather than table
order.

A complete marker search with a positive owned merged terminal member has
higher precedence than cancellation, live cardinality, ownership defects on
other members, current-head remediation, and ordinary publication progress:
the forge merge already happened. The selected bytewise-lowest positive merged
member fixes the terminal outcome permanently. Its Reservation may close only
positive-owned reliance-free live duplicates, may detach only the exact Run
marker from a positive-owned relied-on duplicate under body/head CAS, and must
record every incompatible, incomplete, or unsafe member without mutation.
Cleanup ambiguity never reopens the Run. Each response/input still receives
one same-state `MERGED` Transition, and one source-unique `INTERNAL`
continuation advances at most one member; restart reconstructs both from the
Reservation/Action/Outbox ledger.

The Reservation is the sole post-terminal workflow authority for these
duplicates. Run terminalization closes ordinary Run Activities, Attempts,
Waits, and schedules, but does not delete or detach an ACTIVE Reservation or
its selected Search Member proof, ordered cleanup Members, Actions, Outboxes,
or Reservation-bound polling schedules. Only its cursor continuation may
advance cleanup or mark the Reservation `COMPLETED`; no terminal-Run garbage
collection may treat the Run's terminal state as proof that this graph is
finished.

A Storage Restoration Fact is object-scoped. Its transaction freezes the
canonically sorted affected Run IDs, and the controller applies the matching
wait or Human Resolution row independently to each listed Run in that order
using the same fact ID. Replay converges through per-Run Transition uniqueness;
one Run's reduction cannot suppress another's.
The transaction also appends the exact `STORAGE` or `SECRET` `RECOVERED`
Health Observation sourced by that Fact. This ordered health projection is
not reduced separately: lifecycle authority remains
`T(STORAGE_RESTORATION, storage_restoration_fact_id)`, preventing two
Transitions for one repair.

## Attempt and Activity transitions

Run state and Activity/Attempt state are related but distinct. The following
rules apply in every Run state. Every first-time worker Result acceptance row
(`valid success`, `valid verification error`, `valid failure`, `valid
non-progress Candidate`, and `valid abstention`) shares the precondition
`controller_now_ms < Attempt.execution_deadline_ms`, in addition to
the row's stated checks. The controller checks an identical already-accepted
Result replay before this deadline rule; that replay returns the original
response after the deadline only while controller time remains strictly before
the capability-auth expiry. Any other first submission at or after the
deadline but strictly before that expiry is rejected and durably ledgered only
when it is schema-valid and authenticated. If the Attempt is still the current
`CLAIMED` generation,
that transaction fences it `EXPIRED`, inserts the `EXECUTION_DEADLINE` Attempt
Terminal Fact, and records timeout Recovery Evidence; an already terminal
Attempt follows the audit-only rule below. At or after capability-auth expiry,
the request is unauthenticated and creates no late ledger or workflow row.

For a sweeper-driven claim or execution deadline, the controller first
persists the matching Attempt-scoped Timer Fact, then inserts the Attempt
Terminal Fact with `source_kind = TIMER_FACT` and
`source_id = timer_fact_id`, and only then reduces
`T(ATTEMPT_TERMINAL, attempt_terminal_fact_id)`. A first late Result request
instead atomically persists the canonical late-disposition Result Request and
uses its durable `result_request_id` as the terminal fact source. Same key/body
replay returns the stored rejection; conflicting body or claim bindings under
that key are rejected without another fact or Transition.
If a non-Result cause already terminalized the Attempt and no Attempt Result
was accepted, the late-request ledger and its own source-unique terminal fact
record `ALREADY_TERMINAL` as audit-only: that Fact reduces to one same-state
`T(ATTEMPT_TERMINAL,attempt_terminal_fact_id)` with no terminal-state effect or
recovery counters and replays the stored bounded response.
Accepted-Result replay/conflict lookup has precedence. A sweeper fact for an
already terminal Attempt is likewise one same-state audit Transition.

The v1 Attempt capability remains cryptographically authentic for exactly
`86_400_000` ms after `execution_deadline_ms`. During that grace it authorizes
only the Result endpoint's exact accepted replay or bounded late Result Request
and timeout rejection. All launch, source, upload, rotation, liveness, and new
output authority ends at the execution deadline. At or after the stored
capability-auth expiry the request is unauthenticated and inserts no late
ledger; the persisted Timer Fact path remains authoritative.

### Normative Activity/Attempt state matrix

This matrix is the authoritative lifecycle for every worker Activity. A
nonterminal Attempt is exactly an `OFFERED` or `CLAIMED` Attempt; all other
Attempt states are terminal. The Activity and its current Attempt are changed
under the same writer transaction, so no committed projection may expose a
state pair outside this matrix.

| Current pair | Trigger and required precondition | Atomic result |
| --- | --- | --- |
| `PLANNED` + no nonterminal Attempt | An eligible initial or retry generation passes the current mode, selected `ACTIVE` key, capacity, latest exact-scope authenticated `AVAILABLE` Budget Report, and all immutable input gates | Insert the next Attempt as `OFFERED` and atomically change Activity `PLANNED -> READY`; commit its Outbox row in the same transaction. |
| `PLANNED` + no nonterminal Attempt | Any offer gate is closed, including Controller Mode, issuance key, capacity, or absent/expired/`EXHAUSTED` budget evidence | Keep Activity `PLANNED`; create neither Attempt nor Outbox. Retain the exact pending continuation/evidence needed for a later deterministic recheck. A current `EXHAUSTED` Report may source only the typed Evidence path; absent/expired/mismatched evidence waits for a real Report and creates no invented Fact or Wait. |
| `READY` + current `OFFERED` Attempt | Authenticated claim arrives strictly before the immutable claim deadline and matches the exact assignment/session | Atomically change Activity `READY -> ACTIVE`, Attempt `OFFERED -> CLAIMED`, and persist the Claim/capability fences. |
| `ACTIVE` + current `CLAIMED` Attempt | Accepted Result has the kind-required, schema-valid output and all bindings are current | Atomically set Attempt `SUCCEEDED` and Activity `SUCCEEDED`; the reducer may then plan the next distinct Activity. |
| `READY` + current `OFFERED` Attempt in `REVIEWING`/`ADJUDICATING` | A persisted `CLAIM_DEADLINE` Terminal Fact applies while another unfilled panel slot has a `CLAIMED` Attempt | Expire only the due Attempt and atomically return its Activity to `PLANNED`; preserve the claimed peer, create no Recovery Evidence, Wait, Attempt, or offer, and retain only the coalesced Candidate/panel/kind staffing pointer. The Run remains `REVIEWING`/`ADJUDICATING`; this exception takes precedence over generic deadline recovery. |
| `READY` or `ACTIVE` + current nonterminal Attempt | Accepted failure or abstention, or a deadline/loss fact, requires autonomous recovery and the Activity is not being abandoned permanently, except for the panel claim-deadline exception above | Terminalize the Attempt (`FAILED`, `ABSTAINED`, or `EXPIRED` as applicable), atomically return Activity to `PLANNED`, then append the typed Recovery Evidence so the Run enters `RECOVERING`. The ordering is logical within the one writer transaction. |
| `READY` or `ACTIVE` + current nonterminal Attempt | The controller deliberately chooses permanent `FAILED`, `SUPERSEDED`, or `CANCELLED` handling and will never retry or complete this Activity | Terminalize the Attempt, set the Activity to that permanent terminal state, and emit no recovery tactic for that Activity. |
| `PLANNED` + no nonterminal Attempt | A Recovery Evidence reduction selects a retry/replace tactic for the same Activity and all offer gates pass | Create the next Attempt generation and atomically change `PLANNED -> READY` with that generation `OFFERED` and its Outbox row. |
| `PLANNED` + no nonterminal Attempt | Recovery selects retry/replace but a mode, issuance-key, capacity, budget, or other offer gate is closed | Keep Activity `PLANNED` and create no Attempt or Outbox; the same durable evidence/continuation remains eligible for a later recheck. |
| Activity `FAILED`, `SUPERSEDED`, or `CANCELLED` | Any result, retry, deadline, loss, or recovery trigger for that Activity | Reject or retain the trigger as bounded audit only. It MUST NOT create an Attempt, retry, or success, and MUST NOT change the Activity back to a nonterminal or successful state. |

Thus an accepted `VERIFY` `ERROR`, an accepted worker failure or abstention,
an execution deadline, and authoritative worker loss all follow the recovery
row: their current Attempt is terminalized and an `ACTIVE`/`READY` Activity is
`PLANNED` before the Run enters `RECOVERING`, except that a claim deadline with
another claimed unfilled panel peer follows the explicit panel row and creates
no Evidence. A schema-invalid or malformed
Result is not accepted at all and leaves the claimed pair unchanged. A
successful Result whose kind-required output is valid completes its Activity;
the same-commit/non-progress case is not such output and is handled as a
controller-detected failure with a new recovery Activity, never by retrying a
completed Activity. No retry path may create a second nonterminal Attempt for
one Activity.

Controller Mode `MAINTENANCE` performs exact-key read-only Result replay before
all other Result logic. An unseen key, including a would-be semantic replay,
creates no ledger or lifecycle row and returns HTTP `503` with exactly these
five fields:

```json
{
  "protocol": "orcest.error/1",
  "code": "CONTROLLER_MAINTENANCE",
  "retryable": true,
  "message": "controller is in maintenance mode",
"retry_after_seconds": 60
}
```

The v1 wire value is the exact integer `60`. No additional protocol field is
permitted.

Every Result-endpoint row below is considered only after authentication,
schema validation, and semantic-conflict checks. Only a request admitted to
one of the five closed Result Request dispositions claims/inserts the global
`result_request_id`: schema-valid completed/failure/abstention paths use
`ACCEPTED`, a semantic replay key points to that same Result without invoking
the reducer, and deadline/upload fences use the other dispositions. Malformed
or schema-invalid requests, authentication/authorization denials, and a
semantic conflict such as `RESULT_ALREADY_ACCEPTED` create no Result Request
row. Same-key replay/conflict lookup may read an existing row but does not
admit a new one.

Before planning any new `OFFERED` Attempt or delivery outbox, the writer reads
the durable Controller Mode and Capability Registry in the same serialized
decision. `RUNNING` and
`INTAKE_PAUSED` permit offer planning; `DISPATCH_PAUSED`, `DRAINING`, and
`MAINTENANCE` leave the Activity `PLANNED` and create neither Attempt nor
outbox. The Registry must also name an existing selected `ACTIVE` issuance key;
an absent/unselected/retired/revoked key likewise leaves `PLANNED`. These gates
apply to initial PLAN, build/remediation, retry, panel staffing, and startup rebuild;
it never consumes a claim deadline while dispatch is paused.
Claim-deadline replacement additionally freezes those projections on its Fact.
A blocked terminal
Transition leaves one pending `INTERNAL` dispatch continuation alongside its
Recovery Evidence; the mode/key reconciler reduces that Evidence/continuation
only when both current gates pass. The serialized recheck may then apply the
Evidence and, in a later reduction, offer from current evidence or create the
applicable capacity Wait. It cannot reuse the Terminal Fact trigger or create
replacement work directly.

| Trigger | Preconditions | Durable result |
| --- | --- | --- |
| dispatch | Activity `READY`; current `OFFERED` Attempt and outbox committed | Redis delivery MAY occur; no Run transition is implied. |
| first claim | Attempt `OFFERED`, current generation, controller time before immutable claim deadline, compatible authenticated worker/session, exact assignment | Under the logical-Secret and Capability Key Registry CAS, insert immutable Claim; freeze exact current source/provider Secret versions and Registry revision/key, deadlines/capabilities, response digest, and model launch nonce; atomically set Attempt/Activity claimed/active and offer delivered. Return source bootstrap and capabilities with provider material withheld. |
| claim replay | Attempt already `CLAIMED`; exact same authenticated session, `attempt_claim_id`, and request digest | Return the identical immutable non-secret contract. Rematerialize only exact frozen Secret versions and normalized claims/JTIs/key/revision/expiries: before deadline source and capabilities, until auth expiry Result-only Attempt capability, afterward none. Never substitute newer credentials, signer, deadline, nonce, or assignment; insert nothing. |
| conflicting claim-key reuse | Existing Claim has the same caller key but different request digest, session, or binding | Return `409 IDEMPOTENCY_CONFLICT`; insert nothing. |
| already-claimed claim | Attempt already `CLAIMED` under another key/session, or generation is stale | Return `409 ATTEMPT_ALREADY_CLAIMED` or stale response; insert nothing and reveal no sensitive Claim material. |
| valid launch attestation | Exact current model-backed claim/session; signed by pinned runner; copied launch digest; unique fresh identities; no parents; unconsumed nonce; controller time before deadline | Insert/bind Attestation and consume nonce atomically. `AVAILABLE.provider` includes exact non-secret `provider`, `model`, `provider_account_ref`, `secret_id`, and `version`; the latter pair equals the Claim-frozen provider SecretRef. Accepted replay after workflow expiry returns `EXPIRED`/provider-null; after crypto expiry the original token is signature-equality proof only under the same runner/session and retained ACTIVE/RETIRED verifier, never authentication or mutation. No Run Transition. |
| Candidate upload expired | Result names the exact Attempt-bound Candidate Upload and controller time is at or after its `expires_at_ms` | Atomically set the unused upload `EXPIRED`, insert Result Request disposition `UPLOAD_EXPIRED`, and return the closed HTTP `410 UPLOAD_EXPIRED` body; create no Result, Candidate, Receipt, Recovery Evidence, or Transition. |
| valid success | Exact current claim/fence/input bindings and, for model-backed work, exact accepted Launch Attestation; required typed output durably exists and semantically fills the Activity | Insert Attempt Result with the exact `launch_attestation_id`; Attempt `SUCCEEDED`; Activity `SUCCEEDED`; invoke reducer. |
| valid verification error | Exact current claim/fence/input bindings; `VERIFY` output is schema-valid Receipt `ERROR` | Insert Receipt and `FAILED_RETRYABLE/VERIFICATION_ERROR` Result; terminalize Attempt and atomically return Activity to `PLANNED`; append Recovery Evidence and enter `RECOVERING`. Only the Evidence Transition may offer a higher generation. |
| valid failure | Exact current claim/fence/input bindings and, for model-backed work, exact accepted Launch Attestation | Insert Attempt Result with the exact `launch_attestation_id`; terminalize Attempt and atomically return Activity to `PLANNED` unless the controller explicitly chooses permanent Activity `FAILED`, `SUPERSEDED`, or `CANCELLED`; invoke recovery only for the recoverable branch. |
| valid non-progress Candidate | Exact current claim/fence/input/Launch-Attestation bindings; Candidate admission resolves to the Activity's existing same-commit Candidate rather than a new required Candidate | Insert the Attempt Result as bounded non-progress artifact evidence; Attempt may be `SUCCEEDED` for that valid artifact, but the semantic replacement Activity is terminally `FAILED` with `REPEATED_NON_PROGRESS`; keep Candidate generation and receipt eligibility unchanged. Append threshold-aware Recovery Evidence for a distinct recovery Activity and enter `RECOVERING`; never offer a higher Attempt generation for the completed producer Activity. |
| valid abstention | Exact current claim/fence/input/Launch-Attestation bindings; Review or Adjudication Receipt is schema-valid and controller-derived `fills_slot = false` | Insert `ABSTAINED` Result/Receipt; terminalize Attempt and atomically return Activity to `PLANNED`; append `REVIEW_DISAGREEMENT` Recovery Evidence and enter `RECOVERING`. Only its next Evidence Transition may create a higher Attempt or typed Wait. |
| stale result before execution deadline | Capability authenticates but generation/claim/Run binding is stale | Insert/replay global Result Request `STALE_ATTEMPT` with closed reason/body; no Result, output, Terminal Fact, Recovery Evidence, or Transition. |
| claim deadline with capacity | Persisted `CLAIM_DEADLINE` Attempt Terminal Fact matches the current `OFFERED` Attempt, freezes `OFFER_ALLOWED + COMPATIBLE_AVAILABLE`, and no claimed unfilled panel peer exists | Attempt `EXPIRED`; atomically return Activity to `PLANNED`; append zero-counter Recovery Evidence selecting `RETRY_EXECUTION` from that Fact's exact health/provider/mode/key evidence. The next Evidence Transition creates generation `g + 1` and performs the atomic `PLANNED -> READY` offer. |
| claim deadline without capacity | Persisted `CLAIM_DEADLINE` Attempt Terminal Fact matches the current `OFFERED` Attempt and freezes `OFFER_ALLOWED + NO_COMPATIBLE_AVAILABLE` | Attempt `EXPIRED`; atomically return Activity to `PLANNED`; append zero-counter Recovery Evidence selecting `WAIT_CAPACITY` from the exact frozen health membership. A non-panel Activity's next Evidence Transition creates its bound `WAITING/CAPACITY`; a panel does so only after proving no peer is `CLAIMED`, superseding peer `OFFERED` Attempts, and freezing every unfilled slot. No branch advances counters. |
| claim deadline with dispatch/key gate closed | Persisted `CLAIM_DEADLINE` Attempt Terminal Fact freezes `MODE_BLOCKED` or `ISSUANCE_KEY_UNAVAILABLE`, and no claimed unfilled panel peer exists | Attempt `EXPIRED`; atomically return Activity to `PLANNED`; append zero-counter Recovery Evidence selecting `RETRY_EXECUTION`; create no Attempt, outbox, or Wait. Retain one pending dispatch continuation for the mode/key reconciler, which may later reduce the Evidence and perform the `PLANNED -> READY` offer only after current gates pass. |
| claim deadline with claimed panel peer | Persisted `CLAIM_DEADLINE` Attempt Terminal Fact matches the current `OFFERED` Attempt and another unfilled panel slot has a `CLAIMED` Attempt | Expire the due Attempt and return its Activity to `PLANNED`; create no Recovery Evidence, Wait, Attempt, or offer; retain only the coalesced panel-staffing pointer. The Run remains `REVIEWING`/`ADJUDICATING`, and only its later pointer reduction may staff or wait. |
| terminal fact after terminal | Source-unique later deadline/loss Fact or `RESULT_AFTER_TERMINAL` Fact from `ALREADY_TERMINAL` Result Request | Append exactly one same-state `T(ATTEMPT_TERMINAL, fact_id)`; no counters, Evidence, Wait, terminal projection, or offer changes. |
| execution deadline | Persisted `EXECUTION_DEADLINE` Attempt Terminal Fact matches the current `CLAIMED` Attempt and proves `controller_now_ms >= execution_deadline_ms` | Fence as `EXPIRED`; revoke every non-Result authority; retain only cryptographic Result authentication until exact capability-auth expiry for accepted replay or a late Result Request/rejection; record ordered timeout Recovery Evidence; enter `RECOVERING`. |
| verified worker loss | Persisted `WORKER_LOST` Attempt Terminal Fact references a `WORKER_SESSION/LOST` Health Observation sourced from the accepted authenticated Worker Loss Report and matches the current Attempt/claimant | Fence Attempt; enter `RECOVERING` without waiting for the execution deadline. Capacity `UNAVAILABLE` and Redis/session silence cannot use this row. |
| Redis lease loss | Controller time remains strictly before the persistent deadline and no authoritative worker-loss observation exists | Rebuild disposable lease/notification; do not supersede the durable claim merely because Redis forgot it. |
| liveness | Authenticated exact current session/Attempt before execution deadline with a monotonically increasing liveness sequence | Update disposable current-control/lease state; repeated or lower sequence is a no-op returning current control. Liveness is not a durable command, has no request-idempotency ledger, and cannot extend a durable deadline or authority. |

Only Attempt generation increases on an execution retry. A semantically new
plan, Candidate, finding set, reviewer slot, or Forge Observation requires a
new Activity. Worker result outcomes are only `SUCCEEDED`,
`FAILED_RETRYABLE`, `FAILED_PERMANENT`, and `ABSTAINED`; `EXPIRED` and
`SUPERSEDED` are controller decisions. Once cancellation, specification
supersession, or another fence commits, a worker's acknowledgement is a stale
response and cannot be accepted as a `cancelled` or other terminal Result.

### Controller Activity transitions

`IMPORT`, `PUBLISH`, `CLOSE_PUBLICATION`, `CLOSE_REDUNDANT_PUBLICATION`, and
`RECONCILE` are controller
Activities and never have Attempts or worker Results. Before external I/O, the outbox executor
conditionally moves the current Activity from `READY` to `ACTIVE` and commits
its stable operation identity and exact preconditions. Every `PUBLISH` or
publication `RECONCILE` identity includes `(publication_id,
effect_generation)`; an observation or response for an older generation is
audit-only. The reducer consumes only a canonical Forge Observation,
Publication Checkpoint, Reconciliation Fact, or Controller Operation Fact; a
mutable Activity row or adapter response is never a trigger. When a successful
`IMPORT` has no more-specific canonical fact, Candidate admission and its
Controller Operation Fact commit before the reducer consumes that fact.

`IMPORT` is a controller read-and-admit operation, not publication authority.
It is bound to one exact ordered Forge Observation and observed head, fetches
that object without running it, applies ordinary Candidate admission, and
succeeds only when the imported Candidate commits durably. A changed
observation fences the Activity. Import failure follows controller recovery;
import success selects no trust shortcut and is followed by the full configured
verification and consensus gate.

`PUBLISH` is a resumable checkpointed Publication Effect. Each mutation's
`REQUEST_READY` checkpoint and stable request identity commit before the call;
each observed result commits as the next checkpoint. A controller restart or
ambiguous response leaves `PUBLISH` `ACTIVE`, reads the highest durable
checkpoint, and reconciles external state before resuming the same effect. It
does not fail the Activity merely because the process restarted. Only a
`COMPLETED` checkpoint moves it to `SUCCEEDED`.

A definite non-resumable controller failure enters `RECOVERING` with a
`RECONCILE` Activity. If the desired effect exists, the reducer attaches it and
advances using the ordinary success transition; if absent and preconditions
still match, it creates the allowed next controller work with the same stable
side-effect identity. A linked/provisional Change Request compare-and-swap
mismatch records a Forge Observation, supersedes the stale effect, and returns
through `PR_MONITORING` for deterministic replanning. A pre-link
deterministic-ref mismatch instead enters `RECOVERING` for ownership
`RECONCILE`, then safe import/full gating, pinned-base reconstruction/full
gating, or the exceptional positive-evidence ownership boundary.
Neither path overwrites the observed head. An Activity
already marked `FAILED` is never changed back to `SUCCEEDED`, and a blind retry
is illegal.

`CLOSE_REDUNDANT_PUBLICATION` is a one-object reconciliation repair, not the
Run-cancellation close. Its Activity/outbox and
`orcest.redundant-publication-cleanup/1` input commit before the adapter call.
The adapter operation is
`close_change_request_if_exact_unreviewed_duplicate`; immediately before the
call the controller must re-prove the Run remains in `PR_MONITORING`, the
current Publication/effect generation, complete-set retained-lowest rule,
exact retained and duplicate heads,
Project/ref/marker equivalence, and the duplicate's unreviewed status. It may
close only the one exact duplicate named by the Activity. A failed precondition
or typed mismatch first persists the exact current Forge Observation evidence;
reducing that observation supersedes the cleanup and schedules a fresh complete
marker search. Only a later changed `CHANGE_REQUEST_SEARCH_RESULT` starts fresh
`RECONCILE`.
A definitive evidence-less failure uses a failed Controller Operation Fact;
an ambiguous response reconciles the same stable operation. Only its exact
authenticated close observation is success. After success, another complete
marker search produces the sole input that may plan fresh reconciliation and
choose whether any next cleanup exists. No duplicate close
changes Run terminal outcome.

`REPAIR_RUN_MARKER` is a separate exact-CAS projection repair. It is legal only
for the already-linked current Change Request when an accepted marker
Observation proves either no Orcest marker or byte-identical duplicate copies,
the durable Run/Publication/Project/ref/head/Effect association proves one
owner, and no incompatible v1, legacy, or human claim exists. The desired
marker is code-derived. Activity/outbox commit before the adapter call;
mismatch becomes a new Observation, ambiguity retains the operation, and only
an exact controller-bound Marker Observation proving one desired copy is
success. It cannot adopt an unlinked object, transfer ownership, rewrite code,
or increment the Effect.

Any Transition out of `PR_MONITORING`, including current-retained-head
feedback/remediation, cancellation, merge, or closure, atomically supersedes a
pending redundant-cleanup or marker-repair Activity/outbox before planning its
higher-precedence work. A delivery that races after that fence fails the
current-Activity check and performs no mutation.

## Verification and consensus branches

### Verification failure

- `FAIL` with actionable evidence transitions to `REMEDIATING` and plans a
  `REMEDIATE` Activity bound to the exact Candidate and failed check receipts.
- A schema-valid `ERROR` Receipt is accepted only with a `FAILED_RETRYABLE`
  Result and `VERIFICATION_ERROR`, then enters `RECOVERING` for a higher
  generation of the same frozen `VERIFY` Activity. A missing or malformed
  Receipt/Result is rejected before Result acceptance and leaves the Attempt
  `CLAIMED` for a corrected pre-deadline submission; only its deadline/loss
  terminal fact can later enter recovery. Tool unavailability enters this
  branch only through a schema-valid `ERROR` Receipt and the exact failure
  pairing. None is a test failure or pass.
- Repeated identical failure after remediation advances the recovery ladder to
  `DIAGNOSING` rather than producing the same unbounded loop.

### Review outcomes

- Missing reviewer capacity enters `WAITING` with reason `CAPACITY` after
  configured compatible substitutions are considered. The approval threshold
  is unchanged.
- One or more unresolved blockers produce `REMEDIATE` unless evidence is
  disputed or contradictory, in which case the decision is `ADJUDICATE`.
- `ABSTAIN` never fills a v1 reviewer slot or approval requirement and never
  becomes approval due to timeout. v1 has no advisory reviewer slots.
- A blocker may be cleared only by deterministic reproduction evidence or an
  adjudication receipt accepted under pinned policy, not by a summarizer.

### Adjudication

Adjudicators receive the exact Candidate, normalized disputed findings, and
available executable evidence. They do not inherit another agent's lifecycle
recommendation. The reducer applies the structured outcome:

- confirmed blocker -> `REMEDIATING`;
- every disputed blocker decisively `OVERRULE`d and no blocker remains -> close
  the old panel, allocate the next positive `panel_round`, and return to
  `REVIEWING` with one fresh `REVIEW` Activity for every gating slot; no prior
  approval or other Receipt carries into that panel;
- insufficient evidence or abstention -> return the same sole `ADJUDICATE`
  Activity/slot `default` to `PLANNED`, then create a higher Attempt generation
  using the next eligible exact assignment only through `PLANNED -> READY`, or
  enter `WAITING/EVIDENCE` or `WAITING/CAPACITY` with no offer;
- newly discovered blocker -> `REMEDIATING`; or
- proven specification/policy boundary -> candidate for the exceptional
  human-boundary test below.

## Specification changes

The controller periodically or eventfully observes the Work Item and trusted
base configuration. Server policy changes enter an active Run only through an
explicit ordered Policy Update. For each accepted input it computes
`specification_hash`, `workflow_hash`, effective `policy_hash`, and the combined
`generation_input_hash` plus the policy-specific `supersession_key` using the
same schema as the current Snapshot.

### No semantic specification change

If `supersession_key` is unchanged, the observation is recorded through its
one same-state Forge Transition and the Run continues. Label changes,
non-opted-in comments, and YAML formatting changes do not supersede a
generation. Base-only movement does not change the key under
`REBASE_BEFORE_PUBLICATION` or `PIN`; under `SUPERSEDE_AT_BOUNDARY` it does.

### Changed specification

If specification text, normalized workflow/policy configuration, or a base
input covered by the selected policy changes so that `supersession_key`
changes, the controller captures a new immutable Snapshot and sets a durable
supersession request. It MUST NOT hot-reload the new inputs into an active
agent invocation.

Publication `ACTIVE` suppresses only a trusted-base-only capture under
`SUPERSEDE_AT_BOUNDARY`. An authorized specification edit, changed normalized
workflow configuration, or explicit Policy Update is still captured,
coalesced, and installed through the ordinary safe-boundary
`SPEC_SUPERSEDE` path after Publication becomes `ACTIVE`; it invalidates old
plans/gates and may resolve an applicable specification Human Boundary. A
combined input is not “base-only”: if specification, workflow, or policy also
changed, the capture includes the latest consumed base and remains eligible.

Work Item observations are first ordered by their per-target Forge Observation
sequence; Policy Updates are ordered by their per-Project policy-update
sequence. The single writer assigns every resulting capture a Run-local
`snapshot_sequence`, which is the cross-source coalescing order and never an
arrival timestamp. For every accepted input the controller retains the captured
Snapshot and appends a Transition naming the previous/resulting pending IDs. A
newer capture whose `supersession_key` differs from the installed Snapshot
becomes `Run.pending_snapshot_id`; a newer capture returning to the installed
key clears it. An older out-of-order source fact is audit history and cannot
replace a later accepted capture. Thus Work Item A -> B -> C installs C, while
A -> B -> A cancels pending supersession; B remains immutable audit evidence.

When a Policy Update triggers capture, the controller composes it with both the
latest accepted ordered `WORK_ITEM_SNAPSHOT` observation and the latest
accepted trusted `BASE_HEAD` observation, not the installed Snapshot. This
includes newer pending specification/config or base input. The capture stores
the Policy Update, Work Item observation, and base observation identities, so
pending specification B and base C followed by policy P produces pending
`B + C + P`; installing or reconstructing `A + P` is forbidden.

Here and in every rebase/approval rule, latest trusted `BASE_HEAD` is selected
by the greatest consuming Run `transition_sequence` across eligible Work Item
and Publication targets, including the generation-1 Snapshot base explicitly
anchored at the `ADMIT` sequence. Their separate per-target observation
sequences and timestamps are not comparable.

The safe boundary is the first point at which no current Attempt can still
produce an accepted lifecycle result:

- immediately, when no Attempt is `CLAIMED`; or
- after the current Attempt reports, expires, or is explicitly fenced.

For specification/workflow change, at that boundary the reducer MUST
perform the following atomically in the separately replayable
`T(SPEC_SUPERSEDE,pending_snapshot_id)` transaction; the capture's earlier
Forge/Policy Transition and any boundary `INTERNAL` continuation may only
schedule this transaction:

1. mark unstarted Activities from the prior generation `SUPERSEDED`;
2. keep any newly uploaded bytes as orphan/audit material but refuse to select
   a Candidate produced under the superseded inputs;
3. increment `specification_generation`;
4. insert the unique Snapshot Generation that installs the exact latest
   pending Snapshot and set `Run.current_snapshot_id`;
5. clear `Run.pending_snapshot_id`, the current Candidate for gating, and any
   Wait Condition or Human Boundary made stale by the old bindings; and
6. transition to `REPLANNING` and plan `REPLAN`.

Prior plans, Candidates, and receipts remain audit history and are not carried
forward. A specification change is autonomous input, not a human escalation.

### Explicit policy-only update

Changing server configuration or a Project registration policy does not
silently alter an active Run. The old Snapshot, effective `POLICY_JSON`, and
`policy_hash` remain authoritative until the controller persists an ordered
Policy Update, recomputes its intersection with the Run's pinned repository
configuration, captures a new Snapshot, and reaches the safe boundary above.

The reducer classifies the capture as policy-only exactly when
`specification_hash`, `workflow_hash`, `base_ref`, and `base_commit` equal the
installed Snapshot, `policy_hash` differs, and therefore `supersession_key`
differs. It then atomically supersedes
unfinished old-policy Activities, increments `specification_generation`,
installs the new Snapshot Generation, clears stale Wait/Human pointers, and
invalidates every old-policy Verification, Review, Adjudication, and Consensus
record for gating. It always enters `REPLANNING` and creates one mandatory
`REPLAN` bound to the new Snapshot/policy. A prior Candidate is cleared as
current and retained only in `policy_replan_candidate_id` as immutable planning
context; the old Plan is never carried implicitly. After a valid new Plan, the
reducer rechecks exact policy-only identity. If specification, workflow, base,
and Candidate content/commit are still identical, it reselects that Candidate,
skips `BUILD`, and traverses the full ordinary
`VERIFYING -> REVIEWING -> AGGREGATING -> APPROVED` path. Otherwise it clears
the context and uses the ordinary `BUILDING` path. Every review subject derives
from the new Plan. No approval carries over, even when the new policy is weaker.
Repository input cannot create a Policy Update or opt out of this gate
invalidation.

## Base changes

Base movement does not by itself change `generation_input_hash`, but it changes
`supersession_key` under `SUPERSEDE_AT_BOUNDARY`. The controller
first loads and normalizes workflow policy from the observed trusted base; a
changed `workflow_hash` follows specification supersession above. When policy
is unchanged, the pinned repository policy selects one of these code-owned
behaviors:

| Policy | Deterministic behavior |
| --- | --- |
| `REBASE_BEFORE_PUBLICATION` | **v1 default.** Continue current work against the pinned base. Before publication, if the trusted base has advanced, plan `REBASE` onto the exact observed commit, admit a new Candidate, and repeat all pre-publication gates. |
| `PIN` | Continue and attempt publication against the pinned base; a forge conflict later enters remediation. |
| `SUPERSEDE_AT_BOUNDARY` | Before Publication `ACTIVE`, capture a new base in a base-only Snapshot, coalesce it by its base-sensitive `supersession_key`, then install it only through `SPEC_SUPERSEDE` at the safe boundary. After `ACTIVE`, base-only movement is observation-only. |

An unknown or moving base revision enters `WAITING` or `RECOVERING`; it never
authorizes publication against an unobserved commit. A rebase always creates a
new Candidate and invalidates earlier receipts.

`REBASE_BEFORE_PUBLICATION` applies through completion of the `INITIAL`
Publication Effect. A newly created/discovered owned Change Request is
provisional in `CHANGE_REQUEST_OBSERVED`; the required `BASE_READ_POST` still
uses this policy. A mismatch preserves that owned provisional object, rebases
and reruns the full gate, then creates a higher `INITIAL` effect that updates
the same ref/Change Request. Only a matching post-link base read completes the
effect and makes the Publication `ACTIVE`.

Both `BASE_READ_PRE` and `BASE_READ_POST` apply the selected policy exactly.
For `PIN`, any successful trusted-base read is
`OBSERVED_SATISFIED` regardless of commit equality, and the effect continues.
For `REBASE_BEFORE_PUBLICATION`, a differing commit is `BASE_MISMATCH` and
selects exact-base rebase/full gating. For `SUPERSEDE_AT_BOUNDARY`, a differing
commit is `BASE_MISMATCH`; the controller supersedes the effect, captures the
base-only pending Snapshot, then installs it only through the separate
`SPEC_SUPERSEDE` safe-boundary Transition and replans. A post-read mismatch under either non-PIN policy keeps
the owned provisional ref/Change Request/head for its later higher-generation
`INITIAL` effect.

After `ACTIVE`, base movement alone—including under
`SUPERSEDE_AT_BOUNDARY`—is recorded as forge state but does not capture, pend,
or install a Snapshot and does not
restart the initial-publication base check or rewrite the branch. A merge
conflict, CI result, or review finding on the exact active head enters ordinary
PR remediation; any resulting `REBASE`/Candidate traverses the full ordinary
gate and an `UPDATE` Publication Effect.

## Dependencies and external waiting

If a required dependency becomes open or unknown after admission, its
`FORGE_OBSERVATION` Transition stores the exact observation ID, canonical
dependency-set digest, and creating Transition sequence in the Run's durable
pending-dependency pointer. It does not interrupt a claimed Attempt. At the
next safe Activity boundary, a uniquely keyed `INTERNAL` continuation consumes
that pointer, appends `EXTERNAL_DEPENDENCY` Recovery Evidence selecting
`WAIT_EXTERNAL`, and enters `RECOVERING`; the next Evidence Transition creates
the immutable Wait. Its `wake_identity` names every dependency and minimum
forge revision. A newer all-satisfied observation clears the pending pointer
before the boundary, or satisfies the current Wait and enters the ordinary
Recovery-Evidence resumption path. The pointer is never Redis-only and cannot
be silently dropped by an unrelated Transition.

Forge, dependency, provider, budget, and capacity unavailability use the same waiting
mechanism. Entering `WAITING` atomically inserts the [canonical Wait
Condition](domain-model.md#wait-condition), sets `Run.wait_condition_id`, and
records `T(created_from_kind, created_from_id)` using the exact persisted input
that the condition names; the newly created `wait_condition_id` is an output,
never the Transition trigger. The immutable condition contains:

- a typed `reason`;
- a `resume_state`;
- a `not_before_ms`, an exact `wake_kind` plus `wake_identity`, or both;
- exact current specification, Candidate, policy, Forge Observation, and other
  applicable bindings; and
- a digest over the complete predicate and bindings.

When both the timer and event arms are present, they are candidate-wake
alternatives: the exact due Timer Fact OR the exact matching event can enter
the satisfaction check. Neither arm waits for the other, and either path still
revalidates the complete condition and current bindings.

### Wait reasons

```text
CAPACITY
RATE_LIMIT
BUDGET
BACKOFF
EXTERNAL_DEPENDENCY
FORGE_UNAVAILABLE
STORAGE_RECOVERY
SECRET_RECOVERY
EVIDENCE
```

The [Domain reason/wake compatibility matrix](domain-model.md#wait-condition)
is exhaustive. In particular, `BUDGET` always has the persisted reset timer
plus `BUDGET_WINDOW` identity bound to one exhausted Report and minimum later
sequence; `FORGE_UNAVAILABLE` always has a bounded retry timer plus `FORGE`
identity bound to the causal Forge Request Failure Fact and exact Schedule;
and timer-only `BACKOFF` has no event wake identity. An implementation cannot
pair a reason with another wake kind.

Budget usage authority exists only through the authenticated
[Budget Report](domain-model.md#budget-report) ledger. An `EXHAUSTED` Report
blocks new offers at a safe planning boundary and sources `BUDGET` Recovery
Evidence. An absent, expired, old-window, or policy-mismatched Report leaves
the Activity `PLANNED` and creates no invented Evidence or Wait. A reset or
Report-expiry Timer only makes reconciliation eligible; it does not assume
zero consumption. Only a later applicable unexpired `AVAILABLE` Report may
wake the Wait or permit the ordinary offer gate. The report fanout and its
per-Run `BUDGET_REPORT` Transitions are restartable from SQLite.

Transient forge read/search/poll failures exist only through a
[Forge Request Failure Fact](domain-model.md#forge-request-failure-fact).
Run-bound Facts source `FORGE_TRANSIENT` Recovery Evidence; Project discovery
and post-terminal cleanup Facts update only their durable Request retry
projection. A raw timeout or adapter callback cannot create a Wait, Recovery
Evidence, Health Observation, or Transition.

Pending CI is not a Wait Condition in v1. The Run remains `PR_MONITORING`, and
the controller's durable observation schedule obtains an authenticated,
head-bound `CHANGE_REQUEST_FEEDBACK` Forge Observation; that observation, not a
synthetic `CI` wake, drives the next reducer decision.

Capacity wake input exists only through the canonical Capacity Report ledger.
One report transaction validates the authenticated principal/sequence/body,
inserts its stable ordered Health Observation IDs, reduces affected Runs in
canonical order, and stores the replay response. Same-body replay returns
those IDs and wake results; conflicting idempotency identity or a stale unseen
sequence writes nothing. Capacity `UNAVAILABLE` may block offers but never
fences a claim. Claim fencing before deadline requires the separate
authenticated Worker Loss Report transaction, its exact
`WORKER_SESSION/LOST` Health Observation, and matching Attempt Terminal Fact.

A timer or observation only proposes satisfaction. The reducer clears
`Run.wait_condition_id` only when the exact persisted trigger meets the current
condition's time/revision predicate and every binding still matches, then
enters `RECOVERING` and revalidates the Snapshot, Candidate, receipts, health,
and external revision before choosing `resume_state`. A stale or mismatched
trigger is audit-only. A specification/Candidate/policy/publication change,
cancellation, terminal transition, or a new recovery strategy that makes the
predicate inapplicable supersedes the condition by clearing the Run reference
and recording the superseding trigger; the immutable condition remains audit
history. Redis presence, absence, or a bare wake notification never satisfies
or destroys a wait.

The deadline sweeper never invokes the reducer from wall clock or Redis state.
For each due current `WaitCondition.not_before_ms`,
`HealthObservation.expires_at_ms`, `BudgetReport.expires_at_ms`, Attempt
claim/execution deadline, or `RecoveryEvidence.next_eligible_at_ms`, it first
inserts the canonical Timer Fact with the exact scope and `fired_for_ms`. Only
a Wait deadline or Health expiry directly reduces
`T(TIMER_FACT, timer_fact_id)`. Budget Report expiry has no Run Transition; it
makes that Report ineligible and causes planned-offer reconciliation to await
a newer authenticated Report. An Attempt deadline
creates the source-bound Attempt Terminal Fact and reduces only
`T(ATTEMPT_TERMINAL, attempt_terminal_fact_id)`. Recovery eligibility makes
the exact Recovery Evidence eligible and applies its already selected tactic
only through `T(RECOVERY_EVIDENCE, recovery_evidence_id)`. A Wait timer may
satisfy only its bound current condition. A Health-expiry timer removes that observation from fallback
eligibility and re-evaluates only current Wait Conditions that explicitly bind
it; it neither retracts an already planned/`OFFERED` Activity nor manufactures
an opposite health observation. Because one global Health Observation may bind
waits in many Runs, its Timer Fact has `run_id = NULL` and is reduced once per
such Run in the fact's frozen `affected_run_ids` order; each Run writes its own
Transition using the same fact ID. After restart or Redis loss, the controller scans
those durable deadline columns and creates any missing due Timer Facts using a
`STARTUP_RECONCILIATION` pass. Uniqueness on the scoped deadline makes
concurrent or repeated sweeps converge.

“Due” is uniformly `controller_now_ms >= fired_for_ms`. Claim and Result
acceptance require controller time strictly before the corresponding deadline,
so a unique Timer Fact created at equality cannot preempt valid work or
deadlock expiry.

## Autonomous recovery

Exhausting one tactic selects another tactic. It does not terminate the Run or
automatically enter `NEEDS_HUMAN`.

### Recovery inputs

The controller classifies failures using code-owned categories:

```text
WORKER_LOST
TIMEOUT
PROVIDER_TRANSIENT
PROVIDER_RATE_LIMIT
CAPACITY
BUDGET
INVALID_RESULT
CREDENTIAL
SOURCE_READ
VERIFICATION_ERROR
VERIFICATION_FAILURE
REPEATED_NON_PROGRESS
REVIEW_DISAGREEMENT
BASE_CONFLICT
FORGE_TRANSIENT
EXTERNAL_DEPENDENCY
STORAGE
INTEGRITY_SUSPECTED
POLICY
```

Free-form agent text may contribute evidence but cannot choose the category.
Classification uses protocol result codes, adapter errors, tool exit data,
deadlines, and validated structured receipts.

`BUDGET` is sourced only by an authenticated Budget Report or by resumption of
the exact Budget Wait it created; neither worker output nor an in-memory usage
counter can select it. `FORGE_TRANSIENT` is sourced only by a Run-bound Forge
Request Failure Fact. A failed Controller Operation Fact copies one of its
closed Domain categories byte-for-byte into Recovery Evidence and can never
claim `FORGE_TRANSIENT` for a timeout or ambiguous response.

The accepted worker failure-class mapping is closed and deterministic in v1:

| Worker failure class | Required reducer category |
| --- | --- |
| `INFRASTRUCTURE` | `WORKER_LOST` |
| `PROVIDER_UNAVAILABLE` | `PROVIDER_TRANSIENT` |
| `PROVIDER_RATE_LIMIT` | `PROVIDER_RATE_LIMIT` |
| `INCOMPATIBLE_WORKER` | `CAPACITY` |
| `INVALID_AGENT_OUTPUT` | `INVALID_RESULT` |
| `VALIDATION_FAILURE` | `INVALID_RESULT` |
| `CREDENTIAL_UNAVAILABLE` | `CREDENTIAL` |
| `SOURCE_READ_FAILED` | `SOURCE_READ` |
| `VERIFICATION_ERROR` | `VERIFICATION_ERROR` |
| `BASE_CONFLICT` | `BASE_CONFLICT` |
| `POLICY_DENIED` | `POLICY` |
| `SPECIFICATION_CONFLICT` | `POLICY` |
| `MISSING_AUTHORITY` | `POLICY` |
| `INTEGRITY_FAILURE` | `INTEGRITY_SUSPECTED`; the structured failure must name one exact live `CANDIDATE_ARTIFACT`, `WORKFLOW_BLOB`, or `SECRET_VERSION` target and can authorize only the typed probe intent below |

`WORKER_LOST` has exactly two typed source paths; both use the same closed
category and neither adds an enum. An accepted worker `INFRASTRUCTURE` Result
terminalizes its current Attempt as `FAILED` through
`T(ATTEMPT_RESULT, attempt_id)` and appends `WORKER_LOST` Recovery Evidence
sourced by that Attempt Result. It does not create a `WORKER_LOST` Attempt
Terminal Fact and does not write `terminal_reason = WORKER_LOST`. The other
path is an authenticated pool-manager Worker Loss Report: its committed
`WORKER_SESSION/LOST` Health Observation creates the `WORKER_LOST` Attempt
Terminal Fact, and that Fact appends Terminal-Fact-sourced Recovery Evidence
and is the only path that sets `terminal_reason = WORKER_LOST` without a
Result. Capacity `UNAVAILABLE`, Redis/session silence, and an unverified
worker disappearance are neither source.

The allowed Activity, evidence, and Result-outcome combinations are the ones in
[worker protocol](worker-protocol.md#failure-results). Any other combination is
an invalid Result, not a new recovery category. In particular,
`SPECIFICATION_CONFLICT` and `MISSING_AUTHORITY` are worker evidence routed
first through autonomous policy recovery. `INTEGRITY_FAILURE` is only a
suspicion: its Recovery Evidence selects `PROBE_INTEGRITY`, which commits the
exact-object Health Probe Request/outbox. Only the resulting Fact/Observation
may classify confirmed artifact/blob failure as `STORAGE` or confirmed Secret
failure as `CREDENTIAL`; the worker Result cannot assert either category or
create a storage/secret Wait directly. None may directly enter `NEEDS_HUMAN`.

For `PROVIDER_RATE_LIMIT`, `failure_retry_after_ms` is absolute Unix time. At
Result acceptance the reducer freezes
`next_eligible_at_ms = min(max(failure_retry_after_ms, accepted_at_ms),
accepted_at_ms + max_provider_rate_limit_wait_ms)` from the installed policy.
That value becomes the Evidence and rate-limit Wait deadline. It is never
clamped to or treated as an extension of the completed Attempt's execution
deadline.

Each Recovery Evidence row selects exactly one closed tactic. Its Activity
mapping is deterministic:

| `selected_tactic` | New Activity or exact non-Activity effect |
| --- | --- |
| `RECONCILE` | controller `RECONCILE`; a still-active checkpointed `PUBLISH` instead resumes the same Activity |
| `REDELIVER` | no new Activity or Attempt; redeliver the current `OFFERED` Attempt outbox |
| `RETRY_EXECUTION` | no new Activity; after the Activity is `PLANNED`, create the next Attempt generation only by the atomic `PLANNED -> READY` offer transition under the same Activity |
| `REPLACE_CAPACITY` | no new Activity; after the Activity is `PLANNED`, create the next Attempt generation with the next compatible execution assignment only by the atomic `PLANNED -> READY` offer transition |
| `STAFF_PANEL` | panel-scoped only; for every exact unfilled `REVIEW` slot or sole `ADJUDICATE/default` slot, atomically perform its `PLANNED -> READY` transition and offer it from the Evidence/Wait memberships when one complete legal staffing set exists, otherwise offer none and create a new bound panel `CAPACITY` Wait |
| `REPAIR_SCHEMA` | no new Activity; after the Activity is `PLANNED`, create one next Attempt generation with the code-owned schema-repair envelope only by the atomic `PLANNED -> READY` offer transition |
| `PROBE_INTEGRITY` | no Activity; commit one exact-object `STORAGE_OBJECT_INTEGRITY` or `SECRET_VERSION_INTEGRITY` Health Probe Request and Outbox before I/O, then remain recoverable until its typed Health Observation is reduced |
| `DIAGNOSE` | new `DIAGNOSE` Activity |
| `REPLAN` | new `REPLAN` Activity |
| `ALTERNATIVE_CANDIDATE` | new `BUILD` when no Candidate exists, `REMEDIATE` before linkage, or `PR_REMEDIATE` after linkage, determined solely from durable publication/Candidate state |
| `ADJUDICATE` | create the sole `ADJUDICATE` Activity for slot `default` only when the frozen disputed set first arises; once it exists, abstention, `INCONCLUSIVE`, execution failure, and capacity replacement use `RETRY_EXECUTION` or `REPLACE_CAPACITY` to create a higher Attempt generation on that same Activity, never another adjudication Activity or slot |
| `REBASE` | new `REBASE` Activity bound to the exact observed base/head |
| `IMPORT_EXTERNAL_HEAD` | controller `IMPORT` bound to the exact Forge Observation |
| `RECONSTRUCT_FOREIGN_HEAD` | new `REMEDIATE` Activity bound to the exact foreign-ref observation, typed import-validation failure, pinned base, and controller-generated non-executable diff/evidence from the safely fetched head; it reconstructs the intended change as a new Candidate rooted at the pinned base and cannot retry `IMPORT` for the same evidence |
| `ENTER_HUMAN_BOUNDARY` | no Activity; permitted only after the evidence proves one allowlisted exceptional reason and exhaustion/inapplicability of every autonomous tactic; creates the exact Human Boundary through `T(RECOVERY_EVIDENCE, recovery_evidence_id)` |
| `WAIT_BACKOFF` | new `BACKOFF` Wait Condition |
| `WAIT_CAPACITY` | new `CAPACITY` Wait Condition |
| `WAIT_RATE_LIMIT` | new `RATE_LIMIT` Wait Condition |
| `WAIT_BUDGET` | new `BUDGET` Wait Condition |
| `WAIT_EXTERNAL` | new `EXTERNAL_DEPENDENCY`, `FORGE_UNAVAILABLE`, `STORAGE_RECOVERY`, or `SECRET_RECOVERY` Wait Condition selected by the typed failure category |
| `WAIT_EVIDENCE` | new `EVIDENCE` Wait Condition |

No other tactic string is valid. A tactic cannot choose a different Activity
from this table, and repository configuration cannot add a tactic. The new
Activity's deterministic key includes this Recovery Evidence, tactic, cycles,
strategy index, and rescue epoch.

### Recovery ladder

For one failure signature, the default pinned strategy order is:

1. **Reconcile before repeating.** Check whether a result, Candidate, secret
   rotation, or external side effect already committed after an ambiguous
   response.
2. **Redeliver safely.** If an `OFFERED` Attempt is still current, redeliver its
   notification. Do not increment generation before its claim deadline.
3. **Retry transient execution.** After the current Attempt is terminal and its
   Activity is `PLANNED`, create a higher Attempt generation in a clean
   workspace only through the atomic `PLANNED -> READY` offer transition, up
   to pinned `maxAttemptsPerActivityBeforeDiagnosis` (default `3`) for the
   current Activity strategy.
4. **Replace execution capacity.** From `PLANNED`, select another worker and,
   when policy permits, the next compatible provider/credential in stable
   configured order, then offer the next generation through `PLANNED -> READY`.
   Substitution must preserve reviewer independence and all gate requirements.
5. **Repair the result shape.** For invalid structured output, retry once with
   schema-repair instructions, then change worker/provider rather than
   accepting malformed data.
6. **Diagnose non-progress.** After pinned
   `maxRepairCyclesBeforeDiagnosis` (default `4`) Candidate repair cycles with
   the same unresolved evidence, or the attempt threshold above, transition to
   `DIAGNOSING` and plan an independent `DIAGNOSE` Activity.
7. **Replan when diagnosis requires it.** A diagnosis may first select a new
   code-owned tactic while the accepted plan remains viable. When diagnosis
   proves the plan invalid or pinned `maxDiagnosesBeforeReplan` (default `2`)
   is reached, use the accumulated diagnoses and original Snapshot to enter
   `REPLANNING`. A replan cannot remove requirements or gates.
8. **Generate alternatives.** Permit two alternative Candidate strategies per
   rescue epoch, each with fresh verification and review.
9. **Resolve disagreement without inventing gates.** Freeze the existing
   verification/review evidence, run only the sole configured adjudication slot `default`,
   and apply their structured dispositions. Sustained/new blockers remediate;
   all-overruled blockers open a fresh full configured panel with no carried
   approvals; inconclusive/non-filling slots retry or wait for evidence/capacity.
10. **Back off without abandoning ownership.** Enter `WAITING/BACKOFF` for 30
    minutes, doubling per rescue epoch to a maximum of 24 hours, then start a
    new rescue epoch. A new model/provider becoming available, a relevant base
    or specification change, or external recovery wakes it earlier.

Repository policy MAY change numeric limits and allowed provider order within
validated bounds. It cannot remove reconciliation, clean execution, gate
preservation, or indefinite resumable rescue. An authenticated latest
`EXHAUSTED` Budget Report at a safe offer boundary appends Recovery Evidence
with category `BUDGET` and selected tactic `WAIT_BUDGET`, then enters
`RECOVERING`; only that Evidence's later Transition may create
`WAITING/BUDGET`. The reset timer starts reconciliation, but only a current
authenticated unexpired `AVAILABLE` Report for the applicable window/policy
can permit the next offer; budget exhaustion is not a human boundary.

Before a classified failure changes strategy or state, the reducer MUST append
the next immutable per-Run Recovery Evidence record and atomically update
`Run.current_recovery_evidence_id`. That record owns the failure fingerprint,
strategy index, attempt/repair/diagnosis counters, rescue epoch, selected
fallback, ordered consulted Health Observation membership/digest, and next
eligibility time; none may be reconstructed from Redis or free-form logs. The
record, membership, selected values, and Transition commit atomically.
Duplicate source identity is idempotent, while a different
payload for that identity is an integrity conflict.

Fallback selection MUST use stable pinned policy order plus the highest
applicable unexpired ordered Health Observation for each scope. An observation
expires only when a persisted timer trigger causes reducer evaluation; Redis
lease loss and wall-clock arrival order are not health facts. The selected
Health Observations are named by the Recovery Evidence's canonical frozen
membership (at most the highest applicable unexpired observation per relevant
scope), so
replay over the same ordered records chooses the same tactic. Randomness and
receipt arrival order cannot select a strategy.

### Recovery transitions

| Condition | Durable transition |
| --- | --- |
| controller/Redis restart | Reconcile current state and outbox; remain in or return to the state implied by durable objects. |
| worker `INFRASTRUCTURE` Result | Accept the Result, set its Attempt `FAILED`, return its Activity to `PLANNED`, append AttemptResult-sourced `WORKER_LOST` Recovery Evidence, and enter `RECOVERING`; only the subsequent Evidence reduction may select replacement/wait work. |
| authenticated pool loss | Commit the Worker Loss Report, `WORKER_SESSION/LOST` Health Observation, and `WORKER_LOST` Attempt Terminal Fact; fence the Attempt, return its Activity to `PLANNED`, append TerminalFact-sourced `WORKER_LOST` Recovery Evidence, and enter `RECOVERING`; only the subsequent Evidence reduction may select replacement/wait work. |
| provider rate limit | Try configured compatible account/provider; otherwise `WAITING/RATE_LIMIT` until persisted reset/wake. |
| all workers unavailable | `WAITING/CAPACITY`; retain quorum and verification requirements. |
| authenticated current budget report is `EXHAUSTED` at an offer boundary | Keep the Activity `PLANNED`; enter `RECOVERING` with Report-sourced `BUDGET` Recovery Evidence, and let only its later Transition create `WAITING/BUDGET`. A reset timer alone never authorizes an offer. |
| budget report is missing, expired, old-window, or policy-mismatched at an offer boundary | Keep the Activity `PLANNED`; create no invented Report, Recovery Evidence, or Wait. Startup/report acceptance rescans the durable Activity, and only a current authenticated Report selects the later branch. |
| malformed output | `RECOVERING`; schema repair then replacement. |
| failing deterministic check | `REMEDIATING`, not `NEEDS_HUMAN`. |
| repeated ineffective fix | `DIAGNOSING -> REPLANNING -> BUILDING/REMEDIATING`. |
| reviewer disagreement | Freeze the configured evidence set and enter configured `ADJUDICATING`; no ad hoc reviewer or verification requirement is invented. |
| merge conflict | `PR_REMEDIATING` with exactly one `REBASE`, causal latest trusted `BASE_HEAD`, and separate exact Change Request-head fence. |
| Run-bound forge read/search/poll is transiently unavailable | Persist the exact Forge Request Failure Fact and retry boundary; enter `WAITING/FORGE_UNAVAILABLE` only through its Recovery Evidence, then reconcile the same Request identity before retry. |
| candidate/secret storage inconsistency | `RECOVERING` and storage/backup reconciliation; only proven unrecoverable integrity loss may cross the exceptional boundary. |

No generic `FAILED` terminal Run exists in v1.

## Exceptional human boundary

`NEEDS_HUMAN` is a resumable exceptional waiting state. It is not a retry
budget, provider health, disagreement, or “agent gave up” state.

### Allowlisted reason codes

The controller may enter `NEEDS_HUMAN` only with one of:

| Code | Required proof |
| --- | --- |
| `MISSING_AUTHORITY` | The next required operation is outside all configured Orcest authority and cannot be replaced by a permitted autonomous operation. |
| `REQUIRED_SECRET_OR_PERMISSION` | A required secret or permission is absent or revoked, configured acquisition/rotation has failed, and Orcest is not authorized to create it. |
| `IRREVERSIBLE_DECISION` | Progress requires an irreversible or destructive action for which policy grants no autonomous authority. |
| `SPECIFICATION_CONFLICT` | Pinned requirements demand incompatible outcomes and independent diagnosis found no declared precedence rule. |
| `SECURITY_POLICY_BOUNDARY` | A code-owned controller/server security policy explicitly requires human authorization for the classified operation. |
| `INTEGRITY_FAILURE` | A live referenced Candidate artifact, Secret Version, or Workflow Blob is proven missing/corrupt after all configured local and backup reconciliation. |
| `UNSATISFIABLE_REQUIREMENTS` | Deterministic evidence proves the requirements cannot all be satisfied in the permitted environment; ordinary test failure is insufficient. |
| `PUBLICATION_OWNERSHIP_CONFLICT` | Exact Project/ref/Change Request observations contain incompatible Run/legacy ownership claims, and reconciliation across the deterministic branch, Run marker, Publication, and legacy ownership record cannot establish one safe owner or an autonomous no-overwrite adoption path. |

An agent may report evidence suggesting one of these conditions. The reducer
MUST first exhaust applicable autonomous diagnosis, permission refresh,
alternative implementation, or reconciliation paths and then apply the
allowlist predicate itself. Agent `needs_human` booleans or prose are ignored as
lifecycle commands.

### Decision packet

Entering `NEEDS_HUMAN` MUST insert the immutable [Human
Boundary](domain-model.md#human-boundary), set `Run.human_boundary_id`, and only
then project its bounded decision packet. The packet includes:

- Run and Work Item identities;
- exact Snapshot generation, Candidate, policy, Forge Observation, Publication,
  and Publication effect generation when applicable;
- for ownership conflict, the exact registered Project, deterministic ref,
  observed Change Request ID, and syntactically valid Orcest v1 Run marker;
- allowlisted reason code;
- the smallest missing decision, information, secret, or authority;
- autonomous strategies already attempted and their evidence digests;
- ordered typed choices, their material consequences, and the closed set of
  permitted resolution kinds;
- the exact authenticated management action or verified external fact that can
  resolve the boundary; and
- the state to revalidate after resolution.

The packet binds all referenced identities and includes canonical evidence and
attempted-strategy digests. The controller, not the worker or repository
configuration, issues its reason and permitted resolution set. A repository
cannot add a reason, create a human gate, weaken proof, or name generic text as
a resolution.

### Resumption

`NEEDS_HUMAN` does not end the Run. Resumption requires insertion of one
immutable, authenticated [Human Resolution](domain-model.md#human-resolution)
for the exact current boundary. The reason-to-resolution mapping is closed:

| Boundary reason | Allowed resolution kind | Required source and behavior |
| --- | --- | --- |
| `MISSING_AUTHORITY` | `AUTHORITY_GRANTED` | Authenticated management command names the granted authority and scope; enter `RECOVERING` and prove it before retry. |
| `REQUIRED_SECRET_OR_PERMISSION` | `SECRET_OR_PERMISSION_PROVIDED` | Authenticated management command, or a verified-current Secret Version plus its immutable creation Receipt and opaque integrity evidence, names only non-secret provenance. Automatic version fanout is authorized by the registered Secret-Store verifier/reconciler service principal; enter `RECOVERING` and validate it. |
| `IRREVERSIBLE_DECISION` | `IRREVERSIBLE_ACTION_AUTHORIZED` | Authenticated management command selects one packet choice and exact action scope; enter `RECOVERING`, never broaden the authority. |
| `SPECIFICATION_CONFLICT` | `SPECIFICATION_AMENDED` | Ordered authorized Observation first captures pending Snapshot without installation; the separate `SPEC_SUPERSEDE` transaction installs it, inserts the source-bound Resolution, and enters `REPLANNING`. Management Command is not allowed. |
| `SECURITY_POLICY_BOUNDARY` | `SECURITY_ACTION_AUTHORIZED` | Authenticated management command authorized by server policy selects an allowed exact action; enter `RECOVERING`. |
| `INTEGRITY_FAILURE` | `INTEGRITY_RESTORED` | Verified storage-restoration fact identifies the restored immutable object; rehash/revalidate it, then enter `RECOVERING`. |
| `UNSATISFIABLE_REQUIREMENTS` | `SPECIFICATION_AMENDED` or `ENVIRONMENT_CAPABILITY_PROVIDED` | Authorized changing `WORK_ITEM_SNAPSHOT` atomically resolves/supersedes into `REPLANNING`; authenticated management provision of an allowed environment capability enters `RECOVERING` after validation. |
| `PUBLICATION_OWNERSHIP_CONFLICT` | `PUBLICATION_OWNERSHIP_RESOLVED` | Authenticated management command supplies the closed resolution with `selected_engine = ORCEST_V1` and exact Project/ref/Change Request/Run marker/Publication/effect-generation binding; enter `RECOVERING` and reconcile without overwrite before any effect. No legacy selection or marker transfer exists in v1. |

Acceptance is idempotent by boundary and resolution idempotency key. It
requires that key equal the canonical `source_id` for its closed source kind,
including the composite Secret Version key; the boundary must still be
current, every copied binding must match, the source must be permitted for
that reason, and the principal must possess server-side authority for the exact
action. If a Snapshot, Candidate, policy, Forge Observation, or Publication
fence changed, the controller supersedes the boundary and
re-evaluates; it does not apply the stale Resolution. Acceptance clears
`Run.human_boundary_id` but retains both immutable records.

Issue or PR comments, repository files, prompts, and generic commands such as
“continue” are never Human Resolutions. Repository configuration cannot opt
into such a shortcut or define a human approval gate.

## Authenticated Run commands

The v1 controller exposes one command endpoint:

```http
POST /api/v1/runs/{run_id}/commands
```

The authenticated JSON body is one of:

```json
{
  "protocol": "orcest.management/1",
  "command_id": "lowercase-uuid",
  "run_id": "lowercase-uuid",
  "expected_last_transition_sequence": 42,
  "kind": "CANCEL",
  "payload": {}
}
```

```json
{
  "protocol": "orcest.management/1",
  "command_id": "lowercase-uuid",
  "run_id": "lowercase-uuid",
  "expected_last_transition_sequence": 42,
  "kind": "RESOLVE_HUMAN_BOUNDARY",
  "payload": {
    "human_boundary_id": "lowercase-uuid",
    "resolution_kind": "one-kind-allowed-by-the-boundary",
    "resolution": {}
  }
}
```

The path/body Run identities must match. Authentication is transport/server
managed; the controller records the authenticated principal and authorization
decision, and applies server-owned RBAC for the exact Project, Run, command,
and resolution kind. The companion management specification must define
principal issuance and role administration before implementation; no role or
credential may come from `.orcest`, a worker, or a forge comment.

Acceptance inserts the canonical Management Command and its resulting
Transition in one writer transaction; `RESOLVE_HUMAN_BOUNDARY` also inserts the
canonical Human Resolution. The command ID is the global idempotency key: an
identical authenticated replay returns the original response, while different
content under that ID returns `409 IDEMPOTENCY_CONFLICT`. A stale transition or
boundary fence returns `409 STALE_RUN`, failed authorization returns `403`, and
schema/protocol errors return `422`; none mutates Run state. Raw secret values
are rejected. `RESOLVE_HUMAN_BOUNDARY` with `SPECIFICATION_AMENDED` is also
rejected because only the authorized Forge Observation transaction above may
produce it. `CANCEL` uses the cancellation transition below.

An accepted command returns HTTP `200` with the exact closed body:

```json
{
  "protocol": "orcest.management-result/1",
  "command_id": "lowercase-uuid",
  "run_id": "lowercase-uuid",
  "kind": "CANCEL",
  "outcome": "ACCEPTED",
  "result_transition_sequence": 43,
  "human_resolution_id": null,
  "replayed": false
}
```

For `RESOLVE_HUMAN_BOUNDARY`, `human_resolution_id` is the exact accepted UUID;
for `CANCEL` it is `null`. The Command stores HTTP status, canonical JSON, and
digest in its acceptance transaction. Identical replay changes only
`replayed` to `true`; that one transport projection is excluded from the
digest, while every other body field and the HTTP status are covered.

## Cancellation and terminal outcomes

Workflow-control v1 publishes, monitors, and remediates the Change Request but
does not issue a merge command. Native forge auto-merge, repository merge
policy, or an authorized external reviewer performs the final merge. Orcest
records the authenticated result and remains responsible for the active Run
until that merge or a non-merge closure is observed.

### Cancellation

Publication state `CHANGE_REQUEST_OBSERVED` is the ordinary cancellation
authority cutoff. Before it, an accepted `CANCEL` Management Command—or Work
Item closure when no Change Request has been observed—may atomically transition
the Run to `CANCELLED` only when no create request can be in flight: no
`CHANGE_REQUEST_CREATE/REQUEST_READY` or `AMBIGUOUS` checkpoint exists and
either no Publication/create workflow exists or a current
`CHANGE_REQUEST_SEARCH/OBSERVED_ABSENT` checkpoint is backed by the exact
matching `CHANGE_REQUEST_ABSENT` Forge Observation. `REF_ABSENT` cannot satisfy
this predicate. The controller MUST:

- fence and supersede every nonterminal Attempt;
- revoke all Attempt authority except the bounded Result-endpoint
  authentication that remains cryptographically valid until its fixed
  capability-auth expiry;
- mark unfinished Activities `CANCELLED` or `SUPERSEDED`;
- cancel pending outbox deliveries where safe;
- retain admitted Candidates and audit evidence under retention policy; and
- reconcile forge Projections and any partially created deterministic branch.

Late results are rejected by their fence. Cancellation does not rely on Redis
deletion. Workflow-control v1 defines no branch/ref deletion operation. A
pre-CR deterministic ref is therefore retained as a reserved audit artifact
after reconciliation proves no open Change Request exists; the terminal Run
marker/Publication identity continue to reserve it, and no later Run may
publish to or silently adopt it.

Whenever a Publication/create workflow exists but no current search checkpoint
backed by `CHANGE_REQUEST_ABSENT` proves absence, cancellation stores its exact
Management Command or Work Item Forge Observation source and enters the
nonterminal cleanup path. This applies
before any create checkpoint as well as after
`CHANGE_REQUEST_CREATE/REQUEST_READY` or `AMBIGUOUS` makes a side effect
possible. Reconciliation searches by the stable Publication/create identity:
proven absence may terminate `CANCELLED` only through a successful
`CLOSE_PUBLICATION` Controller Operation Fact that names that exact observation;
discovery switches through the next
immutable owned-close Activity; ambiguity remains active. It is forbidden to
commit `CANCELLED` merely because `CHANGE_REQUEST_OBSERVED` has not yet been
written.

At or after `CHANGE_REQUEST_OBSERVED`, Work Item closure alone is recorded but
does not cancel the Run. An accepted `CANCEL` command atomically stores
`Run.cancellation_source_kind` and `Run.cancellation_source_id`, fences new
semantic work, and creates one
current idempotent `CLOSE_PUBLICATION` Activity/outbox for the applicable
immutable cleanup phase. The Run remains nonterminal.
The controller rereads the Change Request and calls the forge adapter's
`close_change_request_if_owned` only after stable ID, Orcest marker,
deterministic source ref, current head, and effect generation prove ownership.
A mismatch appends a Forge Observation and reconciles again; it never closes
an unverified object or overwrites the head. Restart rebuilds cleanup from the
Run intent and outbox.

Only an authenticated exact Change Request observation terminates this race:
unmerged close while cancellation is pending produces `CANCELLED`; merge wins
as `MERGED`. The controller never commits `CANCELLED` before the close is
observed, so it never rewrites one terminal outcome into another. The
provisional Change Request remains run-owned and excluded from the legacy
engine until either observation arrives.

### Merge and non-merge closure

Only an authenticated Forge Observation for the Publication's exact Change
Request can create `MERGED`, `CLOSED`, or the cancellation-specific terminal
outcome:

- merged -> `MERGED`, record merge commit and time;
- closed without merge while cancellation is pending -> `CANCELLED`, record
  final head and the cancellation source; and
- closed without merge otherwise -> `CLOSED`, record final head and closure
  reason.

All unfinished work is superseded, capabilities are revoked, and future
observations are audit-only. A Work Item closure produced by merging the linked
Change Request is represented as `MERGED`, never `CANCELLED`.

## Publication and PR remediation fencing

Initial publication MUST point to the exact approved Candidate commit and use
a deterministic branch and Run marker. Before retrying after any error, the
controller searches for the branch and Change Request and validates Project,
marker, commit, and current `Publication.effect_generation`. Every publication
intent, outbox entry, adapter request, and resulting observation carries
`(publication_id, effect_generation)`. Retrying an unchanged intent reuses that
generation and stable idempotency identity. Planning a different desired
commit or other authoritative intent field increments the generation atomically
before its outbox is visible. Ordered initial-ref and Change Request
suboperations share their immutable `INITIAL` Publication Effect generation.
An older-generation response is audit evidence and cannot update the
Publication.

Every `PR_REMEDIATE` Activity binds to:

```text
publication_id
forge_observation_id
change_request_head_observation_id
observed_change_request_head
normalized CI/review finding set
```

For `PR_REMEDIATE`, the causal and head observation normally match. A
post-link `REBASE` instead binds its causal `BASE_HEAD` observation in
`forge_observation_id`/`input_ref` and separately binds the latest exact
Change Request head observation and head. Change Request `IMPORT` likewise
binds the exact head observation. After the full gate, an active Publication's
`UPDATE` Effect retains that head as its compare-and-swap expectation; when the
Change Request is still provisional, the higher-generation `INITIAL` Effect
retains the same expectation instead. A base-only observation can never supply
or weaken the head fence.

Update-mode `PUBLISH` performs compare-and-swap against
`observed_change_request_head`. If a human or another automation advances the
head, the update fails without overwrite; the controller records a fresh Forge
Observation, supersedes stale remediation work, and deterministically replans.

Run ownership MUST be visible through the stable Publication marker. The
legacy PR engine MUST exclude any Change Request carrying a syntactically valid
Orcest v1 marker, even when its Run or Publication is absent from the current
durable store. Independently, for every live v1 Publication it MUST exclude the
exact `change_request_external_id` when present and the deterministic source
ref unconditionally; marker state and repair phase are not predicate inputs.
That durable association/ref exclusion lasts until the Run is terminal or an
explicit engine-migration transaction transfers it; v1 has no migration or
legacy-handoff operation. The Run engine MUST not claim unmarked legacy/human PRs and MUST
not treat an unknown marker as ownership authority. An unknown marker fails
closed while the controller reconciles the durable store, backup, deterministic
ref, and forge observations. If reconciliation can prove one owner, it repairs
or restores projections and continues autonomously. Only when exact ordered
observations prove incompatible ownership claims and reconciliation cannot
establish a single safe owner or a no-overwrite adoption path may the
controller create `PUBLICATION_OWNERSHIP_CONFLICT`. Its authenticated
`PUBLICATION_OWNERSHIP_RESOLVED` resolution can name only `ORCEST_V1` and MUST
copy the exact Project, deterministic ref, observed Change Request ID, valid
Orcest v1 marker, Publication, and immutable effect generation from its
Boundary. The reducer then enters `RECOVERING` and re-proves those bindings
before allowing any mutation. Legacy selection, engine handoff, and marker
removal or transfer are outside v1 and cannot be encoded as a resolution.

## Projection rules

Projection failures never change Run state. The controller repairs them from
durable state.

- Admission requests `orcest:ready -> orcest:working` only after the Run
  commits.
- Waiting states SHOULD use bounded status projections without posting one
  comment per retry.
- `NEEDS_HUMAN` MAY project its decision packet and exceptional label only
  after the state and packet commit.
- Terminal state MAY remove working/ready projections and post a single
  idempotently marked summary.
- A missing or stale projection is not evidence that a Run is absent, waiting,
  complete, or cancelled.

## Lifecycle conformance cases

An implementation of this page MUST demonstrate at least:

1. two concurrent admissions produce one active Run and capture-sequence-1
   Snapshot; separate replay-safe `SPEC_SUPERSEDE` and `INTERNAL` reductions
   produce one generation-1 installation and one initial plan;
2. a crash during admission leaves no Run, an `ADMITTED` Run with the Snapshot
   pending, an `ADMITTED` Run with generation 1 installed, or a `PLANNING` Run
   with its first outbox; restart converges each phase without duplicate work;
3. receipts returned in every order produce the same Consensus Decision;
4. a new Candidate makes prior approvals ineligible before any publication;
5. Redis loss after claim preserves the durable Attempt and deadline;
6. a first Result at or after its execution deadline atomically expires and is
   rejected, while an identical replay of a Result accepted strictly before the
   deadline returns the original response;
7. repeated tests, reviewer disagreement, provider exhaustion, and malformed
   output traverse autonomous recovery rather than `NEEDS_HUMAN`;
8. an authenticated cumulative `EXHAUSTED` Budget Report blocks new offers
   and creates the exact budget recovery path; an authenticated later
   `AVAILABLE` Report resumes its frozen Run membership idempotently after any
   fanout-prefix crash, while the reset Timer alone can never authorize an
   offer; capacity waits likewise resume without lowering their configured
   gate;
9. a specification edit during an active Attempt waits for or creates a safe
   fence, increments the generation, and replans;
10. a base advance under the default policy creates a new rebased Candidate and
    reruns all pre-publication gates;
11. a crash after external branch or PR creation reconciles the existing
   Publication at the same effect generation, and a delayed prior-generation
   response cannot update it;
12. a concurrent PR-head update makes remediation compare-and-swap fail without
    overwrite;
13. a working-labeled Work Item with no Run is autonomously reconciled or
    readmitted;
14. every human reason rejects insufficient evidence, stale bindings, generic
    comments, and unauthorized resolution kinds, then resumes the same Run
    after an idempotent valid typed resolution;
15. merged, unmerged closure, and pre-publication cancellation produce three
    distinct terminal outcomes;
16. deleting Redis during a timed or observation-driven wait reconstructs the
    current Wait Condition and only its exact persisted trigger can resume it;
17. ordered specification captures A -> B -> C install only C at the safe
    boundary, while A -> B -> A clears the pending supersession without
    deleting B;
18. a Forge Observation sequence A -> B -> A is retained even without delivery
    IDs, while a duplicate immediately consecutive payload is coalesced;
19. an external advance of a run-owned PR head produces a controller-imported
    Candidate and the full verification and consensus gate runs before any
    later branch update; and
20. exhausted automated ownership reconciliation is the only path to
    `PUBLICATION_OWNERSHIP_CONFLICT`, whose typed resolution is reconciled
    without overwrite before publication continues; and
21. two distinct episodes with the same capacity payload and Wait predicate
    create distinct ordered Health Observations and Wait Conditions, while
    replay of either exact source identity is idempotent and cannot advance the
    Run twice;
22. a server configuration change alone leaves an active Run's policy intact;
    an explicit ordered policy-only update at a safe boundary invalidates the
    old Plan/gates, retains the exact Candidate only as non-gating context, and
    requires a new policy-bound `REPLAN`; only a successful Plan plus repeated
    exact identity check can reselect that Candidate, skip `BUILD`, derive new
    review subjects, and rerun the full ordinary gate;
23. an `OFFERED` Attempt expiring with no compatible healthy worker creates a
    unique capacity wait only when mode/key gates allow replacement and without
    advancing failure/diagnosis counters; a blocked gate leaves the Activity
    planned for its one later dispatch continuation;
24. controller restart and ambiguous adapter response resume the same active
    PUBLISH from its highest checkpoint and request identity, while a CAS
    mismatch records the head and replans without overwrite;
25. an exact linked Change Request merge/close observation terminates the Run
    and supersedes work from every nonterminal state; and
26. command replay, stale Run fence, stale Human Boundary, unauthorized
    principal, invalid resolution kind, and conflicting command ID exercise the
    exact management API outcomes without duplicate transitions;
27. under `REBASE_BEFORE_PUBLICATION`, an `INITIAL` pre-mutation base mismatch
    supersedes the effect and rebases without creating a ref or Change Request;
28. a pre-link foreign deterministic-ref head or create/update CAS mismatch
    never overwrites: ownership reconciliation either safely imports the exact
    head through the full gate, selects one evidence-bound base-rooted
    reconstruction when pinned-base/Candidate admission fails without
    incompatible ownership, or proves the exceptional ownership conflict;
29. under `REBASE_BEFORE_PUBLICATION`, a newly created Change Request remains
    `CHANGE_REQUEST_OBSERVED`; a second base mismatch preserves it and
    rebases/full-gates through a higher `INITIAL` generation, while only a
    matching second read makes it `ACTIVE`;
30. replaying a reducer Transition produces the same Activity idempotency key,
    while identical semantic work in a later recovery/repair cycle gets a
    distinct evidence/cycle-bound key; same-commit non-progress cannot recreate
    the completed producer Activity;
31. accepted worker failures transition only by `ATTEMPT_RESULT`, deadline/loss
    only by an Attempt Terminal Fact, recovery strategy only by Recovery
    Evidence, and Wait/Human creation only by their persisted causal input;
32. pending specification B and trusted base C followed by Policy Update P
    captures `B + C + P`, then a separate `SPEC_SUPERSEDE` installs it; fields
    copied from installed A and install-in-capture are both rejected;
33. every illegal Publication checkpoint mode/order/status/null combination and
    every unknown Forge Observation kind or recovery tactic is rejected; and
34. an authorized changing forge specification observation captures the
    pending Snapshot without installation; its separate canonical
    `T(SPEC_SUPERSEDE,snapshot_id)` atomically installs and writes the Human
    Resolution, while an unauthorized, non-changing, or management-command
    substitute does neither;
35. deleting Redis across a due Wait, Health expiry, Attempt deadline, and
    recovery eligibility reconstructs the same scoped Timer Facts from durable
    deadlines before any reduction;
36. every new source-unique deadline/loss Fact and accepted
    `ALREADY_TERMINAL` Result Request after terminalization appends exactly one
    same-state Attempt-Terminal Transition and no recovery counter, while exact
    replay appends no duplicate Transition;
37. Candidate, Secret Version, and Workflow Blob restoration each require a
    verified Storage Restoration Fact with matching kind-specific integrity/source authority,
    while an unverified copied file cannot wake or resolve the Run;
38. cancellation before `CHANGE_REQUEST_OBSERVED` terminates immediately only
    when no create request is possible and an exact matching
    `CHANGE_REQUEST_ABSENT` observation proves search absence; cancellation
    after `CHANGE_REQUEST_CREATE/REQUEST_READY` reconciles the stable request,
    closes any discovered owned Change Request, and lets a racing merge yield
    `MERGED` without rewriting a terminal outcome;
39. a safely fetchable foreign pre-link head that fails pinned-base or
    Candidate admission selects one `RECONSTRUCT_FOREIGN_HEAD` tactic and never
    repeats import for the same evidence or claims ownership conflict without
    positive incompatible evidence; and
40. a post-link base-triggered `REBASE` binds both its causal `BASE_HEAD` and a
    separate exact Change Request head observation, and the provisional
    `INITIAL` or active `UPDATE` compare-and-swap uses the latter;
41. replaying a Capacity Report returns the identical Health Observation IDs,
    sequences, wakes, and response, while a conflicting body or stale unseen
    sequence creates no partial health state; and
42. one global Health-expiry Timer Fact fans out deterministically to every
    Run with a current explicitly bound Wait Condition, and one object-scoped
    Storage Restoration Fact fans out to every affected Run, without
    duplicating the source fact or losing a per-Run Transition; and
43. after SQLite commit but Redis loss, every `REVIEW` and `ADJUDICATE` claim
    reconstructs the identical tagged slot/context/finding projection from its
    Activity Review Assignment, and mutable execution-profile registry changes
    cannot alter the producing Attempt's frozen provider/model family,
    classification revision, or independence result;
44. every accepted Forge Observation produces exactly one Transition across
    all specification generations, including a same-state Transition for a
    no-op, and an `APPROVED` base check uses only
    `T(INTERNAL, approved_transition_sequence)` rather than reusing the latest
    `BASE_HEAD` trigger;
45. identical specification/workflow/policy inputs with only a base advance
    retain the same `supersession_key` under `REBASE_BEFORE_PUBLICATION` and
    `PIN`, but produce a different pending key and safe-boundary generation
    under `SUPERSEDE_AT_BOUNDARY`; and
46. differing commits at both `BASE_READ_PRE` and `BASE_READ_POST` select
    `BASE_MISMATCH` plus rebase for `REBASE_BEFORE_PUBLICATION`,
    `OBSERVED_SATISFIED` plus continuation for `PIN`, and `BASE_MISMATCH` plus
    a base-only Snapshot/replan for `SUPERSEDE_AT_BOUNDARY`, with post-read
    non-PIN paths retaining the exact owned provisional object; and
47. a `PUBLICATION_OWNERSHIP_CONFLICT` resolution rejects every engine value
    except `ORCEST_V1`, rejects any Project/ref/Change Request/marker/
    Publication/effect mismatch, and resumes only through exact-object
    ownership reconciliation without marker transfer or legacy handoff; and
48. both pre-link reconciliation outcomes reject an absent or changed
    `observed_ref_commit`; the accepted commit equals the exact causal
    `REF_HEAD` observation and frozen `RECONCILE` Activity input; and
49. replay every closed trigger kind after a specification-generation advance,
    including `INTERNAL` and `SPEC_SUPERSEDE`; each lookup returns the original
    `(run_id, trigger_kind, trigger_id)` Transition and its audited evaluated
    generation without applying the input or planning work twice, while an
    admission Forge Observation also cannot reappear under
    `FORGE_OBSERVATION`; and
50. source-kind Human Resolution fixtures require `idempotency_key = source_id`,
    including the canonical composite Secret Version key, and reject a
    UUID-only substitute or conflicting reuse; and
51. after Redis loss, every Review/Adjudication claim reconstructs the exact
    ordered subject list from contiguous unique Activity Review Subject rows;
    missing, reordered, duplicated, policy-extended, or digest-mismatched
    membership prevents dispatch and cannot validate a Receipt; and
52. claim-deadline expiry atomically freezes the highest-applicable unexpired
    Health Observations and one capacity disposition, expires the Attempt,
    returns its Activity to `PLANNED`, and appends zero-counter Recovery
    Evidence without consulting later health state or advancing recovery
    counters; only a later `T(RECOVERY_EVIDENCE, ...)` may offer the
    replacement or create the capacity Wait, and a claimed unfilled panel peer
    follows the explicit no-Evidence staffing-pointer exception; and
53. every model-backed Attempt consumes one unique launch nonce through one
    signed Attestation from its pinned runner principal/image/key, and rejects
    reused workspace/context/invocation IDs, any parent/resume binding, a stale
    deadline, or a Result/Receipt with a missing or different Attestation; and
54. a schema-valid Verification `ERROR` plus matching
    `FAILED_RETRYABLE/VERIFICATION_ERROR` creates the sole error Result path,
    while missing or malformed verification output creates no Result or
    Transition and remains correctable until deadline/loss recovery; and
55. `REF_ABSENT` cannot close a `CHANGE_REQUEST_SEARCH` or cancellation path;
    only an exact `CHANGE_REQUEST_ABSENT` observation bound to the source
    repository/ref/Run marker/search revision/nonexistence token may record
    `OBSERVED_ABSENT`, and terminal pre-link cleanup additionally requires the
    successful `CLOSE_PUBLICATION` Fact naming that observation; and
56. a `SECRET_VERSION` Human Resolution rejects a missing/mismatched creation
    Receipt, stale current-version reference, wrong Secret Store attestation,
    or worker/operator/synthetic resolution actor; the registered verifier/
    reconciler service principal and exact non-secret provenance are required,
    while replay of a pending Secret Provision Operation resumes its staged
    checkpoint with the frozen target version rather than asking for bytes or
    creating another version; a terminal provision rejection returns its stored
    response and never wakes the Run as if a Secret Version existed; and
57. after a terminal Secret Provision rejection, a corrected operation can
    reuse the released target version only after the per-Secret/storage lock
    proves no installed version, live reservation, target object, or unexpired
    quarantine; the corrected operation then installs exactly once without
    adopting or clobbering rejected staged bytes; and
58. one disputed set creates exactly one `ADJUDICATE` Activity for slot
    `default`; abstention, `INCONCLUSIVE`, failure, loss, and compatible-capacity
    replacement return that Activity to `PLANNED` before recovery and create
    only higher Attempt generations through `PLANNED -> READY` on that same
    Activity, and restart cannot invent a second adjudication Activity or slot; and
59. a lost first-claim response replays the exact immutable Attempt Claim for
    the same session/key/digest, different bytes under that key conflict, and a
    different key or session cannot acquire an already claimed Attempt; and
60. an Attempt capability expires exactly `86_400_000` milliseconds after its
    execution deadline: before the deadline its closed endpoint scopes work,
    from the deadline until that expiry only accepted-Result replay or the
    bounded late Result Request/rejection works, and at expiry neither the
    latter nor any workflow row is created while Timer Fact recovery remains;
    and
61. a Claim exposes the normalized signed-claims
    `launch_capability_digest`, an accepted Attestation copies it exactly, a
    changed bearer serialization does not change it, and the accepted replay
    response returns `AVAILABLE` with provider material only before deadline;
    consumed/expired launch-cap verification can only retrieve that exact
    accepted identity as `EXPIRED` with `provider = null` afterward and cannot
    accept or mutate anything; and
62. credential rotation before deadline records exactly one immutable Request:
    `APPLIED` atomically installs its Receipt/version/reference/fanout,
    `CAS_LOST` records only the `409` ledger, identical replay is stable,
    different bytes/key conflict, and no Attempt Result can name either path;
    exact replay after terminalization remains possible only strictly before
    execution deadline, while the rotation endpoint denies every request at or
    after that deadline; and
63. every controller surface rejects plaintext or certificate-invalid
    transport, including loopback and private-network callers, before claim,
    worker, pool, management, restoration, secret, registration, or lifecycle
    authority is evaluated; and
64. accepted Management Commands replay the exact stored
    `orcest.management-result/1` status/body with only the non-digested
    `replayed` projection changed; storage restoration replays its derived
    deterministic `202` while pending and its exact stored closed terminal
    response afterward; and Project registration success/rejection replays
    only the closed status/body mapping with the same projection rule; and
65. a Capacity Report is rejected before ledger creation unless every expiry
    is strictly after controller `accepted_at_ms` and no later than
    `accepted_at_ms + configured_max_ttl_ms`; caller observation time cannot
    extend the accepted TTL; and
66. `PLAN`, `REPLAN`, and `DIAGNOSE` success is accepted only through the exact
    structured-output tagged Result variants, Candidate- and Receipt-producing
    kinds reject structured output, and the Result schema rejects every
    credential-rotation Receipt field; and
67. after an ambiguous endpoint response, claim, launch-attestation,
    Candidate-upload creation/content, Result, credential-rotation, management,
    restoration, provisioning, and registration clients reuse that endpoint's
    exact durable identity and identical body within its authority window;
    liveness alone has no durable request ledger and retries with the next
    strictly higher sequence, which returns the freshly derived current-control
    projection without extending a deadline; and
68. one lowercase UUID is globally unique in the Result Request registry:
    accepted semantic work, an expired Candidate Upload, a first current late
    Result, and an already-terminal late Result select exactly one of
    `ACCEPTED`, `UPLOAD_EXPIRED`, `STALE_ATTEMPT`, `EXPIRED_CURRENT`, or `ALREADY_TERMINAL`;
    reuse with different body or binding conflicts, while a semantic replay
    under a new key points to the existing Attempt Result and invokes no
    reducer transition; malformed/schema-invalid, unauthorized, and semantic
    conflict requests such as `RESULT_ALREADY_ACCEPTED` are rejected before
    registry admission and create no Result Request row; and
69. both content `PUT` and Result finalization at or after Candidate Upload
    expiry return the identical closed HTTP `410`
    `orcest.candidate-upload-expired/1` body derived from the durable upload,
    accept no new bytes, and create no Result, Candidate, Receipt, Recovery
    Evidence, or Transition; and
70. every issued Attempt and launch capability names one durable
    `CapabilitySigningKey` ID and `ED25519`; retirement prevents new issuance
    while preserving verification through each prior capability's expiry,
    revocation rejects authentication immediately, and backup/restore plus
    retention preserve every referenced public verifier and its exact state;
    and
71. forge/provider availability and exact Candidate-artifact/Workflow-Blob/
    Secret-Version integrity failure can enter recovery only through the
    closed Health Probe Fact scope/outcome/object/failure matrix and its
    atomically derived Health Observation; an adapter callback, generic probe,
    secret-bearing evidence, or out-of-range TTL cannot substitute; and
72. Project Registration replay digests only the closed public status/body
    excluding `replayed`, while its separate required `resolution_digest`
    covers authorization and conditional internal Secret References; neither
    digest can cause an internal reference to appear in the public response;
    and
73. every accepted failure Result uses one closed `failure_class`, its permitted
    Activity/outcome, normalized code/retry/evidence tagged fields, and one
    required `result_digest`; unsorted, secret-bearing, unknown, or
    class-incompatible fields create no accepted Result or Result Request.
74. every satisfied Wait and non-specification Human Resolution copies the
    immutable resume state, freezes exact source/Wait or Boundary/Resolution
    IDs, appends one Recovery Evidence record, and exits `RECOVERING` only by
    reducing that record; restart never takes a direct work-state shortcut;
75. an open/unknown dependency observation survives restart in the Run pending
    pointer, does not fence a claimed Attempt, and at the next safe boundary
    deterministically becomes an `EXTERNAL_DEPENDENCY` Evidence/Wait, while an
    earlier satisfying observation clears it without waiting;
76. a linked Change Request head advance during every gate, remediation,
    recovery, wait, approval, publication, or Human-boundary state supersedes
    all old-head work and imports the observed head through the full gate;
77. admission, forge/policy capture, approved-base continuation, and authorized
    specification amendment never install a Snapshot; every installation is
    uniquely `T(SPEC_SUPERSEDE,snapshot_id)`, including generation 1;
78. an accepted launch response exposes the exact Claim-frozen non-secret
    provider Secret ID/version and resolves material only from it; rotation
    cannot substitute a version on replay;
79. a provider retry timestamp is treated as absolute Unix milliseconds and
    clamps exactly to `min(max(value,accepted_at),accepted_at+pinned_max)`;
    neither it nor its Wait extends execution deadline;
80. Project registration retains logical read/publication Secret IDs and
    provenance versions, while every Claim/Effect freezes current exact
    versions and a Secret Wait names the logical ID plus minimum version;
81. reviewer/adjudicator no-capacity is decided by the exact Result or
    `INTERNAL` planning Transition from frozen Health membership; a later
    Health input only wakes the resulting condition and cannot become the
    original planning trigger;
82. under `SUPERSEDE_AT_BOUNDARY`, base-only movement before Publication
    `ACTIVE` captures then separately installs a Snapshot, while the same
    base-only movement after `ACTIVE` is observation-only and cannot pend or
    install a generation;
83. Capability Key Operations exercise register/select/retire/revoke CAS,
    including required replacement for current-key retirement and atomic
    replacement-or-issuance-disablement for emergency current-key revocation;
    old unrevoked capabilities verify only through their cryptographic expiry;
84. every controller-owned external health probe has a committed Health Probe
    Request/outbox before I/O and exactly one reciprocal Fact/Observation on
    completion; a crash cannot synthesize an unrequested probe result;
85. controller mode survives process/Redis restart, CASes by durable revision,
    and enforces the closed admission/claim/result matrix without creating a
    Run Transition or retroactively discarding accepted input;
86. a lost Claim response remints only the original frozen secrets and signed
    claims/key/revision/deadlines; registry or credential rotation cannot alter
    its non-secret response contract or substitute sensitive authority; and
87. Review/Adjudication abstention or inconclusive evidence enters typed
    `RECOVERING` and creates replacement/wait work only through the following
    Recovery Evidence Transition; it never directly creates a new slot or
    Attempt in the Result Transition;
88. before cryptographic launch-capability expiry, a consumed token may
    authenticate only exact retained-Attestation lookup; at or after expiry,
    the identical original token under the same registered runner/session and
    an `ACTIVE` or retained `RETIRED` verifier is only signature-equality proof
    for the exact stored Attestation and `EXPIRED`/provider-null response, while
    any claim/JTI/digest mismatch or `REVOKED` key is denied and no authority,
    material, or mutation is granted;
89. every `AVAILABLE` launch response exposes flat non-secret
    `provider.secret_id` and `provider.version` fields exactly equal to the
    Attempt Claim's frozen `provider_secret_ref`; expiry returns
    `provider = null`, and replay never substitutes the Project's newer Secret
    version; and
90. duplicate-marker reconciliation freezes a complete canonically ordered
    `REDUNDANT_PUBLICATIONS_PROVEN` member set, retains the bytewise-lowest
    stable Change Request ID as the association post-link or as an unlinked
    candidate pre-link, and closes at most one exact unreviewed duplicate
    through `CLOSE_REDUNDANT_PUBLICATION`. Restart or an ambiguous response
    reuses the Activity operation identity; a changed head, marker, ref,
    effect generation, review/proof revision, or member set prevents close and
    first persists/reduces exact Forge Observation evidence before scheduling
    a fresh complete marker search; only the later changed search result may
    start fresh reconciliation, and only the bound authenticated close
    observation completes the Activity, leaves the retained Publication/Run
    active, and triggers a fresh set proof. A non-retained same-marker
    observation received during another post-link state is consumed once as a
    same-state audit input and can authorize cleanup only after the next
    `PR_MONITORING` complete search; and
91. a revision-0/null Controller Mode can become initialized only through the
    bootstrap service principal's `INITIALIZE` operation to revision-1
    `MAINTENANCE`; every distinct initialized mode pair CASes once, same-mode
is rejected except a real `DISPATCH_PAUSED` intake-policy change, backup
restore uses the closed three-branch barrier: in-place when already
    `MAINTENANCE`, in-place for exact `DISPATCH_PAUSED/PAUSE_ADMISSION`, and a
    CAS through that paused pair for every other initialized mode, while
    preserving the backed-up prior-mode projection; neither
    `DISPATCH_PAUSED` nor `DRAINING` creates an offered Attempt/outbox;
92. after Publication `ACTIVE`, a base-only `SUPERSEDE_AT_BOUNDARY` observation
    is consumed as audit-only, while a specification, workflow, policy, or
    combined change still captures and separately installs a Snapshot and
    resolves any resulting specification boundary;
93. a missing/corrupt Candidate artifact, Workflow Blob, or Secret Version can
    enter recovery only after Request/outbox, Fact plus `UNAVAILABLE`
    Observation, `T(HEALTH_OBSERVATION,...)`, mapped `STORAGE`/`CREDENTIAL`
    Evidence, and the separate `T(RECOVERY_EVIDENCE,...)` that creates its
    exact-object Wait; restart cannot collapse those two Transitions;
94. reviewer or sole-adjudicator no-capacity persists the complete ordered
    consulted Health and unfilled panel-slot memberships and creates no
    capacity Decision; after wake, `STAFF_PANEL` either offers every still-
    unfilled slot as one legal independent set or offers none and creates a new
    fully bound Wait;
95. after linkage, a complete duplicate search with no closable member stores
    `NO_ACTIONABLE_DUPLICATE` and suppresses reconciliation for the unchanged
    search-revision/set-digest pair; a later merge or close of a non-retained
    same-marker object is consumed without terminalizing the retained Run,
    supersedes a stale in-flight search, and only a later typed complete-search
    Observation whose pair changes plans reconciliation;
96. when eligible Work-Item and Publication `BASE_HEAD` observations interleave,
    every capture/rebase/approval chooses the one consumed by the highest Run
    Transition sequence, including the generation-1 base explicitly anchored
    at `ADMIT`, never target-local sequence or adapter time;
97. a Secret Wait is not created when the under-lock current verified version
    already meets its logical Secret/minimum-version predicate, and a Wait with
    both timer and event arms may be proposed by either arm while the reducer
    still rejects stale bindings;
98. each forge connectivity probe commits a Request/outbox with the exact
    current `ForgeInstance.credential_secret_id` version before I/O, and its
    Fact, Observation, subject bindings, scope hash, and digests copy that
    version; rotation cannot reuse old availability;
99. an entering Transition schedules at most its one keyed continuation, whose
    fixed precedence is cancellation, pending `SPEC_SUPERSEDE`, pending
    dependency, current-panel staffing, then state-specific work; a
    `PUBLISHING` Run whose current
    effect is fenced may have no `PUBLISH` only while the pending-Snapshot
    continuation is its sole eligible action;
100. cancellation retains the deterministic ref as reserved audit state after
    exact Change Request absence/cleanup reconciliation; no v1 branch-delete
    call exists, and another Run cannot adopt or publish that ref; and
101. claim-deadline compatibility requires unexpired compatible `AVAILABLE`
    profile and session evidence and rejects only a latest unexpired negative
    observation for the exact provider account/Secret version; absent provider
    evidence is neutral and cannot be synthesized as availability; and
102. an already-linked owned Change Request with a missing or byte-identically
    duplicated marker can be repaired only by an Effect-fenced
    `REPAIR_RUN_MARKER` Activity whose exact-CAS input binds stable ID/head/ref,
    body revision, marker-set digest, and positive ownership proof; a mismatch
    mutates nothing, ambiguity reuses the operation, conflicting ownership is
    not repairable, and only a controller-bound observation of one desired
    marker completes the Activity without changing Candidate, gates, or Effect;
103. an absent Capability Registry starts at revision 0 with no key; `REGISTER`
    creates revision 1 plus an unselected `ACTIVE` key, a separate `SELECT`
    enables issuance, and emergency selected-key `REVOKE` may succeed with a
    null result key while offers remain fail-closed;
104. a claim deadline atomically resolves the model-backed logical provider
    account's current Secret version, freezes the exact health/mode/key
    evidence, expires the Attempt, returns its Activity to `PLANNED`, and
    appends zero-counter Recovery Evidence; only the later Evidence reduction
    offers for `OFFER_ALLOWED + COMPATIBLE_AVAILABLE` or creates a capacity
    Wait, while a claimed unfilled panel peer takes the explicit no-Evidence
    staffing-pointer exception;
105. Wait and Health-expiry Timer Facts reduce directly, Attempt-deadline Timer
    Facts reduce only through Attempt Terminal Facts, and Recovery-eligibility
    Timer Facts make the exact Evidence eligible without becoming its trigger;
106. a Health Probe Fact freezes bytewise ordered affected Runs and resumes its
    cursor after crash; a member whose exact scope was superseded still consumes
    the Health Observation through one same-state Transition;
107. every Work Item/base/ref/Change Request/CI/initial search/complete marker
    poll and every Project-scoped Work Item discovery scan commits one typed
    Forge Observation Request and reciprocal Outbox from
    its durable Schedule before I/O; restart reuses a pending request and a CAS
    mismatch produces no Observation;
108. worker-reported integrity suspicion can only schedule the exact typed
    probe; positive proof resumes ordinary recovery, while only a negative
    Fact/Observation creates `STORAGE` or `CREDENTIAL` Evidence and its later
    exact-object Wait;
109. the repeated identical verification or review/adjudicated-blocker
    fingerprint reaches the pinned repair threshold by appending typed Recovery
    Evidence selecting `DIAGNOSE`, never by silently planning another repair;
110. Stage 0 can provision distinct installation-owned `FORGE_API`,
    `SOURCE_READ`, and `PUBLICATION` Secrets before Project creation, while
    registration rejects owner/purpose substitution and records all resolved
    provenance; and
111. in `MAINTENANCE`, an existing Result ledger entry is read-only replay, but
    an unseen Result key receives the closed `503 CONTROLLER_MAINTENANCE`
    response and creates no durable workflow row;
112. Schedule activation, pause, closure, and due-Request creation each CAS and
    increment the exact Schedule revision; terminalizing a Run closes every
    ordinary Run-owned Schedule and supersedes its pending Request and pending
    Outbox in the terminal writer transaction. Positive merged selection may
    atomically create only Reservation-bound cleanup schedules, which remain
    restartable until Reservation completion closes them;
    and
113. when more than one eligible trusted `BASE_HEAD` was accepted before
    admission, the serialized `ADMIT` transaction selects the greatest
    Work-Item-targeted per-target observation sequence and anchors that exact
    row; adapter arrival time and a later unconsumed base cannot replace it;
    and
114. Project registration creates one durable `WORK_ITEM_DISCOVERY` Schedule;
    an empty or nonempty scan records its adapter search revision and semantic
    set digest, retries by the same Request identity, and orders discovered
    Work Item snapshots bytewise, while every multi-result per-object poll uses
    the closed kind precedence before allocating new observation sequences;
    discovery completion owns Run-null Work Item/base schedules and `ADMIT`
    atomically closes/replaces them with Run-bound schedules; and
115. restoring a backup while the controller is already in `MAINTENANCE`
    preserves its nested prior mode and permission envelope in place; exact
    `DISPATCH_PAUSED/PAUSE_ADMISSION` also restores in place, while any other
    initialized mode first CASes to that paused pair and never exposes an
    ordinary endpoint between barrier entry and restored projection;
116. a Result submitted in `MAINTENANCE` replays only by an existing exact
    `result_request_id`; a new key, even for semantically identical accepted
    bytes, returns the closed five-field `503 CONTROLLER_MAINTENANCE` body
    (`protocol`, `code`, `retryable`, `message`, and
    `retry_after_seconds = 60`) and creates no Result Request, Result, Fact, or
    Transition;
117. successful Project `REGISTER` commits its ACTIVE revision-0
    `WORK_ITEM_DISCOVERY` Schedule and reciprocal internal pointers in the same
    transaction, while `REVALIDATE` retains that identity; ACTIVE means durable
    cadence state, not permission to dispatch it during Stage-0
    `MAINTENANCE`;
118. a Forge Observation Request found stale before I/O is superseded with its
    pending Outbox, while a stale response after I/O marks the Request
    `SUPERSEDED` and Outbox `DELIVERED`; `MAINTENANCE` retains ordinary due and
    pending requests without creation, delivery, or completion;
119. fresh-store Stage 0 succeeds only in the sequence Mode `INITIALIZE`, real
    controller signing-Secret provision/adoption Receipt, key `REGISTER`, key
    `SELECT`, then remaining Secrets/Projects; every synthetic, combined, or
    reordered shortcut fails closed;
120. multiple peer completions replace one Run panel-staffing pointer; only the
    latest entering Transition's `INTERNAL` continuation survives, waits while
    any peer is `CLAIMED` or mode/key blocks, and then staffs every unfilled
    slot or creates one complete panel Wait—never a partial offer set;
121. every entry to `RECOVERING` appends its exact typed Recovery Evidence in
    the entering transaction, and only `T(RECOVERY_EVIDENCE,...)` applies a
    retry, Activity, probe, or Wait; accepted-object reconciliation cannot use
    a direct `INTERNAL` shortcut;
122. when a happy Result or Attempt Terminal Fact reaches a safe boundary with
    cancellation, `supersede_requested`/pending Snapshot, or pending dependency,
    it accepts/fences the input but emits no ordinary next work; the one
    continuation applies fixed precedence and Snapshot install, coalescing,
    cancellation, or terminalization clears the durable supersede pair;
123. legacy discovery excludes the exact stable Change Request ID and
    deterministic ref of every live v1 Publication unconditionally until
    terminal release or an explicit
    migration protocol that v1 does not define;
124. before initial linkage or merge/close authority, a complete marker search
    freezes separately ordered live and terminal memberships, each member's
    closed ownership proof, and one digest over both, while cardinality counts
    live only: a positive owned merged terminal member takes precedence at any
    cardinality; otherwise zero-live/no-terminal may search/create but must loop
    through a fresh complete search, one positive live plus a fresh exact-object
    read permits linkage despite positive closed audit members, multiple
    positive live suspends publication and closes only proven reliance-free
    duplicates one at a time, and zero live plus positive closed members never
    creates and selects the lowest closed ID;
125. changing the current `PUBLISH` approval/effect while any initial marker
    proof, subordinate reconciliation, or duplicate close is stale fences that
    work; no merge/close observation collected before a fresh one-member proof
    can terminalize the Run;
126. `WAIT_EVIDENCE` insertion rechecks its exact target, minimum Forge
    Observation sequence, allowed kinds, predicate digest, and lifecycle fences
    under the writer lock; already-satisfied evidence appends a source-unique
    retry/replacement Recovery Evidence and creates no stale Wait, while an
    inserted Wait always has both a bounded timer and exact event arm;
127. a complete marker search rejects a member whose exhaustive ownership
    union/tag, proof-kind nullability, create checkpoint/request, registered creator,
    Effect generation, ref/marker/desired-commit/head evidence, defect set, or
    proof/member/full-set digest is inconsistent; `INCOMPLETE` autonomously
    rereads/backs off without association, `INCOMPATIBLE` traverses exact
    ownership Recovery Evidence and a positive `OWNERSHIP_CONFLICT` Fact before
    the boundary, and neither is silently treated as owned;
128. at every live cardinality and from every nonterminal state, one-or-more
    positive owned merged terminal members select the bytewise-lowest stable ID
    before cancellation/live/closed/conflict routing, atomically fix the merged
    Publication/Run outcome, and create exactly one durable Reservation whose
    ordered membership is every and only live search member;
129. a crash immediately after that terminal transaction resumes the
    Reservation without reopening the Run: positive reliance-free members use
    exact-CAS close, positive relied-on members use exact-CAS marker detach,
    and incomplete/incompatible/CAS-unsafe members complete only as bounded
    `RETAINED_AUDIT`; mutation ambiguity never repeats blindly;
130. every cleanup response or proof change appends one same-state `MERGED`
    Transition; every completed member schedules one source-unique `INTERNAL`
    continuation that advances at most one ordinal, and replay creates neither
    a second Action generation nor a second external mutation; and
131. the legacy engine excludes every unresolved Reservation member and its
    deterministic ref after Run terminalization, then releases only the
    resolved ID/ref while continuing to exclude any syntactically valid Orcest
    marker independently;
132. a scheduled forge read/search/poll transport failure replays one exact
    pre-I/O attempt-ordinal Forge Request Failure Fact; Run-bound failure
    enters FORGE_TRANSIENT recovery exactly once, pre-admission/discovery and
    terminal-cleanup failure retries directly, and a crash before or after
    Fact acknowledgement produces neither duplicate recovery nor any synthetic
    Forge/Health Observation; and
133. a definitive failed Controller Operation Fact accepts only the category
    allowed for its Activity kind and copies it unchanged into Recovery
    Evidence; temporary forge transport/ambiguous-write outcomes cannot be
    smuggled into that Fact or remapped from text.

## Evidence and migration

### Current evidence retained

- `src/orcest/orchestrator/issue_ops.py` already applies an ordered intake
  cascade and treats unresolved dependencies as deferral. v1 preserves ordered,
  fail-closed intake but replaces Redis liveness with durable admission.
- `src/orcest/orchestrator/pr_ops.py` and
  `docs/wiki/current-orchestrator-state-model.md` reject stale PR work using
  exact head SHA and predicates. v1 generalizes the fence to Snapshot,
  Candidate, Attempt generation, and Forge Observation.
- `src/orcest/orchestrator/loop.py` already distinguishes ordinary failures,
  usage exhaustion, backoff, and stale results, and recent logic avoids
  automatically labeling retry exhaustion `needs-human`. v1 formalizes this as
  typed recovery and resumable waits.
- `src/orcest/fleet/pool_manager.py` reports a reaped VM as transient failure so
  the orchestrator retries. v1 retains the recovery intent and adds durable
  claim fencing.
- `src/orcest/orchestrator/issue_deps.py` treats transient dependency lookup
  failure as unknown/blocking and rechecks next cycle. This maps directly to
  `WAITING/EXTERNAL_DEPENDENCY`.

### Behavior replaced

- Current issue discovery clears orphan Redis attempts when no pending marker
  exists, so a Redis restart can make active issue work look unclaimed. v1
  persists Activity/Attempt claims and deadlines independently of Redis.
- Current issue success removes `orcest:ready` immediately after one worker
  reports completion. v1 keeps one Run active through Candidate gates,
  publication, PR monitoring, remediation, and terminal forge observation.
- Current issue prompts instruct the worker to push and open a PR. v1 changes
  worker success to Candidate upload and lets the reducer decide whether the
  exact commit may be published.
- Current `TaskResult.needs_human` is accepted from a worker and can directly
  label the issue/PR. v1 accepts only controller-issued typed exceptional
  reasons after autonomous recovery.
- Current max-attempt and cooldown values live in Redis and can suppress work
  until expiry or mutation. v1 stores strategy counters and wait conditions
  durably and guarantees a rescue path rather than a failed terminal state.
- Current PR result validation checks the observed head before side effects,
  but a worker still pushes directly. v1 requires controller-side
  compare-and-swap at the actual publication update.

### Deliberately deferred rollout and implementation validation

These experiments are not prerequisites for reviewing the normative lifecycle.
Production enablement deliberately defers until repository evidence
demonstrates these implementation and rollout gates.

1. Current code has no pure reducer or append-only Transition table. The first
   implementation must prove replay determinism and idempotent trigger handling
   before connecting real workers.
2. Current Work Item discovery scans only `orcest:ready`; there is no
   `orcest:working` reconciliation or unique active-Run database constraint.
3. Current issue/config observation does not compute the normalized Snapshot,
   specification hash, or safe-boundary supersession request defined here.
4. Current retry counters do not preserve typed recovery strategy, failure
   fingerprints, rescue epochs, or wait wake conditions across Redis loss.
5. Current provider selection is round-robin over available credentials. v1
   needs stable, persisted recovery selection and review-independence checks.
6. Current GitHub helper code observes and mutates existing PRs but lacks
   deterministic publication discovery, Run markers, and a push-time
   compare-and-swap primitive.
7. Current `blocked` and `needs-human` labels are treated as terminal skips.
   v1 must migrate them carefully: temporary conditions become `WAITING`, and
   `NEEDS_HUMAN` becomes an exceptional resumable state with a decision packet.
