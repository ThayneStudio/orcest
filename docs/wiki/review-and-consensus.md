# Review and Consensus

> **Status:** Accepted normative v1 specification (2026-08-27)
>
> **Canonical owner:** exact-Candidate verification evidence, adversarial
> reviewer panels, independence, typed verdicts and findings, adjudication,
> deterministic consensus, and evidence invalidation.

This page defines how Orcest decides whether one exact Candidate is ready for
publication. The [domain model](domain-model.md) owns durable identities, the
[workflow lifecycle](workflow-lifecycle.md) owns state transitions and recovery,
the [worker protocol](worker-protocol.md) owns authenticated receipt submission,
and [repository configuration](repository-configuration.md) owns the pinned
parameters accepted by the reducer.

The requirements on this page are normative for workflow-control v1.

## Safety contract

Orcest MUST NOT publish a Candidate merely because its producing worker says
the work is complete. The controller admits evidence in this order:

1. the single v1 deterministic Verification Profile named `default` runs
   against the exact current Candidate;
2. every required adversarial reviewer slot independently compares that same
   Candidate with the pinned Work Item Snapshot and accepted plan;
3. disputes may be classified from already admitted typed evidence but are
   resolved only through an independent adjudication Activity; and
4. the code-owned reducer computes a Consensus Decision from a closed,
   canonical input set.

Model output may vary, but it cannot change the quorum, waive verification,
discard a blocker, declare itself independent, or select a lifecycle state.
Missing capacity, provider failure, malformed output, timeout, and repeated
disagreement never weaken the effective policy.

Every receipt and decision MUST bind both `candidate_id` and the controller-
verified Candidate commit `{object_format, oid}`. A binding mismatch is stale
input, not evidence about a similar commit or bundle.

## Effective pinned policy

For each specification generation the controller materializes one normalized
effective policy from the trusted repository policy intersected with server
minimums. The Snapshot persists its canonical `policy_hash`. At minimum it
contains:

- the single Verification Profile `default` and its ordered commands;
- the ordered reviewer slots, role prompts, primary server-registered Execution
  Profile IDs, allowed alternate Execution Profile IDs, their permitted exact
  `(worker_profile, provider, model, provider_account_ref)` resolutions, and
  each resolution's canonical `provider_family` and `model_family` IDs under
  the immutable `classification_revision`, plus the independence constraints;
- the server-minimum `fresh_invocation: true` and `fresh_context: true` rules
  for every reviewer and adjudicator assignment, which repository policy may
  strengthen but cannot disable;
- the pinned registered launch-isolation runner principal, image digest,
  `runner_signature_algorithm`, `runner_signing_key_id`, and
  `runner_registration_revision` used to verify the mandatory one-shot Launch
  Attestation for every model-backed Attempt;
- `approvals_required`, whose v1 server minimum and default are `2` and which
  equals the number of materialized gating reviewer slots;
- the blocker rule, which in v1 is always “no unresolved blocker”;
- the one v1 adjudicator slot `default`, required count `1`, its primary
  server-registered Execution Profile ID, and ordered alternate Execution
  Profile IDs;
- schema and evidence-size limits; and
- the same full verification and consensus gate for every post-publication
  replacement Candidate.

Repository configuration MAY raise an approval count or add verification
commands, slots, and independence constraints. It MUST NOT lower server minimums,
convert an abstention into an approval, permit unresolved blockers, or select a
weaker fallback when capacity is unavailable. A Candidate's `.orcest` changes
have no effect on its active Run.

Before server-policy intersection, repository `slots` length MUST equal its
`approvals_required`. Server-added mandatory slots increase the effective
threshold by the same number. v1 has no extra or advisory review slot.

## Candidate gate identity and rounds

A verification set is identified by:

```text
(candidate_id, candidate commit, policy_hash,
 default verification Activity ID)
```

A review panel is identified by:

```text
(candidate_id, candidate commit, policy_hash, panel_round)
```

Repository `spec.verification` plus server-required command normalization
materializes exactly one v1 Verification Profile whose ID is `default`;
repository policy may add commands but cannot add or rename Verification
Profiles. The reducer freezes that Verification Profile's ordered commands and one `VERIFY` Activity ID before dispatch and
allocates positive, monotonically increasing panel rounds independently per
Candidate: each Candidate begins at `1`, adjudication may increment only that
Candidate's sequence, and a replacement Candidate begins again at `1`. It also
freezes the reviewer slots, assignments, and context digests. Arrival order
cannot add, remove, or renumber a command or slot.

A new panel round on the same Candidate is allowed only when the reducer's
code-owned recovery policy requests wholly fresh independent evidence. It
creates new `REVIEW` Activities for every gating slot. Receipts from an earlier
round remain audit history and do not fill the new round.

## Common receipt admission

Verification, review, and adjudication payloads are typed receipt bodies carried
by the Attempt result endpoint defined in the worker protocol. A schema-valid
`FAIL`, `ERROR`, `BLOCK`, permitted `ABSTAIN`, or `INCONCLUSIVE` remains typed evidence.
A non-filling Review or Adjudication Receipt is enclosed by an `ABSTAINED`
Attempt Result; a filling receipt is enclosed by `SUCCEEDED`. Neither Attempt
outcome means the Candidate passed. A decisive Verification `PASS` or `FAIL`
is enclosed by `SUCCEEDED`; Verification `ERROR` is the sole receipt allowed on
a `FAILED_RETRYABLE` Attempt Result.

The controller accepts a receipt only in the same SQLite transaction that:

1. verifies the Attempt capability, claimed worker, current generation,
   Activity kind, immutable Candidate and Snapshot inputs, and that controller
   time is strictly before the fixed execution deadline;
2. verifies the receipt media version and exact Activity-specific schema;
3. derives trusted Candidate, Verification Profile, Execution Profile,
   `worker_profile`, `provider`, `model`, `provider_account_ref`, slot, role,
   command, and context metadata from durable assignments rather than worker
   claims;
4. normalizes the bounded receipt and computes its digest;
5. inserts the Attempt-bound receipt and accepted Attempt Result, deriving
   whether it fills its assigned semantic slot; and
6. invokes the reducer and persists its Transition and any next Activities and
   outbox rows.

The receipt types are:

```text
orcest.verification-receipt/1
orcest.review-receipt/1
orcest.adjudication-receipt/1
```

Readers reject missing required fields and ignore unknown fields within major
version 1. Changing a field's meaning, verdict semantics, evidence binding,
or reducer treatment requires a new receipt major version. Unknown enum values
are rejected; they are not treated as an abstention or pass.

An identical Attempt-result replay returns the already accepted receipt,
including after the execution deadline under the worker protocol's replay-only
exception. No first Receipt or Result acceptance is permitted after that
deadline. Different content for an accepted Attempt returns an integrity
conflict. A stale-generation or stale-Candidate receipt is rejected before
insertion and cannot affect consensus.

## Verification Receipt

### Execution contract

Each `VERIFY` Activity receives the exact Candidate and the pinned Verification
Profile `default`. Commands execute in declared policy order as literal
argument arrays, without a shell, in a clean credential-free sandbox. Network
is disabled unless the server-constrained `default` Verification Profile
explicitly permits it. Candidate code receives no forge-write, provider,
controller, Secret Store, or unrelated-Run credential.

The worker records every command result after an ordinary nonzero exit. It may
stop only when a sandbox, tool, resource, or integrity error prevents later
commands from running. The controller MUST NOT execute Candidate-owned hooks,
filters, credential helpers, or a Candidate-modified workflow while preparing
the checkout.

### Verification Receipt wire schema

```json
{
  "protocol": "orcest.verification-receipt/1",
  "candidate": {
    "candidate_id": "uuid",
    "commit": {"object_format": "sha1", "oid": "40-hex-oid"}
  },
  "profile_id": "default",
  "profile_hash": "sha256:64-lowercase-hex",
  "checks": [
    {
      "command_id": "unit",
      "invocation_digest": "sha256:64-lowercase-hex",
      "termination": "EXITED",
      "exit_code": 0,
      "stdout_digest": "sha256:64-lowercase-hex",
      "stderr_digest": "sha256:64-lowercase-hex",
      "evidence": []
    }
  ],
  "outcome": "PASS",
  "error": null
}
```

`termination` is `EXITED`, `SIGNALED`, `TIMED_OUT`, or `NOT_RUN`. `exit_code`
is present only for `EXITED` and is an integer `0..255`; a code-owned signal
name is present only for `SIGNALED`. `evidence` is a bounded ordered list of
normalized non-secret diagnostic records or controller-admitted digest
references; it cannot contain credentials or create a new gate. For `ERROR`,
`error` contains one code from `SANDBOX`, `TOOL_MISSING`, `TIMEOUT`, `SIGNALED`,
`RESOURCE_LIMIT`, `EVIDENCE_CAPTURE`, or `INTEGRITY`, the nullable failed
`command_id`, and bounded evidence references.

The controller recomputes the receipt outcome:

- `PASS` requires every required command exactly once, matching invocation
  digests, `EXITED`, and exit code `0`.
- `FAIL` requires complete trustworthy execution with one or more nonzero
  exits or process signals that the runner proves were not caused by its
  sandbox, timeout, or resource enforcement.
- `ERROR` means the required answer is unknown because execution, sandbox,
  signal, timeout, evidence capture, or integrity failed. It requires a
  code-owned `error.code`.

A worker-declared outcome that differs from the recomputed outcome makes the
receipt invalid. Missing/extra commands, an invocation digest mismatch, or an
unexplained `NOT_RUN` also makes it invalid rather than `PASS`.

Every Verification Receipt is uniquely bound to its producing Attempt and
persists the exact `activity_id`, `attempt_id`, `attempt_generation`,
`profile_id: "default"`, and `profile_hash` derived from the frozen assignment.
V1 has no separate verification-set generation counter: a new Candidate or policy creates a
new `VERIFY` Activity, while an execution retry increments only that Activity's
Attempt generation. A `PASS` or `FAIL` is the decisive receipt that completes
its `VERIFY` Activity; Attempt fencing and terminal Activity state prevent a
second decisive receipt. An `ERROR` remains Attempt-bound audit evidence and
does not fill the `default` Verification Profile, so a higher Attempt
generation of the same Activity may later produce a decisive receipt.

`PASS` is eligible only for the exact pinned `default` Verification Profile.
`FAIL` produces actionable remediation evidence. `ERROR` is carried by a
`FAILED_RETRYABLE` Attempt Result
with failure class `VERIFICATION_ERROR`; an invalid receipt is rejected. Both
enter autonomous recovery, and neither is a Candidate failure or a pass.

## Independent Review Receipt

### Reviewer input

Before a panel round starts, the controller creates a role-specific immutable
review context containing:

- the exact Work Item Snapshot and accepted implementation plan;
- the Candidate ID, verified commit, base commit, and read-only checkout;
- the pinned workflow and role prompt;
- the required `default` Verification Receipt for that Candidate; and
- the closed ordered subject list `snapshot:overall` followed by one
  `plan:requirement:<requirement_key>` for every accepted Plan requirement in
  the Plan array's semantic order; and
- a code-owned receipt schema and bounded evidence rules.

Every `REVIEW` invocation, including a replacement for a non-filling receipt,
MUST NOT receive another reviewer's receipt, verdict, findings, prose summary,
private reasoning, or execution transcript from its or an earlier panel round.
It reviews the
specification against the actual Candidate, not merely the producing worker's
summary. Untrusted issue, plan, repository, and tool text is delimited as data
and cannot alter the output schema or lifecycle rules.

### Minimum independence

The normalized effective policy, not a worker claim, is the authority for
independence. Its repository constraints are intersected with server minimums;
the v1 server minimum always requires a fresh model invocation and fresh
conversation/context instance for every `REVIEW` and `ADJUDICATE` Attempt,
including replacements. “Fresh context” forbids transcript, mutable memory,
workspace, or generated-output reuse; two contexts may have the same digest
when their immutable input bytes are intentionally identical.

Two approvals are independent only if all of these hold:

- they occupy different frozen reviewer slots and come from different
  `REVIEW` Activities and Attempts;
- each is a fresh agent invocation with a fresh conversation and clean
  read-only workspace;
- no invocation, transcript, mutable workspace, or generated receipt is reused
  across slots;
- neither invocation is the Candidate-producing invocation and neither is
  given another slot's review output before its own receipt commits; and
- the durable Execution Profile resolution and resulting Worker Profile,
  `provider`/`model`/`provider_account_ref` attributes satisfy every provider-family,
  model-family, account, or other independence dimension in the effective
  pinned policy.

The controller derives execution/classification attributes from its assignment
and authenticated claim, and accepts freshness only from the exact signed
Launch Attestation produced by the pinned registered runner shim. Its globally
unique workspace/context/invocation IDs and null parent/resume bindings cannot
be reused by another Attempt. Worker-provided identity strings cannot prove
independence. Merely running the same text twice in one conversation is not
independent. Separate fresh invocations of the same model family may count only
when the effective policy does not require family separation.

### Panel staffing and substitution

The reducer schedules every frozen gating slot. For a slot, it selects the first
currently eligible primary or alternate Execution Profile in stable configured
order, resolves it to an exact
`(worker_profile, provider, model, provider_account_ref)` assignment,
freezes its server-derived `provider_family`, `model_family`, and
`classification_revision`, and verifies that resolution against persisted
capacity/health observations and already assigned panel identities. The
complete execution assignment and classification are persisted on the Attempt
before dispatch; a later registry edit cannot change an independence decision.

Capacity is decided by the same planning Transition that would create offers:
the last `VERIFY` Result when opening a panel, the current Review Result when
advancing/replacing a slot, or the `AGGREGATING` `INTERNAL` continuation when
opening the sole adjudication slot. That Transition freezes the
highest-applicable unexpired Health membership. If no complete legal staffing
selection exists, it creates all durable slot Activities/Assignments/subjects
but no new offers. It may insert the bound `WAITING/CAPACITY` condition only
when no unfilled slot has a `CLAIMED` Attempt; that transaction supersedes any
peer `OFFERED` Attempts so every Wait member is undispatched. If a peer remains
claimed, the Run stays in its panel state, preserves that Attempt, and defers
complete staffing through the Domain's one coalesced
`latest_staffing_recheck_transition_sequence` projection. Every later peer
Result/Terminal boundary replaces the Candidate/panel/kind/sequence pointer and
discharges the older obligation. When no peer remains claimed, only the latest
pointer's unique `INTERNAL` continuation may offer all unfilled slots or create
the panel Wait; it never offers a subset. Closed mode/key gates leave that same
pointer pending for the reconciler, and a Candidate/panel change clears it as
stale. The eventual
Wait freezes the complete ordered highest-applicable Health Observation
membership/digest and exact ordered membership of every planned unfilled
Activity/panel/slot. It creates no Consensus Decision. A
later Health Observation cannot retroactively become the trigger for the
original no-capacity decision.

Consequently, a panel `CAPACITY` Wait has no live `CLAIMED` or `OFFERED`
Attempt for any member slot. A Result arriving from a superseded offer is
`STALE_ATTEMPT` and cannot fill a slot or create a Transition. Any filling
Receipt accepted before Wait creation is visible to the serialized planning
transaction and removes that slot from the frozen membership; it cannot appear
later as an unmodeled `WAITING` Result.

The pending staffing pointer or an exact panel Wait is the sole `NO_CAP`
exception to the ordinary panel invariant that each unfilled slot has live
dispatch. No implementation may represent one peer boundary as multiple queued
continuations or allow an older continuation to run after replacement.

An execution failure or accepted non-filling receipt does not fill a gating
slot. Its Result appends typed Recovery Evidence; only the following
`RECOVERY_EVIDENCE` Transition tries the next allowed assignment in stable order using a
higher Attempt generation for the same slot Activity when its immutable
Activity inputs still match. If no eligible assignment is available, the Run enters
`WAITING/CAPACITY` with its original slots and approval threshold intact. A
later capacity observation appends Recovery Evidence selecting the closed
panel-scoped `STAFF_PANEL` tactic. Its separate Evidence Transition atomically
offers every still-unfilled slot only when the frozen evidence proves one
complete mutually legal assignment set; otherwise it offers none and writes a
new Wait with fresh Health and slot memberships. It never recomputes a smaller
panel or writes a Consensus Decision.

All gating slots close before aggregation, and every one must ultimately carry
an independent `APPROVE` for approval. This prevents a late blocker from
arriving after early approvals caused publication. Advisory slots, if a future
schema adds them, cannot count toward v1 quorum.

### Independent Review Receipt wire schema

```json
{
  "protocol": "orcest.review-receipt/1",
  "candidate": {
    "candidate_id": "uuid",
    "commit": {"object_format": "sha1", "oid": "40-hex-oid"}
  },
  "panel_round": 1,
  "reviewer_slot": "correctness",
  "role": "correctness",
  "subject_refs_digest": "sha256:64-lowercase-hex",
  "context_digest": "sha256:64-lowercase-hex",
  "assessments": [
    {
      "subject_ref": "snapshot:overall",
      "outcome": "SATISFIED",
      "evidence_refs": ["candidate:path/to/file:42"]
    },
    {
      "subject_ref": "plan:requirement:r1",
      "outcome": "SATISFIED",
      "evidence_refs": ["candidate:path/to/file:42"]
    }
  ],
  "verdict": "APPROVE",
  "findings": [],
  "abstention_code": null
}
```

The controller adds trusted `execution_profile_id`, `worker_profile`,
`provider`, `model`, `provider_account_ref`, `provider_family`, `model_family`,
`classification_revision`, `launch_attestation_id`, worker session, producing
Attempt metadata, and derived `fills_slot` before hashing and persistence.
`assessments` MUST contain every controller-supplied subject in the closed
membership. In v1 that membership is exactly `snapshot:overall` followed by
one `plan:requirement:<requirement_key>` for every requirement in the accepted
versioned Plan Result, preserving Plan order. Roles and repository
configuration cannot add, remove, or reorder subjects. `subject_refs_digest`
must equal the durable Assignment membership digest. Assessment outcome is
`SATISFIED`, `VIOLATED`, or `UNVERIFIABLE`.

The controller orders assessments by its frozen subject list and findings by
`finding_key`; duplicate or unknown subject references and duplicate finding
keys are invalid. Set-valued `subject_refs` and `evidence_refs` are
duplicate-free and lexicographically sorted before hashing. Model emission
order therefore cannot change consensus.

A finding has this normalized shape:

```json
{
  "finding_key": "unique-within-receipt",
  "severity": "BLOCKER",
  "category": "CORRECTNESS",
  "subject_refs": ["plan:requirement:r1"],
  "summary": "bounded statement of the defect",
  "location": {
    "path": "src/example.py",
    "start_line": 42,
    "end_line": 42
  },
  "evidence_refs": ["verification:unit:stderr"],
  "reproduction": {
    "argv": ["pytest", "tests/test_example.py::test_case"],
    "cwd": "."
  }
}
```

`severity` is `BLOCKER` or `ADVISORY`. Categories are the code-owned values
`SPECIFICATION`, `CORRECTNESS`, `SECURITY`, `TEST_COVERAGE`,
`MAINTAINABILITY`, and `OTHER`. Paths are normalized repository-relative paths;
line numbers are positive and refer to the exact Candidate. `location` and
`reproduction` may be null only when the finding explains why neither applies.
Free-form text is bounded untrusted evidence and cannot set severity by using a
keyword outside the typed field.

`reproduction.argv` is untrusted adjudication/remediation evidence, not an
executable workflow command. Orcest MUST NOT execute it directly, map it into
a new Verification Profile, or schedule an ad hoc verification Activity. The
only v1 verification gate remains the already frozen `default` Verification
Profile.

The controller assigns a durable `finding_id` after admission. Reducer sorting
uses `(panel_round, reviewer_slot, finding_key)`, not UUID or arrival order.

Verdict validity is exact:

- `APPROVE` requires every subject assessment in the frozen closed membership
  be `SATISFIED`, with no `BLOCKER`
  finding, and `abstention_code: null`.
- `BLOCK` requires at least one `BLOCKER` finding with a nonempty summary and
  evidence or a typed explanation of why executable evidence is impossible.
- `ABSTAIN` requires no blocking finding and one `abstention_code`:
  `ROLE_CONFLICT`, `INSUFFICIENT_EXPERTISE`, `EVIDENCE_INACCESSIBLE`, or
  `POLICY_PROHIBITS_JUDGMENT`.

The controller sets `fills_slot=true` only for a schema-valid `APPROVE` or
`BLOCK` receipt whose trusted assignment and independence checks pass. It sets
`fills_slot=false` for every `ABSTAIN` receipt.

Timeout, provider/capacity failure, malformed output, generic uncertainty, and
“no opinion” do not manufacture an `ABSTAIN` receipt; they are Attempt failure
or recovery inputs. An `ABSTAIN` Attempt Result MUST carry a valid `ABSTAIN`
Review Receipt, has `fills_slot=false`, and never counts as approval.

Every Review Receipt is uniquely bound to its producing Attempt. Multiple
non-filling receipts may remain as audit evidence for one slot. A partial unique
constraint permits at most one receipt with `fills_slot=true` for
`(candidate_id, panel_round, reviewer_slot)`. Once one receipt fills the slot,
a later Attempt cannot replace it. A controller may open a new panel round but
cannot edit the old verdict.

## Findings and disputes

Every `BLOCKER` finding is unresolved on admission. Advisory findings are
preserved for audit and projections but do not block the v1 gate. Original
findings and Review Receipts are immutable; remediation and adjudication add
new evidence rather than editing history.

The default reducer sends an undisputed unresolved blocker to `REMEDIATE`.
For one admitted `BLOCKER` finding `F` from filling reviewer slot `S`, let
`K(F)` be its sorted nonempty set of exact
`plan:requirement:<requirement_key>` subject references. `F` is disputed if and
only if there exists a `k` in `K(F)` and another filling Review Receipt in the
same Candidate/panel from slot `S2 != S` that both:

- records the persisted assessment `(k, SATISFIED)`; and
- contains no `BLOCKER` finding whose `subject_refs` contains `k`.

`VIOLATED` agrees with the blocker and `UNVERIFIABLE` creates no dispute. A
second `BLOCK` on `k` does not dispute it. Assessment/free-form arrival order
does not matter: the reducer reads the canonical persisted assessments and
findings, evaluates `K(F)` in byte-sorted order, and stores the resulting
finding set in the Consensus Decision input digest.

The v1 `orcest.diagnosis/1` schema has no `disputed_finding_ids` and cannot
classify a Review finding as disputed. Diagnosis remains recovery evidence,
not adjudication authority.

`snapshot:overall`, category, file location, shared prose, and overlapping
keywords are never sufficient dispute keys. A broad-only finding remains
undisputed and routes to remediation. Free-form phrases such as “I disagree”
do not create a dispute. A disputed blocker produces `ADJUDICATE`; an
undisputed or already sustained blocker produces `REMEDIATE`. If both exist,
`REMEDIATE` has priority because the exact Candidate already has a confirmed
unresolved blocker.

## Adjudication Receipt

Adjudication resolves evidence; it does not add an approval. An adjudicator
receives the exact Candidate, Snapshot, policy, disputed finding IDs, original
structured evidence, and the already admitted `default` Verification Receipt.
It does not receive a requested lifecycle outcome, and v1 does not schedule a
targeted verification or an extra reviewer to resolve a dispute.

V1 materializes exactly one adjudicator slot whose ID is `default` and whose
required count is the literal `1`. The pinned policy selects its primary
Execution Profile and ordered alternates; alternates are capacity fallbacks for
the same slot, not extra votes. For one Candidate and `panel_round`, the
controller freezes at most one disputed Finding set and creates one
`ADJUDICATE` Activity bound to that exact set. Its `adjudication_round` is
always the literal `1`. Failure, abstention, or `INCONCLUSIVE` evidence may
create a higher Attempt generation for that same Activity, slot, set, and
round; it does not increment `adjudication_round`.

If a decisive overrule opens a wholly fresh panel and that later panel creates
a distinct disputed set, the controller creates a new `ADJUDICATE` Activity
bound to the new `panel_round` and resets `adjudication_round` to `1`. V1 never
uses a value greater than `1`; the field is retained for explicit binding and
future protocol evolution, not as a retry counter.

An adjudication invocation MUST be fresh, MUST NOT be the Candidate-producing
or originating reviewer invocation, and MUST satisfy the adjudicator
independence constraints in the pinned policy. An alternate adjudicator is
selected in stable order without changing the one required slot or count.
The adjudicator Activity is distinct from the Candidate producer and every
originating reviewer. Each initial or replacement assignment uses a new
Attempt generation, model invocation,
conversation/context instance, and read-only workspace. It receives the frozen
dispute context but no prior adjudicator transcript, Receipt, or private
reasoning. These are normalized repository-plus-server policy checks performed
from durable assignments. Freshness and non-resumption additionally require
the exact signed Launch Attestation from the pinned registered runner shim;
worker prose or self-reported identity strings cannot satisfy them. The
attester is trusted only for launch isolation, not for Receipt truth or
workflow authority.

```json
{
  "protocol": "orcest.adjudication-receipt/1",
  "candidate": {
    "candidate_id": "uuid",
    "commit": {"object_format": "sha1", "oid": "40-hex-oid"}
  },
  "panel_round": 1,
  "adjudication_round": 1,
  "adjudicator_slot": "default",
  "subject_refs_digest": "sha256:64-lowercase-hex",
  "context_digest": "sha256:64-lowercase-hex",
  "dispositions": [
    {
      "finding_id": "uuid",
      "disposition": "OVERRULE",
      "evidence_refs": ["verification:default:command:unit"]
    }
  ],
  "new_findings": [],
  "abstention_code": null
}
```

`subject_refs_digest` MUST equal the exact durable ordered Activity Review
Subject membership projected with the adjudication Assignment. The controller
adds the producing Attempt's exact `launch_attestation_id` and other trusted
execution/session fields before computing the persisted Receipt digest.

Every assigned disputed finding appears exactly once. Resolution outcome is
`SUSTAIN`, `OVERRULE`, or `INCONCLUSIVE`. `SUSTAIN` keeps the blocker and routes
to remediation. `OVERRULE` resolves that finding only on that exact Candidate
and panel round; it is not an approval. If every disputed blocker is
`OVERRULE`d and no blocker remains, the reducer closes the adjudication evidence
and opens a wholly fresh `panel_round` with every gating slot. No prior receipt
fills the new round. `INCONCLUSIVE` resolves nothing and schedules the next
policy-eligible fresh adjudication Attempt or waits for capacity/evidence. A
newly discovered blocker uses the Review finding schema and routes to
remediation. Its canonical key is
`(panel_round, adjudication_round, adjudicator_slot, finding_key)`; a UUID never
selects ordering.

An adjudicator may abstain only with the Review abstention codes and no
dispositions. The controller derives `fills_slot=true` only for a schema-valid,
independent receipt in which every assigned disputed finding is decisively
`SUSTAIN`ed or `OVERRULE`d. Any abstention or `INCONCLUSIVE` disposition makes
`fills_slot=false`. The one accepted filling Receipt for slot `default` is
decisive: any `SUSTAIN` leaves the safety blocker unresolved and routes it to
remediation, while `OVERRULE` must cover every assigned disputed blocker before
a fresh full panel can open. Any abstention or `INCONCLUSIVE` triggers the next
policy-eligible fresh Attempt generation for the same adjudication Activity or
waiting—never a new Verification Profile, extra reviewer, extra adjudicator,
or lower threshold.

Every Adjudication Receipt is uniquely bound to its producing Attempt. Multiple
non-filling receipts may remain as audit evidence; a partial unique constraint
permits at most one `fills_slot=true` receipt for
`(candidate_id, panel_round, adjudication_round, adjudicator_slot)`. The reducer sorts
dispositions by the original finding's canonical key and adjudicator slot.

## Deterministic consensus reducer

The controller invokes the consensus reducer and writes a Consensus Decision
only while the Run is in `AGGREGATING`, after the `default` Verification
Receipt is `PASS` and every frozen reviewer gating slot has exactly one
eligible `fills_slot=true` Receipt. Missing reviewer or adjudicator capacity is
handled in `REVIEWING` or `ADJUDICATING` by a bound `WAITING/CAPACITY` Wait
Condition and `STAFF_PANEL`; it never creates a Consensus Decision.

The reducer evaluates one immutable normalized input object:

```text
candidate_id and commit
specification_generation
policy_hash
default VERIFY activity_id, attempt_id/generation, and PASS Receipt
panel_round, frozen reviewer-slot assignments, and one filling Receipt per slot
```

It validates the `default` Verification Profile checks in pinned command order,
reviewer Receipts by frozen slot order, and findings by their canonical keys.
It MUST NOT sort by Receipt UUID, completion timestamp, Redis entry ID, worker
speed, or arrival order.

The v1 decision table is evaluated top to bottom:

| Condition on the exact closed aggregation input | Decision or lifecycle action |
| --- | --- |
| Candidate, Snapshot generation, commit, or policy binding is stale | No decision; reject the trigger and reconcile current work |
| The `default` Receipt is not an eligible `PASS`, or any reviewer slot is missing, non-filling, duplicated, or independence-invalid | No decision; fail closed and return the invalid input to its owning recovery/review path |
| Any undisputed blocker exists | `REMEDIATE` with all canonical unresolved blockers |
| Only disputed unresolved blockers remain | `ADJUDICATE` with their canonical finding set |
| Every filling reviewer Receipt is `APPROVE` and no blocker remains | `APPROVED` |

The only v1 Consensus Decision outcomes are `APPROVED`, `REMEDIATE`, and
`ADJUDICATE`. The `ADJUDICATING` reducer later applies Adjudication Receipts
directly: sustain/new blocker routes to remediation, complete overrule opens a
wholly fresh full reviewer panel, and non-filling evidence retries or waits.
It does not return to `AGGREGATING` or write a second Decision for the old
panel.

Exactly one Consensus Decision may exist for `(candidate_id, panel_round)`; v1
has no `decision_generation`. The Decision stores the exact Candidate and
policy bindings, default Verification Receipt ID, canonically ordered reviewer
Receipt semantic keys and IDs, outcome, and canonically ordered unresolved
finding IDs. Its `decision_digest` is SHA-256 over that normalized object.
Human-facing summaries are later Projections and cannot alter the stored
outcome.

## Invalidation and reuse

Selecting a different current Candidate immediately makes every earlier
Candidate's Verification Receipts, Review Receipts, Adjudication Receipts, and
Consensus Decisions ineligible for gating. The records remain immutable audit
history. No verification, approval, overrule, or “unchanged files” assertion is
carried to the new Candidate in v1.

A changed specification generation, policy hash, base/parent binding, or
Activity `observed_change_request_head` binding derived from its exact
`change_request_head_observation_id` supersedes Activities and evidence bound
to the prior input. A new Candidate always begins with a
fresh single `VERIFY` Activity containing every ordered `default` command
before review. A same-Candidate execution retry does not mutate an accepted
Attempt-bound receipt. It may add another non-filling receipt, but it cannot
create a second filling receipt for the same slot.

After Change Request linkage, a newer authenticated head observation has
precedence in every gate/recovery/wait/boundary state. It supersedes all
Activities and gate evidence bound to the old head, enters `PR_REMEDIATING`,
imports the observed head as a Candidate, and runs the entire verification and
fresh-panel gate. No old-head receipt can arrive late and restore eligibility.

Post-publication remediation uses the same full configured verification and
consensus gate plus the exact causal Forge Observation and the Activity's
separate `change_request_head_observation_id`/`observed_change_request_head`
fence. v1 has no weaker post-publication gate, and provider availability can
never cause a reduction.

## Failure behavior

| Failure | Required behavior |
| --- | --- |
| Verification command returns nonzero | Admit `FAIL`; remediate using exact evidence; never reinterpret it as infrastructure error to obtain approval |
| Verification sandbox/tool fails | Admit `ERROR` when schema-valid; recover or replace; never pass |
| Reviewer times out or provider fails | Fence/retry/replace the Attempt; do not create an approval or automatic abstention |
| Reviewer output is malformed | Reject the receipt; schema repair then replacement under lifecycle policy |
| Two workers race for one slot | Attempt fencing accepts only the current claimed generation; partial uniqueness admits at most one filling receipt while retaining non-filling audit receipts |
| Receipt arrives for an older Candidate/panel/policy | Reject for gating, record bounded audit metadata, and do not mutate the current decision |
| Reviewer returns `APPROVE` with missing assessment or blocker | Reject as internally inconsistent, not as an approval |
| Required reviewer capacity disappears | Use compatible ordered alternates, then `WAITING/CAPACITY`; preserve quorum and independence |
| Reviewers disagree | Classify only a narrow requirement-bound typed contradiction and adjudicate it independently; do not add an ad hoc verifier or reviewer |
| Adjudicator abstains or is inconclusive | Dispatch the next policy-eligible fresh adjudication Attempt or wait; never overrule by timeout |
| Remediation repeats an equivalent blocker | Diagnose, replan, generate alternatives, and continue the autonomous recovery ladder |
| Controller/Redis restarts after receipt acceptance | Recover receipts and Decisions from SQLite; Redis reconstruction cannot erase or invent a vote |
| Receipt arrival order changes | Canonical sets produce the same Decision digest and outcome |

No failure on this page is by itself a valid `NEEDS_HUMAN` reason. Only the
lifecycle's allowlisted exceptional boundary can enter that resumable state
after autonomous recovery and evidence gathering.

Repeated gate failure is counted by the lifecycle's normalized failure
fingerprint, not prose similarity. When the same verification failure or
review/adjudicated-blocker fingerprint reaches the pinned
`maxRepairCyclesBeforeDiagnosis`, the Transition stores the otherwise valid
Receipt/Decision, appends `VERIFICATION_FAILURE` or `REVIEW_DISAGREEMENT`
Recovery Evidence with `selected_tactic = DIAGNOSE`, enters `RECOVERING`, and
plans no further remediation in that Transition. Only the subsequent Evidence
Transition may enter `DIAGNOSING`.

## Security requirements

- Review and adjudication workspaces are fresh and read-only with respect to
  the Candidate Store and forge; workers cannot mutate the Candidate they rate.
- Candidate code is not executed during model review. Only declared
  verification commands execute it, in the verification sandbox.
- Reviewers receive no forge-write, publication, controller-write, Secret
  Store, or unrelated-Run credential. Provider material remains Attempt-scoped.
- Work Item, repository, finding, and model text is untrusted. Schema validators
  impose byte, array, nesting, path, and evidence-reference limits before it is
  persisted or projected.
- Candidate-authored workflow or prompt changes cannot affect the active Run.
- Private model reasoning is neither required nor used as a reducer input;
  only the bounded structured receipt is authoritative evidence.

## Conformance tests

An implementation is not conformant until automated tests demonstrate:

- the same receipt set in every arrival order yields byte-identical normalized
  decision inputs, outcome, unresolved finding set, and decision digest;
- reviewer/adjudicator capacity loss creates a bound Wait Condition in its
  owning state and no Consensus Decision;
- exactly one Consensus Decision is inserted per Candidate/panel round, only
  from `AGGREGATING`, and its outcome is exactly `APPROVED`, `REMEDIATE`, or
  `ADJUDICATE`;
- no combination of fewer than two independent approvals passes the default
  policy;
- repository policy whose gating-slot count differs from
  `approvals_required` is rejected, and every server-added mandatory slot
  raises the effective approval threshold by one;
- one unresolved blocker prevents approval regardless of approval count;
- timeout, `ABSTAIN`, `ERROR`, missing capacity, and malformed output never
  count as a pass or approval;
- every Candidate/specification/policy gate has exactly one `VERIFY` Activity
  for Verification Profile ID `default`; a policy change creates a new bound
  Activity, while command additions remain within that Verification Profile;
- prior Attempt-bound Verification `ERROR` Receipts remain audit history but do
  not poison or replace a later decisive `PASS` or `FAIL` from the same
  Activity;
- an Execution Profile resolution or substitution that produces a forbidden
  `(worker_profile, provider, model, provider_account_ref)` combination is
  rejected, while loss of all valid substitutions waits without weakening;
- a reviewer cannot see another review receipt before its own receipt commits;
- normalized repository-plus-server independence rejects any reviewer or
  adjudicator assignment that reuses an invocation, conversation/context
  instance, mutable workspace, or prohibited
  `provider`/`model`/`provider_account_ref` identity;
- every model-backed Review/Adjudication Receipt copies the exact accepted
  Launch Attestation, and a reused attestation, nonce, workspace/context/
  invocation identity, resumed parent, wrong registered image/key/revision, or
  receipt/Attempt mismatch is rejected before it can fill a slot;
- every adjudication uses exactly slot `default`, required count `1`, and
  `adjudication_round = 1`; replacement execution increments only its Attempt
  generation, while a later fresh panel's distinct disputed set creates a new
  Activity also bound to round `1`;
- `snapshot:overall`, overlapping prose, category, or location alone never
  creates a dispute; only an exact shared `plan:requirement:<requirement_key>`
  plus the allowed typed contradiction can do so;
- a blocker on requirement `k` plus another filling slot's `SATISFIED`
  assessment for `k` and no blocker on `k` is disputed, while `UNVERIFIABLE`,
  `VIOLATED`, or a second blocker on `k` is not; receipt arrival order produces
  the same classification;
- a Diagnosis Result cannot create a dispute or supply
  `disputed_finding_ids` in v1;
- stale generation, Candidate, panel round, policy, and Forge Observation
  receipts cannot affect a current gate;
- every new Candidate requires fresh verification and review;
- `SUSTAIN`, `OVERRULE`, and `INCONCLUSIVE` produce the exact decision-table
  outcomes without editing the original finding;
- disagreement dispatches only the configured adjudication path and, after a
  complete overrule, a fresh full panel; it never invents a targeted
  Verification Profile or extra reviewer slot;
- decisively overruling every disputed blocker closes that adjudication and
  opens a wholly fresh full panel whose slots receive no earlier Receipt and
  writes no second Consensus Decision for the old panel;
- a first receipt-bearing Result at or after its execution deadline is rejected,
  while an exact replay of one accepted before the deadline returns the same
  Receipt identity;
- duplicate Attempt results are idempotent only for identical normalized
  content, and concurrent receipts cannot create two filling receipts for one
  slot; and
- controller/Redis failure at each receipt/Decision transaction boundary
  recovers to zero or one accepted fact, never a fabricated vote.

## Evidence and migration

Current behavior being replaced or retained:

- `src/orcest/orchestrator/task_publisher.py` currently asks one implementation
  worker to finish, push, and open a PR. V1 separates Candidate production from
  independent verification/review and gives publication only to the controller.
- `src/orcest/orchestrator/pr_ops.py` currently treats forge CI, review state,
  and unresolved review threads as the main feedback loop after publication.
  V1 retains that final forge loop but adds a durable pre-PR gate, reducing
  avoidable PR/CI churn.
- `src/orcest/orchestrator/provider_pool.py` and
  `src/orcest/shared/providers.py` select providers and account credentials, but
  do not persist a review panel, prove fresh-context independence, or aggregate
  exact-Candidate receipts. V1 adds those durable assignments and checks.
- `src/orcest/shared/models.py` has no Candidate, Verification Receipt, Review
  Receipt, adjudication, or Consensus Decision schema. Its Redis Task/Result is
  replaced at this boundary by typed, fenced controller admission.
- `tests/orchestrator/test_pr_ops.py` proves that current post-PR review feedback
  is observed and re-enqueued. That behavior remains a post-publication input;
  it does not substitute for the new pre-publication consensus tests.
- `.github/workflows/claude-review.yml` demonstrates an existing independent
  review surface after PR creation. V1 does not treat that forge check as one of
  its pre-PR receipts unless a future adapter explicitly imports and binds such
  evidence to the exact Candidate under this protocol.

Deliberately deferred rollout and implementation validation gates:

These experiments are not prerequisites for reviewing the normative review
contract. Production enablement requires their recorded results; deferring
them does not weaken receipt admission, independence, or consensus rules:

- define and exercise the server Execution Profile registry, including its
  exact `(worker_profile, provider, model, provider_account_ref)` resolutions
  and fresh-invocation independence dimensions;
- validate structured review and adjudication schemas across every supported
  harness without accepting prose-only fallback;
- establish sandbox and evidence size/retention limits on representative and
  adversarial repositories;
- evaluate whether the Plan Result's requirement mappings and mandatory
  overall Snapshot assessment detect omission and scope drift across
  representative Work Items; and
- failure-inject panel staffing, concurrent slot results, Candidate
  invalidation, adjudication, and Decision commits.
