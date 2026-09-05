# Orcest Engineering Wiki

This directory is the design home for Orcest's orchestration model. It keeps
the behavior implemented today separate from the accepted workflow-control
specification so that intended requirements are not mistaken for shipped
guarantees.

## Document classes

- **Current behavior** describes the implementation as it exists. Start with
  [Current orchestrator state](current-orchestrator-state-model.md) when
  diagnosing or changing the present system.
- **Accepted normative specification** defines the intended v1 design and its
  implementation requirements. Acceptance does not mean the design is already
  implemented or deployed.
- **Historical plans** explain how earlier behavior was introduced. They are
  useful evidence, but they are neither current behavior nor the accepted
  specification.
- **Architecture decision records (ADRs)** will capture later decisions that
  change or interpret an accepted specification. Do not create ADRs merely to
  restate a page in this wiki.

Every normative page begins with a review status. The words `MUST`, `SHOULD`,
and `MAY` are normative only in an accepted specification page.

## Current behavior

- [Current orchestrator state](current-orchestrator-state-model.md) — GitHub
  snapshot authority, Redis coordination, stale-work fencing, and current
  recovery guarantees.
- [Issue dependencies](../issue-dependencies.md) — current dependency syntax
  and issue deferral behavior.
- [Adding a provider](../adding-a-provider.md) — current provider integration
  contract.

## Accepted v1 specification

The accepted design gives Orcest authority over work between forge intake and
final change publication. The forge remains authoritative for issue intake and
for the published pull request, CI, and review state. The pages below are the
smallest useful split for that design; avoid adding a page until a topic has a
distinct owner or review boundary.

Read them in this order:

1. [Architecture](architecture.md) — system boundary, topology, trust zones,
   and forge-neutral seams.
2. [Domain model](domain-model.md) — Runs, Activities, Attempts, Candidates,
   reviews, transitions, and Publications.
3. [Workflow lifecycle](workflow-lifecycle.md) — deterministic reducer,
   transition rules, recovery ladder, and exceptional human escalation.
4. [Worker protocol](worker-protocol.md) — versioned delivery, claims,
   generation fencing, credentials, and Candidate submission.
5. [Review and consensus](review-and-consensus.md) — deterministic
   verification, independent review, quorum, remediation, and adjudication.
6. [Persistence and recovery](persistence-and-recovery.md) — SQLite, Redis,
   transactional outbox, artifacts, reconciliation, backup, and crash safety.
7. [Forge integration](forge-integration.md) — issue admission, status
   Projections, idempotent PR publication, CI monitoring, and remediation.
8. [Repository configuration](repository-configuration.md) — trusted
   `.orcest` configuration, schemas, pinning, and CLI onboarding.
9. [Operations and rollout](operations-and-rollout.md) — upgrades,
   compatibility, observability, garbage collection, migration, and
   acceptance tests.

[Spec-writing plan](spec-writing-plan.md) defines the page ownership,
dependencies, evidence, review sequence, and completion gates. It is a plan,
not part of the normative specification.

Security is a cross-cutting concern and belongs beside each protocol's
requirements rather than in a detached security page. The architecture page
owns the overall threat and trust model; the worker, persistence, forge, and
operations pages own enforcement within their boundaries.

## Historical plans and designs

These records may describe behavior that has since changed:

- [Phase 1: core loop](../plans/phase-1-core-loop.md)
- [Phase 2: full PR management](../plans/phase-2-full-pr-management.md)
- [Phase 3: issue processing](../plans/phase-3-issue-processing.md)
- [Phase 4: codebase improvement](../plans/phase-4-codebase-improvement.md)
- [Superpowers plans and specs](../superpowers/)

When historical text conflicts with current behavior,
`current-orchestrator-state-model.md` and the code win. When it conflicts with
an accepted normative page, the accepted page defines the intended
implementation.

## Future ADRs

Create `docs/wiki/adrs/` only when the first cross-page decision needs a stable
record. ADRs should identify the affected canonical pages, alternatives, and
migration impact. Accepted ADRs must be folded back into those pages so readers
do not need to replay an ADR history to learn the active contract.
