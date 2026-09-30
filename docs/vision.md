# Orcest vision

Last updated: 2026-09-05

This document records the product direction agreed with Austin. It guides future
design and implementation; it is not a claim that every capability below exists
or is deployed. Update it when the direction changes. Detailed specifications,
issues, and implementation plans should follow this vision.

## Purpose

Orcest is a lightweight coordination system for autonomous agents working across
projects. A person sets the vision, priorities, and constraints; agents help turn
that direction into a roadmap and actionable work; Orcest keeps eligible work
moving and makes the system's progress understandable.

The long-term aspiration is delegated product development: agents plan, implement,
review, and improve a project with progressively less intervention on individual
tasks. The human sets direction and handles exceptional decisions or access
problems. This is a direction to build toward, not a description of today's fleet.

## What Orcest owns

- Discover work and understand what makes it eligible to proceed.
- Coordinate execution across projects, provider accounts, and workers.
- Observe external outcomes and decide when to dispatch follow-up work, wait,
  retry, or escalate.
- Preserve enough context and evidence for work to continue across agent runs.
- Explain what the system knows, what it is doing, and why work is not moving.

Orcest observes and responds to the delivery process. It does not need to become
a CI/CD engine, issue tracker, or replacement for every tool used by a project.
It may request actions in those systems; they remain responsible for executing
their own checks, reviews, and deployment processes.

## Current foundations and future direction

| Area | Current foundation | Direction |
| --- | --- | --- |
| Work discovery | GitHub issues labeled `orcest:ready`, plus PR monitoring | Keep discovery separable from the system that supplies work |
| Eligibility | GitHub issue dependency links and other observed conditions | Explain prerequisites and re-evaluate eligibility as conditions change |
| Delivery signals | GitHub Actions, PR checks, reviews, and merge state | Observe external delivery systems and coordinate the next action |
| Execution | Configured coding providers and a managed fleet of ephemeral VMs | Coordinate usage and compute capacity while keeping those concepts separate |
| Direction | Human-authored issues and agent-assisted planning | Carry high-level roadmaps through decomposition, execution, and improvement |
| Project policy | Existing configuration and repository instructions | Project-owned `.orcest` rules; proposed, not deployed |
| Visibility | Existing operational surfaces and a separate dashboard prototype | A live management view of work, outcomes, blockers, and fleet capacity |

These foundations describe the repository and our discussion, not a verified
inventory of the currently deployed environment.

## Principles

### Keep the coordinator lightweight

Use external systems for capabilities they already provide. Prefer explicit
eligibility rules and inexpensive checks where those are sufficient; use agents
for work that benefits from reasoning. New abstractions should solve concrete
coordination needs rather than prebuild a general company-management platform.

### Integrations are replaceable

GitHub is useful because issues, dependency relationships, reviews, and Actions
already exist together. It is the current integration, not the product boundary.
Domain concepts should describe work, dependencies, checks, outcomes, and policy
without requiring every future project to use GitHub. Preserve native links and
evidence so users can inspect the underlying system.

### Autonomy is the normal path

Eligible work should progress without per-task human approval unless project
policy requires it. Ordinary dependency waits, CI waits, usage cooldowns, and
recoverable failures should have automatic continuation paths. Human attention
should be an exceptional, actionable escalation explaining what is missing and
what would allow work to resume. Do not hide unresolved failures to appear
autonomous.

### Separate work, runs, accounts, and workers

A work item can span multiple agent runs. A configured provider account supplies
credentials and has its own usage constraints. A worker is temporary compute
that executes a task using a compatible provider. Account availability, worker
liveness, agent progress, and delivery success are distinct facts.

### Observe outcomes rather than assume success

An agent finishing is evidence about an execution attempt. It does not by itself
prove that the intended project outcome was achieved. Continuation and completion
should use relevant external evidence and project policy. Context must survive
the replacement of a VM or a change of agent.

## The dashboard is the next step

The dashboard should answer: **Is the fleet progressing everything it can, and
what explains anything that is not progressing?**

It should make it easy to see:

- What Orcest knows about, including work that cannot yet be enqueued.
- What is ready, executing, waiting, and completed.
- Why an item is waiting and what event or condition will let it proceed.
- The context, current output, attempt history, and outcome evidence for an item.
- Available execution capacity and provider usage or cooldowns where known.
- The rare situations that require a human action.

The initial prototype remains separate from the live system and uses mock data.
Its purpose is to validate the experience before integration. Backend changes
are in scope when needed to support accurate visibility or better coordination;
the existing Redis representation should not dictate the user interface.

Dependencies matter before the first execution as well as between attempts.
Being outside a queue does not put an item outside Orcest's responsibility.
The prototype uses Upcoming → In progress → Done for the work lifecycle.
Waiting is an activity status within a stage: an unmet prerequisite can keep
work Upcoming, while CI or a retry can pause work already In progress. Queued
and actively executing are also activity states, separate from the lifecycle.
The board includes tracked work that previously appeared in a Discovered tab.

See [dashboard data mapping](dashboard-data-mapping.md) for the source audit,
existing API coverage, and recommended integration sequence. That document is an
implementation aid; it does not change this vision or claim a live rollout.

## Project rules and longer-term delegation

A proposed `.orcest` directory would let each participating project express rules
Orcest can follow. Its schema, relationship to existing agent instructions,
precedence, and rollout need a separate design. This document does not establish
any configuration contract.

Beyond the dashboard, the direction includes connecting roadmap intent to work
and outcomes, allowing planning agents to decompose and delegate work, and using
reviewers or tools such as Grok bot to feed subsequent improvements. These are
capability horizons, not committed milestones or permission for agents to expand
project scope without bounds. Project policy and human direction govern them.

## Relationship to Symphony

Symphony is a useful reference for agent orchestration, observable execution,
continuation, and repository-defined workflows. Orcest's direction also includes
coordinating external delivery signals, multiple provider accounts, and an
ephemeral fleet across projects. Replacing Orcest with Symphony would require
evaluating how those responsibilities would be retained or rebuilt.

Borrow useful ideas without treating Symphony's example workflow or dashboard
as Orcest's product specification. This is not a permanent rejection of reuse.

## Maintaining this direction

When proposing a significant change, explain how it advances this vision,
whether it changes a principle, and which parts are implemented versus planned.
Record agreed changes here; keep implementation details and task tracking in
their own documents and issues. Historical plans may contain older labels or
assumptions and should not silently override this direction.

Progress means work advances with fewer routine interventions, waits and failures
are explainable, resources are used responsibly, and a person can connect the
fleet's activity to the direction they set.
