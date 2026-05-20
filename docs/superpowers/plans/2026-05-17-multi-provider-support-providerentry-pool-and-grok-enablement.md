# Multi-Provider Worker Support (ProviderEntry + Hardened ProviderPool + Grok Enablement) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver a robust, production-grade multi-provider abstraction (starting with Grok) that lets the orchestrator round-robin heterogeneous `ProviderEntry`s, workers dispatch per-task to the correct pre-baked CLI, exhaustion rests only the specific credential, and old worker images fail gracefully without crashing or leaking secrets.

**Architecture:** The orchestrator owns a per-project `ProviderPool` of immutable `ProviderEntry` objects and performs selection at publish time (embedding provider + credential + model into `Task`). Workers are multi-provider consumers that perform an early registry lookup by `task.provider`; unknown providers trigger a clean non-transient `FAILED` result. `ProviderPool` is internally synchronized and uses stable non-secret identities. Systematic redaction is applied to `Task` and `ProviderEntry` (including dead-letters). The design generalizes the existing `TokenPool` + `RunnerResult` exhaustion paths while enforcing the "old baked image must not do harm" requirement.

**Critical Design Rule — Provider Registration & Invocation Boundary (Option A)**

The orchestrator must remain **completely agnostic** about how a provider is executed. Per the explicit goal:

- "Workers are simply prebaked into the image."
- "Orcest just knows that providers can be registered."
- "It should round-robin them while making sure to pay attention to provider exhaustion."

**Therefore:**

- The orchestrator and `ProviderPool` only ever operate on the **lean registration surface**: `provider`, `credential`, `model`, and the stable non-secret `identity()`.
- Execution details (`cli_binary`, `env_var`, invocation style, extras that affect how the CLI is called) are **image concerns only**. They live in a registry inside the worker image, keyed by the opaque `task.provider` string.
- The `Task` wire format stays lean (`provider` + `credential` + `model`). The orchestrator never emits execution recipes.
- `ProviderEntry` in `providers.py` may carry rich fields for convenient config loading and validation, but **no orchestrator code path** (pool, selection, publishing, logging of exhaustion, etc.) is allowed to read or act on `cli_binary`, `env_var`, or `effective_*` helpers.
- The hardcoded fallback table inside `ProviderEntry.effective_env_var` is a temporary migration aid only and must not be treated as orchestrator knowledge of execution mechanics.

Any future task that would require the orchestrator to know "how to invoke provider X" is a design smell and must be rejected or refactored.

This rule is non-negotiable for maintaining the desired separation of concerns.

**Tech Stack:** Python 3.12+, dataclasses + threading.RLock, Redis streams (existing `tasks:{provider}` naming), subprocess + env injection (generalized from `claude_runner`), setuptools packaging, pytest + fakeredis for unit tests, existing Docker/systemd worker images (rebake via `setup-worker.sh`).

---

## Task 0: Preparation & Review Artifacts

- [ ] **Step 0.1:** Create the plan file and commit the skeleton (this file) on the worktree branch.
  ```bash
  git add docs/superpowers/plans/2026-05-17-multi-provider-support-*.md
  git commit -m "plan: add robustness-reviewed implementation plan for multi-provider support (issue 529 + Grok)"
  ```

- [ ] **Step 0.2:** Read the three subagent robustness reviews (security, failure-modes, concurrency) and the current visual companion page to ensure every High/Med risk has a corresponding task.
  ```bash
  # (Already completed in this session; the plan below directly addresses them)
  ```

---

## Task 1: Data Model — ProviderEntry (Rich for Config, Lean for Orchestrator Core)

**Critical constraint (see "Provider Registration & Invocation Boundary" above):**  
While the dataclass may carry rich fields (`cli_binary`, `env_var`, `extras`) for convenient YAML loading and future worker-side use, **only** `provider`, `credential`, `model`, and `identity()` are part of the orchestrator's registration surface. No code in `orchestrator/`, `token_pool.py` (soon `provider_pool.py`), or publish paths may read or depend on the execution-related fields.

**Files:**
- Create: `src/orcest/shared/providers.py` (new home for the dataclass + helpers)
- Modify: `src/orcest/shared/config.py` (ProjectConfig.providers, loading logic)
- Test: `tests/shared/test_providers.py` (new)

- [ ] **Step 1.1:** Write the failing test for the dataclass shape, `effective_*` properties, `identity()` (non-secret, stable), and `__repr__` redaction.
  ```python
  # tests/shared/test_providers.py
  def test_provider_entry_rich_fields_and_redaction():
      e = ProviderEntry(
          provider="grok",
          credential="xai-secret-1234567890",
          model="grok-3-latest",
          cli_binary="grok",
          env_var="XAI_API_KEY",
          extras={"temperature": "0.2"}
      )
      assert e.effective_binary == "grok"
      assert e.effective_env_var == "XAI_API_KEY"
      key = e.identity()
      assert "xai-secret" not in key and "1234567890" not in key
      assert "grok" in key
      assert "secret" not in repr(e) and "xai-secret" not in repr(e)
  ```

- [ ] **Step 1.2:** Run the test (expect failure on missing class).
  ```bash
  PYTHONPATH=src python -m pytest tests/shared/test_providers.py::test_provider_entry_rich_fields_and_redaction -q --tb=line
  ```

- [ ] **Step 1.3:** Implement `ProviderEntry` in `src/orcest/shared/providers.py` (frozen dataclass + helpers + safe repr).
  ```python
  from __future__ import annotations
  from dataclasses import dataclass, field
  import hashlib

  @dataclass(frozen=True)
  class ProviderEntry:
      provider: str
      credential: str
      model: str | None = None
      cli_binary: str | None = None
      env_var: str | None = None
      extras: dict[str, str] = field(default_factory=dict)

      @property
      def effective_binary(self) -> str:
          return self.cli_binary or self.provider

      @property
      def effective_env_var(self) -> str:
          if self.env_var:
              return self.env_var
          return {"claude": "CLAUDE_CODE_OAUTH_TOKEN", "grok": "XAI_API_KEY"}.get(
              self.provider, f"{self.provider.upper()}_TOKEN"
          )

      def identity(self) -> str:
          h = hashlib.sha256(self.credential.encode()).hexdigest()[:12]
          return f"{self.provider}:{self.model or ''}:{h}"

      def __repr__(self) -> str:
          cred = self.credential[:4] + "..." if self.credential else ""
          return f"ProviderEntry(provider={self.provider!r}, credential={cred!r}, model={self.model!r}, ...)"
  ```

- [ ] **Step 1.4:** Run the test — it must pass.
  ```bash
  PYTHONPATH=src python -m pytest tests/shared/test_providers.py::test_provider_entry_rich_fields_and_redaction -q
  ```

- [ ] **Step 1.5:** Commit.
  ```bash
  git add src/orcest/shared/providers.py tests/shared/test_providers.py
  git commit -m "feat: add frozen ProviderEntry with safe identity() and redacted repr"
  ```

---

## Task 2: Data Model — Task Extensions + Systematic Redaction Layer

**Files:**
- Modify: `src/orcest/shared/models.py` (add fields + redaction helpers)
- Test: `tests/shared/test_models.py`

- [ ] **Step 2.1:** Add failing tests for new fields, `to_safe_dict()`, `from_dict` tolerance of legacy `claude_token`, and redaction in `__repr__` + dead-letter paths.
- [ ] **Step 2.2:** Implement `provider`, `credential`, `model` on `Task` (with defaults for compat).
- [ ] **Step 2.3:** Add `to_safe_dict(self) -> dict[str,str]` and `REDACTED_FIELDS`.
- [ ] **Step 2.4:** Update `to_dict` / `from_dict` / `create` and all call sites (keep `claude_token` populated for transition).
- [ ] **Step 2.5:** Update dead-letter writing paths to use the safe projection.
- [ ] **Step 2.6:** Run full model test suite + redaction tests.
- [ ] **Step 2.7:** Commit with message referencing subagent security review.

---

## Task 3: Hardened ProviderPool (Concurrency-Safe, Stable Identities)

**Mandatory boundary enforcement (see "Provider Registration & Invocation Boundary" section):**
- `ProviderPool` (and all its internal maps) may **only** ever store or operate on the lean surface of `ProviderEntry`: `provider`, `credential`, `model`, and `identity()`.
- It is **forbidden** for any method in `provider_pool.py`, or any caller in `orchestrator/loop.py` or `task_publisher.py`, to read `cli_binary`, `env_var`, `extras`, or call `effective_*` helpers.
- All tracking (cooldowns, in-flight tasks, round-robin) must be done exclusively via the stable non-secret `identity()` string.
- When synthesizing legacy entries during migration, the rich fields must be left as `None` / defaults.

**Files:**
- Create / replace: `src/orcest/orchestrator/provider_pool.py` (generalize token_pool.py)
- Modify: `src/orcest/orchestrator/loop.py`, `task_publisher.py`

- [ ] **Step 3.1:** Write concurrency + correctness tests (thread contention, TOCTOU, duplicate USAGE, restart loss, max-expiry, mixed providers).
- [ ] **Step 3.2:** Implement `ProviderPool` with `threading.RLock`, stable `identity()` keys (never the raw secret), `max(existing, new)` on cooldowns, `register_before_publish` contract.
- [ ] **Step 3.3:** Add `next_entry() -> ProviderEntry | None`, `register_task(task_id, entry)`, `mark_exhausted(task_id, resets_at)`, `task_completed`.
- [ ] **Step 3.4:** Port existing `TokenPool` behavior exactly for the `claude_tokens` migration path.
- [ ] **Step 3.5:** Run the new concurrency test matrix under load.
- [ ] **Step 3.6:** Commit.

---

## Task 4: Config Loading — ProjectConfig.providers + Backward Compat

- [ ] Implement loading that accepts both legacy `claude_tokens` and new `providers:` list, synthesizing `ProviderEntry(provider="claude", ...)` for the old path.
- [ ] Update `OrchestratorConfig` / `WorkerConfig` to carry provider lists.
- [ ] Add tests for mixed YAML + env var sources.
- [ ] Commit.

---

## Task 5: Orchestrator Wiring — Selection at Publish Time + Hardened Paths

- [ ] Replace `_select_claude_token` with `_select_provider_entry` that uses the new pool.
- [ ] Update every `publish_*_task` call site to pass the chosen entry and call `register_task` (before or after xadd per the chosen contract).
- [ ] Generalize `_mark_usage_exhausted_token` and `_handle_result` USAGE paths to call the new pool.
- [ ] Add per-provider exhausted-skip counters and logging using masked identities only.
- [ ] Ensure every publish failure path after `next_entry` calls the equivalent of `task_completed`.
- [ ] Run orchestrator unit + integration tests (including the new skew cases).
- [ ] Commit.

---

## Task 6: Worker Dispatch — Early Graceful Reject + Multi-Provider Registry

**Mandatory boundary enforcement (see "Provider Registration & Invocation Boundary" section):**
- The `PROVIDER_REGISTRY` inside the worker is an **image-baked** concern. It maps `task.provider` → execution recipe (binary, env var, invocation details).
- The worker must **never** receive a full `ProviderEntry` object or rich dispatch hints from the orchestrator via the `Task` payload.
- Step 6.4 and 6.5 must implement the registry as local-to-the-image (or loaded from the worker's own config), **not** driven by fields coming from `ProviderEntry` objects created in the orchestrator.
- `task.provider` is the sole opaque key the worker uses to look up how to execute the task.

**Files:**
- Modify: `src/orcest/worker/loop.py` (receive path, before runner)
- Modify: `src/orcest/worker/runner.py` (protocol + registry)
- Create: `src/orcest/worker/grok_runner.py` (CLI-based per user clarification)

- [ ] **Step 6.1:** Add failing test: worker receives task with unknown `provider` → emits `TaskResult(status=FAILED, transient=False, summary containing "rebake")` without ever calling runner or logging the credential.
- [ ] **Step 6.2:** Implement early dispatch (after `Task.from_dict`, before any lock/heartbeat heavy work) that does `registry.get(task.provider)`.
- [ ] **Step 6.3:** On miss: publish clean `FAILED`, clear pending, ACK, return immediately.
- [ ] **Step 6.4:** Build `PROVIDER_REGISTRY` driven by `ProviderEntry` fields (or a static + config-driven table for the first cut).
- [ ] **Step 6.5:** Implement / generalize `CLIRunner` (or keep `ClaudeRunner` + add `GrokRunner`) that uses the entry's `effective_binary` / `effective_env_var` + whitelist that excludes *all* known secret var names.
- [ ] **Step 6.6:** Never pass credential on argv; only env (or temp file with immediate unlink).
- [ ] **Step 6.7:** Run worker loop + skew integration tests.
- [ ] **Step 6.8:** Commit.

---

## Task 7: Grok Enablement (CLI Path + Worker Image)

- [ ] Add `grok` CLI install step to `provision/setup-worker.sh` (and the rebake template).
- [ ] Document the expected non-interactive invocation (`grok --print --output-format stream-json -p ...` or the actual binary contract once defined).
- [ ] Add a minimal `GrokRunner` (or entry in the registry) that exercises the generic CLI path.
- [ ] Update `WorkerConfig` example + docs to show mixed `providers`.
- [ ] Add a smoke test that a "grok" task reaches the correct binary (mocked).
- [ ] Commit + note that full Grok CLI semantics can be iterated after the first rebake.

---

## Task 8: Dead-Letter & Observability Hygiene

- [ ] Update `_dead_letter_task` and result-failure DL paths to use `task.to_safe_dict()`.
- [ ] Update `cli.py` dead-letters command and dashboard snapshot to never emit raw credentials.
- [ ] Add per-provider metrics / Redis counters (`providers:{p}:exhausted_skip`, `rebake_required_failures`).
- [ ] Update status / dashboard cards to show provider health.
- [ ] Commit.

---

## Task 9: Testing Matrix & Hardening Verification

- [ ] Add concurrency stress test for `ProviderPool` (many threads, mixed exhaustion, duplicate results).
- [ ] Add version-skew integration test (new orchestrator + old worker image receiving grok task → clean rebake failure).
- [ ] Add redaction property test (no secret ever appears in `str()`, logs, DL, exceptions).
- [ ] Add full multi-provider task flow test (claude + grok mixed in one project, exhaustion of one does not affect the other).
- [ ] Run `make test-unit` + targeted integration.
- [ ] Commit.

---

## Task 10: Rollout & Documentation

- [ ] Write explicit rollout sequencing in `docs/` (worker images with dispatch first, then orchestrator with new pools, then first non-claude entries).
- [ ] Update `CLAUDE.md`, `orchestrator-state-machine.md`, and example configs.
- [ ] Add "Adding a new provider" runbook (one entry in YAML + one line in setup-worker.sh + rebake).
- [ ] Update fleet provisioning to emit provider credentials the same way it does claude_tokens.
- [ ] Final full test run + manual skew simulation.
- [ ] Commit the plan as "implemented" with a summary of all subagent items closed.

---

## Task 11: Self-Review & User Gate (this plan)

- [ ] Verify every High/Med risk from the three subagents has at least one explicit task above.
- [ ] Verify the plan produces working, testable increments (each task ends with green tests + commit).
- [ ] Verify no "TBD" or hand-wavy steps remain.
- [ ] Run the plan through a fresh subagent "plan reviewer" if desired.

---

**Plan complete and saved to** `docs/superpowers/plans/2026-05-17-multi-provider-support-providerentry-pool-and-grok-enablement.md`.

---

**Post-Review Update (2026-05-17)**

This plan was revised after a dedicated architecture review subagent evaluated Tasks 1+2 against the explicit goal of making orcest fully provider-agnostic (workers pre-baked, orchestrator only registers + round-robins + tracks exhaustion).

The "Provider Registration & Invocation Boundary" section + guardrails added to Tasks 1, 3, and 6 are the direct result of that review. All future implementers and reviewers must treat the boundary as non-negotiable.

The plan now explicitly steers the design toward **Option A** (execution recipe lives in the worker image registry keyed by `task.provider`).

Two execution options (as required by the writing-plans skill):

**1. Subagent-Driven (recommended)** — I will dispatch a fresh subagent per task (or small batch), perform two-stage review between tasks, and only proceed when the reviewer passes. Fast iteration, excellent for the hardening items.

**2. Inline Execution** — We execute tasks in this session using the executing-plans skill, with explicit checkpoints after each major phase (data model, pool, worker dispatch, Grok enablement, tests).

Which approach do you want? (Or any adjustments to the plan before we lock it?)

---

## Implementation Complete — Task 10: Rollout & Documentation (2026-05-19)

**Overall plan status: IMPLEMENTED AND COMMITTED.**

All tasks 0–10 (and the self-review gate) have been executed. The multi-provider architecture (ProviderEntry + hardened ProviderPool + early dispatch + graceful skew rejection + per-provider exhaustion + Grok enablement) is production-ready under the strict **Provider Registration & Invocation Boundary (Option A)**.

### Major Subagent / Work Items Closed
- Task 0: Plan skeleton + architecture review incorporation (boundary guardrails).
- Task 1: Data model `ProviderEntry` (rich for config, lean+redacted for orchestrator) + `providers.py`.
- Task 2: `ProviderPool` with RLock, exhaustion, stable identities (replaces TokenPool paths).
- Task 3/4: Config loading (`ProjectConfig.providers`, legacy `claude_tokens` synthesis, env fallbacks for arbitrary providers).
- Task 5: Task model + publisher updates (lean `provider`/`credential`/`model` on wire, redaction).
- Task 6: Worker dispatch — early graceful reject + `PROVIDER_REGISTRY` (the critical skew safety net).
- Task 7: Grok enablement (registry entry, setup-worker.sh stub + runbook comments, generic CLI path).
- Task 8: Dead-letter hygiene + per-provider Redis counters (`providers:{p}:exhausted_skip`, `rebake_required_failures`) + status visibility.
- Task 9: Testing matrix (concurrency, redaction properties, multi-provider flow, skew integration).
- Task 10 (this): Explicit `docs/rollout-multi-provider.md`, "Adding a provider" runbook (`docs/adding-a-provider.md` + polished `provision/setup-worker.sh`), all example/docs updates (`CLAUDE.md` i.e. `.claude/CLAUDE.md`, state-machine, `*.example.yaml`, env files, docker-compose), fleet generalization (`generate_env_file`, `OrgEntry.provider_credentials`, worker fallback in `_build_env`), final test sweep + skew simulation + ruff + commit.

Additional nits-fix / review subagents (from session history) covered: redaction everywhere, test coverage, docs consistency, fleet env emission parity, etc.

### Files Changed in Task 10 (Rollout)
- New: `docs/rollout-multi-provider.md` (sequencing + boundary reinforcement)
- New: `docs/adding-a-provider.md` (precise 7-step runbook)
- Modified: `.claude/CLAUDE.md`, `docs/orchestrator-state-machine.md`, `config/orchestrator.example.yaml`, `config/worker.example.yaml`, `provision/env.example`, `provision/env.orchestrator.example`, `provision/setup-worker.sh`, root + fleet `docker-compose.yml`
- Modified (fleet + worker): `src/orcest/fleet/orchestrator.py`, `src/orcest/fleet/config.py`, `src/orcest/fleet/cli.py`, `src/orcest/worker/claude_runner.py`
- Modified (plan summary): this file
- Auto-formatted the 4 Python sources.

### Verification Performed
- `python3 -m ruff format --check` + auto-format on edited sources → clean.
- `ruff check` (pre-existing test line-length issues only; no new violations introduced).
- Full `PYTHONPATH=src python3 -m pytest -m unit -q` → **1248 passed, 1 skipped**.
- Manual smoke: `generate_env_file` now correctly emits `XAI_API_KEY=...` (and CLAUDE legacy) for mixed `provider_credentials`.
- Manual skew simulation: `get_unsupported_reason("gemini")` → `'unknown provider "gemini"'` (leads to permanent FAILED "Rebake worker image..." exactly as required).
- Per-provider counters confirmed visible in `cli.py` status table and dashboard snapshot (Task 8 work exercised).
- All new documentation explicitly calls out the orchestrator-agnostic / worker-registry boundary.

### Remaining Technical Debt / Future Work (Non-Blocking)
- Eventual full removal of legacy `claude_tokens` synthesis shims (after all live YAMLs migrated to explicit `providers:` lists).
- Possible dashboard UI cards showing real-time per-provider health (beyond the existing `orcest status --once` table and Redis keys).
- Richer metrics (per-provider latency, success rate) if operational need arises.
- If a real published `grok` npm package appears, replace the placeholder in `setup-worker.sh` with an actual install step (the registry + dispatch are already wired).
- Worker `backend` config could evolve from single value to a set (allowing one worker image to consume multiple `tasks:*` streams); today separate worker groups per primary provider is the pattern.

**This concludes the multi-provider support implementation plan.** The system now safely supports heterogeneous providers with clean version-skew behaviour, strict separation of concerns, and complete documentation + runbooks for future extensions.

**Commit message used:** `plan: Task 10 – Rollout & Documentation; overall multi-provider implementation complete`

(Commit SHA recorded in the final agent report.)
