# Orcest Audit Remediation — Workflow Plan

> **Execution model:** This plan is executed by **two `Workflow` runs** (multi-agent orchestration), not by hand. Workflow A produces exact TDD specs (read-only, parallel); after a human review gate, Workflow B implements + adversarially verifies each theme in an isolated git worktree and leaves a branch per theme. PR creation is done by the main session after review (outward-facing action). Tasks use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the 16 Critical/High/Medium findings from the 2026-06-10 audit, packaged as one reviewable PR per subsystem theme, each fix written test-first and independently verified.

**Architecture:** Findings are grouped into 7 themes by *file ownership* so parallel agents never edit the same function. Each theme is implemented in its own git worktree (branch `fix/<theme>`), runs `make test-unit` + `make lint` (parallel-safe, fakeredis), and is adversarially reviewed before its branch is finalized. Integration tests (`make test`, real Redis) run serially after, or via per-PR CI.

**Tech stack:** Python 3.12, pytest (`unit`/`integration`/`stress` markers, fakeredis for unit), ruff, Click/Rich, Redis streams, Docker Compose, Proxmox, TypeScript dashboard.

---

## Execution Model

### Two workflows, one gate

```
Workflow A (read-only, 7 agents, fully parallel)
  └─ Spec phase: one agent per theme reads the cited code, CONFIRMS each bug
     still exists at current line numbers, and emits a TDD spec per finding:
     { finding_id, files, current_snippet, proposed_change, test_file,
       test_name, red_expectation, regressions_to_watch }
        │
        ▼
  [HUMAN GATE]  ← I review the specs with you; adjust scope/approach.
                  Security-critical changes (Redis auth, VM-destroy, Proxmox
                  TLS, exhaustion-keying semantics) get explicit sign-off here.
        │
        ▼
Workflow B (per-theme, worktree-isolated, pipelined)
  pipeline over 7 themes:
    stage 1 — implement: agent in isolation:'worktree', TDD per spec
              (write failing tests → run to confirm RED → implement → GREEN),
              `make test-unit` + `make lint` green, commit to `fix/<theme>`.
    stage 2 — adversarial verify: 2 independent reviewers per theme try to
              REFUTE: "does the new test actually fail without the fix?",
              "what regression does this introduce?", "does it match spec?".
              Default-to-rejected on uncertainty. One bounded repair iteration.
        │
        ▼
  [HUMAN GATE]  ← I show you the 7 diffs + verifier verdicts; on your OK I
                  push branches and open PRs (criticals first). Pushing/PRs
                  are outward-facing — never done inside the workflow.
```

Why two workflows instead of one: the fixes touch a live security-critical system (Redis auth, VM destruction, Proxmox API). Re-verifying the exact patch *before* any code is written, with a human gate, is worth one extra round-trip. It also lets the Spec phase be fully parallel and read-only (zero conflict risk).

### Parallel-safety rules (derived from the repo)

- **Unit tests are parallel-safe; integration tests are not.** `make test-unit` (`pytest -m unit`) uses fakeredis with Lua support — no external process. `make test`/`redis-up` bind Redis on `localhost:6379`, so two worktrees running it at once collide. **Workflow B stage 1 runs `make test-unit` + `make lint` only.** Full `make test` (integration + stress) runs **serially** in a post-step per branch, or is delegated to per-PR CI.
- **Worktree isolation** (`isolation: 'worktree'`) gives each theme its own checkout, so parallel edits to shared files (`shared/config.py`, `orchestrator/loop.py`, `fleet/cloud_init.py`, `shared/redis_client.py`) never collide at runtime.
- New unit tests must live **outside** `tests/integration/` and `tests/stress/` (conftest auto-marks by path: those dirs → integration/stress, everything else → `unit`).

### Shared-file map + merge order

Shared files are only edited in **different functions** per theme, so git auto-merges; conflicts (if any) are resolved by rebasing later PRs. Recommended merge order = severity:

| File | Themes that touch it | Regions (disjoint) |
|------|----------------------|--------------------|
| `shared/config.py` | 1, 5 | `build_redis_config` (1) vs `_with_legacy_claude_synthesis`/`task_key_prefix` (5) |
| `shared/redis_client.py` | 1, 2 | `xtrim_minid` wiring (1) vs `delconsumer`/`xautoclaim` (2) |
| `orchestrator/loop.py` | 1, 4 | per-cycle trim call (1) vs cred-less/starvation/restart-dup (4) |
| `fleet/cloud_init.py` | 1, 7 | worker Redis-password env (1) vs installer-SHA/systemd/dead-code (7) |
| `fleet/config.py` | 2, 6 | `max_task_duration` (2) vs `verify_ssl` (6) |

**Merge order:** PR1 → PR2 → PR3 → PR4 → PR5 → PR6 → PR7. Rebase each on the prior before merge. The two Criticals (PR1, PR2) merge first.

---

## Themes (one PR each)

Each theme lists its findings (audit ID · severity), the files it owns, the fix direction, and the test home + command. **Exact patch text and exact current line numbers are produced by Workflow A's Spec phase** (the audit's line citations are from 2026-06-10 and must be re-confirmed). Concrete code is sketched where the fix is fully determined.

---

### Theme 1 — `fix/redis-security`  (CRITICAL)

**Findings:** C1 (Redis open + no auth, plaintext creds in streams) · M1-conc (task streams never trimmed → credentials retained forever).

**Files:**
- Modify: `src/orcest/fleet/deploy/docker-compose.redis.yml` (add `--requirepass ${ORCEST_REDIS_PASSWORD}`)
- Modify: `src/orcest/fleet/deploy/docker-compose.yml`, `src/orcest/fleet/deploy/docker-compose.pool.yml` (pass `ORCEST_REDIS_PASSWORD` into orchestrator + pool-manager env)
- Modify: `src/orcest/fleet/orchestrator.py` (generate `ORCEST_REDIS_PASSWORD` into the per-project `.env`, 0600)
- Modify: `src/orcest/fleet/cloud_init.py` (inject `ORCEST_REDIS_PASSWORD` into worker env — its own region, not theme 7's)
- Modify: `src/orcest/shared/config.py` (`build_redis_config`: when deploying, treat empty password as a hard error rather than silent none)
- Modify: `src/orcest/shared/redis_client.py` (call existing `xtrim_minid` — currently zero callers)
- Modify: `src/orcest/orchestrator/loop.py` (once per poll cycle, trim each task stream up to the consumer group's lowest un-acked id)
- Tests: `tests/shared/test_config.py`, `tests/fleet/test_orchestrator.py`, `tests/fleet/test_cloud_init.py`, `tests/shared/test_redis_client.py`, `tests/orchestrator/test_loop.py`

**Fix direction:**
- Redis: require a password everywhere (`requirepass` on the server; `ORCEST_REDIS_PASSWORD` injected into orchestrator, pool-manager, and worker environments and the generated `.env`). Keep the `0.0.0.0` bind (multi-VM architecture needs it) but it is no longer unauthenticated. Optionally document a TLS follow-up.
- Trimming: after a task entry is ACKed by a worker, it is dead weight that still holds the credential. Add a per-cycle `XTRIM MINID` up to the group's lowest still-pending id so ACKed (delivered + processed) entries — and the credentials in them — are reclaimed. Must never trim above the lowest un-acked id (would drop undelivered work).

**TDD targets (unit):**
- `test_config.py`: `build_redis_config` with deploy flag + empty password → raises; with password set → `RedisConfig.password` populated.
- `test_orchestrator.py`: generated `.env` contains `ORCEST_REDIS_PASSWORD`; file mode 0600.
- `test_cloud_init.py`: rendered worker user-data/env contains the Redis password variable.
- `test_redis_client.py`: `xtrim_minid` removes ACKed entries below min-pending, leaves pending entries.
- `test_loop.py`: a completed+acked task's stream entry is trimmed on the next cycle; an in-flight (un-acked) entry is retained.

**Non-unit verification:** `docker compose -f src/orcest/fleet/deploy/docker-compose.redis.yml config` parses; manual review that no compose path leaves Redis password-less.

**Run:** `make test-unit && make lint`

---

### Theme 2 — `fix/pool-safety`  (CRITICAL + HIGH + MEDIUM)

**Findings:** C2 (VM-destroy has no VMID range guard) · H2-conc (pool reaps VM mid-task with no Redis coordination; default `max_task_duration` 3600s < runner `timeout` 5400s) · H3-conc (no `XAUTOCLAIM`/`DELCONSUMER` anywhere → dead-consumer PEL entries only recovered by name reuse) · M2-conc (`_fill_pool` excess-drain can destroy a VM that just claimed a task).

**Files:**
- Modify: `src/orcest/fleet/pool_manager.py` (`_destroy_vm`/`_check_done_workers` range guard; `_health_check` reap → DELCONSUMER + clear pending marker + publish FAILED result; orphan-PEL sweeper via `XAUTOCLAIM`; `_fill_pool` re-check active before draining)
- Modify: `src/orcest/fleet/config.py` (`max_task_duration` default ≥ runner timeout, or derive from it)
- Modify: `src/orcest/shared/redis_client.py` (add `delconsumer`/`xautoclaim` helpers — own region, not theme 1's `xtrim`)
- Modify (read/reuse): `src/orcest/shared/coordination.py` (reuse `clear_pending_task_if_matches`), `src/orcest/shared/models.py` (build a FAILED `TaskResult`)
- Tests: `tests/fleet/test_pool_manager.py`, `tests/fleet/test_config.py`, `tests/shared/test_redis_client.py`

**Fix direction:**
- **C2 (range guard) — fully determined.** Before any destroy, assert the VMID is inside the pool range:
  ```python
  if not (self._cfg.vm_id_start <= vm_id <= self._cfg.vm_id_end):
      logger.error("Refusing to destroy out-of-range VM %d (pool range %d-%d)",
                   vm_id, self._cfg.vm_id_start, self._cfg.vm_id_end)
      self._redis.delete(key)   # drop the poisoned done-key
      return
  ```
  Apply at the top of `_destroy_vm` (covers the done-key path + every other caller).
- **H2 (reap coordination):** when `_health_check` force-destroys an over-duration VM, it must also: `XGROUP DELCONSUMER` the worker's consumer, clear the PR/issue pending marker, and publish a transient-FAILED `TaskResult` so the orchestrator re-enqueues instead of waiting out the ~4.6h marker TTL. Raise default `max_task_duration` above the runner `timeout` (or compute `timeout + grace`) so healthy long tasks aren't killed.
- **H3 (sweeper):** add a periodic `XAUTOCLAIM`/`DELCONSUMER` janitor for PEL entries whose consumer no longer maps to a live VM, so stranded tasks recover even when the VMID isn't reused.
- **M2 (fill-pool race):** re-check active-consumer state immediately before draining an excess idle VM; never drain a VM that has a pending entry.

**TDD targets (unit, fakeredis):**
- `test_pool_manager.py`: out-of-range done-key → no destroy call, key deleted, error logged; reap path calls DELCONSUMER + clears marker + publishes FAILED; `_fill_pool` skips a VM that gained a pending entry.
- `test_config.py`: `max_task_duration >= runner timeout` invariant holds for defaults.
- `test_redis_client.py`: `xautoclaim`/`delconsumer` helpers behave on a seeded PEL.

**Run:** `make test-unit && make lint` (+ `tests/integration/test_task_flow.py` in the serial integration pass).

---

### Theme 3 — `fix/worker-lock`  (HIGH + MEDIUM)

**Findings:** H1-conc (heartbeat thread dies silently when `lock.refresh()` raises on a Redis blip → `_on_lock_lost` never fires → worker runs unlocked, enabling two agents on one branch) · M4-conc (duplicate result on crash between publish and XACK; issue results have no staleness guard).

**Files:**
- Modify: `src/orcest/worker/heartbeat.py` (`_run`: wrap `refresh()` so a raise is treated like a failed refresh)
- Modify: `src/orcest/worker/loop.py` (honor `lock_lost` when downgrading a result to STALE; add issue-result staleness/dedup mirroring the PR path)
- Tests: `tests/worker/test_heartbeat.py`, `tests/worker/test_loop.py`

**Fix direction — H1 fully determined:**
```python
try:
    refreshed = self.lock.refresh()
except Exception:
    if self.logger:
        self.logger.warning(f"Heartbeat: refresh raised for {self.lock.key}; treating as lost")
    refreshed = False
```
(then the existing `if not refreshed:` path fires `_on_lock_lost` + sets the stop event). M4: give issue results the same `original_entry_id` dedup / head-state staleness check the PR path already has, so a replayed FAILED can't clear attempts on an already-completed issue.

**TDD targets (unit):**
- `test_heartbeat.py`: a lock whose `refresh()` raises → `_on_lock_lost` called exactly once and `_stop_event` set (today: thread dies, neither happens).
- `test_loop.py`: replayed issue result for a completed issue is treated as stale (no attempt-clear, no re-trigger).

**Run:** `make test-unit && make lint`

---

### Theme 4 — `fix/orchestrator-correctness`  (HIGH + MEDIUM)  ← largest; may split 4a/4b

**Findings:** H1-logic (issue discovery capped at `--limit 100`, no pagination → oldest ready issues never picked up) · H2-logic (`get_issue_state` returns "missing" for PR-number blockers → `after #PR merges` never defers) · M1-sec (issue title interpolated into a `gh pr create` shell command in the prompt → injection via crafted title) · M3-logic (issue-task publish failure resets the whole attempt budget instead of rolling back one) · M4-logic (project with no providers/tokens silently publishes credential-less tasks) · M5-logic (cross-project issue starvation via shared issue-stream gating) · M6-logic (`incompatible` dependency pattern matched before CODE → mypy errors misclassified) · M5-conc (orchestrator duplicates comments/labels on restart) · M6-conc (crash between `set_pending_task` and `xadd` strands an issue for the marker TTL).

**Files:**
- Modify: `src/orcest/orchestrator/gh.py` (`list_labeled_issues` → cursor pagination like `list_open_prs`; `get_issue_state` → fall back to `gh pr view` for PR numbers)
- Modify: `src/orcest/orchestrator/issue_deps.py` (treat an open PR blocker as blocking)
- Modify: `src/orcest/orchestrator/ci_triage.py` (match CODE before DEPENDENCY, or tighten the bare `incompatible`/`timeout` patterns)
- Modify: `src/orcest/orchestrator/task_publisher.py` (don't template raw title into a shell snippet; roll back one issue attempt on publish failure; M6-conc ordering)
- Modify: `src/orcest/orchestrator/loop.py` (cred-less task guard; per-project issue fairness; comment/label idempotency keys — its own regions, not theme 1's trim call)
- Tests: `tests/orchestrator/test_gh.py`, `test_issue_deps.py`, `test_ci_triage.py`, `test_task_publisher.py`, `test_loop.py`

**Fix direction highlights:**
- **M1-sec:** the prompt currently emits `` gh pr create --title "{issue_title}" ``. Stop embedding untrusted text in a runnable shell command — instruct the agent to set the title from the issue body it already has, or strip shell metacharacters. (Confirmed at `task_publisher.py:922`.)
- **H1-logic:** mirror the existing `_MAX_PAGES` cursor loop used by `list_open_prs`/`get_unresolved_review_threads`.
- **H2-logic:** when `gh issue view N` fails to resolve, try `gh pr view N`; an open PR blocker should defer (not be "missing").
- **M5-logic** (design-sensitive): per-project gating on a shared stream lets one busy project starve others. Options: per-project issue streams, or fairness accounting. Flag for design sign-off at the gate.

**TDD targets (unit):** pagination returns >100 issues; PR-number blocker defers; malicious title is not placed in a shell-executable position; one-attempt rollback on publish failure; cred-less config raises/skips with a clear error instead of publishing; mypy "Incompatible types" log classifies as CODE.

**Run:** `make test-unit && make lint`

**Split option:** 4a `github-interaction` (gh.py, issue_deps.py, ci_triage.py) + 4b `task-publishing-correctness` (task_publisher.py, loop.py) if the single diff is too large to review.

---

### Theme 5 — `fix/provider-config`  (HIGH + MEDIUM)

**Findings:** H3-logic (exhaustion keyed per (provider, model, credential) but Claude limits are per-account; legacy `claude_tokens` synthesis can double-register the same account) · M2-logic (`task_key_prefix: null` in YAML → literal string `"None"` → tasks published to an unconsumed stream).

**Files:**
- Modify: `src/orcest/shared/providers.py` (`identity()` — separate the *exhaustion key* (account = credential) from the *selection identity*)
- Modify: `src/orcest/orchestrator/provider_pool.py` (cooldowns keyed by account, not per-model)
- Modify: `src/orcest/shared/config.py` (`_with_legacy_claude_synthesis` dedups by credential; `task_key_prefix` uses the existing `_safe_str` so `null` → fallback to `redis_config.key_prefix`)
- Tests: `tests/shared/test_providers.py`, `tests/orchestrator/test_provider_pool.py`, `tests/shared/test_config.py`

**Fix direction:** an account that is rate-limited should be benched regardless of which model entry triggered it — exhaustion cooldown key = credential hash (per account), while round-robin selection can still distinguish models. Synthesis must not append a duplicate entry for a credential already present. `task_key_prefix` is a one-liner: route it through `_safe_str` (which already rejects YAML null) like the other config fields.

**TDD targets (unit):** two entries same credential/different model → benching one benches the account; synthesis given an already-present credential adds nothing; `task_key_prefix: null` → resolves to the Redis key prefix, never `"None"`.

**Run:** `make test-unit && make lint`

---

### Theme 6 — `fix/template-proxmox-safety`  (HIGH + MEDIUM)

**Findings:** H1-infra (half-baked template can become the active pointer — bake proceeds when `cloud-init status != done`) · H2-infra (Proxmox API `verify_ssl=False` hardcoded, no opt-in) · M3-conc (`gc-templates` races `rebake` and can destroy the fresh/active template) · M5-infra (fleet image download skips checksum verification).

**Files:**
- Modify: `src/orcest/fleet/cli.py` (`_wait_for_cloud_init`: fail the bake when status ≠ `done`; pre-pointer-swap smoke check of required provider binaries; `gc_templates` re-reads/locks the active pointer before each destroy)
- Modify: `src/orcest/fleet/proxmox_api.py` (`verify_ssl` honored from config; `download_image` passes checksum + algorithm)
- Modify: `src/orcest/fleet/config.py` (`verify_ssl` flag — own region, not theme 2's `max_task_duration`; expected image checksum)
- Tests: `tests/fleet/test_cli.py`, `tests/fleet/test_proxmox_api.py`, `tests/fleet/test_config.py`

**Fix direction (design-sensitive — sign off at gate):** template bake must hard-fail (not warn) if cloud-init didn't finish, and ideally smoke-test that each provider CLI is on `$PATH` before flipping `orcest:pool:current_template_vmid`. Proxmox TLS: add a config flag; decide default (keep `False` for self-signed labs but make it explicit + allow CA pinning). `gc-templates`: take the rebake lock or re-read the pointer immediately before each destroy.

**TDD targets (unit):** cloud-init not-done → bake raises, pointer not swapped; `verify_ssl` flag threads to the proxmoxer client; `download_image` includes checksum args; gc refuses to destroy the current pointer even if it changed mid-run.

**Run:** `make test-unit && make lint`

---

### Theme 7 — `fix/worker-image-deploy-hardening`  (MEDIUM)

**Findings:** M2-sec (dashboard auth fails open when `DASHBOARD_TOKEN` unset; port published on all interfaces) · M1-infra (`requirements.lock` stale — missing `proxmoxer`/`requests`; Dockerfiles install unpinned) · M2-infra (`curl|bash` installers with empty Grok SHA gate) · M3-infra (systemd unit drift: static unit has old `StartLimitBurst=5` and stronger hardening than the live cloud-init unit) · M4-infra (dead `render_worker_userdata` still embeds long-lived secrets).

**Files:**
- Modify: `dashboard/server/index.ts` (fail **closed** when `DASHBOARD_TOKEN` unset), `docker-compose.dashboard.yml` (bind `127.0.0.1`)
- Regenerate: `requirements.lock` (via `make lock`); modify `Dockerfile` + `src/orcest/fleet/deploy/Dockerfile` to install from the lock
- Modify: `src/orcest/fleet/cloud_init.py` (populate/verify Grok installer SHA; align systemd `StartLimit*` + hardening with the static unit; **delete** dead `render_worker_userdata` — own regions, not theme 1's Redis-env)
- Modify: `provision/setup-worker.sh` (Grok installer SHA), `provision/systemd/orcest-worker.service` (StartLimit + hardening parity)
- Tests: `tests/fleet/test_cloud_init.py` (remove/adjust dead-fn tests; assert systemd unit fields), `tests/test_dashboard.py`

**Fix direction:** dashboard returns 401 when no token is configured (today it returns `true` = open). Reconcile the two systemd unit sources to one hardened definition with the `StartLimitBurst=10`/`StartLimitIntervalSec=300` fix. Pin the lock and build from it. Remove the dead secret-embedding cloud-init function so it can't be reintroduced.

**TDD targets:** `make lock` produces a lock containing `proxmoxer`+`requests`; cloud-init render asserts no credentials embedded + correct StartLimit; dashboard returns 401 with no token. (Dashboard TS side verified by its own test runner / review.)

**Run:** `make test-unit && make lint` (+ `make lock`, `make build-dashboard` as build checks).

---

## Workflow scripts (sketch — finalized at launch)

**Workflow A — spec (read-only, parallel):**
```js
export const meta = {
  name: 'orcest-audit-spec',
  description: 'Read-only: produce TDD specs for each audit-remediation theme',
  phases: [{ title: 'Spec' }],
}
const THEMES = [ /* 7 theme descriptors: id, findings[], files[], test_files[] */ ]
const SPEC_SCHEMA = { /* finding_id, files, current_snippet, proposed_change,
                        test_file, test_name, red_expectation, regressions */ }
const specs = await parallel(THEMES.map(t => () =>
  agent(specPrompt(t), { label: `spec:${t.id}`, phase: 'Spec',
                         agentType: 'Explore', schema: SPEC_SCHEMA })))
return specs.filter(Boolean)
```

**Workflow B — implement + adversarially verify (per-theme, worktree, pipelined):**
```js
export const meta = {
  name: 'orcest-audit-implement',
  description: 'Implement + adversarially verify each remediation theme in a worktree',
  phases: [{ title: 'Implement' }, { title: 'Verify' }],
}
const THEMES = args  // approved specs from Workflow A
const results = await pipeline(
  THEMES,
  t => agent(implementPrompt(t), { label: `impl:${t.id}`, phase: 'Implement',
        isolation: 'worktree', schema: IMPL_SCHEMA }),     // TDD; make test-unit + lint; commit fix/<id>
  (impl, t) => parallel([0,1].map(i => () =>
        agent(verifyPrompt(t, impl, i), { label: `verify:${t.id}:${i}`, phase: 'Verify',
          schema: VERDICT_SCHEMA })))                       // 2 skeptics; refute the fix
      .then(vs => ({ ...impl, verdicts: vs.filter(Boolean) }))
)
return results.filter(Boolean)
// I then run the serial `make test` integration pass + open PRs after your OK.
```

Scale: Workflow A ≈ 7 agents; Workflow B ≈ 7 impl + 14 verify ≈ 21 agents. Total ~28.

---

## Design-sensitive items (require sign-off at the spec gate)

These change behavior/semantics, not just fix a clear bug — I will not let the workflow pick a direction unilaterally:
1. **Provider exhaustion granularity** (Theme 5): move cooldown key to account (credential). Confirms intended semantics.
2. **Cross-project issue fairness** (Theme 4, M5-logic): per-project streams vs fairness accounting — a structural choice.
3. **Proxmox TLS default** (Theme 6): keep `verify_ssl=False` default for self-signed labs vs require explicit opt-out.
4. **Redis bind** (Theme 1): keep `0.0.0.0` + AUTH (chosen) vs `127.0.0.1` + tunnel.
5. **Template smoke-check strictness** (Theme 6): warn vs hard-fail on incomplete cloud-init.

## Explicitly out of scope (Low findings, per scope decision)

TokenPool deletion, needs-human "this PR" wording for issues, duplicate-check-name log collapse, fence-stripping edge cases (`~~~`, unclosed fences), self-referencing blocker guard, `gh` env minimization, clone-time token-on-argv window, shutdown-latency on rate-limit sleeps. Track separately if desired.

---

## Self-review

- **Spec coverage:** all 16 Critical/High/Medium findings map to exactly one theme (C1,M1c→T1; C2,H2c,H3c,M2c→T2; H1c,M4c→T3; H1l,H2l,M1s,M3l,M4l,M5l,M6l,M5c,M6c→T4; H3l,M2l→T5; H1i,H2i,M3c,M5i→T6; M2s,M1i,M2i,M3i,M4i→T7). ✓
- **No silent file collisions:** shared files enumerated with disjoint regions + merge order. ✓
- **Test homes verified to exist** for every theme (real files listed from `tests/`). ✓
- **Parallel-safety:** unit-only in parallel; integration serial — grounded in the Makefile. ✓
- **Placeholders:** exact patch text is intentionally deferred to Workflow A (line numbers must be re-confirmed); fully-determined fixes (C2 guard, H1-conc try/except, task_key_prefix) include concrete code. ✓
