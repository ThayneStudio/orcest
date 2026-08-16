# Multi-Provider Rollout & Migration Guide

**Status:** Part of Task 10 (Rollout & Documentation) for the overall multi-provider implementation plan.

> **Fleet-managed routing constraint:** every published provider must have a
> scheduled `pool.worker_profiles` slot. Each worker remains specialized to one
> provider stream, while the fleet manager maintains the exact ordered mix.
> Fleet commands reject uncovered streams before mutation.

## Critical Boundary (Reinforced)

All rollout steps and documentation adhere to the **Provider Registration & Invocation Boundary (Option A)**:

- **Orchestrator** (and `ProviderPool`, task publishing, exhaustion tracking, CLI status): completely agnostic. Operates only on the lean surface `ProviderEntry(provider, credential, model, ...)` and embeds `provider` + `credential` + `model` + `extras` into `Task`. Never knows binaries, env var names, CLI flags, or parsing rules.
- **Worker images** (pre-baked): own the execution recipes exclusively via the local `PROVIDER_REGISTRY` in `src/orcest/worker/runner.py` (keyed by the opaque `task.provider` string). The matching `ProviderRecipe` supplies `binary` and `env_var` for credential injection (only via child env, never argv). Unknown provider → immediate, permanent `FAILED` (non-transient) with operator message: "rebake worker image to include `<provider>` CLI".
- This separation guarantees safe incremental rollout and version skew handling without secret leaks or stuck tasks.

Old worker images that have never seen the Task 6 dispatch logic will safely reject (or in pre-Task6 images may need the rebake to gain the guard).

## Recommended Rollout Sequencing

Follow this order to minimize risk. Claude-only operation is unaffected until you explicitly add other providers.

1. **Rebake Worker Images (Prerequisites — Do This First)**

   - Merge and deploy the worker changes from Tasks 6/7 (early dispatch in `worker/loop.py` + `runner.py` `PROVIDER_REGISTRY` + graceful reject path + Grok stub in setup).
   - Run `orcest fleet rebake` (or invoke `provision/setup-worker.sh` manually on your template VM + update the active template pointer via `orcest fleet set-current-template`).
   - The new image must contain:
     - The `PROVIDER_REGISTRY` with at least `"claude"` (and optionally `"grok"` entry even if the binary is absent on disk — the `get_unsupported_reason` will trigger clean reject).
     - The generic CLI runner path (in `claude_runner.py`).
   - **Verification on new image:** check CLI versions with `which`/`--version`
     and inspect the registry without connecting to deployment Redis, for example
     `python -c "from orcest.worker.runner import get_provider_recipe; print(get_provider_recipe('claude'))"`.
     Do not start `orcest work` against deployment Redis as a template smoke test;
     it can create a real consumer and claim real work.
   - At this stage you can still only run claude tasks safely. New-provider tasks sent to a freshly-rebaked image that lacks the CLI will produce the documented permanent FAILED.

2. **Deploy the Updated Orchestrator**

   - Roll out the orchestrator binary/code containing Tasks 1–5 + 8 (ProviderEntry, ProviderPool with per-credential exhaustion, config loading with `providers:` + legacy `claude_tokens` synthesis, per-provider counters, redaction everywhere, dead-letter hygiene).
   - The orchestrator may be deployed while some workers are still on old images (for claude tasks only).
   - Legacy `claude_tokens` paths continue to work via synthesis into `ProviderEntry(provider="claude", ...)` — zero breaking change for existing single-claude projects.
   - `orcest status --once` will now surface the new per-provider tables (exhausted_skip, rebake_required_failures).

3. **Configure the Managed Worker Mix**

   - Existing projects using only `claude_tokens` (or synthesized providers) continue to round-robin exactly as before on any worker that has "claude" in its registry.
   - A single project may declare a mixed `providers:` list when the fleet
     schedules a worker profile for every resulting stream, e.g.:

     ```yaml
     providers:
       - provider: claude
         credential: ""          # falls back to CLAUDE_CODE_OAUTH_TOKEN (or env)
         model: claude-3-5-sonnet-20241022
       - provider: grok
         credential: ""          # falls back to XAI_API_KEY (or env)
         model: grok-3-latest
     ```

   - The pool selects across heterogeneous entries; exhaustion of one provider's credential (e.g. grok rate limit) only skips that entry for its cooldown window — other providers remain usable.
   - Configure the matching worker layout:

     ```yaml
     pool:
       size: 4
       vm_id_start: 10000
       worker_profiles:
         - backend: clauder
         - backend: codex
         - backend: grok
     ```

     Slots repeat in order, so this four-worker example has two `clauder`, one
     `codex`, and one `grok` worker. Repetition is worker capacity, not task
     publication weight; task selection remains round-robin across credential
     entries and execution times can differ by provider.

4. **Introduce the First Non-Claude Provider**

   - Update your orchestrator YAML (or the config emitted by fleet tooling) to include the new provider entry (see `config/orchestrator.example.yaml`).
   - Ensure the credential is either:
     - Inline in YAML (discouraged for secrets), or
     - Omitted (`credential: ""` or absent) so that `config.py` `_parse_provider_entry` falls back to the conventional env var (`XAI_API_KEY`, `GROK_API_KEY`, etc.) present in the orchestrator process environment.
   - For fleet-managed orchestrators, every provider name must appear in a
     scheduled `pool.worker_profiles` slot. An uncovered credential entry is
     rejected before fleet mutation.
   - Rebake workers that will handle the new provider (install the CLI in `setup-worker.sh` + one-line registry entry).
   - Test with a dedicated throwaway project or by temporarily adding the provider to an existing project's list.

5. **Skew / Negative-Path Verification (Mandatory Before Production Use of New Provider)**

   - Manually (or via test) publish a task for the new provider while at least one worker in the pool is still on an old image (or a rebaked image lacking the CLI).
   - Expected observable result (visible via `orcest status`, Redis dead-letters, or GitHub comment):
     - `status=FAILED`, `transient=False`
     - `summary` contains the string "rebake worker image to include <provider> CLI" (or the exact message from `get_unsupported_reason`).
     - No raw credential appears anywhere (redaction + dead-letter safe dict).
     - Pending marker is cleared; no retry storm; the task does not become stuck.
   - This proves the graceful-reject contract from Task 6.

6. **Cutover & Steady State**

   - Rebake every worker template so the desired provider CLIs + registry entries are present.
   - Deploy a coordinated mixed layout with
     `orcest fleet deploy --rebuild-template --drain-active`. Profile order,
     runner settings and VMID bounds are part of the layout signature; the
     full drain prevents old workers from being silently reinterpreted.
     VMID range changes are deliberately rejected while a deployed config
     exists. Drain using the old checksummed config, verify every old worker is
     absent, retire the remote deployed config, and only then deploy the new
     range as a first deployment.
   - A general-purpose worker image that has multiple registry entries can in principle consume from any stream it is pointed at (the dispatch inside the loop is provider-agnostic once the task arrives).
   - Monitor:
     - `orcest status --once` (provider health cards + counters)
     - Per-provider exhausted_skip and rebake_required_failures keys under the project's Redis prefix (`providers:<prov>:...`)
   - Remove legacy `claude_tokens` shims from YAML once all configs have migrated to explicit `providers:` lists (future cleanup task).

## Fleet Provisioning Notes (Generalized in Task 10)

Fleet `OrgEntry` now supports (in addition to the legacy `claude_oauth_tokens`):

```yaml
orgs:
  myorg:
    github_token: ...
    claude_oauth_tokens: ["sk-..."]
    provider_credentials:
      grok: ["xai-..."]   # list; first used for the env var in generated .env
```

This example requires a scheduled `grok` worker profile. If the org also has
legacy Claude tokens, the layout needs a `claude` or `clauder` profile as well.
The fleet preflight intentionally rejects uncovered streams.

Provider names are restricted to lowercase letters, digits, and underscores.
This keeps Redis stream names and derived environment-variable names unambiguous;
rename any older custom provider containing uppercase letters, dots, or hyphens
before rollout.

`generate_env_file(...)` (and the caller in `fleet/cli.py`) emits:

- `CLAUDE_CODE_OAUTH_TOKEN` + `CLAUDE_CODE_OAUTH_TOKENS` for claude (exact prior behaviour)
- `XAI_API_KEY=...` (or `GROK_API_KEY`) for grok, and analogous `{UPPER}_API_KEY` for future providers

These land in the orchestrator's Docker `.env` so that `providers:` entries with empty `credential` can fall back at config load time.

Analogous support exists (or is added) for worker cloud-init `.env` files and the `_ENV_WHITELIST` + fallback logic in `claude_runner._build_env` so that a Task carrying `credential=""` for a provider will cause the worker to pick the value from its own `/opt/orcest/.env` (the declared `env_var` from the local registry entry).

## Post-Rollout Cleanup (Future)

- Full removal of `claude_tokens` synthesis shims in `config.py`, `provider_pool.py`, tests, and `loop.py` once all live configs have been migrated to `providers:`.
- Dashboard / UI cards for per-provider health (beyond the current `orcest status` table).
- Richer per-provider metrics (latency, success rate) if desired.

This sequencing + the built-in skew rejection makes it safe to experiment with new providers without ever risking production Claude workloads.

See also:
- `docs/adding-a-provider.md` (step-by-step for extending the registry)
- `config/orchestrator.example.yaml`
- `provision/setup-worker.sh` (the install + verification blocks)
