# Adding a New Provider (Runbook)

**Goal:** Extend Orcest to a new coding agent (for example, Gemini) while
obeying the **Provider Registration & Invocation Boundary**.

**Never** add execution knowledge (binaries, flags, env var names for invocation) to the orchestrator. The orchestrator only registers names + credentials + models and round-robins them.

## Prerequisites
- You have a working `claude` setup (the reference provider).
- You understand that workers are immutable pre-baked images.
- See `docs/rollout-multi-provider.md` for full rollout order.
- The built-in fleet manager supports an ordered list of dedicated worker
  profiles. Add enough `pool.worker_profiles` entries to cover every provider
  stream a fleet-managed project can publish; uncovered streams are rejected by
  fleet preflight.

## Step-by-Step

1. **Choose the provider name**

   Lowercase string, stable, used as the key in `Task.provider` and the stream name (`tasks:<name>`). Examples: `grok`, `gemini`.

2. **Implement the provider runner and register it worker-side**

   ```python
   # src/orcest/worker/__init__.py (after importing GeminiRunner)
   PROVIDER_REGISTRY["gemini"] = ProviderRecipe(
       binary="gemini",
       env_var="GOOGLE_API_KEY",
       runner_cls=GeminiRunner,
   )
   ```

   Implement `GeminiRunner` as a `_BaseCliRunner` subclass in
   `src/orcest/worker/gemini_runner.py`, including its argv, output parsing,
   exhaustion/overload classification, auth-prompt detection, and credential
   preparation. The registry entry is the worker-image source of truth for
   dispatch. The orchestrator never imports or reads this table.

3. **Add the CLI install step in provision/setup-worker.sh**

   Mirror the existing Claude / Grok blocks:

   ```bash
   if ! command -v gemini &>/dev/null; then
       echo "Installing Gemini CLI..."
       # your curl/npm/dpkg/whatever one-liner
       ...
   fi
   ```

   Also add `gemini` to the final verification `for cmd in ...` list (or keep it optional like the current grok stub and rely on the early-reject path).

   Update the "Grok CLI not present" style message if you want a different wording.

4. **Runner / parsing**

   Provider CLIs have distinct flags and event schemas, so each supported
   provider owns a small runner subclass. Reuse `_BaseCliRunner` for process,
   timeout, retry, credential isolation, and streaming mechanics; keep only the
   provider-specific argv/parsing/auth hooks in the new class. Add fixtures for
   every event type and failure classification supported by the pinned CLI.

5. **Rebake the worker image**

   ```bash
   # on the template VM or via fleet
   sudo provision/setup-worker.sh
   # then create new template + update active pointer (orcest fleet rebake)
   ```

   After rebake, any worker cloned from the new template has the binary + the registry entry.

6. **Declare the provider in the orchestrator (declarative only)**

   In `orchestrator.yaml` (top-level or inside a `projects[].providers`):

   ```yaml
   providers:
     - provider: gemini
       credential: ""                 # or a literal value; "" => env fallback
       model: gemini-1.5-pro
       # cli_binary / env_var / extras are ignored by orchestrator (worker-owned)
   ```

   The credential can come from the YAML or from env vars (see `_PROVIDER_ENV_CANDIDATES` + generic fallbacks in `shared/config.py`): `GEMINI_TOKEN`, `GEMINI_API_KEY`, `GOOGLE_API_KEY`, etc.

   For fleet-managed deployments, put the credential(s) under the org in your
   `fleet.yaml` only when the managed pool consumes the same provider stream:

   ```yaml
   orgs:
     myorg:
       github_token: ...
       claude_oauth_tokens: [...]
       provider_credentials:
         gemini: ["AIza..."]
   ```

   Fleet-provisioned multi-token round-robin is currently Claude-only. For
   non-Claude `provider_credentials`, fleet emits only the first configured
   credential as the provider's singular env var. See
   `docs/rollout-multi-provider.md#fleet-credential-multiplicity` for the
   current limitation and the worker/config + fleet changes needed for parity.

   Fleet credential generation emits the generic provider variable
   `GEMINI_API_KEY=...` into the project's `.env`. Docker Compose only forwards
   variables explicitly named in its `environment:` list, so add
   `GEMINI_API_KEY` to both the repository-root `docker-compose.yml` and
   `src/orcest/fleet/deploy/docker-compose.yml`. The worker registry can still
   map the credential carried in each task to the CLI's required
   `GOOGLE_API_KEY` at execution time.

   Add a dedicated `gemini` entry to `pool.worker_profiles`. Fleet preflight
   rejects the project until every provider it can publish has at least one
   scheduled slot.

7. **Test, including skew**

   - Add the provider to a test project's config and worker profile layout.
   - Publish a task for it.
   - Happy path on a correctly rebaked worker: runs, reports result, exhaustion tracked under `providers:gemini:...`.
   - Skew path (task reaches a worker without the entry or binary): permanent FAILED, summary contains "rebake worker image to include gemini CLI", no secret leak, coordination cleaned up.

   Use `orcest status --once` to see the new per-provider counters.

## What Never Changes on the Orchestrator Side
- No new Python files or conditionals for the new provider.
- No knowledge of its binary or CLI contract leaks into `orchestrator/`,
  `fleet/`, `shared/providers.py` (except the generic env-var guess used to load
  fleet credentials), or task publishing. A fleet-managed provider does require
  its generic `<PROVIDER>_API_KEY` passthrough to be listed in both Compose
  files.
- The `Task` wire format stays lean.

## Cleanup / Iteration
- Pin the real CLI version and its installer/package integrity metadata in
  `setup-worker.sh` and the cloud-init template.
- Update example files and this runbook with the exact command line and output
  contract the provider-specific runner will use.
- Keep each worker dedicated to one backend stream; scale capacity by repeating
  that backend in `pool.worker_profiles`.

This process is exercised by the built-in Claude/Clauder, Codex, and Grok
runners and is the template for future providers.

Cross-references:
- `provision/setup-worker.sh` (the actual install + verification code + the Grok example comments)
- `src/orcest/worker/runner.py` (the registry)
- `config/orchestrator.example.yaml`
- `docs/rollout-multi-provider.md`
