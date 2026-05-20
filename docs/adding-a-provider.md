# Adding a New Provider (Runbook)

**Goal:** Extend Orcest to a new coding agent (e.g. "gemini", "codex", future "grok" real binary) while obeying the **Provider Registration & Invocation Boundary**.

**Never** add execution knowledge (binaries, flags, env var names for invocation) to the orchestrator. The orchestrator only registers names + credentials + models and round-robins them.

## Prerequisites
- You have a working `claude` setup (the reference provider).
- You understand that workers are immutable pre-baked images.
- See `docs/rollout-multi-provider.md` for full rollout order.

## Step-by-Step

1. **Choose the provider name**

   Lowercase string, stable, used as the key in `Task.provider` and the stream name (`tasks:<name>`). Examples: `grok`, `gemini`.

2. **One-line addition to the worker registry (src/orcest/worker/runner.py)**

   ```python
   PROVIDER_REGISTRY: dict[str, ProviderRecipe] = {
       "claude": ProviderRecipe(binary="claude", env_var="CLAUDE_CODE_OAUTH_TOKEN"),
       "grok": ProviderRecipe(binary="grok", env_var="XAI_API_KEY"),
       # NEW:
       "gemini": ProviderRecipe(binary="gemini", env_var="GOOGLE_API_KEY"),
   }
   ```

   `ProviderRecipe` is a tiny dataclass (binary, env_var). This line is the **single source of truth** for dispatch. The orchestrator never imports or reads this table.

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

4. **Runner / parsing (usually zero changes)**

   The generic path in `claude_runner.py` (invoked for any provider) builds:

       <binary> --print --verbose --output-format stream-json \
                 --dangerously-skip-permissions -p "<prompt>"

   and parses stdout for summary + rate_limit_event objects, stderr for exhaustion signals.

   Only if your new provider uses completely different flags or output (not stream-json), you may:
   - Extend the registry recipe with more fields (still worker-only), or
   - Add a thin `<name>_runner.py` selected inside `create_runner` by worker config (not per-task).

   In the common case, reuse the existing path.

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

   For fleet-managed deployments, put the credential(s) under the org in your `fleet.yaml`:

   ```yaml
   orgs:
     myorg:
       github_token: ...
       claude_oauth_tokens: [...]
       provider_credentials:
         gemini: ["AIza..."]
   ```

   `generate_env_file` will emit the appropriate `GOOGLE_API_KEY=...` (or whatever canonical name) into the project's `.env`.

7. **Test, including skew**

   - Add the provider to a test project's config.
   - Publish a task for it.
   - Happy path on a correctly rebaked worker: runs, reports result, exhaustion tracked under `providers:gemini:...`.
   - Skew path (task reaches a worker without the entry or binary): permanent FAILED, summary contains "rebake worker image to include gemini CLI", no secret leak, coordination cleaned up.

   Use `orcest status --once` to see the new per-provider counters.

## What Never Changes on the Orchestrator Side
- No new Python files or conditionals for the new provider.
- No knowledge of its binary or CLI contract leaks into `orchestrator/`, `fleet/`, `shared/providers.py` (except the tiny default env-var guess for convenience in parsing), or task publishing.
- The `Task` wire format stays lean.

## Cleanup / Iteration
- Once the real CLI for the provider is stable, replace any placeholder in setup-worker.sh.
- Update example files and this runbook with the exact command line the generic runner will use.
- If many providers, consider making the worker's `backend` config allow a list (future work); today you typically run separate worker groups per primary stream.

This process has been exercised for the initial "grok" enablement (Task 7) and is the template for all future providers.

Cross-references:
- `provision/setup-worker.sh` (the actual install + verification code + the Grok example comments)
- `src/orcest/worker/runner.py` (the registry)
- `config/orchestrator.example.yaml`
- `docs/rollout-multi-provider.md`
