#!/usr/bin/env bash
# Orcest worker template rebake — non-destructive (blue/green).
#
# Invoked weekly by orcest-rebake-template.timer on the Proxmox host.
# Builds a NEW template at the next free VMID in pool.template_vmid_range,
# then atomically swaps the Redis pointer so new clones come from it. The
# previous template stays alive for any in-flight workers; old templates
# get garbage-collected once their clones churn out.
#
# No idle-gate needed — `orcest fleet rebake` never touches the active
# template or running workers.
#
# Logs to journal under unit orcest-rebake-template.service.
set -euo pipefail

echo "Starting non-destructive template rebake."
orcest fleet rebake

# Sweep templates with no live clones (skips the active pointer).
# --dry-run first so the journal records what GC would do, then commit.
echo "Garbage-collecting orphaned templates."
orcest fleet gc-templates --dry-run
orcest fleet gc-templates

echo "Template rebake complete. Pool manager will pick up the new pointer within ~30s."
