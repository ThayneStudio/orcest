#!/usr/bin/env bash
# Orcest worker template rebake — idle-gated.
#
# Invoked weekly by orcest-rebake-template.timer on the Proxmox host.
# Defers if any worker is currently processing a task; rebakes the template
# (~10 min) and restarts the warm pool if idle.
#
# Logs to journal under unit orcest-rebake-template.service.
set -euo pipefail

ORCHESTRATOR_HOST="${ORCHESTRATOR_HOST:-10.20.1.129}"
ORCHESTRATOR_USER="${ORCHESTRATOR_USER:-orcest}"
REDIS_CONTAINER="${REDIS_CONTAINER:-orcest-redis-redis-1}"

active_count() {
    ssh -o StrictHostKeyChecking=no \
        "${ORCHESTRATOR_USER}@${ORCHESTRATOR_HOST}" \
        "sudo docker exec ${REDIS_CONTAINER} redis-cli HLEN orcest:pool:active" \
        2>/dev/null | tr -d '[:space:]'
}

active="$(active_count || echo "?")"
if [[ "${active}" != "0" ]]; then
    echo "Pool not idle (active=${active}); deferring rebake to next scheduled run."
    exit 0
fi

echo "Pool idle. Starting template rebake."
orcest fleet stop
orcest fleet create-template
orcest fleet start
echo "Template rebake complete. Pool manager will re-clone workers."
