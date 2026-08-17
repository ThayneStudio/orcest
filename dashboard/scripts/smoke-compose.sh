#!/usr/bin/env sh
set -eu

project="orcest-dashboard-smoke-$$"
redis_name="${project}-redis"
network_name="${DASHBOARD_SMOKE_NETWORK:-${project}-network}"
smoke_image="${DASHBOARD_SMOKE_IMAGE:-orcest-dashboard-smoke:${project}}"
token="${DASHBOARD_SMOKE_TOKEN:-dashboard-smoke-token}"
redis_password="${DASHBOARD_SMOKE_REDIS_PASSWORD:-dashboard-smoke-redis-password}"
DASHBOARD_NODE_VERSION=${DASHBOARD_NODE_VERSION:-20.18.3}
node_version="$DASHBOARD_NODE_VERSION"
node_image="${DASHBOARD_SMOKE_NODE_IMAGE:-node:${DASHBOARD_NODE_VERSION:-20.18.3}-slim}"
repo_root="$(pwd -P)"
host_port="${DASHBOARD_SMOKE_HOST_PORT:-$(python3 - <<'PY'
import socket

with socket.socket() as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
)}"
env_file="$(mktemp)"
compose_file="$(mktemp)"
compose_state_file="$(mktemp)"
network_created=0

cleanup() {
  docker compose --env-file "$env_file" -p "$project" \
    -f "$compose_file" down --remove-orphans --rmi local >/dev/null 2>&1 || true
  docker rm -f "$redis_name" >/dev/null 2>&1 || true
  docker image rm "$smoke_image" >/dev/null 2>&1 || true
  if [ "$network_created" = "1" ]; then
    docker network rm "$network_name" >/dev/null 2>&1 || true
  fi
  rm -f "$env_file" "$compose_file" "$compose_state_file"
}

trap cleanup EXIT INT TERM

redis_cli() {
  docker exec "$redis_name" redis-cli --no-auth-warning -a "$redis_password" "$@"
}

wait_for_redis() {
  attempt=0
  until redis_cli PING >/dev/null 2>&1; do
    attempt=$((attempt + 1))
    if [ "$attempt" -ge 60 ]; then
      echo "Smoke Redis did not become ready" >&2
      return 1
    fi
    sleep 0.2
  done
}

seed_dashboard_smoke_data() {
  redis_cli HSET tasks:metadata purpose dashboard-smoke >/dev/null
  redis_cli XADD tasks:claude 1-0 \
    id smoke-task \
    type smoke \
    repo owner/repo \
    resource_type pr \
    resource_id 42 \
    created_at 2026-07-07T00:00:00Z \
    key_prefix smoke >/dev/null
}

verify_seeded_snapshot_contract() {
  container="$($compose ps -q dashboard 2>/dev/null | sed -n '1p')"
  if [ -z "$container" ]; then
    echo "Dashboard smoke snapshot check could not find the dashboard container" >&2
    return 1
  fi

  # Name-only `-e DASHBOARD_TOKEN` plus a command-scoped assignment: the token
  # reaches the container through the Docker client's environment instead of the
  # host `docker` argv, which any local user can read via `ps aux`.
  if ! DASHBOARD_TOKEN="$token" docker run --rm -i --network "container:$container" \
    -e DASHBOARD_TOKEN \
    "$node_image" node --input-type=module <<'NODE'
const response = await fetch("http://127.0.0.1:8080/api/snapshot", {
  headers: { Authorization: `Bearer ${process.env.DASHBOARD_TOKEN || ""}` },
});
const body = await response.json();
if (!response.ok) {
  throw new Error(`/api/snapshot returned ${response.status}: ${JSON.stringify(body)}`);
}

const snapshot = body.snapshot;
if (!snapshot || snapshot.redis_ok !== true) {
  throw new Error("dashboard smoke snapshot did not report Redis OK");
}
const degraded = Array.isArray(snapshot.degraded_sections)
  ? snapshot.degraded_sections
  : [];
const queueSections = new Set(["queue depths", "consumer groups", "queued tasks"]);
const queueDegraded = degraded.filter((section) => queueSections.has(section));
if (queueDegraded.length > 0) {
  throw new Error(`non-stream tasks:* smoke data degraded queue sections: ${queueDegraded.join(", ")}`);
}

const queueDepths = snapshot.queue_depths || {};
if (queueDepths["tasks:claude"] !== 1) {
  throw new Error(`expected tasks:claude queue depth 1, got ${queueDepths["tasks:claude"]}`);
}
if (Object.prototype.hasOwnProperty.call(queueDepths, "tasks:metadata")) {
  throw new Error("non-stream tasks:metadata appeared in queue_depths");
}
if ((snapshot.consumer_groups || []).some((group) => group.stream === "tasks:metadata")) {
  throw new Error("non-stream tasks:metadata appeared in consumer_groups");
}
if ((snapshot.queued_tasks || []).some((task) => task.stream === "tasks:metadata")) {
  throw new Error("non-stream tasks:metadata appeared in queued_tasks");
}
NODE
  then
    return 1
  fi
  echo "Dashboard Redis snapshot smoke verified non-stream tasks:* filtering"
}

cat >"$env_file" <<EOF
DASHBOARD_TOKEN=$token
ORCEST_REDIS_PASSWORD=$redis_password
DASHBOARD_NODE_VERSION=$node_version
DASHBOARD_STRICT_DEGRADED=1
ORCEST_DOCKER_NETWORK=$network_name
EOF

cat >"$compose_file" <<EOF
services:
  dashboard:
    image: $smoke_image
    build:
      context: "$repo_root/dashboard"
      args:
        NODE_VERSION: $node_version
    ports:
      - "127.0.0.1:$host_port:8080"
    environment:
      - REDIS_HOST=$redis_name
      - REDIS_PORT=6379
      - ORCEST_REDIS_PASSWORD=$redis_password
      - DASHBOARD_TOKEN=$token
    healthcheck:
      test: ["CMD", "node", "-e", "fetch('http://localhost:8080/api/health').then(r => process.exit(r.ok ? 0 : 1))"]
      interval: 30s
      timeout: 5s
      retries: 3
      start_period: 10s
    mem_limit: 512m
    networks:
      - orcest

networks:
  orcest:
    external: true
    name: $network_name
EOF

if ! docker network inspect "$network_name" >/dev/null 2>&1; then
  docker network create "$network_name" >/dev/null
  network_created=1
fi

docker run -d --name "$redis_name" --network "$network_name" redis:7 \
  redis-server --requirepass "$redis_password" >/dev/null
wait_for_redis
seed_dashboard_smoke_data

compose="docker compose --env-file $env_file -p $project -f $compose_file"
if DASHBOARD_NODE_VERSION="$node_version" \
  DASHBOARD_NODE_IMAGE="$node_image" \
  DASHBOARD_IMAGE="$smoke_image" \
  DASHBOARD_BASE_URL="http://127.0.0.1:$host_port" \
  DASHBOARD_VERIFY_HOST_PUBLISHED=1 \
  DASHBOARD_ENV_FILE="$env_file" \
  DASHBOARD_COMPOSE_FILE="$compose_file" \
  DASHBOARD_COMPOSE_STATE_FILE="$compose_state_file" \
  sh dashboard/scripts/deploy-compose-dashboard.sh $compose
then
  if verify_seeded_snapshot_contract; then
    exit 0
  fi
fi

$compose logs --tail=100 dashboard || true
docker logs "$redis_name" || true
echo "Dashboard compose smoke check failed" >&2
exit 1
