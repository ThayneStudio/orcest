#!/usr/bin/env sh
set -eu

if [ "$#" -eq 0 ]; then
  echo "Usage: $0 <docker> <compose> [compose args...]" >&2
  exit 2
fi

node_version="${DASHBOARD_NODE_VERSION:-20.18.3}"
node_image="${DASHBOARD_NODE_IMAGE:-node:${node_version}-slim}"
rollback_image="${DASHBOARD_ROLLBACK_IMAGE:-orcest-dashboard:rollback-$$}"
base_url="${DASHBOARD_BASE_URL:-http://127.0.0.1:8080}"
container_base_url="${DASHBOARD_PUBLISHED_CONTAINER_BASE_URL:-http://127.0.0.1:8080}"
host_published_network="${DASHBOARD_HOST_PUBLISHED_DOCKER_NETWORK:-host}"
env_file="${DASHBOARD_ENV_FILE:-}"
deploy_lock_dir="${DASHBOARD_DEPLOY_LOCK_DIR:-.dashboard-deploy.lock}"
deploy_lock_acquired=0
deploy_lock_held="${DASHBOARD_DEPLOY_LOCK_HELD:-0}"
rollback_image_pinned=0
candidate_may_be_live=0

compose_env_value() {
  file="$1"
  key="$2"
  [ -n "$file" ] && [ -r "$file" ] || return 1
  awk -v key="$key" '
    function trim(s) {
      sub(/^[[:space:]]+/, "", s)
      sub(/[[:space:]]+$/, "", s)
      return s
    }
    function unquote(s, q, body, end) {
      s = trim(s)
      q = substr(s, 1, 1)
      if (q == "\"" || q == sprintf("%c", 39)) {
        body = substr(s, 2)
        end = index(body, q)
        return end > 0 ? substr(body, 1, end - 1) : body
      }
      sub(/[[:space:]]+#.*$/, "", s)
      return trim(s)
    }
    /^[[:space:]]*(#|$)/ { next }
    {
      line = $0
      if (line !~ /^[[:space:]]*[A-Za-z_][A-Za-z0-9_]*[[:space:]]*=/) next
      name = line
      sub(/^[[:space:]]*/, "", name)
      sub(/[[:space:]]*=.*$/, "", name)
      if (name != key) next
      value = line
      sub(/^[[:space:]]*[A-Za-z_][A-Za-z0-9_]*[[:space:]]*=[[:space:]]*/, "", value)
      found = 1
      result = unquote(value)
    }
    END {
      if (found) print result
      else exit 1
    }
  ' "$file"
}

env_file_value() {
  compose_env_value "$env_file" "$1" 2>/dev/null || true
}

if [ "${DASHBOARD_IMAGE+x}" = "x" ]; then
  dashboard_image="${DASHBOARD_IMAGE:-orcest-dashboard:latest}"
else
  dashboard_image="$(env_file_value DASHBOARD_IMAGE)"
  dashboard_image="${dashboard_image:-orcest-dashboard:latest}"
fi
if [ "${DASHBOARD_VERIFY_HOST_PUBLISHED+x}" = "x" ]; then
  verify_host_published="$DASHBOARD_VERIFY_HOST_PUBLISHED"
else
  verify_host_published="$(env_file_value DASHBOARD_VERIFY_HOST_PUBLISHED)"
fi

truthy() {
  case "$1" in
    1|true|TRUE|yes|YES) return 0 ;;
    *) return 1 ;;
  esac
}

collect_assets() {
  sh dashboard/scripts/list-published-assets.sh "$@"
}

published_dashboard_network() {
  container="$("$@" ps -q dashboard 2>/dev/null | sed -n '1p' || true)"
  if [ -n "$container" ]; then
    printf 'container:%s\n' "$container"
  fi
}

check_published() {
  expected_assets="$1"
  shift
  published_network="$(published_dashboard_network "$@")"
  if [ -n "$published_network" ]; then
    DASHBOARD_EXPECTED_ASSETS="$expected_assets" \
      DASHBOARD_NODE_IMAGE="$node_image" \
      DASHBOARD_BASE_URL="$container_base_url" \
      DASHBOARD_ENV_FILE="$env_file" \
      DASHBOARD_PUBLISHED_DOCKER_NETWORK="$published_network" \
      sh dashboard/scripts/check-published.sh || return $?
    if truthy "$verify_host_published"; then
      DASHBOARD_EXPECTED_ASSETS="$expected_assets" \
        DASHBOARD_NODE_IMAGE="$node_image" \
        DASHBOARD_BASE_URL="$base_url" \
        DASHBOARD_ENV_FILE="$env_file" \
        DASHBOARD_PUBLISHED_DOCKER_NETWORK="$host_published_network" \
        sh dashboard/scripts/check-published.sh || return $?
    fi
  else
    DASHBOARD_EXPECTED_ASSETS="$expected_assets" \
      DASHBOARD_NODE_IMAGE="$node_image" \
      DASHBOARD_BASE_URL="$base_url" \
      DASHBOARD_ENV_FILE="$env_file" \
      sh dashboard/scripts/check-published.sh || return $?
  fi
}

check_published_unpinned() {
  published_network="$(published_dashboard_network "$@")"
  if [ -n "$published_network" ]; then
    DASHBOARD_EXPECTED_ASSETS="" \
      DASHBOARD_ALLOW_UNPINNED_ASSETS=1 \
      DASHBOARD_NODE_IMAGE="$node_image" \
      DASHBOARD_BASE_URL="$container_base_url" \
      DASHBOARD_ENV_FILE="$env_file" \
      DASHBOARD_PUBLISHED_DOCKER_NETWORK="$published_network" \
      sh dashboard/scripts/check-published.sh || return $?
    if truthy "$verify_host_published"; then
      DASHBOARD_EXPECTED_ASSETS="" \
        DASHBOARD_ALLOW_UNPINNED_ASSETS=1 \
        DASHBOARD_NODE_IMAGE="$node_image" \
        DASHBOARD_BASE_URL="$base_url" \
        DASHBOARD_ENV_FILE="$env_file" \
        DASHBOARD_PUBLISHED_DOCKER_NETWORK="$host_published_network" \
        sh dashboard/scripts/check-published.sh || return $?
    fi
  else
    DASHBOARD_EXPECTED_ASSETS="" \
      DASHBOARD_ALLOW_UNPINNED_ASSETS=1 \
      DASHBOARD_NODE_IMAGE="$node_image" \
      DASHBOARD_BASE_URL="$base_url" \
      DASHBOARD_ENV_FILE="$env_file" \
      sh dashboard/scripts/check-published.sh || return $?
  fi
}

check_bundle_runtime() {
  "$@" exec -T dashboard node scripts/check-bundle-runtime.mjs
}

check_candidate_bundle_runtime() {
  docker run --rm "$1" node scripts/check-bundle-runtime.mjs
}

collect_candidate_assets() {
  image="$1"
  asset_file="$(mktemp)"
  if ! docker run --rm "$image" sh -lc '
set -eu

emit_assets() {
  kind="$1"
  pattern="$2"
  set -- $pattern
  if [ "$1" = "$pattern" ] || [ ! -f "$1" ]; then
    echo "missing dashboard $kind asset matching $pattern" >&2
    exit 1
  fi
  for asset in "$@"; do
    [ -f "$asset" ] || continue
    printf "%s\n" "$asset"
  done
}

emit_assets JS "dist/assets/index-*.js"
emit_assets CSS "dist/assets/index-*.css"
' >"$asset_file"; then
    rm -f "$asset_file"
    echo "Dashboard candidate image did not publish built JS/CSS assets" >&2
    return 1
  fi
  if ! grep -Eq '\.js$' "$asset_file" || ! grep -Eq '\.css$' "$asset_file"; then
    rm -f "$asset_file"
    echo "Dashboard candidate image did not publish both built JS and CSS assets" >&2
    return 1
  fi
  tr '\n' ' ' <"$asset_file"
  printf '\n'
  rm -f "$asset_file"
}

cleanup_rollback_image() {
  if [ "$rollback_image_pinned" = "1" ]; then
    docker image rm "$rollback_image" >/dev/null 2>&1 || true
    rollback_image_pinned=0
  fi
}

validate_deploy_lock_dir() {
  case "$deploy_lock_dir" in
    ""|"/"|"."|".."|"/."|"/.."|../*|*/..|*/../*)
      echo "Unsafe DASHBOARD_DEPLOY_LOCK_DIR: $deploy_lock_dir" >&2
      exit 2
      ;;
  esac
}

write_deploy_lock_metadata() {
  {
    printf 'pid=%s\n' "$$"
    printf 'started_at_epoch=%s\n' "$(date +%s 2>/dev/null || true)"
    printf 'cwd=%s\n' "$(pwd 2>/dev/null || true)"
    host="$(hostname 2>/dev/null || true)"
    if [ -n "$host" ]; then
      printf 'host=%s\n' "$host"
    fi
    i=0
    for arg do
      i=$((i + 1))
      printf 'arg_%s=%s\n' "$i" "$arg"
    done
  } >"$deploy_lock_dir/info"
}

acquire_deploy_lock() {
  validate_deploy_lock_dir
  if mkdir "$deploy_lock_dir" 2>/dev/null; then
    deploy_lock_acquired=1
    write_deploy_lock_metadata "$@" || true
    return 0
  fi

  if [ ! -d "$deploy_lock_dir" ]; then
    echo "Dashboard deploy lock could not be created: $deploy_lock_dir" >&2
    exit 1
  fi

  echo "Dashboard deploy lock is already held: $deploy_lock_dir" >&2
  if [ -r "$deploy_lock_dir/info" ]; then
    sed 's/^/  /' "$deploy_lock_dir/info" >&2 || true
  fi
  echo "Refusing to run a concurrent dashboard deploy; remove the lock only after confirming no deploy is active." >&2
  exit 1
}

release_deploy_lock() {
  if [ "$deploy_lock_acquired" = "1" ]; then
    rm -rf "$deploy_lock_dir" || true
    deploy_lock_acquired=0
  fi
}

cleanup() {
  cleanup_rollback_image
  release_deploy_lock
}

handle_signal() {
  code="$1"
  shift
  trap - EXIT INT TERM
  if [ "$candidate_may_be_live" = "1" ]; then
    echo "Dashboard deploy interrupted after candidate start; rolling back" >&2
    rollback_dashboard "$@" || true
    candidate_may_be_live=0
  fi
  cleanup
  exit "$code"
}

trap cleanup EXIT
trap 'handle_signal 130 "$@"' INT
trap 'handle_signal 143 "$@"' TERM

if [ "$deploy_lock_held" = "1" ]; then
  validate_deploy_lock_dir
  if [ ! -d "$deploy_lock_dir" ]; then
    echo "DASHBOARD_DEPLOY_LOCK_HELD=1 but dashboard deploy lock is missing: $deploy_lock_dir" >&2
    exit 1
  fi
else
  acquire_deploy_lock "$@"
fi

previous_container="$("$@" ps -q dashboard 2>/dev/null || true)"
previous_image_id=""
previous_image_name=""
previous_assets=""

if [ -n "$previous_container" ]; then
  previous_image_id="$(docker inspect -f '{{.Image}}' "$previous_container" 2>/dev/null || true)"
  previous_image_name="$(docker inspect -f '{{.Config.Image}}' "$previous_container" 2>/dev/null || true)"
  previous_assets="$(collect_assets "$@" 2>/dev/null || true)"
fi

if [ -n "$previous_image_id" ]; then
  if docker tag "$previous_image_id" "$rollback_image"; then
    rollback_image_pinned=1
  else
    echo "Dashboard deploy could not pin previous image $previous_image_id for rollback" >&2
    exit 1
  fi
fi

restorable_image_name() {
  case "$1" in
    ""|sha256:*|*@*) return 1 ;;
    *) return 0 ;;
  esac
}

restore_rollback_image_tag() {
  image_name="$1"
  if docker tag "$rollback_image" "$image_name"; then
    echo "Restored previous dashboard image tag $image_name" >&2
    return 0
  fi
  echo "Dashboard deploy could not restore previous image tag $image_name" >&2
  return 1
}

restore_previous_image_tag() {
  if [ "$rollback_image_pinned" = "1" ]; then
    if [ "$previous_image_name" = "$dashboard_image" ]; then
      restore_rollback_image_tag "$dashboard_image"
      return $?
    fi
    docker image rm "$dashboard_image" >/dev/null 2>&1 || true
    return 0
  fi

  docker image rm "$dashboard_image" >/dev/null 2>&1 || true
  return 0
}

fail_before_live_start() {
  restore_previous_image_tag || true
  exit 1
}

rollback_dashboard() {
  if [ "$rollback_image_pinned" != "1" ]; then
    echo "No previous dashboard image is available for rollback; removing failed dashboard service" >&2
    if "$@" rm -sf dashboard; then
      echo "Removed failed dashboard service" >&2
      return 0
    fi
    echo "Failed dashboard service could not be removed" >&2
    return 1
  fi

  echo "Rolling dashboard back to previous image $previous_image_id..."
  rollback_compose_image="$rollback_image"
  if [ "$previous_image_name" = "$dashboard_image" ]; then
    if ! restore_previous_image_tag; then
      echo "Dashboard rollback could not restore the previous image tag" >&2
      return 1
    fi
    rollback_compose_image="$dashboard_image"
  else
    docker image rm "$dashboard_image" >/dev/null 2>&1 || true
    if restorable_image_name "$previous_image_name"; then
      if restore_rollback_image_tag "$previous_image_name"; then
        rollback_compose_image="$previous_image_name"
      else
        echo "Dashboard rollback will use pinned image $rollback_image" >&2
      fi
    fi
  fi
  if ! DASHBOARD_IMAGE="$rollback_compose_image" "$@" up -d --no-build --force-recreate dashboard; then
    echo "Dashboard rollback could not restart the previous image" >&2
    return 1
  fi
  if [ -n "$previous_assets" ]; then
    if check_published "$previous_assets" "$@"; then
      echo "Dashboard rollback published readiness verified"
      return 0
    fi
    "$@" logs --tail=100 dashboard || true
    echo "Dashboard rollback did not become published-ready" >&2
    return 1
  fi

  echo "Dashboard rollback restarted the previous image; previous asset list was unavailable; checking readiness without asset pin" >&2
  if check_published_unpinned "$@"; then
    echo "Dashboard rollback readiness verified without asset pin"
    return 0
  fi
  "$@" logs --tail=100 dashboard || true
  echo "Dashboard rollback did not become readiness-ready without asset pin" >&2
  return 1
}

if ! DASHBOARD_NODE_VERSION="$node_version" "$@" build dashboard; then
  echo "Dashboard compose build failed" >&2
  fail_before_live_start
fi

candidate_image="$dashboard_image"
candidate_image_id="$(docker image inspect -f '{{.Id}}' "$candidate_image" 2>/dev/null || true)"
if [ -z "$candidate_image_id" ]; then
  echo "Dashboard compose build did not publish a candidate dashboard image ($candidate_image)" >&2
  fail_before_live_start
fi

if ! check_candidate_bundle_runtime "$candidate_image"; then
  echo "Dashboard candidate bundle runtime check failed" >&2
  fail_before_live_start
fi

if ! expected_assets="$(collect_candidate_assets "$candidate_image")"; then
  fail_before_live_start
fi

candidate_may_be_live=1
if ! "$@" up -d --no-build --force-recreate dashboard; then
  echo "Dashboard compose start failed" >&2
  rollback_dashboard "$@" || true
  exit 1
fi

running_container="$("$@" ps -q dashboard 2>/dev/null || true)"
running_image_id=""
if [ -n "$running_container" ]; then
  running_image_id="$(docker inspect -f '{{.Image}}' "$running_container" 2>/dev/null || true)"
fi
if [ -z "$running_image_id" ] || [ "$running_image_id" != "$candidate_image_id" ]; then
  "$@" logs --tail=100 dashboard || true
  echo "Dashboard live container image did not match candidate image $candidate_image_id" >&2
  rollback_dashboard "$@" || true
  exit 1
fi

if ! check_bundle_runtime "$@"; then
  "$@" logs --tail=100 dashboard || true
  echo "Dashboard bundle runtime check failed" >&2
  rollback_dashboard "$@" || true
  exit 1
fi

echo "Waiting for dashboard published readiness..."
if check_published "$expected_assets" "$@"; then
  candidate_may_be_live=0
  echo "Dashboard published readiness verified"
  exit 0
fi

"$@" logs --tail=100 dashboard || true
echo "Dashboard did not become published-ready" >&2
rollback_dashboard "$@" || true
exit 1
