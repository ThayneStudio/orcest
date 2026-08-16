#!/usr/bin/env sh
set -eu

image="${1:-orcest-dashboard:ci}"
token="${DASHBOARD_SMOKE_TOKEN:-dashboard-smoke-token}"
# Hand the token to Docker through the environment, never through argv:
# `-e NAME=VALUE` publishes the plaintext value in the host docker process's
# command line, which any unprivileged local user can read from `ps aux` or
# /proc/<pid>/cmdline. The bare `-e NAME` forms below make the Docker client
# read these exported values instead.
DASHBOARD_TOKEN="$token"
DASHBOARD_SMOKE_TOKEN="$token"
export DASHBOARD_TOKEN DASHBOARD_SMOKE_TOKEN
cid=""
asset_file="$(mktemp)"
asset_line_file="$(mktemp)"

cleanup() {
  if [ -n "$cid" ]; then
    docker rm -f "$cid" >/dev/null 2>&1 || true
  fi
  rm -f "$asset_file" "$asset_line_file"
}

trap cleanup EXIT INT TERM

cid="$(docker run -d -e DASHBOARD_TOKEN "$image")"
if ! docker exec "$cid" sh -lc '
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
  echo "Dashboard image did not publish built JS/CSS assets" >&2
  exit 1
fi
tr '\n' ' ' <"$asset_file" >"$asset_line_file"
printf '\n' >>"$asset_line_file"
IFS= read -r expected_assets <"$asset_line_file"

if ! docker exec "$cid" node scripts/check-bundle-runtime.mjs; then
  docker logs "$cid" || true
  echo "Dashboard image bundle runtime check failed for $image" >&2
  exit 1
fi

for _attempt in $(seq 1 60); do
  if docker exec -i \
    -e DASHBOARD_SMOKE_TOKEN \
    -e DASHBOARD_EXPECTED_ASSETS="$expected_assets" \
    "$cid" node <<'NODE'
const token = process.env.DASHBOARD_SMOKE_TOKEN;
const expectedAssetNames = (process.env.DASHBOARD_EXPECTED_ASSETS || "")
  .split(/\s+/)
  .map((path) => path.replaceAll("\\", "/").split("/").filter(Boolean).pop() || "")
  .filter(Boolean);
const baseUrl = new URL("http://127.0.0.1:8080");

function cookieFromSetCookie(header) {
  const cookie = (header || "").split(";")[0].trim();
  return cookie.includes("=") ? cookie : "";
}

function assetPathsFromHtml(html, extension, pagePath = "/") {
  const pageUrl = new URL(pagePath, baseUrl);
  return [...html.matchAll(/<(?:script|link)\b[^>]*(?:src|href)="([^"]+)"/gi)]
    .map((match) => match[1])
    .filter((path) => new URL(path, pageUrl).pathname.endsWith(extension));
}

function assertExpectedAsset(path, extension, kind) {
  const expected = expectedAssetNames.filter((name) => name.endsWith(extension));
  if (expected.length === 0) {
    throw new Error(`expected ${kind} asset list was empty`);
  }

  const actual = new URL(path, baseUrl).pathname
    .split("/")
    .filter(Boolean)
    .pop();
  if (!actual || !expected.includes(actual)) {
    throw new Error(
      `dashboard ${kind} asset ${actual || "(missing)"} did not match expected built asset(s): ${expected.join(", ")}`,
    );
  }
}

async function fetchAsset(path, pagePath, cookie, expectedType, kind) {
  const response = await fetch(new URL(path, new URL(pagePath, baseUrl)), {
    headers: { cookie },
  });
  const contentType = response.headers.get("content-type") || "";
  if (response.status !== 200 || !contentType.includes(expectedType)) {
    throw new Error(`expected ${kind} asset from ${pagePath} to return 200 ${expectedType}, got ${response.status} ${contentType}`);
  }
}

async function main() {
  const health = await fetch("http://127.0.0.1:8080/api/health");
  if (health.status !== 200) {
    throw new Error(`expected health check to return 200, got ${health.status}`);
  }
  const healthBody = await health.json();
  if (healthBody.ok !== true) {
    throw new Error(`expected health check ok=true, got ${JSON.stringify(healthBody)}`);
  }

  const ready = await fetch("http://127.0.0.1:8080/api/ready");
  if (ready.status !== 503) {
    throw new Error(`expected readiness without Redis to return 503, got ${ready.status}`);
  }
  const readyBody = await ready.json();
  if (readyBody.ok !== false || readyBody.redis_ok !== false) {
    throw new Error(`expected readiness ok=false, got ${JSON.stringify(readyBody)}`);
  }

  const unauthorized = await fetch("http://127.0.0.1:8080/");
  if (unauthorized.status !== 401) {
    throw new Error(`expected unauthenticated request to return 401, got ${unauthorized.status}`);
  }

  const authorized = await fetch(
    `http://127.0.0.1:8080/?token=${encodeURIComponent(token)}`,
  );
  const html = await authorized.text();
  if (authorized.status !== 200) {
    throw new Error(`expected authenticated request to return 200, got ${authorized.status}`);
  }
  if (!authorized.headers.get("set-cookie")) {
    throw new Error("authenticated request did not set the dashboard cookie");
  }
  if (!html.includes("Orcest Dashboard")) {
    throw new Error("authenticated request did not return the dashboard HTML");
  }
  const cookie = cookieFromSetCookie(authorized.headers.get("set-cookie"));
  if (!cookie.startsWith("orcest_dashboard_token=")) {
    throw new Error("authenticated request did not set a usable dashboard cookie");
  }

  const jsPath = assetPathsFromHtml(html, ".js")[0];
  const cssPath = assetPathsFromHtml(html, ".css")[0];
  if (!jsPath || !cssPath) {
    throw new Error("authenticated dashboard HTML did not reference both JS and CSS assets");
  }

  assertExpectedAsset(jsPath, ".js", "JS");
  assertExpectedAsset(cssPath, ".css", "CSS");

  const unauthenticatedAsset = await fetch(new URL(jsPath, baseUrl));
  if (unauthenticatedAsset.status !== 401) {
    throw new Error(`expected unauthenticated asset request to return 401, got ${unauthenticatedAsset.status}`);
  }

  await fetchAsset(jsPath, "/", cookie, "javascript", "JS");
  await fetchAsset(cssPath, "/", cookie, "text/css", "CSS");

  const deepLinkPath = "/work/results";
  const deepLink = await fetch(new URL(deepLinkPath, baseUrl), {
    headers: { cookie },
  });
  const deepLinkHtml = await deepLink.text();
  if (deepLink.status !== 200 || !deepLinkHtml.includes("Orcest Dashboard")) {
    throw new Error(`expected authenticated deep link to return dashboard HTML, got ${deepLink.status}`);
  }
  const deepLinkJsPath = assetPathsFromHtml(deepLinkHtml, ".js", deepLinkPath)[0];
  const deepLinkCssPath = assetPathsFromHtml(deepLinkHtml, ".css", deepLinkPath)[0];
  if (!deepLinkJsPath || !deepLinkCssPath) {
    throw new Error("authenticated deep-link dashboard HTML did not reference both JS and CSS assets");
  }
  assertExpectedAsset(deepLinkJsPath, ".js", "JS");
  assertExpectedAsset(deepLinkCssPath, ".css", "CSS");
  await fetchAsset(deepLinkJsPath, deepLinkPath, cookie, "javascript", "JS");
  await fetchAsset(deepLinkCssPath, deepLinkPath, cookie, "text/css", "CSS");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
NODE
  then
    exit 0
  fi
  sleep 0.5
done

docker logs "$cid" || true
echo "Dashboard image smoke check failed for $image" >&2
exit 1
