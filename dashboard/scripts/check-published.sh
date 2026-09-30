#!/usr/bin/env sh
set -eu

node_image="${DASHBOARD_NODE_IMAGE:-node:24.20.0-slim}"
base_url="${DASHBOARD_BASE_URL:-http://127.0.0.1:8080}"
env_file="${DASHBOARD_ENV_FILE:-}"

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

if [ "${DASHBOARD_TOKEN+x}" = "x" ]; then
  readiness_token="$DASHBOARD_TOKEN"
else
  readiness_token="$(env_file_value DASHBOARD_TOKEN)"
fi
if [ "${DASHBOARD_READY_ATTEMPTS+x}" = "x" ]; then
  ready_attempts="$DASHBOARD_READY_ATTEMPTS"
else
  ready_attempts="$(env_file_value DASHBOARD_READY_ATTEMPTS)"
fi
if [ "${DASHBOARD_READY_INTERVAL_MS+x}" = "x" ]; then
  ready_interval_ms="$DASHBOARD_READY_INTERVAL_MS"
else
  ready_interval_ms="$(env_file_value DASHBOARD_READY_INTERVAL_MS)"
fi
if [ "${DASHBOARD_ALLOW_DEGRADED+x}" = "x" ]; then
  allow_degraded="$DASHBOARD_ALLOW_DEGRADED"
else
  allow_degraded="$(env_file_value DASHBOARD_ALLOW_DEGRADED)"
fi
if [ "${DASHBOARD_STRICT_DEGRADED+x}" = "x" ]; then
  strict_degraded="$DASHBOARD_STRICT_DEGRADED"
else
  strict_degraded="$(env_file_value DASHBOARD_STRICT_DEGRADED)"
fi
if [ "${DASHBOARD_ALLOW_UNPINNED_ASSETS+x}" = "x" ]; then
  allow_unpinned_assets="$DASHBOARD_ALLOW_UNPINNED_ASSETS"
else
  allow_unpinned_assets="$(env_file_value DASHBOARD_ALLOW_UNPINNED_ASSETS)"
fi
if [ "${DASHBOARD_PUBLISHED_DOCKER_NETWORK+x}" = "x" ]; then
  published_docker_network="$DASHBOARD_PUBLISHED_DOCKER_NETWORK"
else
  published_docker_network="$(env_file_value DASHBOARD_PUBLISHED_DOCKER_NETWORK)"
fi
published_docker_network="${published_docker_network:-host}"

set --
if [ -n "$published_docker_network" ]; then
  set -- "$@" --network "$published_docker_network"
fi
if [ -n "$readiness_token" ]; then
  # Pass the token by NAME ONLY. `-e DASHBOARD_TOKEN=<value>` would put the
  # plaintext token in the argv of the host `docker` process, readable by any
  # unprivileged local user through `ps aux` / /proc/<pid>/cmdline for the whole
  # readiness wait (up to 60s on every deploy). With a bare `-e DASHBOARD_TOKEN`
  # the Docker client reads the value from its own environment instead, and the
  # value below is scoped to that single invocation.
  set -- "$@" -e DASHBOARD_TOKEN
fi

if [ -n "$ready_attempts" ]; then
  set -- "$@" -e "DASHBOARD_READY_ATTEMPTS=$ready_attempts"
fi
if [ -n "$ready_interval_ms" ]; then
  set -- "$@" -e "DASHBOARD_READY_INTERVAL_MS=$ready_interval_ms"
fi
if [ -n "$allow_degraded" ]; then
  set -- "$@" -e "DASHBOARD_ALLOW_DEGRADED=$allow_degraded"
fi
if [ -n "$strict_degraded" ]; then
  set -- "$@" -e "DASHBOARD_STRICT_DEGRADED=$strict_degraded"
fi
if [ -n "$allow_unpinned_assets" ]; then
  set -- "$@" -e "DASHBOARD_ALLOW_UNPINNED_ASSETS=$allow_unpinned_assets"
fi
if [ -n "${DASHBOARD_EXPECTED_REVISION:-}" ]; then
  set -- "$@" -e "DASHBOARD_EXPECTED_REVISION=$DASHBOARD_EXPECTED_REVISION"
fi

DASHBOARD_TOKEN="$readiness_token" docker run --rm -i "$@" \
  -e DASHBOARD_BASE_URL="$base_url" \
  -e DASHBOARD_EXPECTED_ASSETS="${DASHBOARD_EXPECTED_ASSETS:-}" \
  "$node_image" node --input-type=module <<'NODE'
import crypto from "node:crypto";
import net from "node:net";
import tls from "node:tls";

const baseUrl = process.env.DASHBOARD_BASE_URL || "http://127.0.0.1:8080";
const token = process.env.DASHBOARD_TOKEN || "";
const attempts = Number.parseInt(process.env.DASHBOARD_READY_ATTEMPTS || "60", 10);
const intervalMs = Number.parseInt(process.env.DASHBOARD_READY_INTERVAL_MS || "1000", 10);
const allowDegraded = /^(?:1|true|yes)$/i.test(process.env.DASHBOARD_ALLOW_DEGRADED || "");
const strictDegraded = /^(?:1|true|yes)$/i.test(process.env.DASHBOARD_STRICT_DEGRADED || "");
const allowUnpinnedAssets = /^(?:1|true|yes)$/i.test(process.env.DASHBOARD_ALLOW_UNPINNED_ASSETS || "");
const expectedAssetNames = (process.env.DASHBOARD_EXPECTED_ASSETS || "")
  .split(/\s+/)
  .map((path) => path.replaceAll("\\", "/").split("/").filter(Boolean).pop() || "")
  .filter(Boolean);

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const rootUrl = new URL(baseUrl);

function urlFor(path) {
  return new URL(path, rootUrl).toString();
}

function setCookieHeader(headers) {
  if (typeof headers.getSetCookie === "function") {
    return headers.getSetCookie()[0] || "";
  }
  return headers.get("set-cookie") || "";
}

function cookieFromSetCookie(header) {
  const cookie = header.split(";")[0].trim();
  return cookie.includes("=") ? cookie : "";
}

function assetPathsFromHtml(html, extension, pagePath = "/") {
  const pageUrl = new URL(pagePath, rootUrl);
  return [...html.matchAll(/<(?:script|link)\b[^>]*(?:src|href)="([^"]+)"/gi)]
    .map((match) => match[1])
    .filter((path) => new URL(path, pageUrl).pathname.endsWith(extension));
}

function assertExpectedAsset(path, extension, kind) {
  const expected = expectedAssetNames.filter((name) => name.endsWith(extension));
  if (expected.length === 0) {
    if (allowUnpinnedAssets) return;
    throw new Error(
      `DASHBOARD_EXPECTED_ASSETS must include a ${kind} asset, or set DASHBOARD_ALLOW_UNPINNED_ASSETS=1 for readiness-only checks`,
    );
  }

  const actual = new URL(path, rootUrl).pathname
    .split("/")
    .filter(Boolean)
    .pop();
  if (!actual || !expected.includes(actual)) {
    throw new Error(
      `dashboard ${kind} asset ${actual || "(missing)"} did not match expected built asset(s): ${expected.join(", ")}`,
    );
  }
}

async function expectStatus(path, status, headers = {}) {
  const response = await fetch(new URL(path, rootUrl), { headers });
  if (response.status !== status) {
    const body = await response.text();
    throw new Error(`${path} returned ${response.status}, expected ${status}: ${body.slice(0, 120)}`);
  }
  return response;
}

async function fetchAsset(path, pagePath, headers, expectedType, kind) {
  const response = await fetch(new URL(path, new URL(pagePath, rootUrl)), { headers });
  const contentType = response.headers.get("content-type") || "";
  if (response.status !== 200 || !contentType.includes(expectedType)) {
    throw new Error(`dashboard ${kind} asset failed from ${pagePath}: ${response.status} ${contentType}`);
  }
}

function parseHttpHeaders(text) {
  const [statusLine, ...headerLines] = text.split(/\r?\n/);
  const status = Number.parseInt(statusLine.split(/\s+/)[1] || "", 10);
  const headers = new Map();
  for (const line of headerLines) {
    const separator = line.indexOf(":");
    if (separator < 0) continue;
    headers.set(
      line.slice(0, separator).trim().toLowerCase(),
      line.slice(separator + 1).trim(),
    );
  }
  return { status, headers };
}

function websocketAccept(key) {
  return crypto
    .createHash("sha1")
    .update(`${key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11`)
    .digest("base64");
}

function parseSnapshotFrame(buffer) {
  if (buffer.length < 2) return null;
  const opcode = buffer[0] & 0x0f;
  if (opcode === 8) {
    throw new Error("snapshot websocket closed before sending a snapshot");
  }
  if (opcode !== 1) return null;

  const masked = Boolean(buffer[1] & 0x80);
  let payloadLength = buffer[1] & 0x7f;
  let offset = 2;
  if (payloadLength === 126) {
    if (buffer.length < offset + 2) return null;
    payloadLength = buffer.readUInt16BE(offset);
    offset += 2;
  } else if (payloadLength === 127) {
    if (buffer.length < offset + 8) return null;
    const length = buffer.readBigUInt64BE(offset);
    if (length > BigInt(Number.MAX_SAFE_INTEGER)) {
      throw new Error("snapshot websocket frame is too large");
    }
    payloadLength = Number(length);
    offset += 8;
  }

  let mask;
  if (masked) {
    if (buffer.length < offset + 4) return null;
    mask = buffer.subarray(offset, offset + 4);
    offset += 4;
  }

  if (buffer.length < offset + payloadLength) return null;
  const payload = Buffer.from(buffer.subarray(offset, offset + payloadLength));
  if (mask) {
    for (let index = 0; index < payload.length; index += 1) {
      payload[index] ^= mask[index % 4];
    }
  }
  const message = JSON.parse(payload.toString("utf8"));
  if (!message || typeof message !== "object" || !message.snapshot) {
    throw new Error("snapshot websocket sent a malformed snapshot frame");
  }
  if (message.snapshot.redis_ok !== true) {
    throw new Error("snapshot websocket reported Redis unavailable");
  }
  const degradedSections = Array.isArray(message.snapshot.degraded_sections)
    ? message.snapshot.degraded_sections.filter((section) => typeof section === "string" && section.trim())
    : [];
  if (degradedSections.length > 0) {
    const degradedMessage = `snapshot reported degraded sections: ${degradedSections.join(", ")}`;
    if (strictDegraded && !allowDegraded) throw new Error(degradedMessage);
    console.error(`Warning: ${degradedMessage}`);
  }
  return true;
}

async function checkSnapshotWebSocket(cookie) {
  await new Promise((resolve, reject) => {
    const secure = rootUrl.protocol === "https:";
    if (!secure && rootUrl.protocol !== "http:") {
      reject(new Error(`unsupported dashboard base URL protocol: ${rootUrl.protocol}`));
      return;
    }

    const port = Number.parseInt(
      rootUrl.port || (secure ? "443" : "80"),
      10,
    );
    const key = crypto.randomBytes(16).toString("base64");
    const socket = secure
      ? tls.connect({ host: rootUrl.hostname, port, servername: rootUrl.hostname })
      : net.connect({ host: rootUrl.hostname, port });
    let buffer = Buffer.alloc(0);
    let upgraded = false;
    const timeout = setTimeout(() => {
      socket.destroy();
      reject(new Error("snapshot websocket timed out"));
    }, 8000);

    const fail = (err) => {
      clearTimeout(timeout);
      socket.destroy();
      reject(err);
    };
    const pass = () => {
      clearTimeout(timeout);
      socket.end();
      resolve();
    };

    socket.once("error", fail);
    socket.once(secure ? "secureConnect" : "connect", () => {
      socket.write([
        "GET /ws/snapshot HTTP/1.1",
        `Host: ${rootUrl.host}`,
        "Upgrade: websocket",
        "Connection: Upgrade",
        `Sec-WebSocket-Key: ${key}`,
        "Sec-WebSocket-Version: 13",
        `Origin: ${rootUrl.origin}`,
        `Cookie: ${cookie}`,
        "",
        "",
      ].join("\r\n"));
    });

    socket.on("data", (chunk) => {
      try {
        buffer = Buffer.concat([buffer, chunk]);
        if (!upgraded) {
          const headerEnd = buffer.indexOf("\r\n\r\n");
          if (headerEnd < 0) return;
          const { status, headers } = parseHttpHeaders(buffer.subarray(0, headerEnd).toString("latin1"));
          if (status !== 101) {
            throw new Error(`snapshot websocket upgrade returned ${status}`);
          }
          const accept = headers.get("sec-websocket-accept") || "";
          if (accept !== websocketAccept(key)) {
            throw new Error("snapshot websocket returned an invalid accept key");
          }
          buffer = buffer.subarray(headerEnd + 4);
          upgraded = true;
        }
        if (parseSnapshotFrame(buffer)) pass();
      } catch (err) {
        fail(err);
      }
    });
  });
}

async function check() {
  const ready = await fetch(urlFor("/api/ready"));
  const readyText = await ready.text();
  let readyBody;
  try {
    readyBody = JSON.parse(readyText);
  } catch {
    throw new Error(`readiness returned non-JSON ${ready.status}: ${readyText.slice(0, 200)}`);
  }
  if (ready.status !== 200 || readyBody.ok !== true || readyBody.redis_ok !== true) {
    throw new Error(`readiness failed: ${ready.status} ${JSON.stringify(readyBody)}`);
  }
  const expectedRevision = (process.env.DASHBOARD_EXPECTED_REVISION || "").trim().toLowerCase();
  if (expectedRevision && readyBody.revision !== expectedRevision) {
    throw new Error(
      `readiness revision mismatch: expected ${expectedRevision}, got ${String(readyBody.revision)}`,
    );
  }
  if (!token) {
    throw new Error("DASHBOARD_TOKEN is required for published static verification");
  }

  await expectStatus("/", 401);
  await expectStatus(`/?token=${encodeURIComponent(`${token}-wrong`)}`, 401);

  const page = await fetch(urlFor(`/?token=${encodeURIComponent(token)}`));
  const pageText = await page.text();
  const contentType = page.headers.get("content-type") || "";
  if (page.status !== 200 || !contentType.includes("text/html") || !pageText.includes('id="root"')) {
    throw new Error(`dashboard HTML failed: ${page.status} ${contentType} ${pageText.slice(0, 120)}`);
  }
  const cookie = cookieFromSetCookie(setCookieHeader(page.headers));
  if (!cookie.startsWith("orcest_dashboard_token=")) {
    throw new Error("dashboard HTML did not set the auth cookie");
  }

  const cookieHeaders = { cookie };
  const cookiePage = await fetch(urlFor("/"), { headers: cookieHeaders });
  const cookiePageText = await cookiePage.text();
  if (
    cookiePage.status !== 200 ||
    !(cookiePage.headers.get("content-type") || "").includes("text/html") ||
    !cookiePageText.includes('id="root"')
  ) {
    throw new Error(`dashboard cookie-auth HTML failed: ${cookiePage.status} ${cookiePageText.slice(0, 120)}`);
  }

  const jsPath = assetPathsFromHtml(pageText, ".js")[0];
  const cssPath = assetPathsFromHtml(pageText, ".css")[0];
  if (!jsPath || !cssPath) {
    throw new Error("dashboard HTML did not reference both JS and CSS assets");
  }
  assertExpectedAsset(jsPath, ".js", "JS");
  assertExpectedAsset(cssPath, ".css", "CSS");

  await expectStatus(jsPath, 401);
  await expectStatus(cssPath, 401);

  await fetchAsset(jsPath, "/", cookieHeaders, "javascript", "JS");
  await fetchAsset(cssPath, "/", cookieHeaders, "text/css", "CSS");

  const deepLinkPath = "/work/results";
  const deepLink = await fetch(urlFor(deepLinkPath), { headers: cookieHeaders });
  const deepLinkText = await deepLink.text();
  const deepLinkType = deepLink.headers.get("content-type") || "";
  if (deepLink.status !== 200 || !deepLinkType.includes("text/html") || !deepLinkText.includes('id="root"')) {
    throw new Error(`dashboard deep-link HTML failed: ${deepLink.status} ${deepLinkType} ${deepLinkText.slice(0, 120)}`);
  }
  const deepLinkJsPath = assetPathsFromHtml(deepLinkText, ".js", deepLinkPath)[0];
  const deepLinkCssPath = assetPathsFromHtml(deepLinkText, ".css", deepLinkPath)[0];
  if (!deepLinkJsPath || !deepLinkCssPath) {
    throw new Error("dashboard deep-link HTML did not reference both JS and CSS assets");
  }
  assertExpectedAsset(deepLinkJsPath, ".js", "JS");
  assertExpectedAsset(deepLinkCssPath, ".css", "CSS");
  await fetchAsset(deepLinkJsPath, deepLinkPath, cookieHeaders, "javascript", "JS");
  await fetchAsset(deepLinkCssPath, deepLinkPath, cookieHeaders, "text/css", "CSS");

  await checkSnapshotWebSocket(cookie);
}

let lastError = null;
for (let attempt = 1; attempt <= attempts; attempt += 1) {
  try {
    await check();
    console.log(`Dashboard published readiness verified at ${baseUrl}`);
    process.exit(0);
  } catch (err) {
    lastError = err;
    console.error(`dashboard published readiness attempt ${attempt}/${attempts}: ${err.message}`);
    if (attempt < attempts) await sleep(intervalMs);
  }
}

throw lastError;
NODE
