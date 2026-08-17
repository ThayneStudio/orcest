import { existsSync, readFileSync } from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { Window } from "happy-dom";

const distDir = path.resolve(process.env.DASHBOARD_DIST_DIR || "dist");
const indexPath = path.join(distDir, "index.html");
const timeoutMs = Number.parseInt(process.env.DASHBOARD_BUNDLE_RUNTIME_TIMEOUT_MS || "2000", 10);
const expectedTexts = (process.env.DASHBOARD_BUNDLE_RUNTIME_EXPECTED_TEXT || [
  "Orcest Dashboard",
  "Overview",
  "Kanban",
  "Dead Letters",
  "Recent Worker Output",
].join("\n")).split(/\n+/).map((text) => text.trim()).filter(Boolean);

if (!existsSync(indexPath)) {
  throw new Error(`dashboard bundle runtime check could not find ${indexPath}`);
}

const html = readFileSync(indexPath, "utf8");
const jsPath = [...html.matchAll(/<script\b[^>]*src="([^"]+\.js)"/gi)][0]?.[1];
if (!jsPath) {
  throw new Error("dashboard bundle runtime check could not find a JS asset in dist/index.html");
}

const window = new Window({
  url: process.env.DASHBOARD_BUNDLE_RUNTIME_URL || "http://127.0.0.1:8080/?token=bundle-runtime-smoke",
});
window.document.write(html);
window.document.close();

const socketUrls = [];
class SmokeWebSocket {
  static CONNECTING = 0;
  static OPEN = 1;
  static CLOSING = 2;
  static CLOSED = 3;

  readyState = SmokeWebSocket.CONNECTING;

  constructor(url) {
    this.url = String(url);
    socketUrls.push(this.url);
    setTimeout(() => {
      if (this.readyState !== SmokeWebSocket.CONNECTING) return;
      this.readyState = SmokeWebSocket.OPEN;
      this.onopen?.({ type: "open" });
      this.onmessage?.({
        data: JSON.stringify({
          snapshot: {
            redis_ok: true,
            fetched_at: new Date().toISOString(),
            queue_depths: {},
            results_depth: 0,
            dead_letter_count: 0,
            locks: [],
            consumer_groups: [],
            recent_results: [],
            attempt_counts: {},
            dead_letter_entries: [],
            queued_tasks: [],
            provider_health: {},
            worker_pool: [],
            degraded_sections: [],
            dashboard_policy: {
              max_attempts: 3,
              pending_task_ttl_seconds: 5700,
              lock_ttl_seconds: 5400,
            },
          },
          stuck_tasks: [],
          workers: [],
        }),
      });
    }, 0);
  }

  close(code = 1000) {
    if (this.readyState === SmokeWebSocket.CLOSED) return;
    this.readyState = SmokeWebSocket.CLOSED;
    this.onclose?.({ code });
  }
}

Object.assign(globalThis, {
  window,
  document: window.document,
  navigator: window.navigator,
  location: window.location,
  history: window.history,
  localStorage: window.localStorage,
  sessionStorage: window.sessionStorage,
  HTMLElement: window.HTMLElement,
  Element: window.Element,
  Node: window.Node,
  Event: window.Event,
  CustomEvent: window.CustomEvent,
  KeyboardEvent: window.KeyboardEvent,
  MouseEvent: window.MouseEvent,
  requestAnimationFrame: window.requestAnimationFrame.bind(window),
  cancelAnimationFrame: window.cancelAnimationFrame.bind(window),
  WebSocket: SmokeWebSocket,
  fetch: async () => ({ ok: true }),
});
window.WebSocket = SmokeWebSocket;
window.fetch = globalThis.fetch;

const errors = [];
window.addEventListener("error", (event) => {
  errors.push(event.error || event.message);
});
window.addEventListener("unhandledrejection", (event) => {
  errors.push(event.reason);
});

await import(pathToFileURL(path.join(distDir, jsPath.replace(/^\//, ""))).href);

const startedAt = Date.now();
while (Date.now() - startedAt < timeoutMs) {
  if (errors.length > 0) {
    throw new Error(`dashboard bundle runtime error: ${errors.map(String).join("\n")}`);
  }

  const bodyText = window.document.body.textContent || "";
  const missingText = expectedTexts.filter((text) => !bodyText.includes(text));
  if (missingText.length === 0) {
    console.log(`Dashboard bundle runtime verified with ${socketUrls.length} snapshot websocket(s)`);
    process.exit(0);
  }

  await new Promise((resolve) => setTimeout(resolve, 25));
}

const bodyText = (window.document.body.textContent || "").replace(/\s+/g, " ").trim();
throw new Error(
  [
    `dashboard bundle runtime check timed out after ${timeoutMs}ms`,
    `missing text: ${expectedTexts.filter((text) => !bodyText.includes(text)).join(", ")}`,
    `websocket URLs: ${socketUrls.join(", ") || "(none)"}`,
    `body preview: ${bodyText.slice(0, 240) || "(empty)"}`,
  ].join("\n"),
);
