import { EventEmitter } from "events";
import fs from "fs";
import os from "os";
import path from "path";
import type { Server } from "http";
import type { AddressInfo } from "net";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { WebSocket, type WebSocketServer } from "ws";
import {
  broadcastSnapshotMessage,
  closeWebSocketServer,
  createCoalescedFetch,
  createDashboardServer,
  dashboardBuildRevision,
  dashboardTaskOutputConnectionOptions,
  dashboardPortFromEnv,
  dashboardRevisionFromEnv,
  safeSendSnapshot,
  startDashboardServer,
  MAX_SNAPSHOT_CONNECTIONS,
  SNAPSHOT_SKIP_BUFFERED_BYTES,
  SNAPSHOT_TERMINATE_BUFFERED_BYTES,
  WEBSOCKET_MAX_PAYLOAD_BYTES,
  type DashboardServerInstance,
} from "./index.js";
import { WorkerDiscoveryPartialError } from "./workers.js";
import type { DashboardMessage } from "./types.js";

const originalToken = process.env.DASHBOARD_TOKEN;
const originalAllowedOrigins = process.env.DASHBOARD_ALLOWED_ORIGINS;
const originalRedisPrefixes = process.env.DASHBOARD_REDIS_PREFIXES;
const originalBuildRevision = process.env.ORCEST_BUILD_REVISION;

type TestServer = {
  instance: DashboardServerInstance;
  port: number;
  root: string;
  healthCheck: ReturnType<typeof vi.fn>;
  discoverWorkers: ReturnType<typeof vi.fn>;
  buildDashboardMessage: ReturnType<typeof vi.fn>;
  redisQuit: ReturnType<typeof vi.fn>;
};

/**
 * A socket whose `emit` has real EventEmitter semantics: emitting "error" with
 * no registered listener throws, exactly like ws does for protocol-level frame
 * errors.
 */
class EmitterWebSocket extends EventEmitter {
  readyState: WebSocket["readyState"] = WebSocket.OPEN;
  bufferedAmount = 0;
  closes: Array<{ code?: number; reason?: string }> = [];

  send(): void {}

  close(code?: number, reason?: string): void {
    if (this.readyState === WebSocket.CLOSED) return;
    this.readyState = WebSocket.CLOSED;
    this.closes.push({ code, reason });
    this.emit("close", code, reason);
  }

  terminate(): void {
    if (this.readyState === WebSocket.CLOSED) return;
    this.readyState = WebSocket.CLOSED;
    this.emit("close");
  }

  ping(): void {}
}

class FakeSnapshotWebSocket {
  readyState: WebSocket["readyState"] = WebSocket.OPEN;
  bufferedAmount = 0;
  sent: string[] = [];
  closes: Array<{ code?: number; reason?: string }> = [];
  pings = 0;
  terminated = false;
  throwOnSend = false;
  private listeners: Record<string, Array<(...args: unknown[]) => void>> = {};

  send(data: string): void {
    if (this.throwOnSend) throw new Error("send failed");
    this.sent.push(data);
  }

  close(code?: number, reason?: string): void {
    if (this.readyState === WebSocket.CLOSED) return;
    this.readyState = WebSocket.CLOSED;
    this.closes.push({ code, reason });
    this.emit("close", code, reason);
  }

  terminate(): void {
    if (this.readyState === WebSocket.CLOSED) return;
    this.terminated = true;
    this.readyState = WebSocket.CLOSED;
    this.emit("close");
  }

  ping(): void {
    this.pings++;
  }

  on(event: string, listener: (...args: unknown[]) => void): this {
    this.listeners[event] = this.listeners[event] || [];
    this.listeners[event].push(listener);
    return this;
  }

  emit(event: string, ...args: unknown[]): void {
    for (const listener of this.listeners[event] || []) {
      listener(...args);
    }
  }

  messages(): unknown[] {
    return this.sent.map((message) => JSON.parse(message));
  }
}

describe("dashboardPortFromEnv", () => {
  it("accepts numeric dashboard ports including zero", () => {
    expect(dashboardPortFromEnv("8081")).toBe(8081);
    expect(dashboardPortFromEnv(" 0 ")).toBe(0);
  });

  it("falls back for malformed or out-of-range dashboard ports", () => {
    expect(dashboardPortFromEnv(undefined)).toBe(8080);
    expect(dashboardPortFromEnv("8080abc")).toBe(8080);
    expect(dashboardPortFromEnv("1.5")).toBe(8080);
    expect(dashboardPortFromEnv("65536")).toBe(8080);
  });

  it("logs the actual bound port when configured with an ephemeral port", async () => {
    const root = createDistRoot();
    const logInfo = vi.fn();
    const instance = startDashboardServer({
      port: 0,
      cwd: root,
      snapshotRefreshIntervalMs: 60_000,
      deps: {
        healthCheck: vi.fn(),
        discoverWorkers: vi.fn(),
        buildDashboardMessage: vi.fn(),
        redisQuit: vi.fn().mockResolvedValue(undefined),
        logInfo,
        logError: vi.fn(),
      },
    });

    try {
      await new Promise<void>((resolve) => instance.server.once("listening", resolve));
      const address = instance.server.address() as AddressInfo;

      expect(address.port).toBeGreaterThan(0);
      expect(logInfo).toHaveBeenCalledWith(
        `Orcest dashboard listening on http://127.0.0.1:${address.port}`,
      );
      expect(logInfo).not.toHaveBeenCalledWith(
        "Orcest dashboard listening on http://127.0.0.1:0",
      );
    } finally {
      await instance.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
});

describe("dashboardRevisionFromEnv", () => {
  it("accepts only exact clean hexadecimal revisions", () => {
    expect(dashboardRevisionFromEnv(" ABCDEF123 ")).toBe("abcdef123");
    expect(dashboardRevisionFromEnv("abcdef123-dirty")).toBe("unknown");
    expect(dashboardRevisionFromEnv("latest")).toBe("unknown");
  });

  it("prefers the immutable image revision file over runtime environment", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "orcest-dashboard-revision-"));
    const revisionPath = path.join(root, "revision");
    fs.writeFileSync(revisionPath, `${"a".repeat(40)}\n`);
    try {
      expect(dashboardBuildRevision(revisionPath, "b".repeat(40))).toBe("a".repeat(40));
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it("fails closed when an image revision file exists but is invalid", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "orcest-dashboard-revision-"));
    const revisionPath = path.join(root, "revision");
    fs.writeFileSync(revisionPath, "unknown\n");
    try {
      expect(dashboardBuildRevision(revisionPath, "b".repeat(40))).toBe("unknown");
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
});

describe("dashboardTaskOutputConnectionOptions", () => {
  it("inherits the server ping interval unless task output overrides it", () => {
    expect(dashboardTaskOutputConnectionOptions(
      { pollIntervalMs: 25 },
      3,
      8080,
      30000,
    )).toMatchObject({
      activeConnections: 3,
      port: 8080,
      pollIntervalMs: 25,
      pingIntervalMs: 30000,
    });

    expect(dashboardTaskOutputConnectionOptions(
      { pollIntervalMs: 25, pingIntervalMs: 1000, activeConnections: 99, port: 9999 },
      4,
      8081,
      30000,
    )).toMatchObject({
      activeConnections: 4,
      port: 8081,
      pollIntervalMs: 25,
      pingIntervalMs: 1000,
    });
  });
});

async function listen(server: Server): Promise<number> {
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      server.off("error", reject);
      resolve();
    });
  });

  const address = server.address() as AddressInfo;
  return address.port;
}

function createDistRoot(): string {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "orcest-dashboard-http-"));
  const dist = path.join(root, "dist");
  fs.mkdirSync(path.join(dist, "assets"), { recursive: true });
  fs.writeFileSync(
    path.join(dist, "index.html"),
    [
      "<!doctype html>",
      "<title>Orcest Dashboard</title>",
      '<div id="root"></div>',
      '<link rel="stylesheet" href="/assets/index-test.css">',
      '<script type="module" src="/assets/index-test.js"></script>',
    ].join(""),
    "utf-8",
  );
  fs.writeFileSync(
    path.join(dist, "assets", "index-test.js"),
    "console.log('dashboard asset');\n",
    "utf-8",
  );
  fs.writeFileSync(
    path.join(dist, "assets", "index-test.css"),
    "body { color: rgb(17, 24, 39); }\n",
    "utf-8",
  );
  return root;
}

async function openDashboardHttpServer(): Promise<TestServer> {
  const root = createDistRoot();
  const healthCheck = vi.fn().mockResolvedValue(true);
  const discoverWorkers = vi.fn().mockResolvedValue(["worker-1"]);
  const buildDashboardMessage = vi.fn();
  const redisQuit = vi.fn().mockResolvedValue(undefined);
  const instance = createDashboardServer({
    port: 0,
    cwd: root,
    pingIntervalMs: 60_000,
    snapshotRefreshIntervalMs: 60_000,
    deps: {
      healthCheck,
      discoverWorkers,
      buildDashboardMessage,
      redisQuit,
      logInfo: vi.fn(),
      logError: vi.fn(),
    },
  });
  const port = await listen(instance.server);
  return { instance, port, root, healthCheck, discoverWorkers, buildDashboardMessage, redisQuit };
}

async function closeDashboardHttpServer(testServer: TestServer): Promise<void> {
  try {
    await testServer.instance.close();
  } finally {
    fs.rmSync(testServer.root, { recursive: true, force: true });
  }
}

async function flushPromises(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
}

async function withTimeout<T>(
  promise: Promise<T>,
  timeoutMs: number,
  message: string,
): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(() => reject(new Error(message)), timeoutMs);
  });
  try {
    return await Promise.race([promise, timeout]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

function waitForOpen(ws: WebSocket): Promise<void> {
  return withTimeout(
    new Promise<void>((resolve, reject) => {
      ws.once("open", () => resolve());
      ws.once("error", reject);
    }),
    2000,
    "Timed out waiting for snapshot WebSocket open",
  );
}

function waitForClose(ws: WebSocket): Promise<{ code: number; reason: string }> {
  return withTimeout(
    new Promise<{ code: number; reason: string }>((resolve) => {
      ws.once("close", (code, reason) => resolve({ code, reason: reason.toString() }));
    }),
    2000,
    "Timed out waiting for snapshot WebSocket close",
  );
}

beforeEach(() => {
  delete process.env.ORCEST_BUILD_REVISION;
});

afterEach(() => {
  if (originalToken === undefined) delete process.env.DASHBOARD_TOKEN;
  else process.env.DASHBOARD_TOKEN = originalToken;
  if (originalAllowedOrigins === undefined) delete process.env.DASHBOARD_ALLOWED_ORIGINS;
  else process.env.DASHBOARD_ALLOWED_ORIGINS = originalAllowedOrigins;
  if (originalRedisPrefixes === undefined) delete process.env.DASHBOARD_REDIS_PREFIXES;
  else process.env.DASHBOARD_REDIS_PREFIXES = originalRedisPrefixes;
  if (originalBuildRevision === undefined) delete process.env.ORCEST_BUILD_REVISION;
  else process.env.ORCEST_BUILD_REVISION = originalBuildRevision;
});

describe("dashboard HTTP integration", () => {
  it("fails fast when DASHBOARD_ALLOWED_ORIGINS contains invalid entries", () => {
    process.env.DASHBOARD_ALLOWED_ORIGINS = "pve-test.lab.prefixa.net";

    expect(() => createDashboardServer({
      port: 0,
      deps: {
        healthCheck: vi.fn(),
        discoverWorkers: vi.fn(),
        buildDashboardMessage: vi.fn(),
        redisQuit: vi.fn(),
        logInfo: vi.fn(),
        logError: vi.fn(),
      },
    })).toThrow(/Invalid DASHBOARD_ALLOWED_ORIGINS entries: pve-test\.lab\.prefixa\.net/);
  });

  it.each(["", "   ", " , , "])(
    "fails fast when DASHBOARD_REDIS_PREFIXES has no usable entries (%j)",
    (configured) => {
      process.env.DASHBOARD_REDIS_PREFIXES = configured;

      expect(() => createDashboardServer({ port: 0 })).toThrow(
        /DASHBOARD_REDIS_PREFIXES must contain at least one prefix/,
      );
    },
  );

  it("keeps health unauthenticated and protects API/static routes with the dashboard cookie", async () => {
    process.env.DASHBOARD_TOKEN = "s3cret";
    const testServer = await openDashboardHttpServer();
    const baseUrl = `http://127.0.0.1:${testServer.port}`;

    try {
      await expect(fetch(`${baseUrl}/api/health`).then((res) => res.json()))
        .resolves.toEqual({ ok: true, revision: "unknown" });
      expect(testServer.healthCheck).not.toHaveBeenCalled();

      await expect(fetch(`${baseUrl}/api/ready`).then((res) => res.json()))
        .resolves.toEqual({ ok: true, redis_ok: true, revision: "unknown" });
      expect(testServer.healthCheck).toHaveBeenCalledTimes(1);

      const unauthorized = await fetch(`${baseUrl}/api/workers`);
      expect(unauthorized.status).toBe(401);
      await expect(unauthorized.json()).resolves.toEqual({ error: "Unauthorized" });
      expect(testServer.discoverWorkers).not.toHaveBeenCalled();

      const bootstrap = await fetch(`${baseUrl}/api/auth/bootstrap?token=s3cret`);
      expect(bootstrap.status).toBe(200);
      await expect(bootstrap.json()).resolves.toEqual({ ok: true });
      expect(bootstrap.headers.get("set-cookie")?.split(";")[0])
        .toBe("orcest_dashboard_token=s3cret");

      const index = await fetch(`${baseUrl}/?token=s3cret`);
      expect(index.status).toBe(200);
      await expect(index.text()).resolves.toContain("Orcest Dashboard");
      const cookie = index.headers.get("set-cookie")?.split(";")[0];
      expect(cookie).toBe("orcest_dashboard_token=s3cret");

      const workers = await fetch(`${baseUrl}/api/workers`, {
        headers: { cookie: cookie || "" },
      });
      expect(workers.status).toBe(200);
      await expect(workers.json()).resolves.toEqual({ workers: ["worker-1"] });
      expect(testServer.discoverWorkers).toHaveBeenCalledTimes(1);

      const missingApi = await fetch(`${baseUrl}/api/missing`, {
        headers: { cookie: cookie || "" },
      });
      expect(missingApi.status).toBe(404);
      await expect(missingApi.json()).resolves.toEqual({ error: "Not Found" });
    } finally {
      await closeDashboardHttpServer(testServer);
      await testServer.instance.close();
    }
    expect(testServer.redisQuit).toHaveBeenCalledTimes(1);
  });

  it("serves built assets without sending the SPA fallback for missing asset files", async () => {
    process.env.DASHBOARD_TOKEN = "s3cret";
    const testServer = await openDashboardHttpServer();
    const baseUrl = `http://127.0.0.1:${testServer.port}`;

    try {
      const unauthorizedAsset = await fetch(`${baseUrl}/assets/index-test.js`);
      expect(unauthorizedAsset.status).toBe(401);
      await expect(unauthorizedAsset.json()).resolves.toEqual({ error: "Unauthorized" });

      const index = await fetch(`${baseUrl}/?token=s3cret`);
      const cookie = index.headers.get("set-cookie")?.split(";")[0] || "";

      const jsAsset = await fetch(`${baseUrl}/assets/index-test.js`, {
        headers: { cookie },
      });
      expect(jsAsset.status).toBe(200);
      expect(jsAsset.headers.get("content-type")).toContain("javascript");
      await expect(jsAsset.text()).resolves.toContain("dashboard asset");

      const cssAsset = await fetch(`${baseUrl}/assets/index-test.css`, {
        headers: { cookie },
      });
      expect(cssAsset.status).toBe(200);
      expect(cssAsset.headers.get("content-type")).toContain("text/css");

      const missingAsset = await fetch(`${baseUrl}/assets/missing.js`, {
        headers: { cookie },
      });
      expect(missingAsset.status).toBe(404);
      expect(missingAsset.headers.get("content-type")).not.toContain("text/html");
      await expect(missingAsset.text()).resolves.not.toContain("Orcest Dashboard");

      const deepLink = await fetch(`${baseUrl}/work/results`, {
        headers: { cookie },
      });
      expect(deepLink.status).toBe(200);
      expect(deepLink.headers.get("content-type")).toContain("text/html");
      const deepLinkHtml = await deepLink.text();
      expect(deepLinkHtml).toContain("Orcest Dashboard");

      const deepLinkAssets = [...deepLinkHtml.matchAll(/<(?:script|link)\b[^>]*(?:src|href)="([^"]+)"/gi)]
        .map((match) => match[1]);
      expect(deepLinkAssets).toEqual(expect.arrayContaining([
        "/assets/index-test.css",
        "/assets/index-test.js",
      ]));
      for (const assetPath of deepLinkAssets) {
        const asset = await fetch(new URL(assetPath, `${baseUrl}/work/results`).toString(), {
          headers: { cookie },
        });
        expect(asset.status).toBe(200);
        const contentType = asset.headers.get("content-type") || "";
        if (assetPath.endsWith(".js")) expect(contentType).toContain("javascript");
        if (assetPath.endsWith(".css")) expect(contentType).toContain("text/css");
      }
    } finally {
      await closeDashboardHttpServer(testServer);
    }
  });

  it("keeps process health green while Redis readiness is unavailable", async () => {
    const root = createDistRoot();
    const instance = createDashboardServer({
      port: 0,
      cwd: root,
      pingIntervalMs: 60_000,
      snapshotRefreshIntervalMs: 60_000,
      deps: {
        healthCheck: vi.fn().mockResolvedValue(false),
        discoverWorkers: vi.fn().mockResolvedValue([]),
        buildDashboardMessage: vi.fn(),
        redisQuit: vi.fn().mockResolvedValue(undefined),
        logInfo: vi.fn(),
        logError: vi.fn(),
      },
    });
    const port = await listen(instance.server);
    const baseUrl = `http://127.0.0.1:${port}`;

    try {
      const health = await fetch(`${baseUrl}/api/health`);
      expect(health.status).toBe(200);
      await expect(health.json()).resolves.toEqual({ ok: true, revision: "unknown" });

      const ready = await fetch(`${baseUrl}/api/ready`);
      expect(ready.status).toBe(503);
      await expect(ready.json()).resolves.toEqual({
        ok: false,
        redis_ok: false,
        revision: "unknown",
      });
    } finally {
      await instance.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it("returns partial workers when worker discovery is degraded", async () => {
    process.env.DASHBOARD_TOKEN = "s3cret";
    const testServer = await openDashboardHttpServer();
    const baseUrl = `http://127.0.0.1:${testServer.port}`;
    testServer.discoverWorkers.mockRejectedValueOnce(
      new WorkerDiscoveryPartialError("one output stream failed", ["worker-ok"]),
    );

    try {
      const index = await fetch(`${baseUrl}/?token=s3cret`);
      const cookie = index.headers.get("set-cookie")?.split(";")[0] || "";
      const workers = await fetch(`${baseUrl}/api/workers`, {
        headers: { cookie },
      });

      expect(workers.status).toBe(206);
      await expect(workers.json()).resolves.toEqual({
        error: "Worker discovery partially unavailable",
        workers: ["worker-ok"],
        degraded_sections: ["worker discovery"],
      });
    } finally {
      await closeDashboardHttpServer(testServer);
    }
  });

  it("serves the dashboard snapshot message over authenticated REST", async () => {
    process.env.DASHBOARD_TOKEN = "s3cret";
    const testServer = await openDashboardHttpServer();
    const baseUrl = `http://127.0.0.1:${testServer.port}`;
    const message: DashboardMessage = {
      snapshot: {
        redis_ok: true,
        fetched_at: "2026-06-30T00:00:00.000Z",
        queue_depths: { "orcest:tasks:issue:codex": 3 },
        results_depth: 2,
        dead_letter_count: 1,
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
          pending_task_ttl_seconds: 16500,
          lock_ttl_seconds: 180,
        },
      },
      stuck_tasks: [],
      workers: ["worker-1"],
    };
    testServer.buildDashboardMessage.mockResolvedValue(message);

    try {
      const unauthorized = await fetch(`${baseUrl}/api/snapshot`);
      expect(unauthorized.status).toBe(401);
      await expect(unauthorized.json()).resolves.toEqual({ error: "Unauthorized" });
      expect(testServer.buildDashboardMessage).not.toHaveBeenCalled();

      const snapshot = await fetch(`${baseUrl}/api/snapshot?token=s3cret`);
      expect(snapshot.status).toBe(200);
      expect(snapshot.headers.get("set-cookie")?.split(";")[0])
        .toBe("orcest_dashboard_token=s3cret");
      await expect(snapshot.json()).resolves.toEqual(message);
      expect(testServer.buildDashboardMessage).toHaveBeenCalledTimes(1);
    } finally {
      await closeDashboardHttpServer(testServer);
    }
  });

  it("returns a clean snapshot REST failure when snapshot building throws", async () => {
    process.env.DASHBOARD_TOKEN = "s3cret";
    const root = createDistRoot();
    const logError = vi.fn();
    const buildDashboardMessage = vi.fn().mockRejectedValue(new Error("snapshot failed"));
    const instance = createDashboardServer({
      port: 0,
      cwd: root,
      pingIntervalMs: 60_000,
      snapshotRefreshIntervalMs: 60_000,
      deps: {
        healthCheck: vi.fn().mockResolvedValue(true),
        discoverWorkers: vi.fn().mockResolvedValue([]),
        buildDashboardMessage,
        redisQuit: vi.fn().mockResolvedValue(undefined),
        logInfo: vi.fn(),
        logError,
      },
    });
    const port = await listen(instance.server);

    try {
      const response = await fetch(`http://127.0.0.1:${port}/api/snapshot?token=s3cret`);

      expect(response.status).toBe(503);
      await expect(response.json()).resolves.toEqual({ error: "Snapshot unavailable" });
      expect(buildDashboardMessage).toHaveBeenCalledTimes(1);
      expect(logError).toHaveBeenCalledWith(
        "Error building dashboard snapshot:",
        expect.any(Error),
      );
    } finally {
      await instance.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it("returns a clean readiness failure when the health check throws", async () => {
    const root = createDistRoot();
    const healthCheck = vi.fn().mockRejectedValue(new Error("redis offline"));
    const logError = vi.fn();
    const instance = createDashboardServer({
      port: 0,
      cwd: root,
      pingIntervalMs: 60_000,
      snapshotRefreshIntervalMs: 60_000,
      deps: {
        healthCheck,
        discoverWorkers: vi.fn().mockResolvedValue([]),
        buildDashboardMessage: vi.fn(),
        redisQuit: vi.fn().mockResolvedValue(undefined),
        logInfo: vi.fn(),
        logError,
      },
    });
    const port = await listen(instance.server);

    try {
      const ready = await fetch(`http://127.0.0.1:${port}/api/ready`);
      expect(ready.status).toBe(503);
      await expect(ready.json()).resolves.toEqual({
        ok: false,
        redis_ok: false,
        revision: "unknown",
      });
      expect(healthCheck).toHaveBeenCalledTimes(1);
      expect(logError).toHaveBeenCalledWith(
        "Dashboard readiness check failed:",
        expect.any(Error),
      );
    } finally {
      await instance.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it("allows close to retry Redis cleanup after a Redis quit failure", async () => {
    const root = createDistRoot();
    const redisQuit = vi.fn()
      .mockRejectedValueOnce(new Error("quit failed"))
      .mockResolvedValueOnce(undefined);
    const instance = createDashboardServer({
      port: 0,
      cwd: root,
      pingIntervalMs: 60_000,
      snapshotRefreshIntervalMs: 60_000,
      deps: {
        healthCheck: vi.fn().mockResolvedValue(true),
        discoverWorkers: vi.fn().mockResolvedValue([]),
        buildDashboardMessage: vi.fn(),
        redisQuit,
        logInfo: vi.fn(),
        logError: vi.fn(),
      },
    });
    await listen(instance.server);

    try {
      await expect(instance.close()).rejects.toThrow("quit failed");
      await expect(instance.close()).resolves.toBeUndefined();
      expect(redisQuit).toHaveBeenCalledTimes(2);
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it("terminates WebSocket clients that do not complete the close handshake", async () => {
    vi.useFakeTimers();
    let closeCallback: ((err?: Error) => void) | undefined;
    let readyState: number = WebSocket.OPEN;
    const client = {
      get readyState() {
        return readyState;
      },
      close: vi.fn(),
      terminate: vi.fn(() => {
        readyState = WebSocket.CLOSED;
        closeCallback?.();
      }),
    };
    const wss = {
      clients: new Set([client]),
      close: vi.fn((callback?: (err?: Error) => void) => {
        closeCallback = callback;
      }),
    } as unknown as WebSocketServer;

    try {
      const closePromise = closeWebSocketServer(wss, 25);
      await Promise.resolve();

      expect(client.close).toHaveBeenCalledWith(1001, "Server shutting down");
      expect(client.terminate).not.toHaveBeenCalled();

      await vi.advanceTimersByTimeAsync(25);
      await closePromise;

      expect(client.terminate).toHaveBeenCalledTimes(1);
    } finally {
      vi.useRealTimers();
    }
  });

  it("keeps snapshot broadcasts isolated when one client send fails", async () => {
    const logError = vi.fn();
    const brokenClient = new FakeSnapshotWebSocket();
    brokenClient.throwOnSend = true;
    const healthyClient = new FakeSnapshotWebSocket();
    const message = JSON.stringify({
      snapshot: { redis_ok: true, fetched_at: "2026-06-25T00:00:00Z" },
      stuck_tasks: [],
      workers: [],
    });

    broadcastSnapshotMessage(
      [brokenClient, healthyClient],
      message,
      logError,
    );

    expect(brokenClient.terminated).toBe(true);
    expect(healthyClient.messages()).toEqual([{
      snapshot: { redis_ok: true, fetched_at: "2026-06-25T00:00:00Z" },
      stuck_tasks: [],
      workers: [],
    }]);
    expect(logError).toHaveBeenCalledWith("Snapshot WS send failed:", expect.any(Error));
  });

  it("terminates snapshot WebSockets that miss a heartbeat pong", async () => {
    vi.useFakeTimers();
    const root = createDistRoot();
    const instance = createDashboardServer({
      port: 0,
      cwd: root,
      pingIntervalMs: 10,
      snapshotRefreshIntervalMs: 60_000,
      deps: {
        healthCheck: vi.fn().mockResolvedValue(true),
        discoverWorkers: vi.fn().mockResolvedValue([]),
        buildDashboardMessage: vi.fn().mockResolvedValue({
          snapshot: { redis_ok: true },
          stuck_tasks: [],
          workers: [],
        }),
        redisQuit: vi.fn().mockResolvedValue(undefined),
        logInfo: vi.fn(),
        logError: vi.fn(),
      },
    });
    const ws = new FakeSnapshotWebSocket();

    try {
      instance.snapshotWss.emit("connection", ws as unknown as WebSocket, {});
      await flushPromises();

      await vi.advanceTimersByTimeAsync(10);
      expect(ws.pings).toBe(1);
      expect(ws.terminated).toBe(false);

      await vi.advanceTimersByTimeAsync(10);
      expect(ws.terminated).toBe(true);
    } finally {
      await instance.close();
      fs.rmSync(root, { recursive: true, force: true });
      vi.useRealTimers();
    }
  });
});

describe("dashboard WebSocket resilience", () => {
  it("survives a protocol error on a snapshot connection rejected during shutdown", async () => {
    const testServer = await openDashboardHttpServer();
    testServer.buildDashboardMessage.mockResolvedValue({
      snapshot: { redis_ok: true },
      stuck_tasks: [],
      workers: [],
    });

    try {
      // close() flips `closing` synchronously, so this connection takes the
      // early `ws.close(1001)` return in the handler.
      const closePromise = testServer.instance.close();
      const ws = new EmitterWebSocket();
      testServer.instance.snapshotWss.emit("connection", ws as unknown as WebSocket, {});

      expect(ws.closes).toEqual([{ code: 1001, reason: "Server shutting down" }]);
      // ws emits this for any malformed frame; with no listener it throws and
      // there is no process-level uncaughtException handler in the dashboard.
      expect(() => ws.emit("error", new Error("Invalid WebSocket frame"))).not.toThrow();

      await closePromise;
    } finally {
      fs.rmSync(testServer.root, { recursive: true, force: true });
    }
  });

  it("survives a protocol error on a task-output connection rejected during shutdown", async () => {
    const testServer = await openDashboardHttpServer();

    try {
      const closePromise = testServer.instance.close();
      const ws = new EmitterWebSocket();
      testServer.instance.taskOutputWss.emit(
        "connection",
        ws as unknown as WebSocket,
        { url: "/ws/task-output?worker_id=worker-1" },
      );

      expect(ws.closes).toEqual([{ code: 1001, reason: "Server shutting down" }]);
      expect(() => ws.emit("error", new Error("Invalid WebSocket frame"))).not.toThrow();

      await closePromise;
    } finally {
      fs.rmSync(testServer.root, { recursive: true, force: true });
    }
  });

  it("caps WebSocket frame size on both servers", async () => {
    const testServer = await openDashboardHttpServer();

    try {
      // ws defaults to a 100 MiB maxPayload; neither server reads client frames.
      expect(testServer.instance.snapshotWss.options.maxPayload)
        .toBe(WEBSOCKET_MAX_PAYLOAD_BYTES);
      expect(testServer.instance.taskOutputWss.options.maxPayload)
        .toBe(WEBSOCKET_MAX_PAYLOAD_BYTES);
      expect(WEBSOCKET_MAX_PAYLOAD_BYTES).toBeLessThan(100 * 1024 * 1024);
    } finally {
      await closeDashboardHttpServer(testServer);
    }
  });

  it("rejects snapshot connections past the connection cap", async () => {
    process.env.DASHBOARD_TOKEN = "s3cret";
    const root = createDistRoot();
    const instance = createDashboardServer({
      port: 0,
      cwd: root,
      pingIntervalMs: 60_000,
      snapshotRefreshIntervalMs: 60_000,
      maxSnapshotConnections: 1,
      deps: {
        healthCheck: vi.fn().mockResolvedValue(true),
        discoverWorkers: vi.fn().mockResolvedValue([]),
        buildDashboardMessage: vi.fn().mockResolvedValue({
          snapshot: { redis_ok: true },
          stuck_tasks: [],
          workers: [],
        }),
        redisQuit: vi.fn().mockResolvedValue(undefined),
        logInfo: vi.fn(),
        logError: vi.fn(),
      },
    });
    const port = await listen(instance.server);
    const first = new WebSocket(`ws://127.0.0.1:${port}/ws/snapshot?token=s3cret`);

    try {
      await waitForOpen(first);

      const second = new WebSocket(`ws://127.0.0.1:${port}/ws/snapshot?token=s3cret`);
      await expect(waitForClose(second)).resolves.toEqual({
        code: 1013,
        reason: "Too many connections",
      });
      expect(MAX_SNAPSHOT_CONNECTIONS).toBeGreaterThan(1);
    } finally {
      if (first.readyState === WebSocket.OPEN) first.close();
      await instance.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it("completes graceful shutdown while a real WebSocket client is connected", async () => {
    process.env.DASHBOARD_TOKEN = "s3cret";
    const testServer = await openDashboardHttpServer();
    testServer.buildDashboardMessage.mockResolvedValue({
      snapshot: { redis_ok: true },
      stuck_tasks: [],
      workers: [],
    });
    const ws = new WebSocket(`ws://127.0.0.1:${testServer.port}/ws/snapshot?token=s3cret`);

    try {
      await waitForOpen(ws);
      const closed = waitForClose(ws);

      // Upgraded sockets stay counted in net.Server._connections, so awaiting
      // the HTTP server before the WebSocket servers deadlocked shutdown and
      // the SIGTERM handler force-exited the container on every deploy.
      await withTimeout(
        testServer.instance.close(),
        3000,
        "close() did not resolve with a WebSocket client connected",
      );

      await expect(closed).resolves.toMatchObject({ code: 1001 });
      expect(testServer.redisQuit).toHaveBeenCalledTimes(1);
    } finally {
      if (ws.readyState === WebSocket.OPEN) ws.close();
      fs.rmSync(testServer.root, { recursive: true, force: true });
    }
  });
});

describe("snapshot send backpressure", () => {
  it("drops frames for a lagging client and terminates a client that never drains", () => {
    const logError = vi.fn();
    const lagging = new FakeSnapshotWebSocket();
    lagging.bufferedAmount = SNAPSHOT_SKIP_BUFFERED_BYTES + 1;
    const stalled = new FakeSnapshotWebSocket();
    stalled.bufferedAmount = SNAPSHOT_TERMINATE_BUFFERED_BYTES + 1;
    const healthy = new FakeSnapshotWebSocket();

    broadcastSnapshotMessage([lagging, stalled, healthy], "{}", logError);

    // Snapshots are idempotent full-state payloads, so dropping is safe.
    expect(lagging.sent).toEqual([]);
    expect(lagging.terminated).toBe(false);
    expect(stalled.sent).toEqual([]);
    expect(stalled.terminated).toBe(true);
    expect(healthy.sent).toEqual(["{}"]);
    expect(logError).toHaveBeenCalledWith(
      "Snapshot WS backpressure limit exceeded; terminating client:",
      SNAPSHOT_TERMINATE_BUFFERED_BYTES + 1,
    );
  });

  it("still sends to a client with a small pending send queue", () => {
    const ws = new FakeSnapshotWebSocket();
    ws.bufferedAmount = 1024;

    expect(safeSendSnapshot(ws, "{}", vi.fn())).toBe(true);
    expect(ws.sent).toEqual(["{}"]);
  });
});

describe("createCoalescedFetch", () => {
  it("shares one in-flight run and reuses the result for the cache window", async () => {
    let resolveFetch!: (value: number) => void;
    const fetcher = vi.fn(() => new Promise<number>((resolve) => {
      resolveFetch = resolve;
    }));
    let now = 1000;
    const coalesced = createCoalescedFetch(fetcher, 1000, () => now);

    const first = coalesced();
    const second = coalesced();
    expect(fetcher).toHaveBeenCalledTimes(1);

    resolveFetch(7);
    await expect(first).resolves.toBe(7);
    await expect(second).resolves.toBe(7);

    now = 1500;
    await expect(coalesced()).resolves.toBe(7);
    expect(fetcher).toHaveBeenCalledTimes(1);

    now = 2001;
    void coalesced();
    expect(fetcher).toHaveBeenCalledTimes(2);
    resolveFetch(9);
  });

  it("does not cache failures", async () => {
    const fetcher = vi.fn()
      .mockRejectedValueOnce(new Error("redis down"))
      .mockResolvedValueOnce("ok");
    const coalesced = createCoalescedFetch(fetcher, 60_000);

    await expect(coalesced()).rejects.toThrow("redis down");
    await expect(coalesced()).resolves.toBe("ok");
    expect(fetcher).toHaveBeenCalledTimes(2);
  });

  it("recovers when the fetcher throws synchronously", async () => {
    // A synchronous throw reaches the cleanup before the in-flight promise has
    // been assigned. If the identity check is not deferred past that
    // assignment, the rejected promise is retained and served to every future
    // caller forever -- the endpoint 503s permanently and the fetcher is never
    // called again. `mockRejectedValueOnce` cannot catch this: it rejects
    // asynchronously and so exercises the wrong path.
    let firstCall = true;
    const fetcher = vi.fn(() => {
      if (firstCall) {
        firstCall = false;
        throw new Error("synchronous boom");
      }
      return Promise.resolve("ok");
    });
    const coalesced = createCoalescedFetch(fetcher, 60_000);

    await expect(coalesced()).rejects.toThrow("synchronous boom");
    await expect(coalesced()).resolves.toBe("ok");
    expect(fetcher).toHaveBeenCalledTimes(2);
  });
});

describe("REST snapshot coalescing", () => {
  it("collapses concurrent snapshot and worker reads onto one Redis sweep", async () => {
    process.env.DASHBOARD_TOKEN = "s3cret";
    const testServer = await openDashboardHttpServer();
    const baseUrl = `http://127.0.0.1:${testServer.port}`;
    const message: DashboardMessage = {
      snapshot: {
        redis_ok: true,
        fetched_at: "2026-06-25T00:00:00Z",
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
          pending_task_ttl_seconds: 16500,
          lock_ttl_seconds: 180,
        },
      },
      stuck_tasks: [],
      workers: [],
    };
    testServer.buildDashboardMessage.mockResolvedValue(message);

    try {
      const responses = await Promise.all([
        fetch(`${baseUrl}/api/snapshot?token=s3cret`),
        fetch(`${baseUrl}/api/snapshot?token=s3cret`),
        fetch(`${baseUrl}/api/snapshot?token=s3cret`),
      ]);
      for (const response of responses) {
        expect(response.status).toBe(200);
        await expect(response.json()).resolves.toEqual(message);
      }
      // Without coalescing each GET queued its own full-keyspace sweep on the
      // single shared ioredis connection.
      expect(testServer.buildDashboardMessage).toHaveBeenCalledTimes(1);

      const workerResponses = await Promise.all([
        fetch(`${baseUrl}/api/workers?token=s3cret`),
        fetch(`${baseUrl}/api/workers?token=s3cret`),
      ]);
      for (const response of workerResponses) {
        expect(response.status).toBe(200);
        await expect(response.json()).resolves.toEqual({ workers: ["worker-1"] });
      }
      expect(testServer.discoverWorkers).toHaveBeenCalledTimes(1);
    } finally {
      await closeDashboardHttpServer(testServer);
    }
  });
});
