import fs from "fs";
import os from "os";
import path from "path";
import type { Server } from "http";
import type { AddressInfo } from "net";
import { afterEach, describe, expect, it, vi } from "vitest";
import { WebSocket, type WebSocketServer } from "ws";
import {
  broadcastSnapshotMessage,
  closeWebSocketServer,
  createDashboardServer,
  dashboardTaskOutputConnectionOptions,
  dashboardPortFromEnv,
  type DashboardServerInstance,
} from "./index.js";
import { WorkerDiscoveryPartialError } from "./workers.js";
import type { DashboardMessage } from "./types.js";

const originalToken = process.env.DASHBOARD_TOKEN;
const originalAllowedOrigins = process.env.DASHBOARD_ALLOWED_ORIGINS;

type TestServer = {
  instance: DashboardServerInstance;
  port: number;
  root: string;
  healthCheck: ReturnType<typeof vi.fn>;
  discoverWorkers: ReturnType<typeof vi.fn>;
  buildDashboardMessage: ReturnType<typeof vi.fn>;
  redisQuit: ReturnType<typeof vi.fn>;
};

class FakeSnapshotWebSocket {
  readyState: WebSocket["readyState"] = WebSocket.OPEN;
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

afterEach(() => {
  if (originalToken === undefined) delete process.env.DASHBOARD_TOKEN;
  else process.env.DASHBOARD_TOKEN = originalToken;
  if (originalAllowedOrigins === undefined) delete process.env.DASHBOARD_ALLOWED_ORIGINS;
  else process.env.DASHBOARD_ALLOWED_ORIGINS = originalAllowedOrigins;
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

  it("keeps health unauthenticated and protects API/static routes with the dashboard cookie", async () => {
    process.env.DASHBOARD_TOKEN = "s3cret";
    const testServer = await openDashboardHttpServer();
    const baseUrl = `http://127.0.0.1:${testServer.port}`;

    try {
      await expect(fetch(`${baseUrl}/api/health`).then((res) => res.json()))
        .resolves.toEqual({ ok: true });
      expect(testServer.healthCheck).not.toHaveBeenCalled();

      await expect(fetch(`${baseUrl}/api/ready`).then((res) => res.json()))
        .resolves.toEqual({ ok: true, redis_ok: true });
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
      await expect(health.json()).resolves.toEqual({ ok: true });

      const ready = await fetch(`${baseUrl}/api/ready`);
      expect(ready.status).toBe(503);
      await expect(ready.json()).resolves.toEqual({ ok: false, redis_ok: false });
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
      await expect(ready.json()).resolves.toEqual({ ok: false, redis_ok: false });
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
