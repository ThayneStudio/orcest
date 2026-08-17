import type { Server } from "http";
import type { AddressInfo } from "net";
import { afterEach, describe, expect, it, vi } from "vitest";
import { WebSocket } from "ws";
import { createDashboardServer, type DashboardServerInstance } from "./index.js";
import type { DashboardMessage, SystemSnapshot } from "./types.js";

const originalToken = process.env.DASHBOARD_TOKEN;
const originalAllowedOrigins = process.env.DASHBOARD_ALLOWED_ORIGINS;

type TestServer = {
  instance: DashboardServerInstance;
  port: number;
  buildDashboardMessage: ReturnType<typeof vi.fn>;
  logError: ReturnType<typeof vi.fn>;
};

function deferred<T>(): {
  promise: Promise<T>;
  resolve: (value: T) => void;
  reject: (reason?: unknown) => void;
} {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((promiseResolve, promiseReject) => {
    resolve = promiseResolve;
    reject = promiseReject;
  });
  return { promise, resolve, reject };
}

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

async function openSnapshotServer(message: DashboardMessage): Promise<TestServer> {
  const buildDashboardMessage = vi.fn().mockResolvedValue(message);
  const logError = vi.fn();
  const instance = createDashboardServer({
    port: 0,
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

  return { instance, port, buildDashboardMessage, logError };
}

function snapshot(overrides: Partial<SystemSnapshot> = {}): SystemSnapshot {
  return {
    redis_ok: true,
    fetched_at: "2026-06-20T00:00:00Z",
    queue_depths: { "tasks:claude": 2 },
    results_depth: 1,
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
    ...overrides,
  };
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

async function readJsonMessage<T>(ws: WebSocket): Promise<T> {
  return withTimeout(new Promise((resolve, reject) => {
    ws.once("message", (data) => {
      resolve(JSON.parse(String(data)) as T);
    });
    ws.once("error", reject);
  }), 1000, "Timed out waiting for snapshot WebSocket message");
}

async function closeWebSocket(ws: WebSocket): Promise<void> {
  if (ws.readyState === WebSocket.CLOSED) return;

  await withTimeout(new Promise<void>((resolve) => {
    ws.once("close", () => resolve());
    ws.close();
  }), 1000, "Timed out waiting for snapshot WebSocket close");
}

function rejectedWebSocketStatus(ws: WebSocket): Promise<number> {
  return withTimeout(new Promise((resolve, reject) => {
    ws.once("unexpected-response", (_request, response) => {
      const status = response.statusCode || 0;
      response.resume();
      resolve(status);
    });
    ws.once("open", () => reject(new Error("WebSocket unexpectedly opened")));
    ws.once("error", reject);
  }), 1000, "Timed out waiting for rejected snapshot WebSocket upgrade");
}

afterEach(() => {
  if (originalToken === undefined) delete process.env.DASHBOARD_TOKEN;
  else process.env.DASHBOARD_TOKEN = originalToken;
  if (originalAllowedOrigins === undefined) delete process.env.DASHBOARD_ALLOWED_ORIGINS;
  else process.env.DASHBOARD_ALLOWED_ORIGINS = originalAllowedOrigins;
});

describe("snapshot WebSocket integration", () => {
  it("sends cached snapshots immediately and refreshes for later clients", async () => {
    process.env.DASHBOARD_TOKEN = "s3cret";
    const message: DashboardMessage = {
      snapshot: snapshot({ degraded_sections: ["recent results"] }),
      stuck_tasks: [{
        prefix: "orcest",
        resource_type: "pr",
        repo: "owner/repo",
        resource_id: "42",
        reason: "lock expired",
        severity: "warning",
      }],
      workers: ["orcest-worker-300"],
    };
    const refreshedMessage: DashboardMessage = {
      snapshot: snapshot({
        fetched_at: "2026-06-20T00:00:02Z",
        queue_depths: { "tasks:claude": 0 },
      }),
      stuck_tasks: [],
      workers: ["orcest-worker-301"],
    };
    const testServer = await openSnapshotServer(message);
    const secondRefresh = deferred<DashboardMessage>();
    let secondRefreshResolved = false;
    testServer.buildDashboardMessage.mockResolvedValueOnce(message);
    testServer.buildDashboardMessage.mockReturnValueOnce(secondRefresh.promise);

    try {
      const first = new WebSocket(
        `ws://127.0.0.1:${testServer.port}/ws/snapshot?token=s3cret`,
        { headers: { Origin: `http://127.0.0.1:${testServer.port}` } },
      );
      await expect(readJsonMessage<DashboardMessage>(first)).resolves.toEqual(message);
      await closeWebSocket(first);

      const second = new WebSocket(
        `ws://127.0.0.1:${testServer.port}/ws/snapshot?token=s3cret`,
        { headers: { Origin: `http://127.0.0.1:${testServer.port}` } },
      );
      await expect(readJsonMessage<DashboardMessage>(second)).resolves.toEqual(message);
      const refreshedRead = readJsonMessage<DashboardMessage>(second);
      secondRefreshResolved = true;
      secondRefresh.resolve(refreshedMessage);
      await expect(refreshedRead).resolves.toEqual(refreshedMessage);
      await closeWebSocket(second);

      expect(testServer.buildDashboardMessage).toHaveBeenCalledTimes(2);
    } finally {
      if (!secondRefreshResolved) secondRefresh.resolve(refreshedMessage);
      await testServer.instance.close();
    }
  });

  it("accepts snapshot upgrades from explicitly allowed browser origins", async () => {
    process.env.DASHBOARD_TOKEN = "s3cret";
    process.env.DASHBOARD_ALLOWED_ORIGINS = "https://ops.example.test";
    const message: DashboardMessage = {
      snapshot: snapshot(),
      stuck_tasks: [],
      workers: [],
    };
    const testServer = await openSnapshotServer(message);

    try {
      const ws = new WebSocket(
        `ws://127.0.0.1:${testServer.port}/ws/snapshot?token=s3cret`,
        { headers: { Origin: "https://ops.example.test" } },
      );

      await expect(readJsonMessage<DashboardMessage>(ws)).resolves.toEqual(message);
      await closeWebSocket(ws);
    } finally {
      await testServer.instance.close();
    }
  });

  it("rejects snapshot upgrades from untrusted browser origins with 403", async () => {
    process.env.DASHBOARD_TOKEN = "s3cret";
    const message: DashboardMessage = {
      snapshot: snapshot(),
      stuck_tasks: [],
      workers: [],
    };
    const testServer = await openSnapshotServer(message);

    try {
      const ws = new WebSocket(
        `ws://127.0.0.1:${testServer.port}/ws/snapshot?token=s3cret`,
        { headers: { Origin: "https://evil.example.test" } },
      );

      await expect(rejectedWebSocketStatus(ws)).resolves.toBe(403);
      expect(testServer.logError).toHaveBeenCalledWith(
        "Dashboard WS upgrade rejected: untrusted origin",
        expect.objectContaining({
          path: "/ws/snapshot",
          origin: "https://evil.example.test",
          host: `127.0.0.1:${testServer.port}`,
        }),
      );
      expect(testServer.buildDashboardMessage).not.toHaveBeenCalled();
    } finally {
      await testServer.instance.close();
    }
  });

  it("waits for an in-flight snapshot refresh before quitting Redis", async () => {
    const message: DashboardMessage = {
      snapshot: snapshot(),
      stuck_tasks: [],
      workers: [],
    };
    const snapshotRefresh = deferred<DashboardMessage>();
    const buildDashboardMessage = vi.fn().mockReturnValue(snapshotRefresh.promise);
    const redisQuit = vi.fn().mockResolvedValue(undefined);
    const instance = createDashboardServer({
      port: 0,
      pingIntervalMs: 60_000,
      snapshotRefreshIntervalMs: 60_000,
      deps: {
        healthCheck: vi.fn().mockResolvedValue(true),
        discoverWorkers: vi.fn().mockResolvedValue([]),
        buildDashboardMessage,
        redisQuit,
        logInfo: vi.fn(),
        logError: vi.fn(),
      },
    });
    await listen(instance.server);
    let closePromise: Promise<void> | undefined;
    let snapshotResolved = false;

    try {
      const refreshPromise = instance.refreshSharedSnapshot();
      await vi.waitFor(() => {
        expect(buildDashboardMessage).toHaveBeenCalledTimes(1);
      });

      closePromise = instance.close();
      await Promise.resolve();
      expect(redisQuit).not.toHaveBeenCalled();

      snapshotResolved = true;
      snapshotRefresh.resolve(message);
      await refreshPromise;
      await closePromise;

      expect(redisQuit).toHaveBeenCalledTimes(1);
    } finally {
      if (!snapshotResolved) {
        snapshotRefresh.resolve(message);
      }
      await (closePromise ?? instance.close()).catch(() => undefined);
    }
  });
});
