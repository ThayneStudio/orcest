import type { Server } from "http";
import type { AddressInfo } from "net";
import { afterEach, describe, expect, it, vi, type Mock } from "vitest";
import { WebSocket } from "ws";
import { createDashboardServer, type DashboardServerInstance } from "./index.js";
import {
  TASK_OUTPUT_COMPLETE_CLOSE_REASON,
  type TaskOutputSocketOptions,
} from "./taskOutputSocket.js";

const originalToken = process.env.DASHBOARD_TOKEN;
const originalAllowedOrigins = process.env.DASHBOARD_ALLOWED_ORIGINS;

type TestServer = {
  instance: DashboardServerInstance;
  port: number;
  logError: ReturnType<typeof vi.fn>;
};

type TaskOutputDeps = NonNullable<TaskOutputSocketOptions["deps"]>;
type FindTaskStart = NonNullable<TaskOutputDeps["findTaskStart"]>;
type ReadTaskOutputFromStream = NonNullable<TaskOutputDeps["readTaskOutputFromStream"]>;
type TaskOutputReadResult = Awaited<ReturnType<ReadTaskOutputFromStream>>;

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

async function closeTestServer({ instance }: TestServer): Promise<void> {
  await instance.close();
}

async function openTaskOutputServer(options: {
  findTaskStart: Mock<FindTaskStart>;
  readTaskOutputFromStream: Mock<ReadTaskOutputFromStream>;
  taskOutputOptions?: Omit<TaskOutputSocketOptions, "deps">;
}): Promise<TestServer> {
  const logError = vi.fn();
  const instance = createDashboardServer({
    port: 0,
    pingIntervalMs: 60_000,
    snapshotRefreshIntervalMs: 60_000,
    taskOutputOptions: {
      pollIntervalMs: 10,
      ...options.taskOutputOptions,
      deps: {
        findTaskStart: options.findTaskStart,
        readTaskOutputFromStream: options.readTaskOutputFromStream,
        logError: vi.fn(),
      },
    },
    deps: {
      healthCheck: vi.fn().mockResolvedValue(true),
      discoverWorkers: vi.fn().mockResolvedValue([]),
      buildDashboardMessage: vi.fn(),
      redisQuit: vi.fn().mockResolvedValue(undefined),
      logInfo: vi.fn(),
      logError,
    },
  });
  const port = await listen(instance.server);

  return { instance, port, logError };
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

function collectMessagesUntilClose(ws: WebSocket): Promise<{
  messages: unknown[];
  close: { code: number; reason: string };
}> {
  const messages: unknown[] = [];

  return withTimeout(new Promise((resolve, reject) => {
    ws.on("message", (data) => {
      messages.push(JSON.parse(String(data)));
    });
    ws.on("error", reject);
    ws.on("close", (code, reason) => {
      resolve({ messages, close: { code, reason: reason.toString() } });
    });
  }), 1000, "Timed out waiting for task-output WebSocket close");
}

function waitForOpen(ws: WebSocket): Promise<void> {
  return withTimeout(new Promise((resolve, reject) => {
    ws.on("open", resolve);
    ws.on("error", reject);
  }), 1000, "Timed out waiting for task-output WebSocket open");
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
  }), 1000, "Timed out waiting for rejected task-output WebSocket upgrade");
}

afterEach(() => {
  if (originalToken === undefined) delete process.env.DASHBOARD_TOKEN;
  else process.env.DASHBOARD_TOKEN = originalToken;
  if (originalAllowedOrigins === undefined) delete process.env.DASHBOARD_ALLOWED_ORIGINS;
  else process.env.DASHBOARD_ALLOWED_ORIGINS = originalAllowedOrigins;
});

describe("task output WebSocket integration", () => {
  it("routes an authorized upgrade, resumes from after_id, and closes after task_end", async () => {
    process.env.DASHBOARD_TOKEN = "s3cret";
    const findTaskStart = vi.fn<FindTaskStart>().mockResolvedValue({
      stream: "output:worker-1",
      entryId: "1-0",
    });
    const readTaskOutputFromStream = vi.fn<ReadTaskOutputFromStream>().mockResolvedValue({
      entries: [{ id: "5-0", line: "resumed output" }],
      lastId: "5-0",
      done: true,
    });
    const testServer = await openTaskOutputServer({
      findTaskStart,
      readTaskOutputFromStream,
    });

    try {
      const ws = new WebSocket(
        `ws://127.0.0.1:${testServer.port}/ws/task-output?` +
          "worker_id=worker-1&task_id=task-1&historical=1&after_id=4-0&token=s3cret",
        { headers: { Origin: `http://127.0.0.1:${testServer.port}` } },
      );

      const result = await collectMessagesUntilClose(ws);

      expect(findTaskStart).toHaveBeenCalledWith("worker-1", "task-1", undefined);
      expect(readTaskOutputFromStream).toHaveBeenCalledWith(
        "output:worker-1",
        "1-0",
        "4-0",
        "task-1",
      );
      expect(result.messages).toEqual([{
        lines: ["resumed output"],
        last_id: "5-0",
        done: true,
      }]);
      expect(result.close).toEqual({
        code: 1000,
        reason: TASK_OUTPUT_COMPLETE_CLOSE_REASON,
      });
    } finally {
      await closeTestServer(testServer);
    }
  });

  it("rejects task-output upgrades from untrusted browser origins with 403", async () => {
    process.env.DASHBOARD_TOKEN = "s3cret";
    const findTaskStart = vi.fn<FindTaskStart>().mockResolvedValue(null);
    const readTaskOutputFromStream = vi.fn<ReadTaskOutputFromStream>();
    const testServer = await openTaskOutputServer({
      findTaskStart,
      readTaskOutputFromStream,
    });

    try {
      const ws = new WebSocket(
        `ws://127.0.0.1:${testServer.port}/ws/task-output?worker_id=worker-1&token=s3cret`,
        { headers: { Origin: "https://evil.example.test" } },
      );

      await expect(rejectedWebSocketStatus(ws)).resolves.toBe(403);
      expect(testServer.logError).toHaveBeenCalledWith(
        "Dashboard WS upgrade rejected: untrusted origin",
        expect.objectContaining({
          path: "/ws/task-output",
          origin: "https://evil.example.test",
          host: `127.0.0.1:${testServer.port}`,
        }),
      );
      expect(findTaskStart).not.toHaveBeenCalled();
      expect(readTaskOutputFromStream).not.toHaveBeenCalled();
    } finally {
      await closeTestServer(testServer);
    }
  });

  it("waits for an in-flight task-output poll before quitting Redis", async () => {
    process.env.DASHBOARD_TOKEN = "s3cret";
    const outputRead = deferred<TaskOutputReadResult>();
    const redisQuit = vi.fn().mockResolvedValue(undefined);
    const findTaskStart = vi.fn<FindTaskStart>().mockResolvedValue({
      stream: "output:worker-1",
      entryId: "1-0",
    });
    const readTaskOutputFromStream = vi.fn<ReadTaskOutputFromStream>()
      .mockReturnValue(outputRead.promise);
    const instance = createDashboardServer({
      port: 0,
      pingIntervalMs: 60_000,
      snapshotRefreshIntervalMs: 60_000,
      taskOutputOptions: {
        pollIntervalMs: 10,
        deps: {
          findTaskStart,
          readTaskOutputFromStream,
          logError: vi.fn(),
        },
      },
      deps: {
        healthCheck: vi.fn().mockResolvedValue(true),
        discoverWorkers: vi.fn().mockResolvedValue([]),
        buildDashboardMessage: vi.fn(),
        redisQuit,
        logInfo: vi.fn(),
        logError: vi.fn(),
      },
    });
    const port = await listen(instance.server);
    const ws = new WebSocket(
      `ws://127.0.0.1:${port}/ws/task-output?worker_id=worker-1&task_id=task-1&token=s3cret`,
    );
    let closePromise: Promise<void> | undefined;
    let outputResolved = false;

    try {
      await vi.waitFor(() => {
        expect(readTaskOutputFromStream).toHaveBeenCalledTimes(1);
      });

      closePromise = instance.close();
      await Promise.resolve();
      expect(redisQuit).not.toHaveBeenCalled();

      outputResolved = true;
      outputRead.resolve({ entries: [], lastId: "1-0", done: true });
      await closePromise;

      expect(redisQuit).toHaveBeenCalledTimes(1);
    } finally {
      if (!outputResolved) {
        outputRead.resolve({ entries: [], lastId: "1-0", done: true });
      }
      if (ws.readyState === WebSocket.OPEN) ws.close();
      await (closePromise ?? instance.close()).catch(() => undefined);
    }
  });

  it("enforces the task-output connection cap through the upgrade path", async () => {
    process.env.DASHBOARD_TOKEN = "s3cret";
    const findTaskStart = vi.fn<FindTaskStart>().mockResolvedValue(null);
    const readTaskOutputFromStream = vi.fn<ReadTaskOutputFromStream>();
    const testServer = await openTaskOutputServer({
      findTaskStart,
      readTaskOutputFromStream,
      taskOutputOptions: {
        maxConnections: 1,
        pollIntervalMs: 60_000,
      },
    });
    const first = new WebSocket(
      `ws://127.0.0.1:${testServer.port}/ws/task-output?worker_id=worker-1&token=s3cret`,
    );

    try {
      await waitForOpen(first);

      const second = new WebSocket(
        `ws://127.0.0.1:${testServer.port}/ws/task-output?worker_id=worker-2&token=s3cret`,
      );
      const result = await collectMessagesUntilClose(second);

      expect(result.messages).toEqual([]);
      expect(result.close).toEqual({
        code: 1013,
        reason: "Too many connections",
      });
      expect(readTaskOutputFromStream).not.toHaveBeenCalled();
    } finally {
      if (first.readyState === WebSocket.OPEN) first.close();
      await closeTestServer(testServer);
    }
  });
});
