import { createServer, type IncomingMessage, type Server } from "http";
import path from "path";
import { fileURLToPath, pathToFileURL } from "url";
import type { Duplex } from "stream";
import express, { type Express } from "express";
import { WebSocketServer, WebSocket } from "ws";
import {
  assertValidDashboardRedisPrefixes,
  healthCheck,
  quitRedis,
} from "./redis.js";
import { authCookieHeader, isAuthorized } from "./auth.js";
import { buildDashboardMessage } from "./message.js";
import {
  assertValidDashboardAllowedOrigins,
  dashboardUpgradeRejectionContext,
  resolveDashboardUpgrade,
} from "./upgrade.js";
import { resolveDashboardDistPath } from "./static.js";
import { discoverWorkers, WorkerDiscoveryPartialError } from "./workers.js";
import {
  handleTaskOutputConnection,
  type TaskOutputConnectionHandle,
  type TaskOutputSocketOptions,
} from "./taskOutputSocket.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DEFAULT_PORT = 8080;
const PING_INTERVAL = 30000;
const SNAPSHOT_REFRESH_INTERVAL = 2000;
const SHUTDOWN_TIMEOUT_MS = 5000;
const WEBSOCKET_CLOSE_GRACE_MS = 1000;

type LogFn = (...args: unknown[]) => void;
type SnapshotSendSocket = Pick<WebSocket, "readyState" | "send" | "terminate">;

type DashboardServerDeps = {
  healthCheck: typeof healthCheck;
  discoverWorkers: typeof discoverWorkers;
  buildDashboardMessage: typeof buildDashboardMessage;
  redisQuit: () => Promise<unknown>;
  logInfo: LogFn;
  logError: LogFn;
};

export interface DashboardServerOptions {
  port?: number;
  cwd?: string;
  serverDir?: string;
  pingIntervalMs?: number;
  snapshotRefreshIntervalMs?: number;
  shutdownTimeoutMs?: number;
  taskOutputOptions?: TaskOutputSocketOptions;
  deps?: Partial<DashboardServerDeps>;
}

export interface DashboardServerInstance {
  app: Express;
  server: Server;
  snapshotWss: WebSocketServer;
  taskOutputWss: WebSocketServer;
  port: number;
  refreshSharedSnapshot: () => Promise<void>;
  close: () => Promise<void>;
  shutdown: () => void;
}

function parsePortValue(
  raw: string | undefined,
  fallback: number,
  allowZero: boolean,
): number {
  const trimmed = raw?.trim() ?? "";
  if (!trimmed || !/^\d+$/.test(trimmed)) return fallback;
  const parsed = Number(trimmed);
  const min = allowZero ? 0 : 1;
  return Number.isSafeInteger(parsed) && parsed >= min && parsed <= 65535
    ? parsed
    : fallback;
}

export function dashboardPortFromEnv(raw = process.env.PORT): number {
  return parsePortValue(raw, DEFAULT_PORT, true);
}

function dashboardServerDeps(overrides: Partial<DashboardServerDeps> = {}): DashboardServerDeps {
  return {
    healthCheck,
    discoverWorkers,
    buildDashboardMessage,
    redisQuit: quitRedis,
    logInfo: console.log,
    logError: console.error,
    ...overrides,
  };
}

async function closeHttpServer(server: Server): Promise<void> {
  if (!server.listening) return;

  await new Promise<void>((resolve, reject) => {
    server.close((err) => {
      if (err) reject(err);
      else resolve();
    });
  });
}

export function safeSendSnapshot(
  ws: SnapshotSendSocket,
  msg: string,
  logError: LogFn,
): boolean {
  if (ws.readyState !== WebSocket.OPEN) return false;
  try {
    ws.send(msg);
    return true;
  } catch (err) {
    logError("Snapshot WS send failed:", err);
    ws.terminate();
    return false;
  }
}

export function broadcastSnapshotMessage(
  clients: Iterable<SnapshotSendSocket>,
  msg: string,
  logError: LogFn,
): void {
  for (const client of clients) {
    safeSendSnapshot(client, msg, logError);
  }
}

export function dashboardTaskOutputConnectionOptions(
  configuredOptions: TaskOutputSocketOptions | undefined,
  activeConnections: number,
  port: number,
  defaultPingIntervalMs: number,
): TaskOutputSocketOptions {
  return {
    ...configuredOptions,
    activeConnections,
    port,
    pingIntervalMs: configuredOptions?.pingIntervalMs ?? defaultPingIntervalMs,
  };
}

export async function closeWebSocketServer(
  wss: WebSocketServer,
  closeGraceMs = WEBSOCKET_CLOSE_GRACE_MS,
): Promise<void> {
  const clients = [...wss.clients];
  for (const client of clients) {
    if (client.readyState === WebSocket.OPEN || client.readyState === WebSocket.CLOSING) {
      client.close(1001, "Server shutting down");
    } else if (client.readyState !== WebSocket.CLOSED) {
      client.terminate();
    }
  }

  let terminateTimer: ReturnType<typeof setTimeout> | undefined;
  if (clients.length > 0) {
    terminateTimer = setTimeout(() => {
      for (const client of clients) {
        if (client.readyState !== WebSocket.CLOSED) {
          client.terminate();
        }
      }
    }, closeGraceMs);
    terminateTimer.unref?.();
  }

  try {
    await new Promise<void>((resolve, reject) => {
      wss.close((err) => {
        if (err && !/server is not running/i.test(err.message)) reject(err);
        else resolve();
      });
    });
  } finally {
    if (terminateTimer) clearTimeout(terminateTimer);
  }
}

export function createDashboardServer(
  options: DashboardServerOptions = {},
): DashboardServerInstance {
  assertValidDashboardAllowedOrigins();
  assertValidDashboardRedisPrefixes();
  const port = options.port ?? dashboardPortFromEnv();
  const pingIntervalMs = options.pingIntervalMs ?? PING_INTERVAL;
  const snapshotRefreshIntervalMs =
    options.snapshotRefreshIntervalMs ?? SNAPSHOT_REFRESH_INTERVAL;
  const shutdownTimeoutMs = options.shutdownTimeoutMs ?? SHUTDOWN_TIMEOUT_MS;
  const deps = dashboardServerDeps(options.deps);
  const app = express();
  const server = createServer(app);
  let closing = false;
  let activeHttpRequests = 0;
  const httpIdleResolvers = new Set<() => void>();

  function notifyHttpIdle(): void {
    if (activeHttpRequests > 0) return;
    for (const resolve of httpIdleResolvers) {
      resolve();
    }
    httpIdleResolvers.clear();
  }

  function waitForHttpRequests(): Promise<void> {
    if (activeHttpRequests === 0) return Promise.resolve();
    return new Promise((resolve) => {
      httpIdleResolvers.add(resolve);
    });
  }

  app.use((_req, res, next) => {
    if (closing) {
      res.setHeader("Connection", "close");
      res.status(503).json({ error: "Server shutting down" });
      return;
    }

    activeHttpRequests++;
    let finished = false;
    const finish = () => {
      if (finished) return;
      finished = true;
      activeHttpRequests = Math.max(0, activeHttpRequests - 1);
      notifyHttpIdle();
    };
    res.once("finish", finish);
    res.once("close", finish);
    next();
  });

  // --- Health checks (intentionally unauthenticated so container/runtime probes can poll them) ---

  app.get("/api/health", (_req, res) => {
    res.json({ ok: true });
  });

  app.get("/api/ready", async (_req, res) => {
    try {
      const redisOk = await deps.healthCheck();
      res.status(redisOk ? 200 : 503).json({ ok: redisOk, redis_ok: redisOk });
    } catch (err) {
      deps.logError("Dashboard readiness check failed:", err);
      res.status(503).json({ ok: false, redis_ok: false });
    }
  });

  // --- Auth middleware for API routes ---

  app.use("/api", (req, res, next) => {
    const cookie = authCookieHeader(req);
    if (cookie) res.setHeader("Set-Cookie", cookie);
    if (!isAuthorized(req)) {
      res.status(401).json({ error: "Unauthorized" });
      return;
    }
    next();
  });

  // --- REST endpoints ---

  app.get("/api/auth/bootstrap", (_req, res) => {
    res.json({ ok: true });
  });

  app.get("/api/workers", async (_req, res) => {
    try {
      const workers = await deps.discoverWorkers();
      res.json({ workers });
    } catch (err) {
      deps.logError("Error discovering workers:", err);
      if (err instanceof WorkerDiscoveryPartialError) {
        res.status(206).json({
          error: "Worker discovery partially unavailable",
          workers: err.workers,
          degraded_sections: ["worker discovery"],
        });
        return;
      }
      res.status(503).json({ error: "Worker discovery unavailable", workers: [] });
    }
  });

  app.get("/api/snapshot", async (_req, res) => {
    try {
      res.json(await deps.buildDashboardMessage());
    } catch (err) {
      deps.logError("Error building dashboard snapshot:", err);
      res.status(503).json({ error: "Snapshot unavailable" });
    }
  });

  // --- Static files (Vite build output) ---
  // Auth check applies to static assets and the SPA fallback as well.

  app.use((req, res, next) => {
    const cookie = authCookieHeader(req);
    if (cookie) res.setHeader("Set-Cookie", cookie);
    if (!isAuthorized(req)) {
      res.status(401).json({ error: "Unauthorized" });
      return;
    }
    next();
  });

  const distPath = resolveDashboardDistPath(
    options.cwd ?? process.cwd(),
    options.serverDir ?? __dirname,
  );
  app.use(express.static(distPath));

  app.use("/assets", (_req, res) => {
    res.status(404).type("text/plain").send("Not Found");
  });

  // API 404 — must come before the SPA fallback
  app.use("/api", (_req, res) => {
    res.status(404).json({ error: "Not Found" });
  });

  // SPA fallback
  app.get("*", (_req, res) => {
    res.sendFile(path.join(distPath, "index.html"));
  });

  // --- WebSocket ---
  // Use noServer mode and handle upgrades manually to avoid conflicts with Express

  const snapshotWss = new WebSocketServer({ noServer: true });
  const taskOutputWss = new WebSocketServer({ noServer: true });

  server.on("upgrade", (req: IncomingMessage, socket: Duplex, head: Buffer) => {
    if (closing) {
      socket.write("HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\n\r\n");
      socket.destroy();
      return;
    }

    const decision = resolveDashboardUpgrade(req, port);
    if (!decision.authorized) {
      if (decision.reason === "origin") {
        deps.logError(
          "Dashboard WS upgrade rejected: untrusted origin",
          dashboardUpgradeRejectionContext(req, port),
        );
        socket.write("HTTP/1.1 403 Forbidden\r\n\r\n");
      } else {
        socket.write("HTTP/1.1 401 Unauthorized\r\n\r\n");
      }
      socket.destroy();
      return;
    }

    if (decision.target === "snapshot") {
      snapshotWss.handleUpgrade(req, socket, head, (ws) => {
        snapshotWss.emit("connection", ws, req);
      });
    } else if (decision.target === "task-output") {
      taskOutputWss.handleUpgrade(req, socket, head, (ws) => {
        taskOutputWss.emit("connection", ws, req);
      });
    } else {
      socket.destroy();
    }
  });

  // --- Snapshot WebSocket ---
  // Single shared refresh loop: all connected clients get the same data,
  // fetched once per interval regardless of how many clients are connected.

  let sharedSnapshotMsg: string | null = null;
  let snapshotRefreshPromise: Promise<void> | null = null;

  function waitForSnapshotRefresh(): Promise<void> {
    return snapshotRefreshPromise ?? Promise.resolve();
  }

  function refreshSharedSnapshot(): Promise<void> {
    if (snapshotRefreshPromise) return snapshotRefreshPromise;
    if (closing) return Promise.resolve();

    let promise!: Promise<void>;
    promise = (async () => {
      try {
        const msg = await deps.buildDashboardMessage();
        sharedSnapshotMsg = JSON.stringify(msg);
        if (closing) return;

        broadcastSnapshotMessage(snapshotWss.clients, sharedSnapshotMsg, deps.logError);
      } catch (err) {
        deps.logError("Error refreshing snapshot:", err);
      } finally {
        if (snapshotRefreshPromise === promise) {
          snapshotRefreshPromise = null;
        }
      }
    })();

    snapshotRefreshPromise = promise;
    return promise;
  }

  function refreshSharedSnapshotAfterCurrent(): void {
    const currentRefresh = snapshotRefreshPromise;
    if (!currentRefresh) {
      void refreshSharedSnapshot();
      return;
    }
    void currentRefresh.finally(() => {
      if (!closing) void refreshSharedSnapshot();
    });
  }

  const snapshotInterval = setInterval(() => {
    if (snapshotWss.clients.size > 0) void refreshSharedSnapshot();
  }, snapshotRefreshIntervalMs);

  snapshotWss.on("connection", (ws) => {
    if (closing) {
      ws.close(1001, "Server shutting down");
      return;
    }

    // Send cached data immediately, then refresh so the first client after an
    // idle period is not stuck on stale queue/lock state until the next tick.
    if (sharedSnapshotMsg) {
      safeSendSnapshot(ws, sharedSnapshotMsg, deps.logError);
      refreshSharedSnapshotAfterCurrent();
    } else {
      void refreshSharedSnapshot();
    }

    let awaitingPong = false;
    const pingTimer = setInterval(() => {
      if (ws.readyState !== WebSocket.OPEN) return;
      if (awaitingPong) {
        ws.terminate();
        return;
      }
      awaitingPong = true;
      try {
        ws.ping();
      } catch (err) {
        deps.logError("Snapshot WS ping failed:", err);
        ws.terminate();
      }
    }, pingIntervalMs);

    ws.on("pong", () => {
      awaitingPong = false;
    });
    ws.on("close", () => clearInterval(pingTimer));
    ws.on("error", (err) => {
      deps.logError("Snapshot WS error:", err);
      clearInterval(pingTimer);
    });
  });

  // --- Task Output WebSocket ---
  // Query params: worker_id (required), task_id (optional — if omitted, streams the most recent task)

  const taskOutputConnectionDone = new Set<Promise<void>>();

  function trackTaskOutputConnection(handle: TaskOutputConnectionHandle): void {
    taskOutputConnectionDone.add(handle.done);
    void handle.done.finally(() => {
      taskOutputConnectionDone.delete(handle.done);
    });
  }

  async function waitForTaskOutputConnections(): Promise<void> {
    await Promise.allSettled([...taskOutputConnectionDone]);
  }

  taskOutputWss.on("connection", (ws, req) => {
    if (closing) {
      ws.close(1001, "Server shutting down");
      return;
    }

    const handle = handleTaskOutputConnection(
      ws,
      req,
      dashboardTaskOutputConnectionOptions(
        options.taskOutputOptions,
        taskOutputWss.clients.size,
        port,
        pingIntervalMs,
      ),
    );
    trackTaskOutputConnection(handle);
  });

  let closePromise: Promise<void> | null = null;
  let transportClosePromise: Promise<void> | null = null;
  let shutdownStarted = false;

  function close(): Promise<void> {
    closing = true;
    if (!transportClosePromise) {
      transportClosePromise = (async () => {
        clearInterval(snapshotInterval);
        await closeHttpServer(server);
        await waitForHttpRequests();
        await Promise.all([
          closeWebSocketServer(snapshotWss),
          closeWebSocketServer(taskOutputWss),
        ]);
        await waitForSnapshotRefresh();
        await waitForTaskOutputConnections();
      })().catch((err) => {
        transportClosePromise = null;
        throw err;
      });
    }

    if (!closePromise) {
      closePromise = (async () => {
        await transportClosePromise;
        await deps.redisQuit();
      })().catch((err) => {
        closePromise = null;
        throw err;
      });
    }
    return closePromise;
  }

  function shutdown(): void {
    if (shutdownStarted) return;
    shutdownStarted = true;
    deps.logInfo("Shutting down...");
    const forceExit = setTimeout(() => process.exit(1), shutdownTimeoutMs);
    void close()
      .then(() => {
        clearTimeout(forceExit);
        process.exit(0);
      })
      .catch((err) => {
        deps.logError("Error during dashboard shutdown:", err);
        clearTimeout(forceExit);
        process.exit(1);
      });
  }

  return {
    app,
    server,
    snapshotWss,
    taskOutputWss,
    port,
    refreshSharedSnapshot,
    close,
    shutdown,
  };
}

export function startDashboardServer(
  options: DashboardServerOptions = {},
): DashboardServerInstance {
  const instance = createDashboardServer(options);
  const logInfo = options.deps?.logInfo ?? console.log;

  instance.server.listen(instance.port, () => {
    // The container binds 0.0.0.0 internally, but the port is published on
    // host loopback only by docker-compose.dashboard.yml. Other containers on
    // the compose network can still reach the internal listener.
    const address = instance.server.address();
    const boundPort = typeof address === "object" && address !== null
      ? address.port
      : instance.port;
    logInfo(`Orcest dashboard listening on http://127.0.0.1:${boundPort}`);
  });

  return instance;
}

function isMainModule(): boolean {
  return Boolean(
    process.argv[1] &&
    import.meta.url === pathToFileURL(process.argv[1]).href,
  );
}

if (isMainModule()) {
  const instance = startDashboardServer();
  process.on("SIGTERM", instance.shutdown);
  process.on("SIGINT", instance.shutdown);
}
