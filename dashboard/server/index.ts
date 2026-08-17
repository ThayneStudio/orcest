import { createServer, type IncomingMessage, type Server } from "http";
import fs from "fs";
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
const CLEAN_REVISION_RE = /^[0-9a-f]{7,64}$/;
const BUILD_REVISION_PATH = "/app/.orcest-revision";
// Neither WebSocket server reads client messages, but ws buffers a whole frame
// before emitting it, so the 100 MiB library default is pure memory exposure.
export const WEBSOCKET_MAX_PAYLOAD_BYTES = 64 * 1024;
export const MAX_SNAPSHOT_CONNECTIONS = 50;
// Snapshot payloads are idempotent full-state dumps: a client that stops
// reading gets frames dropped rather than growing the sender queue forever.
export const SNAPSHOT_SKIP_BUFFERED_BYTES = 1024 * 1024;
export const SNAPSHOT_TERMINATE_BUFFERED_BYTES = 8 * 1024 * 1024;
// REST snapshot/worker reads coalesce onto one Redis sweep and reuse it briefly
// so N concurrent GETs cannot queue N full-keyspace sweeps on one connection.
const REST_CACHE_TTL_MS = 1000;

type LogFn = (...args: unknown[]) => void;
type SnapshotSendSocket = Pick<
  WebSocket,
  "readyState" | "send" | "terminate" | "bufferedAmount"
>;

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
  maxSnapshotConnections?: number;
  restCacheTtlMs?: number;
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

export function dashboardRevisionFromEnv(
  raw = process.env.ORCEST_BUILD_REVISION,
): string {
  const revision = raw?.trim().toLowerCase() ?? "";
  return CLEAN_REVISION_RE.test(revision) ? revision : "unknown";
}

export function dashboardBuildRevision(
  path = BUILD_REVISION_PATH,
  environmentFallback = process.env.ORCEST_BUILD_REVISION,
): string {
  try {
    return dashboardRevisionFromEnv(fs.readFileSync(path, "utf8"));
  } catch {
    // Source checkouts do not have the image-baked revision file.
    return dashboardRevisionFromEnv(environmentFallback);
  }
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
  // readyState stays OPEN while a client completes the handshake and then stops
  // reading, and ping/pong keeps passing while the kernel still ACKs, so
  // bufferedAmount is the only signal that the sender queue is growing.
  const buffered = ws.bufferedAmount;
  if (buffered > SNAPSHOT_TERMINATE_BUFFERED_BYTES) {
    logError("Snapshot WS backpressure limit exceeded; terminating client:", buffered);
    ws.terminate();
    return false;
  }
  if (buffered > SNAPSHOT_SKIP_BUFFERED_BYTES) return false;
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

/**
 * Wrap an expensive fetch so concurrent callers share one in-flight run and a
 * result is reused for `ttlMs`. Failures are shared too, but never cached.
 */
export function createCoalescedFetch<T>(
  fetcher: () => Promise<T>,
  ttlMs: number,
  now: () => number = Date.now,
): () => Promise<T> {
  let inFlight: Promise<T> | null = null;
  let cached: { value: T; at: number } | null = null;

  return () => {
    if (cached && now() - cached.at < ttlMs) return Promise.resolve(cached.value);
    if (inFlight) return inFlight;

    // `inFlight` is published BEFORE the fetcher runs, so the cleanup below can
    // always identify and clear it. The obvious `promise = (async () => …)()`
    // shape cannot: a fetcher that throws SYNCHRONOUSLY reaches the cleanup
    // while `promise` is still undefined, so the identity check fails,
    // `inFlight` is never cleared, and the rejected promise is then handed to
    // every future caller forever -- permanently 503-ing the endpoint.
    // The fetcher is still invoked synchronously, preserving call-timing.
    let settle!: (value: T) => void;
    let fail!: (reason: unknown) => void;
    const promise = new Promise<T>((resolve, reject) => {
      settle = resolve;
      fail = reject;
    });
    inFlight = promise;

    const release = () => {
      if (inFlight === promise) inFlight = null;
    };

    try {
      Promise.resolve(fetcher()).then(
        (value) => {
          cached = { value, at: now() };
          release();
          settle(value);
        },
        (error) => {
          release();
          fail(error);
        },
      );
    } catch (error) {
      release();
      fail(error);
    }
    return promise;
  };
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
  const maxSnapshotConnections =
    options.maxSnapshotConnections ?? MAX_SNAPSHOT_CONNECTIONS;
  const restCacheTtlMs = options.restCacheTtlMs ?? REST_CACHE_TTL_MS;
  const deps = dashboardServerDeps(options.deps);
  const restSnapshot = createCoalescedFetch(
    () => deps.buildDashboardMessage(),
    restCacheTtlMs,
  );
  const restWorkers = createCoalescedFetch(() => deps.discoverWorkers(), restCacheTtlMs);
  const revision = dashboardBuildRevision();
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
    res.json({ ok: true, revision });
  });

  app.get("/api/ready", async (_req, res) => {
    try {
      const redisOk = await deps.healthCheck();
      res.status(redisOk ? 200 : 503).json({ ok: redisOk, redis_ok: redisOk, revision });
    } catch (err) {
      deps.logError("Dashboard readiness check failed:", err);
      res.status(503).json({ ok: false, redis_ok: false, revision });
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
      const workers = await restWorkers();
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
      res.json(await restSnapshot());
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

  const snapshotWss = new WebSocketServer({
    noServer: true,
    maxPayload: WEBSOCKET_MAX_PAYLOAD_BYTES,
  });
  const taskOutputWss = new WebSocketServer({
    noServer: true,
    maxPayload: WEBSOCKET_MAX_PAYLOAD_BYTES,
  });

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
    // Must be the very first statement: ws emits 'error' for any protocol-level
    // frame error, and an EventEmitter 'error' with no listener throws, so a
    // single malformed frame on a connection that hits an early return below
    // would otherwise take down the process.
    ws.on("error", (err) => {
      deps.logError("Snapshot WS error:", err);
    });

    if (closing) {
      ws.close(1001, "Server shutting down");
      return;
    }
    if (snapshotWss.clients.size > maxSnapshotConnections) {
      ws.close(1013, "Too many connections");
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
    // Errors are logged by the listener attached at the top of this handler.
    ws.on("error", () => clearInterval(pingTimer));
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
    // Must be the very first statement — see the snapshot handler above.
    // handleTaskOutputConnection attaches its own logging listener, but it is
    // not reached on the `closing` early return below.
    ws.on("error", () => {});

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
        // The WebSocket servers must be closed concurrently with (not after)
        // the HTTP server: upgraded sockets stay counted in the net.Server
        // connection table, so server.close() never fires its callback while a
        // client is connected. Awaiting the HTTP server first deadlocks the two
        // steps against each other whenever a dashboard tab is open.
        await Promise.all([
          closeHttpServer(server),
          closeWebSocketServer(snapshotWss),
          closeWebSocketServer(taskOutputWss),
        ]);
        await waitForHttpRequests();
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
