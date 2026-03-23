import { createServer, type IncomingMessage } from "http";
import path from "path";
import { fileURLToPath } from "url";
import type { Duplex } from "stream";
import express from "express";
import { WebSocketServer, WebSocket } from "ws";
import { healthCheck } from "./redis.js";
import { fetchSnapshot } from "./snapshot.js";
import { detectStuck } from "./stuck.js";
import { discoverWorkers, findTaskStartId, readTaskOutput } from "./workers.js";
import type { DashboardMessage, TaskOutputMessage } from "./types.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PORT = parseInt(process.env.PORT || "8080", 10);
const DASHBOARD_TOKEN = process.env.DASHBOARD_TOKEN;

function isAuthorized(req: IncomingMessage): boolean {
  if (!DASHBOARD_TOKEN) return true;
  const auth = (req as { headers: Record<string, string | string[] | undefined> }).headers.authorization;
  if (typeof auth === "string" && auth.startsWith("Bearer ") && auth.slice(7) === DASHBOARD_TOKEN) return true;
  const url = new URL(req.url || "", `http://localhost:${PORT}`);
  if (url.searchParams.get("token") === DASHBOARD_TOKEN) return true;
  return false;
}

const app = express();
const server = createServer(app);

// --- Auth middleware for API routes ---

app.use("/api", (req, res, next) => {
  if (!isAuthorized(req)) {
    res.status(401).json({ error: "Unauthorized" });
    return;
  }
  next();
});

// --- REST endpoints ---

app.get("/api/health", async (_req, res) => {
  const ok = await healthCheck();
  res.status(ok ? 200 : 503).json({ ok });
});

app.get("/api/workers", async (_req, res) => {
  const workers = await discoverWorkers();
  res.json({ workers });
});

// --- Static files (Vite build output) ---

const distPath = path.resolve(__dirname, "../../dist");
app.use(express.static(distPath));

// SPA fallback
app.get("*", (_req, res) => {
  res.sendFile(path.join(distPath, "index.html"));
});

// --- WebSocket ---
// Use noServer mode and handle upgrades manually to avoid conflicts with Express

const snapshotWss = new WebSocketServer({ noServer: true });
const taskOutputWss = new WebSocketServer({ noServer: true });

server.on("upgrade", (req: IncomingMessage, socket: Duplex, head: Buffer) => {
  if (!isAuthorized(req)) {
    socket.write("HTTP/1.1 401 Unauthorized\r\n\r\n");
    socket.destroy();
    return;
  }

  const pathname = new URL(req.url || "", `http://localhost:${PORT}`).pathname;

  if (pathname === "/ws/snapshot") {
    snapshotWss.handleUpgrade(req, socket, head, (ws) => {
      snapshotWss.emit("connection", ws, req);
    });
  } else if (pathname === "/ws/task-output") {
    taskOutputWss.handleUpgrade(req, socket, head, (ws) => {
      taskOutputWss.emit("connection", ws, req);
    });
  } else {
    socket.destroy();
  }
});

// --- Snapshot WebSocket ---

snapshotWss.on("connection", (ws) => {
  let inFlight = false;

  const sendSnapshot = async () => {
    if (ws.readyState !== WebSocket.OPEN || inFlight) return;
    inFlight = true;
    try {
      const snapshot = await fetchSnapshot();
      const stuckTasks = await detectStuck(snapshot);
      const workers = await discoverWorkers();
      const msg: DashboardMessage = {
        snapshot,
        stuck_tasks: stuckTasks,
        workers,
      };
      ws.send(JSON.stringify(msg));
    } catch (err) {
      console.error("Error sending snapshot:", err);
    } finally {
      inFlight = false;
    }
  };

  sendSnapshot();
  const interval = setInterval(sendSnapshot, 2000);

  ws.on("close", () => clearInterval(interval));
  ws.on("error", () => clearInterval(interval));
});

// --- Task Output WebSocket ---
// Query params: worker_id (required), task_id (optional — if omitted, streams the most recent task)

taskOutputWss.on("connection", (ws, req) => {
  const url = new URL(req.url || "", `http://localhost:${PORT}`);
  const workerId = url.searchParams.get("worker_id");
  const taskId = url.searchParams.get("task_id") || undefined;

  if (!workerId) {
    ws.close(1008, "Missing worker_id");
    return;
  }

  let lastId = "0-0";
  let startId: string | null = null;
  let initialized = false;
  let taskDone = false;
  let inFlight = false;

  const poll = async () => {
    if (ws.readyState !== WebSocket.OPEN || taskDone || inFlight) return;
    inFlight = true;

    try {
      // First call: find where the task starts in the stream
      if (!initialized) {
        startId = await findTaskStartId(workerId, taskId);
        if (!startId) {
          // No task_start found — send empty and keep trying
          ws.send(JSON.stringify({ lines: [], last_id: "0-0", done: false } satisfies TaskOutputMessage));
          return;
        }
        initialized = true;
      }

      const result = await readTaskOutput(workerId, startId!, lastId, taskId);
      if (result.entries.length > 0) {
        lastId = result.lastId;
        const msg: TaskOutputMessage = {
          lines: result.entries.map((e) => e.line),
          last_id: lastId,
          done: result.done,
        };
        ws.send(JSON.stringify(msg));
      }
      if (result.done) {
        taskDone = true;
      }
    } catch (err) {
      console.error("Error reading task output:", err);
    } finally {
      inFlight = false;
    }
  };

  poll();
  const interval = setInterval(poll, 500);

  ws.on("close", () => clearInterval(interval));
  ws.on("error", () => clearInterval(interval));
});

// --- Start ---

server.listen(PORT, () => {
  console.log(`Orcest dashboard listening on http://0.0.0.0:${PORT}`);
});
