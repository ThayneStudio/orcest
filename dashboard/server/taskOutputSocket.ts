import type { IncomingMessage } from "http";
import { WebSocket } from "ws";
import {
  findTaskStart,
  normalizeTaskOutputCursor,
  readTaskOutputFromStream,
  taskOutputReadFailureMessage,
  taskOutputUnavailableMessage,
} from "./workers.js";
import { dashboardRedisPrefixes } from "./redis.js";
import type { TaskOutputMessage } from "./types.js";

export const MAX_TASK_OUTPUT_CONNECTIONS = 20;
export const TASK_START_LOOKUP_MAX_ATTEMPTS = 20;
export const TASK_OUTPUT_READ_ERROR_LIMIT = 3;
export const TASK_OUTPUT_COMPLETE_CLOSE_REASON = "Task output complete";
export const TASK_OUTPUT_UNAVAILABLE_CLOSE_REASON = "Task output unavailable";
export const TASK_OUTPUT_POLL_INTERVAL = 500;
// Task-output frames are not idempotent (dropping one loses lines), so a client
// that stops reading is terminated instead; the UI reconnects with after_id.
export const TASK_OUTPUT_MAX_BUFFERED_BYTES = 4 * 1024 * 1024;

type TaskStart = { stream: string; entryId: string };
type TaskOutputReadResult = {
  entries: Array<{ id: string; line: string }>;
  lastId: string;
  done: boolean;
  unavailable?: boolean;
};

type TaskOutputSocket = Pick<
  WebSocket,
  "readyState" | "send" | "close" | "terminate" | "ping" | "on" | "bufferedAmount"
>;

type TaskOutputSocketDeps = {
  findTaskStart: typeof findTaskStart;
  readTaskOutputFromStream: typeof readTaskOutputFromStream;
  logError: (...args: unknown[]) => void;
};

export type TaskOutputSocketOptions = {
  activeConnections?: number;
  maxConnections?: number;
  port?: number;
  pingIntervalMs?: number;
  pollIntervalMs?: number;
  maxBufferedBytes?: number;
  taskStartLookupMaxAttempts?: number;
  readErrorLimit?: number;
  deps?: Partial<TaskOutputSocketDeps>;
};

export type TaskOutputConnectionHandle = {
  done: Promise<void>;
  close: () => void;
};

function resolvedTaskOutputConnectionHandle(): TaskOutputConnectionHandle {
  return {
    done: Promise.resolve(),
    close: () => undefined,
  };
}

function taskOutputSocketDeps(overrides: Partial<TaskOutputSocketDeps> = {}): TaskOutputSocketDeps {
  return {
    findTaskStart,
    readTaskOutputFromStream,
    logError: console.error,
    ...overrides,
  };
}

function taskOutputRedisPrefixAllowed(redisPrefix: string | null | undefined): boolean {
  const configuredPrefixes = dashboardRedisPrefixes();
  if (!configuredPrefixes || redisPrefix === undefined) return true;
  return configuredPrefixes.includes(redisPrefix ?? "");
}

export function handleTaskOutputConnection(
  ws: TaskOutputSocket,
  req: IncomingMessage,
  options: TaskOutputSocketOptions = {},
): TaskOutputConnectionHandle {
  const deps = taskOutputSocketDeps(options.deps);

  // Must be the very first statement: ws emits 'error' for any protocol-level
  // frame error, and an EventEmitter 'error' with no listener throws. Every
  // early return below leaves the client holding a live socket, so attaching
  // this later means one malformed frame crashes the process.
  ws.on("error", (err) => {
    deps.logError("Task output WS error:", err);
  });

  const activeConnections = options.activeConnections ?? 1;
  const maxConnections = options.maxConnections ?? MAX_TASK_OUTPUT_CONNECTIONS;
  if (activeConnections > maxConnections) {
    ws.close(1013, "Too many connections");
    return resolvedTaskOutputConnectionHandle();
  }

  const port = options.port ?? 8080;
  const url = new URL(req.url || "", `http://localhost:${port}`);
  const workerId = url.searchParams.get("worker_id")?.trim() || "";
  const taskId = url.searchParams.get("task_id")?.trim() || undefined;
  const redisPrefix = url.searchParams.has("redis_prefix")
    ? (url.searchParams.get("redis_prefix")?.trim() || null)
    : undefined;
  const historical = url.searchParams.get("historical") === "1";
  const afterId = normalizeTaskOutputCursor(url.searchParams.get("after_id"));

  if (!workerId) {
    ws.close(1008, "Missing worker_id");
    return resolvedTaskOutputConnectionHandle();
  }
  if (!taskOutputRedisPrefixAllowed(redisPrefix)) {
    ws.close(1008, "Redis prefix is not allowed");
    return resolvedTaskOutputConnectionHandle();
  }
  if (ws.readyState !== WebSocket.OPEN) {
    return resolvedTaskOutputConnectionHandle();
  }

  const taskStartLookupMaxAttempts = options.taskStartLookupMaxAttempts ??
    TASK_START_LOOKUP_MAX_ATTEMPTS;
  const readErrorLimit = options.readErrorLimit ?? TASK_OUTPUT_READ_ERROR_LIMIT;
  const pollIntervalMs = options.pollIntervalMs ?? TASK_OUTPUT_POLL_INTERVAL;
  const pingIntervalMs = options.pingIntervalMs ?? 30000;
  const maxBufferedBytes = options.maxBufferedBytes ?? TASK_OUTPUT_MAX_BUFFERED_BYTES;

  let lastId = afterId;
  let taskStart: TaskStart | null = null;
  let initialized = false;
  let taskDone = false;
  let inFlight = false;
  let startLookupAttempts = 0;
  let readErrorCount = 0;
  let idleTailPolls = 0;
  let interval: ReturnType<typeof setInterval> | undefined;
  let pingTimer: ReturnType<typeof setInterval> | undefined;
  let closeTimer: ReturnType<typeof setTimeout> | undefined;
  let closeQueued = false;
  let connectionClosed = false;
  let doneResolved = false;
  let activePoll: Promise<void> | null = null;
  let resolveDone!: () => void;
  const done = new Promise<void>((resolve) => {
    resolveDone = resolve;
  });

  const cleanupTimers = () => {
    if (interval) clearInterval(interval);
    if (pingTimer) clearInterval(pingTimer);
    if (closeTimer) clearTimeout(closeTimer);
  };

  const resolveDoneOnce = () => {
    if (doneResolved) return;
    doneResolved = true;
    resolveDone();
  };

  const finishConnection = () => {
    connectionClosed = true;
    cleanupTimers();
    if (activePoll) {
      void activePoll.finally(resolveDoneOnce);
    } else {
      resolveDoneOnce();
    }
  };

  const terminateConnection = () => {
    if (ws.readyState !== WebSocket.CLOSED) {
      ws.terminate();
    } else {
      finishConnection();
    }
  };

  const sendTaskOutput = (msg: TaskOutputMessage, closeReason?: string): boolean => {
    if (ws.readyState !== WebSocket.OPEN) return false;
    // readyState stays OPEN for a client that completes the handshake and then
    // stops reading, and ping/pong keeps passing while the kernel still ACKs,
    // so bufferedAmount is the only signal that the sender queue is growing.
    if (ws.bufferedAmount > maxBufferedBytes) {
      deps.logError(
        "Task output WS backpressure limit exceeded; terminating client:",
        ws.bufferedAmount,
      );
      taskDone = true;
      if (interval) clearInterval(interval);
      terminateConnection();
      return false;
    }
    try {
      ws.send(JSON.stringify(msg));
    } catch (err) {
      deps.logError("Error sending task output:", err);
      taskDone = true;
      if (interval) clearInterval(interval);
      terminateConnection();
      return false;
    }
    if (closeReason && !closeQueued) {
      closeQueued = true;
      closeTimer = setTimeout(() => {
        if (ws.readyState === WebSocket.OPEN) {
          ws.close(1000, closeReason);
        }
      }, 0);
    }
    return true;
  };

  const findTaskStartForRequest = async (): Promise<TaskStart | null> => {
    return deps.findTaskStart(workerId, taskId, redisPrefix);
  };

  const poll = async () => {
    if (ws.readyState !== WebSocket.OPEN || taskDone) return;
    inFlight = true;

    try {
      if (!initialized) {
        taskStart = await findTaskStartForRequest();
        readErrorCount = 0;
        if (!taskStart) {
          startLookupAttempts++;
          if (historical && taskId && startLookupAttempts >= taskStartLookupMaxAttempts) {
            taskDone = true;
            if (interval) clearInterval(interval);
            sendTaskOutput({
              lines: [],
              last_id: "0-0",
              done: true,
              error: taskOutputUnavailableMessage(taskId),
            }, TASK_OUTPUT_UNAVAILABLE_CLOSE_REASON);
            return;
          }
          sendTaskOutput({ lines: [], last_id: "0-0", done: false });
          return;
        }
        startLookupAttempts = 0;
        initialized = true;
      }

      const currentTaskStart = taskStart;
      if (!currentTaskStart) return;

      const result: TaskOutputReadResult = await deps.readTaskOutputFromStream(
        currentTaskStart.stream,
        currentTaskStart.entryId,
        lastId,
        taskId,
      );
      readErrorCount = 0;
      if (result.lastId !== lastId) {
        lastId = result.lastId;
        idleTailPolls = 0;
      } else if (historical && taskId && initialized && !result.done) {
        idleTailPolls++;
        if (idleTailPolls >= 2) {
          result.done = true;
        }
      }
      if (result.unavailable && taskId) {
        taskDone = true;
        if (interval) clearInterval(interval);
        sendTaskOutput({
          lines: [],
          last_id: lastId,
          done: true,
          error: taskOutputUnavailableMessage(taskId),
        }, TASK_OUTPUT_UNAVAILABLE_CLOSE_REASON);
      } else if (result.entries.length > 0) {
        const msg: TaskOutputMessage = {
          lines: result.entries.map((e) => e.line),
          last_id: lastId,
          done: result.done,
        };
        sendTaskOutput(
          msg,
          result.done ? TASK_OUTPUT_COMPLETE_CLOSE_REASON : undefined,
        );
      } else if (result.done) {
        sendTaskOutput(
          { lines: [], last_id: lastId, done: true },
          TASK_OUTPUT_COMPLETE_CLOSE_REASON,
        );
      }
      if (result.done) {
        taskDone = true;
        if (interval) clearInterval(interval);
      }
    } catch (err) {
      readErrorCount++;
      deps.logError("Error reading task output:", err);
      if (readErrorCount >= readErrorLimit) {
        taskDone = true;
        if (interval) clearInterval(interval);
        sendTaskOutput({
          lines: [],
          last_id: lastId,
          done: true,
          error: taskOutputReadFailureMessage(),
        }, TASK_OUTPUT_UNAVAILABLE_CLOSE_REASON);
      }
    } finally {
      inFlight = false;
    }
  };

  const startPoll = () => {
    if (inFlight) return;
    const promise = poll();
    activePoll = promise;
    void promise.finally(() => {
      if (activePoll === promise) activePoll = null;
      if (connectionClosed) resolveDoneOnce();
    });
  };

  interval = setInterval(startPoll, pollIntervalMs);
  startPoll();
  let awaitingPong = false;
  pingTimer = setInterval(() => {
    if (ws.readyState !== WebSocket.OPEN) return;
    if (awaitingPong) {
      terminateConnection();
      return;
    }
    awaitingPong = true;
    try {
      ws.ping();
    } catch (err) {
      deps.logError("Task output WS ping failed:", err);
      terminateConnection();
    }
  }, pingIntervalMs);

  ws.on("pong", () => {
    awaitingPong = false;
  });
  ws.on("close", finishConnection);
  // Errors are logged by the listener attached at the top of this function.
  ws.on("error", finishConnection);

  return {
    done,
    close: () => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.close(1001, "Server shutting down");
      } else {
        finishConnection();
      }
    },
  };
}
