import { EventEmitter } from "events";
import type { IncomingMessage } from "http";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { WebSocket } from "ws";
import {
  handleTaskOutputConnection,
  TASK_OUTPUT_COMPLETE_CLOSE_REASON,
  TASK_OUTPUT_UNAVAILABLE_CLOSE_REASON,
} from "./taskOutputSocket.js";

const originalDashboardRedisPrefixes = process.env.DASHBOARD_REDIS_PREFIXES;

/**
 * A socket whose `emit` has real EventEmitter semantics: emitting "error" with
 * no registered listener throws, exactly like ws does for protocol-level frame
 * errors (`receiverOnError` calls `websocket.emit("error", err)`).
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

class FakeWebSocket {
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

function req(url: string): IncomingMessage {
  return { url } as IncomingMessage;
}

async function flushPromises(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
}

beforeEach(() => {
  vi.useFakeTimers();
});

afterEach(() => {
  vi.clearAllTimers();
  vi.useRealTimers();
  if (originalDashboardRedisPrefixes === undefined) delete process.env.DASHBOARD_REDIS_PREFIXES;
  else process.env.DASHBOARD_REDIS_PREFIXES = originalDashboardRedisPrefixes;
});

describe("handleTaskOutputConnection", () => {
  it("does not allocate polling work for an already closed socket", async () => {
    const ws = new FakeWebSocket();
    ws.readyState = WebSocket.CLOSED;
    const findTaskStart = vi.fn();

    handleTaskOutputConnection(ws as unknown as WebSocket, req("/ws/task-output?worker_id=worker-1"), {
      pollIntervalMs: 10,
      pingIntervalMs: 20,
      deps: {
        findTaskStart,
        readTaskOutputFromStream: vi.fn(),
        logError: vi.fn(),
      },
    });
    await flushPromises();
    await vi.advanceTimersByTimeAsync(100);

    expect(findTaskStart).not.toHaveBeenCalled();
    expect(ws.sent).toEqual([]);
    expect(ws.pings).toBe(0);
  });

  it("rejects whitespace-only worker ids without polling", async () => {
    const ws = new FakeWebSocket();
    const findTaskStart = vi.fn();

    const handle = handleTaskOutputConnection(
      ws as unknown as WebSocket,
      req("/ws/task-output?worker_id=%20%20"),
      {
        pollIntervalMs: 10,
        pingIntervalMs: 20,
        deps: {
          findTaskStart,
          readTaskOutputFromStream: vi.fn(),
          logError: vi.fn(),
        },
      },
    );
    await handle.done;
    await vi.advanceTimersByTimeAsync(100);

    expect(ws.closes).toEqual([{ code: 1008, reason: "Missing worker_id" }]);
    expect(findTaskStart).not.toHaveBeenCalled();
    expect(ws.sent).toEqual([]);
    expect(ws.pings).toBe(0);
  });

  it("keeps retrying live task-start lookup until output appears", async () => {
    const ws = new FakeWebSocket();
    const findTaskStart = vi.fn()
      .mockResolvedValueOnce(null)
      .mockResolvedValueOnce({ stream: "output:worker-1", entryId: "1-0" });
    const readTaskOutputFromStream = vi.fn().mockResolvedValue({
      entries: [{ id: "2-0", line: "hello from task" }],
      lastId: "2-0",
      done: false,
    });

    handleTaskOutputConnection(ws as unknown as WebSocket, req("/ws/task-output?worker_id=worker-1"), {
      pollIntervalMs: 10,
      pingIntervalMs: 60000,
      deps: {
        findTaskStart,
        readTaskOutputFromStream,
        logError: vi.fn(),
      },
    });
    await flushPromises();

    expect(ws.messages()).toEqual([{ lines: [], last_id: "0-0", done: false }]);

    await vi.advanceTimersByTimeAsync(10);
    await flushPromises();

    expect(findTaskStart).toHaveBeenCalledTimes(2);
    expect(readTaskOutputFromStream).toHaveBeenCalledWith(
      "output:worker-1",
      "1-0",
      "0-0",
      undefined,
    );
    expect(ws.messages()).toContainEqual({
      lines: ["hello from task"],
      last_id: "2-0",
      done: false,
    });
  });

  it("passes requested Redis prefixes through to task-start lookup", async () => {
    const ws = new FakeWebSocket();
    const findTaskStart = vi.fn()
      .mockResolvedValueOnce({ stream: "project-a:output:worker-1", entryId: "1-0" });
    const readTaskOutputFromStream = vi.fn().mockResolvedValue({
      entries: [],
      lastId: "1-0",
      done: false,
    });

    handleTaskOutputConnection(
      ws as unknown as WebSocket,
      req("/ws/task-output?worker_id=worker-1&task_id=task-1&redis_prefix=project-a"),
      {
        pollIntervalMs: 10,
        pingIntervalMs: 60000,
        deps: {
          findTaskStart,
          readTaskOutputFromStream,
          logError: vi.fn(),
        },
      },
    );
    await flushPromises();

    expect(findTaskStart).toHaveBeenCalledWith("worker-1", "task-1", "project-a");
  });

  it("rejects explicit Redis prefixes outside the configured dashboard prefixes", async () => {
    process.env.DASHBOARD_REDIS_PREFIXES = "project-a";
    const ws = new FakeWebSocket();
    const findTaskStart = vi.fn();

    const handle = handleTaskOutputConnection(
      ws as unknown as WebSocket,
      req("/ws/task-output?worker_id=worker-1&task_id=task-1&redis_prefix=project-b"),
      {
        pollIntervalMs: 10,
        pingIntervalMs: 60000,
        deps: {
          findTaskStart,
          readTaskOutputFromStream: vi.fn(),
          logError: vi.fn(),
        },
      },
    );
    await handle.done;
    await vi.advanceTimersByTimeAsync(100);

    expect(ws.closes).toEqual([{ code: 1008, reason: "Redis prefix is not allowed" }]);
    expect(findTaskStart).not.toHaveBeenCalled();
    expect(ws.sent).toEqual([]);
    expect(ws.pings).toBe(0);
  });

  it("rejects explicit unprefixed output unless unprefixed streams are configured", async () => {
    process.env.DASHBOARD_REDIS_PREFIXES = "project-a";
    const ws = new FakeWebSocket();
    const findTaskStart = vi.fn();

    const handle = handleTaskOutputConnection(
      ws as unknown as WebSocket,
      req("/ws/task-output?worker_id=worker-1&task_id=task-1&redis_prefix="),
      {
        pollIntervalMs: 10,
        pingIntervalMs: 60000,
        deps: {
          findTaskStart,
          readTaskOutputFromStream: vi.fn(),
          logError: vi.fn(),
        },
      },
    );
    await handle.done;
    await vi.advanceTimersByTimeAsync(100);

    expect(ws.closes).toEqual([{ code: 1008, reason: "Redis prefix is not allowed" }]);
    expect(findTaskStart).not.toHaveBeenCalled();
  });

  it("does not fall back to another output prefix when an explicit namespace has no stream", async () => {
    const ws = new FakeWebSocket();
    const findTaskStart = vi.fn()
      .mockResolvedValue(null);
    const readTaskOutputFromStream = vi.fn();

    handleTaskOutputConnection(
      ws as unknown as WebSocket,
      req("/ws/task-output?worker_id=worker-1&task_id=task-project&redis_prefix=project-a&historical=1"),
      {
        pollIntervalMs: 10,
        pingIntervalMs: 60000,
        taskStartLookupMaxAttempts: 1,
        deps: {
          findTaskStart,
          readTaskOutputFromStream,
          logError: vi.fn(),
        },
      },
    );
    await flushPromises();
    await vi.advanceTimersByTimeAsync(1);

    expect(findTaskStart).toHaveBeenCalledTimes(1);
    expect(findTaskStart).toHaveBeenCalledWith("worker-1", "task-project", "project-a");
    expect(readTaskOutputFromStream).not.toHaveBeenCalled();
    expect(ws.messages()).toContainEqual({
      lines: [],
      last_id: "0-0",
      done: true,
      error: "Output for task task-project was not found. The worker output stream may have been trimmed or the task may have finished before output capture started.",
    });
    expect(ws.closes).toContainEqual({
      code: 1000,
      reason: TASK_OUTPUT_UNAVAILABLE_CLOSE_REASON,
    });
  });

  it("trims task output lookup query parameters before polling", async () => {
    const ws = new FakeWebSocket();
    const findTaskStart = vi.fn()
      .mockResolvedValueOnce({ stream: "project-a:output:worker-1", entryId: "1-0" });
    const readTaskOutputFromStream = vi.fn().mockResolvedValue({
      entries: [],
      lastId: "1-0",
      done: false,
    });

    handleTaskOutputConnection(
      ws as unknown as WebSocket,
      req("/ws/task-output?worker_id=%20worker-1%20&task_id=%20task-1%20&redis_prefix=%20project-a%20"),
      {
        pollIntervalMs: 10,
        pingIntervalMs: 60000,
        deps: {
          findTaskStart,
          readTaskOutputFromStream,
          logError: vi.fn(),
        },
      },
    );
    await flushPromises();

    expect(findTaskStart).toHaveBeenCalledWith("worker-1", "task-1", "project-a");
  });

  it("passes an explicit unprefixed Redis stream request through to task-start lookup", async () => {
    process.env.DASHBOARD_REDIS_PREFIXES = "project-a, unprefixed";
    const ws = new FakeWebSocket();
    const findTaskStart = vi.fn()
      .mockResolvedValueOnce({ stream: "output:worker-1", entryId: "1-0" });
    const readTaskOutputFromStream = vi.fn().mockResolvedValue({
      entries: [],
      lastId: "1-0",
      done: false,
    });

    handleTaskOutputConnection(
      ws as unknown as WebSocket,
      req("/ws/task-output?worker_id=worker-1&task_id=task-1&redis_prefix="),
      {
        pollIntervalMs: 10,
        pingIntervalMs: 60000,
        deps: {
          findTaskStart,
          readTaskOutputFromStream,
          logError: vi.fn(),
        },
      },
    );
    await flushPromises();

    expect(findTaskStart).toHaveBeenCalledWith("worker-1", "task-1", null);
  });

  it("bounds exact historical lookup and closes as unavailable", async () => {
    const ws = new FakeWebSocket();
    const findTaskStart = vi.fn().mockResolvedValue(null);

    handleTaskOutputConnection(
      ws as unknown as WebSocket,
      req("/ws/task-output?worker_id=worker-1&task_id=task-1234567890abcdef&historical=1"),
      {
        pollIntervalMs: 10,
        pingIntervalMs: 60000,
        taskStartLookupMaxAttempts: 2,
        deps: {
          findTaskStart,
          readTaskOutputFromStream: vi.fn(),
          logError: vi.fn(),
        },
      },
    );
    await flushPromises();
    await vi.advanceTimersByTimeAsync(10);
    await flushPromises();
    await vi.advanceTimersByTimeAsync(1);

    expect(ws.messages()).toEqual([
      { lines: [], last_id: "0-0", done: false },
      {
        lines: [],
        last_id: "0-0",
        done: true,
        error: "Output for task task-1234567890abcdef was not found. The worker output stream may have been trimmed or the task may have finished before output capture started.",
      },
    ]);
    expect(ws.closes).toContainEqual({
      code: 1000,
      reason: TASK_OUTPUT_UNAVAILABLE_CLOSE_REASON,
    });
  });

  it("reports repeated task-start lookup errors as Redis read failures", async () => {
    const ws = new FakeWebSocket();
    const logError = vi.fn();
    const findTaskStart = vi.fn().mockRejectedValue(new Error("partial output lookup failed"));

    handleTaskOutputConnection(
      ws as unknown as WebSocket,
      req("/ws/task-output?worker_id=worker-1&task_id=task-1&historical=1"),
      {
        pollIntervalMs: 10,
        pingIntervalMs: 60000,
        readErrorLimit: 2,
        deps: {
          findTaskStart,
          readTaskOutputFromStream: vi.fn(),
          logError,
        },
      },
    );
    await flushPromises();
    await vi.advanceTimersByTimeAsync(10);
    await flushPromises();
    await vi.advanceTimersByTimeAsync(1);

    expect(findTaskStart).toHaveBeenCalledTimes(2);
    expect(logError).toHaveBeenCalledWith(
      "Error reading task output:",
      expect.any(Error),
    );
    expect(ws.messages()).toContainEqual({
      lines: [],
      last_id: "0-0",
      done: true,
      error: "Task output could not be read from Redis. Check dashboard Redis connectivity and worker output streams.",
    });
    expect(ws.closes).toContainEqual({
      code: 1000,
      reason: TASK_OUTPUT_UNAVAILABLE_CLOSE_REASON,
    });
  });

  it("sends terminal completion and close reason when the task is done", async () => {
    const ws = new FakeWebSocket();
    const findTaskStart = vi.fn().mockResolvedValue({ stream: "output:worker-1", entryId: "1-0" });
    const readTaskOutputFromStream = vi.fn().mockResolvedValue({
      entries: [{ id: "2-0", line: "done line" }],
      lastId: "2-0",
      done: true,
    });

    handleTaskOutputConnection(
      ws as unknown as WebSocket,
      req("/ws/task-output?worker_id=worker-1&task_id=task-1"),
      {
        pollIntervalMs: 10,
        pingIntervalMs: 60000,
        deps: {
          findTaskStart,
          readTaskOutputFromStream,
          logError: vi.fn(),
        },
      },
    );
    await flushPromises();
    await vi.advanceTimersByTimeAsync(1);

    expect(ws.messages()).toEqual([{
      lines: ["done line"],
      last_id: "2-0",
      done: true,
    }]);
    expect(ws.closes).toContainEqual({
      code: 1000,
      reason: TASK_OUTPUT_COMPLETE_CLOSE_REASON,
    });
  });

  it("resumes reads from a valid after_id cursor", async () => {
    const ws = new FakeWebSocket();
    const findTaskStart = vi.fn().mockResolvedValue({ stream: "output:worker-1", entryId: "1-0" });
    const readTaskOutputFromStream = vi.fn().mockResolvedValue({
      entries: [{ id: "6-0", line: "resumed line" }],
      lastId: "6-0",
      done: false,
    });

    handleTaskOutputConnection(
      ws as unknown as WebSocket,
      req("/ws/task-output?worker_id=worker-1&task_id=task-1&after_id=5-0"),
      {
        pollIntervalMs: 10,
        pingIntervalMs: 60000,
        deps: {
          findTaskStart,
          readTaskOutputFromStream,
          logError: vi.fn(),
        },
      },
    );
    await flushPromises();

    expect(readTaskOutputFromStream).toHaveBeenCalledWith(
      "output:worker-1",
      "1-0",
      "5-0",
      "task-1",
    );
    await flushPromises();
    expect(ws.messages()).toEqual([{
      lines: ["resumed line"],
      last_id: "6-0",
      done: false,
    }]);
  });

  it("terminates task output sockets that miss a heartbeat pong", async () => {
    const ws = new FakeWebSocket();
    const findTaskStart = vi.fn().mockResolvedValue(null);
    const handle = handleTaskOutputConnection(
      ws as unknown as WebSocket,
      req("/ws/task-output?worker_id=worker-1"),
      {
        pollIntervalMs: 100,
        pingIntervalMs: 10,
        deps: {
          findTaskStart,
          readTaskOutputFromStream: vi.fn(),
          logError: vi.fn(),
        },
      },
    );
    await flushPromises();

    expect(findTaskStart).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(10);
    expect(ws.pings).toBe(1);
    expect(ws.terminated).toBe(false);

    await vi.advanceTimersByTimeAsync(10);
    await handle.done;
    expect(ws.terminated).toBe(true);

    await vi.advanceTimersByTimeAsync(100);
    expect(findTaskStart).toHaveBeenCalledTimes(1);
  });

  it("keeps task output sockets alive when heartbeat pongs arrive", async () => {
    const ws = new FakeWebSocket();
    const handle = handleTaskOutputConnection(
      ws as unknown as WebSocket,
      req("/ws/task-output?worker_id=worker-1"),
      {
        pollIntervalMs: 100,
        pingIntervalMs: 10,
        deps: {
          findTaskStart: vi.fn().mockResolvedValue(null),
          readTaskOutputFromStream: vi.fn(),
          logError: vi.fn(),
        },
      },
    );
    await flushPromises();

    await vi.advanceTimersByTimeAsync(10);
    ws.emit("pong");
    await vi.advanceTimersByTimeAsync(10);

    expect(ws.pings).toBe(2);
    expect(ws.terminated).toBe(false);

    handle.close();
    await handle.done;
  });

  it("terminates send failures without reporting them as Redis read failures", async () => {
    const ws = new FakeWebSocket();
    ws.throwOnSend = true;
    const logError = vi.fn();
    const handle = handleTaskOutputConnection(
      ws as unknown as WebSocket,
      req("/ws/task-output?worker_id=worker-1&task_id=task-1"),
      {
        pollIntervalMs: 10,
        pingIntervalMs: 60000,
        deps: {
          findTaskStart: vi.fn().mockResolvedValue({ stream: "output:worker-1", entryId: "1-0" }),
          readTaskOutputFromStream: vi.fn().mockResolvedValue({
            entries: [{ id: "2-0", line: "line that cannot be sent" }],
            lastId: "2-0",
            done: false,
          }),
          logError,
        },
      },
    );
    await flushPromises();
    await handle.done;

    expect(ws.terminated).toBe(true);
    expect(ws.sent).toEqual([]);
    expect(logError).toHaveBeenCalledTimes(1);
    expect(logError).toHaveBeenCalledWith("Error sending task output:", expect.any(Error));
  });

  it("survives a protocol error on every early-reject path", () => {
    // ws emits "error" for malformed frames, and an EventEmitter "error" with no
    // listener throws — which took the whole dashboard process down, because the
    // error listener used to be attached only after these early returns.
    process.env.DASHBOARD_REDIS_PREFIXES = "orcest";
    const rejects: Array<{ name: string; url: string; options: Record<string, unknown> }> = [
      { name: "missing worker_id", url: "/ws/task-output", options: {} },
      {
        name: "over connection limit",
        url: "/ws/task-output?worker_id=worker-1",
        options: { activeConnections: 21, maxConnections: 20 },
      },
      {
        name: "disallowed redis_prefix",
        url: "/ws/task-output?worker_id=worker-1&redis_prefix=other",
        options: {},
      },
    ];

    for (const reject of rejects) {
      const ws = new EmitterWebSocket();
      const logError = vi.fn();
      const findTaskStart = vi.fn();

      handleTaskOutputConnection(ws as unknown as WebSocket, req(reject.url), {
        ...reject.options,
        deps: { findTaskStart, readTaskOutputFromStream: vi.fn(), logError },
      });

      expect(findTaskStart, reject.name).not.toHaveBeenCalled();
      expect(ws.closes, reject.name).toHaveLength(1);
      expect(
        () => ws.emit("error", new Error("Invalid WebSocket frame: RSV1 must be clear")),
        reject.name,
      ).not.toThrow();
      expect(logError, reject.name).toHaveBeenCalledWith(
        "Task output WS error:",
        expect.any(Error),
      );
    }
  });

  it("terminates a client whose send queue outgrows the backpressure limit", async () => {
    const ws = new FakeWebSocket();
    ws.bufferedAmount = 5 * 1024 * 1024;
    const logError = vi.fn();

    const handle = handleTaskOutputConnection(
      ws as unknown as WebSocket,
      req("/ws/task-output?worker_id=worker-1&task_id=task-1"),
      {
        pollIntervalMs: 10,
        pingIntervalMs: 60000,
        maxBufferedBytes: 4 * 1024 * 1024,
        deps: {
          findTaskStart: vi.fn().mockResolvedValue({ stream: "output:worker-1", entryId: "1-0" }),
          readTaskOutputFromStream: vi.fn().mockResolvedValue({
            entries: [{ id: "2-0", line: "line for a client that stopped reading" }],
            lastId: "2-0",
            done: false,
          }),
          logError,
        },
      },
    );
    await flushPromises();
    await handle.done;

    expect(ws.sent).toEqual([]);
    expect(ws.terminated).toBe(true);
    expect(logError).toHaveBeenCalledWith(
      "Task output WS backpressure limit exceeded; terminating client:",
      5 * 1024 * 1024,
    );
  });

  it("keeps sending while the send queue stays under the backpressure limit", async () => {
    const ws = new FakeWebSocket();
    ws.bufferedAmount = 1024;

    const handle = handleTaskOutputConnection(
      ws as unknown as WebSocket,
      req("/ws/task-output?worker_id=worker-1&task_id=task-1"),
      {
        pollIntervalMs: 10,
        pingIntervalMs: 60000,
        maxBufferedBytes: 4 * 1024 * 1024,
        deps: {
          findTaskStart: vi.fn().mockResolvedValue({ stream: "output:worker-1", entryId: "1-0" }),
          readTaskOutputFromStream: vi.fn().mockResolvedValue({
            entries: [{ id: "2-0", line: "line" }],
            lastId: "2-0",
            done: true,
          }),
          logError: vi.fn(),
        },
      },
    );
    await flushPromises();
    await vi.advanceTimersByTimeAsync(10);
    await handle.done;

    expect(ws.terminated).toBe(false);
    expect(ws.messages()).toEqual([{ lines: ["line"], last_id: "2-0", done: true }]);
  });
});
