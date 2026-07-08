/**
 * @vitest-environment happy-dom
 */
import { act, renderHook } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { TaskOutputParams } from "../lib/types";
import {
  TASK_OUTPUT_MALFORMED_MESSAGE_ERROR,
  TASK_OUTPUT_PROTOCOL_ERROR,
  type TaskOutputClientState,
} from "../lib/taskOutput";
import { resetDashboardAuthTokenForTesting } from "../lib/authToken";
import { useTaskOutput } from "./useTaskOutput";

class FakeWebSocket {
  static instances: FakeWebSocket[] = [];

  readonly url: string;
  onopen: (() => void) | null = null;
  onmessage: ((event: { data: string }) => void) | null = null;
  onclose: ((event: { code: number; reason: string }) => void) | null = null;
  onerror: (() => void) | null = null;
  closed = false;

  constructor(url: string) {
    this.url = url;
    FakeWebSocket.instances.push(this);
  }

  open() {
    this.onopen?.();
  }

  message(value: unknown) {
    this.onmessage?.({ data: JSON.stringify(value) });
  }

  rawMessage(data: string) {
    this.onmessage?.({ data });
  }

  close(code = 1000, reason = "") {
    if (this.closed) return;
    this.closed = true;
    this.onclose?.({ code, reason });
  }
}

describe("useTaskOutput", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    resetDashboardAuthTokenForTesting();
    (globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
    FakeWebSocket.instances = [];
    vi.stubGlobal("window", {
      location: {
        protocol: "https:",
        host: "dashboard.local",
        search: "?token=dev-token",
      },
    });
    vi.stubGlobal("WebSocket", FakeWebSocket);
  });

  afterEach(() => {
    resetDashboardAuthTokenForTesting();
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it("does not open a socket until output params are available", async () => {
    const { result, unmount } = renderHook<TaskOutputClientState, TaskOutputParams | null>(
      (params) => useTaskOutput(params),
      { initialProps: null },
    );

    try {
      expect(FakeWebSocket.instances).toHaveLength(0);
      expect(result.current).toEqual({
        lines: [],
        startIndex: 0,
        connected: false,
        retrying: false,
        done: false,
        error: null,
      });
    } finally {
      unmount();
    }
  });

  it("does not open a socket for worker ids that are blank after trimming", async () => {
    window.location.search = "";
    const { result, unmount } = renderHook<TaskOutputClientState, TaskOutputParams | null>(
      (params) => useTaskOutput(params),
      { initialProps: { workerId: "   ", taskId: "task-1", historical: true } },
    );

    try {
      expect(FakeWebSocket.instances).toHaveLength(0);
      expect(result.current).toEqual({
        lines: [],
        startIndex: 0,
        connected: false,
        retrying: false,
        done: false,
        error: null,
      });
    } finally {
      unmount();
    }
  });

  it("normalizes output params before connecting", async () => {
    window.location.search = "";
    const { unmount } = renderHook<TaskOutputClientState, TaskOutputParams | null>(
      (params) => useTaskOutput(params),
      {
        initialProps: {
          workerId: " worker-1 ",
          taskId: " task-1 ",
          historical: true,
          prefix: " project-a ",
        },
      },
    );

    try {
      expect(FakeWebSocket.instances).toHaveLength(1);
      expect(FakeWebSocket.instances[0].url).toBe(
        "wss://dashboard.local/ws/task-output?worker_id=worker-1&task_id=task-1&redis_prefix=project-a&historical=1",
      );
    } finally {
      unmount();
    }
  });

  it("streams output and resumes reconnects from the last received ID", async () => {
    window.location.search = "";
    const { result, unmount } = renderHook<TaskOutputClientState, TaskOutputParams | null>(
      (params) => useTaskOutput(params),
      {
        initialProps: { workerId: "orcest-worker-300", taskId: "task-1", historical: true },
      },
    );

    try {
      expect(FakeWebSocket.instances).toHaveLength(1);
      expect(FakeWebSocket.instances[0].url).toBe(
        "wss://dashboard.local/ws/task-output?worker_id=orcest-worker-300&task_id=task-1&historical=1",
      );

      await act(async () => {
        FakeWebSocket.instances[0].open();
        FakeWebSocket.instances[0].message({
          lines: ["first", "second"],
          last_id: "7-0",
          done: false,
        });
      });

      expect(result.current.connected).toBe(true);
      expect(result.current.lines).toEqual(["first", "second"]);

      await act(async () => {
        FakeWebSocket.instances[0].close(1006, "");
      });

      expect(result.current.connected).toBe(false);
      expect(result.current.retrying).toBe(true);
      expect(FakeWebSocket.instances).toHaveLength(1);

      await act(async () => {
        vi.advanceTimersByTime(1000);
      });

      expect(FakeWebSocket.instances).toHaveLength(2);
      expect(result.current.retrying).toBe(true);
      expect(FakeWebSocket.instances[1].url).toBe(
        "wss://dashboard.local/ws/task-output?worker_id=orcest-worker-300&task_id=task-1&historical=1&after_id=7-0",
      );
    } finally {
      unmount();
    }
  });

  it("ignores stale output frames without moving the reconnect cursor backward", async () => {
    window.location.search = "";
    const { result, unmount } = renderHook<TaskOutputClientState, TaskOutputParams | null>(
      (params) => useTaskOutput(params),
      {
        initialProps: { workerId: "orcest-worker-300", taskId: "task-1", historical: true },
      },
    );

    try {
      await act(async () => {
        FakeWebSocket.instances[0].open();
        FakeWebSocket.instances[0].message({
          lines: ["new output"],
          last_id: "7-0",
          done: false,
        });
        FakeWebSocket.instances[0].message({
          lines: ["stale output"],
          last_id: "6-0",
          done: false,
        });
      });

      expect(result.current.lines).toEqual(["new output"]);

      await act(async () => {
        FakeWebSocket.instances[0].close(1006, "");
      });
      await act(async () => {
        vi.advanceTimersByTime(1000);
      });

      expect(FakeWebSocket.instances).toHaveLength(2);
      expect(FakeWebSocket.instances[1].url).toBe(
        "wss://dashboard.local/ws/task-output?worker_id=orcest-worker-300&task_id=task-1&historical=1&after_id=7-0",
      );
    } finally {
      unmount();
    }
  });

  it("ignores duplicate output frames at the current cursor", async () => {
    window.location.search = "";
    const { result, unmount } = renderHook<TaskOutputClientState, TaskOutputParams | null>(
      (params) => useTaskOutput(params),
      {
        initialProps: { workerId: "orcest-worker-300", taskId: "task-1", historical: true },
      },
    );

    try {
      await act(async () => {
        FakeWebSocket.instances[0].open();
        FakeWebSocket.instances[0].message({
          lines: ["new output"],
          last_id: "7-0",
          done: false,
        });
        FakeWebSocket.instances[0].message({
          lines: ["duplicate output"],
          last_id: "7-0",
          done: false,
        });
      });

      expect(result.current.lines).toEqual(["new output"]);
    } finally {
      unmount();
    }
  });

  it("ignores stale terminal frames but accepts terminal frames at the current cursor", async () => {
    window.location.search = "";
    const { result, unmount } = renderHook<TaskOutputClientState, TaskOutputParams | null>(
      (params) => useTaskOutput(params),
      {
        initialProps: { workerId: "orcest-worker-300", taskId: "task-1", historical: true },
      },
    );

    try {
      await act(async () => {
        FakeWebSocket.instances[0].open();
        FakeWebSocket.instances[0].message({
          lines: ["new output"],
          last_id: "7-0",
          done: false,
        });
        FakeWebSocket.instances[0].message({
          lines: [],
          last_id: "6-0",
          done: true,
        });
      });

      expect(result.current).toMatchObject({
        lines: ["new output"],
        done: false,
        error: null,
      });

      await act(async () => {
        FakeWebSocket.instances[0].message({
          lines: [],
          last_id: "7-0",
          done: true,
        });
      });

      expect(result.current).toMatchObject({
        lines: ["new output"],
        done: true,
        error: null,
      });
    } finally {
      unmount();
    }
  });

  it("stops without appending output when a frame has no resumable cursor", async () => {
    window.location.search = "";
    const { result, unmount } = renderHook<TaskOutputClientState, TaskOutputParams | null>(
      (params) => useTaskOutput(params),
      {
        initialProps: { workerId: "orcest-worker-300", taskId: "task-1", historical: true },
      },
    );

    try {
      await act(async () => {
        FakeWebSocket.instances[0].open();
        FakeWebSocket.instances[0].message({
          lines: ["unsafe output"],
          last_id: "1-*",
          done: false,
        });
      });

      expect(result.current).toMatchObject({
        connected: false,
        retrying: false,
        done: true,
        error: TASK_OUTPUT_PROTOCOL_ERROR,
      });
      expect(result.current.lines).toEqual([]);

      await act(async () => {
        FakeWebSocket.instances[0].message({
          lines: ["late output"],
          last_id: "8-0",
          done: false,
        });
        FakeWebSocket.instances[0].close(1006, "");
        vi.advanceTimersByTime(30_000);
      });

      expect(result.current.lines).toEqual([]);
      expect(result.current.error).toBe(TASK_OUTPUT_PROTOCOL_ERROR);
      expect(FakeWebSocket.instances).toHaveLength(1);
    } finally {
      unmount();
    }
  });

  it("stops and reports malformed JSON task output frames", async () => {
    window.location.search = "";
    const { result, unmount } = renderHook<TaskOutputClientState, TaskOutputParams | null>(
      (params) => useTaskOutput(params),
      {
        initialProps: { workerId: "orcest-worker-300", taskId: "task-1", historical: true },
      },
    );

    try {
      await act(async () => {
        FakeWebSocket.instances[0].open();
        FakeWebSocket.instances[0].rawMessage("{");
      });

      expect(result.current).toMatchObject({
        connected: false,
        retrying: false,
        done: true,
        error: TASK_OUTPUT_MALFORMED_MESSAGE_ERROR,
      });
      expect(FakeWebSocket.instances[0].closed).toBe(true);

      await act(async () => {
        vi.advanceTimersByTime(30_000);
      });

      expect(FakeWebSocket.instances).toHaveLength(1);
    } finally {
      unmount();
    }
  });

  it("stops and reports structurally malformed task output frames", async () => {
    window.location.search = "";
    const { result, unmount } = renderHook<TaskOutputClientState, TaskOutputParams | null>(
      (params) => useTaskOutput(params),
      {
        initialProps: { workerId: "orcest-worker-300", taskId: "task-1", historical: true },
      },
    );

    try {
      await act(async () => {
        FakeWebSocket.instances[0].open();
        FakeWebSocket.instances[0].message({ ok: true });
      });

      expect(result.current).toMatchObject({
        connected: false,
        retrying: false,
        done: true,
        error: TASK_OUTPUT_MALFORMED_MESSAGE_ERROR,
      });
      expect(FakeWebSocket.instances[0].closed).toBe(true);

      await act(async () => {
        vi.advanceTimersByTime(30_000);
      });

      expect(FakeWebSocket.instances).toHaveLength(1);
    } finally {
      unmount();
    }
  });

  it("scopes task output sockets to prefixed and unprefixed Redis streams", async () => {
    window.location.search = "";
    const { rerender, unmount } = renderHook<TaskOutputClientState, TaskOutputParams | null>(
      (params) => useTaskOutput(params),
      {
        initialProps: {
          workerId: "worker-1",
          taskId: "task-1",
          historical: true,
          prefix: "project-a",
        },
      },
    );

    try {
      expect(FakeWebSocket.instances[0].url).toBe(
        "wss://dashboard.local/ws/task-output?worker_id=worker-1&task_id=task-1&redis_prefix=project-a&historical=1",
      );

      await act(async () => {
        rerender({
          workerId: "worker-1",
          taskId: "task-1",
          historical: true,
          prefix: null,
        });
      });

      expect(FakeWebSocket.instances[0].closed).toBe(true);
      expect(FakeWebSocket.instances[1].url).toBe(
        "wss://dashboard.local/ws/task-output?worker_id=worker-1&task_id=task-1&redis_prefix=&historical=1",
      );
    } finally {
      unmount();
    }
  });

  it("treats terminal close reasons as complete and does not reconnect", async () => {
    const { result, unmount } = renderHook<TaskOutputClientState, TaskOutputParams | null>(
      (params) => useTaskOutput(params),
      { initialProps: { workerId: "orcest-worker-300", taskId: "task-1" } },
    );

    try {
      await act(async () => {
        FakeWebSocket.instances[0].open();
        FakeWebSocket.instances[0].close(1000, "Task output complete");
      });

      expect(result.current).toMatchObject({
        connected: false,
        retrying: false,
        done: true,
        error: null,
      });

      await act(async () => {
        vi.advanceTimersByTime(30_000);
      });

      expect(FakeWebSocket.instances).toHaveLength(1);
    } finally {
      unmount();
    }
  });

  it.each([
    {
      code: 1008,
      reason: "Task output request was rejected for this worker.",
      error: "Task output request was rejected for this worker.",
    },
    {
      code: 1013,
      reason: "",
      error: "Too many task output streams are open.",
    },
  ])("surfaces terminal close code $code without reconnecting", async ({ code, reason, error }) => {
    const { result, unmount } = renderHook<TaskOutputClientState, TaskOutputParams | null>(
      (params) => useTaskOutput(params),
      { initialProps: { workerId: "orcest-worker-300", taskId: "task-1" } },
    );

    try {
      await act(async () => {
        FakeWebSocket.instances[0].open();
        FakeWebSocket.instances[0].close(code, reason);
      });

      expect(result.current).toMatchObject({
        connected: false,
        retrying: false,
        done: true,
        error,
      });

      await act(async () => {
        vi.advanceTimersByTime(30_000);
      });

      expect(FakeWebSocket.instances).toHaveLength(1);
    } finally {
      unmount();
    }
  });

  it("stops retrying and reports an error after repeated abnormal closes", async () => {
    const { result, unmount } = renderHook<TaskOutputClientState, TaskOutputParams | null>(
      (params) => useTaskOutput(params),
      { initialProps: { workerId: "orcest-worker-300", taskId: "task-1", historical: true } },
    );

    try {
      for (let attempt = 0; attempt < 4; attempt += 1) {
        await act(async () => {
          FakeWebSocket.instances[attempt].open();
          FakeWebSocket.instances[attempt].close(1006, "");
        });

        expect(result.current.done).toBe(false);
        expect(result.current.retrying).toBe(true);

        await act(async () => {
          vi.advanceTimersByTime(1000 * Math.pow(2, attempt));
        });

        expect(FakeWebSocket.instances).toHaveLength(attempt + 2);
      }

      await act(async () => {
        FakeWebSocket.instances[4].open();
        FakeWebSocket.instances[4].close(1006, "");
      });

      expect(result.current).toMatchObject({
        connected: false,
        retrying: false,
        done: true,
        error: "Task output connection could not be opened. Refresh with a valid dashboard token or check connectivity.",
      });

      await act(async () => {
        vi.advanceTimersByTime(30_000);
      });

      expect(FakeWebSocket.instances).toHaveLength(5);
    } finally {
      unmount();
    }
  });

  it("keeps retrying live output after repeated abnormal closes", async () => {
    const { result, unmount } = renderHook<TaskOutputClientState, TaskOutputParams | null>(
      (params) => useTaskOutput(params),
      { initialProps: { workerId: "orcest-worker-300", taskId: "task-1" } },
    );

    try {
      for (let attempt = 0; attempt < 5; attempt += 1) {
        await act(async () => {
          FakeWebSocket.instances[attempt].open();
          FakeWebSocket.instances[attempt].close(1006, "");
        });

        expect(result.current.done).toBe(false);
        expect(result.current.retrying).toBe(true);

        await act(async () => {
          vi.advanceTimersByTime(Math.min(1000 * Math.pow(2, attempt), 30000));
        });
      }

      expect(FakeWebSocket.instances).toHaveLength(6);
      expect(result.current).toMatchObject({
        connected: false,
        retrying: true,
        done: false,
        error: "Task output connection could not be opened. Refresh with a valid dashboard token or check connectivity.",
      });

      await act(async () => {
        FakeWebSocket.instances[5].open();
      });

      expect(result.current).toMatchObject({
        connected: true,
        retrying: false,
        done: false,
        error: null,
      });
    } finally {
      unmount();
    }
  });

  it("does not reset repeated abnormal close accounting on empty keepalive frames", async () => {
    const { result, unmount } = renderHook<TaskOutputClientState, TaskOutputParams | null>(
      (params) => useTaskOutput(params),
      { initialProps: { workerId: "orcest-worker-300", taskId: "task-1", historical: true } },
    );

    try {
      for (let attempt = 0; attempt < 4; attempt += 1) {
        await act(async () => {
          FakeWebSocket.instances[attempt].open();
          FakeWebSocket.instances[attempt].message({
            lines: [],
            last_id: "0-0",
            done: false,
          });
          FakeWebSocket.instances[attempt].close(1006, "");
        });

        expect(result.current.done).toBe(false);
        expect(result.current.retrying).toBe(true);

        await act(async () => {
          vi.advanceTimersByTime(1000 * Math.pow(2, attempt));
        });

        expect(FakeWebSocket.instances).toHaveLength(attempt + 2);
      }

      await act(async () => {
        FakeWebSocket.instances[4].open();
        FakeWebSocket.instances[4].message({
          lines: [],
          last_id: "0-0",
          done: false,
        });
        FakeWebSocket.instances[4].close(1006, "");
      });

      expect(result.current).toMatchObject({
        connected: false,
        retrying: false,
        done: true,
        error: "Task output connection could not be opened. Refresh with a valid dashboard token or check connectivity.",
      });
    } finally {
      unmount();
    }
  });

  it("closes stale sockets when params change and starts a fresh output stream", async () => {
    window.location.search = "";
    const { result, rerender, unmount } = renderHook<
      TaskOutputClientState,
      TaskOutputParams | null
    >(
      (params) => useTaskOutput(params),
      { initialProps: { workerId: "orcest-worker-300", taskId: "task-1" } },
    );
    const firstSocket = FakeWebSocket.instances[0];

    try {
      await act(async () => {
        firstSocket.open();
        firstSocket.message({ lines: ["old output"], last_id: "3-0", done: false });
        rerender({ workerId: "orcest-worker-301", taskId: "task-2" });
      });

      expect(firstSocket.closed).toBe(true);
      expect(result.current.lines).toEqual([]);
      expect(result.current.connected).toBe(false);
      expect(result.current.retrying).toBe(false);
      expect(FakeWebSocket.instances).toHaveLength(2);
      expect(FakeWebSocket.instances[1].url).toBe(
        "wss://dashboard.local/ws/task-output?worker_id=orcest-worker-301&task_id=task-2",
      );
    } finally {
      unmount();
    }
  });
});
