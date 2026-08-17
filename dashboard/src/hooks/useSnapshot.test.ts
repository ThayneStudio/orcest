/**
 * @vitest-environment happy-dom
 */
import { act, renderHook } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { SNAPSHOT_PROTOCOL_ERROR } from "../lib/connection";
import { resetDashboardAuthTokenForTesting } from "../lib/authToken";
import type { DashboardMessage, SystemSnapshot } from "../lib/types";
import { useSnapshot } from "./useSnapshot";

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

type SnapshotHookState = ReturnType<typeof useSnapshot>;

function snapshot(overrides: Partial<SystemSnapshot> = {}): SystemSnapshot {
  return {
    redis_ok: true,
    fetched_at: "2026-06-20T00:00:00.000Z",
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
      pending_task_ttl_seconds: 5700,
      lock_ttl_seconds: 5400,
    },
    ...overrides,
  };
}

describe("useSnapshot", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-20T02:00:00.000Z"));
    resetDashboardAuthTokenForTesting();
    (globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
    FakeWebSocket.instances = [];
    vi.stubGlobal("window", {
      location: {
        protocol: "http:",
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

  it("connects with the dashboard token and applies snapshot messages", async () => {
    const { result, unmount } = renderHook<SnapshotHookState, void>(() => useSnapshot());

    try {
      expect(FakeWebSocket.instances).toHaveLength(1);
      expect(FakeWebSocket.instances[0].url).toBe(
        "ws://dashboard.local/ws/snapshot?token=dev-token",
      );

      await act(async () => {
        FakeWebSocket.instances[0].open();
      });
      expect(result.current.connected).toBe(true);
      expect(result.current.error).toBeNull();

      const msg: DashboardMessage = {
        snapshot: snapshot({ fetched_at: "2026-06-20T01:02:03.000Z" }),
        stuck_tasks: [{
          prefix: null,
          resource_type: "pr",
          repo: "owner/repo",
          resource_id: "42",
          reason: "stale pending task",
          severity: "warning",
        }],
        workers: ["orcest-worker-300"],
      };

      await act(async () => {
        FakeWebSocket.instances[0].message(msg);
      });

      expect(result.current.snapshot?.fetched_at).toBe("2026-06-20T01:02:03.000Z");
      expect(result.current.stuckTasks).toHaveLength(1);
      expect(result.current.workers).toEqual(["orcest-worker-300"]);
      // Staleness clock: browser receipt time, not the server's fetched_at.
      expect(result.current.lastUpdate?.toISOString()).toBe("2026-06-20T02:00:00.000Z");
      // Server time is retained for display only.
      expect(result.current.serverFetchedAt?.toISOString()).toBe("2026-06-20T01:02:03.000Z");
    } finally {
      unmount();
    }
  });

  it("keeps the staleness clock local when the server clock is skewed", async () => {
    const { result, unmount } = renderHook<SnapshotHookState, void>(() => useSnapshot());

    try {
      await act(async () => {
        FakeWebSocket.instances[0].open();
        // Server clock 60s AHEAD of the browser. Using it for freshness would
        // clamp the age to 0 forever and pin the badge green on a hung feed.
        FakeWebSocket.instances[0].message({
          snapshot: snapshot({ fetched_at: "2026-06-20T02:01:00.000Z" }),
          stuck_tasks: [],
          workers: [],
        });
      });

      expect(result.current.lastUpdate?.toISOString()).toBe("2026-06-20T02:00:00.000Z");
      expect(result.current.serverFetchedAt?.toISOString()).toBe("2026-06-20T02:01:00.000Z");

      // Twenty seconds later the receipt-based age is a real 20s, which the
      // stale threshold (15s) can actually reach.
      expect(
        Math.floor((Date.now() + 20_000 - (result.current.lastUpdate?.getTime() ?? 0)) / 1000),
      ).toBe(20);
    } finally {
      unmount();
    }
  });

  it("still stamps a receipt time when the snapshot carries no usable fetched_at", async () => {
    const { result, unmount } = renderHook<SnapshotHookState, void>(() => useSnapshot());

    try {
      await act(async () => {
        FakeWebSocket.instances[0].open();
        FakeWebSocket.instances[0].message({
          snapshot: snapshot({ fetched_at: "" }),
          stuck_tasks: [],
          workers: [],
        });
      });

      expect(result.current.lastUpdate?.toISOString()).toBe("2026-06-20T02:00:00.000Z");
      expect(result.current.serverFetchedAt).toBeNull();
    } finally {
      unmount();
    }
  });

  it("reconnects with exponential backoff after a non-terminal close", async () => {
    const { result, unmount } = renderHook<SnapshotHookState, void>(() => useSnapshot());

    try {
      await act(async () => {
        FakeWebSocket.instances[0].open();
        FakeWebSocket.instances[0].close(1006, "");
      });

      expect(result.current.connected).toBe(false);
      expect(result.current.error).toBe(
        "Dashboard connection could not be opened. Refresh with a valid dashboard token or check connectivity.",
      );
      expect(FakeWebSocket.instances).toHaveLength(1);
      window.location.search = "";

      await act(async () => {
        vi.advanceTimersByTime(999);
      });
      expect(FakeWebSocket.instances).toHaveLength(1);

      await act(async () => {
        vi.advanceTimersByTime(1);
      });
      expect(FakeWebSocket.instances).toHaveLength(2);
      expect(FakeWebSocket.instances[1].url).toBe(
        "ws://dashboard.local/ws/snapshot?token=dev-token",
      );
    } finally {
      unmount();
    }
  });

  it("does not show an auth/connectivity error on the first transient reconnect after a snapshot", async () => {
    const { result, unmount } = renderHook<SnapshotHookState, void>(() => useSnapshot());

    try {
      const msg: DashboardMessage = {
        snapshot: snapshot({ fetched_at: "2026-06-20T01:02:03.000Z" }),
        stuck_tasks: [],
        workers: [],
      };

      await act(async () => {
        FakeWebSocket.instances[0].open();
        FakeWebSocket.instances[0].message(msg);
        FakeWebSocket.instances[0].close(1006, "");
      });

      expect(result.current.connected).toBe(false);
      expect(result.current.error).toBeNull();
      expect(result.current.lastUpdate?.toISOString()).toBe("2026-06-20T02:00:00.000Z");

      for (let attempt = 1; attempt < 4; attempt += 1) {
        await act(async () => {
          vi.advanceTimersByTime(1000 * Math.pow(2, attempt - 1));
        });
        expect(FakeWebSocket.instances).toHaveLength(attempt + 1);

        await act(async () => {
          FakeWebSocket.instances[attempt].open();
          FakeWebSocket.instances[attempt].close(1006, "");
        });
        expect(result.current.error).toBeNull();
      }

      await act(async () => {
        vi.advanceTimersByTime(8000);
      });
      expect(FakeWebSocket.instances).toHaveLength(5);

      await act(async () => {
        FakeWebSocket.instances[4].open();
        FakeWebSocket.instances[4].close(1006, "");
      });

      expect(result.current.error).toBe(
        "Dashboard connection could not be opened. Refresh with a valid dashboard token or check connectivity.",
      );
    } finally {
      unmount();
    }
  });

  it("surfaces reconnect auth failures immediately when the retry socket never opens", async () => {
    const { result, unmount } = renderHook<SnapshotHookState, void>(() => useSnapshot());

    try {
      const msg: DashboardMessage = {
        snapshot: snapshot({ fetched_at: "2026-06-20T01:02:03.000Z" }),
        stuck_tasks: [],
        workers: [],
      };

      await act(async () => {
        FakeWebSocket.instances[0].open();
        FakeWebSocket.instances[0].message(msg);
        FakeWebSocket.instances[0].close(1006, "");
      });

      expect(result.current.error).toBeNull();

      await act(async () => {
        vi.advanceTimersByTime(1000);
      });
      expect(FakeWebSocket.instances).toHaveLength(2);

      await act(async () => {
        FakeWebSocket.instances[1].close(1006, "");
      });

      expect(result.current.error).toBe(
        "Dashboard connection could not be opened. Refresh with a valid dashboard token or check connectivity.",
      );
    } finally {
      unmount();
    }
  });

  it("surfaces malformed snapshot frames as protocol errors", async () => {
    const { result, unmount } = renderHook<SnapshotHookState, void>(() => useSnapshot());

    try {
      await act(async () => {
        FakeWebSocket.instances[0].open();
        FakeWebSocket.instances[0].rawMessage("{");
      });

      expect(result.current).toMatchObject({
        connected: false,
        error: SNAPSHOT_PROTOCOL_ERROR,
      });
      expect(FakeWebSocket.instances[0].closed).toBe(true);
    } finally {
      unmount();
    }
  });

  it("surfaces structurally invalid snapshot messages as protocol errors", async () => {
    const { result, unmount } = renderHook<SnapshotHookState, void>(() => useSnapshot());

    try {
      await act(async () => {
        FakeWebSocket.instances[0].open();
        FakeWebSocket.instances[0].message({ ok: true });
      });

      expect(result.current).toMatchObject({
        connected: false,
        error: SNAPSHOT_PROTOCOL_ERROR,
      });
      expect(FakeWebSocket.instances[0].closed).toBe(true);
    } finally {
      unmount();
    }
  });
});
