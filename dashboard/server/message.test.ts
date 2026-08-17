import { describe, expect, it, vi } from "vitest";
import type { StuckTask, SystemSnapshot } from "./types.js";
import { buildDashboardMessage } from "./message.js";
import { WorkerDiscoveryPartialError } from "./workers.js";

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

function snapshot(overrides: Partial<SystemSnapshot> = {}): SystemSnapshot {
  return {
    redis_ok: true,
    fetched_at: "2026-06-18T00:00:00Z",
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
      pending_task_ttl_seconds: 16500,
      lock_ttl_seconds: 180,
    },
    ...overrides,
  };
}

describe("buildDashboardMessage", () => {
  it("sends Redis-down snapshots without Redis-backed enrichment", async () => {
    const redisDown = snapshot({ redis_ok: false });
    const detectStuck = vi.fn().mockRejectedValue(new Error("redis down"));
    const discoverWorkers = vi.fn().mockRejectedValue(new Error("redis down"));

    await expect(buildDashboardMessage({
      fetchSnapshot: async () => redisDown,
      detectStuck,
      discoverWorkers,
    })).resolves.toEqual({
      snapshot: redisDown,
      stuck_tasks: [],
      workers: [],
    });

    expect(detectStuck).not.toHaveBeenCalled();
    expect(discoverWorkers).not.toHaveBeenCalled();
  });

  it("keeps the snapshot visible when enrichment fails", async () => {
    const okSnapshot = snapshot();
    const logError = vi.fn();

    await expect(buildDashboardMessage({
      fetchSnapshot: async () => okSnapshot,
      detectStuck: vi.fn().mockRejectedValue(new Error("xpending failed")),
      discoverWorkers: vi.fn().mockResolvedValue(["worker-1"]),
      logError,
    })).resolves.toEqual({
      snapshot: okSnapshot,
      stuck_tasks: [],
      workers: ["worker-1"],
    });

    expect(logError).toHaveBeenCalledWith(
      "Error detecting stuck tasks:",
      expect.any(Error),
    );
    expect(okSnapshot.degraded_sections).toEqual(["stuck tasks"]);
  });

  it("marks worker discovery degraded when worker enrichment fails", async () => {
    const okSnapshot = snapshot({ degraded_sections: ["recent results"] });
    const logError = vi.fn();

    const message = await buildDashboardMessage({
      fetchSnapshot: async () => okSnapshot,
      detectStuck: vi.fn().mockResolvedValue([]),
      discoverWorkers: vi.fn().mockRejectedValue(new Error("scan failed")),
      logError,
    });

    expect(message.workers).toEqual([]);
    expect(message.snapshot.degraded_sections).toEqual([
      "recent results",
      "worker discovery",
    ]);
    expect(logError).toHaveBeenCalledWith(
      "Error discovering workers:",
      expect.any(Error),
    );
  });

  it("keeps partially discovered workers while marking worker discovery degraded", async () => {
    const okSnapshot = snapshot();
    const logError = vi.fn();

    const message = await buildDashboardMessage({
      fetchSnapshot: async () => okSnapshot,
      detectStuck: vi.fn().mockResolvedValue([]),
      discoverWorkers: vi.fn().mockRejectedValue(
        new WorkerDiscoveryPartialError("one output stream failed", ["worker-1"]),
      ),
      logError,
    });

    expect(message.workers).toEqual(["worker-1"]);
    expect(message.snapshot.degraded_sections).toEqual(["worker discovery"]);
    expect(logError).toHaveBeenCalledWith(
      "Error discovering workers:",
      expect.any(WorkerDiscoveryPartialError),
    );
  });

  it("marks concurrent enrichment failures in deterministic banner order", async () => {
    const okSnapshot = snapshot({ degraded_sections: ["recent results"] });
    const stuckLoad = deferred<StuckTask[]>();
    const workerLoad = deferred<string[]>();
    const logError = vi.fn();
    const detectStuck = vi.fn().mockReturnValue(stuckLoad.promise);
    const discoverWorkers = vi.fn().mockReturnValue(workerLoad.promise);

    const messagePromise = buildDashboardMessage({
      fetchSnapshot: async () => okSnapshot,
      detectStuck,
      discoverWorkers,
      logError,
    });

    await vi.waitFor(() => {
      expect(detectStuck).toHaveBeenCalledTimes(1);
      expect(discoverWorkers).toHaveBeenCalledTimes(1);
    });

    workerLoad.reject(new Error("scan failed"));
    await Promise.resolve();
    stuckLoad.reject(new Error("xpending failed"));

    const message = await messagePromise;

    expect(message.snapshot.degraded_sections).toEqual([
      "recent results",
      "stuck tasks",
      "worker discovery",
    ]);
    expect(logError).toHaveBeenCalledWith(
      "Error discovering workers:",
      expect.any(Error),
    );
    expect(logError).toHaveBeenCalledWith(
      "Error detecting stuck tasks:",
      expect.any(Error),
    );
  });
});
