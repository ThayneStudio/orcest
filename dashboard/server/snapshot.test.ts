import { beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("./redis.js", () => ({
  redis: {},
  healthCheck: vi.fn(),
  dashboardRedisKeyPatterns: vi.fn((suffixes: string[]) =>
    suffixes.flatMap((suffix) => [`*:${suffix}`, suffix])
  ),
  scanKeys: vi.fn(),
  scanKeysMany: vi.fn(),
}));

import {
  addProviderMetricValue,
  attemptCountValueFromFields,
  attemptCountLabelFromKey,
  consumerGroupInfoFromRedisGroup,
  deadLetterEntryFromFields,
  fetchSnapshot,
  lockInfoFromRedisValues,
  parseAttemptKey,
  normalizeWorkerPoolSnapshot,
  oldestLastDeliveredIdForGroups,
  parsePendingTaskMetadata,
  providerMetricFromKey,
  queuedTaskRangeStartForGroups,
  queueDepthFromKnownGroupState,
  recentResultFromFields,
} from "./snapshot.js";
import { dashboardRedisKeyPatterns, healthCheck, redis, scanKeys, scanKeysMany } from "./redis.js";
import { clearWorkerCachesForTesting } from "./workers.js";

function pipelineWith(results: unknown[][]) {
  return {
    get: vi.fn().mockReturnThis(),
    hgetall: vi.fn().mockReturnThis(),
    smembers: vi.fn().mockReturnThis(),
    ttl: vi.fn().mockReturnThis(),
    xlen: vi.fn().mockReturnThis(),
    exec: vi.fn().mockResolvedValue(results),
  };
}

function failingPipeline(error: Error) {
  return {
    get: vi.fn().mockReturnThis(),
    hgetall: vi.fn().mockReturnThis(),
    smembers: vi.fn().mockReturnThis(),
    ttl: vi.fn().mockReturnThis(),
    xlen: vi.fn().mockReturnThis(),
    exec: vi.fn().mockRejectedValue(error),
  };
}

function setPipelineResults(results: unknown[][][]): void {
  let index = 0;
  (redis as unknown as { pipeline: ReturnType<typeof vi.fn> }).pipeline =
    vi.fn(() => pipelineWith(results[index++] || []));
}

describe("fetchSnapshot queue accounting", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    clearWorkerCachesForTesting();
    vi.mocked(dashboardRedisKeyPatterns).mockImplementation((suffixes: string[]) =>
      suffixes.flatMap((suffix) => [`*:${suffix}`, suffix])
    );
    vi.mocked(healthCheck).mockResolvedValue(true);
    vi.mocked(scanKeys).mockResolvedValue([]);
    vi.mocked(scanKeysMany)
      .mockResolvedValueOnce(["tasks:claude"])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([]);
    (redis as unknown as { xinfo: ReturnType<typeof vi.fn> }).xinfo =
      vi.fn().mockResolvedValue([]);
    (redis as unknown as { xrange: ReturnType<typeof vi.fn> }).xrange =
      vi.fn().mockResolvedValue([]);
    (redis as unknown as { xrevrange: ReturnType<typeof vi.fn> }).xrevrange =
      vi.fn().mockResolvedValue([]);
    (redis as unknown as { type: ReturnType<typeof vi.fn> }).type =
      vi.fn().mockResolvedValue("stream");
  });

  it("scopes snapshot scans to configured dashboard Redis prefixes", async () => {
    vi.mocked(dashboardRedisKeyPatterns).mockImplementation((suffixes: string[]) =>
      suffixes.map((suffix) => `orcest:${suffix}`)
    );
    vi.mocked(scanKeysMany).mockReset();
    vi.mocked(scanKeysMany).mockResolvedValue([]);
    vi.mocked(scanKeys).mockResolvedValue([]);

    const snapshot = await fetchSnapshot();

    expect(snapshot.redis_ok).toBe(true);
    expect(dashboardRedisKeyPatterns).toHaveBeenCalledWith(["tasks:*"]);
    expect(dashboardRedisKeyPatterns).toHaveBeenCalledWith(["results"]);
    expect(dashboardRedisKeyPatterns).toHaveBeenCalledWith(["dead-letter"]);
    expect(dashboardRedisKeyPatterns).toHaveBeenCalledWith(["lock:pr:*", "lock:issue:*"]);
    expect(dashboardRedisKeyPatterns).toHaveBeenCalledWith([
      "pr:*:attempts",
      "issue:*:attempts",
    ]);
    expect(dashboardRedisKeyPatterns).toHaveBeenCalledWith(["providers:*"]);
    expect(scanKeysMany).toHaveBeenNthCalledWith(1, ["orcest:tasks:*"]);
    expect(scanKeysMany).toHaveBeenNthCalledWith(2, ["orcest:results"]);
    expect(scanKeysMany).toHaveBeenNthCalledWith(3, ["orcest:dead-letter"]);
    expect(scanKeysMany).toHaveBeenNthCalledWith(4, [
      "orcest:lock:pr:*",
      "orcest:lock:issue:*",
    ]);
    expect(scanKeysMany).toHaveBeenNthCalledWith(5, [
      "orcest:pr:*:attempts",
      "orcest:issue:*:attempts",
    ]);
    expect(scanKeysMany).toHaveBeenNthCalledWith(6, ["orcest:providers:*"]);
    expect(scanKeys).toHaveBeenCalledWith("orcest:pool:current_template_vmid");
    expect(scanKeys).toHaveBeenCalledWith("orcest:pool:idle");
    expect(scanKeys).toHaveBeenCalledWith("orcest:pool:active");
  });

  it("ignores non-stream keys discovered by broad task scans", async () => {
    vi.mocked(scanKeysMany).mockReset();
    vi.mocked(scanKeys).mockReset();
    vi.mocked(scanKeysMany)
      .mockResolvedValueOnce(["tasks:claude", "tasks:metadata"])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([]);
    vi.mocked(scanKeys).mockResolvedValue([]);
    setPipelineResults([[[null, 2]]]);
    const type = vi.fn(async (key: string) => key === "tasks:metadata" ? "hash" : "stream");
    (redis as unknown as { type: ReturnType<typeof vi.fn> }).type = type;
    const xinfo = vi.fn().mockResolvedValue([{
      name: "workers",
      consumers: "1",
      pending: "0",
      lag: "2",
      "last-delivered-id": "10-0",
    }]);
    const xrange = vi.fn().mockResolvedValue([]);
    (redis as unknown as { xinfo: ReturnType<typeof vi.fn> }).xinfo = xinfo;
    (redis as unknown as { xrange: ReturnType<typeof vi.fn> }).xrange = xrange;

    const snapshot = await fetchSnapshot();

    expect(type).toHaveBeenCalledWith("tasks:claude");
    expect(type).toHaveBeenCalledWith("tasks:metadata");
    expect(snapshot.queue_depths).toEqual({ "tasks:claude": 2 });
    expect(snapshot.consumer_groups).toEqual([{
      stream: "tasks:claude",
      name: "workers",
      consumers: 1,
      pending: 0,
      lag: 2,
    }]);
    expect(xinfo).toHaveBeenCalledTimes(1);
    expect(xinfo).toHaveBeenCalledWith("GROUPS", "tasks:claude");
    expect(xrange).toHaveBeenCalledWith("tasks:claude", "(10-0", "+", "COUNT", 50);
    expect(snapshot.degraded_sections).not.toContain("queue depths");
    expect(snapshot.degraded_sections).not.toContain("consumer groups");
    expect(snapshot.degraded_sections).not.toContain("queued tasks");
  });

  it("keeps queue sections degraded when task stream type checks fail", async () => {
    vi.mocked(scanKeysMany).mockReset();
    vi.mocked(scanKeys).mockReset();
    vi.mocked(scanKeysMany)
      .mockResolvedValueOnce(["tasks:claude"])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([]);
    vi.mocked(scanKeys).mockResolvedValue([]);
    setPipelineResults([[[null, 1]]]);
    (redis as unknown as { type: ReturnType<typeof vi.fn> }).type =
      vi.fn().mockRejectedValue(new Error("TYPE failed"));

    const snapshot = await fetchSnapshot();

    expect(snapshot.queue_depths).toEqual({ "tasks:claude": 1 });
    expect(snapshot.degraded_sections).toEqual(expect.arrayContaining([
      "queue depths",
      "consumer groups",
      "queued tasks",
    ]));
  });

  it("aggregates result and dead-letter depths across prefixed streams", async () => {
    vi.mocked(scanKeysMany).mockReset();
    vi.mocked(scanKeys).mockReset();
    vi.mocked(scanKeysMany)
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce(["project-a:results", "project-b:results"])
      .mockResolvedValueOnce(["project-a:dead-letter", "project-b:dead-letter"])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([]);
    vi.mocked(scanKeys).mockResolvedValue([]);
    setPipelineResults([
      [[null, 2], [null, 5]],
      [[null, 1], [null, 3]],
    ]);

    const snapshot = await fetchSnapshot();

    expect(snapshot.redis_ok).toBe(true);
    expect(snapshot.results_depth).toBe(7);
    expect(snapshot.dead_letter_count).toBe(4);
    expect(snapshot.degraded_sections).not.toContain("results depth");
    expect(snapshot.degraded_sections).not.toContain("dead-letter depth");
  });

  it("ignores non-stream keys discovered by result and dead-letter scans", async () => {
    vi.mocked(scanKeysMany).mockReset();
    vi.mocked(scanKeys).mockReset();
    vi.mocked(scanKeysMany)
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce(["project-a:results", "project-a:results:metadata"])
      .mockResolvedValueOnce(["project-a:dead-letter", "project-a:dead-letter:metadata"])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([]);
    vi.mocked(scanKeys).mockResolvedValue([]);
    setPipelineResults([
      [[null, 2]],
      [[null, 1]],
    ]);
    const type = vi.fn(async (key: string) =>
      key.endsWith(":metadata") ? "hash" : "stream"
    );
    (redis as unknown as { type: ReturnType<typeof vi.fn> }).type = type;
    const xrevrange = vi.fn().mockResolvedValue([]);
    (redis as unknown as { xrevrange: ReturnType<typeof vi.fn> }).xrevrange = xrevrange;

    const snapshot = await fetchSnapshot();

    expect(snapshot.redis_ok).toBe(true);
    expect(snapshot.results_depth).toBe(2);
    expect(snapshot.dead_letter_count).toBe(1);
    expect(type).toHaveBeenCalledWith("project-a:results:metadata");
    expect(type).toHaveBeenCalledWith("project-a:dead-letter:metadata");
    expect(xrevrange).toHaveBeenCalledTimes(2);
    expect(xrevrange).toHaveBeenCalledWith("project-a:results", "+", "-", "COUNT", 20);
    expect(xrevrange).toHaveBeenCalledWith("project-a:dead-letter", "+", "-", "COUNT", 5);
    expect(xrevrange).not.toHaveBeenCalledWith(
      "project-a:results:metadata",
      expect.anything(),
      expect.anything(),
      expect.anything(),
      expect.anything(),
    );
    expect(xrevrange).not.toHaveBeenCalledWith(
      "project-a:dead-letter:metadata",
      expect.anything(),
      expect.anything(),
      expect.anything(),
      expect.anything(),
    );
    expect(snapshot.degraded_sections).not.toContain("results depth");
    expect(snapshot.degraded_sections).not.toContain("recent results");
    expect(snapshot.degraded_sections).not.toContain("dead-letter depth");
    expect(snapshot.degraded_sections).not.toContain("dead-letter entries");
  });

  it("marks terminal stream totals degraded while preserving successful partial depths", async () => {
    vi.mocked(scanKeysMany).mockReset();
    vi.mocked(scanKeys).mockReset();
    vi.mocked(scanKeysMany)
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce(["project-a:results", "project-b:results"])
      .mockResolvedValueOnce(["project-a:dead-letter", "project-b:dead-letter"])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([]);
    vi.mocked(scanKeys).mockResolvedValue([]);
    setPipelineResults([
      [[null, 2], [new Error("results xlen failed"), null]],
      [[new Error("dead-letter xlen failed"), null], [null, 3]],
    ]);

    const snapshot = await fetchSnapshot();

    expect(snapshot.redis_ok).toBe(true);
    expect(snapshot.results_depth).toBe(2);
    expect(snapshot.dead_letter_count).toBe(3);
    expect(snapshot.degraded_sections).toEqual(expect.arrayContaining([
      "results depth",
      "dead-letter depth",
    ]));
    expect(snapshot.degraded_sections).not.toContain("recent results");
    expect(snapshot.degraded_sections).not.toContain("dead-letter entries");
  });

  it("preserves recent results from healthy streams when one result stream read fails", async () => {
    vi.mocked(scanKeysMany).mockReset();
    vi.mocked(scanKeys).mockReset();
    vi.mocked(scanKeysMany)
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce(["project-a:results", "project-b:results"])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([]);
    vi.mocked(scanKeys).mockResolvedValue([]);
    setPipelineResults([
      [[null, 1], [null, 1]],
    ]);
    (redis as unknown as { xrevrange: ReturnType<typeof vi.fn> }).xrevrange = vi.fn()
      .mockRejectedValueOnce(new Error("project-a xrevrange failed"))
      .mockResolvedValueOnce([
        ["200-0", [
          "task_id", "task-b",
          "worker_id", "worker-b",
          "status", "COMPLETED",
          "repo", "owner/repo",
          "resource_type", "pr",
          "resource_id", "42",
        ]],
      ]);

    const snapshot = await fetchSnapshot();

    expect(snapshot.redis_ok).toBe(true);
    expect(snapshot.results_depth).toBe(2);
    expect(snapshot.recent_results.map((result) => result.task_id)).toEqual(["task-b"]);
    expect(snapshot.recent_results[0]).toMatchObject({
      result_stream: "project-b:results",
      entry_id: "200-0",
    });
    expect(snapshot.degraded_sections).toContain("recent results");
    expect(snapshot.degraded_sections).not.toContain("results depth");
  });

  it("resolves recent result output prefixes from the actual worker output stream", async () => {
    vi.mocked(scanKeysMany).mockReset();
    vi.mocked(scanKeys).mockReset();
    vi.mocked(scanKeysMany)
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce(["project-a:results"])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce(["orcest:output:worker-1"]);
    vi.mocked(scanKeys).mockResolvedValue([]);
    setPipelineResults([
      [[null, 1]],
    ]);
    (redis as unknown as { xrevrange: ReturnType<typeof vi.fn> }).xrevrange = vi.fn()
      .mockResolvedValueOnce([
        ["200-0", [
          "task_id", "task-project",
          "worker_id", "worker-1",
          "status", "COMPLETED",
          "repo", "owner/repo",
          "resource_type", "pr",
          "resource_id", "42",
        ]],
      ])
      .mockResolvedValueOnce([
        ["100-0", [
          "type", "task_start",
          "task_id", "task-project",
        ]],
      ]);

    const snapshot = await fetchSnapshot();

    expect(snapshot.recent_results).toHaveLength(1);
    expect(snapshot.recent_results[0]).toMatchObject({
      result_stream: "project-a:results",
      worker_id: "worker-1",
      task_id: "task-project",
      output_prefix: "orcest",
    });
  });

  it("resolves active lock output prefixes from the actual worker output stream", async () => {
    vi.mocked(scanKeysMany).mockReset();
    vi.mocked(scanKeys).mockReset();
    vi.mocked(scanKeysMany)
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce(["project-a:lock:pr:owner/repo:42"])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce(["orcest:output:worker-1"]);
    vi.mocked(scanKeys).mockResolvedValue([]);
    setPipelineResults([
      [[null, "worker-1"], [null, 180], [null, JSON.stringify({
        task_id: "task-live",
        created_at: "2026-06-30T00:00:00Z",
      })]],
    ]);
    (redis as unknown as { xrevrange: ReturnType<typeof vi.fn> }).xrevrange = vi.fn()
      .mockResolvedValueOnce([
        ["100-0", [
          "type", "task_start",
          "task_id", "task-live",
        ]],
      ]);

    const snapshot = await fetchSnapshot();

    expect(snapshot.locks).toHaveLength(1);
    expect(snapshot.locks[0]).toMatchObject({
      prefix: "project-a",
      owner: "worker-1",
      task_id: "task-live",
      output_prefix: "orcest",
    });
  });

  it("batches task output prefix discovery across recent results and active locks", async () => {
    vi.mocked(scanKeysMany).mockReset();
    vi.mocked(scanKeys).mockReset();
    vi.mocked(scanKeysMany)
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce(["project-a:results"])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce(["project-a:lock:pr:owner/repo:42"])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce(["orcest:output:worker-1"]);
    vi.mocked(scanKeys).mockResolvedValue([]);
    setPipelineResults([
      [[null, 1]],
      [[null, "worker-1"], [null, 180], [null, JSON.stringify({
        task_id: "task-live",
        created_at: "2026-06-30T00:00:00Z",
      })]],
    ]);
    (redis as unknown as { xrevrange: ReturnType<typeof vi.fn> }).xrevrange = vi.fn()
      .mockResolvedValueOnce([
        ["200-0", [
          "task_id", "task-project",
          "worker_id", "worker-1",
          "status", "COMPLETED",
          "repo", "owner/repo",
          "resource_type", "pr",
          "resource_id", "43",
        ]],
      ])
      .mockResolvedValueOnce([
        ["120-0", ["type", "task_start", "task_id", "task-live"]],
        ["100-0", ["type", "task_start", "task_id", "task-project"]],
      ]);

    const snapshot = await fetchSnapshot();

    expect(snapshot.recent_results[0].output_prefix).toBe("orcest");
    expect(snapshot.locks[0].output_prefix).toBe("orcest");
    expect(vi.mocked(scanKeysMany).mock.calls.filter(([patterns]) =>
      Array.isArray(patterns) && patterns.includes("*:output:*")
    )).toHaveLength(1);
    expect(redis.xrevrange).toHaveBeenCalledTimes(2);
    expect(redis.xrevrange).toHaveBeenLastCalledWith(
      "orcest:output:worker-1",
      "+",
      "-",
      "COUNT",
      500,
    );
  });

  it("marks task output prefixes degraded when output stream discovery fails", async () => {
    vi.mocked(scanKeysMany).mockReset();
    vi.mocked(scanKeys).mockReset();
    vi.mocked(scanKeysMany)
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce(["project-a:results"])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockRejectedValueOnce(new Error("output scan failed"));
    vi.mocked(scanKeys).mockResolvedValue([]);
    setPipelineResults([
      [[null, 1]],
    ]);
    (redis as unknown as { xrevrange: ReturnType<typeof vi.fn> }).xrevrange = vi.fn()
      .mockResolvedValueOnce([
        ["200-0", [
          "task_id", "task-project",
          "worker_id", "worker-1",
          "status", "COMPLETED",
          "repo", "owner/repo",
          "resource_type", "pr",
          "resource_id", "42",
        ]],
      ]);

    const snapshot = await fetchSnapshot();

    expect(snapshot.recent_results).toHaveLength(1);
    expect(Object.prototype.hasOwnProperty.call(
      snapshot.recent_results[0],
      "output_prefix",
    )).toBe(false);
    expect(snapshot.recent_results[0].output_prefix_unresolved).toBe(true);
    expect(snapshot.degraded_sections).toContain("task output prefixes");
  });

  it("marks task output prefixes degraded when output stream reads fail", async () => {
    vi.mocked(scanKeysMany).mockReset();
    vi.mocked(scanKeys).mockReset();
    vi.mocked(scanKeysMany)
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce(["project-a:results"])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce(["orcest:output:worker-1"]);
    vi.mocked(scanKeys).mockResolvedValue([]);
    setPipelineResults([
      [[null, 1]],
    ]);
    (redis as unknown as { xrevrange: ReturnType<typeof vi.fn> }).xrevrange = vi.fn()
      .mockResolvedValueOnce([
        ["200-0", [
          "task_id", "task-project",
          "worker_id", "worker-1",
          "status", "COMPLETED",
          "repo", "owner/repo",
          "resource_type", "pr",
          "resource_id", "42",
        ]],
      ])
      .mockRejectedValueOnce(new Error("output xrevrange failed"));

    const snapshot = await fetchSnapshot();

    expect(snapshot.recent_results).toHaveLength(1);
    expect(Object.prototype.hasOwnProperty.call(
      snapshot.recent_results[0],
      "output_prefix",
    )).toBe(false);
    expect(snapshot.recent_results[0].output_prefix_unresolved).toBe(true);
    expect(snapshot.degraded_sections).toContain("task output prefixes");
  });

  it("marks unresolved active lock output prefixes when lookup partially fails", async () => {
    vi.mocked(scanKeysMany).mockReset();
    vi.mocked(scanKeys).mockReset();
    vi.mocked(scanKeysMany)
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce(["project-a:lock:pr:owner/repo:42"])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce(["orcest:output:worker-1"]);
    vi.mocked(scanKeys).mockResolvedValue([]);
    setPipelineResults([
      [[null, "worker-1"], [null, 180], [null, JSON.stringify({
        task_id: "task-live",
        created_at: "2026-06-30T00:00:00Z",
      })]],
    ]);
    (redis as unknown as { xrevrange: ReturnType<typeof vi.fn> }).xrevrange = vi.fn()
      .mockRejectedValueOnce(new Error("output xrevrange failed"));

    const snapshot = await fetchSnapshot();

    expect(snapshot.locks).toHaveLength(1);
    expect(Object.prototype.hasOwnProperty.call(
      snapshot.locks[0],
      "output_prefix",
    )).toBe(false);
    expect(snapshot.locks[0].output_prefix_unresolved).toBe(true);
    expect(snapshot.degraded_sections).toContain("task output prefixes");
  });

  it("preserves dead-letter entries from healthy streams when one dead-letter read fails", async () => {
    vi.mocked(scanKeysMany).mockReset();
    vi.mocked(scanKeys).mockReset();
    vi.mocked(scanKeysMany)
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce(["project-a:dead-letter", "project-b:dead-letter"])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([]);
    vi.mocked(scanKeys).mockResolvedValue([]);
    setPipelineResults([
      [[null, 1], [null, 1]],
    ]);
    (redis as unknown as { xrevrange: ReturnType<typeof vi.fn> }).xrevrange = vi.fn()
      .mockRejectedValueOnce(new Error("project-a dead-letter failed"))
      .mockResolvedValueOnce([
        ["300-0", [
          "id", "task-b",
          "type", "fix_pr",
          "repo", "owner/repo",
          "resource_type", "pr",
          "resource_id", "42",
        ]],
      ]);

    const snapshot = await fetchSnapshot();

    expect(snapshot.redis_ok).toBe(true);
    expect(snapshot.dead_letter_count).toBe(2);
    expect(snapshot.dead_letter_entries.map((entry) => entry.task_id)).toEqual(["task-b"]);
    expect(snapshot.dead_letter_entries[0]).toMatchObject({
      dead_letter_stream: "project-b:dead-letter",
      entry_id: "300-0",
    });
    expect(snapshot.degraded_sections).toContain("dead-letter entries");
    expect(snapshot.degraded_sections).not.toContain("dead-letter depth");
  });

  it("orders merged dead-letter entries by full Redis stream ID before trimming", async () => {
    vi.mocked(scanKeysMany).mockReset();
    vi.mocked(scanKeys).mockReset();
    vi.mocked(scanKeysMany)
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce(["project-a:dead-letter", "project-b:dead-letter"])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([]);
    vi.mocked(scanKeys).mockResolvedValue([]);
    setPipelineResults([
      [[null, 6], [null, 1]],
    ]);
    (redis as unknown as { xrevrange: ReturnType<typeof vi.fn> }).xrevrange = vi.fn()
      .mockResolvedValueOnce([
        ["1000-1", ["id", "task-a-1"]],
        ["999-0", ["id", "task-a-old"]],
        ["1000-8", ["id", "task-a-8"]],
        ["1000-3", ["id", "task-a-3"]],
        ["1000-4", ["id", "task-a-4"]],
      ])
      .mockResolvedValueOnce([
        ["1000-9", ["id", "task-b-9"]],
        ["1000-2", ["id", "task-b-2"]],
      ]);

    const snapshot = await fetchSnapshot();

    expect(snapshot.dead_letter_entries.map((entry) => entry.task_id)).toEqual([
      "task-b-9",
      "task-a-8",
      "task-a-4",
      "task-a-3",
      "task-b-2",
    ]);
  });

  it("uses only the workers consumer group for queue depth and preview position", async () => {
    setPipelineResults([[[null, 50]]]);
    (redis as unknown as { xinfo: ReturnType<typeof vi.fn> }).xinfo =
      vi.fn().mockResolvedValue([
        {
          name: "workers",
          consumers: "2",
          pending: "2",
          lag: "3",
          "last-delivered-id": "10-0",
        },
        {
          name: "debug-inspector",
          consumers: "1",
          pending: "20",
          lag: "30",
          "last-delivered-id": "0-0",
        },
      ]);
    const xrange = vi.fn().mockResolvedValue([
      ["11-0", [
        "id", "task-11",
        "type", "fix_pr",
        "key_prefix", "project-a",
        "repo", "owner/repo",
        "resource_type", "pr",
        "resource_id", "11",
        "created_at", "2026-06-16T00:00:00Z",
      ]],
    ]);
    (redis as unknown as { xrange: ReturnType<typeof vi.fn> }).xrange = xrange;

    const snapshot = await fetchSnapshot();

    expect(snapshot.redis_ok).toBe(true);
    expect(snapshot.queue_depths["tasks:claude"]).toBe(5);
    expect(snapshot.consumer_groups.map((group) => group.name)).toEqual([
      "workers",
      "debug-inspector",
    ]);
    expect(xrange).toHaveBeenCalledWith("tasks:claude", "(10-0", "+", "COUNT", 50);
    expect(snapshot.queued_tasks).toEqual([{
      entry_id: "11-0",
      task_id: "task-11",
      task_type: "fix_pr",
      prefix: "project-a",
      repo: "owner/repo",
      resource_type: "pr",
      resource_id: "11",
      created_at: "2026-06-16T00:00:00Z",
      stream: "tasks:claude",
    }]);
  });

  it("shows retained task entries when the workers consumer group is missing", async () => {
    setPipelineResults([[[null, 2]]]);
    (redis as unknown as { xinfo: ReturnType<typeof vi.fn> }).xinfo =
      vi.fn().mockResolvedValue([
        {
          name: "debug-inspector",
          consumers: "1",
          pending: "0",
          lag: "0",
          "last-delivered-id": "2-0",
        },
      ]);
    const xrange = vi.fn().mockResolvedValue([
      ["1-0", [
        "id", "task-1",
        "type", "implement_issue",
        "repo", "owner/repo",
        "resource_type", "issue",
        "resource_id", "1",
      ]],
      ["2-0", [
        "id", "task-2",
        "type", "fix_ci",
        "repo", "owner/repo",
        "resource_type", "pr",
        "resource_id", "2",
      ]],
    ]);
    (redis as unknown as { xrange: ReturnType<typeof vi.fn> }).xrange = xrange;

    const snapshot = await fetchSnapshot();

    expect(snapshot.redis_ok).toBe(true);
    expect(snapshot.queue_depths["tasks:claude"]).toBe(2);
    expect(snapshot.consumer_groups).toEqual([
      {
        stream: "tasks:claude",
        name: "debug-inspector",
        consumers: 1,
        pending: 0,
        lag: 0,
      },
      {
        stream: "tasks:claude",
        name: "workers",
        consumers: 0,
        pending: 0,
        lag: 2,
      },
    ]);
    expect(xrange).toHaveBeenCalledWith("tasks:claude", "-", "+", "COUNT", 50);
    expect(snapshot.queued_tasks.map((task) => task.task_id)).toEqual(["task-1", "task-2"]);
  });

  it("reports degraded sections when best-effort Redis reads fail", async () => {
    vi.mocked(scanKeysMany).mockReset();
    vi.mocked(scanKeys).mockReset();
    vi.mocked(scanKeysMany)
      .mockResolvedValueOnce(["tasks:claude"])
      .mockResolvedValueOnce(["results"])
      .mockResolvedValueOnce(["dead-letter"])
      .mockResolvedValueOnce(["lock:pr:owner/repo:42"])
      .mockResolvedValueOnce(["pr:owner/repo:42:attempts"])
      .mockResolvedValueOnce(["providers:claude:exhausted_skip"]);
    vi.mocked(scanKeys)
      .mockResolvedValueOnce(["pool:current_template_vmid"])
      .mockResolvedValue([]);
    setPipelineResults([
      [[new Error("xlen tasks failed"), null]],
      [[new Error("xlen results failed"), null]],
      [[new Error("xlen dead-letter failed"), null]],
      [[new Error("lock owner failed"), null], [null, 180], [null, null]],
      [[new Error("attempts failed"), null]],
      [[new Error("provider failed"), null]],
      [[new Error("pool template failed"), null], [null, []], [null, {}]],
    ]);
    (redis as unknown as { xinfo: ReturnType<typeof vi.fn> }).xinfo =
      vi.fn().mockRejectedValue(new Error("xinfo failed"));
    (redis as unknown as { xrevrange: ReturnType<typeof vi.fn> }).xrevrange =
      vi.fn().mockRejectedValue(new Error("xrevrange failed"));
    (redis as unknown as { xrange: ReturnType<typeof vi.fn> }).xrange =
      vi.fn().mockRejectedValue(new Error("xrange failed"));

    const snapshot = await fetchSnapshot();

    expect(snapshot.redis_ok).toBe(true);
    expect(snapshot.degraded_sections).toEqual(expect.arrayContaining([
      "queue depths",
      "results depth",
      "dead-letter depth",
      "dead-letter entries",
      "active locks",
      "consumer groups",
      "recent results",
      "attempt counts",
      "queued tasks",
      "provider health",
      "worker pool",
    ]));
  });

  it("sorts malformed recent-result stream IDs after valid results", async () => {
    vi.mocked(scanKeysMany).mockReset();
    vi.mocked(scanKeys).mockReset();
    vi.mocked(scanKeysMany)
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce(["results"])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([]);
    vi.mocked(scanKeys).mockResolvedValue([]);
    setPipelineResults([
      [[null, 3]],
    ]);
    (redis as unknown as { xrevrange: ReturnType<typeof vi.fn> }).xrevrange =
      vi.fn().mockResolvedValueOnce([
        ["9999999999999abc-0", ["task_id", "task-bad", "worker_id", "worker-bad"]],
        ["100-0", ["task_id", "task-old", "worker_id", "worker-old"]],
        ["200-0", ["task_id", "task-new", "worker_id", "worker-new"]],
      ]);

    const snapshot = await fetchSnapshot(2);

    expect(snapshot.recent_results.map((result) => result.task_id)).toEqual([
      "task-new",
      "task-old",
    ]);
  });

  it("orders same-ID recent results by stream before trimming", async () => {
    vi.mocked(scanKeysMany).mockReset();
    vi.mocked(scanKeys).mockReset();
    vi.mocked(scanKeysMany)
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce(["project-b:results", "project-a:results"])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([]);
    vi.mocked(scanKeys).mockResolvedValue([]);
    setPipelineResults([
      [[null, 2], [null, 2]],
    ]);
    (redis as unknown as { xrevrange: ReturnType<typeof vi.fn> }).xrevrange = vi.fn()
      .mockResolvedValueOnce([
        ["1000-0", ["task_id", "task-b", "worker_id", "worker-b"]],
        ["999-0", ["task_id", "task-b-old", "worker_id", "worker-b"]],
      ])
      .mockResolvedValueOnce([
        ["1000-0", ["task_id", "task-a", "worker_id", "worker-a"]],
        ["999-0", ["task_id", "task-a-old", "worker_id", "worker-a"]],
      ]);

    const snapshot = await fetchSnapshot(1);

    expect(snapshot.recent_results.map((result) => result.task_id)).toEqual(["task-a"]);
  });

  it("keeps Redis marked healthy when a best-effort pipeline throws", async () => {
    (redis as unknown as { pipeline: ReturnType<typeof vi.fn> }).pipeline =
      vi.fn(() => failingPipeline(new Error("pipeline failed")));

    const snapshot = await fetchSnapshot();

    expect(snapshot.redis_ok).toBe(true);
    expect(snapshot.degraded_sections).toContain("queue depths");
  });

  it("keeps Redis marked healthy when best-effort key discovery fails", async () => {
    vi.mocked(scanKeysMany).mockReset();
    vi.mocked(scanKeys).mockReset();
    vi.mocked(scanKeysMany)
      .mockRejectedValueOnce(new Error("task scan failed"))
      .mockResolvedValueOnce(["results"])
      .mockResolvedValueOnce(["dead-letter"])
      .mockResolvedValueOnce(["lock:pr:owner/repo:42"])
      .mockResolvedValueOnce(["pr:owner/repo:42:attempts"])
      .mockResolvedValueOnce(["providers:claude:exhausted_skip"]);
    vi.mocked(scanKeys).mockResolvedValue([]);
    setPipelineResults([
      [[null, 1]],
      [[null, 1]],
      [[null, "worker-1"], [null, 180], [null, null]],
      [[null, { count: "2" }]],
      [[null, "1"]],
    ]);

    const snapshot = await fetchSnapshot();

    expect(snapshot.redis_ok).toBe(true);
    expect(snapshot.degraded_sections).toEqual(expect.arrayContaining([
      "queue depths",
      "consumer groups",
      "queued tasks",
    ]));
  });

  it("marks worker pool degraded when pool Redis pipeline results are incomplete", async () => {
    vi.mocked(scanKeysMany).mockReset();
    vi.mocked(scanKeys).mockReset();
    vi.mocked(scanKeysMany).mockResolvedValue([]);
    vi.mocked(scanKeys)
      .mockResolvedValueOnce(["pool:current_template_vmid"])
      .mockResolvedValue([]);
    setPipelineResults([
      [[null, "9001"]],
    ]);

    const snapshot = await fetchSnapshot();

    expect(snapshot.redis_ok).toBe(true);
    expect(snapshot.degraded_sections).toContain("worker pool");
    expect(snapshot.worker_pool).toEqual([{
      prefix: "",
      template_vmid: "9001",
      idle: [],
      active: [],
      idle_count: 0,
      active_count: 0,
    }]);
  });

  it("does not synthesize active locks from incomplete pipeline replies", async () => {
    vi.mocked(scanKeysMany).mockReset();
    vi.mocked(scanKeys).mockReset();
    vi.mocked(scanKeysMany)
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce(["lock:pr:owner/repo:42"])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([]);
    vi.mocked(scanKeys).mockResolvedValue([]);
    setPipelineResults([
      [[null, "worker-1"]],
    ]);

    const snapshot = await fetchSnapshot();

    expect(snapshot.redis_ok).toBe(true);
    expect(snapshot.locks).toEqual([]);
    expect(snapshot.degraded_sections).toContain("active locks");
  });

  it("does not show retained tasks as queued when consumer group lookup fails", async () => {
    setPipelineResults([[[null, 50]]]);
    (redis as unknown as { xinfo: ReturnType<typeof vi.fn> }).xinfo =
      vi.fn().mockRejectedValue(new Error("xinfo failed"));
    const xrange = vi.fn().mockResolvedValue([
      ["1-0", ["id", "already-delivered"]],
    ]);
    (redis as unknown as { xrange: ReturnType<typeof vi.fn> }).xrange = xrange;

    const snapshot = await fetchSnapshot();

    expect(snapshot.redis_ok).toBe(true);
    expect(snapshot.queue_depths).not.toHaveProperty("tasks:claude");
    expect(snapshot.queued_tasks).toEqual([]);
    expect(xrange).not.toHaveBeenCalledWith("tasks:claude", "-", "+", "COUNT", 50);
    expect(snapshot.degraded_sections).toEqual(expect.arrayContaining([
      "consumer groups",
      "queue depths",
      "queued tasks",
    ]));
  });

  it("isolates consumer-group failures to the affected task stream", async () => {
    vi.mocked(scanKeysMany).mockReset();
    vi.mocked(scanKeys).mockReset();
    vi.mocked(scanKeysMany)
      .mockResolvedValueOnce(["project-a:tasks:claude", "project-b:tasks:claude"])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([]);
    vi.mocked(scanKeys).mockResolvedValue([]);
    setPipelineResults([
      [[null, 10], [null, 10]],
    ]);
    const xinfo = vi.fn(async (_command: string, stream: string) => {
      if (stream === "project-a:tasks:claude") {
        throw new Error("project-a xinfo failed");
      }
      return [{
        name: "workers",
        consumers: "1",
        pending: "1",
        lag: "2",
        "last-delivered-id": "5-0",
      }];
    });
    const xrange = vi.fn().mockResolvedValue([
      ["6-0", [
        "id", "task-b",
        "type", "fix_pr",
        "repo", "owner/repo",
        "resource_type", "pr",
        "resource_id", "42",
      ]],
    ]);
    (redis as unknown as { xinfo: ReturnType<typeof vi.fn> }).xinfo = xinfo;
    (redis as unknown as { xrange: ReturnType<typeof vi.fn> }).xrange = xrange;

    const snapshot = await fetchSnapshot();

    expect(snapshot.redis_ok).toBe(true);
    expect(snapshot.consumer_groups).toEqual([{
      stream: "project-b:tasks:claude",
      name: "workers",
      consumers: 1,
      pending: 1,
      lag: 2,
    }]);
    expect(snapshot.queue_depths).toEqual({
      "project-b:tasks:claude": 3,
    });
    expect(snapshot.queued_tasks).toEqual([{
      entry_id: "6-0",
      task_id: "task-b",
      task_type: "fix_pr",
      prefix: null,
      repo: "owner/repo",
      resource_type: "pr",
      resource_id: "42",
      created_at: null,
      stream: "project-b:tasks:claude",
    }]);
    expect(xrange).toHaveBeenCalledTimes(1);
    expect(xrange).toHaveBeenCalledWith(
      "project-b:tasks:claude",
      "(5-0",
      "+",
      "COUNT",
      50,
    );
    expect(snapshot.degraded_sections).toEqual(expect.arrayContaining([
      "consumer groups",
      "queue depths",
      "queued tasks",
    ]));
  });

  it("marks queue depths degraded when fallback depth counting fails", async () => {
    setPipelineResults([[[null, 50]]]);
    (redis as unknown as { xinfo: ReturnType<typeof vi.fn> }).xinfo =
      vi.fn().mockResolvedValue([
        {
          name: "workers",
          consumers: "1",
          pending: "2",
          lag: null,
          "last-delivered-id": "10-0",
        },
      ]);
    (redis as unknown as { xrange: ReturnType<typeof vi.fn> }).xrange =
      vi.fn().mockRejectedValue(new Error("xrange failed"));

    const snapshot = await fetchSnapshot();

    expect(snapshot.redis_ok).toBe(true);
    expect(snapshot.degraded_sections).toContain("queue depths");
    expect(snapshot.queue_depths).not.toHaveProperty("tasks:claude");
  });

  it("bounds lag-null fallback depth counting", async () => {
    setPipelineResults([[[null, 50]]]);
    (redis as unknown as { xinfo: ReturnType<typeof vi.fn> }).xinfo =
      vi.fn().mockResolvedValue([
        {
          name: "workers",
          consumers: "1",
          pending: "0",
          lag: null,
          "last-delivered-id": "0-0",
        },
      ]);
    const fullPage = (page: number) =>
      Array.from({ length: 1000 }, (_, index) => [
        `${page}-${index}`,
        ["id", `task-${page}-${index}`],
      ]);
    const xrange = vi.fn(async () => fullPage(xrange.mock.calls.length + 1));
    (redis as unknown as { xrange: ReturnType<typeof vi.fn> }).xrange = xrange;

    const snapshot = await fetchSnapshot();

    expect(snapshot.redis_ok).toBe(true);
    expect(snapshot.degraded_sections).toContain("queue depths");
    expect(snapshot.queue_depths).not.toHaveProperty("tasks:claude");
    const fallbackCalls = (xrange.mock.calls as unknown[][]).filter((call) => call[4] === 1000);
    expect(fallbackCalls).toHaveLength(10);
  });

  it("falls back to range counting when XLEN fails and the workers group is missing", async () => {
    setPipelineResults([[[new Error("xlen failed"), null]]]);
    (redis as unknown as { xinfo: ReturnType<typeof vi.fn> }).xinfo =
      vi.fn().mockResolvedValue([
        {
          name: "debug-inspector",
          consumers: "1",
          pending: "0",
          lag: "0",
          "last-delivered-id": "2-0",
        },
      ]);
    const xrange = vi.fn()
      .mockResolvedValueOnce([
        ["1-0", ["id", "task-1"]],
        ["2-0", ["id", "task-2"]],
      ])
      .mockResolvedValueOnce([]);
    (redis as unknown as { xrange: ReturnType<typeof vi.fn> }).xrange = xrange;

    const snapshot = await fetchSnapshot();

    expect(snapshot.redis_ok).toBe(true);
    expect(snapshot.queue_depths["tasks:claude"]).toBe(2);
    expect(snapshot.consumer_groups).toEqual([
      {
        stream: "tasks:claude",
        name: "debug-inspector",
        consumers: 1,
        pending: 0,
        lag: 0,
      },
      {
        stream: "tasks:claude",
        name: "workers",
        consumers: 0,
        pending: 0,
        lag: 2,
      },
    ]);
    expect(xrange).toHaveBeenNthCalledWith(1, "tasks:claude", "-", "+", "COUNT", 1000);
    expect(snapshot.degraded_sections).toContain("queue depths");
  });
});

describe("queueDepthFromKnownGroupState", () => {
  it("uses retained stream length when the worker group is missing", () => {
    expect(queueDepthFromKnownGroupState(42, [])).toBe(42);
  });

  it("returns unknown when the worker group and stream length are both unknown", () => {
    expect(queueDepthFromKnownGroupState(null, [])).toBeNull();
  });

  it("uses pending plus consumer-group lag instead of retained stream length", () => {
    expect(queueDepthFromKnownGroupState(42, [{ pending: 3, lag: 0 }])).toBe(3);
  });

  it("sums pending plus lag across provided worker-group positions", () => {
    expect(queueDepthFromKnownGroupState(42, [
      { pending: 1, lag: 2 },
      { pending: 3, lag: 7 },
    ])).toBe(13);
  });

  it("returns null when groups exist but Redis did not report lag", () => {
    expect(queueDepthFromKnownGroupState(42, [{ pending: 3, lag: null }])).toBeNull();
  });

  it("returns null when any group has unknown lag", () => {
    expect(queueDepthFromKnownGroupState(42, [
      { pending: 0, lag: 0 },
      { pending: 3, lag: null },
    ])).toBeNull();
  });

  it("returns null when any group has negative lag", () => {
    expect(queueDepthFromKnownGroupState(42, [{ pending: 3, lag: -1 }])).toBeNull();
  });
});

describe("consumerGroupInfoFromRedisGroup", () => {
  it("parses object-shaped XINFO GROUPS output", () => {
    expect(consumerGroupInfoFromRedisGroup("tasks:claude", {
      name: "orcest-workers",
      consumers: "2",
      pending: "3",
      lag: "4",
      "last-delivered-id": "10-0",
    })).toEqual({
      info: {
        stream: "tasks:claude",
        name: "orcest-workers",
        consumers: 2,
        pending: 3,
        lag: 4,
      },
      lastDeliveredId: "10-0",
    });
  });

  it("parses flat-array output and normalizes malformed counts", () => {
    expect(consumerGroupInfoFromRedisGroup("tasks:claude", [
      "name", "orcest-workers",
      "consumers", "bad",
      "pending", "-5",
      "lag", "not-a-number",
      "last-delivered-id", "11-2",
    ])).toEqual({
      info: {
        stream: "tasks:claude",
        name: "orcest-workers",
        consumers: 0,
        pending: 0,
        lag: null,
      },
      lastDeliveredId: "11-2",
    });
  });

  it("does not coerce boolean or array consumer-group counts", () => {
    expect(consumerGroupInfoFromRedisGroup("tasks:claude", {
      name: "workers",
      consumers: true,
      pending: [3],
      lag: [],
      "last-delivered-id": "12-0",
    })).toEqual({
      info: {
        stream: "tasks:claude",
        name: "workers",
        consumers: 0,
        pending: 0,
        lag: null,
      },
      lastDeliveredId: "12-0",
    });
  });

  it("treats negative Redis lag as unknown", () => {
    expect(consumerGroupInfoFromRedisGroup("tasks:claude", {
      name: "workers",
      consumers: "1",
      pending: "0",
      lag: "-1",
      "last-delivered-id": "12-0",
    })).toEqual({
      info: {
        stream: "tasks:claude",
        name: "workers",
        consumers: 1,
        pending: 0,
        lag: null,
      },
      lastDeliveredId: "12-0",
    });
  });

  it("treats blank Redis lag as unknown", () => {
    expect(consumerGroupInfoFromRedisGroup("tasks:claude", {
      name: "workers",
      consumers: "1",
      pending: "0",
      lag: "   ",
      "last-delivered-id": "12-0",
    })).toEqual({
      info: {
        stream: "tasks:claude",
        name: "workers",
        consumers: 1,
        pending: 0,
        lag: null,
      },
      lastDeliveredId: "12-0",
    });
  });
});

describe("oldestLastDeliveredIdForGroups", () => {
  it("uses 0-0 when a stream has no consumer groups", () => {
    expect(oldestLastDeliveredIdForGroups([])).toBe("0-0");
  });

  it("returns the only group position", () => {
    expect(
      oldestLastDeliveredIdForGroups([{ lastDeliveredId: "10-2" }]),
    ).toBe("10-2");
  });

  it("returns the slowest group position using numeric stream ID ordering", () => {
    expect(
      oldestLastDeliveredIdForGroups([
        { lastDeliveredId: "10-10" },
        { lastDeliveredId: "10-2" },
        { lastDeliveredId: "9-99" },
      ]),
    ).toBe("9-99");
  });

  it("compares stream ID sequence numbers numerically", () => {
    expect(
      oldestLastDeliveredIdForGroups([
        { lastDeliveredId: "10-10" },
        { lastDeliveredId: "10-2" },
      ]),
    ).toBe("10-2");
  });

  it("treats malformed group positions as undelivered", () => {
    expect(
      oldestLastDeliveredIdForGroups([
        { lastDeliveredId: "12abc-0" },
        { lastDeliveredId: "10-2" },
      ]),
    ).toBe("0-0");
  });
});

describe("queuedTaskRangeStartForGroups", () => {
  it("starts at the beginning when the worker group is missing", () => {
    expect(queuedTaskRangeStartForGroups([])).toBe("-");
  });

  it("starts at the beginning when at least one group has delivered nothing", () => {
    expect(queuedTaskRangeStartForGroups([{ lastDeliveredId: "0-0" }])).toBe("-");
  });

  it("starts after the slowest consumer group position", () => {
    expect(queuedTaskRangeStartForGroups([
      { lastDeliveredId: "10-2" },
      { lastDeliveredId: "9-99" },
    ])).toBe("(9-99");
  });

  it("starts at the beginning when a group position is malformed", () => {
    expect(queuedTaskRangeStartForGroups([
      { lastDeliveredId: "10-2" },
      { lastDeliveredId: "12abc-0" },
    ])).toBe("-");
  });
});

describe("parsePendingTaskMetadata", () => {
  it("parses current JSON pending metadata", () => {
    expect(parsePendingTaskMetadata(JSON.stringify({
      task_id: "task-123",
      created_at: "2026-06-16T03:00:00Z",
    }))).toEqual({
      taskId: "task-123",
      createdAt: "2026-06-16T03:00:00Z",
    });
  });

  it("accepts legacy plain task ID pending markers", () => {
    expect(parsePendingTaskMetadata("legacy-task")).toEqual({
      taskId: "legacy-task",
      createdAt: null,
    });
  });

  it("returns null for missing pending markers", () => {
    expect(parsePendingTaskMetadata(null)).toBeNull();
  });
});

describe("lockInfoFromRedisValues", () => {
  it("joins a prefixed lock key with pending task metadata", () => {
    const lock = lockInfoFromRedisValues(
      "project-a:lock:pr:owner/repo:42",
      "worker-1",
      180,
      JSON.stringify({
        task_id: "task-123",
        created_at: "2026-06-16T03:00:00Z",
      }),
    );

    expect(lock).toEqual({
      lock_key: "project-a:lock:pr:owner/repo:42",
      prefix: "project-a",
      resource: "owner/repo:42",
      resource_type: "pr",
      repo: "owner/repo",
      resource_id: "42",
      owner: "worker-1",
      ttl: 180,
      task_id: "task-123",
      pending_created_at: "2026-06-16T03:00:00Z",
    });
  });

  it("handles unprefixed locks and absent pending metadata", () => {
    const lock = lockInfoFromRedisValues(
      "lock:issue:owner/repo:7",
      "",
      "bad ttl",
      null,
    );

    expect(lock).toMatchObject({
      lock_key: "lock:issue:owner/repo:7",
      prefix: null,
      resource_type: "issue",
      repo: "owner/repo",
      resource_id: "7",
      owner: "(expired)",
      ttl: -1,
      task_id: null,
      pending_created_at: null,
    });
  });

  it("drops locks that expired between scan and ttl lookup", () => {
    expect(lockInfoFromRedisValues(
      "lock:pr:owner/repo:42",
      null,
      -2,
      null,
    )).toBeNull();
  });
});

describe("parseAttemptKey", () => {
  it("parses prefixed PR attempt keys", () => {
    expect(parseAttemptKey("project-a:pr:owner/repo:42:attempts")).toEqual({
      prefix: "project-a",
      resourceType: "pr",
      repo: "owner/repo",
      resourceId: "42",
    });
  });

  it("parses unprefixed issue attempt keys", () => {
    expect(parseAttemptKey("issue:owner/repo:7:attempts")).toEqual({
      prefix: "",
      resourceType: "issue",
      repo: "owner/repo",
      resourceId: "7",
    });
  });

  it("ignores unrelated attempt-like keys", () => {
    expect(parseAttemptKey("project-a:branch:owner/repo:main:attempts")).toBeNull();
    expect(parseAttemptKey("project-a:pr:owner/repo:42:total_attempts")).toBeNull();
  });
});

describe("attemptCountLabelFromKey", () => {
  it("formats PR and issue attempt counter labels", () => {
    expect(attemptCountLabelFromKey("project-a:pr:owner/repo:42:attempts"))
      .toBe("[project-a] owner/repo PR #42");
    expect(attemptCountLabelFromKey("issue:owner/repo:7:attempts"))
      .toBe("owner/repo Issue #7");
  });

  it("ignores total-attempt and unrelated keys", () => {
    expect(attemptCountLabelFromKey("project-a:pr:owner/repo:42:total_attempts"))
      .toBeNull();
    expect(attemptCountLabelFromKey("project-a:branch:owner/repo:main:attempts"))
      .toBeNull();
  });
});

describe("attemptCountValueFromFields", () => {
  it("accepts positive integer attempt counts", () => {
    expect(attemptCountValueFromFields({ count: "2" })).toBe(2);
  });

  it("rejects malformed, fractional, and non-positive attempt counts", () => {
    expect(attemptCountValueFromFields(null)).toBeNull();
    expect(attemptCountValueFromFields({ count: "2bad" })).toBeNull();
    expect(attemptCountValueFromFields({ count: "1.5" })).toBeNull();
    expect(attemptCountValueFromFields({ count: "0" })).toBeNull();
    expect(attemptCountValueFromFields({ count: "-1" })).toBeNull();
    expect(attemptCountValueFromFields({ count: true })).toBeNull();
    expect(attemptCountValueFromFields({ count: [2] })).toBeNull();
  });
});

describe("providerMetricFromKey", () => {
  it("parses prefixed and unprefixed provider metric keys", () => {
    expect(providerMetricFromKey("orcest:providers:claude:exhausted_skip")).toEqual({
      provider: "[orcest] claude",
      metric: "exhausted_skip",
    });
    expect(providerMetricFromKey("providers:grok:refresh_failures")).toEqual({
      provider: "grok",
      metric: "refresh_failures",
    });
  });

  it("keeps project-prefixed provider counters distinct", () => {
    expect(providerMetricFromKey("project-a:providers:grok:refresh_failures")).toEqual({
      provider: "[project-a] grok",
      metric: "refresh_failures",
    });
    expect(providerMetricFromKey("project-b:providers:grok:refresh_failures")).toEqual({
      provider: "[project-b] grok",
      metric: "refresh_failures",
    });
  });

  it("ignores credential override keys and malformed provider keys", () => {
    expect(providerMetricFromKey("orcest:providers:credential_overrides")).toBeNull();
    expect(providerMetricFromKey("providers:credential_overrides")).toBeNull();
    expect(providerMetricFromKey("orcest:providers:credential_overrides:grok")).toBeNull();
    expect(providerMetricFromKey("providers:credential_overrides:grok")).toBeNull();
    expect(providerMetricFromKey("providers:claude")).toBeNull();
  });
});

describe("addProviderMetricValue", () => {
  it("aggregates duplicate provider metric counters", () => {
    const health: Record<string, Record<string, number>> = {};

    expect(addProviderMetricValue(
      health,
      { provider: "claude", metric: "exhausted_skip" },
      "2",
    )).toBe(true);
    expect(addProviderMetricValue(
      health,
      { provider: "claude", metric: "exhausted_skip" },
      "3",
    )).toBe(true);

    expect(health).toEqual({
      claude: { exhausted_skip: 5 },
    });
  });

  it("ignores malformed and negative counter values", () => {
    const health: Record<string, Record<string, number>> = {};

    expect(addProviderMetricValue(
      health,
      { provider: "claude", metric: "exhausted_skip" },
      "not-a-number",
    )).toBe(false);
    expect(addProviderMetricValue(
      health,
      { provider: "claude", metric: "exhausted_skip" },
      "2bad",
    )).toBe(false);
    expect(addProviderMetricValue(
      health,
      { provider: "claude", metric: "exhausted_skip" },
      "-1",
    )).toBe(false);
    expect(addProviderMetricValue(
      health,
      { provider: "claude", metric: "exhausted_skip" },
      "1.5",
    )).toBe(false);
    expect(addProviderMetricValue(
      health,
      { provider: "claude", metric: "exhausted_skip" },
      true,
    )).toBe(false);
    expect(addProviderMetricValue(
      health,
      { provider: "claude", metric: "exhausted_skip" },
      [2],
    )).toBe(false);
    expect(health).toEqual({});
  });
});

describe("normalizeWorkerPoolSnapshot", () => {
  it("sorts idle and active VM IDs numerically", () => {
    const pool = normalizeWorkerPoolSnapshot(
      "orcest",
      "9001",
      ["302", "300", " 301 ", "301", "", "   "],
      { "310": "1000", "305": "900", " 305 ": "950", "   ": "1100" },
      1200,
    );

    expect(pool).toMatchObject({
      prefix: "orcest",
      template_vmid: "9001",
      idle: ["300", "301", "302"],
      idle_count: 3,
      active_count: 2,
    });
    expect(pool.active.map((vm) => vm.vmid)).toEqual(["305", "310"]);
    expect(pool.active[0]).toMatchObject({
      started_at: "1970-01-01T00:15:00.000Z",
      age_seconds: 300,
    });
  });

  it("handles missing template and invalid active timestamps", () => {
    const pool = normalizeWorkerPoolSnapshot(
      "",
      "",
      null,
      { bad: "not-a-number" },
      1200,
    );

    expect(pool.template_vmid).toBeNull();
    expect(pool.idle_count).toBe(0);
    expect(pool.active).toEqual([
      { vmid: "bad", started_at: null, age_seconds: null },
    ]);
  });

  it("keeps active VM rows when timestamps are outside the JavaScript Date range", () => {
    const pool = normalizeWorkerPoolSnapshot(
      "orcest",
      null,
      [],
      { "300": "1e20" },
      1200,
    );

    expect(pool.active).toEqual([
      { vmid: "300", started_at: null, age_seconds: null },
    ]);
  });
});

describe("recentResultFromFields", () => {
  it("adds a stable identity from the Redis stream and entry ID", () => {
    const result = recentResultFromFields("orcest:results", "123-4", {
      task_id: "task-a",
      worker_id: "worker-1",
      status: "COMPLETED",
      repo: "owner/repo",
      resource_type: "pr",
      resource_id: "42",
      duration_seconds: "17",
      summary: "done",
    });

    expect(result).toEqual({
      result_id: "orcest:results:123-4",
      result_stream: "orcest:results",
      entry_id: "123-4",
      task_id: "task-a",
      worker_id: "worker-1",
      status: "COMPLETED",
      repo: "owner/repo",
      resource_type: "pr",
      resource_id: "42",
      duration_seconds: 17,
      summary: "done",
    });
  });

  it("falls back safely for malformed durations and missing fields", () => {
    const result = recentResultFromFields("project:results", "456-0", {
      duration_seconds: "not-a-number",
    });

    expect(result.result_id).toBe("project:results:456-0");
    expect(result.duration_seconds).toBe(-1);
    expect(result.task_id).toBe("");
    expect(result.repo).toBeNull();
    expect(result.summary).toBe("");
  });
});

describe("deadLetterEntryFromFields", () => {
  it("adds stable identity from the Redis stream and entry ID", () => {
    expect(deadLetterEntryFromFields("orcest:dead-letter", "123-4", {
      id: "task-1234567890",
      type: "fix_pr",
      repo: "owner/repo",
      resource_type: "pr",
      resource_id: "42",
      dead_letter_reason: "max attempts",
    })).toEqual({
      dead_letter_id: "orcest:dead-letter:123-4",
      dead_letter_stream: "orcest:dead-letter",
      entry_id: "123-4",
      task_id: "task-1234567890",
      task_type: "fix_pr",
      repo: "owner/repo",
      resource_type: "pr",
      resource_id: "42",
      timestamp_ms: 123,
      reason: "max attempts",
    });
  });

  it("falls back safely for malformed entry IDs and missing fields", () => {
    const entry = deadLetterEntryFromFields("dead-letter", "bad-id", {});

    expect(entry.dead_letter_id).toBe("dead-letter:bad-id");
    expect(entry.timestamp_ms).toBeNull();
    expect(entry.task_id).toBe("");
    expect(entry.task_type).toBe("?");
    expect(entry.reason).toBeNull();
  });

  it("does not parse partial numeric stream IDs as timestamps", () => {
    const entry = deadLetterEntryFromFields("dead-letter", "123abc-0", {});

    expect(entry.timestamp_ms).toBeNull();
  });
});
