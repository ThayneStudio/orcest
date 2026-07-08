import { describe, expect, it } from "vitest";
import {
  latestResultByEntryId,
  partitionResultsByStatus,
  resultColumnForStatus,
  resultEntryIdParts,
  resultResourceLabel,
  resultStreamResourcePrefix,
  resultStatusLabel,
  resultTimestampMs,
  resultTypeLabel,
} from "./results";
import type { RecentResult } from "./types";

function result(status: string, id = status): RecentResult {
  return {
    result_id: id,
    result_stream: "results",
    entry_id: `${id}-0`,
    task_id: `task-${id}`,
    worker_id: "worker-1",
    status,
    repo: "owner/repo",
    resource_type: "issue",
    resource_id: "1",
    duration_seconds: 10,
    summary: "",
  };
}

describe("resultColumnForStatus", () => {
  it("classifies completed results separately", () => {
    expect(resultColumnForStatus("completed")).toBe("completed");
    expect(resultColumnForStatus(" COMPLETED ")).toBe("completed");
  });

  it("classifies attention states as failed", () => {
    expect(resultColumnForStatus("failed")).toBe("failed");
    expect(resultColumnForStatus("blocked")).toBe("failed");
    expect(resultColumnForStatus("usage_exhausted")).toBe("failed");
  });

  it("keeps stale and unknown statuses out of the failure column", () => {
    expect(resultColumnForStatus("stale")).toBe("neutral");
    expect(resultColumnForStatus("")).toBe("neutral");
    expect(resultColumnForStatus("cancelled")).toBe("neutral");
  });
});

describe("partitionResultsByStatus", () => {
  it("partitions recent results into Kanban columns", () => {
    const partitions = partitionResultsByStatus([
      result("completed", "done"),
      result("failed", "failed"),
      result("blocked", "blocked"),
      result("usage_exhausted", "usage"),
      result("stale", "stale"),
      result("unknown", "unknown"),
    ]);

    expect(partitions.completed.map((r) => r.result_id)).toEqual(["done"]);
    expect(partitions.failed.map((r) => r.result_id)).toEqual(["failed", "blocked", "usage"]);
    expect(partitions.neutral.map((r) => r.result_id)).toEqual(["stale", "unknown"]);
  });
});

describe("result display labels", () => {
  it("formats known status labels for display", () => {
    expect(resultStatusLabel("COMPLETED")).toBe("Completed");
    expect(resultStatusLabel("  failed  ")).toBe("Failed");
    expect(resultStatusLabel("blocked")).toBe("Blocked");
    expect(resultStatusLabel("usage_exhausted")).toBe("Usage exhausted");
    expect(resultStatusLabel("stale")).toBe("Stale");
  });

  it("falls back for blank and unknown statuses", () => {
    expect(resultStatusLabel("")).toBe("unknown");
    expect(resultStatusLabel("cancelled")).toBe("cancelled");
  });

  it("formats resource type labels", () => {
    expect(resultTypeLabel("pr")).toBe("PR");
    expect(resultTypeLabel(" ISSUE ")).toBe("Issue");
    expect(resultTypeLabel("")).toBe("?");
  });

  it("formats resource labels with safe fallbacks", () => {
    expect(resultResourceLabel(result("completed"))).toBe("Issue owner/repo #1");
    expect(resultResourceLabel({
      ...result("completed"),
      repo: null,
      resource_type: "pr",
      resource_id: "",
    })).toBe("PR #?");
    expect(resultResourceLabel({
      ...result("completed"),
      repo: null,
      resource_type: "",
      resource_id: "",
    })).toBe("? #?");
  });

  it("derives resource label prefixes from prefixed result streams", () => {
    expect(resultStreamResourcePrefix("results")).toBeNull();
    expect(resultStreamResourcePrefix("bbr-platform:results")).toBe("bbr-platform");
    expect(resultStreamResourcePrefix(" team:a :results ")).toBe("team:a");

    expect(resultResourceLabel({
      ...result("completed"),
      result_stream: "bbr-platform:results",
      repo: null,
      resource_type: "pr",
      resource_id: "4248",
    })).toBe("[bbr-platform] PR #4248");
  });
});

describe("resultTimestampMs", () => {
  it("derives comparable parts from Redis stream entry IDs", () => {
    expect(resultEntryIdParts("1710000000000-12")).toEqual([1710000000000, 12]);
    expect(resultEntryIdParts(" 1710000000000-0 ")).toEqual([1710000000000, 0]);
  });

  it("derives timestamps from Redis stream entry IDs", () => {
    expect(resultTimestampMs("1710000000000-12")).toBe(1710000000000);
    expect(resultTimestampMs(" 1710000000000-0 ")).toBe(1710000000000);
  });

  it("rejects malformed or unsafe stream IDs", () => {
    expect(resultTimestampMs("")).toBeNull();
    expect(resultTimestampMs("not-a-stream-id")).toBeNull();
    expect(resultTimestampMs("1-*")).toBeNull();
    expect(resultTimestampMs("-1-0")).toBeNull();
    expect(resultTimestampMs("9007199254740992-0")).toBeNull();
    expect(resultEntryIdParts("1-9007199254740992")).toBeNull();
  });
});

describe("latestResultByEntryId", () => {
  it("returns the newest result by Redis stream timestamp and sequence", () => {
    expect(latestResultByEntryId([
      { ...result("failed", "older"), entry_id: "1710000001000-10" },
      { ...result("failed", "newer"), entry_id: "1710000001000-11" },
      { ...result("failed", "oldest"), entry_id: "1710000000000-0" },
    ])?.result_id).toBe("newer");
  });

  it("prefers parseable entry IDs over malformed entry IDs", () => {
    expect(latestResultByEntryId([
      { ...result("failed", "bad"), entry_id: "not-a-stream-id" },
      { ...result("failed", "good"), entry_id: "1710000000000-0" },
    ])?.result_id).toBe("good");
  });
});
