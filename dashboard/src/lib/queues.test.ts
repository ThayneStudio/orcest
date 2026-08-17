import { describe, expect, it } from "vitest";
import {
  isTaskStreamName,
  normalizeDepth,
  normalizeQueueDepths,
  queueStreamDisplayName,
  redisStreamDisplayName,
} from "./queues";

describe("isTaskStreamName", () => {
  it("recognizes prefixed and unprefixed task streams", () => {
    expect(isTaskStreamName("orcest:tasks:claude")).toBe(true);
    expect(isTaskStreamName("transit-platform:tasks:issue:grok")).toBe(true);
    expect(isTaskStreamName("project:tasks:issue:claude")).toBe(true);
    expect(isTaskStreamName("tasks:claude")).toBe(true);
  });

  it("does not classify retained streams as task backlog", () => {
    expect(isTaskStreamName("results")).toBe(false);
    expect(isTaskStreamName("orcest:results")).toBe(false);
    expect(isTaskStreamName("dead-letter")).toBe(false);
    expect(isTaskStreamName("providers:claude:exhausted_skip")).toBe(false);
  });

  it("rejects malformed task stream names without a provider", () => {
    expect(isTaskStreamName("tasks:")).toBe(false);
    expect(isTaskStreamName("tasks::claude")).toBe(false);
    expect(isTaskStreamName("project-a:tasks:")).toBe(false);
    expect(isTaskStreamName("project-a::tasks:claude")).toBe(false);
    expect(isTaskStreamName("project-a:tasks")).toBe(false);
  });
});

describe("normalizeDepth", () => {
  it("normalizes runtime depth values to non-negative integers", () => {
    expect(normalizeDepth(3)).toBe(3);
    expect(normalizeDepth("4")).toBe(4);
    expect(normalizeDepth(2.8)).toBe(2);
    expect(normalizeDepth(0)).toBe(0);
    expect(normalizeDepth(-1)).toBe(0);
    expect(normalizeDepth(null)).toBe(0);
    expect(normalizeDepth("not-a-number")).toBe(0);
    expect(normalizeDepth(true)).toBe(0);
    expect(normalizeDepth([])).toBe(0);
  });
});

describe("normalizeQueueDepths", () => {
  it("drops empty queue names, normalizes depths, and sorts by backlog priority", () => {
    expect(normalizeQueueDepths({
      "tasks:z": "2",
      "": 10,
      "results": 7,
      "dead-letter": 4,
      "tasks:": 6,
      "tasks::claude": 8,
      "project-a:tasks:": 7,
      "tasks:a": -1,
      "tasks:boolean": true,
      " tasks:m ": 3.9,
    })).toEqual([
      { name: "tasks:m", depth: 3 },
      { name: "tasks:z", depth: 2 },
      { name: "tasks:a", depth: 0 },
      { name: "tasks:boolean", depth: 0 },
    ]);
  });

  it("sorts queues alphabetically when backlog depth ties", () => {
    expect(normalizeQueueDepths({
      "tasks:z": 2,
      "tasks:a": 2,
      "tasks:c": 0,
      "tasks:b": 0,
    })).toEqual([
      { name: "tasks:a", depth: 2 },
      { name: "tasks:z", depth: 2 },
      { name: "tasks:b", depth: 0 },
      { name: "tasks:c", depth: 0 },
    ]);
  });

  it("collapses duplicate trimmed queue names to the highest depth", () => {
    expect(normalizeQueueDepths({
      " tasks:claude ": "2",
      "tasks:claude": "5",
      "tasks:grok": "1",
    })).toEqual([
      { name: "tasks:claude", depth: 5 },
      { name: "tasks:grok", depth: 1 },
    ]);
  });
});

describe("queueStreamDisplayName", () => {
  it("formats task stream names for compact display", () => {
    expect(queueStreamDisplayName("tasks:claude")).toBe("claude");
    expect(queueStreamDisplayName("tasks:issue:grok")).toBe("issue grok");
    expect(queueStreamDisplayName("project-a:tasks:clauder")).toBe("[project-a] clauder");
    expect(queueStreamDisplayName("project-a:tasks:issue:grok")).toBe("[project-a] issue grok");
  });

  it("keeps non-task stream names recognizable", () => {
    expect(queueStreamDisplayName("results")).toBe("results");
    expect(queueStreamDisplayName(" ")).toBe("?");
  });
});

describe("redisStreamDisplayName", () => {
  it("formats prefixed terminal streams without losing the raw namespace", () => {
    expect(redisStreamDisplayName("results")).toBe("results");
    expect(redisStreamDisplayName("dead-letter")).toBe("dead-letter");
    expect(redisStreamDisplayName("project-a:results")).toBe("[project-a] results");
    expect(redisStreamDisplayName("project-a:dead-letter")).toBe("[project-a] dead-letter");
  });

  it("still formats task streams for queue and card displays", () => {
    expect(redisStreamDisplayName("project-a:tasks:issue:grok")).toBe("[project-a] issue grok");
  });
});
