import { describe, expect, it } from "vitest";
import type { ConsumerGroupInfo } from "./types";
import {
  consumerGroupAttentionCount,
  consumerGroupAttentionLabel,
  consumerGroupBacklogEvidence,
  consumerGroupBacklogEstimate,
  consumerGroupConsumerTitle,
  consumerGroupConsumerTone,
  consumerGroupCountTone,
  consumerGroupHasNoConsumersWhileBacklogged,
  consumerGroupNeedsAttention,
  consumerGroupNoConsumerBacklogCount,
  consumerGroupNoConsumerBacklogSummary,
  consumerGroupQueueDepth,
  consumerGroupQueuedPreviewCount,
  consumerGroupRowKey,
  consumerGroupRoleLabel,
  consumerGroupStatusLabel,
  consumerGroupStatusTone,
  consumerGroupStreamKey,
  isWorkerConsumerGroup,
  queuedPreviewCountsByStream,
  sortConsumerGroups,
} from "./consumerGroups";

function group(
  stream: string,
  name: string,
  overrides: Partial<ConsumerGroupInfo> = {},
): ConsumerGroupInfo {
  return {
    stream,
    name,
    consumers: 1,
    pending: 0,
    lag: 0,
    ...overrides,
  };
}

describe("consumer group helpers", () => {
  it("identifies the operational worker group", () => {
    expect(isWorkerConsumerGroup(group("tasks:claude", "workers"))).toBe(true);
    expect(isWorkerConsumerGroup(group("tasks:claude", "debug-inspector"))).toBe(false);
    expect(consumerGroupRoleLabel(group("tasks:claude", "workers"))).toBe("worker");
    expect(consumerGroupRoleLabel(group("tasks:claude", "debug-inspector"))).toBe("aux");
  });

  it("only highlights pending and lag counts for worker groups", () => {
    expect(consumerGroupCountTone(group("tasks:claude", "workers"), 1))
      .toBe("text-yellow-400");
    expect(consumerGroupCountTone(group("tasks:claude", "workers"), 0))
      .toBe("text-zinc-400");
    expect(consumerGroupCountTone(group("tasks:claude", "workers"), null))
      .toBe("text-yellow-300");
    expect(consumerGroupCountTone(group("tasks:claude", "debug-inspector"), 99))
      .toBe("text-zinc-500");
  });

  it("highlights zero consumers when a worker group has unhandled work", () => {
    const laggingWorker = group("tasks:issue:codex", "workers", {
      consumers: 0,
      lag: 1,
    });
    const healthyIdleWorker = group("tasks:issue:codex", "workers", {
      consumers: 0,
    });
    const auxiliaryGroup = group("tasks:issue:codex", "debug-inspector", {
      consumers: 0,
      lag: 1,
    });

    expect(consumerGroupHasNoConsumersWhileBacklogged(laggingWorker)).toBe(true);
    expect(consumerGroupHasNoConsumersWhileBacklogged(group("tasks:issue:codex", "workers", {
      consumers: 0,
      lag: null,
    }), 2)).toBe(true);
    expect(consumerGroupConsumerTone(laggingWorker)).toBe("text-red-300");
    expect(consumerGroupConsumerTitle(laggingWorker))
      .toBe("0 consumers, worker group has unhandled work");
    expect(consumerGroupHasNoConsumersWhileBacklogged(healthyIdleWorker)).toBe(false);
    expect(consumerGroupConsumerTone(healthyIdleWorker)).toBe("text-zinc-500");
    expect(consumerGroupConsumerTitle(healthyIdleWorker)).toBe("0");
    expect(consumerGroupHasNoConsumersWhileBacklogged(auxiliaryGroup)).toBe(false);
    expect(consumerGroupConsumerTone(auxiliaryGroup)).toBe("text-zinc-500");
  });

  it("summarizes worker consumer group status by the most actionable state", () => {
    expect(consumerGroupStatusLabel(group("tasks:claude", "debug-inspector", {
      pending: 1,
    }))).toBeNull();
    expect(consumerGroupStatusLabel(group("tasks:claude", "workers", {
      consumers: 0,
      pending: 2,
      lag: 3,
    }))).toBe("no consumers");
    expect(consumerGroupStatusLabel(group("tasks:claude", "workers", {
      pending: 2,
      lag: 3,
    }))).toBe("pending");
    expect(consumerGroupStatusLabel(group("tasks:claude", "workers", {
      consumers: 0,
      lag: null,
    }))).toBe("unknown lag");
    expect(consumerGroupStatusLabel(group("tasks:claude", "workers", {
      lag: null,
    }))).toBe("unknown lag");
    expect(consumerGroupStatusLabel(group("tasks:claude", "workers", {
      lag: 3,
    }))).toBe("lagging");
    expect(consumerGroupStatusLabel(group("tasks:claude", "workers", {
      consumers: 0,
    }))).toBe("idle");
    expect(consumerGroupStatusLabel(group("tasks:claude", "workers"))).toBe("healthy");
    expect(consumerGroupStatusTone(group("tasks:claude", "workers", {
      consumers: 0,
      lag: 3,
    }))).toContain("text-red-300");
    expect(consumerGroupStatusTone(group("tasks:claude", "workers", {
      lag: 3,
    }))).toContain("text-yellow-300");
    expect(consumerGroupStatusTone(group("tasks:claude", "workers", {
      consumers: 0,
    }))).toContain("text-zinc-400");
    expect(consumerGroupStatusTone(group("tasks:claude", "workers")))
      .toContain("text-emerald-300");
    expect(consumerGroupStatusTone(group("tasks:claude", "debug-inspector")))
      .toContain("text-zinc-500");
  });

  it("identifies worker consumer groups with pending work or lag as needing attention", () => {
    expect(consumerGroupNeedsAttention(group("tasks:claude", "workers"))).toBe(false);
    expect(consumerGroupNeedsAttention(group("tasks:claude", "workers", { pending: 1 })))
      .toBe(true);
    expect(consumerGroupNeedsAttention(group("tasks:claude", "workers", { lag: 1 })))
      .toBe(true);
    expect(consumerGroupNeedsAttention(group("tasks:claude", "workers", { lag: null })))
      .toBe(false);
    expect(consumerGroupHasNoConsumersWhileBacklogged(group("tasks:claude", "workers", {
      consumers: 0,
      lag: null,
    }))).toBe(false);
    expect(consumerGroupNeedsAttention(group("tasks:claude", "workers", {
      consumers: 0,
      lag: 0,
    }), 2)).toBe(true);
    expect(consumerGroupNeedsAttention(group("tasks:claude", "workers", {
      consumers: 0,
      lag: 0,
    }), 0, 1)).toBe(true);
    expect(consumerGroupStatusLabel(group("tasks:claude", "workers", {
      consumers: 0,
      lag: null,
    }), 2)).toBe("no consumers");
    expect(consumerGroupStatusLabel(group("tasks:claude", "workers", {
      consumers: 0,
      lag: null,
    }), 0, 1)).toBe("no consumers");
    expect(consumerGroupNeedsAttention(group("tasks:claude", "debug-inspector", {
      pending: 99,
      lag: null,
    }))).toBe(false);
  });

  it("counts and labels worker consumer groups needing attention", () => {
    const groups = [
      group("tasks:claude", "workers", { pending: 1 }),
      group("tasks:grok", "workers", { consumers: 0, lag: null }),
      group("tasks:codex", "workers", { consumers: 0, lag: 2 }),
      group("tasks:codex", "workers"),
      group("tasks:claude", "debug-inspector", { pending: 3 }),
    ];

    expect(consumerGroupAttentionCount(groups)).toBe(2);
    expect(consumerGroupNoConsumerBacklogCount(groups)).toBe(1);
    expect(consumerGroupAttentionCount(groups, { "tasks:grok": 2 })).toBe(3);
    expect(consumerGroupNoConsumerBacklogCount(groups, { "tasks:grok": 2 })).toBe(2);
    expect(consumerGroupNoConsumerBacklogCount(groups, {}, { "tasks:grok": 1 })).toBe(2);
    expect(consumerGroupNoConsumerBacklogSummary(groups, { "tasks:grok": 2 })).toEqual({
      groupCount: 2,
      backlogCount: 4,
      exact: true,
    });
    expect(consumerGroupNoConsumerBacklogSummary(groups, {}, { "tasks:grok": 1 })).toEqual({
      groupCount: 2,
      backlogCount: 3,
      exact: false,
    });
    expect(consumerGroupAttentionLabel(1)).toBe("1 worker group needs attention");
    expect(consumerGroupAttentionLabel(2)).toBe("2 worker groups need attention");
    expect(consumerGroupAttentionLabel(1, false, 1))
      .toBe("1 worker group has no consumers");
    expect(consumerGroupAttentionLabel(1, false, 1, 2))
      .toBe("1 worker group has no consumers (2 queued/pending)");
    expect(consumerGroupAttentionLabel(2, false, 2, 3, false))
      .toBe("2 worker groups have no consumers (at least 3 queued/pending)");
    expect(consumerGroupAttentionLabel(3, false, 2))
      .toBe("2 worker groups have no consumers; 1 more needs attention");
    expect(consumerGroupAttentionLabel(3, false, 2, 4))
      .toBe("2 worker groups have no consumers (4 queued/pending); 1 more needs attention");
    expect(consumerGroupAttentionLabel(0)).toBeNull();
    expect(consumerGroupAttentionLabel(0, true)).toBe("partial data");
  });

  it("looks up queue depth and queued previews by trimmed stream names", () => {
    const padded = group(" tasks:grok ", "workers", {
      consumers: 0,
      lag: null,
    });
    const queueDepths = { "tasks:grok": 2 };
    const queuedPreviews = { "tasks:grok": 1 };

    expect(consumerGroupStreamKey(padded)).toBe("tasks:grok");
    expect(consumerGroupQueueDepth(padded, queueDepths)).toBe(2);
    expect(consumerGroupQueuedPreviewCount(padded, queuedPreviews)).toBe(1);
    expect(consumerGroupAttentionCount([padded], queueDepths, {})).toBe(1);
    expect(consumerGroupNoConsumerBacklogCount([padded], {}, queuedPreviews)).toBe(1);
  });

  it("estimates backlog from pending, lag, and queued preview evidence", () => {
    expect(consumerGroupBacklogEstimate(group("tasks:codex", "workers", {
      pending: 3,
      lag: 8,
    }))).toBe(11);
    expect(consumerGroupBacklogEstimate(group("tasks:grok", "workers", {
      pending: 0,
      lag: null,
    }), 4)).toBe(4);
    expect(consumerGroupBacklogEstimate(group("tasks:grok", "workers", {
      pending: 3,
      lag: null,
    }), 4)).toBe(7);
  });

  it("reports backlog evidence source and exactness", () => {
    expect(consumerGroupBacklogEvidence(group("tasks:codex", "workers", {
      pending: 3,
      lag: 8,
    }), { queueDepth: 4, queuedPreview: 10 })).toEqual({
      count: 11,
      source: "consumer-backlog",
      exact: true,
    });
    expect(consumerGroupBacklogEvidence(group("tasks:codex", "workers", {
      pending: 0,
      lag: null,
    }), { queueDepth: 12, queuedPreview: 10 })).toEqual({
      count: 12,
      source: "queue-depth",
      exact: true,
    });
    expect(consumerGroupBacklogEvidence(group("tasks:grok", "workers", {
      pending: 0,
      lag: null,
    }), { queueDepth: 0, queuedPreview: 4 })).toEqual({
      count: 4,
      source: "queued-preview",
      exact: false,
    });
    expect(consumerGroupBacklogEvidence(group("tasks:grok", "workers", {
      pending: 2,
      lag: null,
    }), { queueDepth: 0, queuedPreview: 4 })).toEqual({
      count: 6,
      source: "queued-pending",
      exact: false,
    });
    expect(consumerGroupBacklogEvidence(group("tasks:grok", "workers", {
      pending: 2,
      lag: null,
    }))).toEqual({
      count: 2,
      source: "consumer-backlog",
      exact: false,
    });
    expect(consumerGroupBacklogEvidence(group("tasks:grok", "workers", {
      pending: 0,
      lag: 0,
    }), { queueDepth: -1, queuedPreview: Number.NaN })).toEqual({
      count: 0,
      source: null,
      exact: true,
    });
  });

  it("sorts attention worker groups before healthy workers and auxiliary groups", () => {
    expect(sortConsumerGroups([
      group("tasks:aaa", "workers"),
      group("tasks:grok", "debug-inspector"),
      group("tasks:zzz", "workers", { pending: 1 }),
      group("tasks:claude", "workers", { lag: null }),
      group("tasks:depth", "workers", { consumers: 0 }),
      group("tasks:preview", "workers", { consumers: 0, lag: 0 }),
      group("tasks:claude", "debug-inspector", { pending: 9 }),
    ], { "tasks:depth": 2 }, { "tasks:preview": 1 }).map((row) => `${row.stream}/${row.name}`)).toEqual([
      "tasks:depth/workers",
      "tasks:preview/workers",
      "tasks:zzz/workers",
      "tasks:aaa/workers",
      "tasks:claude/workers",
      "tasks:claude/debug-inspector",
      "tasks:grok/debug-inspector",
    ]);
  });

  it("builds row keys without stream/name separator collisions", () => {
    expect(consumerGroupRowKey(group("a-b", "c")))
      .not.toBe(consumerGroupRowKey(group("a", "b-c")));
  });

  it("counts queued previews by non-empty stream", () => {
    expect(queuedPreviewCountsByStream([
      { stream: "tasks:codex" },
      { stream: " tasks:codex " },
      { stream: "tasks:grok" },
      { stream: " " },
    ])).toEqual({
      "tasks:codex": 2,
      "tasks:grok": 1,
    });
  });
});
