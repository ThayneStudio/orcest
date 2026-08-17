import { describe, expect, it } from "vitest";
import {
  WORKER_RUNTIME_COUNT_CLASS,
  WorkerPool,
  workerDiscoveryStatusText,
  workerConsumerCoverageLabel,
  workerPoolStatusText,
  workerRuntimeCountLabel,
  workerRuntimeUnitLabel,
  workerVmListEmptyMessage,
  workerOutputStreamsEmptyMessage,
  workerPoolEmptyMessage,
} from "./WorkerPool";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";

describe("worker runtime empty messages", () => {
  it("distinguishes no worker activity from unavailable worker discovery", () => {
    expect(workerOutputStreamsEmptyMessage(false)).toBe("no worker output in the last 7 days");
    expect(workerOutputStreamsEmptyMessage(true)).toBe("worker discovery unavailable");
  });

  it("distinguishes missing pool state from degraded pool state", () => {
    expect(workerPoolEmptyMessage(false)).toBe(
      "No VM pool state reported. Recent worker output is activity evidence only.",
    );
    expect(workerPoolEmptyMessage(true)).toBe(
      "VM pool state unavailable. Recent worker output is activity evidence only.",
    );
  });

  it("marks degraded worker runtime counts as loaded or unknown", () => {
    expect(workerRuntimeCountLabel(0, false)).toBe("0");
    expect(workerRuntimeCountLabel(2, false)).toBe("2");
    expect(workerRuntimeCountLabel(0, true)).toBe("?");
    expect(workerRuntimeCountLabel(2, true)).toBe("2 loaded");
  });

  it("labels recent output counts as worker IDs, not Redis streams", () => {
    expect(workerRuntimeUnitLabel(1)).toBe("worker");
    expect(workerRuntimeUnitLabel(2)).toBe("workers");
  });

  it("labels partial worker and pool data quality states", () => {
    expect(workerDiscoveryStatusText(0, false)).toBeNull();
    expect(workerDiscoveryStatusText(0, true)).toBe("worker discovery unavailable");
    expect(workerDiscoveryStatusText(2, true)).toBe("partial worker discovery");
    expect(workerPoolStatusText(false, true)).toBeNull();
    expect(workerPoolStatusText(true, false)).toBeNull();
    expect(workerPoolStatusText(true, true)).toBe("VM pool data may be incomplete");
  });

  it("summarizes worker queues with backlog but no consumers", () => {
    const groups = [
      {
        stream: " orcest:tasks:codex ",
        name: "workers",
        consumers: 0,
        pending: 0,
        lag: null,
      },
      {
        stream: "orcest:tasks:issue:grok",
        name: "workers",
        consumers: 0,
        pending: 0,
        lag: 2,
      },
      {
        stream: "orcest:tasks:issue:codex",
        name: "debug-inspector",
        consumers: 0,
        pending: 0,
        lag: 2,
      },
    ];

    expect(workerConsumerCoverageLabel(
      groups,
      { "orcest:tasks:codex": 3 },
      [],
    )).toBe("[orcest] codex and [orcest] issue grok have 5 queued/pending items with no consumers");
    expect(workerConsumerCoverageLabel([
      {
        stream: "orcest:tasks:codex",
        name: "workers",
        consumers: 0,
        pending: 0,
        lag: null,
      },
    ], {}, [{
      entry_id: "1-0",
      task_id: "task-1",
      task_type: "fix_pr",
      repo: "owner/repo",
      resource_type: "pr",
      resource_id: "1",
      created_at: null,
      stream: "orcest:tasks:codex",
    }])).toBe("[orcest] codex has at least 1 queued/pending item with no consumers");
    expect(workerConsumerCoverageLabel([], {}, [])).toBeNull();
  });

  it("prioritizes the largest no-consumer backlogs in runtime summaries", () => {
    const groups = [
      {
        stream: "orcest:tasks:codex",
        name: "workers",
        consumers: 0,
        pending: 0,
        lag: 11,
      },
      {
        stream: "orcest:tasks:grok",
        name: "workers",
        consumers: 0,
        pending: 0,
        lag: 13,
      },
      {
        stream: "orcest:tasks:issue:codex",
        name: "workers",
        consumers: 0,
        pending: 0,
        lag: 29,
      },
      {
        stream: "orcest:tasks:issue:grok",
        name: "workers",
        consumers: 0,
        pending: 0,
        lag: 25,
      },
    ];

    expect(workerConsumerCoverageLabel(groups)).toBe(
      "[orcest] issue codex, [orcest] issue grok, and 2 more have 78 queued/pending items with no consumers",
    );
  });

  it("does not claim empty VMID lists are none when pool state is degraded", () => {
    expect(workerVmListEmptyMessage(false)).toBe("none");
    expect(workerVmListEmptyMessage(true)).toBe("unavailable");
    expect(workerVmListEmptyMessage(false, 2)).toBe("details unavailable");
  });

  it("renders degraded pool empty VMID lists as unavailable", () => {
    const html = renderToStaticMarkup(
      createElement(WorkerPool, {
        pools: [{
          prefix: "orcest",
          template_vmid: "9001",
          idle: [],
          active: [],
          idle_count: 0,
          active_count: 0,
        }],
        workers: [],
        poolDegraded: true,
      }),
    );

    expect(html).toContain("Idle VMIDs");
    expect(html).toContain("Active VMIDs");
    expect(html).toContain("VM pool data may be incomplete");
    expect(html).toContain("unavailable");
  });

  it("surfaces partial worker discovery when some worker IDs are still loaded", () => {
    const html = renderToStaticMarkup(
      createElement(WorkerPool, {
        pools: [],
        workers: ["orcest-worker-10000"],
        workerDiscoveryDegraded: true,
      }),
    );

    expect(html).toContain("partial worker discovery");
    expect(html).toContain("title=\"1 loaded\">1 loaded</div>");
    expect(html).toContain("border-yellow-500/30");
    expect(html).toContain("text-yellow-300");
  });

  it("does not claim VMID details are empty when only counts are available", () => {
    const html = renderToStaticMarkup(
      createElement(WorkerPool, {
        pools: [{
          prefix: "orcest",
          template_vmid: "9001",
          idle: [],
          active: [],
          idle_count: 3,
          active_count: 2,
        }],
        workers: [],
      }),
    );

    expect(html).toContain("3");
    expect(html).toContain("2");
    expect(html).toContain("details unavailable");
    expect(html).not.toContain(">none<");
  });

  it("renders recent worker output copy without implying current liveness", () => {
    const html = renderToStaticMarkup(
      createElement(WorkerPool, {
        pools: [],
        workers: ["orcest-worker-10000", "orcest-worker-10001"],
      }),
    );

    expect(html).toContain("Recent Worker Output");
    expect(html).toContain("worker IDs with output in the last 7 days, not current liveness");
    expect(html).toContain(">2</div>");
    expect(html).toContain(">workers</div>");
    expect(html).toContain("orcest-worker-10000");
    expect(html).not.toContain("Recent Output Streams");
    expect(html).not.toContain(">streams</div>");
  });

  it("renders no-consumer worker queue coverage in the runtime panel", () => {
    const html = renderToStaticMarkup(
      createElement(WorkerPool, {
        pools: [{
          prefix: "orcest",
          template_vmid: "9001",
          idle: ["10000", "10001"],
          active: [],
          idle_count: 2,
          active_count: 0,
        }],
        workers: ["orcest-worker-10000", "orcest-worker-10001"],
        consumerGroups: [
          {
            stream: "orcest:tasks:codex",
            name: "workers",
            consumers: 0,
            pending: 0,
            lag: 11,
          },
          {
            stream: "orcest:tasks:issue:grok",
            name: "workers",
            consumers: 0,
            pending: 0,
            lag: 25,
          },
        ],
      }),
    );

    expect(html).toContain(
      "[orcest] issue grok and [orcest] codex have 36 queued/pending items with no consumers",
    );
    expect(html).toContain("text-red-300");
  });

  it("keeps large worker runtime counters from widening cards", () => {
    const html = renderToStaticMarkup(
      createElement(WorkerPool, {
        pools: [{
          prefix: "orcest",
          template_vmid: "9001",
          idle: [],
          active: [],
          idle_count: 123456789012345,
          active_count: 987654321098765,
        }],
        workers: Array.from({ length: 12 }, (_, index) => `orcest-worker-${index}`),
        poolDegraded: true,
      }),
    );

    expect(WORKER_RUNTIME_COUNT_CLASS).toContain("break-all");
    expect(html).toContain(
      `class="${WORKER_RUNTIME_COUNT_CLASS} text-sky-300" title="12">12</div>`,
    );
    expect(html).toContain(
      `class="${WORKER_RUNTIME_COUNT_CLASS} text-yellow-300" title="123456789012345 loaded">123456789012345 loaded</div>`,
    );
    expect(html).toContain(
      `class="${WORKER_RUNTIME_COUNT_CLASS} text-yellow-300" title="987654321098765 loaded">987654321098765 loaded</div>`,
    );
  });
});
