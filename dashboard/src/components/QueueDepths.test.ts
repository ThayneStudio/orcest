import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import type { SystemSnapshot } from "../lib/types";
import {
  QueueDepths,
  QUEUE_DEPTH_VALUE_CLASS,
  queueBacklogSummaryLabel,
  queueDepthDetailLabel,
  queueDepthDisplayValue,
  queueDepthTaskRows,
  queueNoConsumerSummaryLabel,
} from "./QueueDepths";

function snapshot(overrides: Partial<SystemSnapshot> = {}): SystemSnapshot {
  return {
    redis_ok: true,
    fetched_at: "2026-06-20T00:00:00Z",
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

describe("queueDepthDisplayValue", () => {
  it("shows unknown instead of zero when a depth section is degraded", () => {
    expect(queueDepthDisplayValue(0, false)).toBe("0");
    expect(queueDepthDisplayValue(12, false)).toBe("12");
    expect(queueDepthDisplayValue(0, true)).toBe("?");
    expect(queueDepthDisplayValue(12, true)).toBe("12 loaded");
  });
});

describe("queueDepthDetailLabel", () => {
  it("marks degraded loaded queue depths as partial", () => {
    expect(queueDepthDetailLabel(true, 12, true)).toBe("partial");
    expect(queueDepthDetailLabel(false, 12, true)).toBe("partial");
    expect(queueDepthDetailLabel(true, 0, true)).toBe("unavailable");
  });

  it("distinguishes healthy task queues from retained streams", () => {
    expect(queueDepthDetailLabel(true, 12, false)).toBe("pending + lag");
    expect(queueDepthDetailLabel(false, 12, false)).toBe("retained");
    expect(queueDepthDetailLabel(false, 12, false, true)).toBe("retained total");
    expect(queueDepthDetailLabel(true, 1, false, false, "queued-preview")).toBe("queued preview");
    expect(queueDepthDetailLabel(true, 2, false, false, "queued-preview")).toBe("queued previews");
    expect(queueDepthDetailLabel(true, 3, false, false, "queued-pending")).toBe("queued/pending");
    expect(queueDepthDetailLabel(true, 2, false, false, "consumer-backlog")).toBe("consumer backlog");
  });
});

describe("queueBacklogSummaryLabel", () => {
  it("summarizes exact healthy task backlog", () => {
    expect(queueBacklogSummaryLabel(2, 7)).toBe("7 queued/pending across 2 streams");
    expect(queueBacklogSummaryLabel(1, 1)).toBe("1 queued/pending across 1 stream");
    expect(queueBacklogSummaryLabel(1, 1, false, false))
      .toBe("at least 1 queued/pending across 1 stream");
  });

  it("does not report exact zero backlog when queue depths are unavailable", () => {
    expect(queueBacklogSummaryLabel(0, 0, true)).toBeNull();
  });

  it("marks degraded loaded backlog as partial data", () => {
    expect(queueBacklogSummaryLabel(2, 7, true)).toBe("7 loaded across 2 streams");
  });
});

describe("queueNoConsumerSummaryLabel", () => {
  it("summarizes streams with backlogged work and no consumers", () => {
    expect(queueNoConsumerSummaryLabel([])).toBeNull();
    expect(queueNoConsumerSummaryLabel(["[orcest] issue codex"]))
      .toBe("[orcest] issue codex has no consumers");
    expect(queueNoConsumerSummaryLabel(["[orcest] issue codex", "[orcest] issue grok"]))
      .toBe("[orcest] issue codex and [orcest] issue grok have no consumers");
    expect(queueNoConsumerSummaryLabel([
      "[orcest] issue codex",
      "[orcest] issue grok",
      "[orcest] issue clauder",
    ])).toBe("[orcest] issue codex, [orcest] issue grok, and [orcest] issue clauder have no consumers");
    expect(queueNoConsumerSummaryLabel([
      "[orcest] issue codex",
      "[orcest] issue grok",
      "[orcest] issue clauder",
      "[orcest] issue claude",
    ])).toBe("[orcest] issue codex, [orcest] issue grok, and 2 more streams have no consumers");
  });
});

describe("queueDepthTaskRows", () => {
  it("adds queued-preview task streams when exact depth rows are missing", () => {
    const snap = snapshot({
      queued_tasks: [
        {
          entry_id: "1-0",
          task_id: "task-1",
          task_type: "issue",
          repo: "example/repo",
          resource_type: "issue",
          resource_id: "123",
          created_at: "2026-06-20T00:00:00Z",
          stream: "orcest:tasks:issue:grok",
        },
      ],
    });

    expect(queueDepthTaskRows(snap)).toEqual([{
      name: "orcest:tasks:issue:grok",
      depth: 1,
      evidence: "queued-preview",
    }]);
  });

  it("uses queued previews to avoid stale zero-depth task rows", () => {
    const snap = snapshot({
      queue_depths: {
        "orcest:tasks:issue:grok": 0,
      },
      queued_tasks: [
        {
          entry_id: "1-0",
          task_id: "task-1",
          task_type: "issue",
          repo: "example/repo",
          resource_type: "issue",
          resource_id: "123",
          created_at: "2026-06-20T00:00:00Z",
          stream: "orcest:tasks:issue:grok",
        },
        {
          entry_id: "2-0",
          task_id: "task-2",
          task_type: "issue",
          repo: "example/repo",
          resource_type: "issue",
          resource_id: "124",
          created_at: "2026-06-20T00:01:00Z",
          stream: "orcest:tasks:issue:grok",
        },
      ],
    });

    expect(queueDepthTaskRows(snap)).toEqual([{
      name: "orcest:tasks:issue:grok",
      depth: 2,
      evidence: "queued-preview",
    }]);
  });

  it("adds no-consumer worker lag when depth rows and previews are absent", () => {
    const snap = snapshot({
      consumer_groups: [
        {
          stream: "orcest:tasks:issue:codex",
          name: "workers",
          consumers: 0,
          pending: 2,
          lag: 3,
        },
      ],
    });

    expect(queueDepthTaskRows(snap)).toEqual([{
      name: "orcest:tasks:issue:codex",
      depth: 5,
      evidence: "consumer-backlog",
    }]);
  });

  it("adds worker pending backlog even when consumers are present", () => {
    const snap = snapshot({
      consumer_groups: [
        {
          stream: "orcest:tasks:issue:codex",
          name: "workers",
          consumers: 1,
          pending: 2,
          lag: null,
        },
      ],
    });

    expect(queueDepthTaskRows(snap)).toEqual([{
      name: "orcest:tasks:issue:codex",
      depth: 2,
      evidence: "consumer-backlog",
    }]);
  });

  it("labels mixed pending plus queued previews as queued/pending", () => {
    const snap = snapshot({
      queued_tasks: [
        {
          entry_id: "1-0",
          task_id: "task-1",
          task_type: "issue",
          repo: "example/repo",
          resource_type: "issue",
          resource_id: "123",
          created_at: "2026-06-20T00:00:00Z",
          stream: "orcest:tasks:issue:codex",
        },
      ],
      consumer_groups: [
        {
          stream: "orcest:tasks:issue:codex",
          name: "workers",
          consumers: 1,
          pending: 2,
          lag: null,
        },
      ],
    });

    expect(queueDepthTaskRows(snap)).toEqual([{
      name: "orcest:tasks:issue:codex",
      depth: 3,
      evidence: "queued-pending",
    }]);
  });
});

describe("QueueDepths", () => {
  it("renders readable task stream labels while preserving raw key titles", () => {
    const html = renderToStaticMarkup(
      createElement(QueueDepths, {
        snapshot: snapshot({
          queue_depths: {
            "project-a:tasks:issue:grok": 4,
            "tasks:claude": 2,
          },
        }),
      }),
    );

    expect(html).toContain('title="project-a:tasks:issue:grok"><span>[project-a] issue grok</span>');
    expect(html).toContain('<span class="sr-only"> raw stream project-a:tasks:issue:grok</span>');
    expect(html).toContain('title="tasks:claude"><span>claude</span>');
    expect(html).toContain('<span class="sr-only"> raw stream tasks:claude</span>');
  });

  it("constrains stream cards so long prefixed labels can truncate", () => {
    const html = renderToStaticMarkup(
      createElement(QueueDepths, {
        snapshot: snapshot({
          queue_depths: {
            "extremely-long-project-prefix-that-should-not-expand-the-grid:tasks:issue:claude": 4,
          },
        }),
      }),
    );

    expect(html).toContain("min-w-0 rounded-lg border bg-zinc-900");
    expect(html).toContain(
      "extremely-long-project-prefix-that-should-not-expand-the-grid:tasks:issue:claude",
    );
  });

  it("labels aggregate result and dead-letter depths distinctly from raw streams", () => {
    const html = renderToStaticMarkup(
      createElement(QueueDepths, {
        snapshot: snapshot({
          results_depth: 8,
          dead_letter_count: 3,
        }),
      }),
    );

    expect(html).toContain(
      'title="Aggregated retained results across configured Redis prefixes"><span>all results</span>',
    );
    expect(html).toContain('<span class="sr-only"> aggregate retained results</span>');
    expect(html).toContain(
      'title="Aggregated retained dead-letter entries across configured Redis prefixes"><span>all dead-letter entries</span>',
    );
    expect(html).toContain(
      '<span class="sr-only"> aggregate retained dead-letter entries</span>',
    );
    expect(html).toContain("retained total");
  });

  it("surfaces degraded aggregate result and dead-letter depths", () => {
    const html = renderToStaticMarkup(
      createElement(QueueDepths, {
        snapshot: snapshot({
          results_depth: 12,
          dead_letter_count: 0,
        }),
        resultsDepthDegraded: true,
        deadLetterDepthDegraded: true,
      }),
    );

    expect(html).toContain("some depths unavailable");
    expect(html).toContain("<span>all results</span>");
    expect(html).toContain("12 loaded");
    expect(html).toContain("partial");
    expect(html).toContain("<span>all dead-letter entries</span>");
    expect(html).toContain("?");
    expect(html).toContain("unavailable");
  });

  it("summarizes task backlog without counting retained result streams", () => {
    const html = renderToStaticMarkup(
      createElement(QueueDepths, {
        snapshot: snapshot({
          queue_depths: {
            "tasks:a": 0,
            "tasks:z": 7,
            "tasks:m": 3,
          },
          results_depth: 99,
          dead_letter_count: 99,
        }),
      }),
    );

    expect(html).toContain("10 queued/pending across 2 streams");
    expect(html).not.toContain("208 queued/pending");
  });

  it("connects backlogged streams to missing worker consumers", () => {
    const html = renderToStaticMarkup(
      createElement(QueueDepths, {
        snapshot: snapshot({
          queue_depths: {
            "orcest:tasks:issue:codex": 1,
            "orcest:tasks:issue:clauder": 0,
          },
          consumer_groups: [
            {
              stream: "orcest:tasks:issue:codex",
              name: "workers",
              consumers: 0,
              pending: 0,
              lag: 1,
            },
            {
              stream: "orcest:tasks:issue:clauder",
              name: "workers",
              consumers: 0,
              pending: 0,
              lag: 1,
            },
            {
              stream: "orcest:tasks:issue:grok",
              name: "debug-inspector",
              consumers: 0,
              pending: 0,
              lag: 1,
            },
          ],
        }),
      }),
    );

    expect(html).toContain("at least 2 queued/pending across 2 streams");
    expect(html).toContain("[orcest] issue codex and [orcest] issue clauder have no consumers");
    expect(html).toContain('title="orcest:tasks:issue:codex"><span>[orcest] issue codex</span>');
    expect(html).toContain('title="orcest:tasks:issue:clauder"><span>[orcest] issue clauder</span>');
    expect(html).toContain("no consumers");
    expect(html).not.toContain("3 streams have no consumers");
  });

  it("uses queue depth as backlog evidence when worker lag is unavailable", () => {
    const html = renderToStaticMarkup(
      createElement(QueueDepths, {
        snapshot: snapshot({
          queue_depths: {
            "orcest:tasks:issue:codex": 2,
          },
          consumer_groups: [
            {
              stream: "orcest:tasks:issue:codex",
              name: "workers",
              consumers: 0,
              pending: 0,
              lag: null,
            },
          ],
        }),
      }),
    );

    expect(html).toContain("2 queued/pending across 1 stream");
    expect(html).toContain("[orcest] issue codex has no consumers");
    expect(html).toContain("no consumers");
  });

  it("uses queued previews as backlog evidence when queue depth is zero", () => {
    const html = renderToStaticMarkup(
      createElement(QueueDepths, {
        snapshot: snapshot({
          queue_depths: {
            "orcest:tasks:issue:grok": 0,
          },
          queued_tasks: [
            {
              entry_id: "1-0",
              task_id: "task-1",
              task_type: "issue",
              repo: "example/repo",
              resource_type: "issue",
              resource_id: "123",
              created_at: "2026-06-20T00:00:00Z",
              stream: "orcest:tasks:issue:grok",
            },
          ],
          consumer_groups: [
            {
              stream: "orcest:tasks:issue:grok",
              name: "workers",
              consumers: 0,
              pending: 0,
              lag: null,
            },
          ],
        }),
      }),
    );

    expect(html).toContain("at least 1 queued/pending across 1 stream");
    expect(html).toContain("[orcest] issue grok has no consumers");
    expect(html).toContain('title="orcest:tasks:issue:grok"><span>[orcest] issue grok</span>');
    expect(html).toContain(`class="${QUEUE_DEPTH_VALUE_CLASS} text-yellow-400" title="1">1</div>`);
    expect(html).toContain("queued preview");
    expect(html).toContain("no consumers");
  });

  it("renders queued-preview streams even when queue depth is missing", () => {
    const html = renderToStaticMarkup(
      createElement(QueueDepths, {
        snapshot: snapshot({
          queued_tasks: [
            {
              entry_id: "1-0",
              task_id: "task-1",
              task_type: "issue",
              repo: "example/repo",
              resource_type: "issue",
              resource_id: "123",
              created_at: "2026-06-20T00:00:00Z",
              stream: "orcest:tasks:issue:grok",
            },
          ],
          consumer_groups: [
            {
              stream: "orcest:tasks:issue:grok",
              name: "workers",
              consumers: 0,
              pending: 0,
              lag: null,
            },
          ],
        }),
      }),
    );

    expect(html).not.toContain("No task streams");
    expect(html).toContain("at least 1 queued/pending across 1 stream");
    expect(html).toContain("[orcest] issue grok has no consumers");
    expect(html).toContain('title="orcest:tasks:issue:grok"><span>[orcest] issue grok</span>');
    expect(html).toContain("queued preview");
    expect(html).toContain("no consumers");
  });

  it("renders no-consumer worker lag even when queue depth and previews are missing", () => {
    const html = renderToStaticMarkup(
      createElement(QueueDepths, {
        snapshot: snapshot({
          consumer_groups: [
            {
              stream: "orcest:tasks:issue:codex",
              name: "workers",
              consumers: 0,
              pending: 2,
              lag: 3,
            },
          ],
        }),
      }),
    );

    expect(html).not.toContain("No task streams");
    expect(html).toContain("at least 5 queued/pending across 1 stream");
    expect(html).toContain("[orcest] issue codex has no consumers");
    expect(html).toContain('title="orcest:tasks:issue:codex"><span>[orcest] issue codex</span>');
    expect(html).toContain("consumer backlog");
    expect(html).toContain("no consumers");
  });

  it("renders active worker pending backlog when queue depth and previews are missing", () => {
    const html = renderToStaticMarkup(
      createElement(QueueDepths, {
        snapshot: snapshot({
          consumer_groups: [
            {
              stream: "orcest:tasks:issue:codex",
              name: "workers",
              consumers: 1,
              pending: 2,
              lag: null,
            },
          ],
        }),
      }),
    );

    expect(html).not.toContain("No task streams");
    expect(html).toContain("at least 2 queued/pending across 1 stream");
    expect(html).not.toContain("[orcest] issue codex has no consumers");
    expect(html).toContain('title="orcest:tasks:issue:codex"><span>[orcest] issue codex</span>');
    expect(html).toContain("consumer backlog");
  });

  it("names multiple live backlogged streams that have no worker consumers", () => {
    const html = renderToStaticMarkup(
      createElement(QueueDepths, {
        snapshot: snapshot({
          queue_depths: {
            "orcest:tasks:issue:codex": 1,
            "orcest:tasks:issue:grok": 1,
          },
          consumer_groups: [
            {
              stream: "orcest:tasks:issue:codex",
              name: "workers",
              consumers: 0,
              pending: 0,
              lag: 1,
            },
            {
              stream: "orcest:tasks:issue:grok",
              name: "workers",
              consumers: 0,
              pending: 0,
              lag: 1,
            },
          ],
        }),
      }),
    );

    expect(html).toContain("2 queued/pending across 2 streams");
    expect(html).toContain(
      "[orcest] issue codex and [orcest] issue grok have no consumers",
    );
    expect(html).not.toContain("2 streams have no consumers");
  });

  it("renders task queues by backlog before retained aggregate cards", () => {
    const html = renderToStaticMarkup(
      createElement(QueueDepths, {
        snapshot: snapshot({
          queue_depths: {
            "tasks:a": 0,
            "tasks:z": 7,
            "tasks:m": 3,
          },
          results_depth: 99,
          dead_letter_count: 99,
        }),
      }),
    );
    const zIndex = html.indexOf('title="tasks:z"><span>z</span>');
    const mIndex = html.indexOf('title="tasks:m"><span>m</span>');
    const aIndex = html.indexOf('title="tasks:a"><span>a</span>');
    const resultsIndex = html.indexOf("<span>all results</span>");

    expect(zIndex).toBeGreaterThanOrEqual(0);
    expect(mIndex).toBeGreaterThan(zIndex);
    expect(aIndex).toBeGreaterThan(mIndex);
    expect(resultsIndex).toBeGreaterThan(aIndex);
  });

  it("keeps large depth values constrained within mobile cards", () => {
    const html = renderToStaticMarkup(
      createElement(QueueDepths, {
        snapshot: snapshot({
          queue_depths: {
            "tasks:claude": 123456789,
          },
          results_depth: 987654321,
        }),
        resultsDepthDegraded: true,
      }),
    );

    expect(QUEUE_DEPTH_VALUE_CLASS).toContain("min-w-0");
    expect(QUEUE_DEPTH_VALUE_CLASS).toContain("break-all");
    expect(QUEUE_DEPTH_VALUE_CLASS).toContain("leading-tight");
    expect(html).toContain(`class="${QUEUE_DEPTH_VALUE_CLASS} text-red-400"`);
    expect(html).toContain('title="987654321 loaded">987654321 loaded</div>');
  });
});
