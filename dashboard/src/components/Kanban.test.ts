import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { afterEach, describe, expect, it, vi } from "vitest";
import { formatTimestampMs } from "../lib/format";
import { resultTimestampMs } from "../lib/results";
import type { LockInfo, QueuedTask, RecentResult, SystemSnapshot } from "../lib/types";
import {
  Kanban,
  KANBAN_BOARD_CLASS,
  KANBAN_OUTPUT_PANEL_CLASS,
  kanbanBacklogEmptyMessage,
  kanbanBacklogPartialMessage,
  kanbanBacklogUnavailableMessage,
  kanbanColumnTitleId,
  kanbanActiveEmptyMessage,
  kanbanColumnsPaneClassName,
  kanbanQueuedTimestampText,
  kanbanQueuedGroupingMessage,
  kanbanOutputSelectionStillVisible,
  kanbanConsumerBacklogRowLabel,
  kanbanResultEmptyMessage,
  kanbanResultColumnCountLabel,
  kanbanResultLimitMessage,
  kanbanResultPreviewMessage,
  kanbanResultTimestampText,
  kanbanTaskIdDisplay,
  kanbanTaskOutputPanelKey,
  kanbanVisibleRecentResults,
  queuedColumnCount,
  queuedResourceDuplicateLabel,
  queuedResourceDuplicateTitle,
  queuedResourceOccurrenceCounts,
  queuedResourcePreviewRows,
  queuedResourceKey,
  queuedResourceTaskTypeLabel,
  queuedResourceTaskTypeTitle,
  queuedResourceStreamDisplay,
  queuedResourceStreamTitle,
  queuedResourceStreamCounts,
  queuedTaskKey,
  queuedPreviewState,
} from "./Kanban";

const KANBAN_VIEW_LABEL = "View output for PR owner/repo #42 (worker-1, task task-live)";

function queuedTask(id: string): QueuedTask {
  return {
    entry_id: `${id}-0`,
    task_id: id,
    task_type: "fix_pr",
    repo: "owner/repo",
    resource_type: "pr",
    resource_id: id,
    created_at: null,
    stream: "tasks:claude",
  };
}

afterEach(() => {
  vi.useRealTimers();
});

function lock(overrides: Partial<LockInfo> = {}): LockInfo {
  return {
    lock_key: "lock:pr:owner/repo:42",
    prefix: null,
    resource: "owner/repo:42",
    resource_type: "pr",
    repo: "owner/repo",
    resource_id: "42",
    owner: "worker-1",
    ttl: 180,
    task_id: "task-live",
    pending_created_at: null,
    ...overrides,
  };
}

function result(overrides: Partial<RecentResult> = {}): RecentResult {
  return {
    result_id: "results:1-0",
    result_stream: "results",
    entry_id: "1-0",
    task_id: "task-historical",
    worker_id: "worker-1",
    status: "COMPLETED",
    repo: "owner/repo",
    resource_type: "pr",
    resource_id: "42",
    duration_seconds: 10,
    summary: "done",
    ...overrides,
  };
}

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

describe("queuedColumnCount", () => {
  it("uses queue depth totals instead of capped preview length", () => {
    expect(queuedColumnCount({
      queue_depths: {
        "tasks:claude": 51,
        "project-a:tasks:codex": 4,
      },
      queued_tasks: [queuedTask("1"), queuedTask("2")],
    })).toBe(55);
  });

  it("falls back to preview length when queue depths are missing or degraded", () => {
    expect(queuedColumnCount({
      queue_depths: {},
      queued_tasks: [queuedTask("1"), queuedTask("2")],
    })).toBe(2);
  });

  it("uses no-consumer consumer group backlog when depth and previews are missing", () => {
    expect(queuedColumnCount({
      queue_depths: {},
      queued_tasks: [],
      consumer_groups: [
        {
          stream: "orcest:tasks:codex",
          name: "workers",
          consumers: 0,
          pending: 3,
          lag: 8,
        },
      ],
    })).toBe(11);
  });

  it("uses worker pending backlog even when consumers are present", () => {
    expect(queuedColumnCount({
      queue_depths: {},
      queued_tasks: [],
      consumer_groups: [
        {
          stream: "orcest:tasks:codex",
          name: "workers",
          consumers: 1,
          pending: 2,
          lag: null,
        },
      ],
    })).toBe(2);
  });
});

describe("queuedTaskKey", () => {
  it("uses Redis stream entry IDs to distinguish otherwise identical queued tasks", () => {
    const first = queuedTask("task-1");
    const second = { ...first, entry_id: "2-0" };

    expect(queuedTaskKey(first)).not.toBe(queuedTaskKey(second));
  });
});

describe("queued resource stream counts", () => {
  it("groups queued resources by prefix, repo, type, and id", () => {
    const task = { ...queuedTask("42"), prefix: "project-a", repo: "owner/repo" };

    expect(queuedResourceKey(task)).toBe("project-a:pr:owner/repo:42");
  });

  it("counts distinct streams for repeated queued resources", () => {
    const tasks = [
      { ...queuedTask("42"), task_id: "task-a", entry_id: "1-0", stream: "tasks:issue:codex" },
      { ...queuedTask("42"), task_id: "task-b", entry_id: "2-0", stream: "tasks:issue:grok" },
      { ...queuedTask("42"), task_id: "task-c", entry_id: "3-0", stream: "tasks:issue:grok" },
      { ...queuedTask("43"), task_id: "task-d", entry_id: "4-0", stream: "tasks:issue:grok" },
    ];
    const counts = queuedResourceStreamCounts(tasks);

    expect(counts.get(queuedResourceKey(tasks[0]))).toBe(2);
    expect(counts.get(queuedResourceKey(tasks[3]))).toBe(1);
    expect(queuedResourceDuplicateLabel(1)).toBeNull();
    expect(queuedResourceDuplicateLabel(2)).toBe("2 streams");
    expect(queuedResourceDuplicateLabel(1, 2)).toBe("2 entries");
    expect(queuedResourceDuplicateLabel(2, 3)).toBe("3 entries in 2 streams");
    expect(queuedResourceDuplicateTitle("2 streams")).toBe(
      "This resource is queued in 2 streams",
    );
    expect(queuedResourceDuplicateTitle("2 entries")).toBe(
      "This resource is queued as 2 entries",
    );
  });

  it("counts duplicate queued entries for a resource within one stream", () => {
    const tasks = [
      { ...queuedTask("42"), task_id: "task-a", entry_id: "1-0", stream: "tasks:codex" },
      { ...queuedTask("42"), task_id: "task-b", entry_id: "2-0", stream: "tasks:codex" },
      { ...queuedTask("43"), task_id: "task-c", entry_id: "3-0", stream: "tasks:codex" },
    ];
    const counts = queuedResourceOccurrenceCounts(tasks);

    expect(counts.get(queuedResourceKey(tasks[0]))).toBe(2);
    expect(counts.get(queuedResourceKey(tasks[2]))).toBe(1);
  });

  it("collapses queued previews by resource while preserving oldest entry and streams", () => {
    const tasks = [
      {
        ...queuedTask("42"),
        task_id: "newer",
        entry_id: "2000-0",
        created_at: "2026-06-20T00:02:00.000Z",
        stream: "tasks:grok",
      },
      {
        ...queuedTask("42"),
        task_id: "older",
        entry_id: "1000-0",
        created_at: "2026-06-20T00:01:00.000Z",
        stream: "tasks:codex",
      },
      {
        ...queuedTask("43"),
        task_id: "other",
        entry_id: "1500-0",
        created_at: "2026-06-20T00:03:00.000Z",
        stream: "tasks:codex",
      },
    ];
    const rows = queuedResourcePreviewRows(tasks);

    expect(rows).toHaveLength(2);
    expect(rows[0]).toMatchObject({
      key: queuedResourceKey(tasks[0]),
      task: expect.objectContaining({ task_id: "older" }),
      occurrenceCount: 2,
      streamCount: 2,
      streams: ["tasks:grok", "tasks:codex"],
    });
    expect(rows[0].tasks.map((task) => task.task_id)).toEqual(["newer", "older"]);
    expect(queuedResourceStreamDisplay(rows[0])).toBe("2 streams");
    expect(queuedResourceStreamTitle(rows[0])).toBe("tasks:grok, tasks:codex");
    expect(queuedResourceStreamDisplay(rows[1])).toBe("codex");
    expect(queuedResourceStreamTitle(rows[1])).toBe("tasks:codex");
    expect(kanbanQueuedGroupingMessage(2, 3)).toBe(
      "Showing 2 queued resources from 3 queued entries",
    );
    expect(kanbanQueuedGroupingMessage(3, 3)).toBeNull();
  });

  it("labels grouped queued resources with mixed task types", () => {
    const rows = queuedResourcePreviewRows([
      { ...queuedTask("42"), task_id: "fix", task_type: "fix_pr", entry_id: "1-0" },
      { ...queuedTask("42"), task_id: "rebase", task_type: "rebase_pr", entry_id: "2-0" },
    ]);

    expect(queuedResourceTaskTypeLabel(rows[0])).toBe("Mixed types");
    expect(queuedResourceTaskTypeTitle(rows[0])).toBe("Queued task types: Fix PR, Rebase");
  });

  it("sorts collapsed queued previews by oldest Redis stream entry ID", () => {
    const tasks = [
      {
        ...queuedTask("43"),
        task_id: "other",
        entry_id: "1710000001000-0",
        stream: "tasks:grok",
      },
      {
        ...queuedTask("42"),
        task_id: "newer-sequence",
        entry_id: "1710000000000-10",
        stream: "tasks:codex",
      },
      {
        ...queuedTask("42"),
        task_id: "older-sequence",
        entry_id: "1710000000000-2",
        stream: "tasks:grok",
      },
    ];
    const rows = queuedResourcePreviewRows(tasks);

    expect(rows.map((row) => row.key)).toEqual([
      queuedResourceKey(tasks[1]),
      queuedResourceKey(tasks[0]),
    ]);
    expect(rows[0].task.task_id).toBe("older-sequence");
    expect(rows[0].tasks.map((task) => task.task_id)).toEqual([
      "newer-sequence",
      "older-sequence",
    ]);
  });
});

describe("kanbanQueuedTimestampText", () => {
  it("falls back to Redis stream ID timestamps when created_at is absent", () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-20T00:02:00.000Z"));

    expect(kanbanQueuedTimestampText({
      entry_id: String(Date.parse("2026-06-20T00:00:00.000Z")) + "-0",
      created_at: null,
    })).toBe("2m ago");
  });

  it("prefers explicit created_at timestamps", () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-20T00:02:00.000Z"));

    expect(kanbanQueuedTimestampText({
      entry_id: String(Date.parse("2026-06-19T00:00:00.000Z")) + "-0",
      created_at: "2026-06-20T00:01:00.000Z",
    })).toBe("1m ago");
  });
});

describe("queuedPreviewState", () => {
  it("distinguishes empty, complete, partial, and unavailable previews", () => {
    expect(queuedPreviewState(0, 0)).toBe("empty");
    expect(queuedPreviewState(2, 2)).toBe("complete");
    expect(queuedPreviewState(55, 50)).toBe("partial");
    expect(queuedPreviewState(3, 0)).toBe("unavailable");
    expect(queuedPreviewState(0, 0, true)).toBe("unavailable");
    expect(queuedPreviewState(2, 2, true)).toBe("partial");
  });
});

describe("kanbanResultColumnCountLabel", () => {
  it("marks preview-derived result column counts as loaded", () => {
    expect(kanbanResultColumnCountLabel(2)).toBe("2");
    expect(kanbanResultColumnCountLabel(2, true)).toBe("2 loaded");
    expect(kanbanResultColumnCountLabel(0, true)).toBe("0 loaded");
    expect(kanbanResultColumnCountLabel(0, true, true)).toBe("0 loaded");
  });
});

describe("kanbanResultPreviewMessage", () => {
  it("explains when Kanban result columns are a retained-results preview", () => {
    expect(kanbanResultPreviewMessage(20, 2695)).toBe(
      "Result columns show 20 loaded of 2695 retained results",
    );
    expect(kanbanResultPreviewMessage(20, 20)).toBeNull();
  });

  it("keeps degraded result preview wording explicit", () => {
    expect(kanbanResultPreviewMessage(20, 2695, true)).toBe(
      "Result columns may be incomplete; showing 20 loaded of 2695 retained results",
    );
    expect(kanbanResultPreviewMessage(20, 0, false, true)).toBe(
      "Result columns show 20 loaded results; total unavailable",
    );
    expect(kanbanResultPreviewMessage(0, 0, false, true)).toBe(
      "Result columns total unavailable",
    );
  });
});

describe("kanban empty messages", () => {
  it("keeps queued/pending wording aligned with queue depth semantics", () => {
    expect(kanbanBacklogEmptyMessage(false)).toBe("No queued or pending work");
    expect(kanbanBacklogEmptyMessage(true)).toBe("Queued/pending work unavailable");
    expect(kanbanBacklogUnavailableMessage(false)).toBe(
      "Queued/pending work reported; task preview unavailable",
    );
    expect(kanbanBacklogUnavailableMessage(true)).toBe("Queued/pending work unavailable");
    expect(kanbanBacklogPartialMessage(3, 10, false)).toBe(
      "Showing 3 queued previews; 10 queued/pending total",
    );
    expect(kanbanBacklogPartialMessage(3, 10, true)).toBe(
      "Queued/pending preview may be incomplete",
    );
  });

  it("distinguishes true empty columns from degraded loaded-result columns", () => {
    expect(kanbanResultEmptyMessage("completed", false)).toBe("No recent completions");
    expect(kanbanResultEmptyMessage("completed", true)).toBe("No completions in loaded results");
    expect(kanbanResultEmptyMessage("failed", false)).toBe("No results need attention");
    expect(kanbanResultEmptyMessage("failed", true)).toBe("No loaded results need attention");
    expect(kanbanResultEmptyMessage("neutral", false)).toBe("No skipped results");
    expect(kanbanResultEmptyMessage("neutral", true)).toBe("No skipped results in loaded results");
  });

  it("does not claim active work is empty when active locks are degraded", () => {
    expect(kanbanActiveEmptyMessage(false)).toBe("No active work");
    expect(kanbanActiveEmptyMessage(true)).toBe("Active work unavailable");
  });
});

describe("kanbanResultLimitMessage", () => {
  it("explains when a result column is capped", () => {
    expect(kanbanResultLimitMessage(20, false)).toBeNull();
    expect(kanbanResultLimitMessage(21, false)).toBe("Showing 20 of 21 results");
    expect(kanbanResultLimitMessage(21, true)).toBe("Showing 20 of 21 loaded results");
  });
});

describe("kanbanTaskIdDisplay", () => {
  it("keeps short task IDs intact and marks truncated IDs explicitly", () => {
    expect(kanbanTaskIdDisplay("task-live")).toBe("task-live");
    expect(kanbanTaskIdDisplay("task-1234567890abcdef")).toBe("task-1234567...");
    expect(kanbanTaskIdDisplay("   ")).toBeNull();
  });
});

describe("kanbanTaskOutputPanelKey", () => {
  it("keeps task output panels distinct across Redis namespaces", () => {
    const base = {
      workerId: "worker-1",
      taskId: "task-live",
      historical: false,
    };

    expect(kanbanTaskOutputPanelKey({ ...base, prefix: "project-a" }))
      .not.toBe(kanbanTaskOutputPanelKey({ ...base, prefix: "project-b" }));
    expect(kanbanTaskOutputPanelKey({ ...base }))
      .not.toBe(kanbanTaskOutputPanelKey({ ...base, prefix: null }));
    expect(kanbanTaskOutputPanelKey({ ...base, prefix: null }))
      .not.toBe(kanbanTaskOutputPanelKey({ ...base, prefix: "project-a" }));
  });
});

describe("kanbanResultTimestampText", () => {
  it("formats result stream IDs as timestamps", () => {
    const entryId = "1710000000000-0";
    expect(kanbanResultTimestampText(result({ entry_id: entryId })))
      .toBe(formatTimestampMs(resultTimestampMs(entryId)));
  });

  it("falls back for malformed result stream IDs", () => {
    expect(kanbanResultTimestampText(result({ entry_id: "not-a-stream-id" }))).toBe("?");
  });
});

describe("kanbanColumnTitleId", () => {
  it("generates stable IDs from visible column titles", () => {
    expect(kanbanColumnTitleId("Needs Attention")).toBe("kanban-column-needs-attention");
  });
});

describe("Kanban selected-output layout", () => {
  it("uses flex min-height classes instead of fixed half-height splits", () => {
    expect(kanbanColumnsPaneClassName(false)).toContain("flex-1");
    const selectedColumnsClass = kanbanColumnsPaneClassName(true);
    expect(selectedColumnsClass).toContain("min-h-0");
    expect(selectedColumnsClass).toContain("flex-[1_1_0]");
    expect(selectedColumnsClass).not.toContain("h-1/2");

    expect(KANBAN_OUTPUT_PANEL_CLASS).toContain("min-h-0");
    expect(KANBAN_OUTPUT_PANEL_CLASS).toContain("flex-[1_1_0]");
    expect(KANBAN_OUTPUT_PANEL_CLASS).not.toContain("h-1/2");
    expect(KANBAN_OUTPUT_PANEL_CLASS).not.toContain("mt-3");
    expect(KANBAN_BOARD_CLASS).toContain("min-h-0");
    expect(KANBAN_BOARD_CLASS).toContain("flex-1");
    expect(KANBAN_BOARD_CLASS).not.toContain("100vh");
  });
});

describe("kanbanOutputSelectionStillVisible", () => {
  it("matches historical output visibility to capped result cards", () => {
    const results = Array.from({ length: 21 }, (_, index) =>
      result({
        result_id: `results:${index}-0`,
        entry_id: `${index}-0`,
        task_id: `task-${index}`,
      }));

    expect(kanbanVisibleRecentResults(results)).toHaveLength(20);
    expect(kanbanOutputSelectionStillVisible(
      {
        workerId: "worker-1",
        taskId: "task-19",
        historical: true,
        prefix: null,
        instanceId: "results:19-0",
      },
      { locks: [], recent_results: results },
      { activeLocksDegraded: false, recentResultsDegraded: false },
    )).toBe(true);
    expect(kanbanOutputSelectionStillVisible(
      {
        workerId: "worker-1",
        taskId: "task-20",
        historical: true,
        prefix: null,
        instanceId: "results:20-0",
      },
      { locks: [], recent_results: results },
      { activeLocksDegraded: false, recentResultsDegraded: false },
    )).toBe(false);
  });

  it("preserves historical result output while recent results are degraded", () => {
    expect(kanbanOutputSelectionStillVisible(
      { workerId: "worker-1", taskId: "task-historical", historical: true },
      { locks: [], recent_results: [] },
      { activeLocksDegraded: false, recentResultsDegraded: true },
    )).toBe(true);
  });

  it("preserves live lock output while active locks are degraded", () => {
    expect(kanbanOutputSelectionStillVisible(
      { workerId: "worker-1", taskId: "task-live", historical: false },
      { locks: [], recent_results: [] },
      { activeLocksDegraded: true, recentResultsDegraded: false },
    )).toBe(true);
  });

  it("clears selected output once its healthy source row is absent", () => {
    expect(kanbanOutputSelectionStillVisible(
      { workerId: "worker-1", taskId: "task-live", historical: false },
      { locks: [], recent_results: [result()] },
      { activeLocksDegraded: false, recentResultsDegraded: true },
    )).toBe(false);
    expect(kanbanOutputSelectionStillVisible(
      {
        workerId: "worker-1",
        taskId: "task-live",
        historical: false,
        prefix: null,
        instanceId: "lock:pr:owner/repo:42",
      },
      { locks: [lock()], recent_results: [] },
      { activeLocksDegraded: false, recentResultsDegraded: false },
    )).toBe(true);
  });
});

describe("Kanban", () => {
  it("renders output-capable cards as native collapsed controls", () => {
    const html = renderToStaticMarkup(
      createElement(Kanban, {
        snapshot: snapshot({
          locks: [lock()],
          recent_results: [result()],
        }),
        stuckTasks: [],
      }),
    );

    expect(html).toContain("In Progress");
    expect(html).toContain("Completed");
    expect(html).toContain('role="group"');
    expect(html).toContain('aria-labelledby="kanban-column-in-progress"');
    expect(html).toContain('id="kanban-column-in-progress"');
    expect(html).toContain("<button");
    expect(html).toContain("type=\"button\"");
    expect(html).toContain("focus-visible:ring-inset");
    expect(html).not.toContain("role=\"button\"");
    expect(html).not.toContain("tabindex=\"0\"");
    expect(html).not.toContain("pointer-events-none");
    expect(html).not.toContain("absolute inset-0");
    expect(html).toContain("title=\"PR owner/repo #42\"");
    expect(html).toContain("title=\"worker-1\"");
    expect(html).toContain("aria-expanded=\"false\"");
    expect(html).toContain("aria-describedby=");
    expect(html).toContain("-card-description");
    expect(html).toContain(`aria-label="${KANBAN_VIEW_LABEL}"`);
    expect(html).not.toContain(`<span class="sr-only">${KANBAN_VIEW_LABEL}</span>`);
    expect(html).toContain("View Output");
  });

  it("keeps full active task IDs available when the visible Kanban card is truncated", () => {
    const html = renderToStaticMarkup(
      createElement(Kanban, {
        snapshot: snapshot({
          locks: [lock({ task_id: "task-1234567890abcdef" })],
        }),
        stuckTasks: [],
      }),
    );

    expect(html).toContain("title=\"task-1234567890abcdef\"");
    expect(html).toContain("<span aria-hidden=\"true\">task-1234567...</span>");
    expect(html).toContain("<span class=\"sr-only\">task-1234567890abcdef</span>");
  });

  it("formats prefixed queue and result streams while preserving raw titles", () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-20T00:02:00.000Z"));
    const html = renderToStaticMarkup(
      createElement(Kanban, {
        snapshot: snapshot({
          queue_depths: { "project-a:tasks:issue:grok": 1 },
          consumer_groups: [
            {
              stream: " project-a:tasks:issue:grok ",
              name: "workers",
              consumers: 0,
              pending: 0,
              lag: 1,
            },
          ],
          queued_tasks: [
            {
              ...queuedTask("42"),
              prefix: "bbr-platform",
              entry_id: String(Date.parse("2026-06-20T00:00:00.000Z")) + "-0",
              stream: "project-a:tasks:issue:grok",
            },
          ],
          recent_results: [result({ result_stream: "project-a:results" })],
        }),
        stuckTasks: [],
      }),
    );

    expect(html).toContain(
      "title=\"[bbr-platform] PR owner/repo #42\">[bbr-platform] PR #42</span>",
    );
    expect(html).toContain("border-red-500/40");
    expect(html).toContain("no consumers");
    expect(html).toContain(
      "title=\"project-a:tasks:issue:grok\"><span>[project-a] issue grok</span>",
    );
    expect(html).toContain("<span class=\"sr-only\"> raw stream project-a:tasks:issue:grok</span>");
    expect(html).toContain(">2m ago</span>");
    expect(html).toContain(
      "title=\"project-a:results\"><span>[project-a] results</span>",
    );
    expect(html).toContain("<span class=\"sr-only\"> raw stream project-a:results</span>");
  });

  it("marks queued cards with no consumers when queue depth confirms unknown-lag backlog", () => {
    const html = renderToStaticMarkup(
      createElement(Kanban, {
        snapshot: snapshot({
          queue_depths: { "project-a:tasks:issue:grok": 2 },
          consumer_groups: [
            {
              stream: "project-a:tasks:issue:grok",
              name: "workers",
              consumers: 0,
              pending: 0,
              lag: null,
            },
          ],
          queued_tasks: [
            {
              ...queuedTask("42"),
              stream: "project-a:tasks:issue:grok",
            },
          ],
        }),
        stuckTasks: [],
      }),
    );

    expect(html).toContain("border-red-500/40");
    expect(html).toContain("no consumers");
  });

  it("marks queued cards with no consumers when only the queued preview confirms backlog", () => {
    const html = renderToStaticMarkup(
      createElement(Kanban, {
        snapshot: snapshot({
          consumer_groups: [
            {
              stream: "project-a:tasks:issue:grok",
              name: "workers",
              consumers: 0,
              pending: 0,
              lag: null,
            },
          ],
          queued_tasks: [
            {
              ...queuedTask("42"),
              stream: "project-a:tasks:issue:grok",
            },
          ],
        }),
        stuckTasks: [],
      }),
    );

    expect(html).toContain("border-red-500/40");
    expect(html).toContain("no consumers");
  });

  it("shows no-consumer backlog evidence when queued preview cards are unavailable", () => {
    const row = { stream: "orcest:tasks:codex", depth: 11 };
    expect(kanbanConsumerBacklogRowLabel(row)).toBe(
      "[orcest] codex has no consumers; at least 11 queued/pending items",
    );

    const html = renderToStaticMarkup(
      createElement(Kanban, {
        snapshot: snapshot({
          consumer_groups: [
            {
              stream: "orcest:tasks:codex",
              name: "workers",
              consumers: 0,
              pending: 3,
              lag: 8,
            },
          ],
        }),
        stuckTasks: [],
      }),
    );

    expect(html).toContain("Queued/pending work reported; task preview unavailable");
    expect(html).toContain("[orcest] codex has no consumers; at least 11 queued/pending items");
    expect(html).toContain('title="orcest:tasks:codex"');
    expect(html).not.toContain("No queued or pending work");
  });

  it("does not render active worker pending backlog as empty when previews are unavailable", () => {
    const html = renderToStaticMarkup(
      createElement(Kanban, {
        snapshot: snapshot({
          consumer_groups: [
            {
              stream: "orcest:tasks:codex",
              name: "workers",
              consumers: 1,
              pending: 2,
              lag: null,
            },
          ],
        }),
        stuckTasks: [],
      }),
    );

    expect(html).toContain("Queued / Pending");
    expect(html).toContain("Queued/pending work reported; task preview unavailable");
    expect(html).not.toContain("No queued or pending work");
    expect(html).not.toContain("no consumers");
  });

  it("marks queued resources that appear in multiple provider streams", () => {
    const html = renderToStaticMarkup(
      createElement(Kanban, {
        snapshot: snapshot({
          queue_depths: {
            "orcest:tasks:issue:codex": 1,
            "orcest:tasks:issue:grok": 1,
          },
          consumer_groups: [
            {
              stream: "orcest:tasks:issue:codex",
              name: "workers",
              consumers: 1,
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
          queued_tasks: [
            {
              ...queuedTask("4251"),
              prefix: "bbr-platform",
              repo: "bluebamboollc/bbr-platform",
              resource_type: "issue",
              resource_id: "4251",
              task_id: "task-codex",
              entry_id: "1782736756141-0",
              stream: "orcest:tasks:issue:codex",
            },
            {
              ...queuedTask("4251"),
              prefix: "bbr-platform",
              repo: "bluebamboollc/bbr-platform",
              resource_type: "issue",
              resource_id: "4251",
              task_id: "task-grok",
              entry_id: "1782753303871-0",
              stream: "orcest:tasks:issue:grok",
            },
          ],
        }),
        stuckTasks: [],
      }),
    );

    expect(html).toContain("Showing 1 queued resources from 2 queued entries");
    expect(html.match(/title="This resource is queued as 2 entries in 2 streams"/g)).toHaveLength(1);
    expect(html.match(/>2 entries in 2 streams<\/span>/g)).toHaveLength(1);
    expect(html.match(/>no consumers<\/span>/g)).toHaveLength(1);
    expect(html).toContain('title="No consumers for [orcest] issue grok"');
    expect(html).toContain('title="orcest:tasks:issue:codex, orcest:tasks:issue:grok"><span>2 streams</span>');
    expect(html).toContain(`title="${formatTimestampMs(resultTimestampMs("1782736756141-0"))}"`);
    expect(html).not.toContain(`title="${formatTimestampMs(resultTimestampMs("1782753303871-0"))}"`);
    expect(html).toContain("title=\"[bbr-platform] Issue bluebamboollc/bbr-platform #4251\"");
  });

  it("marks repeated queued entries for one resource in the same stream", () => {
    const html = renderToStaticMarkup(
      createElement(Kanban, {
        snapshot: snapshot({
          queue_depths: {
            "orcest:tasks:codex": 2,
          },
          queued_tasks: [
            {
              ...queuedTask("4259"),
              prefix: "bbr-platform",
              repo: "bluebamboollc/bbr-platform",
              resource_type: "pr",
              resource_id: "4259",
              task_id: "task-first",
              entry_id: "1782867651957-0",
              stream: "orcest:tasks:codex",
            },
            {
              ...queuedTask("4259"),
              prefix: "bbr-platform",
              repo: "bluebamboollc/bbr-platform",
              resource_type: "pr",
              resource_id: "4259",
              task_id: "task-second",
              entry_id: "1782922707261-0",
              stream: "orcest:tasks:codex",
            },
          ],
        }),
        stuckTasks: [],
      }),
    );

    expect(html).toContain("Showing 1 queued resources from 2 queued entries");
    expect(html.match(/title="This resource is queued as 2 entries"/g)).toHaveLength(1);
    expect(html.match(/>2 entries<\/span>/g)).toHaveLength(1);
    expect(html).toContain(
      'title="orcest:tasks:codex"><span>[orcest] codex</span><span class="sr-only"> raw stream orcest:tasks:codex</span>',
    );
    expect(html).not.toContain(">2 streams</span>");
  });

  it("renders mixed task type labels for grouped queued resources", () => {
    const html = renderToStaticMarkup(
      createElement(Kanban, {
        snapshot: snapshot({
          queue_depths: {
            "orcest:tasks:codex": 2,
          },
          queued_tasks: [
            {
              ...queuedTask("4259"),
              prefix: "bbr-platform",
              repo: "bluebamboollc/bbr-platform",
              resource_type: "pr",
              resource_id: "4259",
              task_id: "task-fix",
              task_type: "fix_pr",
              entry_id: "1782867651957-0",
              stream: "orcest:tasks:codex",
            },
            {
              ...queuedTask("4259"),
              prefix: "bbr-platform",
              repo: "bluebamboollc/bbr-platform",
              resource_type: "pr",
              resource_id: "4259",
              task_id: "task-rebase",
              task_type: "rebase_pr",
              entry_id: "1782922707261-0",
              stream: "orcest:tasks:codex",
            },
          ],
        }),
        stuckTasks: [],
      }),
    );

    expect(html).toContain('title="Queued task types: Fix PR, Rebase">Mixed types</span>');
    expect(html).toContain(">2 entries</span>");
  });

  it("shows result timestamps on Kanban result cards", () => {
    const completedEntryId = "1710000000000-0";
    const failedEntryId = "1710000001000-0";
    const neutralEntryId = "1710000002000-0";
    const html = renderToStaticMarkup(
      createElement(Kanban, {
        snapshot: snapshot({
          recent_results: [
            result({ entry_id: completedEntryId, result_id: `results:${completedEntryId}` }),
            result({
              entry_id: failedEntryId,
              result_id: `results:${failedEntryId}`,
              status: "failed",
              duration_seconds: 42,
            }),
            result({
              entry_id: neutralEntryId,
              result_id: `results:${neutralEntryId}`,
              status: "stale",
              duration_seconds: 84,
            }),
          ],
        }),
        stuckTasks: [],
      }),
    );

    expect(html).toContain(`title="entry ${completedEntryId}"`);
    expect(html).toContain(formatTimestampMs(resultTimestampMs(completedEntryId)));
    expect(html).toContain(`title="entry ${failedEntryId}; duration 42s"`);
    expect(html).toContain(`${formatTimestampMs(resultTimestampMs(failedEntryId))} - 42s`);
    expect(html).toContain(`title="entry ${neutralEntryId}; duration 1m 24s"`);
    expect(html).toContain(`${formatTimestampMs(resultTimestampMs(neutralEntryId))} - 1m 24s`);
  });

  it("labels usage-exhausted results as needing attention", () => {
    const html = renderToStaticMarkup(
      createElement(Kanban, {
        snapshot: snapshot({
          recent_results: [
            result({
              result_id: "results:usage",
              entry_id: "1710000002000-0",
              status: "usage_exhausted",
              resource_id: "42",
              summary: "usage cap reached",
            }),
          ],
        }),
        stuckTasks: [],
      }),
    );

    expect(html).toContain("Needs Attention");
    expect(html).toContain("Usage exhausted");
    expect(html).toContain("usage cap reached");
    expect(html).not.toContain("Failed / Blocked");
    expect(html).not.toMatch(/failures?/i);
  });

  it("renders degraded active-lock empty state with an unknown count", () => {
    const html = renderToStaticMarkup(
      createElement(Kanban, {
        snapshot: snapshot({ degraded_sections: ["active locks"] }),
        stuckTasks: [],
      }),
    );

    expect(html).toContain("In Progress");
    expect(html).toContain("?");
    expect(html).toContain("Active work unavailable");
  });

  it("makes capped result columns visible instead of silently hiding cards", () => {
    const results = Array.from({ length: 25 }, (_, index) =>
      result({
        result_id: `results:${index}-0`,
        entry_id: `${index}-0`,
        task_id: `task-${index}`,
        summary: `summary-${index}`,
      }));
    const html = renderToStaticMarkup(
      createElement(Kanban, {
        snapshot: snapshot({ recent_results: results }),
        stuckTasks: [],
      }),
    );

    expect(html).toContain("Showing 20 of 25 results");
    expect(html).toContain("summary-19");
    expect(html).not.toContain("summary-20");
    expect(html).not.toContain("summary-24");
  });

  it("allows long result summaries to wrap inside Kanban cards", () => {
    const html = renderToStaticMarkup(
      createElement(Kanban, {
        snapshot: snapshot({
          recent_results: [
            result({ summary: "x".repeat(120) }),
            result({
              result_id: "results:2-0",
              entry_id: "2-0",
              status: "failed",
              summary: "y".repeat(120),
            }),
            result({
              result_id: "results:3-0",
              entry_id: "3-0",
              status: "stale",
              summary: "z".repeat(120),
            }),
          ],
        }),
        stuckTasks: [],
      }),
    );

    expect(html.match(/line-clamp-2 break-words/g)).toHaveLength(3);
  });

  it("marks capped result columns as loaded results when recent results are degraded", () => {
    const results = Array.from({ length: 21 }, (_, index) =>
      result({
        result_id: `results:${index}-0`,
        entry_id: `${index}-0`,
        task_id: `task-${index}`,
      }));
    const html = renderToStaticMarkup(
      createElement(Kanban, {
        snapshot: snapshot({
          recent_results: results,
          degraded_sections: ["recent_results"],
        }),
        stuckTasks: [],
      }),
    );

    expect(html).toContain("Showing 20 of 21 loaded results");
  });

  it("marks capped result columns as loaded results when result depth is degraded", () => {
    const results = Array.from({ length: 21 }, (_, index) =>
      result({
        result_id: `results:${index}-0`,
        entry_id: `${index}-0`,
        task_id: `task-${index}`,
      }));
    const html = renderToStaticMarkup(
      createElement(Kanban, {
        snapshot: snapshot({
          recent_results: results,
          degraded_sections: ["results depth"],
        }),
        stuckTasks: [],
      }),
    );

    expect(html).toContain("Showing 20 of 21 loaded results");
    expect(html).toContain("Result columns show 21 loaded results; total unavailable");
  });

  it("marks result column counts as loaded when retained results exceed the preview", () => {
    const html = renderToStaticMarkup(
      createElement(Kanban, {
        snapshot: snapshot({
          results_depth: 8,
          recent_results: [
            result({ status: "completed" }),
          ],
        }),
        stuckTasks: [],
      }),
    );

    expect(html).toContain("Completed");
    expect(html).toContain("Result columns show 1 loaded of 8 retained results");
    expect(html).toContain('role="note"');
    expect(html).toContain("1 loaded");
    expect(html).toContain("Needs Attention");
    expect(html).toContain("0 loaded");
    expect(html).toContain("No loaded results need attention");
    expect(html).not.toContain("No results need attention");
  });

  it("renders the live-sized retained-results preview note", () => {
    const results = Array.from({ length: 20 }, (_, index) =>
      result({
        result_id: `results:${index}-0`,
        entry_id: `${index}-0`,
        status: index < 7 ? "completed" : "failed",
      }));
    const html = renderToStaticMarkup(
      createElement(Kanban, {
        snapshot: snapshot({
          results_depth: 2695,
          recent_results: results,
        }),
        stuckTasks: [],
      }),
    );

    expect(html).toContain("Result columns show 20 loaded of 2695 retained results");
    expect(html).toContain("Completed");
    expect(html).toContain("7 loaded");
    expect(html).toContain("Needs Attention");
    expect(html).toContain("13 loaded");
  });

  it("marks queued previews degraded when queue depth aliases use hyphens", () => {
    const html = renderToStaticMarkup(
      createElement(Kanban, {
        snapshot: snapshot({
          queue_depths: { "tasks:claude": 2 },
          queued_tasks: [queuedTask("1")],
          degraded_sections: ["queue-depths"],
        }),
        stuckTasks: [],
      }),
    );

    expect(html).toContain("Queued / Pending");
    expect(html).toContain("Queued/pending preview may be incomplete");
  });

  it("does not label pending-only work as queued task cards", () => {
    const html = renderToStaticMarkup(
      createElement(Kanban, {
        snapshot: snapshot({
          queue_depths: { "tasks:claude": 2 },
          queued_tasks: [],
        }),
        stuckTasks: [],
      }),
    );

    expect(html).toContain("Queued / Pending");
    expect(html).not.toContain("Fix PR");
    expect(html).toContain("Queued/pending work reported; task preview unavailable");
  });

  it("does not claim a queue depth was reported when queue depths are degraded", () => {
    const html = renderToStaticMarkup(
      createElement(Kanban, {
        snapshot: snapshot({
          degraded_sections: ["queue depths"],
        }),
        stuckTasks: [],
      }),
    );

    expect(html).toContain("Queued/pending work unavailable");
    expect(html).not.toContain("Queued/pending depth reported; task preview unavailable");
  });
});
