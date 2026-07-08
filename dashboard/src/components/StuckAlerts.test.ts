import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import type { ConsumerGroupInfo, QueuedTask, StuckTask } from "../lib/types";
import {
  StuckAlerts,
  groupedStuckTasks,
  stuckTaskAffectedResourcesText,
  stuckTaskGroupOccurrenceLabel,
  stuckTaskQueueContextText,
  stuckTaskQueueSummaries,
  stuckTaskQueueSummaryText,
  stuckTaskReasonText,
  stuckTaskSeverityGroupLabel,
  stuckTaskSeverityGroupsLabel,
} from "./StuckAlerts";

function stuckTask(overrides: Partial<StuckTask> = {}): StuckTask {
  return {
    prefix: null,
    resource_type: "pr",
    repo: "owner/repo",
    resource_id: "42",
    reason: "pending task stale",
    severity: "warning",
    ...overrides,
  };
}

function queuedTask(overrides: Partial<QueuedTask> = {}): QueuedTask {
  return {
    entry_id: "1-0",
    task_id: "task-1",
    task_type: "implement_issue",
    repo: "owner/repo",
    resource_type: "issue",
    resource_id: "42",
    created_at: null,
    stream: "orcest:tasks:issue:codex",
    ...overrides,
  };
}

function consumerGroup(overrides: Partial<ConsumerGroupInfo> = {}): ConsumerGroupInfo {
  return {
    stream: "orcest:tasks:issue:codex",
    name: "workers",
    consumers: 0,
    pending: 0,
    lag: 1,
    ...overrides,
  };
}

describe("stuckTaskReasonText", () => {
  it("trims visible reasons and falls back for blank values", () => {
    expect(stuckTaskReasonText(" pending task stale ")).toBe("pending task stale");
    expect(stuckTaskReasonText("   ")).toBe("reason unavailable");
  });
});

describe("stuckTaskSeverityGroupLabel", () => {
  it("names stuck task counts explicitly", () => {
    expect(stuckTaskSeverityGroupLabel(1, "critical")).toBe("1 critical stuck task");
    expect(stuckTaskSeverityGroupLabel(2, "critical")).toBe("2 critical stuck tasks");
    expect(stuckTaskSeverityGroupLabel(2, "critical", 1)).toBe(
      "1 critical stuck resource (2 occurrences)",
    );
    expect(stuckTaskSeverityGroupLabel(3, "warning", 2)).toBe(
      "2 warning stuck resources (3 occurrences)",
    );
    expect(stuckTaskSeverityGroupLabel(1, "warning")).toBe("1 warning stuck task");
  });
});

describe("groupedStuckTasks", () => {
  it("groups repeated resource alerts while preserving separate reasons", () => {
    const first = stuckTask({
      prefix: "bbr-platform",
      resource_type: "issue",
      repo: "bluebamboollc/bbr-platform",
      resource_id: "4251",
      reason: "Worker group has backlog but no consumers (1 lag)",
      severity: "critical",
      stream: "orcest:tasks:issue:codex",
    });
    const second = {
      ...first,
      stream: "orcest:tasks:issue:grok",
    };
    const third = {
      ...first,
      reason: "Attempt count at max (3/3)",
    };

    const groups = groupedStuckTasks([first, second, third]);

    expect(groups).toHaveLength(2);
    expect(groups[0]).toEqual([first, second]);
    expect(groups[1]).toEqual([third]);
    expect(stuckTaskGroupOccurrenceLabel(groups[0])).toBe("2 queues");
    expect(stuckTaskGroupOccurrenceLabel(groups[1])).toBeNull();
  });

  it("aggregates one no-consumer queue blocking multiple resources", () => {
    const first = stuckTask({
      prefix: "bbr-platform",
      resource_type: "issue",
      repo: "bluebamboollc/bbr-platform",
      resource_id: "4251",
      reason: "Worker group has backlog but no consumers (28 lag)",
      severity: "critical",
      stream: "orcest:tasks:issue:codex",
      consumer_group: "workers",
      no_worker_consumers: true,
    });
    const second = {
      ...first,
      resource_id: "4260",
    };
    const third = {
      ...second,
      task_id: "duplicate-resource-task",
    };

    const groups = groupedStuckTasks([first, second, third]);

    expect(groups).toEqual([[first, second, third]]);
    expect(stuckTaskGroupOccurrenceLabel(groups[0])).toBe("2 shown resources");
    expect(stuckTaskAffectedResourcesText(groups[0])).toBe(
      "Shown resources: [bbr-platform] Issue bluebamboollc/bbr-platform #4251, [bbr-platform] Issue bluebamboollc/bbr-platform #4260",
    );
    expect(stuckTaskSeverityGroupsLabel(groups, "critical")).toBe(
      "1 critical stuck queue (2 shown resources)",
    );
    expect(stuckTaskSeverityGroupsLabel(groups, "critical", [{
      stream: "orcest:tasks:issue:codex",
      label: "[orcest] issue codex",
      count: 3,
      noWorkerConsumers: 3,
      backlogCount: 28,
      backlogExact: true,
    }])).toBe(
      "1 critical stuck queue (28 queued/pending; 2 shown resources)",
    );
    expect(stuckTaskSeverityGroupsLabel(groups, "critical", [{
      stream: "orcest:tasks:issue:codex",
      label: "[orcest] issue codex",
      count: 3,
      noWorkerConsumers: 3,
      backlogCount: 28,
      backlogExact: false,
    }])).toBe(
      "1 critical stuck queue (at least 28 queued/pending; 2 shown resources)",
    );
  });

  it("uses structured no-consumer fields instead of reason text for queue grouping", () => {
    const first = stuckTask({
      prefix: "bbr-platform",
      resource_type: "issue",
      repo: "bluebamboollc/bbr-platform",
      resource_id: "4251",
      reason: "Workers unavailable for retained backlog",
      severity: "critical",
      stream: "orcest:tasks:issue:codex",
      consumer_group: "workers",
      no_worker_consumers: true,
    });
    const second = {
      ...first,
      resource_id: "4260",
      reason: "No consumers are attached to this worker group",
    };

    const groups = groupedStuckTasks([first, second]);

    expect(groups).toEqual([[first, second]]);
    expect(stuckTaskSeverityGroupsLabel(groups, "critical")).toBe(
      "1 critical stuck queue (2 shown resources)",
    );
  });
});

describe("stuckTaskQueueContextText", () => {
  it("names the matching queued stream and no-consumer state", () => {
    expect(stuckTaskQueueContextText(
      stuckTask({
        prefix: "bbr-platform",
        repo: "bluebamboollc/bbr-platform",
        resource_type: "issue",
        resource_id: "4251",
      }),
      [queuedTask({
        repo: "bluebamboollc/bbr-platform",
        resource_type: "issue",
        resource_id: "4251",
      })],
      [consumerGroup()],
    )).toBe("Queued in [orcest] issue codex with no worker consumers");
  });

  it("uses queued previews to mark inferred queue context with no consumers", () => {
    expect(stuckTaskQueueContextText(
      stuckTask({
        repo: "owner/repo",
        resource_type: "issue",
        resource_id: "42",
      }),
      [queuedTask()],
      [consumerGroup({ lag: null })],
    )).toBe("Queued in [orcest] issue codex with no worker consumers");
  });

  it("uses queue depths to mark direct stream context with no consumers", () => {
    expect(stuckTaskQueueContextText(
      stuckTask({
        stream: "orcest:tasks:issue:codex",
        entry_id: "1718496000000-0",
        task_id: "task-4251",
      }),
      [],
      [consumerGroup({ lag: null })],
      { "orcest:tasks:issue:codex": 2 },
    )).toBe(
      "Queued in [orcest] issue codex with no worker consumers; entry 1718496000000-0; task task-4251",
    );
  });

  it("uses queue depths to mark inferred queue context with no consumers", () => {
    expect(stuckTaskQueueContextText(
      stuckTask({
        repo: "owner/repo",
        resource_type: "issue",
        resource_id: "42",
      }),
      [queuedTask()],
      [consumerGroup({ lag: null })],
      { "orcest:tasks:issue:codex": 2 },
    )).toBe("Queued in [orcest] issue codex with no worker consumers");
  });

  it("normalizes consumer group stream keys before matching queued preview context", () => {
    expect(stuckTaskQueueContextText(
      stuckTask({
        repo: "owner/repo",
        resource_type: "issue",
        resource_id: "42",
      }),
      [queuedTask()],
      [consumerGroup({ stream: " orcest:tasks:issue:codex ", lag: null })],
    )).toBe("Queued in [orcest] issue codex with no worker consumers");
  });

  it("prefers explicit stuck stream context when present", () => {
    expect(stuckTaskQueueContextText(
      stuckTask({
        stream: "orcest:tasks:issue:codex",
        entry_id: "1718496000000-0",
        task_id: "task-4251",
        consumer_group: "workers",
        no_worker_consumers: true,
      }),
      [queuedTask({ stream: "orcest:tasks:issue:clauder" })],
      [],
    )).toBe(
      "Queued in [orcest] issue codex with no worker consumers; entry 1718496000000-0; task task-4251",
    );
  });

  it("does not invent queue context for a different resource", () => {
    expect(stuckTaskQueueContextText(
      stuckTask({ resource_type: "issue", resource_id: "99" }),
      [queuedTask({ resource_id: "42" })],
      [consumerGroup()],
    )).toBeNull();
  });

  it("does not borrow queue context from another known Redis prefix", () => {
    expect(stuckTaskQueueContextText(
      stuckTask({
        prefix: "project-a",
        repo: "owner/repo",
        resource_type: "issue",
        resource_id: "42",
      }),
      [queuedTask({
        prefix: "project-b",
        repo: "owner/repo",
        resource_type: "issue",
        resource_id: "42",
      })],
      [consumerGroup()],
    )).toBeNull();
    expect(stuckTaskQueueContextText(
      stuckTask({
        prefix: null,
        repo: "owner/repo",
        resource_type: "issue",
        resource_id: "42",
      }),
      [queuedTask({
        prefix: "project-b",
        repo: "owner/repo",
        resource_type: "issue",
        resource_id: "42",
      })],
      [consumerGroup()],
    )).toBeNull();
  });

  it("allows legacy queued previews without a prefix to explain prefixed stuck tasks", () => {
    expect(stuckTaskQueueContextText(
      stuckTask({
        prefix: "project-a",
        repo: "owner/repo",
        resource_type: "issue",
        resource_id: "42",
      }),
      [queuedTask({
        prefix: null,
        repo: "owner/repo",
        resource_type: "issue",
        resource_id: "42",
      })],
      [consumerGroup()],
    )).toBe("Queued in [orcest] issue codex with no worker consumers");
  });
});

describe("stuckTaskQueueSummaries", () => {
  it("summarizes affected streams by stuck-task count", () => {
    const summaries = stuckTaskQueueSummaries([
      stuckTask({
        stream: "orcest:tasks:issue:codex",
        consumer_group: "workers",
        no_worker_consumers: true,
      }),
      stuckTask({
        stream: "orcest:tasks:codex",
        consumer_group: "workers",
        no_worker_consumers: true,
      }),
      stuckTask({
        stream: "orcest:tasks:codex",
        consumer_group: "workers",
        no_worker_consumers: true,
      }),
      stuckTask({
        stream: "orcest:tasks:grok",
      }),
      stuckTask({
        stream: " ",
      }),
    ]);

    expect(summaries).toEqual([
      {
        stream: "orcest:tasks:codex",
        label: "[orcest] codex",
        count: 2,
        noWorkerConsumers: 2,
        backlogCount: 0,
        backlogExact: true,
      },
      {
        stream: "orcest:tasks:grok",
        label: "[orcest] grok",
        count: 1,
        noWorkerConsumers: 0,
        backlogCount: 0,
        backlogExact: true,
      },
      {
        stream: "orcest:tasks:issue:codex",
        label: "[orcest] issue codex",
        count: 1,
        noWorkerConsumers: 1,
        backlogCount: 0,
        backlogExact: true,
      },
    ]);
    expect(stuckTaskQueueSummaryText(summaries[0])).toBe("[orcest] codex: 2 tasks, no consumers");
    expect(stuckTaskQueueSummaryText(summaries[1])).toBe("[orcest] grok: 1 task");
  });

  it("uses queue evidence when preview rows understate backlog", () => {
    const summaries = stuckTaskQueueSummaries(
      [
        stuckTask({
          stream: "orcest:tasks:issue:codex",
          consumer_group: "workers",
          no_worker_consumers: true,
        }),
      ],
      [
        {
          stream: "orcest:tasks:issue:codex",
          name: "workers",
          consumers: 0,
          pending: 0,
          lag: null,
        },
      ],
      {
        "orcest:tasks:issue:codex": 28,
      },
    );

    expect(summaries[0]).toMatchObject({
      stream: "orcest:tasks:issue:codex",
      count: 1,
      noWorkerConsumers: 1,
      backlogCount: 28,
      backlogExact: true,
    });
    expect(stuckTaskQueueSummaryText(summaries[0]))
      .toBe("[orcest] issue codex: 28 queued/pending, 1 task shown, no consumers");
  });

  it("uses lower-bound backlog text for queued preview evidence", () => {
    const summaries = stuckTaskQueueSummaries(
      [
        stuckTask({
          stream: "orcest:tasks:issue:grok",
          consumer_group: "workers",
          no_worker_consumers: true,
        }),
      ],
      [
        {
          stream: "orcest:tasks:issue:grok",
          name: "workers",
          consumers: 0,
          pending: 2,
          lag: null,
        },
      ],
      {},
      [queuedTask({ stream: "orcest:tasks:issue:grok" })],
    );

    expect(stuckTaskQueueSummaryText(summaries[0]))
      .toBe("[orcest] issue grok: at least 3 queued/pending, 1 task shown, no consumers");
  });
});

describe("StuckAlerts", () => {
  it("renders degraded detection state when no stuck tasks are available", () => {
    const html = renderToStaticMarkup(
      createElement(StuckAlerts, { stuckTasks: [], degraded: true }),
    );

    expect(html).toContain("Stuck task detection is partially unavailable");
    expect(html).toContain('role="alert"');
  });

  it("groups critical and warning alerts and renders blank reasons visibly", () => {
    const html = renderToStaticMarkup(
      createElement(StuckAlerts, {
        stuckTasks: [
          stuckTask({
            reason: " attempts exhausted ",
            severity: "critical",
          }),
          stuckTask({
            resource_id: "43",
            reason: "   ",
          }),
        ],
        degraded: true,
      }),
    );

    expect(html).toContain("Stuck task detection is partially unavailable");
    expect(html).toContain("1 critical stuck task");
    expect(html).toContain("1 warning stuck task");
    expect(html).toContain("PR owner/repo #42: attempts exhausted");
    expect(html).toContain("PR owner/repo #43: reason unavailable");
    expect(html.match(/role="alert"/g)).toHaveLength(2);
    expect(html).toContain('role="status"');
  });

  it("renders matching queue context under stuck resources", () => {
    const html = renderToStaticMarkup(
      createElement(StuckAlerts, {
        stuckTasks: [
          stuckTask({
            prefix: "bbr-platform",
            repo: "bluebamboollc/bbr-platform",
            resource_type: "issue",
            resource_id: "4251",
            reason: "Queued but no worker has picked it up",
          }),
        ],
        queuedTasks: [
          queuedTask({
            repo: "bluebamboollc/bbr-platform",
            resource_type: "issue",
            resource_id: "4251",
          }),
        ],
        consumerGroups: [consumerGroup()],
      }),
    );

    expect(html).toContain(
      "[bbr-platform] Issue bluebamboollc/bbr-platform #4251: Queued but no worker has picked it up",
    );
    expect(html).toContain("Queued in [orcest] issue codex with no worker consumers");
  });

  it("renders queue-depth no-consumer context under direct stream stuck resources", () => {
    const html = renderToStaticMarkup(
      createElement(StuckAlerts, {
        stuckTasks: [
          stuckTask({
            stream: "orcest:tasks:issue:codex",
            reason: "Queued but no worker has picked it up",
            severity: "critical",
            entry_id: "1718496000000-0",
            task_id: "task-4251",
          }),
        ],
        consumerGroups: [consumerGroup({ lag: null })],
        queueDepths: { "orcest:tasks:issue:codex": 2 },
      }),
    );

    expect(html).toContain(
      "Queued in [orcest] issue codex with no worker consumers; entry 1718496000000-0; task task-4251",
    );
  });

  it("groups duplicate resource alerts and lists each blocked queue", () => {
    const html = renderToStaticMarkup(
      createElement(StuckAlerts, {
        stuckTasks: [
          stuckTask({
            prefix: "bbr-platform",
            repo: "bluebamboollc/bbr-platform",
            resource_type: "issue",
            resource_id: "4251",
            reason: "Worker group has backlog but no consumers (1 lag)",
            severity: "critical",
            stream: "orcest:tasks:issue:codex",
            consumer_group: "workers",
            entry_id: "1782736756141-0",
            task_id: "task-codex",
            no_worker_consumers: true,
          }),
          stuckTask({
            prefix: "bbr-platform",
            repo: "bluebamboollc/bbr-platform",
            resource_type: "issue",
            resource_id: "4251",
            reason: "Worker group has backlog but no consumers (1 lag)",
            severity: "critical",
            stream: "orcest:tasks:issue:grok",
            consumer_group: "workers",
            entry_id: "1782753303871-0",
            task_id: "task-grok",
            no_worker_consumers: true,
          }),
        ],
      }),
    );

    expect(html).toContain("1 critical stuck resource (2 occurrences)");
    expect(html).not.toContain("2 critical stuck tasks");
    expect(html.match(/\[bbr-platform\] Issue bluebamboollc\/bbr-platform #4251:/g))
      .toHaveLength(1);
    expect(html).toContain("2 queues");
    expect(html).toContain("Queues:");
    expect(html).toContain("[orcest] issue codex: 1 task, no consumers");
    expect(html).toContain("[orcest] issue grok: 1 task, no consumers");
    expect(html).toContain("Queued in [orcest] issue codex with no worker consumers; entry 1782736756141-0; task task-codex");
    expect(html).toContain("Queued in [orcest] issue grok with no worker consumers; entry 1782753303871-0; task task-grok");
  });

  it("renders queue-level no-consumer backlog groups with affected resources", () => {
    const html = renderToStaticMarkup(
      createElement(StuckAlerts, {
        stuckTasks: [
          stuckTask({
            prefix: "bbr-platform",
            repo: "bluebamboollc/bbr-platform",
            resource_type: "issue",
            resource_id: "4251",
            reason: "Worker group has backlog but no consumers (28 lag)",
            severity: "critical",
            stream: "orcest:tasks:issue:codex",
            consumer_group: "workers",
            entry_id: "1782736756141-0",
            task_id: "task-4251",
            no_worker_consumers: true,
          }),
          stuckTask({
            prefix: "bbr-platform",
            repo: "bluebamboollc/bbr-platform",
            resource_type: "issue",
            resource_id: "4260",
            reason: "Worker group has backlog but no consumers (28 lag)",
            severity: "critical",
            stream: "orcest:tasks:issue:codex",
            consumer_group: "workers",
            entry_id: "1782736756142-0",
            task_id: "task-4260",
            no_worker_consumers: true,
          }),
          stuckTask({
            prefix: "bbr-platform",
            repo: "bluebamboollc/bbr-platform",
            resource_type: "issue",
            resource_id: "4260",
            reason: "Worker group has backlog but no consumers (28 lag)",
            severity: "critical",
            stream: "orcest:tasks:issue:codex",
            consumer_group: "workers",
            entry_id: "1782736756143-0",
            task_id: "task-4260-duplicate",
            no_worker_consumers: true,
          }),
        ],
        consumerGroups: [
          consumerGroup({
            stream: "orcest:tasks:issue:codex",
            pending: 0,
            lag: 28,
          }),
        ],
      }),
    );

    expect(html).toContain("1 critical stuck queue (28 queued/pending; 2 shown resources)");
    expect(html).toContain(
      "Queue [orcest] issue codex: Worker group has backlog but no consumers (28 lag)",
    );
    expect(html).toContain(
      "Shown resources: [bbr-platform] Issue bluebamboollc/bbr-platform #4251, [bbr-platform] Issue bluebamboollc/bbr-platform #4260",
    );
    expect(html).toContain("[orcest] issue codex: 28 queued/pending, 3 tasks shown, no consumers");
    expect(html.match(/Queue \[orcest\] issue codex:/g) ?? []).toHaveLength(1);
  });

  it("renders queue summaries with larger queue-depth evidence", () => {
    const html = renderToStaticMarkup(
      createElement(StuckAlerts, {
        stuckTasks: [
          stuckTask({
            prefix: "bbr-platform",
            repo: "bluebamboollc/bbr-platform",
            resource_type: "issue",
            resource_id: "4251",
            reason: "Worker group has backlog but no consumers (lag unknown, 28 queued)",
            severity: "critical",
            stream: "orcest:tasks:issue:codex",
            consumer_group: "workers",
            entry_id: "1782736756141-0",
            task_id: "task-4251",
            no_worker_consumers: true,
          }),
        ],
        consumerGroups: [{
          stream: "orcest:tasks:issue:codex",
          name: "workers",
          consumers: 0,
          pending: 0,
          lag: null,
        }],
        queueDepths: {
          "orcest:tasks:issue:codex": 28,
        },
      }),
    );

    expect(html).toContain("[orcest] issue codex: 28 queued/pending, 1 task shown, no consumers");
    expect(html).toContain("1 critical stuck queue (28 queued/pending; 1 shown resource)");
    expect(html).not.toContain("1 critical stuck task");
    expect(html).not.toContain("[orcest] issue codex: 1 task, no consumers");
  });

  it("renders nothing when detection is healthy and there are no stuck tasks", () => {
    const html = renderToStaticMarkup(
      createElement(StuckAlerts, { stuckTasks: [] }),
    );

    expect(html).toBe("");
  });
});
