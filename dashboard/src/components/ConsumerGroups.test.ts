import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import type { ConsumerGroupInfo } from "../lib/types";
import {
  CONSUMER_GROUP_BACKLOG_CELL_CLASS,
  CONSUMER_GROUP_COUNT_CELL_CLASS,
  CONSUMER_GROUP_LAG_CELL_CLASS,
  ConsumerGroups,
  consumerGroupBacklogCount,
  consumerGroupBacklogText,
  consumerGroupBacklogTitle,
  consumerGroupBacklogTone,
} from "./ConsumerGroups";

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

describe("ConsumerGroups", () => {
  it("summarizes visible backlog evidence for worker consumer groups", () => {
    const queueDepthWorker = group("orcest:tasks:issue:codex", "workers", {
      consumers: 0,
      pending: 0,
      lag: null,
    });
    const queuedPreviewWorker = group("orcest:tasks:issue:grok", "workers", {
      consumers: 0,
      pending: 0,
      lag: null,
    });
    const pendingAndPreviewWorker = group("orcest:tasks:issue:grok", "workers", {
      consumers: 0,
      pending: 2,
      lag: null,
    });
    const idleWorker = group("orcest:tasks:codex", "workers", {
      consumers: 0,
      pending: 0,
      lag: 0,
    });
    const auxiliaryGroup = group("orcest:tasks:codex", "debug-inspector", {
      pending: 3,
      lag: 5,
    });

    expect(consumerGroupBacklogCount(queueDepthWorker, 2, 0)).toBe(2);
    expect(consumerGroupBacklogText(queueDepthWorker, 2, 0)).toBe("2");
    expect(consumerGroupBacklogTitle(queueDepthWorker, 2, 0))
      .toBe("2 queued/pending; pending 0, lag unknown, queue depth 2");
    expect(consumerGroupBacklogTone(queueDepthWorker, 2, 0)).toBe("text-red-300");

    expect(consumerGroupBacklogCount(queuedPreviewWorker, 0, 1)).toBe(1);
    expect(consumerGroupBacklogText(queuedPreviewWorker, 0, 1)).toBe("1");
    expect(consumerGroupBacklogTitle(queuedPreviewWorker, 0, 1))
      .toBe("at least 1 queued/pending; pending 0, lag unknown, 1 queued preview");
    expect(consumerGroupBacklogTone(queuedPreviewWorker, 0, 1)).toBe("text-red-300");

    expect(consumerGroupBacklogCount(pendingAndPreviewWorker, 0, 1)).toBe(3);
    expect(consumerGroupBacklogTitle(pendingAndPreviewWorker, 0, 1))
      .toBe("at least 3 queued/pending; pending 2, lag unknown, 1 queued preview");

    expect(consumerGroupBacklogText(idleWorker)).toBe("0");
    expect(consumerGroupBacklogTitle(idleWorker))
      .toBe("no confirmed backlog; pending 0, lag 0");
    expect(consumerGroupBacklogTone(idleWorker)).toBe("text-zinc-400");

    expect(consumerGroupBacklogText(auxiliaryGroup)).toBe("-");
    expect(consumerGroupBacklogTitle(auxiliaryGroup)).toBe("auxiliary group");
    expect(consumerGroupBacklogTone(auxiliaryGroup)).toBe("text-zinc-600");
  });

  it("renders healthy and degraded empty states distinctly", () => {
    const healthyHtml = renderToStaticMarkup(
      createElement(ConsumerGroups, { groups: [] }),
    );
    const degradedHtml = renderToStaticMarkup(
      createElement(ConsumerGroups, { groups: [], degraded: true }),
    );

    expect(healthyHtml).toContain("Consumer Groups (0)");
    expect(healthyHtml).toContain("No consumer groups");
    expect(degradedHtml).toContain("Consumer Groups (?)");
    expect(degradedHtml).toContain("Consumer groups unavailable");
  });

  it("renders sorted rows with worker roles and count tones", () => {
    const html = renderToStaticMarkup(
      createElement(ConsumerGroups, {
        groups: [
          group("tasks:grok", "debug-inspector", { pending: 7, lag: 9 }),
          group("project-a:tasks:issue:claude", "debug-inspector", { pending: 5, lag: 6 }),
          group("project-a:tasks:issue:claude", "workers", { pending: 2, lag: null }),
        ],
      }),
    );

    expect(html).toContain("Consumer Groups (3)");
    expect(html).toContain("1 worker group needs attention");
    expect(html).toContain("aria-label=\"Consumer groups\"");
    expect(html).toContain("<th scope=\"col\" class=\"pb-2 pr-4\">Stream</th>");
    expect(html).toContain("<th scope=\"col\" class=\"pb-2 pr-4\">Status</th>");
    expect(html).toContain(">Backlog</th>");
    expect(html).toContain('title="project-a:tasks:issue:claude"><span>[project-a] issue claude</span>');
    expect(html).toContain("<span class=\"sr-only\"> raw stream project-a:tasks:issue:claude</span>");
    expect(html).toContain("title=\"workers on project-a:tasks:issue:claude\"");
    expect(html).toContain("<span class=\"sr-only\"> on project-a:tasks:issue:claude</span>");
    expect(html.indexOf("[project-a] issue claude")).toBeLessThan(html.indexOf("grok"));
    expect(html.indexOf("workers")).toBeLessThan(html.indexOf("debug-inspector"));
    expect(html).toContain("text-blue-300\">worker</span>");
    expect(html).toContain("text-zinc-500\">aux</span>");
    expect(html).toContain("text-yellow-300\">pending</span>");
    expect(html).toContain("text-yellow-400\">2</span>");
    expect(html).toContain("text-yellow-300\">?</span>");
    expect(html).toContain("text-zinc-500\">5</span>");
  });

  it("moves worker groups that need attention before healthy groups", () => {
    const html = renderToStaticMarkup(
      createElement(ConsumerGroups, {
        groups: [
          group("tasks:aaa", "workers"),
          group("tasks:grok", "debug-inspector", { pending: 7, lag: 9 }),
          group("tasks:zzz", "workers", { lag: 3 }),
          group("tasks:claude", "workers", { pending: 2 }),
        ],
      }),
    );

    expect(html).toContain("2 worker groups need attention");
    expect(html.indexOf("tasks:claude")).toBeLessThan(html.indexOf("tasks:zzz"));
    expect(html.indexOf("tasks:zzz")).toBeLessThan(html.indexOf("tasks:aaa"));
    expect(html.indexOf("tasks:aaa")).toBeLessThan(html.indexOf("tasks:grok"));
  });

  it("calls out worker groups with lag but no consumers", () => {
    const html = renderToStaticMarkup(
      createElement(ConsumerGroups, {
        groups: [
          group("orcest:tasks:issue:codex", "workers", {
            consumers: 0,
            lag: 1,
          }),
        ],
      }),
    );

    expect(html).toContain("1 worker group has no consumers (1 queued/pending)");
    expect(html).toContain("text-red-300\">1 worker group has no consumers (1 queued/pending)</span>");
    expect(html).toContain("text-red-300\">no consumers</span>");
    expect(html).toContain(
      `class="${CONSUMER_GROUP_COUNT_CELL_CLASS}" title="0 consumers, worker group has unhandled work"><span class="text-red-300">0</span></td>`,
    );
    expect(html).toContain(
      `class="${CONSUMER_GROUP_BACKLOG_CELL_CLASS}" title="1 queued/pending; pending 0, lag 1"><span class="text-red-300">1</span></td>`,
    );
    expect(html).toContain(
      `class="${CONSUMER_GROUP_LAG_CELL_CLASS}" title="1"><span class="text-yellow-400">1</span></td>`,
    );
  });

  it("uses queue depth as backlog evidence when worker lag is unavailable", () => {
    const html = renderToStaticMarkup(
      createElement(ConsumerGroups, {
        groups: [
          group("orcest:tasks:issue:codex", "workers", {
            consumers: 0,
            pending: 0,
            lag: null,
          }),
        ],
        queueDepths: {
          "orcest:tasks:issue:codex": 2,
        },
      }),
    );

    expect(html).toContain("1 worker group has no consumers (2 queued/pending)");
    expect(html).toContain("text-red-300\">1 worker group has no consumers (2 queued/pending)</span>");
    expect(html).toContain("text-red-300\">no consumers</span>");
    expect(html).not.toContain("unknown lag</span>");
    expect(html).toContain(
      `class="${CONSUMER_GROUP_COUNT_CELL_CLASS}" title="0 consumers, worker group has unhandled work"><span class="text-red-300">0</span></td>`,
    );
    expect(html).toContain(
      `class="${CONSUMER_GROUP_BACKLOG_CELL_CLASS}" title="2 queued/pending; pending 0, lag unknown, queue depth 2"><span class="text-red-300">2</span></td>`,
    );
    expect(html).toContain(
      `class="${CONSUMER_GROUP_LAG_CELL_CLASS}" title="unknown lag"><span class="text-yellow-300">?</span></td>`,
    );
  });

  it("normalizes stream keys before joining consumer groups to queue depth", () => {
    const html = renderToStaticMarkup(
      createElement(ConsumerGroups, {
        groups: [
          group(" orcest:tasks:issue:codex ", "workers", {
            consumers: 0,
            pending: 0,
            lag: null,
          }),
        ],
        queueDepths: {
          "orcest:tasks:issue:codex": 2,
        },
      }),
    );

    expect(html).toContain("1 worker group has no consumers (2 queued/pending)");
    expect(html).toContain("text-red-300\">no consumers</span>");
    expect(html).toContain(
      `class="${CONSUMER_GROUP_COUNT_CELL_CLASS}" title="0 consumers, worker group has unhandled work"><span class="text-red-300">0</span></td>`,
    );
    expect(html).toContain(
      `class="${CONSUMER_GROUP_BACKLOG_CELL_CLASS}" title="2 queued/pending; pending 0, lag unknown, queue depth 2"><span class="text-red-300">2</span></td>`,
    );
  });

  it("uses queued previews as backlog evidence when lag and depth are unavailable", () => {
    const html = renderToStaticMarkup(
      createElement(ConsumerGroups, {
        groups: [
          group("orcest:tasks:issue:grok", "workers", {
            consumers: 0,
            pending: 0,
            lag: null,
          }),
        ],
        queuedTasks: [
          {
            entry_id: "1-0",
            task_id: "task-1",
            task_type: "implement_issue",
            repo: "owner/repo",
            resource_type: "issue",
            resource_id: "42",
            created_at: null,
            stream: "orcest:tasks:issue:grok",
          },
        ],
      }),
    );

    expect(html).toContain("1 worker group has no consumers (at least 1 queued/pending)");
    expect(html).toContain("text-red-300\">no consumers</span>");
    expect(html).toContain(
      `class="${CONSUMER_GROUP_COUNT_CELL_CLASS}" title="0 consumers, worker group has unhandled work"><span class="text-red-300">0</span></td>`,
    );
    expect(html).toContain(
      `class="${CONSUMER_GROUP_BACKLOG_CELL_CLASS}" title="at least 1 queued/pending; pending 0, lag unknown, 1 queued preview"><span class="text-red-300">1</span></td>`,
    );
  });

  it("labels zero-consumer worker groups without backlog as idle", () => {
    const html = renderToStaticMarkup(
      createElement(ConsumerGroups, {
        groups: [
          group("orcest:tasks:codex", "workers", {
            consumers: 0,
            pending: 0,
            lag: 0,
          }),
        ],
      }),
    );

    expect(html).not.toContain("needs attention");
    expect(html).toContain("text-zinc-400\">idle</span>");
    expect(html).toContain(
      `class="${CONSUMER_GROUP_COUNT_CELL_CLASS}" title="0"><span class="text-zinc-500">0</span></td>`,
    );
    expect(html).toContain(
      `class="${CONSUMER_GROUP_BACKLOG_CELL_CLASS}" title="no confirmed backlog; pending 0, lag 0"><span class="text-zinc-400">0</span></td>`,
    );
  });

  it("summarizes multiple live no-consumer worker groups before generic attention", () => {
    const html = renderToStaticMarkup(
      createElement(ConsumerGroups, {
        groups: [
          group("orcest:tasks:issue:codex", "workers", {
            consumers: 0,
            lag: 1,
          }),
          group("orcest:tasks:issue:grok", "workers", {
            consumers: 0,
            lag: 1,
          }),
          group("orcest:tasks:issue:claude", "workers", {
            pending: 1,
          }),
        ],
      }),
    );

    expect(html).toContain(
      "2 worker groups have no consumers (2 queued/pending); 1 more needs attention",
    );
    expect(html).toContain(
      "text-red-300\">2 worker groups have no consumers (2 queued/pending); 1 more needs attention</span>",
    );
  });

  it("keeps large counters from widening the consumer group table", () => {
    const html = renderToStaticMarkup(
      createElement(ConsumerGroups, {
        groups: [
          group("project-a:tasks:issue:claude", "workers", {
            consumers: 123456789012345,
            pending: 987654321098765,
            lag: 555555555555555,
          }),
        ],
      }),
    );

    expect(CONSUMER_GROUP_COUNT_CELL_CLASS).toContain("break-all");
    expect(CONSUMER_GROUP_BACKLOG_CELL_CLASS).toContain("break-all");
    expect(CONSUMER_GROUP_LAG_CELL_CLASS).toContain("break-all");
    expect(html).toContain(
      `class="${CONSUMER_GROUP_COUNT_CELL_CLASS}" title="123456789012345"><span class="text-zinc-400">123456789012345</span></td>`,
    );
    expect(html).toContain(
      `class="${CONSUMER_GROUP_COUNT_CELL_CLASS}" title="987654321098765"><span`,
    );
    expect(html).toContain(
      `class="${CONSUMER_GROUP_LAG_CELL_CLASS}" title="555555555555555"><span`,
    );
  });
});
