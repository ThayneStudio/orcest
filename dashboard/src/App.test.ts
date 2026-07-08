import { describe, expect, it } from "vitest";

import {
  dashboardTabControls,
  dashboardTabId,
  dashboardTabPanelClassName,
  dashboardTabPanelId,
  dashboardLoadingMessage,
  deadLetterTabBadgeSrText,
  deadLetterTabBadgeText,
  isRecentResultFilter,
  nextDashboardTab,
  overviewStuckCounts,
  overviewStuckBadgeSrText,
  overviewStuckBadgeText,
  overviewStuckBadgeTone,
  redisDisconnectedDetail,
  resultsTabBadgeSrText,
  resultsTabBadgeText,
  resultsTabAttentionCount,
  sanitizeDashboardUrl,
} from "./App";
import type { StuckTask } from "./lib/types";

describe("dashboard tabs", () => {
  it("generates stable tab and panel ids", () => {
    expect(dashboardTabId("overview")).toBe("dashboard-tab-overview");
    expect(dashboardTabPanelId("dead-letters")).toBe("dashboard-panel-dead-letters");
  });

  it("exposes stable aria-controls for every tab panel", () => {
    expect(dashboardTabControls("overview")).toBe("dashboard-panel-overview");
    expect(dashboardTabControls("results")).toBe("dashboard-panel-results");
  });

  it("lets the Kanban tab fill remaining content height", () => {
    expect(dashboardTabPanelClassName("kanban", true)).toBe(
      "flex min-h-0 flex-1 flex-col gap-6",
    );
    expect(dashboardTabPanelClassName("kanban", true)).not.toContain("space-y");
    expect(dashboardTabPanelClassName("overview", true)).toBe("space-y-6");
    expect(dashboardTabPanelClassName("kanban", false)).toBe("");
  });

  it("moves between tabs with arrow, home, and end keys", () => {
    expect(nextDashboardTab("overview", "ArrowRight")).toBe("kanban");
    expect(nextDashboardTab("overview", "ArrowLeft")).toBe("dead-letters");
    expect(nextDashboardTab("results", "ArrowDown")).toBeNull();
    expect(nextDashboardTab("results", "ArrowUp")).toBeNull();
    expect(nextDashboardTab("results", "Home")).toBe("overview");
    expect(nextDashboardTab("results", "End")).toBe("dead-letters");
    expect(nextDashboardTab("results", "Tab")).toBeNull();
  });
});

describe("deadLetterTabBadgeText", () => {
  it("shows unknown counts when dead-letter depth is degraded", () => {
    expect(deadLetterTabBadgeText(0, false)).toBeNull();
    expect(deadLetterTabBadgeText(3, false)).toBe("3");
    expect(deadLetterTabBadgeText(0, true)).toBe("?");
    expect(deadLetterTabBadgeText(0, true, 2)).toBe("2+");
  });

  it("names exact and degraded badge counts for assistive technology", () => {
    expect(deadLetterTabBadgeSrText(1, false)).toBe("dead-letter entry");
    expect(deadLetterTabBadgeSrText(3, false)).toBe("dead-letter entries");
    expect(deadLetterTabBadgeSrText(0, true)).toBe("unknown dead-letter count");
    expect(deadLetterTabBadgeSrText(0, true, 2)).toBe(
      "at least 2 dead-letter entries, total unknown",
    );
  });
});

describe("results tab badge", () => {
  it("recognizes URL-safe recent result filters", () => {
    expect(isRecentResultFilter("all")).toBe(true);
    expect(isRecentResultFilter("failed")).toBe(true);
    expect(isRecentResultFilter("completed")).toBe(true);
    expect(isRecentResultFilter("neutral")).toBe(true);
    expect(isRecentResultFilter("other")).toBe(false);
    expect(isRecentResultFilter(null)).toBe(false);
  });

  it("counts recent results that need attention", () => {
    expect(resultsTabAttentionCount([
      { status: "completed" },
      { status: "failed" },
      { status: "blocked" },
      { status: "usage_exhausted" },
      { status: "stale" },
    ])).toBe(3);
  });

  it("shows attention counts or unknown degraded state", () => {
    expect(resultsTabBadgeText(0, false)).toBeNull();
    expect(resultsTabBadgeText(2, false)).toBe("2");
    expect(resultsTabBadgeText(2, true)).toBe("2+");
    expect(resultsTabBadgeText(0, true)).toBe("?");
    expect(resultsTabBadgeText(0, false, true)).toBe("?");
    expect(resultsTabBadgeText(2, false, true)).toBe("2+");
  });

  it("names visible and unknown result badges without repeating the count", () => {
    expect(resultsTabBadgeSrText(1, false)).toBe("recent result needs attention");
    expect(resultsTabBadgeSrText(2, false)).toBe("recent results need attention");
    expect(resultsTabBadgeSrText(2, true)).toBe("loaded recent results need attention");
    expect(resultsTabBadgeSrText(2, false, true)).toBe("loaded recent results need attention");
    expect(resultsTabBadgeSrText(0, true)).toBe("unknown recent results needing attention");
    expect(resultsTabBadgeSrText(0, false, true)).toBe(
      "unknown recent results needing attention",
    );
  });
});

describe("overview stuck task badge", () => {
  it("prioritizes critical stuck work and preserves degraded uncertainty", () => {
    expect(overviewStuckBadgeText(0, 0)).toBeNull();
    expect(overviewStuckBadgeText(2, 5)).toBe("2");
    expect(overviewStuckBadgeText(0, 5)).toBe("5");
    expect(overviewStuckBadgeText(2, 5, true)).toBe("2+");
    expect(overviewStuckBadgeText(0, 5, true)).toBe("5+");
    expect(overviewStuckBadgeText(0, 0, true)).toBe("?");
    expect(overviewStuckBadgeText(0, 0, false, 2)).toBe("2");
    expect(overviewStuckBadgeText(0, 0, true, 2)).toBe("2+");

    expect(overviewStuckBadgeTone(2, 5)).toBe("critical");
    expect(overviewStuckBadgeTone(0, 5, false, 2)).toBe("critical");
    expect(overviewStuckBadgeTone(0, 0, false, 2)).toBe("critical");
    expect(overviewStuckBadgeTone(0, 5)).toBe("warning");
    expect(overviewStuckBadgeTone(0, 0, true)).toBe("warning");
    expect(overviewStuckBadgeTone(0, 0)).toBeNull();
  });

  it("names critical, warning, and unknown stuck task counts", () => {
    expect(overviewStuckBadgeSrText(2, 5)).toBe(
      "2 critical stuck tasks and 5 warning stuck tasks",
    );
    expect(overviewStuckBadgeSrText(1, 0)).toBe("1 critical stuck task");
    expect(overviewStuckBadgeSrText(0, 1)).toBe("1 warning stuck task");
    expect(overviewStuckBadgeSrText(0, 5, true)).toBe("at least 5 warning stuck tasks");
    expect(overviewStuckBadgeSrText(0, 0, false, 1)).toBe("1 worker queue has no consumers");
    expect(overviewStuckBadgeSrText(0, 0, true, 2))
      .toBe("at least 2 worker queues have no consumers");
    expect(overviewStuckBadgeSrText(0, 2, false, 3))
      .toBe("3 worker queues have no consumers and 2 warning stuck tasks");
    expect(overviewStuckBadgeSrText(2, 1, false, 3))
      .toBe("2 critical stuck tasks and 3 worker queues have no consumers and 1 warning stuck task");
    expect(overviewStuckBadgeSrText(0, 0, true)).toBe("unknown stuck task count");
  });

  it("counts grouped no-consumer queues once in the Overview badge", () => {
    const stuckTasks: StuckTask[] = [
      {
        prefix: "bbr-platform",
        resource_type: "issue",
        repo: "bluebamboollc/bbr-platform",
        resource_id: "4251",
        reason: "Workers unavailable for retained backlog",
        severity: "critical",
        stream: "orcest:tasks:issue:codex",
        consumer_group: "workers",
        no_worker_consumers: true,
      },
      {
        prefix: "bbr-platform",
        resource_type: "issue",
        repo: "bluebamboollc/bbr-platform",
        resource_id: "4260",
        reason: "No consumers are attached to this worker group",
        severity: "critical",
        stream: "orcest:tasks:issue:codex",
        consumer_group: "workers",
        no_worker_consumers: true,
      },
    ];

    expect(overviewStuckCounts(stuckTasks, 1)).toEqual({
      criticalCount: 1,
      warningCount: 0,
      noConsumerQueueCount: 0,
      criticalQueueCount: 1,
      warningQueueCount: 0,
    });
    expect(overviewStuckBadgeSrText(1, 0, false, 0, 1))
      .toBe("1 critical stuck queue");
  });
});

describe("dashboardLoadingMessage", () => {
  it("does not show stale connection errors once the socket is open", () => {
    expect(dashboardLoadingMessage(true, "previous failure")).toBe("Waiting for snapshot...");
  });

  it("shows current connection errors while disconnected", () => {
    expect(dashboardLoadingMessage(false, "bad token")).toBe("bad token");
    expect(dashboardLoadingMessage(false, null)).toBe("Connecting to server...");
  });
});

describe("redisDisconnectedDetail", () => {
  it("distinguishes Redis reachability from dashboard socket reachability", () => {
    expect(redisDisconnectedDetail(true, null)).toBe(
      "Dashboard server is reachable, but it cannot reach Redis.",
    );
    expect(redisDisconnectedDetail(false, "socket closed")).toBe(
      "Dashboard connection issue: socket closed. Last snapshot reported Redis unavailable.",
    );
    expect(redisDisconnectedDetail(false, "   ")).toBe(
      "Dashboard connection is disconnected. Last snapshot reported Redis unavailable.",
    );
  });
});

describe("sanitizeDashboardUrl", () => {
  it("removes one-time auth tokens while preserving valid tab and other filters", () => {
    const url = new URL(
      "https://dashboard.local/?token=dev-token&tab=results&result_filter=failed&repo=owner%2Frepo",
    );

    expect(sanitizeDashboardUrl(url)).toBe(true);
    expect(url.toString()).toBe(
      "https://dashboard.local/?tab=results&result_filter=failed&repo=owner%2Frepo",
    );
  });

  it("removes invalid tab params so the URL matches the overview fallback", () => {
    const url = new URL(
      "https://dashboard.local/?tab=bogus&result_filter=failed&repo=owner%2Frepo",
    );

    expect(sanitizeDashboardUrl(url)).toBe(true);
    expect(url.toString()).toBe("https://dashboard.local/?repo=owner%2Frepo");
  });

  it("removes invalid or non-canonical result filters", () => {
    const invalid = new URL("https://dashboard.local/?tab=results&result_filter=bogus");
    const all = new URL("https://dashboard.local/?tab=results&result_filter=all");
    const hidden = new URL("https://dashboard.local/?tab=kanban&result_filter=failed");

    expect(sanitizeDashboardUrl(invalid)).toBe(true);
    expect(invalid.toString()).toBe("https://dashboard.local/?tab=results");
    expect(sanitizeDashboardUrl(all)).toBe(true);
    expect(all.toString()).toBe("https://dashboard.local/?tab=results");
    expect(sanitizeDashboardUrl(hidden)).toBe(true);
    expect(hidden.toString()).toBe("https://dashboard.local/?tab=kanban");
  });

  it("does not rewrite already-clean dashboard URLs", () => {
    const url = new URL("https://dashboard.local/?tab=kanban");

    expect(sanitizeDashboardUrl(url)).toBe(false);
    expect(url.toString()).toBe("https://dashboard.local/?tab=kanban");
  });
});
