/**
 * @vitest-environment happy-dom
 */
import { act, cleanup, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import App from "./App";
import type { RecentResult, StuckTask, SystemSnapshot } from "./lib/types";

interface MockSnapshotState {
  snapshot: SystemSnapshot | null;
  stuckTasks: StuckTask[];
  workers: string[];
  connected: boolean;
  error: string | null;
  lastUpdate: Date | null;
}

const mockUseSnapshot = vi.hoisted(() => ({
  current: {
    snapshot: null,
    stuckTasks: [],
    workers: [],
    connected: false,
    error: null,
    lastUpdate: null,
  } as MockSnapshotState,
}));

vi.mock("./hooks/useSnapshot", () => ({
  useSnapshot: () => mockUseSnapshot.current,
}));

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

function recentResult(overrides: Partial<RecentResult> = {}): RecentResult {
  return {
    result_id: "results:1-0",
    result_stream: "results",
    entry_id: "1-0",
    task_id: "task-1",
    worker_id: "worker-1",
    status: "completed",
    repo: "owner/repo",
    resource_type: "pr",
    resource_id: "42",
    duration_seconds: 10,
    summary: "done",
    ...overrides,
  };
}

function stuckTask(overrides: Partial<StuckTask> = {}): StuckTask {
  return {
    prefix: null,
    resource_type: "pr",
    repo: "owner/repo",
    resource_id: "42",
    reason: "Worker group has backlog but no consumers",
    severity: "critical",
    ...overrides,
  };
}

function resetSnapshotState(overrides: Partial<MockSnapshotState> = {}) {
  mockUseSnapshot.current = {
    snapshot: null,
    stuckTasks: [],
    workers: [],
    connected: false,
    error: null,
    lastUpdate: null,
    ...overrides,
  };
}

function selectedTab(name: string): HTMLElement {
  const tab = screen.getByRole("tab", { name });
  expect(tab.getAttribute("aria-selected")).toBe("true");
  return tab;
}

describe("App DOM tab interactions", () => {
  beforeEach(() => {
    (globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
    window.history.replaceState(null, "", "/");
    vi.stubGlobal("requestAnimationFrame", (callback: FrameRequestCallback) => {
      callback(0);
      return 1;
    });
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({ ok: true }));
    resetSnapshotState({
      snapshot: snapshot(),
      connected: true,
      lastUpdate: new Date("2026-06-20T00:00:00Z"),
    });
  });

  afterEach(() => {
    cleanup();
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it("drives tab selection, focus, and URL state through real DOM events", async () => {
    const user = userEvent.setup();
    window.history.replaceState(
      null,
      "",
      "/?token=dev-token&tab=bogus&repo=owner%2Frepo",
    );
    const replaceState = vi.spyOn(window.history, "replaceState");

    render(<App />);

    expect(fetch).toHaveBeenCalledWith("/api/auth/bootstrap?token=dev-token", {
      credentials: "same-origin",
    });
    expect(new URL(window.location.href).searchParams.has("token")).toBe(false);
    expect(new URL(window.location.href).searchParams.has("tab")).toBe(false);
    expect(new URL(window.location.href).searchParams.get("repo")).toBe("owner/repo");
    expect(replaceState).toHaveBeenCalledTimes(1);
    expect(selectedTab("Overview").getAttribute("aria-controls")).toBe("dashboard-panel-overview");
    expect(screen.getByRole("tabpanel", { name: "Overview" }).id)
      .toBe("dashboard-panel-overview");
    expect(document.getElementById("dashboard-panel-overview")?.hasAttribute("hidden"))
      .toBe(false);
    expect(screen.getByRole("tab", { name: "Kanban" }).getAttribute("aria-controls"))
      .toBe("dashboard-panel-kanban");
    expect(screen.getByRole("tab", { name: "Results" }).getAttribute("aria-controls"))
      .toBe("dashboard-panel-results");
    expect(document.getElementById("dashboard-panel-kanban")?.hasAttribute("hidden"))
      .toBe(true);
    expect(document.getElementById("dashboard-panel-results")?.hasAttribute("hidden"))
      .toBe(true);

    await user.click(screen.getByRole("tab", { name: "Kanban" }));

    expect(selectedTab("Kanban").getAttribute("aria-controls")).toBe("dashboard-panel-kanban");
    expect(new URL(window.location.href).searchParams.get("tab")).toBe("kanban");
    expect(screen.getByRole("tabpanel", { name: "Kanban" }).id)
      .toBe("dashboard-panel-kanban");
    expect(document.getElementById("dashboard-panel-overview")?.hasAttribute("hidden"))
      .toBe(true);
    expect(document.getElementById("dashboard-panel-kanban")?.hasAttribute("hidden"))
      .toBe(false);

    await user.keyboard("{ArrowRight}");

    const resultsTab = selectedTab("Results");
    expect(document.activeElement).toBe(resultsTab);
    expect(new URL(window.location.href).searchParams.get("tab")).toBe("results");
    expect(screen.getByRole("tabpanel", { name: "Results" }).id)
      .toBe("dashboard-panel-results");
    expect(document.getElementById("dashboard-panel-results")?.hasAttribute("hidden"))
      .toBe(false);

    await act(async () => {
      window.history.pushState(null, "", "/?tab=dead-letters");
      window.dispatchEvent(new PopStateEvent("popstate"));
    });

    expect(selectedTab("Dead Letters").getAttribute("aria-controls"))
      .toBe("dashboard-panel-dead-letters");
    expect(screen.getByRole("tabpanel", { name: "Dead Letters" }).id)
      .toBe("dashboard-panel-dead-letters");
  });

  it("keeps the active tabpanel mounted while waiting for the first snapshot", () => {
    resetSnapshotState({
      snapshot: null,
      connected: false,
      error: null,
    });

    render(<App />);

    expect(selectedTab("Overview").getAttribute("aria-controls")).toBe("dashboard-panel-overview");
    expect(screen.getByRole("tabpanel", { name: "Overview" }).id)
      .toBe("dashboard-panel-overview");
    expect(screen.getByText("Connecting to server...")).toBeTruthy();
  });

  it("renders Redis disconnected snapshots as an actionable alert", () => {
    resetSnapshotState({
      snapshot: snapshot({ redis_ok: false }),
      connected: true,
      error: null,
    });

    render(<App />);

    const alert = screen.getByRole("alert");
    expect(alert.textContent).toContain("Redis Disconnected");
    expect(alert.textContent).toContain(
      "Dashboard server is reachable, but it cannot reach Redis.",
    );
  });

  it("renders degraded snapshot state in the active tabpanel", () => {
    resetSnapshotState({
      snapshot: snapshot({ degraded_sections: ["recent_results"] }),
      connected: true,
    });

    render(<App />);

    expect(screen.getByRole("tabpanel", { name: "Overview" })).toBeTruthy();
    expect(screen.getByText("Dashboard data is partially unavailable")).toBeTruthy();
    expect(screen.getByText("Incomplete sections: Recent results")).toBeTruthy();
    expect(screen.getByText("Worker Queue & Stream Depths")).toBeTruthy();
  });

  it("passes no-consumer worker queue coverage into the Worker Runtime panel", () => {
    resetSnapshotState({
      snapshot: snapshot({
        queue_depths: {
          "orcest:tasks:codex": 3,
        },
        consumer_groups: [
          {
            stream: "orcest:tasks:codex",
            name: "workers",
            consumers: 0,
            pending: 0,
            lag: null,
          },
        ],
        worker_pool: [
          {
            prefix: "orcest",
            template_vmid: "9001",
            idle: [],
            active: [],
            idle_count: 0,
            active_count: 0,
          },
        ],
      }),
      connected: true,
    });

    render(<App />);

    expect(screen.getByRole("heading", { name: "Worker Runtime" })).toBeTruthy();
    expect(screen.getByText("[orcest] codex has 3 queued/pending items with no consumers"))
      .toBeTruthy();
  });

  it("badges Overview when worker queues have backlog but no consumers", () => {
    resetSnapshotState({
      snapshot: snapshot({
        queue_depths: {
          "orcest:tasks:codex": 3,
        },
        consumer_groups: [
          {
            stream: "orcest:tasks:codex",
            name: "workers",
            consumers: 0,
            pending: 0,
            lag: null,
          },
        ],
      }),
      stuckTasks: [],
      connected: true,
    });

    render(<App />);

    const overview = selectedTab("Overview 1 worker queue has no consumers");
    expect(overview.textContent).toContain("1");
    expect(overview.querySelector("span")?.className).toContain("bg-red-500/20");
  });

  it("badges the Overview tab when stuck tasks need attention", () => {
    resetSnapshotState({
      snapshot: snapshot(),
      stuckTasks: [
        stuckTask({ severity: "critical", resource_id: "42" }),
        stuckTask({ severity: "critical", resource_id: "43" }),
        stuckTask({ severity: "warning", resource_id: "44" }),
      ],
      connected: true,
    });

    render(<App />);

    const overview = selectedTab("Overview 2 critical stuck tasks and 1 warning stuck task");
    expect(overview.textContent).toContain("Overview");
    expect(overview.textContent).toContain("2");
    expect(screen.getByText("2 critical stuck tasks")).toBeTruthy();
  });

  it("badges grouped no-consumer stuck queues with the rendered alert count", () => {
    resetSnapshotState({
      snapshot: snapshot({
        consumer_groups: [
          {
            stream: "orcest:tasks:issue:codex",
            name: "workers",
            consumers: 0,
            pending: 0,
            lag: 3,
          },
        ],
      }),
      stuckTasks: [
        stuckTask({
          prefix: "bbr-platform",
          repo: "bluebamboollc/bbr-platform",
          resource_type: "issue",
          resource_id: "4251",
          reason: "Workers unavailable for retained backlog",
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
          reason: "No consumers are attached to this worker group",
          severity: "critical",
          stream: "orcest:tasks:issue:codex",
          consumer_group: "workers",
          entry_id: "1782736756142-0",
          task_id: "task-4260",
          no_worker_consumers: true,
        }),
      ],
      connected: true,
    });

    render(<App />);

    const overview = selectedTab("Overview 1 critical stuck queue");
    expect(overview.textContent).toContain("1");
    expect(screen.getByText("1 critical stuck queue (3 queued/pending; 2 shown resources)"))
      .toBeTruthy();
  });

  it("passes retained result depth into the Results tab heading", () => {
    window.history.replaceState(null, "", "/?tab=results");
    resetSnapshotState({
      snapshot: snapshot({
        results_depth: 2694,
        recent_results: [recentResult({
          result_id: "bbr-platform:results:1-0",
          result_stream: "bbr-platform:results",
          repo: null,
          resource_id: "4248",
        })],
      }),
      connected: true,
    });

    render(<App />);

    expect(selectedTab("Results unknown recent results needing attention")).toBeTruthy();
    expect(screen.getByRole("heading", {
      name: "Recent Results (1 loaded, 2694 total)",
    })).toBeTruthy();
    expect(screen.getByRole("rowheader", { name: "[bbr-platform] PR #4248" }))
      .toBeTruthy();
  });

  it("loads and updates the Results filter through the URL", async () => {
    const user = userEvent.setup();
    window.history.replaceState(null, "", "/?tab=results&result_filter=failed");
    resetSnapshotState({
      snapshot: snapshot({
        recent_results: [
          recentResult({ status: "failed", result_id: "results:failed", resource_id: "1" }),
          recentResult({ status: "completed", result_id: "results:done", resource_id: "2" }),
          recentResult({ status: "stale", result_id: "results:stale", resource_id: "3" }),
        ],
      }),
      connected: true,
    });

    render(<App />);

    expect(selectedTab("Results 1 recent result needs attention")).toBeTruthy();
    expect(screen.getByRole("button", { name: "Needs attention 1" }).getAttribute("aria-pressed"))
      .toBe("true");
    expect(screen.getByRole("rowheader", { name: "PR owner/repo #1" })).toBeTruthy();
    expect(screen.queryByRole("rowheader", { name: "PR owner/repo #2" })).toBeNull();

    await user.click(screen.getByRole("button", { name: "Completed 1" }));

    expect(new URL(window.location.href).searchParams.get("result_filter"))
      .toBe("completed");
    expect(screen.getByRole("rowheader", { name: "PR owner/repo #2" })).toBeTruthy();
    expect(screen.queryByRole("rowheader", { name: "PR owner/repo #1" })).toBeNull();

    await user.click(screen.getByRole("button", { name: "All 3" }));

    expect(new URL(window.location.href).searchParams.get("tab")).toBe("results");
    expect(new URL(window.location.href).searchParams.has("result_filter")).toBe(false);
    expect(screen.getByRole("rowheader", { name: "PR owner/repo #1" })).toBeTruthy();
    expect(screen.getByRole("rowheader", { name: "PR owner/repo #2" })).toBeTruthy();
  });

  it("names the Results badge when recent results need attention", () => {
    resetSnapshotState({
      snapshot: snapshot({
        recent_results: [
          recentResult({ status: "failed", result_id: "results:failed", entry_id: "2-0" }),
          recentResult({ status: "blocked", result_id: "results:blocked", entry_id: "3-0" }),
          recentResult({ status: "completed", result_id: "results:completed", entry_id: "4-0" }),
        ],
      }),
      connected: true,
    });

    render(<App />);

    expect(screen.getByRole("tab", {
      name: "Results 2 recent results need attention",
    })).toBeTruthy();
  });

  it("names blocked and usage-exhausted Results badges without calling them failures", () => {
    resetSnapshotState({
      snapshot: snapshot({
        recent_results: [
          recentResult({ status: "blocked", result_id: "results:blocked", entry_id: "3-0" }),
          recentResult({
            status: "usage_exhausted",
            result_id: "results:usage-exhausted",
            entry_id: "4-0",
          }),
          recentResult({ status: "completed", result_id: "results:completed", entry_id: "5-0" }),
        ],
      }),
      connected: true,
    });

    render(<App />);

    const resultsTab = screen.getByRole("tab", {
      name: "Results 2 recent results need attention",
    });
    expect(resultsTab.textContent?.toLowerCase()).not.toContain("failure");
  });

  it("surfaces recent result health on the Overview tab and opens Results details", async () => {
    const user = userEvent.setup();
    resetSnapshotState({
      snapshot: snapshot({
        results_depth: 2694,
        recent_results: [
          recentResult({
            status: "failed",
            result_id: "bbr-platform:results:2-0",
            result_stream: "bbr-platform:results",
            entry_id: "1710000001000-0",
            repo: null,
            resource_id: "4248",
            summary: "[transient] Failed after 3 attempts",
          }),
          recentResult({ status: "completed", result_id: "results:done", entry_id: "3-0" }),
        ],
      }),
      connected: true,
    });

    render(<App />);

    expect(screen.getByRole("heading", { name: "Recent Result Health" })).toBeTruthy();
    expect(screen.getByText("2 loaded, 2694 total")).toBeTruthy();
    expect(screen.getByText("[bbr-platform] PR #4248")).toBeTruthy();
    expect(screen.getByText("[transient] Failed after 3 attempts")).toBeTruthy();

    await user.click(screen.getByRole("button", { name: "View Results" }));

    const resultsTab = selectedTab("Results 1+ loaded recent result needs attention");
    expect(document.activeElement).toBe(resultsTab);
    expect(new URL(window.location.href).searchParams.get("tab")).toBe("results");
    expect(new URL(window.location.href).searchParams.get("result_filter")).toBe("failed");
    expect(screen.getByRole("tabpanel", { name: "Results 1+ loaded recent result needs attention" }).id)
      .toBe("dashboard-panel-results");
    expect(screen.getByRole("button", { name: "Needs attention 1 loaded" }).getAttribute("aria-pressed"))
      .toBe("true");
    expect(screen.getByRole("rowheader", { name: "[bbr-platform] PR #4248" })).toBeTruthy();
    expect(screen.queryByRole("rowheader", { name: "PR owner/repo #42" })).toBeNull();
  });

  it("opens filtered Results from Overview result health count cards", async () => {
    const user = userEvent.setup();
    resetSnapshotState({
      snapshot: snapshot({
        recent_results: [
          recentResult({ status: "failed", result_id: "results:failed", resource_id: "1" }),
          recentResult({ status: "completed", result_id: "results:done", resource_id: "2" }),
          recentResult({ status: "stale", result_id: "results:stale", resource_id: "3" }),
        ],
      }),
      connected: true,
    });

    render(<App />);

    await user.click(screen.getByRole("button", {
      name: "View completed results, 1 loaded",
    }));

    expect(selectedTab("Results 1 recent result needs attention")).toBeTruthy();
    expect(new URL(window.location.href).searchParams.get("tab")).toBe("results");
    expect(new URL(window.location.href).searchParams.get("result_filter")).toBe("completed");
    expect(screen.getByRole("button", { name: "Completed 1" }).getAttribute("aria-pressed"))
      .toBe("true");
    expect(screen.getByRole("rowheader", { name: "PR owner/repo #2" })).toBeTruthy();
    expect(screen.queryByRole("rowheader", { name: "PR owner/repo #1" })).toBeNull();
  });

  it("opens all Results from the Overview health action when no loaded results need attention", async () => {
    const user = userEvent.setup();
    resetSnapshotState({
      snapshot: snapshot({
        results_depth: 2694,
        recent_results: [
          recentResult({
            status: "completed",
            result_id: "bbr-platform:results:3-0",
            result_stream: "bbr-platform:results",
            entry_id: "1710000003000-0",
            repo: null,
            resource_id: "4248",
          }),
        ],
      }),
      connected: true,
    });

    render(<App />);

    await user.click(screen.getByRole("button", { name: "View Results" }));

    expect(selectedTab("Results unknown recent results needing attention")).toBeTruthy();
    expect(new URL(window.location.href).searchParams.get("tab")).toBe("results");
    expect(new URL(window.location.href).searchParams.has("result_filter")).toBe(false);
    expect(screen.getByRole("button", { name: "All 1 loaded" }).getAttribute("aria-pressed"))
      .toBe("true");
    expect(screen.getByRole("button", { name: "Needs attention 0 loaded" }).getAttribute("aria-pressed"))
      .toBe("false");
    expect(screen.getByRole("rowheader", { name: "[bbr-platform] PR #4248" })).toBeTruthy();
  });

  it("names the degraded Results badge as unknown attention state", () => {
    resetSnapshotState({
      snapshot: snapshot({ degraded_sections: ["recent_results"] }),
      connected: true,
    });

    render(<App />);

    expect(screen.getByRole("tab", {
      name: "Results unknown recent results needing attention",
    })).toBeTruthy();
  });

  it("names the degraded dead-letter badge as an unknown count", () => {
    resetSnapshotState({
      snapshot: snapshot({ degraded_sections: ["dead_letter_depth"] }),
      connected: true,
    });

    render(<App />);

    expect(screen.getByRole("tab", {
      name: "Dead Letters unknown dead-letter count",
    })).toBeTruthy();
  });

  it("keeps known loaded dead-letter counts visible when total depth is unknown", () => {
    resetSnapshotState({
      snapshot: snapshot({
        degraded_sections: ["dead_letter_depth"],
        dead_letter_entries: [{
          dead_letter_id: "dead-letter:1-0",
          dead_letter_stream: "dead-letter",
          entry_id: "1-0",
          task_id: "task-1",
          task_type: "fix_pr",
          repo: "owner/repo",
          resource_type: "pr",
          resource_id: "42",
          timestamp_ms: null,
          reason: "worker failed",
        }],
      }),
      connected: true,
    });

    render(<App />);

    const tab = screen.getByRole("tab", {
      name: "Dead Letters at least 1 dead-letter entry, total unknown",
    });
    expect(tab.textContent).toContain("1+");
  });

  it("names exact dead-letter badge counts with an accessible unit", () => {
    resetSnapshotState({
      snapshot: snapshot({ dead_letter_count: 3 }),
      connected: true,
    });

    render(<App />);

    expect(screen.getByRole("tab", {
      name: "Dead Letters 3 dead-letter entries",
    })).toBeTruthy();
  });

  it("sanitizes and bootstraps tokenized URLs restored through browser history", async () => {
    render(<App />);
    expect(fetch).not.toHaveBeenCalled();

    await act(async () => {
      window.history.pushState(
        null,
        "",
        "/?token=rotated-token&tab=bogus&repo=owner%2Frepo",
      );
      window.dispatchEvent(new PopStateEvent("popstate"));
    });

    expect(fetch).toHaveBeenCalledWith("/api/auth/bootstrap?token=rotated-token", {
      credentials: "same-origin",
    });
    expect(new URL(window.location.href).searchParams.has("token")).toBe(false);
    expect(new URL(window.location.href).searchParams.has("tab")).toBe(false);
    expect(new URL(window.location.href).searchParams.get("repo")).toBe("owner/repo");
    expect(selectedTab("Overview").getAttribute("aria-controls")).toBe("dashboard-panel-overview");
  });

  it("restores Results filter state from browser history", async () => {
    resetSnapshotState({
      snapshot: snapshot({
        recent_results: [
          recentResult({ status: "failed", result_id: "results:failed", resource_id: "1" }),
          recentResult({ status: "completed", result_id: "results:done", resource_id: "2" }),
        ],
      }),
      connected: true,
    });
    render(<App />);

    await act(async () => {
      window.history.pushState(
        null,
        "",
        "/?tab=results&result_filter=completed&repo=owner%2Frepo",
      );
      window.dispatchEvent(new PopStateEvent("popstate"));
    });

    expect(selectedTab("Results 1 recent result needs attention")).toBeTruthy();
    expect(new URL(window.location.href).searchParams.get("result_filter"))
      .toBe("completed");
    expect(screen.getByRole("button", { name: "Completed 1" }).getAttribute("aria-pressed"))
      .toBe("true");
    expect(screen.getByRole("rowheader", { name: "PR owner/repo #2" })).toBeTruthy();
    expect(screen.queryByRole("rowheader", { name: "PR owner/repo #1" })).toBeNull();
  });
});
