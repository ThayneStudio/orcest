/**
 * @vitest-environment happy-dom
 */
import { createElement } from "react";
import { cleanup, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { RecentResult, TaskOutputParams } from "../lib/types";
import { taskOutputDomId } from "../lib/taskOutput";
import { formatTimestampMs } from "../lib/format";
import { resultTimestampMs } from "../lib/results";
import {
  filterRecentResults,
  RecentResults,
  recentResultFilterCounts,
  recentResultFilterCountLabel,
  recentResultFilterEmptyMessage,
  recentResultFilterLabel,
  recentResultsHeadingText,
  recentResultTaskOutputPanelKey,
  recentResultsEmptyMessage,
  recentSummaryToggleLabel,
  resultIdStillVisible,
} from "./RecentResults";

const mockUseTaskOutput = vi.hoisted(() => ({
  current: {
    lines: ["historical output line"],
    startIndex: 0,
    connected: true,
    retrying: false,
    done: false,
    error: null,
  },
  params: null as unknown,
}));

vi.mock("../hooks/useTaskOutput", () => ({
  useTaskOutput: (params: unknown) => {
    mockUseTaskOutput.params = params;
    return mockUseTaskOutput.current;
  },
}));

const RESULT_VIEW_LABEL = "View output for PR owner/repo #42 (worker-1, task task-result-1, historical)";
const RESULT_HIDE_LABEL = "Hide output for PR owner/repo #42 (worker-1, task task-result-1, historical)";

function result(resultId: string, overrides: Partial<RecentResult> = {}): RecentResult {
  return {
    result_id: resultId,
    result_stream: "results",
    entry_id: `${resultId}-0`,
    task_id: `task-${resultId}`,
    worker_id: "worker-1",
    status: "completed",
    repo: "owner/repo",
    resource_type: "pr",
    resource_id: "42",
    duration_seconds: 10,
    summary: "summary",
    ...overrides,
  };
}

describe("resultIdStillVisible", () => {
  it("tracks whether an expanded result still exists in the current result set", () => {
    expect(resultIdStillVisible("result-1", [result("result-1")])).toBe(true);
    expect(resultIdStillVisible("result-1", [result("result-2")])).toBe(false);
    expect(resultIdStillVisible(null, [result("result-1")])).toBe(false);
  });
});

describe("recentResultsEmptyMessage", () => {
  it("does not claim there are no results when the section is degraded", () => {
    expect(recentResultsEmptyMessage(false)).toBe("No results yet");
    expect(recentResultsEmptyMessage(true)).toBe("Recent results unavailable");
    expect(recentResultsEmptyMessage(false, true)).toBe("No recent results loaded");
    expect(recentResultsEmptyMessage(false, false, 3)).toBe("No recent results loaded");
    expect(recentResultsEmptyMessage(true, true)).toBe("Recent results unavailable");
  });
});

describe("recent result filters", () => {
  it("labels and filters loaded result groups", () => {
    const rows = [
      result("done", { status: "completed" }),
      result("failed", { status: "failed" }),
      result("unknown", { status: "cancelled" }),
      result("stale", { status: "stale" }),
    ];

    expect(recentResultFilterLabel("all")).toBe("All");
    expect(recentResultFilterLabel("failed")).toBe("Needs attention");
    expect(filterRecentResults(rows, "all").map((row) => row.result_id))
      .toEqual(["done", "failed", "unknown", "stale"]);
    expect(filterRecentResults(rows, "failed").map((row) => row.result_id))
      .toEqual(["failed"]);
    expect(filterRecentResults(rows, "completed").map((row) => row.result_id))
      .toEqual(["done"]);
    expect(filterRecentResults(rows, "neutral").map((row) => row.result_id))
      .toEqual(["unknown", "stale"]);
    expect(recentResultFilterCounts(rows)).toEqual({
      all: 4,
      completed: 1,
      failed: 1,
      neutral: 2,
    });
    expect(recentResultFilterCountLabel(2)).toBe("2");
    expect(recentResultFilterCountLabel(2, true)).toBe("2 loaded");
    expect(recentResultFilterCountLabel(0, true)).toBe("0 loaded");
    expect(recentResultFilterCountLabel(0, true, true)).toBe("0 loaded");
  });

  it("describes empty filtered result sets without claiming all results are empty", () => {
    expect(recentResultFilterEmptyMessage("failed", false))
      .toBe("No loaded results need attention");
    expect(recentResultFilterEmptyMessage("completed", false))
      .toBe("No loaded completions");
    expect(recentResultFilterEmptyMessage("neutral", false))
      .toBe("No loaded other results");
    expect(recentResultFilterEmptyMessage("failed", true))
      .toBe("No loaded results need attention");
    expect(recentResultFilterEmptyMessage("all", false, true))
      .toBe("No recent results loaded");
    expect(recentResultFilterEmptyMessage("all", false, false, 3))
      .toBe("No recent results loaded");
  });
});

describe("recentResultsHeadingText", () => {
  it("shows loaded and total result counts when the retained stream is larger than the preview", () => {
    expect(recentResultsHeadingText(20, 2694)).toBe(
      "Recent Results (20 loaded, 2694 total)",
    );
    expect(recentResultsHeadingText(1, 1)).toBe("Recent Results (1)");
    expect(recentResultsHeadingText(0, 0)).toBe("Recent Results (0)");
  });

  it("keeps partial and unknown result depths explicit", () => {
    expect(recentResultsHeadingText(20, 2694, true)).toBe(
      "Recent Results (20 loaded, 2694 total)",
    );
    expect(recentResultsHeadingText(20, 0)).toBe(
      "Recent Results (20)",
    );
    expect(recentResultsHeadingText(20, 0, false, true)).toBe(
      "Recent Results (20 loaded, ? total)",
    );
    expect(recentResultsHeadingText(0, 0, false, true)).toBe(
      "Recent Results (? total)",
    );
  });
});

describe("recentSummaryToggleLabel", () => {
  it("names summary expansion actions with the target resource", () => {
    expect(recentSummaryToggleLabel("PR owner/repo #42", false)).toBe(
      "Show full summary for PR owner/repo #42",
    );
    expect(recentSummaryToggleLabel("PR owner/repo #42", true)).toBe(
      "Collapse summary for PR owner/repo #42",
    );
  });
});

describe("recentResultTaskOutputPanelKey", () => {
  it("keeps output panels distinct across Redis prefix modes", () => {
    const base = {
      workerId: "worker-1",
      taskId: "task-result-1",
      historical: true,
    };

    expect(recentResultTaskOutputPanelKey({ ...base }))
      .not.toBe(recentResultTaskOutputPanelKey({ ...base, prefix: null }));
    expect(recentResultTaskOutputPanelKey({ ...base, prefix: "project-a" }))
      .not.toBe(recentResultTaskOutputPanelKey({ ...base, prefix: "project-b" }));
  });
});

describe("RecentResults", () => {
  beforeEach(() => {
    (globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
    mockUseTaskOutput.current = {
      lines: ["historical output line"],
      startIndex: 0,
      connected: true,
      retrying: false,
      done: false,
      error: null,
    };
    mockUseTaskOutput.params = null;
  });

  afterEach(() => {
    cleanup();
  });

  it("renders historical output rows with collapsed accessible controls", () => {
    const entryId = "1710000000000-0";
    render(createElement(RecentResults, {
      results: [result("result-1", { entry_id: entryId })],
      total: 2694,
    }));

    expect(screen.getByRole("heading", {
      name: "Recent Results (1 loaded, 2694 total)",
    })).toBeTruthy();
    expect(screen.getByText("worker-1")).toBeTruthy();
    expect(screen.getByRole("table", { name: "Recent results" })).toBeTruthy();
    expect(screen.getByRole("group", { name: "Recent result status filter" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "All 1 loaded" }).getAttribute("aria-pressed"))
      .toBe("true");
    expect(screen.getByRole("button", { name: "Needs attention 0 loaded" }).getAttribute("aria-pressed"))
      .toBe("false");
    expect(screen.getByRole("columnheader", { name: "Time" }).getAttribute("scope"))
      .toBe("col");
    expect(screen.getByRole("columnheader", { name: "Summary" }).getAttribute("scope"))
      .toBe("col");
    expect(screen.getByRole("rowheader", { name: "PR owner/repo #42" }).getAttribute("scope"))
      .toBe("row");
    expect(screen.getByText(formatTimestampMs(resultTimestampMs(entryId))).getAttribute("title"))
      .toBe(`entry ${entryId}`);

    const outputButton = screen.getByRole("button", {
      name: RESULT_VIEW_LABEL,
    });
    expect(outputButton.textContent).toBe("View");
    expect(outputButton.getAttribute("aria-expanded")).toBe("false");
    expect(outputButton.getAttribute("aria-controls")).toBeNull();
  });

  it("filters visible rows by loaded result status", async () => {
    const user = userEvent.setup();
    render(createElement(RecentResults, {
      results: [
        result("failed", { status: "failed", resource_id: "1" }),
        result("done", { status: "completed", resource_id: "2" }),
        result("stale", { status: "stale", resource_id: "3" }),
      ],
      total: 2694,
    }));

    expect(screen.getByRole("button", { name: "All 3 loaded" }).getAttribute("aria-pressed"))
      .toBe("true");
    expect(screen.getByRole("button", { name: "Needs attention 1 loaded" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "Completed 1 loaded" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "Other 1 loaded" })).toBeTruthy();
    expect(screen.getByRole("rowheader", { name: "PR owner/repo #1" })).toBeTruthy();
    expect(screen.getByRole("rowheader", { name: "PR owner/repo #2" })).toBeTruthy();
    expect(screen.getByRole("rowheader", { name: "PR owner/repo #3" })).toBeTruthy();

    await user.click(screen.getByRole("button", { name: "Needs attention 1 loaded" }));

    expect(screen.getByRole("button", { name: "Needs attention 1 loaded" }).getAttribute("aria-pressed"))
      .toBe("true");
    expect(screen.getByRole("rowheader", { name: "PR owner/repo #1" })).toBeTruthy();
    expect(screen.queryByRole("rowheader", { name: "PR owner/repo #2" })).toBeNull();
    expect(screen.queryByRole("rowheader", { name: "PR owner/repo #3" })).toBeNull();

    await user.click(screen.getByRole("button", { name: "Other 1 loaded" }));

    expect(screen.getByRole("rowheader", { name: "PR owner/repo #3" })).toBeTruthy();
    expect(screen.queryByRole("rowheader", { name: "PR owner/repo #1" })).toBeNull();
  });

  it("renders a filtered empty state for non-empty result sets", async () => {
    const user = userEvent.setup();
    render(createElement(RecentResults, {
      results: [result("done", { status: "completed" })],
    }));

    await user.click(screen.getByRole("button", { name: "Needs attention 0" }));

    expect(screen.queryByRole("table", { name: "Recent results" })).toBeNull();
    expect(screen.getByText("No loaded results need attention")).toBeTruthy();
  });

  it("closes mounted historical output when the selected result is hidden by a filter", async () => {
    const user = userEvent.setup();
    render(createElement(RecentResults, {
      results: [
        result("result-1", { status: "completed" }),
        result("result-2", { status: "failed", resource_id: "43" }),
      ],
    }));

    await user.click(screen.getByRole("button", {
      name: RESULT_VIEW_LABEL,
    }));
    expect(screen.getByRole("region", { name: "Output: PR owner/repo #42" })).toBeTruthy();

    await user.click(screen.getByRole("button", { name: "Needs attention 1" }));

    expect(screen.queryByRole("region", { name: "Output: PR owner/repo #42" })).toBeNull();
    expect(screen.getByRole("rowheader", { name: "PR owner/repo #43" })).toBeTruthy();
  });

  it("restores focus when refreshed filter state hides a focused output panel", async () => {
    const user = userEvent.setup();
    const rows = [
      result("result-1", { status: "completed" }),
      result("result-2", { status: "failed", resource_id: "43" }),
    ];
    const { rerender } = render(createElement(RecentResults, {
      results: rows,
      filter: "all",
    }));

    await user.click(screen.getByRole("button", {
      name: RESULT_VIEW_LABEL,
    }));
    const outputLog = screen.getByRole("log", { name: "Output log for PR owner/repo #42" });
    outputLog.focus();
    expect(document.activeElement).toBe(outputLog);

    rerender(createElement(RecentResults, {
      results: rows,
      filter: "failed",
    }));

    await waitFor(() =>
      expect(screen.queryByRole("region", { name: "Output: PR owner/repo #42" })).toBeNull()
    );
    await waitFor(() =>
      expect(document.activeElement).toBe(screen.getByRole("heading", {
        name: "Recent Results (2)",
      }))
    );
    expect(screen.getByRole("rowheader", { name: "PR owner/repo #43" })).toBeTruthy();
  });

  it("restores focus when refreshed filter state hides a focused output trigger", async () => {
    const user = userEvent.setup();
    const rows = [
      result("result-1", { status: "completed" }),
      result("result-2", { status: "failed", resource_id: "43" }),
    ];
    const { rerender } = render(createElement(RecentResults, {
      results: rows,
      filter: "all",
    }));

    await user.click(screen.getByRole("button", {
      name: RESULT_VIEW_LABEL,
    }));
    const selectedButton = screen.getByRole("button", {
      name: RESULT_HIDE_LABEL,
    });
    selectedButton.focus();
    expect(document.activeElement).toBe(selectedButton);

    rerender(createElement(RecentResults, {
      results: rows,
      filter: "failed",
    }));

    await waitFor(() =>
      expect(screen.queryByRole("region", { name: "Output: PR owner/repo #42" })).toBeNull()
    );
    await waitFor(() =>
      expect(document.activeElement).toBe(screen.getByRole("heading", {
        name: "Recent Results (2)",
      }))
    );
    expect(screen.getByRole("rowheader", { name: "PR owner/repo #43" })).toBeTruthy();
  });

  it("honors a controlled filter and reports filter changes", async () => {
    const user = userEvent.setup();
    const onFilterChange = vi.fn();
    render(createElement(RecentResults, {
      results: [
        result("failed", { status: "failed", resource_id: "1" }),
        result("done", { status: "completed", resource_id: "2" }),
      ],
      filter: "failed",
      onFilterChange,
    }));

    expect(screen.getByRole("button", { name: "Needs attention 1" }).getAttribute("aria-pressed"))
      .toBe("true");
    expect(screen.getByRole("rowheader", { name: "PR owner/repo #1" })).toBeTruthy();
    expect(screen.queryByRole("rowheader", { name: "PR owner/repo #2" })).toBeNull();

    await user.click(screen.getByRole("button", { name: "Completed 1" }));

    expect(onFilterChange).toHaveBeenCalledWith("completed");
    expect(screen.queryByRole("rowheader", { name: "PR owner/repo #2" })).toBeNull();
  });

  it("formats prefixed result streams while preserving the raw title", () => {
    render(createElement(RecentResults, {
      results: [result("result-1", { result_stream: "project-a:results" })],
    }));

    const streamCell = screen.getByText("[project-a] results").closest("td");
    expect(streamCell?.getAttribute("title")).toBe("project-a:results");
    expect(screen.getByText("raw stream project-a:results").className).toBe("sr-only");
  });

  it("labels expandable summaries with show and collapse actions", async () => {
    const user = userEvent.setup();
    const longSummary = "x".repeat(100);
    render(createElement(RecentResults, {
      results: [result("result-1", { summary: longSummary })],
    }));

    const expandButton = screen.getByRole("button", {
      name: "Show full summary for PR owner/repo #42",
    });
    expect(expandButton.getAttribute("aria-expanded")).toBe("false");
    expect(expandButton.textContent).toBe(`${"x".repeat(80)}...`);
    expect(expandButton.getAttribute("aria-label")).not.toContain(`${"x".repeat(80)}...`);

    await user.click(expandButton);

    const collapseButton = screen.getByRole("button", {
      name: "Collapse summary for PR owner/repo #42",
    });
    expect(collapseButton.getAttribute("aria-expanded")).toBe("true");
    expect(collapseButton.textContent).toBe(longSummary);
    expect(collapseButton.getAttribute("aria-label")).not.toContain(longSummary);
    expect(collapseButton.className).toContain("break-words");
  });

  it("breaks long non-expandable summary tokens instead of widening the table", () => {
    const summary = "x".repeat(80);
    render(createElement(RecentResults, {
      results: [result("result-1", { summary })],
    }));

    expect(screen.getByText(summary).className).toContain("break-words");
  });

  it("opens and closes historical task output from a result row", async () => {
    const user = userEvent.setup();
    render(createElement(RecentResults, {
      results: [result("result-1")],
    }));

    await user.click(screen.getByRole("button", {
      name: RESULT_VIEW_LABEL,
    }));

    const selectedButton = screen.getByRole("button", {
      name: RESULT_HIDE_LABEL,
    });
    expect(selectedButton.getAttribute("aria-expanded")).toBe("true");
    expect(selectedButton.getAttribute("aria-controls"))
      .toBe(taskOutputDomId({
        workerId: "worker-1",
        taskId: "task-result-1",
        historical: true,
        prefix: null,
        instanceId: "result-1",
      }, "results-output"));
    expect(screen.getByRole("region", { name: "Output: PR owner/repo #42" })).toBeTruthy();
    expect(screen.getByRole("log", { name: "Output log for PR owner/repo #42" })).toBeTruthy();
    expect(screen.getByText("historical output line")).toBeTruthy();

    await user.click(screen.getByRole("button", {
      name: "Close output for PR owner/repo #42",
    }));

    const outputButton = screen.getByRole("button", { name: RESULT_VIEW_LABEL });
    expect(outputButton.getAttribute("aria-expanded")).toBe("false");
    expect(screen.queryByRole("region", { name: "Output: PR owner/repo #42" })).toBeNull();
    await waitFor(() => expect(document.activeElement).toBe(outputButton));
  });

  it("keeps historical output mounted when the resolved output prefix changes", async () => {
    const user = userEvent.setup();
    const selectedResult = result("result-1", { result_stream: "project-a:results" });
    const { rerender } = render(createElement(RecentResults, {
      results: [selectedResult],
    }));

    await user.click(screen.getByRole("button", {
      name: "View output for [project-a] PR owner/repo #42 (worker-1, prefix project-a, task task-result-1, historical)",
    }));
    expect(screen.getByRole("region", { name: "Output: [project-a] PR owner/repo #42" })).toBeTruthy();

    rerender(createElement(RecentResults, {
      results: [{ ...selectedResult, output_prefix: "orcest" }],
    }));

    await waitFor(() => {
      const selectedButton = screen.getByRole("button", {
        name: "Hide output for [project-a] PR owner/repo #42 (worker-1, prefix orcest, task task-result-1, historical)",
      });
      expect(selectedButton.getAttribute("aria-controls")).toBe(taskOutputDomId({
        workerId: "worker-1",
        taskId: "task-result-1",
        historical: true,
        prefix: "orcest",
        instanceId: "result-1",
      }, "results-output"));
      expect((mockUseTaskOutput.params as TaskOutputParams | null)?.prefix).toBe("orcest");
    });
    expect(screen.getByRole("region", { name: "Output: [project-a] PR owner/repo #42" })).toBeTruthy();
  });

  it("selects only one historical row when repeated results share worker and task IDs", async () => {
    const user = userEvent.setup();
    render(createElement(RecentResults, {
      results: [
        result("result-1", { task_id: "shared-task", resource_id: "42" }),
        result("result-2", { task_id: "shared-task", resource_id: "43" }),
      ],
    }));

    await user.click(screen.getByRole("button", {
      name: "View output for PR owner/repo #42 (worker-1, task shared-task, historical)",
    }));

    expect(screen.getByRole("button", {
      name: "Hide output for PR owner/repo #42 (worker-1, task shared-task, historical)",
    })).toBeTruthy();
    expect(screen.getByRole("button", {
      name: "View output for PR owner/repo #43 (worker-1, task shared-task, historical)",
    })).toBeTruthy();
    expect(screen.getByRole("region", { name: "Output: PR owner/repo #42" })).toBeTruthy();
    expect(screen.queryByRole("region", { name: "Output: PR owner/repo #43" })).toBeNull();
  });

  it("clears mounted historical output when the selected result disappears", async () => {
    const user = userEvent.setup();
    const { rerender } = render(createElement(RecentResults, {
      results: [result("result-1")],
    }));

    await user.click(screen.getByRole("button", {
      name: RESULT_VIEW_LABEL,
    }));
    expect(screen.getByRole("region", { name: "Output: PR owner/repo #42" })).toBeTruthy();
    const outputLog = screen.getByRole("log", { name: "Output log for PR owner/repo #42" });
    outputLog.focus();
    expect(document.activeElement).toBe(outputLog);

    rerender(createElement(RecentResults, { results: [] }));

    await waitFor(() =>
      expect(screen.queryByRole("region", { name: "Output: PR owner/repo #42" })).toBeNull()
    );
    const heading = screen.getByRole("heading", { name: "Recent Results (0)" });
    expect(screen.getByText("No results yet")).toBeTruthy();
    await waitFor(() => expect(document.activeElement).toBe(heading));
  });

  it("does not steal unrelated focus when the selected result disappears", async () => {
    const user = userEvent.setup();
    const view = (results: RecentResult[]) =>
      createElement("div", null,
        createElement("button", { type: "button" }, "External action"),
        createElement(RecentResults, { results }),
      );
    const { rerender } = render(view([result("result-1")]));

    await user.click(screen.getByRole("button", {
      name: RESULT_VIEW_LABEL,
    }));
    const external = screen.getByRole("button", { name: "External action" });
    external.focus();
    expect(document.activeElement).toBe(external);

    rerender(view([]));

    await waitFor(() =>
      expect(screen.queryByRole("region", { name: "Output: PR owner/repo #42" })).toBeNull()
    );
    await waitFor(() => expect(document.activeElement).toBe(external));
  });

  it("keeps mounted historical output when degraded result data no longer contains the selected row", async () => {
    const user = userEvent.setup();
    const { rerender } = render(createElement(RecentResults, {
      results: [result("result-1")],
    }));

    await user.click(screen.getByRole("button", {
      name: RESULT_VIEW_LABEL,
    }));
    expect(screen.getByRole("region", { name: "Output: PR owner/repo #42" })).toBeTruthy();
    const outputLog = screen.getByRole("log", { name: "Output log for PR owner/repo #42" });
    outputLog.focus();
    expect(document.activeElement).toBe(outputLog);

    rerender(createElement(RecentResults, {
      results: [],
      degraded: true,
    }));

    await waitFor(() =>
      expect(screen.getByRole("region", { name: "Output: PR owner/repo #42" })).toBeTruthy()
    );
    expect(screen.getByRole("heading", { name: "Recent Results (?)" })).toBeTruthy();
    expect(screen.getByText("Recent results unavailable")).toBeTruthy();
    expect(document.activeElement).toBe(outputLog);
  });

  it("refreshes the mounted output label when the selected result changes resource", async () => {
    const user = userEvent.setup();
    const { rerender } = render(createElement(RecentResults, {
      results: [result("result-1")],
    }));

    await user.click(screen.getByRole("button", {
      name: RESULT_VIEW_LABEL,
    }));
    expect(screen.getByRole("region", { name: "Output: PR owner/repo #42" })).toBeTruthy();

    rerender(createElement(RecentResults, {
      results: [result("result-1", { resource_id: "43" })],
    }));

    expect(screen.queryByRole("region", { name: "Output: PR owner/repo #42" })).toBeNull();
    expect(screen.getByRole("region", { name: "Output: PR owner/repo #43" })).toBeTruthy();
    expect(screen.getByRole("button", {
      name: "Close output for PR owner/repo #43",
    })).toBeTruthy();
  });

  it("renders degraded empty state with an unknown count", () => {
    render(createElement(RecentResults, { results: [], degraded: true }));

    expect(screen.getByRole("heading", { name: "Recent Results (?)" })).toBeTruthy();
    expect(screen.getByText("Recent results unavailable")).toBeTruthy();
  });

  it("renders unknown total when result depth is degraded", () => {
    render(createElement(RecentResults, {
      results: [result("result-1")],
      total: 0,
      depthDegraded: true,
    }));

    expect(screen.getByRole("heading", {
      name: "Recent Results (1 loaded, ? total)",
    })).toBeTruthy();
  });

  it("does not claim there are no results when no preview rows are loaded", () => {
    render(createElement(RecentResults, {
      results: [],
      total: 2694,
    }));

    expect(screen.getByRole("heading", {
      name: "Recent Results (0 loaded, 2694 total)",
    })).toBeTruthy();
    expect(screen.getByText("No recent results loaded")).toBeTruthy();
    expect(screen.queryByText("No results yet")).toBeNull();
  });

  it("does not claim the retained result stream is empty when depth is unavailable", () => {
    render(createElement(RecentResults, {
      results: [],
      total: 0,
      depthDegraded: true,
    }));

    expect(screen.getByRole("heading", { name: "Recent Results (? total)" })).toBeTruthy();
    expect(screen.getByText("No recent results loaded")).toBeTruthy();
    expect(screen.queryByText("No results yet")).toBeNull();
  });
});
