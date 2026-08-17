import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import { formatTimestampMs } from "../lib/format";
import { resultTimestampMs } from "../lib/results";
import type { RecentResult } from "../lib/types";
import {
  ResultHealth,
  resultHealthCountActionLabel,
  resultHealthCountDisplayLabel,
  resultHealthEmptyMessage,
  resultHealthLoadedLabel,
  resultHealthStatusLabel,
} from "./ResultHealth";

function result(overrides: Partial<RecentResult> = {}): RecentResult {
  return {
    result_id: "results:1-0",
    result_stream: "results",
    entry_id: "1710000000000-0",
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

describe("resultHealthLoadedLabel", () => {
  it("describes loaded and retained result counts", () => {
    expect(resultHealthLoadedLabel(20, 2694)).toBe("20 loaded, 2694 total");
    expect(resultHealthLoadedLabel(3, 2)).toBe("3 loaded");
    expect(resultHealthLoadedLabel(0, 0, true)).toBe("? total");
    expect(resultHealthLoadedLabel(2, 0, true)).toBe("2 loaded, ? total");
  });
});

describe("resultHealthStatusLabel", () => {
  it("marks attention and degraded states distinctly", () => {
    expect(resultHealthStatusLabel(0, false)).toBe("no loaded results need attention");
    expect(resultHealthStatusLabel(1, false)).toBe("1 loaded result needs attention");
    expect(resultHealthStatusLabel(2, false)).toBe("2 loaded results need attention");
    expect(resultHealthStatusLabel(0, true)).toBe("partial data");
    expect(resultHealthStatusLabel(2, true)).toBe("2 loaded results need attention, partial data");
    expect(resultHealthStatusLabel(0, false, true)).toBe("partial data");
    expect(resultHealthStatusLabel(1, false, true)).toBe("1 loaded result needs attention, partial data");
  });
});

describe("resultHealthEmptyMessage", () => {
  it("does not claim zero results when result data is unavailable", () => {
    expect(resultHealthEmptyMessage(false)).toBe("No recent results loaded");
    expect(resultHealthEmptyMessage(true)).toBe("Recent results unavailable");
  });
});

describe("resultHealthCountActionLabel", () => {
  it("names filtered result navigation actions with loaded counts", () => {
    expect(resultHealthCountActionLabel("all", 3)).toBe("View all results, 3 loaded");
    expect(resultHealthCountActionLabel("completed", 1)).toBe(
      "View completed results, 1 loaded",
    );
    expect(resultHealthCountActionLabel("failed", 2)).toBe(
      "View results that need attention, 2 loaded",
    );
    expect(resultHealthCountActionLabel("neutral", 0, true)).toBe(
      "View other results, 0 loaded",
    );
    expect(resultHealthCountActionLabel("completed", 1, true)).toBe(
      "View completed results, 1 loaded",
    );
  });
});

describe("resultHealthCountDisplayLabel", () => {
  it("marks visible card counts as loaded for preview-only data", () => {
    expect(resultHealthCountDisplayLabel(3)).toBe("3");
    expect(resultHealthCountDisplayLabel(3, true)).toBe("3 loaded");
    expect(resultHealthCountDisplayLabel(0, true)).toBe("0 loaded");
    expect(resultHealthCountDisplayLabel(0, true, true)).toBe("0 loaded");
  });
});

describe("ResultHealth", () => {
  it("summarizes loaded recent results and highlights the latest attention result", () => {
    const attentionEntryId = "1710000001000-0";
    const html = renderToStaticMarkup(createElement(ResultHealth, {
      results: [
        result({
          result_id: `results:${attentionEntryId}`,
          entry_id: attentionEntryId,
          result_stream: "bbr-platform:results",
          status: "failed",
          repo: null,
          resource_id: "4248",
          worker_id: "orcest-worker-10002",
          duration_seconds: 32,
          summary: "[transient] Failed after 3 attempts",
        }),
        result({ result_id: "results:done", status: "completed" }),
        result({ result_id: "results:stale", status: "stale" }),
      ],
      total: 2694,
    }));

    expect(html).toContain("Recent Result Health");
    expect(html).toContain("3 loaded, 2694 total");
    expect(html).toContain("1 loaded result needs attention");
    expect(html).toContain("Completed");
    expect(html).toContain("Needs attention");
    expect(html).toContain("Other");
    expect(html.match(/title="1 loaded">1 loaded<\/div>/g)).toHaveLength(3);
    expect(html).toContain("[bbr-platform] PR #4248");
    expect(html).toContain("Failed");
    expect(html).toContain(formatTimestampMs(resultTimestampMs(attentionEntryId)));
    expect(html).toContain("orcest-worker-10002");
    expect(html).toContain("32s");
    expect(html).toContain("[transient] Failed after 3 attempts");
  });

  it("highlights the newest loaded attention result even when recent results arrive unsorted", () => {
    const newestEntryId = "1710000002000-0";
    const html = renderToStaticMarkup(createElement(ResultHealth, {
      results: [
        result({
          result_id: "results:older-failure",
          entry_id: "1710000001000-0",
          status: "failed",
          resource_id: "41",
          worker_id: "older-worker",
          summary: "older failure",
        }),
        result({
          result_id: "results:completed",
          entry_id: "1710000003000-0",
          status: "completed",
          resource_id: "43",
          worker_id: "completed-worker",
          summary: "completed",
        }),
        result({
          result_id: "results:newest-failure",
          entry_id: newestEntryId,
          status: "blocked",
          resource_id: "42",
          worker_id: "newest-worker",
          summary: "newest failure",
        }),
      ],
    }));

    expect(html).toContain("PR owner/repo #42");
    expect(html).toContain("Blocked");
    expect(html).toContain(formatTimestampMs(resultTimestampMs(newestEntryId)));
    expect(html).toContain("newest-worker");
    expect(html).toContain("newest failure");
    expect(html).not.toContain("older-worker");
  });

  it("does not call blocked or usage-exhausted attention results failures", () => {
    const html = renderToStaticMarkup(createElement(ResultHealth, {
      results: [
        result({
          result_id: "results:blocked",
          entry_id: "1710000001000-0",
          status: "blocked",
          resource_id: "41",
          summary: "blocked by policy",
        }),
        result({
          result_id: "results:usage",
          entry_id: "1710000002000-0",
          status: "usage_exhausted",
          resource_id: "42",
          summary: "usage cap reached",
        }),
      ],
    }));

    expect(html).toContain("Needs attention");
    expect(html).toContain("Usage exhausted");
    expect(html).not.toMatch(/failures?/i);
  });

  it("renders an optional command for opening the full Results tab", () => {
    const html = renderToStaticMarkup(createElement(ResultHealth, {
      results: [result()],
      onOpenResults: () => undefined,
    }));

    expect(html).toContain("<button");
    expect(html).toContain("View Results");
    expect(html).toContain("View completed results, 1 loaded");
    expect(html).toContain("View results that need attention, 0 loaded");
    expect(html).toContain("View other results, 0 loaded");
  });

  it("shows degraded loaded counts without implying completeness", () => {
    const html = renderToStaticMarkup(createElement(ResultHealth, {
      results: [
        result({ result_id: "results:done", status: "completed" }),
      ],
      degraded: true,
      depthDegraded: true,
    }));

    expect(html).toContain("1 loaded, ? total");
    expect(html).toContain("partial data");
    expect(html).toContain("1 loaded");
    expect(html).toContain("No loaded results need attention");
  });

  it("marks unknown result depth as partial data even when loaded rows are clean", () => {
    const html = renderToStaticMarkup(createElement(ResultHealth, {
      results: [
        result({ result_id: "results:done", status: "completed" }),
      ],
      total: 0,
      depthDegraded: true,
    }));

    expect(html).toContain("1 loaded, ? total");
    expect(html).toContain("partial data");
    expect(html).not.toContain("no loaded results need attention</span>");
  });

  it("renders unavailable empty state when recent results are degraded", () => {
    const html = renderToStaticMarkup(createElement(ResultHealth, {
      results: [],
      degraded: true,
    }));

    expect(html).toContain("? total");
    expect(html).toContain("Recent results unavailable");
  });
});
