import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import { formatTimestampMs } from "../lib/format";
import type { DeadLetterEntry } from "../lib/types";
import {
  DeadLetters,
  deadLetterEmptyMessage,
  deadLetterHeadingText,
  deadLetterReasonText,
  deadLetterTaskIdDisplay,
  deadLetterTaskTypeText,
} from "./DeadLetters";

function entry(overrides: Partial<DeadLetterEntry> = {}): DeadLetterEntry {
  return {
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
    ...overrides,
  };
}

describe("deadLetterEmptyMessage", () => {
  it("distinguishes no dead letters from unavailable entry details", () => {
    expect(deadLetterEmptyMessage(0)).toBe("No dead-letter entries");
    expect(deadLetterEmptyMessage(0, true, false)).toBe("No dead-letter entries");
    expect(deadLetterEmptyMessage(0, false, true)).toBe("Dead-letter data unavailable");
    expect(deadLetterEmptyMessage(3)).toBe(
      "Dead-letter entries exist, but recent details are unavailable",
    );
  });
});

describe("deadLetterHeadingText", () => {
  it("shows exact totals when dead-letter data is complete", () => {
    expect(deadLetterHeadingText(3, 3)).toBe("Dead Letters (3 total)");
    expect(deadLetterHeadingText(2, 3)).toBe("Dead Letters (3 total)");
  });

  it("shows loaded rows separately when retained dead-letter entries exceed the preview", () => {
    expect(deadLetterHeadingText(8, 5)).toBe("Dead Letters (5 loaded, 8 total)");
    expect(deadLetterHeadingText(3, 0)).toBe("Dead Letters (0 loaded, 3 total)");
  });

  it("shows loaded rows separately when dead-letter depth is unavailable", () => {
    expect(deadLetterHeadingText(0, 0, false, true)).toBe("Dead Letters (? total)");
    expect(deadLetterHeadingText(0, 2, false, true)).toBe("Dead Letters (2 loaded, ? total)");
  });

  it("shows loaded rows separately when entry details are partial", () => {
    expect(deadLetterHeadingText(5, 2, true, false)).toBe("Dead Letters (2 loaded, 5 total)");
  });
});

describe("deadLetterReasonText", () => {
  it("trims visible reasons and falls back for blank values", () => {
    expect(deadLetterReasonText(" worker failed ")).toBe("worker failed");
    expect(deadLetterReasonText("   ")).toBe("?");
    expect(deadLetterReasonText(null)).toBe("?");
  });
});

describe("deadLetterTaskIdDisplay", () => {
  it("keeps short task IDs intact and marks truncated IDs explicitly", () => {
    expect(deadLetterTaskIdDisplay("task-1")).toBe("task-1");
    expect(deadLetterTaskIdDisplay("task-1234567890abcdef")).toBe("task-1234567...");
    expect(deadLetterTaskIdDisplay("   ")).toBe("?");
  });
});

describe("deadLetterTaskTypeText", () => {
  it("formats known backend task types for display", () => {
    expect(deadLetterTaskTypeText("fix_pr")).toBe("Fix PR");
    expect(deadLetterTaskTypeText("FIX_CI")).toBe("Fix CI");
    expect(deadLetterTaskTypeText("classify_ci")).toBe("Classify CI");
    expect(deadLetterTaskTypeText("implement_issue")).toBe("Implement");
    expect(deadLetterTaskTypeText("triage_followups")).toBe("Triage");
    expect(deadLetterTaskTypeText("rebase_pr")).toBe("Rebase");
    expect(deadLetterTaskTypeText("improve")).toBe("Improve");
  });

  it("keeps unknown task types readable", () => {
    expect(deadLetterTaskTypeText(" custom_task ")).toBe("custom task");
    expect(deadLetterTaskTypeText("   ")).toBe("?");
  });
});

describe("DeadLetters", () => {
  it("renders blank reasons visibly and wraps long reason text", () => {
    const html = renderToStaticMarkup(
      createElement(DeadLetters, {
        entries: [entry({ reason: "   " })],
        total: 1,
      }),
    );

    expect(html).toContain("Dead Letters (1 total)");
    expect(html).toContain("aria-label=\"Dead-letter entries\"");
    expect(html).toContain("<th scope=\"col\" class=\"pb-2\">Reason</th>");
    expect(html).toContain("<th scope=\"row\" class=\"max-w-[18rem] py-2 pr-4 text-left font-mono font-normal\">");
    expect(html).toContain("<span class=\"sr-only\"> owner/repo</span>");
    expect(html).toContain(
      "<td class=\"max-w-[14rem] truncate py-2 pr-4\" title=\"fix_pr\">Fix PR</td>",
    );
    expect(html).toContain("break-words");
    expect(html).toContain("title=\"?\"");
    expect(html).toContain("<td class=\"max-w-[32rem] break-words py-2 text-red-400\" title=\"?\">?</td>");
  });

  it("keeps full task IDs available when the visible task cell is truncated", () => {
    const html = renderToStaticMarkup(
      createElement(DeadLetters, {
        entries: [entry({ task_id: "task-1234567890abcdef" })],
        total: 1,
      }),
    );

    expect(html).toContain("title=\"task-1234567890abcdef\"");
    expect(html).toContain("<span aria-hidden=\"true\">task-1234567...</span>");
    expect(html).toContain("<span class=\"sr-only\">task-1234567890abcdef</span>");
  });

  it("formats prefixed dead-letter streams while preserving the raw title", () => {
    const html = renderToStaticMarkup(
      createElement(DeadLetters, {
        entries: [entry({ dead_letter_stream: "project-a:dead-letter" })],
        total: 1,
      }),
    );

    expect(html).toContain(
      "title=\"project-a:dead-letter\"><span>[project-a] dead-letter</span>",
    );
    expect(html).toContain("<span class=\"sr-only\"> raw stream project-a:dead-letter</span>");
  });

  it("makes capped dead-letter previews explicit in the heading", () => {
    const html = renderToStaticMarkup(
      createElement(DeadLetters, {
        entries: [entry({ dead_letter_id: "dead-letter:1-0" })],
        total: 8,
      }),
    );

    expect(html).toContain("Dead Letters (1 loaded, 8 total)");
    expect(html).toContain("aria-label=\"Dead-letter entries\"");
  });

  it("does not claim data is unavailable when degraded entries have an exact zero total", () => {
    const html = renderToStaticMarkup(
      createElement(DeadLetters, {
        entries: [],
        total: 0,
        entriesDegraded: true,
        depthDegraded: false,
      }),
    );

    expect(html).toContain("Dead Letters (0 total)");
    expect(html).toContain("No dead-letter entries");
    expect(html).not.toContain("Dead-letter data unavailable");
  });
});

describe("DeadLetters timestamp formatting", () => {
  it("does not expose invalid Date strings", () => {
    expect(formatTimestampMs(Number.MAX_VALUE)).toBe("?");
  });
});
