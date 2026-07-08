import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import {
  ATTEMPT_COUNT_CELL_CLASS,
  AttemptCounts,
  attemptAttentionCounts,
  attemptAttentionLabel,
  attemptBudgetLabel,
  attemptBudgetTitle,
  attemptCountsSectionStatus,
} from "./AttemptCounts";

describe("attemptCountsSectionStatus", () => {
  it("stays quiet when retry counters are complete", () => {
    expect(attemptCountsSectionStatus(false, 0)).toBeNull();
    expect(attemptCountsSectionStatus(false, 3)).toBeNull();
  });

  it("distinguishes unavailable retry counters from partially loaded counters", () => {
    expect(attemptCountsSectionStatus(true, 0)).toBe("unavailable");
    expect(attemptCountsSectionStatus(true, 3)).toBe("partial counters");
  });
});

describe("AttemptCounts", () => {
  it("describes remaining retry budget for individual rows", () => {
    expect(attemptBudgetLabel(2, 3)).toBe("1 attempt left");
    expect(attemptBudgetTitle(2, 3)).toBe("2 of 3 attempts used; 1 attempt left");
    expect(attemptBudgetLabel(3, 3)).toBe("budget exhausted");
    expect(attemptBudgetTitle(3, 3)).toBe("3 of 3 attempts used; retry budget exhausted");
  });

  it("summarizes retry rows that need attention", () => {
    const counts = attemptAttentionCounts([
      { severity: "normal" },
      { severity: "warning" },
      { severity: "critical" },
      { severity: "critical" },
    ]);

    expect(counts).toEqual({ critical: 2, warning: 1 });
    expect(attemptAttentionLabel({ critical: 0, warning: 0 })).toBeNull();
    expect(attemptAttentionLabel({ critical: 1, warning: 0 })).toBe("1 at max");
    expect(attemptAttentionLabel({ critical: 0, warning: 2 })).toBe("2 near max");
    expect(attemptAttentionLabel(counts)).toBe("2 at max, 1 near max");
  });

  it("renders retry attempts as a named table with scoped column headers", () => {
    const html = renderToStaticMarkup(
      createElement(AttemptCounts, {
        attempts: { "PR owner/repo #42": 2 },
        maxAttempts: 3,
      }),
    );

    expect(html).toContain("aria-label=\"Retry attempts\"");
    expect(html).toContain("<th scope=\"col\" class=\"px-4 py-2 font-medium\">Resource</th>");
    expect(html).toContain("<th scope=\"col\" class=\"px-4 py-2 text-right font-medium\">State</th>");
    expect(html).toContain("<th scope=\"row\" class=\"max-w-[28rem] truncate px-4 py-2 text-left font-mono font-normal text-zinc-300\" title=\"PR owner/repo #42\">PR owner/repo #42</th>");
  });

  it("keeps large retry counters from widening the table", () => {
    const html = renderToStaticMarkup(
      createElement(AttemptCounts, {
        attempts: { "PR owner/repo #42": 123456789012345 },
        maxAttempts: 3,
      }),
    );

    expect(ATTEMPT_COUNT_CELL_CLASS).toContain("break-all");
    expect(html).toContain(
      `class="${ATTEMPT_COUNT_CELL_CLASS} text-red-300" title="123456789012345 of 3 attempts used; retry budget exhausted">123456789012345</td>`,
    );
  });

  it("shows remaining attempts in the row state", () => {
    const html = renderToStaticMarkup(
      createElement(AttemptCounts, {
        attempts: {
          "PR owner/repo #42": 2,
          "PR owner/repo #43": 3,
        },
        maxAttempts: 3,
      }),
    );

    expect(html).toContain("Attempts / 3");
    expect(html).toContain(
      'title="near max; 2 of 3 attempts used; 1 attempt left">1 attempt left</td>',
    );
    expect(html).toContain(
      'title="at max; 3 of 3 attempts used; retry budget exhausted">budget exhausted</td>',
    );
  });

  it("shows split section attention and warning summaries without hiding degraded state", () => {
    const html = renderToStaticMarkup(
      createElement(AttemptCounts, {
        attempts: {
          "PR owner/repo #42": 3,
          "PR owner/repo #43": 2,
          "PR owner/repo #44": 1,
        },
        maxAttempts: 3,
        degraded: true,
      }),
    );

    expect(html).toContain('<span class="text-xs text-red-300">1 at max</span>');
    expect(html).toContain('<span class="text-xs text-yellow-300">1 near max</span>');
    expect(html).toContain("partial counters");
  });
});
