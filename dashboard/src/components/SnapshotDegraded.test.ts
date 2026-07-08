import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import {
  degradedSectionLabel,
  degradedSectionsSummary,
  SnapshotDegraded,
} from "./SnapshotDegraded";

describe("SnapshotDegraded", () => {
  it("renders nothing when there are no readable degraded sections", () => {
    const html = renderToStaticMarkup(
      createElement(SnapshotDegraded, { sections: [" ", ""] }),
    );

    expect(html).toBe("");
  });

  it("formats internal degraded section keys as operator-facing labels", () => {
    expect(degradedSectionLabel("dead-letter entries")).toBe("Dead-letter entries");
    expect(degradedSectionLabel(" queue depths ")).toBe("Task stream depths");
    expect(degradedSectionLabel("task_output-prefixes")).toBe("Task output lookup");
    expect(degradedSectionLabel("custom_section-name")).toBe("custom section name");
  });

  it("caps the visible degraded section summary", () => {
    expect(degradedSectionsSummary([
      "recent results",
      "active locks",
      "queue depths",
      "worker discovery",
      "dead-letter entries",
    ])).toBe(
      "Recent results, Active work, Task stream depths, Worker output discovery, and 1 more",
    );
  });

  it("deduplicates degraded sections by the visible operator label", () => {
    expect(degradedSectionsSummary([
      "recent results",
      " Recent Results ",
      "recent_results",
      "queue depths",
      "queue-depths",
    ])).toBe("Recent results, Task stream depths");
  });

  it("keeps the full degraded section list in the title", () => {
    const html = renderToStaticMarkup(
      createElement(SnapshotDegraded, {
        sections: [
          "recent results",
          "active locks",
          "queue depths",
          "worker discovery",
          "dead-letter entries",
        ],
      }),
    );

    expect(html).toContain("Dashboard data is partially unavailable");
    expect(html).toContain(
      "Incomplete sections: Recent results, Active work, Task stream depths, Worker output discovery, and 1 more",
    );
    expect(html).toContain(
      'title="Recent results, Active work, Task stream depths, Worker output discovery, Dead-letter entries"',
    );
  });
});
