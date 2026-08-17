import { describe, expect, it } from "vitest";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import {
  PROVIDER_METRIC_VALUE_CLASS,
  ProviderHealth,
  providerHealthAttentionCount,
  providerHealthAttentionCounts,
  providerHealthCardStatus,
  providerHealthCardStatusLabel,
  providerHealthCriticalLabel,
  providerHealthSectionStatus,
  providerHealthWarningLabel,
} from "./ProviderHealth";

describe("providerHealthSectionStatus", () => {
  it("stays quiet when provider counters are complete", () => {
    expect(providerHealthSectionStatus(false, 0)).toBeNull();
    expect(providerHealthSectionStatus(false, 2)).toBeNull();
  });

  it("distinguishes unavailable counters from partially loaded counters", () => {
    expect(providerHealthSectionStatus(true, 0)).toBe("unavailable");
    expect(providerHealthSectionStatus(true, 2)).toBe("partial counters");
  });
});

describe("provider health card status", () => {
  it("summarizes the highest metric severity on the provider card", () => {
    expect(providerHealthCardStatus([["exhausted_skip", 0]])).toBe("normal");
    expect(providerHealthCardStatus([["tasks_completed", 5]])).toBe("normal");
    expect(providerHealthCardStatus([["exhausted_skip", 2]])).toBe("warning");
    expect(providerHealthCardStatus([["credential_refresh_failures", 1]])).toBe("warning");
    expect(providerHealthCardStatus([["refresh_failures", 2]])).toBe("warning");
    expect(providerHealthCardStatus([
      ["exhausted_skip", 2],
      ["rebake_required_failures", 1],
    ])).toBe("critical");
  });

  it("labels warning cards separately from critical attention cards", () => {
    expect(providerHealthCardStatusLabel("critical", 2)).toBe("attention");
    expect(providerHealthCardStatusLabel("warning", 1)).toBe("warning");
    expect(providerHealthCardStatusLabel("normal", 2)).toBe("2 metrics");
  });

  it("summarizes providers that need attention", () => {
    expect(providerHealthAttentionCounts(["normal", "warning", "critical"]))
      .toEqual({ critical: 1, warning: 1 });
    expect(providerHealthAttentionCount(["normal", "warning", "critical"])).toBe(2);
    expect(providerHealthCriticalLabel(0)).toBeNull();
    expect(providerHealthCriticalLabel(1)).toBe("1 needs attention");
    expect(providerHealthCriticalLabel(2)).toBe("2 need attention");
    expect(providerHealthWarningLabel(0)).toBeNull();
    expect(providerHealthWarningLabel(1)).toBe("1 warning");
    expect(providerHealthWarningLabel(2)).toBe("2 warnings");
  });

  it("sorts providers needing attention before healthy counters", () => {
    const html = renderToStaticMarkup(createElement(ProviderHealth, {
      health: {
        z_normal: { exhausted_skip: 0 },
        a_warning: { exhausted_skip: 2 },
        m_critical: { rebake_required_failures: 1 },
      },
    }));

    expect(html.indexOf("m_critical")).toBeLessThan(html.indexOf("a_warning"));
    expect(html.indexOf("a_warning")).toBeLessThan(html.indexOf("z_normal"));
    expect(html).toContain("border-red-500/30");
    expect(html).toContain("border-yellow-500/30");
    expect(html).toContain(">attention<");
    expect(html).toContain(">warning<");
  });

  it("sorts metric rows by severity before metric name within each provider", () => {
    const html = renderToStaticMarkup(createElement(ProviderHealth, {
      health: {
        claude: {
          credential_refresh_failures: 2,
          exhausted_skip: 2,
          refresh_failures: 2,
          rebake_required_failures: 1,
          tasks_completed: 5,
        },
      },
    }));

    expect(html.indexOf("Rebake failures")).toBeLessThan(html.indexOf("Exhausted skips"));
    expect(html.indexOf("Credential refresh failures")).toBeLessThan(html.indexOf("Exhausted skips"));
    expect(html.indexOf("Exhausted skips")).toBeLessThan(html.indexOf("Refresh failures"));
    expect(html.indexOf("Refresh failures")).toBeLessThan(html.indexOf("tasks completed"));
    expect(html.indexOf("Exhausted skips")).toBeLessThan(html.indexOf("tasks completed"));
  });

  it("shows split section attention and warning summaries without hiding degraded state", () => {
    const html = renderToStaticMarkup(createElement(ProviderHealth, {
      health: {
        z_normal: { exhausted_skip: 0 },
        a_warning: { exhausted_skip: 2 },
        m_critical: { rebake_required_failures: 1 },
      },
      degraded: true,
    }));

    expect(html).toContain('<span class="text-xs text-red-300">1 needs attention</span>');
    expect(html).toContain('<span class="text-xs text-yellow-300">1 warning</span>');
    expect(html).toContain("partial counters");
  });

  it("keeps large metric values from widening provider cards", () => {
    const html = renderToStaticMarkup(createElement(ProviderHealth, {
      health: {
        claude: {
          tasks_completed: 123456789012345,
        },
      },
    }));

    expect(PROVIDER_METRIC_VALUE_CLASS).toContain("break-all");
    expect(html).toContain(
      `class="${PROVIDER_METRIC_VALUE_CLASS} text-zinc-500" title="123456789012345">123456789012345</span>`,
    );
  });
});
