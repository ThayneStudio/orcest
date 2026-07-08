import { describe, expect, it } from "vitest";
import {
  providerMetricLabel,
  providerMetricSeverity,
  providerMetricTone,
} from "./providerHealth";

describe("providerMetricLabel", () => {
  it("formats known and unknown provider metrics", () => {
    expect(providerMetricLabel("credential_refresh_failures")).toBe("Credential refresh failures");
    expect(providerMetricLabel("exhausted_skip")).toBe("Exhausted skips");
    expect(providerMetricLabel("refresh_failures")).toBe("Refresh failures");
    expect(providerMetricLabel("rebake_required_failures")).toBe("Rebake failures");
    expect(providerMetricLabel("custom_counter")).toBe("custom counter");
  });
});

describe("providerMetricSeverity", () => {
  it("treats zero and invalid values as normal", () => {
    expect(providerMetricSeverity("exhausted_skip", 0)).toBe("normal");
    expect(providerMetricSeverity("exhausted_skip", Number.NaN)).toBe("normal");
  });

  it("distinguishes exhausted skips from rebake failures", () => {
    expect(providerMetricSeverity("exhausted_skip", 2)).toBe("warning");
    expect(providerMetricSeverity("credential_refresh_failures", 1)).toBe("warning");
    expect(providerMetricSeverity("refresh_failures", 2)).toBe("warning");
    expect(providerMetricSeverity("rebake_required_failures", 1)).toBe("critical");
  });

  it("does not mark unknown positive counters unhealthy by default", () => {
    expect(providerMetricSeverity("tasks_completed", 10)).toBe("normal");
  });
});

describe("providerMetricTone", () => {
  it("maps provider metric severity to text tones", () => {
    expect(providerMetricTone("exhausted_skip", 0)).toBe("text-zinc-500");
    expect(providerMetricTone("exhausted_skip", 1)).toBe("text-yellow-300");
    expect(providerMetricTone("credential_refresh_failures", 1)).toBe("text-yellow-300");
    expect(providerMetricTone("refresh_failures", 1)).toBe("text-yellow-300");
    expect(providerMetricTone("rebake_required_failures", 1)).toBe("text-red-300");
    expect(providerMetricTone("tasks_completed", 1)).toBe("text-zinc-500");
  });
});
