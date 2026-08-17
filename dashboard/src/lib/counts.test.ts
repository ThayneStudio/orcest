import { describe, expect, it } from "vitest";
import { degradedCountLabel, loadedCountLabel } from "./counts";

describe("degradedCountLabel", () => {
  it("shows exact counts when data is complete", () => {
    expect(degradedCountLabel(0, false)).toBe("0");
    expect(degradedCountLabel(3, false)).toBe("3");
  });

  it("marks empty degraded counts as unknown and non-empty degraded counts as loaded", () => {
    expect(degradedCountLabel(0, true)).toBe("?");
    expect(degradedCountLabel(3, true)).toBe("3 loaded");
  });
});

describe("loadedCountLabel", () => {
  it("reports loaded rows exactly even when the total is unknown", () => {
    expect(loadedCountLabel(0)).toBe("0 loaded");
    expect(loadedCountLabel(3)).toBe("3 loaded");
    expect(loadedCountLabel(0, true)).toBe("0 loaded");
  });
});
