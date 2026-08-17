import { describe, expect, it } from "vitest";
import {
  attemptLimit,
  normalizeAttemptCountMap,
  normalizeAttemptCounts,
  remainingAttempts,
} from "./attempts";

describe("normalizeAttemptCounts", () => {
  it("sorts attempts by highest count and then label", () => {
    expect(normalizeAttemptCounts({
      "owner/b PR #2": 1,
      "owner/a PR #1": 3,
      "owner/c PR #3": 3,
    })).toEqual([
      { label: "owner/a PR #1", count: 3, severity: "critical" },
      { label: "owner/c PR #3", count: 3, severity: "critical" },
      { label: "owner/b PR #2", count: 1, severity: "normal" },
    ]);
  });

  it("classifies attempts near and at the max retry threshold", () => {
    expect(normalizeAttemptCounts({
      one: 1,
      two: 2,
      three: 3,
    }).map((row) => row.severity)).toEqual([
      "critical",
      "warning",
      "normal",
    ]);
  });

  it("supports a configured max retry threshold", () => {
    expect(normalizeAttemptCounts({
      three: 3,
      four: 4,
      five: 5,
    }, 5).map((row) => row.severity)).toEqual([
      "critical",
      "warning",
      "normal",
    ]);
  });

  it("normalizes the retry limit before computing severity and remaining attempts", () => {
    expect(attemptLimit(4.8)).toBe(4);
    expect(attemptLimit(0)).toBe(3);
    expect(attemptLimit(Number.NaN)).toBe(3);
    expect(remainingAttempts(2, 3)).toBe(1);
    expect(remainingAttempts(5, 3)).toBe(0);
    expect(remainingAttempts(-1, 3)).toBe(3);
  });

  it("drops empty, non-finite, fractional, and zero attempt rows", () => {
    expect(normalizeAttemptCounts({
      "": 3,
      zero: 0,
      fractional: 0.8,
      high_fractional: 2.9,
      negative: -1,
      nan: Number.NaN,
      boolean: true,
      array: [2],
      valid: 1,
    })).toEqual([
      { label: "valid", count: 1, severity: "normal" },
    ]);
  });

  it("trims labels and accepts numeric strings", () => {
    expect(normalizeAttemptCounts({
      " owner/repo PR #1 ": "2",
    })).toEqual([
      { label: "owner/repo PR #1", count: 2, severity: "warning" },
    ]);
  });

  it("collapses duplicate trimmed labels to the highest count", () => {
    expect(normalizeAttemptCounts({
      " owner/repo PR #1 ": "2",
      "owner/repo PR #1": "3",
      "owner/repo PR #2": "1",
    })).toEqual([
      { label: "owner/repo PR #1", count: 3, severity: "critical" },
      { label: "owner/repo PR #2", count: 1, severity: "normal" },
    ]);
  });
});

describe("normalizeAttemptCountMap", () => {
  it("normalizes to positive integer values keyed by trimmed label", () => {
    expect(normalizeAttemptCountMap({
      " owner/repo PR #1 ": "2",
      "owner/repo PR #1": 1,
      "owner/repo PR #fractional": "2.9",
      "owner/repo PR #2": "bad",
      "owner/repo PR #3": 0,
    })).toEqual({
      "owner/repo PR #1": 2,
    });
  });
});
