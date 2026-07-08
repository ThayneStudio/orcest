import { afterEach, describe, expect, it, vi } from "vitest";
import { formatDuration, formatTimestampMs, formatTtl, timeAgo } from "./format";

describe("formatTtl", () => {
  it("formats finite TTL values", () => {
    expect(formatTtl(12)).toBe("12s");
    expect(formatTtl(75)).toBe("1m 15s");
    expect(formatTtl(3661)).toBe("1h 1m");
  });

  it("handles Redis sentinel and invalid TTL values", () => {
    expect(formatTtl(-1)).toBe("no TTL");
    expect(formatTtl(-2)).toBe("expired");
    expect(formatTtl(Number.NaN)).toBe("unknown");
    expect(formatTtl(Number.POSITIVE_INFINITY)).toBe("unknown");
  });

  it("floors fractional TTLs before formatting", () => {
    expect(formatTtl(59.9)).toBe("59s");
  });
});

describe("formatDuration", () => {
  it("formats finite durations", () => {
    expect(formatDuration(12)).toBe("12s");
    expect(formatDuration(75)).toBe("1m 15s");
    expect(formatDuration(3661)).toBe("1h 1m");
  });

  it("does not leak invalid numeric output", () => {
    expect(formatDuration(Number.NaN)).toBe("unknown");
    expect(formatDuration(Number.POSITIVE_INFINITY)).toBe("unknown");
    expect(formatDuration(-1)).toBe("unknown");
  });
});

describe("formatTimestampMs", () => {
  it("formats valid timestamps", () => {
    expect(formatTimestampMs(0)).not.toBe("?");
  });

  it("hides missing, non-finite, and out-of-range timestamps", () => {
    expect(formatTimestampMs(null)).toBe("?");
    expect(formatTimestampMs(Number.NaN)).toBe("?");
    expect(formatTimestampMs(Number.POSITIVE_INFINITY)).toBe("?");
    expect(formatTimestampMs(Number.MAX_VALUE)).toBe("?");
  });

  it("hides negative and fractional timestamps instead of rendering misleading dates", () => {
    expect(formatTimestampMs(-1)).toBe("?");
    expect(formatTimestampMs(123.4)).toBe("?");
  });
});

describe("timeAgo", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("formats relative timestamps", () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-16T12:00:00Z"));

    expect(timeAgo("2026-06-16T11:59:30Z")).toBe("just now");
    expect(timeAgo("2026-06-16T11:42:00Z")).toBe("18m ago");
    expect(timeAgo("2026-06-16T09:00:00Z")).toBe("3h ago");
    expect(timeAgo("2026-06-14T11:00:00Z")).toBe("2d ago");
  });

  it("hides invalid and missing timestamps", () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-16T12:00:00Z"));

    expect(timeAgo(null)).toBe("");
    expect(timeAgo("not-a-date")).toBe("");
  });

  it("clamps future timestamps to just now", () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-16T12:00:00Z"));

    expect(timeAgo("2026-06-16T12:05:00Z")).toBe("just now");
  });
});
