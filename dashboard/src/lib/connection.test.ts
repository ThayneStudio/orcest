import { describe, expect, it } from "vitest";
import {
  formatSnapshotAge,
  snapshotCloseErrorMessage,
  snapshotAgeSeconds,
  snapshotConnectionStatus,
  snapshotDisplayTime,
  snapshotFetchedDate,
  snapshotServerTimeLabel,
} from "./connection";

describe("snapshotFetchedDate", () => {
  it("parses the server-stamped fetch time for display", () => {
    expect(snapshotFetchedDate("2026-06-16T12:00:00Z")?.toISOString()).toBe(
      "2026-06-16T12:00:00.000Z",
    );
  });

  it("returns null for missing or invalid payload times instead of inventing one", () => {
    expect(snapshotFetchedDate("")).toBeNull();
    expect(snapshotFetchedDate("   ")).toBeNull();
    expect(snapshotFetchedDate("not-a-date")).toBeNull();
    expect(snapshotFetchedDate(undefined)).toBeNull();
    expect(snapshotFetchedDate(null)).toBeNull();
  });
});

describe("snapshotServerTimeLabel", () => {
  it("discloses the server clock and that ages come from the browser clock", () => {
    const label = snapshotServerTimeLabel(new Date("2026-06-16T12:00:00Z"));

    expect(label).toBe(
      "Snapshot generated 2026-06-16T12:00:00.000Z (server clock). " +
        "Age is measured from browser receipt time.",
    );
  });

  it("hides the tooltip when no usable server time is available", () => {
    expect(snapshotServerTimeLabel(null)).toBeNull();
    expect(snapshotServerTimeLabel(undefined)).toBeNull();
    expect(snapshotServerTimeLabel(new Date("not-a-date"))).toBeNull();
  });
});

describe("snapshotAgeSeconds", () => {
  it("computes non-negative ages from the last update time", () => {
    const now = new Date("2026-06-16T12:00:20Z").getTime();

    expect(snapshotAgeSeconds(new Date("2026-06-16T12:00:00Z"), now)).toBe(20);
    expect(snapshotAgeSeconds(new Date("2026-06-16T12:00:30Z"), now)).toBe(0);
    expect(snapshotAgeSeconds(null, now)).toBeNull();
  });
});

describe("formatSnapshotAge", () => {
  it("formats compact snapshot ages", () => {
    expect(formatSnapshotAge(null)).toBeNull();
    expect(formatSnapshotAge(12)).toBe("12s ago");
    expect(formatSnapshotAge(60)).toBe("1m ago");
    expect(formatSnapshotAge(75)).toBe("1m 15s ago");
    expect(formatSnapshotAge(3600)).toBe("1h ago");
    expect(formatSnapshotAge(3660)).toBe("1h 1m ago");
  });
});

describe("snapshotDisplayTime", () => {
  it("formats valid snapshot times and hides missing or invalid times", () => {
    expect(snapshotDisplayTime(null)).toBeNull();
    expect(snapshotDisplayTime(new Date("not-a-date"))).toBeNull();
    expect(snapshotDisplayTime(new Date("2026-06-16T12:00:00Z"))).toMatch(/\d/);
  });
});

describe("snapshotConnectionStatus", () => {
  it("shows an initial connecting state before the first snapshot arrives", () => {
    expect(snapshotConnectionStatus(false, null, null)).toEqual({
      kind: "waiting",
      label: "Connecting",
      detail: null,
      announcement: "Connecting",
    });
  });

  it("distinguishes disconnected and connection issue states", () => {
    expect(snapshotConnectionStatus(false, "bad token", null)).toEqual({
      kind: "issue",
      label: "Connection issue",
      detail: "bad token",
      announcement: "Connection issue: bad token",
    });
    expect(snapshotConnectionStatus(false, "   ", null)).toEqual({
      kind: "waiting",
      label: "Connecting",
      detail: null,
      announcement: "Connecting",
    });
  });

  it("shows retained snapshot age while disconnected", () => {
    const now = new Date("2026-06-16T12:01:15Z").getTime();

    expect(snapshotConnectionStatus(
      false,
      null,
      new Date("2026-06-16T12:00:00Z"),
      now,
    )).toEqual({
      kind: "disconnected",
      label: "Disconnected",
      detail: "Last update 1m 15s ago",
      announcement: "Disconnected",
    });
    expect(snapshotConnectionStatus(
      false,
      "reconnecting",
      new Date("2026-06-16T12:00:00Z"),
      now,
    )).toEqual({
      kind: "issue",
      label: "Connection issue",
      detail: "reconnecting (Last update 1m 15s ago)",
      announcement: "Connection issue: reconnecting",
    });
  });

  it("marks connected snapshots stale after the configured threshold", () => {
    const now = new Date("2026-06-16T12:00:20Z").getTime();

    expect(snapshotConnectionStatus(
      true,
      null,
      new Date("2026-06-16T12:00:10Z"),
      now,
      15_000,
    )).toEqual({
      kind: "connected",
      label: "Connected",
      detail: null,
      announcement: "Connected",
    });
    expect(snapshotConnectionStatus(
      true,
      null,
      new Date("2026-06-16T12:00:05Z"),
      now,
      15_000,
    )).toEqual({
      kind: "stale",
      label: "Stale snapshot",
      detail: "Last update 15s ago",
      announcement: "Stale snapshot",
    });
  });

  it("does not report fully connected before the first valid snapshot", () => {
    expect(snapshotConnectionStatus(true, null, null)).toEqual({
      kind: "waiting",
      label: "Waiting for snapshot",
      detail: null,
      announcement: "Waiting for snapshot",
    });
    expect(snapshotConnectionStatus(true, null, new Date("not-a-date"))).toEqual({
      kind: "waiting",
      label: "Waiting for snapshot",
      detail: null,
      announcement: "Waiting for snapshot",
    });
  });
});

describe("snapshotCloseErrorMessage", () => {
  it("surfaces abnormal snapshot close failures immediately", () => {
    expect(snapshotCloseErrorMessage(1006, 1)).toBe(
      "Dashboard connection could not be opened. Refresh with a valid dashboard token or check connectivity.",
    );
  });

  it("ignores normal snapshot closes", () => {
    expect(snapshotCloseErrorMessage(1000, 1)).toBeNull();
  });
});
