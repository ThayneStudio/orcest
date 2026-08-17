/**
 * @vitest-environment happy-dom
 */
import { createElement } from "react";
import { act, cleanup, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ConnectionStatus } from "./ConnectionStatus";

function statusText(): string {
  return screen.getByRole("status").textContent || "";
}

describe("ConnectionStatus", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-20T00:00:10Z"));
    (globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
  });

  afterEach(() => {
    cleanup();
    vi.useRealTimers();
  });

  it("exposes connection state as a live status and ages into stale", () => {
    render(createElement(ConnectionStatus, {
      connected: true,
      error: null,
      lastUpdate: new Date("2026-06-20T00:00:00Z"),
    }));

    const status = screen.getByRole("status");
    expect(status.getAttribute("aria-live")).toBe("polite");
    expect(status.getAttribute("aria-atomic")).toBe("true");
    expect(statusText()).toBe("Connected");

    act(() => {
      vi.advanceTimersByTime(5000);
    });

    expect(statusText()).toBe("Stale snapshot");
    expect(screen.getByText("Last update 15s ago")).toBeTruthy();
  });

  it("shows connection issues with retained snapshot age", () => {
    render(createElement(ConnectionStatus, {
      connected: false,
      error: "socket closed",
      lastUpdate: new Date("2026-06-19T23:59:00Z"),
    }));

    expect(statusText()).toBe("Connection issue: socket closed");
    expect(screen.getByText("socket closed (Last update 1m 10s ago)")).toBeTruthy();
  });

  it("ages from the receipt clock and shows the server clock only in the tooltip", () => {
    // Receipt was 20s ago on the browser clock, but the server stamped the
    // snapshot 40s in the future. Freshness must follow the receipt clock.
    render(createElement(ConnectionStatus, {
      connected: true,
      error: null,
      lastUpdate: new Date("2026-06-19T23:59:50Z"),
      serverFetchedAt: new Date("2026-06-20T00:00:50Z"),
    }));

    expect(statusText()).toBe("Stale snapshot");
    expect(screen.getByText("Last update 20s ago")).toBeTruthy();

    const displayed = screen.getByTitle(
      "Snapshot generated 2026-06-20T00:00:50.000Z (server clock). " +
        "Age is measured from browser receipt time.",
    );
    expect(displayed.textContent).toBe(new Date("2026-06-19T23:59:50Z").toLocaleTimeString());
  });

  it("omits the server tooltip when no server timestamp is available", () => {
    render(createElement(ConnectionStatus, {
      connected: true,
      error: null,
      lastUpdate: new Date("2026-06-20T00:00:05Z"),
    }));

    expect(statusText()).toBe("Connected");
    expect(screen.queryByTitle(/server clock/)).toBeNull();
  });
});
