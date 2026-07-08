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
});
