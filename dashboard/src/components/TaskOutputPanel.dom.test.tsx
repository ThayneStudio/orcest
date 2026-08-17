/**
 * @vitest-environment happy-dom
 */
import { cleanup, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { TaskOutputParams } from "../lib/types";
import type { TaskOutputClientState } from "../lib/taskOutput";
import { TaskOutputPanel } from "./TaskOutputPanel";

const mockUseTaskOutput = vi.hoisted(() => ({
  current: {
    lines: ["worker output line"],
    startIndex: 0,
    connected: true,
    retrying: false,
    done: false,
    error: null,
  } as TaskOutputClientState,
}));

vi.mock("../hooks/useTaskOutput", () => ({
  useTaskOutput: () => mockUseTaskOutput.current,
}));

const params: TaskOutputParams = {
  workerId: "worker-1",
  taskId: "task-live",
  historical: false,
};

describe("TaskOutputPanel DOM semantics", () => {
  beforeEach(() => {
    (globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
    mockUseTaskOutput.current = {
      lines: ["worker output line"],
      startIndex: 0,
      connected: true,
      retrying: false,
      done: false,
      error: null,
    };
  });

  afterEach(() => {
    cleanup();
  });

  it("exposes output as a named region with a live focusable log", async () => {
    const user = userEvent.setup();
    const onClose = vi.fn();

    render(
      <TaskOutputPanel
        id="task-output-worker-1-task-live-0"
        params={params}
        label="PR owner/repo #42"
        onClose={onClose}
      />,
    );

    const region = screen.getByRole("region", { name: "Output: PR owner/repo #42" });
    expect(region.id).toBe("task-output-worker-1-task-live-0");
    expect(screen.getByText("worker: worker-1 | task: task-live").getAttribute("title"))
      .toBe("worker: worker-1 | task: task-live");

    const log = screen.getByRole("log", { name: "Output log for PR owner/repo #42" });
    expect(log.getAttribute("aria-live")).toBe("polite");
    expect(log.getAttribute("aria-relevant")).toBe("additions text");
    expect(log.getAttribute("aria-atomic")).toBe("false");
    expect(log.getAttribute("tabindex")).toBe("0");
    await waitFor(() => expect(document.activeElement).toBe(log));
    expect(screen.getByText("worker output line")).toBeTruthy();
    const status = screen.getByRole("status");
    expect(status.textContent).toBe("Live");
    expect(status.getAttribute("aria-live")).toBe("polite");
    expect(status.getAttribute("aria-atomic")).toBe("true");

    await user.click(screen.getByRole("button", { name: "Close output for PR owner/repo #42" }));

    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it("closes from the focused output log with Escape", async () => {
    const user = userEvent.setup();
    const onClose = vi.fn();

    render(
      <TaskOutputPanel
        id="task-output-worker-1-task-live-0"
        params={params}
        label="PR owner/repo #42"
        onClose={onClose}
      />,
    );

    const log = screen.getByRole("log", { name: "Output log for PR owner/repo #42" });
    await waitFor(() => expect(document.activeElement).toBe(log));

    await user.keyboard("{Escape}");

    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it("leaves existing focus alone when log autofocus is disabled", async () => {
    const { rerender } = render(
      <div>
        <button type="button">External action</button>
      </div>,
    );

    const external = screen.getByRole("button", { name: "External action" });
    external.focus();

    rerender(
      <div>
        <button type="button">External action</button>
        <TaskOutputPanel
          id="task-output-worker-1-task-live-0"
          params={params}
          label="PR owner/repo #42"
          onClose={vi.fn()}
          autoFocusLog={false}
        />
      </div>,
    );

    await waitFor(() => expect(document.activeElement).toBe(external));
  });

  it("stops live announcements once output is complete", () => {
    mockUseTaskOutput.current = {
      lines: [],
      startIndex: 0,
      connected: true,
      retrying: false,
      done: true,
      error: null,
    };

    render(
      <TaskOutputPanel
        id="task-output-worker-1-task-live-0"
        params={params}
        label="PR owner/repo #42"
        onClose={vi.fn()}
      />,
    );

    expect(screen.getByRole("log", { name: "Output log for PR owner/repo #42" })
      .getAttribute("aria-live")).toBe("off");
    expect(screen.getByText("No output captured")).toBeTruthy();
  });

  it("announces terminal task output errors", () => {
    mockUseTaskOutput.current = {
      lines: [],
      startIndex: 0,
      connected: false,
      retrying: false,
      done: true,
      error: "Task output is unavailable.",
    };

    render(
      <TaskOutputPanel
        id="task-output-worker-1-task-live-0"
        params={params}
        label="PR owner/repo #42"
        onClose={vi.fn()}
      />,
    );

    expect(screen.getByRole("log", { name: "Output log for PR owner/repo #42" })
      .getAttribute("aria-live")).toBe("polite");
    expect(screen.getByRole("alert").textContent).toBe("Task output is unavailable.");
  });

  it("shows reconnecting copy before any output has arrived", () => {
    mockUseTaskOutput.current = {
      lines: [],
      startIndex: 0,
      connected: false,
      retrying: true,
      done: false,
      error: null,
    };

    render(
      <TaskOutputPanel
        id="task-output-worker-1-task-live-0"
        params={params}
        label="PR owner/repo #42"
        onClose={vi.fn()}
      />,
    );

    expect(screen.getByText("Reconnecting")).toBeTruthy();
    expect(screen.getByText("Reconnecting...")).toBeTruthy();
  });

  it("announces reconnecting status while retained output remains visible", () => {
    mockUseTaskOutput.current = {
      lines: ["worker output line"],
      startIndex: 0,
      connected: false,
      retrying: true,
      done: false,
      error: null,
    };

    render(
      <TaskOutputPanel
        id="task-output-worker-1-task-live-0"
        params={params}
        label="PR owner/repo #42"
        onClose={vi.fn()}
      />,
    );

    expect(screen.getByText("worker output line")).toBeTruthy();
    const status = screen.getByRole("status");
    expect(status.textContent).toBe("Reconnecting");
    expect(status.getAttribute("aria-live")).toBe("polite");
    expect(status.getAttribute("aria-atomic")).toBe("true");
  });

  it("keeps the full task ID available when the header display is truncated", () => {
    render(
      <TaskOutputPanel
        id="task-output-worker-1-task-long-0"
        params={{ ...params, taskId: "task-1234567890abcdef" }}
        label="PR owner/repo #42"
        onClose={vi.fn()}
      />,
    );

    expect(screen.getByText(/full task: task-1234567890abcdef/)).toBeTruthy();
  });
});
