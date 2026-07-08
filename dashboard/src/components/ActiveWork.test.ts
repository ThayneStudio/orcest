/**
 * @vitest-environment happy-dom
 */
import { createElement } from "react";
import { cleanup, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { LockInfo, TaskOutputParams } from "../lib/types";
import { taskOutputDomId } from "../lib/taskOutput";
import {
  ActiveWork,
  activeWorkEmptyMessage,
  activeWorkTaskIdDisplay,
  activeWorkTaskOutputPanelKey,
} from "./ActiveWork";

const mockUseTaskOutput = vi.hoisted(() => ({
  current: {
    lines: ["worker output line"],
    startIndex: 0,
    connected: true,
    retrying: false,
    done: false,
    error: null,
  },
  params: null as unknown,
}));

vi.mock("../hooks/useTaskOutput", () => ({
  useTaskOutput: (params: unknown) => {
    mockUseTaskOutput.params = params;
    return mockUseTaskOutput.current;
  },
}));

const ACTIVE_VIEW_LABEL = "View output for PR owner/repo #42 (orcest-worker-300, task task-live)";
const ACTIVE_HIDE_LABEL = "Hide output for PR owner/repo #42 (orcest-worker-300, task task-live)";

function lock(overrides: Partial<LockInfo> = {}): LockInfo {
  return {
    lock_key: "lock:pr:owner/repo:42",
    prefix: null,
    resource: "owner/repo:42",
    resource_type: "pr",
    repo: "owner/repo",
    resource_id: "42",
    owner: "orcest-worker-300",
    ttl: 180,
    task_id: "task-live",
    pending_created_at: null,
    ...overrides,
  };
}

describe("activeWorkEmptyMessage", () => {
  it("does not claim no active locks when lock discovery is degraded", () => {
    expect(activeWorkEmptyMessage(false)).toBe("No active locks");
    expect(activeWorkEmptyMessage(true)).toBe("Active locks unavailable");
  });
});

describe("activeWorkTaskOutputPanelKey", () => {
  it("keeps output panels distinct across Redis prefix modes", () => {
    const base = {
      workerId: "worker-1",
      taskId: "task-live",
      historical: false,
    };

    expect(activeWorkTaskOutputPanelKey({ ...base }))
      .not.toBe(activeWorkTaskOutputPanelKey({ ...base, prefix: null }));
    expect(activeWorkTaskOutputPanelKey({ ...base, prefix: "project-a" }))
      .not.toBe(activeWorkTaskOutputPanelKey({ ...base, prefix: "project-b" }));
  });
});

describe("activeWorkTaskIdDisplay", () => {
  it("keeps short task IDs intact and marks missing or long IDs clearly", () => {
    expect(activeWorkTaskIdDisplay("task-live")).toBe("task-live");
    expect(activeWorkTaskIdDisplay(null)).toBe("?");
    expect(activeWorkTaskIdDisplay("   ")).toBe("?");
    expect(activeWorkTaskIdDisplay("task-1234567890abcdef")).toBe("task-1234567890a...");
  });
});

describe("ActiveWork", () => {
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
    mockUseTaskOutput.params = null;
  });

  afterEach(() => {
    cleanup();
  });

  it("renders output-capable locks with collapsed accessible controls", () => {
    render(createElement(ActiveWork, { locks: [lock()], stuckTasks: [] }));

    expect(screen.getByRole("heading", { name: "Active Work (1)" })).toBeTruthy();
    expect(screen.getByText("orcest-worker-300")).toBeTruthy();
    expect(screen.getByRole("table", { name: "Active work" })).toBeTruthy();
    expect(screen.getByRole("columnheader", { name: "Resource" }).getAttribute("scope"))
      .toBe("col");
    expect(screen.getByRole("columnheader", { name: "Task" }).getAttribute("scope"))
      .toBe("col");
    expect(screen.getByRole("rowheader", { name: "PR #42 owner/repo" }).getAttribute("scope"))
      .toBe("row");
    expect(screen.getByText("task-live")).toBeTruthy();

    const outputButton = screen.getByRole("button", {
      name: ACTIVE_VIEW_LABEL,
    });
    expect(outputButton.textContent).toBe("View output");
    expect(outputButton.getAttribute("aria-expanded")).toBe("false");
    expect(outputButton.getAttribute("aria-controls")).toBeNull();
  });

  it("keeps full active task IDs available when the visible table cell is truncated", () => {
    const longTaskId = "task-1234567890abcdef";
    render(createElement(ActiveWork, {
      locks: [lock({ task_id: longTaskId })],
      stuckTasks: [],
    }));

    const taskCell = screen.getByTitle(longTaskId);
    expect(taskCell.querySelector("[aria-hidden='true']")?.textContent)
      .toBe("task-1234567890a...");
    expect(screen.getByText(longTaskId).className).toBe("sr-only");
  });

  it("opens and closes the task output panel from the row control", async () => {
    const user = userEvent.setup();
    render(createElement(ActiveWork, { locks: [lock()], stuckTasks: [] }));

    await user.click(screen.getByRole("button", {
      name: ACTIVE_VIEW_LABEL,
    }));

    const selectedButton = screen.getByRole("button", {
      name: ACTIVE_HIDE_LABEL,
    });
    expect(selectedButton.getAttribute("aria-expanded")).toBe("true");
    expect(selectedButton.getAttribute("aria-controls"))
      .toBe(taskOutputDomId({
        workerId: "orcest-worker-300",
        taskId: "task-live",
        historical: false,
        prefix: null,
        instanceId: "lock:pr:owner/repo:42",
      }, "active-output"));
    expect(screen.getByRole("region", { name: "Output: PR owner/repo #42" })).toBeTruthy();
    expect(screen.getByRole("log", { name: "Output log for PR owner/repo #42" })).toBeTruthy();
    expect(screen.getByText("worker output line")).toBeTruthy();

    await user.click(screen.getByRole("button", {
      name: "Close output for PR owner/repo #42",
    }));

    const outputButton = screen.getByRole("button", { name: ACTIVE_VIEW_LABEL });
    expect(outputButton.getAttribute("aria-expanded")).toBe("false");
    expect(screen.queryByRole("region", { name: "Output: PR owner/repo #42" })).toBeNull();
    await waitFor(() => expect(document.activeElement).toBe(outputButton));
  });

  it("closes task output with Escape and restores focus to the row control", async () => {
    const user = userEvent.setup();
    render(createElement(ActiveWork, { locks: [lock()], stuckTasks: [] }));

    await user.click(screen.getByRole("button", {
      name: ACTIVE_VIEW_LABEL,
    }));
    const outputLog = screen.getByRole("log", { name: "Output log for PR owner/repo #42" });
    await waitFor(() => expect(document.activeElement).toBe(outputLog));

    await user.keyboard("{Escape}");

    const outputButton = screen.getByRole("button", { name: ACTIVE_VIEW_LABEL });
    expect(screen.queryByRole("region", { name: "Output: PR owner/repo #42" })).toBeNull();
    await waitFor(() => expect(document.activeElement).toBe(outputButton));
  });

  it("keeps active output mounted when the resolved output prefix changes", async () => {
    const user = userEvent.setup();
    const selectedLock = lock({ prefix: "project-a" });
    const { rerender } = render(
      createElement(ActiveWork, { locks: [selectedLock], stuckTasks: [] }),
    );

    await user.click(screen.getByRole("button", {
      name: "View output for [project-a] PR owner/repo #42 (orcest-worker-300, prefix project-a, task task-live)",
    }));
    expect(screen.getByRole("region", { name: "Output: [project-a] PR owner/repo #42" })).toBeTruthy();

    rerender(createElement(ActiveWork, {
      locks: [{ ...selectedLock, output_prefix: "orcest" }],
      stuckTasks: [],
    }));

    await waitFor(() => {
      const selectedButton = screen.getByRole("button", {
        name: "Hide output for [project-a] PR owner/repo #42 (orcest-worker-300, prefix orcest, task task-live)",
      });
      expect(selectedButton.getAttribute("aria-controls"))
        .toBe(taskOutputDomId({
          workerId: "orcest-worker-300",
          taskId: "task-live",
          historical: false,
          prefix: "orcest",
          instanceId: "lock:pr:owner/repo:42",
        }, "active-output"));
      expect((mockUseTaskOutput.params as TaskOutputParams | null)?.prefix).toBe("orcest");
    });
    expect(screen.getByRole("region", { name: "Output: [project-a] PR owner/repo #42" })).toBeTruthy();
  });

  it("does not steal unrelated focus when a refreshed lock retargets output", async () => {
    const user = userEvent.setup();
    const selectedLock = lock({ prefix: "project-a" });
    const view = (locks: LockInfo[]) =>
      createElement("div", null,
        createElement("button", { type: "button" }, "External action"),
        createElement(ActiveWork, { locks, stuckTasks: [] }),
      );
    const { rerender } = render(view([selectedLock]));

    await user.click(screen.getByRole("button", {
      name: "View output for [project-a] PR owner/repo #42 (orcest-worker-300, prefix project-a, task task-live)",
    }));
    const external = screen.getByRole("button", { name: "External action" });
    external.focus();
    expect(document.activeElement).toBe(external);

    rerender(view([{ ...selectedLock, output_prefix: "orcest" }]));

    await waitFor(() => {
      expect((mockUseTaskOutput.params as TaskOutputParams | null)?.prefix).toBe("orcest");
    });
    expect(screen.getByRole("region", { name: "Output: [project-a] PR owner/repo #42" })).toBeTruthy();
    await waitFor(() => expect(document.activeElement).toBe(external));
  });

  it("clears mounted task output when the selected lock is no longer visible", async () => {
    const user = userEvent.setup();
    const { rerender } = render(
      createElement(ActiveWork, { locks: [lock()], stuckTasks: [] }),
    );

    await user.click(screen.getByRole("button", {
      name: ACTIVE_VIEW_LABEL,
    }));
    expect(screen.getByRole("region", { name: "Output: PR owner/repo #42" })).toBeTruthy();
    const outputLog = screen.getByRole("log", { name: "Output log for PR owner/repo #42" });
    outputLog.focus();
    expect(document.activeElement).toBe(outputLog);

    rerender(createElement(ActiveWork, { locks: [], stuckTasks: [] }));

    await waitFor(() =>
      expect(screen.queryByRole("region", { name: "Output: PR owner/repo #42" })).toBeNull()
    );
    const heading = screen.getByRole("heading", { name: "Active Work (0)" });
    expect(screen.getByText("No active locks")).toBeTruthy();
    await waitFor(() => expect(document.activeElement).toBe(heading));
  });

  it("restores focus when a refreshed lock removal hides a focused output trigger", async () => {
    const user = userEvent.setup();
    const { rerender } = render(
      createElement(ActiveWork, { locks: [lock()], stuckTasks: [] }),
    );

    await user.click(screen.getByRole("button", {
      name: ACTIVE_VIEW_LABEL,
    }));
    const selectedButton = screen.getByRole("button", {
      name: ACTIVE_HIDE_LABEL,
    });
    selectedButton.focus();
    expect(document.activeElement).toBe(selectedButton);

    rerender(createElement(ActiveWork, { locks: [], stuckTasks: [] }));

    await waitFor(() =>
      expect(screen.queryByRole("region", { name: "Output: PR owner/repo #42" })).toBeNull()
    );
    const heading = screen.getByRole("heading", { name: "Active Work (0)" });
    await waitFor(() => expect(document.activeElement).toBe(heading));
  });

  it("does not steal unrelated focus when refreshed data removes selected output", async () => {
    const user = userEvent.setup();
    const view = (locks: LockInfo[]) =>
      createElement("div", null,
        createElement("button", { type: "button" }, "External action"),
        createElement(ActiveWork, { locks, stuckTasks: [] }),
      );
    const { rerender } = render(view([lock()]));

    await user.click(screen.getByRole("button", {
      name: ACTIVE_VIEW_LABEL,
    }));
    const external = screen.getByRole("button", { name: "External action" });
    external.focus();
    expect(document.activeElement).toBe(external);

    rerender(view([]));

    await waitFor(() =>
      expect(screen.queryByRole("region", { name: "Output: PR owner/repo #42" })).toBeNull()
    );
    await waitFor(() =>
      expect(document.activeElement).toBe(screen.getByRole("button", { name: "External action" }))
    );
  });

  it("refreshes the mounted output label when the selected task row changes resource", async () => {
    const user = userEvent.setup();
    const { rerender } = render(
      createElement(ActiveWork, { locks: [lock()], stuckTasks: [] }),
    );

    await user.click(screen.getByRole("button", { name: ACTIVE_VIEW_LABEL }));
    expect(screen.getByRole("region", { name: "Output: PR owner/repo #42" })).toBeTruthy();

    rerender(createElement(ActiveWork, {
      locks: [lock({
        lock_key: "lock:pr:owner/repo:43",
        resource: "owner/repo:43",
        resource_id: "43",
      })],
      stuckTasks: [],
    }));

    expect(screen.queryByRole("region", { name: "Output: PR owner/repo #42" })).toBeNull();
    expect(screen.getByRole("region", { name: "Output: PR owner/repo #43" })).toBeTruthy();
    expect(screen.getByRole("button", {
      name: "Close output for PR owner/repo #43",
    })).toBeTruthy();
  });

  it("renders degraded empty state with an unknown count", () => {
    render(createElement(ActiveWork, { locks: [], stuckTasks: [], degraded: true }));

    expect(screen.getByRole("heading", { name: "Active Work (?)" })).toBeTruthy();
    expect(screen.getByText("Active locks unavailable")).toBeTruthy();
  });
});
