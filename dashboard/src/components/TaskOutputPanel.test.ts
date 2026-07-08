import { describe, expect, it } from "vitest";
import {
  TASK_OUTPUT_PANEL_DEFAULT_CLASS,
  taskOutputBodyMode,
  taskOutputEmptyMessage,
  taskOutputHeaderStatus,
  taskOutputMetaTitle,
  taskOutputPanelShouldCloseOnKey,
  taskOutputPrefixDisplay,
  taskOutputTaskIdDisplay,
} from "./TaskOutputPanel";

describe("TaskOutputPanel layout classes", () => {
  it("uses shrinkable flex sizing by default", () => {
    expect(TASK_OUTPUT_PANEL_DEFAULT_CLASS).toContain("min-h-0");
    expect(TASK_OUTPUT_PANEL_DEFAULT_CLASS).toContain("flex-1");
    expect(TASK_OUTPUT_PANEL_DEFAULT_CLASS).not.toContain("h-1/2");
  });
});

describe("taskOutputBodyMode", () => {
  it("keeps retained output visible when an error arrives later", () => {
    expect(taskOutputBodyMode(2, "Task output is unavailable.")).toBe("lines");
  });

  it("uses an error-only body when no output was retained", () => {
    expect(taskOutputBodyMode(0, "Task output is unavailable.")).toBe("error-only");
  });

  it("shows the empty state before output or errors arrive", () => {
    expect(taskOutputBodyMode(0, null)).toBe("empty");
  });

  it("shows a completed empty stream as captured but blank", () => {
    expect(taskOutputEmptyMessage(false, true)).toBe("No output captured");
  });

  it("distinguishes reconnecting from the initial empty connection state", () => {
    expect(taskOutputEmptyMessage(false, false)).toBe("Connecting...");
    expect(taskOutputEmptyMessage(false, false, true)).toBe("Reconnecting...");
  });
});

describe("taskOutputHeaderStatus", () => {
  it("shows retained output as reconnecting when the stream drops mid-task", () => {
    expect(taskOutputHeaderStatus(false, false, null, 3)).toBe("reconnecting");
  });

  it("keeps explicit terminal states ahead of connection state", () => {
    expect(taskOutputHeaderStatus(true, false, "Task output is unavailable.", 3)).toBe("unavailable");
    expect(taskOutputHeaderStatus(true, true, null, 3)).toBe("complete");
    expect(taskOutputHeaderStatus(true, false, null, 0)).toBe("live");
    expect(taskOutputHeaderStatus(false, false, null, 0, true)).toBe("reconnecting");
    expect(taskOutputHeaderStatus(false, false, null, 0)).toBeNull();
  });
});

describe("task output header metadata", () => {
  it("formats explicit task-output prefixes", () => {
    expect(taskOutputPrefixDisplay(undefined)).toBeNull();
    expect(taskOutputPrefixDisplay(null)).toBe("unprefixed");
    expect(taskOutputPrefixDisplay(" project-a ")).toBe("project-a");
  });

  it("does not add an ellipsis to short task IDs", () => {
    expect(taskOutputTaskIdDisplay("task-live")).toBe("task-live");
  });

  it("truncates long task IDs only in the visible label", () => {
    expect(taskOutputTaskIdDisplay("task-1234567890abcdef")).toBe("task-1234567890a...");
  });

  it("keeps the full worker and task identity in the title", () => {
    expect(taskOutputMetaTitle("worker-1", "task-1234567890abcdef")).toBe(
      "worker: worker-1 | task: task-1234567890abcdef",
    );
    expect(taskOutputMetaTitle("worker-1", "task-1", "project-a")).toBe(
      "prefix: project-a | worker: worker-1 | task: task-1",
    );
    expect(taskOutputMetaTitle("worker-1", null)).toBe("worker: worker-1");
  });
});

describe("taskOutputPanelShouldCloseOnKey", () => {
  it("only treats Escape as a panel-close command", () => {
    expect(taskOutputPanelShouldCloseOnKey("Escape")).toBe(true);
    expect(taskOutputPanelShouldCloseOnKey("Enter")).toBe(false);
    expect(taskOutputPanelShouldCloseOnKey(" ")).toBe(false);
  });
});
