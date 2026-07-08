/**
 * @vitest-environment happy-dom
 */
import { cleanup, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { LockInfo, RecentResult, SystemSnapshot, TaskOutputParams } from "../lib/types";
import { taskOutputDomId } from "../lib/taskOutput";
import { Kanban, KANBAN_RESULT_COLUMN_LIMIT } from "./Kanban";

const mockUseTaskOutput = vi.hoisted(() => ({
  current: {
    lines: ["kanban output line"],
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

const KANBAN_VIEW_NAME = "View output for PR owner/repo #42 (worker-1, task task-live)";
const KANBAN_HIDE_NAME = "Hide output for PR owner/repo #42 (worker-1, task task-live)";
const KANBAN_HISTORICAL_VIEW_NAME =
  "View output for PR owner/repo #42 (worker-1, task task-historical, historical)";

function lock(overrides: Partial<LockInfo> = {}): LockInfo {
  return {
    lock_key: "lock:pr:owner/repo:42",
    prefix: null,
    resource: "owner/repo:42",
    resource_type: "pr",
    repo: "owner/repo",
    resource_id: "42",
    owner: "worker-1",
    ttl: 180,
    task_id: "task-live",
    pending_created_at: null,
    ...overrides,
  };
}

function result(overrides: Partial<RecentResult> = {}): RecentResult {
  return {
    result_id: "results:1-0",
    result_stream: "results",
    entry_id: "1-0",
    task_id: "task-historical",
    worker_id: "worker-1",
    status: "COMPLETED",
    repo: "owner/repo",
    resource_type: "pr",
    resource_id: "42",
    duration_seconds: 10,
    summary: "done",
    ...overrides,
  };
}

function snapshot(overrides: Partial<SystemSnapshot> = {}): SystemSnapshot {
  return {
    redis_ok: true,
    fetched_at: "2026-06-20T00:00:00.000Z",
    queue_depths: {},
    results_depth: 0,
    dead_letter_count: 0,
    locks: [],
    consumer_groups: [],
    recent_results: [],
    attempt_counts: {},
    dead_letter_entries: [],
    queued_tasks: [],
    provider_health: {},
    worker_pool: [],
    degraded_sections: [],
    dashboard_policy: {
      max_attempts: 3,
      pending_task_ttl_seconds: 5700,
      lock_ttl_seconds: 5400,
    },
    ...overrides,
  };
}

describe("Kanban DOM interactions", () => {
  beforeEach(() => {
    (globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
    mockUseTaskOutput.current = {
      lines: ["kanban output line"],
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

  it("opens and closes live task output through a native card button", async () => {
    const user = userEvent.setup();
    render(
      <Kanban
        snapshot={snapshot({ locks: [lock()] })}
        stuckTasks={[]}
      />,
    );

    const inProgressColumn = screen.getByRole("group", { name: "In Progress" });
    const outputButton = within(inProgressColumn).getByRole("button", {
      name: KANBAN_VIEW_NAME,
    });
    const descriptionId = outputButton.getAttribute("aria-describedby");
    expect(descriptionId).toBeTruthy();
    const description = document.getElementById(descriptionId!);
    expect(description?.textContent).toContain("PR #42");
    expect(description?.textContent).toContain("owner/repo");
    expect(description?.textContent).toContain("View Output");
    expect(outputButton.getAttribute("aria-expanded")).toBe("false");
    expect(outputButton.getAttribute("aria-controls")).toBeNull();

    outputButton.focus();
    await user.keyboard("{Enter}");

    const selectedButton = screen.getByRole("button", {
      name: KANBAN_HIDE_NAME,
    });
    expect(selectedButton.getAttribute("aria-expanded")).toBe("true");
    expect(selectedButton.getAttribute("aria-controls")).toBe(taskOutputDomId({
      workerId: "worker-1",
      taskId: "task-live",
      historical: false,
      prefix: null,
      instanceId: "lock:pr:owner/repo:42",
    }));
    expect(selectedButton.textContent).toContain("Hide Output");
    expect(screen.getByText("Output: PR owner/repo #42")).toBeTruthy();
    expect(screen.getByText("kanban output line")).toBeTruthy();

    await user.click(screen.getByRole("button", { name: "Close output for PR owner/repo #42" }));

    const collapsedButton = screen.getByRole("button", { name: KANBAN_VIEW_NAME });
    expect(collapsedButton.getAttribute("aria-expanded")).toBe("false");
    expect(screen.queryByText("Output: PR owner/repo #42")).toBeNull();
    await waitFor(() => expect(document.activeElement).toBe(collapsedButton));
  });

  it("moves focus to the board when refreshed data removes an open output card", async () => {
    const user = userEvent.setup();
    const { rerender } = render(
      <Kanban
        snapshot={snapshot({ locks: [lock()] })}
        stuckTasks={[]}
      />,
    );

    await user.click(screen.getByRole("button", { name: KANBAN_VIEW_NAME }));
    const outputLog = screen.getByRole("log", { name: "Output log for PR owner/repo #42" });
    outputLog.focus();
    expect(document.activeElement).toBe(outputLog);

    rerender(
      <Kanban
        snapshot={snapshot({ locks: [] })}
        stuckTasks={[]}
      />,
    );

    await waitFor(() =>
      expect(screen.queryByRole("region", { name: "Output: PR owner/repo #42" })).toBeNull()
    );
    await waitFor(() =>
      expect(document.activeElement).toBe(screen.getByRole("region", { name: "Kanban board" }))
    );
  });

  it("moves focus to the board when refreshed data removes a focused output card", async () => {
    const user = userEvent.setup();
    const { rerender } = render(
      <Kanban
        snapshot={snapshot({ locks: [lock()] })}
        stuckTasks={[]}
      />,
    );

    await user.click(screen.getByRole("button", { name: KANBAN_VIEW_NAME }));
    const selectedButton = screen.getByRole("button", { name: KANBAN_HIDE_NAME });
    selectedButton.focus();
    expect(document.activeElement).toBe(selectedButton);

    rerender(
      <Kanban
        snapshot={snapshot({ locks: [] })}
        stuckTasks={[]}
      />,
    );

    await waitFor(() =>
      expect(screen.queryByRole("region", { name: "Output: PR owner/repo #42" })).toBeNull()
    );
    await waitFor(() =>
      expect(document.activeElement).toBe(screen.getByRole("region", { name: "Kanban board" }))
    );
  });

  it("does not steal unrelated focus when refreshed data removes selected output", async () => {
    const user = userEvent.setup();
    const view = (locks: LockInfo[]) => (
      <div>
        <button type="button">External action</button>
        <Kanban
          snapshot={snapshot({ locks })}
          stuckTasks={[]}
        />
      </div>
    );
    const { rerender } = render(view([lock()]));

    await user.click(screen.getByRole("button", { name: KANBAN_VIEW_NAME }));
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

  it("keeps live output mounted when the resolved output prefix changes", async () => {
    const user = userEvent.setup();
    const selectedLock = lock({ prefix: "project-a" });
    const { rerender } = render(
      <Kanban
        snapshot={snapshot({ locks: [selectedLock] })}
        stuckTasks={[]}
      />,
    );

    await user.click(screen.getByRole("button", {
      name: "View output for [project-a] PR owner/repo #42 (worker-1, prefix project-a, task task-live)",
    }));
    expect(screen.getByRole("region", { name: "Output: [project-a] PR owner/repo #42" })).toBeTruthy();

    rerender(
      <Kanban
        snapshot={snapshot({ locks: [{ ...selectedLock, output_prefix: "orcest" }] })}
        stuckTasks={[]}
      />,
    );

    await waitFor(() => {
      const selectedButton = screen.getByRole("button", {
        name: "Hide output for [project-a] PR owner/repo #42 (worker-1, prefix orcest, task task-live)",
      });
      expect(selectedButton.getAttribute("aria-controls")).toBe(taskOutputDomId({
        workerId: "worker-1",
        taskId: "task-live",
        historical: false,
        prefix: "orcest",
        instanceId: "lock:pr:owner/repo:42",
      }));
      expect((mockUseTaskOutput.params as TaskOutputParams | null)?.prefix).toBe("orcest");
    });
    expect(screen.getByRole("region", { name: "Output: [project-a] PR owner/repo #42" })).toBeTruthy();
  });

  it("does not steal unrelated focus when refreshed data retargets live output", async () => {
    const user = userEvent.setup();
    const selectedLock = lock({ prefix: "project-a" });
    const view = (locks: LockInfo[]) => (
      <div>
        <button type="button">External action</button>
        <Kanban
          snapshot={snapshot({ locks })}
          stuckTasks={[]}
        />
      </div>
    );
    const { rerender } = render(view([selectedLock]));

    await user.click(screen.getByRole("button", {
      name: "View output for [project-a] PR owner/repo #42 (worker-1, prefix project-a, task task-live)",
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

  it("closes historical output when refreshed data pushes its result past the rendered cap", async () => {
    const user = userEvent.setup();
    const selectedResult = result();
    const { rerender } = render(
      <Kanban
        snapshot={snapshot({ recent_results: [selectedResult] })}
        stuckTasks={[]}
      />,
    );

    await user.click(within(screen.getByRole("group", { name: "Completed" })).getByRole("button", {
      name: KANBAN_HISTORICAL_VIEW_NAME,
    }));
    const outputLog = screen.getByRole("log", { name: "Output log for PR owner/repo #42" });
    outputLog.focus();
    expect(document.activeElement).toBe(outputLog);

    rerender(
      <Kanban
        snapshot={snapshot({
          recent_results: [
            ...Array.from({ length: KANBAN_RESULT_COLUMN_LIMIT }, (_, index) =>
              result({
                result_id: `results:new-${index}`,
                entry_id: `${index + 2}-0`,
                task_id: `task-new-${index}`,
                resource_id: String(index + 100),
              })),
            selectedResult,
          ],
        })}
        stuckTasks={[]}
      />,
    );

    await waitFor(() =>
      expect(screen.queryByRole("region", { name: "Output: PR owner/repo #42" })).toBeNull()
    );
    await waitFor(() =>
      expect(document.activeElement).toBe(screen.getByRole("region", { name: "Kanban board" }))
    );
  });

  it("selects only one result card when repeated Kanban results share worker and task IDs", async () => {
    const user = userEvent.setup();
    render(
      <Kanban
        snapshot={snapshot({
          recent_results: [
            result({ result_id: "results:1-0", task_id: "shared-task", resource_id: "42" }),
            result({ result_id: "results:2-0", task_id: "shared-task", resource_id: "43" }),
          ],
        })}
        stuckTasks={[]}
      />,
    );

    await user.click(within(screen.getByRole("group", { name: "Completed" })).getByRole("button", {
      name: "View output for PR owner/repo #42 (worker-1, task shared-task, historical)",
    }));

    expect(screen.getByRole("button", {
      name: "Hide output for PR owner/repo #42 (worker-1, task shared-task, historical)",
    }).textContent).toContain("Hide Output");
    expect(screen.getByRole("button", {
      name: "View output for PR owner/repo #43 (worker-1, task shared-task, historical)",
    }).textContent).toContain("View Output");
    expect(screen.getByRole("region", { name: "Output: PR owner/repo #42" })).toBeTruthy();
    expect(screen.queryByRole("region", { name: "Output: PR owner/repo #43" })).toBeNull();
  });

  it("refreshes the mounted output label when the selected card changes resource", async () => {
    const user = userEvent.setup();
    const { rerender } = render(
      <Kanban
        snapshot={snapshot({ locks: [lock()] })}
        stuckTasks={[]}
      />,
    );

    await user.click(screen.getByRole("button", { name: KANBAN_VIEW_NAME }));
    expect(screen.getByRole("region", { name: "Output: PR owner/repo #42" })).toBeTruthy();

    rerender(
      <Kanban
        snapshot={snapshot({
          locks: [lock({
            lock_key: "lock:pr:owner/repo:43",
            resource: "owner/repo:43",
            resource_id: "43",
          })],
        })}
        stuckTasks={[]}
      />,
    );

    expect(screen.queryByRole("region", { name: "Output: PR owner/repo #42" })).toBeNull();
    expect(screen.getByRole("region", { name: "Output: PR owner/repo #43" })).toBeTruthy();
    expect(screen.getByRole("button", {
      name: "Close output for PR owner/repo #43",
    })).toBeTruthy();
  });
});
