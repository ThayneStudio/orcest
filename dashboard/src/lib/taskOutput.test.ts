import { describe, expect, it } from "vitest";
import type { LockInfo, RecentResult } from "./types";
import {
  applyTaskOutputMessage,
  normalizeTaskOutputCursor,
  normalizeTaskOutputMessage,
  TASK_OUTPUT_MALFORMED_MESSAGE_ERROR,
  TASK_OUTPUT_PROTOCOL_ERROR,
  taskOutputCursorIsBefore,
  taskOutputCursorIsAfter,
  taskOutputControlLabel,
  taskOutputMessageMarksDone,
  taskOutputCloseMessage,
  taskOutputCloseStatus,
  taskOutputDomId,
  taskOutputParamsEqual,
  taskOutputParamsForLock,
  taskOutputParamsForResult,
  taskOutputParamsInstanceEqual,
  taskOutputParamsTargetEqual,
  taskOutputSelectionStillVisible,
  taskOutputTerminalError,
  type TaskOutputClientState,
} from "./taskOutput";

function lock(overrides: Partial<LockInfo>): LockInfo {
  return {
    lock_key: "orcest:lock:pr:owner/repo:42",
    prefix: "orcest",
    resource: "owner/repo:42",
    resource_type: "pr",
    repo: "owner/repo",
    resource_id: "42",
    owner: "worker-1",
    ttl: 180,
    task_id: "task-1",
    pending_created_at: null,
    ...overrides,
  };
}

function result(overrides: Partial<RecentResult>): RecentResult {
  return {
    result_id: "orcest:results:1-0",
    result_stream: "orcest:results",
    entry_id: "1-0",
    task_id: "task-1",
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

function outputState(overrides: Partial<TaskOutputClientState> = {}): TaskOutputClientState {
  return {
    lines: [],
    startIndex: 0,
    connected: true,
    retrying: false,
    done: false,
    error: null,
    ...overrides,
  };
}

describe("taskOutputParamsForLock", () => {
  it("uses active lock worker and task IDs when both are present", () => {
    expect(taskOutputParamsForLock(lock({}))).toEqual({
      workerId: "worker-1",
      taskId: "task-1",
      historical: false,
      prefix: "orcest",
      instanceId: "orcest:lock:pr:owner/repo:42",
    });
  });

  it("prefers the resolved output prefix for active lock output", () => {
    expect(taskOutputParamsForLock(lock({
      prefix: "project-a",
      output_prefix: "orcest",
    }))).toMatchObject({
      prefix: "orcest",
    });
    expect(taskOutputParamsForLock(lock({
      prefix: "project-a",
      output_prefix: null,
    }))).toMatchObject({
      prefix: null,
    });
  });

  it("does not fall back to the lock prefix when output prefix lookup is unresolved", () => {
    expect(taskOutputParamsForLock(lock({
      prefix: "project-a",
      output_prefix_unresolved: true,
    }))).toEqual(expect.objectContaining({
      prefix: undefined,
    }));
  });

  it("rejects active lock output when the exact task ID is unknown", () => {
    expect(taskOutputParamsForLock(lock({ task_id: null }))).toBeNull();
  });

  it("rejects expired or missing lock owners", () => {
    expect(taskOutputParamsForLock(lock({ owner: "(expired)" }))).toBeNull();
    expect(taskOutputParamsForLock(lock({ owner: " " }))).toBeNull();
  });
});

describe("taskOutputParamsForResult", () => {
  it("requires both worker and task IDs for historical results", () => {
    expect(taskOutputParamsForResult(result({}))).toEqual({
      workerId: "worker-1",
      taskId: "task-1",
      historical: true,
      prefix: "orcest",
      instanceId: "orcest:results:1-0",
    });
    expect(taskOutputParamsForResult(result({ worker_id: "" }))).toBeNull();
    expect(taskOutputParamsForResult(result({ task_id: "" }))).toBeNull();
  });

  it("marks unprefixed historical result streams explicitly", () => {
    expect(taskOutputParamsForResult(result({ result_stream: "results" }))).toMatchObject({
      prefix: null,
    });
  });

  it("prefers resolved output prefixes over result stream prefixes", () => {
    expect(taskOutputParamsForResult(result({
      result_stream: "project-a:results",
      output_prefix: "orcest",
    }))).toMatchObject({
      prefix: "orcest",
    });
    expect(taskOutputParamsForResult(result({
      result_stream: "project-a:results",
      output_prefix: null,
    }))).toMatchObject({
      prefix: null,
    });
  });

  it("does not fall back to the result stream prefix when output prefix lookup is unresolved", () => {
    const params = taskOutputParamsForResult(result({
      result_stream: "project-a:results",
      output_prefix_unresolved: true,
    }));

    expect(params).not.toHaveProperty("prefix");
  });
});

describe("taskOutputParamsEqual", () => {
  it("compares null, worker, task, history, and prefix identity", () => {
    expect(taskOutputParamsEqual(null, null)).toBe(false);
    expect(taskOutputParamsEqual({ workerId: "w" }, { workerId: "w" })).toBe(true);
    expect(taskOutputParamsEqual({ workerId: "w" }, { workerId: "w", taskId: "t" })).toBe(false);
    expect(taskOutputParamsEqual(
      { workerId: "w", taskId: "t", historical: false },
      { workerId: "w", taskId: "t", historical: true },
    )).toBe(false);
    expect(taskOutputParamsEqual(
      { workerId: "w", taskId: "t", historical: false, prefix: "project-a" },
      { workerId: "w", taskId: "t", historical: false, prefix: "project-b" },
    )).toBe(false);
    expect(taskOutputParamsEqual(
      { workerId: "w", taskId: "t", historical: true, instanceId: "result-1" },
      { workerId: "w", taskId: "t", historical: true, instanceId: "result-1" },
    )).toBe(true);
    expect(taskOutputParamsEqual(
      { workerId: "w", taskId: "t", historical: true, instanceId: "result-1" },
      { workerId: "w", taskId: "t", historical: true, instanceId: "result-2" },
    )).toBe(false);
  });
});

describe("taskOutputParamsTargetEqual", () => {
  it("matches the websocket target while ignoring rendered row identity", () => {
    expect(taskOutputParamsTargetEqual(
      { workerId: "w", taskId: "t", historical: true, prefix: null, instanceId: "result-1" },
      { workerId: "w", taskId: "t", historical: true, prefix: null, instanceId: "result-2" },
    )).toBe(true);
    expect(taskOutputParamsTargetEqual(
      { workerId: "w", taskId: "t", historical: true, prefix: null },
      { workerId: "w", taskId: "t", historical: false, prefix: null },
    )).toBe(false);
  });
});

describe("taskOutputParamsInstanceEqual", () => {
  it("matches the rendered lock or result identity while ignoring stream prefix changes", () => {
    expect(taskOutputParamsInstanceEqual(
      {
        workerId: "worker-1",
        taskId: "task-1",
        historical: true,
        prefix: "project-a",
        instanceId: "result-1",
      },
      {
        workerId: "worker-1",
        taskId: "task-1",
        historical: true,
        prefix: "orcest",
        instanceId: "result-1",
      },
    )).toBe(true);
    expect(taskOutputParamsInstanceEqual(
      { workerId: "worker-1", taskId: "task-1", historical: true, instanceId: "result-1" },
      { workerId: "worker-1", taskId: "task-2", historical: true, instanceId: "result-1" },
    )).toBe(false);
    expect(taskOutputParamsInstanceEqual(
      { workerId: "worker-1", taskId: "task-1", historical: true },
      { workerId: "worker-1", taskId: "task-1", historical: true },
    )).toBe(false);
  });
});

describe("taskOutputSelectionStillVisible", () => {
  it("keeps selected output open while the source section is degraded", () => {
    const selected = { workerId: "worker-1", taskId: "task-1", historical: true };

    expect(taskOutputSelectionStillVisible(selected, [], true)).toBe(true);
    expect(taskOutputSelectionStillVisible(selected, [], false)).toBe(false);
  });

  it("matches selected output against normalized candidate params", () => {
    const selected = { workerId: "worker-1", taskId: "task-1", historical: false };

    expect(taskOutputSelectionStillVisible(selected, [
      { workerId: "worker-1", taskId: "task-1", historical: false },
    ])).toBe(true);
    expect(taskOutputSelectionStillVisible(selected, [
      { workerId: "worker-1", taskId: "task-1", historical: true },
    ])).toBe(false);
    expect(taskOutputSelectionStillVisible(null, [], true)).toBe(false);
  });

  it("keeps a rendered selection visible when only the resolved output prefix changes", () => {
    const selected = {
      workerId: "worker-1",
      taskId: "task-1",
      historical: true,
      prefix: "project-a",
      instanceId: "result-1",
    };

    expect(taskOutputSelectionStillVisible(selected, [
      {
        workerId: "worker-1",
        taskId: "task-1",
        historical: true,
        prefix: "orcest",
        instanceId: "result-1",
      },
    ])).toBe(true);
    expect(taskOutputSelectionStillVisible(selected, [
      {
        workerId: "worker-1",
        taskId: "task-1",
        historical: true,
        prefix: null,
        instanceId: "result-1",
      },
    ])).toBe(true);
  });
});

describe("taskOutputDomId", () => {
  it("builds stable DOM-safe IDs for output panels", () => {
    expect(taskOutputDomId({
      workerId: "orcest-worker-300",
      taskId: "task/with spaces",
      historical: true,
    })).toBe(
      "task-output-pu-w6f_72_63_65_73_74_2d_77_6f_72_6b_65_72_2d_33_30_30-t74_61_73_6b_2f_77_69_74_68_20_73_70_61_63_65_73-h1",
    );
    expect(taskOutputDomId({
      workerId: "worker",
      historical: false,
    }, "results-output")).toBe("results-output-pu-w77_6f_72_6b_65_72-t0-h0");
  });

  it("keeps IDs distinct when separators appear in different fields", () => {
    expect(taskOutputDomId({
      workerId: "worker-1",
      taskId: "task",
      historical: false,
    })).not.toBe(taskOutputDomId({
      workerId: "worker",
      taskId: "1-task",
      historical: false,
    }));
  });

  it("keeps legacy wildcard and explicit unprefixed lookups distinct", () => {
    const base = { workerId: "worker-1", taskId: "task-1", historical: false };

    expect(taskOutputDomId({ ...base }))
      .not.toBe(taskOutputDomId({ ...base, prefix: null }));
    expect(taskOutputDomId({ ...base, prefix: null }))
      .not.toBe(taskOutputDomId({ ...base, prefix: "" }));
  });

  it("keeps distinct rendered rows separate when they share one output stream target", () => {
    const base = { workerId: "worker-1", taskId: "task-1", historical: true };

    expect(taskOutputDomId({ ...base, instanceId: "result-1" }))
      .not.toBe(taskOutputDomId({ ...base, instanceId: "result-2" }));
  });
});

describe("taskOutputControlLabel", () => {
  it("adds worker, task, and history context to output control labels", () => {
    expect(taskOutputControlLabel(
      "View",
      "PR owner/repo #42",
      { workerId: "worker-1", taskId: "task-result-1", historical: true },
    )).toBe("View output for PR owner/repo #42 (worker-1, task task-result-1, historical)");
  });

  it("adds project prefix context when output lookup is scoped", () => {
    expect(taskOutputControlLabel(
      "View",
      "PR owner/repo #42",
      { workerId: "worker-1", taskId: "task-result-1", historical: true, prefix: "project-a" },
    )).toBe(
      "View output for PR owner/repo #42 (worker-1, prefix project-a, task task-result-1, historical)",
    );
  });

  it("keeps task IDs distinct when they share the same prefix", () => {
    expect(taskOutputControlLabel(
      "View",
      "PR owner/repo #42",
      { workerId: "worker-1", taskId: "task-result-alpha", historical: true },
    )).not.toBe(taskOutputControlLabel(
      "View",
      "PR owner/repo #42",
      { workerId: "worker-1", taskId: "task-result-bravo", historical: true },
    ));
  });
});

describe("taskOutputCloseMessage", () => {
  it("maps terminal server close codes to user-facing messages", () => {
    expect(taskOutputCloseMessage(1008, "Missing worker_id")).toBe("Missing worker_id");
    expect(taskOutputCloseMessage(1013, "Too many connections")).toBe("Too many connections");
  });

  it("uses fallback messages for blank terminal reasons", () => {
    expect(taskOutputCloseMessage(1008, "")).toBe("Task output request was rejected.");
    expect(taskOutputCloseMessage(1013, " ")).toBe("Too many task output streams are open.");
  });

  it("leaves ordinary network closes retryable", () => {
    expect(taskOutputCloseMessage(1006, "")).toBeNull();
    expect(taskOutputCloseMessage(1001, "Server shutting down")).toBeNull();
  });
});

describe("taskOutputCloseStatus", () => {
  it("treats task-output normal completion closes as terminal", () => {
    expect(taskOutputCloseStatus(1000, "Task output complete")).toEqual({
      terminal: true,
      error: null,
    });
  });

  it("keeps a fallback error for normal unavailable closes", () => {
    expect(taskOutputCloseStatus(1000, "Task output unavailable")).toEqual({
      terminal: true,
      error: "Task output is unavailable.",
    });
  });

  it("keeps unrelated normal closes retryable", () => {
    expect(taskOutputCloseStatus(1000, "")).toEqual({
      terminal: false,
      error: null,
    });
  });
});

describe("taskOutputTerminalError", () => {
  it("preserves a detailed message received before the terminal close", () => {
    expect(
      taskOutputTerminalError(
        "Output for task 1234567890abcdef was not found.",
        "Task output is unavailable.",
      ),
    ).toBe("Output for task 1234567890abcdef was not found.");
  });

  it("uses the close error when there is no existing message", () => {
    expect(taskOutputTerminalError(null, "Too many connections")).toBe(
      "Too many connections",
    );
  });
});

describe("applyTaskOutputMessage", () => {
  it("keeps retained lines visible when an error frame arrives", () => {
    expect(applyTaskOutputMessage(
      outputState({ lines: ["already printed"], startIndex: 7 }),
      {
        lines: [],
        last_id: "9-0",
        done: true,
        error: "Task output is unavailable.",
      },
    )).toEqual(outputState({
      lines: ["already printed"],
      startIndex: 7,
      done: true,
      error: "Task output is unavailable.",
    }));
  });

  it("trims old lines and advances startIndex when output exceeds the cap", () => {
    expect(applyTaskOutputMessage(
      outputState({ lines: ["a", "b"], startIndex: 10 }),
      {
        lines: ["c", "d", "e"],
        last_id: "5-0",
        done: false,
      },
      3,
    )).toEqual(outputState({
      lines: ["c", "d", "e"],
      startIndex: 12,
    }));
  });

  it("marks empty terminal frames complete without changing retained lines", () => {
    expect(applyTaskOutputMessage(
      outputState({ lines: ["last line"] }),
      {
        lines: [],
        last_id: "7-0",
        done: true,
      },
    )).toEqual(outputState({
      lines: ["last line"],
      done: true,
    }));
  });

  it("clears reconnect state when output resumes", () => {
    expect(applyTaskOutputMessage(
      outputState({ retrying: true }),
      {
        lines: ["resumed"],
        last_id: "9-0",
        done: false,
      },
    )).toEqual(outputState({ lines: ["resumed"], retrying: false }));
  });

  it("ignores whitespace-only error frames", () => {
    const state = outputState({ lines: ["retained"] });

    expect(applyTaskOutputMessage(state, {
      lines: [],
      last_id: "8-0",
      done: false,
      error: "   ",
    })).toBe(state);
  });

  it("returns the same state object for empty non-terminal keepalive frames", () => {
    const state = outputState({ lines: ["retained"] });

    expect(applyTaskOutputMessage(state, {
      lines: [],
      last_id: "0-0",
      done: false,
    })).toBe(state);
  });
});

describe("taskOutputMessageMarksDone", () => {
  it("treats done and error frames as terminal for reconnect state", () => {
    expect(taskOutputMessageMarksDone({ lines: [], last_id: "1-0", done: true })).toBe(true);
    expect(taskOutputMessageMarksDone({
      lines: [],
      last_id: "1-0",
      done: false,
      error: "unavailable",
    })).toBe(true);
    expect(taskOutputMessageMarksDone({
      lines: [],
      last_id: "1-0",
      done: false,
      error: "   ",
    })).toBe(false);
    expect(taskOutputMessageMarksDone({ lines: ["hello"], last_id: "1-0", done: false })).toBe(false);
  });
});

describe("normalizeTaskOutputCursor", () => {
  it("keeps valid non-zero Redis stream cursors", () => {
    expect(normalizeTaskOutputCursor(" 123-4 ")).toBe("123-4");
  });

  it("drops default, missing, and malformed cursors", () => {
    expect(normalizeTaskOutputCursor("0-0")).toBeNull();
    expect(normalizeTaskOutputCursor(null)).toBeNull();
    expect(normalizeTaskOutputCursor("1-*")).toBeNull();
  });
});

describe("taskOutputCursorIsAfter", () => {
  it("compares Redis stream cursors numerically", () => {
    expect(taskOutputCursorIsAfter("10-0", "9-99")).toBe(true);
    expect(taskOutputCursorIsAfter("10-1", "10-0")).toBe(true);
    expect(taskOutputCursorIsAfter("10-0", "10-0")).toBe(false);
    expect(taskOutputCursorIsAfter("9-99", "10-0")).toBe(false);
  });

  it("treats missing previous cursors as the stream origin", () => {
    expect(taskOutputCursorIsAfter("1-0", "0-0")).toBe(true);
    expect(taskOutputCursorIsAfter("1-0", null)).toBe(true);
    expect(taskOutputCursorIsAfter("0-0", null)).toBe(false);
    expect(taskOutputCursorIsAfter("bad", "1-0")).toBe(false);
  });
});

describe("taskOutputCursorIsBefore", () => {
  it("detects cursors older than the retained reconnect cursor", () => {
    expect(taskOutputCursorIsBefore("9-99", "10-0")).toBe(true);
    expect(taskOutputCursorIsBefore("10-0", "10-1")).toBe(true);
    expect(taskOutputCursorIsBefore("10-0", "10-0")).toBe(false);
    expect(taskOutputCursorIsBefore("10-1", "10-0")).toBe(false);
  });

  it("does not treat missing or malformed cursors as stale", () => {
    expect(taskOutputCursorIsBefore("0-0", "10-0")).toBe(false);
    expect(taskOutputCursorIsBefore("bad", "10-0")).toBe(false);
    expect(taskOutputCursorIsBefore("10-0", null)).toBe(false);
  });
});

describe("normalizeTaskOutputMessage", () => {
  it("keeps only string lines and boolean done values", () => {
    expect(normalizeTaskOutputMessage({
      lines: ["ok", 123, null, "next"],
      last_id: " 12-0 ",
      done: false,
      error: "   ",
    })).toEqual({
      lines: ["ok", "next"],
      last_id: "12-0",
      done: false,
    });
  });

  it("turns output frames without resumable cursors into terminal protocol errors", () => {
    for (const lastId of [undefined, "", "0-0", "1-*", 42]) {
      expect(normalizeTaskOutputMessage({
        lines: ["unsafe output"],
        ...(lastId === undefined ? {} : { last_id: lastId }),
        done: false,
      })).toEqual({
        lines: [],
        last_id: "0-0",
        done: true,
        error: TASK_OUTPUT_PROTOCOL_ERROR,
      });
    }
  });

  it("normalizes malformed cursors on empty keepalive frames", () => {
    expect(normalizeTaskOutputMessage({
      lines: [],
      last_id: "not-a-cursor",
      done: false,
    })).toEqual({
      lines: [],
      last_id: "0-0",
      done: false,
    });
  });

  it("preserves valid terminal errors", () => {
    expect(normalizeTaskOutputMessage({
      lines: [],
      last_id: "9-0",
      done: true,
      error: "Output unavailable",
    })).toEqual({
      lines: [],
      last_id: "9-0",
      done: true,
      error: "Output unavailable",
    });
  });

  it("rejects malformed frames", () => {
    expect(normalizeTaskOutputMessage(null)).toBeNull();
    expect(normalizeTaskOutputMessage([])).toBeNull();
    expect(normalizeTaskOutputMessage({ ok: true })).toBeNull();
    expect(normalizeTaskOutputMessage({ lines: [], last_id: "0-0", done: "false" }))
      .toBeNull();
    expect(normalizeTaskOutputMessage({ lines: [], done: false })).toBeNull();
    expect(TASK_OUTPUT_MALFORMED_MESSAGE_ERROR).toBe(
      "Task output stream sent a malformed message.",
    );
  });
});
