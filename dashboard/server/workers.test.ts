import { beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("./redis.js", () => ({
  redis: {},
  dashboardRedisKeyPatterns: vi.fn((suffixes: string[]) =>
    suffixes.flatMap((suffix) => [`*:${suffix}`, suffix])
  ),
  scanKeysMany: vi.fn(),
}));

import { dashboardRedisKeyPatterns, redis, scanKeysMany } from "./redis.js";
import {
  clearWorkerCachesForTesting,
  discoverWorkers,
  findTaskOutputPrefixes,
  findTaskStart,
  formatRawTerminalLine,
  formatStreamLine,
  normalizeTaskOutputCursor,
  normalizeDiscoveredWorkers,
  readTaskOutput,
  readTaskOutputFromStream,
  taskOutputReadFailureMessage,
  taskOutputUnavailableMessage,
  taskOutputPrefixLookupKey,
  TaskOutputPartialReadError,
  WorkerDiscoveryPartialError,
  workerIdFromOutputStream,
} from "./workers.js";

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(dashboardRedisKeyPatterns).mockImplementation((suffixes: string[]) =>
    suffixes.flatMap((suffix) => [`*:${suffix}`, suffix])
  );
  clearWorkerCachesForTesting();
});

describe("formatRawTerminalLine", () => {
  it("strips terminal escape sequences and keeps readable text", () => {
    const raw = [
      "\x1b[1D\x1b[4B",
      "\x1b[38;5;220mNew MCP server found in this project: supabase\x1b[39m\r\n",
      "   \x1b[38;5;246m3. \x1b[39mContinue without using this MCP server\r\n",
      "\x1b[?25l",
    ].join("");

    expect(formatRawTerminalLine(raw)).toBe(
      "New MCP server found in this project: supabase\n" +
        "3. Continue without using this MCP server",
    );
  });

  it("drops pure terminal frame noise", () => {
    expect(formatRawTerminalLine("\x1b[38;5;244m────────────\x1b[39m\r\n")).toBeNull();
  });

  it("keeps readable lines throughout a long interactive chunk", () => {
    const raw = Array.from({ length: 25 }, (_, index) => `line ${index + 1}`).join("\n");

    expect(formatRawTerminalLine(raw)).toContain("line 1");
    expect(formatRawTerminalLine(raw)).toContain("line 25");
  });

  it("marks truncated raw terminal chunks", () => {
    const formatted = formatRawTerminalLine(`${"x".repeat(4100)}\nfinal line`);

    expect(formatted).not.toBeNull();
    expect(formatted).toContain("[truncated]");
    expect(formatted!.length).toBeLessThanOrEqual(4000);
  });
});

describe("formatStreamLine", () => {
  it("falls back to sanitized raw output when line is not JSON", () => {
    expect(formatStreamLine({ line: "\x1b[31mhello\x1b[0m\r\n" })).toBe("hello");
  });

  it("formats Codex agent messages", () => {
    expect(
      formatStreamLine({
        line: JSON.stringify({
          type: "item.completed",
          item: { type: "agent_message", text: "Done." },
        }),
      }),
    ).toBe("Done.");
  });

  it("sanitizes Codex agent messages before rendering them", () => {
    expect(
      formatStreamLine({
        line: JSON.stringify({
          type: "item.completed",
          item: { type: "agent_message", text: "\x1b[31mDone\x1b[0m\x07" },
        }),
      }),
    ).toBe("Done");
  });

  it("sanitizes assistant tool metadata before rendering it", () => {
    expect(
      formatStreamLine({
        line: JSON.stringify({
          message: {
            role: "assistant",
            content: [
              {
                type: "tool_use",
                name: "Bash",
                input: { command: "\x1b[31mecho hi\x1b[0m\r\nrm -rf /" },
              },
              {
                type: "tool_use",
                name: "Read",
                input: { file_path: "/tmp/a\x07\nb" },
              },
            ],
          },
        }),
      }),
    ).toBe("  $ echo hi rm -rf /\n  Read /tmp/a b");
  });

  it("formats assistant messages with plain string content", () => {
    expect(
      formatStreamLine({
        line: JSON.stringify({
          message: {
            role: "assistant",
            content: "\x1b[32mplain answer\x1b[0m",
          },
        }),
      }),
    ).toBe("plain answer");

    expect(
      formatStreamLine({
        line: JSON.stringify({
          role: "assistant",
          content: "top-level answer",
        }),
      }),
    ).toBe("top-level answer");
  });

  it("caps long JSON provider output lines", () => {
    const longText = "x".repeat(5000);
    const cases = [
      JSON.stringify({
        message: {
          role: "assistant",
          content: [{ type: "text", text: longText }],
        },
      }),
      JSON.stringify({ type: "text", data: longText }),
      JSON.stringify({
        type: "item.completed",
        item: { type: "agent_message", text: longText },
      }),
    ];

    for (const line of cases) {
      const formatted = formatStreamLine({ line });
      expect(formatted).not.toBeNull();
      expect(formatted!.length).toBeLessThanOrEqual(4000);
      expect(formatted).toContain("[truncated]");
    }
  });

  it("sanitizes task boundary marker metadata before rendering it", () => {
    const startLine = formatStreamLine({
      type: "task_start",
      task_id: "\x1b[31mtask-1\x1b[0m\nspoofed",
      resource: "pr #42\r\nERROR injected",
    });
    expect(startLine).toContain("Task task-1 spoofed: pr #42 ERROR injected");
    expect(startLine).not.toContain("\n");

    const endLine = formatStreamLine({
      type: "task_end",
      task_id: "task-1\x07",
      status: "completed\nFAILED",
    });
    expect(endLine).toContain("End task-1: completed FAILED");
    expect(endLine).not.toContain("\n");
  });

  it("formats Codex file changes", () => {
    expect(
      formatStreamLine({
        line: JSON.stringify({
          type: "item.completed",
          item: {
            type: "file_change",
            status: "completed",
            changes: [{ kind: "add", path: "/tmp/CODEX.txt" }],
          },
        }),
      }),
    ).toBe("  File change completed: add /tmp/CODEX.txt");
  });

  it("sanitizes Codex file change and command metadata before rendering it", () => {
    expect(
      formatStreamLine({
        line: JSON.stringify({
          type: "item.completed",
          item: {
            type: "file_change",
            status: "completed\x07",
            changes: [{ kind: "\x1b[32madd\x1b[0m", path: "/tmp/a\nb" }],
          },
        }),
      }),
    ).toBe("  File change completed: add /tmp/a b");

    expect(
      formatStreamLine({
        line: JSON.stringify({
          type: "item.started",
          item: {
            type: "command_execution",
            status: "running\x07",
            command: "\x1b[31mnpm test\x1b[0m\nnpm build",
          },
        }),
      }),
    ).toBe("  Command started running: npm test npm build");
  });

  it("formats JSON error events", () => {
    expect(
      formatStreamLine({
        line: JSON.stringify({ type: "error", message: "token expired" }),
      }),
    ).toBe("Error: token expired");
  });

  it("formats Grok text chunks", () => {
    expect(
      formatStreamLine({
        line: JSON.stringify({ type: "text", data: "hello" }),
      }),
    ).toBe("hello");
  });

  it("ignores malformed assistant text blocks instead of rendering object placeholders", () => {
    expect(
      formatStreamLine({
        line: JSON.stringify({
          message: {
            role: "assistant",
            content: [
              { type: "text", text: { nested: "not text" } },
              { type: "text", text: "\x1b[32mreal text\x1b[0m" },
            ],
          },
        }),
      }),
    ).toBe("real text");
  });
});

describe("workerIdFromOutputStream", () => {
  it("parses prefixed output stream names", () => {
    expect(workerIdFromOutputStream("orcest:output:worker-1")).toBe("worker-1");
  });

  it("parses unprefixed output stream names", () => {
    expect(workerIdFromOutputStream("output:worker_2")).toBe("worker_2");
  });

  it("parses worker IDs that include dots or colons", () => {
    expect(workerIdFromOutputStream("orcest:output:pve-test.lab.prefixa.net")).toBe(
      "pve-test.lab.prefixa.net",
    );
    expect(workerIdFromOutputStream("output:worker:with:colon")).toBe("worker:with:colon");
  });

  it("rejects unrelated stream names", () => {
    expect(workerIdFromOutputStream("orcest:tasks:claude")).toBeNull();
  });
});

describe("normalizeDiscoveredWorkers", () => {
  it("dedupes worker IDs and keeps the newest activity timestamp", () => {
    expect(
      normalizeDiscoveredWorkers([
        { id: "worker-b", lastEntryMs: 2000 },
        { id: "worker-a", lastEntryMs: 1000 },
        { id: "worker-a", lastEntryMs: 3000 },
      ], 4000),
    ).toEqual(["worker-a", "worker-b"]);
  });

  it("filters stale and malformed worker entries", () => {
    const now = 10 * 24 * 60 * 60 * 1000;
    expect(
      normalizeDiscoveredWorkers([
        { id: "fresh", lastEntryMs: now - 1000 },
        { id: "stale", lastEntryMs: now - 8 * 24 * 60 * 60 * 1000 },
        { id: "", lastEntryMs: now },
        { id: "bad", lastEntryMs: Number.NaN },
      ], now),
    ).toEqual(["fresh"]);
  });
});

describe("discoverWorkers", () => {
  it("scopes worker output discovery to configured dashboard Redis prefixes", async () => {
    vi.mocked(dashboardRedisKeyPatterns).mockImplementation((suffixes: string[]) =>
      suffixes.map((suffix) => `orcest:${suffix}`)
    );
    vi.mocked(scanKeysMany).mockResolvedValueOnce([]);

    await expect(discoverWorkers()).resolves.toEqual([]);

    expect(dashboardRedisKeyPatterns).toHaveBeenCalledWith(["output:*"]);
    expect(scanKeysMany).toHaveBeenCalledWith(["orcest:output:*"]);
  });

  it("surfaces worker discovery scan failures to the dashboard message", async () => {
    vi.mocked(scanKeysMany).mockRejectedValueOnce(new Error("scan failed"));

    await expect(discoverWorkers()).rejects.toThrow("scan failed");
  });

  it("surfaces partial worker discovery read failures with the healthy workers", async () => {
    const freshEntryId = `${Date.now()}-0`;
    vi.mocked(scanKeysMany).mockResolvedValueOnce([
      "output:worker-1",
      "output:worker-2",
    ]);
    const pipeline = {
      xrevrange: vi.fn().mockReturnThis(),
      exec: vi.fn().mockResolvedValue([
        [null, [[freshEntryId, []]]],
        [new Error("WRONGTYPE"), null],
      ]),
    };
    (redis as unknown as { pipeline: ReturnType<typeof vi.fn> }).pipeline =
      vi.fn(() => pipeline);

    const discovery = discoverWorkers();
    await expect(discovery).rejects.toMatchObject({
      workers: ["worker-1"],
    });
    await expect(discovery).rejects.toBeInstanceOf(WorkerDiscoveryPartialError);
  });

  it("marks worker discovery failed when every output stream read fails", async () => {
    vi.mocked(scanKeysMany).mockResolvedValueOnce([
      "output:worker-1",
      "output:worker-2",
    ]);
    const pipeline = {
      xrevrange: vi.fn().mockReturnThis(),
      exec: vi.fn().mockResolvedValue([
        [new Error("WRONGTYPE"), null],
        [new Error("trimmed"), null],
      ]),
    };
    (redis as unknown as { pipeline: ReturnType<typeof vi.fn> }).pipeline =
      vi.fn(() => pipeline);

    await expect(discoverWorkers()).rejects.toThrow(
      "Failed to inspect worker output stream output:worker-1: WRONGTYPE",
    );
  });

  it("surfaces missing or malformed pipeline slots with readable workers", async () => {
    const freshEntryId = `${Date.now()}-0`;
    vi.mocked(scanKeysMany).mockResolvedValueOnce([
      "output:worker-missing",
      "output:worker-malformed",
      "output:worker-ok",
    ]);
    const pipeline = {
      xrevrange: vi.fn().mockReturnThis(),
      exec: vi.fn().mockResolvedValue([
        undefined,
        [null, null],
        [null, [[freshEntryId, []]]],
      ]),
    };
    (redis as unknown as { pipeline: ReturnType<typeof vi.fn> }).pipeline =
      vi.fn(() => pipeline);

    await expect(discoverWorkers()).rejects.toMatchObject({
      workers: ["worker-ok"],
    });
  });

  it("skips output streams with malformed latest entry IDs", async () => {
    const freshEntryId = `${Date.now()}-0`;
    vi.mocked(scanKeysMany).mockResolvedValueOnce([
      "output:worker-malformed",
      "output:worker-ok",
    ]);
    const pipeline = {
      xrevrange: vi.fn().mockReturnThis(),
      exec: vi.fn().mockResolvedValue([
        [null, [["9999999999999abc-0", []]]],
        [null, [[freshEntryId, []]]],
      ]),
    };
    (redis as unknown as { pipeline: ReturnType<typeof vi.fn> }).pipeline =
      vi.fn(() => pipeline);

    await expect(discoverWorkers()).resolves.toEqual(["worker-ok"]);
  });

  it("surfaces missing pipeline slots when no output stream can be inspected", async () => {
    vi.mocked(scanKeysMany).mockResolvedValueOnce([
      "output:worker-missing",
    ]);
    const pipeline = {
      xrevrange: vi.fn().mockReturnThis(),
      exec: vi.fn().mockResolvedValue([]),
    };
    (redis as unknown as { pipeline: ReturnType<typeof vi.fn> }).pipeline =
      vi.fn(() => pipeline);

    await expect(discoverWorkers()).rejects.toThrow(
      "Worker discovery did not return a result for output stream output:worker-missing",
    );
  });
});

describe("readTaskOutput", () => {
  it("resolves output prefixes for many tasks with one output stream discovery scan", async () => {
    vi.mocked(scanKeysMany).mockResolvedValueOnce([
      "orcest:output:worker-1",
      "project-b:output:worker-2",
    ]);
    (redis as unknown as { xrevrange?: ReturnType<typeof vi.fn> }).xrevrange = vi.fn()
      .mockResolvedValueOnce([
        ["40-0", ["type", "task_start", "task_id", "task-b"]],
        ["30-0", ["type", "task_start", "task_id", "task-a"]],
      ])
      .mockResolvedValueOnce([
        ["50-0", ["task_id", "task-c", "line", "retained output"]],
      ])
      .mockResolvedValueOnce([]);

    const result = await findTaskOutputPrefixes([
      { workerId: "worker-1", taskId: "task-a" },
      { workerId: "worker-1", taskId: "task-b" },
      { workerId: "worker-1", taskId: "task-b" },
      { workerId: "worker-2", taskId: "task-c" },
    ]);

    expect(result.degraded).toBe(false);
    expect(scanKeysMany).toHaveBeenCalledTimes(1);
    expect(scanKeysMany).toHaveBeenCalledWith(["*:output:*", "output:*"]);
    expect(redis.xrevrange).toHaveBeenCalledTimes(2);
    expect(result.prefixes.get(taskOutputPrefixLookupKey("worker-1", "task-a"))).toBe("orcest");
    expect(result.prefixes.get(taskOutputPrefixLookupKey("worker-1", "task-b"))).toBe("orcest");
    expect(result.prefixes.get(taskOutputPrefixLookupKey("worker-2", "task-c"))).toBe("project-b");
  });

  it("reuses resolved batch output prefixes on later snapshot lookups", async () => {
    vi.mocked(scanKeysMany).mockResolvedValueOnce([
      "orcest:output:worker-1",
    ]);
    (redis as unknown as { xrevrange?: ReturnType<typeof vi.fn> }).xrevrange = vi.fn()
      .mockResolvedValueOnce([
        ["30-0", ["type", "task_start", "task_id", "task-a"]],
      ]);

    const first = await findTaskOutputPrefixes([
      { workerId: "worker-1", taskId: "task-a" },
    ]);
    const second = await findTaskOutputPrefixes([
      { workerId: "worker-1", taskId: "task-a" },
    ]);

    expect(first.degraded).toBe(false);
    expect(second.degraded).toBe(false);
    expect(first.prefixes.get(taskOutputPrefixLookupKey("worker-1", "task-a"))).toBe("orcest");
    expect(second.prefixes.get(taskOutputPrefixLookupKey("worker-1", "task-a"))).toBe("orcest");
    expect(scanKeysMany).toHaveBeenCalledTimes(1);
    expect(redis.xrevrange).toHaveBeenCalledTimes(1);
  });

  it("keeps cached batch output prefixes when a later output scan fails", async () => {
    vi.mocked(scanKeysMany)
      .mockResolvedValueOnce([
        "orcest:output:worker-1",
      ])
      .mockRejectedValueOnce(new Error("SCAN failed"));
    (redis as unknown as { xrevrange?: ReturnType<typeof vi.fn> }).xrevrange = vi.fn()
      .mockResolvedValueOnce([
        ["30-0", ["type", "task_start", "task_id", "task-a"]],
      ]);

    const first = await findTaskOutputPrefixes([
      { workerId: "worker-1", taskId: "task-a" },
    ]);
    const second = await findTaskOutputPrefixes([
      { workerId: "worker-1", taskId: "task-a" },
      { workerId: "worker-2", taskId: "task-b" },
    ]);

    expect(first.degraded).toBe(false);
    expect(second.degraded).toBe(true);
    expect(second.prefixes.get(taskOutputPrefixLookupKey("worker-1", "task-a"))).toBe("orcest");
    expect(second.prefixes.get(taskOutputPrefixLookupKey("worker-2", "task-b"))).toBeUndefined();
    expect(scanKeysMany).toHaveBeenCalledTimes(2);
    expect(redis.xrevrange).toHaveBeenCalledTimes(1);
  });

  it("does not cache batch output prefixes from partially degraded lookups", async () => {
    vi.mocked(scanKeysMany)
      .mockResolvedValueOnce([
        "output:worker-1",
        "project-a:output:worker-1",
      ])
      .mockResolvedValueOnce([
        "project-a:output:worker-1",
      ]);
    (redis as unknown as { xrevrange?: ReturnType<typeof vi.fn> }).xrevrange = vi.fn()
      .mockRejectedValueOnce(new Error("WRONGTYPE"))
      .mockResolvedValueOnce([
        ["10-0", ["type", "task_start", "task_id", "task-1"]],
      ])
      .mockResolvedValueOnce([
        ["10-0", ["type", "task_start", "task_id", "task-1"]],
      ]);

    const first = await findTaskOutputPrefixes([
      { workerId: "worker-1", taskId: "task-1" },
    ]);
    const second = await findTaskOutputPrefixes([
      { workerId: "worker-1", taskId: "task-1" },
    ]);

    expect(first.degraded).toBe(true);
    expect(second.degraded).toBe(false);
    expect(first.prefixes.get(taskOutputPrefixLookupKey("worker-1", "task-1"))).toBe("project-a");
    expect(second.prefixes.get(taskOutputPrefixLookupKey("worker-1", "task-1"))).toBe("project-a");
    expect(scanKeysMany).toHaveBeenCalledTimes(2);
    expect(redis.xrevrange).toHaveBeenCalledTimes(3);
  });

  it("keeps healthy batch output prefix results while reporting partial stream failures", async () => {
    vi.mocked(scanKeysMany).mockResolvedValueOnce([
      "output:worker-1",
      "project-a:output:worker-1",
    ]);
    (redis as unknown as { xrevrange?: ReturnType<typeof vi.fn> }).xrevrange = vi.fn()
      .mockRejectedValueOnce(new Error("WRONGTYPE"))
      .mockResolvedValueOnce([
        ["10-0", ["type", "task_start", "task_id", "task-1"]],
      ]);

    const result = await findTaskOutputPrefixes([
      { workerId: "worker-1", taskId: "task-1" },
    ]);

    expect(result.degraded).toBe(true);
    expect(result.prefixes.get(taskOutputPrefixLookupKey("worker-1", "task-1"))).toBe("project-a");
  });

  it("stops batch prefix lookup once tagged output identifies every requested task", async () => {
    vi.mocked(scanKeysMany).mockResolvedValueOnce(["orcest:output:worker-1"]);
    (redis as unknown as { xrevrange?: ReturnType<typeof vi.fn> }).xrevrange = vi.fn()
      .mockResolvedValueOnce([
        ["50-0", ["task_id", "task-b", "line", "retained b"]],
        ["40-0", ["task_id", "task-a", "line", "retained a"]],
      ])
      .mockResolvedValueOnce([
        ["10-0", ["type", "task_start", "task_id", "task-a"]],
      ]);

    const result = await findTaskOutputPrefixes([
      { workerId: "worker-1", taskId: "task-a" },
      { workerId: "worker-1", taskId: "task-b" },
    ]);

    expect(result.degraded).toBe(false);
    expect(redis.xrevrange).toHaveBeenCalledTimes(1);
    expect(result.prefixes.get(taskOutputPrefixLookupKey("worker-1", "task-a"))).toBe("orcest");
    expect(result.prefixes.get(taskOutputPrefixLookupKey("worker-1", "task-b"))).toBe("orcest");
  });

  it("finds exact task starts across duplicate worker output streams", async () => {
    vi.mocked(scanKeysMany).mockResolvedValueOnce([
      "orcest:output:worker-dup",
      "project-a:output:worker-dup",
    ]);
    (redis as unknown as { xrevrange?: ReturnType<typeof vi.fn> }).xrevrange = vi.fn()
      .mockResolvedValueOnce([
        ["10-0", ["type", "task_start", "task_id", "other-task"]],
      ])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([
        ["9-0", ["type", "task_start", "task_id", "task-1"]],
      ]);

    await expect(findTaskStart("worker-dup", "task-1")).resolves.toEqual({
      stream: "project-a:output:worker-dup",
      entryId: "9-0",
    });
  });

  it("does not prefer malformed task-start IDs across duplicate worker output streams", async () => {
    vi.mocked(scanKeysMany).mockResolvedValueOnce([
      "orcest:output:worker-dup",
      "project-a:output:worker-dup",
    ]);
    (redis as unknown as { xrevrange?: ReturnType<typeof vi.fn> }).xrevrange = vi.fn()
      .mockResolvedValueOnce([
        ["9999999999999abc-0", ["type", "task_start", "task_id", "task-1"]],
      ])
      .mockResolvedValueOnce([
        ["10-0", ["type", "task_start", "task_id", "task-1"]],
      ]);

    await expect(findTaskStart("worker-dup", "task-1")).resolves.toEqual({
      stream: "project-a:output:worker-dup",
      entryId: "10-0",
    });
  });

  it("scopes exact task-start lookup to the requested Redis prefix", async () => {
    (redis as unknown as { xrevrange?: ReturnType<typeof vi.fn> }).xrevrange = vi.fn()
      .mockResolvedValueOnce([
        ["9-0", ["type", "task_start", "task_id", "task-1"]],
      ]);

    await expect(findTaskStart("worker-dup", "task-1", "project-a")).resolves.toEqual({
      stream: "project-a:output:worker-dup",
      entryId: "9-0",
    });
    expect(scanKeysMany).not.toHaveBeenCalled();
    expect(redis.xrevrange).toHaveBeenCalledTimes(1);
    expect(redis.xrevrange).toHaveBeenCalledWith(
      "project-a:output:worker-dup",
      "+",
      "-",
      "COUNT",
      500,
    );
  });

  it("scopes task-start lookup to explicitly unprefixed output streams", async () => {
    (redis as unknown as { xrevrange?: ReturnType<typeof vi.fn> }).xrevrange = vi.fn()
      .mockResolvedValueOnce([
        ["8-0", ["type", "task_start", "task_id", "task-1"]],
      ]);

    await expect(findTaskStart("worker-dup", "task-1", null)).resolves.toEqual({
      stream: "output:worker-dup",
      entryId: "8-0",
    });
    expect(scanKeysMany).not.toHaveBeenCalled();
    expect(redis.xrevrange).toHaveBeenCalledTimes(1);
    expect(redis.xrevrange).toHaveBeenCalledWith(
      "output:worker-dup",
      "+",
      "-",
      "COUNT",
      500,
    );
  });

  it("finds task starts for worker IDs with dots", async () => {
    vi.mocked(scanKeysMany).mockResolvedValueOnce([
      "orcest:output:pve-test.lab.prefixa.net",
      "orcest:output:other-worker",
    ]);
    (redis as unknown as { xrevrange?: ReturnType<typeof vi.fn> }).xrevrange = vi.fn()
      .mockResolvedValueOnce([
        ["10-0", ["type", "task_start", "task_id", "task-1"]],
      ]);

    await expect(findTaskStart("pve-test.lab.prefixa.net", "task-1")).resolves.toEqual({
      stream: "orcest:output:pve-test.lab.prefixa.net",
      entryId: "10-0",
    });
    expect(scanKeysMany).toHaveBeenCalledWith(["*:output:*", "output:*"]);
  });

  it("skips bad duplicate output streams while finding an exact task start", async () => {
    vi.mocked(scanKeysMany).mockResolvedValueOnce([
      "orcest:output:worker-dup",
      "project-a:output:worker-dup",
    ]);
    (redis as unknown as { xrevrange?: ReturnType<typeof vi.fn> }).xrevrange = vi.fn()
      .mockRejectedValueOnce(new Error("WRONGTYPE"))
      .mockResolvedValueOnce([
        ["9-0", ["type", "task_start", "task_id", "task-1"]],
      ]);

    await expect(findTaskStart("worker-dup", "task-1")).resolves.toEqual({
      stream: "project-a:output:worker-dup",
      entryId: "9-0",
    });
  });

  it("surfaces partial stream read failures when no stream proves the requested task exists", async () => {
    vi.mocked(scanKeysMany).mockResolvedValueOnce([
      "orcest:output:worker-dup",
      "project-a:output:worker-dup",
    ]);
    (redis as unknown as { xrevrange?: ReturnType<typeof vi.fn> }).xrevrange = vi.fn()
      .mockRejectedValueOnce(new Error("WRONGTYPE"))
      .mockResolvedValueOnce([]);

    const lookup = findTaskStart("worker-dup", "task-1");

    await expect(lookup).rejects.toBeInstanceOf(
      TaskOutputPartialReadError,
    );
    await expect(lookup).rejects.toThrow(
      "Task output lookup was incomplete",
    );
  });

  it("falls back to the oldest retained tagged output when task_start is missing", async () => {
    vi.mocked(scanKeysMany).mockResolvedValueOnce(["output:worker-tagged"]);
    (redis as unknown as { xrevrange?: ReturnType<typeof vi.fn> }).xrevrange = vi.fn()
      .mockResolvedValueOnce([
        ["10-0", ["task_id", "task-1", "line", "latest"]],
        ["9-0", ["task_id", "other-task", "line", "other"]],
        ["8-0", ["task_id", "task-1", "line", "earliest retained"]],
      ])
      .mockResolvedValueOnce([]);

    await expect(findTaskStart("worker-tagged", "task-1")).resolves.toEqual({
      stream: "output:worker-tagged",
      entryId: "8-0",
    });
  });

  it("falls back to the newest tagged task when latest task_start is missing", async () => {
    vi.mocked(scanKeysMany).mockResolvedValueOnce(["output:worker-tagged"]);
    (redis as unknown as { xrevrange?: ReturnType<typeof vi.fn> }).xrevrange = vi.fn()
      .mockResolvedValueOnce([
        ["12-0", ["task_id", "task-2", "line", "latest retained"]],
        ["11-0", ["task_id", "task-2", "line", "earliest retained for latest task"]],
        ["10-0", ["task_id", "task-1", "line", "previous task"]],
        ["9-0", ["type", "task_start", "task_id", "task-1"]],
      ]);

    await expect(findTaskStart("worker-tagged")).resolves.toEqual({
      stream: "output:worker-tagged",
      entryId: "11-0",
    });
  });

  it("prefers a matching task_start over latest tagged fallback", async () => {
    vi.mocked(scanKeysMany).mockResolvedValueOnce(["output:worker-tagged"]);
    (redis as unknown as { xrevrange?: ReturnType<typeof vi.fn> }).xrevrange = vi.fn()
      .mockResolvedValueOnce([
        ["12-0", ["task_id", "task-2", "line", "latest retained"]],
        ["10-0", ["type", "task_start", "task_id", "task-2"]],
        ["9-0", ["task_id", "task-1", "line", "previous task"]],
      ]);

    await expect(findTaskStart("worker-tagged")).resolves.toEqual({
      stream: "output:worker-tagged",
      entryId: "10-0",
    });
  });

  it("prefers task_start over tagged output fallback", async () => {
    vi.mocked(scanKeysMany).mockResolvedValueOnce(["output:worker-tagged"]);
    (redis as unknown as { xrevrange?: ReturnType<typeof vi.fn> }).xrevrange = vi.fn()
      .mockResolvedValueOnce([
        ["10-0", ["task_id", "task-1", "line", "latest"]],
        ["8-0", ["type", "task_start", "task_id", "task-1"]],
        ["7-0", ["task_id", "task-1", "line", "previous retained"]],
      ]);

    await expect(findTaskStart("worker-tagged", "task-1")).resolves.toEqual({
      stream: "output:worker-tagged",
      entryId: "8-0",
    });
  });

  it("surfaces Redis task-start lookup failures to the task-output socket", async () => {
    vi.mocked(scanKeysMany).mockResolvedValueOnce(["output:worker-error"]);
    (redis as unknown as { xrevrange?: ReturnType<typeof vi.fn> }).xrevrange = vi.fn()
      .mockRejectedValueOnce(new Error("xrevrange unavailable"));

    await expect(findTaskStart("worker-error", "task-1")).rejects.toThrow(
      "xrevrange unavailable",
    );
  });

  it("advances the cursor over entries that do not render output", async () => {
    vi.mocked(scanKeysMany).mockResolvedValueOnce(["output:worker-ignore"]);
    (redis as unknown as { xrange?: ReturnType<typeof vi.fn> }).xrange = vi.fn().mockResolvedValueOnce([
      ["1-0", ["line", JSON.stringify({ type: "session.started" })]],
      ["2-0", ["line", JSON.stringify({ type: "session.updated" })]],
    ]);

    const result = await readTaskOutput("worker-ignore", "1-0", "0-0");

    expect(result).toEqual({ entries: [], lastId: "2-0", done: false });
  });

  it("stops exact task output when a later task starts without a matching task_end", async () => {
    vi.mocked(scanKeysMany).mockResolvedValueOnce(["output:worker-boundary"]);
    (redis as unknown as { xrange?: ReturnType<typeof vi.fn> }).xrange = vi.fn().mockResolvedValueOnce([
      ["1-0", ["type", "task_start", "task_id", "task-1", "resource", "pr #1"]],
      ["2-0", ["line", JSON.stringify({ type: "text", data: "hello" })]],
      ["3-0", ["type", "task_start", "task_id", "task-2", "resource", "pr #2"]],
      ["4-0", ["line", JSON.stringify({ type: "text", data: "other task" })]],
    ]);

    const result = await readTaskOutput("worker-boundary", "1-0", "0-0", "task-1");

    expect(result.done).toBe(true);
    expect(result.lastId).toBe("3-0");
    expect(result.entries.map((entry) => entry.line).join("\n")).toContain("hello");
    expect(result.entries.map((entry) => entry.line).join("\n")).not.toContain("other task");
  });

  it("does not render unrelated retained lines when a cached task start was trimmed", async () => {
    (redis as unknown as { xrange?: ReturnType<typeof vi.fn> }).xrange = vi.fn().mockResolvedValueOnce([
      ["2-0", ["line", JSON.stringify({ type: "text", data: "unrelated retained line" })]],
      ["3-0", ["type", "task_start", "task_id", "task-2", "resource", "pr #2"]],
      ["4-0", ["line", JSON.stringify({ type: "text", data: "other task" })]],
    ]);

    const result = await readTaskOutputFromStream(
      "output:worker-trimmed",
      "1-0",
      "0-0",
      "task-1",
    );

    expect(result).toEqual({
      entries: [],
      lastId: "4-0",
      done: true,
      unavailable: true,
    });
  });

  it("marks exact output unavailable when the located task start disappears before reading", async () => {
    (redis as unknown as { xrange?: ReturnType<typeof vi.fn> }).xrange = vi.fn().mockResolvedValueOnce([]);

    const result = await readTaskOutputFromStream(
      "output:worker-race",
      "1-0",
      "0-0",
      "task-1",
    );

    expect(result).toEqual({
      entries: [],
      lastId: "0-0",
      done: true,
      unavailable: true,
    });
  });

  it("skips unrelated retained entries while recovering tagged exact output after trim", async () => {
    (redis as unknown as { xrange?: ReturnType<typeof vi.fn> }).xrange = vi.fn().mockResolvedValueOnce([
      ["2-0", ["task_id", "other-task", "line", JSON.stringify({ type: "text", data: "other" })]],
      ["3-0", ["type", "task_start", "task_id", "newer-task", "resource", "pr #2"]],
      ["4-0", ["task_id", "task-1", "line", JSON.stringify({ type: "text", data: "target survived" })]],
      ["5-0", ["type", "task_end", "task_id", "task-1", "status", "COMPLETED"]],
    ]);

    const result = await readTaskOutputFromStream(
      "output:worker-trimmed",
      "1-0",
      "0-0",
      "task-1",
    );

    expect(result.done).toBe(true);
    expect(result.lastId).toBe("5-0");
    expect(result.unavailable).toBeUndefined();
    expect(result.entries.map((entry) => entry.line).join("\n")).toContain("target survived");
    expect(result.entries.map((entry) => entry.line).join("\n")).not.toContain("other");
  });

  it("marks exact output unavailable when a resumed cursor was trimmed before untagged lines", async () => {
    (redis as unknown as { xrange?: ReturnType<typeof vi.fn> }).xrange = vi.fn()
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([
        ["6-0", ["line", JSON.stringify({ type: "text", data: "unrelated retained line" })]],
      ]);

    const result = await readTaskOutputFromStream(
      "output:worker-trimmed",
      "1-0",
      "5-0",
      "task-1",
    );

    expect(result).toEqual({
      entries: [],
      lastId: "6-0",
      done: true,
      unavailable: true,
    });
  });

  it("does not trust a resumed cursor from another task for legacy untagged output", async () => {
    const xrange = vi.fn()
      .mockResolvedValueOnce([
        ["5-0", ["task_id", "other-task", "line", JSON.stringify({ type: "text", data: "other" })]],
      ])
      .mockResolvedValueOnce([
        ["1-0", ["type", "task_start", "task_id", "task-1", "resource", "pr #1"]],
        ["2-0", ["line", JSON.stringify({ type: "text", data: "target legacy line" })]],
        ["3-0", ["type", "task_start", "task_id", "other-task", "resource", "pr #2"]],
      ]);
    (redis as unknown as { xrange?: ReturnType<typeof vi.fn> }).xrange = xrange;

    const result = await readTaskOutputFromStream(
      "output:worker-foreign-cursor",
      "1-0",
      "5-0",
      "task-1",
    );

    expect(xrange).toHaveBeenNthCalledWith(
      1,
      "output:worker-foreign-cursor",
      "5-0",
      "5-0",
      "COUNT",
      1,
    );
    expect(xrange).toHaveBeenNthCalledWith(
      2,
      "output:worker-foreign-cursor",
      "1-0",
      "+",
      "COUNT",
      100,
    );
    expect(result.done).toBe(true);
    expect(result.lastId).toBe("3-0");
    expect(result.entries.map((entry) => entry.line).join("\n")).toContain("target legacy line");
    expect(result.entries.map((entry) => entry.line).join("\n")).not.toContain("other");
  });

  it("resumes after a legacy untagged cursor when the nearest task boundary matches", async () => {
    const xrange = vi.fn()
      .mockResolvedValueOnce([
        ["2-0", ["line", JSON.stringify({ type: "text", data: "already sent" })]],
      ])
      .mockResolvedValueOnce([
        ["3-0", ["line", JSON.stringify({ type: "text", data: "new output" })]],
      ]);
    const xrevrange = vi.fn().mockResolvedValueOnce([
      ["2-0", ["line", JSON.stringify({ type: "text", data: "already sent" })]],
      ["1-0", ["type", "task_start", "task_id", "task-1", "resource", "pr #1"]],
    ]);
    (redis as unknown as {
      xrange?: ReturnType<typeof vi.fn>;
      xrevrange?: ReturnType<typeof vi.fn>;
    }).xrange = xrange;
    (redis as unknown as { xrevrange?: ReturnType<typeof vi.fn> }).xrevrange = xrevrange;

    const result = await readTaskOutputFromStream(
      "output:worker-legacy",
      "1-0",
      "2-0",
      "task-1",
    );

    expect(xrange).toHaveBeenNthCalledWith(
      2,
      "output:worker-legacy",
      "(2-0",
      "+",
      "COUNT",
      100,
    );
    expect(result).toEqual({
      entries: [{ id: "3-0", line: "new output" }],
      lastId: "3-0",
      done: false,
    });
  });

  it("keeps tagged exact output after a resumed cursor was trimmed", async () => {
    (redis as unknown as { xrange?: ReturnType<typeof vi.fn> }).xrange = vi.fn()
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([
        ["6-0", ["task_id", "task-1", "line", JSON.stringify({ type: "text", data: "still here" })]],
      ]);

    const result = await readTaskOutputFromStream(
      "output:worker-trimmed",
      "1-0",
      "5-0",
      "task-1",
    );

    expect(result).toEqual({
      entries: [{ id: "6-0", line: "still here" }],
      lastId: "6-0",
      done: false,
    });
  });

  it("keeps tagged fallback output when a later task starts in the same read", async () => {
    (redis as unknown as { xrange?: ReturnType<typeof vi.fn> }).xrange = vi.fn().mockResolvedValueOnce([
      ["6-0", ["task_id", "task-1", "line", JSON.stringify({ type: "text", data: "retained output" })]],
      ["7-0", ["type", "task_start", "task_id", "other-task", "resource", "pr #2"]],
    ]);

    const result = await readTaskOutputFromStream(
      "output:worker-tagged",
      "5-0",
      "0-0",
      "task-1",
    );

    expect(result).toEqual({
      entries: [{ id: "6-0", line: "retained output" }],
      lastId: "7-0",
      done: true,
    });
  });

  it("caps accumulated streamed text chunks", async () => {
    (redis as unknown as { xrange?: ReturnType<typeof vi.fn> }).xrange = vi.fn().mockResolvedValueOnce([
      ["1-0", ["line", JSON.stringify({ type: "text", data: "a".repeat(2500) })]],
      ["2-0", ["line", JSON.stringify({ type: "text", data: "b".repeat(2500) })]],
    ]);

    const result = await readTaskOutputFromStream(
      "output:worker-long",
      "1-0",
      "0-0",
    );

    expect(result.entries).toHaveLength(1);
    expect(result.entries[0].id).toBe("2-0");
    expect(result.entries[0].line.length).toBeLessThanOrEqual(4000);
    expect(result.entries[0].line).toContain("[truncated]");
  });

  it("surfaces Redis read failures to the task-output socket", async () => {
    (redis as unknown as { xrange?: ReturnType<typeof vi.fn> }).xrange = vi.fn()
      .mockRejectedValueOnce(new Error("redis unavailable"));

    await expect(readTaskOutputFromStream(
      "output:worker-error",
      "1-0",
      "0-0",
      "task-1",
    )).rejects.toThrow("redis unavailable");
  });

  it("invalidates cached task starts that Redis has trimmed", async () => {
    vi.mocked(scanKeysMany)
      .mockResolvedValueOnce(["output:worker-cache"])
      .mockResolvedValueOnce(["output:worker-cache"]);
    (redis as unknown as {
      xrevrange?: ReturnType<typeof vi.fn>;
      xrange?: ReturnType<typeof vi.fn>;
    }).xrevrange = vi.fn()
      .mockResolvedValueOnce([
        ["10-0", ["type", "task_start", "task_id", "task-1"]],
      ])
      .mockResolvedValueOnce([]);

    await expect(findTaskStart("worker-cache", "task-1")).resolves.toEqual({
      stream: "output:worker-cache",
      entryId: "10-0",
    });

    (redis as unknown as { xrange?: ReturnType<typeof vi.fn> }).xrange = vi.fn()
      .mockResolvedValueOnce([]);

    await expect(findTaskStart("worker-cache", "task-1")).resolves.toBeNull();
  });
});

describe("taskOutputUnavailableMessage", () => {
  it("includes the full task identifier and why output may be missing", () => {
    expect(taskOutputUnavailableMessage("1234567890abcdef")).toContain("1234567890abcdef");
    expect(taskOutputUnavailableMessage("1234567890abcdef")).toContain("trimmed");
  });

  it("keeps unavailable messages distinct for task IDs with the same prefix", () => {
    expect(taskOutputUnavailableMessage("task-shared-alpha")).not.toBe(
      taskOutputUnavailableMessage("task-shared-bravo"),
    );
  });
});

describe("normalizeTaskOutputCursor", () => {
  it("accepts valid Redis stream IDs and rejects malformed cursors", () => {
    expect(normalizeTaskOutputCursor(" 9-10 ")).toBe("9-10");
    expect(normalizeTaskOutputCursor("9-*")).toBe("0-0");
    expect(normalizeTaskOutputCursor(null)).toBe("0-0");
  });
});

describe("taskOutputReadFailureMessage", () => {
  it("tells the operator that Redis/output streams should be checked", () => {
    expect(taskOutputReadFailureMessage()).toContain("Redis");
    expect(taskOutputReadFailureMessage()).toContain("worker output streams");
  });
});
