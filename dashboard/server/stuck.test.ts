import { beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("./redis.js", () => ({
  redis: {},
  dashboardRedisKeyPatterns: vi.fn((suffixes: string[]) =>
    suffixes.flatMap((suffix) => [`*:${suffix}`, suffix])
  ),
  scanKeysMany: vi.fn(),
}));

import { dashboardRedisKeyPatterns, redis, scanKeysMany } from "./redis.js";
import type { SystemSnapshot } from "./types.js";
import {
  attemptStuckSeverity,
  detectStuck,
  parseAttemptLabel,
  pendingAgeSeconds,
  pendingEntryFromKey,
  pendingTtlState,
} from "./stuck.js";

function snapshot(overrides: Partial<SystemSnapshot> = {}): SystemSnapshot {
  return {
    redis_ok: true,
    fetched_at: "2026-06-16T00:00:00Z",
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
      pending_task_ttl_seconds: 16500,
      lock_ttl_seconds: 180,
    },
    ...overrides,
  };
}

beforeEach(() => {
  vi.mocked(dashboardRedisKeyPatterns).mockImplementation((suffixes: string[]) =>
    suffixes.flatMap((suffix) => [`*:${suffix}`, suffix])
  );
  vi.mocked(scanKeysMany).mockReset();
  vi.mocked(scanKeysMany).mockResolvedValue([]);
  (redis as unknown as { call?: ReturnType<typeof vi.fn> }).call = vi.fn()
    .mockResolvedValue([]);
  (redis as unknown as { xrange?: ReturnType<typeof vi.fn> }).xrange = vi.fn()
    .mockResolvedValue([]);
});

describe("pendingEntryFromKey", () => {
  it("maps prefixed pending markers to matching lock keys", () => {
    expect(pendingEntryFromKey("project-a:pending:pr:owner/repo:42")).toEqual({
      key: "project-a:pending:pr:owner/repo:42",
      lockKey: "project-a:lock:pr:owner/repo:42",
      prefix: "project-a",
      resourceType: "pr",
      repo: "owner/repo",
      resourceId: "42",
    });
  });

  it("maps unprefixed pending markers to unprefixed lock keys", () => {
    expect(pendingEntryFromKey("pending:issue:owner/repo:7")).toEqual({
      key: "pending:issue:owner/repo:7",
      lockKey: "lock:issue:owner/repo:7",
      prefix: null,
      resourceType: "issue",
      repo: "owner/repo",
      resourceId: "7",
    });
  });

  it("ignores unsupported pending marker shapes", () => {
    expect(pendingEntryFromKey("pending:branch:owner/repo:main")).toBeNull();
  });
});

describe("parseAttemptLabel", () => {
  it("extracts repo and PR number from dashboard attempt labels", () => {
    expect(parseAttemptLabel("owner/repo PR #42")).toEqual({
      prefix: null,
      resourceType: "pr",
      repo: "owner/repo",
      resourceId: "42",
    });
  });

  it("extracts repo and issue number from dashboard attempt labels", () => {
    expect(parseAttemptLabel("owner/repo Issue #7")).toEqual({
      prefix: null,
      resourceType: "issue",
      repo: "owner/repo",
      resourceId: "7",
    });
  });

  it("extracts Redis namespace prefixes in dashboard attempt labels", () => {
    expect(parseAttemptLabel("[project-a] owner/repo PR #42")).toEqual({
      prefix: "project-a",
      resourceType: "pr",
      repo: "owner/repo",
      resourceId: "42",
    });
  });

  it("keeps legacy labels without repo as repo-less resource ids", () => {
    expect(parseAttemptLabel("42")).toEqual({
      prefix: null,
      resourceType: "pr",
      repo: null,
      resourceId: "42",
    });
  });
});

describe("attemptStuckSeverity", () => {
  it("uses the configured retry threshold", () => {
    expect(attemptStuckSeverity(3, 5)).toBeNull();
    expect(attemptStuckSeverity(4, 5)).toBe("warning");
    expect(attemptStuckSeverity(5, 5)).toBe("critical");
  });
});

describe("detectStuck", () => {
  it("scopes pending marker scans to configured dashboard Redis prefixes", async () => {
    vi.mocked(dashboardRedisKeyPatterns).mockImplementation((suffixes: string[]) =>
      suffixes.map((suffix) => `orcest:${suffix}`)
    );

    await detectStuck(snapshot());

    expect(dashboardRedisKeyPatterns).toHaveBeenCalledWith(["pending:*"]);
    expect(scanKeysMany).toHaveBeenCalledWith(["orcest:pending:*"]);
  });

  it("marks stuck task detection degraded when required snapshot inputs are partial", async () => {
    const stuckSnapshot = snapshot({
      degraded_sections: ["consumer groups", "queued tasks", "queue depths"],
    });

    await expect(detectStuck(stuckSnapshot)).resolves.toEqual([]);

    expect(stuckSnapshot.degraded_sections).toEqual(expect.arrayContaining([
      "consumer groups",
      "queued tasks",
      "queue depths",
      "stuck tasks",
    ]));
  });

  it("uses pending marker created_at instead of inferring age from mismatched policy TTL", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-16T00:03:01Z"));
    try {
      vi.mocked(scanKeysMany).mockResolvedValue(["orcest:pending:pr:owner/repo:42"]);
      const pipeline = {
        exists: vi.fn().mockReturnThis(),
        ttl: vi.fn().mockReturnThis(),
        get: vi.fn().mockReturnThis(),
        exec: vi.fn().mockResolvedValue([
          [null, 0],
          [null, 5700],
          [null, JSON.stringify({
            task_id: "task-1",
            created_at: "2026-06-16T00:00:00Z",
          })],
        ]),
      };
      (redis as unknown as { pipeline: ReturnType<typeof vi.fn> }).pipeline =
        vi.fn(() => pipeline);

      const stuck = await detectStuck(snapshot());

      expect(pipeline.exists).toHaveBeenCalledWith("orcest:lock:pr:owner/repo:42");
      expect(pipeline.ttl).toHaveBeenCalledWith("orcest:pending:pr:owner/repo:42");
      expect(pipeline.get).toHaveBeenCalledWith("orcest:pending:pr:owner/repo:42");
      expect(stuck).toContainEqual({
        prefix: "orcest",
        resource_type: "pr",
        repo: "owner/repo",
        resource_id: "42",
        reason: "Queued but no worker has picked it up (age: 181s, pending TTL: 5700s)",
        severity: "warning",
      });
    } finally {
      vi.useRealTimers();
    }
  });

  it("connects orphaned pending markers to their queued worker stream", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-16T00:03:01Z"));
    try {
      vi.mocked(scanKeysMany).mockResolvedValue([
        "bbr-platform:pending:issue:bluebamboollc/bbr-platform:4251",
      ]);
      const pipeline = {
        exists: vi.fn().mockReturnThis(),
        ttl: vi.fn().mockReturnThis(),
        get: vi.fn().mockReturnThis(),
        exec: vi.fn().mockResolvedValue([
          [null, 0],
          [null, 5700],
          [null, JSON.stringify({
            task_id: "task-4251",
            created_at: "2026-06-16T00:00:00Z",
          })],
        ]),
      };
      (redis as unknown as { pipeline: ReturnType<typeof vi.fn> }).pipeline =
        vi.fn(() => pipeline);

      const stuck = await detectStuck(snapshot({
        queued_tasks: [{
          entry_id: "1718496000000-0",
          task_id: "task-4251",
          task_type: "implement_issue",
          repo: "bluebamboollc/bbr-platform",
          resource_type: "issue",
          resource_id: "4251",
          created_at: "2026-06-16T00:00:00Z",
          stream: "orcest:tasks:issue:codex",
        }],
        consumer_groups: [{
          stream: "orcest:tasks:issue:codex",
          name: "workers",
          consumers: 1,
          pending: 0,
          lag: 1,
        }],
      }));

      expect(stuck).toContainEqual({
        prefix: "bbr-platform",
        resource_type: "issue",
        repo: "bluebamboollc/bbr-platform",
        resource_id: "4251",
        reason: "Queued but no worker has picked it up (age: 181s, pending TTL: 5700s)",
        severity: "warning",
        stream: "orcest:tasks:issue:codex",
        consumer_group: "workers",
        entry_id: "1718496000000-0",
        task_id: "task-4251",
      });
    } finally {
      vi.useRealTimers();
    }
  });

  it("suppresses queued pickup warnings covered by no-consumer critical backlog", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-16T00:03:01Z"));
    try {
      vi.mocked(scanKeysMany).mockResolvedValue([
        "bbr-platform:pending:issue:bluebamboollc/bbr-platform:4251",
      ]);
      const pipeline = {
        exists: vi.fn().mockReturnThis(),
        ttl: vi.fn().mockReturnThis(),
        get: vi.fn().mockReturnThis(),
        exec: vi.fn().mockResolvedValue([
          [null, 0],
          [null, 5700],
          [null, JSON.stringify({
            task_id: "task-4251",
            created_at: "2026-06-16T00:00:00Z",
          })],
        ]),
      };
      (redis as unknown as { pipeline: ReturnType<typeof vi.fn> }).pipeline =
        vi.fn(() => pipeline);

      const stuck = await detectStuck(snapshot({
        queued_tasks: [{
          prefix: "bbr-platform",
          entry_id: "1718496000000-0",
          task_id: "task-4251",
          task_type: "implement_issue",
          repo: "bluebamboollc/bbr-platform",
          resource_type: "issue",
          resource_id: "4251",
          created_at: "2026-06-16T00:00:00Z",
          stream: "orcest:tasks:issue:codex",
        }],
        consumer_groups: [{
          stream: "orcest:tasks:issue:codex",
          name: "workers",
          consumers: 0,
          pending: 0,
          lag: 1,
        }],
      }));

      expect(stuck).toEqual([{
        prefix: "bbr-platform",
        resource_type: "issue",
        repo: "bluebamboollc/bbr-platform",
        resource_id: "4251",
        reason: "Worker group has backlog but no consumers (1 lag)",
        severity: "critical",
        stream: "orcest:tasks:issue:codex",
        consumer_group: "workers",
        entry_id: "1718496000000-0",
        task_id: "task-4251",
        no_worker_consumers: true,
      }]);
    } finally {
      vi.useRealTimers();
    }
  });

  it("suppresses covered queued pickup warnings when queued previews have no prefix", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-16T00:03:01Z"));
    try {
      vi.mocked(scanKeysMany).mockResolvedValue([
        "bbr-platform:pending:issue:bluebamboollc/bbr-platform:4251",
      ]);
      const pipeline = {
        exists: vi.fn().mockReturnThis(),
        ttl: vi.fn().mockReturnThis(),
        get: vi.fn().mockReturnThis(),
        exec: vi.fn().mockResolvedValue([
          [null, 0],
          [null, 5700],
          [null, JSON.stringify({
            task_id: "task-4251",
            created_at: "2026-06-16T00:00:00Z",
          })],
        ]),
      };
      (redis as unknown as { pipeline: ReturnType<typeof vi.fn> }).pipeline =
        vi.fn(() => pipeline);

      const stuck = await detectStuck(snapshot({
        queued_tasks: [{
          entry_id: "1718496000000-0",
          task_id: "task-4251",
          task_type: "implement_issue",
          repo: "bluebamboollc/bbr-platform",
          resource_type: "issue",
          resource_id: "4251",
          created_at: "2026-06-16T00:00:00Z",
          stream: "orcest:tasks:issue:codex",
        }],
        consumer_groups: [{
          stream: "orcest:tasks:issue:codex",
          name: "workers",
          consumers: 0,
          pending: 0,
          lag: 1,
        }],
      }));

      expect(stuck).toEqual([{
        prefix: null,
        resource_type: "issue",
        repo: "bluebamboollc/bbr-platform",
        resource_id: "4251",
        reason: "Worker group has backlog but no consumers (1 lag)",
        severity: "critical",
        stream: "orcest:tasks:issue:codex",
        consumer_group: "workers",
        entry_id: "1718496000000-0",
        task_id: "task-4251",
        no_worker_consumers: true,
      }]);
    } finally {
      vi.useRealTimers();
    }
  });

  it("does not connect orphaned pending markers to queued tasks from another known prefix", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-16T00:03:01Z"));
    try {
      vi.mocked(scanKeysMany).mockResolvedValue([
        "project-a:pending:issue:owner/repo:42",
      ]);
      const pipeline = {
        exists: vi.fn().mockReturnThis(),
        ttl: vi.fn().mockReturnThis(),
        get: vi.fn().mockReturnThis(),
        exec: vi.fn().mockResolvedValue([
          [null, 0],
          [null, 5700],
          [null, JSON.stringify({
            task_id: "shared-task",
            created_at: "2026-06-16T00:00:00Z",
          })],
        ]),
      };
      (redis as unknown as { pipeline: ReturnType<typeof vi.fn> }).pipeline =
        vi.fn(() => pipeline);

      const stuck = await detectStuck(snapshot({
        queued_tasks: [{
          prefix: "project-b",
          entry_id: "1718496000000-0",
          task_id: "shared-task",
          task_type: "implement_issue",
          repo: "owner/repo",
          resource_type: "issue",
          resource_id: "42",
          created_at: "2026-06-16T00:00:00Z",
          stream: "project-b:tasks:issue:codex",
        }],
      }));

      expect(stuck).toEqual([{
        prefix: "project-a",
        resource_type: "issue",
        repo: "owner/repo",
        resource_id: "42",
        reason: "Queued but no worker has picked it up (age: 181s, pending TTL: 5700s)",
        severity: "warning",
      }]);
    } finally {
      vi.useRealTimers();
    }
  });

  it("reports queued worker streams with backlog and no consumers as stuck", async () => {
    const call = vi.fn().mockResolvedValue([]);
    (redis as unknown as { call: ReturnType<typeof vi.fn> }).call = call;

    const stuck = await detectStuck(snapshot({
      queued_tasks: [{
        prefix: "bbr-platform",
        entry_id: "1782736756141-0",
        task_id: "task-4251",
        task_type: "implement_issue",
        repo: "bluebamboollc/bbr-platform",
        resource_type: "issue",
        resource_id: "4251",
        created_at: "2026-06-29T12:39:16.140939+00:00",
        stream: "orcest:tasks:issue:codex",
      }],
      consumer_groups: [{
        stream: "orcest:tasks:issue:codex",
        name: "workers",
        consumers: 0,
        pending: 0,
        lag: 1,
      }],
    }));

    expect(call).not.toHaveBeenCalled();
    expect(stuck).toEqual([{
      prefix: "bbr-platform",
      resource_type: "issue",
      repo: "bluebamboollc/bbr-platform",
      resource_id: "4251",
      reason: "Worker group has backlog but no consumers (1 lag)",
      severity: "critical",
      stream: "orcest:tasks:issue:codex",
      consumer_group: "workers",
      entry_id: "1782736756141-0",
      task_id: "task-4251",
      no_worker_consumers: true,
    }]);
  });

  it("reports no-consumer backlogged streams even without queued task previews", async () => {
    const stuck = await detectStuck(snapshot({
      consumer_groups: [{
        stream: "orcest:tasks:issue:grok",
        name: "workers",
        consumers: 0,
        pending: 2,
        lag: 0,
      }],
    }));

    expect(stuck).toEqual([{
      prefix: null,
      resource_type: "stream",
      repo: null,
      resource_id: "orcest:tasks:issue:grok/workers",
      reason: "Worker group has backlog but no consumers (2 pending, 0 lag)",
      severity: "critical",
      stream: "orcest:tasks:issue:grok",
      consumer_group: "workers",
      no_worker_consumers: true,
    }]);
  });

  it("uses queue depth fallback as confirmed backlog when consumer lag is unavailable", async () => {
    const stuck = await detectStuck(snapshot({
      queue_depths: {
        "orcest:tasks:issue:grok": 3,
      },
      consumer_groups: [{
        stream: "orcest:tasks:issue:grok",
        name: "workers",
        consumers: 0,
        pending: 0,
        lag: null,
      }],
    }));

    expect(stuck).toEqual([{
      prefix: null,
      resource_type: "stream",
      repo: null,
      resource_id: "orcest:tasks:issue:grok/workers",
      reason: "Worker group has backlog but no consumers (lag unknown, 3 queued)",
      severity: "critical",
      stream: "orcest:tasks:issue:grok",
      consumer_group: "workers",
      no_worker_consumers: true,
    }]);
  });

  it("uses queued task previews as confirmed backlog when lag and depth are unavailable", async () => {
    const stuck = await detectStuck(snapshot({
      queued_tasks: [{
        prefix: "orcest",
        entry_id: "1782736756141-0",
        task_id: "task-4251",
        task_type: "implement_issue",
        repo: "bluebamboollc/bbr-platform",
        resource_type: "issue",
        resource_id: "4251",
        created_at: "2026-06-29T12:39:16.140939+00:00",
        stream: "orcest:tasks:issue:grok",
      }],
      consumer_groups: [{
        stream: "orcest:tasks:issue:grok",
        name: "workers",
        consumers: 0,
        pending: 0,
        lag: null,
      }],
    }));

    expect(stuck).toEqual([{
      prefix: "orcest",
      resource_type: "issue",
      repo: "bluebamboollc/bbr-platform",
      resource_id: "4251",
      reason: "Worker group has backlog but no consumers (lag unknown, 1 queued preview)",
      severity: "critical",
      stream: "orcest:tasks:issue:grok",
      consumer_group: "workers",
      entry_id: "1782736756141-0",
      task_id: "task-4251",
      no_worker_consumers: true,
    }]);
  });

  it("includes pending and queued preview evidence when lag is unavailable", async () => {
    const stuck = await detectStuck(snapshot({
      queued_tasks: [{
        prefix: "orcest",
        entry_id: "1782736756141-0",
        task_id: "task-4251",
        task_type: "implement_issue",
        repo: "bluebamboollc/bbr-platform",
        resource_type: "issue",
        resource_id: "4251",
        created_at: "2026-06-29T12:39:16.140939+00:00",
        stream: "orcest:tasks:issue:grok",
      }],
      consumer_groups: [{
        stream: "orcest:tasks:issue:grok",
        name: "workers",
        consumers: 0,
        pending: 2,
        lag: null,
      }],
    }));

    expect(stuck).toEqual([{
      prefix: "orcest",
      resource_type: "issue",
      repo: "bluebamboollc/bbr-platform",
      resource_id: "4251",
      reason: "Worker group has backlog but no consumers (2 pending, lag unknown, 1 queued preview)",
      severity: "critical",
      stream: "orcest:tasks:issue:grok",
      consumer_group: "workers",
      entry_id: "1782736756141-0",
      task_id: "task-4251",
      no_worker_consumers: true,
    }]);
  });

  it("uses queue depth instead of capped queued previews in no-consumer reasons", async () => {
    const stuck = await detectStuck(snapshot({
      queue_depths: {
        "orcest:tasks:issue:grok": 28,
      },
      queued_tasks: [{
        prefix: "orcest",
        entry_id: "1782736756141-0",
        task_id: "task-4251",
        task_type: "implement_issue",
        repo: "bluebamboollc/bbr-platform",
        resource_type: "issue",
        resource_id: "4251",
        created_at: "2026-06-29T12:39:16.140939+00:00",
        stream: "orcest:tasks:issue:grok",
      }],
      consumer_groups: [{
        stream: "orcest:tasks:issue:grok",
        name: "workers",
        consumers: 0,
        pending: 0,
        lag: null,
      }],
    }));

    expect(stuck).toEqual([{
      prefix: "orcest",
      resource_type: "issue",
      repo: "bluebamboollc/bbr-platform",
      resource_id: "4251",
      reason: "Worker group has backlog but no consumers (lag unknown, 28 queued)",
      severity: "critical",
      stream: "orcest:tasks:issue:grok",
      consumer_group: "workers",
      entry_id: "1782736756141-0",
      task_id: "task-4251",
      no_worker_consumers: true,
    }]);
  });

  it("does not report no-consumer streams as stuck when lag is unknown but backlog is not confirmed", async () => {
    const stuck = await detectStuck(snapshot({
      consumer_groups: [{
        stream: "orcest:tasks:issue:grok",
        name: "workers",
        consumers: 0,
        pending: 0,
        lag: null,
      }],
    }));

    expect(stuck).toEqual([]);
  });

  it("reports active locks that have no expiry", async () => {
    const stuck = await detectStuck(snapshot({
      locks: [{
        lock_key: "orcest:lock:pr:owner/repo:42",
        prefix: "orcest",
        resource: "owner/repo:42",
        resource_type: "pr",
        repo: "owner/repo",
        resource_id: "42",
        owner: "worker-1",
        ttl: -1,
        task_id: "task-123",
        pending_created_at: null,
      }],
    }));

    expect(stuck).toContainEqual({
      prefix: "orcest",
      resource_type: "pr",
      repo: "owner/repo",
      resource_id: "42",
      reason: "Lock has no TTL — resource can remain blocked indefinitely",
      severity: "critical",
    });
  });

  it("keeps snapshot-derived stuck signals when pending marker discovery fails", async () => {
    vi.mocked(scanKeysMany).mockRejectedValue(new Error("scan failed"));

    const stuckSnapshot = snapshot({
      locks: [{
        lock_key: "orcest:lock:pr:owner/repo:42",
        prefix: "orcest",
        resource: "owner/repo:42",
        resource_type: "pr",
        repo: "owner/repo",
        resource_id: "42",
        owner: "worker-1",
        ttl: -1,
        task_id: "task-123",
        pending_created_at: null,
      }],
      attempt_counts: {
        "[orcest] owner/repo PR #42": 3,
      },
    });
    const stuck = await detectStuck(stuckSnapshot);

    expect(stuck).toEqual(expect.arrayContaining([
      {
        prefix: "orcest",
        resource_type: "pr",
        repo: "owner/repo",
        resource_id: "42",
        reason: "Lock has no TTL — resource can remain blocked indefinitely",
        severity: "critical",
      },
      {
        prefix: "orcest",
        resource_type: "pr",
        repo: "owner/repo",
        resource_id: "42",
        reason: "Attempt count at max (3/3)",
        severity: "critical",
      },
    ]));
    expect(stuckSnapshot.degraded_sections).toContain("stuck tasks");
  });

  it("skips pending markers when Redis pipeline commands fail", async () => {
    vi.mocked(scanKeysMany).mockResolvedValue(["orcest:pending:pr:owner/repo:42"]);
    const pipeline = {
      exists: vi.fn().mockReturnThis(),
      ttl: vi.fn().mockReturnThis(),
      get: vi.fn().mockReturnThis(),
      exec: vi.fn().mockResolvedValue([
        [null, 0],
        [new Error("ttl failed"), null],
        [null, JSON.stringify({
          task_id: "task-1",
          created_at: "2026-06-16T00:00:00Z",
        })],
      ]),
    };
    (redis as unknown as { pipeline: ReturnType<typeof vi.fn> }).pipeline =
      vi.fn(() => pipeline);
    const stuckSnapshot = snapshot();

    await expect(detectStuck(stuckSnapshot)).resolves.toEqual([]);
    expect(stuckSnapshot.degraded_sections).toContain("stuck tasks");
  });

  it("queries consumer-group pending entries by idle threshold", async () => {
    const call = vi.fn().mockResolvedValue([
      ["20-0", "worker-1", 181000, 2],
    ]);
    (redis as unknown as { call: ReturnType<typeof vi.fn> }).call = call;
    const xrange = vi.fn().mockResolvedValue([
      ["20-0", [
        "id", "task-1",
        "key_prefix", "orcest",
        "resource_type", "pr",
        "repo", "owner/repo",
        "resource_id", "42",
      ]],
    ]);
    (redis as unknown as { xrange: ReturnType<typeof vi.fn> }).xrange = xrange;

    const stuck = await detectStuck(snapshot({
      consumer_groups: [{
        stream: "tasks:claude",
        name: "workers",
        consumers: 1,
        pending: 25,
        lag: 0,
      }],
    }));

    expect(call).toHaveBeenCalledWith(
      "XPENDING",
      "tasks:claude",
      "workers",
      "IDLE",
      180000,
      "-",
      "+",
      "100",
    );
    expect(xrange).toHaveBeenCalledWith("tasks:claude", "20-0", "20-0");
      expect(stuck).toContainEqual({
        prefix: "orcest",
        resource_type: "pr",
        repo: "owner/repo",
        resource_id: "42",
        reason: "Pending entry idle for 181s (2 deliveries)",
        severity: "critical",
        stream: "tasks:claude",
        consumer_group: "workers",
        entry_id: "20-0",
        task_id: "task-1",
      });
    });

  it("does not coerce malformed pending idle and delivery counters into stuck alerts", async () => {
    const call = vi.fn().mockResolvedValue([
      ["19-0", "worker-1", [181000], [2]],
      ["20-0", "worker-1", "181000", "2"],
    ]);
    (redis as unknown as { call: ReturnType<typeof vi.fn> }).call = call;
    const xrange = vi.fn().mockResolvedValue([
      ["20-0", [
        "id", "task-20",
        "key_prefix", "orcest",
        "resource_type", "pr",
        "repo", "owner/repo",
        "resource_id", "20",
      ]],
    ]);
    (redis as unknown as { xrange: ReturnType<typeof vi.fn> }).xrange = xrange;

    const stuck = await detectStuck(snapshot({
      consumer_groups: [{
        stream: "tasks:claude",
        name: "workers",
        consumers: 1,
        pending: 2,
        lag: 0,
      }],
    }));

    expect(xrange).toHaveBeenCalledTimes(1);
    expect(xrange).toHaveBeenCalledWith("tasks:claude", "20-0", "20-0");
    expect(stuck).toEqual([{
      prefix: "orcest",
      resource_type: "pr",
      repo: "owner/repo",
      resource_id: "20",
      reason: "Pending entry idle for 181s (2 deliveries)",
      severity: "critical",
      stream: "tasks:claude",
      consumer_group: "workers",
      entry_id: "20-0",
      task_id: "task-20",
    }]);
  });

  it("pages pending entries so stale fallback rows past the first page are detected", async () => {
    const firstPage = Array.from({ length: 100 }, (_, index) => [
      `${index + 1}-0`,
      "worker-1",
      1000,
      1,
    ]);
    const call = vi.fn()
      .mockRejectedValueOnce(new Error("XPENDING IDLE unsupported"))
      .mockResolvedValueOnce(firstPage)
      .mockResolvedValueOnce([
        ["101-0", "worker-1", 181000, 2],
      ]);
    (redis as unknown as { call: ReturnType<typeof vi.fn> }).call = call;
    (redis as unknown as { xrange: ReturnType<typeof vi.fn> }).xrange = vi.fn()
      .mockResolvedValue([["101-0", [
        "id", "task-101",
        "key_prefix", "orcest",
        "resource_type", "pr",
        "repo", "owner/repo",
        "resource_id", "101",
      ]]]);

    const stuck = await detectStuck(snapshot({
      consumer_groups: [{
        stream: "tasks:claude",
        name: "workers",
        consumers: 1,
        pending: 101,
        lag: 0,
      }],
    }));

    expect(call).toHaveBeenNthCalledWith(
      1,
      "XPENDING",
      "tasks:claude",
      "workers",
      "IDLE",
      180000,
      "-",
      "+",
      "100",
    );
    expect(call).toHaveBeenNthCalledWith(
      2,
      "XPENDING",
      "tasks:claude",
      "workers",
      "-",
      "+",
      "100",
    );
    expect(call).toHaveBeenNthCalledWith(
      3,
      "XPENDING",
      "tasks:claude",
      "workers",
      "100-1",
      "+",
      "100",
    );
    expect(stuck).toContainEqual({
      prefix: "orcest",
      resource_type: "pr",
      repo: "owner/repo",
      resource_id: "101",
      reason: "Pending entry idle for 181s (2 deliveries)",
      severity: "critical",
      stream: "tasks:claude",
      consumer_group: "workers",
      entry_id: "101-0",
      task_id: "task-101",
    });
  });

  it("does not report idle worker-group entries while their task has an active lock", async () => {
    const call = vi.fn().mockResolvedValue([
      ["20-0", "worker-1", 181000, 2],
    ]);
    (redis as unknown as { call: ReturnType<typeof vi.fn> }).call = call;
    (redis as unknown as { xrange: ReturnType<typeof vi.fn> }).xrange = vi.fn()
      .mockResolvedValue([["20-0", ["id", "task-1"]]]);

    const stuck = await detectStuck(snapshot({
      locks: [{
        lock_key: "orcest:lock:pr:owner/repo:42",
        prefix: "orcest",
        resource: "owner/repo:42",
        resource_type: "pr",
        repo: "owner/repo",
        resource_id: "42",
        owner: "worker-1",
        ttl: 120,
        task_id: "task-1",
        pending_created_at: "2026-06-16T00:00:00Z",
      }],
      consumer_groups: [{
        stream: "tasks:claude",
        name: "workers",
        consumers: 1,
        pending: 1,
        lag: 0,
      }],
    }));

    expect(stuck).toEqual([]);
  });

  it("does not report idle worker-group entries while their resource has an active lock without task id", async () => {
    const call = vi.fn().mockResolvedValue([
      ["20-0", "worker-1", 181000, 2],
    ]);
    (redis as unknown as { call: ReturnType<typeof vi.fn> }).call = call;
    (redis as unknown as { xrange: ReturnType<typeof vi.fn> }).xrange = vi.fn()
      .mockResolvedValue([["20-0", [
        "key_prefix", "orcest",
        "resource_type", "pr",
        "repo", "owner/repo",
        "resource_id", "42",
      ]]]);

    const stuck = await detectStuck(snapshot({
      locks: [{
        lock_key: "orcest:lock:pr:owner/repo:42",
        prefix: "orcest",
        resource: "owner/repo:42",
        resource_type: "pr",
        repo: "owner/repo",
        resource_id: "42",
        owner: "worker-1",
        ttl: 120,
        task_id: null,
        pending_created_at: "2026-06-16T00:00:00Z",
      }],
      consumer_groups: [{
        stream: "tasks:claude",
        name: "workers",
        consumers: 1,
        pending: 1,
        lag: 0,
      }],
    }));

    expect(stuck).toEqual([]);
  });

  it("marks stuck tasks degraded when XPENDING paging reaches the page cap", async () => {
    const fullPage = Array.from({ length: 100 }, (_, index) => [
      `${index + 1}-0`,
      "worker-1",
      181000,
      2,
    ]);
    const call = vi.fn().mockResolvedValue(fullPage);
    const xrange = vi.fn();
    (redis as unknown as { call: ReturnType<typeof vi.fn> }).call = call;
    (redis as unknown as { xrange: ReturnType<typeof vi.fn> }).xrange = xrange;

    const stuckSnapshot = snapshot({
      consumer_groups: [{
        stream: "tasks:claude",
        name: "workers",
        consumers: 1,
        pending: 2500,
        lag: 0,
      }],
    });
    const stuck = await detectStuck(stuckSnapshot);

    expect(stuck).toEqual([]);
    expect(call).toHaveBeenCalledTimes(20);
    expect(xrange).not.toHaveBeenCalled();
    expect(stuckSnapshot.degraded_sections).toContain("stuck tasks");
  });

  it("does not report idle worker-group entries when active locks are partial", async () => {
    const call = vi.fn().mockResolvedValue([
      ["20-0", "worker-1", 181000, 2],
    ]);
    (redis as unknown as { call: ReturnType<typeof vi.fn> }).call = call;

    const stuckSnapshot = snapshot({
      degraded_sections: ["active locks"],
      consumer_groups: [{
        stream: "tasks:claude",
        name: "workers",
        consumers: 1,
        pending: 1,
        lag: 0,
      }],
    });

    const stuck = await detectStuck(stuckSnapshot);

    expect(call).not.toHaveBeenCalled();
    expect(stuck).toEqual([]);
    expect(stuckSnapshot.degraded_sections).toEqual([
      "active locks",
      "stuck tasks",
    ]);
  });

  it("skips idle worker-group entries when task metadata cannot be read", async () => {
    const call = vi.fn().mockResolvedValue([
      ["20-0", "worker-1", 181000, 2],
    ]);
    (redis as unknown as { call: ReturnType<typeof vi.fn> }).call = call;
    (redis as unknown as { xrange: ReturnType<typeof vi.fn> }).xrange = vi.fn()
      .mockRejectedValue(new Error("xrange failed"));

    const stuckSnapshot = snapshot({
      consumer_groups: [{
        stream: "tasks:claude",
        name: "workers",
        consumers: 1,
        pending: 1,
        lag: 0,
      }],
    });
    const stuck = await detectStuck(stuckSnapshot);

    expect(stuck).toEqual([]);
    expect(stuckSnapshot.degraded_sections).toContain("stuck tasks");
  });

  it("ignores pending entries from non-worker consumer groups", async () => {
    const call = vi.fn().mockResolvedValue([
      ["20-0", "debug-reader", 3600000, 99],
    ]);
    (redis as unknown as { call: ReturnType<typeof vi.fn> }).call = call;

    const stuck = await detectStuck(snapshot({
      consumer_groups: [{
        stream: "tasks:claude",
        name: "debug-inspector",
        consumers: 1,
        pending: 25,
        lag: 0,
      }],
    }));

    expect(call).not.toHaveBeenCalled();
    expect(stuck).toEqual([]);
  });
});

describe("pendingTtlState", () => {
  it("uses created_at when available", () => {
    const now = new Date("2026-06-16T00:03:01Z").getTime();

    expect(pendingTtlState(
      5700,
      16500,
      180,
      "2026-06-16T00:00:00Z",
      now,
    )).toBe("warning");
    expect(pendingTtlState(
      5700,
      16500,
      180,
      "2026-06-16T00:00:02Z",
      now,
    )).toBeNull();
    expect(pendingTtlState(
      1000,
      16500,
      180,
      "2026-06-16T00:00:02Z",
      now,
    )).toBeNull();
  });

  it("does not infer old age from legacy TTL when policy expected TTL is much larger", () => {
    expect(pendingTtlState(5700, 16500, 180)).toBeNull();
  });

  it("warns after the lock TTL has elapsed from the configured pending TTL", () => {
    expect(pendingTtlState(16500, 16500, 180)).toBeNull();
    expect(pendingTtlState(16321, 16500, 180)).toBeNull();
    expect(pendingTtlState(16320, 16500, 180)).toBe("warning");
  });

  it("escalates missing and nearly expired pending markers", () => {
    expect(pendingTtlState(-1, 16500, 180)).toBe("critical");
    expect(pendingTtlState(1650, 16500, 180)).toBe("critical");
  });
});

describe("pendingAgeSeconds", () => {
  it("normalizes invalid and future pending creation times", () => {
    const now = new Date("2026-06-16T00:03:00Z").getTime();

    expect(pendingAgeSeconds(null, now)).toBeNull();
    expect(pendingAgeSeconds("not-a-date", now)).toBeNull();
    expect(pendingAgeSeconds("2026-06-16T00:02:00Z", now)).toBe(60);
    expect(pendingAgeSeconds("2026-06-16T00:04:00Z", now)).toBe(0);
  });
});
