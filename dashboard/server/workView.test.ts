import { describe, expect, it, vi } from "vitest";
import type { DashboardMessage } from "./types.js";
import type { WorkAttempt } from "../src/lib/workTypes.js";
const { store } = vi.hoisted(() => ({ store: new Map<string, unknown>() }));
vi.mock("./redis.js", () => ({
  dashboardRedisKeyPatterns: (p: string[]) => p,
  scanKeysMany: async (patterns: string[]) =>
    [...store.keys()].filter((k) =>
      patterns.some((p) => k.includes(p.replace("*", ""))),
    ),
  redis: {
    pipeline: () => {
      const operations: Array<() => unknown> = [];
      const pipe = {
        hgetall: (k: string) => {
          operations.push(() => store.get(k) || {});
          return pipe;
        },
        zrevrange: (k: string) => {
          operations.push(() => store.get(k) || []);
          return pipe;
        },
        get: (k: string) => {
          operations.push(() => store.get(k));
          return pipe;
        },
        ttl: () => {
          operations.push(() => 120);
          return pipe;
        },
        exec: async () => operations.map((op) => [null, op()]),
      };
      return pipe;
    },
  },
}));
import { projectWork, fetchWorkView, workId } from "./workView.js";
const message: DashboardMessage = {
  snapshot: {
    redis_ok: true,
    fetched_at: "",
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
      pending_task_ttl_seconds: 100,
      lock_ttl_seconds: 180,
    },
  },
  workers: [],
  stuck_tasks: [],
};
const fields = (more: Record<string, string> = {}) => ({
  repo: "org/repo",
  kind: "issue",
  number: "12",
  prefix: "project-a",
  title: "Ship alerts",
  action: "skip_dependency",
  observed_at: "1000",
  ...more,
});
const run: WorkAttempt = {
  taskId: "attempt-1",
  workerId: "worker-a",
  workerPrefix: "project-a",
  provider: "codex",
  model: "test",
  accountId: "safe-id",
  startedAt: 900,
  finishedAt: null,
  status: "running",
  outputPrefix: "project-a",
};
describe("work lifecycle projection", () => {
  it("keeps dependency waiting before execution in Upcoming and CI waiting after execution In progress", () => {
    expect(projectWork(fields(), null, message, 1000)).toMatchObject({
      stage: "upcoming",
      activity: "waiting",
      stale: false,
    });
    expect(
      projectWork(
        fields({ started_at: "900", action: "skip_pending" }),
        { ...run, status: "completed" },
        message,
        1000,
      ),
    ).toMatchObject({
      stage: "in_progress",
      activity: "waiting",
      outcome: null,
    });
  });
  it("requires matching lock evidence to claim a running agent, and never treats successful exit as delivered", () => {
    const work = fields({ started_at: "900", action: "skip_locked" });
    expect(projectWork(work, run, message, 1000)).toMatchObject({
      activity: "waiting",
      reason: "Worker status unavailable",
    });
    const active = {
      ...message,
      snapshot: {
        ...message.snapshot,
        locks: [
          {
            lock_key: "a",
            prefix: "project-a",
            resource: "issue:org/repo:12",
            resource_type: "issue",
            repo: "org/repo",
            resource_id: "12",
            owner: "worker-a",
            ttl: 90,
            task_id: "attempt-1",
            pending_created_at: null,
          },
        ],
      },
    };
    expect(projectWork(work, run, active, 1000)).toMatchObject({
      activity: "executing",
      stage: "in_progress",
    });
    expect(
      projectWork(work, { ...run, status: "completed" }, message, 1000),
    ).toMatchObject({ stage: "in_progress", outcome: null });
    expect(
      projectWork(fields({ outcome: "merged" }), null, message, 1000),
    ).toMatchObject({ stage: "done" });
  });
  it("retains stale work and treats unavailable workflow ownership explicitly", () => {
    expect(projectWork(fields(), null, message, 2000)).toMatchObject({
      stale: true,
      stage: "upcoming",
    });
    expect(
      projectWork(
        fields({ action: "skip_v1_lookup_unavailable" }),
        null,
        message,
        1000,
      ),
    ).toMatchObject({ stage: "unknown" });
  });
  it("uses only explicit verified publication links to consolidate issue and merged PR", async () => {
    store.clear();
    const now = String(Date.now() / 1000);
    store.set("project-a:dashboard:project", {
      repo: "org/repo",
      observed_at: now,
      accounts: "[]",
    });
    store.set(
      "project-a:dashboard:work:issue:org/repo:12",
      fields({ related_pr: "15", observed_at: now }),
    );
    store.set(
      "project-a:dashboard:work:pr:org/repo:15",
      fields({ kind: "pr", number: "15", outcome: "merged", observed_at: now }),
    );
    const result = await fetchWorkView(message);
    expect(result.items).toHaveLength(1);
    expect(result.items[0]).toMatchObject({ number: 12, stage: "done" });
    expect(result.counts.done).toBe(1);
  });
  it("binds scope to physical keys and allowlists fields so credential-shaped payloads never reach the browser", async () => {
    store.clear();
    const now = String(Date.now() / 1000);
    store.set("project-a:dashboard:project", {
      repo: "org/repo",
      observed_at: now,
      accounts: JSON.stringify([
        {
          id: "safe-id",
          provider: "codex",
          credential: "SECRET",
          availability: "available",
        },
      ]),
    });
    store.set(
      "project-a:dashboard:work:issue:org/repo:12",
      fields({
        prefix: "other-project",
        repo: "other/repo",
        credential: "SECRET",
        observed_at: now,
      }),
    );
    store.set("project-a:dashboard:work:issue:org/repo:12:attempts", [
      "attempt-1",
    ]);
    store.set("project-a:dashboard:attempt:attempt-1", {
      task_id: "attempt-1",
      worker_id: "worker-a",
      credential: "SECRET",
      provider: "codex",
      account_id: "safe-id",
    });
    const result = await fetchWorkView(message);
    expect(result.items[0]).toMatchObject({
      id: workId("project-a", "org/repo", "issue", 12),
      prefix: "project-a",
      project: "org/repo",
      latestAttempt: { taskId: "attempt-1" },
    });
    expect(JSON.stringify(result)).not.toContain("SECRET");
    expect(JSON.stringify(result)).not.toContain("other-project");
  });
});
