import { describe, expect, it } from "vitest";
import { normalizeDashboardMessage } from "./snapshot";

describe("normalizeDashboardMessage", () => {
  it("fills defaults for older snapshot payloads", () => {
    const message = normalizeDashboardMessage({
      snapshot: {
        redis_ok: true,
        fetched_at: "2026-06-16T00:00:00Z",
        queue_depths: {
          "tasks:claude": "2",
          "tasks:bad": -1,
          "results": 7,
          "": 9,
        },
        attempt_counts: {
          " owner/repo PR #1 ": "2",
          "owner/repo PR #2": "bad",
          "owner/repo PR #3": 0,
          "owner/repo PR #4": "2.9",
        },
        results_depth: "bad",
        dead_letter_count: 3.9,
      },
    });

    expect(message?.snapshot.queue_depths).toEqual({
      "tasks:bad": 0,
      "tasks:claude": 2,
    });
    expect(message?.snapshot.results_depth).toBe(0);
    expect(message?.snapshot.dead_letter_count).toBe(3);
    expect(message?.snapshot.attempt_counts).toEqual({
      "owner/repo PR #1": 2,
    });
    expect(message?.snapshot.worker_pool).toEqual([]);
    expect(message?.snapshot.degraded_sections).toEqual([]);
    expect(message?.snapshot.provider_health).toEqual({});
    expect(message?.snapshot.dashboard_policy).toEqual({
      max_attempts: 3,
      pending_task_ttl_seconds: 16500,
      lock_ttl_seconds: 180,
    });
    expect(message?.stuck_tasks).toEqual([]);
    expect(message?.workers).toEqual([]);
  });

  it("keeps provided policy values while filling missing ones", () => {
    const message = normalizeDashboardMessage({
      snapshot: {
        redis_ok: true,
        dashboard_policy: {
          max_attempts: "5",
          pending_task_ttl_seconds: "7200",
          lock_ttl_seconds: "bad",
        },
      },
      stuck_tasks: [],
      workers: ["worker-1"],
    });

    expect(message?.snapshot.dashboard_policy).toEqual({
      max_attempts: 5,
      pending_task_ttl_seconds: 7200,
      lock_ttl_seconds: 180,
    });
    expect(message?.workers).toEqual(["worker-1"]);
  });

  it("keeps aggregate retained depths at least as large as loaded rows", () => {
    const message = normalizeDashboardMessage({
      snapshot: {
        redis_ok: true,
        results_depth: "bad",
        dead_letter_count: "bad",
        recent_results: [
          {
            result_stream: "results",
            entry_id: "1-0",
            task_id: "task-result-1",
          },
          {
            result_stream: "results",
            entry_id: "2-0",
            task_id: "task-result-2",
          },
        ],
        dead_letter_entries: [
          {
            dead_letter_stream: "dead-letter",
            entry_id: "3-0",
            task_id: "task-dead-letter-1",
          },
        ],
      },
    });

    expect(message?.snapshot.results_depth).toBe(2);
    expect(message?.snapshot.dead_letter_count).toBe(1);
    expect(message?.snapshot.recent_results).toHaveLength(2);
    expect(message?.snapshot.dead_letter_entries).toHaveLength(1);
  });

  it("requires a boolean true Redis status", () => {
    expect(normalizeDashboardMessage({
      snapshot: {
        redis_ok: "true",
        fetched_at: 123,
      },
    })?.snapshot).toMatchObject({
      redis_ok: false,
      fetched_at: "",
    });

    expect(normalizeDashboardMessage({
      snapshot: {
        redis_ok: true,
        fetched_at: "2026-06-16T00:00:00Z",
      },
    })?.snapshot).toMatchObject({
      redis_ok: true,
      fetched_at: "2026-06-16T00:00:00Z",
    });
  });

  it("normalizes partial row arrays before components render them", () => {
    const message = normalizeDashboardMessage({
      snapshot: {
        redis_ok: true,
        locks: [
          "bad",
          {
            lock_key: " lock:1 ",
            ttl: "bad",
            owner: "   ",
            resource_id: 42,
            task_id: " task-lock ",
            pending_created_at: "   ",
          },
        ],
        consumer_groups: [
          {
            stream: " tasks:claude ",
            name: "   ",
            consumers: "bad",
            pending: -5,
            lag: "   ",
          },
        ],
        recent_results: [
          "bad",
          {
            result_stream: " results ",
            entry_id: " 1-0 ",
            duration_seconds: "12",
            repo: "   ",
            resource_id: 43,
            summary: " summary with padding ",
          },
          {
            result_stream: "results",
            entry_id: "2-0",
            duration_seconds: "not-a-number",
          },
        ],
        dead_letter_entries: [
          {
            dead_letter_stream: "dead-letter",
            entry_id: "2-0",
            task_id: 123,
            task_type: "   ",
            repo: "   ",
            resource_type: " issue ",
            resource_id: 44,
            timestamp_ms: "   ",
            reason: "   ",
          },
        ],
        queued_tasks: [
          {
            entry_id: " 7-0 ",
            task_id: 123,
            prefix: " project-a ",
            repo: " owner/repo ",
            task_type: "   ",
            resource_id: 45,
            stream: "   ",
          },
        ],
        provider_health: {
          claude: {
            exhausted_skip: "2",
            negative: -1,
            decimal: 1.5,
            bad: "2bad",
            "": 3,
          },
          " claude ": {
            " exhausted_skip ": "4",
            rebake_required_failures: "1",
          },
          "": {
            exhausted_skip: 1,
          },
          grok: null,
        },
        worker_pool: [
          {
            prefix: "orcest",
            idle: [300, "301", " 301 ", "", "   ", null, true, { vmid: 303 }],
            idle_count: 99,
            active: [
              { vmid: "302", age_seconds: "bad" },
              { vmid: " 302 ", age_seconds: 10 },
              { vmid: 304, age_seconds: 12 },
              { vmid: 307, age_seconds: null },
              { vmid: 308, age_seconds: "   " },
              { vmid: true, age_seconds: 13 },
              { vmid: { id: 305 }, age_seconds: 14 },
              { vmid: "", age_seconds: 11 },
            ],
            active_count: 99,
          },
          {
            prefix: " orcest ",
            template_vmid: 9001,
            idle: ["301", "305"],
            active: [
              { vmid: "304", started_at: "2026-06-20T00:00:00Z", age_seconds: 4 },
              { vmid: "306", age_seconds: "5" },
            ],
          },
        ],
        degraded_sections: [" recent results ", "recent results", "", 123],
      },
      stuck_tasks: [
        {
          resource_type: "pr",
          resource_id: 42,
          severity: "unexpected",
          stream: " tasks:claude ",
          consumer_group: " workers ",
          entry_id: " 7-0 ",
          task_id: " task-7 ",
          no_worker_consumers: true,
        },
      ],
      workers: [123, " worker-1 ", "worker-1", "", "   ", null, true, { id: "worker-2" }],
    });

    expect(message?.snapshot.locks).toEqual([{
      lock_key: "lock:1",
      prefix: null,
      resource: "",
      resource_type: "",
      repo: "",
      resource_id: "42",
      owner: "(expired)",
      ttl: Number.NaN,
      task_id: "task-lock",
      pending_created_at: null,
    }]);
    expect(message?.snapshot.consumer_groups).toEqual([{
      stream: "tasks:claude",
      name: "?",
      consumers: 0,
      pending: 0,
      lag: null,
    }]);
    expect(message?.snapshot.recent_results[0]).toMatchObject({
      result_id: "results:1-0",
      result_stream: "results",
      entry_id: "1-0",
      duration_seconds: 12,
      repo: null,
      resource_id: "43",
      summary: "summary with padding",
    });
    expect(message?.snapshot.recent_results[1]).toMatchObject({
      result_id: "results:2-0",
      duration_seconds: -1,
    });
    expect(message?.snapshot.dead_letter_entries[0]).toMatchObject({
      dead_letter_id: "dead-letter:2-0",
      timestamp_ms: null,
      task_id: "",
      task_type: "?",
      repo: "?",
      resource_type: "issue",
      resource_id: "44",
      reason: null,
    });
    expect(message?.snapshot.queued_tasks[0]).toMatchObject({
      entry_id: "7-0",
      task_id: "",
      prefix: "project-a",
      repo: "owner/repo",
      task_type: "?",
      resource_id: "45",
      stream: "",
    });
    expect(message?.snapshot.provider_health).toEqual({
      claude: {
        exhausted_skip: 4,
        rebake_required_failures: 1,
      },
    });
    expect(message?.snapshot.worker_pool[0]).toMatchObject({
      prefix: "orcest",
      template_vmid: "9001",
      idle: ["300", "301", "305"],
      active: [
        { vmid: "302", started_at: null, age_seconds: null },
        { vmid: "304", started_at: "2026-06-20T00:00:00Z", age_seconds: 4 },
        { vmid: "307", started_at: null, age_seconds: null },
        { vmid: "308", started_at: null, age_seconds: null },
        { vmid: "306", started_at: null, age_seconds: 5 },
      ],
      idle_count: 3,
      active_count: 5,
    });
    expect(message?.snapshot.worker_pool).toHaveLength(1);
    expect(message?.snapshot.degraded_sections).toEqual(["recent results"]);
    expect(message?.stuck_tasks).toEqual([{
      prefix: null,
      resource_type: "pr",
      repo: null,
      resource_id: "42",
      reason: "",
      severity: "warning",
      stream: "tasks:claude",
      consumer_group: "workers",
      entry_id: "7-0",
      task_id: "task-7",
      no_worker_consumers: true,
    }]);
    expect(message?.workers).toEqual(["123", "worker-1"]);
  });

  it("distinguishes invalid lock TTLs from Redis no-expiry locks", () => {
    const message = normalizeDashboardMessage({
      snapshot: {
        redis_ok: true,
        locks: [
          { lock_key: "no-expiry", ttl: -1 },
          { lock_key: "missing" },
          { lock_key: "invalid", ttl: "bad" },
        ],
      },
    });

    expect(message?.snapshot.locks[0].ttl).toBe(-1);
    expect(message?.snapshot.locks[1].ttl).toBeNaN();
    expect(message?.snapshot.locks[2].ttl).toBeNaN();
  });

  it("does not coerce non-string non-number values into nullable numeric fields", () => {
    const message = normalizeDashboardMessage({
      snapshot: {
        redis_ok: true,
        dashboard_policy: {
          max_attempts: true,
        },
        consumer_groups: [
          {
            stream: "tasks:claude",
            name: "workers",
            lag: true,
          },
        ],
        dead_letter_entries: [
          {
            dead_letter_stream: "dead-letter",
            entry_id: "1-0",
            timestamp_ms: false,
          },
          {
            dead_letter_stream: "dead-letter",
            entry_id: "2-0",
            timestamp_ms: "-1",
          },
          {
            dead_letter_stream: "dead-letter",
            entry_id: "3-0",
            timestamp_ms: "123.4",
          },
        ],
        provider_health: {
          claude: {
            exhausted_skip: true,
          },
        },
        worker_pool: [
          {
            prefix: "orcest",
            active: [
              { vmid: "300", age_seconds: [] },
              { vmid: "301", age_seconds: "6" },
            ],
          },
        ],
      },
    });

    expect(message?.snapshot.dashboard_policy.max_attempts).toBe(3);
    expect(message?.snapshot.consumer_groups[0].lag).toBeNull();
    expect(message?.snapshot.dead_letter_entries.map((entry) => entry.timestamp_ms))
      .toEqual([null, null, null]);
    expect(message?.snapshot.provider_health).toEqual({});
    expect(message?.snapshot.worker_pool[0].active).toEqual([
      { vmid: "300", started_at: null, age_seconds: null },
      { vmid: "301", started_at: null, age_seconds: 6 },
    ]);
  });

  it("preserves task output prefix annotations for locks and recent results", () => {
    const message = normalizeDashboardMessage({
      snapshot: {
        redis_ok: true,
        locks: [
          { lock_key: "lock:string", output_prefix: " orcest " },
          { lock_key: "lock:null", output_prefix: null },
          { lock_key: "lock:missing" },
          { lock_key: "lock:invalid", output_prefix: 123 },
          { lock_key: "lock:unresolved", output_prefix_unresolved: true },
          { lock_key: "lock:false", output_prefix_unresolved: false },
        ],
        recent_results: [
          { result_id: "result:string", output_prefix: " orcest " },
          { result_id: "result:null", output_prefix: null },
          { result_id: "result:missing" },
          { result_id: "result:invalid", output_prefix: 123 },
          { result_id: "result:unresolved", output_prefix_unresolved: true },
          { result_id: "result:false", output_prefix_unresolved: false },
        ],
      },
    });

    expect(message?.snapshot.locks.map((lock) =>
      Object.prototype.hasOwnProperty.call(lock, "output_prefix")
        ? lock.output_prefix
        : undefined
    )).toEqual(["orcest", null, undefined, undefined, undefined, undefined]);
    expect(message?.snapshot.recent_results.map((result) =>
      Object.prototype.hasOwnProperty.call(result, "output_prefix")
        ? result.output_prefix
        : undefined
    )).toEqual(["orcest", null, undefined, undefined, undefined, undefined]);
    expect(message?.snapshot.locks.map((lock) => lock.output_prefix_unresolved))
      .toEqual([undefined, undefined, undefined, undefined, true, undefined]);
    expect(message?.snapshot.recent_results.map((result) => result.output_prefix_unresolved))
      .toEqual([undefined, undefined, undefined, undefined, true, undefined]);
  });

  it("preserves worker pool counts when VM detail arrays are absent", () => {
    const message = normalizeDashboardMessage({
      snapshot: {
        redis_ok: true,
        worker_pool: [
          {
            prefix: "orcest",
            template_vmid: 9001,
            idle_count: "3",
            active_count: 2,
          },
        ],
      },
    });

    expect(message?.snapshot.worker_pool).toEqual([{
      prefix: "orcest",
      template_vmid: "9001",
      idle: [],
      active: [],
      idle_count: 3,
      active_count: 2,
    }]);
  });

  it("rejects malformed messages", () => {
    expect(normalizeDashboardMessage(null)).toBeNull();
    expect(normalizeDashboardMessage({})).toBeNull();
  });
});
