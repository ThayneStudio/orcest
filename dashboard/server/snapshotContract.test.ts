import { beforeEach, describe, expect, it, vi } from "vitest";

type StreamEntry = {
  id: string;
  fields: Record<string, string>;
};

type ConsumerGroupState = {
  name: string;
  lastDeliveredId: string;
  pending: number;
  consumers: Set<string>;
};

type PipelineCall = {
  command: "get" | "hgetall" | "smembers" | "ttl" | "xlen";
  key: string;
};

const redisMock = vi.hoisted(() => {
  const state = { fake: null as unknown };
  const fake = () => state.fake as {
    get(key: string): Promise<string | null>;
    hgetall(key: string): Promise<Record<string, string>>;
    pipeline(): unknown;
	    scanKeys(pattern: string): Promise<string[]>;
	    scanKeysMany(patterns: string[]): Promise<string[]>;
	    smembers(key: string): Promise<string[]>;
	    ttl(key: string): Promise<number>;
	    type(key: string): Promise<string>;
	    xinfo(command: string, stream: string): Promise<unknown[]>;
    xlen(key: string): Promise<number>;
    xrange(stream: string, start: string, end: string, countKeyword?: string, count?: number): Promise<Array<[string, string[]]>>;
    xrevrange(stream: string, start: string, end: string, countKeyword?: string, count?: number): Promise<Array<[string, string[]]>>;
  };
  return {
    state,
    healthCheck: vi.fn(),
    dashboardRedisKeyPatterns: vi.fn((suffixes: string[]) =>
      suffixes.flatMap((suffix) => [`*:${suffix}`, suffix])
    ),
    scanKeys: vi.fn((pattern: string) => fake().scanKeys(pattern)),
    scanKeysMany: vi.fn((patterns: string[]) => fake().scanKeysMany(patterns)),
    redis: {
      get: (key: string) => fake().get(key),
      hgetall: (key: string) => fake().hgetall(key),
      pipeline: () => fake().pipeline(),
	      smembers: (key: string) => fake().smembers(key),
	      ttl: (key: string) => fake().ttl(key),
	      type: (key: string) => fake().type(key),
	      xinfo: (command: string, stream: string) => fake().xinfo(command, stream),
      xlen: (key: string) => fake().xlen(key),
      xrange: (
        stream: string,
        start: string,
        end: string,
        countKeyword?: string,
        count?: number,
      ) => fake().xrange(stream, start, end, countKeyword, count),
      xrevrange: (
        stream: string,
        start: string,
        end: string,
        countKeyword?: string,
        count?: number,
      ) => fake().xrevrange(stream, start, end, countKeyword, count),
    },
  };
});

vi.mock("./redis.js", () => ({
  redis: redisMock.redis,
  healthCheck: redisMock.healthCheck,
  dashboardRedisKeyPatterns: redisMock.dashboardRedisKeyPatterns,
  scanKeys: redisMock.scanKeys,
  scanKeysMany: redisMock.scanKeysMany,
}));

import { fetchSnapshot } from "./snapshot.js";

class RedisStreamContractFake {
  private readonly streams = new Map<string, StreamEntry[]>();
  private readonly groups = new Map<string, Map<string, ConsumerGroupState>>();
  private readonly strings = new Map<string, string>();
  private readonly ttls = new Map<string, number>();
  private readonly hashes = new Map<string, Record<string, string>>();
  private readonly sets = new Map<string, Set<string>>();

  xadd(stream: string, id: string, fields: Record<string, string>): string {
    const entries = this.streams.get(stream) || [];
    entries.push({ id, fields: { ...fields } });
    entries.sort((left, right) => compareStreamIds(left.id, right.id));
    this.streams.set(stream, entries);
    return id;
  }

  xgroupCreate(stream: string, group: string, lastDeliveredId = "0-0"): void {
    const groups = this.groups.get(stream) || new Map<string, ConsumerGroupState>();
    groups.set(group, {
      name: group,
      lastDeliveredId,
      pending: 0,
      consumers: new Set<string>(),
    });
    this.groups.set(stream, groups);
  }

  xreadgroup(group: string, consumer: string, stream: string, count: number): Array<[string, string[]]> {
    const groupState = this.groups.get(stream)?.get(group);
    if (!groupState) throw new Error(`missing group ${group} on ${stream}`);
    const entries = this.entriesAfter(stream, groupState.lastDeliveredId).slice(0, count);
    if (entries.length === 0) return [];

    groupState.consumers.add(consumer);
    groupState.pending += entries.length;
    groupState.lastDeliveredId = entries[entries.length - 1].id;
    return entries.map((entry) => [entry.id, fieldsToArray(entry.fields)]);
  }

  xack(stream: string, group: string, ...entryIds: string[]): number {
    const groupState = this.groups.get(stream)?.get(group);
    if (!groupState) return 0;
    const acked = Math.min(groupState.pending, entryIds.length);
    groupState.pending -= acked;
    return acked;
  }

  set(key: string, value: string, ttl?: number): void {
    this.strings.set(key, value);
    if (ttl !== undefined) this.ttls.set(key, ttl);
  }

  hset(key: string, fields: Record<string, string>): void {
    this.hashes.set(key, { ...(this.hashes.get(key) || {}), ...fields });
  }

  sadd(key: string, ...values: string[]): void {
    const set = this.sets.get(key) || new Set<string>();
    for (const value of values) set.add(value);
    this.sets.set(key, set);
  }

  async get(key: string): Promise<string | null> {
    return this.strings.get(key) ?? null;
  }

  async hgetall(key: string): Promise<Record<string, string>> {
    return { ...(this.hashes.get(key) || {}) };
  }

  async smembers(key: string): Promise<string[]> {
    return [...(this.sets.get(key) || new Set<string>())];
  }

	  async ttl(key: string): Promise<number> {
	    if (!this.allKeys().has(key)) return -2;
	    return this.ttls.get(key) ?? -1;
	  }

	  async type(key: string): Promise<string> {
	    if (this.streams.has(key)) return "stream";
	    if (this.strings.has(key)) return "string";
	    if (this.hashes.has(key)) return "hash";
	    if (this.sets.has(key)) return "set";
	    return "none";
	  }

	  async xlen(key: string): Promise<number> {
    return this.streams.get(key)?.length ?? 0;
  }

  async xinfo(command: string, stream: string): Promise<unknown[]> {
    if (command !== "GROUPS") throw new Error(`unsupported XINFO ${command}`);
    const groups = this.groups.get(stream);
    if (!groups) return [];
    return [...groups.values()].map((group) => ({
      name: group.name,
      consumers: group.consumers.size,
      pending: group.pending,
      "last-delivered-id": group.lastDeliveredId,
      lag: this.entriesAfter(stream, group.lastDeliveredId).length,
    }));
  }

  async xrange(
    stream: string,
    start: string,
    end: string,
    countKeyword?: string,
    count?: number,
  ): Promise<Array<[string, string[]]>> {
    const limit = countKeyword === "COUNT" ? count : undefined;
    const entries = (this.streams.get(stream) || [])
      .filter((entry) => streamIdInRange(entry.id, start, end));
    return entries.slice(0, limit).map((entry) => [entry.id, fieldsToArray(entry.fields)]);
  }

  async xrevrange(
    stream: string,
    start: string,
    end: string,
    countKeyword?: string,
    count?: number,
  ): Promise<Array<[string, string[]]>> {
    const limit = countKeyword === "COUNT" ? count : undefined;
    const entries = (this.streams.get(stream) || [])
      .filter((entry) => streamIdInRange(entry.id, end, start))
      .reverse();
    return entries.slice(0, limit).map((entry) => [entry.id, fieldsToArray(entry.fields)]);
  }

  pipeline(): {
    get: (key: string) => unknown;
    hgetall: (key: string) => unknown;
    smembers: (key: string) => unknown;
    ttl: (key: string) => unknown;
    xlen: (key: string) => unknown;
    exec: () => Promise<Array<[null, unknown]>>;
  } {
    const calls: PipelineCall[] = [];
    const pipeline = {
      get: (key: string) => {
        calls.push({ command: "get", key });
        return pipeline;
      },
      hgetall: (key: string) => {
        calls.push({ command: "hgetall", key });
        return pipeline;
      },
      smembers: (key: string) => {
        calls.push({ command: "smembers", key });
        return pipeline;
      },
      ttl: (key: string) => {
        calls.push({ command: "ttl", key });
        return pipeline;
      },
      xlen: (key: string) => {
        calls.push({ command: "xlen", key });
        return pipeline;
      },
      exec: async () => {
        const values = await Promise.all(calls.map((call) => this.pipelineValue(call)));
        return values.map((value): [null, unknown] => [null, value]);
      },
    };
    return pipeline;
  }

  async scanKeys(pattern: string): Promise<string[]> {
    const regex = redisGlobToRegExp(pattern);
    return [...this.allKeys()].filter((key) => regex.test(key)).sort();
  }

  async scanKeysMany(patterns: string[]): Promise<string[]> {
    const keys = new Set<string>();
    for (const pattern of patterns) {
      for (const key of await this.scanKeys(pattern)) keys.add(key);
    }
    return [...keys].sort();
  }

  private async pipelineValue(call: PipelineCall): Promise<unknown> {
    switch (call.command) {
      case "get": return this.get(call.key);
      case "hgetall": return this.hgetall(call.key);
      case "smembers": return this.smembers(call.key);
      case "ttl": return this.ttl(call.key);
      case "xlen": return this.xlen(call.key);
    }
  }

  private entriesAfter(stream: string, entryId: string): StreamEntry[] {
    return (this.streams.get(stream) || [])
      .filter((entry) => compareStreamIds(entry.id, entryId) > 0);
  }

  private allKeys(): Set<string> {
    return new Set([
      ...this.streams.keys(),
      ...this.strings.keys(),
      ...this.hashes.keys(),
      ...this.sets.keys(),
    ]);
  }
}

beforeEach(() => {
  redisMock.state.fake = new RedisStreamContractFake();
  redisMock.healthCheck.mockResolvedValue(true);
  redisMock.dashboardRedisKeyPatterns.mockImplementation((suffixes: string[]) =>
    suffixes.flatMap((suffix) => [`*:${suffix}`, suffix])
  );
  redisMock.scanKeys.mockClear();
  redisMock.scanKeysMany.mockClear();
});

describe("fetchSnapshot Redis stream contract", () => {
  it("derives operator-facing snapshot state from Redis-like stream operations", async () => {
    const fake = redisMock.state.fake as RedisStreamContractFake;
    fake.xadd("orcest:tasks:issue:codex", "1000-0", {
      id: "task-delivered",
      type: "fix_issue",
      key_prefix: "orcest",
      repo: "owner/repo",
      resource_type: "issue",
      resource_id: "4250",
      created_at: "2026-06-29T00:00:00.000Z",
    });
    fake.xadd("orcest:tasks:issue:codex", "1001-0", {
      id: "task-queued-a",
      type: "fix_issue",
      key_prefix: "orcest",
      repo: "owner/repo",
      resource_type: "issue",
      resource_id: "4251",
      created_at: "2026-06-29T00:01:00.000Z",
    });
    fake.xadd("orcest:tasks:issue:codex", "1002-0", {
      id: "task-queued-b",
      type: "fix_issue",
      key_prefix: "orcest",
      repo: "owner/repo",
      resource_type: "issue",
      resource_id: "4252",
      created_at: "2026-06-29T00:02:00.000Z",
    });
    fake.xgroupCreate("orcest:tasks:issue:codex", "workers", "0-0");
    fake.xreadgroup("workers", "orcest-worker-1", "orcest:tasks:issue:codex", 1);

    fake.xadd("orcest:tasks:issue:grok", "1100-0", {
      id: "task-no-consumer",
      type: "fix_issue",
      key_prefix: "orcest",
      repo: "owner/repo",
      resource_type: "issue",
      resource_id: "4253",
      created_at: "2026-06-29T00:03:00.000Z",
    });

    fake.xadd("orcest:results", "2000-0", {
      task_id: "task-complete",
      worker_id: "orcest-worker-1",
      status: "completed",
      repo: "owner/repo",
      resource_type: "issue",
      resource_id: "4250",
      duration_seconds: "12",
      summary: "completed issue",
    });
    fake.xadd("project-a:results", "2100-0", {
      task_id: "task-failed",
      worker_id: "project-a-worker-1",
      status: "failed",
      repo: "owner/repo",
      resource_type: "pr",
      resource_id: "42",
      duration_seconds: "7",
      summary: "failed pr",
    });
    fake.xadd("orcest:output:orcest-worker-1", "1990-0", {
      type: "task_start",
      task_id: "task-complete",
      worker_id: "orcest-worker-1",
    });
    fake.xadd("orcest:output:project-a-worker-1", "2090-0", {
      type: "task_start",
      task_id: "task-failed",
      worker_id: "project-a-worker-1",
    });
    fake.xadd("orcest:dead-letter", "2200-0", {
      id: "task-dead",
      type: "fix_issue",
      repo: "owner/repo",
      resource_type: "issue",
      resource_id: "4254",
      dead_letter_reason: "max attempts",
    });

    fake.set("orcest:lock:issue:owner/repo:4255", "orcest-worker-live", 120);
    fake.set("orcest:pending:issue:owner/repo:4255", JSON.stringify({
      task_id: "task-live",
      created_at: "2026-06-29T00:04:00.000Z",
    }));
    fake.xadd("orcest:output:orcest-worker-live", "2190-0", {
      type: "task_start",
      task_id: "task-live",
      worker_id: "orcest-worker-live",
    });
    fake.hset("orcest:issue:owner/repo:4251:attempts", { count: "2" });
    fake.set("orcest:providers:claude:exhausted_skip", "3");
    fake.set("orcest:pool:current_template_vmid", "9003");
    fake.sadd("orcest:pool:idle", "10001", "10000");
    fake.hset("orcest:pool:active", { "10002": "1710000000" });

    const snapshot = await fetchSnapshot(5);

    expect(snapshot.redis_ok).toBe(true);
    expect(snapshot.degraded_sections).toEqual([]);
    expect(snapshot.queue_depths).toEqual({
      "orcest:tasks:issue:codex": 3,
      "orcest:tasks:issue:grok": 1,
    });
    expect(snapshot.consumer_groups).toEqual(expect.arrayContaining([
      {
        stream: "orcest:tasks:issue:codex",
        name: "workers",
        consumers: 1,
        pending: 1,
        lag: 2,
      },
      {
        stream: "orcest:tasks:issue:grok",
        name: "workers",
        consumers: 0,
        pending: 0,
        lag: 1,
      },
    ]));
    expect(snapshot.queued_tasks.map((task) => ({
      entry_id: task.entry_id,
      task_id: task.task_id,
      stream: task.stream,
      prefix: task.prefix,
      resource_id: task.resource_id,
    }))).toEqual([
      {
        entry_id: "1001-0",
        task_id: "task-queued-a",
        stream: "orcest:tasks:issue:codex",
        prefix: "orcest",
        resource_id: "4251",
      },
      {
        entry_id: "1002-0",
        task_id: "task-queued-b",
        stream: "orcest:tasks:issue:codex",
        prefix: "orcest",
        resource_id: "4252",
      },
      {
        entry_id: "1100-0",
        task_id: "task-no-consumer",
        stream: "orcest:tasks:issue:grok",
        prefix: "orcest",
        resource_id: "4253",
      },
    ]);
    expect(snapshot.results_depth).toBe(2);
    expect(snapshot.recent_results.map((result) => ({
      result_id: result.result_id,
      task_id: result.task_id,
      status: result.status,
      resource_id: result.resource_id,
      output_prefix: result.output_prefix,
    }))).toEqual([
      {
        result_id: "project-a:results:2100-0",
        task_id: "task-failed",
        status: "failed",
        resource_id: "42",
        output_prefix: "orcest",
      },
      {
        result_id: "orcest:results:2000-0",
        task_id: "task-complete",
        status: "completed",
        resource_id: "4250",
        output_prefix: "orcest",
      },
    ]);
    expect(snapshot.dead_letter_count).toBe(1);
    expect(snapshot.dead_letter_entries).toEqual([expect.objectContaining({
      dead_letter_id: "orcest:dead-letter:2200-0",
      task_id: "task-dead",
      reason: "max attempts",
      timestamp_ms: 2200,
    })]);
    expect(snapshot.locks).toEqual([expect.objectContaining({
      lock_key: "orcest:lock:issue:owner/repo:4255",
      prefix: "orcest",
      owner: "orcest-worker-live",
      ttl: 120,
      task_id: "task-live",
      pending_created_at: "2026-06-29T00:04:00.000Z",
      output_prefix: "orcest",
    })]);
    expect(snapshot.attempt_counts).toEqual({
      "[orcest] owner/repo Issue #4251": 2,
    });
    expect(snapshot.provider_health).toEqual({
      "[orcest] claude": { exhausted_skip: 3 },
    });
    expect(snapshot.worker_pool).toEqual([expect.objectContaining({
      prefix: "orcest",
      template_vmid: "9003",
      idle: ["10000", "10001"],
      idle_count: 2,
      active_count: 1,
    })]);
    expect(snapshot.worker_pool[0].active[0]).toEqual(expect.objectContaining({
      vmid: "10002",
      started_at: "2024-03-09T16:00:00.000Z",
    }));
  });
});

function fieldsToArray(fields: Record<string, string>): string[] {
  return Object.entries(fields).flatMap(([key, value]) => [key, value]);
}

function compareStreamIds(left: string, right: string): number {
  const [leftMs, leftSeq] = streamIdParts(left);
  const [rightMs, rightSeq] = streamIdParts(right);
  return leftMs - rightMs || leftSeq - rightSeq;
}

function streamIdParts(id: string): [number, number] {
  const match = id.match(/^(\d+)-(\d+)$/);
  if (!match) return [0, 0];
  return [Number(match[1]), Number(match[2])];
}

function streamIdInRange(id: string, start: string, end: string): boolean {
  const afterStart = start === "-"
    ? true
    : start.startsWith("(")
      ? compareStreamIds(id, start.slice(1)) > 0
      : compareStreamIds(id, start) >= 0;
  const beforeEnd = end === "+"
    ? true
    : end.startsWith("(")
      ? compareStreamIds(id, end.slice(1)) < 0
      : compareStreamIds(id, end) <= 0;
  return afterStart && beforeEnd;
}

function redisGlobToRegExp(pattern: string): RegExp {
  let source = "^";
  for (const char of pattern) {
    if (char === "*") source += ".*";
    else if (char === "?") source += ".";
    else source += char.replace(/[\\^$+?.()|{}[\]]/g, "\\$&");
  }
  source += "$";
  return new RegExp(source);
}
