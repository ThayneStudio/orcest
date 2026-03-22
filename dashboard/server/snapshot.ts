import { redis, healthCheck, scanKeys } from "./redis.js";
import type {
  SystemSnapshot,
  LockInfo,
  ConsumerGroupInfo,
  RecentResult,
  DeadLetterEntry,
  QueuedTask,
} from "./types.js";

export async function fetchSnapshot(maxResults = 20): Promise<SystemSnapshot> {
  const ok = await healthCheck();
  if (!ok) {
    return {
      redis_ok: false,
      fetched_at: new Date().toISOString(),
      queue_depths: {},
      results_depth: 0,
      dead_letter_count: 0,
      locks: [],
      consumer_groups: [],
      recent_results: [],
      attempt_counts: {},
      dead_letter_entries: [],
      queued_tasks: [],
    };
  }

  try {
    return await fetchSnapshotInner(maxResults);
  } catch {
    return {
      redis_ok: false,
      fetched_at: new Date().toISOString(),
      queue_depths: {},
      results_depth: 0,
      dead_letter_count: 0,
      locks: [],
      consumer_groups: [],
      recent_results: [],
      attempt_counts: {},
      dead_letter_entries: [],
      queued_tasks: [],
    };
  }
}

async function fetchSnapshotInner(maxResults: number): Promise<SystemSnapshot> {
  // Queue depths — keys are prefixed: orcest:tasks:claude, transit-platform:tasks:issue:claude, etc.
  const taskStreamKeys = (await scanKeys("*:tasks:*")).sort();
  const queueDepths: Record<string, number> = {};
  for (const stream of taskStreamKeys) {
    try {
      queueDepths[stream] = await redis.xlen(stream);
    } catch {
      // Key exists but is not a stream
    }
  }

  // Results — each project prefix has its own results stream: orcest:results, transit-platform:results
  let resultsDepth = 0;
  const resultsKeys = await scanKeys("*:results");
  for (const key of resultsKeys) {
    try {
      resultsDepth += await redis.xlen(key);
    } catch {
      // ignore
    }
  }

  // Dead-letter streams: *:dead-letter
  let deadLetterCount = 0;
  const dlKeys = await scanKeys("*:dead-letter");
  for (const key of dlKeys) {
    try {
      deadLetterCount += await redis.xlen(key);
    } catch {
      // ignore
    }
  }

  // Dead-letter entries (most recent 5, across all dead-letter streams)
  const deadLetterEntries: DeadLetterEntry[] = [];
  for (const dlKey of dlKeys) {
    try {
      const dlRaw = await redis.xrevrange(dlKey, "+", "-", "COUNT", 5);
      for (const [entryId, fields] of dlRaw) {
        const fieldMap = arrayToMap(fields);
        let timestampMs: number | null = null;
        try {
          timestampMs = parseInt(entryId.split("-")[0], 10);
        } catch {
          // ignore
        }
        deadLetterEntries.push({
          entry_id: entryId,
          task_type: fieldMap.type || "?",
          repo: fieldMap.repo || "?",
          resource_type: fieldMap.resource_type || "?",
          resource_id: fieldMap.resource_id || "?",
          timestamp_ms: timestampMs,
          reason: fieldMap.dead_letter_reason || null,
        });
      }
    } catch {
      // ignore
    }
  }
  // Sort by timestamp descending, keep top 5
  deadLetterEntries.sort((a, b) => (b.timestamp_ms || 0) - (a.timestamp_ms || 0));
  deadLetterEntries.splice(5);

  // Active locks — keys are prefixed: orcest:lock:pr:*, orcest:lock:issue:*
  const locks: LockInfo[] = [];
  for (const pattern of ["*:lock:pr:*", "*:lock:issue:*"]) {
    const lockKeys = await scanKeys(pattern);
    for (const key of lockKeys) {
      const owner = (await redis.get(key)) || "(expired)";
      const ttl = await redis.ttl(key);
      // Strip prefix and lock:pr: / lock:issue: to get the resource
      const resource = key.replace(/^[^:]+:lock:(pr|issue):/, "");
      locks.push({ resource, owner, ttl });
    }
  }

  // Consumer groups
  const consumerGroups: ConsumerGroupInfo[] = [];
  for (const stream of taskStreamKeys) {
    try {
      const groups = await redis.xinfo("GROUPS", stream) as unknown[];
      for (const group of groups) {
        // ioredis v5+ returns objects directly; older versions return flat arrays
        const g = typeof (group as Record<string, unknown>).name === "string"
          ? group as Record<string, unknown>
          : flatArrayToMap(group as string[]);
        consumerGroups.push({
          stream,
          name: String(g.name || "?"),
          consumers: Number(g.consumers || 0),
          pending: Number(g.pending || 0),
        });
      }
    } catch {
      // Stream has no consumer groups
    }
  }

  // Recent results (across all result streams, merged by recency)
  const resultEntries: Array<{ entryId: string; result: RecentResult }> = [];
  for (const resultsKey of resultsKeys) {
    try {
      const entries = await redis.xrevrange(resultsKey, "+", "-", "COUNT", maxResults);
      for (const [entryId, fields] of entries) {
        const f = arrayToMap(fields);
        try {
          resultEntries.push({
            entryId,
            result: {
              task_id: f.task_id || "",
              worker_id: f.worker_id || "",
              status: f.status || "",
              resource_type: f.resource_type || "",
              resource_id: f.resource_id || "",
              duration_seconds: parseInt(f.duration_seconds || "0", 10),
              summary: f.summary || "",
            },
          });
        } catch {
          // skip malformed entry
        }
      }
    } catch {
      // ignore
    }
  }
  // Sort by entry ID descending (most recent first) and trim
  resultEntries.sort((a, b) => b.entryId.localeCompare(a.entryId));
  const recentResults = resultEntries.slice(0, maxResults).map((e) => e.result);

  // Attempt counters — keys are prefixed: orcest:pr:*:attempts, transit-platform:pr:*:attempts
  const attemptCounts: Record<string, number> = {};
  const attemptKeys = await scanKeys("*:pr:*:attempts");
  for (const key of attemptKeys) {
    // Skip total_attempts keys
    if (key.includes(":total_attempts")) continue;
    const data = await redis.hgetall(key);
    if (data && data.count) {
      // Key format: prefix:pr:repo:number:attempts — extract PR number (second-to-last segment)
      const parts = key.replace(/:attempts$/, "").split(":");
      const prNum = parts[parts.length - 1];
      try {
        attemptCounts[`PR #${prNum}`] = parseInt(data.count, 10);
      } catch {
        // ignore
      }
    }
  }

  // Queued tasks — read actual entries from task streams for kanban view
  const queuedTasks: QueuedTask[] = [];
  for (const stream of taskStreamKeys) {
    try {
      const entries = await redis.xrange(stream, "-", "+", "COUNT", 50);
      for (const [, fields] of entries) {
        const f = arrayToMap(fields);
        queuedTasks.push({
          task_id: f.id || "",
          task_type: f.type || "?",
          repo: f.repo || "?",
          resource_type: f.resource_type || "?",
          resource_id: f.resource_id || "?",
          created_at: f.created_at || null,
          stream,
        });
      }
    } catch {
      // ignore
    }
  }

  return {
    redis_ok: true,
    fetched_at: new Date().toISOString(),
    queue_depths: queueDepths,
    results_depth: resultsDepth,
    dead_letter_count: deadLetterCount,
    locks,
    consumer_groups: consumerGroups,
    recent_results: recentResults,
    attempt_counts: attemptCounts,
    dead_letter_entries: deadLetterEntries,
    queued_tasks: queuedTasks,
  };
}

/**
 * ioredis returns xrevrange fields as flat arrays: [key, val, key, val, ...].
 * Convert to a map.
 */
function arrayToMap(arr: string[]): Record<string, string> {
  const map: Record<string, string> = {};
  for (let i = 0; i < arr.length; i += 2) {
    map[arr[i]] = arr[i + 1];
  }
  return map;
}

/**
 * XINFO GROUPS returns each group as a flat array: [field, value, field, value, ...].
 */
function flatArrayToMap(arr: unknown[]): Record<string, string> {
  const map: Record<string, string> = {};
  for (let i = 0; i < arr.length; i += 2) {
    map[String(arr[i])] = String(arr[i + 1]);
  }
  return map;
}
