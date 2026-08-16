import { dashboardRedisKeyPatterns, redis, healthCheck, scanKeys, scanKeysMany } from "./redis.js";
import type {
  SystemSnapshot,
  LockInfo,
  ConsumerGroupInfo,
  RecentResult,
  DeadLetterEntry,
  QueuedTask,
  WorkerPoolInfo,
} from "./types.js";
import { readDashboardPolicy } from "./policy.js";
import {
  findTaskOutputPrefixes,
  taskOutputPrefixLookupKey,
  type TaskOutputPrefixLookup,
} from "./workers.js";

const WORKER_CONSUMER_GROUP = "workers";
const QUEUE_DEPTH_FALLBACK_PAGE_SIZE = 1000;
const QUEUE_DEPTH_FALLBACK_MAX_PAGES = 10;

export async function fetchSnapshot(maxResults = 20): Promise<SystemSnapshot> {
  const dashboardPolicy = readDashboardPolicy();
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
      provider_health: {},
      worker_pool: [],
      degraded_sections: [],
      dashboard_policy: dashboardPolicy,
    };
  }

  try {
    return await fetchSnapshotInner(maxResults);
  } catch (err) {
    console.error("Error fetching snapshot:", err);
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
      provider_health: {},
      worker_pool: [],
      degraded_sections: [],
      dashboard_policy: dashboardPolicy,
    };
  }
}

async function fetchSnapshotInner(maxResults: number): Promise<SystemSnapshot> {
  const degradedSections = new Set<string>();
  const markDegraded = (section: string) => degradedSections.add(section);

  // Task streams — keys are prefixed: orcest:tasks:claude,
  // transit-platform:tasks:issue:claude, etc.
  const discoveredTaskStreamKeys = await scanKeysManyForSections(
    dashboardRedisKeyPatterns(["tasks:*"]),
    ["queue depths", "consumer groups", "queued tasks"],
    markDegraded,
  );
  const taskStreamKeys = await filterStreamKeys(
    discoveredTaskStreamKeys,
    ["queue depths", "consumer groups", "queued tasks"],
    markDegraded,
  );
  const taskStreamLengths: Record<string, number | null> = {};
  if (taskStreamKeys.length > 0) {
    const pipeline = redis.pipeline();
    for (const stream of taskStreamKeys) pipeline.xlen(stream);
    const results = await execPipelineForSections(
      pipeline,
      taskStreamKeys.length,
      "queue depths",
      markDegraded,
    );
    for (let i = 0; i < taskStreamKeys.length; i++) {
      const result = results?.[i];
      if (pipelineResultFailed(result)) {
        markDegraded("queue depths");
        taskStreamLengths[taskStreamKeys[i]] = null;
      } else {
        taskStreamLengths[taskStreamKeys[i]] = result[1] as number;
      }
    }
  }

  // Results — each project prefix has its own results stream: orcest:results, transit-platform:results
  let resultsDepth = 0;
  const discoveredResultsKeys = await scanKeysManyForSections(
    dashboardRedisKeyPatterns(["results"]),
    ["results depth", "recent results"],
    markDegraded,
  );
  const resultsKeys = await filterStreamKeys(
    discoveredResultsKeys,
    ["results depth", "recent results"],
    markDegraded,
  );
  if (resultsKeys.length > 0) {
    const pipeline = redis.pipeline();
    for (const key of resultsKeys) pipeline.xlen(key);
    const results = await execPipelineForSections(
      pipeline,
      resultsKeys.length,
      "results depth",
      markDegraded,
    );
    for (const r of results) {
      if (pipelineResultFailed(r)) markDegraded("results depth");
      else resultsDepth += r[1] as number;
    }
  }

  // Dead-letter streams: *:dead-letter
  let deadLetterCount = 0;
  const discoveredDeadLetterKeys = await scanKeysManyForSections(
    dashboardRedisKeyPatterns(["dead-letter"]),
    ["dead-letter depth", "dead-letter entries"],
    markDegraded,
  );
  const dlKeys = await filterStreamKeys(
    discoveredDeadLetterKeys,
    ["dead-letter depth", "dead-letter entries"],
    markDegraded,
  );
  if (dlKeys.length > 0) {
    const pipeline = redis.pipeline();
    for (const key of dlKeys) pipeline.xlen(key);
    const results = await execPipelineForSections(
      pipeline,
      dlKeys.length,
      "dead-letter depth",
      markDegraded,
    );
    for (const r of results) {
      if (pipelineResultFailed(r)) markDegraded("dead-letter depth");
      else deadLetterCount += r[1] as number;
    }
  }

  // Dead-letter entries (most recent 5, across all dead-letter streams)
  const deadLetterEntries: DeadLetterEntry[] = [];
  for (const dlKey of dlKeys) {
    try {
      const dlRaw = await redis.xrevrange(dlKey, "+", "-", "COUNT", 5);
      for (const [entryId, fields] of dlRaw) {
        const fieldMap = arrayToMap(fields);
        deadLetterEntries.push(deadLetterEntryFromFields(dlKey, entryId, fieldMap));
      }
    } catch {
      markDegraded("dead-letter entries");
    }
  }
  // Sort by full stream ID descending, keep top 5. Redis stream timestamps can
  // share the same millisecond, so compare the sequence part before stream key.
  deadLetterEntries.sort(compareDeadLetterEntriesByRecency);
  deadLetterEntries.splice(5);

  // Active locks — keys are prefixed: orcest:lock:pr:*, orcest:lock:issue:*
  const locks: LockInfo[] = [];
  const allLockKeys = await scanKeysManyForSections(
    dashboardRedisKeyPatterns(["lock:pr:*", "lock:issue:*"]),
    "active locks",
    markDegraded,
  );
  const parsedLockKeys = allLockKeys
    .map(parseLockKey)
    .filter((entry): entry is ParsedLockKey => entry !== null);
  if (parsedLockKeys.length > 0) {
    // Batch all owner + TTL + pending-task lookups into one pipeline.
    const lockPipeline = redis.pipeline();
    for (const entry of parsedLockKeys) {
      lockPipeline.get(entry.key);
      lockPipeline.ttl(entry.key);
      lockPipeline.get(pendingKeyForLock(entry));
    }
    const lockResults = await execPipelineForSections(
      lockPipeline,
      parsedLockKeys.length * 3,
      "active locks",
      markDegraded,
    );
    for (let i = 0; i < parsedLockKeys.length; i++) {
      const entry = parsedLockKeys[i];
      const offset = i * 3;
      const ownerResult = lockResults[offset];
      const ttlResult = lockResults[offset + 1];
      const pendingResult = lockResults[offset + 2];
      if (
        pipelineResultFailed(ownerResult) ||
        pipelineResultFailed(ttlResult) ||
        pipelineResultFailed(pendingResult)
      ) {
        markDegraded("active locks");
        continue;
      }
      const lock = lockInfoFromRedisValues(
        entry.key,
        ownerResult[1],
        ttlResult[1],
        pendingResult[1],
      );
      if (lock) locks.push(lock);
    }
  }

  // Consumer groups
  const consumerGroups: ConsumerGroupInfo[] = [];
  const consumerGroupFailedStreams = new Set<string>();
  const streamWorkerGroupProgress = new Map<
    string,
    Array<{ pending: number; lag: number | null; lastDeliveredId: string }>
  >();
  for (const stream of taskStreamKeys) {
    try {
      const groups = await redis.xinfo("GROUPS", stream) as unknown[];
      for (const group of groups) {
        const parsed = consumerGroupInfoFromRedisGroup(stream, group);
        const { pending, lag } = parsed.info;
        consumerGroups.push(parsed.info);
        if (parsed.info.name === WORKER_CONSUMER_GROUP) {
          const progress = streamWorkerGroupProgress.get(stream) || [];
          progress.push({ pending, lag, lastDeliveredId: parsed.lastDeliveredId });
          streamWorkerGroupProgress.set(stream, progress);
        }
      }
    } catch {
      markDegraded("consumer groups");
      consumerGroupFailedStreams.add(stream);
    }
  }
  // Queue depths follow the Python dashboard contract: pending PEL entries plus
  // consumer-group lag. Redis streams keep delivered entries, so XLEN alone makes
  // an idle system look queued. Prefer XINFO GROUPS lag, then fall back to
  // counting entries after each group's last-delivered-id when lag is unavailable.
  const queueDepths: Record<string, number> = {};
  for (const stream of taskStreamKeys) {
    if (consumerGroupFailedStreams.has(stream)) {
      markDegraded("queue depths");
      continue;
    }
    const groups = streamWorkerGroupProgress.get(stream) || [];
    const knownDepth = queueDepthFromKnownGroupState(
      taskStreamLengths[stream] ?? null,
      groups,
    );
    if (knownDepth !== null) {
      queueDepths[stream] = knownDepth;
      continue;
    }
    try {
      const counted = await countQueueDepthFromGroupPositions(stream, groups);
      queueDepths[stream] = counted.depth;
      // A saturated count is a floor, not the true depth: report it, but flag
      // the section so the UI does not present it as exact.
      if (counted.saturated) markDegraded("queue depths");
    } catch {
      markDegraded("queue depths");
    }
  }
  for (const stream of taskStreamKeys) {
    if (consumerGroupFailedStreams.has(stream)) continue;
    if ((streamWorkerGroupProgress.get(stream) || []).length > 0) continue;
    const confirmedDepth = normalizeNonNegativeCount(queueDepths[stream] ?? null);
    if (confirmedDepth === 0) continue;
    consumerGroups.push({
      stream,
      name: WORKER_CONSUMER_GROUP,
      consumers: 0,
      pending: 0,
      lag: confirmedDepth,
    });
    streamWorkerGroupProgress.set(stream, [{
      pending: 0,
      lag: confirmedDepth,
      lastDeliveredId: "0-0",
    }]);
  }

  // Recent results (across all result streams, merged by recency)
  const resultEntries: Array<{ entryId: string; result: RecentResult }> = [];
  for (const resultsKey of resultsKeys) {
    try {
      const entries = await redis.xrevrange(resultsKey, "+", "-", "COUNT", maxResults);
      for (const [entryId, fields] of entries) {
        const f = arrayToMap(fields);
        resultEntries.push({
          entryId,
          result: recentResultFromFields(resultsKey, entryId, f),
        });
      }
    } catch {
      markDegraded("recent results");
    }
  }
  // Sort by entry ID descending (most recent first) and trim.
  // Stream IDs are "<ms>-<seq>" — use numeric comparison to avoid string-comparison bugs
  // (e.g., "9" > "10" lexicographically).
  resultEntries.sort(compareResultEntriesByRecency);
  const recentResults = resultEntries.slice(0, maxResults).map((e) => e.result);

  // Attempt counters — keys are prefixed:
  // orcest:pr:*:attempts, transit-platform:issue:*:attempts, etc.
  const attemptCounts: Record<string, number> = {};
  const attemptKeys = await scanKeysManyForSections(
    dashboardRedisKeyPatterns(["pr:*:attempts", "issue:*:attempts"]),
    "attempt counts",
    markDegraded,
  );
  const filteredAttemptKeys = attemptKeys.filter(k => !k.includes(":total_attempts"));
  if (filteredAttemptKeys.length > 0) {
    const attemptPipeline = redis.pipeline();
    for (const key of filteredAttemptKeys) attemptPipeline.hgetall(key);
    const attemptResults = await execPipelineForSections(
      attemptPipeline,
      filteredAttemptKeys.length,
      "attempt counts",
      markDegraded,
    );
    for (let i = 0; i < filteredAttemptKeys.length; i++) {
      const key = filteredAttemptKeys[i];
      const result = attemptResults[i];
      if (pipelineResultFailed(result)) {
        markDegraded("attempt counts");
        continue;
      }
      const data = (result[1] as Record<string, string> | null);
      if (data && data.count) {
        const label = attemptCountLabelFromKey(key);
        if (!label) continue;
        const count = attemptCountValueFromFields(data);
        if (count !== null) {
          attemptCounts[label] = count;
        }
      }
    }
  }

  // Queued tasks — read only entries not yet delivered to any consumer
  const queuedTasks: QueuedTask[] = [];
  for (const stream of taskStreamKeys) {
    if (consumerGroupFailedStreams.has(stream)) {
      markDegraded("queued tasks");
      continue;
    }
    try {
      // Start after the worker consumer group's last-delivered-id. This preview
      // shows entries still undelivered to workers; pending PEL entries
      // are reflected in queue_depths and consumer_groups.
      // If the workers group is missing, workers cannot consume via the normal
      // group path, so retained entries are actionable and should be visible.
      // NOTE: COUNT 50 caps the entries returned for performance; the true queue depth
      // is available in queue_depths and may be larger.
      const startId = queuedTaskRangeStartForGroups(streamWorkerGroupProgress.get(stream) || []);
      const entries = await redis.xrange(stream, startId, "+", "COUNT", 50);
      for (const [entryId, fields] of entries) {
        const f = arrayToMap(fields);
        queuedTasks.push({
          entry_id: entryId,
          task_id: f.id || "",
          task_type: f.type || "?",
          prefix: f.key_prefix || null,
          repo: f.repo || "?",
          resource_type: f.resource_type || "?",
          resource_id: f.resource_id || "?",
          created_at: f.created_at || null,
          stream,
        });
      }
    } catch {
      markDegraded("queued tasks");
    }
  }

  // Provider health counters (Task 8) — project-scoped providers:{provider}:*
  const providerHealth: Record<string, Record<string, number>> = {};
  try {
    const providerKeys = (await scanKeysManyForSections(
      dashboardRedisKeyPatterns(["providers:*"]),
      "provider health",
      markDegraded,
    ))
      .map((key) => ({ key, metric: providerMetricFromKey(key) }))
      .filter((entry): entry is { key: string; metric: ProviderMetricKey } =>
        entry.metric !== null
      );
    if (providerKeys.length > 0) {
      const ppipe = redis.pipeline();
      for (const entry of providerKeys) ppipe.get(entry.key);
      const pres = await execPipelineForSections(
        ppipe,
        providerKeys.length,
        "provider health",
        markDegraded,
      );
      for (let i = 0; i < providerKeys.length; i++) {
        const result = pres[i];
        if (pipelineResultFailed(result)) {
          markDegraded("provider health");
          continue;
        }
        const val = (result[1] as string | null) || "0";
        addProviderMetricValue(providerHealth, providerKeys[i].metric, val);
      }
    }
  } catch {
    markDegraded("provider health");
  }

  const workerPool = await fetchWorkerPool(markDegraded);
  const dashboardPolicy = readDashboardPolicy();
  await enrichTaskOutputPrefixes(locks, recentResults, markDegraded);

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
    provider_health: providerHealth,
    worker_pool: workerPool,
    degraded_sections: [...degradedSections],
    dashboard_policy: dashboardPolicy,
  };
}

async function enrichTaskOutputPrefixes(
  locks: LockInfo[],
  recentResults: RecentResult[],
  markDegraded: (section: string) => void,
): Promise<void> {
  const lookups: TaskOutputPrefixLookup[] = [];
  const lockKeys: Array<{ lock: LockInfo; key: string }> = [];
  const resultKeys: Array<{ result: RecentResult; key: string }> = [];

  const addLookup = (
    workerId: string,
    taskId: string | null | undefined,
  ): string | null => {
    const worker = workerId.trim();
    const task = taskId?.trim() || "";
    if (!worker || !task || worker === "(expired)") return null;
    const key = taskOutputPrefixLookupKey(worker, task);
    lookups.push({ workerId: worker, taskId: task });
    return key;
  };

  for (const lock of locks) {
    const key = addLookup(lock.owner, lock.task_id);
    if (key) lockKeys.push({ lock, key });
  }
  for (const result of recentResults) {
    const key = addLookup(result.worker_id, result.task_id);
    if (key) resultKeys.push({ result, key });
  }
  if (lookups.length === 0) return;

  let prefixes: Map<string, string | null | undefined>;
  let prefixLookupDegraded = false;
  try {
    const result = await findTaskOutputPrefixes(lookups);
    prefixes = result.prefixes;
    prefixLookupDegraded = result.degraded;
    if (prefixLookupDegraded) markDegraded("task output prefixes");
  } catch {
    markDegraded("task output prefixes");
    for (const { lock } of lockKeys) {
      lock.output_prefix_unresolved = true;
    }
    for (const { result } of resultKeys) {
      result.output_prefix_unresolved = true;
    }
    return;
  }

  for (const { lock, key } of lockKeys) {
    const prefix = prefixes.get(key);
    if (prefix !== undefined) lock.output_prefix = prefix;
    else if (prefixLookupDegraded) lock.output_prefix_unresolved = true;
  }
  for (const { result, key } of resultKeys) {
    const prefix = prefixes.get(key);
    if (prefix !== undefined) result.output_prefix = prefix;
    else if (prefixLookupDegraded) result.output_prefix_unresolved = true;
  }
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

async function scanKeysManyForSections(
  patterns: string[],
  sections: string | string[],
  markDegraded: (section: string) => void,
): Promise<string[]> {
  try {
    return await scanKeysMany(patterns);
  } catch {
    for (const section of Array.isArray(sections) ? sections : [sections]) {
      markDegraded(section);
    }
    return [];
  }
}

async function filterStreamKeys(
  keys: string[],
  sections: string | string[],
  markDegraded: (section: string) => void,
): Promise<string[]> {
  const mark = () => {
    for (const section of Array.isArray(sections) ? sections : [sections]) {
      markDegraded(section);
    }
  };
  if (keys.length === 0) return [];

  // Batch the TYPE probes instead of paying one blocking round trip per key —
  // this runs three times per snapshot build over every discovered key.
  const types = await Promise.all(
    keys.map(async (key) => {
      try {
        return await redis.type(key);
      } catch {
        return null;
      }
    }),
  );

  const streams: string[] = [];
  for (let i = 0; i < keys.length; i++) {
    const type = types[i];
    if (type === null) {
      // Unverified: drop the key rather than passing it through to XINFO/XRANGE
      // as if it were a confirmed stream.
      mark();
      continue;
    }
    if (type === "stream") streams.push(keys[i]);
  }
  return streams;
}

type RedisPipelineResult = [unknown, unknown];
type RedisPipelineExec = {
  exec(): Promise<RedisPipelineResult[] | null>;
};

async function execPipelineForSections(
  pipeline: RedisPipelineExec,
  expectedResults: number,
  sections: string | string[],
  markDegraded: (section: string) => void,
): Promise<Array<RedisPipelineResult | null>> {
  const mark = () => {
    for (const section of Array.isArray(sections) ? sections : [sections]) {
      markDegraded(section);
    }
  };

  try {
    const results = await pipeline.exec();
    if (!results || results.length < expectedResults) mark();
    return Array.from({ length: expectedResults }, (_, index) => results?.[index] ?? null);
  } catch {
    mark();
    return Array.from({ length: expectedResults }, () => null);
  }
}

function pipelineResultFailed(
  result: RedisPipelineResult | null | undefined,
): result is null | undefined {
  return !result || Boolean(result[0]);
}

type ParsedLockKey = {
  key: string;
  prefix: string;
  resourceType: string;
  repo: string;
  resourceId: string;
  resource: string;
};

function parseLockKey(key: string): ParsedLockKey | null {
  const prefixed = key.match(/^(.+):lock:(pr|issue):(.+):([^:]+)$/);
  const unprefixed = key.match(/^lock:(pr|issue):(.+):([^:]+)$/);
  const prefix = prefixed?.[1] ?? "";
  const resourceType = prefixed?.[2] ?? unprefixed?.[1];
  const repo = prefixed?.[3] ?? unprefixed?.[2];
  const resourceId = prefixed?.[4] ?? unprefixed?.[3];

  if (!resourceType || !repo || !resourceId) return null;

  return {
    key,
    prefix,
    resourceType,
    repo,
    resourceId,
    resource: `${repo}:${resourceId}`,
  };
}

function pendingKeyForLock(entry: ParsedLockKey): string {
  const key = `pending:${entry.resourceType}:${entry.repo}:${entry.resourceId}`;
  return entry.prefix ? `${entry.prefix}:${key}` : key;
}

export function parsePendingTaskMetadata(
  value: unknown,
): { taskId: string; createdAt: string | null } | null {
  if (typeof value !== "string" || value.length === 0) return null;

  try {
    const data = JSON.parse(value) as unknown;
    if (data && typeof data === "object") {
      const record = data as Record<string, unknown>;
      const taskId = record.task_id;
      if (typeof taskId === "string" && taskId.length > 0) {
        const createdAt = record.created_at;
        return {
          taskId,
          createdAt: typeof createdAt === "string" && createdAt.length > 0
            ? createdAt
            : null,
        };
      }
    }
  } catch {
    // Legacy pending markers stored the task ID directly as the value.
  }

  return { taskId: value, createdAt: null };
}

export function lockInfoFromRedisValues(
  key: string,
  ownerValue: unknown,
  ttlValue: unknown,
  pendingValue: unknown,
): LockInfo | null {
  const entry = parseLockKey(key);
  if (!entry) return null;

  const ttl = typeof ttlValue === "number" && Number.isFinite(ttlValue)
    ? ttlValue
    : -1;
  if (ttl === -2) return null;

  const pending = parsePendingTaskMetadata(pendingValue);
  return {
    lock_key: entry.key,
    prefix: entry.prefix || null,
    resource: entry.resource,
    resource_type: entry.resourceType,
    repo: entry.repo,
    resource_id: entry.resourceId,
    owner: typeof ownerValue === "string" && ownerValue.length > 0
      ? ownerValue
      : "(expired)",
    ttl,
    task_id: pending?.taskId ?? null,
    pending_created_at: pending?.createdAt ?? null,
  };
}

export function parseAttemptKey(
  key: string,
): {
  prefix: string;
  resourceType: "pr" | "issue";
  repo: string;
  resourceId: string;
} | null {
  const prefixed = key.match(/^(.+):(pr|issue):(.+):([^:]+):attempts$/);
  const unprefixed = key.match(/^(pr|issue):(.+):([^:]+):attempts$/);
  const prefix = prefixed?.[1] ?? "";
  const resourceType = prefixed?.[2] ?? unprefixed?.[1];
  const repo = prefixed?.[3] ?? unprefixed?.[2];
  const resourceId = prefixed?.[4] ?? unprefixed?.[3];

  if ((resourceType !== "pr" && resourceType !== "issue") || !repo || !resourceId) {
    return null;
  }
  return { prefix, resourceType, repo, resourceId };
}

export function attemptCountLabelFromKey(key: string): string | null {
  const parsed = parseAttemptKey(key);
  if (!parsed) return null;
  return attemptLabel(parsed);
}

export function attemptCountValueFromFields(
  fields: Record<string, unknown> | null | undefined,
): number | null {
  const count = parseInteger(fields?.count);
  if (count === null || count <= 0) return null;
  return count;
}

function attemptLabel(
  parsed: {
    prefix: string;
    resourceType: "pr" | "issue";
    repo: string;
    resourceId: string;
  },
): string {
  const resourceLabel = parsed.resourceType === "pr" ? "PR" : "Issue";
  const prefixLabel = parsed.prefix ? `[${parsed.prefix}] ` : "";
  return `${prefixLabel}${parsed.repo} ${resourceLabel} #${parsed.resourceId}`;
}

type ProviderMetricKey = {
  provider: string;
  metric: string;
};

export function providerMetricFromKey(key: string): ProviderMetricKey | null {
  const prefixed = key.match(/^(.+):providers:([^:]+):(.+)$/);
  const unprefixed = key.match(/^providers:([^:]+):(.+)$/);
  if (!prefixed && !unprefixed) return null;

  const prefix = prefixed?.[1] || "";
  const provider = prefixed?.[2] ?? unprefixed?.[1] ?? "";
  const metric = prefixed?.[3] ?? unprefixed?.[2] ?? "";
  if (!provider || !metric) return null;
  if (provider === "credential_overrides") return null;

  return { provider: prefix ? `[${prefix}] ${provider}` : provider, metric };
}

export function addProviderMetricValue(
  health: Record<string, Record<string, number>>,
  parsed: ProviderMetricKey,
  value: unknown,
): boolean {
  const n = parseInteger(value);
  if (n === null || n < 0) return false;

  if (!health[parsed.provider]) health[parsed.provider] = {};
  health[parsed.provider][parsed.metric] =
    (health[parsed.provider][parsed.metric] || 0) + n;
  return true;
}

export function queueDepthFromKnownGroupState(
  streamLength: number | null,
  groups: Array<{ pending?: number; lag: number | null }>,
): number | null {
  if (groups.length === 0) {
    return streamLength === null ? null : normalizeNonNegativeCount(streamLength);
  }
  let total = 0;
  for (const group of groups) {
    if (group.lag === null || !Number.isFinite(group.lag) || group.lag < 0) return null;
    total += normalizeNonNegativeCount(group.pending) + normalizeNonNegativeCount(group.lag);
  }
  return total;
}

export function consumerGroupInfoFromRedisGroup(
  stream: string,
  group: unknown,
): {
  info: ConsumerGroupInfo;
  lastDeliveredId: string;
} {
  // ioredis v5+ returns objects directly; older versions return flat arrays.
  const g = group && typeof group === "object" && !Array.isArray(group)
    ? group as Record<string, unknown>
    : flatArrayToMap(Array.isArray(group) ? group : []);
  const lastDeliveredId = String(g["last-delivered-id"] || "0-0");
  return {
    info: {
      stream,
      name: String(g.name || "?"),
      consumers: normalizeNonNegativeCount(g.consumers),
      pending: normalizeNonNegativeCount(g.pending),
      lag: parseNullableNonNegativeNumber(g.lag),
    },
    lastDeliveredId,
  };
}

export function oldestLastDeliveredIdForGroups(
  groups: Array<{ lastDeliveredId: string }>,
): string {
  if (groups.length === 0) return "0-0";
  return groups
    .map((group) => normalizeStreamId(group.lastDeliveredId))
    .sort(compareStreamIds)[0];
}

export function queuedTaskRangeStartForGroups(
  groups: Array<{ lastDeliveredId: string }>,
): string {
  if (groups.length === 0) return "-";
  const lastDeliveredId = oldestLastDeliveredIdForGroups(groups);
  return lastDeliveredId && lastDeliveredId !== "0-0" ? `(${lastDeliveredId}` : "-";
}

export function recentResultFromFields(
  resultStream: string,
  entryId: string,
  fields: Record<string, string>,
): RecentResult {
  return {
    result_id: `${resultStream}:${entryId}`,
    result_stream: resultStream,
    entry_id: entryId,
    task_id: fields.task_id || "",
    worker_id: fields.worker_id || "",
    status: fields.status || "",
    repo: fields.repo || null,
    resource_type: fields.resource_type || "",
    resource_id: fields.resource_id || "",
    duration_seconds: parseDurationSeconds(fields.duration_seconds),
    summary: fields.summary || "",
  };
}

function parseDurationSeconds(value: string | undefined): number {
  const trimmed = value?.trim() ?? "";
  if (!/^\d+$/.test(trimmed)) return -1;
  const parsed = Number(trimmed);
  return Number.isSafeInteger(parsed) ? parsed : -1;
}

export function deadLetterEntryFromFields(
  deadLetterStream: string,
  entryId: string,
  fields: Record<string, string>,
): DeadLetterEntry {
  const timestampMs = streamTimestampMs(entryId);

  return {
    dead_letter_id: `${deadLetterStream}:${entryId}`,
    dead_letter_stream: deadLetterStream,
    entry_id: entryId,
    task_id: fields.id || fields.task_id || "",
    task_type: fields.type || "?",
    repo: fields.repo || "?",
    resource_type: fields.resource_type || "?",
    resource_id: fields.resource_id || "?",
    timestamp_ms: timestampMs,
    reason: fields.dead_letter_reason || null,
  };
}

function streamTimestampMs(entryId: string): number | null {
  return streamIdParts(entryId)?.[0] ?? null;
}

async function fetchWorkerPool(markDegraded?: (section: string) => void): Promise<WorkerPoolInfo[]> {
  try {
    const prefixes = await discoverWorkerPoolPrefixes();
    if (prefixes.length === 0) return [];

    const pools: WorkerPoolInfo[] = [];
    for (const prefix of prefixes) {
      try {
        const pipeline = redis.pipeline();
        pipeline.get(poolKey(prefix, "current_template_vmid"));
        pipeline.smembers(poolKey(prefix, "idle"));
        pipeline.hgetall(poolKey(prefix, "active"));
        const results = await pipeline.exec();
        if (
          !results ||
          results.length < 3 ||
          results.some((result) => !result || result[0])
        ) {
          markDegraded?.("worker pool");
        }
        const templateResult = results?.[0];
        const idleResult = results?.[1];
        const activeResult = results?.[2];

        pools.push(normalizeWorkerPoolSnapshot(
          prefix,
          !templateResult || templateResult[0] ? null : templateResult[1],
          !idleResult || idleResult[0] ? [] : idleResult[1],
          !activeResult || activeResult[0] ? {} : activeResult[1],
        ));
      } catch {
        markDegraded?.("worker pool");
      }
    }

    return pools.filter((pool) =>
      pool.template_vmid ||
      pool.idle_count > 0 ||
      pool.active_count > 0,
    );
  } catch {
    markDegraded?.("worker pool");
    return [];
  }
}

async function discoverWorkerPoolPrefixes(): Promise<string[]> {
  const keys = new Set<string>();
  for (const pattern of dashboardRedisKeyPatterns([
    "pool:current_template_vmid",
    "pool:idle",
    "pool:active",
  ])) {
    for (const key of await scanKeys(pattern)) keys.add(key);
  }

  const prefixes = new Set<string>();
  for (const key of keys) {
    const prefix = workerPoolPrefixFromKey(key);
    if (prefix !== null) prefixes.add(prefix);
  }

  return [...prefixes].sort((a, b) => {
    if (a === "orcest") return -1;
    if (b === "orcest") return 1;
    return a.localeCompare(b);
  });
}

function workerPoolPrefixFromKey(key: string): string | null {
  if (key === "pool:current_template_vmid" || key === "pool:idle" || key === "pool:active") {
    return "";
  }

  const match = key.match(/^(.+):pool:(current_template_vmid|idle|active)$/);
  return match?.[1] ?? null;
}

function poolKey(prefix: string, suffix: "current_template_vmid" | "idle" | "active"): string {
  return prefix ? `${prefix}:pool:${suffix}` : `pool:${suffix}`;
}

export function normalizeWorkerPoolSnapshot(
  prefix: string,
  templateValue: unknown,
  idleValue: unknown,
  activeValue: unknown,
  nowSeconds = Date.now() / 1000,
): WorkerPoolInfo {
  const idle = Array.isArray(idleValue)
    ? normalizeVmIdList(idleValue)
    : [];
  const activeEntries = activeValue && typeof activeValue === "object" && !Array.isArray(activeValue)
    ? Object.entries(activeValue as Record<string, unknown>)
    : [];
  const activeVmids = new Set<string>();
  const active = activeEntries
    .map(([vmid, startedAt]) => normalizeActiveVm(vmid, startedAt, nowSeconds))
    .filter((vm) => {
      if (!vm.vmid || activeVmids.has(vm.vmid)) return false;
      activeVmids.add(vm.vmid);
      return true;
    })
    .sort((a, b) => compareVmIds(a.vmid, b.vmid));
  const template = typeof templateValue === "string" && templateValue.trim()
    ? templateValue.trim()
    : null;

  return {
    prefix,
    template_vmid: template,
    idle,
    active,
    idle_count: idle.length,
    active_count: active.length,
  };
}

function normalizeVmIdList(value: unknown[]): string[] {
  const seen = new Set<string>();
  const vmids: string[] = [];
  for (const item of value) {
    const vmid = String(item).trim();
    if (!vmid || seen.has(vmid)) continue;
    seen.add(vmid);
    vmids.push(vmid);
  }
  return vmids.sort(compareVmIds);
}

function normalizeActiveVm(
  vmid: string,
  startedAt: unknown,
  nowSeconds: number,
): WorkerPoolInfo["active"][number] {
  vmid = vmid.trim();
  const startedAtSeconds = parseNullableNumber(startedAt);
  if (startedAtSeconds === null || startedAtSeconds <= 0) {
    return { vmid, started_at: null, age_seconds: null };
  }
  const startedAtDate = new Date(startedAtSeconds * 1000);
  if (!Number.isFinite(startedAtDate.getTime())) {
    return { vmid, started_at: null, age_seconds: null };
  }

  return {
    vmid,
    started_at: startedAtDate.toISOString(),
    age_seconds: Math.max(0, Math.floor(nowSeconds - startedAtSeconds)),
  };
}

function compareVmIds(a: string, b: string): number {
  const aNumber = Number(a);
  const bNumber = Number(b);
  if (Number.isFinite(aNumber) && Number.isFinite(bNumber)) {
    return aNumber - bNumber;
  }
  return a.localeCompare(b);
}

function parseNullableNumber(value: unknown): number | null {
  if (value === null || value === undefined) return null;
  if (typeof value !== "number" && typeof value !== "string") return null;
  const trimmed = typeof value === "string" ? value.trim() : value;
  if (trimmed === "") return null;
  const parsed = Number(trimmed);
  return Number.isFinite(parsed) ? parsed : null;
}

function parseNullableNonNegativeNumber(value: unknown): number | null {
  const parsed = parseNullableNumber(value);
  return parsed !== null && parsed >= 0 ? parsed : null;
}

function parseInteger(value: unknown): number | null {
  const parsed = parseNullableNumber(value);
  return parsed !== null && Number.isInteger(parsed) ? parsed : null;
}

function compareStreamIds(a: string, b: string): number {
  const [aMs, aSeq] = streamIdParts(a) ?? [0, 0];
  const [bMs, bSeq] = streamIdParts(b) ?? [0, 0];
  return aMs - bMs || aSeq - bSeq;
}

function compareDeadLetterEntriesByRecency(a: DeadLetterEntry, b: DeadLetterEntry): number {
  return compareStreamIds(b.entry_id, a.entry_id) ||
    a.dead_letter_stream.localeCompare(b.dead_letter_stream);
}

function compareResultEntriesByRecency(
  a: { entryId: string; result: RecentResult },
  b: { entryId: string; result: RecentResult },
): number {
  return compareStreamIds(b.entryId, a.entryId) ||
    a.result.result_stream.localeCompare(b.result.result_stream);
}

function normalizeStreamId(value: string | null | undefined): string {
  const trimmed = (value || "").trim();
  return streamIdParts(trimmed) ? trimmed : "0-0";
}

function streamIdParts(entryId: string): [number, number] | null {
  const match = entryId.match(/^(\d+)-(\d+)$/);
  if (!match) return null;
  const ms = Number(match[1]);
  const seq = Number(match[2]);
  return Number.isSafeInteger(ms) && Number.isSafeInteger(seq) ? [ms, seq] : null;
}

type QueueDepthCount = { depth: number; saturated: boolean };

async function countStreamEntriesAfter(
  stream: string,
  lastDeliveredId: string,
): Promise<QueueDepthCount> {
  let total = 0;
  let start = lastDeliveredId && lastDeliveredId !== "0-0" ? `(${lastDeliveredId}` : "-";

  for (let page = 0; page < QUEUE_DEPTH_FALLBACK_MAX_PAGES; page++) {
    const entries = await redis.xrange(stream, start, "+", "COUNT", QUEUE_DEPTH_FALLBACK_PAGE_SIZE);
    total += entries.length;
    if (entries.length < QUEUE_DEPTH_FALLBACK_PAGE_SIZE) return { depth: total, saturated: false };
    start = `(${entries[entries.length - 1][0]}`;
  }

  // The backlog is deeper than the page budget. Report the partial count — the
  // deepest queue is exactly when the dashboard must not report nothing.
  return { depth: total, saturated: true };
}

async function countQueueDepthFromGroupPositions(
  stream: string,
  groups: Array<{ pending: number; lastDeliveredId: string }>,
): Promise<QueueDepthCount> {
  if (groups.length === 0) {
    return countStreamEntriesAfter(stream, "0-0");
  }

  let total = 0;
  let saturated = false;
  for (const group of groups) {
    total += normalizeNonNegativeCount(group.pending);
    const counted = await countStreamEntriesAfter(stream, group.lastDeliveredId);
    total += counted.depth;
    saturated = saturated || counted.saturated;
  }
  return { depth: total, saturated };
}

function normalizeNonNegativeCount(value: unknown): number {
  const parsed = parseInteger(value);
  return parsed !== null && parsed > 0 ? parsed : 0;
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
